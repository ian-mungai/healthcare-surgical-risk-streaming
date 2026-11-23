"""The batch entry point: windows from a plain JSON file in, feature rows in a plain JSON file out.

``python -m features.batch WINDOWS.json ROWS.json``. The input is ``{"windows": [...]}``, a non-empty array. A window holds ``window_id`` (a text, distinct in
the file, that the output repeats), ``cutoff_us``, ``numeric`` (a track to a list of ``[time_us, value, valid]``, except the per-second arterial series
``Silver/ART_MBP_1S``, whose reading is ``[time_us, value, valid, sample_count, sum_high, sum_low]``: its exact mean is the count and the two floats of the
exact sum of its samples, as ``features.series_input`` makes them, and a valid reading inside the window with no exact mean is refused), ``waveforms`` (a track
to ``{"sample_rate", "seconds": [{"second", "samples", "valid", "start_offset_us"}]}``) and ``clinical`` (a field to ``{"value", "available_us"}``). Every key
is required and no other key is accepted: the key sets are ``INPUT_OBJECTS`` in the registry, read by ``features.document``. A member that appears twice in
one object is refused. The output is ``{"rows": [...]}``, in window order.
Spark wiring comes with the Silver and streaming jobs: they read their windows elsewhere and call ``features.row.build_row``, as this entry point does.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from features import document
from features.leakage import LeakageError, row_problems
from features.model import FeatureInputError, FeatureRow, WindowInput
from features.row import build_row


def compute_rows(windows: Iterable[WindowInput], include_sensitivity: bool = False) -> list[FeatureRow]:
    """The rows of the windows, each checked for leakage before it is returned."""
    rows: list[FeatureRow] = []
    for window in windows:
        row = build_row(window, include_sensitivity)
        problems = row_problems(row)
        if problems:
            raise LeakageError("; ".join(problems))
        rows.append(row)
    return rows


def row_document(window_id: object, row: FeatureRow) -> dict[str, Any]:
    return {
        "window_id": window_id,
        "cutoff_us": row.cutoff_us,
        "latest_reading_us": row.latest_reading_us,
        "latest_clinical_us": row.latest_clinical_us,
        "features": row.values,
    }


def serialise(rows: Iterable[tuple[object, FeatureRow]]) -> str:
    """The output text for rows with their window IDs: the one way a row is written, for every path."""
    return json.dumps({"rows": [row_document(window_id, row) for window_id, row in rows]}, sort_keys=True, allow_nan=False) + "\n"


def run(source: Path, target: Path) -> int:
    """Read the windows, write the rows. The target is replaced in one step, or not written at all."""
    windows = document.parse_file(document.load(source.read_text()))
    rows = compute_rows(window for _, window in windows)
    text = serialise((window_id, row) for (window_id, _), row in zip(windows, rows, strict=True))
    handle, temporary = tempfile.mkstemp(dir=target.parent, prefix=f".{target.name}.")
    try:
        with os.fdopen(handle, "w") as stream:
            stream.write(text)
        os.replace(temporary, target)
    finally:
        Path(temporary).unlink(missing_ok=True)
    return len(rows)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) != 2:
        sys.stderr.write("usage: python -m features.batch WINDOWS.json ROWS.json\n")
        return 2
    try:
        run(Path(arguments[0]), Path(arguments[1]))
    except (FeatureInputError, LeakageError, OSError, KeyError, TypeError, ValueError) as error:
        sys.stderr.write(f"features.batch: {error}\n")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
