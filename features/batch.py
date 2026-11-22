"""The batch entry point: windows from a plain JSON file in, feature rows in a plain JSON file out.

``python -m features.batch WINDOWS.json ROWS.json``. The input is ``{"windows": [...]}``. A window holds ``cutoff_us``, ``numeric`` (a track to a list of
``[time_us, value, valid]``), ``waveforms`` (a track to ``{"sample_rate", "seconds": [{"second", "samples", "valid", "start_offset_us"}]}``) and ``clinical``
(a field to ``{"value", "available_us"}``), and may hold a ``window_id`` that the output repeats. The output is ``{"rows": [...]}``, in window order.
Spark wiring comes with the Silver and streaming jobs: they read their windows elsewhere and call ``features.row.build_row``, as this entry point does.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from features.leakage import LeakageError, row_problems
from features.model import ClinicalValue, FeatureInputError, FeatureRow, Reading, WaveformSecond, WaveformWindow, WindowInput
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


def parse_window(document: Mapping[str, Any]) -> WindowInput:
    try:
        return WindowInput(
            int(document["cutoff_us"]),
            {
                track: [Reading(int(time), float(value), bool(valid)) for time, value, valid in readings]
                for track, readings in document.get("numeric", {}).items()
            },
            {
                track: WaveformWindow(
                    float(body["sample_rate"]),
                    tuple(
                        WaveformSecond(
                            int(second["second"]), [float(sample) for sample in second["samples"]], bool(second["valid"]), int(second.get("start_offset_us", 0))
                        )
                        for second in body["seconds"]
                    ),
                )
                for track, body in document.get("waveforms", {}).items()
            },
            {
                field: ClinicalValue(item["value"], None if item["available_us"] is None else int(item["available_us"]))
                for field, item in document.get("clinical", {}).items()
            },
        )
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        raise FeatureInputError(f"a window is not in the batch format: {error!r}") from error


def row_document(window_id: object, row: FeatureRow) -> dict[str, Any]:
    return {
        "window_id": window_id,
        "cutoff_us": row.cutoff_us,
        "latest_reading_us": row.latest_reading_us,
        "latest_clinical_us": row.latest_clinical_us,
        "features": row.values,
    }


def run(source: Path, target: Path) -> int:
    """Read the windows, write the rows. The target is replaced in one step, or not written at all."""
    documents = json.loads(source.read_text())["windows"]
    rows = compute_rows(parse_window(document) for document in documents)
    text = json.dumps(
        {"rows": [row_document(document.get("window_id"), row) for document, row in zip(documents, rows, strict=True)]}, sort_keys=True, allow_nan=False
    )
    handle, temporary = tempfile.mkstemp(dir=target.parent, prefix=f".{target.name}.")
    try:
        with os.fdopen(handle, "w") as stream:
            stream.write(text + "\n")
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
