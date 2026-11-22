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
from collections.abc import Iterable, Sequence
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


WINDOW_KEYS = ("window_id", "cutoff_us", "numeric", "waveforms", "clinical")
SECOND_KEYS = ("second", "samples", "valid", "start_offset_us")


def as_object(value: object, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise FeatureInputError(f"{path} is not an object")
    return value


def as_array(value: object, path: str) -> list[Any]:
    if not isinstance(value, list):
        raise FeatureInputError(f"{path} is not an array")
    return value


def required(body: dict[str, Any], key: str, path: str) -> Any:
    if key not in body:
        raise FeatureInputError(f"{path}: {key} is missing")
    return body[key]


def parse_window(document: object, path: str = "windows[0]") -> WindowInput:
    """The window of a JSON document, with no conversion: ``build_row`` checks every type, so a text is never taken for a flag and a float never for a time.

    Every container is checked before it is read: an object where an array is expected, or an array where an object is expected, is a named input error.
    """
    body = as_object(document, path)
    for key in body:
        if key not in WINDOW_KEYS:
            raise FeatureInputError(f"{path}: unknown key {key}")
    numeric: dict[str, list[Reading]] = {}
    for track, readings in as_object(body.get("numeric", {}), f"{path}.numeric").items():
        where = f"{path}.numeric[{track}]"
        numeric[track] = []
        for index, reading in enumerate(as_array(readings, where)):
            triple = as_array(reading, f"{where}[{index}]")
            if len(triple) != 3:
                raise FeatureInputError(f"{where}[{index}] is not an array of time, value and valid flag")
            numeric[track].append(Reading(triple[0], triple[1], triple[2]))
    waveforms: dict[str, WaveformWindow] = {}
    for track, window in as_object(body.get("waveforms", {}), f"{path}.waveforms").items():
        where = f"{path}.waveforms[{track}]"
        block = as_object(window, where)
        seconds = []
        for index, item in enumerate(as_array(required(block, "seconds", where), f"{where}.seconds")):
            second = as_object(item, f"{where}.seconds[{index}]")
            for key in ("second", "samples", "valid"):
                required(second, key, f"{where}.seconds[{index}]")
            seconds.append(WaveformSecond(second["second"], second["samples"], second["valid"], second.get("start_offset_us", 0)))
        waveforms[track] = WaveformWindow(required(block, "sample_rate", where), tuple(seconds))
    clinical: dict[str, ClinicalValue] = {}
    for field, item in as_object(body.get("clinical", {}), f"{path}.clinical").items():
        where = f"{path}.clinical[{field}]"
        entry = as_object(item, where)
        clinical[field] = ClinicalValue(required(entry, "value", where), required(entry, "available_us", where))
    return WindowInput(required(body, "cutoff_us", path), numeric, waveforms, clinical)


def read_windows(source: Path) -> list[dict[str, Any]]:
    """The window documents of a file: an object with the one key ``windows``, a non-empty array."""
    document = json.loads(source.read_text())
    if not isinstance(document, dict):
        raise FeatureInputError("the top level of the file is not an object")
    for key in document:
        if key != "windows":
            raise FeatureInputError(f"the top level has an unknown key {key}")
    windows = as_array(required(document, "windows", "the top level"), "windows")
    if not windows:
        raise FeatureInputError("windows is empty")
    return [as_object(window, f"windows[{index}]") for index, window in enumerate(windows)]


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
    documents = read_windows(source)
    rows = compute_rows(parse_window(document, f"windows[{index}]") for index, document in enumerate(documents))
    text = serialise((document.get("window_id"), row) for document, row in zip(documents, rows, strict=True))
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
