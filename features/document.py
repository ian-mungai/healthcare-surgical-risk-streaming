"""The shapes of the input: a JSON file of windows, or one window as a mapping. The JSON entry point and a direct call share these functions.

Every object has a closed key set (``INPUT_OBJECTS`` in the registry): each key is required, and a key that is not in the set is refused by name. A key has no
default. A member that appears twice in one object, at any depth, is refused. Every container is checked before it is read. The values are not converted: the
checks of ``build_row`` decide whether a time is whole, a flag a boolean and a value a number.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from features.model import ClinicalValue, FeatureInputError, Reading, WaveformSecond, WaveformWindow, WindowInput
from features.registry import ARTERIAL_SERIES, INPUT_OBJECTS
from features.series_input import series_readings_from_rows


def reject_repeats(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """The hook of ``json.loads`` for every object: a member name that appears twice is refused instead of the later one overwriting the earlier."""
    seen: set[str] = set()
    for name, _ in pairs:
        if name in seen:
            raise FeatureInputError(f"the member {name} appears twice in one object")
        seen.add(name)
    return dict(pairs)


def load(text: str) -> Any:
    return json.loads(text, object_pairs_hook=reject_repeats)


def as_object(value: object, path: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise FeatureInputError(f"{path} is not an object")
    return dict(value)


def as_array(value: object, path: str) -> list[Any]:
    if not isinstance(value, list):
        raise FeatureInputError(f"{path} is not an array")
    return value


def closed(value: object, kind: str, path: str) -> dict[str, Any]:
    """The object of one kind, with exactly the keys of its set: an unknown key and a missing key are each refused by name."""
    body = as_object(value, path)
    keys = INPUT_OBJECTS[kind]
    for key in body:
        if key not in keys:
            raise FeatureInputError(f"{path}: unknown key {key}")
    for key in keys:
        if key not in body:
            raise FeatureInputError(f"{path}: {key} is missing")
    return body


def parse_window(value: object, path: str = "the window") -> WindowInput:
    """The window of a mapping, with no conversion of its values."""
    body = closed(value, "window", path)
    if not isinstance(body["window_id"], str) or not body["window_id"]:
        raise FeatureInputError(f"{path}: window_id is not a non-empty text")
    numeric: dict[str, list[Reading]] = {}
    for track, readings in as_object(body["numeric"], f"{path}.numeric").items():
        where = f"{path}.numeric[{track}]"
        numeric[track] = []
        for index, reading in enumerate(as_array(readings, where)):
            triple = as_array(reading, f"{where}[{index}]")
            if len(triple) != 3:
                raise FeatureInputError(f"{where}[{index}] is not an array of time, value and valid flag")
            numeric[track].append(Reading(triple[0], triple[1], triple[2]))
        if track == ARTERIAL_SERIES:
            numeric[track] = series_readings_from_rows([(reading.time_us, reading.value, reading.valid) for reading in numeric[track]])
    waveforms: dict[str, WaveformWindow] = {}
    for track, item in as_object(body["waveforms"], f"{path}.waveforms").items():
        where = f"{path}.waveforms[{track}]"
        block = closed(item, "waveform", where)
        seconds = []
        for index, entry in enumerate(as_array(block["seconds"], f"{where}.seconds")):
            second = closed(entry, "second", f"{where}.seconds[{index}]")
            seconds.append(WaveformSecond(second["second"], second["samples"], second["valid"], second["start_offset_us"]))
        waveforms[track] = WaveformWindow(block["sample_rate"], tuple(seconds))
    clinical: dict[str, ClinicalValue] = {}
    for field, item in as_object(body["clinical"], f"{path}.clinical").items():
        entry = closed(item, "clinical_value", f"{path}.clinical[{field}]")
        clinical[field] = ClinicalValue(entry["value"], entry["available_us"])
    return WindowInput(body["cutoff_us"], numeric, waveforms, clinical)


def parse_file(value: object) -> list[tuple[str, WindowInput]]:
    """The windows of a file as ``(window_id, window)``: an object with the one key ``windows``, a non-empty array of windows with distinct IDs."""
    body = closed(value, "file", "the top level")
    windows = as_array(body["windows"], "windows")
    if not windows:
        raise FeatureInputError("windows is empty")
    parsed: list[tuple[str, WindowInput]] = []
    seen: set[str] = set()
    for index, window in enumerate(windows):
        result = parse_window(window, f"windows[{index}]")
        window_id = window["window_id"]
        if window_id in seen:
            raise FeatureInputError(f"windows[{index}]: the window_id {window_id} appears twice")
        seen.add(window_id)
        parsed.append((window_id, result))
    return parsed


def parse_mapping(window: WindowInput | Mapping[str, Any]) -> WindowInput:
    return window if isinstance(window, WindowInput) else parse_window(window)
