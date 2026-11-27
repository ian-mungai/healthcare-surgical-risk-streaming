"""The one entry point of the feature code: ``build_row``. The batch job and the streaming job both call it, and it makes every row the same way.

It checks every input, refuses a track in the wrong input map, and gives every number as a float and every count as an int.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from features.clinical import clinical_group
from features.document import parse_mapping
from features.model import FeatureInputError, FeatureRow, FeatureValue, WaveformWindow, WindowInput
from features.numeric import numeric_group
from features.registry import ARTERIAL_SERIES, NUMERIC_TRACKS, WAVEFORM_TRACKS
from features.series_input import SeriesReadings, check_window
from features.waveform import waveform_group

EMPTY_WAVEFORM = WaveformWindow(1.0, ())


def build_row(window: WindowInput | Mapping[str, Any], include_sensitivity: bool = False) -> FeatureRow:
    """The features of one window: every registry track has its group, present or not, so the columns never depend on the data.

    A mapping is read by the same shared function as the JSON entry point (``features.document``), so both refuse the same shapes.
    """
    window = parse_mapping(window)
    for track in window.numeric:
        if track not in NUMERIC_TRACKS:
            raise FeatureInputError(f"unknown track {track} in the numeric input")
    series = window.numeric.get(ARTERIAL_SERIES)
    if series and not isinstance(series, SeriesReadings):
        raise FeatureInputError(f"{ARTERIAL_SERIES}: the readings were not made by features.pressure_series (use features.series_input.series_readings)")
    if series:
        check_window(series, window.cutoff_us)
    for track in window.waveforms:
        if track not in WAVEFORM_TRACKS:
            raise FeatureInputError(f"unknown track {track} in the waveform input")
    values: dict[str, FeatureValue] = {}
    latest: list[int] = []
    for track in NUMERIC_TRACKS:
        group = numeric_group(track, window.numeric.get(track, ()), window.cutoff_us)
        values |= group.values
        if group.latest_us is not None:
            latest.append(group.latest_us)
    for track in WAVEFORM_TRACKS:
        group = waveform_group(track, window.waveforms.get(track, EMPTY_WAVEFORM), window.cutoff_us)
        values |= group.values
        if group.latest_us is not None:
            latest.append(group.latest_us)
    clinical = clinical_group(window.clinical, window.cutoff_us, include_sensitivity)
    values |= clinical.values
    return FeatureRow(window.cutoff_us, values, max(latest) if latest else None, clinical.latest_us)
