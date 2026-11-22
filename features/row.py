"""The one entry point of the feature code: ``build_row``. The batch job and the streaming job both call it."""

from __future__ import annotations

from features.clinical import clinical_group
from features.model import FeatureInputError, FeatureRow, FeatureValue, WaveformWindow, WindowInput
from features.numeric import numeric_group
from features.registry import NUMERIC_TRACKS, WAVEFORM_TRACKS
from features.waveform import waveform_group

EMPTY_WAVEFORM = WaveformWindow(1.0, ())


def build_row(window: WindowInput, include_sensitivity: bool = False) -> FeatureRow:
    """The features of one window: every registry track has its group, present or not, so the columns never depend on the data."""
    for track in (*window.numeric, *window.waveforms):
        if track not in NUMERIC_TRACKS and track not in WAVEFORM_TRACKS:
            raise FeatureInputError(f"unknown track {track}")
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
