"""The features of one waveform over the 20-second window that ends at the cutoff."""

from __future__ import annotations

import math

from features.model import SECOND, FeatureInputError, FeatureValue, GroupResult, WaveformSecond, WaveformValid, WaveformWindow
from features.registry import MIN_VALID_SHARE, WAVEFORM_TRACKS, WAVEFORM_WINDOW_SECONDS, slug


def window_seconds(track: str, window: WaveformWindow, cutoff_us: int) -> WaveformValid:
    """The valid seconds among the 20 complete seconds that end at or before the cutoff. A second not complete at the cutoff is never read."""
    if not math.isfinite(window.sample_rate) or window.sample_rate <= 0:
        raise FeatureInputError(f"{track}: the sample rate is not a positive finite number")
    last = cutoff_us // SECOND - 1
    first = last - WAVEFORM_WINDOW_SECONDS + 1
    inside = sorted((second for second in window.seconds if first <= second.second <= last), key=lambda second: second.second)
    if len({second.second for second in inside}) != len(inside):
        raise FeatureInputError(f"{track}: the window holds more than one second with one index")
    valid: list[WaveformSecond] = []
    for second in inside:
        if not second.valid:
            continue
        if not second.samples or not all(math.isfinite(sample) for sample in second.samples):
            raise FeatureInputError(f"{track}: a valid second has no sample or a sample that is not a finite number")
        if not 0 <= second.start_offset_us < SECOND:
            raise FeatureInputError(f"{track}: the start offset of a second is outside the second")
        valid.append(second)
    return WaveformValid(window.sample_rate, first, valid)


def waveform_group(track: str, window: WaveformWindow, cutoff_us: int) -> GroupResult:
    if track not in WAVEFORM_TRACKS:
        raise FeatureInputError(f"unknown track {track}")
    valid = window_seconds(track, window, cutoff_us)
    enough = len(valid.seconds) >= MIN_VALID_SHARE * WAVEFORM_WINDOW_SECONDS
    values: dict[str, FeatureValue] = {f"{slug(track)}__{stat.key}": stat.compute(valid) if enough or stat.always else None for stat in WAVEFORM_TRACKS[track]}
    return GroupResult(values, (valid.seconds[-1].second + 1) * SECOND if enough else None)
