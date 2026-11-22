"""The features of one waveform over the 20-second window that ends at the cutoff."""

from __future__ import annotations

import math

from features.model import FeatureInputError, FeatureValue, GroupResult, WaveformSecond, WaveformValid, WaveformWindow
from features.registry import WAVEFORM_TRACKS, WAVEFORM_WINDOW_SECONDS, complete_seconds, last_sample_us, slug
from features.validate import boolean, number, whole_microseconds, whole_number


def whole_rate(track: str, rate: object) -> int:
    """The sample rate as a positive whole number. A float with a fraction, zero, a negative, not a number, a boolean and a text are refused."""
    if isinstance(rate, bool) or not isinstance(rate, int | float) or not math.isfinite(rate) or rate <= 0 or rate != int(rate):
        raise FeatureInputError(f"{track}: the sample rate is not a positive whole number")
    return int(rate)


def window_seconds(track: str, window: WaveformWindow, cutoff_us: int) -> WaveformValid:
    """The valid seconds among the newest 20 complete seconds at the cutoff (``complete_seconds``), oldest first, with their samples as floats.

    A valid second is a whole-second block: it starts on its second boundary and holds exactly ``rate`` finite samples. Anything else is refused.
    """
    whole_microseconds(cutoff_us, "the cutoff")
    rate = whole_rate(track, window.sample_rate)
    for second in window.seconds:
        whole_number(second.second, f"{track}: a second index")
        boolean(second.valid, f"{track}: a second's valid flag")
        whole_microseconds(second.start_offset_us, f"{track}: the start offset of a second")
    span = complete_seconds(cutoff_us, WAVEFORM_WINDOW_SECONDS)
    inside = sorted((second for second in window.seconds if second.second in span), key=lambda second: second.second)
    if len({second.second for second in inside}) != len(inside):
        raise FeatureInputError(f"{track}: the window holds more than one second with one index")
    valid: list[WaveformSecond] = []
    for second in inside:
        if not second.valid:
            continue
        if second.start_offset_us != 0:
            raise FeatureInputError(f"{track}: a valid second does not start on a whole second")
        if len(second.samples) != rate:
            raise FeatureInputError(f"{track}: a valid second holds {len(second.samples)} samples and its sample count must be the rate {rate}")
        samples = tuple(number(sample, f"{track}: a sample") for sample in second.samples)
        if not all(math.isfinite(sample) for sample in samples):
            raise FeatureInputError(f"{track}: a valid second holds a sample that is not a finite number")
        valid.append(WaveformSecond(second.second, samples, True, 0))
    return WaveformValid(rate, span[0], valid)


def waveform_group(track: str, window: WaveformWindow, cutoff_us: int) -> GroupResult:
    if track not in WAVEFORM_TRACKS:
        raise FeatureInputError(f"unknown track {track}")
    valid = window_seconds(track, window, cutoff_us)
    values: dict[str, FeatureValue] = {}
    for stat in WAVEFORM_TRACKS[track]:
        name = f"{slug(track)}__{stat.key}"
        try:
            value = stat.compute(valid) if valid.seconds or not stat.needs_value else None
        except OverflowError as error:
            raise FeatureInputError(f"{name}: the result is outside the range of a float") from error
        if value is not None and not math.isfinite(value):
            raise FeatureInputError(f"{name}: the result is outside the range of a float")
        values[name] = value
    return GroupResult(values, last_sample_us(valid.seconds[-1].second, valid.sample_rate) if valid.seconds else None)
