"""The waveform second: which samples of a track belong to which second of the case.

A message holds the samples of one waveform track for one second. A sample's time in microseconds is its record's start plus its index over the track's sample
rate, as an exact fraction, and its second is the floor of that time over one second. The sample rate is a float of the file, and its exact value is a ratio
of two integers, so no rounding of a float decides a boundary: a sample on a second boundary opens the next second. A second with no sample has no message.
A second holds its samples in the order of the records and the indices. Its start offset is the time of its first sample after the start of the second.
This module cuts each record at the seconds with integer arithmetic. The manifest derives the same seconds sample by sample with its own code.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from replay.events.errors import EventModelError
from replay.events.recording import WaveformTrack

MICROSECONDS = 1_000_000


@dataclass(frozen=True, slots=True)
class SecondSpan:
    """One second of a waveform: its index, the offset of its first sample and the pieces of records that hold its samples as ``(record, first, end)``."""

    second: int
    start_offset_us: int
    chunks: tuple[tuple[int, int, int], ...]


def rate_ratio(sample_rate: float) -> tuple[int, int]:
    """The exact value of a sample rate as ``(numerator, denominator)``. A rate that is not a positive finite number is refused."""
    if not math.isfinite(sample_rate) or sample_rate <= 0:
        raise EventModelError("a waveform's sample rate must be a positive finite number")
    return sample_rate.as_integer_ratio()


def first_index_at_or_after(instant_us: int, start_us: int, numerator: int, step: int) -> int:
    """The index of the first sample of a record whose time is at or after ``instant_us``. It can be negative or past the record's end."""
    return -((-(instant_us - start_us) * numerator) // step)


def second_spans(track: WaveformTrack) -> list[SecondSpan]:
    numerator, denominator = rate_ratio(track.sample_rate)
    step = denominator * MICROSECONDS
    pieces: dict[int, list[tuple[int, int, int]]] = {}
    offsets: dict[int, int] = {}
    for index, record in enumerate(track.records):
        count = len(record.samples)
        if count == 0:
            continue
        first_second = (record.start_us * numerator) // (numerator * MICROSECONDS)
        last_second = (record.start_us * numerator + (count - 1) * step) // (numerator * MICROSECONDS)
        for second in range(first_second, last_second + 1):
            low = max(0, first_index_at_or_after(second * MICROSECONDS, record.start_us, numerator, step))
            high = min(count, first_index_at_or_after((second + 1) * MICROSECONDS, record.start_us, numerator, step))
            if low >= high:
                continue
            if second not in pieces:
                offsets[second] = (record.start_us * numerator + low * step) // numerator - second * MICROSECONDS
            pieces.setdefault(second, []).append((index, low, high))
    return [SecondSpan(second, offsets[second], tuple(pieces[second])) for second in sorted(pieces)]


def second_samples(track: WaveformTrack, span: SecondSpan) -> list[float]:
    """The raw samples of a second, as floats, in order."""
    return [float(value) for record, first, end in span.chunks for value in track.records[record].samples[first:end]]
