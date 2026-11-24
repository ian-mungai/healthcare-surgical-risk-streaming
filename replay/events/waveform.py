"""The waveform second: which samples of a track belong to which second of the case.

A message holds the samples of one waveform track for one second. A sample's time is its packet's encoded start plus its index over the track's sample rate,
each at its exact value, and its second is the floor of that time since the anonymised start. ``features.sampling.packet_pieces`` decides it, the one function
that Bronze calls too, so the stream reads the seconds that the batch path reads from the same packets. A sample on a second boundary opens the next second.
A second with no sample has no message. A second holds its samples in the order of the packets and the indices. Its start offset is the time of its first
sample after the start of the second. The manifest derives the same seconds sample by sample with its own code.
"""

from __future__ import annotations

from dataclasses import dataclass

from features.sampling import ANONYMISED_START, SamplingError, packet_pieces
from replay.events.errors import EventModelError
from replay.events.recording import WaveformTrack

MICROSECONDS = 1_000_000


@dataclass(frozen=True, slots=True)
class SecondSpan:
    """One second of a waveform: its index, the offset of its first sample and the pieces of records that hold its samples as ``(record, first, end)``."""

    second: int
    start_offset_us: int
    chunks: tuple[tuple[int, int, int], ...]


def second_spans(track: WaveformTrack) -> list[SecondSpan]:
    pieces: dict[int, list[tuple[int, int, int]]] = {}
    offsets: dict[int, int] = {}
    for index, record in enumerate(track.records):
        try:
            cut = packet_pieces(record.start_s, len(record.samples), track.sample_rate, ANONYMISED_START)
        except SamplingError as error:
            raise EventModelError(f"the waveform {track.name} cannot be cut into seconds: {error}") from None
        for piece in cut:
            offsets.setdefault(piece.second, piece.offset_us)
            pieces.setdefault(piece.second, []).append((index, piece.first, piece.end))
    return [SecondSpan(second, offsets[second], tuple(pieces[second])) for second in sorted(pieces)]


def second_samples(track: WaveformTrack, span: SecondSpan) -> list[float]:
    """The raw samples of a second, as floats, in order."""
    return [float(value) for record, first, end in span.chunks for value in track.records[record].samples[first:end]]
