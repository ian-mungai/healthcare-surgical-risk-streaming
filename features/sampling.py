"""The one definition of the second of each waveform sample, from the encoded values with no rounding on the way. Pure code: no library, no file.

Bronze (``spark/bronze/rows.py``) and the replay's event model (``replay/events/waveform.py``, so the stream) call ``packet_pieces`` and nothing else to cut a
packet into seconds, so the same packet gives the same seconds, the same offsets and the same samples on both paths. The replay's manifest derives the seconds
again sample by sample with code that shares no step with this module (``replay/events/manifest.py``).

A packet's start time is the file's own float of seconds since the epoch and a track's rate is the file's own float. Each is taken at its exact value, as a
``Fraction``. The time of sample ``i`` is the start, minus the anonymised start, plus ``i`` over the rate. Its second is the floor of that time. The offset of
the first sample of a piece in its second is the exact time within the second, floored once to whole microseconds, so it is always below one second.
Nothing is rounded before that: rounding a packet's start to whole microseconds first would move a sample that lies less than half a microsecond before
a boundary into the next second.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from fractions import Fraction

MICROSECONDS = 1_000_000
ANONYMISED_START = 4102444800.0


class SamplingError(ValueError):
    """A start or a rate that cannot place samples in seconds."""


@dataclass(frozen=True, slots=True)
class SecondPiece:
    """The samples of one packet that fall in one second: the second, the index of the first sample and the end index (exclusive), and the microsecond offset
    of the first sample within its second."""

    second: int
    first: int
    end: int
    offset_us: int


def packet_pieces(start: float, count: int, rate: float, origin: float) -> list[SecondPiece]:
    """The pieces of a packet of ``count`` samples from ``start`` seconds since the epoch, in the order of their seconds. ``origin`` is the anonymised start."""
    if count <= 0:
        return []
    if not (math.isfinite(start) and math.isfinite(origin) and math.isfinite(rate) and rate > 0):
        raise SamplingError("a packet needs a finite start and a sample rate that is a positive number")
    begin = Fraction(start) - Fraction(origin)
    per_second = Fraction(rate)

    def offset(index: int, second: int) -> int:
        return math.floor((begin + Fraction(index) / per_second - second) * MICROSECONDS)

    if per_second < 1:
        return [SecondPiece(second := math.floor(begin + Fraction(index) / per_second), index, index + 1, offset(index, second)) for index in range(count)]
    pieces = []
    for second in range(math.floor(begin), math.floor(begin + Fraction(count - 1) / per_second) + 1):
        low = max(0, math.ceil((second - begin) * per_second))
        high = min(count, math.ceil((second + 1 - begin) * per_second))
        if low < high:
            pieces.append(SecondPiece(second, low, high, offset(low, second)))
    return pieces
