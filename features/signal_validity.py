"""The one definition of signal validity and of source deduplication, used by the batch path (Silver) and by the stream path alike.

Silver marks each reading and each waveform second valid or invalid with a cause, and the feature code only counts the flag (``features.model``). Both paths
call the functions of this module, so the same readings give the same flags at the same cutoff. The module uses the standard library only and imports no Spark.

**Sources.** Every limit below is fixed before any profiling. Row 4 of the plan ("Analysis decisions and their evidence" in the project standard) gives,
for the arterial pressure: a second is invalid when it is missing, when a sample is outside 20 to 200 mmHg, when the range of the segment is under 20
mmHg ("a range under 20 mmHg within the segment that ends at the second under test") or when a sample jumps by more than 30 mmHg from the reading before
it. The owner chose the segment (Q50 "B", Nov 23 2026; Choe 2021 and the 20-second waveform window): the 20 seconds that end with the second under test.
Each test uses the readings that end at the second under test, so only what is known at the cutoff decides. The plan names the arterial pressure only,
so the range, flat and jump rules and the pressure limits apply to ``PRESSURE_WAVEFORMS`` and to the tracks of ``NUMERIC_LIMITS_MMHG``. Any other track
is judged on its structure only. No rule here is estimated from data.

**Causes**, in the order that decides between them when several hold (``INVALID_CAUSES``):

- ``missing``: no sample, or a sample that is null, not finite or above ``MISSING_SENTINEL`` (the publisher's library turns both into a missing sample,
  ``vitaldb`` 1.7.2 ``VitalFile.to_numpy``). A numeric reading is missing when it is null or not finite.
- ``incomplete``: a second that does not hold exactly one sample for each hertz of a whole-number rate, or whose first sample is not on the second boundary.
  The feature code reads a valid second as a whole-second block of exactly ``rate`` samples, so a second of any other form is never valid.
- ``out_of_range``: a pressure sample outside 20 to 200 mmHg, or a reading of a limited numeric track outside its limits.
- ``flat``: the range of the valid samples of the 20 seconds that end with the second is under 20 mmHg. A valid sample is inside 20 to 200 mmHg. The
  segment counts only when each of its 20 seconds is present and complete (neither ``missing`` nor ``incomplete``): a segment with a second that is
  absent, short, overfull, late or holding a missing sample is not complete, as at the start of a recording or after a gap, and its second is not flat,
  because only what is known at the cutoff decides. The caller gives the extent (complete seconds, least and most) of the 19 seconds before the second;
  ``second_extent`` makes the extent of one second and ``combine_extents`` joins extents.
- ``jump``: a sample that differs from the sample before it by more than 30 mmHg. The sample before the first one of a second is the last sample of the
  second before it, when that second exists and its last sample is not missing.

A limit is part of the valid range. A comparison with a limit uses the exact values of the floats: a difference that a float rounds onto its limit is decided
by the exact difference.

**Samples.** A waveform sample of an integer format is a raw count, and its physical value is ``raw * gain + offset``, except the format's gap marker, which
is a missing sample and is recognised on the raw value, before the calibration (``GAP_MARKERS``). A sample of a float format is already physical (``vitaldb``
1.7.2, ``VitalFile.to_numpy``). Bronze holds the raw samples and the track's gain, offset and format code.

**Duplicates.** A second reading of one track at one case time is a duplicate. The first reading in the file's order is the reading, because a feature at a
cutoff must not change when a later duplicate arrives. The position of a reading among those of its track and time is Bronze's ``source_occurrence`` (the
replay's event discriminator), so the first is ``FIRST_OCCURRENCE`` and each other is a duplicate. Silver keeps the dropped positions in its ledger.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from fractions import Fraction

MISSING = "missing"
INCOMPLETE = "incomplete"
OUT_OF_RANGE = "out_of_range"
FLAT = "flat"
JUMP = "jump"
INVALID_CAUSES = (MISSING, INCOMPLETE, OUT_OF_RANGE, FLAT, JUMP)

ARTERIAL_WAVEFORM = "SNUADC/ART"
PRESSURE_WAVEFORMS = frozenset({ARTERIAL_WAVEFORM})
PRESSURE_LOW_MMHG = 20.0
PRESSURE_HIGH_MMHG = 200.0
MIN_SEGMENT_RANGE_MMHG = 20.0
MAX_JUMP_MMHG = 30.0
NUMERIC_LIMITS_MMHG: dict[str, tuple[float, float]] = {"Solar8000/ART_MBP": (PRESSURE_LOW_MMHG, PRESSURE_HIGH_MMHG)}
MISSING_SENTINEL = 4e9
FLOAT_FORMATS = (1, 2)
FLAT_WINDOW_SECONDS = 20
# The raw sample that the publisher's library writes into a gap of each integer format (``vitaldb`` 1.7.2 ``FMT_NAN``, used by
# ``_pack_wav_recs``). A float format uses NaN.
GAP_MARKERS: dict[int, float] = {3: -128.0, 4: 255.0, 5: -32768.0, 6: 65535.0, 7: -2147483648.0, 8: 4294967295.0}
FIRST_OCCURRENCE = 0


@dataclass(frozen=True, slots=True)
class Verdict:
    """Whether a reading or a second is valid, and the first cause of ``INVALID_CAUSES`` that holds when it is not."""

    valid: bool
    cause: str | None


VALID = Verdict(True, None)


@dataclass(frozen=True, slots=True)
class Extent:
    """Some seconds of a waveform: how many are complete, and the least and the most of their valid samples (``None`` for none)."""

    seconds: int
    low: float | None
    high: float | None


EMPTY_EXTENT = Extent(0, None, None)


def is_duplicate(source_occurrence: int) -> bool:
    return source_occurrence != FIRST_OCCURRENCE


def difference_below(high: float, low: float, limit: float) -> bool:
    """Whether ``high - low`` is below ``limit``, by the exact values of the floats. A float difference away from the limit already decides it."""
    difference = high - low
    if difference != limit:
        return difference < limit
    return Fraction(high) - Fraction(low) < Fraction(limit)


def difference_above(first: float, second: float, limit: float) -> bool:
    """Whether ``|first - second|`` is above ``limit``, by the exact values of the floats."""
    difference = abs(first - second)
    if difference != limit:
        return difference > limit
    return abs(Fraction(first) - Fraction(second)) > Fraction(limit)


def present(sample: float | None) -> bool:
    return sample is not None and math.isfinite(sample) and sample <= MISSING_SENTINEL


def physical_samples(raw: Sequence[float | None], sample_format: int, gain: float, offset: float) -> list[float | None]:
    """The physical samples of a waveform second from its raw samples. A missing sample stays missing and is never filled.

    The gap marker of an integer format is a missing sample and is recognised on the raw value, before the calibration turns it into a plausible pressure.
    """
    if sample_format in FLOAT_FORMATS:
        return [None if sample is None else float(sample) for sample in raw]
    marker = GAP_MARKERS.get(sample_format)
    return [None if sample is None or sample == marker else float(sample) * gain + offset for sample in raw]


def second_extent(samples: Sequence[float | None], sample_rate: float, start_offset_us: int) -> Extent:
    """The extent of one second: nothing when it is missing or not complete, else one complete second with the least and the most of its samples inside the
    pressure limits."""
    if is_missing(samples) or is_incomplete(samples, sample_rate, start_offset_us):
        return EMPTY_EXTENT
    values = [float(sample) for sample in samples if sample is not None and PRESSURE_LOW_MMHG <= sample <= PRESSURE_HIGH_MMHG]
    return Extent(1, min(values), max(values)) if values else Extent(1, None, None)


def combine_extents(extents: Iterable[Extent]) -> Extent:
    """The extent of the seconds that the given extents stand for."""
    held = [extent for extent in extents if extent.seconds]
    if not held:
        return EMPTY_EXTENT
    lows = [extent.low for extent in held if extent.low is not None]
    highs = [extent.high for extent in held if extent.high is not None]
    return Extent(sum(extent.seconds for extent in held), min(lows) if lows else None, max(highs) if highs else None)


def numeric_verdict(track: str, value: float | None) -> Verdict:
    if value is None or not math.isfinite(value):
        return Verdict(False, MISSING)
    limits = NUMERIC_LIMITS_MMHG.get(track)
    if limits is not None and not limits[0] <= value <= limits[1]:
        return Verdict(False, OUT_OF_RANGE)
    return VALID


def whole_rate(sample_rate: float) -> int | None:
    """The sample rate as a positive whole number, or ``None``."""
    if not math.isfinite(sample_rate) or sample_rate <= 0 or sample_rate != int(sample_rate):
        return None
    return int(sample_rate)


def is_missing(samples: Sequence[float | None]) -> bool:
    return not samples or not all(present(sample) for sample in samples)


def is_incomplete(samples: Sequence[float | None], sample_rate: float, start_offset_us: int) -> bool:
    """Whether a second does not hold exactly one sample for each hertz of a whole-number rate, starting on the second boundary."""
    rate = whole_rate(sample_rate)
    return rate is None or len(samples) != rate or start_offset_us != 0


def waveform_verdict(
    track: str, samples: Sequence[float | None], sample_rate: float, start_offset_us: int, previous_last: float | None, prior: Extent = EMPTY_EXTENT
) -> Verdict:
    """The validity of the second that holds ``samples`` (physical values), on a track of ``sample_rate`` hertz.

    ``start_offset_us`` is the time of the first sample after the start of the second. ``previous_last`` is the last physical sample of the second before this
    one, or ``None`` when there is none. A sample that is not ``present`` there is no reading to compare with. ``prior`` is the extent of the 19 seconds before
    this one that Silver holds.
    """
    if is_missing(samples):
        return Verdict(False, MISSING)
    if is_incomplete(samples, sample_rate, start_offset_us):
        return Verdict(False, INCOMPLETE)
    if track not in PRESSURE_WAVEFORMS:
        return VALID
    values = [float(sample) for sample in samples if sample is not None]
    if min(values) < PRESSURE_LOW_MMHG or max(values) > PRESSURE_HIGH_MMHG:
        return Verdict(False, OUT_OF_RANGE)
    segment = combine_extents([prior, second_extent(samples, sample_rate, start_offset_us)])
    complete = segment.low is not None and segment.high is not None and segment.seconds >= FLAT_WINDOW_SECONDS
    if complete and difference_below(float(segment.high or 0.0), float(segment.low or 0.0), MIN_SEGMENT_RANGE_MMHG):
        return Verdict(False, FLAT)
    before = [previous_last] if previous_last is not None and present(previous_last) else []
    sequence = [*before, *values]
    if any(difference_above(newer, older, MAX_JUMP_MMHG) for older, newer in zip(sequence, sequence[1:], strict=False)):
        return Verdict(False, JUMP)
    return VALID
