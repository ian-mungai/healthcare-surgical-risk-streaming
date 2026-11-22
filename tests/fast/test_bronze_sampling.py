"""The second of each waveform sample, from the encoded values with no rounding on the way.

A packet's start time is a float of seconds since the epoch (the file's own value) and a track's rate is a float. The time of sample ``i`` is the exact value
of the start plus ``i`` over the exact value of the rate, and its second is the floor of that time minus the anonymised start. Rounding the start to whole
microseconds first moves a sample that lies less than half a microsecond before a boundary into the next second: the published recordings of cases 565 and 5754
hold such samples, one for each of the ECG and pleth tracks.

Failure modes covered here:

- The start is rounded to microseconds (or any other way) before the samples are placed, so a sample just before a boundary lands in the next second.
- A sample on a boundary lands in the second before it, or the first index of a second is off by one.
- A packet that spans two or three seconds loses or repeats a sample, or two pieces of one packet overlap.
- A rate that is not a whole number (62.5, 0.3, 7) is read through a rounded value.
- The offset of a second's first sample is rounded up to a whole second or is not the exact time floored once.
- A rate below one sample a second, a start before the origin or a bad rate gives a wrong second or loops without end.

Each case is also compared with a sample-by-sample derivation that shares no step with the function. The tables and the independent read of the published
files are proved by ``tests/e2e/bronze_decode.py``.
"""

from __future__ import annotations

import itertools
import math
import unittest
from fractions import Fraction

from spark.bronze.sampling import SamplingError, SecondPiece, packet_pieces

ORIGIN = 4102444800.0
MICROSECONDS = 1_000_000


def by_sample(start: float, count: int, rate: float) -> dict[int, tuple[list[int], int]]:
    """The seconds of every sample, one sample at a time with exact fractions: each second's sample indexes and the floored microsecond offset of the first."""
    seconds: dict[int, tuple[list[int], int]] = {}
    for index in range(count):
        instant = Fraction(start) - Fraction(ORIGIN) + Fraction(index) / Fraction(rate)
        second = math.floor(instant)
        if second not in seconds:
            seconds[second] = ([], math.floor((instant - second) * MICROSECONDS))
        seconds[second][0].append(index)
    return seconds


def expanded(pieces: list[SecondPiece]) -> dict[int, tuple[list[int], int]]:
    return {piece.second: (list(range(piece.first, piece.end)), piece.offset_us) for piece in pieces}


class Boundaries(unittest.TestCase):
    def test_a_sample_exactly_on_a_second_opens_that_second(self) -> None:
        pieces = packet_pieces(ORIGIN + 2.0, 5, 4.0, ORIGIN)
        self.assertEqual([SecondPiece(2, 0, 4, 0), SecondPiece(3, 4, 5, 0)], pieces)

    def test_the_float_just_before_a_boundary_is_in_the_second_before(self) -> None:
        start = math.nextafter(ORIGIN + 3.0, 0.0)
        pieces = packet_pieces(start, 3, 500.0, ORIGIN)
        self.assertEqual([(2, 0, 1, 999_999), (3, 1, 3, 1999)], [(p.second, p.first, p.end, p.offset_us) for p in pieces])

    def test_the_float_just_after_a_boundary_is_in_that_second(self) -> None:
        start = math.nextafter(ORIGIN + 3.0, math.inf)
        pieces = packet_pieces(start, 3, 500.0, ORIGIN)
        self.assertEqual([(3, 0, 3, 0)], [(p.second, p.first, p.end, p.offset_us) for p in pieces])

    def test_a_start_that_rounds_up_to_a_boundary_stays_in_the_second_before_it(self) -> None:
        start = ORIGIN + 0.9999996
        self.assertEqual(1_000_000, round((start - ORIGIN) * MICROSECONDS), "the start rounds up to a whole second")
        pieces = packet_pieces(start, 4, 500.0, ORIGIN)
        self.assertEqual([(0, 0, 1), (1, 1, 4)], [(p.second, p.first, p.end) for p in pieces])
        self.assertEqual(999_999, pieces[0].offset_us)

    def test_the_counts_of_two_adjacent_seconds_follow_the_encoded_times(self) -> None:
        start = ORIGIN + 0.9999996
        counts = [piece.end - piece.first for piece in packet_pieces(start, 1000, 500.0, ORIGIN)]
        self.assertEqual([1, 500, 499], counts)

    def test_a_packet_that_spans_two_and_three_seconds_is_cut_without_loss(self) -> None:
        two = packet_pieces(ORIGIN + 0.5, 4, 2.0, ORIGIN)
        self.assertEqual([(0, 0, 1), (1, 1, 3), (2, 3, 4)], [(p.second, p.first, p.end) for p in two])
        three = packet_pieces(ORIGIN + 0.5, 9, 4.0, ORIGIN)
        self.assertEqual([(0, 0, 2, 500_000), (1, 2, 6, 0), (2, 6, 9, 0)], [(p.second, p.first, p.end, p.offset_us) for p in three])

    def test_a_packet_inside_one_second_is_one_piece(self) -> None:
        self.assertEqual([SecondPiece(7, 0, 3, 250_000)], packet_pieces(ORIGIN + 7.25, 3, 4.0, ORIGIN))

    def test_a_start_before_the_origin_has_negative_seconds(self) -> None:
        pieces = packet_pieces(ORIGIN - 0.25, 3, 4.0, ORIGIN)
        self.assertEqual([(-1, 0, 1, 750_000), (0, 1, 3, 0)], [(p.second, p.first, p.end, p.offset_us) for p in pieces])

    def test_the_offset_is_the_exact_time_floored_once_and_stays_below_a_second(self) -> None:
        pieces = packet_pieces(math.nextafter(ORIGIN + 3.0, 0.0), 1, 500.0, ORIGIN)
        self.assertEqual(999_999, pieces[0].offset_us)
        for piece in packet_pieces(ORIGIN + 0.1, 5000, 500.0, ORIGIN):
            self.assertTrue(0 <= piece.offset_us < MICROSECONDS)


class Rates(unittest.TestCase):
    def test_a_rate_that_is_not_a_whole_number_is_used_at_its_exact_value(self) -> None:
        for rate in (62.5, 0.3, 7.0, 1.0 / 3.0, 512.0 / 7.0):
            for start in (ORIGIN + 0.1, ORIGIN + 12.0, math.nextafter(ORIGIN + 12.0, 0.0)):
                self.assertEqual(by_sample(start, 400, rate), expanded(packet_pieces(start, 400, rate, ORIGIN)), (rate, start))

    def test_a_rate_below_one_sample_a_second_gives_each_sample_its_own_second(self) -> None:
        pieces = packet_pieces(ORIGIN + 1.5, 4, 0.5, ORIGIN)
        self.assertEqual(
            [(1, 0, 1, 500_000), (3, 1, 2, 500_000), (5, 2, 3, 500_000), (7, 3, 4, 500_000)], [(p.second, p.first, p.end, p.offset_us) for p in pieces]
        )

    def test_a_rate_that_is_not_a_positive_number_or_a_start_that_is_not_finite_is_refused(self) -> None:
        for rate in (0.0, -1.0, math.nan, math.inf):
            with self.assertRaises(SamplingError):
                packet_pieces(ORIGIN, 3, rate, ORIGIN)
        for start in (math.nan, math.inf, -math.inf):
            with self.assertRaises(SamplingError):
                packet_pieces(start, 3, 4.0, ORIGIN)

    def test_a_packet_with_no_sample_has_no_piece(self) -> None:
        self.assertEqual([], packet_pieces(ORIGIN, 0, 500.0, ORIGIN))


class AgainstTheSampleBySample(unittest.TestCase):
    def test_a_grid_of_packets_agrees_with_the_derivation_that_shares_no_step(self) -> None:
        rates = (500.0, 250.0, 125.0, 62.5, 100.0, 7.0, 0.3, 1.0, 2.5)
        steps = (lambda value: value, lambda value: math.nextafter(value, 0.0), lambda value: math.nextafter(value, math.inf))
        checked = 0
        for rate, second, part, step, count in itertools.product(rates, (0, 7, 39_999), (0.0, 0.25, 0.9999996, 0.0000004, 0.123456), steps, (1, 9, 1100)):
            start = step(ORIGIN + second + part)
            pieces = packet_pieces(start, count, rate, ORIGIN)
            self.assertEqual(by_sample(start, count, rate), expanded(pieces), (start, count, rate))
            self.assertEqual(count, sum(piece.end - piece.first for piece in pieces))
            self.assertEqual([piece.first for piece in pieces[1:]], [piece.end for piece in pieces[:-1]])
            checked += 1
        self.assertEqual(9 * 3 * 5 * 3 * 3, checked)


if __name__ == "__main__":
    unittest.main()
