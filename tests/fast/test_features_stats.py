"""The standard deviation is the float nearest to the exact one.

Failure modes covered here:

- The exact variance is rounded through an approximate square root, so a result that lies just above the midpoint of two floats is rounded down one step.
- The square root of a variance that a float cannot hold overflows or underflows on the way.

No real input of ``sample_std`` is known to reach the rationals just above a midpoint, so the first test gives ``square_root`` such rationals directly (the
isolation is needed: a real window cannot place its variance there). The second compares ``square_root`` and ``sample_std`` with a reference built from
``math.isqrt`` on a scaled integer, over inputs of a seeded generator.
"""

from __future__ import annotations

import math
import unittest
from fractions import Fraction

from features.stats import common_denominator, sample_std, square_root


def reference(value: Fraction) -> float:
    """The float nearest to the square root, from the integer root of the value scaled by 4 ** 1200 (right unless a midpoint is within 2 ** -1200)."""
    scale = 1200
    return float(Fraction(math.isqrt(value.numerator * 4**scale // value.denominator), 2**scale))


class Generator:
    """A small seeded generator, so that the inputs are the same on every run."""

    def __init__(self, seed: int) -> None:
        self.state: int = seed

    def next(self, bits: int) -> int:
        result: int = 0
        for _ in range((bits + 63) // 64):
            self.state = (self.state * 6364136223846793005 + 1442695040888963407) % 2**64
            result = (result << 64) | self.state
        return result % (1 << bits)


class NearestFloat(unittest.TestCase):
    def test_a_rational_just_above_the_squared_midpoint_of_two_floats_gives_the_upper_float_and_one_just_below_gives_the_lower(self) -> None:
        for lower in (1.0, 1.5, 3.0, 0.1, 123456.789, 1e-5, 1e300, 2.0**-1000):
            upper = math.nextafter(lower, math.inf)
            midpoint = (Fraction(lower) + Fraction(upper)) / 2
            for power in range(60, 200, 7):
                squared = midpoint * midpoint
                shift = max(0, 200 - (squared.numerator.bit_length() - squared.denominator.bit_length()))  # enough bits for tiny floats too
                denominator = 3**power << shift
                scaled = squared * denominator
                above = Fraction(math.floor(scaled) + 1, denominator)
                below = Fraction(math.ceil(scaled) - 1, denominator)
                with self.subTest(lower=lower, power=power):
                    self.assertEqual(upper, square_root(above))
                    self.assertEqual(lower, square_root(below))

    def test_a_value_exactly_at_the_squared_midpoint_of_two_floats_rounds_to_the_even_one(self) -> None:
        for lower in (1.0, 1.0 + 2**-52, 3.0, 0.1, 1e300):
            upper = math.nextafter(lower, math.inf)
            midpoint = (Fraction(lower) + Fraction(upper)) / 2
            with self.subTest(lower=lower):
                self.assertEqual(float(midpoint), square_root(midpoint * midpoint))

    def test_the_square_root_of_a_perfect_square_and_of_zero_is_exact(self) -> None:
        self.assertEqual(0.0, square_root(Fraction(0)))
        self.assertEqual(3.0, square_root(Fraction(9)))
        self.assertEqual(0.5, square_root(Fraction(1, 4)))
        self.assertEqual(1.0 + 2**-52, square_root(Fraction(1 + 2**-52) ** 2))

    def test_the_square_root_agrees_with_the_integer_reference_over_seeded_rationals(self) -> None:
        generator = Generator(20261122)
        for index in range(400):
            numerator = 1 + generator.next(1 + generator.next(8) % 300)
            denominator = 1 + generator.next(1 + generator.next(8) % 300)
            value = Fraction(numerator, denominator)
            with self.subTest(index=index):
                self.assertEqual(reference(value), square_root(value))

    def test_the_standard_deviation_agrees_with_the_integer_reference_over_seeded_data(self) -> None:
        generator = Generator(7)
        for index in range(300):
            count = 2 + generator.next(3)
            values = [math.ldexp(generator.next(53) / 2**53 - 0.5, generator.next(6) - 32) for _ in range(count)]
            numerators, denominator = common_denominator(values)
            total = sum(numerators)
            variance = Fraction(sum((count * numerator - total) ** 2 for numerator in numerators), (count * denominator) ** 2 * (count - 1))
            with self.subTest(index=index):
                self.assertEqual(reference(variance), sample_std(values))


if __name__ == "__main__":
    unittest.main()
