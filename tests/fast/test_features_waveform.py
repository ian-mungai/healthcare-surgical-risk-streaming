"""The waveform features of one track over the 20-second window that ends at the cutoff.

Failure modes covered here:

- A feature's formula is wrong: each value of a hand-built window is compared with its known answer.
- The window edge moves by one second, or a second that is not complete at the cutoff is used.
- An invalid second is counted in a value, in the valid seconds or in the coverage share, or a missing second is not counted as invalid.
- A window with few valid seconds is dropped as a whole, or a statistic that cannot be computed (no valid second, one second for a spread) returns a number.
- The dominant frequency leaves the band of 0.5 to 3.5 Hz, takes the wrong bin, or is moved by the gap where an invalid second was.
- A flat signal reports a frequency.
- The result depends on the order of the seconds.
"""

from __future__ import annotations

import math
import statistics
import unittest
from collections.abc import Callable
from typing import Any

from features.model import FeatureInputError, FeatureValue, WaveformSecond, WaveformWindow
from features.waveform import waveform_group
from tests.fast.features_support import CUTOFF_US, RATE_HZ, SECOND, alternating, second_of, sine, waveform_window, window_seconds

ART, ECG, PLETH = "SNUADC/ART", "SNUADC/ECG_II", "SNUADC/PLETH"
STD_OF_ALTERNATING = 12.092088031635567
AMPLITUDE_STD = math.sqrt(2000 / 19)


def values_of(track: str, window: WaveformWindow, cutoff_us: int = CUTOFF_US) -> dict[str, FeatureValue]:
    return waveform_group(track, window, cutoff_us).values


def get(result: dict[str, FeatureValue], key: str) -> float:
    value = result[key]
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise AssertionError(f"{key} is not a number: {value!r}")
    return value


class Arterial(unittest.TestCase):
    def test_the_hand_built_window_gives_every_value_its_known_answer(self) -> None:
        result = values_of(ART, waveform_window(alternating))
        self.assertAlmostEqual(97.75, get(result, "snuadc_art__mean"), places=9)
        self.assertAlmostEqual(STD_OF_ALTERNATING, get(result, "snuadc_art__std"), places=9)
        self.assertAlmostEqual(40.0, get(result, "snuadc_art__pulse_pressure_mean"), places=9)
        self.assertEqual(20, result["snuadc_art__valid_seconds"])
        self.assertEqual(1.0, result["snuadc_art__coverage"])
        self.assertEqual(
            {"snuadc_art__" + key for key in ("mean", "std", "pulse_pressure_mean", "dominant_freq_hz", "valid_seconds", "coverage")},
            set(result),
        )

    def test_the_dominant_frequency_of_a_sine_is_its_frequency(self) -> None:
        for frequency in (0.5, 1.2, 2.0, 3.5):
            with self.subTest(frequency=frequency):
                result = values_of(ART, waveform_window(sine(frequency, 90.0, 20.0)))
                self.assertAlmostEqual(frequency, get(result, "snuadc_art__dominant_freq_hz"), places=9)

    def test_a_stronger_component_outside_the_band_is_ignored(self) -> None:
        low, high = sine(0.3, 0.0, 30.0), sine(1.0, 0.0, 5.0)
        window = waveform_window(lambda position: [a + b for a, b in zip(low(position), high(position), strict=True)])
        self.assertAlmostEqual(1.0, get(values_of(ART, window), "snuadc_art__dominant_freq_hz"), places=9)
        above, mid = sine(4.0, 0.0, 30.0), sine(3.0, 0.0, 5.0)
        window = waveform_window(lambda position: [a + b for a, b in zip(above(position), mid(position), strict=True)])
        self.assertAlmostEqual(3.0, get(values_of(ART, window), "snuadc_art__dominant_freq_hz"), places=9)

    def test_a_flat_signal_has_no_dominant_frequency(self) -> None:
        result = values_of(ART, waveform_window(lambda position: [90.0] * 10))
        self.assertIsNone(result["snuadc_art__dominant_freq_hz"])
        self.assertEqual(90.0, result["snuadc_art__mean"])
        self.assertEqual(0.0, result["snuadc_art__std"])
        self.assertEqual(0.0, result["snuadc_art__pulse_pressure_mean"])


class OtherWaveforms(unittest.TestCase):
    def test_the_ecg_has_the_standard_deviation_and_the_frequency(self) -> None:
        result = values_of(ECG, waveform_window(alternating))
        self.assertEqual({"snuadc_ecg_ii__" + key for key in ("std", "dominant_freq_hz", "valid_seconds", "coverage")}, set(result))
        self.assertAlmostEqual(STD_OF_ALTERNATING, get(result, "snuadc_ecg_ii__std"), places=9)
        sine_result = values_of(ECG, waveform_window(sine(1.5, 0.0, 1.0)))
        self.assertAlmostEqual(1.5, get(sine_result, "snuadc_ecg_ii__dominant_freq_hz"), places=9)

    def test_the_plethysmograph_has_the_mean_and_the_spread_of_the_second_amplitudes(self) -> None:
        result = values_of(PLETH, waveform_window(alternating))
        self.assertEqual({"snuadc_pleth__" + key for key in ("amplitude_mean", "amplitude_std", "dominant_freq_hz", "valid_seconds", "coverage")}, set(result))
        self.assertAlmostEqual(40.0, get(result, "snuadc_pleth__amplitude_mean"), places=9)
        self.assertAlmostEqual(AMPLITUDE_STD, get(result, "snuadc_pleth__amplitude_std"), places=9)
        self.assertAlmostEqual(1.2, get(values_of(PLETH, waveform_window(sine(1.2, 0.0, 1.0))), "snuadc_pleth__dominant_freq_hz"), places=9)


class Window(unittest.TestCase):
    def marker_window(self, first: int, last: int) -> WaveformWindow:
        """The seconds ``first`` to ``last``, each holding ten samples equal to its index minus ``first``."""
        return WaveformWindow(RATE_HZ, tuple(second_of(index, [float(index - first)] * 10) for index in range(first, last + 1)))

    def test_the_window_is_the_20_newest_complete_seconds_at_each_cutoff_around_a_second_boundary(self) -> None:
        k = 3600
        window = self.marker_window(k - 30, k + 5)
        cases = ((k * SECOND - 1, k - 2), (k * SECOND, k - 1), (k * SECOND + 1, k - 1), (k * SECOND + 999_999, k - 1), ((k + 1) * SECOND, k))
        for cutoff, newest in cases:
            with self.subTest(cutoff=cutoff):
                group = waveform_group(ART, window, cutoff)
                self.assertEqual(20, group.values["snuadc_art__valid_seconds"])
                self.assertAlmostEqual(newest - 9.5 - (k - 30), get(group.values, "snuadc_art__mean"), places=9)
                self.assertEqual(newest * SECOND + 900_000, group.latest_us)
                self.assertLess(group.latest_us or 0, cutoff)

    def test_a_second_outside_the_window_changes_nothing(self) -> None:
        indexes = window_seconds(20)
        outside = [second_of(indexes[0] - 1, [999.0] * 10), second_of(indexes[-1] + 1, [999.0] * 10), second_of(indexes[-1] + 5, [999.0] * 10)]
        window = waveform_window(alternating)
        padded = WaveformWindow(window.sample_rate, tuple(outside[:1]) + window.seconds + tuple(outside[1:]))
        self.assertEqual(values_of(ART, window), values_of(ART, padded))

    def test_the_first_second_of_the_window_is_used_and_the_one_before_it_is_not(self) -> None:
        indexes = window_seconds(21)
        seconds = tuple(second_of(index, [50.0] * 10 if position == 0 else [100.0] * 10) for position, index in enumerate(indexes))
        result = values_of(ART, WaveformWindow(RATE_HZ, seconds))
        self.assertEqual(100.0, result["snuadc_art__mean"])
        self.assertEqual(20, result["snuadc_art__valid_seconds"])

    def test_the_latest_reading_time_is_the_last_sample_of_the_newest_valid_second_and_never_the_cutoff(self) -> None:
        group = waveform_group(ART, waveform_window(alternating, invalid=frozenset({19})), CUTOFF_US)
        self.assertEqual((CUTOFF_US // SECOND - 2) * SECOND + 900_000, group.latest_us)
        fast = WaveformWindow(500.0, tuple(second_of(index, [1.0, 2.0] * 250) for index in window_seconds(20)))
        self.assertEqual((CUTOFF_US // SECOND - 1) * SECOND + 998_000, waveform_group(ART, fast, CUTOFF_US).latest_us)


class Blocks(unittest.TestCase):
    def window_with(self, position: int, block: WaveformSecond) -> WaveformWindow:
        window = waveform_window(alternating)
        seconds = list(window.seconds)
        seconds[position] = block
        return WaveformWindow(window.sample_rate, tuple(seconds))

    def test_a_valid_second_that_does_not_start_on_a_whole_second_is_refused(self) -> None:
        newest = window_seconds(20)[-1]
        for offset in (1, 500_000, 990_001, 999_999, -1):
            with self.subTest(offset=offset), self.assertRaisesRegex(FeatureInputError, "whole second"):
                values_of(ART, self.window_with(19, WaveformSecond(newest, (1.0,) * 10, True, offset)))

    def test_an_invalid_second_is_not_examined(self) -> None:
        newest = window_seconds(20)[-1]
        result = values_of(ART, self.window_with(19, WaveformSecond(newest, (1.0,) * 3, False, 990_001)))
        self.assertEqual(19, result["snuadc_art__valid_seconds"])

    def test_a_valid_second_whose_sample_count_is_not_the_rate_is_refused(self) -> None:
        newest = window_seconds(20)[-1]
        for count in (0, 9, 11, 20):
            with self.subTest(count=count), self.assertRaisesRegex(FeatureInputError, "sample count"):
                values_of(ART, self.window_with(19, WaveformSecond(newest, (1.0,) * count, True)))

    def test_two_rates_in_one_window_are_refused(self) -> None:
        window = waveform_window(alternating)
        doubled = tuple(
            WaveformSecond(second.second, tuple(second.samples) * 2, True) if position % 2 else second for position, second in enumerate(window.seconds)
        )
        with self.assertRaisesRegex(FeatureInputError, "sample count"):
            values_of(ART, WaveformWindow(window.sample_rate, doubled))

    def test_a_sample_rate_that_is_not_a_positive_whole_number_is_refused(self) -> None:
        window = waveform_window(alternating)
        for rate in (0.0, -10.0, 10.5, 0.5, math.inf, math.nan, True, "10"):
            with self.subTest(rate=rate), self.assertRaisesRegex(FeatureInputError, "sample rate"):
                values_of(ART, WaveformWindow(rate, window.seconds))  # type: ignore[arg-type]

    def test_a_flag_that_is_not_a_boolean_and_an_index_that_is_not_an_integer_are_refused(self) -> None:
        newest = window_seconds(20)[-1]
        bad_flag: Any = "false"
        with self.assertRaisesRegex(FeatureInputError, "valid flag"):
            values_of(ART, self.window_with(19, WaveformSecond(newest, (1.0,) * 10, bad_flag)))
        bad_index: Any = float(newest)
        with self.assertRaisesRegex(FeatureInputError, "second index"):
            values_of(ART, self.window_with(19, WaveformSecond(bad_index, (1.0,) * 10, True)))
        bad_offset: Any = 0.0
        with self.assertRaisesRegex(FeatureInputError, "offset"):
            values_of(ART, self.window_with(19, WaveformSecond(newest, (1.0,) * 10, True, bad_offset)))

    def test_a_valid_sample_that_is_not_a_finite_number_is_refused_with_its_track(self) -> None:
        newest = window_seconds(20)[-1]
        for bad in (math.nan, math.inf, "1.0", None):
            with self.subTest(bad=bad), self.assertRaisesRegex(FeatureInputError, "SNUADC/ART"):
                values_of(ART, self.window_with(19, WaveformSecond(newest, (1.0,) * 9 + (bad,), True)))  # type: ignore[arg-type]


class Validity(unittest.TestCase):
    def test_an_invalid_second_is_in_no_value_and_no_count(self) -> None:
        window = waveform_window(lambda position: [999.0] * 10 if position == 3 else alternating(position), invalid=frozenset({3}))
        result = values_of(ART, window)
        self.assertEqual(19, result["snuadc_art__valid_seconds"])
        self.assertAlmostEqual(19 / 20, get(result, "snuadc_art__coverage"), places=12)
        self.assertAlmostEqual((10 * 955 + 9 * 1000) / 190, get(result, "snuadc_art__mean"), places=9)
        self.assertAlmostEqual((50.0 * 10 + 30.0 * 9) / 19, get(result, "snuadc_art__pulse_pressure_mean"), places=9)

    def test_a_missing_second_counts_as_invalid(self) -> None:
        window = waveform_window(alternating)
        shorter = WaveformWindow(window.sample_rate, window.seconds[:5] + window.seconds[6:])
        self.assertEqual(19, values_of(ART, shorter)["snuadc_art__valid_seconds"])

    def test_a_window_with_few_valid_seconds_still_returns_its_values(self) -> None:
        # Window seconds 11 to 19 are valid: 4 of pattern A (even positions) and 5 of pattern B.
        window = waveform_window(alternating, invalid=frozenset(range(11)))
        result = values_of(ART, window)
        self.assertEqual(9, result["snuadc_art__valid_seconds"])
        self.assertAlmostEqual(9 / 20, get(result, "snuadc_art__coverage"), places=12)
        self.assertAlmostEqual((4 * 955 + 5 * 1000) / 90, get(result, "snuadc_art__mean"), places=9)
        self.assertAlmostEqual((4 * 50 + 5 * 30) / 9, get(result, "snuadc_art__pulse_pressure_mean"), places=9)
        self.assertIsNotNone(values_of(ECG, window)["snuadc_ecg_ii__std"])
        self.assertAlmostEqual((4 * 50 + 5 * 30) / 9, get(values_of(PLETH, window), "snuadc_pleth__amplitude_mean"), places=9)

    def test_one_valid_second_has_no_spread_across_seconds_and_every_other_value(self) -> None:
        window = waveform_window(alternating, invalid=frozenset(range(1, 20)))
        result = values_of(PLETH, window)
        self.assertEqual(1, result["snuadc_pleth__valid_seconds"])
        self.assertEqual(50.0, result["snuadc_pleth__amplitude_mean"])
        self.assertIsNone(result["snuadc_pleth__amplitude_std"])
        art = values_of(ART, window)
        self.assertAlmostEqual(95.5, get(art, "snuadc_art__mean"), places=9)
        self.assertAlmostEqual(14.804278660801636, get(art, "snuadc_art__std"), places=9)

    def test_a_window_with_no_second_is_all_missing(self) -> None:
        result = values_of(ART, WaveformWindow(RATE_HZ, ()))
        self.assertEqual(0, result["snuadc_art__valid_seconds"])
        self.assertEqual(0.0, result["snuadc_art__coverage"])
        self.assertIsNone(result["snuadc_art__mean"])


def flat(level: float) -> Callable[[int], list[float]]:
    return lambda position: [level] * 10


def mixed(low: Callable[[int], list[float]], high: Callable[[int], list[float]], ratio: float) -> Callable[[int], list[float]]:
    return lambda position: [a + ratio * b for a, b in zip(low(position), high(position), strict=True)]


class Frequency(unittest.TestCase):
    def test_one_invalid_second_makes_the_frequency_missing_and_leaves_the_other_values(self) -> None:
        window = waveform_window(sine(1.2, 90.0, 20.0), invalid=frozenset({3}))
        result = values_of(ART, window)
        self.assertIsNone(result["snuadc_art__dominant_freq_hz"])
        self.assertEqual(19, result["snuadc_art__valid_seconds"])
        self.assertIsNotNone(result["snuadc_art__mean"])
        self.assertAlmostEqual(1.2, get(values_of(ART, waveform_window(sine(1.2, 90.0, 20.0))), "snuadc_art__dominant_freq_hz"), places=9)

    def test_a_flat_signal_has_a_standard_deviation_of_exactly_zero_and_no_frequency(self) -> None:
        for level in (0.1, 1 / 3, 1e10 + 0.3, -7.7, 90.0, 1e-300, 1e308):
            for track, prefix in ((ART, "snuadc_art__"), (ECG, "snuadc_ecg_ii__")):
                with self.subTest(level=level, track=track):
                    result = values_of(track, waveform_window(flat(level)))
                    self.assertEqual(0.0, result[prefix + "std"])
                    self.assertIsNone(result[prefix + "dominant_freq_hz"])

    def test_two_frequencies_of_equal_power_give_the_lower_one_even_when_roundoff_favours_the_higher(self) -> None:
        low, high = sine(1.0, 0.0, 10.0), sine(2.0, 0.0, 10.0)
        for ratio in (1.0, 1 + 5e-13, 1 - 5e-13):
            with self.subTest(ratio=ratio):
                window = waveform_window(mixed(low, high, ratio))
                self.assertAlmostEqual(1.0, get(values_of(ART, window), "snuadc_art__dominant_freq_hz"), places=9)

    def test_a_frequency_with_clearly_more_power_wins_over_a_lower_one(self) -> None:
        low, high = sine(1.0, 0.0, 10.0), sine(2.0, 0.0, 10.0)
        for ratio in (1.001, 1 + 5e-7):
            with self.subTest(ratio=ratio):
                window = waveform_window(mixed(low, high, ratio))
                self.assertAlmostEqual(2.0, get(values_of(ART, window), "snuadc_art__dominant_freq_hz"), places=9)


def rate_window(rate: int, signal: Callable[[float], float]) -> WaveformWindow:
    """Twenty complete seconds at ``rate`` samples a second, each sample the signal at its time in seconds from the start of the window."""
    seconds = tuple(second_of(index, [signal(position + k / rate) for k in range(rate)]) for position, index in enumerate(window_seconds(20)))
    return WaveformWindow(float(rate), seconds)


def tone(frequency: float, amplitude: float = 1.0) -> Callable[[float], float]:
    return lambda time: 50.0 + amplitude * math.sin(2 * math.pi * frequency * time)


class Aliasing(unittest.TestCase):
    def test_a_signal_at_the_nyquist_frequency_has_no_frequency_in_the_band(self) -> None:
        # The reviewer's input: 112 and 88 alternating at 100 Hz is a 50 Hz wave. Rounding must not report a peak in the band.
        seconds = tuple(second_of(index, [112.0, 88.0] * 50) for index in window_seconds(20))
        result = values_of(ART, WaveformWindow(100.0, seconds))
        self.assertIsNone(result["snuadc_art__dominant_freq_hz"])
        self.assertEqual(20, result["snuadc_art__valid_seconds"])

    def test_a_frequency_at_or_above_the_nyquist_frequency_is_not_searched(self) -> None:
        # The reviewer's input: a 0.2 Hz cosine at 1 Hz has its Nyquist frequency at 0.5 Hz, so no bin of the band is below it.
        result = values_of(ART, rate_window(1, lambda time: 50.0 + math.cos(2 * math.pi * 0.2 * time)))
        self.assertIsNone(result["snuadc_art__dominant_freq_hz"])

    def test_only_bins_strictly_below_the_nyquist_frequency_and_up_to_3_5_hz_are_searched(self) -> None:
        self.assertAlmostEqual(0.7, get(values_of(ART, rate_window(2, tone(0.7))), "snuadc_art__dominant_freq_hz"), places=9)
        self.assertAlmostEqual(3.45, get(values_of(ART, rate_window(7, tone(3.45))), "snuadc_art__dominant_freq_hz"), places=9)
        self.assertIsNone(
            values_of(ART, rate_window(7, lambda time: 50.0 + math.cos(2 * math.pi * 3.5 * time)))["snuadc_art__dominant_freq_hz"]
        )  # an alternating wave at the Nyquist frequency

    def test_a_clean_sine_gives_its_frequency_at_every_rate(self) -> None:
        for rate in (10, 25, 100):
            with self.subTest(rate=rate):
                self.assertAlmostEqual(1.2, get(values_of(ART, rate_window(rate, tone(1.2))), "snuadc_art__dominant_freq_hz"), places=9)
        self.assertAlmostEqual(3.5, get(values_of(ART, rate_window(100, tone(3.5))), "snuadc_art__dominant_freq_hz"), places=9)

    def test_a_band_with_less_than_1e_minus_9_of_the_power_has_no_frequency(self) -> None:
        def loud_with(amplitude: float) -> Callable[[float], float]:
            return lambda time: 50.0 + 10.0 * math.sin(2 * math.pi * 10.0 * time) + amplitude * math.sin(2 * math.pi * 1.2 * time)

        self.assertIsNone(values_of(ART, rate_window(100, loud_with(1e-6)))["snuadc_art__dominant_freq_hz"])
        self.assertIsNone(values_of(ART, rate_window(100, loud_with(1e-4)))["snuadc_art__dominant_freq_hz"])  # a power ratio of 5e-11 of the total
        self.assertAlmostEqual(1.2, get(values_of(ART, rate_window(100, loud_with(1e-3))), "snuadc_art__dominant_freq_hz"), places=9)


class Exact(unittest.TestCase):
    def test_adjacent_floats_give_the_exact_standard_deviation(self) -> None:
        values = [1.0, 1.0 + 2**-52] * 5
        result = values_of(ART, waveform_window(lambda position: values))
        self.assertTrue(math.isclose(statistics.stdev(values * 20), get(result, "snuadc_art__std"), rel_tol=1e-14))

    def test_the_mean_has_one_rounding_only(self) -> None:
        result = values_of(ART, waveform_window(lambda position: [0.8e308, -0.8e308, 1e-100] + [0.0] * 7))
        self.assertEqual(1e-100 / 10, get(result, "snuadc_art__mean"))

    def test_the_amplitudes_are_exact_differences_even_when_a_float_subtraction_would_round_them_away(self) -> None:
        tiny = 1e-20
        window = waveform_window(lambda position: [1.0, tiny] * 5 if position % 2 == 0 else [1.0, 2 * tiny] * 5)
        result = values_of(PLETH, window)
        self.assertTrue(math.isclose(tiny / 2 * math.sqrt(20 / 19), get(result, "snuadc_pleth__amplitude_std"), rel_tol=1e-12))

    def test_the_amplitudes_are_exact_differences(self) -> None:
        low, high = -1e308, 0.5e308
        result = values_of(PLETH, waveform_window(lambda position: [low, high] * 5))
        self.assertTrue(math.isclose(1.5e308, get(result, "snuadc_pleth__amplitude_mean"), rel_tol=1e-15))
        self.assertEqual(0.0, result["snuadc_pleth__amplitude_std"])


class Range(unittest.TestCase):
    def test_a_sample_too_large_for_a_float_is_refused_with_the_track(self) -> None:
        newest = window_seconds(20)[-1]
        window = waveform_window(alternating)
        bad: Any = 10**400
        seconds = (*window.seconds[:-1], WaveformSecond(newest, (1.0,) * 9 + (bad,), True))
        with self.assertRaisesRegex(FeatureInputError, "SNUADC/ART"):
            values_of(ART, WaveformWindow(window.sample_rate, seconds))

    def test_a_sample_rate_too_large_for_a_float_is_refused(self) -> None:
        with self.assertRaisesRegex(FeatureInputError, "sample rate"):
            values_of(ART, WaveformWindow(10**400, waveform_window(alternating).seconds))

    def test_the_samples_of_an_invalid_second_may_be_anything(self) -> None:
        newest = window_seconds(20)[-1]
        window = waveform_window(alternating)
        reference = values_of(ART, WaveformWindow(window.sample_rate, window.seconds[:-1]))
        for junk in (None, "x", 5, 10**400, [math.nan]):
            with self.subTest(junk=junk):
                bad: Any = junk
                seconds = (*window.seconds[:-1], WaveformSecond(newest, bad, False))
                self.assertEqual(reference, values_of(ART, WaveformWindow(window.sample_rate, seconds)))

    def test_the_samples_of_a_valid_second_must_be_an_array(self) -> None:
        newest = window_seconds(20)[-1]
        window = waveform_window(alternating)
        for junk in (5, "0123456789", {"a": 1}, None):
            with self.subTest(junk=junk), self.assertRaisesRegex(FeatureInputError, "samples"):
                bad: Any = junk
                values_of(ART, WaveformWindow(window.sample_rate, (*window.seconds[:-1], WaveformSecond(newest, bad, True))))

    def test_samples_of_1e200_do_not_overflow_the_standard_deviation(self) -> None:
        result = values_of(ART, waveform_window(lambda position: [1e200, -1e200] * 5))
        self.assertAlmostEqual(0.0, get(result, "snuadc_art__mean"), places=9)
        self.assertTrue(math.isclose(1e200 * math.sqrt(200 / 199), get(result, "snuadc_art__std"), rel_tol=1e-12))

    def test_samples_of_1e308_do_not_overflow_the_mean(self) -> None:
        result = values_of(ART, waveform_window(lambda position: [1.7e308, 1.6e308] * 5))
        self.assertTrue(math.isclose(1.65e308, get(result, "snuadc_art__mean"), rel_tol=1e-12))

    def test_a_result_that_a_float_cannot_hold_is_refused_with_its_name(self) -> None:
        with self.assertRaisesRegex(FeatureInputError, "snuadc_art__pulse_pressure_mean"):
            values_of(ART, waveform_window(lambda position: [1e308, -1e308] * 5))


class Determinism(unittest.TestCase):
    def test_the_order_of_the_seconds_does_not_change_one_bit(self) -> None:
        window = waveform_window(lambda position: [90.0 + 50.0 * math.sin(position * 10 + sample * 1.7) for sample in range(10)])
        seconds = list(window.seconds)
        seconds = seconds[::3] + seconds[1::3] + seconds[2::3]
        seconds.reverse()
        first = values_of(ART, window)
        second = values_of(ART, WaveformWindow(window.sample_rate, tuple(seconds)))
        self.assertEqual({name: repr(value) for name, value in first.items()}, {name: repr(value) for name, value in second.items()})


class Refusals(unittest.TestCase):
    def test_a_cutoff_that_is_not_a_whole_number_of_microseconds_is_refused(self) -> None:
        bad_cutoff: Any = CUTOFF_US + 0.9
        with self.assertRaisesRegex(FeatureInputError, "cutoff"):
            values_of(ART, waveform_window(alternating), bad_cutoff)

    def test_two_seconds_with_one_index_are_refused(self) -> None:
        window = waveform_window(alternating)
        with self.assertRaisesRegex(FeatureInputError, "one second"):
            values_of(ART, WaveformWindow(window.sample_rate, window.seconds + (window.seconds[3],)))

    def test_a_track_the_registry_does_not_know_is_refused(self) -> None:
        with self.assertRaisesRegex(FeatureInputError, "unknown track"):
            values_of("SNUADC/NOT_A_TRACK", waveform_window(alternating))


if __name__ == "__main__":
    unittest.main()
