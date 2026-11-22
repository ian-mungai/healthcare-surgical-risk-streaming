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
import unittest

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
    def test_the_window_is_the_20_complete_seconds_that_end_at_the_cutoff(self) -> None:
        indexes = window_seconds(20)
        outside = [second_of(indexes[0] - 1, [999.0] * 10), second_of(indexes[-1] + 1, [999.0] * 10), second_of(indexes[-1] + 5, [999.0] * 10)]
        window = waveform_window(alternating)
        padded = WaveformWindow(window.sample_rate, tuple(outside[:1]) + window.seconds + tuple(outside[1:]))
        self.assertEqual(values_of(ART, window), values_of(ART, padded))
        self.assertEqual(CUTOFF_US, waveform_group(ART, padded, CUTOFF_US).latest_us)

    def test_a_second_that_is_not_complete_at_the_cutoff_is_not_used(self) -> None:
        cutoff = CUTOFF_US + 500_000
        window = waveform_window(alternating)  # ends with the second that ends at CUTOFF_US
        partial = second_of(CUTOFF_US // SECOND, [999.0] * 10)
        padded = WaveformWindow(window.sample_rate, window.seconds + (partial,))
        result = values_of(ART, padded, cutoff)
        self.assertEqual(20, result["snuadc_art__valid_seconds"])
        self.assertAlmostEqual(97.75, get(result, "snuadc_art__mean"), places=9)
        self.assertEqual(CUTOFF_US, waveform_group(ART, padded, cutoff).latest_us)

    def test_the_first_second_of_the_window_is_used_and_the_one_before_it_is_not(self) -> None:
        indexes = window_seconds(21)
        seconds = tuple(second_of(index, [50.0] * 10 if position == 0 else [100.0] * 10) for position, index in enumerate(indexes))
        result = values_of(ART, WaveformWindow(RATE_HZ, seconds))
        self.assertEqual(100.0, result["snuadc_art__mean"])
        self.assertEqual(20, result["snuadc_art__valid_seconds"])


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

    def test_a_gap_does_not_move_the_frequency(self) -> None:
        window = waveform_window(sine(1.2, 90.0, 20.0), invalid=frozenset({3, 4, 11}))
        self.assertAlmostEqual(1.2, get(values_of(ART, window), "snuadc_art__dominant_freq_hz"), places=9)

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
    def test_two_seconds_with_one_index_are_refused(self) -> None:
        window = waveform_window(alternating)
        with self.assertRaisesRegex(FeatureInputError, "one second"):
            values_of(ART, WaveformWindow(window.sample_rate, window.seconds + (window.seconds[3],)))

    def test_a_valid_second_with_no_sample_or_a_sample_that_is_not_a_number_is_refused(self) -> None:
        window = waveform_window(alternating)
        for bad in ((), (1.0, math.nan)):
            with self.subTest(bad=bad):
                seconds = (WaveformSecond(window.seconds[0].second, bad, True),) + window.seconds[1:]
                with self.assertRaises(FeatureInputError):
                    values_of(ART, WaveformWindow(window.sample_rate, seconds))

    def test_a_sample_rate_that_is_not_positive_and_finite_is_refused(self) -> None:
        window = waveform_window(alternating)
        for rate in (0.0, -1.0, math.inf, math.nan):
            with self.subTest(rate=rate), self.assertRaises(FeatureInputError):
                values_of(ART, WaveformWindow(rate, window.seconds))

    def test_a_track_the_registry_does_not_know_is_refused(self) -> None:
        with self.assertRaisesRegex(FeatureInputError, "unknown track"):
            values_of("SNUADC/NOT_A_TRACK", waveform_window(alternating))


if __name__ == "__main__":
    unittest.main()
