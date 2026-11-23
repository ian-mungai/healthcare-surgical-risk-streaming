"""The passes over a case file: which tracks one read of the file takes.

Failure modes covered here:

- A track is in no pass, so its rows are missing from a table, or in two passes, so its rows are written twice.
- A waveform track shares a pass with another track, so a worker holds two large tracks at once and its memory is no longer the largest single track's.
- A track of another kind than the table's is read in the table's pass.
- The two modes (all numeric tracks in one pass or one pass per track) give different tracks or a different order, so a run for comparison is not a comparison.
- A mode that is not named is taken for the default.

The decoder reads the passes from the library: ``tests/e2e/bronze_decode.py`` compares the rows of both modes and counts the reads of each.
"""

from __future__ import annotations

import unittest

from spark.bronze.plan import GROUPED, PER_TRACK, PlanError, track_passes

TYPES = {"EVENT": 5, "Solar8000/HR": 2, "Solar8000/SPO2": 2, "Primus/CO2": 1, "SNUADC/ECG_II": 1, "Primus/ETCO2": 2, "Orchestra/NOTE": 5}


class Passes(unittest.TestCase):
    def test_grouped_numeric_tracks_are_one_pass_in_name_order(self) -> None:
        self.assertEqual([["Primus/ETCO2", "Solar8000/HR", "Solar8000/SPO2"]], track_passes(TYPES, "numeric", GROUPED))

    def test_grouped_string_tracks_are_one_pass(self) -> None:
        self.assertEqual([["EVENT", "Orchestra/NOTE"]], track_passes(TYPES, "string", GROUPED))

    def test_each_waveform_track_has_a_pass_of_its_own_in_both_modes(self) -> None:
        expected = [["Primus/CO2"], ["SNUADC/ECG_II"]]
        self.assertEqual(expected, track_passes(TYPES, "waveform", GROUPED))
        self.assertEqual(expected, track_passes(TYPES, "waveform", PER_TRACK))

    def test_per_track_mode_has_one_pass_for_each_track(self) -> None:
        self.assertEqual([["Primus/ETCO2"], ["Solar8000/HR"], ["Solar8000/SPO2"]], track_passes(TYPES, "numeric", PER_TRACK))
        self.assertEqual([["EVENT"], ["Orchestra/NOTE"]], track_passes(TYPES, "string", PER_TRACK))

    def test_both_modes_take_the_same_tracks_in_the_same_order(self) -> None:
        for kind in ("numeric", "string", "waveform"):
            grouped = [name for names in track_passes(TYPES, kind, GROUPED) for name in names]
            single = [name for names in track_passes(TYPES, kind, PER_TRACK) for name in names]
            self.assertEqual(single, grouped, kind)

    def test_every_track_is_in_exactly_one_pass_of_its_kind(self) -> None:
        seen: list[str] = []
        for kind in ("numeric", "string", "waveform"):
            for names in track_passes(TYPES, kind, GROUPED):
                seen += names
        self.assertEqual(sorted(TYPES), sorted(seen))

    def test_a_case_with_no_track_of_a_kind_has_no_pass_for_it(self) -> None:
        self.assertEqual([], track_passes({"EVENT": 5}, "numeric", GROUPED))
        self.assertEqual([], track_passes({}, "waveform", PER_TRACK))

    def test_a_kind_or_a_mode_that_is_not_named_is_refused(self) -> None:
        with self.assertRaises(PlanError):
            track_passes(TYPES, "tracks", GROUPED)
        with self.assertRaises(PlanError):
            track_passes(TYPES, "numeric", "grouped ")


if __name__ == "__main__":
    unittest.main()
