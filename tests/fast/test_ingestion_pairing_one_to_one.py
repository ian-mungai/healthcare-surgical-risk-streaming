"""The check pairs files one to one: a landed version that two sources point at is a fault of both and is never read.

The pairing step refuses such records. The check also refuses them, so a pairing file that was written by hand or by another tool cannot make one landed version
stand for two source files.
"""

from __future__ import annotations

import unittest

from scripts.ingestion.check import run_check
from scripts.ingestion.landing import PairingRecord
from scripts.ingestion.notice import Cause
from tests.fast.ingestion_support import SOURCE_PREFIX, landed_set, pin_the_synthetic_list


def setUpModule() -> None:
    pin_the_synthetic_list()


class OneToOneTest(unittest.TestCase):
    def test_control_one_landed_version_for_each_source_passes(self) -> None:
        landed = landed_set()
        self.assertTrue(landed.check().passed)

    def test_two_sources_that_name_one_landed_version_are_each_a_finding_and_nothing_is_read(self) -> None:
        landed = landed_set()
        first, second = landed.keys["vital_files/0001.vital"], "vital_files/0002.vital"
        pairings = [PairingRecord(SOURCE_PREFIX + path, first if path == second else key, landed.sizes[path]) for path, key in landed.keys.items()]
        landed.store.reads.clear()
        result = run_check(landed.store, landed.selection, landed.list_bytes, pairings)
        shared = sorted(finding.source_key for finding in result.findings if finding.cause is Cause.LANDED_SHARED)
        self.assertFalse(result.passed)
        self.assertEqual([SOURCE_PREFIX + "vital_files/0001.vital", SOURCE_PREFIX + "vital_files/0002.vital"], shared)
        self.assertEqual([], [read for read in landed.store.reads if read.key == first])

    def test_a_record_of_a_file_outside_the_selection_that_shares_a_landed_version_is_a_finding_of_the_selected_file(self) -> None:
        landed = landed_set()
        pairings = [
            *landed.pairings(),
            PairingRecord(SOURCE_PREFIX + "vital_files/0999.vital", landed.keys["clinical_data.csv"], landed.sizes["clinical_data.csv"]),
        ]
        result = run_check(landed.store, landed.selection, landed.list_bytes, pairings)
        self.assertEqual([Cause.LANDED_SHARED], [finding.cause for finding in result.findings])
        self.assertEqual(SOURCE_PREFIX + "clinical_data.csv", result.findings[0].source_key)

    def test_the_same_record_twice_is_a_duplicate_pairing_and_not_a_shared_landed_version(self) -> None:
        landed = landed_set()
        result = run_check(landed.store, landed.selection, landed.list_bytes, [*landed.pairings(), landed.pairings()[0]])
        self.assertEqual([Cause.DUPLICATE_PAIRING], [finding.cause for finding in result.findings])


if __name__ == "__main__":
    unittest.main()
