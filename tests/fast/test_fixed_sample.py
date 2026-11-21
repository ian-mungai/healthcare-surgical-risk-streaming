"""The patient assignment rule and the form of the tracked candidate files.

Failure modes covered here:

- The rule changes for a patient number, so a patient moves between the parts after the split was frozen.
- The thresholds are swapped or off by one, so the shares are not 70, 10 and 20 percent.
- A number that is not a patient number is accepted and given a part.
- A tracked candidate line has a key outside the release, a checksum that is not 64 hex characters, a case listed twice or a key that names another case.
- The candidates cannot serve the later selection: no case with and without the arterial waveform, no repeat patient, no development case or no test case.
- The pin of the checksum list or of a table is missing, malformed or outside the release.

The selection itself is proved against the publisher's real files in ``tests/e2e/fixed_sample.py``.
"""

from __future__ import annotations

import hashlib
import json
import unittest
from collections import Counter
from pathlib import Path

from scripts.sample.candidates import (
    CANDIDATES_FILE,
    RELEASE_FILE,
    SLOTS,
    TOTAL_SIZE_LIMIT_BYTES,
    Candidate,
    candidate_problems,
    read_candidates,
    release_problems,
)
from scripts.sample.publisher import RELEASE_PREFIX
from scripts.sample.split import BUCKETS, SEED, patient_bucket, patient_part

FROZEN_BUCKETS = {1: 92, 2: 92, 3: 48, 4: 24, 5: 13, 6: 6, 7: 61, 8: 83, 9: 46, 10: 90, 14: 69, 17: 75, 25: 70, 107: 79, 130: 80, 6090: 4, 10000: 67}
FROZEN_PARTS = {1: "test", 3: "train", 14: "train", 25: "validation", 17: "validation", 107: "validation", 130: "test", 6090: "train"}
SHARE_SAMPLE = range(1, 100_001)


class AssignmentRule(unittest.TestCase):
    def test_the_hash_of_one_input_is_the_documented_one(self) -> None:
        digest = hashlib.sha256(b"1:1").hexdigest()
        self.assertEqual("d6b5915c46057bcb005f46f6433df65609dd3a7a57af75ac1a5a4a7c299ebffb", digest)
        self.assertEqual(int(digest[:15], 16) % BUCKETS, patient_bucket(1))

    def test_each_example_patient_keeps_its_bucket(self) -> None:
        self.assertEqual(FROZEN_BUCKETS, {patient: patient_bucket(patient) for patient in FROZEN_BUCKETS})

    def test_each_example_patient_keeps_its_part(self) -> None:
        self.assertEqual(FROZEN_PARTS, {patient: patient_part(patient) for patient in FROZEN_PARTS})

    def test_the_frozen_seed_and_bucket_count(self) -> None:
        self.assertEqual((1, 100), (SEED, BUCKETS))

    def test_the_same_input_gives_the_same_part(self) -> None:
        self.assertEqual([patient_part(patient) for patient in range(1, 500)], [patient_part(patient) for patient in range(1, 500)])

    def test_another_seed_changes_some_part(self) -> None:
        self.assertTrue(any(patient_part(patient) != patient_part(patient, seed=2) for patient in range(1, 200)))

    def test_every_patient_number_has_exactly_one_of_the_three_parts(self) -> None:
        shares = Counter(patient_part(patient) for patient in SHARE_SAMPLE)
        self.assertEqual({"train", "validation", "test"}, set(shares))
        self.assertEqual(len(SHARE_SAMPLE), sum(shares.values()))

    def test_the_shares_are_70_10_and_20_percent(self) -> None:
        shares = Counter(patient_part(patient) for patient in SHARE_SAMPLE)
        observed: dict[str, float] = {part: shares[part] / len(SHARE_SAMPLE) for part in shares}
        for part, expected in {"train": 0.70, "validation": 0.10, "test": 0.20}.items():
            self.assertAlmostEqual(expected, observed[part], delta=0.01, msg=part)

    def test_a_number_that_is_no_patient_number_is_refused(self) -> None:
        for number in (0, -5):
            with self.assertRaises(ValueError, msg=str(number)):
                patient_part(number)


class TrackedFiles(unittest.TestCase):
    candidates: list[Candidate]
    release: dict[str, object]

    @classmethod
    def setUpClass(cls) -> None:
        cls.candidates = read_candidates(CANDIDATES_FILE)
        cls.release = json.loads(Path(RELEASE_FILE).read_text())

    def test_the_tracked_candidates_have_no_problem(self) -> None:
        self.assertEqual([], candidate_problems(self.candidates))

    def test_the_tracked_release_pin_has_no_problem(self) -> None:
        self.assertEqual([], release_problems(self.release))

    def test_the_number_of_candidates_and_their_total_size(self) -> None:
        self.assertTrue(12 <= len(self.candidates) <= 20, len(self.candidates))
        self.assertLess(sum(candidate.size_bytes for candidate in self.candidates), TOTAL_SIZE_LIMIT_BYTES)

    def test_every_slot_of_a_candidate_is_a_declared_rule(self) -> None:
        self.assertLessEqual({candidate.slot for candidate in self.candidates}, {slot.name for slot in SLOTS})

    def test_the_candidates_can_serve_the_later_selection(self) -> None:
        patients = Counter(candidate.patient for candidate in self.candidates)
        development = {patient_part(candidate.patient) != "test" for candidate in self.candidates}
        self.assertEqual({True, False}, {candidate.arterial_waveform for candidate in self.candidates})
        self.assertEqual({True, False}, development)
        self.assertGreaterEqual(sum(1 for count in patients.values() if count > 1), 2)


class PlantedFaults(unittest.TestCase):
    def setUp(self) -> None:
        self.candidates = read_candidates(CANDIDATES_FILE)

    def replaced(self, index: int, **changes: object) -> list[Candidate]:
        return [Candidate(**(vars(candidate) | changes)) if position == index else candidate for position, candidate in enumerate(self.candidates)]

    def test_a_key_outside_the_release_is_found(self) -> None:
        problems = candidate_problems(self.replaced(0, key="other/1.0.0/vital_files/0001.vital"))
        self.assertTrue(any("outside the release" in problem for problem in problems), problems)

    def test_a_key_that_names_another_case_is_found(self) -> None:
        wrong = f"{RELEASE_PREFIX}vital_files/{self.candidates[0].case + 1:04d}.vital"
        problems = candidate_problems(self.replaced(0, key=wrong))
        self.assertTrue(any("names another case" in problem for problem in problems), problems)

    def test_a_checksum_that_is_not_64_hex_characters_is_found(self) -> None:
        for bad in ("abc", "g" * 64, "A" * 64, "a" * 65):
            problems = candidate_problems(self.replaced(0, sha256=bad))
            self.assertTrue(any("64 hex" in problem for problem in problems), f"{bad}: {problems}")

    def test_a_case_listed_twice_is_found(self) -> None:
        problems = candidate_problems([*self.candidates, self.candidates[0]])
        self.assertTrue(any("listed twice" in problem for problem in problems), problems)

    def test_a_malformed_pin_is_found(self) -> None:
        pin = json.loads(Path(RELEASE_FILE).read_text())
        pin["checksum_list"]["sha256"] = "z" * 64
        self.assertTrue(any("64 hex" in problem for problem in release_problems(pin)))
        pin = json.loads(Path(RELEASE_FILE).read_text())
        pin["checksum_list"]["key"] = "elsewhere/SHA256SUMS.txt"
        self.assertTrue(any("outside the release" in problem for problem in release_problems(pin)))

    def test_a_malformed_table_or_track_list_pin_is_found(self) -> None:
        pin = json.loads(Path(RELEASE_FILE).read_text())
        pin["tables"][0]["sha256"] = "abc"
        self.assertTrue(any(problem.startswith("tables[0]") and "64 hex" in problem for problem in release_problems(pin)))
        pin = json.loads(Path(RELEASE_FILE).read_text())
        pin["tables"][1]["key"] = "elsewhere/lab_data.csv"
        self.assertTrue(any("outside the release" in problem for problem in release_problems(pin)))
        pin = json.loads(Path(RELEASE_FILE).read_text())
        pin["tables"][1]["key"] = f"{RELEASE_PREFIX}track_names.csv"
        self.assertTrue(any("must name clinical_data.csv and lab_data.csv" in problem for problem in release_problems(pin)))
        pin = json.loads(Path(RELEASE_FILE).read_text())
        pin["tables"].pop()
        self.assertTrue(any("must name clinical_data.csv and lab_data.csv" in problem for problem in release_problems(pin)))
        pin = json.loads(Path(RELEASE_FILE).read_text())
        pin["track_list"]["sha256"] = "abc"
        self.assertTrue(any(problem.startswith("track_list") and "64 hex" in problem for problem in release_problems(pin)))
        pin = json.loads(Path(RELEASE_FILE).read_text())
        del pin["track_list"]
        self.assertTrue(any(problem.startswith("track_list") for problem in release_problems(pin)))


if __name__ == "__main__":
    unittest.main()
