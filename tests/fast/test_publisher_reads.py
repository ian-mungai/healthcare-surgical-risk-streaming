"""How the reads of the publisher's files fail, and which files the parsers and the pin accept.

Failure modes covered here:

- A read passes its deadline, a program is missing or a program exits non-zero, and the caller gets a raw exception instead of a clear message that
  names the target and the deadline and holds no command arguments.
- The key listing is not JSON, has no ``Contents`` or has an entry without a key or a size.
- The track list or the clinical table has another column set than the one the code reads, a line with too few or too many fields, a case number or a
  time that is not an integer, a case that ends before it starts or a case listed twice, and the parser skips the line or fails later with a raw error.
- A case of the clinical table is missing from the checksum list or the key listing, or the track list names a case that the clinical table lacks.
- The release pin names another track-list address or another checksum-list key than the one the code reads, or lists a table twice.

The inline text in these tests is a refusal case for a parser rule. It is not a data sample. The published files themselves are read in
``tests/e2e/fixed_sample.py``.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from unittest import mock

from scripts.process import CompletedProcess, TimeoutExpired
from scripts.sample import candidates, publisher
from scripts.sample.candidates import CANDIDATES_FILE, RELEASE_FILE, read_candidates, release_problems
from scripts.sample.publisher import PublisherError

TRACK_HEADER = "caseid,tname,tid\n"
CLINICAL_HEADER = "﻿caseid,subjectid,casestart,caseend,age\n"


def completed(returncode: int, stdout: str = "") -> CompletedProcess[str]:
    return CompletedProcess(["aws"], returncode, stdout, "")


class ReadFailures(unittest.TestCase):
    def raised(self, call: Callable[[], object], runner: mock.Mock) -> PublisherError:
        with mock.patch.object(publisher, "run_command", runner), self.assertRaises(PublisherError) as caught:
            call()
        return caught.exception

    def test_a_listing_past_its_deadline_names_the_deadline(self) -> None:
        error = self.raised(publisher.list_release, mock.Mock(side_effect=TimeoutExpired("aws", 300)))
        self.assertEqual("listing the release timed out after 300 seconds", str(error))

    def test_a_key_read_past_its_deadline_names_the_key_and_no_argument(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "file"
            error = self.raised(
                lambda: publisher.download_key(publisher.RELEASE_PREFIX + "clinical_data.csv", target), mock.Mock(side_effect=TimeoutExpired("aws", 300))
            )
        self.assertEqual("reading vitaldb/1.0.0/clinical_data.csv timed out after 300 seconds", str(error))

    def test_a_url_read_past_its_deadline_names_the_deadline(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            error = self.raised(
                lambda: publisher.download_url(publisher.TRACK_LIST_URL, Path(folder) / "file"), mock.Mock(side_effect=TimeoutExpired("curl", 330))
            )
        self.assertIn("timed out after 330 seconds", str(error))

    def test_a_missing_program_is_named(self) -> None:
        error = self.raised(publisher.list_release, mock.Mock(side_effect=FileNotFoundError("program not found on PATH: aws")))
        self.assertIn("aws", str(error))
        self.assertIn("not installed", str(error))

    def test_a_non_zero_exit_names_the_exit_status(self) -> None:
        error = self.raised(publisher.list_release, mock.Mock(return_value=completed(255)))
        self.assertIn("exit status 255", str(error))

    def test_a_listing_that_is_not_json_is_refused(self) -> None:
        error = self.raised(publisher.list_release, mock.Mock(return_value=completed(0, "not json")))
        self.assertIn("listing", str(error))

    def test_a_listing_without_contents_is_refused(self) -> None:
        error = self.raised(publisher.list_release, mock.Mock(return_value=completed(0, "{}")))
        self.assertIn("Contents", str(error))

    def test_a_listing_entry_without_a_size_is_refused(self) -> None:
        error = self.raised(
            publisher.list_release, mock.Mock(return_value=completed(0, json.dumps({"Contents": [{"Key": "vitaldb/1.0.0/vital_files/0001.vital"}]})))
        )
        self.assertIn("entries", str(error))

    def test_a_listing_whose_contents_are_no_list_of_entries_is_refused(self) -> None:
        for contents in (5, [5], [None]):
            error = self.raised(publisher.list_release, mock.Mock(return_value=completed(0, json.dumps({"Contents": contents}))))
            self.assertIn("entries", str(error))

    def test_a_listing_with_an_entry_is_read(self) -> None:
        item = read_candidates(CANDIDATES_FILE)[0]
        with mock.patch.object(publisher, "run_command", return_value=completed(0, json.dumps({"Contents": [{"Key": item.key, "Size": item.size_bytes}]}))):
            self.assertEqual({item.key: item.size_bytes}, publisher.list_release())


class Parsers(unittest.TestCase):
    def parsed(self, parser: str, text: str, *arguments: str) -> object:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "file"
            path.write_text(text)
            return getattr(publisher, parser)(path, *arguments)

    def refused(self, parser: str, text: str, cause: str, *arguments: str) -> None:
        with self.assertRaises(PublisherError, msg=text) as caught:
            self.parsed(parser, text, *arguments)
        self.assertIn(cause, str(caught.exception))

    def test_the_track_list_needs_exactly_its_columns(self) -> None:
        for header in ("caseid,tname\n", "caseid,tname,tid,extra\n", "tname,caseid,tid\n", ""):
            self.refused("parse_track_list", header, "columns", "SNUADC/ART")

    def test_a_track_list_line_with_too_few_or_too_many_fields_is_refused(self) -> None:
        for line in ("1,SNUADC/ART\n", "1,SNUADC/ART,a,b\n", "\n"):
            self.refused("parse_track_list", TRACK_HEADER + line, "line 2", "SNUADC/ART")

    def test_a_track_list_case_that_is_no_number_is_refused(self) -> None:
        for case in ("x", "", "1.5", "-1"):
            self.refused("parse_track_list", TRACK_HEADER + f"{case},SNUADC/ART,a\n", "line 2", "SNUADC/ART")

    def test_a_track_list_reads_the_cases_with_the_named_track(self) -> None:
        text = TRACK_HEADER + "7,SNUADC/ART,a\n7,Solar8000/ART_MBP,b\n9,Solar8000/ART_MBP,c\n"
        self.assertEqual({7}, self.parsed("parse_track_list", text, "SNUADC/ART"))

    def test_the_clinical_table_needs_its_four_columns_once(self) -> None:
        for header in ("﻿caseid,subjectid,casestart\n", "﻿caseid,subjectid,casestart,caseend,caseid\n", ""):
            self.refused("parse_clinical_table", header, "columns")

    def test_a_clinical_line_with_the_wrong_number_of_fields_is_refused(self) -> None:
        for line in ("5,7,0\n", "5,7,0,100,40,9\n"):
            self.refused("parse_clinical_table", CLINICAL_HEADER + line, "line 2")

    def test_a_clinical_value_that_is_no_integer_is_refused_without_showing_it(self) -> None:
        for line in ("5,seven,0,100,40\n", "5,7,,100,40\n", "5,7,0,1e3,40\n"):
            with self.assertRaises(PublisherError, msg=line) as caught:
                self.parsed("parse_clinical_table", CLINICAL_HEADER + line)
            self.assertIn("line 2", str(caught.exception))
            self.assertNotIn("seven", str(caught.exception))

    def test_a_case_that_ends_before_it_starts_is_refused(self) -> None:
        self.refused("parse_clinical_table", CLINICAL_HEADER + "5,7,100,50,40\n", "line 2")

    def test_a_case_listed_twice_is_refused(self) -> None:
        self.refused("parse_clinical_table", CLINICAL_HEADER + "5,7,0,100,40\n5,8,0,100,40\n", "twice")

    def test_a_clinical_table_is_read_with_its_extra_columns_ignored(self) -> None:
        self.assertEqual([(5, 7, 100), (6, 7, 90)], self.parsed("parse_clinical_table", CLINICAL_HEADER + "5,7,0,100,40\n6,7,10,100,41\n"))

    def test_a_checksum_list_line_with_no_checksum_is_refused(self) -> None:
        with self.assertRaises(PublisherError):
            publisher.parse_checksum_list("not a checksum  vital_files/0001.vital\n")


class CaseFactsBinding(unittest.TestCase):
    def setUp(self) -> None:
        self.tracked = read_candidates(CANDIDATES_FILE)[:2]
        self.sums = {item.key: item.sha256 for item in self.tracked}
        self.sizes = {item.key: item.size_bytes for item in self.tracked}
        self.rows = [(item.case, item.patient, 600) for item in self.tracked]

    def test_each_case_takes_its_checksum_size_and_waveform_from_the_publisher(self) -> None:
        facts = candidates.case_facts(self.rows, self.sums, self.sizes, {self.tracked[0].case})
        self.assertEqual([item.sha256 for item in self.tracked], [item.sha256 for item in facts])
        self.assertEqual([item.size_bytes for item in self.tracked], [item.size_bytes for item in facts])
        self.assertEqual([True, False], [item.arterial_waveform for item in facts])

    def test_a_case_missing_from_the_checksum_list_is_refused_by_number(self) -> None:
        del self.sums[self.tracked[1].key]
        with self.assertRaises(PublisherError) as caught:
            candidates.case_facts(self.rows, self.sums, self.sizes, set())
        self.assertIn(f"case {self.tracked[1].case}", str(caught.exception))

    def test_a_case_missing_from_the_listing_is_refused_by_number(self) -> None:
        del self.sizes[self.tracked[0].key]
        with self.assertRaises(PublisherError) as caught:
            candidates.case_facts(self.rows, self.sums, self.sizes, set())
        self.assertIn(f"case {self.tracked[0].case}", str(caught.exception))

    def test_a_waveform_case_the_clinical_table_lacks_is_refused(self) -> None:
        with self.assertRaises(PublisherError) as caught:
            candidates.case_facts(self.rows, self.sums, self.sizes, {99999})
        self.assertIn("case 99999", str(caught.exception))


class TemporaryFolder(unittest.TestCase):
    def test_the_download_folder_is_removed_after_a_failed_read(self) -> None:
        seen: list[Path] = []

        def failing(release: dict[str, object], workdir: Path) -> None:
            (workdir / "partial").write_text("partial download")
            seen.append(workdir)
            raise PublisherError("reading failed")

        with mock.patch.object(candidates, "read_inputs", failing), self.assertRaises(PublisherError):
            candidates.rebuild(RELEASE_FILE)
        self.assertEqual(1, len(seen))
        self.assertFalse(seen[0].exists())


class ClosedPin(unittest.TestCase):
    def pin(self) -> dict[str, object]:
        loaded: dict[str, object] = json.loads(Path(RELEASE_FILE).read_text())
        return loaded

    def test_the_tracked_pin_names_the_addresses_the_code_reads(self) -> None:
        self.assertEqual([], release_problems(self.pin()))

    def test_another_track_list_address_is_refused(self) -> None:
        pin = self.pin()
        pin["track_list"] = pin["track_list"] | {"url": "https://example.invalid/trks"}  # type: ignore[operator]
        self.assertTrue(any("address" in problem for problem in release_problems(pin)))

    def test_another_checksum_list_key_inside_the_release_is_refused(self) -> None:
        pin = self.pin()
        pin["checksum_list"] = pin["checksum_list"] | {"key": publisher.RELEASE_PREFIX + "clinical_data.csv"}  # type: ignore[operator]
        self.assertTrue(any("SHA256SUMS.txt" in problem for problem in release_problems(pin)))

    def test_a_table_listed_twice_is_refused(self) -> None:
        pin = self.pin()
        tables = list(pin["tables"])  # type: ignore[call-overload]
        pin["tables"] = [*tables, tables[0]]
        self.assertTrue(any("tables" in problem for problem in release_problems(pin)))

    def test_a_pin_that_is_no_object_is_refused(self) -> None:
        self.assertTrue(release_problems([]))  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
