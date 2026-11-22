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

import contextlib
import io
import json
import os
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any
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


class PinnedFiles(unittest.TestCase):
    def test_a_file_of_the_pinned_size_with_another_checksum_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "file"
            path.write_bytes(b"abc")
            publisher.read_pinned("the track list", path, 3, publisher.file_sha256(path))
            with self.assertRaisesRegex(PublisherError, "SHA-256"):
                publisher.read_pinned("the track list", path, 3, "0" * 64)

    def test_a_checksum_list_that_names_a_key_twice_is_refused_by_its_line(self) -> None:
        good = "0" * 64
        self.assertEqual({publisher.RELEASE_PREFIX + "a.csv": good}, publisher.parse_checksum_list(f"{good}  a.csv\n"))
        with self.assertRaisesRegex(PublisherError, "line 2") as caught:
            publisher.parse_checksum_list(f"{good}  private-value.csv\n{good}  private-value.csv\n")
        self.assertNotIn("private-value", str(caught.exception))

    def test_an_unsigned_read_runs_without_any_aws_setting_of_the_caller(self) -> None:
        with mock.patch.dict("os.environ", {"AWS_PROFILE": "owner", "AWS_ACCESS_KEY_ID": "x", "LANG": "C.UTF-8"}):
            environment = publisher.public_environment()
        self.assertEqual("C.UTF-8", environment["LANG"])
        self.assertNotIn("AWS_PROFILE", environment)
        self.assertNotIn("AWS_ACCESS_KEY_ID", environment)
        self.assertEqual(os.devnull, environment["AWS_SHARED_CREDENTIALS_FILE"])


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

    def test_a_pin_for_another_bucket_region_or_release_path_than_the_reads_use_is_refused(self) -> None:
        for field, other in (("bucket", "another-bucket"), ("region", "eu-west-1"), ("release_path", "vitaldb/2.0.0/")):
            pin = self.pin()
            pin["source"] = pin["source"] | {field: other}  # type: ignore[operator]
            with self.subTest(field=field):
                self.assertTrue(any(problem.startswith(f"source: the {field}") for problem in release_problems(pin)), release_problems(pin))
        pin = self.pin()
        del pin["source"]
        self.assertTrue(any(problem.startswith("source") for problem in release_problems(pin)))

    def test_a_pin_with_a_table_list_of_another_type_is_refused_and_not_raised(self) -> None:
        for tables in (None, 5, "ab", {"a": 1}):
            pin = self.pin()
            pin["tables"] = tables
            with self.subTest(tables=tables):
                self.assertTrue(any(problem.startswith("tables") for problem in release_problems(pin)))

    def test_a_size_that_is_a_boolean_is_refused(self) -> None:
        pin = self.pin()
        pin["track_list"] = pin["track_list"] | {"size_bytes": True}  # type: ignore[operator]
        self.assertTrue(any("size" in problem for problem in release_problems(pin)))


class QuietMessages(unittest.TestCase):
    MARKER = "private-value"

    def test_no_problem_of_the_pin_names_a_value_of_the_file(self) -> None:
        pin = json.loads(Path(RELEASE_FILE).read_text())
        pin["checksum_list"]["key"] = f"{self.MARKER}/SHA256SUMS.txt"
        pin["tables"][0]["key"] = f"{self.MARKER}/clinical_data.csv"
        pin["tables"][1]["sha256"] = self.MARKER
        pin["track_list"]["url"] = f"https://{self.MARKER}.invalid/trks"
        pin["source"]["bucket"] = self.MARKER
        problems = release_problems(pin)
        self.assertTrue(problems)
        self.assertNotIn(self.MARKER, " ".join(problems))

    def test_no_problem_of_a_candidate_names_its_key(self) -> None:
        item = read_candidates(CANDIDATES_FILE)[0]
        for key in (f"{self.MARKER}/vital_files/0001.vital", f"{publisher.RELEASE_PREFIX}vital_files/{self.MARKER}.vital"):
            problems = candidates.candidate_problems([candidates.Candidate(**(vars(item) | {"key": key}))])
            self.assertTrue(problems)
            self.assertNotIn(self.MARKER, " ".join(problems))

    def test_no_problem_against_the_publisher_names_the_key_of_a_candidate(self) -> None:
        item = candidates.Candidate(**(vars(read_candidates(CANDIDATES_FILE)[0]) | {"key": f"{self.MARKER}/vital_files/0001.vital"}))
        problems = candidates.listing_problems([item], {}, {})
        self.assertEqual(2, len(problems))
        self.assertNotIn(self.MARKER, " ".join(problems))

    def test_a_candidate_line_with_a_value_that_is_no_integer_is_refused_by_its_line_alone(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "candidates.tsv"
            fields = ["x", "7", "k", "10", "a" * 64, "true", "slot"]
            fields[0] = self.MARKER
            path.write_text("\t".join(candidates.HEADER) + "\n" + "\t".join(fields) + "\n")
            with self.assertRaises(ValueError) as caught:
                read_candidates(path)
        self.assertIn("line 2", str(caught.exception))
        self.assertNotIn(self.MARKER, str(caught.exception))

    def test_no_size_problem_shows_the_size(self) -> None:
        item = candidates.Candidate(**(vars(read_candidates(CANDIDATES_FILE)[0]) | {"size_bytes": -987654}))
        problems = candidates.candidate_problems([item])
        self.assertTrue(any("size" in problem for problem in problems), problems)
        self.assertNotIn("987654", " ".join(problems))
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "file"
            path.write_bytes(b"abc")
            with self.assertRaises(PublisherError) as caught:
                publisher.read_pinned("the track list", path, 987654, "0" * 64)
        self.assertIn("size", str(caught.exception))
        self.assertNotIn("987654", str(caught.exception))

    def test_a_build_names_no_path_of_its_output(self) -> None:
        tracked = read_candidates(CANDIDATES_FILE)
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / self.MARKER / "candidates.tsv"
            target.parent.mkdir()
            output = io.StringIO()
            with (
                mock.patch.object(candidates, "rebuild", return_value=(tracked, {}, {})),
                mock.patch("sys.argv", ["candidates", "build", "--output", str(target)]),
                contextlib.redirect_stdout(output),
            ):
                self.assertEqual(0, candidates.main())
            self.assertEqual(candidates.format_candidates(tracked), target.read_text())
        self.assertIn(f"wrote {len(tracked)} candidates", output.getvalue())
        self.assertNotIn(self.MARKER, output.getvalue())

    def test_a_command_line_that_is_not_valid_is_refused_without_echoing_it(self) -> None:
        for arguments in ([self.MARKER], ["check", f"--{self.MARKER}"], []):
            errors = io.StringIO()
            with (
                self.subTest(arguments=arguments),
                mock.patch("sys.argv", ["candidates", *arguments]),
                contextlib.redirect_stderr(errors),
                self.assertRaises(SystemExit) as stopped,
            ):
                candidates.main()
            self.assertEqual(2, stopped.exception.code)
            self.assertNotIn(self.MARKER, errors.getvalue())

    def run_command(self, release: object, candidates_text: str | None = None) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as folder:
            pin = Path(folder) / "release.json"
            pin.write_text(json.dumps(release))
            arguments = ["candidates", "check", "--release", str(pin)]
            if candidates_text is not None:
                tracked = Path(folder) / "candidates.tsv"
                tracked.write_text(candidates_text)
                arguments += ["--candidates", str(tracked)]
            errors = io.StringIO()
            with mock.patch("sys.argv", arguments), contextlib.redirect_stderr(errors):
                code = candidates.main()
        return code, errors.getvalue()

    def test_the_command_reports_a_bad_pin_by_field_and_never_by_value(self) -> None:
        pin = json.loads(Path(RELEASE_FILE).read_text())
        pin["checksum_list"]["key"] = f"{self.MARKER}/SHA256SUMS.txt"
        pin["tables"] = self.MARKER
        code, errors = self.run_command(pin)
        self.assertEqual(1, code)
        self.assertIn("checksum_list", errors)
        self.assertNotIn(self.MARKER, errors)

    def test_a_pin_that_repeats_a_field_is_refused_before_any_read(self) -> None:
        text = Path(RELEASE_FILE).read_text()
        repeated = text.replace('"tables": [', '"tables": [], "tables": [', 1)
        self.assertNotEqual(text, repeated)
        with tempfile.TemporaryDirectory() as folder:
            pin = Path(folder) / "release.json"
            pin.write_text(repeated)
            with mock.patch.object(candidates, "read_inputs", side_effect=AssertionError("a read started")), self.assertRaises(ValueError) as caught:
                candidates.rebuild(pin)
        self.assertNotIn("tables", str(caught.exception))

    def test_an_absent_or_unreadable_input_is_reported_by_its_role_without_a_traceback(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            private = Path(folder) / self.MARKER
            private.mkdir()
            undecodable = private / "release.json"
            undecodable.write_bytes(b"\xff\xfe")
            for arguments in (["check", "--release", str(private / "absent.json")], ["check", "--release", str(undecodable)]):
                errors = io.StringIO()
                with self.subTest(arguments=arguments[-1]), mock.patch("sys.argv", ["candidates", *arguments]), contextlib.redirect_stderr(errors):
                    self.assertEqual(1, candidates.main())
                self.assertIn("cannot read the release pin", errors.getvalue())
                self.assertNotIn(self.MARKER, errors.getvalue())
            errors = io.StringIO()
            arguments = ["candidates", "check", "--candidates", str(private / "absent.tsv")]
            with (
                mock.patch.object(candidates, "rebuild", side_effect=AssertionError("a read started")),
                mock.patch("sys.argv", arguments),
                contextlib.redirect_stderr(errors),
            ):
                self.assertEqual(1, candidates.main())
            self.assertIn("cannot read the candidate file", errors.getvalue())
            self.assertNotIn(self.MARKER, errors.getvalue())

    def test_a_build_whose_output_cannot_be_written_is_reported_by_its_role(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            blocked = Path(folder) / self.MARKER
            blocked.write_text("a file where a folder must be")
            errors = io.StringIO()
            with (
                mock.patch.object(candidates, "rebuild", return_value=(read_candidates(CANDIDATES_FILE), {}, {})),
                mock.patch("sys.argv", ["candidates", "build", "--output", str(blocked / "candidates.tsv")]),
                contextlib.redirect_stderr(errors),
            ):
                self.assertEqual(1, candidates.main())
        self.assertIn("cannot write the candidate file", errors.getvalue())
        self.assertNotIn(self.MARKER, errors.getvalue())

    def test_a_pin_whose_table_key_is_no_text_is_reported_and_not_raised(self) -> None:
        keys: tuple[Any, ...] = ([], {}, [self.MARKER], 5, None)
        for key in keys:
            pin = json.loads(Path(RELEASE_FILE).read_text())
            pin["tables"][0]["key"] = key
            with self.subTest(key=key):
                code, errors = self.run_command(pin)
                self.assertEqual(1, code)
                self.assertIn("tables[0]", errors)
                self.assertNotIn(self.MARKER, errors)
                self.assertNotIn("Traceback", errors)

    def test_the_command_reports_a_pin_that_is_not_valid_json_without_echoing_it(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            pin = Path(folder) / "release.json"
            pin.write_text(f"{self.MARKER}")
            errors = io.StringIO()
            with mock.patch("sys.argv", ["candidates", "check", "--release", str(pin)]), contextlib.redirect_stderr(errors):
                self.assertEqual(1, candidates.main())
        self.assertNotIn(self.MARKER, errors.getvalue())


class TrackedFileForms(unittest.TestCase):
    def read(self, text: str) -> list[candidates.Candidate]:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "candidates.tsv"
            path.write_text(text)
            return read_candidates(path)

    def test_control_the_tracked_file_reads(self) -> None:
        self.assertEqual(read_candidates(CANDIDATES_FILE), self.read(CANDIDATES_FILE.read_text()))

    def test_a_header_line_field_count_or_flag_that_is_not_the_form_is_refused_by_its_line(self) -> None:
        lines = CANDIDATES_FILE.read_text().splitlines()
        fields = lines[1].split("\t")
        cases = {
            "another header": ("\t".join(reversed(candidates.HEADER)), "header"),
            "a field too few": ("\t".join(fields[:-1]), "line 2"),
            "a field too many": ("\t".join([*fields, "x"]), "line 2"),
            "a flag that is not true or false": ("\t".join([*fields[:5], "yes", fields[6]]), "line 2"),
        }
        for name, (line, cause) in cases.items():
            text = "\n".join([line, *lines[1:]] if cause == "header" else [lines[0], line, *lines[2:]]) + "\n"
            with self.subTest(name), self.assertRaisesRegex(ValueError, cause):
                self.read(text)


class RebuildChecks(unittest.TestCase):
    def setUp(self) -> None:
        self.tracked = read_candidates(CANDIDATES_FILE)

    def rebuilt(self, chosen: list[candidates.Candidate]) -> object:
        with mock.patch.object(candidates, "read_inputs", return_value=([], {}, {})), mock.patch.object(candidates, "select_candidates", return_value=chosen):
            return candidates.rebuild(RELEASE_FILE)

    def test_control_a_rebuild_of_the_tracked_candidates_passes(self) -> None:
        self.assertEqual((self.tracked, {}, {}), self.rebuilt(self.tracked))

    def test_a_rebuild_over_the_size_limit_or_with_a_bad_candidate_is_refused(self) -> None:
        huge = [candidates.Candidate(**(vars(self.tracked[0]) | {"size_bytes": candidates.TOTAL_SIZE_LIMIT_BYTES})), *self.tracked[1:]]
        with self.assertRaisesRegex(PublisherError, "over the limit"):
            self.rebuilt(huge)
        bad = [candidates.Candidate(**(vars(self.tracked[0]) | {"sha256": "x"})), *self.tracked[1:]]
        with self.assertRaisesRegex(PublisherError, "64 hex"):
            self.rebuilt(bad)

    def test_a_tracked_line_that_differs_from_the_rebuild_is_found(self) -> None:
        changed = [candidates.Candidate(**(vars(self.tracked[0]) | {"slot": "arterial_test_other"})), *self.tracked[1:]]
        self.assertEqual([], candidates.difference_problems(self.tracked, self.tracked))
        self.assertEqual(
            [f"case {self.tracked[0].case}: the tracked line differs from the rebuilt line"], candidates.difference_problems(changed, self.tracked)
        )

    def test_a_pinned_table_whose_checksum_is_not_the_lists_is_refused_before_the_table_is_read(self) -> None:
        release = json.loads(RELEASE_FILE.read_text())
        listed = "".join(f"{table['sha256']}  {table['key'].removeprefix(publisher.RELEASE_PREFIX)}\n" for table in release["tables"])
        other = listed.replace(release["tables"][1]["sha256"], "0" * 64)
        for text, error, cause in ((listed, AssertionError, "listing was read"), (other, PublisherError, "pinned checksum")):
            with (
                tempfile.TemporaryDirectory() as folder,
                self.subTest(cause=cause),
                mock.patch.object(candidates, "download_key", side_effect=lambda key, target, text=text: target.write_text(text)),
                mock.patch.object(candidates, "read_pinned"),
                mock.patch.object(candidates, "list_release", side_effect=AssertionError("the listing was read")),
                self.assertRaisesRegex(error, cause),
            ):
                candidates.read_inputs(release, Path(folder))


class ClosedPinFields(unittest.TestCase):
    """The release pin is closed: each object holds exactly its known fields, each of its type. A problem names the object, never a field's name or value."""

    MARKER = "private-value"

    def changed(self, change: Callable[[dict[str, Any]], None]) -> list[str]:
        pin: dict[str, Any] = json.loads(Path(RELEASE_FILE).read_text())
        change(pin)
        return release_problems(pin)

    def test_control_the_tracked_pin_is_closed(self) -> None:
        self.assertEqual([], self.changed(lambda pin: None))

    def test_a_missing_object_is_named_missing_and_a_size_that_is_not_positive_is_found(self) -> None:
        self.assertIn("source: missing", self.changed(lambda pin: pin.pop("source")))
        self.assertIn("tables[1]: size is not a positive number", self.changed(lambda pin: pin["tables"][1].update(size_bytes=0)))

    def test_a_third_table_that_is_not_well_formed_is_refused_as_another_table_list(self) -> None:
        problems = self.changed(lambda pin: pin["tables"].append({"key": 1}))
        self.assertIn("tables: the pin must name clinical_data.csv and lab_data.csv of the release once each", problems)

    def test_an_unknown_missing_or_mistyped_field_is_a_problem_of_its_object(self) -> None:
        changes: dict[str, tuple[Callable[[dict[str, Any]], None], str]] = {
            "unknown top field": (lambda pin: pin.update({self.MARKER: 1}), "release pin"),
            "unknown source field": (lambda pin: pin["source"].update({self.MARKER: 1}), "source"),
            "source page missing": (lambda pin: pin["source"].pop("page"), "source"),
            "unknown list field": (lambda pin: pin["checksum_list"].update({self.MARKER: 1}), "checksum_list"),
            "unknown table field": (lambda pin: pin["tables"][1].update({self.MARKER: 1}), "tables[1]"),
            "table size as text": (lambda pin: pin["tables"][0].update(size_bytes="1"), "tables[0]"),
            "unknown track list field": (lambda pin: pin["track_list"].update({self.MARKER: 1}), "track_list"),
            "track list address as a list": (lambda pin: pin["track_list"].update(url=[self.MARKER]), "track_list"),
            "credit missing": (lambda pin: pin.pop("credit"), "credit"),
            "unknown credit field": (lambda pin: pin["credit"].update({self.MARKER: 1}), "credit"),
            "credit as text": (lambda pin: pin.update(credit=self.MARKER), "credit"),
        }
        for name, (change, label) in changes.items():
            with self.subTest(name):
                problems = self.changed(change)
                self.assertTrue(any(problem.startswith(label) for problem in problems), problems)
                self.assertNotIn(self.MARKER, " ".join(problems))


if __name__ == "__main__":
    unittest.main()
