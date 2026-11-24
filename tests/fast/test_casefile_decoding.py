"""The shared case-file decoding helpers (``casefile/decoding.py``) that the replay's decoder and the Bronze decoder both call.

The helpers are pure code: the clinical table's times, a record time in whole microseconds and a waveform record's samples as a compact array. Each test pins
the behavior that the replay's decoder had before the helpers moved, so the move changes nothing.

Failure modes covered here:

- The clinical table is read leniently: a missing or repeated column, a case that does not start at 0, a value that is not whole seconds, a case listed twice,
  a wanted case that is absent or times that are out of order are accepted instead of refused by name.
- A record time is truncated or rounded the other way, so a numeric reading or a string event has another key in Bronze than in the replay.
- A waveform record's samples change on the way to an array: a raw integer is turned into a float, or a type that has no typed array is not kept as doubles.
- A job imports another job's package: the Bronze job imports the replay service, or the shared package imports a job's package, so an image has to copy the
  other image's files.
- The one definition is copied: Bronze and the replay each keep a copy of the tolerance, the track type codes or the helper, and the copies drift.
"""

from __future__ import annotations

import ast
import subprocess
import sys
import unittest
from array import array
from pathlib import Path

from casefile.decoding import (
    END_TOLERANCE_SECONDS,
    NUMERIC,
    STRING,
    WAVEFORM,
    DecodeError,
    clinical_times,
    samples_of,
    to_microseconds,
)
from features.sampling import ANONYMISED_START
from tests.fast.fake_vitaldb import FakeSamples

ROOT = Path(__file__).resolve().parents[2]
HEADER = "caseid,casestart,opstart,opend,caseend\n"


def table(*rows: str, header: str = HEADER) -> bytes:
    return (header + "".join(row + "\n" for row in rows)).encode()


class ClinicalTimes(unittest.TestCase):
    def test_each_wanted_case_has_its_times_in_microseconds_and_other_cases_are_left_out(self) -> None:
        found = clinical_times(table("1,0,100,200,300", "2,0,10,20,30", "3,0,1,2,3"), [1, 3])
        self.assertEqual([1, 3], sorted(found))
        self.assertEqual((100_000_000, 200_000_000, 300_000_000), (found[1].opstart_us, found[1].opend_us, found[1].caseend_us))
        self.assertEqual((1_000_000, 2_000_000, 3_000_000), (found[3].opstart_us, found[3].opend_us, found[3].caseend_us))

    def test_a_byte_order_mark_does_not_hide_the_first_column(self) -> None:
        self.assertEqual([1], list(clinical_times(b"\xef\xbb\xbf" + table("1,0,1,2,3"), [1])))

    def test_a_row_whose_case_number_is_not_a_number_is_not_a_wanted_case(self) -> None:
        self.assertEqual([1], list(clinical_times(table("x,0,1,2,3", "1,0,1,2,3"), [1])))

    def test_a_missing_or_repeated_column_is_refused_by_name(self) -> None:
        for name, header in (("missing", "caseid,casestart,opstart,opend\n"), ("repeated", "caseid,casestart,opstart,opend,caseend,caseend\n")):
            with self.subTest(name), self.assertRaisesRegex(DecodeError, "does not have each of the columns"):
                clinical_times(table(header=header), [1])

    def test_a_case_that_does_not_start_at_zero_is_refused(self) -> None:
        with self.assertRaisesRegex(DecodeError, "case 1: the case starts at 5 seconds, not at 0"):
            clinical_times(table("1,5,6,7,8"), [1])

    def test_a_value_that_is_not_a_whole_number_of_seconds_is_refused(self) -> None:
        for row in ("1,0,1.5,2,3", "1,0,,2,3", "1,0,a,2,3"):
            with self.subTest(row), self.assertRaisesRegex(DecodeError, "case 1: the clinical table has no whole-second value"):
                clinical_times(table(row), [1])

    def test_a_case_listed_twice_is_refused(self) -> None:
        with self.assertRaisesRegex(DecodeError, "case 1 is listed twice"):
            clinical_times(table("1,0,1,2,3", "1,0,1,2,3"), [1])

    def test_a_row_that_is_listed_twice_but_not_wanted_is_not_judged(self) -> None:
        self.assertEqual([1], list(clinical_times(table("2,0,1,2,3", "2,0,1,2,3", "1,0,1,2,3"), [1])))

    def test_a_wanted_case_with_no_row_is_refused_and_the_first_missing_case_is_named_with_the_count_of_the_rest(self) -> None:
        with self.assertRaisesRegex(DecodeError, "no row for case 5$"):
            clinical_times(table("1,0,1,2,3"), [1, 5])
        with self.assertRaisesRegex(DecodeError, "no row for case 5 and 1 more$"):
            clinical_times(table("1,0,1,2,3"), [1, 5, 9])

    def test_times_that_are_out_of_order_are_refused_with_the_event_model_reason(self) -> None:
        for row in ("1,0,5,4,9", "1,0,1,9,8", "1,0,0,0,0"):
            with self.subTest(row), self.assertRaisesRegex(DecodeError, "case 1: the times must satisfy"):
                clinical_times(table(row), [1])


class Microseconds(unittest.TestCase):
    def test_a_time_is_the_seconds_since_the_anonymised_start_in_whole_microseconds(self) -> None:
        self.assertEqual(0, to_microseconds(ANONYMISED_START))
        self.assertEqual(1_500_000, to_microseconds(ANONYMISED_START + 1.5))
        self.assertEqual(-2_000_000, to_microseconds(ANONYMISED_START - 2.0))

    def test_a_time_is_rounded_to_the_nearest_microsecond_and_never_truncated(self) -> None:
        # A double near the anonymised start holds steps of 2**-21 seconds (0.477 microseconds): 4 steps are 1.907 microseconds and 1 step is 0.477.
        step = 2.0**-21
        self.assertEqual(2, to_microseconds(ANONYMISED_START + 4 * step))
        self.assertEqual(0, to_microseconds(ANONYMISED_START + step))
        self.assertEqual(-2, to_microseconds(ANONYMISED_START - 4 * step))


class Samples(unittest.TestCase):
    def test_a_type_with_a_typed_array_of_the_same_size_keeps_its_raw_values(self) -> None:
        for char, size in (("h", 2), ("H", 2), ("f", 4), ("d", 8), ("b", 1), ("B", 1)):
            raw = FakeSamples(array(char, [1, 2, 3]).tobytes(), char, size)
            with self.subTest(char):
                result = samples_of(raw)
                self.assertEqual(char, result.typecode)  # type: ignore[attr-defined]
                self.assertEqual([1, 2, 3], list(result))

    def test_an_integer_sample_stays_an_integer_so_that_a_gap_marker_is_the_raw_value(self) -> None:
        raw = FakeSamples(array("H", [65535, 100]).tobytes(), "H", 2)
        self.assertEqual([65535, 100], list(samples_of(raw)))
        self.assertTrue(all(isinstance(value, int) for value in samples_of(raw)))

    def test_a_type_with_no_typed_array_is_kept_as_doubles(self) -> None:
        class Other(FakeSamples):
            def astype(self, name: str) -> FakeSamples:
                self.converted = name
                return FakeSamples(array("d", [1.0, 2.0]).tobytes(), "d", 8)

        raw = Other(b"\x00" * 4, "e", 2)
        result = samples_of(raw)
        self.assertEqual("float64", raw.converted)
        self.assertEqual(("d", [1.0, 2.0]), (result.typecode, list(result)))  # type: ignore[attr-defined]

    def test_a_type_whose_size_is_not_the_arrays_size_is_kept_as_doubles(self) -> None:
        class Wide(FakeSamples):
            def astype(self, name: str) -> FakeSamples:
                return FakeSamples(array("d", [7.0]).tobytes(), "d", 8)

        result = samples_of(Wide(b"\x00" * 4, "H", 4))
        self.assertEqual(("d", [7.0]), (result.typecode, list(result)))  # type: ignore[attr-defined]


class OneDefinition(unittest.TestCase):
    def test_the_track_type_codes_and_the_end_tolerance_are_the_formats_own_values(self) -> None:
        self.assertEqual((1, 2, 5), (WAVEFORM, NUMERIC, STRING))
        self.assertEqual(2.0, END_TOLERANCE_SECONDS)

    def test_bronze_and_the_replay_hold_the_shared_definitions_and_no_copy(self) -> None:
        # The Bronze modules and the replay's decoder are read as text (``ast``): the library and the Spark session are not needed to know what they define.
        for path, banned in (
            ("spark/bronze/structure.py", {"END_TOLERANCE_SECONDS"}),
            (
                "replay/service/decode.py",
                {"END_TOLERANCE_SECONDS", "WAVEFORM", "NUMERIC", "STRING", "TYPECODES", "to_microseconds", "samples_of", "clinical_times"},
            ),
            ("spark/bronze/library.py", {"ANONYMISED_START", "samples_of", "to_microseconds"}),
        ):
            tree = ast.parse((ROOT / path).read_text())
            defined = {node.name for node in tree.body if isinstance(node, ast.FunctionDef)}
            defined |= {target.id for node in tree.body if isinstance(node, ast.Assign) for target in node.targets if isinstance(target, ast.Name)}
            defined |= {node.target.id for node in tree.body if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)}
            defined |= {
                name
                for node in tree.body
                if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Tuple)
                for name in ast.unparse(node.targets[0]).split(", ")
            }
            with self.subTest(path):
                self.assertEqual(set(), defined & banned)


def imports_of(path: Path) -> set[str]:
    """Every module that a file imports, at any depth of the file (an import inside a function counts)."""
    names: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            names |= {alias.name for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names |= {node.module, *(f"{node.module}.{alias.name}" for alias in node.names)}
    return names


class Boundaries(unittest.TestCase):
    """Neither image copies the other's package: each job imports the shared package, and the shared package imports no job."""

    def test_the_shared_package_imports_no_job_package_and_no_replay_service_module(self) -> None:
        for path in sorted((ROOT / "casefile").glob("*.py")):
            for name in imports_of(path):
                with self.subTest(file=path.name, module=name):
                    self.assertFalse(name.split(".")[0] in {"spark", "orchestration", "tests"} or name.startswith("replay.service"), name)

    def test_the_bronze_decoder_imports_nothing_from_the_replay_service(self) -> None:
        for path in sorted((ROOT / "spark" / "bronze").glob("*.py")) + [ROOT / "spark" / "jobs" / "bronze_decode.py"]:
            for name in imports_of(path):
                with self.subTest(file=path.name, module=name):
                    self.assertFalse(name == "replay.service" or name.startswith("replay.service."), name)

    def test_the_replay_service_imports_nothing_from_spark(self) -> None:
        for path in sorted((ROOT / "replay").rglob("*.py")):
            for name in imports_of(path):
                with self.subTest(file=path.name, module=name):
                    self.assertFalse(name == "spark" or name.startswith("spark."), name)

    def test_the_bronze_job_and_its_stores_import_without_the_publishers_library(self) -> None:
        """The Bronze modules import with no ``vitaldb`` and no ``boto3`` in the process: the replay's decoder module, which imports the library, is not a
        dependency of the Bronze job any more (a bounded child process, so that the fast tests' own stand-in does not hide a dependency)."""
        code = (
            "import sys; sys.path.insert(0, sys.argv[1]); import spark.bronze.library, spark.bronze.readers, casefile.decoding; "
            "sys.exit(7 if {'vitaldb', 'boto3'} & set(sys.modules) else 0)"
        )
        done = subprocess.run([sys.executable, "-I", "-c", code, str(ROOT)], capture_output=True, text=True, timeout=120, check=False)  # noqa: S603
        self.assertEqual((0, ""), (done.returncode, done.stderr), done.stderr)


if __name__ == "__main__":
    unittest.main()
