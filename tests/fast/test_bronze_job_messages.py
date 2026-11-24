"""The Bronze jobs show no word of a command line, a reply or an exception on standard error, after the merge of the publish and verify commands.

The sweep's rule (``tests/fast/test_error_formatter.py``): a caught exception reaches a message only through ``describe_error``, and no message holds the value
of an argument. The two jobs that publish (``bronze_decode`` and ``bronze_clinical_lab``) raise ``PublicationError`` with a branch name or a Nessie content key
in its text, and ``bronze_decode`` also names a package of an import error and the branch in its last line. A marker in each place shows that none is printed.
"""

from __future__ import annotations

import contextlib
import importlib
import io
import unittest
from typing import Any
from unittest import mock

from scripts.stack.nessie import CONFLICT_TEXT, NessieConflict, NessieError
from spark.bronze.publication import PublicationError
from spark.bronze.quality import QualityError
from spark.bronze_clinical_lab_run import finish
from spark.jobs import bronze_clinical_lab
from tests.fast.fake_vitaldb import installed

MARKER = "marker_value_x9"

with installed():
    bronze_decode = importlib.import_module("spark.jobs.bronze_decode")  # it imports the publisher's library when it loads; pyspark is not in the type check


def run_main(module: Any, argv: list[str], *, raises: BaseException | None = None, returns: dict[str, Any] | None = None) -> tuple[int, str, str]:
    """The exit code and the two output texts of ``module.main`` when its work raises ``raises`` or returns ``returns``."""
    out, err = io.StringIO(), io.StringIO()
    work = "run_command" if module is bronze_decode else "run_job"
    with (
        mock.patch.object(module, "start_deadline"),
        mock.patch.dict("os.environ", {"NESSIE_URI": "http://nessie.invalid:19120/api/v2"}),
        mock.patch.object(module, work, side_effect=raises, return_value=returns),
        mock.patch.object(module, "NessieClient"),
        contextlib.redirect_stdout(out),
        contextlib.redirect_stderr(err),
    ):
        code = module.main(argv)
    return code, out.getvalue(), err.getvalue()


DECODE = ["write", "--branch", f"{MARKER}_branch", "--manifest-reference", f"{MARKER}.json"]
CLINICAL = ["verify", "--branch", f"{MARKER}_branch", "--source-hash", "a" * 64]


class DecodeMessageTest(unittest.TestCase):
    def test_a_refused_publication_or_a_failed_check_library_shows_the_type_name_and_no_text(self) -> None:
        for error in (PublicationError(f"{MARKER}_branch is not a branch"), QualityError(f"the check {MARKER} of {MARKER} failed")):
            with self.subTest(error=type(error).__name__):
                code, _, err = run_main(bronze_decode, DECODE, raises=error)
                self.assertEqual((1, f"bronze_decode: {type(error).__name__}\n"), (code, err))

    def test_a_nessie_conflict_shows_a_fixed_line_and_no_word_of_nessies_error_code(self) -> None:
        """Join round 1, finding 4: the error code is a value of Nessie's reply and is not checked, so no job prints it."""
        conflict = NessieConflict(f"POST /trees was refused: {MARKER}", status=409, error_code=f"{MARKER}_code")
        for module, argv in ((bronze_decode, DECODE), (bronze_clinical_lab, CLINICAL)):
            with self.subTest(job=module.JOB):
                code, out, err = run_main(module, argv, raises=conflict)
                self.assertEqual((1, "", f"{module.JOB}: {CONFLICT_TEXT}\n"), (code, out, err))

    def test_a_missing_package_is_a_fixed_line_that_names_no_package(self) -> None:
        code, _, err = run_main(bronze_decode, DECODE, raises=ImportError(MARKER, name=MARKER))
        self.assertEqual((2, "bronze_decode: the image does not hold a package that the checks need\n"), (code, err))

    def test_the_line_of_a_failed_check_names_neither_the_branch_nor_the_base(self) -> None:
        failed = {"passed": False, "checks": {"failed": [{"table": "tracks", "name": "key unique", "guard": "key", "unexpected": 2}]}, "publication": None}
        code, _, err = run_main(
            bronze_decode, ["write", "--branch", f"{MARKER}_branch", "--base", f"{MARKER}_base", "--manifest-reference", "r.json"], returns=failed
        )
        self.assertEqual(1, code)
        self.assertNotIn(MARKER, err)
        self.assertEqual("bronze_decode: the checks stopped the job and the base is unchanged: tracks: key unique (key guard, 2 unexpected)\n", err)


class ClinicalLabMessageTest(unittest.TestCase):
    def test_a_refused_publication_shows_the_type_name_and_no_text(self) -> None:
        code, _, err = run_main(bronze_clinical_lab, CLINICAL, raises=PublicationError(f"{MARKER}_branch is not a branch"))
        self.assertEqual((1, "bronze_clinical_lab: PublicationError\n"), (code, err))

    def test_a_nessie_failure_still_shows_the_type_name(self) -> None:
        code, _, err = run_main(bronze_clinical_lab, CLINICAL, raises=NessieError(MARKER))
        self.assertEqual((1, "bronze_clinical_lab: cannot work with Nessie: NessieError\n"), (code, err))

    def test_the_line_of_failed_table_checks_names_no_branch(self) -> None:
        result = {
            "passed": False,
            "tables": {"clinical": {"bronze_table": "clinical_information", "checks": [{"name": "row count", "passed": False, "detail": "1 of 2"}]}},
        }
        out, err = io.StringIO(), io.StringIO()
        self.assertEqual(1, finish(result, out, err))
        self.assertNotIn(MARKER, err.getvalue())
        self.assertEqual("bronze_clinical_lab: the table checks failed: clinical_information: row count (1 of 2)\n", err.getvalue())


if __name__ == "__main__":
    unittest.main()
