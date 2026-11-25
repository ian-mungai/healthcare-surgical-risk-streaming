"""The Bronze decode job shows no word of a command line, a reply or an exception on standard error, after the merge of the publish and verify commands.

The sweep's rule (``tests/fast/test_error_formatter.py``): a caught exception reaches a message only through ``describe_error``, and no message holds the value
of an argument. ``bronze_decode`` raises ``PublicationError`` with a branch name or a Nessie content key in its text, and it also names a package of an import
error and the branch in its last line. A marker in each place shows that none is printed. The Bronze clinical and laboratory job is sealed (decision D): it
prints fixed codes only, and ``tests/fast/test_bronze_clinical_cli.py`` pins that for every ending, so its cases are no longer here.
"""

from __future__ import annotations

import contextlib
import importlib
import io
import unittest
from typing import Any
from unittest import mock

from scripts.stack.nessie import CONFLICT_TEXT, NessieConflict
from spark.bronze.publication import PublicationError
from spark.bronze.quality import QualityError
from tests.fast.fake_vitaldb import installed

MARKER = "marker_value_x9"

with installed():
    bronze_decode = importlib.import_module("spark.jobs.bronze_decode")  # it imports the publisher's library when it loads; pyspark is not in the type check


def run_main(module: Any, argv: list[str], *, raises: BaseException | None = None, returns: dict[str, Any] | None = None) -> tuple[int, str, str]:
    """The exit code and the two output texts of ``module.main`` when its work raises ``raises`` or returns ``returns``."""
    out, err = io.StringIO(), io.StringIO()
    with (
        mock.patch.object(module, "start_deadline"),
        mock.patch.dict("os.environ", {"NESSIE_URI": "http://nessie.invalid:19120/api/v2"}),
        mock.patch.object(module, "run_command", side_effect=raises, return_value=returns),
        mock.patch.object(module, "NessieClient"),
        contextlib.redirect_stdout(out),
        contextlib.redirect_stderr(err),
    ):
        code = module.main(argv)
    return code, out.getvalue(), err.getvalue()


DECODE = ["write", "--branch", f"{MARKER}_branch", "--manifest-reference", f"{MARKER}.json"]


class DecodeMessageTest(unittest.TestCase):
    def test_a_refused_publication_or_a_failed_check_library_shows_the_type_name_and_no_text(self) -> None:
        for error in (PublicationError(f"{MARKER}_branch is not a branch"), QualityError(f"the check {MARKER} of {MARKER} failed")):
            with self.subTest(error=type(error).__name__):
                code, _, err = run_main(bronze_decode, DECODE, raises=error)
                self.assertEqual((1, f"bronze_decode: {type(error).__name__}\n"), (code, err))

    def test_a_nessie_conflict_shows_a_fixed_line_and_no_word_of_nessies_error_code(self) -> None:
        """Join round 1, finding 4: the error code is a value of Nessie's reply and is not checked, so no job prints it."""
        conflict = NessieConflict(f"POST /trees was refused: {MARKER}", status=409, error_code=f"{MARKER}_code")
        code, out, err = run_main(bronze_decode, DECODE, raises=conflict)
        self.assertEqual((1, "", f"{bronze_decode.JOB}: {CONFLICT_TEXT}\n"), (code, out, err))

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


if __name__ == "__main__":
    unittest.main()
