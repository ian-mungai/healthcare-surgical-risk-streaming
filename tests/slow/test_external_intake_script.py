"""The intake script on the host writes one external secret into OpenBao when its path is empty, and says so in one code.

The script is run as the role installs it: beside its helper, in the host's interpreter in isolated mode. The server is the stand-in of
``tests/slow/openbao_stand_in.py``. The values are synthetic markers in the form of the check, and each case looks for them in every output.
"""

from __future__ import annotations

import hashlib
import importlib.machinery
import importlib.util
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import ModuleType
from unittest import mock

from scripts.process import run_command
from scripts.project_config import ROOT
from tests.fast.ansible_paths import SHARED_MODULE_UTILS
from tests.fast.external_intake_constants import NAME, SCRIPT, VALUE
from tests.slow.openbao_stand_in import Role, StandIn

LIST = ROOT / "compose" / "external_secrets.txt"
OTHER = "dckr_pat_" + "AnotherMarkerThatMustNotLeak2"
ADMIN_MARKER = "admin-secret-id-marker"
INTAKE_ROLE = "external_intake"
INTAKE_MARKER = "intake-secret-id-marker"
INTAKE_FILE_NAME = "external_intake.secret_id"
PATH = f"secret/data/external/{NAME}"
INTAKE_WRITES = ("secret/data/external/*",)


def intake_role() -> Role:
    """The intake script's own role, which the reconcile step writes: it reads and writes the external paths and nothing else."""
    return Role(INTAKE_MARKER, "intake-token-marker", INTAKE_WRITES, INTAKE_WRITES)


class Case(unittest.TestCase):
    def setUp(self) -> None:
        self.folder = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.folder, True)
        bin_folder = self.folder / "bin"
        bin_folder.mkdir()
        for name in ("external_secrets.py",):
            shutil.copy(SHARED_MODULE_UTILS / name, bin_folder / name)
        self.script = bin_folder / "external_secret_intake"
        shutil.copy(SCRIPT, self.script)
        self.script.chmod(0o700)
        (self.folder / "secrets" / "openbao").mkdir(parents=True)
        (self.folder / "secrets" / "openbao" / "external_intake.secret_id").write_text(INTAKE_MARKER)
        self.server = self.make_server()
        self.server.__enter__()
        self.addCleanup(self.server.__exit__)

    def make_server(self) -> StandIn:
        return StandIn({INTAKE_ROLE: intake_role()})

    def run_script(self, document: object, *, raw: bytes | None = None, server: StandIn | None = None) -> tuple[int, str, str]:
        """Run the script's main in this process on the document as standard input, and return the exit code, the standard output and the standard error."""
        data = raw if raw is not None else json.dumps(document).encode()
        arguments = [
            "--directory",
            str(self.folder / "secrets"),
            "--list",
            str(LIST),
            "--address",
            (server or self.server).address,
        ]
        out, error = io.StringIO(), io.StringIO()
        module = self.load()
        with mock.patch.object(sys, "stdin", mock.Mock(buffer=io.BytesIO(data))), redirect_stdout(out), redirect_stderr(error):
            code = module.main(arguments)
        return code, out.getvalue(), error.getvalue()

    def load(self) -> ModuleType:
        loader = importlib.machinery.SourceFileLoader("external_secret_intake", str(self.script))
        spec = importlib.util.spec_from_file_location("external_secret_intake", self.script, loader=loader)
        if spec is None or spec.loader is None:
            raise ImportError(str(self.script))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


class StatusTest(Case):
    def test_an_empty_path_is_reported_empty_and_nothing_is_written(self) -> None:
        self.assertEqual((0, "empty\n", ""), self.run_script({"name": NAME, "value": None}))
        self.assertEqual(0, self.server.writes())

    def test_a_filled_path_is_reported_present(self) -> None:
        self.run_script({"name": NAME, "value": VALUE})
        self.assertEqual((0, "present\n", ""), self.run_script({"name": NAME, "value": None}))

    def test_a_stored_value_that_is_not_in_its_form_is_refused_by_a_code(self) -> None:
        self.server.versions[f"external/{NAME}"] = ["not a token"]
        self.assertEqual((3, "stored_bad_form\n", ""), self.run_script({"name": NAME, "value": None}))


class WriteTest(Case):
    def test_an_empty_path_gets_the_value_once_and_the_script_prints_one_code(self) -> None:
        self.assertEqual((0, "written\n", ""), self.run_script({"name": NAME, "value": VALUE}))
        self.assertEqual(VALUE, self.server.stored(f"external/{NAME}"))
        self.assertEqual(1, self.server.writes())

    def test_a_second_run_writes_nothing_and_keeps_the_first_value(self) -> None:
        self.run_script({"name": NAME, "value": VALUE})
        self.assertEqual((0, "unchanged\n", ""), self.run_script({"name": NAME, "value": OTHER}))
        self.assertEqual(VALUE, self.server.stored(f"external/{NAME}"))
        self.assertEqual(1, self.server.writes())

    def test_the_value_is_read_back_and_compared_by_its_hash_and_a_different_stored_text_is_removed_and_refused(self) -> None:
        self.server.tamper_next_write(OTHER)
        self.assertEqual((3, "mismatch\n", ""), self.run_script({"name": NAME, "value": VALUE}))
        self.assertIsNone(self.server.stored(f"external/{NAME}"))
        self.assertEqual((0, "written\n", ""), self.run_script({"name": NAME, "value": VALUE}))
        self.assertEqual(VALUE, self.server.stored(f"external/{NAME}"))

    def test_a_write_that_the_server_answers_with_a_status_other_than_200_is_a_code_and_stores_nothing(self) -> None:
        for status in (500, 204):
            with self.subTest(status=status):
                self.server.fail_next("POST", "/v1/secret/data/", status, "answer")
                self.assertEqual((3, "cannot_write\n", ""), self.run_script({"name": NAME, "value": VALUE}))
                self.assertIsNone(self.server.stored(f"external/{NAME}"))

    def test_a_write_that_never_arrived_completes_on_the_next_run(self) -> None:
        self.server.drop_next("POST", "/v1/secret/data/")
        code, out, _ = self.run_script({"name": NAME, "value": VALUE})
        self.assertEqual((3, "no_answer\n"), (code, out))
        self.assertIsNone(self.server.stored(f"external/{NAME}"))
        self.assertEqual((0, "written\n", ""), self.run_script({"name": NAME, "value": VALUE}))
        self.assertEqual(VALUE, self.server.stored(f"external/{NAME}"))

    def test_a_write_whose_read_back_was_lost_leaves_the_whole_value_and_the_next_run_finds_it_present(self) -> None:
        self.server.drop_next("GET", "/v1/secret/data/", after=1)
        self.assertEqual((3, "no_answer\n", ""), self.run_script({"name": NAME, "value": VALUE}))
        self.assertEqual(VALUE, self.server.stored(f"external/{NAME}"))
        self.assertEqual((0, "unchanged\n", ""), self.run_script({"name": NAME, "value": VALUE}))
        self.assertEqual(1, self.server.writes())

    def test_a_value_that_is_not_in_the_form_is_refused_before_the_server_is_called(self) -> None:
        for value in ("", "dckr_pat_short", VALUE + "\n", VALUE + " x", "ghp_" + "a" * 30):
            with self.subTest(value=value[:12]):
                self.assertEqual((3, "bad_form\n", ""), self.run_script({"name": NAME, "value": value}))
        self.assertEqual([], self.server.calls)

    def test_a_name_that_the_list_does_not_declare_is_refused_before_the_server_is_called(self) -> None:
        self.assertEqual((3, "unknown_name\n", ""), self.run_script({"name": "other", "value": VALUE}))
        self.assertEqual([], self.server.calls)

    def test_a_document_that_is_not_exactly_the_name_and_the_value_is_refused_by_a_code(self) -> None:
        cases = {
            "not_json": b"not json",
            "bad_keys": json.dumps({"name": NAME}).encode(),
            "oversized": b" " * 5000,
        }
        cases["bad_keys_extra"] = json.dumps({"name": NAME, "value": VALUE, "more": 1}).encode()
        cases["not_json_list"] = b"[1]"
        cases["bad_type"] = json.dumps({"name": NAME, "value": 5}).encode()
        expected = {
            "not_json": "not_json",
            "bad_keys": "bad_keys",
            "oversized": "oversized",
            "bad_keys_extra": "bad_keys",
            "not_json_list": "not_json",
            "bad_type": "bad_form",
        }
        for label, raw in cases.items():
            with self.subTest(case=label):
                self.assertEqual((3, f"{expected[label]}\n", ""), self.run_script(None, raw=raw))
        self.assertEqual([], self.server.calls)

    def test_a_login_that_is_refused_and_a_policy_that_forbids_the_write_have_their_own_codes(self) -> None:
        (self.folder / "secrets" / "openbao" / "external_intake.secret_id").write_text("wrong")
        self.assertEqual((3, "no_login\n", ""), self.run_script({"name": NAME, "value": VALUE}))
        (self.folder / "secrets" / "openbao" / "external_intake.secret_id").write_text(INTAKE_MARKER)
        self.server.roles[INTAKE_ROLE].writable = ()
        self.assertEqual((3, "not_allowed\n", ""), self.run_script({"name": NAME, "value": VALUE}))
        self.assertIsNone(self.server.stored(f"external/{NAME}"))

    def test_no_server_at_the_address_is_a_code_and_not_an_error_text(self) -> None:
        with StandIn({}) as dead:
            address = dead.address
        arguments = ["--directory", str(self.folder / "secrets"), "--list", str(LIST), "--address", address]
        out = io.StringIO()
        with mock.patch.object(sys, "stdin", mock.Mock(buffer=io.BytesIO(json.dumps({"name": NAME, "value": VALUE}).encode()))), redirect_stdout(out):
            code = self.load().main(arguments)
        self.assertEqual((3, "no_answer\n"), (code, out.getvalue()))


def digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


class DigestTest(Case):
    """The value and its SHA-256 are stored in one check-and-set write, every read checks them, and a cleanup is reported only after a read confirms it."""

    PATH = f"external/{NAME}"

    def test_the_digest_goes_in_the_same_write_as_the_value(self) -> None:
        self.assertEqual((0, "written\n", ""), self.run_script({"name": NAME, "value": VALUE}))
        self.assertEqual(1, self.server.writes())
        self.assertEqual({0: digest(VALUE)}, self.server.digests[self.PATH])

    def test_a_stored_text_that_does_not_match_its_digest_is_refused_by_a_code_and_is_never_present_or_unchanged(self) -> None:
        self.server.versions[self.PATH] = [VALUE]
        self.server.digests[self.PATH] = {0: digest(OTHER)}
        for document in ({"name": NAME, "value": None}, {"name": NAME, "value": OTHER}):
            with self.subTest(sent=document["value"] is not None):
                self.assertEqual((3, "stored_mismatch\n", ""), self.run_script(document))
        self.assertEqual(0, self.server.writes())

    def test_a_stored_text_with_no_digest_is_refused_the_same_way(self) -> None:
        self.server.versions[self.PATH] = [VALUE]
        self.server.bare.add(self.PATH)
        self.assertEqual((3, "stored_mismatch\n", ""), self.run_script({"name": NAME, "value": None}))

    def test_a_read_back_that_differs_removes_the_version_and_says_so_only_when_a_read_shows_the_path_empty(self) -> None:
        self.server.tamper_next_write(OTHER)
        self.assertEqual((3, "mismatch\n", ""), self.run_script({"name": NAME, "value": VALUE}))
        self.assertIsNone(self.server.stored(self.PATH))

    def test_a_read_back_whose_digest_differs_from_the_sent_one_is_the_same_mismatch_even_when_the_text_is_right(self) -> None:
        self.server.tamper_next_digest(digest(OTHER))
        self.assertEqual((3, "mismatch\n", ""), self.run_script({"name": NAME, "value": VALUE}))
        self.assertIsNone(self.server.stored(self.PATH))

    def test_a_removal_that_the_server_answers_but_does_not_do_is_not_reported_as_done(self) -> None:
        self.server.tamper_next_write(OTHER)
        self.server.ignore_deletes = True
        self.assertEqual((3, "mismatch_kept\n", ""), self.run_script({"name": NAME, "value": VALUE}))
        self.assertEqual(OTHER, self.server.stored(self.PATH))
        self.assertEqual((3, "stored_mismatch\n", ""), self.run_script({"name": NAME, "value": None}), "the kept version is never read as present")

    def test_a_removal_whose_call_never_arrived_is_not_reported_as_done(self) -> None:
        self.server.tamper_next_write(OTHER)
        self.server.drop_next("DELETE", "/v1/secret/data/")
        self.assertEqual((3, "mismatch_kept\n", ""), self.run_script({"name": NAME, "value": VALUE}))
        self.assertEqual(OTHER, self.server.stored(self.PATH))


class IsolatedRunTest(Case):
    def test_the_installed_script_runs_in_isolated_mode_reads_the_value_from_standard_input_only_and_prints_no_marker(self) -> None:
        arguments = [sys.executable, "-I", str(self.script), "--directory", str(self.folder / "secrets"), "--list", str(LIST), "--address", self.server.address]
        result = run_command(arguments[0], arguments[1:], input_text=json.dumps({"name": NAME, "value": VALUE}), timeout=30, env={})
        self.assertEqual((0, "written\n", ""), (result.returncode, result.stdout, result.stderr))
        for marker in (VALUE, ADMIN_MARKER):
            self.assertNotIn(marker, " ".join(arguments))
        self.assertEqual(VALUE, self.server.stored(f"external/{NAME}"))
        self.assertNotIn("VALUE", os.environ)


if __name__ == "__main__":
    unittest.main()
