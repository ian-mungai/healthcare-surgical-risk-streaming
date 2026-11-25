"""The two steps of the OpenBao module that move a secret between OpenBao and the host.

``read`` returns an external secret from OpenBao to the playbook for its consumer, which hands it to one task's standard input (the pull of the images reads
the Docker Hub token that way) and writes no file. ``generated`` keeps a secret that the host makes for itself (the Grafana administrator password): made once,
stored in OpenBao while its path is empty, and written from OpenBao to the file that its container reads. The server is the stand-in of
``tests/fast/openbao_stand_in.py``; the values are markers.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import stat
import tempfile
import unittest
from pathlib import Path
from typing import Any

from tests.fast.openbao_stand_in import ADMIN_ROLE, Role, StandIn
from tests.fast.test_external_intake_script import ADMIN_MARKER, LIST, NAME, VALUE
from tests.fast.test_openbao_bootstrap import MY_OWNER, bootstrap

DEPLOY_MARKER = "deploy-login-marker-that-must-not-leak"
NESSIE_MARKER = "nessie-login-marker-that-must-not-leak"
GRAFANA_NAME = "grafana/admin_password"
OWNER_TEXT = f"{MY_OWNER[0]}:{MY_OWNER[1]}"


class StepCase(unittest.TestCase):
    def setUp(self) -> None:
        self.folder = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.folder, True)
        self.secrets = self.folder / "secrets"
        (self.secrets / "openbao").mkdir(parents=True)
        for role, marker in (("bootstrap", ADMIN_MARKER), ("deploy", DEPLOY_MARKER), ("nessie", NESSIE_MARKER)):
            (self.secrets / "openbao" / f"{role}.secret_id" if role != "bootstrap" else self.secrets / "openbao" / "bootstrap.secret_id").write_text(marker)
        self.server = StandIn(
            {
                ADMIN_ROLE: Role(
                    ADMIN_MARKER, "admin-token", ("secret/data/services/*", "secret/data/external/*"), ("secret/data/services/*", "secret/data/external/*")
                ),
                "deploy": Role(DEPLOY_MARKER, "deploy-token", (f"secret/data/external/{NAME}",)),
                "nessie": Role(NESSIE_MARKER, "nessie-token", ("secret/data/services/nessie/*",)),
            }
        )
        self.server.__enter__()
        self.addCleanup(self.server.__exit__)

    def read(self, *, consumer: str = "deploy", name: str = NAME) -> dict[str, Any]:
        parameters = {
            "secrets_directory": str(self.secrets),
            "external_secrets_file": str(LIST),
            "address": self.server.address,
            "consumer": consumer,
            "name": name,
        }
        return dict(bootstrap.read_external(parameters))

    def generated(self, destination: Path, *, check_only: bool = False) -> dict[str, Any]:
        parameters = {
            "secrets_directory": str(self.secrets),
            "address": self.server.address,
            "name": GRAFANA_NAME,
            "dest": str(destination),
            "file_owner": OWNER_TEXT,
        }
        return dict(bootstrap.generated_secret(parameters, check_only=check_only))


class ReadTest(StepCase):
    def test_the_consumer_gets_the_value_in_the_result_and_the_step_changes_and_writes_nothing(self) -> None:
        self.server.versions[f"external/{NAME}"] = [VALUE]
        before = sorted(path.name for path in self.folder.rglob("*"))
        result = self.read()
        self.assertEqual({"changed": False, "value": VALUE}, result)
        self.assertEqual(before, sorted(path.name for path in self.folder.rglob("*")), "no file of any name was made")
        self.assertEqual(0, self.server.writes())

    def test_the_value_is_the_text_that_is_stored_now(self) -> None:
        self.server.versions[f"external/{NAME}"] = [VALUE]
        self.assertEqual(VALUE, self.read()["value"])
        newer = "dckr_pat_" + "ReplacedMarkerValue0123456"
        self.server.versions[f"external/{NAME}"].append(newer)
        self.assertEqual(newer, self.read()["value"])

    def test_a_refused_login_names_the_role_and_the_status_and_gives_no_advice_to_reset_the_data(self) -> None:
        (self.secrets / "openbao" / "deploy.secret_id").write_text("a-secret-id-that-the-server-does-not-know")
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            self.read()
        text = str(caught.exception)
        self.assertIn("the login of role deploy was refused (HTTP 400)", text)
        for advice in ("reset", "volume"):
            self.assertNotIn(advice, text)
        self.assertNotIn("a-secret-id-that-the-server-does-not-know", text)

    def test_a_secret_of_another_consumer_is_refused_before_any_call(self) -> None:
        self.server.versions[f"external/{NAME}"] = [VALUE]
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            self.read(consumer="nessie")
        self.assertIn(NAME, str(caught.exception))
        self.assertEqual([], self.server.calls)

    def test_a_policy_that_does_not_let_the_consumer_read_is_a_server_refusal_that_holds_no_value(self) -> None:
        self.server.versions[f"external/{NAME}"] = [VALUE]
        self.server.roles["deploy"].readable = ()
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            self.read()
        self.assertNotIn(VALUE, str(caught.exception))

    def test_an_empty_path_is_refused_by_name_with_the_way_to_fill_it(self) -> None:
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            self.read()
        self.assertIn(NAME, str(caught.exception))
        self.assertIn("intake", str(caught.exception))

    def test_a_stored_value_that_fails_its_form_is_refused_and_no_error_holds_it(self) -> None:
        for stored in ("not a token", VALUE + "\n", ""):
            self.server.versions[f"external/{NAME}"] = [stored]
            with self.subTest(stored=stored[:8]), self.assertRaises(bootstrap.BootstrapError) as caught:
                self.read()
            if stored.strip():
                self.assertNotIn(stored.strip(), str(caught.exception))

    def test_a_stored_text_that_does_not_match_its_digest_or_has_none_is_refused(self) -> None:
        for label in ("another digest", "no digest"):
            self.server.versions[f"external/{NAME}"] = [VALUE]
            self.server.bare.discard(f"external/{NAME}")
            if label == "no digest":
                self.server.bare.add(f"external/{NAME}")
            else:
                self.server.digests[f"external/{NAME}"] = {0: "0" * 64}
            with self.subTest(label), self.assertRaises(bootstrap.BootstrapError) as caught:
                self.read()
            self.assertNotIn(VALUE, str(caught.exception))


class GeneratedTest(StepCase):
    def test_a_new_secret_is_made_once_stored_in_openbao_and_written_to_the_file_for_its_owner(self) -> None:
        target = self.folder / "admin.password"
        result = self.generated(target)
        stored = self.server.stored(f"services/{GRAFANA_NAME}")
        self.assertTrue(stored)
        self.assertEqual(stored, target.read_text())
        self.assertEqual(0o400, stat.S_IMODE(target.stat().st_mode))
        self.assertTrue(result["changed"])
        self.assertNotIn(stored, repr(result))
        self.assertGreaterEqual(len(stored or ""), 32)

    def test_a_second_run_changes_nothing_and_writes_nothing_to_openbao(self) -> None:
        target = self.folder / "admin.password"
        self.generated(target)
        before = (target.read_text(), self.server.writes())
        self.assertFalse(self.generated(target)["changed"])
        self.assertEqual(before, (target.read_text(), self.server.writes()))

    def test_a_file_that_exists_while_openbao_is_empty_is_adopted_and_not_replaced(self) -> None:
        target = self.folder / "admin.password"
        target.write_text("password-of-an-older-host-marker\n")
        os.chmod(target, 0o400)
        self.generated(target)
        self.assertEqual("password-of-an-older-host-marker", self.server.stored(f"services/{GRAFANA_NAME}"))
        self.assertEqual("password-of-an-older-host-marker\n", target.read_text())

    def test_a_file_with_the_stored_text_and_a_looser_mode_is_given_mode_0400(self) -> None:
        self.server.versions[f"services/{GRAFANA_NAME}"] = ["stored-password-marker"]
        target = self.folder / "admin.password"
        target.write_text("stored-password-marker")
        target.chmod(0o644)
        self.assertTrue(self.generated(target)["changed"])
        self.assertEqual((0o400, "stored-password-marker"), (stat.S_IMODE(target.stat().st_mode), target.read_text()))

    def test_a_password_file_that_starts_or_ends_with_white_space_is_refused_by_name_while_openbao_is_empty(self) -> None:
        target = self.folder / "grafana" / "admin.password"
        target.parent.mkdir()
        target.write_text(" leading-space-marker")
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            self.generated(target)
        self.assertIn("grafana/admin.password", str(caught.exception))
        self.assertNotIn("leading-space-marker", str(caught.exception))
        self.assertIsNone(self.server.stored(f"services/{GRAFANA_NAME}"))

    def test_a_password_file_with_no_byte_is_refused_by_name_like_any_empty_file_and_nothing_is_made_or_stored(self) -> None:
        target = self.folder / "grafana" / "admin.password"
        target.parent.mkdir()
        target.write_text("")
        with self.assertRaises(bootstrap.SecretFileError) as caught:
            self.generated(target)
        self.assertIn("admin.password", str(caught.exception))
        self.assertIn("empty", str(caught.exception))
        self.assertIsNone(self.server.stored(f"services/{GRAFANA_NAME}"))
        self.assertEqual("", target.read_text())

    def test_a_missing_file_is_written_again_from_openbao_and_the_stored_secret_is_kept(self) -> None:
        self.server.versions[f"services/{GRAFANA_NAME}"] = ["stored-password-marker"]
        target = self.folder / "admin.password"
        self.assertTrue(self.generated(target)["changed"])
        self.assertEqual("stored-password-marker", target.read_text())
        self.assertEqual(0, self.server.writes())

    def test_a_file_that_differs_from_openbao_is_replaced_by_openbao_s_value(self) -> None:
        self.server.versions[f"services/{GRAFANA_NAME}"] = ["stored-password-marker"]
        target = self.folder / "admin.password"
        target.write_text("someone-edited-the-file")
        os.chmod(target, 0o400)
        self.generated(target)
        self.assertEqual("stored-password-marker", target.read_text())

    def test_a_write_to_openbao_that_never_arrived_leaves_no_file_and_the_next_run_completes(self) -> None:
        target = self.folder / "admin.password"
        self.server.drop_next("POST", "/v1/secret/data/")
        with self.assertRaises(bootstrap.BootstrapError):
            self.generated(target)
        self.assertFalse(target.exists())
        self.generated(target)
        self.assertEqual(self.server.stored(f"services/{GRAFANA_NAME}"), target.read_text())

    def test_a_text_that_openbao_stored_differently_from_the_one_sent_is_an_error_and_no_file_is_written(self) -> None:
        target = self.folder / "admin.password"
        self.server.tamper_next_write("another-text-marker")
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            self.generated(target)
        self.assertNotIn("another-text-marker", str(caught.exception))
        self.assertFalse(target.exists())

    def test_the_digest_goes_in_the_same_write_as_the_generated_value(self) -> None:
        target = self.folder / "admin.password"
        self.generated(target)
        stored = self.server.stored(f"services/{GRAFANA_NAME}") or ""
        self.assertEqual(1, self.server.writes())
        self.assertEqual({0: hashlib.sha256(stored.encode()).hexdigest()}, self.server.digests[f"services/{GRAFANA_NAME}"])

    def test_a_stored_text_that_does_not_match_its_digest_or_has_none_is_an_error_that_is_never_adopted_and_no_file_is_written(self) -> None:
        for label in ("another digest", "no digest"):
            path = f"services/{GRAFANA_NAME}"
            self.server.versions[path] = ["stored-password-marker"]
            self.server.bare.discard(path)
            if label == "no digest":
                self.server.bare.add(path)
            else:
                self.server.digests[path] = {0: "0" * 64}
            target = self.folder / "admin.password"
            with self.subTest(label), self.assertRaises(bootstrap.BootstrapError) as caught:
                self.generated(target)
            self.assertNotIn("stored-password-marker", str(caught.exception))
            self.assertFalse(target.exists())

    def test_a_server_error_that_repeats_the_generated_value_shows_no_value(self) -> None:
        target = self.folder / "admin.password"
        target.write_text("password-of-an-older-host-marker\n")
        self.server.fail_next("POST", "/v1/secret/data/services/", 400, "invalid value password-of-an-older-host-marker")
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            self.generated(target)
        self.assertIn("HTTP 400", str(caught.exception))
        self.assertNotIn("password-of-an-older-host-marker", str(caught.exception))

    def test_a_check_run_makes_nothing(self) -> None:
        target = self.folder / "admin.password"
        self.assertTrue(self.generated(target, check_only=True)["changed"])
        self.assertFalse(target.exists())
        self.assertIsNone(self.server.stored(f"services/{GRAFANA_NAME}"))


if __name__ == "__main__":
    unittest.main()
