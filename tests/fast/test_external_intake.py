"""The launcher's intake gives the host's OpenBao each declared external secret that is still empty there, from the source that .env names.

The host's side is the real intake script run against the stand-in server, so a case covers the whole path from the source to the stored value.
Both source kinds are covered (the owner's local OpenBao and the hidden prompt).
The values are synthetic markers; each case looks for them in every output and every error text.
"""

from __future__ import annotations

import io
import json
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from typing import Any
from unittest import mock

from scripts.infra import external_intake
from scripts.infra.external_intake import Ask, IntakeError, Source
from tests.fast.ansible_paths import shared_module
from tests.fast.openbao_stand_in import Role, StandIn
from tests.fast.test_external_intake_script import ADMIN_MARKER, LIST, NAME, OTHER, VALUE, Case

OWNER_MARKER = "owner-login-marker-that-must-not-leak"


def declared() -> list[Any]:
    return list(shared_module("external_secrets").parse_external(LIST.read_text()))


def answering(answer: str) -> Source:
    return external_intake.prompt_source(lambda _: answer, is_terminal=lambda: True)


def replying(reply: tuple[int, str]) -> Ask:
    return lambda _: reply


class IntakeCase(Case):
    def setUp(self) -> None:
        super().setUp()
        self.sent: list[str] = []

    def ask_host(self, document: str) -> tuple[int, str]:
        self.sent.append(document)
        code, out, _ = self.run_script(None, raw=document.encode())
        return code, out

    def intake(self, source: Source) -> str:
        out, error = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(error):
            external_intake.run_intake(declared(), self.ask_host, source, external_intake.write_line)
        return out.getvalue() + error.getvalue()


class PromptSourceTest(IntakeCase):
    def test_an_empty_host_gets_the_value_from_the_prompt_once_and_a_second_run_asks_for_nothing(self) -> None:
        prompt = mock.Mock(return_value=VALUE)
        source = external_intake.prompt_source(prompt, is_terminal=lambda: True)
        printed = self.intake(source)
        self.assertEqual(VALUE, self.server.stored(f"external/{NAME}"))
        self.assertEqual(1, prompt.call_count)
        self.assertNotIn(VALUE, printed)
        again = self.intake(source)
        self.assertEqual(1, prompt.call_count)
        self.assertEqual(1, self.server.writes())
        self.assertNotIn(VALUE, again)
        self.assertIn(NAME, again)

    def test_the_value_goes_only_in_the_document_to_the_host_and_never_in_an_output(self) -> None:
        printed = self.intake(external_intake.prompt_source(lambda _: f"  {VALUE}  ", is_terminal=lambda: True))
        self.assertEqual({"name": NAME, "value": VALUE}, json.loads(self.sent[1]))
        self.assertEqual({"name": NAME, "value": None}, json.loads(self.sent[0]))
        self.assertNotIn(VALUE, printed)

    def test_no_terminal_is_a_fixed_refusal_that_names_the_secret_and_writes_nothing(self) -> None:
        prompt = mock.Mock()
        with self.assertRaises(IntakeError) as caught:
            self.intake(external_intake.prompt_source(prompt, is_terminal=lambda: False))
        self.assertIn(NAME, str(caught.exception))
        prompt.assert_not_called()
        self.assertEqual(0, self.server.writes())

    def test_an_empty_answer_and_an_answer_in_the_wrong_form_are_refused_by_name_before_anything_is_sent(self) -> None:
        for answer in ("", "   ", "not a token", VALUE[:-30]):
            with self.subTest(answer=answer[:6]):
                self.sent.clear()
                with self.assertRaises(IntakeError) as caught:
                    self.intake(answering(answer))
                self.assertIn(NAME, str(caught.exception))
                self.assertNotIn(answer.strip() or "x" * 40, str(caught.exception))
                self.assertEqual(1, len(self.sent))
        self.assertEqual(0, self.server.writes())


class LocalOpenBaoSourceTest(IntakeCase):
    def setUp(self) -> None:
        super().setUp()
        self.local = StandIn({"owner": Role("unused", OWNER_MARKER, ("secret/data/*",))})
        self.local.__enter__()
        self.addCleanup(self.local.__exit__)
        self.local.versions[NAME] = [VALUE]

    def source(self, keychain: Callable[[], str] | None = None, address: str | None = None) -> Source:
        return external_intake.openbao_source(address or self.local.address, keychain or (lambda: OWNER_MARKER))

    def test_an_empty_host_gets_the_value_that_the_owners_local_openbao_holds_and_a_second_run_reads_nothing_there(self) -> None:
        printed = self.intake(self.source())
        self.assertEqual(VALUE, self.server.stored(f"external/{NAME}"))
        reads = len([call for call in self.local.calls if call[1] == f"/v1/secret/data/{NAME}"])
        self.assertEqual(1, reads)
        self.intake(self.source())
        self.assertEqual(reads, len([call for call in self.local.calls if call[1] == f"/v1/secret/data/{NAME}"]))
        self.assertEqual(1, self.server.writes())
        for marker in (VALUE, OWNER_MARKER, ADMIN_MARKER):
            self.assertNotIn(marker, printed)

    def test_a_missing_secret_a_refused_login_a_missing_server_and_a_missing_keychain_item_are_refusals_that_name_the_secret_and_no_value(self) -> None:
        del self.local.versions[NAME]
        cases = {
            "not found": self.source(),
            "denied": self.source(keychain=lambda: "wrong-login"),
            "keychain": self.source(keychain=mock.Mock(side_effect=IntakeError("the keychain item is missing"))),
        }
        with StandIn({}) as dead:
            cases["no server"] = self.source(address=dead.address)
        for label, source in cases.items():
            with self.subTest(case=label), self.assertRaises(IntakeError) as caught:
                self.intake(source)
            for marker in (VALUE, OWNER_MARKER):
                self.assertNotIn(marker, str(caught.exception))
        self.assertEqual(0, self.server.writes())

    def test_the_refusal_for_a_missing_secret_names_the_secret_and_the_source_kind(self) -> None:
        del self.local.versions[NAME]
        with self.assertRaises(IntakeError) as caught:
            self.intake(self.source())
        self.assertIn(NAME, str(caught.exception))
        self.assertIn("EXTERNAL_SECRETS_SOURCE", str(caught.exception))

    def test_only_a_loopback_http_address_is_used_so_the_login_never_leaves_the_computer(self) -> None:
        for address in ("http://example.com:8200", "https://127.0.0.1:8200", "http://10.0.0.5:8200", "127.0.0.1"):
            with self.subTest(address=address), self.assertRaises(IntakeError):
                external_intake.openbao_source(address, lambda: OWNER_MARKER)

    def test_a_stored_value_that_is_not_text_is_refused_by_name(self) -> None:
        self.local.versions[NAME] = [""]
        with self.assertRaises(IntakeError) as caught:
            self.intake(self.source())
        self.assertIn(NAME, str(caught.exception))


class InterruptionTest(IntakeCase):
    def test_a_write_that_never_arrived_is_completed_by_the_next_run(self) -> None:
        source = external_intake.prompt_source(lambda _: VALUE, is_terminal=lambda: True)
        self.server.drop_next("POST", "/v1/secret/data/")
        with self.assertRaises(IntakeError) as caught:
            self.intake(source)
        self.assertIn(NAME, str(caught.exception))
        self.assertNotIn(VALUE, str(caught.exception))
        self.assertIsNone(self.server.stored(f"external/{NAME}"))
        self.intake(source)
        self.assertEqual(VALUE, self.server.stored(f"external/{NAME}"))

    def test_a_stored_text_that_differs_from_the_sent_one_is_removed_and_refused_and_the_next_run_stores_the_right_one(self) -> None:
        source = external_intake.prompt_source(lambda _: VALUE, is_terminal=lambda: True)
        self.server.tamper_next_write(OTHER)
        with self.assertRaises(IntakeError) as caught:
            self.intake(source)
        self.assertIn(NAME, str(caught.exception))
        self.assertIsNone(self.server.stored(f"external/{NAME}"))
        self.intake(source)
        self.assertEqual(VALUE, self.server.stored(f"external/{NAME}"))


class HostCodeTest(IntakeCase):
    def test_every_code_of_the_host_script_has_a_fixed_message_that_holds_no_value(self) -> None:
        for code in (
            "oversized",
            "not_json",
            "bad_keys",
            "unknown_name",
            "bad_form",
            "stored_bad_form",
            "no_login",
            "not_allowed",
            "no_answer",
            "mismatch",
            "cannot_write",
        ):
            with self.subTest(code=code), self.assertRaises(IntakeError) as caught:
                external_intake.run_intake(declared(), replying((3, f"{code}\n")), answering(VALUE), external_intake.write_line)
            self.assertIn(NAME, str(caught.exception))
            self.assertNotIn(VALUE, str(caught.exception))
            for advice in ("reset", "volume", "recreate"):
                self.assertNotIn(advice, str(caught.exception), "no message tells the owner to destroy the host's OpenBao or its data")

    def test_a_code_that_the_program_does_not_know_and_a_failed_ssh_are_refused(self) -> None:
        for reply in ((0, "surprise\n"), (255, ""), (1, "")):
            with self.subTest(reply=reply), self.assertRaises(IntakeError):
                external_intake.run_intake(declared(), replying(reply), answering(VALUE), external_intake.write_line)
