"""The launcher reads AIStor's licence from this computer's OpenBao and places it on the host, and never prints or logs it.

The owner keeps the licence at ``secret/aistor_licence`` (field ``value``) in the local OpenBao, like every secret that a third party issues. The
launcher reads it with a token of its own from the Keychain (``openbao-aistor-licence-token``, whose OpenBao policy allows ``read`` on that one path and
nothing else, never the owner's full token), over plain HTTP on the loopback interface, and sends it to the host on standard input of one SSH command. An
absent licence (the server's 404) and a computer without that Keychain item or without the ``security`` command are no error: the deploy skips AIStor with a
notice. Any other failure stops the deploy by a fixed message. These cases use a marker for the licence and look for it in every argument, environment,
output and message.
"""

from __future__ import annotations

import contextlib
import hashlib
import http.client
import io
import json
import re
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.infra import aistor_licence, host
from scripts.infra.host import HostError
from scripts.process import CompletedProcess, TimeoutExpired
from scripts.project_config import ROOT
from tests.fast.ansible_paths import ROLES, group_variables

LICENCE = "marker.licence.text-that-must-not-leak-0123456789"
SESSION = "hvs.example-owner-token-0123456789"
ADDRESS = "platform-dev.example.ts.net"
RUN_ID = "20260101t0000-abcdef"


class FakeConnection:
    """An HTTP connection that sends nothing: it keeps the request and answers with the status and body that the case set."""

    requests: list[tuple[str, int, str, str, dict[str, str]]] = []
    answer: tuple[int, bytes] = (200, b"{}")
    error: Exception | None = None

    def __init__(self, host: str, port: int, timeout: float) -> None:
        self.host, self.port = host, port

    def request(self, method: str, path: str, body: str | None = None, headers: dict[str, str] | None = None) -> None:
        self.requests.append((self.host, self.port, method, path, headers or {}))
        if self.error is not None:
            raise self.error

    def getresponse(self) -> Any:
        status, body = self.answer
        return mock.Mock(status=status, read=lambda: body)

    def close(self) -> None:
        pass


def stored(value: Any = LICENCE) -> tuple[int, bytes]:
    return 200, json.dumps({"data": {"data": {"value": value}}}).encode()


class ReadTest(unittest.TestCase):
    def setUp(self) -> None:
        FakeConnection.requests, FakeConnection.answer, FakeConnection.error = [], stored(), None
        self.keychain: list[tuple[str, list[str]]] = []
        self.token: CompletedProcess[str] = CompletedProcess(["security"], 0, SESSION + "\n", "")
        for patcher in (
            mock.patch.object(http.client, "HTTPConnection", FakeConnection),
            mock.patch.object(aistor_licence, "run_command", side_effect=self.run_command),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.output = io.StringIO()
        self.enterContext(contextlib.redirect_stdout(self.output))
        self.enterContext(contextlib.redirect_stderr(self.output))

    def run_command(self, program: str, arguments: list[str], **keywords: Any) -> CompletedProcess[str]:
        self.keychain.append((program, arguments))
        if isinstance(self.token, Exception):
            raise self.token
        return self.token

    def refused(self) -> str:
        with self.assertRaises(aistor_licence.LicenceError) as caught:
            aistor_licence.read_licence()
        self.caught = caught.exception
        return str(caught.exception)

    def test_the_licence_is_read_from_the_local_openbao_at_its_own_path_with_the_owners_token_in_the_header_only(self) -> None:
        self.assertEqual(LICENCE, aistor_licence.read_licence())
        self.assertEqual([("127.0.0.1", 8200, "GET", "/v1/secret/data/aistor_licence", {"X-Vault-Token": SESSION})], FakeConnection.requests)
        program, arguments = self.keychain[0]
        self.assertEqual("security", program)
        self.assertEqual(["find-generic-password", "-a"], arguments[:2])
        self.assertEqual(["-s", "openbao-aistor-licence-token", "-w"], arguments[3:])
        self.assertNotIn(LICENCE, self.output.getvalue())
        self.assertNotIn(SESSION, self.output.getvalue())

    def test_the_path_is_the_one_the_owner_saved_and_the_token_is_the_launchers_own_and_neither_the_owners_nor_the_agents(self) -> None:
        self.assertEqual("secret/data/aistor_licence", aistor_licence.LOCAL_PATH)
        self.assertEqual("openbao-aistor-licence-token", aistor_licence.KEYCHAIN_ITEM)
        self.assertNotIn("agent", aistor_licence.KEYCHAIN_ITEM)

    def test_the_launcher_never_asks_the_keychain_for_the_owners_full_token_not_even_when_its_own_item_is_missing(self) -> None:
        source = (ROOT / "scripts" / "infra" / "aistor_licence.py").read_text()
        self.assertNotIn("openbao-owner-token", source, "the launcher's code names only the licence token's item")
        for case in (self.token, CompletedProcess(["security"], 44, "", ""), FileNotFoundError("security")):
            self.token = case  # type: ignore[assignment]
            self.keychain.clear()
            with contextlib.suppress(aistor_licence.LicenceError):
                aistor_licence.read_licence()
            self.assertEqual(["openbao-aistor-licence-token"], [arguments[arguments.index("-s") + 1] for _, arguments in self.keychain])

    def test_the_keychain_item_that_is_not_there_or_a_computer_without_the_security_command_is_no_licence_source_and_names_the_owners_setup_step(self) -> None:
        for case in (CompletedProcess(["security"], 44, "", f"the item {SESSION} could not be found"), FileNotFoundError("security")):
            with self.subTest(case=str(case)[:30]):
                self.token = case  # type: ignore[assignment]
                with self.assertRaises(aistor_licence.NoLicenceSource) as caught:
                    aistor_licence.read_licence()
                message = str(caught.exception)
                self.assertIn("openbao-aistor-licence-token", message)
                self.assertIn("README.md", message)
                self.assertIn("aistor-licence-read", message)
                self.assertNotIn(SESSION, message)
                self.assertIsInstance(caught.exception, aistor_licence.LicenceError)
        self.assertEqual([], FakeConnection.requests)

    def test_an_absent_licence_is_the_servers_404_and_is_not_an_error(self) -> None:
        FakeConnection.answer = (404, b'{"errors":[]}')
        self.assertIsNone(aistor_licence.read_licence())

    def test_every_other_status_is_a_fixed_message_with_the_status_and_no_part_of_the_answer(self) -> None:
        for status in (400, 403, 500, 503):
            with self.subTest(status=status):
                FakeConnection.answer = (status, json.dumps({"errors": [f"echo {LICENCE} {SESSION}"]}).encode())
                message = self.refused()
                self.assertIn(f"HTTP {status}", message)
                self.assertNotIn(LICENCE, message)
                self.assertNotIn(SESSION, message)

    def test_a_server_that_does_not_answer_is_named_and_the_librarys_text_is_dropped(self) -> None:
        for error in (OSError(f"refused {SESSION}"), http.client.HTTPException(LICENCE)):
            with self.subTest(error=type(error).__name__):
                FakeConnection.error = error
                message = self.refused()
                self.assertIn("did not answer", message)
                self.assertNotIn(SESSION, message)
                self.assertNotIn(LICENCE, message)

    def test_an_unusable_keychain_item_is_a_failure_with_a_fixed_message_and_nothing_is_asked_of_the_server(self) -> None:
        cases = (
            CompletedProcess(["security"], 36, "", f"the keychain {SESSION} is locked"),
            CompletedProcess(["security"], 0, "", ""),
            CompletedProcess(["security"], 0, "bad token\nwith a break", ""),
            TimeoutExpired(["security"], 30),
        )
        for case in cases:
            with self.subTest(case=str(case)[:30]):
                self.token = case  # type: ignore[assignment]
                message = self.refused()
                self.assertIn("Keychain", message)
                self.assertNotIn(SESSION, message)
                self.assertNotIsInstance(self.caught, aistor_licence.NoLicenceSource)
        self.assertEqual([], FakeConnection.requests)

    def test_the_stored_value_must_be_text_of_the_form_of_a_licence_and_is_never_echoed(self) -> None:
        for value in (None, 5, True, "", "   \n", ["x"], "x" * 16385, f"{LICENCE}\x00", f"{LICENCE}é"):
            with self.subTest(value=repr(value)[:20]):
                FakeConnection.answer = stored(value)
                message = self.refused()
                self.assertIn("not a licence", message)
                self.assertNotIn(LICENCE, message)
        FakeConnection.answer = (200, b"not json " + LICENCE.encode())
        self.assertNotIn(LICENCE, self.refused())

    def test_the_value_is_returned_with_the_edges_trimmed_and_an_inner_line_break_kept(self) -> None:
        FakeConnection.answer = stored(f"\n  {LICENCE}\nsecond-line \n")
        self.assertEqual(f"{LICENCE}\nsecond-line", aistor_licence.read_licence())

    def test_the_address_must_be_plain_http_on_the_loopback_interface_so_the_login_never_leaves_the_computer(self) -> None:
        for address in ("https://127.0.0.1:8200", "http://example.com:8200", "http://192.168.1.2:8200", "127.0.0.1:8200"):
            with self.subTest(address=address), self.assertRaises(aistor_licence.LicenceError):
                aistor_licence.read_licence(address)
        self.assertEqual([], FakeConnection.requests)

    def test_the_module_prints_nothing_and_holds_no_print_statement(self) -> None:
        aistor_licence.read_licence()
        self.assertEqual("", self.output.getvalue())
        self.assertNotIn("print(", (ROOT / "scripts" / "infra" / "aistor_licence.py").read_text())


class ReportTest(unittest.TestCase):
    """The notice that AIStor is skipped is printed after the host has said that it has no licence, and never before."""

    STATE = "skipped: no AIStor licence file was on the host at the last deploy; place the licence and deploy again\n"

    def report(self, placement: str, state: str | None) -> str:
        output = io.StringIO()
        with contextlib.redirect_stdout(output), mock.patch.object(host, "ssh_output", return_value=state) as ssh:
            host.report_aistor_state(RUN_ID, ADDRESS, placement)
        self.ssh = ssh
        return output.getvalue()

    def test_a_skipped_aistor_prints_the_notice_for_its_reason_after_the_host_confirmed_it(self) -> None:
        absent = self.report("absent", self.STATE)
        self.assertIn("AIStor is skipped", absent)
        self.assertIn("secret/aistor_licence", absent)
        self.assertIn("make host-up", absent)
        no_source = self.report("no_source", self.STATE)
        self.assertIn("AIStor is skipped", no_source)
        self.assertIn("openbao-aistor-licence-token", no_source)
        self.assertIn("README.md", no_source)
        self.assertIn("aistor-licence-read", no_source)

    def test_the_state_is_read_with_one_fixed_read_only_command_on_the_file_the_playbook_writes(self) -> None:
        self.report("absent", self.STATE)
        run_id, address, command, _ = self.ssh.call_args.args
        self.assertEqual((RUN_ID, ADDRESS, f"cat {host.AISTOR_STATE_FILE}"), (run_id, address, command))
        variables = group_variables()
        self.assertEqual("{{ compose_stack_compose_directory }}/aistor.state", variables["compose_stack_aistor_state_file"])
        self.assertEqual("/opt/platform/compose/aistor.state", host.AISTOR_STATE_FILE)

    def test_a_licence_that_was_placed_prints_nothing_and_reads_nothing(self) -> None:
        for placement in ("placed", "unchanged"):
            with self.subTest(placement):
                self.assertEqual("", self.report(placement, self.STATE))
                self.ssh.assert_not_called()

    def test_an_aistor_that_started_with_a_licence_file_that_was_already_on_the_host_says_so_and_does_not_claim_a_skip(self) -> None:
        for placement in ("absent", "no_source"):
            with self.subTest(placement):
                text = self.report(placement, "started\n")
                self.assertIn("started with the licence file that was already on the host", text)
                self.assertNotIn("is skipped", text)

    def test_a_state_that_cannot_be_read_or_is_not_a_known_record_claims_no_skip(self) -> None:
        for state in (None, "", "garbage\n", "skipped-but-not-really"):
            with self.subTest(state=state):
                text = self.report("absent", state)
                self.assertNotIn("is skipped", text)
                self.assertIn("could not confirm", text)


class OwnerSetupStepTest(unittest.TestCase):
    """The README holds the exact commands of the owner's one-time step, and they match what the launcher reads."""

    def readme(self) -> str:
        return (ROOT / "README.md").read_text()

    def test_the_step_creates_the_one_path_policy_a_periodic_token_with_only_that_policy_and_the_keychain_item_the_launcher_reads(self) -> None:
        text = self.readme()
        self.assertIn("bao policy write aistor-licence-read -", text)
        self.assertIn(f'path "{aistor_licence.LOCAL_PATH}" {{ capabilities = ["read"] }}', text)
        self.assertRegex(text, r"bao token create -field=token -orphan -policy=aistor-licence-read -no-default-policy -period=\d+h")
        self.assertIn(f'security add-generic-password -a "$USER" -s {aistor_licence.KEYCHAIN_ITEM} -U -w', text)
        for name in ("aistor-licence-read", aistor_licence.KEYCHAIN_ITEM):
            self.assertIn(name, aistor_licence.SETUP_MESSAGE)

    def test_the_token_is_never_printed_and_never_an_argument_of_a_command(self) -> None:
        text = self.readme()
        step = text[text.index("export BAO_ADDR") : text.index("pbcopy < /dev/null")]
        self.assertNotRegex(step, r"add-generic-password[^\n]*-w\s+[\"'$A-Za-z0-9]", "the password is typed at security's own prompt")
        self.assertNotRegex(step, r"BAO_TOKEN=\S", "the administration token is read without an echo")
        self.assertIn('read -rs "BAO_TOKEN?', step)
        self.assertNotIn('echo "$', step)
        self.assertRegex(step, r"bao token create [^\n]*\| pbcopy")

    def test_the_token_is_an_orphan_so_revoking_the_administration_token_does_not_revoke_it(self) -> None:
        text = self.readme()
        (create,) = re.findall(r"^bao token create [^\n]*", text, re.MULTILINE)
        self.assertIn(" -orphan ", create + " ", "a child of the administration token is revoked with it")
        self.assertLess(text.index("bao token create -field"), text.index("\nbao token revoke -self;"))
        self.assertIn("openbao.org/docs/concepts/tokens/#token-hierarchies-and-orphan-tokens", text)

    def test_after_the_administration_token_is_revoked_the_saved_token_is_checked_by_reading_the_licence_path_without_printing_it(self) -> None:
        text = self.readme()
        after = text[text.index("\nbao token revoke -self;") :]
        (line,) = re.findall(r"^[^\n]*bao read secret/data/aistor_licence[^\n]*", after, re.MULTILINE)
        self.assertIn("security find-generic-password", line)
        self.assertIn(aistor_licence.KEYCHAIN_ITEM, line)
        self.assertRegex(line, r"> ?/dev/null", "the value goes nowhere")
        self.assertNotRegex(line, r"-field|-format|\| *(?:cat|echo|pbcopy|tee)|\becho \"?\$", "the value is never printed")
        self.assertIn(
            "&&",
            line.split("security find-generic-password")[1].split("bao read")[0],
            "an empty token never reaches bao, which would use its token helper file",
        )

    def test_the_readme_says_that_the_launcher_never_uses_the_owners_full_token(self) -> None:
        self.assertIn("never with the owner's full token (`openbao-owner-token`)", self.readme())


class PlaceTest(unittest.TestCase):
    """``place_aistor_licence`` sends the licence on standard input of one SSH command and never in an argument, an environment, an output or a message."""

    def setUp(self) -> None:
        self.runs: list[tuple[str, list[str], dict[str, Any]]] = []
        self.sudoers = ROLES / "compose_stack" / "files" / "aistor_licence_install.sudoers"
        self.policy_status, self.script_status, self.script_output = 0, 0, "placed\n"
        self.licence: str | None | Exception = LICENCE
        self.output = io.StringIO()
        self.enterContext(contextlib.redirect_stdout(self.output))
        for patcher in (
            mock.patch.object(host, "run_command", side_effect=self.run_command),
            mock.patch.object(aistor_licence, "read_licence", side_effect=self.read_licence),
            mock.patch.object(host, "ssh_arguments", return_value=["-o", "BatchMode=yes"]),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def read_licence(self) -> str | None:
        if isinstance(self.licence, Exception):
            raise self.licence
        return self.licence

    def run_command(self, program: str, arguments: list[str], **keywords: Any) -> CompletedProcess[str]:
        self.runs.append((program, arguments, keywords))
        if "/usr/bin/sha256sum" in arguments:
            digest = hashlib.sha256(self.sudoers.read_bytes()).hexdigest()
            return CompletedProcess([program], self.policy_status, f"{digest}  /etc/sudoers.d/aistor_licence_install\n" if self.policy_status == 0 else "", "")
        return CompletedProcess([program], self.script_status, self.script_output, "")

    def everything(self) -> str:
        return json.dumps(self.runs, default=str) + self.output.getvalue()

    def test_the_licence_goes_on_standard_input_of_one_ssh_command_that_runs_the_fixed_script_and_nowhere_else(self) -> None:
        self.assertEqual("placed", host.place_aistor_licence(RUN_ID, ADDRESS))
        sent = [run for run in self.runs if "/usr/bin/sha256sum" not in run[1]]
        self.assertEqual(1, len(sent))
        program, arguments, keywords = sent[0]
        self.assertEqual("ssh", program)
        self.assertEqual(["sudo", "/usr/bin/python3", "-I", "/opt/platform/bin/aistor_licence_install"], arguments[-4:])
        self.assertEqual(LICENCE, keywords["input_text"].rstrip("\n"))
        self.assertNotIn(LICENCE, json.dumps([(program, arguments)]))
        self.assertNotIn(LICENCE, json.dumps({key: value for key, value in keywords.items() if key != "input_text"}, default=str))
        self.assertNotIn(LICENCE, self.output.getvalue())

    def test_the_sudo_policy_file_is_checked_by_its_hash_before_the_licence_is_sent(self) -> None:
        self.policy_status = 1
        with self.assertRaises(HostError) as caught:
            host.place_aistor_licence(RUN_ID, ADDRESS)
        self.assertIn("sudo policy", str(caught.exception))
        self.assertEqual(1, len(self.runs))
        self.assertNotIn(LICENCE, self.everything())

    def test_a_licence_that_is_not_in_the_local_openbao_makes_no_ssh_call_prints_no_notice_yet_and_is_not_an_error(self) -> None:
        self.licence = None
        self.assertEqual("absent", host.place_aistor_licence(RUN_ID, ADDRESS))
        self.assertEqual([], self.runs)
        self.assertEqual("", self.output.getvalue(), "the notice waits until the host has confirmed that it has no licence")

    def test_a_computer_with_no_licence_source_makes_no_ssh_call_prints_no_notice_yet_and_is_not_an_error(self) -> None:
        self.licence = aistor_licence.NoLicenceSource(aistor_licence.SETUP_MESSAGE)
        self.assertEqual("no_source", host.place_aistor_licence(RUN_ID, ADDRESS))
        self.assertEqual([], self.runs)
        self.assertEqual("", self.output.getvalue())

    def test_a_failure_to_read_the_licence_still_stops_the_deploy_with_the_fixed_message(self) -> None:
        self.licence = aistor_licence.LicenceError("this computer's OpenBao refused the read of the AIStor licence (HTTP 403)")
        with self.assertRaises(HostError) as caught:
            host.place_aistor_licence(RUN_ID, ADDRESS)
        self.assertIn("HTTP 403", str(caught.exception))
        self.assertEqual([], self.runs)

    def test_the_placement_is_given_to_ansible_as_placed_or_absent_and_nothing_else(self) -> None:
        self.assertEqual("placed", host.licence_fact("placed"))
        self.assertEqual("placed", host.licence_fact("unchanged"))
        self.assertEqual("absent", host.licence_fact("absent"))
        self.assertEqual("absent", host.licence_fact("no_source"))
        for other in ("", "skipped", "PLACED", None, 5):
            with self.subTest(other=other), self.assertRaises(ValueError):
                host.licence_fact(other)  # type: ignore[arg-type]

    def test_each_answer_of_the_script_is_a_fixed_message_and_the_refusals_name_no_value(self) -> None:
        for code in ("oversized", "empty", "bad_form", "bad_owner", "cannot_write"):
            with self.subTest(code=code):
                self.script_status, self.script_output = 3, f"{code}\n"
                with self.assertRaises(HostError) as caught:
                    host.place_aistor_licence(RUN_ID, ADDRESS)
                self.assertNotIn(LICENCE, str(caught.exception))
        self.script_status, self.script_output = 0, "something else\n"
        with self.assertRaises(HostError):
            host.place_aistor_licence(RUN_ID, ADDRESS)
        self.script_status = 255
        with self.assertRaises(HostError) as caught:
            host.place_aistor_licence(RUN_ID, ADDRESS)
        self.assertIn("255", str(caught.exception))

    def test_an_unchanged_licence_is_reported_as_in_place_and_a_timeout_is_a_fixed_message(self) -> None:
        self.script_output = "unchanged\n"
        self.assertEqual("unchanged", host.place_aistor_licence(RUN_ID, ADDRESS))
        with mock.patch.object(host, "run_command", side_effect=TimeoutExpired(["ssh"], 60)), self.assertRaises(HostError) as caught:
            host.place_aistor_licence(RUN_ID, ADDRESS)
        self.assertNotIn(LICENCE, str(caught.exception))

    def test_the_sudoers_file_the_launcher_checks_is_the_one_the_role_installs(self) -> None:
        self.assertEqual(self.sudoers, host.LICENCE_POLICY_SOURCE)
        self.assertEqual("/etc/sudoers.d/aistor_licence_install", host.LICENCE_SUDOERS_FILE)
        variables = group_variables()
        self.assertEqual("/etc/sudoers.d/aistor_licence_install", variables["compose_stack_aistor_licence_sudoers"])
        self.assertEqual("{{ compose_stack_bin_directory }}/aistor_licence_install", variables["compose_stack_aistor_licence_script"])
        self.assertEqual("/opt/platform/bin", variables["compose_stack_bin_directory"])
        self.assertEqual(["/usr/bin/python3", "-I", host.LICENCE_SCRIPT], host.LICENCE_COMMAND)
        self.assertEqual(Path(host.LICENCE_SCRIPT).name, "aistor_licence_install")


if __name__ == "__main__":
    unittest.main()
