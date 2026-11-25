"""The launcher sends each external secret to the host on standard input of one SSH command, and takes its source from .env.

The SSH command is replaced by a recorder that answers like the host's script. The cases search every argument list and every environment for the markers.
"""

from __future__ import annotations

import hashlib
import io
import json
import unittest
from collections.abc import Sequence
from contextlib import redirect_stdout
from typing import Any
from unittest import mock

from scripts.infra import external_intake, host
from scripts.infra.host import HostError
from scripts.process import CompletedProcess, TimeoutExpired
from tests.fast.test_external_intake_script import NAME, VALUE

ADDRESS = "platform-dev.example.ts.net"
POLICY_DIGEST = hashlib.sha256(host.INTAKE_POLICY_SOURCE.read_bytes()).hexdigest()


class Recorder:
    """Stands in for ``run_command``: the policy check, then the host's script, which answers from a list of codes."""

    def __init__(self, codes: list[str], policy: str = POLICY_DIGEST) -> None:
        self.codes, self.policy = list(codes), policy
        self.calls: list[tuple[str, list[str], dict[str, Any]]] = []

    def __call__(self, program: str, args: Sequence[str], **options: Any) -> CompletedProcess[str]:
        self.calls.append((program, list(args), options))
        if "/usr/bin/sha256sum" in args:
            return CompletedProcess(args=[program], returncode=0, stdout=f"{self.policy}  {host.INTAKE_SUDOERS}\n", stderr="")
        return CompletedProcess(args=[program], returncode=0, stdout=f"{self.codes.pop(0)}\n", stderr="")


def configuration(source: str | None = None) -> dict[str, str]:
    return {"AWS_PROFILE": "example_project_dev", "AWS_REGION": "us-west-2", "ENVIRONMENT": "dev"} | ({"EXTERNAL_SECRETS_SOURCE": source} if source else {})


class SendTest(unittest.TestCase):
    def place(self, recorder: Recorder, source: str | None = "prompt", answer: str = VALUE) -> str:
        output = io.StringIO()
        with (
            mock.patch.object(host, "run_command", recorder),
            mock.patch("scripts.infra.external_intake.getpass.getpass", return_value=answer),
            mock.patch("scripts.infra.external_intake.sys.stdin.isatty", return_value=True),
            redirect_stdout(output),
        ):
            host.place_external_secrets("run1", ADDRESS, configuration(source))
        return output.getvalue()

    def script_calls(self, recorder: Recorder) -> list[tuple[str, list[str], dict[str, Any]]]:
        return [call for call in recorder.calls if "/usr/bin/sha256sum" not in call[1]]

    def test_the_value_goes_on_standard_input_of_one_ssh_command_to_the_installed_script_and_nowhere_else(self) -> None:
        recorder = Recorder(["empty", "written"])
        printed = self.place(recorder)
        status_call, write_call = self.script_calls(recorder)
        for program, args, options in recorder.calls:
            self.assertEqual("ssh", program)
            self.assertNotIn(VALUE, " ".join(args))
            self.assertNotIn(VALUE, json.dumps(options.get("env") or {}))
        self.assertEqual(["sudo", "/usr/bin/python3", "-I", host.INTAKE_SCRIPT], write_call[1][-4:])
        self.assertEqual({"name": NAME, "value": None}, json.loads(status_call[2]["input_text"]))
        self.assertEqual({"name": NAME, "value": VALUE}, json.loads(write_call[2]["input_text"]))
        self.assertNotIn(VALUE, printed)
        self.assertIn(NAME, printed)

    def test_a_filled_path_sends_no_value_and_never_asks_the_source(self) -> None:
        recorder = Recorder(["present"])
        with (
            mock.patch("scripts.infra.external_intake.getpass.getpass") as ask,
            mock.patch.object(host, "run_command", recorder),
            mock.patch("scripts.infra.external_intake.sys.stdin.isatty", return_value=True),
            redirect_stdout(io.StringIO()),
        ):
            host.place_external_secrets("run1", ADDRESS, configuration("prompt"))
        ask.assert_not_called()
        self.assertEqual(1, len(self.script_calls(recorder)))

    def test_the_default_source_is_the_prompt_and_openbao_is_chosen_by_name(self) -> None:
        for source, function in (("openbao", "openbao_source"), ("prompt", "prompt_source"), (None, "prompt_source")):
            with self.subTest(source=source):
                recorder = Recorder(["present"])
                with (
                    mock.patch.object(host, "run_command", recorder),
                    mock.patch.object(external_intake, function, wraps=getattr(external_intake, function)) as chosen,
                    redirect_stdout(io.StringIO()),
                ):
                    host.place_external_secrets("run1", ADDRESS, configuration(source))
                chosen.assert_called_once()

    def test_a_sudo_policy_that_is_missing_or_differs_stops_the_intake_before_any_value_is_read(self) -> None:
        for policy in ("0" * 64,):
            with self.subTest(policy="differs"):
                recorder = Recorder([], policy=policy)
                with mock.patch("scripts.infra.external_intake.getpass.getpass") as ask, self.assertRaises(HostError) as caught:
                    self.place(recorder)
                ask.assert_not_called()
                self.assertIn("sudo policy", str(caught.exception))
        self.assertEqual([], self.script_calls(recorder))

    def test_a_ssh_that_times_out_is_a_fixed_message_that_names_the_secret_and_holds_no_value(self) -> None:
        def slow(program: str, args: Sequence[str], **options: Any) -> CompletedProcess[str]:
            if "/usr/bin/sha256sum" in args:
                return CompletedProcess(args=[program], returncode=0, stdout=f"{POLICY_DIGEST}  x\n", stderr="")
            raise TimeoutExpired(program, 1)

        with (
            mock.patch.object(host, "run_command", slow),
            mock.patch("scripts.infra.external_intake.getpass.getpass", return_value=VALUE),
            mock.patch("scripts.infra.external_intake.sys.stdin.isatty", return_value=True),
            redirect_stdout(io.StringIO()),
            self.assertRaises(HostError) as caught,
        ):
            host.place_external_secrets("run1", ADDRESS, configuration("prompt"))
        self.assertIn(NAME, str(caught.exception))
        self.assertNotIn(VALUE, str(caught.exception))

    def test_a_refusal_of_the_intake_is_a_host_error_with_the_fixed_message(self) -> None:
        class Refusing(Recorder):
            def __call__(self, program: str, args: Sequence[str], **options: Any) -> CompletedProcess[str]:
                result = super().__call__(program, args, **options)
                if result.stdout.strip() == "bad_form":
                    return CompletedProcess(args=[program], returncode=3, stdout="bad_form\n", stderr="")
                return result

        with self.assertRaises(HostError) as caught:
            self.place(Refusing(["empty", "bad_form"]))
        self.assertIn(NAME, str(caught.exception))
        self.assertNotIn(VALUE, str(caught.exception))


if __name__ == "__main__":
    unittest.main()
