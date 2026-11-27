"""The five host runners treat a read that failed as a failure and never as an answer (fix round 3, recheck 2 of d3cdebe).

The runners stay untracked, so each test skips when its runner is not on this machine. Docker, SSH, Ansible and the host are stand-ins: no cloud, Docker or
Spark runs here.

Failure modes covered here:

- Two failed reads of a start time compare equal, so "the container was not started again" passes when Docker answered neither read (Caddy, Nessie, monitoring).
- Tailscale's status is parsed from a read that exited 255, so a failed read that printed valid JSON gives the host's name and address.
- A failed listing reads as "none left" (``remove_project``), and a cleanup that left something is only text in the record (monitoring, Caddy, catalog,
  Nessie, OpenBao), so the run passes with a leftover.
- A diagnostic read that failed reads as "the password is absent from it" (Nessie).
- The guard line of the Caddy service is matched in the merged output of Compose, which also holds Compose's own warnings that name the variable, and the
  Ansible recap is read from the merged output, so a warning on standard error forges it (Caddy, OpenBao).
- Secret ID files that read as empty twice are "kept" (OpenBao).
"""

from __future__ import annotations

import ipaddress
import json
import re
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

from scripts.project_config import ROOT
from tests.e2e.run_record import Record
from tests.fast.test_runner_stdout_alone import Noisy, runner

E2E = Path(__file__).resolve().parents[1] / "e2e"
RUNNERS = ("stack_caddy", "stack_nessie", "stack_monitoring", "stack_openbao", "catalog_proof")


def record() -> Record:
    return Record(feature="x", slug="x", rerun="x")


def failed(run: Record) -> list[str]:
    return [item.case for item in run.observations if not item.passed]


def only(run: Record, case: str) -> Any:
    return next(item for item in run.observations if item.case == case)


class DockerOfThisComputer:
    """The Docker of this computer, called through ``run_command``. ``items`` holds each kind's names. ``fails`` lists the calls that fail with exit 1."""

    def __init__(self, items: dict[str, list[str]] | None = None, *, fails: tuple[str, ...] = (), after: int = 0, stubborn: bool = False) -> None:
        self.items = {"containers": [], "volumes": [], "networks": []} if items is None else items
        self.fails = fails
        self.after = after
        self.stubborn = stubborn
        self.listings = 0
        self.calls: list[list[str]] = []

    def __call__(self, program: str, arguments: list[str], **_: object) -> SimpleNamespace:
        self.calls.append(arguments)
        kind = {"ps": "containers", "volume": "volumes", "network": "networks"}.get(arguments[0])
        if "--filter" in arguments and kind is not None:
            self.listings += 1
            if kind in self.fails and self.listings > self.after:
                return SimpleNamespace(returncode=1, stdout="", stderr="Cannot connect to the Docker daemon")
            return SimpleNamespace(returncode=0, stdout="".join(f"{name}\n" for name in self.items[kind]), stderr="")
        if arguments[0] == "rm":
            kind = "containers"
        elif arguments[:2] == ["volume", "rm"]:
            kind = "volumes"
        elif arguments[:2] == ["network", "rm"]:
            kind = "networks"
        else:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if not self.stubborn:
            self.items[kind] = []
        return SimpleNamespace(returncode=1 if self.stubborn else 0, stdout="", stderr="")


def call_remove_project(module: ModuleType, docker: DockerOfThisComputer) -> bool:
    arguments = ["p2_x", []] if module.__name__ != "tests.e2e.stack_caddy" else [[]]
    with mock.patch.object(module, "run_command", docker):
        found: bool = module.remove_project(*arguments)
    return found


class RemovedProject(unittest.TestCase):
    def test_a_project_with_nothing_left_reads_as_removed(self) -> None:
        for name in RUNNERS:
            module = runner(name)
            with self.subTest(runner=name):
                self.assertTrue(call_remove_project(module, DockerOfThisComputer()))

    def test_a_project_with_resources_is_removed_and_reads_as_removed(self) -> None:
        for name in RUNNERS:
            module = runner(name)
            with self.subTest(runner=name):
                docker = DockerOfThisComputer({"containers": ["c1"], "volumes": ["v1"], "networks": ["n1"]})
                self.assertTrue(call_remove_project(module, docker))
                self.assertTrue(any(call[0] == "rm" for call in docker.calls))

    def test_a_resource_that_is_refused_reads_as_left(self) -> None:
        for name in RUNNERS:
            module = runner(name)
            with self.subTest(runner=name):
                docker = DockerOfThisComputer({"containers": ["c1"], "volumes": ["v1"], "networks": ["n1"]}, stubborn=True)
                self.assertFalse(call_remove_project(module, docker))

    def test_a_listing_that_fails_does_not_read_as_none_left(self) -> None:
        for name in RUNNERS:
            module = runner(name)
            for kind in ("containers", "volumes"):
                with self.subTest(runner=name, kind=kind):
                    self.assertFalse(call_remove_project(module, DockerOfThisComputer(fails=(kind,))))

    def test_a_listing_that_fails_only_in_the_check_after_the_removal_does_not_read_as_none_left(self) -> None:
        for name in RUNNERS:
            module = runner(name)
            first_pass = 2 if name == "stack_caddy" else 3
            with self.subTest(runner=name):
                docker = DockerOfThisComputer({"containers": ["c1"], "volumes": [], "networks": []}, fails=("containers",), after=first_pass)
                self.assertFalse(call_remove_project(module, docker))

    def test_the_networks_listing_of_the_runners_that_check_networks_must_succeed_too(self) -> None:
        for name in ("stack_nessie", "stack_monitoring", "stack_openbao", "catalog_proof"):
            module = runner(name)
            with self.subTest(runner=name):
                self.assertFalse(call_remove_project(module, DockerOfThisComputer(fails=("networks",))))


class MonitoringCleanup(unittest.TestCase):
    """The local cleanup of the monitoring runner fails the run when a labelled resource or a project resource is left or a listing is unreadable."""

    def clean(self, *, target_fails: bool = False, stubborn: bool = False, docker: DockerOfThisComputer | None = None) -> Record:
        from tests.fast.test_runner_monitoring_cleanup import HostDocker, stack_of

        module = runner("stack_monitoring")
        run = record()
        stack = stack_of(module, HostDocker("abc123", listing_fails=target_fails, stubborn=stubborn))
        stack.record = run
        scratch = Path(tempfile.mkdtemp(prefix="reads_test_"))
        with mock.patch.object(module, "run_command", docker or DockerOfThisComputer()):
            module.clean_local(run, [], stack, scratch)
        return run

    def test_a_clean_run_has_no_failed_assertion_and_names_each_result_in_the_cleanup_text(self) -> None:
        run = self.clean()
        self.assertEqual(failed(run), [])
        self.assertIn("labelled test resources: True", run.cleanup)
        self.assertIn("scratch folder removed: True", run.cleanup)

    def test_a_labelled_resource_that_is_left_fails_the_run(self) -> None:
        self.assertEqual(failed(self.clean(stubborn=True)), ["labelled test resources"])

    def test_a_labelled_listing_that_cannot_be_read_fails_the_run(self) -> None:
        self.assertEqual(failed(self.clean(target_fails=True)), ["labelled test resources"])

    def test_a_project_listing_that_cannot_be_read_fails_the_run(self) -> None:
        run = self.clean(docker=DockerOfThisComputer(fails=("containers",)))
        self.assertEqual(failed(run), ["project removed"])

    def test_a_project_resource_that_is_left_fails_the_run(self) -> None:
        run = self.clean(docker=DockerOfThisComputer({"containers": ["c1"], "volumes": [], "networks": []}, stubborn=True))
        self.assertEqual(failed(run), ["project removed"])

    def test_a_scratch_folder_that_is_left_fails_the_run(self) -> None:
        module = runner("stack_monitoring")
        run = record()
        scratch = Path(tempfile.mkdtemp(prefix="reads_test_"))
        with mock.patch.object(module, "run_command", DockerOfThisComputer()), mock.patch.object(module.shutil, "rmtree"):
            module.clean_local(run, [], None, scratch)
        self.assertEqual(failed(run), ["scratch folder removed"])
        scratch.rmdir()


def inspect_answers(before: tuple[int, str], after: tuple[int, str], *, id_text: str = "abc123") -> Callable[[list[str]], tuple[int, str]]:
    """A target whose ``docker inspect`` gives ``before`` until the stack is started again (``compose up``) and ``after`` from then on."""
    state = {"started_again": False}

    def answer(args: list[str]) -> tuple[int, str]:
        if "compose" in args and "ps" in args:
            return 0, f"{id_text}\n"
        if "compose" in args and "up" in args:
            state["started_again"] = True
            return 0, ""
        if "inspect" in args:
            return after if state["started_again"] else before
        return 0, ""

    return answer


GOOD = (0, "2026-11-25T00:00:00Z\n")
OTHER = (0, "2026-11-25T00:00:09Z\n")
ID = "0123456789abcdef" * 4


def shaped(name: str, answer: tuple[int, str], identifier: str = ID) -> tuple[int, str]:
    """The answer of ``docker inspect`` in the form that the runner's format asks for: the monitoring runner asks for the ID and the start time."""
    code, text = answer
    return (code, f"{identifier} {text}") if name == "stack_monitoring" and text.strip() else answer


UNREAD_CASES = {
    "exit 1, no text": (1, ""),
    "exit 255, a timestamp": (255, "2026-11-25T00:00:00Z\n"),
    "exit 0, no text": (0, ""),
    "exit 0, blank": (0, "\n"),
}


class StartTimes(unittest.TestCase):
    """Two reads of a start time compare only when both succeeded and printed something."""

    def second_start(self, name: str, first: tuple[int, str], second: tuple[int, str]) -> Record:
        module = runner(name)
        run = record()
        target = Noisy(inspect_answers(shaped(name, first), shaped(name, second)))
        if name == "stack_caddy":
            stack = module.Stack(target, run, [], ["docker", "compose"])
            module.second_start_case(stack, stack.container())
        elif name == "stack_nessie":
            stack = module.Stack(target, run, [])
            module.idempotence(stack)
        else:
            stack = module.Stack(target, run, [])
            module.idempotence_case(stack)
        return run

    def case_name(self, name: str) -> str:
        return "second start of the stack" if name != "stack_caddy" else "second start"

    def test_a_start_time_that_is_read_twice_and_is_the_same_passes(self) -> None:
        for name in ("stack_caddy", "stack_nessie", "stack_monitoring"):
            with self.subTest(runner=name):
                self.assertEqual(failed(self.second_start(name, GOOD, GOOD)), [])

    def test_a_start_time_that_changed_fails(self) -> None:
        for name in ("stack_caddy", "stack_nessie", "stack_monitoring"):
            with self.subTest(runner=name):
                self.assertEqual(failed(self.second_start(name, GOOD, OTHER)), [self.case_name(name)])

    def test_two_reads_that_both_failed_do_not_pass_as_unchanged(self) -> None:
        for name in ("stack_caddy", "stack_nessie", "stack_monitoring"):
            for label, answer in UNREAD_CASES.items():
                with self.subTest(runner=name, answer=label):
                    self.assertEqual(failed(self.second_start(name, answer, answer)), [self.case_name(name)])

    def test_one_read_that_failed_does_not_pass_whatever_the_other_gave(self) -> None:
        for name in ("stack_caddy", "stack_nessie", "stack_monitoring"):
            for label, answer in UNREAD_CASES.items():
                with self.subTest(runner=name, answer=label, side="first"):
                    self.assertEqual(failed(self.second_start(name, answer, GOOD)), [self.case_name(name)])
                with self.subTest(runner=name, answer=label, side="second"):
                    self.assertEqual(failed(self.second_start(name, GOOD, answer)), [self.case_name(name)])

    def test_the_observation_names_no_timestamp(self) -> None:
        for name in ("stack_caddy", "stack_nessie", "stack_monitoring"):
            with self.subTest(runner=name):
                run = self.second_start(name, GOOD, GOOD)
                self.assertNotIn("2026-11-25", only(run, self.case_name(name)).observed)

    def test_a_container_that_cannot_be_listed_does_not_pass_in_the_caddy_runner_although_both_start_reads_agree(self) -> None:
        module = runner("stack_caddy")
        run = record()
        inspected = inspect_answers(GOOD, GOOD)
        stack = module.Stack(Noisy(lambda args: (0, "") if "compose" in args and "ps" in args else inspected(args)), run, [], ["docker", "compose"])
        module.second_start_case(stack, stack.container())
        self.assertEqual(failed(run), ["second start"])

    def test_a_service_with_no_container_does_not_pass_in_the_monitoring_runner(self) -> None:
        module = runner("stack_monitoring")
        run = record()
        module.idempotence_case(module.Stack(Noisy(lambda args: (0, "") if "compose" in args and "ps" in args else (1, "")), run, []))
        self.assertEqual(failed(run), ["second start of the stack"])


class CaddyRestart(unittest.TestCase):
    def restart(self, answer: Callable[[list[str]], tuple[int, str]]) -> Record:
        module = runner("stack_caddy")
        run = record()
        stack = module.Stack(Noisy(answer), run, [], ["docker", "compose"])
        with (
            mock.patch.object(module.Stack, "wait_healthy", return_value=1.0),
            mock.patch.object(module, "start_standins"),
            mock.patch.object(module, "authority", return_value="root"),
        ):
            module.restart_cases(stack, [], {}, SimpleNamespace(name="down"), "root")
        return run

    def test_the_same_container_after_the_restart_passes(self) -> None:
        self.assertEqual(failed(self.restart(lambda args: (0, "abc123\n") if "ps" in args else (0, ""))), [])

    def test_a_container_that_cannot_be_listed_is_not_the_same_container(self) -> None:
        self.assertEqual(failed(self.restart(lambda args: (0, ""))), ["restart"])
        self.assertEqual(failed(self.restart(lambda args: (1, "") if "ps" in args else (0, ""))), ["restart"])


def tailscale(code: int, text: str) -> tuple[ModuleType, Any]:
    module = runner("stack_caddy")
    target = Noisy(lambda args: (code, text), warning="")
    return module, target


STATUS = json.dumps({"Self": {"DNSName": "h.ts.net.", "TailscaleIPs": ["100.1.1.1", "fd7a::1"]}}) + "\n"


class TailscaleFacts(unittest.TestCase):
    def facts(self, code: int, text: str) -> tuple[Any, Record]:
        module, target = tailscale(code, text)
        run = record()
        return module.host_facts(target, run), run

    def test_a_status_from_a_read_that_exited_0_gives_the_name_and_the_address(self) -> None:
        found, run = self.facts(0, STATUS)
        self.assertEqual(found, ("h.ts.net", "100.1.1.1"))
        self.assertEqual(failed(run), [])

    def test_a_status_from_a_read_that_failed_gives_nothing_although_it_is_valid_json(self) -> None:
        for code in (1, 255):
            with self.subTest(code=code):
                found, run = self.facts(code, STATUS)
                self.assertIsNone(found)
                self.assertEqual(failed(run), ["host's Tailscale facts"])

    def test_an_empty_or_unparseable_or_incomplete_status_gives_nothing(self) -> None:
        for text in (
            "",
            "\n",
            "not json",
            "{}",
            '{"Self": {"DNSName": "h.ts.net."}}',
            '{"Self": {"DNSName": "h.ts.net.", "TailscaleIPs": ["fd7a::1"]}}',
            "[]",
            "null",
        ):
            with self.subTest(text=text):
                found, run = self.facts(0, text)
                self.assertIsNone(found)
                self.assertEqual(failed(run), ["host's Tailscale facts"])


class TargetWithSplit:
    """A target whose ``split`` gives the exit code, the standard output and the standard error that ``respond`` makes from the command."""

    name = "local Docker"

    def __init__(self, respond: Callable[[list[str]], tuple[int, str, str]]) -> None:
        self.respond = respond
        self.calls: list[list[str]] = []

    def split(self, args: list[str], *, timeout: float = 0) -> tuple[int, str, str]:
        self.calls.append(args)
        return self.respond(args)

    def run(self, args: list[str], *, timeout: float = 0) -> tuple[int, str]:
        code, out, err = self.split(args, timeout=timeout)
        return code, out + err

    def read(self, args: list[str], *, timeout: float = 0) -> tuple[int, str]:
        code, out, _ = self.split(args, timeout=timeout)
        return code, out


WILDCARD = str(ipaddress.ip_address(0))
ADDRESS_LINE = "caddy: TS_ADDRESS must be the host's Tailscale address"
IPV4_LINE = "caddy: TS_ADDRESS is not an IPv4 address"
NAME_LINE = "caddy: TS_HOST_NAME is not a lower case host name"
NAME_PATTERN = re.compile(r"[a-z0-9]([a-z0-9.-]*[a-z0-9])?")
ADDRESS_PATTERN = re.compile(r"[0-9]{1,3}(\.[0-9]{1,3}){3}")


def guard(command: list[str]) -> tuple[str, str]:
    """What the service's command does with the two settings: the guard line it prints on stderr, or the line of Caddy's start."""
    values = {item.split("=", 1)[0]: item.split("=", 1)[1] for item in command if re.fullmatch(r"TS_[A-Z_]+=.*", item)}
    address, name = values.get("TS_ADDRESS", "100.64.0.1"), values.get("TS_HOST_NAME", "host.ts.net")
    if address in ("", WILDCARD) or address.startswith("127."):
        return "guard", ADDRESS_LINE
    if not ADDRESS_PATTERN.fullmatch(address):
        return "guard", IPV4_LINE
    if not NAME_PATTERN.fullmatch(name):
        return "guard", NAME_LINE
    return "start", "serving initial configuration"


class GuardLines(unittest.TestCase):
    """The refusal cases match the fixed guard line, as a whole line of standard error, for the setting that was made unusable."""

    def run_cases(self, respond: Callable[[list[str], str, str], tuple[int, str, str]]) -> Record:
        module = runner("stack_caddy")
        run = record()

        def answer(args: list[str]) -> tuple[int, str, str]:
            kind, line = guard(args)
            return respond(args, kind, line)

        stack = module.Stack(TargetWithSplit(answer), run, [], ["docker", "compose"])
        module.refusal_cases(stack)
        return run

    def service(self, args: list[str], kind: str, line: str) -> tuple[int, str, str]:
        return (1, "", line + "\n") if kind == "guard" else (0, "", line + "\n")

    def test_the_service_file_prints_the_three_fixed_lines(self) -> None:
        text = (ROOT / "compose" / "services" / "caddy.yml").read_text()
        for line in (ADDRESS_LINE, IPV4_LINE, NAME_LINE):
            self.assertIn(f'echo "{line}" >&2', text)

    def test_the_service_that_prints_its_line_on_stderr_passes_every_case(self) -> None:
        run = self.run_cases(self.service)
        self.assertEqual(failed(run), [])
        self.assertEqual(len(run.observations), 10)

    def test_a_warning_of_compose_on_stderr_does_not_change_the_result(self) -> None:
        run = self.run_cases(lambda args, kind, line: (1, "", f'time="x" level=warning msg="The \\"TS_ADDRESS\\" variable is not set"\n{line}\n'))
        self.assertEqual(failed(run), [])

    def test_a_compose_warning_that_names_the_variable_is_not_the_guard_line(self) -> None:
        run = self.run_cases(lambda args, kind, line: (1, "", 'level=warning msg="The \\"TS_ADDRESS\\" variable is not set. TS_HOST_NAME too"\n'))
        self.assertEqual(len(failed(run)), 10)

    def test_the_guard_line_on_standard_output_does_not_pass(self) -> None:
        run = self.run_cases(lambda args, kind, line: (1, line + "\n", ""))
        self.assertEqual(len(failed(run)), 10)

    def test_a_line_that_only_contains_the_guard_line_does_not_pass(self) -> None:
        run = self.run_cases(lambda args, kind, line: (1, "", f"error: {line}\n"))
        self.assertEqual(len(failed(run)), 10)

    def test_the_line_of_another_guard_does_not_pass(self) -> None:
        run = self.run_cases(lambda args, kind, line: (1, "", f"{NAME_LINE if line != NAME_LINE else IPV4_LINE}\n"))
        self.assertEqual(len(failed(run)), 10)

    def test_a_start_of_caddy_or_a_repeated_value_or_another_exit_code_does_not_pass(self) -> None:
        run = self.run_cases(lambda args, kind, line: (0, "", line + "\n"))
        self.assertEqual(len(failed(run)), 10)
        run = self.run_cases(lambda args, kind, line: (1, "", line + "\nserving initial configuration\n"))
        self.assertEqual(len(failed(run)), 10)
        run = self.run_cases(lambda args, kind, line: (2, "", line + "\n"))
        self.assertEqual(len(failed(run)), 10)
        run = self.run_cases(lambda args, kind, line: (1, "", f"{line}\nvalue was {next(item for item in args if item.startswith('TS_')).split('=', 1)[1]}\n"))
        self.assertEqual(len(failed(run)), 8)


class AnsibleRecap(unittest.TestCase):
    """The recap of an Ansible run is read from its standard output, and the messages of a failed run from both streams."""

    RECAP = "localhost : ok=3 changed=0 unreachable=0 failed=0 skipped=0 rescued=0 ignored=0\n"

    def test_the_recap_is_the_one_on_the_standard_output(self) -> None:
        for name in ("stack_caddy", "stack_openbao"):
            module = runner(name)
            with self.subTest(runner=name):
                self.assertEqual(module.ansible_recap("TASK [x]\nlocalhost : ok=3 changed=2 unreachable=0 failed=1 skipped=0\n"), (2, 0, 1))

    def test_no_recap_is_a_recap_that_equals_no_expectation(self) -> None:
        for name in ("stack_caddy", "stack_openbao"):
            module = runner(name)
            with self.subTest(runner=name):
                found = module.ansible_recap("TASK [x]\n")
                self.assertEqual(found, (-1, -1, -1))
                self.assertNotEqual(found, (0, 0, 0))

    def test_a_recap_on_standard_error_is_not_read_in_the_openbao_runner(self) -> None:
        module = runner("stack_openbao")
        with tempfile.TemporaryDirectory() as folder:
            scratch = Path(folder)
            result = SimpleNamespace(returncode=0, stdout="TASK [x]\n", stderr=self.RECAP)
            with mock.patch.object(module, "run_command", lambda *a, **k: result):
                code, output, recap = module.run_role(scratch, "x", {}, "bootstrap.yml")
        self.assertEqual((code, recap), (0, (-1, -1, -1)))
        self.assertIn("changed=0", output)

    def test_a_recap_on_standard_error_does_not_pass_the_second_run_of_the_caddy_settings_task(self) -> None:
        module = runner("stack_caddy")
        run = record()
        result = SimpleNamespace(returncode=0, stdout="TASK [x]\n", stderr=self.RECAP)
        with tempfile.TemporaryDirectory() as folder, mock.patch.object(module, "run_command", lambda *a, **k: result):
            module.host_settings_cases(run, [], Path(folder))
        self.assertIn("second run of the host settings task", failed(run))

    def test_a_recap_on_standard_output_does_pass_the_second_run_of_the_caddy_settings_task(self) -> None:
        module = runner("stack_caddy")
        run = record()
        result = SimpleNamespace(returncode=0, stdout="TASK [x]\n" + self.RECAP, stderr="")
        with tempfile.TemporaryDirectory() as folder, mock.patch.object(module, "run_command", lambda *a, **k: result):
            module.host_settings_cases(run, [], Path(folder))
        self.assertNotIn("second run of the host settings task", failed(run))


class MonitoringAbsence(unittest.TestCase):
    """A query that failed does not read as "no series" or "no lines", and a grafana password file that read as empty twice is not "kept"."""

    def stack(self, status: int, text: str) -> Any:
        module = runner("stack_monitoring")
        target = SimpleNamespace(
            http=lambda port, method, path, **keywords: (status, text),
            ports={"prometheus": 9090, "loki": 3100},
            project="p",
            compose_directory="/c",
            secrets_directory="/s",
            env_files=(),
        )
        return module.Stack(target, record(), [])

    PROMETHEUS_EMPTY = json.dumps({"status": "success", "data": {"resultType": "vector", "result": []}})
    PROMETHEUS_ONE = json.dumps({"status": "success", "data": {"resultType": "vector", "result": [{"metric": {}, "value": [1, "1"]}]}})
    LOKI_EMPTY = json.dumps({"status": "success", "data": {"resultType": "streams", "result": []}})
    LOKI_ONE = json.dumps({"status": "success", "data": {"resultType": "streams", "result": [{"stream": {"a": "b"}, "values": [["1", "line"]]}]}})

    def test_a_query_with_no_series_is_absent_and_one_with_a_series_is_not(self) -> None:
        self.assertTrue(self.stack(200, self.PROMETHEUS_EMPTY).no_series("x"))
        self.assertFalse(self.stack(200, self.PROMETHEUS_ONE).no_series("x"))

    def test_a_query_that_failed_is_not_absent(self) -> None:
        for status, text in ((0, "no answer (curl exit 7)"), (503, ""), (500, self.PROMETHEUS_EMPTY), (200, ""), (200, "not json"), (200, "{}")):
            with self.subTest(status=status, text=text):
                self.assertFalse(self.stack(status, text).no_series("x"))

    def test_a_loki_query_with_no_lines_is_absent_and_one_with_a_line_is_not(self) -> None:
        self.assertTrue(self.stack(200, self.LOKI_EMPTY).no_lines('{a="b"}', 1, 2))
        self.assertFalse(self.stack(200, self.LOKI_ONE).no_lines('{a="b"}', 1, 2))

    def test_a_loki_query_that_failed_is_not_absent(self) -> None:
        for status, text in ((0, "no answer (curl exit 7)"), (503, ""), (500, self.LOKI_EMPTY), (200, ""), (200, "not json"), (200, "{}")):
            with self.subTest(status=status, text=text):
                self.assertFalse(self.stack(status, text).no_lines('{a="b"}', 1, 2))

    def test_the_positive_reads_still_give_an_empty_list_when_the_query_failed(self) -> None:
        stack = self.stack(503, "")
        self.assertEqual(stack.prometheus("x"), [])
        self.assertEqual(stack.prometheus_range("x", 1.0, 2.0), [])
        self.assertEqual(stack.loki_lines('{a="b"}', 1, 2), [])
        stack = self.stack(200, self.LOKI_ONE)
        self.assertEqual(stack.loki_lines('{a="b"}', 1, 2), [(1, {"a": "b"}, "line")])

    def test_every_conclusion_of_absence_in_the_runner_goes_through_the_two_checks(self) -> None:
        runner("stack_monitoring")
        text = (E2E / "stack_monitoring.py").read_text()
        self.assertEqual(text.count(".no_series("), 2)
        self.assertEqual(text.count(".no_lines("), 1)
        self.assertNotIn("not stray", text)
        self.assertNotIn("not unknown", text)
        self.assertNotIn("not absent", text)

    def test_the_recap_of_the_grafana_task_is_read_from_the_standard_output(self) -> None:
        module = runner("stack_monitoring")
        result = SimpleNamespace(returncode=0, stdout="TASK [x]\n", stderr="localhost : ok=1 changed=3 unreachable=0 failed=0\n")
        with tempfile.TemporaryDirectory() as folder, mock.patch.object(module, "run_command", lambda *a, **k: result):
            _, _, recap = module.run_playbook(Path(folder), "x", {})
        self.assertEqual(recap, (-1, -1, -1))


class GrafanaPasswordFile(unittest.TestCase):
    def cases(self, content: str) -> Record:
        module = runner("stack_monitoring")
        run = record()
        with tempfile.TemporaryDirectory() as folder:
            secrets = Path(folder) / "secrets"
            (secrets / "grafana").mkdir(parents=True)
            path = secrets / "grafana" / "admin.password"
            path.write_text(content)
            path.chmod(0o400)
            with mock.patch.object(module, "run_playbook", lambda *a, **k: (0, "", (1, 0, 0) if not run.observations else (0, 0, 0))):
                module.grafana_password_file(SimpleNamespace(record=run), Path(folder), secrets)
        return run

    def test_a_password_that_is_kept_passes_the_second_run(self) -> None:
        self.assertNotIn("Grafana password file, second run", failed(self.cases("p" * 40)))

    def test_a_file_that_is_empty_twice_does_not_pass_the_second_run_as_kept(self) -> None:
        self.assertIn("Grafana password file, second run", failed(self.cases("")))


def nessie_answers(*, failing: tuple[str, ...] = (), leak: str = "", logs: str = "ready") -> Callable[[list[str]], tuple[int, str]]:
    """A host whose diagnostics hold ``logs`` and ``leak``, and whose commands named in ``failing`` (``inspect``, ``logs``, ``config``, ``ps``) fail."""

    def answer(args: list[str]) -> tuple[int, str]:
        if args[:1] == ["cat"]:
            return 0, f"pw-{args[1].rsplit('/', 1)[1].removesuffix('.password')}\n"
        if "compose" in args and "ps" in args and "--quiet" in args:
            return 0, "abc123\n"
        for command, text in (("inspect", "{}"), ("logs", logs), ("config", "services: {}"), ("ps", "NAME")):
            if command in args:
                return (1, "") if command in failing else (0, text + leak + "\n")
        return 0, ""

    return answer


class NessieDiagnostics(unittest.TestCase):
    def stack(self, **keywords: Any) -> tuple[ModuleType, Any]:
        module = runner("stack_nessie")
        return module, module.Stack(Noisy(nessie_answers(**keywords)), record(), [])

    def test_diagnostics_that_all_succeeded_and_hold_no_password_pass(self) -> None:
        module, stack = self.stack()
        module.no_password_outside_its_file(stack, [])
        self.assertEqual(failed(stack.record), [])

    def test_a_password_in_a_diagnostic_fails(self) -> None:
        for leak in ("pw-nessie", "pw-postgres"):
            with self.subTest(leak=leak):
                module, stack = self.stack(leak=leak)
                module.no_password_outside_its_file(stack, [])
                self.assertEqual(failed(stack.record), ["no password outside its file"])

    def test_a_diagnostic_that_failed_does_not_read_as_no_password(self) -> None:
        for command in ("inspect", "logs", "config", "ps"):
            with self.subTest(command=command):
                module, stack = self.stack(failing=(command,))
                module.no_password_outside_its_file(stack, [])
                self.assertEqual(failed(stack.record), ["no password outside its file"])
                self.assertIn(command, only(stack.record, "no password outside its file").observed)

    def wrong_password(self, **keywords: Any) -> Record:
        module, stack = self.stack(**keywords)
        client = SimpleNamespace()
        with tempfile.TemporaryDirectory() as folder, mock.patch.object(module.Stack, "wait_healthy", return_value=None):
            module.wrong_password(stack, Path(folder), {}, client, [])
        run: Record = stack.record
        return run

    def test_the_wrong_password_case_passes_with_diagnostics_that_name_the_refusal_and_hold_no_password(self) -> None:
        run = self.wrong_password(logs="FATAL: password authentication failed for user nessie")
        self.assertNotIn("Nessie with a wrong database password", failed(run))

    def test_the_wrong_password_case_fails_when_a_diagnostic_failed(self) -> None:
        for command in ("inspect", "config", "ps"):
            with self.subTest(command=command):
                run = self.wrong_password(failing=(command,), logs="FATAL: password authentication failed for user nessie")
                self.assertIn("Nessie with a wrong database password", failed(run))

    def test_the_wrong_password_case_fails_when_the_log_read_failed_although_it_is_not_known_to_hold_a_password(self) -> None:
        run = self.wrong_password(failing=("logs",))
        self.assertIn("Nessie with a wrong database password", failed(run))


class OpenbaoSecretIds(unittest.TestCase):
    """The second bootstrap and the second preparation keep the secret IDs only when each read succeeded and printed an ID."""

    def run_idempotence(self, read_text: Callable[[str], str]) -> Record:
        module = runner("stack_openbao")
        run = record()
        stack = SimpleNamespace(
            record=run,
            tools=["nessie"],
            services=["nessie"],
            target=SimpleNamespace(secrets_directory="/s", read_text=read_text),
            secret_id=lambda name: read_text(f"/s/openbao/{name}.secret_id").strip(),
            login=lambda role, secret: (200, "token", {}),
            api=lambda *a, **k: (404, {}),
        )
        with tempfile.TemporaryDirectory() as folder, mock.patch.object(module, "run_role", lambda *a, **k: (0, module.EMPTY_ACTIONS, (0, 0, 0))):
            module.idempotence(stack, Path(folder), {})
        return run

    def test_ids_that_are_read_twice_and_are_the_same_pass(self) -> None:
        run = self.run_idempotence(lambda path: "id-" + path.rsplit("/", 1)[1] + "\n")
        self.assertEqual(set(failed(run)) & {"second bootstrap", "second preparation"}, set())

    def test_ids_that_changed_fail(self) -> None:
        reads = iter(range(1000))
        run = self.run_idempotence(lambda path: f"id-{next(reads)}\n")
        self.assertIn("second bootstrap", failed(run))

    def test_files_that_read_as_empty_twice_do_not_pass_as_kept(self) -> None:
        run = self.run_idempotence(lambda path: "")
        self.assertEqual(set(failed(run)) & {"second bootstrap", "second preparation"}, {"second bootstrap", "second preparation"})


if __name__ == "__main__":
    unittest.main()
