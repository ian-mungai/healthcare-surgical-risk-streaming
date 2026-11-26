"""The host modes of the secret sink runner read a container's processes from the host, and read the host's public address without the instance metadata.

The runner stays untracked, so each test skips when it is not on this machine. The host and AWS are stand-ins. The script that reads the processes on the
host is the runner's own and runs in a real ``sh`` against a fake process folder. No SSH, Docker, AWS or host runs here.

Failure modes covered here:

- A container whose image holds no shell (loki) cannot be entered, so the sweep execs ``sh``, gets exit 127 and never reads its command lines or its
  environment. The host reads them from its own ``/proc`` for the processes that ``docker top`` names, and enters no container.
- A secret in the command line or the environment of such a container is not found, or the unlock key is allowed in another read than OpenBao's environment.
- ``docker top`` fails, names no process or answers with something that is no process number, and the empty or partial read passes. A text that is no
  number reaches the host's shell.
- A process of either listing that no read read passes: one that is gone, one that is there and cannot be read, or one that only the second listing names
  and that is never read, so a secret in it is not searched. A process that no read reported is not read again.
- Two containers of one Compose service (a one-off beside a retained one) are taken for one, so only one of them is inspected and its processes read. A
  retained stopped container, which has no process, fails the sweep or hides a service that has no running container. A line of the container listing that
  is not understood is dropped and the sweep passes.
- The public address is asked of the instance metadata, which the host's metadata guard drops, so the access test cannot read it. EC2 is asked with the
  project profile for the instance that the host names as itself.
- The instance ID or the answer of EC2 cannot be read or is not one IPv4 address, and the access test passes with nothing probed.
"""

from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any, cast
from unittest import mock

from scripts.process import run_command
from tests.e2e.run_record import Record
from tests.fast.runner_import import import_runner
from tests.fast.secret_inputs import Marker, make_marker

SHELL_LESS = "loki"
TOP_FORM = ["docker", "top"]
INSTANCE = "i-0123456789abcdef0"
PUBLIC = "203.0.113.7"
PEER = "100.64.0.9"


class FakeHost:
    """The live host: it answers each command of the sweep, keeps what was sent and runs the process read script against a fake process folder."""

    name = "live host"
    compose_directory = "/opt/platform/compose"
    secrets_directory = "/opt/platform/secrets"
    project = "surgical_risk"
    ports: dict[str, int] = {}
    stack: Any

    def __init__(self, tree: Path, containers: dict[str, list[str]]) -> None:
        self.tree = tree
        self.containers = containers
        self.rows = [(service, f"id_{service}", "running") for service in containers]
        self.pids = {f"id_{service}": list(numbers) for service, numbers in containers.items()}
        self.health_by_id: dict[str, str] = {}
        self.listing_extra = ""
        self.sent: list[list[str]] = []
        self.process_reads: list[list[str]] = []
        self.top_reply: Callable[[str, int], tuple[int, str]] | None = None
        self.tops: dict[str, int] = {}
        self.silent: set[str] = set()

    def add(self, service: str, container: str, numbers: list[str], state: str = "running") -> None:
        self.rows.append((service, container, state))
        self.pids[container] = numbers

    def process(self, number: str, command_line: str | None = "server", environment: str | None = "PATH=/usr/bin") -> None:
        """A process of the fake folder. A file given as None is a folder, so the process is there and that file cannot be read."""
        folder = self.tree / number
        folder.mkdir()
        for name, text, end in (("cmdline", command_line, "\0"), ("environ", environment, "\0")):
            if text is None:
                (folder / name).mkdir()
            else:
                (folder / name).write_text(text + end)

    def capture(
        self, args: list[str], *, stdin: str | None = None, timeout: float = 60, cwd: Path | None = None, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        self.sent.append(list(args))
        if args[:2] == ["docker", "compose"]:
            if "ps" in args:
                return 0, "".join(f"{service} {container} {state}\n" for service, container, state in self.rows) + self.listing_extra, ""
            if "exec" in args:
                service = next(word for word in args[args.index("exec") :] if word in self.containers)
                if service == SHELL_LESS:
                    return 127, 'OCI runtime exec failed: exec failed: unable to start container process: exec: "sh": executable file not found in $PATH', ""
                return 0, "", ""
            return 0, "text of the project\n", ""
        if args[:4] == ["docker", "inspect", "--format", "{{.State.Health.Status}}"]:
            return 0, f"{self.health_by_id.get(args[4], 'healthy')}\n", ""
        if args[:2] == TOP_FORM:
            container = args[2]
            call = self.tops[container] = self.tops.get(container, 0) + 1
            if self.top_reply is not None:
                code, listed = self.top_reply(container.removeprefix("id_"), call)
                return code, listed, ""
            if next(state for _, known, state in self.rows if known == container) != "running":
                return 1, "", "Error response from daemon: container is not running"
            return 0, "".join(f"{line}\n" for line in ["PID", *self.pids[container]]), ""
        if args[:2] == ["sh", "-c"] and len(args) > 5 and ('"$p/environ"' in args[2] or '"$p/cmdline"' in args[2]):
            self.process_reads.append(list(args))
            result = run_command("sh", ["-c", args[2], args[3], str(self.tree), *args[5:]], timeout=30, keep_newlines=True)
            errors = "".join(line for line in result.stderr.splitlines(keepends=True) if line.strip().strip("<>") not in self.silent)
            self.silent.clear()
            return result.returncode, result.stdout, errors
        return 0, "a line of a sink\n", ""

    def run(self, args: list[str], *, stdin: str | None = None, timeout: float = 60, marker_source: bool = False) -> tuple[int, str]:
        code, out = self.stack.execute("live host: command", args, stdin=stdin, timeout=timeout, marker_source=marker_source)
        return int(code), str(out)

    def entered(self) -> list[list[str]]:
        return [args for args in self.sent if args[:2] == ["docker", "compose"] and "exec" in args]


class HostCase(unittest.TestCase):
    def setUp(self) -> None:
        self.module: ModuleType = import_runner("secret_sinks")
        self.tree = Path(tempfile.mkdtemp(prefix="secret_sinks_fake_proc_"))
        self.addCleanup(shutil.rmtree, self.tree)

    def host(self, **containers: list[str]) -> tuple[FakeHost, Any]:
        host = FakeHost(self.tree, containers or {SHELL_LESS: ["101"], "openbao": ["201", "202"]})
        stack = self.module.Stack(cast("Any", host), Record(feature="x", slug="x", rerun="x"), [])
        return host, stack


class ContainerProcesses(HostCase):
    def deployed(self, **kwargs: Any) -> tuple[FakeHost, Any]:
        host, stack = self.host()
        host.process("101", "/usr/bin/loki -config.file=/etc/loki/config.yml", kwargs.get("loki_environment", "PATH=/usr/bin"))
        host.process("201", kwargs.get("openbao_line", "bao server"), kwargs.get("openbao_environment", "HOME=/openbao"))
        host.process("202")
        return host, stack

    def test_every_container_is_read_with_no_failure_and_none_is_entered(self) -> None:
        host, stack = self.deployed()
        sizes, failed = self.module.collect_host(stack)
        self.assertEqual(failed, [])
        self.assertEqual(stack.failures, [])
        for service in (SHELL_LESS, "openbao"):
            self.assertGreater(sizes[f"command lines of {service}"], 0, service)
            self.assertGreater(sizes[f"environment of {service}"], 0, service)
        self.assertEqual(host.entered(), [])
        self.assertEqual(set(host.tops), {f"id_{SHELL_LESS}", "id_openbao"})

    def test_a_secret_in_the_environment_or_the_command_line_of_the_shell_less_container_is_found(self) -> None:
        for keyword, read in (("loki_environment", "environment"), ("line", "command lines")):
            with self.subTest(read=read):
                marker = Marker("canary", make_marker("urlsafe").value)
                for entry in self.tree.iterdir():
                    shutil.rmtree(entry)
                host, stack = self.host()
                host.process("101", f"loki --password={marker.value}" if keyword == "line" else "loki", f"A={marker.value}" if keyword != "line" else "A=1")
                host.process("201")
                host.process("202")
                stack.markers.append(marker)
                self.module.collect_host(stack)
                self.assertTrue(any("canary" in leak for leak in stack.leaks))

    def test_the_unlock_key_is_allowed_in_the_environment_of_openbao_only(self) -> None:
        key = Marker("openbao unlock key", make_marker("hex64").value)
        for where, leaked in (("openbao_environment", False), ("openbao_line", True), ("loki_environment", True)):
            with self.subTest(where=where):
                for entry in self.tree.iterdir():
                    shutil.rmtree(entry)
                _, stack = self.deployed(**{where: f"KEY={key.value}"})
                stack.markers.append(key)
                self.module.collect_host(stack)
                self.assertEqual(bool(stack.leaks), leaked)
                self.assertEqual(stack.unlock_seen, not leaked)

    def read(self, stack: Any, service: str = SHELL_LESS) -> tuple[int, str]:
        code, out = self.module.read_host_processes(stack, f"id_{service}", self.module.HOST_ENVIRONMENT_READ)
        return int(code), str(out)

    def test_a_listing_that_fails_or_is_no_list_of_process_numbers_fails_the_read_and_nothing_is_read(self) -> None:
        for reply in ((1, ""), (1, "PID\n101\n"), (0, ""), (0, "PID\n"), (0, "101\n"), (0, "PID\n101; id\n"), (0, "PID CMD\n101 loki\n")):
            with self.subTest(reply=reply):
                host, stack = self.host(loki=["101"])
                host.top_reply = lambda service, call, reply=reply: reply
                code, out = self.read(stack)
                self.assertNotEqual(code, 0)
                self.assertEqual((out, host.process_reads), ("", []))

    def test_a_second_listing_that_fails_fails_the_read(self) -> None:
        host, stack = self.host(loki=["101"])
        host.process("101")
        host.top_reply = lambda service, call: (0, "PID\n101\n") if call == 1 else (1, "")
        self.assertNotEqual(self.read(stack)[0], 0)

    def test_a_process_that_is_gone_before_its_read_fails_the_read_whether_the_second_listing_still_names_it_or_not(self) -> None:
        for second in ("PID\n101\n102\n", "PID\n101\n"):
            with self.subTest(second=second):
                for entry in self.tree.iterdir():
                    shutil.rmtree(entry)
                host, stack = self.host(loki=["101", "102"])
                host.process("101", environment="KEPT=one")
                host.top_reply = lambda service, call, second=second: (0, "PID\n101\n102\n" if call == 1 else second)
                code, out = self.read(stack)
                self.assertNotEqual(code, 0)
                self.assertIn("KEPT=one", out)

    def test_a_process_that_only_the_second_listing_names_is_read_and_its_secret_is_found(self) -> None:
        marker = Marker("canary", make_marker("urlsafe").value)
        host, stack = self.host(loki=["101"])
        host.process("101")
        host.process("102", environment=f"LATE={marker.value}")
        host.top_reply = lambda service, call: (0, "PID\n101\n" if call == 1 else "PID\n101\n102\n")
        stack.markers.append(marker)
        code, out = self.read(stack)
        self.assertEqual((code, "LATE=" in out), (0, True))
        self.assertTrue(any("canary" in leak for leak in stack.leaks))
        self.assertEqual([args[5:] for args in host.process_reads], [["101"], ["102"]])

    def test_a_process_that_only_the_second_listing_names_and_that_is_gone_fails_the_read(self) -> None:
        host, stack = self.host(loki=["101"])
        host.process("101")
        host.top_reply = lambda service, call: (0, "PID\n101\n" if call == 1 else "PID\n101\n102\n")
        self.assertNotEqual(self.read(stack)[0], 0)

    def test_a_process_that_cannot_be_read_fails(self) -> None:
        host, stack = self.host(loki=["101", "102"])
        host.process("101", environment="KEPT=one")
        host.process("102", environment=None)
        code, out = self.read(stack)
        self.assertNotEqual(code, 0)
        self.assertIn("KEPT=one", out)

    def test_a_process_that_no_second_read_reports_fails_beside_one_that_was_read(self) -> None:
        host, stack = self.host(loki=["101", "102"])
        host.process("101")
        host.process("102")
        original = host.capture

        def unreported(args: list[str], **options: Any) -> tuple[int, str, str]:
            host.silent = {"102"}
            return original(args, **options)

        host.capture = unreported  # type: ignore[method-assign]
        self.assertNotEqual(self.read(stack)[0], 0)

    def test_a_read_that_reaches_no_process_fails(self) -> None:
        _, stack = self.host(loki=["101", "102"])
        self.assertNotEqual(self.read(stack)[0], 0)

    def test_a_process_that_no_read_reported_is_read_again(self) -> None:
        host, stack = self.host(loki=["101", "102"])
        host.process("101", environment="FIRST=one")
        host.process("102", environment="SECOND=two")
        host.silent = {"102"}
        code, out = self.read(stack)
        self.assertEqual(code, 0)
        self.assertEqual([args[5:] for args in host.process_reads], [["101", "102"], ["102"]])
        self.assertEqual(out.count("SECOND=two"), 2)

    def test_a_process_that_two_reads_do_not_report_fails(self) -> None:
        host, stack = self.host(loki=["101"])
        host.process("101")
        original = host.capture

        def unreported(args: list[str], **options: Any) -> tuple[int, str, str]:
            host.silent = {"101"}
            return original(args, **options)

        host.capture = unreported  # type: ignore[method-assign]
        self.assertNotEqual(self.read(stack)[0], 0)


class ContainersByID(HostCase):
    def twins(self, state: str = "running", first_environment: str = "PATH=/usr/bin") -> tuple[FakeHost, Any]:
        """Two containers of one service: the one listed first holds process 301, the one listed later holds process 302."""
        host, stack = self.host(spark_bronze=["301"])
        host.add("spark_bronze", "id_later", ["302"] if state == "running" else [], state)
        host.process("301", environment=first_environment)
        host.process("302")
        return host, stack

    def inspected(self, host: FakeHost) -> list[str]:
        return next(args[2:] for args in host.sent if args[:2] == ["docker", "inspect"])

    def test_both_containers_of_one_service_are_inspected_and_their_processes_read(self) -> None:
        host, stack = self.twins()
        sizes, failed = self.module.collect_host(stack)
        self.assertEqual(failed, [])
        self.assertEqual(self.inspected(host), ["id_spark_bronze", "id_later"])
        self.assertEqual(set(host.tops), {"id_spark_bronze", "id_later"})
        self.assertEqual(sizes["container list"], 2)
        for number in (1, 2):
            self.assertGreater(sizes[f"command lines of spark_bronze (container {number})"], 0)
            self.assertGreater(sizes[f"environment of spark_bronze (container {number})"], 0)

    def test_a_secret_in_the_environment_of_the_container_listed_first_is_found(self) -> None:
        marker = Marker("canary", make_marker("urlsafe").value)
        _, stack = self.twins(first_environment=f"A={marker.value}")
        stack.markers.append(marker)
        self.module.collect_host(stack)
        self.assertTrue(any("canary" in leak for leak in stack.leaks))

    def test_a_retained_stopped_container_is_inspected_and_named_and_has_no_process_to_read(self) -> None:
        for state in ("exited", "created", "dead"):
            with self.subTest(state=state):
                for entry in self.tree.iterdir():
                    shutil.rmtree(entry)
                host, stack = self.twins(state)
                _, failed = self.module.collect_host(stack)
                self.assertEqual((failed, stack.failures), ([], []))
                self.assertEqual(self.inspected(host), ["id_spark_bronze", "id_later"])
                self.assertEqual(set(host.tops), {"id_spark_bronze"})
                self.assertEqual(stack.stopped, ["spark_bronze (container 2)"])

    def test_a_container_in_a_state_that_is_not_stopped_is_read_and_a_failed_read_of_it_fails(self) -> None:
        for state in ("removing", "unheard_of"):
            with self.subTest(state=state):
                for entry in self.tree.iterdir():
                    shutil.rmtree(entry)
                host, stack = self.twins(state)
                _, failed = self.module.collect_host(stack)
                self.assertIn("command lines of spark_bronze (container 2)", failed)

    def test_a_line_of_the_listing_that_is_not_understood_fails_the_listing(self) -> None:
        for line in ("grafana id_grafana\n", "grafana id_grafana running extra\n"):
            with self.subTest(line=line):
                for entry in self.tree.iterdir():
                    shutil.rmtree(entry)
                host, stack = self.twins()
                host.listing_extra = line
                self.assertIn("container list", self.module.collect_host(stack)[1])

    def unhealthy(self, stack: Any) -> list[str]:
        return list(self.module.unhealthy_consumers(stack, stack.container_list()[1]))

    def test_a_healthy_consumer_beside_a_retained_stopped_container_is_up(self) -> None:
        host, stack = self.twins("exited")
        host.health_by_id = {"id_later": "unhealthy"}
        self.assertEqual(self.unhealthy(stack), [])

    def test_every_running_container_of_a_service_must_be_healthy(self) -> None:
        for sick, label in (("id_spark_bronze", "spark_bronze (container 1)"), ("id_later", "spark_bronze (container 2)")):
            with self.subTest(sick=sick):
                for entry in self.tree.iterdir():
                    shutil.rmtree(entry)
                host, stack = self.twins()
                host.health_by_id = {sick: "unhealthy"}
                self.assertEqual(self.unhealthy(stack), [label])

    def test_a_service_with_no_running_container_is_not_up(self) -> None:
        host, stack = self.host(grafana=["301"])
        host.rows = [("grafana", "id_grafana", "exited")]
        self.assertEqual(self.unhealthy(stack), ["grafana: no running container"])


class PublicAddress(HostCase):
    def access(
        self, *, instance: tuple[int, str] = (0, f"{INSTANCE}\n"), answer: tuple[int, str] = (0, json.dumps([PUBLIC])), open_ports: tuple[int, ...] = ()
    ) -> Any:
        sent: list[list[str]] = []
        local: list[list[str]] = []
        probed: list[tuple[str, int]] = []

        class Target(FakeHost):
            address = PEER
            configuration = {"AWS_PROFILE": "project-profile", "AWS_REGION": "us-west-2", "ENVIRONMENT": "dev"}

            def capture(self, args: list[str], **options: Any) -> tuple[int, str, str]:
                sent.append(list(args))
                if args == ["cat", "/var/lib/cloud/data/instance-id"]:
                    return instance[0], instance[1], ""
                return 28, "", ""

        def here(args: list[str], **options: Any) -> tuple[int, str, str]:
            local.append(list(args))
            return answer[0], answer[1], ""

        def probe(address: str, port: int) -> bool:
            probed.append((address, port))
            return (address == PEER and port in (22, 9440)) or (address == PUBLIC and port in open_ports)

        target = Target(self.tree, {})
        record = Record(feature="x", slug="x", rerun="x")
        original = self.module.Stack

        def stack(chosen: Any, kept: Record, log: list[str]) -> Any:
            made = original(chosen, kept, log)
            made.local_capture = here
            return made

        with (
            mock.patch.object(self.module, "Remote", lambda: target),
            mock.patch.object(self.module, "probe_port", probe),
            mock.patch.object(self.module, "Stack", stack),
        ):
            self.module.run_host_access(record, [])
        cases = {item.case: item for item in record.observations}
        return cases, sent, local, probed

    def test_ec2_is_asked_with_the_project_profile_for_the_instance_that_the_host_names_and_its_ports_are_probed(self) -> None:
        cases, sent, local, probed = self.access()
        self.assertTrue(cases["public address"].passed, cases["public address"].observed)
        self.assertTrue(cases["no command failed and no marker found"].passed)
        self.assertEqual(
            local,
            [
                [
                    "aws",
                    "ec2",
                    "describe-instances",
                    "--instance-ids",
                    INSTANCE,
                    "--query",
                    "Reservations[].Instances[].PublicIpAddress",
                    "--output",
                    "json",
                    "--profile",
                    "project-profile",
                    "--region",
                    "us-west-2",
                ]
            ],
        )
        self.assertFalse(any("169.254.169.254" in word for args in sent for word in args))
        self.assertTrue({(PUBLIC, 22), (PUBLIC, 5432), (PUBLIC, 9459)} <= set(probed))

    def test_a_port_that_answers_on_the_public_address_fails(self) -> None:
        cases = self.access(open_ports=(9440,))[0]
        self.assertFalse(cases["public address"].passed)
        self.assertIn("9440", cases["public address"].observed)

    def test_an_instance_id_that_cannot_be_read_fails_and_ec2_is_not_asked(self) -> None:
        for instance in ((1, f"{INSTANCE}\n"), (0, ""), (0, "i-xyz\n"), (0, f"{INSTANCE} --profile other\n")):
            with self.subTest(instance=instance):
                cases, _, local, probed = self.access(instance=instance)
                self.assertFalse(cases["public address"].passed)
                self.assertIn("public address read: False", cases["public address"].observed)
                self.assertEqual(local, [])
                self.assertEqual({address for address, _ in probed}, {PEER})

    def test_an_answer_that_is_not_one_ipv4_address_fails(self) -> None:
        answers = ((255, ""), (255, json.dumps([PUBLIC])), (0, ""), (0, "[]"), (0, "[null]"), (0, json.dumps([PUBLIC, "203.0.113.8"])),
                   (0, json.dumps(PUBLIC)), (0, json.dumps(["host.example"])), (0, json.dumps(["2001:db8::1"])), (0, json.dumps([3])))  # fmt: skip
        for answer in answers:
            with self.subTest(answer=answer):
                cases, _, _, probed = self.access(answer=answer)
                self.assertFalse(cases["public address"].passed)
                self.assertEqual({address for address, _ in probed}, {PEER})

    def test_a_failed_aws_call_is_a_failed_command_of_the_run(self) -> None:
        cases = self.access(answer=(255, ""))[0]
        self.assertFalse(cases["no command failed and no marker found"].passed)


if __name__ == "__main__":
    unittest.main()
