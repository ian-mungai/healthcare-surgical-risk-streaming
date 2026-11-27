"""The routes and the files that the record load needs on the host, through the real ``ansible-playbook``.

The code location container reaches Airbyte's API and Airbyte's pods reach Nessie through socket-activated forwarders on the k3s bridge address, in the form
of the PostgreSQL route. A network policy admits the Airbyte namespace to the Nessie forwarder and no pod to the API forwarder. The container reads the
instance's client credentials and the destination's AWS access key from files that its own user owns. Starting the units, applying the policy and reading the
cluster's secret need k3s and are proven on the host; these cases cover what a clean computer shows.
"""

from __future__ import annotations

import base64
import json
import os
import re
import stat
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any

from scripts.process import TimeoutExpired, run_command
from scripts.stack.yaml_text import load_yaml
from tests.slow.ansible_support import environment
from tests.slow.test_airbyte_role import BRIDGE, CHANGED_NONE, ROLE, flatten, run_tasks, values

ROLE_TASKS = ROLE / "tasks"
CLIENT_ID = "client-id-value-1"
CLIENT_VALUE = "client-value-2"
KEY_ID = "AKIAEXAMPLEEXAMPLE0"
KEY_VALUE = "access-value-3"
USER = str(os.getuid())


def mapping(document: object) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise TypeError("expected a mapping")
    return document


def record_values(folder: Path, **changes: object) -> dict[str, object]:
    (folder / "secrets" / "airbyte").mkdir(parents=True, exist_ok=True)
    return (
        values(folder)
        | {
            "airbyte_api_route_port": 8001,
            "airbyte_nessie_route_port": 19120,
            "compose_stack_nessie_port": 19120,
            "airbyte_secrets_directory": str(folder / "secrets"),
            "airbyte_record_container_user": USER,
            "airbyte_record_container_group": str(os.getgid()),
            "airbyte_record_client_id": CLIENT_ID,
            "airbyte_record_client_secret": CLIENT_VALUE,
        }
        | changes
    )


def place_aws_files(folder: Path) -> None:
    """The two key files of the raw connection, as the key script places them: bait that the client files must never read or copy."""
    (folder / "secrets" / "airbyte").mkdir(parents=True, exist_ok=True)
    (folder / "secrets" / "airbyte" / "aws_access_key_id").write_text(KEY_ID + "\n")
    (folder / "secrets" / "airbyte" / "aws_secret_access_key").write_text(KEY_VALUE + "\n")


def unit_files(folder: Path, name: str) -> tuple[str, str]:
    socket, service = (folder / "units" / f"{name}.{kind}" for kind in ("socket", "service"))
    return (socket.read_text() if socket.exists() else "", service.read_text() if service.exists() else "")


class RouteUnitsTest(unittest.TestCase):
    def units(self, route: dict[str, object], **changes: object) -> tuple[int, str, tuple[str, str]]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = run_tasks(["record_route_units.yml"], record_values(folder, **route) | changes)
            return code, output, unit_files(folder, str(route["airbyte_route_unit"]))

    API = {"airbyte_route_unit": "airbyte-api-route", "airbyte_route_listen_port": 8001, "airbyte_route_target_port": 8000}
    NESSIE = {"airbyte_route_unit": "airbyte-nessie-route", "airbyte_route_listen_port": 19120, "airbyte_route_target_port": 19120}

    def test_each_socket_listens_on_the_bridge_address_and_its_port_only_and_may_bind_before_the_bridge_exists(self) -> None:
        for route in (self.API, self.NESSIE):
            with self.subTest(unit=route["airbyte_route_unit"]):
                code, output, (socket, _) = self.units(route)
                self.assertEqual(0, code, output)
                self.assertEqual(
                    [f"ListenStream={BRIDGE}:{route['airbyte_route_listen_port']}"], [line for line in socket.splitlines() if line.startswith("Listen")]
                )
                self.assertIn("FreeBind=true", socket)
                self.assertIn("WantedBy=sockets.target", socket)
                self.assertNotRegex(socket, r"0\.0\.0\.0|\[::\]|ListenStream=\d+\s")

    def test_each_service_forwards_to_the_servers_local_address_with_the_shipped_proxy_and_stays_in_the_host_network(self) -> None:
        for route in (self.API, self.NESSIE):
            with self.subTest(unit=route["airbyte_route_unit"]):
                _, _, (_, service) = self.units(route)
                command = next(line for line in service.splitlines() if line.startswith("ExecStart="))
                self.assertEqual(f"ExecStart=/usr/lib/systemd/systemd-socket-proxyd 127.0.0.1:{route['airbyte_route_target_port']}", command)
                self.assertIn(f"Requires={route['airbyte_route_unit']}.socket", service)
                self.assertIn("Type=notify", service)
                self.assertIn("NoNewPrivileges=yes", service)
                self.assertNotIn("PrivateNetwork", service)

    def test_the_bridge_address_comes_from_the_variable_and_a_second_run_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            run_tasks(["record_route_units.yml"], record_values(folder, **self.API, airbyte_bridge_address="10.42.9.1"))
            socket, _ = unit_files(folder, "airbyte-api-route")
            code, output = run_tasks(["record_route_units.yml"], record_values(folder, **self.API, airbyte_bridge_address="10.42.9.1"))
        self.assertIn("ListenStream=10.42.9.1:8001", socket)
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_NONE)

    def test_a_port_that_is_not_a_port_number_or_a_unit_name_that_is_not_a_plain_name_stops_before_any_unit_is_written(self) -> None:
        for change in (
            {"airbyte_route_listen_port": 0},
            {"airbyte_route_listen_port": 70000},
            {"airbyte_route_target_port": "80;rm"},
            {"airbyte_route_unit": "../x"},
            {"airbyte_route_unit": "a b"},
        ):
            with self.subTest(change=change):
                code, output, (socket, service) = self.units(self.API | change)
                self.assertNotEqual(0, code, output)
                self.assertEqual(("", ""), (socket, service))


class PolicyTest(unittest.TestCase):
    def policy(self, **changes: object) -> tuple[int, str, list[dict[str, Any]]]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = run_tasks(["record_route_policy.yml"], record_values(folder, **changes))
            written = folder / "airbyte" / "record_route_policy.yaml"
            documents = [item for item in re.split(r"^---$", written.read_text(), flags=re.MULTILINE) if item.strip()] if written.exists() else []
            return code, output, [mapping(load_yaml(item)) for item in documents]

    def test_one_policy_admits_the_airbyte_namespace_to_the_bridge_address_on_the_nessie_port_only(self) -> None:
        code, output, documents = self.policy()
        self.assertEqual(0, code, output)
        self.assertEqual(1, len(documents))
        policy = documents[0]
        self.assertEqual(("NetworkPolicy", "airbyte", "nessie-bridge"), (policy["kind"], policy["metadata"]["namespace"], policy["metadata"]["name"]))
        self.assertEqual({}, policy["spec"]["podSelector"])
        self.assertEqual(["Egress"], policy["spec"]["policyTypes"])
        self.assertEqual([{"to": [{"ipBlock": {"cidr": f"{BRIDGE}/32"}}], "ports": [{"protocol": "TCP", "port": 19120}]}], policy["spec"]["egress"])

    def test_no_pod_is_admitted_to_the_api_forwarder_and_no_other_namespace_to_anything(self) -> None:
        _, _, documents = self.policy(airbyte_api_route_port=8001)
        text = json.dumps(documents)
        self.assertNotIn("8001", text)
        self.assertEqual({"airbyte"}, {document["metadata"]["namespace"] for document in documents})

    def test_the_port_and_the_namespace_follow_the_variables(self) -> None:
        _, _, documents = self.policy(airbyte_nessie_route_port=19999, airbyte_namespace="other")
        self.assertEqual("other", documents[0]["metadata"]["namespace"])
        self.assertEqual(19999, documents[0]["spec"]["egress"][0]["ports"][0]["port"])

    def test_a_second_run_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            run_tasks(["record_route_policy.yml"], record_values(folder))
            code, output = run_tasks(["record_route_policy.yml"], record_values(folder))
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_NONE)


class ClientFilesTest(unittest.TestCase):
    def write(self, folder: Path, **changes: object) -> tuple[int, str]:
        return run_tasks(["record_client_files.yml"], record_values(folder, **changes))

    def files(self, folder: Path) -> dict[str, tuple[str, int, int]]:
        found = {}
        for path in sorted((folder / "secrets" / "airbyte_api").glob("*")):
            status = path.stat()
            found[path.name] = (path.read_text(), stat.S_IMODE(status.st_mode), status.st_uid)
        return found

    def test_the_four_files_are_owned_by_the_container_user_with_mode_0400_in_a_folder_only_its_owner_enters_and_no_key_file_is_among_them(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            place_aws_files(folder)
            code, output = self.write(folder)
            found = self.files(folder)
            mode = stat.S_IMODE((folder / "secrets" / "airbyte_api").stat().st_mode)
        self.assertEqual(0, code, output)
        self.assertEqual({"api_url", "nessie_url", "client_id", "client_secret"}, set(found))
        self.assertEqual(
            {
                "api_url": f"http://{BRIDGE}:8001",
                "nessie_url": f"http://{BRIDGE}:19120/api/v2",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_VALUE,
            },
            {name: text.rstrip("\n") for name, (text, _, _) in found.items()},
        )
        self.assertEqual({0o400}, {file_mode for _, file_mode, _ in found.values()})
        self.assertEqual({int(USER)}, {owner for _, _, owner in found.values()})
        self.assertEqual(0o700, mode)

    def test_the_run_never_reads_the_key_files_so_it_succeeds_without_them_and_copies_no_key_value_when_they_exist(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = self.write(folder)
            self.assertEqual(0, code, output)
            self.assertEqual({"api_url", "nessie_url", "client_id", "client_secret"}, set(self.files(folder)), "no key file is needed")
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            place_aws_files(folder)
            code, output = self.write(folder)
            held = "".join(text for text, _, _ in self.files(folder).values())
        self.assertEqual(0, code, output)
        for value in (KEY_ID, KEY_VALUE):
            self.assertNotIn(value, held)
            for form in (value, base64.b64encode(value.encode()).decode(), base64.b64encode((value + "\n").encode()).decode()):
                self.assertNotIn(form, output)

    def test_the_address_file_holds_the_bridge_address_and_the_api_route_port_the_deploy_configured(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            place_aws_files(folder)
            self.write(folder, airbyte_bridge_address="10.42.9.1", airbyte_api_route_port=8123)
            found = self.files(folder)
        self.assertEqual("http://10.42.9.1:8123", found["api_url"][0].rstrip("\n"))
        self.assertEqual("http://10.42.9.1:19120/api/v2", found["nessie_url"][0].rstrip("\n"))

    def test_the_nessie_address_file_follows_the_nessie_route_port(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            place_aws_files(folder)
            self.write(folder, airbyte_nessie_route_port=19999)
            found = self.files(folder)
        self.assertEqual(f"http://{BRIDGE}:19999/api/v2", found["nessie_url"][0].rstrip("\n"))

    def test_a_bridge_address_that_is_not_a_private_IPv4_literal_or_a_port_that_is_not_a_port_stops_the_run_before_any_file_is_placed(self) -> None:
        for change in (
            {"airbyte_bridge_address": "airbyte.example"},
            {"airbyte_bridge_address": "8.8.8.8"},
            {"airbyte_bridge_address": "127.0.0.1"},
            {"airbyte_bridge_address": "10.42.0.1@8.8.8.8"},
            {"airbyte_bridge_address": "10.42.0.1/x"},
            {"airbyte_bridge_address": ""},
            {"airbyte_bridge_address": "172.32.0.1"},
            {"airbyte_bridge_address": "100.64.0.1"},
            {"airbyte_bridge_address": "10.042.0.1"},
            {"airbyte_api_route_port": "8001?"},
            {"airbyte_api_route_port": "8001/"},
            {"airbyte_api_route_port": "8001#"},
            {"airbyte_api_route_port": "08001"},
            {"airbyte_api_route_port": 0},
            {"airbyte_api_route_port": 70000},
            {"airbyte_api_route_port": "80/x"},
            {"airbyte_nessie_route_port": 0},
            {"airbyte_nessie_route_port": "19120/x"},
        ):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as scratch:
                folder = Path(scratch)
                place_aws_files(folder)
                code, output = self.write(folder, **change)
                self.assertNotEqual(0, code, output)
                self.assertEqual({}, self.files(folder))

    def test_no_secret_value_appears_in_the_playbook_output(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            place_aws_files(folder)
            code, output = self.write(folder)
            held = {name: content.rstrip("\n") for name, (content, _, _) in self.files(folder).items()}
        self.assertEqual(0, code, output[-2000:])
        self.assertEqual(
            (CLIENT_ID, CLIENT_VALUE), (held["client_id"], held["client_secret"]), "control: the files hold the values that the output is searched for"
        )
        for value in (CLIENT_ID, CLIENT_VALUE):
            for form in (value, base64.b64encode(value.encode()).decode(), base64.b64encode((value + "\n").encode()).decode()):
                self.assertNotIn(form, output)

    def test_a_second_run_changes_nothing_and_a_rotated_client_value_is_replaced(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            self.write(folder)
            code, output = self.write(folder)
            self.write(folder, **{"airbyte_record_client_secret": "rotated-value-4"})
            rotated = self.files(folder)["client_secret"][0].rstrip("\n")
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_NONE)
        self.assertEqual("rotated-value-4", rotated)

    def test_an_empty_client_credential_stops_the_run(self) -> None:
        for name in ("airbyte_record_client_id", "airbyte_record_client_secret"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as scratch:
                folder = Path(scratch)
                place_aws_files(folder)
                code, output = self.write(folder, **{name: ""})
                self.assertNotEqual(0, code, output)
                self.assertEqual({}, self.files(folder))


class ReadWaitTest(unittest.TestCase):
    """The lookup of the client credentials has one elapsed-time deadline, enforced by ``timeout`` around one loop, and no task outlasts it by a fresh bound."""

    def task(self) -> dict[str, Any]:
        tasks = flatten(load_yaml((ROLE_TASKS / "record_client_read.yml").read_text()))
        return next(task for task in tasks if task.get("name", "").startswith("Wait for and read"))

    def argv(self, deadline: int) -> list[str]:
        words = [
            str(word).replace("{{ airbyte_record_client_deadline_seconds | default(120) }}", str(deadline)).replace("{{ airbyte_namespace }}", "airbyte")
            for word in self.task()["ansible.builtin.command"]["argv"]
        ]
        self.assertNotIn("{{", "".join(words))
        return words

    def fake_k3s(self, folder: Path, ready_after: int) -> dict[str, str]:
        bin_directory = folder / "bin"
        bin_directory.mkdir()
        count = folder / "count"
        count.write_text("0")
        (bin_directory / "k3s").write_text(
            f'#!/bin/sh\nn=$(($(cat {count}) + 1))\necho $n > {count}\nif [ "$n" -ge {ready_after} ]; then printf "aWQ=:c2VjcmV0"; else printf ":"; fi\n'
        )
        (bin_directory / "k3s").chmod(0o755)
        return {"PATH": f"{bin_directory}:/usr/bin:/bin"}

    def run_argv(self, folder: Path, ready_after: int, deadline: int) -> tuple[int, str, float]:
        started = time.monotonic()
        argv = self.argv(deadline)
        result = run_command(argv[0], argv[1:], env=self.fake_k3s(folder, ready_after), timeout=deadline + 20)
        return result.returncode, result.stdout, time.monotonic() - started

    def test_the_command_is_wrapped_by_one_deadline_and_the_task_limit_is_a_little_above_it(self) -> None:
        task = self.task()
        argv = [str(word) for word in task["ansible.builtin.command"]["argv"]]
        self.assertEqual("timeout", argv[0])
        self.assertIn("airbyte_record_client_deadline_seconds | default(120)", argv[1])
        self.assertNotIn("retries", task)
        self.assertNotIn("until", task)
        self.assertIn("airbyte_record_client_deadline_seconds | default(120)", str(task["timeout"]))
        self.assertTrue(str(task["timeout"]).strip().endswith("+ 15 }}") or "+ 15" in str(task["timeout"]))

    def test_control_the_values_are_read_as_soon_as_both_exist(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            code, output, _ = self.run_argv(Path(scratch), 2, 30)
        self.assertEqual((0, "aWQ=:c2VjcmV0"), (code, output))

    def test_a_secret_that_never_holds_them_ends_at_the_one_deadline_with_the_timeout_status(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            code, output, seconds = self.run_argv(Path(scratch), 10**6, 3)
        self.assertEqual(124, code)
        self.assertEqual("", output)
        self.assertGreaterEqual(seconds, 3.0)
        self.assertLess(seconds, 3.0 + 4.0, "the total wait is the deadline and one poll, not a deadline for each attempt")

    def test_only_a_value_with_both_parts_ends_the_wait(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            env = self.fake_k3s(folder, 10**6)
            (folder / "bin" / "k3s").write_text('#!/bin/sh\nprintf "aWQ=:"\n')
            argv = self.argv(2)
            result = run_command(argv[0], argv[1:], env=env, timeout=20)
        self.assertEqual(124, result.returncode)


KUBECTL_ERROR = 'Error from server (Forbidden): secrets "airbyte-auth-secrets" is forbidden: User "nobody" cannot get resource'
NOT_FOUND_ERROR = 'Error from server (NotFound): secrets "airbyte-auth-secrets" not found'
UNREACHABLE_ERROR = "The connection to the server localhost:6443 was refused - did you specify the right host or port?"
LEAKED_PATH = "/srv/example.invalid/kube/leaked-config"
LEAKED_VALUE = "eyJleaked-token-value-7"
MALFORMED_ERROR = f'error: error loading config file "{LEAKED_PATH}": yaml: line 9: bad token: {LEAKED_VALUE}'
READ_ID = "read-id-value-5"
READ_VALUE = "read-value-6"
REASON_MARK = "kubectl-reason:"
FAILURES = {
    "refused": KUBECTL_ERROR,
    "not found": NOT_FOUND_ERROR,
    "unreachable": UNREACHABLE_ERROR,
    "failed": MALFORMED_ERROR,
}
FAIL = "echo '{text}' >&2\nexit 1"
STALL = "exec sleep 100"


def encoded(text: str) -> str:
    return base64.b64encode(text.encode()).decode()


def reasons(errors: str) -> list[str]:
    return [line for line in errors.splitlines() if line.startswith(REASON_MARK)]


class ReadFailureTest(unittest.TestCase):
    """A failed read says why in a fixed word and never in kubectl's own text.

    kubectl prints the path and the token of a malformed kubeconfig, and Ansible 2.22 printed that text from a hidden task. The wait keeps the diagnosis of its
    last completed attempt as one of a few fixed words, in memory only, so no file holds it and no stream carries kubectl's text.
    """

    def argv(self, deadline: int) -> list[str]:
        return ReadWaitTest().argv(deadline)

    def fake_k3s(self, folder: Path, body: str) -> dict[str, str]:
        (folder / "bin").mkdir(exist_ok=True)
        (folder / "bin" / "k3s").write_text(f"#!/bin/sh\n{body}\n")
        (folder / "bin" / "k3s").chmod(0o755)
        return {"PATH": f"{folder / 'bin'}:/usr/bin:/bin"}

    def sequence(self, folder: Path, *steps: str) -> dict[str, str]:
        """A kubectl whose n-th call runs the n-th step and whose later calls repeat the last one."""
        count = folder / "count"
        count.write_text("0")
        cases = "".join(f"  {number}) {step} ;;\n" for number, step in enumerate(steps[:-1], 1))
        body = f"n=$(($(cat {count}) + 1))\necho $n > {count}\ncase $n in\n{cases}  *) {steps[-1]} ;;\nesac"
        return self.fake_k3s(folder, body)

    def run_wait(self, env: dict[str, str], deadline: int = 3, shell: str = "sh") -> tuple[int, str, str]:
        argv = self.argv(deadline)
        self.assertEqual("sh", argv[2])
        argv[2] = shell
        result = run_command(argv[0], argv[1:], env=env, timeout=30)
        return result.returncode, result.stdout, result.stderr

    def playbook(self, folder: Path, env: dict[str, str], **changes: object) -> tuple[int, str]:
        env = environment(folder) | env
        variables = record_values(folder, airbyte_record_client_deadline_seconds=3, **changes)
        playbook = folder / "check.yml"
        include = f"    - ansible.builtin.include_role:\n        name: {ROLE}\n        tasks_from: record_client_read.yml\n"
        playbook.write_text(f"- hosts: localhost\n  connection: local\n  gather_facts: false\n  tasks:\n{include}")
        result = run_command(
            str(Path(__file__).resolve().parents[2] / ".venv/bin/ansible-playbook"),
            ["-v", "-i", "localhost,", "--extra-vars", json.dumps(variables), str(playbook)],
            cwd=folder,
            env=env,
            timeout=120,
        )
        return result.returncode, result.stdout + result.stderr

    def test_each_kind_of_failure_prints_one_fixed_reason_at_the_deadline_and_none_of_kubectls_text(self) -> None:
        for reason, text in FAILURES.items():
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as scratch:
                code, output, errors = self.run_wait(self.fake_k3s(Path(scratch), FAIL.format(text=text)))
                self.assertEqual((124, "", [f"{REASON_MARK}{reason}"]), (code, output, reasons(errors)))
                for fragment in (text, "airbyte-auth-secrets", LEAKED_PATH, LEAKED_VALUE, "nobody", "localhost:6443"):
                    self.assertNotIn(fragment, errors)

    def test_a_refusal_followed_by_a_stalled_retry_keeps_the_refusal_and_the_timeout_status(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            code, output, errors = self.run_wait(self.sequence(Path(scratch), FAIL.format(text=KUBECTL_ERROR), STALL))
        self.assertEqual((124, "", [f"{REASON_MARK}refused"]), (code, output, reasons(errors)))
        self.assertNotIn(KUBECTL_ERROR, errors)

    def test_a_newer_completed_attempt_replaces_the_kept_reason_and_a_stalled_one_does_not(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            steps = (FAIL.format(text=KUBECTL_ERROR), FAIL.format(text=NOT_FOUND_ERROR), STALL)
            _, _, errors = self.run_wait(self.sequence(Path(scratch), *steps), deadline=6)
        self.assertEqual([f"{REASON_MARK}not found"], reasons(errors))

    def test_a_first_attempt_that_stalls_leaves_no_reason_to_show(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            code, output, errors = self.run_wait(self.fake_k3s(Path(scratch), STALL))
        self.assertEqual((124, "", [f"{REASON_MARK}none"]), (code, output, reasons(errors)))

    def test_an_attempt_killed_after_it_wrote_the_values_keeps_the_last_completed_reason_and_shows_no_value(self) -> None:
        """The attempt's own subshell is killed after kubectl wrote both values: what it captured is not a completed attempt, so it never becomes the reason."""
        pair = f"{encoded(READ_ID)}:{encoded(READ_VALUE)}"
        # The waiter is the shell whose parent is ``timeout``; the attempt's subshell is the waiter's direct child in this stand-in's ancestry, at the depth
        # that the shell puts it. At most 10 steps up, so a changed layout fails the case instead of killing the waiter.
        child_of_waiter = (
            "p=$$; i=0; while [ $i -lt 10 ]; do pp=$(ps -o ppid= -p $p | tr -d ' '); ppp=$(ps -o ppid= -p $pp | tr -d ' '); "
            'case "$(ps -o comm= -p $ppp)" in *timeout) kill -9 $p; break ;; esac; p=$pp; i=$((i + 1)); done'
        )
        killed = f"printf '%s' '{pair}'\n{child_of_waiter}\nsleep 1"
        # The Ubuntu CI job's /bin/sh is dash, whose subshells differ from bash's in depth: the case runs under each shell that this computer has.
        shells = ["sh", *(["/bin/dash"] if Path("/bin/dash").exists() else [])]
        cases = {"first attempt": (killed, STALL), "after a refusal": (FAIL.format(text=KUBECTL_ERROR), killed, STALL)}
        for shell, (label, steps) in ((shell, case) for shell in shells for case in cases.items()):
            with self.subTest(label, shell=shell), tempfile.TemporaryDirectory() as scratch:
                code, output, errors = self.run_wait(self.sequence(Path(scratch), *steps), deadline=6, shell=shell)
                expected = "none" if label == "first attempt" else "refused"
                self.assertEqual((124, "", [f"{REASON_MARK}{expected}"]), (code, output, reasons(errors)))
                for fragment in (encoded(READ_ID), encoded(READ_VALUE), READ_ID, READ_VALUE):
                    self.assertNotIn(fragment, errors)

    def test_values_that_end_with_a_newline_are_read(self) -> None:
        pair = f"{encoded(READ_ID)}:{encoded(READ_VALUE)}"
        with tempfile.TemporaryDirectory() as scratch:
            code, output, errors = self.run_wait(self.fake_k3s(Path(scratch), f"printf '%s\\n' '{pair}'"))
        self.assertEqual((0, pair, ""), (code, output, errors))

    def test_a_secret_without_both_values_is_reported_as_not_filled(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            code, output, errors = self.run_wait(self.fake_k3s(Path(scratch), "printf ':'"))
        self.assertEqual((124, "", [f"{REASON_MARK}not filled"]), (code, output, reasons(errors)))

    def test_a_warning_on_standard_error_before_or_after_the_values_changes_nothing_on_either_stream(self) -> None:
        pair = f"{encoded(READ_ID)}:{encoded(READ_VALUE)}"
        warning = "echo 'Warning: deprecated: use the new flag' >&2"
        bodies = {
            "warning first": f"{warning}\nprintf '%s' '{pair}'",
            "warning last": f"printf '%s' '{pair}'\n{warning}",
            "warning between two writes": f"printf '%s' '{encoded(READ_ID)}:'\n{warning}\nprintf '%s' '{encoded(READ_VALUE)}'",
        }
        for label, body in bodies.items():
            with self.subTest(label), tempfile.TemporaryDirectory() as scratch:
                code, output, errors = self.run_wait(self.fake_k3s(Path(scratch), body), 10)
                self.assertEqual((0, pair, ""), (code, output, errors))

    def test_a_failed_attempt_that_also_wrote_standard_output_is_classified_from_standard_error_alone(self) -> None:
        pair = f"{encoded(READ_ID)}:{encoded(READ_VALUE)}"
        with tempfile.TemporaryDirectory() as scratch:
            code, output, errors = self.run_wait(self.fake_k3s(Path(scratch), f"printf '%s' '{pair}'\n" + FAIL.format(text=KUBECTL_ERROR)))
        self.assertEqual((124, "", [f"{REASON_MARK}refused"]), (code, output, reasons(errors)))
        self.assertNotIn(encoded(READ_ID), errors)

    def test_a_standard_output_that_looks_like_a_reason_never_becomes_the_reason(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            code, output, errors = self.run_wait(self.fake_k3s(Path(scratch), "printf 'x\\nkubectl-result:ok\\n'\n" + FAIL.format(text=NOT_FOUND_ERROR)))
        self.assertEqual((124, "", [f"{REASON_MARK}not found"]), (code, output, reasons(errors)))

    def test_control_a_wait_that_ends_with_both_values_prints_nothing_on_stderr_and_leaves_no_file_behind(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder, temporary = Path(scratch), Path(scratch) / "tmp"
            temporary.mkdir()
            argv = self.argv(10)
            env = self.fake_k3s(folder, "printf 'aWQ=:c2VjcmV0'") | {"TMPDIR": str(temporary)}
            result = run_command(argv[0], argv[1:], env=env, timeout=30)
            left = list(temporary.iterdir())
        self.assertEqual((0, "aWQ=:c2VjcmV0", "", []), (result.returncode, result.stdout, result.stderr, left))

    def test_no_file_holds_kubectls_text_after_the_wait_is_killed_without_a_signal_to_catch(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder, temporary = Path(scratch), Path(scratch) / "tmp"
            temporary.mkdir()
            argv = self.argv(60)
            env = self.sequence(folder, FAIL.format(text=MALFORMED_ERROR)) | {"TMPDIR": str(temporary)}
            with self.assertRaises(TimeoutExpired):
                run_command(argv[0], argv[1:], env=env, timeout=3)
            left = {path.name: path.read_text() for path in temporary.iterdir()}
        self.assertEqual({}, left)

    def test_the_run_stops_with_the_secret_name_and_a_fixed_reason_and_not_kubectls_message(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = self.playbook(folder, self.fake_k3s(folder, FAIL.format(text=KUBECTL_ERROR)))
        self.assertNotEqual(0, code, output)
        self.assertIn("airbyte-auth-secrets", output)
        self.assertIn("refused", output)
        self.assertNotIn(KUBECTL_ERROR, output)
        self.assertNotIn("nobody", output)

    def test_a_malformed_kubeconfig_shows_neither_its_path_nor_its_token(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = self.playbook(folder, self.fake_k3s(folder, FAIL.format(text=MALFORMED_ERROR)))
        self.assertNotEqual(0, code, output)
        self.assertIn("airbyte-auth-secrets", output)
        for leaked in (LEAKED_PATH, LEAKED_VALUE, "error loading config"):
            self.assertNotIn(leaked, output)

    def test_a_refusal_followed_by_a_stalled_retry_still_names_the_refusal_in_the_stop_message(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = self.playbook(folder, self.sequence(folder, FAIL.format(text=KUBECTL_ERROR), STALL))
        self.assertNotEqual(0, code, output)
        self.assertIn("refused", output)
        self.assertNotIn(KUBECTL_ERROR, output)

    def test_text_that_another_program_writes_on_the_commands_standard_error_never_reaches_a_message(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            env = self.fake_k3s(folder, FAIL.format(text=KUBECTL_ERROR))
            stray = f"{LEAKED_VALUE} {LEAKED_PATH}"
            wrapper = (
                f'#!/bin/sh\necho "{stray}" >&2\necho "{REASON_MARK}{stray}" >&2\n/usr/bin/timeout "$@"\ncode=$?\necho "{REASON_MARK}{stray}" >&2\nexit $code\n'
            )
            (folder / "bin" / "timeout").write_text(wrapper)
            (folder / "bin" / "timeout").chmod(0o755)
            code, output = self.playbook(folder, env)
        self.assertNotEqual(0, code, output)
        self.assertIn("refused", output)
        for leaked in (LEAKED_VALUE, LEAKED_PATH):
            self.assertNotIn(leaked, output)

    def test_a_secret_that_exists_but_holds_no_values_stops_with_its_name_and_no_message_of_kubectl(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = self.playbook(folder, self.fake_k3s(folder, "printf ':'"))
        self.assertNotEqual(0, code, output)
        self.assertIn("airbyte-auth-secrets", output)
        self.assertIn("not filled", output)
        self.assertNotIn("Error from server", output)

    def test_a_half_filled_secret_that_never_completes_shows_no_part_of_its_values(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = self.playbook(folder, self.fake_k3s(folder, f"printf '%s:' '{encoded(READ_ID)}'"))
        self.assertNotEqual(0, code, output)
        self.assertIn("airbyte-auth-secrets", output)
        for secret in (READ_ID, encoded(READ_ID)):
            self.assertNotIn(secret, output)

    def test_values_on_standard_output_with_a_warning_after_them_on_standard_error_pass_and_show_neither(self) -> None:
        pair = f"{encoded(READ_ID)}:{encoded(READ_VALUE)}"
        for label, body in {
            "warning last": f"printf '%s' '{pair}'\necho 'Warning: deprecated: x' >&2",
            "warning first": f"echo 'Warning: deprecated: x' >&2\nprintf '%s' '{pair}'",
        }.items():
            with self.subTest(label), tempfile.TemporaryDirectory() as scratch:
                folder = Path(scratch)
                code, output = self.playbook(folder, self.fake_k3s(folder, body))
                self.assertEqual(0, code, output)
                for secret in (READ_ID, READ_VALUE, encoded(READ_ID), encoded(READ_VALUE), "deprecated"):
                    self.assertNotIn(secret, output)

    def test_control_both_values_pass_and_neither_the_encoded_nor_the_decoded_value_is_shown(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = self.playbook(folder, self.fake_k3s(folder, f"printf '%s' '{encoded(READ_ID)}:{encoded(READ_VALUE)}'"))
        self.assertEqual(0, code, output)
        for secret in (READ_ID, READ_VALUE, encoded(READ_ID), encoded(READ_VALUE)):
            self.assertNotIn(secret, output)


class RoleIncludeTest(unittest.TestCase):
    def test_the_role_includes_the_record_routes_after_the_release_and_the_web_forward_and_before_the_verification(self) -> None:
        main = (ROLE_TASKS / "main.yml").read_text()
        self.assertEqual(1, main.count("record_routes.yml"))
        for earlier in ("release.yml", "web_forward.yml", "network_policy.yml", "secrets.yml"):
            self.assertLess(main.index(earlier), main.index("record_routes.yml"))
        self.assertLess(main.index("record_routes.yml"), main.index("verify.yml"))

    def test_the_include_is_one_task_with_a_name(self) -> None:
        tasks = flatten(load_yaml((ROLE_TASKS / "main.yml").read_text()))
        include = [task for task in tasks if task.get("ansible.builtin.include_tasks") == "record_routes.yml"]
        self.assertEqual(1, len(include))
        self.assertTrue(include[0].get("name"))


class StaticRulesTest(unittest.TestCase):
    FILES = ("record_routes.yml", "record_route.yml", "record_route_units.yml", "record_route_policy.yml", "record_client_files.yml", "record_client_read.yml")

    def tasks(self) -> dict[str, list[dict[str, Any]]]:
        return {name: flatten(load_yaml((ROLE_TASKS / name).read_text())) for name in self.FILES}

    def test_the_entry_file_runs_the_units_the_policy_and_the_client_files_in_that_order(self) -> None:
        text = (ROLE_TASKS / "record_routes.yml").read_text()
        order = [text.index(name) for name in ("record_route.yml", "record_route_policy.yml", "record_client_read.yml", "record_client_files.yml")]
        self.assertEqual(sorted(order), order)
        self.assertEqual(2, text.count("record_route.yml"), "one route for Airbyte's API and one for Nessie")

    def test_every_task_that_holds_a_credential_hides_its_values(self) -> None:
        for name, tasks in self.tasks().items():
            for task in tasks:
                body = json.dumps(task)
                if (
                    any(word in body for word in ("airbyte_record_client", "instance-admin-client", "aws_access_key_id", "aws_secret_access_key"))
                    and "include_tasks" not in task
                ):
                    with self.subTest(file=name, task=task.get("name")):
                        self.assertIs(True, task.get("no_log"))

    def test_no_file_types_the_bridge_address_a_credential_or_a_literal_secret_on_a_command_line(self) -> None:
        for name in self.FILES:
            text = (ROLE_TASKS / name).read_text()
            with self.subTest(file=name):
                self.assertNotIn("10.42.0.1", text)
                for forbidden in ("--from-literal", "from-file", "stringData", "password=", "PASSWORD="):
                    self.assertNotIn(forbidden, text)

    def test_every_systemd_action_has_a_task_deadline(self) -> None:
        for name, tasks in self.tasks().items():
            for task in tasks:
                if "ansible.builtin.systemd_service" in task:
                    with self.subTest(file=name, task=task["name"]):
                        self.assertIn("timeout", task)
                        self.assertLessEqual(int(task["timeout"]), 300)

    def test_every_command_and_every_wait_of_the_files_is_bounded(self) -> None:
        for name, tasks in self.tasks().items():
            for task in tasks:
                if "ansible.builtin.command" in task:
                    with self.subTest(file=name, task=task["name"]):
                        self.assertIn("timeout", task)
                        self.assertNotIn("until", task, "a retry loop gives each attempt a fresh bound, so the total is not the deadline")

    def test_no_task_of_the_client_files_names_a_key_file_or_a_key_value_or_reads_a_file_of_the_raw_connection(self) -> None:
        text = (ROLE_TASKS / "record_client_files.yml").read_text()
        for forbidden in ("aws_access_key_id", "aws_secret_access_key", "airbyte/", "slurp", "b64decode"):
            self.assertNotIn(forbidden, text)
        for task in self.tasks()["record_client_files.yml"]:
            self.assertNotIn("ansible.builtin.slurp", task)

    def test_the_files_are_owned_by_the_container_user_with_mode_0400(self) -> None:
        place = next(task for task in self.tasks()["record_client_files.yml"] if task.get("name", "").startswith("Place the four files"))
        copy = place["ansible.builtin.copy"]
        self.assertIn("airbyte_record_container_user", copy["owner"])
        self.assertIn("airbyte_record_container_user", copy["group"])
        self.assertEqual("0400", copy["mode"])

    def test_the_deploy_checks_the_api_address_with_the_codes_own_check_and_holds_no_second_list_of_ranges(self) -> None:
        tasks = self.tasks()["record_client_files.yml"]
        checks = [task for task in tasks if "ansible.builtin.command" in task and "scripts.ingestion.airbyte_api" in json.dumps(task)]
        self.assertEqual(1, len(checks))
        argv = checks[0]["ansible.builtin.command"]["argv"]
        self.assertEqual(["check", "--address"], argv[3:5])
        self.assertEqual("http://{{ airbyte_bridge_address }}:{{ airbyte_api_route_port | default(8001) }}", argv[5])
        self.assertEqual("localhost", checks[0]["delegate_to"])
        text = (ROLE_TASKS / "record_client_files.yml").read_text()
        for range_text in ("169", "172\\.", "192\\.168", "match('^(10"):
            self.assertNotIn(range_text, text, "the allowed ranges are defined once, in scripts/ingestion/airbyte_api.py")


if __name__ == "__main__":
    unittest.main()
