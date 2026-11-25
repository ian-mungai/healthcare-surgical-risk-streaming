"""The role that deploys Airbyte on k3s: its pins, its files and the checks that run without a cluster, through the real ``ansible-playbook``.

Installing Helm, creating the secrets and the release need k3s and are proven by the acceptance test. These cases cover what a clean computer shows:
the pinned downloads, the placed values and their idempotence, the web forwarder's unit, the chart check and the rules that every task of the role keeps.
"""

from __future__ import annotations

import grp
import hashlib
import json
import os
import pwd
import re
import stat
import tempfile
import unittest
from pathlib import Path
from typing import Any

from scripts.process import run_command
from scripts.stack.k3s_limits import CHART_WORKLOAD_KEYS
from scripts.stack.limits import load_limits
from scripts.stack.yaml_text import load_yaml
from tests.fast.ansible_paths import GROUP_VARIABLES, group_variables
from tests.fast.test_postgres_accounts import TOOLS_FILE, accounts
from tests.slow.ansible_support import ROLES, environment, included_roles, run_playbook

ROOT = Path(__file__).resolve().parents[2]
ROLE = ROLES / "airbyte"
OWNER = pwd.getpwuid(os.getuid()).pw_name
GROUP = grp.getgrgid(os.getgid()).gr_name
BRIDGE = "10.42.0.1"
HEX64 = re.compile(r"[0-9a-f]{64}")
CHANGED_NONE = re.compile(r"changed=0\s")
COMMAND_MODULES = ("ansible.builtin.command", "ansible.builtin.shell")


def mapping(document: object) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise TypeError("expected a mapping")
    return document


def flatten(tasks: object) -> list[dict[str, Any]]:
    """Every task of a task list, with the tasks inside blocks, rescues and always sections in file order."""
    found: list[dict[str, Any]] = []
    for task in tasks if isinstance(tasks, list) else []:
        found.append(task)
        for section in ("block", "rescue", "always"):
            found.extend(flatten(task.get(section)))
    return found


def role_tasks() -> dict[str, list[dict[str, Any]]]:
    return {path.name: flatten(load_yaml(path.read_text())) for path in sorted((ROLE / "tasks").glob("*.yml"))}


def values(folder: Path) -> dict[str, object]:
    (folder / "units").mkdir(exist_ok=True)
    return {
        "airbyte_directory": str(folder / "airbyte"),
        "airbyte_owner": OWNER,
        "airbyte_group": GROUP,
        "airbyte_bridge_address": BRIDGE,
        "airbyte_database_port": 5432,
        "compose_stack_postgres_port": 5432,
        "airbyte_postgres_unit_directory": str(folder / "units"),
        "airbyte_postgres_unit": "airbyte-postgres",
        "airbyte_flannel_subnet_file": str(folder / "subnet.env"),
        "airbyte_bridge_deadline_seconds": 2,
        "airbyte_database_name": "airbyte",
        "airbyte_temporal_database": "airbyte_temporal",
        "airbyte_temporal_visibility_database": "airbyte_temporal_visibility",
        "airbyte_staging_size": "7G",
        "airbyte_server_service_port": 8001,
        "airbyte_public_url": "",
        "airbyte_namespace": "airbyte",
        "airbyte_release": "airbyte",
        "airbyte_web_port": 8000,
        "airbyte_web_unit": str(folder / "airbyte-web.service"),
        "airbyte_kubeconfig": "/etc/rancher/k3s/k3s.yaml",
    }


def run_tasks(task_files: list[str], variables: dict[str, object]) -> tuple[int, str]:
    with tempfile.TemporaryDirectory() as scratch:
        work = Path(scratch)
        includes = "".join(f"    - ansible.builtin.include_role:\n        name: {ROLE}\n        tasks_from: {name}\n" for name in task_files)
        playbook = work / "check.yml"
        playbook.write_text(f"- hosts: localhost\n  connection: local\n  gather_facts: false\n  tasks:\n{includes}")
        result = run_playbook(work, playbook, ["-v", "-i", "localhost,", "--extra-vars", json.dumps(variables)])
    return result.returncode, result.stdout + result.stderr


class PinsTest(unittest.TestCase):
    def test_each_pinned_hash_is_a_sha256_of_64_hex_characters_and_each_version_is_set(self) -> None:
        variables = group_variables()
        for name in ("helm_archive_sha256", "airbyte_chart_sha256"):
            with self.subTest(name=name):
                self.assertRegex(str(variables[name]), HEX64.pattern)
        self.assertRegex(str(variables["helm_version"]), r"^v\d+\.\d+\.\d+$")
        self.assertRegex(str(variables["airbyte_chart_version"]), r"^\d+\.\d+\.\d+$")

    def test_every_download_of_the_role_names_the_version_and_is_checked_against_a_pinned_hash(self) -> None:
        variables = group_variables()
        downloads = [task for tasks in role_tasks().values() for task in tasks if "ansible.builtin.get_url" in task]
        self.assertEqual(2, len(downloads), "the role downloads the Helm archive and the chart archive")
        for task in downloads:
            module = task["ansible.builtin.get_url"]
            with self.subTest(task=task["name"]):
                match = re.fullmatch(r"sha256:\{\{ (\w+_sha256) \}\}", str(module["checksum"]))
                if match is None:
                    self.fail("the checksum must name a pinned hash variable")
                self.assertRegex(str(variables[match.group(1)]), HEX64.pattern)
                self.assertTrue(re.search(r"\{\{ \w*(version)\w* \}\}", str(module["url"])), "the url must name the pinned version")

    def test_the_chart_is_installed_from_the_pinned_archive_and_never_from_a_repository_index(self) -> None:
        text = "".join(path.read_text() for path in (ROLE / "tasks").glob("*.yml"))
        self.assertNotIn("repo add", text)
        self.assertNotIn("--repo", text)
        self.assertNotIn("--set", text)


class TaskRulesTest(unittest.TestCase):
    def test_every_command_of_the_role_has_a_deadline(self) -> None:
        for name, tasks in role_tasks().items():
            for task in tasks:
                if any(module in task for module in COMMAND_MODULES):
                    with self.subTest(file=name, task=task["name"]):
                        self.assertIn("timeout", task)

    def test_no_secret_value_goes_through_a_command_line_a_literal_or_a_rendered_file(self) -> None:
        text = "".join(path.read_text() for path in (ROLE / "tasks").glob("*.yml"))
        for forbidden in ("--from-literal", "from-file", "stringData", "password=", "PASSWORD="):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, text)

    def test_the_release_takes_its_three_values_files_in_order_and_waits_with_a_deadline(self) -> None:
        release = next(task for task in role_tasks()["release.yml"] if "ansible.builtin.command" in task and "upgrade" in str(task["ansible.builtin.command"]))
        arguments = [str(item) for item in release["ansible.builtin.command"]["argv"]]
        files = [argument for index, argument in enumerate(arguments) if index and arguments[index - 1] == "--values"]
        self.assertEqual(["values.yaml", "limits.values.yaml", "host.values.yaml"], [Path(item).name for item in files])
        self.assertIn("--wait", arguments)
        self.assertIn("--timeout", arguments)
        self.assertIn("timeout", release)


# A ``helm`` that keeps one release in files: its status (empty when none is installed), its history (one status per line) and the ConfigMap that the
# chart's pre-upgrade hook writes from the values it is given. Each call is logged as one line of arguments. A ``flag_<name>`` file in the folder switches a
# failure on: ``fail_<command>`` makes that command fail with a text that must never reach the task log, ``fail_status_after_recovery`` makes the status
# read fail once a rollback or an uninstall ran, ``fail_status_second`` makes the second status read of a run fail, and ``interrupt`` stops an upgrade after
# the hook, with the pending revision stored before the hook as Helm v3.22.1 does (pkg/action/upgrade.go: ``Releases.Create(upgradedRelease)`` runs before
# ``execHook(HookPreUpgrade)``), so no stop between the hook and the stored revision exists. Helm refuses an upgrade in two ways before any hook: with
# "another operation (install/upgrade/rollback) is in progress" when the last revision is pending-install, pending-upgrade or pending-rollback
# (``Status.IsPending``), and with "has no deployed releases" when the last revision is neither deployed, failed nor superseded and no revision of the
# release is deployed (``prepareUpgrade``, which an ``uninstalling`` last revision meets). An ``uninstalled`` last revision is installed again, as
# ``upgrade --install`` does (cmd/helm/upgrade.go, ``isReleaseUninstalled``); without ``--install`` an absent or uninstalled release is refused.
# ``upgrade`` and ``rollback`` leave the release deployed, and
# ``uninstall`` removes it, as the real commands do. A rollback does not undo the hook's ConfigMap, as the real one does not.
HELM_STAND_IN = """#!/bin/sh
echo "$*" >> "$STAND_IN/log"
state=$(cat "$STAND_IN/status")
refuse_pending() { echo "Error: UPGRADE FAILED: another operation (install/upgrade/rollback) is in progress" >&2; exit 1; }
refuse_no_deployed() { echo "Error: UPGRADE FAILED: \"airbyte\" has no deployed releases" >&2; exit 1; }
refuse() { echo "Error: LEAKED-STDERR $1" >&2; echo "LEAKED-STDOUT $1"; exit 1; }
[ -e "$STAND_IN/flag_fail_$1" ] && refuse "$1"
case "$1" in
status)
  if [ -e "$STAND_IN/flag_fail_status_after_recovery" ] && [ -e "$STAND_IN/recovered" ]; then refuse status; fi
  if [ -e "$STAND_IN/flag_fail_status_second" ] && [ -e "$STAND_IN/status_seen" ]; then refuse status; fi
  : > "$STAND_IN/status_seen"
  if [ -z "$state" ]; then echo "Error: release: not found" >&2; exit 1; fi
  printf '{"name":"airbyte","info":{"status":"%s"}}' "$state" ;;
history) awk 'BEGIN{printf "["} {printf "%s{\\"revision\\":%d,\\"status\\":\\"%s\\"}", (NR>1?",":""), NR, $0} END{printf "]"}' "$STAND_IN/history" ;;
upgrade)
  case "$state" in
    pending-install | pending-upgrade | pending-rollback) refuse_pending ;;
    "" | uninstalled) case " $* " in *" --install "*) ;; *) refuse_no_deployed ;; esac ;;
    deployed | failed | superseded) ;;
    *) grep -qx deployed "$STAND_IN/history" || refuse_no_deployed ;;
  esac
  prev=""
  for argument in "$@"; do
    [ "$prev" = "--values" ] && cat "$argument"
    prev="$argument"
  done > "$STAND_IN/configmap"
  if [ -e "$STAND_IN/flag_interrupt" ]; then printf pending-upgrade > "$STAND_IN/status"; echo pending-upgrade >> "$STAND_IN/history"; exit 137; fi
  printf deployed > "$STAND_IN/status"; echo deployed >> "$STAND_IN/history" ;;
rollback) : > "$STAND_IN/recovered"; printf deployed > "$STAND_IN/status"; echo deployed >> "$STAND_IN/history" ;;
uninstall) : > "$STAND_IN/recovered"; : > "$STAND_IN/status"; : > "$STAND_IN/history" ;;
*) echo "unexpected: $*" >&2; exit 9 ;;
esac
"""
# The k3s stand-in logs each call and answers ``kubectl get deployment --output name`` with two deployments (``flag_no_deployments`` lists none).
# ``flag_fail_restart`` fails the restart and ``flag_fail_rollout`` fails the rollout status of the second deployment, each with text that must never
# reach the task log.
K3S_STAND_IN = """#!/bin/sh
echo "$*" >> "$STAND_IN/log"
leak() { echo "Error: LEAKED-STDERR $1" >&2; echo "LEAKED-STDOUT $1"; exit 1; }
case "$2 $3" in
"rollout restart") [ -e "$STAND_IN/flag_fail_restart" ] && leak restart ;;
"get deployment") [ -e "$STAND_IN/flag_no_deployments" ] || printf 'deployment.apps/airbyte-server\ndeployment.apps/airbyte-workload-launcher\n' ;;
"rollout status")
  [ -e "$STAND_IN/flag_fail_rollout" ] && [ "$4" = deployment.apps/airbyte-workload-launcher ] && leak rollout
  [ -e "$STAND_IN/flag_slow_rollout" ] && sleep 2 ;;
esac
exit 0
"""
CHART_SHA = "c" * 64
DEADLINE = 600
UPGRADE = "upgrade --install"
NOT_INSTALLED = ""
VALUES_FILES = ("values.yaml", "limits.values.yaml", "host.values.yaml")
HOST_VALUES_B = "host.values.yaml from inputs B\n"
CONFIGMAP_A = "".join(f"{name}\n" for name in VALUES_FILES)
CONFIGMAP_B = CONFIGMAP_A.replace("host.values.yaml\n", HOST_VALUES_B)
LEAK = "LEAKED"
RECORD = "release_inputs.sha256"
# The form of the recorded inputs: the role writes it into the digest, so a record from a role that did not restart the deployments never matches.
RECORD_FORM = "release-inputs-2"
RESTART = [
    "kubectl rollout restart deployment",
    "kubectl get deployment",
    "kubectl rollout status deployment.apps/airbyte-server",
    "kubectl rollout status deployment.apps/airbyte-workload-launcher",
]


def history(*states: str) -> list[dict[str, object]]:
    return [{"revision": number, "status": state} for number, state in enumerate(states, start=1)]


class ReleaseSandbox:
    """One release in files with the stand-in programs, and the role's ``release.yml`` run against it as often as a case needs."""

    def __init__(self, scratch: str, status: str, past: list[dict[str, object]] | None, recorded: str, changes: dict[str, object]) -> None:
        folder = Path(scratch)
        self.stand_in = folder / "stand_in"
        self.folder = folder
        programs = folder / "bin"
        for directory in (self.stand_in, programs):
            directory.mkdir()
        for name, text in (("helm", HELM_STAND_IN), ("k3s", K3S_STAND_IN)):
            (programs / name).write_text(text)
            (programs / name).chmod(stat.S_IRWXU)
        (self.stand_in / "status").write_text(status)
        (self.stand_in / "history").write_text("".join(f"{item['status']}\n" for item in past or []))
        self.variables = (
            values(folder) | {"airbyte_chart_version": "1.2.3", "airbyte_chart_sha256": CHART_SHA, "airbyte_release_deadline_seconds": DEADLINE} | changes
        )
        self.directory = Path(str(self.variables["airbyte_directory"]))
        self.directory.mkdir()
        for name in VALUES_FILES:
            (self.directory / name).write_text(f"{name}\n")
        sums = [hashlib.sha256((self.directory / name).read_bytes()).hexdigest() for name in VALUES_FILES]
        self.inputs = hashlib.sha256(":".join([RECORD_FORM, CHART_SHA, *sums]).encode()).hexdigest()
        if recorded == "match":
            (self.directory / RECORD).write_text(f"{self.inputs}\n")
        elif recorded == "legacy":
            # The record that the role wrote before it restarted the deployments: the same inputs without the form, so it never matches now.
            (self.directory / RECORD).write_text(hashlib.sha256(":".join([CHART_SHA, *sums]).encode()).hexdigest() + "\n")
        elif recorded == "other":
            (self.directory / RECORD).write_text("0" * 64 + "\n")
        self.playbook = folder / "check.yml"
        include = f"    - ansible.builtin.include_role:\n        name: {ROLE}\n        tasks_from: release.yml\n"
        self.playbook.write_text(f"- hosts: localhost\n  connection: local\n  gather_facts: false\n  tasks:\n{include}")
        # The host launcher passes its own environment on, so a shell with ANSIBLE_VERBOSITY=1 prints every task's result: the cases run at that level.
        self.env = environment(folder) | {"PATH": f"{programs}:/usr/bin:/bin", "STAND_IN": str(self.stand_in), "ANSIBLE_VERBOSITY": "1"}

    def run(self, flags: tuple[str, ...] = (), host_values: str | None = None) -> tuple[int, str, list[str]]:
        for old in self.stand_in.glob("flag_*"):
            old.unlink()
        for name in ("status_seen", "recovered", "log"):
            (self.stand_in / name).unlink(missing_ok=True)
        for flag in flags:
            (self.stand_in / f"flag_{flag}").write_text("")
        if host_values is not None:
            (self.directory / "host.values.yaml").write_text(host_values)
        result = run_command(
            str(ROOT / ".venv" / "bin" / "ansible-playbook"),
            ["-i", "localhost,", "--extra-vars", json.dumps(self.variables), str(self.playbook)],
            cwd=self.folder,
            env=self.env,
            timeout=120,
        )
        log = (self.stand_in / "log").read_text().splitlines() if (self.stand_in / "log").exists() else []
        return result.returncode, result.stdout + result.stderr, [line for line in log if not line.startswith("status")]

    def configmap(self) -> str:
        return (self.stand_in / "configmap").read_text()

    def state(self) -> str:
        return (self.stand_in / "status").read_text()

    def record(self) -> Path:
        return self.directory / RECORD


def commands(log: list[str]) -> list[str]:
    """The Helm and k3s calls of a run in order, without each one's namespace and values arguments that the cases do not look at."""
    return [UPGRADE if line.startswith(UPGRADE + " ") else line.split(" --namespace")[0] for line in log]


class ReleaseRecoveryTest(unittest.TestCase):
    """A run that was stopped inside Helm leaves a release with an operation pending, and ``helm upgrade --install`` refuses it (host run, Nov 23 2026).
    The role first brings such a release to a state that Helm accepts, with ``helm`` run by a stand-in program that keeps the release in files."""

    def run_release(
        self, status: str, past: list[dict[str, object]] | None = None, recorded: str = "match", twice: bool = False, **changes: object
    ) -> tuple[int, str, list[str]]:
        with tempfile.TemporaryDirectory() as scratch:
            box = ReleaseSandbox(scratch, status, past, recorded, changes)
            for _ in range(2 if twice else 1):
                result = box.run()
            return result

    def commands(self, log: list[str]) -> list[str]:
        return commands(log)

    def test_a_release_with_an_upgrade_pending_is_rolled_back_to_its_last_deployed_revision_before_the_install(self) -> None:
        code, output, log = self.run_release("pending-upgrade", history("superseded", "deployed", "pending-upgrade"), recorded="other")
        self.assertEqual(0, code, output)
        self.assertEqual(["history airbyte", "rollback airbyte 2", UPGRADE, *RESTART], self.commands(log))
        self.assertIn(f"rollback airbyte 2 --namespace airbyte --wait --timeout {DEADLINE}s", log)

    def test_a_release_with_a_rollback_pending_is_rolled_back_to_the_last_deployed_revision_and_not_to_a_failed_one(self) -> None:
        past = history("superseded", "deployed", "failed", "pending-rollback")
        code, output, log = self.run_release("pending-rollback", past, recorded="other")
        self.assertEqual(0, code, output)
        self.assertEqual(["history airbyte", "rollback airbyte 2", UPGRADE, *RESTART], self.commands(log))

    def test_with_two_deployed_revisions_in_the_history_the_later_one_is_the_target(self) -> None:
        code, output, log = self.run_release("pending-upgrade", history("deployed", "deployed", "pending-upgrade"), recorded="other")
        self.assertEqual(0, code, output)
        self.assertEqual(["history airbyte", "rollback airbyte 2", UPGRADE, *RESTART], self.commands(log))

    def test_a_first_install_that_is_pending_is_uninstalled_before_the_install_with_the_same_deadline(self) -> None:
        code, output, log = self.run_release("pending-install", history("pending-install"), recorded="none")
        self.assertEqual(0, code, output)
        self.assertEqual(["uninstall airbyte", UPGRADE], self.commands(log))
        self.assertIn(f"uninstall airbyte --namespace airbyte --wait --timeout {DEADLINE}s", log)

    def test_a_release_that_an_uninstall_left_uninstalling_is_uninstalled_again_before_the_install(self) -> None:
        for recorded in ("none", "match"):
            with self.subTest(recorded=recorded):
                code, output, log = self.run_release("uninstalling", history("superseded", "uninstalling"), recorded=recorded)
                self.assertEqual(0, code, output)
                self.assertEqual(["uninstall airbyte", UPGRADE], self.commands(log))
                self.assertIn(f"uninstall airbyte --namespace airbyte --wait --timeout {DEADLINE}s", log)

    def stand_in_upgrade(self, status: str, past: list[dict[str, object]]) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as scratch:
            box = ReleaseSandbox(scratch, status, past, "none", {})
            result = run_command(str(box.env["PATH"].split(":")[0]) + "/helm", ["upgrade", "--install", "airbyte"], cwd=box.folder, env=box.env, timeout=30)
        return result.returncode, result.stderr

    def test_the_stand_in_refuses_an_upgrade_over_a_pending_revision_as_helm_does_with_its_pending_error(self) -> None:
        for status in ("pending-install", "pending-upgrade", "pending-rollback"):
            with self.subTest(status=status):
                code, error = self.stand_in_upgrade(status, history("deployed", status))
                self.assertEqual(1, code)
                self.assertIn("another operation (install/upgrade/rollback) is in progress", error)

    def test_the_stand_in_refuses_an_upgrade_when_no_revision_is_deployed_and_the_last_one_is_neither_failed_nor_superseded_as_helm_does(self) -> None:
        for status in ("uninstalling", "unknown"):
            with self.subTest(status=status):
                code, error = self.stand_in_upgrade(status, history("superseded", status))
                self.assertEqual(1, code)
                self.assertIn("has no deployed releases", error)
                self.assertNotIn("another operation", error)
        for status in ("failed", "superseded"):
            with self.subTest(status=status):
                self.assertEqual(0, self.stand_in_upgrade(status, history(status))[0], "Helm upgrades from the last revision when it is failed or superseded")
        self.assertEqual(
            0, self.stand_in_upgrade("uninstalling", history("deployed", "uninstalling"))[0], "a deployed revision in the history lets the upgrade run"
        )

    def test_the_recovery_commands_follow_the_release_namespace_and_deadline_they_are_given(self) -> None:
        _, _, log = self.run_release(
            "pending-upgrade", history("deployed", "pending-upgrade"), recorded="other", airbyte_release="named", airbyte_namespace="space"
        )
        self.assertIn("rollback named 1 --namespace space --wait --timeout 600s", log)
        _, _, log = self.run_release(
            "pending-install", None, recorded="none", airbyte_release="named", airbyte_namespace="space", airbyte_release_deadline_seconds=90
        )
        self.assertIn("uninstall named --namespace space --wait --timeout 90s", log)

    def test_a_pending_upgrade_with_no_deployed_revision_stops_before_any_change_and_names_the_status(self) -> None:
        code, output, log = self.run_release("pending-upgrade", history("failed", "pending-upgrade"), recorded="other")
        self.assertNotEqual(0, code)
        self.assertIn("pending-upgrade", output)
        self.assertEqual(["history airbyte"], self.commands(log))

    def test_a_pending_release_whose_recorded_inputs_match_is_still_upgraded_from_this_runs_inputs_after_the_recovery(self) -> None:
        """Older code left the record in place when it was stopped inside Helm, so the record matches while the hook's ConfigMap still holds inputs B."""
        # A rollback leaves a deployed release whose pods keep their settings, so the upgrade restarts them; an uninstall leaves none, so the install
        # starts every pod new and nothing restarts.
        for status, past, recovery, restart in (
            ("pending-upgrade", history("deployed", "pending-upgrade"), ["history airbyte", "rollback airbyte 1"], RESTART),
            ("pending-rollback", history("superseded", "deployed", "pending-rollback"), ["history airbyte", "rollback airbyte 2"], RESTART),
            ("pending-install", history("pending-install"), ["uninstall airbyte"], []),
            ("uninstalling", history("superseded", "uninstalling"), ["uninstall airbyte"], []),
        ):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as scratch:
                box = ReleaseSandbox(scratch, status, past, "match", {})
                (box.stand_in / "configmap").write_text(CONFIGMAP_B)
                code, output, log = box.run()
                self.assertEqual(0, code, output)
                self.assertEqual([*recovery, UPGRADE, *restart], commands(log))
                self.assertEqual(CONFIGMAP_A, box.configmap(), "the upgrade ran from this run's inputs and not from the inputs that the interrupted run wrote")
                self.assertEqual(f"{box.inputs}\n", box.record().read_text())

    def test_a_deployed_release_with_unchanged_inputs_runs_no_helm_command_but_the_status_read(self) -> None:
        code, output, log = self.run_release("deployed", history("deployed"))
        self.assertEqual(0, code, output)
        self.assertEqual([], log)

    def test_a_second_run_after_a_successful_install_runs_no_helm_command_but_the_status_read(self) -> None:
        code, output, log = self.run_release(NOT_INSTALLED, recorded="none", twice=True)
        self.assertEqual(0, code, output)
        self.assertEqual([], log, "the first run recorded its inputs in the form that the second run reads")

    def test_an_upgrade_of_a_deployed_release_restarts_every_deployment_and_waits_for_each_so_changed_settings_reach_the_pods(self) -> None:
        """The chart passes its settings to the pods as environment values from one ConfigMap, which a pod reads only when it starts, and an upgrade that
        changes only that ConfigMap leaves the pods running (live host, Nov 24 2026: the workload launcher kept a 256Mi sidecar limit after the limits file
        and the ConfigMap said 512Mi, and the sidecar was OOMKilled). After the upgrade, every deployment of the namespace restarts and the run waits until
        each is available, within the release deadline."""
        code, output, log = self.run_release("deployed", history("deployed"), recorded="other")
        self.assertEqual(0, code, output)
        self.assertEqual([UPGRADE, *RESTART], self.commands(log))
        waits = [line for line in log if line.startswith("kubectl rollout status deployment.apps/airbyte-workload-launcher --namespace airbyte --timeout=")]
        self.assertEqual(1, len(waits), log)
        seconds = int(waits[0].rsplit("--timeout=", 1)[1].rstrip("s"))
        self.assertTrue(0 < seconds < DEADLINE, f"the second wait gets what is left of the one deadline, less a second: {seconds}")

    def test_a_release_whose_latest_revision_failed_is_upgraded_and_restarted(self) -> None:
        """Helm upgrades a release whose latest revision failed over an older deployed one, and its pods still run the old settings."""
        code, output, log = self.run_release("failed", history("deployed", "failed"), recorded="other")
        self.assertEqual(0, code, output)
        self.assertEqual([UPGRADE, *RESTART], self.commands(log))

    def test_a_rollout_that_does_not_finish_or_finds_no_deployment_stops_the_run_and_leaves_no_record(self) -> None:
        """The new pods must be ready before the inputs are recorded: an Available condition can still hold on an old replica alone."""
        for flag in ("fail_rollout", "no_deployments"):
            with self.subTest(flag=flag), tempfile.TemporaryDirectory() as scratch:
                box = ReleaseSandbox(scratch, "deployed", history("deployed"), "other", {})
                code, output, _ = box.run(flags=(flag,))
                self.assertNotEqual(0, code)
                self.assertIn("could not restart the Airbyte deployments", output)
                self.assertNotIn(LEAK, output)
                self.assertFalse(box.record().exists())

    def test_a_record_written_before_the_restart_existed_upgrades_and_restarts_once_and_a_second_run_changes_nothing(self) -> None:
        """A host whose last deploy ran the role without the restart (live host, Nov 24 2026) recorded the same inputs; its pods may hold old settings."""
        with tempfile.TemporaryDirectory() as scratch:
            box = ReleaseSandbox(scratch, "deployed", history("deployed"), "legacy", {})
            code, output, log = box.run()
            self.assertEqual(0, code, output)
            self.assertEqual([UPGRADE, *RESTART], commands(log))
            code, output, log = box.run()
            self.assertEqual(0, code, output)
            self.assertEqual([], log)

    def test_the_rollout_waits_together_stay_within_one_release_deadline(self) -> None:
        """Each rollout of the stand-in takes two seconds. With a three-second deadline the first wait gets two seconds and nothing is left for the second,
        so the run stops with the rollout code and records nothing; with a seven-second deadline both waits fit and the run passes."""
        for deadline, passes in ((3, False), (7, True)):
            with self.subTest(deadline=deadline), tempfile.TemporaryDirectory() as scratch:
                box = ReleaseSandbox(scratch, "deployed", history("deployed"), "other", {"airbyte_release_deadline_seconds": deadline})
                code, output, _ = box.run(flags=("slow_rollout",))
                self.assertEqual(passes, code == 0, output)
                self.assertEqual(passes, box.record().exists())
                if not passes:
                    self.assertIn("exit status 6", output)
                    self.assertNotIn(LEAK, output)

    def test_a_first_install_restarts_nothing_because_every_pod_starts_with_the_new_settings(self) -> None:
        code, output, log = self.run_release(NOT_INSTALLED, recorded="none")
        self.assertEqual(0, code, output)
        self.assertEqual([UPGRADE], self.commands(log))

    def test_a_restart_that_fails_stops_the_run_with_fixed_text_and_leaves_no_record(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            box = ReleaseSandbox(scratch, "deployed", history("deployed"), "other", {})
            code, output, _ = box.run(flags=("fail_restart",))
            self.assertNotEqual(0, code)
            self.assertIn("could not restart the Airbyte deployments", output)
            self.assertNotIn(LEAK, output)
            self.assertFalse(box.record().exists(), "a release whose pods kept the old settings is not recorded as done")

    def test_an_upgrade_without_install_is_refused_for_a_release_that_is_absent_or_uninstalled_and_is_not_counted_as_one(self) -> None:
        """Recheck 4: only ``upgrade --install`` installs a release that is not there, and the log keeps a bare upgrade as it was called."""
        for status, past in ((NOT_INSTALLED, []), ("uninstalled", history("superseded", "uninstalled"))):
            with self.subTest(status=status or "not installed"), tempfile.TemporaryDirectory() as scratch:
                box = ReleaseSandbox(scratch, status, past, "none", {})
                helm = str(box.env["PATH"].split(":")[0]) + "/helm"
                result = run_command(helm, ["upgrade", "airbyte", "chart"], cwd=box.folder, env=box.env, timeout=30)
                self.assertEqual(1, result.returncode)
                self.assertIn("has no deployed releases", result.stderr)
        self.assertEqual(["upgrade airbyte chart"], commands(["upgrade airbyte chart --namespace airbyte"]))
        self.assertEqual([UPGRADE], commands(["upgrade --install airbyte chart --namespace airbyte"]))

    def test_an_uninstalled_last_revision_is_installed_again_as_helm_does_and_the_playbook_proceeds(self) -> None:
        """Recheck 3: Helm v3.22.1 ``upgrade --install`` (cmd/helm/upgrade.go, ``isReleaseUninstalled``) runs a replacement install when the last revision is
        uninstalled, so the stand-in must not refuse it."""
        self.assertEqual(0, self.stand_in_upgrade("uninstalled", history("superseded", "uninstalled"))[0])
        code, output, log = self.run_release("uninstalled", history("superseded", "uninstalled"), recorded="match")
        self.assertEqual(0, code, output)
        self.assertEqual([UPGRADE], self.commands(log))

    def test_each_other_state_passes_on_to_the_install_with_no_recovery(self) -> None:
        for status in ("failed", "superseded", "unknown", NOT_INSTALLED):
            with self.subTest(status=status or "not installed"):
                code, output, log = self.run_release(status, history("deployed", "failed"), recorded="match")
                self.assertEqual(0, code, output)
                # An existing release keeps its pods, so the upgrade restarts them; only a release not installed starts every pod new.
                self.assertEqual([UPGRADE, *([] if status == NOT_INSTALLED else RESTART)], self.commands(log))

    def test_a_transient_error_on_a_second_status_read_changes_nothing_when_no_recovery_ran(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            box = ReleaseSandbox(scratch, "deployed", history("deployed"), "match", {})
            code, output, log = box.run(flags=("fail_status_second",))
        self.assertEqual(0, code, output)
        self.assertEqual([], log, "the decision comes from the first read when no recovery ran")

    def test_a_status_that_cannot_be_read_stops_the_run_with_fixed_text_and_no_helm_output_before_any_change(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            box = ReleaseSandbox(scratch, "deployed", history("deployed"), "other", {})
            code, output, log = box.run(flags=("fail_status",))
        self.assertNotEqual(0, code)
        self.assertIn("read the status", output)
        self.assertNotIn(LEAK, output)
        self.assertEqual([], log)

    def test_after_a_recovery_a_status_that_cannot_be_read_stops_the_run_naming_the_status_and_leaves_no_record(self) -> None:
        for status, past in (
            ("pending-upgrade", history("deployed", "pending-upgrade")),
            ("pending-rollback", history("deployed", "pending-rollback")),
            ("pending-install", history("pending-install")),
            ("uninstalling", history("deployed", "uninstalling")),
        ):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as scratch:
                box = ReleaseSandbox(scratch, status, past, "match", {})
                code, output, log = box.run(flags=("fail_status_after_recovery",))
                self.assertNotEqual(0, code)
                self.assertIn(status, output)
                self.assertIn("after the recovery", output)
                self.assertNotIn(LEAK, output)
                self.assertNotIn(UPGRADE, commands(log))
                self.assertFalse(box.record().exists(), "the recorded inputs are gone before the first recovery command runs")

    def test_a_recovery_or_install_command_that_fails_stops_the_run_with_fixed_text_naming_the_status_and_the_step_and_no_helm_output(self) -> None:
        for command, step, status, past in (
            ("history", "read the history", "pending-upgrade", history("deployed", "pending-upgrade")),
            ("rollback", "roll back", "pending-rollback", history("deployed", "pending-rollback")),
            ("uninstall", "uninstall", "pending-install", history("pending-install")),
            ("uninstall", "uninstall", "uninstalling", history("uninstalling")),
            ("upgrade", "install or upgrade", "deployed", history("deployed")),
        ):
            with self.subTest(command=command, status=status), tempfile.TemporaryDirectory() as scratch:
                box = ReleaseSandbox(scratch, status, past, "other", {})
                code, output, _ = box.run(flags=(f"fail_{command}",))
                self.assertNotEqual(0, code)
                self.assertIn(step, output)
                self.assertIn(status, output)
                self.assertNotIn(LEAK, output)

    def test_a_run_at_verbosity_1_prints_no_output_of_any_successful_helm_command(self) -> None:
        for status, past in (("pending-upgrade", history("deployed", "pending-upgrade")), ("pending-install", history("pending-install"))):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as scratch:
                box = ReleaseSandbox(scratch, status, past, "other", {})
                self.assertEqual("1", box.env["ANSIBLE_VERBOSITY"], "the host launcher inherits this level from the shell")
                code, output, log = box.run()
                self.assertEqual(0, code, output)
                self.assertIn(UPGRADE, commands(log))
                for pattern in (r'\\*"info\\*"', r'\\*"revision\\*"', LEAK):
                    self.assertIsNone(re.search(pattern, output), "the status and history JSON and every command's output stay out of the log")

    def test_inputs_b_stopped_inside_helm_and_then_inputs_a_end_with_an_upgrade_from_inputs_a(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            box = ReleaseSandbox(scratch, "deployed", history("deployed"), "match", {})
            code, output, log = box.run(flags=("interrupt",), host_values=HOST_VALUES_B)
            self.assertNotEqual(0, code)
            self.assertEqual("pending-upgrade", box.state())
            self.assertEqual(CONFIGMAP_B, box.configmap(), "the pre-upgrade hook wrote the ConfigMap from inputs B")
            self.assertFalse(box.record().exists())
            code, output, log = box.run(host_values="host.values.yaml\n")
            self.assertEqual(0, code, output)
            self.assertEqual(["history airbyte", "rollback airbyte 1", UPGRADE, *RESTART], commands(log))
            self.assertEqual(CONFIGMAP_A, box.configmap())
            self.assertEqual(f"{box.inputs}\n", box.record().read_text())

    def test_a_second_stop_after_a_recovery_stays_recoverable(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            box = ReleaseSandbox(scratch, "deployed", history("deployed"), "match", {})
            box.run(flags=("interrupt",), host_values=HOST_VALUES_B)
            code, _, log = box.run(flags=("interrupt",), host_values="host.values.yaml\n")
            self.assertNotEqual(0, code)
            self.assertEqual(["history airbyte", "rollback airbyte 1", UPGRADE], commands(log)[:3])
            self.assertFalse(box.record().exists())
            code, output, log = box.run()
            self.assertEqual(0, code, output)
            self.assertEqual(["history airbyte", "rollback airbyte 3", UPGRADE, *RESTART], commands(log))
            self.assertEqual(CONFIGMAP_A, box.configmap())

    def test_the_inputs_are_recorded_only_after_the_install_succeeded(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            box = ReleaseSandbox(scratch, "deployed", history("deployed"), "other", {})
            code, _, _ = box.run(flags=("fail_upgrade",))
            self.assertNotEqual(0, code)
            self.assertFalse(box.record().exists())
            code, output, _ = box.run()
            self.assertEqual(0, code, output)
            self.assertEqual(f"{box.inputs}\n", box.record().read_text())


class ReleaseRecoveryStructureTest(unittest.TestCase):
    def tasks(self) -> list[dict[str, Any]]:
        return role_tasks()["release.yml"]

    def index(self, name: str) -> int:
        return next(index for index, task in enumerate(self.tasks()) if task.get("name") == name)

    def test_the_status_is_read_then_each_recovery_then_the_status_again_and_only_then_the_decision(self) -> None:
        names = [
            "Read the release's status",
            "Stop when the release's status cannot be read",
            "Read the release's history",
            "Forget the recorded inputs before the recovery",
            "Roll the release back to its last deployed revision",
            "Uninstall the release that never finished its first install or its uninstall",
            "Read the release's status again",
            "Stop when the release's status cannot be read after the recovery",
            "Decide whether the release must be installed or upgraded",
            "Forget the recorded inputs before the install starts",
            "Install or upgrade the release from the pinned chart archive",
            "Record the inputs of this successful release",
        ]
        positions = [self.index(name) for name in names]
        self.assertEqual(sorted(positions), positions)

    def test_the_decision_reads_the_state_after_the_recovery_when_one_ran_and_the_first_read_when_none_ran(self) -> None:
        text = str(self.tasks()[self.index("Decide whether the release must be installed or upgraded")]["ansible.builtin.set_fact"])
        self.assertIn("airbyte_release_state_now", text)
        self.assertNotRegex(text, r"airbyte_release_status\b")
        self.assertNotRegex(text, r"airbyte_release_state\b(?!_now)")
        again = self.tasks()[self.index("Read the release's status again")]
        self.assertEqual("airbyte_release_status_now", again["register"])
        self.assertIn("airbyte_release_recovery", str(again["when"]))

    def test_every_helm_command_of_the_release_is_registered_without_failing_and_followed_by_a_task_that_stops_with_fixed_text(self) -> None:
        tasks = self.tasks()
        found = 0
        for index, task in enumerate(tasks):
            command = task.get("ansible.builtin.command")
            if not isinstance(command, dict) or str(command.get("argv", [""])[0]) != "helm":
                continue
            found += 1
            with self.subTest(task=task["name"]):
                self.assertIs(False, task["failed_when"])
                self.assertIs(True, task.get("no_log"), "Helm's output must not reach the log at any verbosity")
                register = {"airbyte_release_status": "airbyte_release_state", "airbyte_release_status_now": "airbyte_release_state_now"}.get(
                    task["register"], task["register"]
                )
                stop = next(later for later in tasks[index + 1 :] if "ansible.builtin.fail" in later and re.search(rf"\b{register}\b", str(later.get("when"))))
                self.assertNotRegex(str(stop["ansible.builtin.fail"]), r"\.(stdout|stderr|msg)\b")
                self.assertNotIn("no_log", stop, "the fixed failure text must stay visible")
                self.assertNotIn("stdout", str(stop["ansible.builtin.fail"]))
        self.assertEqual(6, found, "the status read, the history, the rollback, the uninstall, the second status read and the install")

    def test_the_install_decision_does_not_trust_the_record_that_was_read_before_a_recovery(self) -> None:
        text = str(self.tasks()[self.index("Decide whether the release must be installed or upgraded")]["ansible.builtin.set_fact"])
        self.assertIn("airbyte_release_recovery", text, "a recovery removed the record and left the hook's ConfigMap at the interrupted run's inputs")

    def test_each_recovery_command_runs_only_for_its_states_changes_only_when_it_runs_and_has_a_deadline(self) -> None:
        for name, argument in (
            ("Roll the release back to its last deployed revision", "rollback"),
            ("Uninstall the release that never finished its first install or its uninstall", "uninstall"),
        ):
            task = self.tasks()[self.index(name)]
            with self.subTest(task=name):
                argv = [str(item) for item in task["ansible.builtin.command"]["argv"]]
                self.assertEqual("helm", argv[0])
                self.assertEqual(argument, argv[1])
                for flag in ("--namespace", "--wait", "--timeout"):
                    self.assertIn(flag, argv)
                self.assertIn("{{ airbyte_release_deadline_seconds }}s", argv)
                self.assertIs(True, task["changed_when"])
                self.assertIn("airbyte_release_deadline_seconds", str(task["timeout"]))
                self.assertIn("when", task)

    def test_the_rollback_is_for_the_two_pending_states_of_an_existing_release_and_the_uninstall_for_the_first_install_and_an_unfinished_uninstall(
        self,
    ) -> None:
        rollback = self.tasks()[self.index("Roll the release back to its last deployed revision")]["when"]
        uninstall = self.tasks()[self.index("Uninstall the release that never finished its first install or its uninstall")]["when"]
        self.assertTrue(all(state in str(rollback) for state in ("pending-upgrade", "pending-rollback")))
        self.assertNotRegex(str(rollback), r"pending-install|uninstalling")
        self.assertTrue(all(state in str(uninstall) for state in ("pending-install", "uninstalling")))
        self.assertNotRegex(str(uninstall), r"pending-(upgrade|rollback)")

    def test_the_recorded_inputs_are_removed_before_any_recovery_command_and_before_the_install_and_written_only_after_the_install(self) -> None:
        for name in ("Forget the recorded inputs before the recovery", "Forget the recorded inputs before the install starts"):
            task = self.tasks()[self.index(name)]
            with self.subTest(task=name):
                self.assertEqual("absent", task["ansible.builtin.file"]["state"])
                self.assertIn("release_inputs.sha256", task["ansible.builtin.file"]["path"])
        record = self.tasks()[self.index("Record the inputs of this successful release")]
        self.assertEqual("{{ airbyte_directory }}/release_inputs.sha256", record["ansible.builtin.copy"]["dest"])
        self.assertIn("airbyte_release_recovery", str(self.tasks()[self.index("Forget the recorded inputs before the recovery")]["when"]))


def merge_values(base: dict[str, Any], later: dict[str, Any]) -> dict[str, Any]:
    """Helm's merge of values files: a later file's mapping merges into the earlier one key by key, and any other value replaces it."""
    merged = dict(base)
    for key, value in later.items():
        merged[key] = merge_values(merged[key], value) if isinstance(value, dict) and isinstance(merged.get(key), dict) else value
    return merged


class PlacementTest(unittest.TestCase):
    def test_the_values_are_placed_with_the_limits_and_the_host_settings_and_a_second_run_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = run_tasks(["place_files.yml"], values(folder) | {"airbyte_bridge_address": "10.42.5.1"})
            placed = folder / "airbyte"
            tracked = (placed / "values.yaml").read_text()
            limits = mapping(load_yaml((placed / "limits.values.yaml").read_text()))
            host = mapping(load_yaml((placed / "host.values.yaml").read_text()))
            again_code, again = run_tasks(["place_files.yml"], values(folder) | {"airbyte_bridge_address": "10.42.5.1"})
        self.assertEqual(0, code, output)
        self.assertEqual((ROLE / "files" / "values.yaml").read_text(), tracked)
        self.assertEqual(f"{load_limits().services['airbyte_server'].memory_limit_mib}Mi", limits["server"]["resources"]["limits"]["memory"])
        self.assertEqual({"host": "10.42.5.1", "port": 5432, "name": "airbyte"}, host["global"]["database"])
        self.assertEqual({"request": "7G", "limit": "7G"}, host["global"]["workloads"]["resources"]["fileTransfer"]["storage"])
        self.assertEqual(8001, host["server"]["service"]["port"])
        self.assertEqual(0, again_code, again)
        self.assertRegex(again, CHANGED_NONE)

    def test_every_database_key_the_chart_reads_with_the_placed_values_is_a_key_the_role_writes(self) -> None:
        """Chart 2.5.0 reads the database account from the secret that ``global.database.secretName`` names, under keys that each values file may set: the
        primary (default ``DATABASE_USER`` and ``DATABASE_PASSWORD``), the replica (default ``CONFIG_DATABASE_REPLICA_USER`` and
        ``CONFIG_DATABASE_REPLICA_PASSWORD``) and Temporal (default: the primary's keys). A key that the role does not write stops the pod that reads it with
        CreateContainerConfigError. The three placed files are merged in Helm's order, later files winning, before the chart's rules are applied."""
        from tests.slow.test_airbyte_secrets import secrets

        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = run_tasks(["place_files.yml"], values(folder))
            self.assertEqual(0, code, output)
            merged: dict[str, Any] = {}
            for name in ("values.yaml", "limits.values.yaml", "host.values.yaml"):
                merged = merge_values(merged, mapping(load_yaml((folder / "airbyte" / name).read_text())))
        database = merged["global"]["database"]
        replica = database.get("replica") or {}
        temporal = (merged.get("temporal") or {}).get("database") or {}
        user = database.get("userSecretKey") or "DATABASE_USER"
        password = database.get("passwordSecretKey") or "DATABASE_PASSWORD"
        read = {
            "user": user,
            "password": password,
            "replica user": replica.get("userSecretKey") or "CONFIG_DATABASE_REPLICA_USER",
            "replica password": replica.get("passwordSecretKey") or "CONFIG_DATABASE_REPLICA_PASSWORD",
            "Temporal user": temporal.get("userSecretKey") or user,
            "Temporal password": temporal.get("passwordSecretKey") or password,
        }
        files = type("Files", (), {"read": lambda self, name: "synthetic-password"})()
        written = secrets.wanted_secrets(files, "synthetic-user")[database["secretName"]]
        for role, key in read.items():
            wanted = "synthetic-password" if role.endswith("password") else "synthetic-user"
            with self.subTest(role):
                self.assertEqual(wanted, written.get(key), f"the {role} key {key!r} does not hold the {role.split()[-1]} in {database['secretName']}")

    def test_temporal_keeps_its_two_databases_apart_and_creates_none(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            run_tasks(["place_files.yml"], values(folder))
            host = mapping(load_yaml((folder / "airbyte" / "host.values.yaml").read_text()))
        environment = {item["name"]: item["value"] for item in host["temporal"]["extraEnv"]}
        self.assertEqual({"DBNAME": "airbyte_temporal", "VISIBILITY_DBNAME": "airbyte_temporal_visibility", "SKIP_DB_CREATE": "true"}, environment)

    def test_temporal_reaches_the_shared_database_without_tls(self) -> None:
        """The shared PostgreSQL server on the k3s bridge has no TLS. Chart 2.5.0 renders Temporal's TLS flags (``SQL_TLS_ENABLED``,
        ``POSTGRES_TLS_ENABLED`` and their host-verification flags) as true exactly when ``global.database.type`` is ``external``, and the schema setup then
        fails with "SSL is not enabled on the server". The chart's default type renders them false while the role's host values still point every JDBC URL
        at the bridge, and ``postgresql.enabled: false`` keeps the chart's own database out. Overriding the flags in ``temporal.extraEnv`` instead repeats
        their names in the Deployment, which Helm's upgrade patch refuses ($setElementOrder), so no TLS name may appear there."""
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = run_tasks(["place_files.yml"], values(folder))
            self.assertEqual(0, code, output)
            merged: dict[str, Any] = {}
            for name in ("values.yaml", "limits.values.yaml", "host.values.yaml"):
                merged = merge_values(merged, mapping(load_yaml((folder / "airbyte" / name).read_text())))
        self.assertNotEqual("external", merged["global"]["database"].get("type"))
        self.assertIs(False, merged["postgresql"]["enabled"])
        names = [item["name"] for item in merged["temporal"]["extraEnv"]]
        self.assertEqual([], [name for name in names if "TLS" in name])

    def test_the_public_url_is_set_only_when_given(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            run_tasks(["place_files.yml"], values(folder))
            without = mapping(load_yaml((folder / "airbyte" / "host.values.yaml").read_text()))
            run_tasks(["place_files.yml"], values(folder) | {"airbyte_public_url": "https://airbyte.example.invalid:9446"})
            with_url = mapping(load_yaml((folder / "airbyte" / "host.values.yaml").read_text()))
        self.assertNotIn("airbyteUrl", without["global"])
        self.assertEqual("https://airbyte.example.invalid:9446", with_url["global"]["airbyteUrl"])

    def test_a_changed_limit_reaches_the_placed_file_on_the_next_run(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            run_tasks(["place_files.yml"], values(folder))
            (folder / "airbyte" / "limits.values.yaml").write_text("server: {}\n")
            code, output = run_tasks(["place_files.yml"], values(folder))
            restored = mapping(load_yaml((folder / "airbyte" / "limits.values.yaml").read_text()))
        self.assertEqual(0, code, output)
        self.assertIn("resources", restored["server"])


class WebForwarderTest(unittest.TestCase):
    def unit(self, **changes: object) -> tuple[int, str, str]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = run_tasks(["web_forward_unit.yml"], values(folder) | changes)
            written = Path(str(values(folder)["airbyte_web_unit"]))
            text = written.read_text() if written.exists() else ""
        return code, output, text

    def test_the_unit_forwards_the_server_service_to_the_local_address_only_and_restarts(self) -> None:
        code, output, text = self.unit()
        self.assertEqual(0, code, output)
        command = next(line for line in text.splitlines() if line.startswith("ExecStart="))
        self.assertIn("--address 127.0.0.1", command)
        self.assertIn("service/airbyte-airbyte-server-svc 8000:8001", command)
        self.assertIn("--namespace airbyte", command)
        self.assertIn("Restart=always", text)
        self.assertNotRegex(text, r"--address\s+(?!127\.0\.0\.1)")

    def test_the_unit_follows_the_namespace_release_and_ports_it_is_given(self) -> None:
        _, _, text = self.unit(airbyte_namespace="other", airbyte_release="named", airbyte_web_port=8123, airbyte_server_service_port=9001)
        command = next(line for line in text.splitlines() if line.startswith("ExecStart="))
        self.assertIn("--namespace other", command)
        self.assertIn("service/named-airbyte-server-svc 8123:9001", command)

    def test_a_second_run_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            run_tasks(["web_forward_unit.yml"], values(folder))
            code, output = run_tasks(["web_forward_unit.yml"], values(folder))
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_NONE)


def write_subnet(folder: Path, text: str) -> None:
    (folder / "subnet.env").write_text(text)


class BridgeAddressTest(unittest.TestCase):
    """The pods reach PostgreSQL at the k3s bridge's address, which the role reads from k3s and never types."""

    def read(self, text: str | None, **changes: object) -> tuple[int, str, str]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            if text is not None:
                write_subnet(folder, text)
            variables = {key: value for key, value in values(folder).items() if key != "airbyte_bridge_address"} | changes
            code, output = run_tasks(["bridge_address.yml", "postgres_route_units.yml"], variables)
            socket = folder / "units" / "airbyte-postgres.socket"
            return code, output, socket.read_text() if socket.exists() else ""

    def test_the_address_is_the_first_address_of_the_nodes_subnet_in_flannels_file(self) -> None:
        code, output, socket = self.read("FLANNEL_NETWORK=10.42.0.0/16\nFLANNEL_SUBNET=10.42.0.1/24\nFLANNEL_MTU=8951\nFLANNEL_IPMASQ=true\n")
        self.assertEqual(0, code, output)
        self.assertIn(f"ListenStream={BRIDGE}:5432", socket)

    def test_another_subnet_gives_another_address(self) -> None:
        _, _, socket = self.read("FLANNEL_NETWORK=10.42.0.0/16\nFLANNEL_SUBNET=10.42.7.1/24\n")
        self.assertIn("ListenStream=10.42.7.1:5432", socket)

    def test_a_missing_file_stops_the_deployment_at_the_deadline_and_names_the_file_and_the_deadline(self) -> None:
        code, output, socket = self.read(None)
        self.assertNotEqual(0, code)
        self.assertIn("subnet.env", output)
        self.assertIn("2 seconds", output)
        self.assertEqual("", socket)

    def test_a_file_with_no_subnet_line_or_a_malformed_one_stops_the_deployment_and_places_no_unit(self) -> None:
        for text in (
            "FLANNEL_NETWORK=10.42.0.0/16\n",
            "FLANNEL_SUBNET=not-an-address/24\n",
            "FLANNEL_SUBNET=10.42.0.1\n",
            "FLANNEL_SUBNET=10.42.0.1/24 extra\n",
        ):
            with self.subTest(text=text):
                code, output, socket = self.read(text)
                self.assertNotEqual(0, code, output)
                self.assertEqual("", socket)

    def test_no_file_of_the_role_types_the_bridge_address(self) -> None:
        for path in [*(ROLE / "tasks").glob("*.yml"), *(ROLE / "templates").glob("*"), ROLE / "files" / "values.yaml", GROUP_VARIABLES]:
            with self.subTest(path=path.name):
                self.assertNotIn("10.42.0.1", path.read_text())


class PostgresRouteTest(unittest.TestCase):
    """The socket-activated forwarder from the bridge address to the server's local address."""

    def units(self, **changes: object) -> tuple[int, str, str, str]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = run_tasks(["postgres_route_units.yml"], values(folder) | changes)
            socket, service = (folder / "units" / f"airbyte-postgres.{kind}" for kind in ("socket", "service"))
            return code, output, socket.read_text() if socket.exists() else "", service.read_text() if service.exists() else ""

    def test_the_socket_listens_on_the_bridge_address_and_the_port_only_and_may_bind_before_the_bridge_exists(self) -> None:
        code, output, socket, _ = self.units()
        self.assertEqual(0, code, output)
        listens = [line for line in socket.splitlines() if line.startswith("Listen")]
        self.assertEqual([f"ListenStream={BRIDGE}:5432"], listens)
        self.assertIn("FreeBind=true", socket)
        self.assertIn("WantedBy=sockets.target", socket)
        self.assertNotRegex(socket, r"0\.0\.0\.0|\[::\]|ListenStream=\d+\s")

    def test_the_service_forwards_to_the_servers_local_address_with_the_shipped_proxy_and_stays_in_the_host_network(self) -> None:
        _, _, _, service = self.units()
        command = next(line for line in service.splitlines() if line.startswith("ExecStart="))
        self.assertEqual("ExecStart=/usr/lib/systemd/systemd-socket-proxyd 127.0.0.1:5432", command)
        self.assertIn("Requires=airbyte-postgres.socket", service)
        self.assertIn("After=airbyte-postgres.socket", service)
        self.assertIn("Type=notify", service)
        self.assertNotIn("PrivateNetwork", service)
        self.assertNotRegex(service, r"ExecStart=.*(0\.0\.0\.0|\b10\.)")

    def test_the_units_follow_the_address_and_the_ports_they_are_given(self) -> None:
        _, _, socket, service = self.units(airbyte_bridge_address="10.42.9.1", airbyte_database_port=6543, compose_stack_postgres_port=6000)
        self.assertIn("ListenStream=10.42.9.1:6543", socket)
        self.assertIn("systemd-socket-proxyd 127.0.0.1:6000", service)

    def test_a_second_run_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            run_tasks(["postgres_route_units.yml"], values(folder))
            code, output = run_tasks(["postgres_route_units.yml"], values(folder))
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_NONE)

    def test_the_route_is_started_before_the_release_that_needs_it(self) -> None:
        main = (ROLE / "tasks" / "main.yml").read_text()
        for earlier, later in (("bridge_address.yml", "postgres_route.yml"), ("postgres_route.yml", "release.yml"), ("cluster.yml", "network_policy.yml")):
            with self.subTest(earlier=earlier, later=later):
                self.assertLess(main.index(earlier), main.index(later))
        self.assertLess(main.index("network_policy.yml"), main.index("release.yml"))

    def test_the_group_commands_get_their_access_after_the_secrets_and_before_the_release(self) -> None:
        main = (ROLE / "tasks" / "main.yml").read_text()
        self.assertIn("ansible.builtin.include_tasks: group_access.yml", main)
        self.assertLess(main.index("secrets.yml"), main.index("group_access.yml"))
        self.assertLess(main.index("group_access.yml"), main.index("release.yml"))


class GroupAccessAloneTest(unittest.TestCase):
    """The stack role runs the access step before the rest of this role, so the step creates the folder it writes into itself."""

    def test_the_access_step_creates_airbytes_folder_before_it_writes_the_account_file(self) -> None:
        document: Any = load_yaml((ROLE / "tasks" / "group_access.yml").read_text())
        tasks = document[0]["block"]
        folder = next(index for index, task in enumerate(tasks) if (task.get("ansible.builtin.file") or {}).get("path") == "{{ airbyte_directory }}")
        template = next(index for index, task in enumerate(tasks) if "ansible.builtin.template" in task)
        self.assertEqual("directory", tasks[folder]["ansible.builtin.file"]["state"])
        self.assertLess(folder, template)


class GroupAccessCheckTest(unittest.TestCase):
    """`kubectl auth can-i VERB kind/sub` reads `sub` as a resource name, so a subresource check must pass `--subresource` (host run, Nov 23 2026)."""

    def check(self) -> dict[str, Any]:
        document: Any = load_yaml((ROLE / "tasks" / "group_access.yml").read_text())
        return next(task for task in document[0]["block"] if task.get("name") == "Check what the account may do and may not do")

    def test_no_check_names_a_subresource_inside_the_resource(self) -> None:
        for item in self.check()["loop"]:
            with self.subTest(item=item):
                self.assertNotIn("/", item["resource"])

    def test_the_scale_and_exec_checks_name_their_subresource_and_the_command_passes_it(self) -> None:
        task = self.check()
        subresources = {(item["verb"], item["resource"], item.get("subresource")) for item in task["loop"]}
        self.assertTrue({("patch", "deployments.apps", "scale"), ("patch", "statefulsets.apps", "scale"), ("create", "pods", "exec")} <= subresources)
        self.assertIn("--subresource={{ item.subresource | default('') }}", task["ansible.builtin.command"]["argv"])

    def test_every_resource_outside_the_core_group_names_its_group(self) -> None:
        """Without its group, kubectl resolves a resource through discovery; when discovery fails it falls back to the core group, warns on stderr and answers
        no, so a denial would pass for the wrong group."""
        core = {"pods", "secrets", "services", "configmaps", "serviceaccounts", "persistentvolumeclaims", "events", "endpoints"}
        for item in self.check()["loop"]:
            with self.subTest(item=item):
                self.assertTrue(item["resource"] in core or "." in item["resource"], "name the API group, as in deployments.apps")

    def test_any_output_on_stderr_fails_the_check(self) -> None:
        condition = self.check()["failed_when"]
        conditions = condition if isinstance(condition, list) else [condition]
        expected = "airbyte_group_access_checks.stdout | trim != item.answer or airbyte_group_access_checks.stderr | trim | length > 0"
        self.assertEqual([expected], [" ".join(text.split()) for text in conditions], "one condition, exactly this one: a second clause could pass a warned no")


def policies(text: str) -> list[dict[str, Any]]:
    return [mapping(load_yaml(part)) for part in text.split("\n---\n") if part.strip()]


class NetworkPolicyTest(unittest.TestCase):
    def render(self, others: list[str] | None = None, **changes: object) -> tuple[int, str, list[dict[str, Any]]]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            (folder / "airbyte").mkdir()
            variables = values(folder) | {"airbyte_other_namespaces": ["kube-system", "default"] if others is None else others} | changes
            code, output = run_tasks(["network_policy_files.yml"], variables)
            written = folder / "airbyte" / "network_policies.yaml"
            return code, output, policies(written.read_text()) if written.exists() else []

    def rule_addresses(self, policy: dict[str, Any]) -> list[tuple[str, list[str], list[tuple[str, int]]]]:
        found = []
        for rule in policy["spec"].get("egress", []):
            ports = [(item["protocol"], int(item["port"])) for item in rule.get("ports", [])]
            for peer in rule.get("to", []):
                block = peer["ipBlock"]
                found.append((block["cidr"], list(block.get("except", [])), ports))
        return found

    def test_airbytes_pods_may_open_connections_to_the_bridge_address_on_the_database_port_and_nowhere_else_on_it(self) -> None:
        code, output, found = self.render()
        self.assertEqual(0, code, output)
        (own,) = [policy for policy in found if policy["metadata"]["namespace"] == "airbyte"]
        self.assertEqual(
            ("NetworkPolicy", "postgres-bridge", {}, ["Egress"]), (own["kind"], own["metadata"]["name"], own["spec"]["podSelector"], own["spec"]["policyTypes"])
        )
        self.assertEqual(
            [(f"{BRIDGE}/32", [], [("TCP", 5432)]), ("0.0.0.0/0", [f"{BRIDGE}/32"], [])],
            self.rule_addresses(own),
        )

    def test_every_other_namespace_may_reach_everything_except_the_bridge_address(self) -> None:
        _, _, found = self.render(["kube-system", "default"])
        others = {policy["metadata"]["namespace"]: policy for policy in found if policy["metadata"]["namespace"] != "airbyte"}
        self.assertEqual({"kube-system", "default"}, set(others))
        for namespace, policy in others.items():
            with self.subTest(namespace=namespace):
                self.assertEqual(
                    ("deny-postgres-bridge", {}, ["Egress"]), (policy["metadata"]["name"], policy["spec"]["podSelector"], policy["spec"]["policyTypes"])
                )
                self.assertEqual([("0.0.0.0/0", [f"{BRIDGE}/32"], [])], self.rule_addresses(policy))

    def test_no_rule_gives_a_path_to_the_bridge_address_except_the_one_for_airbytes_own_namespace(self) -> None:
        _, _, found = self.render(["a", "b", "c"])
        granting = [policy["metadata"]["namespace"] for policy in found for cidr, _, _ in self.rule_addresses(policy) if cidr == f"{BRIDGE}/32"]
        self.assertEqual(["airbyte"], granting)

    def test_the_airbyte_namespace_is_never_given_the_denying_policy_even_when_listed(self) -> None:
        _, _, found = self.render(["airbyte", "other"])
        names = sorted((policy["metadata"]["namespace"], policy["metadata"]["name"]) for policy in found)
        self.assertEqual([("airbyte", "postgres-bridge"), ("other", "deny-postgres-bridge")], names)

    def test_the_policies_follow_the_address_and_the_port_they_are_given(self) -> None:
        _, _, found = self.render(["other"], airbyte_bridge_address="10.42.9.1", airbyte_database_port=6543)
        (own,) = [policy for policy in found if policy["metadata"]["namespace"] == "airbyte"]
        self.assertEqual(("10.42.9.1/32", [], [("TCP", 6543)]), self.rule_addresses(own)[0])

    def test_a_second_run_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            (folder / "airbyte").mkdir()
            variables = values(folder) | {"airbyte_other_namespaces": ["kube-system"]}
            run_tasks(["network_policy_files.yml"], variables)
            code, output = run_tasks(["network_policy_files.yml"], variables)
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_NONE)


class TemporalDatabasesTest(unittest.TestCase):
    def test_the_two_databases_that_temporal_is_given_are_owned_by_the_airbyte_tool_in_the_tracked_list(self) -> None:
        owned = accounts.parse_tools(TOOLS_FILE.read_text())["airbyte"]
        variables = group_variables()
        self.assertIn(variables["airbyte_temporal_database"], owned)
        self.assertIn(variables["airbyte_temporal_visibility_database"], owned)
        self.assertIn(variables["airbyte_database_name"], owned)


class ChartCheckTest(unittest.TestCase):
    def chart_defaults(self, extra: dict[str, object] | None = None) -> str:
        enabled = {
            "server",
            "worker",
            "workloadLauncher",
            "airbyteBootloader",
            "temporal",
            "postgresql",
            "minio",
            "cron",
            "manifestServer",
            "workloadApiServer",
        }
        chart: dict[str, object] = {key: {"enabled": key in enabled, "resources": {"requests": {}, "limits": {}}} for key in CHART_WORKLOAD_KEYS}
        mapping(chart["temporal"])["adminTools"] = {"resources": {"limits": {}}}
        chart["ingress"] = {"enabled": False}
        chart["global"] = {
            "workloads": {"resources": {kind: {"cpu": {}, "memory": {}} for kind in ("mainContainer", "check", "discover", "replication", "sidecar")}}
        }
        return json.dumps(chart | (extra or {}))

    def verdict(self, text: str) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as scratch:
            return run_tasks(["verify_chart.yml"], values(Path(scratch)) | {"airbyte_chart_defaults": {"stdout": text}})

    def test_the_chart_that_matches_the_tracked_values_and_the_limits_passes(self) -> None:
        code, output = self.verdict(self.chart_defaults())
        self.assertEqual(0, code, output)

    def test_a_workload_that_a_newer_chart_adds_stops_the_deployment_and_is_named(self) -> None:
        code, output = self.verdict(self.chart_defaults({"telemetryCollector": {"enabled": True}}))
        self.assertNotEqual(0, code)
        self.assertIn("telemetryCollector", output)


class PlaybookTest(unittest.TestCase):
    """The deployment is split in three runs, in this order: the host playbook, the launcher's intake of the external secrets, the stack playbook."""

    def test_the_role_is_not_in_the_host_playbook_and_follows_the_stack_deployment_in_the_stack_playbook(self) -> None:
        playbook = load_yaml((ROOT / "ansible" / "host.yml").read_text())
        if not isinstance(playbook, list):
            self.fail("the playbook must be a list")
        roles = [str(role) for role in playbook[0]["roles"]]
        self.assertEqual("compose_stack", roles[-1], "the host playbook ends with the part that prepares OpenBao and installs the intake script")
        self.assertNotIn("airbyte", roles, "the role needs the images that the stack playbook pulls with the token that the intake stores")
        self.assertEqual([("compose_stack", "deploy.yml"), ("airbyte", "main.yml")], included_roles("stack.yml"))

    def test_the_launcher_runs_the_host_playbook_then_the_intake_then_the_stack_playbook(self) -> None:
        source = (ROOT / "scripts" / "infra" / "host.py").read_text()
        order = [
            source.index(step) for step in ("run_ansible(run.run_id, address, variables)\n", "place_external_secrets(run.run_id", 'variables, "stack.yml")')
        ]
        self.assertEqual(sorted(order), order)


if __name__ == "__main__":
    unittest.main()
