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
import time
import unittest
from pathlib import Path
from typing import Any

from scripts.process import run_command
from scripts.stack.k3s_limits import CHART_WORKLOAD_KEYS
from scripts.stack.limits import load_limits
from scripts.stack.yaml_text import load_yaml
from tests.fast.ansible_paths import GROUP_VARIABLES, group_variables
from tests.fast.test_postgres_accounts import TOOLS_FILE, accounts
from tests.slow.ansible_support import ROLES, environment, run_playbook

ROOT = Path(__file__).resolve().parents[2]
ROLE = ROLES / "airbyte"
OWNER = pwd.getpwuid(os.getuid()).pw_name
GROUP = grp.getgrgid(os.getgid()).gr_name
BRIDGE = "10.42.0.1"
HEX64 = re.compile(r"[0-9a-f]{64}")
CHANGED_NONE = re.compile(r"changed=0\s")
COMMAND_MODULES = ("ansible.builtin.command", "ansible.builtin.shell")
# The deployments of the chart that hold PostgreSQL connections and that its templates leave to a rolling update with a surge (the rendered release shows the
# strategy of each: tests/slow/test_airbyte_rendered_environment.py). The server and the workload launcher are Recreate in the chart already.
RECREATE_DEPLOYMENTS = ("temporal", "cron", "worker", "workload-api-server")


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
        "airbyte_recreate_deployments": list(RECREATE_DEPLOYMENTS),
        "airbyte_scale_down_deadline_seconds": 30,
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

    def test_the_variable_that_names_the_deployments_to_recreate_is_the_one_of_the_fixed_list(self) -> None:
        self.assertEqual(list(RECREATE_DEPLOYMENTS), group_variables()["airbyte_recreate_deployments"])

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
  renderer=""; renderer_arguments=""; prev=""
  for argument in "$@"; do
    [ "$prev" = "--post-renderer" ] && renderer="$argument"
    [ "$prev" = "--post-renderer-args" ] && renderer_arguments="$renderer_arguments $argument"
    prev="$argument"
  done
  if [ -n "$renderer" ]; then
    # Helm runs the post-renderer on the rendered manifests and applies its output; a failing post-renderer fails the upgrade before anything is applied.
    # shellcheck disable=SC2086
    "$STAND_IN_PYTHON" -I "$renderer" $renderer_arguments < "$STAND_IN/manifests.yaml" > "$STAND_IN/applied.yaml" 2>/dev/null || refuse post-renderer
  else
    cp "$STAND_IN/manifests.yaml" "$STAND_IN/applied.yaml"
  fi
  "$STAND_IN_PYTHON" "$STAND_IN/rollout.py" "$STAND_IN/applied.yaml"
  printf deployed > "$STAND_IN/status"; echo deployed >> "$STAND_IN/history" ;;
rollback)
  # A rollback applies the manifests of an older revision, which no post-renderer shaped (Helm's rollback has no --post-renderer): the chart's own manifests.
  "$STAND_IN_PYTHON" "$STAND_IN/rollout.py" "$STAND_IN/manifests.yaml"
  : > "$STAND_IN/recovered"; printf deployed > "$STAND_IN/status"; echo deployed >> "$STAND_IN/history" ;;
uninstall) : > "$STAND_IN/recovered"; : > "$STAND_IN/status"; : > "$STAND_IN/history" ;;
*) echo "unexpected: $*" >&2; exit 9 ;;
esac
"""
# The k3s stand-in logs each call and answers ``kubectl get deployment --output name`` with two deployments (``flag_no_deployments`` lists none).
# ``flag_fail_restart`` fails the restart and ``flag_fail_rollout`` fails the rollout status of the second deployment, each with text that must never reach
# the task log. It keeps the running pods in the file ``pods`` (one deployment name per line, the old pod of each) and, for each deployment, the replicas it
# wants (``spec_<name>``, one when absent), its generation (``gen_<name>``) and the generation the controller has seen (``observed_<name>``). ``get
# deployment <name>`` answers by its ``--output`` template: the uid, replicas and selector (``flag_scale_missing`` answers with the NotFound error for an
# absent one, ``flag_fail_scale_read`` fails every read and ``flag_fail_read_<short name>`` fails the read of one deployment with a ServiceUnavailable
# error), the generation state (``flag_observed_lags`` lets the controller see a scale one read late, ``flag_observed_stuck`` never) or the replicas alone.
# ``scale`` takes the deployment's pod off the list at once and logs ``old exits <name>`` (``flag_fail_scale`` fails every scale, ``flag_fail_scale_<short
# name>`` fails the scale to zero of one deployment, ``flag_fail_scale_applied`` after it took effect, ``flag_fail_restore`` fails each scale to more than
# zero and the file ``after_fail_scale`` is sourced after a failed scale to model another writer). ``get replicaset`` shows the replicas the owned
# ReplicaSets want: one until the first poll after the scale with ``flag_rs_lingers`` and for ever with ``flag_rs_stays``. ``get pods --selector`` shows the
# old pod until the poll count of ``flag_pods_linger`` (one poll) has passed, and for ever with ``flag_pods_stay``. With the file ``clock`` the stand-in
# programs ``date`` and ``sleep`` run on a clock of whole seconds that only ``sleep`` and the answer time of ``get pods`` (the seconds in the file
# ``delay_pods``, only for the deployment that the file ``delay_pods_only`` names when it exists) move; a request whose ``--request-timeout`` is shorter than
# its answer time fails after that timeout, as kubectl's client does.
K3S_STAND_IN = """#!/bin/sh
echo "$*" >> "$STAND_IN/log"
leak() { echo "Error: LEAKED-STDERR $1" >&2; echo "LEAKED-STDOUT $1"; exit 1; }
fail_after_scale() { [ -e "$STAND_IN/after_fail_scale" ] && . "$STAND_IN/after_fail_scale"; leak scale; }
flag() { [ -e "$STAND_IN/flag_$1" ]; }
recall() { if [ -e "$STAND_IN/$1" ]; then cat "$STAND_IN/$1"; else echo "$2"; fi; }
advance() { [ -e "$STAND_IN/clock" ] && echo $(( $(cat "$STAND_IN/clock") + $1 )) > "$STAND_IN/clock"; return 0; }
limit=""; output=""; replicas=""; prev=""
for argument in "$@"; do
  case "$argument" in --request-timeout=*) limit=${argument#*=} ;; --replicas=*) replicas=${argument#*=} ;; esac
  [ "$prev" = "--request-timeout" ] && limit=$argument
  [ "$prev" = "--output" ] && output=$argument
  prev="$argument"
done
limit=${limit%s}
case "$2 $3" in
"scale deployment")
  short=${4#airbyte-}
  [ -e "$STAND_IN/flag_fail_scale" ] && leak scale
  [ "$replicas" != 0 ] && flag fail_restore && leak restore
  if [ "$replicas" = 0 ] && flag "fail_scale_$short" && ! flag fail_scale_applied; then fail_after_scale; fi
  if [ "$(recall "spec_$4" 1)" != "$replicas" ]; then
    generation=$(recall "gen_$4" 1)
    if flag observed_lags || flag observed_stuck; then echo "$generation" > "$STAND_IN/observed_$4"; fi
    echo $(( generation + 1 )) > "$STAND_IN/gen_$4"; echo "$replicas" > "$STAND_IN/spec_$4"
  fi
  if [ "$replicas" = 0 ]; then
    if grep -qx "$4" "$STAND_IN/pods"; then
      grep -vx "$4" "$STAND_IN/pods" > "$STAND_IN/pods.next"; mv "$STAND_IN/pods.next" "$STAND_IN/pods"; echo "old exits $4" >> "$STAND_IN/pods.log"
    fi
    : > "$STAND_IN/scaled_$4"
  fi
  if [ "$replicas" = 0 ] && flag "fail_scale_$short" && flag fail_scale_applied; then fail_after_scale; fi ;;
"get pods")
  selector=""; prev=""
  for argument in "$@"; do [ "$prev" = "--selector" ] && selector="$argument"; prev="$argument"; done
  [ -n "$selector" ] || exit 0
  name=${selector#airbyte=}; name=${name%,}
  delay=$(recall delay_pods 0)
  [ "$(recall delay_pods_only "$name")" = "$name" ] || delay=0
  if [ -n "$limit" ] && [ "$delay" -gt "$limit" ]; then advance "$limit"; echo "Unable to connect to the server: Client.Timeout exceeded" >&2; exit 1; fi
  advance "$delay"
  flag pods_stay && { echo "pod/$name-0"; exit 0; }
  if flag pods_linger && [ -e "$STAND_IN/scaled_$name" ] && [ ! -e "$STAND_IN/polled_$name" ]; then
    : > "$STAND_IN/polled_$name"; echo "pod/$name-0"
  fi ;;
"get replicaset")
  selector=""; prev=""
  for argument in "$@"; do [ "$prev" = "--selector" ] && selector="$argument"; prev="$argument"; done
  name=${selector#airbyte=}; name=${name%,}
  case "$output" in *"uid-$name"*) ;; *) leak "replicaset template without the uid of $name" ;; esac
  if flag rs_stays || [ ! -e "$STAND_IN/scaled_$name" ]; then echo "1 "; exit 0; fi
  if flag rs_lingers && [ ! -e "$STAND_IN/rs_polled_$name" ]; then : > "$STAND_IN/rs_polled_$name"; echo "1 "; exit 0; fi
  echo "0 0 " ;;
"rollout restart") [ -e "$STAND_IN/flag_fail_restart" ] && leak restart ;;
"get deployment")
  case "$4" in
  --*) [ -e "$STAND_IN/flag_no_deployments" ] || printf 'deployment.apps/airbyte-server\ndeployment.apps/airbyte-workload-launcher\n' ;;
  *)
    short=${4#airbyte-}
    [ -e "$STAND_IN/flag_fail_scale_read" ] && leak read
    [ -e "$STAND_IN/flag_scale_missing" ] && { echo "Error from server (NotFound): deployments.apps \"$4\" not found" >&2; exit 1; }
    spec=$(recall "spec_$4" 1)
    case "$output" in
    *observedGeneration*)
      generation=$(recall "gen_$4" 1)
      echo "$generation $(recall "observed_$4" "$generation") $spec"
      flag observed_lags && rm -f "$STAND_IN/observed_$4" ;;
    *metadata.uid*)
      flag "fail_read_$short" && { echo "Error from server (ServiceUnavailable): the server is currently unable to handle the request" >&2; exit 1; }
      if flag empty_selector || flag "empty_selector_$short"; then echo "uid-$4 $spec "; else echo "uid-$4 $spec airbyte=$4,"; fi ;;
    *) echo "$spec" ;;
    esac ;;
  esac ;;
"rollout status")
  [ -e "$STAND_IN/flag_fail_rollout" ] && [ "$4" = deployment.apps/airbyte-workload-launcher ] && leak rollout
  [ -e "$STAND_IN/flag_slow_rollout" ] && sleep 2 ;;
esac
exit 0
"""
# A ``date`` and a ``sleep`` that follow the stand-in's clock when the file ``clock`` exists and are the real programs otherwise.
CLOCK_SHIM = """#!/bin/sh
PATH=/usr/bin:/bin
[ -e "$STAND_IN/clock" ] || exec {program} "$@"
{body}
"""
DATE_SHIM = CLOCK_SHIM.format(program="date", body='[ "$*" = "+%s" ] && cat "$STAND_IN/clock" || exec date "$@"')
SLEEP_SHIM = CLOCK_SHIM.format(program="sleep", body='echo $(( $(cat "$STAND_IN/clock") + $1 )) > "$STAND_IN/clock"')
CHART_SHA = "c" * 64
DEADLINE = 600
UPGRADE = "upgrade --install"
NOT_INSTALLED = ""
VALUES_FILES = ("values.yaml", "limits.values.yaml", "host.values.yaml")
POST_RENDERER = "recreate_post_renderer.py"
# The files whose checksums are part of the release's inputs: the three values files and the post-renderer that shapes every manifest.
INPUT_FILES = (*VALUES_FILES, POST_RENDERER)
HOST_VALUES_B = "host.values.yaml from inputs B\n"
CONFIGMAP_A = "".join(f"{name}\n" for name in VALUES_FILES)
CONFIGMAP_B = CONFIGMAP_A.replace("host.values.yaml\n", HOST_VALUES_B)
LEAK = "LEAKED"
RECORD = "release_inputs.sha256"
# The form of the recorded inputs: the role writes it into the digest, so a record from a role that did not restart the deployments never matches.
RECORD_FORM = "release-inputs-3"
RESTART = [
    "kubectl rollout restart deployment",
    "kubectl get deployment",
    "kubectl rollout status deployment.apps/airbyte-server",
    "kubectl rollout status deployment.apps/airbyte-workload-launcher",
]
# The post-renderer makes each deployment that holds connections Recreate in the manifests of the upgrade itself, so no patch follows the upgrade. A rollback
# applies the manifests of an older revision, which no post-renderer shaped, so the role first takes the pods of each such deployment away. It reads every
# deployment's selector and replicas before it scales any (one read for each deployment), scales each to zero (one call for each), and then, for each
# deployment, polls until the controller has seen the scale, every owned ReplicaSet wants no replica and no pod has the selector (one deployment read, one
# ReplicaSet list and one pod list for each poll).
SCALE_DOWN = [
    *[f"kubectl get deployment airbyte-{name}" for name in RECREATE_DEPLOYMENTS],
    *[f"kubectl scale deployment airbyte-{name}" for name in RECREATE_DEPLOYMENTS],
    *[line for name in RECREATE_DEPLOYMENTS for line in (f"kubectl get deployment airbyte-{name}", "kubectl get replicaset", "kubectl get pods")],
]
UPGRADE_AND_RESTART = [UPGRADE, *RESTART]
# The chart's own manifests for the deployments that hold connections and two that the role leaves alone, as ``helm template`` renders them for chart 2.5.0
# (tests/slow/test_airbyte_rendered_environment.py renders the real ones): no strategy for Temporal, the cron and the worker, a rolling update with a surge
# for the workload API server and for the manifest server (which holds no connection), and Recreate for the server.
DEPLOYMENT = """\
---
# Source: airbyte/templates/airbyte-{name}/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  namespace: airbyte
  name: airbyte-{name}
spec:
  replicas: 1
  selector:
    matchLabels:
      airbyte: {name}
{strategy}  template:
    metadata:
      labels:
        airbyte: {name}
"""
STRATEGIES = {
    "temporal": "",
    "cron": "",
    "worker": "",
    "workload-api-server": (
        "  strategy:\n    type: RollingUpdate\n    rollingUpdate:\n      maxUnavailable: 0\n      # Surge one pod at a time instead.\n      maxSurge: 1\n"
    ),
    "manifest-server": "  strategy:\n    type: RollingUpdate\n",
    "server": "  strategy:\n    type: Recreate # Needed due to volume claims\n",
}
MANIFESTS = "".join(DEPLOYMENT.format(name=name, strategy=strategy) for name, strategy in STRATEGIES.items())
# Applies a set of manifests to the pods in the file ``pods`` the way the Deployment controller would, one event for each deployment in ``pods.log``:
# Recreate stops the old pod before it starts the new one, and a rolling update (also the default, which surges by one pod) starts the new pod first. A
# deployment with no old pod only starts the new one.
ROLLOUT = """\
import sys
from pathlib import Path

import yaml

folder = Path(sys.argv[0]).parent
running = (folder / "pods").read_text().split()
events = []
for document in yaml.safe_load_all(Path(sys.argv[1]).read_text()):
    if not document or document["kind"] != "Deployment":
        continue
    name = document["metadata"]["name"]
    recreate = document["spec"].get("strategy", {}).get("type") == "Recreate"
    if name not in running:
        events.append(f"new starts {name}")
    elif recreate:
        events += [f"old exits {name}", f"new starts {name}"]
    else:
        events += [f"new starts {name}", f"old exits {name}"]
    running = [item for item in running if item != name] + [name]
(folder / "pods").write_text("".join(f"{item}\\n" for item in running))
with (folder / "pods.log").open("a") as log:
    log.write("".join(f"{item}\\n" for item in events))
"""


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
        for name, text in (("helm", HELM_STAND_IN), ("k3s", K3S_STAND_IN), ("date", DATE_SHIM), ("sleep", SLEEP_SHIM)):
            (programs / name).write_text(text)
            (programs / name).chmod(stat.S_IRWXU)
        (self.stand_in / "rollout.py").write_text(ROLLOUT)
        (self.stand_in / "manifests.yaml").write_text(MANIFESTS)
        (self.stand_in / "pods").write_text("".join(f"airbyte-{name}\n" for name in STRATEGIES))
        (self.stand_in / "status").write_text(status)
        (self.stand_in / "history").write_text("".join(f"{item['status']}\n" for item in past or []))
        self.variables = (
            values(folder) | {"airbyte_chart_version": "1.2.3", "airbyte_chart_sha256": CHART_SHA, "airbyte_release_deadline_seconds": DEADLINE} | changes
        )
        self.directory = Path(str(self.variables["airbyte_directory"]))
        self.directory.mkdir()
        for name in VALUES_FILES:
            (self.directory / name).write_text(f"{name}\n")
        # The role's place_files step copies the post-renderer beside the values files, and the release reads its checksum with theirs.
        (self.directory / POST_RENDERER).write_bytes((ROLE / "files" / POST_RENDERER).read_bytes())
        sums = [hashlib.sha256((self.directory / name).read_bytes()).hexdigest() for name in INPUT_FILES]
        names = ",".join(changes.get("airbyte_recreate_deployments", RECREATE_DEPLOYMENTS))  # type: ignore[arg-type]
        self.inputs = hashlib.sha256(":".join([RECORD_FORM, CHART_SHA, *sums, names]).encode()).hexdigest()
        if recorded == "match":
            (self.directory / RECORD).write_text(f"{self.inputs}\n")
        elif recorded == "legacy":
            # The record that the role wrote before it restarted the deployments: the same inputs without the form, so it never matches now.
            (self.directory / RECORD).write_text(hashlib.sha256(":".join([CHART_SHA, *sums[:3]]).encode()).hexdigest() + "\n")
        elif recorded == "other":
            (self.directory / RECORD).write_text("0" * 64 + "\n")
        self.playbook = folder / "check.yml"
        include = f"    - ansible.builtin.include_role:\n        name: {ROLE}\n        tasks_from: release.yml\n"
        self.playbook.write_text(f"- hosts: localhost\n  connection: local\n  gather_facts: false\n  tasks:\n{include}")
        # The host launcher passes its own environment on, so a shell with ANSIBLE_VERBOSITY=1 prints every task's result: the cases run at that level.
        self.env = environment(folder) | {
            "PATH": f"{programs}:/usr/bin:/bin",
            "STAND_IN": str(self.stand_in),
            "STAND_IN_PYTHON": str(ROOT / ".venv" / "bin" / "python"),
            "ANSIBLE_VERBOSITY": "1",
        }

    def run(self, flags: tuple[str, ...] = (), host_values: str | None = None, seed: dict[str, str] | None = None) -> tuple[int, str, list[str]]:
        """Run ``release.yml`` once. ``seed`` writes files of the k3s stand-in (the replicas of a deployment, the clock) before the run."""
        for name in ("status_seen", "recovered", "log", "pods.log", "pods.next", "clock", "delay_pods", "delay_pods_only", "after_fail_scale"):
            (self.stand_in / name).unlink(missing_ok=True)
        for pattern in ("flag_*", "scaled_*", "polled_*", "rs_polled_*", "spec_*", "gen_*", "observed_*"):
            for old in self.stand_in.glob(pattern):
                old.unlink()
        for name, text in (seed or {}).items():
            (self.stand_in / name).write_text(text)
        (self.stand_in / "pods").write_text("".join(f"airbyte-{name}\n" for name in STRATEGIES))
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

    def pod_events(self) -> list[str]:
        """The events of the pods in order: the old pod of a deployment exiting and the new one starting."""
        return (self.stand_in / "pods.log").read_text().splitlines() if (self.stand_in / "pods.log").exists() else []

    def configmap(self) -> str:
        return (self.stand_in / "configmap").read_text()

    def state(self) -> str:
        return (self.stand_in / "status").read_text()

    def record(self) -> Path:
        return self.directory / RECORD


def without_timeouts(log: list[str]) -> list[str]:
    """The calls of a run without the ``--request-timeout`` of each, which the cases that do not look at the time leave out."""
    return [re.sub(r" --request-timeout[= ]\S+", "", line) for line in log]


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
        self.assertEqual(["history airbyte", *SCALE_DOWN, "rollback airbyte 2", *UPGRADE_AND_RESTART], self.commands(log))
        self.assertIn(f"rollback airbyte 2 --namespace airbyte --wait --timeout {DEADLINE}s", log)

    def test_a_release_with_a_rollback_pending_is_rolled_back_to_the_last_deployed_revision_and_not_to_a_failed_one(self) -> None:
        past = history("superseded", "deployed", "failed", "pending-rollback")
        code, output, log = self.run_release("pending-rollback", past, recorded="other")
        self.assertEqual(0, code, output)
        self.assertEqual(["history airbyte", *SCALE_DOWN, "rollback airbyte 2", *UPGRADE_AND_RESTART], self.commands(log))

    def test_with_two_deployed_revisions_in_the_history_the_later_one_is_the_target(self) -> None:
        code, output, log = self.run_release("pending-upgrade", history("deployed", "deployed", "pending-upgrade"), recorded="other")
        self.assertEqual(0, code, output)
        self.assertEqual(["history airbyte", *SCALE_DOWN, "rollback airbyte 2", *UPGRADE_AND_RESTART], self.commands(log))

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
            ("pending-upgrade", history("deployed", "pending-upgrade"), ["history airbyte", *SCALE_DOWN, "rollback airbyte 1"], UPGRADE_AND_RESTART),
            (
                "pending-rollback",
                history("superseded", "deployed", "pending-rollback"),
                ["history airbyte", *SCALE_DOWN, "rollback airbyte 2"],
                UPGRADE_AND_RESTART,
            ),
            ("pending-install", history("pending-install"), ["uninstall airbyte"], [UPGRADE]),
            ("uninstalling", history("superseded", "uninstalling"), ["uninstall airbyte"], [UPGRADE]),
        ):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as scratch:
                box = ReleaseSandbox(scratch, status, past, "match", {})
                (box.stand_in / "configmap").write_text(CONFIGMAP_B)
                code, output, log = box.run()
                self.assertEqual(0, code, output)
                self.assertEqual([*recovery, *restart], commands(log))
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
        self.assertEqual([*UPGRADE_AND_RESTART], self.commands(log))
        waits = [line for line in log if line.startswith("kubectl rollout status deployment.apps/airbyte-workload-launcher --namespace airbyte --timeout=")]
        self.assertEqual(1, len(waits), log)
        seconds = int(waits[0].rsplit("--timeout=", 1)[1].rstrip("s"))
        self.assertTrue(0 < seconds < DEADLINE, f"the second wait gets what is left of the one deadline, less a second: {seconds}")

    def test_a_release_whose_latest_revision_failed_is_upgraded_and_restarted(self) -> None:
        """Helm upgrades a release whose latest revision failed over an older deployed one, and its pods still run the old settings."""
        code, output, log = self.run_release("failed", history("deployed", "failed"), recorded="other")
        self.assertEqual(0, code, output)
        self.assertEqual([*UPGRADE_AND_RESTART], self.commands(log))

    def test_the_upgrade_is_post_rendered_with_the_release_and_the_deployments_that_hold_connections_and_no_patch_follows_it(self) -> None:
        """A patch after the upgrade comes too late: Helm reapplies the chart's rolling update (a surge of 1 for the workload API server) in the upgrade."""
        with tempfile.TemporaryDirectory() as scratch:
            box = ReleaseSandbox(scratch, "deployed", history("deployed"), "other", {"airbyte_release": "other"})
            code, output, log = box.run()
            script = box.directory / POST_RENDERER
        self.assertEqual(0, code, output)
        upgrade = next(line for line in log if line.startswith(UPGRADE + " "))
        self.assertIn(f"--post-renderer {script} --post-renderer-args other --post-renderer-args {','.join(RECREATE_DEPLOYMENTS)}", upgrade)
        self.assertEqual([], [line for line in log if line.startswith("kubectl patch")])

    def test_the_old_pod_of_each_deployment_that_holds_connections_exits_before_the_new_pod_starts_in_an_upgrade(self) -> None:
        """The stand-in applies the post-rendered manifests as the Deployment controller does: the pod of a Recreate deployment exits first, a rolling
        update starts the new pod first. The deployments that hold connections are all Recreate, and a deployment that the role leaves alone keeps what the
        chart states."""
        with tempfile.TemporaryDirectory() as scratch:
            box = ReleaseSandbox(scratch, "deployed", history("deployed"), "other", {})
            code, output, _ = box.run()
            events = box.pod_events()
        self.assertEqual(0, code, output)
        for name in RECREATE_DEPLOYMENTS:
            with self.subTest(name=name):
                self.assertLess(events.index(f"old exits airbyte-{name}"), events.index(f"new starts airbyte-{name}"), events)
        self.assertLess(
            events.index("new starts airbyte-manifest-server"), events.index("old exits airbyte-manifest-server"), "the role leaves this one to the chart"
        )

    def test_the_chart_manifests_without_the_post_renderer_start_the_new_pod_first_so_the_check_above_can_fail(self) -> None:
        """The same stand-in applied to the manifests that the chart renders: Temporal, the cron and the worker (no strategy) and the workload API server
        (a surge of 1) start the new pod beside the old one, which is the failure that the post-renderer removes."""
        with tempfile.TemporaryDirectory() as scratch:
            box = ReleaseSandbox(scratch, "deployed", history("deployed"), "other", {})
            result = run_command(
                str(ROOT / ".venv" / "bin" / "python"), [str(box.stand_in / "rollout.py"), str(box.stand_in / "manifests.yaml")], cwd=box.folder, timeout=30
            )
            events = box.pod_events()
        self.assertEqual(0, result.returncode, result.stderr)
        for name in RECREATE_DEPLOYMENTS:
            with self.subTest(name=name):
                self.assertLess(events.index(f"new starts airbyte-{name}"), events.index(f"old exits airbyte-{name}"), events)

    def test_a_post_renderer_that_fails_stops_the_upgrade_before_any_pod_changes_with_fixed_text_and_no_record(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            box = ReleaseSandbox(scratch, "deployed", history("deployed"), "other", {})
            (box.stand_in / "manifests.yaml").write_text(
                MANIFESTS.replace(
                    "spec:\n  replicas: 1\n  selector:\n    matchLabels:\n      airbyte: temporal",
                    "not_spec:\n  selector:\n    matchLabels:\n      airbyte: temporal",
                )
            )
            code, output, log = box.run()
            self.assertNotEqual(0, code)
            self.assertIn("could not install or upgrade", output)
            self.assertNotIn(LEAK, output)
            self.assertEqual([], box.pod_events())
            self.assertFalse(box.record().exists())
            self.assertNotIn("kubectl rollout restart deployment", commands(log))

    def test_a_release_with_an_upgrade_pending_first_loses_the_pods_of_each_deployment_that_holds_connections_so_the_old_manifests_cannot_surge(self) -> None:
        """Helm's rollback has no post-renderer and applies the manifests of an older revision, which the chart shaped: the old pod of each deployment is
        gone, by the deployment's own selector, before the rollback starts the new one."""
        for status, past in (("pending-upgrade", history("deployed", "pending-upgrade")), ("pending-rollback", history("deployed", "pending-rollback"))):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as scratch:
                box = ReleaseSandbox(scratch, status, past, "other", {})
                code, output, log = box.run(flags=("pods_linger",))
                events = box.pod_events()
                self.assertEqual(0, code, output)
                log = without_timeouts(log)
                scale = [line for line in log if line.startswith("kubectl scale deployment")]
                self.assertEqual([f"kubectl scale deployment airbyte-{name} --namespace airbyte --replicas=0" for name in RECREATE_DEPLOYMENTS], scale)
                reads = [line for line in log if line.startswith("kubectl get deployment airbyte-")]
                self.assertGreaterEqual(len(reads), 2 * len(RECREATE_DEPLOYMENTS), "one read of the selector and one of the state for each deployment")
                for name in RECREATE_DEPLOYMENTS:
                    self.assertLess(events.index(f"old exits airbyte-{name}"), events.index(f"new starts airbyte-{name}"), events)
                steps = commands(log)
                first = SCALE_DOWN[: 2 * len(RECREATE_DEPLOYMENTS)]
                polls = steps[1 + len(first) : steps.index("rollback airbyte 1")]
                self.assertEqual(["history airbyte", *first], steps[: 1 + len(first)])
                self.assertEqual(
                    {"kubectl get replicaset", "kubectl get pods", *(f"kubectl get deployment airbyte-{name}" for name in RECREATE_DEPLOYMENTS)}, set(polls)
                )
                self.assertGreater(polls.count("kubectl get pods"), len(RECREATE_DEPLOYMENTS), "a pod that lingers is polled again before the rollback")

    def test_the_rollback_is_not_run_while_a_pod_of_a_deployment_that_holds_connections_is_still_there_and_the_wait_ends_at_its_deadline(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            box = ReleaseSandbox(scratch, "pending-upgrade", history("deployed", "pending-upgrade"), "other", {"airbyte_scale_down_deadline_seconds": 3})
            started = time.monotonic()
            code, output, log = box.run(flags=("pods_stay",))
            elapsed = time.monotonic() - started
        self.assertNotEqual(0, code)
        self.assertLess(elapsed, 60, "the wait is bounded by its deadline")
        self.assertIn("still had pods", output)
        self.assertIn("3 seconds", output)
        self.assertNotIn(LEAK, output)
        self.assertEqual(
            [], [line for line in log if line.startswith("rollback") or line.startswith(UPGRADE)], "no Helm command changes the release while the pods stay"
        )

    def test_a_deployment_that_is_absent_is_skipped_by_the_scale_down_and_any_other_failure_stops_the_run_before_the_rollback_with_fixed_text(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            box = ReleaseSandbox(scratch, "pending-upgrade", history("deployed", "pending-upgrade"), "other", {})
            code, output, log = box.run(flags=("scale_missing",))
            self.assertEqual(0, code, output)
            self.assertEqual([], [line for line in log if line.startswith("kubectl scale")], "an absent deployment has no pod to take away")
            self.assertIn("rollback airbyte 1", commands(log))
        for flag in ("fail_scale", "fail_scale_read", "empty_selector"):
            with self.subTest(flag=flag), tempfile.TemporaryDirectory() as scratch:
                box = ReleaseSandbox(scratch, "pending-upgrade", history("deployed", "pending-upgrade"), "other", {})
                code, output, log = box.run(flags=(flag,))
                self.assertNotEqual(0, code)
                self.assertIn("could not take the pods", output)
                self.assertNotIn(LEAK, output)
                self.assertEqual([], [line for line in commands(log) if line.startswith("rollback") or line == UPGRADE])
                self.assertEqual(
                    [], [line for line in log if line.startswith("kubectl get pods")], "an empty selector would match every pod, so no pod is listed with it"
                )

    def pending_box(self, scratch: str, **changes: object) -> ReleaseSandbox:
        return ReleaseSandbox(scratch, "pending-upgrade", history("deployed", "pending-upgrade"), "other", changes)

    def assert_stopped_before_the_rollback(self, code: int, output: str, log: list[str], status: int) -> None:
        self.assertNotEqual(0, code)
        self.assertIn(f"exit status {status}", output)
        self.assertIn("could not take the pods", output)
        self.assertNotIn(LEAK, output)
        self.assertEqual([], [line for line in commands(log) if line.startswith("rollback") or line == UPGRADE], "no Helm command changes the release")

    def test_the_rollback_waits_until_the_controller_has_seen_the_scale_down_and_every_replicaset_wants_no_replica_although_no_pod_is_left(self) -> None:
        """``kubectl scale`` returns before the controller has reconciled: a ReplicaSet can still want a replica while no pod exists, and a new pod can
        start from it (Codex recheck 2, medium finding 1)."""
        for flag, counted in (("rs_lingers", "kubectl get replicaset"), ("observed_lags", "kubectl get deployment airbyte-")):
            with self.subTest(flag=flag), tempfile.TemporaryDirectory() as scratch:
                code, output, log = self.pending_box(scratch).run(flags=(flag,), seed={"clock": "1000"})
                self.assertEqual(0, code, output)
                polled = len([line for line in log if line.startswith(counted)])
                self.assertGreater(polled, len(RECREATE_DEPLOYMENTS) * (2 if flag == "observed_lags" else 1), "the state is read again before the rollback")
                self.assertLess(
                    max(index for index, line in enumerate(log) if line.startswith(counted)),
                    log.index("rollback airbyte 1 --namespace airbyte --wait --timeout 600s"),
                )
        for flag in ("rs_stays", "observed_stuck"):
            with self.subTest(flag=flag), tempfile.TemporaryDirectory() as scratch:
                box = self.pending_box(scratch, airbyte_scale_down_deadline_seconds=5)
                code, output, log = box.run(flags=(flag,), seed={"clock": "1000"})
                self.assert_stopped_before_the_rollback(code, output, log, 6)

    def test_every_selector_is_read_and_checked_before_the_first_deployment_is_scaled(self) -> None:
        """A failed or empty read of a later deployment left an earlier one at zero with the release still pending (Codex recheck 2, medium finding 2)."""
        for flag, status in (("fail_read_cron", 4), ("empty_selector_worker", 4)):
            with self.subTest(flag=flag), tempfile.TemporaryDirectory() as scratch:
                code, output, log = self.pending_box(scratch).run(flags=(flag,))
                self.assert_stopped_before_the_rollback(code, output, log, status)
                self.assertEqual([], [line for line in log if line.startswith("kubectl scale")], "nothing is scaled before every deployment was read")

    def test_a_scale_that_fails_after_other_deployments_were_scaled_puts_back_only_the_replicas_it_changed_after_it_read_the_live_state(self) -> None:
        """Temporal wants two replicas, the cron none and the others one. The scale of the worker (third) fails."""
        seed = {"spec_airbyte-temporal": "2", "spec_airbyte-cron": "0"}
        zero = [f"kubectl scale deployment airbyte-{name} --namespace airbyte --replicas=0" for name in ("temporal", "cron", "worker")]
        put_back = "kubectl scale deployment airbyte-{} --namespace airbyte --replicas={}".format
        cases: tuple[tuple[str, tuple[str, ...], dict[str, str], list[str]], ...] = (
            ("the failed scale did not take effect", ("fail_scale_worker",), {}, [*zero, put_back("temporal", 2)]),
            ("the failed scale took effect", ("fail_scale_worker", "fail_scale_applied"), {}, [*zero, put_back("temporal", 2), put_back("worker", 1)]),
            ("another writer changed temporal", ("fail_scale_worker",), {"after_fail_scale": 'echo 4 > "$STAND_IN/spec_airbyte-temporal"\n'}, zero),
        )
        for name, flags, extra, expected in cases:
            with self.subTest(case=name), tempfile.TemporaryDirectory() as scratch:
                code, output, log = self.pending_box(scratch).run(flags=flags, seed=seed | extra)
                self.assert_stopped_before_the_rollback(code, output, log, 3)
                log = without_timeouts(log)
                self.assertEqual(expected, [line for line in log if line.startswith("kubectl scale")], "the deployment that was not reached is never touched")
                for line in [line for line in log if line.startswith("kubectl scale") and "--replicas=0" not in line]:
                    target = line.split(" --namespace")[0].removeprefix("kubectl scale deployment ")
                    self.assertIn(
                        f"kubectl get deployment {target} --namespace airbyte --output",
                        " ".join(log[: log.index(line)]).replace("--request-timeout", ""),
                        "the live state is read first",
                    )
        with tempfile.TemporaryDirectory() as scratch:
            code, output, log = self.pending_box(scratch).run(flags=("fail_scale_worker", "fail_restore"), seed=seed)
            self.assert_stopped_before_the_rollback(code, output, log, 7)
            self.assertIn("could not be put back", output)

    def test_every_request_is_bounded_by_the_time_that_is_left_and_an_answer_after_the_deadline_is_not_accepted(self) -> None:
        """A one-second budget accepted an empty pod list that came after two seconds (Codex recheck 2, medium finding 3). The stand-in's clock moves only
        by ``sleep`` and by the answer time of the pod list, and a request with a shorter timeout fails after it."""
        with tempfile.TemporaryDirectory() as scratch:
            box = self.pending_box(scratch, airbyte_scale_down_deadline_seconds=1)
            code, output, log = box.run(seed={"clock": "1000", "delay_pods": "2"})
            self.assert_stopped_before_the_rollback(code, output, log, 5)
        with tempfile.TemporaryDirectory() as scratch:
            box = self.pending_box(scratch, airbyte_scale_down_deadline_seconds=1)
            code, output, log = box.run(seed={"clock": "1000", "delay_pods": "1", "delay_pods_only": f"airbyte-{RECREATE_DEPLOYMENTS[-1]}"})
            self.assert_stopped_before_the_rollback(code, output, log, 6)
        with tempfile.TemporaryDirectory() as scratch:
            box = self.pending_box(scratch, airbyte_scale_down_deadline_seconds=40)
            code, output, log = box.run(flags=("pods_linger",), seed={"clock": "1000", "delay_pods": "3"})
            self.assertEqual(0, code, output)
            asked = [line for line in log if re.match(r"kubectl (scale|get pods|get replicaset|get deployment airbyte-)", line)]
            budgets = [int(match.group(1)) for line in asked if (match := re.search(r"--request-timeout[= ](\d+)s", line))]
            self.assertEqual(len(asked), len(budgets), "every request of the scale-down names a timeout")
            self.assertEqual(sorted(budgets, reverse=True), budgets, "the timeout only shrinks")
            self.assertLessEqual(budgets[0], 40)
            self.assertLess(budgets[-1], budgets[0], "the timeout is the time that is left, not the whole deadline")
            self.assertGreaterEqual(budgets[-1], 1)

    def test_a_release_that_needs_no_rollback_scales_nothing_down(self) -> None:
        for status, past, recorded in (
            ("deployed", history("deployed"), "other"),
            ("failed", history("deployed", "failed"), "other"),
            (NOT_INSTALLED, None, "none"),
        ):
            with self.subTest(status=status or "not installed"):
                code, output, log = self.run_release(status, past, recorded=recorded)
                self.assertEqual(0, code, output)
                self.assertEqual([], [line for line in log if line.startswith("kubectl scale") or line.startswith("kubectl get pods")])
        for status, past in (("pending-install", history("pending-install")), ("uninstalling", history("superseded", "uninstalling"))):
            with self.subTest(status=status):
                code, output, log = self.run_release(status, past, recorded="none")
                self.assertEqual(0, code, output)
                self.assertEqual(
                    [], [line for line in log if line.startswith("kubectl scale")], "an uninstall removes the deployments, so no pod needs to go first"
                )

    def test_a_changed_post_renderer_or_deployment_list_is_a_changed_input_and_upgrades_the_release_once(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            box = ReleaseSandbox(scratch, "deployed", history("deployed"), "match", {})
            code, output, log = box.run()
            self.assertEqual((0, []), (code, log), output)
            (box.directory / POST_RENDERER).write_text((box.directory / POST_RENDERER).read_text() + "# a changed script\n")
            code, output, log = box.run()
            self.assertEqual(0, code, output)
            self.assertEqual(UPGRADE_AND_RESTART, commands(log))
        with tempfile.TemporaryDirectory() as scratch:
            box = ReleaseSandbox(scratch, "deployed", history("deployed"), "match", {})
            box.variables["airbyte_recreate_deployments"] = [*RECREATE_DEPLOYMENTS, "server"]
            code, output, log = box.run()
            self.assertEqual(0, code, output)
            self.assertEqual(UPGRADE_AND_RESTART, commands(log), "the list is part of the inputs")

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
            self.assertEqual([*UPGRADE_AND_RESTART], commands(log))
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
                self.assertEqual([UPGRADE] if status == NOT_INSTALLED else UPGRADE_AND_RESTART, self.commands(log))

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
            self.assertEqual(["history airbyte", *SCALE_DOWN, "rollback airbyte 1", *UPGRADE_AND_RESTART], commands(log))
            self.assertEqual(CONFIGMAP_A, box.configmap())
            self.assertEqual(f"{box.inputs}\n", box.record().read_text())

    def test_a_second_stop_after_a_recovery_stays_recoverable(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            box = ReleaseSandbox(scratch, "deployed", history("deployed"), "match", {})
            box.run(flags=("interrupt",), host_values=HOST_VALUES_B)
            code, _, log = box.run(flags=("interrupt",), host_values="host.values.yaml\n")
            self.assertNotEqual(0, code)
            self.assertEqual(["history airbyte", *SCALE_DOWN, "rollback airbyte 1", UPGRADE, "kubectl get pods"], commands(log))
            self.assertFalse(box.record().exists())
            code, output, log = box.run()
            self.assertEqual(0, code, output)
            self.assertEqual(["history airbyte", *SCALE_DOWN, "rollback airbyte 3", *UPGRADE_AND_RESTART], commands(log))
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
            "Take the pods of the deployments that hold database connections away before the rollback",
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

    def test_the_post_renderer_is_placed_beside_the_values_as_a_root_program_and_a_second_run_changes_nothing(self) -> None:
        """Helm runs it as the user of the run (root on the host), so it is placed with the role's owner and group and no write bit for anyone else."""
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            variables = values(folder) | {"airbyte_bridge_address": "10.42.5.1"}
            code, output = run_tasks(["place_files.yml"], variables)
            placed = folder / "airbyte" / POST_RENDERER
            mode = stat.S_IMODE(placed.stat().st_mode)
            text = placed.read_text()
            again_code, again = run_tasks(["place_files.yml"], variables)
        self.assertEqual(0, code, output)
        self.assertEqual((ROLE / "files" / POST_RENDERER).read_text(), text)
        self.assertEqual(0o755, mode)
        self.assertEqual(0, again_code, again)
        self.assertRegex(again, CHANGED_NONE)

    def test_the_release_step_runs_after_the_placement_and_both_name_the_same_script_path(self) -> None:
        main = (ROLE / "tasks" / "main.yml").read_text()
        self.assertLess(main.index("place_files.yml"), main.index("release.yml"))
        upgrade = next(task for task in role_tasks()["release.yml"] if "ansible.builtin.command" in task and "upgrade" in str(task["ansible.builtin.command"]))
        argv = [str(item) for item in upgrade["ansible.builtin.command"]["argv"]]
        self.assertEqual(f"{{{{ airbyte_directory }}}}/{POST_RENDERER}", argv[argv.index("--post-renderer") + 1])
        placed = next(
            task for task in role_tasks()["place_files.yml"] if "ansible.builtin.copy" in task and POST_RENDERER in str(task["ansible.builtin.copy"]["dest"])
        )
        self.assertEqual(f"{{{{ airbyte_directory }}}}/{POST_RENDERER}", placed["ansible.builtin.copy"]["dest"])
        self.assertEqual("0755", placed["ansible.builtin.copy"]["mode"])

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
    def test_the_role_is_added_after_the_compose_stack_in_the_playbook(self) -> None:
        playbook = load_yaml((ROOT / "ansible" / "host.yml").read_text())
        if not isinstance(playbook, list):
            self.fail("the playbook must be a list")
        roles = [str(role) for role in playbook[0]["roles"]]
        self.assertEqual("airbyte", roles[-1])
        self.assertLess(roles.index("compose_stack"), roles.index("airbyte"))


if __name__ == "__main__":
    unittest.main()
