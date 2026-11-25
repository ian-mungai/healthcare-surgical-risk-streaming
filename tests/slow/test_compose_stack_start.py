"""How the stack role starts the stack and pulls the images the project pushed, through the real ``ansible-playbook`` and a stand-in ``docker``.

The stand-in is a small script that logs each call and answers like Docker for the calls the role makes, so the cases prove the order and the form of the
commands and that the registry token reaches Docker through standard input only. Real containers and a real pull are the acceptance test's job.
"""

from __future__ import annotations

import base64
import contextlib
import functools
import grp
import json
import os
import pwd
import re
import signal
import stat
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Any

from scripts.project_config import ROOT
from scripts.stack.yaml_text import load_yaml
from tests.slow.ansible_support import ROLES, load_variable, run_playbook

ROLE = ROLES / "compose_stack"
CHANGED_NONE = re.compile(r"changed=0\s")
SOURCE_PATH = r"[A-Za-z0-9_][A-Za-z0-9_.-]*(/[A-Za-z0-9_][A-Za-z0-9_.-]*)*"

STAND_IN = r"""#!/bin/sh
here="$(dirname "$0")"
printf '%s\n' "$*" >> "$here/calls.log"
store="${DOCKER_CONFIG:-$here/default_config}"
printf '%s|%s\n' "${DOCKER_CONFIG-unset}" "$(ls -ld "${DOCKER_CONFIG:-/nonexistent}" 2>/dev/null | cut -c1-10)" >> "$here/config.log"
printf '%s: %s\n' "$1" "$(env | cut -d= -f1 | grep -E '^(IMAGE_|DOCKER_AUTH_CONFIG$)' | sort | tr '\n' ' ')" >> "$here/names.log"
env >> "$here/env.log"
for word in "$@"; do
  case "$word" in
    inspect) verb=inspect ;;
    up) verb=up ;;
    ps) verb=ps ;;
    login) verb=login ;;
    logout) verb=logout ;;
    pull) verb=pull ;;
  esac
done
case " $* " in *" config "*) verb=config ;; esac
case " $* " in *" --services "*) verb=config_services ;; esac
case " $* " in *" --format json "*) case "$verb" in ps) verb=ps_json ;; esac ;; esac
case "$verb" in
  config_services) cat "$here/services.txt" ;;
  config) if [ -f "$here/config.json" ]; then cat "$here/config.json"; else echo '{"services": {}}'; fi ;;
  ps_json) if [ -f "$here/ps.json" ]; then cat "$here/ps.json"; fi ;;
  login|logout) echo "the role must not run docker $verb" >&2; exit 1 ;;
  pull)
    mine=$(ps -o pgid= -p $$ | tr -d ' ')
    parents=$(ps -o pgid= -p "$PPID" | tr -d ' ')
    ps -ww -eo pgid=,args= | awk -v a="$mine" -v b="$parents" '$1 == a || $1 == b { sub(/^ *[0-9]+ +/, ""); print }' >> "$here/ps.log"
    if [ -f "$here/fail_pull" ]; then echo "pull failed" >&2; exit 1; fi
    if [ ! -f "$store/config.json" ]; then echo "pull denied: not logged in" >&2; exit 1; fi
    cp "$store/config.json" "$here/pull_config.json"
    ls -l "$store/config.json" | cut -c1-10 >> "$here/pull_file_mode.log"
    if [ -f "$here/ignore_term_pull" ]; then
      trap 'echo term >> "$here/term.log"' TERM
      echo $$ > "$here/pull.pid"
      echo "$PPID" > "$here/shell.pid"
      if [ -f "$here/kill_shell" ]; then kill -s KILL "$PPID"; fi
      while :; do
        [ -d "$DOCKER_CONFIG" ] || touch "$here/folder_gone_early"
        sleep 1
      done
    fi
    if [ -f "$here/hang_pull" ]; then exec sleep 30; fi ;;
  up)
    touch "$here/started"
    case " $* " in *" --force-recreate "*) sed -i.bak 's/ .*$/ 2099-01-01T00:00:00Z/' "$here/inspect.txt"; rm -f "$here/inspect.txt.bak" ;; esac ;;
  ps) if [ -f "$here/started" ]; then echo abc123; fi ;;
  inspect) if [ -f "$here/inspect.txt" ]; then cat "$here/inspect.txt"; else echo "abc123 2026-01-01T00:00:00Z"; fi ;;
esac
"""


def prepare(folder: Path) -> Path:
    """The stand-in ``docker`` in ``folder``."""
    bin_folder = folder / "bin"
    bin_folder.mkdir()
    stand_in = bin_folder / "docker"
    stand_in.write_text(STAND_IN)
    stand_in.chmod(0o755)
    return stand_in


def calls(stand_in: Path) -> list[str]:
    log = stand_in.parent / "calls.log"
    return log.read_text().splitlines() if log.exists() else []


def run_tasks(task_file: str, values: dict[str, object], environment: dict[str, str] | None = None) -> tuple[int, str]:
    """Run one task file of the role. ``environment`` is the environment that every task of the play inherits, like a host's own."""
    with tempfile.TemporaryDirectory() as scratch:
        work = Path(scratch)
        playbook = work / "check.yml"
        include = f"    - ansible.builtin.include_role:\n        name: {ROLE}\n        tasks_from: {task_file}\n"
        inherited = "".join(
            f"  environment:\n    {name}: {json.dumps(value)}\n" if index == 0 else f"    {name}: {json.dumps(value)}\n"
            for index, (name, value) in enumerate((environment or {}).items())
        )
        playbook.write_text(f"- hosts: localhost\n  connection: local\n  gather_facts: false\n{inherited}  tasks:\n{include}")
        result = run_playbook(work, playbook, ["-v", "-i", "localhost,", "--extra-vars", json.dumps(values)])
    return result.returncode, result.stdout + result.stderr


@functools.cache
def unset_prefix() -> tuple[str, ...]:
    """The words that the role's Compose command starts with, as the group variables define them: ``env`` and one ``-u`` for each project image setting."""
    found: Any = load_variable(ROOT / "ansible" / "group_vars" / "all.yml", "compose_stack_environment_unset")
    return tuple(found)


def variables(folder: Path, stand_in: Path, **more: object) -> dict[str, object]:
    return {
        "compose_stack_directory": str(folder / "platform"),
        "compose_stack_registry_config_directory": str(folder / "platform" / "registry_docker"),
        "compose_stack_owner": pwd.getpwuid(os.getuid()).pw_name,
        "compose_stack_group": grp.getgrgid(os.getgid()).gr_name,
        "compose_stack_docker_command": str(stand_in),
        "compose_stack_docker": [*unset_prefix(), str(stand_in), "compose", "--project-name", "sample"],
        "compose_stack_pull_deadline_seconds": 60,
        "compose_stack_start_deadline_seconds": 60,
        "compose_stack_health_deadline_seconds": 30,
        **more,
    }


class StartTest(unittest.TestCase):
    def start(self, services: list[str]) -> tuple[list[str], int, str]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in = prepare(folder)
            code, output = run_tasks("start.yml", variables(folder, stand_in, compose_stack_start_services=services))
            return calls(stand_in), code, output

    def test_the_start_names_the_services_it_is_given_and_waits_for_their_health_with_the_deadline(self) -> None:
        logged, code, output = self.start(["postgres"])
        self.assertEqual(0, code, output)
        ups = [line for line in logged if " up " in f" {line} "]
        self.assertEqual(["compose --project-name sample up --detach --wait --wait-timeout 30 postgres"], ups)

    def test_a_start_with_no_service_named_starts_every_service(self) -> None:
        logged, code, output = self.start([])
        self.assertEqual(0, code, output)
        self.assertEqual(["compose --project-name sample up --detach --wait --wait-timeout 30"], [line for line in logged if " up " in f" {line} "])

    def start_except(
        self, left_out: list[str], names: str = "postgres\nnessie\ndagster_code_location\ndagster_daemon\ndagster_webserver\n"
    ) -> tuple[list[str], int, str]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in = prepare(folder)
            (stand_in.parent / "services.txt").write_text(names)
            values = variables(folder, stand_in, compose_stack_start_services=[], compose_stack_start_except=left_out)
            code, output = run_tasks("start.yml", values)
            return calls(stand_in), code, output

    def test_an_empty_start_with_services_left_out_starts_every_other_service_by_name(self) -> None:
        logged, code, output = self.start_except(["dagster_code_location", "dagster_daemon", "dagster_webserver"])
        self.assertEqual(0, code, output)
        self.assertEqual(
            ["compose --project-name sample up --detach --wait --wait-timeout 30 nessie postgres"], [line for line in logged if " up " in f" {line} "]
        )
        self.assertIn("compose --project-name sample config --services", logged)

    def test_nothing_is_left_out_when_the_list_is_empty_and_the_service_names_are_not_read(self) -> None:
        logged, code, output = self.start_except([])
        self.assertEqual(0, code, output)
        self.assertEqual(["compose --project-name sample up --detach --wait --wait-timeout 30"], [line for line in logged if " up " in f" {line} "])
        self.assertNotIn("compose --project-name sample config --services", logged)

    def test_a_start_that_names_services_ignores_the_ones_left_out(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in = prepare(folder)
            values = variables(folder, stand_in, compose_stack_start_services=["postgres"], compose_stack_start_except=["postgres"])
            code, output = run_tasks("start.yml", values)
            self.assertEqual(0, code, output)
            self.assertEqual(
                ["compose --project-name sample up --detach --wait --wait-timeout 30 postgres"], [line for line in calls(stand_in) if " up " in f" {line} "]
            )

    def test_the_containers_are_read_through_the_same_docker_command_as_the_start(self) -> None:
        logged, code, output = self.start(["postgres"])
        self.assertEqual(0, code, output)
        self.assertTrue(any(line.startswith("inspect --format") for line in logged), logged)

    def test_the_second_start_of_the_same_containers_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in = prepare(folder)
            values = variables(folder, stand_in, compose_stack_start_services=[])
            first, _ = run_tasks("start.yml", values)
            code, output = run_tasks("start.yml", values)
        self.assertEqual((0, 0), (first, code), output)
        self.assertRegex(output, CHANGED_NONE)


class RecreateTest(unittest.TestCase):
    """A service whose placed configuration file is newer than its container is recreated, alone, and a second run changes nothing."""

    def setup(self, folder: Path, *, started: dict[str, str]) -> tuple[Path, Path, Path]:
        stand_in = prepare(folder)
        files = {"alloy": folder / "alloy.alloy", "grafana": folder / "grafana.yml"}
        for path in files.values():
            path.write_text("x")
            os.utime(path, (1_700_000_000, 1_700_000_000))
        config = {
            "services": {"alloy": {"configs": [{"source": "alloy_config"}]}, "grafana": {"configs": ["grafana_config"]}, "nessie": {}},
            "configs": {"alloy_config": {"file": str(files["alloy"])}, "grafana_config": {"file": str(files["grafana"])}},
        }
        (stand_in.parent / "config.json").write_text(json.dumps(config))
        (stand_in.parent / "ps.json").write_text("".join(json.dumps({"Service": name, "ID": f"id_{name}"}) + "\n" for name in started))
        (stand_in.parent / "inspect.txt").write_text("".join(f"id_{name} {moment}\n" for name, moment in started.items()))
        (stand_in.parent / "started").write_text("")
        return stand_in, files["alloy"], files["grafana"]

    def recreations(self, stand_in: Path) -> list[str]:
        return [line for line in calls(stand_in) if "--force-recreate" in line]

    def start(self, folder: Path, stand_in: Path, services: list[str]) -> tuple[int, str]:
        return run_tasks("start.yml", variables(folder, stand_in, compose_stack_start_services=services))

    def test_only_the_service_whose_file_is_newer_than_its_container_is_recreated_without_its_dependencies(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, alloy_file, _ = self.setup(folder, started={"alloy": "2025-01-01T00:00:00Z", "grafana": "2025-01-01T00:00:00Z"})
            os.utime(alloy_file, (1_800_000_000, 1_800_000_000))
            code, output = self.start(folder, stand_in, [])
            forced = self.recreations(stand_in)
        self.assertEqual(0, code, output)
        self.assertEqual(["compose --project-name sample up --detach --wait --wait-timeout 30 --force-recreate --no-deps alloy"], forced)

    def test_a_second_run_recreates_nothing_and_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, alloy_file, _ = self.setup(folder, started={"alloy": "2025-01-01T00:00:00Z", "grafana": "2025-01-01T00:00:00Z"})
            os.utime(alloy_file, (1_800_000_000, 1_800_000_000))
            self.start(folder, stand_in, [])
            code, output = self.start(folder, stand_in, [])
            forced = self.recreations(stand_in)
        self.assertEqual(0, code, output)
        self.assertEqual(1, len(forced), forced)
        self.assertRegex(output, CHANGED_NONE)

    def test_a_stack_whose_files_are_older_than_its_containers_recreates_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, _, _ = self.setup(folder, started={"alloy": "2025-01-01T00:00:00Z", "grafana": "2025-01-01T00:00:00Z"})
            code, output = self.start(folder, stand_in, [])
            forced = self.recreations(stand_in)
        self.assertEqual(0, code, output)
        self.assertEqual([], forced)

    def test_the_first_start_of_postgresql_alone_leaves_a_stale_service_of_the_second_start(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, alloy_file, _ = self.setup(folder, started={"alloy": "2025-01-01T00:00:00Z", "grafana": "2025-01-01T00:00:00Z"})
            os.utime(alloy_file, (1_800_000_000, 1_800_000_000))
            code, output = self.start(folder, stand_in, ["postgres"])
            forced = self.recreations(stand_in)
        self.assertEqual(0, code, output)
        self.assertEqual([], forced)

    def test_a_service_that_is_named_in_the_start_and_stale_is_recreated(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, alloy_file, grafana_file = self.setup(folder, started={"alloy": "2025-01-01T00:00:00Z", "grafana": "2025-01-01T00:00:00Z"})
            os.utime(alloy_file, (1_800_000_000, 1_800_000_000))
            os.utime(grafana_file, (1_800_000_000, 1_800_000_000))
            code, output = self.start(folder, stand_in, ["grafana"])
            forced = self.recreations(stand_in)
        self.assertEqual(0, code, output)
        self.assertEqual(["compose --project-name sample up --detach --wait --wait-timeout 30 --force-recreate --no-deps grafana"], forced)

    def test_every_compose_command_of_the_start_runs_with_no_project_image_setting_even_when_the_host_environment_holds_stale_ones(self) -> None:
        planted = {name: f"stale/{name.lower()}@sha256:{'a' * 64}" for name in ("IMAGE_DAGSTER", "IMAGE_REPLAY", "IMAGE_SPARK")}
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, alloy_file, _ = self.setup(folder, started={"alloy": "2025-01-01T00:00:00Z", "grafana": "2025-01-01T00:00:00Z"})
            os.utime(alloy_file, (1_800_000_000, 1_800_000_000))
            code, output = run_tasks("start.yml", variables(folder, stand_in, compose_stack_start_services=[]), environment=planted)
            names = (stand_in.parent / "names.log").read_text().splitlines()
            forced = self.recreations(stand_in)
        self.assertEqual(0, code, output)
        self.assertEqual(1, len(forced))
        compose = [line for line in names if line.startswith("compose:")]
        self.assertGreaterEqual(len(compose), 5, "config, ps, the start, the recreation and the second ps")
        self.assertEqual({"compose: "}, set(compose))
        self.assertIn("inspect: IMAGE_DAGSTER IMAGE_REPLAY IMAGE_SPARK ", names, "the stale settings did reach a command that is not a Compose command")


class CommandVariableTest(unittest.TestCase):
    GROUP_VARIABLES = ROOT / "ansible" / "group_vars" / "all.yml"

    def test_the_compose_command_removes_exactly_the_project_image_settings_with_env_and_the_docker_command_is_its_own_variable(self) -> None:
        from scripts.stack.groups import PROJECT_IMAGE_SETTINGS

        prefix = list(unset_prefix())
        self.assertEqual("env", prefix[0])
        self.assertEqual(["-u"] * len(PROJECT_IMAGE_SETTINGS), prefix[1::2])
        self.assertEqual(sorted(PROJECT_IMAGE_SETTINGS), sorted(prefix[2::2]))
        command: Any = load_variable(self.GROUP_VARIABLES, "compose_stack_docker")
        self.assertEqual([*prefix, "docker", "compose"], command[: len(prefix) + 2])
        self.assertEqual("docker", load_variable(self.GROUP_VARIABLES, "compose_stack_docker_command"))

    def test_no_task_builds_a_docker_command_of_its_own_and_no_module_takes_a_word_of_the_compose_command_as_the_docker_command(self) -> None:
        for path in sorted(ROLE.glob("tasks/*.yml")):
            code = "\n".join(line for line in path.read_text().splitlines() if not line.lstrip().startswith("#"))
            with self.subTest(path=path.name):
                self.assertNotRegex(code, r"docker compose|'compose'|\"compose\"")
                self.assertNotIn("compose_stack_docker[0]", code)
        for path in sorted(ROLE.glob("library/*.py")):
            with self.subTest(path=path.name):
                self.assertNotIn("command[0]", path.read_text())


class SecretRecreateTest(unittest.TestCase):
    """A service whose secret file was replaced after its container started is recreated alone by the Compose command; only a named service is read."""

    RECREATE = "compose --project-name sample up --detach --wait --wait-timeout 30 --force-recreate --no-deps dagster_code_location"

    def setup(self, folder: Path, *, secret_age: int | None) -> tuple[Path, Path]:
        """The stand-in with two containers that started in 2025, and a secret file of the code location that is as old as ``secret_age`` or absent."""
        stand_in = prepare(folder)
        secret = folder / "client_id"
        if secret_age is not None:
            secret.write_text("x")
            os.utime(secret, (secret_age, secret_age))
        config = {
            "services": {"postgres": {}, "dagster_code_location": {"secrets": [{"source": "airbyte_client_id"}]}},
            "secrets": {"airbyte_client_id": {"file": str(secret)}},
        }
        started = {"postgres": "2025-01-01T00:00:00Z", "dagster_code_location": "2025-01-01T00:00:00Z"}
        (stand_in.parent / "config.json").write_text(json.dumps(config))
        (stand_in.parent / "ps.json").write_text("".join(json.dumps({"Service": name, "ID": f"id_{name}"}) + "\n" for name in started))
        (stand_in.parent / "inspect.txt").write_text("".join(f"id_{name} {moment}\n" for name, moment in started.items()))
        (stand_in.parent / "started").write_text("")
        return stand_in, secret

    def start(self, folder: Path, stand_in: Path, services: list[str]) -> tuple[int, str]:
        return run_tasks("start.yml", variables(folder, stand_in, compose_stack_start_services=services))

    def recreations(self, stand_in: Path) -> list[str]:
        return [line for line in calls(stand_in) if "--force-recreate" in line]

    def test_a_secret_file_that_was_replaced_after_the_start_is_recreated_by_the_compose_command_and_a_second_run_recreates_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, _ = self.setup(folder, secret_age=1_800_000_000)
            code, output = self.start(folder, stand_in, ["dagster_code_location"])
            self.assertEqual(0, code, output)
            self.assertEqual([self.RECREATE], self.recreations(stand_in))
            code, output = self.start(folder, stand_in, ["dagster_code_location"])
            self.assertEqual((0, 1), (code, len(self.recreations(stand_in))), output)
            self.assertRegex(output, CHANGED_NONE)

    def test_a_secret_file_older_than_its_container_recreates_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, _ = self.setup(folder, secret_age=1_700_000_000)
            code, output = self.start(folder, stand_in, ["dagster_code_location"])
        self.assertEqual(0, code, output)
        self.assertEqual([], self.recreations(stand_in))

    def test_a_start_that_does_not_name_the_service_does_not_look_for_its_secret_file_that_is_not_there_yet(self) -> None:
        """The in-place redeploy: the PostgreSQL start comes before the step that writes a copy that is missing, and it must reach that step."""
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, _ = self.setup(folder, secret_age=None)
            code, output = self.start(folder, stand_in, ["postgres"])
        self.assertEqual(0, code, output)
        self.assertEqual([], self.recreations(stand_in))

    def test_a_start_that_names_the_service_refuses_its_missing_secret_file_by_name(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, secret = self.setup(folder, secret_age=None)
            code, output = self.start(folder, stand_in, ["dagster_code_location"])
        self.assertNotEqual(0, code)
        self.assertIn(f"service dagster_code_location mounts a file that is not there: {secret}", output)


class PullTest(unittest.TestCase):
    TOKEN = "dckr_pat_example_token_value"  # noqa: S105 - a stand-in value that proves the token never reaches a command line, an environment or the output
    USER = "example_account"
    SHAPE = "login.[A-Za-z0-9]+"

    def prepare_pull(self, folder: Path, *, mode: int | None = 0o400, content: str = TOKEN + "\n") -> tuple[Path, dict[str, object]]:
        stand_in = prepare(folder)
        (folder / "platform").mkdir()
        token_file = folder / "registry_token"
        if mode is not None:
            token_file.write_text(content)
            token_file.chmod(mode)
        values = variables(folder, stand_in, compose_stack_registry_user=self.USER, compose_stack_registry_token_file=str(token_file))
        return stand_in, values

    def parent(self, folder: Path) -> Path:
        """The parent that the role's own login folders are made in (the variable's value)."""
        return folder / "platform" / "registry_docker"

    def plant_old_folder(self, folder: Path) -> dict[str, str]:
        """A folder at the old path of the configuration folder with a login in it and a file beside it: the role must change nothing in it."""
        parent = self.parent(folder)
        (parent / "keep").mkdir(parents=True)
        (parent / "config.json").write_text("old-login\n")
        (parent / "keep" / "note.txt").write_text("note\n")
        parent.chmod(0o700)
        return self.snapshot(parent)

    def snapshot(self, parent: Path) -> dict[str, str]:
        return {str(path.relative_to(parent)): path.read_text() if path.is_file() else "<dir>" for path in sorted(parent.rglob("*"))}

    def plant_existing_login(self, stand_in: Path) -> Path:
        """A login of the host's own Docker client, which the role must never read, replace or remove."""
        existing = stand_in.parent / "default_config" / "config.json"
        existing.parent.mkdir()
        existing.write_text("existing-login\n")
        return existing

    def configurations(self, stand_in: Path) -> list[tuple[str, str]]:
        """The ``DOCKER_CONFIG`` value and the folder mode that each docker call saw, in the order of the calls."""
        log = stand_in.parent / "config.log"
        return [tuple(line.split("|")) for line in log.read_text().splitlines()] if log.exists() else []  # type: ignore[misc]

    def helper_path(self, folder: Path) -> str:
        helpers = folder / "helpers"
        helpers.mkdir()
        (helpers / "docker-credential-fake").write_text("#!/bin/sh\n")
        (helpers / "docker-credential-fake").chmod(0o755)
        return f"/usr/bin:/bin:{helpers}"

    def test_the_pull_runs_once_in_a_fresh_folder_under_the_parent_with_no_login_and_no_logout_and_nothing_is_left(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, values = self.prepare_pull(folder)
            code, output = run_tasks("pull_images.yml", values)
            logged = calls(stand_in)
            seen = self.configurations(stand_in)
            parent = self.parent(folder)
            listing = os.listdir(parent)
            parent_mode = stat.S_IMODE(parent.stat().st_mode)
            used = json.loads((stand_in.parent / "pull_config.json").read_text())
            file_mode = (stand_in.parent / "pull_file_mode.log").read_text().strip()
            running = self.running_from(scratch)
        self.assertEqual(0, code, output)
        self.assertEqual([], running, "the task left a process of its own: the pull, the shell or its watchdog")
        self.assertEqual(["compose --project-name sample --profile batch pull --quiet"], logged)
        self.assertEqual(1, len(seen))
        used_folder, mode = seen[0]
        self.assertRegex(used_folder, re.escape(str(parent)) + "/" + self.SHAPE + "$")
        self.assertEqual("drwx------", mode)
        self.assertEqual("-rw-------", file_mode)
        auth = base64.b64encode(f"{self.USER}:{self.TOKEN}".encode()).decode()
        self.assertEqual({"auths": {"https://index.docker.io/v1/": {"auth": auth}}}, used, "one auth entry for Docker Hub and nothing else")
        self.assertEqual([], listing, "the folder is removed and the parent stays")
        self.assertEqual(0o700, parent_mode)

    def test_the_token_is_on_no_command_line_in_no_environment_and_in_no_output(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, values = self.prepare_pull(folder)
            code, output = run_tasks("pull_images.yml", values)
            seen = {name: (stand_in.parent / name).read_text() for name in ("calls.log", "env.log", "ps.log", "names.log")}
        self.assertEqual(0, code, output)
        auth = base64.b64encode(f"{self.USER}:{self.TOKEN}".encode()).decode()
        for name, text in {**seen, "output": output}.items():
            self.assertNotIn(self.TOKEN, text, name)
            self.assertNotIn(auth, text, name)
        self.assertIn("compose --project-name sample --profile batch pull --quiet", seen["ps.log"], "the process list was read while the pull ran")

    def test_the_pull_sees_neither_a_docker_auth_setting_nor_a_project_image_setting_of_the_hosts_environment(self) -> None:
        planted = {"DOCKER_AUTH_CONFIG": '{"auths": {}}', "IMAGE_SPARK": "stale/x@sha256:" + "a" * 64, "IMAGE_DAGSTER": "stale/y", "IMAGE_REPLAY": "stale/z"}
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, values = self.prepare_pull(folder)
            code, output = run_tasks("pull_images.yml", values, environment=planted)
            names = (stand_in.parent / "names.log").read_text().splitlines()
        self.assertEqual(0, code, output)
        self.assertEqual(["compose: "], names)

    def test_a_docker_credential_helper_on_the_path_does_not_stop_the_pull_because_the_one_entry_file_gives_it_no_helper_to_use(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, values = self.prepare_pull(folder)
            code, output = run_tasks("pull_images.yml", values, environment={"PATH": self.helper_path(folder)})
            logged = calls(stand_in)
            used = json.loads((stand_in.parent / "pull_config.json").read_text())
        self.assertEqual(0, code, output)
        self.assertEqual(["compose --project-name sample --profile batch pull --quiet"], logged)
        self.assertEqual(["auths"], list(used))
        self.assertEqual(1, len(used["auths"]))

    def test_a_login_file_that_is_not_exactly_one_auth_entry_with_no_credential_store_or_helper_key_is_refused_before_the_pull(self) -> None:
        """The auth value comes from ``base64`` on the PATH. A program there that adds a key of the file's own shape must not reach Docker."""
        shims = {
            "credsStore": 'eA==","credsStore":"desktop',
            "credHelpers": 'eA=="},"credHelpers":{"index.docker.io":"fake',
            "second entry": 'eA=="},"https://other.example":{"auth":"eA==',
            "empty": "",
            "space": "eA== ",
        }
        for label, value in shims.items():
            with self.subTest(case=label), tempfile.TemporaryDirectory() as scratch:
                folder = Path(scratch)
                stand_in, values = self.prepare_pull(folder)
                helpers = folder / "helpers"
                helpers.mkdir()
                shim = helpers / "base64"
                shim.write_text(f"#!/bin/sh\ncat > /dev/null\nprintf '%s' '{value}'\n")
                shim.chmod(0o755)
                code, output = run_tasks("pull_images.yml", values, environment={"PATH": f"{helpers}:/usr/bin:/bin"})
                left = os.listdir(self.parent(folder))
            self.assertNotEqual(0, code, output)
            self.assertIn("the pull's login file is not a file with exactly one auth entry and no credsStore or credHelpers key", output)
            self.assertEqual([], calls(stand_in), "no Docker command ran with that file")
            self.assertEqual([], left)
            self.assertNotIn(self.TOKEN, output)

    def test_the_login_file_is_checked_as_raw_bytes_so_a_nul_or_other_stray_byte_is_refused_before_the_pull(self) -> None:
        """A program on the PATH that runs right after the file is written (``chmod``) changes its bytes. A shell read that drops NUL would not see it."""
        head = '{"auths": {"https://index.docker.io/v1/": {"auth": "'
        changes = {
            "nul at the end": "printf '\\000' >> \"$2\"",
            "nul inside": f'printf \'{head}eA\\000=="}}}}\\n\' > "$2"',
            "non-ascii byte": f'printf \'{head}eA\\303\\251="}}}}\\n\' > "$2"',
            "second line": "printf '{}\\n' >> \"$2\"",
            "carriage return": "printf '\\r' >> \"$2\"",
            "no final newline": 'printf \'%s\' "$(cat "$2")" > "$2"',
        }
        for label, change in changes.items():
            with self.subTest(case=label), tempfile.TemporaryDirectory() as scratch:
                folder = Path(scratch)
                stand_in, values = self.prepare_pull(folder)
                helpers = folder / "helpers"
                helpers.mkdir()
                shim = helpers / "chmod"
                shim.write_text(f'#!/bin/sh\n{change}\nexec /bin/chmod "$@"\n')
                shim.chmod(0o755)
                code, output = run_tasks("pull_images.yml", values, environment={"PATH": f"{helpers}:/usr/bin:/bin"})
                left = os.listdir(self.parent(folder))
            self.assertNotEqual(0, code, output)
            self.assertIn("the pull's login file is not a file with exactly one auth entry and no credsStore or credHelpers key", output)
            self.assertEqual([], calls(stand_in), "no Docker command ran with that file")
            self.assertEqual([], left)

    def test_the_task_file_has_no_refusal_for_a_credential_helper_on_the_path(self) -> None:
        text = (ROLE / "tasks" / "pull_images.yml").read_text()
        code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
        self.assertNotIn("docker-credential", code)

    def test_an_existing_folder_at_the_old_path_and_an_existing_docker_login_are_untouched_by_every_outcome(self) -> None:
        outcomes: dict[str, dict[str, object]] = {
            "success": {},
            "blank_token": {"content": "  \n"},
            "loose_token_file": {"mode": 0o600},
            "failed_pull": {"marker": "fail_pull"},
            "helper": {"helper": True},
        }
        for label, more in outcomes.items():
            with self.subTest(outcome=label), tempfile.TemporaryDirectory() as scratch:
                folder = Path(scratch)
                prepared = {key: more[key] for key in ("content", "mode") if key in more}
                stand_in, values = self.prepare_pull(folder, **prepared)  # type: ignore[arg-type]
                before = self.plant_old_folder(folder)
                existing = self.plant_existing_login(stand_in)
                if "marker" in more:
                    (stand_in.parent / str(more["marker"])).write_text("")
                environment = {"PATH": self.helper_path(folder)} if more.get("helper") else None
                run_tasks("pull_images.yml", values, environment=environment)
                self.assertEqual(before, self.snapshot(self.parent(folder)), "the old folder holds what it held, and no login folder is left in it")
                self.assertEqual(0o700, stat.S_IMODE(self.parent(folder).stat().st_mode))
                self.assertEqual("existing-login\n", existing.read_text())
                self.assertEqual(["config.json"], os.listdir(existing.parent))
                self.assertNotIn("unset", [configuration for configuration, _ in self.configurations(stand_in)])

    def test_a_parent_that_is_not_a_root_only_folder_is_refused_and_left_as_it_is(self) -> None:
        for label in ("open_mode", "symbolic_link", "plain_file"):
            with self.subTest(parent=label), tempfile.TemporaryDirectory() as scratch:
                folder = Path(scratch)
                stand_in, values = self.prepare_pull(folder)
                parent = self.parent(folder)
                if label == "open_mode":
                    parent.mkdir()
                    parent.chmod(0o755)
                elif label == "symbolic_link":
                    (folder / "elsewhere").mkdir(mode=0o700)
                    parent.symlink_to(folder / "elsewhere")
                else:
                    parent.write_text("x")
                code, output = run_tasks("pull_images.yml", values)
                self.assertNotEqual(0, code)
                self.assertIn("is not a directory with mode 0700", output)
                self.assertEqual([], calls(stand_in))
                if label == "open_mode":
                    self.assertEqual(0o755, stat.S_IMODE(parent.stat().st_mode), "the folder's mode is not changed")
                if label == "symbolic_link":
                    self.assertEqual([], os.listdir(folder / "elsewhere"))

    def test_a_pull_that_fails_stops_the_deploy_with_a_fixed_message_and_removes_its_folder(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, values = self.prepare_pull(folder)
            (stand_in.parent / "fail_pull").write_text("")
            code, output = run_tasks("pull_images.yml", values)
            left = os.listdir(self.parent(folder))
        self.assertNotEqual(0, code, output)
        self.assertIn("the image pull failed with status 1", output)
        self.assertEqual([], left)
        self.assertNotIn(self.TOKEN, output)

    def pull_numbers(self) -> dict[str, int]:
        """The reserve, the TERM grace and the KILL wait that the pull task's block sets, read from the file."""
        tasks: Any = load_yaml((ROLE / "tasks" / "pull_images.yml").read_text())
        block = next(task for task in tasks if "block" in task)
        return {name: int(block["vars"][f"compose_stack_pull_{name}_seconds"]) for name in ("reserve", "grace", "reap")}

    def ignoring_run(self, folder: Path, deadline: int, *markers: str) -> tuple[Path, int, str]:
        """A run whose pull ignores TERM, with the given marker files, at an Ansible timeout of ``deadline`` seconds."""
        stand_in, values = self.prepare_pull(folder)
        for marker in ("ignore_term_pull", *markers):
            (stand_in.parent / marker).write_text("")
        code, output = run_tasks("pull_images.yml", values | {"compose_stack_pull_deadline_seconds": deadline})
        return stand_in, code, output

    @staticmethod
    def running_from(scratch: str) -> list[str]:
        """The processes whose command line holds the run's folder, after at most three seconds for the last one to go."""
        found: list[str] = []
        for _ in range(30):
            listing = subprocess.run(["/bin/ps", "-ww", "-axo", "args="], capture_output=True, text=True, check=False).stdout  # noqa: S603
            found = [line for line in listing.splitlines() if scratch in line and "ps -ww" not in line]
            if not found:
                break
            time.sleep(0.1)
        return found

    @staticmethod
    def terms(stand_in: Path) -> list[str]:
        """One line for each TERM that the pull received."""
        log = stand_in.parent / "term.log"
        return log.read_text().splitlines() if log.exists() else []

    def watch_pull(self, stand_in: Path) -> int:
        """The pull's pid, once the pull has started, and an end of the test that kills it if it is still there."""
        pid = int((stand_in.parent / "pull.pid").read_text())
        self.addCleanup(self.kill_quietly, pid)
        return pid

    def signal_shell(self, stand_in: Path, count: int, gap: float) -> threading.Thread:
        """Send TERM to the task's shell ``count`` times, ``gap`` seconds apart, once the pull has started, while the test waits for the run."""

        def send() -> None:
            marker = stand_in.parent / "shell.pid"
            for _ in range(300):
                if marker.exists() and marker.read_text().strip():
                    break
                time.sleep(0.1)
            pid = int(marker.read_text())
            for index in range(count):
                if index:
                    time.sleep(gap)
                os.kill(pid, signal.SIGTERM)

        thread = threading.Thread(target=send)
        thread.start()
        self.addCleanup(thread.join)
        return thread

    def test_a_pull_deadline_that_leaves_no_time_for_the_shells_own_end_is_refused_before_any_docker_call(self) -> None:
        reserve = self.pull_numbers()["reserve"]
        for deadline in (3, reserve):
            with self.subTest(deadline=deadline), tempfile.TemporaryDirectory() as scratch:
                folder = Path(scratch)
                stand_in, code, output = self.ignoring_run(folder, deadline)
                made = self.parent(folder).exists()
            self.assertNotEqual(0, code, output)
            self.assertIn(f"compose_stack_pull_deadline_seconds must be more than {reserve}", output)
            self.assertEqual([], calls(stand_in))
            self.assertFalse(made)
            self.assertNotIn("expected time frame", output, "Ansible's own timeout ended the task: the role's own end did not fit inside it")

    def test_a_pull_that_runs_past_its_deadline_is_ended_by_the_task_with_a_fixed_message_and_its_folder_is_removed(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, values = self.prepare_pull(folder)
            (stand_in.parent / "hang_pull").write_text("")
            deadline = self.pull_numbers()["reserve"] + 1
            code, output = run_tasks("pull_images.yml", values | {"compose_stack_pull_deadline_seconds": deadline})
            left = os.listdir(self.parent(folder))
        self.assertNotEqual(0, code, output)
        self.assertIn(f"the image pull ran past its deadline of {deadline} seconds", output)
        self.assertEqual([], left)
        self.assertNotIn(self.TOKEN, output)

    def test_a_pull_that_ignores_term_is_killed_with_its_group_before_the_folder_goes_and_the_deadline_is_named_inside_ansibles_timeout(self) -> None:
        deadline = self.pull_numbers()["reserve"] + 5
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, code, output = self.ignoring_run(folder, deadline)
            left = os.listdir(self.parent(folder))
            pid = self.watch_pull(stand_in)
            gone = self.is_gone(pid, tries=1)
            early = (stand_in.parent / "folder_gone_early").exists()
            terms = self.terms(stand_in)
        self.assertNotEqual(0, code, output)
        self.assertIn(f"the image pull ran past its deadline of {deadline} seconds", output)
        self.assertNotIn("expected time frame", output, "Ansible's own timeout ended the task before the role's own end")
        self.assertTrue(gone, "the pull was still there when the task returned: one TERM is not enough")
        self.assertFalse(early, "the folder was removed while the pull still ran")
        self.assertEqual([], left, "the folder was still there when the task returned")
        self.assertEqual(["term"], terms, "the group got TERM once and then KILL")
        self.assertNotIn(self.TOKEN, output)

    def test_a_pull_that_ends_on_the_term_signal_is_not_held_for_the_whole_grace(self) -> None:
        """A run that ends on TERM takes about the time of a run that ends by itself, plus the one second of the deadline, and not the grace on top."""
        numbers = self.pull_numbers()
        times = {}
        for marker in ("none", "hang_pull"):
            with tempfile.TemporaryDirectory() as scratch:
                folder = Path(scratch)
                stand_in, values = self.prepare_pull(folder)
                if marker != "none":
                    (stand_in.parent / marker).write_text("")
                started = time.monotonic()
                run_tasks("pull_images.yml", values | {"compose_stack_pull_deadline_seconds": numbers["reserve"] + 1})
                times[marker] = time.monotonic() - started
        self.assertLess(times["hang_pull"], times["none"] + numbers["grace"] - 2, str(times))

    def test_a_cancelled_task_keeps_the_login_folder_until_the_client_that_ignores_term_is_reaped(self) -> None:
        deadline = self.pull_numbers()["reserve"] + 20
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, values = self.prepare_pull(folder)
            (stand_in.parent / "ignore_term_pull").write_text("")
            self.signal_shell(stand_in, 1, 0)
            code, output = run_tasks("pull_images.yml", values | {"compose_stack_pull_deadline_seconds": deadline})
            left = os.listdir(self.parent(folder))
            pid = self.watch_pull(stand_in)
            gone = self.is_gone(pid, tries=1)
            early = (stand_in.parent / "folder_gone_early").exists()
            terms = self.terms(stand_in)
        self.assertNotEqual(0, code, output)
        self.assertFalse(early, "the folder was removed while the client was still alive on the signal path")
        self.assertTrue(gone, "the client was still there when the task returned")
        self.assertEqual([], left)
        self.assertEqual(["term"], terms)

    def test_a_second_term_during_the_cleanup_changes_nothing_and_the_cleanup_runs_once(self) -> None:
        deadline = self.pull_numbers()["reserve"] + 20
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, values = self.prepare_pull(folder)
            (stand_in.parent / "ignore_term_pull").write_text("")
            self.signal_shell(stand_in, 2, 1.5)
            code, output = run_tasks("pull_images.yml", values | {"compose_stack_pull_deadline_seconds": deadline})
            left = os.listdir(self.parent(folder))
            pid = self.watch_pull(stand_in)
            gone = self.is_gone(pid, tries=1)
            early = (stand_in.parent / "folder_gone_early").exists()
            terms = self.terms(stand_in)
        self.assertNotEqual(0, code, output)
        self.assertTrue(gone, "a second TERM interrupted the cleanup and left the client")
        self.assertEqual([], left, "a second TERM interrupted the cleanup and left the folder")
        self.assertFalse(early)
        self.assertEqual(["term"], terms, "the cleanup ran more than once")

    def test_a_killed_shell_leaves_its_watchdog_to_kill_the_pull_group_and_remove_the_folder_after_the_client_is_gone(self) -> None:
        deadline = self.pull_numbers()["reserve"] + 20
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, code, output = self.ignoring_run(folder, deadline, "kill_shell")
            pid = self.watch_pull(stand_in)
            gone = self.is_gone(pid, tries=300)
            for _ in range(50):
                left = os.listdir(self.parent(folder))
                if not left:
                    break
                time.sleep(0.1)
            early = (stand_in.parent / "folder_gone_early").exists()
            terms = self.terms(stand_in)
        self.assertNotEqual(0, code, output)
        self.assertTrue(gone, "nothing killed the pull group after its shell was killed")
        self.assertEqual([], left, "nothing removed the folder after its shell was killed")
        self.assertFalse(early, "the folder was removed while the client still ran")
        self.assertEqual(["term"], terms)

    def test_a_pull_that_survives_kill_keeps_its_login_folder_fails_with_a_fixed_message_and_the_watchdog_removes_the_folder_once_the_client_is_gone(
        self,
    ) -> None:
        deadline = self.pull_numbers()["reserve"] + 5
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, values = self.prepare_pull(folder)
            (stand_in.parent / "ignore_term_pull").write_text("")
            swallow = folder / "swallow_kill.sh"
            swallow.write_text('kill() { case "$*" in "-s KILL -- -$pull") return 0 ;; esac; builtin kill "$@"; }\n')
            code, output = run_tasks("pull_images.yml", values | {"compose_stack_pull_deadline_seconds": deadline}, environment={"BASH_ENV": str(swallow)})
            pid = self.watch_pull(stand_in)
            alive = not self.is_gone(pid, tries=1)
            left = os.listdir(self.parent(folder))
            early = (stand_in.parent / "folder_gone_early").exists()
            os.kill(pid, signal.SIGKILL)
            for _ in range(150):
                after = os.listdir(self.parent(folder))
                if not after:
                    break
                time.sleep(0.1)
        self.assertNotEqual(0, code, output)
        self.assertIn("the image pull could not be stopped", output)
        self.assertNotIn("ran past its deadline", output, "the deadline message hid that the pull could not be stopped")
        self.assertTrue(alive, "the injected KILL failure did not leave the client alive: the case proves nothing")
        self.assertEqual(1, len(left), "the folder was removed while the client was alive")
        self.assertFalse(early, "the folder was removed while the client was alive")
        self.assertNotIn(self.TOKEN, output)
        self.assertNotIn(left[0], output)
        self.assertEqual([], after, "the watchdog did not remove the folder once the client was gone")

    def test_a_login_folder_that_cannot_be_removed_fails_the_task_with_a_fixed_message(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, values = self.prepare_pull(folder)
            failing = folder / "failing_rm.sh"
            failing.write_text('rm() { case "$*" in *login.*) return 1 ;; esac; command rm "$@"; }\n')
            code, output = run_tasks("pull_images.yml", values, environment={"BASH_ENV": str(failing)})
            left = os.listdir(self.parent(folder))
            running = self.running_from(scratch)
        self.assertNotEqual(0, code, output)
        self.assertIn("the pull's login folder could not be removed", output)
        self.assertEqual(1, len(left), "the injected removal failure left no folder: the case proves nothing")
        self.assertNotIn(left[0], output)
        self.assertNotIn(self.TOKEN, output)
        self.assertEqual([], running, "the task left a process of its own")

    @staticmethod
    def is_gone(pid: int, tries: int = 50) -> bool:
        for _ in range(tries):
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return True
            time.sleep(0.1)
        return False

    @staticmethod
    def kill_quietly(pid: int) -> None:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)

    def test_the_process_list_that_the_pull_takes_holds_nothing_from_outside_the_runs_process_groups(self) -> None:
        outsider = subprocess.Popen(["/bin/sleep", "4711"], start_new_session=True)  # noqa: S603 - a process in a session of its own, to prove the list is scoped
        self.addCleanup(outsider.wait)
        self.addCleanup(outsider.kill)
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, values = self.prepare_pull(folder)
            code, output = run_tasks("pull_images.yml", values)
            listing = (stand_in.parent / "ps.log").read_text()
        self.assertEqual(0, code, output)
        self.assertNotIn("sleep 4711", listing, "a process outside the run's groups is in the list")
        self.assertIn("compose --project-name sample --profile batch pull --quiet", listing)

    def test_a_second_run_changes_nothing_on_the_host_and_runs_the_same_pull(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, values = self.prepare_pull(folder)
            first, _ = run_tasks("pull_images.yml", values)
            code, output = run_tasks("pull_images.yml", values)
            left = os.listdir(self.parent(folder))
            count = len(calls(stand_in))
        self.assertEqual((0, 0), (first, code), output)
        self.assertRegex(output, CHANGED_NONE)
        self.assertEqual(2, count)
        self.assertEqual([], left)

    def test_a_missing_empty_or_loose_token_file_stops_the_deploy_with_the_fixed_message_before_any_docker_call(self) -> None:
        cases = {"missing": {"mode": None}, "empty": {"content": ""}, "loose": {"mode": 0o600}}
        for label, more in cases.items():
            with self.subTest(case=label), tempfile.TemporaryDirectory() as scratch:
                stand_in, values = self.prepare_pull(Path(scratch), **more)  # type: ignore[arg-type]
                code, output = run_tasks("pull_images.yml", values)
                self.assertNotEqual(0, code)
                self.assertIn("the registry token file is missing, is empty or is not a plain file that only", output)
                self.assertEqual([], calls(stand_in))
                self.assertNotIn(self.TOKEN, output)

    def test_a_token_file_of_white_space_only_is_refused_before_any_write_and_no_docker_call_runs(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, values = self.prepare_pull(folder, content="  \n")
            code, output = run_tasks("pull_images.yml", values)
            made = self.parent(folder).exists()
        self.assertNotEqual(0, code)
        self.assertIn("the registry token file is empty", output)
        self.assertEqual([], calls(stand_in))
        self.assertFalse(made)

    def test_an_unset_user_or_token_file_setting_is_refused_by_name(self) -> None:
        for name in ("compose_stack_registry_user", "compose_stack_registry_token_file"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as scratch:
                stand_in, values = self.prepare_pull(Path(scratch))
                values[name] = ""
                code, output = run_tasks("pull_images.yml", values)
                self.assertNotEqual(0, code)
                self.assertIn("set compose_stack_registry_user and compose_stack_registry_token_file", output)
                self.assertEqual([], calls(stand_in))

    def test_the_task_file_runs_no_docker_login_or_logout_and_keeps_no_token_in_a_variable(self) -> None:
        text = (ROLE / "tasks" / "pull_images.yml").read_text()
        code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
        for word in ("login'", "logout'", "docker login", "docker logout", "slurp", "b64decode", "--password"):
            self.assertNotIn(word, code)
        self.assertNotIn("registry_token.content", code)


class SourceCoverageTest(unittest.TestCase):
    def test_a_path_is_held_by_a_source_that_is_it_or_a_folder_above_it_and_not_by_a_sibling_with_the_same_prefix(self) -> None:
        sources = ["spark", "tests/fixtures/vitaldb"]
        self.assertEqual(set(), uncovered({"spark", "spark/Dockerfile", "tests/fixtures/vitaldb/release.json", "tests/fixtures/vitaldb"}, sources))
        self.assertEqual(
            {"tests", "tests/fixtures", "tests/fixtures/vitaldb2/release.json", "tests/other.json", "sparkle/x"},
            uncovered({"tests", "tests/fixtures", "tests/fixtures/vitaldb2/release.json", "tests/other.json", "sparkle/x"}, sources),
        )

    def test_the_paths_that_a_dockerfile_copies_are_found_with_their_full_path(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            dockerfile = Path(scratch) / "Dockerfile"
            dockerfile.write_text("FROM scratch\nCOPY a/b.txt /x/\nCOPY --from=other c /y\nCOPY scripts/ingestion \\\n  /z\n")
            found = copied_paths(dockerfile)
        self.assertEqual({"a/b.txt", "scripts/ingestion"}, found)


def from_contexts(dockerfile: Path) -> set[str]:
    """The named build contexts that a Dockerfile copies from: every ``COPY --from=<name>`` whose name is neither an image nor a stage of the file."""
    text = dockerfile.read_text()
    stages = set(re.findall(r"(?im)^FROM\s.*\sAS\s+(\S+)\s*$", text))
    return {name for name in re.findall(r"--from=(\S+)", text) if ":" not in name and "@" not in name and name not in stages}


def copied_paths(dockerfile: Path) -> set[str]:
    """Each path that a Dockerfile copies from its build context, without a trailing slash."""
    lines = dockerfile.read_text().replace("\\\n", " ").splitlines()
    copied = [line.split()[1:-1] for line in lines if line.startswith("COPY ") and "--from=" not in line]
    return {source.rstrip("/") for sources in copied for source in sources}


def uncovered(paths: set[str], sources: list[str]) -> set[str]:
    """The paths that no source of the image holds: a source holds itself and every path below it, and a sibling that only shares its prefix is not held."""
    return {path for path in paths if not any(path == source or path.startswith(source + "/") for source in sources)}


class BuiltImagesTest(unittest.TestCase):
    def test_the_images_the_publish_command_builds_are_the_project_images_that_the_service_files_name_and_each_dockerfile_is_in_its_sources(self) -> None:
        built: Any = load_variable(ROOT / "ansible" / "group_vars" / "all.yml", "compose_stack_built_images")
        named = set()
        for service_file in sorted((ROOT / "compose" / "services").glob("*.yml")):
            document: Any = load_yaml(service_file.read_text())
            (service,) = document["services"].values()
            if str(service["image"]).startswith("${IMAGE_"):
                named.add(str(service["image"]).split(":")[0].removeprefix("${"))
        self.assertTrue(named)
        self.assertEqual(named, {"IMAGE_" + image["name"].upper() for image in built})
        for image in built:
            context = image.get("context", image["sources"][0])
            dockerfile = image.get("dockerfile", f"{context}/Dockerfile")
            with self.subTest(image=image["tag"]):
                self.assertEqual(set(), uncovered({dockerfile}, image["sources"]))
                self.assertTrue((ROOT / dockerfile).is_file())
                self.assertTrue(context == "." or context in image["sources"])
                self.assertTrue(all(re.fullmatch(SOURCE_PATH, source) for source in image["sources"]), image["sources"])
                self.assertEqual(from_contexts(ROOT / dockerfile), set(image.get("build_contexts", {})))
                if context == ".":
                    self.assertEqual(set(), uncovered(copied_paths(ROOT / dockerfile), image["sources"]))
                self.assertTrue(set(image.get("build_contexts", {}).values()) <= set(image["sources"]))

    def test_the_spark_image_stages_the_release_pin_folder_of_tests_and_no_other_file_of_tests(self) -> None:
        images: Any = load_variable(ROOT / "ansible" / "group_vars" / "all.yml", "compose_stack_built_images")
        sources = {image["name"]: image["sources"] for image in images}
        self.assertEqual(["tests/fixtures/vitaldb"], [source for source in sources["spark"] if source.split("/")[0] == "tests"])
        self.assertEqual(["spark", "scripts", "replay", "casefile", "features"], [source for source in sources["spark"] if source.split("/")[0] != "tests"])
        self.assertEqual(["orchestration", "scripts", "compose", "spark", "tests/fixtures/vitaldb"], sources["dagster"])
        self.assertEqual(["replay", "casefile", "scripts", "features", "tests"], sources["replay"])
        self.assertTrue((ROOT / "tests" / "fixtures" / "vitaldb" / "release.json").is_file())

    def test_the_dagster_image_gets_the_scripts_and_compose_folders_and_the_spark_image_is_built_from_the_folder_above_its_dockerfile(self) -> None:
        images: Any = load_variable(ROOT / "ansible" / "group_vars" / "all.yml", "compose_stack_built_images")
        built = {image["name"]: image for image in images}
        self.assertEqual(
            {"stack_scripts": "scripts", "stack_compose": "compose", "stack_spark": "spark", "stack_fixtures": "tests/fixtures/vitaldb"},
            built["dagster"]["build_contexts"],
        )
        self.assertIn("tests/fixtures/vitaldb", built["dagster"]["sources"])
        self.assertEqual(("spark/Dockerfile", "."), (built["spark"]["dockerfile"], built["spark"]["context"]))
        self.assertEqual("surgical-risk-spark:4.1.4", built["spark"]["tag"])


if __name__ == "__main__":
    unittest.main()
