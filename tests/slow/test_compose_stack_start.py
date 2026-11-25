"""How the stack role starts the stack and pulls the images the project pushed, through the real ``ansible-playbook`` and a stand-in ``docker``.

The stand-in is a small script that logs each call and answers like Docker for the calls the role makes, so the cases prove the order and the form of the
commands and that the registry token reaches Docker through standard input only. Real containers and a real pull are the acceptance test's job.
"""

from __future__ import annotations

import grp
import json
import os
import pwd
import re
import tempfile
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
for word in "$@"; do
  case "$word" in
    inspect) if [ "$1" = "image" ]; then verb=image_inspect; else verb=inspect; fi ;;
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
  image_inspect) if [ -f "$here/label" ]; then cat "$here/label"; else echo "Error: No such image" >&2; exit 1; fi ;;
  login)
    cat > "$here/login_stdin.txt"
    if [ -f "$here/fail_login" ]; then cat "$here/login_stdin.txt" >&2; exit 1; fi ;;
  logout) : ;;
  pull) if [ -f "$here/fail_pull" ]; then echo "pull failed" >&2; exit 1; fi ;;
  up)
    touch "$here/started"
    case " $* " in *" --force-recreate "*) sed -i.bak 's/ .*$/ 2099-01-01T00:00:00Z/' "$here/inspect.txt"; rm -f "$here/inspect.txt.bak" ;; esac ;;
  ps) if [ -f "$here/started" ]; then echo abc123; fi ;;
  inspect) if [ -f "$here/inspect.txt" ]; then cat "$here/inspect.txt"; else echo "abc123 2026-01-01T00:00:00Z"; fi ;;
esac
"""


def prepare(folder: Path) -> tuple[Path, Path]:
    """The stand-in ``docker`` and a build context with an executable script, both in ``folder``."""
    bin_folder = folder / "bin"
    bin_folder.mkdir()
    stand_in = bin_folder / "docker"
    stand_in.write_text(STAND_IN)
    stand_in.chmod(0o755)
    context = folder / "source" / "sample"
    context.mkdir(parents=True)
    (context / "Dockerfile").write_text("FROM scratch\n")
    (context / "run.sh").write_text("#!/bin/sh\n")
    (context / "run.sh").chmod(0o755)
    return stand_in, context


def calls(stand_in: Path) -> list[str]:
    log = stand_in.parent / "calls.log"
    return log.read_text().splitlines() if log.exists() else []


def run_tasks(task_file: str, values: dict[str, object]) -> tuple[int, str]:
    with tempfile.TemporaryDirectory() as scratch:
        work = Path(scratch)
        playbook = work / "check.yml"
        include = f"    - ansible.builtin.include_role:\n        name: {ROLE}\n        tasks_from: {task_file}\n"
        playbook.write_text(f"- hosts: localhost\n  connection: local\n  gather_facts: false\n  tasks:\n{include}")
        result = run_playbook(work, playbook, ["-v", "-i", "localhost,", "--extra-vars", json.dumps(values)])
    return result.returncode, result.stdout + result.stderr


def variables(folder: Path, stand_in: Path, context: Path, **more: object) -> dict[str, object]:
    return {
        "compose_stack_directory": str(folder / "platform"),
        "compose_stack_owner": pwd.getpwuid(os.getuid()).pw_name,
        "compose_stack_group": grp.getgrgid(os.getgid()).gr_name,
        "compose_stack_docker": [str(stand_in), "compose", "--project-name", "sample"],
        "compose_stack_source_root": str(context.parent),
        "compose_stack_pull_deadline_seconds": 60,
        "compose_stack_start_deadline_seconds": 60,
        "compose_stack_health_deadline_seconds": 30,
        **more,
    }


class StartTest(unittest.TestCase):
    def start(self, services: list[str]) -> tuple[list[str], int, str]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, context = prepare(folder)
            code, output = run_tasks("start.yml", variables(folder, stand_in, context, compose_stack_start_services=services))
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
            stand_in, context = prepare(folder)
            (stand_in.parent / "services.txt").write_text(names)
            values = variables(folder, stand_in, context, compose_stack_start_services=[], compose_stack_start_except=left_out)
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
            stand_in, context = prepare(folder)
            values = variables(folder, stand_in, context, compose_stack_start_services=["postgres"], compose_stack_start_except=["postgres"])
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
            stand_in, context = prepare(folder)
            values = variables(folder, stand_in, context, compose_stack_start_services=[])
            first, _ = run_tasks("start.yml", values)
            code, output = run_tasks("start.yml", values)
        self.assertEqual((0, 0), (first, code), output)
        self.assertRegex(output, CHANGED_NONE)


class RecreateTest(unittest.TestCase):
    """A service whose placed configuration file is newer than its container is recreated, alone, and a second run changes nothing."""

    def setup(self, folder: Path, *, started: dict[str, str]) -> tuple[Path, Path, Path]:
        stand_in, context = prepare(folder)
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

    def start(self, folder: Path, stand_in: Path, context: Path, services: list[str]) -> tuple[int, str]:
        return run_tasks("start.yml", variables(folder, stand_in, context, compose_stack_start_services=services))

    def test_only_the_service_whose_file_is_newer_than_its_container_is_recreated_without_its_dependencies(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, alloy_file, _ = self.setup(folder, started={"alloy": "2025-01-01T00:00:00Z", "grafana": "2025-01-01T00:00:00Z"})
            os.utime(alloy_file, (1_800_000_000, 1_800_000_000))
            code, output = self.start(folder, stand_in, folder, [])
            forced = self.recreations(stand_in)
        self.assertEqual(0, code, output)
        self.assertEqual(["compose --project-name sample up --detach --wait --wait-timeout 30 --force-recreate --no-deps alloy"], forced)

    def test_a_second_run_recreates_nothing_and_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, alloy_file, _ = self.setup(folder, started={"alloy": "2025-01-01T00:00:00Z", "grafana": "2025-01-01T00:00:00Z"})
            os.utime(alloy_file, (1_800_000_000, 1_800_000_000))
            self.start(folder, stand_in, folder, [])
            code, output = self.start(folder, stand_in, folder, [])
            forced = self.recreations(stand_in)
        self.assertEqual(0, code, output)
        self.assertEqual(1, len(forced), forced)
        self.assertRegex(output, CHANGED_NONE)

    def test_a_stack_whose_files_are_older_than_its_containers_recreates_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, _, _ = self.setup(folder, started={"alloy": "2025-01-01T00:00:00Z", "grafana": "2025-01-01T00:00:00Z"})
            code, output = self.start(folder, stand_in, folder, [])
            forced = self.recreations(stand_in)
        self.assertEqual(0, code, output)
        self.assertEqual([], forced)

    def test_the_first_start_of_postgresql_alone_leaves_a_stale_service_of_the_second_start(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, alloy_file, _ = self.setup(folder, started={"alloy": "2025-01-01T00:00:00Z", "grafana": "2025-01-01T00:00:00Z"})
            os.utime(alloy_file, (1_800_000_000, 1_800_000_000))
            code, output = self.start(folder, stand_in, folder, ["postgres"])
            forced = self.recreations(stand_in)
        self.assertEqual(0, code, output)
        self.assertEqual([], forced)

    def test_a_service_that_is_named_in_the_start_and_stale_is_recreated(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, alloy_file, grafana_file = self.setup(folder, started={"alloy": "2025-01-01T00:00:00Z", "grafana": "2025-01-01T00:00:00Z"})
            os.utime(alloy_file, (1_800_000_000, 1_800_000_000))
            os.utime(grafana_file, (1_800_000_000, 1_800_000_000))
            code, output = self.start(folder, stand_in, folder, ["grafana"])
            forced = self.recreations(stand_in)
        self.assertEqual(0, code, output)
        self.assertEqual(["compose --project-name sample up --detach --wait --wait-timeout 30 --force-recreate --no-deps grafana"], forced)


class SecretRecreateTest(unittest.TestCase):
    """A service whose secret file was replaced after its container started is recreated alone by the Compose command; only a named service is read."""

    RECREATE = "compose --project-name sample up --detach --wait --wait-timeout 30 --force-recreate --no-deps dagster_code_location"

    def setup(self, folder: Path, *, secret_age: int | None) -> tuple[Path, Path]:
        """The stand-in with two containers that started in 2025, and a secret file of the code location that is as old as ``secret_age`` or absent."""
        stand_in, _ = prepare(folder)
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
        return run_tasks("start.yml", variables(folder, stand_in, folder, compose_stack_start_services=services))

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
    TOKEN = "dckr_pat_example_token_value"  # noqa: S105 - a stand-in value that proves the token never reaches a command line or the output

    def prepare_pull(self, folder: Path, *, mode: int | None = 0o400, content: str = TOKEN + "\n") -> tuple[Path, dict[str, object]]:
        stand_in, context = prepare(folder)
        token_file = folder / "registry_token"
        if mode is not None:
            token_file.write_text(content)
            token_file.chmod(mode)
        values = variables(
            folder,
            stand_in,
            context,
            compose_stack_registry_user="example_account",
            compose_stack_registry_token_file=str(token_file),
        )
        return stand_in, values

    def test_the_login_reads_the_token_from_standard_input_the_pull_follows_and_the_logout_ends_it_with_the_token_on_no_command_line_and_in_no_output(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, values = self.prepare_pull(folder)
            code, output = run_tasks("pull_images.yml", values)
            logged = calls(stand_in)
            stdin = (stand_in.parent / "login_stdin.txt").read_text()
        self.assertEqual(0, code, output)
        self.assertEqual(
            [
                "login --username example_account --password-stdin",
                "compose --project-name sample --profile batch pull --quiet",
                "logout",
            ],
            logged,
        )
        self.assertEqual(self.TOKEN, stdin.strip())
        self.assertNotIn(self.TOKEN, "\n".join(logged) + output)

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

    def test_a_token_file_of_white_space_only_is_refused_before_the_login_and_only_the_logout_runs(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            stand_in, values = self.prepare_pull(Path(scratch), content="  \n")
            code, output = run_tasks("pull_images.yml", values)
            self.assertNotEqual(0, code)
            self.assertIn("the registry token file is empty", output)
            self.assertEqual(["logout"], calls(stand_in))

    def test_an_unset_user_or_token_file_setting_is_refused_by_name(self) -> None:
        for name in ("compose_stack_registry_user", "compose_stack_registry_token_file"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as scratch:
                stand_in, values = self.prepare_pull(Path(scratch))
                values[name] = ""
                code, output = run_tasks("pull_images.yml", values)
                self.assertNotEqual(0, code)
                self.assertIn("set compose_stack_registry_user and compose_stack_registry_token_file", output)
                self.assertEqual([], calls(stand_in))

    def test_a_refused_login_prints_no_token_even_though_docker_echoes_it_and_no_pull_follows(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            stand_in, values = self.prepare_pull(Path(scratch))
            (stand_in.parent / "fail_login").write_text("")
            code, output = run_tasks("pull_images.yml", values)
            logged = calls(stand_in)
        self.assertNotEqual(0, code)
        self.assertNotIn(self.TOKEN, output)
        self.assertEqual(["login --username example_account --password-stdin", "logout"], logged)

    def test_a_failed_pull_stops_the_deploy_and_still_logs_out(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            stand_in, values = self.prepare_pull(Path(scratch))
            (stand_in.parent / "fail_pull").write_text("")
            code, output = run_tasks("pull_images.yml", values)
            logged = calls(stand_in)
        self.assertNotEqual(0, code, output)
        self.assertEqual("logout", logged[-1])


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
