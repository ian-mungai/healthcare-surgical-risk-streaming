"""How the stack role starts the stack and builds the images the project makes itself, through the real ``ansible-playbook`` and a stand-in ``docker``.

The stand-in is a small script that logs each call and answers like Docker for the calls the role makes, so the cases prove the order and the form of the
commands, and the rule that an unchanged build context is not built again. Real containers and a real build are the acceptance test's job.
"""

from __future__ import annotations

import grp
import json
import os
import pwd
import re
import signal
import stat
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any

from scripts.project_config import ROOT
from scripts.stack.yaml_text import load_yaml
from tests.fast.ansible_support import ROLES, load_variable, run_playbook

ROLE = ROLES / "compose_stack"
CHANGED_NONE = re.compile(r"changed=0\s")
LABEL = re.compile(r"surgical-risk\.context=([0-9a-f]{64})")
TAG = "surgical-risk-sample:1.0.0"

STAND_IN = r"""#!/bin/sh
here="$(dirname "$0")"
printf '%s\n' "$*" >> "$here/calls.log"
for word in "$@"; do
  case "$word" in
    inspect) if [ "$1" = "image" ]; then verb=image_inspect; else verb=inspect; fi ;;
    up) verb=up ;;
    ps) verb=ps ;;
    build) verb=build ;;
  esac
done
case " $* " in *" config "*) verb=config ;; esac
case " $* " in *" --format json "*) case "$verb" in ps) verb=ps_json ;; esac ;; esac
case "$verb" in
  config) if [ -f "$here/config.json" ]; then cat "$here/config.json"; else echo '{"services": {}}'; fi ;;
  ps_json) if [ -f "$here/ps.json" ]; then cat "$here/ps.json"; fi ;;
  image_inspect) if [ -f "$here/label" ]; then cat "$here/label"; else echo "Error: No such image" >&2; exit 1; fi ;;
  build)
    if [ -f "$here/fail_build" ]; then echo "build failed" >&2; exit 1; fi
    if [ -f "$here/slow_build" ]; then echo $$ > "$here/slow.pid"; exec sleep 59; fi
    for word in "$@"; do case "$word" in surgical-risk.context=*) printf '%s' "${word#surgical-risk.context=}" > "$here/label" ;; esac; done ;;
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
        "compose_stack_built_images": [{"tag": TAG, "name": "sample", "sources": [str(context)]}],
        "compose_stack_build_deadline_seconds": 60,
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


class BuildTest(unittest.TestCase):
    def build(self, values_from: tuple[Path, Path, Path], **more: object) -> tuple[int, str]:
        folder, stand_in, context = values_from
        return run_tasks("build_images.yml", variables(folder, stand_in, context, **more))

    def builds(self, stand_in: Path) -> list[str]:
        return [line for line in calls(stand_in) if " build " in f" {line} " and "image inspect" not in line]

    def test_the_first_deploy_places_the_context_and_builds_the_image_once_with_the_hash_of_the_context(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, context = prepare(folder)
            code, output = self.build((folder, stand_in, context))
            placed = folder / "platform" / "images" / "sample" / "sample"
            mode = stat.S_IMODE((placed / "run.sh").stat().st_mode)
            logged = self.builds(stand_in)
            files = sorted(path.name for path in placed.iterdir())
        self.assertEqual(0, code, output)
        self.assertEqual(["Dockerfile", "run.sh"], files)
        self.assertEqual(0o755, mode)
        self.assertEqual(1, len(logged), logged)
        self.assertRegex(logged[0], rf"^build --platform linux/amd64 --tag {re.escape(TAG)} --label {LABEL.pattern} {re.escape(str(placed))}$")

    def test_a_second_deploy_of_an_unchanged_context_builds_nothing_and_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, context = prepare(folder)
            self.build((folder, stand_in, context))
            code, output = self.build((folder, stand_in, context))
            logged = self.builds(stand_in)
        self.assertEqual(0, code, output)
        self.assertEqual(1, len(logged), logged)
        self.assertRegex(output, CHANGED_NONE)

    def test_a_changed_file_a_changed_mode_a_new_file_or_a_missing_image_each_builds_again_and_an_unchanged_context_does_not(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, context = prepare(folder)
            self.build((folder, stand_in, context))
            labels = [LABEL.search(self.builds(stand_in)[-1])]
            for label, change in (
                ("a changed file", lambda: (context / "Dockerfile").write_text("FROM scratch\nRUN true\n")),
                ("a changed mode", lambda: (context / "run.sh").chmod(0o644)),
                ("a new file", lambda: (context / "more.txt").write_text("x")),
                ("a missing image", lambda: (stand_in.parent / "label").unlink()),
            ):
                before = len(self.builds(stand_in))
                change()
                code, output = self.build((folder, stand_in, context))
                with self.subTest(label):
                    self.assertEqual(0, code, output)
                    self.assertEqual(before + 1, len(self.builds(stand_in)))
                    labels.append(LABEL.search(self.builds(stand_in)[-1]))
            before = len(self.builds(stand_in))
            self.build((folder, stand_in, context))
            unchanged = len(self.builds(stand_in))
        self.assertEqual(before, unchanged)
        hashes = [match[1] for match in labels if match]
        self.assertEqual(len(hashes), len(set(hashes)) + 1, hashes)

    def test_a_failed_build_stops_the_deploy_naming_the_image_and_the_next_deploy_builds_again(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, context = prepare(folder)
            (stand_in.parent / "fail_build").write_text("")
            code, output = self.build((folder, stand_in, context))
            (stand_in.parent / "fail_build").unlink()
            again, again_output = self.build((folder, stand_in, context))
            logged = self.builds(stand_in)
        self.assertNotEqual(0, code)
        self.assertIn(TAG, output)
        self.assertEqual(0, again, again_output)
        self.assertEqual(2, len(logged), logged)

    def test_a_build_that_passes_its_deadline_fails_visibly_and_leaves_no_process_behind(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, context = prepare(folder)
            (stand_in.parent / "slow_build").write_text("")
            started = time.monotonic()
            code, output = self.build((folder, stand_in, context), compose_stack_build_deadline_seconds=3)
            elapsed = time.monotonic() - started
            pid_file = stand_in.parent / "slow.pid"
            pid = int(pid_file.read_text()) if pid_file.exists() else 0
            time.sleep(1)
            alive = pid > 0 and Path(f"/proc/{pid}").exists() if Path("/proc").exists() else self.alive(pid)
            if alive:
                os.kill(pid, signal.SIGKILL)
        self.assertNotEqual(0, code)
        self.assertRegex(output, r"(?i)time|deadline")
        self.assertLess(elapsed, 40)
        self.assertFalse(alive, "the build command is still running after its deadline")

    @staticmethod
    def alive(pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        return True


class ExtraContextBuildTest(unittest.TestCase):
    """An image whose Dockerfile reads named build contexts, or that is built from the folder above its Dockerfile, like the Dagster and Spark images."""

    def prepare(self, folder: Path) -> tuple[Path, Path, Path, dict[str, object]]:
        stand_in, context = prepare(folder)
        extra = folder / "source" / "extra"
        extra.mkdir()
        (extra / "module.py").write_text("VALUE = 1\n")
        image = {"tag": TAG, "name": "sample", "sources": [str(context), str(extra)], "context": ".", "dockerfile": "sample/Dockerfile"}
        return stand_in, context, extra, image | {"build_contexts": {"more": "extra"}}

    def build(self, folder: Path, stand_in: Path, context: Path, image: dict[str, object]) -> tuple[int, str]:
        return run_tasks("build_images.yml", variables(folder, stand_in, context, compose_stack_built_images=[image]))

    def test_each_source_is_placed_in_the_images_folder_and_the_build_names_the_dockerfile_the_named_contexts_and_the_folder(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, context, extra, image = self.prepare(folder)
            code, output = self.build(folder, stand_in, context, image)
            placed = folder / "platform" / "images" / "sample"
            files = sorted(str(path.relative_to(placed)) for path in placed.rglob("*") if path.is_file())
            logged = [line for line in calls(stand_in) if line.startswith("build ")]
        self.assertEqual(0, code, output)
        self.assertEqual(["extra/module.py", "sample/Dockerfile", "sample/run.sh"], files)
        self.assertEqual(1, len(logged), logged)
        self.assertRegex(
            logged[0],
            rf"^build --platform linux/amd64 --tag {re.escape(TAG)} --label {LABEL.pattern} --file={re.escape(str(placed / 'sample' / 'Dockerfile'))} "
            rf"--build-context=more={re.escape(str(placed / 'extra'))} {re.escape(str(placed))}$",
        )

    def test_a_change_in_a_named_context_builds_again_and_an_unchanged_one_does_not(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            stand_in, context, extra, image = self.prepare(folder)
            self.build(folder, stand_in, context, image)
            code, output = self.build(folder, stand_in, context, image)
            unchanged = len([line for line in calls(stand_in) if line.startswith("build ")])
            (extra / "module.py").write_text("VALUE = 2\n")
            changed_code, changed_output = self.build(folder, stand_in, context, image)
            changed = len([line for line in calls(stand_in) if line.startswith("build ")])
        self.assertEqual((0, 0), (code, changed_code), output + changed_output)
        self.assertRegex(output, CHANGED_NONE)
        self.assertEqual((1, 2), (unchanged, changed))


def from_contexts(dockerfile: Path) -> set[str]:
    """The named build contexts that a Dockerfile copies from: every ``COPY --from=<name>`` whose name is neither an image nor a stage of the file."""
    text = dockerfile.read_text()
    stages = set(re.findall(r"(?im)^FROM\s.*\sAS\s+(\S+)\s*$", text))
    return {name for name in re.findall(r"--from=(\S+)", text) if ":" not in name and "@" not in name and name not in stages}


def context_sources(dockerfile: Path) -> set[str]:
    """The first folder of each path that a Dockerfile copies from its build context: each must be a source of the image on the host."""
    lines = dockerfile.read_text().replace("\\\n", " ").splitlines()
    copied = [line.split()[1:-1] for line in lines if line.startswith("COPY ") and "--from=" not in line]
    return {source.split("/")[0] for sources in copied for source in sources}


class BuiltImagesTest(unittest.TestCase):
    def test_the_images_the_role_builds_are_the_project_images_that_the_service_files_name_and_each_dockerfile_is_in_its_sources(self) -> None:
        built: Any = load_variable(ROOT / "ansible" / "group_vars" / "all.yml", "compose_stack_built_images")
        named = set()
        for service_file in sorted((ROOT / "compose" / "services").glob("*.yml")):
            document: Any = load_yaml(service_file.read_text())
            (service,) = document["services"].values()
            if str(service["image"]).startswith("surgical-risk-"):
                named.add(str(service["image"]).split("@")[0])
        self.assertTrue(named)
        self.assertEqual(named, {image["tag"] for image in built})
        for image in built:
            context = image.get("context", image["sources"][0])
            dockerfile = image.get("dockerfile", f"{context}/Dockerfile")
            with self.subTest(image=image["tag"]):
                self.assertIn(dockerfile.split("/")[0], image["sources"])
                self.assertTrue((ROOT / dockerfile).is_file())
                self.assertTrue(context == "." or context in image["sources"])
                self.assertEqual(from_contexts(ROOT / dockerfile), set(image.get("build_contexts", {})))
                if context == ".":
                    self.assertEqual(set(), context_sources(ROOT / dockerfile) - set(image["sources"]))
                self.assertTrue(set(image.get("build_contexts", {}).values()) <= set(image["sources"]))

    def test_the_dagster_image_gets_the_scripts_and_compose_folders_and_the_spark_image_is_built_from_the_folder_above_its_dockerfile(self) -> None:
        images: Any = load_variable(ROOT / "ansible" / "group_vars" / "all.yml", "compose_stack_built_images")
        built = {image["name"]: image for image in images}
        self.assertEqual({"stack_scripts": "scripts", "stack_compose": "compose"}, built["dagster"]["build_contexts"])
        self.assertEqual(("spark/Dockerfile", "."), (built["spark"]["dockerfile"], built["spark"]["context"]))
        self.assertEqual("surgical-risk-spark:4.1.4", built["spark"]["tag"])


if __name__ == "__main__":
    unittest.main()
