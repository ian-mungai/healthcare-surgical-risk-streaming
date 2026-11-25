"""The runner checkout role, run through the real ``ansible-playbook`` on this computer against a temporary Git repository.

The interpreter archive and the package index are stand-ins that need no network: the interpreter is this project's own pinned Python (a link at the place
where the role installs it), the archive test serves a small archive from a file, and ``pip`` reads one local wheel through ``PIP_NO_INDEX`` and
``PIP_FIND_LINKS``. Everything else is the role's own work: ``git archive``, the folder's protection, the environment and the removal of the old folder.
"""

from __future__ import annotations

import grp
import hashlib
import io
import json
import os
import pwd
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import unittest
import zipfile
from pathlib import Path

from scripts.infra import runner_place
from scripts.process import CompletedProcess, run_command
from tests.fast.ansible_paths import GROUP_VARIABLES, group_variables
from tests.slow.ansible_support import ROLES, environment

ROLE = ROLES / "runner_checkout"
OWNER = pwd.getpwuid(os.getuid()).pw_name
GROUP = grp.getgrgid(os.getgid()).gr_name
VERSION = ".".join(str(part) for part in sys.version_info[:3])
MARKER = ".runner_checkout.json"
MANIFEST = "runner_manifest.json"
PYTHON_MARKER = ".archive_sha256"
PINNED_DIGEST = str(group_variables()["runner_checkout_python_archive_sha256"])
CHANGED_NONE = re.compile(r"changed=0\s")
GIT_ENV = {
    "PATH": os.environ["PATH"],
    "GIT_CONFIG_GLOBAL": os.devnull,
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_AUTHOR_NAME": "Fixture",
    "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
    "GIT_COMMITTER_NAME": "Fixture",
    "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
}


def make_wheel(folder: Path) -> str:
    """One tiny wheel in ``folder``; returns its SHA-256."""
    path = folder / "runner_probe-1.0-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as wheel:
        wheel.writestr("runner_probe.py", "VALUE = 1\n")
        wheel.writestr("runner_probe-1.0.dist-info/METADATA", "Metadata-Version: 2.1\nName: runner-probe\nVersion: 1.0\n")
        wheel.writestr("runner_probe-1.0.dist-info/WHEEL", "Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n")
        wheel.writestr("runner_probe-1.0.dist-info/RECORD", "")
    return hashlib.sha256(path.read_bytes()).hexdigest()


class Scratch:
    """A temporary repository with one hash-pinned requirement, the folders the role writes, and a way to run the role."""

    def __init__(self, test: unittest.TestCase) -> None:
        holder = tempfile.TemporaryDirectory(prefix="runner_checkout_")
        test.addCleanup(holder.cleanup)
        self.base = Path(holder.name)
        self.repo = self.base / "repo"
        self.repo.mkdir()
        self.wheels = self.base / "wheels"
        self.wheels.mkdir()
        self.runner_root = self.base / "runner"
        self.python_root = self.base / "python"
        self.wheel_hash = make_wheel(self.wheels)
        self.git("init", "--quiet", "--initial-branch", "main")
        (self.repo / ".gitignore").write_text("ignored_runner.py\n")
        self.commit_a = self.commit({"requirements.txt": self.requirements(), "app/a.py": "A = 1\n", "tests/e2e/tracked.py": "T = 1\n"})
        interpreter = self.python_root / f"{VERSION}-fixture" / "bin" / f"python{VERSION.rsplit('.', 1)[0]}"
        interpreter.parent.mkdir(parents=True)
        interpreter.symlink_to(Path(sys.executable))
        (self.python_root / f"{VERSION}-fixture" / PYTHON_MARKER).write_text(PINNED_DIGEST + "\n")
        (self.python_root / VERSION).symlink_to(f"{VERSION}-fixture")
        self.scratch_root = self.base / "controller_scratch"
        self.scratch_root.mkdir()

    def requirements(self, digest: str | None = None) -> str:
        return f"runner-probe==1.0 \\\n    --hash=sha256:{digest or self.wheel_hash}\n"

    def git(self, *args: str) -> CompletedProcess[str]:
        return run_command("git", args, cwd=self.repo, env=GIT_ENV, timeout=60, check=True)

    def commit(self, files: dict[str, str]) -> str:
        for name, text in files.items():
            (self.repo / name).parent.mkdir(parents=True, exist_ok=True)
            (self.repo / name).write_text(text)
        self.git("add", "--all", ".")
        self.git("commit", "--quiet", "--allow-empty", "-m", "chore: fixture")
        return self.git("rev-parse", "HEAD").stdout.strip()

    def folder(self, commit: str) -> Path:
        return self.runner_root / commit

    def variables(self, commit: str, **more: object) -> dict[str, object]:
        return {
            "runner_checkout_commit": commit,
            "runner_checkout_source_root": str(self.repo),
            "runner_checkout_directory": str(self.runner_root),
            "runner_checkout_owner": OWNER,
            "runner_checkout_group": GROUP,
            "runner_checkout_python_directory": str(self.python_root),
            "runner_checkout_python_version": VERSION,
            "runner_checkout_install_deadline_seconds": 120,
            "runner_checkout_stop_check_delay_seconds": 1,
        } | more

    def run(self, commit: str, tasks_from: str = "main", **more: object) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as scratch:
            work = Path(scratch)
            playbook = work / "check.yml"
            includes = f"    - ansible.builtin.include_role:\n        name: {ROLE}\n        tasks_from: {tasks_from}\n"
            header = f"- hosts: localhost\n  connection: local\n  gather_facts: false\n  vars_files: [{GROUP_VARIABLES}]\n  tasks:\n"
            playbook.write_text(header + includes)
            env = environment(work) | {
                "PIP_NO_INDEX": "1",
                "PIP_FIND_LINKS": str(self.wheels),
                "PIP_CACHE_DIR": str(work / "pip"),
                "HOME": str(work),
                "TMPDIR": str(self.scratch_root),
                "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
            }
            result = run_command(
                str(Path(sys.prefix) / "bin" / "ansible-playbook"),
                ["-v", "-i", "localhost,", "--extra-vars", json.dumps(self.variables(commit, **more)), str(playbook)],
                cwd=work,
                env=env,
                timeout=300,
            )
        return result.returncode, result.stdout + result.stderr


def files_under(folder: Path) -> set[str]:
    return {str(path.relative_to(folder)) for path in folder.rglob("*") if path.is_file() and ".venv" not in path.relative_to(folder).parts}


class PlacementTest(unittest.TestCase):
    def test_the_folder_holds_exactly_the_tracked_files_of_the_commit_with_no_history_and_no_ignored_or_uncommitted_file(self) -> None:
        scratch = Scratch(self)
        (scratch.repo / "ignored_runner.py").write_text("IGNORED = 1\n")
        (scratch.repo / "app" / "a.py").write_text("A = 2  # uncommitted\n")
        (scratch.repo / "untracked.py").write_text("U = 1\n")
        code, output = scratch.run(scratch.commit_a)
        folder = scratch.folder(scratch.commit_a)
        tracked = set(scratch.git("ls-tree", "-r", "--name-only", scratch.commit_a).stdout.split())
        self.assertEqual(0, code, output)
        self.assertEqual(tracked | {MARKER, MANIFEST}, files_under(folder))
        self.assertEqual("A = 1\n", (folder / "app" / "a.py").read_text())
        self.assertFalse((folder / ".git").exists())
        self.assertEqual([], [path for path in folder.rglob(".git*") if ".venv" not in path.parts and path.name != ".gitignore"])

    def test_the_environment_is_the_pinned_interpreter_with_the_hash_pinned_requirements_installed(self) -> None:
        scratch = Scratch(self)
        code, output = scratch.run(scratch.commit_a)
        venv_python = scratch.folder(scratch.commit_a) / ".venv" / "bin" / "python"
        probe = run_command(
            str(venv_python),
            ["-I", "-c", "import runner_probe, sys; print(runner_probe.VALUE, sys.version_info[:3] == tuple(map(int, sys.argv[1].split('.'))))", VERSION],
            timeout=60,
        )
        self.assertEqual(0, code, output)
        self.assertEqual("1 True", probe.stdout.strip())

    def test_a_requirement_whose_hash_is_wrong_fails_the_deploy_and_leaves_no_finished_folder(self) -> None:
        scratch = Scratch(self)
        commit = scratch.commit({"requirements.txt": scratch.requirements("0" * 64)})
        code, output = scratch.run(commit)
        self.assertNotEqual(0, code)
        self.assertIn("hash", output.lower())
        self.assertFalse((scratch.folder(commit) / MARKER).exists())

    def test_a_requirement_with_no_hash_fails_the_deploy(self) -> None:
        scratch = Scratch(self)
        commit = scratch.commit({"requirements.txt": "runner-probe==1.0\n"})
        code, output = scratch.run(commit)
        self.assertNotEqual(0, code)
        self.assertFalse((scratch.folder(commit) / MARKER).exists(), output)

    def test_a_commit_that_is_not_forty_hexadecimal_digits_or_not_in_the_repository_is_refused_before_a_folder_is_made(self) -> None:
        scratch = Scratch(self)
        for commit in ("main", "A" * 40, scratch.commit_a[:10], "f" * 40):
            with self.subTest(commit=commit):
                code, _ = scratch.run(commit)
                self.assertNotEqual(0, code)
                self.assertFalse(scratch.folder(commit).exists())

    def test_a_commit_that_the_repository_cannot_give_fails_before_the_interpreter_is_downloaded_or_any_folder_of_the_host_is_made(self) -> None:
        scratch = Scratch(self)
        (scratch.repo / "app" / "bad_link").symlink_to("/etc/passwd")
        scratch.git("add", "app/bad_link")
        scratch.git("commit", "--quiet", "-m", "chore: link")
        with_link = scratch.git("rev-parse", "HEAD").stdout.strip()
        fresh = scratch.base / "fresh_python"
        for name, commit in (("not in the repository", "f" * 40), ("holds a link", with_link)):
            with self.subTest(commit=name):
                code, _ = scratch.run(commit, runner_checkout_python_directory=str(fresh))
                self.assertNotEqual(0, code)
                self.assertFalse(fresh.exists(), "the interpreter's folder was made before the archive was read")
                self.assertFalse(scratch.runner_root.exists(), "the runner folder was made before the archive was read")

    def test_the_marker_that_the_role_writes_is_accepted_by_the_guard_of_the_runner_placement_and_a_changed_commit_is_not(self) -> None:
        scratch = Scratch(self)
        code, output = scratch.run(scratch.commit_a)
        self.assertEqual(0, code, output)
        directory = str(scratch.runner_root)
        for commit, expected in ((scratch.commit_a, 0), ("e" * 40, 3)):
            guard = runner_place.runner_folder_guard(commit, directory)
            if expected == 0:
                result = run_command("sh", ["-c", guard + "echo accepted"], timeout=30)
                self.assertEqual((0, "accepted"), (result.returncode, result.stdout.strip()), result.stderr)
            else:
                (scratch.runner_root / commit).mkdir()
                shutil.copy(scratch.folder(scratch.commit_a) / MARKER, scratch.runner_root / commit / MARKER)
                result = run_command("sh", ["-c", guard + "echo accepted"], timeout=30)
                self.assertEqual((expected, runner_place.NO_FOLDER_WORD), (result.returncode, result.stdout.strip()), "the marker of another commit")


class ProtectionTest(unittest.TestCase):
    def test_nothing_in_the_folder_is_open_to_other_users_and_the_folders_are_for_the_owner_only(self) -> None:
        scratch = Scratch(self)
        code, output = scratch.run(scratch.commit_a)
        self.assertEqual(0, code, output)
        folder = scratch.folder(scratch.commit_a)
        self.assertEqual(0o750, stat.S_IMODE(scratch.runner_root.stat().st_mode))
        self.assertEqual(0o750, stat.S_IMODE(folder.stat().st_mode))
        opened = [str(path) for path in [folder, *folder.rglob("*")] if not path.is_symlink() and stat.S_IMODE(path.lstat().st_mode) & 0o007]
        self.assertEqual([], opened)
        for path in [folder, *folder.rglob("*")]:
            if not path.is_symlink():
                self.assertEqual((OWNER, GROUP), (pwd.getpwuid(path.stat().st_uid).pw_name, grp.getgrgid(path.stat().st_gid).gr_name), str(path))

    def test_a_file_that_a_commit_made_world_readable_is_closed_in_the_folder(self) -> None:
        scratch = Scratch(self)
        script = scratch.repo / "tool.sh"
        script.write_text("#!/bin/sh\n")
        script.chmod(0o777)
        commit = scratch.commit({})
        code, output = scratch.run(commit)
        self.assertEqual(0, code, output)
        self.assertEqual(0, stat.S_IMODE((scratch.folder(commit) / "tool.sh").stat().st_mode) & 0o007)

    def test_the_folder_does_not_carry_the_stacks_files_or_a_secret_beyond_what_the_commit_tracks(self) -> None:
        scratch = Scratch(self)
        code, output = scratch.run(scratch.commit_a)
        self.assertEqual(0, code, output)
        self.assertEqual([], [str(path) for path in scratch.base.rglob("*") if path.name in (".env", "host.env", "limits.env")])


class RedeployTest(unittest.TestCase):
    def test_a_second_deploy_of_the_same_commit_changes_nothing_and_keeps_a_placed_runner(self) -> None:
        scratch = Scratch(self)
        scratch.run(scratch.commit_a)
        placed = scratch.folder(scratch.commit_a) / "tests" / "e2e" / "silver_signals.py"
        placed.write_text("PLACED = 1\n")
        before = {str(path): path.stat().st_mtime_ns for path in scratch.folder(scratch.commit_a).rglob("*") if path.is_file()}
        code, output = scratch.run(scratch.commit_a)
        after = {str(path): path.stat().st_mtime_ns for path in scratch.folder(scratch.commit_a).rglob("*") if path.is_file()}
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_NONE)
        self.assertEqual(before, after)
        self.assertEqual("PLACED = 1\n", placed.read_text())

    def test_a_new_commit_replaces_the_folder_and_removes_the_old_one_with_everything_in_it(self) -> None:
        scratch = Scratch(self)
        scratch.run(scratch.commit_a)
        (scratch.folder(scratch.commit_a) / "tests" / "e2e" / "silver_signals.py").write_text("PLACED = 1\n")
        (scratch.runner_root / "stray_folder").mkdir()
        (scratch.runner_root / "stray_file").write_text("x\n")
        commit_b = scratch.commit({"app/a.py": "A = 3\n"})
        code, output = scratch.run(commit_b)
        self.assertEqual(0, code, output)
        self.assertEqual([commit_b], sorted(path.name for path in scratch.runner_root.iterdir()))
        self.assertEqual("A = 3\n", (scratch.folder(commit_b) / "app" / "a.py").read_text())
        self.assertFalse((scratch.folder(commit_b) / "tests" / "e2e" / "silver_signals.py").exists())

    def test_a_folder_with_no_finished_marker_is_removed_and_built_again(self) -> None:
        scratch = Scratch(self)
        scratch.folder(scratch.commit_a).mkdir(parents=True)
        (scratch.folder(scratch.commit_a) / "left_over.txt").write_text("half done\n")
        code, output = scratch.run(scratch.commit_a)
        self.assertEqual(0, code, output)
        self.assertFalse((scratch.folder(scratch.commit_a) / "left_over.txt").exists())
        self.assertTrue((scratch.folder(scratch.commit_a) / MARKER).is_file())

    def test_a_changed_interpreter_pin_builds_the_folder_again(self) -> None:
        scratch = Scratch(self)
        scratch.run(scratch.commit_a)
        marker = scratch.folder(scratch.commit_a) / MARKER
        document = json.loads(marker.read_text())
        document["python"] = "3.14.0"
        marker.write_text(json.dumps(document))
        (scratch.folder(scratch.commit_a) / "tests" / "e2e" / "silver_signals.py").write_text("PLACED = 1\n")
        code, output = scratch.run(scratch.commit_a)
        self.assertEqual(0, code, output)
        self.assertEqual(VERSION, json.loads(marker.read_text())["python"])
        self.assertFalse((scratch.folder(scratch.commit_a) / "tests" / "e2e" / "silver_signals.py").exists())


class ManifestTest(unittest.TestCase):
    """The source manifest ``runner_manifest.json`` in the folder's root: what the streaming host runner checks its provenance with."""

    def deployed(self) -> tuple[Scratch, Path]:
        scratch = Scratch(self)
        code, output = scratch.run(scratch.commit_a)
        self.assertEqual(0, code, output)
        return scratch, scratch.folder(scratch.commit_a)

    def test_it_holds_exactly_the_commit_and_every_archived_file_with_the_sha256_of_its_bytes(self) -> None:
        scratch, folder = self.deployed()
        document = json.loads((folder / MANIFEST).read_text())
        tracked = set(scratch.git("ls-tree", "-r", "--name-only", scratch.commit_a).stdout.split())
        self.assertEqual(["commit", "files"], sorted(document))
        self.assertEqual(scratch.commit_a, document["commit"])
        self.assertEqual(tracked, set(document["files"]))
        for name, digest in document["files"].items():
            with self.subTest(name=name):
                self.assertEqual(hashlib.sha256((folder / name).read_bytes()).hexdigest(), digest)
        self.assertEqual(
            {"app/a.py": hashlib.sha256(b"A = 1\n").hexdigest()}, {name: digest for name, digest in document["files"].items() if name == "app/a.py"}
        )

    def test_it_lists_no_environment_file_no_marker_no_manifest_and_no_placed_runner(self) -> None:
        scratch, folder = self.deployed()
        (folder / "tests" / "e2e" / "silver_signals.py").write_text("PLACED = 1\n")
        code, output = scratch.run(scratch.commit_a)
        self.assertEqual(0, code, output)
        names = set(json.loads((folder / MANIFEST).read_text())["files"])
        self.assertNotIn("tests/e2e/silver_signals.py", names)
        self.assertEqual([], [name for name in names if name.startswith(".venv") or name in (MARKER, MANIFEST)])

    def test_it_is_read_only_to_its_owner_closed_to_others_and_written_before_the_marker(self) -> None:
        _, folder = self.deployed()
        manifest = folder / MANIFEST
        self.assertEqual(0o400, stat.S_IMODE(manifest.stat().st_mode))
        self.assertEqual((OWNER, GROUP), (pwd.getpwuid(manifest.stat().st_uid).pw_name, grp.getgrgid(manifest.stat().st_gid).gr_name))
        self.assertLessEqual(manifest.stat().st_mtime_ns, (folder / MARKER).stat().st_mtime_ns)

    def test_a_second_deploy_of_the_same_commit_leaves_the_manifest_and_a_placed_runner_as_they_are(self) -> None:
        scratch, folder = self.deployed()
        placed = folder / "tests" / "e2e" / "silver_signals.py"
        placed.write_text("PLACED = 1\n")
        before = (folder / MANIFEST).stat().st_mtime_ns
        code, output = scratch.run(scratch.commit_a)
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_NONE)
        self.assertEqual(before, (folder / MANIFEST).stat().st_mtime_ns)
        self.assertEqual("PLACED = 1\n", placed.read_text())

    def test_a_changed_archived_file_makes_the_folder_unfinished_and_the_next_deploy_builds_it_again(self) -> None:
        scratch, folder = self.deployed()
        placed = folder / "tests" / "e2e" / "silver_signals.py"
        placed.write_text("PLACED = 1\n")
        (folder / "app" / "a.py").chmod(0o640)
        (folder / "app" / "a.py").write_text("A = 99\n")
        code, output = scratch.run(scratch.commit_a)
        self.assertEqual(0, code, output)
        self.assertEqual("A = 1\n", (folder / "app" / "a.py").read_text())
        self.assertFalse(placed.exists(), "a rebuilt folder holds no placed runner")

    def test_a_removed_archived_file_makes_the_folder_unfinished(self) -> None:
        scratch, folder = self.deployed()
        (folder / "app" / "a.py").unlink()
        code, output = scratch.run(scratch.commit_a)
        self.assertEqual(0, code, output)
        self.assertEqual("A = 1\n", (folder / "app" / "a.py").read_text())

    def test_a_missing_manifest_makes_the_folder_unfinished_and_the_next_deploy_builds_it_again(self) -> None:
        scratch, folder = self.deployed()
        placed = folder / "tests" / "e2e" / "silver_signals.py"
        placed.write_text("PLACED = 1\n")
        (folder / MANIFEST).unlink()
        code, output = scratch.run(scratch.commit_a)
        self.assertEqual(0, code, output)
        self.assertTrue((folder / MANIFEST).is_file())
        self.assertFalse(placed.exists())

    def test_a_manifest_that_was_edited_to_fit_a_changed_file_is_not_the_one_of_the_commit_and_the_check_still_passes_only_for_its_own_commit(self) -> None:
        scratch, folder = self.deployed()
        document = json.loads((folder / MANIFEST).read_text())
        document["commit"] = "e" * 40
        (folder / MANIFEST).chmod(0o600)
        (folder / MANIFEST).write_text(json.dumps(document))
        code, output = scratch.run(scratch.commit_a)
        self.assertEqual(0, code, output)
        self.assertEqual(scratch.commit_a, json.loads((folder / MANIFEST).read_text())["commit"])

    def test_a_link_in_the_archive_fails_the_deploy_before_a_marker_is_written(self) -> None:
        scratch = Scratch(self)
        (scratch.repo / "link_to_a.py").symlink_to("app/a.py")
        commit = scratch.commit({})
        code, output = scratch.run(commit)
        self.assertNotEqual(0, code)
        self.assertIn("link_to_a.py", output)
        self.assertFalse((scratch.folder(commit) / MARKER).exists())


def venv_site(folder: Path) -> Path:
    return next((folder / ".venv" / "lib").glob("python*/site-packages"))


def add_distribution(folder: Path, name: str, version: str) -> Path:
    info = venv_site(folder) / f"{name.replace('-', '_')}-{version}.dist-info"
    info.mkdir()
    (info / "METADATA").write_text(f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n")
    return info


class RealFolderTest(unittest.TestCase):
    """The runner folder and the commit's folder are real folders: a link is never followed, written through or pruned through."""

    def test_a_runner_folder_that_is_a_link_fails_the_deploy_and_nothing_is_written_through_it(self) -> None:
        scratch = Scratch(self)
        target = scratch.base / "elsewhere"
        target.mkdir()
        (target / "keep.txt").write_text("keep\n")
        target.chmod(0o755)
        scratch.runner_root.symlink_to(target)
        code, output = scratch.run(scratch.commit_a)
        self.assertNotEqual(0, code)
        self.assertIn("link", output)
        self.assertEqual(["keep.txt"], sorted(path.name for path in target.iterdir()))
        self.assertEqual(0o755, stat.S_IMODE(target.stat().st_mode))
        self.assertTrue(scratch.runner_root.is_symlink())

    def test_a_runner_folder_that_another_user_owns_fails_the_deploy_before_any_change_and_nothing_in_it_is_pruned(self) -> None:
        scratch = Scratch(self)
        foreign = "root" if OWNER != "root" else "nobody"
        scratch.runner_root.mkdir()
        scratch.runner_root.chmod(0o711)
        (scratch.runner_root / "older_folder").mkdir()
        (scratch.runner_root / "older_folder" / "keep.txt").write_text("keep\n")
        code, output = scratch.run(scratch.commit_a, runner_checkout_owner=foreign)
        self.assertNotEqual(0, code)
        self.assertIn('"msg": "', output)
        self.assertRegex(output, r'"msg": "[^"]*is owned by another user', "the refusal of the check itself is the message of the failure")
        self.assertNotIn("chown failed", output, "the role did not try to take the folder over")
        self.assertEqual(0o711, stat.S_IMODE(scratch.runner_root.stat().st_mode), "no permission was changed")
        self.assertEqual("keep\n", (scratch.runner_root / "older_folder" / "keep.txt").read_text())
        self.assertEqual(["older_folder"], sorted(path.name for path in scratch.runner_root.iterdir()))

    def test_a_commit_folder_that_is_a_link_to_a_finished_copy_is_replaced_by_a_real_folder_and_the_target_is_not_touched(self) -> None:
        scratch = Scratch(self)
        self.assertEqual(0, scratch.run(scratch.commit_a)[0])
        copy = scratch.base / "copy_of_finished_folder"
        shutil.copytree(scratch.folder(scratch.commit_a), copy, symlinks=True)
        before = {str(path.relative_to(copy)): path.stat().st_mtime_ns for path in copy.rglob("*") if path.is_file() and not path.is_symlink()}
        shutil.rmtree(scratch.folder(scratch.commit_a))
        scratch.folder(scratch.commit_a).symlink_to(copy)
        code, output = scratch.run(scratch.commit_a)
        self.assertEqual(0, code, output)
        folder = scratch.folder(scratch.commit_a)
        self.assertFalse(folder.is_symlink())
        self.assertTrue(folder.is_dir())
        self.assertEqual(before, {str(path.relative_to(copy)): path.stat().st_mtime_ns for path in copy.rglob("*") if path.is_file() and not path.is_symlink()})

    def test_a_marker_that_is_a_link_is_not_a_finished_marker(self) -> None:
        scratch = Scratch(self)
        self.assertEqual(0, scratch.run(scratch.commit_a)[0])
        marker = scratch.folder(scratch.commit_a) / MARKER
        elsewhere = scratch.base / "marker_copy.json"
        shutil.copy(marker, elsewhere)
        marker.unlink()
        marker.symlink_to(elsewhere)
        (scratch.folder(scratch.commit_a) / "tests" / "e2e" / "silver_signals.py").write_text("PLACED = 1\n")
        code, output = scratch.run(scratch.commit_a)
        self.assertEqual(0, code, output)
        self.assertFalse(marker.is_symlink())
        self.assertFalse((scratch.folder(scratch.commit_a) / "tests" / "e2e" / "silver_signals.py").exists())

    def test_a_marker_that_names_another_commit_makes_the_folder_unfinished(self) -> None:
        scratch = Scratch(self)
        self.assertEqual(0, scratch.run(scratch.commit_a)[0])
        marker = scratch.folder(scratch.commit_a) / MARKER
        document = json.loads(marker.read_text())
        document["commit"] = "e" * 40
        marker.write_text(json.dumps(document, sort_keys=True) + "\n")
        (scratch.folder(scratch.commit_a) / "tests" / "e2e" / "silver_signals.py").write_text("PLACED = 1\n")
        code, output = scratch.run(scratch.commit_a)
        self.assertEqual(0, code, output)
        self.assertEqual(scratch.commit_a, json.loads(marker.read_text())["commit"])
        self.assertFalse((scratch.folder(scratch.commit_a) / "tests" / "e2e" / "silver_signals.py").exists())

    def test_a_link_among_the_other_entries_is_removed_and_what_it_leads_to_is_not(self) -> None:
        scratch = Scratch(self)
        self.assertEqual(0, scratch.run(scratch.commit_a)[0])
        target = scratch.base / "linked_target"
        target.mkdir()
        (target / "keep.txt").write_text("keep\n")
        (scratch.runner_root / "stray_link").symlink_to(target)
        commit_b = scratch.commit({"app/a.py": "A = 5\n"})
        code, output = scratch.run(commit_b)
        self.assertEqual(0, code, output)
        self.assertEqual([commit_b], sorted(path.name for path in scratch.runner_root.iterdir()))
        self.assertEqual("keep\n", (target / "keep.txt").read_text())


class ExactManifestTest(unittest.TestCase):
    """The expected files come from the controller's archive: a manifest that was emptied, cut or edited to fit a changed file does not finish a folder."""

    def deployed(self) -> tuple[Scratch, Path, Path]:
        scratch = Scratch(self)
        self.assertEqual(0, scratch.run(scratch.commit_a)[0])
        folder = scratch.folder(scratch.commit_a)
        placed = folder / "tests" / "e2e" / "silver_signals.py"
        placed.write_text("PLACED = 1\n")
        return scratch, folder, placed

    def replace_manifest(self, folder: Path, files: dict[str, str] | None) -> None:
        document = json.loads((folder / MANIFEST).read_text())
        (folder / MANIFEST).chmod(0o600)
        (folder / MANIFEST).write_text(json.dumps({"commit": document["commit"], "files": document["files"] if files is None else files}))

    def test_a_manifest_with_an_empty_files_map_makes_the_folder_unfinished(self) -> None:
        scratch, folder, placed = self.deployed()
        self.replace_manifest(folder, {})
        code, output = scratch.run(scratch.commit_a)
        self.assertEqual(0, code, output)
        self.assertFalse(placed.exists(), "the folder was built again")
        self.assertTrue(json.loads((folder / MANIFEST).read_text())["files"])

    def test_a_manifest_that_lists_only_a_part_of_the_archive_or_more_than_it_makes_the_folder_unfinished(self) -> None:
        for case in ("part", "extra"):
            with self.subTest(case=case):
                scratch, folder, placed = self.deployed()
                files = dict(json.loads((folder / MANIFEST).read_text())["files"])
                if case == "part":
                    files.pop("app/a.py")
                else:
                    files["app/not_in_the_archive.py"] = "1" * 64
                self.replace_manifest(folder, files)
                code, output = scratch.run(scratch.commit_a)
                self.assertEqual(0, code, output)
                self.assertFalse(placed.exists())
                tracked = set(scratch.git("ls-tree", "-r", "--name-only", scratch.commit_a).stdout.split())
                self.assertEqual(tracked, set(json.loads((folder / MANIFEST).read_text())["files"]))

    def test_a_changed_file_with_a_manifest_edited_to_fit_it_is_still_found_and_the_folder_is_built_again(self) -> None:
        scratch, folder, placed = self.deployed()
        (folder / "app" / "a.py").chmod(0o640)
        (folder / "app" / "a.py").write_text("A = 99\n")
        files = dict(json.loads((folder / MANIFEST).read_text())["files"])
        files["app/a.py"] = hashlib.sha256(b"A = 99\n").hexdigest()
        self.replace_manifest(folder, files)
        code, output = scratch.run(scratch.commit_a)
        self.assertEqual(0, code, output)
        self.assertEqual("A = 1\n", (folder / "app" / "a.py").read_text())
        self.assertFalse(placed.exists())

    def test_a_link_in_the_commit_fails_before_the_folder_that_was_there_is_touched(self) -> None:
        scratch, folder, placed = self.deployed()
        (scratch.repo / "link_to_a.py").symlink_to("app/a.py")
        commit_b = scratch.commit({})
        code, output = scratch.run(commit_b)
        self.assertNotEqual(0, code)
        self.assertIn("link_to_a.py", output)
        self.assertTrue(placed.is_file(), "the finished folder of the earlier commit is still there")
        self.assertTrue((folder / MARKER).is_file())
        self.assertFalse(scratch.folder(commit_b).exists())


class EnvironmentCheckTest(unittest.TestCase):
    """A finished folder is kept only when its environment runs, says the pinned version and holds exactly what the lock file pins."""

    def deployed(self) -> tuple[Scratch, Path, Path]:
        scratch = Scratch(self)
        self.assertEqual(0, scratch.run(scratch.commit_a)[0])
        folder = scratch.folder(scratch.commit_a)
        placed = folder / "tests" / "e2e" / "silver_signals.py"
        placed.write_text("PLACED = 1\n")
        return scratch, folder, placed

    def test_an_untouched_environment_keeps_the_folder_and_changes_nothing(self) -> None:
        scratch, folder, placed = self.deployed()
        code, output = scratch.run(scratch.commit_a)
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_NONE)
        self.assertTrue(placed.exists())
        self.assertTrue((folder / ".venv").is_dir())

    def test_a_package_that_the_lock_file_does_not_pin_a_missing_package_and_another_version_each_build_the_folder_again(self) -> None:
        for case in ("extra", "missing", "version"):
            with self.subTest(case=case):
                scratch, folder, placed = self.deployed()
                if case == "extra":
                    stray = add_distribution(folder, "stray-package", "1.0")
                elif case == "missing":
                    stray = next(venv_site(folder).glob("runner_probe-1.0.dist-info"))
                    shutil.rmtree(stray)
                else:
                    stray = next(venv_site(folder).glob("runner_probe-1.0.dist-info"))
                    (stray / "METADATA").write_text("Metadata-Version: 2.1\nName: runner-probe\nVersion: 9.9\n")
                code, output = scratch.run(scratch.commit_a)
                self.assertEqual(0, code, output)
                self.assertFalse(placed.exists(), "the folder was built again")
                self.assertEqual([], [path.name for path in venv_site(folder).glob("stray_package*")])
                self.assertTrue(next(venv_site(folder).glob("runner_probe-1.0.dist-info")).is_dir())

    def test_a_package_whose_marker_is_true_for_the_interpreter_and_that_is_missing_builds_the_folder_again_and_one_whose_marker_is_false_is_not_installed(
        self,
    ) -> None:
        scratch = Scratch(self)
        lock = (
            f"runner-probe==1.0 ; platform_python_implementation == 'CPython' \\\n    --hash=sha256:{scratch.wheel_hash}\n"
            f"other-probe==1.0 ; platform_python_implementation == 'NoSuchPython' \\\n    --hash=sha256:{'0' * 64}\n"
        )
        commit = scratch.commit({"requirements.txt": lock})
        code, output = scratch.run(commit)
        self.assertEqual(0, code, output)
        folder = scratch.folder(commit)
        self.assertEqual([], list(venv_site(folder).glob("other_probe*")), "the marker is false here, so pip did not install it")
        placed = folder / "tests" / "e2e" / "silver_signals.py"
        placed.parent.mkdir(parents=True, exist_ok=True)
        placed.write_text("PLACED = 1\n")
        code, output = scratch.run(commit)
        self.assertEqual(0, code, output)
        self.assertTrue(placed.exists(), "the environment holds what the lock files require here, so the folder is kept")
        shutil.rmtree(next(venv_site(folder).glob("runner_probe-1.0.dist-info")))
        code, output = scratch.run(commit)
        self.assertEqual(0, code, output)
        self.assertFalse(placed.exists(), "a package that the marker requires here was missing, so the folder was built again")
        self.assertTrue(next(venv_site(folder).glob("runner_probe-1.0.dist-info")).is_dir())

    def test_an_environment_whose_interpreter_says_another_version_or_does_not_run_builds_the_folder_again(self) -> None:
        for case in ("another version", "does not run"):
            with self.subTest(case=case):
                scratch, folder, placed = self.deployed()
                python = folder / ".venv" / "bin" / "python"
                python.unlink()
                python.write_text("#!/bin/sh\n" + ("echo Python 3.0.0\n" if case == "another version" else "exit 7\n"))
                python.chmod(0o755)
                code, output = scratch.run(scratch.commit_a)
                self.assertEqual(0, code, output)
                self.assertFalse(placed.exists())
                self.assertIn(f"Python {VERSION}", run_command(str(python), ["--version"], timeout=30).stdout)


def timeout_signals_the_whole_group() -> bool:
    """GNU ``timeout`` (the host's) signals its whole process group; the ``timeout`` that ships with macOS signals the command only."""
    return "GNU coreutils" in run_command("timeout", ["--version"], timeout=10).stdout


class DeadlineTest(unittest.TestCase):
    """A command that never ends is stopped where it runs, with the processes ``timeout`` reaches, and nothing is cleaned up while one still works."""

    WAITER = "sleep 7003 & wait" if timeout_signals_the_whole_group() else "exec sleep 7003"
    STUB = (
        "#!/bin/sh\n"
        'case "$1" in\n'
        f"  --version) echo 'Python {VERSION}' ;;\n"
        '  -m) case "$2" in\n'
        '        venv) mkdir -p "$3/bin" && cp "$0" "$3/bin/python" ;;\n'
        f"        pip) {WAITER} ;;\n"
        "        *) exit 0 ;;\n"
        "      esac ;;\n"
        "esac\n"
    )

    def stub_interpreter(self, scratch: Scratch) -> None:
        interpreter = scratch.python_root / VERSION / "bin" / f"python{VERSION.rsplit('.', 1)[0]}"
        interpreter.unlink()
        interpreter.write_text(self.STUB)
        interpreter.chmod(0o755)

    def pgrep(self, pattern: str) -> int:
        return run_command("pgrep", ["-f", pattern], timeout=10).returncode

    def test_an_install_that_never_ends_is_stopped_at_its_deadline_with_its_processes_and_leaves_no_finished_folder_and_no_archive(self) -> None:
        scratch = Scratch(self)
        self.stub_interpreter(scratch)
        started = time.monotonic()
        code, output = scratch.run(scratch.commit_a, runner_checkout_install_deadline_seconds=2, runner_checkout_kill_after_seconds=1)
        self.assertNotEqual(0, code)
        self.assertLess(time.monotonic() - started, 120)
        self.assertRegex(output, r'"rc": 124|rc=124')
        self.assertEqual(1, self.pgrep("sleep 7003"), "the install's process group is gone")
        self.assertFalse((scratch.folder(scratch.commit_a) / MARKER).exists())
        self.assertFalse(Path(f"{scratch.folder(scratch.commit_a)}.tar").exists())
        self.assertEqual([], list(scratch.scratch_root.iterdir()), "the controller's scratch folder is removed")

    def test_a_process_that_still_works_in_the_runner_folder_stops_the_deploy_before_anything_is_removed_and_the_deploy_says_so(self) -> None:
        scratch = Scratch(self)
        commit = scratch.commit({"requirements.txt": scratch.requirements("0" * 64)})
        folder = scratch.folder(commit)
        worker = subprocess.Popen([f"{folder}/worker", "7004"], executable="/bin/sleep")  # noqa: S603
        self.addCleanup(worker.kill)
        code, output = scratch.run(commit, runner_checkout_stop_check_retries=2)
        self.assertNotEqual(0, code)
        self.assertIn("so nothing was cleaned up", output)
        self.assertFalse(folder.exists(), "the process that works in the runner folder stopped the deploy before it removed or made anything")
        self.assertFalse(Path(f"{folder}.tar").exists())
        worker.kill()
        worker.wait()

    def test_a_process_that_ends_while_the_check_waits_lets_the_cleanup_go_on(self) -> None:
        scratch = Scratch(self)
        commit = scratch.commit({"requirements.txt": scratch.requirements("0" * 64)})
        folder = scratch.folder(commit)
        worker = subprocess.Popen([f"{folder}/worker", "32"], executable="/bin/sleep")  # noqa: S603
        self.addCleanup(worker.kill)
        code, output = scratch.run(commit, runner_checkout_stop_check_retries=20, runner_checkout_stop_check_delay_seconds=2)
        self.assertNotEqual(0, code)
        self.assertNotIn("so nothing was cleaned up", output, "the process ended while the check tried again")
        self.assertIn("The runner folder was not finished", output)
        self.assertFalse(Path(f"{folder}.tar").exists())
        worker.wait(timeout=60)

    ORPHAN = (
        "#!/bin/sh\n"
        'case "$1" in\n'
        f"  --version) echo 'Python {VERSION}' ;;\n"
        '  -m) case "$2" in\n'
        '        venv) mkdir -p "$3/bin" && cp "$0" "$3/bin/python" ;;\n'
        '        pip) cd "${0%/.venv/bin/python}" && { sleep 7005 </dev/null >/dev/null 2>&1 & } ; exit 1 ;;\n'
        "        *) exit 0 ;;\n"
        "      esac ;;\n"
        "esac\n"
    )

    def orphan_interpreter(self, scratch: Scratch) -> None:
        """An interpreter whose install step leaves a process behind that works in the checkout (its working folder) and names no path in its command line."""
        self.stub_interpreter(scratch)
        (scratch.python_root / VERSION / "bin" / f"python{VERSION.rsplit('.', 1)[0]}").write_text(self.ORPHAN)
        self.addCleanup(run_command, "pkill", ["-f", "sleep 7005"], timeout=10)

    def test_a_process_that_the_build_started_and_that_works_in_the_checkout_stops_the_cleanup_even_when_its_command_line_names_no_path(self) -> None:
        scratch = Scratch(self)
        self.orphan_interpreter(scratch)
        code, output = scratch.run(scratch.commit_a, runner_checkout_stop_check_retries=1)
        self.assertNotEqual(0, code)
        self.assertEqual(0, self.pgrep("sleep 7005"), "the process of the failed install is still there")
        self.assertIn("so nothing was cleaned up", output)
        self.assertTrue(Path(f"{scratch.folder(scratch.commit_a)}.tar").exists(), "nothing was cleaned up while the process worked")

    def test_a_retry_removes_nothing_while_a_process_of_the_failed_build_still_works_and_goes_on_when_it_is_gone(self) -> None:
        scratch = Scratch(self)
        self.orphan_interpreter(scratch)
        self.assertNotEqual(0, scratch.run(scratch.commit_a, runner_checkout_stop_check_retries=1)[0])
        stale = scratch.runner_root / "another_folder"
        stale.mkdir()
        (stale / "keep.txt").write_text("keep\n")
        before = sorted(path.name for path in scratch.runner_root.iterdir())
        code, output = scratch.run(scratch.commit_a, runner_checkout_stop_check_retries=1)
        self.assertNotEqual(0, code)
        self.assertIn("so nothing was cleaned up", output)
        self.assertEqual(before, sorted(path.name for path in scratch.runner_root.iterdir()), "the retry pruned nothing before it looked for the process")
        self.assertEqual("keep\n", (stale / "keep.txt").read_text())
        run_command("pkill", ["-f", "sleep 7005"], timeout=10)
        for _ in range(50):
            if self.pgrep("sleep 7005") == 1:
                break
            time.sleep(0.1)
        interpreter = scratch.python_root / VERSION / "bin" / f"python{VERSION.rsplit('.', 1)[0]}"
        interpreter.unlink()
        interpreter.symlink_to(Path(sys.executable))
        code, output = scratch.run(scratch.commit_a)
        self.assertEqual(0, code, output)
        self.assertFalse(stale.exists(), "once the process was gone the retry pruned the other folders and built this one")
        self.assertTrue((scratch.folder(scratch.commit_a) / MARKER).is_file())

    def test_a_failed_build_with_no_process_left_removes_the_archive_and_says_which_step_failed(self) -> None:
        scratch = Scratch(self)
        commit = scratch.commit({"requirements.txt": scratch.requirements("0" * 64)})
        code, output = scratch.run(commit)
        self.assertNotEqual(0, code)
        self.assertFalse(Path(f"{scratch.folder(commit)}.tar").exists())
        self.assertIn("Install the hash-pinned requirements", output)
        self.assertEqual([], list(scratch.scratch_root.iterdir()))


class InterpreterTest(unittest.TestCase):
    def archive(self, folder: Path, version_line: str, name: str = "python.tar.gz", comment: str = "") -> tuple[Path, str]:
        data = io.BytesIO()
        script = f"#!/bin/sh\necho '{version_line}'\n{comment}\n".encode()
        with tarfile.open(fileobj=data, mode="w:gz") as archive:
            info = tarfile.TarInfo(f"python/bin/python{VERSION.rsplit('.', 1)[0]}")
            info.size, info.mode = len(script), 0o755
            archive.addfile(info, io.BytesIO(script))
        path = folder / name
        path.write_bytes(data.getvalue())
        return path, hashlib.sha256(path.read_bytes()).hexdigest()

    def run_python(self, scratch: Scratch, archive: Path, digest: str) -> tuple[int, str]:
        return scratch.run(
            scratch.commit_a,
            "python",
            runner_checkout_python_url=archive.as_uri(),
            runner_checkout_python_archive_sha256=digest,
            runner_checkout_python_directory=str(scratch.base / "fresh_python"),
            runner_checkout_python_download_directory=str(scratch.base),
        )

    def test_the_interpreter_is_downloaded_checked_against_its_hash_unpacked_and_its_version_checked(self) -> None:
        scratch = Scratch(self)
        archive, digest = self.archive(scratch.base, f"Python {VERSION}")
        code, output = self.run_python(scratch, archive, digest)
        self.assertEqual(0, code, output)
        self.assertTrue((scratch.base / "fresh_python" / VERSION / "bin" / f"python{VERSION.rsplit('.', 1)[0]}").is_file())
        again_code, again = self.run_python(scratch, archive, digest)
        self.assertEqual(0, again_code, again)
        self.assertRegex(again, CHANGED_NONE)

    def installed(self, scratch: Scratch) -> Path:
        return scratch.base / "fresh_python" / VERSION

    def entries(self, scratch: Scratch) -> list[str]:
        """The names in the interpreters' folder with each link's target: the published link and the folders that the role made."""
        found = sorted((path.name, os.readlink(path) if path.is_symlink() else "") for path in (scratch.base / "fresh_python").iterdir())
        return [f"{name} -> {target}" if target else name for name, target in found]

    def test_the_installed_interpreter_carries_the_digest_of_the_archive_it_came_from(self) -> None:
        scratch = Scratch(self)
        archive, digest = self.archive(scratch.base, f"Python {VERSION}")
        self.assertEqual(0, self.run_python(scratch, archive, digest)[0])
        self.assertEqual(digest, (self.installed(scratch) / PYTHON_MARKER).read_text().strip())
        self.assertEqual(0o755, stat.S_IMODE(self.installed(scratch).stat().st_mode))
        self.assertEqual([VERSION], [path.name for path in (scratch.base / "fresh_python").iterdir() if path.is_symlink()], "the published name is a link")
        self.assertEqual(2, len(self.entries(scratch)), "the link and the one folder that it names: no temporary folder is left")

    def test_an_archive_of_the_same_version_with_another_digest_replaces_the_installed_interpreter(self) -> None:
        scratch = Scratch(self)
        archive, digest = self.archive(scratch.base, f"Python {VERSION}")
        self.assertEqual(0, self.run_python(scratch, archive, digest)[0])
        interpreter = self.installed(scratch) / "bin" / f"python{VERSION.rsplit('.', 1)[0]}"
        interpreter.write_text(f"#!/bin/sh\necho 'Python {VERSION}'\n# tampered\n")
        second, other = self.archive(scratch.base, f"Python {VERSION}", name="second.tar.gz", comment="# the second archive")
        self.assertNotEqual(digest, other)
        code, output = self.run_python(scratch, second, other)
        self.assertEqual(0, code, output)
        self.assertIn("# the second archive", interpreter.read_text())
        self.assertNotIn("tampered", interpreter.read_text())
        self.assertEqual(other, (self.installed(scratch) / PYTHON_MARKER).read_text().strip())

    def test_an_installed_interpreter_with_no_marker_is_installed_again(self) -> None:
        scratch = Scratch(self)
        archive, digest = self.archive(scratch.base, f"Python {VERSION}")
        self.assertEqual(0, self.run_python(scratch, archive, digest)[0])
        (self.installed(scratch) / PYTHON_MARKER).unlink()
        (self.installed(scratch) / "left_over.txt").write_text("from an older install\n")
        code, output = self.run_python(scratch, archive, digest)
        self.assertEqual(0, code, output)
        self.assertFalse((self.installed(scratch) / "left_over.txt").exists())
        self.assertEqual(digest, (self.installed(scratch) / PYTHON_MARKER).read_text().strip())

    def test_a_new_archive_whose_interpreter_says_another_version_changes_nothing_of_the_installed_one_and_leaves_no_temporary_folder(self) -> None:
        scratch = Scratch(self)
        archive, digest = self.archive(scratch.base, f"Python {VERSION}")
        self.assertEqual(0, self.run_python(scratch, archive, digest)[0])
        before = (self.installed(scratch) / "bin" / f"python{VERSION.rsplit('.', 1)[0]}").read_text()
        before_entries = self.entries(scratch)
        wrong, wrong_digest = self.archive(scratch.base, "Python 3.14.0", name="wrong.tar.gz")
        code, _ = self.run_python(scratch, wrong, wrong_digest)
        self.assertNotEqual(0, code)
        self.assertEqual(before, (self.installed(scratch) / "bin" / f"python{VERSION.rsplit('.', 1)[0]}").read_text())
        self.assertEqual(digest, (self.installed(scratch) / PYTHON_MARKER).read_text().strip())
        self.assertEqual(before_entries, self.entries(scratch), "the installed folder and its link are as they were and no temporary folder is left")

    def test_the_published_interpreter_is_a_link_to_a_folder_of_its_own_and_the_old_folder_is_removed_after_the_link_changed(self) -> None:
        scratch = Scratch(self)
        archive, digest = self.archive(scratch.base, f"Python {VERSION}")
        self.assertEqual(0, self.run_python(scratch, archive, digest)[0])
        first = os.readlink(self.installed(scratch))
        second, other = self.archive(scratch.base, f"Python {VERSION}", name="second.tar.gz", comment="# the second archive")
        self.assertEqual(0, self.run_python(scratch, second, other)[0])
        later = os.readlink(self.installed(scratch))
        self.assertNotEqual(first, later)
        self.assertRegex(later, rf"^{re.escape(VERSION)}-[A-Za-z0-9_]+$")
        self.assertEqual([f"{VERSION} -> {later}", later], self.entries(scratch), "the folder of the first archive is gone")

    def test_the_interpreter_is_there_at_every_moment_while_another_archive_replaces_it(self) -> None:
        scratch = Scratch(self)
        archive, digest = self.archive(scratch.base, f"Python {VERSION}")
        self.assertEqual(0, self.run_python(scratch, archive, digest)[0])
        second, other = self.archive(scratch.base, f"Python {VERSION}", name="second.tar.gz", comment="# the second archive")
        interpreter = self.installed(scratch) / "bin" / f"python{VERSION.rsplit('.', 1)[0]}"
        gone: list[float] = []
        looked = [0]
        stop = threading.Event()

        def watch() -> None:
            while not stop.is_set():
                looked[0] += 1
                try:
                    os.stat(interpreter)
                except OSError:
                    gone.append(time.monotonic())

        watcher = threading.Thread(target=watch)
        watcher.start()
        try:
            code, output = self.run_python(scratch, second, other)
        finally:
            stop.set()
            watcher.join(timeout=30)
        self.assertEqual(0, code, output)
        self.assertGreater(looked[0], 1000, "the watcher looked often enough")
        self.assertEqual(0, len(gone), "the interpreter was missing while it was replaced")
        self.assertIn("# the second archive", interpreter.read_text())

    def test_a_folder_that_is_not_the_roles_link_is_refused_and_left_as_it_is(self) -> None:
        scratch = Scratch(self)
        archive, digest = self.archive(scratch.base, f"Python {VERSION}")
        installed = self.installed(scratch)
        (installed / "bin").mkdir(parents=True)
        (installed / "bin" / "kept.txt").write_text("not made by the role\n")
        code, output = self.run_python(scratch, archive, digest)
        self.assertNotEqual(0, code)
        self.assertIn("is not a link that this role made", output)
        self.assertEqual("not made by the role\n", (installed / "bin" / "kept.txt").read_text())
        self.assertEqual([VERSION], self.entries(scratch), "nothing was unpacked beside it")

    def test_a_folder_left_by_an_install_that_never_finished_is_removed_with_the_next_good_install_and_other_folders_stay(self) -> None:
        scratch = Scratch(self)
        archive, digest = self.archive(scratch.base, f"Python {VERSION}")
        unfinished = scratch.base / "fresh_python" / f"{VERSION}-unfinished"
        unrelated = scratch.base / "fresh_python" / "other_tool"
        unrelated.mkdir(parents=True)
        unfinished.mkdir()
        (unfinished / "half.txt").write_text("a crash left this\n")
        code, output = self.run_python(scratch, archive, digest)
        self.assertEqual(0, code, output)
        self.assertFalse(unfinished.exists())
        self.assertTrue(unrelated.is_dir())

    def test_an_archive_that_cannot_be_unpacked_publishes_nothing(self) -> None:
        scratch = Scratch(self)
        broken = scratch.base / "broken.tar.gz"
        broken.write_bytes(b"this is not a tar archive\n" * 50)
        code, _ = self.run_python(scratch, broken, hashlib.sha256(broken.read_bytes()).hexdigest())
        self.assertNotEqual(0, code)
        self.assertEqual([], [path.name for path in (scratch.base / "fresh_python").iterdir()])

    def test_an_archive_with_another_hash_is_refused_and_nothing_is_unpacked(self) -> None:
        scratch = Scratch(self)
        archive, _ = self.archive(scratch.base, f"Python {VERSION}")
        code, output = self.run_python(scratch, archive, "0" * 64)
        self.assertNotEqual(0, code)
        self.assertIn("checksum", output.lower())
        self.assertFalse((scratch.base / "fresh_python" / VERSION / "bin").exists())

    def test_an_interpreter_that_reports_another_version_fails_the_deploy(self) -> None:
        scratch = Scratch(self)
        archive, digest = self.archive(scratch.base, "Python 3.14.0")
        code, output = self.run_python(scratch, archive, digest)
        self.assertNotEqual(0, code)
        self.assertIn(f"Python {VERSION}", output)


if __name__ == "__main__":
    unittest.main()
