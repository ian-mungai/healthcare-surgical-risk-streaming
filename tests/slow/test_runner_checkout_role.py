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
import stat
import sys
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path

from scripts.process import CompletedProcess, run_command
from tests.fast.ansible_paths import GROUP_VARIABLES
from tests.slow.ansible_support import ROLES, environment

ROLE = ROLES / "runner_checkout"
OWNER = pwd.getpwuid(os.getuid()).pw_name
GROUP = grp.getgrgid(os.getgid()).gr_name
VERSION = ".".join(str(part) for part in sys.version_info[:3])
MARKER = ".runner_checkout.json"
MANIFEST = "runner_manifest.json"
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
        interpreter = self.python_root / VERSION / "bin" / f"python{VERSION.rsplit('.', 1)[0]}"
        interpreter.parent.mkdir(parents=True)
        interpreter.symlink_to(Path(sys.executable))

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
        } | more

    def run(self, commit: str, tasks_from: str = "main", **more: object) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as scratch:
            work = Path(scratch)
            playbook = work / "check.yml"
            includes = f"    - ansible.builtin.include_role:\n        name: {ROLE}\n        tasks_from: {tasks_from}\n"
            header = f"- hosts: localhost\n  connection: local\n  gather_facts: false\n  vars_files: [{GROUP_VARIABLES}]\n  tasks:\n"
            playbook.write_text(header + includes)
            env = environment(work) | {"PIP_NO_INDEX": "1", "PIP_FIND_LINKS": str(self.wheels), "PIP_CACHE_DIR": str(work / "pip"), "HOME": str(work)}
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


class InterpreterTest(unittest.TestCase):
    def archive(self, folder: Path, version_line: str) -> tuple[Path, str]:
        data = io.BytesIO()
        script = f"#!/bin/sh\necho '{version_line}'\n".encode()
        with tarfile.open(fileobj=data, mode="w:gz") as archive:
            info = tarfile.TarInfo(f"python/bin/python{VERSION.rsplit('.', 1)[0]}")
            info.size, info.mode = len(script), 0o755
            archive.addfile(info, io.BytesIO(script))
        path = folder / "python.tar.gz"
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
