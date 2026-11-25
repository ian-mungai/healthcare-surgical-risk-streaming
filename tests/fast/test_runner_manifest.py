"""The source manifest of the runner checkout: ``runner_manifest.json``, which the deploy writes in the checkout's root, and the check of a folder against the
archive that the controller read.

The streaming host runner reads it to prove where its code came from. Its form is exactly ``{"commit": "<40 hex>", "files": {"<path>": "<sha256>"}}``, one entry
for each file that ``git archive`` placed (never ``.venv``, the finished marker, the manifest itself or a runner that the launcher placed later). The module is
``ansible/roles/runner_checkout/library/runner_manifest.py``; ``tests/slow/test_runner_checkout_role.py`` runs it through the role.

Failure modes covered here:

- A file that the archive placed is missing from the manifest, so a changed copy of it is never noticed.
- A hash that does not match the file is accepted, or the manifest holds a hash of anything but the file's bytes.
- ``.venv``, the marker, the manifest itself or a placed runner is listed, so the manifest changes with the environment or with each placement.
- A folder that cannot be read is skipped, so its files are missing from the manifest and a change to them is never noticed.
- A link or another special file is hashed as if it were a plain file, so a link can lead to different content later.
- A changed, a removed or a replaced (by a link) file passes the check.
- A manifest that is missing, is not JSON, has another form, names another commit or names a path outside the folder is read as good.
- The form drifts: a key besides ``commit`` and ``files``, a path with ``./`` or a backslash, an upper-case or short hash.
- The expected files are read from the host's own manifest, so an empty or a partial ``files`` map passes: the controller's archive gives the map.
- The controller's archive holds a link, a device, a pipe, a path outside the folder or a name twice, and the host places what the controller never read.
- The host unpacks something other than the archive that the controller read, and the manifest is written from what is there.
"""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
import re
import sys
import tarfile
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any

from tests.fast.ansible_paths import ROLES

LIBRARY = ROLES / "runner_checkout" / "library"
COMMIT = "c" * 40
MARKER = ".runner_checkout.json"
MANIFEST = "runner_manifest.json"


def load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("runner_manifest", LIBRARY / "runner_manifest.py")
    if spec is None or spec.loader is None:
        raise ImportError("runner_manifest")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


manifest_module: Any = load_module()


def sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


class Folder(unittest.TestCase):
    """A temporary checkout folder: what ``git archive`` placed, plus the entries that the deploy adds around it."""

    FILES = {
        "requirements.txt": "pin==1\n",
        "app/a.py": "A = 1\n",
        "app/deep/b.py": "B = 2\n",
        "empty.txt": "",
        "app/.venv/not_the_environment.py": "X = 1\n",
    }

    def setUp(self) -> None:
        # The checkout and every file that a test puts beside it live in one temporary directory that this test owns, so nothing is written to a shared place.
        self.base = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="manifest_")))
        self.folder = self.base / "checkout"
        self.folder.mkdir()
        for name, text in self.FILES.items():
            self.put(name, text)
        self.put(".venv/bin/python", "environment\n")
        self.put(MARKER, "{}\n")
        self.expected = manifest_module.archived_files(self.folder)

    def put(self, name: str, text: str) -> None:
        (self.folder / name).parent.mkdir(parents=True, exist_ok=True)
        (self.folder / name).write_text(text)

    def write(self) -> dict[str, Any]:
        """Write the manifest of what the folder holds and keep that map as the files that the controller sent."""
        self.expected = manifest_module.archived_files(self.folder)
        text = manifest_module.manifest_text(COMMIT, self.expected)
        (self.folder / MANIFEST).write_text(text)
        document: dict[str, Any] = json.loads(text)
        return document

    def problems(self) -> list[str]:
        found: list[str] = manifest_module.problems(self.folder, COMMIT, self.expected)
        return found


class OwnedDirectoryTest(Folder):
    def test_the_checkout_is_the_only_entry_of_the_temporary_directory_that_the_test_owns_and_a_sibling_stays_inside_it(self) -> None:
        self.assertEqual(["checkout"], sorted(path.name for path in self.base.iterdir()))
        self.assertTrue(self.base.name.startswith("manifest_"))
        self.assertNotEqual(Path(tempfile.gettempdir()).resolve(), self.folder.parent.resolve(), "a sibling would be written to the shared temporary folder")
        sibling = self.base / "outside.txt"
        sibling.write_text("outside\n")
        self.assertEqual(self.base.resolve(), sibling.resolve().parent)


class ContentTest(Folder):
    def test_it_lists_every_file_of_the_folder_with_the_sha256_of_its_bytes(self) -> None:
        files = manifest_module.archived_files(self.folder)
        self.assertEqual({name: sha(text) for name, text in self.FILES.items()}, files)

    def test_it_never_lists_the_environment_the_marker_the_manifest_or_a_runner_placed_later_than_the_archive(self) -> None:
        # The deploy writes the manifest before the marker and before any placement; the exclusions keep it the same when it is written again.
        self.put(MANIFEST, "{}\n")
        files = manifest_module.archived_files(self.folder)
        self.assertEqual(set(self.FILES), set(files))
        for refused in (".venv/bin/python", MARKER, MANIFEST):
            self.assertNotIn(refused, files)

    def test_a_folder_named_venv_below_the_top_is_archived_content_and_is_listed(self) -> None:
        self.assertIn("app/.venv/not_the_environment.py", manifest_module.archived_files(self.folder))

    def test_the_text_is_exactly_the_commit_and_the_files_in_a_fixed_order_with_one_final_newline(self) -> None:
        files = manifest_module.archived_files(self.folder)
        text = manifest_module.manifest_text(COMMIT, files)
        document = json.loads(text)
        self.assertEqual(["commit", "files"], sorted(document))
        self.assertEqual({"commit": COMMIT, "files": files}, document)
        self.assertEqual(sorted(files), list(document["files"]))
        self.assertTrue(text.endswith("}\n") and not text.endswith("\n\n"))
        self.assertEqual(text, manifest_module.manifest_text(COMMIT, dict(reversed(list(files.items())))), "the same files give the same text in any order")

    def test_every_path_is_relative_with_forward_slashes_and_every_hash_is_64_lower_case_hexadecimal_digits(self) -> None:
        for path, digest in manifest_module.archived_files(self.folder).items():
            with self.subTest(path=path):
                self.assertFalse(path.startswith(("/", "./")) or ".." in path.split("/") or "\\" in path)
                self.assertRegex(digest, r"^[0-9a-f]{64}$")

    def test_a_link_to_a_file_or_a_folder_and_a_special_file_are_refused_by_name(self) -> None:
        outside = self.base / "outside.txt"
        outside.write_text("outside\n")
        cases: dict[str, Callable[[Path], object]] = {
            "link_to_file": lambda path: path.symlink_to(outside),
            "link_to_folder": lambda path: path.symlink_to(self.folder / "app"),
            "pipe": lambda path: os.mkfifo(path),
        }
        for name, make in cases.items():
            with self.subTest(case=name):
                make(self.folder / name)
                with self.assertRaisesRegex(manifest_module.ManifestError, name):
                    manifest_module.archived_files(self.folder)
                (self.folder / name).unlink()

    def test_a_folder_that_cannot_be_read_stops_the_listing_and_is_not_skipped(self) -> None:
        closed = self.folder / "app" / "deep"
        closed.chmod(0)
        self.addCleanup(closed.chmod, 0o700)
        if os.access(closed, os.R_OK):
            self.skipTest("this account reads every folder")
        with self.assertRaises(PermissionError):
            manifest_module.archived_files(self.folder)

    def test_a_commit_that_is_not_forty_lower_case_hexadecimal_digits_is_refused(self) -> None:
        for commit in ("", "main", "C" * 40, "c" * 39, "c" * 41, f"{COMMIT}\n"):
            with self.subTest(commit=commit), self.assertRaisesRegex(manifest_module.ManifestError, "commit"):
                manifest_module.manifest_text(commit, {})


class CheckTest(Folder):
    def test_a_folder_that_is_as_the_archive_made_it_has_no_problem_even_with_the_environment_and_a_placed_runner_added(self) -> None:
        self.write()
        self.put("tests/e2e/silver_signals.py", "PLACED = 1\n")
        self.put(".venv/lib/more.py", "more\n")
        self.assertEqual([], self.problems())

    def test_a_changed_file_is_found_by_its_name(self) -> None:
        self.write()
        self.put("app/a.py", "A = 2\n")
        self.assertEqual(["changed: app/a.py"], self.problems())

    def test_a_removed_file_is_found_by_its_name(self) -> None:
        self.write()
        (self.folder / "app" / "deep" / "b.py").unlink()
        self.assertEqual(["missing: app/deep/b.py"], self.problems())

    def test_a_file_replaced_by_a_link_with_the_same_bytes_is_found(self) -> None:
        self.write()
        copy = self.folder / "copy.txt"
        copy.write_text("A = 1\n")
        (self.folder / "app" / "a.py").unlink()
        (self.folder / "app" / "a.py").symlink_to(copy)
        self.assertEqual(["not a plain file: app/a.py"], self.problems())

    def test_a_missing_manifest_is_found(self) -> None:
        self.assertEqual(["no manifest"], self.problems())

    def test_a_manifest_that_is_a_link_is_found(self) -> None:
        self.write()
        (self.folder / "real.json").write_text((self.folder / MANIFEST).read_text())
        (self.folder / MANIFEST).unlink()
        (self.folder / MANIFEST).symlink_to(self.folder / "real.json")
        self.assertEqual(["no manifest"], self.problems())

    def test_a_manifest_of_another_commit_or_another_form_is_found(self) -> None:
        good = self.write()
        files = good["files"]
        forms: dict[str, Any] = {
            "another commit": {"commit": "d" * 40, "files": files},
            "extra key": {"commit": COMMIT, "files": files, "more": 1},
            "no files key": {"commit": COMMIT},
            "files is a list": {"commit": COMMIT, "files": list(files)},
            "short hash": {"commit": COMMIT, "files": {"app/a.py": "ab"}},
            "upper-case hash": {"commit": COMMIT, "files": {"app/a.py": "A" * 64}},
            "hash is a number": {"commit": COMMIT, "files": {"app/a.py": 1}},
            "not an object": [COMMIT],
        }
        for name, document in forms.items():
            with self.subTest(form=name):
                (self.folder / MANIFEST).write_text(json.dumps(document))
                found = self.problems()
                self.assertEqual(1, len(found))
                self.assertRegex(found[0], r"^manifest: ")

    def test_a_manifest_that_is_not_json_or_not_text_is_found(self) -> None:
        for data in (b"", b"{", b"\xff\xfe", b"null"):
            with self.subTest(data=data):
                (self.folder / MANIFEST).write_bytes(data)
                found = self.problems()
                self.assertEqual(1, len(found))
                self.assertRegex(found[0], r"^manifest: ")

    def test_a_path_that_leaves_the_folder_or_is_not_in_the_one_form_is_found_and_the_file_it_names_is_never_read(self) -> None:
        secret = self.base / "secret.txt"
        secret.write_text("secret\n")
        for path in ("../secret.txt", "/etc/hostname", "./app/a.py", "app//a.py", "app\\a.py", "app/../app/a.py", "", ".venv/bin/python", MARKER, MANIFEST):
            with self.subTest(path=path):
                (self.folder / MANIFEST).write_text(json.dumps({"commit": COMMIT, "files": {path: sha("secret\n")}}))
                found = self.problems()
                self.assertEqual(1, len(found))
                self.assertRegex(found[0], r"^manifest: ")

    def test_the_problems_are_listed_in_the_manifests_order_and_at_most_twenty_with_a_count_of_the_rest(self) -> None:
        for number in range(30):
            self.put(f"many/f{number:02}.txt", "x\n")
        self.write()
        for number in range(30):
            self.put(f"many/f{number:02}.txt", "y\n")
        found = self.problems()
        self.assertEqual(21, len(found))
        self.assertEqual("changed: many/f00.txt", found[0])
        self.assertEqual("and 10 more", found[-1])


class ExactMatchTest(Folder):
    """The manifest and the folder are checked against the files that the controller sent, never against what the host holds."""

    def test_a_manifest_with_no_file_or_with_a_part_of_the_files_is_not_the_archive_and_is_found(self) -> None:
        self.write()
        (self.folder / MANIFEST).write_text(json.dumps({"commit": COMMIT, "files": {}}))
        self.assertEqual(["manifest: the files are not a map with at least one file"], self.problems())
        part = dict(list(self.expected.items())[:2])
        (self.folder / MANIFEST).write_text(json.dumps({"commit": COMMIT, "files": part}))
        found = self.problems()
        self.assertEqual(len(self.expected) - 2, len([line for line in found if line.startswith("manifest lacks: ")]), found)
        self.assertNotIn("changed", " ".join(found))

    def test_a_manifest_with_an_extra_file_or_a_hash_that_the_archive_does_not_have_is_found(self) -> None:
        self.write()
        for name, files in {
            "extra file": {**self.expected, "extra.py": "1" * 64},
            "other hash": {**self.expected, "app/a.py": "2" * 64},
        }.items():
            with self.subTest(case=name):
                (self.folder / MANIFEST).write_text(json.dumps({"commit": COMMIT, "files": files}))
                found = self.problems()
                self.assertEqual(1, len(found), found)
                self.assertRegex(found[0], r"^manifest has (an extra file: extra\.py|a different hash: app/a\.py)$")

    def test_a_manifest_that_was_edited_to_fit_a_changed_file_does_not_hide_the_change_from_the_controllers_files(self) -> None:
        self.write()
        self.put("app/a.py", "A = 2\n")
        edited = {**self.expected, "app/a.py": sha("A = 2\n")}
        (self.folder / MANIFEST).write_text(manifest_module.manifest_text(COMMIT, edited))
        self.assertEqual(["manifest has a different hash: app/a.py"], self.problems())

    def test_no_files_from_the_controller_is_a_problem_and_never_an_intact_folder(self) -> None:
        self.write()
        cases: tuple[object, ...] = ({}, [], None, {"app/a.py": "short"}, {"../x": "1" * 64})
        for expected in cases:
            with self.subTest(expected=expected):
                found = manifest_module.problems(self.folder, COMMIT, expected)
                self.assertEqual(1, len(found))
                self.assertRegex(found[0], r"^expected files: ")


class FakeModule:
    """The few calls of AnsibleModule that the writer makes, on the real file system."""

    params: dict[str, object] = {}

    def atomic_move(self, source: str, destination: str) -> None:
        os.replace(source, destination)

    def load_file_common_arguments(self, params: object, path: str) -> dict[str, str]:
        return {"path": path}

    def set_fs_attributes_if_different(self, arguments: object, changed: bool) -> bool:
        return changed


class WriteTest(Folder):
    def test_the_manifest_is_written_from_the_controllers_files_after_the_folder_was_found_to_hold_exactly_them(self) -> None:
        result = manifest_module.write_manifest(FakeModule(), self.folder, COMMIT, self.expected)
        self.assertTrue(result["changed"])
        self.assertEqual(manifest_module.manifest_text(COMMIT, self.expected), (self.folder / MANIFEST).read_text())
        self.assertFalse(manifest_module.write_manifest(FakeModule(), self.folder, COMMIT, self.expected)["changed"])

    def test_a_folder_that_holds_a_changed_an_extra_or_a_missing_file_is_refused_and_no_manifest_is_written(self) -> None:
        cases: dict[str, Callable[[], object]] = {
            "changed": lambda: self.put("app/a.py", "A = 2\n"),
            "extra": lambda: self.put("extra.py", "E = 1\n"),
            "missing": lambda: (self.folder / "app" / "a.py").unlink(),
        }
        for name, make in cases.items():
            with self.subTest(case=name):
                self.put("app/a.py", "A = 1\n")
                (self.folder / "extra.py").unlink(missing_ok=True)
                make()
                with self.assertRaisesRegex(manifest_module.ManifestError, "not the archive that the controller read"):
                    manifest_module.write_manifest(FakeModule(), self.folder, COMMIT, self.expected)
                self.assertFalse((self.folder / MANIFEST).exists())

    def test_files_that_the_controller_did_not_send_in_a_usable_form_are_refused(self) -> None:
        for expected in ({}, {"app/a.py": "x"}, {"../a": "1" * 64}):
            with self.subTest(expected=expected), self.assertRaisesRegex(manifest_module.ManifestError, "not usable"):
                manifest_module.write_manifest(FakeModule(), self.folder, COMMIT, expected)


class ArchiveTest(Folder):
    """The archive that ``git archive`` made on the controller, read before the host changes."""

    def tar(self, members: dict[str, bytes | str | None], name: str = "runner.tar") -> Path:
        """A tar file in the test's own directory: a ``bytes`` value is a file, ``None`` is a folder and a ``str`` is a link to that target."""
        path = self.base / name
        with tarfile.open(path, "w") as archive:
            for member, value in members.items():
                info = tarfile.TarInfo(member)
                if value is None:
                    info.type = tarfile.DIRTYPE
                    archive.addfile(info)
                elif isinstance(value, str):
                    info.type, info.linkname = tarfile.SYMTYPE, value
                    archive.addfile(info)
                else:
                    info.size = len(value)
                    archive.addfile(info, io.BytesIO(value))
        return path

    def test_the_map_is_the_sha256_of_every_file_in_sorted_order_and_folders_are_not_files(self) -> None:
        path = self.tar({"app": None, "app/b.py": b"B\n", "app/a.py": b"A\n", "empty.txt": b"", "top": None})
        files = manifest_module.archive_files(path)
        self.assertEqual({"app/a.py": sha("A\n"), "app/b.py": sha("B\n"), "empty.txt": sha("")}, files)
        self.assertEqual(sorted(files), list(files))

    def test_it_is_the_map_that_the_unpacked_folder_gives(self) -> None:
        path = self.tar({"app": None, "app/a.py": b"A = 1\n", "requirements.txt": b"pin==1\n"})
        with tarfile.open(path) as archive:
            archive.extractall(self.base / "unpacked", filter="data")
        self.assertEqual(manifest_module.archive_files(path), manifest_module.archived_files(self.base / "unpacked"))

    def test_a_link_a_device_a_pipe_a_path_outside_the_folder_and_a_name_that_comes_twice_are_refused_by_name(self) -> None:
        cases: dict[str, tuple[dict[str, bytes | str | None], str]] = {
            "link": ({"a.py": b"A\n", "link.py": "a.py"}, "link.py"),
            "outside": ({"../x.py": b"X\n"}, r"\.\./x\.py"),
            "absolute": ({"/etc/x.py": b"X\n"}, "/etc/x.py"),
            "environment": ({".venv/bin/python": b"X\n"}, r"\.venv/bin/python"),
            "marker": ({MARKER: b"{}\n"}, MARKER),
            "manifest": ({MANIFEST: b"{}\n"}, MANIFEST),
            "backslash": ({"a\\b.py": b"X\n"}, "b.py"),
        }
        for name, (members, shown) in cases.items():
            with self.subTest(case=name), self.assertRaisesRegex(manifest_module.ManifestError, shown):
                manifest_module.archive_files(self.tar(members))

    def test_a_pipe_a_device_and_a_second_entry_of_a_name_are_refused(self) -> None:
        path = self.base / "special.tar"
        with tarfile.open(path, "w") as archive:
            info = tarfile.TarInfo("pipe")
            info.type = tarfile.FIFOTYPE
            archive.addfile(info)
        with self.assertRaisesRegex(manifest_module.ManifestError, "pipe is a link or a special file"):
            manifest_module.archive_files(path)
        twice = self.base / "twice.tar"
        with tarfile.open(twice, "w") as archive:
            for text in (b"one\n", b"two\n"):
                info = tarfile.TarInfo("same.py")
                info.size = len(text)
                archive.addfile(info, io.BytesIO(text))
        with self.assertRaisesRegex(manifest_module.ManifestError, "same.py is in the archive twice"):
            manifest_module.archive_files(twice)

    def test_an_archive_with_no_file_and_a_file_that_is_not_a_tar_are_refused(self) -> None:
        with self.assertRaisesRegex(manifest_module.ManifestError, "no file"):
            manifest_module.archive_files(self.tar({"folder": None}))
        (self.base / "not.tar").write_bytes(b"not a tar file at all\n" * 100)
        with self.assertRaises(tarfile.TarError):
            manifest_module.archive_files(self.base / "not.tar")


class FormTest(unittest.TestCase):
    def test_the_module_is_plain_standard_library_for_the_hosts_python_and_names_its_three_states(self) -> None:
        text = (LIBRARY / "runner_manifest.py").read_text()
        for state in ("archive", "write", "verify"):
            self.assertRegex(text, rf"\"{state}\"")
        imports = set(re.findall(r"(?m)^(?:from|import) ([a-z_.]+)", text))
        self.assertLessEqual(
            {name.split(".")[0] for name in imports},
            {"__future__", "hashlib", "json", "os", "re", "stat", "tarfile", "tempfile", "collections", "typing", "ansible", "pathlib"},
        )


if __name__ == "__main__":
    unittest.main()
