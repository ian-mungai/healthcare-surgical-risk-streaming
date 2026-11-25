"""The source manifest of the runner checkout: ``runner_manifest.json``, which the deploy writes in the checkout's root, and the check of a folder against it.

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
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import re
import sys
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
        self.folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="manifest_")))
        for name, text in self.FILES.items():
            self.put(name, text)
        self.put(".venv/bin/python", "environment\n")
        self.put(MARKER, "{}\n")

    def put(self, name: str, text: str) -> None:
        (self.folder / name).parent.mkdir(parents=True, exist_ok=True)
        (self.folder / name).write_text(text)

    def write(self) -> dict[str, Any]:
        text = manifest_module.manifest_text(COMMIT, manifest_module.archived_files(self.folder))
        (self.folder / MANIFEST).write_text(text)
        document: dict[str, Any] = json.loads(text)
        return document


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
        outside = self.folder.parent / "outside.txt"
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
        self.assertEqual([], manifest_module.problems(self.folder, COMMIT))

    def test_a_changed_file_is_found_by_its_name(self) -> None:
        self.write()
        self.put("app/a.py", "A = 2\n")
        self.assertEqual(["changed: app/a.py"], manifest_module.problems(self.folder, COMMIT))

    def test_a_removed_file_is_found_by_its_name(self) -> None:
        self.write()
        (self.folder / "app" / "deep" / "b.py").unlink()
        self.assertEqual(["missing: app/deep/b.py"], manifest_module.problems(self.folder, COMMIT))

    def test_a_file_replaced_by_a_link_with_the_same_bytes_is_found(self) -> None:
        self.write()
        copy = self.folder / "copy.txt"
        copy.write_text("A = 1\n")
        (self.folder / "app" / "a.py").unlink()
        (self.folder / "app" / "a.py").symlink_to(copy)
        self.assertEqual(["not a plain file: app/a.py"], manifest_module.problems(self.folder, COMMIT))

    def test_a_missing_manifest_is_found(self) -> None:
        self.assertEqual(["no manifest"], manifest_module.problems(self.folder, COMMIT))

    def test_a_manifest_that_is_a_link_is_found(self) -> None:
        self.write()
        (self.folder / "real.json").write_text((self.folder / MANIFEST).read_text())
        (self.folder / MANIFEST).unlink()
        (self.folder / MANIFEST).symlink_to(self.folder / "real.json")
        self.assertEqual(["no manifest"], manifest_module.problems(self.folder, COMMIT))

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
                found = manifest_module.problems(self.folder, COMMIT)
                self.assertEqual(1, len(found))
                self.assertRegex(found[0], r"^manifest: ")

    def test_a_manifest_that_is_not_json_or_not_text_is_found(self) -> None:
        for data in (b"", b"{", b"\xff\xfe", b"null"):
            with self.subTest(data=data):
                (self.folder / MANIFEST).write_bytes(data)
                found = manifest_module.problems(self.folder, COMMIT)
                self.assertEqual(1, len(found))
                self.assertRegex(found[0], r"^manifest: ")

    def test_a_path_that_leaves_the_folder_or_is_not_in_the_one_form_is_found_and_the_file_it_names_is_never_read(self) -> None:
        secret = self.folder.parent / "secret.txt"
        secret.write_text("secret\n")
        for path in ("../secret.txt", "/etc/hostname", "./app/a.py", "app//a.py", "app\\a.py", "app/../app/a.py", "", ".venv/bin/python", MARKER, MANIFEST):
            with self.subTest(path=path):
                (self.folder / MANIFEST).write_text(json.dumps({"commit": COMMIT, "files": {path: sha("secret\n")}}))
                found = manifest_module.problems(self.folder, COMMIT)
                self.assertEqual(1, len(found))
                self.assertRegex(found[0], r"^manifest: ")

    def test_the_problems_are_listed_in_the_manifests_order_and_at_most_twenty_with_a_count_of_the_rest(self) -> None:
        for number in range(30):
            self.put(f"many/f{number:02}.txt", "x\n")
        self.write()
        for number in range(30):
            self.put(f"many/f{number:02}.txt", "y\n")
        found = manifest_module.problems(self.folder, COMMIT)
        self.assertEqual(21, len(found))
        self.assertEqual("changed: many/f00.txt", found[0])
        self.assertEqual("and 10 more", found[-1])


class FormTest(unittest.TestCase):
    def test_the_module_is_plain_standard_library_for_the_hosts_python_and_names_its_two_states(self) -> None:
        text = (LIBRARY / "runner_manifest.py").read_text()
        self.assertRegex(text, r"\"write\"")
        self.assertRegex(text, r"\"verify\"")
        imports = set(re.findall(r"(?m)^(?:from|import) ([a-z_.]+)", text))
        self.assertLessEqual(
            {name.split(".")[0] for name in imports},
            {"__future__", "hashlib", "json", "os", "re", "stat", "tempfile", "collections", "typing", "ansible", "pathlib"},
        )


if __name__ == "__main__":
    unittest.main()
