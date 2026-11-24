"""The signal profile acceptance runner refuses a selected input that lies anywhere in the checkout, before it reads or opens anything.

The runner is temporary and untracked, so every check that needs it is skipped when it is absent. The guard itself is also held here as a template, so the
weaker versions of it can be tried in a checkout without the runner.

Failure modes covered here:

- The guard looks only at ``compose/``: a selected input hard-linked under ``spark/`` (which Git status reads and the image build copies), under a Git-ignored
  folder or under ``.git`` is not found.
- The guard skips a folder, follows a folder link (a loop or a walk out of the checkout), compares only the inode (another device has the same number), or
  accepts a link to a selected input.
- The guard is given an empty list of sources, or the list of the raw settings instead of the resolved real paths.
- The run goes on after a refusal: the Git state is read, the record is made or a staging or Compose helper runs.
- A refusal names a path.
- The tracked release manifest is opened when the module is imported, before the pin check of the inputs, so a selected input that is the manifest is read.
"""

from __future__ import annotations

import builtins
import contextlib
import importlib
import importlib.util
import io
import os
import tempfile
import unittest
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "tests" / "e2e" / "signal_profile.py"
RELEASE = "tests/fixtures/vitaldb/release.json"

GUARD_TEMPLATE = """
def check_checkout_source(root, sources):
    identities = {(os.stat(source).st_dev, os.stat(source).st_ino) for source in sources}
    if os.path.islink(root / "compose"):
        raise RuntimeError("the compose folder is a link")
    for folder, names, files in os.walk(root, followlinks=False):
        for name in [*names, *files]:
            path = os.path.join(folder, name)
            seen = os.lstat(path)
            if stat.S_ISREG(seen.st_mode) and (seen.st_dev, seen.st_ino) in identities:
                raise RuntimeError("a file of the checkout is a selected input")
            if stat.S_ISLNK(seen.st_mode):
                try:
                    followed = os.stat(path)
                except OSError:
                    continue
                if (followed.st_dev, followed.st_ino) in identities:
                    raise RuntimeError("a link of the checkout leads to a selected input")
"""


def load_guard(source: str) -> Any:
    """The guard function of ``source`` (the template or the runner), built alone with the modules it needs."""
    import ast

    tree = ast.parse(source)
    wanted = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "check_checkout_source"]
    if len(wanted) != 1:
        raise RuntimeError("check_checkout_source is not defined exactly once")
    text = "import os\nimport stat\nfrom pathlib import Path\n\n" + ast.unparse(wanted[0]) + "\n"
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / "guard_under_test.py"
        path.write_text(text)
        spec = importlib.util.spec_from_file_location("guard_under_test", path)
        if spec is None or spec.loader is None:
            raise RuntimeError("cannot load")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module.check_checkout_source


def make_checkout(parent: Path) -> tuple[Path, list[Path]]:
    """A fake checkout with the folders of the real one that matter, and two selected inputs outside it."""
    root = parent / "checkout"
    for folder in ("compose", "spark", "ignored_dir/deep", ".git", "tests/fixtures/vitaldb"):
        (root / folder).mkdir(parents=True)
    for name in ("compose/compose.yml", "spark/Dockerfile", ".git/HEAD", "ignored_dir/deep/a.txt", RELEASE, ".gitignore"):
        (root / name).write_text("x")
    inputs = parent / "inputs"
    inputs.mkdir()
    clinical, track_list = inputs / "clinical.csv", inputs / "trks.csv"
    clinical.write_text("same")
    track_list.write_text("other")
    return root, [clinical, track_list]


def plant(root: Path, kind: str, clinical: Path, track_list: Path, parent: Path) -> None:
    """Plant one alias of a selected input in the checkout."""
    places = {
        "hard_compose": (root / "compose" / "alias.csv", clinical),
        "hard_spark": (root / "spark" / "alias.csv", clinical),
        "hard_spark_second": (root / "spark" / "alias.csv", track_list),
        "hard_ignored": (root / "ignored_dir" / "deep" / "alias.csv", clinical),
        "hard_git": (root / ".git" / "alias", clinical),
        "hard_top": (root / "alias.csv", track_list),
    }
    if kind in places:
        os.link(places[kind][1], places[kind][0])
    elif kind == "link_spark":
        (root / "spark" / "alias.csv").symlink_to(clinical)
    elif kind == "relative_link":
        (root / "spark" / "alias.csv").symlink_to(os.path.relpath(clinical, root / "spark"))
    elif kind == "link_of_link":
        (parent / "hop.csv").symlink_to(clinical)
        (root / "spark" / "alias.csv").symlink_to(parent / "hop.csv")
    elif kind == "same_content":
        (root / "spark" / "copy.csv").write_text("same")
    elif kind == "dangling":
        (root / "spark" / "alias.csv").symlink_to(parent / "none.csv")
    elif kind == "folder_link_to_inputs":
        (root / "spark" / "inputs_link").symlink_to(clinical.parent, target_is_directory=True)
    elif kind == "folder_link_loop":
        (root / "spark" / "loop").symlink_to(root, target_is_directory=True)
    elif kind == "compose_is_link":
        (root / "compose").rename(parent / "moved_compose")
        (root / "compose").symlink_to(parent / "moved_compose", target_is_directory=True)


GUARD_CASES: dict[str, tuple[bool, str]] = {
    "a tree of ordinary files": (False, "none"),
    "a file with the same content as an input": (False, "same_content"),
    "a dangling link": (False, "dangling"),
    "a folder link to the folder of the inputs, which is not entered": (False, "folder_link_to_inputs"),
    "a folder link to the checkout itself, which is not followed": (False, "folder_link_loop"),
    "a hard link under compose": (True, "hard_compose"),
    "a hard link to the clinical file under spark": (True, "hard_spark"),
    "a hard link to the track list under spark": (True, "hard_spark_second"),
    "a hard link under a Git-ignored folder": (True, "hard_ignored"),
    "a hard link under .git": (True, "hard_git"),
    "a hard link at the top": (True, "hard_top"),
    "a link to an input": (True, "link_spark"),
    "a relative link to an input": (True, "relative_link"),
    "a link to a link to an input": (True, "link_of_link"),
    "a compose folder that is a link": (True, "compose_is_link"),
}


def guard_wrong(source: str) -> list[str]:
    """Run the guard over a fresh fake checkout for each case and name each case it gets wrong."""
    wrong: list[str] = []
    check = load_guard(source)
    for label, (refused, kind) in GUARD_CASES.items():
        with tempfile.TemporaryDirectory() as parent_name:
            parent = Path(parent_name).resolve()
            root, (clinical, track_list) = make_checkout(parent)
            plant(root, kind, clinical, track_list, parent)
            try:
                check(root, [str(clinical), str(track_list)])
                if refused:
                    wrong.append(f"accepted {label}")
            except RuntimeError as error:
                if not refused:
                    wrong.append(f"refused {label}")
                elif parent_name in str(error) or str(parent) in str(error):
                    wrong.append(f"the refusal of {label} names the path")
    with tempfile.TemporaryDirectory() as parent_name:
        parent = Path(parent_name).resolve()
        root, (clinical, track_list) = make_checkout(parent)
        os.link(clinical, root / "spark" / "other_device.csv")
        real = os.lstat

        def moved(path: Any, *args: Any, **kwargs: Any) -> os.stat_result:
            seen = real(path, *args, **kwargs)
            if str(path).endswith("other_device.csv"):
                fields = list(seen)
                fields[2] += 1
                return os.stat_result(fields)
            return seen

        with mock.patch("os.lstat", moved):
            try:
                check(root, [str(clinical), str(track_list)])
            except RuntimeError:
                wrong.append("refused a file with the inode of an input on another device")
    return wrong


@contextlib.contextmanager
def recorded_opens() -> Iterator[list[str]]:
    """Every file the code under test opens by name or reads, recorded while the real call goes on."""
    seen: list[str] = []

    def record(argument: Any) -> None:
        if isinstance(argument, (str, bytes, os.PathLike)):
            seen.append(os.fsdecode(argument))

    real_open, real_os_open = builtins.open, os.open
    real_read_text, real_read_bytes, real_path_open = Path.read_text, Path.read_bytes, Path.open

    def wrap_open(file: Any, *args: Any, **kwargs: Any) -> Any:
        record(file)
        return real_open(file, *args, **kwargs)

    def wrap_os_open(path: Any, *args: Any, **kwargs: Any) -> int:
        record(path)
        return real_os_open(path, *args, **kwargs)

    def wrap_read_text(self: Path, *args: Any, **kwargs: Any) -> str:
        record(self)
        return real_read_text(self, *args, **kwargs)

    def wrap_read_bytes(self: Path, *args: Any, **kwargs: Any) -> bytes:
        record(self)
        return real_read_bytes(self, *args, **kwargs)

    def wrap_path_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        record(self)
        return real_path_open(self, *args, **kwargs)

    with (
        mock.patch.object(builtins, "open", wrap_open),
        mock.patch.object(io, "open", wrap_open),
        mock.patch.object(os, "open", wrap_os_open),
        mock.patch.object(Path, "read_text", wrap_read_text),
        mock.patch.object(Path, "read_bytes", wrap_read_bytes),
        mock.patch.object(Path, "open", wrap_path_open),
    ):
        yield seen


def opened_inputs(seen: list[str], inputs: list[Path]) -> list[str]:
    """The recorded opens that are one of the inputs, by file identity."""
    found = []
    for name in seen:
        for item in inputs:
            try:
                if os.path.samefile(name, item):
                    found.append(name)
            except OSError:
                continue
    return found


class GuardTemplate(unittest.TestCase):
    def sources(self) -> list[tuple[str, str]]:
        found = [("template", GUARD_TEMPLATE)]
        if RUNNER.is_file():
            found.append(("runner", RUNNER.read_text()))
        return found

    def test_an_alias_of_a_selected_input_anywhere_in_the_checkout_is_refused_and_every_other_tree_is_accepted(self) -> None:
        for label, source in self.sources():
            with self.subTest(source=label):
                self.assertEqual([], guard_wrong(source))

    def test_each_weaker_guard_gets_a_case_wrong(self) -> None:
        for old, new in (
            ("for folder, names, files in os.walk(root, followlinks=False):", 'for folder, names, files in os.walk(root / "compose", followlinks=False):'),
            ("for folder, names, files in os.walk(root, followlinks=False):", "for folder, names, files in os.walk(root, followlinks=True):"),
            ("[*names, *files]", "[*names, *files][1:]"),
            ("(seen.st_dev, seen.st_ino) in identities", "seen.st_ino in {item[1] for item in identities}"),
            ("(followed.st_dev, followed.st_ino) in identities", "False"),
            ("if stat.S_ISLNK(seen.st_mode):", "if False:"),
            ('    if os.path.islink(root / "compose"):\n        raise RuntimeError("the compose folder is a link")\n', ""),
            ("for source in sources}", "for source in sources[:1]}"),
            ('RuntimeError("a file of the checkout is a selected input")', 'RuntimeError(f"a file {path} is a selected input")'),
            (
                "for folder, names, files in os.walk(root, followlinks=False):",
                "for folder, names, files in os.walk(root, followlinks=False):\n        names[:] = [n for n in names if n not in ('spark',)]",
            ),
            (
                "for folder, names, files in os.walk(root, followlinks=False):",
                "for folder, names, files in os.walk(root, followlinks=False):\n        names[:] = [n for n in names if not n.startswith('.')]",
            ),
        ):
            with self.subTest(change=new[:60]):
                self.assertNotEqual(GUARD_TEMPLATE, GUARD_TEMPLATE.replace(old, new), "the weaker version equals the template")
                self.assertTrue(guard_wrong(GUARD_TEMPLATE.replace(old, new)), "the weaker guard got every case right")


@unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
class RunnerEntry(unittest.TestCase):
    def setUp(self) -> None:
        self.module: ModuleType = importlib.import_module("tests.e2e.signal_profile")
        self.parent_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.parent_directory.cleanup)
        self.parent = Path(self.parent_directory.name).resolve()
        self.root, self.inputs = make_checkout(self.parent)
        self.saved_sources = list(self.module.RESOLVED_SOURCES)
        self.addCleanup(lambda: self.module.RESOLVED_SOURCES.__setitem__(slice(None), self.saved_sources))
        saved_mounts = dict(self.module.RESOLVED_MOUNTS)
        self.addCleanup(self.restore_mounts, saved_mounts)
        for name, value in (
            ("ROOT", self.root),
            ("RELEASE_FILE", self.root / RELEASE),
            ("CLINICAL_FILE", str(self.inputs[0])),
            ("TRACK_LIST_FILE", str(self.inputs[1])),
        ):
            patcher = mock.patch.object(self.module, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def restore_mounts(self, saved: dict[str, Any]) -> None:
        self.module.RESOLVED_MOUNTS.clear()
        self.module.RESOLVED_MOUNTS.update(saved)

    def test_a_selected_input_hard_linked_under_spark_or_a_git_ignored_folder_is_refused_by_the_start(self) -> None:
        for kind in ("hard_spark", "hard_spark_second", "hard_ignored", "hard_git", "hard_compose", "link_spark"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as folder:
                parent = Path(folder).resolve()
                root, inputs = make_checkout(parent)
                plant(root, kind, inputs[0], inputs[1], parent)
                with (
                    mock.patch.object(self.module, "ROOT", root),
                    mock.patch.object(self.module, "CLINICAL_FILE", str(inputs[0])),
                    mock.patch.object(self.module, "TRACK_LIST_FILE", str(inputs[1])),
                ):
                    refused = self.module.sealed_start()
                self.assertIsNotNone(refused, "the start accepted an alias of a selected input")
                self.assertNotIn(folder, str(refused))
                self.assertNotIn(str(parent), str(refused))

    def test_a_clean_checkout_is_accepted_and_the_guard_gets_the_resolved_sources_and_the_whole_root(self) -> None:
        calls: list[tuple[Path, list[str]]] = []
        real = self.module.check_checkout_source

        def spy(root: Path, sources: list[str]) -> None:
            calls.append((root, list(sources)))
            real(root, sources)

        with mock.patch.object(self.module, "check_checkout_source", spy):
            self.assertIsNone(self.module.sealed_start())
        self.assertEqual([(self.root, [str(self.inputs[0]), str(self.inputs[1])])], calls)
        self.assertTrue(all(os.path.isfile(item) for item in calls[0][1]))

    def test_a_planted_alias_is_refused_through_the_resolved_list_not_an_empty_one(self) -> None:
        plant(self.root, "hard_spark", self.inputs[0], self.inputs[1], self.parent)
        seen: list[list[str]] = []
        real = self.module.check_checkout_source

        def spy(root: Path, sources: list[str]) -> None:
            seen.append(list(sources))
            real(root, sources)

        with mock.patch.object(self.module, "check_checkout_source", spy):
            self.assertIsNotNone(self.module.sealed_start())
        self.assertEqual([[str(self.inputs[0]), str(self.inputs[1])]], seen)

    def test_a_selected_input_that_is_the_release_manifest_is_refused_and_no_input_is_opened(self) -> None:
        os.link(self.root / RELEASE, self.parent / "inputs" / "manifest_alias.csv")
        with (
            mock.patch.object(self.module, "CLINICAL_FILE", str(self.parent / "inputs" / "manifest_alias.csv")),
            recorded_opens() as seen,
        ):
            refused = self.module.sealed_start()
        self.assertIsNotNone(refused, "the manifest was accepted as a selected input")
        self.assertNotIn(str(self.parent), str(refused))
        self.assertEqual([], opened_inputs(seen, [self.root / RELEASE, *self.inputs]))

    def test_the_start_opens_neither_input_when_it_accepts_or_refuses(self) -> None:
        with recorded_opens() as seen:
            self.assertIsNone(self.module.sealed_start())
        self.assertEqual([], opened_inputs(seen, self.inputs))
        plant(self.root, "hard_spark", self.inputs[0], self.inputs[1], self.parent)
        with recorded_opens() as seen:
            self.assertIsNotNone(self.module.sealed_start())
        self.assertEqual([], opened_inputs(seen, self.inputs))

    def helpers(self) -> list[str]:
        return [
            name
            for name in ("recorded_state", "Record", "run_local", "run_prepared", "check_tree_links", "Stack", "preflight", "place_nessie_password", "finish")
            if hasattr(self.module, name)
        ]

    def test_main_returns_2_with_no_path_and_calls_no_later_step_after_a_refusal(self) -> None:
        names = self.helpers()
        self.assertGreaterEqual(len(names), 7, "a staging or Compose helper is missing from the runner")
        plant(self.root, "hard_spark", self.inputs[0], self.inputs[1], self.parent)
        called: list[str] = []

        def failing(name: str) -> Any:
            def call(*args: Any, **kwargs: Any) -> Any:
                called.append(name)
                raise AssertionError(f"{name} was called after a refusal")

            return call

        patches = [mock.patch.object(self.module, name, failing(name)) for name in names]
        patches += [mock.patch.object(self.module.shutil, name, failing(f"shutil.{name}")) for name in ("copytree", "copyfile", "copy2")]
        patches += [mock.patch.object(self.module, "run_command", failing("run_command"))]
        errors = io.StringIO()
        with contextlib.ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            with contextlib.redirect_stderr(errors):
                code = self.module.main()
        self.assertEqual(2, code)
        self.assertEqual([], called)
        self.assertNotIn(str(self.parent), errors.getvalue())
        self.assertNotIn(str(self.root), errors.getvalue())
        self.assertIn("refused", errors.getvalue())

    def test_main_goes_on_to_the_git_state_when_the_checkout_is_clean(self) -> None:
        class Reached(Exception):
            pass

        def reached(*args: Any, **kwargs: Any) -> Any:
            raise Reached

        with (
            mock.patch.object(self.module, "recorded_state", reached),
            mock.patch.object(self.module, "Record", mock.MagicMock()),
            self.assertRaises(Reached),
        ):
            self.module.main()


@unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
class RunnerImport(unittest.TestCase):
    def test_importing_the_runner_opens_no_file_of_a_selected_input_even_the_release_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            parent = Path(folder).resolve()
            manifest = ROOT / RELEASE
            alias = parent / "manifest_alias.csv"
            other = parent / "other.csv"
            other.write_text("x")
            try:
                os.link(manifest, alias)
            except OSError:
                self.skipTest("a hard link to the manifest cannot be made here")
            spec = importlib.util.spec_from_file_location("signal_profile_fresh", RUNNER)
            if spec is None or spec.loader is None:
                self.fail("the runner cannot be loaded")
            module = importlib.util.module_from_spec(spec)
            with mock.patch.dict(os.environ, {"CLINICAL_FILE": str(alias), "TRACK_LIST_FILE": str(other)}), recorded_opens() as seen:
                spec.loader.exec_module(module)
            self.assertEqual([], opened_inputs(seen, [alias, other]))
            self.assertFalse(hasattr(module, "PINS"), "the pins are read at import")
            with mock.patch.object(module, "ROOT", ROOT), recorded_opens() as seen:
                refused = module.sealed_start()
            self.assertIsNotNone(refused, "the manifest was accepted as a selected input")
            self.assertEqual([], opened_inputs(seen, [alias, other]))


if __name__ == "__main__":
    unittest.main()
