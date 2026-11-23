"""The signal profile acceptance runner resolves both pinned sources before it stages anything, and copies the Compose tree without following a link out of it.

Failure modes covered here:

- The runner copies the Compose tree, or builds the image, before the two pinned sources are resolved, so a source placed inside ``compose/`` is already copied
  when the refusal comes.
- The Compose tree is copied with its links followed (a link in the tree then pulls a file from outside it into the copy), or a link that resolves outside the
  copied tree is not refused.
- The results of the two resolutions are not kept, so the settings are resolved again later and could resolve to something else.
"""

from __future__ import annotations

import ast
import importlib.util
import tempfile
import unittest
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "tests" / "e2e" / "signal_profile.py"

TEMPLATE = """
def check_tree_links(tree):
    top = tree.resolve()
    for path in sorted(tree.rglob("*")):
        if path.is_symlink() and not path.resolve().is_relative_to(top):
            raise RuntimeError("a link of the copied tree leaves it")
"""


PINNED_TEMPLATE = """
def pinned_mounts(mode):
    if mode in LOAD_MODES and mode not in RESOLVED_MOUNTS:
        raise RuntimeError("the pinned sources were not resolved before the run")
    return RESOLVED_MOUNTS.get(mode, [])
"""


def load_function(source: str, name: str, prelude: str = "") -> Any:
    tree = ast.parse(source)
    wanted = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name]
    if len(wanted) != 1:
        raise RuntimeError(f"{name} is not defined exactly once")
    text = "from pathlib import Path\n\n" + prelude + ast.unparse(wanted[0]) + "\n"
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / "under_test.py"
        path.write_text(text)
        spec = importlib.util.spec_from_file_location("under_test", path)
        if spec is None or spec.loader is None:
            raise RuntimeError("cannot load")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return getattr(module, name)


def link_wrong(source: str) -> list[str]:
    """Run ``check_tree_links`` over trees and name each case it gets wrong."""
    wrong: list[str] = []
    check = load_function(source, "check_tree_links")
    with tempfile.TemporaryDirectory() as parent_name:
        parent = Path(parent_name).resolve()
        (parent / "outside").mkdir()
        (parent / "outside" / "secret.csv").write_text("x")
        cases: dict[str, tuple[bool, list[tuple[str, Path | str]]]] = {
            "a tree with no link": (False, []),
            "a link inside the tree": (False, [("inner_link", "services.yml")]),
            "a link to a folder inside the tree": (False, [("folder_link", "sub")]),
            "an absolute link out of the tree": (True, [("out", parent / "outside" / "secret.csv")]),
            "a relative link out of the tree": (True, [("up", "../../outside/secret.csv")]),
            "a link to a folder outside the tree": (True, [("dir_out", parent / "outside")]),
            "a dangling link out of the tree": (True, [("dangling", parent / "outside" / "none.csv")]),
            "a link in a subfolder out of the tree": (True, [("sub/deep_out", parent / "outside" / "secret.csv")]),
        }
        for label, (refused, links) in cases.items():
            tree = parent / "trees" / label.replace(" ", "_")
            (tree / "sub").mkdir(parents=True)
            (tree / "services.yml").write_text("x")
            for name, target in links:
                (tree / name).symlink_to(target)
            try:
                check(tree)
                if refused:
                    wrong.append(f"accepted {label}")
            except RuntimeError as error:
                if not refused:
                    wrong.append(f"refused {label}")
                elif parent_name in str(error) or str(parent) in str(error):
                    wrong.append(f"the refusal of {label} names the path")
    return wrong


def call_lines(function: ast.FunctionDef, name: str) -> list[int]:
    return [
        part.lineno
        for part in ast.walk(function)
        if isinstance(part, ast.Call) and (getattr(part.func, "id", None) == name or getattr(part.func, "attr", None) == name)
    ]


def order_problems(source: str) -> list[str]:
    """``run_local`` resolves both pinned sources before it makes the scratch folder or runs anything, and ``run_prepared`` copies the Compose tree with its
    links kept and checks the copy's links before it creates the stack."""
    tree = ast.parse(source)
    functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
    problems: list[str] = []
    local, prepared = functions.get("run_local"), functions.get("run_prepared")
    if local is None or prepared is None:
        return ["run_local or run_prepared is missing"]
    resolved = call_lines(local, "resolve_pinned_mounts")
    made = call_lines(local, "mkdtemp")
    ran = call_lines(local, "run_prepared")
    if len(resolved) != 1 or not made or not ran or resolved[0] >= min(made) or resolved[0] >= min(ran):
        problems.append("run_local does not resolve both pinned sources before it makes the scratch folder and runs")
    copies = [part for part in ast.walk(prepared) if isinstance(part, ast.Call) and getattr(part.func, "attr", "") == "copytree"]
    if len(copies) != 1:
        problems.append("run_prepared does not copy the Compose tree exactly once")
    elif not any(keyword.arg == "symlinks" and isinstance(keyword.value, ast.Constant) and keyword.value.value is True for keyword in copies[0].keywords):
        problems.append("the Compose tree is copied without keeping its links")
    checks = call_lines(prepared, "check_tree_links")
    stacks = call_lines(prepared, "Stack")
    if len(checks) != 1 or not copies or not stacks or not (copies[0].lineno < checks[0] < min(stacks)):
        problems.append("the links of the copied tree are not checked after the copy and before the stack")
    if call_lines(prepared, "resolve_source") or call_lines(prepared, "mount_for") or call_lines(prepared, "resolve_pinned_mounts"):
        problems.append("run_prepared resolves a pinned source again")
    return problems


ORDER_GOOD = """
def run_local(record, log, cache):
    resolve_pinned_mounts()
    scratch = Path(tempfile.mkdtemp())
    run_prepared(record, log, cache, scratch)


def run_prepared(record, log, cache, scratch):
    shutil.copytree(ROOT / "compose", scratch, symlinks=True, dirs_exist_ok=True)
    check_tree_links(scratch)
    stack = Stack(scratch)
"""


class Links(unittest.TestCase):
    def sources(self) -> list[tuple[str, str]]:
        found = [("template", TEMPLATE)]
        if RUNNER.is_file():
            found.append(("runner", RUNNER.read_text()))
        return found

    def test_a_link_that_leaves_the_copied_tree_is_refused_and_every_other_tree_is_accepted(self) -> None:
        for label, source in self.sources():
            with self.subTest(source=label):
                self.assertEqual([], link_wrong(source))

    def test_each_weaker_check_gets_a_case_wrong(self) -> None:
        for old, new in (
            ("path.is_symlink() and not path.resolve().is_relative_to(top)", "False"),
            ("not path.resolve().is_relative_to(top)", "not path.readlink().is_relative_to(top)"),
            ('tree.rglob("*")', 'tree.glob("*")'),
        ):
            with self.subTest(change=new[:40]):
                self.assertTrue(link_wrong(TEMPLATE.replace(old, new)), "the weaker check got every case right")


class Pinned(unittest.TestCase):
    def wrong(self, source: str) -> list[str]:
        wrong = []
        prelude = "LOAD_MODES = ('load_clinical', 'load_track_list')\nRESOLVED_MOUNTS = {'dump': [], 'load_track_list': [{'x': 1}]}\n\n"
        pinned = load_function(source, "pinned_mounts", prelude)
        try:
            pinned("load_clinical")
            wrong.append("a load whose source was not resolved went ahead")
        except RuntimeError:
            pass
        if pinned("load_track_list") != [{"x": 1}]:
            wrong.append("the kept mount is not returned")
        if pinned("anything_else") != []:
            wrong.append("a mode with no pinned source gets a mount")
        return wrong

    def test_a_load_without_a_resolved_mount_is_refused_and_a_resolved_one_is_returned(self) -> None:
        for label, source in (("template", PINNED_TEMPLATE), *([("runner", RUNNER.read_text())] if RUNNER.is_file() else [])):
            with self.subTest(source=label):
                self.assertEqual([], self.wrong(source))

    def test_each_weaker_version_gets_a_case_wrong(self) -> None:
        for old, new in (
            (
                "    if mode in LOAD_MODES and mode not in RESOLVED_MOUNTS:\n"
                '        raise RuntimeError("the pinned sources were not resolved before the run")\n',
                "",
            ),
            ("mode not in RESOLVED_MOUNTS", "False"),
            ("RESOLVED_MOUNTS.get(mode, [])", "RESOLVED_MOUNTS.get(mode, [{'x': 2}])"),
        ):
            with self.subTest(change=new[:30]):
                self.assertTrue(self.wrong(PINNED_TEMPLATE.replace(old, new)))


class Order(unittest.TestCase):
    def test_the_order_that_resolves_first_and_copies_links_unfollowed_is_accepted(self) -> None:
        self.assertEqual([], order_problems(ORDER_GOOD))

    def test_each_other_order_is_refused(self) -> None:
        for old, new in (
            ("    resolve_pinned_mounts()\n    scratch = Path(tempfile.mkdtemp())\n", "    scratch = Path(tempfile.mkdtemp())\n    resolve_pinned_mounts()\n"),
            ("    resolve_pinned_mounts()\n", ""),
            ("symlinks=True, ", ""),
            ("symlinks=True, ", "symlinks=False, "),
            ("    check_tree_links(scratch)\n", ""),
            ("    check_tree_links(scratch)\n    stack = Stack(scratch)\n", "    stack = Stack(scratch)\n    check_tree_links(scratch)\n"),
            ("    stack = Stack(scratch)\n", "    stack = Stack(scratch)\n    resolve_source(CLINICAL_FILE)\n"),
        ):
            with self.subTest(change=new[:50]):
                self.assertTrue(order_problems(ORDER_GOOD.replace(old, new)))

    @unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
    def test_the_runner_resolves_first_and_copies_links_unfollowed(self) -> None:
        self.assertEqual([], order_problems(RUNNER.read_text()))


if __name__ == "__main__":
    unittest.main()
