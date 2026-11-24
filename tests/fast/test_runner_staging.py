"""The signal profile acceptance runner resolves both pinned sources before it stages or reads anything and copies the Compose tree with its links kept.

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


RESOLVE_TEMPLATE = """
def resolve_pinned_mounts():
    RESOLVED_MOUNTS["load_clinical"] = [mount_for(CLINICAL_FILE, CLINICAL_TARGET)]
    RESOLVED_MOUNTS["load_track_list"] = [mount_for(TRACK_LIST_FILE, TRACK_LIST_TARGET)]
    RESOLVED_SOURCES[:] = [mount["source"].replace("$$", "$") for mounts in RESOLVED_MOUNTS.values() for mount in mounts]
    return RESOLVED_MOUNTS
"""
RESOLVE_PRELUDE = (
    "CLINICAL_FILE, TRACK_LIST_FILE = '/a/clinical-$X.csv', '/b/trks.csv'\nCLINICAL_TARGET, TRACK_LIST_TARGET = '/t1', '/t2'\n"
    "RESOLVED_MOUNTS = {}\nRESOLVED_SOURCES = []\n"
    "def mount_for(path, target):\n    return {'source': path.replace('$', '$$'), 'target': target}\n\n"
)


def load_function(source: str, name: str, prelude: str = "", *, module_vars: bool = False) -> Any:
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
    return vars(module) if module_vars else getattr(module, name)


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
    """``main`` stops at a refusal of ``sealed_start`` before anything else, ``sealed_start`` resolves both pinned sources and then checks the whole checkout
    against the resolved list, nothing else resolves or checks them, the pins are not read at import, and ``run_prepared`` copies the Compose tree with its
    links kept and checks the copy's links before it creates the stack."""
    tree = ast.parse(source)
    functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
    problems: list[str] = []
    local, prepared = functions.get("run_local"), functions.get("run_prepared")
    if local is None or prepared is None:
        return ["run_local or run_prepared is missing"]
    for name, function in (("run_local", local), ("run_prepared", prepared)):
        if call_lines(function, "resolve_pinned_mounts") or call_lines(function, "check_checkout_source"):
            problems.append(f"{name} resolves or checks the sources again: that is done once, at the entry")
    entry, start = functions.get("main"), functions.get("sealed_start")
    if entry is None or start is None:
        return [*problems, "main or sealed_start is missing"]
    started = call_lines(entry, "sealed_start")
    recorded = call_lines(entry, "recorded_state") + call_lines(entry, "Record") + call_lines(entry, "run_local")
    if len(started) != 1 or not recorded:
        return [*problems, "main does not resolve and check the sources once and then record the Git state, make the record and run"]
    if started[0] >= min(recorded):
        problems.append("main does not resolve and check the sources before it records the Git state, makes the record or runs")
    stops = [
        node
        for node in entry.body
        if isinstance(node, ast.If) and node.body and isinstance(node.body[-1], ast.Return) and node.lineno > started[0] - 1 and node.lineno <= min(recorded)
    ]
    if len(stops) != 1 or ast.unparse(stops[0].test) != "refused is not None":
        problems.append("main does not return at once when sealed_start refuses")
    resolved, checked = call_lines(start, "resolve_pinned_mounts"), call_lines(start, "check_checkout_source")
    if len(resolved) != 1 or len(checked) != 1 or resolved[0] >= checked[0]:
        problems.append("sealed_start does not resolve both sources and then check the checkout, once each")
    calls = [part for part in ast.walk(start) if isinstance(part, ast.Call) and getattr(part.func, "id", None) == "check_checkout_source"]
    if len(calls) != 1 or [ast.unparse(argument) for argument in calls[0].args] != ["ROOT", "RESOLVED_SOURCES"]:
        problems.append("the checkout guard is not given the repository root and the resolved list of sources")
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
    pins = functions.get("release_pins")
    if pins is None or not any(ast.unparse(decorator) in ("functools.cache", "cache") for decorator in pins.decorator_list):
        problems.append("release_pins is not cached")
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.Import, ast.ImportFrom)):
            continue
        if any(isinstance(part, ast.Name) and part.id in ("PINS", "release_pins") for part in ast.walk(node)):
            problems.append("the pins are read or named at import, before the pin check of the inputs")
    return problems


ORDER_GOOD = """
import functools


@functools.cache
def release_pins():
    return json.loads(RELEASE_FILE.read_text())


def sealed_start():
    try:
        resolve_pinned_mounts()
        check_checkout_source(ROOT, RESOLVED_SOURCES)
    except RuntimeError as error:
        return str(error)
    return None


def main():
    refused = sealed_start()
    if refused is not None:
        sys.stderr.write(refused)
        return 2
    record = Record()
    record.revision = recorded_state(ROOT)
    run_local(record, [], cache)


def run_local(record, log, cache):
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


class SelectedInputs(unittest.TestCase):
    def wrong(self, source: str) -> list[str]:
        namespace = load_function(source, "resolve_pinned_mounts", RESOLVE_PRELUDE, module_vars=True)
        wrong = []
        mounts = namespace["resolve_pinned_mounts"]()
        if sorted(mounts) != ["load_clinical", "load_track_list"]:
            wrong.append("a load has no mount")
        if namespace["RESOLVED_SOURCES"] != ["/a/clinical-$X.csv", "/b/trks.csv"]:
            wrong.append("the selected inputs are not the real paths with their dollars undoubled")
        return wrong

    def test_both_selected_inputs_are_known_to_the_checkout_check_by_their_real_paths(self) -> None:
        for label, source in (("template", RESOLVE_TEMPLATE), *([("runner", RUNNER.read_text())] if RUNNER.is_file() else [])):
            with self.subTest(source=label):
                self.assertEqual([], self.wrong(source))

    def test_each_weaker_version_gets_a_case_wrong(self) -> None:
        for old, new in (
            ('    RESOLVED_SOURCES[:] = [mount["source"].replace("$$", "$") for mounts in RESOLVED_MOUNTS.values() for mount in mounts]\n', ""),
            ('RESOLVED_SOURCES[:] = [mount["source"].replace("$$", "$")', 'RESOLVED_SOURCES[:] = [mount["source"]'),
            ("for mounts in RESOLVED_MOUNTS.values() for mount in mounts]", "for mounts in [RESOLVED_MOUNTS['load_clinical']] for mount in mounts]"),
            ("RESOLVED_SOURCES[:] =", "x ="),
        ):
            with self.subTest(change=new[:40]):
                self.assertTrue(self.wrong(RESOLVE_TEMPLATE.replace(old, new)))


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
        start = "    refused = sealed_start()\n    if refused is not None:\n        sys.stderr.write(refused)\n        return 2\n"
        for old, new in (
            (
                start + "    record = Record()\n    record.revision = recorded_state(ROOT)\n",
                "    record = Record()\n    record.revision = recorded_state(ROOT)\n" + start,
            ),
            (
                start + "    record = Record()\n    record.revision = recorded_state(ROOT)\n",
                "    record = Record()\n" + start + "    record.revision = recorded_state(ROOT)\n",
            ),
            (start, "    sealed_start()\n"),
            (start, "    refused = sealed_start()\n    if refused is not None:\n        sys.stderr.write(refused)\n"),
            (start, start.replace("return 2", "log(refused)")),
            (start, "    refused = sealed_start()\n    if refused is None:\n        return 2\n"),
            ("def run_local(record, log, cache):\n", "def run_local(record, log, cache):\n    resolve_pinned_mounts()\n"),
            ("def run_local(record, log, cache):\n", "def run_local(record, log, cache):\n    check_checkout_source(ROOT, RESOLVED_SOURCES)\n"),
            (
                "def run_prepared(record, log, cache, scratch):\n",
                "def run_prepared(record, log, cache, scratch):\n    check_checkout_source(ROOT, RESOLVED_SOURCES)\n",
            ),
            (
                "        resolve_pinned_mounts()\n        check_checkout_source(ROOT, RESOLVED_SOURCES)\n",
                "        check_checkout_source(ROOT, RESOLVED_SOURCES)\n        resolve_pinned_mounts()\n",
            ),
            ("        resolve_pinned_mounts()\n        check_checkout_source(ROOT, RESOLVED_SOURCES)\n", "        resolve_pinned_mounts()\n"),
            ("check_checkout_source(ROOT, RESOLVED_SOURCES)", "check_checkout_source(ROOT, [])"),
            ("check_checkout_source(ROOT, RESOLVED_SOURCES)", 'check_checkout_source(ROOT / "compose", RESOLVED_SOURCES)'),
            ("check_checkout_source(ROOT, RESOLVED_SOURCES)", "check_checkout_source(ROOT)"),
            ("symlinks=True, ", ""),
            ("symlinks=True, ", "symlinks=False, "),
            ("    check_tree_links(scratch)\n", ""),
            ("    check_tree_links(scratch)\n    stack = Stack(scratch)\n", "    stack = Stack(scratch)\n    check_tree_links(scratch)\n"),
            ("    stack = Stack(scratch)\n", "    stack = Stack(scratch)\n    resolve_source(CLINICAL_FILE)\n"),
            ("@functools.cache\n", ""),
            ("\n\ndef sealed_start", "\n\nPINS = release_pins()\n\n\ndef sealed_start"),
            ("\n\ndef sealed_start", "\n\nMANIFEST = json.loads(RELEASE_FILE.read_text())\nPINS = None\n\n\ndef sealed_start"),
            ("    refused = sealed_start()\n", "    refused = None\n"),
            ("    record = Record()\n    record.revision = recorded_state(ROOT)\n    run_local(record, [], cache)\n", "    pass\n"),
        ):
            with self.subTest(change=new[:50]):
                self.assertNotEqual(ORDER_GOOD, ORDER_GOOD.replace(old, new), "the change equals the good order")
                self.assertTrue(order_problems(ORDER_GOOD.replace(old, new)))

    @unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
    def test_the_runner_resolves_first_and_copies_links_unfollowed(self) -> None:
        self.assertEqual([], order_problems(RUNNER.read_text()))


if __name__ == "__main__":
    unittest.main()
