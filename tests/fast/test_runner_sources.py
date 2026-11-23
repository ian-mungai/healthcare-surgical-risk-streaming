"""The pinned source paths of the signal profile runner are made once, refused unless a regular file, and read in the container only after the pin.

Failure modes covered here:

- A source path names a directory (or a link to one, or any link) and the whole directory is bound into the container before the pin is checked. ``mount_for``
  must refuse any source that is not a regular file by metadata alone (``os.lstat`` and ``stat.S_ISREG``, no read).
- A source path is built a second time: a string or ``Path`` that names either file outside its one definition, a second ``os.environ`` or ``getenv`` lookup of
  either name, or a read or a hash inside ``mount_for``.
- The clinical default does not follow ``LANDED_CACHE``, so setting it alone leaves the file where it was.
- The container parses a pinned file before its pin is checked: the stand-in reads only through ``first_read``, and each load runs ``pin_check`` before it.
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
SOURCES = ("CLINICAL_FILE", "TRACK_LIST_FILE")
FILE_WORDS = ("clinical_data.", "trks_plain", "track_list.csv")
METADATA_CALLS = {"lstat", "S_ISREG", "RuntimeError"}
EXEMPT_FUNCTIONS = {"host_copies"}
EXEMPT_ASSIGNMENTS = {"STAND_IN", "TRACK_LIST_NAME"}


def top_assignment(statement: ast.stmt) -> str | None:
    if isinstance(statement, ast.Assign) and len(statement.targets) == 1 and isinstance(statement.targets[0], ast.Name):
        return statement.targets[0].id
    return None


def assigned_value(statement: ast.stmt, name: str) -> ast.expr | None:
    """The value that a top-level statement gives to ``name``, or ``None`` when it is not that assignment."""
    if top_assignment(statement) == name and isinstance(statement, ast.Assign):
        return statement.value
    return None


def construction_problems(source: str) -> list[str]:
    """The lines that build either source path outside its one definition, read it by an environment lookup again, or read or hash inside ``mount_for``."""
    tree = ast.parse(source)
    exempt: set[int] = set()
    problems: list[str] = []
    for statement in tree.body:
        name = top_assignment(statement)
        if (
            name in SOURCES
            or name in EXEMPT_ASSIGNMENTS
            or (name or "").endswith("_TARGET")
            or (isinstance(statement, ast.FunctionDef) and statement.name in EXEMPT_FUNCTIONS)
        ):
            exempt.update(id(part) for part in ast.walk(statement))
        elif isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Constant):
            exempt.add(id(statement.value))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name in EXEMPT_FUNCTIONS:
            exempt.update(id(part) for part in ast.walk(node))
        if isinstance(node, ast.FunctionDef) and node.body and isinstance(node.body[0], ast.Expr) and isinstance(node.body[0].value, ast.Constant):
            exempt.add(id(node.body[0].value))
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in exempt:
            if node.value in SOURCES:
                problems.append(f"line {node.lineno}: a source name is looked up outside its definition")
            if node.value not in SOURCES and any(word in node.value for word in FILE_WORDS):
                problems.append(f"line {node.lineno}: a path that names a pinned file is built outside its definition")
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "mount_for":
            for part in ast.walk(node):
                if isinstance(part, ast.Call):
                    called = part.func.id if isinstance(part.func, ast.Name) else getattr(part.func, "attr", "")
                    if called != "require_regular_file":
                        problems.append(f"line {part.lineno}: mount_for calls something other than the metadata check")
    return sorted(set(problems), key=lambda item: int(item.split(":")[0].split()[1]))


def metadata_problems(source: str) -> list[str]:
    """``require_regular_file`` may only look at metadata: it calls ``os.lstat``, ``stat.S_ISREG`` and raises."""
    functions = [node for node in ast.walk(ast.parse(source)) if isinstance(node, ast.FunctionDef) and node.name == "require_regular_file"]
    if len(functions) != 1:
        return ["require_regular_file is not defined exactly once"]
    problems = []
    called = {
        part.func.id if isinstance(part.func, ast.Name) else getattr(part.func, "attr", "") for part in ast.walk(functions[0]) if isinstance(part, ast.Call)
    }
    if not {"lstat", "S_ISREG"} <= called:
        problems.append("require_regular_file does not use os.lstat and stat.S_ISREG")
    if called - METADATA_CALLS:
        problems.append("require_regular_file calls more than the metadata check")
    return problems


def default_problems(source: str) -> list[str]:
    """The clinical default must follow ``LANDED_CACHE``."""
    for statement in ast.parse(source).body:
        value = assigned_value(statement, "CLINICAL_FILE")
        if value is not None:
            return [] if "LANDED_CACHE" in ast.unparse(value) else ["the clinical default does not follow LANDED_CACHE"]
    return ["CLINICAL_FILE is not defined"]


def stand_in_order_problems(source: str) -> list[str]:
    """In the container script, every read goes through ``first_read`` and each load runs ``pin_check`` before its first read."""
    stand_in = None
    for statement in ast.parse(source).body:
        value = assigned_value(statement, "STAND_IN")
        if isinstance(value, ast.Constant):
            stand_in = value.value
    if not isinstance(stand_in, str):
        return ["STAND_IN is not a text"]
    tree = ast.parse(stand_in)
    problems = []
    functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and node.attr == "csv"
            and node.lineno not in {n.lineno for n in ast.walk(functions.get("first_read", tree)) if hasattr(n, "lineno")}
        ):
            problems.append(f"line {node.lineno}: a file is parsed outside first_read")
    first = functions.get("first_read")
    if first is None or not any(isinstance(part, ast.Call) and getattr(part.func, "id", "") == "step" for part in ast.walk(first)):
        problems.append("first_read does not report its step")
    for name in ("load_clinical", "load_track_list"):
        load = functions.get(name)
        if load is None:
            problems.append(f"{name} is missing")
            continue
        lines = {
            call: min(part.lineno for part in ast.walk(load) if isinstance(part, ast.Call) and getattr(part.func, "id", "") == call)
            for call in ("pin_check", "first_read")
            if any(isinstance(part, ast.Call) and getattr(part.func, "id", "") == call for part in ast.walk(load))
        }
        if set(lines) != {"pin_check", "first_read"} or lines["pin_check"] >= lines["first_read"]:
            problems.append(f"{name} does not check the pin before its first read")
    return problems


MOUNT_SOURCE = """
def require_regular_file(path):
    try:
        mode = os.lstat(path).st_mode
    except OSError:
        raise RuntimeError("cannot inspect") from None
    if not stat.S_ISREG(mode):
        raise RuntimeError("not a regular file")


def mount_for(path, target):
    require_regular_file(path)
    return {"type": "bind", "source": path, "target": target, "read_only": True, "bind": {"create_host_path": False}}
"""
GOOD = (
    "CLINICAL_FILE = os.environ.get('CLINICAL_FILE', str(Path(LANDED_CACHE) / 'clinical_data.csv'))\n"
    "TRACK_LIST_FILE = os.environ.get('TRACK_LIST_FILE', '/y/trks_plain.csv')\n"
    "CLINICAL_TARGET = '/pinned/clinical_data.csv'\n" + MOUNT_SOURCE
)


def load_mount(source: str) -> dict[str, Any]:
    """Import only ``require_regular_file`` and ``mount_for`` from ``source``, as a module of their own."""
    tree = ast.parse(source)
    wanted: list[ast.stmt] = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in {"require_regular_file", "mount_for"}]
    text = "import os\nimport stat\nfrom typing import Any\n\n" + "\n\n".join(ast.unparse(node) for node in wanted) + "\n"
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / "mount_under_test.py"
        path.write_text(text)
        spec = importlib.util.spec_from_file_location("mount_under_test", path)
        if spec is None or spec.loader is None:
            raise RuntimeError("the mount module cannot be loaded")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return vars(module)


class Behaviour(unittest.TestCase):
    def sources(self) -> list[tuple[str, str]]:
        found = [("template", GOOD)]
        if RUNNER.is_file():
            found.append(("runner", RUNNER.read_text()))
        return found

    def test_a_regular_file_is_bound_and_everything_else_is_refused_before_the_bind(self) -> None:
        for label, source in self.sources():
            mount_for = load_mount(source)["mount_for"]
            with self.subTest(source=label), tempfile.TemporaryDirectory() as parent:
                folder, regular, missing = Path(parent) / "folder", Path(parent) / "file.csv", Path(parent) / "none.csv"
                folder.mkdir()
                regular.write_text("x")
                (Path(parent) / "link_to_folder").symlink_to(folder)
                (Path(parent) / "link_to_file").symlink_to(regular)
                self.assertEqual(str(regular), mount_for(str(regular), "/t")["source"])
                for refused in (folder, Path(parent) / "link_to_folder", Path(parent) / "link_to_file", missing):
                    with self.subTest(refused=refused.name), self.assertRaises((RuntimeError, OSError)) as caught:
                        mount_for(str(refused), "/t")
                    self.assertNotIn(parent, str(caught.exception))

    def test_a_weaker_check_is_refused_by_the_same_cases(self) -> None:
        for old, new in (
            ("stat.S_ISREG(mode)", "True"),
            ("os.lstat(path)", "os.stat(path)"),
            ("stat.S_ISREG(mode)", "not stat.S_ISLNK(mode)"),
        ):
            with self.subTest(change=new), tempfile.TemporaryDirectory() as parent:
                weaker = load_mount(GOOD.replace(old, new))["mount_for"]
                folder, regular = Path(parent) / "folder", Path(parent) / "file.csv"
                folder.mkdir()
                regular.write_text("x")
                (Path(parent) / "link_to_file").symlink_to(regular)
                accepted = []
                for candidate in (folder, Path(parent) / "link_to_file"):
                    try:
                        weaker(str(candidate), "/t")
                        accepted.append(candidate.name)
                    except RuntimeError, OSError:
                        continue
                self.assertTrue(accepted, "the weaker check refused every case")

    def test_the_template_and_the_runner_only_look_at_metadata(self) -> None:
        self.assertEqual([], metadata_problems(GOOD))
        self.assertTrue(metadata_problems(GOOD.replace("os.lstat(path)", "open(path).read()")))
        self.assertTrue(metadata_problems(GOOD.replace('raise RuntimeError("not a regular file")', "hashlib.sha256(b'x')")))
        if RUNNER.is_file():
            self.assertEqual([], metadata_problems(RUNNER.read_text()))


class Construction(unittest.TestCase):
    def test_the_definitions_alone_are_accepted(self) -> None:
        self.assertEqual([], construction_problems(GOOD))

    def test_each_other_construction_of_a_source_path_is_refused(self) -> None:
        for planted in (
            "p = Path('/x/clinical_data.csv')\n",
            "p = os.path.join(d, 'clinical_data.csv')\n",
            "p = cache / 'trks_plain.csv'\n",
            "p = d / 'track_list.csv'\n",
            "p = os.environ.get('CLINICAL_FILE', '/z')\n",
            "p = os.getenv('TRACK_LIST_FILE')\n",
            "p = os.environ['CLINICAL_FILE']\n",
            "m = {'CLINICAL_FILE': 1}\n",
        ):
            with self.subTest(planted=planted):
                self.assertTrue(construction_problems(GOOD + planted))

    def test_a_read_or_a_hash_inside_mount_for_is_refused(self) -> None:
        for planted in ("    open(path).read()\n", "    file_sha256(path)\n", "    Path(path).read_bytes()\n", "    hashlib.sha256(b'x')\n"):
            with self.subTest(planted=planted):
                self.assertTrue(construction_problems(GOOD.replace("    require_regular_file(path)\n", "    require_regular_file(path)\n" + planted)))

    def test_what_the_runner_may_say_is_not_refused(self) -> None:
        source = GOOD + (
            '"""A docstring may name clinical_data.csv."""\n'
            "def host_copies():\n    return {'clinical_data.csv'}\n"
            "TRACK_LIST_NAME = 'trks_plain.csv'\n"
            "STAND_IN = 'x = open(\"/pinned/clinical_data.csv\")'\n"
            "name = 'clinical_data'\n"
        )
        self.assertEqual([], construction_problems(source))

    @unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
    def test_the_runner_builds_each_source_path_once(self) -> None:
        self.assertEqual([], construction_problems(RUNNER.read_text()))


class Defaults(unittest.TestCase):
    def test_the_clinical_default_must_follow_landed_cache(self) -> None:
        self.assertEqual([], default_problems(GOOD))
        self.assertTrue(default_problems(GOOD.replace("Path(LANDED_CACHE)", "Path(DEFAULT_CACHE)")))
        self.assertTrue(default_problems("TRACK_LIST_FILE = 'x'\n"))

    @unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
    def test_the_runner_follows_landed_cache(self) -> None:
        self.assertEqual([], default_problems(RUNNER.read_text()))


STAND_IN_GOOD = (
    "STAND_IN = '''\n"
    "def step(name):\n    print(name)\n"
    "def pin_check(name, path, pin):\n    step('pin_check')\n"
    "def first_read(session, path):\n    step('first_read')\n    return session.read.csv(path)\n"
    "def load_clinical(session, spec):\n    pin_check('c', '/p', {})\n    frame = first_read(session, '/p')\n"
    "def load_track_list(session, spec):\n    pin_check('t', '/p', {})\n    frame = first_read(session, '/p')\n"
    "'''\n"
)


class StandIn(unittest.TestCase):
    def test_a_container_that_checks_the_pin_first_and_reads_through_first_read_is_accepted(self) -> None:
        self.assertEqual([], stand_in_order_problems(STAND_IN_GOOD))

    def test_each_reordering_or_bypass_is_refused(self) -> None:
        for old, new in (
            ("    pin_check('c', '/p', {})\n    frame = first_read(session, '/p')\n", "    frame = first_read(session, '/p')\n    pin_check('c', '/p', {})\n"),
            ("    pin_check('t', '/p', {})\n", ""),
            ("    frame = first_read(session, '/p')\ndef load_track_list", "    frame = session.read.csv('/p')\ndef load_track_list"),
            ("    step('first_read')\n", ""),
        ):
            with self.subTest(change=new[:40]):
                self.assertTrue(stand_in_order_problems(STAND_IN_GOOD.replace(old, new)))

    @unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
    def test_the_runner_container_script_checks_the_pin_before_any_read(self) -> None:
        self.assertEqual([], stand_in_order_problems(RUNNER.read_text()))


if __name__ == "__main__":
    unittest.main()
