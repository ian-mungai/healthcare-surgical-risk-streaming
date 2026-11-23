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
import contextlib
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "tests" / "e2e" / "signal_profile.py"
SOURCES = ("CLINICAL_FILE", "TRACK_LIST_FILE")
FILE_WORDS = ("clinical_data.", "trks_plain", "track_list.csv")
METADATA_CALLS = {"Path", "resolve", "lstat", "S_ISREG", "is_relative_to", "RuntimeError", "str"}
EXEMPT_ASSIGNMENTS = {"STAND_IN"}
COPY_HELPERS = {"host_copies", "TRACK_LIST_NAME"}
RAW_READS = {"open", "read_text", "read_bytes", "read"}


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
        if name in SOURCES or name in EXEMPT_ASSIGNMENTS or (name or "").endswith("_TARGET"):
            exempt.update(id(part) for part in ast.walk(statement))
        elif isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Constant):
            exempt.add(id(statement.value))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.body and isinstance(node.body[0], ast.Expr) and isinstance(node.body[0].value, ast.Constant):
            exempt.add(id(node.body[0].value))
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in exempt:
            if node.value in SOURCES:
                problems.append(f"line {node.lineno}: a source name is looked up outside its definition")
            if node.value not in SOURCES and any(word in node.value for word in FILE_WORDS):
                problems.append(f"line {node.lineno}: a path that names a pinned file is built outside its definition")
    for node in ast.walk(tree):
        if (isinstance(node, ast.FunctionDef) and node.name in COPY_HELPERS) or (isinstance(node, ast.Name) and node.id in COPY_HELPERS):
            problems.append(f"line {node.lineno}: a copy helper is back")
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "mount_for":
            for part in ast.walk(node):
                if isinstance(part, ast.Call):
                    called = part.func.id if isinstance(part.func, ast.Name) else getattr(part.func, "attr", "")
                    if called != "resolve_source":
                        problems.append(f"line {part.lineno}: mount_for calls something other than the metadata check")
    return sorted(set(problems), key=lambda item: int(item.split(":")[0].split()[1]))


def metadata_problems(source: str) -> list[str]:
    """``resolve_source`` may only look at metadata: it resolves the path, calls ``os.lstat`` and ``stat.S_ISREG``, tests the repository root and raises."""
    functions = [node for node in ast.walk(ast.parse(source)) if isinstance(node, ast.FunctionDef) and node.name == "resolve_source"]
    if len(functions) != 1:
        return ["resolve_source is not defined exactly once"]
    problems = []
    called = {
        part.func.id if isinstance(part.func, ast.Name) else getattr(part.func, "attr", "") for part in ast.walk(functions[0]) if isinstance(part, ast.Call)
    }
    if not {"resolve", "lstat", "S_ISREG", "is_relative_to"} <= called:
        problems.append("resolve_source does not resolve, test for a regular file and test for the repository root")
    if called - METADATA_CALLS:
        problems.append("resolve_source calls more than the metadata check")
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
        before = lines.get("pin_check", 0)
        for part in ast.walk(load):
            if isinstance(part, ast.Call) and getattr(part.func, "id", getattr(part.func, "attr", "")) in RAW_READS:
                problems.append(f"line {part.lineno}: {name} opens or reads a file itself")
            elif isinstance(part, ast.Call) and part.lineno < before:
                problems.append(f"line {part.lineno}: {name} calls something before the pin check")
    hooks = [
        node for node in tree.body if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call) and getattr(node.value.func, "attr", "") == "addaudithook"
    ]
    if not hooks:
        problems.append("the container script reports no raw open of a pinned file (no audit hook)")
    return problems


MOUNT_SOURCE = """
def resolve_source(setting):
    try:
        resolved = Path(setting).resolve(strict=True)
        mode = os.lstat(resolved).st_mode
    except (OSError, RuntimeError):
        raise RuntimeError("cannot resolve") from None
    if not stat.S_ISREG(mode):
        raise RuntimeError("not a regular file")
    if resolved.is_relative_to(ROOT.resolve()):
        raise RuntimeError("inside the repository")
    return str(resolved)


def mount_for(path, target):
    return {"type": "bind", "source": resolve_source(path), "target": target, "read_only": True, "bind": {"create_host_path": False}}
"""
GOOD = (
    "CLINICAL_FILE = os.environ.get('CLINICAL_FILE', str(Path(LANDED_CACHE) / 'clinical_data.csv'))\n"
    "TRACK_LIST_FILE = os.environ.get('TRACK_LIST_FILE', '/y/trks_plain.csv')\n"
    "CLINICAL_TARGET = '/pinned/clinical_data.csv'\n" + MOUNT_SOURCE
)


def load_mount(source: str, root: Path) -> dict[str, Any]:
    """Import only ``resolve_source`` and ``mount_for`` from ``source``, as a module of their own, with ``ROOT`` set to ``root``."""
    tree = ast.parse(source)
    wanted: list[ast.stmt] = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in {"resolve_source", "mount_for"}]
    head = f"import os\nimport stat\nfrom pathlib import Path\nfrom typing import Any\n\nROOT = Path({str(root)!r})\n\n"
    text = head + "\n\n".join(ast.unparse(node) for node in wanted) + "\n"
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / "mount_under_test.py"
        path.write_text(text)
        spec = importlib.util.spec_from_file_location("mount_under_test", path)
        if spec is None or spec.loader is None:
            raise RuntimeError("the mount module cannot be loaded")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return vars(module)


def judge(source: str) -> list[str]:
    """Run the mount of ``source`` over the cases and name each one it gets wrong: a source it binds that it must refuse, a refusal that holds a path, or a
    bind whose source is not the one absolute real path."""
    wrong: list[str] = []
    with tempfile.TemporaryDirectory() as parent_name:
        parent = Path(parent_name)
        repo, outside = parent / "repo", parent / "outside"
        (repo / "compose").mkdir(parents=True)
        outside.mkdir()
        (repo / "inside.csv").write_text("x")
        (outside / "file.csv").write_text("x")
        (outside / "services").mkdir()
        (outside / "folder").mkdir()
        (outside / "link_to_folder").symlink_to(outside / "folder")
        (outside / "link_to_file").symlink_to(outside / "file.csv")
        (outside / "link_into_repo").symlink_to(repo / "inside.csv")
        mount_for = load_mount(source, repo)["mount_for"]
        for label, candidate in {"file": outside / "file.csv", "link to a file outside": outside / "link_to_file"}.items():
            try:
                bound = mount_for(str(candidate), "/t")["source"]
            except RuntimeError, OSError:
                wrong.append(f"refused {label}")
                continue
            if bound != str((outside / "file.csv").resolve()) or not Path(bound).is_absolute():
                wrong.append(f"{label} is not bound by its one real path")
        refused = {
            "directory": outside / "folder",
            "link to a directory": outside / "link_to_folder",
            "file inside the repository": repo / "inside.csv",
            "directory inside the repository": repo / "compose",
            "link into the repository": outside / "link_into_repo",
            "missing": outside / "none.csv",
        }
        for label, candidate in refused.items():
            try:
                mount_for(str(candidate), "/t")
                wrong.append(f"bound {label}")
            except (RuntimeError, OSError) as error:
                if parent_name in str(error) or str(parent.resolve()) in str(error):
                    wrong.append(f"the refusal of {label} names the path")
        with contextlib.chdir(outside):
            try:
                mount_for("services", "/t")
                wrong.append("bound a relative name that is a directory")
            except RuntimeError, OSError:
                pass
            try:
                if mount_for("file.csv", "/t")["source"] != str((outside / "file.csv").resolve()):
                    wrong.append("a relative name is not bound by its absolute real path")
            except RuntimeError, OSError:
                wrong.append("refused a relative name of a file")
    return wrong


class Behaviour(unittest.TestCase):
    def sources(self) -> list[tuple[str, str]]:
        found = [("template", GOOD)]
        if RUNNER.is_file():
            found.append(("runner", RUNNER.read_text()))
        return found

    def test_one_absolute_real_path_outside_the_repository_is_bound_and_everything_else_is_refused(self) -> None:
        for label, source in self.sources():
            with self.subTest(source=label):
                self.assertEqual([], judge(source))

    def test_each_weaker_resolver_gets_a_case_wrong(self) -> None:
        for old, new in (
            ('    if not stat.S_ISREG(mode):\n        raise RuntimeError("not a regular file")\n', ""),
            ('    if resolved.is_relative_to(ROOT.resolve()):\n        raise RuntimeError("inside the repository")\n', ""),
            ("resolved.is_relative_to(ROOT.resolve())", "resolved.is_relative_to(ROOT)"),
            ("Path(setting).resolve(strict=True)", "Path(setting).absolute()"),
            ("    return str(resolved)", "    return setting"),
            ('"source": resolve_source(path)', '"source": path'),
            ("mode = os.lstat(resolved).st_mode", "mode = os.lstat(setting).st_mode"),
        ):
            with self.subTest(change=new[:40]):
                self.assertTrue(judge(GOOD.replace(old, new)), "the weaker resolver got every case right")

    def test_the_template_and_the_runner_only_look_at_metadata(self) -> None:
        self.assertEqual([], metadata_problems(GOOD))
        self.assertTrue(metadata_problems(GOOD.replace("os.lstat(resolved)", "open(resolved).read()")))
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
            "def host_copies():\n    return []\n",
            "TRACK_LIST_NAME = 'x'\n",
            "copies = host_copies()\n",
        ):
            with self.subTest(planted=planted):
                self.assertTrue(construction_problems(GOOD + planted))

    def test_a_read_or_a_hash_inside_mount_for_is_refused(self) -> None:
        for planted in ("    open(path).read()\n", "    file_sha256(path)\n", "    Path(path).read_bytes()\n", "    hashlib.sha256(b'x')\n"):
            with self.subTest(planted=planted):
                self.assertTrue(construction_problems(GOOD.replace("def mount_for(path, target):\n", "def mount_for(path, target):\n" + planted)))

    def test_what_the_runner_may_say_is_not_refused(self) -> None:
        source = GOOD + ('"""A docstring may name clinical_data.csv."""\nSTAND_IN = \'x = open("/pinned/clinical_data.csv")\'\nname = \'clinical_data\'\n')
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


TEMPLATE_STAND_IN = (
    "STAND_IN = '''\n"
    "STEPS = []\n"
    "PINNED_PATHS = {'/pinned/clinical_data.csv'}\n"
    "HASHING = []\n"
    "def audit(event, arguments):\n"
    "    if event == 'open' and arguments and str(arguments[0]) in PINNED_PATHS and not HASHING:\n"
    "        STEPS.append('raw_open')\n"
    "        print('STEP raw_open', file=sys.stderr, flush=True)\n"
    "sys.addaudithook(audit)\n"
    "def step(name):\n"
    "    STEPS.append(name)\n"
    "    print('STEP ' + name, file=sys.stderr, flush=True)\n"
    "def pin_check(name, path, pin):\n"
    "    step('pin_check')\n"
    "    target = Path(path)\n"
    "    digest = hashlib.sha256()\n"
    "    HASHING.append(1)\n"
    "    try:\n"
    "        with target.open('rb') as handle:\n"
    "            digest.update(handle.read())\n"
    "    finally:\n"
    "        HASHING.pop()\n"
    "    if digest.hexdigest() != pin['sha256']:\n"
    "        raise SystemExit('pin_mismatch: ' + name)\n"
    "    step('pin_ok')\n"
    "'''\n"
)

STAND_IN_GOOD = (
    "STAND_IN = '''\n"
    "sys.addaudithook(audit)\n"
    "def step(name):\n    print(name)\n"
    "def pin_check(name, path, pin):\n    step('pin_check')\n"
    "def first_read(session, path):\n    step('first_read')\n    return session.read.csv(path)\n"
    "def load_clinical(session, spec):\n    path = '/p'\n    pin_check('c', '/p', {})\n    frame = first_read(session, '/p')\n"
    "def load_track_list(session, spec):\n    pin_check('t', '/p', {})\n    frame = first_read(session, '/p')\n"
    "'''\n"
)


def pin_script(source: str, pinned: Path, body: str) -> str:
    """The pin and audit parts of the container script, with the pinned paths set to ``pinned``, and ``body`` to run after them."""
    stand_in = ""
    for statement in ast.parse(source).body:
        value = assigned_value(statement, "STAND_IN")
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            stand_in = value.value
    tree = ast.parse(stand_in)
    names = {"STEPS", "PINNED_PATHS", "HASHING"}
    functions = {"audit", "step", "pin_check"}
    kept: list[ast.stmt] = []
    for node in tree.body:
        if (
            (isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") in names)
            or (isinstance(node, ast.FunctionDef) and node.name in functions)
            or isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Call)
            and getattr(node.value.func, "attr", "") == "addaudithook"
        ):
            kept.append(node)
    text = ast.unparse(ast.Module(body=kept, type_ignores=[]))
    text = text.replace("/pinned/clinical_data.csv", str(pinned))
    return "import hashlib, json, sys\nfrom pathlib import Path\n" + text + "\n" + body + "\n"


def run_pin_script(source: str, body: str) -> tuple[int, list[str]]:
    """Run the container's pin and audit code in a fresh interpreter on a file of the right content and report the exit code and the ``STEP`` lines."""
    with tempfile.TemporaryDirectory() as folder:
        pinned = Path(folder) / "clinical_data.csv"
        pinned.write_text("caseid,subjectid\n1,2\n")
        script = Path(folder) / "run.py"
        script.write_text(pin_script(source, pinned, body.replace("PINNED", repr(str(pinned)))))
        done = run_command(sys.executable, ["-I", str(script)], timeout=60)
        return done.returncode, [line for line in done.stderr.splitlines() if line.startswith("STEP ")]


RIGHT = "{'sha256': hashlib.sha256(b'caseid,subjectid\\n1,2\\n').hexdigest()}"
WRONG = "{'sha256': '0' * 64}"


class PinBehaviour(unittest.TestCase):
    def sources(self) -> list[tuple[str, str]]:
        found = [("template", TEMPLATE_STAND_IN)]
        if RUNNER.is_file():
            found.append(("runner", RUNNER.read_text()))
        return found

    def test_the_right_pin_reports_the_check_and_no_raw_open(self) -> None:
        for label, source in self.sources():
            with self.subTest(source=label):
                self.assertEqual((0, ["STEP pin_check", "STEP pin_ok"]), run_pin_script(source, f"pin_check('clinical_data', PINNED, {RIGHT})"))

    def test_a_wrong_pin_is_refused_with_the_check_as_the_only_step(self) -> None:
        for label, source in self.sources():
            with self.subTest(source=label):
                code, steps = run_pin_script(source, f"pin_check('clinical_data', PINNED, {WRONG})")
                self.assertNotEqual(0, code)
                self.assertEqual(["STEP pin_check"], steps)

    def test_any_raw_open_of_the_pinned_file_before_the_check_is_reported(self) -> None:
        for spelling in ("open(PINNED).read()", "Path(PINNED).read_bytes()", "Path(PINNED).read_text()", "Path(PINNED).open().read()"):
            for label, source in self.sources():
                with self.subTest(source=label, spelling=spelling):
                    code, steps = run_pin_script(source, f"{spelling}\npin_check('clinical_data', PINNED, {WRONG})")
                    self.assertNotEqual(0, code)
                    self.assertEqual(["STEP raw_open", "STEP pin_check"], steps)

    def test_a_template_without_the_hook_or_the_flag_gets_a_case_wrong(self) -> None:
        for old, new in (("sys.addaudithook(audit)\n", ""), ("    HASHING.append(1)\n", ""), ("not HASHING", "True")):
            with self.subTest(change=new or old[:20]):
                weaker = TEMPLATE_STAND_IN.replace(old, new)
                right = run_pin_script(weaker, f"pin_check('clinical_data', PINNED, {RIGHT})")
                raw = run_pin_script(weaker, f"open(PINNED).read()\npin_check('clinical_data', PINNED, {WRONG})")
                self.assertTrue(right != (0, ["STEP pin_check", "STEP pin_ok"]) or raw[1] != ["STEP raw_open", "STEP pin_check"])


class StandIn(unittest.TestCase):
    def test_a_container_that_checks_the_pin_first_and_reads_through_first_read_is_accepted(self) -> None:
        self.assertEqual([], stand_in_order_problems(STAND_IN_GOOD))

    def test_each_reordering_or_bypass_is_refused(self) -> None:
        for old, new in (
            ("    pin_check('c', '/p', {})\n    frame = first_read(session, '/p')\n", "    frame = first_read(session, '/p')\n    pin_check('c', '/p', {})\n"),
            ("    pin_check('t', '/p', {})\n", ""),
            ("    frame = first_read(session, '/p')\ndef load_track_list", "    frame = session.read.csv('/p')\ndef load_track_list"),
            ("    step('first_read')\n", ""),
            ("    pin_check('c', '/p', {})\n", "    open('/p').read()\n    pin_check('c', '/p', {})\n"),
            ("    pin_check('c', '/p', {})\n", "    Path('/p').read_bytes()\n    pin_check('c', '/p', {})\n"),
            ("    pin_check('t', '/p', {})\n", "    pin_check('t', '/p', {})\n    data = Path('/p').read_text()\n"),
            ("    pin_check('c', '/p', {})\n", "    prepare()\n    pin_check('c', '/p', {})\n"),
            ("sys.addaudithook(audit)\n", ""),
        ):
            with self.subTest(change=new[:40]):
                self.assertTrue(stand_in_order_problems(STAND_IN_GOOD.replace(old, new)))

    @unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
    def test_the_runner_container_script_checks_the_pin_before_any_read(self) -> None:
        self.assertEqual([], stand_in_order_problems(RUNNER.read_text()))


if __name__ == "__main__":
    unittest.main()
