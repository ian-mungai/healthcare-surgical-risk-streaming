"""The signal profile acceptance runner never touches the clinical file or the track list on the host. The two source files are named once (``CLINICAL_FILE``
and ``TRACK_LIST_FILE``, host paths that the runner never opens) and are only ever an argument of ``mount_for``, which builds a read-only single-file bind
mount of the Spark container that fails when the file is missing (``create_host_path`` false): the container sees no other file of the folder. The container
checks each file against its pin and reads it. A host call that opens, reads, hashes, copies or parses either file needs the folder name in some other
place, and this check refuses every such place. The runner imports no CSV or table reader.

Failure modes covered here:

- A host call (``parse_clinical_table``, ``open``, ``shutil.copyfile``, ``file_sha256``, ``Path(...).read_text``) is given a source folder or a name made
  from it.
- A source folder is named a second time (a second variable or a rebuilt path), so the first rule cannot see where it goes.
- The runner imports a CSV or table reader.
"""

from __future__ import annotations

import ast
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "tests" / "e2e" / "signal_profile.py"
SOURCES = ("CLINICAL_FILE", "TRACK_LIST_FILE")
MOUNT = "mount_for"
READERS = ("csv", "pandas")


def reader_import(node: ast.AST) -> bool:
    if isinstance(node, ast.Import):
        return any(alias.name.split(".")[0] in READERS for alias in node.names)
    return isinstance(node, ast.ImportFrom) and (node.module or "").split(".")[0] in READERS


def host_access_problems(source: str) -> list[str]:
    """The lines of ``source`` where a source folder is used for anything but the mount, or a reader is imported. A problem names the line only."""
    tree = ast.parse(source)
    allowed: set[int] = set()
    problems: list[str] = []
    defined = {name: 0 for name in SOURCES}
    for statement in tree.body:
        if (
            isinstance(statement, ast.Assign)
            and len(statement.targets) == 1
            and isinstance(statement.targets[0], ast.Name)
            and statement.targets[0].id in SOURCES
        ):
            defined[statement.targets[0].id] += 1
            allowed.add(id(statement.targets[0]))
            for inner in ast.walk(statement.value):
                if isinstance(inner, ast.Name) and inner.id in SOURCES:
                    problems.append(f"line {inner.lineno}: a source folder is made from a source folder")
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == MOUNT:
            allowed.update(id(argument) for argument in node.args if isinstance(argument, ast.Name) and argument.id in SOURCES)
        elif reader_import(node):
            problems.append(f"line {getattr(node, 'lineno', 0)}: a CSV or table reader is imported")
    for name_node in ast.walk(tree):
        if isinstance(name_node, ast.Name) and name_node.id in SOURCES and id(name_node) not in allowed:
            problems.append(f"line {name_node.lineno}: a source folder is used for more than the mount")
    problems += [f"line 0: {name} is not defined exactly once at the top" for name, count in defined.items() if count != 1]
    return sorted(set(problems), key=lambda item: int(item.split(":")[0].split()[1]))


def mount_problems(source: str) -> list[str]:
    """What ``mount_for`` builds must be a bind of one file, read-only, that is never created on the host."""
    tree = ast.parse(source)
    functions = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == MOUNT]
    if len(functions) != 1:
        return ["mount_for is not defined exactly once"]
    returns = [node.value for node in ast.walk(functions[0]) if isinstance(node, ast.Return) and isinstance(node.value, ast.Dict)]
    if len(returns) != 1:
        return ["mount_for does not return a mapping"]
    try:
        built = ast.literal_eval(
            ast.Dict(
                keys=[k for k in returns[0].keys if isinstance(k, ast.Constant)],
                values=[
                    v if isinstance(v, ast.Constant | ast.Dict) else ast.Constant(value="<name>")
                    for k, v in zip(returns[0].keys, returns[0].values, strict=True)
                    if isinstance(k, ast.Constant)
                ],
            )
        )
    except ValueError:
        return ["mount_for returns a mapping that cannot be read"]
    problems = []
    if built.get("type") != "bind":
        problems.append("the mount is not a bind")
    if built.get("read_only") is not True:
        problems.append("the mount is not read-only")
    if (built.get("bind") or {}).get("create_host_path") is not False:
        problems.append("the mount may create the host path")
    return problems


def default_problems(source: str) -> list[str]:
    """Each source constant must default to one file (its text ends in ``.csv``), never to a folder that holds other files."""
    problems = []
    for statement in ast.parse(source).body:
        if isinstance(statement, ast.Assign) and isinstance(statement.targets[0], ast.Name) and statement.targets[0].id in SOURCES:
            call = statement.value
            default = call.args[1] if isinstance(call, ast.Call) and len(call.args) == 2 else None
            if default is None or not re.search(r"\.csv['\")\s]*$", ast.unparse(default)):
                problems.append(f"{statement.targets[0].id} does not default to a single .csv file")
    return problems


MOUNT_GOOD = (
    "def mount_for(path, target):\n    return {'type': 'bind', 'source': path, 'target': target, 'read_only': True, 'bind': {'create_host_path': False}}\n"
)

GOOD = (
    "CLINICAL_FILE = os.environ.get('CLINICAL_FILE', '/x')\n"
    "TRACK_LIST_FILE = os.environ.get('TRACK_LIST_FILE', '/y')\n"
    + MOUNT_GOOD
    + "mounts = [mount_for(CLINICAL_FILE, '/pinned/clinical_data.csv'), mount_for(TRACK_LIST_FILE, '/pinned/trks_plain.csv')]\n"
)


class Guard(unittest.TestCase):
    def test_the_folders_that_only_reach_the_mount_are_accepted(self) -> None:
        self.assertEqual([], host_access_problems(GOOD))

    def test_each_other_use_of_a_source_folder_is_refused(self) -> None:
        for planted in (
            "parse_clinical_table(CLINICAL_FILE)\n",
            "open(CLINICAL_FILE)\n",
            "shutil.copyfile(TRACK_LIST_FILE, data)\n",
            "file_sha256(CLINICAL_FILE / 'clinical_data.csv')\n",
            "Path(TRACK_LIST_FILE).read_text()\n",
            "other = CLINICAL_FILE\n",
            "mount_for(CLINICAL_FILE / 'x', '/c')\n",
            "mount_for(str(TRACK_LIST_FILE), '/c')\n",
            "print(CLINICAL_FILE)\n",
            "os.lstat(CLINICAL_FILE)\n",
            "Path(CLINICAL_FILE).resolve(strict=True)\n",
            "Path(TRACK_LIST_FILE).read_bytes()\n",
            "open(TRACK_LIST_FILE, 'rb')\n",
            "hashlib.sha256(CLINICAL_FILE.encode())\n",
        ):
            with self.subTest(planted=planted):
                self.assertTrue(host_access_problems(GOOD + planted))

    def test_a_source_folder_named_twice_or_not_at_all_is_refused(self) -> None:
        self.assertTrue(host_access_problems(GOOD + "CLINICAL_FILE = '/z'\n"))
        self.assertTrue(host_access_problems(GOOD.replace("CLINICAL_FILE = os.environ.get('CLINICAL_FILE', '/x')\n", "")))

    def test_a_reader_import_is_refused(self) -> None:
        for planted in ("import csv\n", "from csv import DictReader\n", "import pandas\n", "from pandas import read_csv\n"):
            with self.subTest(planted=planted):
                self.assertTrue(host_access_problems(GOOD + planted))

    def test_a_problem_names_the_line_and_nothing_else(self) -> None:
        problems = host_access_problems(GOOD + "open(CLINICAL_FILE)\n")
        self.assertEqual(1, len(problems))
        self.assertRegex(problems[0], r"^line \d+: ")

    def test_a_mount_that_is_a_read_only_bind_that_never_creates_the_path_is_accepted(self) -> None:
        self.assertEqual([], mount_problems(MOUNT_GOOD))

    def test_a_default_that_is_a_folder_is_refused(self) -> None:
        self.assertEqual(
            [],
            default_problems(
                "CLINICAL_FILE = os.environ.get('CLINICAL_FILE', str(CACHE / 'clinical_data.csv'))\nTRACK_LIST_FILE = os.environ.get('T', '/y/trks.csv')\n"
            ),
        )
        self.assertTrue(default_problems("CLINICAL_FILE = os.environ.get('CLINICAL_FILE', str(CACHE))\n"))
        self.assertTrue(default_problems("TRACK_LIST_FILE = os.environ.get('T', '/y/trks_data')\n"))
        self.assertTrue(default_problems("TRACK_LIST_FILE = '/y/trks_plain.csv'\n"))

    @unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
    def test_the_signal_profile_runner_mounts_single_files_by_default(self) -> None:
        self.assertEqual([], default_problems(RUNNER.read_text()))

    def test_each_weaker_mount_is_refused(self) -> None:
        for old, new in (
            ("'create_host_path': False", "'create_host_path': True"),
            ("'bind': {'create_host_path': False}", "'x': 1"),
            ("'read_only': True", "'read_only': False"),
            ("'type': 'bind'", "'type': 'volume'"),
        ):
            with self.subTest(change=new):
                self.assertTrue(mount_problems(MOUNT_GOOD.replace(old, new)))
        self.assertTrue(mount_problems("def mount_for(path, target):\n    return f'{path}:{target}'\n"))

    @unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
    def test_the_signal_profile_runner_mounts_each_file_as_a_read_only_bind_that_never_creates_the_path(self) -> None:
        self.assertEqual([], mount_problems(RUNNER.read_text()))

    @unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
    def test_the_signal_profile_runner_holds_no_host_access_to_either_file(self) -> None:
        self.assertEqual([], host_access_problems(RUNNER.read_text()))


if __name__ == "__main__":
    unittest.main()
