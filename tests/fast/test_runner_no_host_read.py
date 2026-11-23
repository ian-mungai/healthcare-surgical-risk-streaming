"""The signal profile acceptance runner never touches the clinical file or the track list on the host.

The two source folders are named once (``CLINICAL_DIR`` and ``TRACK_LIST_DIR``) and are only ever an argument of ``mount_for``, which builds the read-only
mount of the Spark container. The container checks each file against its pin and reads it. A host call that opens, reads, hashes, copies or parses either
file needs the folder name in some other place, and this check refuses every such place. The runner imports no CSV or table reader.

Failure modes covered here:

- A host call (``parse_clinical_table``, ``open``, ``shutil.copyfile``, ``file_sha256``, ``Path(...).read_text``) is given a source folder or a name made
  from it.
- A source folder is named a second time (a second variable or a rebuilt path), so the first rule cannot see where it goes.
- The runner imports a CSV or table reader.
"""

from __future__ import annotations

import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "tests" / "e2e" / "signal_profile.py"
SOURCES = ("CLINICAL_DIR", "TRACK_LIST_DIR")
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


GOOD = (
    "CLINICAL_DIR = os.environ.get('CLINICAL_DIR', '/x')\n"
    "TRACK_LIST_DIR = os.environ.get('TRACK_LIST_DIR', '/y')\n"
    "def mount_for(folder, container):\n    return f'{folder}:{container}:ro'\n"
    "mounts = [mount_for(CLINICAL_DIR, '/pinned/clinical'), mount_for(TRACK_LIST_DIR, '/pinned/track_list')]\n"
)


class Guard(unittest.TestCase):
    def test_the_folders_that_only_reach_the_mount_are_accepted(self) -> None:
        self.assertEqual([], host_access_problems(GOOD))

    def test_each_other_use_of_a_source_folder_is_refused(self) -> None:
        for planted in (
            "parse_clinical_table(CLINICAL_DIR)\n",
            "open(CLINICAL_DIR)\n",
            "shutil.copyfile(TRACK_LIST_DIR, data)\n",
            "file_sha256(CLINICAL_DIR / 'clinical_data.csv')\n",
            "Path(TRACK_LIST_DIR).read_text()\n",
            "other = CLINICAL_DIR\n",
            "mount_for(CLINICAL_DIR / 'x', '/c')\n",
            "mount_for(str(TRACK_LIST_DIR), '/c')\n",
            "print(CLINICAL_DIR)\n",
        ):
            with self.subTest(planted=planted):
                self.assertTrue(host_access_problems(GOOD + planted))

    def test_a_source_folder_named_twice_or_not_at_all_is_refused(self) -> None:
        self.assertTrue(host_access_problems(GOOD + "CLINICAL_DIR = '/z'\n"))
        self.assertTrue(host_access_problems(GOOD.replace("CLINICAL_DIR = os.environ.get('CLINICAL_DIR', '/x')\n", "")))

    def test_a_reader_import_is_refused(self) -> None:
        for planted in ("import csv\n", "from csv import DictReader\n", "import pandas\n", "from pandas import read_csv\n"):
            with self.subTest(planted=planted):
                self.assertTrue(host_access_problems(GOOD + planted))

    def test_a_problem_names_the_line_and_nothing_else(self) -> None:
        problems = host_access_problems(GOOD + "open(CLINICAL_DIR)\n")
        self.assertEqual(1, len(problems))
        self.assertRegex(problems[0], r"^line \d+: ")

    @unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
    def test_the_signal_profile_runner_holds_no_host_access_to_either_file(self) -> None:
        self.assertEqual([], host_access_problems(RUNNER.read_text()))


if __name__ == "__main__":
    unittest.main()
