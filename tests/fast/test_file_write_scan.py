"""Every file write in production code goes through one of two helpers, or is on the explicit list below with its reason.

The two helpers live in ``scripts/ingestion/output_file.py``: ``write_new_file`` for every output that a command line names and for every receipt, sidecar
or checksum file (it never replaces a path), and ``replace_owned_file`` for state that a long-running command owns and must update (it replaces only a
regular file directly inside the state folder that the command created or was given).

``test_no_call_writes_a_file_outside_the_helpers_or_the_list`` parses every production Python file (``scripts``, ``replay``, ``spark``, ``features``,
``infra`` and ``orchestration``; not a ``tests`` folder) with ``ast`` and lists each call that writes, moves or links a file. A call fails the test unless
it sits in one of the two helpers or matches an entry of ``ALLOWED`` (file, enclosing function, call text, count, reason). An entry that no longer matches
a call fails the test too, so the list never keeps a site that was moved onto a helper. The scan tests plant one direct write per pattern and check that
the scan finds it.
"""

from __future__ import annotations

import ast
import re
import unittest
from pathlib import Path

from tests.fast.git_fixture import ROOT

SCANNED = ("scripts", "replay", "spark", "features", "infra", "orchestration", "dagster")
HELPER_FILE = "scripts/ingestion/output_file.py"
HELPERS = frozenset({"write_new_file", "replace_owned_file"})
OPEN_MODE = re.compile(r"^[rwxabt+]+(:[A-Za-z0-9*]+)?$")
WRITE_LETTERS = frozenset("wax+")
WRITE_FLAGS = frozenset({"O_WRONLY", "O_RDWR", "O_CREAT", "O_APPEND", "O_TRUNC", "O_EXCL"})
WRITE_METHODS = frozenset({"write_text", "write_bytes", "touch", "symlink_to", "hardlink_to"})
OS_MOVES = frozenset({"replace", "rename", "renames", "link", "symlink", "truncate", "mkfifo", "mknod"})
SHUTIL_WRITES = frozenset({"copy", "copy2", "copyfile", "copyfileobj", "copytree", "copymode", "copystat", "move", "make_archive", "unpack_archive"})
DUMPS = frozenset({"json", "pickle", "marshal", "yaml", "tomli_w", "plistlib"})
TEMPORARY_FILES = frozenset({"mkstemp", "NamedTemporaryFile", "TemporaryFile", "SpooledTemporaryFile"})
ARCHIVES = frozenset({"gzip", "bz2", "lzma", "tarfile", "codecs"})

TD_REASON = "Teardown code: phase 13 reviews it; the file is in the removal folder of the run that the teardown created"
TF_REASON = "A Terraform file in a run folder or work folder that the wrapper itself created; phase 13 reviews the wrapper with the teardown"

# One entry per call site: (file, enclosing function, text of the call) -> (how many such calls, reason). A reason names why the site is not on a helper.
ALLOWED: dict[tuple[str, str, str], tuple[int, str]] = {
    ("scripts/evidence/export.py", "main", "tarfile.open"): (
        1,
        "The evidence archive is streamed into the export folder that the command made; it is too large to hold in memory for the helper",
    ),
    ("scripts/evidence/verify.py", "verify_export", "partial.replace"): (
        1,
        "Renames the verified readback folder, which this command extracted into a partial folder of its own, into its final name",
    ),
    ("scripts/infra/teardown.py", "plan_teardown", "(folder / 'teardown' / 'approved_inventory.json').write_text"): (1, TD_REASON),
    ("scripts/infra/teardown.py", "hold_state_lock", "body.write_text"): (1, TD_REASON),
    ("scripts/infra/teardown.py", "clean_bucket", "(removal / 'destroy_written_state_entries.json').write_text"): (1, TD_REASON),
    ("scripts/infra/teardown.py", "write_progress", "pending.write_text"): (1, TD_REASON),
    ("scripts/infra/teardown.py", "write_progress", "os.replace"): (1, TD_REASON),
    ("scripts/infra/teardown.py", "destroy", "(removal / 'retained.json').write_text"): (1, TD_REASON),
    ("scripts/infra/teardown.py", "destroy", "(removal / 'checks.json').write_text"): (1, TD_REASON),
    ("scripts/infra/terraform_wrapper.py", "save_raw", "os.open"): (1, TF_REASON),
    ("scripts/infra/terraform_wrapper.py", "save_raw", "os.fdopen"): (1, TF_REASON),
    ("scripts/infra/terraform_wrapper.py", "prepare_configuration", "shutil.copy2"): (1, TF_REASON),
    ("scripts/infra/terraform_wrapper.py", "prepare_configuration", "shutil.copytree"): (1, TF_REASON),
    ("scripts/infra/terraform_wrapper.py", "prepare_configuration", "(target / HEAVY_INPUTS_FILE).write_text"): (1, TF_REASON),
    ("scripts/infra/terraform_wrapper.py", "prepare_configuration", "(target / 'backend_override.tf').write_text"): (1, TF_REASON),
    ("scripts/infra/terraform_wrapper.py", "approved_plan_copy", "tempfile.mkstemp"): (1, TF_REASON),
    ("scripts/infra/terraform_wrapper.py", "approved_plan_copy", "os.fdopen"): (1, TF_REASON),
    ("scripts/infra/terraform_wrapper.py", "write_private", "os.open"): (1, TF_REASON),
    ("scripts/infra/terraform_wrapper.py", "write_private", "os.fdopen"): (1, TF_REASON),
    ("scripts/infra/terraform_wrapper.py", "save_plan", "(folder / f'{label}_binding.json').write_text"): (1, TF_REASON),
    ("scripts/infra/terraform_wrapper.py", "record_created", "target.write_text"): (1, TF_REASON),
    ("scripts/ingestion/table_checks.py", "load_lock", "lock_path(manifest_sha256, directory).open"): (
        1,
        "The lock file of a load is opened to take a lock and holds no data; it is never replaced and is created in the state folder",
    ),
    ("scripts/quality/install_tools.py", "verified_archive", "partial.replace"): (
        1,
        "Renames the download that curl wrote into a partial file of the tools folder, after its SHA-256 matched the pinned value",
    ),
    ("scripts/quality/worktree_scan_hook.py", "install", "os.rename"): (
        1,
        "Moves the owner's older hook aside to a kept name, and only when that name is free; it replaces nothing",
    ),
    ("scripts/stack/capacity_sampler.py", "Output.__init__", "os.open"): (
        1,
        "Opens the sampler's output for append after check_new_output, with an exclusive create; the file grows line by line for hours",
    ),
    ("scripts/stack/tags.py", "stage_record", "tempfile.mkstemp"): (
        1,
        "Stages the run's record in the run folder before the tag is made, so every failure of the folder comes first; publish_record never replaces",
    ),
    ("scripts/stack/tags.py", "stage_record", "os.fdopen"): (
        1,
        "Stages the run's record in the run folder before the tag is made, so every failure of the folder comes first; publish_record never replaces",
    ),
    ("scripts/stack/tags.py", "publish_record", "os.link"): (
        1,
        "Publishes the staged record with a hard link, which fails when the name exists, so it never replaces a file",
    ),
    ("replay/service/folder_store.py", "FolderStore.note_read", "self.read_log.open"): (
        1,
        "Appends one line to the read log that a test of the folder store names; the log is evidence of the reads and is never replaced",
    ),
    ("replay/service/inputs.py", "VerifiedInputs.decode", "target.open"): (
        1,
        "Streams one verified object into a private scratch folder that is removed afterwards; a case file is too large to hold in memory",
    ),
    ("replay/service/serve.py", "held", "os.open"): (
        1,
        "Opens the lock file locks/<run ID> with create, to take a lock; it holds no data and is never replaced or removed",
    ),
    ("replay/service/serve.py", "archive", "os.replace"): (
        2,
        "Moves a request and its reference from the inbox into done/ or failed/, folders of the service's own state; the run ID is reserved once",
    ),
    ("replay/service/serve.py", "kept_outcome", "os.replace"): (
        1,
        "Moves a result file that is not valid aside under the suffix .invalid, inside the service's own results folder",
    ),
    ("spark/bronze/inputs.py", "fetch_case", "target.open"): (
        1,
        "Streams one case file into a scratch folder that the job made; a case file is too large to hold in memory",
    ),
    ("infra/iam/scripts/manage_policies.py", "write_new_key", "os.open"): (
        1,
        "Reserves the owner-only key backup path with an exclusive create before the key exists, so a failed write cannot lose a secret key",
    ),
    ("infra/iam/scripts/manage_policies.py", "write_new_key", "os.fdopen"): (
        1,
        "Reserves the owner-only key backup path with an exclusive create before the key exists, so a failed write cannot lose a secret key",
    ),
    ("orchestration/surgical_risk/run_folder.py", "write_record", "os.rename"): (
        1,
        "Renames the private staging folder of a run record, which holds a file made by write_new_file, to its final run folder name",
    ),
}


def mode_writes(call: ast.Call, positions: tuple[int, ...], strict: tuple[int, ...] = ()) -> bool:
    """Whether the mode argument of an ``open``-like call may write: a string with ``w``, ``a``, ``x`` or ``+``, or (at a ``strict`` position or by keyword)
    a mode that is not a string literal."""
    candidates = [(item.value, True) for item in call.keywords if item.arg == "mode"]
    candidates += [(call.args[position], position in strict) for position in positions if position < len(call.args)]
    for value, must_be_literal in candidates:
        if not isinstance(value, ast.Constant):
            if must_be_literal:
                return True
        elif isinstance(value.value, str) and OPEN_MODE.match(value.value) and WRITE_LETTERS & set(value.value.split(":")[0]):
            return True
    return False


def flags_write(call: ast.Call) -> bool:
    flags = [call.args[1]] if len(call.args) > 1 else [item.value for item in call.keywords if item.arg == "flags"]
    return any(
        isinstance(part, (ast.Attribute, ast.Name)) and (part.attr if isinstance(part, ast.Attribute) else part.id) in WRITE_FLAGS
        for item in flags
        for part in ast.walk(item)
    )


def writes_a_file(call: ast.Call) -> bool:
    function = call.func
    if isinstance(function, ast.Attribute):
        owner = ast.unparse(function.value)
        name = function.attr
        if name in WRITE_METHODS:
            return True
        if owner in {"os", "posix"} and name in OS_MOVES:
            return True
        if owner == "os" and name == "open":
            return flags_write(call)
        if owner == "os" and name == "fdopen" or owner == "io" and name == "open":
            return mode_writes(call, (1,), (1,))
        if owner == "shutil" and name in SHUTIL_WRITES:
            return True
        if name == "dump" and owner in DUMPS and len(call.args) + len(call.keywords) >= 2:
            return True
        if owner == "tempfile" and name in TEMPORARY_FILES:
            return True
        if name in {"replace", "rename"} and len(call.args) == 1 and not call.keywords:
            return True
        if name == "open":
            return mode_writes(call, (0, 1), (1,) if owner in ARCHIVES else ())
        return False
    if isinstance(function, ast.Name):
        if function.id == "open":
            return mode_writes(call, (1,), (1,))
        if function.id in TEMPORARY_FILES:
            return True
    return False


def enclosing(tree: ast.AST) -> dict[ast.AST, str]:
    names: dict[ast.AST, str] = {}

    def visit(node: ast.AST, scope: str) -> None:
        for child in ast.iter_child_nodes(node):
            inner = scope
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                inner = f"{scope}.{child.name}" if scope else child.name
            names[child] = inner
            visit(child, inner)

    visit(tree, "")
    return names


def write_calls(source: str) -> list[tuple[int, str, str]]:
    """The calls in ``source`` that write or move a file, as (line, enclosing function, call text)."""
    tree = ast.parse(source)
    scopes = enclosing(tree)
    found = [(node.lineno, scopes.get(node, ""), ast.unparse(node.func)) for node in ast.walk(tree) if isinstance(node, ast.Call) and writes_a_file(node)]
    return sorted(found)


def production_files() -> list[str]:
    names = []
    for folder in SCANNED:
        for path in sorted((ROOT / folder).rglob("*.py")) if (ROOT / folder).is_dir() else []:
            relative = path.relative_to(ROOT)
            if "tests" in relative.parts or ".venv" in relative.parts or "__pycache__" in relative.parts:
                continue
            names.append(relative.as_posix())
    return names


def inside_a_helper(name: str, scope: str) -> bool:
    return name == HELPER_FILE and scope.split(".")[0] in HELPERS


def violations(files: dict[str, str], allowed: dict[tuple[str, str, str], tuple[int, str]]) -> tuple[list[str], list[str]]:
    """The calls that are on neither helper nor list, and the list entries that match fewer or more calls than they say."""
    counts: dict[tuple[str, str, str], int] = {}
    outside = []
    for name, source in files.items():
        for line, scope, text in write_calls(source):
            if inside_a_helper(name, scope):
                continue
            key = (name, scope, text)
            counts[key] = counts.get(key, 0) + 1
            if key not in allowed:
                outside.append(f"{name}:{line} {scope or '<module>'}: {text}")
    stale = [f"{key}: says {count}, found {counts.get(key, 0)}" for key, (count, _) in allowed.items() if counts.get(key, 0) != count]
    return outside, stale


PLANTS = {
    "write_text": "def f(path):\n    path.write_text('x')\n",
    "write_bytes": "def f(path):\n    path.write_bytes(b'x')\n",
    "open for writing": "def f(path):\n    open(path, 'w')\n",
    "open for appending": "def f(path):\n    open(path, mode='a')\n",
    "open for creating": "def f(path):\n    open(path, 'x')\n",
    "open for update": "def f(path):\n    open(path, 'r+b')\n",
    "open with a mode that is not a literal": "def f(path, mode):\n    open(path, mode)\n",
    "io.open": "import io\n\n\ndef f(path):\n    io.open(path, 'w')\n",
    "Path.open": "def f(path):\n    path.open('w')\n",
    "Path.open with mode keyword": "def f(path):\n    path.open(mode='wb')\n",
    "gzip.open": "import gzip\n\n\ndef f(path):\n    gzip.open(path, 'wb')\n",
    "tarfile.open": "import tarfile\n\n\ndef f(path):\n    tarfile.open(path, 'w:gz')\n",
    "os.fdopen": "import os\n\n\ndef f(descriptor):\n    os.fdopen(descriptor, 'wb')\n",
    "os.open with flags to create": "import os\n\n\ndef f(path):\n    os.open(path, os.O_WRONLY | os.O_CREAT)\n",
    "os.open read and write": "import os\n\n\ndef f(path):\n    os.open(path, os.O_RDWR)\n",
    "os.open with flags by keyword": "import os\n\n\ndef f(path):\n    os.open(path, flags=os.O_TRUNC)\n",
    "os.replace": "import os\n\n\ndef f(a, b):\n    os.replace(a, b)\n",
    "os.rename": "import os\n\n\ndef f(a, b):\n    os.rename(a, b)\n",
    "Path.replace": "def f(path, target):\n    path.replace(target)\n",
    "Path.rename": "def f(path, target):\n    path.rename(target)\n",
    "shutil.copy2": "import shutil\n\n\ndef f(a, b):\n    shutil.copy2(a, b)\n",
    "shutil.copyfile": "import shutil\n\n\ndef f(a, b):\n    shutil.copyfile(a, b)\n",
    "shutil.copytree": "import shutil\n\n\ndef f(a, b):\n    shutil.copytree(a, b)\n",
    "shutil.move": "import shutil\n\n\ndef f(a, b):\n    shutil.move(a, b)\n",
    "json.dump": "import json\n\n\ndef f(value, handle):\n    json.dump(value, handle)\n",
    "pickle.dump": "import pickle\n\n\ndef f(value, handle):\n    pickle.dump(value, handle)\n",
    "os.link": "import os\n\n\ndef f(a, b):\n    os.link(a, b)\n",
    "os.symlink": "import os\n\n\ndef f(a, b):\n    os.symlink(a, b)\n",
    "Path.symlink_to": "def f(path, target):\n    path.symlink_to(target)\n",
    "Path.touch": "def f(path):\n    path.touch()\n",
    "tempfile.mkstemp": "import tempfile\n\n\ndef f():\n    tempfile.mkstemp()\n",
}
READS = {
    "open for reading": "def f(path):\n    open(path)\n    open(path, 'rb')\n    path.open('r')\n    path.open(newline='')\n",
    "os.open for reading": "import os\n\n\ndef f(path):\n    os.open(path, os.O_RDONLY | os.O_NONBLOCK)\n",
    "string replace": "def f(text):\n    return text.replace('a', 'b')\n",
    "json.dumps and loads": "import json\n\n\ndef f(value):\n    return json.loads(json.dumps(value))\n",
    "tarfile for reading": "import tarfile\n\n\ndef f(path):\n    tarfile.open(path, 'r:gz')\n",
}


class ScanFindsEachPatternTests(unittest.TestCase):
    def test_the_scan_finds_one_planted_write_for_each_pattern(self) -> None:
        missed = [name for name, source in PLANTS.items() if len(write_calls(source)) != 1]
        self.assertEqual([], missed)

    def test_the_scan_names_the_enclosing_function_and_the_call(self) -> None:
        source = "class Box:\n    def put(self, path):\n        path.write_text('x')\n"
        self.assertEqual([(3, "Box.put", "path.write_text")], write_calls(source))

    def test_the_scan_leaves_reads_and_string_edits_alone(self) -> None:
        found = {name: write_calls(source) for name, source in READS.items()}
        self.assertEqual({}, {name: calls for name, calls in found.items() if calls})

    def test_a_planted_write_outside_the_helpers_and_the_list_is_reported(self) -> None:
        outside, stale = violations({"scripts/new_module.py": PLANTS["write_text"]}, {})
        self.assertEqual(["scripts/new_module.py:2 f: path.write_text"], outside)
        self.assertEqual([], stale)

    def test_a_write_inside_a_helper_is_not_reported_and_the_same_call_elsewhere_in_the_helper_file_is(self) -> None:
        source = "def write_new_file(path):\n    path.write_bytes(b'x')\n\n\ndef other(path):\n    path.write_bytes(b'x')\n"
        outside, _ = violations({HELPER_FILE: source}, {})
        self.assertEqual([f"{HELPER_FILE}:6 other: path.write_bytes"], outside)

    def test_a_helper_name_in_another_file_does_not_excuse_a_write(self) -> None:
        source = "def write_new_file(path):\n    path.write_bytes(b'x')\n"
        outside, _ = violations({"scripts/other.py": source}, {})
        self.assertEqual(["scripts/other.py:2 write_new_file: path.write_bytes"], outside)

    def test_a_listed_call_passes_only_for_its_function_and_its_count(self) -> None:
        source = "def f(path):\n    path.write_text('x')\n    path.write_text('y')\n"
        listed = {("scripts/a.py", "f", "path.write_text"): (2, "a reason")}
        self.assertEqual(([], []), violations({"scripts/a.py": source}, listed))
        outside, stale = violations({"scripts/a.py": source + "    path.write_text('z')\n"}, listed)
        self.assertEqual(([], 1), (outside, len(stale)))
        moved = "def g(path):\n    path.write_text('x')\n    path.write_text('y')\n"
        outside, stale = violations({"scripts/a.py": moved}, listed)
        self.assertEqual(2, len(outside))
        self.assertEqual(1, len(stale))

    def test_a_list_entry_that_matches_no_call_is_stale(self) -> None:
        listed = {("scripts/a.py", "f", "path.write_text"): (1, "a reason")}
        _, stale = violations({"scripts/a.py": "def f(path):\n    return path.read_text()\n"}, listed)
        self.assertEqual(1, len(stale))

    def test_a_list_entry_with_the_wrong_count_is_stale(self) -> None:
        source = "def f(path):\n    path.write_text('x')\n    path.write_text('y')\n"
        listed = {("scripts/a.py", "f", "path.write_text"): (1, "a reason")}
        outside, stale = violations({"scripts/a.py": source}, listed)
        self.assertEqual([], outside)
        self.assertEqual(1, len(stale))


class ProductionWritesTests(unittest.TestCase):
    def test_the_scan_reaches_every_folder_of_production_code(self) -> None:
        names = production_files()
        for folder in ("scripts/", "replay/", "spark/", "features/", "infra/", "orchestration/"):
            self.assertTrue(any(name.startswith(folder) for name in names), folder)
        self.assertIn(HELPER_FILE, names)
        self.assertFalse([name for name in names if "/tests/" in name or name.startswith("tests/")])

    def test_no_call_writes_a_file_outside_the_helpers_or_the_list(self) -> None:
        files = {name: Path(ROOT / name).read_text() for name in production_files()}
        outside, stale = violations(files, ALLOWED)
        self.assertEqual([], outside)
        self.assertEqual([], stale)

    def test_every_list_entry_gives_a_reason(self) -> None:
        self.assertEqual([], [key for key, (count, reason) in ALLOWED.items() if count < 1 or len(reason.split()) < 4])


if __name__ == "__main__":
    unittest.main()
