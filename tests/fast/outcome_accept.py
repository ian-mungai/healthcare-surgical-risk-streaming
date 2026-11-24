"""The pass or fail rules that the end-to-end runs of the six outcome runners share, as plain functions on a job's output.

Nothing here imports Spark or Docker. The six runners (``OUTCOME_RUNNERS``) stay untracked, so each rule that decides from a job's output lives here,
where ``tests/fast/test_outcome_acceptance.py`` tests it.

A runner reads one file with a closed schema and never a line of a job's streams. ``read_result_file`` reads the file, and ``bronze_problem``,
``split_problem`` and ``cases_problem`` judge it: exactly the fields that the job writes at every level, Boolean flags, whole-number snapshot IDs, the job,
the command or mode and the branch that the runner asked for, and the exit status. The clinical and laboratory tables hold the outcome columns and the
creatinine results, so a result holds no count, digest or field that says whether a table was written. A write is read from the base hash and the branch
hash, an equality with the loaded table from the checks that compare them, and a rebuild after the source moved from the snapshot IDs, which the runner also
compares with Nessie's own (``snapshots_are``).
"""

from __future__ import annotations

import ast
import json
import os
import re
import stat
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from spark.bronze_clinical_lab_cli import CHECKS_FAILED, EXIT_CODES
from spark.bronze_clinical_lab_run import CHECK_NAMES, CHECKS_PASSED
from spark.jobs.silver_cases import READ_TABLES

RESULT_NAME = "result.json"
PART_NAME = f"{RESULT_NAME}.part"
RESULT_LIMIT_BYTES = 1_000_000
BRONZE_JOB = "bronze_clinical_lab"
BRONZE_COMMANDS = ("write", "verify")
BRONZE_TABLES = {
    "clinical_cases": ("clinical", "raw_records.clinical_data", "bronze.clinical_cases"),
    "lab_results": ("lab", "raw_records.lab_data", "bronze.lab_results"),
}
EQUALITY_CHECKS = ("row count", "no value changed", "key unique", "key not null")
REMOVED_FIELDS = ("outcome", "written", "patients", "counts", "rows", "source_rows", "keys_digest", "rows_digest", "digest", "checksum")
REMOVED_TOP_LEVEL = ("cases",)
CODE = re.compile(r"[a-z][a-z_]*")
HASH = re.compile(r"[0-9a-f]{64}")
# The exit status of a refusal of the patient split and the Silver cases jobs: a setting is 2, an unmapped error is 70 and any other code is 1.
SEALED_EXIT = {"settings": 2, "internal_error": 70}

Spec = Any


def is_hash(value: Any) -> bool:
    return isinstance(value, str) and HASH.fullmatch(value) is not None


def named(value: Any) -> bool:
    return isinstance(value, str) and bool(value)


def snapshot(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def fits(value: Any, spec: Spec) -> bool:
    """Whether ``value`` has exactly the form of ``spec``.

    A dict spec is closed: the value is a dict with exactly its keys, each of the form of its spec. A list spec of one item holds any number of items of that
    form. A tuple spec is a list of alternatives. A frozenset is a list of allowed text values. ``str``, ``int`` and ``bool`` are exact types (a bool is not a
    number, a float is not a whole number), ``None`` is none, a text is that text, and a function is a rule."""
    if isinstance(spec, dict):
        return isinstance(value, dict) and set(value) == set(spec) and all(fits(value[key], item) for key, item in spec.items())
    if isinstance(spec, list):
        return isinstance(value, list) and all(fits(item, spec[0]) for item in value)
    if isinstance(spec, tuple):
        return any(fits(value, item) for item in spec)
    if isinstance(spec, frozenset):
        return isinstance(value, str) and value in spec
    if spec is None:
        return value is None
    if isinstance(spec, str):
        return isinstance(value, str) and value == spec
    if spec in (str, int, bool):
        return type(value) is spec
    return bool(spec(value))


def pairs_without_repeat(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    names = [name for name, _ in pairs]
    if len(set(names)) != len(names):
        raise ValueError("a key is repeated")
    return dict(pairs)


def read_result_file(folder: Path) -> tuple[dict[str, Any] | None, str | None]:
    """The one result file that a job left in its fresh result folder, and why it is refused as a fixed text that holds nothing of it, or ``None``.

    A folder with no file is no result and no problem (a bad call and a job that the watchdog stopped write none); the caller decides whether one was due. A
    folder that holds only the part file is the same: a job that was killed after it reserved its result and before it renamed it. A
    folder that holds anything else than ``result.json`` is refused, as is a file that is not a plain file of at most ``RESULT_LIMIT_BYTES``, is not text, is
    not one JSON document with no key written twice, or is not an object."""
    names = sorted(os.listdir(folder))
    if not names or names == [PART_NAME]:
        return None, None
    if names != [RESULT_NAME]:
        return None, "the result folder holds a file other than the result"
    path = folder / RESULT_NAME
    try:
        info = os.lstat(path)
        if not stat.S_ISREG(info.st_mode) or info.st_size > RESULT_LIMIT_BYTES:
            return None, "the result is not a plain file of a size that is allowed"
        with open(path, "rb") as handle:
            document = json.loads(handle.read(RESULT_LIMIT_BYTES + 1).decode("utf-8"), object_pairs_hook=pairs_without_repeat)
    except OSError, ValueError, RecursionError:
        return None, "the result file is not one whole JSON document"
    if not isinstance(document, dict):
        return None, "the result is not one document"
    return document, None


def diagnostics_are(document: Mapping[str, Any], wanted: Sequence[str]) -> bool:
    items = document.get("diagnostics")
    return isinstance(items, list) and all(fits(item, {"code": str}) for item in items) and [item["code"] for item in items] == list(wanted)


def codes_are(document: Mapping[str, Any], wanted: Sequence[str]) -> bool:
    """Whether the codes of the diagnostics of a result are exactly the wanted ones, in order, each one a code and nothing else."""
    return diagnostics_are(document, wanted)


# --- Bronze clinical and laboratory

TABLE_SPEC: Spec = {
    "source_table": str,
    "bronze_table": str,
    "source_snapshot_id": (int, None),
    "bronze_snapshot_id": (int, None),
    "passed": bool,
    "checks": [{"name": frozenset(CHECK_NAMES), "passed": bool}],
}
RUN_SPEC: Spec = {
    "job": BRONZE_JOB,
    "command": str,
    "branch": str,
    "branch_hash": is_hash,
    "default_branch": named,
    "source_hash": is_hash,
    "passed": bool,
    "tables": {"clinical_cases": TABLE_SPEC, "lab_results": TABLE_SPEC},
    "diagnostics": [{"code": str}],
}
REFUSAL_SPEC: Spec = {"job": BRONZE_JOB, "command": str, "branch": str, "passed": bool, "diagnostics": [{"code": frozenset(EXIT_CODES)}]}


def sealed_table(table: Mapping[str, Any]) -> bool:
    """Whether a table of a Bronze result has exactly the fields that the job writes: the snapshot IDs, the flag and checks of a name and a flag, and no
    count, no digest, no metadata path and no check detail."""
    return fits(dict(table), TABLE_SPEC) and bool(table["checks"])


def passed_checks(table: Mapping[str, Any]) -> set[str]:
    return {check["name"] for check in table.get("checks") or [] if isinstance(check, Mapping) and check.get("passed") is True}


def table_problem(name: str, table: Mapping[str, Any]) -> str | None:
    kind, source_table, bronze_table = BRONZE_TABLES[name]
    names = [check["name"] for check in table["checks"]]
    if table["source_table"] != source_table or table["bronze_table"] != bronze_table:
        return "a table of the result is not the one that the job checks"
    if len(set(names)) != len(names) or not names:
        return "a check of a table is repeated or the table has none"
    if table["passed"] != all(check["passed"] for check in table["checks"]):
        return "the flag of a table does not agree with its checks"
    if table["passed"] and names != list(CHECKS_PASSED[kind]):
        return "a passed table does not hold exactly the checks of its kind"
    return None


def bronze_problem(document: Any, command: str, branch: str, exit_code: int) -> str | None:
    """Why a Bronze result file is not the closed schema of the job's result for this command and branch, as a fixed text that holds no value of it, or
    ``None``.

    There are two forms, both closed. A run (it holds ``tables``) holds the hashes, both tables and one diagnostic list that is empty when every table
    passed (exit 0) and is ``checks_failed`` when one did not (exit 1). A refusal holds the job, the command, the branch, ``passed`` false and one code
    of the closed list, with that code's exit status. The command and the branch are exactly the ones that the runner asked for."""
    if not isinstance(document, dict):
        return "the result is not one document"
    if command not in BRONZE_COMMANDS or document.get("command") != command or document.get("branch") != branch:
        return "a fixed value at the top of the result is not the expected one"
    if "tables" not in document:
        if not fits(document, REFUSAL_SPEC) or document["passed"] is not False:
            return "a refusal is not the closed schema"
        code = document["diagnostics"][0]["code"] if len(document["diagnostics"]) == 1 else None
        if code is None or EXIT_CODES[code] != exit_code:
            return "a refusal does not hold exactly one code that agrees with the exit status"
        return None
    if not fits(document, RUN_SPEC):
        return "the result is not the closed schema"
    for name, table in document["tables"].items():
        problem = table_problem(name, table)
        if problem is not None:
            return problem
    every = all(table["passed"] for table in document["tables"].values())
    if document["passed"] != every:
        return "the flag of the result does not agree with its tables"
    wanted = [] if every else [CHECKS_FAILED]
    if [item["code"] for item in document["diagnostics"]] != wanted or exit_code != (0 if every else 1):
        return "the diagnostics of the result do not agree with its flags and the exit status"
    return None


def equals_loaded(table: Mapping[str, Any]) -> bool:
    """Whether a table equals the loaded table: it is sealed, it passed, both snapshot IDs are named and the checks of the row count, the values, the key
    and its null check passed. The value check compares both tables as multisets, in both directions. It holds for the clinical and the laboratory
    table."""
    return (
        sealed_table(table)
        and table["passed"] is True
        and snapshot(table["source_snapshot_id"])
        and snapshot(table["bronze_snapshot_id"])
        and set(EQUALITY_CHECKS) <= passed_checks(table)
    )


def snapshots_are(table: Mapping[str, Any], source: int | None, bronze: int | None) -> bool:
    """Whether the snapshot IDs that a table reports are the ones that Nessie holds for the loaded table at the source hash and for the Bronze table on the
    branch. A missing ID (None) or a Boolean is never equal."""
    return (
        snapshot(source)
        and snapshot(bronze)
        and table.get("source_snapshot_id") == source
        and table.get("bronze_snapshot_id") == bronze
        and snapshot(table.get("source_snapshot_id"))
        and snapshot(table.get("bronze_snapshot_id"))
    )


def same_source(first: Mapping[str, Any], again: Mapping[str, Any]) -> bool:
    """Whether a second write read the same loaded table as the first and equals it."""
    return equals_loaded(first) and equals_loaded(again) and first["source_snapshot_id"] == again["source_snapshot_id"]


def moved(before: Mapping[str, Any], after: Mapping[str, Any]) -> bool:
    """Whether a write after the loaded table moved read a new source snapshot, wrote a new Bronze snapshot and equals the moved table."""
    return (
        equals_loaded(before)
        and equals_loaded(after)
        and after["source_snapshot_id"] != before["source_snapshot_id"]
        and after["bronze_snapshot_id"] != before["bronze_snapshot_id"]
    )


# --- Patient split and Silver cases


CASES_SNAPSHOT_TABLES = tuple(READ_TABLES)


def snapshot_map(value: Any) -> bool:
    """Whether a map of snapshot IDs holds exactly the seven tables that the Silver cases job reads, each a whole number (never ``None`` or a Boolean)."""
    return isinstance(value, dict) and set(value) == set(CASES_SNAPSHOT_TABLES) and all(snapshot(item) for item in value.values())


SPLIT_JOB, CASES_JOB = "patient_split", "silver_cases"
MODES = frozenset({"build", "verify"})
SPLIT_SPEC: Spec = {
    "mode": MODES,
    "branch": str,
    "base_hash": (is_hash, None),
    "pinned_release": bool,
    "table": "silver.patient_split",
    "source_table": "bronze.clinical_cases",
    "checked_hash": is_hash,
    "write_hash": (is_hash, None),
    "branch_hash": is_hash,
    "snapshot_id": snapshot,
    "source_snapshot_id": snapshot,
    "rule_version": named,
    "passed": {"patient_split": bool},
    "checks_run": [str],
    "fixed_sample_cases_checked": (int, None),
    "diagnostics": [],
}
CASES_TABLES = ("cases", "lab", "operation_length_measure", "score_cutoffs")
CASES_SPEC: Spec = {
    "mode": MODES,
    "branch": str,
    "base_hash": (is_hash, None),
    "branch_hash": is_hash,
    "pinned_release": bool,
    "checks_run": [str],
    "passed": dict.fromkeys(CASES_TABLES, bool),
    "development_lab_rows": int,
    "development_lab_copies": int,
    "development_cases": int,
    "snapshot_ids": snapshot_map,
    "measure": [{"statistic": str, "unit": str, "value": int}],
    "cutoffs": [
        {
            "cutoff": str,
            "anchor": str,
            "elapsed_seconds": (int, None),
            "basis": str,
            "rule_version": str,
            "development_cases": int,
            "median_length_seconds": int,
        }
    ],
    "diagnostics": [],
}
SEALED_REFUSAL_SPEC: Spec = {"mode": MODES, "branch": str, "job": str, "diagnostics": [{"code": CODE.fullmatch}]}


def sealed_problem(document: Any, spec: Spec, job: str, mode: str, branch: str, exit_code: int) -> str | None:
    """Why a result file of the patient split or the Silver cases job is not the closed schema of the job's result, as a fixed text, or ``None``.

    A clean result holds the facts of the job and no diagnostic (exit 0), and every pass flag is true. A refusal holds the mode, the branch, the job and one
    diagnostic for each code, each exactly a code and no count, with exit 70 for ``internal_error``, 2 for ``settings`` and 1 for the others. The mode and the
    branch are exactly the ones that the runner asked for."""
    if not isinstance(document, dict):
        return "the result is not one document"
    if document.get("mode") != mode or document.get("branch") != branch:
        return "a fixed value at the top of the result is not the expected one"
    if document.get("diagnostics"):
        if not fits(document, SEALED_REFUSAL_SPEC) or document["job"] != job:
            return "a refusal is not the closed schema"
        wanted = {SEALED_EXIT.get(item["code"], 1) for item in document["diagnostics"]}
        return None if wanted == {exit_code} else "the codes of a refusal do not agree with the exit status"
    if not fits(document, spec):
        return "the result is not the closed schema"
    if exit_code != 0:
        return "a clean result does not agree with the exit status"
    flags = document["passed"]
    return None if all(value is True for value in flags.values()) else "a pass flag of a clean result is not true"


def split_problem(document: Any, mode: str, branch: str, exit_code: int) -> str | None:
    return sealed_problem(document, SPLIT_SPEC, SPLIT_JOB, mode, branch, exit_code)


def cases_problem(document: Any, mode: str, branch: str, exit_code: int) -> str | None:
    return sealed_problem(document, CASES_SPEC, CASES_JOB, mode, branch, exit_code)


def wrote(result: Mapping[str, Any]) -> bool:
    """Whether a build moved its branch off the base: it names both hashes and they differ."""
    base, branch = result.get("base_hash"), result.get("branch_hash")
    return is_hash(base) and is_hash(branch) and base != branch


def kept_at_base(result: Mapping[str, Any]) -> bool:
    """Whether a build wrote nothing: the branch is at the base hash."""
    base, branch = result.get("base_hash"), result.get("branch_hash")
    return is_hash(base) and is_hash(branch) and base == branch


def all_passed(result: Mapping[str, Any], tables: Sequence[str]) -> bool:
    """Whether the result has one pass flag for each of ``tables``, each the Boolean true, and no other."""
    flags = result.get("passed")
    return isinstance(flags, Mapping) and set(flags) == set(tables) and all(flags[table] is True for table in tables)


def keys_of(value: Any) -> set[str]:
    if isinstance(value, Mapping):
        return {str(key) for key in value} | {name for item in value.values() for name in keys_of(item)}
    if isinstance(value, list | tuple):
        return {name for item in value for name in keys_of(item)}
    return set()


def no_removed_fields(result: Mapping[str, Any]) -> bool:
    """Whether the result holds none of the fields that a held-out outcome could change: no outcome, no written flag, no count and no digest at any depth, and
    no count of all cases at the top (``cases`` is also the name of a table in the pass flags)."""
    return not keys_of(result) & set(REMOVED_FIELDS) and not set(result) & set(REMOVED_TOP_LEVEL)


# --- The untracked runners

STREAMS = r"(?:out|err|output|stdout|stderr)"
# The keys of a result or a stand-in's answer that hold a count, a list of rows or causes, a digest or a length: a runner may compare them and never print them.
COUNT_KEYS = (
    "rows|columns|patients|csv_rows|copies|missing|extra|copies_differ|sampled_cases|sampled_found|cells_wrong|lab_columns|fixed_sample_cases_checked"
    "|development_cases|development_lab_rows|development_lab_copies|lab_development_rows|lab_development_collapsed"
    "|bronze_numeric|bronze_duplicates|bronze_waveform|silver_numeric|silver_waveform|numeric_causes|waveform_causes|arterial_seconds|series_rows"
    "|series_absent|series_mismatch|clinical_rows|split_rows|waveform_rows|duplicate_keys|seconds|expected_events|stored_events|probes|plant_events"
    "|planted|tracks_added|expected_rows|absent|verdict_mismatch|sample_mismatch|field_mismatch|ledger_mismatch|value_mismatch"
    "|arterial_track_of_the_plant_case|numeric|waveform"
)
SPEC_CALL = "write_spec("
# A key that names itself as computed from development cases alone may hold a count (the Silver cases job writes ``development_cases``).
DEVELOPMENT_KEY = re.compile(r"[\"'](?:lab_)?development_[a-z_]+[\"']:")
COMPARED = r"[^()=<>!]*(?:\([^()]*\)[^()=<>!]*)*"
RUNNER_RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("stream_length", re.compile(rf"\blen\((?:os\.fsencode\()?\s*{STREAMS}\b")),
    ("stream_tail", re.compile(rf"\b{STREAMS}\[-\d+:\]")),
    ("stream_logged", re.compile(rf"\{{{STREAMS}\}}")),
    ("digest_saved", re.compile(r"(?:facts|environment)[^#\n]*digest|digest[^#\n]*\b(?:facts|environment)\b", re.IGNORECASE)),
    ("count_in_result", re.compile(rf"\.get\(['\"](?:{COUNT_KEYS})['\"]\)(?!\s*[=!]=)")),
    ("count_printed", re.compile(r"print\(.*\"[a-z_]+\": [^,}=]*(?:\.count\(\)|\blen\()[^,}=]*(?=[,}])")),
    (
        "count_emitted",
        re.compile(rf"[\"'][a-z_]+[\"']:\s*(?:[^,}}=]*(?:\.count\(\)(?!\s*[=!<>])|\blen\((?!{COMPARED}\)\s*[=!<>]))|[^,}}=]*\b(?:int|sum)\({COMPARED}\))"),
    ),
    ("data_identifier", re.compile(r"\b(?:differing_group|zero_cell|lab_key)\b")),
    ("count_in_text", re.compile(r"\bf(?:\"[^\"]*|'[^']*)\{[^{}=<>]*\blen\([^{}=<>]*\}")),
    ("refusal_count", re.compile(r"[\"'][a-z]+(?:_[a-z]+)*: \d")),
)


def runner_leaks(source: str) -> list[tuple[str, int]]:
    """The rule and the 1-based line of each line of a runner that records what the sealed outputs forbid, one rule for each line (the first that matches).

    The rules: the length of a stream (``stream_length``), a tail of one (``stream_tail``), a stream put into a text (``stream_logged``), a digest in a saved
    fact (``digest_saved``), a count that a result or a stand-in answers (``count_in_result``, ``count_printed``) or emits (``count_emitted``), an identifier
    that was chosen by looking at data (``data_identifier``), a length in an f-string (``count_in_text``) and a refusal code with a count after it
    (``refusal_count``). A comparison
    is not a leak (``==``, ``!=`` or an order in a line, or a count that is only compared): only whether it matched may be recorded. A comment is not read. A
    ``write_spec(`` call holds the values that go into the scratch folder, not into a result, so ``count_emitted`` skips it, and so a key that begins
    ``development_`` (``DEVELOPMENT_KEY``)."""
    found: list[tuple[str, int]] = []
    depth = 0
    for number, line in enumerate(source.splitlines(), 1):
        if line.lstrip().startswith("#"):
            continue
        start = line.find(SPEC_CALL) if depth == 0 else 0
        inside_spec = depth > 0 or start >= 0
        if inside_spec:
            depth = max(depth + line[max(start, 0) :].count("(") - line[max(start, 0) :].count(")"), 0)
        for name, pattern in RUNNER_RULES:
            if name == "count_emitted" and (inside_spec or DEVELOPMENT_KEY.search(line)):
                continue
            if pattern.search(line):
                found.append((name, number))
                break
    return found


# --- The sealed helper: what a runner may print or save

OUTCOME_RUNNERS = (
    "bronze_clinical_lab.py",
    "bronze_clinical_lab_container.py",
    "silver_patient_split.py",
    "silver_cases.py",
    "silver_signals.py",
    "silver_hypotension.py",
)
SEALED_CALL = "sealed_observed"
RAW_TEXT_CALLS = frozenset({"str", "repr", "format"})
FACT_NAMES = frozenset({"facts", "environment"})


def outcome_runner_paths(folder: Path) -> list[Path]:
    """The files of the six outcome runners that are in ``folder``, by name and in the order of ``OUTCOME_RUNNERS``. Another local runner is not read."""
    return [folder / name for name in OUTCOME_RUNNERS if (folder / name).is_file()]


def call_name(node: ast.AST) -> str | None:
    """The last name of what a call calls (``expect`` for ``self.record.expect(...)``), or ``None``."""
    if not isinstance(node, ast.Call):
        return None
    func = node.func
    return func.id if isinstance(func, ast.Name) else func.attr if isinstance(func, ast.Attribute) else None


def is_sealed_call(node: ast.AST) -> bool:
    return call_name(node) == SEALED_CALL


def is_fixed_text(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and isinstance(node.value, str)


def is_fixed_choice(node: ast.AST) -> bool:
    """A text that is written in the runner: a string constant, or a choice between two of them."""
    return is_fixed_text(node) or (isinstance(node, ast.IfExp) and is_fixed_text(node.body) and is_fixed_text(node.orelse))


def fact_target(node: ast.AST) -> str | None:
    """``facts`` or ``environment`` when the node names one of them, as a variable, an attribute or an item of either."""
    if isinstance(node, ast.Subscript):
        return fact_target(node.value)
    name = node.id if isinstance(node, ast.Name) else node.attr if isinstance(node, ast.Attribute) else None
    return name if name in FACT_NAMES else None


def sealed_facts(node: ast.AST) -> bool:
    """The facts of one sealed text (``sealed_observed(...).facts``), or the accumulated facts of the run itself, which hold nothing else by this rule."""
    if isinstance(node, ast.Attribute) and node.attr == "facts" and is_sealed_call(node.value):
        return True
    return fact_target(node) is not None and not isinstance(node, ast.Subscript)


def fact_value_problem(target: ast.AST, value: ast.AST | None) -> bool:
    """Whether the value written to a fact target is anything but written text, empty facts or the facts of the run."""
    if value is None:
        return False
    if isinstance(target, ast.Subscript):
        return not is_fixed_choice(value)
    if isinstance(value, ast.Dict):
        return not all(key is not None and is_fixed_text(key) and is_fixed_choice(item) for key, item in zip(value.keys, value.values, strict=True))
    if isinstance(value, ast.Call):
        return not (call_name(value) == "run_local" or (call_name(value) == "dict" and not value.keywords and all(sealed_facts(arg) for arg in value.args)))
    return not sealed_facts(value)


def raw_text_in(node: ast.AST) -> bool:
    """Whether an f-string, ``str()``, ``repr()`` or ``format`` makes text anywhere under the node."""
    return any(isinstance(item, ast.JoinedStr) or call_name(item) in RAW_TEXT_CALLS for item in ast.walk(node))


def constant_keys(node: ast.AST) -> bool:
    return isinstance(node, ast.Dict) and all(key is not None and is_fixed_text(key) for key in node.keys)


def sealed_call_problem(call: ast.Call) -> str | None:
    """The rule that a call of ``sealed_observed`` breaks, or ``None``: flags and exit names are constants, a confirmed value is never a constant or text made
    by the runner, and no text is made inside the call."""
    flags = call.args[0] if call.args else next((item.value for item in call.keywords if item.arg == "flags"), None)
    named = {item.arg: item.value for item in call.keywords}
    if (flags is not None and not constant_keys(flags)) or ("exits" in named and not constant_keys(named["exits"])):
        return "flag_name_not_constant"
    confirmed = named.get("confirmed")
    if confirmed is not None and (
        isinstance(confirmed, (ast.Constant, ast.JoinedStr, ast.BinOp))
        or (isinstance(confirmed, ast.Dict) and any(isinstance(item, (ast.Constant, ast.JoinedStr, ast.BinOp)) for item in confirmed.values))
    ):
        return "confirmed_not_made"
    return "raw_in_sealed" if raw_text_in(call) else None


def is_print(call: ast.Call) -> bool:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id == "print"
    return isinstance(func, ast.Attribute) and func.attr == "write" and isinstance(func.value, ast.Attribute) and func.value.attr == "stdout"


def sealed_line(node: ast.AST) -> bool:
    """The printed line of one sealed text: ``sealed_observed(...).line(...)``."""
    return isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "line" and is_sealed_call(node.func.value)


def printed_problem(call: ast.Call) -> bool:
    """Whether a printed line holds anything but written text or the line of a sealed text."""
    return any(not (is_fixed_choice(arg) or sealed_line(arg)) for arg in call.args)


def sink_problem(node: ast.AST) -> str | None:
    """The rule that a node breaks, or ``None``. A sink is an observation (the fourth argument of ``expect``), a saved fact, a printed line or a
    ``sealed_observed`` call itself."""
    if isinstance(node, ast.Call):
        name = call_name(node)
        if name == "expect":
            observed = node.args[3] if len(node.args) > 3 else next((item.value for item in node.keywords if item.arg == "observed"), None)
            return "observation_not_sealed" if observed is not None and not (is_fixed_text(observed) or is_sealed_call(observed)) else None
        if name == "ConfirmedId":
            return "forged_id"
        if name == SEALED_CALL:
            return sealed_call_problem(node)
        if is_print(node):
            return "printed_raw" if printed_problem(node) else None
        if name == "update" and isinstance(node.func, ast.Attribute) and fact_target(node.func.value):
            return "fact_not_sealed" if node.keywords or not all(sealed_facts(arg) for arg in node.args) else None
    targets: list[tuple[ast.AST, ast.AST | None]] = []
    if isinstance(node, ast.Assign):
        targets = [(target, node.value) for target in node.targets]
    elif isinstance(node, ast.AnnAssign):
        targets = [(node.target, node.value)]
    for target, value in targets:
        if fact_target(target) and fact_value_problem(target, value):
            return "fact_not_sealed"
    return None


def sink_leaks(source: str) -> list[tuple[str, int]]:
    """The rule and the 1-based line of each observation, saved fact or printed line of a runner that is not the output of ``sealed_observed``.

    The rules, in line order:

    - ``observation_not_sealed``: an observation that is not written text or a sealed text.
    - ``fact_not_sealed``: a fact written by hand.
    - ``printed_raw``: a printed line that is not written text or the line of a sealed text.
    - ``forged_id``: a ``ConfirmedId`` made by hand.
    - ``flag_name_not_constant``: a flag or exit name that is not a constant.
    - ``confirmed_not_made``: a confirmed value that is a constant or made text.
    - ``raw_in_sealed``: an f-string, ``str()``, ``repr()`` or ``format`` inside a sealed call.

    A source that does not parse raises ``SyntaxError``."""
    found = [(problem, getattr(node, "lineno", 0)) for node in ast.walk(ast.parse(source)) if (problem := sink_problem(node)) is not None]
    return sorted(found, key=lambda item: item[1])
