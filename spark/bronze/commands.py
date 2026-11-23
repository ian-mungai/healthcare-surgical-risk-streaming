"""The commands of the Bronze decode job that need no Spark: the report of a check or a publication, the text of a stop and the rules for the arguments.

The job (``spark/jobs/bronze_decode.py``) passes the Nessie operations and the checker in, so the whole report is made on fakes in the fast tests.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from spark.bronze.checks import Verdict, result_document
from spark.bronze.publication import PublishOps, publish, verify


def command_problem(command: str, has_manifest: bool, has_cases: bool, has_landed_folder: bool, publish_flag: bool) -> str | None:
    """Why the arguments do not fit the command, or ``None``.

    ``write`` needs the manifest reference. ``verify`` and ``publish`` take the branch and the base only.
    """
    if command == "write":
        return None if has_manifest else "write needs --manifest-reference"
    if has_manifest or has_cases or has_landed_folder or publish_flag:
        return f"{command} takes --branch and --base only"
    return None


def branch_report(
    ops: PublishOps, run_checks: Callable[[str], Verdict], branch: str, base: str, expected: Mapping[str, int | None], *, publishing: bool
) -> dict[str, Any]:
    """The checks of the branch at its head and, when ``publishing``, its merge into the base. The document holds names, counts and hashes, never a value.

    ``expected`` maps the content key of each table that the job wrote to the snapshot it wrote, or to ``None`` where this run did not write it.
    """
    if not publishing:
        branch_hash, result = verify(ops, run_checks, branch)
        return {"checked_hash": branch_hash, "passed": result.passed, "checks": result_document(result)}
    outcome = publish(ops, run_checks, branch, base, expected)
    return {
        "checked_hash": outcome.branch_hash,
        "passed": outcome.status != "blocked",
        "checks": result_document(outcome.verdict) if outcome.verdict is not None else None,
        "publication": {
            "status": outcome.status,
            "base": outcome.base,
            "base_hash_before": outcome.base_hash_before,
            "merged_hash": outcome.merged_hash,
            "snapshots": outcome.snapshots,
            "blocked_by": outcome.blocked_by,
        },
    }


def failed_text(document: dict[str, Any]) -> str:
    """The failed checks and the guards that stopped a publication, as one line of names and counts."""
    failed = (document.get("checks") or {}).get("failed", [])
    stopped = (document.get("publication") or {}).get("blocked_by", [])
    return "; ".join([f"{item['table']}: {item['name']} ({item['guard']} guard, {item['unexpected']} unexpected)" for item in failed] + stopped)


def exit_code(document: dict[str, Any]) -> int:
    return 0 if document["passed"] else 1
