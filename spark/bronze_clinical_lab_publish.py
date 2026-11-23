"""Publication of the clinical and laboratory Bronze branch: the checks of ``verify`` at the head of the branch, and a merge into Nessie's default branch only
when every one passed (``spark/bronze/publication.py`` has the sequence and its guards).

The checks are the job's own (``spark/bronze_clinical_lab_run.py``): the loaded tables did not change since the branch was made, the columns, the key, the row
count, the value of every row and, for the laboratory table, the occurrence numbers. Each belongs to a guard, so a stop names what failed.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from spark.bronze.checks import Check, CheckResult, Failure, Verdict
from spark.bronze.publication import Publication, PublishOps, publish
from spark.bronze_clinical_lab import BRONZE_NAMESPACE, TABLES
from spark.bronze_clinical_lab_run import NessieOps, SparkOps, run_job

GUARDS = {
    "source_changed": "source",
    "bronze table exists": "structure",
    "columns": "structure",
    "key not null": "key",
    "key unique": "key",
}
DEFAULT_GUARD = "equality"
EXPECTATION = "plain code"


def guard_of(check_name: str) -> str:
    return GUARDS.get(check_name, DEFAULT_GUARD)


def verdict_of(document: dict[str, Any]) -> Verdict:
    """The verdict of a ``verify`` result. A result that holds fewer tables than the job checks fails the guard ``structure``."""
    results: list[CheckResult] = []
    failed: list[Failure] = []
    for name, table in document["tables"].items():
        for item in table["checks"]:
            check = Check(name, guard_of(item["name"]), item["name"], EXPECTATION, {})
            results.append(CheckResult(check, bool(item["passed"]), 0 if item["passed"] else 1))
            if not item["passed"]:
                failed.append(Failure(name, item["name"], check.guard, 1))
    if len(document["tables"]) != len(TABLES):
        failed.append(Failure("tables", "every table is checked", "structure", -1))
    guards = {guard: not any(item.guard == guard for item in failed) for guard in ("key", "structure", "source", DEFAULT_GUARD)}
    return Verdict(tuple(results), tuple(failed), guards)


def publish_job(
    branch: str, source_hash: str, nessie: NessieOps, publisher: PublishOps, open_spark: Callable[[str, str, str, str | None], SparkOps]
) -> dict[str, Any]:
    """Run ``verify`` at the head of ``branch``, every read of the branch at that hash, and merge that head into the default branch when it passed.

    The tables of the job are the two Bronze tables, and the change from the default branch to the branch must be exactly those
    (``spark/bronze/publication.py``).
    A publication is a job of its own, so it does not know the snapshots that the write made. The document names counts and hashes only.
    """
    default = nessie.default_branch()
    documents: list[dict[str, Any]] = []

    def run_checks(branch_hash: str) -> Verdict:
        documents.append(run_job("verify", branch, source_hash, nessie, open_spark, branch_hash))
        return verdict_of(documents[-1])

    outcome: Publication = publish(publisher, run_checks, branch, default, {f"{BRONZE_NAMESPACE}.{bronze}": None for _, bronze, _ in TABLES.values()})
    document = documents[-1] if documents else {"tables": {}}
    return {
        **{key: value for key, value in document.items() if key in ("job", "branch", "default_branch", "source_hash", "tables")},
        "command": "publish",
        "passed": outcome.status != "blocked",
        "checked_hash": outcome.branch_hash,
        "publication": {
            "status": outcome.status,
            "base": outcome.base,
            "base_hash_before": outcome.base_hash_before,
            "merged_hash": outcome.merged_hash,
            "snapshots": outcome.snapshots,
            "blocked_by": outcome.blocked_by,
        },
    }
