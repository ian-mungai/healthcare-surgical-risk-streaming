"""The Bronze column-level checks, run in Great Expectations on the Spark tables of one Nessie hash.

``spark/bronze/checks.py`` holds the checks as data and decides the verdict. This module runs each of them as an expectation of the pinned release on the table
as the session's catalog holds it, and returns a count for each. The session is pinned to the hash that is checked, so a branch that moves cannot change what is
read. Great Expectations is imported when a check runs, because the image of the Spark jobs holds it and the tools environment does not.

Nothing leaves the process: the context is in memory, its usage statistics are off, no data documentation is built and no result keeps a value (the result
format asks for no unexpected values, so a result holds counts only). The tables are not persisted, so a check holds no second copy of a table in memory.
"""

from __future__ import annotations

import importlib
import os
from typing import Any

from spark.bronze.checks import Check, CheckResult, Verdict, plan, verdict
from spark.bronze.tables import TABLES

ANALYTICS_VARIABLE = "GX_ANALYTICS_ENABLED"
RESULT_FORMAT = {"result_format": "BASIC", "partial_unexpected_count": 0}
QUERY_RESULT_KEY = "observed_value"


class QualityError(RuntimeError):
    """The expectation library did not answer a check in the form that this module reads."""


def open_context() -> Any:
    """An in-memory Great Expectations context that sends nothing anywhere."""
    os.environ[ANALYTICS_VARIABLE] = "false"
    gx = importlib.import_module("great_expectations")
    context = gx.get_context(mode="ephemeral")
    context.enable_analytics(False)
    return context


def unexpected_count(result: Any, check: Check) -> int:
    """The number of unexpected rows or values in an expectation result. A failed result without a count is an error, never a pass."""
    found = result.result or {}
    if check.expectation == "ExpectTableRowCountToBeBetween":
        return 0 if result.success else 1
    count = found.get(QUERY_RESULT_KEY) if check.expectation == "UnexpectedRowsExpectation" else found.get("unexpected_count")
    if isinstance(count, int | float):
        return int(count)
    if result.success:
        return 0
    raise QualityError(f"the check {check.name} of {check.table} failed and its result holds no count")


def run_checks(session: Any, checks: list[Check] | None = None) -> Verdict:
    """Run ``checks`` (the whole plan when none are given) on the Bronze tables that the session's catalog reads, and return their verdict.

    A table that the catalog does not hold fails all of its checks (their count is ``-1`` and they have no result), so it is a failed verdict and not an error.
    """
    gx = importlib.import_module("great_expectations")
    wanted = plan() if checks is None else checks
    context = open_context()
    source = context.data_sources.add_spark(name="bronze", persist=False)
    results: list[CheckResult] = []
    for table in TABLES:
        mine = [check for check in wanted if check.table == table.name]
        if not mine or not session.catalog.tableExists(table.full_name):
            continue
        asset = source.add_dataframe_asset(name=table.name)
        definition = asset.add_batch_definition_whole_dataframe(table.name)
        batch = definition.get_batch(batch_parameters={"dataframe": session.table(table.full_name)})
        for check in mine:
            expectation = getattr(gx.expectations, check.expectation)(**check.kwargs)
            outcome = batch.validate(expectation, result_format=RESULT_FORMAT)
            results.append(CheckResult(check, bool(outcome.success), unexpected_count(outcome, check)))
    return verdict(wanted, results)
