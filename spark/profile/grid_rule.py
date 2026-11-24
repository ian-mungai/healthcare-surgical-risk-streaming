"""The warning grid rule that the signal profile saves in ``silver.warning_grid_rule``: its constants, its text, its row and the check of a stored row. No
Spark runs here.

The rule sets the one-minute warning grid of a case, including how its first and its last cutoff are set. The job writes it as a one-row table, with the
snapshot ID of the frozen patient split that it read, before it measures the unscored share. It never changes the split. A rerun with the same rule and
split changes nothing; a rerun whose rule or split differs from the stored row refuses with ``grid_rule_changed`` and writes nothing (owner, Nov 23 2026
PST: "B").

**Status: decided.** The owner decided the values below on Nov 23 2026 PST ("A"; project standard, Data, "Warning grid rule"). They come from the approved
analysis plan ("Sample grid": one sample at each fixed 1-minute elapsed time; "Eligible monitored time": between ``opstart`` and ``opend``; the primary label
window is 5 minutes). The unscored share is measured on this grid and reads these constants and no copy of them: the cutoffs, the step, the first cutoff, the
last cutoff margin and the names of the origin and end columns (``spark.profile.unscored``, ``spark.profile.frames``).
"""

from __future__ import annotations

from dataclasses import dataclass, fields

GRID_RULE_VERSION = "warning-grid-1"
GRID_ORIGIN = "opstart"
GRID_STEP_SECONDS = 60
FIRST_CUTOFF_OFFSET_SECONDS = 0
LAST_CUTOFF_MARGIN_SECONDS = 300
RULE_END = "opend"

ROWS_CODE = "grid_rule_rows"
CHANGED_CODE = "grid_rule_changed"


def unit(count: int, noun: str) -> str:
    """A count and its noun. It is here and not imported, so that the Spark worker, which imports the grid, needs the standard library only."""
    return f"{count} {noun}{'' if count == 1 else 's'}"


@dataclass(frozen=True, slots=True)
class GridRule:
    """The columns of the rule row, in table order, except the time of the write. It holds no case, patient or count."""

    rule_version: str
    rule_text: str
    grid_origin: str
    grid_step_seconds: int
    first_cutoff_offset_seconds: int
    last_cutoff_margin_seconds: int
    split_snapshot_id: int


@dataclass(frozen=True, slots=True)
class RuleRead:
    """The rule table at one hash: its row count, its first row (``None`` for no row), its snapshot ID and whether every row has a time of write."""

    rows: int
    rule: GridRule | None
    snapshot_id: int | None
    dated: bool


def rule_text(origin: str, step: int, first_offset: int, last_margin: int) -> str:
    """The rule in plain words, from the four values."""
    return (
        f"The grid of a case starts at {origin}. One cutoff falls each {step} seconds. "
        f"The first cutoff is at {origin} plus {first_offset} seconds. "
        f"The last cutoff is the latest grid time at or before {RULE_END} minus {last_margin} seconds."
    )


def planned_rule(split_snapshot_id: int) -> GridRule:
    """The row of the constants, with the snapshot ID of the patient split that the job read."""
    return GridRule(
        GRID_RULE_VERSION,
        rule_text(GRID_ORIGIN, GRID_STEP_SECONDS, FIRST_CUTOFF_OFFSET_SECONDS, LAST_CUTOFF_MARGIN_SECONDS),
        GRID_ORIGIN,
        GRID_STEP_SECONDS,
        FIRST_CUTOFF_OFFSET_SECONDS,
        LAST_CUTOFF_MARGIN_SECONDS,
        split_snapshot_id,
    )


def rule_problems(read: RuleRead, wanted: GridRule) -> list[str]:
    """The problems of a stored rule table against the planned row: not one row, or a row that differs in a column or has no time of write. Each problem is a
    code and a count and holds no value."""
    if read.rows != 1 or read.rule is None:
        return [f"{ROWS_CODE}: {unit(read.rows, 'row')}"]
    differing = sum(1 for field in fields(wanted) if getattr(read.rule, field.name) != getattr(wanted, field.name)) + (0 if read.dated else 1)
    return [f"{CHANGED_CODE}: {unit(differing, 'column')}"] if differing else []
