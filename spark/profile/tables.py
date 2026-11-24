"""The signal profile tables: their names, columns and keys, as data. Nothing here imports Spark.

The namespace ``silver`` of the catalog ``lake`` holds, besides the patient split and the two signal tables, four tables that this module defines. Three of
them are measures of development patients (the training and validation sets of ``silver.patient_split``) and no row of any of them is a function of a test
patient, with the one exception that the sealed source counts say. The fourth, ``warning_grid_rule``, holds no case, patient or count.

- ``warning_grid_rule``: one row, the rule that sets the one-minute warning grid of a case (``spark.profile.grid_rule``): its version and text, each of its
  constants as a column, the snapshot ID of the frozen patient split that the job read and the time of the write. The job writes it before it measures the
  unscored share and never changes it: a rerun whose rule or split snapshot differs refuses.

- ``signal_profile``: one row for each track of the development cases, by kind (``numeric`` or ``waveform``). ``rows`` are the readings or the seconds
  that Silver holds for the track, ``valid_rows`` those marked valid and each ``cause_*`` column those marked invalid with that cause, so that the valid
  rows and the cause columns add up to the rows. ``valid_share`` is ``valid_rows / rows``. ``cases`` is the number of development cases that hold the track.
- ``unscored_share``: one row, ``scope = development``, of the development cases that hold the arterial waveform. The cutoffs are those of the saved warning
  grid of each case (``warning_grid_rule``: from ``opstart`` in 60-second steps through the latest at or before ``opend`` minus 300 seconds), and a cutoff
  whose history is not recorded is unscored (``spark.profile.unscored``). ``waveform`` and ``numeric`` count the cutoffs that break the limit of analysis
  decision 5 for that window, ``either`` those that break one, and ``unscored_share`` is ``either / cutoffs``. ``no_grid`` counts the cases (of ``cases``) with
  no ``opstart``, no ``opend`` or an ``opend`` that is not after ``opstart``: they hold no cutoff. The rule columns hold the grid's step, first cutoff
  offset and last cutoff margin and the limits that the count used.
- ``source_counts``: one row for each prespecified source count (``spark.profile.counts``): the value that the guide or the standard names, whether the
  reproduced count equals it, and its input, method and denominator. A count of the full cohort is sealed: it is saved as a flag only and its ``value`` is null.
"""

from __future__ import annotations

from features.signal_validity import INVALID_CAUSES
from spark.silver.tables import SilverTable

DEVELOPMENT_SCOPE = "development"
NUMERIC_KIND = "numeric"
WAVEFORM_KIND = "waveform"
KINDS = (NUMERIC_KIND, WAVEFORM_KIND)
CAUSE_COLUMNS = tuple(f"cause_{cause}" for cause in INVALID_CAUSES)

PROFILE = SilverTable(
    "signal_profile",
    (
        ("kind", "string"),
        ("track", "string"),
        ("cases", "bigint"),
        ("rows", "bigint"),
        ("valid_rows", "bigint"),
        ("valid_share", "double"),
        *((name, "bigint") for name in CAUSE_COLUMNS),
    ),
    ("kind", "track"),
)
UNSCORED = SilverTable(
    "unscored_share",
    (
        ("scope", "string"),
        ("cases", "bigint"),
        ("no_grid", "bigint"),
        ("cutoffs", "bigint"),
        ("waveform", "bigint"),
        ("numeric", "bigint"),
        ("either", "bigint"),
        ("unscored_share", "double"),
        ("cutoff_step", "int"),
        ("cutoff_offset", "int"),
        ("last_margin", "int"),
        ("waveform_window", "int"),
        ("waveform_limit", "int"),
        ("numeric_window", "int"),
        ("numeric_limit", "int"),
    ),
    ("scope",),
)
COUNTS = SilverTable(
    "source_counts",
    (
        ("name", "string"),
        ("scope", "string"),
        ("expected", "double"),
        ("value", "double"),
        ("matches", "boolean"),
        ("sealed", "boolean"),
        ("input", "string"),
        ("method", "string"),
        ("denominator", "string"),
    ),
    ("name",),
)
GRID_RULE = SilverTable(
    "warning_grid_rule",
    (
        ("rule_version", "string"),
        ("rule_text", "string"),
        ("grid_origin", "string"),
        ("grid_step_seconds", "int"),
        ("first_cutoff_offset_seconds", "int"),
        ("last_cutoff_margin_seconds", "int"),
        ("split_snapshot_id", "bigint"),
        ("written_at", "timestamp"),
    ),
    ("rule_version",),
)
PROFILE_TABLES = {table.name: table for table in (PROFILE, UNSCORED, COUNTS)}
