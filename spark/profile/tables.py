"""The signal profile tables: their names, columns and keys, as data. Nothing here imports Spark.

The namespace ``silver`` of the catalog ``lake`` holds, besides the patient split and the two signal tables, three tables that this module defines. All of
them are measures of development patients (the training and validation sets of ``silver.patient_split``) and no row of any of them is a function of a test
patient, with the one exception that the sealed source counts say.

- ``signal_profile``: one row for each track of the development cases, by kind (``numeric`` or ``waveform``). ``rows`` are the readings or the seconds
  that Silver holds for the track, ``valid_rows`` those marked valid and each ``cause_*`` column those marked invalid with that cause, so that the valid
  rows and the cause columns add up to the rows. ``valid_share`` is ``valid_rows / rows``. ``cases`` is the number of development cases that hold the track.
- ``unscored_share``: one row, ``scope = development``. The cutoffs are the whole minutes of the arterial waveform's recorded span with a full 15-minute
  history (``spark.profile.unscored``). ``waveform`` and ``numeric`` count the cutoffs that break the limit of analysis decision 5 for that window, ``either``
  those that break one, and ``unscored_share`` is ``either / cutoffs``. The rule columns hold the limits that the count used.
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
        ("cutoffs", "bigint"),
        ("waveform", "bigint"),
        ("numeric", "bigint"),
        ("either", "bigint"),
        ("unscored_share", "double"),
        ("cutoff_step", "int"),
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
PROFILE_TABLES = {table.name: table for table in (PROFILE, UNSCORED, COUNTS)}
