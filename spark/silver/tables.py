"""The Silver signal tables: their names, columns and keys, as data. Nothing here imports Spark.

The namespace ``silver`` of the catalog ``lake`` holds, besides the patient split, two tables that this module defines. Both keep Bronze's keys, the raw source
IDs, and Bronze's lineage columns, and neither fills, interpolates or changes a value that the case file holds, apart from the one standard conversion of a raw
waveform sample to its physical value.

- ``numeric_observations``: one row for each numeric reading that is the first of its time. Key: case, track and case time in microseconds. A second reading of
  the same track and time is a duplicate (``features.signal_validity.is_duplicate``): it is dropped, and its ``source_occurrence`` is in
  ``duplicate_source_occurrences`` of the reading that was kept, so every Bronze row stays accounted for. ``valid`` and ``invalid_cause`` are the verdict of
  ``numeric_verdict``.
- ``waveform_seconds``: one row for each second that Bronze holds. Key: case, track and second. ``samples`` are the physical values of the raw samples with
  the track's gain and offset (``physical_samples``), ``sample_rate`` is the track's, ``sample_count`` is the number of samples that the second holds, and
  ``valid`` and ``invalid_cause`` are the verdict of ``waveform_verdict``.

The job ``spark/jobs/silver_hypotension.py`` builds two more tables from ``waveform_seconds`` and the clinical table (``HYPOTENSION_TABLES``):

- ``art_mbp_1s``: one row for each second of ``SNUADC/ART``, the trailing 2-second average (``features.pressure_series``). Key: case and second. The reading of
  second ``s`` is known at the end of ``s``. ``value`` is null when the reading is invalid, and ``invalid_cause`` is the cause then.
- ``hypotension_events``: one row for each run of at least 60 consecutive valid seconds of the series below 65 mmHg inside the operation
  (``features.hypotension``). Key: case and onset second. It holds every time the event used: the onset and confirmation times, the end of the run and the
  operation start and end that bounded it, and why the run ended.
"""

from __future__ import annotations

from dataclasses import dataclass

from spark.bronze.tables import LINEAGE_COLUMNS

CATALOG = "lake"
NAMESPACE = "silver"


@dataclass(frozen=True, slots=True)
class SilverTable:
    name: str
    columns: tuple[tuple[str, str], ...]
    key: tuple[str, ...]

    @property
    def table_key(self) -> tuple[str, str]:
        """The key of the table in Nessie's content: its namespace and name."""
        return (NAMESPACE, self.name)


NUMERIC = SilverTable(
    "numeric_observations",
    (
        ("case_id", "bigint"),
        ("track", "string"),
        ("case_time_us", "bigint"),
        ("source_occurrence", "int"),
        ("source_time", "double"),
        ("value", "double"),
        ("duplicate_source_occurrences", "array<int>"),
        ("valid", "boolean"),
        ("invalid_cause", "string"),
        *LINEAGE_COLUMNS,
    ),
    ("case_id", "track", "case_time_us"),
)
WAVEFORM = SilverTable(
    "waveform_seconds",
    (
        ("case_id", "bigint"),
        ("track", "string"),
        ("case_second", "bigint"),
        ("start_offset_us", "bigint"),
        ("sample_rate", "double"),
        ("sample_count", "int"),
        ("samples", "array<double>"),
        ("valid", "boolean"),
        ("invalid_cause", "string"),
        *LINEAGE_COLUMNS,
    ),
    ("case_id", "track", "case_second"),
)
ART_MBP_1S = SilverTable(
    "art_mbp_1s",
    (
        ("case_id", "bigint"),
        ("case_second", "bigint"),
        ("value", "double"),
        ("valid", "boolean"),
        ("invalid_cause", "string"),
        *LINEAGE_COLUMNS,
    ),
    ("case_id", "case_second"),
)
HYPOTENSION_EVENTS = SilverTable(
    "hypotension_events",
    (
        ("case_id", "bigint"),
        ("onset_second", "bigint"),
        ("onset_us", "bigint"),
        ("confirmed_us", "bigint"),
        ("last_low_second", "bigint"),
        ("end_us", "bigint"),
        ("duration_seconds", "int"),
        ("end_reason", "string"),
        ("opstart_s", "double"),
        ("opend_s", "double"),
        *LINEAGE_COLUMNS,
    ),
    ("case_id", "onset_second"),
)
SILVER_TABLES = {NUMERIC.name: NUMERIC, WAVEFORM.name: WAVEFORM}
HYPOTENSION_TABLES = {ART_MBP_1S.name: ART_MBP_1S, HYPOTENSION_EVENTS.name: HYPOTENSION_EVENTS}
