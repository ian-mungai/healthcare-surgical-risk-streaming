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

- ``art_mbp_1s``: one row for each second of ``SNUADC/ART``, the trailing 2-second average (``features.pressure_series``). Key: case and second. The
  reading of second ``s`` is known at the end of ``s``. ``sample_count``, ``sum_high`` and ``sum_low`` are the exact mean (the number of samples of the two
  seconds and their exact sum as two floats), and ``value`` is that mean rounded once; a limit test uses the exact mean. The three and ``value`` are null
  when the reading is invalid, and ``invalid_cause`` is the cause then.
- ``hypotension_events``: one row for each run of at least 60 consecutive valid seconds of the series below 65 mmHg inside the operation
  (``features.hypotension``), for the cases of the development patients only (the training and validation sets of ``silver.patient_split``, trusted after its
  rule check). Key: case and onset second. It holds every time the event used: the onset and confirmation times, the end of the run and the
  operation start and end that bounded it, and why the run ended.

The job ``spark/jobs/silver_labels.py`` builds three more tables from ``silver.cases``, ``silver.lab_results`` and the patient split (``LABEL_TABLES``), in this
order, the first before the others:

- ``icu_days_measure``: the measured values of ``icu_days`` of the development patients only (how many are missing, invalid and recorded, how many are zero,
  above zero and a fraction of a day, the least, the greatest and the percentiles), recorded before the intensive care label is built. Key: statistic.
- ``icu_labels``: one row for each case, the held-out patients' included. It records the case's ``icu_days`` as published (a missing value stays missing), the
  end of the operation that the stay follows, the label (``positive``, ``negative`` or ``unknown``), its eligibility and why (``features.icu_label``).
  Key: case.
- ``kidney_injury_labels``: one row for each case. It records the baseline (value, time and its number among the results of that case, time and test: with the
  case and the test ``cr`` they name the row of ``silver.lab_results``), the baseline, 48-hour and 7-day windows, how many results fell in each follow-up
  window, the two measurements of the absolute rise and the result of the relative rise, the latest time that the rule read, the label and its eligibility
  reason (``features.kidney_label``). Times are seconds from the start of the case. Key: case.

The labels are outcomes and the test set is sealed: a label table is complete (every case has a row), but no check, log or result shows a label value of a
held-out patient. The undated baseline (``preop_cr``) and the sensitivity analyses are not built here.
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
        ("sample_count", "int"),
        ("sum_high", "double"),
        ("sum_low", "double"),
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
ICU_DAYS_MEASURE = SilverTable("icu_days_measure", (("statistic", "string"), ("unit", "string"), ("value", "double")), ("statistic",))
ICU_LABELS = SilverTable(
    "icu_labels",
    (
        ("case_id", "bigint"),
        ("opend_s", "bigint"),
        ("icu_days", "double"),
        ("label", "string"),
        ("eligible", "boolean"),
        ("eligibility_reason", "string"),
    ),
    ("case_id",),
)
KIDNEY_INJURY_LABELS = SilverTable(
    "kidney_injury_labels",
    (
        ("case_id", "bigint"),
        ("baseline_value", "double"),
        ("baseline_time_s", "bigint"),
        ("baseline_occurrence", "int"),
        ("baseline_window_start_s", "bigint"),
        ("baseline_window_end_s", "bigint"),
        ("followup_start_s", "bigint"),
        ("absolute_window_end_s", "bigint"),
        ("relative_window_end_s", "bigint"),
        ("early_results", "int"),
        ("late_results", "int"),
        ("absolute_rise", "boolean"),
        ("earlier_value", "double"),
        ("earlier_time_s", "bigint"),
        ("earlier_occurrence", "int"),
        ("later_value", "double"),
        ("later_time_s", "bigint"),
        ("later_occurrence", "int"),
        ("relative_rise", "boolean"),
        ("relative_value", "double"),
        ("relative_time_s", "bigint"),
        ("relative_occurrence", "int"),
        ("latest_time_s", "bigint"),
        ("label", "string"),
        ("eligible", "boolean"),
        ("eligibility_reason", "string"),
    ),
    ("case_id",),
)
SILVER_TABLES = {NUMERIC.name: NUMERIC, WAVEFORM.name: WAVEFORM}
HYPOTENSION_TABLES = {ART_MBP_1S.name: ART_MBP_1S, HYPOTENSION_EVENTS.name: HYPOTENSION_EVENTS}
LABEL_TABLES = {ICU_DAYS_MEASURE.name: ICU_DAYS_MEASURE, ICU_LABELS.name: ICU_LABELS, KIDNEY_INJURY_LABELS.name: KIDNEY_INJURY_LABELS}
