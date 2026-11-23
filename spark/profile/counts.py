"""The prespecified source counts, as data, and the judgement of a reproduced count. No Spark runs here.

The values are fixed before profiling: the guide's section 8 (the lead's counts of the track list and the clinical table, preliminary until Silver reproduces
them) and the standard's analysis decisions 9 and 10 (the split's counts and the number of development cases). A count of the full cohort includes the test
patients, so its check is sealed: the table keeps whether the reproduced count equals the prespecified one, and never the reproduced value. A count of
development data keeps its value.

``places`` is the rounding that the guide's figure states: ``None`` is exact, ``0`` is a whole number of hours and ``2`` is hours to two places. The track total
is the lead's count of the publisher's track list. The publisher's own figure is 486,451 and the difference of 2 is to be explained, so a pinned run that finds
the publisher's figure saves a false flag for ``tracks``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

DEVELOPMENT = "development"
FULL_COHORT = "full cohort"
ARTERIAL_TRACK = "SNUADC/ART"
NONINVASIVE_TRACK = "Solar8000/NIBP_MBP"
SECONDS_PER_HOUR = 3600


@dataclass(frozen=True, slots=True)
class SourceCount:
    name: str
    scope: str
    expected: float
    places: int | None
    input: str
    method: str
    denominator: str


PRESPECIFIED = (
    SourceCount(
        "development_cases",
        DEVELOPMENT,
        5125,
        None,
        "bronze.clinical_cases and silver.patient_split",
        "the cases of the patients in the training and validation sets",
        "the cases of bronze.clinical_cases (analysis decision 10)",
    ),
    SourceCount(
        "development_patients_training",
        DEVELOPMENT,
        4287,
        None,
        "silver.patient_split",
        "the patients in the training set",
        "the patients of silver.patient_split (analysis decision 9)",
    ),
    SourceCount(
        "development_patients_validation",
        DEVELOPMENT,
        594,
        None,
        "silver.patient_split",
        "the patients in the validation set",
        "the patients of silver.patient_split (analysis decision 9)",
    ),
    SourceCount("cases", FULL_COHORT, 6388, None, "bronze.clinical_cases", "the rows, one for each case", "the cases of the release"),
    SourceCount("patients", FULL_COHORT, 6090, None, "bronze.clinical_cases", "the distinct patient numbers", "the patients of the release"),
    SourceCount(
        "tracks",
        FULL_COHORT,
        486449,
        None,
        "bronze.tracks",
        "the rows, one for each case and track",
        "the tracks of the cases of the release (the lead's count of the publisher's track list; the publisher reports 486,451)",
    ),
    SourceCount(
        "cases_with_arterial_waveform",
        FULL_COHORT,
        3645,
        None,
        "bronze.tracks",
        f"the distinct cases that hold the waveform track {ARTERIAL_TRACK}",
        "the cases of the release",
    ),
    SourceCount(
        "cases_with_noninvasive_pressure",
        FULL_COHORT,
        5763,
        None,
        "bronze.tracks",
        f"the distinct cases that hold the numeric track {NONINVASIVE_TRACK} (every other non-invasive pressure track is in the same cases)",
        "the cases of the release",
    ),
    SourceCount(
        "recorded_hours",
        FULL_COHORT,
        20138,
        0,
        "bronze.clinical_cases",
        "the sum of caseend (seconds from the start of the recording, whose clock starts at zero) over the cases, divided by 3,600, rounded to whole hours",
        "the cases of the release with a caseend",
    ),
    SourceCount(
        "mean_case_hours",
        FULL_COHORT,
        3.15,
        2,
        "bronze.clinical_cases",
        "the mean of caseend over the cases, divided by 3,600, rounded to two places",
        "the cases of the release with a caseend",
    ),
    SourceCount(
        "median_case_hours",
        FULL_COHORT,
        2.76,
        2,
        "bronze.clinical_cases",
        "the median of caseend over the cases (the mean of the middle two for an even number), divided by 3,600, rounded to two places",
        "the cases of the release with a caseend",
    ),
)
BY_NAME = {count.name: count for count in PRESPECIFIED}


def judged(count: SourceCount, value: float | None) -> bool:
    """Whether a reproduced value equals the prespecified count, exactly or after the rounding that the count states. No value is no match."""
    if value is None:
        return False
    return value == count.expected if count.places is None else round(value, count.places) == count.expected


def count_rows(values: Mapping[str, float | None], *, compared: bool) -> list[dict[str, Any]]:
    """The rows of ``source_counts`` from the reproduced ``values``. A sealed count keeps no value. Nothing is judged when ``compared`` is false."""
    rows = []
    for count in PRESPECIFIED:
        value = values.get(count.name)
        sealed = count.scope == FULL_COHORT
        rows.append(
            {
                "name": count.name,
                "scope": count.scope,
                "expected": float(count.expected),
                "value": None if sealed or value is None else float(value),
                "matches": judged(count, value) if compared else None,
                "sealed": sealed,
                "input": count.input,
                "method": count.method,
                "denominator": count.denominator,
            }
        )
    return rows
