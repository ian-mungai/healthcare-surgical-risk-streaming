"""The registry: every feature's name, unit, missing rule, formula and status in one place.

A change to the frozen feature list is one edit here and its test in ``tests/fast/test_features_row.py``. A feature of a track is named
``<track in lower case, / as _>__<statistic>``. A track has one input kind:

- ``source_time``: a monitor track at its published times in microseconds, irregular spacing allowed. Silver removes duplicate readings before the features.
- ``per_second``: Silver's per-second series, one value for each whole second with a valid flag. The pressure features come from this kind only.

A statistic is missing only when it cannot be computed. The only limits on the data are the sample eligibility limits of the analysis plan (``eligibility.py``).
Every track name and unit is ``to_confirm`` until its status says ``confirmed`` with an evidence note.

**Seconds and windows.** A second ``s`` is a block that covers ``[s, s + 1)`` seconds of case time and starts exactly at ``s * 1_000_000``
microseconds. A waveform block holds exactly ``rate`` samples at ``i / rate`` seconds after its start, and the rate is a positive whole number. A reading of
the per-second series is given at the start of its second, and its value is known at the end of that second: its time is ``(s + 1) * 1_000_000``. A second is
complete at a cutoff when ``(s + 1) * 1_000_000 <= cutoff``. The window of the waveform (20 seconds), of the per-second series (900 seconds) and of the sample
eligibility limits is the newest ``n`` complete seconds, from ``complete_seconds``: no sample of the second that a fractional cutoff falls in is used. A
source-time track keeps its published times: its window is ``cutoff - 15 minutes < time <= cutoff``. The latest reading time of a waveform is the time of the
last sample of its newest valid second, ``s * 1_000_000 + round((rate - 1) * 1_000_000 / rate)``, and never the cutoff. The latest reading time of the
per-second series is the end of its newest valid second, which can equal the cutoff and is never after it, and the time since a low second is counted from the
end of that second.

A reading or a second flagged invalid may hold any value, a non-finite number or a null included, and its value is never used: it changes no feature and no
latest time.

**Arithmetic.** The mean, median, standard deviation, slope, projection and the pulse pressure and amplitude statistics are exact: each float is taken as the
rational number that it is, and the result is rounded once. A result that a float cannot hold is a named input error. The row has exactly the keys of this
registry, in its order, whatever the input holds, and a clinical field of the input that is neither in the registry nor forbidden is refused by name.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from fractions import Fraction
from functools import cache

from features import stats
from features.model import MINUTE, SECOND, NumericWindow, WaveformValid

SOURCE_TIME = "source_time"
PER_SECOND = "per_second"
NUMERIC_WINDOW_SECONDS = 900
WAVEFORM_WINDOW_SECONDS = 20
BAND_LOW_HZ = 0.5
BAND_HIGH_HZ = 3.5
LOW_PRESSURE_65 = 65.0
LOW_PRESSURE_60 = 60.0
PROJECTION_MINUTES = 5
ARTERIAL_SERIES = "Silver/ART_MBP_1S"
PUBLISHER_UNIT = "the publisher's unit of the track"

# The sample eligibility limits of the analysis plan (project standard, "Analysis decisions and their evidence", row 5).
MAX_INVALID_WAVEFORM_SECONDS = 0
MAX_INVALID_NUMERIC_PERCENT = 10
WAVEFORM_INVALID_SECOND = "waveform_invalid_second"
NUMERIC_OVER_10_PERCENT_INVALID = "numeric_over_10_percent_invalid"

MISSING_NO_VALUE = "missing when the window holds no valid reading"
MISSING_FEWER_THAN_TWO = "missing when the window holds fewer than 2 valid readings"
MISSING_NO_SECOND = "missing when the window holds no valid second"
MISSING_ONE_SECOND = "missing when the window holds fewer than 2 valid seconds"
NEVER_MISSING = "never missing"
SAMPLE_STD = "the sample standard deviation (n - 1), exactly 0 when all values are equal"


@dataclass(frozen=True, slots=True)
class TrackStatus:
    """Whether a track's name and unit are confirmed, and the evidence (confirmed) or what to confirm (not confirmed)."""

    confirmed: bool
    evidence: str


TO_CONFIRM = TrackStatus(False, "to confirm against bronze.tracks before the list is frozen")
ARTERIAL_WAVEFORM_STATUS = TrackStatus(True, "confirmed: the waveform of the fixed sample's candidate list (scripts/sample/candidates.py)")
ARTERIAL_MEAN_STATUS = TrackStatus(True, "confirmed in the review of the feature list on Nov 22 2026")
SERIES_STATUS = TrackStatus(False, "to confirm against the Silver table when it is built")
PROJECTION_NOTE = "to confirm against Jacquet-Lagrèze 2022 before the list is frozen"


def complete_seconds(cutoff_us: int, count: int) -> range:
    """The newest ``count`` complete seconds at ``cutoff_us``: the seconds ``s`` with ``(s + 1) * 1_000_000 <= cutoff_us``. The one definition of a window."""
    newest = cutoff_us // SECOND - 1
    return range(newest - count + 1, newest + 1)


def last_sample_us(second: int, rate: int) -> int:
    """The time of the last sample of the block ``second``, in microseconds, as the latest reading time that a waveform second reports."""
    return second * SECOND + round((rate - 1) * SECOND / rate)


def slug(track: str) -> str:
    return track.lower().replace("/", "_")


def below_minutes(window: NumericWindow, limit: float) -> float:
    """Each valid second of the per-second series stands for one second: the minutes below ``limit`` are the seconds below it over 60."""
    return sum(1 for value in window.values if value < limit) / 60


def seconds_since_below(window: NumericWindow, limit: float) -> float | None:
    times = [time for time, value in zip(window.times_us, window.values, strict=True) if value < limit]
    return (window.cutoff_us - times[-1]) / SECOND if times else None


def exact_slope_per_minute(window: NumericWindow) -> Fraction | None:
    """The exact least-squares slope on the actual times of the valid readings, per minute."""
    slope = stats.slope_exact([time - window.cutoff_us for time in window.times_us], window.values)
    return None if slope is None else slope * MINUTE


def window_slope_per_minute(window: NumericWindow) -> float | None:
    slope = exact_slope_per_minute(window)
    return None if slope is None else float(slope)


def projection(window: NumericWindow) -> float | None:
    """The last valid value plus the window's slope per minute times 5 minutes, exact and rounded once."""
    slope = exact_slope_per_minute(window)
    return None if slope is None else float(Fraction(window.values[-1]) + PROJECTION_MINUTES * slope)


@dataclass(frozen=True, slots=True)
class NumericTrackSpec:
    kind: str
    family: str
    unit: str
    status: TrackStatus


NUMERIC_TRACKS: dict[str, NumericTrackSpec] = {
    "Solar8000/ART_MBP": NumericTrackSpec(SOURCE_TIME, "arterial pressure, mean", "mmHg", ARTERIAL_MEAN_STATUS),
    "Solar8000/ART_SBP": NumericTrackSpec(SOURCE_TIME, "arterial pressure, systolic", "mmHg", TO_CONFIRM),
    "Solar8000/ART_DBP": NumericTrackSpec(SOURCE_TIME, "arterial pressure, diastolic", "mmHg", TO_CONFIRM),
    "Solar8000/NIBP_MBP": NumericTrackSpec(SOURCE_TIME, "non-invasive pressure, mean", "mmHg", TO_CONFIRM),
    "Solar8000/HR": NumericTrackSpec(SOURCE_TIME, "heart rate", "beats per minute", TO_CONFIRM),
    "Solar8000/PLETH_SPO2": NumericTrackSpec(SOURCE_TIME, "oxygen saturation", "%", TO_CONFIRM),
    "Solar8000/ETCO2": NumericTrackSpec(SOURCE_TIME, "end-tidal carbon dioxide", "mmHg", TO_CONFIRM),
    "Primus/MV": NumericTrackSpec(SOURCE_TIME, "minute ventilation", "L/min", TO_CONFIRM),
    "Orchestra/PPF20_RATE": NumericTrackSpec(SOURCE_TIME, "infusion rate, propofol", PUBLISHER_UNIT, TO_CONFIRM),
    "Orchestra/RFTN20_RATE": NumericTrackSpec(SOURCE_TIME, "infusion rate, remifentanil", PUBLISHER_UNIT, TO_CONFIRM),
    "Orchestra/NEPI_RATE": NumericTrackSpec(SOURCE_TIME, "infusion rate, vasopressor (norepinephrine)", PUBLISHER_UNIT, TO_CONFIRM),
    "Orchestra/EPI_RATE": NumericTrackSpec(SOURCE_TIME, "infusion rate, vasopressor (epinephrine)", PUBLISHER_UNIT, TO_CONFIRM),
    "Orchestra/PHEN_RATE": NumericTrackSpec(SOURCE_TIME, "infusion rate, vasopressor (phenylephrine)", PUBLISHER_UNIT, TO_CONFIRM),
    "Orchestra/VASO_RATE": NumericTrackSpec(SOURCE_TIME, "infusion rate, vasopressor (vasopressin)", PUBLISHER_UNIT, TO_CONFIRM),
    "Orchestra/DOPA_RATE": NumericTrackSpec(SOURCE_TIME, "infusion rate, vasopressor (dopamine)", PUBLISHER_UNIT, TO_CONFIRM),
    ARTERIAL_SERIES: NumericTrackSpec(PER_SECOND, "arterial mean pressure, per second (the trailing 2-second average of SNUADC/ART)", "mmHg", SERIES_STATUS),
}


@dataclass(frozen=True, slots=True)
class NumericStat:
    """One statistic of a numeric track. ``unit`` holds ``{unit}`` for the track's unit. A statistic that does not ``needs_value`` is never missing.

    A ``note`` marks the statistic as to be confirmed, with what to confirm it against.
    """

    key: str
    unit: str
    missing: str
    definition: str
    compute: Callable[[NumericWindow], float | int | None]
    needs_value: bool = True
    note: str = ""
    count: bool = False


SOURCE_STATS: tuple[NumericStat, ...] = (
    NumericStat("last", "{unit}", MISSING_NO_VALUE, "the value of the latest valid reading in the window", lambda w: w.values[-1]),
    NumericStat("mean", "{unit}", MISSING_NO_VALUE, "the mean of the valid readings", lambda w: stats.mean(w.values)),
    NumericStat(
        "median",
        "{unit}",
        MISSING_NO_VALUE,
        "the median of the valid readings; the mean of the middle pair for an even count",
        lambda w: stats.median(w.values),
    ),
    NumericStat("min", "{unit}", MISSING_NO_VALUE, "the smallest valid reading", lambda w: min(w.values)),
    NumericStat("max", "{unit}", MISSING_NO_VALUE, "the largest valid reading", lambda w: max(w.values)),
    NumericStat("std", "{unit}", MISSING_FEWER_THAN_TWO, f"{SAMPLE_STD} of the valid readings", lambda w: stats.sample_std(w.values)),
    NumericStat(
        "slope_per_min",
        "{unit} per minute",
        MISSING_FEWER_THAN_TWO,
        "the least-squares slope of the valid readings on their actual times, per minute",
        window_slope_per_minute,
    ),
    NumericStat("valid_count", "readings", NEVER_MISSING, "the number of valid readings in the window", lambda w: len(w.values), needs_value=False, count=True),
    NumericStat(
        "seconds_since_last",
        "seconds",
        MISSING_NO_VALUE,
        "the cutoff minus the time of the latest valid reading",
        lambda w: (w.cutoff_us - w.times_us[-1]) / SECOND,
    ),
)

SERIES_STATS: tuple[NumericStat, ...] = (
    NumericStat("valid_count", "seconds", NEVER_MISSING, "the number of valid seconds in the window", lambda w: len(w.values), needs_value=False, count=True),
    NumericStat(
        "coverage",
        "share of the window's 900 seconds",
        NEVER_MISSING,
        "the valid seconds over the 900 seconds of the window",
        lambda w: len(w.values) / NUMERIC_WINDOW_SECONDS,
        needs_value=False,
    ),
    NumericStat(
        "minutes_below_65",
        "minutes",
        MISSING_NO_VALUE,
        "the valid seconds strictly below 65 mmHg, one second each, over 60",
        lambda w: below_minutes(w, LOW_PRESSURE_65),
    ),
    NumericStat(
        "minutes_below_60",
        "minutes",
        MISSING_NO_VALUE,
        "the valid seconds strictly below 60 mmHg, one second each, over 60",
        lambda w: below_minutes(w, LOW_PRESSURE_60),
    ),
    NumericStat(
        "seconds_since_below_65",
        "seconds",
        f"{MISSING_NO_VALUE}; missing when no valid second in the window is below 65 mmHg",
        "the cutoff minus the end time of the newest valid second strictly below 65 mmHg, in seconds",
        lambda w: seconds_since_below(w, LOW_PRESSURE_65),
    ),
    NumericStat(
        "projection_5min",
        "mmHg",
        MISSING_FEWER_THAN_TWO,
        "the last valid value plus 5 times the least-squares slope per minute of the window's valid seconds",
        projection,
        note=PROJECTION_NOTE,
    ),
)

NUMERIC_STATS: dict[str, tuple[NumericStat, ...]] = {SOURCE_TIME: SOURCE_STATS, PER_SECOND: SERIES_STATS}


@dataclass(frozen=True, slots=True)
class WaveformStat:
    key: str
    unit: str
    missing: str
    definition: str
    compute: Callable[[WaveformValid], float | int | None]
    needs_value: bool = True
    count: bool = False


def amplitudes(valid: WaveformValid) -> list[Fraction]:
    """The exact largest minus smallest sample of each valid second."""
    return [Fraction(max(second.samples)) - Fraction(min(second.samples)) for second in valid.seconds]


def all_samples(valid: WaveformValid) -> list[float]:
    return [sample for second in valid.seconds for sample in second.samples]


def dominant(valid: WaveformValid) -> float | None:
    if len(valid.seconds) < WAVEFORM_WINDOW_SECONDS:
        return None
    return stats.dominant_frequency(valid.seconds, valid.sample_rate, valid.first_second, WAVEFORM_WINDOW_SECONDS, BAND_LOW_HZ, BAND_HIGH_HZ)


FREQUENCY = WaveformStat(
    "dominant_freq_hz",
    "Hz",
    f"{MISSING_NO_SECOND}; missing unless all 20 seconds of the window are valid; missing when the signal is flat, when no bin of the band is below the "
    "Nyquist frequency or when the band holds less than 1e-9 of the power",
    "the lowest frequency from 0.5 to 3.5 Hz and below the Nyquist frequency, in steps of 0.05 Hz, whose power is within a relative 1e-9 of the largest, from "
    "the samples at their block times",
    dominant,
)
VALID_SECONDS = WaveformStat(
    "valid_seconds", "seconds", NEVER_MISSING, "the valid seconds in the 20-second window", lambda v: len(v.seconds), needs_value=False, count=True
)
WAVEFORM_COVERAGE = WaveformStat(
    "coverage",
    "share of the window's 20 seconds",
    NEVER_MISSING,
    "the valid seconds over 20",
    lambda v: len(v.seconds) / WAVEFORM_WINDOW_SECONDS,
    needs_value=False,
)

WAVEFORM_TRACKS: dict[str, tuple[WaveformStat, ...]] = {
    "SNUADC/ART": (
        WaveformStat("mean", "mmHg", MISSING_NO_SECOND, "the mean of the valid samples", lambda v: stats.mean(all_samples(v))),
        WaveformStat(
            "std",
            "mmHg",
            MISSING_NO_SECOND,
            "the sample standard deviation (n - 1) of the valid samples, exactly 0 when all are equal",
            lambda v: stats.sample_std(all_samples(v)),
        ),
        WaveformStat(
            "pulse_pressure_mean",
            "mmHg",
            MISSING_NO_SECOND,
            "the mean over the valid seconds of each second's largest minus smallest sample",
            lambda v: stats.mean(amplitudes(v)),
        ),
        FREQUENCY,
        VALID_SECONDS,
        WAVEFORM_COVERAGE,
    ),
    "SNUADC/ECG_II": (
        WaveformStat(
            "std",
            "mV",
            MISSING_NO_SECOND,
            "the sample standard deviation (n - 1) of the valid samples, exactly 0 when all are equal",
            lambda v: stats.sample_std(all_samples(v)),
        ),
        FREQUENCY,
        VALID_SECONDS,
        WAVEFORM_COVERAGE,
    ),
    "SNUADC/PLETH": (
        WaveformStat(
            "amplitude_mean",
            "signal unit",
            MISSING_NO_SECOND,
            "the mean over the valid seconds of each second's largest minus smallest sample",
            lambda v: stats.mean(amplitudes(v)),
        ),
        WaveformStat(
            "amplitude_std",
            "signal unit",
            MISSING_ONE_SECOND,
            "the sample standard deviation (n - 1) of the valid seconds' amplitudes, exactly 0 when all are equal",
            lambda v: stats.sample_std(amplitudes(v)),
        ),
        FREQUENCY,
        VALID_SECONDS,
        WAVEFORM_COVERAGE,
    ),
}

WAVEFORM_STATUS: dict[str, TrackStatus] = {"SNUADC/ART": ARTERIAL_WAVEFORM_STATUS, "SNUADC/ECG_II": TO_CONFIRM, "SNUADC/PLETH": TO_CONFIRM}

CLINICAL_FIELDS = ("age", "sex", "height", "weight", "bmi", "asa", "emop", "department", "optype")
# The pre-operative columns of the header of the pinned clinical table, in its order.
PREOP_COLUMNS = (
    "preop_htn", "preop_dm", "preop_ecg", "preop_pft", "preop_hb", "preop_plt", "preop_pt", "preop_aptt", "preop_na", "preop_k", "preop_gluc",
    "preop_alb", "preop_ast", "preop_alt", "preop_bun", "preop_cr", "preop_ph", "preop_hco3", "preop_be", "preop_pao2", "preop_paco2", "preop_sao2",
)  # fmt: skip
PREOP_NOTE = "to confirm against bronze.clinical_cases before the list is frozen"
SENSITIVITY_FIELDS = ("approach", "position", "ane_type")
CLINICAL_MISSING = "missing when the value is not in the table or its availability time is unknown or after the cutoff"
CLINICAL_UNITS = {"age": "years", "height": "cm", "weight": "kg", "bmi": "kg/m^2"}


@dataclass(frozen=True, slots=True)
class FeatureSpec:
    """A feature. ``status`` is ``confirmed`` or ``to_confirm``, and ``note`` is the evidence or what to confirm."""

    name: str
    unit: str
    group: str
    input_kind: str
    missing: str
    definition: str
    status: str
    note: str
    sensitivity_only: bool = False


def status_word(confirmed: bool) -> str:
    return "confirmed" if confirmed else "to_confirm"


def feature_specs() -> tuple[FeatureSpec, ...]:
    """Every feature, in the order of a row: the numeric tracks, the waveforms, the clinical fields, the pre-operative columns, the sensitivity fields."""
    specs: list[FeatureSpec] = []
    for track, track_spec in NUMERIC_TRACKS.items():
        for stat in NUMERIC_STATS[track_spec.kind]:
            confirmed = track_spec.status.confirmed and not stat.note
            note = stat.note or track_spec.status.evidence
            definition = f"{track_spec.family}: {stat.definition}"
            specs.append(
                FeatureSpec(
                    f"{slug(track)}__{stat.key}",
                    stat.unit.format(unit=track_spec.unit),
                    track,
                    track_spec.kind,
                    stat.missing,
                    definition,
                    status_word(confirmed),
                    note,
                )
            )
    for track, waveform_stats in WAVEFORM_TRACKS.items():
        status = WAVEFORM_STATUS[track]
        specs += [
            FeatureSpec(
                f"{slug(track)}__{stat.key}", stat.unit, track, "waveform", stat.missing, stat.definition, status_word(status.confirmed), status.evidence
            )
            for stat in waveform_stats
        ]
    plan_note = "a field of the owner-approved feature list"
    specs += [
        FeatureSpec(
            field,
            CLINICAL_UNITS.get(field, "category or code"),
            "clinical",
            "clinical",
            CLINICAL_MISSING,
            "the value in the clinical table",
            "confirmed",
            plan_note,
        )
        for field in CLINICAL_FIELDS
    ]
    specs += [
        FeatureSpec(
            column,
            "the unit of the table column",
            "clinical",
            "clinical",
            CLINICAL_MISSING,
            "the pre-operative result or condition in the clinical table",
            "to_confirm",
            PREOP_NOTE,
        )
        for column in PREOP_COLUMNS
    ]
    specs += [
        FeatureSpec(
            field,
            "category",
            "clinical",
            "clinical",
            CLINICAL_MISSING,
            "the value in the clinical table; a sensitivity analysis only",
            "confirmed",
            plan_note,
            True,
        )
        for field in SENSITIVITY_FIELDS
    ]
    return tuple(specs)


@cache
def registered_names() -> frozenset[str]:
    return frozenset(spec.name for spec in feature_specs())


def is_registered(name: str) -> bool:
    """Whether ``name`` is a feature of the registry. The keys of a row come from the registry only, never from the input."""
    return name in registered_names()
