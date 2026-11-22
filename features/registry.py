"""The registry: every feature's name, unit, missing rule and formula in one place.

A change to the frozen feature list is one edit here and its test in ``tests/fast/test_features_row.py``. The tracks are the publisher's, and their exact list
is fixed from ``bronze.tracks`` on development cases: change ``NUMERIC_TRACKS`` and ``WAVEFORM_TRACKS`` then. A feature of a track is named
``<track in lower case, / as _>__<statistic>``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from functools import cache

from features import stats
from features.model import MINUTE, SECOND, FeatureValue, NumericWindow, WaveformValid

NUMERIC_WINDOW_SECONDS = 900
WAVEFORM_WINDOW_SECONDS = 20
MIN_VALID_SHARE = 0.5
BAND_LOW_HZ = 0.5
BAND_HIGH_HZ = 3.5
LOW_PRESSURE_65 = 65.0
LOW_PRESSURE_60 = 60.0
PROJECTION_MINUTES = 5
ARTERIAL_MEAN_TRACK = "Solar8000/ART_MBP"
PUBLISHER_UNIT = "the publisher's unit of the track"
PREOP_PREFIX = "preop_"

MISSING_GROUP = "missing when fewer than half of the window's seconds are valid"


def slug(track: str) -> str:
    return track.lower().replace("/", "_")


def below_minutes(window: NumericWindow, limit: float) -> float:
    """Each valid reading stands for one second of Silver's grid: the minutes below ``limit`` are the readings below it over 60."""
    return sum(1 for value in window.values if value < limit) / 60


def seconds_since_below(window: NumericWindow, limit: float) -> float | None:
    times = [time for time, value in zip(window.times_us, window.values, strict=True) if value < limit]
    return (window.cutoff_us - times[-1]) / SECOND if times else None


def window_slope_per_minute(window: NumericWindow) -> float | None:
    return stats.slope([(time - window.cutoff_us) / MINUTE for time in window.times_us], window.values)


def projection(window: NumericWindow) -> float | None:
    """The last valid value plus the window's slope times 5 minutes."""
    window_slope = window_slope_per_minute(window)
    return None if window_slope is None else window.values[-1] + PROJECTION_MINUTES * window_slope


@dataclass(frozen=True, slots=True)
class NumericTrackSpec:
    family: str
    unit: str


NUMERIC_TRACKS: dict[str, NumericTrackSpec] = {
    "Solar8000/ART_MBP": NumericTrackSpec("arterial pressure, mean", "mmHg"),
    "Solar8000/ART_SBP": NumericTrackSpec("arterial pressure, systolic", "mmHg"),
    "Solar8000/ART_DBP": NumericTrackSpec("arterial pressure, diastolic", "mmHg"),
    "Solar8000/NIBP_MBP": NumericTrackSpec("non-invasive pressure, mean", "mmHg"),
    "Solar8000/HR": NumericTrackSpec("heart rate", "beats per minute"),
    "Solar8000/PLETH_SPO2": NumericTrackSpec("oxygen saturation", "%"),
    "Solar8000/ETCO2": NumericTrackSpec("end-tidal carbon dioxide", "mmHg"),
    "Primus/MV": NumericTrackSpec("minute ventilation", "L/min"),
    "Orchestra/PPF20_RATE": NumericTrackSpec("infusion rate, propofol", PUBLISHER_UNIT),
    "Orchestra/RFTN20_RATE": NumericTrackSpec("infusion rate, remifentanil", PUBLISHER_UNIT),
    "Orchestra/NEPI_RATE": NumericTrackSpec("infusion rate, vasopressor (norepinephrine)", PUBLISHER_UNIT),
    "Orchestra/EPI_RATE": NumericTrackSpec("infusion rate, vasopressor (epinephrine)", PUBLISHER_UNIT),
    "Orchestra/PHEN_RATE": NumericTrackSpec("infusion rate, vasopressor (phenylephrine)", PUBLISHER_UNIT),
    "Orchestra/VASO_RATE": NumericTrackSpec("infusion rate, vasopressor (vasopressin)", PUBLISHER_UNIT),
    "Orchestra/DOPA_RATE": NumericTrackSpec("infusion rate, vasopressor (dopamine)", PUBLISHER_UNIT),
}


@dataclass(frozen=True, slots=True)
class NumericStat:
    """One statistic of a numeric track. ``unit`` holds ``{unit}`` for the track's unit. A statistic that is ``always`` is never missing."""

    key: str
    unit: str
    missing: str
    definition: str
    compute: Callable[[NumericWindow], FeatureValue]
    always: bool = False
    arterial_mean_only: bool = False


NUMERIC_STATS: tuple[NumericStat, ...] = (
    NumericStat("last", "{unit}", MISSING_GROUP, "the value of the latest valid reading in the window", lambda w: w.values[-1]),
    NumericStat("mean", "{unit}", MISSING_GROUP, "the mean of the valid readings", lambda w: stats.mean(w.values)),
    NumericStat(
        "median", "{unit}", MISSING_GROUP, "the median of the valid readings; the mean of the middle pair for an even count", lambda w: stats.median(w.values)
    ),
    NumericStat("min", "{unit}", MISSING_GROUP, "the smallest valid reading", lambda w: min(w.values)),
    NumericStat("max", "{unit}", MISSING_GROUP, "the largest valid reading", lambda w: max(w.values)),
    NumericStat("std", "{unit}", MISSING_GROUP, "the sample standard deviation (n - 1) of the valid readings", lambda w: stats.sample_std(w.values)),
    NumericStat(
        "slope_per_min",
        "{unit} per minute",
        MISSING_GROUP,
        "the least-squares slope of the valid readings on their time in minutes",
        window_slope_per_minute,
    ),
    NumericStat("valid_count", "readings", "never missing", "the number of valid readings in the window", lambda w: len(w.values), always=True),
    NumericStat(
        "coverage",
        "share of the window's 900 seconds",
        "never missing",
        "the valid readings over the 900 seconds of the window",
        lambda w: len(w.values) / NUMERIC_WINDOW_SECONDS,
        always=True,
    ),
    NumericStat(
        "minutes_below_65",
        "minutes",
        MISSING_GROUP,
        "the valid readings strictly below 65 mmHg, one second each, over 60",
        lambda w: below_minutes(w, LOW_PRESSURE_65),
        arterial_mean_only=True,
    ),
    NumericStat(
        "minutes_below_60",
        "minutes",
        MISSING_GROUP,
        "the valid readings strictly below 60 mmHg, one second each, over 60",
        lambda w: below_minutes(w, LOW_PRESSURE_60),
        arterial_mean_only=True,
    ),
    NumericStat(
        "seconds_since_below_65",
        "seconds",
        f"{MISSING_GROUP}; missing when no valid reading in the window is below 65 mmHg",
        "the cutoff minus the time of the latest valid reading strictly below 65 mmHg",
        lambda w: seconds_since_below(w, LOW_PRESSURE_65),
        arterial_mean_only=True,
    ),
    NumericStat(
        "projection_5min",
        "mmHg",
        MISSING_GROUP,
        "the last valid value plus 5 times the window's slope per minute",
        projection,
        arterial_mean_only=True,
    ),
)


@dataclass(frozen=True, slots=True)
class WaveformStat:
    key: str
    unit: str
    missing: str
    definition: str
    compute: Callable[[WaveformValid], FeatureValue]
    always: bool = False


def amplitudes(valid: WaveformValid) -> list[float]:
    return [max(second.samples) - min(second.samples) for second in valid.seconds]


def all_samples(valid: WaveformValid) -> list[float]:
    return [sample for second in valid.seconds for sample in second.samples]


def dominant(valid: WaveformValid) -> float | None:
    return stats.dominant_frequency(valid.seconds, valid.sample_rate, valid.first_second, WAVEFORM_WINDOW_SECONDS, BAND_LOW_HZ, BAND_HIGH_HZ)


FREQUENCY = WaveformStat(
    "dominant_freq_hz",
    "Hz",
    f"{MISSING_GROUP}; missing when the signal is flat",
    "the frequency from 0.5 to 3.5 Hz in steps of 0.05 Hz with the most power, from the valid samples at their true times; the lowest wins a tie",
    dominant,
)
VALID_SECONDS = WaveformStat("valid_seconds", "seconds", "never missing", "the valid seconds in the 20-second window", lambda v: len(v.seconds), always=True)
WAVEFORM_COVERAGE = WaveformStat(
    "coverage",
    "share of the window's 20 seconds",
    "never missing",
    "the valid seconds over 20",
    lambda v: len(v.seconds) / WAVEFORM_WINDOW_SECONDS,
    always=True,
)

WAVEFORM_TRACKS: dict[str, tuple[WaveformStat, ...]] = {
    "SNUADC/ART": (
        WaveformStat("mean", "mmHg", MISSING_GROUP, "the mean of the valid samples", lambda v: stats.mean(all_samples(v))),
        WaveformStat("std", "mmHg", MISSING_GROUP, "the sample standard deviation (n - 1) of the valid samples", lambda v: stats.sample_std(all_samples(v))),
        WaveformStat(
            "pulse_pressure_mean",
            "mmHg",
            MISSING_GROUP,
            "the mean over the valid seconds of each second's largest minus smallest sample",
            lambda v: stats.mean(amplitudes(v)),
        ),
        FREQUENCY,
        VALID_SECONDS,
        WAVEFORM_COVERAGE,
    ),
    "SNUADC/ECG_II": (
        WaveformStat("std", "mV", MISSING_GROUP, "the sample standard deviation (n - 1) of the valid samples", lambda v: stats.sample_std(all_samples(v))),
        FREQUENCY,
        VALID_SECONDS,
        WAVEFORM_COVERAGE,
    ),
    "SNUADC/PLETH": (
        WaveformStat(
            "amplitude_mean",
            "signal unit",
            MISSING_GROUP,
            "the mean over the valid seconds of each second's largest minus smallest sample",
            lambda v: stats.mean(amplitudes(v)),
        ),
        WaveformStat(
            "amplitude_std",
            "signal unit",
            MISSING_GROUP,
            "the sample standard deviation (n - 1) of the valid seconds' amplitudes",
            lambda v: stats.sample_std(amplitudes(v)),
        ),
        FREQUENCY,
        VALID_SECONDS,
        WAVEFORM_COVERAGE,
    ),
}

CLINICAL_FIELDS = ("age", "sex", "height", "weight", "bmi", "asa", "emop", "department", "optype")
SENSITIVITY_FIELDS = ("approach", "position", "ane_type")
CLINICAL_MISSING = "missing when the value is not in the table or its availability time is unknown or after the cutoff"
CLINICAL_UNITS = {"age": "years", "height": "cm", "weight": "kg", "bmi": "kg/m^2"}


@dataclass(frozen=True, slots=True)
class FeatureSpec:
    name: str
    unit: str
    group: str
    missing: str
    definition: str
    sensitivity_only: bool = False


def feature_specs() -> tuple[FeatureSpec, ...]:
    """Every feature with a fixed name. The ``preop_`` family has one name per result or condition of the table and is known by its prefix."""
    specs: list[FeatureSpec] = []
    for track, track_spec in NUMERIC_TRACKS.items():
        for stat in NUMERIC_STATS:
            if stat.arterial_mean_only and track != ARTERIAL_MEAN_TRACK:
                continue
            specs.append(
                FeatureSpec(
                    f"{slug(track)}__{stat.key}", stat.unit.format(unit=track_spec.unit), track, stat.missing, f"{track_spec.family}: {stat.definition}"
                )
            )
    for track, waveform_stats in WAVEFORM_TRACKS.items():
        specs += [FeatureSpec(f"{slug(track)}__{stat.key}", stat.unit, track, stat.missing, stat.definition) for stat in waveform_stats]
    specs += [
        FeatureSpec(field, CLINICAL_UNITS.get(field, "category or code"), "clinical", CLINICAL_MISSING, "the value in the clinical table")
        for field in CLINICAL_FIELDS
    ]
    specs += [
        FeatureSpec(field, "category", "clinical", CLINICAL_MISSING, "the value in the clinical table; a sensitivity analysis only", True)
        for field in SENSITIVITY_FIELDS
    ]
    return tuple(specs)


@cache
def registered_names() -> frozenset[str]:
    return frozenset(spec.name for spec in feature_specs())


def is_registered(name: str) -> bool:
    """Whether ``name`` is a feature: a fixed name of the registry or a ``preop_`` result or condition."""
    if name.startswith(PREOP_PREFIX) and len(name) > len(PREOP_PREFIX):
        return True
    return name in registered_names()
