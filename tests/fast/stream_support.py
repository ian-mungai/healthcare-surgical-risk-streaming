"""Shared set-up for the fast tests of the streaming job: a synthetic case recording, the events the replay would send for it, a stand-in for Spark's keyed
state and an oracle that builds the feature row of a cutoff straight from the recording, with no state, no lateness and no buffers.

The oracle uses the feature code and the signal rules and nothing of the job's own state handling, so a row that the job builds from buffers is compared with a
row built from the whole recording. The stand-in for Spark groups the events of each batch by case occurrence, hands them to ``process`` with the state of the
previous batch and can write each state to JSON and read it back between batches, as a restart does.
"""

from __future__ import annotations

import json
import math
from array import array
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace

from features.batch import row_document
from features.eligibility import warning_eligibility
from features.model import ClinicalValue, FeatureRow, Reading, WaveformSecond, WaveformWindow, WindowInput
from features.registry import ARTERIAL_SERIES, NUMERIC_TRACKS, WAVEFORM_TRACKS
from features.row import build_row
from replay.events.formats import FORMATS, KIND_FORMAT
from replay.events.identity import Occurrence, source_event_id
from replay.events.manifest import reference_seconds
from replay.events.recording import CaseRecording, CaseTimes, NumericTrack, StringTrack, WaveformRecord, WaveformTrack
from replay.events.schedule import ScheduledOccurrence
from replay.events.stream import occurrence_events, outgoing
from spark.streaming.model import Context, Event, FeatureRecord, Policy, Record
from spark.streaming.occurrence import process
from spark.streaming.signals import ARTERIAL_WAVEFORM, physical_samples, series_reading, summarise_second
from spark.streaming.state import OccurrenceState
from spark.streaming.wire import event_from_record

SECOND = 1_000_000
RUN = "stream_0a1b2c3d"
ORIGIN_US = 5_000 * SECOND
HEART_RATE = "Solar8000/HR"
ARTERIAL_MEAN = "Solar8000/ART_MBP"
NO_CLINICAL: Callable[[int], Mapping[str, ClinicalValue]] = lambda case_id: {}  # noqa: E731
CONTEXT = Context(Policy(), "features_test", NO_CLINICAL)


def second_wave(index: int, rate: int, bad: bool) -> list[float]:
    """The physical samples of arterial second ``index``: a pressure wave of 60 mmHg of range that stays valid, or a second that holds a 250 mmHg sample."""
    pattern = [80.0, 90.0, 100.0, 110.0, 100.0, 90.0, 80.0, 70.0, 60.0, 70.0]
    samples = [pattern[(position + index) % len(pattern)] + (index % 3) for position in range(rate)]
    if bad:
        samples[rate // 2] = 250.0
    return samples


def synthetic_recording(
    case_id: int = 101,
    *,
    caseend_s: int = 400,
    opstart_s: int = 30,
    opend_s: int = 300,
    rate: int = 10,
    hr_base: float = 60.0,
    hr_step_s: int = 2,
    arterial_rate: float | None = None,
    arterial_start_s: int = 0,
    extra_hr: Sequence[tuple[int, float]] = (),
) -> CaseRecording:
    """A case with registry tracks: two numeric tracks (one with a duplicate reading and one with a not-a-number reading), an unregistered numeric track, a
    string track, an arterial waveform with one invalid second, and a second waveform with a gain and an offset."""
    heart = [(second * SECOND, hr_base + (second % 7)) for second in range(1, caseend_s, hr_step_s)]
    heart.append((41 * SECOND, hr_base + 30.0))
    heart.extend((time, value) for time, value in extra_hr)
    heart.sort(key=lambda reading: reading[0])
    mean_pressure = [(second * SECOND, 70.0 + (second % 5) * 2.0) for second in range(1, caseend_s, 2)]
    middle = min(49, len(mean_pressure) - 1)
    mean_pressure[middle] = (mean_pressure[middle][0], float("nan"))
    arterial_samples = [value for second in range(arterial_start_s, caseend_s) for value in second_wave(second, rate, second == 150)]
    pleth_samples = [float((second * rate + position) % 50) for second in range(caseend_s) for position in range(rate)]
    return CaseRecording(
        CaseTimes(case_id, opstart_s * SECOND, opend_s * SECOND, caseend_s * SECOND),
        numeric=(
            NumericTrack(HEART_RATE, "bpm", tuple(heart)),
            NumericTrack(ARTERIAL_MEAN, "mmHg", tuple(mean_pressure)),
            NumericTrack("Other/UNREGISTERED", "x", ((5 * SECOND, 1.0), (9 * SECOND, 2.0))),
        ),
        strings=(StringTrack("Test/EVENT", ((50 * SECOND, "note"),)),),
        waveforms=(
            WaveformTrack(
                ARTERIAL_WAVEFORM, "mmHg", arterial_rate or float(rate), 1.0, 0.0, (WaveformRecord(arterial_start_s * SECOND, array("d", arterial_samples)),)
            ),
            WaveformTrack("SNUADC/PLETH", "%", float(rate), 0.5, 1.0, (WaveformRecord(0, array("d", pleth_samples)),)),
        ),
    )


def events_for(recording: CaseRecording, *, room: int = 0, sequence: int = 0, repeat: int = 0, start_us: int = ORIGIN_US, run: str = RUN) -> list[Event]:
    """The events of an occurrence in the replay's sending order, as the job decodes them. The Kafka position is set by ``stamp``."""
    occurrence = Occurrence(sequence, recording.times.case_id, repeat, room)
    scheduled = ScheduledOccurrence(occurrence, recording.times, start_us)
    events = []
    for case_event in occurrence_events(recording):
        sent = outgoing(run, scheduled, recording, case_event)
        events.append(event_from_record(FORMATS[KIND_FORMAT[sent.kind]].record_name, sent.payload, 0, 0))
    return events


def reading_event(track: str, case_time_us: int, value: float, *, room: int = 0, case_id: int = 101, repeat: int = 0, start_us: int = ORIGIN_US) -> Event:
    occurrence = f"{case_id}.{repeat}"
    identity = source_event_id(RUN, room, occurrence, track, case_time_us, 0)
    payload = {
        "event_id": identity,
        "run_id": RUN,
        "room": room,
        "occurrence_id": occurrence,
        "case_id": case_id,
        "track": track,
        "case_time_us": case_time_us,
        "discriminator": 0,
        "replay_time_us": start_us + case_time_us,
        "value": value,
    }
    return event_from_record("NumericReading", payload, 0, 0)


def stamp(events: Sequence[Event]) -> list[Event]:
    """Give each event the Kafka position it would have: its room's partition and the next offset of that partition, in the order given."""
    offsets: defaultdict[int, int] = defaultdict(int)
    stamped = []
    for event in events:
        stamped.append(replace(event, kafka_partition=event.room, kafka_offset=offsets[event.room]))
        offsets[event.room] += 1
    return stamped


def by_replay_time(*streams: Sequence[Event]) -> list[Event]:
    """Merge the streams of several rooms by due time, as a broker that reads all partitions would hand them over, one room's order kept."""
    merged = [event for stream in streams for event in stream]
    return sorted(merged, key=lambda event: (event.replay_time_us, event.room))


def run_stream(
    arrival: Sequence[Event], context: Context = CONTEXT, *, batch_size: int = 10_000, restart: bool = False
) -> tuple[list[Record], dict[tuple[str, int, str], OccurrenceState]]:
    """Feed the events in batches to ``process``, each occurrence with its own state. With ``restart`` every state goes through JSON between batches."""
    states: dict[tuple[str, int, str], OccurrenceState] = {}
    records: list[Record] = []
    stamped = stamp(arrival)
    for first in range(0, len(stamped), batch_size):
        groups: defaultdict[tuple[str, int, str], list[Event]] = defaultdict(list)
        for event in stamped[first : first + batch_size]:
            groups[event.key].append(event)
        for key, events in groups.items():
            state, produced = process(states.get(key), events, context)
            states[key] = OccurrenceState.from_json(state.to_json()) if restart else state
            records.extend(produced)
    return records, states


def features_of(records: Sequence[Record]) -> dict[int, FeatureRecord]:
    """The feature records of one occurrence by cutoff."""
    found: dict[int, FeatureRecord] = {}
    for record in records:
        if isinstance(record, FeatureRecord):
            if record.cutoff_case_time_us in found:
                raise AssertionError(f"two feature rows for the cutoff {record.cutoff_case_time_us}")
            found[record.cutoff_case_time_us] = record
    return found


def values_of(record: FeatureRecord) -> dict[str, object]:
    features: dict[str, object] = json.loads(record.row_json)["features"]
    return features


def oracle_window(recording: CaseRecording, cutoff_us: int, clinical: Mapping[str, ClinicalValue] | None = None) -> WindowInput:
    """The window of a cutoff from the whole recording: the first reading of a track and time, every waveform second and the series built from them."""
    numeric: dict[str, list[Reading]] = {}
    for track in recording.numeric:
        if track.name in NUMERIC_TRACKS:
            first: dict[int, float] = {}
            for time, value in track.readings:
                first.setdefault(time, value)
            numeric[track.name] = [Reading(time, value, math.isfinite(value)) for time, value in sorted(first.items())]
    waveforms: dict[str, WaveformWindow] = {}
    summaries = {}
    for wave in recording.waveforms:
        if wave.name not in WAVEFORM_TRACKS:
            continue
        rate = int(wave.sample_rate)
        seconds = []
        for second, (offset, raw) in sorted(reference_seconds(wave).items()):
            samples = physical_samples(raw, wave.gain, wave.offset)
            summary = summarise_second(samples, rate, offset, arterial=wave.name == ARTERIAL_WAVEFORM)
            seconds.append(WaveformSecond(second, samples, summary.valid, offset))
            if wave.name == ARTERIAL_WAVEFORM:
                summaries[second] = summary
        waveforms[wave.name] = WaveformWindow(wave.sample_rate, tuple(seconds))
    if summaries:
        series = []
        for second, summary in sorted(summaries.items()):
            value, valid = series_reading(summaries.get(second - 1), summary)
            series.append(Reading(second * SECOND, value, valid))
        numeric[ARTERIAL_SERIES] = series
    return WindowInput(cutoff_us, numeric, waveforms, dict(clinical or {}))


def oracle_row(recording: CaseRecording, cutoff_us: int, clinical: Mapping[str, ClinicalValue] | None = None) -> FeatureRow:
    return build_row(oracle_window(recording, cutoff_us, clinical))


def oracle_values(recording: CaseRecording, cutoff_us: int, clinical: Mapping[str, ClinicalValue] | None = None) -> dict[str, object]:
    document = json.loads(json.dumps(row_document("oracle", oracle_row(recording, cutoff_us, clinical)), allow_nan=False))
    values: dict[str, object] = document["features"]
    return values


def oracle_eligibility(recording: CaseRecording, cutoff_us: int) -> tuple[bool, tuple[str, ...]]:
    window = oracle_window(recording, cutoff_us)
    result = warning_eligibility(cutoff_us, window.numeric.get(ARTERIAL_SERIES, ()), window.waveforms[ARTERIAL_WAVEFORM])
    return result.eligible, result.reasons
