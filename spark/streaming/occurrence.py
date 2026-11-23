"""One case occurrence: the late-event decision of each event, the buffers, the cutoffs and the feature rows.

**Lateness is on the replay time.** The watermark of an occurrence is the highest replay time among the events it has applied. An event whose
replay time is at most ``late_limit_us`` behind it is applied, and recorded as late when it is behind at all. An event further behind is recorded
with its decision and counted and is never applied. The watermark belongs to the occurrence: another room, or the next case of the same room, has
its own, so a room that starts its next case or is read ahead of another room discards no in-policy event of another occurrence. An event whose
ID the occurrence applied a moment ago is a duplicate: a replay that stopped after Kafka's acknowledgment and before its position write sends the
same IDs again, and they have no second effect.

**Features are on the case time.** A row is built at every ``cutoff_step_us`` of case time and at ``opend``, the case time of the final-score
marker. A cutoff is emitted once no in-policy event can still arrive for it: when the watermark is further than the limit past the cutoff, or at
the case-end marker. So a row does not depend on how the events were cut into batches. The values are ``features.row.build_row`` of the window of
the cutoff, and a window that the feature code refuses leaves an error record and does not stop the job.

**Source duplicates.** Two readings of one track and one case time keep the one first in the file, the lowest discriminator, whatever their
arrival order.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence

from features.batch import compute_rows, row_document
from features.eligibility import warning_eligibility
from features.model import ClinicalValue, Reading, WaveformSecond, WaveformWindow, WindowInput
from features.registry import ARTERIAL_SERIES, NUMERIC_TRACKS, NUMERIC_WINDOW_SECONDS, WAVEFORM_TRACKS, WAVEFORM_WINDOW_SECONDS, complete_seconds
from spark.streaming.model import (
    AFTER_CASE_END,
    DUPLICATE,
    LATE_ACCEPTED,
    ON_TIME,
    SECOND,
    SOURCE_DUPLICATE,
    TOO_LATE,
    Context,
    DecisionRecord,
    ErrorRecord,
    Event,
    FeatureRecord,
    LedgerRecord,
    Record,
)
from spark.streaming.signals import ARTERIAL_WAVEFORM, physical_samples, series_reading, summarise_second
from spark.streaming.state import OccurrenceState, WaveSecondState
from spark.streaming.version import error_id, feature_id, ledger_id

PRUNE_SLACK_SECONDS = 2


def new_state(event: Event) -> OccurrenceState:
    return OccurrenceState(event.run_id, event.room, event.occurrence_id, event.case_id)


def whole_rate(sample_rate: float) -> int:
    """The sample rate as a whole number, or 0 when it is not one: a second of such a track is never valid, and the feature code names the rate."""
    return int(sample_rate) if math.isfinite(sample_rate) and sample_rate > 0 and sample_rate == int(sample_rate) else 0


def process(state: OccurrenceState | None, events: Sequence[Event], context: Context) -> tuple[OccurrenceState, list[Record]]:
    """Apply the events of one case occurrence, in the order of their Kafka offsets, to its state and return the new state and the records.

    The state is changed in place and returned. Every event must belong to the occurrence of the state, and the same input gives the same records.
    """
    if not events:
        raise ValueError("a call needs at least one event")
    if state is None:
        state = new_state(events[0])
    for event in events:
        if event.key != (state.run_id, state.room, state.occurrence_id):
            raise ValueError("every event of a call must belong to the occurrence of its state")
    records: list[Record] = []
    for event in sorted(events, key=lambda item: item.kafka_offset):
        decide(state, event, context, records)
    if not state.closed:
        emit_due(state, context, records, force_to=None)
    prune_recent(state, context)
    return state, records


def decision_record(state: OccurrenceState, event: Event, decision: str, lateness_us: int) -> DecisionRecord:
    return DecisionRecord(
        f"{event.event_id}#{event.kafka_partition}:{event.kafka_offset}",
        event.event_id,
        state.run_id,
        state.room,
        state.occurrence_id,
        event.kind,
        event.track,
        event.case_time_us,
        event.replay_time_us,
        state.high_water_us if state.high_water_us is not None else event.replay_time_us,
        lateness_us,
        decision,
        event.kafka_partition,
        event.kafka_offset,
    )


def decide(state: OccurrenceState, event: Event, context: Context, records: list[Record]) -> None:
    watermark = state.high_water_us
    lateness = 0 if watermark is None else max(0, watermark - event.replay_time_us)
    seen = event.suffix in state.recent
    if state.closed:
        decision = DUPLICATE if seen else AFTER_CASE_END
        state.count(decision)
        records.append(decision_record(state, event, decision, lateness))
        state.recent.setdefault(event.suffix, event.replay_time_us)
        return
    if lateness > context.policy.late_limit_us:
        state.count(TOO_LATE)
        records.append(decision_record(state, event, TOO_LATE, lateness))
        return
    if seen:
        state.count(DUPLICATE)
        records.append(decision_record(state, event, DUPLICATE, lateness))
        return
    state.recent[event.suffix] = event.replay_time_us
    if not apply(state, event, lateness, context, records):
        return
    if watermark is None or event.replay_time_us > watermark:
        state.high_water_us = event.replay_time_us
    if lateness > 0:
        state.count(LATE_ACCEPTED)
        records.append(decision_record(state, event, LATE_ACCEPTED, lateness))
    else:
        state.count(ON_TIME)
    if event.kind == "case_end":
        finish(state, event, context, records)


def apply(state: OccurrenceState, event: Event, lateness_us: int, context: Context, records: list[Record]) -> bool:
    """Put the event into the buffers. Returns False for an event that is dropped as a source duplicate (and recorded as one)."""
    if state.start_us is None and event.case_time_us >= 0:
        state.start_us = event.replay_time_us - event.due_offset_us
    if event.kind == "op_start":
        state.opstart_us = event.case_time_us
        wanted = (event.case_time_us, *(event.case_time_us + offset for offset in context.policy.risk_offsets_us))
        state.risk_pending = sorted({cutoff for cutoff in wanted if state.last_cutoff_us is None or cutoff > state.last_cutoff_us})
    elif event.kind == "final_score":
        state.opend_us = event.case_time_us
    elif event.kind == "string":
        state.count("string_events")
    elif event.kind == "numeric":
        if event.track not in NUMERIC_TRACKS or event.track == ARTERIAL_SERIES:
            state.count("ignored")
            return True
        readings = state.numeric.setdefault(event.track, {})
        held = readings.get(event.case_time_us)
        if held is not None and held[0] <= event.discriminator:
            state.count(SOURCE_DUPLICATE)
            records.append(decision_record(state, event, SOURCE_DUPLICATE, lateness_us))
            return False
        readings[event.case_time_us] = (event.discriminator, event.value)
    elif event.kind == "waveform":
        if event.track not in WAVEFORM_TRACKS:
            state.count("ignored")
            return True
        second = event.case_time_us // SECOND
        samples = physical_samples(event.samples, event.gain, event.physical_offset)
        state.wave_samples.setdefault(event.track, {})[second] = WaveSecondState(event.sample_rate, event.start_offset_us, samples)
        if event.track == ARTERIAL_WAVEFORM:
            state.art_summary[second] = summarise_second(samples, whole_rate(event.sample_rate), event.start_offset_us, arterial=True)
    return True


def next_cutoff(state: OccurrenceState, context: Context, bound_us: int | None) -> tuple[int, bool] | None:
    """The next cutoff to emit and whether it is the final one, or None.

    The cutoffs are the minute grid, the risk cutoffs that the start marker gave (``opstart`` and the frozen offsets after it) and ``opend``, the smallest
    first. A grid or risk cutoff at ``opend`` or after it is not built apart: ``opend`` is the final row. ``bound_us`` limits the cutoffs when ``opend``
    is not known.
    """
    grid = state.next_index * context.policy.cutoff_step_us
    opend = state.opend_us
    candidates = []
    if opend is None or grid < opend:
        candidates.append(grid)
    risk = [cutoff for cutoff in state.risk_pending if opend is None or cutoff < opend]
    if risk:
        candidates.append(min(risk))
    if opend is not None and not state.final_emitted:
        candidates.append(opend)
    if opend is None and bound_us is not None:
        candidates = [cutoff for cutoff in candidates if cutoff <= bound_us]
    if not candidates:
        return None
    cutoff = min(candidates)
    return cutoff, opend is not None and cutoff == opend and not state.final_emitted


def is_closed(state: OccurrenceState, context: Context, cutoff_us: int) -> bool:
    """No in-policy event can still arrive for the cutoff: every later applied event is due after it."""
    if state.start_us is None or state.high_water_us is None:
        return False
    return state.high_water_us - context.policy.late_limit_us > state.start_us + cutoff_us


def emit_due(state: OccurrenceState, context: Context, records: list[Record], force_to: int | None, *, force: bool = False) -> None:
    """Emit every cutoff that is closed, or every cutoff up to ``force_to`` (the case end) when forced, and drop what no later cutoff reads."""
    while (step := next_cutoff(state, context, force_to if force else None)) is not None:
        cutoff, final = step
        if not force and not is_closed(state, context, cutoff):
            break
        records.append(row_record(state, context, cutoff, final))
        if final:
            state.final_emitted = True
        while state.next_index * context.policy.cutoff_step_us <= cutoff:
            state.next_index += 1
        state.risk_pending = [pending for pending in state.risk_pending if pending > cutoff]
        state.last_cutoff_us = cutoff
    prune(state, context)


def row_record(state: OccurrenceState, context: Context, cutoff_us: int, final: bool) -> Record:
    identity = feature_id(state.run_id, state.room, state.occurrence_id, cutoff_us, context.version)
    window = window_at(state, cutoff_us, context)
    try:
        row = compute_rows([window])[0]
        series = window.numeric.get(ARTERIAL_SERIES, ())
        eligibility = warning_eligibility(cutoff_us, series, window.waveforms.get(ARTERIAL_WAVEFORM, WaveformWindow(1.0, ())))
        text = json.dumps(row_document(identity, row), sort_keys=True, allow_nan=False)
    except ValueError as error:
        return ErrorRecord(
            error_id(state.run_id, state.room, state.occurrence_id, cutoff_us, context.version),
            state.run_id,
            state.room,
            state.occurrence_id,
            cutoff_us,
            str(error)[:500],
        )
    start = state.start_us if state.start_us is not None else 0
    return FeatureRecord(
        identity,
        state.run_id,
        state.room,
        state.occurrence_id,
        state.case_id,
        context.version,
        cutoff_us,
        start + cutoff_us,
        final,
        text,
        eligibility.eligible,
        eligibility.reasons,
    )


def window_at(state: OccurrenceState, cutoff_us: int, context: Context) -> WindowInput:
    """The window of a cutoff from the buffers: only what the feature code reads at that cutoff."""
    numeric: dict[str, list[Reading]] = {}
    lower = cutoff_us - NUMERIC_WINDOW_SECONDS * SECOND
    for track, readings in state.numeric.items():
        inside = sorted((time, value) for time, (_, value) in readings.items() if lower < time <= cutoff_us)
        numeric[track] = [Reading(time, value, math.isfinite(value)) for time, value in inside]
    if state.art_summary:
        series = []
        for second in complete_seconds(cutoff_us, NUMERIC_WINDOW_SECONDS):
            summary = state.art_summary.get(second)
            if summary is not None:
                value, valid = series_reading(state.art_summary.get(second - 1), summary)
                series.append(Reading(second * SECOND, value, valid))
        numeric[ARTERIAL_SERIES] = series
    waveforms: dict[str, WaveformWindow] = {}
    span = complete_seconds(cutoff_us, WAVEFORM_WINDOW_SECONDS)
    for track, held in state.wave_samples.items():
        seconds = []
        for second in span:
            item = held.get(second)
            if item is None:
                continue
            if track == ARTERIAL_WAVEFORM:
                valid = state.art_summary[second].valid
            else:
                valid = summarise_second(item.samples, whole_rate(item.sample_rate), item.start_offset_us, arterial=False).valid
            seconds.append(WaveformSecond(second, item.samples, valid, item.start_offset_us))
        if not seconds:
            continue
        waveforms[track] = WaveformWindow(held[seconds[0].second].sample_rate, tuple(seconds))
    clinical: Mapping[str, ClinicalValue] = context.clinical(state.case_id)
    return WindowInput(cutoff_us, numeric, waveforms, dict(clinical))


def prune(state: OccurrenceState, context: Context) -> None:
    """Drop the buffered data that no cutoff still to come reads. After the final row nothing is read."""
    following = next_cutoff(state, context, None)
    if following is None:
        state.numeric.clear()
        state.wave_samples.clear()
        state.art_summary.clear()
        return
    margin_us = context.policy.cutoff_step_us + context.policy.late_limit_us
    cutoff = following[0] - margin_us
    lower_us = cutoff - NUMERIC_WINDOW_SECONDS * SECOND
    for readings in state.numeric.values():
        for time in [time for time in readings if time <= lower_us]:
            del readings[time]
    first_series = (cutoff // SECOND) - 1 - NUMERIC_WINDOW_SECONDS - PRUNE_SLACK_SECONDS
    for second in [second for second in state.art_summary if second < first_series]:
        del state.art_summary[second]
    first_wave = (cutoff // SECOND) - 1 - WAVEFORM_WINDOW_SECONDS - PRUNE_SLACK_SECONDS
    for seconds in state.wave_samples.values():
        for second in [second for second in seconds if second < first_wave]:
            del seconds[second]


def prune_recent(state: OccurrenceState, context: Context) -> None:
    if state.high_water_us is None:
        return
    horizon = state.high_water_us - context.policy.late_limit_us
    for suffix in [suffix for suffix, time in state.recent.items() if time < horizon]:
        del state.recent[suffix]


def finish(state: OccurrenceState, event: Event, context: Context, records: list[Record]) -> None:
    """The case-end marker: every cutoff still to come is emitted, the ledger is written once and the buffers are cleared."""
    emit_due(state, context, records, event.case_time_us, force=True)
    counts = dict(sorted(state.counts.items())) | {"final_seen": state.opend_us is not None}
    records.append(
        LedgerRecord(
            ledger_id(state.run_id, state.room, state.occurrence_id),
            state.run_id,
            state.room,
            state.occurrence_id,
            state.case_id,
            json.dumps(counts, sort_keys=True),
        )
    )
    state.closed = True
    state.numeric.clear()
    state.wave_samples.clear()
    state.art_summary.clear()
