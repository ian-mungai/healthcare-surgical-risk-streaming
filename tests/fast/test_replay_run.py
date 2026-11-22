"""The replay loop over its edges: rooms that overlap, a restart in the middle of a case, the position write and the frozen schedule.

The loop is run against in-memory stand-ins for the sink, the state file and the clock (``replay_support``). They prove the loop's own logic: the order of the
sends, which events are sent again after a restart and when the position is written. The broker, the volume and the real clock are proved by
``tests/e2e/stack_replay.py``.

Failure modes covered here:

- Two rooms are played one after the other, so a room never starts its next case while the other is still active, or one room's events are sent out of order.
- A restart in the middle of a case loses events, or sends events that were never sent, or sends one ID with another payload.
- The position is written before Kafka acknowledged the events it covers, so a stop between the two leaves a gap that no restart fills.
- A stop after the acknowledgment and before the position write sends nothing again, so the replay is not at least once, or it sends more than the events after
  the last position.
- A restart with a changed schedule or with a changed plan resumes against the wrong positions instead of being refused.
- The replay time goes backwards, or the speed changes the IDs, the payloads or the replay times.
- An occurrence outside the window is sent, an occurrence inside it is not, or a case is decoded more than once for one occurrence.
- A finished run sends again when it is started again.
- A stop that is asked for leaves the position behind the last event sent, so the restart sends events twice that it did not need to.
"""

from __future__ import annotations

import unittest
from collections import Counter
from typing import Any

from replay.events.manifest import Observed, build_manifest, compare
from replay.events.payload import payload_digest
from replay.events.run import Replayer, ReplayError, RunSettings, RunState
from replay.events.schedule import admitted_occurrences
from tests.fast.replay_support import FakeClock, MemorySink, MemoryStore, RecordingSource, StopHere, boundary_recordings, boundary_schedule, saved_state

FAST = RunSettings(speed=1000.0, checkpoint_every=5, idle_checkpoint_us=10**12)


def observed_from(sink: MemorySink, state: RunState | None) -> Observed:
    digests: dict[str, list[str]] = {}
    for event in sink.sent:
        digests.setdefault(event.event_id, []).append(payload_digest(event.payload))
    first = state.first_send.event_id if state and state.first_send else None
    return Observed(digests, frozenset(state.admissions) if state else frozenset(), first)


def run_once(frozen_options: dict[str, Any], settings: RunSettings = FAST) -> tuple[MemorySink, MemoryStore, FakeClock, Replayer]:
    frozen = boundary_schedule(**frozen_options)
    sink, store, clock = MemorySink(), MemoryStore(), FakeClock()
    replayer = Replayer(frozen, settings, RecordingSource(boundary_recordings()), sink, store, clock)
    return sink, store, clock, replayer


class Overlap(unittest.TestCase):
    def test_control_a_full_run_sends_every_expected_event_once_and_passes_the_manifest(self) -> None:
        options = {"rooms": 2, "window_us": 40_000_000}
        sink, store, _, replayer = run_once(options)
        result = replayer.run()
        manifest = build_manifest(boundary_schedule(**options), boundary_recordings())
        report = compare(manifest, observed_from(sink, store.state))
        self.assertTrue(report.ok, report.failures())
        self.assertEqual((len(manifest.events), True), (result.sent, result.finished))
        self.assertEqual(len(manifest.events), len(sink.sent))
        self.assertEqual(0, report.duplicate_count)

    def test_one_room_starts_its_next_case_while_the_other_room_is_still_active(self) -> None:
        sink, _, _, replayer = run_once({"rooms": 2, "window_us": 40_000_000})
        replayer.run()
        order = [(event.room, event.occurrence_id) for event in sink.sent]
        last_of_long = max(index for index, item in enumerate(order) if item == (1, "9002.0"))
        first_of_next = min(index for index, item in enumerate(order) if item == (0, "9001.1"))
        self.assertLess(first_of_next, last_of_long)
        self.assertLess(min(index for index, item in enumerate(order) if item == (1, "9002.0")), first_of_next)

    def test_the_events_of_one_room_stay_in_their_own_order(self) -> None:
        sink, _, _, replayer = run_once({"rooms": 2, "window_us": 40_000_000})
        replayer.run()
        for room in (0, 1):
            dues = [event.due_us for event in sink.sent if event.room == room]
            self.assertEqual(sorted(dues), dues, f"room {room}")

    def test_the_replay_time_only_moves_forward_across_all_rooms(self) -> None:
        sink, _, _, replayer = run_once({"rooms": 2, "window_us": 40_000_000})
        replayer.run()
        dues = [event.due_us for event in sink.sent]
        self.assertEqual(sorted(dues), dues)
        self.assertEqual(dues[0], boundary_schedule(rooms=2, window_us=40_000_000).first_due_us)

    def test_the_replay_time_of_a_room_stays_forward_where_the_file_holds_events_outside_its_case(self) -> None:
        sink, _, _, replayer = run_once({"rooms": 1, "window_us": 25_000_000, "cases": (9003,)})
        replayer.run()
        dues = [event.due_us for event in sink.sent]
        self.assertEqual(sorted(dues), dues)
        self.assertEqual({"9003.0", "9003.1", "9003.2"}, {event.occurrence_id for event in sink.sent})
        last_of_first = max(index for index, event in enumerate(sink.sent) if event.occurrence_id == "9003.0")
        first_of_second = min(index for index, event in enumerate(sink.sent) if event.occurrence_id == "9003.1")
        self.assertLess(last_of_first, first_of_second)
        self.assertEqual("case_end", sink.sent[last_of_first].kind)

    def test_an_event_carries_the_room_of_its_occurrence(self) -> None:
        sink, _, _, replayer = run_once({"rooms": 2, "window_us": 40_000_000})
        replayer.run()
        rooms = {item.occurrence.id: item.occurrence.room for item in admitted_occurrences(boundary_schedule(rooms=2, window_us=40_000_000))}
        self.assertTrue(all(event.room == rooms[event.occurrence_id] and event.payload["room"] == event.room for event in sink.sent))

    def test_an_occurrence_outside_the_window_is_not_sent_and_an_admitted_one_plays_in_full(self) -> None:
        sink, store, _, replayer = run_once({"rooms": 2, "window_us": 15_000_000})
        replayer.run()
        self.assertEqual({"9001.0", "9002.0"}, {event.occurrence_id for event in sink.sent})
        self.assertEqual({"9001.0", "9002.0"}, set(store.state.admissions if store.state else {}))
        self.assertEqual(2, sum(1 for event in sink.sent if event.kind == "case_end"))

    def test_each_admitted_case_is_decoded_once_when_its_occurrence_starts(self) -> None:
        options = {"rooms": 2, "window_us": 40_000_000}
        frozen = boundary_schedule(**options)
        source = RecordingSource(boundary_recordings())
        Replayer(frozen, FAST, source, MemorySink(), MemoryStore(), FakeClock()).run()
        self.assertEqual(Counter({9001: 3, 9002: 2}), Counter(source.reads))

    def test_the_speed_changes_the_waits_and_nothing_that_is_sent(self) -> None:
        options = {"rooms": 2, "window_us": 40_000_000}
        slow_sink, _, slow_clock, slow = run_once(options, RunSettings(speed=1.0, checkpoint_every=5, idle_checkpoint_us=10**12))
        fast_sink, _, fast_clock, fast = run_once(options, RunSettings(speed=4.0, checkpoint_every=5, idle_checkpoint_us=10**12))
        start = slow_clock.now
        slow.run()
        fast.run()
        self.assertEqual([(event.event_id, event.payload) for event in slow_sink.sent], [(event.event_id, event.payload) for event in fast_sink.sent])
        self.assertEqual(60 * 1_000_000, slow_clock.now - start)
        self.assertEqual(15 * 1_000_000, fast_clock.now - start)

    def test_the_wall_time_of_a_send_is_the_first_send_plus_the_replay_time_over_the_speed(self) -> None:
        frozen = boundary_schedule(rooms=2, window_us=40_000_000)
        sink, store, clock = MemorySink(), MemoryStore(), FakeClock()
        anchor = clock.now
        times: list[int] = []
        sink.on_send = lambda event: times.append(clock.now)
        settings = RunSettings(speed=2.0, checkpoint_every=5, idle_checkpoint_us=10**12)
        Replayer(frozen, settings, RecordingSource(boundary_recordings()), sink, store, clock).run()
        self.assertEqual([anchor + (event.due_us - frozen.first_due_us) // 2 for event in sink.sent], times)
        self.assertEqual(anchor, store.state.anchor_wall_us if store.state else None)


class Positions(unittest.TestCase):
    def test_the_position_is_written_only_after_the_events_it_covers_were_acknowledged(self) -> None:
        sink, store, _, replayer = run_once({"rooms": 2, "window_us": 40_000_000})
        store.before_save = lambda state: self.assertEqual(len(sink.sent), sink.acknowledged, "a position was written for events that were not flushed")
        replayer.run()
        self.assertGreater(store.saves, 3)

    def test_the_saved_positions_count_the_events_of_each_room_that_were_sent(self) -> None:
        sink, store, _, replayer = run_once({"rooms": 2, "window_us": 40_000_000})
        replayer.run()
        state = saved_state(store)
        self.assertTrue(state.finished)
        self.assertEqual({0, 1}, set(state.positions))
        self.assertEqual({0: 3, 1: 2}, {room: position.completed for room, position in state.positions.items()})

    def test_a_state_written_and_read_back_is_the_same_state(self) -> None:
        _, store, _, replayer = run_once({"rooms": 2, "window_us": 40_000_000})
        replayer.run()
        state = saved_state(store)
        self.assertEqual(state, RunState.from_json(state.to_json()))


class Restart(unittest.TestCase):
    OPTIONS = {"rooms": 2, "window_us": 40_000_000}

    def stop_then_resume(self, stop_before_save: int) -> tuple[MemorySink, MemoryStore, int]:
        frozen = boundary_schedule(**self.OPTIONS)
        sink, store, clock = MemorySink(), MemoryStore(), FakeClock()
        store.stop_before_save = stop_before_save
        with self.assertRaises(StopHere):
            Replayer(frozen, FAST, RecordingSource(boundary_recordings()), sink, store, clock).run()
        sent_before = len(sink.sent)
        store.stop_before_save = None
        Replayer(frozen, FAST, RecordingSource(boundary_recordings()), sink, store, clock).run()
        return sink, store, sent_before

    def test_a_stop_after_the_acknowledgment_and_before_the_position_write_sends_the_events_after_the_last_position_again(self) -> None:
        sink, store, sent_before = self.stop_then_resume(3)
        manifest = build_manifest(boundary_schedule(**self.OPTIONS), boundary_recordings())
        counts = Counter(event.event_id for event in sink.sent)
        repeated = sorted(key for key, count in counts.items() if count > 1)
        self.assertEqual(5, len(repeated), "five events were acknowledged after the last position and are sent again")
        self.assertEqual(set(manifest.events), set(counts))
        report = compare(manifest, observed_from(sink, store.state))
        self.assertTrue(report.ok, report.failures())
        self.assertEqual(5, report.duplicate_count)
        self.assertEqual(len(manifest.events) + 5, len(sink.sent))
        self.assertGreater(sent_before, 10)

    def test_every_event_sent_again_is_the_same_id_with_the_same_payload(self) -> None:
        sink, _, _ = self.stop_then_resume(3)
        seen: dict[str, str] = {}
        for event in sink.sent:
            digest = payload_digest(event.payload)
            self.assertEqual(seen.setdefault(event.event_id, digest), digest, event.event_id)

    def test_a_stop_at_each_position_write_of_the_run_leaves_no_gap_and_no_unexpected_id(self) -> None:
        manifest = build_manifest(boundary_schedule(**self.OPTIONS), boundary_recordings())
        _, whole_store, _, whole = run_once(self.OPTIONS)
        whole.run()
        self.assertGreater(whole_store.saves, 8)
        for stop in range(1, whole_store.saves + 1):
            with self.subTest(stop=stop):
                sink, store, _ = self.stop_then_resume(stop)
                report = compare(manifest, observed_from(sink, store.state))
                self.assertTrue(report.ok, report.failures())
                self.assertTrue(store.state is not None and store.state.finished)

    def test_the_restart_in_the_middle_of_a_case_resumes_at_the_last_saved_position(self) -> None:
        sink, store, sent_before = self.stop_then_resume(3)
        self.assertEqual(15, sent_before)
        self.assertEqual([event.event_id for event in sink.sent[10:15]], [event.event_id for event in sink.sent[15:20]])
        self.assertTrue(store.state is not None and store.state.finished)

    def test_the_first_send_and_the_admissions_survive_a_restart(self) -> None:
        sink, store, _ = self.stop_then_resume(3)
        state = saved_state(store)
        manifest = build_manifest(boundary_schedule(**self.OPTIONS), boundary_recordings())
        first = state.first_send.event_id if state.first_send else None
        self.assertIn(first, manifest.first_due_event_ids)
        self.assertEqual(set(manifest.occurrences), set(state.admissions))
        self.assertEqual(sink.sent[0].event_id, first)

    def test_the_pacing_continues_from_the_first_send_after_a_restart(self) -> None:
        frozen = boundary_schedule(**self.OPTIONS)
        sink, store, clock = MemorySink(), MemoryStore(), FakeClock()
        anchor = clock.now
        store.stop_before_save = 3
        settings = RunSettings(speed=2.0, checkpoint_every=5, idle_checkpoint_us=10**12)
        with self.assertRaises(StopHere):
            Replayer(frozen, settings, RecordingSource(boundary_recordings()), sink, store, clock).run()
        clock.now += 3_000_000
        store.stop_before_save = None
        result = Replayer(frozen, settings, RecordingSource(boundary_recordings()), sink, store, clock).run()
        self.assertTrue(result.finished)
        last = max(event.due_us for event in sink.sent)
        self.assertEqual(anchor + (last - frozen.first_due_us) // 2, clock.now)
        self.assertEqual(anchor, saved_state(store).anchor_wall_us)

    def test_a_stop_that_is_asked_for_saves_the_position_at_the_last_event_sent_and_the_restart_sends_nothing_twice(self) -> None:
        frozen = boundary_schedule(**self.OPTIONS)
        sink, store, clock = MemorySink(), MemoryStore(), FakeClock()
        sink.on_send = lambda event: None
        stop = {"after": 7}

        def requested() -> bool:
            return len(sink.sent) >= stop["after"]

        first = Replayer(frozen, FAST, RecordingSource(boundary_recordings()), sink, store, clock, requested).run()
        self.assertEqual((7, False), (first.sent, first.finished))
        self.assertFalse(saved_state(store).finished)
        stop["after"] = 10**9
        second = Replayer(frozen, FAST, RecordingSource(boundary_recordings()), sink, store, clock).run()
        manifest = build_manifest(frozen, boundary_recordings())
        self.assertEqual(len(manifest.events), len(sink.sent))
        self.assertEqual(len(manifest.events) - 7, second.sent)
        self.assertTrue(compare(manifest, observed_from(sink, store.state)).ok)

    def test_a_stop_asked_for_during_a_wait_sends_no_event_after_it_saves_the_position_and_the_restart_sends_nothing_twice(self) -> None:
        class StopsDuringWait(FakeClock):
            """The stop arrives while the loop waits for the event that is due next: the wait ends at once, as the service's clock does on a signal."""

            def __init__(self) -> None:
                super().__init__()
                self.stopped = False
                self.waiting = 0
                self.sent_at_stop = -1

            def wait_until(self, wall_us: int) -> None:
                if wall_us > self.now:
                    self.waiting += 1
                    if self.waiting == 3:
                        self.stopped = True
                        self.sent_at_stop = len(sink.sent)
                        return
                super().wait_until(wall_us)

        frozen = boundary_schedule(**self.OPTIONS)
        slow = RunSettings(speed=1.0, checkpoint_every=1000, idle_checkpoint_us=10**12)
        sink, store, clock = MemorySink(), MemoryStore(), StopsDuringWait()
        first = Replayer(frozen, slow, RecordingSource(boundary_recordings()), sink, store, clock, lambda: clock.stopped).run()
        self.assertFalse(first.finished)
        self.assertEqual(clock.sent_at_stop, len(sink.sent), "an event was sent after the stop was asked for")
        self.assertEqual(first.sent, len(sink.sent))
        self.assertEqual(len(sink.sent), sink.acknowledged)
        self.assertFalse(saved_state(store).finished)
        sent_before = len(sink.sent)
        second = Replayer(frozen, slow, RecordingSource(boundary_recordings()), sink, store, FakeClock(clock.now), lambda: False).run()
        manifest = build_manifest(frozen, boundary_recordings())
        self.assertEqual(len(manifest.events) - sent_before, second.sent)
        self.assertEqual(len(manifest.events), len(sink.sent))
        self.assertTrue(compare(manifest, observed_from(sink, store.state)).ok)

    def test_a_stop_asked_for_while_the_cases_of_the_rooms_are_decoded_reads_no_further_case_and_sends_nothing(self) -> None:
        frozen = boundary_schedule(**self.OPTIONS)
        source = RecordingSource(boundary_recordings())
        sink, store = MemorySink(), MemoryStore()
        result = Replayer(frozen, FAST, source, sink, store, FakeClock(), lambda: len(source.reads) >= 1).run()
        self.assertEqual((0, False), (result.sent, result.finished))
        self.assertEqual(1, len(source.reads))
        self.assertEqual([], sink.sent)
        resumed = Replayer(frozen, FAST, RecordingSource(boundary_recordings()), sink, store, FakeClock(), lambda: False).run()
        self.assertEqual(len(build_manifest(frozen, boundary_recordings()).events), resumed.sent)
        self.assertTrue(resumed.finished)

    def test_a_finished_run_started_again_sends_nothing(self) -> None:
        frozen = boundary_schedule(**self.OPTIONS)
        sink, store, clock = MemorySink(), MemoryStore(), FakeClock()
        Replayer(frozen, FAST, RecordingSource(boundary_recordings()), sink, store, clock).run()
        count = len(sink.sent)
        source = RecordingSource(boundary_recordings())
        result = Replayer(frozen, FAST, source, sink, store, clock).run()
        self.assertEqual((count, 0, True, []), (len(sink.sent), result.sent, result.finished, source.reads))

    def test_a_saved_state_of_another_schedule_is_refused_before_anything_is_sent(self) -> None:
        sink, store, clock = MemorySink(), MemoryStore(), FakeClock()
        Replayer(boundary_schedule(**self.OPTIONS), FAST, RecordingSource(boundary_recordings()), sink, store, clock).run()
        other = Replayer(boundary_schedule(rooms=2, window_us=41_000_000), FAST, RecordingSource(boundary_recordings()), MemorySink(), store, clock)
        with self.assertRaisesRegex(ReplayError, "schedule"):
            other.run()

    def test_a_speed_outside_0_01_to_1000_or_a_checkpoint_that_is_not_positive_is_refused_and_the_edges_are_accepted(self) -> None:
        for settings in ((0.0, 5), (-1.0, 5), (float("inf"), 5), (float("nan"), 5), (5e-324, 5), (0.0099, 5), (1000.5, 5), (1.0, 0)):
            with self.subTest(settings=settings), self.assertRaises(ReplayError):
                RunSettings(speed=settings[0], checkpoint_every=settings[1])
        for speed in (0.01, 1000.0):
            self.assertEqual(speed, RunSettings(speed=speed).speed)
