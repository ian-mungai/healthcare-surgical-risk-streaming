"""The replay schedule: when each case occurrence starts in each room, the first due instant, the half-open window and which occurrences are admitted.

Failure modes covered here:

- A room starts its next case at the wrong instant, so two rooms that overlap in real life never overlap in the replay (the stream clock case).
- The window is closed at its end, or open at its start, so an occurrence whose mapped ``opstart`` sits on an edge is admitted or refused wrongly.
- An occurrence is admitted by its start instead of its mapped ``opstart``.
- The first due instant is taken from the plan and not from the first reading of the cases, so the first send is checked against the wrong instant.
- The frozen schedule changes after a restart, or a changed plan is accepted against a saved schedule, so the expected window moves.
- The speed enters the schedule, so the same run at another speed has other IDs or another window.
"""

from __future__ import annotations

import unittest
from collections.abc import Callable

from replay.events.errors import EventModelError
from replay.events.recording import CaseTimes
from replay.events.schedule import FrozenSchedule, Plan, ScheduleError, admitted_occurrences, ensure_same_schedule, first_due_instant, freeze
from replay.events.stream import earliest_due_offset_us
from tests.fast.replay_support import RUN_ID, SECOND, boundary_plan, boundary_recordings, boundary_schedule

A = CaseTimes(9001, 4 * SECOND, 10 * SECOND, 14 * SECOND)
B = CaseTimes(9002, 6 * SECOND, 20 * SECOND, 30 * SECOND)


def plan(**changes: int) -> Plan:
    values = {"rooms": 2, "stagger_us": 0, "turnover_us": 0, "window_us": 100 * SECOND} | changes
    return Plan(RUN_ID, values["rooms"], (A, B), values["stagger_us"], values["turnover_us"], values["window_us"])


def occurrences(frozen: FrozenSchedule) -> list[tuple[str, int, int, int]]:
    return [(item.occurrence.id, item.occurrence.room, item.start_us // SECOND, item.end_us // SECOND) for item in admitted_occurrences(frozen)]


class Timeline(unittest.TestCase):
    def test_each_room_plays_its_cases_one_after_the_other_from_its_own_start(self) -> None:
        frozen = freeze(plan(window_us=60 * SECOND), 0)
        self.assertEqual(
            [("9001.0", 0, 0, 14), ("9002.0", 1, 0, 30), ("9001.1", 0, 14, 28), ("9002.1", 1, 30, 60)],
            [row for row in occurrences(frozen) if row[0] in ("9001.0", "9002.0", "9001.1", "9002.1")],
        )

    def test_a_room_starts_its_next_case_while_the_other_room_is_still_active(self) -> None:
        by_id = {item.occurrence.id: item for item in admitted_occurrences(freeze(plan(window_us=60 * SECOND), 0))}
        other_room = by_id["9002.0"]
        next_in_room = by_id["9001.1"]
        self.assertNotEqual(other_room.occurrence.room, next_in_room.occurrence.room)
        self.assertTrue(other_room.start_us < next_in_room.start_us < other_room.end_us)

    def test_rooms_start_apart_by_the_stagger_and_a_turnover_separates_the_cases_of_a_room(self) -> None:
        frozen = freeze(plan(stagger_us=5 * SECOND, turnover_us=3 * SECOND, window_us=60 * SECOND), 0)
        rows = {row[0]: row for row in occurrences(frozen)}
        self.assertEqual((0, 14), rows["9001.0"][2:])
        self.assertEqual((5, 35), rows["9002.0"][2:])
        self.assertEqual((17, 31), rows["9001.1"][2:])

    def test_the_mapped_markers_lie_at_the_start_of_the_occurrence_plus_the_clinical_times(self) -> None:
        item = next(item for item in admitted_occurrences(freeze(plan(window_us=60 * SECOND), 0)) if item.occurrence.id == "9001.1")
        self.assertEqual((14 * SECOND, 18 * SECOND, 24 * SECOND, 28 * SECOND), (item.start_us, item.mapped_opstart_us, item.mapped_opend_us, item.end_us))

    def test_the_replay_time_does_not_depend_on_a_speed(self) -> None:
        self.assertNotIn("speed", FrozenSchedule.__dataclass_fields__)
        self.assertNotIn("speed", Plan.__dataclass_fields__)


class WindowAndAdmission(unittest.TestCase):
    def test_the_window_runs_from_the_first_due_instant_for_the_window_length(self) -> None:
        frozen = freeze(plan(window_us=50 * SECOND), 3 * SECOND)
        self.assertEqual((3 * SECOND, 3 * SECOND, 53 * SECOND), (frozen.first_due_us, frozen.window_start_us, frozen.window_end_us))

    def test_an_occurrence_is_admitted_when_its_mapped_opstart_is_inside_the_half_open_window(self) -> None:
        ids = {item.occurrence.id for item in admitted_occurrences(freeze(plan(window_us=17 * SECOND + 1), 0))}
        self.assertEqual({"9001.0", "9002.0"}, ids)
        ids = {item.occurrence.id for item in admitted_occurrences(freeze(plan(window_us=18 * SECOND + 1), 0))}
        self.assertEqual({"9001.0", "9002.0", "9001.1"}, ids)

    def test_a_mapped_opstart_on_the_end_of_the_window_is_not_admitted(self) -> None:
        ids = {item.occurrence.id for item in admitted_occurrences(freeze(plan(window_us=18 * SECOND), 0))}
        self.assertEqual({"9001.0", "9002.0"}, ids)

    def test_a_mapped_opstart_on_the_start_of_the_window_is_admitted(self) -> None:
        frozen = freeze(plan(window_us=100 * SECOND), 4 * SECOND)
        self.assertIn("9001.0", {item.occurrence.id for item in admitted_occurrences(frozen)})
        later = freeze(plan(window_us=100 * SECOND), 4 * SECOND + 1)
        self.assertNotIn("9001.0", {item.occurrence.id for item in admitted_occurrences(later)})

    def test_an_occurrence_that_starts_inside_but_maps_its_opstart_outside_is_not_admitted(self) -> None:
        frozen = freeze(plan(window_us=15 * SECOND), 0)
        self.assertEqual({"9001.0", "9002.0"}, {item.occurrence.id for item in admitted_occurrences(frozen)})
        self.assertLess(14 * SECOND, frozen.window_end_us)

    def test_an_admitted_occurrence_plays_in_full_even_where_it_ends_after_the_window(self) -> None:
        frozen = freeze(plan(window_us=15 * SECOND), 0)
        long_one = next(item for item in admitted_occurrences(frozen) if item.occurrence.id == "9002.0")
        self.assertGreater(long_one.end_us, frozen.window_end_us)

    def test_the_admitted_occurrences_come_in_the_order_of_their_start_then_their_place_in_the_play_order(self) -> None:
        got = admitted_occurrences(freeze(plan(window_us=100 * SECOND), 0))
        keys = [(item.start_us, item.occurrence.sequence) for item in got]
        self.assertEqual(sorted(keys), keys)

    def test_a_longer_window_admits_more_and_never_fewer(self) -> None:
        sizes = [len(admitted_occurrences(freeze(plan(window_us=seconds * SECOND), 0))) for seconds in range(1, 120, 7)]
        self.assertEqual(sorted(sizes), sizes)


class FirstDueInstant(unittest.TestCase):
    def test_it_is_the_earliest_first_reading_over_the_first_occurrence_of_every_room(self) -> None:
        recordings = boundary_recordings()
        offsets = {case: earliest_due_offset_us(recordings[case]) for case in recordings}
        self.assertEqual({9001: 0, 9002: 0, 9003: 0}, offsets)
        self.assertEqual(0, first_due_instant(boundary_plan(stagger_us=5 * SECOND), lambda case: offsets[case]))

    def test_a_room_that_starts_later_can_hold_the_earliest_reading(self) -> None:
        self.assertEqual(2 * SECOND, first_due_instant(boundary_plan(stagger_us=2 * SECOND), lambda case: 9 * SECOND if case == 9001 else 0))

    def test_more_rooms_than_cases_still_start_each_room_with_a_case(self) -> None:
        frozen = boundary_schedule(rooms=5)
        first_cases = {item.occurrence.room: item.occurrence.case_id for item in admitted_occurrences(frozen) if item.occurrence.sequence < 5}
        self.assertEqual({0: 9001, 1: 9002, 2: 9001, 3: 9002, 4: 9001}, first_cases)


class Freezing(unittest.TestCase):
    def test_the_frozen_schedule_has_a_digest_that_the_same_plan_and_instant_reproduce(self) -> None:
        self.assertEqual(freeze(plan(), 0).sha256, freeze(plan(), 0).sha256)
        self.assertRegex(freeze(plan(), 0).sha256, r"[0-9a-f]{64}")

    def test_each_part_of_the_plan_and_the_first_due_instant_changes_the_digest(self) -> None:
        reference = freeze(plan(), 0).sha256
        changed = [
            freeze(plan(rooms=3), 0),
            freeze(plan(stagger_us=1), 0),
            freeze(plan(turnover_us=1), 0),
            freeze(plan(window_us=99 * SECOND), 0),
            freeze(plan(), 1),
            freeze(Plan("replay_ffffffff", 2, (A, B), 0, 0, 100 * SECOND), 0),
            freeze(Plan(RUN_ID, 2, (B, A), 0, 0, 100 * SECOND), 0),
            freeze(Plan(RUN_ID, 2, (A, CaseTimes(9002, 6 * SECOND, 20 * SECOND, 31 * SECOND)), 0, 0, 100 * SECOND), 0),
        ]
        self.assertEqual(len(changed), len({item.sha256 for item in changed} - {reference}))

    def test_a_schedule_written_and_read_back_is_the_same_schedule(self) -> None:
        frozen = freeze(plan(stagger_us=2 * SECOND), 7)
        self.assertEqual(frozen, FrozenSchedule.from_json(frozen.to_json()))

    def test_a_saved_schedule_that_was_edited_is_refused(self) -> None:
        text = freeze(plan(), 0).to_json().replace('"rooms": 2', '"rooms": 3')
        with self.assertRaisesRegex(ScheduleError, "digest"):
            FrozenSchedule.from_json(text)

    def test_a_plan_that_differs_from_the_saved_schedule_is_refused_with_the_part_that_differs(self) -> None:
        saved = freeze(plan(), 0)
        ensure_same_schedule(saved, freeze(plan(), 0))
        with self.assertRaisesRegex(ScheduleError, "window"):
            ensure_same_schedule(saved, freeze(plan(window_us=99 * SECOND), 0))
        with self.assertRaisesRegex(ScheduleError, "first due"):
            ensure_same_schedule(saved, freeze(plan(), 1))

    def test_a_plan_that_cannot_be_scheduled_is_refused(self) -> None:
        bad: list[Callable[[], object]] = [
            lambda: Plan(RUN_ID, 0, (A,), 0, 0, SECOND),
            lambda: Plan(RUN_ID, 1, (), 0, 0, SECOND),
            lambda: Plan(RUN_ID, 1, (A, A), 0, 0, SECOND),
            lambda: Plan(RUN_ID, 1, (A,), -1, 0, SECOND),
            lambda: Plan(RUN_ID, 1, (A,), 0, -1, SECOND),
            lambda: Plan(RUN_ID, 1, (A,), 0, 0, 0),
            lambda: Plan("replay", 1, (A,), 0, 0, SECOND),
            lambda: Plan(RUN_ID, 1, (CaseTimes(1, 0, 0, 0),), 0, 0, SECOND),
            lambda: Plan(RUN_ID, 1, (CaseTimes(1, 5, 4, 9),), 0, 0, SECOND),
            lambda: Plan(RUN_ID, 1, (CaseTimes(1, 1, 5, 4),), 0, 0, SECOND),
            lambda: Plan(RUN_ID, 1, (CaseTimes(1, -1, 5, 9),), 0, 0, SECOND),
        ]
        for build in bad:
            with self.subTest(), self.assertRaises(EventModelError):
                build()
