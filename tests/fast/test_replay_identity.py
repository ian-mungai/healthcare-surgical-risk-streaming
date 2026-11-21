"""The run, the case occurrence, the room and the event ID: the identities that the replay, the streaming job and the batch feature path share.

Failure modes covered here:

- The same reading gets another ID on another derivation, so a replay that is sent again is not recognised and consumers keep both copies.
- Two readings that share a track and a case time get one ID, so one of them is lost when a consumer drops repeats.
- A component of the ID is left out, so two rooms, two occurrences of one case or two runs share an ID.
- A generated scoring marker can take the ID of a source event, or the reverse.
- A delimiter inside a component makes two different readings produce one text.
- The room of an occurrence changes between derivations, so one room's events leave its partition.
"""

from __future__ import annotations

import unittest
from collections.abc import Callable

from replay.events.identity import (
    CASE_END_TRACK,
    FINAL_SCORE_TRACK,
    IdentityError,
    Occurrence,
    check_run_id,
    marker_event_id,
    occurrence_for,
    partition_of,
    room_of,
    run_id,
    source_event_id,
)

RUN = "replay_0a1b2c3d"


class RunIdentity(unittest.TestCase):
    def test_a_run_id_is_a_name_and_eight_hex_digits_derived_from_its_seed(self) -> None:
        identity = run_id("replay", "schedule of one day")
        self.assertRegex(identity, r"replay_[0-9a-f]{8}")
        self.assertEqual(identity, run_id("replay", "schedule of one day"))
        self.assertNotEqual(identity, run_id("replay", "another schedule"))

    def test_a_later_attempt_keeps_the_hex_and_adds_its_number(self) -> None:
        self.assertEqual(f"{RUN}_2", check_run_id(f"{RUN}_2"))
        self.assertEqual(f"{RUN}_12", check_run_id(f"{RUN}_12"))

    def test_a_text_that_is_not_a_run_id_is_refused(self) -> None:
        for text in (
            "",
            "replay",
            "Replay_0a1b2c3d",
            "replay_0A1B2C3D",
            "replay_0a1b2c3",
            "replay_0a1b2c3d_1",
            "replay_0a1b2c3d_0",
            "replay-0a1b2c3d",
            "r|eplay_0a1b2c3d",
            "2026-11-21_0a1b2c3d",
        ):
            with self.subTest(text=text), self.assertRaises(IdentityError):
                check_run_id(text)


class OccurrenceAndRoom(unittest.TestCase):
    def test_the_occurrences_follow_the_cases_in_turn_and_the_rooms_in_turn(self) -> None:
        cases = (908, 565, 1147)
        got = [occurrence_for(sequence, cases, 2) for sequence in range(7)]
        self.assertEqual(
            [
                Occurrence(0, 908, 0, 0),
                Occurrence(1, 565, 0, 1),
                Occurrence(2, 1147, 0, 0),
                Occurrence(3, 908, 1, 1),
                Occurrence(4, 565, 1, 0),
                Occurrence(5, 1147, 1, 1),
                Occurrence(6, 908, 2, 0),
            ],
            got,
        )
        self.assertEqual(["908.0", "565.0", "1147.0", "908.1", "565.1", "1147.1", "908.2"], [occurrence.id for occurrence in got])

    def test_every_occurrence_of_a_run_has_its_own_id(self) -> None:
        ids = [occurrence_for(sequence, (908, 565), 3).id for sequence in range(200)]
        self.assertEqual(len(ids), len(set(ids)))

    def test_the_room_of_an_occurrence_depends_on_its_place_in_the_play_order_only(self) -> None:
        self.assertEqual([0, 1, 2, 0, 1, 2, 0], [room_of(sequence, 3) for sequence in range(7)])
        self.assertEqual(0, room_of(5, 1))

    def test_a_room_count_or_a_sequence_that_cannot_exist_is_refused(self) -> None:
        calls: list[Callable[[], object]] = [
            lambda: room_of(0, 0),
            lambda: room_of(-1, 2),
            lambda: occurrence_for(0, (), 2),
            lambda: occurrence_for(0, (1, 1), 2),
        ]
        for call in calls:
            with self.subTest(), self.assertRaises(IdentityError):
                call()


class Partition(unittest.TestCase):
    def test_a_room_is_sent_to_the_partition_of_its_own_number(self) -> None:
        self.assertEqual([0, 1, 2, 9], [partition_of(room, 10) for room in (0, 1, 2, 9)])

    def test_a_room_count_above_the_partition_count_is_refused_and_so_is_a_room_that_does_not_fit(self) -> None:
        for room, partitions in ((10, 10), (3, 2), (-1, 10), (0, 0)):
            with self.subTest(room=room, partitions=partitions), self.assertRaises(IdentityError):
                partition_of(room, partitions)


class EventIdentity(unittest.TestCase):
    def test_the_id_is_run_room_occurrence_track_case_time_and_discriminator(self) -> None:
        self.assertEqual(f"{RUN}|3|908.1|Solar8000/HR|78208000|0", source_event_id(RUN, 3, "908.1", "Solar8000/HR", 78_208_000, 0))

    def test_the_same_reading_always_gets_the_same_id(self) -> None:
        first = source_event_id(RUN, 3, "908.1", "Solar8000/HR", 78_208_000, 0)
        self.assertEqual(first, source_event_id(RUN, 3, "908.1", "Solar8000/HR", 78_208_000, 0))

    def test_each_component_changes_the_id(self) -> None:
        base = (RUN, 3, "908.1", "Solar8000/HR", 78_208_000, 0)
        reference = source_event_id(*base)
        variants = [
            ("replay_ffffffff", 3, "908.1", "Solar8000/HR", 78_208_000, 0),
            (RUN, 4, "908.1", "Solar8000/HR", 78_208_000, 0),
            (RUN, 3, "908.2", "Solar8000/HR", 78_208_000, 0),
            (RUN, 3, "908.1", "Solar8000/RR", 78_208_000, 0),
            (RUN, 3, "908.1", "Solar8000/HR", 78_208_001, 0),
            (RUN, 3, "908.1", "Solar8000/HR", 78_208_000, 1),
        ]
        self.assertEqual(len(variants), len({source_event_id(*variant) for variant in variants} - {reference}))

    def test_two_readings_that_share_a_track_and_a_case_time_differ_by_their_discriminator(self) -> None:
        self.assertNotEqual(source_event_id(RUN, 0, "9001.0", "Test/HR", 1_500_000, 0), source_event_id(RUN, 0, "9001.0", "Test/HR", 1_500_000, 1))

    def test_a_case_time_before_the_case_start_is_a_valid_part_of_an_id(self) -> None:
        self.assertEqual(f"{RUN}|0|9001.0|Test/HR|-250000|0", source_event_id(RUN, 0, "9001.0", "Test/HR", -250_000, 0))

    def test_a_marker_id_names_its_reserved_track_and_never_equals_a_source_id(self) -> None:
        final = marker_event_id(RUN, 3, "908.1", "final_score", 2_965_000_000)
        ended = marker_event_id(RUN, 3, "908.1", "case_end", 4_207_000_000)
        self.assertEqual(f"{RUN}|3|908.1|{FINAL_SCORE_TRACK}|2965000000|0", final)
        self.assertEqual(f"{RUN}|3|908.1|{CASE_END_TRACK}|4207000000|0", ended)
        self.assertNotEqual(final, ended)
        for track in (FINAL_SCORE_TRACK, CASE_END_TRACK, "!anything"):
            with self.subTest(track=track), self.assertRaises(IdentityError):
                source_event_id(RUN, 3, "908.1", track, 2_965_000_000, 0)

    def test_a_delimiter_inside_a_component_is_refused(self) -> None:
        calls: list[Callable[[], object]] = [
            lambda: source_event_id(RUN, 0, "9001.0", "Test|HR", 0, 0),
            lambda: source_event_id(RUN, 0, "9001|0", "Test/HR", 0, 0),
            lambda: source_event_id("replay|0a1b2c3d", 0, "9001.0", "Test/HR", 0, 0),
            lambda: source_event_id(RUN, 0, "9001.0", "", 0, 0),
            lambda: source_event_id(RUN, -1, "9001.0", "Test/HR", 0, 0),
            lambda: source_event_id(RUN, 0, "9001.0", "Test/HR", 0, -1),
        ]
        for call in calls:
            with self.subTest(), self.assertRaises(IdentityError):
                call()

    def test_a_track_with_a_control_character_is_refused(self) -> None:
        for track in ("Test\nHR", "Test\tHR", "Test\x00HR"):
            with self.subTest(track=repr(track)), self.assertRaises(IdentityError):
                source_event_id(RUN, 0, "9001.0", track, 0, 0)
