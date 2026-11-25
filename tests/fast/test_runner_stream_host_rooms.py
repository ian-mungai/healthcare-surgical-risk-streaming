"""The ten-room measurement, the overlap of rooms and the isolation of a reused occurrence ID of the host batch.

The runners are untracked, so the tests run only where they are on disk. Nothing starts: stand-ins answer every command.

Failure modes covered here:

- The lag is judged from samples only, so a job that was behind when the replay ended is not noticed: the final committed offsets of the measured job are not
  compared with the final end offsets.
- The samples name containers by a pattern of their names, so a container of another run or of the platform with a like name is counted, or the sampled
  container is a different one from the one measured.
- Ten rooms are compared by their count only, so two rooms in one partition, or a room in another partition, pass.
- A storage reading that failed (the ``du`` exit status, an empty answer) is taken as zero bytes.
- The overlap of rooms is read from the schedule and not from what the replay sent, or an occurrence with no observed end is taken as not overlapping.
- An occurrence ID that another room or another run reuses shares the state of the first (the late event, the readings or the count of one leaks into the
  other), and the check looks at the occurrence ID only.
"""

from __future__ import annotations

import importlib
import json
import unittest
from types import ModuleType
from typing import Any
from unittest import mock

from scripts.process import CompletedProcess
from tests.e2e.run_record import ROOT

RUNNERS = (ROOT / "tests" / "e2e" / "stream_host.py", ROOT / "tests" / "e2e" / "stream_features.py")
SECOND = 1_000_000


def done(code: int = 0, text: str = "", error: str = "") -> CompletedProcess[str]:
    return CompletedProcess(["docker"], code, text, error)


@unittest.skipUnless(all(path.is_file() for path in RUNNERS), "the temporary runners are not in this checkout")
class RoomsCase(unittest.TestCase):
    def setUp(self) -> None:
        self.host: ModuleType = importlib.import_module("tests.e2e.stream_host")
        self.features: ModuleType = importlib.import_module("tests.e2e.stream_features")
        self.calls: list[tuple[str, ...]] = []


class RoomPartitions(RoomsCase):
    def full(self) -> dict[str, list[int]]:
        return {str(room): [room] for room in range(10)}

    def test_the_full_map_of_ten_rooms_to_their_own_partitions_passes(self) -> None:
        self.assertEqual([], self.host.room_map_problems(self.full(), rooms=10, partitions=10))

    def test_two_rooms_in_one_partition_fail_even_when_the_count_is_ten(self) -> None:
        wrong = self.full()
        wrong["9"] = [8]
        found = self.host.room_map_problems(wrong, rooms=10, partitions=10)
        self.assertTrue(any("room 9" in item for item in found))

    def test_a_missing_room_and_an_extra_room_are_named(self) -> None:
        wrong = self.full()
        del wrong["4"]
        wrong["10"] = [0]
        found = " ".join(self.host.room_map_problems(wrong, rooms=10, partitions=10))
        self.assertIn("room 4", found)
        self.assertIn("room 10", found)

    def test_a_room_in_two_partitions_is_named(self) -> None:
        wrong = self.full()
        wrong["3"] = [3, 4]
        self.assertTrue(self.host.room_map_problems(wrong, rooms=10, partitions=10))


class FinalOffsets(RoomsCase):
    def test_the_final_committed_offsets_must_equal_the_final_end_offsets(self) -> None:
        self.assertEqual([], self.host.final_offset_problems({0: 5, 1: 7}, {0: 5, 1: 7}))
        found = self.host.final_offset_problems({0: 5, 1: 6}, {0: 5, 1: 7})
        self.assertEqual(1, len(found))
        self.assertIn("partition 1", found[0])

    def test_a_partition_with_no_commit_is_behind_by_its_whole_end_offset(self) -> None:
        self.assertTrue(self.host.final_offset_problems({}, {0: 3}))

    def test_a_replay_that_sent_nothing_proves_nothing(self) -> None:
        self.assertTrue(self.host.final_offset_problems({}, {}))


class Sampling(RoomsCase):
    def stats(self, rows: list[dict[str, str]]) -> str:
        return "\n".join(json.dumps(row) for row in rows)

    def test_the_sample_names_the_exact_container_ids_and_reads_nothing_else(self) -> None:
        text = self.stats(
            [
                {"ID": "aaaaaaaaaaaa", "Name": "x", "CPUPerc": "10.00%", "MemUsage": "512MiB / 1GiB"},
                {"ID": "bbbbbbbbbbbb", "Name": "y", "CPUPerc": "150.00%", "MemUsage": "2GiB / 4GiB"},
                {"ID": "cccccccccccc", "Name": "surgical_risk-kafka-1", "CPUPerc": "99.00%", "MemUsage": "9GiB / 9GiB"},
            ]
        )

        def runner(program: str, args: list[str], **options: Any) -> CompletedProcess[str]:
            self.calls.append(tuple(args))
            return done(0, text)

        sampler = self.host.Sampler(lambda: {}, lambda: {}, {"kafka": "aaaaaaaaaaaa" + "0" * 52, "stream": "bbbbbbbbbbbb" + "1" * 52})
        with mock.patch.object(self.host, "run_command", runner):
            usage = sampler.usage()
        self.assertEqual({"kafka": (10.0, 512.0), "stream": (150.0, 2048.0)}, usage)
        command = self.calls[0]
        self.assertEqual(("-n", "docker", "stats", "--no-stream", "--format", "{{json .}}"), command[:6])
        self.assertEqual({"aaaaaaaaaaaa" + "0" * 52, "bbbbbbbbbbbb" + "1" * 52}, set(command[6:]))

    def test_a_container_that_is_not_in_the_map_is_not_counted_even_when_its_name_looks_like_one(self) -> None:
        text = self.stats([{"ID": "dddddddddddd", "Name": "p8-host-ffffffff-stream-1", "CPUPerc": "50.00%", "MemUsage": "1GiB / 4GiB"}])
        sampler = self.host.Sampler(lambda: {}, lambda: {}, {"stream": "bbbbbbbbbbbb" + "1" * 52})
        with mock.patch.object(self.host, "run_command", lambda program, args, **options: done(0, text)):
            self.assertEqual({}, sampler.usage())

    def test_a_stats_command_that_fails_is_a_failed_reading(self) -> None:
        sampler = self.host.Sampler(lambda: {}, lambda: {}, {"stream": "bbbbbbbbbbbb" + "1" * 52})
        with mock.patch.object(self.host, "run_command", lambda program, args, **options: done(1)), self.assertRaises(RuntimeError):
            sampler.usage()

    def test_a_line_names_its_container_by_id(self) -> None:
        line = json.dumps({"ID": "abc123abc123", "Name": "n", "CPUPerc": "1.5%", "MemUsage": "100MiB / 1GiB"})
        self.assertEqual(("abc123abc123", 1.5, 100.0), self.host.parse_stats_id_line(line))
        self.assertIsNone(self.host.parse_stats_id_line("not json"))


class Storage(RoomsCase):
    class Stack:
        def __init__(self, answer: CompletedProcess[str]) -> None:
            self.answer = answer

        def docker(self, *args: str, timeout: float = 60) -> CompletedProcess[str]:
            return self.answer

    def test_the_sizes_are_read_when_the_command_succeeds(self) -> None:
        data = "/var/lib/kafka/data"
        text = f"100\t{data}/replay_events_p8h_x_rooms10-0\n50\t{data}/replay_events_p8h_x_rooms10-1\n"
        sizes = self.host.kafka_sizes(self.Stack(done(0, text)), "abc")
        self.assertEqual({"replay_events_p8h_x_rooms10": 150 * 1024}, sizes)

    def test_a_failed_read_blocks_the_run_and_is_never_zero_bytes(self) -> None:
        for answer in (done(1, "100\tx-0\n", "denied"), done(0, ""), done(0, "no digits here")):
            with self.subTest(answer=answer), self.assertRaises(self.host.Blocked):
                self.host.kafka_sizes(self.Stack(answer), "abc")


class Overlap(RoomsCase):
    def bounds(self, *rows: tuple[int, str, int, int]) -> list[dict[str, Any]]:
        return [
            {"room": room, "occurrence_id": identity, "first_us": first * SECOND, "last_us": last * SECOND, "messages": 10}
            for room, identity, first, last in rows
        ]

    def test_a_room_that_starts_while_another_is_active_is_an_overlap_by_what_was_sent(self) -> None:
        found, detail = self.host.observed_overlap(self.bounds((0, "1.0", 0, 100), (1, "2.0", 50, 150)))
        self.assertTrue(found, detail)
        self.assertIn("2.0", detail)

    def test_rooms_that_never_run_together_are_not_an_overlap(self) -> None:
        found, _ = self.host.observed_overlap(self.bounds((0, "1.0", 0, 100), (1, "2.0", 100, 150), (0, "3.0", 200, 300)))
        self.assertFalse(found)

    def test_a_start_inside_the_same_rooms_own_case_is_not_an_overlap_of_rooms(self) -> None:
        found, _ = self.host.observed_overlap(self.bounds((0, "1.0", 0, 100), (0, "2.0", 50, 150)))
        self.assertFalse(found)

    def test_an_occurrence_with_no_observed_message_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            self.host.observed_overlap([{"room": 0, "occurrence_id": "1.0", "first_us": 0, "last_us": 0, "messages": 0}])

    def test_the_observed_occurrences_must_be_the_manifests_occurrences(self) -> None:
        self.assertEqual([], self.host.bounds_problems(self.bounds((0, "1.0", 0, 9), (1, "2.0", 0, 9)), {"1.0": 0, "2.0": 1}))
        found = self.host.bounds_problems(self.bounds((0, "1.0", 0, 9)), {"1.0": 0, "2.0": 1})
        self.assertTrue(any("2.0" in item for item in found))
        found = self.host.bounds_problems(self.bounds((3, "1.0", 0, 9)), {"1.0": 0})
        self.assertTrue(any("room" in item for item in found))


class Isolation(RoomsCase):
    KEYS: list[dict[str, Any]] = [
        {"run_id": "r1", "room": 0, "occurrence_id": "9101.0", "base": 60.0, "late": True},
        {"run_id": "r1", "room": 0, "occurrence_id": "9102.1", "base": 150.0, "late": False},
        {"run_id": "r1", "room": 2, "occurrence_id": "9101.0", "base": 210.0, "late": False},
        {"run_id": "r2", "room": 0, "occurrence_id": "9101.0", "base": 270.0, "late": False},
    ]

    def rows(self) -> dict[tuple[str, int, str, int], dict[str, Any]]:
        rows: dict[tuple[str, int, str, int], dict[str, Any]] = {}
        for key in self.KEYS:
            at120 = 61 if key["late"] else 60
            rows[(key["run_id"], key["room"], key["occurrence_id"], 60 * SECOND)] = {"hr_valid_count": 30, "hr_min": key["base"]}
            rows[(key["run_id"], key["room"], key["occurrence_id"], 120 * SECOND)] = {"hr_valid_count": at120, "hr_min": key["base"]}
        return rows

    def test_every_key_holds_only_its_own_readings(self) -> None:
        self.assertEqual([], self.features.isolation_problems(self.rows(), self.KEYS))

    def test_a_reused_id_that_took_the_late_event_of_another_key_is_named_with_its_full_key(self) -> None:
        rows = self.rows()
        rows[("r1", 2, "9101.0", 120 * SECOND)]["hr_valid_count"] = 61
        found = self.features.isolation_problems(rows, self.KEYS)
        self.assertEqual(1, len(found))
        self.assertIn("r1", found[0])
        self.assertIn("room 2", found[0])

    def test_a_key_that_took_readings_of_another_key_with_the_same_id_is_named(self) -> None:
        rows = self.rows()
        rows[("r2", 0, "9101.0", 60 * SECOND)]["hr_valid_count"] = 60
        self.assertTrue(self.features.isolation_problems(rows, self.KEYS))
        rows = self.rows()
        rows[("r2", 0, "9101.0", 60 * SECOND)]["hr_min"] = 60.0
        self.assertTrue(self.features.isolation_problems(rows, self.KEYS))

    def test_a_missing_row_of_a_key_is_named(self) -> None:
        rows = self.rows()
        del rows[("r1", 2, "9101.0", 60 * SECOND)]
        self.assertTrue(self.features.isolation_problems(rows, self.KEYS))

    def test_the_plant_script_reuses_an_occurrence_id_across_a_room_and_across_a_run(self) -> None:
        source = (ROOT / "tests" / "e2e" / "stream_stack" / "plant_late.py").read_text()
        self.assertIn("RUN_TWO", source)
        self.assertIn("Occurrence(0, 9101, 0, 2)", source)


if __name__ == "__main__":
    unittest.main()
