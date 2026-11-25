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
- The overlap is judged from the replay time that a message carries (the schedule again, in the payload) and not from the time at which the broker appended it.
- The ten-room run is accepted with final offsets for fewer than ten partitions, with no reading taken while the replay played, or with no final reading of the
  lag against the end offsets that were fixed when the replay ended.
- A reading of the lag that holds fewer than the ten partitions (the offsets tool listed some of them) counts as the reading taken while the replay played, sets
  the peak lag or the memory peak, or is dropped without a trace.
- An occurrence ID that another room or another run reuses shares the state of the first (the late event, the readings or the count of one leaks into the
  other), and the check looks at the occurrence ID only.
"""

from __future__ import annotations

import importlib
import importlib.util
import inspect
import json
import unittest
from types import ModuleType
from typing import Any
from unittest import mock

from scripts.process import CompletedProcess
from tests.e2e.run_record import ROOT
from tests.fast.runner_import import import_runner

BOUNDS = ROOT / "tests" / "e2e" / "stream_stack" / "read_bounds.py"

RUNNERS = (ROOT / "tests" / "e2e" / "stream_host.py", ROOT / "tests" / "e2e" / "stream_features.py")
SECOND = 1_000_000


def done(code: int = 0, text: str = "", error: str = "") -> CompletedProcess[str]:
    return CompletedProcess(["docker"], code, text, error)


@unittest.skipUnless(all(path.is_file() for path in RUNNERS), "the temporary runners are not in this checkout")
class RoomsCase(unittest.TestCase):
    def setUp(self) -> None:
        self.host: ModuleType = import_runner("stream_host")
        self.features: ModuleType = import_runner("stream_features")
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

    def test_ten_rooms_need_final_offsets_for_all_ten_partitions(self) -> None:
        ends = {partition: 7 for partition in range(10)}
        self.assertEqual([], self.host.final_offset_problems(dict(ends), ends, range(10)))
        nine = {partition: 7 for partition in range(9)}
        found = self.host.final_offset_problems(dict(nine), nine, range(10))
        self.assertTrue(any("partition 9" in item for item in found), found)
        found = self.host.final_offset_problems(dict(nine), nine)
        self.assertEqual([], found, "without a required set the old reading stands")

    def test_a_partition_that_holds_no_event_is_a_room_that_sent_nothing(self) -> None:
        ends = {partition: 7 for partition in range(10)}
        ends[4] = 0
        found = self.host.final_offset_problems(dict(ends), ends, range(10))
        self.assertTrue(any("partition 4" in item for item in found), found)

    def test_a_partition_that_the_job_did_not_report_is_named(self) -> None:
        ends = {partition: 7 for partition in range(10)}
        committed = {partition: 7 for partition in range(10) if partition != 6}
        found = self.host.final_offset_problems(committed, ends, range(10))
        self.assertTrue(any("partition 6" in item for item in found), found)


class Measurement(RoomsCase):
    TEN = tuple(range(10))

    def sample(self, seconds: float, lag: int = 1, usage: bool = True, partitions: tuple[int, ...] = TEN) -> Any:
        return self.host.Sample(seconds, {partition: lag if partition == 0 else 0 for partition in partitions}, {"stream": (1.0, 2.0)} if usage else {})

    def problems(self, samples: list[Any], window: tuple[float, float] = (10.0, 40.0), final: int = 5) -> list[str]:
        ends = {partition: 7 for partition in range(10)}
        committed = {partition: 7 for partition in range(10)}
        committed[0] = final
        found: list[str] = self.host.measurement_problems(samples, window, ends, committed, range(10))
        return found

    def test_a_reading_while_the_replay_played_and_a_drained_end_pass(self) -> None:
        self.assertEqual([], self.problems([self.sample(5.0), self.sample(20.0)], final=7))

    def test_no_reading_while_the_replay_played_is_a_problem(self) -> None:
        found = self.problems([self.sample(5.0), self.sample(55.0)], final=7)
        self.assertTrue(any("while the replay played" in item for item in found), found)

    def test_a_reading_with_no_usage_does_not_count_as_a_reading_of_the_run(self) -> None:
        found = self.problems([self.sample(20.0, usage=False)], final=7)
        self.assertTrue(any("while the replay played" in item for item in found), found)

    def test_a_reading_of_fewer_than_ten_partitions_does_not_count_as_a_reading_while_the_replay_played(self) -> None:
        for partitions in ((0, 1, 2), tuple(range(9)), tuple(range(11)), (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)):
            with self.subTest(partitions=partitions):
                found = self.problems([self.sample(20.0, partitions=partitions)], final=7)
                self.assertTrue(any("complete ten-partition" in item and "while the replay played" in item for item in found), found)

    def test_one_complete_reading_among_partial_ones_is_enough(self) -> None:
        samples = [self.sample(12.0, partitions=(0, 1)), self.sample(20.0), self.sample(30.0, partitions=(0,))]
        self.assertEqual([], self.problems(samples, final=7))

    def test_a_complete_reading_outside_the_replay_does_not_make_up_for_partial_ones_during_it(self) -> None:
        found = self.problems([self.sample(5.0), self.sample(20.0, partitions=(0, 1)), self.sample(55.0)], final=7)
        self.assertTrue(any("complete ten-partition" in item for item in found), found)

    def test_a_final_lag_against_the_fixed_end_offsets_must_be_zero_in_every_partition(self) -> None:
        found = self.problems([self.sample(20.0)], final=5)
        self.assertTrue(any("final lag" in item for item in found), found)

    def test_the_final_reading_is_a_sample_of_the_lag_against_the_fixed_ends(self) -> None:
        reading = self.host.final_reading(9.0, {0: 7, 1: 7}, {0: 5, 1: 7})
        self.assertEqual((9.0, {0: 2, 1: 0}, {}), (reading.seconds, reading.lag, reading.usage))

    def test_the_drain_time_is_the_final_reading_against_the_fixed_ends_when_the_live_readings_never_reached_zero(self) -> None:
        samples = [self.sample(1.0, lag=50), self.sample(6.0, lag=3)]
        samples.append(self.host.final_reading(9.0, {partition: 7 for partition in self.TEN}, {partition: 7 for partition in self.TEN}))
        summary = self.host.summarize(samples, self.TEN)
        self.assertEqual(9.0, summary["drained_at_seconds"])
        self.assertEqual(8.0, summary["drain_seconds_after_peak"])

    def test_a_partial_reading_sets_no_peak_and_no_drain_and_is_recorded_as_partial(self) -> None:
        huge = self.host.Sample(2.0, {0: 9000, 1: 9000}, {"stream": (99.0, 9999.0), "kafka": (99.0, 9999.0)})
        empty = self.host.Sample(7.0, {0: 0, 1: 0}, {"stream": (1.0, 1.0)})
        samples = [self.sample(1.0, lag=40), huge, self.sample(6.0, lag=10), empty, self.sample(11.0, lag=0)]
        summary = self.host.summarize(samples, self.TEN)
        self.assertEqual(3, summary["samples"])
        self.assertEqual(40, summary["peak_lag_events"])
        self.assertEqual(1.0, summary["peak_lag_at_seconds"])
        self.assertEqual(11.0, summary["drained_at_seconds"])
        self.assertEqual({"stream": 2.0}, summary["peak_memory_mib"])
        self.assertEqual({"stream": 1.0}, summary["peak_cpu_percent"])
        self.assertEqual([{"seconds": 2.0, "partitions": [0, 1]}, {"seconds": 7.0, "partitions": [0, 1]}], summary["partial_samples"])

    def test_a_summary_without_a_partial_reading_names_none(self) -> None:
        self.assertEqual([], self.host.summarize([self.sample(1.0)], self.TEN)["partial_samples"])

    def test_readings_that_are_all_partial_give_a_summary_with_no_peak_and_never_a_peak_of_the_partial_ones(self) -> None:
        summary = self.host.summarize([self.sample(1.0, lag=500, partitions=(0, 1))], self.TEN)
        self.assertEqual(0, summary["samples"])
        self.assertIsNone(summary["peak_lag_events"])
        self.assertIsNone(summary["drained_at_seconds"])
        self.assertEqual({}, summary["peak_memory_mib"])
        self.assertEqual(1, len(summary["partial_samples"]))

    def test_the_run_gives_the_summary_the_ten_partitions_of_the_rooms(self) -> None:
        self.assertTrue(
            "summarize(sampler.samples, range(ROOMS))" in inspect.getsource(self.host.ten_rooms), "ten_rooms does not give summarize its ten partitions"
        )


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

    def sampled(self, checkpoint: CompletedProcess[str]) -> Any:
        """One round of the ten-room sampler over the real stack reads: ten broker end offsets and the given answer of the checkpoint command."""
        stack = object.__new__(self.host.HostStack)
        stack.input_topic = "replay_events"
        stack.docker = lambda *arguments, timeout=60: checkpoint
        stack.end_offsets = lambda timeout=60: {partition: 7 for partition in range(10)}
        sampler = self.host.rooms_sampler(stack, "job", "rooms10", {"stream": "b" * 64})

        def usage() -> dict[str, tuple[float, float]]:
            sampler.stop.set()
            return {"stream": (1.0, 1.0)}

        sampler.usage = usage
        sampler.loop()
        return sampler

    def test_a_checkpoint_read_that_fails_is_a_failed_reading_and_never_a_zero_commit(self) -> None:
        sampler = self.sampled(done(1, "", "ls: no such file"))
        self.assertEqual(1, sampler.failures)
        self.assertEqual([], sampler.samples)

    def test_a_job_that_has_not_committed_yet_is_a_reading_with_the_whole_end_offset_behind(self) -> None:
        sampler = self.sampled(done(0, "NONE\n"))
        self.assertEqual(0, sampler.failures)
        self.assertEqual([{partition: 7 for partition in range(10)}], [sample.lag for sample in sampler.samples])

    def test_a_committed_batch_is_a_reading_of_the_lag_behind_it(self) -> None:
        offsets = {"replay_events": {str(partition): 5 for partition in range(10)}}
        sampler = self.sampled(done(0, json.dumps(offsets) + "\n"))
        self.assertEqual(0, sampler.failures)
        self.assertEqual([{partition: 2 for partition in range(10)}], [sample.lag for sample in sampler.samples])

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
            {"room": room, "occurrence_id": identity, "first_us": first * SECOND, "last_us": last * SECOND, "messages": 10, "clock": "log_append_time"}
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
            self.host.observed_overlap([{"room": 0, "occurrence_id": "1.0", "first_us": 0, "last_us": 0, "messages": 0, "clock": "log_append_time"}])

    def test_times_that_are_not_the_brokers_append_times_are_refused(self) -> None:
        for clock in (None, "replay_time", "create_time"):
            with self.subTest(clock=clock):
                rows = self.bounds((0, "1.0", 0, 100), (1, "2.0", 50, 150))
                for row in rows:
                    if clock is None:
                        del row["clock"]
                    else:
                        row["clock"] = clock
                with self.assertRaisesRegex(ValueError, "append"):
                    self.host.observed_overlap(rows)

    def test_the_observed_occurrences_must_be_the_manifests_occurrences(self) -> None:
        self.assertEqual([], self.host.bounds_problems(self.bounds((0, "1.0", 0, 9), (1, "2.0", 0, 9)), {"1.0": 0, "2.0": 1}))
        found = self.host.bounds_problems(self.bounds((0, "1.0", 0, 9)), {"1.0": 0, "2.0": 1})
        self.assertTrue(any("2.0" in item for item in found))
        found = self.host.bounds_problems(self.bounds((3, "1.0", 0, 9)), {"1.0": 0})
        self.assertTrue(any("room" in item for item in found))


def load_bounds() -> Any:
    spec = importlib.util.spec_from_file_location("read_bounds_probe", BOUNDS)
    if spec is None or spec.loader is None:
        raise ImportError(str(BOUNDS))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Received:
    def __init__(self, room: int, occurrence: str, replay_us: int, appended_ms: int | None) -> None:
        self.payload = {"room": room, "occurrence_id": occurrence, "replay_time_us": replay_us}
        self.appended_ms = appended_ms


@unittest.skipUnless(BOUNDS.is_file(), "the temporary probe is not in this checkout")
class BoundsProbe(unittest.TestCase):
    def test_the_bounds_are_the_brokers_append_times_and_not_the_replay_times_of_the_payloads(self) -> None:
        module = load_bounds()
        rows = module.bounds_of([Received(0, "1.0", 5_000_000, 1_000_000), Received(0, "1.0", 9_000_000, 1_000_300), Received(1, "2.0", 1, 1_000_100)])
        self.assertEqual(
            [
                {"room": 0, "occurrence_id": "1.0", "first_us": 1_000_000_000, "last_us": 1_000_300_000, "messages": 2, "clock": "log_append_time"},
                {"room": 1, "occurrence_id": "2.0", "first_us": 1_000_100_000, "last_us": 1_000_100_000, "messages": 1, "clock": "log_append_time"},
            ],
            rows,
        )

    def test_a_message_with_no_append_time_refuses_the_whole_answer(self) -> None:
        module = load_bounds()
        with self.assertRaisesRegex(ValueError, "append"):
            module.bounds_of([Received(0, "1.0", 5, 1_000_000), Received(0, "1.0", 6, None)])


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
