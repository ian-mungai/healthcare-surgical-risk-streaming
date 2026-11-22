"""The replay service: one program, several commands, each safe to run again.

``freeze``    verify the inputs, freeze the schedule and its first due replay instant on the state volume, and stop. Nothing is sent.
``run``       freeze the schedule when it is not yet frozen, then replay into Kafka from the saved position until every admitted occurrence is complete.
``manifest``  derive the manifest of expected events from the verified files and the frozen schedule, and write it.
``observe``   read every message of the topics and write the digest of each payload under its event ID.
``compare``   compare the observed file and the saved state with the manifest. Exit 0 only when every expected ID is there with its payload.
``formats``   register the Avro formats in the registry, which must already hold its global compatibility rule.

Every command takes the run record (``--run-record``), which holds the settings of the run, the speed among them. Exit codes: 0 done, 1 the check failed,
3 the gate refused an object version, 2 anything else that stopped the command.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from pathlib import Path
from types import FrameType
from typing import Any

from replay.events.formats import COMPATIBILITY_RULE, FORMATS, INPUT_TOPIC, REGISTRY_GROUP, schema_text
from replay.events.identity import partition_of
from replay.events.manifest import Manifest, build_manifest, compare
from replay.events.run import Replayer, RunSettings
from replay.events.schedule import FrozenSchedule, ScheduleError, admitted_occurrences, first_due_instant, freeze
from replay.events.stream import earliest_due_offset_us
from replay.service.check import observed_document, observed_from, report_document
from replay.service.inputs import InputError, VerifiedInputs, plan_of, verify_inputs
from replay.service.kafka_io import DeliveryError, KafkaSink, KafkaStartError, partition_count, read_all
from replay.service.registry import RegistryClient, RegistryError
from replay.service.run_record import RunRecord, RunRecordError, parse_run_record
from replay.service.state_file import ScheduleFile, StateFile, atomic_write
from replay.service.wire_codec import FormatMismatch, Reader, Writer
from scripts.ingestion.gate import GateRefused
from scripts.ingestion.manifest import ManifestError
from scripts.ingestion.store import StoreError

READ_DEADLINE_SECONDS = 600.0


def log(event: str, **fields: Any) -> None:
    sys.stderr.write(json.dumps({"event": event, **fields}, sort_keys=True) + "\n")


class SystemClock:
    """The wall clock in microseconds. A wait is cut into slices of at most a second, so the process stays responsive and every wait ends at its target."""

    def __init__(self, poll: Any = None) -> None:
        self.poll = poll

    def now_us(self) -> int:
        return time.time_ns() // 1000

    def wait_until(self, wall_us: int) -> None:
        while (remaining := wall_us - self.now_us()) > 0:
            time.sleep(min(remaining / 1_000_000, 1.0))
            if self.poll is not None:
                self.poll()


class Frozen:
    """The frozen schedule of a run, with the first-occurrence recordings that freezing already decoded, so the run does not decode them twice."""

    def __init__(self, schedule: FrozenSchedule, decoded: dict[int, Any]) -> None:
        self.schedule = schedule
        self.decoded = decoded


def freeze_run(record: RunRecord, inputs: VerifiedInputs) -> Frozen:
    """Load the frozen schedule, or freeze one now from the verified inputs. A saved schedule that differs from the run record's plan is refused."""
    times = inputs.times()
    plan = plan_of(record, times)
    schedule_file = ScheduleFile(record.schedule_path)
    saved = schedule_file.load()
    if saved is not None:
        if saved.plan != plan:
            raise ScheduleError("the run record's plan differs from the frozen schedule on the volume; start a new run ID or restore the record")
        return Frozen(saved, {})
    decoded = {}
    first_cases = {plan.cases[room % len(plan.cases)].case_id for room in range(plan.rooms)}
    for case in sorted(first_cases):
        decoded[case] = inputs.decode(times[case])
    schedule = freeze(plan, first_due_instant(plan, lambda case: earliest_due_offset_us(decoded[case])))
    schedule_file.freeze(schedule)
    log(
        "frozen",
        schedule_sha256=schedule.sha256,
        first_due_us=schedule.first_due_us,
        window_end_us=schedule.window_end_us,
        occurrences=len(admitted_occurrences(schedule)),
    )
    return Frozen(schedule, decoded)


def command_freeze(record: RunRecord, _: argparse.Namespace) -> int:
    frozen = freeze_run(record, verify_inputs(record))
    sys.stdout.write(
        json.dumps({"schedule_sha256": frozen.schedule.sha256, "first_due_us": frozen.schedule.first_due_us, "window_end_us": frozen.schedule.window_end_us})
        + "\n"
    )
    return 0


def command_run(record: RunRecord, _: argparse.Namespace) -> int:
    partitions = partition_count(record.kafka_bootstrap, INPUT_TOPIC)
    partition_of(record.rooms - 1, partitions)
    inputs = verify_inputs(record)
    frozen = freeze_run(record, inputs)
    times = inputs.times()
    writer = Writer(RegistryClient(record.registry_url))
    sink = KafkaSink(record.kafka_bootstrap, writer, partitions)
    stopped = False

    def request_stop(signum: int, frame: FrameType | None) -> None:
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    prefetched = dict(frozen.decoded)

    def source(case_id: int) -> Any:
        return prefetched.pop(case_id) if case_id in prefetched else inputs.decode(times[case_id])

    settings = RunSettings(record.speed, record.checkpoint_every, round(record.idle_checkpoint_seconds * 1_000_000))
    replayer = Replayer(frozen.schedule, settings, source, sink, StateFile(record.state_path), SystemClock(lambda: sink.producer.poll(0)), lambda: stopped)
    log("replay_start", run_id=record.run_id, speed=record.speed, schedule_sha256=frozen.schedule.sha256)
    result = replayer.run()
    log("replay_end", sent=result.sent, finished=result.finished)
    sys.stdout.write(json.dumps({"sent": result.sent, "finished": result.finished}) + "\n")
    return 0 if result.finished else 2


def command_manifest(record: RunRecord, arguments: argparse.Namespace) -> int:
    schedule = ScheduleFile(record.schedule_path).load()
    if schedule is None:
        raise ScheduleError("no schedule is frozen on the volume: run freeze or run first")
    inputs = verify_inputs(record)
    times = inputs.times()
    if schedule.plan != plan_of(record, times):
        raise ScheduleError("the run record's plan differs from the frozen schedule on the volume")
    recordings = {case: inputs.decode(times[case]) for case in sorted({item.occurrence.case_id for item in admitted_occurrences(schedule)})}
    manifest = build_manifest(schedule, recordings)
    atomic_write(Path(arguments.out), manifest.to_json())
    sys.stdout.write(json.dumps({"events": len(manifest.events), "occurrences": len(manifest.occurrences), "schedule_sha256": manifest.schedule_sha256}) + "\n")
    return 0


def command_observe(record: RunRecord, arguments: argparse.Namespace) -> int:
    reader = Reader(RegistryClient(record.registry_url))
    received = read_all(record.kafka_bootstrap, reader, [INPUT_TOPIC], READ_DEADLINE_SECONDS)
    atomic_write(Path(arguments.out), observed_document(received, record.run_id))
    sys.stdout.write(json.dumps({"messages_read": len(received)}) + "\n")
    return 0


def command_compare(record: RunRecord, arguments: argparse.Namespace) -> int:
    manifest = Manifest.from_json(Path(arguments.manifest).read_text())
    state = StateFile(record.state_path).load()
    report = compare(manifest, observed_from(Path(arguments.observed).read_text(), state), window_only=arguments.window_only)
    sys.stdout.write(report_document(manifest, report, window_only=arguments.window_only))
    return 0 if report.ok else 1


def command_formats(record: RunRecord, _: argparse.Namespace) -> int:
    registry = RegistryClient(record.registry_url)
    held = registry.global_rule()
    if held != COMPATIBILITY_RULE:
        raise RegistryError(f"the registry's global compatibility rule is {held}, not {COMPATIBILITY_RULE}: the stack's registry step sets it first")
    registered = {key: registry.register(REGISTRY_GROUP, format_.subject, schema_text(key)) for key, format_ in FORMATS.items()}
    sys.stdout.write(
        json.dumps(
            {key: {"subject": item.artifact_id, "version": item.version, "content_id": item.content_id} for key, item in registered.items()}, sort_keys=True
        )
        + "\n"
    )
    return 0


COMMANDS = {
    "freeze": command_freeze,
    "run": command_run,
    "manifest": command_manifest,
    "observe": command_observe,
    "compare": command_compare,
    "formats": command_formats,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    for name in COMMANDS:
        command = commands.add_parser(name)
        command.add_argument("--run-record", type=Path, required=True)
        if name == "manifest":
            command.add_argument("--out", type=Path, required=True)
        if name == "observe":
            command.add_argument("--out", type=Path, required=True)
        if name == "compare":
            command.add_argument("--manifest", type=Path, required=True)
            command.add_argument("--observed", type=Path, required=True)
            command.add_argument("--window-only", action="store_true")
    arguments = parser.parse_args(argv)
    try:
        record = parse_run_record(arguments.run_record.read_text())
        return COMMANDS[arguments.command](record, arguments)
    except GateRefused as refused:
        problems = [{"position": problem.position, "requested": problem.requested, "cause": problem.cause} for problem in refused.problems]
        log("gate_refused", reason=str(refused), problems=problems)
        return 3
    except (
        OSError,
        ValueError,
        RuntimeError,
        ManifestError,
        StoreError,
        InputError,
        RunRecordError,
        RegistryError,
        FormatMismatch,
        KafkaStartError,
        DeliveryError,
    ) as error:
        log("stopped", error=type(error).__name__, message=str(error))
        return 2


if __name__ == "__main__":
    sys.exit(main())
