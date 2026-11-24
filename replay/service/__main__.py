"""The replay service: one program, several commands, each safe to run again.

``freeze``    verify the inputs, freeze the schedule and its first due replay instant on the state volume, and stop. Nothing is sent.
``run``       freeze the schedule when it is not yet frozen, then replay into Kafka from the saved position until every admitted occurrence is complete.
``manifest``  derive the manifest of expected events from the verified files and the frozen schedule, and write it.
``observe``   read every message of the topics and write the digest of each payload under its event ID.
``compare``   compare the observed file and the saved state with the manifest. Exit 0 only when every expected ID is there with its payload.
``formats``   register the Avro formats in the registry, which must already hold its global compatibility rule.
``serve``     the always-on service: wait for a run record in the inbox, run it as ``run`` does, keep it with its result and return to waiting.
``submit``    read a run record on standard input and put it in the inbox under its run ID, with the manifest reference file that ``--manifest-reference``
              names beside it, both as their exact bytes; the same record and reference again change nothing, other bytes under that run ID are
              refused (exit 2), as is a record or reference that is not valid UTF-8 text.
``health``    exit 0 when the service waits or runs and its health file is fresh: the container's health check.

Every command but the last three takes the run record (``--run-record``), which holds the settings of the run, the speed among them, and may take
``--inbox``: the inbox that a record from ``done/`` or ``failed/`` came from, so its relative ``state_dir`` and ``read_log`` are taken from the folder that
holds the resolved inbox, where the service ran it. Without ``--inbox`` the record is read as it is. A run that ``serve``
takes reads its manifest reference from beside its record in the inbox, not from the record's ``manifest_reference`` field. Exit codes: 0 done,
1 the check failed or the service is not healthy, 3 the gate refused an object version, 2 anything else that stopped the command.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Callable
from pathlib import Path
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
from replay.service.serve import Outcome, anchor_record, healthy, queued_record, serve, submit_command
from replay.service.state_file import ScheduleFile, StateFile, atomic_write
from replay.service.stopping import StopFlag, SystemClock
from replay.service.wire_codec import FormatMismatch, Reader, Writer
from scripts.ingestion.gate import GateRefused
from scripts.ingestion.manifest import ManifestError
from scripts.ingestion.store import StoreError

READ_DEADLINE_SECONDS = 600.0
INBOX = Path("/opt/replay/state/inbox")
HEALTH_FILE = Path("/opt/replay/state/health.json")
POLL_SECONDS = 1.0
STOPPED_CODE = 2
HEALTH_MAX_AGE_SECONDS = 600.0


def log(event: str, **fields: Any) -> None:
    sys.stderr.write(json.dumps({"event": event, **fields}, sort_keys=True) + "\n")


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


def replay_record(record: RunRecord, flag: StopFlag, beat: Callable[[], None]) -> int:
    """Replay one run record into Kafka from its saved position. ``beat`` is called as the run makes progress and ``flag`` ends every wait of the run when a
    stop is asked for. Return 0 when every occurrence is done and ``STOPPED_CODE`` when a stop ended it first.

    The stop flag is checked immediately before each start-up step (the Kafka lookup, the input verification, the freezing, the clinical table, the Kafka
    producer) and, inside the loop, before each event, after each wait and before each case is decoded. Limit: one operation that is already running when the
    stop arrives (a Kafka lookup, a verification, one case's decode) is not interrupted; if it outlasts the grace period of the container, the container is
    killed and that start counts as an attempt."""
    if flag.is_set():
        return STOPPED_CODE
    partitions = partition_count(record.kafka_bootstrap, INPUT_TOPIC)
    partition_of(record.rooms - 1, partitions)
    if flag.is_set():
        return STOPPED_CODE
    inputs = verify_inputs(record)
    beat()
    if flag.is_set():
        return STOPPED_CODE
    frozen = freeze_run(record, inputs)
    beat()
    if flag.is_set():
        return STOPPED_CODE
    times = inputs.times()
    if flag.is_set():
        return STOPPED_CODE
    writer = Writer(RegistryClient(record.registry_url))
    sink = KafkaSink(record.kafka_bootstrap, writer, partitions)
    prefetched = dict(frozen.decoded)

    def source(case_id: int) -> Any:
        recording = prefetched.pop(case_id) if case_id in prefetched else inputs.decode(times[case_id])
        beat()
        return recording

    def poll() -> None:
        sink.producer.poll(0)
        beat()

    def stop_requested() -> bool:
        beat()
        return flag.is_set()

    settings = RunSettings(record.speed, record.checkpoint_every, round(record.idle_checkpoint_seconds * 1_000_000))
    replayer = Replayer(frozen.schedule, settings, source, sink, StateFile(record.state_path), SystemClock(flag, poll), stop_requested)
    log("replay_start", run_id=record.run_id, speed=record.speed, schedule_sha256=frozen.schedule.sha256)
    result = replayer.run()
    log("replay_end", sent=result.sent, finished=result.finished)
    sys.stdout.write(json.dumps({"sent": result.sent, "finished": result.finished}) + "\n")
    return 0 if result.finished else STOPPED_CODE


def command_run(record: RunRecord, _: argparse.Namespace) -> int:
    flag = StopFlag()
    return replay_record(record, flag, lambda: None)


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


def run_request(path: Path, beat: Callable[[], None], flag: StopFlag) -> Outcome:
    """One request of the inbox as an outcome: a stop before the end leaves it to run again, and every error that stops a run fails it with its text."""
    log("request_start", request=path.stem)
    try:
        code = replay_record(queued_record(path), flag, beat)
        outcome = Outcome("finished", 0) if code == 0 else Outcome("stopped" if flag.is_set() else "failed", code)
    except GateRefused as refused:
        outcome = Outcome("failed", 3, str(refused))
    except STOPPING_ERRORS as error:
        outcome = Outcome("failed", 2, f"{type(error).__name__}: {error}")
    log("request_end", request=path.stem, outcome=outcome.kind, exit_code=outcome.exit_code, error=outcome.error)
    return outcome


def command_serve(arguments: argparse.Namespace) -> int:
    flag = StopFlag()
    log("serve_start", inbox=str(arguments.inbox))
    return serve(arguments.inbox, arguments.health, lambda path, beat: run_request(path, beat, flag), flag.is_set, time.time, flag.wait, POLL_SECONDS, log)


def command_submit(arguments: argparse.Namespace) -> int:
    return submit_command(arguments.inbox, arguments.manifest_reference, sys.stdin.buffer, sys.stdout, log)


def command_health(arguments: argparse.Namespace) -> int:
    return 0 if healthy(arguments.health, time.time(), arguments.max_age) else 1


COMMANDS = {
    "freeze": command_freeze,
    "run": command_run,
    "manifest": command_manifest,
    "observe": command_observe,
    "compare": command_compare,
    "formats": command_formats,
}


STOPPING_ERRORS = (
    OSError,
    ValueError,
    ArithmeticError,
    RuntimeError,
    ManifestError,
    StoreError,
    InputError,
    RunRecordError,
    RegistryError,
    FormatMismatch,
    KafkaStartError,
    DeliveryError,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("serve", "submit"):
        command = commands.add_parser(name)
        command.add_argument("--inbox", type=Path, default=INBOX)
        if name == "serve":
            command.add_argument("--health", type=Path, default=HEALTH_FILE)
        else:
            command.add_argument("--manifest-reference", type=Path, required=True, help="the file that holds the manifest reference of the run")
    health = commands.add_parser("health")
    health.add_argument("--health", type=Path, default=HEALTH_FILE)
    health.add_argument("--max-age", type=float, default=HEALTH_MAX_AGE_SECONDS, help="the age in seconds above which the health file is stale")
    for name in COMMANDS:
        command = commands.add_parser(name)
        command.add_argument("--run-record", type=Path, required=True)
        command.add_argument(
            "--inbox", type=Path, help="the inbox the record came from: its relative paths are taken from the folder that holds the resolved inbox"
        )
        if name == "manifest":
            command.add_argument("--out", type=Path, required=True)
        if name == "observe":
            command.add_argument("--out", type=Path, required=True)
        if name == "compare":
            command.add_argument("--manifest", type=Path, required=True)
            command.add_argument("--observed", type=Path, required=True)
            command.add_argument("--window-only", action="store_true")
    arguments = parser.parse_args(argv)
    service_commands = {"serve": command_serve, "submit": command_submit, "health": command_health}
    if arguments.command in service_commands:
        return service_commands[arguments.command](arguments)
    try:
        record = parse_run_record(arguments.run_record.read_text())
        if arguments.inbox is not None:
            record = anchor_record(record, arguments.inbox)
        return COMMANDS[arguments.command](record, arguments)
    except GateRefused as refused:
        problems = [{"position": problem.position, "requested": problem.requested, "cause": problem.cause} for problem in refused.problems]
        log("gate_refused", reason=str(refused), problems=problems)
        return 3
    except STOPPING_ERRORS as error:
        log("stopped", error=type(error).__name__, message=str(error))
        return 2


if __name__ == "__main__":
    sys.exit(main())
