"""The staging sampler and the peak of its file: how much disk the copy's pods held on the node, and the lowest free space.

Airbyte stages each file in the ephemeral storage of the pod that copies it, on the node's disk. The kubelet reports that use for every pod and the free space
of the node's file system in its summary document. ``sample`` reads that document through kubectl at each interval, from before the sync starts until after it
ends, and writes one JSON line for each reading. ``peak`` reads the file and writes the figures the copy probe saves.

``.venv/bin/python -m scripts.ingestion.staging_peak sample --output samples.jsonl --node NODE --interval-seconds 5 --max-seconds 1800 -- k3s kubectl``

``.venv/bin/python -m scripts.ingestion.staging_peak peak --samples samples.jsonl --output peak.json``

The words after ``--`` are the kubectl command, so the sampler runs where kubectl runs or through ``ssh``. The sampler starts nothing and stops when it
is signalled or at ``--max-seconds``. A sample whose reading fails names the failure and holds no reading. ``peak`` refuses a file that was cut off (no end
line, or an end line that counts other samples), a file with no sample and a file with a failed reading, because a missing sample could have held the peak.
The peak is the largest total of the namespace's pods in one sample. The increase is that total less the first sample's, which is what the pods already held
before the copy.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from scripts.ingestion.cli import read_input, say, write_private
from scripts.ingestion.closed import QuietParser
from scripts.ingestion.notice import ClosedError, Code, Notice, Subject, describe_error, render
from scripts.ingestion.output_file import check_new_output
from scripts.process import run_command
from scripts.stack.capacity_sampler import (
    DEFAULT_INTERVAL_SECONDS,
    DEFAULT_RUN_SECONDS,
    MAX_INTERVAL_SECONDS,
    MAX_RUN_SECONDS,
    Clock,
    Failure,
    Output,
    Runner,
    SamplerError,
    StopFlag,
    emit,
    timed,
)

PROGRAM = "scripts.ingestion.staging_peak"
SCHEMA = 1
PEAK_FORMAT = "staging_peak_1"
NODE_NAME = re.compile(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?")
NAMESPACE_NAME = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
SUMMARY_PATH = "/api/v1/nodes/{node}/proxy/stats/summary"


class StagingRefused(ClosedError, RuntimeError):
    """The staging samples cannot give the peak, or an input of the command cannot be read."""


def whole(value: object) -> bool:
    return type(value) is int and value >= 0


def summary_body(runner: Runner, kubectl: Sequence[str], node: str, namespace: str, deadline: float, clock: Clock) -> dict[str, Any]:
    """The pods of ``namespace`` with their ephemeral storage in bytes, and the free and total bytes of the node's file system, from one summary document."""
    left = deadline - clock()
    if left <= 0:
        raise Failure("timeout")
    result = runner(kubectl[0], [*kubectl[1:], "get", "--raw", SUMMARY_PATH.format(node=node)], timeout=left)
    if result.returncode != 0:
        raise Failure("exit_status")
    document = json.loads(result.stdout)
    node_fs = document["node"]["fs"]
    available, capacity = node_fs["availableBytes"], node_fs["capacityBytes"]
    pods: dict[str, int] = {}
    for row in document["pods"]:
        reference = row["podRef"]
        if reference["namespace"] != namespace:
            continue
        used = row.get("ephemeral-storage", {}).get("usedBytes")
        if type(reference["name"]) is not str or not whole(used):
            raise Failure("unparsable")
        pods[reference["name"]] = used
    if not (whole(available) and whole(capacity)):
        raise Failure("unparsable")
    return {"node_fs_available_bytes": available, "node_fs_capacity_bytes": capacity, "pods": pods}


def sample_line(
    seq: int, deadline: float, kubectl: Sequence[str], node: str, namespace: str, runner: Runner, clock: Clock, wall_ns: Callable[[], int]
) -> dict[str, Any]:
    """The line of one reading: the summary with its times, or the fixed cause of its failure in ``missing`` and no summary."""
    line: dict[str, Any] = {"kind": "sample", "seq": seq, "missing": []}
    try:
        line["summary"] = timed(clock, wall_ns, deadline, lambda: summary_body(runner, kubectl, node, namespace, deadline, clock))
    except Failure as failure:
        line["missing"] = [{"reading": "summary", "cause": failure.cause}]
    return line


def run_sampler(
    output: Path,
    *,
    interval_seconds: float,
    max_seconds: float,
    kubectl: Sequence[str],
    node: str,
    namespace: str,
    runner: Runner = run_command,
    stop: StopFlag | None = None,
    clock: Clock = time.monotonic,
    wall_ns: Callable[[], int] = time.time_ns,
    pause: Callable[[float], None] = time.sleep,
    say: Callable[[str], None] = emit,
) -> int:
    """Write the header, one line for each reading and the end line. Return 0 when the end line was written."""
    flag = stop or StopFlag()
    try:
        if NODE_NAME.fullmatch(node) is None or NAMESPACE_NAME.fullmatch(namespace) is None or not kubectl:
            raise SamplerError("the node, the namespace or the kubectl command is not valid")
        header = {
            "kind": "header",
            "schema": SCHEMA,
            "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "interval_seconds": interval_seconds,
            "max_seconds": max_seconds,
            "namespace": namespace,
        }
        out = Output(output)
    except SamplerError as error:
        emit(f"staging sampler: {describe_error(error)}")
        return 1
    seq, reason = 0, ""
    try:
        out.write_line(header)
        started = clock()
        while True:
            if flag.requested:
                reason = "stop_flag"
                break
            if clock() - started >= max_seconds:
                reason = "max_seconds"
                break
            began = clock()
            out.write_line(sample_line(seq, min(began + interval_seconds, started + max_seconds), kubectl, node, namespace, runner, clock, wall_ns))
            seq += 1
            flag.sleep(max(0.0, min(interval_seconds - (clock() - began), max_seconds - (clock() - started))), pause=pause, clock=clock)
        out.write_line({"kind": "end", "reason": reason, "samples": seq})
    except SamplerError as error:
        emit(f"staging sampler: {describe_error(error)}")
        return 1
    finally:
        out.close()
    say(f"staging sampler: end line written after {seq} samples ({reason})")
    return 0


def parse_lines(text: str) -> list[dict[str, Any]]:
    """The JSON object of each line. Raise ``StagingRefused`` naming the first line that is not one."""
    rows = []
    for number, line in enumerate(text.splitlines(), 1):
        try:
            row = json.loads(line)
        except ValueError:
            raise StagingRefused(Notice(Code.STAGING_FORM, position=number)) from None
        if type(row) is not dict or type(row.get("kind")) is not str:
            raise StagingRefused(Notice(Code.STAGING_FORM, position=number))
        rows.append(row)
    return rows


def sample_reading(row: dict[str, Any], seq: int) -> dict[str, Any] | None:
    """The reading of a sample line, or None when the line names a failed reading. Raise ``ValueError`` when the line is not in the documented form."""
    missing = row.get("missing")
    if row.get("seq") != seq or type(missing) is not list:
        raise ValueError
    if missing:
        if "summary" in row or any(type(item) is not dict or set(item) != {"reading", "cause"} for item in missing):
            raise ValueError
        return None
    body = row.get("summary")
    if (
        type(body) is not dict
        or not (whole(body.get("node_fs_available_bytes")) and whole(body.get("node_fs_capacity_bytes")))
        or type(body.get("pods")) is not dict
    ):
        raise ValueError
    if any(type(name) is not str or not whole(used) for name, used in body["pods"].items()):
        raise ValueError
    return body


def peak_record(text: str) -> dict[str, Any]:
    """The figures of the staging samples. Raise ``StagingRefused`` for a file that is cut off, empty of samples, malformed or holding a failed reading."""
    rows = parse_lines(text)
    if not rows or rows[0]["kind"] != "header" or rows[0].get("schema") != SCHEMA:
        raise StagingRefused(Notice(Code.STAGING_FORM, position=1))
    samples = rows[1:-1]
    last = rows[-1]
    if last["kind"] != "end" or type(last.get("samples")) is not int or last["samples"] != len(samples):
        raise StagingRefused(Notice(Code.STAGING_CUT_OFF))
    readings = []
    for position, row in enumerate(samples, 2):
        try:
            if row["kind"] != "sample":
                raise ValueError
            readings.append(sample_reading(row, position - 2))
        except ValueError:
            raise StagingRefused(Notice(Code.STAGING_FORM, position=position)) from None
    if not samples:
        raise StagingRefused(Notice(Code.STAGING_NO_SAMPLES))
    failed = sum(1 for reading in readings if reading is None)
    if failed:
        raise StagingRefused(Notice(Code.STAGING_MISSING, count=failed))
    bodies = [reading for reading in readings if reading is not None]
    totals = [sum(body["pods"].values()) for body in bodies]
    peak_at = totals.index(max(totals))
    pods = bodies[peak_at]["pods"]
    biggest = max(sorted(pods), key=lambda name: pods[name], default="")
    return {
        "format": PEAK_FORMAT,
        "samples": len(bodies),
        "interval_seconds": rows[0].get("interval_seconds"),
        "namespace": rows[0].get("namespace"),
        "peak_pod_bytes_total": totals[peak_at],
        "peak_at_seq": peak_at,
        "peak_pod": biggest,
        "peak_pod_bytes": pods.get(biggest, 0),
        "first_sample_pod_bytes_total": totals[0],
        "peak_increase_bytes": totals[peak_at] - totals[0],
        "min_node_free_bytes": min(body["node_fs_available_bytes"] for body in bodies),
        "node_capacity_bytes": bodies[-1]["node_fs_capacity_bytes"],
    }


def command_peak(arguments: argparse.Namespace) -> int:
    record = peak_record(read_input(arguments.samples, Subject.STAGING_SAMPLES))
    write_private(arguments.output, json.dumps(record, indent=1, sort_keys=True) + "\n")
    say(Notice(Code.STAGING_PEAK_WROTE, count=record["samples"]))
    return 0


def command_sample(arguments: argparse.Namespace) -> int:
    program = shutil.which(arguments.kubectl[0]) if arguments.kubectl else None
    if program is None:
        emit("staging sampler: the kubectl program cannot be found")
        return 1
    kubectl = [program, *arguments.kubectl[1:]]
    return run_sampler(
        arguments.output,
        interval_seconds=arguments.interval_seconds,
        max_seconds=arguments.max_seconds,
        kubectl=kubectl,
        node=arguments.node,
        namespace=arguments.namespace,
        stop=install_stop(),
    )


def install_stop() -> StopFlag:
    flag = StopFlag()
    flag.install()
    return flag


def main(argv: list[str] | None = None) -> int:
    """Run ``sample`` or ``peak`` and return 0 when it holds, 1 for a refusal or an output that cannot be written."""
    parser = QuietParser(prog=PROGRAM, description=(__doc__ or "").splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    sampler = commands.add_parser("sample")
    sampler.add_argument("--output", type=Path, required=True, help="a new file for the samples")
    sampler.add_argument("--node", required=True, help="the name of the node, from kubectl get nodes")
    sampler.add_argument("--namespace", default="airbyte")
    sampler.add_argument("--interval-seconds", type=float, default=DEFAULT_INTERVAL_SECONDS)
    sampler.add_argument("--max-seconds", type=float, default=DEFAULT_RUN_SECONDS)
    sampler.add_argument("kubectl", nargs="+", help="the kubectl command, after --")
    peaker = commands.add_parser("peak")
    peaker.add_argument("--samples", type=Path, required=True)
    peaker.add_argument("--output", type=Path, required=True, help="where to write the peak record")
    arguments = parser.parse_args(argv)
    if arguments.command == "sample" and not (0 < arguments.interval_seconds <= MAX_INTERVAL_SECONDS and 0 < arguments.max_seconds <= MAX_RUN_SECONDS):
        emit("staging sampler: the interval or the run time is out of range")
        return 1
    try:
        check_new_output(arguments.output)
        if arguments.command == "sample":
            return command_sample(arguments)
        return command_peak(arguments)
    except ClosedError as error:
        for notice in error.notices:
            sys.stderr.write(f"{render(notice)}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
