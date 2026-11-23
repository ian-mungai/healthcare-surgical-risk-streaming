"""The disk summary: the peak staging use and the lowest free disk space of the copy and of the retry, from the capacity sampler's files.

``.venv/bin/python -m scripts.stack.disk_summary --phase copy=copy_samples.jsonl --phase retry=retry_samples.jsonl --output disk_measurements.json``

Each phase is one run of ``scripts.stack.capacity_sampler`` with at least one ``--staging NAME=PATTERN``, started before the copy (or the retry) and
stopped after it. The summary reads each file and writes one record: for each phase the lowest ``free_disk_mib`` the sampler read and the largest
``allocated_bytes`` and ``apparent_bytes`` it read for each staging name, each with the sample where it came first. A ``--limit NAME=BYTES`` adds the
limit and the headroom (the limit less the peak allocated bytes, negative when the peak is above it) to that name. The record states figures and states
no verdict. The copy and the retry are both required; any other phase, such as a controlled run that exercises the staging limit, is summarised beside them.

A file that cannot give both figures is refused with no output: a file with no end line (it was cut off and can hide the peak), an end line whose count is
not the samples read, a file of an earlier schema, a run with no staging source and a file with no free disk reading or no staging reading. The same
files give the same bytes. The output is a new file for its owner only.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from scripts.ingestion.closed import QuietParser
from scripts.stack.capacity_sampler import SCHEMA, SamplerError, parse_staging

SUMMARY_FORMAT = "disk_measurements_1"
REQUIRED_PHASES = ("copy", "retry")
END_REASONS = ("stop_flag", "max_seconds")
READINGS = ("host", "containers", "memory", "staging")
NAME = re.compile(r"[a-z][a-z0-9_]{0,31}")


class SummaryError(RuntimeError):
    """A sampler file cannot give its figures, or the output cannot be written. The message names a phase and holds no text of a file."""


def sampler_lines(phase: str, path: Path) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    """The header, the sample lines and the end line of one file, each checked to be where the sampler writes it."""
    try:
        text = path.read_text()
    except OSError, UnicodeDecodeError:
        raise SummaryError(f"the {phase} file cannot be read") from None
    lines = text.split("\n")
    if lines.pop() != "":
        raise SummaryError(f"the {phase} file has an incomplete last line: it was cut off")
    try:
        found = [json.loads(line) for line in lines]
    except ValueError:
        raise SummaryError(f"the {phase} file has a line that is not JSON") from None
    kinds = [line.get("kind") if isinstance(line, dict) else None for line in found]
    if not found or kinds[0] != "header":
        raise SummaryError(f"the {phase} file does not start with one header line")
    if kinds[-1] != "end":
        raise SummaryError(f"the {phase} file has no end line: it was cut off and can hide the peak")
    samples = found[1:-1]
    if any(kind != "sample" for kind in kinds[1:-1]):
        raise SummaryError(f"the {phase} file has a line of another kind between its header and its end line")
    if found[-1].get("samples") != len(samples):
        raise SummaryError(f"the {phase} file's end line count is not the number of its sample lines")
    return found[0], samples, found[-1]


def plain_number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def check_header(phase: str, header: Mapping[str, Any]) -> None:
    """Refuse a header whose values the record would copy and that are not in the form the sampler writes them."""
    sources = header["sources"]
    free_source = sources["free_disk"]
    patterns = sources.get("staging", {})
    plain = isinstance(free_source, str) and not any(ord(character) < 32 for character in free_source)
    if not plain or not plain_number(header["interval_seconds"]) or not isinstance(patterns, dict):
        raise SummaryError(f"the {phase} file's header holds a value that is not in the sampler's form")
    try:
        for name, pattern in patterns.items():
            parse_staging(f"{name}={pattern}")
    except SamplerError:
        raise SummaryError(f"the {phase} file's header has a staging source that is not in the sampler's form") from None


def phase_summary(phase: str, path: Path, limits: Mapping[str, int]) -> dict[str, Any]:
    header, samples, end = sampler_lines(phase, path)
    if header.get("schema") != SCHEMA:
        raise SummaryError(f"the {phase} file is of another schema than {SCHEMA}")
    check_header(phase, header)
    patterns = header["sources"].get("staging")
    if not patterns:
        raise SummaryError(f"the {phase} run had no staging source: start the sampler with --staging")
    if end["reason"] not in END_REASONS:
        raise SummaryError(f"the {phase} file's end line has a reason that the sampler does not write")
    free: list[tuple[float, int]] = []
    peaks: dict[str, dict[str, int]] = {name: {"readings": 0, "allocated": -1, "apparent": -1, "at_seq": -1} for name in patterns}
    missing: Counter[str] = Counter()
    for line in samples:
        if any(item["reading"] not in READINGS for item in line["missing"]):
            raise SummaryError(f"the {phase} file names a missing reading that the sampler does not write")
        missing.update(item["reading"] for item in line["missing"])
        if "host" in line:
            free.append((line["host"]["free_disk_mib"], line["seq"]))
        for row in line["staging"]["rows"] if "staging" in line else ():
            if row["name"] not in peaks:
                raise SummaryError(f"the {phase} file has a staging name that is not in the header")
            peak = peaks[row["name"]]
            peak["readings"] += 1
            peak["apparent"] = max(peak["apparent"], row["apparent_bytes"])
            if row["allocated_bytes"] > peak["allocated"]:
                peak["allocated"], peak["at_seq"] = row["allocated_bytes"], line["seq"]
    if not free:
        raise SummaryError(f"the {phase} file has no free disk reading")
    if any(peak["readings"] == 0 for peak in peaks.values()):
        raise SummaryError(f"the {phase} file has no staging reading for each named source")
    lowest = min(free, key=lambda item: item[0])
    staging = []
    for name, pattern in patterns.items():
        peak = peaks[name]
        figures: dict[str, Any] = {
            "name": name,
            "pattern": pattern,
            "peak_allocated_bytes": peak["allocated"],
            "peak_apparent_bytes": peak["apparent"],
            "at_seq": peak["at_seq"],
            "readings": peak["readings"],
        }
        if name in limits:
            figures["limit_bytes"], figures["headroom_bytes"] = limits[name], limits[name] - peak["allocated"]
        staging.append(figures)
    return {
        "file": path.name,
        "end_reason": end["reason"],
        "samples": len(samples),
        "interval_seconds": header["interval_seconds"],
        "free_disk": {"source": header["sources"]["free_disk"], "lowest_mib": lowest[0], "at_seq": lowest[1], "first_mib": free[0][0], "readings": len(free)},
        "staging": staging,
        "missing": dict(sorted(missing.items())),
    }


def summarize(phases: Mapping[str, Path], limits: Mapping[str, int]) -> dict[str, Any]:
    """The record of the named sampler files. The copy and the retry come first. Any figure that cannot be read refuses the whole record."""
    if not set(REQUIRED_PHASES) <= set(phases):
        raise SummaryError("the copy and retry phases are both required")
    ordered = [*REQUIRED_PHASES, *(name for name in phases if name not in REQUIRED_PHASES)]
    seen: set[tuple[int, int]] = set()
    for name in ordered:
        try:
            info = phases[name].stat()
        except OSError:
            raise SummaryError(f"the {name} file cannot be read") from None
        if (info.st_dev, info.st_ino) in seen:
            raise SummaryError("one sampler file is named for two phases")
        seen.add((info.st_dev, info.st_ino))
    summaries: dict[str, Any] = {}
    for name in ordered:
        try:
            summaries[name] = phase_summary(name, phases[name], limits)
        except KeyError, TypeError, AttributeError, ValueError:
            raise SummaryError(f"the {name} file is not in the form of the sampler's lines") from None
    known = {row["name"] for summary in summaries.values() for row in summary["staging"]}
    if set(limits) - known:
        raise SummaryError("a limit names a staging source that no file holds")
    return {"format": SUMMARY_FORMAT, "phases": summaries}


def write_record(path: Path, document: Mapping[str, Any]) -> None:
    """Create the output for its owner only. An existing path, also a link, is refused and left as it was."""
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        raise SummaryError("the output path exists") from None
    except OSError:
        raise SummaryError("the output path cannot be created") from None
    with os.fdopen(descriptor, "w") as handle:
        handle.write(json.dumps(document, indent=1) + "\n")


def phase_argument(text: str) -> tuple[str, Path]:
    name, separator, path = text.partition("=")
    if not separator or NAME.fullmatch(name) is None or not path:
        raise argparse.ArgumentTypeError("a phase is NAME=FILE with a lowercase name")
    return name, Path(path)


def limit_argument(text: str) -> tuple[str, int]:
    name, separator, number = text.partition("=")
    if not separator or NAME.fullmatch(name) is None or not (number.isascii() and number.isdigit()):
        raise argparse.ArgumentTypeError("a limit is NAME=BYTES with a lowercase name and a whole number of bytes")
    return name, int(number)


def describe(name: str, summary: Mapping[str, Any]) -> list[str]:
    free = summary["free_disk"]
    lines = [f"{name}: lowest free disk {free['lowest_mib']} MiB at sample {free['at_seq']} of {summary['samples']}"]
    for row in summary["staging"]:
        limit = f", limit {row['limit_bytes']}, headroom {row['headroom_bytes']}" if "limit_bytes" in row else ""
        lines.append(f"{name}: peak staging {row['name']} {row['peak_allocated_bytes']} allocated bytes at sample {row['at_seq']}{limit}")
    return lines


def main(argv: Sequence[str] | None = None) -> int:
    command = QuietParser(prog="python -m scripts.stack.disk_summary", description=(__doc__ or "").split("\n\n")[0])
    command.add_argument(
        "--phase", type=phase_argument, action="append", required=True, metavar="NAME=FILE", help="a sampler file; copy and retry are required"
    )
    command.add_argument("--limit", type=limit_argument, action="append", default=[], metavar="NAME=BYTES", help="the size limit of a staging source")
    command.add_argument("--output", type=Path, required=True, help="a new file for the record")
    arguments = command.parse_args(argv)
    names = [name for name, _ in arguments.phase]
    limit_names = [name for name, _ in arguments.limit]
    if len(set(names)) != len(names) or len(set(limit_names)) != len(limit_names):
        command.error("a phase or a limit is named twice")
    try:
        document = summarize(dict(arguments.phase), dict(arguments.limit))
        write_record(arguments.output, document)
    except SummaryError as error:
        sys.stderr.write(f"disk summary: {error}\n")
        return 1
    for name, summary in document["phases"].items():
        sys.stdout.write("\n".join(describe(name, summary)) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
