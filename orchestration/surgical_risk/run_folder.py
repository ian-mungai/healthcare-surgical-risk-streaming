"""The run folder: one folder for each finished Dagster run, holding ``record.json``.

The record keeps what the project's rule for every run asks for, in the form of a run of Dagster: the UTC time, the environment and the commit, the command and
the configuration, the inputs and seeds, each step and check as an assertion with its outcome, the outputs, the limits of the record, what the run left running
and the safe rerun. A passing, a failing and a canceled run each get a folder.

The record is built from plain facts (``RunFacts``) so that it has no Dagster import: ``run_records.py`` reads the facts from Dagster. A secret never reaches
the record: a value under a secret-looking key is replaced, and each such value is also removed from every text of the record.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REDACTED = "[redacted]"
LEFT_RUNNING_TAG = "surgical_risk/left_running"
NO_SERVICE_LEFT = "-"
SECRET_KEY = re.compile(r"pass|secret|token|key|credential|auth|private", re.IGNORECASE)
MIN_MASKED_LENGTH = 4
MAX_ERROR_LENGTH = 1000
SYSTEM_TAG_PREFIX = "dagster/"
OUTCOMES = {"SUCCESS": "pass", "FAILURE": "fail", "CANCELED": "canceled"}
LIMITS = (
    "a value under a secret-looking key is replaced, and each such value is removed from every text of this record",
    "the record holds run facts only: no event log and no compute log, which Dagster keeps in its own storage",
    "Dagster stores the run configuration as it was given: pass a secret as an environment reference, never as a value",
    "a run that is still queued or started has no folder until it ends",
)
SEEDS_TEXT = "Dagster sets no seed. A seed given in the run configuration is in the configuration above."
RESET_TEXT = "none: a Dagster run changes only its own history. The run is not undone by this record."


@dataclass(frozen=True)
class StepFacts:
    key: str
    status: str
    started: float | None
    ended: float | None


@dataclass(frozen=True)
class CheckFacts:
    name: str
    asset: str
    passed: bool


@dataclass(frozen=True)
class RunFacts:
    run_id: str
    job_name: str
    status: str
    run_config: Mapping[str, Any]
    tags: Mapping[str, str]
    enqueued: float | None
    launched: float | None
    started: float | None
    ended: float | None
    steps: Sequence[StepFacts]
    checks: Sequence[CheckFacts]
    error: str
    parent_run_id: str | None
    asset_selection: Sequence[str]


def left_running_tag_value(names: Sequence[str]) -> str:
    """The run tag value for the services a job left running: their names joined by dots, or a dash for none."""
    return ".".join(names) if names else NO_SERVICE_LEFT


def iso(seconds: float | None) -> str | None:
    return None if seconds is None else datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def gap(later: float | None, earlier: float | None) -> float | None:
    return None if later is None or earlier is None else round(later - earlier, 3)


def strings_in(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, Mapping):
        return [text for item in value.values() for text in strings_in(item)]
    if isinstance(value, list | tuple):
        return [text for item in value for text in strings_in(item)]
    return []


def redact(value: object, secrets: set[str]) -> object:
    """A copy of ``value`` with every value under a secret-looking key replaced. The replaced strings are added to ``secrets``."""
    if isinstance(value, Mapping):
        clean: dict[str, object] = {}
        for key, item in value.items():
            if SECRET_KEY.search(str(key)):
                secrets.update(strings_in(item))
                clean[str(key)] = REDACTED
            else:
                clean[str(key)] = redact(item, secrets)
        return clean
    if isinstance(value, list | tuple):
        return [redact(item, secrets) for item in value]
    return value


def mask(value: object, secrets: set[str]) -> object:
    """A copy of ``value`` with every secret removed from each of its texts."""
    if isinstance(value, str):
        for secret in secrets:
            value = value.replace(secret, REDACTED)
        return value
    if isinstance(value, dict):
        return {key: mask(item, secrets) for key, item in value.items()}
    if isinstance(value, list):
        return [mask(item, secrets) for item in value]
    return value


def redact_tags(tags: Mapping[str, str], secrets: set[str]) -> dict[str, object]:
    """The tags without the left-running tag. Tags Dagster sets itself stay as they are, and every other value under a secret-looking key is replaced."""
    system = {key: value for key, value in tags.items() if key.startswith(SYSTEM_TAG_PREFIX)}
    others = {key: value for key, value in tags.items() if not key.startswith(SYSTEM_TAG_PREFIX) and key != LEFT_RUNNING_TAG}
    redacted = redact(others, secrets)
    return {**system, **(redacted if isinstance(redacted, dict) else {})}


def left_running(tags: Mapping[str, str]) -> tuple[list[str], bool]:
    """The services a job reported as left running, and whether the job reported at all."""
    value = tags.get(LEFT_RUNNING_TAG)
    if value is None:
        return [], False
    return ([] if value == NO_SERVICE_LEFT else value.split(".")), True


def cleanup_text(names: Sequence[str], reported: bool) -> str:
    if not reported:
        return "the job reported nothing about what it left running"
    return f"left running after the run: {', '.join(names)}" if names else "the job reported that nothing was left running"


def launched_by(tags: Mapping[str, str]) -> str:
    if "dagster/schedule_name" in tags:
        return f"by the schedule {tags['dagster/schedule_name']}"
    if "dagster/sensor_name" in tags:
        return f"by the sensor {tags['dagster/sensor_name']}"
    return "by hand or through the API"


def build_record(facts: RunFacts, *, commit: str, environment: Mapping[str, str], recorded: datetime) -> dict[str, Any]:
    """The record of one finished run. Secrets are removed before it is returned."""
    secrets: set[str] = set()
    configuration = redact(facts.run_config, secrets)
    tags = redact_tags(facts.tags, secrets)
    assertions = [
        {"name": f"step {step.key}", "expected": "the step succeeds", "observed": step.status, "passed": step.status == "SUCCESS"} for step in facts.steps
    ] + [
        {
            "name": f"check {check.name} on {check.asset}",
            "expected": "the check passes",
            "observed": "passed" if check.passed else "failed",
            "passed": check.passed,
        }
        for check in facts.checks
    ]
    names, reported = left_running(facts.tags)
    outcome = OUTCOMES.get(facts.status, facts.status.lower())
    if outcome == "pass" and not all(item["passed"] for item in assertions):
        outcome = "fail"
    record: dict[str, Any] = {
        "run_id": facts.run_id,
        "job_name": facts.job_name,
        "status": facts.status,
        "outcome": outcome,
        "commit": commit or "unknown",
        "environment": dict(environment),
        "started_utc": iso(facts.started),
        "ended_utc": iso(facts.ended),
        "recorded_utc": recorded.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "configuration": configuration,
        "tags": tags,
        "timings": {
            "enqueued_utc": iso(facts.enqueued),
            "launched_utc": iso(facts.launched),
            "queued_seconds": gap(facts.started, facts.enqueued),
            "run_seconds": gap(facts.ended, facts.started),
            "steps": [
                {
                    "step": step.key,
                    "status": step.status,
                    "started_utc": iso(step.started),
                    "ended_utc": iso(step.ended),
                    "seconds": gap(step.ended, step.started),
                }
                for step in facts.steps
            ],
        },
        "commands": [f"launch the job {facts.job_name} with the configuration above, {launched_by(facts.tags)}"],
        "reset": RESET_TEXT,
        "inputs": {"asset_selection": list(facts.asset_selection), "parent_run_id": facts.parent_run_id},
        "seeds": SEEDS_TEXT,
        "assertions": assertions,
        "outputs": {
            "error": str(mask(facts.error, secrets))[:MAX_ERROR_LENGTH],
            "steps_succeeded": sum(step.status == "SUCCESS" for step in facts.steps),
            "steps_failed": sum(step.status == "FAILURE" for step in facts.steps),
        },
        "limits": list(LIMITS),
        "left_running": names,
        "left_running_reported": reported,
        "cleanup": cleanup_text(names, reported),
        "rerun": f"launch the job {facts.job_name} again in Dagster with the configuration above, from its interface or the launchRun mutation",
    }
    masked = mask(record, secrets)
    if not isinstance(masked, dict):
        raise TypeError("the masked record is not a mapping")
    return masked


def folder_name(record: Mapping[str, Any]) -> str:
    stamp = re.sub(r"[-:]", "", str(record["started_utc"] or record["recorded_utc"]))
    return f"{stamp}_{record['job_name']}_{record['run_id']}"


def write_record(runs: Path, record: Mapping[str, Any]) -> Path:
    """Write the record into its folder under ``runs`` and return the folder. A run that has a folder keeps it unchanged."""
    runs.mkdir(parents=True, exist_ok=True)
    existing = sorted(runs.glob(f"*_{record['run_id']}"))
    if existing:
        return existing[0]
    text = json.dumps(record, indent=2) + "\n"
    staging = Path(tempfile.mkdtemp(prefix=".writing_", dir=runs))
    try:
        (staging / "record.json").write_text(text)
        staging.chmod(0o755)
        (staging / "record.json").chmod(0o644)
        final = runs / folder_name(record)
        os.rename(staging, final)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return final
