"""The run record: the settings of one replay run, in one JSON file with exactly its known fields.

It holds what the run is told and nothing the run decides: the run ID, the rooms, the speed, the stagger, the turnover and the window, the cases (all case
files of the manifest when absent), where the manifest reference and the object store are, and where Kafka, the registry and the saved files are. The speed
is here, so a run's record says how fast it was played. A missing, extra or mistyped field refuses the file, and so does a timing that is not a finite
number from 0 to 86,400 seconds (a day) or a speed that is not a finite number from 0.01 to 1,000.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from replay.events.run import DEFAULT_CHECKPOINT_EVENTS, MAX_SPEED, MIN_SPEED


class RunRecordError(ValueError):
    """The run record is not valid JSON or has an unknown, missing or mistyped field."""


@dataclass(frozen=True, slots=True)
class StoreSpec:
    kind: str
    root: str | None = None
    region: str | None = None
    read_log: str | None = None


@dataclass(frozen=True, slots=True)
class RunRecord:
    run_id: str
    rooms: int
    speed: float
    stagger_seconds: int
    turnover_seconds: int
    window_seconds: int
    cases: tuple[int, ...] | None
    manifest_reference: str
    object_store: StoreSpec
    kafka_bootstrap: str
    registry_url: str
    state_dir: str
    checkpoint_every: int = DEFAULT_CHECKPOINT_EVENTS
    idle_checkpoint_seconds: float = 5.0

    @property
    def state_path(self) -> Path:
        return Path(self.state_dir) / "state.json"

    @property
    def schedule_path(self) -> Path:
        return Path(self.state_dir) / "schedule.json"

    @property
    def input_paths(self) -> tuple[Path, ...]:
        """The files that the record names and a command reads or keeps: the saved state, the frozen schedule and the manifest reference."""
        return (self.state_path, self.schedule_path, Path(self.manifest_reference))


REQUIRED: dict[str, type | tuple[type, ...]] = {
    "run_id": str,
    "rooms": int,
    "speed": (int, float),
    "stagger_seconds": int,
    "turnover_seconds": int,
    "window_seconds": int,
    "manifest_reference": str,
    "object_store": dict,
    "kafka_bootstrap": str,
    "registry_url": str,
    "state_dir": str,
}
OPTIONAL: dict[str, type | tuple[type, ...]] = {"cases": list, "checkpoint_every": int, "idle_checkpoint_seconds": (int, float)}
TIMING_FIELDS = ("stagger_seconds", "turnover_seconds", "window_seconds", "idle_checkpoint_seconds")
MAX_TIMING_SECONDS = 86_400


def closed(document: Any, required: dict[str, Any], optional: dict[str, Any], what: str) -> dict[str, Any]:
    if not isinstance(document, dict) or set(required) - set(document) or set(document) - set(required) - set(optional):
        raise RunRecordError(f"{what} must be an object with the fields: {', '.join(sorted(required))}; optional: {', '.join(sorted(optional))}")
    wrong = sorted(
        name
        for name, expected in {**required, **optional}.items()
        if name in document and (not isinstance(document[name], expected) or isinstance(document[name], bool))
    )
    if wrong:
        raise RunRecordError(f"{what} has a field of the wrong type: {', '.join(wrong)}")
    return document


def parse_run_record(text: str) -> RunRecord:
    try:
        document = closed(json.loads(text), REQUIRED, OPTIONAL, "the run record")
    except ValueError as error:
        if isinstance(error, RunRecordError):
            raise
        raise RunRecordError("the run record is not valid JSON") from None
    for field in TIMING_FIELDS:
        if field in document and not 0 <= document[field] <= MAX_TIMING_SECONDS:
            raise RunRecordError(f"{field} must be a finite number from 0 to {MAX_TIMING_SECONDS:,}")
    try:
        speed = float(document["speed"])
    except OverflowError:
        speed = math.inf
    if not math.isfinite(speed) or not MIN_SPEED <= speed <= MAX_SPEED:
        raise RunRecordError(f"speed must be a finite number from {MIN_SPEED:g} to {MAX_SPEED:,.0f}")
    store = closed(document["object_store"], {"kind": str}, {"root": str, "region": str, "read_log": str}, "the run record's object_store")
    if store["kind"] not in ("folder", "s3") or (store["kind"] == "folder" and "root" not in store) or (store["kind"] == "s3" and "region" not in store):
        raise RunRecordError("object_store is a folder with a root or an s3 store with a region")
    cases = document.get("cases")
    if cases is not None and (not cases or any(not isinstance(case, int) or isinstance(case, bool) for case in cases)):
        raise RunRecordError("cases is a non-empty list of case numbers")
    return RunRecord(
        document["run_id"],
        document["rooms"],
        speed,
        document["stagger_seconds"],
        document["turnover_seconds"],
        document["window_seconds"],
        None if cases is None else tuple(cases),
        document["manifest_reference"],
        StoreSpec(store["kind"], store.get("root"), store.get("region"), store.get("read_log")),
        document["kafka_bootstrap"],
        document["registry_url"],
        document["state_dir"],
        document.get("checkpoint_every", DEFAULT_CHECKPOINT_EVENTS),
        float(document.get("idle_checkpoint_seconds", 5.0)),
    )
