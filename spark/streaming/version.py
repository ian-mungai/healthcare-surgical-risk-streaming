"""The version of the feature code and the stable IDs of the job's records.

The version is the SHA-256 of the sources of the feature package, so a change to any feature, rule or limit gives a new version and new feature IDs,
and a rebuild
from one commit gives the same version. The batch path takes its version from this function too.
"""

from __future__ import annotations

import hashlib
from functools import cache
from pathlib import Path

from replay.events.identity import DELIMITER, component

FEATURES_FOLDER = Path(__file__).resolve().parents[2] / "features"
VERSION_PREFIX = "features_"
VERSION_DIGITS = 12


@cache
def feature_version() -> str:
    digest = hashlib.sha256()
    for path in sorted(FEATURES_FOLDER.glob("*.py")):
        digest.update(path.name.encode() + b"\0" + path.read_bytes() + b"\0")
    return f"{VERSION_PREFIX}{digest.hexdigest()[:VERSION_DIGITS]}"


def feature_id(run_id: str, room: int, occurrence_id: str, cutoff_case_time_us: int, version: str) -> str:
    """The ID of the feature row of an occurrence at a cutoff: the run, room, occurrence, cutoff in microseconds of case time and the feature version."""
    return DELIMITER.join(
        [component(run_id, "run"), str(room), component(occurrence_id, "occurrence"), str(cutoff_case_time_us), component(version, "version")]
    )


def ledger_id(run_id: str, room: int, occurrence_id: str) -> str:
    return DELIMITER.join([component(run_id, "run"), str(room), component(occurrence_id, "occurrence"), "ledger"])


def error_id(run_id: str, room: int, occurrence_id: str, cutoff_case_time_us: int, version: str) -> str:
    return DELIMITER.join(
        [component(run_id, "run"), str(room), component(occurrence_id, "occurrence"), str(cutoff_case_time_us), component(version, "version"), "error"]
    )
