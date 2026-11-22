"""The run's state file ``<run ID>.state.json``: what this run created, so that a retry deletes only that.

It names each object version the run wrote (read key and version ID), the load branch with the hash it was created from, and each job the run started. It is
written whole (a temporary file and a rename) right after each creation and before the next step. A creation that a stop left unrecorded is not owned by the run
on the retry: the retry refuses it and deletes nothing. Deletion uses only this evidence, version ID by version ID and never "every version at a key", and
deletes the branch only when the file records that this run created it.

``RunState`` is the run's ledger of object versions as ``record_copy`` uses it: ``versions``, ``keys``, ``add`` and ``discard``.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any

from scripts.ingestion.record_definitions import READ_PREFIX, check_run

STATE_FORMAT = "load_state_1"
STATE_FIELDS = ("format", "run_id", "copies", "branch", "jobs")
BRANCH_FIELDS = ("name", "created_from")
HASH = re.compile(r"[0-9a-f]{64}")
LOAD_BRANCH = re.compile(r"load_[a-z0-9][a-z0-9_]{0,63}")


class StateError(RuntimeError):
    """The state file is not of its form, or a record is not valid. The message holds no value of the file."""


class RunState:
    def __init__(self, directory: Path, run_id: str) -> None:
        self.directory = directory
        self.run_id = check_run(run_id)
        self.path = directory / f"{run_id}.state.json"
        self.copies: dict[str, set[str]] = {}
        self.branch: tuple[str, str] | None = None
        self.jobs: dict[str, int] = {}
        if self.path.exists():
            self.read()

    def read(self) -> None:
        try:
            document = json.loads(self.path.read_text())
            if type(document) is not dict or set(document) != set(STATE_FIELDS) or document["format"] != STATE_FORMAT or document["run_id"] != self.run_id:
                raise ValueError
            copies, jobs, branch = document["copies"], document["jobs"], document["branch"]
            if type(copies) is not dict or type(jobs) is not dict:
                raise ValueError
            for key, versions in copies.items():
                if not key.startswith(READ_PREFIX) or type(versions) is not list or not all(type(version) is str and version for version in versions):
                    raise ValueError
            if not all(type(table) is str and type(job) is int and type(job) is not bool for table, job in jobs.items()):
                raise ValueError
            if branch is not None and (
                type(branch) is not dict
                or set(branch) != set(BRANCH_FIELDS)
                or LOAD_BRANCH.fullmatch(str(branch["name"])) is None
                or HASH.fullmatch(str(branch["created_from"])) is None
            ):
                raise ValueError
        except ValueError, TypeError, AttributeError, KeyError, OSError:
            raise StateError("the state file of this run is not a state file of this form") from None
        self.copies = {key: set(versions) for key, versions in copies.items()}
        self.branch = None if branch is None else (branch["name"], branch["created_from"])
        self.jobs = dict(jobs)

    def document(self) -> dict[str, Any]:
        return {
            "format": STATE_FORMAT,
            "run_id": self.run_id,
            "copies": {key: sorted(versions) for key, versions in sorted(self.copies.items()) if versions},
            "branch": None if self.branch is None else {"name": self.branch[0], "created_from": self.branch[1]},
            "jobs": dict(sorted(self.jobs.items())),
        }

    def save(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=".writing_", dir=self.directory)
        try:
            with os.fdopen(descriptor, "w") as handle:
                handle.write(json.dumps(self.document(), indent=2, sort_keys=True) + "\n")
            os.chmod(temporary, 0o644)
            os.replace(temporary, self.path)
        except BaseException:
            Path(temporary).unlink(missing_ok=True)
            raise

    def versions(self, key: str) -> frozenset[str]:
        return frozenset(self.copies.get(key, set()))

    def keys(self) -> frozenset[str]:
        return frozenset(key for key, versions in self.copies.items() if versions)

    def add(self, key: str, version_id: str) -> None:
        self.copies.setdefault(key, set()).add(version_id)
        self.save()

    def discard(self, key: str, version_id: str) -> None:
        self.copies.get(key, set()).discard(version_id)
        self.save()

    def record_branch(self, name: str, created_from: str) -> None:
        if LOAD_BRANCH.fullmatch(name) is None or HASH.fullmatch(created_from) is None:
            raise StateError("a branch is recorded only as a load branch with the 64 hex digit hash it was created from")
        self.branch = (name, created_from)
        self.save()

    def forget_branch(self) -> None:
        self.branch = None
        self.save()

    def record_job(self, table: str, job: int) -> None:
        self.jobs[table] = job
        self.save()
