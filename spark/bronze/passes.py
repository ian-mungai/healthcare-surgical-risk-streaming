"""The functions that a Spark partition runs in the Bronze job. They live in a module of their own so that a worker imports them by name.

A partition gets the case files assigned to it and reads one case file at a time into a scratch folder, one track of it at a time (``spark/bronze/library.py``).
"""

from __future__ import annotations

import resource
import tempfile
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

from spark.bronze.inputs import CaseFile, StoreSpec, fetch_case
from spark.bronze.library import READS, case_problem, case_rows
from spark.bronze.readers import open_store
from spark.bronze.rows import Lineage
from spark.session import SCRATCH_FOLDER

KIB_PER_MIB = 1024


def worker_peak_mib() -> int:
    """The peak resident memory of this process so far (Linux counts ``ru_maxrss`` in KiB)."""
    return -(-resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // KIB_PER_MIB)


def case_files(files: Sequence[CaseFile], spec: StoreSpec, *, kind: str, mode: str) -> Iterator[tuple[Any, ...]]:
    """The rows of one kind for each case of a partition: one case file at a time in a scratch folder that is removed when its rows have been taken."""
    store = open_store(spec)
    for file in files:
        lineage = Lineage(file.version_id, file.sha256)
        with tempfile.TemporaryDirectory(prefix="bronze_case_", dir=scratch_folder()) as folder:
            path = fetch_case(store, file, Path(folder))
            yield from case_rows(path, file.case_id, kind, lineage, mode)


def scratch_folder() -> str | None:
    """The scratch folder of the job when it exists (the Spark service's), otherwise the system's."""
    return SCRATCH_FOLDER if Path(SCRATCH_FOLDER).is_dir() else None


def partition_rows(kind: str, spec: StoreSpec, peaks: Any, reads: Any, mode: str) -> Any:
    """The function that a Spark partition runs: its cases' rows of one kind. At its end it reports the peak memory to ``peaks`` and the reads to ``reads``."""

    def run(files: Iterator[CaseFile]) -> Iterator[tuple[Any, ...]]:
        before = dict(READS)
        yield from case_files(list(files), spec, kind=kind, mode=mode)
        peaks.add(worker_peak_mib())
        reads.add({f"{kind}_{name}": READS[name] - before[name] for name in READS})

    return run


def partition_problems(spec: StoreSpec, caseends: dict[int, int]) -> Any:
    """The function that a Spark partition runs for the structural check: ``(case, problem)`` for each case, where ``problem`` is ``None`` for a whole file."""

    def run(files: Iterator[CaseFile]) -> Iterator[tuple[int, str | None]]:
        store = open_store(spec)
        for file in files:
            with tempfile.TemporaryDirectory(prefix="bronze_check_", dir=scratch_folder()) as folder:
                path = fetch_case(store, file, Path(folder))
                yield (file.case_id, case_problem(path, file.case_id, caseends[file.case_id]))

    return run


class MaxParameter:
    """A Spark accumulator parameter that keeps the largest value added."""

    def zero(self, value: int) -> int:
        return 0

    def addInPlace(self, first: int, second: int) -> int:
        return max(first, second)


class SumByName:
    """A Spark accumulator parameter for a mapping of names to counts: it adds the counts of each name."""

    def zero(self, value: dict[str, int]) -> dict[str, int]:
        return {}

    def addInPlace(self, first: dict[str, int], second: dict[str, int]) -> dict[str, int]:
        return {name: first.get(name, 0) + second.get(name, 0) for name in first.keys() | second.keys()}
