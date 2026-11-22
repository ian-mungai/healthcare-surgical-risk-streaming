"""The reading of a case file with the publisher's library (``vitaldb``), in passes that hold few tracks.

``VitalFile.load_vital`` streams the gzip file packet by packet but keeps every decoded record of the selected tracks in memory. So the file is read
in passes: a read of the header alone lists the tracks, then the passes of ``spark/bronze/plan.py`` read the tracks with ``track_names=[...]``. The
default reads the numeric tracks of a case in one pass and each waveform track in a pass of its own, and the other mode reads every track by itself.
The records of a track are turned into rows as Spark takes them and freed before the next track's rows are made. The most that one worker holds is a
pass's records: the largest waveform track of the largest file, or all the numeric tracks of the case.

The library can select more tracks than the one named (it matches a name by its ending). Only the exact track is used, and the others are dropped with the
library object. The library never says that it stopped at an error: ``load_vital`` returns ``False`` and a cut file returns what it had read. That is why
``case_problem`` judges a file before any row is made (``spark/bronze/structure.py``) and why every pass checks the library's answer.

A case time is the record time minus the anonymised start, in whole microseconds, as the replay's decoder (``replay/service/decode.py``) makes it, so a row
and the replay's event for the same reading have the same key.
"""

from __future__ import annotations

import gc
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import vitaldb

from replay.events.recording import WaveformRecord, WaveformTrack
from replay.service.decode import ANONYMISED_START, samples_of, to_microseconds
from spark.bronze.plan import GROUPED, track_passes
from spark.bronze.rows import Lineage, StructureError, numeric_rows, string_rows, track_info, track_row, waveform_rows
from spark.bronze.structure import END_TOLERANCE_SECONDS, file_problem, recording_problem

READS = {"header": 0, "load": 0}


def load(path: Path, names: list[str] | None = None, *, header_only: bool = False) -> Any:
    """The library's reading of the file: all tracks (``names`` None) or the named ones, records included unless ``header_only``. A failed read is refused.

    ``READS`` counts the reads of this process, a read of the header alone and a read with records, so that a run can say how many times it read its files.
    """
    READS["header" if header_only else "load"] += 1
    library = vitaldb.VitalFile()
    library.dtstart = 0
    library.dgmt = 0
    if not library.load_vital(str(path), track_names=names, header_only=header_only):
        raise StructureError("the library could not read the case file to its end")
    return library


def case_problem(path: Path, case_id: int, caseend_us: int) -> str | None:
    """Why the file is not a complete recording of case ``case_id`` (the structural check of ``spark/bronze/structure.py``), or ``None``."""
    problem = file_problem(path)
    if problem is not None:
        return f"case {case_id}: {problem}"
    try:
        library = load(path, header_only=True)
    except StructureError:
        return f"case {case_id}: the library could not read the file to its end"
    return recording_problem(
        case_id,
        loaded=True,
        track_types=[track.type for track in library.trks.values()],
        dtstart=library.dtstart,
        dtend=library.dtend,
        caseend_us=caseend_us,
        anonymised_start=ANONYMISED_START,
        tolerance_s=END_TOLERANCE_SECONDS,
    )


def library_group(path: Path, case_id: int, names: list[str]) -> Any:
    """The library's reading of the tracks ``names`` in one pass, with their records. The other tracks that the library may select are dropped."""
    library = load(path, names)
    missing = [name for name in names if name not in library.trks]
    if missing:
        raise StructureError(f"case {case_id}: the track {missing[0]} is in the header and not in the pass that reads it")
    for other in [other for other in library.trks if other not in names]:
        del library.trks[other]
    return library


def track_rows(case_id: int, name: str, track: Any, kind: str, lineage: Lineage) -> Iterator[tuple[Any, ...]]:
    """The rows of one track that the library read. The track's records are freed as soon as its rows are made from them."""
    if kind == "numeric":
        readings = [(to_microseconds(record["dt"]), float(record["dt"]), record["val"]) for record in track.recs]
        track.recs.clear()
        yield from numeric_rows(case_id, name, readings, lineage)
    elif kind == "string":
        events = [(to_microseconds(record["dt"]), float(record["dt"]), str(record["val"])) for record in track.recs]
        track.recs.clear()
        yield from string_rows(case_id, name, events, lineage)
    else:
        records = tuple(WaveformRecord(to_microseconds(record["dt"]), samples_of(record["val"])) for record in track.recs)
        track.recs.clear()
        waveform = WaveformTrack(name, str(track.unit), float(track.srate), float(track.gain), float(track.offset), records)
        yield from waveform_rows(case_id, waveform, lineage)


def case_rows(path: Path, case_id: int, kind: str, lineage: Lineage, mode: str = GROUPED) -> Iterator[tuple[Any, ...]]:
    """The rows of one kind (``numeric``, ``string``, ``waveform`` or ``tracks``) of every track of the case, in name order, in the passes of ``mode``."""
    header = load(path, header_only=True)
    if kind == "tracks":
        for name in sorted(header.trks):
            track = header.trks[name]
            yield track_row(case_id, track_info(name, track, header.devs.get(track.dname)), lineage)
        return
    types = {name: track.type for name, track in header.trks.items()}
    del header
    for names in track_passes(types, kind, mode):
        library = library_group(path, case_id, names)
        for name in names:
            yield from track_rows(case_id, name, library.trks[name], kind, lineage)
        del library
        gc.collect()
