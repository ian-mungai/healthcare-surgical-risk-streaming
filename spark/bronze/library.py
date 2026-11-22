"""The reading of a case file with the publisher's library (``vitaldb``), one track at a time.

``VitalFile.load_vital`` streams the gzip file packet by packet but keeps every decoded record of the selected tracks in memory. So the file is read in passes:
a read of the header alone lists the tracks, then each track is read by itself with ``track_names=[name]``, turned into rows as Spark takes them and freed
before the next track is read. The most that one worker holds is the largest single track of the largest file.

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
from replay.service.decode import ANONYMISED_START, NUMERIC, STRING, WAVEFORM, samples_of, to_microseconds
from spark.bronze.rows import Lineage, StructureError, numeric_rows, string_rows, track_info, track_row, waveform_rows
from spark.bronze.structure import END_TOLERANCE_SECONDS, file_problem, recording_problem

TYPE_OF_KIND = {"numeric": NUMERIC, "string": STRING, "waveform": WAVEFORM}


def load(path: Path, names: list[str] | None = None, *, header_only: bool = False) -> Any:
    """The library's reading of the file: all tracks (``names`` None) or the named ones, records included unless ``header_only``. A failed read is refused."""
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


def library_track(path: Path, case_id: int, name: str) -> Any:
    """The library's reading of the one track ``name``, with its records. The other tracks that the library may select are dropped."""
    library = load(path, [name])
    track = library.trks.get(name)
    if track is None:
        raise StructureError(f"case {case_id}: the track {name} is in the header and not in the pass that reads it")
    for other in [other for other in library.trks if other != name]:
        del library.trks[other]
    return library


def track_rows(path: Path, case_id: int, name: str, kind: str, lineage: Lineage) -> Iterator[tuple[Any, ...]]:
    """The rows of one track, read by itself. The track's records are freed when its rows have been taken."""
    library = library_track(path, case_id, name)
    track = library.trks[name]
    if kind == "numeric":
        yield from numeric_rows(case_id, name, [(to_microseconds(record["dt"]), float(record["dt"]), record["val"]) for record in track.recs], lineage)
    elif kind == "string":
        yield from string_rows(case_id, name, [(to_microseconds(record["dt"]), float(record["dt"]), str(record["val"])) for record in track.recs], lineage)
    else:
        records = tuple(WaveformRecord(to_microseconds(record["dt"]), samples_of(record["val"])) for record in track.recs)
        track.recs.clear()
        waveform = WaveformTrack(name, str(track.unit), float(track.srate), float(track.gain), float(track.offset), records)
        yield from waveform_rows(case_id, waveform, lineage)
    del library, track
    gc.collect()


def case_rows(path: Path, case_id: int, kind: str, lineage: Lineage) -> Iterator[tuple[Any, ...]]:
    """The rows of one kind (``numeric``, ``string``, ``waveform`` or ``tracks``) of every track of the case, tracks in name order, one track at a time."""
    header = load(path, header_only=True)
    names = sorted(header.trks)
    if kind == "tracks":
        for name in names:
            track = header.trks[name]
            yield track_row(case_id, track_info(name, track, header.devs.get(track.dname)), lineage)
        return
    for name in names:
        if TYPE_OF_KIND[kind] == header.trks[name].type:
            yield from track_rows(path, case_id, name, kind, lineage)
