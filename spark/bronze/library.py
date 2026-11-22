"""The reading of a case file with the publisher's library (``vitaldb``), in passes that hold few tracks.

``VitalFile.load_vital`` streams the gzip file packet by packet but keeps every decoded record of the selected tracks in memory. So the file is read
in passes: a read of the header alone lists the tracks, then the passes of ``spark/bronze/plan.py`` read the tracks with ``track_names=[...]``. The
default reads the numeric tracks of a case in one pass and each waveform track in a pass of its own, and the other mode reads every track by itself.
The records of a track are turned into rows as Spark takes them and freed before the next track's rows are made. The most that one worker holds is a
pass's records: the largest waveform track of the largest file, or all the numeric tracks of the case.

The library can select more tracks than the one named (it matches a name by its ending). Only the exact track is used, and the others are dropped with the
library object. The library never says that it stopped at an error: ``load_vital`` returns ``False`` and a cut file returns what it had read. That is why
``case_problem`` judges a file before any row is made (``spark/bronze/structure.py``) and why every pass checks the library's answer.

A numeric or text reading's case time is the record time minus the anonymised start, in whole microseconds, as the replay's decoder
(``replay/service/decode.py``) makes it, so a row and the replay's event for the same reading have the same key. A waveform sample's second is placed
from the exact encoded values and not from a rounded start (``spark/bronze/sampling.py``). The numeric tracks of the four-byte integer formats (codes
7 and 8) are read by the packet walk with fixed-width little-endian formats, because the library unpacks eight bytes from each four-byte value.
"""

from __future__ import annotations

import gc
from collections.abc import Collection, Iterator
from pathlib import Path
from typing import Any

import vitaldb

from replay.service.decode import ANONYMISED_START, samples_of, to_microseconds
from spark.bronze.plan import GROUPED, track_passes
from spark.bronze.rows import Lineage, StructureError, numeric_rows, string_rows, track_info, track_row, waveform_rows
from spark.bronze.structure import (
    END_TOLERANCE_SECONDS,
    NUMERIC,
    OWN_FORMATS,
    Walk,
    recording_problem,
    records_problem,
    tracks_problem,
    walk_file,
)

READS = {"header": 0, "load": 0, "walk": 0}


def read_counts() -> dict[str, int]:
    """The reads of this process so far. A function, so that a Spark worker takes the counts from this module and not from a copy of them."""
    return dict(READS)


def load(path: Path, names: list[str] | None = None, *, header_only: bool = False) -> Any:
    """The library's reading of the file: all tracks (``names`` None) or the named ones, records included unless ``header_only``. A failed read is refused.

    ``READS`` counts the reads of this process (the header alone, with records, and the packet walk), so a run can say how often it read its files.
    """
    READS["header" if header_only else "load"] += 1
    library = vitaldb.VitalFile()
    library.dtstart = 0
    library.dgmt = 0
    if not library.load_vital(str(path), track_names=names, header_only=header_only):
        raise StructureError("the library could not read the case file to its end")
    return library


def walk(path: Path, values_for: Collection[str] = ()) -> Walk:
    """The packet walk of the file (``spark/bronze/structure.py``), counted among the reads of this process."""
    READS["walk"] += 1
    return walk_file(path, values_for)


def own_numeric_names(header: Any) -> set[str]:
    """The numeric tracks of a four-byte integer format, which the library cannot read (it unpacks eight bytes from a four-byte value): the walk reads them."""
    return {name for name, track in header.trks.items() if track.type == NUMERIC and track.fmt in OWN_FORMATS}


def counted_problem(path: Path, case_id: int, walked: Walk, mode: str) -> str | None:
    """Why what the library returns for the tracks of the file is not what the walk counted, or ``None``. Each track's records are freed when it is judged."""
    header = load(path, header_only=True)
    own = own_numeric_names(header)
    for kind in ("numeric", "string", "waveform"):
        types = {name: track.track_type for name, track in walked.tracks.items() if name not in own}
        for names in track_passes(types, kind, mode):
            library = library_group(path, case_id, names)
            for name in names:
                track = library.trks[name]
                times = None if kind == "waveform" else [record["dt"] for record in track.recs]
                found = records_problem(
                    case_id, name, walked.tracks[name].count, None if kind == "waveform" else walked.tracks[name].times, len(track.recs), times
                )
                track.recs.clear()
                if found:
                    return found
            del library
            gc.collect()
    return None


def case_problem(path: Path, case_id: int, caseend_us: int, mode: str = GROUPED) -> str | None:
    """Why the file is not a complete recording of case ``case_id``, or ``None``: the walk, the library's header and its records against the walk's counts.

    Nothing is written for a case that fails here. The records are read by the same passes as the decode and dropped, so the check holds what one pass holds.
    """
    walked = walk(path)
    if walked.problem is not None:
        return f"case {case_id}: {walked.problem}"
    try:
        library = load(path, header_only=True)
    except StructureError:
        return f"case {case_id}: the library could not read the file to its end"
    problem = recording_problem(
        case_id,
        loaded=True,
        track_types=[track.type for track in library.trks.values()],
        dtstart=library.dtstart,
        dtend=library.dtend,
        caseend_us=caseend_us,
        anonymised_start=ANONYMISED_START,
        tolerance_s=END_TOLERANCE_SECONDS,
    )
    if problem is not None:
        return problem
    problem = tracks_problem(case_id, walked.tracks, library.trks)
    if problem is not None:
        return problem
    del library
    return counted_problem(path, case_id, walked, mode)


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
        records = [(float(record["dt"]), samples_of(record["val"])) for record in track.recs]
        track.recs.clear()
        yield from waveform_rows(case_id, name, float(track.srate), records, ANONYMISED_START, lineage)


def case_rows(path: Path, case_id: int, kind: str, lineage: Lineage, mode: str = GROUPED) -> Iterator[tuple[Any, ...]]:
    """The rows of one kind (``numeric``, ``string``, ``waveform`` or ``tracks``) of every track of the case, in the passes of ``mode``.

    The tracks come in name order, except that the numeric tracks of a four-byte integer format come last: the library cannot read them, so the walk does.
    """
    header = load(path, header_only=True)
    if kind == "tracks":
        for name in sorted(header.trks):
            track = header.trks[name]
            yield track_row(case_id, track_info(name, track, header.devs.get(track.dname)), lineage)
        return
    types = {name: track.type for name, track in header.trks.items()}
    own = own_numeric_names(header) if kind == "numeric" else set()
    del header
    owned: dict[str, list[tuple[int, float, float]]] = {}
    if own:
        walked = walk(path, own)
        if walked.problem is not None:
            raise StructureError(f"case {case_id}: {walked.problem}")
        owned = {name: [(to_microseconds(time), time, value) for time, value in walked.tracks[name].values or []] for name in own}
    for names in track_passes({name: track_type for name, track_type in types.items() if name not in own}, kind, mode):
        library = library_group(path, case_id, names)
        for name in names:
            yield from track_rows(case_id, name, library.trks[name], kind, lineage)
        del library
        gc.collect()
    for name in sorted(own):
        yield from numeric_rows(case_id, name, owned.pop(name), lineage)
