"""Decode a verified ``.vital`` case file into the event model's recording, with the publisher's library (``vitaldb``).

The file is read from a path that the caller has already checked against the manifest's SHA-256. The decoder checks the file against the clinical times too: the
recording's clock starts at the publisher's anonymised start (2100-01-01 00:00:00 UTC), so a case time is the record time minus that instant, and the recording
ends within a couple of seconds of the clinical ``caseend``. A file that fails either check, or that the library stopped reading half way, is refused by name.

The library keeps a numeric reading as a float or an integer, a waveform record as an array of raw samples and a string event as a text, each with the record's
time as seconds since the epoch. The decoder keeps the order of the records, which the discriminators depend on.
"""

from __future__ import annotations

import csv
import io
from array import array
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

import vitaldb

from replay.events.recording import CaseRecording, CaseTimes, NumericTrack, RecordingError, StringTrack, WaveformRecord, WaveformTrack

ANONYMISED_START = 4102444800.0
END_TOLERANCE_SECONDS = 2.0
WAVEFORM, NUMERIC, STRING = 1, 2, 5
TYPECODES = {"B": "B", "b": "b", "H": "H", "h": "h", "I": "I", "i": "i", "f": "f", "d": "d"}
CLINICAL_COLUMNS = ("caseid", "casestart", "opstart", "opend", "caseend")


class DecodeError(RuntimeError):
    """A case file or the clinical table cannot be decoded, or does not fit its clinical times."""


def to_microseconds(seconds_since_epoch: float) -> int:
    return round((seconds_since_epoch - ANONYMISED_START) * 1_000_000)


def clinical_times(table: bytes, cases: Iterable[int]) -> dict[int, CaseTimes]:
    """The ``opstart``, ``opend`` and ``caseend`` of each wanted case, from the verified clinical table, in microseconds from the case start."""
    wanted = set(cases)
    reader = csv.DictReader(io.StringIO(table.decode("utf-8-sig"), newline=""))
    if reader.fieldnames is None or any(reader.fieldnames.count(name) != 1 for name in CLINICAL_COLUMNS):
        raise DecodeError(f"the clinical table does not have each of the columns {', '.join(CLINICAL_COLUMNS)} once")
    found: dict[int, CaseTimes] = {}
    for row in reader:
        if not row["caseid"].isdigit() or int(row["caseid"]) not in wanted:
            continue
        case = int(row["caseid"])
        try:
            start, opstart, opend, caseend = (int(row[name]) for name in CLINICAL_COLUMNS[1:])
        except ValueError:
            raise DecodeError(f"case {case}: the clinical table has no whole-second value for casestart, opstart, opend or caseend") from None
        if start != 0:
            raise DecodeError(f"case {case}: the case starts at {start} seconds, not at 0, so its times are not the recording's times")
        if case in found:
            raise DecodeError(f"case {case} is listed twice in the clinical table")
        try:
            found[case] = CaseTimes(case, opstart * 1_000_000, opend * 1_000_000, caseend * 1_000_000)
        except RecordingError as error:
            raise DecodeError(str(error)) from None
    missing = sorted(wanted - set(found))
    if missing:
        raise DecodeError(f"the clinical table has no row for case {missing[0]}" + (f" and {len(missing) - 1} more" if len(missing) > 1 else ""))
    return found


def samples_of(raw: Any) -> Sequence[float]:
    """The raw samples of a waveform record as a compact typed array. A type with no typed array is kept as doubles."""
    code = TYPECODES.get(raw.dtype.char)
    if code is not None and raw.dtype.itemsize == array(code).itemsize:
        return array(code, raw.tobytes())
    return array("d", raw.astype("float64").tobytes())


def decode_case(path: Path, times: CaseTimes) -> CaseRecording:
    """The recording of a verified case file, or a ``DecodeError`` that names the case and the reason."""
    case = times.case_id
    library = vitaldb.VitalFile(str(path))
    if not library.trks:
        raise DecodeError(f"case {case}: the file holds no track")
    if library.dtstart != ANONYMISED_START:
        raise DecodeError(f"case {case}: the recording does not start at the anonymised start")
    if abs((library.dtend - ANONYMISED_START) * 1_000_000 - times.caseend_us) > END_TOLERANCE_SECONDS * 1_000_000:
        raise DecodeError(
            f"case {case}: the recording does not end within {END_TOLERANCE_SECONDS:g} seconds of the clinical caseend, so it was not read to its end"
        )
    numeric: list[NumericTrack] = []
    strings: list[StringTrack] = []
    waveforms: list[WaveformTrack] = []
    for name, track in sorted(library.trks.items()):
        if track.type == NUMERIC:
            numeric.append(NumericTrack(name, track.unit, tuple((to_microseconds(record["dt"]), float(record["val"])) for record in track.recs)))
        elif track.type == STRING:
            strings.append(StringTrack(name, tuple((to_microseconds(record["dt"]), str(record["val"])) for record in track.recs)))
        elif track.type == WAVEFORM:
            records = tuple(WaveformRecord(to_microseconds(record["dt"]), samples_of(record["val"])) for record in track.recs)
            waveforms.append(WaveformTrack(name, track.unit, float(track.srate), float(track.gain), float(track.offset), records))
        else:
            raise DecodeError(f"case {case}: the track {name} has a type that the replay does not carry")
    try:
        return CaseRecording(times, tuple(numeric), tuple(strings), tuple(waveforms))
    except RecordingError as error:
        raise DecodeError(str(error)) from None
