"""Decode a verified ``.vital`` case file into the event model's recording, with the publisher's library (``vitaldb``).

The file is read from a path that the caller has already checked against the manifest's SHA-256. The decoder checks the file against the clinical times too: the
recording's clock starts at the publisher's anonymised start (2100-01-01 00:00:00 UTC), so a case time is the record time minus that instant, and the recording
ends within a couple of seconds of the clinical ``caseend``. A file that fails either check, or that the library stopped reading half way, is refused by name.

The library keeps a numeric reading as a float or an integer, a waveform record as an array of raw samples and a string event as a text, each with the record's
time as seconds since the epoch. The decoder keeps the order of the records, which the discriminators depend on. A numeric reading or a string event has its
time rounded to whole microseconds from the case start, as Bronze rounds it. A waveform packet keeps its start as the file holds it, with no rounding, because
the second of each sample depends on the exact value (``features/sampling.py``).
"""

from __future__ import annotations

from pathlib import Path

import vitaldb

from casefile.decoding import END_TOLERANCE_SECONDS, NUMERIC, STRING, WAVEFORM, DecodeError, samples_of, to_microseconds
from features.sampling import ANONYMISED_START
from replay.events.recording import CaseRecording, CaseTimes, NumericTrack, RecordingError, StringTrack, WaveformRecord, WaveformTrack


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
            records = tuple(WaveformRecord(float(record["dt"]), samples_of(record["val"])) for record in track.recs)
            waveforms.append(WaveformTrack(name, track.unit, float(track.srate), float(track.gain), float(track.offset), int(track.fmt), records))
        else:
            raise DecodeError(f"case {case}: the track {name} has a type that the replay does not carry")
    try:
        return CaseRecording(times, tuple(numeric), tuple(strings), tuple(waveforms))
    except RecordingError as error:
        raise DecodeError(str(error)) from None
