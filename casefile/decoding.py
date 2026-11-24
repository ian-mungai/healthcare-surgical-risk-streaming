"""The decoding of a case file that the replay's decoder and the Bronze decoder share: pure code, no library, no file.

The replay (``replay/service/decode.py``) and Bronze (``spark/bronze/``) each read a ``.vital`` file with the publisher's library, and both must read the same
values from it. This module holds what they share: the track type codes, the end tolerance, a record time in whole microseconds from the case start, the
clinical table's times and a waveform record's samples as a compact array. A waveform packet's start is not rounded here: ``features/sampling.py`` places each
sample in its second from the exact start. The read-only object stores of the same two jobs are ``casefile/folder_store.py`` and ``casefile/s3_store.py``.
"""

from __future__ import annotations

import csv
import io
from array import array
from collections.abc import Iterable, Sequence
from typing import Any

from features.sampling import ANONYMISED_START
from replay.events.recording import CaseTimes, RecordingError

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
