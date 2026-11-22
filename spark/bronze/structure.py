"""What Bronze refuses: structural corruption of a case file, and nothing else.

Bronze keeps every published value, so it refuses a file only when the file is not a complete case file. The publisher's library ends a read at the first
error and still returns what it had read, so a cut file looks like a short recording. Two checks stand in front of the decoder:

1. ``file_problem`` reads the whole gzip stream of the file once, packet by packet. A stream that ends early, a checksum that does not match, a file that is not
   gzip, a stream that does not start with the mark ``VITA`` of a case file, a packet that is cut short and a track whose records are not as long as it declares
   are refused. A recording that was cut and compressed again is cut inside a packet unless the cut falls on a packet's end.
2. ``recording_problem`` judges what the library read from the header of the file: it must have read the file, the file must hold a track, every track must
   be of a type that Bronze carries, the recording must start at the anonymised start and must end within a couple of seconds of the clinical end of the case.
   The published recordings end within a second of the whole-second clinical end, which is why the tolerance is not zero.

A refusal is a message that names the case and the cause, never a reading and never a path.
"""

from __future__ import annotations

import gzip
import struct
import zlib
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path

END_TOLERANCE_SECONDS = 2.0
CARRIED_TYPES = (1, 2, 5)
MARK = b"VITA"
CUT_SHORT = "the file ends inside its header or inside a packet, so it is cut short"
PACKET_HEAD_BYTES = 5
TRACK_PACKET, RECORD_PACKET = 0, 1
TRACK_FIXED_BYTES = 4 + 4 + 4 + 4 + 8 + 8 + 1 + 4
RECORD_TRACK_START, RECORD_TRACK_END = 10, 12


def read_exactly(stream: gzip.GzipFile, size: int) -> bytes | None:
    """``size`` bytes, or ``None`` when the stream ends first."""
    data = stream.read(size)
    return data if len(data) == size else None


def declared_length(body: bytes) -> int:
    """The length of its records that a track-information packet declares, or 0 when it declares none. The packet's fields follow the file format in order."""
    position = 4
    for _ in range(2):
        (size,) = struct.unpack_from("<I", body, position)
        position += 4 + size
    position += TRACK_FIXED_BYTES
    return struct.unpack_from("<I", body, position)[0] if len(body) >= position + 4 else 0


def file_problem(path: Path) -> str | None:
    """Why the file at ``path`` is not a complete case file, or ``None``. It reads the whole gzip stream once, packet by packet.

    The stream must be whole, start with the mark ``VITA``, have its header and end on the end of a packet. A track that declares the length of its records must
    have exactly that many bytes of record packets. A track that declares none is not checked. A file that is cut at the end of a packet and declares no
    lengths cannot be told from a shorter recording by this check: the publisher's checksum is what stops it.
    """
    declared: dict[int, int] = {}
    held: defaultdict[int, int] = defaultdict(int)
    try:
        with gzip.open(path, "rb") as stream:
            lead = stream.read(len(MARK))
            if lead != MARK:
                return "the file does not start with the mark VITA of a case file"
            sizes = read_exactly(stream, 6)
            header = read_exactly(stream, struct.unpack("<xxxxH", sizes)[0]) if sizes else None
            if header is None:
                return CUT_SHORT
            while head := stream.read(PACKET_HEAD_BYTES):
                if len(head) < PACKET_HEAD_BYTES:
                    return CUT_SHORT
                kind, length = struct.unpack("<BI", head)
                body = read_exactly(stream, length)
                if body is None:
                    return CUT_SHORT
                if kind == TRACK_PACKET:
                    declared[struct.unpack_from("<H", body)[0]] = declared_length(body)
                elif kind == RECORD_PACKET:
                    if len(body) < RECORD_TRACK_END:
                        return "a record packet is too short to name its track"
                    held[struct.unpack_from("<H", body, RECORD_TRACK_START)[0]] += PACKET_HEAD_BYTES + length
    except OSError, EOFError, zlib.error:
        return "the file is not a complete gzip stream: it is cut short, damaged or not a gzip file"
    except struct.error:
        return "a packet is too short for the fields that its type holds"
    if any(size and held[track] != size for track, size in declared.items()):
        return "a track has records of another length than it declares"
    return None


def recording_problem(
    case_id: int,
    *,
    loaded: bool,
    track_types: Sequence[int],
    dtstart: float,
    dtend: float,
    caseend_us: int,
    anonymised_start: float,
    tolerance_s: float,
) -> str | None:
    """Why what the library read from a file is not a recording of case ``case_id`` that Bronze can keep, or ``None`` when it is one."""
    if not loaded:
        return f"case {case_id}: the library could not read the file to its end"
    if not track_types:
        return f"case {case_id}: the file holds no track"
    if any(kind not in CARRIED_TYPES for kind in track_types):
        return f"case {case_id}: the file holds a track of a type that Bronze does not carry"
    if dtstart != anonymised_start:
        return f"case {case_id}: the recording does not start at the anonymised start"
    if abs((dtend - anonymised_start) * 1_000_000 - caseend_us) > tolerance_s * 1_000_000:
        return f"case {case_id}: the recording does not end within {tolerance_s:g} seconds of the clinical end of the case, so it was not read to its end"
    return None
