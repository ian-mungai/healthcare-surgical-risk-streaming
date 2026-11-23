"""What Bronze refuses: structural corruption of a case file, and nothing else.

Bronze keeps every published value, so it refuses a file only when the file is not a complete, readable case file. The publisher's library ends a read at the
first error and still returns what it had read, and it skips a record that it cannot place without a word, so a cut file looks like a short recording and a
damaged record is a reading that is not there. Three checks stand in front of the decoder, and the job runs all of them for every case before the branch exists:

1. ``walk_file`` reads the whole gzip stream of the file once, packet by packet, with its own reading of the packets' framing. A stream that ends
   early, a checksum that does not match, a file that is not gzip, a stream that does not start with the mark ``VITA``, a packet that is cut short, a
   packet over the library's limit of one million bytes and a track ID whose records are not as long as it declares are refused. So is a name or a
   text that is not valid UTF-8. So is a record whose declared lengths do not fit its packet, a record of a track that the file does not declare, a
   track of a type that Bronze does not carry and a sample format that the file format does not define. The walk counts the records of each track by
   its name, keeps the time of each numeric and text record, and reads the values of the tracks that it is asked to read itself.
2. ``recording_problem`` judges what the library read from the header of the file: it must have read the file, the file must hold a track, every track must
   be of a type that Bronze carries, the recording must start at the anonymised start and must end within a couple of seconds of the clinical end of the case.
   The published recordings end within a second of the whole-second clinical end, which is why the tolerance is not zero.
3. ``tracks_problem`` and ``records_problem`` compare what the library returned with what the walk counted: the same tracks, and for each track the same number
   of records, and for a numeric or text track the same times in the same order.

A refusal is a message that names the case and the cause, never a reading and never a path.
"""

from __future__ import annotations

import gzip
import struct
import zlib
from array import array
from collections import defaultdict
from collections.abc import Collection, Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

END_TOLERANCE_SECONDS = 2.0
CARRIED_TYPES = (1, 2, 5)
WAVEFORM, NUMERIC, TEXT = CARRIED_TYPES
MARK = b"VITA"
CUT_SHORT = "the file ends inside its header or inside a packet, so it is cut short"
PACKET_HEAD_BYTES = 5
DEVICE_PACKET, TRACK_PACKET, RECORD_PACKET = 9, 0, 1
RECORD_TIME_START, RECORD_TRACK_START, RECORD_FIXED_BYTES = 2, 10, 12
INFO_FIXED_BYTES = 10
MAX_PACKET_BYTES = 1_000_000
PACKED_FLAG_AT = 26
SAMPLE_BYTES = {1: 4, 2: 8, 3: 1, 4: 1, 5: 2, 6: 2, 7: 4, 8: 4}
OWN_FORMATS = {7: "<i", 8: "<I"}


@dataclass(slots=True)
class WalkedTrack:
    """One track of a file as the walk found it, by the library's name for it. ``times`` holds the file's own time of each numeric and text record.

    ``values`` holds the ``(time, value)`` of each record when the walk was asked to read the track's values itself, and is ``None`` otherwise.
    """

    track_type: int
    fmt: int
    count: int = 0
    times: array[float] = field(default_factory=lambda: array("d"))
    values: list[tuple[float, float]] | None = None


@dataclass(slots=True)
class Walk:
    problem: str | None = None
    tracks: dict[str, WalkedTrack] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TrackHead:
    tid: int
    track_type: int
    fmt: int
    name: str
    did: int
    declared: int


def read_exactly(stream: gzip.GzipFile, size: int) -> bytes | None:
    """``size`` bytes, or ``None`` when the stream ends first."""
    data = stream.read(size)
    return data if len(data) == size else None


def text_at(body: bytes, position: int) -> tuple[str, int]:
    """The text of the file format (four bytes of length, then the bytes) at ``position`` and the position after it.

    A text past the packet is refused, and so is one that is not valid UTF-8: the library decodes with ``errors="ignore"`` and would drop the bytes.
    """
    (size,) = struct.unpack_from("<I", body, position)
    if position + 4 + size > len(body):
        raise struct.error("a text runs past its packet")
    return body[position + 4 : position + 4 + size].decode("utf-8"), position + 4 + size


def parse_track(body: bytes) -> TrackHead:
    """The fields of a track-information packet that the walk needs. The packet's later fields are optional, as the library reads them."""
    tid, track_type, fmt = struct.unpack_from("<HBB", body)
    name, position = text_at(body, 4)
    for width in (None, 4, 4, 4, 4, 8, 8, 1):
        if position >= len(body):
            return TrackHead(tid, track_type, fmt, name, 0, 0)
        position = text_at(body, position)[1] if width is None else position + width
    did = struct.unpack_from("<I", body, position)[0] if len(body) > position else 0
    declared = struct.unpack_from("<I", body, position + 4)[0] if len(body) > position + 4 else 0
    return TrackHead(tid, track_type, fmt, name, did, declared)


def device_name(body: bytes) -> tuple[int, str]:
    """The ID of a device-information packet and the name that the library gives the device: its own name, or its type when it has none."""
    (did,) = struct.unpack_from("<I", body)
    devtype, position = text_at(body, 4)
    name = text_at(body, position)[0]
    return did, name or devtype


def head_problem(head: TrackHead) -> str | None:
    if head.track_type not in CARRIED_TYPES:
        return "a track is of a type that Bronze does not carry"
    if head.track_type != TEXT and head.fmt not in SAMPLE_BYTES:
        return "a track has a sample format that the file format does not define"
    if head.track_type == WAVEFORM and head.fmt in OWN_FORMATS:
        return "a waveform track has a four-byte integer sample format, which the library cannot read"
    return None


def record_problem(track: WalkedTrack, body: bytes) -> str | None:
    """Why a record packet cannot be a record of its track, judged from the lengths that it declares and the track's format alone, or ``None``."""
    (infolen,) = struct.unpack_from("<H", body)
    if infolen < INFO_FIXED_BYTES or 2 + infolen > len(body):
        return "a record's info block is shorter than its time and track or runs past its packet"
    size = len(body) - 2 - infolen
    payload = body[2 + infolen :]
    if track.track_type == NUMERIC and size != SAMPLE_BYTES[track.fmt]:
        return "a numeric record's value is not as long as its track's format"
    if track.track_type == TEXT:
        if size < 8 or 8 + struct.unpack_from("<I", payload, 4)[0] > size:
            return "a text record's text is not inside its packet"
        try:
            payload[8 : 8 + struct.unpack_from("<I", payload, 4)[0]].decode("utf-8")
        except UnicodeDecodeError:
            return "a text record's text is not valid UTF-8, and the library would drop the bytes that are not"
    if track.track_type == WAVEFORM and (size < 4 or 4 + struct.unpack_from("<I", payload)[0] * SAMPLE_BYTES[track.fmt] > size):
        return "a waveform record's samples are not inside its packet"
    return None


def walk_file(path: Path, values_for: Collection[str] = ()) -> Walk:
    """The walk of the file at ``path``: why it is not a complete case file (``problem``) and what it holds (``tracks``).

    The stream must be whole, start with the mark ``VITA``, have its header and end on the end of a packet, and no packet may be over the library's limit
    (a non-packed file). A track ID that declares the length of its records must have exactly that many bytes of record packets, each ID by its own declaration
    even when two share a name. A track that declares none is not checked. A file that
    is cut at the end of a packet and declares no lengths cannot be told from a shorter recording by this check: the publisher's checksum is what stops it.
    The tracks in ``values_for`` have the value of each record read here, which only the numeric formats that the library cannot read allow.
    """
    walk = Walk()
    devices: dict[int, str] = {}
    by_tid: dict[int, str] = {}
    declared: dict[int, int] = {}
    held: defaultdict[int, int] = defaultdict(int)
    packed = False
    try:
        with gzip.open(path, "rb") as stream:
            if stream.read(len(MARK)) != MARK:
                walk.problem = "the file does not start with the mark VITA of a case file"
                return walk
            sizes = read_exactly(stream, 6)
            header = read_exactly(stream, struct.unpack("<xxxxH", sizes)[0]) if sizes else None
            if header is None:
                walk.problem = CUT_SHORT
                return walk
            packed = len(header) > PACKED_FLAG_AT and header[PACKED_FLAG_AT] == 1
            while head := stream.read(PACKET_HEAD_BYTES):
                if len(head) < PACKET_HEAD_BYTES:
                    walk.problem = CUT_SHORT
                    return walk
                kind, length = struct.unpack("<BI", head)
                if length > MAX_PACKET_BYTES and not packed:
                    walk.problem = "a packet is over the library's limit of one million bytes, so the library stops reading there and drops what follows"
                    return walk
                body = read_exactly(stream, length)
                if body is None:
                    walk.problem = CUT_SHORT
                    return walk
                if kind == DEVICE_PACKET:
                    did, name = device_name(body)
                    devices[did] = name
                elif kind == TRACK_PACKET:
                    track = parse_track(body)
                    walk.problem = head_problem(track)
                    if walk.problem:
                        return walk
                    name = f"{devices[track.did]}/{track.name}" if track.did and track.did in devices else track.name
                    known = walk.tracks.get(name)
                    if known is not None and (known.track_type, known.fmt) != (track.track_type, track.fmt):
                        walk.problem = "two tracks have one name and another type or sample format"
                        return walk
                    walk.tracks.setdefault(
                        name, WalkedTrack(track.track_type, track.fmt, values=[] if name in values_for and track.fmt in OWN_FORMATS else None)
                    )
                    by_tid[track.tid] = name
                    declared[track.tid] = track.declared
                elif kind == RECORD_PACKET:
                    if len(body) < RECORD_FIXED_BYTES:
                        walk.problem = "a record packet is too short to name its track"
                        return walk
                    (tid,) = struct.unpack_from("<H", body, RECORD_TRACK_START)
                    name = by_tid.get(tid, "")
                    if not name:
                        walk.problem = "a record names a track that the file does not declare before it"
                        return walk
                    track_walked = walk.tracks[name]
                    walk.problem = record_problem(track_walked, body)
                    if walk.problem:
                        return walk
                    held[tid] += PACKET_HEAD_BYTES + length
                    track_walked.count += 1
                    (time,) = struct.unpack_from("<d", body, RECORD_TIME_START)
                    if track_walked.track_type != WAVEFORM:
                        track_walked.times.append(time)
                    if track_walked.values is not None:
                        (value,) = struct.unpack_from(OWN_FORMATS[track_walked.fmt], body, len(body) - SAMPLE_BYTES[track_walked.fmt])
                        track_walked.values.append((time, float(value)))
    except OSError, EOFError, zlib.error:
        walk.problem = "the file is not a complete gzip stream: it is cut short, damaged or not a gzip file"
        return walk
    except struct.error:
        walk.problem = "a packet is too short for the fields that its type holds"
        return walk
    except UnicodeDecodeError:
        walk.problem = "a device or track name is not valid UTF-8, and the library would drop the bytes that are not"
        return walk
    if any(size and held[tid] != size for tid, size in declared.items()):
        walk.problem = "a track has records of another length than it declares"
    return walk


def tracks_problem(case_id: int, walked: Iterable[str], read: Iterable[str]) -> str | None:
    """Why the tracks that the library read are not the tracks that the walk found, or ``None``. A track is named by its place in the sorted names."""
    missing = sorted(set(walked) - set(read))
    extra = sorted(set(read) - set(walked))
    if missing:
        return f"case {case_id}: the file declares the track {missing[0]} and the library did not read it"
    if extra:
        return f"case {case_id}: the library read the track {extra[0]}, which the file does not declare"
    return None


def records_problem(
    case_id: int, name: str, walked_count: int, walked_times: Sequence[float] | None, read_count: int, read_times: Sequence[float] | None
) -> str | None:
    """Why what the library returned for a track is not what the walk counted, or ``None``. Times are compared for the tracks that have them."""
    if walked_count != read_count:
        return f"case {case_id}: the track {name} has {walked_count} records in the file and the library returned {read_count}"
    if walked_times is not None and read_times is not None and list(walked_times) != list(read_times):
        return f"case {case_id}: the track {name} has the same number of records ({walked_count}) as the library returned and another time for one of them"
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
