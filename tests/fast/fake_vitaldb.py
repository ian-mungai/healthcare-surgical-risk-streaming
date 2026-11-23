"""A stand-in for the publisher's library (``vitaldb`` 1.7.2) at the boundary of the decoder, for fast tests that drive the decoder's own functions.

``FakeVitalFile.load_vital`` is a port of ``VitalFile.load_vital`` of the installed library (``vitaldb/utils.py``, lines 1879 to 2210), reduced to the parts
that decide what the decoder sees: the match of a requested name against a track's name (lines 2059 to 2066: a name matches a track when it is the track's whole
name, or when the track's name ends with it), the packet limit of non-packed files (line 1932), the skip of a record that does not fit its packet and of a
record of a track that was not selected, the decode of a numeric value with the platform's native format code (line 2159: ``l`` and ``L`` are eight bytes on a
64-bit Linux, so a four-byte value fails to unpack, the read stops and ``False`` is returned), the text decode that drops invalid bytes (lines 34 to 44) and the
end of a read at an error. numpy is not in the tools environment, so a waveform record's samples are a small object with the attributes that the decoder reads.

Its limit: it is a port made by reading the library, so a difference that the port misses is caught only by the acceptance test, which runs the real library.
"""

from __future__ import annotations

import gzip
import struct
import sys
import types
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

FMT_TYPE_LEN = {1: ("f", 4), 2: ("d", 8), 3: ("b", 1), 4: ("B", 1), 5: ("h", 2), 6: ("H", 2), 7: ("l", 4), 8: ("L", 4)}


class FakeDtype:
    def __init__(self, char: str, itemsize: int) -> None:
        self.char = char
        self.itemsize = itemsize


class FakeSamples:
    """The samples of a waveform record, with the attributes of a numpy array that the decoder reads."""

    def __init__(self, data: bytes, char: str, itemsize: int) -> None:
        self.data = data
        self.dtype = FakeDtype(char, itemsize)

    def tobytes(self) -> bytes:
        return self.data

    def __len__(self) -> int:
        return len(self.data) // self.dtype.itemsize


class FakeTrack:
    def __init__(
        self,
        name: str,
        type: int,
        fmt: int,
        unit: str,
        srate: float,
        mindisp: float,
        maxdisp: float,
        col: int,
        montype: int,
        gain: float,
        offset: float,
        dname: str,
    ) -> None:
        self.name, self.type, self.fmt, self.unit, self.srate = name, type, fmt, unit, srate
        self.mindisp, self.maxdisp, self.col, self.montype, self.gain, self.offset, self.dname = mindisp, maxdisp, col, montype, gain, offset, dname
        self.recs: list[dict[str, Any]] = []


class FakeDevice:
    def __init__(self, name: str, devtype: str, port: str) -> None:
        self.name, self.type, self.port = name, devtype, port


def unpack_text(buf: bytes, pos: int) -> tuple[str, int]:
    (size,) = struct.unpack_from("<I", buf, pos)
    return buf[pos + 4 : pos + 4 + size].decode("utf-8", "ignore"), pos + 4 + size


class FakeVitalFile:
    drop_records: dict[str, int] = {}  # a test sets it to make the stand-in skip the nth record of a track, as the real library skips a record it cannot place
    shift_times: dict[
        str, int
    ] = {}  # a test sets it to give the nth record of a track another time than the file holds, as a record that is read wrongly would
    hidden_tracks: set[str] = set()  # a test sets it to make the stand-in not register a track that the file declares
    errors: list[str] = []  # the messages that the real library prints when a read ends at an error
    loads: list[tuple[str, list[str] | None, bool]] = []  # each read: the path, the names asked for, and whether it was a header-only read

    def __init__(self) -> None:
        self.devs: dict[str, FakeDevice] = {}
        self.trks: dict[str, FakeTrack] = {}
        self.dtstart: float = 0
        self.dtend: float = 0
        self.dgmt = 0
        self.packed = False

    def load_vital(self, ipath: str, track_names: Any = None, header_only: bool = False, exclude: Any = None, maxlen: Any = None) -> bool:
        requested = None if track_names is None else sorted(getattr(track_names, "names", track_names))
        FakeVitalFile.loads.append((ipath, requested, header_only))
        with gzip.open(ipath, "rb") as stream:
            raw = stream.read()
        if raw[:4] != b"VITA":
            return False
        headerlen = struct.unpack_from("<H", raw, 8)[0]
        header = raw[10 : 10 + headerlen]
        if headerlen >= 26:
            self.dtstart = struct.unpack_from("<d", header, 10)[0]
            self.dtend = struct.unpack_from("<d", header, 18)[0]
        self.packed = headerlen >= 27 and header[26] == 1
        position = 10 + headerlen
        tid_dtnames: dict[int, str] = {}
        did_dnames: dict[int, str] = {}
        seen: dict[str, int] = {}
        try:
            while position + 5 <= len(raw):
                kind, packet_len = raw[position], struct.unpack_from("<I", raw, position + 1)[0]
                position += 5
                if not self.packed and packet_len > 1000000:
                    break
                buf = raw[position : position + packet_len]
                position += packet_len
                if buf == b"":
                    break
                if kind == 9:
                    (did,) = struct.unpack_from("<I", buf, 0)
                    devtype, at = unpack_text(buf, 4)
                    name, at = unpack_text(buf, at)
                    port = unpack_text(buf, at)[0] if len(buf) > at + 4 else ""
                    name = name or devtype
                    self.devs[name] = FakeDevice(name, devtype, port)
                    did_dnames[did] = name
                elif kind == 0:
                    self.read_track(buf, track_names, exclude, tid_dtnames, did_dnames)
                elif kind == 1:
                    self.read_record(buf, tid_dtnames, header_only, seen)
        except Exception as error:  # the library's own catch-all, which prints the message and returns False
            FakeVitalFile.errors.append(str(error))
            return False
        return True

    def read_track(self, buf: bytes, track_names: Any, exclude: Any, tid_dtnames: dict[int, str], did_dnames: dict[int, str]) -> None:
        tid, trktype, fmt = struct.unpack_from("<HBB", buf, 0)
        if trktype in (1, 2) and fmt not in FMT_TYPE_LEN:
            return
        tname, pos = unpack_text(buf, 4)
        unit, mindisp, maxdisp, col, srate, gain, offset, montype, did = "", 0.0, 0.0, 0, 0.0, 0.0, 0.0, 0, 0
        if len(buf) > pos:
            unit, pos = unpack_text(buf, pos)
        for width, name in ((4, "mindisp"), (4, "maxdisp"), (4, "col"), (4, "srate"), (8, "gain"), (8, "offset"), (1, "montype"), (4, "did")):
            if len(buf) > pos:
                code = {"mindisp": "<f", "maxdisp": "<f", "col": "<I", "srate": "<f", "gain": "<d", "offset": "<d", "montype": "<B", "did": "<I"}[name]
                value = struct.unpack_from(code, buf, pos)[0]
                pos += width
                if name == "mindisp":
                    mindisp = value
                elif name == "maxdisp":
                    maxdisp = value
                elif name == "col":
                    col = value
                elif name == "srate":
                    srate = value
                elif name == "gain":
                    gain = value
                elif name == "offset":
                    offset = value
                elif name == "montype":
                    montype = value
                else:
                    did = value
        dname = did_dnames.get(did, "") if did and did in did_dnames else ""
        dtname = f"{dname}/{tname}" if dname else tname
        matched = False
        if not track_names or dtname in track_names:
            matched = True
        else:
            for sel_dtname in track_names:
                if dtname.endswith(sel_dtname) or (dname + "/*" == sel_dtname):
                    matched = True
        if exclude and matched:
            matched = dtname not in exclude
        if not matched or dtname in FakeVitalFile.hidden_tracks:
            return
        tid_dtnames[tid] = dtname
        self.trks[dtname] = FakeTrack(tname, trktype, fmt, unit, srate, mindisp, maxdisp, col, montype, gain, offset, dname)

    def read_record(self, buf: bytes, tid_dtnames: dict[int, str], header_only: bool, seen: dict[str, int]) -> None:
        if len(buf) < 12:
            return
        infolen = struct.unpack_from("<H", buf, 0)[0]
        dt = struct.unpack_from("<d", buf, 2)[0]
        tid = struct.unpack_from("<H", buf, 10)[0]
        pos = 2 + infolen
        if tid not in tid_dtnames:
            return
        dtname = tid_dtnames[tid]
        trk = self.trks[dtname]
        if self.dtstart == 0 or (dt > 0 and dt < self.dtstart):
            self.dtstart = dt
        if dt > self.dtend:
            self.dtend = dt
        if header_only:
            return
        seen[dtname] = seen.get(dtname, 0) + 1
        if FakeVitalFile.drop_records.get(dtname) == seen[dtname]:
            return
        if FakeVitalFile.shift_times.get(dtname) == seen[dtname]:
            dt += 1.0
        if trk.type == 1:
            fmtcode, fmtlen = FMT_TYPE_LEN[trk.fmt]
            if len(buf) < pos + 4:
                return
            (nsamp,) = struct.unpack_from("<I", buf, pos)
            pos += 4
            if len(buf) < pos + nsamp * fmtlen:
                return
            trk.recs.append({"dt": dt, "val": FakeSamples(buf[pos : pos + nsamp * fmtlen], fmtcode, fmtlen)})
        elif trk.type == 2:
            fmtcode, fmtlen = FMT_TYPE_LEN[trk.fmt]
            if len(buf) < pos + fmtlen:
                return
            code = ("<q" if fmtcode == "l" else "<Q") if fmtcode in "lL" else "<" + fmtcode  # the native long is eight bytes: a four-byte value fails here
            value = struct.unpack_from(code, buf, pos)[0]
            trk.recs.append({"dt": dt, "val": value})
        elif trk.type == 5:
            pos += 4
            if len(buf) < pos + 4:
                return
            trk.recs.append({"dt": dt, "val": unpack_text(buf, pos)[0]})


@contextmanager
def installed() -> Iterator[type[FakeVitalFile]]:
    """The stand-in as the module ``vitaldb`` for the length of the block, with its record of reads and planted drops cleared before and after."""
    module = types.ModuleType("vitaldb")
    module.__dict__["VitalFile"] = FakeVitalFile
    previous = sys.modules.get("vitaldb")
    sys.modules["vitaldb"] = module
    FakeVitalFile.loads, FakeVitalFile.errors, FakeVitalFile.drop_records, FakeVitalFile.shift_times = [], [], {}, {}
    FakeVitalFile.hidden_tracks = set()
    try:
        yield FakeVitalFile
    finally:
        FakeVitalFile.loads, FakeVitalFile.errors, FakeVitalFile.drop_records, FakeVitalFile.shift_times = [], [], {}, {}
        FakeVitalFile.hidden_tracks = set()
        if previous is None:
            del sys.modules["vitaldb"]
        else:
            sys.modules["vitaldb"] = previous
