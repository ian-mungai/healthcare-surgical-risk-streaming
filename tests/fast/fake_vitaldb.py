"""A stand-in for the publisher's library (``vitaldb`` 1.7.2) at the boundary of the decoder, for fast tests that drive the decoder's own functions.

``FakeVitalFile.load_vital`` is a port of ``VitalFile.load_vital`` of the installed library (``vitaldb/utils.py``, lines 1879 to 2210), reduced to the parts
that decide what the decoder sees. Each part names the lines it follows:

- the packet limit of a file that is not packed (line 1932);
- the description of a track, which is registered for its declared length and its place in the file order before it is matched (lines 2077 and 2078);
- the match of a requested name against a track's name (lines 2059 to 2066): a name matches a track when it is the track's whole name, or when the track's name
  ends with it;
- the early end of a read of a packed file once the last wanted track's records are consumed (lines 2089 to 2099 and 2184 to 2187);
- the record of a track that was not selected: it is skipped, and when its track declared a length the reader seeks over the rest of that length without
  reading it (lines 2112 to 2119), so the records of other tracks that lie inside that length are lost;
- the skip of a record that does not fit its packet, and the decode of a numeric value with the platform's native format code (line 2159: ``l`` and ``L`` are
  eight bytes on a 64-bit Linux, so a four-byte value fails to unpack, the read stops and ``False`` is returned);
- the reset command, after which the records of the track named ``EVENT`` that were read before it are gone (lines 2169 to 2173);
- the end of the recording: it is the latest record time, and a waveform record that was read also extends it to its start plus its samples over the
  track's rate (lines 2129 to 2130 and 2151 to 2154);
- the text decode that drops invalid bytes (lines 34 to 44) and the end of a read at an error.

numpy is not in the tools environment, so a waveform record's samples are a small object with the attributes that the decoder reads.

Its limit: it is a port made by reading the library. ``tests/fast/test_fake_vitaldb_parity.py`` runs the same synthetic files through the real library, where
it is installed (the Spark image), and requires the same records, so a difference that the port misses fails there. It is skipped where it is not installed.
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
OPTIONAL_TRACK_FIELDS = (("<f", 4), ("<f", 4), ("<I", 4), ("<f", 4), ("<d", 8), ("<d", 8), ("<B", 1), ("<I", 4), ("<I", 4), ("<d", 8), ("<d", 8))


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
    def __init__(self, name: str, type: int, fmt: int, unit: str, fields: list[Any], dname: str) -> None:
        self.name, self.type, self.fmt, self.unit, self.dname = name, type, fmt, unit, dname
        mindisp, maxdisp, col, srate, gain, offset, montype = fields[:7]
        self.mindisp, self.maxdisp, self.col, self.srate, self.gain, self.offset, self.montype = mindisp, maxdisp, col, srate, gain, offset, montype
        self.recs: list[dict[str, Any]] = []


class FakeDevice:
    def __init__(self, name: str, devtype: str, port: str) -> None:
        self.name, self.type, self.port = name, devtype, port


def unpack_text(buf: bytes, pos: int) -> tuple[str, int]:
    (size,) = struct.unpack_from("<I", buf, pos)
    return buf[pos + 4 : pos + 4 + size].decode("utf-8", "ignore"), pos + 4 + size


class FakeVitalFile:
    drop_records: dict[str, int] = {}  # a test sets it to make the stand-in skip the nth record of a track, as the real library skips a record it cannot place
    shift_times: dict[str, int] = {}  # a test sets it to give the nth record of a track another time than the file holds
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
        tid_reclens: dict[int, int] = {}
        trk_order_tids: list[int] = []
        seen: dict[str, int] = {}
        rec_consumed = 0
        stop_offset: int | None = None
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
                    self.read_track(buf, track_names, exclude, tid_dtnames, did_dnames, tid_reclens, trk_order_tids)
                elif kind == 1:
                    if stop_offset is None:
                        stop_offset = -1
                        if self.packed and track_names:
                            wanted = [index for index, tid in enumerate(trk_order_tids) if tid in tid_dtnames]
                            if wanted and all(tid_reclens.get(tid, 0) > 0 for tid in trk_order_tids):
                                stop_offset = sum(tid_reclens[tid] for tid in trk_order_tids[: wanted[-1] + 1])
                    rec_consumed += 5 + packet_len
                    if len(buf) < 12:
                        continue
                    tid = struct.unpack_from("<H", buf, 10)[0]
                    if tid not in tid_dtnames:
                        if tid in tid_reclens and tid_reclens[tid] > 5 + packet_len:
                            skipped = tid_reclens[tid] - (5 + packet_len)
                            position += skipped
                            rec_consumed += skipped
                        if stop_offset > 0 and rec_consumed >= stop_offset:
                            break
                        continue
                    if self.read_record(buf, tid_dtnames[tid], header_only, seen):
                        continue
                elif kind == 6 and buf[0] == 6 and "EVENT" in self.trks:
                    self.trks["EVENT"].recs = []
                if stop_offset is not None and stop_offset > 0 and rec_consumed >= stop_offset:
                    break
        except Exception as error:  # the library's own catch-all, which prints the message and returns False
            FakeVitalFile.errors.append(str(error))
            return False
        return True

    def read_track(
        self,
        buf: bytes,
        track_names: Any,
        exclude: Any,
        tid_dtnames: dict[int, str],
        did_dnames: dict[int, str],
        tid_reclens: dict[int, int],
        trk_order_tids: list[int],
    ) -> None:
        tid, trktype, fmt = struct.unpack_from("<HBB", buf, 0)
        if trktype in (1, 2) and fmt not in FMT_TYPE_LEN:
            return
        tname, pos = unpack_text(buf, 4)
        unit = ""
        if len(buf) > pos:
            unit, pos = unpack_text(buf, pos)
        fields: list[Any] = []
        for code, width in OPTIONAL_TRACK_FIELDS:
            if len(buf) > pos:
                fields.append(struct.unpack_from(code, buf, pos)[0])
                pos += width
            else:
                fields.append(0)
        mindisp, maxdisp, col, srate, gain, offset, montype, did, reclen = fields[:9]
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
        tid_reclens[tid] = reclen
        trk_order_tids.append(tid)
        if not matched or dtname in FakeVitalFile.hidden_tracks:
            return
        tid_dtnames[tid] = dtname
        self.trks[dtname] = FakeTrack(tname, trktype, fmt, unit, [mindisp, maxdisp, col, srate, gain, offset, montype], dname)

    def read_record(self, buf: bytes, dtname: str, header_only: bool, seen: dict[str, int]) -> bool:
        """Decode one record of a selected track. True when the record is skipped, as the real loop's ``continue`` skips it."""
        infolen = struct.unpack_from("<H", buf, 0)[0]
        dt = struct.unpack_from("<d", buf, 2)[0]
        pos = 2 + infolen
        trk = self.trks[dtname]
        if self.dtstart == 0 or (dt > 0 and dt < self.dtstart):
            self.dtstart = dt
        if dt > self.dtend:
            self.dtend = dt
        if header_only:
            return True
        seen[dtname] = seen.get(dtname, 0) + 1
        if FakeVitalFile.drop_records.get(dtname) == seen[dtname]:
            return True
        if FakeVitalFile.shift_times.get(dtname) == seen[dtname]:
            dt += 1.0
        if trk.type == 1:
            fmtcode, fmtlen = FMT_TYPE_LEN[trk.fmt]
            if len(buf) < pos + 4:
                return True
            (nsamp,) = struct.unpack_from("<I", buf, pos)
            pos += 4
            if len(buf) < pos + nsamp * fmtlen:
                return True
            trk.recs.append({"dt": dt, "val": FakeSamples(buf[pos : pos + nsamp * fmtlen], fmtcode, fmtlen)})
            if trk.srate > 0 and dt + nsamp / trk.srate > self.dtend:
                self.dtend = dt + nsamp / trk.srate
        elif trk.type == 2:
            fmtcode, fmtlen = FMT_TYPE_LEN[trk.fmt]
            if len(buf) < pos + fmtlen:
                return True
            code = ("<q" if fmtcode == "l" else "<Q") if fmtcode in "lL" else "<" + fmtcode  # the native long is eight bytes: a four-byte value fails here
            trk.recs.append({"dt": dt, "val": struct.unpack_from(code, buf, pos)[0]})
        elif trk.type == 5:
            pos += 4
            if len(buf) < pos + 4:
                return True
            trk.recs.append({"dt": dt, "val": unpack_text(buf, pos)[0]})
        return False


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
