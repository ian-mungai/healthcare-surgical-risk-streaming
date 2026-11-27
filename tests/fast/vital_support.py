"""Builders of the packets of a case file, for the fast tests of the structural check and of the packet walk.

A case file is a gzip stream of the mark ``VITA``, a version, a header and packets. A packet is its type (0 track information, 1 record, 9 device information),
its length as four bytes and its body, all little-endian. The builders follow the layout that the publisher's library reads. A test plants a defect by giving
a builder another length, format or time, or by cutting the bytes.
"""

from __future__ import annotations

import struct

START = 4102444800.0
FORMAT_CODES = {1: "<f", 2: "<d", 3: "<b", 4: "<B", 5: "<h", 6: "<H", 7: "<i", 8: "<I"}


def text(value: str) -> bytes:
    data = value.encode()
    return struct.pack("<I", len(data)) + data


def packet(kind: int, body: bytes) -> bytes:
    return bytes([kind]) + struct.pack("<I", len(body)) + body


def device_packet(did: int, devtype: str, name: str, port: str = "") -> bytes:
    return packet(9, struct.pack("<I", did) + text(devtype) + text(name) + (text(port) if port else b""))


def track_packet(tid: int, reclen: int | None = None, *, name: str | None = None, kind: int = 2, fmt: int = 1, did: int = 0, rate: float = 0.0) -> bytes:
    """A track-information packet; ``reclen`` None leaves the field out, as the files of the fixed sample do."""
    body = (
        struct.pack("<HBB", tid, kind, fmt)
        + text(f"T{tid}" if name is None else name)
        + text("unit")
        + struct.pack("<ffIfddBI", 0.0, 1.0, 0, rate, 1.0, 0.0, 0, did)
    )
    return packet(0, body if reclen is None else body + struct.pack("<I", reclen))


def record_packet(tid: int, value: float = 1.0, *, payload: bytes | None = None, dt: float = START, infolen: int = 10) -> bytes:
    """A record packet. The default payload is a four-byte float. ``infolen`` is the declared length of the info block that holds the time and the track."""
    body = struct.pack("<HdH", infolen, dt, tid) + (struct.pack("<f", value) if payload is None else payload)
    return packet(1, body)


def numeric_record(tid: int, fmt: int, value: float, *, dt: float = START) -> bytes:
    return record_packet(tid, payload=struct.pack(FORMAT_CODES[fmt], value), dt=dt)


def text_record(tid: int, content: str, *, dt: float = START) -> bytes:
    return record_packet(tid, payload=struct.pack("<I", 0) + text(content), dt=dt)


def waveform_record(tid: int, fmt: int, samples: list[float], *, dt: float = START) -> bytes:
    return record_packet(tid, payload=struct.pack("<I", len(samples)) + b"".join(struct.pack(FORMAT_CODES[fmt], value) for value in samples), dt=dt)


def vital(*packets: bytes, header: bytes = bytes(10)) -> bytes:
    """The uncompressed payload of a case file: the mark, the version, the header and the packets."""
    return b"VITA" + struct.pack("<IH", 3, len(header)) + header + b"".join(packets)
