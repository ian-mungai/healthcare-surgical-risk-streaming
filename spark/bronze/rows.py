"""The rows of the Bronze tables, built from what the publisher's library read. Pure code: no Spark, no library, no file.

A builder takes one track of one case and yields tuples in the column order of its table (``spark/bronze/tables.py``), each with its lineage. A builder is a
generator, so a track's rows are made as Spark takes them and a track is never held twice.

The position of a reading among the readings that share its time is the event model's own definition (``replay.events.stream.discriminators``), so a Bronze row
and the replay's event for the same reading have the same discriminator. The second of a waveform sample is placed from the encoded values with no rounding
(``spark/bronze/sampling.py``).
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any

from replay.events.stream import discriminators
from spark.bronze.sampling import SamplingError, packet_pieces

KIND_OF_TYPE = {1: "waveform", 2: "numeric", 5: "string"}
SAMPLE_TYPES = {1: "float32", 2: "float64", 3: "int8", 4: "uint8", 5: "int16", 6: "uint16", 7: "int32", 8: "uint32"}


class StructureError(RuntimeError):
    """A case file or a track that Bronze cannot decode. The message names the case and never a reading."""


@dataclass(frozen=True, slots=True)
class Lineage:
    """The case file that a row came from: its object version and its SHA-256 from the manifest."""

    version_id: str
    sha256: str

    @property
    def columns(self) -> tuple[str, str]:
        return (self.version_id, self.sha256)


@dataclass(frozen=True, slots=True)
class TrackInfo:
    """What the case file publishes about one track, in the column order of ``bronze.tracks`` after the case."""

    track: str
    device: str
    track_name: str
    device_type: str
    device_port: str
    kind: str
    track_type: int
    sample_format: int
    sample_type: str
    unit: str
    sample_rate: float
    gain: float
    value_offset: float
    min_display: float
    max_display: float
    color: int
    monitor_type: int


def sample_type(dtname: str, kind: str, fmt: int) -> str:
    """The type name of a track's samples: ``text`` for a text track whatever its code, otherwise the file format's name for it. Another code is refused."""
    if kind == "string":
        return "text"
    if fmt not in SAMPLE_TYPES:
        raise StructureError(f"the track {dtname} has a sample format that the file format does not define")
    return SAMPLE_TYPES[fmt]


def track_info(dtname: str, track: Any, device: Any | None) -> TrackInfo:
    """The metadata of a library track and of its device (``None`` when it has none). A track type that the design does not carry is refused."""
    kind = KIND_OF_TYPE.get(track.type)
    if kind is None:
        raise StructureError(f"the track {dtname} has a type that Bronze does not carry")
    return TrackInfo(
        dtname,
        str(track.dname),
        str(track.name),
        "" if device is None else str(device.type),
        "" if device is None else str(device.port),
        kind,
        int(track.type),
        int(track.fmt),
        sample_type(dtname, kind, int(track.fmt)),
        str(track.unit),
        float(track.srate),
        float(track.gain),
        float(track.offset),
        float(track.mindisp),
        float(track.maxdisp),
        int(track.col),
        int(track.montype),
    )


def track_row(case_id: int, info: TrackInfo, lineage: Lineage) -> tuple[Any, ...]:
    return (
        case_id,
        info.track,
        info.device,
        info.track_name,
        info.device_type,
        info.device_port,
        info.kind,
        info.track_type,
        info.sample_format,
        info.sample_type,
        info.unit,
        info.sample_rate,
        info.gain,
        info.value_offset,
        info.min_display,
        info.max_display,
        info.color,
        info.monitor_type,
        *lineage.columns,
    )


def numeric_rows(case_id: int, track: str, readings: Sequence[tuple[int, float, float]], lineage: Lineage) -> Iterator[tuple[Any, ...]]:
    """One row for each reading ``(case time in microseconds, the file's own time in seconds, value)``, in the order of the file."""
    ranks = discriminators([time for time, _, _ in readings])
    for rank, (time, source_time, value) in zip(ranks, readings, strict=True):
        yield (case_id, track, time, rank, float(source_time), float(value), *lineage.columns)


def string_rows(case_id: int, track: str, events: Sequence[tuple[int, float, str]], lineage: Lineage) -> Iterator[tuple[Any, ...]]:
    """One row for each string event ``(case time in microseconds, the file's own time in seconds, text)``. The source ID is ``case/track/position``."""
    ranks = discriminators([time for time, _, _ in events])
    for position, (rank, (time, source_time, text)) in enumerate(zip(ranks, events, strict=True)):
        yield (case_id, track, time, rank, float(source_time), text, f"{case_id}/{track}/{position}", *lineage.columns)


def waveform_rows(
    case_id: int, track: str, sample_rate: float, records: Sequence[tuple[float, Sequence[float]]], origin: float, lineage: Lineage
) -> Iterator[tuple[Any, ...]]:
    """One row for each second of a waveform track that holds a sample: ``(case, track, second, offset of its first sample, its samples)``.

    ``records`` are the packets of the track in the order of the file, each the file's own start time (float seconds since the epoch) and its samples. A
    sample's second is placed from the exact values (``spark/bronze/sampling.py``). A second takes its samples in the order of the records, and its offset
    from the first packet that has a sample in it.
    """
    chunks: dict[int, list[tuple[int, int, int]]] = {}
    offsets: dict[int, int] = {}
    for index, (start, samples) in enumerate(records):
        try:
            pieces = packet_pieces(start, len(samples), sample_rate, origin)
        except SamplingError as error:
            raise StructureError(f"case {case_id}: the waveform track {track} cannot be cut into seconds: {error}") from None
        for piece in pieces:
            offsets.setdefault(piece.second, piece.offset_us)
            chunks.setdefault(piece.second, []).append((index, piece.first, piece.end))
    for second in sorted(chunks):
        samples_of_second = [float(value) for index, first, end in chunks[second] for value in records[index][1][first:end]]
        yield (case_id, track, second, offsets[second], samples_of_second, *lineage.columns)
