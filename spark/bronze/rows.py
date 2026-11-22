"""The rows of the Bronze tables, built from what the publisher's library read. Pure code: no Spark, no library, no file.

A builder takes one track of one case and yields tuples in the column order of its table (``spark/bronze/tables.py``), each with its lineage. A builder is a
generator, so a track's rows are made as Spark takes them and a track is never held twice.

The second of a waveform sample and the position of a reading among the readings that share its time are the event model's own definitions, so a Bronze row
and the replay's event for the same reading have the same key: ``replay.events.waveform.second_spans`` cuts the records at the seconds with exact integer
arithmetic and ``replay.events.stream.discriminators`` numbers the readings.
"""

from __future__ import annotations

import math
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any

from replay.events.errors import EventModelError
from replay.events.recording import WaveformTrack
from replay.events.stream import discriminators
from replay.events.waveform import second_samples, second_spans

KIND_OF_TYPE = {1: "waveform", 2: "numeric", 5: "string"}


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
    unit: str
    sample_rate: float
    gain: float
    value_offset: float
    min_display: float
    max_display: float
    color: int
    monitor_type: int


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


def waveform_rows(case_id: int, track: WaveformTrack, lineage: Lineage) -> Iterator[tuple[Any, ...]]:
    """One row for each second of a waveform track that holds a sample: ``(case, track, second, offset of its first sample, its samples)``."""
    if not math.isfinite(track.sample_rate) or track.sample_rate <= 0:
        raise StructureError(f"case {case_id}: the waveform track {track.name} has a sample rate that is not a positive number")
    try:
        spans = second_spans(track)
    except EventModelError as error:
        raise StructureError(f"case {case_id}: the waveform track {track.name} cannot be cut into seconds: {error}") from None
    for span in spans:
        yield (case_id, track.name, span.second, span.start_offset_us, second_samples(track, span), *lineage.columns)
