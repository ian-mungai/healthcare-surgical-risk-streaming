"""The state of one case occurrence, kept by Spark under the key run, room and case occurrence, as one JSON text.

A new occurrence has a new key and so an empty state: a rolling feature of it never holds a value of the case before it in the room.
The buffers hold only what a
later cutoff can still read: the readings of the last 15 minutes before the next cutoff, the last 20 seconds of each waveform and the summary of each arterial
second of the series window. The recent event suffixes are the events of the last late-limit span, which a re-sent event is checked against. The state is
plain data, so a restart that reads it back from JSON gives the same rows.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from spark.streaming.signals import SecondSummary

STATE_FORMAT = "stream_state_1"


@dataclass(slots=True)
class WaveSecondState:
    sample_rate: float
    start_offset_us: int
    samples: tuple[float, ...]


@dataclass(slots=True)
class OccurrenceState:
    run_id: str
    room: int
    occurrence_id: str
    case_id: int
    start_us: int | None = None
    high_water_us: int | None = None
    opend_us: int | None = None
    next_index: int = 1
    final_emitted: bool = False
    closed: bool = False
    numeric: dict[str, dict[int, tuple[int, float]]] = field(default_factory=dict)
    wave_samples: dict[str, dict[int, WaveSecondState]] = field(default_factory=dict)
    art_summary: dict[int, SecondSummary] = field(default_factory=dict)
    recent: dict[str, int] = field(default_factory=dict)
    counts: dict[str, int] = field(default_factory=dict)

    def count(self, name: str) -> None:
        self.counts[name] = self.counts.get(name, 0) + 1

    def to_json(self) -> str:
        document: dict[str, Any] = {
            "format": STATE_FORMAT,
            "run_id": self.run_id,
            "room": self.room,
            "occurrence_id": self.occurrence_id,
            "case_id": self.case_id,
            "start_us": self.start_us,
            "high_water_us": self.high_water_us,
            "opend_us": self.opend_us,
            "next_index": self.next_index,
            "final_emitted": self.final_emitted,
            "closed": self.closed,
            "numeric": {track: [[time, disc, value] for time, (disc, value) in sorted(readings.items())] for track, readings in sorted(self.numeric.items())},
            "wave_samples": {
                track: [[second, item.sample_rate, item.start_offset_us, list(item.samples)] for second, item in sorted(seconds.items())]
                for track, seconds in sorted(self.wave_samples.items())
            },
            "art_summary": [[second, s.valid, s.first, s.last, s.total, s.count] for second, s in sorted(self.art_summary.items())],
            "recent": self.recent,
            "counts": self.counts,
        }
        return json.dumps(document, sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_json(cls, text: str) -> OccurrenceState:
        document = json.loads(text)
        if document.get("format") != STATE_FORMAT:
            raise ValueError("the saved state is not of this format")
        return cls(
            document["run_id"],
            document["room"],
            document["occurrence_id"],
            document["case_id"],
            document["start_us"],
            document["high_water_us"],
            document["opend_us"],
            document["next_index"],
            document["final_emitted"],
            document["closed"],
            {track: {time: (disc, value) for time, disc, value in readings} for track, readings in document["numeric"].items()},
            {
                track: {second: WaveSecondState(rate, offset, tuple(samples)) for second, rate, offset, samples in seconds}
                for track, seconds in document["wave_samples"].items()
            },
            {second: SecondSummary(valid, first, last, total, count) for second, valid, first, last, total, count in document["art_summary"]},
            {suffix: int(time) for suffix, time in document["recent"].items()},
            {name: int(number) for name, number in document["counts"].items()},
        )
