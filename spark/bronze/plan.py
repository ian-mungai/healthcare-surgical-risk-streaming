"""The passes over a case file: which tracks one read of the file takes. Pure code: no library, no file.

The publisher's library reads a whole file for each call and keeps the records of the tracks it was asked for. A pass is one such call. The costs pull two
ways: a pass for each track reads the file once for each track, and a pass for many tracks holds their records together.

- ``GROUPED`` (the default): the numeric tracks of a case are one pass and the string tracks are another, because their records are few and small. Each waveform
  track has a pass of its own, because a waveform track holds most of a case's bytes, so a worker never holds more than its largest waveform track.
- ``PER_TRACK``: every track has a pass of its own, so a worker never holds more than its largest single track.

The tracks are in name order in both modes, so the two modes make the same rows in the same order.
"""

from __future__ import annotations

from collections.abc import Mapping

GROUPED = "grouped"
PER_TRACK = "per_track"
MODES = (GROUPED, PER_TRACK)
TYPE_OF_KIND = {"numeric": 2, "string": 5, "waveform": 1}


class PlanError(ValueError):
    """A kind of track or a mode of reading that the plan does not know."""


def track_passes(types: Mapping[str, int], kind: str, mode: str) -> list[list[str]]:
    """The passes for the tracks of one kind: ``types`` maps each track's name to its library type. Each pass is a list of track names in name order."""
    if kind not in TYPE_OF_KIND:
        raise PlanError(f"the kind {kind!r} has no tracks to read: it is one of {', '.join(TYPE_OF_KIND)}")
    if mode not in MODES:
        raise PlanError(f"the mode {mode!r} is not one of {', '.join(MODES)}")
    names = sorted(name for name, track_type in types.items() if track_type == TYPE_OF_KIND[kind])
    if not names:
        return []
    return [names] if mode == GROUPED and kind != "waveform" else [[name] for name in names]
