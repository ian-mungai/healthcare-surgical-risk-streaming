"""The stop signal of the replay service and the wall clock that waits on it.

``StopFlag`` is set by SIGTERM or SIGINT. The handler only writes one byte to a pipe and sets a plain flag, because a handler that takes a lock (as an event
does) can deadlock when the signal lands while the interrupted code holds that lock. Every wait of the service, the pacing waits of the replay loop and the idle
wait of the service loop, waits on the pipe with its timeout instead of sleeping, so a stop ends the wait at once and the loop saves its position and returns
before it sends another event.
"""

from __future__ import annotations

import contextlib
import os
import select
import signal
import time
from collections.abc import Callable
from types import FrameType

SLICE_SECONDS = 1.0


class StopFlag:
    """Set by SIGTERM or SIGINT; ``wait`` returns as soon as it is set. ``handle=False`` installs no signal handler, for a caller that sets the flag itself."""

    def __init__(self, *, handle: bool = True) -> None:
        self.requested = False
        self.reader, self.writer = os.pipe()
        os.set_blocking(self.reader, False)
        os.set_blocking(self.writer, False)
        if handle:
            signal.signal(signal.SIGTERM, self.request)
            signal.signal(signal.SIGINT, self.request)

    def request(self, signum: int = 0, frame: FrameType | None = None) -> None:
        self.requested = True
        with contextlib.suppress(OSError):
            os.write(self.writer, b"x")

    def is_set(self) -> bool:
        return self.requested

    def wait(self, seconds: float) -> bool:
        """Wait at most ``seconds`` for the stop and say whether it is set. A stop that was set before the call returns at once."""
        if not self.requested and seconds > 0:
            select.select([self.reader], [], [], seconds)
        return self.requested

    def close(self) -> None:
        for descriptor in (self.reader, self.writer):
            with contextlib.suppress(OSError):
                os.close(descriptor)


class SystemClock:
    """The wall clock in microseconds. A wait is cut into slices of at most a second, so ``poll`` runs about once a second, and it ends at its target or as soon
    as the stop is set, whichever comes first."""

    def __init__(self, stop: StopFlag, poll: Callable[[], None] | None = None) -> None:
        self.stop = stop
        self.poll = poll

    def now_us(self) -> int:
        return time.time_ns() // 1000

    def wait_until(self, wall_us: int) -> None:
        while (remaining := wall_us - self.now_us()) > 0:
            if self.stop.wait(min(remaining / 1_000_000, SLICE_SECONDS)):
                return
            if self.poll is not None:
                self.poll()
