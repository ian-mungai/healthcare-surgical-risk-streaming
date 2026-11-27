"""The fixed sample files of the host figures: the text of a Linux host's memory, CPU and disk counters and of Docker's formatted output."""

from __future__ import annotations

from pathlib import Path

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "proc"


def fixture(name: str) -> str:
    return (FIXTURES / name).read_text()
