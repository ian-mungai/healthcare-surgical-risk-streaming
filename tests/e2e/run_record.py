"""The record that every end-to-end run writes under ``runs/``, passing or failing."""

from __future__ import annotations

import json
import re
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


@dataclass
class Observation:
    case: str
    step: str
    expected: str
    observed: str
    passed: bool


@dataclass
class Record:
    feature: str
    slug: str
    rerun: str
    mode: str = "full"
    environment: dict[str, str] = field(default_factory=dict)
    revision: str = "no commit"
    tree_sha256: str = ""
    started_utc: str = field(default_factory=lambda: datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"))
    commands: list[str] = field(default_factory=list)
    observations: list[Observation] = field(default_factory=list)
    outcome: str = "blocked"
    limits: list[str] = field(default_factory=list)
    cleanup: str = "pending"

    def expect(self, case: str, step: str, expected: str, observed: str, passed: bool) -> bool:
        self.observations.append(Observation(case, step, expected, observed, passed))
        return passed

    def close(self, log: list[str], *, blocked: str = "") -> int:
        """Set the outcome, write the record and its log in a new folder, print each assertion and return the exit code."""
        self.outcome = f"blocked: {blocked}" if blocked else ("pass" if self.observations and all(item.passed for item in self.observations) else "fail")
        stamp = self.started_utc.replace(":", "").replace("-", "")
        runs = ROOT / "runs"
        runs.mkdir(parents=True, exist_ok=True)
        mode = re.sub(r"[^a-z0-9]+", "_", self.mode.lower()).strip("_")
        folder = Path(tempfile.mkdtemp(prefix=f"{stamp}_{self.slug}_{mode}_", dir=runs))
        (folder / "record.json").write_text(json.dumps(asdict(self), indent=2) + "\n")
        (folder / "log.txt").write_text("\n".join(log) + "\n")
        for item in self.observations:
            sys.stdout.write(f"{'PASS' if item.passed else 'FAIL'} {item.case} ({item.step}): {item.observed}\n")
        sys.stdout.write(f"{self.outcome}: record in {folder.relative_to(ROOT)}\n")
        return 0 if self.outcome == "pass" else 1
