"""Call the AWS command line with an explicit profile and region, a deadline and JSON output."""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.process import run_command
from scripts.project_config import ROOT

AWS_TIMEOUT_SECONDS = 120
ERROR_CODE = re.compile(r"An error occurred \(([A-Za-z0-9.]+)\)")
RAW_ERRORS = ROOT / "runs" / "raw_errors"


def keep_raw_error(name: str, text: str) -> Path:
    """Keep a tool's raw error text, which can hold bucket and role names, in an owner-only file outside every run's evidence."""
    RAW_ERRORS.mkdir(mode=0o700, parents=True, exist_ok=True)
    RAW_ERRORS.chmod(0o700)
    target = RAW_ERRORS / f"{time.time_ns()}_{name}.txt"
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(text)
    return target


class AwsError(RuntimeError):
    """An AWS command-line call failed."""


@dataclass(frozen=True)
class Aws:
    profile: str
    region: str

    def call(self, *arguments: str, missing: tuple[str, ...] = (), timeout: float = AWS_TIMEOUT_SECONDS) -> Any:
        """Run one AWS command within ``timeout`` seconds and return its parsed JSON output, or None when its error names one of ``missing``."""
        fixed = ["--profile", self.profile, "--region", self.region, "--output", "json", "--no-cli-pager"]
        result = run_command("aws", [*arguments, *fixed], timeout=timeout)
        if result.returncode != 0:
            if any(marker in result.stderr for marker in missing):
                return None
            code = ERROR_CODE.search(result.stderr)
            kept = keep_raw_error("_".join(arguments[:2]), result.stderr)
            reason = code.group(1) if code else f"exit status {result.returncode}"
            raise AwsError(f"aws {' '.join(arguments[:2])} failed: {reason}; its full error text is in {kept.relative_to(RAW_ERRORS.parent.parent)}")
        return json.loads(result.stdout) if result.stdout.strip() else {}
