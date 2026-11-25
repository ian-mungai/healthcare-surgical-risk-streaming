"""Call the AWS command line with an explicit profile and region, a deadline and JSON output."""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from scripts.ingestion.closed import ClosedInputError
from scripts.ingestion.output_file import write_new_file
from scripts.process import TimeoutExpired, partial_text, run_command, timeout_text
from scripts.project_config import ROOT

AWS_TIMEOUT_SECONDS = 120
ERROR_CODE = re.compile(r"An error occurred \(([A-Za-z0-9.]+)\)")
RAW_ERRORS = ROOT / "runs" / "raw_errors"


def keep_raw_error(name: str, text: str) -> Path:
    """Keep a tool's raw error text, which can hold bucket and role names, in an owner-only file outside every run's evidence."""
    RAW_ERRORS.mkdir(mode=0o700, parents=True, exist_ok=True)
    RAW_ERRORS.chmod(0o700)
    target = RAW_ERRORS / f"{time.time_ns()}_{name}.txt"
    try:
        write_new_file(target, text)
    except ClosedInputError:
        raise OSError("the raw error text could not be kept") from None
    return target


def keep_partial_error(arguments: tuple[str, ...], error: TimeoutExpired) -> str:
    """Keep the error text a timed-out call had written, apart from its standard output, which can hold a new key. Return what to add to the message."""
    text = partial_text(error.stderr)
    if not text:
        return ""
    try:
        kept = keep_raw_error("_".join(arguments[:2]) + "_timeout", text)
    except OSError:
        return "; its partial error text could not be kept"
    return f"; its partial error text is in {kept.relative_to(RAW_ERRORS.parent.parent)}"


class AwsError(RuntimeError):
    """An AWS command-line call failed."""


class AwsTimeout(AwsError, TimeoutExpired):
    """An AWS command-line call passed its deadline. It stays a TimeoutExpired for callers that wait or retry, and its text and cmd hold no argument."""

    def __init__(self, label: str, deadline: float, note: str = "") -> None:
        TimeoutExpired.__init__(self, label, deadline)
        self.message = timeout_text(label, deadline) + note

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True)
class Aws:
    profile: str
    region: str
    max_attempts: int | None = field(default=None, kw_only=True)

    def single_attempt(self) -> Aws:
        """The same profile and region with the AWS command line limited to one request for each call: it retries a lost reply by default."""
        return replace(self, max_attempts=1)

    def call(self, *arguments: str, missing: tuple[str, ...] = (), timeout: float = AWS_TIMEOUT_SECONDS) -> Any:
        """Run one AWS command within ``timeout`` seconds and return its parsed JSON output, or None when its error names one of ``missing``."""
        fixed = ["--profile", self.profile, "--region", self.region, "--output", "json", "--no-cli-pager"]
        environment = None if self.max_attempts is None else os.environ | {"AWS_MAX_ATTEMPTS": str(self.max_attempts)}
        try:
            result = run_command("aws", [*arguments, *fixed], timeout=timeout, env=environment)
        except TimeoutExpired as error:
            raise AwsTimeout(f"aws {' '.join(arguments[:2])}", timeout, keep_partial_error(arguments, error)) from None
        if result.returncode != 0:
            if any(marker in result.stderr for marker in missing):
                return None
            code = ERROR_CODE.search(result.stderr)
            reason = code.group(1) if code else f"exit status {result.returncode}"
            summary = f"aws {' '.join(arguments[:2])} failed: {reason}"
            try:
                kept = keep_raw_error("_".join(arguments[:2]), result.stderr)
            except OSError:
                raise AwsError(f"{summary}; its full error text could not be kept") from None
            raise AwsError(f"{summary}; its full error text is in {kept.relative_to(RAW_ERRORS.parent.parent)}")
        return json.loads(result.stdout) if result.stdout.strip() else {}
