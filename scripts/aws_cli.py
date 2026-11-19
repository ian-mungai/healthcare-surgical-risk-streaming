"""Call the AWS command line with an explicit profile and region, a deadline and JSON output."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from scripts.process import run_command

AWS_TIMEOUT_SECONDS = 120


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
            raise AwsError(f"aws {' '.join(arguments[:2])} failed: {result.stderr.strip()}")
        return json.loads(result.stdout) if result.stdout.strip() else {}
