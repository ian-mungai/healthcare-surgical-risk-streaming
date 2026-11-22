"""The proof that a CI job ran its checks: what the run was, as one notice annotation on the job's check run.

Run from the repository root of the checkout as the last step of a job that runs checks: ``python -m scripts.quality.ci_proof``. The
plan job writes the same proof itself. A later push to main reads the proof of every job of a pull request run through the API
(``scripts/quality/ci_reuse.py``), because GitHub keeps no record of the runner's tools.

The proof names the run and its attempt, the commit of the checkout and the runner: its image and the versions of the tools that the
checks take from it. Pull request code writes it, so every field is a claim that the reader checks against what the API says; the
pull request and the workflow are never read from it. It holds no secret: everything in it is shown on the run's page.
"""

from __future__ import annotations

import json
import os
import platform
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from scripts.process import run_command
from scripts.quality.repo_checks import git

TITLE = "ci-proof"
TOOL_TIMEOUT_SECONDS = 30


def runner(environment: Mapping[str, str]) -> dict[str, str]:
    """The runner image that GitHub names (``ImageOS``, ``ImageVersion``) and the versions of the runner's tools that the checks start
    without a pin of their own: Python, Git and Make."""
    tools = {name: run_command(name, ["--version"], timeout=TOOL_TIMEOUT_SECONDS, check=True).stdout.splitlines()[0] for name in ("git", "make")}
    return {"image_os": environment.get("ImageOS", ""), "image_version": environment.get("ImageVersion", ""), "python": platform.python_version(), **tools}


def event_payload(environment: Mapping[str, str]) -> dict[str, Any]:
    """The webhook payload of the event that started the run, or an empty mapping when the run names none."""
    path = environment.get("GITHUB_EVENT_PATH")
    if not path:
        return {}
    payload: dict[str, Any] = json.loads(Path(path).read_text(encoding="utf-8"))
    return payload


def proof(root: Path, environment: Mapping[str, str]) -> dict[str, Any]:
    """The proof of the job that runs in ``environment`` with its checkout at ``root``."""
    return {
        "run_id": int(environment.get("GITHUB_RUN_ID") or 0),
        "run_attempt": int(environment.get("GITHUB_RUN_ATTEMPT") or 0),
        "head": git("rev-parse", "HEAD", cwd=root).strip(),
        "runner": runner(environment),
    }


def annotation(record: dict[str, Any]) -> str:
    """The workflow command that writes ``record`` as a notice annotation titled ``TITLE``."""
    message = json.dumps(record, separators=(",", ":"), sort_keys=True)
    return f"::notice title={TITLE}::" + message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A") + "\n"


def main() -> int:
    """Write the proof of this job to standard output, where the runner turns it into an annotation."""
    sys.stdout.write(annotation(proof(Path.cwd(), os.environ)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
