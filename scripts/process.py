"""The one place this project's scripts start another program.

Every call resolves the program to its full path, passes a list of arguments, closes stdin or feeds it the given text
and applies a deadline. The program runs in its own process group. When the deadline passes, the whole group is killed
and the call raises ``TimeoutExpired``. Other modules import ``run_command`` instead of ``subprocess``.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import signal
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from subprocess import CompletedProcess, TimeoutExpired

__all__ = ["CompletedProcess", "TimeoutExpired", "run_command"]

KILL_WAIT_SECONDS = 10


def resolve(program: str, cwd: Path | None, env: Mapping[str, str] | None) -> str:
    if os.sep in program:
        candidate = Path(program) if Path(program).is_absolute() else (cwd or Path.cwd()) / program
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
        raise FileNotFoundError(f"program not found or not executable: {program}")
    found = shutil.which(program, path=None if env is None else env.get("PATH"))
    if found is None:
        raise FileNotFoundError(f"program not found on PATH: {program}")
    return found


def run_command(
    program: str,
    args: Sequence[str],
    *,
    timeout: float,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    input_text: str | None = None,
    check: bool = False,
) -> CompletedProcess[str]:
    """Run ``program`` with ``args`` and return its exit code and captured output.

    ``timeout`` is the deadline in seconds. ``check`` raises ``CalledProcessError`` on a non-zero exit.
    """
    command = [resolve(program, cwd, env), *args]
    stdin = subprocess.PIPE if input_text is not None else subprocess.DEVNULL
    process = subprocess.Popen(command, stdin=stdin, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=cwd, env=env, start_new_session=True)  # noqa: S603 - the sole launcher: full path, list arguments, no shell, closed stdin and a deadline
    try:
        stdout, stderr = process.communicate(input_text, timeout=timeout)
    except TimeoutExpired:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        process.communicate(timeout=KILL_WAIT_SECONDS)
        raise
    if check and process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, command, stdout, stderr)
    return CompletedProcess(command, process.returncode, stdout, stderr)
