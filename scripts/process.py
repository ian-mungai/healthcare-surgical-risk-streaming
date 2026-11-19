"""The one place this project's scripts start another program.

Every call resolves the program to its full path, passes a list of arguments, closes stdin or feeds it the given text
and applies a deadline. The program runs in its own process group. On every exit, a normal one, a deadline that raises
``TimeoutExpired``, an interrupt or any other error, the whole group is killed and the call waits until the group is gone.
A process that survives the kill raises ``ProcessGroupSurvived``. A descendant that starts a session of its own leaves the
group and is outside this cleanup. Other modules import ``run_command`` instead of ``subprocess``.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import signal
import subprocess
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from subprocess import CompletedProcess, TimeoutExpired

__all__ = ["CompletedProcess", "ProcessGroupSurvived", "TimeoutExpired", "run_command"]

KILL_WAIT_SECONDS = 10
POLL_SECONDS = 0.02


class ProcessGroupSurvived(RuntimeError):
    """A process of the program's group was still running after SIGKILL and the wait."""


def stop_group(process: subprocess.Popen[str]) -> None:
    """Kill the program's process group, reap the program and wait until no process of the group is left."""
    group = process.pid
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(group, signal.SIGKILL)
    deadline = time.monotonic() + KILL_WAIT_SECONDS
    try:
        process.wait(timeout=KILL_WAIT_SECONDS)
    finally:
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None:
                stream.close()
    while True:
        try:
            os.killpg(group, 0)
        except ProcessLookupError:
            return
        except PermissionError:
            pass
        if time.monotonic() > deadline:
            raise ProcessGroupSurvived(f"process group {group} still had a process {KILL_WAIT_SECONDS} seconds after SIGKILL")
        time.sleep(POLL_SECONDS)


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
    finally:
        stop_group(process)
    if check and process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, command, stdout, stderr)
    return CompletedProcess(command, process.returncode, stdout, stderr)
