"""The one place this project's scripts start another program.

Every call resolves the program to its full path, passes a list of arguments, closes stdin or feeds it the given text
and applies a deadline. The program runs in its own process group. On every exit, a normal one, a deadline that raises
``TimeoutExpired``, an interrupt or any other error, the whole group is killed and the call waits until the group is gone.
A process that survives the kill raises ``ProcessGroupSurvived``. A ``TimeoutExpired`` leaves the launcher holding the program's path and
no argument, so a failed cleanup that follows it shows no argument. Each tool boundary still catches it and raises its own error with
``timeout_text``. A descendant that starts a session of its own leaves the
group and is outside this cleanup. Other modules import ``run_command`` instead of ``subprocess``.

Output is text with every line ending converted to a newline. A call with ``keep_newlines`` gets its output exactly as the program wrote
it: carriage returns stay, and a byte that is not UTF-8 comes back as the surrogate that ``os.fsencode`` turns into the same byte.
That form is for the NUL-separated file names of Git, where a carriage return is part of a name.
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
from typing import Any

__all__ = ["CompletedProcess", "ProcessGroupSurvived", "TimeoutExpired", "partial_text", "run_command", "timeout_text"]

KILL_WAIT_SECONDS = 10
POLL_SECONDS = 0.02


class ProcessGroupSurvived(RuntimeError):
    """A process of the program's group was still running after SIGKILL and the wait."""


def timeout_text(label: str, deadline: float) -> str:
    """The text of a timeout: the caller's label for the command and the deadline, and never the command's arguments, which can hold private names."""
    return f"{label} timed out after {deadline:g} seconds"


def partial_text(value: bytes | str | None) -> str:
    """The output a timed-out program had written, which ``TimeoutExpired`` holds as bytes or text, or the empty text."""
    return value.decode(errors="replace") if isinstance(value, bytes) else (value or "")


def stop_group(process: subprocess.Popen[Any]) -> None:
    """Kill the program's process group, reap the program and wait until no process of the group is left."""
    group = process.pid
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(group, signal.SIGKILL)
    deadline = time.monotonic() + KILL_WAIT_SECONDS
    reaped = True
    try:
        process.wait(timeout=KILL_WAIT_SECONDS)
    except TimeoutExpired:
        reaped = False
    finally:
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None:
                stream.close()
    if not reaped:
        raise ProcessGroupSurvived(f"process {group} was not reaped {KILL_WAIT_SECONDS} seconds after SIGKILL")
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
    keep_newlines: bool = False,
) -> CompletedProcess[str]:
    """Run ``program`` with ``args`` and return its exit code and captured output.

    ``timeout`` is the deadline in seconds. ``check`` raises ``CalledProcessError`` on a non-zero exit. ``keep_newlines`` returns the output
    without newline conversion.
    """
    command = [resolve(program, cwd, env), *args]
    stdin = subprocess.PIPE if input_text is not None else subprocess.DEVNULL
    process = subprocess.Popen(  # noqa: S603 - the sole launcher: full path, list arguments, no shell, closed stdin and a deadline
        command, stdin=stdin, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=not keep_newlines, cwd=cwd, env=env, start_new_session=True
    )
    try:
        stdin_data = input_text if input_text is None or not keep_newlines else os.fsencode(input_text)
        stdout, stderr = process.communicate(stdin_data, timeout=timeout)
    except TimeoutExpired as error:
        error.cmd, error.args = command[0], (command[0], timeout)
        raise
    finally:
        stop_group(process)
    if keep_newlines:
        stdout, stderr = os.fsdecode(stdout), os.fsdecode(stderr)
    if check and process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, command, stdout, stderr)
    return CompletedProcess(command, process.returncode, stdout, stderr)
