"""The settings, figures and refusals that the host mode of the Silver acceptance runners shares (``tests/e2e/silver_signals.py`` and
``silver_hypotension.py``, both untracked).

A runner in host mode runs natively on the build and test host: its own Compose project, the host's Docker with linux/amd64 images and no emulation, the
pinned files in folders that the caller names, and the Spark image that the host's deploy built (``SPARK_IMAGE``). The mode is on only for the exact value
``1``. In host mode no folder has a default, and a folder must be an absolute path. A settings error names the setting and never its value.

Every wait of a host run has a figure here. The figures are stated, not measured: no run on the host has timed these steps yet. The basis is the one
emulated run that reached the Silver job (the decoder load took 59 seconds under emulation; the Silver job did not finish in 3,500 seconds and was idle,
which is the open diagnostic of the signals card). The host runs the same images natively on 8 vCPUs, so a step that is not stalled is far inside its
limit, and a stalled step is stopped after half of the local wait. The run has a deadline of its own, and every active wait (a build, a job, a health wait, a
Docker command) gets the shorter of its own limit and the rest of the run (``active_cap``), so that no wait goes past the deadline. The cleanup has a budget of
its own (``cleanup_seconds``, ``cleanup_cap``). The whole run therefore ends within ``ceiling_seconds``, however many jobs it holds, apart from the seconds
that a stopped command takes to leave (``KILL_WAIT_SECONDS`` in ``scripts/process.py``) and one call to Nessie (``call_seconds``) that straddles the deadline.

Every Compose and Docker call of a run gets an explicit environment (``docker_environment``): the path, the home folder, what Docker itself needs and the run's
own binding, never the caller's other settings. Compose lets a variable of the environment beat the env file, so a caller's ``WAREHOUSE_LOCATION`` or
``NESSIE_URI`` would otherwise reach Spark.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

MIB = 1024 * 1024
FOLDER_SETTINGS = ("LANDED_CACHE", "SIGNAL_CASE_CACHE")
# What a Compose or Docker call may take from the caller: where to find the programs, the home folder (the Docker client's plugins and contexts) and the
# settings that choose the daemon. Nothing else: no deployment setting and no ``COMPOSE_*`` setting.
DOCKER_PASSTHROUGH = ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG", "DOCKER_CERT_PATH", "DOCKER_TLS_VERIFY", "XDG_RUNTIME_DIR", "TMPDIR")
IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}")
IMAGE_REFERENCE = re.compile(
    r"(?:[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?(?::[0-9]+)?/)?[a-z0-9]+(?:[._-][a-z0-9]+)*(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*:[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}"
)


class HostSettingError(ValueError):
    """A setting that cannot be used. Its text is fixed: it names the setting and never holds the value."""


@dataclass(frozen=True)
class Limits:
    """The seconds that each wait of a run may take. ``job_deadline_seconds`` is the deadline that the job gives itself (``JOB_DEADLINE_SECONDS``), always
    shorter than ``job_seconds``, the runner's outer limit. ``run_seconds`` is the deadline of the whole run, or None when the run has none.
    ``cleanup_seconds`` is the budget of the cleanup after the run (and after a stopped step), apart from the run's own."""

    command_seconds: int
    build_seconds: int
    job_seconds: int
    job_deadline_seconds: int
    health_seconds: int
    call_seconds: int
    run_seconds: int | None = None
    cleanup_seconds: int = 600

    @property
    def ceiling_seconds(self) -> int | None:
        """The longest that the steps of a run take: no wait goes past the run deadline (it gets the rest of the run as its cap) and the cleanup has its own
        budget. A run without a deadline has no ceiling."""
        return None if self.run_seconds is None else self.run_seconds + self.cleanup_seconds


LOCAL_LIMITS = Limits(180, 3000, 3600, 3500, 300, 30)
HOST_LIMITS = Limits(180, 1800, 1800, 1700, 300, 30, 5400, 600)


@dataclass(frozen=True)
class Folders:
    """The folders of the pinned files, or None for one that is not usable, and the problems in fixed words."""

    landed: Path | None
    extra: Path | None
    problems: tuple[str, ...]


@dataclass(frozen=True)
class Image:
    """The Spark image of the run. ``tag`` is the run's own tag, which the run creates (by a build, or by tagging the verified image) and removes. ``source`` is
    the given image (``SPARK_IMAGE``) that the run only reads and never changes or removes, or None when the run builds. ``expected_id`` is the immutable ID
    that the deploy recorded for the tested build: the given image must have it."""

    tag: str
    built_here: bool
    source: str | None
    expected_id: str | None


def limits_for(host: bool) -> Limits:
    return HOST_LIMITS if host else LOCAL_LIMITS


def host_flag(environ: Mapping[str, str], name: str) -> bool:
    """True for ``1``, False when the flag is not set, empty or ``0``. Any other value is refused, so that a flag written as ``true`` never runs the local mode
    on the host."""
    value = environ.get(name, "")
    if value in ("", "0"):
        return False
    if value == "1":
        return True
    raise HostSettingError(f"{name} must be 1, 0 or not set")


def absolute_folder(name: str, value: str | None, problems: list[str]) -> Path | None:
    """The folder as a path when it is set and absolute. Otherwise None, and the problem is added in fixed words."""
    if value is None:
        problems.append(f"{name} is not set")
        return None
    if not Path(value).is_absolute():
        problems.append(f"{name} is not an absolute path")
        return None
    return Path(value)


def pinned_folders(environ: Mapping[str, str], host: bool, landed_default: Path, extra_default: Path) -> Folders:
    """The folder of the pinned files and the folder of the extra case file. Locally each has a default. On the host none has: the case folder falls back to the
    landed folder, and each folder must be set and absolute."""
    landed_given, extra_given = (environ.get(name) or None for name in FOLDER_SETTINGS)
    if not host:
        return Folders(Path(landed_given or landed_default), Path(extra_given or extra_default), ())
    problems: list[str] = []
    landed = absolute_folder("LANDED_CACHE", landed_given, problems)
    extra = landed if extra_given is None else absolute_folder("SIGNAL_CASE_CACHE", extra_given, problems)
    return Folders(landed, extra, tuple(problems))


def image_choice(environ: Mapping[str, str], tag: str) -> Image:
    """The image of the run. ``tag`` is the run's own tag. With ``SPARK_IMAGE`` the run reads that image (it must exist) and gives its own tag to the image
    whose immutable ID is ``SPARK_IMAGE_ID`` (the ID that the deploy recorded for the tested build), and it leaves the given image as it is. Without it the run
    builds the image under its own tag. An empty setting is no setting. A reference without a tag, or one that ``docker`` could read as an option or as two
    words, an ID that is not a full sha256 ID, a given image without an ID and an ID without an image are refused, and the text never holds the value."""
    given = environ.get("SPARK_IMAGE", "")
    identifier = environ.get("SPARK_IMAGE_ID", "")
    if given == "":
        if identifier != "":
            raise HostSettingError("SPARK_IMAGE_ID is set without SPARK_IMAGE")
        return Image(tag, True, None, None)
    if IMAGE_REFERENCE.fullmatch(given) is None:
        raise HostSettingError("SPARK_IMAGE is not an image name with a tag")
    if identifier == "":
        raise HostSettingError("SPARK_IMAGE_ID is not set")
    if IMAGE_ID.fullmatch(identifier) is None:
        raise HostSettingError("SPARK_IMAGE_ID is not an image ID (sha256: and 64 hexadecimal characters)")
    return Image(tag, False, given, identifier)


def image_id_problem(actual: str, expected: str) -> str | None:
    """A fixed code when the ID that Docker gave for the given image is not the one that the deploy recorded, or None. An ID that is not a full sha256 ID
    (empty, a placeholder, with a line end) is never a match."""
    if IMAGE_ID.fullmatch(actual) is None:
        return "image_id_unreadable"
    return None if actual == expected else "image_id_mismatch"


def read_env_file(text: str) -> dict[str, str]:
    """The ``NAME=value`` lines of a Compose env file. A blank line, a comment and a line without a name or without ``=`` are skipped."""
    values: dict[str, str] = {}
    for line in text.splitlines():
        name, separator, value = line.partition("=")
        if separator and name.strip() and not name.lstrip().startswith("#"):
            values[name.strip()] = value
    return values


def docker_environment(environ: Mapping[str, str], binding: Mapping[str, str] | None = None) -> dict[str, str]:
    """The environment of one Compose or Docker call: the names of ``DOCKER_PASSTHROUGH`` that the caller has, then the run's binding (its catalog, warehouse,
    ports, mounts and memory limits), which beats a caller's value of the same name. No other setting of the caller is passed."""
    result = {name: environ[name] for name in DOCKER_PASSTHROUGH if name in environ}
    result.update(binding or {})
    return result


def available_problem(available_mib: int | None, needed_bytes: int, reserve_bytes: int) -> str | None:
    """A fixed code when the memory that is free now does not cover the services' limits and the host's reserve, or None. A host that gives no free figure is
    refused, never assumed to fit."""
    if available_mib is None:
        return "available_unknown"
    return "available_memory" if available_mib * MIB < needed_bytes + reserve_bytes else None


def memory_problem(total_bytes: int, available_mib: int | None, needed_bytes: int, host: bool, reserve_bytes: int = 0) -> str | None:
    """A fixed code for the memory that is not enough, or None. The daemon holds twice the memory of the services' limits in both modes. On the host the
    deployed stack already holds memory, so the needed memory and the host's reserve (``host_reserve_mib`` of ``compose/limits.yml``) must also be free now."""
    if total_bytes < 2 * needed_bytes:
        return "daemon_memory"
    if not host:
        return None
    return available_problem(available_mib, needed_bytes, reserve_bytes)


def active_cap(started: float, now: float, limits: Limits, step_seconds: float) -> float | None:
    """The seconds that an active step may wait: its own limit, or the rest of the run when that is shorter. None when the run deadline has passed, so that the
    step is not started. A run without a deadline gives every step its own limit, and a clock that went back never shortens a step."""
    if limits.run_seconds is None:
        return step_seconds
    remaining = limits.run_seconds - max(0.0, now - started)
    return None if remaining <= 0 else min(step_seconds, remaining)


def cleanup_cap(cleanup_started: float | None, now: float, limits: Limits, step_seconds: float) -> float | None:
    """The seconds that a cleanup step may wait: its own limit, or what is left of the cleanup budget when that is shorter. ``cleanup_started`` is the time of
    the first cleanup step, or None before it. None when the budget is spent, so that the step is not started and the caller counts it as not confirmed."""
    if cleanup_started is None:
        return min(step_seconds, float(limits.cleanup_seconds))
    remaining = limits.cleanup_seconds - max(0.0, now - cleanup_started)
    return None if remaining <= 0 else min(step_seconds, remaining)


def job_deadline_for(cap: float, limits: Limits) -> int | None:
    """The deadline that a job gives itself when its outer cap is ``cap``: its own deadline when the cap is the full limit, otherwise the cap less the margin
    between the job's limit and its deadline, so that the job reports its own timeout before the runner stops it. None when the cap is not longer than it."""
    margin = limits.job_seconds - limits.job_deadline_seconds
    if cap >= limits.job_seconds:
        return limits.job_deadline_seconds
    deadline = int(cap) - margin
    return deadline if deadline >= 1 else None
