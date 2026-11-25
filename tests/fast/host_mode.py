"""The settings, figures and refusals that the host mode of the Silver acceptance runners shares (``tests/e2e/silver_signals.py`` and
``silver_hypotension.py``, both untracked).

A runner in host mode runs natively on the build and test host: its own Compose project, the host's Docker with linux/amd64 images and no emulation, the
pinned files in folders that the caller names, and the Spark image that the host's deploy built (``SPARK_IMAGE``). The mode is on only for the exact value
``1``. In host mode no folder has a default, and a folder must be an absolute path. A settings error names the setting and never its value.

Every wait of a host run has a figure here. The figures are stated, not measured: no run on the host has timed these steps yet. The basis is the one
emulated run that reached the Silver job (the decoder load took 59 seconds under emulation; the Silver job did not finish in 3,500 seconds and was idle,
which is the open diagnostic of the signals card). The host runs the same images natively on 8 vCPUs, so a step that is not stalled is far inside its
limit, and a stalled step is stopped after half of the local wait. The run has a deadline of its own, checked before each job starts, so that the whole run
ends within ``ceiling_seconds`` however many jobs it holds.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

MIB = 1024 * 1024
FOLDER_SETTINGS = ("LANDED_CACHE", "SIGNAL_CASE_CACHE")
IMAGE_REFERENCE = re.compile(
    r"(?:[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?(?::[0-9]+)?/)?[a-z0-9]+(?:[._-][a-z0-9]+)*(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*:[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}"
)


class HostSettingError(ValueError):
    """A setting that cannot be used. Its text is fixed: it names the setting and never holds the value."""


@dataclass(frozen=True)
class Limits:
    """The seconds that each wait of a run may take. ``job_deadline_seconds`` is the deadline that the job gives itself (``JOB_DEADLINE_SECONDS``), always
    shorter than ``job_seconds``, the runner's outer limit. ``run_seconds`` is the deadline of the whole run, or None when the run has none."""

    command_seconds: int
    build_seconds: int
    job_seconds: int
    job_deadline_seconds: int
    health_seconds: int
    call_seconds: int
    run_seconds: int | None = None

    @property
    def ceiling_seconds(self) -> int | None:
        """The longest that the steps of a run take: no job starts after the run deadline, and the last one that started before it runs to its limit."""
        return None if self.run_seconds is None else self.run_seconds + self.job_seconds


LOCAL_LIMITS = Limits(180, 3000, 3600, 3500, 300, 30)
HOST_LIMITS = Limits(180, 1800, 1800, 1700, 300, 30, 5400)


@dataclass(frozen=True)
class Folders:
    """The folders of the pinned files, or None for one that is not usable, and the problems in fixed words."""

    landed: Path | None
    extra: Path | None
    problems: tuple[str, ...]


@dataclass(frozen=True)
class Image:
    """The Spark image of the run and whether the run builds it (and then removes it) or only uses it and leaves it."""

    tag: str
    built_here: bool


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
    """The image that the run uses: the one that ``SPARK_IMAGE`` names (it must exist, and the run leaves it), or else the run's own tag, which it builds and
    removes. An empty setting is no setting. A reference without a tag, or one that ``docker`` could read as an option or as two words, is refused."""
    given = environ.get("SPARK_IMAGE", "")
    if given == "":
        return Image(tag, True)
    if IMAGE_REFERENCE.fullmatch(given) is None:
        raise HostSettingError("SPARK_IMAGE is not an image name with a tag")
    return Image(given, False)


def memory_problem(total_bytes: int, available_mib: int | None, needed_bytes: int, host: bool) -> str | None:
    """A fixed code for the memory that is not enough, or None. The daemon holds twice the memory of the services' limits in both modes. On the host the
    deployed stack already holds memory, so the needed memory must also be free now; a host that gives no free figure is refused, never assumed to fit."""
    if total_bytes < 2 * needed_bytes:
        return "daemon_memory"
    if not host:
        return None
    if available_mib is None:
        return "available_unknown"
    return "available_memory" if available_mib * MIB < needed_bytes else None


def run_expired(started: float, now: float, limits: Limits) -> bool:
    """True when the run has a deadline and it has passed. A clock that went back never expires the run."""
    return limits.run_seconds is not None and now - started >= limits.run_seconds
