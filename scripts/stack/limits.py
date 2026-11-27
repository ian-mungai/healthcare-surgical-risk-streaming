"""Load and check the limits file, the one source of every memory limit in the stack.

``compose/limits.yml`` holds the host's reserve and one entry for each service: its runtime (``compose`` or ``k3s``), its service group and its memory
limit in MiB. Compose reads the limits as variables that ``render`` writes, so no limit is typed in a Compose file. The admission check reads the
same file through ``load_limits``.

``.venv/bin/python -m scripts.stack.limits render`` prints the Compose variables, one line for each Compose service.
"""

from __future__ import annotations

import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from scripts.ingestion.notice import describe_error
from scripts.project_config import ROOT
from scripts.stack.yaml_text import YamlError, load_yaml

LIMITS_FILE = ROOT / "compose" / "limits.yml"
SERVICE_NAME = re.compile(r"[a-z][a-z0-9_]*")
TOP_LEVEL_KEYS = {"host_reserve_mib", "services"}
SERVICE_KEYS = {"runtime", "group", "memory_limit_mib"}
Runtime = Literal["compose", "k3s"]
RUNTIMES: tuple[Runtime, ...] = ("compose", "k3s")


class LimitsError(ValueError):
    """The limits file is missing or malformed."""


@dataclass(frozen=True)
class ServiceLimit:
    runtime: Runtime
    group: str
    memory_limit_mib: int


@dataclass(frozen=True)
class Limits:
    host_reserve_mib: int
    services: Mapping[str, ServiceLimit]


def variable_name(service: str) -> str:
    """The Compose variable that carries a service's memory limit."""
    return f"MEM_LIMIT_{service.upper()}"


def positive_whole_number(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def parse_service(name: str, entry: object) -> ServiceLimit:
    if not isinstance(entry, dict):
        raise LimitsError(f"service {name}: the entry must be a mapping")
    if set(entry) != SERVICE_KEYS:
        raise LimitsError(f"service {name}: the entry must hold exactly {', '.join(sorted(SERVICE_KEYS))}; found {', '.join(sorted(map(str, entry)))}")
    runtime, group, memory = entry["runtime"], entry["group"], entry["memory_limit_mib"]
    if runtime not in RUNTIMES:
        raise LimitsError(f"service {name}: runtime must be one of {', '.join(RUNTIMES)}")
    if not isinstance(group, str) or not SERVICE_NAME.fullmatch(group):
        raise LimitsError(f"service {name}: group must be a lower-case name")
    if not positive_whole_number(memory):
        raise LimitsError(f"service {name}: memory_limit_mib must be a positive whole number")
    return ServiceLimit(runtime, group, memory)


def parse_limits(text: str) -> Limits:
    """The checked limits in ``text``, or ``LimitsError`` naming what is wrong."""
    try:
        document = load_yaml(text)
    except YamlError as error:
        raise LimitsError(str(error)) from error
    if not isinstance(document, dict):
        raise LimitsError("the limits file must be a mapping")
    if set(document) != TOP_LEVEL_KEYS:
        raise LimitsError(f"the limits file must hold exactly {', '.join(sorted(TOP_LEVEL_KEYS))}; found {', '.join(sorted(map(str, document)))}")
    if not positive_whole_number(document["host_reserve_mib"]):
        raise LimitsError("host_reserve_mib must be a positive whole number")
    entries = document["services"]
    if not isinstance(entries, dict) or not entries:
        raise LimitsError("services must be a mapping with at least one service")
    for name in entries:
        if not isinstance(name, str) or not SERVICE_NAME.fullmatch(name):
            raise LimitsError(f"service name {name!r} must be lower case letters, digits and underscores, starting with a letter")
    return Limits(document["host_reserve_mib"], {name: parse_service(name, entry) for name, entry in entries.items()})


def load_limits(path: Path = LIMITS_FILE) -> Limits:
    try:
        text = path.read_text()
    except OSError as error:
        raise LimitsError(f"cannot read {path.name}: {error.strerror}") from error
    return parse_limits(text)


def render_environment(limits: Limits) -> str:
    """One ``MEM_LIMIT_<SERVICE>=<n>m`` line for each Compose service, in name order."""
    return "".join(f"{variable_name(name)}={entry.memory_limit_mib}m\n" for name, entry in sorted(limits.services.items()) if entry.runtime == "compose")


def main() -> int:
    if sys.argv[1:] != ["render"]:
        sys.stderr.write("usage: python -m scripts.stack.limits render\n")
        return 2
    try:
        sys.stdout.write(render_environment(load_limits()))
    except LimitsError as error:
        sys.stderr.write(f"limits: {describe_error(error)}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
