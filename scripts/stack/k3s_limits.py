"""Hold Airbyte's k3s workloads to the limits file, the one source of every memory limit.

``compose/limits.yml`` has one entry with ``runtime: k3s`` for each Airbyte workload that the chart starts. ``WORKLOADS`` says where each entry
lands in the chart's values, so the tracked values file types no memory figure: ``values`` renders them from the limits file, the way ``limits``
renders the Compose variables. One entry, the hook pod that creates MinIO's buckets, has no resources setting in the chart. Its figure becomes
the default container limit of Airbyte's namespace (``limit-range``), so no container there runs without a memory limit.

Commands, run from the repository root with ``.venv/bin/python -m scripts.stack.k3s_limits``:

``values``                  the memory request and limit of every chart workload, as a Helm values file
``limit-range <namespace>`` the namespace's default container limit, as a Kubernetes manifest
``check-chart``             reads the chart's default values on standard input and checks them against the tracked values and the limits file

The memory request equals the limit, so what the scheduler reserves is what the admission check counts. Exit status: 0 good, 1 a problem, 2 wrong usage.
"""

from __future__ import annotations

import importlib
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from scripts.project_config import ROOT
from scripts.stack.limits import Limits, LimitsError, load_limits
from scripts.stack.yaml_text import YamlError, load_yaml

TRACKED_VALUES = ROOT / "ansible" / "roles" / "airbyte" / "files" / "values.yaml"
GROUP = "ingestion"
RUNTIME = "k3s"
LIMIT_RANGE_NAME = "memory-defaults"
Shape = Literal["resources", "memory", "limit_range"]

# Every top-level block of chart 2.5.0 that starts a workload. The tracked values set `enabled` for each one, so a chart upgrade that adds one is refused.
CHART_WORKLOAD_KEYS = (
    "webapp",
    "server",
    "worker",
    "workloadLauncher",
    "connectorRolloutWorker",
    "metrics",
    "airbyteBootloader",
    "temporal",
    "temporalUi",
    "postgresql",
    "minio",
    "cron",
    "keycloak",
    "manifestServer",
    "workloadApiServer",
    "directJobRunner",
    "featureflagServer",
    "stiggSidecar",
)
NOT_A_WORKLOAD = {"ingress"}
SECRET_KEY = re.compile(r"password|secret|token|credentials|accesskey|apikey|privatekey", re.IGNORECASE)
NAME_OF_A_SECRET = re.compile(r"(SecretKey|SecretName|secretName)$")
EXPOSING_KEYS = {"hostNetwork", "hostPort"}


@dataclass(frozen=True)
class Setting:
    """Where an entry's figure goes: a path in the chart's values and the shape of the mapping there."""

    path: tuple[str, ...]
    shape: Shape


LIMIT_RANGE_ENTRY = "airbyte_minio_create_bucket"
WORKLOADS: dict[str, Setting] = {
    "airbyte_server": Setting(("server", "resources"), "resources"),
    "airbyte_worker": Setting(("worker", "resources"), "resources"),
    "airbyte_workload_launcher": Setting(("workloadLauncher", "resources"), "resources"),
    "airbyte_workload_api_server": Setting(("workloadApiServer", "resources"), "resources"),
    "airbyte_cron": Setting(("cron", "resources"), "resources"),
    "airbyte_manifest_server": Setting(("manifestServer", "resources"), "resources"),
    "airbyte_bootloader": Setting(("airbyteBootloader", "resources"), "resources"),
    "airbyte_temporal": Setting(("temporal", "resources"), "resources"),
    "airbyte_temporal_admin": Setting(("temporal", "adminTools", "resources"), "resources"),
    "airbyte_minio": Setting(("minio", "resources"), "resources"),
    LIMIT_RANGE_ENTRY: Setting((), "limit_range"),
    "airbyte_job_connector": Setting(("global", "workloads", "resources", "mainContainer"), "memory"),
    "airbyte_job_check": Setting(("global", "workloads", "resources", "check"), "memory"),
    "airbyte_job_discover": Setting(("global", "workloads", "resources", "discover"), "memory"),
    "airbyte_job_replication": Setting(("global", "workloads", "resources", "replication"), "memory"),
    "airbyte_job_sidecar": Setting(("global", "workloads", "resources", "sidecar"), "memory"),
}


def dotted(path: tuple[str, ...]) -> str:
    return ".".join(path)


def entry_problems(limits: Limits) -> list[str]:
    """Each way the limits file and ``WORKLOADS`` disagree: a workload with no entry, an entry with no workload, an entry in the wrong runtime or group."""
    problems: list[str] = []
    for name in WORKLOADS:
        entry = limits.services.get(name)
        if entry is None:
            problems.append(f"{name}: a chart workload has no entry in the limits file")
        elif (entry.runtime, entry.group) != (RUNTIME, GROUP):
            problems.append(f"{name}: the entry must have runtime {RUNTIME} and group {GROUP}")
    for name, entry in limits.services.items():
        if entry.runtime == RUNTIME and name not in WORKLOADS:
            problems.append(f"{name}: a k3s entry in the limits file has no workload in the chart table")
    return problems


def figure(limits: Limits, name: str) -> str:
    return f"{limits.services[name].memory_limit_mib}Mi"


def render_values(limits: Limits) -> str:
    """The memory request and limit of every chart workload, as Helm values with the entries' figures."""
    document: dict[str, Any] = {}
    for name, setting in WORKLOADS.items():
        if setting.shape == "limit_range":
            continue
        amount = figure(limits, name)
        node = document
        for part in setting.path[:-1]:
            node = node.setdefault(part, {})
        node[setting.path[-1]] = (
            {"requests": {"memory": amount}, "limits": {"memory": amount}} if setting.shape == "resources" else {"memory": {"request": amount, "limit": amount}}
        )
    return dump(document)


def render_limit_range(limits: Limits, namespace: str) -> str:
    """The default container memory limit of ``namespace``, taken from the hook pod's entry."""
    amount = {"memory": figure(limits, LIMIT_RANGE_ENTRY)}
    manifest = {
        "apiVersion": "v1",
        "kind": "LimitRange",
        "metadata": {"name": LIMIT_RANGE_NAME, "namespace": namespace},
        "spec": {"limits": [{"type": "Container", "default": amount, "defaultRequest": amount}]},
    }
    return dump(manifest)


def dump(document: Mapping[str, Any]) -> str:
    text: str = importlib.import_module("yaml").safe_dump(dict(document), sort_keys=True, default_flow_style=False)
    return text


def walk(node: object, path: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], str, object]]:
    """Every mapping entry of a document as (path of the mapping, key, value)."""
    found: list[tuple[tuple[str, ...], str, object]] = []
    if isinstance(node, dict):
        for key, value in node.items():
            found.append((path, str(key), value))
            found.extend(walk(value, (*path, str(key))))
    elif isinstance(node, list):
        for index, item in enumerate(node):
            found.extend(walk(item, (*path, str(index))))
    return found


def has_memory(node: object) -> bool:
    return any(key == "memory" and value not in ({}, None, "") for _, key, value in walk(node))


def descend(document: object, path: tuple[str, ...]) -> object | None:
    node = document
    for part in path:
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def typed_memory_problems(tracked: Mapping[str, Any]) -> list[str]:
    """Each place where the tracked values type a memory figure that the limits file must give."""
    problems = []
    for name, setting in WORKLOADS.items():
        if setting.shape != "limit_range" and has_memory(descend(tracked, setting.path)):
            problems.append(f"{dotted(setting.path)}: a typed memory figure; the entry {name} of the limits file is the one source")
    return problems


def exposure_problems(tracked: Mapping[str, Any]) -> list[str]:
    """Each way the tracked values would expose a service: an ingress, a service that is not cluster-internal, a node or host port."""
    problems = []
    for path, key, value in walk(tracked):
        where = dotted((*path, key))
        if key == "ingress" and isinstance(value, dict) and value.get("enabled"):
            problems.append(f"{where}: an ingress is on")
        elif key == "service" and isinstance(value, dict) and (value.get("type", "ClusterIP") != "ClusterIP" or value.get("nodePort")):
            problems.append(f"{where}: a service that is not a cluster-internal ClusterIP, or that sets a node port")
        elif key in EXPOSING_KEYS and value:
            problems.append(f"{where}: {key} publishes a port outside the cluster network")
    return problems


def secret_value_problems(tracked: Mapping[str, Any]) -> list[str]:
    """Each key that looks like a secret and holds a value. The name of a secret and the name of a key inside it are not secrets."""
    return [
        f"{dotted((*path, key))}: a secret value in a tracked file"
        for path, key, value in walk(tracked)
        if SECRET_KEY.search(key) and not NAME_OF_A_SECRET.search(key) and isinstance(value, str | int | float) and value not in ("", False)
    ]


def workload_keys(document: Mapping[str, Any]) -> list[str]:
    return [key for key, value in document.items() if key not in NOT_A_WORKLOAD and isinstance(value, dict) and "enabled" in value]


def chart_problems(chart: Mapping[str, Any], tracked: Mapping[str, Any], limits: Limits) -> list[str]:
    """Where the chart, the tracked values and the limits file disagree about which workloads exist and which are sized."""
    problems = []
    for key in workload_keys(chart):
        if not isinstance(tracked.get(key), dict) or "enabled" not in tracked[key]:
            problems.append(f"{key}: the chart has this workload and the tracked values do not set enabled for it")
    started = {key for key in workload_keys(tracked) if tracked[key]["enabled"]}
    sized = {setting.path[0] for setting in WORKLOADS.values() if setting.path and setting.path[0] != "global"}
    problems += [f"{key}: the values start this workload and no limits entry sizes it" for key in sorted(started - sized)]
    problems += [f"{key}: a limits entry sizes this workload and the values do not start it" for key in sorted(sized - started)]
    for name, setting in WORKLOADS.items():
        if setting.shape != "limit_range" and descend(chart, setting.path) is None:
            problems.append(f"{dotted(setting.path)}: the chart has no such setting for the entry {name}")
    return problems


def check_chart(chart: Mapping[str, Any], tracked: Mapping[str, Any], limits: Limits) -> list[str]:
    return [
        *entry_problems(limits),
        *chart_problems(chart, tracked, limits),
        *typed_memory_problems(tracked),
        *exposure_problems(tracked),
        *secret_value_problems(tracked),
    ]


def load_mapping(text: str, what: str) -> dict[str, Any]:
    document = load_yaml(text)
    if not isinstance(document, dict):
        raise YamlError(f"{what} must be a mapping")
    return document


def main() -> int:
    arguments = sys.argv[1:]
    try:
        limits = load_limits()
        if arguments == ["values"]:
            sys.stdout.write(render_values(limits))
        elif len(arguments) == 2 and arguments[0] == "limit-range":
            sys.stdout.write(render_limit_range(limits, arguments[1]))
        elif arguments == ["check-chart"]:
            problems = check_chart(load_mapping(sys.stdin.read(), "the chart's values"), load_mapping(TRACKED_VALUES.read_text(), "the tracked values"), limits)
            for problem in problems:
                sys.stderr.write(f"k3s limits: {problem}\n")
            return 1 if problems else 0
        else:
            sys.stderr.write("usage: python -m scripts.stack.k3s_limits values | limit-range <namespace> | check-chart\n")
            return 2
    except (LimitsError, YamlError) as error:
        sys.stderr.write(f"k3s limits: {error}\n")
        return 1
    except OSError:
        sys.stderr.write("k3s limits: a file cannot be read\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
