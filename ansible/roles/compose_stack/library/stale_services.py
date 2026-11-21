"""Ansible module: the services that run with an older copy of a configuration file than the one placed on the host.

Compose compares a service's definition when it starts the stack, not the content of the files that its ``configs`` name, so a changed file leaves the
running container on the old copy. The module reads the files that each service mounts from ``docker compose config`` and the start time of each
container, and names the services that have a container and a file newer than that container's start. A service with no container yet is not named,
because its first start reads the file. The module changes nothing. It uses only the standard library and runs on the host's Python.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from ansible.module_utils.basic import AnsibleModule

ERROR_CHARACTERS = 300


class StaleError(RuntimeError):
    """The output of Docker is not what the module reads. The message holds no file content."""


def service_files(config: Any) -> dict[str, list[Path]]:
    """The files of the ``configs`` of each service in a ``docker compose config`` document. A config with no file is not a file to watch."""
    if not isinstance(config, dict) or not isinstance(config.get("services"), dict):
        raise StaleError("the compose configuration holds no services")
    defined = config.get("configs") or {}
    files: dict[str, list[Path]] = {}
    for name, service in config["services"].items():
        files[name] = []
        for entry in (service or {}).get("configs") or []:
            source = entry if isinstance(entry, str) else entry.get("source") if isinstance(entry, dict) else None
            if not isinstance(source, str) or source not in defined:
                raise StaleError(f"service {name} names a config that the configuration does not define")
            file = defined[source].get("file")
            if file:
                files[name].append(Path(file))
    return files


def parse_started(text: str) -> float:
    """The seconds since the epoch of a container's start time, as Docker prints it (nanoseconds and a zone letter)."""
    try:
        return datetime.fromisoformat(text.strip()).timestamp()
    except ValueError as error:
        raise StaleError("a container start time is not a time") from error


def parse_containers(text: str) -> dict[str, str]:
    """The container of each service from ``docker compose ps --format json``, printed as one document or as one document for each line."""
    if not text.strip():
        return {}
    try:
        document = json.loads(text)
        items = document if isinstance(document, list) else [document]
    except ValueError:
        try:
            items = [json.loads(line) for line in text.splitlines() if line.strip()]
        except ValueError as error:
            raise StaleError("the container list is not JSON") from error
    containers: dict[str, str] = {}
    for item in items:
        if not isinstance(item, dict) or not isinstance(item.get("Service"), str) or not isinstance(item.get("ID"), str):
            raise StaleError("a container of the list names no service or no identifier")
        containers[item["Service"]] = item["ID"]
    return containers


def stale_services(files: dict[str, list[Path]], started: dict[str, float], modified: Callable[[Path], float]) -> list[str]:
    """The services, in list order, that have a container and a file modified after that container started."""
    return [name for name, paths in files.items() if name in started and any(modified(path) > started[name] for path in paths)]


def run(module: AnsibleModule, arguments: list[str]) -> str:
    code, output, error = module.run_command(arguments, check_rc=False)
    if code != 0:
        step = next((word for word in arguments if word in ("config", "ps", "inspect")), "command")
        raise StaleError(f"docker {step} exited with {code}: {error.strip()[:ERROR_CHARACTERS]}")
    return str(output)


def main() -> None:
    module = AnsibleModule(argument_spec={"compose_command": {"type": "list", "elements": "str", "required": True}}, supports_check_mode=True)
    command: list[str] = module.params["compose_command"]
    try:
        files = service_files(json.loads(run(module, [*command, "config", "--format", "json"])))
        containers = parse_containers(run(module, [*command, "ps", "--all", "--format", "json"]))
        started: dict[str, float] = {}
        if containers:
            lines = run(module, [command[0], "inspect", "--format", "{{ .Id }} {{ .State.StartedAt }}", *containers.values()]).splitlines()
            by_id = dict(line.split(" ", 1) for line in lines if " " in line)
            for service, identifier in containers.items():
                moment = next((text for full, text in by_id.items() if full.startswith(identifier) or identifier.startswith(full)), None)
                if moment is not None:
                    started[service] = parse_started(moment)
        names = stale_services(files, started, lambda path: os.stat(path).st_mtime)
    except (StaleError, ValueError, OSError) as error:
        module.fail_json(msg=str(error))
    module.exit_json(changed=False, stale=names)


if __name__ == "__main__":
    main()
