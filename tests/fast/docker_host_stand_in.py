"""A stand-in for the live host's Docker as it answered on 2026-11-24.

The Compose project is ``surgical_risk`` and each container is ``surgical_risk-<service>-1``. It answers the commands that the runners send over SSH as
``sudo <command>`` and no others. A command that names another Compose project finds no container, as on the host, where the projects the runners used
before (``p2_join3_monitoring`` and ``p2_caddy``) do not exist. It starts nothing.
"""

from __future__ import annotations

import hashlib
import shlex
from types import SimpleNamespace

PROJECT = "surgical_risk"
HEALTHY = {"postgres": "healthy", "prometheus": "healthy", "grafana": "healthy", "loki": "healthy", "alloy": "healthy", "caddy": "healthy"}


class DockerHost:
    """``services`` maps each service of the deployed project to its health status. A service in ``stopped`` exists and is not running."""

    def __init__(self, services: dict[str, str] | None = None) -> None:
        self.services = dict(HEALTHY if services is None else services)
        self.stopped: set[str] = set()
        self.commands: list[str] = []

    def identifier(self, service: str) -> str:
        return hashlib.sha256(service.encode()).hexdigest()[:12]

    def service_of(self, identifier: str) -> str | None:
        return next((service for service in self.services if self.identifier(service) == identifier), None)

    def option(self, words: list[str], name: str) -> str:
        return words[words.index(name) + 1] if name in words else ""

    def answer(self, command: str) -> tuple[int, str]:
        self.commands.append(command)
        words = shlex.split(command)
        if words[:1] == ["sudo"]:
            words = words[1:]
        if words[:2] == ["docker", "compose"] and "ps" in words:
            service = words[-1]
            if self.option(words, "--project-name") != PROJECT or service not in self.services:
                return 0, ""
            return 0, self.identifier(service) + "\n"
        if words[:2] == ["docker", "inspect"]:
            found = self.service_of(words[-1])
            if found is None:
                return 1, f"Error: No such object: {words[-1]}\n"
            if "State.Status" in words[3]:
                return 0, f"running/{self.services[found]}\n"
            if "State.Health.Status" in words[3]:
                return 0, self.services[found] + "\n"
            return 1, "unsupported format\n"
        if words[:2] == ["docker", "ps"]:
            filters = [words[index + 1] for index, word in enumerate(words) if word == "--filter"]
            if f"label=com.docker.compose.project={PROJECT}" in filters:
                listed = [service for service in self.services if "status=running" not in filters or service not in self.stopped]
                return 0, "".join(f"{service}\n" for service in listed)
            names = [item.removeprefix("name=^").removesuffix("$") for item in filters if item.startswith("name=")]
            return 0, "".join(self.identifier(service) + "\n" for service in self.services if f"{PROJECT}-{service}-1" in names)
        return 127, f"{words[0]}: not served by the stand-in\n"

    def run_command(self, program: str, arguments: list[str], **_: object) -> SimpleNamespace:
        """The shape of ``scripts.process.run_command`` for an ``ssh`` call: the remote command is the last argument."""
        code, output = self.answer(arguments[-1])
        return SimpleNamespace(returncode=code, stdout=output, stderr="")
