"""A stand-in for the Docker engine, with the rules that the one Spark slot relies on, for the tests of every Spark launcher.

The engine keeps its containers in a dictionary and answers the few commands that the launchers send: ``docker compose ... run`` (it starts a container, and
refuses a name that exists with the daemon's conflict text), ``docker container ls`` with the name filter, ``docker container stop`` and
``docker container rm``.
What a started job does is the ``on_start`` function: it returns the job's result, or raises ``TimeoutExpired`` to show a client that was killed while its
container runs on. The names and the label key are literals, so a test fails when the module changes them by mistake.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from subprocess import CompletedProcess
from typing import Any

SLOT_NAME = "surgical-risk-spark-slot"
OWNER_LABEL = "surgical-risk.spark_slot_owner"
# The whole refusal of Docker Engine 29.9.3 for a name that exists (``nameConflictError`` in ``daemon/errors.go``), as the client shows it.
CONFLICT = (
    'Error response from daemon: Conflict. The container name "/{name}" is already in use by container "{other}". '
    "You have to remove (or rename) that container to be able to reuse that name."
)


@dataclass
class Box:
    id: str
    name: str
    state: str
    labels: dict[str, str]
    auto_remove: bool = False


def with_a_free_slot(run: Callable[..., tuple[int, str, str]]) -> Callable[..., tuple[int, str, str]]:
    """The ``run`` of a runner's stand-in on an engine whose slot is free: it answers the reads of the slot's name with no container, which a launch makes
    before and after its job, and gives every other command to ``run``."""

    def answer(command: list[str], *arguments: Any, **keywords: Any) -> tuple[int, str, str]:
        if command[:3] == ["docker", "container", "ls"]:
            return 0, "", ""
        return run(command, *arguments, **keywords)

    return answer


def finishes(code: int = 0, out: str = "", err: str = "") -> Callable[[Engine, Box], CompletedProcess[str]]:
    return lambda engine, box: CompletedProcess([], code, out, err)


@dataclass
class Engine:
    boxes: dict[str, Box] = field(default_factory=dict)
    calls: list[list[str]] = field(default_factory=list)
    on_start: Callable[[Engine, Box], CompletedProcess[str]] = field(default_factory=finishes)
    removal_fails: bool = False
    counter: int = 0

    def hold(self, name: str = SLOT_NAME, *, owner: str = "another_launcher", state: str = "running") -> Box:
        """A container that exists already, as one that another launcher started."""
        return self.add(name, state, {OWNER_LABEL: owner} if owner else {}, False)

    def add(self, name: str, state: str, labels: dict[str, str], auto_remove: bool) -> Box:
        self.counter += 1
        box = Box(f"{self.counter:064x}", name, state, labels, auto_remove)
        self.boxes[box.id] = box
        return box

    def named(self, name: str = SLOT_NAME) -> list[Box]:
        return [box for box in self.boxes.values() if box.name == name]

    def launches(self) -> list[list[str]]:
        return [call for call in self.calls if "run" in call and call[0] == "compose"]

    def docker(self, arguments: Sequence[str], timeout: float) -> CompletedProcess[str]:
        words = list(arguments)
        self.calls.append(words)
        if words[0] == "compose":
            return self.run(words)
        if words[:2] == ["container", "ls"]:
            return self.listing(words)
        if words[:2] == ["container", "stop"]:
            box = self.boxes.get(words[-1])
            if box is not None:
                box.state = "exited"
            return CompletedProcess(words, 0, words[-1], "")
        if words[:2] == ["container", "rm"]:
            if not self.removal_fails:
                self.boxes.pop(words[-1], None)
            return CompletedProcess(words, 0, words[-1], "")
        return CompletedProcess(words, 1, "", "unknown command")

    def run_command(self, program: str, arguments: Sequence[str], *, timeout: float, env: object = None, **_: object) -> CompletedProcess[str]:
        """The stand-in for ``scripts.process.run_command`` of every launcher module."""
        if program != "docker":
            raise AssertionError(program)
        return self.docker(arguments, timeout)

    def listing(self, words: list[str]) -> CompletedProcess[str]:
        pattern = next(word.removeprefix("name=") for word in words if word.startswith("name="))
        rows = [f"{box.id}|{box.state}|{box.labels.get(OWNER_LABEL, '')}" for box in self.boxes.values() if re.search(pattern, f"/{box.name}")]
        return CompletedProcess(words, 0, "".join(f"{row}\n" for row in rows), "")

    def run(self, words: list[str]) -> CompletedProcess[str]:
        tail = words[words.index("run") + 1 :]
        name = tail[tail.index("--name") + 1] if "--name" in tail else f"stack_example-spark_job-run-{self.counter + 1}"
        labels = dict(item.split("=", 1) for flag, item in zip(tail, tail[1:], strict=False) if flag == "--label")
        existing = self.named(name)
        if existing:
            return CompletedProcess(words, 1, "", CONFLICT.format(name=name, other=existing[0].id) + "\n")
        box = self.add(name, "running", labels, "--rm" in tail)  # the engine removes it at its exit only when a launch asks for that
        result = self.on_start(self, box)  # a TimeoutExpired from here leaves the container as it is: the client is gone and the container is not
        if box.id in self.boxes:
            if box.auto_remove:
                del self.boxes[box.id]
            else:
                box.state = "exited"
        return CompletedProcess(words, result.returncode, result.stdout, result.stderr)
