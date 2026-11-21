"""A job that needs a service group declares the resource ``GroupResource`` and runs inside the group.

The resource asks the admission check, starts the group and waits for its services to be healthy before the first step, and stops what it started after the
last step, also when a step failed or the start itself failed. A service that ran before the job is left running. The run's tags record what the job
started and what is left running, so a failed job's record says whether anything remains. Every wait has a deadline, and a failure names the service and
holds no text of a container. The job must use the in-process executor: the resource is set up once for the whole run.
"""

from __future__ import annotations

import contextlib
import json
import os
from collections.abc import Generator

from dagster import ConfigurableResource, InitResourceContext

from scripts.stack.groups import (
    GroupError,
    Settings,
    describe_states,
    find_group,
    read_states,
    running_services,
    settings_from_environment,
    start_group,
    stop_group,
)
from scripts.stack.groups_k3s import control_from_environment
from scripts.stack.limits import Limits, load_limits

TAG_PREFIX = "surgical_risk/group"
TAG_VALUE_LIMIT = 250
NOTHING = "none"


def current_settings() -> Settings:
    return settings_from_environment(k3s=control_from_environment(os.environ))


def tag_text(names: list[str] | tuple[str, ...]) -> str:
    return (",".join(names) or NOTHING)[:TAG_VALUE_LIMIT]


class GroupResource(ConfigurableResource):
    """The service group that a job needs, with the deadlines of its waits in seconds."""

    group: str
    wait_seconds: float = 600.0
    start_deadline_seconds: float = 300.0
    stop_deadline_seconds: float = 120.0

    def services_text(self) -> str:
        """The state of the group's Compose services and the k3s workloads that are ready, for the job's own log."""
        settings = current_settings()
        limits = load_limits(settings.limits_file)
        found = find_group(limits, self.group)
        states = read_states(settings)
        text = describe_states([states[name] for name in found.compose if name in states], [name for name in found.compose if name not in states])
        ready = sorted(running_services(settings, limits) & set(found.k3s))
        return "; ".join(part for part in (text, f"k3s workloads ready: {', '.join(ready) or 'none'}" if found.k3s else "") if part)

    @contextlib.contextmanager
    def yield_for_execution(self, context: InitResourceContext) -> Generator[GroupResource]:
        settings = current_settings()
        limits = load_limits(settings.limits_file)
        before = running_services(settings, limits)
        try:
            result = start_group(
                settings,
                self.group,
                wait_seconds=self.wait_seconds,
                deadline_seconds=self.start_deadline_seconds,
                log=lambda message: context.log.info(message),
            )
            context.log.info(f"group {self.group} is up: started {', '.join(result.started) or 'nothing'}")
            yield self
        finally:
            self.close(context, settings, limits, before)

    def close(self, context: InitResourceContext, settings: Settings, limits: Limits, before: frozenset[str]) -> None:
        """Stop what this run started, write the run's record of what was started and what is left running, then raise if the stop failed."""
        problems: list[str] = []
        stopped: tuple[str, ...] = ()
        left: list[str] | None = None
        try:
            stopped = stop_group(settings, self.group, keep=before, deadline_seconds=self.stop_deadline_seconds, log=lambda message: context.log.info(message))
        except GroupError as error:
            problems.append(str(error))
        try:
            left = sorted(running_services(settings, limits) - before)
        except GroupError as error:
            problems.append(f"what is left running is unknown: {error}")
        record = {"group": self.group, "stopped": list(stopped), "left_running": left, "problems": problems}
        context.log.info(f"group record: {json.dumps(record)}")
        if context.dagster_run is not None and context.instance is not None:
            tags = {
                f"{TAG_PREFIX}/{self.group}/stopped": tag_text(stopped),
                f"{TAG_PREFIX}/{self.group}/left_running": "unknown" if left is None else tag_text(left),
            }
            context.instance.add_run_tags(context.dagster_run.run_id, tags)
        if problems:
            raise GroupError("; ".join(problems))
