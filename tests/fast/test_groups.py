"""The group commands take their groups from the limits file, name the Compose profile only for a group that has one, refuse what must never be stopped
and report a service's state through a closed list of fields, so a container's own text never reaches a message.

Each case changes one fact of a good setup and expects the result to follow from that fact alone.
"""

from __future__ import annotations

import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.project_config import ROOT
from scripts.stack.groups import (
    AISTOR_STATE_NAME,
    ALWAYS_ON_GROUPS,
    LICENSED_SERVICES,
    PROJECT_IMAGE_SETTINGS,
    GroupError,
    ServiceState,
    Settings,
    check_stoppable,
    compose_arguments,
    describe_states,
    find_group,
    groups_of,
    main,
    parse_inspect,
    profile_of,
    rendered_environment,
    start_compose,
    start_group,
    stop_arguments,
    stop_compose,
    up_arguments,
    wait_ready,
)
from scripts.stack.limits import LIMITS_FILE, load_limits, parse_limits
from scripts.stack.yaml_text import load_yaml
from tests.fast.test_groups_k3s import Cluster, control

GOOD = """\
host_reserve_mib: 4096
services:
  postgres:
    runtime: compose
    group: supporting
    memory_limit_mib: 1024
  batch_worker:
    runtime: compose
    group: batch
    memory_limit_mib: 2048
  batch_helper:
    runtime: compose
    group: batch
    memory_limit_mib: 512
  airbyte_worker:
    runtime: k3s
    group: ingestion
    memory_limit_mib: 1024
  ingest_gateway:
    runtime: compose
    group: ingestion
    memory_limit_mib: 256
"""


def settings(host_env: Path | None = None) -> Settings:
    return Settings(compose_directory=Path("/stack/compose"), project="stack_project", host_env=host_env)


class GroupsTest(unittest.TestCase):
    def test_every_service_of_the_tracked_limits_file_is_in_exactly_one_group_by_its_runtime(self) -> None:
        limits = load_limits(LIMITS_FILE)
        found = groups_of(limits)
        compose = sorted(name for group in found.values() for name in group.compose)
        k3s = sorted(name for group in found.values() for name in group.k3s)
        self.assertEqual(sorted(name for name, entry in limits.services.items() if entry.runtime == "compose"), compose)
        self.assertEqual(sorted(name for name, entry in limits.services.items() if entry.runtime == "k3s"), k3s)

    def test_a_group_holds_its_compose_services_and_its_k3s_workloads_apart_in_name_order(self) -> None:
        group = find_group(parse_limits(GOOD), "batch")
        self.assertEqual(("batch_helper", "batch_worker"), group.compose)
        self.assertEqual((), group.k3s)
        mixed = find_group(parse_limits(GOOD), "ingestion")
        self.assertEqual(("ingest_gateway",), mixed.compose)
        self.assertEqual(("airbyte_worker",), mixed.k3s)

    def test_an_unknown_group_is_refused_with_the_groups_that_exist(self) -> None:
        with self.assertRaisesRegex(GroupError, "unknown group 'training'.*batch, ingestion, supporting"):
            find_group(parse_limits(GOOD), "training")

    def test_a_name_that_is_not_a_group_name_is_refused(self) -> None:
        for name in ("", "Batch", "batch;rm", "../batch", "batch worker"):
            with self.subTest(name=name), self.assertRaisesRegex(GroupError, "not a valid group name"):
                find_group(parse_limits(GOOD), name)


class ProfileTest(unittest.TestCase):
    def test_the_always_on_group_has_no_profile_and_every_other_group_is_its_own_profile(self) -> None:
        self.assertEqual(frozenset({"supporting"}), ALWAYS_ON_GROUPS)
        self.assertIsNone(profile_of("supporting"))
        self.assertEqual("batch", profile_of("batch"))

    def test_the_profile_is_named_in_the_command_only_for_a_group_that_has_one(self) -> None:
        self.assertEqual(["--profile", "batch"], compose_arguments(settings(), "batch", "up")[-3:-1])
        self.assertNotIn("--profile", compose_arguments(settings(), "supporting", "up"))

    def test_the_command_names_the_project_and_the_files_and_ends_with_the_given_arguments(self) -> None:
        arguments = compose_arguments(settings(), "batch", "stop", "--timeout", "30", "batch_worker")
        self.assertEqual(
            ["compose", "--project-directory", "/stack/compose", "--file", "/stack/compose/compose.yml", "--project-name", "stack_project"], arguments[:7]
        )
        self.assertEqual(["stop", "--timeout", "30", "batch_worker"], arguments[-4:])

    def test_the_host_settings_file_is_passed_only_when_there_is_one(self) -> None:
        self.assertNotIn("--env-file", compose_arguments(settings(), "batch", "up"))
        arguments = compose_arguments(settings(Path("/stack/compose/host.env")), "batch", "up")
        self.assertEqual(["--env-file", "/stack/compose/host.env"], arguments[arguments.index("--env-file") : arguments.index("--env-file") + 2])


class CommandTest(unittest.TestCase):
    def test_the_start_names_only_the_groups_own_services_and_replaces_no_container(self) -> None:
        arguments = up_arguments(settings(), find_group(parse_limits(GOOD), "batch"))
        self.assertEqual(["up", "--detach", "--no-recreate", "batch_helper", "batch_worker"], arguments[-5:])
        self.assertNotIn("postgres", arguments)
        self.assertIn("--profile", arguments)

    def test_the_start_of_the_always_on_group_names_its_services_and_no_profile(self) -> None:
        arguments = up_arguments(settings(), find_group(parse_limits(GOOD), "supporting"))
        self.assertEqual(["up", "--detach", "--no-recreate", "postgres"], arguments[-4:])
        self.assertNotIn("--profile", arguments)

    def test_the_stop_names_exactly_the_services_it_is_given_and_a_time_to_stop(self) -> None:
        arguments = stop_arguments(settings(), "batch", ["batch_worker"])
        self.assertEqual(["stop", "--timeout", "30", "batch_worker"], arguments[-4:])
        self.assertEqual(["--profile", "batch"], arguments[-6:-4])


class StopTest(unittest.TestCase):
    def test_the_always_on_group_is_never_stopped_by_a_group_command(self) -> None:
        with self.assertRaisesRegex(GroupError, "always on.*supporting"):
            check_stoppable("supporting")

    def test_any_other_group_may_be_stopped(self) -> None:
        check_stoppable("batch")


PLANTED = {name: "planted/shell_value@sha256:" + "f" * 64 for name in sorted(PROJECT_IMAGE_SETTINGS)}


class ProjectImageSettingTest(unittest.TestCase):
    """The digests of the project images come from the rendered host settings file only: a shell variable of the same name would win in interpolation."""

    def test_the_three_project_image_settings_are_the_ones_that_the_pin_list_names(self) -> None:
        self.assertEqual({"IMAGE_DAGSTER", "IMAGE_REPLAY", "IMAGE_SPARK"}, PROJECT_IMAGE_SETTINGS)

    def test_a_shell_variable_of_a_project_image_setting_is_not_in_the_environment_of_a_compose_command_and_the_other_variables_stay(self) -> None:
        with mock.patch.dict(os.environ, PLANTED | {"KEEP_THIS_ONE": "yes", "IMAGE_UNRELATED": "kept"}):
            found = rendered_environment(load_limits(LIMITS_FILE))
        self.assertEqual([], [name for name in PLANTED if name in found])
        self.assertEqual(("yes", "kept"), (found["KEEP_THIS_ONE"], found["IMAGE_UNRELATED"]))
        self.assertTrue(any(name.startswith("MEM_LIMIT_") for name in found), "the memory limits of the limits file are still there")

    def test_the_start_and_the_stop_of_a_group_send_that_environment_to_docker(self) -> None:
        limits = parse_limits(GOOD)
        group = find_group(limits, "batch")
        states = {name: ServiceState(name, "running", "healthy", 0, False, 0) for name in group.compose}
        stopped = {name: ServiceState(name, "exited", "none", 0, False, 0) for name in group.compose}
        with (
            mock.patch.dict(os.environ, PLANTED),
            mock.patch("scripts.stack.groups.docker", return_value="") as docker,
            mock.patch("scripts.stack.groups.read_states", side_effect=[{}, states, stopped]),
            mock.patch("scripts.stack.groups.wait_ready"),
        ):
            start_compose(settings(), limits, group, 5, lambda _text: None)
            stop_compose(settings(), limits, group, (), lambda _text: None)
        self.assertEqual(2, docker.call_count)
        for call in docker.call_args_list:
            self.assertEqual([], [name for name in PLANTED if name in call.kwargs["env"]])


LINE = "{service}|{state}|{health}|{exit_code}|{oom}|{restarts}"


def inspect_text(*rows: dict[str, str]) -> str:
    defaults = {"service": "batch_worker", "state": "running", "health": "healthy", "exit_code": "0", "oom": "false", "restarts": "0"}
    return "\n".join(LINE.format(**(defaults | row)) for row in rows) + "\n"


LICENSED = (
    GOOD
    + """\
  aistor:
    runtime: compose
    group: supporting
    memory_limit_mib: 1024
"""
)


class StorageLicenceGateTest(unittest.TestCase):
    """AIStor needs its licence, so group-up starts it only while the host's record of the AIStor step says that the step started it."""

    def start(self, record: str | None) -> tuple[list[str], tuple[str, ...], list[str]]:
        """The services that one start of the always-on group names to docker, the services it reports as started and its messages."""
        named: list[str] = []
        messages: list[str] = []

        def docker(arguments: list[str], **_options: object) -> str:
            named.extend(arguments[arguments.index("--no-recreate") + 1 :])
            return ""

        def states(*_arguments: object) -> dict[str, ServiceState]:
            return {name: ServiceState(name, "running", "healthy", 0, False, 0) for name in named}

        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            (folder / "limits.yml").write_text(LICENSED)
            if record is not None:
                (folder / "aistor.state").write_text(record)
            found = Settings(compose_directory=folder, project="stack_project", available_mib=100000)
            with mock.patch("scripts.stack.groups.docker", side_effect=docker), mock.patch("scripts.stack.groups.read_states", side_effect=states):
                result = start_group(found, "supporting", log=messages.append)
        return named, result.started, messages

    def test_a_recorded_start_of_aistor_lets_group_up_start_it_with_the_rest_of_the_group(self) -> None:
        named, started, messages = self.start("started\n")
        self.assertEqual(["aistor", "postgres"], named)
        self.assertEqual(("aistor", "postgres"), started)
        self.assertFalse(any("left out" in message for message in messages))

    def test_a_skipped_missing_empty_or_unknown_record_leaves_aistor_out_and_says_so(self) -> None:
        for record in ("skipped: no AIStor licence file was on the host at the last deploy; place the licence and deploy again\n", None, "", "starting\n"):
            with self.subTest(record=record):
                named, started, messages = self.start(record)
                self.assertEqual(["postgres"], named)
                self.assertEqual(("postgres",), started)
                self.assertEqual(1, sum("aistor is left out" in message for message in messages), messages)

    def test_the_record_is_the_file_that_the_aistor_step_writes_in_the_compose_folder(self) -> None:
        variables: Any = load_yaml((ROOT / "ansible" / "group_vars" / "all.yml").read_text())
        self.assertEqual(f"{{{{ compose_stack_compose_directory }}}}/{AISTOR_STATE_NAME}", variables["compose_stack_aistor_state_file"])
        self.assertEqual(list(LICENSED_SERVICES), variables["compose_stack_storage_services"])


class ReadinessTest(unittest.TestCase):
    def ready_states(self) -> dict[str, ServiceState]:
        return {
            "batch_worker": ServiceState("batch_worker", "running", "healthy", 0, False, 0),
            "batch_helper": ServiceState("batch_helper", "running", "none", 0, False, 0),
        }

    def wait(self, clock: list[float]) -> None:
        group = find_group(parse_limits(GOOD), "batch")
        with (
            mock.patch("scripts.stack.groups.read_states", return_value=self.ready_states()),
            mock.patch("scripts.stack.groups.time.monotonic", side_effect=clock),
        ):
            wait_ready(settings(), group, 300.0, lambda message: None)

    def test_services_that_are_ready_inside_the_deadline_are_accepted(self) -> None:
        self.wait([0.0, 299.0])

    def test_services_that_are_ready_only_after_the_deadline_are_not_accepted(self) -> None:
        with self.assertRaisesRegex(GroupError, "group batch: ready after the deadline of 300 seconds"):
            self.wait([0.0, 301.0])

    def test_services_that_are_ready_exactly_at_the_deadline_are_not_accepted(self) -> None:
        with self.assertRaisesRegex(GroupError, "ready after the deadline"):
            self.wait([0.0, 300.0])


class StateReportTest(unittest.TestCase):
    def test_a_container_reads_back_as_its_state_health_exit_code_oom_flag_and_restarts(self) -> None:
        found = parse_inspect(inspect_text({"health": "unhealthy", "exit_code": "137", "oom": "true", "restarts": "4"}))
        self.assertEqual({"batch_worker": ServiceState("batch_worker", "running", "unhealthy", 137, True, 4)}, found)

    def test_a_container_without_a_health_check_reads_back_with_health_none_and_is_ready_once_it_runs(self) -> None:
        state = parse_inspect(inspect_text({"health": "none"}))["batch_worker"]
        self.assertTrue(state.ready)

    def test_only_a_running_healthy_container_is_ready(self) -> None:
        for row in ({"health": "starting"}, {"health": "unhealthy"}, {"state": "exited"}, {"state": "restarting", "health": "none"}):
            with self.subTest(row=row):
                self.assertFalse(parse_inspect(inspect_text(row))["batch_worker"].ready)
        self.assertTrue(parse_inspect(inspect_text({}))["batch_worker"].ready)

    def test_a_value_outside_the_known_lists_is_reported_as_unknown_and_never_repeated(self) -> None:
        planted = "password=hunter2"
        found = parse_inspect(inspect_text({"state": planted, "health": planted, "exit_code": planted, "restarts": planted}))["batch_worker"]
        self.assertEqual(("unknown", "unknown", -1, 0), (found.state, found.health, found.exit_code, found.restarts))
        self.assertNotIn("hunter2", describe_states([found]))

    def test_a_line_that_does_not_have_six_fields_is_dropped(self) -> None:
        self.assertEqual({}, parse_inspect("batch_worker|running|healthy|0|false\nbatch_worker|a|b|c|d|e|f\n\n"))

    def test_the_description_names_each_service_with_the_five_fields_and_nothing_else(self) -> None:
        text = describe_states(
            parse_inspect(inspect_text({"health": "starting", "restarts": "2"}, {"service": "batch_helper", "state": "exited", "exit_code": "1"})).values()
        )
        self.assertEqual(
            "batch_helper: state exited, health healthy, exit code 1, out of memory false, restarts 0; "
            "batch_worker: state running, health starting, exit code 0, out of memory false, restarts 2",
            text,
        )

    def test_a_service_with_no_container_is_described_as_not_created(self) -> None:
        self.assertEqual("batch_worker: no container", describe_states([], missing=["batch_worker"]))


class CommandMessageTest(unittest.TestCase):
    """The command of a group that does not fit shows the admission check's four figures, not only the class of its refusal."""

    def test_a_group_that_does_not_fit_exits_75_and_the_message_names_the_group_figure_and_the_available_figure(self) -> None:
        cluster = Cluster()
        for found in cluster.objects.values():
            found.update(desired=0, ready=0)
        kubernetes, _ = control(cluster)
        limits = load_limits(LIMITS_FILE).services
        group_mib = sum(entry.memory_limit_mib for entry in limits.values() if entry.group == "ingestion")
        arguments = ["up", "ingestion", "--available-mib", "4100", "--compose-directory", str(ROOT / "compose"), "--project", "stack_project"]
        with (
            mock.patch("scripts.stack.groups.docker", return_value=""),
            mock.patch("scripts.stack.groups_k3s.control_from_environment", return_value=kubernetes),
            contextlib.redirect_stderr(io.StringIO()) as err,
        ):
            code = main(arguments)
        self.assertEqual(75, code)
        for figure in ("reserve 4096 MiB", "supporting services ", f"group {group_mib} MiB", "available 4100 MiB"):
            self.assertIn(figure, err.getvalue())
        self.assertEqual({0}, {found["desired"] for found in cluster.objects.values()})


if __name__ == "__main__":
    unittest.main()
