"""The probe's Docker readings parse the formats they ask for and fail by step and exit status, and never with a container's text."""

from __future__ import annotations

import json
import unittest
from collections.abc import Sequence

from scripts.process import CompletedProcess, TimeoutExpired
from scripts.stack.probe_docker import (
    ContainerEvent,
    Docker,
    ProbeError,
    parse_events,
    parse_listing,
    parse_state,
    timestamp,
)


def event(action: str, *, service: str = "nessie", nano: int = 1_763_800_000_000_000_001, **attributes: str) -> str:
    actor = {"name": f"project-{service}-1", "com.docker.compose.service": service, **attributes}
    return json.dumps({"Type": "container", "Action": action, "Actor": {"ID": "abc", "Attributes": actor}, "time": nano // 10**9, "timeNano": nano})


class Fake:
    """A runner that answers each command from a script and records the arguments."""

    def __init__(self, *answers: tuple[int, str] | BaseException) -> None:
        self.answers = list(answers)
        self.calls: list[list[str]] = []
        self.timeouts: list[float] = []

    def __call__(self, program: str, args: Sequence[str], *, timeout: float, **_: object) -> CompletedProcess[str]:
        self.calls.append([program, *args])
        self.timeouts.append(timeout)
        answer = self.answers.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return CompletedProcess([program, *args], answer[0], answer[1], "SECRET-FROM-THE-CONTAINER")


class ParseTest(unittest.TestCase):
    def test_the_listing_gives_the_id_the_name_and_the_service_of_each_container(self) -> None:
        found = parse_listing("a1b2c3d4e5f6|p-spark-run-1|spark\nb2c3d4e5f6a1|p-nessie-1|nessie\n")
        self.assertEqual(["spark", "nessie"], [item.service for item in found])
        self.assertEqual("p-spark-run-1", found[0].name)

    def test_a_listing_line_in_another_form_is_refused(self) -> None:
        with self.assertRaisesRegex(ProbeError, "docker ps line 2"):
            parse_listing("a1b2c3d4e5f6|p-spark-1|spark\nbroken\n")

    def test_the_state_of_a_container_is_read_with_its_exit_status_kill_flag_restarts_and_memory_cap(self) -> None:
        state = parse_state("exited|137|true|0|2|4294967296\n")
        self.assertEqual(
            ("exited", 137, True, 0, 2, 4294967296), (state.status, state.exit_code, state.oom_killed, state.pid, state.restarts, state.memory_cap_bytes)
        )

    def test_a_state_in_another_form_is_refused_and_never_echoed(self) -> None:
        with self.assertRaisesRegex(ProbeError, "did not answer in the expected form") as caught:
            parse_state("running|0|maybe|1|0|0 SECRET")
        self.assertNotIn("SECRET", str(caught.exception))

    def test_the_events_give_the_action_the_service_and_the_detail(self) -> None:
        text = event("die", exitCode="137") + "\n" + event("kill", signal="9", service="postgres") + "\n" + event("oom") + "\n"
        found = parse_events(text)
        self.assertEqual(["die", "kill", "oom"], [item.action for item in found])
        self.assertEqual(["nessie", "postgres", "nessie"], [item.service for item in found])
        self.assertEqual("die of nessie (exitCode 137)", found[0].describe())
        self.assertEqual("oom of nessie", found[2].describe())

    def test_an_event_line_that_is_not_in_the_form_is_refused_by_its_number(self) -> None:
        for line in ("not json", "{}", json.dumps({"Action": "die", "Actor": {"Attributes": {}}, "timeNano": "late"})):
            with self.subTest(line=line), self.assertRaisesRegex(ProbeError, "docker events line 2"):
                parse_events(event("die") + "\n" + line + "\n")

    def test_no_events_is_an_empty_list(self) -> None:
        self.assertEqual([], parse_events(""))

    def test_a_time_is_written_without_losing_a_digit(self) -> None:
        self.assertEqual("1763800000.000000001", timestamp(1_763_800_000_000_000_001))


class DockerTest(unittest.TestCase):
    def test_a_command_that_exits_with_an_error_names_the_step_and_the_status_and_never_the_output(self) -> None:
        docker = Docker(runner=Fake((125, "")))
        with self.assertRaisesRegex(ProbeError, r"reading the job's output: docker exited 125") as caught:
            docker.logs("job")
        self.assertNotIn("SECRET", str(caught.exception))

    def test_a_command_that_passes_its_deadline_is_a_failure_that_names_the_step(self) -> None:
        docker = Docker(runner=Fake(TimeoutExpired("docker", 5)))
        with self.assertRaisesRegex(ProbeError, "reading a container's state: docker timed out after 30 seconds"):
            docker.state("job")

    def test_a_missing_docker_program_is_a_failure(self) -> None:
        with self.assertRaisesRegex(ProbeError, "docker is not installed"):
            Docker(runner=Fake(FileNotFoundError("docker"))).logs("job")

    def test_every_deadline_is_cut_to_the_time_the_run_has_left(self) -> None:
        fake = Fake((0, "a1b2c3d4e5f6|n|spark\n"))
        Docker(runner=fake, limit=lambda seconds: min(seconds, 4.0)).list_containers("project", service="spark", include_stopped=True)
        self.assertEqual([4.0], fake.timeouts)

    def test_a_run_with_no_time_left_does_not_start_a_command(self) -> None:
        def exhausted(seconds: float) -> float:
            raise ProbeError("the run's deadline has passed")

        fake = Fake()
        with self.assertRaisesRegex(ProbeError, "deadline has passed"):
            Docker(runner=fake, limit=exhausted).logs("job")
        self.assertEqual([], fake.calls)

    def test_the_listing_of_a_project_asks_for_its_stopped_containers_only_when_told_to(self) -> None:
        fake = Fake((0, ""), (0, ""))
        docker = Docker(runner=fake)
        docker.list_containers("p", service="spark")
        docker.list_containers("p", service="spark", include_stopped=True)
        self.assertNotIn("--all", fake.calls[0])
        self.assertIn("--all", fake.calls[1])
        self.assertIn("label=com.docker.compose.service=spark", fake.calls[1])

    def test_a_container_exists_when_the_listing_by_its_exact_name_is_not_empty(self) -> None:
        fake = Fake((0, "a1b2c3d4e5f6\n"), (0, "\n"))
        docker = Docker(runner=fake)
        self.assertTrue(docker.exists("job"))
        self.assertFalse(docker.exists("job"))
        self.assertIn("name=^/job$", fake.calls[0])

    def test_a_container_whose_existence_cannot_be_read_is_never_taken_as_absent(self) -> None:
        with self.assertRaisesRegex(ProbeError, "looking for a container: docker exited 1"):
            Docker(runner=Fake((1, ""))).exists("job")

    def test_a_name_with_a_character_that_a_filter_would_read_as_a_pattern_is_refused(self) -> None:
        with self.assertRaisesRegex(ProbeError, "container name"):
            Docker(runner=Fake()).exists("job$|.*")

    def test_a_removal_of_a_container_that_is_already_gone_is_not_an_error_and_another_failure_is(self) -> None:
        Docker(runner=Fake((1, ""))).remove("job")
        with self.assertRaisesRegex(ProbeError, "docker exited 125"):
            Docker(runner=Fake((125, ""))).remove("job")

    def test_the_events_command_names_the_project_the_window_the_actions_and_its_own_deadline(self) -> None:
        fake = Fake((0, event("die") + "\n"))
        found = Docker(runner=fake).events("p", 10, 5_000_000_010, seconds=12.0)
        self.assertEqual(["die"], [item.action for item in found])
        call = fake.calls[0]
        self.assertEqual("0.000000010", call[call.index("--since") + 1])
        self.assertEqual("5.000000010", call[call.index("--until") + 1])
        for action in ("die", "oom", "kill", "restart"):
            self.assertIn(f"event={action}", call)
        self.assertIn("label=com.docker.compose.project=p", call)
        self.assertEqual([12.0], fake.timeouts)

    def test_the_memory_statistics_are_the_listing_and_the_statistics_text(self) -> None:
        listing, stats = Docker(runner=Fake((0, "a1b2c3d4e5f6|spark\n"), (0, "a1b2c3d4e5f6|1MiB / 2MiB\n"))).usage_texts("p")
        self.assertEqual("a1b2c3d4e5f6|spark\n", listing)
        self.assertEqual("a1b2c3d4e5f6|1MiB / 2MiB\n", stats)

    def test_event_is_comparable_by_its_fields(self) -> None:
        self.assertEqual(ContainerEvent(1, "die", "a", "b", ""), ContainerEvent(1, "die", "a", "b", ""))


if __name__ == "__main__":
    unittest.main()
