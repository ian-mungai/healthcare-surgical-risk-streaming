"""The host mode of the phase 8 live batch (``tests/e2e/stream_host.py``) never reaches the deployed stack by accident, and its rules read the deploy's files.

The runner is untracked, so the tests run only where it is on disk. Nothing starts: a stand-in answers every Docker command.

Failure modes covered here:

- A Compose command that names a platform service (the deployed broker) takes a verb the batch does not need (``rm``, ``kill``, ``restart``, ``down``), or names
  no service at all, and would stop or remove the deployed stack. A platform ``up`` that could recreate the broker is not marked ``--no-recreate``.
- The removal after a failed run lists containers by a label that the deployed project also carries.
- The project name of the batch is not its own (it starts with the deployed project's name), so a cleanup by label reaches the deployed containers.
- The environment file that the one-off services read holds a variable of the host's own environment (a credential) or leaves out a name that the overlay needs.
- The image tags, the host ports or the topic settings drift from the deploy's own files (``ansible/group_vars/all.yml``, the Compose service files and
  ``compose/kafka/topics.txt``), so the batch would test an image or a topic that is not the deployed one.
- The overlay publishes a port, names a container, mounts a volume of the platform, restarts a container by itself, depends on a platform service (a start would
  recreate it), uses a literal memory limit or points a service at a host name that is not the platform's.
- A topic that is not the batch's own (``feature_ready`` itself, ``warnings``, ``risk_scores``) is created or deleted by the batch.
- The ten-room measurement accepts a run that sampled nothing, or reads the lag, the memory or the processor use from the wrong sample.
"""

from __future__ import annotations

import contextlib
import importlib
import json
import os
import re
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock

from scripts.process import CompletedProcess
from scripts.stack.yaml_text import load_yaml
from tests.e2e.run_record import Record

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "tests" / "e2e" / "stream_host.py"
OVERLAY = ROOT / "tests" / "e2e" / "stream_stack" / "compose.host.yml"
COMPOSE = ROOT / "compose"
SERVICES = {"replay", "stream", "probe", "batch"}


def done(code: int = 0, text: str = "", error: str = "") -> CompletedProcess[str]:
    return CompletedProcess(["docker"], code, text, error)


@unittest.skipUnless(RUNNER.is_file(), "the temporary host runner is not in this checkout")
class HostRunnerCase(unittest.TestCase):
    def setUp(self) -> None:
        self.module: ModuleType = importlib.import_module("tests.e2e.stream_host")
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.scratch = Path(scratch.name)
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    def stack(self, answers: dict[str, CompletedProcess[str]] | None = None) -> Any:
        """A host stack whose every Docker command is answered by the first matching word of ``answers`` and recorded in ``self.calls``."""
        record = Record(feature="stand-in", slug="stand_in", rerun="none")
        work = self.scratch / "work"
        run = work / "run"
        run.mkdir(parents=True, exist_ok=True)

        def runner(program: str, args: list[str], **options: Any) -> CompletedProcess[str]:
            self.calls.append((program, tuple(args)))
            for word, answer in (answers or {}).items():
                if word in args:
                    return answer
            return done()

        with mock.patch.object(self.module, "run_command", runner):
            stack = self.module.HostStack(record, [], "replay:test", "spark:test", work / "landed", run, work)
        self.runner = runner
        return stack

    def run_stack(self, stack: Any, method: str, *args: Any, **options: Any) -> Any:
        with mock.patch.object(self.module, "run_command", self.runner):
            return getattr(stack, method)(*args, **options)


class PlatformTargetRules(HostRunnerCase):
    def test_a_platform_service_takes_only_the_verbs_the_batch_needs(self) -> None:
        for arguments in (
            ("exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--list"),
            ("stop", "kafka"),
            ("start", "kafka"),
            ("up", "--detach", "--wait", "--wait-timeout", "400", "kafka"),
        ):
            with self.subTest(arguments=arguments):
                self.assertEqual("platform", self.module.compose_target(arguments))

    def test_the_batchs_own_services_take_every_verb(self) -> None:
        for arguments in (
            ("run", "--rm", "-T", "stream"),
            ("run", "--detach", "-T", "stream"),
            ("up", "--detach", "replay"),
            ("kill", "replay"),
            ("rm", "--force", "replay"),
            ("run", "--rm", "-T", "--no-deps", "--entrypoint", "python", "replay", "/probe/plant_late.py"),
        ):
            with self.subTest(arguments=arguments):
                self.assertEqual("batch", self.module.compose_target(arguments))

    def test_a_verb_that_could_stop_or_remove_the_deployed_stack_is_refused(self) -> None:
        for arguments in (
            ("rm", "--force", "kafka"),
            ("kill", "kafka"),
            ("restart", "kafka"),
            ("down",),
            ("down", "--volumes"),
            ("down", "replay"),
            ("stop",),
            ("up", "--detach"),
            ("stop", "apicurio"),
            ("exec", "-T", "nessie", "ls"),
            ("run", "--rm", "kafka"),
            ("pause", "kafka"),
        ):
            with self.subTest(arguments=arguments), self.assertRaises(self.module.PlatformRefusal):
                self.module.compose_target(arguments)

    def test_a_platform_up_never_recreates_the_broker(self) -> None:
        arguments = self.module.platform_arguments(("up", "--detach", "--wait", "--wait-timeout", "400", "kafka"))
        self.assertEqual("up", arguments[0])
        self.assertIn("--no-recreate", arguments)
        self.assertEqual(("stop", "kafka"), self.module.platform_arguments(("stop", "kafka")))

    def test_the_platform_command_names_the_deployed_project_and_both_env_files(self) -> None:
        stack = self.stack()
        self.run_stack(stack, "compose", "exec", "-T", "kafka", "true")
        program, arguments = self.calls[-1]
        self.assertEqual("sudo", program)
        self.assertEqual(("-n", "docker", "compose"), arguments[:3])
        self.assertEqual("surgical_risk", arguments[arguments.index("--project-name") + 1])
        self.assertEqual("/opt/platform/compose", arguments[arguments.index("--project-directory") + 1])
        env_files = [arguments[index + 1] for index, word in enumerate(arguments) if word == "--env-file"]
        self.assertEqual(["/opt/platform/compose/limits.env", "/opt/platform/compose/host.env"], env_files)
        self.assertEqual("/opt/platform/compose/compose.yml", arguments[arguments.index("--file") + 1])

    def test_the_batch_command_names_its_own_project_and_the_overlay(self) -> None:
        stack = self.stack()
        self.run_stack(stack, "compose", "run", "--rm", "-T", "stream")
        _, arguments = self.calls[-1]
        project = arguments[arguments.index("-p") + 1]
        self.assertTrue(project.startswith("p8-host-"))
        self.assertNotEqual("surgical_risk", project)
        self.assertEqual(str(self.module.OVERLAY), arguments[arguments.index("-f") + 1])
        self.assertEqual("/opt/platform/compose/limits.env", arguments[arguments.index("--env-file") + 1])

    def test_every_docker_command_runs_under_sudo_without_a_prompt(self) -> None:
        stack = self.stack()
        self.run_stack(stack, "docker", "ps")
        self.assertEqual(("sudo", ("-n", "docker", "ps")), self.calls[-1])

    def test_a_project_that_is_not_the_batchs_own_is_refused(self) -> None:
        for name in ("surgical_risk", "surgical_risk-x", "p8-stream-abcd"):
            with self.subTest(name=name), self.assertRaises(self.module.PlatformRefusal):
                self.module.check_project(name)
        self.module.check_project("p8-host-abcd1234")

    def test_the_stack_project_passes_the_check(self) -> None:
        stack = self.stack()
        self.module.check_project(stack.project)


class Cleanup(HostRunnerCase):
    def test_the_listing_filters_on_the_batchs_own_project_label(self) -> None:
        stack = self.stack({"ps": done(0, "aaa\nbbb\n")})
        ids, listed = self.run_stack(stack, "label_ids")
        self.assertEqual((["aaa", "bbb"], True), (ids, listed))
        listing = next(arguments for _, arguments in self.calls if "ps" in arguments)
        self.assertEqual(f"label=com.docker.compose.project={stack.project}", listing[listing.index("--filter") + 1])
        self.assertNotIn("surgical_risk", " ".join(listing))

    def test_a_removal_touches_only_the_listed_containers_of_the_batch(self) -> None:
        stack = self.stack({"ps": done(0, "aaa\n")})
        self.run_stack(stack, "remove_project_containers")
        removed = [arguments for _, arguments in self.calls if "rm" in arguments]
        self.assertTrue(removed)
        for arguments in removed:
            self.assertEqual("aaa", arguments[-1])

    def test_a_stop_keeps_only_the_lake_volume_and_never_stops_the_platform(self) -> None:
        stack = self.stack({"ps": done(0, "aaa\n"), "volume": done(0, "")})
        text, good = self.run_stack(stack, "stop_and_keep")
        stops = [arguments for _, arguments in self.calls if "stop" in arguments]
        self.assertTrue(stops)
        for arguments in stops:
            self.assertEqual("aaa", arguments[-1])
        self.assertFalse(good, text)

    def test_a_stop_is_good_when_nothing_runs_and_the_lake_volume_is_kept(self) -> None:
        stack = self.stack()
        stack.docker = lambda *arguments, **options: done(0, f"{stack.project}_lake_warehouse\n") if arguments[:2] == ("volume", "ls") else done(0, "")
        text, good = stack.stop_and_keep()
        self.assertTrue(good, text)
        self.assertIn("--teardown", text)
        self.assertIn(stack.project, text)

    def test_the_teardown_refuses_a_project_that_is_not_the_batchs_own(self) -> None:
        with mock.patch.object(self.module, "run_command") as runner:
            self.assertEqual(2, self.module.teardown_host("surgical_risk"))
            runner.assert_not_called()


class Topics(HostRunnerCase):
    def test_the_input_topic_is_made_with_the_settings_of_the_deployed_list(self) -> None:
        rows = self.module.topic_rows()
        arguments = self.module.create_arguments("replay_events", rows)
        text = " ".join(arguments)
        self.assertIn("--partitions 10", text)
        self.assertIn("retention.ms=129600000", text)
        self.assertIn("retention.bytes=2147483648", text)
        self.assertIn("segment.bytes=134217728", text)
        self.assertIn("flush.messages=1", text)
        self.assertIn("cleanup.policy=delete", text)
        self.assertIn("--topic replay_events", text)

    def test_a_private_output_topic_takes_the_settings_of_feature_ready(self) -> None:
        rows = self.module.topic_rows()
        private = " ".join(self.module.create_arguments("feature_ready_control", rows))
        deployed = " ".join(self.module.create_arguments("feature_ready", rows))
        self.assertEqual(deployed.replace("--topic feature_ready", "--topic feature_ready_control"), private)

    def test_a_topic_of_the_platform_is_not_the_batchs_to_create_or_delete(self) -> None:
        rows = self.module.topic_rows()
        for name in ("feature_ready", "warnings", "risk_scores", "other", "feature_ready_"):
            with self.subTest(name=name), self.assertRaises(self.module.PlatformRefusal):
                self.module.check_private_topic(name)
        self.assertEqual("feature_ready_control", self.module.check_private_topic("feature_ready_control"))
        with self.assertRaises(self.module.PlatformRefusal):
            self.module.create_arguments("warnings_x", rows)

    def test_a_stack_refuses_to_create_a_topic_of_the_platform_and_makes_the_input_topic(self) -> None:
        stack = self.stack()
        for name in ("warnings", "risk_scores", "feature_ready"):
            with self.subTest(name=name), self.assertRaises(self.module.PlatformRefusal):
                self.run_stack(stack, "create_topic", name)
        self.assertFalse([arguments for _, arguments in self.calls if "--create" in arguments])
        self.run_stack(stack, "create_topic", "replay_events")
        self.assertTrue([arguments for _, arguments in self.calls if "--create" in arguments])
        self.assertEqual([], stack.private_topics)

    def test_a_stack_deletes_only_its_private_topics_and_resets_the_input_topic(self) -> None:
        stack = self.stack({"--list": done(0, "")})
        with mock.patch.object(self.module.time, "sleep", lambda seconds: None):
            self.run_stack(stack, "create_topic", "feature_ready_control")
            failed = self.run_stack(stack, "drop_private_topics")
        self.assertEqual([], failed)
        deleted = [arguments[arguments.index("--topic") + 1] for _, arguments in self.calls if "--delete" in arguments]
        self.assertEqual({"feature_ready_control", "replay_events"}, set(deleted))


class Environment(HostRunnerCase):
    def test_the_env_file_holds_the_overlays_names_and_nothing_of_the_hosts_environment(self) -> None:
        stack = self.stack()
        with mock.patch.dict(os.environ, {"AWS_SECRET_ACCESS_KEY": "hunter2", "SOME_HOST_SETTING": "kept-out", "GITHUB_TOKEN": "t0ken"}):
            self.run_stack(stack, "compose", "run", "--rm", "-T", "stream")
        text = stack.env_file.read_text()
        self.assertNotIn("hunter2", text)
        self.assertNotIn("t0ken", text)
        self.assertNotIn("SOME_HOST_SETTING", text)
        self.assertNotIn("kept-out", text)
        values = self.module.parse_environment_file(text)
        for name in ("REPLAY_IMAGE", "SPARK_IMAGE", "RUN_DIR", "LANDED_DIR", "PROBE_DIR", "PLATFORM_NETWORK", "STREAM_CASE", "STREAM_BRANCH"):
            self.assertIn(name, values)
        self.assertEqual("surgical_risk_default", values["PLATFORM_NETWORK"])

    def test_a_setting_of_the_job_reaches_the_file_on_the_next_command(self) -> None:
        stack = self.stack()
        self.run_stack(stack, "compose", "run", "--rm", "-T", "stream")
        stack.env["STREAM_MAX_SECONDS"] = "77"
        self.run_stack(stack, "compose", "run", "--rm", "-T", "stream")
        self.assertEqual("77", self.module.parse_environment_file(stack.env_file.read_text())["STREAM_MAX_SECONDS"])

    def test_parse_environment_file_reads_pairs_and_refuses_a_line_without_one(self) -> None:
        self.assertEqual({"A": "1", "B": "two words"}, self.module.parse_environment_file("# a comment\n\nA=1\nB=two words\n"))
        with self.assertRaises(ValueError):
            self.module.parse_environment_file("A=1\nnot a pair\n")

    def test_the_host_ports_come_from_the_host_file_and_default_to_the_compose_defaults(self) -> None:
        self.assertEqual((8123, 19999), self.module.host_ports({"APICURIO_HOST_PORT": "8123", "NESSIE_HOST_PORT": "19999"}))
        apicurio = re.search(r"APICURIO_HOST_PORT:-(\d+)", (COMPOSE / "services" / "apicurio.yml").read_text())
        nessie = re.search(r"NESSIE_HOST_PORT:-(\d+)", (COMPOSE / "services" / "nessie.yml").read_text())
        if apicurio is None or nessie is None:
            self.fail("a compose service file has no default host port")
        self.assertEqual((int(apicurio[1]), int(nessie[1])), self.module.host_ports({}))

    def test_the_image_tags_are_the_ones_the_deploy_builds_and_the_compose_files_name(self) -> None:
        replay, spark = self.module.deployed_images()
        document: Any = load_yaml((ROOT / "ansible" / "group_vars" / "all.yml").read_text())
        built = {item["name"]: item["tag"] for item in document["compose_stack_built_images"]}
        self.assertEqual((built["replay"], built["spark"]), (replay, spark))
        self.assertIn(f"image: {replay}", (COMPOSE / "services" / "replay.yml").read_text())
        self.assertIn(f"image: {spark}", (COMPOSE / "services" / "spark.yml").read_text())

    def test_the_deployed_network_is_the_compose_project_name_and_default(self) -> None:
        name = re.search(r"^name: (\S+)", (COMPOSE / "compose.yml").read_text(), re.MULTILINE)
        if name is None:
            self.fail("compose.yml names no project")
        self.assertEqual(f"{name[1]}_default", self.module.PLATFORM_NETWORK)
        self.assertEqual(name[1], self.module.DEPLOYED_PROJECT)


@unittest.skipUnless(OVERLAY.is_file(), "the temporary overlay is not in this checkout")
class Overlay(unittest.TestCase):
    def setUp(self) -> None:
        self.document: Any = load_yaml(OVERLAY.read_text())

    def test_it_names_the_four_one_off_services_and_no_other(self) -> None:
        self.assertEqual(SERVICES, set(self.document["services"]))

    def test_every_service_joins_the_external_platform_network_and_only_that(self) -> None:
        network = self.document["networks"]["platform"]
        self.assertTrue(network["external"])
        self.assertIn("PLATFORM_NETWORK", network["name"])
        for name, service in self.document["services"].items():
            with self.subTest(service=name):
                self.assertEqual(["platform"], list(service["networks"]))

    def test_nothing_is_published_named_or_restarted_and_no_platform_service_is_a_dependency(self) -> None:
        for name, service in self.document["services"].items():
            with self.subTest(service=name):
                for forbidden in ("ports", "container_name", "depends_on", "links", "extends", "network_mode"):
                    self.assertNotIn(forbidden, service)
                self.assertEqual("no", service["restart"])

    def test_memory_limits_come_from_the_deployed_limits_file(self) -> None:
        for name, service in self.document["services"].items():
            with self.subTest(service=name):
                self.assertRegex(str(service["mem_limit"]), r"^\$\{MEM_LIMIT_(REPLAY|SPARK):\?")

    def test_the_services_reach_the_platform_by_its_own_host_names(self) -> None:
        text = OVERLAY.read_text()
        self.assertIn("KAFKA_BOOTSTRAP: kafka:9092", text)
        self.assertIn("http://apicurio:8080/apis/registry/v3", text)
        self.assertIn("http://nessie:19120/api/v2", text)
        self.assertNotIn("http://registry:", text)

    def test_only_the_batchs_own_named_volumes_are_declared_and_none_is_external(self) -> None:
        volumes = self.document["volumes"]
        self.assertTrue(volumes)
        for name, body in volumes.items():
            with self.subTest(volume=name):
                self.assertFalse(body and body.get("external"))
                self.assertFalse(body and body.get("name"))
        for service in self.document["services"].values():
            for mount in service["volumes"]:
                source = mount.split(":", 1)[0]
                self.assertTrue(source in volumes or source.startswith("${"), mount)

    def test_the_images_and_folders_are_variables_not_literals(self) -> None:
        services = self.document["services"]
        self.assertEqual("${REPLAY_IMAGE:?name the replay image}", services["replay"]["image"])
        for name in ("stream", "probe", "batch"):
            self.assertEqual("${SPARK_IMAGE:?name the Spark image}", services[name]["image"])


class Measurement(HostRunnerCase):
    def test_a_stats_line_gives_the_name_the_processor_share_and_the_memory_in_mib(self) -> None:
        line = json.dumps({"Name": "p8-host-x-stream-1", "CPUPerc": "187.5%", "MemUsage": "1.5GiB / 4GiB"})
        self.assertEqual(("p8-host-x-stream-1", 187.5, 1536.0), self.module.parse_stats_line(line))
        small = json.dumps({"Name": "k", "CPUPerc": "0.00%", "MemUsage": "512KiB / 1GiB"})
        self.assertEqual(("k", 0.0, 0.5), self.module.parse_stats_line(small))
        self.assertIsNone(self.module.parse_stats_line("not json"))
        self.assertIsNone(self.module.parse_stats_line(json.dumps({"Name": "k", "CPUPerc": "--", "MemUsage": "--"})))

    def test_the_summary_takes_the_peaks_and_the_time_the_lag_took_to_drain(self) -> None:
        samples = [
            self.module.Sample(0.0, {0: 100, 1: 50}, {"stream": (10.0, 500.0), "kafka": (5.0, 900.0)}),
            self.module.Sample(5.0, {0: 400, 1: 10}, {"stream": (190.0, 2500.0), "kafka": (20.0, 1000.0)}),
            self.module.Sample(10.0, {0: 20, 1: 0}, {"stream": (120.0, 3100.0), "kafka": (8.0, 1100.0)}),
            self.module.Sample(15.0, {0: 0, 1: 0}, {"stream": (30.0, 3000.0)}),
        ]
        summary = self.module.summarize(samples)
        self.assertEqual(410, summary["peak_lag_events"])
        self.assertEqual(5.0, summary["peak_lag_at_seconds"])
        self.assertEqual(15.0, summary["drained_at_seconds"])
        self.assertEqual(10.0, summary["drain_seconds_after_peak"])
        self.assertEqual(3100.0, summary["peak_memory_mib"]["stream"])
        self.assertEqual(1100.0, summary["peak_memory_mib"]["kafka"])
        self.assertEqual(190.0, summary["peak_cpu_percent"]["stream"])
        self.assertEqual(4, summary["samples"])

    def test_a_lag_that_was_zero_before_its_peak_has_not_drained_after_it(self) -> None:
        samples = [self.module.Sample(0.0, {0: 0}, {}), self.module.Sample(5.0, {0: 100}, {}), self.module.Sample(10.0, {0: 90}, {})]
        summary = self.module.summarize(samples)
        self.assertEqual(5.0, summary["peak_lag_at_seconds"])
        self.assertIsNone(summary["drained_at_seconds"])

    def test_a_lag_that_never_drains_is_reported_as_not_drained(self) -> None:
        samples = [self.module.Sample(0.0, {0: 100}, {}), self.module.Sample(5.0, {0: 90}, {})]
        summary = self.module.summarize(samples)
        self.assertIsNone(summary["drained_at_seconds"])
        self.assertIsNone(summary["drain_seconds_after_peak"])

    def test_no_sample_is_refused_and_a_lag_free_run_has_no_peak_to_drain(self) -> None:
        with self.assertRaises(ValueError):
            self.module.summarize([])
        summary = self.module.summarize([self.module.Sample(0.0, {0: 0}, {"a": (1.0, 2.0)})])
        self.assertEqual(0, summary["peak_lag_events"])
        self.assertEqual(0.0, summary["drain_seconds_after_peak"])

    def test_the_lag_of_a_partition_is_the_end_offset_minus_the_committed_one_and_never_negative(self) -> None:
        self.assertEqual({0: 10, 1: 0, 2: 7}, self.module.lag_by_partition({0: 110, 1: 5, 2: 7}, {0: 100, 1: 5}))
        self.assertEqual({0: 0}, self.module.lag_by_partition({0: 3}, {0: 9}))

    def test_the_kafka_size_adds_the_partitions_of_each_topic(self) -> None:
        data = "/var/lib/kafka/data"
        text = f"100\t{data}/replay_events-0\n250\t{data}/replay_events-1\n7\t{data}/feature_ready_x-0\n9\t{data}/__cluster_metadata-0\n"
        self.assertEqual({"replay_events": 350, "feature_ready_x": 7, "__cluster_metadata": 9}, self.module.parse_topic_sizes(text))
        self.assertEqual({}, self.module.parse_topic_sizes(""))

    def test_the_bytes_for_each_replayed_hour_come_from_the_window(self) -> None:
        self.assertEqual(7200.0, self.module.bytes_per_replayed_hour(3600 * 2, 3600))
        self.assertEqual(1800.0, self.module.bytes_per_replayed_hour(900, 1800))
        with self.assertRaises(ValueError):
            self.module.bytes_per_replayed_hour(1, 0)

    def test_the_ten_room_document_has_ten_rooms_the_control_cases_and_the_speed(self) -> None:
        document = self.module.ten_room_document("run_x")
        self.assertEqual(10, document["rooms"])
        self.assertEqual(list(self.module.ROOMS_CASES), document["cases"])
        self.assertEqual(self.module.SPEED, document["speed"])
        self.assertEqual(f"/opt/replay/state/{document['run_id']}", document["state_dir"])
        self.assertEqual([], self.module.rooms_plan_problems())

    def test_a_room_plan_with_a_case_of_the_test_set_is_refused(self) -> None:
        with mock.patch.object(self.module, "ROOMS_CASES", (908, 565)):
            self.assertTrue(self.module.rooms_plan_problems())


class Arguments(HostRunnerCase):
    def test_the_landed_cache_has_no_default(self) -> None:
        with contextlib.redirect_stderr(__import__("io").StringIO()), self.assertRaises(SystemExit) as caught:
            self.module.parse_arguments([])
        self.assertEqual(2, caught.exception.code)

    def test_the_parts_the_runner_can_run_alone_are_named(self) -> None:
        arguments = self.module.parse_arguments(["--landed-cache", "/data/landed", "--only", "rooms10"])
        self.assertEqual(Path("/data/landed"), arguments.landed_cache)
        self.assertEqual("rooms10", arguments.only)
        with contextlib.redirect_stderr(__import__("io").StringIO()), self.assertRaises(SystemExit):
            self.module.parse_arguments(["--landed-cache", "/data/landed", "--only", "everything"])

    def test_the_limits_name_what_the_host_run_does_not_prove(self) -> None:
        text = "\n".join(self.module.HOST_LIMITS)
        for phrase in (
            "S3 is not exercised",
            "the always-on replay service",
            "the phase 7 batch tables are not built on the host yet",
            "lifecycle-format upgrade",
            "compat_probe",
            "resets the deployed topic replay_events",
            "--teardown",
            "flush.messages=1",
            "no full-release copy, no full-history load",
        ):
            with self.subTest(phrase=phrase):
                self.assertIn(phrase, text)


if __name__ == "__main__":
    unittest.main()
