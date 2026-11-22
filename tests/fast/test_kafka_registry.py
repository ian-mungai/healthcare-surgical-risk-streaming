"""The Kafka service, the registry service and the data that configures them: the topic list, its storage budget, the registry rules and the listeners.

Real brokers and registries are the acceptance test's job. The cases here are the rules that a real run cannot show without a planted fault: a listener on
another address, a topic list that no longer fits its budget, a topic that drifted, a rule that is not backward.
"""

from __future__ import annotations

import copy
import importlib.util
import re
import sys
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any

from scripts.stack.limits import load_limits
from scripts.stack.yaml_text import load_yaml
from tests.fast.test_postgres_accounts import TOOLS_FILE, accounts

ROOT = Path(__file__).resolve().parents[2]
LIBRARY = ROOT / "ansible" / "roles" / "compose_stack" / "library"
COMPOSE = ROOT / "compose"
MIB = 1024 * 1024
GIB = 1024 * MIB
ALLOWED_LISTENERS = {"INTERNAL://:9092", "HOST://:9094", "CONTROLLER://127.0.0.1:9093"}
ALLOWED_ADVERTISED = {"INTERNAL://kafka:9092", "HOST://127.0.0.1:${KAFKA_HOST_PORT:-9094}"}
KAFKA_PORT = "127.0.0.1:${KAFKA_HOST_PORT:-9094}:9094"
DESCRIBE = (
    "Topic: replay_events\tTopicId: 3A6tywHzSt2_Bdwmlp733A\tPartitionCount: 10\tReplicationFactor: 1\t"
    "Configs: min.insync.replicas=1,cleanup.policy=delete,flush.messages=1,segment.bytes=134217728,retention.ms=129600000,retention.bytes=2147483648\n"
    "\tTopic: replay_events\tPartition: 0\tLeader: 1\tReplicas: 1\tIsr: 1\tElr: \tLastKnownElr: \n"
    "\tTopic: replay_events\tPartition: 1\tLeader: 1\tReplicas: 1\tIsr: 1\tElr: \tLastKnownElr: \n"
    "Topic: __consumer_offsets\tTopicId: Zq5Y0y6mRn-9sQy4Zo0pFA\tPartitionCount: 10\tReplicationFactor: 1\t"
    "Configs: cleanup.policy=compact,segment.bytes=104857600\n"
    "\tTopic: __consumer_offsets\tPartition: 0\tLeader: 1\tReplicas: 1\tIsr: 1\tElr: \tLastKnownElr: \n"
)


def load_library(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, LIBRARY / f"{name}.py")
    if spec is None or spec.loader is None:
        raise ImportError(name)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


topics_module: Any = load_library("kafka_topics")
rules_module: Any = load_library("registry_rules")


def service_file(name: str) -> dict[str, Any]:
    document: Any = load_yaml((COMPOSE / "services" / f"{name}.yml").read_text())
    return copy.deepcopy(document)


def listener_problems(document: dict[str, Any]) -> list[str]:
    """Each way that the Kafka service file could open a listener beyond the Compose network and the host's loopback address."""
    service = document["services"]["kafka"]
    environment = service.get("environment") or {}
    problems: list[str] = []
    listeners = set(str(environment.get("KAFKA_LISTENERS", "")).split(","))
    if listeners != ALLOWED_LISTENERS:
        problems.append(f"kafka: KAFKA_LISTENERS must be exactly {sorted(ALLOWED_LISTENERS)}, found {sorted(listeners)}")
    advertised = set(str(environment.get("KAFKA_ADVERTISED_LISTENERS", "")).split(","))
    if advertised != ALLOWED_ADVERTISED:
        problems.append(f"kafka: KAFKA_ADVERTISED_LISTENERS must be exactly {sorted(ALLOWED_ADVERTISED)}, found {sorted(advertised)}")
    protocols = dict(item.split(":") for item in str(environment.get("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "")).split(",") if item)
    if set(protocols) != {"INTERNAL", "HOST", "CONTROLLER"}:
        problems.append(f"kafka: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP must name INTERNAL, HOST and CONTROLLER only, found {sorted(protocols)}")
    if service.get("ports") != [KAFKA_PORT]:
        problems.append(f"kafka: the only published port must be {KAFKA_PORT}")
    if service.get("network_mode"):
        problems.append("kafka: network_mode would put the listeners on the host's addresses")
    return problems


class TopicListTest(unittest.TestCase):
    topics = topics_module.parse_topics((COMPOSE / "kafka" / "topics.txt").read_text())

    def test_the_input_topic_has_one_partition_for_each_room_of_the_default_run(self) -> None:
        by_name = {topic.name: topic for topic in self.topics}
        self.assertEqual(10, by_name["replay_events"].partitions)

    def test_every_topic_has_both_retention_limits_and_the_delete_policy(self) -> None:
        for topic in self.topics:
            with self.subTest(topic=topic.name):
                self.assertGreater(topic.retention_hours, 0)
                self.assertGreater(topic.retention_mib, topic.segment_mib)
                self.assertEqual({"cleanup.policy", "retention.ms", "retention.bytes", "segment.bytes", "flush.messages"}, set(topic.configs()))
                self.assertEqual("delete", topic.configs()["cleanup.policy"])

    def test_every_event_topic_is_written_to_disk_before_the_broker_acknowledges_a_record(self) -> None:
        self.assertEqual({"replay_events", "feature_ready", "warnings", "risk_scores"}, {topic.name for topic in self.topics})
        for topic in self.topics:
            with self.subTest(topic=topic.name):
                self.assertEqual("1", topic.configs()["flush.messages"])

    def test_the_worst_cases_of_the_topics_add_up_to_no_more_than_the_storage_budget(self) -> None:
        budget = topics_module.parse_budget((COMPOSE / "kafka" / "storage_budget_gib.txt").read_text())
        total = sum(topic.worst_case_bytes() for topic in self.topics)
        self.assertLessEqual(total, budget * GIB)
        self.assertGreater(total, 0)

    def test_the_worst_case_of_a_topic_is_its_partitions_times_its_size_limit_plus_one_segment(self) -> None:
        by_name = {topic.name: topic for topic in self.topics}
        self.assertEqual(10 * (2048 + 128) * MIB, by_name["replay_events"].worst_case_bytes())
        self.assertEqual(10 * (128 + 32) * MIB, by_name["warnings"].worst_case_bytes())

    def test_the_budget_check_names_a_list_that_no_longer_fits(self) -> None:
        planted = topics_module.parse_topics("big 10 36 4096 128 delete\n")
        self.assertGreater(sum(topic.worst_case_bytes() for topic in planted), 30 * GIB)

    def test_a_malformed_line_is_refused_by_name(self) -> None:
        for line in (
            "Bad 1 1 8 1 delete",
            "t 0 1 8 1 delete",
            "t 1 0 8 1 delete",
            "t 1 1 1 1 delete",
            "t 1 1 8 1 compact",
            "t 1 1 8 1",
            "t 1 1 8 1 delete extra",
            "t 1 1 x 1 delete",
        ):
            with self.subTest(line=line), self.assertRaises(topics_module.TopicError):
                topics_module.parse_topics(line + "\n")

    def test_a_repeated_name_or_an_empty_list_is_refused(self) -> None:
        with self.assertRaisesRegex(topics_module.TopicError, "more than once"):
            topics_module.parse_topics("t 1 1 8 1 delete\nt 2 1 8 1 delete\n")
        with self.assertRaisesRegex(topics_module.TopicError, "no topic"):
            topics_module.parse_topics("# nothing\n")

    def test_the_budget_must_be_one_positive_whole_number(self) -> None:
        for text in ("", "# only a comment\n", "0\n", "-3\n", "1.5\n", "10 20\n"):
            with self.subTest(text=text), self.assertRaises(topics_module.TopicError):
                topics_module.parse_budget(text)


class TopicPlanTest(unittest.TestCase):
    wanted = topics_module.parse_topics("replay_events 10 36 2048 128 delete\n")

    def state(self, text: str = DESCRIBE) -> Any:
        return topics_module.parse_state(text)

    def test_the_state_reads_each_topic_with_its_partitions_and_overrides_and_skips_the_partition_lines(self) -> None:
        state = self.state()
        self.assertEqual({"replay_events", "__consumer_offsets"}, set(state))
        self.assertEqual(10, state["replay_events"].partitions)
        self.assertEqual("129600000", state["replay_events"].configs["retention.ms"])

    def test_a_topic_that_matches_needs_no_action(self) -> None:
        self.assertEqual([], topics_module.plan(self.wanted, self.state()))

    def test_a_missing_topic_is_created_with_every_setting(self) -> None:
        actions = topics_module.plan(self.wanted, {})
        self.assertEqual(1, len(actions))
        arguments = " ".join(actions[0].arguments)
        self.assertEqual("kafka-topics", actions[0].tool)
        for part in (
            "--create",
            "--topic replay_events",
            "--partitions 10",
            "--replication-factor 1",
            "retention.ms=129600000",
            "retention.bytes=2147483648",
            "segment.bytes=134217728",
            "cleanup.policy=delete",
            "flush.messages=1",
        ):
            self.assertIn(part, arguments)

    def test_a_setting_that_drifted_is_corrected_and_only_that_one(self) -> None:
        for key, value in (
            ("retention.bytes", "9999"),
            ("retention.ms", "1"),
            ("segment.bytes", "1048576"),
            ("cleanup.policy", "compact"),
            ("flush.messages", "9223372036854775807"),
        ):
            with self.subTest(key=key):
                drifted = re.sub(rf"{re.escape(key)}=[^,\n]*", f"{key}={value}", DESCRIBE, count=1)
                actions = topics_module.plan(self.wanted, self.state(drifted))
                self.assertEqual(1, len(actions), actions)
                self.assertEqual("kafka-configs", actions[0].tool)
                self.assertEqual(f"{key}={self.wanted[0].configs()[key]}", actions[0].arguments[-1])

    def test_a_topic_without_the_flush_setting_gets_it_set_and_nothing_else(self) -> None:
        unflushed = DESCRIBE.replace("flush.messages=1,", "")
        actions = topics_module.plan(self.wanted, self.state(unflushed))
        self.assertEqual(1, len(actions))
        self.assertEqual(("kafka-configs", "flush.messages=1"), (actions[0].tool, actions[0].arguments[-1]))

    def test_a_setting_that_is_missing_from_the_overrides_is_set(self) -> None:
        missing = DESCRIBE.replace(",retention.bytes=2147483648", "")
        actions = topics_module.plan(self.wanted, self.state(missing))
        self.assertEqual(["retention.bytes=2147483648"], [actions[0].arguments[-1]])

    def test_fewer_partitions_than_the_list_are_added_and_more_are_refused_because_kafka_cannot_remove_them(self) -> None:
        fewer = DESCRIBE.replace("PartitionCount: 10", "PartitionCount: 4", 1)
        actions = topics_module.plan(self.wanted, self.state(fewer))
        self.assertEqual(["--alter", "--topic", "replay_events", "--partitions", "10"], actions[0].arguments)
        more = DESCRIBE.replace("PartitionCount: 10", "PartitionCount: 12", 1)
        with self.assertRaisesRegex(topics_module.TopicError, "cannot remove partitions"):
            topics_module.plan(self.wanted, self.state(more))

    def test_a_topic_that_the_list_does_not_name_is_reported_and_never_removed(self) -> None:
        self.assertEqual(["__consumer_offsets"], [name for name in self.state() if name not in {topic.name for topic in self.wanted}])
        self.assertEqual([], topics_module.unlisted(self.wanted, self.state()))
        self.assertEqual(
            ["extra"],
            topics_module.unlisted(self.wanted, self.state(DESCRIBE + "Topic: extra\tTopicId: x\tPartitionCount: 1\tReplicationFactor: 1\tConfigs: \n")),
        )


class ListenerTest(unittest.TestCase):
    def kafka(self) -> dict[str, Any]:
        return service_file("kafka")

    def test_the_real_service_file_opens_the_compose_network_and_the_loopback_address_only(self) -> None:
        self.assertEqual([], listener_problems(self.kafka()))

    def test_a_planted_listener_on_another_address_is_named(self) -> None:
        for listeners in (
            "INTERNAL://:9092,HOST://:9094,CONTROLLER://127.0.0.1:9093,EXTERNAL://0.0.0.0:9095",
            "INTERNAL://:9092,HOST://0.0.0.0:9094,CONTROLLER://127.0.0.1:9093",
            "INTERNAL://:9092,HOST://:9094,CONTROLLER://0.0.0.0:9093",
        ):
            with self.subTest(listeners=listeners):
                document = self.kafka()
                document["services"]["kafka"]["environment"]["KAFKA_LISTENERS"] = listeners
                problems = listener_problems(document)
                self.assertEqual(1, len(problems))
                self.assertIn("KAFKA_LISTENERS", problems[0])

    def test_an_advertised_address_outside_the_compose_network_and_the_loopback_address_is_named(self) -> None:
        document = self.kafka()
        document["services"]["kafka"]["environment"]["KAFKA_ADVERTISED_LISTENERS"] = "INTERNAL://kafka:9092,HOST://203.0.113.7:${KAFKA_HOST_PORT:-9094}"
        problems = listener_problems(document)
        self.assertEqual(1, len(problems))
        self.assertIn("KAFKA_ADVERTISED_LISTENERS", problems[0])

    def test_a_published_port_on_every_address_or_the_host_network_is_named(self) -> None:
        for ports in (["9094:9094"], ["0.0.0.0:9094:9094"], [KAFKA_PORT, "127.0.0.1:9092:9092"]):
            document = self.kafka()
            document["services"]["kafka"]["ports"] = ports
            problems = listener_problems(document)
            self.assertEqual(1, len(problems))
            self.assertIn("published port", problems[0])
        document = self.kafka()
        document["services"]["kafka"]["network_mode"] = "host"
        self.assertIn("network_mode", listener_problems(document)[0])

    def test_the_jmx_agent_that_the_launch_script_opens_on_every_address_is_turned_off(self) -> None:
        options = str(self.kafka()["services"]["kafka"]["environment"].get("KAFKA_JMX_OPTS", ""))
        self.assertTrue(options.strip())
        self.assertNotIn("jmxremote", options)

    def test_a_security_protocol_for_a_listener_that_does_not_exist_is_named(self) -> None:
        document = self.kafka()
        document["services"]["kafka"]["environment"]["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"] += ",EXTERNAL:PLAINTEXT"
        self.assertIn("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", listener_problems(document)[0])


class StackBoundsTest(unittest.TestCase):
    def test_the_two_services_are_in_the_supporting_group_so_the_stream_path_is_always_on(self) -> None:
        services = load_limits().services
        for name in ("kafka", "apicurio"):
            with self.subTest(service=name):
                self.assertEqual(("compose", "supporting"), (services[name].runtime, services[name].group))

    def test_neither_service_sets_a_log_option_so_the_size_limit_of_the_docker_role_applies(self) -> None:
        for name in ("kafka", "apicurio"):
            with self.subTest(service=name):
                self.assertNotIn("logging", service_file(name)["services"][name])

    def test_the_docker_role_keeps_five_log_files_of_50_mib_for_each_container(self) -> None:
        variables: Any = load_yaml((ROOT / "ansible" / "group_vars" / "all.yml").read_text())
        size = int(str(variables["docker_log_max_size"]).rstrip("m"))
        self.assertEqual(250, size * int(variables["docker_log_max_file"]))

    def test_the_registry_user_owns_its_password_file_and_the_account_and_its_line_exist(self) -> None:
        self.assertEqual(["apicurio"], accounts.parse_tools(TOOLS_FILE.read_text()).get("apicurio"))
        self.assertIn("apicurio 10002:10002", (COMPOSE / "openbao_services.txt").read_text())
        self.assertEqual("10002:10002", service_file("apicurio")["services"]["apicurio"]["user"])

    def test_the_registry_reads_its_database_password_from_a_file_and_holds_no_value(self) -> None:
        environment = service_file("apicurio")["services"]["apicurio"]["environment"]
        self.assertEqual("/run/secrets/apicurio_database_password", environment["APICURIO_DATASOURCE_PASSWORD_FILE"])
        self.assertNotIn("APICURIO_DATASOURCE_PASSWORD", environment)
        self.assertEqual("sql", environment["APICURIO_STORAGE_KIND"])
        self.assertEqual("postgresql", environment["APICURIO_STORAGE_SQL_KIND"])


class RegistryRulesTest(unittest.TestCase):
    rules = rules_module.parse_rules((COMPOSE / "registry" / "rules.txt").read_text())

    def test_the_tracked_file_sets_the_backward_rule_globally_and_on_every_group_it_names(self) -> None:
        self.assertEqual(("global", "", "COMPATIBILITY", "BACKWARD"), (self.rules[0].scope, self.rules[0].group, self.rules[0].rule, self.rules[0].setting))
        for rule in self.rules:
            self.assertEqual(("COMPATIBILITY", "BACKWARD"), (rule.rule, rule.setting))

    def test_a_group_line_names_its_group_and_a_global_line_names_none(self) -> None:
        parsed = rules_module.parse_rules("global COMPATIBILITY BACKWARD\ngroup replay_events COMPATIBILITY BACKWARD\n")
        self.assertEqual(["", "replay_events"], [rule.group for rule in parsed])

    def test_a_line_that_is_not_backward_or_is_malformed_is_refused(self) -> None:
        for line in (
            "global COMPATIBILITY NONE",
            "global COMPATIBILITY FORWARD",
            "global VALIDITY FULL",
            "global",
            "group COMPATIBILITY BACKWARD",
            "group Bad_Name COMPATIBILITY BACKWARD",
            "group g COMPATIBILITY BACKWARD extra",
            "everyone COMPATIBILITY BACKWARD",
        ):
            with self.subTest(line=line), self.assertRaises(rules_module.RuleError):
                rules_module.parse_rules(line + "\n")

    def test_a_repeated_rule_or_a_file_with_no_global_line_is_refused(self) -> None:
        with self.assertRaisesRegex(rules_module.RuleError, "more than once"):
            rules_module.parse_rules("global COMPATIBILITY BACKWARD\nglobal COMPATIBILITY BACKWARD\n")
        with self.assertRaisesRegex(rules_module.RuleError, "global"):
            rules_module.parse_rules("group g COMPATIBILITY BACKWARD\n")

    def test_the_plan_sets_a_missing_rule_changes_a_different_one_and_leaves_a_matching_one(self) -> None:
        wanted = rules_module.parse_rules("global COMPATIBILITY BACKWARD\ngroup g COMPATIBILITY BACKWARD\n")
        missing = rules_module.plan(wanted, None, {})
        self.assertEqual(
            ["set the global COMPATIBILITY rule to BACKWARD", "create group g", "set the COMPATIBILITY rule of group g to BACKWARD"],
            [a.description for a in missing],
        )
        other = rules_module.plan(wanted, "NONE", {"g": "FULL"})
        self.assertEqual(["set the global COMPATIBILITY rule to BACKWARD", "set the COMPATIBILITY rule of group g to BACKWARD"], [a.description for a in other])
        self.assertEqual([], rules_module.plan(wanted, "BACKWARD", {"g": "BACKWARD"}))


if __name__ == "__main__":
    unittest.main()
