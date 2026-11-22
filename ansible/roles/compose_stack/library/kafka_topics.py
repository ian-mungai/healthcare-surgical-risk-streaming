"""Ansible module: the Kafka topics, from the tracked list.

The step makes the running broker match ``compose/kafka/topics.txt``: it creates a missing topic with its partitions and settings, corrects a topic whose
retention limits, segment size, cleanup policy or flush setting differ and adds partitions when the list asks for more. Kafka cannot remove partitions, so a
topic with more partitions than the list stops the step with a message that names it. A topic that the list does not name is reported and never removed.
A broker that already matches needs no change and the step reports none. A setting counts as matching only when it is set on the topic itself: a value that
the topic inherits from the broker's default is set on the topic too, because removing the default would otherwise change the topic.

Every topic in the list carries events that the platform acknowledges, so every topic gets ``flush.messages=1``: the broker writes each record to disk before it
acknowledges it. The single broker has no replica to restore a record that a power loss took from the operating system's cache. Kafka applies the setting
to each partition.

It reaches the broker through ``docker compose exec`` and the tools that ship in the image. It uses only the standard library and runs on the host's Python.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, replace

from ansible.module_utils.basic import AnsibleModule

MIB = 1024 * 1024
GIB = 1024 * MIB
HOUR_MS = 3_600_000
TOPIC_NAME = re.compile(r"[a-z][a-z0-9_]{0,59}")
CLEANUP_POLICIES = ("delete",)
MAXIMUM_PARTITIONS = 100
SERVICE = "kafka"
TOOLS_FOLDER = "/opt/kafka/bin"
BOOTSTRAP = "127.0.0.1:9092"
REPLICATION_FACTOR = 1
FLUSH_MESSAGES = 1
MANAGED = ("cleanup.policy", "retention.ms", "retention.bytes", "segment.bytes", "flush.messages")
LIST_SETTINGS = ("cleanup.policy",)
OVERRIDES_HEADER = re.compile(r"Dynamic configs for topic (\S+) are:")
OVERRIDE_START = re.compile(r"  ([^\s=]+)=")
ERROR_CHARACTERS = 400


class TopicError(RuntimeError):
    """The topic list, the budget or the broker's answer is not usable."""


@dataclass(frozen=True)
class Topic:
    name: str
    partitions: int
    retention_hours: int
    retention_mib: int
    segment_mib: int
    cleanup: str

    def configs(self) -> dict[str, str]:
        return {
            "cleanup.policy": self.cleanup,
            "retention.ms": str(self.retention_hours * HOUR_MS),
            "retention.bytes": str(self.retention_mib * MIB),
            "segment.bytes": str(self.segment_mib * MIB),
            "flush.messages": str(FLUSH_MESSAGES),
        }

    def worst_case_bytes(self) -> int:
        """The most one topic can hold: each partition keeps up to its size limit of closed segments and one segment that is still open."""
        return self.partitions * (self.retention_mib + self.segment_mib) * MIB


@dataclass(frozen=True)
class TopicState:
    """A topic of the broker: its partitions and the settings set on the topic itself. A value that it only inherits from the broker is not in ``configs``."""

    partitions: int
    configs: dict[str, str]


@dataclass(frozen=True)
class Action:
    description: str
    tool: str
    arguments: list[str]


def whole_number(text: str, what: str, line: str, minimum: int = 1) -> int:
    if not re.fullmatch(r"[0-9]{1,9}", text) or int(text) < minimum:
        raise TopicError(f"line {line!r}: {what} must be a whole number of at least {minimum}")
    return int(text)


def parse_topic(line: str) -> Topic:
    fields = line.split()
    if len(fields) != 6:
        raise TopicError(f"line {line!r}: a topic has six fields, name, partitions, retention_hours, retention_mib, segment_mib and cleanup")
    name, partitions, hours, size, segment, cleanup = fields
    if not TOPIC_NAME.fullmatch(name):
        raise TopicError(f"topic name {name!r} must be 1 to 60 lower case letters, digits or underscores, starting with a letter")
    if cleanup not in CLEANUP_POLICIES:
        raise TopicError(f"topic {name}: cleanup must be one of {', '.join(CLEANUP_POLICIES)}")
    count = whole_number(partitions, "partitions", line)
    if count > MAXIMUM_PARTITIONS:
        raise TopicError(f"topic {name}: partitions must be at most {MAXIMUM_PARTITIONS}")
    topic = Topic(
        name,
        count,
        whole_number(hours, "retention_hours", line),
        whole_number(size, "retention_mib", line),
        whole_number(segment, "segment_mib", line),
        cleanup,
    )
    if topic.retention_mib <= topic.segment_mib:
        raise TopicError(f"topic {name}: the size limit must be larger than one segment, or the broker could never keep a closed segment")
    return topic


def parse_topics(text: str) -> list[Topic]:
    """The topics in the tracked list: one for each line, with comments and blank lines ignored."""
    topics: list[Topic] = []
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        topic = parse_topic(line)
        if topic.name in [other.name for other in topics]:
            raise TopicError(f"topic {topic.name!r} is listed more than once")
        topics.append(topic)
    if not topics:
        raise TopicError("the topic list names no topic")
    return topics


def parse_budget(text: str) -> int:
    """The storage budget in GiB: the one whole number in the file."""
    lines = [raw.split("#", 1)[0].strip() for raw in text.splitlines()]
    numbers = [line for line in lines if line]
    if len(numbers) != 1 or not re.fullmatch(r"[1-9][0-9]{0,6}", numbers[0]):
        raise TopicError("the storage budget file must hold exactly one positive whole number of GiB")
    return int(numbers[0])


def parse_state(output: str) -> dict[str, TopicState]:
    """Each topic of the broker's ``--describe`` output with its partition count. The partition lines are skipped.

    The ``Configs`` field of that output joins the effective values, the inherited ones too, with commas, and a list value has commas too, so it is not read.
    """
    state: dict[str, TopicState] = {}
    for line in output.splitlines():
        if not line.startswith("Topic: "):
            continue
        fields = dict(part.split(": ", 1) for part in line.split("\t") if ": " in part)
        try:
            state[fields["Topic"]] = TopicState(int(fields["PartitionCount"]), {})
        except (KeyError, ValueError) as error:
            raise TopicError("the broker's description of a topic has an unexpected shape") from error
    return state


def parse_override_line(line: str) -> tuple[str, str] | None:
    """The setting and the value of one line, ``<key>=<value> sensitive=<true or false> synonyms={DYNAMIC_TOPIC_CONFIG:<key>=<value>, ...}``, or None.

    The value may hold commas, colons, equals signs and the text `` sensitive=``. It ends where the line goes on with the synonyms, whose first entry is the
    topic's own setting and repeats the value, so a split that does not repeat it is not the value's end.
    """
    start = OVERRIDE_START.match(line)
    if start is None or not line.endswith("}"):
        return None
    key, rest = start[1], line[start.end() :]
    marker = re.compile(rf" sensitive=(?:true|false) synonyms=\{{DYNAMIC_TOPIC_CONFIG:{re.escape(key)}=")
    for found in marker.finditer(rest):
        value, tail = rest[: found.start()], rest[found.end() :]
        if tail.startswith(value) and tail[len(value) :][:1] in (",", "}"):
            return key, value
    return None


def parse_overrides(topic: str, output: str) -> dict[str, str]:
    """The settings set on the topic itself, from ``kafka-configs --describe`` without ``--all``. Any other shape of output stops the step.

    Every setting is kept as read, whether the step manages it or not, and the step never changes one that it does not manage. Only a setting that the step
    manages has a rule for its value: a comma is part of the value only for a list setting.
    """
    lines = [line.rstrip() for line in output.splitlines() if line.strip()]
    header = OVERRIDES_HEADER.fullmatch(lines[0]) if lines else None
    if header is None or header[1] != topic:
        raise TopicError(f"the broker's configuration of topic {topic} has an unexpected shape")
    overrides: dict[str, str] = {}
    for line in lines[1:]:
        found = parse_override_line(line)
        if found is None or found[0] in overrides or ("," in found[1] and found[0] in MANAGED and found[0] not in LIST_SETTINGS):
            raise TopicError(f"the broker's configuration of topic {topic} has an unexpected shape")
        overrides[found[0]] = found[1]
    return overrides


def read_state(broker: Broker, topics: list[Topic]) -> dict[str, TopicState]:
    """The broker's topics, and for each listed topic that exists the settings set on it. Only these decide whether a setting needs setting."""
    state = parse_state(broker.tool("kafka-topics", ["--describe"]))
    for topic in topics:
        if topic.name in state:
            described = broker.tool("kafka-configs", ["--entity-type", "topics", "--entity-name", topic.name, "--describe"])
            state[topic.name] = replace(state[topic.name], configs=parse_overrides(topic.name, described))
    return state


def plan(topics: list[Topic], state: dict[str, TopicState]) -> list[Action]:
    """The commands that make the broker match the list, and none when it already does."""
    actions: list[Action] = []
    for topic in topics:
        configs = topic.configs()
        current = state.get(topic.name)
        if current is None:
            arguments = ["--create", "--topic", topic.name, "--partitions", str(topic.partitions), "--replication-factor", str(REPLICATION_FACTOR)]
            for key, value in configs.items():
                arguments += ["--config", f"{key}={value}"]
            actions.append(Action(f"create topic {topic.name}", "kafka-topics", arguments))
            continue
        if current.partitions > topic.partitions:
            raise TopicError(f"topic {topic.name} has {current.partitions} partitions and the list asks for {topic.partitions}: Kafka cannot remove partitions")
        if current.partitions < topic.partitions:
            actions.append(
                Action(f"add partitions to topic {topic.name}", "kafka-topics", ["--alter", "--topic", topic.name, "--partitions", str(topic.partitions)])
            )
        for key in MANAGED:
            if current.configs.get(key) != configs[key]:
                arguments = ["--alter", "--entity-type", "topics", "--entity-name", topic.name, "--add-config", f"{key}={configs[key]}"]
                actions.append(Action(f"set {key} of topic {topic.name}", "kafka-configs", arguments))
    return actions


def unlisted(topics: list[Topic], state: dict[str, TopicState]) -> list[str]:
    """The topics on the broker that the list does not name. Kafka's own topics start with two underscores."""
    named = {topic.name for topic in topics}
    return sorted(name for name in state if name not in named and not name.startswith("__"))


class Broker:
    """The running Kafka service, reached through ``docker compose exec`` with the tools of its image."""

    def __init__(self, run: Callable[[list[str]], tuple[int, str, str]], compose_command: list[str]) -> None:
        self.run = run
        self.compose_command = compose_command

    def tool(self, tool: str, arguments: list[str]) -> str:
        command = [*self.compose_command, "exec", "-T", SERVICE, f"{TOOLS_FOLDER}/{tool}.sh", "--bootstrap-server", BOOTSTRAP, *arguments]
        code, output, error = self.run(command)
        if code != 0:
            raise TopicError(f"service {SERVICE}: {tool} exited with {code}: {error.strip()[:ERROR_CHARACTERS]}")
        return output


def main() -> None:
    module = AnsibleModule(
        argument_spec={"topics_file": {"type": "path", "required": True}, "compose_command": {"type": "list", "elements": "str", "required": True}},
        supports_check_mode=True,
    )
    try:
        with open(module.params["topics_file"]) as handle:
            topics = parse_topics(handle.read())
        broker = Broker(lambda args: module.run_command(args, check_rc=False), module.params["compose_command"])
        state = read_state(broker, topics)
        actions = plan(topics, state)
        if not module.check_mode:
            for action in actions:
                broker.tool(action.tool, action.arguments)
        module.exit_json(changed=bool(actions), actions=[action.description for action in actions], unlisted=unlisted(topics, state))
    except (TopicError, OSError) as error:
        module.fail_json(msg=str(error))


if __name__ == "__main__":
    main()
