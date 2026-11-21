"""Ansible module: the compatibility rules of Apicurio Registry, from the tracked list.

The step makes the running registry match ``compose/registry/rules.txt``. A global line sets the rule for every group. A group line creates the group when
it is missing and sets the rule on it. A registry that already matches needs no change and the step reports none. It uses only the standard library and
runs on the host's Python.
"""

from __future__ import annotations

import http.client
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ansible.module_utils.basic import AnsibleModule

API = "/apis/registry/v3"
RULE = "COMPATIBILITY"
SETTING = "BACKWARD"
GROUP_NAME = re.compile(r"[a-z][a-z0-9_]{0,63}")
TIMEOUT_SECONDS = 15
ERROR_CHARACTERS = 300


class RuleError(RuntimeError):
    """The rule list is not usable or the registry's answer is not the expected one."""


@dataclass(frozen=True)
class Rule:
    scope: str
    group: str
    rule: str
    setting: str


@dataclass(frozen=True)
class Action:
    description: str
    method: str
    path: str
    body: dict[str, str]


def parse_rule(line: str) -> Rule:
    fields = line.split()
    scope = fields[0]
    if scope == "global" and len(fields) == 3:
        group, rule, setting = "", fields[1], fields[2]
    elif scope == "group" and len(fields) == 4:
        group, rule, setting = fields[1:]
        if not GROUP_NAME.fullmatch(group):
            raise RuleError(f"group name {group!r} must be lower case letters, digits and underscores, starting with a letter")
    else:
        raise RuleError(f"line {line!r}: a line is `global RULE SETTING` or `group NAME RULE SETTING`")
    if (rule, setting) != (RULE, SETTING):
        raise RuleError(f"line {line!r}: the only rule is {RULE} and its only setting is {SETTING}")
    return Rule(scope, group, rule, setting)


def parse_rules(text: str) -> list[Rule]:
    """The rules in the tracked list: one for each line, with comments and blank lines ignored. The list holds the global rule first."""
    rules: list[Rule] = []
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        rule = parse_rule(line)
        if (rule.scope, rule.group) in [(other.scope, other.group) for other in rules]:
            raise RuleError(f"line {line!r}: the {rule.rule} rule of {rule.scope} {rule.group}".rstrip() + " is listed more than once")
        rules.append(rule)
    if not rules or rules[0].scope != "global":
        raise RuleError("the rule list must start with the global line")
    return rules


def rule_action(rule: Rule, current: str | None) -> Action:
    base = f"{API}/admin/rules" if rule.scope == "global" else f"{API}/groups/{rule.group}/rules"
    subject = "global COMPATIBILITY rule" if rule.scope == "global" else f"COMPATIBILITY rule of group {rule.group}"
    description = f"set the {subject} to {rule.setting}"
    body = {"ruleType": rule.rule, "config": rule.setting}
    return Action(description, "POST" if current is None else "PUT", base if current is None else f"{base}/{rule.rule}", body)


def plan(rules: list[Rule], global_setting: str | None, groups: dict[str, str | None]) -> list[Action]:
    """The requests that make the registry match the list. ``groups`` holds each existing group with its current rule setting, or None for no rule."""
    actions: list[Action] = []
    for rule in rules:
        if rule.scope == "global":
            current = global_setting
        elif rule.group not in groups:
            actions.append(Action(f"create group {rule.group}", "POST", f"{API}/groups", {"groupId": rule.group}))
            current = None
        else:
            current = groups[rule.group]
        if current != rule.setting:
            actions.append(rule_action(rule, current))
    return actions


class Registry:
    """The running registry, reached over its local address."""

    def __init__(self, host: str, port: int, send: Callable[[str, str, dict[str, str] | None], tuple[int, Any]] | None = None) -> None:
        self.host = host
        self.port = port
        self.send = send or self.request

    def request(self, method: str, path: str, body: dict[str, str] | None) -> tuple[int, Any]:
        data = None if body is None else json.dumps(body)
        connection = http.client.HTTPConnection(self.host, self.port, timeout=TIMEOUT_SECONDS)
        try:
            connection.request(method, path, body=data, headers={"Content-Type": "application/json"})
            response = connection.getresponse()
            text = response.read().decode()
            return response.status, json.loads(text) if text and response.status == 200 else None
        except (OSError, http.client.HTTPException, ValueError) as error:
            raise RuleError(f"the registry at {self.host}:{self.port} gave no usable answer: {type(error).__name__}") from error
        finally:
            connection.close()

    def setting(self, path: str) -> str | None:
        status, body = self.send("GET", path, None)
        if status == 404:
            return None
        if status != 200 or not isinstance(body, dict):
            raise RuleError(f"GET {path} answered {status}")
        return str(body["config"])

    def state(self, rules: list[Rule]) -> tuple[str | None, dict[str, str | None]]:
        global_setting = self.setting(f"{API}/admin/rules/{RULE}")
        groups: dict[str, str | None] = {}
        for rule in rules:
            if rule.scope == "group":
                status, _ = self.send("GET", f"{API}/groups/{rule.group}", None)
                if status == 200:
                    groups[rule.group] = self.setting(f"{API}/groups/{rule.group}/rules/{RULE}")
                elif status != 404:
                    raise RuleError(f"GET group {rule.group} answered {status}")
        return global_setting, groups

    def apply(self, action: Action) -> None:
        status, _ = self.send(action.method, action.path, action.body)
        if status not in (200, 204):
            raise RuleError(f"{action.description}: the registry answered {status}"[:ERROR_CHARACTERS])


def main() -> None:
    module = AnsibleModule(
        argument_spec={
            "rules_file": {"type": "path", "required": True},
            "host": {"type": "str", "default": "127.0.0.1"},
            "port": {"type": "int", "required": True},
        },
        supports_check_mode=True,
    )
    try:
        with open(module.params["rules_file"]) as handle:
            rules = parse_rules(handle.read())
        registry = Registry(module.params["host"], module.params["port"])
        actions = plan(rules, *registry.state(rules))
        if not module.check_mode:
            for action in actions:
                registry.apply(action)
        module.exit_json(changed=bool(actions), actions=[action.description for action in actions])
    except (RuleError, OSError) as error:
        module.fail_json(msg=str(error))


if __name__ == "__main__":
    main()
