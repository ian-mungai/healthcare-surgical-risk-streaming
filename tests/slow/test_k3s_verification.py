"""The k3s check decodes the node's arguments and refuses any component that is not disabled by its own ``--disable``.

The role reads ``k3s.io/node-args`` from the running node: a JSON list of the arguments k3s started with. The check
runs here through the real ``ansible-playbook`` with the values a host would give, so these cases cover wrong
arguments that a live host does not readily produce.
"""

from __future__ import annotations

import json
import shlex
import unittest
from pathlib import Path
from typing import Any

from scripts.stack.yaml_text import load_yaml
from tests.slow.ansible_support import ROLES, run_task_file

GOOD_ARGUMENTS = ["server", "--disable", "traefik", "--disable", "servicelb", "--write-kubeconfig-mode", "0600"]
GOOD_OBJECTS = "pod/coredns-1\nservice/kubernetes\nservice/kube-dns\n"
MESSAGE = "k3s runs with its bundled ingress controller or service load balancer"


def verdict(arguments: object, objects: str = GOOD_OBJECTS) -> tuple[int, str]:
    stdout = arguments if isinstance(arguments, str) else json.dumps(arguments)
    result = run_task_file("k3s", "verify_disabled.yml", {"k3s_node_arguments": {"stdout": stdout}, "k3s_objects": {"stdout": objects}})
    return result.returncode, result.stdout + result.stderr


class DisabledComponentsTest(unittest.TestCase):
    def test_both_names_after_their_own_disable_pass(self) -> None:
        code, output = verdict(GOOD_ARGUMENTS)
        self.assertEqual(0, code, output)

    def test_a_name_that_is_not_directly_after_disable_is_refused(self) -> None:
        code, output = verdict(["server", "--disable", "traefik", "servicelb"])
        self.assertNotEqual(0, code)
        self.assertIn(MESSAGE, output)

    def test_a_name_after_another_flag_is_refused(self) -> None:
        code, output = verdict(["server", "--no-deploy", "traefik", "--disable", "servicelb"])
        self.assertNotEqual(0, code)
        self.assertIn(MESSAGE, output)

    def test_a_missing_name_is_refused(self) -> None:
        for arguments in (["server", "--disable", "traefik"], ["server", "--disable", "servicelb"]):
            with self.subTest(arguments=arguments):
                code, output = verdict(arguments)
                self.assertNotEqual(0, code)
                self.assertIn(MESSAGE, output)

    def test_arguments_that_are_not_a_json_list_are_refused(self) -> None:
        code, output = verdict("--disable traefik --disable servicelb")
        self.assertNotEqual(0, code)
        self.assertRegex(output, "Expecting value|JSON")

    def test_a_running_ingress_object_is_refused_although_the_arguments_are_right(self) -> None:
        code, output = verdict(GOOD_ARGUMENTS, GOOD_OBJECTS + "pod/traefik-abc\n")
        self.assertNotEqual(0, code)
        self.assertIn(MESSAGE, output)

    def test_a_running_load_balancer_pod_is_refused_although_the_arguments_are_right(self) -> None:
        code, output = verdict(GOOD_ARGUMENTS, GOOD_OBJECTS + "pod/svclb-abc\n")
        self.assertNotEqual(0, code)
        self.assertIn(MESSAGE, output)


ENABLED = "Encryption Status: Enabled\nCurrent Rotation Stage: reencrypt_finished\nServer Encryption Hashes: All hashes match\n"
STARTED_ENABLED = (
    "Encryption Status: Enabled\nCurrent Rotation Stage: start\nServer Encryption Hashes: All hashes match\n\n"
    "Active  Key Type  Name\n------  --------  ----\n *      AES-CBC   aescbckey\n"
)
ENCRYPTION_MESSAGE = "k3s secret encryption at rest is not on"


def encryption_verdict(status: str) -> tuple[int, str]:
    result = run_task_file("k3s", "verify_secrets_encryption.yml", {"k3s_secrets_status": {"stdout": status}})
    return result.returncode, result.stdout + result.stderr


def command_words(role: Path = ROLES / "k3s") -> list[list[str]]:
    """The words of every command or shell task in the k3s role, whether written as ``cmd``, ``argv`` or free-form, blocks included."""
    found: list[list[str]] = []

    def walk(tasks: object) -> None:
        if not isinstance(tasks, list):
            return
        for task in tasks:
            if not isinstance(task, dict):
                continue
            for key in ("block", "rescue", "always"):
                walk(task.get(key))
            for module in ("ansible.builtin.command", "ansible.builtin.shell", "command", "shell"):
                if module in task:
                    found.append(words_of(task[module]) + words_of(task.get("args")))

    for path in sorted([*(role / "tasks").rglob("*.yml"), *(role / "handlers").rglob("*.yml")]):
        walk(load_yaml(path.read_text()))
    return found


def words_of(value: object) -> list[str]:
    """The words of a command as Ansible splits them: shell quoting removed, from a free-form string or from ``argv`` and ``cmd``."""
    if isinstance(value, str):
        return shlex.split(value)
    if isinstance(value, dict):
        return [str(word) for word in value.get("argv", [])] + shlex.split(str(value.get("cmd", "")))
    return []


def configuration() -> dict[str, Any]:
    tasks = load_yaml((ROLES / "k3s" / "tasks" / "main.yml").read_text())
    if not isinstance(tasks, list):
        raise TypeError("the task file must be a list")
    document = load_yaml(next(task["ansible.builtin.copy"]["content"] for task in tasks if task.get("name", "").startswith("Set the k3s configuration")))
    if not isinstance(document, dict):
        raise TypeError("the configuration must be a mapping")
    return document


class SecretsEncryptionTest(unittest.TestCase):
    def test_the_configuration_turns_encryption_on_and_keeps_the_two_components_off(self) -> None:
        document = configuration()
        self.assertIs(True, document["secrets-encryption"])
        self.assertEqual(["traefik", "servicelb"], document["disable"])

    def test_an_enabled_status_with_a_finished_rotation_passes(self) -> None:
        code, output = encryption_verdict(ENABLED)
        self.assertEqual(0, code, output)

    def test_an_enabled_cluster_at_the_start_stage_with_matching_hashes_passes(self) -> None:
        """The live host's reading (Nov 23 2026). Owner Q64 "A": the role runs no key rotation, so Enabled at the start stage with every
        hash matching is accepted; secrets written before the flag may keep plain-text copies in Kine rows, a recorded limit."""
        code, output = encryption_verdict(STARTED_ENABLED)
        self.assertEqual(0, code, output)

    def test_no_task_rotates_the_keys_or_calls_enable_in_any_command_form(self) -> None:
        """The pinned rotate-keys can drop a key in use (owner Q64). enable writes an identity-only configuration that the pinned k3s keeps on a
        restart, so a run stopped before its restart could leave the cluster at Disabled/start (recheck 3); a restart with the flag makes the file."""
        for words in command_words():
            with self.subTest(words=words):
                self.assertNotIn("rotate-keys", words)
                self.assertFalse("secrets-encrypt" in words and "enable" in words, words)

    def test_the_guard_reads_task_args_quoted_words_nested_task_files_and_handlers(self) -> None:
        """Review recheck 4 (Low): each of these forms runs a key rotation in Ansible and must reach the guard."""
        import tempfile

        forms = {
            "tasks/nested/rotation.yml": "- ansible.builtin.command:\n  args:\n    argv: [k3s, secrets-encrypt, rotate-keys]\n",
            "tasks/quoted.yml": "- ansible.builtin.command:\n    cmd: k3s secrets-encrypt 'rotate-keys'\n",
            "handlers/main.yml": "- name: rotate\n  ansible.builtin.shell: k3s secrets-encrypt rotate-keys\n",
        }
        for relative, text in forms.items():
            with self.subTest(form=relative), tempfile.TemporaryDirectory() as scratch:
                path = Path(scratch) / relative
                path.parent.mkdir(parents=True)
                path.write_text(text)
                self.assertTrue(any("rotate-keys" in words for words in command_words(Path(scratch))), relative)

    def test_a_cluster_with_no_encryption_configuration_is_restarted_with_the_flag_and_read_again(self) -> None:
        tasks = load_yaml((ROLES / "k3s" / "tasks" / "secrets_encryption.yml").read_text())
        if not isinstance(tasks, list):
            raise TypeError("the task file must be a list")
        branch = next(task for task in tasks if "no configuration file found" in str(task.get("when", "")))
        steps = [next(key for key in step if key.startswith("ansible.builtin.")) for step in branch["block"]]
        self.assertEqual("ansible.builtin.systemd_service", steps[0])
        self.assertEqual("restarted", branch["block"][0]["ansible.builtin.systemd_service"]["state"])
        self.assertIn("ansible.builtin.include_tasks", steps)
        self.assertEqual(["ansible.builtin.command"], [step for step in steps if step == "ansible.builtin.command"])
        self.assertEqual("k3s secrets-encrypt status", branch["block"][steps.index("ansible.builtin.command")]["ansible.builtin.command"]["cmd"])

    def test_every_other_status_is_refused_with_the_same_message(self) -> None:
        cases = {
            "disabled with no file": "Encryption Status: Disabled, no configuration file found\n",
            "disabled at the start stage": "Encryption Status: Disabled\nCurrent Rotation Stage: start\n",
            "enabled while keys rotate": "Encryption Status: Enabled\nCurrent Rotation Stage: reencrypt_active\n",
            "enabled while keys rotate, hashes matching": (
                "Encryption Status: Enabled\nCurrent Rotation Stage: reencrypt_active\nServer Encryption Hashes: All hashes match\n"
            ),
            "disabled with a finished stage, hashes matching": (
                "Encryption Status: Disabled\nCurrent Rotation Stage: reencrypt_finished\nServer Encryption Hashes: All hashes match\n"
            ),
            "disabled with a finished stage": "Encryption Status: Disabled\nCurrent Rotation Stage: reencrypt_finished\n",
            "enabled at the start stage with hashes that differ": (
                "Encryption Status: Enabled\nCurrent Rotation Stage: start\nServer Encryption Hashes: hash does not match\n"
            ),
            "enabled and finished with no hash line": "Encryption Status: Enabled\nCurrent Rotation Stage: reencrypt_finished\n",
            "enabled at a stage whose name only starts like start": (
                "Encryption Status: Enabled\nCurrent Rotation Stage: started\nServer Encryption Hashes: All hashes match\n"
            ),
            "enabled at a stage whose name only starts like reencrypt_finished": (
                "Encryption Status: Enabled\nCurrent Rotation Stage: reencrypt_finished_x\nServer Encryption Hashes: All hashes match\n"
            ),
            "empty": "",
            "unrelated text": "Enabled\n",
        }
        for label, status in cases.items():
            with self.subTest(label):
                code, output = encryption_verdict(status)
                self.assertNotEqual(0, code)
                self.assertIn(ENCRYPTION_MESSAGE, output)

    def test_the_refusal_shows_fixed_text_and_none_of_the_status_output(self) -> None:
        """k3s prints paths, key names and error text in its status output. The message never carries it."""
        leaks = ("/srv/example.invalid/k3s/leaked-path", "leaked-key-name-7", "error loading leaked-token-8")
        statuses = {
            "disabled": "Encryption Status: Disabled\n" + "\n".join(leaks) + "\n",
            "rotating": "Encryption Status: Enabled\nCurrent Rotation Stage: reencrypt_active\n" + "\n".join(leaks) + "\n",
            "hashes differ": "Encryption Status: Enabled\nCurrent Rotation Stage: start\nServer Encryption Hashes: " + " ".join(leaks) + "\n",
        }
        for label, status in statuses.items():
            with self.subTest(label):
                code, output = encryption_verdict(status)
                self.assertNotEqual(0, code)
                self.assertIn(ENCRYPTION_MESSAGE, output)
                for leaked in leaks:
                    self.assertNotIn(leaked, output)


if __name__ == "__main__":
    unittest.main()
