"""The k3s check decodes the node's arguments and refuses any component that is not disabled by its own ``--disable``.

The role reads ``k3s.io/node-args`` from the running node: a JSON list of the arguments k3s started with. The check
runs here through the real ``ansible-playbook`` with the values a host would give, so these cases cover wrong
arguments that a live host does not readily produce.
"""

from __future__ import annotations

import json
import unittest

from tests.fast.ansible_support import run_task_file

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


if __name__ == "__main__":
    unittest.main()
