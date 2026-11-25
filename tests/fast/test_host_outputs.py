"""No Terraform output may hold an address that the host gets anew on every start.

The host has no Elastic IP, so AWS gives its instance a new public address and a new public DNS name on every ``make host-start``. The plan review gate counts
each output change on purpose: the deploy reads ``heavy_memory_mib`` from state, and an output that is not applied must never reach the host. An output that
holds one of these changing values therefore turns every start into a plan change, and ``make host-up`` stops for an owner review each time. Nothing reads
such an output. The check reads the Terraform text under ``infra/`` with no parsing of Terraform and no cloud. The root outputs may name no public address or
public DNS name at all. A module output may not take one from an instance. The cases cover the planted forms of both.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

INFRA = Path(__file__).resolve().parents[2] / "infra"
ROOT_OUTPUTS = INFRA / "outputs.tf"
CHANGING_ATTRIBUTE = re.compile(r"\bpublic_(?:ip|dns)\b")
INSTANCE_CHANGING_ATTRIBUTE = re.compile(r"\baws_instance\.\w+(?:\[[^\]]*\])?\.public_(?:ip|dns)\b")
OUTPUT_START = re.compile(r'^\s*output\s+"([^"]+)"\s*\{', re.MULTILINE)
PROSE_LINE = re.compile(r"^\s*(?:description|error_message|#|//)")


def block_end(text: str, start: int) -> int:
    """The index after the brace that closes the block whose opening brace is at ``start``. Braces inside a quoted string do not count."""
    depth = 0
    quoted = False
    index = start
    while index < len(text):
        character = text[index]
        if quoted:
            if character == "\\":
                index += 1
            elif character == '"':
                quoted = False
        elif character == '"':
            quoted = True
        elif character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
            if depth == 0:
                return index + 1
        index += 1
    raise ValueError("an output block has no closing brace")


def declared_outputs(text: str) -> dict[str, str]:
    """Each output's name and the text of its block, without the lines of prose (description, error message and comments) that may name a word."""
    found: dict[str, str] = {}
    for match in OUTPUT_START.finditer(text):
        block = text[match.end() - 1 : block_end(text, match.end() - 1)]
        found[match.group(1)] = "\n".join(line for line in block.splitlines() if not PROSE_LINE.match(line))
    return found


def root_problems(text: str) -> list[str]:
    return [f"output {name} names an address that changes on every start" for name, block in declared_outputs(text).items() if CHANGING_ATTRIBUTE.search(block)]


def module_problems(text: str) -> list[str]:
    return [
        f"output {name} takes an address that changes on every start from an instance"
        for name, block in declared_outputs(text).items()
        if INSTANCE_CHANGING_ATTRIBUTE.search(block)
    ]


class RootOutputsTest(unittest.TestCase):
    def test_the_root_outputs_name_no_public_address_and_no_public_dns_name(self) -> None:
        self.assertEqual([], root_problems(ROOT_OUTPUTS.read_text()))

    def test_the_check_reads_the_root_outputs_that_the_deploy_needs(self) -> None:
        found = declared_outputs(ROOT_OUTPUTS.read_text())
        self.assertIn("heavy_memory_mib", found)
        self.assertIn("workload_role_arns", found)
        self.assertIn("host_instance_id", found)

    def test_a_root_output_of_the_public_address_is_named_whatever_its_name(self) -> None:
        for label, text in (
            ("the former output", 'output "host_public_ip" {\n  value = one(module.host[*].public_ip)\n}\n'),
            ("another name", 'output "address" {\n  value = module.host.public_ip\n}\n'),
            ("the DNS name", 'output "host_public_dns" {\n  value = one(module.host[*].public_dns)\n}\n'),
            ("the instance itself", 'output "address" {\n  value = aws_instance.host.public_ip\n}\n'),
            ("inside a string", 'output "address" {\n  value = "ssh ${module.host.public_ip}"\n}\n'),
            (
                "a nested block",
                'output "address" {\n  value = module.host.public_dns\n  precondition {\n    condition = true\n    error_message = "x"\n  }\n}\n',
            ),
        ):
            with self.subTest(label):
                self.assertEqual(1, len(root_problems(text)))

    def test_a_root_output_that_does_not_change_on_start_is_not_named(self) -> None:
        for label, text in (
            ("the instance ID", 'output "host_instance_id" {\n  value = one(module.host[*].instance_id)\n}\n'),
            ("a description that names the word", 'output "x" {\n  description = "not the public_ip"\n  value = 1\n}\n'),
            ("a comment that names the word", '# the public_ip is not output\noutput "x" {\n  value = 1\n}\n'),
            ("a name that only starts like it", 'output "x" {\n  value = module.host.public_ipv4_pool\n}\n'),
        ):
            with self.subTest(label):
                self.assertEqual([], root_problems(text))


class ModuleOutputsTest(unittest.TestCase):
    def test_no_module_output_takes_the_public_address_or_the_public_dns_name_of_an_instance(self) -> None:
        files = sorted(path for path in INFRA.rglob("outputs.tf") if ".terraform" not in path.parts and path != ROOT_OUTPUTS)
        self.assertTrue(files)
        for path in files:
            with self.subTest(path.relative_to(INFRA).as_posix()):
                self.assertEqual([], module_problems(path.read_text()))

    def test_the_host_module_outputs_are_read(self) -> None:
        found = declared_outputs((INFRA / "modules" / "host" / "outputs.tf").read_text())
        self.assertIn("instance_id", found)
        self.assertIn("role_arn", found)

    def test_a_module_output_of_an_instance_attribute_that_changes_is_named(self) -> None:
        for label, text in (
            ("the former output", 'output "public_ip" {\n  value = aws_instance.host.public_ip\n}\n'),
            ("another name", 'output "address" {\n  value = aws_instance.host.public_ip\n}\n'),
            ("the DNS name", 'output "dns" {\n  value = aws_instance.host.public_dns\n}\n'),
            ("an instance with a count", 'output "dns" {\n  value = aws_instance.host[0].public_dns\n}\n'),
        ):
            with self.subTest(label):
                self.assertEqual(1, len(module_problems(text)))

    def test_a_module_output_that_does_not_come_from_an_instance_is_not_named(self) -> None:
        for label, text in (
            ("the instance ID", 'output "instance_id" {\n  value = aws_instance.host.id\n}\n'),
            ("an address that stays", 'output "address" {\n  value = aws_eip.host.public_ip\n}\n'),
        ):
            with self.subTest(label):
                self.assertEqual([], module_problems(text))


if __name__ == "__main__":
    unittest.main()
