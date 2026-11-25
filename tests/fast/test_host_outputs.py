"""Every root Terraform output has its check that it holds no address that the host gets anew on every start.

The host has no Elastic IP, so AWS gives its instance a new public address and a new public DNS name on every ``make host-start``. The plan review gate counts
each output change on purpose: the deploy reads ``heavy_memory_mib`` from state, and an output that is not applied must never reach the host. An output that
holds one of these changing values therefore turns every start into a plan change, and ``make host-up`` stops for an owner review each time.

The check itself is the offline Terraform test ``infra/tests/changing_address.tftest.hcl``. It gives the instance a marker address and a marker DNS name,
plans, and asserts that the evaluated value of each root output holds neither marker. A text scan cannot do this, because a local, an alias, a splat, a
``for`` projection or a whole-instance passthrough hides the attribute. A module output reaches the outside only through a root output, so the root outputs
cover the module outputs.

This test closes the one gap that Terraform leaves: its test file can only name the outputs that it lists. It reads every ``.tf`` file of the root module,
whatever the file's name, and fails for a root output that the Terraform test does not assert on, so a new output cannot skip the check.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

INFRA = Path(__file__).resolve().parents[2] / "infra"
ADDRESS_TEST = INFRA / "tests" / "changing_address.tftest.hcl"
MARKERS = ("198.51.100.77", "marker-ip.invalid")
OUTPUT_START = re.compile(r'(?:^|(?<=[\s;}]))output\s+"([^"]+)"\s*\{', re.MULTILINE)
BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
COMMENT_LINE = re.compile(r"^\s*(?:#|//).*$", re.MULTILINE)


def root_output_names(texts: list[str]) -> set[str]:
    """The name of every root output in the given Terraform texts. A commented line or a block comment declares none, and a block comment or a line break
    inside an output's header does not hide it."""
    found: set[str] = set()
    for text in texts:
        bare = COMMENT_LINE.sub("", BLOCK_COMMENT.sub(" ", text))
        found.update(match.group(1) for match in OUTPUT_START.finditer(bare))
    return found


def unchecked_outputs(names: set[str], test_text: str) -> list[str]:
    """The outputs that the Terraform test does not assert on. Only the form ``jsonencode(output.<name>)`` outside a comment counts."""
    code = COMMENT_LINE.sub("", test_text)
    return sorted(name for name in names if not re.search(rf"\bjsonencode\(output\.{re.escape(name)}\)", code))


class RootOutputChecksTest(unittest.TestCase):
    def root_texts(self) -> list[str]:
        return [path.read_text() for path in sorted(INFRA.glob("*.tf"))]

    def test_every_root_output_has_an_assertion_in_the_terraform_test(self) -> None:
        names = root_output_names(self.root_texts())
        self.assertIn("heavy_memory_mib", names)
        self.assertIn("host_instance_id", names)
        self.assertEqual([], unchecked_outputs(names, ADDRESS_TEST.read_text()))

    def test_the_root_module_has_no_json_form_that_this_scan_cannot_read(self) -> None:
        self.assertEqual([], sorted(path.name for path in INFRA.glob("*.tf.json")))

    def test_the_terraform_test_plants_both_markers_in_the_instance(self) -> None:
        code = COMMENT_LINE.sub("", ADDRESS_TEST.read_text())
        for marker in MARKERS:
            self.assertIn(f'"{marker}"', code)
        self.assertRegex(code, r"target\s*=\s*module\.host\[0\]\.aws_instance\.host")
        self.assertRegex(code, r'public_ip\s*=\s*"198\.51\.100\.77"')
        self.assertRegex(code, r'public_dns\s*=\s*"marker-ip\.invalid"')

    def test_an_output_in_any_file_is_found_and_a_missing_assertion_is_named(self) -> None:
        in_main = 'resource "x" "y" {}\n\noutput "address" {\n  value = aws_instance.host.public_ip\n}\n'
        in_outputs = 'output "host_instance_id" {\n  value = 1\n}\n'
        names = root_output_names([in_main, in_outputs])
        self.assertEqual({"address", "host_instance_id"}, names)
        test_text = "assert { condition = !strcontains(jsonencode(output.host_instance_id), marker) }\n"
        self.assertEqual(["address"], unchecked_outputs(names, test_text))

    def test_a_name_in_a_comment_or_a_longer_name_is_not_an_assertion(self) -> None:
        names = {"host_role_arn"}
        for label, text in (
            ("a comment", "# jsonencode(output.host_role_arn)\n"),
            ("a longer name", "assert { condition = jsonencode(output.host_role_arn_extra) }\n"),
            ("no jsonencode", "assert { condition = output.host_role_arn == null }\n"),
        ):
            with self.subTest(label):
                self.assertEqual(["host_role_arn"], unchecked_outputs(names, text))

    def test_a_commented_output_declares_nothing(self) -> None:
        self.assertEqual(set(), root_output_names(['# output "x" {\n', '/* output "y" {\n} */\n']))

    def test_an_output_header_with_block_comments_or_line_breaks_is_found(self) -> None:
        texts = ['output /* moved */ "a" {\n', 'output\n"b" /* c */ {\n', '/* x */ output "c" {\n', 'output "d"\n{\n']
        self.assertEqual({"a", "b", "c", "d"}, root_output_names(texts))


if __name__ == "__main__":
    unittest.main()
