"""No root Terraform output holds an address that the host gets anew on every start, read from Terraform's own plan and never from the source text.

The host has no Elastic IP, so AWS gives its instance a new public address and a new public DNS name on every ``make host-start``. The plan review gate counts
each output change on purpose: the deploy reads ``heavy_memory_mib`` from state, and an output that is not applied must never reach the host. An output that
holds one of these changing values therefore turns every start into a plan change, and ``make host-up`` stops for an owner review each time.

The offline Terraform test ``infra/tests/changing_address.tftest.hcl`` gives the instance a marker address and a marker DNS name and plans. This test runs
that file with ``terraform test -json -verbose`` on a copy of ``infra/`` and reads the ``test_plan`` message. The decision comes only from that plan: the
planned value of every root output, sensitive ones included, must hold neither marker, and the same plan must show both markers on the instance. A
heredoc, a comment, an override file, a local, an alias, a splat, a ``for`` projection or a whole-instance passthrough cannot hide an output from Terraform's
own plan, and a new output needs no listing anywhere. A module output reaches the outside only through a root output, so the root outputs cover them.

Accepted limit: a value derived from the address, such as a hash of it, holds no marker and is not caught. An output that the plan cannot know (computed at
apply) is a failure too: give its resource a fixed value in the test file's ``override_resource`` blocks.

Failure modes covered here, each planted in a copy of the real tree:

- An output declared after a heredoc that holds ``artifacts/*`` and before a block comment, in a file of any name.
- A block-commented assertion in the Terraform test, with the marker in an output.
- An output that exists only in an ``*_override.tf`` file and overrides an existing one.
- A sensitive output, and an output that passes the whole instance on.
- The marker not planted on the instance, an output that is unknown in the plan, no plan at all, and a plan with no outputs.
"""

from __future__ import annotations

import contextlib
import json
import tempfile
import unittest
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

from tests.slow.terraform_support import Terraform

TEST_SECONDS = 300
TEST_FILE = "tests/changing_address.tftest.hcl"
INSTANCE = "module.host[0].aws_instance.host"
MARKER_IP = "198.51.100.77"
MARKER_DNS = "marker-ip.invalid"

LEAK_MODULE_OUTPUTS = 'output "public_ip" {\n  value = aws_instance.host.public_ip\n}\n\noutput "instance" {\n  value = aws_instance.host\n}\n'
HEREDOC_THEN_COMMENT = (
    'locals {\n  note = <<-EOT\n    see artifacts/*\n  EOT\n}\n\noutput "leak" {\n  value = module.host[0].public_ip\n}\n\n/* a later block comment */\n'
)
COMMENTED_ASSERTION = (
    '\n/*\nrun "later" {\n  command = plan\n  assert {\n    condition     = !strcontains(jsonencode(output.leak), "marker-ip.invalid")\n'
    '    error_message = "leak"\n  }\n}\n*/\n'
)
OVERRIDE_OF_AN_EXISTING_OUTPUT = 'output "heavy_memory_mib" {\n  value = module.host[0].public_ip\n}\n'
SENSITIVE_LEAK = 'output "leak_sensitive" {\n  value     = module.host[0].public_ip\n  sensitive = true\n}\n'
WHOLE_INSTANCE = 'output "leak_instance" {\n  value = module.host[0].instance\n}\n'
SECURITY_GROUP_OVERRIDE = (
    "  override_resource {\n    target          = module.host[0].aws_security_group.host\n    override_during = plan\n    values = {\n"
    '      id = "sg-0123456789abcdef0"\n    }\n  }\n\n'
)


def contains_unknown(value: Any) -> bool:
    """``after_unknown`` is true, or a structure with a true value inside."""
    if isinstance(value, dict):
        return any(contains_unknown(item) for item in value.values())
    if isinstance(value, list):
        return any(contains_unknown(item) for item in value)
    return value is True


def address_problems(stream: str) -> list[str]:
    """What the ``test_plan`` message of ``terraform test -json -verbose`` for the changing-address test says is wrong. An empty list is a pass."""
    plans: list[dict[str, Any]] = []
    for line in stream.splitlines():
        try:
            message = json.loads(line)
        except ValueError:
            continue
        if isinstance(message, dict) and message.get("type") == "test_plan" and message.get("@testfile") == TEST_FILE:
            plans.append(message["test_plan"])
    if len(plans) != 1:
        return [f"expected one plan of {TEST_FILE} in the JSON stream, found {len(plans)}"]
    plan = plans[0]
    problems: list[str] = []
    instance = [change for change in plan.get("resource_changes", []) if change.get("address") == INSTANCE]
    after = instance[0]["change"].get("after") or {} if instance else {}
    if after.get("public_ip") != MARKER_IP or after.get("public_dns") != MARKER_DNS:
        problems.append(f"the plan does not hold both markers on {INSTANCE}, so the outputs were not tested against a changing address")
    outputs = plan.get("output_changes") or {}
    if not outputs:
        problems.append("the plan holds no root output")
    for name, change in sorted(outputs.items()):
        if contains_unknown(change.get("after_unknown")):
            problems.append(f"the root output {name} is unknown in the plan and cannot be checked")
        text = json.dumps(change.get("after"))
        problems.extend(
            f"the root output {name} holds the {kind}" for kind, marker in (("public address", MARKER_IP), ("public DNS name", MARKER_DNS)) if marker in text
        )
    return problems


@contextlib.contextmanager
def planted(config: Path, files: Mapping[str, str | None]) -> Iterator[None]:
    """Write each file of the copy (``None`` removes it) and put every one back as it was."""
    saved = {name: ((config / name).read_text() if (config / name).exists() else None) for name in files}
    try:
        for name, text in files.items():
            path = config / name
            if text is None:
                path.unlink(missing_ok=True)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text)
        yield
    finally:
        for name, text in saved.items():
            path = config / name
            if text is None:
                path.unlink(missing_ok=True)
            else:
                path.write_text(text)


class HostOutputPlanTest(unittest.TestCase):
    terraform: Terraform
    directory: tempfile.TemporaryDirectory[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.directory = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.directory.cleanup)
        cls.terraform = Terraform(Path(cls.directory.name))
        cls.terraform.init()

    def problems(self, files: Mapping[str, str | None] | None = None) -> list[str]:
        with planted(self.terraform.config, files or {}):
            result = self.terraform.run("test", "-json", "-verbose", f"-filter={TEST_FILE}", seconds=TEST_SECONDS)
        return address_problems(result.stdout)

    def read(self, name: str) -> str:
        return (self.terraform.config / name).read_text()

    def test_the_real_tree_holds_no_marker_in_any_root_output(self) -> None:
        self.assertEqual([], self.problems())

    def test_an_output_after_a_heredoc_with_a_glob_and_before_a_block_comment_is_caught(self) -> None:
        problems = self.problems({"zz_leak.tf": HEREDOC_THEN_COMMENT, "modules/host/zz_leak.tf": LEAK_MODULE_OUTPUTS})
        self.assertIn("the root output leak holds the public address", problems)

    def test_a_block_commented_assertion_does_not_count_as_coverage(self) -> None:
        files = {"zz_leak.tf": HEREDOC_THEN_COMMENT, "modules/host/zz_leak.tf": LEAK_MODULE_OUTPUTS, TEST_FILE: self.read(TEST_FILE) + COMMENTED_ASSERTION}
        self.assertIn("the root output leak holds the public address", self.problems(files))

    def test_an_output_only_in_an_override_file_that_overrides_an_existing_one_is_caught(self) -> None:
        problems = self.problems({"outputs_override.tf": OVERRIDE_OF_AN_EXISTING_OUTPUT, "modules/host/zz_leak.tf": LEAK_MODULE_OUTPUTS})
        self.assertIn("the root output heavy_memory_mib holds the public address", problems)

    def test_a_sensitive_output_is_checked(self) -> None:
        problems = self.problems({"zz_leak.tf": SENSITIVE_LEAK, "modules/host/zz_leak.tf": LEAK_MODULE_OUTPUTS})
        self.assertIn("the root output leak_sensitive holds the public address", problems)

    def test_an_output_that_passes_the_whole_instance_on_is_checked(self) -> None:
        problems = self.problems({"zz_leak.tf": WHOLE_INSTANCE, "modules/host/zz_leak.tf": LEAK_MODULE_OUTPUTS})
        self.assertIn("the root output leak_instance holds the public address", problems)
        self.assertIn("the root output leak_instance holds the public DNS name", problems)

    def test_a_plan_without_the_markers_on_the_instance_fails(self) -> None:
        text = self.read(TEST_FILE).replace(f'"{MARKER_IP}"', '"203.0.113.9"')
        self.assertTrue(any("does not hold both markers" in problem for problem in self.problems({TEST_FILE: text})))

    def test_an_output_that_is_unknown_in_the_plan_fails(self) -> None:
        text = self.read(TEST_FILE).replace(SECURITY_GROUP_OVERRIDE, "")
        self.assertNotEqual(text, self.read(TEST_FILE), "control: the override block was found")
        self.assertIn("the root output host_security_group_id is unknown in the plan and cannot be checked", self.problems({TEST_FILE: text}))

    def test_a_missing_terraform_test_file_fails(self) -> None:
        self.assertEqual([f"expected one plan of {TEST_FILE} in the JSON stream, found 0"], self.problems({TEST_FILE: None}))

    def test_a_plan_with_no_root_output_fails(self) -> None:
        self.assertIn("the plan holds no root output", self.problems({"outputs.tf": ""}))


if __name__ == "__main__":
    unittest.main()
