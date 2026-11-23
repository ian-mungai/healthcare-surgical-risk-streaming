"""``make evidence-verify`` removes the earlier receipt and signature before anything that can stop it, once it has resolved the run.

Failure modes covered here:

- A malformed or missing ``.env`` stops the command before the removal, so the old receipt and signature stay valid.
- Any other exit between resolving the run and the removal, such as a configuration read that raises, leaves them.
- A missing export, a missing verifier key and a malformed manifest leave them.

The run cannot be resolved when the binding is missing or unreadable, and no earlier receipt can be named then.
"""

from __future__ import annotations

import contextlib
import io
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.evidence import verify
from scripts.infra.state_bootstrap import RunBinding
from scripts.project_config import ConfigurationError, load_configuration

RUN_ID = "20260101t0000-abcdef"
GOOD_ENV = (
    f"AWS_REGION=us-west-2\nAWS_PROFILE=example_project_dev\nAWS_ACCOUNT_ID={'1' * 12}\nPROJECT_NAME=example_project\nENVIRONMENT=dev\n"
    "S3_BUCKET=example-project\n"
)


class ReceiptInvalidationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.binding = self.root / "run_binding.json"
        RunBinding(RUN_ID, "0" * 40, "s3", "us-west-2", "example-project", "default", ["terraform.tfstate"], "verified", True).write(self.binding)
        self.export = self.root / "runs" / RUN_ID / "export"
        self.export.mkdir(parents=True)
        self.receipt = self.export / "receipt.json"
        self.signature = self.export / "receipt.json.sig"
        self.receipt.write_text('{"old": "receipt"}\n')
        self.signature.write_text("old signature\n")
        self.env_file = self.root / ".env"

    def run_main(self, *, configuration: object | None = None) -> tuple[int, str]:
        read = configuration if configuration is not None else (lambda: load_configuration(self.env_file))
        error = io.StringIO()
        with (
            mock.patch.object(verify, "ROOT", self.root),
            mock.patch.object(verify, "load_configuration", read),
            mock.patch.object(sys, "argv", ["verify", "--binding", str(self.binding)]),
            contextlib.redirect_stderr(error),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            code = verify.main()
        return code, error.getvalue()

    def assert_removed(self) -> None:
        self.assertFalse(self.receipt.exists())
        self.assertFalse(self.signature.exists())

    def test_a_malformed_configuration_removes_the_earlier_receipt_and_signature(self) -> None:
        self.env_file.write_text(GOOD_ENV.replace("us-west-2", "not a region"))
        code, error = self.run_main()
        self.assertEqual(1, code)
        self.assertIn("ConfigurationError", error)
        self.assert_removed()

    def test_a_missing_configuration_file_removes_the_earlier_receipt_and_signature(self) -> None:
        code, error = self.run_main()
        self.assertEqual(1, code)
        self.assertIn("ConfigurationError", error)
        self.assert_removed()

    def test_the_earlier_receipt_is_already_gone_when_the_configuration_is_read(self) -> None:
        seen: list[bool] = []

        def read() -> dict[str, str]:
            seen.append(self.receipt.exists() or self.signature.exists())
            raise ConfigurationError("unreadable")

        code, _ = self.run_main(configuration=read)
        self.assertEqual(1, code)
        self.assertEqual([False], seen)
        self.assert_removed()

    def test_a_missing_export_removes_the_earlier_receipt_and_signature(self) -> None:
        self.env_file.write_text(GOOD_ENV)
        code, error = self.run_main()
        self.assertEqual(1, code)
        self.assertIn("no export to verify", error)
        self.assert_removed()

    def test_a_missing_verifier_key_removes_the_earlier_receipt_and_signature(self) -> None:
        self.env_file.write_text(GOOD_ENV)
        (self.export / "manifest.json").write_text("{}")
        (self.export / "evidence.tar.gz").write_bytes(b"")
        code, error = self.run_main()
        self.assertEqual(1, code)
        self.assertIn("EVIDENCE_VERIFIER_KEY", error)
        self.assert_removed()

    def test_a_binding_that_cannot_be_read_names_no_run_and_stops(self) -> None:
        self.binding.unlink()
        code, error = self.run_main()
        self.assertEqual(1, code)
        self.assertIn("WrapperError", error)


if __name__ == "__main__":
    unittest.main()
