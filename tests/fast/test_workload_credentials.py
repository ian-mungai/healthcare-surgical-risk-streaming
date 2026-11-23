"""The host helper that renews each workload's short-lived AWS credentials.

Its calls to instance metadata and to STS are proven on the live host. These cases cover what a live run cannot
readily force: a file that is missing, malformed or close to expiry, the request signature against the worked example
in AWS's Signature Version 4 documentation and a response that carries no credentials.
"""

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from unittest import mock

HELPER = Path(__file__).resolve().parents[2] / "ansible" / "roles" / "workload_access" / "files" / "workload_credentials.py"
NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def load_helper() -> ModuleType:
    spec = importlib.util.spec_from_file_location("workload_credentials", HELPER)
    if spec is None or spec.loader is None:
        raise ImportError(str(HELPER))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


helper = load_helper()


def document(expires_in: timedelta) -> dict[str, object]:
    return {"Version": 1, "AccessKeyId": "A", "SecretAccessKey": "B", "SessionToken": "C", "Expiration": (NOW + expires_in).strftime("%Y-%m-%dT%H:%M:%SZ")}


class FolderTest(unittest.TestCase):
    def test_the_folder_of_a_workload_is_made_under_its_name_for_its_group_and_holds_the_credentials(self) -> None:
        """The compose service mounts ``/run/workload_access/orchestrator``, so the helper must make a folder of exactly the workload's name."""
        with tempfile.TemporaryDirectory() as scratch, mock.patch.object(helper, "TARGET", Path(scratch)), mock.patch.object(helper.os, "chown") as chown:
            helper.write_credentials("orchestrator", 20105, {"Version": 1})
            folder = Path(scratch) / "orchestrator"
            self.assertEqual(["credentials.json"], sorted(path.name for path in folder.iterdir()))
            self.assertEqual(0o750, folder.stat().st_mode & 0o777)
            self.assertEqual(20105, chown.call_args_list[0].args[2])


class RenewalTest(unittest.TestCase):
    def test_fresh_credentials_are_kept(self) -> None:
        self.assertFalse(helper.needs_renewal(document(timedelta(minutes=45)), NOW))

    def test_credentials_within_the_renewal_window_are_renewed(self) -> None:
        self.assertTrue(helper.needs_renewal(document(timedelta(minutes=29)), NOW))

    def test_expired_missing_and_malformed_credentials_are_renewed(self) -> None:
        self.assertTrue(helper.needs_renewal(document(timedelta(minutes=-1)), NOW))
        self.assertTrue(helper.needs_renewal(None, NOW))
        self.assertTrue(helper.needs_renewal({"Version": 1}, NOW))
        self.assertTrue(helper.needs_renewal({"Expiration": "soon"}, NOW))


class SignatureTest(unittest.TestCase):
    def test_signing_key_equals_the_documented_example(self) -> None:
        documented = "".join(["wJalrXUtnFEMI", "/K7MDENG+", "bPxRfiCY", "EXAMPLEKEY"])
        self.assertEqual(
            "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d", helper.signing_key(documented, "20120215", "us-east-1", "iam").hex()
        )

    def test_signed_request_names_the_scope_and_the_session_token(self) -> None:
        headers = helper.signed_headers(
            {"AccessKeyId": "AKIDEXAMPLE", "SecretAccessKey": "secret", "Token": "session"}, "us-west-2", "Action=GetCallerIdentity&Version=2011-06-15", NOW
        )
        self.assertEqual("session", headers["X-Amz-Security-Token"])
        self.assertEqual("20260101T120000Z", headers["X-Amz-Date"])
        scope = "Credential=AKIDEXAMPLE/20260101/us-west-2/sts/aws4_request"
        signed = "SignedHeaders=content-type;host;x-amz-date;x-amz-security-token"
        self.assertTrue(headers["Authorization"].startswith(f"AWS4-HMAC-SHA256 {scope}, {signed}, Signature="))


class ResponseTest(unittest.TestCase):
    def test_credentials_are_read_from_the_response(self) -> None:
        fields = "<AccessKeyId>A</AccessKeyId><SecretAccessKey>B</SecretAccessKey><SessionToken>C</SessionToken><Expiration>2026-01-01T13:00:00Z</Expiration>"
        body = f"<AssumeRoleResponse><Credentials>{fields}</Credentials></AssumeRoleResponse>"
        self.assertEqual(
            {"Version": 1, "AccessKeyId": "A", "SecretAccessKey": "B", "SessionToken": "C", "Expiration": "2026-01-01T13:00:00Z"},
            helper.credential_document(body),
        )

    def test_response_without_credentials_is_refused(self) -> None:
        with self.assertRaisesRegex(helper.CredentialError, "no credentials"):
            helper.credential_document("<ErrorResponse><Error><Code>AccessDenied</Code></Error></ErrorResponse>")


if __name__ == "__main__":
    unittest.main()
