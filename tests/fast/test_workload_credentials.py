"""The host helper that renews each workload's short-lived AWS credentials.

Its calls to instance metadata and to STS are proven on the live host. These cases cover what a live run cannot
readily force: a file that is missing, malformed or close to expiry, the request signature against the worked example
in AWS's Signature Version 4 documentation and a response that carries no credentials.
"""

from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from types import ModuleType
from unittest import mock

from tests.fast.header_secret_support import everything_said_about, unsafe_values

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


PLANTED_MARKER = "planted-credential-value"
SAFE_CREDENTIALS = {"AccessKeyId": "AKIDEXAMPLE", "SecretAccessKey": "secret/key+x", "Token": "IQoJb3JpZ2luX2VjE-_~+/w=="}
STS_FIELDS = {
    "AccessKeyId": "ASIAEXAMPLE",
    "SecretAccessKey": "".join(["wJalr", "/K7MDENG", "+bPxRfiCY"]),
    "SessionToken": "".join(["IQoJb3Jp", "/Z2luX2Vj", "+Ez=="]),
    "Expiration": "2026-01-01T13:00:00Z",
}


def signed_with(field: str, value: object) -> dict[str, str]:
    """The signed headers of the safe credentials with ``field`` set to ``value``, which may not be text."""
    headers: dict[str, str] = helper.signed_headers({**SAFE_CREDENTIALS, field: value}, "us-west-2", "Action=GetCallerIdentity", NOW)
    return headers


def sts_body(**changes: str) -> str:
    fields = "".join(f"<{name}>{value}</{name}>" for name, value in (STS_FIELDS | changes).items())
    return f"<AssumeRoleResponse><AssumeRoleResult><Credentials>{fields}</Credentials></AssumeRoleResult></AssumeRoleResponse>"


class HeaderValueTest(unittest.TestCase):
    """A credential value goes into a header only when it is of the closed header-safe form, and no error holds a refused value."""

    def assert_refused(self, message: str, call: Callable[[], object]) -> None:
        with self.assertRaises(helper.CredentialError) as raised:
            call()
        self.assertEqual(message, str(raised.exception))
        self.assertNotIn(PLANTED_MARKER, everything_said_about(raised.exception))

    def test_the_access_key_and_the_session_token_of_the_host_role_are_refused_in_any_unsafe_form_before_a_header_is_made(self) -> None:
        for field in ("AccessKeyId", "Token"):
            for name, value in unsafe_values(PLANTED_MARKER).items():
                with self.subTest(field=field, character=name):
                    self.assert_refused(
                        f"the host role's {field} is not a valid credential value",
                        partial(signed_with, field, value),
                    )

    def test_a_credential_of_the_host_role_that_is_not_text_or_is_empty_is_refused_the_same_way(self) -> None:
        for field, value in (("Token", 5), ("Token", None), ("Token", ""), ("AccessKeyId", ["x"])):
            with self.subTest(field=field, value=value):
                self.assert_refused(
                    f"the host role's {field} is not a valid credential value",
                    partial(signed_with, field, value),
                )

    def test_credentials_of_the_header_safe_form_are_signed(self) -> None:
        headers = helper.signed_headers(SAFE_CREDENTIALS, "us-west-2", "Action=GetCallerIdentity", NOW)
        self.assertEqual(SAFE_CREDENTIALS["Token"], headers["X-Amz-Security-Token"])
        self.assertIn(f"Credential={SAFE_CREDENTIALS['AccessKeyId']}/", headers["Authorization"])

    def test_the_session_token_of_instance_metadata_is_refused_before_it_is_sent_back(self) -> None:
        for name, value in unsafe_values(PLANTED_MARKER).items():
            with self.subTest(character=name), mock.patch.object(helper, "metadata", return_value=value) as metadata:
                self.assert_refused("instance metadata answered a session token that is not a valid token", helper.host_credentials)
                self.assertEqual(1, metadata.call_count)

    def test_a_session_token_of_instance_metadata_of_the_safe_form_is_sent_back_as_it_is(self) -> None:
        answers = iter(["AQAEA-_x~y+z/w==", "role-name\n", json.dumps(SAFE_CREDENTIALS)])
        sent: list[dict[str, str]] = []

        def metadata(method: str, path: str, headers: dict[str, str]) -> str:
            sent.append(headers)
            return next(answers)

        with mock.patch.object(helper, "metadata", metadata):
            self.assertEqual(SAFE_CREDENTIALS, helper.host_credentials())
        self.assertEqual([{"X-aws-ec2-metadata-token": "AQAEA-_x~y+z/w=="}] * 2, sent[1:])

    def test_a_value_of_the_sts_response_with_a_character_a_header_cannot_hold_is_refused_and_no_error_holds_it(self) -> None:
        for field in ("AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration"):
            for name, value in unsafe_values(PLANTED_MARKER).items():
                with self.subTest(field=field, character=name):
                    self.assert_refused(
                        f"the STS response's {field} is not a valid credential value",
                        partial(helper.credential_document, sts_body(**{field: value})),
                    )

    def test_an_expiration_that_is_not_a_time_is_refused_and_a_credential_of_the_safe_form_is_kept(self) -> None:
        self.assert_refused("the STS response's Expiration is not a valid credential value", partial(helper.credential_document, sts_body(Expiration="soon")))
        self.assertEqual({"Version": 1, **STS_FIELDS}, helper.credential_document(sts_body()))


if __name__ == "__main__":
    unittest.main()
