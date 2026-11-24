"""The host helper that renews each workload's short-lived AWS credentials.

Its calls to instance metadata and to STS are proven on the live host. These cases cover what a live run cannot
readily force: a file that is missing, malformed or close to expiry, the request signature against the worked example
in AWS's Signature Version 4 documentation and a response that carries no credentials.
"""

from __future__ import annotations

import importlib.util
import io
import json
import os
import stat
import tempfile
import unittest
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock

from tests.fast.header_secret_support import raised_text, unsafe_values

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
        with (
            tempfile.TemporaryDirectory() as scratch,
            mock.patch.object(helper, "TARGET", Path(scratch)),
            mock.patch.object(helper.os, "chown") as chown,
            mock.patch.object(helper.os, "fchown") as fchown,
        ):
            helper.write_credentials("orchestrator", 20105, {"Version": 1})
            folder = Path(scratch) / "orchestrator"
            self.assertEqual(["credentials.json"], sorted(path.name for path in folder.iterdir()))
            self.assertEqual(0o750, folder.stat().st_mode & 0o777)
            self.assertEqual(20105, chown.call_args_list[0].args[2])
            self.assertEqual((0, 20105), fchown.call_args.args[1:])


class RenewalTest(unittest.TestCase):
    def test_fresh_credentials_are_kept(self) -> None:
        self.assertFalse(helper.needs_renewal(document(timedelta(minutes=45)), NOW))

    def test_credentials_within_the_renewal_window_are_renewed(self) -> None:
        self.assertTrue(helper.needs_renewal(document(timedelta(minutes=29)), NOW))

    def test_an_expiration_with_milliseconds_is_read_as_a_time_of_its_own(self) -> None:
        """AWS Query's time may hold milliseconds: ``.500Z`` is no malformed time, and it is not renewed at every run."""
        self.assertFalse(helper.needs_renewal({**document(timedelta(minutes=45)), "Expiration": "2026-01-01T12:45:00.500Z"}, NOW))
        self.assertTrue(helper.needs_renewal({**document(timedelta(minutes=29)), "Expiration": "2026-01-01T12:29:00.500Z"}, NOW))

    def test_an_expiration_that_is_no_date_is_renewed(self) -> None:
        for text in ("2026-02-31T13:00:00Z", "2026-01-01T25:00:00Z", "2026-01-01T13:00:00.Z", "2026-01-01T13:00:00+00:00", "2026-01-01 13:00:00Z"):
            with self.subTest(text=text):
                self.assertTrue(helper.needs_renewal({**document(timedelta(minutes=45)), "Expiration": text}, NOW))

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


class ExpirationTest(unittest.TestCase):
    """One parser reads the expiration for the response check and for the renewal, so the two cannot disagree on what a time is."""

    VALID = {
        "2026-01-01T13:00:00Z": datetime(2026, 1, 1, 13, 0, 0, tzinfo=UTC),
        "2026-01-01T13:00:00.500Z": datetime(2026, 1, 1, 13, 0, 0, 500000, tzinfo=UTC),
        "2026-01-01T13:00:00.5Z": datetime(2026, 1, 1, 13, 0, 0, 500000, tzinfo=UTC),
        "2028-02-29T00:00:00.000Z": datetime(2028, 2, 29, tzinfo=UTC),
    }
    INVALID = (
        "2026-02-31T13:00:00Z",
        "2026-02-29T13:00:00Z",
        "2026-04-31T13:00:00Z",
        "2026-13-01T13:00:00Z",
        "2026-01-01T24:00:00Z",
        "2026-01-01T13:60:00Z",
        "2026-01-01T13:00:00.Z",
        "2026-01-01T13:00:00.1234567890Z",
        "2026-01-01T13:00:00",
        "2026-01-01T13:00:00+00:00",
        "2026-01-01 13:00:00Z",
        "2026-01-01T13:00:00Z\n",
        "٢٠٢٦-01-01T13:00:00Z",
        "",
    )

    def test_a_time_with_or_without_milliseconds_is_read(self) -> None:
        for text, expected in self.VALID.items():
            with self.subTest(text=text):
                self.assertEqual(expected, helper.parse_expiration(text))

    def test_a_time_that_does_not_exist_or_has_another_form_is_refused(self) -> None:
        for text in self.INVALID:
            with self.subTest(text=text):
                self.assertIsNone(helper.parse_expiration(text))

    def test_the_response_check_and_the_renewal_agree_on_every_time(self) -> None:
        for text in [*self.VALID, *self.INVALID]:
            with self.subTest(text=text):
                body = f"<AssumeRoleResponse><Credentials>{''.join(f'<{name}>{value}</{name}>' for name, value in {**STS_FIELDS, 'Expiration': text}.items())}"
                if text in self.VALID:
                    self.assertEqual(text, helper.credential_document(body)["Expiration"])
                else:
                    self.assertRaises(helper.CredentialError, helper.credential_document, body)
                far = {"AccessKeyId": "A", "SecretAccessKey": "B", "SessionToken": "C", "Expiration": text}
                if text in self.VALID:
                    self.assertEqual(self.VALID[text] - NOW < helper.RENEW_BEFORE, helper.needs_renewal(far, NOW))
                else:
                    self.assertTrue(helper.needs_renewal(far, NOW))


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

    def assert_refused(self, message: str, call: Callable[[], object], raiser: str) -> None:
        """The call raises the fixed ``message``, its traceback (read inside the handler) names ``raiser`` and nothing of it holds the planted value."""
        error, text = raised_text(call, helper.CredentialError, raiser)
        self.assertEqual(message, str(error))
        self.assertNotIn(PLANTED_MARKER, text)

    def test_the_access_key_and_the_session_token_of_the_host_role_are_refused_in_any_unsafe_form_before_a_header_is_made(self) -> None:
        for field in ("AccessKeyId", "Token"):
            for name, value in unsafe_values(PLANTED_MARKER).items():
                with self.subTest(field=field, character=name):
                    self.assert_refused(
                        f"the host role's {field} is not a valid credential value",
                        partial(signed_with, field, value),
                        "header_value",
                    )

    def test_a_credential_of_the_host_role_that_is_not_text_or_is_empty_is_refused_the_same_way(self) -> None:
        for field, value in (("Token", 5), ("Token", None), ("Token", ""), ("AccessKeyId", ["x"])):
            with self.subTest(field=field, value=value):
                self.assert_refused(
                    f"the host role's {field} is not a valid credential value",
                    partial(signed_with, field, value),
                    "header_value",
                )

    def test_credentials_of_the_header_safe_form_are_signed(self) -> None:
        headers = helper.signed_headers(SAFE_CREDENTIALS, "us-west-2", "Action=GetCallerIdentity", NOW)
        self.assertEqual(SAFE_CREDENTIALS["Token"], headers["X-Amz-Security-Token"])
        self.assertIn(f"Credential={SAFE_CREDENTIALS['AccessKeyId']}/", headers["Authorization"])

    def test_the_session_token_of_instance_metadata_is_refused_before_it_is_sent_back(self) -> None:
        for name, value in unsafe_values(PLANTED_MARKER).items():
            with self.subTest(character=name), mock.patch.object(helper, "metadata", return_value=value) as metadata:
                self.assert_refused("instance metadata answered a session token that is not a valid token", helper.host_credentials, "host_credentials")
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
                # A ``<`` ends the field for the parser's own pattern, so the field is missing and the answer holds no credentials.
                message = "the STS response holds no credentials" if "<" in value else f"the STS response's {field} is not a valid credential value"
                with self.subTest(field=field, character=name):
                    self.assert_refused(message, partial(helper.credential_document, sts_body(**{field: value})), "credential_document")

    def test_an_expiration_that_is_not_a_time_is_refused_and_a_credential_of_the_safe_form_is_kept(self) -> None:
        self.assert_refused(
            "the STS response's Expiration is not a valid credential value",
            partial(helper.credential_document, sts_body(Expiration="soon")),
            "credential_document",
        )
        self.assertEqual({"Version": 1, **STS_FIELDS}, helper.credential_document(sts_body()))


class FakeConnection:
    """An HTTP or HTTPS connection that sends nothing: each connection answers with the next ``(status, body bytes)`` of ``answers``."""

    answers: list[tuple[int, bytes]] = []
    sent: list[tuple[str, str, dict[str, str]]] = []

    def __init__(self, host: str, timeout: float | None = None) -> None:
        pass

    def request(self, method: str, path: str, body: str | None = None, headers: dict[str, str] | None = None) -> None:
        self.sent.append((method, path, headers or {}))

    def getresponse(self) -> Any:
        status, body = self.answers.pop(0)
        return mock.Mock(status=status, read=lambda: body)

    def close(self) -> None:
        pass


def answering(*answers: tuple[int, bytes]) -> mock._patch[Any]:
    FakeConnection.answers = list(answers)
    FakeConnection.sent = []
    return mock.patch.multiple(helper.http.client, HTTPConnection=FakeConnection, HTTPSConnection=FakeConnection)


NOT_UTF8 = PLANTED_MARKER.encode() + b"\xff\xfe"
ROLE_DOCUMENT = json.dumps({**SAFE_CREDENTIALS, "Code": "Success"})


class AnswerTextTest(unittest.TestCase):
    """An answer that is not UTF-8 or not the JSON that it should be raises a fixed error after the parser's handler, with no part of the answer."""

    def refused(self, message: str, call: Callable[[], object], raiser: str) -> None:
        error, text = raised_text(call, helper.CredentialError, raiser)
        self.assertEqual(message, str(error))
        self.assertNotIn(PLANTED_MARKER, text)
        self.assertIsNone(error.__cause__)
        self.assertIsNone(error.__context__, "the error was raised inside the handler of the parser's error, whose arguments hold the answer")

    def test_a_session_token_a_role_name_and_a_role_document_that_are_not_utf_8_are_refused(self) -> None:
        for position in range(3):
            with self.subTest(answer=position):
                answers = [(200, b"AQAEA-_x~y+z/w=="), (200, b"role-name"), (200, ROLE_DOCUMENT.encode())]
                answers[position] = (200, NOT_UTF8)
                with answering(*answers):
                    self.refused("instance metadata answered text that is not UTF-8", helper.host_credentials, "decoded")

    def test_a_role_document_that_is_cut_short_or_is_no_object_is_refused(self) -> None:
        for what, document in {
            "cut short": ROLE_DOCUMENT[:40],
            "empty": "",
            "a list": json.dumps([PLANTED_MARKER]),
            "a text": json.dumps(PLANTED_MARKER),
            "a number": "5",
            "null": "null",
        }.items():
            with self.subTest(document=what), answering((200, b"AQAEA-_x~y+z/w=="), (200, b"role-name"), (200, document.encode())):
                self.refused("instance metadata answered a role document that is not a JSON object", helper.host_credentials, "role_document")

    def test_a_cut_short_role_document_that_holds_the_planted_value_does_not_reach_the_error(self) -> None:
        cut = '{"AccessKeyId": "' + PLANTED_MARKER + '", "Secret'
        with answering((200, b"AQAEA-_x~y+z/w=="), (200, b"role-name"), (200, cut.encode())):
            self.refused("instance metadata answered a role document that is not a JSON object", helper.host_credentials, "role_document")

    def test_a_role_document_that_is_a_json_object_is_returned_as_it_is(self) -> None:
        with answering((200, b"AQAEA-_x~y+z/w=="), (200, b"role-name\n"), (200, ROLE_DOCUMENT.encode())):
            self.assertEqual(json.loads(ROLE_DOCUMENT), helper.host_credentials())
        self.assertEqual("/latest/meta-data/iam/security-credentials/role-name", FakeConnection.sent[-1][1])

    def test_an_sts_answer_that_is_not_utf_8_is_refused_whatever_its_status(self) -> None:
        for status in (200, 403):
            with self.subTest(status=status), answering((status, NOT_UTF8)):
                self.refused(
                    "STS answered text that is not UTF-8",
                    partial(helper.assume_role, SAFE_CREDENTIALS, "us-west-2", "arn:aws:iam::<ACCOUNT>:role/r", "orchestrator", NOW),
                    "decoded",
                )

    def test_an_sts_answer_of_credentials_is_read(self) -> None:
        with answering((200, sts_body().encode())):
            got = helper.assume_role(SAFE_CREDENTIALS, "us-west-2", "arn:aws:iam::<ACCOUNT>:role/r", "orchestrator", NOW)
        self.assertEqual({"Version": 1, **STS_FIELDS}, got)

    def test_a_renewal_with_such_an_answer_fails_with_a_message_and_no_traceback_that_holds_the_answer(self) -> None:
        previous = os.umask(0o027)
        self.addCleanup(os.umask, previous)
        with tempfile.TemporaryDirectory() as scratch:
            configuration = Path(scratch) / "workloads.json"
            configuration.write_text(json.dumps({"region": "us-west-2", "workloads": [{"name": "orchestrator", "gid": 1, "role_arn": "arn"}]}))
            for position in range(3):
                answers = [(200, b"AQAEA-_x~y+z/w=="), (200, b"role-name"), (200, ROLE_DOCUMENT.encode())]
                answers[position] = (200, NOT_UTF8 if position < 2 else ROLE_DOCUMENT[:40].encode())
                with (
                    self.subTest(answer=position),
                    answering(*answers),
                    mock.patch.object(helper, "CONFIGURATION", configuration),
                    mock.patch.object(helper, "TARGET", Path(scratch) / "run"),
                    mock.patch.object(helper.sys, "stderr", io.StringIO()) as stderr,
                ):
                    self.assertEqual(1, helper.main())
                    self.assertIn("cannot read the host role's credentials", stderr.getvalue())
                    self.assertNotIn(PLANTED_MARKER, stderr.getvalue())


class WriteTest(unittest.TestCase):
    """The credentials go into a file that this run created, with its group and mode set on the open descriptor and never wider than that mode."""

    def written(self, scratch: str, name: str = "orchestrator") -> Path:
        return Path(scratch) / name

    def write(self, scratch: str, document: dict[str, Any] | None = None) -> None:
        with mock.patch.object(helper, "TARGET", Path(scratch)), mock.patch.object(helper.os, "chown"), mock.patch.object(helper.os, "fchown"):
            helper.write_credentials("orchestrator", os.getgid(), document or {"Version": 1})

    def test_the_file_has_the_mode_of_the_group_whatever_the_umask(self) -> None:
        for umask in (0o000, 0o027, 0o077):
            with self.subTest(umask=oct(umask)), tempfile.TemporaryDirectory() as scratch:
                previous = os.umask(umask)
                self.addCleanup(os.umask, previous)
                self.write(scratch)
                os.umask(previous)
                self.assertEqual(0o640, stat.S_IMODE((self.written(scratch) / "credentials.json").stat().st_mode))

    def test_the_partial_file_is_created_exclusively_with_the_mode_and_its_group_is_set_on_the_descriptor(self) -> None:
        calls: list[tuple[str, int, int]] = []
        real_open = os.open

        def recording_open(path: Any, flags: int, mode: int = 0o777) -> int:
            calls.append((Path(path).name, flags, mode))
            return real_open(path, flags, mode)

        with (
            tempfile.TemporaryDirectory() as scratch,
            mock.patch.object(helper, "TARGET", Path(scratch)),
            mock.patch.object(helper.os, "chown") as chown,
            mock.patch.object(helper.os, "fchown") as fchown,
            mock.patch.object(helper.os, "open", recording_open),
        ):
            helper.write_credentials("orchestrator", 20105, {"Version": 1})
        ((name, flags, mode),) = calls
        self.assertEqual("credentials.json.partial", name)
        self.assertEqual(os.O_CREAT | os.O_EXCL | os.O_WRONLY, flags & (os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_RDWR | os.O_TRUNC | os.O_APPEND))
        self.assertEqual(0o640, mode)
        self.assertEqual((0, 20105), fchown.call_args.args[1:])
        self.assertEqual(1, chown.call_count, "only the folder is given to the group by its path")

    def test_a_partial_file_that_is_a_link_is_replaced_and_the_file_that_it_points_to_is_not_written(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            elsewhere = Path(scratch) / "elsewhere"
            elsewhere.write_text("kept")
            folder = self.written(scratch)
            folder.mkdir(mode=0o750)
            (folder / "credentials.json.partial").symlink_to(elsewhere)
            self.write(scratch, {"Version": 1})
            self.assertEqual("kept", elsewhere.read_text())
            self.assertEqual({"Version": 1}, json.loads((folder / "credentials.json").read_text()))
            self.assertEqual(["credentials.json"], sorted(path.name for path in folder.iterdir()))
            self.assertFalse((folder / "credentials.json").is_symlink())

    def test_a_partial_file_that_an_earlier_run_left_is_replaced_and_no_old_text_is_kept(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = self.written(scratch)
            folder.mkdir(mode=0o750)
            (folder / "credentials.json.partial").write_text("old text that is longer than the document " * 5)
            self.write(scratch, {"Version": 1})
            self.assertEqual('{"Version": 1}', (folder / "credentials.json").read_text())

    def test_a_failed_write_leaves_no_partial_file_and_keeps_the_credentials_that_were_there(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = self.written(scratch)
            folder.mkdir(mode=0o750)
            (folder / "credentials.json").write_text('{"Version": 1}')
            with (
                mock.patch.object(helper, "TARGET", Path(scratch)),
                mock.patch.object(helper.os, "chown"),
                mock.patch.object(helper.os, "fchown", side_effect=PermissionError("not allowed")),
                self.assertRaises(PermissionError),
            ):
                helper.write_credentials("orchestrator", os.getgid(), {"Version": 2})
            self.assertEqual(["credentials.json"], sorted(path.name for path in folder.iterdir()))
            self.assertEqual('{"Version": 1}', (folder / "credentials.json").read_text())


if __name__ == "__main__":
    unittest.main()
