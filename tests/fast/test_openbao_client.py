"""The small OpenBao client of the AIStor step: it reads and writes the project user's sign-in with fixed diagnostics, a safe token form and no body echoed.

Every request of this client carries a secret (a secret ID, a token or the project user's password), so no failure may hold any part of the answer: an answer
can repeat what it was sent. The cases plant a marker in the answer's body and in the text of the transport error, and no raised text may hold it.
"""

from __future__ import annotations

import json
import unittest
from typing import Any
from unittest import mock

from tests.fast.ansible_paths import shared_module
from tests.fast.header_secret_support import raised_text

client_module: Any = shared_module("openbao_client")
MARKER = "planted-marker-0123456789"
ISSUED = "hvs.CAESIJ-_x~y+z/w=="


class FakeConnection:
    """An HTTP connection that sends nothing: it keeps each request and answers each one with the status and body that the case set."""

    requests: list[tuple[str, str, str | None, dict[str, str]]] = []
    answers: list[tuple[int, bytes]] = []
    error: Exception | None = None

    def __init__(self, host: str, port: int, timeout: float) -> None:
        self.host, self.port, self.timeout = host, port, timeout

    def request(self, method: str, path: str, body: str | None, headers: dict[str, str]) -> None:
        self.requests.append((method, path, body, headers))
        if self.error is not None:
            raise self.error

    def getresponse(self) -> Any:
        status, body = self.answers.pop(0)
        return mock.Mock(status=status, read=lambda: body)

    def close(self) -> None:
        pass


class ClientTest(unittest.TestCase):
    def setUp(self) -> None:
        FakeConnection.requests, FakeConnection.answers, FakeConnection.error = [], [], None
        patcher = mock.patch.object(client_module.http.client, "HTTPConnection", FakeConnection)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.client = client_module.OpenBaoClient("http://127.0.0.1:8200")

    def answer(self, status: int, document: Any = None, raw: bytes | None = None) -> None:
        FakeConnection.answers.append((status, raw if raw is not None else json.dumps(document if document is not None else {}).encode()))

    def raised(self, call: Any) -> str:
        with self.assertRaises(client_module.OpenBaoError) as caught:
            call()
        return str(caught.exception)

    def test_the_address_must_be_plain_http(self) -> None:
        for address in ("https://127.0.0.1:8200", "127.0.0.1:8200", "http://", ""):
            with self.subTest(address=address), self.assertRaises(client_module.OpenBaoError):
                client_module.OpenBaoClient(address)

    def test_a_login_sends_the_role_and_the_secret_id_and_gives_the_token(self) -> None:
        self.answer(200, {"auth": {"client_token": ISSUED}})
        self.assertEqual(ISSUED, self.client.login("bootstrap", "a-secret-id"))
        method, path, body, headers = FakeConnection.requests[0]
        self.assertEqual(
            ("POST", "/v1/auth/approle/login", {"role_id": "bootstrap", "secret_id": "a-secret-id"}, {}), (method, path, json.loads(body or ""), headers)
        )

    def test_a_refused_login_is_a_fixed_message_with_the_status_and_no_part_of_the_answer(self) -> None:
        self.answer(400, {"errors": [f"invalid secret id {MARKER}"]})
        text = self.raised(lambda: self.client.login("bootstrap", MARKER))
        self.assertEqual("OpenBao refused the login of the role bootstrap (HTTP 400)", text)

    def test_a_login_answer_without_a_usable_token_is_refused_and_no_error_holds_the_answer(self) -> None:
        for token in (None, "", 5, True, ["x"], f"bad token {MARKER}\nsecond line", f"{MARKER} space"):
            with self.subTest(token=token):
                FakeConnection.answers = []
                self.answer(200, {"auth": {"client_token": token}})
                text = self.raised(lambda: self.client.login("bootstrap", "a-secret-id"))
                self.assertNotIn(MARKER, text)

    def test_a_read_gives_the_secrets_data_and_sends_the_token_in_the_header_only(self) -> None:
        self.answer(200, {"data": {"data": {"user": "project_user", "password": "x"}, "metadata": {}}})
        self.assertEqual({"user": "project_user", "password": "x"}, self.client.read(ISSUED, "secret/data/services/aistor/aistor"))
        method, path, body, headers = FakeConnection.requests[0]
        self.assertEqual(("GET", "/v1/secret/data/services/aistor/aistor", None, {"X-Vault-Token": ISSUED}), (method, path, body, headers))

    def test_only_a_404_is_an_absent_secret_and_every_other_status_fails_with_a_fixed_message(self) -> None:
        self.answer(404, {"errors": []})
        self.assertIsNone(self.client.read(ISSUED, "secret/data/services/aistor/aistor"))
        for status in (400, 403, 500, 503):
            with self.subTest(status=status):
                self.answer(status, {"errors": [f"permission denied {MARKER}"]})
                text = self.raised(lambda: self.client.read(ISSUED, "secret/data/services/aistor/aistor"))
                self.assertEqual(f"OpenBao refused the read of a secret (HTTP {status})", text)

    def test_an_answer_to_a_read_that_is_not_a_secret_is_refused_and_not_echoed(self) -> None:
        for raw in (b"", b"not json " + MARKER.encode(), b"[]", json.dumps({"data": {"data": MARKER}}).encode(), json.dumps({"data": MARKER}).encode()):
            with self.subTest(raw=raw[:12]):
                self.answer(200, raw=raw)
                text = self.raised(lambda: self.client.read(ISSUED, "secret/data/services/aistor/aistor"))
                self.assertNotIn(MARKER, text)

    def test_a_write_sends_the_data_in_the_body_and_a_refusal_is_a_fixed_message_with_no_part_of_the_answer(self) -> None:
        self.answer(200, {})
        self.client.write(ISSUED, "secret/data/services/aistor/aistor", {"user": "project_user", "password": MARKER})
        method, path, body, headers = FakeConnection.requests[0]
        self.assertEqual(
            ("POST", "/v1/secret/data/services/aistor/aistor", {"data": {"user": "project_user", "password": MARKER}}), (method, path, json.loads(body or ""))
        )
        self.assertEqual({"X-Vault-Token": ISSUED}, headers)
        for status in (204,):
            self.answer(status)
            self.client.write(ISSUED, "secret/data/services/aistor/aistor", {"user": "u", "password": "p"})
        for status in (400, 403, 500):
            with self.subTest(status=status):
                self.answer(status, {"errors": [f"cannot store {MARKER}"]})
                text = self.raised(lambda: self.client.write(ISSUED, "secret/data/services/aistor/aistor", {"user": "u", "password": MARKER}))
                self.assertEqual(f"OpenBao refused the write of a secret (HTTP {status})", text)

    def test_a_transport_error_is_a_fixed_message_and_nothing_of_the_librarys_text_is_chained_to_it(self) -> None:
        for error in (OSError(f"connection to {MARKER} failed"), client_module.http.client.HTTPException(MARKER)):
            with self.subTest(error=type(error).__name__):
                FakeConnection.error = error
                for name, call in (
                    ("login", lambda: self.client.login("bootstrap", MARKER)),
                    ("read", lambda: self.client.read(ISSUED, "secret/data/x")),
                    ("write", lambda: self.client.write(ISSUED, "secret/data/x", {"password": MARKER})),
                ):
                    caught, text = raised_text(call, client_module.OpenBaoError, name)
                    self.assertNotIn(MARKER, text, "the whole traceback with its chained exceptions")
                    self.assertIn("gave no answer", str(caught))

    def test_a_token_of_a_form_that_a_header_cannot_hold_is_refused_before_any_request(self) -> None:
        for token in (f"{MARKER}\r\nX-Other: 1", f"{MARKER} x", "", "a\x00b"):
            with self.subTest(token=token[:8]):
                for call in (lambda t=token: self.client.read(t, "secret/data/x"), lambda t=token: self.client.write(t, "secret/data/x", {"a": "b"})):
                    text = self.raised(call)
                    self.assertNotIn(MARKER, text)
        self.assertEqual([], FakeConnection.requests)


if __name__ == "__main__":
    unittest.main()
