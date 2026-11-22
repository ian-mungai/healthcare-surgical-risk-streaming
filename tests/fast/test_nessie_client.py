"""The Nessie client reaches a real HTTP server, ends every call at its deadline and names what Nessie refused.

A small server on the loopback address stands in for Nessie only to produce the two failures that a healthy Nessie does not give on demand:
an answer that never comes and a refusal. The calls themselves are proven against a real Nessie by the acceptance test of the Nessie service.
"""

from __future__ import annotations

import json
import threading
import time
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from scripts.stack.nessie import NessieClient, NessieConflict, NessieError, Reference
from scripts.stack.warehouse import WarehouseError, warehouse_location

BRANCH = {"type": "BRANCH", "name": "main", "hash": "a" * 64}


def handler_for(status: int, body: dict[str, Any] | None, delay_seconds: float = 0.0, methods: tuple[str, ...] = ("GET", "POST", "DELETE")) -> Any:
    class Handler(BaseHTTPRequestHandler):
        seen: list[tuple[str, str]] = []

        def respond(self) -> None:
            Handler.seen.append((self.command, self.path))
            length = int(self.headers.get("Content-Length") or 0)
            self.rfile.read(length)
            time.sleep(delay_seconds)
            answered, document = (status, body) if self.command in methods else (200, {"reference": BRANCH})
            payload = json.dumps(document or {}).encode()
            try:
                self.send_response(answered)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            except OSError:
                return

        do_GET = do_POST = do_DELETE = respond

        def log_message(self, format: str, *args: Any) -> None:
            return

    return Handler


@contextmanager
def serving(
    status: int, body: dict[str, Any] | None, delay_seconds: float = 0.0, methods: tuple[str, ...] = ("GET", "POST", "DELETE")
) -> Iterator[tuple[str, Any]]:
    handler = handler_for(status, body, delay_seconds, methods)
    server = HTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/api/v2", handler
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


class ClientTest(unittest.TestCase):
    def test_a_reference_is_read_with_its_type_name_and_hash(self) -> None:
        with serving(200, {"reference": BRANCH}) as (url, handler):
            reference = NessieClient(url, timeout_seconds=5).reference("main")
        self.assertEqual(Reference("BRANCH", "main", "a" * 64), reference)
        self.assertEqual([("GET", "/api/v2/trees/main")], handler.seen)

    def test_an_answer_that_does_not_come_ends_at_the_deadline_with_the_call_named(self) -> None:
        with serving(200, {"reference": BRANCH}, delay_seconds=3) as (url, _handler):
            started = time.monotonic()
            with self.assertRaises(NessieError) as caught:
                NessieClient(url, timeout_seconds=0.5).reference("main")
            elapsed = time.monotonic() - started
        self.assertLess(elapsed, 2.5)
        self.assertIn("GET", str(caught.exception))
        self.assertIn("0.5 seconds", str(caught.exception))

    def test_a_server_that_is_not_listening_is_an_error_not_a_hang(self) -> None:
        with serving(200, None) as (url, _handler):
            pass
        with self.assertRaises(NessieError):
            NessieClient(url, timeout_seconds=2).reference("main")

    def test_a_refusal_for_a_stale_hash_is_a_conflict_that_carries_nessies_error_code(self) -> None:
        refusal = {"status": 409, "reason": "Conflict", "message": "Expected hash does not match", "errorCode": "REFERENCE_CONFLICT"}
        with serving(409, refusal, methods=("POST",)) as (url, _handler), self.assertRaises(NessieConflict) as caught:
            NessieClient(url, timeout_seconds=5).merge("feature", "main", "b" * 64, "merge", source_hash="a" * 64)
        self.assertEqual("REFERENCE_CONFLICT", caught.exception.error_code)

    def test_any_other_refusal_is_an_error_that_is_not_a_conflict(self) -> None:
        refusal = {"status": 404, "message": "not found", "errorCode": "REFERENCE_NOT_FOUND"}
        with serving(404, refusal) as (url, _handler), self.assertRaises(NessieError) as caught:
            NessieClient(url, timeout_seconds=5).reference("missing")
        self.assertNotIsInstance(caught.exception, NessieConflict)
        self.assertIn("REFERENCE_NOT_FOUND", str(caught.exception))

    def test_an_address_that_is_not_a_plain_http_url_is_refused(self) -> None:
        for url in ("file:///etc/passwd", "ftp://127.0.0.1/api/v2", "http:///api/v2", "127.0.0.1:19120/api/v2"):
            with self.subTest(url=url), self.assertRaises(NessieError):
                NessieClient(url, timeout_seconds=1)


class WarehouseTest(unittest.TestCase):
    def test_the_location_is_the_projects_bucket_with_the_warehouse_prefix(self) -> None:
        self.assertEqual("s3://example-project-bucket/warehouse/", warehouse_location("example-project-bucket"))

    def test_a_bucket_that_s3_would_refuse_is_refused_with_the_value_not_echoed_as_a_path(self) -> None:
        for bucket in ("", "Upper", "a", "has space", "has/slash", "s3://bucket", "-leading", "trailing-"):
            with self.subTest(bucket=bucket), self.assertRaises(WarehouseError):
                warehouse_location(bucket)


def recording_handler(answers: list[tuple[str, str, dict[str, Any] | None]]) -> Any:
    """A handler that records each request's method, path and decoded body, and answers a merge as Nessie does."""

    class Handler(BaseHTTPRequestHandler):
        def respond(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length)) if length else None
            answers.append((self.command, self.path, body))
            reply = {"wasApplied": True, "wasSuccessful": True, "resultantTargetHash": "c" * 64} if self.command == "POST" else {"reference": BRANCH}
            payload = json.dumps(reply).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        do_GET = do_POST = respond

        def log_message(self, format: str, *args: Any) -> None:
            return

    return Handler


class MergeRequestTest(unittest.TestCase):
    """The merge request names the source hash the caller gives and the client never reads the source's head: a moved branch is merged at that hash only."""

    def merge(self, **extra: str) -> list[tuple[str, str, dict[str, Any] | None]]:
        seen: list[tuple[str, str, dict[str, Any] | None]] = []
        server = HTTPServer(("127.0.0.1", 0), recording_handler(seen))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            url = f"http://127.0.0.1:{server.server_address[1]}/api/v2"
            hash_after = NessieClient(url, timeout_seconds=5).merge("feature", "main", "b" * 64, "the message", source_hash="a" * 64)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=10)
        self.assertEqual("c" * 64, hash_after)
        return seen

    def test_the_request_carries_the_given_source_hash_the_expected_target_hash_and_the_message(self) -> None:
        (method, path, body) = self.merge()[-1]
        self.assertEqual("POST", method)
        self.assertEqual("/api/v2/trees/main@" + "b" * 64 + "/history/merge", path)
        self.assertEqual({"fromRefName": "feature", "fromHash": "a" * 64, "message": "the message"}, body)

    def test_the_client_makes_one_call_and_never_looks_up_the_head_of_the_source(self) -> None:
        self.assertEqual([("POST", "/api/v2/trees/main@" + "b" * 64 + "/history/merge")], [(method, path) for method, path, _ in self.merge()])

    def test_the_source_hash_is_required_and_must_be_a_commit_hash(self) -> None:
        with self.assertRaises(TypeError):
            NessieClient("http://127.0.0.1:1/api/v2").merge("feature", "main", "b" * 64, "m")  # type: ignore[call-arg]
        for value in ("", "main", "a" * 63, "A" * 64):
            with self.subTest(value=value), self.assertRaises(ValueError):
                NessieClient("http://127.0.0.1:1/api/v2").merge("feature", "main", "b" * 64, "m", source_hash=value)


if __name__ == "__main__":
    unittest.main()
