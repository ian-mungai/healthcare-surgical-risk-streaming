"""The Trino client follows the statement protocol to its end, ends every query at its deadline and names what Trino refused.

A small server on the loopback address stands in for Trino only to produce what a healthy Trino does not give on demand: a query that never
finishes, a refusal with its error name and a result in several pages. The queries themselves are proven against a real Trino by the catalog proof.
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

from scripts.stack.trino import TrinoClient, TrinoError


def serving(script: list[dict[str, Any]], seen: list[tuple[str, str, str]]) -> Any:
    """A handler that answers the first request with the first document of ``script``, the next with the next and the last one for every later one."""
    calls = iter(range(10**6))

    class Handler(BaseHTTPRequestHandler):
        def respond(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            seen.append((self.command, self.path, self.headers.get("X-Trino-User", "") + "|" + self.rfile.read(length).decode()))
            document = script[min(next(calls), len(script) - 1)]
            payload = json.dumps(document).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        do_GET = do_POST = do_DELETE = respond

        def log_message(self, format: str, *args: Any) -> None:
            return

    return Handler


@contextmanager
def fake_trino(script: list[dict[str, Any]]) -> Iterator[tuple[str, list[tuple[str, str, str]]]]:
    seen: list[tuple[str, str, str]] = []
    server = HTTPServer(("127.0.0.1", 0), serving(script, seen))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", seen
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)


def page(next_uri: str | None, rows: list[list[Any]] | None = None, **extra: Any) -> dict[str, Any]:
    document: dict[str, Any] = {"id": "q1", "stats": {"state": "RUNNING"}, **extra}
    if next_uri:
        document["nextUri"] = f"http://trino.example:8080{next_uri}"
    if rows is not None:
        document["data"] = rows
    return document


class QueryTest(unittest.TestCase):
    def test_the_statement_goes_to_the_statement_path_with_the_user_and_every_page_of_rows_is_collected_in_order(self) -> None:
        script = [page("/v1/statement/queued/q1/1"), page("/v1/statement/executing/q1/2", [[1], [2]]), page(None, [[3]])]
        with fake_trino(script) as (url, seen):
            rows = TrinoClient(url, user="reader").query("SELECT n FROM t")
        self.assertEqual([[1], [2], [3]], rows)
        self.assertEqual(("POST", "/v1/statement", "reader|SELECT n FROM t"), seen[0])
        self.assertEqual(["/v1/statement/queued/q1/1", "/v1/statement/executing/q1/2"], [path for _, path, _ in seen[1:]])

    def test_a_query_with_no_rows_returns_an_empty_list(self) -> None:
        with fake_trino([page(None)]) as (url, _):
            self.assertEqual([], TrinoClient(url).query("SHOW TABLES"))

    def test_a_refusal_names_the_error_and_holds_trinos_message(self) -> None:
        refusal = {"id": "q1", "stats": {"state": "FAILED"}, "error": {"message": "Access Denied: Cannot create table", "errorName": "PERMISSION_DENIED"}}
        with fake_trino([refusal]) as (url, _), self.assertRaises(TrinoError) as caught:
            TrinoClient(url).query("CREATE TABLE t (n bigint)")
        self.assertEqual("PERMISSION_DENIED", caught.exception.error_name)
        self.assertIn("Access Denied", str(caught.exception))

    def test_a_query_that_never_finishes_ends_at_its_deadline_and_says_so(self) -> None:
        started = time.monotonic()
        with fake_trino([page("/v1/statement/queued/q1/1")]) as (url, seen), self.assertRaisesRegex(TrinoError, "did not finish within 1 seconds"):
            TrinoClient(url, deadline_seconds=1.0).query("SELECT 1")
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual("DELETE", seen[-1][0], "the unfinished query is cancelled")

    def test_a_server_that_cannot_be_reached_is_an_error_that_names_the_address_and_not_a_hang(self) -> None:
        with self.assertRaisesRegex(TrinoError, "127.0.0.1:9"):
            TrinoClient("http://127.0.0.1:9", deadline_seconds=3.0).query("SELECT 1")

    def test_an_address_that_is_not_plain_http_is_refused(self) -> None:
        for url in ("https://trino:8080", "trino:8080", "http://trino:8080/path", ""):
            with self.subTest(url=url), self.assertRaises(TrinoError):
                TrinoClient(url)


if __name__ == "__main__":
    unittest.main()
