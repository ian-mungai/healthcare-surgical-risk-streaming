"""The deploy's Nessie step gives main its first commit, the project's namespace, once: a second run changes nothing.

A small server on the loopback address stands in for Nessie and keeps the one state that matters here, main's hash and the commits it was sent, so the
cases prove the command's decision and its messages. The calls against a real Nessie, from the code location container, are proved by the acceptance test
of the joined stack.
"""

from __future__ import annotations

import contextlib
import io
import json
import threading
import unittest
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.stack import nessie

EMPTY = "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d"
DOCKERFILE = Path(__file__).resolve().parents[2] / "orchestration" / "Dockerfile"


class StandIn:
    """Main's hash, the commits received and an optional refusal status for every call."""

    def __init__(self, refuse: int = 0) -> None:
        self.hash = EMPTY
        self.commits: list[dict[str, Any]] = []
        self.refuse = refuse

    def handler(self) -> type[BaseHTTPRequestHandler]:
        state = self

        class Handler(BaseHTTPRequestHandler):
            def answer(self, status: int, document: dict[str, Any]) -> None:
                payload = json.dumps(document).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_GET(self) -> None:
                if state.refuse:
                    self.answer(state.refuse, {"errorCode": "UNKNOWN", "message": "refused"})
                elif self.path.endswith("/config"):
                    self.answer(200, {"noAncestorHash": EMPTY, "defaultBranch": "main"})
                else:
                    self.answer(200, {"reference": {"type": "BRANCH", "name": "main", "hash": state.hash}})

            def do_POST(self) -> None:
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)))
                state.commits.append(body)
                state.hash = "b" * 64
                self.answer(200, {"targetBranch": {"type": "BRANCH", "name": "main", "hash": state.hash}})

            def log_message(self, format: str, *args: Any) -> None:
                return

        return Handler


@contextlib.contextmanager
def serving(state: StandIn) -> Iterator[str]:
    server = HTTPServer(("127.0.0.1", 0), state.handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/api/v2"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


def run(arguments: list[str], uri: str | None) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    environment = {} if uri is None else {"NESSIE_URI": uri}
    with mock.patch.dict("os.environ", environment, clear=True), contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = nessie.main(arguments)
    return code, out.getvalue(), err.getvalue()


class InitialiseCommandTest(unittest.TestCase):
    def test_an_empty_main_gets_the_namespace_once_and_a_second_run_changes_nothing(self) -> None:
        state = StandIn()
        with serving(state) as uri:
            first = run(["initialise", "--namespace", "surgical_risk"], uri)
            second = run(["initialise", "--namespace", "surgical_risk"], uri)
        self.assertEqual((0, 0), (first[0], second[0]))
        self.assertIn("initialised", first[1])
        self.assertIn("unchanged", second[1])
        self.assertNotIn("initialised", second[1])
        self.assertEqual(1, len(state.commits))
        self.assertEqual([{"elements": ["surgical_risk"]}], [operation["key"] for operation in state.commits[0]["operations"]])

    def test_a_refusal_exits_1_and_names_the_call_and_nothing_is_committed(self) -> None:
        state = StandIn(refuse=500)
        with serving(state) as uri:
            code, out, err = run(["initialise", "--namespace", "surgical_risk"], uri)
        self.assertEqual(1, code)
        self.assertIn("GET /api/v2/trees/main", err)
        self.assertEqual([], state.commits)

    def test_no_address_or_a_bad_namespace_exits_2_before_any_call(self) -> None:
        state = StandIn()
        with serving(state) as uri:
            unset = run(["initialise", "--namespace", "surgical_risk"], None)
            bad = run(["initialise", "--namespace", "Surgical Risk"], uri)
        self.assertEqual((2, 2), (unset[0], bad[0]))
        self.assertIn("NESSIE_URI", unset[2])
        self.assertEqual([], state.commits)


class CodeLocationImageTest(unittest.TestCase):
    def test_the_dagster_image_holds_the_nessie_module_that_the_deploy_runs_in_it(self) -> None:
        lines = [line for line in DOCKERFILE.read_text().splitlines() if line.startswith("COPY --from=stack_scripts stack/")]
        self.assertEqual(1, len(lines))
        self.assertIn(" stack/nessie.py ", f" {lines[0]} ")


if __name__ == "__main__":
    unittest.main()
