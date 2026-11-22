"""The deploy's Nessie step gives the default branch its first commit, the project's namespace, once: a second run changes nothing.

A small server on the loopback address stands in for Nessie and keeps the one state that matters here, the default branch's hash and the commits it was sent,
and a second branch ``main`` that is not the default when the default is ``trunk``, so the cases prove the command's decision, the branch it reads, the hash
at which it reads the entries and its messages. The calls against a real Nessie, from the code location container, are proved by the acceptance test
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
from urllib.parse import unquote

from scripts.stack import nessie

EMPTY = "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d"
DOCKERFILE = Path(__file__).resolve().parents[2] / "orchestration" / "Dockerfile"


class StandIn:
    """The default branch's name, hash and namespaces, the commits received, the answer to the configuration call and an optional refusal status for every
    call. A default other than ``main`` leaves ``main`` as a branch of its own, with its own hash and namespace. ``entries_hash`` is the hash that every entries
    answer names as its own, when it differs from the one asked for."""

    def __init__(self, refuse: int = 0, default: str = "main") -> None:
        self.default = default
        self.hash = EMPTY
        self.commits: list[dict[str, Any]] = []
        self.refuse = refuse
        self.config: dict[str, Any] = {"noAncestorHash": EMPTY, "defaultBranch": default}
        self.namespaces: list[str] = []
        self.entry_type = "NAMESPACE"
        self.keep_commits = True
        self.entries_hash: str | None = None
        self.asked: list[str] = []

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

            def held(self, name: str) -> tuple[str, list[str]]:
                """The hash and the namespaces of a branch: the default branch's own, or those of ``main`` when that is another branch."""
                return (state.hash, state.namespaces) if name == state.default else ("d" * 64, ["surgical_risk"])

            def do_GET(self) -> None:
                target = unquote(self.path.removeprefix("/api/v2"))
                state.asked.append(target)
                if state.refuse:
                    self.answer(state.refuse, {"errorCode": "UNKNOWN", "message": "refused"})
                elif target.endswith("/config"):
                    self.answer(200, state.config)
                elif target.endswith("/entries"):
                    name, _, asked_hash = target.removeprefix("/trees/").removesuffix("/entries").partition("@")
                    hash_, namespaces = self.held(name)
                    reference = {"type": "BRANCH", "name": name, "hash": state.entries_hash or asked_hash or hash_}
                    entries = [{"type": state.entry_type, "name": {"elements": [namespace]}} for namespace in namespaces]
                    self.answer(200, {"entries": entries, "effectiveReference": reference})
                else:
                    name = target.removeprefix("/trees/")
                    self.answer(200, {"reference": {"type": "BRANCH", "name": name, "hash": self.held(name)[0]}})

            def do_POST(self) -> None:
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)))
                state.commits.append(body)
                if state.keep_commits:
                    state.namespaces += [operation["key"]["elements"][0] for operation in body["operations"]]
                state.hash = "b" * 64
                self.answer(200, {"targetBranch": {"type": "BRANCH", "name": state.default, "hash": state.hash}})

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

    def test_a_configuration_answer_without_a_default_branch_and_a_hash_is_refused_and_nothing_is_committed(self) -> None:
        for config in (
            {},
            {"defaultBranch": "main"},
            {"noAncestorHash": EMPTY},
            {"defaultBranch": "", "noAncestorHash": EMPTY},
            {"defaultBranch": "main", "noAncestorHash": "abc"},
        ):
            with self.subTest(config=config):
                state = StandIn()
                state.config = config
                with serving(state) as uri:
                    code, out, err = run(["initialise", "--namespace", "surgical_risk"], uri)
                self.assertEqual(1, code)
                self.assertIn("configuration", err)
                self.assertNotIn("unchanged", out)
                self.assertEqual([], state.commits)

    def test_a_head_past_the_empty_history_without_the_namespace_is_not_unchanged_and_gets_the_namespace_once(self) -> None:
        state = StandIn()
        state.hash = "c" * 64
        with serving(state) as uri:
            first = run(["initialise", "--namespace", "surgical_risk"], uri)
            second = run(["initialise", "--namespace", "surgical_risk"], uri)
        self.assertEqual((0, 0), (first[0], second[0]))
        self.assertIn("initialised", first[1])
        self.assertNotIn("unchanged", first[1])
        self.assertIn("unchanged", second[1])
        self.assertEqual(1, len(state.commits))

    def test_a_head_past_the_empty_history_with_the_namespace_is_unchanged(self) -> None:
        state = StandIn()
        state.hash = "c" * 64
        state.namespaces = ["surgical_risk"]
        with serving(state) as uri:
            code, out, err = run(["initialise", "--namespace", "surgical_risk"], uri)
        self.assertEqual(0, code)
        self.assertIn("unchanged", out)
        self.assertEqual([], state.commits)

    def test_a_content_of_the_namespace_name_that_is_not_a_namespace_does_not_count(self) -> None:
        state = StandIn()
        state.hash = "c" * 64
        state.namespaces = ["surgical_risk"]
        state.entry_type = "ICEBERG_TABLE"
        with serving(state) as uri:
            code, out, err = run(["initialise", "--namespace", "surgical_risk"], uri)
        self.assertEqual(1, len(state.commits))
        self.assertNotIn("unchanged", out)

    def test_the_branch_is_the_default_branch_of_the_configuration_and_the_namespace_on_another_branch_does_not_count(self) -> None:
        state = StandIn(default="trunk")
        state.hash = "c" * 64
        with serving(state) as uri:
            first = run(["initialise", "--namespace", "surgical_risk"], uri)
            second = run(["initialise", "--namespace", "surgical_risk"], uri)
        self.assertEqual((0, 0), (first[0], second[0]))
        self.assertIn("initialised: trunk holds", first[1])
        self.assertNotIn("unchanged", first[1])
        self.assertIn("unchanged: trunk already holds", second[1])
        self.assertEqual(1, len(state.commits))
        self.assertTrue(all("/trees/main" not in asked for asked in state.asked), state.asked)

    def test_a_branch_that_is_not_the_default_is_refused_with_both_names_and_nothing_is_committed(self) -> None:
        for default, given in (("trunk", "main"), ("main", "trunk")):
            with self.subTest(default=default, given=given):
                state = StandIn(default=default)
                with serving(state) as uri:
                    code, out, err = run(["initialise", "--namespace", "surgical_risk", "--branch", given], uri)
                self.assertEqual(1, code)
                self.assertIn(default, err)
                self.assertIn(given, err)
                self.assertEqual("", out)
                self.assertEqual([], state.commits)
        with serving(StandIn(default="trunk")) as uri:
            code, out, err = run(["initialise", "--namespace", "surgical_risk", "--branch", "trunk"], uri)
        self.assertEqual(0, code)

    def test_every_namespace_lookup_reads_the_entries_at_the_exact_hash_that_was_observed(self) -> None:
        state = StandIn(default="trunk")
        state.hash = "c" * 64
        with serving(state) as uri:
            run(["initialise", "--namespace", "surgical_risk"], uri)
        lookups = [asked for asked in state.asked if asked.endswith("/entries")]
        self.assertEqual(["/trees/trunk@" + "c" * 64 + "/entries", "/trees/trunk@" + "b" * 64 + "/entries"], lookups)

    def test_an_entries_answer_for_another_hash_is_refused_in_the_check_and_in_the_read_after_the_commit(self) -> None:
        for label, head, commits in (("check", "c" * 64, 0), ("after the commit", EMPTY, 1)):
            with self.subTest(read=label):
                state = StandIn()
                state.hash = head
                state.entries_hash = "e" * 64
                state.namespaces = ["surgical_risk"] if label == "check" else []
                with serving(state) as uri:
                    code, out, err = run(["initialise", "--namespace", "surgical_risk"], uri)
                self.assertEqual(1, code)
                self.assertIn("hash", err)
                self.assertNotIn("unchanged", out)
                self.assertNotIn("initialised", out)
                self.assertEqual(commits, len(state.commits))

    def test_a_commit_that_leaves_the_namespace_missing_is_refused(self) -> None:
        state = StandIn()
        state.keep_commits = False
        with serving(state) as uri:
            code, out, err = run(["initialise", "--namespace", "surgical_risk"], uri)
        self.assertEqual(1, code)
        self.assertIn("surgical_risk", err)
        self.assertNotIn("initialised", out)

    def test_a_refusal_exits_1_and_names_the_call_and_nothing_is_committed(self) -> None:
        state = StandIn(refuse=500)
        with serving(state) as uri:
            code, out, err = run(["initialise", "--namespace", "surgical_risk"], uri)
        self.assertEqual(1, code)
        self.assertIn("GET /api/v2/config was refused with status 500", err)
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
