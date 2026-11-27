"""What the OpenBao step's visible output can show of the server's replies: validated booleans, HTTP status codes and the names of the tracked lists.

The playbook prints the actions, the count of unlisted roles and the refusal of the hidden reconcile step, so each reply field that one of them reads is fed
a value in the shape of a secret here. No real server sends such a reply, so the connection is replaced; the run through ``ansible-playbook`` is in
``tests/slow/test_openbao_role_tasks.py``. The values are synthetic markers.

The failures covered: a seal status whose flags are no Booleans; a role listing that is refused; a role or policy list that is no list of names, holds another
thing than a name or has no data; a role or a ``service_`` policy whose name the service list does not hold, in the action that retires it, in the refusal of
its read or its write and in a request that cannot be sent.
"""

from __future__ import annotations

import http.client
import json
import os
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from tests.fast.header_secret_support import everything_said_about
from tests.fast.test_openbao_bootstrap import MAX_TTL, TTL, bootstrap

MARKER = "hvs.synthetic-secret-shaped-marker-0123456789"
SECRET_SHAPES: tuple[Any, ...] = ({"client_token": MARKER}, [MARKER], MARKER, {MARKER: True})
ROLE_LIST = "auth/approle/role?list=true"
POLICY_LIST = "sys/policies/acl?list=true"
GRANT = 'path "secret/data/external/dockerhub_read" { capabilities = ["read"] }\n'
RETIRED_ACTION = "drop the external grants of the policy of a service that left the list"


class Server:
    """A server that already matches the list of one service, with the replies that a case replaces. A reply is a status and a document."""

    def __init__(self) -> None:
        self.replies: dict[tuple[str, str], tuple[int, Any]] = {
            ("GET", "sys/seal-status"): (200, {"initialized": True, "sealed": False}),
            ("POST", "auth/approle/login"): (200, {"auth": {"client_token": "admin-token"}}),
            ("GET", "sys/policies/acl/service_nessie"): (200, {"data": {"policy": bootstrap.policy_text("nessie")}}),
            ("GET", "auth/approle/role/nessie"): (200, {"data": bootstrap.role_settings("nessie", TTL, MAX_TTL)}),
            ("GET", "auth/approle/role/nessie/role-id"): (200, {"data": {"role_id": "nessie"}}),
            ("POST", "auth/approle/role/nessie/secret-id/lookup"): (200, {"data": {"valid": True}}),
            ("GET", POLICY_LIST): (200, {"data": {"keys": ["service_nessie"]}}),
            ("GET", ROLE_LIST): (200, {"data": {"keys": ["bootstrap", "nessie"]}}),
        }

    def connection(self) -> type:
        replies = self.replies

        class Connection:
            def __init__(self, host: str, port: int, timeout: float) -> None:
                self.reply: tuple[int, Any] = (404, {})

            def request(self, method: str, path: str, body: str | None, headers: dict[str, str]) -> None:
                # The two refusals of the HTTP library for a path that it cannot send, both of which repeat the path.
                if not path.isascii():
                    path.encode("ascii")
                if " " in path:
                    raise http.client.InvalidURL(f"URL can't contain control characters. {path!r}")
                self.reply = replies.get((method, path.removeprefix("/v1/")), (404, {}))

            def getresponse(self) -> Any:
                status, document = self.reply
                return mock.Mock(status=status, read=mock.Mock(return_value=json.dumps(document).encode()))

            def close(self) -> None:
                return

        return Connection


class VisibleOutputCase(unittest.TestCase):
    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.folder = Path(scratch.name)
        (self.folder / "secrets" / "openbao").mkdir(parents=True)
        for name in ("bootstrap", "nessie"):
            path = self.folder / "secrets" / "openbao" / f"{name}.secret_id"
            path.write_text("a-secret-id")
            path.chmod(0o400)
        (self.folder / "services.txt").write_text("nessie\n")
        (self.folder / "tools.txt").write_text("unused\n")
        self.server = Server()
        patcher = mock.patch.object(bootstrap.http.client, "HTTPConnection", self.server.connection())
        patcher.start()
        self.addCleanup(patcher.stop)

    def visible(self, *, deadline: int = 5) -> str:
        """What the two visible tasks of ``bootstrap.yml`` can print: the actions and the count, or the refusal, with everything that its error says."""
        owner = f"{os.getuid()}:{os.getgid()}"
        (self.folder / "tools.txt").write_text("nessie\n")
        (self.folder / "secrets" / "postgres").mkdir(exist_ok=True)
        (self.folder / "secrets" / "postgres" / "nessie.password").write_text("a-password")
        self.server.replies.setdefault(("GET", "secret/data/services/nessie/postgres"), (200, {"data": {"data": {"password": "a-password"}}}))
        parameters = {
            "secrets_directory": str(self.folder / "secrets"),
            "services_file": str(self.folder / "services.txt"),
            "tools_file": str(self.folder / "tools.txt"),
            "address": "http://127.0.0.1:8200",
            "file_owner": owner,
            "root_owner": owner,
            "ready_deadline_seconds": deadline,
            "token_ttl_seconds": TTL,
            "token_max_ttl_seconds": MAX_TTL,
        }
        try:
            result = bootstrap.reconcile(parameters, check_only=False)
        except (bootstrap.BootstrapError, bootstrap.SecretFileError) as error:
            return f"refusal: {error}\n{everything_said_about(error)}"
        return json.dumps({"actions": result["actions"], "unlisted": result["unlisted"]})


class MatchingServerTest(VisibleOutputCase):
    def test_a_server_that_matches_shows_no_action_and_a_count_of_zero(self) -> None:
        self.assertEqual({"actions": [], "unlisted": 0}, json.loads(self.visible()))


class SealStatusTest(VisibleOutputCase):
    def test_a_flag_that_is_not_a_boolean_gives_a_fixed_text_and_nothing_of_the_reply(self) -> None:
        for field in ("initialized", "sealed"):
            for shape in (*SECRET_SHAPES, 0, 1, None):
                with self.subTest(field=field, shape=shape):
                    self.server.replies[("GET", "sys/seal-status")] = (200, {"initialized": True, "sealed": True, field: shape})
                    shown = self.visible(deadline=0)
                    self.assertIn("did not become ready within 0 seconds (last answer: HTTP 200, a malformed seal status)", shown)
                    self.assertNotIn(MARKER, shown)

    def test_two_booleans_are_shown_with_the_status(self) -> None:
        self.server.replies[("GET", "sys/seal-status")] = (503, {"initialized": True, "sealed": True, "cluster_name": MARKER})
        shown = self.visible(deadline=0)
        self.assertIn("(last answer: HTTP 503, initialized True, sealed True)", shown)
        self.assertNotIn(MARKER, shown)


class RoleListTest(VisibleOutputCase):
    def test_a_role_that_the_service_list_does_not_hold_is_counted_and_never_named(self) -> None:
        self.server.replies[("GET", ROLE_LIST)] = (200, {"data": {"keys": ["bootstrap", "nessie", MARKER, f"{MARKER}-two"]}})
        shown = self.visible()
        self.assertEqual({"actions": [], "unlisted": 2}, json.loads(shown))

    def test_a_role_listing_that_is_refused_gives_no_count_but_unavailable_with_the_status(self) -> None:
        for status in (204, 403, 404, 500, 503):
            with self.subTest(status=status):
                self.server.replies[("GET", ROLE_LIST)] = (status, {"errors": [MARKER], "data": {"keys": [MARKER]}})
                shown = self.visible()
                self.assertEqual({"actions": [], "unlisted": f"unavailable (HTTP {status})"}, json.loads(shown))

    def test_a_reply_that_is_no_list_of_names_is_refused_with_a_fixed_text(self) -> None:
        replies: list[Any] = [{"data": {"keys": [shape]}} for shape in ({"client_token": MARKER}, [MARKER], 7, None)]
        replies += [{"data": {"keys": shape}} for shape in (MARKER, {MARKER: True}, None)]
        replies += [{"data": MARKER}, {"data": [MARKER]}, {"data": {}}, {"errors": [MARKER]}, [MARKER], MARKER]
        for reply in replies:
            with self.subTest(reply=reply):
                self.server.replies[("GET", ROLE_LIST)] = (200, reply)
                shown = self.visible()
                self.assertIn(f"refusal: GET {ROLE_LIST}: a malformed reply", shown)
                self.assertNotIn(MARKER, shown)


class PolicyListTest(VisibleOutputCase):
    def retired(self, name: str, *, policy: str = GRANT, read: int = 200, write: int = 204) -> str:
        """A policy list that holds a ``service_`` policy of a service that left the list, with the statuses of its read and its write."""
        self.server.replies[("GET", POLICY_LIST)] = (200, {"data": {"keys": ["service_nessie", f"service_{name}"]}})
        self.server.replies[("GET", f"sys/policies/acl/service_{name}")] = (read, {"data": {"policy": policy}, "errors": [MARKER]})
        self.server.replies[("PUT", f"sys/policies/acl/service_{name}")] = (write, {"errors": [MARKER]})
        return self.visible()

    def test_the_action_that_retires_a_policy_does_not_name_the_service_that_the_server_listed(self) -> None:
        shown = self.retired(MARKER)
        self.assertEqual({"actions": [RETIRED_ACTION], "unlisted": 0}, json.loads(shown))

    def test_a_refused_read_or_write_of_that_policy_names_its_method_and_status_and_not_the_listed_name(self) -> None:
        for method, read, write, status in (("GET", 403, 204, 403), ("PUT", 200, 400, 400)):
            with self.subTest(method=method):
                shown = self.retired(MARKER, read=read, write=write)
                self.assertIn(f"refusal: {method} sys/policies/acl/<a service that left the list>: HTTP {status}", shown)
                self.assertNotIn(MARKER, shown)

    def test_a_listed_name_that_cannot_be_sent_is_refused_without_the_name(self) -> None:
        for name, kind in ((f"{MARKER} tail", "InvalidURL"), (f"{MARKER}é", "UnicodeEncodeError")):
            with self.subTest(kind=kind):
                shown = self.retired(name)
                self.assertIn(f"GET sys/policies/acl/<a service that left the list>: no answer from http://127.0.0.1:8200 ({kind})", shown)
                self.assertNotIn(MARKER, shown)

    def test_a_reply_that_is_no_list_of_names_is_refused_with_a_fixed_text(self) -> None:
        replies: list[Any] = [{"data": {"keys": [shape]}} for shape in ({"client_token": MARKER}, [MARKER], 7, None)]
        replies += [{"data": {"keys": shape}} for shape in (MARKER, {MARKER: True}, None)]
        replies += [{"data": MARKER}, {"data": [MARKER]}, {"data": {}}, MARKER]
        for reply in replies:
            with self.subTest(reply=reply):
                self.server.replies[("GET", POLICY_LIST)] = (200, reply)
                shown = self.visible()
                self.assertIn(f"refusal: GET {POLICY_LIST}: a malformed reply", shown)
                self.assertNotIn(MARKER, shown)


if __name__ == "__main__":
    unittest.main()
