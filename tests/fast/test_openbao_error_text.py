"""A call to OpenBao that carries a secret reports a failure by its method, its path and its HTTP status, and never by the body of the answer.

A real server can repeat a rejected value in its error list, and the launcher keeps the whole output of the playbook. The server here is the stand-in of
``tests/fast/openbao_stand_in.py``, made to answer each call with an error list that holds a marker. The values are synthetic markers.
"""

from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

from tests.fast.openbao_stand_in import Role, StandIn
from tests.fast.test_openbao_bootstrap import MAX_TTL, MY_OWNER, TTL, bootstrap

MARKER = "marker-that-a-server-error-repeats-0123"


class ErrorTextTest(unittest.TestCase):
    def setUp(self) -> None:
        self.folder = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.folder, True)
        self.server = StandIn({"bootstrap": Role("login-marker", "token-marker", ("secret/*",), ("secret/*",))})
        self.server.__enter__()
        self.addCleanup(self.server.__exit__)
        self.api = bootstrap.Api(self.server.address)
        self.token = self.api.login("bootstrap", "login-marker")

    def test_a_failed_call_reports_the_method_the_path_and_the_status_and_nothing_of_the_answer(self) -> None:
        calls = (
            ("POST", "secret/data/services/nessie/postgres", {"data": {"password": MARKER}}),
            ("POST", "auth/approle/role/nessie/secret-id", None),
            ("POST", "secret/data/external/dockerhub_read", {"options": {"cas": 0}, "data": {"value": MARKER}}),
            ("PUT", "sys/policies/acl/service_nessie", {"policy": MARKER}),
            ("POST", "auth/approle/role/nessie", {"role_id": "nessie"}),
        )
        for method, path, body in calls:
            with self.subTest(path=path):
                self.server.fail_next("POST", f"/v1/{path}", 400, f"invalid value {MARKER}")
                with self.assertRaises(bootstrap.BootstrapError) as caught:
                    self.api.must(method, path, self.token, body)
                self.assertEqual(f"{method} {path}: HTTP 400", str(caught.exception))

    def test_a_password_write_to_openbao_that_fails_shows_no_password(self) -> None:
        (self.folder / "postgres").mkdir()
        (self.folder / "postgres" / "nessie.password").write_text(MARKER)
        self.server.fail_next("POST", "/v1/secret/data/services/nessie/postgres", 400, f"invalid password {MARKER}")
        action = bootstrap.Action("password", "nessie", "write the PostgreSQL password of nessie")
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            bootstrap.apply(self.api, self.token, action, self.folder, MY_OWNER, TTL, MAX_TTL)
        self.assertIn("HTTP 400", str(caught.exception))
        self.assertNotIn(MARKER, str(caught.exception))

    def test_a_secret_id_that_the_server_refuses_to_issue_shows_no_text_of_the_answer(self) -> None:
        self.server.fail_next("POST", "/v1/auth/approle/role/nessie/secret-id", 500, f"internal {MARKER}")
        action = bootstrap.Action("secret_id", "nessie", "issue a secret ID for nessie")
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            bootstrap.apply(self.api, self.token, action, self.folder, MY_OWNER, TTL, MAX_TTL)
        self.assertNotIn(MARKER, str(caught.exception))


if __name__ == "__main__":
    unittest.main()
