"""The registry client's registration of a format, proved against a stand-in for Apicurio Registry's answers.

Failure modes covered here:

- A format whose content changed is registered under the version name of its first version. Apicurio Registry 3.3.5 creates the new version with
  the name that the request holds, so a name that exists is refused as a repeated version (its primary key) and the writer cannot start after a
  format gains a field or a symbol. The request must leave the name out, so that the registry gives the next one
  (``AbstractSqlRegistryStorage.createArtifactVersion`` of the 3.3.5 tag generates the name when it is null).
- A format that is already registered with the same bytes gets a new version.
"""

from __future__ import annotations

import json
import unittest
from typing import Any

from replay.service.registry import RegistryClient


class Recording(RegistryClient):
    def __init__(self) -> None:
        super().__init__("http://127.0.0.1:1/apis/registry/v3")
        self.requests: list[tuple[str, str, dict[str, Any]]] = []

    def send(self, method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
        self.requests.append((method, target, json.loads(payload) if payload else {}))
        answer = {"version": {"version": "2", "contentId": 7, "globalId": 9}}
        return 200, json.dumps(answer).encode()


class Register(unittest.TestCase):
    def test_the_request_names_no_version_so_the_registry_gives_the_next_one(self) -> None:
        client = Recording()
        registered = client.register("g", "subject", '{"type": "record"}')
        ((method, target, body),) = client.requests
        self.assertEqual("POST", method)
        self.assertIn("ifExists=FIND_OR_CREATE_VERSION", target)
        self.assertNotIn("version", body["firstVersion"])
        self.assertEqual({"content": '{"type": "record"}', "contentType": "application/json"}, body["firstVersion"]["content"])
        self.assertEqual(("2", 7), (registered.version, registered.content_id))


if __name__ == "__main__":
    unittest.main()
