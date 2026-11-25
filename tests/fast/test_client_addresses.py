"""A client for Nessie, the registry or Trino refuses an address whose port cannot be used with its own error, not ``urlsplit``'s.

Failure mode: ``parts.port`` raises ``ValueError("Port could not be cast to integer value as '<port>'")`` for a port that is not a number, and a ``ValueError``
for one out of range. A job that does not catch ``ValueError`` shows that text with the port in a traceback. Each client turns it into its own error, with the
same fixed text as for any other bad address, so a caller that already maps that error (and ``describe_error``) shows no value.

Failure mode (``urlsplit``): an address with an ``[`` that is not closed raises ``ValueError("Invalid IPv6 URL")`` and a host with a character that Unicode
normalisation turns into a separator raises a ``ValueError`` that holds the whole host. Both escape the constructor as a traceback (exit 1), and the second one
shows the input. They are the same error of the client, with the same fixed text, as a bad port.
"""

from __future__ import annotations

import json
import unittest

from replay.service.registry import RegistryClient, RegistryError
from scripts.stack.nessie import NessieClient, NessieError
from scripts.stack.trino import TrinoClient, TrinoError

MARKER = "marker_value_x9"


class Answering(TrinoClient):
    """A Trino client whose wire always answers with the same body."""

    answer = b"{}"

    def send(self, method: str, target: str, payload: bytes | None, headers: dict[str, str], timeout_seconds: float) -> tuple[int, bytes]:
        return 200, self.answer


class Addresses(unittest.TestCase):
    def test_a_port_that_is_not_usable_is_the_error_of_the_client_with_a_fixed_text(self) -> None:
        cases = (
            (NessieClient, NessieError, "http://nessie:{port}/api/v2"),
            (RegistryClient, RegistryError, "http://registry:{port}/apis/registry/v3"),
            (TrinoClient, TrinoError, "http://trino:{port}"),
        )
        for client, error, template in cases:
            fixed = None
            for port in (MARKER, "99999", "-1", "80x", "1_0"):
                with self.subTest(client=client.__name__, port=port):
                    with self.assertRaises(error) as caught:
                        client(template.format(port=port))
                    self.assertNotIn(port, str(caught.exception))
                    self.assertIsNone(caught.exception.__cause__)
                    self.assertTrue(caught.exception.__suppress_context__)
                    fixed = fixed or str(caught.exception)
                    self.assertEqual(fixed, str(caught.exception))

    def test_an_address_that_urlsplit_refuses_is_the_error_of_the_client_with_a_fixed_text(self) -> None:
        cases = (
            (NessieClient, NessieError, "api/v2"),
            (RegistryClient, RegistryError, "apis/registry/v3"),
            (TrinoClient, TrinoError, ""),
        )
        hosts = ("[::1", "[", f"[{MARKER}", f"nes\u2100{MARKER}", f"{MARKER}\uff03x", f"nessie:[{MARKER}")
        for client, error, path in cases:
            fixed = None
            for host in hosts:
                with self.subTest(client=client.__name__, host=host):
                    with self.assertRaises(error) as caught:
                        client(f"http://{host}:80/{path}")
                    self.assertNotIn(MARKER, str(caught.exception))
                    self.assertNotIn("NFKC", str(caught.exception))
                    self.assertNotIn("IPv6", str(caught.exception))
                    self.assertIsNone(caught.exception.__cause__)
                    self.assertTrue(caught.exception.__suppress_context__)
                    fixed = fixed or str(caught.exception)
                    self.assertEqual(fixed, str(caught.exception))

    def test_a_next_address_from_trino_that_urlsplit_refuses_is_a_trino_error_with_a_fixed_text(self) -> None:
        for next_uri in (f"http://[{MARKER}/v1/statement/x", f"http://trino:8080//[{MARKER}/x", f"http://tri\u2100{MARKER}:8080/v1/x"):
            with self.subTest(next_uri=next_uri):
                client = Answering("http://trino:8080", deadline_seconds=0.3)
                client.answer = json.dumps({"nextUri": next_uri}).encode()
                with self.assertRaises(TrinoError) as caught:
                    client.query("SELECT 1")
                self.assertNotIn(MARKER, str(caught.exception))

    def test_a_port_that_is_usable_and_an_absent_port_still_work(self) -> None:
        self.assertEqual((19120, 80), (NessieClient("http://nessie:19120/api/v2").port, NessieClient("http://nessie/api/v2").port))
        self.assertEqual((8080, 80), (RegistryClient("http://r:8080/v3").port, RegistryClient("http://r/v3").port))
        self.assertEqual((8080, 80), (TrinoClient("http://t:8080").port, TrinoClient("http://t").port))


if __name__ == "__main__":
    unittest.main()
