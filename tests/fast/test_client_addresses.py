"""A client for Nessie, the registry or Trino refuses an address whose port cannot be used with its own error, not ``urlsplit``'s.

Failure mode: ``parts.port`` raises ``ValueError("Port could not be cast to integer value as '<port>'")`` for a port that is not a number, and a ``ValueError``
for one out of range. A job that does not catch ``ValueError`` shows that text with the port in a traceback. Each client turns it into its own error, with the
same fixed text as for any other bad address, so a caller that already maps that error (and ``describe_error``) shows no value.
"""

from __future__ import annotations

import unittest

from replay.service.registry import RegistryClient, RegistryError
from scripts.stack.nessie import NessieClient, NessieError
from scripts.stack.trino import TrinoClient, TrinoError

MARKER = "marker_value_x9"


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

    def test_a_port_that_is_usable_and_an_absent_port_still_work(self) -> None:
        self.assertEqual((19120, 80), (NessieClient("http://nessie:19120/api/v2").port, NessieClient("http://nessie/api/v2").port))
        self.assertEqual((8080, 80), (RegistryClient("http://r:8080/v3").port, RegistryClient("http://r/v3").port))
        self.assertEqual((8080, 80), (TrinoClient("http://t:8080").port, TrinoClient("http://t").port))


if __name__ == "__main__":
    unittest.main()
