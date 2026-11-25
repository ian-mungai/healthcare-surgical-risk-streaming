"""A call to Nessie that has a run deadline takes the rest of the run at its own start, and never starts after the deadline.

No test opens a socket: the connection class of ``http.client`` is replaced by a stand-in that records the timeout it was given.

Failure modes covered here:

- A call got its full ``timeout_seconds`` (30) with only seconds of the run left, because the rest of the run was read before an earlier step and not at the
  start of the call, so one call ended after the run deadline.
- A call started after the run deadline had passed.
- A client with no run deadline (a local run, the deploy's own first-commit step) was shortened or refused.
"""

from __future__ import annotations

import unittest
from collections.abc import Callable
from typing import Any
from unittest import mock

from scripts.stack.nessie import NessieClient, NessieError

URL = "http://127.0.0.1:19120/api/v2"


class Connection:
    """Stands for ``http.client.HTTPConnection``: it records the timeout of each call and answers an empty JSON object with status 200."""

    timeouts: list[float] = []

    def __init__(self, host: str, port: int, timeout: float) -> None:
        self.timeouts.append(timeout)

    def request(self, *_: Any, **__: Any) -> None:
        return None

    def getresponse(self) -> Any:
        return mock.Mock(status=200, read=lambda *_: b"{}")

    def close(self) -> None:
        return None


def rest(seconds: float | None) -> Callable[[], float | None]:
    """A reading of the rest of the run that always gives the same figure."""
    return lambda: seconds


class NessieDeadlineTest(unittest.TestCase):
    def setUp(self) -> None:
        Connection.timeouts = []
        patch = mock.patch("scripts.stack.nessie.http.client.HTTPConnection", Connection)
        patch.start()
        self.addCleanup(patch.stop)

    def test_a_client_with_no_run_deadline_keeps_its_own_timeout(self) -> None:
        NessieClient(URL, 30).request("GET", "/config")
        self.assertEqual([30], Connection.timeouts)

    def test_a_call_gets_the_smaller_of_its_own_timeout_and_the_rest_of_the_run(self) -> None:
        NessieClient(URL, 30, remaining=rest(5.5)).request("GET", "/config")
        NessieClient(URL, 30, remaining=rest(500.0)).request("GET", "/config")
        self.assertEqual([5.5, 30], Connection.timeouts)

    def test_the_rest_of_the_run_is_read_again_at_the_start_of_each_call(self) -> None:
        left = iter([100.0, 12.0, 3.0])
        client = NessieClient(URL, 30, remaining=lambda: next(left))
        for _ in range(3):
            client.request("GET", "/config")
        self.assertEqual([30, 12.0, 3.0], Connection.timeouts)

    def test_a_call_that_would_start_at_or_after_the_deadline_is_not_sent(self) -> None:
        for left in (0.0, -1.0):
            with self.subTest(left=left), self.assertRaises(NessieError) as caught:
                NessieClient(URL, 30, remaining=rest(left)).request("GET", "/config")
            self.assertIn("deadline", str(caught.exception))
        self.assertEqual([], Connection.timeouts)

    def test_a_clock_that_reports_no_deadline_keeps_the_own_timeout(self) -> None:
        NessieClient(URL, 30, remaining=rest(None)).request("GET", "/config")
        self.assertEqual([30], Connection.timeouts)


if __name__ == "__main__":
    unittest.main()
