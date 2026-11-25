"""The TCP connect of the end-to-end runners reads open and refused from a real local socket.

These cases bind a loopback socket, so they run in the slow group (``make test-slow``). The quick group keeps the connect cases that need no socket:
``tests/fast/test_run_record_exact_forms.py``.
"""

from __future__ import annotations

import socket
import unittest

from tests.e2e.run_record import OPEN, REFUSED, tcp_state


class ConnectToALocalSocket(unittest.TestCase):
    def test_a_port_that_accepts_is_open(self) -> None:
        with socket.socket() as server:
            server.bind(("127.0.0.1", 0))
            server.listen()
            self.assertEqual(tcp_state("127.0.0.1", server.getsockname()[1], 2), OPEN)

    def test_a_port_with_no_listener_is_refused(self) -> None:
        with socket.socket() as holder:
            holder.bind(("127.0.0.1", 0))
            port = holder.getsockname()[1]
        self.assertEqual(tcp_state("127.0.0.1", port, 2), REFUSED)

    def test_a_listener_that_never_speaks_tls_is_still_open(self) -> None:
        with socket.socket() as server:
            server.bind(("127.0.0.1", 0))
            server.listen()
            first = tcp_state("127.0.0.1", server.getsockname()[1], 2)
            second = tcp_state("127.0.0.1", server.getsockname()[1], 2)
        self.assertEqual((first, second), (OPEN, OPEN))


if __name__ == "__main__":
    unittest.main()
