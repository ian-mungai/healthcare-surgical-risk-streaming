"""The shared reads of the end-to-end runners accept only their exact form: a start time, a container ID, a TCP connect and a removed scratch folder.

Failure modes covered here:

- A start time is any non-empty text, or is cut to the second or the microsecond, so two starts that differ in the last digits compare equal.
- A container ID is any word, so a warning or a short name is taken as an ID.
- A TCP connect that failed for any reason reads as "refused", so an unreachable network or a missing route passes a case that asks for a refusal.
- A connect that succeeded reads as "refused" when the TLS handshake after it fails.
- A scratch folder that is still there (or a broken link that only ``lexists`` sees) reads as removed.
"""

from __future__ import annotations

import errno
import socket
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tests.e2e.run_record import (
    DROPPED,
    OPEN,
    REFUSED,
    Record,
    Unread,
    all_closed,
    container_id,
    parsed,
    remove_scratch,
    rfc3339_text,
    state_counts,
    tcp_state,
)

GOOD_TIMES = (
    "2026-11-25T00:00:00Z",
    "2026-11-25T00:00:00.1Z",
    "2026-11-25T00:00:00.123456789Z",
    "2026-11-25T09:30:59.999999999+03:00",
    "2026-02-28T23:59:59-08:00",
)
BAD_TIMES = (
    "",
    "never",
    "2026-11-25",
    "2026-11-25 00:00:00Z",
    "2026-11-25T00:00:00",
    "2026-11-25T00:00:00.Z",
    "2026-11-25T00:00:00.1234567890Z",
    "2026-13-25T00:00:00Z",
    "2026-02-30T00:00:00Z",
    "2026-11-25T24:00:00Z",
    "2026-11-25T00:60:00Z",
    "2026-11-25T00:00:60Z",
    "2026-11-25T00:00:00+24:00",
    "2026-11-25T00:00:00+0100",
    "2026-11-25t00:00:00z",
    "2026-11-25T00:00:00Z extra",
    "Warning: Permanently added '100.64.0.9' to the list of known hosts.",
    "0001-01-01T00:00:00Z",
)
ID = "a" * 64


class StartTime(unittest.TestCase):
    def test_a_timestamp_of_rfc_3339_is_kept_with_every_digit(self) -> None:
        for text in GOOD_TIMES:
            with self.subTest(text=text):
                self.assertEqual(rfc3339_text(text), text)

    def test_anything_else_is_refused(self) -> None:
        for text in BAD_TIMES:
            with self.subTest(text=text), self.assertRaises(ValueError):
                rfc3339_text(text)

    def test_a_read_that_does_not_parse_is_unread(self) -> None:
        for text in BAD_TIMES[1:]:
            with self.subTest(text=text):
                self.assertIsInstance(parsed(0, text + "\n", rfc3339_text), Unread)
        self.assertEqual(parsed(0, GOOD_TIMES[2] + "\n", rfc3339_text), GOOD_TIMES[2])

    def test_two_starts_that_differ_only_in_the_last_nanosecond_digit_are_not_equal(self) -> None:
        first, second = "2026-11-25T00:00:00.123456789Z", "2026-11-25T00:00:00.123456788Z"
        self.assertNotEqual(rfc3339_text(first), rfc3339_text(second))


class ContainerId(unittest.TestCase):
    def test_sixty_four_lower_case_hex_digits_are_an_id(self) -> None:
        self.assertEqual(container_id(ID), ID)
        self.assertEqual(container_id("0123456789abcdef" * 4), "0123456789abcdef" * 4)

    def test_anything_else_is_refused(self) -> None:
        for text in ("", "abc123", "a" * 63, "a" * 65, "A" * 64, "g" * 64, f"{ID} ", f" {ID}", "Warning: Permanently added to the list of known hosts."):
            with self.subTest(text=text), self.assertRaises(ValueError):
                container_id(text)


class Connect(unittest.TestCase):
    """``tcp_state`` decides open, refused, dropped or unread from the connect alone, whatever happens after it."""

    def failing(self, error: BaseException) -> str | Unread:
        with mock.patch.object(socket.socket, "connect", side_effect=error):
            return tcp_state("192.0.2.1", 9, 1)

    def test_a_refusal_or_a_reset_is_refused(self) -> None:
        self.assertEqual(self.failing(ConnectionRefusedError(errno.ECONNREFUSED, "refused")), REFUSED)
        self.assertEqual(self.failing(OSError(errno.ECONNREFUSED, "refused")), REFUSED)
        self.assertEqual(self.failing(ConnectionResetError(errno.ECONNRESET, "reset")), REFUSED)

    def test_a_timeout_is_dropped(self) -> None:
        self.assertEqual(self.failing(TimeoutError("timed out")), DROPPED)
        self.assertEqual(self.failing(TimeoutError(errno.ETIMEDOUT, "timed out")), DROPPED)

    def test_an_unreachable_network_or_host_or_any_other_error_is_unread(self) -> None:
        for number in (errno.ENETUNREACH, errno.EHOSTUNREACH, errno.EPERM, errno.EACCES, errno.EADDRNOTAVAIL, errno.ECONNABORTED):
            with self.subTest(number=errno.errorcode[number]):
                found = self.failing(OSError(number, "x"))
                self.assertIsInstance(found, Unread)
                self.assertIn(errno.errorcode[number], str(found))

    def test_a_name_that_does_not_resolve_is_unread(self) -> None:
        self.assertIsInstance(self.failing(socket.gaierror(socket.EAI_NONAME, "no such name")), Unread)

    def test_an_unread_state_is_neither_open_nor_refused_nor_dropped(self) -> None:
        found = self.failing(OSError(errno.ENETUNREACH, "x"))
        self.assertNotIn(found, (OPEN, REFUSED, DROPPED))

    def test_the_connect_is_bounded_by_the_deadline_that_is_given(self) -> None:
        seen: list[float | None] = []
        original = socket.socket.settimeout

        def spy(self: socket.socket, value: float | None) -> None:
            seen.append(value)
            original(self, value)

        with mock.patch.object(socket.socket, "settimeout", spy), mock.patch.object(socket.socket, "connect"):
            self.assertEqual(tcp_state("192.0.2.1", 9, 7), OPEN)
        self.assertEqual(seen, [7])


class Counts(unittest.TestCase):
    def test_each_state_is_counted_and_named(self) -> None:
        found: dict[str, str | Unread] = {"a": REFUSED, "b": REFUSED, "c": DROPPED, "d": Unread("ENETUNREACH")}
        self.assertEqual(state_counts(found), "refused: 2, dropped: 1, open: 0, unread: 1")

    def test_only_a_non_empty_set_of_refused_or_dropped_states_is_closed(self) -> None:
        self.assertTrue(all_closed({"a": REFUSED, "b": DROPPED}))
        self.assertFalse(all_closed({}))
        self.assertFalse(all_closed({"a": REFUSED, "b": OPEN}))
        self.assertFalse(all_closed({"a": REFUSED, "b": Unread("EHOSTUNREACH")}))


class ScratchFolder(unittest.TestCase):
    def test_a_folder_that_is_removed_passes(self) -> None:
        run = Record(feature="x", slug="x", rerun="x")
        scratch = Path(tempfile.mkdtemp(prefix="exact_forms_"))
        (scratch / "inner").mkdir()
        self.assertTrue(remove_scratch(run, scratch))
        self.assertEqual([(item.case, item.passed) for item in run.observations], [("scratch folder removed", True)])
        self.assertFalse(scratch.exists())

    def test_a_folder_that_is_left_fails_the_record(self) -> None:
        run = Record(feature="x", slug="x", rerun="x")
        scratch = Path(tempfile.mkdtemp(prefix="exact_forms_"))
        with mock.patch("shutil.rmtree"):
            self.assertFalse(remove_scratch(run, scratch))
        self.assertEqual([(item.case, item.passed) for item in run.observations], [("scratch folder removed", False)])
        scratch.rmdir()

    def test_a_broken_link_in_the_place_of_the_folder_is_not_removed(self) -> None:
        run = Record(feature="x", slug="x", rerun="x")
        with tempfile.TemporaryDirectory() as folder:
            link = Path(folder) / "scratch"
            link.symlink_to(Path(folder) / "missing")
            self.assertFalse(link.exists())
            with mock.patch("shutil.rmtree"):
                self.assertFalse(remove_scratch(run, link))
            self.assertEqual([item.passed for item in run.observations], [False])


if __name__ == "__main__":
    unittest.main()
