"""The wire format of one message: the magic byte, the 4-byte big-endian content ID and the Avro body.

Failure modes covered here:

- The content ID is written in the wrong byte order or width, so a reader looks up another schema.
- A message without the prefix, or with another magic byte, is read as if it had one.
- A zero or negative content ID, which the registry never gives, is framed.
"""

from __future__ import annotations

import unittest

from replay.events.wire import WireError, frame, unframe


class Wire(unittest.TestCase):
    def test_a_framed_message_is_the_magic_byte_the_big_endian_content_id_and_the_body(self) -> None:
        self.assertEqual(b"\x00\x00\x00\x01\x02body", frame(258, b"body"))
        self.assertEqual(b"\x00\x7f\xff\xff\xff", frame(2**31 - 1, b""))

    def test_a_framed_message_is_read_back_into_its_content_id_and_body(self) -> None:
        for content_id in (1, 255, 256, 65_536, 2**31 - 1):
            self.assertEqual((content_id, b"\x01\x02"), unframe(frame(content_id, b"\x01\x02")))

    def test_a_message_that_does_not_follow_the_format_is_refused(self) -> None:
        for message in (b"", b"\x00\x00\x00\x00", b"\x01\x00\x00\x00\x01body", b"{}"):
            with self.subTest(message=message), self.assertRaises(WireError):
                unframe(message)

    def test_a_content_id_the_registry_never_gives_is_refused(self) -> None:
        for content_id in (0, -1, 2**31):
            with self.subTest(content_id=content_id), self.assertRaises(WireError):
                frame(content_id, b"")
