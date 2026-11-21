"""The digest of a decoded payload: the one comparison form of an expected and an observed message.

Failure modes covered here:

- Two payloads that differ in one value, one type or one list item share a digest, so a changed reading passes the manifest.
- The digest depends on the order of the fields, so the same message decoded by another library gets another digest.
- A float is compared through a rounded text, so a value that differs in its last bit passes.
- A value that JSON cannot hold (not a number, negative zero) breaks the digest or is merged with another value.
"""

from __future__ import annotations

import unittest

from replay.events.payload import canonical_bytes, payload_digest

BASE = {"event_id": "a|0|1.0|T|0|0", "room": 0, "value": 61.0, "samples": [1.0, 2.5], "text": "x"}


class Digest(unittest.TestCase):
    def test_the_digest_is_a_sha256_that_the_same_payload_reproduces_whatever_the_field_order(self) -> None:
        self.assertRegex(payload_digest(BASE), r"[0-9a-f]{64}")
        self.assertEqual(payload_digest(BASE), payload_digest(dict(reversed(list(BASE.items())))))

    def test_a_change_of_any_value_type_or_list_item_changes_the_digest(self) -> None:
        reference = payload_digest(BASE)
        changed = [
            BASE | {"event_id": "a|0|1.0|T|0|1"},
            BASE | {"room": 1},
            BASE | {"value": 61.00000000000001},
            BASE | {"value": 61},
            BASE | {"samples": [1.0, 2.50000000000001]},
            BASE | {"samples": [1.0]},
            BASE | {"samples": [2.5, 1.0]},
            BASE | {"text": "y"},
            {key: value for key, value in BASE.items() if key != "text"},
            BASE | {"extra": 1},
        ]
        self.assertEqual(len(changed), len({payload_digest(item) for item in changed} - {reference}))

    def test_a_value_that_json_cannot_hold_has_its_own_digest(self) -> None:
        zero, negative = payload_digest(BASE | {"value": 0.0}), payload_digest(BASE | {"value": -0.0})
        self.assertNotEqual(zero, negative)
        nan = payload_digest(BASE | {"value": float("nan")})
        self.assertEqual(nan, payload_digest(BASE | {"value": float("-nan")}))
        self.assertNotEqual(nan, zero)
        self.assertNotEqual(payload_digest(BASE | {"value": float("inf")}), payload_digest(BASE | {"value": float("-inf")}))

    def test_a_text_with_a_quote_a_backslash_or_a_line_break_cannot_pass_for_another_payload(self) -> None:
        self.assertNotEqual(payload_digest(BASE | {"text": 'a"b'}), payload_digest(BASE | {"text": "a\\b"}))
        self.assertNotEqual(payload_digest(BASE | {"text": "a\nb"}), payload_digest(BASE | {"text": "a b"}))
        self.assertNotEqual(payload_digest(BASE | {"text": "f:0x1.0p+0"}), payload_digest(BASE | {"text": 1.0}))

    def test_a_payload_with_a_type_the_formats_do_not_use_is_refused(self) -> None:
        for value in (None, b"bytes", {"a": 1}, (1, 2), {1, 2}):
            with self.subTest(value=repr(value)), self.assertRaises(TypeError):
                canonical_bytes(BASE | {"value": value})
