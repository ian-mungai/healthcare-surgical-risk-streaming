"""The S3 store's reading of the AWS command line: listing pages, exact-version ranged reads and the refusal of a reply for another version.

The stub below answers with the JSON shapes that the AWS command line documents. It is not S3: the acceptance test runs the same calls
against a real short-lived bucket. These cases cover replies a real bucket cannot readily be made to give.
"""

from __future__ import annotations

import unittest
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

from scripts.ingestion.store import S3Store, StoreError


class StubAws:
    """Answers ``s3api`` calls from a dictionary of versions and writes the requested range to the output file, as ``get-object`` does."""

    def __init__(self, listing: dict[str, Any], payloads: dict[tuple[str, str], bytes]) -> None:
        self.listing = listing
        self.payloads = payloads
        self.calls: list[tuple[str, ...]] = []
        self.reply_version: str | None = None
        self.put_reply: dict[str, Any] = {}

    def call(self, *arguments: str, missing: Sequence[str] = (), timeout: float = 0) -> Any:
        self.calls.append(arguments)
        name = arguments[1]
        if name == "list-object-versions":
            return self.listing
        if name == "put-object":
            return self.put_reply
        options = {arguments[index]: arguments[index + 1] for index in range(2, len(arguments) - 1) if arguments[index].startswith("--")}
        data = self.payloads[(options["--key"], options["--version-id"])]
        start, _, end = options["--range"].removeprefix("bytes=").partition("-")
        piece = data[int(start) : int(end) + 1]
        Path(arguments[-1]).write_bytes(piece)
        return {"VersionId": self.reply_version or options["--version-id"], "ContentLength": len(piece)}


VERSIONS = {
    "Versions": [
        {"Key": "landing/a", "VersionId": "v1", "IsLatest": False, "Size": 3, "LastModified": "2026-01-01T00:00:01+00:00"},
        {"Key": "landing/a", "VersionId": "v2", "IsLatest": True, "Size": 5, "LastModified": "2026-01-01T00:00:02.000Z"},
    ],
    "DeleteMarkers": [{"Key": "landing/b", "VersionId": "v3", "IsLatest": True, "LastModified": "2026-01-01T00:00:03+00:00"}],
}


class ListingTest(unittest.TestCase):
    def test_versions_and_delete_markers_are_listed_with_the_time_in_one_form(self) -> None:
        store = S3Store(StubAws(VERSIONS, {}), "example-bucket")
        listed = store.list_versions("landing/")
        self.assertEqual(
            [("landing/a", "v1", 3, False, False), ("landing/a", "v2", 5, True, False), ("landing/b", "v3", 0, True, True)],
            [(item.key, item.version_id, item.size, item.is_latest, item.is_delete_marker) for item in listed],
        )
        self.assertEqual(["2026-01-01T00:00:01Z", "2026-01-01T00:00:02Z", "2026-01-01T00:00:03Z"], [item.last_modified for item in listed])

    def test_an_empty_listing_has_no_versions(self) -> None:
        self.assertEqual([], S3Store(StubAws({}, {}), "example-bucket").list_versions("landing/"))

    def test_a_key_only_matching_by_prefix_is_not_the_key(self) -> None:
        aws = StubAws(VERSIONS, {})
        S3Store(aws, "example-bucket").list_versions("landing/a")
        self.assertIn("--prefix", aws.calls[0])


class WriteTest(unittest.TestCase):
    def test_a_write_that_returns_no_version_id_is_refused_because_versioning_is_off(self) -> None:
        for reply in ({}, {"ETag": "x"}, {"VersionId": "null"}):
            with self.subTest(reply=reply):
                aws = StubAws(VERSIONS, {})
                aws.put_reply = reply
                with self.assertRaisesRegex(StoreError, "no version ID"):
                    S3Store(aws, "example-bucket").put_object("manifests/x", b"{}")

    def test_a_write_returns_the_version_the_listing_shows_for_the_returned_id(self) -> None:
        aws = StubAws(VERSIONS, {})
        aws.put_reply = {"VersionId": "v2"}
        self.assertEqual(
            ("landing/a", "v2", 5),
            (lambda version: (version.key, version.version_id, version.size))(S3Store(aws, "example-bucket").put_object("landing/a", b"12345")),
        )


class RangedReadTest(unittest.TestCase):
    def test_a_version_is_read_in_contiguous_ranges_of_that_exact_version_and_no_more_than_a_range_is_held(self) -> None:
        data = bytes(range(256)) * 20
        aws = StubAws(VERSIONS, {("landing/a", "v2"): data})
        store = S3Store(aws, "example-bucket", range_bytes=1000, block_bytes=300)
        version = store.list_versions("landing/")[1]
        version = replace(version, size=len(data))
        blocks = list(store.iter_version(version))
        self.assertEqual(data, b"".join(blocks))
        self.assertLessEqual(max(len(block) for block in blocks), 300)
        ranges = [call[call.index("--range") + 1] for call in aws.calls if call[1] == "get-object"]
        self.assertEqual(["bytes=0-999", "bytes=1000-1999", "bytes=2000-2999", "bytes=3000-3999", "bytes=4000-4999", "bytes=5000-5119"], ranges)
        self.assertTrue(all(call[call.index("--version-id") + 1] == "v2" for call in aws.calls if call[1] == "get-object"))

    def test_a_reply_for_another_version_is_refused(self) -> None:
        aws = StubAws(VERSIONS, {("landing/a", "v2"): b"12345"})
        aws.reply_version = "v1"
        store = S3Store(aws, "example-bucket")
        with self.assertRaisesRegex(StoreError, "another version"):
            list(store.iter_version(store.list_versions("landing/")[1]))

    def test_a_short_range_is_refused(self) -> None:
        aws = StubAws(VERSIONS, {("landing/a", "v2"): b"123"})
        store = S3Store(aws, "example-bucket")
        with self.assertRaisesRegex(StoreError, "bytes"):
            list(store.iter_version(store.list_versions("landing/")[1]))


if __name__ == "__main__":
    unittest.main()
