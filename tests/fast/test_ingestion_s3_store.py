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

from scripts.aws_cli import AwsError
from scripts.ingestion.store import S3Store, StoreError


class StubAws:
    """Answers ``s3api`` calls from a dictionary of versions and writes the requested range to the output file, as ``get-object`` does."""

    def __init__(self, listing: dict[str, Any], payloads: dict[tuple[str, str], bytes]) -> None:
        self.listing = listing
        self.payloads = payloads
        self.calls: list[tuple[str, ...]] = []
        self.reply_version: str | None = None
        self.put_reply: dict[str, Any] = {}
        self.targets: list[Path] = []
        self.write_nothing = False
        self.omit_length = False
        self.truncate_file = False
        self.raw_reply: Any = None

    def call(self, *arguments: str, missing: Sequence[str] = (), timeout: float = 0) -> Any:
        self.calls.append(arguments)
        name = arguments[1]
        if name == "get-bucket-versioning":
            return {"Status": "Enabled"}
        if name == "list-object-versions":
            return self.listing
        if name == "put-object":
            return self.put_reply
        options = {arguments[index]: arguments[index + 1] for index in range(2, len(arguments) - 1) if arguments[index].startswith("--")}
        data = self.payloads[(options["--key"], options["--version-id"])]
        start, _, end = options.get("--range", f"bytes=0-{len(data) - 1}").removeprefix("bytes=").partition("-")
        piece = data[int(start) : int(end) + 1]
        self.targets.append(Path(arguments[-1]))
        if not self.write_nothing:
            Path(arguments[-1]).write_bytes(piece[:-1] if self.truncate_file else piece)
        if self.raw_reply is not None:
            return self.raw_reply
        reply: dict[str, Any] = {"VersionId": self.reply_version or options["--version-id"]}
        if not self.omit_length:
            reply["ContentLength"] = len(piece)
        return reply


class WriteStubAws:
    """A bucket as the AWS command line meets it: versioned writes, the listing, the reads and a ``put-object`` that honours ``--if-none-match``.

    ``lose_first_reply`` plays the lost reply: the first request is stored, its reply never arrives and the command line sends the same request again, as
    it does by default. What the caller sees is the answer to that second request.
    """

    def __init__(self, versioning: str | None = "Enabled") -> None:
        self.versioning = versioning
        self.versions: dict[str, list[tuple[str, bytes]]] = {}
        self.calls: list[tuple[str, ...]] = []
        self.lose_first_reply = False

    def call(self, *arguments: str, missing: Sequence[str] = (), timeout: float = 0) -> Any:
        self.calls.append(arguments)
        name = arguments[1]
        options = {arguments[index]: arguments[index + 1] for index in range(2, len(arguments) - 1) if arguments[index].startswith("--")}
        if name == "get-bucket-versioning":
            return {"Status": self.versioning} if self.versioning else {}
        if name == "put-object":
            return self.put(options, "--if-none-match" in arguments, missing)
        if name == "list-object-versions":
            found = [
                {"Key": key, "VersionId": version, "IsLatest": position == len(items) - 1, "Size": len(data), "LastModified": "2026-01-01T00:00:00+00:00"}
                for key, items in self.versions.items()
                if key.startswith(options["--prefix"])
                for position, (version, data) in enumerate(items)
            ]
            return {"Versions": found}
        data = dict(self.versions[options["--key"]])[options["--version-id"]]
        Path(arguments[-1]).write_bytes(data)
        return {"VersionId": options["--version-id"], "ContentLength": len(data)}

    def put(self, options: dict[str, str], conditional: bool, missing: Sequence[str]) -> Any:
        reply: Any = None
        for _ in range(2 if self.lose_first_reply else 1):
            items = self.versions.setdefault(options["--key"], [])
            if conditional and items:
                if "PreconditionFailed" not in missing:
                    raise AwsError("aws s3api put-object failed: PreconditionFailed")
                reply = None
                continue
            version = f"v{sum(len(stored) for stored in self.versions.values()) + 1}"
            items.append((version, Path(options["--body"]).read_bytes()))
            reply = {"VersionId": version}
        return reply

    def stored(self, key: str) -> list[bytes]:
        return [data for _, data in self.versions.get(key, [])]


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

    def test_a_listing_that_is_not_in_the_documented_form_is_refused_with_a_fixed_message(self) -> None:
        entry = VERSIONS["Versions"][1]
        without_size = {name: value for name, value in entry.items() if name != "Size"}
        listings: list[Any] = [
            [],
            {"Versions": 5},
            {"Versions": [5]},
            {"Versions": [without_size]},
            {"Versions": [{**entry, "Size": "5"}]},
            {"Versions": [{**entry, "Size": True}]},
            {"Versions": [{**entry, "IsLatest": "true"}]},
            {"Versions": [{**entry, "LastModified": "private-value"}]},
            {"Versions": [{**entry, "Key": 5}]},
            {"Versions": [{**entry, "VersionId": None}]},
            {"DeleteMarkers": [{"Key": "private-value", "VersionId": "v3", "IsLatest": True}]},
        ]
        for number, listing in enumerate(listings):
            with self.subTest(listing=number), self.assertRaisesRegex(StoreError, "documented form") as raised:
                S3Store(StubAws(listing, {}), "example-bucket").list_versions("landing/")
            self.assertNotIn("private-value", str(raised.exception))

    def test_a_reply_that_is_not_an_object_is_refused(self) -> None:
        aws = StubAws(VERSIONS, {("landing/a", "v2"): b"12345"})
        aws.put_reply = ["private-value"]  # type: ignore[assignment]
        with self.assertRaisesRegex(StoreError, "documented form"):
            S3Store(aws, "example-bucket").create_object("manifests/x", b"{}")
        aws.raw_reply = ["private-value"]
        store = S3Store(aws, "example-bucket")
        with self.assertRaisesRegex(StoreError, "documented form"):
            store.get_version_bytes(store.list_versions("landing/")[1], 100)


class WriteTest(unittest.TestCase):
    def test_a_write_that_returns_no_version_id_is_refused_because_versioning_is_off(self) -> None:
        for reply in ({}, {"ETag": "x"}, {"VersionId": "null"}):
            with self.subTest(reply=reply):
                aws = StubAws(VERSIONS, {})
                aws.put_reply = reply
                with self.assertRaisesRegex(StoreError, "no version ID"):
                    S3Store(aws, "example-bucket").create_object("manifests/x", b"{}")

    def test_a_write_returns_the_version_the_listing_shows_for_the_returned_id(self) -> None:
        aws = StubAws(VERSIONS, {})
        aws.put_reply = {"VersionId": "v2"}
        self.assertEqual(
            ("landing/a", "v2", 5),
            (lambda version: (version.key, version.version_id, version.size))(S3Store(aws, "example-bucket").create_object("landing/a", b"12345")),
        )


class ConditionalWriteTest(unittest.TestCase):
    KEY = "manifests/set/abc.json"

    def test_control_a_write_makes_one_version_and_asks_for_it_only_when_the_key_is_new(self) -> None:
        aws = WriteStubAws()
        version = S3Store(aws, "example-bucket").create_object(self.KEY, b"{}")
        self.assertEqual([b"{}"], aws.stored(self.KEY))
        self.assertEqual(self.KEY, version.key)
        put = next(call for call in aws.calls if call[1] == "put-object")
        self.assertEqual("*", put[put.index("--if-none-match") + 1])

    def test_a_lost_reply_that_the_command_line_retries_leaves_one_version_and_returns_it(self) -> None:
        aws = WriteStubAws()
        aws.lose_first_reply = True
        version = S3Store(aws, "example-bucket").create_object(self.KEY, b"{}")
        self.assertEqual([b"{}"], aws.stored(self.KEY))
        self.assertEqual(aws.versions[self.KEY][0][0], version.version_id)

    def test_a_key_that_already_holds_the_same_bytes_returns_its_version_and_writes_nothing_more(self) -> None:
        aws = WriteStubAws()
        store = S3Store(aws, "example-bucket")
        first = store.create_object(self.KEY, b"{}")
        self.assertEqual(first, store.create_object(self.KEY, b"{}"))
        self.assertEqual([b"{}"], aws.stored(self.KEY))

    def test_a_key_that_holds_other_bytes_is_refused_and_left_alone(self) -> None:
        for other in (b"[]", b"{} "):
            aws = WriteStubAws()
            aws.versions[self.KEY] = [("v0", other)]
            with self.subTest(other=other), self.assertRaisesRegex(StoreError, "other content"):
                S3Store(aws, "example-bucket").create_object(self.KEY, b"{}")
            self.assertEqual([other], aws.stored(self.KEY))

    def test_a_key_whose_current_version_is_a_null_version_is_refused_although_it_holds_the_same_bytes(self) -> None:
        for null in ("null", ""):
            aws = WriteStubAws()
            aws.versions[self.KEY] = [(null, b"{}")]
            with self.subTest(null=null), self.assertRaisesRegex(StoreError, "null version"):
                S3Store(aws, "example-bucket").create_object(self.KEY, b"{}")
            self.assertEqual([b"{}"], aws.stored(self.KEY))
            self.assertEqual([], [call for call in aws.calls if call[1] == "get-object"])

    def test_a_bucket_whose_versioning_is_not_enabled_is_refused_before_anything_is_written(self) -> None:
        for status in (None, "Suspended"):
            aws = WriteStubAws(status)
            with self.subTest(status=status), self.assertRaisesRegex(StoreError, "versioning is not enabled"):
                S3Store(aws, "example-bucket").create_object(self.KEY, b"{}")
            self.assertEqual([], [call for call in aws.calls if call[1] == "put-object"])
            self.assertEqual({}, aws.versions)


class TemporaryFileTest(unittest.TestCase):
    def read_all(self, aws: StubAws) -> None:
        store = S3Store(aws, "example-bucket", range_bytes=2, block_bytes=1)
        list(store.iter_version(store.list_versions("landing/")[1]))

    def test_control_the_range_folder_is_gone_after_a_complete_read(self) -> None:
        aws = StubAws(VERSIONS, {("landing/a", "v2"): b"12345"})
        self.read_all(aws)
        self.assertTrue(aws.targets)
        self.assertFalse(any(target.parent.exists() for target in aws.targets))

    def test_the_range_folder_is_gone_after_each_kind_of_failure(self) -> None:
        for name in ("short reply", "no file written", "no length", "another version", "file shorter than the reply"):
            with self.subTest(name):
                aws = StubAws(VERSIONS, {("landing/a", "v2"): b"123" if name == "short reply" else b"12345"})
                aws.write_nothing = name == "no file written"
                aws.omit_length = name == "no length"
                aws.truncate_file = name == "file shorter than the reply"
                aws.reply_version = "v1" if name == "another version" else None
                with self.assertRaises(StoreError):
                    self.read_all(aws)
                self.assertTrue(aws.targets)
                self.assertFalse(any(target.parent.exists() for target in aws.targets))

    def test_the_range_folder_is_gone_when_the_reader_stops_early(self) -> None:
        aws = StubAws(VERSIONS, {("landing/a", "v2"): b"12345"})
        store = S3Store(aws, "example-bucket", range_bytes=2, block_bytes=1)
        blocks = store.iter_version(store.list_versions("landing/")[1])
        next(blocks)
        blocks.close()
        self.assertFalse(any(target.parent.exists() for target in aws.targets))

    def test_a_whole_read_is_refused_when_the_reply_gives_no_byte_count_or_wrote_no_data(self) -> None:
        for name in ("no length", "no file written"):
            with self.subTest(name):
                aws = StubAws(VERSIONS, {("landing/a", "v2"): b"12345"})
                aws.omit_length = name == "no length"
                aws.write_nothing = name == "no file written"
                store = S3Store(aws, "example-bucket")
                with self.assertRaises(StoreError):
                    store.get_version_bytes(store.list_versions("landing/")[1], 100)
                self.assertFalse(any(target.parent.exists() for target in aws.targets))

    def test_a_whole_read_of_a_small_object_also_leaves_no_folder_and_refuses_a_larger_one(self) -> None:
        aws = StubAws(VERSIONS, {})
        store = S3Store(aws, "example-bucket")
        with self.assertRaisesRegex(StoreError, "larger than"):
            store.get_version_bytes(store.list_versions("landing/")[1], 4)


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
