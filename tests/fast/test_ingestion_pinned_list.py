"""Every selection names the publisher's checksum list by the tracked pin's SHA-256, so a substitute list never verifies anything.

A substitute list here is synthetic: it lists the SHA-256 of bytes that the test lands in the stand-in store, so every other check of the commands
would pass with it. The commands run through ``main`` against the tracked pin, with the stand-in store in the place of S3.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.ingestion import cli
from scripts.ingestion.check import canonical, result_digest
from scripts.ingestion.selection import keys_digest, selection_digest
from tests.fast.memory_store import MemoryStore

PIN = json.loads(Path("tests/fixtures/vitaldb/release.json").read_text())
PREFIX = PIN["source"]["release_path"]
RELEASE = PREFIX.strip("/").split("/")[-1]
CONFIGURATION = {"AWS_PROFILE": "example_profile", "AWS_REGION": "us-west-2", "S3_BUCKET": "example-bucket"}
SUBSTITUTE = {"clinical_data.csv": b"caseid,age\n1,60\n", "vital_files/0001.vital": b"a synthetic recording that the publisher never had"}
PERIOD = "2016-08 to 2017-06"


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class SubstituteListTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="ingestion_pinned_list_")
        self.addCleanup(folder.cleanup)
        self.folder = Path(folder.name)
        self.store = MemoryStore()
        self.list_bytes = "".join(f"{sha256(data)} {path}\n" for path, data in sorted(SUBSTITUTE.items())).encode()
        self.versions = {
            path: self.store.put_object(f"landing/sync_{index:02d}/{PREFIX}{path}", data) for index, (path, data) in enumerate(sorted(SUBSTITUTE.items()))
        }
        selection = {
            "name": "probe_set",
            "release": RELEASE,
            "source_bucket": PIN["source"]["bucket"],
            "source_prefix": PREFIX,
            "measurement_period": PERIOD,
            "checksum_list": {"key": PIN["checksum_list"]["key"], "sha256": sha256(self.list_bytes)},
            "files": sorted(SUBSTITUTE),
        }
        pairings = [{"source_key": PREFIX + path, "landed_key": version.key, "size": version.size} for path, version in self.versions.items()]
        for name, text in (("selection.json", json.dumps(selection)), ("pairings.json", json.dumps(pairings))):
            (self.folder / name).write_text(text)
        (self.folder / "SHA256SUMS.txt").write_bytes(self.list_bytes)

    def run_command(self, *arguments: str) -> tuple[int, str, str]:
        output, errors = io.StringIO(), io.StringIO()
        with (
            mock.patch.object(cli, "load_configuration", return_value=CONFIGURATION),
            mock.patch.object(cli, "S3Store", return_value=self.store),
            contextlib.redirect_stdout(output),
            contextlib.redirect_stderr(errors),
        ):
            code = cli.main(list(arguments))
        return code, output.getvalue(), errors.getvalue()

    def path(self, name: str) -> str:
        return str(self.folder / name)

    def test_a_subset_selection_that_pins_a_substitute_list_is_refused_before_any_read_and_publishes_nothing(self) -> None:
        check = ["check", "--selection", self.path("selection.json"), "--checksum-list", self.path("SHA256SUMS.txt")]
        code, _, errors = self.run_command(*check, "--pairings", self.path("pairings.json"), "--result", self.path("result.json"))
        self.assertEqual((1, "the selection's checksum_list_sha256 is not the tracked pin's\n"), (code, errors))
        self.assertEqual([], self.store.reads)
        self.assertFalse((self.folder / "result.json").exists())

    def test_a_manifest_bound_to_a_substitute_list_is_refused_by_the_gate_although_its_digests_agree(self) -> None:
        entries = [
            {
                "bucket": self.store.bucket,
                "key": version.key,
                "version_id": version.version_id,
                "sha256": sha256(SUBSTITUTE[path]),
                "source_key": PREFIX + path,
                "size": version.size,
                "release": RELEASE,
                "retrieved_utc": version.last_modified,
            }
            for path, version in sorted(self.versions.items())
        ]
        listed = {"key": PIN["checksum_list"]["key"], "sha256": sha256(self.list_bytes)}
        keys = keys_digest(entry["source_key"] for entry in entries)
        selection_sha256 = selection_digest("probe_set", RELEASE, PIN["source"]["bucket"], PREFIX, PERIOD, listed["key"], listed["sha256"], keys)
        selection: dict[str, Any] = {
            "name": "probe_set",
            "source_bucket": PIN["source"]["bucket"],
            "source_prefix": PREFIX,
            "file_count": len(entries),
            "keys_sha256": keys,
            "selection_sha256": selection_sha256,
        }
        document = {
            "format": "landing_manifest_2",
            "status": "passed",
            "measurement_period": PERIOD,
            "selection": selection,
            "checksum_list": listed,
            "check_result_sha256": result_digest(selection_sha256, listed["sha256"], entries),
            "entries": entries,
        }
        data = canonical(document)
        key = f"manifests/probe_set/{sha256(data)}.json"
        version = self.store.put_object(key, data)
        (self.folder / "reference.json").write_text(
            json.dumps({"bucket": self.store.bucket, "key": key, "version_id": version.version_id, "sha256": sha256(data)})
        )
        code, output, errors = self.run_command("verify", "--reference", self.path("reference.json"))
        self.assertEqual((1, ""), (code, output))
        self.assertEqual("refused before any payload was read\nthe manifest's checksum list is not the tracked pin's\n", errors)
        self.assertEqual([], self.store.payload_reads("landing/"))


if __name__ == "__main__":
    unittest.main()
