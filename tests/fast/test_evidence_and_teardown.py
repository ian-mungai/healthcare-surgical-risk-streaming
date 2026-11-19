"""The checks that must refuse before any deletion: the evidence readback, the owner's receipt and the teardown guards.

The export, the signature tool, Terraform and S3 are proven in the live batch and its isolated fixtures. These cases
cover each refusal as a pure decision, each with a passing control and each failing for its own stated cause.
"""

from __future__ import annotations

import json
import tarfile
import tempfile
import unittest
from pathlib import Path
from typing import Any

from scripts.evidence.manifest import REQUIRED_EVIDENCE, archive_problems, build_manifest, manifest_problems, manifest_sha256, readback_problems
from scripts.evidence.receipt import receipt_problems
from scripts.evidence.verify import verify_export
from scripts.infra.state_bootstrap import RunBinding
from scripts.infra.teardown import bucket_entry_problems, delete_targets, settled_checks, state_entry_problems, target_problems

STATE_KEYS = ["environment/terraform.tfstate", "environment/terraform.tfstate.tflock"]
RUN = RunBinding("20260101t0000-abcdef", "c0ffee", "s3", "us-west-2", "example-state", "default", STATE_KEYS, "verified", state_bucket_creation="created")
LOCAL = RunBinding("20260101t0000-abcdef", "c0ffee", "local", "us-west-2", "", "default", ["/srv/run/terraform.tfstate"], "verified", provision_host=False)
GIVEN_HOST = RunBinding(**{**RUN.__dict__, "provision_host": False})
HASH = "a" * 64


def receipt(**changes: str) -> bytes:
    return json.dumps({"run_id": RUN.run_id, "commit": RUN.commit, "manifest_sha256": HASH} | changes).encode()


class ReadbackTest(unittest.TestCase):
    def setUp(self) -> None:
        self.folder = Path(tempfile.mkdtemp(prefix="evidence_"))
        (self.folder / "plans").mkdir()
        (self.folder / "inventory.json").write_text("{}")
        (self.folder / "plans" / "apply.txt").write_text("plan")
        self.manifest = build_manifest(self.folder, RUN.run_id, RUN.commit)

    def test_manifest_lists_every_file_with_its_hash(self) -> None:
        self.assertEqual(["inventory.json", "plans/apply.txt"], [item["path"] for item in self.manifest["files"]])
        self.assertEqual(64, len(manifest_sha256(self.manifest)))

    def test_complete_readback_passes(self) -> None:
        self.assertEqual([], readback_problems(self.manifest, self.folder))

    def test_missing_file_is_an_incomplete_readback(self) -> None:
        (self.folder / "inventory.json").unlink()
        self.assertEqual(["missing from the readback: inventory.json"], readback_problems(self.manifest, self.folder))

    def test_changed_file_is_a_corrupt_readback(self) -> None:
        (self.folder / "plans" / "apply.txt").write_text("other")
        self.assertEqual(["differs from its manifest hash: plans/apply.txt"], readback_problems(self.manifest, self.folder))

    def test_unlisted_file_is_reported(self) -> None:
        (self.folder / "extra.txt").write_text("extra")
        self.assertEqual(["not in the manifest: extra.txt"], readback_problems(self.manifest, self.folder))


class ManifestShapeTest(unittest.TestCase):
    def manifest(self, *paths: str) -> dict[str, object]:
        return {"run_id": RUN.run_id, "commit": RUN.commit, "files": [{"path": path, "sha256": HASH, "bytes": 1} for path in paths]}

    def test_manifest_with_every_required_file_passes(self) -> None:
        self.assertEqual([], manifest_problems(self.manifest(*REQUIRED_EVIDENCE, "acceptance/extra.json")))

    def test_missing_required_file_is_refused(self) -> None:
        kept = [path for path in REQUIRED_EVIDENCE if path != "inventory.json"]
        self.assertEqual(["required evidence is not in the manifest: inventory.json"], manifest_problems(self.manifest(*kept)))

    def test_path_that_leaves_the_folder_is_refused(self) -> None:
        for path in ("../outside.json", "/etc/hosts", "plans/../../outside.json", ""):
            with self.subTest(path=path):
                self.assertIn(f"manifest path is not a plain relative path: {path!r}", manifest_problems(self.manifest(*REQUIRED_EVIDENCE, path)))

    def test_duplicate_path_is_refused(self) -> None:
        self.assertEqual(["manifest lists a path twice: inventory.json"], manifest_problems(self.manifest(*REQUIRED_EVIDENCE, "inventory.json")))

    def test_archive_members_must_equal_the_manifest(self) -> None:
        manifest = self.manifest("a.json", "b.json")
        self.assertEqual([], archive_problems(manifest, ["b.json", "a.json"]))
        self.assertEqual(["in the archive and not in the manifest: c.json"], archive_problems(manifest, ["a.json", "b.json", "c.json"]))
        self.assertEqual(["in the manifest and not in the archive: b.json"], archive_problems(manifest, ["a.json"]))


class VerifyExportTest(unittest.TestCase):
    """The whole verification with a stand-in signer: a failed readback must leave no earlier receipt."""

    def setUp(self) -> None:
        self.folder = Path(tempfile.mkdtemp(prefix="verify_"))
        for path in REQUIRED_EVIDENCE:
            (self.folder / path).parent.mkdir(parents=True, exist_ok=True)
            (self.folder / path).write_text(path)
        self.export = self.folder / "export"
        self.export.mkdir()
        self.pack(build_manifest(self.folder, RUN.run_id, RUN.commit))

    def pack(self, manifest: dict[str, Any], skip: str = "") -> None:
        (self.export / "manifest.json").write_text(json.dumps(manifest))
        with tarfile.open(self.export / "evidence.tar.gz", "w:gz") as archive:
            for item in manifest["files"]:
                if item["path"] != skip:
                    archive.add(self.folder / item["path"], arcname=item["path"])

    def sign(self, receipt: Path) -> bool:
        receipt.with_name(receipt.name + ".sig").write_text("signed")
        return True

    def test_complete_export_gets_a_signed_receipt(self) -> None:
        self.assertEqual([], verify_export(self.folder, RUN, self.sign))
        self.assertTrue((self.export / "receipt.json").is_file() and (self.export / "receipt.json.sig").is_file())

    def test_failed_readback_removes_the_earlier_receipt(self) -> None:
        self.assertEqual([], verify_export(self.folder, RUN, self.sign))
        self.pack(build_manifest(self.folder, RUN.run_id, RUN.commit), skip="inventory.json")
        self.assertEqual(["in the manifest and not in the archive: inventory.json"], verify_export(self.folder, RUN, self.sign))
        self.assertFalse((self.export / "receipt.json").exists() or (self.export / "receipt.json.sig").exists())

    def test_corrupt_archive_removes_the_earlier_receipt(self) -> None:
        self.assertEqual([], verify_export(self.folder, RUN, self.sign))
        (self.export / "evidence.tar.gz").write_bytes(b"not an archive")
        self.assertEqual(["the archive cannot be read"], verify_export(self.folder, RUN, self.sign))
        self.assertFalse((self.export / "receipt.json").exists())

    def test_failed_signing_leaves_no_receipt(self) -> None:
        self.assertEqual(["the receipt could not be signed"], verify_export(self.folder, RUN, lambda receipt: False))
        self.assertFalse((self.export / "receipt.json").exists())


class ReceiptTest(unittest.TestCase):
    def test_genuine_current_receipt_passes(self) -> None:
        self.assertEqual([], receipt_problems(receipt(), True, RUN, HASH))

    def test_absent_receipt_is_refused(self) -> None:
        self.assertEqual(["no receipt: the evidence readback is not verified on the owner's computer"], receipt_problems(None, False, RUN, HASH))

    def test_corrupt_receipt_is_refused(self) -> None:
        self.assertEqual(["the receipt is corrupt: it is not a JSON record"], receipt_problems(b"{not json", True, RUN, HASH))

    def test_receipt_without_the_owner_signature_is_refused(self) -> None:
        self.assertEqual(["the receipt is not signed by the owner's verifier"], receipt_problems(receipt(), False, RUN, HASH))

    def test_stale_receipt_is_refused(self) -> None:
        self.assertEqual(
            ["the receipt is stale: it verifies a different export manifest"], receipt_problems(receipt(manifest_sha256="b" * 64), True, RUN, HASH)
        )

    def test_receipt_of_another_run_or_commit_is_refused(self) -> None:
        self.assertEqual(["the receipt belongs to another run"], receipt_problems(receipt(run_id="20250101t0000-abcdef"), True, RUN, HASH))
        self.assertEqual(["the receipt belongs to another commit"], receipt_problems(receipt(commit="deadbeef"), True, RUN, HASH))


def change(address: str, kind: str, identifier: str, actions: list[str]) -> dict[str, object]:
    return {"address": address, "type": kind, "change": {"actions": actions, "before": {"id": identifier}}}


PLAN = {
    "resource_changes": [
        change("module.host[0].aws_instance.host", "aws_instance", "i-0aaa", ["delete"]),
        change('module.storage.aws_s3_bucket.this["landing"]', "aws_s3_bucket", "example-landing", ["delete"]),
        change("module.deploy.aws_iam_role.deploy", "aws_iam_role", "example_deploy", ["no-op"]),
    ]
}
INVENTORY = [
    {"address": "module.host[0].aws_instance.host", "type": "aws_instance", "service_id": "i-0aaa"},
    {"address": 'module.storage.aws_s3_bucket.this["landing"]', "type": "aws_s3_bucket", "service_id": "example-landing"},
    {"address": "state bucket", "type": "aws_s3_bucket", "service_id": "example-state"},
]


class TargetTest(unittest.TestCase):
    def test_delete_targets_are_read_from_the_plan(self) -> None:
        self.assertEqual(
            [
                ("module.host[0].aws_instance.host", "aws_instance", "i-0aaa"),
                ('module.storage.aws_s3_bucket.this["landing"]', "aws_s3_bucket", "example-landing"),
            ],
            delete_targets(PLAN),
        )

    def test_targets_in_the_inventory_pass(self) -> None:
        self.assertEqual([], target_problems(delete_targets(PLAN), INVENTORY, RUN, []))

    def test_target_outside_the_inventory_is_refused(self) -> None:
        foreign = {"resource_changes": [change("module.host[0].aws_instance.host", "aws_instance", "i-0bbb", ["delete"])]}
        self.assertEqual(
            ["delete target outside this run's inventory: module.host[0].aws_instance.host"], target_problems(delete_targets(foreign), INVENTORY, RUN, [])
        )

    def test_pre_provisioned_host_is_refused(self) -> None:
        self.assertEqual(
            ["delete target is a host that existed before this run: module.host[0].aws_instance.host"],
            target_problems(delete_targets(PLAN), INVENTORY, RUN, ["i-0aaa"]),
        )

    def test_any_host_is_refused_in_a_run_that_was_given_its_host(self) -> None:
        local_inventory = [entry for entry in INVENTORY if entry["address"] != "state bucket"]
        expected = ["delete target is a host in a run that did not create its host: module.host[0].aws_instance.host"]
        self.assertEqual(expected, target_problems(delete_targets(PLAN), local_inventory, LOCAL, []))
        self.assertEqual(expected, target_problems(delete_targets(PLAN), INVENTORY, GIVEN_HOST, []))

    def test_state_bucket_must_be_the_one_this_run_created(self) -> None:
        other = [entry | {"service_id": "other-state"} if entry["address"] == "state bucket" else entry for entry in INVENTORY]
        self.assertEqual(["the bound state bucket is not the state bucket in this run's inventory"], target_problems(delete_targets(PLAN), other, RUN, []))

    def test_state_bucket_the_run_did_not_create_is_refused(self) -> None:
        adopted = RunBinding(**{**RUN.__dict__, "state_bucket_creation": "none"})
        self.assertEqual(["the state bucket was not created by this run"], target_problems(delete_targets(PLAN), INVENTORY, adopted, []))

    def test_state_bucket_target_is_refused_under_the_local_backend(self) -> None:
        only_buckets = {"resource_changes": [change('module.storage.aws_s3_bucket.this["landing"]', "aws_s3_bucket", "example-landing", ["delete"])]}
        expected = ["a state bucket is named for removal under the local backend"]
        self.assertEqual(expected, target_problems(delete_targets(only_buckets), INVENTORY, LOCAL, []))
        local_inventory = [entry for entry in INVENTORY if entry["address"] != "state bucket"]
        self.assertEqual([], target_problems(delete_targets(only_buckets), local_inventory, LOCAL, []))


def entry(key: str, version: str) -> dict[str, str]:
    return {"Key": key, "VersionId": version}


class ObjectInventoryTest(unittest.TestCase):
    def test_approved_application_entries_pass(self) -> None:
        approved = [["example-landing", "a.vital", "v1"], ["example-landing", "a.vital", "v2"]]
        self.assertEqual([], bucket_entry_problems("example-landing", [entry("a.vital", "v1"), entry("a.vital", "v2")], approved))

    def test_unapproved_object_is_refused(self) -> None:
        approved = [["example-landing", "a.vital", "v1"]]
        self.assertEqual(
            ["example-landing: entry outside the approved inventory: b.vital"],
            bucket_entry_problems("example-landing", [entry("a.vital", "v1"), entry("b.vital", "v9")], approved),
        )

    def test_unapproved_version_of_an_approved_key_is_refused(self) -> None:
        approved = [["example-warehouse", "table/data.parquet", "v1"]]
        found = [entry("table/data.parquet", "v1"), entry("table/data.parquet", "v2")]
        self.assertEqual(
            ["example-warehouse: entry outside the approved inventory: table/data.parquet"], bucket_entry_problems("example-warehouse", found, approved)
        )

    def test_entry_without_a_version_id_is_refused(self) -> None:
        self.assertEqual(
            ["example-landing: entry with no version ID: a.vital"],
            bucket_entry_problems("example-landing", [entry("a.vital", "null")], [["example-landing", "a.vital", "null"]]),
        )

    def test_approved_state_entries_pass(self) -> None:
        approved = [[STATE_KEYS[0], "v1"], [STATE_KEYS[1], "v2"]]
        self.assertEqual([], state_entry_problems([entry(STATE_KEYS[0], "v1"), entry(STATE_KEYS[1], "v2")], STATE_KEYS, approved))

    def test_foreign_state_key_is_refused(self) -> None:
        self.assertEqual(
            ["state bucket: key outside this run's state keys: other/terraform.tfstate"],
            state_entry_problems([entry("other/terraform.tfstate", "v1")], STATE_KEYS, [["other/terraform.tfstate", "v1"]]),
        )

    def test_unapproved_noncurrent_state_version_is_refused(self) -> None:
        approved = [[STATE_KEYS[0], "v2"]]
        expected = [f"state bucket: version outside the approved inventory: {STATE_KEYS[0]}"]
        self.assertEqual(expected, state_entry_problems([entry(STATE_KEYS[0], "v1"), entry(STATE_KEYS[0], "v2")], STATE_KEYS, approved))

    def test_entries_written_by_the_destroy_apply_pass_once_recorded(self) -> None:
        approved = [[STATE_KEYS[0], "v1"]]
        found = [entry(STATE_KEYS[0], "v1"), entry(STATE_KEYS[0], "v3")]
        self.assertEqual([], state_entry_problems(found, STATE_KEYS, approved, [[STATE_KEYS[0], "v3"]]))


class RemovalCheckTest(unittest.TestCase):
    def test_a_removal_that_the_service_reports_late_is_confirmed(self) -> None:
        answers = {"example-landing": iter(["still exists", "still exists", "gone"]), "i-0aaa": iter(["gone"]), "example-state": iter(["gone"])}
        checks = settled_checks(lambda entry: next(answers[entry["service_id"]]), INVENTORY, attempts=5, pause_seconds=0)
        self.assertEqual(["gone", "gone", "gone"], [check["reported"] for check in checks])

    def test_a_resource_that_stays_is_reported_after_the_last_attempt(self) -> None:
        checks = settled_checks(lambda entry: "still exists" if entry["service_id"] == "i-0aaa" else "gone", INVENTORY, attempts=3, pause_seconds=0)
        self.assertEqual(["still exists", "gone", "gone"], [check["reported"] for check in checks])


if __name__ == "__main__":
    unittest.main()
