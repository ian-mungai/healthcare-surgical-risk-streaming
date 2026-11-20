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
from scripts.evidence.verify import export_problem, verify_export
from scripts.infra.host import HostError
from scripts.infra.state_bootstrap import RunBinding
from scripts.infra.teardown import (
    APPROVAL_FILES,
    bucket_entry_problems,
    classify_entries,
    consumed_problems,
    delete_targets,
    destroy_write_problem,
    device_check,
    local_state_problem,
    other_entry_problems,
    parent_check,
    progress_problem,
    remaining_stages,
    settled_checks,
    state_entry_problems,
    target_problems,
)

STATE_KEYS = ["state/dev/terraform.tfstate", "state/dev/terraform.tfstate.tflock"]
RUN = RunBinding("20260101t0000-abcdef", "c0ffee", "s3", "us-west-2", "example-project", "default", STATE_KEYS, "verified", bucket_creation="created")
LOCAL = RunBinding(
    "20260101t0000-abcdef",
    "c0ffee",
    "local",
    "us-west-2",
    "example-project",
    "default",
    ["/srv/run/terraform.tfstate"],
    "verified",
    provision_host=False,
    bucket_creation="created",
)
GIVEN_HOST = RunBinding(**{**RUN.__dict__, "provision_host": False})
HASH = "a" * 64


def receipt(**changes: str) -> bytes:
    return json.dumps({"run_id": RUN.run_id, "commit": RUN.commit, "manifest_sha256": HASH} | changes).encode()


class ReadbackTest(unittest.TestCase):
    def setUp(self) -> None:
        self.folder = Path(tempfile.mkdtemp(prefix="evidence_"))
        for folder in ("plans", "raw", "acceptance"):
            (self.folder / folder).mkdir()
        (self.folder / "inventory.json").write_text("{}")
        (self.folder / "plans" / "apply_binding.json").write_text("binding")
        (self.folder / "plans" / "apply.txt").write_text("raw plan text with private values")
        (self.folder / "plans" / "apply.tfplan").write_text("raw plan")
        (self.folder / "raw" / "apply_output.txt").write_text("raw tool output")
        (self.folder / "acceptance" / "after-up.json").write_text("{}")
        self.manifest = build_manifest(self.folder, RUN.run_id, RUN.commit)

    def test_manifest_lists_only_the_named_evidence_and_never_raw_tool_files(self) -> None:
        self.assertEqual(["acceptance/after-up.json", "inventory.json", "plans/apply_binding.json"], [item["path"] for item in self.manifest["files"]])
        self.assertEqual(64, len(manifest_sha256(self.manifest)))

    def test_complete_readback_passes(self) -> None:
        self.assertEqual([], readback_problems(self.manifest, self.folder))

    def test_missing_file_is_an_incomplete_readback(self) -> None:
        (self.folder / "inventory.json").unlink()
        self.assertEqual(["missing from the readback: inventory.json"], readback_problems(self.manifest, self.folder))

    def test_changed_file_is_a_corrupt_readback(self) -> None:
        (self.folder / "plans" / "apply_binding.json").write_text("other")
        self.assertEqual(["differs from its manifest hash: plans/apply_binding.json"], readback_problems(self.manifest, self.folder))

    def test_unlisted_file_is_reported(self) -> None:
        (self.folder / "acceptance" / "extra.json").write_text("extra")
        self.assertEqual(["not in the manifest: acceptance/extra.json"], readback_problems(self.manifest, self.folder))


class ManifestShapeTest(unittest.TestCase):
    def manifest(self, *paths: str) -> dict[str, object]:
        return {"run_id": RUN.run_id, "commit": RUN.commit, "files": [{"path": path, "sha256": HASH, "bytes": 1} for path in paths]}

    def test_manifest_with_every_required_file_passes(self) -> None:
        self.assertEqual([], manifest_problems(self.manifest(*REQUIRED_EVIDENCE, "acceptance/extra.json")))

    def test_a_run_that_never_planned_a_teardown_can_be_exported(self) -> None:
        self.assertEqual([], manifest_problems(self.manifest("inventory.json", "acceptance/after-up.json")))
        self.assertEqual(("inventory.json",), REQUIRED_EVIDENCE)

    def test_the_teardown_still_requires_its_approval_files_in_the_export(self) -> None:
        files = {name: b"content" for name in APPROVAL_FILES}
        listed = self.manifest("inventory.json")
        missing = consumed_problems(listed, files)
        self.assertEqual(len(APPROVAL_FILES), len(missing))
        for name in ("plans/destroy_binding.json", "teardown/approved_inventory.json"):
            self.assertIn(f"an approval file changed while it was checked: {name}", missing)

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
        self.assertEqual(["in the archive more than once: a.json"], archive_problems(manifest, ["a.json", "b.json", "a.json"]))


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

    def test_a_missing_export_removes_the_earlier_receipt_before_anything_else(self) -> None:
        self.assertEqual([], verify_export(self.folder, RUN, self.sign))
        (self.folder / "export" / "evidence.tar.gz").unlink()
        self.assertEqual("no export to verify: run make evidence-export first", export_problem(self.folder))
        self.assertFalse((self.folder / "export" / "receipt.json").exists())
        self.assertFalse((self.folder / "export" / "receipt.json.sig").exists())

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
        change("module.host[0].aws_iam_role.host", "aws_iam_role", "example_host", ["delete"]),
        change("module.deploy.aws_iam_role.deploy", "aws_iam_role", "example_deploy", ["no-op"]),
    ]
}
INVENTORY = [
    {"address": "module.host[0].aws_instance.host", "type": "aws_instance", "service_id": "i-0aaa"},
    {"address": "module.host[0].aws_iam_role.host", "type": "aws_iam_role", "service_id": "example_host"},
    {"address": "project bucket", "type": "aws_s3_bucket", "service_id": "example-project"},
    {"address": "tailscale device", "type": "tailscale_device", "service_id": "nNEWCNTRL", "host_name": "platform-dev"},
]


class TargetTest(unittest.TestCase):
    def test_delete_targets_are_read_from_the_plan(self) -> None:
        self.assertEqual(
            [
                ("module.host[0].aws_instance.host", "aws_instance", "i-0aaa"),
                ("module.host[0].aws_iam_role.host", "aws_iam_role", "example_host"),
            ],
            delete_targets(PLAN),
        )

    def test_targets_in_the_inventory_pass(self) -> None:
        self.assertEqual([], target_problems(delete_targets(PLAN), INVENTORY, RUN, []))

    def test_a_bucket_is_never_a_terraform_delete_target(self) -> None:
        with_bucket = {"resource_changes": [change("aws_s3_bucket.other", "aws_s3_bucket", "example-project", ["delete"])]}
        self.assertEqual(
            ["delete target is a bucket, and Terraform manages no bucket: aws_s3_bucket.other"],
            target_problems(delete_targets(with_bucket), INVENTORY, RUN, []),
        )

    def test_a_host_with_no_recorded_tailscale_device_is_refused(self) -> None:
        without_device = [entry for entry in INVENTORY if entry["type"] != "tailscale_device"]
        expected = ["the plan deletes a host and the run has no record of its Tailscale device: run make host-start or make host-up first"]
        self.assertEqual(expected, target_problems(delete_targets(PLAN), without_device, RUN, []))

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
        expected = ["delete target is a host in a run that did not create its host: module.host[0].aws_instance.host"]
        self.assertEqual(expected, target_problems(delete_targets(PLAN), INVENTORY, LOCAL, []))
        self.assertEqual(expected, target_problems(delete_targets(PLAN), INVENTORY, GIVEN_HOST, []))

    def test_bucket_named_for_removal_must_be_the_one_this_run_created(self) -> None:
        expected = ["the bucket in this run's inventory is not the bucket this run created"]
        other = [entry | {"service_id": "other-project"} if entry["address"] == "project bucket" else entry for entry in INVENTORY]
        self.assertEqual(expected, target_problems(delete_targets(PLAN), other, RUN, []))
        adopted = RunBinding(**{**RUN.__dict__, "bucket_creation": "none"})
        self.assertEqual(expected, target_problems(delete_targets(PLAN), INVENTORY, adopted, []))

    def test_bucket_the_run_did_not_create_is_kept_without_a_refusal(self) -> None:
        adopted = RunBinding(**{**RUN.__dict__, "bucket_creation": "none"})
        without_bucket = [entry for entry in INVENTORY if entry["address"] != "project bucket"]
        self.assertEqual([], target_problems(delete_targets(PLAN), without_bucket, adopted, []))


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

    def test_only_the_data_prefixes_hold_data_and_only_this_runs_keys_hold_its_state(self) -> None:
        found = [
            entry("landing/a.vital", "v1"),
            entry(STATE_KEYS[0], "v2"),
            entry("state/prod/terraform.tfstate", "v3"),
            entry(STATE_KEYS[1], "v4"),
            entry("warehouse/table/data.parquet", "v5"),
            entry("landing", "v6"),
            entry("other/notes.txt", "v7"),
        ]
        self.assertEqual(([found[0], found[4]], [found[1], found[3]], [found[2], found[5], found[6]]), classify_entries(found, RUN))
        self.assertEqual(([found[0], found[4]], [], [found[1], found[2], found[3], found[5], found[6]]), classify_entries(found, LOCAL))

    def test_an_entry_that_is_neither_data_nor_this_runs_state_stops_the_removal_of_a_bucket_this_run_created(self) -> None:
        other = [entry("state/prod/terraform.tfstate", "v3")]
        expected = ["bucket: entry outside the data prefixes and this run's state keys: state/prod/terraform.tfstate"]
        self.assertEqual(expected, other_entry_problems(other, RUN))

    def test_such_an_entry_is_left_alone_in_a_bucket_this_run_did_not_create(self) -> None:
        adopted = RunBinding(**{**RUN.__dict__, "bucket_creation": "none"})
        self.assertEqual([], other_entry_problems([entry("state/prod/terraform.tfstate", "v3")], adopted))

    def test_approved_state_entries_pass(self) -> None:
        approved = [[STATE_KEYS[0], "v1"], [STATE_KEYS[1], "v2"]]
        self.assertEqual([], state_entry_problems([entry(STATE_KEYS[0], "v1"), entry(STATE_KEYS[1], "v2")], STATE_KEYS, approved))

    def test_foreign_state_key_is_refused(self) -> None:
        self.assertEqual(
            ["bucket: key outside this run's state keys: other/terraform.tfstate"],
            state_entry_problems([entry("other/terraform.tfstate", "v1")], STATE_KEYS, [["other/terraform.tfstate", "v1"]]),
        )

    def test_unapproved_noncurrent_state_version_is_refused(self) -> None:
        approved = [[STATE_KEYS[0], "v2"]]
        expected = [f"bucket: state version outside the approved inventory: {STATE_KEYS[0]}"]
        self.assertEqual(expected, state_entry_problems([entry(STATE_KEYS[0], "v1"), entry(STATE_KEYS[0], "v2")], STATE_KEYS, approved))

    def test_entries_written_by_the_destroy_apply_pass_once_recorded(self) -> None:
        approved = [[STATE_KEYS[0], "v1"]]
        found = [entry(STATE_KEYS[0], "v1"), entry(STATE_KEYS[0], "v3")]
        self.assertEqual([], state_entry_problems(found, STATE_KEYS, approved, [[STATE_KEYS[0], "v3"]]))


LINEAGE = "1b2c3d4e-0000-0000-0000-000000000000"


def state_bytes(**changes: object) -> bytes:
    return json.dumps({"version": 4, "lineage": LINEAGE, "serial": 9, "resources": []} | changes).encode()


class DestroyWriteTest(unittest.TestCase):
    def check(self, key: str, content: bytes | None, marker: bool = False) -> str | None:
        return destroy_write_problem(key, content, marker, STATE_KEYS, LINEAGE, 7)

    def test_empty_state_of_this_lineage_with_a_higher_serial_is_the_destroys_own(self) -> None:
        self.assertIsNone(self.check(STATE_KEYS[0], state_bytes()))

    def test_state_of_another_lineage_is_not(self) -> None:
        self.assertEqual("a state version that this run's destroy did not write", self.check(STATE_KEYS[0], state_bytes(lineage="other")))

    def test_state_that_still_holds_resources_is_not(self) -> None:
        self.assertEqual("a state version that this run's destroy did not write", self.check(STATE_KEYS[0], state_bytes(resources=[{"type": "aws_instance"}])))

    def test_state_with_an_older_serial_is_not(self) -> None:
        self.assertEqual("a state version that this run's destroy did not write", self.check(STATE_KEYS[0], state_bytes(serial=7)))

    def test_unreadable_state_and_a_delete_marker_at_the_state_key_are_not(self) -> None:
        self.assertEqual("a state version that this run's destroy did not write", self.check(STATE_KEYS[0], b"not json"))
        self.assertEqual("a delete marker at the state key", self.check(STATE_KEYS[0], None, marker=True))

    def test_lock_entries_for_this_state_are_the_destroys_own(self) -> None:
        lock = json.dumps({"ID": "x", "Operation": "OperationTypeApply", "Path": f"example-project/{STATE_KEYS[0]}"}).encode()
        self.assertIsNone(self.check(STATE_KEYS[1], lock))
        self.assertIsNone(self.check(STATE_KEYS[1], None, marker=True))

    def test_lock_entry_for_another_state_is_not(self) -> None:
        lock = json.dumps({"ID": "x", "Operation": "OperationTypeApply", "Path": "example-project/other/terraform.tfstate"}).encode()
        self.assertEqual("a lock entry that is not for this run's state", self.check(STATE_KEYS[1], lock))

    def test_entry_at_another_key_is_not(self) -> None:
        self.assertEqual("an entry outside this run's state keys", self.check("other/terraform.tfstate", state_bytes()))


class TeardownStageTest(unittest.TestCase):
    def test_local_state_must_be_the_file_the_plan_was_saved_for(self) -> None:
        state = Path(tempfile.mkdtemp(prefix="local_state_")) / "terraform.tfstate"
        state.write_text("{}")
        unchanged = "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
        self.assertIsNone(local_state_problem(state, unchanged))
        self.assertEqual("the local state changed after the destroy plan was saved", local_state_problem(state, "0" * 64))
        self.assertEqual("the bound local state file does not exist", local_state_problem(state.with_name("missing.tfstate"), unchanged))

    def test_a_new_teardown_runs_every_stage(self) -> None:
        self.assertEqual(["release", "apply", "purge", "state", "checks"], remaining_stages([]))

    def test_a_rerun_resumes_after_the_last_finished_stage(self) -> None:
        self.assertEqual(["apply", "purge", "state", "checks"], remaining_stages(["release"]))
        self.assertEqual(["purge", "state", "checks"], remaining_stages(["release", "apply"]))
        self.assertEqual([], remaining_stages(["release", "apply", "purge", "state", "checks"]))

    def test_a_rerun_must_carry_the_approval_the_teardown_started_with(self) -> None:
        same = {"code": "abc", "approved_sha256": "1" * 64, "plan_sha256": "2" * 64, "inventory_sha256": "3" * 64, "receipt_sha256": "4" * 64}
        started: dict[str, Any] = same | {"finished": ["release"]}
        self.assertIsNone(progress_problem(started, same))
        for name in same:
            changed = same | {name: "other"}
            self.assertEqual(f"this teardown started with another {name}: a rerun must use the approval it started with", progress_problem(started, changed))

    def test_a_part_is_verified_only_through_its_own_check_or_a_parent_that_is_gone(self) -> None:
        reported = {("aws_iam_role", "example_host"): "gone", ("aws_iam_role", "example_deploy"): "still exists"}
        self.assertEqual("gone with its parent", parent_check({"type": "aws_iam_role_policy", "service_id": "example_host:storage"}, reported))
        self.assertEqual("still exists: its parent remains", parent_check({"type": "aws_iam_role_policy", "service_id": "example_deploy:access"}, reported))
        self.assertEqual(
            "unverified: its parent was not checked", parent_check({"type": "aws_iam_role_policy", "service_id": "example_other:access"}, reported)
        )
        self.assertEqual("unverified: no check for this type", parent_check({"type": "aws_lambda_function", "service_id": "example"}, reported))


DEVICE_ENTRY = {"address": "tailscale device", "type": "tailscale_device", "service_id": "nNEWCNTRL", "host_name": "platform-dev"}


class DeviceCheckTest(unittest.TestCase):
    def test_a_device_that_left_the_network_is_gone(self) -> None:
        other = {"BackendState": "Running", "Peer": {"key": {"ID": "nOTHER", "HostName": "laptop", "Tags": []}}}
        self.assertEqual("gone", device_check(DEVICE_ENTRY, lambda: other))
        self.assertEqual("gone", device_check(DEVICE_ENTRY, lambda: {"BackendState": "Running", "Peer": None}))

    def test_the_recorded_device_or_a_tagged_device_with_the_hosts_name_still_exists(self) -> None:
        recorded = {"BackendState": "Running", "Peer": {"key": {"ID": "nNEWCNTRL", "HostName": "renamed", "Tags": []}}}
        rejoined = {"BackendState": "Running", "Peer": {"key": {"ID": "nLATER", "HostName": "platform-dev", "Tags": ["tag:surgical-host"]}}}
        self.assertEqual("still exists", device_check(DEVICE_ENTRY, lambda: recorded))
        self.assertEqual("still exists", device_check(DEVICE_ENTRY, lambda: rejoined))

    def test_a_client_that_is_not_connected_leaves_the_device_unverified(self) -> None:
        unverified = "unverified: the Tailscale network could not be read"
        self.assertEqual(unverified, device_check(DEVICE_ENTRY, lambda: {"BackendState": "NeedsLogin", "Peer": None}))
        self.assertEqual(unverified, device_check(DEVICE_ENTRY, lambda: {"Peer": None}))

    def test_a_network_that_cannot_be_read_leaves_the_device_unverified(self) -> None:
        def unreadable() -> dict[str, Any]:
            raise HostError("tailscale status failed")

        self.assertEqual("unverified: the Tailscale network could not be read", device_check(DEVICE_ENTRY, unreadable))


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class RemovalCheckTest(unittest.TestCase):
    def test_a_removal_that_the_service_reports_late_is_confirmed(self) -> None:
        answers = {
            "example_host": iter(["still exists", "still exists", "gone"]),
            "i-0aaa": iter(["gone"]),
            "example-project": iter(["gone"]),
            "nNEWCNTRL": iter(["gone"]),
        }
        clock = Clock()
        checks = settled_checks(
            lambda entry, remaining: next(answers[entry["service_id"]]), INVENTORY, deadline_seconds=60, pause_seconds=5, clock=clock.time, sleep=clock.sleep
        )
        self.assertEqual(["gone", "gone", "gone", "gone"], [check["reported"] for check in checks])

    def test_a_resource_that_stays_is_reported_at_the_deadline(self) -> None:
        clock = Clock()
        checks = settled_checks(
            lambda entry, remaining: "still exists" if entry["service_id"] == "i-0aaa" else "gone",
            INVENTORY,
            deadline_seconds=12,
            pause_seconds=5,
            clock=clock.time,
            sleep=clock.sleep,
        )
        self.assertEqual(["still exists", "gone", "gone", "gone"], [check["reported"] for check in checks])
        self.assertLessEqual(clock.now, 12)

    def test_every_check_gets_only_the_time_that_is_left_and_slow_calls_cannot_pass_the_deadline(self) -> None:
        clock = Clock()
        budgets: list[float] = []

        def slow(entry: dict[str, str], remaining: float) -> str:
            budgets.append(remaining)
            clock.sleep(min(remaining, 30))
            return "still exists"

        settled_checks(slow, INVENTORY, deadline_seconds=100, pause_seconds=5, clock=clock.time, sleep=clock.sleep)
        self.assertLessEqual(clock.now, 100)
        self.assertTrue(all(0 < budget <= 100 for budget in budgets))
        self.assertEqual(sorted(budgets, reverse=True), budgets)


if __name__ == "__main__":
    unittest.main()
