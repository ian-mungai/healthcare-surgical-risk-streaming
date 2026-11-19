"""What a run created: the record written at apply time, the inventory built from it and the use of the project's one bucket.

The apply and the bucket calls are proven in the live batch and the isolated fixtures. These cases cover the decisions
those runs cannot readily force: a resource that was in the state before the apply, a bucket that already exists for
a new binding, a retry after an interrupted creation and the name of a test's own short-lived bucket.
"""

from __future__ import annotations

import unittest

from scripts.infra.created import created_entries, device_entries, merge_created, run_inventory
from scripts.infra.state_bootstrap import RunBinding, adoption_problem, bucket_problem, short_lived_bucket_name, state_keys
from scripts.project_config import ConfigurationError

KEYS = ["terraform_state/dev/terraform.tfstate", "terraform_state/dev/terraform.tfstate.tflock"]
RUN = RunBinding("20260101t0000-abcdef", "c0ffee", "s3", "us-west-2", "example-project", "default", KEYS, "verified", bucket_creation="created")


def resource(address: str, kind: str, identifier: str, **values: object) -> dict[str, object]:
    return {"address": address, "mode": "managed", "type": kind, "values": {"id": identifier, **values}}


HOST = resource(
    "module.host[0].aws_instance.host", "aws_instance", "i-0aaa", root_block_device=[{"volume_id": "vol-0aaa"}], primary_network_interface_id="eni-0aaa"
)
LANDING = resource('module.storage.aws_s3_bucket.this["landing"]', "aws_s3_bucket", "example-landing")
EARLIER = resource("module.deploy.aws_iam_role.deploy", "aws_iam_role", "example_deploy")
DATA = {"address": "module.host[0].data.aws_vpc.default", "mode": "data", "type": "aws_vpc", "values": {"id": "vpc-0aaa"}}
STATE = {"values": {"root_module": {"child_modules": [{"resources": [HOST, DATA]}, {"resources": [LANDING, EARLIER]}]}}}
PLAN = {
    "resource_changes": [
        {"address": HOST["address"], "change": {"actions": ["create"]}},
        {"address": LANDING["address"], "change": {"actions": ["delete", "create"]}},
        {"address": EARLIER["address"], "change": {"actions": ["no-op"]}},
    ]
}
HOST_ENTRIES = [
    {"address": "module.host[0].aws_instance.host", "type": "aws_instance", "service_id": "i-0aaa"},
    {"address": "module.host[0].aws_instance.host network interface", "type": "aws_network_interface", "service_id": "eni-0aaa"},
    {"address": "module.host[0].aws_instance.host root volume", "type": "aws_ebs_volume", "service_id": "vol-0aaa"},
]
LANDING_ENTRY = {"address": 'module.storage.aws_s3_bucket.this["landing"]', "type": "aws_s3_bucket", "service_id": "example-landing"}


class CreatedRecordTest(unittest.TestCase):
    def test_only_resources_the_applied_plan_created_are_recorded_with_the_hosts_own_parts(self) -> None:
        self.assertEqual([*HOST_ENTRIES, LANDING_ENTRY], created_entries(PLAN, STATE))

    def test_a_later_apply_adds_to_the_record(self) -> None:
        role = {"address": "module.deploy.aws_iam_role.deploy", "type": "aws_iam_role", "service_id": "example_deploy"}
        self.assertEqual([role, *HOST_ENTRIES], merge_created(HOST_ENTRIES, [role]))

    def test_inventory_separates_what_the_run_did_not_create(self) -> None:
        created, not_created = run_inventory([*HOST_ENTRIES, LANDING_ENTRY], STATE, RUN)
        self.assertEqual([*HOST_ENTRIES, LANDING_ENTRY, {"address": "project bucket", "type": "aws_s3_bucket", "service_id": "example-project"}], created)
        self.assertEqual([{"address": "module.deploy.aws_iam_role.deploy", "type": "aws_iam_role", "service_id": "example_deploy"}], not_created)

    def test_a_bucket_the_run_did_not_create_is_not_in_its_inventory(self) -> None:
        adopted = RunBinding(**{**RUN.__dict__, "bucket_creation": "none"})
        created, _ = run_inventory([LANDING_ENTRY], STATE, adopted)
        self.assertEqual([LANDING_ENTRY], created)

    def test_a_recorded_resource_that_left_the_state_is_dropped(self) -> None:
        gone = {"address": "module.old.aws_s3_bucket.gone", "type": "aws_s3_bucket", "service_id": "example-gone"}
        created, _ = run_inventory([gone, LANDING_ENTRY], STATE, RUN)
        self.assertNotIn(gone, created)


class DeviceEntryTest(unittest.TestCase):
    DEVICE = {"host_name": "platform-dev", "device_id": "nNEWCNTRL"}

    def test_the_hosts_tailscale_device_is_listed_with_a_host_this_run_created(self) -> None:
        expected = [{"address": "tailscale device", "type": "tailscale_device", "service_id": "nNEWCNTRL", "host_name": "platform-dev"}]
        self.assertEqual(expected, device_entries(HOST_ENTRIES, self.DEVICE))

    def test_no_device_is_listed_without_a_record_or_without_a_host_this_run_created(self) -> None:
        self.assertEqual([], device_entries(HOST_ENTRIES, None))
        self.assertEqual([], device_entries([LANDING_ENTRY], self.DEVICE))


class AdoptionTest(unittest.TestCase):
    def test_new_run_with_no_bucket_creates_it(self) -> None:
        self.assertIsNone(adoption_problem("none", exists=False, owned=False, empty=False, short_lived=False))
        self.assertIsNone(adoption_problem("none", exists=False, owned=False, empty=False, short_lived=True))

    def test_the_projects_bucket_that_already_exists_is_used_when_this_account_owns_it(self) -> None:
        self.assertIsNone(adoption_problem("none", exists=True, owned=True, empty=False, short_lived=False))
        self.assertEqual(
            "the project's bucket name belongs to a bucket this account does not own",
            adoption_problem("none", exists=True, owned=False, empty=False, short_lived=False),
        )

    def test_an_existing_bucket_is_never_adopted_as_a_tests_own_bucket(self) -> None:
        self.assertEqual(
            "a bucket with this test's bucket name already exists and this run did not create it",
            adoption_problem("none", exists=True, owned=True, empty=True, short_lived=True),
        )

    def test_retry_after_an_interrupted_creation_adopts_only_an_owned_empty_bucket(self) -> None:
        self.assertIsNone(adoption_problem("attempted", exists=True, owned=True, empty=True, short_lived=False))
        expected = "the bucket from this run's interrupted creation is not an empty bucket of this account"
        self.assertEqual(expected, adoption_problem("attempted", exists=True, owned=True, empty=False, short_lived=False))
        self.assertEqual(expected, adoption_problem("attempted", exists=True, owned=False, empty=True, short_lived=True))

    def test_created_bucket_that_is_gone_is_refused(self) -> None:
        self.assertEqual("this run's bucket no longer exists", adoption_problem("created", exists=False, owned=False, empty=False, short_lived=False))
        self.assertIsNone(adoption_problem("created", exists=True, owned=True, empty=False, short_lived=False))


SETTINGS = {"S3_BUCKET": "example-project"}
TEST_RUN = RunBinding(**{**RUN.__dict__, "bucket": "example-project-test-01010000abcdef", "bucket_mode": "short_lived"})


class BucketModeTest(unittest.TestCase):
    def test_a_permanent_run_uses_the_bucket_in_the_settings_and_a_test_run_its_own_derived_bucket(self) -> None:
        self.assertIsNone(bucket_problem(RUN, SETTINGS))
        self.assertIsNone(bucket_problem(TEST_RUN, SETTINGS))
        self.assertIsNone(bucket_problem(RUN, SETTINGS, short_lived=False))
        self.assertIsNone(bucket_problem(TEST_RUN, SETTINGS, short_lived=True))

    def test_a_test_is_refused_on_a_permanent_binding_and_a_permanent_run_on_a_test_binding(self) -> None:
        self.assertEqual(
            "the run is bound to the permanent bucket, and a short-lived test bucket was asked for", bucket_problem(RUN, SETTINGS, short_lived=True)
        )
        self.assertEqual(
            "the run is bound to a short-lived test bucket, and the permanent bucket was asked for", bucket_problem(TEST_RUN, SETTINGS, short_lived=False)
        )

    def test_a_binding_whose_bucket_does_not_fit_its_mode_is_refused(self) -> None:
        moved = RunBinding(**{**RUN.__dict__, "bucket": "example-other"})
        self.assertEqual("the binding's permanent bucket is not the bucket named by S3_BUCKET", bucket_problem(moved, SETTINGS))
        disguised = RunBinding(**{**TEST_RUN.__dict__, "bucket": "example-project"})
        self.assertEqual("the binding's test bucket is not the short-lived bucket of this run", bucket_problem(disguised, SETTINGS))
        other_run = RunBinding(**{**TEST_RUN.__dict__, "bucket": "example-project-test-02020000abcdef"})
        self.assertEqual("the binding's test bucket is not the short-lived bucket of this run", bucket_problem(other_run, SETTINGS))
        unknown = RunBinding(**{**RUN.__dict__, "bucket_mode": "other"})
        self.assertEqual("the binding's bucket mode is neither permanent nor short_lived", bucket_problem(unknown, SETTINGS))


class BucketLayoutTest(unittest.TestCase):
    def test_state_lives_under_its_own_prefix_for_the_environment(self) -> None:
        self.assertEqual(KEYS, state_keys("dev"))

    def test_a_tests_bucket_carries_the_test_suffix_and_the_run(self) -> None:
        self.assertEqual("example-project-test-01010000abcdef", short_lived_bucket_name("example-project", RUN.run_id))

    def test_a_tests_bucket_name_over_the_s3_limit_is_refused(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "longer than 63 characters"):
            short_lived_bucket_name("e" * 44, RUN.run_id)
        self.assertEqual(63, len(short_lived_bucket_name("e" * 43, RUN.run_id)))


if __name__ == "__main__":
    unittest.main()
