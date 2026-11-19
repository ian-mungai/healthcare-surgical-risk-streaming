"""What a run created: the record written at apply time, the inventory built from it and the state bucket's adoption.

The apply and the bucket calls are proven in the live batch and the isolated fixtures. These cases cover the decisions
those runs cannot readily force: a resource that was in the state before the apply, a bucket that already exists for
a new binding and a retry after an interrupted creation.
"""

from __future__ import annotations

import unittest

from scripts.infra.created import created_entries, merge_created, run_inventory
from scripts.infra.state_bootstrap import RunBinding, adoption_problem

KEYS = ["environment/terraform.tfstate", "environment/terraform.tfstate.tflock"]
RUN = RunBinding("20260101t0000-abcdef", "c0ffee", "s3", "us-west-2", "example-state", "default", KEYS, "verified", state_bucket_creation="created")


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
        self.assertEqual([*HOST_ENTRIES, LANDING_ENTRY, {"address": "state bucket", "type": "aws_s3_bucket", "service_id": "example-state"}], created)
        self.assertEqual([{"address": "module.deploy.aws_iam_role.deploy", "type": "aws_iam_role", "service_id": "example_deploy"}], not_created)

    def test_a_state_bucket_the_run_did_not_create_is_not_in_its_inventory(self) -> None:
        adopted = RunBinding(**{**RUN.__dict__, "state_bucket_creation": "none"})
        created, _ = run_inventory([LANDING_ENTRY], STATE, adopted)
        self.assertEqual([LANDING_ENTRY], created)

    def test_a_recorded_resource_that_left_the_state_is_dropped(self) -> None:
        gone = {"address": "module.old.aws_s3_bucket.gone", "type": "aws_s3_bucket", "service_id": "example-gone"}
        created, _ = run_inventory([gone, LANDING_ENTRY], STATE, RUN)
        self.assertNotIn(gone, created)


class AdoptionTest(unittest.TestCase):
    def test_new_run_with_no_bucket_creates_it(self) -> None:
        self.assertIsNone(adoption_problem("none", exists=False, owned_and_empty=False))

    def test_existing_bucket_is_never_adopted_by_a_run_that_did_not_try_to_create_it(self) -> None:
        self.assertEqual(
            "a bucket with this run's state bucket name already exists and this run did not create it",
            adoption_problem("none", exists=True, owned_and_empty=True),
        )

    def test_retry_after_an_interrupted_creation_adopts_only_an_owned_empty_bucket(self) -> None:
        self.assertIsNone(adoption_problem("attempted", exists=True, owned_and_empty=True))
        self.assertEqual(
            "the bucket from this run's interrupted creation is not an empty bucket of this account",
            adoption_problem("attempted", exists=True, owned_and_empty=False),
        )

    def test_created_bucket_that_is_gone_is_refused(self) -> None:
        self.assertEqual("this run's state bucket no longer exists", adoption_problem("created", exists=False, owned_and_empty=False))
        self.assertIsNone(adoption_problem("created", exists=True, owned_and_empty=False))


if __name__ == "__main__":
    unittest.main()
