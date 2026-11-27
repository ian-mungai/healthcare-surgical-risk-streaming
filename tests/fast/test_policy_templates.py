"""Every policy template renders and keeps its scope, and each planted defect is refused for its own cause.

The grant tool's calls to AWS are proven by the owner's real plan and apply. These cases cover what must hold before
any call: the rendered documents and the plan computed from them.
"""

from __future__ import annotations

import json
import unittest
from collections.abc import Mapping
from fnmatch import fnmatchcase
from typing import Any

from infra.iam.scripts.manage_policies import LiveState, Names, Step, differences, plan_steps, policy_arn, unchanged_differences
from infra.iam.scripts.render_policy import BOUNDARY_DIRECTORY, POLICIES_DIRECTORY, PolicyRenderError, as_list, load_documents, render_template, scope_problems

VALUES = {
    "AWS_REGION": "us-west-2",
    "AWS_ACCOUNT_ID": "1" * 12,
    "PROJECT_NAME": "example_project",
    "ENVIRONMENT": "dev",
    "S3_BUCKET": "example-project",
    "TAILSCALE_AUDIENCE": "example-audience",
}
STATEMENT = {"Sid": "ReadOffers", "Effect": "Allow", "Action": "ec2:DescribeInstanceTypeOfferings", "Resource": "*"}
GOOD = {"Version": "2012-10-17", "Statement": [STATEMENT]}
NAMES = Names("example_project", "dev")
ACCOUNT = "1" * 12


def template(statement: dict[str, object]) -> str:
    return json.dumps({"Version": "2012-10-17", "Statement": [statement]})


class TemplateTest(unittest.TestCase):
    def test_every_template_renders_within_its_scope(self) -> None:
        documents = load_documents(VALUES)
        self.assertEqual(sorted(documents), sorted(path.stem.split("_")[-2] for path in POLICIES_DIRECTORY.glob("*_policy.json")))
        for service, document in documents.items():
            with self.subTest(service=service):
                self.assertEqual([], scope_problems(document))
                self.assertNotIn("${", json.dumps(document))

    def test_boundary_renders_within_its_scope(self) -> None:
        boundaries = load_documents(VALUES, BOUNDARY_DIRECTORY, suffix="_boundary")
        self.assertEqual(["role"], sorted(boundaries))
        self.assertEqual([], scope_problems(boundaries["role"]))

    def test_boundary_admits_one_audience_only_and_requires_it(self) -> None:
        boundary = load_documents(VALUES, BOUNDARY_DIRECTORY, suffix="_boundary")["role"]
        statement = next(item for item in boundary["Statement"] if item["Action"] == "sts:GetWebIdentityToken")
        self.assertEqual(
            {
                "ForAllValues:StringEquals": {"sts:IdentityTokenAudience": "example-audience"},
                "Null": {"sts:IdentityTokenAudience": "false"},
                "NumericLessThanEquals": {"sts:DurationSeconds": "300"},
            },
            statement["Condition"],
        )

    def test_boundary_is_refused_without_the_audience(self) -> None:
        with self.assertRaisesRegex(PolicyRenderError, "missing values for: TAILSCALE_AUDIENCE"):
            load_documents(VALUES | {"TAILSCALE_AUDIENCE": ""}, BOUNDARY_DIRECTORY, suffix="_boundary")

    def test_templates_hold_no_account_number(self) -> None:
        for path in [*POLICIES_DIRECTORY.glob("*.json"), *BOUNDARY_DIRECTORY.glob("*.json")]:
            with self.subTest(template=path.name):
                self.assertNotRegex(path.read_text(), r"\d{12}")

    def test_good_template_passes(self) -> None:
        self.assertEqual(GOOD, render_template(json.dumps(GOOD), VALUES))
        self.assertEqual([], scope_problems(GOOD))

    def test_unknown_placeholder_is_refused(self) -> None:
        with self.assertRaisesRegex(PolicyRenderError, "unknown placeholders: OTHER_NAME"):
            render_template(template({"Sid": "A", "Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::${OTHER_NAME}/*"}), VALUES)

    def test_missing_value_is_refused(self) -> None:
        with self.assertRaisesRegex(PolicyRenderError, "missing values for: S3_BUCKET"):
            render_template(
                template({"Sid": "A", "Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::${S3_BUCKET}/*"}),
                VALUES | {"S3_BUCKET": ""},
            )

    def test_wildcard_write_is_refused(self) -> None:
        document = render_template(template({"Sid": "WriteAnything", "Effect": "Allow", "Action": ["s3:PutObject"], "Resource": "*"}), VALUES)
        self.assertEqual(["WriteAnything: s3:PutObject is allowed on every resource with no condition"], scope_problems(document))

    def test_statement_without_sid_is_refused(self) -> None:
        document = render_template(template({"Effect": "Allow", "Action": "ec2:DescribeRegions", "Resource": "*"}), VALUES)
        self.assertEqual(["statement 1: no Sid"], scope_problems(document))

    def test_oversized_policy_is_refused(self) -> None:
        actions = [f"s3:GetObject{number:04d}" for number in range(600)]
        with self.assertRaisesRegex(PolicyRenderError, "IAM allows 6144"):
            render_template(template({"Sid": "A", "Effect": "Allow", "Action": actions, "Resource": "arn:aws:s3:::x/*"}), VALUES)


class CostReadTest(unittest.TestCase):
    """The project profile may read the cost and usage of its account, to read the copy's bill, and may do nothing else with Cost Explorer.

    AWS's service authorization reference for AWS Cost Explorer lists ``ce:GetCostAndUsage`` at the Read access level, with the optional resource type
    ``billingview`` and no required one, so a call that names no billing view is allowed only on ``*``
    (https://docs.aws.amazon.com/service-authorization/latest/reference/list_ce.html, read Nov 23 2026).
    """

    def statements(self) -> list[dict[str, Any]]:
        documents = load_documents(VALUES)
        self.assertIn("ce", documents, "a Cost Explorer policy template exists")
        return list(documents["ce"]["Statement"])

    def test_one_statement_allows_only_the_read_of_cost_and_usage_on_every_resource(self) -> None:
        statements = self.statements()
        self.assertEqual(1, len(statements))
        self.assertEqual(
            {"Sid": "ReadCostAndUsage", "Effect": "Allow", "Action": "ce:GetCostAndUsage", "Resource": "*"},
            statements[0],
        )

    def test_the_grant_passes_the_scope_rules_because_the_action_is_a_read(self) -> None:
        document = load_documents(VALUES)["ce"]
        self.assertEqual([], scope_problems(document))
        self.assertEqual(["ce:GetCostAndUsage"], [action for statement in document["Statement"] for action in as_list(statement["Action"])])

    def test_no_other_template_and_no_boundary_statement_grants_a_cost_explorer_action(self) -> None:
        others = {name: document for name, document in load_documents(VALUES).items() if name != "ce"}
        boundary = load_documents(VALUES, BOUNDARY_DIRECTORY, suffix="_boundary")
        for name, document in {**others, **boundary}.items():
            with self.subTest(template=name):
                self.assertEqual([], [a for s in document["Statement"] for a in as_list(s.get("Action", [])) if str(a).startswith("ce:") or a == "*"])


def resources(document: Mapping[str, Any], *sids: str) -> list[str]:
    found: list[str] = []
    for statement in document["Statement"]:
        if statement["Sid"] in sids:
            found += [statement["Resource"]] if isinstance(statement["Resource"], str) else statement["Resource"]
    return sorted(found)


class BucketScopeTest(unittest.TestCase):
    BUCKETS = ["arn:aws:s3:::example-project", "arn:aws:s3:::example-project-test-*"]

    PUBLISHER_SIDS = ("ListPublisherDataset", "ReadPublisherDataset")

    def test_the_projects_user_reaches_only_the_one_bucket_and_its_test_buckets_and_the_publishers_release(self) -> None:
        document = load_documents(VALUES)["s3"]
        project = [statement["Sid"] for statement in document["Statement"] if statement["Sid"] not in self.PUBLISHER_SIDS]
        every = sorted({item for sid in project for item in resources(document, sid)})
        self.assertEqual(sorted([*self.BUCKETS, *(f"{bucket}/*" for bucket in self.BUCKETS)]), every)
        self.assertEqual(sorted(f"{bucket}/*" for bucket in self.BUCKETS), resources(document, "ReadAndWriteProjectObjects"))
        self.assertEqual(["arn:aws:s3:::physionet-open", "arn:aws:s3:::physionet-open/vitaldb/1.0.0/*"], resources(document, *self.PUBLISHER_SIDS))

    def test_the_publishers_release_is_listed_under_its_prefix_and_read_only(self) -> None:
        statements = {statement["Sid"]: statement for statement in load_documents(VALUES)["s3"]["Statement"]}
        listing, reading = statements["ListPublisherDataset"], statements["ReadPublisherDataset"]
        self.assertEqual(("Allow", "s3:ListBucket", "arn:aws:s3:::physionet-open"), (listing["Effect"], listing["Action"], listing["Resource"]))
        self.assertEqual({"StringLike": {"s3:prefix": ["vitaldb/1.0.0/*"]}}, listing["Condition"])
        self.assertEqual(("Allow", "s3:GetObject", "arn:aws:s3:::physionet-open/vitaldb/1.0.0/*"), (reading["Effect"], reading["Action"], reading["Resource"]))
        self.assertNotIn("Condition", reading)

    def test_no_statement_writes_to_or_lists_the_publishers_bucket_beyond_the_release(self) -> None:
        for statement in load_documents(VALUES)["s3"]["Statement"]:
            if not any("physionet-open" in item for item in as_list(statement["Resource"])):
                continue
            with self.subTest(statement=statement["Sid"]):
                self.assertEqual(["s3:GetObject"] if statement["Sid"] == "ReadPublisherDataset" else ["s3:ListBucket"], as_list(statement["Action"]))
                self.assertTrue(
                    all(
                        item == "arn:aws:s3:::physionet-open" or item.startswith("arn:aws:s3:::physionet-open/vitaldb/1.0.0/")
                        for item in as_list(statement["Resource"])
                    )
                )

    def test_no_role_reaches_objects_outside_the_data_prefixes(self) -> None:
        document = load_documents(VALUES, BOUNDARY_DIRECTORY, suffix="_boundary")["role"]
        self.assertEqual(self.BUCKETS, resources(document, "ProjectBucketListing"))
        expected = sorted(f"{bucket}/{prefix}/*" for bucket in self.BUCKETS for prefix in ("landing", "warehouse"))
        self.assertEqual(expected, resources(document, "ProjectDataObjects"))
        self.assertEqual([], [item for item in resources(document, *[statement["Sid"] for statement in document["Statement"]]) if "/state/" in item])

    def test_manifest_objects_have_their_own_statement_with_the_fewest_actions(self) -> None:
        document = load_documents(VALUES, BOUNDARY_DIRECTORY, suffix="_boundary")["role"]
        statements = {statement["Sid"]: statement for statement in document["Statement"]}
        for sid, prefix, actions in (("ProjectManifestObjects", "manifests", ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject"]),):
            with self.subTest(statement=sid):
                self.assertEqual("Allow", statements[sid]["Effect"])
                self.assertEqual(actions, sorted(as_list(statements[sid]["Action"])))
                self.assertEqual(sorted(f"{bucket}/{prefix}/*" for bucket in self.BUCKETS), resources(document, sid))
                self.assertNotIn("Condition", statements[sid])

    def test_the_read_copies_of_the_record_load_have_their_own_statement_with_no_whole_object_delete_and_no_multipart_action(self) -> None:
        document = load_documents(VALUES, BOUNDARY_DIRECTORY, suffix="_boundary")["role"]
        statements = {statement["Sid"]: statement for statement in document["Statement"]}
        statement = statements["ProjectRecordCopyObjects"]
        self.assertEqual("Allow", statement["Effect"])
        self.assertEqual(["s3:DeleteObjectVersion", "s3:GetObject", "s3:GetObjectVersion", "s3:PutObject"], sorted(as_list(statement["Action"])))
        self.assertEqual(sorted(f"{bucket}/records_input/*" for bucket in self.BUCKETS), resources(document, "ProjectRecordCopyObjects"))
        self.assertNotIn("Condition", statement)
        self.assertNotIn("records_input", json.dumps(statements["ProjectDataObjects"]))

    def test_no_other_statement_of_the_boundary_reaches_records_input(self) -> None:
        document = load_documents(VALUES, BOUNDARY_DIRECTORY, suffix="_boundary")["role"]
        reaching = [statement["Sid"] for statement in document["Statement"] if "records_input" in json.dumps(statement)]
        self.assertEqual(["ProjectRecordCopyListing", "ProjectRecordCopyObjects"], sorted(reaching))

    def test_no_statement_of_the_boundary_names_the_airbyte_prefix(self) -> None:
        document = load_documents(VALUES, BOUNDARY_DIRECTORY, suffix="_boundary")["role"]
        self.assertNotIn("airbyte", json.dumps(document).lower())

    def test_no_allow_statement_of_the_boundary_reaches_the_state_prefix(self) -> None:
        document = load_documents(VALUES, BOUNDARY_DIRECTORY, suffix="_boundary")["role"]
        keys = [f"{bucket}/state/terraform.tfstate" for bucket in ("arn:aws:s3:::example-project", "arn:aws:s3:::example-project-test-01020304abcdef")]
        reaching = []
        for statement in document["Statement"]:
            if statement["Effect"] != "Allow" or not any(action.startswith("s3:") for action in as_list(statement["Action"])):
                continue
            patterns = as_list(statement["Resource"])
            listed = as_list(statement.get("Condition", {}).get("StringLike", {}).get("s3:prefix", []))
            if any(
                fnmatchcase(key, pattern) for key in keys for pattern in patterns if pattern.count("/") > 0 and not pattern.startswith("arn:aws:s3:::physionet")
            ):
                reaching.append(statement["Sid"])
            if any(fnmatchcase(prefix, pattern) for prefix in ("state/", "state/terraform.tfstate") for pattern in listed):
                reaching.append(statement["Sid"])
        self.assertEqual([], reaching)


class RoleListingTest(unittest.TestCase):
    BUCKETS = ["arn:aws:s3:::example-project", "arn:aws:s3:::example-project-test-*"]
    LISTING = ["s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:ListBucketVersions"]
    PREFIXES = {"StringLike": {"s3:prefix": ["landing/*", "warehouse/*", "manifests/*"]}}
    RECORD_PREFIXES = {"StringLike": {"s3:prefix": ["records_input/*"]}}

    def boundary(self) -> dict[str, dict[str, Any]]:
        document = load_documents(VALUES, BOUNDARY_DIRECTORY, suffix="_boundary")["role"]
        return {statement["Sid"]: statement for statement in document["Statement"]}

    def test_a_role_locates_the_project_buckets_with_no_condition(self) -> None:
        statement = self.boundary()["ProjectBucketLocation"]
        self.assertEqual(["s3:GetBucketLocation"], as_list(statement["Action"]))
        self.assertEqual(self.BUCKETS, sorted(as_list(statement["Resource"])))
        self.assertNotIn("Condition", statement)

    def test_a_role_lists_the_project_buckets_under_the_data_prefixes_only(self) -> None:
        statement = self.boundary()["ProjectBucketListing"]
        self.assertEqual(self.LISTING, sorted(as_list(statement["Action"])))
        self.assertEqual(self.BUCKETS, sorted(as_list(statement["Resource"])))
        self.assertEqual(self.PREFIXES, statement["Condition"])

    def test_no_allow_statement_lists_a_project_bucket_without_the_prefix_condition(self) -> None:
        listing = [
            statement
            for statement in self.boundary().values()
            if statement["Effect"] == "Allow"
            and set(as_list(statement["Action"])) & set(self.LISTING)
            and set(as_list(statement["Resource"])) & set(self.BUCKETS)
        ]
        self.assertEqual(["ProjectBucketListing", "ProjectRecordCopyListing"], sorted(statement["Sid"] for statement in listing))
        for statement in listing:
            self.assertEqual(self.PREFIXES if statement["Sid"] == "ProjectBucketListing" else self.RECORD_PREFIXES, statement.get("Condition"))

    def test_the_read_copies_prefix_has_its_own_listing_statement_with_only_the_two_actions_the_load_calls(self) -> None:
        statement = self.boundary()["ProjectRecordCopyListing"]
        self.assertEqual("Allow", statement["Effect"])
        self.assertEqual(["s3:ListBucket", "s3:ListBucketVersions"], sorted(as_list(statement["Action"])))
        self.assertEqual(self.BUCKETS, sorted(as_list(statement["Resource"])))
        self.assertEqual(self.RECORD_PREFIXES, statement["Condition"])
        self.assertNotIn("records_input", json.dumps(self.boundary()["ProjectBucketListing"]))

    def test_the_multipart_listing_is_not_allowed_on_the_read_copies_prefix(self) -> None:
        for sid, statement in self.boundary().items():
            if statement["Effect"] == "Allow" and "s3:ListBucketMultipartUploads" in as_list(statement["Action"]):
                with self.subTest(sid):
                    patterns = as_list(statement.get("Condition", {}).get("StringLike", {}).get("s3:prefix", []))
                    self.assertTrue(patterns, "a multipart listing without a prefix condition would reach every prefix")
                    self.assertFalse(any(fnmatchcase("records_input/run/", pattern) for pattern in patterns))


def arn(name: str) -> str:
    return policy_arn(ACCOUNT, name)


def live_account(
    documents: Mapping[str, Mapping[str, Any]],
    attached: frozenset[str] = frozenset(),
    inline: frozenset[str] = frozenset(),
    groups: frozenset[str] = frozenset(),
) -> LiveState:
    """An existing user whose policies carry the required tags."""
    return LiveState(True, documents, attached, inline, groups, {name: NAMES.tags for name in documents}, NAMES.tags)


class PlanTest(unittest.TestCase):
    def test_an_existing_user_with_missing_or_wrong_tags_plans_a_tag_step_and_extra_tags_are_reported(self) -> None:
        documents = {"example_project_ec2_policy": GOOD}
        base = live_account(documents, frozenset({arn("example_project_ec2_policy")}))
        for tags, expected in (
            ({}, [Step("tag-user", "example_project_dev")]),
            ({"Project": "example_project", "Environment": "prod"}, [Step("tag-user", "example_project_dev")]),
            (NAMES.tags | {"Owner": "someone"}, [Step("report-extra-user-tag", "example_project_dev Owner")]),
            (NAMES.tags, []),
        ):
            with self.subTest(tags=tags):
                live = LiveState(True, base.documents, base.attached, base.inline, base.groups, base.tags, tags)
                self.assertEqual(expected, plan_steps(NAMES, documents, live, account=ACCOUNT))

    def test_new_account_plans_user_policy_and_attachment(self) -> None:
        steps = plan_steps(NAMES, {"example_project_ec2_policy": GOOD}, LiveState(user_exists=False), account=ACCOUNT)
        self.assertEqual(
            [
                Step("create-user", "example_project_dev"),
                Step("create-policy", "example_project_ec2_policy"),
                Step("attach-policy", "example_project_ec2_policy"),
            ],
            steps,
        )

    def test_changed_document_plans_one_update(self) -> None:
        live = live_account({"example_project_ec2_policy": {"Version": "2012-10-17", "Statement": []}}, frozenset({arn("example_project_ec2_policy")}))
        self.assertEqual([Step("update-policy", "example_project_ec2_policy")], plan_steps(NAMES, {"example_project_ec2_policy": GOOD}, live, account=ACCOUNT))

    def test_rerun_plans_nothing(self) -> None:
        live = live_account({"example_project_ec2_policy": json.loads(json.dumps(GOOD))}, frozenset({arn("example_project_ec2_policy")}))
        self.assertEqual([], plan_steps(NAMES, {"example_project_ec2_policy": GOOD}, live, account=ACCOUNT))

    def test_single_action_string_equals_one_item_list(self) -> None:
        listed = {"Version": "2012-10-17", "Statement": [STATEMENT | {"Action": ["ec2:DescribeInstanceTypeOfferings"]}]}
        live = live_account({"example_project_ec2_policy": listed}, frozenset({arn("example_project_ec2_policy")}))
        self.assertEqual([], plan_steps(NAMES, {"example_project_ec2_policy": GOOD}, live, account=ACCOUNT))

    def test_boundary_is_created_and_never_attached(self) -> None:
        documents = {"example_project_ec2_policy": GOOD, "example_project_role_boundary": GOOD}
        steps = plan_steps(NAMES, documents, LiveState(True, user_tags=NAMES.tags), account=ACCOUNT, unattached={"example_project_role_boundary"})
        expected = [
            Step("create-policy", "example_project_ec2_policy"),
            Step("attach-policy", "example_project_ec2_policy"),
            Step("create-policy", "example_project_role_boundary"),
        ]
        self.assertEqual(expected, steps)

    def test_attached_boundary_is_reported(self) -> None:
        documents = {"example_project_role_boundary": GOOD}
        live = live_account(documents, frozenset({arn("example_project_role_boundary")}))
        steps = plan_steps(NAMES, documents, live, account=ACCOUNT, unattached={"example_project_role_boundary"})
        self.assertEqual([Step("report-attached-boundary", "example_project_role_boundary")], steps)

    def test_unlisted_live_policy_is_reported_not_removed(self) -> None:
        live = live_account({"example_project_ec2_policy": GOOD, "example_project_old_policy": GOOD}, frozenset({arn("example_project_ec2_policy")}))
        steps = plan_steps(NAMES, {"example_project_ec2_policy": GOOD}, live, account=ACCOUNT)
        self.assertEqual([Step("report-unlisted", "example_project_old_policy")], steps)
        self.assertEqual([], differences(steps))
        self.assertEqual(steps, unchanged_differences(steps))

    def test_each_foreign_grant_is_a_reported_difference(self) -> None:
        live = live_account(
            {"example_project_ec2_policy": GOOD},
            frozenset({arn("example_project_ec2_policy"), "arn:aws:iam::aws:policy/AdministratorAccess"}),
            frozenset({"inline_grant"}),
            frozenset({"admins"}),
        )
        steps = plan_steps(NAMES, {"example_project_ec2_policy": GOOD}, live, account=ACCOUNT)
        expected = [
            Step("report-unexpected-attachment", "policy/AdministratorAccess"),
            Step("report-inline-policy", "inline_grant"),
            Step("report-group-membership", "admins"),
        ]
        self.assertEqual(expected, steps)
        self.assertEqual(expected, differences(steps))
        self.assertEqual(expected, unchanged_differences(steps))

    def test_a_policy_with_our_name_in_another_account_is_not_our_attachment(self) -> None:
        other = "arn:aws:iam::" + "2" * 12 + ":policy/example_project_ec2_policy"
        live = live_account({"example_project_ec2_policy": GOOD}, frozenset({other}))
        steps = plan_steps(NAMES, {"example_project_ec2_policy": GOOD}, live, account=ACCOUNT)
        self.assertEqual(
            [Step("attach-policy", "example_project_ec2_policy"), Step("report-unexpected-attachment", "policy/example_project_ec2_policy")], steps
        )


if __name__ == "__main__":
    unittest.main()
