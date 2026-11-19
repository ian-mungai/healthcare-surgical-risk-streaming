"""Every policy template renders and keeps its scope, and each planted defect is refused for its own cause.

The grant tool's calls to AWS are proven by the owner's real plan and apply. These cases cover what must hold before
any call: the rendered documents and the plan computed from them.
"""

from __future__ import annotations

import json
import unittest

from infra.iam.scripts.manage_policies import Names, Step, plan_steps
from infra.iam.scripts.render_policy import BOUNDARY_DIRECTORY, POLICIES_DIRECTORY, PolicyRenderError, load_documents, render_template, scope_problems

VALUES = {
    "AWS_REGION": "us-west-2",
    "AWS_ACCOUNT_ID": "1" * 12,
    "PROJECT_NAME": "example_project",
    "ENVIRONMENT": "dev",
    "BUCKET_PREFIX": "example-project",
    "TAILSCALE_AUDIENCE": "example-audience",
}
STATEMENT = {"Sid": "ReadOffers", "Effect": "Allow", "Action": "ec2:DescribeInstanceTypeOfferings", "Resource": "*"}
GOOD = {"Version": "2012-10-17", "Statement": [STATEMENT]}
NAMES = Names("example_project", "dev")


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
        with self.assertRaisesRegex(PolicyRenderError, "missing values for: BUCKET_PREFIX"):
            render_template(
                template({"Sid": "A", "Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::${BUCKET_PREFIX}-x/*"}),
                VALUES | {"BUCKET_PREFIX": ""},
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


class PlanTest(unittest.TestCase):
    def test_new_account_plans_user_policy_and_attachment(self) -> None:
        steps = plan_steps(NAMES, {"example_project_ec2_policy": GOOD}, user_exists=False, live={}, attached=set())
        self.assertEqual(
            [
                Step("create-user", "example_project_dev"),
                Step("create-policy", "example_project_ec2_policy"),
                Step("attach-policy", "example_project_ec2_policy"),
            ],
            steps,
        )

    def test_changed_document_plans_one_update(self) -> None:
        live = {"example_project_ec2_policy": {"Version": "2012-10-17", "Statement": []}}
        steps = plan_steps(NAMES, {"example_project_ec2_policy": GOOD}, user_exists=True, live=live, attached={"example_project_ec2_policy"})
        self.assertEqual([Step("update-policy", "example_project_ec2_policy")], steps)

    def test_rerun_plans_nothing(self) -> None:
        live = {"example_project_ec2_policy": json.loads(json.dumps(GOOD))}
        self.assertEqual([], plan_steps(NAMES, {"example_project_ec2_policy": GOOD}, user_exists=True, live=live, attached={"example_project_ec2_policy"}))

    def test_single_action_string_equals_one_item_list(self) -> None:
        listed = {"Version": "2012-10-17", "Statement": [STATEMENT | {"Action": ["ec2:DescribeInstanceTypeOfferings"]}]}
        self.assertEqual(
            [],
            plan_steps(
                NAMES,
                {"example_project_ec2_policy": GOOD},
                user_exists=True,
                live={"example_project_ec2_policy": listed},
                attached={"example_project_ec2_policy"},
            ),
        )

    def test_boundary_is_created_and_never_attached(self) -> None:
        documents = {"example_project_ec2_policy": GOOD, "example_project_role_boundary": GOOD}
        steps = plan_steps(NAMES, documents, unattached={"example_project_role_boundary"}, user_exists=True, live={}, attached=set())
        expected = [
            Step("create-policy", "example_project_ec2_policy"),
            Step("attach-policy", "example_project_ec2_policy"),
            Step("create-policy", "example_project_role_boundary"),
        ]
        self.assertEqual(expected, steps)

    def test_attached_boundary_is_reported(self) -> None:
        documents = {"example_project_role_boundary": GOOD}
        steps = plan_steps(
            NAMES, documents, unattached={"example_project_role_boundary"}, user_exists=True, live=documents, attached={"example_project_role_boundary"}
        )
        self.assertEqual([Step("report-attached-boundary", "example_project_role_boundary")], steps)

    def test_unlisted_live_policy_is_reported_not_removed(self) -> None:
        live = {"example_project_ec2_policy": GOOD, "example_project_old_policy": GOOD}
        steps = plan_steps(
            NAMES, {"example_project_ec2_policy": GOOD}, user_exists=True, live=live, attached={"example_project_ec2_policy", "example_project_old_policy"}
        )
        self.assertEqual([Step("report-unlisted", "example_project_old_policy")], steps)


if __name__ == "__main__":
    unittest.main()
