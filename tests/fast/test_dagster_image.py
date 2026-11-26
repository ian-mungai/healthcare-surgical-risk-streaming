"""The Dagster image takes every outside image and program by version and digest, and runs as the one user that the Compose files name.

The text of the Dockerfile is the contract here: the build reads these lines. The second group plants one change at a time.
"""

from __future__ import annotations

import ast
import re
import unittest
from pathlib import Path

DOCKERFILE = Path(__file__).resolve().parents[2] / "orchestration" / "Dockerfile"
NAMED_CONTEXTS = {"stack_scripts", "stack_compose", "stack_spark", "stack_fixtures"}
PINNED = re.compile(r"^[a-z0-9./_-]+:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}$")


def image_references(text: str) -> list[str]:
    lines = text.replace("\\\n", " ").splitlines()
    found = [line.split()[1] for line in lines if line.startswith("FROM ")]
    found += [match for line in lines for match in re.findall(r"--from=(\S+)", line) if match not in NAMED_CONTEXTS]
    return found


def problems(text: str) -> list[str]:
    found = [
        f"{reference}: an image must be pinned by a version tag and a sha256 digest" for reference in image_references(text) if not PINNED.match(reference)
    ]
    users = [line.split()[1] for line in text.splitlines() if line.startswith("USER ")]
    if users[-1:] != ["10001:10001"]:
        found.append(f"the last USER must be 10001:10001, found {users[-1:]}")
    digests = {reference.partition("@")[2] for reference in image_references(text) if reference.startswith("docker:")}
    if len(digests) != 1:
        found.append(f"the Docker command line and its Compose plugin must come from one image digest, found {sorted(digests)}")
    return found


class RealDockerfileTest(unittest.TestCase):
    def test_the_dockerfile_pins_every_image_and_runs_as_the_container_user(self) -> None:
        text = DOCKERFILE.read_text()
        self.assertEqual([], problems(text))
        self.assertGreaterEqual(len(image_references(text)), 3)


class PlantedDefectTest(unittest.TestCase):
    def test_an_image_without_a_digest_or_a_tag_is_named(self) -> None:
        text = DOCKERFILE.read_text()
        for old, new in (
            (re.search(r"docker:29\.10\.0-cli@sha256:[0-9a-f]{64}", text), "docker:29.10.0-cli"),
            (re.search(r"docker:29\.10\.0-cli@sha256:[0-9a-f]{64}", text), "docker:latest"),
            (re.search(r"python:3\.14\.10-slim-trixie@sha256:[0-9a-f]{64}", text), "python:3.14.10-slim-trixie"),
        ):
            self.assertIsNotNone(old)
            if old is None:
                continue
            with self.subTest(new=new):
                self.assertTrue(any("must be pinned" in problem for problem in problems(text.replace(old.group(0), new, 1))))

    def test_a_user_other_than_the_container_user_is_named(self) -> None:
        for user in ("root", "0", "10002:10002"):
            with self.subTest(user=user):
                self.assertTrue(any("USER" in problem for problem in problems(DOCKERFILE.read_text().replace("USER 10001:10001", f"USER {user}"))))

    def test_two_digests_for_the_docker_command_line_and_its_plugin_are_named(self) -> None:
        text = DOCKERFILE.read_text()
        digest = re.search(r"docker:29\.10\.0-cli@sha256:([0-9a-f]{64})", text)
        self.assertIsNotNone(digest)
        if digest is not None:
            other = text.replace(digest.group(1), "0" * 64, 1)
            self.assertTrue(any("one image digest" in problem for problem in problems(other)))


ROOT = DOCKERFILE.parents[1]
IMAGE_APP = "/opt/dagster/app/"
ENTRY_MODULES = ("scripts/ingestion/record_run.py", "orchestration/surgical_risk/record_load.py", "scripts/ingestion/raw_copy.py")


def module_file(name: str) -> str | None:
    """The tracked file of a project module, or None for a module of the standard library or of a package."""
    path = name.replace(".", "/")
    for candidate in (f"{path}.py", f"{path}/__init__.py"):
        if (ROOT / candidate).is_file():
            return candidate
    return None


def imported_files(entry: str) -> set[str]:
    """Every file of the ``scripts`` package that ``entry`` imports, directly or through another, and the ``__init__.py`` of each package on the way."""
    found: set[str] = set()
    pending = [entry]
    while pending:
        current = pending.pop()
        tree = ast.parse((ROOT / current).read_text())
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                names = [node.module, *(f"{node.module}.{alias.name}" for alias in node.names)]
            elif isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            for name in names:
                path = module_file(name) if name.split(".")[0] == "scripts" else None
                if path is not None and path not in found:
                    found.add(path)
                    pending.append(path)
    for path in list(found):
        parts = path.split("/")
        found.update("/".join(parts[:depth]) + "/__init__.py" for depth in range(1, len(parts)))
    return {path for path in found if (ROOT / path).is_file()}


def copied_from(text: str, context: str) -> set[str]:
    """The files that ``COPY --from=<context>`` takes from the repository's ``scripts`` folder, as paths of the repository."""
    found: set[str] = set()
    for line in text.replace("\\\n", " ").splitlines():
        words = line.split()
        if len(words) > 3 and words[0] == "COPY" and words[1] == f"--from={context}":
            found.update(f"scripts/{source}" for source in words[2:-1])
    return found


AWS_CLI_REFERENCE = "amazon/aws-cli:2.42.1@sha256:b0102d2269f87d8531d931c1a59e248101ace586886e6c077058d94c1c37b846"
AWS_COPY = f"COPY --from={AWS_CLI_REFERENCE} /usr/local/aws-cli /usr/local/aws-cli"
AWS_LINK = "ln -s /usr/local/aws-cli/v2/current/bin/aws /usr/local/bin/aws"
# The subcommands and options that the S3 store calls, each read on Nov 22 2026 in the help of the AWS CLI 2.42.1: ``aws s3api <subcommand> help`` run in
# amazon/aws-cli:2.42.1. ``--range`` of get-object is added by the store to a list and is in the help too.
AWS_2_42_S3API = {
    "list-object-versions": {"--bucket", "--prefix"},
    "get-object": {"--bucket", "--key", "--version-id", "--range"},
    "put-object": {"--bucket", "--key", "--body", "--content-type", "--if-none-match"},
    "get-bucket-versioning": {"--bucket"},
    "copy-object": {"--bucket", "--key", "--copy-source"},
    "delete-object": {"--bucket", "--key", "--version-id"},
}


def s3api_calls(path: str) -> dict[str, set[str]]:
    """The ``s3api`` subcommands that a module gives the AWS caller, as call arguments or as a list that starts with ``s3api``, each with the options that are
    string constants in that call or list. An option that the module adds to a list later (``--range``) is not found, and is not asserted either."""
    found: dict[str, set[str]] = {}
    tree = ast.parse((ROOT / path).read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "call":
            elements: list[ast.expr] = list(node.args)
        elif isinstance(node, ast.List):
            elements = list(node.elts)
        else:
            continue
        words = [item.value for item in elements if isinstance(item, ast.Constant) and isinstance(item.value, str)]
        if words[:1] == ["s3api"] and len(words) > 1:
            found.setdefault(words[1], set()).update(word for word in words[2:] if word.startswith("--"))
    return found


class AwsCliTest(unittest.TestCase):
    """The S3 store runs the ``aws`` program, so the image holds it: the bundle comes from the official image by digest and is linked into the path."""

    def test_the_bundle_is_copied_from_a_digest_pinned_image_and_linked_as_aws(self) -> None:
        text = DOCKERFILE.read_text()
        self.assertIn(AWS_COPY, text)
        self.assertIn(AWS_LINK, text)
        self.assertIn(AWS_CLI_REFERENCE, image_references(text))
        self.assertEqual([], problems(text))

    def test_the_link_is_made_as_root_before_the_user_is_dropped_and_in_a_folder_of_the_path(self) -> None:
        text = DOCKERFILE.read_text()
        self.assertLess(text.index(AWS_LINK), text.index("USER 10001:10001"))
        self.assertLess(text.index(AWS_COPY), text.index(AWS_LINK))

    def test_a_reference_without_a_digest_or_with_another_tag_form_is_named(self) -> None:
        text = DOCKERFILE.read_text()
        for new in ("amazon/aws-cli:2.42.1", "amazon/aws-cli:latest", "amazon/aws-cli"):
            with self.subTest(new=new):
                self.assertTrue(any("must be pinned" in problem for problem in problems(text.replace(AWS_CLI_REFERENCE, new, 1))))

    def test_the_program_that_the_caller_starts_is_aws_and_the_store_uses_only_subcommands_and_options_of_the_pinned_cli(self) -> None:
        self.assertIn('run_command("aws"', (ROOT / "scripts" / "aws_cli.py").read_text())
        used: dict[str, set[str]] = {}
        for path in ("scripts/ingestion/store.py", "scripts/ingestion/record_copy.py"):
            for name, options in s3api_calls(path).items():
                used.setdefault(name, set()).update(options)
        self.assertEqual({"list-object-versions", "get-object", "put-object", "get-bucket-versioning", "copy-object", "delete-object"}, set(used))
        for name, options in used.items():
            with self.subTest(subcommand=name):
                self.assertIn(name, AWS_2_42_S3API)
                self.assertEqual(set(), options - AWS_2_42_S3API[name])

    def test_a_planted_subcommand_that_the_pinned_cli_does_not_have_is_named(self) -> None:
        self.assertNotIn("get-object-version", AWS_2_42_S3API)


class RecordLoadImageTest(unittest.TestCase):
    """The record load runs in this image, so the image holds every module it imports and the release pin that its manifest reader opens."""

    def test_every_module_that_the_record_load_imports_is_copied_into_the_image(self) -> None:
        needed = set().union(*(imported_files(entry) for entry in ENTRY_MODULES))
        self.assertIn("scripts/ingestion/record_load.py", needed)
        self.assertIn("scripts/sample/candidates.py", needed)
        self.assertIn("scripts/aws_cli.py", needed)
        self.assertIn("scripts/ingestion/connection.py", needed)
        self.assertEqual([], sorted(needed - copied_from(DOCKERFILE.read_text(), "stack_scripts")))

    def test_the_release_pin_that_the_manifest_reader_opens_is_copied_to_where_the_code_looks(self) -> None:
        text = DOCKERFILE.read_text()
        self.assertIn(f"COPY --from=stack_fixtures release.json {IMAGE_APP}tests/fixtures/vitaldb/release.json", text)

    def test_the_candidate_list_that_the_fixed_sample_selection_reads_is_copied_to_where_the_code_looks(self) -> None:
        text = DOCKERFILE.read_text()
        self.assertIn(f"COPY --from=stack_fixtures candidates.tsv {IMAGE_APP}tests/fixtures/vitaldb/candidates.tsv", text)

    def test_the_raw_copy_is_copied_into_the_image_as_the_command_that_the_host_runs(self) -> None:
        self.assertIn("scripts/ingestion/raw_copy.py", copied_from(DOCKERFILE.read_text(), "stack_scripts"))

    def test_a_missing_module_is_named_by_the_check(self) -> None:
        text = DOCKERFILE.read_text().replace("ingestion/record_state.py", "")
        needed = imported_files("scripts/ingestion/record_run.py")
        self.assertIn("scripts/ingestion/record_state.py", needed - copied_from(text, "stack_scripts"))


if __name__ == "__main__":
    unittest.main()
