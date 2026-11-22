"""The Dagster image takes every outside image and program by version and digest, and runs as the one user that the Compose files name.

The text of the Dockerfile is the contract here: the build reads these lines. The second group plants one change at a time.
"""

from __future__ import annotations

import ast
import re
import unittest
from pathlib import Path

DOCKERFILE = Path(__file__).resolve().parents[2] / "orchestration" / "Dockerfile"
NAMED_CONTEXTS = {"stack_scripts", "stack_compose", "stack_fixtures"}
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
ENTRY_MODULES = ("scripts/ingestion/record_run.py", "orchestration/surgical_risk/record_load.py")


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


class RecordLoadImageTest(unittest.TestCase):
    """The record load runs in this image, so the image holds every module it imports and the release pin that its manifest reader opens."""

    def test_every_module_that_the_record_load_imports_is_copied_into_the_image(self) -> None:
        needed = set().union(*(imported_files(entry) for entry in ENTRY_MODULES))
        self.assertIn("scripts/ingestion/record_load.py", needed)
        self.assertIn("scripts/sample/candidates.py", needed)
        self.assertIn("scripts/aws_cli.py", needed)
        self.assertEqual([], sorted(needed - copied_from(DOCKERFILE.read_text(), "stack_scripts")))

    def test_the_release_pin_that_the_manifest_reader_opens_is_copied_to_where_the_code_looks(self) -> None:
        text = DOCKERFILE.read_text()
        self.assertIn(f"COPY --from=stack_fixtures release.json {IMAGE_APP}tests/fixtures/vitaldb/release.json", text)

    def test_a_missing_module_is_named_by_the_check(self) -> None:
        text = DOCKERFILE.read_text().replace("ingestion/record_state.py", "")
        needed = imported_files("scripts/ingestion/record_run.py")
        self.assertIn("scripts/ingestion/record_state.py", needed - copied_from(text, "stack_scripts"))


if __name__ == "__main__":
    unittest.main()
