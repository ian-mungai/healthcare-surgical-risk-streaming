"""The Dagster image takes every outside image and program by version and digest, and runs as the one user that the Compose files name.

The text of the Dockerfile is the contract here: the build reads these lines. The second group plants one change at a time.
"""

from __future__ import annotations

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


if __name__ == "__main__":
    unittest.main()
