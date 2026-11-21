"""The Spark image is built only from pinned inputs that the build checks, and nothing is downloaded when a job starts.

A base image taken by a moving tag or a jar taken without its checksum changes the platform silently, and an image that fetches a package at
start gives a job a different result on each run. These checks read the tracked Dockerfile and its ignore file.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = ROOT / "spark" / "Dockerfile"
IGNORE_FILE = ROOT / "spark" / "Dockerfile.dockerignore"
PINNED_IMAGE = re.compile(r"^[a-z0-9./_-]+:(?!latest@)[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}$")


def lines() -> list[str]:
    """The instructions of the Dockerfile, with each continued line joined and each comment and blank line dropped."""
    joined = DOCKERFILE.read_text().replace("\\\n", " ")
    return [line.strip() for line in joined.splitlines() if line.strip() and not line.lstrip().startswith("#")]


def arguments() -> dict[str, str]:
    return {name: value for name, _, value in (line[4:].partition("=") for line in lines() if line.startswith("ARG ") and "=" in line)}


def expanded(text: str) -> str:
    values = arguments()
    return re.sub(r"\$\{(\w+)\}", lambda match: values.get(match.group(1), match.group(0)), text)


class ImageTest(unittest.TestCase):
    def test_every_base_image_is_for_x86_64_and_pinned_by_a_tag_and_a_digest(self) -> None:
        stages: set[str] = set()
        bases = 0
        for line in lines():
            if not line.startswith("FROM "):
                continue
            parts = expanded(line).split()
            self.assertIn("--platform=linux/amd64", parts, line)
            image = next(part for part in parts[1:] if not part.startswith("--"))
            if image not in stages:
                bases += 1
                self.assertRegex(image, PINNED_IMAGE, line)
            if "AS" in parts:
                stages.add(parts[parts.index("AS") + 1])
        self.assertGreaterEqual(bases, 2)

    def test_python_is_3_14_the_project_target(self) -> None:
        self.assertTrue(arguments()["PYTHON_IMAGE"].startswith("python:3.14."))

    def test_every_download_is_https_and_checked_against_a_sha256_at_build(self) -> None:
        downloads = [line for line in lines() if line.startswith("ADD ")]
        self.assertGreaterEqual(len(downloads), 3, "Spark, the Iceberg runtime and the object store bundle")
        for line in downloads:
            with self.subTest(line=line[:90]):
                self.assertRegex(expanded(line), r"^ADD --checksum=sha256:[0-9a-f]{64} https://\S+ \S+$")

    def test_the_iceberg_runtime_matches_the_spark_and_scala_versions_of_the_image(self) -> None:
        values = arguments()
        spark_minor = ".".join(values["SPARK_VERSION"].split(".")[:2])
        runtime = f"iceberg-spark-runtime-{spark_minor}_{values['SCALA_VERSION']}-{values['ICEBERG_VERSION']}.jar"
        self.assertTrue(any(runtime in expanded(line) for line in lines() if line.startswith("ADD ")), runtime)

    def test_nothing_is_fetched_when_a_job_starts(self) -> None:
        for line in lines():
            if line.startswith(("ENTRYPOINT", "CMD")):
                self.assertNotRegex(line, r"curl|wget|pip|apt|ivy|--packages", line)
        self.assertNotIn("spark.jars.packages", DOCKERFILE.read_text())

    def test_the_image_runs_as_one_numeric_user_and_never_as_root(self) -> None:
        users = [line.split()[1] for line in lines() if line.startswith("USER ")]
        self.assertEqual(1, len(users))
        self.assertRegex(users[0], r"^[1-9][0-9]*:[1-9][0-9]*$")

    def test_the_build_context_admits_only_what_the_image_copies(self) -> None:
        rules = [line.strip() for line in IGNORE_FILE.read_text().splitlines() if line.strip() and not line.startswith("#")]
        self.assertEqual("*", rules[0], "everything is excluded first, so the environment file and the run records never enter the context")
        copied = {line.split()[1].rstrip("/") for line in lines() if line.startswith("COPY ") and "--from=" not in line}
        allowed = {rule[1:].rstrip("/") for rule in rules[1:] if rule.startswith("!")}
        self.assertLessEqual(copied, allowed)
        self.assertLessEqual(allowed, copied)


if __name__ == "__main__":
    unittest.main()
