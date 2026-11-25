"""The host modes of the runners start the pinned images that the host pulled, never a local tag that only the publisher's build makes.

The host builds nothing: it pulls the digests of ``compose/image_pins.yml`` and its Compose files name ``${IMAGE_<NAME>:?...}``. The runners are untracked, so
the tests run only where they are on disk. Nothing starts and no Docker, host or registry is reached.

Failure modes covered here:

- A host mode inspects or starts ``surgical-risk-spark:4.1.4`` or ``surgical-risk-dagster:1.13.31`` on the host, where no such tag exists, so its setup
  check fails or its container does not start.
- The pin file is read from the working tree (an uncommitted change) and not from the commit under test, or a failed ``git show`` is taken as an empty file.
- A runner that builds its own Spark image replaces a line of ``spark.yml`` that no longer exists, so the replacement does nothing and Compose stops with
  "render the image pins first".
- A pin file with no pin for an image, or one that is not the fixed shape, gives a reference anyway.
"""

from __future__ import annotations

import importlib
import inspect
import os
import re
import tempfile
import unittest
from pathlib import Path
from types import ModuleType

from scripts.images import pins
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
E2E = ROOT / "tests" / "e2e"
HELPER = E2E / "pinned_images.py"
HOST_RUNNERS = ("stream_host", "catalog_proof", "secret_sinks", "gold_dimensions", "silver_hypotension")
LOCAL_TAG = re.compile(r"surgical-risk-(spark|dagster|replay):[0-9A-Za-z._-]+")


def git(folder: Path, *arguments: str) -> None:
    own_repository = {name: value for name, value in os.environ.items() if not name.startswith("GIT_")}
    done = run_command("git", ["-C", str(folder), "-c", "user.name=t", "-c", "user.email=t@example.invalid", *arguments], timeout=30, env=own_repository)
    if done.returncode != 0:
        raise RuntimeError(done.stderr)


@unittest.skipUnless(HELPER.is_file(), "the temporary runners are not in this checkout")
class CommittedReferences(unittest.TestCase):
    def setUp(self) -> None:
        self.module: ModuleType = importlib.import_module("tests.e2e.pinned_images")
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.repository = Path(scratch.name)
        git(self.repository, "init", "--quiet")
        (self.repository / "compose").mkdir()

    def commit(self, text: str) -> None:
        (self.repository / "compose" / "image_pins.yml").write_text(text)
        git(self.repository, "add", "--", "compose/image_pins.yml")
        git(self.repository, "commit", "--quiet", "-m", "test: pins")

    def rendered(self) -> dict[str, str]:
        text = pins.render_environment(pins.load(), [image["name"] for image in pins.load_images()])
        return dict(line.split("=", 1) for line in text.splitlines())

    def test_the_references_are_the_ones_the_compose_stack_renders_from_the_committed_pin_file(self) -> None:
        self.commit((ROOT / "compose" / "image_pins.yml").read_text())
        references = self.module.committed_references(self.repository)
        self.assertEqual(self.rendered(), references)
        self.assertEqual({"IMAGE_DAGSTER", "IMAGE_REPLAY", "IMAGE_SPARK"}, set(references))
        for name, reference in references.items():
            with self.subTest(name=name):
                self.assertRegex(reference, r"^[^@\s]+@sha256:[0-9a-f]{64}$")
                self.assertIsNone(LOCAL_TAG.search(reference))

    def test_one_reference_by_image_name(self) -> None:
        self.commit((ROOT / "compose" / "image_pins.yml").read_text())
        self.assertEqual(self.rendered()["IMAGE_SPARK"], self.module.committed_reference("spark", self.repository))
        with self.assertRaises(self.module.PinnedImagesError):
            self.module.committed_reference("trino", self.repository)

    def test_the_commit_is_read_and_never_the_working_tree(self) -> None:
        text = (ROOT / "compose" / "image_pins.yml").read_text()
        self.commit(text)
        (self.repository / "compose" / "image_pins.yml").write_text(text.replace("images:", "images: {}\n#", 1))
        self.assertEqual(self.rendered(), self.module.committed_references(self.repository))

    def test_a_pin_file_that_is_not_committed_refuses(self) -> None:
        (self.repository / "compose" / "image_pins.yml").write_text((ROOT / "compose" / "image_pins.yml").read_text())
        git(self.repository, "commit", "--quiet", "--allow-empty", "-m", "test: empty")
        with self.assertRaises(self.module.PinnedImagesError):
            self.module.committed_references(self.repository)

    def test_a_folder_that_is_no_repository_refuses(self) -> None:
        with tempfile.TemporaryDirectory() as folder, self.assertRaises(self.module.PinnedImagesError):
            self.module.committed_references(Path(folder))

    def test_a_committed_pin_file_without_the_spark_pin_refuses(self) -> None:
        found = {name: value for name, value in pins.load().items() if name != "spark"}
        path = self.repository / "trimmed.yml"
        pins.write(path, found)
        self.commit(path.read_text())
        with self.assertRaises(self.module.PinnedImagesError):
            self.module.committed_references(self.repository)

    def test_a_committed_pin_file_of_another_shape_refuses(self) -> None:
        self.commit("images:\n  spark:\n    tag: surgical-risk-spark:4.1.4\n    digest: surgical-risk-spark:4.1.4\n")
        with self.assertRaises(self.module.PinnedImagesError):
            self.module.committed_references(self.repository)


@unittest.skipUnless(HELPER.is_file(), "the temporary runners are not in this checkout")
class LocalSparkImage(unittest.TestCase):
    def setUp(self) -> None:
        self.module: ModuleType = importlib.import_module("tests.e2e.pinned_images")
        self.text = (ROOT / "compose" / "services" / "spark.yml").read_text()

    def test_the_image_line_of_the_tracked_spark_file_names_the_runners_own_image_and_nothing_else_changes(self) -> None:
        changed = self.module.local_spark_image(self.text, "surgical-risk-spark:p7-dim")
        self.assertIn("    image: surgical-risk-spark:p7-dim\n", changed)
        self.assertNotIn("IMAGE_SPARK", changed)
        self.assertEqual(self.text.count("\n"), changed.count("\n"))
        self.assertEqual(
            [line for line in self.text.splitlines() if "IMAGE_SPARK" not in line],
            [line for line in changed.splitlines() if "p7-dim" not in line],
        )

    def test_a_file_with_no_image_line_or_two_stops_the_run(self) -> None:
        for text in ("services:\n  spark:\n    image: surgical-risk-spark:4.1.4\n", self.text + self.text):
            with self.subTest(text=text[:40]), self.assertRaises(self.module.PinnedImagesError):
                self.module.local_spark_image(text, "surgical-risk-spark:p7-dim")


@unittest.skipUnless(all((E2E / f"{name}.py").is_file() for name in HOST_RUNNERS), "the temporary runners are not in this checkout")
class HostModesNameNoLocalTag(unittest.TestCase):
    def source(self, name: str, function: str | None = None) -> str:
        module = importlib.import_module(f"tests.e2e.{name}")
        return inspect.getsource(getattr(module, function)) if function else (E2E / f"{name}.py").read_text()

    def test_the_stream_host_runner_names_no_local_tag_in_its_code(self) -> None:
        self.assertIsNone(LOCAL_TAG.search(self.source("stream_host")))

    def test_the_workload_cases_of_the_secret_sinks_host_mode_start_the_pinned_dagster_image(self) -> None:
        for function in ("workload_aws_cases", "metadata_cases", "run_host_workload"):
            with self.subTest(function=function):
                self.assertIsNone(LOCAL_TAG.search(self.source("secret_sinks", function)))
        self.assertIn('committed_reference("dagster")', self.source("secret_sinks", "workload_aws_cases"))
        self.assertIn('committed_reference("dagster")', self.source("secret_sinks", "metadata_cases"))

    def test_the_catalog_proof_host_mode_looks_for_the_pinned_spark_image_and_not_the_publishers_tag(self) -> None:
        source = self.source("catalog_proof", "run_host")
        self.assertIn('committed_reference("spark")', source)
        self.assertIsNone(re.search(r"\bIMAGE\b", source))
        self.assertIsNone(LOCAL_TAG.search(source))

    def test_the_runners_that_build_their_own_spark_image_use_the_helper_and_not_a_replacement_of_a_line_that_is_gone(self) -> None:
        spark = (ROOT / "compose" / "services" / "spark.yml").read_text()
        self.assertNotIn("surgical-risk-spark", spark)
        for name in ("gold_dimensions", "silver_hypotension"):
            with self.subTest(runner=name):
                source = self.source(name)
                self.assertNotIn('.replace("image: surgical-risk-spark', source)
                self.assertIn("local_spark_image(spark_file.read_text(), IMAGE)", source)


if __name__ == "__main__":
    unittest.main()
