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

import inspect
import json
import os
import re
import shutil
import tempfile
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

import yaml

from scripts.images import pins
from scripts.process import run_command
from tests.fast.runner_import import import_runner

ROOT = Path(__file__).resolve().parents[2]
E2E = ROOT / "tests" / "e2e"
HELPER = E2E / "pinned_images.py"
HOST_RUNNERS = ("stream_host", "catalog_proof", "secret_sinks", "gold_dimensions", "silver_hypotension")
# The runners that build their own Spark image and name it in a copy of ``spark.yml`` (the host modes of the two Silver runners and the six runners of the
# local mode only).
LOCAL_BUILDERS = (
    "gold_dimensions",
    "silver_hypotension",
    "silver_signals",
    "silver_cases",
    "silver_labels",
    "silver_patient_split",
    "silver_reconcile",
    "signal_profile",
)
SILVER_HOST_MODES = ("silver_signals", "silver_hypotension")
# A tag that only the publisher's build makes (a version after the image name). The run's own tag of a Silver runner (``p6-sig``) is no such tag.
PUBLISHER_TAG = re.compile(r"surgical-risk-(spark|dagster|replay):[0-9]+\.[0-9]+\.[0-9]+")
LOCAL_TAG = re.compile(r"surgical-risk-(spark|dagster|replay):[0-9A-Za-z._-]+")


def git(folder: Path, *arguments: str) -> None:
    own_repository = {name: value for name, value in os.environ.items() if not name.startswith("GIT_")}
    done = run_command("git", ["-C", str(folder), "-c", "user.name=t", "-c", "user.email=t@example.invalid", *arguments], timeout=30, env=own_repository)
    if done.returncode != 0:
        raise RuntimeError(done.stderr)


@unittest.skipUnless(HELPER.is_file(), "the temporary runners are not in this checkout")
class CommittedReferences(unittest.TestCase):
    def setUp(self) -> None:
        self.module: ModuleType = import_runner("pinned_images")
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.repository = Path(scratch.name)
        git(self.repository, "init", "--quiet")
        (self.repository / "compose").mkdir()

    def commit(self, text: str, images: str | None = None) -> None:
        """Commit the pin file and the image list of the deploy (the repository's own, or ``images``)."""
        (self.repository / "compose" / "image_pins.yml").write_text(text)
        listing = self.repository / "ansible" / "group_vars" / "all.yml"
        listing.parent.mkdir(parents=True, exist_ok=True)
        listing.write_text((ROOT / "ansible" / "group_vars" / "all.yml").read_text() if images is None else images)
        git(self.repository, "add", "--", "compose/image_pins.yml", "ansible/group_vars/all.yml")
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

    SPARK_ONLY = "compose_stack_built_images:\n  - name: spark\n    tag: surgical-risk-spark:4.1.4\n"

    def spark_only_pins(self) -> str:
        path = self.repository / "spark_only.yml"
        pins.write(path, {"spark": pins.load()["spark"]})
        return path.read_text()

    def test_the_image_list_is_read_from_the_commit_and_never_from_the_working_tree(self) -> None:
        self.commit(self.spark_only_pins(), self.SPARK_ONLY)
        self.assertEqual({"IMAGE_SPARK"}, set(self.module.committed_references(self.repository)))
        (self.repository / "ansible" / "group_vars" / "all.yml").write_text((ROOT / "ansible" / "group_vars" / "all.yml").read_text())
        self.assertEqual({"IMAGE_SPARK"}, set(self.module.committed_references(self.repository)))

    def test_both_files_come_from_one_commit_that_is_resolved_once(self) -> None:
        self.commit((ROOT / "compose" / "image_pins.yml").read_text())
        calls: list[list[str]] = []
        original = self.module.run_command

        def spy(program: str, arguments: list[str], **options: Any) -> Any:
            calls.append(list(arguments))
            return original(program, arguments, **options)

        with mock.patch.object(self.module, "run_command", spy):
            self.module.committed_references(self.repository)
        shown = [call for call in calls if "show" in call]
        self.assertEqual(2, len(shown))
        for call in shown:
            self.assertRegex(call[-1], r"^[0-9a-f]{40}:(compose/image_pins\.yml|ansible/group_vars/all\.yml)$")
        self.assertEqual(1, len({call[-1].split(":")[0] for call in shown}))

    def test_a_commit_that_lands_between_the_two_reads_changes_neither(self) -> None:
        self.commit(self.spark_only_pins(), self.SPARK_ONLY)
        original = self.module.run_command
        moved: list[bool] = []

        def move_after_the_first_read(program: str, arguments: list[str], **options: Any) -> Any:
            done = original(program, arguments, **options)
            if "show" in arguments and not moved:
                moved.append(True)
                self.commit((ROOT / "compose" / "image_pins.yml").read_text())
            return done

        with mock.patch.object(self.module, "run_command", move_after_the_first_read):
            references = self.module.committed_references(self.repository)
        self.assertEqual([True], moved)
        self.assertEqual({"IMAGE_SPARK"}, set(references))

    def test_a_commit_without_the_image_list_refuses(self) -> None:
        self.commit((ROOT / "compose" / "image_pins.yml").read_text(), "other: 1\n")
        with self.assertRaises(self.module.PinnedImagesError):
            self.module.committed_references(self.repository)

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
        self.module: ModuleType = import_runner("pinned_images")
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


@unittest.skipUnless(all((E2E / f"{name}.py").is_file() for name in (*HOST_RUNNERS, *LOCAL_BUILDERS)), "the temporary runners are not in this checkout")
class HostModesNameNoLocalTag(unittest.TestCase):
    def source(self, name: str, function: str | None = None) -> str:
        module = import_runner(name)
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
        source = self.source("catalog_proof", "run_host") + self.source("catalog_proof", "spark_image_state")
        self.assertIn('committed_reference("spark")', source)
        self.assertIsNone(re.search(r"\bIMAGE\b", source))
        self.assertIsNone(LOCAL_TAG.search(source))

    def test_the_runners_that_build_their_own_spark_image_use_the_helper_and_not_a_replacement_of_a_line_that_is_gone(self) -> None:
        spark = (ROOT / "compose" / "services" / "spark.yml").read_text()
        self.assertNotIn("surgical-risk-spark", spark)
        for name in LOCAL_BUILDERS:
            with self.subTest(runner=name):
                source = self.source(name)
                self.assertNotIn('.replace("image: surgical-risk-spark', source)
                self.assertIn("local_spark_image(spark_file.read_text(), IMAGE)", source)


def unset_variables(compose: Path, host_env: str) -> set[str]:
    """The ``${NAME:?...}`` settings of the service files that the project's root file includes and that neither the host settings nor the limits file give:
    what Compose stops on when it loads the project (it reads every included file, whichever service the command names)."""
    include = yaml.safe_load((compose / "compose.yml").read_text())["include"]
    wanted: set[str] = set()
    for entry in include:
        wanted |= set(re.findall(r"\$\{([A-Z0-9_]+):\?", (compose / entry).read_text()))
    given = {line.split("=", 1)[0] for line in host_env.splitlines() if "=" in line}
    given |= {line.split("=", 1)[0] for line in (ROOT / "compose" / "limits.yml").read_text().splitlines() if "=" in line}
    limits = {name for name in wanted if name.startswith("MEM_LIMIT_")}
    return wanted - given - limits - {"DOCKER_SOCKET_GID"}


@unittest.skipUnless(HELPER.is_file(), "the temporary runners are not in this checkout")
class ScratchProject(unittest.TestCase):
    """A scratch Compose project holds the services that its runner starts and what they wait for, and the image settings of exactly those."""

    def setUp(self) -> None:
        self.module: ModuleType = import_runner("pinned_images")
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.compose = Path(scratch.name) / "compose"
        shutil.copytree(ROOT / "compose", self.compose)

    def included(self) -> list[str]:
        return [Path(entry).stem for entry in yaml.safe_load((self.compose / "compose.yml").read_text())["include"]]

    def test_the_tracked_project_stops_on_the_images_that_a_scratch_host_env_lacks(self) -> None:
        host_env = "SECRETS_DIR=/x\nPOSTGRES_HOST_PORT=1\nNESSIE_HOST_PORT=2\nWAREHOUSE_LOCATION=file:///warehouse/\n"
        self.assertEqual(
            {"IMAGE_DAGSTER", "IMAGE_REPLAY", "IMAGE_SPARK"}, {name for name in unset_variables(self.compose, host_env) if name.startswith("IMAGE_")}
        )

    def test_a_project_of_postgres_nessie_and_the_spark_job_includes_those_three_and_needs_the_spark_image_only(self) -> None:
        lines = self.module.scratch_project(self.compose, ("postgres", "nessie", "spark"), {"IMAGE_SPARK": "example.invalid/spark:run"})
        self.assertEqual(["nessie", "postgres", "spark"], sorted(self.included()))
        self.assertEqual("IMAGE_SPARK=example.invalid/spark:run\n", lines)
        self.assertEqual(set(), {name for name in unset_variables(self.compose, lines) if name.startswith("IMAGE_")})

    def test_the_services_that_a_service_waits_for_are_included_with_it(self) -> None:
        self.module.scratch_project(self.compose, ("spark",), {"IMAGE_SPARK": "example.invalid/spark:run"})
        self.assertEqual({"nessie", "postgres", "spark"}, set(self.included()))

    def test_the_three_dagster_services_need_the_dagster_image_and_no_other(self) -> None:
        names = ("dagster_code_location", "dagster_daemon", "dagster_webserver")
        lines = self.module.scratch_project(self.compose, names, {"IMAGE_DAGSTER": "example.invalid/dagster:run"})
        self.assertEqual({*names, "postgres"}, set(self.included()))
        self.assertEqual("IMAGE_DAGSTER=example.invalid/dagster:run\n", lines)

    def test_a_project_of_services_with_no_project_image_needs_no_image_setting(self) -> None:
        self.assertEqual("", self.module.scratch_project(self.compose, ("postgres", "nessie", "trino")))
        self.assertEqual({"nessie", "postgres", "trino"}, set(self.included()))

    def test_the_project_name_and_the_order_of_the_tracked_file_are_kept(self) -> None:
        tracked = yaml.safe_load((ROOT / "compose" / "compose.yml").read_text())
        self.module.scratch_project(self.compose, ("trino", "postgres"))
        document = yaml.safe_load((self.compose / "compose.yml").read_text())
        self.assertEqual(tracked["name"], document["name"])
        self.assertEqual([entry for entry in tracked["include"] if Path(entry).stem in {"postgres", "nessie", "trino"}], document["include"])

    def test_a_missing_or_an_unneeded_image_setting_stops_the_run(self) -> None:
        with self.assertRaises(self.module.PinnedImagesError):
            self.module.scratch_project(self.compose, ("spark",))
        with self.assertRaises(self.module.PinnedImagesError):
            self.module.scratch_project(self.compose, ("postgres", "nessie"), {"IMAGE_SPARK": "example.invalid/spark:run"})
        with self.assertRaises(self.module.PinnedImagesError):
            self.module.scratch_project(self.compose, ("spark",), {"IMAGE_SPARK": "example.invalid/spark:run", "IMAGE_REPLAY": "example.invalid/replay:run"})

    def test_a_name_that_the_tracked_project_does_not_include_stops_the_run(self) -> None:
        with self.assertRaises(self.module.PinnedImagesError):
            self.module.scratch_project(self.compose, ("postgres", "no_such_service"))
        with self.assertRaises(self.module.PinnedImagesError):
            self.module.scratch_project(self.compose, ())

    def test_a_spark_file_that_names_the_runners_own_image_needs_no_image_setting(self) -> None:
        spark_file = self.compose / "services" / "spark.yml"
        spark_file.write_text(self.module.local_spark_image(spark_file.read_text(), "surgical-risk-spark:p7-dim"))
        self.assertEqual("", self.module.scratch_project(self.compose, ("postgres", "nessie", "spark")))


# The runners that copy ``compose/`` and start a project of their own, and the services that each one starts. A runner that is not listed here and copies the
# folder fails ``EveryScratchProjectIsTrimmed``; one that starts a service that its constant lacks fails when it runs (Compose names the missing service).
SCRATCH_RUNNERS = (
    "bronze_clinical_lab",
    "bronze_decode",
    "catalog_proof",
    "gold_dimensions",
    "retention_stack",
    "signal_profile",
    "silver_cases",
    "silver_hypotension",
    "silver_labels",
    "silver_patient_split",
    "silver_reconcile",
    "silver_signals",
    "stack_dagster",
    "stack_jobs_records",
    "stack_monitoring",
    "stack_nessie",
    "stack_openbao",
)
# Runners that copy the folder and already name the services that they run in their own root file, or that start one service file alone.
OWN_ROOT_FILE = ("stack_groups", "stack_kafka", "stack_caddy")


@unittest.skipUnless(
    all((E2E / f"{name}.py").is_file() for name in (*SCRATCH_RUNNERS, *OWN_ROOT_FILE, "pinned_images")), "the temporary runners are not in this checkout"
)
class EveryScratchProjectIsTrimmed(unittest.TestCase):
    def source(self, name: str) -> str:
        return (E2E / f"{name}.py").read_text()

    def test_every_runner_that_copies_the_compose_folder_is_listed_here_or_names_its_own_root_file(self) -> None:
        copying = {path.stem for path in E2E.glob("*.py") if 'copytree(ROOT / "compose"' in path.read_text()}
        self.assertEqual(copying, {*SCRATCH_RUNNERS, *OWN_ROOT_FILE} & copying)
        self.assertEqual(set(), copying - {*SCRATCH_RUNNERS, *OWN_ROOT_FILE})

    def test_each_listed_runner_trims_its_copy_to_the_services_that_it_names_and_the_image_settings_of_those(self) -> None:
        helper = import_runner("pinned_images")
        for name in SCRATCH_RUNNERS:
            with self.subTest(runner=name):
                module = import_runner(name)
                services = module.SCRATCH_SERVICES
                self.assertIsInstance(services, tuple)
                self.assertTrue(services)
                source = self.source(name)
                self.assertEqual(1, source.count("scratch_project("))
                self.assertIn("SCRATCH_SERVICES", source.split("scratch_project(", 1)[1].split(")", 1)[0])
                scratch = Path(tempfile.mkdtemp())
                self.addCleanup(shutil.rmtree, scratch, True)
                shutil.copytree(ROOT / "compose", scratch / "compose")
                spark_file = scratch / "compose" / "services" / "spark.yml"
                if "local_spark_image(" in source:
                    spark_file.write_text(helper.local_spark_image(spark_file.read_text(), "surgical-risk-spark:run"))
                needed = {
                    variable
                    for service in helper.closure(scratch / "compose", services)
                    for variable in re.findall(r"\$\{(IMAGE_[A-Z]+):\?", (scratch / "compose" / "services" / f"{service}.yml").read_text())
                }
                for variable in ("IMAGE_DAGSTER", "IMAGE_REPLAY", "IMAGE_SPARK"):
                    self.assertEqual(variable in needed, f'"{variable}"' in source, variable)
                if '"IMAGE_' in source:
                    self.assertRegex(source, r"\bimages\s*\+|\+\s*images\b", "the image settings that the project returns reach the host settings file")
                if "local_spark_image(" in source or '"IMAGE_SPARK"' in source:
                    self.assertIn("spark", services, "a runner that names a Spark image runs the Spark service")


@unittest.skipUnless((E2E / "catalog_proof.py").is_file(), "the temporary runners are not in this checkout")
class CatalogProofHostSetup(unittest.TestCase):
    """The setup of the catalog proof's host mode runs no case unless the Spark image that the host's Compose renders is the pinned image of the commit."""

    PINNED = "example.invalid/x@sha256:" + "a" * 64
    OTHER = "example.invalid/x@sha256:" + "b" * 64

    def setUp(self) -> None:
        self.module: ModuleType = import_runner("catalog_proof")

    def setup_run(self, rendered: Any, config_exit: int = 0) -> tuple[list[tuple[str, bool]], list[str]]:
        """Run the host mode up to its first expectation, which stops it. ``rendered`` is the Spark image that ``docker compose config`` gives on the host (or
        None for no image line). Returns the expectations that were recorded and the images that the host was asked about."""
        shown: list[tuple[str, bool]] = []
        inspected: list[str] = []
        document: dict[str, Any] = {"services": {"spark": {} if rendered is None else {"image": rendered}}}

        def docker(*arguments: str) -> tuple[int, str]:
            inspected.append(arguments[-1])
            return 0, ""

        def expect(_: str, __: str, ___: str, observed: str, passed: bool) -> bool:
            shown.append((observed, passed))
            return False

        stack = SimpleNamespace(health=lambda _: "healthy", docker=docker, compose_out=lambda *_: (config_exit, json.dumps(document)))
        record = SimpleNamespace(expect=expect)
        with (
            mock.patch.object(self.module, "Remote", lambda: SimpleNamespace()),
            mock.patch.object(self.module, "Stack", lambda *_: stack),
            mock.patch.object(self.module, "load_configuration", lambda: {"S3_BUCKET": "bucket"}),
            mock.patch.object(self.module, "configured_warehouse_location", lambda: "s3://example/"),
            mock.patch.object(self.module, "ExecNessie", lambda *_: SimpleNamespace()),
            mock.patch.object(self.module, "ExecTrino", lambda *_: SimpleNamespace()),
            mock.patch.object(self.module, "Proof", lambda *_: SimpleNamespace()),
            mock.patch.object(self.module, "engine_locations", lambda _: {"trino": "s3://example/", "spark": "s3://example/"}),
            mock.patch.object(self.module, "committed_reference", lambda _: self.PINNED),
        ):
            self.module.run_host(record, [])
        return shown, inspected

    def test_a_host_that_renders_the_pinned_image_passes_the_setup(self) -> None:
        shown, inspected = self.setup_run(self.PINNED)
        self.assertEqual([True], [passed for _, passed in shown])
        self.assertEqual([self.PINNED], inspected)

    def test_a_host_that_renders_another_image_refuses_before_any_case_though_the_pinned_image_is_cached(self) -> None:
        shown, inspected = self.setup_run(self.OTHER)
        self.assertEqual([False], [passed for _, passed in shown])
        self.assertIn(self.PINNED, inspected)

    def test_a_host_whose_rendered_settings_cannot_be_read_or_name_no_image_refuses(self) -> None:
        for rendered, exit_code in ((self.PINNED, 1), (None, 0), ("", 0)):
            with self.subTest(rendered=rendered, exit_code=exit_code):
                shown, _ = self.setup_run(rendered, exit_code)
                self.assertEqual([False], [passed for _, passed in shown])


@unittest.skipUnless((E2E / "bronze_host.py").is_file(), "the temporary runners are not in this checkout")
class BronzeHostMode(unittest.TestCase):
    """The Bronze host mode inspects and runs the pinned Spark and replay images of the commit, never the publisher's local tags."""

    def setUp(self) -> None:
        self.module: ModuleType = import_runner("bronze_host")

    def test_the_runner_names_no_local_tag_and_no_image_name_of_the_decoder_runner(self) -> None:
        source = (E2E / "bronze_host.py").read_text()
        self.assertIsNone(PUBLISHER_TAG.search(source))
        self.assertNotIn("decode.REPLAY_IMAGE", source)
        self.assertNotIn("proof.IMAGE", source)

    def test_the_stage_of_the_deployed_stack_looks_for_the_pinned_images(self) -> None:
        source = inspect.getsource(self.module.stage_stack)
        self.assertIn("committed_reference(name)", source)
        self.assertIn('("spark", "replay")', source)

    def test_the_replay_container_starts_the_pinned_replay_image(self) -> None:
        source = inspect.getsource(self.module.HostStack.replay)
        self.assertIn('committed_reference("replay")', source)
        self.assertNotIn("REPLAY_IMAGE", source)

    def stage(self, reference: Any) -> tuple[list[str], list[tuple[str, bool]]]:
        """Run the stage of the deployed stack on stand-ins that stop it at its first expectation, with ``reference`` standing in for the pin file reader.
        Returns the images that it asked the host about and the observations that it recorded."""
        inspected: list[str] = []
        shown: list[tuple[str, bool]] = []

        def docker(*arguments: str) -> tuple[int, str]:
            inspected.append(arguments[-1])
            return 0, ""

        def expect(_: str, __: str, ___: str, observed: str, passed: bool) -> bool:
            shown.append((observed, passed))
            return False

        stack = SimpleNamespace(health=lambda _: "healthy", docker=docker, compose_out=lambda *_: (0, ""), target=SimpleNamespace(run=lambda *_: (0, "")))
        run = SimpleNamespace(stack=stack, record=SimpleNamespace(expect=expect))
        with (
            mock.patch.object(self.module, "committed_reference", reference),
            mock.patch.object(self.module, "configured_warehouse_location", lambda: "s3://example/"),
        ):
            self.assertFalse(self.module.stage_stack(run))
        return inspected, shown

    def test_the_stage_asks_the_host_about_the_pinned_references_of_the_commit(self) -> None:
        spark, replay = "example.invalid/x@sha256:" + "a" * 64, "example.invalid/x@sha256:" + "b" * 64
        inspected, _ = self.stage(lambda name: {"spark": spark, "replay": replay}[name])
        self.assertEqual([spark, replay], inspected)

    def test_a_commit_with_no_readable_pin_counts_as_images_that_are_not_on_the_host_and_asks_the_host_about_none(self) -> None:
        def unreadable(name: str) -> str:
            raise self.module.PinnedImagesError(name)

        inspected, shown = self.stage(unreadable)
        self.assertEqual([], inspected)
        self.assertEqual([False], [passed for _, passed in shown])


@unittest.skipUnless(all((E2E / f"{name}.py").is_file() for name in SILVER_HOST_MODES), "the temporary runners are not in this checkout")
class SilverHostModes(unittest.TestCase):
    """The host modes of the Silver runners start their own image tag and name no local tag of the publisher, also in the text for the caller."""

    def test_no_runner_names_a_tag_that_only_the_publisher_makes(self) -> None:
        for name in SILVER_HOST_MODES:
            with self.subTest(runner=name):
                self.assertIsNone(PUBLISHER_TAG.search((E2E / f"{name}.py").read_text()))

    def test_the_text_for_the_caller_points_to_the_pin_file_for_the_given_image(self) -> None:
        for name in SILVER_HOST_MODES:
            with self.subTest(runner=name):
                self.assertIn("compose/image_pins.yml", (E2E / f"{name}.py").read_text())


if __name__ == "__main__":
    unittest.main()
