"""The native Gold and Silver runners name their revision and their pinned Spark image from the source manifest when they run on the host checkout.

The project standard (owner, Nov 25 2026, "Host runners on the host") has the deploy place the tested commit's code with no Git history on the build and
test host, so that the native host-mode runners of phases 6 and 7 run there. ``recorded_state`` asks Git and exits 128 in that checkout. The runners are
untracked, so the tests run only where they are on disk. Nothing starts and no Docker, host or registry is reached.

Failure modes covered here:

- A host run asks Git for its revision, which fails in a checkout with no history, or names the revision of the working tree and not the manifest's commit.
- A host checkout with no manifest, or one that is not the fixed shape, names a revision anyway.
- The pinned Spark image that a host run takes is read from a pin file that differs from the manifest, or from a second read of the file.
- A run on the owner's computer (not the host) takes its revision from the manifest or its image from the pin file.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest import mock

from scripts.images import pins
from tests.fast.runner_import import import_runner, safe_execute

ROOT = Path(__file__).resolve().parents[2]
E2E = ROOT / "tests" / "e2e"
HELPER = E2E / "host_checkout.py"
ENTRY_POINTS = ("gold_dimensions", "silver_signals", "silver_hypotension")
PIN = "compose/image_pins.yml"


@unittest.skipUnless(HELPER.is_file(), "the temporary runners are not in this checkout")
class HostCheckout(unittest.TestCase):
    def setUp(self) -> None:
        self.module: ModuleType = import_runner("host_checkout")
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        (self.root / "compose").mkdir()
        self.write_pin_file((ROOT / PIN).read_text())

    def write_pin_file(self, text: str) -> None:
        (self.root / PIN).write_text(text)
        self.write_manifest({PIN: hashlib.sha256(text.encode()).hexdigest(), "compose/services/kafka.yml": "1" * 64})

    def write_manifest(self, files: dict[str, str], commit: str = "a" * 40) -> None:
        (self.root / "runner_manifest.json").write_text(json.dumps({"commit": commit, "files": files}))

    def test_the_revision_is_the_manifests_commit_and_the_digest_names_its_file_list_whatever_the_order(self) -> None:
        self.write_manifest({"b": "2" * 64, "a": "1" * 64}, commit="b" * 40)
        revision, digest = self.module.checkout_state(self.root, True)
        self.assertEqual("b" * 40, revision)
        self.assertEqual(hashlib.sha256(json.dumps({"a": "1" * 64, "b": "2" * 64}, sort_keys=True).encode()).hexdigest(), digest)
        self.write_manifest({"a": "1" * 64, "b": "2" * 64}, commit="b" * 40)
        self.assertEqual((revision, digest), self.module.checkout_state(self.root, True))

    def test_a_host_checkout_with_no_git_history_gives_a_revision_and_asks_no_process(self) -> None:
        with mock.patch.object(self.module, "recorded_state", side_effect=AssertionError("Git was asked")):
            self.assertEqual("a" * 40, self.module.checkout_state(self.root, True)[0])

    def test_a_computer_that_is_not_the_host_asks_git(self) -> None:
        with mock.patch.object(self.module, "recorded_state", return_value=("revision", "digest")) as asked:
            self.assertEqual(("revision", "digest"), self.module.checkout_state(self.root, False))
        asked.assert_called_once_with(self.root)

    def test_a_missing_or_malformed_manifest_refuses(self) -> None:
        (self.root / "runner_manifest.json").unlink()
        with self.assertRaises(self.module.HostCheckoutError):
            self.module.checkout_state(self.root, True)
        for text in ("not json", "[]", json.dumps({"commit": "short", "files": {"a": "1" * 64}}), json.dumps({"commit": "a" * 40, "files": {}}),
                     json.dumps({"commit": "a" * 40, "files": {"a": "xyz"}}), json.dumps({"commit": "a" * 40, "files": {"../a": "1" * 64}}),
                     json.dumps({"commit": "a" * 40, "files": {"a": "1" * 64}, "extra": 1})):  # fmt: skip
            with self.subTest(text=text[:30]):
                (self.root / "runner_manifest.json").write_text(text)
                with self.assertRaises(self.module.HostCheckoutError):
                    self.module.checkout_state(self.root, True)

    def test_the_pinned_reference_is_the_one_that_the_compose_stack_renders_from_the_pin_file(self) -> None:
        names = [image["name"] for image in pins.load_images()]
        rendered = dict(line.split("=", 1) for line in pins.render_environment(pins.load(), names).splitlines())
        for name in ("spark", "replay", "dagster"):
            with self.subTest(image=name):
                self.assertEqual(rendered[pins.variable_name(name)], self.module.pinned_reference(name, self.root))

    def test_a_pin_file_that_the_manifest_does_not_list_or_that_differs_from_it_or_is_gone_refuses(self) -> None:
        self.write_manifest({"compose/services/kafka.yml": "1" * 64})
        with self.assertRaises(self.module.HostCheckoutError):
            self.module.pinned_reference("spark", self.root)
        self.write_manifest({PIN: "f" * 64})
        with self.assertRaises(self.module.HostCheckoutError):
            self.module.pinned_reference("spark", self.root)
        self.write_pin_file((ROOT / PIN).read_text())
        (self.root / PIN).unlink()
        with self.assertRaises(self.module.HostCheckoutError):
            self.module.pinned_reference("spark", self.root)

    def test_a_pin_file_without_the_image_or_of_another_shape_refuses(self) -> None:
        found = {name: value for name, value in pins.load().items() if name != "spark"}
        path = self.root / "trimmed.yml"
        pins.write(path, found)
        self.write_pin_file(path.read_text())
        with self.assertRaises(self.module.HostCheckoutError):
            self.module.pinned_reference("spark", self.root)
        self.write_pin_file("images:\n  spark:\n    tag: surgical-risk-spark:4.1.4\n    digest: surgical-risk-spark:4.1.4\n")
        with self.assertRaises(self.module.HostCheckoutError):
            self.module.pinned_reference("spark", self.root)

    def test_the_reference_comes_from_the_bytes_that_were_hashed(self) -> None:
        pinned = pins.load()
        other = (ROOT / PIN).read_text().replace(pinned["spark"]["digest"][-64:], "e" * 64)
        original = Path.read_bytes
        swapped: list[bool] = []

        def swap_after_the_read(path: Path) -> bytes:
            data = original(path)
            if path.name == "image_pins.yml" and not swapped:
                swapped.append(True)
                path.write_text(other)
            return data

        with mock.patch.object(Path, "read_bytes", swap_after_the_read):
            reference = self.module.pinned_reference("spark", self.root)
        self.assertEqual([True], swapped)
        self.assertEqual(pinned["spark"]["digest"], reference)

    def test_a_refusal_names_the_file_and_never_a_value_of_it(self) -> None:
        self.write_pin_file("images:\n  spark:\n    tag: private_tag_value\n    digest: private_digest_value\n")
        with self.assertRaises(self.module.HostCheckoutError) as caught:
            self.module.pinned_reference("spark", self.root)
        self.assertNotIn("private", str(caught.exception))


@unittest.skipUnless(all((E2E / f"{name}.py").is_file() for name in (*ENTRY_POINTS, "host_checkout")), "the temporary runners are not in this checkout")
class EntryPoints(unittest.TestCase):
    def test_no_native_entry_point_asks_git_for_its_state_directly(self) -> None:
        for name in ENTRY_POINTS:
            with self.subTest(runner=name):
                source = (E2E / f"{name}.py").read_text()
                self.assertNotIn("recorded_state(", source)
                self.assertIn("checkout_state(ROOT, HOST)", source)

    def test_the_state_of_a_run_on_a_checkout_with_no_history_is_the_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "runner_manifest.json").write_text(json.dumps({"commit": "c" * 40, "files": {"spark/jobs/x.py": "1" * 64}}))
            for name in ENTRY_POINTS:
                with self.subTest(runner=name):
                    module = import_runner(name)
                    self.assertEqual("c" * 40, module.checkout_state(root, True)[0])


@unittest.skipUnless((E2E / "gold_dimensions.py").is_file() and HELPER.is_file(), "the temporary runners are not in this checkout")
class GoldHostImage(unittest.TestCase):
    """The Gold host mode runs the pinned Spark digest of its checkout when the operator gives no image, and the image that the operator gives otherwise.

    The runner reads its image when it is imported (its record texts print it as a constant), so each case imports a private copy under its own settings."""

    DIGEST = "ianmungai/healthcare_surgical_risk_streaming@sha256:" + "cd" * 32

    def load(self, pinned: object, **given: str) -> tuple[ModuleType | None, int | None, str]:
        """``(the module, None, "")`` or ``(None, the exit code, the standard error)`` of a private copy that is imported with the settings ``given``."""
        import_runner("gold_dimensions")
        errors = io.StringIO()
        environment = {name: value for name, value in os.environ.items() if name not in ("SPARK_IMAGE", "GOLD_DIMENSIONS_HOST")} | given
        with (
            mock.patch.dict(os.environ, environment, clear=True),
            mock.patch.dict(sys.modules),
            mock.patch.object(import_runner("host_checkout"), "pinned_reference", pinned),
            mock.patch.object(sys, "stderr", errors),
        ):
            try:
                return safe_execute(E2E / "gold_dimensions.py", "gold_host_copy"), None, ""
            except SystemExit as stop:
                return None, int(str(stop.code)), errors.getvalue()

    def loaded(self, pinned: object, **given: str) -> ModuleType:
        module, code, _ = self.load(pinned, **given)
        if module is None:
            self.fail(f"the runner stopped with exit {code}")
        return module

    def test_a_host_that_is_given_no_image_runs_the_pinned_digest_and_builds_nothing(self) -> None:
        asked: list[str] = []

        def reference(name: str) -> str:
            asked.append(name)
            return self.DIGEST

        module = self.loaded(reference, GOLD_DIMENSIONS_HOST="1")
        self.assertEqual((["spark"], self.DIGEST, False), (asked, module.IMAGE, module.BUILT_HERE))

    def test_an_image_that_the_operator_gives_is_the_image_and_the_pin_file_is_not_read(self) -> None:
        pinned = mock.Mock(side_effect=AssertionError("the pin file was read"))
        module = self.loaded(pinned, GOLD_DIMENSIONS_HOST="1", SPARK_IMAGE=self.DIGEST)
        self.assertEqual((self.DIGEST, False), (module.IMAGE, module.BUILT_HERE))

    def test_a_host_whose_pin_file_cannot_be_used_stops_with_fixed_words_before_any_work(self) -> None:
        failure = import_runner("host_checkout").HostCheckoutError("compose/image_pins.yml differs from the source manifest")
        module, code, errors = self.load(mock.Mock(side_effect=failure), GOLD_DIMENSIONS_HOST="1")
        self.assertEqual((None, 2), (module, code))
        self.assertEqual("gold_dimensions: compose/image_pins.yml differs from the source manifest\n", errors)

    def test_a_run_on_this_computer_builds_under_its_own_tag_and_never_reads_the_pin_file(self) -> None:
        module = self.loaded(mock.Mock(side_effect=AssertionError("the pin file was read")))
        self.assertEqual(("surgical-risk-spark:p7-dim", True), (module.IMAGE, module.BUILT_HERE))
