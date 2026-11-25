"""The pin file names each project image by the digest that was pushed, and the host settings that Compose reads come from it and from nothing else."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.images import pins
from scripts.images.pins import PinError

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY = "example/project_images"
DIGEST = "a" * 64
HASH = "0123456789ab"
NAMES = ["dagster", "spark", "replay"]


def entry(name: str, digest: str = DIGEST, version: str = "1.0") -> dict[str, str]:
    return {"tag": f"{REPOSITORY}:{name}-{version}-{HASH}", "digest": f"{REPOSITORY}@sha256:{digest}"}


def complete() -> dict[str, dict[str, str]]:
    return {name: entry(name) for name in NAMES}


class RenderTest(unittest.TestCase):
    def test_each_image_becomes_one_sorted_variable_that_holds_its_digest_reference(self) -> None:
        text = pins.render_environment(complete(), NAMES)
        lines = text.splitlines()
        self.assertEqual(["IMAGE_DAGSTER", "IMAGE_REPLAY", "IMAGE_SPARK"], [line.split("=")[0] for line in lines])
        self.assertEqual(f"IMAGE_SPARK={REPOSITORY}@sha256:{DIGEST}", lines[2])
        self.assertTrue(text.endswith("\n"))

    def test_an_image_with_no_pin_stops_the_render_with_the_fixed_text_that_names_the_publish_command(self) -> None:
        for missing in NAMES:
            with self.subTest(missing=missing):
                found = {name: value for name, value in complete().items() if name != missing}
                with self.assertRaises(PinError) as raised:
                    pins.render_environment(found, NAMES)
                self.assertEqual(f"the image {missing} has no pin: run make images-push", str(raised.exception))

    def test_a_pin_for_an_image_that_the_list_does_not_name_is_refused(self) -> None:
        found = {**complete(), "extra": entry("extra")}
        with self.assertRaisesRegex(PinError, "extra"):
            pins.render_environment(found, NAMES)


class ValidateTest(unittest.TestCase):
    def problem(self, name: str, value: object) -> str:
        with self.assertRaises(PinError) as raised:
            pins.validate({name: value})
        return str(raised.exception)

    def test_a_good_entry_passes(self) -> None:
        pins.validate(complete())

    def test_a_digest_that_is_not_the_repository_with_sha256_and_64_hex_digits_is_refused(self) -> None:
        for digest in (
            f"{REPOSITORY}@sha256:{'a' * 63}",
            f"{REPOSITORY}@sha256:{'A' * 64}",
            f"other/repo@sha256:{DIGEST}",
            f"{REPOSITORY}:latest",
            f"{REPOSITORY}@sha256:{DIGEST}\n",
        ):
            with self.subTest(digest=digest):
                self.assertIn("digest", self.problem("spark", {**entry("spark"), "digest": digest}))

    def test_a_tag_that_is_not_the_repository_name_version_and_12_hex_digits_is_refused(self) -> None:
        for tag in (
            f"{REPOSITORY}:spark-1.0",
            f"{REPOSITORY}:other-1.0-{HASH}",
            f"{REPOSITORY}:spark-1.0-{HASH}ab",
            f"x/y:spark-1.0-{HASH}",
            f"{REPOSITORY}:spark-1.0-{HASH.upper()}",
        ):
            with self.subTest(tag=tag):
                self.assertIn("tag", self.problem("spark", {**entry("spark"), "tag": tag}))

    def test_a_tag_and_a_digest_of_different_repositories_and_two_repositories_in_one_file_are_refused(self) -> None:
        self.assertIn("different repositories", self.problem("spark", {"tag": f"x/y:spark-1.0-{HASH}", "digest": entry("spark")["digest"]}))
        other = {"tag": f"x/y:replay-1.0-{HASH}", "digest": f"x/y@sha256:{DIGEST}"}
        with self.assertRaisesRegex(PinError, "more than one repository"):
            pins.validate({"spark": entry("spark"), "replay": other})

    def test_a_field_that_is_missing_or_extra_is_refused(self) -> None:
        self.assertIn("fields", self.problem("spark", {"tag": entry("spark")["tag"]}))
        self.assertIn("fields", self.problem("spark", {**entry("spark"), "token": "x"}))
        self.assertIn("fields", self.problem("spark", "text"))


class FileTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.file = Path(folder.name) / "image_pins.yml"

    def test_a_pin_file_round_trips_in_name_order_and_the_missing_file_is_the_empty_pin_set(self) -> None:
        self.assertEqual({}, pins.load(self.file))
        pins.write(self.file, {"spark": entry("spark"), "dagster": entry("dagster")})
        self.assertEqual(["dagster", "spark"], list(pins.load(self.file)))
        self.assertEqual(entry("spark"), pins.load(self.file)["spark"])
        self.assertEqual(["image_pins.yml"], os.listdir(self.file.parent))

    def test_the_write_is_atomic_so_a_failed_replace_leaves_the_old_file_and_no_temporary_file(self) -> None:
        pins.write(self.file, {"spark": entry("spark")})
        before = self.file.read_text()
        with mock.patch("scripts.images.pins.os.replace", side_effect=OSError("disk")), self.assertRaises(PinError) as raised:
            pins.write(self.file, {"spark": entry("spark", "b" * 64)})
        self.assertEqual("the pin file could not be written", str(raised.exception))
        self.assertEqual(before, self.file.read_text())
        self.assertEqual(["image_pins.yml"], os.listdir(self.file.parent))

    def test_a_file_that_is_not_yaml_or_has_no_images_mapping_is_refused_with_fixed_text(self) -> None:
        for text in ("images: [", "images: 3\n", "other: {}\n", "images:\n  spark: {tag: x, digest: y}\n"):
            with self.subTest(text=text):
                self.file.write_text(text)
                with self.assertRaises(PinError):
                    pins.load(self.file)

    def test_a_repeated_name_is_refused(self) -> None:
        self.file.write_text("images:\n  spark: {}\n  spark: {}\n")
        with self.assertRaises(PinError):
            pins.load(self.file)


class RealFilesTest(unittest.TestCase):
    def test_the_tracked_pin_file_is_valid_and_is_either_empty_before_the_first_publish_or_complete(self) -> None:
        found = pins.load(pins.PIN_FILE)
        names = [image["name"] for image in pins.load_images()]
        self.assertEqual(sorted(names), sorted(names), "the list names each image once")
        self.assertEqual(len(names), len(set(names)))
        if found:
            self.assertEqual(sorted(names), sorted(found))
            for image in pins.load_images():
                self.assertTrue(found[image["name"]]["tag"].startswith(f"{REPOSITORY}:{image['name']}-{image['tag'].partition(':')[2]}-"))

    def test_the_images_come_from_the_one_list_in_the_group_variables_and_the_repository_is_not_a_group_variable(self) -> None:
        import yaml

        document = yaml.safe_load((ROOT / "ansible" / "group_vars" / "all.yml").read_text())
        self.assertEqual(document["compose_stack_built_images"], pins.load_images())
        self.assertNotIn("compose_stack_image_repository", document)


if __name__ == "__main__":
    unittest.main()
