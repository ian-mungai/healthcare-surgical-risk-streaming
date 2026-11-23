"""The shapes that the JSON entry point and a direct call with a mapping accept: closed objects, no repeated member and no default for a missing key.

Failure modes covered here:

- A key that the table does not know is ignored (a misspelled ``start_offset_usec`` read as offset zero), or a required key that is missing gets a default.
- A member that appears twice in one object lets the later one overwrite the earlier, at any depth.
- A window ID that is not a text, or that two windows share, reaches the output.
- The direct call takes a shape that the file refuses, or the other way round.
- An object kind is added to the entry point without its closed key set in the table and without a test.

Every object kind of ``INPUT_OBJECTS`` is tested from the table, so a new kind fails ``test_every_object_kind_of_the_table_has_its_place_in_the_tests``.
"""

from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from types import MappingProxyType
from typing import Any

from features import batch, document
from features.model import FeatureInputError
from features.registry import INPUT_OBJECTS
from features.row import build_row
from tests.fast.test_features_batch import window_document

WAVEFORM = "SNUADC/ART"
LOCATORS: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    "file": lambda file: file,
    "window": lambda file: file["windows"][0],
    "waveform": lambda file: file["windows"][0]["waveforms"][WAVEFORM],
    "second": lambda file: file["windows"][0]["waveforms"][WAVEFORM]["seconds"][0],
    "clinical_value": lambda file: file["windows"][0]["clinical"]["age"],
}
# The objects keyed by data (track names, field names): the places where a member can be repeated besides the closed objects.
MAPS: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    "numeric": lambda file: file["windows"][0]["numeric"],
    "waveforms": lambda file: file["windows"][0]["waveforms"],
    "clinical": lambda file: file["windows"][0]["clinical"],
}


class Repeated(dict[str, Any]):
    """An object that the serializer below writes with its first member twice."""


def dumps(value: Any) -> str:
    if isinstance(value, dict):
        items = list(value.items())
        if isinstance(value, Repeated):
            items = [*items, items[0]]
        return "{" + ", ".join(f"{json.dumps(key)}: {dumps(item)}" for key, item in items) + "}"
    if isinstance(value, list):
        return "[" + ", ".join(dumps(item) for item in value) + "]"
    return json.dumps(value)


def good_file() -> dict[str, Any]:
    return {"windows": [window_document("w")]}


def refused_directly(kind: str, file: dict[str, Any]) -> str:
    """The message of the direct path for the same shape: a mapping to ``build_row``, or the whole file to ``parse_file``."""
    try:
        if kind == "file":
            document.parse_file(file)
        else:
            build_row(file["windows"][0])
    except FeatureInputError as error:
        return str(error)
    raise AssertionError("the direct path took the shape")


class Entry(unittest.TestCase):
    def setUp(self) -> None:
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)

    def run_text(self, text: str) -> tuple[int, str, Path]:
        """Run the entry point on the text of a file, to a target that is free."""
        source, target = self.root / "windows.json", self.root / "rows.json"
        source.write_text(text)
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            code = batch.main([str(source), str(target)])
        return code, stderr.getvalue(), target

    def assert_refused(self, text: str, *patterns: str) -> None:
        code, message, target = self.run_text(text)
        self.assertEqual(2, code, message)
        for pattern in patterns:
            self.assertIn(pattern, message)
        self.assertFalse(target.exists())


class ClosedKeys(Entry):
    def test_every_object_kind_of_the_table_has_its_place_in_the_tests(self) -> None:
        self.assertEqual(set(INPUT_OBJECTS), set(LOCATORS))
        self.assertTrue(all(INPUT_OBJECTS[kind] for kind in INPUT_OBJECTS))

    def test_a_good_file_is_accepted_by_both_paths(self) -> None:
        code, message, target = self.run_text(json.dumps(good_file()))
        self.assertEqual(0, code, message)
        self.assertEqual(batch.serialise([("w", build_row(good_file()["windows"][0]))]), target.read_text())

    def test_a_read_only_mapping_is_read_like_a_dict_at_every_depth(self) -> None:
        window = good_file()["windows"][0]
        frozen = MappingProxyType({key: MappingProxyType(value) if isinstance(value, dict) else value for key, value in window.items()})
        self.assertEqual(build_row(window), build_row(frozen))

    def test_the_reviewers_misspelled_offset_is_refused_by_name(self) -> None:
        file = good_file()
        second = LOCATORS["second"](file)
        second["start_offset_usec"] = second.pop("start_offset_us")
        self.assert_refused(json.dumps(file), "start_offset_usec")
        self.assertIn("start_offset_usec", refused_directly("second", file))

    def test_an_unknown_key_in_any_object_kind_is_refused_by_name_by_both_paths(self) -> None:
        for kind in INPUT_OBJECTS:
            with self.subTest(kind=kind):
                file = good_file()
                LOCATORS[kind](file)["zz_unknown"] = 1
                self.assert_refused(json.dumps(file), "zz_unknown")
                self.assertIn("zz_unknown", refused_directly(kind, file))

    def test_a_missing_key_in_any_object_kind_is_refused_by_name_by_both_paths_and_given_no_default(self) -> None:
        for kind, keys in INPUT_OBJECTS.items():
            for key in keys:
                with self.subTest(kind=kind, key=key):
                    file = good_file()
                    del LOCATORS[kind](file)[key]
                    self.assert_refused(json.dumps(file), key)
                    self.assertIn(key, refused_directly(kind, file))


class Repeats(Entry):
    def test_the_reviewers_repeated_windows_member_is_refused(self) -> None:
        text = dumps(Repeated({"windows": [window_document("a")]}))
        self.assertEqual(text.count('"windows"'), 2)
        self.assert_refused(text, "windows", "twice")

    def test_a_member_repeated_in_any_object_at_any_depth_is_refused(self) -> None:
        places = {**{kind: locate for kind, locate in LOCATORS.items()}, **MAPS}
        for name, locate in places.items():
            with self.subTest(place=name):
                file = good_file()
                parent_of = locate(file)
                repeated = Repeated(parent_of)
                # Put the repeating copy where the original object is.
                self.assert_refused(dumps(self.replaced(file, parent_of, repeated)), "twice")

    @staticmethod
    def replaced(file: dict[str, Any], original: dict[str, Any], repeated: Repeated) -> Any:
        """The file with the object ``original`` (found by identity) replaced by ``repeated``."""
        if original is file:
            return repeated

        def walk(value: Any) -> Any:
            if value is original:
                return repeated
            if isinstance(value, dict):
                return {key: walk(item) for key, item in value.items()}
            if isinstance(value, list):
                return [walk(item) for item in value]
            return value

        return walk(file)


class WindowIds(Entry):
    def test_a_window_id_that_is_not_a_non_empty_text_is_refused_by_both_paths(self) -> None:
        for bad in (5, None, "", ["a"], True):
            with self.subTest(bad=bad):
                file = good_file()
                file["windows"][0]["window_id"] = bad
                self.assert_refused(json.dumps(file), "window_id")
                self.assertIn("window_id", refused_directly("window", file))

    def test_two_windows_with_one_window_id_are_refused(self) -> None:
        file = {"windows": [window_document("same"), window_document("same")]}
        self.assert_refused(json.dumps(file), "same")
        with self.assertRaisesRegex(FeatureInputError, "same"):
            document.parse_file(file)


class Parity(Entry):
    def test_a_track_that_is_not_in_the_registry_is_refused_by_name_by_both_paths(self) -> None:
        file = good_file()
        MAPS["numeric"](file)["Solar8000/NOT_A_TRACK"] = []
        self.assert_refused(json.dumps(file), "Solar8000/NOT_A_TRACK")
        self.assertIn("Solar8000/NOT_A_TRACK", refused_directly("window", file))

    def test_a_mapping_gives_the_row_that_the_file_gives(self) -> None:
        file = good_file()
        code, message, target = self.run_text(json.dumps(file))
        self.assertEqual(0, code, message)
        self.assertEqual(batch.serialise([("w", build_row(file["windows"][0]))]), target.read_text())


if __name__ == "__main__":
    unittest.main()
