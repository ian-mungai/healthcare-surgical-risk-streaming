"""The one helper that every outcome runner records through (``tests/fast/sealed_record.py``), tested on its own.

Owner decision D: a runner prints and saves only exit codes, fixed codes, Boolean flags, seconds, and identifiers that Nessie confirmed. The helper is the one
place that makes such a text and such facts, so a raw value (a count, a row, a reported ID that Nessie does not hold, an error text) cannot reach an
observation or a fact.

Failure modes covered here:

- A flag that is not exactly ``True`` or ``False`` (``1``, ``None``, text, a count) is recorded.
- A code that is not one of the fixed codes of a job is recorded, or a code that is text of a file.
- An identifier that no Nessie comparison made is recorded: a plain number, a hand-built ``ConfirmedId``, an ID that Nessie does not hold, a Boolean, a
  hash that is not 64 lowercase hexadecimal characters, an ID with no Nessie value.
- An exit code or a number of seconds that is a flag, text or a negative number is recorded.
- An error is recorded with its message instead of its class name.
- A rejected call is recorded with a phrase that is not one of the fixed phrases.
- The helper's text or facts hold a value that the caller did not pass as a flag, an ID or a fixed code.
"""

from __future__ import annotations

import unittest
from typing import Any

from tests.fast.sealed_record import ConfirmedId, Sealed, confirm_id, confirm_ids, confirm_reference, sealed_observed
from tests.fast.silver_accept import NOT_CLOSED, WRONG_EXIT

HASH = "a" * 64


def made(reported: object, held: object) -> ConfirmedId:
    found = confirm_id(reported, held)
    if found is None:
        raise AssertionError("the ID should be confirmed")
    return found


def made_reference(name: str, head: object) -> ConfirmedId:
    found = confirm_reference(name, head)
    if found is None:
        raise AssertionError("the reference should be confirmed")
    return found


class Flags(unittest.TestCase):
    def test_a_flag_that_is_true_or_false_is_recorded_with_its_name(self) -> None:
        result = sealed_observed({"main unchanged": True, "branch created": False})
        self.assertEqual("main unchanged True, branch created False", str(result))
        self.assertIsInstance(result, str)
        self.assertEqual({"main unchanged": "True", "branch created": "False"}, result.facts)

    def test_every_value_that_is_not_exactly_a_boolean_raises_and_names_no_value(self) -> None:
        for bad in (1, 0, None, "True", "SECRET_4417", 5.5, [True], 4417, ()):
            with self.subTest(bad=repr(bad)):
                with self.assertRaises(ValueError) as caught:
                    sealed_observed({"rows as expected": bad})  # type: ignore[dict-item]
                self.assertNotIn("4417", str(caught.exception))
                self.assertIn("rows as expected", str(caught.exception))

    def test_a_flag_name_that_is_not_a_plain_phrase_raises(self) -> None:
        for name in ("", "a\nb", "{rows}", "x" * 400, "rows 4417\nleft"):
            with self.subTest(name=name[:20]), self.assertRaises(ValueError):
                sealed_observed({name: True})

    def test_the_flags_must_be_a_mapping(self) -> None:
        for bad in ([("a", True)], "a", 5):
            with self.subTest(bad=repr(bad)), self.assertRaises(ValueError):
                sealed_observed(bad)  # type: ignore[arg-type]


class ExitsAndSeconds(unittest.TestCase):
    def test_an_exit_code_and_the_seconds_are_recorded(self) -> None:
        self.assertEqual("exit 0, after 12 seconds, flag True", str(sealed_observed({"flag": True}, exit_code=0, seconds=12.4)))
        self.assertEqual("plant exit 0, build exit 1", str(sealed_observed(exits={"plant exit": 0, "build exit": 1})))

    def test_a_process_that_a_signal_stopped_has_a_negative_exit_code_and_is_recorded(self) -> None:
        self.assertEqual("exit -9", str(sealed_observed(exit_code=-9)))

    def test_an_exit_code_that_is_not_a_whole_number_of_a_process_raises(self) -> None:
        for bad in (True, -256, 256, "1", 1.5):
            with self.subTest(bad=repr(bad)):
                with self.assertRaises(ValueError):
                    sealed_observed(exit_code=bad)  # type: ignore[arg-type]
                with self.assertRaises(ValueError):
                    sealed_observed(exits={"exit": bad})  # type: ignore[dict-item]

    def test_seconds_that_are_a_flag_text_or_negative_raise(self) -> None:
        for bad in (True, "5", -1.0, float("nan"), float("inf")):
            with self.subTest(bad=repr(bad)), self.assertRaises(ValueError):
                sealed_observed(seconds=bad)  # type: ignore[arg-type]


class Codes(unittest.TestCase):
    def test_a_fixed_code_is_recorded_and_text_that_is_not_one_raises(self) -> None:
        self.assertEqual("codes [two_sets, shares]", str(sealed_observed(codes=["two_sets", "shares"])))
        self.assertEqual("codes []", str(sealed_observed(codes=[])))
        for bad in ("secret_4417", "two_sets: 2 patients", "TWO_SETS", "", 5, True):
            with self.subTest(bad=repr(bad)):
                with self.assertRaises(ValueError) as caught:
                    sealed_observed(codes=[bad])  # type: ignore[list-item]
                self.assertNotIn("4417", str(caught.exception))

    def test_no_codes_given_records_no_codes_text(self) -> None:
        self.assertEqual("x True", str(sealed_observed({"x": True})))


class ConfirmedIds(unittest.TestCase):
    def test_an_id_that_nessie_holds_is_confirmed_and_any_other_is_not(self) -> None:
        self.assertIsInstance(confirm_id(7, 7), ConfirmedId)
        self.assertIsInstance(confirm_id(HASH, HASH), ConfirmedId)
        for reported, held in (
            (7, 8),
            (7, None),
            (True, True),
            (None, None),
            ("7", 7),
            (7, "7"),
            (7.0, 7),
            (1, True),
            (1, 1.0),
            ("A" * 64, "A" * 64),
            ("a" * 63, "a" * 63),
        ):
            with self.subTest(reported=repr(reported), held=repr(held)):
                self.assertIsNone(confirm_id(reported, held))

    def test_a_reference_name_that_nessie_holds_is_confirmed_and_text_with_a_space_or_a_line_is_not(self) -> None:
        self.assertIsInstance(confirm_id("p6_trunk_ab12", "p6_trunk_ab12"), ConfirmedId)
        for text in ("two words", "a\nb", "x" * 300, ""):
            with self.subTest(text=text[:10]):
                self.assertIsNone(confirm_id(text, text))

    def test_a_reference_name_is_confirmed_only_when_nessie_gave_it_a_head(self) -> None:
        self.assertEqual("p6_trunk_ab12", made_reference("p6_trunk_ab12", HASH).value)
        for head in (None, "", "reference", "A" * 64, "a" * 63, 7):
            with self.subTest(head=repr(head)):
                self.assertIsNone(confirm_reference("p6_trunk_ab12", head))
        for name in ("two words", "", "a\nb", "main", 7, True):
            with self.subTest(name=repr(name)):
                self.assertIsNone(confirm_reference(name, HASH))

    def test_a_confirmed_id_cannot_be_made_by_hand_or_changed(self) -> None:
        with self.assertRaises(TypeError):
            ConfirmedId(7)  # type: ignore[call-arg]
        with self.assertRaises(TypeError):
            ConfirmedId(7, object())
        with self.assertRaises(AttributeError):
            made(7, 7).value = 8  # type: ignore[misc]

    def test_a_confirmed_id_is_recorded_as_its_value_and_a_plain_number_raises(self) -> None:
        result = sealed_observed({"hash equal": True}, confirmed={"branch_hash": made(HASH, HASH)})
        self.assertEqual(f"hash equal True, branch hash {HASH}", str(result))
        self.assertEqual({"hash equal": "True", "branch_hash": HASH}, result.facts)
        for bad in (7, HASH, True, None, "7"):
            with self.subTest(bad=repr(bad)), self.assertRaises(ValueError):
                sealed_observed(confirmed={"branch_hash": bad})  # type: ignore[dict-item]

    def test_a_map_of_ids_is_confirmed_only_when_every_expected_table_matches_what_nessie_holds(self) -> None:
        held = {"a": 1, "b": 2, "c": 3}
        found = confirm_ids(dict(held), tuple(held), held.get)
        self.assertIsNotNone(found)
        self.assertEqual({"a": 1, "b": 2, "c": 3}, {key: value.value for key, value in (found or {}).items()})
        asked: list[str] = []

        def lookup(name: str) -> int | None:
            asked.append(name)
            return held.get(name)

        plants: dict[str, Any] = {
            "an id that differs": {**held, "b": 9},
            "a key missing": {"a": 1, "b": 2},
            "an eighth key": {**held, "d": 4},
            "a null": {**held, "c": None},
            "a Boolean": {**held, "c": True},
            "not a map": [1, 2, 3],
            "empty": {},
        }
        for name, reported in plants.items():
            with self.subTest(name):
                asked.clear()
                self.assertIsNone(confirm_ids(reported, tuple(held), lookup))
                self.assertEqual(sorted(asked), sorted(held) if name != "not a map" else asked, "every expected table is compared with Nessie")
        self.assertIsNone(confirm_ids(dict(held), tuple(held), lambda name: None))


class ErrorsAndReasons(unittest.TestCase):
    def test_an_error_is_recorded_by_its_class_and_never_by_its_message(self) -> None:
        text = str(sealed_observed(error=RuntimeError("SECRET_4417 in the message")))
        self.assertEqual("stopped by RuntimeError", text)
        self.assertEqual("stopped by NessieLike", str(sealed_observed(error=type("NessieLike", (Exception,), {})("x"))))

    def test_a_rejected_call_is_recorded_by_a_fixed_phrase_only(self) -> None:
        self.assertEqual(f"rejected: {WRONG_EXIT}", str(sealed_observed(reason=WRONG_EXIT)))
        self.assertEqual(f"rejected: {NOT_CLOSED}", str(sealed_observed(reason=NOT_CLOSED)))
        for bad in ("the field height is 1e3", "", 5):
            with self.subTest(bad=repr(bad)), self.assertRaises(ValueError):
                sealed_observed(reason=bad)  # type: ignore[arg-type]


class Line(unittest.TestCase):
    def test_a_report_line_names_the_verdict_the_case_and_the_sealed_text(self) -> None:
        self.assertEqual("PASS a case (control): x True\n", sealed_observed({"x": True}).line(True, "a case", "control"))
        self.assertEqual("FAIL a case (control): x False\n", sealed_observed({"x": False}).line(False, "a case", "control"))

    def test_the_sealed_type_is_made_by_the_helper_only(self) -> None:
        with self.assertRaises(TypeError):
            Sealed("rows 4417")  # type: ignore[call-arg]


if __name__ == "__main__":
    unittest.main()
