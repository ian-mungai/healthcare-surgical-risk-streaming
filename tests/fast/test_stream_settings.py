"""The settings of the streaming job and the file of clinical facts.

Failure modes covered here:

- A setting is missing, empty or not a number and the job starts anyway, or a limit is zero or negative.
- A late limit or a step is read in the wrong unit.
- The test input that stops the job after a sink commit is on without being asked for.
- A clinical file with an unknown key, a repeated field or a value of the wrong type is read, or a case that is not in the file gets the facts of another case.
"""

from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from features.model import ClinicalValue
from spark.streaming.clinical import ClinicalFileError, load_clinical
from spark.streaming.settings import StreamSettings, StreamSettingsError, from_environment

BASE = {"KAFKA_BOOTSTRAP": "kafka:9092", "REGISTRY_URL": "http://apicurio:8080/apis/registry/v3", "STREAM_CHECKPOINT_DIR": "/checkpoints/stream"}


class Settings(unittest.TestCase):
    def test_the_defaults_are_the_policy_of_the_job_and_nothing_is_on_that_was_not_asked_for(self) -> None:
        settings = from_environment(BASE)
        self.assertEqual(("replay_events", "feature_ready", "stream"), (settings.input_topic, settings.output_topic, settings.branch))
        self.assertEqual((30_000_000, 60_000_000), (settings.late_limit_us, settings.cutoff_step_us))
        self.assertIsNone(settings.test_exit_after_sink)
        self.assertEqual("", settings.clinical_file)
        self.assertIsInstance(settings, StreamSettings)

    def test_the_topics_and_the_branch_can_be_named_for_a_test_and_default_to_the_platforms(self) -> None:
        settings = from_environment({**BASE, "STREAM_INPUT_TOPIC": "in_x", "STREAM_OUTPUT_TOPIC": "out_x", "STREAM_BRANCH": "b_x"})
        self.assertEqual(("in_x", "out_x", "b_x"), (settings.input_topic, settings.output_topic, settings.branch))

    def test_the_limits_are_read_in_seconds_and_kept_in_microseconds(self) -> None:
        settings = from_environment({**BASE, "STREAM_LATE_LIMIT_SECONDS": "12.5", "STREAM_CUTOFF_STEP_SECONDS": "30"})
        self.assertEqual((12_500_000, 30_000_000), (settings.late_limit_us, settings.cutoff_step_us))

    def test_a_missing_or_bad_setting_is_refused_and_named(self) -> None:
        for name in BASE:
            with self.subTest(missing=name), self.assertRaisesRegex(StreamSettingsError, name):
                from_environment({key: value for key, value in BASE.items() if key != name})
        for name, value in (
            ("STREAM_LATE_LIMIT_SECONDS", "-1"),
            ("STREAM_LATE_LIMIT_SECONDS", "x"),
            ("STREAM_CUTOFF_STEP_SECONDS", "0"),
            ("STREAM_MAX_SECONDS", "-5"),
            ("STREAM_TRIGGER_SECONDS", "0"),
            ("STREAM_MAX_OFFSETS_PER_TRIGGER", "0"),
            ("STREAM_TEST_EXIT_AFTER_SINK", "0"),
            ("STREAM_TEST_EXIT_AFTER_SINK", "x"),
        ):
            with self.subTest(name=name, value=value), self.assertRaisesRegex(StreamSettingsError, name):
                from_environment({**BASE, name: value})

    def test_a_step_that_rounds_to_no_microsecond_is_refused_and_one_microsecond_is_kept(self) -> None:
        for text in ("1e-7", "4e-7", "5e-7", "0.0000004"):
            with self.subTest(text=text), self.assertRaisesRegex(StreamSettingsError, "STREAM_CUTOFF_STEP_SECONDS") as caught:
                from_environment({**BASE, "STREAM_CUTOFF_STEP_SECONDS": text})
            self.assertNotIn(text, str(caught.exception))
        self.assertEqual(1, from_environment({**BASE, "STREAM_CUTOFF_STEP_SECONDS": "1e-6"}).cutoff_step_us)
        self.assertEqual(0, from_environment({**BASE, "STREAM_LATE_LIMIT_SECONDS": "1e-7"}).late_limit_us)

    def test_the_offsets_per_trigger_are_a_whole_number_from_one_and_a_fraction_is_never_cut(self) -> None:
        for text in ("0", "0.5", "0.9999999", "1.5", "2.5", "49999.5", "1e-300", "1e30", "-1", "x"):
            with self.subTest(text=text), self.assertRaisesRegex(StreamSettingsError, "STREAM_MAX_OFFSETS_PER_TRIGGER") as caught:
                from_environment({**BASE, "STREAM_MAX_OFFSETS_PER_TRIGGER": text})
            self.assertNotIn(text, str(caught.exception))
        for text, wanted in (("1", 1), ("7", 7), ("1e3", 1000), ("2.0", 2), ("50000", 50_000)):
            with self.subTest(text=text):
                self.assertEqual(wanted, from_environment({**BASE, "STREAM_MAX_OFFSETS_PER_TRIGGER": text}).max_offsets_per_trigger)
        self.assertEqual(50_000, from_environment(BASE).max_offsets_per_trigger)

    def test_an_offset_count_that_a_float_would_round_to_a_whole_number_is_refused_and_one_that_is_exact_is_kept(self) -> None:
        for text in (
            "1.0000000000000001",
            "1.00000000000000000001",
            "0.99999999999999999",
            "2.0000000000000001",
            "49999.99999999999999",
            "2147483647.0000000001",
            "2147483647.5",
            "2147483648",
            "nan",
            "NaN",
            "sNaN",
            "inf",
            "-inf",
            "Infinity",
            "1e999999999999999999999",
            "\u0661\u0662",
        ):
            with self.subTest(value=text), self.assertRaisesRegex(StreamSettingsError, "STREAM_MAX_OFFSETS_PER_TRIGGER") as caught:
                from_environment({**BASE, "STREAM_MAX_OFFSETS_PER_TRIGGER": text})
            self.assertNotIn(text, str(caught.exception))
        for text, expected in (("1.0000000000", 1), ("1e3", 1_000), ("1E+3", 1_000), ("00050000", 50_000), ("2147483647.0", 2_147_483_647), ("5.00", 5)):
            with self.subTest(value=text):
                self.assertEqual(expected, from_environment({**BASE, "STREAM_MAX_OFFSETS_PER_TRIGGER": text}).max_offsets_per_trigger)

    def test_the_test_input_counts_sink_commits_from_one(self) -> None:
        self.assertEqual(1, from_environment({**BASE, "STREAM_TEST_EXIT_AFTER_SINK": "1"}).test_exit_after_sink)
        self.assertEqual(7, from_environment({**BASE, "STREAM_TEST_EXIT_AFTER_SINK": "7"}).test_exit_after_sink)


class ClinicalFile(unittest.TestCase):
    def write(self, text: str) -> str:
        folder = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, folder)
        path = Path(folder) / "clinical.json"
        path.write_text(text)
        return str(path)

    def test_a_case_gets_its_own_facts_and_an_unknown_case_gets_none(self) -> None:
        lookup = load_clinical(
            self.write(json.dumps({"cases": {"908": {"age": {"value": 61.0, "available_us": 0}, "sex": {"value": "F", "available_us": None}}}}))
        )
        self.assertEqual({"age": ClinicalValue(61.0, 0), "sex": ClinicalValue("F", None)}, dict(lookup(908)))
        self.assertEqual({}, dict(lookup(1147)))

    def test_no_file_means_no_clinical_facts(self) -> None:
        self.assertEqual({}, dict(load_clinical("")(908)))

    def test_a_file_that_does_not_follow_the_form_is_refused(self) -> None:
        bad = [
            "{}",
            '{"cases": []}',
            '{"cases": {"x": {}}}',
            '{"cases": {"1": {"age": {"value": 1}}}}',
            '{"cases": {"1": {"age": {"value": 1, "available_us": 0, "extra": 1}}}}',
            '{"cases": {"1": {"age": {"value": 1, "available_us": 0.5}}}}',
            '{"cases": {"1": {"age": {"value": 1, "available_us": 0}, "age": {"value": 2, "available_us": 0}}}}',
            "not json",
        ]
        for text in bad:
            with self.subTest(text=text), self.assertRaises(ClinicalFileError):
                load_clinical(self.write(text))

    def test_a_file_that_is_not_there_is_refused(self) -> None:
        with self.assertRaises(ClinicalFileError):
            load_clinical("/nonexistent/clinical.json")


if __name__ == "__main__":
    unittest.main()
