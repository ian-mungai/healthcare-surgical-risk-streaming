"""The streaming job refuses a checkpoint of another state format before its query starts.

Spark's state store holds the keyed state of each case occurrence as JSON of one format (``STATE_FORMAT``), and the job reads a key's state only when an event
of that key arrives. A checkpoint that an older job wrote (``stream_state_2``) would pass the start and fail much later, when the first old key is read, after
the query had committed batches. The job writes a marker file with the format into a new checkpoint folder before the query starts and refuses any folder that
holds a checkpoint without that marker or with another one.

Failure modes covered here:

- A checkpoint of an older format starts the query, and the refusal comes only when an old key is read.
- A checkpoint with no marker (every checkpoint that the earlier jobs wrote) is taken for a new one.
- A new folder is not marked, so the next start cannot tell its format, or a start that stopped while it wrote the marker locks its own folder.
- The refusal has no fixed code, so a caller cannot tell it from another failure, or it comes after the job has already called Nessie or the registry.
- A marker that is not a file, or is unreadable, passes.
"""

from __future__ import annotations

import io
import os
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest import mock

from spark.jobs import stream_features
from spark.streaming.checkpoint import EXIT_CODE, REFUSAL_CODE, STATE_FORMAT_FILE, TEMPORARY_FILE, CheckpointFormatError, check_checkpoint
from spark.streaming.state import STATE_FORMAT


class CheckpointFolder(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)
        self.checkpoint = self.root / "case"

    def refused(self) -> CheckpointFormatError:
        with self.assertRaises(CheckpointFormatError) as caught:
            check_checkpoint(self.checkpoint)
        return caught.exception

    def test_a_folder_that_does_not_exist_is_made_and_marked_with_the_current_format(self) -> None:
        check_checkpoint(self.checkpoint)
        self.assertEqual(STATE_FORMAT, (self.checkpoint / STATE_FORMAT_FILE).read_text().strip())

    def test_an_empty_folder_is_marked(self) -> None:
        self.checkpoint.mkdir()
        check_checkpoint(self.checkpoint)
        self.assertEqual(STATE_FORMAT, (self.checkpoint / STATE_FORMAT_FILE).read_text().strip())

    def test_a_folder_that_holds_only_the_temporary_file_of_an_interrupted_marker_write_is_marked(self) -> None:
        self.checkpoint.mkdir()
        (self.checkpoint / TEMPORARY_FILE).write_text("partial")
        check_checkpoint(self.checkpoint)
        self.assertEqual(STATE_FORMAT, (self.checkpoint / STATE_FORMAT_FILE).read_text().strip())

    def test_a_marked_folder_of_the_current_format_passes_again_and_is_left_as_it_is(self) -> None:
        check_checkpoint(self.checkpoint)
        (self.checkpoint / "offsets").mkdir()
        before = sorted(path.name for path in self.checkpoint.iterdir())
        check_checkpoint(self.checkpoint)
        check_checkpoint(self.checkpoint)
        self.assertEqual(before, sorted(path.name for path in self.checkpoint.iterdir()))

    def test_a_checkpoint_marked_with_an_older_format_is_refused_and_names_both_formats(self) -> None:
        for older in ("stream_state_1", "stream_state_2"):
            with self.subTest(older=older):
                self.checkpoint.mkdir(exist_ok=True)
                (self.checkpoint / STATE_FORMAT_FILE).write_text(older + "\n")
                message = str(self.refused())
                self.assertIn(older, message)
                self.assertIn(STATE_FORMAT, message)

    def test_a_checkpoint_with_content_and_no_marker_is_refused_as_one_of_an_older_job(self) -> None:
        (self.checkpoint / "offsets").mkdir(parents=True)
        (self.checkpoint / "offsets" / "0").write_text("v1\n")
        self.assertIn("no state format marker", str(self.refused()))
        self.assertFalse((self.checkpoint / STATE_FORMAT_FILE).exists())

    def test_a_marker_that_is_a_folder_or_unreadable_or_empty_is_refused(self) -> None:
        marker = self.checkpoint / STATE_FORMAT_FILE
        marker.mkdir(parents=True)
        self.refused()
        marker.rmdir()
        marker.write_bytes(b"\xff\xfe")
        self.refused()
        marker.write_text("")
        self.refused()

    def test_a_path_that_is_not_a_local_folder_is_refused_because_its_format_cannot_be_read(self) -> None:
        with self.assertRaises(CheckpointFormatError):
            check_checkpoint("s3a://bucket/checkpoint")

    def test_the_refusal_carries_the_fixed_code(self) -> None:
        (self.checkpoint / "state").mkdir(parents=True)
        error = self.refused()
        self.assertEqual(REFUSAL_CODE, error.code)
        self.assertTrue(str(error).startswith(REFUSAL_CODE))


class JobStart(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.checkpoint = Path(folder.name) / "case"
        environment = {"KAFKA_BOOTSTRAP": "kafka:9092", "REGISTRY_URL": "http://registry/apis/registry/v3", "STREAM_CHECKPOINT_DIR": str(self.checkpoint)}
        patcher = mock.patch.dict(os.environ, environment)
        patcher.start()
        self.addCleanup(patcher.stop)

    def run_main(self) -> tuple[int, str, mock.Mock, mock.Mock]:
        errors = io.StringIO()
        with (
            mock.patch.object(stream_features, "NessieClient") as nessie,
            mock.patch.object(stream_features, "build_session") as session,
            mock.patch.object(stream_features, "load_clinical"),
            redirect_stderr(errors),
        ):
            nessie.side_effect = RuntimeError("stop here: the job went past the checkpoint check")
            with mock.patch.object(stream_features, "required_environment", return_value="http://nessie"):
                try:
                    code = stream_features.main()
                except RuntimeError:
                    code = -1
        return code, errors.getvalue(), nessie, session

    def test_a_checkpoint_of_an_older_format_ends_the_job_with_the_fixed_exit_code_before_nessie_or_spark_are_touched(self) -> None:
        (self.checkpoint / "offsets").mkdir(parents=True)
        (self.checkpoint / STATE_FORMAT_FILE).write_text("stream_state_2\n")
        code, errors, nessie, session = self.run_main()
        self.assertEqual(EXIT_CODE, code)
        self.assertIn(REFUSAL_CODE, errors)
        nessie.assert_not_called()
        session.assert_not_called()

    def test_control_a_new_folder_passes_the_check_and_the_job_goes_on_to_nessie(self) -> None:
        code, _, nessie, _ = self.run_main()
        self.assertEqual(-1, code)
        nessie.assert_called_once()
        self.assertEqual(STATE_FORMAT, (self.checkpoint / STATE_FORMAT_FILE).read_text().strip())

    def test_the_exit_code_is_not_one_that_the_job_already_uses(self) -> None:
        from spark.streaming.query import TEST_EXIT_CODE

        self.assertNotIn(EXIT_CODE, (0, 1, 2, TEST_EXIT_CODE))


if __name__ == "__main__":
    unittest.main()
