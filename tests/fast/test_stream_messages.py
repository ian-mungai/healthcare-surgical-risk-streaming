"""The streaming job shows no word of an environment value, a file, a path or an exception on standard error, and writes its checkpoint marker as a new file.

The sweep's rules for the code that the streaming branch brought in: a caught exception reaches a message only through ``describe_error``; a refusal that
the job shows (``CheckpointFormatError``) is fixed text with no path and no value read from the folder; no folder is listed with ``pathlib``, because a
folder that cannot be read must raise and not look empty (the feature version would then be the digest of no file); the marker is written with
``write_new_file`` in the checkpoint folder: it is written only when the folder has none, so it is never replaced. A marker value in each place shows that
none is printed.
"""

from __future__ import annotations

import io
import os
import stat
import tempfile
import unittest
from contextlib import ExitStack, redirect_stderr
from pathlib import Path
from unittest import mock

from scripts.stack.nessie import NessieError
from spark.jobs import stream_features
from spark.settings import SettingsError
from spark.streaming import version
from spark.streaming.checkpoint import EXIT_CODE, REFUSAL_CODE, STATE_FORMAT_FILE, CheckpointFormatError, check_checkpoint
from spark.streaming.clinical import ClinicalFileError, load_clinical
from spark.streaming.settings import StreamSettingsError
from spark.streaming.state import STATE_FORMAT

MARKER = "marker_value_x9"


class Folder(unittest.TestCase):
    def setUp(self) -> None:
        made = tempfile.TemporaryDirectory()
        self.addCleanup(made.cleanup)
        self.root = Path(made.name).resolve()


class MainMessages(Folder):
    def run_main(self, **patches: BaseException) -> tuple[int, str]:
        environment = {
            "KAFKA_BOOTSTRAP": "kafka:9092",
            "REGISTRY_URL": "http://registry/apis/registry/v3",
            "STREAM_CHECKPOINT_DIR": str(self.root / MARKER / "ck"),
            "NESSIE_URI": "http://nessie.invalid:19120/api/v2",
        }
        errors = io.StringIO()
        with ExitStack() as stack:
            stack.enter_context(mock.patch.dict(os.environ, environment))
            stack.enter_context(redirect_stderr(errors))
            if "settings" in patches:
                stack.enter_context(mock.patch.object(stream_features, "from_environment", side_effect=patches["settings"]))
            stack.enter_context(mock.patch.object(stream_features, "check_checkpoint"))
            stack.enter_context(mock.patch.object(stream_features, "load_clinical", side_effect=patches.get("clinical")))
            stack.enter_context(mock.patch.object(stream_features, "NessieClient", side_effect=patches.get("nessie")))
            code = stream_features.main()
        return code, errors.getvalue()

    def test_a_setting_or_clinical_error_shows_the_type_name_and_none_of_its_text(self) -> None:
        for label, name, error in (
            ("stream setting", "settings", StreamSettingsError(f"{MARKER} must be set")),
            ("setting", "settings", SettingsError(f"{MARKER} is not a number")),
            ("clinical file", "clinical", ClinicalFileError(f"cases.{MARKER} must be a case number")),
        ):
            with self.subTest(label=label):
                code, err = self.run_main(**{name: error})
                self.assertEqual((2, f"stream_features: {type(error).__name__}\n"), (code, err))

    def test_a_nessie_error_shows_the_type_name_and_none_of_its_text(self) -> None:
        code, err = self.run_main(nessie=NessieError(MARKER))
        self.assertEqual((1, "stream_features: cannot work with Nessie: NessieError\n"), (code, err))

    def test_a_failed_query_shows_the_type_name_and_none_of_its_text(self) -> None:
        session, writer = mock.Mock(), mock.Mock()
        with (
            mock.patch.dict(os.environ, {"KAFKA_BOOTSTRAP": "k:9092", "REGISTRY_URL": "http://r/v3", "STREAM_CHECKPOINT_DIR": str(self.root / "ck")}),
            mock.patch.object(stream_features, "NessieClient"),
            mock.patch.object(stream_features, "required_environment", return_value="http://nessie"),
            mock.patch.object(stream_features, "ensure_branch"),
            mock.patch.object(stream_features, "load_clinical"),
            mock.patch.object(stream_features, "build_session", return_value=session),
            mock.patch.object(stream_features, "LakeSinks"),
            mock.patch.object(stream_features, "start_query"),
            mock.patch.object(stream_features, "run_query", return_value=({}, RuntimeError(f"{MARKER} stack text"))),
            mock.patch.dict("sys.modules", {"replay.service.registry": mock.Mock(), "replay.service.wire_codec": mock.Mock(Writer=writer)}),
            mock.patch("signal.signal"),
            redirect_stderr(io.StringIO()) as errors,
        ):
            code = stream_features.main()
        self.assertEqual((1, "stream_features: the query failed: RuntimeError\n"), (code, errors.getvalue()))


class CheckpointRefusals(Folder):
    def refused(self, checkpoint: str | Path) -> str:
        with self.assertRaises(CheckpointFormatError) as caught:
            check_checkpoint(checkpoint)
        self.assertEqual(REFUSAL_CODE, caught.exception.code)
        return str(caught.exception)

    def test_the_text_of_a_refusal_names_no_path_and_no_format_read_from_the_folder(self) -> None:
        folder = self.root / MARKER
        folder.mkdir()
        (folder / STATE_FORMAT_FILE).write_text(f"{MARKER}_format\n")
        for label, checkpoint in (
            ("another format", folder),
            ("no marker", self.root / f"{MARKER}_other"),
            ("a uri", f"s3a://{MARKER}/checkpoint"),
        ):
            if label == "no marker":
                (self.root / f"{MARKER}_other" / "offsets").mkdir(parents=True)
            with self.subTest(label=label):
                text = self.refused(checkpoint)
                self.assertNotIn(MARKER, text)
                self.assertTrue(text.startswith(REFUSAL_CODE))

    def test_the_job_shows_the_fixed_text_of_a_refusal_and_exits_with_its_code(self) -> None:
        folder = self.root / MARKER
        folder.mkdir()
        (folder / STATE_FORMAT_FILE).write_text(f"{MARKER}_format\n")
        errors = io.StringIO()
        with (
            mock.patch.dict(os.environ, {"KAFKA_BOOTSTRAP": "k:9092", "REGISTRY_URL": "http://r/v3", "STREAM_CHECKPOINT_DIR": str(folder)}),
            redirect_stderr(errors),
        ):
            code = stream_features.main()
        self.assertEqual(EXIT_CODE, code)
        self.assertTrue(errors.getvalue().startswith(f"stream_features: {REFUSAL_CODE}"))
        self.assertNotIn(MARKER, errors.getvalue())
        self.assertIn(STATE_FORMAT, errors.getvalue())

    def test_the_marker_is_a_new_file_that_is_whole_and_private_and_leaves_no_temporary_file(self) -> None:
        folder = self.root / "ck"
        check_checkpoint(folder)
        marker = folder / STATE_FORMAT_FILE
        self.assertEqual(f"{STATE_FORMAT}\n", marker.read_text())
        self.assertEqual(0o600, stat.S_IMODE(marker.stat().st_mode))
        self.assertEqual([STATE_FORMAT_FILE], sorted(path.name for path in folder.iterdir()))

    def test_a_write_that_stops_leaves_no_marker_and_a_part_file_of_the_helper_does_not_lock_the_folder(self) -> None:
        folder = self.root / "ck"
        with mock.patch("scripts.ingestion.output_file.os.link", side_effect=OSError("stopped")), self.assertRaises(Exception):  # noqa: B017
            check_checkpoint(folder)
        self.assertEqual([], list(folder.iterdir()))
        (folder / ".0a1b2c3d.part").write_text("partial")
        check_checkpoint(folder)
        self.assertEqual(f"{STATE_FORMAT}\n", (folder / STATE_FORMAT_FILE).read_text())

    def test_a_marker_that_the_new_file_write_refuses_is_the_refusal_with_its_fixed_code(self) -> None:
        """A marker that cannot be written (the helper refuses: a file is there that no check saw) ends as the checkpoint's refusal and names no path."""
        folder = self.root / MARKER / "ck"
        real = os.link

        def late(source: str, target: str) -> None:
            Path(target).write_text("other")
            real(source, target)

        with mock.patch("scripts.ingestion.output_file.os.link", late):
            text = self.refused(folder)
        self.assertNotIn(MARKER, text)
        self.assertEqual("other", (folder / STATE_FORMAT_FILE).read_text())

    def test_a_folder_that_cannot_be_read_is_an_error_and_not_a_new_folder(self) -> None:
        folder = self.root / "ck"
        folder.mkdir()
        (folder / "offsets").mkdir()
        folder.chmod(0)
        self.addCleanup(folder.chmod, 0o755)
        with self.assertRaises(OSError):
            check_checkpoint(folder)
        self.assertFalse(any(name == STATE_FORMAT_FILE for name in os.listdir(self.root)))


class ClinicalFile(Folder):
    def test_a_file_that_cannot_be_read_gives_fixed_text_without_the_path_or_the_error(self) -> None:
        for label, make in (("missing", lambda path: None), ("not json", lambda path: path.write_text(f"{MARKER} not json"))):
            path = self.root / f"{MARKER}_{label}.json"
            make(path)
            with self.subTest(label=label), self.assertRaises(ClinicalFileError) as caught:
                load_clinical(str(path))
            self.assertEqual("the clinical file cannot be read", str(caught.exception))


class FeatureVersion(Folder):
    def test_a_features_folder_that_cannot_be_read_is_an_error_and_not_the_digest_of_no_file(self) -> None:
        folder = self.root / "features"
        folder.mkdir()
        (folder / "a.py").write_text("X = 1\n")
        with mock.patch.object(version, "FEATURES_FOLDER", folder):
            version.feature_version.cache_clear()
            self.addCleanup(version.feature_version.cache_clear)
            readable = version.feature_version()
            version.feature_version.cache_clear()
            folder.chmod(0)
            self.addCleanup(folder.chmod, 0o755)
            with self.assertRaises(OSError):
                version.feature_version()
        self.assertTrue(readable.startswith(version.VERSION_PREFIX))


if __name__ == "__main__":
    unittest.main()
