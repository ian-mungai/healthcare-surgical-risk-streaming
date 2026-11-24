"""The streaming job's startup ends with a fixed line and a fixed exit code for every setting or checkpoint that cannot be used, and shows no path or value.

Failure modes covered here:

- A checkpoint folder that cannot be listed (no read right) or made (a parent that is a file) escapes as a traceback that holds the path. It is the fixed
  checkpoint refusal (``checkpoint_state_format_refused``, exit 3).
- A Nessie or registry address with a port that is not a number, or is out of range, escapes as a ``ValueError`` that holds the port. It is the type name
  ``SettingsError`` and exit 2.
- A number setting that is infinite, not a number, negative or beyond its range (``STREAM_LATE_LIMIT_SECONDS`` and the others) escapes as an ``OverflowError``
  or builds an impossible figure. It is ``StreamSettingsError`` and exit 2.

Each case runs the real ``main`` with the real settings and the real checkpoint check, and asserts that the two streams hold no marker from the input.
"""

from __future__ import annotations

import io
import os
import tempfile
import unittest
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from scripts.stack.nessie import NessieClient
from spark.jobs import stream_features
from spark.streaming.checkpoint import EXIT_CODE, REFUSAL_CODE

MARKER = "marker_value_x9"


class StartupRefusals(unittest.TestCase):
    def setUp(self) -> None:
        made = tempfile.TemporaryDirectory()
        self.addCleanup(made.cleanup)
        self.root = Path(made.name).resolve()
        self.checkpoint = self.root / f"ck_{MARKER}"

    def run_main(self, **changes: str) -> tuple[int, str, str]:
        environment = {
            "KAFKA_BOOTSTRAP": "kafka:9092",
            "REGISTRY_URL": "http://registry:8080/apis/registry/v3",
            "STREAM_CHECKPOINT_DIR": str(self.checkpoint),
            "NESSIE_URI": "http://nessie.invalid:19120/api/v2",
            **changes,
        }
        out, err = io.StringIO(), io.StringIO()
        with ExitStack() as stack:
            stack.enter_context(mock.patch.dict(os.environ, environment))
            stack.enter_context(redirect_stdout(out))
            stack.enter_context(redirect_stderr(err))
            stack.enter_context(mock.patch.object(stream_features, "NessieClient", wraps=NessieClient))
            stack.enter_context(mock.patch.object(stream_features, "ensure_branch"))
            stack.enter_context(mock.patch.dict("sys.modules", {"replay.service.wire_codec": mock.Mock()}))
            stack.enter_context(mock.patch("scripts.stack.nessie.NessieClient.config"))
            code = stream_features.main()
        return code, out.getvalue(), err.getvalue()

    def assert_fixed(self, code: int, out: str, err: str, expected_code: int, expected_line: str) -> None:
        self.assertEqual((expected_code, "", expected_line), (code, out, err))
        for text in (out, err):
            self.assertNotIn(MARKER, text)
            self.assertNotIn(str(self.root), text)
            self.assertNotIn("Traceback", text)

    def test_a_checkpoint_folder_that_cannot_be_listed_is_the_fixed_refusal_and_names_no_path(self) -> None:
        self.checkpoint.mkdir()
        self.checkpoint.chmod(0)
        self.addCleanup(self.checkpoint.chmod, 0o700)
        code, out, err = self.run_main()
        self.assertEqual((EXIT_CODE, ""), (code, out))
        self.assertTrue(err.startswith(f"stream_features: {REFUSAL_CODE}"), err)
        self.assertEqual(1, err.count("\n"))
        self.assertNotIn(MARKER, err)
        self.assertNotIn(str(self.root), err)

    def test_a_checkpoint_folder_that_cannot_be_made_is_the_fixed_refusal_and_names_no_path(self) -> None:
        (self.root / "file").write_text("x")
        self.checkpoint = self.root / "file" / f"ck_{MARKER}"
        code, out, err = self.run_main()
        self.assertEqual((EXIT_CODE, ""), (code, out))
        self.assertTrue(err.startswith(f"stream_features: {REFUSAL_CODE}"), err)
        self.assertNotIn(MARKER, err)
        self.assertNotIn(str(self.root), err)

    def test_a_nessie_port_that_is_not_usable_is_the_settings_exit_and_shows_no_value(self) -> None:
        for port in (f"{MARKER}", "99999", "-1", "1_0", "80x"):
            with self.subTest(port=port):
                self.checkpoint = self.root / f"ck_{port}"
                self.assert_fixed(*self.run_main(NESSIE_URI=f"http://nessie.invalid:{port}/api/v2"), 2, "stream_features: SettingsError\n")

    def test_a_registry_port_that_is_not_usable_is_the_settings_exit_and_shows_no_value(self) -> None:
        for port in (f"{MARKER}", "99999", "-1", "80x"):
            with self.subTest(port=port):
                self.checkpoint = self.root / f"ck_{port}"
                self.assert_fixed(*self.run_main(REGISTRY_URL=f"http://registry:{port}/apis/registry/v3"), 2, "stream_features: SettingsError\n")

    def test_a_number_setting_that_is_not_finite_or_not_in_range_is_the_settings_exit_and_shows_no_value(self) -> None:
        names = (
            "STREAM_LATE_LIMIT_SECONDS",
            "STREAM_CUTOFF_STEP_SECONDS",
            "STREAM_TRIGGER_SECONDS",
            "STREAM_MAX_OFFSETS_PER_TRIGGER",
            "STREAM_MAX_SECONDS",
        )
        for name in names:
            for value in ("inf", "-inf", "nan", "1e400", "1e30", f"{MARKER}", "-1"):
                with self.subTest(name=name, value=value):
                    self.checkpoint = self.root / f"ck_{name}_{value}"
                    self.assert_fixed(*self.run_main(**{name: value}), 2, "stream_features: StreamSettingsError\n")

    def test_a_late_limit_of_zero_and_an_ordinary_one_still_start(self) -> None:
        for value in ("0", "30", "86400"):
            with self.subTest(value=value):
                self.checkpoint = self.root / f"ck_{value}"
                with (
                    mock.patch.object(stream_features, "load_clinical"),
                    mock.patch.object(stream_features, "build_session", side_effect=KeyError("stop")),
                    mock.patch.dict("sys.modules", {"replay.service.wire_codec": mock.Mock()}),
                    self.assertRaises(KeyError),
                ):
                    self.run_main(STREAM_LATE_LIMIT_SECONDS=value)


if __name__ == "__main__":
    unittest.main()
