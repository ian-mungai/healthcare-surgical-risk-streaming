"""The inventory of the stack's secret inputs and the scanner that looks for a marker in every sink.

Every secret file the code can name must have a line in the inventory, so no input is added without a harmless-marker case. The scanner finds a marker in
the forms that a log, a rendered file or an API answer may hold: plain, percent-encoded, hexadecimal and Base64 at any byte offset.
"""

from __future__ import annotations

import base64
import re
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from urllib.parse import quote

from tests.fast.secret_inputs import FORMS, INPUTS, NOT_SECRET, Marker, derived_files, make_marker, scan_text, scan_tree

KEY_ID_FORM = re.compile(r"[A-Z0-9]{16,128}")
KEY_VALUE_FORM = re.compile(r"[A-Za-z0-9/+=]{20,128}")
UNLOCK_KEY_FORM = re.compile(r"[0-9a-f]{64}")


class MarkerTest(unittest.TestCase):
    def test_each_form_matches_the_check_of_the_entry_point_that_takes_it(self) -> None:
        self.assertRegex(make_marker("hex64").value, UNLOCK_KEY_FORM)
        self.assertRegex(make_marker("aws_key_id").value, KEY_ID_FORM)
        self.assertRegex(make_marker("aws_secret").value, KEY_VALUE_FORM)
        self.assertGreaterEqual(len(make_marker("urlsafe").value), 32)
        self.assertGreaterEqual(len(make_marker("plain").value), 16)

    def test_two_markers_of_one_form_differ_and_a_marker_is_never_a_key_the_provider_would_issue(self) -> None:
        for form in FORMS:
            with self.subTest(form=form):
                self.assertNotEqual(make_marker(form).value, make_marker(form).value)
        self.assertFalse(make_marker("aws_key_id").value.startswith(("AKIA", "ASIA", "AIDA")))

    def test_an_unknown_form_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            make_marker("base58")


class ScannerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.marker = Marker("marker secret", "sinkmark/value+with/specials=01234567")

    def forms(self) -> dict[str, str]:
        raw = self.marker.value.encode()
        return {
            "plain": f"password={self.marker.value} end",
            "percent": f"password={quote(self.marker.value, safe='')}&x=1",
            "percent lower": f"password={quote(self.marker.value, safe='').lower()}&x=1",
            "hex": f"digest {raw.hex()} end",
            "hex upper": f"digest {raw.hex().upper()} end",
            "base64": f"data: {base64.b64encode(raw).decode()}",
            "base64 url": f"data: {base64.urlsafe_b64encode(raw).decode()}",
            "json slash": f'{{"v": "{self.marker.value.replace("/", chr(92) + "/")}"}}',
        }

    def test_each_form_is_found_and_named(self) -> None:
        for name, text in self.forms().items():
            with self.subTest(form=name):
                hits = scan_text(text, [self.marker])
                self.assertEqual([self.marker.label], [hit.label for hit in hits], text)
                self.assertEqual(name.split()[0], hits[0].form)

    def test_base64_is_found_at_every_offset_inside_a_longer_value(self) -> None:
        raw = self.marker.value.encode()
        for offset in range(3):
            with self.subTest(offset=offset):
                encoded = base64.b64encode(b"x" * offset + raw + b"yz").decode()
                self.assertEqual(["base64"], [hit.form for hit in scan_text(encoded, [self.marker])])
                self.assertEqual(["base64"], [hit.form for hit in scan_text(encoded.replace("+", "-").replace("/", "_"), [self.marker])])

    def test_text_without_the_marker_is_clean_and_a_near_miss_is_clean(self) -> None:
        near = self.marker.value[:12] + "9" + self.marker.value[13:]
        for text in ("", "nothing here", f"x={near}", base64.b64encode(near.encode()).decode(), "sinkmark"):
            with self.subTest(text=text[:20]):
                self.assertEqual([], scan_text(text, [self.marker]))

    def test_bytes_are_scanned_like_text(self) -> None:
        self.assertEqual(["plain"], [hit.form for hit in scan_text(b"\x00\xffpassword=" + self.marker.value.encode(), [self.marker])])

    def test_several_markers_are_each_named(self) -> None:
        other = Marker("other", "second-marker-value-abcdef0123")
        hits = scan_text(f"{self.marker.value} {other.value}", [self.marker, other])
        self.assertEqual(["marker secret", "other"], sorted(hit.label for hit in hits))

    def test_an_empty_marker_is_refused_because_it_would_match_everything(self) -> None:
        with self.assertRaises(ValueError):
            scan_text("anything", [Marker("empty", "")])

    def test_a_short_marker_is_refused_because_it_would_match_by_chance(self) -> None:
        with self.assertRaises(ValueError):
            scan_text("anything", [Marker("short", "abc123")])

    def test_a_tree_is_scanned_in_every_file_and_the_allowed_files_are_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            (root / "own").mkdir()
            (root / "own" / "secret.file").write_text(self.marker.value)
            (root / "deep" / "er").mkdir(parents=True)
            (root / "deep" / "er" / "leak.log").write_text(f"line\n{self.marker.value}\n")
            (root / "binary.bin").write_bytes(b"\x00\x01" + self.marker.value.encode())
            (root / "clean.txt").write_text("nothing")
            hits = scan_tree(root, [self.marker], allowed={root / "own" / "secret.file"})
            self.assertEqual(["binary.bin", "deep/er/leak.log"], sorted(hit.path for hit in hits))

    def test_a_link_in_a_tree_is_not_followed(self) -> None:
        with tempfile.TemporaryDirectory() as scratch, tempfile.TemporaryDirectory() as elsewhere:
            (Path(elsewhere) / "outside.txt").write_text(self.marker.value)
            (Path(scratch) / "link").symlink_to(Path(elsewhere) / "outside.txt")
            self.assertEqual([], scan_tree(Path(scratch), [self.marker]))


class InventoryTest(unittest.TestCase):
    def test_every_secret_file_the_code_names_has_one_line_in_the_inventory_or_a_reason_that_it_is_no_secret(self) -> None:
        unclaimed = []
        for path in derived_files():
            claims = [entry.name for entry in INPUTS if entry.claims(path)]
            reason = NOT_SECRET.get(path)
            if len(claims) + (1 if reason else 0) != 1:
                unclaimed.append(f"{path}: claimed by {claims}, no-secret reason {reason!r}")
        self.assertEqual([], unclaimed)

    def test_every_path_of_every_line_of_the_inventory_is_a_file_the_code_names(self) -> None:
        files = derived_files()
        stale = [f"{entry.name}: {path}" for entry in INPUTS for path in sorted(entry.paths()) if path not in files]
        self.assertEqual([], stale)
        self.assertEqual([], [path for path in NOT_SECRET if path not in files])

    def test_a_fictitious_path_added_to_a_valid_line_is_found_by_that_check(self) -> None:
        files = derived_files()
        valid = next(entry for entry in INPUTS if entry.name == "grafana_admin_password")
        widened = replace(valid, patterns=(*valid.patterns, "grafana/not_a_file"))
        self.assertEqual(["grafana/not_a_file"], sorted(path for path in widened.paths() if path not in files))

    def test_each_line_names_its_consumer_its_control_and_where_the_case_runs(self) -> None:
        names = [entry.name for entry in INPUTS]
        self.assertEqual(len(names), len(set(names)))
        for entry in INPUTS:
            with self.subTest(entry=entry.name):
                self.assertIn(entry.where, ("local", "host"))
                self.assertIn(entry.form, FORMS)
                self.assertTrue(entry.entry_point and entry.consumer and entry.control)

    def test_the_files_the_code_names_include_the_ones_this_slice_was_asked_to_list(self) -> None:
        files = derived_files()
        for expected in (
            ".env",
            "openbao/bootstrap.secret_id",
            "openbao/nessie.secret_id",
            "postgres/postgres.password",
            "postgres/airbyte.password",
            "grafana/admin.password",
            "airbyte/admin.password",
            "airbyte/storage_secret_access_key",
            "airbyte/aws_secret_access_key",
            "airbyte_api/aws_secret_access_key",
            "airbyte_api/client_secret",
            "k3s/dagster.token",
            "instance_metadata/host_role_credentials",
        ):
            with self.subTest(file=expected):
                self.assertIn(expected, files)


if __name__ == "__main__":
    unittest.main()
