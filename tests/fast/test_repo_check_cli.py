"""The repository checks through their command-line entry, in a temporary Git repository.

Each case runs ``python -m scripts.quality.repo_checks`` the way a hook or CI runs it. Failure modes covered here, none of
which the planted-fault run in ``tests/e2e/commit_guards.py`` reaches cheaply:

- A path that Git quotes in its default listing (a non-ASCII name or a control character) is skipped by the privacy scan.
- The ``Generated-with`` trailer is accepted in the commit-msg entry or in the commit-range entry that CI uses.
- A factual tool mention in the same trailer form is refused (too wide a rule).
"""

from __future__ import annotations

import unittest

from scripts.process import CompletedProcess
from tests.fast.git_fixture import TemporaryRepository

EMAIL = "person@" + "mail.io"
CREDIT = "assistant credit in commit message"
QUOTED_NAMES = ["café.txt", "tab\tname.txt"]


class PrivacyScanPathTest(TemporaryRepository):
    def test_a_path_that_git_quotes_is_scanned(self) -> None:
        for name in QUOTED_NAMES:
            (self.repo / f"tracked_{name}").write_text(f"contact: {EMAIL}\n")
            (self.repo / f"untracked_{name}").write_text(f"contact: {EMAIL}\n")
            self.git("add", "--", f"tracked_{name}")
        result = self.check("privacy-scan", "--all")
        self.assertEqual(1, result.returncode, result.stderr)
        for name in QUOTED_NAMES:
            for prefix in ("tracked_", "untracked_"):
                with self.subTest(path=f"{prefix}{name}"):
                    self.assertIn(f"{prefix}{name}:1: email address", result.stderr)

    def test_clean_quoted_paths_pass(self) -> None:
        for name in QUOTED_NAMES:
            (self.repo / name).write_text("contact: person@example.com\n")
        self.git("add", "--all")
        result = self.check("privacy-scan", "--all")
        self.assertEqual(0, result.returncode, result.stderr)


class GeneratedWithTrailerTest(TemporaryRepository):
    CREDITED = ["Generated-with: Claude Code", "generated-with: Codex", "Generated-With: ChatGPT"]
    FACTUAL = ["Generated-with: ruff", "Generated-by: a person"]

    def message_check(self, trailer: str) -> CompletedProcess[str]:
        message = self.repo / "message.txt"
        message.write_text(f"docs: add a note\n\n{trailer}\n")
        return self.check("commit-msg", str(message))

    def range_check(self, trailer: str) -> CompletedProcess[str]:
        self.git("commit", "--quiet", "--allow-empty", "-m", "docs: add a note", "-m", trailer)
        return self.check("commit-range", "HEAD")

    def test_credited_trailer_is_refused_in_both_entries(self) -> None:
        for trailer in self.CREDITED:
            with self.subTest(trailer=trailer, entry="commit-msg"):
                result = self.message_check(trailer)
                self.assertEqual(1, result.returncode, result.stderr)
                self.assertIn(CREDIT, result.stderr)
            with self.subTest(trailer=trailer, entry="commit-range"):
                result = self.range_check(trailer)
                self.assertEqual(1, result.returncode, result.stderr)
                self.assertIn(CREDIT, result.stderr)

    def test_factual_tool_mention_is_accepted_in_both_entries(self) -> None:
        for trailer in self.FACTUAL:
            with self.subTest(trailer=trailer, entry="commit-msg"):
                result = self.message_check(trailer)
                self.assertEqual(0, result.returncode, result.stderr)
            with self.subTest(trailer=trailer, entry="commit-range"):
                result = self.range_check(trailer)
                self.assertEqual(0, result.returncode, result.stderr)


if __name__ == "__main__":
    unittest.main()
