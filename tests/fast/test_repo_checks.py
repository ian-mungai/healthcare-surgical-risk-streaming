"""Edge cases of the commit message and privacy patterns.

The end-to-end run in ``tests/e2e/commit_guards.py`` proves each guard through a real commit, at several seconds a
case. These tables cover the pattern edges that would each cost one more commit there: a wrongly refused scope or
breaking mark, a comment line read as the subject, a human co-author refused, a renamed credit trailer accepted, a
reserved example domain refused and a placeholder account refused.
"""

from __future__ import annotations

import unittest

from scripts.quality.repo_checks import line_privacy, message_findings

ACCEPTED_MESSAGES = [
    "feat: add the state bootstrap step",
    "fix(terraform): pin the provider",
    "refactor(infra/host)!: rename the stop command",
    "# a comment line\nci: pin the hook versions",
    "docs: add a note\n\nCo-authored-by: A Person <person@example.com>",
    "docs: describe how the Copilot extension is configured",
]
REFUSED_MESSAGES = [
    "",
    "Add the state bootstrap step",
    "feat:missing space",
    "feature: unknown type",
    "feat(Scope): upper-case scope",
    "docs: add a note\n\nAssisted-by: Codex",
    "docs: add a note\n\nGenerated with ChatGPT",
    "docs: add a note\n\nclaude-session: 1234",
]
CLEAN_LINES = [
    "contact: person@example.com",
    "role: arn:aws:iam::<ACCOUNT_ID>:role/host",
    "path: ~/projects/data",
    "case 6388 of 6388, value 120.5",
]
FLAGGED_LINES = {
    "contact: person@" + "mail.io": "email",
    "role: arn:aws:iam::" + "1" * 12 + ":role/host": "aws-account",
    "path: /" + "home/someone/data": "home-path",
    "call (555) " + "010-1234": "phone",
}


class CommitMessageTest(unittest.TestCase):
    def test_accepted(self) -> None:
        for message in ACCEPTED_MESSAGES:
            with self.subTest(message=message):
                self.assertEqual([], message_findings("message", message, comments=True))

    def test_refused(self) -> None:
        for message in REFUSED_MESSAGES:
            with self.subTest(message=message):
                self.assertNotEqual([], message_findings("message", message, comments=True))


class PrivacyLineTest(unittest.TestCase):
    def test_clean(self) -> None:
        for line in CLEAN_LINES:
            with self.subTest(line=line):
                self.assertEqual([], line_privacy(line, []))

    def test_flagged(self) -> None:
        for line, kind in FLAGGED_LINES.items():
            with self.subTest(kind=kind):
                self.assertEqual([kind], line_privacy(line, []))

    def test_declared_environment_value(self) -> None:
        self.assertEqual(["env-value"], line_privacy("bucket = planted-bucket-name", ["planted-bucket-name"]))
        self.assertEqual([], line_privacy("bucket = <BUCKET>", ["planted-bucket-name"]))


if __name__ == "__main__":
    unittest.main()
