"""Repository checks that the commit hooks and CI run.

Run from the repository root: ``.venv/bin/python -m scripts.quality.repo_checks <check> [paths]``. Each finding names
its file and line, the rule and the fix. The privacy scan reports the type of a finding and never its value.
"""

from __future__ import annotations

import argparse
import fnmatch
import os
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from scripts.process import run_command

GIT_TIMEOUT_SECONDS = 60
ASK = "if the rule seems wrong here, stop and ask the owner; do not skip the hook"

CREDENTIAL_RULE = "no credential, key or Terraform state file is committed"
CREDENTIAL_NAMES = ("*.tfstate", "*.tfstate.*", "*.tfplan", "*.pem", "*.key", "*.p12", "*.pfx", "id_rsa", "id_ecdsa", "id_ed25519", "credentials")

DATA_RULE = "data files stay out of Git except in the declared fixture folders, and no file is over 5 MB"
DATA_EXTENSIONS = {".csv", ".tsv", ".parquet", ".xlsx", ".xls", ".jsonl", ".ndjson", ".avro", ".db", ".sqlite", ".vital"}
DATA_FOLDERS = ("tests/fixtures/",)
MAX_BYTES = 5 * 1024 * 1024

MESSAGE_RULE = "commit messages follow Conventional Commits and carry no assistant credit"
COMMIT_TYPES = ("feat", "fix", "docs", "chore", "refactor", "test", "build", "ci", "perf", "style", "revert")
CONVENTIONAL_SUBJECT = re.compile(rf"(?:{'|'.join(COMMIT_TYPES)})(?:\([a-z0-9._/-]+\))?!?: \S.*")
ASSISTANTS = ("claude", "codex", "grok", "antigravity", "chatgpt", "copilot", "gemini", "cursor", "devin", "aider", "windsurf")
ASSISTANT_NAMES = re.compile(rf"\b(?:{'|'.join((*ASSISTANTS, 'anthropic', 'openai', 'xai'))})\b", re.IGNORECASE)
CREDIT_TRAILER = re.compile(r"^\s*(?:co-authored-by|co-developed-by|assisted-by|generated-by|generated-with)\s*:\s*(.*)$", re.IGNORECASE)
GENERATED_LINE = re.compile(r"^\s*(?:generated (?:with|by)|written by)\b(.*)$", re.IGNORECASE)
SESSION_TRAILER = re.compile(rf"^\s*(?:{'|'.join(ASSISTANTS)})-session\s*:", re.IGNORECASE)

PRIVACY_RULE = "no personal data or environment-specific value is in a file that Git does not ignore"
PRIVACY_FIX = (
    "replace the value with a placeholder (~, $TMPDIR, <NAME>, example.invalid) or read it from configuration;"
    " if it is meant to be public, add a reasoned entry to .privacy_allowlist"
)
ALLOWLIST = ".privacy_allowlist"
PRIVACY_PATTERNS = {
    "home-path": ("home-directory path with a user name", re.compile(r"/(?:" + "Us" + r"ers|home)/(?![<$])(?!Shared/)[A-Za-z0-9._-]+")),
    "temp-path": ("machine temporary path", re.compile(r"/var/" + r"folders/[A-Za-z0-9_+-]+/[A-Za-z0-9_+-]+")),
    "email": ("email address", re.compile(r"\b[A-Za-z0-9._%+-]+@((?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,})\b")),
    "phone": (
        "phone number",
        re.compile(
            r"(?<![\w.])(?:\+\d{1,3}[ .-]?)?(?:\(\d{3}\)[ .-]?|\d{3}[ .-])\d{3}[ .-]\d{4}(?![\w.])|(?<![\w.])\+\d{1,3}[ -]\d{2,4}[ -]\d{3}[ -]\d{3,4}(?![\w.])"
        ),
    ),
    "aws-account": ("AWS account ID", re.compile(r"arn:aws[a-z-]*:[a-z0-9-]*:[a-z0-9-]*:\d{12}:|(?i:account[_ -]?id)\W{1,4}\d{12}\b")),
}
RESERVED_DOMAINS = (".invalid", ".test", ".localhost", ".example", "example.com", "example.org", "example.net")
ENV_VALUE_KEYS = re.compile(r"PROFILE|BUCKET|ACCOUNT|ARN|ENDPOINT|HOST|EMAIL|USER")
ENV_VALUE_MINIMUM = 4


@dataclass(frozen=True)
class Finding:
    location: str
    problem: str
    rule: str
    fix: str


def git(*args: str, cwd: Path | None = None, keep_newlines: bool = False) -> str:
    """Run Git. A call that names a repository by ``cwd`` ignores the caller's Git environment, such as ``GIT_DIR`` inside a hook."""
    env = None if cwd is None else {name: value for name, value in os.environ.items() if not name.startswith("GIT_")}
    return run_command("git", args, cwd=cwd, env=env, timeout=GIT_TIMEOUT_SECONDS, check=True, keep_newlines=keep_newlines).stdout


def visible_files(cwd: Path | None = None) -> list[str]:
    """List every path that Git does not ignore, tracked or not, as NUL-delimited names so that no name is quoted."""
    listed = git("ls-files", "-z", "--cached", "--others", "--exclude-standard", cwd=cwd, keep_newlines=True)
    return sorted(set(listed.split("\0")) - {""})


def on_one_line(text: str) -> str:
    return text.replace("\r", "\\r").replace("\n", "\\n")


def report(findings: list[Finding]) -> int:
    for finding in findings:
        sys.stderr.write(f"{on_one_line(finding.location)}: {finding.problem}\n  rule: {finding.rule}\n  fix: {finding.fix}\n  {ASK}\n")
    return 1 if findings else 0


def credential_files(paths: list[str]) -> list[Finding]:
    findings = []
    for path in paths:
        name = Path(path).name
        is_env = (name == ".env" or name.startswith(".env.")) and name != ".env.example"
        if is_env or any(fnmatch.fnmatch(name, pattern) for pattern in CREDENTIAL_NAMES):
            findings.append(Finding(path, "credential, state or saved-plan file staged", CREDENTIAL_RULE, "unstage it (git rm --cached) and ignore it"))
    return findings


def data_files(paths: list[str]) -> list[Finding]:
    findings = []
    for path in paths:
        if Path(path).suffix.lower() in DATA_EXTENSIONS and not path.startswith(DATA_FOLDERS):
            findings.append(Finding(path, "data file outside the declared folders", DATA_RULE, f"move a small published sample into {', '.join(DATA_FOLDERS)}"))
        if os.path.getsize(path) > MAX_BYTES:
            findings.append(Finding(path, f"{os.path.getsize(path):,} bytes, over the 5 MB limit", DATA_RULE, "keep it out of Git"))
    return findings


def message_findings(location: str, message: str, *, comments: bool = False) -> list[Finding]:
    lines = [line for line in message.splitlines() if not (comments and line.startswith("#"))]
    subject = lines[0] if lines else ""
    findings = []
    if CONVENTIONAL_SUBJECT.fullmatch(subject) is None:
        fix = "reword as <type>(<scope>): <description>, for example ci: pin the hook versions"
        findings.append(Finding(location, f"not a Conventional Commit: {subject!r}", MESSAGE_RULE, fix))
    for number, line in enumerate(lines, 1):
        credit = CREDIT_TRAILER.match(line) or GENERATED_LINE.match(line)
        if SESSION_TRAILER.match(line) or (credit and ASSISTANT_NAMES.search(credit.group(1))):
            findings.append(
                Finding(f"{location}:{number}", "assistant credit in commit message", MESSAGE_RULE, "remove the credit or session line; human co-authors stay")
            )
    return findings


def commit_message(path: str) -> list[Finding]:
    return message_findings(path, Path(path).read_text(), comments=True)


GITLEAKS = Path(__file__).resolve().parents[2] / ".tools" / "bin" / "gitleaks"
BODY_RULE = "no credential value is in a commit message"


def message_secret_findings(messages: str) -> list[Finding]:
    """Scan whole commit messages with the pinned gitleaks, which the scan of the diffs does not read. A finding does not print the value."""
    if not GITLEAKS.is_file():
        return [Finding("commit messages", f"the secret scanner {GITLEAKS.name} is missing", BODY_RULE, "run: make setup")]
    run = run_command(str(GITLEAKS), ["stdin", "--redact", "--no-banner", "--no-color"], timeout=GIT_TIMEOUT_SECONDS, input_text=messages)
    if run.returncode == 0:
        return []
    return [Finding("commit messages", "a secret is in a commit message body (the value is not shown)", BODY_RULE, "rewrite the commit message without it")]


def commit_range(revisions: str) -> list[Finding]:
    if revisions.startswith("-"):
        raise SystemExit(f"commit-range takes a revision or a range, not an option: {revisions!r}")
    findings = []
    messages = []
    for commit in git("rev-list", revisions, "--").splitlines():
        message = git("show", "-s", "--format=%B", commit)
        messages.append(message)
        findings.extend(message_findings(f"commit {commit}", message))
    return [*findings, *message_secret_findings("\n\n".join(messages))] if messages else findings


def read_allowlist() -> tuple[list[tuple[str, str]], list[Finding]]:
    entries: list[tuple[str, str]] = []
    findings: list[Finding] = []
    kinds = [*PRIVACY_PATTERNS, "env-value"]
    if not Path(ALLOWLIST).exists():
        return entries, findings
    for number, line in enumerate(Path(ALLOWLIST).read_text().splitlines(), 1):
        text = "" if line.lstrip().startswith("#") else line.strip()
        if not text:
            continue
        match = re.fullmatch(r"(\S+)\s+(\S+)(?:\s+--\s*(.*))?", text)
        location = f"{ALLOWLIST}:{number}"
        if not match or match.group(1) not in kinds:
            findings.append(Finding(location, "allowlist entry with an unknown type", PRIVACY_RULE, f"use one of: {', '.join(kinds)}"))
        elif not (match.group(3) or "").strip():
            findings.append(Finding(location, "allowlist entry without a reason", PRIVACY_RULE, "add ' -- <why this value is intentionally public>'"))
        else:
            entries.append((match.group(1), match.group(2)))
    return entries, findings


def env_values() -> list[str]:
    if not Path(".env").is_file():
        return []
    values = []
    for line in Path(".env").read_text().splitlines():
        key, _, value = line.partition("=")
        value = value.strip().strip("\"'")
        if ENV_VALUE_KEYS.search(key.strip().upper()) and len(value) >= ENV_VALUE_MINIMUM and not line.lstrip().startswith("#"):
            values.append(value)
    return values


def line_privacy(line: str, values: list[str]) -> list[str]:
    kinds = []
    for kind, (_, pattern) in PRIVACY_PATTERNS.items():
        for match in pattern.finditer(line):
            if kind == "email" and match.group(1).lower().endswith(RESERVED_DOMAINS):
                continue
            kinds.append(kind)
            break
    if any(value in line for value in values):
        kinds.append("env-value")
    return kinds


def privacy_scan() -> tuple[list[Finding], list[str]]:
    allowed, findings = read_allowlist()
    values = env_values()
    unreviewed = []
    descriptions = {kind: description for kind, (description, _) in PRIVACY_PATTERNS.items()} | {"env-value": "value declared in .env"}
    for path in visible_files():
        if os.path.islink(path) or not os.path.isfile(path):
            continue
        data = Path(path).read_bytes()
        if b"\0" in data[:8192]:
            unreviewed.append(f"{on_one_line(path)}: unreviewed: cannot be read as text; inspect it before publishing")
            continue
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            unreviewed.append(f"{on_one_line(path)}: unreviewed: cannot be read as text; inspect it before publishing")
            continue
        for number, line in enumerate(text.splitlines(), 1):
            for kind in line_privacy(line, values):
                if not any(kind == entry_kind and fnmatch.fnmatch(path, glob) for entry_kind, glob in allowed):
                    findings.append(Finding(f"{path}:{number}", descriptions[kind], PRIVACY_RULE, PRIVACY_FIX))
    return findings, unreviewed


def main() -> int:
    """Run the named check and return 1 when it has findings."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("check", choices=["credential-files", "data-files", "commit-msg", "commit-range", "privacy-scan"])
    parser.add_argument("paths", nargs="*")
    parser.add_argument("--all", action="store_true", help="privacy-scan: every file Git does not ignore, which is the only scope")
    args = parser.parse_args()
    if args.check == "privacy-scan":
        findings, unreviewed = privacy_scan()
        for line in unreviewed:
            sys.stderr.write(f"{line}\n")
        return report(findings)
    runners: dict[str, Callable[[], list[Finding]]] = {
        "credential-files": lambda: credential_files(args.paths),
        "data-files": lambda: data_files(args.paths),
        "commit-msg": lambda: commit_message(args.paths[0]),
        "commit-range": lambda: commit_range(args.paths[0]),
    }
    return report(runners[args.check]())


if __name__ == "__main__":
    sys.exit(main())
