"""End-to-end check of the commit guards through real ``git commit`` calls.

Run from the repository root: ``.venv/bin/python -m tests.e2e.commit_guards``. ``--shard INDEX/COUNT`` runs one share
of the cases, for parallel jobs. The run copies every file Git does not ignore into a temporary repository, installs
the hooks there and makes a baseline commit. Each case then makes a passing control commit and a planted-fault commit.
The control must advance HEAD. The fault must be refused by its named guard alone, with every message that guard is expected
to print, and leave HEAD unchanged. The planted fast test is proven by its full test name, its assertion message and a
summary of one failure. Every run, passing, failing, blocked or interrupted, writes one record in a new folder under
``runs/`` and removes its temporary repository. The source tree is hashed before and after the run, and a change between
the two fails the run.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import platform
import re
import shutil
import sys
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

from scripts.process import run_command
from scripts.quality import worktree_scan_hook
from scripts.quality.repo_checks import git, visible_files
from tests.e2e.run_record import Observation, Record

ROOT = Path(__file__).resolve().parents[2]
RERUN = ".venv/bin/python -m tests.e2e.commit_guards"
COMMAND_TIMEOUT_SECONDS = 300
LINKED_FOLDERS = (".venv", ".tools")
FAILED_HOOK = re.compile(r"\.Failed\n- hook id: (\S+)")
GOOD_MESSAGE = "docs: add a planted note"
TRACKED_NOTE = "notes/tracked.txt"
QUOTED_NOTE = "notes/caf\u00e9.txt"
CARRIAGE_NOTE = "notes/line\rbreak.txt"
WORKTREE_GUARD = "working-tree privacy hook"
PLANTED_TEST = "tests.fast.test_planted.PlantedTest.test_planted"
PLANTED_MESSAGE = "planted fast-test fault"
# unittest's summary for exactly one failure and no error; the suite's skipped tests may follow. A pattern message is matched against the
# run's last summary line only, so a summary quoted inside a failure message cannot stand for the run.
ONE_FAILURE_SUMMARY = re.compile(r"FAILED \(failures=1(, skipped=\d+)?\)")
SUMMARY_LINE = re.compile(r"^(?:OK|FAILED)\b.*$", re.MULTILINE)
CLEAN_PYTHON = "def double(value: int) -> int:\n    return value * 2\n"


@dataclass(frozen=True)
class Change:
    """One attempted commit: files to write, the ones to stage and the commit message."""

    files: dict[str, str]
    staged: tuple[str, ...]
    message: str = GOOD_MESSAGE
    unstaged: dict[str, str] = field(default_factory=dict)
    force: bool = False


@dataclass(frozen=True)
class Case:
    name: str
    guard: str
    hooks: tuple[str, ...]
    messages: tuple[str | re.Pattern[str], ...]
    control: Change
    fault: Change


def planted_token() -> str:
    return "ghp_" + hashlib.sha256(b"planted").hexdigest()[:36]


def planted_account_reference() -> str:
    return "arn:aws:iam::" + "4" * 4 + "7" * 4 + "1" * 4 + ":role/planted"


def hook_case(name: str, hook: str, message: str | tuple[str | re.Pattern[str], ...], control: Change, fault: Change) -> Case:
    return Case(name, hook, (hook,), (message,) if isinstance(message, str) else message, control, fault)


def planted_cases() -> list[Case]:
    note = {"notes/planted.txt": "A planted note.\n"}
    staged_note = ("notes/planted.txt",)
    failing_test = (
        "import unittest\n\n\nclass PlantedTest(unittest.TestCase):\n    def test_planted(self) -> None:\n"
        f'        self.assertEqual(1, {{value}}, "{PLANTED_MESSAGE}")\n'
    )
    clean_role = "role: arn:aws:iam::<ACCOUNT_ID>:role/planted\n"
    marker_role = f"role: {planted_account_reference()}\n"
    account = "AWS account ID"
    return [
        hook_case(
            "privacy marker in an untracked file",
            "privacy-scan",
            f"scratch/untracked.txt:1: {account}",
            Change({**note, "scratch/untracked.txt": clean_role}, staged_note),
            Change({**note, "scratch/untracked.txt": marker_role}, staged_note),
        ),
        Case(
            "privacy marker in a tracked file with an unstaged edit",
            WORKTREE_GUARD,
            (),
            (f"{TRACKED_NOTE}:1: {account}",),
            Change(note, staged_note, unstaged={TRACKED_NOTE: clean_role}),
            Change(note, staged_note, unstaged={TRACKED_NOTE: marker_role}),
        ),
        hook_case(
            "privacy marker in a file whose name Git quotes",
            "privacy-scan",
            f"{QUOTED_NOTE}:1: {account}",
            Change({QUOTED_NOTE: clean_role}, (QUOTED_NOTE,)),
            Change({QUOTED_NOTE: marker_role}, (QUOTED_NOTE,)),
        ),
        hook_case(
            "privacy marker in a file whose name holds a carriage return",
            "privacy-scan",
            f"notes/line\\rbreak.txt:1: {account}",
            Change({CARRIAGE_NOTE: clean_role}, (CARRIAGE_NOTE,)),
            Change({CARRIAGE_NOTE: marker_role}, (CARRIAGE_NOTE,)),
        ),
        hook_case(
            "secret-like string in a staged file",
            "gitleaks",
            "leaks found: 1",
            Change({"notes/token.txt": "token = ghp_<TOKEN>\n"}, ("notes/token.txt",)),
            Change({"notes/token.txt": f"token = {planted_token()}\n"}, ("notes/token.txt",)),
        ),
        hook_case(
            "badly formatted Python file",
            "ruff-format",
            "unformatted: File would be reformatted\n --> scripts/planted.py",
            Change({"scripts/planted.py": CLEAN_PYTHON}, ("scripts/planted.py",)),
            Change({"scripts/planted.py": CLEAN_PYTHON.replace("value * 2", "value*2")}, ("scripts/planted.py",)),
        ),
        hook_case(
            "unused import",
            "ruff-check",
            "F401 [*] `os` imported but unused\n --> scripts/planted.py:1:8",
            Change({"scripts/planted.py": CLEAN_PYTHON}, ("scripts/planted.py",)),
            Change({"scripts/planted.py": "import os\n\n\n" + CLEAN_PYTHON}, ("scripts/planted.py",)),
        ),
        hook_case(
            "untyped function signature",
            "mypy",
            "scripts/planted.py:1: error: Function is missing a type annotation",
            Change({"scripts/planted.py": CLEAN_PYTHON}, ("scripts/planted.py",)),
            Change({"scripts/planted.py": "def double(value):\n    return value * 2\n"}, ("scripts/planted.py",)),
        ),
        hook_case(
            "credential file",
            "credential-files",
            "notes/deploy.pem: credential, state or saved-plan file staged",
            Change({"notes/deploy.txt": "placeholder\n"}, ("notes/deploy.txt",)),
            Change({"notes/deploy.pem": "placeholder\n"}, ("notes/deploy.pem",)),
        ),
        hook_case(
            "numbered Terraform state snapshot",
            "credential-files",
            "notes/dev.tfstate.1700000000: credential, state or saved-plan file staged",
            Change({"notes/deploy.txt": "placeholder\n"}, ("notes/deploy.txt",)),
            Change({"notes/dev.tfstate.1700000000": "placeholder\n"}, ("notes/dev.tfstate.1700000000",), force=True),
        ),
        hook_case(
            "data file outside the declared folders",
            "data-files",
            "notes/sample.csv: data file outside the declared folders",
            Change({"notes/sample.txt": "caseid,value\n"}, ("notes/sample.txt",)),
            Change({"notes/sample.csv": "caseid,value\n"}, ("notes/sample.csv",)),
        ),
        hook_case(
            "commit message without a type",
            "commit-message",
            "not a Conventional Commit: 'updated the planted note'",
            Change(note, staged_note),
            Change(note, staged_note, "updated the planted note"),
        ),
        hook_case(
            "commit message with an assistant credit",
            "commit-message",
            "assistant credit in commit message",
            Change(note, staged_note, GOOD_MESSAGE + "\n\nCo-authored-by: A Person <person@example.com>"),
            Change(note, staged_note, GOOD_MESSAGE + "\n\nCo-authored-by: Claude <assistant@example.com>"),
        ),
        hook_case(
            "commit message with a Generated-with trailer",
            "commit-message",
            "assistant credit in commit message",
            Change(note, staged_note, GOOD_MESSAGE + "\n\nGenerated-with: ruff"),
            Change(note, staged_note, GOOD_MESSAGE + "\n\nGenerated-with: Claude Code"),
        ),
        hook_case(
            "failing fast test",
            "fast-tests",
            (f"FAIL: test_planted ({PLANTED_TEST})", f"AssertionError: 1 != 2 : {PLANTED_MESSAGE}", ONE_FAILURE_SUMMARY),
            Change({"tests/fast/test_planted.py": failing_test.format(value=1)}, ("tests/fast/test_planted.py",)),
            Change({"tests/fast/test_planted.py": failing_test.format(value=2)}, ("tests/fast/test_planted.py",)),
        ),
    ]


def source_files(root: Path = ROOT) -> list[str]:
    return [path for path in visible_files(root) if (root / path).is_file()]


def tree_digest(paths: list[str]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.encode() + b"\0" + hashlib.sha256((ROOT / path).read_bytes()).digest())
    return digest.hexdigest()


def recorded_state(root: Path) -> tuple[str, str]:
    """The revision and the digest a run record holds.

    The digest names the tree of the commit, so a clean checkout of the same content gives the same value. Nothing is hashed from the folder: a
    tracked change that is not committed is named in the revision instead, and an untracked file changes neither value.
    """
    env = {name: value for name, value in os.environ.items() if not name.startswith("GIT_")}
    # Exit 1 is Git's answer for a branch with no commit yet, and also for a branch it cannot read, which the checked call then refuses.
    if run_command("git", ["rev-parse", "--verify", "--quiet", "HEAD"], cwd=root, env=env, timeout=COMMAND_TIMEOUT_SECONDS).returncode == 1:
        git("symbolic-ref", "--quiet", "HEAD", cwd=root)
        return "no commit", hashlib.sha256(b"no commit").hexdigest()
    head, tree = git("rev-parse", "HEAD", "HEAD^{tree}", cwd=root).split()
    # Without the optional locks, the status call does not rewrite the index.
    changed = git("--no-optional-locks", "status", "--porcelain", "--untracked-files=no", cwd=root)
    return head + (" with uncommitted changes" if changed else ""), hashlib.sha256(tree.encode()).hexdigest()


def tool_version(program: str, *args: str) -> str:
    return run_command(str(ROOT / program), [*args, "--version"], cwd=ROOT, timeout=COMMAND_TIMEOUT_SECONDS, check=True).stdout.strip()


class Fixture:
    """A temporary repository holding a copy of the source tree, with the hooks installed."""

    def __init__(self, record: Record) -> None:
        self.record = record
        self.home = Path(tempfile.mkdtemp(prefix="commit_guards_"))
        self.repo = self.home / "repo"
        self.env = {
            "PATH": os.environ["PATH"],
            "HOME": str(self.home),
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "PRE_COMMIT_HOME": str(self.home / "pre_commit"),
            "GIT_AUTHOR_NAME": "Fixture",
            "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
            "GIT_COMMITTER_NAME": "Fixture",
            "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
        }

    def populate(self, paths: list[str]) -> None:
        for path in paths:
            target = self.repo / path
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / path, target)
        for folder in LINKED_FOLDERS:
            (self.repo / folder).symlink_to(ROOT / folder, target_is_directory=True)
        (self.repo / TRACKED_NOTE).parent.mkdir(parents=True, exist_ok=True)
        (self.repo / TRACKED_NOTE).write_text("A tracked note.\n")

    def run(self, program: str, *args: str) -> tuple[int, str]:
        self.record.commands.append(" ".join([program, *args]))
        result = run_command(program, list(args), cwd=self.repo, env=self.env, timeout=COMMAND_TIMEOUT_SECONDS)
        return result.returncode, (result.stdout + result.stderr).replace(str(self.home), "<FIXTURE>")

    def must(self, program: str, *args: str) -> str:
        code, output = self.run(program, *args)
        if code != 0:
            raise RuntimeError(f"fixture command failed: {program} {' '.join(args)}\n{output}")
        return output

    def head(self) -> str:
        return self.must("git", "rev-parse", "HEAD").strip()

    def attempt(self, change: Change) -> tuple[int, str, bool]:
        before = self.head()
        for path, content in change.files.items():
            target = self.repo / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
        self.must("git", "add", *(["--force"] if change.force else []), "--", *change.staged)
        for path, content in change.unstaged.items():
            (self.repo / path).write_text(content)
        code, output = self.run("git", "commit", "-m", change.message)
        return code, output, self.head() != before

    def reset(self, to: str) -> None:
        self.must("git", "reset", "--quiet", "--hard", to)
        self.must("git", "clean", "--quiet", "-fd")

    def remove(self) -> None:
        shutil.rmtree(self.home)


def judge_fault(case: Case, code: int, output: str, advanced: bool) -> Observation:
    """Judge a planted-fault commit: refused by the named hooks alone, with the guard's expected message, and HEAD unchanged."""
    failed = sorted(set(FAILED_HOOK.findall(output)))
    summaries = SUMMARY_LINE.findall(output)
    found = all(
        (bool(summaries) and message.fullmatch(summaries[-1].strip()) is not None) if isinstance(message, re.Pattern) else message in output
        for message in case.messages
    )
    named = f"only {', '.join(case.hooks)} failed" if case.hooks else "no pre-commit hook failed"
    expected = f"non-zero exit, {named}, every message of {list(case.messages)!r} found and HEAD unchanged"
    observed = f"exit {code}, failed hooks: {failed}, every message found: {found}, HEAD advanced: {advanced}"
    return Observation(case.name, "fault", expected, observed, code != 0 and failed == sorted(case.hooks) and found and not advanced)


def run_case(fixture: Fixture, case: Case, log: list[str]) -> list[Observation]:
    start = fixture.head()
    code, output, advanced = fixture.attempt(case.control)
    log.append(f"== {case.name}: control\n{output}")
    control = Observation(case.name, "control", "exit 0 and HEAD advanced", f"exit {code}, HEAD advanced: {advanced}", code == 0 and advanced)
    fixture.reset(start)
    code, output, advanced = fixture.attempt(case.fault)
    log.append(f"== {case.name}: fault\n{output}")
    fault = judge_fault(case, code, output, advanced)
    fixture.reset(start)
    return [control, fault]


def parse_shard(text: str) -> tuple[int, int]:
    match = re.fullmatch(r"(\d+)/(\d+)", text)
    if not match or not 1 <= int(match.group(1)) <= int(match.group(2)):
        raise argparse.ArgumentTypeError("use INDEX/COUNT with 1 <= INDEX <= COUNT, for example 1/2")
    return int(match.group(1)), int(match.group(2))


def describe_run(record: Record) -> None:
    record.environment = {
        "platform": f"{platform.system()} {platform.machine()}",
        "python": platform.python_version(),
        "pre_commit": tool_version(".venv/bin/pre-commit"),
        "ruff": tool_version(".venv/bin/ruff"),
        "mypy": tool_version(".venv/bin/mypy"),
        "gitleaks": tool_version(".tools/bin/gitleaks"),
    }
    record.revision, record.tree_sha256 = recorded_state(ROOT)


def run_cases(fixture: Fixture, record: Record, log: list[str], shard: tuple[int, int]) -> None:
    index, count = shard
    fixture.must("git", "init", "--quiet", "--initial-branch", "main")
    fixture.must(".venv/bin/pre-commit", "install")
    worktree_scan_hook.install(fixture.repo)
    fixture.must("git", "add", "--all")
    code, output = fixture.run("git", "commit", "-m", "chore: add the source tree")
    log.append(f"== baseline\n{output}")
    record.observations.append(Observation("baseline", "control", "exit 0", f"exit {code}", code == 0))
    if code == 0:
        for number, case in enumerate(planted_cases()):
            if number % count == index - 1:
                record.observations.extend(run_case(fixture, case, log))


def remove_fixture(fixture: Fixture | None, record: Record) -> None:
    if fixture is None:
        record.cleanup = "no temporary repository was made"
        return
    try:
        fixture.remove()
    except OSError as error:
        record.cleanup = f"removal failed: {type(error).__name__}"
        record.expect("cleanup", "remove", "temporary repository removed", record.cleanup, False)
    else:
        record.cleanup = "temporary repository removed"


def main(argv: Sequence[str] | None = None) -> int:
    """Run the planted-fault cases, write the run record and return 0 only when every assertion held."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--shard", type=parse_shard, default=(1, 1), metavar="INDEX/COUNT", help="run one share of the cases")
    index, count = parser.parse_args(argv).shard
    record = Record(feature="commit guards, local commit path", slug="commit_guards", rerun=RERUN, mode="full" if count == 1 else f"shard {index} of {count}")
    record.limits = ["local commit path only: the hosted CI path has its own run", "the planted token and account reference are fake values built at run time"]
    log: list[str] = []
    fixture: Fixture | None = None
    blocked = ""
    try:
        paths = source_files()
        before = tree_digest(paths)
        describe_run(record)
        fixture = Fixture(record)
        fixture.populate(paths)
        run_cases(fixture, record, log, (index, count))
        unchanged = tree_digest(source_files()) == before
        record.observations.append(Observation("source tree", "snapshot", "unchanged during the run", f"unchanged: {unchanged}", unchanged))
    except KeyboardInterrupt:
        blocked = "interrupted"
        raise
    except Exception as error:
        log.append(f"{type(error).__name__}: {error}")
        record.expect("run", "protected execution", "every step completes", f"{type(error).__name__}; see log.txt", False)
    finally:
        code = record.close_after(lambda: remove_fixture(fixture, record), log, blocked=blocked)
    return code


if __name__ == "__main__":
    sys.exit(main())
