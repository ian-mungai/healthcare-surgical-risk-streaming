"""End-to-end check of the commit guards through real ``git commit`` calls.

Run from the repository root: ``.venv/bin/python -m tests.e2e.commit_guards``. ``--shard INDEX/COUNT`` runs one share
of the cases, for parallel jobs. ``--parallel N`` runs the shards ``1/N`` to ``N/N`` at the same time in one process, each in its own temporary
repository, within the free cores and memory (one worker when the memory is unknown), and writes one record that lists every observation and checks that every
case ran once. A parallel run holds one lock for the whole computer from before its worker plan until its repositories are removed, and a second one fails
at once. ``--compare FIRST SECOND`` compares the records of two complete passing runs of the same code and of the same copied files.
The run copies every file Git does not ignore into a temporary repository, installs the hooks there and makes a baseline commit.
Each case then makes a passing control commit and a planted-fault commit.
The control must advance HEAD. The fault must be refused by its named guard alone, with every message that guard is expected
to print, and leave HEAD unchanged. The planted fast test is proven by its full test name, its assertion message and a
summary of one failure. Every run, passing, failing, blocked or interrupted, writes one record in a new folder under
``runs/`` and removes its temporary repositories, except one whose worker is still running after the shutdown deadline: that repository is kept, and the
record names it. A parallel request records the coverage on every exit. An interrupt that arrives during the worker shutdown, the merge, the removal or
the record write waits until they end. The source tree is hashed before and after the run, and a change between the two fails the run. Each fixture reads
only its own Git settings.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import platform
import re
import shutil
import signal
import stat
import sys
import tempfile
import threading
import time
from collections import Counter
from collections.abc import Iterator, Sequence
from contextlib import ExitStack
from dataclasses import dataclass, field
from pathlib import Path
from subprocess import CalledProcessError, TimeoutExpired
from types import FrameType
from typing import Any

from scripts.process import run_command
from scripts.quality import worktree_scan_hook
from scripts.quality.repo_checks import git, visible_files
from tests.e2e.run_record import Observation, Record

ROOT = Path(__file__).resolve().parents[2]
RERUN = ".venv/bin/python -m tests.e2e.commit_guards"
COMMAND_TIMEOUT_SECONDS = 300
# An estimate, not a measurement, of the memory that one worker's commits use. It caps the workers by free memory.
WORKER_MIB = 2048
MEMINFO = Path("/proc/meminfo")
# One lock file for the whole computer: a parallel run holds it from before the worker plan to after the cleanup.
MACHINE_LOCK = Path(tempfile.gettempdir()) / "healthcare_surgical_risk_streaming_commit_guards.lock"
# The time that the workers have to end after a stop: one command's deadline, plus a minute for the reset that follows it.
SHUTDOWN_SECONDS = COMMAND_TIMEOUT_SECONDS + 60
BASELINE = "baseline"
COVERAGE = "shards"
COPIED_DIGEST = "copied_files_sha256"
LINKED_FOLDERS = (".venv", ".tools")
FAILED_HOOK = re.compile(r"\.Failed\n- hook id: (\S+)")
GOOD_MESSAGE = "docs: add a planted note"
TRACKED_NOTE = "notes/tracked.txt"
QUOTED_NOTE = "notes/caf\u00e9.txt"
CARRIAGE_NOTE = "notes/line\rbreak.txt"
WORKTREE_GUARD = "working-tree privacy hook"
PLANTED_TEST = "tests.fast.test_planted.PlantedTest.test_planted"
PLANTED_MESSAGE = "planted fast-test fault"
# unittest's summary for exactly one failure and no error; the suite's skipped tests may follow. A pattern message is matched only against
# the status line that directly follows unittest's own "Ran N tests in X s" footer, so a summary quoted in a failure message or printed by a
# test (pre-commit can emit buffered output after the footer) cannot stand for the run.
ONE_FAILURE_SUMMARY = re.compile(r"FAILED \(failures=1(, skipped=\d+)?\)")
SUMMARY_LINE = re.compile(r"^Ran \d+ tests? in \d+(?:\.\d+)?s\n\n((?:OK|FAILED)\b.*)$", re.MULTILINE)
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


def git_mode(path: Path) -> str:
    """The mode that Git records for the copy of a file: 100755 when the owner may execute it, else 100644. A link to a file counts as its target."""
    return "100755" if path.stat().st_mode & stat.S_IXUSR else "100644"


def tree_digest(paths: list[str]) -> str:
    """The digest of the copied files: each path, its Git mode and its bytes, so two runs that differ in an executable bit are not the same code."""
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.encode() + b"\0" + git_mode(ROOT / path).encode() + b"\0" + hashlib.sha256((ROOT / path).read_bytes()).digest())
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


def meminfo_available_mib(text: str) -> int | None:
    """The ``MemAvailable`` line of a Linux ``/proc/meminfo`` in mebibytes, or None when it has none."""
    found = re.search(r"^MemAvailable:\s+(\d+) kB$", text, re.MULTILINE)
    return int(found.group(1)) // 1024 if found else None


def vm_stat_available_mib(text: str) -> int | None:
    """The free, inactive and speculative pages of macOS ``vm_stat`` in mebibytes, or None when one of the three or the page size is missing."""
    size = re.search(r"page size of (\d+) bytes", text)
    pages = dict(re.findall(r"^Pages (free|inactive|speculative):\s+(\d+)\.$", text, re.MULTILINE))
    if size is None or len(pages) != 3:
        return None
    return sum(int(count) for count in pages.values()) * int(size.group(1)) // 1024 // 1024


def available_memory_mib() -> int | None:
    """The memory that a new process can use, in mebibytes, from Linux or macOS. None when this computer gives no figure."""
    try:
        if MEMINFO.is_file():
            return meminfo_available_mib(MEMINFO.read_text())
        if platform.system() == "Darwin":
            return vm_stat_available_mib(run_command("vm_stat", [], timeout=COMMAND_TIMEOUT_SECONDS, check=True).stdout)
    except OSError, CalledProcessError, TimeoutExpired:
        return None
    return None


def plan_workers(requested: int, case_count: int, cores: int, available_mib: int | None) -> int:
    """The workers to start: the request, capped by the cases, by the cores less one for the computer's other work and by the free memory. At least one.

    A computer that gives no memory figure gets one worker: nothing shows that a second one fits.
    """
    memory_limit = 1 if available_mib is None else available_mib // WORKER_MIB
    return max(1, min(requested, case_count, cores - 1, memory_limit))


class MachineLockError(RuntimeError):
    """Another guard run holds the machine-wide lock."""


@contextlib.contextmanager
def machine_lock() -> Iterator[None]:
    """Hold the one lock of this computer's parallel guard runs, or fail at once when another run holds it. The lock ends with the process.

    Two runs that each read the free memory and each admit all their workers would use twice the memory that the plan counted, so a parallel run reads the
    memory only under this lock and keeps the lock until its repositories are removed.
    """
    descriptor = os.open(MACHINE_LOCK, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(descriptor)
        raise MachineLockError(f"another guard run holds the machine-wide lock {MACHINE_LOCK}, so this run did nothing") from None
    try:
        yield
    finally:
        os.close(descriptor)


class Interrupts:
    """The SIGINTs of one run. The first raises ``KeyboardInterrupt`` as usual, in the main thread. From then on, or from ``defer``, a SIGINT is only counted.

    The worker shutdown, the merge, the removal of the repositories and the writing of the record then end before the run raises its ``KeyboardInterrupt``.
    """

    def __init__(self) -> None:
        self.count = 0
        self.deferring = False

    @property
    def seen(self) -> bool:
        return self.count > 0

    def defer(self) -> None:
        self.deferring = True

    def handle(self, signum: int, frame: FrameType | None) -> None:
        self.count += 1
        if not self.deferring:
            self.deferring = True
            raise KeyboardInterrupt

    def raise_pending(self) -> None:
        """Raise the ``KeyboardInterrupt`` that was deferred, once the work that it must not cut short has ended."""
        if self.seen:
            raise KeyboardInterrupt


@contextlib.contextmanager
def interrupt_control() -> Iterator[Interrupts]:
    """Install the run's SIGINT handler and put the previous one back. Outside the main thread no handler can be installed, and the SIGINT reaches no worker."""
    interrupts = Interrupts()
    if threading.current_thread() is not threading.main_thread():
        yield interrupts
        return
    previous = signal.signal(signal.SIGINT, interrupts.handle)
    try:
        yield interrupts
    finally:
        signal.signal(signal.SIGINT, signal.SIG_DFL if previous is None else previous)


class Fixture:
    """A temporary repository holding a copy of the source tree, with the hooks installed."""

    def __init__(self, record: Record) -> None:
        self.record = record
        self.home = Path(tempfile.mkdtemp(prefix="commit_guards_"))
        self.repo = self.home / "repo"
        # Every Git setting is read from the fixture: a global core.hooksPath of the caller must not send a hook outside the repository.
        self.env = {
            "PATH": os.environ["PATH"],
            "HOME": str(self.home),
            "XDG_CONFIG_HOME": str(self.home / "xdg_config"),
            "GIT_CONFIG_GLOBAL": str(self.home / "gitconfig"),
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

    def install_scan_hook(self) -> None:
        """Install the working-tree privacy hook into this repository's own hook folder, resolved in the fixture's Git environment."""
        worktree_scan_hook.install(self.repo, env=self.env)

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


def run_case(fixture: Fixture, case: Case, log: list[str], stop: threading.Event | None = None) -> None:
    """Run one case and record each step in the fixture's record when it ends, so a later step that raises keeps the steps before it.

    A set ``stop`` starts no fault commit after the control.
    """
    start = fixture.head()
    code, output, advanced = fixture.attempt(case.control)
    log.append(f"== {case.name}: control\n{output}")
    fixture.record.observations.append(
        Observation(case.name, "control", "exit 0 and HEAD advanced", f"exit {code}, HEAD advanced: {advanced}", code == 0 and advanced)
    )
    fixture.reset(start)
    if stop is not None and stop.is_set():
        return
    code, output, advanced = fixture.attempt(case.fault)
    log.append(f"== {case.name}: fault\n{output}")
    fixture.record.observations.append(judge_fault(case, code, output, advanced))
    fixture.reset(start)


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


def run_cases(fixture: Fixture, record: Record, log: list[str], shard: tuple[int, int], stop: threading.Event | None = None) -> None:
    """Run the cases of one shard in the fixture. A set ``stop`` starts no further case."""
    index, count = shard
    fixture.must("git", "init", "--quiet", "--initial-branch", "main")
    fixture.must(".venv/bin/pre-commit", "install")
    fixture.install_scan_hook()
    fixture.must("git", "add", "--all")
    code, output = fixture.run("git", "commit", "-m", "chore: add the source tree")
    log.append(f"== baseline\n{output}")
    record.observations.append(Observation(BASELINE, "control", "exit 0", f"exit {code}", code == 0))
    if code == 0:
        for number, case in enumerate(planted_cases()):
            if stop is not None and stop.is_set():
                break
            if number % count == index - 1:
                run_case(fixture, case, log, stop)


def remove_fixtures(fixtures: list[Fixture], record: Record, kept: Sequence[Fixture] = ()) -> None:
    """Remove every temporary repository except those in ``kept``, whose workers are still running. A removal that fails does not leave the others behind."""
    note = f", {len(kept)} kept because its worker is still running" if kept else ""
    if not fixtures:
        record.cleanup = "no temporary repository was made" if not kept else f"no temporary repository was removed{note}"
        return
    try:
        with ExitStack() as stack:
            for fixture in fixtures:
                stack.callback(fixture.remove)
    except OSError as error:
        record.cleanup = f"removal failed: {type(error).__name__}{note}"
        record.expect("cleanup", "remove", "temporary repository removed", record.cleanup, False)
    else:
        record.cleanup = ("temporary repository removed" if len(fixtures) == 1 else f"{len(fixtures)} temporary repositories removed") + note


@dataclass
class Worker:
    """One shard of a parallel run, with its own observations and log, so no two workers write the same list."""

    index: int
    count: int
    record: Record
    log: list[str] = field(default_factory=list)
    fixture: Fixture | None = None

    @property
    def label(self) -> str:
        return f"shard {self.index} of {self.count}"


def work(worker: Worker, paths: list[str], fixtures: list[Fixture], stop: threading.Event) -> None:
    """The body of a worker's thread: its fixture, filled and run for its shard. A failure is the worker's own failed observation."""
    try:
        fixture = Fixture(worker.record)
        worker.fixture = fixture
        fixtures.append(fixture)
        fixture.populate(paths)
        run_cases(fixture, worker.record, worker.log, (worker.index, worker.count), stop)
    except Exception as error:
        worker.log.append(f"{type(error).__name__}: {error}")
        worker.record.expect(worker.label, "protected execution", "every step completes", f"{type(error).__name__}; see log.txt", False)


def join_all(threads: list[threading.Thread], deadline: float | None = None) -> None:
    """Wait for every started thread, or until ``deadline`` on the monotonic clock. Without a deadline the wait is bounded by the deadlines of the commands."""
    for thread in threads:
        if thread.ident is not None:
            thread.join(None if deadline is None else max(0.0, deadline - time.monotonic()))


def case_gaps(seen: Counter[tuple[str, str]]) -> tuple[list[str], list[str]]:
    """The cases and steps of the case list that ``seen`` lacks, and those that it holds more than once."""
    wanted = [(case.name, step) for case in planted_cases() for step in ("control", "fault")]
    return [f"{case} ({step})" for case, step in wanted if seen[(case, step)] == 0], [f"{case} ({step})" for case, step in wanted if seen[(case, step)] > 1]


def covered(observations: list[Observation]) -> Observation:
    """Every case once, control and fault, in the merged observations of the workers."""
    missing, repeated = case_gaps(Counter((item.case, item.step) for item in observations))
    observed = f"missing: {missing}, repeated: {repeated}" if missing or repeated else "every case observed once"
    return Observation(COVERAGE, "coverage", "every case observed once, control and fault", observed, not missing and not repeated)


def record_coverage(record: Record) -> None:
    """Add the coverage observation. The run's finalizer, its only caller, calls it once on every exit of a parallel request: a lock refusal, a failed source
    read, an interrupt before the workers, a failed fixture, one worker after the cap, and a complete run."""
    record.observations.append(covered(record.observations))


def merge_workers(record: Record, log: list[str], team: list[Worker]) -> None:
    """Put the workers' baselines, then their case observations in the order of the case list, then their commands and logs into the one record."""
    order = {case.name: number for number, case in enumerate(planted_cases())}
    every = [item for worker in team for item in list(worker.record.observations)]
    record.observations.extend(item for item in every if item.case == BASELINE)
    record.observations.extend(sorted((item for item in every if item.case != BASELINE), key=lambda item: order.get(item.case, len(order))))
    for worker in team:
        record.commands.extend(worker.record.commands)
        log.extend(worker.log)


def keep_running_workers(
    team: list[Worker], threads: list[threading.Thread], fixtures: list[Fixture], kept: list[Fixture], record: Record, log: list[str]
) -> None:
    """Name each worker that did not end in time, and keep its repository out of the removal: no repository is removed under a running worker."""
    for worker, thread in zip(team, threads, strict=True):
        if not thread.is_alive():
            continue
        if worker.fixture is not None and worker.fixture in fixtures:
            fixtures.remove(worker.fixture)
            kept.append(worker.fixture)
        record.expect(
            worker.label, "shutdown", f"the worker ends within {SHUTDOWN_SECONDS:g} seconds of the stop", "still running; its repository is kept", False
        )
        log.append(f"{worker.label} was still running after the stop; its repository stays at {getattr(worker.fixture, 'home', 'an unknown place')}")


def run_parallel(paths: list[str], record: Record, log: list[str], fixtures: list[Fixture], kept: list[Fixture], workers: int, interrupts: Interrupts) -> None:
    """Run the shards ``1/workers`` to ``workers/workers`` at the same time and merge them into ``record``.

    An interrupt stops the workers before any repository is removed: no case or fault starts after it, and each running commit ends first. The shutdown wait
    has a deadline, a repository whose worker is still running is kept, and a further interrupt waits until the merge has ended.
    """
    team = [Worker(index, workers, Record(feature=record.feature, slug=record.slug, rerun=record.rerun)) for index in range(1, workers + 1)]
    stop = threading.Event()
    threads = [threading.Thread(target=work, args=(worker, paths, fixtures, stop), name=f"guard_shard_{worker.index}", daemon=True) for worker in team]
    try:
        for thread in threads:
            thread.start()
        join_all(threads)
    finally:
        interrupts.defer()
        stop.set()
        join_all(threads, time.monotonic() + SHUTDOWN_SECONDS)
        keep_running_workers(team, threads, fixtures, kept, record, log)
        merge_workers(record, log, team)
    interrupts.raise_pending()


OBSERVATION_KEYS = {"case", "step", "expected", "observed", "passed"}


def record_problems(label: str, loaded: dict[str, Any]) -> list[str]:
    """Why a record cannot be compared: it did not pass, lacks a case or holds one twice, holds an observation that did not pass (the coverage observation of
    a parallel run is one), or names no digest of the copied files. A record that is not a list of observations is the only problem that is reported alone."""
    observations = loaded.get("observations")
    if not isinstance(observations, list) or not all(isinstance(item, dict) and item.keys() >= OBSERVATION_KEYS for item in observations):
        return [f"the {label} record holds no list of observations"]
    problems = [f"the {label} record's outcome is {loaded.get('outcome')!r}, not 'pass'"] if loaded.get("outcome") != "pass" else []
    missing, repeated = case_gaps(Counter((item["case"], item["step"]) for item in observations))
    problems += [f"{name} is missing from the {label} record" for name in missing]
    problems += [f"{name} is observed more than once in the {label} record" for name in repeated]
    problems += [f"{item['case']} ({item['step']}) did not pass in the {label} record" for item in observations if item["passed"] is not True]
    environment = loaded.get("environment")
    digest = environment.get(COPIED_DIGEST) if isinstance(environment, dict) else None
    if not isinstance(digest, str) or not digest:
        problems.append(f"the {label} record names no {COPIED_DIGEST}")
    return problems


def compare_records(first: dict[str, Any], second: dict[str, Any]) -> list[str]:
    """What stops two run records of the same code from agreeing: a record that is partial, interrupted, failed or without a digest of its copied files, and
    what differs: the code's revision and tree, the digest of the files that each run copied, and each case's expected text, observed text and result.

    Each worker of a parallel run has its own baseline, and only a parallel run has the coverage observation, so those are not compared.
    """
    problems = record_problems("first", first) + record_problems("second", second)
    if any("holds no list of observations" in problem for problem in problems):
        return problems
    problems += [f"{key} differs: {first.get(key)!r} and {second.get(key)!r}" for key in ("revision", "tree_sha256") if first.get(key) != second.get(key)]
    digests = [loaded["environment"].get(COPIED_DIGEST) if isinstance(loaded.get("environment"), dict) else None for loaded in (first, second)]
    if digests[0] != digests[1] and all(isinstance(digest, str) and digest for digest in digests):
        problems.append(f"{COPIED_DIGEST} differs: {digests[0]!r} and {digests[1]!r}")

    def keyed(loaded: dict[str, Any]) -> dict[tuple[str, str], list[dict[str, Any]]]:
        found: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for item in loaded["observations"]:
            if item["case"] not in (BASELINE, COVERAGE):
                found.setdefault((item["case"], item["step"]), []).append(item)
        return found

    one, two = keyed(first), keyed(second)
    for case, step in sorted(set(one) | set(two)):
        left, right = one.get((case, step), []), two.get((case, step), [])
        if len(left) != len(right):
            problems.append(f"{case} ({step}): {len(left)} observations in the first record, {len(right)} in the second")
            continue
        problems += [
            f"{case} ({step}): {name} differs: {a[name]!r} and {b[name]!r}"
            for a, b in zip(left, right, strict=True)
            for name in ("expected", "observed", "passed")
            if a[name] != b[name]
        ]
    return problems


def compare_command(first: str, second: str) -> int:
    """Print what differs between the records in two run folders and return 1, or print that they agree and return 0."""
    try:
        records = [json.loads((Path(folder) / "record.json").read_text()) for folder in (first, second)]
        if not all(isinstance(loaded, dict) for loaded in records):
            raise ValueError("a record is not an object")
    except (OSError, ValueError) as error:
        sys.stdout.write(f"cannot read a record: {type(error).__name__}\n")
        return 1
    problems = compare_records(*records)
    sys.stdout.write("\n".join(problems) + "\n" if problems else "the records agree on the code and on every case\n")
    return 1 if problems else 0


def parse_workers(text: str) -> int:
    if not re.fullmatch(r"\d+", text) or int(text) < 1:
        raise argparse.ArgumentTypeError("use a whole number of workers, 1 or more, for example 4")
    return int(text)


def worker_plan(requested: int, cores: int, available_mib: int | None, workers: int) -> str:
    memory = "not readable" if available_mib is None else f"{available_mib} MiB"
    return f"parallel run: {workers} of {requested} requested workers ({cores} cores, available memory {memory}, {WORKER_MIB} MiB estimated for each worker)"


def main(argv: Sequence[str] | None = None) -> int:
    """Run the planted-fault cases, write the run record and return 0 only when every assertion held."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    choice = parser.add_mutually_exclusive_group()
    choice.add_argument("--shard", type=parse_shard, default=None, metavar="INDEX/COUNT", help="run one share of the cases")
    choice.add_argument("--parallel", type=parse_workers, default=None, metavar="N", help="run N shards at the same time and write one record")
    choice.add_argument("--compare", nargs=2, default=None, metavar=("FIRST", "SECOND"), help="compare the records in two run folders")
    args = parser.parse_args(argv)
    if args.compare:
        return compare_command(*args.compare)
    index, count = args.shard or (1, 1)
    cores = os.cpu_count() or 1
    requested = args.parallel or 1
    record = Record(feature="commit guards, local commit path", slug="commit_guards", rerun=RERUN, mode="parallel not started" if requested > 1 else "full")
    record.limits = ["local commit path only: the hosted CI path has its own run", "the planted token and account reference are fake values built at run time"]
    log: list[str] = []
    fixtures: list[Fixture] = []
    kept: list[Fixture] = []
    workers = 1
    blocked = ""
    with ExitStack() as held:
        interrupts = held.enter_context(interrupt_control())
        try:
            if requested > 1:
                held.enter_context(machine_lock())
                available = available_memory_mib()
                workers = plan_workers(requested, len(planted_cases()), cores, available)
                record.limits.append(worker_plan(requested, cores, available, workers))
            record.mode = "full" if count == 1 and workers == 1 else f"shard {index} of {count}" if count > 1 else f"parallel {workers}"
            paths = source_files()
            before = tree_digest(paths)
            describe_run(record)
            record.environment[COPIED_DIGEST] = before
            if workers > 1:
                run_parallel(paths, record, log, fixtures, kept, workers, interrupts)
            else:
                fixtures.append(Fixture(record))
                fixtures[0].populate(paths)
                run_cases(fixtures[0], record, log, (index, count))
            unchanged = tree_digest(source_files()) == before
            record.observations.append(Observation("source tree", "snapshot", "unchanged during the run", f"unchanged: {unchanged}", unchanged))
        except KeyboardInterrupt:
            blocked = "interrupted"
            raise
        except MachineLockError as error:
            blocked = str(error)
            log.append(blocked)
        except Exception as error:
            log.append(f"{type(error).__name__}: {error}")
            record.expect("run", "protected execution", "every step completes", f"{type(error).__name__}; see log.txt", False)
        finally:
            interrupts.defer()
            if requested > 1:
                record_coverage(record)
            code = record.close_after(
                lambda: remove_fixtures(fixtures, record, kept), log, blocked=lambda: blocked or ("interrupted" if interrupts.seen else "")
            )
        interrupts.raise_pending()
    return code


if __name__ == "__main__":
    sys.exit(main())
