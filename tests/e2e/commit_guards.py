"""End-to-end check of the commit guards through real ``git commit`` calls.

Run from the repository root: ``.venv/bin/python -m tests.e2e.commit_guards``. ``--shard INDEX/COUNT`` runs one share
of the cases, for parallel jobs. The run copies every file Git does not ignore into a temporary repository, installs
the hooks there and makes a baseline commit. Each case then makes a passing control commit and a planted-fault commit.
The control must advance HEAD. The fault must be refused by its named guard alone and leave HEAD unchanged. Every run,
passing or failing, writes a record under ``runs/``. The source tree is hashed before and after the run, and a change
between the two fails the run.
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
from dataclasses import dataclass
from pathlib import Path

from scripts.process import run_command
from tests.e2e.run_record import Observation, Record

ROOT = Path(__file__).resolve().parents[2]
RERUN = ".venv/bin/python -m tests.e2e.commit_guards"
COMMAND_TIMEOUT_SECONDS = 300
LINKED_FOLDERS = (".venv", ".tools")
FAILED_HOOK = re.compile(r"\.Failed\n- hook id: (\S+)")
GOOD_MESSAGE = "docs: add a planted note"
CLEAN_PYTHON = "def double(value: int) -> int:\n    return value * 2\n"


@dataclass(frozen=True)
class Change:
    """One attempted commit: files to write, the ones to stage and the commit message."""

    files: dict[str, str]
    staged: tuple[str, ...]
    message: str = GOOD_MESSAGE


@dataclass(frozen=True)
class Case:
    name: str
    guard: str
    control: Change
    fault: Change


def planted_token() -> str:
    return "ghp_" + hashlib.sha256(b"planted").hexdigest()[:36]


def planted_account_reference() -> str:
    return "arn:aws:iam::" + "4" * 4 + "7" * 4 + "1" * 4 + ":role/planted"


def planted_cases() -> list[Case]:
    note = {"notes/planted.txt": "A planted note.\n"}
    staged_note = ("notes/planted.txt",)
    failing_test = "import unittest\n\n\nclass PlantedTest(unittest.TestCase):\n    def test_planted(self) -> None:\n        self.assertEqual(1, {value})\n"
    return [
        Case(
            "privacy marker in an unstaged file",
            "privacy-scan",
            Change({**note, "scratch/unstaged.txt": "role: arn:aws:iam::<ACCOUNT_ID>:role/planted\n"}, staged_note),
            Change({**note, "scratch/unstaged.txt": f"role: {planted_account_reference()}\n"}, staged_note),
        ),
        Case(
            "secret-like string in a staged file",
            "gitleaks",
            Change({"notes/token.txt": "token = ghp_<TOKEN>\n"}, ("notes/token.txt",)),
            Change({"notes/token.txt": f"token = {planted_token()}\n"}, ("notes/token.txt",)),
        ),
        Case(
            "badly formatted Python file",
            "ruff-format",
            Change({"scripts/planted.py": CLEAN_PYTHON}, ("scripts/planted.py",)),
            Change({"scripts/planted.py": CLEAN_PYTHON.replace("value * 2", "value*2")}, ("scripts/planted.py",)),
        ),
        Case(
            "unused import",
            "ruff-check",
            Change({"scripts/planted.py": CLEAN_PYTHON}, ("scripts/planted.py",)),
            Change({"scripts/planted.py": "import os\n\n\n" + CLEAN_PYTHON}, ("scripts/planted.py",)),
        ),
        Case(
            "untyped function signature",
            "mypy",
            Change({"scripts/planted.py": CLEAN_PYTHON}, ("scripts/planted.py",)),
            Change({"scripts/planted.py": "def double(value):\n    return value * 2\n"}, ("scripts/planted.py",)),
        ),
        Case(
            "credential file",
            "credential-files",
            Change({"notes/deploy.txt": "placeholder\n"}, ("notes/deploy.txt",)),
            Change({"notes/deploy.pem": "placeholder\n"}, ("notes/deploy.pem",)),
        ),
        Case(
            "data file outside the declared folders",
            "data-files",
            Change({"notes/sample.txt": "caseid,value\n"}, ("notes/sample.txt",)),
            Change({"notes/sample.csv": "caseid,value\n"}, ("notes/sample.csv",)),
        ),
        Case(
            "commit message without a type",
            "commit-message",
            Change(note, staged_note),
            Change(note, staged_note, "updated the planted note"),
        ),
        Case(
            "commit message with an assistant credit",
            "commit-message",
            Change(note, staged_note, GOOD_MESSAGE + "\n\nCo-authored-by: A Person <person@example.com>"),
            Change(note, staged_note, GOOD_MESSAGE + "\n\nCo-authored-by: Claude <assistant@example.com>"),
        ),
        Case(
            "failing fast test",
            "fast-tests",
            Change({"tests/fast/test_planted.py": failing_test.format(value=1)}, ("tests/fast/test_planted.py",)),
            Change({"tests/fast/test_planted.py": failing_test.format(value=2)}, ("tests/fast/test_planted.py",)),
        ),
    ]


def source_files() -> list[str]:
    listed = run_command("git", ["ls-files", "--cached", "--others", "--exclude-standard"], cwd=ROOT, timeout=COMMAND_TIMEOUT_SECONDS, check=True).stdout
    return sorted(path for path in set(listed.splitlines()) if (ROOT / path).is_file())


def tree_digest(paths: list[str]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.encode() + b"\0" + hashlib.sha256((ROOT / path).read_bytes()).digest())
    return digest.hexdigest()


def revision() -> str:
    head = run_command("git", ["rev-parse", "--verify", "--quiet", "HEAD"], cwd=ROOT, timeout=COMMAND_TIMEOUT_SECONDS)
    return head.stdout.strip() or "no commit"


def tool_version(program: str, *args: str) -> str:
    return run_command(str(ROOT / program), [*args, "--version"], cwd=ROOT, timeout=COMMAND_TIMEOUT_SECONDS, check=True).stdout.strip()


class Fixture:
    """A temporary repository holding a copy of the source tree, with the hooks installed."""

    def __init__(self, paths: list[str], record: Record) -> None:
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
        for path in paths:
            target = self.repo / path
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / path, target)
        for folder in LINKED_FOLDERS:
            (self.repo / folder).symlink_to(ROOT / folder, target_is_directory=True)

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
        self.must("git", "add", "--", *change.staged)
        code, output = self.run("git", "commit", "-m", change.message)
        return code, output, self.head() != before

    def reset(self, to: str) -> None:
        self.must("git", "reset", "--quiet", "--hard", to)
        self.must("git", "clean", "--quiet", "-fd")

    def remove(self) -> None:
        shutil.rmtree(self.home)


def run_case(fixture: Fixture, case: Case, log: list[str]) -> list[Observation]:
    start = fixture.head()
    code, output, advanced = fixture.attempt(case.control)
    log.append(f"== {case.name}: control\n{output}")
    control = Observation(case.name, "control", "exit 0 and HEAD advanced", f"exit {code}, HEAD advanced: {advanced}", code == 0 and advanced)
    fixture.reset(start)
    code, output, advanced = fixture.attempt(case.fault)
    log.append(f"== {case.name}: fault\n{output}")
    failed = sorted(set(FAILED_HOOK.findall(output)))
    expected = f"non-zero exit, only {case.guard} failed and HEAD unchanged"
    fault = Observation(
        case.name, "fault", expected, f"exit {code}, failed hooks: {failed}, HEAD advanced: {advanced}", code != 0 and failed == [case.guard] and not advanced
    )
    fixture.reset(start)
    return [control, fault]


def parse_shard(text: str) -> tuple[int, int]:
    match = re.fullmatch(r"(\d+)/(\d+)", text)
    if not match or not 1 <= int(match.group(1)) <= int(match.group(2)):
        raise argparse.ArgumentTypeError("use INDEX/COUNT with 1 <= INDEX <= COUNT, for example 1/2")
    return int(match.group(1)), int(match.group(2))


def main() -> int:
    """Run the planted-fault cases, write the run record and return 0 only when every assertion held."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--shard", type=parse_shard, default=(1, 1), metavar="INDEX/COUNT", help="run one share of the cases")
    index, count = parser.parse_args().shard
    paths = source_files()
    record = Record(feature="commit guards, local commit path", slug="commit_guards", rerun=RERUN, mode="full" if count == 1 else f"shard {index} of {count}")
    record.environment = {
        "platform": f"{platform.system()} {platform.machine()}",
        "python": platform.python_version(),
        "pre_commit": tool_version(".venv/bin/pre-commit"),
        "ruff": tool_version(".venv/bin/ruff"),
        "mypy": tool_version(".venv/bin/mypy"),
        "gitleaks": tool_version(".tools/bin/gitleaks"),
    }
    record.revision = revision()
    record.tree_sha256 = tree_digest(paths)
    record.limits = ["local commit path only: the hosted CI path has its own run", "the planted token and account reference are fake values built at run time"]
    log: list[str] = []
    fixture = Fixture(paths, record)
    try:
        fixture.must("git", "init", "--quiet", "--initial-branch", "main")
        fixture.must(".venv/bin/pre-commit", "install")
        fixture.must("git", "add", "--all")
        code, output = fixture.run("git", "commit", "-m", "chore: add the source tree")
        log.append(f"== baseline\n{output}")
        record.observations.append(Observation("baseline", "control", "exit 0", f"exit {code}", code == 0))
        if code == 0:
            for number, case in enumerate(planted_cases()):
                if number % count == index - 1:
                    record.observations.extend(run_case(fixture, case, log))
        unchanged = tree_digest(source_files()) == record.tree_sha256
        record.observations.append(Observation("source tree", "snapshot", "unchanged during the run", f"unchanged: {unchanged}", unchanged))
    except RuntimeError as error:
        log.append(str(error))
        record.expect("fixture", "setup", "every fixture command succeeds", "a fixture command failed; see log.txt", False)
    finally:
        fixture.remove()
        record.cleanup = "temporary repository removed"
    return record.close(log)


if __name__ == "__main__":
    sys.exit(main())
