"""The CI plan: which job groups a pull request selects, which passed pull request run a push to main reuses, and what the run says.

Failure modes covered here, each with a planted case and a control:

- A tracked file is an input of no job and on no list, so a change to it is judged by a map that does not know it.
- A group's inputs miss a module that its commands start or import, or a path that the guard suite writes in its copy of the tree.
- The changed paths miss a file: the old name of a rename, a deleted file, or a change that the base has and the head lacks.
- A pull request skips a group although its range cannot be verified: no base, an unknown base, a checkout that is not its head, a base
  whose newest run on main did not pass, or an answer of the API that cannot be read or comes too late.
- A path on no list, or a change to the plan or the workflow, selects fewer than every group.
- A push to main reuses a run that is not the newest passed pull request run of the merged head with the pushed tree and every required
  job, or reuses anything when the checkout is not the pushed commit; a push without a pull request runs fewer than every group.
- A skipped or reused group is shown without its reason, or the record that the jobs read names the wrong jobs.
"""

from __future__ import annotations

import contextlib
import json
import platform
import re
import shutil
import sys
import tempfile
import threading
import time
import unittest
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, ClassVar
from urllib.parse import parse_qs, urlsplit

from scripts.process import run_command
from scripts.quality import ci_groups, ci_plan, ci_reuse
from scripts.quality.repo_checks import git
from tests.e2e import commit_guards
from tests.fast.git_fixture import ROOT, TIMEOUT_SECONDS, TemporaryRepository, fixture_environment

REPOSITORY = "owner/project"
REPOSITORY_ID = 7
ZERO = "0" * 40
WORKFLOW_TEXT = (ROOT / ci_reuse.WORKFLOW).read_text()
CONFIG = ci_groups.load(ROOT)


class FakeGitHub:
    """The answers of the GitHub API for workflow runs, their jobs, the pull requests of a commit and commit trees, with every path kept."""

    def __init__(self) -> None:
        self.runs: list[dict[str, Any]] = []
        self.jobs: dict[int, list[dict[str, Any]]] = {}
        self.pulls: dict[str, list[dict[str, Any]]] = {}
        self.trees: dict[str, str] = {}
        self.paths: list[str] = []
        self.broken = False
        self.short_job_pages: set[int] = set()

    def add_run(self, run_id: int, head: str, *, event: str = "push", branch: str = "main", conclusion: str | None = "success", **fields: Any) -> None:
        run = {
            "id": run_id,
            "run_number": run_id,
            "run_attempt": 1,
            "created_at": f"2026-11-21T10:{run_id % 60:02d}:00Z",
            "path": ci_reuse.WORKFLOW,
            "event": event,
            "head_branch": branch,
            "head_sha": head,
            "status": "completed",
            "conclusion": conclusion,
            "html_url": f"https://github.example.invalid/runs/{run_id}",
            "repository": {"id": REPOSITORY_ID},
            "head_repository": {"id": REPOSITORY_ID},
        }
        run.update(fields)
        self.runs.append(run)
        self.jobs.setdefault(run_id, [])

    def add_job(self, run_id: int, name: str, *, conclusion: str = "success", status: str = "completed", head: str | None = None) -> None:
        run = next(run for run in self.runs if run["id"] == run_id)
        self.jobs[run_id].append({"name": name, "status": status, "conclusion": conclusion, "head_sha": head or run["head_sha"]})

    def add_pull(self, commit: str, number: int, head: str, *, branch: str = "feature") -> None:
        pull = {
            "number": number,
            "merged_at": "2026-11-21T11:00:00Z",
            "merge_commit_sha": commit,
            "base": {"ref": "main"},
            "head": {"sha": head, "ref": branch, "repo": {"id": REPOSITORY_ID}},
        }
        self.pulls.setdefault(commit, []).append(pull)

    def __call__(self, path: str) -> Any:
        self.paths.append(path)
        if self.broken:
            raise ci_reuse.ApiError("the API answered 502")
        url = urlsplit(path)
        query = parse_qs(url.query)
        parts = url.path.split("/")
        if url.path == f"repos/{REPOSITORY}/actions/workflows/ci.yml/runs":
            runs = [run for run in self.runs if "head_sha" not in query or run["head_sha"] == query["head_sha"][0]]
            return {"total_count": len(runs), "workflow_runs": runs}
        if url.path.startswith(f"repos/{REPOSITORY}/actions/runs/") and parts[-1] == "jobs" and query.get("filter") == ["latest"]:
            jobs = self.jobs[int(parts[-2])]
            return {"total_count": len(jobs) + (1 if int(parts[-2]) in self.short_job_pages else 0), "jobs": jobs}
        if url.path.startswith(f"repos/{REPOSITORY}/commits/") and parts[-1] == "pulls":
            return self.pulls.get(parts[-2], [])
        if url.path.startswith(f"repos/{REPOSITORY}/git/commits/") and parts[-1] in self.trees:
            return {"sha": parts[-1], "tree": {"sha": self.trees[parts[-1]]}}
        raise ci_reuse.ApiError(f"GET {url.path} answered 404")


class PlanRepository(TemporaryRepository):
    """A repository that holds a copy of every tracked file of this checkout, so the plan derives each group's inputs from the real code.

    The class makes the copy once; each test starts again from its first commit, with an empty stand-in of the API.
    """

    template: ClassVar[tuple[Path, dict[str, str], str]]

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        home = tempfile.TemporaryDirectory(prefix="ci_plan_tree_")
        cls.addClassCleanup(home.cleanup)
        repo, env = Path(home.name) / "repo", fixture_environment(home.name)
        for path in git("ls-files", "-z", cwd=ROOT, keep_newlines=True).split("\0"):
            if path and (ROOT / path).is_file():
                (repo / path).parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT / path, repo / path)
        for arguments in (["init", "--quiet", "--initial-branch", "main"], ["add", "--all"], ["commit", "--quiet", "-m", "chore: copy the tree"]):
            run_command("git", arguments, cwd=repo, env=env, timeout=TIMEOUT_SECONDS, check=True)
        cls.template = (repo, env, git("rev-parse", "HEAD", cwd=repo).strip())

    def setUp(self) -> None:
        self.repo, self.env, self.base = self.template
        self.start_over()

    def start_over(self) -> None:
        self.git("reset", "--quiet", "--hard", self.base)
        self.git("clean", "--quiet", "-fdx")
        self.github = FakeGitHub()

    def commit(self, files: dict[str, str | None], message: str = "chore: change files") -> str:
        for path, content in files.items():
            target = self.repo / path
            if content is None:
                target.unlink()
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
        self.git("add", "--all", "--force", "--", *files)
        self.git("commit", "--quiet", "-m", message)
        return self.head()

    def head(self) -> str:
        return self.git("rev-parse", "HEAD").stdout.strip()

    def tree(self, commit: str = "HEAD") -> str:
        return self.git("rev-parse", f"{commit}^{{tree}}").stdout.strip()

    def passed(self, *commits: str) -> None:
        """A passed push run on main for each of ``commits``."""
        for number, commit in enumerate(commits, 1):
            self.github.add_run(number, commit)

    def environment(self, sha: str | None = None, ref: str = "refs/heads/main") -> dict[str, str]:
        return {"GITHUB_SHA": sha or self.head(), "GITHUB_REF": ref, "GITHUB_REPOSITORY": REPOSITORY, "GITHUB_REPOSITORY_ID": str(REPOSITORY_ID)}

    def plan(self, start: str, *, event: str = "pull_request", head: str | None = None) -> dict[str, Any]:
        """The plan of a pull request from ``start`` to ``head`` (the checkout by default), or of another event with that range."""
        return ci_plan.plan(self.repo, event, start, head or self.head(), self.environment(), self.github)

    def push(self, *, sha: str | None = None, head: str | None = None, ref: str = "refs/heads/main") -> dict[str, Any]:
        """The plan of a push of ``sha`` (the checkout by default) to ``ref``, as the workflow passes it."""
        sha = sha or self.head()
        return ci_plan.plan(self.repo, "push", "", head or sha, self.environment(sha, ref), self.github)

    def runs(self, record: dict[str, Any]) -> dict[str, bool]:
        return {group: decision["run"] for group, decision in record["groups"].items()}

    def shards(self, record: dict[str, Any]) -> list[int]:
        return [entry["shard"] for entry in record["groups"]["guards"]["matrix"]["include"] if entry["run"]]


ALL = {"hooks": True, "infra": True, "guards": True}
HOOKS_ONLY = {"hooks": True, "infra": False, "guards": False}
GUARDS = {"hooks": True, "infra": False, "guards": True}


class MapCompletenessTest(unittest.TestCase):
    inputs: ClassVar[dict[str, ci_groups.Inputs]]

    @classmethod
    def setUpClass(cls) -> None:
        cls.inputs = ci_groups.group_inputs(ROOT, CONFIG)

    def test_every_tracked_file_is_an_input_of_a_job_or_on_none(self) -> None:
        tracked = git("ls-files", "-z", cwd=ROOT, keep_newlines=True).split("\0")
        self.assertEqual(ci_groups.unmapped([path for path in tracked if path], CONFIG, self.inputs), [])

    def test_a_path_on_no_list_is_reported(self) -> None:
        self.assertEqual(ci_groups.unmapped(["planned/new.txt", "README.md"], CONFIG, self.inputs), ["planned/new.txt"])

    def test_every_pattern_states_its_reason(self) -> None:
        entries = [
            *CONFIG["plan"]["paths"],
            *CONFIG["none"],
            *CONFIG["loads_by_path"],
            *(entry for group in CONFIG["groups"].values() for entry in group.get("paths", [])),
        ]
        self.assertEqual([entry for entry in entries if not entry.get("reason", "").strip()], [])


class DerivedInputsTest(PlanRepository):
    """Each group runs for a change to a file its job imports, runs or writes, and for no other file."""

    def changed(self, path: str, content: str | None = None) -> dict[str, Any]:
        """Commit a change to ``path`` (a new line, or ``content``) on the first commit, whose run passed, and plan the pull request."""
        self.start_over()
        self.passed(self.base)
        target = self.repo / path
        self.commit({path: content if content is not None else (target.read_text() if target.is_file() else "") + "# changed\n"})
        return self.plan(self.base)

    def test_a_module_that_the_setup_imports_selects_the_infrastructure(self) -> None:
        for path in ("scripts/process.py", "scripts/quality/worktree_scan_hook.py", "scripts/quality/install_tools.py"):
            with self.subTest(path=path):
                record = self.changed(path)
                self.assertTrue(record["groups"]["infra"]["run"], record["groups"]["infra"]["reason"])
                self.assertIn(path, record["groups"]["infra"]["reason"])

    def test_a_path_that_the_guard_suite_writes_selects_the_guards(self) -> None:
        record = self.changed("notes/planted.txt")
        self.assertTrue(record["groups"]["guards"]["run"], record["groups"]["guards"]["reason"])
        self.assertIn("the guard suite writes notes/planted.txt", record["groups"]["guards"]["reason"])
        written = {commit_guards.TRACKED_NOTE, *commit_guards.LINKED_FOLDERS}
        written |= {path for case in commit_guards.planted_cases() for change in (case.control, case.fault) for path in (*change.files, *change.unstaged)}
        guards = ci_groups.group_inputs(self.repo, CONFIG)["guards"]
        self.assertEqual([path for path in sorted(written) if guards.reason(path) is None], [])

    def test_a_file_where_the_guard_suite_writes_a_folder_is_an_input_of_the_guards(self) -> None:
        guards = ci_groups.group_inputs(self.repo, CONFIG)["guards"]
        self.assertRegex(guards.reason("notes") or "", r"^the guard suite writes notes/\S+ in its copy of the tree$")
        self.assertEqual(guards.reason("scripts/planted/__init__.py"), "the guard suite writes scripts/planted.py in its copy of the tree")
        self.assertIsNone(guards.reason("notes.txt"))

    def test_a_module_that_a_command_string_names_is_an_input(self) -> None:
        self.passed(self.base)
        self.commit({"tests/fast/test_started.py": 'MODULE = "scripts.quality.only_started"\n', "scripts/quality/only_started.py": "VALUE = 1\n"})
        start = self.head()
        self.github.add_run(2, start)
        self.commit({"scripts/quality/only_started.py": "VALUE = 2\n"})
        record = self.plan(start)
        self.assertEqual(self.runs(record), GUARDS)
        self.assertIn("scripts/quality/only_started.py is an input", record["groups"]["guards"]["reason"])

    def test_a_module_that_the_fast_tests_import_selects_the_guards(self) -> None:
        for path in ("scripts/infra/host.py", "scripts/evidence/receipt.py", "infra/iam/scripts/render_policy.py"):
            with self.subTest(path=path):
                record = self.changed(path)
                self.assertTrue(record["groups"]["guards"]["run"], record["groups"]["guards"]["reason"])
                self.assertIn(path, record["groups"]["guards"]["reason"])

    def test_a_file_that_no_job_reads_selects_no_group(self) -> None:
        for path in ("README.md", "tests/e2e/ci_plan.py", ".github/workflows/deploy.yml"):
            with self.subTest(path=path):
                self.assertEqual(self.runs(self.changed(path)), HOOKS_ONLY)


class ImportMapTest(unittest.TestCase):
    """The derived inputs are complete: they hold every module that each job's entry points load, and every file that loads code by path."""

    inputs: ClassVar[dict[str, ci_groups.Inputs]]

    @classmethod
    def setUpClass(cls) -> None:
        cls.inputs = ci_groups.group_inputs(ROOT, CONFIG)

    def loaded(self, modules: list[str]) -> set[str]:
        code = (
            "import importlib, pathlib, sys\n"
            "root = pathlib.Path.cwd().resolve()\n"
            "for name in sys.argv[1:]:\n"
            "    importlib.import_module(name)\n"
            "for module in list(sys.modules.values()):\n"
            "    path = pathlib.Path(getattr(module, '__file__', None) or '/').resolve()\n"
            "    if path.is_relative_to(root) and '.venv' not in path.relative_to(root).parts:\n"
            "        print(path.relative_to(root).as_posix())\n"
        )
        result = run_command(sys.executable, ["-c", code, *modules], cwd=ROOT, timeout=TIMEOUT_SECONDS, check=True)
        return set(result.stdout.split())

    def test_the_derived_modules_hold_every_module_that_the_entry_points_load(self) -> None:
        for name, inputs in self.inputs.items():
            with self.subTest(group=name):
                self.assertTrue(inputs.entries, "every derived group has an entry module")
                self.assertEqual(self.loaded(inputs.entries) - set(inputs.modules), set())

    def test_the_entry_points_are_the_modules_that_the_commands_start(self) -> None:
        self.assertEqual(self.inputs["infra"].entries, ["scripts.quality.install_tools"])
        fast = sorted(f"tests.fast.{path.stem}" for path in (ROOT / "tests" / "fast").glob("test*.py"))
        self.assertEqual(
            self.inputs["guards"].entries, sorted({"scripts.quality.install_tools", "tests.e2e.commit_guards", "scripts.quality.repo_checks", *fast})
        )
        self.assertEqual(self.inputs["plan"].entries, ["scripts.quality.ci_plan"])

    def test_every_file_that_loads_code_by_path_is_declared(self) -> None:
        modules = sorted({path for inputs in self.inputs.values() for path in inputs.modules})
        declared = {entry["file"]: entry["loads"] for entry in CONFIG["loads_by_path"]}
        self.assertEqual(ci_groups.code_loaders(ROOT, modules), sorted(declared))
        for file, target in declared.items():
            groups = [name for name, inputs in self.inputs.items() if file in inputs.modules]
            self.assertTrue(groups)
            for name in groups:
                self.assertIsNotNone(self.inputs[name].reason(target), f"{name} imports {file}, which loads {target}")

    def test_a_hook_whose_entry_is_not_on_one_line_is_refused(self) -> None:
        with tempfile.TemporaryDirectory(prefix="ci_plan_hooks_") as folder:
            root = Path(folder)
            for path in (ci_groups.MAKEFILE, ci_groups.HOOKS):
                shutil.copy2(ROOT / path, root / path)
            hooks = (root / ci_groups.HOOKS).read_text()
            (root / ci_groups.HOOKS).write_text(hooks.replace("        entry: .venv/bin/mypy\n", "        entry: >\n          .venv/bin/mypy\n", 1))
            with self.assertRaisesRegex(ci_groups.Unverified, "has no entry on one line"):
                ci_groups.hook_modules(root)

    def test_a_command_that_names_no_make_target_is_refused(self) -> None:
        config = json.loads(json.dumps(CONFIG))
        config["groups"]["infra"]["commands"].append("make no-such-target")
        with self.assertRaisesRegex(ci_groups.Unverified, "no-such-target"):
            ci_groups.group_inputs(ROOT, config)


class PullRequestSelectionTest(PlanRepository):
    """A pull request runs the groups whose inputs its range changes, and every group when the range or its base cannot be trusted."""

    def test_a_change_to_a_guard_input_selects_the_guards(self) -> None:
        self.passed(self.base)
        self.commit({"tests/fast/test_new.py": "NEW = 1\n"})
        record = self.plan(self.base)
        self.assertEqual(self.runs(record), GUARDS)
        self.assertIn("tests/fast/test_new.py", record["groups"]["guards"]["reason"])

    def test_a_change_that_no_job_reads_skips_both_groups(self) -> None:
        self.passed(self.base)
        self.commit({"README.md": "changed\n"})
        record = self.plan(self.base)
        self.assertEqual(self.runs(record), HOOKS_ONLY)
        self.assertIn("none of the 1 changed paths", record["groups"]["guards"]["reason"])

    def test_a_change_to_the_terraform_code_selects_its_checks_alone(self) -> None:
        self.passed(self.base)
        self.commit({"infra/main.tf": "# changed\n"})
        self.assertEqual(self.runs(self.plan(self.base)), {"hooks": True, "infra": True, "guards": False})

    def test_a_rename_counts_its_old_name(self) -> None:
        self.passed(self.base)
        self.git("mv", "tests/fast/test_repo_checks.py", "tests/e2e/test_repo_checks.py")
        self.git("commit", "--quiet", "-m", "chore: move the test")
        record = self.plan(self.base)
        self.assertEqual(self.runs(record), GUARDS)
        self.assertIn("tests/fast/test_repo_checks.py", record["groups"]["guards"]["reason"])

    def test_a_rename_counts_its_new_name(self) -> None:
        self.passed(self.base)
        self.git("mv", "tests/e2e/ci_plan.py", "tests/fast/ci_plan.py")
        self.git("commit", "--quiet", "-m", "chore: move the acceptance test")
        self.assertEqual(self.runs(self.plan(self.base)), GUARDS)

    def test_a_rename_between_paths_that_no_job_reads_skips_the_guards(self) -> None:
        self.passed(self.base)
        self.git("mv", "tests/e2e/ci_plan.py", "tests/e2e/ci_plan_moved.py")
        self.git("commit", "--quiet", "-m", "chore: rename the acceptance test")
        self.assertEqual(self.runs(self.plan(self.base)), HOOKS_ONLY)

    def test_a_deleted_guard_input_selects_the_guards(self) -> None:
        self.passed(self.base)
        self.commit({"tests/fast/test_tool_boundaries.py": None})
        self.assertEqual(self.runs(self.plan(self.base)), GUARDS)

    def test_a_deleted_file_that_no_job_reads_skips_the_guards(self) -> None:
        self.passed(self.base)
        self.commit({"scripts/evidence/export.py": None})
        self.assertEqual(self.runs(self.plan(self.base)), HOOKS_ONLY)

    def test_a_change_that_the_base_has_and_the_head_lacks_counts(self) -> None:
        head = self.commit({"README.md": "head\n"})
        self.git("checkout", "--quiet", "--detach", self.base)
        newer_base = self.commit({"tests/fast/test_repo_checks.py": "CHANGED = 1\n"})
        self.passed(newer_base)
        self.git("checkout", "--quiet", head)
        record = self.plan(newer_base)
        self.assertEqual(self.runs(record), GUARDS)
        self.assertIn("tests/fast/test_repo_checks.py", record["groups"]["guards"]["reason"])

    def test_no_base_commit_selects_every_group(self) -> None:
        self.commit({"README.md": "changed\n"})
        for start in ("", ZERO):
            with self.subTest(start=start):
                record = self.plan(start)
                self.assertEqual(self.runs(record), ALL)
                self.assertIn("names no base commit", record["basis"])

    def test_a_base_that_is_not_in_the_checkout_selects_every_group(self) -> None:
        record = self.plan("1" * 40)
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("not in this checkout", record["basis"])

    def test_a_checkout_that_is_not_the_head_selects_every_group(self) -> None:
        self.passed(self.base)
        head = self.commit({"README.md": "changed\n"})
        self.git("checkout", "--quiet", "--detach", self.base)
        record = self.plan(self.base, head=head)
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("is not the commit of the run", record["basis"])

    def test_a_base_whose_newest_run_failed_selects_every_group(self) -> None:
        self.github.add_run(1, self.base)
        self.github.add_run(2, self.base, conclusion="failure")
        self.commit({"README.md": "changed\n"})
        record = self.plan(self.base)
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("the newest run 2 of the base commit", record["basis"])

    def test_a_base_whose_newest_attempt_failed_selects_every_group(self) -> None:
        self.github.add_run(1, self.base, run_attempt=2, conclusion="failure")
        self.commit({"README.md": "changed\n"})
        self.assertEqual(self.runs(self.plan(self.base)), ALL)

    def test_a_base_whose_newest_run_is_in_progress_selects_every_group(self) -> None:
        self.github.add_run(1, self.base)
        self.github.add_run(2, self.base, status="in_progress", conclusion=None)
        self.commit({"README.md": "changed\n"})
        self.assertEqual(self.runs(self.plan(self.base)), ALL)

    def test_a_base_with_no_trusted_run_on_main_selects_every_group(self) -> None:
        planted: dict[str, dict[str, Any]] = {
            "no run": {},
            "a pull request run of the base": {"event": "pull_request"},
            "a run on another branch": {"branch": "feature"},
            "a run of another repository": {"repository": {"id": REPOSITORY_ID + 1}},
            "a run of a fork": {"head_repository": {"id": REPOSITORY_ID + 1}},
            "a run of another workflow": {"path": ".github/workflows/deploy.yml"},
        }
        self.commit({"README.md": "changed\n"})
        for label, fields in planted.items():
            with self.subTest(label=label):
                self.github = FakeGitHub()
                if fields:
                    self.github.add_run(1, self.base, **fields)
                self.assertEqual(self.runs(self.plan(self.base)), ALL)
        self.github = FakeGitHub()
        self.passed(self.base)
        self.assertEqual(self.runs(self.plan(self.base)), HOOKS_ONLY)

    def test_an_api_error_selects_every_group(self) -> None:
        self.passed(self.base)
        self.github.broken = True
        self.commit({"README.md": "changed\n"})
        record = self.plan(self.base)
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("502", record["basis"])

    def test_an_api_past_its_deadline_selects_every_group(self) -> None:
        self.commit({"README.md": "changed\n"})
        api = ci_reuse.github_api(SlowApi(self, pause=0.2).url, "token", deadline_seconds=0.5)
        record = ci_plan.plan(self.repo, "pull_request", self.base, self.head(), self.environment(), api)
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("deadline of 0.5 seconds", record["basis"])

    def test_no_api_selects_every_group(self) -> None:
        self.commit({"README.md": "changed\n"})
        record = ci_plan.plan(self.repo, "pull_request", self.base, self.head(), self.environment(), None)
        self.assertEqual(self.runs(record), ALL)

    def test_a_path_on_no_list_selects_every_group(self) -> None:
        self.passed(self.base)
        self.commit({"planned/new.txt": "new\n"})
        record = self.plan(self.base)
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("planned/new.txt is on no list", record["basis"])

    def test_a_change_to_the_workflow_selects_every_group(self) -> None:
        self.passed(self.base)
        self.commit({ci_reuse.WORKFLOW: (self.repo / ci_reuse.WORKFLOW).read_text() + "# changed\n"})
        record = self.plan(self.base)
        self.assertEqual(self.runs(record), ALL)
        self.assertIn(ci_reuse.WORKFLOW, record["basis"])

    def test_another_event_selects_every_group(self) -> None:
        self.passed(self.base)
        self.commit({"README.md": "changed\n"})
        record = self.plan(self.base, event="workflow_dispatch")
        self.assertEqual(self.runs(record), ALL)
        self.assertNotIn("reused", record)


class PushReuseTest(PlanRepository):
    """A push to main reuses only the newest passed run of the merged pull request's head, and only for the same tree."""

    def setUp(self) -> None:
        super().setUp()
        self.passed(self.base)
        self.pull_head = self.commit({"tests/fast/test_new.py": "NEW = 1\n", "infra/main.tf": "# changed\n"})
        self.merge(self.pull_head)
        self.pull_run(60)

    def merge(self, head: str, number: int = 5) -> None:
        """A squash merge of ``head`` onto the first commit, checked out as main and kept as ``merged``, with its pull request and trees known to the API."""
        self.merged = self.git("commit-tree", self.tree(head), "-p", self.base, "-m", "feat: merge the pull request").stdout.strip()
        self.git("checkout", "--quiet", "--detach", self.merged)
        self.github.add_pull(self.merged, number, head)
        self.github.trees |= {self.merged: self.tree(self.merged), head: self.tree(head)}

    def pull_run(self, run_id: int, *, idle: tuple[str, ...] = (), skip: tuple[str, ...] = (), **fields: Any) -> list[str]:
        """A pull request run of the head with every required job passed: the plan, the hooks and each group, ``idle`` ones as not run."""
        self.github.add_run(run_id, self.pull_head, **({"event": "pull_request", "branch": "feature"} | fields))
        names = [CONFIG["plan"]["job"], CONFIG["groups"]["hooks"]["job"]]
        for group in ("infra", "guards"):
            names += [ci_groups.idle_name(CONFIG, group)] if group in idle else list(ci_groups.job_names(CONFIG, group).values())
        for name in names:
            if name not in skip:
                self.github.add_job(run_id, name)
        return names

    def test_a_merge_with_the_pull_requests_tree_reuses_its_passed_run(self) -> None:
        record = self.push()
        self.assertEqual(self.runs(record), HOOKS_ONLY)
        self.assertEqual(self.shards(record), [])
        self.assertEqual(record["reused"]["run_url"], "https://github.example.invalid/runs/60")
        self.assertEqual((record["reused"]["pull_request"], record["reused"]["head"], record["reused"]["tree"]), (5, self.pull_head, self.tree()))
        self.assertIn("reused: every required job passed on this tree in https://github.example.invalid/runs/60", record["groups"]["guards"]["reason"])
        self.assertEqual(record["groups"]["hooks"]["run"], True)

    def test_a_pull_request_run_that_skipped_a_group_is_reused(self) -> None:
        self.github = FakeGitHub()
        self.merge(self.pull_head)
        self.pull_run(61, idle=("infra",))
        self.assertEqual(self.runs(self.push()), HOOKS_ONLY)

    def test_a_merge_with_another_tree_runs_every_group(self) -> None:
        other = self.commit({"README.md": "a change that main had and the pull request lacked\n"})
        self.github.add_pull(other, 6, self.pull_head)
        self.github.trees[other] = self.tree(other)
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertEqual(self.shards(record), list(range(1, 9)))
        self.assertIn("differs from the tree", record["basis"])
        self.assertNotIn("reused", record)

    def test_a_push_without_a_pull_request_runs_every_group(self) -> None:
        self.commit({"README.md": "a direct push\n"})
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("no pull request was merged", record["basis"])

    def test_a_newer_failed_run_is_not_overridden_by_an_older_pass(self) -> None:
        self.github.add_run(62, self.pull_head, event="pull_request", branch="feature", conclusion="failure")
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("the newest run 62", record["basis"])

    def test_a_newer_failed_attempt_is_not_overridden_by_an_older_pass(self) -> None:
        self.github = FakeGitHub()
        self.merge(self.pull_head)
        third = ci_groups.job_names(CONFIG, "guards")[3]
        self.pull_run(63, skip=(third,), run_attempt=2, conclusion="failure")
        self.github.add_job(63, third, conclusion="failure")
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("concluded failure", record["basis"])

    def test_a_failed_job_of_the_latest_attempt_refuses_reuse(self) -> None:
        self.github = FakeGitHub()
        self.merge(self.pull_head)
        third = ci_groups.job_names(CONFIG, "guards")[3]
        self.pull_run(64, skip=(third,))
        self.github.add_job(64, third, conclusion="failure")
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("shard 3 of 8 of the run 64", record["basis"])

    def test_a_run_without_every_required_job_is_not_reused(self) -> None:
        guards = ci_groups.job_names(CONFIG, "guards")
        planted: dict[str, tuple[tuple[str, ...], list[tuple[str, dict[str, Any]]]]] = {
            "no hooks job": ((CONFIG["groups"]["hooks"]["job"],), []),
            "no plan job": ((CONFIG["plan"]["job"],), []),
            "seven of eight shards": ((guards[8],), []),
            "a group both run and not run": ((), [(ci_groups.idle_name(CONFIG, "guards"), {})]),
            "a job the map does not name": ((), [("Another job", {})]),
            "two jobs with one name": ((), [(guards[1], {})]),
            "a job of another commit": ((guards[2],), [(guards[2], {"head": "2" * 40})]),
            "a job still running": ((guards[2],), [(guards[2], {"status": "in_progress"})]),
        }
        for label, (skip, extra) in planted.items():
            with self.subTest(label=label):
                self.github = FakeGitHub()
                self.merge(self.pull_head)
                self.pull_run(70, skip=skip)
                for name, fields in extra:
                    self.github.add_job(70, name, **fields)
                self.assertEqual(self.runs(self.push()), ALL)
        self.github = FakeGitHub()
        self.merge(self.pull_head)
        self.pull_run(70)
        self.github.short_job_pages.add(70)
        self.assertIn("the answer holds", self.push()["basis"])

    def test_a_run_that_is_not_the_pull_requests_own_is_not_reused(self) -> None:
        planted: dict[str, dict[str, Any]] = {
            "a push run": {"event": "push"},
            "another head branch": {"head_branch": "other"},
            "another workflow": {"path": ".github/workflows/deploy.yml"},
            "a fork": {"head_repository": {"id": REPOSITORY_ID + 1}},
            "another repository": {"repository": {"id": REPOSITORY_ID + 1}},
            "a run still in progress": {"status": "in_progress", "conclusion": None},
        }
        for label, fields in planted.items():
            with self.subTest(label=label):
                self.github = FakeGitHub()
                self.merge(self.pull_head)
                self.pull_run(71, **fields)
                self.assertEqual(self.runs(self.push()), ALL)

    def test_a_pull_request_that_cannot_be_trusted_is_not_reused(self) -> None:
        planted: dict[str, dict[str, Any]] = {
            "from a fork": {"head": {"sha": self.pull_head, "ref": "feature", "repo": {"id": REPOSITORY_ID + 1}}},
            "from a deleted fork": {"head": {"sha": self.pull_head, "ref": "feature", "repo": None}},
            "into another branch": {"base": {"ref": "release"}},
            "not merged": {"merged_at": None},
            "merged as another commit": {"merge_commit_sha": "5" * 40},
        }
        for label, fields in planted.items():
            with self.subTest(label=label):
                self.github = FakeGitHub()
                self.merge(self.pull_head)
                self.pull_run(72)
                self.github.pulls[self.merged][0].update(fields)
                self.assertEqual(self.runs(self.push()), ALL)

    def test_two_merged_pull_requests_of_one_commit_refuse_reuse(self) -> None:
        self.github.add_pull(self.merged, 9, self.pull_head)
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("2 pull requests", record["basis"])

    def test_a_checkout_that_is_not_the_pushed_commit_reuses_nothing(self) -> None:
        pushed = self.commit({"tests/fast/test_other.py": "OTHER = 1\n"})
        self.github.add_pull(pushed, 7, self.pull_head)
        self.github.trees[pushed] = self.tree(pushed)
        self.git("checkout", "--quiet", "--detach", self.merged)
        record = self.push(sha=pushed)
        self.assertEqual(self.runs(record), ALL)
        self.assertEqual(self.shards(record), list(range(1, 9)))
        self.assertIn("is not the commit of the run", record["basis"])
        self.assertNotIn("reused", record)

    def test_a_head_that_is_not_the_pushed_commit_reuses_nothing(self) -> None:
        record = self.push(head=self.pull_head)
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("is not the pushed commit", record["basis"])

    def test_a_tree_from_the_api_that_differs_from_the_checkout_refuses_reuse(self) -> None:
        self.github.trees[self.merged] = "4" * 40
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("differs from the tree that the API gives", record["basis"])

    def test_a_push_to_another_branch_runs_every_group(self) -> None:
        record = self.push(ref="refs/heads/feature")
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("not to refs/heads/main", record["basis"])

    def test_an_api_error_during_reuse_runs_every_group(self) -> None:
        self.github.broken = True
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("502", record["basis"])

    def test_no_api_reuses_nothing(self) -> None:
        sha = self.head()
        record = ci_plan.plan(self.repo, "push", "", sha, self.environment(sha), None)
        self.assertEqual(self.runs(record), ALL)


class RecordAndSummaryTest(PlanRepository):
    def test_the_record_names_each_job_the_workflow_starts(self) -> None:
        self.passed(self.base)
        self.commit({"infra/main.tf": "# changed\n", "tests/fast/test_new.py": "NEW = 1\n"})
        record = self.plan(self.base)
        include = record["groups"]["guards"]["matrix"]["include"]
        self.assertEqual([entry["shard"] for entry in include], list(range(1, 9)))
        self.assertEqual(include[0], {"shard": 1, "shards": 8, "name": "Planted faults through the real commit path, shard 1 of 8", "run": True})
        self.assertEqual(record["groups"]["infra"]["matrix"]["include"], [{"name": "Static checks of the Terraform and Ansible code", "run": True}])

    def test_a_skipped_group_is_named_as_not_run_with_its_reason(self) -> None:
        self.passed(self.base)
        self.commit({"README.md": "changed\n"})
        record = self.plan(self.base)
        guards = record["groups"]["guards"]
        self.assertEqual(guards["matrix"]["include"], [{"shard": 0, "shards": 8, "name": "Planted faults through the real commit path: not run", "run": False}])
        self.assertEqual(record["groups"]["infra"]["matrix"]["include"], [{"name": "Static checks of the Terraform and Ansible code: not run", "run": False}])
        self.assertTrue(guards["reason"].startswith("skipped: "))

    def test_the_summary_gives_each_group_its_decision_and_reason(self) -> None:
        self.passed(self.base)
        self.commit({"tests/fast/test_new.py": "NEW = 1\n"})
        record = self.plan(self.base)
        text = ci_plan.summary(record)
        self.assertIn(record["basis"], text)
        for group, decision in record["groups"].items():
            self.assertIn(f"| {group} | {'runs' if decision['run'] else 'does not run'} | {decision['reason']} |", text)

    def test_the_summary_names_the_reused_run_and_its_jobs(self) -> None:
        record = {
            "basis": "push",
            "reused": {
                "pull_request": 5,
                "head": "h" * 40,
                "tree": "t" * 40,
                "run_id": 60,
                "run_url": "https://github.example.invalid/runs/60",
                "jobs": ["A job"],
            },
            "groups": {},
        }
        text = ci_plan.summary(record)
        self.assertIn(f"Reused run: https://github.example.invalid/runs/60 (id 60), head {'h' * 40}, tree {'t' * 40}.", text)
        self.assertIn("| A job | success |", text)

    def test_the_output_is_one_line_of_json(self) -> None:
        record = self.plan(ZERO)
        name, _, value = ci_plan.output_line(record).rstrip("\n").partition("=")
        self.assertEqual(name, "record")
        self.assertEqual(json.loads(value), record)
        self.assertNotIn("\n", value)


class SlowApi:
    """A stand-in for the API on the loopback interface that waits ``pause`` seconds before each byte of its answer."""

    def __init__(self, test: unittest.TestCase, pause: float, body: bytes = b'{"answer": 1}') -> None:
        self.requests = 0
        stand_in = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                stand_in.requests += 1
                with contextlib.suppress(OSError):
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    for byte in body:
                        time.sleep(pause)
                        self.wfile.write(bytes([byte]))
                        self.wfile.flush()

            def log_message(self, format: str, *args: Any) -> None:
                return

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        server.block_on_close = False
        threading.Thread(target=server.serve_forever, daemon=True).start()
        test.addCleanup(server.server_close)
        test.addCleanup(server.shutdown)
        self.url = f"http://127.0.0.1:{server.server_address[1]}"


class ApiDeadlineTest(unittest.TestCase):
    """The API phase has one deadline from the moment its reader is made, so neither a slow answer nor many requests pass it."""

    def test_an_answer_that_arrives_slowly_ends_at_the_deadline(self) -> None:
        api = ci_reuse.github_api(SlowApi(self, pause=0.2).url, "token", deadline_seconds=1.0)
        started = time.monotonic()
        with self.assertRaisesRegex(ci_reuse.ApiError, "deadline of 1 seconds"):
            api("repos/owner/project/actions/runs")
        self.assertLess(time.monotonic() - started, 2.5)

    def test_the_deadline_covers_every_request_together(self) -> None:
        slow = SlowApi(self, pause=0.02)
        api = ci_reuse.github_api(slow.url, "token", deadline_seconds=1.0)
        started = time.monotonic()
        with self.assertRaisesRegex(ci_reuse.ApiError, "deadline of 1 seconds"):
            for _ in range(20):
                self.assertEqual(api("repos/owner/project/actions/runs"), {"answer": 1})
        self.assertLess(time.monotonic() - started, 2.0)
        self.assertGreater(slow.requests, 1)

    def test_no_request_is_sent_after_the_deadline(self) -> None:
        slow = SlowApi(self, pause=0.0)
        api = ci_reuse.github_api(slow.url, "token", deadline_seconds=0.3)
        self.assertEqual(api("repos/owner/project/actions/runs"), {"answer": 1})
        time.sleep(0.4)
        with self.assertRaisesRegex(ci_reuse.ApiError, "not sent: the API reads passed their deadline of 0.3 seconds"):
            api("repos/owner/project/actions/runs")
        self.assertEqual(slow.requests, 1)

    def test_an_answer_within_the_deadline_is_read(self) -> None:
        api = ci_reuse.github_api(SlowApi(self, pause=0.0).url, "token", deadline_seconds=5.0)
        self.assertEqual(api("repos/owner/project/actions/runs"), {"answer": 1})


class WorkflowContractTest(unittest.TestCase):
    """The workflow's text is the contract: its jobs take their names and decisions from the plan's record, on the runner and Python of the map."""

    def test_every_job_uses_the_runner_and_python_of_the_map(self) -> None:
        self.assertEqual(set(re.findall(r"runs-on: (\S+)", WORKFLOW_TEXT)), {CONFIG["runner"]})
        self.assertEqual(set(re.findall(r'python-version: "([^"]+)"', WORKFLOW_TEXT)), {CONFIG["python"]})

    def test_python_is_one_patch_release_and_the_local_one(self) -> None:
        self.assertRegex(CONFIG["python"], r"^\d+\.\d+\.\d+$", "a minor version resolves to the runner's newest patch release")
        self.assertEqual(platform.python_version(), CONFIG["python"], "local checks run on the Python that the hosted jobs install")

    def test_trivy_uses_the_checks_built_into_its_pinned_binary(self) -> None:
        makefile = (ROOT / "Makefile").read_text()
        trivy = [line for line in makefile.splitlines() if ".tools/bin/trivy" in line]
        self.assertEqual(len(trivy), 1)
        self.assertIn("--skip-check-update", trivy[0], "without it Trivy downloads the newest checks bundle of a rolling tag")
        self.assertNotIn(".tools/trivy_cache ", trivy[0], "that cache can hold a bundle that an earlier run downloaded")

    def test_only_the_plan_job_may_read_more_than_the_contents(self) -> None:
        jobs = workflow_jobs(WORKFLOW_TEXT)
        self.assertEqual(jobs["plan"].blocks["permissions"], {"contents": "read", "actions": "read", "pull-requests": "read"})
        self.assertEqual([name for name, job in jobs.items() if "permissions" in job.blocks and name != "plan"], [])
        self.assertIn("permissions:\n  contents: read\n", WORKFLOW_TEXT.split("\njobs:\n")[0])
        self.assertNotRegex(WORKFLOW_TEXT, r":\s*write\b")

    def test_the_workflow_runs_every_required_step_of_each_job(self) -> None:
        self.assertEqual(contract_problems(WORKFLOW_TEXT, CONFIG), [])

    def test_a_disabled_or_missing_step_fails_the_contract(self) -> None:
        guards = '      - run: make test-guards SHARD="${SHARD}/${SHARDS}"\n        if: matrix.run\n'
        terraform = "      - run: make check-terraform\n        if: matrix.run\n"
        ansible = "      - run: make check-ansible\n        if: matrix.run\n"
        checkout = "        with:\n          ref: ${{ github.event.pull_request.head.sha || github.sha }}\n          persist-credentials: false\n"
        planted = {
            "the guard suite removed": (guards, ""),
            "the guard suite never run": (guards, guards.replace("if: matrix.run", "if: false")),
            "the Terraform checks removed": (terraform, ""),
            "the Ansible check behind another condition": (ansible, ansible.replace("if: matrix.run", "if: matrix.shard != 0")),
            "the Ansible check before the Terraform checks": (terraform + ansible, ansible + terraform),
            "a checkout of GitHub's merge commit": (checkout, "        with:\n          persist-credentials: false\n"),
            "the hooks job skipped": ("      - run: make check\n", "      - run: make check\n        if: matrix.run\n"),
            "the plan module replaced": ("python -m scripts.quality.ci_plan", "python -m scripts.quality.ci_groups"),
            "the Terraform setup behind another condition": (
                "        if: matrix.run\n        with:\n          terraform_version",
                "        if: always()\n        with:\n          terraform_version",
            ),
            "a skipped job that still checks out": (
                "      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n        if: matrix.run\n",
                "      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n",
            ),
            "the skip summary removed": ("printf 'Planted faults: %s\\n'", "true # "),
        }
        for label, (before, after) in planted.items():
            with self.subTest(label=label):
                self.assertIn(before, WORKFLOW_TEXT)
                self.assertNotEqual(contract_problems(WORKFLOW_TEXT.replace(before, after, 1), CONFIG), [])


@dataclass
class WorkflowJob:
    """A job of the workflow: its single-line keys, its key-value blocks and its steps."""

    keys: dict[str, str] = field(default_factory=dict)
    blocks: dict[str, dict[str, str]] = field(default_factory=dict)
    steps: list[dict[str, Any]] = field(default_factory=list)


STEP_KEYS = {"name", "id", "uses", "run", "if", "with", "env"}
CHECKOUT = "actions/checkout@"
HEAD_REF = "${{ github.event.pull_request.head.sha || github.sha }}"
STATES_REASON = re.compile(r"printf '[^']*%s\\n' \"\$\{GROUP_REASON\}\" >> \"\$\{GITHUB_STEP_SUMMARY\}\"")


def workflow_jobs(text: str) -> dict[str, WorkflowJob]:
    """The jobs of the workflow, read from the layout that it uses: two spaces per level, a step's keys four spaces past its dash."""
    jobs: dict[str, WorkflowJob] = {}
    job: WorkflowJob | None = None
    block: dict[str, str] | None = None
    step: dict[str, Any] | None = None
    target: dict[str, str] | None = None
    lines = text.split("\njobs:\n", 1)[1].splitlines()
    index = 0
    while index < len(lines):
        line = lines[index]
        index += 1
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        indent = len(line) - len(line.lstrip())
        key, _, value = line.strip().removeprefix("- ").partition(":")
        value = value.strip().split(" # ")[0]
        if indent == 2:
            job = jobs.setdefault(key, WorkflowJob())
        elif job is None:
            raise AssertionError(f"a line outside a job: {line!r}")
        elif indent == 4:
            block = None if value else job.blocks.setdefault(key, {})
            if value:
                job.keys[key] = value
        elif indent == 6 and line.strip().startswith("- "):
            step = {key: value}
            job.steps.append(step)
            target = None
        elif indent == 6 and block is not None:
            block[key] = value
        elif indent == 8 and step is not None:
            if key not in STEP_KEYS:
                raise AssertionError(f"a step key the contract does not know: {key}")
            if value == "|":
                body = []
                while index < len(lines) and (not lines[index].strip() or lines[index].startswith(" " * 10)):
                    body.append(lines[index].strip())
                    index += 1
                value = "\n".join(part for part in body if part)
            target = step.setdefault(key, {}) if not value else None
            if value:
                step[key] = value
        elif indent == 10 and target is not None:
            target[key] = value
        else:
            raise AssertionError(f"a line the contract cannot place: {line!r}")
    return jobs


def executed(job: WorkflowJob, entry: dict[str, Any], *, failed: bool = False) -> list[dict[str, Any]]:
    """The steps that GitHub runs for one matrix entry. A step behind a condition the contract does not know counts as not run."""
    conditions = {"matrix.run": bool(entry.get("run")), "failure() && matrix.run": failed and bool(entry.get("run"))}
    return [step for step in job.steps if "if" not in step or conditions.get(step["if"], False)]


def command_steps(steps: list[dict[str, Any]]) -> list[str]:
    return [step["run"] for step in steps if str(step.get("run", "")).startswith("make ")]


def setup_problems(where: str, steps: list[dict[str, Any]], config: dict[str, Any], first_command: int) -> list[str]:
    """A checkout of the head commit and the map's Python must come before the first command of a job."""
    before = steps[:first_command]
    problems = []
    if not any(str(step.get("uses", "")).startswith(CHECKOUT) and step.get("with", {}).get("ref") == HEAD_REF for step in before):
        problems.append(f"{where}: no checkout of the head commit before its first command")
    if not any(step.get("with", {}).get("python-version") == f'"{config["python"]}"' for step in before):
        problems.append(f"{where}: no setup of Python {config['python']} before its first command")
    return problems


def contract_problems(text: str, config: dict[str, Any]) -> list[str]:
    """What the workflow fails to do: run each job's commands when its record entry runs, run none when it does not, and plan first."""
    problems: list[str] = []
    try:
        jobs = workflow_jobs(text)
        for job in jobs.values():
            for step in job.steps:
                if str(step.get("uses", "")).startswith(CHECKOUT) and step.get("with", {}).get("ref") != HEAD_REF:
                    problems.append(f"a checkout of {step.get('with', {}).get('ref')} instead of the head commit")
        plan = jobs["plan"]
        plan_runs = [step["run"] for step in plan.steps if "run" in step]
        if plan_runs != [f'python -m {config["plan"]["modules"][0]} --event "${{EVENT_NAME}}" --start "${{RANGE_START}}" --head "${{RANGE_END}}"']:
            problems.append(f"the plan job runs {plan_runs}")
        if plan.blocks.get("outputs") != {"record": "${{ steps.plan.outputs.record }}"}:
            problems.append("the plan job does not give its record to the other jobs")
        hooks = jobs["hooks"]
        if any("if" in step for step in hooks.steps) or "needs" in hooks.keys:
            problems.append("the hooks job has a condition or waits for the plan")
        hook_steps = executed(hooks, {})
        hook_commands = command_steps(hook_steps)
        if hook_commands != ["make setup", "make check"] or "repo_checks commit-range" not in " ".join(str(step.get("run", "")) for step in hook_steps):
            problems.append(f"the hooks job runs {hook_commands} and the commit-message check of the range")
        else:
            problems += setup_problems("hooks", hook_steps, config, next(index for index, step in enumerate(hook_steps) if step.get("run") == "make setup"))
        for name in ci_groups.selectable(config):
            job = jobs[name]
            record = "${{ fromJSON(needs.plan.outputs.record).groups." + name
            if (
                job.keys.get("name") != "${{ matrix.name }}"
                or job.keys.get("needs") != "plan"
                or job.blocks.get("strategy", {}).get("matrix") != record + ".matrix }}"
            ):
                problems.append(f"{name}: its name, its wait for the plan or its matrix does not come from the record")
            for decision in (True, False):
                entries = ci_plan.group_entry(config, name, ci_groups.Decision(decision, "a reason"))["matrix"]["include"]
                for entry in entries:
                    steps = executed(job, entry)
                    commands = command_steps(steps)
                    wanted = config["groups"][name]["commands"] if decision else []
                    if len(commands) != len(wanted) or not all(
                        command.split(" ")[:2] == want.split(" ") for command, want in zip(commands, wanted, strict=False)
                    ):
                        problems.append(f"{name} {'runs' if decision else 'skips'}: it runs {commands}, not {wanted}")
                    elif decision:
                        problems += setup_problems(name, steps, config, steps.index(next(step for step in steps if step.get("run") == commands[0])))
                    stated = [
                        step
                        for step in steps
                        if STATES_REASON.fullmatch(str(step.get("run", ""))) and step.get("env", {}).get("GROUP_REASON") == record + ".reason }}"
                    ]
                    if not stated:
                        problems.append(f"{name} {'runs' if decision else 'skips'}: no step states the group's reason in the run summary")
                    if not decision and len(steps) != len(stated):
                        problems.append(f"{name} skips: it runs {len(steps) - len(stated)} steps besides stating its reason")
                    tail = [step for step in job.steps if step.get("if") == "failure() && matrix.run"]
                    if decision and len(steps) != len(job.steps) - len(tail):
                        problems.append(f"{name} runs: it leaves out {len(job.steps) - len(tail) - len(steps)} of its steps")
            sharded = [step for step in job.steps if str(step.get("run", "")).startswith("make test-guards")]
            if name == "guards" and (not sharded or sharded[0].get("env") != {"SHARD": "${{ matrix.shard }}", "SHARDS": "${{ matrix.shards }}"}):
                problems.append("guards: the suite does not run the shard of its matrix entry")
    except (AssertionError, KeyError, StopIteration, ValueError) as error:
        problems.append(f"the workflow cannot be read as the contract expects: {type(error).__name__}: {error}")
    return problems


if __name__ == "__main__":
    unittest.main()
