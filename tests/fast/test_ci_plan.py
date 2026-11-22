"""The CI plan: which job groups a change selects, which passed groups an identical tree reuses, and what the run's summary says.

Failure modes covered here, each with a planted case and a control:

- A tracked file maps to no group, so a change to it is judged by a list that does not know it.
- The changed paths miss a file: the old name of a rename, a deleted file, or the files a rollback restores (a three-dot range drops them).
- A range the plan cannot verify selects fewer groups: a first push, an unknown commit, a force push, a checkout that is not the run's commit,
  a start commit whose own run did not pass or an answer of the API that cannot be read.
- A path on no list, or a change to the plan or the workflow, selects fewer than every group.
- The reuse key ignores one of its inputs, or a missing input still yields a key.
- A passed group is reused from a run that is not this repository's own push run on main with the same key, tree, runner and success.
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
RUN_ID = 900
ZERO = "0" * 40
WORKFLOW_TEXT = (ROOT / ci_reuse.WORKFLOW).read_text()
CONFIG = ci_groups.load(ROOT)
RUNNER = CONFIG["runner"]


class FakeGitHub:
    """The answers of the GitHub API for the workflow runs and their jobs, with every requested path kept."""

    def __init__(self) -> None:
        self.runs: list[dict[str, Any]] = []
        self.jobs: dict[int, list[dict[str, Any]]] = {}
        self.paths: list[str] = []
        self.broken = False
        self.short_job_pages: set[int] = set()

    def add_run(self, run_id: int, head: str, tree: str, *, conclusion: str = "success", **fields: Any) -> None:
        run = {
            "id": run_id,
            "run_number": run_id,
            "run_attempt": 1,
            "created_at": f"2026-11-21T10:{run_id % 60:02d}:00Z",
            "path": ci_reuse.WORKFLOW,
            "event": "push",
            "head_branch": "main",
            "head_sha": head,
            "head_commit": {"id": head, "tree_id": tree},
            "status": "completed",
            "conclusion": conclusion,
            "html_url": f"https://github.example.invalid/runs/{run_id}",
            "repository": {"id": REPOSITORY_ID},
            "head_repository": {"id": REPOSITORY_ID},
        }
        run.update(fields)
        self.runs.append(run)
        self.jobs.setdefault(run_id, [])

    def add_job(self, run_id: int, name: str, *, conclusion: str = "success", attempt: int = 1, label: str = RUNNER, head: str | None = None) -> None:
        run = next(run for run in self.runs if run["id"] == run_id)
        job = {"name": name, "status": "completed", "conclusion": conclusion, "run_attempt": attempt, "labels": [label], "head_sha": head or run["head_sha"]}
        self.jobs[run_id].append(job)

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
        if url.path.startswith(f"repos/{REPOSITORY}/actions/runs/") and parts[-1] == "jobs":
            jobs = self.jobs[int(parts[-2])]
            return {"total_count": len(jobs) + (1 if int(parts[-2]) in self.short_job_pages else 0), "jobs": jobs}
        raise ci_reuse.ApiError(f"no answer for {url.path}")


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

    def tree(self) -> str:
        return self.git("rev-parse", "HEAD^{tree}").stdout.strip()

    def passed(self, *commits: str) -> None:
        for number, commit in enumerate(commits, 1):
            self.github.add_run(number, commit, git("rev-parse", f"{commit}^{{tree}}", cwd=self.repo).strip())

    def environment(self, head: str | None = None) -> dict[str, str]:
        return {
            "GITHUB_SHA": head or self.head(),
            "GITHUB_REPOSITORY": REPOSITORY,
            "GITHUB_REPOSITORY_ID": str(REPOSITORY_ID),
            "GITHUB_RUN_ID": str(RUN_ID),
            "GITHUB_RUN_ATTEMPT": "1",
        }

    def plan(self, start: str, *, event: str = "push", head: str | None = None) -> dict[str, Any]:
        return ci_plan.plan(self.repo, event, start, self.environment(head), self.github)

    def runs(self, record: dict[str, Any]) -> dict[str, bool]:
        return {group: decision["run"] for group, decision in record["groups"].items()}


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
        return self.plan(self.base, event="pull_request")

    def test_a_module_that_the_setup_imports_selects_the_infrastructure(self) -> None:
        for path in ("scripts/process.py", "scripts/quality/worktree_scan_hook.py", "scripts/quality/install_tools.py"):
            with self.subTest(path=path):
                record = self.changed(path)
                self.assertTrue(record["groups"]["infra"]["run"], record["groups"]["infra"]["reason"])
                self.assertIn(path, record["groups"]["infra"]["reason"])

    def test_a_path_that_the_guard_suite_writes_selects_the_guards(self) -> None:
        record = self.changed("notes/planted.txt", "A planted note.\n")
        self.assertTrue(record["groups"]["guards"]["run"], record["groups"]["guards"]["reason"])
        self.assertIn("the guard suite writes notes/planted.txt", record["groups"]["guards"]["reason"])
        written = {commit_guards.TRACKED_NOTE, *commit_guards.LINKED_FOLDERS}
        written |= {path for case in commit_guards.planted_cases() for change in (case.control, case.fault) for path in (*change.files, *change.unstaged)}
        guards = ci_groups.group_inputs(self.repo, CONFIG)["guards"]
        self.assertEqual([path for path in sorted(written) if guards.reason(path) is None], [])

    def test_a_file_where_the_guard_suite_writes_a_folder_selects_the_guards(self) -> None:
        record = self.changed("notes", "a file where the suite makes its notes folder\n")
        self.assertTrue(record["groups"]["guards"]["run"], record["groups"]["guards"]["reason"])

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

    def test_a_command_that_names_no_make_target_is_refused(self) -> None:
        config = json.loads(json.dumps(CONFIG))
        config["groups"]["infra"]["commands"].append("make no-such-target")
        with self.assertRaisesRegex(ci_groups.Unverified, "no-such-target"):
            ci_groups.group_inputs(ROOT, config)


class SelectionTest(PlanRepository):
    def test_a_push_that_changes_a_guarded_path_selects_the_guards(self) -> None:
        self.passed(self.base)
        self.commit({"tests/fast/test_new.py": "NEW = 1\n"})
        record = self.plan(self.base)
        self.assertEqual(self.runs(record), GUARDS)
        self.assertIn("tests/fast/test_new.py", record["groups"]["guards"]["reason"])

    def test_a_push_that_changes_only_unguarded_paths_skips_both_groups(self) -> None:
        self.passed(self.base)
        self.commit({"README.md": "changed\n"})
        record = self.plan(self.base)
        self.assertEqual(self.runs(record), HOOKS_ONLY)
        self.assertIn("none of the 1 changed paths", record["groups"]["guards"]["reason"])

    def test_a_rename_counts_its_old_name(self) -> None:
        self.passed(self.base)
        self.git("mv", "tests/fast/test_repo_checks.py", "tests/e2e/test_repo_checks.py")
        self.git("commit", "--quiet", "-m", "chore: move the check")
        record = self.plan(self.base)
        self.assertEqual(self.runs(record), GUARDS)
        self.assertIn("tests/fast/test_repo_checks.py", record["groups"]["guards"]["reason"])

    def test_a_rename_counts_its_new_name(self) -> None:
        self.passed(self.base)
        self.git("mv", "tests/e2e/ci_plan.py", "tests/fast/ci_plan.py")
        self.git("commit", "--quiet", "-m", "chore: move the host code")
        self.assertEqual(self.runs(self.plan(self.base)), GUARDS)

    def test_a_rename_between_unguarded_paths_skips_the_guards(self) -> None:
        self.passed(self.base)
        self.git("mv", "tests/e2e/ci_plan.py", "tests/e2e/ci_plan_moved.py")
        self.git("commit", "--quiet", "-m", "chore: rename the host code")
        self.assertEqual(self.runs(self.plan(self.base)), HOOKS_ONLY)

    def test_a_deleted_guarded_file_selects_the_guards(self) -> None:
        self.passed(self.base)
        self.commit({"tests/fast/test_tool_boundaries.py": None})
        self.assertEqual(self.runs(self.plan(self.base)), GUARDS)

    def test_a_deleted_unguarded_file_skips_the_guards(self) -> None:
        self.passed(self.base)
        self.commit({"scripts/evidence/export.py": None})
        self.assertEqual(self.runs(self.plan(self.base)), HOOKS_ONLY)

    def test_a_rollback_lists_the_files_it_restores(self) -> None:
        older = self.commit({"README.md": "one\n"})
        newer = self.commit({"tests/fast/test_repo_checks.py": "CHANGED = 1\n"})
        self.assertIn("tests/fast/test_repo_checks.py", ci_groups.changed_paths(self.repo, newer, older))

    def test_a_first_push_selects_every_group(self) -> None:
        record = self.plan(ZERO)
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("no earlier commit", record["range"])

    def test_an_unknown_start_commit_selects_every_group(self) -> None:
        record = self.plan("1" * 40)
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("not in this checkout", record["range"])

    def test_a_force_push_selects_every_group(self) -> None:
        newer = self.commit({"README.md": "newer\n"})
        self.passed(self.base, newer)
        self.git("reset", "--quiet", "--hard", self.base)
        self.commit({"scripts/infra/host.py": "HOST = 2\n"})
        record = self.plan(newer)
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("not an ancestor", record["range"])

    def test_a_checkout_that_is_not_the_run_commit_selects_every_group(self) -> None:
        self.passed(self.base)
        self.commit({"README.md": "changed\n"})
        record = self.plan(self.base, head=self.base)
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("is not the commit of the run", record["range"])

    def test_a_start_commit_whose_run_failed_selects_every_group(self) -> None:
        self.github.add_run(1, self.base, "tree", conclusion="failure")
        self.commit({"README.md": "changed\n"})
        record = self.plan(self.base)
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("concluded failure", record["range"])

    def test_a_start_commit_with_no_run_selects_every_group(self) -> None:
        self.commit({"README.md": "changed\n"})
        record = self.plan(self.base)
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("no push run", record["range"])

    def test_a_start_run_of_another_repository_does_not_prove_the_start(self) -> None:
        self.github.add_run(1, self.base, "tree", head_repository={"id": REPOSITORY_ID + 1})
        self.commit({"README.md": "changed\n"})
        self.assertEqual(self.runs(self.plan(self.base)), ALL)

    def test_an_api_error_selects_every_group(self) -> None:
        self.passed(self.base)
        self.github.broken = True
        self.commit({"README.md": "changed\n"})
        record = self.plan(self.base)
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("502", record["range"])

    def test_an_api_past_its_deadline_selects_every_group(self) -> None:
        self.passed(self.base)
        self.commit({"README.md": "changed\n"})
        api = ci_reuse.github_api(SlowApi(self, pause=0.2).url, "token", deadline_seconds=0.5)
        record = ci_plan.plan(self.repo, "push", self.base, self.environment(), api)
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("deadline of 0.5 seconds", record["range"])

    def test_no_api_selects_every_group(self) -> None:
        self.commit({"README.md": "changed\n"})
        record = ci_plan.plan(self.repo, "push", self.base, self.environment(), None)
        self.assertEqual(self.runs(record), ALL)

    def test_a_path_on_no_list_selects_every_group(self) -> None:
        self.passed(self.base)
        self.commit({"planned/new.txt": "new\n"})
        record = self.plan(self.base)
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("planned/new.txt is on no list", record["range"])

    def test_a_change_to_the_workflow_selects_every_group(self) -> None:
        self.passed(self.base)
        self.commit({ci_reuse.WORKFLOW: "name: ci changed\n"})
        record = self.plan(self.base)
        self.assertEqual(self.runs(record), ALL)
        self.assertIn(ci_reuse.WORKFLOW, record["range"])

    def test_another_event_selects_every_group(self) -> None:
        self.passed(self.base)
        self.commit({"README.md": "changed\n"})
        self.assertEqual(self.runs(self.plan(self.base, event="workflow_dispatch")), ALL)

    def test_a_pull_request_compares_its_base_with_the_checkout(self) -> None:
        self.passed(self.base)
        self.commit({"infra/main.tf": "# changed\n"})
        self.assertEqual(self.runs(self.plan(self.base, event="pull_request")), {"hooks": True, "infra": True, "guards": False})

    def test_a_pull_request_that_changes_only_unguarded_paths_skips_both_groups(self) -> None:
        self.passed(self.base)
        self.commit({"README.md": "changed\n"})
        self.assertEqual(self.runs(self.plan(self.base, event="pull_request")), HOOKS_ONLY)


class ReuseKeyTest(PlanRepository):
    def key(self, config: dict[str, Any] | None = None, group: str = "guards") -> str:
        return ci_reuse.reuse_key(self.repo, config or ci_groups.load(self.repo), group)

    def test_the_key_is_the_same_for_the_same_inputs(self) -> None:
        self.assertEqual(self.key(), self.key())
        self.assertNotEqual(self.key(group="guards"), self.key(group="infra"))

    def test_each_file_input_changes_the_key(self) -> None:
        for path in (ci_reuse.WORKFLOW, "requirements.txt", "scripts/quality/install_tools.py", "README.md"):
            with self.subTest(path=path):
                before = self.key()
                self.commit({path: (self.repo / path).read_text() + "# changed\n"})
                self.assertNotEqual(self.key(), before)

    def test_each_file_input_is_a_part_of_the_key(self) -> None:
        parts = ci_reuse.key_parts(self.repo, ci_groups.load(self.repo), "guards")
        self.assertEqual(parts["tree"], self.tree())
        for name, path in (("workflow", ci_reuse.WORKFLOW), ("requirements", "requirements.txt"), ("tools", "scripts/quality/install_tools.py")):
            before = parts[name]
            self.commit({path: (self.repo / path).read_text() + "# changed\n"})
            self.assertNotEqual(ci_reuse.key_parts(self.repo, ci_groups.load(self.repo), "guards")[name], before, name)

    def test_the_runner_the_python_and_the_group_definition_change_the_key(self) -> None:
        before = self.key()
        for change in ({"runner": "ubuntu-26.04"}, {"python": "3.15"}):
            with self.subTest(change=change):
                self.assertNotEqual(self.key({**ci_groups.load(self.repo), **change}), before)
        config = ci_groups.load(self.repo)
        config["groups"]["guards"]["shards"] = 9
        self.assertNotEqual(self.key(config), before)

    def test_a_missing_input_refuses_a_key(self) -> None:
        for path in (ci_reuse.WORKFLOW, "requirements.txt", "scripts/quality/install_tools.py"):
            with self.subTest(path=path):
                kept = (self.repo / path).read_text()
                self.commit({path: None})
                with self.assertRaisesRegex(ci_reuse.NoReuse, re.escape(path)):
                    self.key()
                self.commit({path: kept})

    def test_an_unknown_group_or_runner_refuses_a_key(self) -> None:
        with self.assertRaises(ci_reuse.NoReuse):
            self.key(group="unknown")
        config = ci_groups.load(self.repo)
        del config["runner"]
        with self.assertRaises(ci_reuse.NoReuse):
            self.key(config)


class ReuseTest(PlanRepository):
    def setUp(self) -> None:
        super().setUp()
        self.passed(self.base)
        self.commit({"tests/fast/test_new.py": "NEW = 1\n"})
        self.key = ci_reuse.reuse_key(self.repo, ci_groups.load(self.repo), "guards")
        self.names = ci_reuse.job_names(ci_groups.load(self.repo), "guards", self.key)

    def earlier_run(self, run_id: int = 50, *, skip: tuple[int, ...] = (), **fields: Any) -> None:
        self.github.add_run(run_id, self.head(), self.tree(), **fields)
        for shard, name in self.names.items():
            if shard not in skip:
                self.github.add_job(run_id, name)

    def guards(self, record: dict[str, Any]) -> list[int]:
        return [entry["shard"] for entry in record["groups"]["guards"]["matrix"]["include"]]

    def test_an_earlier_passed_run_with_the_same_key_is_reused(self) -> None:
        self.earlier_run()
        record = self.plan(self.base)
        self.assertFalse(record["groups"]["guards"]["run"])
        self.assertEqual(self.guards(record), [0])
        self.assertIn("https://github.example.invalid/runs/50", record["groups"]["guards"]["reason"])

    def test_without_an_earlier_run_every_shard_runs(self) -> None:
        record = self.plan(self.base)
        self.assertEqual(self.guards(record), list(range(1, 9)))

    def test_shards_that_passed_in_two_runs_are_reused_together(self) -> None:
        self.earlier_run(50, skip=(8,))
        self.earlier_run(51, skip=(1, 2, 3, 4, 5, 6, 7))
        self.assertEqual(self.guards(self.plan(self.base)), [0])

    def test_only_the_shards_that_did_not_pass_run(self) -> None:
        self.earlier_run(50, skip=(3,))
        record = self.plan(self.base)
        self.assertEqual(self.guards(record), [3])
        self.assertIn("reused", record["groups"]["guards"]["reason"])

    def test_a_run_that_cannot_be_trusted_is_not_reused(self) -> None:
        planted: dict[str, dict[str, Any]] = {
            "a failed job": {"job": {"conclusion": "failure"}},
            "another runner": {"job": {"label": "ubuntu-22.04"}},
            "another commit in the job": {"job": {"head": "2" * 40}},
            "the current attempt": {"run": {"id": RUN_ID, "run_attempt": 1}},
            "another workflow": {"run": {"path": ".github/workflows/deploy.yml"}},
            "a pull request run": {"run": {"event": "pull_request"}},
            "another branch": {"run": {"head_branch": "feature"}},
            "a fork": {"run": {"head_repository": {"id": REPOSITORY_ID + 1}}},
            "another tree": {"run": {"head_commit": {"id": "x", "tree_id": "3" * 40}}},
            "another key": {"names": "other"},
        }
        for label, change in planted.items():
            with self.subTest(label=label):
                self.github = FakeGitHub()
                self.passed(self.base)
                run_id = change.get("run", {}).get("id", 50)
                self.github.add_run(run_id, self.head(), self.tree(), **{name: value for name, value in change.get("run", {}).items() if name != "id"})
                names = ci_reuse.job_names(ci_groups.load(self.repo), "guards", "f" * 64) if "names" in change else self.names
                for name in names.values():
                    self.github.add_job(run_id, name, **change.get("job", {}))
                self.assertEqual(self.guards(self.plan(self.base)), list(range(1, 9)))

    def test_an_earlier_attempt_of_the_same_run_is_reused(self) -> None:
        self.github.add_run(RUN_ID, self.head(), self.tree(), run_attempt=2)
        for name in self.names.values():
            self.github.add_job(RUN_ID, name, attempt=1)
        record = ci_plan.plan(self.repo, "push", self.base, {**self.environment(), "GITHUB_RUN_ATTEMPT": "2"}, self.github)
        self.assertEqual(self.guards(record), [0])

    def test_an_incomplete_job_list_is_not_reused(self) -> None:
        self.earlier_run()
        self.github.short_job_pages.add(50)
        self.assertEqual(self.guards(self.plan(self.base)), list(range(1, 9)))

    def test_an_api_error_during_reuse_runs_every_shard(self) -> None:
        self.earlier_run()
        answer = self.github

        def broken_after_the_start_check(path: str) -> Any:
            if "head_sha=" not in path:
                raise ci_reuse.ApiError("the API answered 502")
            return answer(path)

        record = ci_plan.plan(self.repo, "push", self.base, self.environment(), broken_after_the_start_check)
        self.assertEqual(self.guards(record), list(range(1, 9)))
        self.assertIn("502", record["groups"]["guards"]["reason"])

    def test_the_hooks_are_never_reused(self) -> None:
        self.earlier_run()
        config = ci_groups.load(self.repo)
        for key in (None, ci_reuse.reuse_key(self.repo, config, "hooks")):
            self.github.add_job(50, ci_reuse.job_names(config, "hooks", key)[1])
        hooks = self.plan(self.base)["groups"]["hooks"]
        self.assertEqual(hooks, {"run": True, "reason": CONFIG["groups"]["hooks"]["reason"], "name": CONFIG["groups"]["hooks"]["job"]})


class RecordAndSummaryTest(PlanRepository):
    def test_the_record_names_each_job_the_workflow_starts(self) -> None:
        self.passed(self.base)
        self.commit({"infra/main.tf": "# changed\n", "tests/fast/test_new.py": "NEW = 1\n"})
        record = self.plan(self.base)
        key = ci_reuse.reuse_key(self.repo, ci_groups.load(self.repo), "guards")
        include = record["groups"]["guards"]["matrix"]["include"]
        self.assertEqual([entry["shard"] for entry in include], list(range(1, 9)))
        self.assertEqual(include[0], {"shard": 1, "shards": 8, "name": f"Planted faults through the real commit path, shard 1 of 8 (key {key[:16]})"})
        self.assertTrue(record["groups"]["infra"]["name"].startswith("Static checks of the Terraform and Ansible code (key "))

    def test_a_skipped_group_is_named_as_not_run_with_its_reason(self) -> None:
        self.passed(self.base)
        self.commit({"README.md": "changed\n"})
        record = self.plan(self.base)
        guards = record["groups"]["guards"]
        self.assertEqual(guards["matrix"]["include"], [{"shard": 0, "shards": 8, "name": "Planted faults through the real commit path: not run"}])
        self.assertEqual(record["groups"]["infra"]["name"], "Static checks of the Terraform and Ansible code: not run")
        self.assertTrue(guards["reason"].startswith("skipped: "))

    def test_the_summary_gives_each_group_its_decision_and_reason(self) -> None:
        self.passed(self.base)
        self.commit({"tests/fast/test_new.py": "NEW = 1\n"})
        record = self.plan(self.base)
        text = ci_plan.summary(record)
        self.assertIn(record["range"], text)
        for group, decision in record["groups"].items():
            self.assertIn(f"| {group} | {'runs' if decision['run'] else 'does not run'} | {decision['reason']} |", text)

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

    def test_only_the_plan_job_may_read_the_actions(self) -> None:
        self.assertEqual(WORKFLOW_TEXT.count("actions: read"), 1)
        plan_job = WORKFLOW_TEXT.split("\n  plan:\n", 1)[1].split("\n\n", 1)[0]
        self.assertIn("actions: read", plan_job)
        self.assertNotRegex(WORKFLOW_TEXT, r":\s*write\b")

    def test_the_selectable_jobs_read_the_record(self) -> None:
        self.assertIn("name: ${{ fromJSON(needs.plan.outputs.record).groups.infra.name }}", WORKFLOW_TEXT)
        self.assertIn("matrix: ${{ fromJSON(needs.plan.outputs.record).groups.guards.matrix }}", WORKFLOW_TEXT)
        self.assertIn('make test-guards SHARD="${SHARD}/${SHARDS}"', WORKFLOW_TEXT)


if __name__ == "__main__":
    unittest.main()
