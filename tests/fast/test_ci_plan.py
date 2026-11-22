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

import json
import platform
import re
import unittest
from typing import Any
from urllib.parse import parse_qs, urlsplit

from scripts.quality import ci_groups, ci_plan, ci_reuse
from scripts.quality.repo_checks import git
from tests.fast.git_fixture import ROOT, TemporaryRepository

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
    """A repository with the real group map and stand-ins for the files that the reuse key reads."""

    def setUp(self) -> None:
        super().setUp()
        self.github = FakeGitHub()
        self.base = self.commit(
            {
                "scripts/quality/ci_groups.json": (ROOT / ci_groups.CONFIG).read_text(),
                ci_reuse.WORKFLOW: "name: ci\n",
                "requirements.txt": "ruff==1\n",
                "scripts/quality/install_tools.py": "VERSION = '1'\n",
                "scripts/infra/host.py": "HOST = 1\n",
                "scripts/quality/old_check.py": "CHECK = 1\n",
                "tests/fast/test_old.py": "OLD = 1\n",
                "README.md": "readme\n",
            }
        )

    def commit(self, files: dict[str, str | None], message: str = "chore: change files") -> str:
        for path, content in files.items():
            target = self.repo / path
            if content is None:
                target.unlink()
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
        self.git("add", "--all", "--", *files)
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
    def test_every_tracked_file_maps_to_a_group_or_to_none(self) -> None:
        tracked = git("ls-files", "-z", cwd=ROOT, keep_newlines=True).split("\0")
        self.assertEqual(ci_groups.unmapped([path for path in tracked if path], CONFIG), [])

    def test_a_path_on_no_list_is_reported(self) -> None:
        self.assertEqual(ci_groups.unmapped(["planned/new.txt", "README.md"], CONFIG), ["planned/new.txt"])

    def test_every_pattern_states_its_reason(self) -> None:
        entries = [*CONFIG["every_group"], *CONFIG["none"], *(entry for group in CONFIG["groups"].values() for entry in group["paths"])]
        self.assertEqual([entry["pattern"] for entry in entries if not entry.get("reason", "").strip()], [])


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
        self.git("mv", "scripts/quality/old_check.py", "scripts/infra/old_check.py")
        self.git("commit", "--quiet", "-m", "chore: move the check")
        record = self.plan(self.base)
        self.assertEqual(self.runs(record), GUARDS)
        self.assertIn("scripts/quality/old_check.py", record["groups"]["guards"]["reason"])

    def test_a_rename_counts_its_new_name(self) -> None:
        self.passed(self.base)
        self.git("mv", "scripts/infra/host.py", "scripts/quality/host.py")
        self.git("commit", "--quiet", "-m", "chore: move the host code")
        self.assertEqual(self.runs(self.plan(self.base)), GUARDS)

    def test_a_rename_between_unguarded_paths_skips_the_guards(self) -> None:
        self.passed(self.base)
        self.git("mv", "scripts/infra/host.py", "scripts/infra/hosts.py")
        self.git("commit", "--quiet", "-m", "chore: rename the host code")
        self.assertEqual(self.runs(self.plan(self.base)), HOOKS_ONLY)

    def test_a_deleted_guarded_file_selects_the_guards(self) -> None:
        self.passed(self.base)
        self.commit({"tests/fast/test_old.py": None})
        self.assertEqual(self.runs(self.plan(self.base)), GUARDS)

    def test_a_deleted_unguarded_file_skips_the_guards(self) -> None:
        self.passed(self.base)
        self.commit({"scripts/infra/host.py": None})
        self.assertEqual(self.runs(self.plan(self.base)), HOOKS_ONLY)

    def test_a_rollback_lists_the_files_it_restores(self) -> None:
        older = self.commit({"README.md": "one\n"})
        newer = self.commit({"tests/fast/test_old.py": "CHANGED = 1\n"})
        self.assertIn("tests/fast/test_old.py", ci_groups.changed_paths(self.repo, newer, older))

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
