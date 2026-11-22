"""The CI plan: a pull request runs every group, a push to main reuses only a proven passed pull request run, and the workflow obeys the plan.

Failure modes covered here, each with a planted case and a control:

- A pull request skips a group, whatever its change: a selection by path cannot see every file a check reads.
- A push to main reuses a run that is not the newest passed pull request run of the merged head with the pushed tree and every job of
  the map, or reuses anything when the checkout is not the pushed commit; a push without a pull request runs fewer than every group.
- The record, the outputs or the summary name the wrong jobs or leave out the reason.
- The workflow disobeys the plan: a job behind a condition, a job that runs a command that its decision forbids or leaves out one that it
  needs, a shard that runs another shard's cases, or a required job that passes while another job failed, was cancelled or was skipped.
"""

from __future__ import annotations

import contextlib
import json
import platform
import re
import shutil
import sys
import threading
import time
import unittest
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

from scripts.process import run_command
from scripts.quality import ci_plan, ci_proof, ci_reuse
from tests.fast.git_fixture import ROOT, TIMEOUT_SECONDS, TemporaryRepository

REPOSITORY = "owner/project"
REPOSITORY_ID = 7
WORKFLOW_TEXT = (ROOT / ci_reuse.WORKFLOW).read_text()
CONFIG = ci_plan.load(ROOT)
EVERY_JOB = sorted(ci_plan.required_jobs(CONFIG))
GUARD_JOBS = ci_plan.job_names(CONFIG, "guards")
WORKFLOW_COMMIT = "9" * 40
WORKFLOW_ID = 425
ALL = {"hooks": True, "infra": True, "guards": True}
HOOKS_ONLY = {"hooks": True, "infra": False, "guards": False}


class FakeGitHub:
    """The answers of the GitHub API for workflow runs, their jobs and the annotations of each job, the pull requests of a commit, commit
    trees and the workflow file's blob at a commit, with every path kept."""

    def __init__(self) -> None:
        self.runs: list[dict[str, Any]] = []
        self.jobs: dict[int, list[dict[str, Any]]] = {}
        self.annotations: dict[int, list[dict[str, Any]]] = {}
        self.workflow_blobs: dict[str, str] = {}
        self.workflow_path = ci_reuse.WORKFLOW
        self.pulls: dict[str, list[dict[str, Any]]] = {}
        self.trees: dict[str, str] = {}
        self.paths: list[str] = []
        self.broken = False
        self.short_job_pages: set[int] = set()

    def add_run(
        self, run_id: int, head: str, *, event: str = "pull_request", branch: str = "feature", conclusion: str | None = "success", **fields: Any
    ) -> None:
        run = {
            "id": run_id,
            "run_number": run_id,
            "run_attempt": 1,
            "created_at": f"2026-11-21T10:{run_id % 60:02d}:00Z",
            "run_started_at": f"2026-11-21T10:{run_id % 60:02d}:00Z",
            "path": ci_reuse.WORKFLOW,
            "workflow_id": WORKFLOW_ID,
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

    def add_job(self, run_id: int, name: str, *, conclusion: str = "success", status: str = "completed", head: str | None = None) -> int:
        """Add a job to the run's latest attempt and return its ID, which is also the ID of its check run."""
        run = next(run for run in self.runs if run["id"] == run_id)
        job_id = run_id * 100 + len(self.jobs[run_id])
        job = {"id": job_id, "name": name, "status": status, "conclusion": conclusion, "head_sha": head or run["head_sha"], "run_attempt": run["run_attempt"]}
        job["check_run_url"] = f"https://api.example.invalid/repos/{REPOSITORY}/check-runs/{job_id}"
        self.jobs[run_id].append(job)
        self.annotations[job_id] = []
        return job_id

    def add_proof(self, job_id: int, proof: dict[str, Any] | str) -> None:
        message = proof if isinstance(proof, str) else json.dumps(proof)
        self.annotations[job_id].append({"title": ci_proof.TITLE, "annotation_level": "notice", "message": message})

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
        if url.path == f"repos/{REPOSITORY}/actions/workflows/ci.yml":
            return {"id": WORKFLOW_ID, "path": self.workflow_path}
        if url.path in (f"repos/{REPOSITORY}/actions/workflows/ci.yml/runs", f"repos/{REPOSITORY}/actions/workflows/{WORKFLOW_ID}/runs"):
            runs = [run for run in self.runs if "head_sha" not in query or run["head_sha"] == query["head_sha"][0]]
            return {"total_count": len(runs), "workflow_runs": runs}
        if url.path.startswith(f"repos/{REPOSITORY}/actions/runs/") and parts[-1] == "jobs" and query.get("filter") == ["latest"]:
            jobs = self.jobs[int(parts[-2])]
            return {"total_count": len(jobs) + (1 if int(parts[-2]) in self.short_job_pages else 0), "jobs": jobs}
        if url.path.startswith(f"repos/{REPOSITORY}/commits/") and parts[-1] == "pulls":
            return self.pulls.get(parts[-2], [])
        if url.path.startswith(f"repos/{REPOSITORY}/git/commits/") and parts[-1] in self.trees:
            return {"sha": parts[-1], "tree": {"sha": self.trees[parts[-1]]}}
        if url.path.startswith(f"repos/{REPOSITORY}/check-runs/") and parts[-1] == "annotations" and int(parts[-2]) in self.annotations:
            return self.annotations[int(parts[-2])]
        if url.path == f"repos/{REPOSITORY}/contents/{ci_reuse.WORKFLOW}" and query.get("ref", [""])[0] in self.workflow_blobs:
            return {"type": "file", "path": ci_reuse.WORKFLOW, "sha": self.workflow_blobs[query["ref"][0]]}
        raise ci_reuse.ApiError(f"GET {url.path} answered 404")


class PlanRepository(TemporaryRepository):
    """A repository with the map and the workflow of this checkout and one first commit, and an empty stand-in of the API."""

    def setUp(self) -> None:
        super().setUp()
        for path in (ci_plan.CONFIG.as_posix(), ci_reuse.WORKFLOW, "LICENSE", "Makefile"):
            (self.repo / path).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / path, self.repo / path)
        self.base = self.commit({"README.md": "first\n"}, "chore: add the tree")
        self.github = FakeGitHub()

    def commit(self, files: dict[str, str | None], message: str = "chore: change files") -> str:
        for path, content in files.items():
            target = self.repo / path
            if content is None:
                target.unlink()
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
        self.git("add", "--all", "--force")
        self.git("commit", "--quiet", "--allow-empty", "-m", message)
        return self.head()

    def head(self) -> str:
        return self.git("rev-parse", "HEAD").stdout.strip()

    def tree(self, commit: str = "HEAD") -> str:
        return self.git("rev-parse", f"{commit}^{{tree}}").stdout.strip()

    def environment(self, event: str, sha: str | None = None, ref: str = "refs/heads/main") -> dict[str, str]:
        return {
            "GITHUB_EVENT_NAME": event,
            "GITHUB_SHA": sha or self.head(),
            "GITHUB_REF": ref,
            "GITHUB_REPOSITORY": REPOSITORY,
            "GITHUB_REPOSITORY_ID": str(REPOSITORY_ID),
        }

    def pull_request(self) -> dict[str, Any]:
        """The plan of a pull request run whose checkout is the head commit."""
        return ci_plan.plan(self.repo, self.environment("pull_request", ref="refs/pull/5/merge"), self.github)

    def push(self, *, sha: str | None = None, ref: str = "refs/heads/main", event: str = "push") -> dict[str, Any]:
        """The plan of a push of ``sha`` (the checkout by default) to ``ref``."""
        return ci_plan.plan(self.repo, self.environment(event, sha, ref), self.github)

    def runs(self, record: dict[str, Any]) -> dict[str, bool]:
        return {group: decision["run"] for group, decision in record["groups"].items()}


class PullRequestTest(PlanRepository):
    """A pull request runs every group, whatever it changes, and reads nothing from the API."""

    def test_a_pull_request_runs_every_group_whatever_it_changes(self) -> None:
        changes: dict[str, Callable[[], object]] = {
            "a change that no check reads by name": lambda: self.commit({"README.md": "changed\n"}),
            "a link to a file that a check reads": self.link_licence,
            "a change to a Makefile prerequisite": lambda: self.commit({"Makefile": "setup: tools\ntools:\n\ttrue\n"}),
            "no change at all": lambda: self.commit({}),
        }
        for label, change in changes.items():
            with self.subTest(label=label):
                change()
                record = self.pull_request()
                self.assertEqual(self.runs(record), ALL)
                self.assertEqual((record["run"], record["reason"]), (True, ci_plan.EVERY_PULL_REQUEST))
                self.assertEqual(self.github.paths, [])

    def link_licence(self) -> None:
        (self.repo / "compose").mkdir(exist_ok=True)
        (self.repo / "compose/sample.csv").symlink_to("../LICENSE")
        self.commit({})


class PushReuseTest(PlanRepository):
    """A push to main reuses only the newest passed run of the merged pull request's head, and only for the same tree."""

    def setUp(self) -> None:
        super().setUp()
        self.pull_head = self.commit({"tests/fast/test_new.py": "NEW = 1\n", "infra/main.tf": "# changed\n"})
        self.merge(self.pull_head)
        self.pull_run(60)

    def workflow_blob(self) -> str:
        return self.git("rev-parse", f"HEAD:{ci_reuse.WORKFLOW}").stdout.strip()

    def merge(self, head: str, number: int = 5) -> None:
        """A squash merge of ``head`` onto the first commit, checked out as main and kept as ``merged``, with its pull request and trees known to the API."""
        self.merged = self.git("commit-tree", self.tree(head), "-p", self.base, "-m", "feat: merge the pull request").stdout.strip()
        self.git("checkout", "--quiet", "--detach", self.merged)
        self.github.add_pull(self.merged, number, head)
        self.github.trees |= {self.merged: self.tree(self.merged), head: self.tree(head)}
        self.github.workflow_blobs[WORKFLOW_COMMIT] = self.workflow_blob()

    def pull_run(self, run_id: int, *, skip: tuple[str, ...] = (), number: int = 5, base: str = "main", proven: bool = True, **fields: Any) -> dict[str, int]:
        """A pull request run of the head for pull request ``number`` into ``base`` with every job of the map passed, except the jobs named
        in ``skip``, and the proof of each job that runs checks. Returns the ID of each job by name."""
        self.github.add_run(run_id, self.pull_head, **fields)
        ids = {}
        for name in EVERY_JOB:
            if name not in skip:
                ids[name] = self.github.add_job(run_id, name)
                if proven and name != CONFIG["required"]["job"]:
                    self.github.add_proof(ids[name], self.proof(run_id, number=number, base=base, run_attempt=fields.get("run_attempt", 1)))
        return ids

    def proof(self, run_id: int, *, number: int = 5, base: str = "main", run_attempt: int = 1) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "run_attempt": run_attempt,
            "event": "pull_request",
            "pull_request": number,
            "base_ref": base,
            "head": self.pull_head,
            "workflow_sha": WORKFLOW_COMMIT,
        }

    def fresh(self) -> None:
        self.github = FakeGitHub()
        self.merge(self.pull_head)

    def test_a_merge_with_the_pull_requests_tree_reuses_its_passed_run(self) -> None:
        record = self.push()
        self.assertEqual(self.runs(record), HOOKS_ONLY)
        self.assertFalse(record["run"])
        self.assertEqual(record["reused"]["run_url"], "https://github.example.invalid/runs/60")
        self.assertEqual((record["reused"]["pull_request"], record["reused"]["head"], record["reused"]["tree"]), (5, self.pull_head, self.tree()))
        self.assertEqual(record["reused"]["jobs"], EVERY_JOB)
        self.assertIn("reused: every job passed on this tree in https://github.example.invalid/runs/60", record["groups"]["guards"]["reason"])

    def test_a_passed_run_of_another_pull_request_with_the_same_head_is_not_reused(self) -> None:
        self.fresh()
        self.pull_run(60, conclusion="failure")
        self.pull_run(61, number=6, base="release")
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("the newest run 60 of pull request 5", record["basis"])

    def test_the_newest_attempt_is_the_one_that_started_last(self) -> None:
        early, late = "2026-11-21T10:00:00Z", "2026-11-21T10:30:00Z"
        self.fresh()
        self.pull_run(60, conclusion="failure", run_attempt=2, run_started_at=late)
        self.pull_run(61, run_started_at=early)
        record = self.push()
        self.assertEqual(self.runs(record), ALL, "the run created last is not the newest attempt: run 60 was rerun after it")
        self.assertIn("the newest run 60 of pull request 5", record["basis"])
        self.fresh()
        self.pull_run(60, conclusion="failure", run_started_at=early)
        self.pull_run(61, run_started_at=late)
        self.assertEqual(self.push()["reused"]["run_id"], 61)

    def test_two_runs_that_started_together_refuse_reuse(self) -> None:
        self.fresh()
        self.pull_run(60, run_started_at="2026-11-21T10:30:00Z")
        self.pull_run(61, run_started_at="2026-11-21T10:30:00Z")
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("started together", record["basis"])

    def test_a_run_whose_workflow_is_not_the_pushed_commits_is_not_reused(self) -> None:
        self.github.workflow_blobs[WORKFLOW_COMMIT] = "8" * 40
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("ran the workflow file 8888", record["basis"])

    def test_a_run_of_the_head_that_proves_no_pull_request_refuses_reuse(self) -> None:
        self.pull_run(61, proven=False)
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("the run 61 of the head", record["basis"])

    def test_a_run_without_the_proof_of_every_job_is_not_reused(self) -> None:
        planted: dict[str, Callable[[dict[str, int]], None]] = {
            "a shard without a proof": lambda ids: self.github.annotations[ids[GUARD_JOBS[3]]].clear(),
            "a shard with two proofs": lambda ids: self.github.add_proof(ids[GUARD_JOBS[3]], self.proof(60)),
            "a proof that is not JSON": lambda ids: self.reprove(ids[GUARD_JOBS[3]], "not json"),
            "a proof of another run": lambda ids: self.reprove(ids[GUARD_JOBS[3]], self.proof(59)),
            "a proof of another attempt": lambda ids: self.reprove(ids[GUARD_JOBS[3]], self.proof(60, run_attempt=2)),
            "a proof of another pull request": lambda ids: self.reprove(ids[GUARD_JOBS[3]], self.proof(60, number=6)),
            "a proof of another base": lambda ids: self.reprove(ids[GUARD_JOBS[3]], self.proof(60, base="release")),
            "a proof of another head": lambda ids: self.reprove(ids[GUARD_JOBS[3]], self.proof(60) | {"head": "3" * 40}),
            "a proof of another event": lambda ids: self.reprove(ids[GUARD_JOBS[3]], self.proof(60) | {"event": "push"}),
            "a proof of another workflow revision": lambda ids: self.reprove(ids[GUARD_JOBS[3]], self.proof(60) | {"workflow_sha": "7" * 40}),
            "a proof with another title": lambda ids: self.github.annotations[ids[GUARD_JOBS[3]]][0].update(title="other"),
            "a warning, not a notice": lambda ids: self.github.annotations[ids[GUARD_JOBS[3]]][0].update(annotation_level="warning"),
            "the plan's proof missing": lambda ids: self.github.annotations[ids[CONFIG["plan"]["job"]]].clear(),
            "a check run of another repository": lambda ids: self.job(60, ids[GUARD_JOBS[3]]).update(
                check_run_url=f"https://api.example.invalid/repos/other/project/check-runs/{ids[GUARD_JOBS[3]]}"
            ),
            "the check run of another job": lambda ids: self.job(60, ids[GUARD_JOBS[3]]).update(
                check_run_url=f"https://api.example.invalid/repos/{REPOSITORY}/check-runs/{ids[GUARD_JOBS[4]]}"
            ),
        }
        for label, plant in planted.items():
            with self.subTest(label=label):
                self.fresh()
                plant(self.pull_run(60))
                self.assertEqual(self.runs(self.push()), ALL)

    def job(self, run_id: int, job_id: int) -> dict[str, Any]:
        return next(job for job in self.github.jobs[run_id] if job["id"] == job_id)

    def reprove(self, job_id: int, proof: dict[str, Any] | str) -> None:
        self.github.annotations[job_id].clear()
        self.github.add_proof(job_id, proof)

    def test_a_run_is_found_by_the_workflow_id_in_every_documented_path_form(self) -> None:
        for path in (ci_reuse.WORKFLOW, f"{ci_reuse.WORKFLOW}@main", f"{ci_reuse.WORKFLOW}@refs/pull/5/merge"):
            with self.subTest(path=path):
                self.fresh()
                self.pull_run(60, path=path)
                record = self.push()
                self.assertEqual(record.get("reused", {}).get("run_id"), 60, record["basis"])

    def test_a_run_of_another_workflow_is_not_reused(self) -> None:
        planted: dict[str, dict[str, Any]] = {
            "another workflow ID": {"workflow_id": WORKFLOW_ID + 1},
            "another workflow file with a ref": {"path": ".github/workflows/deploy.yml@main"},
            "a path that only starts like the workflow": {"path": f"{ci_reuse.WORKFLOW}.old"},
            "an empty ref": {"path": f"{ci_reuse.WORKFLOW}@"},
        }
        for label, fields in planted.items():
            with self.subTest(label=label):
                self.fresh()
                self.pull_run(60, **fields)
                self.assertEqual(self.runs(self.push()), ALL)
        self.fresh()
        self.github.workflow_path = ".github/workflows/deploy.yml"
        self.assertIn("is not the workflow", self.push()["basis"])

    def test_a_merge_with_another_tree_runs_every_group(self) -> None:
        other = self.commit({"README.md": "a change that main had and the pull request lacked\n"})
        self.github.add_pull(other, 6, self.pull_head)
        self.github.trees[other] = self.tree(other)
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("differs from the tree", record["basis"])
        self.assertNotIn("reused", record)

    def test_a_push_without_a_pull_request_runs_every_group(self) -> None:
        self.commit({"README.md": "a direct push\n"})
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("no pull request was merged", record["basis"])

    def test_a_newer_failed_run_is_not_overridden_by_an_older_pass(self) -> None:
        self.pull_run(62, conclusion="failure")
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("the newest run 62", record["basis"])

    def test_a_newer_failed_attempt_is_not_overridden_by_an_older_pass(self) -> None:
        self.fresh()
        self.pull_run(63, skip=(GUARD_JOBS[2],), run_attempt=2, conclusion="failure")
        self.github.add_job(63, GUARD_JOBS[2], conclusion="failure")
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("concluded failure", record["basis"])

    def test_a_failed_job_of_the_latest_attempt_refuses_reuse(self) -> None:
        self.fresh()
        self.pull_run(64, skip=(GUARD_JOBS[2],))
        self.github.add_job(64, GUARD_JOBS[2], conclusion="failure")
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("shard 3 of 8 of the run 64", record["basis"])

    def test_a_run_without_every_job_of_the_map_is_not_reused(self) -> None:
        planted: dict[str, tuple[tuple[str, ...], list[tuple[str, dict[str, Any]]]]] = {
            "no hooks job": ((CONFIG["groups"]["hooks"]["job"],), []),
            "no plan job": ((CONFIG["plan"]["job"],), []),
            "no required job": ((CONFIG["required"]["job"],), []),
            "no infrastructure job": ((CONFIG["groups"]["infra"]["job"],), []),
            "seven of eight shards": ((GUARD_JOBS[7],), []),
            "a job the map does not name": ((), [("Another job", {})]),
            "two jobs with one name": ((), [(GUARD_JOBS[0], {})]),
            "a job of another commit": ((GUARD_JOBS[1],), [(GUARD_JOBS[1], {"head": "2" * 40})]),
            "a job still running": ((GUARD_JOBS[1],), [(GUARD_JOBS[1], {"status": "in_progress"})]),
        }
        for label, (skip, extra) in planted.items():
            with self.subTest(label=label):
                self.fresh()
                self.pull_run(70, skip=skip)
                for name, fields in extra:
                    self.github.add_job(70, name, **fields)
                self.assertEqual(self.runs(self.push()), ALL)
        self.fresh()
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
                self.fresh()
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
                self.fresh()
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
        self.assertIn("is not the pushed commit", record["basis"])
        self.assertNotIn("reused", record)

    def test_a_tree_from_the_api_that_differs_from_the_checkout_refuses_reuse(self) -> None:
        self.github.trees[self.merged] = "4" * 40
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("differs from the tree that the API gives", record["basis"])

    def test_a_push_to_another_branch_runs_every_group(self) -> None:
        record = self.push(ref="refs/heads/feature")
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("not to refs/heads/main", record["basis"])

    def test_another_event_runs_every_group(self) -> None:
        record = self.push(event="workflow_dispatch")
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("the event workflow_dispatch reuses no run", record["basis"])

    def test_an_api_error_during_reuse_runs_every_group(self) -> None:
        self.github.broken = True
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("502", record["basis"])

    def test_no_api_reuses_nothing(self) -> None:
        record = ci_plan.plan(self.repo, self.environment("push"), None)
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("no GitHub API token", record["basis"])


class ProofTest(PlanRepository):
    """The proof command writes one notice annotation whose message, as the runner reads it, names the job's run, pull request and head."""

    def test_the_proof_of_a_pull_request_job_names_its_run(self) -> None:
        event = self.repo.parent / "event.json"
        event.write_text(json.dumps({"pull_request": {"number": 5, "base": {"ref": "main"}, "head": {"sha": self.head()}}}))
        environment = self.env | {
            "GITHUB_EVENT_NAME": "pull_request",
            "GITHUB_EVENT_PATH": str(event),
            "GITHUB_RUN_ID": "60",
            "GITHUB_RUN_ATTEMPT": "2",
            "GITHUB_WORKFLOW_SHA": WORKFLOW_COMMIT,
        }
        result = run_command(sys.executable, ["-m", "scripts.quality.ci_proof"], cwd=self.repo, env=environment, timeout=TIMEOUT_SECONDS, check=True)
        command, _, message = result.stdout.rstrip("\n").partition("::")[2].partition("::")
        self.assertEqual(command, f"notice title={ci_proof.TITLE}")
        self.assertEqual(result.stdout.count("\n"), 1)
        record = json.loads(message.replace("%0A", "\n").replace("%0D", "\r").replace("%25", "%"))
        expected = {
            "run_id": 60,
            "run_attempt": 2,
            "event": "pull_request",
            "pull_request": 5,
            "base_ref": "main",
            "head": self.head(),
            "workflow_sha": WORKFLOW_COMMIT,
        }
        self.assertEqual({key: record[key] for key in expected}, expected)

    def test_a_message_with_a_line_break_or_a_percent_sign_stays_on_one_line(self) -> None:
        line = ci_proof.annotation({"text": "50%\nnext\r"})
        self.assertEqual(line, f'::notice title={ci_proof.TITLE}::{{"text":"50%25\\nnext\\r"}}\n')


class RecordAndSummaryTest(PlanRepository):
    def test_the_record_names_each_job_of_each_group(self) -> None:
        record = self.pull_request()
        self.assertEqual(record["groups"]["guards"]["jobs"], [f"Planted faults through the real commit path, shard {shard} of 8" for shard in range(1, 9)])
        self.assertEqual(record["groups"]["infra"]["jobs"], ["Static checks of the Terraform and Ansible code"])

    def test_the_summary_gives_each_group_its_decision_and_reason(self) -> None:
        record = self.pull_request()
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

    def test_the_outputs_are_one_line_each(self) -> None:
        self.assertEqual(ci_plan.output_lines({"run": True, "reason": "runs: a | b\nc"}), "run=true\nreason=runs: a \\| b c\n")
        self.assertEqual(ci_plan.output_lines({"run": False, "reason": "reused"}), "run=false\nreason=reused\n")


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


def parse_workflow(text: str) -> dict[str, Any]:
    """The workflow as nested mappings and lists, read from the subset of YAML that it uses.

    Block mappings, block lists of mappings, ``|`` block scalars, flow lists of plain words and double-quoted words. A comment after `` # ``
    on a value line is dropped. A line the subset does not know raises ``AssertionError``.
    """
    lines = text.splitlines()
    index = 0

    def skip() -> None:
        nonlocal index
        while index < len(lines) and (not lines[index].strip() or lines[index].lstrip().startswith("#")):
            index += 1

    def indent_of(line: str) -> int:
        return len(line) - len(line.lstrip(" "))

    def scalar(value: str) -> Any:
        value = value.split(" # ")[0].strip()
        if value.startswith("[") and value.endswith("]"):
            return [part.strip() for part in value[1:-1].split(",") if part.strip()]
        if len(value) > 1 and value[0] == value[-1] == '"':
            return value[1:-1]
        return value

    def node(indent: int) -> Any:
        skip()
        if index >= len(lines) or indent_of(lines[index]) != indent:
            raise AssertionError(f"a block that does not start at indent {indent}: line {index + 1}")
        return sequence(indent) if lines[index].lstrip().startswith("- ") else mapping(indent)

    def value_of(value: str, indent: int) -> Any:
        nonlocal index
        if value == "|":
            body = []
            while index < len(lines) and (not lines[index].strip() or indent_of(lines[index]) > indent):
                body.append(lines[index].strip())
                index += 1
            return "\n".join(part for part in body if part)
        if value:
            return scalar(value)
        skip()
        return node(indent_of(lines[index])) if index < len(lines) and indent_of(lines[index]) > indent else None

    def mapping(indent: int) -> dict[str, Any]:
        nonlocal index
        result: dict[str, Any] = {}
        while True:
            skip()
            if index >= len(lines) or indent_of(lines[index]) < indent:
                return result
            line = lines[index]
            if indent_of(line) != indent or line.lstrip().startswith("- "):
                raise AssertionError(f"a line the workflow subset cannot place: {line!r}")
            key, colon, value = line.strip().partition(":")
            if not colon or key in result:
                raise AssertionError(f"not a new key: {line!r}")
            index += 1
            result[key] = value_of(value.strip(), indent)

    def sequence(indent: int) -> list[Any]:
        items: list[Any] = []
        while True:
            skip()
            if index >= len(lines) or indent_of(lines[index]) != indent or not lines[index].lstrip().startswith("- "):
                return items
            lines[index] = " " * (indent + 2) + lines[index].lstrip()[2:]
            items.append(mapping(indent + 2))

    return mapping(0)


ON_RUN = "needs.plan.outputs.run == 'true'"
AFTER_FAILURE = f"failure() && {ON_RUN}"
CHECKOUT = "actions/checkout@"
HEAD_REF = "${{ github.event.pull_request.head.sha || github.sha }}"
REASON = "${{ needs.plan.outputs.reason }}"
STATES_REASON = re.compile(r"printf '[^']*%s\\n' \"\$\{GROUP_REASON\}\" >> \"\$\{GITHUB_STEP_SUMMARY\}\"")
SHARD_COMMAND = 'make test-guards SHARD="${SHARD}/{shards}"'
PROOF = "python -m scripts.quality.ci_proof"
PROOF_STEP = f"      - run: {PROOF}\n        if: needs.plan.outputs.run == 'true'\n"


def executed(steps: list[dict[str, Any]], run: bool, *, failed: bool = False) -> list[dict[str, Any]]:
    """The steps that GitHub runs for the plan's decision ``run``. A step behind a condition the contract does not know counts as not run."""
    conditions = {ON_RUN: run, AFTER_FAILURE: failed and run}
    return [step for step in steps if "if" not in step or conditions.get(step["if"], False)]


def command_steps(steps: list[dict[str, Any]]) -> list[str]:
    return [step["run"] for step in steps if str(step.get("run", "")).startswith("make ")]


def setup_problems(where: str, steps: list[dict[str, Any]], config: dict[str, Any], first_command: int) -> list[str]:
    """A checkout of the head commit and the map's Python must come before the first command of a job."""
    before = steps[:first_command]
    problems = []
    if not any(str(step.get("uses", "")).startswith(CHECKOUT) and (step.get("with") or {}).get("ref") == HEAD_REF for step in before):
        problems.append(f"{where}: no checkout of the head commit before its first command")
    if not any((step.get("with") or {}).get("python-version") == config["python"] for step in before):
        problems.append(f"{where}: no setup of Python {config['python']} before its first command")
    return problems


def shard_arguments(job: dict[str, Any], config: dict[str, Any]) -> list[str]:
    """The ``SHARD`` argument that the guard suite gets in each matrix entry, with the entry's value put into the step's environment."""
    shards = config["groups"]["guards"]["shards"]
    arguments = []
    for value in job["strategy"]["matrix"]["shard"]:
        suite = [step for step in executed(job["steps"], True) if str(step.get("run", "")).startswith("make test-guards")]
        if len(suite) != 1 or suite[0]["run"] != SHARD_COMMAND.replace("{shards}", str(shards)):
            return [f"the suite runs {[step.get('run') for step in suite]}"]
        environment = {name: str(text).replace("${{ matrix.shard }}", value) for name, text in (suite[0].get("env") or {}).items()}
        arguments.append(f"{environment.get('SHARD')}/{shards}")
    return arguments


def group_problems(name: str, job: dict[str, Any], config: dict[str, Any]) -> list[str]:
    """A group job runs its commands, after its setup, when the plan's decision is to run, and states the reason and runs nothing else when not."""
    problems = []
    if job.get("needs") != "plan":
        problems.append(f"{name}: it does not wait for the plan")
    for run in (True, False):
        steps = executed(job["steps"], run)
        commands = command_steps(steps)
        wanted = config["groups"][name]["commands"] if run else []
        if len(commands) != len(wanted) or not all(command.split(" ")[:2] == want.split(" ") for command, want in zip(commands, wanted, strict=False)):
            problems.append(f"{name} {'runs' if run else 'skips'}: it runs {commands}, not {wanted}")
        elif run:
            problems += setup_problems(name, steps, config, steps.index(next(step for step in steps if step.get("run") == commands[0])))
        stated = [step for step in steps if STATES_REASON.fullmatch(str(step.get("run", ""))) and (step.get("env") or {}).get("GROUP_REASON") == REASON]
        if not stated:
            problems.append(f"{name} {'runs' if run else 'skips'}: no step states the plan's reason in the run summary")
        tail = [step for step in job["steps"] if step.get("if") == AFTER_FAILURE]
        if run and (not steps or steps[-1].get("run") != PROOF):
            problems.append(f"{name} runs: its last step does not write its proof")
        if run and len(steps) != len(job["steps"]) - len(tail):
            problems.append(f"{name} runs: it leaves out {len(job['steps']) - len(tail) - len(steps)} of its steps")
        if not run and len(steps) != len(stated):
            problems.append(f"{name} skips: it runs {len(steps) - len(stated)} steps besides stating its reason")
    return problems


def contract_problems(text: str, config: dict[str, Any]) -> list[str]:
    """What the workflow fails to do: run every job on each pull request, obey the plan on a push and require every job."""
    problems: list[str] = []
    try:
        workflow = parse_workflow(text)
        if workflow.get("on") != {"pull_request": None, "push": {"branches": ["main"]}}:
            problems.append(f"the workflow starts on {workflow.get('on')}")
        jobs: dict[str, Any] = workflow["jobs"]
        if set(jobs) != {"plan", "hooks", "infra", "guards", "required"}:
            problems.append(f"the workflow's jobs are {sorted(jobs)}")
        for name, job in jobs.items():
            if job.get("runs-on") != config["runner"]:
                problems.append(f"{name}: it runs on {job.get('runs-on')}")
            if "if" in job and name != "required":
                problems.append(f"{name}: it runs behind the condition {job['if']!r}")
            if "permissions" in job and name != "plan":
                problems.append(f"{name}: it asks for permissions {job['permissions']}")
            for step in job["steps"]:
                if str(step.get("uses", "")).startswith(CHECKOUT) and (step.get("with") or {}).get("ref") != HEAD_REF:
                    problems.append(f"{name}: a checkout of {(step.get('with') or {}).get('ref')} instead of the head commit")
                version = (step.get("with") or {}).get("python-version")
                if version is not None and version != config["python"]:
                    problems.append(f"{name}: Python {version}")
        if workflow.get("permissions") != {"contents": "read"} or jobs["plan"].get("permissions") != {
            "contents": "read",
            "actions": "read",
            "checks": "read",
            "pull-requests": "read",
        }:
            problems.append("the workflow or the plan job asks for more than reading")
        expected = {
            "plan": config["plan"]["job"],
            "hooks": config["groups"]["hooks"]["job"],
            "infra": config["groups"]["infra"]["job"],
            "required": config["required"]["job"],
        }
        expected["guards"] = config["groups"]["guards"]["job"].format(shard="${{ matrix.shard }}", shards=config["groups"]["guards"]["shards"])
        for name, title in expected.items():
            if jobs[name].get("name") != title:
                problems.append(f"{name}: it is named {jobs[name].get('name')!r}, not {title!r}")
        plan = jobs["plan"]
        if [step.get("run") for step in plan["steps"] if "run" in step] != ["python -m scripts.quality.ci_plan"] or not any(
            step.get("id") == "plan" for step in plan["steps"]
        ):
            problems.append("the plan job does not run the plan module as its step plan")
        if plan.get("outputs") != {"run": "${{ steps.plan.outputs.run }}", "reason": "${{ steps.plan.outputs.reason }}"}:
            problems.append("the plan job does not give its decision and reason to the group jobs")
        hooks = jobs["hooks"]
        hook_commands = command_steps(hooks["steps"])
        if any("if" in step for step in hooks["steps"]) or "needs" in hooks:
            problems.append("the hooks job has a condition or waits for the plan")
        elif hook_commands != ["make setup", "make check"] or "repo_checks commit-range" not in " ".join(str(step.get("run", "")) for step in hooks["steps"]):
            problems.append(f"the hooks job runs {hook_commands} and the commit-message check of the range")
        else:
            problems += setup_problems(
                "hooks", hooks["steps"], config, next(index for index, step in enumerate(hooks["steps"]) if step.get("run") == "make setup")
            )
        if hooks["steps"][-1].get("run") != PROOF:
            problems.append("the hooks job's last step does not write its proof")
        for name in ci_plan.selectable(config):
            problems += group_problems(name, jobs[name], config)
        guards = jobs["guards"]
        shards = config["groups"]["guards"]["shards"]
        if guards.get("strategy", {}).get("fail-fast") != "false":
            problems.append("guards: one failed shard cancels the others")
        if shard_arguments(guards, config) != [f"{shard}/{shards}" for shard in range(1, shards + 1)]:
            problems.append(f"guards: the matrix entries run the shards {shard_arguments(guards, config)}")
        required = jobs["required"]
        needs = required.get("needs") if isinstance(required.get("needs"), list) else [required.get("needs")]
        check = "\n".join(
            [
                'printf \'Results of the needed jobs: %s\\n\' "${RESULTS}" >> "${GITHUB_STEP_SUMMARY}"',
                f'test "${{RESULTS}}" = "{" ".join(["success"] * len(needs))}"',
            ]
        )
        if sorted(needs) != sorted(set(jobs) - {"required"}) or required.get("if") != "always()":
            problems.append(f"required: it waits for {needs} with the condition {required.get('if')!r}, not for every other job always")
        if required["steps"] != [{"name": "Check that every needed job succeeded", "env": {"RESULTS": "${{ join(needs.*.result, ' ') }}"}, "run": check}]:
            problems.append("required: its step does not fail unless every needed job succeeded")
    except (AssertionError, KeyError, StopIteration, ValueError, TypeError, AttributeError) as error:
        problems.append(f"the workflow cannot be read as the contract expects: {type(error).__name__}: {error}")
    return problems


class WorkflowContractTest(unittest.TestCase):
    """The workflow's text is the contract: its jobs obey the plan's decision, on the runner and Python of the map, and one job requires all."""

    def test_python_is_one_patch_release_and_the_local_one(self) -> None:
        self.assertRegex(CONFIG["python"], r"^\d+\.\d+\.\d+$", "a minor version resolves to the runner's newest patch release")
        self.assertEqual(platform.python_version(), CONFIG["python"], "local checks run on the Python that the hosted jobs install")

    def test_trivy_uses_the_checks_built_into_its_pinned_binary(self) -> None:
        makefile = (ROOT / "Makefile").read_text()
        trivy = [line for line in makefile.splitlines() if ".tools/bin/trivy" in line]
        self.assertEqual(len(trivy), 1)
        self.assertIn("--skip-check-update", trivy[0], "without it Trivy downloads the newest checks bundle of a rolling tag")
        self.assertNotIn(".tools/trivy_cache ", trivy[0], "that cache can hold a bundle that an earlier run downloaded")

    def test_the_workflow_keeps_the_contract(self) -> None:
        self.assertEqual(contract_problems(WORKFLOW_TEXT, CONFIG), [])

    def test_a_broken_workflow_fails_the_contract(self) -> None:
        guards = "      - run: make test-guards SHARD=\"${SHARD}/8\"\n        if: needs.plan.outputs.run == 'true'\n"
        terraform = "      - run: make check-terraform\n        if: needs.plan.outputs.run == 'true'\n"
        ansible = "      - run: make check-ansible\n        if: needs.plan.outputs.run == 'true'\n"
        checkout = "        with:\n          ref: ${{ github.event.pull_request.head.sha || github.sha }}\n          persist-credentials: false\n"
        guards_job = "    name: Planted faults through the real commit path, shard ${{ matrix.shard }} of 8\n    needs: plan\n"
        results = 'test "${RESULTS}" = "success success success success"'
        planted = {
            "the guards job never run": (guards_job, guards_job + "    if: false\n"),
            "the infrastructure job on pull requests only": (
                "    needs: plan\n    runs-on",
                "    needs: plan\n    if: github.event_name == 'push'\n    runs-on",
            ),
            "shard 1 in every matrix entry": ('SHARD="${SHARD}/8"', 'SHARD="1/8"'),
            "shard 1 in the step's environment": ("SHARD: ${{ matrix.shard }}", 'SHARD: "1"'),
            "one shard twice": ("shard: [1, 2, 3", "shard: [1, 1, 3"),
            "seven shards": ("shard: [1, 2, 3, 4, 5, 6, 7, 8]", "shard: [1, 2, 3, 4, 5, 6, 7]"),
            "one failed shard cancels the others": ("fail-fast: false", "fail-fast: true"),
            "the guard suite removed": (guards, ""),
            "the guard suite never run": (guards, guards.replace("if: needs.plan.outputs.run == 'true'", "if: false")),
            "the Terraform checks removed": (terraform, ""),
            "the Ansible check behind another condition": (ansible, ansible.replace("needs.plan.outputs.run == 'true'", "github.event_name == 'push'")),
            "the Ansible check before the Terraform checks": (terraform + ansible, ansible + terraform),
            "a checkout of GitHub's merge commit": (checkout, "        with:\n          persist-credentials: false\n"),
            "the hooks job skipped": ("      - run: make check\n", "      - run: make check\n        if: needs.plan.outputs.run == 'true'\n"),
            "the plan module replaced": ("run: python -m scripts.quality.ci_plan", "run: python -m scripts.quality.ci_reuse"),
            "a skipped job that still checks out": (
                "      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n        if: needs.plan.outputs.run == 'true'\n",
                "      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n",
            ),
            "the skip summary removed": ("printf 'Planted faults: %s\\n'", "true # "),
            "the required job does not wait for the guards": ("needs: [plan, hooks, infra, guards]", "needs: [plan, hooks, infra]"),
            "the required job skipped after a failure": ("    if: always()\n", ""),
            "the required job passes a failure": (results, results + " || true"),
            "the required job accepts a skipped job": (results, 'test "${RESULTS}" != "failure"'),
            "another Python in one job": ('python-version: "3.14.8"\n          cache', 'python-version: "3.14"\n          cache'),
            "a job asks to write": ("    timeout-minutes: 10\n    steps:", "    timeout-minutes: 10\n    permissions:\n      contents: write\n    steps:"),
            "pull requests into main only": ("  pull_request:\n", "  pull_request:\n    branches: [main]\n"),
            "a shard without its proof": ("          SHARD: ${{ matrix.shard }}\n" + PROOF_STEP, "          SHARD: ${{ matrix.shard }}\n"),
            "the proof before the checks": (terraform + ansible + PROOF_STEP, PROOF_STEP + terraform + ansible),
            "the hooks job without its proof": (
                'commit-range "${HEAD}"\n          fi\n      - run: python -m scripts.quality.ci_proof\n',
                'commit-range "${HEAD}"\n          fi\n',
            ),
            "the plan job cannot read the proofs": ("      actions: read\n      checks: read\n", "      actions: read\n"),
        }
        for label, (before, after) in planted.items():
            with self.subTest(label=label):
                self.assertIn(before, WORKFLOW_TEXT)
                self.assertNotEqual(contract_problems(WORKFLOW_TEXT.replace(before, after, 1), CONFIG), [])


if __name__ == "__main__":
    unittest.main()
