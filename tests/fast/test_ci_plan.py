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
import functools
import json
import os
import platform
import re
import shutil
import sys
import threading
import time
import unittest
from collections import Counter
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from unittest import mock
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
WORKFLOW_ID = 425
PUSH_RUN = 900
RUNNER_IMAGE = {"ImageOS": "ubuntu24", "ImageVersion": "20261116.1.0"}
HERE = ci_proof.runner(RUNNER_IMAGE)
ALL = {"hooks": True, "infra": True, "guards": True}
HOOKS_ONLY = {"hooks": True, "infra": False, "guards": False}


class FakeGitHub:
    """The answers of the GitHub API for workflow runs, their jobs per attempt and the annotations of each job, the pull requests of a
    commit, commit trees and the comparison of two commits, with every path kept. It cuts each list at ``page_size`` items, as GitHub
    cuts it at the ``per_page`` that the caller asks for."""

    def __init__(self) -> None:
        self.runs: list[dict[str, Any]] = []
        self.jobs: dict[int, list[dict[str, Any]]] = {}
        self.annotations: dict[int, list[dict[str, Any]]] = {}
        self.pulls: dict[str, list[dict[str, Any]]] = {}
        self.trees: dict[str, str] = {}
        self.compares: dict[tuple[str, str], tuple[str, str]] = {}
        self.workflow_path = ci_reuse.WORKFLOW
        self.paths: list[str] = []
        self.broken = False
        self.page_size = 100
        self.missing_runs = 0
        self.missing_jobs: set[int] = set()
        self.ignore_head_filter = False

    def add_run(
        self,
        run_id: int,
        head: str,
        *,
        event: str = "pull_request",
        branch: str = "feature",
        conclusion: str | None = "success",
        number: int = 5,
        **fields: Any,
    ) -> None:
        ref = f"refs/pull/{number}/merge" if event == "pull_request" else "refs/heads/main"
        run = {
            "id": run_id,
            "run_number": run_id,
            "run_attempt": 1,
            "created_at": f"2026-11-21T10:{run_id % 60:02d}:00Z",
            "run_started_at": f"2026-11-21T10:{run_id % 60:02d}:00Z",
            "path": f"{ci_reuse.WORKFLOW}@{ref}",
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

    def add_job(
        self, run_id: int, name: str, *, conclusion: str = "success", status: str = "completed", head: str | None = None, attempt: int | None = None
    ) -> int:
        """Add a job to the run, as listed for the run's latest attempt unless ``attempt`` names the attempt that the job ran in, and return
        its ID, which is also the ID of its check run."""
        run = next(run for run in self.runs if run["id"] == run_id)
        job_id = run_id * 100 + len(self.jobs[run_id])
        job = {"id": job_id, "name": name, "status": status, "conclusion": conclusion, "head_sha": head or run["head_sha"]}
        job["run_attempt"] = attempt or run["run_attempt"]
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

    def page_of(self, items: list[Any], query: dict[str, list[str]]) -> list[Any]:
        size = min(int(query.get("per_page", ["30"])[0]), self.page_size)
        first = (int(query.get("page", ["1"])[0]) - 1) * size
        return items[first : first + size]

    def __call__(self, path: str) -> Any:
        self.paths.append(path)
        if self.broken:
            raise ci_reuse.ApiError("the API answered 502")
        url = urlsplit(path)
        query = parse_qs(url.query)
        parts = url.path.split("/")
        if url.path == f"repos/{REPOSITORY}/actions/workflows/ci.yml":
            return {"id": WORKFLOW_ID, "path": self.workflow_path}
        if url.path == f"repos/{REPOSITORY}/actions/workflows/{WORKFLOW_ID}/runs":
            runs = [run for run in self.runs if self.ignore_head_filter or "head_sha" not in query or run["head_sha"] == query["head_sha"][0]]
            return {"total_count": len(runs) + self.missing_runs, "workflow_runs": self.page_of(runs, query)}
        if url.path.startswith(f"repos/{REPOSITORY}/actions/runs/") and parts[-1] == "jobs" and parts[-3] == "attempts":
            run = next(run for run in self.runs if run["id"] == int(parts[-4]))
            if int(parts[-2]) != run["run_attempt"]:
                raise ci_reuse.ApiError(f"GET {url.path} answered 404: the stand-in holds only the latest attempt")
            jobs = self.jobs[run["id"]]
            return {"total_count": len(jobs) + (1 if run["id"] in self.missing_jobs else 0), "jobs": self.page_of(jobs, query)}
        if url.path.startswith(f"repos/{REPOSITORY}/actions/runs/") and parts[-1].isdigit():
            return next(run for run in self.runs if run["id"] == int(parts[-1]))
        if url.path.startswith(f"repos/{REPOSITORY}/commits/") and parts[-1] == "pulls":
            return self.page_of(self.pulls.get(parts[-2], []), query)
        if url.path.startswith(f"repos/{REPOSITORY}/git/commits/") and parts[-1] in self.trees:
            return {"sha": parts[-1], "tree": {"sha": self.trees[parts[-1]]}}
        if url.path.startswith(f"repos/{REPOSITORY}/check-runs/") and parts[-1] == "annotations" and int(parts[-2]) in self.annotations:
            return self.page_of(self.annotations[int(parts[-2])], query)
        if url.path.startswith(f"repos/{REPOSITORY}/compare/"):
            ancestor, _, commit = parts[-1].partition("...")
            if (ancestor, commit) in self.compares:
                status, base = self.compares[(ancestor, commit)]
                return {"status": status, "merge_base_commit": {"sha": base}}
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
            **RUNNER_IMAGE,
        }

    def pull_request(self) -> dict[str, Any]:
        """The plan of a pull request run whose checkout is the head commit."""
        return ci_plan.plan(self.repo, self.environment("pull_request", ref="refs/pull/5/merge"), self.github)

    def push(
        self,
        *,
        sha: str | None = None,
        ref: str = "refs/heads/main",
        event: str = "push",
        before: str | None = None,
        forced: bool = False,
        created: str = "2026-11-21T11:00:30Z",
        own: dict[str, Any] | None = None,
        without: tuple[str, ...] = (),
        no_api: bool = False,
    ) -> dict[str, Any]:
        """The plan of the push run 900 of ``sha`` (the checkout by default) to ``ref``, created at ``created``, whose event moved the
        branch from ``before`` (the pushed commit's first parent by default), by force when ``forced``, with the variables ``without``
        left out of its environment, and with no API reader when ``no_api``."""
        sha = sha or self.head()
        parent = self.git("rev-parse", "--verify", "--quiet", f"{sha}^1").stdout.strip() if self.git("rev-list", "--count", sha).stdout.strip() != "1" else ""
        event_file = self.repo.parent / "event.json"
        event_file.write_text(json.dumps({"ref": ref, "before": parent if before is None else before, "after": sha, "forced": forced}))
        self.github.runs = [run for run in self.github.runs if run["id"] != PUSH_RUN]
        self.github.add_run(PUSH_RUN, sha, event="push", branch="main", created_at=created)
        self.github.runs[-1].update(own or {})
        environment = self.environment(event, sha, ref) | {"GITHUB_EVENT_PATH": str(event_file), "GITHUB_RUN_ID": str(PUSH_RUN)}
        return ci_plan.plan(self.repo, {key: value for key, value in environment.items() if key not in without}, None if no_api else self.github)

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
                self.assertEqual((record["reuse"], record["reason"]), (False, ci_plan.EVERY_PULL_REQUEST))
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

    def merge(self, head: str, number: int = 5) -> None:
        """A squash merge of ``head`` onto the first commit, checked out as main and kept as ``merged``, with its pull request and trees known to the API."""
        self.merged = self.git("commit-tree", self.tree(head), "-p", self.base, "-m", "feat: merge the pull request").stdout.strip()
        self.git("checkout", "--quiet", "--detach", self.merged)
        self.github.add_pull(self.merged, number, head)
        self.github.trees |= {self.merged: self.tree(self.merged), head: self.tree(head)}
        self.github.compares[(self.base, head)] = ("ahead", self.base)

    def pull_run(
        self, run_id: int, *, skip: tuple[str, ...] = (), number: int = 5, claimed: int | None = None, base: str = "main", proven: bool = True, **fields: Any
    ) -> dict[str, int]:
        """A pull request run of the head, GitHub's run of pull request ``number`` (whose proofs claim pull request ``claimed`` into
        ``base``), with every job of the map passed, except the jobs named in ``skip``, and the proof of each job that runs checks.
        Returns the ID of each job by name."""
        self.github.add_run(run_id, self.pull_head, number=number, **fields)
        ids = {}
        for name in EVERY_JOB:
            if name not in skip:
                ids[name] = self.github.add_job(run_id, name)
                if proven and name != CONFIG["required"]["job"]:
                    self.github.add_proof(ids[name], self.proof(run_id, number=claimed or number, base=base, run_attempt=fields.get("run_attempt", 1)))
        return ids

    def proof(self, run_id: int, *, number: int = 5, base: str = "main", run_attempt: int = 1) -> dict[str, Any]:
        """The proof that a job writes. ``number`` and ``base`` are extra claims that the reader must ignore: GitHub's run names the pull request."""
        return {"run_id": run_id, "run_attempt": run_attempt, "head": self.pull_head, "runner": HERE, "pull_request": number, "base_ref": base}

    def fresh(self) -> None:
        self.github = FakeGitHub()
        self.merge(self.pull_head)

    def test_a_merge_with_the_pull_requests_tree_reuses_its_passed_run(self) -> None:
        record = self.push()
        self.assertEqual(self.runs(record), HOOKS_ONLY)
        self.assertTrue(record["reuse"])
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

    def test_the_newer_run_of_another_pull_request_does_not_hide_the_pull_requests_own(self) -> None:
        self.fresh()
        self.pull_run(60)
        self.pull_run(61, number=6)
        record = self.push()
        self.assertEqual(record.get("reused", {}).get("run_id"), 60, record["basis"])

    def test_the_pull_request_comes_from_the_run_that_github_names_not_from_the_proofs(self) -> None:
        self.fresh()
        self.pull_run(60, number=6, claimed=5)
        record = self.push()
        self.assertEqual(self.runs(record), ALL, "a run of pull request 6 whose proofs claim pull request 5")
        self.assertIn("no run of the workflow ran the head", record["basis"])
        self.fresh()
        self.pull_run(60, claimed=6, base="release")
        record = self.push()
        self.assertEqual(record.get("reused", {}).get("run_id"), 60, "the proofs' claims of another pull request and base do not count: " + record["basis"])

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

    def test_a_head_that_lacks_the_merges_first_parent_is_not_reused(self) -> None:
        """The workflow that a pull request run ran is the pushed commit's only when the run's merge tree is the head's tree: the head holds the
        base that the merge was made on."""
        for label, answer in (
            ("behind", ("behind", self.base)),
            ("diverged", ("diverged", "4" * 40)),
            ("a merge base that is another commit", ("ahead", "4" * 40)),
        ):
            with self.subTest(label=label):
                self.github.compares[(self.base, self.pull_head)] = answer
                record = self.push()
                self.assertEqual(self.runs(record), ALL)
                self.assertIn("does not hold", record["basis"])
        del self.github.compares[(self.base, self.pull_head)]
        self.assertIn("404", self.push()["basis"])
        self.github.compares[(self.base, self.pull_head)] = ("identical", self.base)
        self.assertTrue(self.push()["reuse"])

    def test_a_newest_run_whose_jobs_prove_nothing_refuses_reuse(self) -> None:
        self.pull_run(61, proven=False)
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("the run 61", record["basis"])

    def test_a_run_without_the_proof_of_every_job_is_not_reused(self) -> None:
        planted: dict[str, Callable[[dict[str, int]], None]] = {
            "a shard without a proof": lambda ids: self.github.annotations[ids[GUARD_JOBS[3]]].clear(),
            "a shard with two proofs": lambda ids: self.github.add_proof(ids[GUARD_JOBS[3]], self.proof(60)),
            "a proof that is not JSON": lambda ids: self.reprove(ids[GUARD_JOBS[3]], "not json"),
            "a proof of another run": lambda ids: self.reprove(ids[GUARD_JOBS[3]], self.proof(59)),
            "a proof of another attempt": lambda ids: self.reprove(ids[GUARD_JOBS[3]], self.proof(60, run_attempt=2)),
            "a proof of another head": lambda ids: self.reprove(ids[GUARD_JOBS[3]], self.proof(60) | {"head": "3" * 40}),
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

    def test_a_pull_request_run_is_the_workflows_run_of_the_pull_requests_merge_ref(self) -> None:
        self.assertEqual(self.push().get("reused", {}).get("run_id"), 60)
        planted: dict[str, str] = {
            "a path without a ref": ci_reuse.WORKFLOW,
            "the branch ref": f"{ci_reuse.WORKFLOW}@refs/heads/feature",
            "the merge ref of another pull request": f"{ci_reuse.WORKFLOW}@refs/pull/6/merge",
            "the head ref of this pull request": f"{ci_reuse.WORKFLOW}@refs/pull/5/head",
            "a longer ref": f"{ci_reuse.WORKFLOW}@refs/pull/5/merge/x",
            "a number that only starts like the pull request's": f"{ci_reuse.WORKFLOW}@refs/pull/50/merge",
        }
        for label, path in planted.items():
            with self.subTest(label=label):
                self.fresh()
                self.pull_run(60, path=path)
                self.assertEqual(self.runs(self.push()), ALL)

    def test_a_run_of_another_workflow_is_not_reused(self) -> None:
        planted: dict[str, dict[str, Any]] = {
            "another workflow ID": {"workflow_id": WORKFLOW_ID + 1},
            "another workflow file with a ref": {"path": ".github/workflows/deploy.yml@refs/pull/5/merge"},
            "a path that only starts like the workflow": {"path": f"{ci_reuse.WORKFLOW}.old@refs/pull/5/merge"},
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

    def test_a_run_on_other_runner_tools_is_not_reused(self) -> None:
        for field in ("image_os", "image_version", "python", "git", "make"):
            with self.subTest(field=field):
                self.fresh()
                ids = self.pull_run(60)
                self.reprove(ids[GUARD_JOBS[3]], self.proof(60) | {"runner": HERE | {field: "other"}})
                record = self.push()
                self.assertEqual(self.runs(record), ALL)
                self.assertIn("ran on the runner", record["basis"])
        self.fresh()
        ids = self.pull_run(60)
        self.reprove(ids[GUARD_JOBS[3]], {key: value for key, value in self.proof(60).items() if key != "runner"})
        self.assertEqual(self.runs(self.push()), ALL)

    def test_a_push_whose_runner_names_no_image_reuses_nothing(self) -> None:
        record = self.push(without=("ImageVersion",))
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("names no image version", record["basis"])

    def test_a_push_that_did_not_come_from_the_merge_reuses_nothing(self) -> None:
        later = self.git("commit-tree", self.tree(self.merged), "-p", self.merged, "-m", "feat: a later merge").stdout.strip()
        planted: dict[str, tuple[dict[str, Any], str]] = {
            "a forced reset of main to the merge": ({"forced": True}, "by force"),
            "a push from another commit than the merge's parent": ({"before": later}, "moved main from"),
            "a push run created long after the merge": ({"created": "2026-11-21T13:00:00Z"}, "minutes after"),
            "a push run created before the merge": ({"created": "2026-11-21T10:59:00Z"}, "minutes after"),
            "a push run of another commit": ({"own": {"head_sha": later}}, "is not the push run"),
            "a push run of another event": ({"own": {"event": "workflow_dispatch"}}, "is not the push run"),
            "a push run of another workflow": ({"own": {"workflow_id": WORKFLOW_ID + 1}}, "is not the push run"),
        }
        for label, (fields, reason) in planted.items():
            with self.subTest(label=label):
                record = self.push(**fields)
                self.assertEqual(self.runs(record), ALL)
                self.assertIn(reason, record["basis"])
        environment = self.environment("push") | {"GITHUB_RUN_ID": str(PUSH_RUN)}
        self.assertIn("names no event", ci_plan.plan(self.repo, environment, self.github)["basis"])

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
            "a job of an earlier attempt": ((GUARD_JOBS[1],), [(GUARD_JOBS[1], {"attempt": 1})]),
        }
        for label, (skip, extra) in planted.items():
            with self.subTest(label=label):
                self.fresh()
                self.pull_run(70, skip=skip, run_attempt=2)
                for name, fields in extra:
                    self.github.add_proof(self.github.add_job(70, name, **fields), self.proof(70, run_attempt=2))
                self.assertEqual(self.runs(self.push()), ALL)

    def test_a_run_of_another_head_in_the_list_is_no_candidate(self) -> None:
        self.fresh()
        self.pull_run(60)
        self.github.add_run(61, "3" * 40, number=5, run_started_at="2026-11-21T10:30:00Z")
        self.github.ignore_head_filter = True
        record = self.push()
        self.assertEqual(record.get("reused", {}).get("run_id"), 60, "the API's filter is not the only guard: " + record["basis"])

    def test_the_jobs_come_from_the_latest_attempt_only(self) -> None:
        self.fresh()
        self.pull_run(70, run_attempt=2)
        record = self.push()
        self.assertEqual(record.get("reused", {}).get("run_id"), 70, record["basis"])
        reads = [path for path in self.github.paths if "/jobs" in path]
        self.assertEqual([path.split("?")[0] for path in reads], [f"repos/{REPOSITORY}/actions/runs/70/attempts/2/jobs"])
        self.assertNotIn("filter=latest", " ".join(reads))

    def test_a_list_is_read_page_by_page_until_it_holds_every_item(self) -> None:
        self.fresh()
        self.github.page_size = 3
        self.pull_run(60)
        record = self.push()
        self.assertEqual(record.get("reused", {}).get("run_id"), 60, record["basis"])
        pages = [path for path in self.github.paths if "/attempts/1/jobs" in path]
        self.assertEqual(len(pages), -(-len(EVERY_JOB) // 3))
        self.assertTrue(all("per_page=100" in path for path in pages))

    def test_a_list_with_fewer_items_than_its_count_refuses_reuse(self) -> None:
        planted: dict[str, Callable[[], None]] = {
            "a run list that lacks a run": lambda: setattr(self.github, "missing_runs", 1),
            "a run list with more runs than its count": lambda: setattr(self.github, "missing_runs", -1),
            "a job list that lacks a job": lambda: self.github.missing_jobs.add(60),
        }
        for label, plant in planted.items():
            with self.subTest(label=label):
                self.fresh()
                self.pull_run(60)
                plant()
                record = self.push()
                self.assertEqual(self.runs(record), ALL)
                self.assertIn("incomplete", record["basis"])
        self.fresh()
        self.pull_run(60)
        self.github.missing_runs = 0
        self.assertTrue(self.push()["reuse"])

    def test_a_page_that_may_hold_more_than_it_shows_refuses_reuse(self) -> None:
        """A list without a count that fills its page may go on: a second proof or a second pull request could be on the next page."""
        other = {"title": "other", "annotation_level": "warning", "message": "x"}
        self.fresh()
        ids = self.pull_run(60)
        self.github.annotations[ids[GUARD_JOBS[3]]] += [other] * (ci_reuse.PAGE_SIZE - 2)
        self.assertTrue(self.push()["reuse"], "the proof and ninety-eight other annotations leave the page short")
        self.github.annotations[ids[GUARD_JOBS[3]]].append(other)
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("full page", record["basis"])
        self.fresh()
        self.pull_run(60)
        extra = ci_reuse.PAGE_SIZE - 2
        self.github.pulls[self.merged] += [dict(self.github.pulls[self.merged][0], number=100 + number, merged_at=None) for number in range(extra)]
        self.assertTrue(self.push()["reuse"], "the merged pull request and ninety-eight others leave the page short")
        self.github.pulls[self.merged].append(dict(self.github.pulls[self.merged][0], number=999, merged_at=None))
        record = self.push()
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("full page", record["basis"])

    def test_two_runs_that_started_together_refuse_reuse_whatever_their_attempts(self) -> None:
        self.fresh()
        self.pull_run(60, run_started_at="2026-11-21T10:30:00Z", run_attempt=2)
        self.pull_run(61, run_started_at="2026-11-21T10:30:00Z")
        self.assertIn("started together", self.push()["basis"])
        self.fresh()
        self.pull_run(60, run_started_at="2026-11-21T10:29:59Z")
        self.pull_run(61, run_started_at="2026-11-21T10:30:00Z")
        self.assertEqual(self.push()["reused"]["run_id"], 61, "a run that started one second later is the newest")
        self.fresh()
        self.pull_run(60, run_started_at="2026-11-21T10:30:00Z")
        self.pull_run(61, run_started_at="2026-11-21T10:30:00Z", number=6)
        self.assertEqual(self.push()["reused"]["run_id"], 60, "a run of another pull request is no candidate, so it ties with none")

    def test_the_summary_names_the_limits_of_a_reuse(self) -> None:
        record = self.push()
        text = ci_plan.summary(record)
        for limit in ci_reuse.LIMITS:
            self.assertIn(limit, text)
        self.assertIn("force-push", text)
        self.assertNotIn(ci_reuse.LIMITS[0], ci_plan.summary(self.pull_request()), "a pull request run reuses nothing")

    def test_a_run_that_is_not_the_pull_requests_own_is_not_reused(self) -> None:
        planted: dict[str, dict[str, Any]] = {
            "a push run": {"event": "push"},
            "a push run that names the merge ref": {"event": "push", "path": f"{ci_reuse.WORKFLOW}@refs/pull/5/merge"},
            "another head branch": {"head_branch": "other"},
            "another workflow": {"path": ".github/workflows/deploy.yml@refs/pull/5/merge"},
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
        record = self.push(no_api=True)
        self.assertEqual(self.runs(record), ALL)
        self.assertIn("no GitHub API token", record["basis"])


class ProofTest(PlanRepository):
    """The proof command writes one notice annotation whose message, as the runner reads it, names the job's run, attempt, head and runner."""

    def test_the_proof_of_a_job_names_its_run(self) -> None:
        event = self.repo.parent / "event.json"
        event.write_text(json.dumps({"pull_request": {"number": 5, "base": {"ref": "main"}, "head": {"sha": self.head()}}}))
        environment = (
            self.env
            | RUNNER_IMAGE
            | {
                "GITHUB_EVENT_NAME": "pull_request",
                "GITHUB_EVENT_PATH": str(event),
                "GITHUB_RUN_ID": "60",
                "GITHUB_RUN_ATTEMPT": "2",
            }
        )
        result = run_command(sys.executable, ["-m", "scripts.quality.ci_proof"], cwd=self.repo, env=environment, timeout=TIMEOUT_SECONDS, check=True)
        command, _, message = result.stdout.rstrip("\n").partition("::")[2].partition("::")
        self.assertEqual(command, f"notice title={ci_proof.TITLE}")
        self.assertEqual(result.stdout.count("\n"), 1)
        record = json.loads(message.replace("%0A", "\n").replace("%0D", "\r").replace("%25", "%"))
        expected = {"run_id": 60, "run_attempt": 2, "head": self.head()}
        self.assertEqual({key: record[key] for key in expected}, expected)
        self.assertEqual(sorted(record), ["head", "run_attempt", "run_id", "runner"], "the proof holds no claim that the reader could take for GitHub's fact")
        tools = {
            name: run_command(name, ["--version"], cwd=self.repo, env=self.env, timeout=TIMEOUT_SECONDS, check=True).stdout.splitlines()[0]
            for name in ("git", "make")
        }
        self.assertEqual(record["runner"], {"image_os": "ubuntu24", "image_version": "20261116.1.0", "python": platform.python_version(), **tools})

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
        self.assertEqual(ci_plan.output_lines({"reuse": False, "reason": "runs: a | b\nc"}), "reuse=false\nreason=runs: a \\| b c\n")
        self.assertEqual(ci_plan.output_lines({"reuse": True, "reason": "reused"}), "reuse=true\nreason=reused\n")
        self.assertEqual(ci_plan.output_lines({"reuse": 1, "reason": "x"}).splitlines()[0], "reuse=false", "only the boolean True says reuse")


class SlowApi:
    """A stand-in for the API on the loopback interface that waits ``pause`` seconds before each byte of its answer."""

    def __init__(
        self, test: unittest.TestCase, pause: float, body: bytes = b'{"answer": 1}', before_last_byte: Callable[[], None] = lambda: None, missing: int = 0
    ) -> None:
        """``missing`` bytes more than the body are announced, so the reader fails when the stand-in closes the connection."""
        self.requests = 0
        stand_in = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                stand_in.requests += 1
                with contextlib.suppress(OSError):
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(body) + missing))
                    self.end_headers()
                    for index, byte in enumerate(body):
                        time.sleep(pause)
                        if index == len(body) - 1:
                            before_last_byte()
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


class Clock:
    """A clock that stands still until a test moves it, for the moment that an answer arrives."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def set(self, now: float) -> None:
        self.now = now


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

    def test_a_slow_name_lookup_ends_at_the_deadline(self) -> None:
        released = threading.Event()
        self.addCleanup(released.set)

        def slow_lookup(*args: Any, **kwargs: Any) -> Any:
            released.wait(5)
            raise OSError("the name lookup was released")

        api = ci_reuse.github_api("https://api.example.invalid", "token", deadline_seconds=0.3)
        started = time.monotonic()
        with mock.patch("socket.getaddrinfo", slow_lookup), self.assertRaisesRegex(ci_reuse.ApiError, "deadline of 0.3 seconds"):
            api("repos/owner/project/actions/runs")
        self.assertLess(time.monotonic() - started, 1.0)

    def test_an_answer_that_arrives_at_the_deadline_is_discarded(self) -> None:
        clock = Clock()
        api = ci_reuse.github_api(SlowApi(self, pause=0.0, before_last_byte=lambda: clock.set(5.0)).url, "token", deadline_seconds=5.0, clock=clock)
        with self.assertRaisesRegex(ci_reuse.ApiError, "at or after the deadline of 5 seconds"):
            api("repos/owner/project/actions/runs")
        for arrives, discarded in ((9.0, True), (8.5, False)):
            with self.subTest(arrives=arrives):
                clock.set(4.0)
                api = ci_reuse.github_api(
                    SlowApi(self, pause=0.0, before_last_byte=functools.partial(clock.set, arrives)).url, "token", deadline_seconds=5.0, clock=clock
                )
                if discarded:
                    with self.assertRaisesRegex(ci_reuse.ApiError, "at or after the deadline of 5 seconds"):
                        api("repos/owner/project/actions/runs")
                else:
                    self.assertEqual(api("repos/owner/project/actions/runs"), {"answer": 1})

    def test_an_error_at_the_deadline_is_named_as_the_deadline(self) -> None:
        for arrives, reason in ((9.0, "failed at or after the deadline of 5 seconds"), (8.5, "failed: IncompleteRead")):
            with self.subTest(arrives=arrives):
                clock = Clock()
                clock.set(4.0)
                stand_in = SlowApi(self, pause=0.0, before_last_byte=functools.partial(clock.set, arrives), missing=5)
                api = ci_reuse.github_api(stand_in.url, "token", deadline_seconds=5.0, clock=clock)
                with self.assertRaisesRegex(ci_reuse.ApiError, reason):
                    api("repos/owner/project/actions/runs")

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


RUN_CONDITION = "github.event_name != 'push' || needs.plan.outputs.reuse != 'true'"
AFTER_FAILURE = "failure()"
CHECKOUT = "actions/checkout@"
HEAD_REF = "${{ github.event.pull_request.head.sha || github.sha }}"
SHARD_COMMAND = 'make test-guards SHARD="${SHARD}/{shards}"'
PROOF = "python -m scripts.quality.ci_proof"
SKIPPABLE = ("infra", "guards")
EXPRESSION = re.compile(r"\$\{\{ ([^}]+?) \}\}")
RESULTS = {"plan": "success", "hooks": "success", "infra": "success", "guards": "success"}
SKIPPED = RESULTS | {"infra": "skipped", "guards": "skipped"}


def aggregate_scenarios() -> list[tuple[str, str, dict[str, str], bool]]:
    """The event, the plan's ``reuse`` output, the result of each needed job and whether the required job must pass."""
    scenarios = [
        *((event, reuse, RESULTS, True) for event in ("pull_request", "push") for reuse in ("", "true", "false")),
        ("push", "true", SKIPPED, True),
        ("pull_request", "", SKIPPED, False),
        ("pull_request", "true", SKIPPED, False),
        ("pull_request", "false", SKIPPED, False),
        ("push", "", SKIPPED, False),
        ("push", "false", SKIPPED, False),
        ("push", "true", SKIPPED | {"guards": "failure"}, False),
    ]
    for job in RESULTS:
        for bad in ("failure", "cancelled", "skipped"):
            if job in SKIPPABLE and bad == "skipped":
                continue
            scenarios += [(event, reuse, RESULTS | {job: bad}, False) for event, reuse in (("pull_request", ""), ("push", "true"), ("push", ""))]
    return scenarios


def steps_run(steps: list[dict[str, Any]], *, failed: bool = False) -> list[dict[str, Any]]:
    """The steps that GitHub runs: each step without a condition, and the steps that run after a failure when ``failed``."""
    return [step for step in steps if "if" not in step or (failed and step["if"] == AFTER_FAILURE)]


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
        suite = [step for step in steps_run(job["steps"]) if str(step.get("run", "")).startswith("make test-guards")]
        if len(suite) != 1 or suite[0]["run"] != SHARD_COMMAND.replace("{shards}", str(shards)):
            return [f"the suite runs {[step.get('run') for step in suite]}"]
        environment = {name: str(text).replace("${{ matrix.shard }}", value) for name, text in (suite[0].get("env") or {}).items()}
        arguments.append(f"{environment.get('SHARD')}/{shards}")
    return arguments


def condition_problems(name: str, job: dict[str, Any]) -> list[str]:
    """A job runs on every pull request and skips only on a push whose plan said ``reuse=true``; the plan and the hooks never skip."""
    condition = job.get("if")
    if name in SKIPPABLE:
        if condition != RUN_CONDITION:
            return [f"{name}: its condition is {condition!r}, not the push-only skip condition {RUN_CONDITION!r}"]
    elif name == "required":
        if condition != "always()":
            return [f"required: its condition is {condition!r}, not always()"]
    elif condition is not None:
        return [f"{name}: it runs behind the condition {condition!r}"]
    return []


def condition_holds(condition: str, event: str, reuse: str) -> bool:
    """Whether GitHub runs a job with the ``if`` ``condition`` for the event and the plan's ``reuse`` output. It reads ``||``, ``&&``, the words
    ``true`` and ``false`` and comparisons of the event or the output with a quoted word; any other text raises ``ValueError``."""
    values = {"github.event_name": event, "needs.plan.outputs.reuse": reuse}

    def atom(text: str) -> bool:
        text = text.strip()
        if text in ("true", "false"):
            return text == "true"
        found = re.fullmatch(r"([\w.]+) (==|!=) '([^']*)'", text)
        if not found or found.group(1) not in values:
            raise ValueError(f"the condition {condition!r} is not one that the contract can evaluate")
        return (values[found.group(1)] == found.group(3)) == (found.group(2) == "==")

    return any(all(atom(part) for part in clause.split("&&")) for clause in condition.split("||"))


def schedule_problems(jobs: dict[str, Any]) -> list[str]:
    """The jobs that run for each event and plan output: all of them, but on a push with ``reuse=true`` the skippable ones skip. A pull request
    runs every job whatever the output, and a push runs every job unless the output is exactly ``true``."""
    problems = []
    for event in ("pull_request", "push"):
        for reuse in ("true", "false", ""):
            expected = {name for name in SKIPPABLE if event == "push" and reuse == "true"}
            try:
                skipped = {name for name in SKIPPABLE if not condition_holds(jobs[name].get("if", "true"), event, reuse)}
            except ValueError:
                continue
            if skipped != expected:
                problems.append(f"for the event {event} and the reuse output {reuse!r}, the jobs {sorted(skipped)} skip, not {sorted(expected)}")
    return problems


def step_condition_problems(name: str, job: dict[str, Any]) -> list[str]:
    allowed = {AFTER_FAILURE} if name == "guards" else set()
    return [f"{name}: a step runs behind the condition {step['if']!r}" for step in job["steps"] if "if" in step and step["if"] not in allowed]


def group_problems(name: str, job: dict[str, Any], config: dict[str, Any]) -> list[str]:
    """A group job waits for the plan, runs its commands after its setup and writes its proof as its last step, each step without a condition."""
    problems = []
    if job.get("needs") != "plan":
        problems.append(f"{name}: it does not wait for the plan")
    steps = steps_run(job["steps"])
    commands = command_steps(steps)
    wanted = config["groups"][name]["commands"]
    if len(commands) != len(wanted) or not all(command.split(" ")[:2] == want.split(" ") for command, want in zip(commands, wanted, strict=False)):
        problems.append(f"{name}: it runs {commands}, not {wanted}")
    else:
        problems += setup_problems(name, steps, config, steps.index(next(step for step in steps if step.get("run") == commands[0])))
    if not steps or steps[-1].get("run") != PROOF:
        problems.append(f"{name}: its last step does not write its proof")
    return problems


def matrix_problems(job: dict[str, Any], config: dict[str, Any]) -> list[str]:
    """The effective guard matrix: every shard of the map once, each entry running its own shard."""
    shards = config["groups"]["guards"]["shards"]
    expected = [f"{shard}/{shards}" for shard in range(1, shards + 1)]
    arguments = shard_arguments(job, config)
    if len(arguments) == 1 and arguments[0].startswith("the suite runs"):
        return [f"guards: {arguments[0]}"]
    counts = Counter(arguments)
    problems = []
    if [shard for shard in expected if shard not in counts]:
        problems.append(f"guards: the shards {[shard for shard in expected if shard not in counts]} never run")
    if [shard for shard, count in counts.items() if count > 1]:
        problems.append(f"guards: the shards {[shard for shard, count in counts.items() if count > 1]} run more than once")
    if [shard for shard in counts if shard not in expected]:
        problems.append(f"guards: the matrix runs shards {[shard for shard in counts if shard not in expected]} that the map does not have")
    if job.get("strategy", {}).get("fail-fast") != "false":
        problems.append("guards: one failed shard cancels the others")
    return problems


def substituted(text: str, values: dict[str, str]) -> str:
    """``text`` with each ``${{ expression }}`` replaced by its value; an expression without a value raises ``KeyError``."""
    return EXPRESSION.sub(lambda found: values[found.group(1)], text)


@functools.cache
def script_problems(script: str, variables: tuple[tuple[str, str], ...]) -> tuple[str, ...]:
    """What is wrong with the required job's ``script`` run for each scenario, with the step's environment ``variables`` (each value may name
    an expression of the workflow). Cached: the variants of the contract test share most scripts."""
    problems = []
    for event, reuse, results, passes in aggregate_scenarios():
        values = {"github.event_name": event, "needs.plan.outputs.reuse": reuse, "needs.plan.outputs.reason": "a reason"}
        values |= {f"needs.{name}.result": result for name, result in results.items()}
        try:
            environment = {name: substituted(text, values) for name, text in variables}
        except KeyError as error:
            return (f"required: its step reads {error}, which no needed job gives",)
        run = run_command(
            "bash", ["-e", "-c", script], env={"PATH": os.environ["PATH"], "GITHUB_STEP_SUMMARY": os.devnull, **environment}, timeout=TIMEOUT_SECONDS
        )
        if (run.returncode == 0) != passes:
            problems.append(
                f"required: on the event {event} with the reuse output {reuse!r} and the results {results} it {'passes' if run.returncode == 0 else 'fails'}"
            )
    return tuple(problems)


def aggregate_problems(jobs: dict[str, Any]) -> list[str]:
    """The required job waits for every other job and always runs; its script passes exactly when each job succeeded, or the skippable jobs were
    skipped on a push whose plan said ``reuse=true``. The script runs here for each scenario with the step's own environment."""
    required = jobs["required"]
    needs = required.get("needs") if isinstance(required.get("needs"), list) else [required.get("needs")]
    problems = []
    if sorted(needs) != sorted(set(jobs) - {"required"}):
        problems.append(f"required: it waits for {needs}, not for every other job")
    steps = required["steps"]
    if len(steps) != 1 or "run" not in steps[0]:
        return [*problems, "required: it has not exactly one step that runs a script"]
    variables = tuple((name, str(text)) for name, text in (steps[0].get("env") or {}).items())
    return [*problems, *script_problems(steps[0]["run"], variables)]


def contract_problems(text: str, config: dict[str, Any]) -> list[str]:
    """What the workflow fails to do: run every job on each pull request, skip a job only on a push whose plan proved a reuse, fail on any
    job that failed, was cancelled or was skipped without that proof, and run each shard once."""
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
            problems += condition_problems(name, job)
            if "permissions" in job and name != "plan":
                problems.append(f"{name}: it asks for permissions {job['permissions']}")
            if name != "required":
                problems += step_condition_problems(name, job)
            for step in job["steps"]:
                if str(step.get("uses", "")).startswith(CHECKOUT) and (step.get("with") or {}).get("ref") != HEAD_REF:
                    problems.append(f"{name}: a checkout of {(step.get('with') or {}).get('ref')} instead of the head commit")
                version = (step.get("with") or {}).get("python-version")
                if version is not None and version != config["python"]:
                    problems.append(f"{name}: Python {version}")
        problems += schedule_problems(jobs)
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
        if plan.get("outputs") != {"reuse": "${{ steps.plan.outputs.reuse }}", "reason": "${{ steps.plan.outputs.reason }}"}:
            problems.append("the plan job does not give its reuse decision and reason to the other jobs")
        hooks = jobs["hooks"]
        hook_commands = command_steps(hooks["steps"])
        if "needs" in hooks:
            problems.append("the hooks job waits for the plan")
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
        problems += matrix_problems(jobs["guards"], config)
        problems += aggregate_problems(jobs)
    except (AssertionError, KeyError, StopIteration, ValueError, TypeError, AttributeError) as error:
        problems.append(f"the workflow cannot be read as the contract expects: {type(error).__name__}: {error}")
    return problems


class WorkflowContractTest(unittest.TestCase):
    """The workflow's text is the contract: every pull request runs every job, a push skips a job only for a proven reuse, one job requires all."""

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

    def test_a_broken_workflow_fails_the_contract_with_its_own_diagnostic(self) -> None:
        guards = '      - run: make test-guards SHARD="${SHARD}/8"\n'
        terraform = "      - run: make check-terraform\n"
        ansible = "      - run: make check-ansible\n"
        checkout = "        with:\n          ref: ${{ github.event.pull_request.head.sha || github.sha }}\n          persist-credentials: false\n"
        infra_job = "    name: Static checks of the Terraform and Ansible code\n    needs: plan\n"
        condition = f"    if: {RUN_CONDITION}\n"
        skips = 'if [ "$2" = skipped ] && [ "$3" = may_skip ] && [ "${EVENT}" = push ] && [ "${REUSE}" = true ]; then return 0; fi'
        planted = {
            "the guards job never run": (
                condition + "    runs-on: ubuntu-24.04\n    timeout-minutes: 15",
                "    if: false\n    runs-on: ubuntu-24.04\n    timeout-minutes: 15",
                "guards: its condition is 'false'",
            ),
            "the infrastructure job run only on an explicit false output": (
                infra_job + condition,
                infra_job + "    if: needs.plan.outputs.reuse == 'false'\n",
                "infra: its condition is \"needs.plan.outputs.reuse == 'false'\"",
            ),
            "the plan output tested as the run decision, which an empty output fails open": (
                infra_job + condition,
                infra_job + "    if: needs.plan.outputs.run == 'true'\n",
                "infra: its condition is \"needs.plan.outputs.run == 'true'\"",
            ),
            "the plan output tested on pull requests too": (
                infra_job + condition,
                infra_job + "    if: github.event_name == 'pull_request' || needs.plan.outputs.reuse != 'true'\n",
                "infra: its condition is",
            ),
            "the infrastructure job skipped when the output is true on any event": (
                infra_job + condition,
                infra_job + "    if: needs.plan.outputs.reuse != 'true'\n",
                "for the event pull_request and the reuse output 'true', the jobs ['infra'] skip, not []",
            ),
            "the hooks job behind the plan's output": (
                "      - run: make check\n",
                "      - run: make check\n        if: needs.plan.outputs.reuse != 'true'\n",
                "hooks: a step runs behind the condition",
            ),
            "a step behind a condition in a skippable job": (
                terraform,
                terraform + "        if: github.event_name == 'push'\n",
                "infra: a step runs behind the condition",
            ),
            "shard 1 in every matrix entry": ('SHARD="${SHARD}/8"', 'SHARD="1/8"', "guards: the suite runs ['make test-guards SHARD=\"1/8\"']"),
            "shard 1 in the step's environment": ("SHARD: ${{ matrix.shard }}", 'SHARD: "1"', "guards: the shards ['2/8', '3/8'"),
            "one shard twice": ("shard: [1, 2, 3", "shard: [1, 1, 3", "guards: the shards ['1/8'] run more than once"),
            "a shard dropped": ("shard: [1, 2, 3, 4, 5, 6, 7, 8]", "shard: [1, 2, 3, 4, 5, 6, 7]", "guards: the shards ['8/8'] never run"),
            "a shard the map does not have": ("shard: [1, 2, 3, 4, 5, 6, 7, 8]", "shard: [1, 2, 3, 4, 5, 6, 7, 9]", "that the map does not have"),
            "one failed shard cancels the others": ("fail-fast: false", "fail-fast: true", "guards: one failed shard cancels the others"),
            "the guard suite removed": (guards, "", "guards: the suite runs []"),
            "the guard suite never run": (guards, guards + "        if: false\n", "guards: a step runs behind the condition 'false'"),
            "the Terraform checks removed": (terraform, "", "infra: it runs ['make setup', 'make check-ansible']"),
            "the Ansible check behind another condition": (
                ansible,
                ansible + "        if: github.event_name == 'push'\n",
                "infra: a step runs behind the condition",
            ),
            "the Ansible check before the Terraform checks": (
                terraform + ansible,
                ansible + terraform,
                "infra: it runs ['make setup', 'make check-ansible', 'make check-terraform']",
            ),
            "a checkout of GitHub's merge commit": (
                checkout,
                "        with:\n          persist-credentials: false\n",
                "a checkout of None instead of the head commit",
            ),
            "the hooks job skipped": (
                "      - run: make check\n",
                "      - run: make check\n        if: github.event_name == 'push'\n",
                "hooks: a step runs behind the condition",
            ),
            "the plan module replaced": (
                "run: python -m scripts.quality.ci_plan",
                "run: python -m scripts.quality.ci_reuse",
                "the plan job does not run the plan module",
            ),
            "the plan's reuse output dropped": ("      reuse: ${{ steps.plan.outputs.reuse }}\n", "", "the plan job does not give its reuse decision"),
            "the required job does not wait for the guards": (
                "needs: [plan, hooks, infra, guards]",
                "needs: [plan, hooks, infra]",
                "required: it waits for ['plan', 'hooks', 'infra']",
            ),
            "the required job skipped after a failure": ("    if: always()\n", "", "required: its condition is None, not always()"),
            "the required job passes a failure": (
                "          failed=1\n",
                "",
                "required: on the event pull_request with the reuse output '' and the results {'plan': 'failure'",
            ),
            "the required job accepts a skipped job without the proof": (
                skips,
                skips.replace(' && [ "${REUSE}" = true ]', ""),
                "required: on the event push with the reuse output '' and the results",
            ),
            "the required job accepts a skip on a pull request": (
                skips,
                skips.replace(' && [ "${EVENT}" = push ]', ""),
                "required: on the event pull_request with the reuse output 'true' and the results",
            ),
            "the required job accepts a skipped plan": ('check plan "${PLAN}" must_run', 'check plan "${PLAN}" may_skip', "'plan': 'skipped'"),
            "the required job reads the wrong result": ("INFRA: ${{ needs.infra.result }}", "INFRA: ${{ needs.hooks.result }}", "'infra': 'failure'"),
            "another Python in one job": ('python-version: "3.14.8"\n          cache', 'python-version: "3.14"\n          cache', "Python 3.14"),
            "a job asks to write": (
                "    timeout-minutes: 10\n    steps:",
                "    timeout-minutes: 10\n    permissions:\n      contents: write\n    steps:",
                "it asks for permissions",
            ),
            "pull requests into main only": ("  pull_request:\n", "  pull_request:\n    branches: [main]\n", "the workflow starts on"),
            "a shard without its proof": (
                "          SHARD: ${{ matrix.shard }}\n      - run: python -m scripts.quality.ci_proof\n",
                "          SHARD: ${{ matrix.shard }}\n",
                "guards: its last step does not write its proof",
            ),
            "the proof before the checks": (
                terraform + ansible + "      - run: python -m scripts.quality.ci_proof\n",
                "      - run: python -m scripts.quality.ci_proof\n" + terraform + ansible,
                "infra: its last step does not write its proof",
            ),
            "the hooks job without its proof": (
                'commit-range "${HEAD}"\n          fi\n      - run: python -m scripts.quality.ci_proof\n',
                'commit-range "${HEAD}"\n          fi\n',
                "the hooks job's last step does not write its proof",
            ),
            "the plan job cannot read the proofs": (
                "      actions: read\n      checks: read\n",
                "      actions: read\n",
                "the workflow or the plan job asks for more than reading",
            ),
        }
        for label, (before, after, diagnostic) in planted.items():
            with self.subTest(label=label):
                self.assertIn(before, WORKFLOW_TEXT)
                problems = contract_problems(WORKFLOW_TEXT.replace(before, after, 1), CONFIG)
                self.assertTrue(any(diagnostic in problem for problem in problems), f"no problem names {diagnostic!r}: {problems}")


class RulesetTest(unittest.TestCase):
    """The ruleset file that the owner applies to main requires a pull request and the one status check that the required job gives."""

    def setUp(self) -> None:
        self.ruleset = json.loads((ROOT / ".github/rulesets/main.json").read_text())
        self.rules = {rule["type"]: rule.get("parameters", {}) for rule in self.ruleset["rules"]}

    def test_main_requires_a_pull_request_and_the_one_status_check(self) -> None:
        self.assertEqual((self.ruleset["target"], self.ruleset["enforcement"]), ("branch", "active"))
        self.assertEqual(self.ruleset["conditions"]["ref_name"], {"include": ["~DEFAULT_BRANCH"], "exclude": []})
        self.assertEqual(sorted(self.rules), ["non_fast_forward", "pull_request", "required_status_checks"])
        checks = self.rules["required_status_checks"]["required_status_checks"]
        self.assertEqual([check["context"] for check in checks], [CONFIG["required"]["job"]], "the one check is the required job's name")
        self.assertEqual([check.get("integration_id") for check in checks], [15368], "only the GitHub Actions app can give the check")

    def test_the_ruleset_names_no_bypass_and_no_review_that_one_owner_cannot_give(self) -> None:
        self.assertEqual(self.ruleset["bypass_actors"], [], "the owner adds a bypass when a force-push to main is needed")
        self.assertEqual(self.rules["pull_request"]["required_approving_review_count"], 0)

    def test_the_required_check_is_the_name_that_the_workflow_gives(self) -> None:
        jobs = parse_workflow(WORKFLOW_TEXT)["jobs"]
        self.assertEqual(jobs["required"]["name"], self.rules["required_status_checks"]["required_status_checks"][0]["context"])


if __name__ == "__main__":
    unittest.main()
