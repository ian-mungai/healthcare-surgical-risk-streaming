"""The GitHub API facts that let a CI run do less: the passed pull request run that the push to main after a merge reuses.

Every answer comes from GitHub's API about this repository's own runs of ``.github/workflows/ci.yml``, read with the workflow's read-only
token. A run counts only when it has that workflow's ID, its path names that file (alone or with ``@`` and a ref) and its repository and
head repository are this repository, so a run of another workflow or of a fork never counts.

A push to main reuses a pull request's result only when all of these hold:

- exactly one pull request of this repository was merged into main with the pushed commit as its merge commit, and the push run is
  this workflow's run of that commit, created within 15 minutes after the merge (``scripts/quality/ci_plan.py`` also requires the
  push event to have moved main from the merge commit's first parent without force);
- the tree of the checkout equals the tree that the API gives for the pushed commit and for the pull request's head commit;
- the runs of that head commit on that head branch are the pull request's own: the proof of each run's plan job
  (``scripts/quality/ci_proof.py``) names the pull request and main as its base, and a run whose plan job proves nothing refuses
  reuse, because it could be the newest;
- of those runs, the one whose latest attempt started last completed with success; an older passed run or attempt never stands in
  for a newer one, and two runs that started together refuse reuse;
- its jobs, as the latest attempt left them, are every job of the map, each once and passed for the head commit, and each job that
  runs checks proves this run, its own attempt, the pull request, main and the head commit, on a runner with the image and the tool
  versions of the push run's own runner;
- the workflow file that the run ran, at the commit its proofs name (for a pull request, GitHub's merge commit), has the blob of the
  pushed commit's workflow file.

Any answer that cannot be read reuses nothing.
"""

from __future__ import annotations

import contextlib
import http.client
import json
import re
import socket
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit

from scripts.quality import ci_proof

WORKFLOW = ".github/workflows/ci.yml"
API_TIMEOUT_SECONDS = 30
API_DEADLINE_SECONDS = 120  # every API read of one plan together; the plan job's own limit is 5 minutes
LOOPBACK = ("127.0.0.1", "localhost")
SHA = re.compile(r"[0-9a-f]{40}")
MAIN = "main"
MERGE_WINDOW_SECONDS = 15 * 60  # GitHub starts the push run within seconds of a merge; a later run may come from another push of the same commit
WORKFLOW_PATH = re.compile(re.escape(WORKFLOW) + r"(?:@\S+)?")

Api = Callable[[str], Any]


class NoReuse(Exception):
    """Nothing is reused, or the start commit is not proven; the message is the reason."""


class ApiError(Exception):
    """The GitHub API gave no readable answer."""


@dataclass(frozen=True)
class Context:
    repository: str
    repository_id: int


@dataclass(frozen=True)
class Checkout:
    """What the push run checked out, the pushed commit, its tree and the blob of its workflow file, the runner it runs on and the ID
    of the push run itself."""

    commit: str
    tree: str
    workflow_blob: str
    runner: dict[str, str]
    run_id: int


@dataclass(frozen=True)
class Jobs:
    """The jobs of a run: ``every`` job, the ``plan`` job whose proof binds a run to its pull request and the ``waiting`` job, which waits
    for every other job, runs no check and writes no proof."""

    every: frozenset[str]
    plan: str
    waiting: str


@dataclass(frozen=True)
class Reused:
    """The pull request run that a push to main reuses."""

    commit: str
    pull_request: int
    head: str
    tree: str
    run_id: int
    run_url: str
    jobs: list[str]


def context_of(environment: Mapping[str, str]) -> Context:
    """The repository that GitHub names in the job's environment."""
    try:
        return Context(environment["GITHUB_REPOSITORY"], int(environment["GITHUB_REPOSITORY_ID"]))
    except (KeyError, ValueError) as error:
        raise NoReuse(f"the run's environment does not name the repository ({type(error).__name__}: {error})") from error


def github_api(base_url: str, token: str, deadline_seconds: float = API_DEADLINE_SECONDS, clock: Callable[[], float] = time.monotonic) -> Api:
    """A reader of the API at ``base_url``: HTTPS, or HTTP on the loopback address for a local stand-in.

    Every request of the reader shares one deadline, ``deadline_seconds`` after the reader is made, measured by ``clock``. A request
    that would start after it is refused. Each request runs in a worker thread, from the name lookup to the last byte of the answer,
    and the caller waits for it only until the deadline: then it shuts the connection's socket, leaves the worker (a daemon thread,
    which a blocked name lookup may keep until the process ends) and raises. An answer that is read or decoded at or after the deadline
    is discarded.
    """
    address = urlsplit(base_url)
    if address.scheme == "https":
        connection_type: type[http.client.HTTPConnection] = http.client.HTTPSConnection
    elif address.scheme == "http" and address.hostname in LOOPBACK:
        connection_type = http.client.HTTPConnection
    else:
        raise ApiError("the API address must use HTTPS, or HTTP on the loopback address")
    headers = {"Accept": "application/vnd.github+json", "Authorization": f"Bearer {token}", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "ci-plan"}
    deadline = clock() + deadline_seconds
    expired = f"the API reads passed their deadline of {deadline_seconds:g} seconds"

    def get(path: str) -> Any:
        where = path.split("?")[0]
        left = deadline - clock()
        if left <= 0:
            raise ApiError(f"GET {where} not sent: {expired}")
        connection = connection_type(address.netloc, timeout=min(API_TIMEOUT_SECONDS, left))
        outcome: dict[str, Any] = {}

        def fetch() -> None:
            try:
                connection.request("GET", f"{address.path.rstrip('/')}/{path}", headers=headers)
                response = connection.getresponse()
                outcome["answer"] = (response.status, response.read())
            except (OSError, http.client.HTTPException) as error:
                outcome["error"] = error

        worker = threading.Thread(target=fetch, name=f"api {where}", daemon=True)
        worker.start()
        worker.join(left)
        if worker.is_alive():
            if connection.sock is not None:
                shut_down(connection.sock)
            raise ApiError(f"GET {where} failed: {expired}")
        connection.close()
        if "error" in outcome:
            raise ApiError(f"GET {where} failed: {type(outcome['error']).__name__}") from outcome["error"]
        status, body = outcome["answer"]
        if status != 200:
            raise ApiError(f"GET {where} answered {status}")
        try:
            answer = json.loads(body)
        except ValueError as error:
            raise ApiError(f"GET {where} answered with text that is not JSON") from error
        if clock() >= deadline:
            raise ApiError(f"GET {where} answered at or after the deadline of {deadline_seconds:g} seconds, so the answer is discarded")
        return answer

    return get


def shut_down(sock: socket.socket) -> None:
    """End a blocked read on ``sock`` of a worker that passed the deadline."""
    with contextlib.suppress(OSError):
        sock.shutdown(socket.SHUT_RDWR)


def workflow_id(api: Api, context: Context) -> int:
    """The ID of this repository's workflow at ``WORKFLOW``."""
    found = api(f"repos/{context.repository}/actions/workflows/{WORKFLOW.rpartition('/')[2]}")
    if found["path"] != WORKFLOW:
        raise NoReuse(f"the workflow that the API names, {found['path']}, is not the workflow {WORKFLOW}")
    return int(found["id"])


def own_runs(api: Api, context: Context, event: str, query: str) -> list[dict[str, Any]]:
    """The runs of this repository's own workflow for ``event`` among the answer for ``query``.

    A run is the workflow's when it has the workflow's ID and its ``path`` names the workflow file alone or, in the form that GitHub also
    documents, with ``@`` and the ref it ran from."""
    workflow = workflow_id(api, context)
    answer = api(f"repos/{context.repository}/actions/workflows/{workflow}/runs?{query}")
    return [
        run
        for run in answer["workflow_runs"]
        if run["workflow_id"] == workflow
        and WORKFLOW_PATH.fullmatch(str(run["path"]))
        and run["event"] == event
        and run["repository"]["id"] == context.repository_id
        and run["head_repository"]["id"] == context.repository_id
    ]


def tree_of(api: Api, context: Context, commit: str) -> str:
    tree = api(f"repos/{context.repository}/git/commits/{commit}")["tree"]["sha"]
    if not isinstance(tree, str) or not SHA.fullmatch(tree):
        raise NoReuse(f"the API names no tree for the commit {commit}")
    return tree


def merged_pull_request(api: Api, context: Context, commit: str) -> dict[str, Any]:
    """The one pull request of this repository that was merged into main with ``commit`` as its merge commit."""
    found = api(f"repos/{context.repository}/commits/{commit}/pulls?per_page=100")
    merged = [pull for pull in found if pull["merged_at"] and pull["merge_commit_sha"] == commit and pull["base"]["ref"] == MAIN]
    if not merged:
        raise NoReuse(f"no pull request was merged into {MAIN} with the commit {commit} (a push without a pull request runs every job)")
    if len(merged) > 1:
        raise NoReuse(f"{len(merged)} pull requests name the commit {commit} as their merge commit")
    pull: dict[str, Any] = merged[0]
    if (pull["head"]["repo"] or {}).get("id") != context.repository_id:
        raise NoReuse(f"pull request {pull['number']} comes from another repository")
    return pull


def latest_jobs(api: Api, context: Context, run: dict[str, Any]) -> list[dict[str, Any]]:
    """The jobs of ``run`` as its latest attempt left them, all on one page."""
    answer = api(f"repos/{context.repository}/actions/runs/{run['id']}/jobs?filter=latest&per_page=100")
    jobs: list[dict[str, Any]] = answer["jobs"]
    if answer["total_count"] != len(jobs):
        raise NoReuse(f"the run {run['id']} has {answer['total_count']} jobs and the answer holds {len(jobs)}")
    return jobs


def proof_of(api: Api, context: Context, run: dict[str, Any], job: dict[str, Any]) -> dict[str, Any]:
    """The one proof that ``job`` of ``run`` wrote as a notice annotation on its own check run."""
    where = f"the job {job['name']} of the run {run['id']}"
    if not str(job["check_run_url"]).endswith(f"/repos/{context.repository}/check-runs/{int(job['id'])}"):
        raise NoReuse(f"{where} names a check run that is not its own: {job['check_run_url']}")
    found = api(f"repos/{context.repository}/check-runs/{int(job['id'])}/annotations?per_page=100")
    proofs = [note for note in found if note["title"] == ci_proof.TITLE and note["annotation_level"] == "notice"]
    if len(proofs) != 1:
        raise NoReuse(f"{where} holds {len(proofs)} proofs, not one")
    try:
        record = json.loads(proofs[0]["message"])
    except ValueError as error:
        raise NoReuse(f"the proof of {where} is not JSON") from error
    if not isinstance(record, dict):
        raise NoReuse(f"the proof of {where} is not a JSON object")
    return record


def job_named(jobs: list[dict[str, Any]], name: str, run: dict[str, Any]) -> dict[str, Any]:
    found = [job for job in jobs if job["name"] == name]
    if len(found) != 1:
        raise NoReuse(f"the run {run['id']} has {len(found)} jobs named {name}")
    return found[0]


def passed_jobs(run: dict[str, Any], jobs: list[dict[str, Any]], head: str, required: frozenset[str]) -> list[str]:
    """The names of ``jobs``, each of which passed for ``head``: exactly the ``required`` jobs, each once."""
    names = [str(job["name"]) for job in jobs]
    if len(set(names)) != len(names):
        raise NoReuse(f"the run {run['id']} has two jobs with one name")
    for job in jobs:
        if job["status"] != "completed" or job["conclusion"] != "success" or job["head_sha"] != head:
            raise NoReuse(f"the job {job['name']} of the run {run['id']} is {job['status']} and concluded {job['conclusion']} for {job['head_sha']}")
    held = set(names)
    if held != required:
        raise NoReuse(f"the run {run['id']} lacks {sorted(required - held)} or holds jobs the map does not name {sorted(held - required)}")
    return sorted(names)


def runs_of_pull_request(api: Api, context: Context, pull: dict[str, Any], plan_job: str) -> list[tuple[dict[str, Any], list[dict[str, Any]]]]:
    """Each run of the pull request's head commit and head branch whose plan job proves that it ran for ``pull`` into main, with its jobs.

    A run of that head whose plan job proves nothing refuses reuse: it could be the pull request's newest run."""
    head, branch, number = str(pull["head"]["sha"]), str(pull["head"]["ref"]), int(pull["number"])
    runs = own_runs(api, context, "pull_request", f"head_sha={head}&event=pull_request&per_page=100")
    bound = []
    for run in (run for run in runs if run["head_sha"] == head and run["head_branch"] == branch):
        jobs = latest_jobs(api, context, run)
        try:
            proof = proof_of(api, context, run, job_named(jobs, plan_job, run))
        except NoReuse as error:
            raise NoReuse(f"the run {run['id']} of the head {head} proves no pull request: {error}") from error
        if proof.get("pull_request") == number and proof.get("base_ref") == MAIN:
            bound.append((run, jobs))
    if not bound:
        raise NoReuse(f"no run of the head {head} proves that it ran for pull request {number} into {MAIN}")
    return bound


def newest(bound: list[tuple[dict[str, Any], list[dict[str, Any]]]], number: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """The run that started last, by the start of its latest attempt and the attempt's number, which must have completed with success."""
    order = sorted(bound, key=lambda pair: (str(pair[0]["run_started_at"]), int(pair[0]["run_attempt"])))
    run, jobs = order[-1]
    if len(order) > 1 and (order[-2][0]["run_started_at"], order[-2][0]["run_attempt"]) == (run["run_started_at"], run["run_attempt"]):
        raise NoReuse(f"the runs {order[-2][0]['id']} and {run['id']} of pull request {number} started together")
    if run["status"] != "completed" or run["conclusion"] != "success":
        raise NoReuse(f"the newest run {run['id']} of pull request {number} is {run['status']} and concluded {run['conclusion']}")
    return run, jobs


def proven_workflow(
    api: Api, context: Context, run: dict[str, Any], jobs: list[dict[str, Any]], pull: dict[str, Any], waiting: str, runner: dict[str, str]
) -> str:
    """The commit of the workflow file that every job of ``run`` but ``waiting`` proves, each for this run, attempt, pull request and
    head, on ``runner``."""
    revisions = set()
    for job in jobs:
        if job["name"] == waiting:
            continue
        proof = proof_of(api, context, run, job)
        expected = {
            "run_id": int(run["id"]),
            "run_attempt": int(job["run_attempt"]),
            "event": "pull_request",
            "pull_request": int(pull["number"]),
            "base_ref": MAIN,
            "head": str(pull["head"]["sha"]),
        }
        found = {key: proof.get(key) for key in expected}
        if found != expected:
            raise NoReuse(f"the proof of the job {job['name']} of the run {run['id']} names {found}, not {expected}")
        if proof.get("runner") != runner:
            raise NoReuse(f"the job {job['name']} of the run {run['id']} ran on the runner {proof.get('runner')}, and this run runs on {runner}")
        revisions.add(proof.get("workflow_sha"))
    if len(revisions) != 1 or not SHA.fullmatch(str(next(iter(revisions)))):
        raise NoReuse(f"the jobs of the run {run['id']} prove the workflow commits {sorted(map(str, revisions))}, not one")
    return str(revisions.pop())


def workflow_blob(api: Api, context: Context, commit: str) -> str:
    """The blob of the workflow file in ``commit``."""
    found = api(f"repos/{context.repository}/contents/{WORKFLOW}?ref={commit}")
    if found["type"] != "file" or found["path"] != WORKFLOW or not SHA.fullmatch(str(found["sha"])):
        raise NoReuse(f"the API names no workflow file in the commit {commit}")
    return str(found["sha"])


def started_by_merge(api: Api, context: Context, checkout: Checkout, pull: dict[str, Any]) -> None:
    """Raise ``NoReuse`` unless the push run is this workflow's run of the pushed commit, created within ``MERGE_WINDOW_SECONDS`` after
    the pull request was merged; a forced reset of main to an older merge commit starts a run long after that merge."""
    own = api(f"repos/{context.repository}/actions/runs/{checkout.run_id}")
    if own["head_sha"] != checkout.commit or own["event"] != "push" or own["workflow_id"] != workflow_id(api, context):
        raise NoReuse(f"the run {checkout.run_id} is not the push run of {checkout.commit} by this workflow")
    seconds = (datetime.fromisoformat(own["created_at"]) - datetime.fromisoformat(pull["merged_at"])).total_seconds()
    if not 0 <= seconds <= MERGE_WINDOW_SECONDS:
        window = MERGE_WINDOW_SECONDS // 60
        raise NoReuse(f"the push run was created {seconds / 60:.1f} minutes after pull request {pull['number']} was merged, not within {window} minutes after")


def reusable_run(api: Api, context: Context, checkout: Checkout, names: Jobs) -> Reused:
    """The passed pull request run that the push of ``checkout`` reuses, holding exactly the jobs ``names``; ``NoReuse`` names why there is none."""
    commit, tree = checkout.commit, checkout.tree
    pull = merged_pull_request(api, context, commit)
    head, number = str(pull["head"]["sha"]), int(pull["number"])
    started_by_merge(api, context, checkout, pull)
    if tree_of(api, context, commit) != tree:
        raise NoReuse(f"the tree of the checkout differs from the tree that the API gives for the commit {commit}")
    head_tree = tree_of(api, context, head)
    if head_tree != tree:
        raise NoReuse(f"the merged tree {tree} differs from the tree {head_tree} of the head {head} of pull request {number}")
    run, jobs = newest(runs_of_pull_request(api, context, pull, names.plan), number)
    passed = passed_jobs(run, jobs, head, names.every)
    revision = proven_workflow(api, context, run, jobs, pull, names.waiting, checkout.runner)
    blob = workflow_blob(api, context, revision)
    if blob != checkout.workflow_blob:
        raise NoReuse(f"the run {run['id']} ran the workflow file {blob} of the commit {revision}, and the pushed commit holds {checkout.workflow_blob}")
    return Reused(commit, number, head, tree, int(run["id"]), str(run["html_url"]), passed)
