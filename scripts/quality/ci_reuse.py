"""The GitHub API facts that let a CI run do less: the passed pull request run that the push to main after a merge reuses.

Every answer comes from GitHub's API about this repository's own runs of ``.github/workflows/ci.yml``, read with the workflow's read-only
token. A run counts only when it has that workflow's ID, its repository and head repository are this repository, and its ``path`` is the
workflow file at the merge ref of the merged pull request (``@refs/pull/<number>/merge``). GitHub sets those fields; pull request code
cannot. So a run of another workflow, of a fork or of another pull request never counts.

A push to main reuses a pull request's result only when all of these hold:

- exactly one pull request of this repository was merged into main with the pushed commit as its merge commit, and the push run is
  this workflow's run of that commit, created within 15 minutes after the merge (``scripts/quality/ci_plan.py`` also requires the
  push event to have moved main from the merge commit's first parent without force);
- the tree of the checkout equals the tree that the API gives for the pushed commit and for the pull request's head commit, and the head
  holds the merge commit's first parent. Then GitHub's merge of the head into the base that the run started from has the head's tree, so
  the workflow file that the run ran is the pushed commit's;
- of the runs of the head on the pull request's merge ref, the one that started last completed with success; an older passed run never
  stands in for a newer one, and two runs that started together refuse reuse;
- its jobs, read for its latest attempt only, are every job of the map, each once, and each passed in that attempt for the head commit;
  each job that runs checks proves, as a notice annotation, this run, its attempt and the head commit, on a runner with the image and the
  tool versions of the push run's own runner.

A list is read page by page until the items read equal the answer's ``total_count``; a list that falls short, or a list without a count
that fills its page, reuses nothing. Any answer that cannot be read reuses nothing.

The annotation is written by the pull request's own code, so its fields are claims. The pull request, its base and the workflow come from
the run, never from the annotation. The limits for the owner are ``LIMITS``.
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
PAGE_SIZE = 100
MAX_PAGES = 20  # 2,000 items; a longer list refuses reuse
LIMITS = (
    "Limit: the 15-minute window after the merge proves timing, not who pushed. A force-push of the merge commit to main inside the window needs "
    "the bypass of the main ruleset, which only the owner holds.",
    "Limit: the proof of each job is written by the pull request's own code, so its runner fields are claims. Only the owner writes to this repository, and "
    "a run from a fork is refused.",
)

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
    """What the push run checked out, the pushed commit, its tree and its first parent, the runner it runs on and the ID of the push run
    itself."""

    commit: str
    tree: str
    parent: str
    runner: dict[str, str]
    run_id: int


@dataclass(frozen=True)
class Jobs:
    """The jobs of a run: ``every`` job and the ``waiting`` job, which waits for every other job, runs no check and writes no proof."""

    every: frozenset[str]
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
        if clock() >= deadline:
            raise ApiError(f"GET {where} answered or failed at or after the deadline of {deadline_seconds:g} seconds, so the outcome is discarded")
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


def read_all(api: Api, path: str, key: str) -> list[dict[str, Any]]:
    """Every item under ``key`` of the pages of ``path``, read until the items read equal the answer's ``total_count``.

    A page set that falls short (an empty page before the count, or ``MAX_PAGES`` pages) or goes over the count is incomplete and refuses reuse."""
    items: list[dict[str, Any]] = []
    for page in range(1, MAX_PAGES + 1):
        answer = api(f"{path}{'&' if '?' in path else '?'}per_page={PAGE_SIZE}&page={page}")
        total, shown = answer["total_count"], answer[key]
        if not isinstance(total, int) or isinstance(total, bool) or not isinstance(shown, list):
            raise NoReuse(f"the answer for {path.split('?')[0]} has no count of its {key}")
        items += shown
        if len(items) == total:
            return items
        if len(items) > total or not shown:
            break
    raise NoReuse(f"the pages of {path.split('?')[0]} are incomplete: {len(items)} {key} read, {total} counted")


def one_page(found: list[dict[str, Any]], where: str) -> list[dict[str, Any]]:
    """``found`` when it fills no page. The list has no count, so a full page may go on."""
    if len(found) >= PAGE_SIZE:
        raise NoReuse(f"{where} fills a full page of {PAGE_SIZE}, so more may follow that were not read")
    return found


def pull_request_runs(api: Api, context: Context, pull: dict[str, Any]) -> list[dict[str, Any]]:
    """The runs of this repository's own workflow for the head of ``pull``, the workflow's runs of the pull request's merge ref.

    GitHub sets the ``path`` (the workflow file and the ref that the run ran from), the event, the head commit, the head branch and
    both repositories of a run. A run counts when its ``path`` is the workflow file at ``refs/pull/<number>/merge``."""
    workflow = workflow_id(api, context)
    head, branch, number = str(pull["head"]["sha"]), str(pull["head"]["ref"]), int(pull["number"])
    runs = read_all(api, f"repos/{context.repository}/actions/workflows/{workflow}/runs?head_sha={head}&event=pull_request", "workflow_runs")
    return [
        run
        for run in runs
        if run["workflow_id"] == workflow
        and run["path"] == f"{WORKFLOW}@refs/pull/{number}/merge"
        and run["event"] == "pull_request"
        and run["head_sha"] == head
        and run["head_branch"] == branch
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
    found = one_page(api(f"repos/{context.repository}/commits/{commit}/pulls?per_page={PAGE_SIZE}"), f"the list of pull requests of the commit {commit}")
    merged = [pull for pull in found if pull["merged_at"] and pull["merge_commit_sha"] == commit and pull["base"]["ref"] == MAIN]
    if not merged:
        raise NoReuse(f"no pull request was merged into {MAIN} with the commit {commit} (a push without a pull request runs every job)")
    if len(merged) > 1:
        raise NoReuse(f"{len(merged)} pull requests name the commit {commit} as their merge commit")
    pull: dict[str, Any] = merged[0]
    if (pull["head"]["repo"] or {}).get("id") != context.repository_id:
        raise NoReuse(f"pull request {pull['number']} comes from another repository")
    return pull


def attempt_jobs(api: Api, context: Context, run: dict[str, Any]) -> list[dict[str, Any]]:
    """The jobs of the latest attempt of ``run``, read from that attempt's own list; each job ran in that attempt."""
    attempt = int(run["run_attempt"])
    jobs = read_all(api, f"repos/{context.repository}/actions/runs/{run['id']}/attempts/{attempt}/jobs", "jobs")
    for job in jobs:
        if int(job["run_attempt"]) != attempt:
            raise NoReuse(f"the job {job['name']} of the run {run['id']} ran in attempt {job['run_attempt']}, not in the latest attempt {attempt}")
    return jobs


def proof_of(api: Api, context: Context, run: dict[str, Any], job: dict[str, Any]) -> dict[str, Any]:
    """The one proof that ``job`` of ``run`` wrote as a notice annotation on its own check run."""
    where = f"the job {job['name']} of the run {run['id']}"
    if not str(job["check_run_url"]).endswith(f"/repos/{context.repository}/check-runs/{int(job['id'])}"):
        raise NoReuse(f"{where} names a check run that is not its own: {job['check_run_url']}")
    found = one_page(api(f"repos/{context.repository}/check-runs/{int(job['id'])}/annotations?per_page={PAGE_SIZE}"), f"the annotations of {where}")
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


def newest(candidates: list[dict[str, Any]], number: int) -> dict[str, Any]:
    """The run that started last, which must have completed with success; two candidates with the newest start time refuse reuse."""
    if not candidates:
        raise NoReuse(f"no run of the workflow ran the head of pull request {number} at the merge ref of the pull request")
    order = sorted(candidates, key=lambda run: datetime.fromisoformat(str(run["run_started_at"])))
    run = order[-1]
    if len(order) > 1 and datetime.fromisoformat(str(order[-2]["run_started_at"])) == datetime.fromisoformat(str(run["run_started_at"])):
        raise NoReuse(f"the runs {order[-2]['id']} and {run['id']} of pull request {number} started together")
    if run["status"] != "completed" or run["conclusion"] != "success":
        raise NoReuse(f"the newest run {run['id']} of pull request {number} is {run['status']} and concluded {run['conclusion']}")
    return run


def contains(api: Api, context: Context, ancestor: str, commit: str) -> None:
    """Raise ``NoReuse`` unless ``commit`` holds ``ancestor``: GitHub's comparison of the two says ``commit`` is ahead of it or identical."""
    found = api(f"repos/{context.repository}/compare/{ancestor}...{commit}?per_page=1")
    if found["status"] not in ("ahead", "identical") or found["merge_base_commit"]["sha"] != ancestor:
        raise NoReuse(f"the head {commit} does not hold {ancestor}, the first parent of the pushed commit: the comparison says {found['status']}")


def proven_jobs(api: Api, context: Context, run: dict[str, Any], jobs: list[dict[str, Any]], head: str, waiting: str, runner: dict[str, str]) -> None:
    """Raise ``NoReuse`` unless the proof of every job of ``run`` but ``waiting`` names this run, its latest attempt and ``head``, on ``runner``.

    The runner fields are claims of the pull request's own code. The run, attempt and head must agree with what the API says."""
    for job in jobs:
        if job["name"] == waiting:
            continue
        proof = proof_of(api, context, run, job)
        expected = {"run_id": int(run["id"]), "run_attempt": int(run["run_attempt"]), "head": head}
        found = {key: proof.get(key) for key in expected}
        if found != expected:
            raise NoReuse(f"the proof of the job {job['name']} of the run {run['id']} names {found}, not {expected}")
        if proof.get("runner") != runner:
            raise NoReuse(f"the job {job['name']} of the run {run['id']} ran on the runner {proof.get('runner')}, and this run runs on {runner}")


def started_by_merge(api: Api, context: Context, checkout: Checkout, pull: dict[str, Any]) -> None:
    """Raise ``NoReuse`` unless the push run is this workflow's run of the pushed commit, created within ``MERGE_WINDOW_SECONDS`` after
    the pull request was merged; a forced reset of main to an older merge commit starts a run long after that merge. The window proves
    timing, not who pushed (``LIMITS``): a force-push of the merge commit within it needs the owner's bypass of the main ruleset."""
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
    contains(api, context, checkout.parent, head)
    run = newest(pull_request_runs(api, context, pull), number)
    jobs = attempt_jobs(api, context, run)
    passed = passed_jobs(run, jobs, head, names.every)
    proven_jobs(api, context, run, jobs, head, names.waiting, checkout.runner)
    return Reused(commit, number, head, tree, int(run["id"]), str(run["html_url"]), passed)
