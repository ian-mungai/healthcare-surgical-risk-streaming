"""The GitHub API facts that let a CI run do less: the passed pull request run that the push to main after a merge reuses.

Every answer comes from GitHub's API about this repository's own runs of ``.github/workflows/ci.yml``, read with the workflow's read-only
token. A run counts only when its workflow file is that path and its repository and head repository are this repository, so a run of
another workflow or of a fork never counts. Of the runs that count, only the newest execution decides: the newest run by creation time,
run number and attempt, and its jobs as the latest attempt left them. It must have completed with success; an older passed run or attempt
never stands in for a newer one.

A push to main reuses a pull request's result only when exactly one pull request of this repository was merged into main with the pushed
commit as its merge commit, the tree of the checkout equals the tree that the API gives for the pushed commit and for the pull request's
head commit, and the newest pull request run of that head commit on that head branch passed with every job of the map, each once and
no other. Any answer that cannot be read reuses nothing.
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
from typing import Any
from urllib.parse import urlsplit

WORKFLOW = ".github/workflows/ci.yml"
API_TIMEOUT_SECONDS = 30
API_DEADLINE_SECONDS = 120  # every API read of one plan together; the plan job's own limit is 5 minutes
LOOPBACK = ("127.0.0.1", "localhost")
SHA = re.compile(r"[0-9a-f]{40}")
MAIN = "main"

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


def github_api(base_url: str, token: str, deadline_seconds: float = API_DEADLINE_SECONDS) -> Api:
    """A reader of the API at ``base_url``: HTTPS, or HTTP on the loopback address for a local stand-in.

    Every request of the reader shares one deadline, ``deadline_seconds`` from now. A request that would start after it is refused, and at
    the deadline the open connection is shut, so an answer that arrives slowly cannot keep the plan waiting.
    """
    address = urlsplit(base_url)
    if address.scheme == "https":
        connection_type: type[http.client.HTTPConnection] = http.client.HTTPSConnection
    elif address.scheme == "http" and address.hostname in LOOPBACK:
        connection_type = http.client.HTTPConnection
    else:
        raise ApiError("the API address must use HTTPS, or HTTP on the loopback address")
    headers = {"Accept": "application/vnd.github+json", "Authorization": f"Bearer {token}", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "ci-plan"}
    deadline = time.monotonic() + deadline_seconds
    expired = f"the API reads passed their deadline of {deadline_seconds:g} seconds"

    def get(path: str) -> Any:
        where = path.split("?")[0]
        left = deadline - time.monotonic()
        if left <= 0:
            raise ApiError(f"GET {where} not sent: {expired}")
        connection = connection_type(address.netloc, timeout=min(API_TIMEOUT_SECONDS, left))
        shut: threading.Timer | None = None
        try:
            connection.connect()
            shut = threading.Timer(deadline - time.monotonic(), shut_down, args=(connection.sock,))
            shut.start()
            connection.request("GET", f"{address.path.rstrip('/')}/{path}", headers=headers)
            response = connection.getresponse()
            body = response.read()
        except (OSError, http.client.HTTPException) as error:
            raise ApiError(f"GET {where} failed: {expired if time.monotonic() >= deadline else type(error).__name__}") from error
        finally:
            if shut is not None:
                shut.cancel()
            connection.close()
        if response.status != 200:
            raise ApiError(f"GET {where} answered {response.status}")
        try:
            return json.loads(body)
        except ValueError as error:
            raise ApiError(f"GET {where} answered with text that is not JSON") from error

    return get


def shut_down(sock: socket.socket) -> None:
    """End a blocked read on ``sock`` from the deadline's timer thread; the response may hold the socket after the connection let it go."""
    with contextlib.suppress(OSError):
        sock.shutdown(socket.SHUT_RDWR)


def own_runs(api: Api, context: Context, event: str, query: str) -> list[dict[str, Any]]:
    """The runs of this repository's own workflow file for ``event`` among the answer for ``query``."""
    answer = api(f"repos/{context.repository}/actions/workflows/ci.yml/runs?{query}")
    return [
        run
        for run in answer["workflow_runs"]
        if run["path"] == WORKFLOW
        and run["event"] == event
        and run["repository"]["id"] == context.repository_id
        and run["head_repository"]["id"] == context.repository_id
    ]


def newest_passed(runs: list[dict[str, Any]], what: str) -> dict[str, Any]:
    """The newest of ``runs``, which must have completed with success."""
    if not runs:
        raise NoReuse(f"{what} has no run of {WORKFLOW}")
    latest = max(runs, key=lambda run: (str(run["created_at"]), int(run["run_number"]), int(run["run_attempt"])))
    if latest["status"] != "completed" or latest["conclusion"] != "success":
        raise NoReuse(f"the newest run {latest['id']} of {what} is {latest['status']} and concluded {latest['conclusion']}")
    return latest


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


def passed_jobs(api: Api, context: Context, run: dict[str, Any], head: str, required: frozenset[str]) -> list[str]:
    """The names of the jobs of ``run`` in their latest attempt, each of which passed for ``head``: exactly the ``required`` jobs."""
    answer = api(f"repos/{context.repository}/actions/runs/{run['id']}/jobs?filter=latest&per_page=100")
    jobs = answer["jobs"]
    if answer["total_count"] != len(jobs):
        raise NoReuse(f"the run {run['id']} has {answer['total_count']} jobs and the answer holds {len(jobs)}")
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


def reusable_run(api: Api, context: Context, commit: str, tree: str, required: frozenset[str]) -> Reused:
    """The passed pull request run that the push of ``commit``, whose checkout has ``tree``, reuses; ``NoReuse`` names why there is none."""
    pull = merged_pull_request(api, context, commit)
    head, branch = str(pull["head"]["sha"]), str(pull["head"]["ref"])
    if tree_of(api, context, commit) != tree:
        raise NoReuse(f"the tree of the checkout differs from the tree that the API gives for the commit {commit}")
    head_tree = tree_of(api, context, head)
    if head_tree != tree:
        raise NoReuse(f"the merged tree {tree} differs from the tree {head_tree} of the head {head} of pull request {pull['number']}")
    runs = own_runs(api, context, "pull_request", f"head_sha={head}&event=pull_request&per_page=100")
    run = newest_passed([run for run in runs if run["head_sha"] == head and run["head_branch"] == branch], f"the head {head} of pull request {pull['number']}")
    return Reused(commit, int(pull["number"]), head, tree, int(run["id"]), str(run["html_url"]), passed_jobs(api, context, run, head, required))
