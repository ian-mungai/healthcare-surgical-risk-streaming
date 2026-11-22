"""Reuse of passed job groups for an identical tree, and the proof that a range's start commit passed, read from the GitHub API.

Every answer comes from GitHub's API about runs of this repository's own workflow file on GitHub, read with the workflow's read-only token
(``actions: read``). Only push runs of ``.github/workflows/ci.yml`` on ``main`` count, whose repository and head repository are this
repository, so a run of another workflow, another branch, a pull request or a fork never counts.

A group's reuse key covers everything that decides its outcome: the tree of the checkout, the workflow file, ``requirements.txt``, the tool
pins in ``scripts/quality/install_tools.py``, the runner label, the Python version and the group's own definition in the map. The files are
read as blobs of the checked-out commit. Each job of a group that runs carries the first 16 characters of its key in its name, so a later
run finds a passed job by its exact name. A job is reused when an earlier run (or an earlier attempt of this run) with the same tree has
it completed with success, for the same commit, on the runner label of the key. A key input that cannot be read, or any answer of the API
that cannot be read, reuses nothing.
"""

from __future__ import annotations

import contextlib
import hashlib
import http.client
import json
import socket
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from subprocess import CalledProcessError
from typing import Any
from urllib.parse import urlsplit

from scripts.quality.repo_checks import git

WORKFLOW = ".github/workflows/ci.yml"
KEY_FILES = {"workflow": WORKFLOW, "requirements": "requirements.txt", "tools": "scripts/quality/install_tools.py"}
KEY_LENGTH = 16
API_TIMEOUT_SECONDS = 30
API_DEADLINE_SECONDS = 120  # every API read of one plan together; the plan job's own limit is 5 minutes
LOOPBACK = ("127.0.0.1", "localhost")
CANDIDATE_RUNS = 20

Api = Callable[[str], Any]


class NoReuse(Exception):
    """Nothing is reused, or the start commit is not proven; the message is the reason."""


class ApiError(Exception):
    """The GitHub API gave no readable answer."""


@dataclass(frozen=True)
class Context:
    repository: str
    repository_id: int
    run_id: int
    run_attempt: int


def context_of(environment: Mapping[str, str]) -> Context:
    """The repository and run that GitHub names in the job's environment."""
    try:
        return Context(
            environment["GITHUB_REPOSITORY"],
            int(environment["GITHUB_REPOSITORY_ID"]),
            int(environment["GITHUB_RUN_ID"]),
            int(environment["GITHUB_RUN_ATTEMPT"]),
        )
    except (KeyError, ValueError) as error:
        raise NoReuse(f"the run's environment does not name the repository and run ({type(error).__name__}: {error})") from error


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
        if time.monotonic() >= deadline:
            raise ApiError(f"GET {where} answered too late: {expired}")
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


def key_parts(root: Path, config: dict[str, Any], group: str) -> dict[str, Any]:
    """The inputs of the reuse key of ``group`` for the checkout at ``root``."""
    try:
        parts: dict[str, Any] = {"runner": config["runner"], "python": config["python"], "group": {group: config["groups"][group]}}
    except KeyError as error:
        raise NoReuse(f"the map names no {error}, so the reuse key cannot be read") from error
    try:
        parts["tree"] = git("rev-parse", "HEAD^{tree}", cwd=root).strip()
    except CalledProcessError as error:
        raise NoReuse("the tree of the checkout cannot be read") from error
    for name, path in KEY_FILES.items():
        try:
            parts[name] = git("rev-parse", f"HEAD:{path}", cwd=root).strip()
        except CalledProcessError as error:
            raise NoReuse(f"{path} is not in the checked-out commit, so the reuse key cannot be read") from error
    return parts


def reuse_key(root: Path, config: dict[str, Any], group: str) -> str:
    """The SHA-256 of the reuse key inputs of ``group``."""
    text = json.dumps(key_parts(root, config, group), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


def job_names(config: dict[str, Any], group: str, key: str | None) -> dict[int, str]:
    """The name of each job of ``group`` that runs, by shard (1 for a group without shards), with the key when it could be read."""
    definition = config["groups"][group]
    suffix = f" (key {key[:KEY_LENGTH]})" if key else ""
    shards = definition.get("shards")
    if not shards:
        return {1: definition["job"] + suffix}
    return {shard: definition["job"] + definition["shard_name"].format(shard=shard, shards=shards) + suffix for shard in range(1, shards + 1)}


def own_runs(api: Api, context: Context, query: str) -> list[dict[str, Any]]:
    """The push runs of this repository's own workflow on main among the answer for ``query``."""
    answer = api(f"repos/{context.repository}/actions/workflows/ci.yml/runs?{query}")
    return [
        run
        for run in answer["workflow_runs"]
        if run["path"] == WORKFLOW
        and run["event"] == "push"
        and run["head_branch"] == "main"
        and run["repository"]["id"] == context.repository_id
        and run["head_repository"]["id"] == context.repository_id
    ]


def prove_start(api: Api, context: Context, start: str) -> None:
    """Raise ``NoReuse`` unless the latest push run of ``start`` concluded success."""
    runs = [run for run in own_runs(api, context, f"head_sha={start}&event=push&branch=main&per_page=100") if run["head_sha"] == start]
    if not runs:
        raise NoReuse(f"the start commit {start} has no push run of {WORKFLOW} on main")
    latest = max(runs, key=lambda run: (str(run["created_at"]), int(run["run_number"])))
    if latest["status"] != "completed" or latest["conclusion"] != "success":
        raise NoReuse(f"the run {latest['id']} of the start commit {start} is {latest['status']} and concluded {latest['conclusion']}")


def passed_jobs(api: Api, context: Context, tree: str, runner: str) -> dict[str, str]:
    """The names of the jobs that passed in earlier runs with ``tree``, each with the run that proves it."""
    runs = [run for run in own_runs(api, context, "branch=main&event=push&per_page=100") if run["head_commit"]["tree_id"] == tree]
    passed: dict[str, str] = {}
    for run in runs[:CANDIDATE_RUNS]:
        answer = api(f"repos/{context.repository}/actions/runs/{run['id']}/jobs?filter=all&per_page=100")
        jobs = answer["jobs"]
        if answer["total_count"] != len(jobs):
            continue
        for job in jobs:
            current = run["id"] == context.run_id and job["run_attempt"] == context.run_attempt
            if current or job["name"] in passed:
                continue
            if job["status"] == "completed" and job["conclusion"] == "success" and job["head_sha"] == run["head_sha"] and runner in job["labels"]:
                passed[job["name"]] = f"{run['html_url']} (attempt {job['run_attempt']})"
    return passed
