"""Place named Git-ignored runner files in the runner folder of the deployed commit on the build and test host.

Run from the repository root under the project profile in ``.env``, through ``make``:

``make host-runners-place RUNNERS="tests/e2e/silver_signals.py tests/e2e/stream_host.py"`` sends each named file over SSH into
``/opt/platform/runner/<commit>/<the same path>`` and prints and records its SHA-256 (``runs/<run id>/runner_files.json``). The deploy (``make host-up``)
makes the folder from ``git archive`` of the commit, which holds no Git-ignored file; this places the runners that it cannot.

The hashes of every runner placed in the folder are also written on the host, in ``placed_runners.json`` in the folder's root (``{"commit": ..., "files":
{path: sha256}}``, read-only to its owner), so that a runner on the host can check its own file. The deploy's source manifest ``runner_manifest.json`` lists
the files of ``git archive`` only and is never touched here: no command names it and no path of its name passes the path form.

Only one kind of file goes: a Python file directly under ``tests/e2e/`` that the deployed commit does not hold, that is a plain file (no link), is text of
at most 1 MiB and that the privacy scan passes. Nothing else is uploaded, and the host's folder must be the finished folder of the commit (its marker
file). A new deploy of another commit removes the folder and so every placed runner: place them again after it. A second deploy of the same commit keeps
them.

Every wait has a deadline and fails with a message that names what did not happen.
"""

from __future__ import annotations

import hashlib
import json
import re
import shlex
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

from scripts.infra import host, state_bootstrap
from scripts.infra.host import FixedHostError, HostError
from scripts.infra.terraform_wrapper import WrapperError, load_run, state_json
from scripts.ingestion.closed import QuietParser
from scripts.ingestion.notice import describe_error
from scripts.ingestion.output_file import replace_owned_file
from scripts.process import CompletedProcess, TimeoutExpired, run_command
from scripts.project_config import ROOT, ConfigurationError, load_configuration
from scripts.quality.repo_checks import PRIVACY_PATTERNS, env_values, line_privacy

# The runner folder and marker of the Ansible role runner_checkout (ansible/group_vars/all.yml); a fast test holds them equal.
RUNNER_DIRECTORY = "/opt/platform/runner"
MARKER = ".runner_checkout.json"
MANIFEST_NAME = "runner_manifest.json"
PLACED_RECORD = "placed_runners.json"
RECORD_FILE = "runner_files.json"
RUNNER_FORM = re.compile(r"tests/e2e/[a-z][a-z0-9_]*\.py")
COMMIT_FORM = re.compile(r"[0-9a-f]{40}")
DIGEST_FORM = re.compile(r"[0-9a-f]{64}")
MAX_BYTES = 1024 * 1024
SSH_SECONDS = 60
GIT_SECONDS = 30
NO_FOLDER_CODE = 3
NO_FOLDER_WORD = "no_runner_folder"

Run = Callable[..., CompletedProcess[str]]


def require_commit(commit: str) -> None:
    if COMMIT_FORM.fullmatch(commit) is None:
        raise FixedHostError("the commit is not 40 lowercase hexadecimal digits")


def git_succeeds(root: Path, env: Mapping[str, str] | None, *arguments: str) -> bool:
    return run_command("git", list(arguments), cwd=root, env=env, timeout=GIT_SECONDS).returncode == 0


def runner_text(relative: str, commit: str, root: Path = ROOT, env: Mapping[str, str] | None = None) -> str:
    """The text of one runner file, after every check: its path form, a plain file in the folder, its size and form, that the commit does not hold it and
    that the privacy scan passes. The file's name is on the line "checking <path>" that comes first and a refusal is fixed text (the path, the content and the
    matched value are never in it; the scan's finding is a line of its own on standard output, with the kind and the line number only)."""
    sys.stdout.write(f"checking {relative}\n")
    require_commit(commit)
    if RUNNER_FORM.fullmatch(relative) is None:
        raise FixedHostError("a path is not one Python runner: it must be one lower-case .py file directly under tests/e2e/")
    path = root / relative
    if path.is_symlink() or not path.is_file() or path.resolve() != root.resolve() / relative:
        raise FixedHostError("the file just named is not a plain file (it is missing, a folder or a link, or a link leads to it)")
    if path.stat().st_size > MAX_BYTES:
        raise FixedHostError("the file just named is too large: at most 1 MiB goes to the host")
    data = path.read_bytes()
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        text = ""
    if not text or b"\0" in data:
        raise FixedHostError("the file just named is not text: an empty file, a file with a NUL byte or a file that is not UTF-8 does not go to the host")
    if not git_succeeds(root, env, "cat-file", "-e", f"{commit}^{{commit}}"):
        raise FixedHostError("the commit is not in this repository: deploy it first")
    if git_succeeds(root, env, "cat-file", "-e", f"{commit}:{relative}"):
        raise FixedHostError("the file just named is already in the commit, so the deploy placed it: this places only files that the commit does not hold")
    names = {kind: description for kind, (description, _) in PRIVACY_PATTERNS.items()} | {"env-value": "a value declared in .env"}
    values = env_values()
    for number, line in enumerate(text.splitlines(), 1):
        kinds = line_privacy(line, values)
        if kinds:
            sys.stdout.write(f"{relative}:{number}: {', '.join(names[kind] for kind in kinds)}\n")
            raise FixedHostError(
                "the file just named holds personal or environment-specific data (the line above says the kind and the line): take it out of the runner"
            )
    return text


def remote_command(commit: str, relative: str) -> str:
    """The one command that receives a file on its standard input: in the commit's finished folder only, to a private temporary name, then the final name."""
    name, parent = shlex.quote(relative), shlex.quote(relative.rsplit("/", 1)[0])
    return f"{folder_guard(commit)}umask 077; mkdir -p -- {parent}; cat > {name}.part; mv -f -- {name}.part {name}; sha256sum -- {name}"


def folder_guard(commit: str) -> str:
    """The start of every remote command: stop with a fixed word when the commit's folder has no finished marker, then work in the folder."""
    folder = f"{RUNNER_DIRECTORY}/{commit}"
    return f"set -eu; test -f {folder}/{MARKER} || {{ echo {NO_FOLDER_WORD}; exit {NO_FOLDER_CODE}; }}; cd {folder}; "


def read_record_command(commit: str) -> str:
    return f"{folder_guard(commit)}if test -f {PLACED_RECORD}; then cat -- {PLACED_RECORD}; fi"


def write_record_command(commit: str) -> str:
    """Receive the record on standard input: a private temporary name made read-only by the umask, then the final name, then its hash."""
    return f"{folder_guard(commit)}umask 277; cat > {PLACED_RECORD}.part; mv -f -- {PLACED_RECORD}.part {PLACED_RECORD}; sha256sum -- {PLACED_RECORD}"


def ssh_call(run: Run, options: Sequence[str], address: str, command: str, input_text: str | None, what: str) -> str:
    """One command on the host: its output, or a refusal with fixed text for a deadline, a missing folder or a failure."""
    try:
        result = run("ssh", [*options, f"{host.HOST_USER}@{address}", command], timeout=SSH_SECONDS, input_text=input_text)
    except TimeoutExpired:
        raise FixedHostError(f"{what} did not finish within its deadline of 60 seconds") from None
    if result.returncode == NO_FOLDER_CODE and result.stdout.strip() == NO_FOLDER_WORD:
        raise FixedHostError("the host has no runner folder for this commit: run make host-up for this commit first")
    if result.returncode != 0:
        raise FixedHostError(f"{what} failed: exit status {result.returncode}")
    return result.stdout


def placed_before(text: str, commit: str) -> dict[str, str]:
    """The hashes in the host's record of this commit, or none when there is no record or it names another commit. Anything else is refused."""
    if not text.strip():
        return {}
    not_valid = FixedHostError("the host's placed_runners.json is not valid: remove it on the host and place the runners again")
    try:
        document = json.loads(text)
    except ValueError:
        raise not_valid from None
    if not isinstance(document, dict) or sorted(document) != ["commit", "files"] or not isinstance(document["files"], dict):
        raise not_valid
    if document["commit"] != commit:
        return {}
    files: dict[str, str] = document["files"]
    if any(RUNNER_FORM.fullmatch(name) is None or not isinstance(digest, str) or DIGEST_FORM.fullmatch(digest) is None for name, digest in files.items()):
        raise not_valid
    return dict(files)


def place(
    run_id: str,
    address: str,
    commit: str,
    relatives: Sequence[str],
    *,
    root: Path = ROOT,
    env: Mapping[str, str] | None = None,
    run: Run = run_command,
    ssh_options: Sequence[str] | None = None,
) -> dict[str, str]:
    """Send each named file and return its SHA-256 as the host computed it, then write the host's record of every placed runner.
    Every file is checked before the first is sent, and the host's earlier record is read and checked before the first is sent too."""
    if not relatives:
        raise FixedHostError("the call names no runner file")
    if len(set(relatives)) != len(relatives):
        raise FixedHostError("a runner file is named twice")
    texts = {relative: runner_text(relative, commit, root, env) for relative in relatives}
    options = list(ssh_options) if ssh_options is not None else host.ssh_arguments(run_id)
    earlier = placed_before(ssh_call(run, options, address, read_record_command(commit), None, "reading the host's record of placed runners"), commit)
    hashes: dict[str, str] = {}
    for relative, text in texts.items():
        words = ssh_call(run, options, address, remote_command(commit, relative), text, "sending a runner file").split()
        local = hashlib.sha256(text.encode()).hexdigest()
        if len(words) != 2 or words[0] != local or words[1] != relative:
            raise FixedHostError("the host's copy of a runner file differs from the one sent: remove it on the host and place it again")
        hashes[relative] = local
    record = json.dumps({"commit": commit, "files": dict(sorted((earlier | hashes).items()))}, indent=2) + "\n"
    words = ssh_call(run, options, address, write_record_command(commit), record, "writing the host's record of placed runners").split()
    if words != [hashlib.sha256(record.encode()).hexdigest(), PLACED_RECORD]:
        raise FixedHostError("the host's record of placed runners differs from the one sent: remove it on the host and place the runners again")
    return hashes


def record_files(folder: Path, commit: str, hashes: Mapping[str, str]) -> Path:
    """Write the run's record of the placed files over its earlier one, keeping the earlier entries only for the same commit."""
    target = folder / RECORD_FILE
    files: dict[str, str] = {}
    if target.is_file():
        earlier = json.loads(target.read_text())
        if earlier.get("commit") == commit:
            files = dict(earlier["files"])
    files |= hashes
    replace_owned_file(target, json.dumps({"commit": commit, "files": files}, indent=2, sort_keys=True) + "\n", state_root=folder, mode=0o644)
    return target


def host_runners_place(relatives: Sequence[str], binding: Path) -> int:
    configuration = load_configuration()
    run = load_run(binding)
    if run.backend == "local" or not run.provision_host:
        raise FixedHostError("this run has no host that Terraform created: nothing to place the runners on")
    commit = host.platform_commit()
    host_name = f"platform-{configuration['ENVIRONMENT']}"
    instance = host.instance_id(state_json(run, configuration))
    address = host.host_peer(host_name).address
    host.keep_host_keys_of(run.run_id, instance)
    found = host.ssh_output(run.run_id, address, host.INSTANCE_ID_COMMAND, host.SSH_ATTEMPT_SECONDS)
    if found is None:
        raise FixedHostError(f"the Tailscale peer {host_name} did not answer over SSH: run make host-start if the host is stopped")
    host.require_instance(found, instance, host_name)
    hashes = place(run.run_id, address, commit, relatives)
    folder = ROOT / "runs" / run.run_id
    folder.mkdir(parents=True, exist_ok=True)
    record_files(folder, commit, hashes)
    for relative, digest in hashes.items():
        sys.stdout.write(f"placed {relative} sha256 {digest}\n")
    sys.stdout.write(
        f"run {run.run_id}: {len(hashes)} runner file(s) are in {RUNNER_DIRECTORY}/{commit} on {host_name}; the record is runs/{run.run_id}/{RECORD_FILE}\n"
    )
    return 0


def main() -> int:
    """Place the named runner files and return a non-zero code on a refusal, an expired deadline or a failure."""
    parser = QuietParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("runners", nargs="+", help="each runner file, relative to the repository's root: one .py file directly under tests/e2e/")
    parser.add_argument("--binding", type=Path, default=state_bootstrap.DEFAULT_BINDING, help="the run binding file; the default is runs/run_binding.json")
    arguments = parser.parse_args()
    try:
        return host_runners_place(arguments.runners, arguments.binding)
    except (ConfigurationError, WrapperError, HostError) as error:
        sys.stderr.write(f"{describe_error(error)}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
