"""Place named Git-ignored runner files in the runner folder of the deployed commit on the build and test host.

Run from the repository root under the project profile in ``.env``, through ``make``:

``make host-runners-place RUNNERS="tests/e2e/silver_signals.py tests/e2e/stream_host.py"`` sends each named file over SSH into
``/opt/platform/runner/<commit>/<the same path>`` and prints its SHA-256 as the host computed it. The names reach the program as data: the raw text of
the variable ``RUNNERS``, which Make does not expand, arrives in ``HOST_RUNNER_NAMES`` and is split on white space in Python, so neither Make nor a shell
expands a name. The run's record ``runs/<run id>/runner_files.json`` is the host's own record after the call. The deploy (``make host-up``) makes the folder
from ``git archive`` of the commit, which holds no Git-ignored file; this places the runners that it cannot.

The hashes of every runner placed in the folder are also written on the host, in ``placed_runners.json`` in the folder's root (``{"commit": ..., "files":
{path: sha256}}``, read-only to its owner), so that a runner on the host can check its own file. A runner that an earlier call placed is hashed on the
host before the record is written, and the record holds only hashes that the host computed. The deploy's source manifest ``runner_manifest.json`` lists
the files of ``git archive`` only and is never touched here: no command names it and no path of its name passes the path form.

Only these files go: a Python file directly under ``tests/e2e/``, and a Python or ``.yml`` file directly under ``tests/e2e/stream_stack/`` (the probes
that the streaming run mounts in its containers, and its Compose overlay; only a ``.yml`` name may hold dots), that Git ignores, that the deployed commit
does not hold, that is a plain file with one name (no link, no hard link), is text of at most 1 MiB, and that the privacy scan and the pinned credential
scanner (gitleaks) both pass over the bytes that are sent. The imports of every Python file, followed through the commit's own files, must resolve. For a
file directly under ``tests/e2e/`` every package must be one that the commit's lock files pin. A probe runs in the replay or the Spark image, with its folder
on the import path: the packages that it reaches through the repository's modules come from the image and are not asked of those lock files (they are the
host environment's), another probe of the same call resolves by its name (a name taken from it must be at its top level, and it has no submodules), and a
package that a probe names itself must still be pinned. A ``.yml`` file is not read as Python. Nothing else is uploaded.

The host's folder, and the runner folder above it, must be real folders (no link) and the commit's folder must hold the finished marker of the commit; every
folder from there to the file is made when it is missing and refused when it is a link; each file is received into a file that ``mktemp`` makes in the
destination folder and is then moved over its final name. Before an earlier file is hashed again, the folders on the way to it pass the same check, and none
is made. A new deploy of another commit removes the folder and so every placed runner: place them again after it. A second deploy of the same commit keeps
them.

Every wait has a deadline and fails with a message that names what did not happen.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import stat
import sys
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from scripts.infra import host, state_bootstrap
from scripts.infra.host import FixedHostError, HostError
from scripts.infra.runner_imports import LOADED_BY_PATH, OverlaySource, closure, locked_distributions, sibling_problems, unlocked
from scripts.infra.terraform_wrapper import WrapperError, load_run, state_json
from scripts.ingestion.closed import QuietParser
from scripts.ingestion.notice import describe_error
from scripts.ingestion.output_file import replace_owned_file
from scripts.process import CompletedProcess, TimeoutExpired, run_command
from scripts.project_config import ROOT, ConfigurationError, load_configuration
from scripts.quality.repo_checks import PRIVACY_PATTERNS, env_values, line_privacy
from scripts.stack.yaml_text import load_yaml

# The runner folder and marker of the Ansible role runner_checkout (ansible/group_vars/all.yml); a fast test holds them equal.
RUNNER_DIRECTORY = "/opt/platform/runner"
MARKER = ".runner_checkout.json"
# The fields of the marker that the role writes (``runner_checkout_expected`` in ansible/group_vars/all.yml; a fast test holds them equal).
MARKER_FIELDS = ("commit", "python", "python_archive_sha256", "requirements")
# The forms that the role's first task asserts for the interpreter pin and for each requirements path (a fast test holds them equal).
PYTHON_FORM = re.compile(r"3\.[0-9]+\.[0-9]+")
LOCK_FORM = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]*(/[A-Za-z0-9_][A-Za-z0-9_.-]*)*")
# The program that the host's Python runs on the marker: standard JSON (no NaN or Infinity), one object, exactly the fields of MARKER_FIELDS (none repeated),
# this commit, and a pin, a digest and requirements paths of the forms that the role writes.
MARKER_CHECK = "\n".join(
    (
        "import json, re, sys",
        "def only(pairs):",
        "    names = [name for name, _ in pairs]",
        "    if len(set(names)) != len(names):",
        "        raise ValueError(names)",
        "    return dict(pairs)",
        "def refuse(constant):",
        "    raise ValueError(constant)",
        "def formed(value, form):",
        "    return isinstance(value, str) and re.fullmatch(form, value) is not None",
        "try:",
        "    with open(sys.argv[1], encoding='utf-8') as handle:",
        "        document = json.load(handle, object_pairs_hook=only, parse_constant=refuse)",
        "except (OSError, ValueError):",
        "    sys.exit(1)",
        f"fields = {MARKER_FIELDS!r}",
        "good = isinstance(document, dict) and sorted(document) == sorted(fields) and document['commit'] == sys.argv[2]",
        f"good = good and formed(document['python'], {PYTHON_FORM.pattern!r}) and formed(document['python_archive_sha256'], '[0-9a-f]{{64}}')",
        "good = good and isinstance(document['requirements'], list) and len(document['requirements']) > 0",
        f"good = good and all(formed(name, {LOCK_FORM.pattern!r}) for name in document['requirements'])",
        "sys.exit(0 if good else 1)",
        "",
    )
)
MANIFEST_NAME = "runner_manifest.json"
PLACED_RECORD = "placed_runners.json"
RECORD_FILE = "runner_files.json"
GROUP_VARIABLES = "ansible/group_vars/all.yml"
# The names that the Makefile passes: the raw text of its variable RUNNERS.
NAMES_VARIABLE = "HOST_RUNNER_NAMES"
# The folder of the streaming run's probes and their Compose overlay. The containers mount it and import its probes by their bare names.
PROBE_FOLDER = "tests/e2e/stream_stack/"
# The one form of a path that is sent and recorded, with nothing deeper: a .py file directly under tests/e2e/, or a .py or .yml file directly under
# PROBE_FOLDER (compose.host.yml has a dot in its name; a dotted .py name could not be imported). The host's reader of placed_runners.json holds the same form.
RUNNER_FORM = re.compile(r"tests/e2e/(?:[a-z][a-z0-9_]*\.py|stream_stack/(?:[a-z][a-z0-9_]*\.py|[a-z][a-z0-9_]*(?:\.[a-z0-9_]+)*\.yml))")
COMMIT_FORM = re.compile(r"[0-9a-f]{40}")
DIGEST_FORM = re.compile(r"[0-9a-f]{64}")
MAX_BYTES = 1024 * 1024
SSH_SECONDS = 60
GIT_SECONDS = 30
SCAN_SECONDS = 60
# The pinned credential scanner that the commit hook runs (.pre-commit-config.yaml, installed by scripts/quality/install_tools.py).
GITLEAKS = ROOT / ".tools" / "bin" / "gitleaks"
# The one configuration of the scan: the scanner's built-in rules and nothing from the caller's surroundings.
GITLEAKS_CONFIGURATION = 'title = "runner placement scan"\n\n[extend]\nuseDefault = true\n'
SCAN_PATH = "/usr/bin:/bin"
NO_FOLDER_CODE = 3
NO_FOLDER_WORD = "no_runner_folder"
LINKED_CODE = 5
LINKED_WORD = "runner_folder_is_a_link"
RETAINED_CODE = 4
RETAINED_WORD = "retained_runner_is_not_a_plain_file"

Run = Callable[..., CompletedProcess[str]]


@dataclass(frozen=True)
class Placement:
    """What a call placed (its files, with the host's hashes) and the host's whole record after it (those files and the verified earlier ones)."""

    placed: dict[str, str]
    record: dict[str, str]


def not_plain() -> FixedHostError:
    return FixedHostError("the file just named is not a plain file (it is missing, a folder, a link or another special file, or a link leads to it)")


def require_commit(commit: str) -> None:
    if COMMIT_FORM.fullmatch(commit) is None:
        raise FixedHostError("the commit is not 40 lowercase hexadecimal digits")


def git_succeeds(root: Path, env: Mapping[str, str] | None, *arguments: str) -> bool:
    return run_command("git", list(arguments), cwd=root, env=env, timeout=GIT_SECONDS).returncode == 0


def read_plain_file(path: Path, before: os.stat_result) -> bytes:
    """The bytes of a plain file with one name, read through one descriptor that does not follow a link and that is the file that was looked at: a file
    that was swapped for a link, a pipe or another file after the look is refused."""
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    except OSError:
        raise not_plain() from None
    try:
        after = os.fstat(descriptor)
        if not stat.S_ISREG(after.st_mode) or (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino):
            raise not_plain()
        if after.st_nlink != 1:
            raise FixedHostError("the file just named has more than one name (a hard link): copy it to a file of its own")
        if after.st_size > MAX_BYTES:
            raise FixedHostError("the file just named is too large: at most 1 MiB goes to the host")
        data = b""
        while len(data) <= MAX_BYTES:
            block = os.read(descriptor, MAX_BYTES + 1 - len(data))
            if not block:
                break
            data += block
    finally:
        os.close(descriptor)
    if len(data) > MAX_BYTES:
        raise FixedHostError("the file just named is too large: at most 1 MiB goes to the host")
    return data


def scan_credentials(data: bytes) -> None:
    """Run the pinned credential scanner over the exact bytes that go to the host. A finding, a missing scanner, a scanner that cannot be started and a
    scanner that fails are refused with fixed text: the scanner's own output is never shown. The scanner gets nothing from the caller's surroundings: its own
    configuration (the built-in rules, in ``GITLEAKS_CONFIG_TOML``, which comes before a ``.gitleaks.toml`` in the working folder), a working folder of its
    own that holds no other file, an environment of only a search path, that folder and the configuration, and no way to skip a finding by a comment in the
    file (``--ignore-gitleaks-allow``), so a variable such as ``GITLEAKS_CONFIG``, a ``.gitleaks.toml`` or ``.gitleaksignore`` in the working folder and a
    ``gitleaks:allow`` comment change nothing (the precedence of its configuration is in ``gitleaks stdin --help``)."""
    if not GITLEAKS.is_file():
        raise FixedHostError("the pinned credential scanner is not installed (.tools/bin/gitleaks): run make setup")
    try:
        with tempfile.TemporaryDirectory(prefix="gitleaks_") as folder:
            arguments = ["stdin", "--gitleaks-ignore-path", folder, "--ignore-gitleaks-allow", "--redact", "--no-banner", "--log-level", "error"]
            environment = {"PATH": SCAN_PATH, "HOME": folder, "GITLEAKS_CONFIG_TOML": GITLEAKS_CONFIGURATION}
            result = run_command(str(GITLEAKS), arguments, input_text=data.decode(), timeout=SCAN_SECONDS, cwd=Path(folder), env=environment)
    except TimeoutExpired:
        raise FixedHostError("the credential scan did not finish within its deadline of 60 seconds") from None
    except OSError:
        raise FixedHostError("the credential scanner cannot be started: run make setup to install the pinned one again") from None
    if result.returncode == 1:
        raise FixedHostError("the credential scanner found a possible secret in the file just named: take it out of the runner")
    if result.returncode != 0:
        raise FixedHostError(f"the credential scan failed: exit status {result.returncode}")


def runner_text(relative: str, commit: str, root: Path = ROOT, env: Mapping[str, str] | None = None) -> str:
    """The text of one runner file, after every check: its path form, a plain file with one name in the folder, that Git ignores it and the commit does not hold
    it, its size and form, the privacy scan and the credential scan, both over the bytes that go. It prints nothing but the privacy scan's finding (a line of
    its own on standard output, with the kind and the line number only), and a refusal is fixed text (the path, the content and the matched value are never in
    it). ``place`` prints "checked <path>" after the imports of every file have passed too."""
    require_commit(commit)
    if RUNNER_FORM.fullmatch(relative) is None:
        raise FixedHostError(
            "a path is not one runner file: it must be one lower-case .py file directly under tests/e2e/, "
            "or one lower-case .py or .yml file directly under tests/e2e/stream_stack/"
        )
    path = root / relative
    try:
        before = path.lstat()
    except OSError:
        raise not_plain() from None
    if not stat.S_ISREG(before.st_mode) or path.resolve() != root.resolve() / relative:
        raise not_plain()
    if before.st_nlink != 1:
        raise FixedHostError("the file just named has more than one name (a hard link): copy it to a file of its own")
    if not git_succeeds(root, env, "cat-file", "-e", f"{commit}^{{commit}}"):
        raise FixedHostError("the commit is not in this repository: deploy it first")
    if git_succeeds(root, env, "cat-file", "-e", f"{commit}:{relative}"):
        raise FixedHostError("the file just named is already in the commit, so the deploy placed it: this places only files that the commit does not hold")
    if not git_succeeds(root, env, "check-ignore", "-q", "--", relative):
        raise FixedHostError("the file just named is not ignored by Git: this places only Git-ignored runner files")
    data = read_plain_file(path, before)
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        text = ""
    if not text or b"\0" in data:
        raise FixedHostError("the file just named is not text: an empty file, a file with a NUL byte or a file that is not UTF-8 does not go to the host")
    names = {kind: description for kind, (description, _) in PRIVACY_PATTERNS.items()} | {"env-value": "a value declared in .env"}
    values = env_values()
    for number, line in enumerate(text.splitlines(), 1):
        kinds = line_privacy(line, values)
        if kinds:
            sys.stdout.write(f"{relative}:{number}: {', '.join(names[kind] for kind in kinds)}\n")
            raise FixedHostError(
                "the file just named holds personal or environment-specific data (the line above says the kind and the line): take it out of the runner"
            )
    scan_credentials(data)
    return text


class CommitSource:
    """The files of one commit, read from Git's objects: the files that the deploy's ``git archive`` puts on the host."""

    def __init__(self, commit: str, root: Path, env: Mapping[str, str] | None) -> None:
        self.commit, self.root, self.env = commit, root, env
        self.found: dict[str, str | None] = {}

    def read(self, relative: str) -> str | None:
        if relative not in self.found:
            result = run_command("git", ["cat-file", "blob", f"{self.commit}:{relative}"], cwd=self.root, env=self.env, timeout=GIT_SECONDS, keep_newlines=True)
            self.found[relative] = os.fsencode(result.stdout).decode("utf-8", "replace") if result.returncode == 0 else None
        return self.found[relative]


def lock_files(source: CommitSource) -> list[str]:
    """The lock files that the commit's runner checkout installs (``runner_checkout_requirements`` in the commit's group variables)."""
    text = source.read(GROUP_VARIABLES)
    document: Any = load_yaml(text) if text is not None else None
    names = document.get("runner_checkout_requirements") if isinstance(document, dict) else None
    if not isinstance(names, list) or not names or any(not isinstance(name, str) or LOCK_FORM.fullmatch(name) is None for name in names):
        raise FixedHostError(
            "the commit does not name the lock files of the runner checkout: its ansible/group_vars/all.yml has no valid runner_checkout_requirements"
        )
    return [str(name) for name in names]


def check_imports(texts: Mapping[str, str], commit: str, root: Path = ROOT, env: Mapping[str, str] | None = None) -> None:
    """Refuse a runner whose imports, followed through the commit's own files, take a package that the commit's lock files do not pin. The runners are read
    as sent (the exact text), every other file as the deploy archived it. A module that a runner loads at run time by its path is read through LOADED_BY_PATH,
    and a file under tests/e2e/ that loads one and is not there is refused, because its imports cannot be read.

    The probes (the .py files of PROBE_FOLDER) are followed on their own. Their imports must resolve in the same way, but only the packages that a probe names
    itself are asked of the lock files: the repository's modules that it imports run with the image's packages. Another probe of this call resolves by its bare
    name, for the probes only, against the text that is sent: a name that a probe takes from it must be defined or imported at its top level, and it has no
    submodules. What a file for the host reaches is checked in full, also when a probe of the same call reaches it too. A .yml file is not Python and has no
    imports."""
    source = CommitSource(commit, root, env)
    locks = []
    for name in lock_files(source):
        lock = source.read(name)
        if lock is None:
            raise FixedHostError("the commit does not hold a lock file that the runner checkout installs")
        locks.append(lock)
    programs = [relative for relative in texts if relative.endswith(".py")]
    probes = {relative for relative in programs if relative.startswith(PROBE_FOLDER)}
    hosted = [relative for relative in programs if relative not in probes]
    overlay = OverlaySource(source, texts)
    on_host = closure(overlay, [*hosted, *(module for relative in hosted for module in LOADED_BY_PATH.get(relative, ()))])
    in_images = closure(overlay, sorted(probes))
    siblings = {PurePosixPath(relative).stem: relative for relative in probes}
    taken = {name: set(files) for name, files in on_host.third_party.items()}
    for name, files in in_images.third_party.items():
        if name not in siblings and files & probes:
            taken.setdefault(name, set()).update(files & probes)
    dynamic = on_host.dynamic | in_images.dynamic
    problems = [
        *dict.fromkeys([*on_host.missing, *in_images.missing]),
        *dict.fromkeys([*on_host.unreadable, *in_images.unreadable]),
        *sibling_problems(overlay, siblings),
        *(
            f"{name}: loads a module at run time that the import check cannot read"
            for name in sorted(d for d in dynamic if d.startswith("tests/e2e/") and d not in LOADED_BY_PATH)
        ),
        *unlocked(taken, locked_distributions(locks)),
    ]
    if problems:
        sys.stdout.writelines(f"{line}\n" for line in problems)
        raise FixedHostError(
            "a runner takes a module or a package that the runner checkout would not hold, or a file that is not valid Python (the lines above name each): "
            "add the package to the lock file and deploy again, name a probe that another probe imports in the same call, or fix the file"
        )


def runner_folder_guard(commit: str, directory: str = RUNNER_DIRECTORY) -> str:
    """The start of every remote command: stop with a fixed word when the runner folder or the commit's folder is a link or is no folder, or when the commit's
    folder has no finished marker that names this commit (the marker is parsed as the JSON that the role writes: no text that merely holds the commit passes),
    then work in the folder."""
    folder = f"{directory}/{commit}"
    marker = f"{folder}/{MARKER}"
    return (
        "set -eu; "
        f'for d in {shlex.quote(directory)} {shlex.quote(folder)}; do if test -L "$d"; then echo {LINKED_WORD}; exit {LINKED_CODE}; fi; done; '
        f"if ! test -d {shlex.quote(folder)}; then echo {NO_FOLDER_WORD}; exit {NO_FOLDER_CODE}; fi; "
        f"if test -L {shlex.quote(marker)} || ! test -f {shlex.quote(marker)} "
        f"|| ! python3 -I -c {shlex.quote(MARKER_CHECK)} {shlex.quote(marker)} {shlex.quote(commit)}; "
        f"then echo {NO_FOLDER_WORD}; exit {NO_FOLDER_CODE}; fi; "
        f"cd -- {shlex.quote(folder)}; "
    )


def parent_folders(relative: str) -> list[str]:
    """The folders on the way to a runner file, from the first one below the commit's folder to the one that holds it."""
    parts = relative.split("/")
    return ["/".join(parts[:end]) for end in range(1, len(parts))]


def folder_step(folder: str, when_missing: str) -> str:
    """Stop with the fixed word when a folder on the way to a runner file is a link or is no folder; run ``when_missing`` when nothing is there."""
    return (
        f"p={shlex.quote(folder)}; "
        f'if test -L "$p"; then echo {LINKED_WORD}; exit {LINKED_CODE}; fi; '
        f'if test -e "$p"; then test -d "$p" || {{ echo {LINKED_WORD}; exit {LINKED_CODE}; }}; else {when_missing}; fi;'
    )


def remote_command(commit: str, relative: str, directory: str = RUNNER_DIRECTORY) -> str:
    """The one command that receives a file on its standard input: in the commit's finished folder only, into a file that ``mktemp`` makes in the destination
    folder with an unpredictable name and an exclusive create, then moved over the final name. Each folder on the way, from the first to the destination, is
    made by the SSH user for itself only when it is missing; one that is a link or is no folder, or a final name that is a folder, stops the command."""
    parent, name = relative.rsplit("/", 1)[0], shlex.quote(relative)
    steps = " ".join(folder_step(folder, 'mkdir -- "$p"') for folder in parent_folders(relative))
    return (
        f"{runner_folder_guard(commit, directory)}umask 077; {steps} "
        f'tmp=$(mktemp {shlex.quote(parent + "/.runner.XXXXXXXXXX")}); trap \'rm -f -- "$tmp"\' EXIT; cat > "$tmp"; '
        f'if test -d {name}; then echo {LINKED_WORD}; exit {LINKED_CODE}; fi; mv -f -- "$tmp" {name}; trap - EXIT; sha256sum -- {name}'
    )


def read_record_command(commit: str, directory: str = RUNNER_DIRECTORY) -> str:
    return f"{runner_folder_guard(commit, directory)}if test -f {PLACED_RECORD} && ! test -L {PLACED_RECORD}; then cat -- {PLACED_RECORD}; fi"


def verify_retained_command(commit: str, relatives: Sequence[str], directory: str = RUNNER_DIRECTORY) -> str:
    """Hash each runner that the host's record keeps from an earlier call. Before any hash, every folder on the way to each one passes the check that the
    placement makes (nothing is made here). Stop with a fixed word when a folder is a link or is no folder, and with another when a runner is missing (with
    its folder too), is a link or is not a plain file."""
    names = " ".join(shlex.quote(relative) for relative in relatives)
    folders = dict.fromkeys(folder for relative in relatives for folder in parent_folders(relative))
    steps = " ".join(folder_step(folder, ":") for folder in folders)
    return (
        f"{runner_folder_guard(commit, directory)}{steps} for f in {names}; do "
        f'if test -L "$f" || ! test -f "$f"; then echo {RETAINED_WORD}; exit {RETAINED_CODE}; fi; done; '
        f"sha256sum -- {names}"
    )


def write_record_command(commit: str, directory: str = RUNNER_DIRECTORY) -> str:
    """Receive the record on standard input: into a file that ``mktemp`` makes in the folder's root, closed to its owner's read, then moved over the final name,
    then its hash."""
    return (
        f'{runner_folder_guard(commit, directory)}umask 077; tmp=$(mktemp .{PLACED_RECORD}.XXXXXXXXXX); trap \'rm -f -- "$tmp"\' EXIT; cat > "$tmp"; '
        f'chmod 0400 "$tmp"; if test -d {PLACED_RECORD}; then echo {LINKED_WORD}; exit {LINKED_CODE}; fi; mv -f -- "$tmp" {PLACED_RECORD}; trap - EXIT; '
        f"sha256sum -- {PLACED_RECORD}"
    )


def ssh_call(run: Run, options: Sequence[str], address: str, command: str, input_text: str | None, what: str) -> str:
    """One command on the host: its output, or a refusal with fixed text for a deadline, a missing or linked folder or a failure."""
    try:
        result = run("ssh", [*options, f"{host.HOST_USER}@{address}", command], timeout=SSH_SECONDS, input_text=input_text)
    except TimeoutExpired:
        raise FixedHostError(f"{what} did not finish within its deadline of 60 seconds") from None
    word = result.stdout.strip()
    if result.returncode == NO_FOLDER_CODE and word == NO_FOLDER_WORD:
        raise FixedHostError("the host has no runner folder for this commit: run make host-up for this commit first")
    if result.returncode == LINKED_CODE and word == LINKED_WORD:
        raise FixedHostError(
            "the host's runner folder, the commit's folder or a folder below it is a link or is no folder: run make host-up for this commit to build it again"
        )
    if result.returncode == RETAINED_CODE and word == RETAINED_WORD:
        raise FixedHostError(
            "a runner that an earlier call placed is missing on the host or is not a plain file: "
            "name it in this call to place it again, or run make host-up to build the folder again"
        )
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


def hashed_lines(text: str, names: Sequence[str]) -> dict[str, str]:
    """The ``sha256sum`` lines of a command as {name: digest}, in the order of the names, or a refusal when a line is not the expected one."""
    pairs = [line.split(None, 1) for line in text.splitlines()]
    if [pair[1].lstrip("*") if len(pair) == 2 else "" for pair in pairs] != list(names) or any(DIGEST_FORM.fullmatch(pair[0]) is None for pair in pairs):
        raise FixedHostError("the host's answer to the hash of the runners is not the one that was asked for")
    return {pair[1].lstrip("*"): pair[0] for pair in pairs}


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
) -> Placement:
    """Send each named file and return its SHA-256 as the host computed it, with the host's record that was then written. Every file is checked, and the imports
    of all of them, before the first is sent, and the paths are printed once all of those checks have passed. The host's earlier record is read and each
    runner that it keeps from an earlier call is hashed on the host before the first is sent too: a kept runner whose file differs from the record is refused,
    and the written record holds only hashes that the host computed."""
    if not relatives:
        raise FixedHostError("the call names no runner file")
    if len(set(relatives)) != len(relatives):
        raise FixedHostError("a runner file is named twice")
    texts = {relative: runner_text(relative, commit, root, env) for relative in relatives}
    check_imports(texts, commit, root, env)
    sys.stdout.writelines(f"checked {relative}\n" for relative in texts)
    options = list(ssh_options) if ssh_options is not None else host.ssh_arguments(run_id)
    earlier = placed_before(ssh_call(run, options, address, read_record_command(commit), None, "reading the host's record of placed runners"), commit)
    kept = [name for name in sorted(earlier) if name not in texts]
    verified: dict[str, str] = {}
    if kept:
        verified = hashed_lines(ssh_call(run, options, address, verify_retained_command(commit, kept), None, "checking the runners placed earlier"), kept)
        if verified != {name: earlier[name] for name in kept}:
            raise FixedHostError(
                "a runner that an earlier call placed differs on the host from the host's record: "
                "name it in this call to place it again, or run make host-up to build the folder again"
            )
    hashes: dict[str, str] = {}
    for relative, text in texts.items():
        words = ssh_call(run, options, address, remote_command(commit, relative), text, "sending a runner file").split()
        local = hashlib.sha256(text.encode()).hexdigest()
        if len(words) != 2 or words[0] != local or words[1] != relative:
            raise FixedHostError("the host's copy of a runner file differs from the one sent: remove it on the host and place it again")
        hashes[relative] = words[0]
    files = dict(sorted((verified | hashes).items()))
    record = json.dumps({"commit": commit, "files": files}, indent=2) + "\n"
    words = ssh_call(run, options, address, write_record_command(commit), record, "writing the host's record of placed runners").split()
    if words != [hashlib.sha256(record.encode()).hexdigest(), PLACED_RECORD]:
        raise FixedHostError("the host's record of placed runners differs from the one sent: remove it on the host and place the runners again")
    return Placement(hashes, files)


def record_files(folder: Path, commit: str, files: Mapping[str, str]) -> Path:
    """Write the run's record of the runners in the host's folder over its earlier one. The files are the host's whole verified record, so nothing of the
    earlier local record is kept."""
    target = folder / RECORD_FILE
    replace_owned_file(
        target, json.dumps({"commit": commit, "files": dict(sorted(files.items()))}, indent=2, sort_keys=True) + "\n", state_root=folder, mode=0o644
    )
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
    placement = place(run.run_id, address, commit, relatives)
    folder = ROOT / "runs" / run.run_id
    folder.mkdir(parents=True, exist_ok=True)
    record_files(folder, commit, placement.record)
    for relative, digest in placement.placed.items():
        sys.stdout.write(f"placed {relative} sha256 {digest}\n")
    sys.stdout.write(
        f"run {run.run_id}: {len(placement.record)} runner file(s) are in {RUNNER_DIRECTORY}/{commit} on {host_name}; "
        f"the record is runs/{run.run_id}/{RECORD_FILE}\n"
    )
    return 0


def runner_names(arguments: argparse.Namespace, environment: Mapping[str, str]) -> list[str]:
    """The named runner files: the arguments, or the words of the variable HOST_RUNNER_NAMES (the raw text of the Makefile's RUNNERS) split on white space
    only, so that no shell expands a name."""
    if arguments.names_from_environment:
        if arguments.runners:
            raise FixedHostError("name the runner files in the arguments or in RUNNERS (HOST_RUNNER_NAMES), not both")
        return environment.get(NAMES_VARIABLE, "").split()
    return list(arguments.runners)


def main() -> int:
    """Place the named runner files and return a non-zero code on a refusal, an expired deadline or a failure."""
    parser = QuietParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument(
        "runners", nargs="*", help="each runner file, relative to the root: a .py file in tests/e2e/, or a .py or .yml file in its stream_stack/"
    )
    parser.add_argument(
        "--names-from-environment",
        action="store_true",
        help=f"read the names from the variable {NAMES_VARIABLE}, split on white space, with no shell expansion",
    )
    parser.add_argument("--binding", type=Path, default=state_bootstrap.DEFAULT_BINDING, help="the run binding file; the default is runs/run_binding.json")
    arguments = parser.parse_args()
    try:
        return host_runners_place(runner_names(arguments, os.environ), arguments.binding)
    except (ConfigurationError, WrapperError, HostError) as error:
        sys.stderr.write(f"{describe_error(error)}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
