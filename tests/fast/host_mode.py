"""The settings, figures and refusals that the host mode of the Silver acceptance runners shares (``tests/e2e/silver_signals.py`` and
``silver_hypotension.py``, both untracked).

A runner in host mode runs natively on the build and test host: its own Compose project, the host's Docker with linux/amd64 images and no emulation, the
pinned files in folders that the caller names, and the Spark image that the host's deploy built (``SPARK_IMAGE``). The mode is on only for the exact value
``1``. In host mode no folder has a default, and a folder must be an absolute path. A settings error names the setting and never its value.

Every wait of a host run has a figure here. The figures are stated, not measured: no run on the host has timed these steps yet. The basis is the one
emulated run that reached the Silver job (the decoder load took 59 seconds under emulation; the Silver job did not finish in 3,500 seconds and was idle,
which is the open diagnostic of the signals card). The host runs the same images natively on 8 vCPUs, so a step that is not stalled is far inside its
limit, and a stalled step is stopped after half of the local wait. The run has a deadline of its own, and every active wait (a build, a job, a health wait, a
Docker command) gets the shorter of its own limit and the rest of the run (``active_cap``), so that no wait goes past the deadline. The cleanup has a budget of
its own (``cleanup_seconds``, ``cleanup_cap``). The whole run therefore ends within ``ceiling_seconds``, however many jobs it holds, apart from the seconds
that a stopped command takes to leave (``KILL_WAIT_SECONDS`` in ``scripts/process.py``). A call to Nessie reads the rest of the run at its own start
(``remaining_seconds``): it gets the shorter of ``call_seconds`` and the rest, and it is not sent when nothing is left.

A given Spark image is bound to the reviewed commit by its content and not by its ID alone: ``SOURCE_SCRIPT`` lists the Git blob ID of each Python file that the
image holds under ``/opt/jobs``, and ``source_problem`` compares the listing with the files that the commit's own Dockerfile copies there (every ``COPY`` into
``/opt/jobs``, read from the Dockerfile at run time, from the files of ``git ls-tree -r HEAD`` that the commit's Docker ignore file
``spark/Dockerfile.dockerignore`` does not leave out): a missing, extra or changed file refuses the image. The record keeps the Boolean only.
A checkout with no Git history (the host's) has no ``git ls-tree``: ``manifest_source`` builds the same inputs from the deploy's ``runner_manifest.json``,
checking each file that it uses against the manifest's SHA-256 and computing its blob ID from the checked bytes.

Every Compose and Docker call of a run gets an explicit environment (``docker_environment``): the path, the home folder, what Docker itself needs and the run's
own binding, never the caller's other settings. Compose lets a variable of the environment beat the env file, so a caller's ``WAREHOUSE_LOCATION`` or
``NESSIE_URI`` would otherwise reach Spark.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

MIB = 1024 * 1024
FOLDER_SETTINGS = ("LANDED_CACHE", "SIGNAL_CASE_CACHE")
# What a Compose or Docker call may take from the caller: where to find the programs, the home folder (the Docker client's plugins and contexts) and the
# settings that choose the daemon. Nothing else: no deployment setting and no ``COMPOSE_*`` setting.
DOCKER_PASSTHROUGH = ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG", "DOCKER_CERT_PATH", "DOCKER_TLS_VERIFY", "XDG_RUNTIME_DIR", "TMPDIR")
IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}")
IMAGE_SOURCE_LINE = re.compile(r"([0-9a-f]{40}) (\S.*)")
TREE_LINE = re.compile(r"[0-7]{6} blob ([0-9a-f]{40})\t(.+)")
JOBS_FOLDER = "/opt/jobs"
DOCKERFILE = "spark/Dockerfile"
DOCKERIGNORE_FILE = "spark/Dockerfile.dockerignore"
CONTROL_CHARACTER = re.compile(r"[\x00-\x1f\x7f]")
# Run in the given image (``python -c``): the Git blob ID (SHA-1 of ``blob <size>`` and a zero byte before the content) and the path of each Python file under
# /opt/jobs, one per line, sorted. The Dockerfile copies files there from ``spark``, ``scripts``, ``features``, ``casefile`` and ``replay``.
SOURCE_SCRIPT = (
    "import hashlib, pathlib\n"
    "root = pathlib.Path('/opt/jobs')\n"
    "for path in sorted(root.rglob('*.py')):\n"
    "    data = path.read_bytes()\n"
    "    print(hashlib.sha1(b'blob %d\\0' % len(data) + data).hexdigest(), path.relative_to(root).as_posix())\n"
)
IMAGE_NAME = r"(?:[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?(?::[0-9]+)?/)?[a-z0-9]+(?:[._-][a-z0-9]+)*(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*"
# A name with a tag, or a name with the digest that ``compose/image_pins.yml`` gives (the host pulls by digest, so it holds no tag of the publisher's).
IMAGE_REFERENCE = re.compile(rf"{IMAGE_NAME}:[A-Za-z0-9_][A-Za-z0-9_.-]{{0,127}}")
DIGEST_REFERENCE = re.compile(rf"{IMAGE_NAME}@sha256:[0-9a-f]{{64}}")


class HostSettingError(ValueError):
    """A setting that cannot be used. Its text is fixed: it names the setting and never holds the value."""


@dataclass(frozen=True)
class Limits:
    """The seconds that each wait of a run may take. ``job_deadline_seconds`` is the deadline that the job gives itself (``JOB_DEADLINE_SECONDS``), always
    shorter than ``job_seconds``, the runner's outer limit. ``run_seconds`` is the deadline of the whole run, or None when the run has none.
    ``cleanup_seconds`` is the budget of the cleanup after the run (and after a stopped step), apart from the run's own."""

    command_seconds: int
    build_seconds: int
    job_seconds: int
    job_deadline_seconds: int
    health_seconds: int
    call_seconds: int
    run_seconds: int | None = None
    cleanup_seconds: int = 600

    @property
    def ceiling_seconds(self) -> int | None:
        """The longest that the steps of a run take: no wait goes past the run deadline (it gets the rest of the run as its cap) and the cleanup has its own
        budget. A run without a deadline has no ceiling."""
        return None if self.run_seconds is None else self.run_seconds + self.cleanup_seconds


LOCAL_LIMITS = Limits(180, 3000, 3600, 3500, 300, 30)
HOST_LIMITS = Limits(180, 1800, 1800, 1700, 300, 30, 5400, 600)


@dataclass(frozen=True)
class Folders:
    """The folders of the pinned files, or None for one that is not usable, and the problems in fixed words."""

    landed: Path | None
    extra: Path | None
    problems: tuple[str, ...]


@dataclass(frozen=True)
class Image:
    """The Spark image of the run. ``tag`` is the run's own tag, which the run creates (by a build, or by tagging the verified image) and removes. ``source`` is
    the given image (``SPARK_IMAGE``) that the run only reads and never changes or removes, or None when the run builds. ``expected_id`` is the immutable ID
    that the deploy recorded for the tested build: the given image must have it."""

    tag: str
    built_here: bool
    source: str | None
    expected_id: str | None


def limits_for(host: bool) -> Limits:
    return HOST_LIMITS if host else LOCAL_LIMITS


def host_flag(environ: Mapping[str, str], name: str) -> bool:
    """True for ``1``, False when the flag is not set, empty or ``0``. Any other value is refused, so that a flag written as ``true`` never runs the local mode
    on the host."""
    value = environ.get(name, "")
    if value in ("", "0"):
        return False
    if value == "1":
        return True
    raise HostSettingError(f"{name} must be 1, 0 or not set")


def absolute_folder(name: str, value: str | None, problems: list[str]) -> Path | None:
    """The folder as a path when it is set and absolute. Otherwise None, and the problem is added in fixed words."""
    if value is None:
        problems.append(f"{name} is not set")
        return None
    if not Path(value).is_absolute():
        problems.append(f"{name} is not an absolute path")
        return None
    return Path(value)


def pinned_folders(environ: Mapping[str, str], host: bool, landed_default: Path, extra_default: Path) -> Folders:
    """The folder of the pinned files and the folder of the extra case file. Locally each has a default. On the host none has: the case folder falls back to the
    landed folder, and each folder must be set and absolute."""
    landed_given, extra_given = (environ.get(name) or None for name in FOLDER_SETTINGS)
    if not host:
        return Folders(Path(landed_given or landed_default), Path(extra_given or extra_default), ())
    problems: list[str] = []
    landed = absolute_folder("LANDED_CACHE", landed_given, problems)
    extra = landed if extra_given is None else absolute_folder("SIGNAL_CASE_CACHE", extra_given, problems)
    return Folders(landed, extra, tuple(problems))


def image_choice(environ: Mapping[str, str], tag: str, pinned: str | None = None) -> Image:
    """The image of the run. ``tag`` is the run's own tag. With ``SPARK_IMAGE`` the run reads that image (it must exist) and gives its own tag to the image
    whose immutable ID is ``SPARK_IMAGE_ID`` (the ID that the deploy recorded for the tested build), and it leaves the given image as it is. Without it the run
    builds the image under its own tag. An empty setting is no setting. A reference with neither a tag nor a digest, or one that ``docker`` could read as an
    option or as two words, an ID that is not a full sha256 ID, a given image with a tag and no ID and an ID with no image are refused, and the text never
    holds the value.

    A digest reference (``<repository>@sha256:<64 hex digits>``) names its content, so it needs no ID from the operator: ``expected_id`` is then the ID that
    is given, or None, and the run only needs Docker to give a readable ID. ``pinned`` is the digest reference of the Spark image that the pin file of the
    checkout names, which a host run takes as the given image when the operator gives none (the host builds nothing and holds no tag of the publisher's)."""
    given = environ.get("SPARK_IMAGE", "")
    identifier = environ.get("SPARK_IMAGE_ID", "")
    if given == "" and pinned is not None:
        if DIGEST_REFERENCE.fullmatch(pinned) is None:
            raise HostSettingError("the pinned Spark image is not a digest reference")
        given = pinned
    if given == "":
        if identifier != "":
            raise HostSettingError("SPARK_IMAGE_ID is set without SPARK_IMAGE")
        return Image(tag, True, None, None)
    by_digest = DIGEST_REFERENCE.fullmatch(given) is not None
    if not by_digest and IMAGE_REFERENCE.fullmatch(given) is None:
        raise HostSettingError("SPARK_IMAGE is not an image name with a tag or a digest")
    if identifier == "" and not by_digest:
        raise HostSettingError("SPARK_IMAGE_ID is not set")
    if identifier != "" and IMAGE_ID.fullmatch(identifier) is None:
        raise HostSettingError("SPARK_IMAGE_ID is not an image ID (sha256: and 64 hexadecimal characters)")
    return Image(tag, False, given, identifier or None)


def image_id_problem(actual: str, expected: str | None) -> str | None:
    """A fixed code when the ID that Docker gave for the given image is not the one that the deploy recorded, or None. An ID that is not a full sha256 ID
    (empty, a placeholder, with a line end) is never a match. With no recorded ID (a digest reference names its content) the ID only has to be readable."""
    if IMAGE_ID.fullmatch(actual) is None:
        return "image_id_unreadable"
    return None if expected is None or actual == expected else "image_id_mismatch"


def dockerfile_copies(dockerfile: str) -> list[tuple[list[str], str]] | None:
    """The ``COPY`` instructions of the Dockerfile that put files under ``JOBS_FOLDER``, in order, as ``(sources, destination)``. None when an instruction that
    does this has a form that this check does not read (a JSON form, a wildcard, a relative destination or a source outside the checkout)."""
    text = re.sub(r"\\[ \t]*\n", " ", dockerfile)
    copies: list[tuple[list[str], str]] = []
    for line in text.splitlines():
        words = line.split()
        if not words or words[0].upper() != "COPY":
            continue
        flags = [word for word in words[1:] if word.startswith("--")]
        if any(flag.startswith("--from=") for flag in flags):
            continue
        words = [word for word in words[1:] if not word.startswith("--")]
        if len(words) < 2:
            return None
        *sources, destination = words
        if not destination.startswith("/"):
            return None
        if not (destination == JOBS_FOLDER or destination.startswith(f"{JOBS_FOLDER}/")):
            continue
        if any(source.startswith(("[", '"', "/", "..")) or "/../" in source or any(mark in source for mark in "*?[") for source in sources):
            return None
        copies.append((sources, destination))
    return copies


# One line of a Docker ignore file: whether it is an exception (``!``), whether it starts with ``**/``, and the names of its path.
IgnoreRule = tuple[bool, bool, tuple[str, ...]]
IGNORE_NOT_READ = re.compile(r"[?\[\\]|\*\*")
BYTE_ORDER_MARK = "\ufeff"
# The characters that Go's ``strings.TrimSpace`` trims (``unicode.IsSpace``): not those of Python's ``str.strip`` (it also trims U+001C to U+001F).
DOCKER_SPACE = "\t\n\v\f\r \x85\xa0\u1680" + "".join(map(chr, range(0x2000, 0x200B))) + "\u2028\u2029\u202f\u205f\u3000"


def dockerignore_rules(text: str) -> list[IgnoreRule] | None:
    """The rules of a Docker ignore file, in order, or None when a line has a form that this check does not read.

    The matching is that of the Docker documentation, "Build context", ".dockerignore files" (https://docs.docker.com/build/concepts/context/#dockerignore-files),
    and only the forms that ``spark/Dockerfile.dockerignore`` uses are read: a blank line and a ``#`` comment (skipped), a path from the root of the context
    whose names are plain or are a lone ``*`` (any one name), a leading ``**/`` (any number of folders), and ``!`` before a pattern (an exception). Anything
    else (``*`` or ``**`` inside a name, ``?``, ``[``, a backslash, a rooted, ``.``, ``..``, empty or trailing-slash name, a bare ``!``) makes the check
    unreadable, so that a form that it does not know is never read as a rule that it does.

    The lines are read as ``ReadAll`` of moby's ``ignorefile`` package reads them at the engine that builds the image (Docker 29.9.3, file
    https://github.com/moby/moby/blob/docker-v29.9.3/vendor/github.com/moby/patternmatcher/ignorefile/ignorefile.go): a line ends at a line feed only, and
    the default split function of ``bufio.NewScanner`` (``ScanLines``, line 33) drops one carriage return before it (the trim of line 46 would drop it too);
    a byte order mark is taken off the first line only (lines 36-39); a line whose first character is ``#`` is a comment, so a ``#`` after white space starts
    a pattern (lines 42-45); the white space around the rest is trimmed with ``strings.TrimSpace`` (line 46), then once more after a ``!`` (lines 52-55). A
    pattern that this reading gives and that the check does not implement     makes the check unreadable."""
    rules: list[IgnoreRule] = []
    lines = text.split("\n")
    lines[0] = lines[0].removeprefix(BYTE_ORDER_MARK)
    for line in lines:
        if line.startswith("#"):
            continue
        line = line.strip(DOCKER_SPACE)
        if not line:
            continue
        exception = line.startswith("!")
        names = (line[1:].strip(DOCKER_SPACE) if exception else line).split("/")
        anywhere = names[0] == "**"
        if anywhere:
            names = names[1:]
        if not names or any(name in ("", ".", "..") or IGNORE_NOT_READ.search(name) or ("*" in name and name != "*") for name in names):
            return None
        rules.append((exception, anywhere, tuple(names)))
    return rules


def rule_matches(rule: IgnoreRule, path: tuple[str, ...]) -> bool:
    """True when the rule matches the path or one of the folders on the way to it (a rule for a folder is a rule for everything in it)."""
    _, anywhere, names = rule
    for end in range(1, len(path) + 1):
        part = path[:end]
        if anywhere:
            part = part[-len(names) :] if len(part) >= len(names) else ()
        if len(part) == len(names) and all(name in ("*", seen) for name, seen in zip(names, part, strict=True)):
            return True
    return False


def dockerignored(path: str, rules: list[IgnoreRule]) -> bool:
    """True when the build context leaves the file out: the last rule that matches it decides, and an exception puts it back."""
    left_out = False
    for rule in rules:
        if rule_matches(rule, tuple(path.split("/"))):
            left_out = not rule[0]
    return left_out


def image_source_files(dockerfile: str, tree_output: str, dockerignore: str) -> dict[str, str] | None:
    """The Python files that an image built from this Dockerfile and this commit holds under ``JOBS_FOLDER``, as ``{path under it: Git blob ID}``, or None when
    the Dockerfile, the tree or the Docker ignore file cannot be read. The build context is the tree without the files that the Docker ignore file
    (``dockerignore``, the text of ``spark/Dockerfile.dockerignore`` of the same commit) leaves out, as ``docker build`` sends it, so a file that it
    leaves out is in no ``COPY`` source. The check then follows each ``COPY`` of the Dockerfile (the whole of a folder, a single file, a destination that ends
    in a slash), takes the blob IDs of ``git ls-tree -r HEAD`` and lets a later ``COPY`` to the same path win."""
    copies = dockerfile_copies(dockerfile)
    rules = dockerignore_rules(dockerignore)
    tree = {found.group(2): found.group(1) for line in tree_output.splitlines() if (found := TREE_LINE.fullmatch(line))}
    if not copies or not tree or rules is None:
        return None
    tree = {path: blob for path, blob in tree.items() if not dockerignored(path, rules)}
    files: dict[str, str] = {}
    for sources, destination in copies:
        for source in sources:
            source = source.rstrip("/")
            if source in tree:
                moved = {source.rsplit("/", 1)[-1]: tree[source]}
            else:
                moved = {path[len(source) + 1 :]: blob for path, blob in tree.items() if path.startswith(f"{source}/")}
            if not moved:
                return None
            for relative, blob in moved.items():
                target = destination.rstrip("/") + "/" + relative if source not in tree or destination.endswith("/") else destination
                if target.startswith(f"{JOBS_FOLDER}/") and target.endswith(".py"):
                    files[target[len(JOBS_FOLDER) + 1 :]] = blob
    return files


def source_problem(image_output: str, tree_output: str, dockerfile: str, dockerignore: str) -> str | None:
    """A fixed code when the Python files of the given image are not the files that the Dockerfile copies from the reviewed commit, or None when they are.

    ``image_output`` is what ``SOURCE_SCRIPT`` printed in the image, ``tree_output`` is ``git ls-tree -r HEAD`` of the checkout, ``dockerfile`` is the
    Dockerfile of the same commit and ``dockerignore`` is its Docker ignore file (``spark/Dockerfile.dockerignore``). The files that the image must hold are
    derived from the Dockerfile's own ``COPY`` instructions (``image_source_files``), so that a copy that is added or changed there is checked with no second
    list. A Docker ignore file that has a form this check does not read is unreadable. Every file of the image must be one of them with the same Git blob ID,
    and every one of them must be in the image. A listing with no file, a line that is not a listing line and a Dockerfile or a tree that names no file are
    never a match.
    """
    image: dict[str, str] = {}
    for line in image_output.splitlines():
        if not line.strip():
            continue
        found = IMAGE_SOURCE_LINE.fullmatch(line)
        if found is None:
            return "image_source_unreadable"
        image[found.group(2)] = found.group(1)
    expected = image_source_files(dockerfile, tree_output, dockerignore)
    if not image or not expected:
        return "image_source_unreadable"
    if any(expected.get(path) != blob for path, blob in image.items()):
        return "image_source_differs"
    return "image_source_missing" if set(expected) - set(image) else None


def blob_id(data: bytes) -> str:
    """The Git blob ID of the content: the SHA-1 of ``blob <size>``, a zero byte and the content (what ``git hash-object`` prints)."""
    return hashlib.sha1(b"blob %d\0" % len(data) + data, usedforsecurity=False).hexdigest()


def manifest_source(files: Mapping[str, str], read: Callable[[str], bytes | None]) -> tuple[str, str, str] | None:
    """The three inputs of ``source_problem`` for a checkout with no Git history, read from the deploy's source manifest, or None when any of them cannot be
    trusted: ``(tree_output, dockerfile, dockerignore)``, with the same form as ``git ls-tree -r HEAD``, ``git show HEAD:spark/Dockerfile`` and
    ``git show HEAD:spark/Dockerfile.dockerignore``.

    ``files`` is the manifest's ``files`` map (path to SHA-256) and ``read`` gives the bytes of a path of the checkout, or None when it cannot be read. Each
    file that is used (the Dockerfile, its Docker ignore file and every file that a ``COPY`` source of the Dockerfile names and the Docker ignore file does
    not leave out) must be listed in the manifest and its bytes must have the listed SHA-256, so an edited, missing or unlisted file gives None. The listing
    holds the Git blob ID that is computed from those verified bytes (the manifest holds no blob ID). A path with a control character (it could forge a line
    of the listing) gives None. A file that the manifest does not list is not in the listing, as a file that the commit does not hold is not in the tree of
    Git: ``source_problem`` then refuses the image (a copied file that is not in the listing is unreadable, an image file that it lacks is another build)."""

    verified: dict[str, bytes | None] = {}

    def checked(path: str) -> bytes | None:
        if path not in verified:
            data = read(path) if path in files else None
            verified[path] = data if data is not None and hashlib.sha256(data).hexdigest() == files[path] else None
        return verified[path]

    try:
        dockerfile = (checked(DOCKERFILE) or b"").decode()
        dockerignore = (checked(DOCKERIGNORE_FILE) or b"").decode()
    except UnicodeDecodeError:
        return None
    copies = dockerfile_copies(dockerfile)
    rules = dockerignore_rules(dockerignore)
    if not dockerfile or not dockerignore or not copies or rules is None:
        return None
    sources = {source.rstrip("/") for names, _ in copies for source in names}
    lines: list[str] = []
    for path in sorted(files):
        if not any(path == source or path.startswith(f"{source}/") for source in sources) or dockerignored(path, rules):
            continue
        data = checked(path)
        if data is None or CONTROL_CHARACTER.search(path):
            return None
        lines.append(f"100644 blob {blob_id(data)}\t{path}\n")
    return ("".join(lines), dockerfile, dockerignore) if lines else None


def read_env_file(text: str) -> dict[str, str]:
    """The ``NAME=value`` lines of a Compose env file. A blank line, a comment and a line without a name or without ``=`` are skipped."""
    values: dict[str, str] = {}
    for line in text.splitlines():
        name, separator, value = line.partition("=")
        if separator and name.strip() and not name.lstrip().startswith("#"):
            values[name.strip()] = value
    return values


def docker_environment(environ: Mapping[str, str], binding: Mapping[str, str] | None = None) -> dict[str, str]:
    """The environment of one Compose or Docker call: the names of ``DOCKER_PASSTHROUGH`` that the caller has, then the run's binding (its catalog, warehouse,
    ports, mounts and memory limits), which beats a caller's value of the same name. No other setting of the caller is passed."""
    result = {name: environ[name] for name in DOCKER_PASSTHROUGH if name in environ}
    result.update(binding or {})
    return result


def available_problem(available_mib: int | None, needed_bytes: int, reserve_bytes: int) -> str | None:
    """A fixed code when the memory that is free now does not cover the services' limits and the host's reserve, or None. A host that gives no free figure is
    refused, never assumed to fit."""
    if available_mib is None:
        return "available_unknown"
    return "available_memory" if available_mib * MIB < needed_bytes + reserve_bytes else None


def memory_problem(total_bytes: int, available_mib: int | None, needed_bytes: int, host: bool, reserve_bytes: int = 0) -> str | None:
    """A fixed code for the memory that is not enough, or None. The daemon holds twice the memory of the services' limits in both modes. On the host the
    deployed stack already holds memory, so the needed memory and the host's reserve (``host_reserve_mib`` of ``compose/limits.yml``) must also be free now."""
    if total_bytes < 2 * needed_bytes:
        return "daemon_memory"
    if not host:
        return None
    return available_problem(available_mib, needed_bytes, reserve_bytes)


def active_cap(started: float, now: float, limits: Limits, step_seconds: float) -> float | None:
    """The seconds that an active step may wait: its own limit, or the rest of the run when that is shorter. None when the run deadline has passed, so that the
    step is not started. A run without a deadline gives every step its own limit, and a clock that went back never shortens a step."""
    if limits.run_seconds is None:
        return step_seconds
    remaining = limits.run_seconds - max(0.0, now - started)
    return None if remaining <= 0 else min(step_seconds, remaining)


def remaining_seconds(started: float, now: float, limits: Limits) -> float | None:
    """The seconds of the run that are left at ``now``, never below zero, or None when the run has no deadline. A caller reads it at the start of each call
    (never earlier), so that the call gets the shorter of its own limit and the rest of the run."""
    if limits.run_seconds is None:
        return None
    return max(0.0, limits.run_seconds - max(0.0, now - started))


def cleanup_cap(cleanup_started: float | None, now: float, limits: Limits, step_seconds: float) -> float | None:
    """The seconds that a cleanup step may wait: its own limit, or what is left of the cleanup budget when that is shorter. ``cleanup_started`` is the time of
    the first cleanup step, or None before it. None when the budget is spent, so that the step is not started and the caller counts it as not confirmed."""
    if cleanup_started is None:
        return min(step_seconds, float(limits.cleanup_seconds))
    remaining = limits.cleanup_seconds - max(0.0, now - cleanup_started)
    return None if remaining <= 0 else min(step_seconds, remaining)


def job_deadline_for(cap: float, limits: Limits) -> int | None:
    """The deadline that a job gives itself when its outer cap is ``cap``: its own deadline when the cap is the full limit, otherwise the cap less the margin
    between the job's limit and its deadline, so that the job reports its own timeout before the runner stops it. None when the cap is not longer than it."""
    margin = limits.job_seconds - limits.job_deadline_seconds
    if cap >= limits.job_seconds:
        return limits.job_deadline_seconds
    deadline = int(cap) - margin
    return deadline if deadline >= 1 else None
