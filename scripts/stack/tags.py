"""The tag step of a reported run, and the reads that table maintenance needs from Nessie's tags.

After a run's branch is merged, the merged hash gets a Nessie tag whose name holds the run ID, and the tag, the hash and the snapshot ID of each table at that
hash go into a record in the run's folder. The step is idempotent: a rerun with the same run ID and hash changes nothing, and the same tag name on another hash
is refused, in Nessie and in the folder, so a run's pin never moves. Table maintenance finds the tags and what each reads with ``list_tags`` and
``table_snapshots``.

The command line runs in a container that reaches Nessie at ``NESSIE_URI``:

``python -m scripts.stack.tags tag --run-id <id> --hash <merged hash> --run-folder <folder> [--trunk <default branch>] [--deadline-seconds 120]``

It says ``tagged`` when it made the tag or wrote the record, and ``unchanged`` when both were already as asked. Exit 0 is success, 1 is a refusal by Nessie
or by a record that holds another hash, and 2 is a bad argument. The step writes ``data_version.json`` into the folder it is given and makes the folder when
it is absent. A folder that the run folder writer would name ``*_<run id>`` must not exist before that writer runs, so give this step another folder.
"""

from __future__ import annotations

import contextlib
import http.client
import json
import math
import os
import re
import signal
import stat
import sys
import tempfile
import time
from dataclasses import dataclass, replace
from pathlib import Path
from types import FrameType
from typing import Any
from urllib.parse import quote

from scripts.ingestion.closed import QuietParser
from scripts.ingestion.notice import describe_error
from scripts.stack.nessie import BODY_LIMIT_BYTES, HASH, NessieClient, NessieConflict, NessieError, Reference

RUN_TAG_PREFIX = "run-"
RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,95}")
RECORD_NAME = "data_version.json"
PAGE_SIZE = 100
MAX_PAGES = 1000
DEFAULT_DEADLINE_SECONDS = 120.0
MAX_DEADLINE_SECONDS = 86_400.0
NO_SNAPSHOT = -1
CALL_SECONDS = 30.0
TABLE_TYPE = "ICEBERG_TABLE"


class TagError(ValueError):
    """An argument is not one that a tag step can use."""


class NotOnTrunk(NessieError):
    """The hash is not in the history of the trunk, or the trunk is not a branch. No tag is made or accepted."""


class RecordNotPublished(NessieError):
    """The tag was made and the run folder's record could not be published. The tag is left in place."""


class TagConflict(NessieError):
    """A tag or a recorded version of this run is already at another hash. Nothing is changed."""


@dataclass(frozen=True)
class TableSnapshot:
    table: str
    snapshot_id: int | None
    metadata_location: str


@dataclass(frozen=True)
class TagResult:
    run_id: str
    tag: str
    hash: str
    trunk: str
    created: bool
    tables: tuple[TableSnapshot, ...]


class TagClient(NessieClient):
    """A Nessie client for the tag step: every call counts against one deadline and an answer over the byte cap is an error.

    The plain client reads at most ``BODY_LIMIT_BYTES`` and hands back the cut text, which a list then shows as an empty list. Here an answer of more bytes than
    the cap is refused, so a long inventory is never taken for a short one.
    """

    def __init__(self, base_url: str, deadline_seconds: float = DEFAULT_DEADLINE_SECONDS) -> None:
        super().__init__(base_url, min(CALL_SECONDS, deadline_seconds))
        self.expires = time.monotonic() + deadline_seconds

    def send(self, method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
        remaining = self.expires - time.monotonic()
        if remaining <= 0:
            raise NessieError(f"{method} {self.prefix}: the deadline of the run passed before the call was sent")
        headers = {"Accept": "application/json", **({"Content-Type": "application/json"} if payload is not None else {})}
        connection = http.client.HTTPConnection(self.host, self.port, timeout=min(self.timeout_seconds, remaining))
        try:
            connection.request(method, target, body=payload, headers=headers)
            response = connection.getresponse()
            body = response.read(BODY_LIMIT_BYTES + 1)
        finally:
            connection.close()
        if len(body) > BODY_LIMIT_BYTES:
            raise NessieError(f"{method} {self.prefix}: the answer is larger than {BODY_LIMIT_BYTES} bytes and is not read in part", status=response.status)
        return response.status, body


class DeadlineExceeded(NessieError):
    """The whole run passed its deadline."""


def tag_name(run_id: str) -> str:
    """The tag of a run: ``run-`` and the run ID, which is letters, digits, dashes and underscores, at most 96 characters."""
    if RUN_ID.fullmatch(run_id) is None:
        raise TagError("the run ID must be 1 to 96 letters, digits, dashes and underscores, starting with a letter or a digit")
    return f"{RUN_TAG_PREFIX}{run_id}"


def paged(client: NessieClient, path: str, field: str, query: dict[str, str] | None = None) -> list[dict[str, Any]]:
    """Every item of the list ``field`` in an answer that Nessie pages with ``token``, in order.

    The pinned server's version 2 names its parameters ``max-records`` and ``page-token``. A page that holds no list of objects, a ``hasMore`` that is not a
    boolean, more pages with no token, a token that was seen before and more than ``MAX_PAGES`` pages are errors: none is taken for the end of the list.
    """
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    token = ""
    for _ in range(MAX_PAGES):
        document = client.request("GET", path, None, {**(query or {}), "max-records": str(PAGE_SIZE), **({"page-token": token} if token else {})})
        page = document.get(field) if isinstance(document, dict) else None
        if not isinstance(document, dict) or not isinstance(page, list) or not all(isinstance(item, dict) for item in page):
            raise NessieError(f"GET {path} answered with a page that holds no list {field!r} of objects")
        items.extend(page)
        if document.get("hasMore") is False:
            return items
        if document.get("hasMore") is not True:
            raise NessieError(f"GET {path} answered with a page whose hasMore is not true or false")
        following = document.get("token")
        if not isinstance(following, str) or not following:
            raise NessieError(f"GET {path} says there are more pages and gives no token")
        if following == token or following in seen:
            raise NessieError(f"GET {path} gave the page token {following!r} again: the list does not advance")
        seen.add(following)
        token = following
    raise NessieError(f"GET {path} has more than {MAX_PAGES} pages")


def list_references(client: NessieClient, kind: str) -> dict[str, str]:
    """Every reference of the type ``kind`` (``TAG`` or ``BRANCH``) with the hash it points to."""
    return {str(item["name"]): str(item["hash"]) for item in paged(client, "/trees", "references") if item.get("type") == kind}


def list_tags(client: NessieClient) -> dict[str, str]:
    """Every tag of the repository with the hash it points to."""
    return list_references(client, "TAG")


def table_name(entry: dict[str, Any]) -> str:
    """The dotted name of a table entry, or ``NessieError`` when the entry names no table."""
    elements = (entry.get("name") or {}).get("elements") if isinstance(entry.get("name"), dict) else None
    if not isinstance(elements, list) or not elements or not all(isinstance(item, str) and item for item in elements):
        raise NessieError("Nessie answered with a table entry that has no name made of text elements")
    return ".".join(elements)


def table_snapshots(client: NessieClient, reference: str) -> tuple[TableSnapshot, ...]:
    """The Iceberg tables at the head of ``reference`` with the snapshot ID and metadata file that Nessie holds for each, in the order of their names.

    Every table entry must carry its content, with a metadata location that is non-empty text and a snapshot ID that is an integer. Nessie's explicit
    no-snapshot value, ``-1``, is recorded as None. Anything else is a ``NessieError`` that names the table: no incomplete entry is passed on as a verified one.
    """
    found = []
    for entry in paged(client, f"/trees/{quote(reference, safe='')}/entries", "entries", {"content": "true"}):
        if entry.get("type") != TABLE_TYPE:
            continue
        table = table_name(entry)
        content = entry.get("content")
        if not isinstance(content, dict):
            raise NessieError(f"the table {table} at {reference} has no content in Nessie's answer")
        location = content.get("metadataLocation")
        if not isinstance(location, str) or not location:
            raise NessieError(f"the table {table} at {reference} has no metadata location in Nessie's answer")
        snapshot = content.get("snapshotId")
        if isinstance(snapshot, bool) or not isinstance(snapshot, int) or snapshot < NO_SNAPSHOT:
            raise NessieError(f"the table {table} at {reference} has the snapshot ID {snapshot!r}, which is not an integer or Nessie's no-snapshot value")
        found.append(TableSnapshot(table, None if snapshot == NO_SNAPSHOT else snapshot, location))
    return tuple(sorted(found, key=lambda item: item.table))


def default_branch(client: NessieClient) -> str:
    """The default branch that Nessie's configuration names. It is the trunk of every tag, and a command never takes another one from its arguments."""
    name = client.config().get("defaultBranch")
    if not isinstance(name, str) or not name:
        raise NessieError("Nessie's configuration answer names no default branch")
    return name


def require_on_trunk(client: NessieClient, trunk: str, merged_hash: str) -> None:
    """Prove that ``merged_hash`` is a commit in the log of the branch ``trunk``, or raise ``NotOnTrunk``.

    Nessie makes a tag at any commit that exists, on any branch, so it accepts a hash from a branch that was never merged. The log of the trunk is read
    with the pinned server's version 2 commit-log call, ``GET /trees/<trunk>/history``, filtered to this hash with a CEL expression, in pages of ``PAGE_SIZE``.
    """
    reference = client.reference(trunk)
    if reference.kind != "BRANCH":
        raise NotOnTrunk(f"{trunk} is a {reference.kind.lower()}, not a branch: a run's tag is made on a hash of the trunk")
    entries = paged(client, f"/trees/{quote(trunk, safe='')}/history", "logEntries", {"filter": f"commit.hash == '{merged_hash}'"})
    if not any((entry.get("commitMeta") or {}).get("hash") == merged_hash for entry in entries):
        raise NotOnTrunk(f"{merged_hash} is not in the history of {trunk}: only a hash that was merged into {trunk} is tagged")


def prepare_tag(client: NessieClient, run_id: str, merged_hash: str, trunk: str | None = None) -> TagResult:
    """Make every Nessie read that the tag needs, and create nothing: the names, the default branch, the proof that the hash is in its log and the tables.

    The result holds the tables read at the exact merged hash, so the record that will be published is known before the tag is created. ``created`` is False.
    """
    name = tag_name(run_id)
    if HASH.fullmatch(merged_hash) is None:
        raise TagError("the hash must be 64 lower-case hexadecimal digits")
    default = default_branch(client)
    if trunk is not None and trunk != default:
        raise NotOnTrunk(f"{trunk} is not Nessie's default branch {default}: a run's tag is made on a hash of the default branch")
    require_on_trunk(client, default, merged_hash)
    return TagResult(run_id, name, merged_hash, default, False, table_snapshots(client, f"{default}@{merged_hash}"))


def check_existing(existing: Reference, prepared: TagResult) -> None:
    """Refuse a reference of the tag's name that is not a tag at the prepared hash."""
    if existing.kind != "TAG" or existing.hash != prepared.hash:
        raise TagConflict(f"{prepared.tag} already exists as a {existing.kind.lower()} at {existing.hash}, not at {prepared.hash}: a run's tag never moves")


def find_tag(client: NessieClient, prepared: TagResult) -> Reference | None:
    """The tag of the prepared name when it exists at the prepared hash, None when there is none, and ``TagConflict`` for anything else of that name."""
    try:
        existing = client.reference(prepared.tag)
    except NessieError as error:
        if error.status != 404:
            raise
        return None
    check_existing(existing, prepared)
    return existing


def post_tag(client: NessieClient, prepared: TagResult) -> bool:
    """Create the prepared tag. True when this call made it, False when a tag of that name appeared first (a race the caller then judges).

    Nessie's answer to the call is checked and no read follows it: the call is the last operation on Nessie, so the publish of the record is the only step
    that can still fail after the tag is made.
    """
    try:
        made = client.create_tag(prepared.tag, prepared.hash, prepared.trunk)
    except NessieConflict:
        return False
    if made.kind != "TAG" or made.hash != prepared.hash:
        raise NessieError(f"{prepared.tag} was made, and Nessie's answer shows a {made.kind.lower()} at {made.hash}, not a tag at {prepared.hash}")
    return True


def create_tag(client: NessieClient, prepared: TagResult) -> TagResult:
    """Make the prepared tag, or find it where it must be. A tag of that name at that hash stays, and one at another hash is refused."""
    made = False
    if find_tag(client, prepared) is None:
        made = post_tag(client, prepared)
        if not made:
            check_existing(client.reference(prepared.tag), prepared)
    return replace(prepared, created=made)


def tag_run(client: NessieClient, run_id: str, merged_hash: str, trunk: str | None = None) -> TagResult:
    """Tag ``merged_hash``, a hash in the history of Nessie's default branch, for ``run_id``: every read first, then the tag.

    A tag of that name at that hash stays, and one at another hash is refused. The trunk is the default branch that Nessie's configuration names, read first.
    ``trunk``, when given, only asserts it: another branch is refused before any other call.
    """
    return create_tag(client, prepare_tag(client, run_id, merged_hash, trunk))


def read_record(folder: Path) -> dict[str, Any] | None:
    """The record in ``folder``, or None when there is none. Anything at the record's path that is not a regular file, a link included, is refused.

    The path is examined with ``lstat``, which does not follow a link, so a dangling link or a link to a good record is not taken for an absent or good one.
    """
    target = folder / RECORD_NAME
    try:
        mode = os.lstat(target).st_mode
    except FileNotFoundError:
        return None
    except OSError as error:
        raise TagConflict(f"{target} cannot be examined ({type(error).__name__}): no tag was made") from error
    if not stat.S_ISREG(mode):
        raise TagConflict(f"{target} is not a regular file (it is a link, a folder or another kind of file): no tag was made")
    try:
        held = json.loads(target.read_text())
    except (OSError, ValueError) as error:
        raise TagConflict(f"{target} exists and cannot be read as JSON ({type(error).__name__}): no tag was made") from error
    if not isinstance(held, dict):
        raise TagConflict(f"{target} does not hold a JSON object: no tag was made")
    return held


def same_record(left: Any, right: Any) -> bool:
    """Whether two parsed JSON values are equal type for type: a Boolean is never an integer, an integer is never a float, and a list is never a tuple."""
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(same_record(left[key], right[key]) for key in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(same_record(one, other) for one, other in zip(left, right, strict=True))
    return bool(left == right)


def judge_record(held: dict[str, Any] | None, folder: Path, run_id: str, tag: str, merged_hash: str) -> None:
    """Refuse a record that is not this run's at this hash. This is the cheap check that runs before any call to Nessie."""
    if held is not None and not same_record([held.get("run_id"), held.get("tag"), held.get("hash")], [run_id, tag, merged_hash]):
        raise TagConflict(f"{folder / RECORD_NAME} records {held.get('tag')} at {held.get('hash')}, not {tag} at {merged_hash}: no tag was made")


def tag_and_record(client: NessieClient, run_id: str, merged_hash: str, folder: Path, trunk: str | None = None) -> tuple[TagResult, bool]:
    """Check everything, prepare the record, then tag, then publish. Returns the result and whether the record was written now.

    The order: the record's path and its run, tag and hash (no call to Nessie yet); every Nessie read, including the tables at the exact merged hash; the whole
    record that would be published against an existing one, which must be equal type for type; the staging file of the record, made and synced in the run
    folder, so that every failure of the folder comes first; the look for an existing tag; and then the create call, the last operation on Nessie. After it the
    only step left is the atomic no-replace publish of the prepared file. Any failure from the create call on, a deadline included, names the tag and says
    that it is left in place (or, when the create call itself did not answer, that it may have been made and is left in place if so). A staging file that is
    left over is removed.
    """
    held = read_record(folder)
    judge_record(held, folder, run_id, tag_name(run_id), merged_hash)
    prepared = prepare_tag(client, run_id, merged_hash, trunk)
    record = version_record(prepared)
    if held is not None and not same_record(held, record):
        raise TagConflict(f"{folder / RECORD_NAME} does not equal the record of {prepared.tag} at {merged_hash} that Nessie's tables give: no tag was made")
    staging = stage_record(folder, record)
    stage, made = "before", False
    try:
        if find_tag(client, prepared) is None:
            stage = "posting"
            made = post_tag(client, prepared)
            if not made:
                stage = "before"
                check_existing(client.reference(prepared.tag), prepared)
        stage = "after"
        result = replace(prepared, created=made)
        return result, publish_record(staging, folder, record, result.tag, result.hash)
    except Exception as error:
        if stage == "before":
            raise
        if made:
            left = f"the tag {prepared.tag} was made at {merged_hash} and is left in place"
        elif stage == "posting":
            left = f"the tag {prepared.tag} may have been made at {merged_hash} and is left in place if it was"
        else:
            left = f"the tag {prepared.tag} was already in place at {merged_hash}"
        raise RecordNotPublished(f"the record could not be published ({error}); {left}") from error
    finally:
        with contextlib.suppress(OSError):
            Path(staging).unlink(missing_ok=True)


def version_record(result: TagResult) -> dict[str, Any]:
    """The record of a run's data version: the tag, the hash and each table's snapshot ID."""
    return {
        "run_id": result.run_id,
        "tag": result.tag,
        "hash": result.hash,
        "trunk": result.trunk,
        "tables": [{"table": item.table, "snapshot_id": item.snapshot_id, "metadata_location": item.metadata_location} for item in result.tables],
    }


def stage_record(folder: Path, record: dict[str, Any]) -> str:
    """Write ``record`` whole into a staging file of ``folder``, synced, and return its path. The folder is made when it is absent.

    Every failure of the folder or of the write is a ``TagConflict`` that says no tag was made, and leaves no staging file.
    """
    staging = ""
    try:
        folder.mkdir(parents=True, exist_ok=True)
        descriptor, staging = tempfile.mkstemp(prefix=".writing_", dir=folder)
        with os.fdopen(descriptor, "w") as handle:
            handle.write(json.dumps(record, indent=2) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(staging, 0o644)
    except OSError as error:
        if staging:
            Path(staging).unlink(missing_ok=True)
        raise TagConflict(f"the run folder cannot be written ({type(error).__name__}): no tag was made") from error
    return staging


def publish_record(staging: str, folder: Path, record: dict[str, Any], tag: str, merged_hash: str) -> bool:
    """Publish the staged file as the run's record with ``os.link``, which fails when the name exists and never replaces a file.

    Returns False when the folder already holds this very record, compared type for type, and refuses one that holds another. A second writer that got past its
    own checks at the same time meets the first one's record and compares the whole of it.
    """
    target = folder / RECORD_NAME
    try:
        os.link(staging, target)
    except FileExistsError:
        try:
            held = json.loads(target.read_text())
        except (OSError, ValueError) as error:
            raise TagConflict(f"{target} exists and cannot be read as JSON ({type(error).__name__}): it is not replaced") from error
        if same_record(held, record):
            return False
        raise TagConflict(
            f"{target} already records {tag} at {held.get('hash') if isinstance(held, dict) else None}, not at {merged_hash}: it is kept"
        ) from None
    return True


def write_version_record(folder: Path, result: TagResult) -> bool:
    """Stage and publish the record of ``result`` in ``folder``. Returns False when the folder already holds this very record."""
    record = version_record(result)
    staging = stage_record(folder, record)
    try:
        return publish_record(staging, folder, record, result.tag, result.hash)
    finally:
        Path(staging).unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    """Tag a run's merged hash and record it. Exit 0 when the tag and the record are as asked, 1 when Nessie or a record refuses, 2 for bad input."""
    parser = QuietParser(prog="scripts.stack.tags", description="Tag a run's merged hash and write the tag, hash and snapshot IDs into its run folder.")
    commands = parser.add_subparsers(dest="command", required=True)
    tag = commands.add_parser("tag")
    tag.add_argument("--run-id", required=True)
    tag.add_argument("--hash", required=True, help="the hash that the merge of the run's branch returned")
    tag.add_argument("--run-folder", required=True, type=Path)
    tag.add_argument("--trunk", default=None, help="an assertion: Nessie's default branch, which is always the trunk, must be this one")
    tag.add_argument("--deadline-seconds", type=float, default=DEFAULT_DEADLINE_SECONDS, help="one finite deadline for the whole run")
    arguments = parser.parse_args(argv)
    uri = os.environ.get("NESSIE_URI", "")
    if not uri:
        sys.stderr.write("NESSIE_URI is not set: it names Nessie's API, such as http://nessie:19120/api/v2\n")
        return 2
    try:
        tag_name(arguments.run_id)
        if HASH.fullmatch(arguments.hash) is None:
            raise TagError("the hash must be 64 lower-case hexadecimal digits")
        if not math.isfinite(arguments.deadline_seconds) or not 0 < arguments.deadline_seconds <= MAX_DEADLINE_SECONDS:
            raise TagError(f"the deadline must be a finite number of seconds above 0 and at most {MAX_DEADLINE_SECONDS:g}")
    except TagError as error:
        sys.stderr.write(f"{describe_error(error)}\n")
        return 2

    def expire(signal_number: int, frame: FrameType | None) -> None:
        raise DeadlineExceeded("the run passed its deadline and was stopped")

    previous = signal.signal(signal.SIGALRM, expire)
    signal.setitimer(signal.ITIMER_REAL, arguments.deadline_seconds)
    try:
        result, written = tag_and_record(TagClient(uri, arguments.deadline_seconds), arguments.run_id, arguments.hash, arguments.run_folder, arguments.trunk)
    except NessieError as error:
        sys.stderr.write(f"{describe_error(error)}\n")
        return 1
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)
    word = "tagged" if result.created or written else "unchanged"
    sys.stdout.write(f"{word}: {result.tag} is at {result.hash} with {len(result.tables)} tables, recorded in {arguments.run_folder / RECORD_NAME}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
