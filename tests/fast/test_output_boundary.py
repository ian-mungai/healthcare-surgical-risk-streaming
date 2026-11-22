"""No value of an input reaches an output of a command, unless the command verified or bound that value.

Every command entry point runs here: ``scripts.ingestion.cli`` ``check``, ``publish`` and ``verify`` in sequence, and ``scripts.sample.candidates`` ``build``
and ``check``. Each run changes one input field of a complete, passing baseline to a marker and collects every output: standard error, standard output,
each file the commands write, each manifest object written to the store and the text of anything a command raises. A run fails when a marker reaches an
output that ``ALLOWED`` does not name for its field.

The changes are generated, not chosen. ``json_edits`` walks each baseline document and, for every field and list element, replaces the value with a text
marker, a number marker and a marker of the field's own form (a prefixed or suffixed text, a 64-digit hex value, a time), removes the field and adds an
unknown field whose name and value are markers. The line inputs (the checksum list, the candidate file, the publisher's tables) are changed cell by cell,
once as they are and once with the pin that binds them rewritten to their new digest, so a changed file also reaches the code behind its pin. The store's
replies, the configuration, the command line, the paths and the errors of the AWS helper get markers the same way. The baseline documents hold every field
that the parsers accept, and ``test_the_baselines_hold_every_field_of_each_schema`` holds that, so a new field of a schema fails that test until the
baseline holds it, and then gets its markers here.

The repository does not hold the publisher's checksum list, so each run holds the tracked pin to the list of its synthetic release, and a change
that rewrites the pin of the list rewrites that one field of the pin too. The S3 stand-in answers the AWS calls of the real ``S3Store`` in the
forms the AWS command line documents. The publisher stand-in answers the three reads of ``scripts.sample.publisher``. Both are not the services:
the acceptance tests run the same commands against them.
"""

from __future__ import annotations

import contextlib
import copy
import functools
import hashlib
import io
import json
import re
import tempfile
import traceback
import unittest
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.aws_cli import AwsError
from scripts.ingestion import cli
from scripts.ingestion.check import CHECKED_FILE_FIELDS, FINDING_FIELDS, RESULT_FIELDS, result_digest
from scripts.ingestion.landing import PAIRING_FIELDS
from scripts.ingestion.manifest import MANIFEST_FIELDS, MANIFEST_LIST_FIELDS, MANIFEST_SELECTION_FIELDS, REFERENCE_FIELDS
from scripts.ingestion.selection import CASE_FILES_FIELDS, LIST_PIN_FIELDS, SELECTION_FIELDS, SELECTION_OPTIONAL, keys_digest, selection_digest
from scripts.process import CompletedProcess
from scripts.project_config import ConfigurationError
from scripts.sample import candidates, publisher
from scripts.sample.split import is_development
from tests.fast.ingestion_support import synthetic_pin

RECORDS = frozenset({"result", "manifest", "reference"})
ALLOWED: dict[str, frozenset[str]] = {
    "selection.name": RECORDS,
    "selection.measurement_period": frozenset({"manifest"}),
    "config.S3_BUCKET": RECORDS,
    "store.landed_version_id": frozenset({"result", "manifest"}),
    "store.landed_time": frozenset({"result", "manifest"}),
    "store.manifest_version_id": frozenset({"reference"}),
    "listing.Size": frozenset({"output"}),
    "clinical[].subjectid": frozenset({"output"}),
    "clinical[].caseid": frozenset({"output"}),
    "checksums[].sha256": frozenset({"output"}),
}
RANGE_FIELDS = ("selection.case_files.first", "selection.case_files.last")
PIN = json.loads(Path("tests/fixtures/vitaldb/release.json").read_text())
PREFIX = PIN["source"]["release_path"]
CONFIGURATION = {"AWS_PROFILE": "example_profile", "AWS_REGION": "us-west-2", "S3_BUCKET": "example-bucket"}


class Markers:
    """Unique markers, each of one kind, and the search for any of them in an output."""

    def __init__(self) -> None:
        self.issued: list[tuple[str, str]] = []

    def issue(self, kind: str, value: str) -> str:
        self.issued.append((kind, value))
        return value

    def text(self) -> str:
        return self.issue("text", f"zq{len(self.issued):04d}k")

    def number(self) -> int:
        return int(self.issue("number", str(8_310_000 + len(self.issued))))

    def hex(self) -> str:
        return self.issue("hex", hashlib.sha256(f"marker {len(self.issued)}".encode()).hexdigest())

    def time(self) -> str:
        return self.issue("time", f"{2100 + len(self.issued)}-03-04T05:06:07+00:00")

    def found(self, text: str) -> list[str]:
        hits = []
        for kind, value in self.issued:
            if kind == "number":
                hit = re.search(rf"(?<![0-9A-Za-z]){value}(?![0-9A-Za-z])", text) is not None
            elif kind in ("hex", "time"):
                hit = value[:10] in text
            else:
                hit = value in text
            if hit:
                hits.append(value)
        return hits


Edit = Callable[[Any, Markers], Any]


def at(document: Any, path: tuple[Any, ...]) -> Any:
    for step in path:
        document = document[step]
    return document


def edited(path: tuple[Any, ...], change: Callable[[Any, Markers], None]) -> Edit:
    """An edit that copies the document and calls ``change(container, markers)`` on the object or list at ``path``."""

    def apply(document: Any, markers: Markers) -> Any:
        result = copy.deepcopy(document)
        change(at(result, path), markers)
        return result

    return apply


def form_markers(value: Any) -> list[tuple[str, Callable[[Any, Markers], Any]]]:
    """Markers in the form of ``value``, so the change passes the form checks and reaches the code behind them."""
    if type(value) is int:
        return [("negative number", lambda old, m: -m.number())]
    if type(value) is not str:
        return []
    if re.fullmatch(r"[0-9a-f]{64}", value):
        return [("hex", lambda old, m: m.hex())]
    if re.fullmatch(r"\d{4}-\d\d-\d\dT.*", value):
        return [("time", lambda old, m: m.time())]
    return [("prefixed", lambda old, m: m.text() + old), ("suffixed", lambda old, m: old + m.text())]


def json_edits(document: Any, name: str, path: tuple[Any, ...] = ()) -> Iterator[tuple[str, str, Edit]]:
    """Every one-field change of a JSON document below ``path``: (field name, change name, edit)."""
    value = at(document, path)
    if type(value) is dict:
        yield name, "unknown field", edited(path, lambda target, m: target.update({m.text(): m.text()}))
        for key in value:
            yield f"{name}.{key}", "removed", edited(path, functools.partial(remove_field, key))
            yield from value_edits(document, f"{name}.{key}", path, key)
    elif type(value) is list and value:
        yield f"{name}[]", "appended text", edited(path, lambda target, m: target.append(m.text()))
        yield from value_edits(document, f"{name}[]", path, 0)


def value_edits(document: Any, name: str, path: tuple[Any, ...], step: Any) -> Iterator[tuple[str, str, Edit]]:
    """The changes of one value: a text marker, a number marker, the markers of its own form, then the changes inside it."""
    yield name, "text", edited(path, lambda target, m: target.__setitem__(step, m.text()))
    yield name, "number", edited(path, lambda target, m: target.__setitem__(step, m.number()))
    for label, make in form_markers(at(document, (*path, step))):
        yield name, label, edited(path, functools.partial(set_formed, step, make))
    yield from json_edits(document, name, (*path, step))


def remove_field(key: Any, target: Any, markers: Markers) -> None:
    target.pop(key)


def set_formed(step: Any, make: Callable[[Any, Markers], Any], target: Any, markers: Markers) -> None:
    target[step] = make(target[step], markers)


def edit_text(edit: Edit, text: str, markers: Markers) -> str:
    return json.dumps(edit(json.loads(text), markers))


def change_cell(separator: str, row: int, position: int, make: Callable[[str, Markers], str], text: str, markers: Markers) -> str:
    rows = [line.split(separator) for line in text.splitlines()]
    rows[row][position] = make(rows[row][position], markers)
    return "\n".join(separator.join(line) for line in rows) + "\n"


def top_edits(document: Any, name: str) -> Iterator[tuple[str, str, Callable[[str, Markers], str]]]:
    """Every change of a JSON file as text: the whole file replaced, and each one-field change of its document."""
    yield name, "not JSON", lambda text, m: m.text()
    yield name, "JSON text", lambda text, m: json.dumps(m.text())
    for field_name, change, edit in json_edits(document, name):
        yield field_name, change, functools.partial(edit_text, edit)


def line_edits(lines: list[list[str]], name: str, columns: tuple[str, ...], separator: str) -> Iterator[tuple[str, str, Callable[[str, Markers], str]]]:
    """Every one-cell change of the first data line of a line file, the whole line replaced and a line of markers added."""

    lines_start = 1 if columns and lines and lines[0] == list(columns) else 0
    first = lines[lines_start]
    for position, column in enumerate(columns or [f"{position}" for position in range(len(first))]):
        cell = first[position]
        makers: list[tuple[str, Callable[[str, Markers], str]]] = [("text", lambda old, m: m.text()), ("number", lambda old, m: str(m.number()))]
        if re.fullmatch(r"[0-9a-f]{64}", cell):
            makers.append(("hex", lambda old, m: m.hex()))
        elif not cell.isdigit():
            makers += [("prefixed", lambda old, m: m.text() + old), ("suffixed", lambda old, m: old + m.text())]
        for label, make in makers:
            yield f"{name}[].{column}", label, functools.partial(change_cell, separator, lines_start, position, make)
    yield (
        f"{name}[]",
        "line replaced",
        lambda text, m: "\n".join(m.text() if index == lines_start else line for index, line in enumerate(text.splitlines())) + "\n",
    )
    yield f"{name}[]", "line added", lambda text, m: text + separator.join(m.text() for _ in first) + "\n"


@dataclass
class Run:
    """The outputs of one run of a command sequence, by output name."""

    outputs: dict[str, str] = field(default_factory=dict)

    def add(self, name: str, text: str) -> None:
        self.outputs[name] = self.outputs.get(name, "") + text


def call_main(run: Run, main: Callable[[], int]) -> int | None:
    output, errors = io.StringIO(), io.StringIO()
    code: int | None = None
    try:
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            code = main()
    except SystemExit as stopped:
        code = stopped.code if isinstance(stopped.code, int) else 1
        run.add("exception", str(stopped.code if not isinstance(stopped.code, int) else ""))
    except Exception as error:
        run.add("exception", "".join(traceback.format_exception(error)))
    run.add("stdout", output.getvalue())
    run.add("stderr", errors.getvalue())
    return code


class FakeS3:
    """The AWS calls of ``S3Store`` against one versioned bucket, with hooks that change a reply's form or the stored state."""

    def __init__(self) -> None:
        self.objects: dict[str, list[dict[str, Any]]] = {}
        self.counter = 0
        self.next_ids: list[str] = []
        self.reply_edit: Callable[[str, Any], Any] | None = None
        self.fail_call: Exception | None = None

    def put(self, key: str, data: bytes, *, by_command: bool = False) -> dict[str, Any]:
        self.counter += 1
        version_id = self.next_ids.pop(0) if self.next_ids else f"v{self.counter:04d}"
        for item in self.objects.get(key, []):
            item["IsLatest"] = False
        entry = {"Key": key, "VersionId": version_id, "IsLatest": True, "Size": len(data), "LastModified": f"2026-01-01T00:00:{self.counter:02d}+00:00"}
        self.objects.setdefault(key, []).append({**entry, "data": data, "by_command": by_command})
        return entry

    def written(self, prefix: str) -> str:
        """What the commands wrote under ``prefix``, apart from what the test itself stored."""
        return "".join(
            item["data"].decode(errors="replace")
            for key, items in sorted(self.objects.items())
            if key.startswith(prefix)
            for item in items
            if item["by_command"]
        )

    def call(self, *arguments: str, missing: tuple[str, ...] = (), timeout: float = 0) -> Any:
        if self.fail_call is not None:
            raise self.fail_call
        name = arguments[1]
        options = {arguments[index]: arguments[index + 1] for index in range(2, len(arguments) - 1) if arguments[index].startswith("--")}
        reply = self.answer(name, options, arguments, missing)
        return self.reply_edit(name, reply) if self.reply_edit and reply is not None else reply

    def answer(self, name: str, options: dict[str, str], arguments: tuple[str, ...], missing: tuple[str, ...]) -> Any:
        if name == "get-bucket-versioning":
            return {"Status": "Enabled"}
        if name == "list-object-versions":
            found = [
                {k: v for k, v in item.items() if k not in ("data", "by_command")}
                for key, items in sorted(self.objects.items())
                if key.startswith(options["--prefix"])
                for item in items
            ]
            return {"Versions": found, "DeleteMarkers": []}
        if name == "put-object":
            if self.objects.get(options["--key"]):
                if "PreconditionFailed" in missing:
                    return None
                raise AwsError("aws s3api put-object failed: PreconditionFailed")
            return {"VersionId": self.put(options["--key"], Path(options["--body"]).read_bytes(), by_command=True)["VersionId"]}
        held = [item for item in self.objects.get(options["--key"], []) if item["VersionId"] == options["--version-id"]]
        if not held:
            if "NoSuchVersion" in missing:
                return None
            raise AwsError("aws s3api get-object failed: NoSuchVersion")
        data = held[0]["data"]
        start, _, end = options.get("--range", f"bytes=0-{len(data) - 1}").removeprefix("bytes=").partition("-")
        piece = data[int(start) : int(end) + 1]
        Path(arguments[-1]).write_bytes(piece)
        return {"VersionId": options["--version-id"], "ContentLength": len(piece)}


SOURCE = {
    "clinical_data.csv": b"caseid,age\n1,60\n2,71\n",
    "vital_files/0001.vital": b"recording one, long enough to need several blocks",
    "vital_files/0002.vital": b"recording two, long enough to need several blocks",
}


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass
class IngestionCase:
    """One change of the baseline of the three ingestion commands."""

    field: str
    change: str
    selection: Callable[[str, Markers], str] | None = None
    checksum_list: Callable[[str, Markers], str] | None = None
    repin_list: bool = False
    pairings: Callable[[str, Markers], str] | None = None
    result: Callable[[str, Markers], str] | None = None
    reference: Callable[[str, Markers], str] | None = None
    manifest: Callable[[Any, Markers], Any] | None = None
    repair_manifest: bool = False
    configuration: Callable[[dict[str, str], Markers], dict[str, str]] | None = None
    store: Callable[[FakeS3, Markers], None] | None = None
    published_store: Callable[[FakeS3, Markers], None] | None = None
    arguments: Callable[[str, list[str], Markers], list[str]] | None = None
    folder: Callable[[Markers], str] | None = None
    raise_on_configuration: bool = False


def ingestion_baseline() -> tuple[dict[str, Any], str, list[dict[str, Any]]]:
    list_text = "".join(f"{digest(data)} {path}\n" for path, data in sorted(SOURCE.items()))
    cases = sorted(path for path in SOURCE if path.startswith("vital_files/"))
    selection = {
        "name": "probe_set",
        "release": PIN["source"]["release_path"].strip("/").split("/")[-1],
        "source_bucket": PIN["source"]["bucket"],
        "source_prefix": PREFIX,
        "measurement_period": "2016-08 to 2017-06",
        "checksum_list": {"key": PIN["checksum_list"]["key"], "sha256": digest(list_text.encode())},
        "files": ["clinical_data.csv"],
        "case_files": {"pattern": "vital_files/{:04d}.vital", "first": 1, "last": len(cases)},
    }
    pairings = [
        {"source_key": PREFIX + path, "landed_key": f"landing/sync_{index:02d}/{PREFIX}{path}", "size": len(data)}
        for index, (path, data) in enumerate(sorted(SOURCE.items()))
    ]
    return selection, list_text, pairings


def repaired_manifest(document: Any) -> Any:
    """The manifest with its two digests recomputed from its own fields, where its fields still have their types."""
    with contextlib.suppress(KeyError, TypeError, AttributeError, IndexError, ValueError):
        selection, listed, entries = document["selection"], document["checksum_list"], document["entries"]
        document["check_result_sha256"] = result_digest(selection["selection_sha256"], listed["sha256"], entries)
        selection["keys_sha256"] = keys_digest(entry["source_key"] for entry in entries)
        selection["selection_sha256"] = selection_digest(
            selection["name"],
            entries[0]["release"],
            selection["source_bucket"],
            selection["source_prefix"],
            document["measurement_period"],
            listed["key"],
            listed["sha256"],
            selection["keys_sha256"],
        )
        document["check_result_sha256"] = result_digest(selection["selection_sha256"], listed["sha256"], entries)
    return document


def run_ingestion(case: IngestionCase, markers: Markers) -> Run:
    run = Run()
    selection, list_text, pairings = ingestion_baseline()
    baseline_list = list_text
    with tempfile.TemporaryDirectory(prefix="boundary_") as root:
        folder = Path(root) / (case.folder(markers) if case.folder else "work")
        folder.mkdir()
        s3 = FakeS3()
        for record, (_, data) in zip(pairings, sorted(SOURCE.items()), strict=True):
            s3.put(record["landed_key"], data)
        if case.store:
            case.store(s3, markers)
        if case.checksum_list:
            list_text = case.checksum_list(list_text, markers)
            if case.repin_list:
                selection["checksum_list"]["sha256"] = digest(list_text.encode())
        selection_text = json.dumps(selection)
        if case.selection:
            selection_text = case.selection(selection_text, markers)
        pairings_text = json.dumps(pairings)
        if case.pairings:
            pairings_text = case.pairings(pairings_text, markers)
        paths = {name: folder / name for name in ("selection.json", "SHA256SUMS.txt", "pairings.json", "result.json", "reference.json")}
        paths["selection.json"].write_text(selection_text)
        paths["SHA256SUMS.txt"].write_text(list_text)
        paths["pairings.json"].write_text(pairings_text)
        configuration = case.configuration(dict(CONFIGURATION), markers) if case.configuration else dict(CONFIGURATION)
        loader = mock.Mock(side_effect=ConfigurationError(f".env: {markers.text()}")) if case.raise_on_configuration else mock.Mock(return_value=configuration)
        commands = {
            "check": ["check", "--selection", str(paths["selection.json"]), "--checksum-list", str(paths["SHA256SUMS.txt"])]
            + ["--pairings", str(paths["pairings.json"]), "--result", str(paths["result.json"])],
            "publish": [
                "publish",
                "--selection",
                str(paths["selection.json"]),
                "--result",
                str(paths["result.json"]),
                "--reference",
                str(paths["reference.json"]),
            ],
            "verify": ["verify", "--reference", str(paths["reference.json"])],
        }
        with (
            mock.patch.object(cli, "load_configuration", loader),
            mock.patch.object(cli, "Aws", return_value=s3),
            synthetic_pin((list_text if case.repin_list else baseline_list).encode()),
        ):
            for command, arguments in commands.items():
                if command == "publish" and case.result and paths["result.json"].is_file():
                    paths["result.json"].write_text(case.result(paths["result.json"].read_text(), markers))
                if command == "verify":
                    if case.published_store:
                        case.published_store(s3, markers)
                    if case.manifest and paths["reference.json"].is_file():
                        rewrite_manifest(s3, paths["reference.json"], case.manifest, case.repair_manifest, markers)
                    if case.reference and paths["reference.json"].is_file():
                        paths["reference.json"].write_text(case.reference(paths["reference.json"].read_text(), markers))
                if case.arguments:
                    arguments = case.arguments(command, arguments, markers)
                call_main(run, functools.partial(cli.main, arguments))
                written = {"check": "result.json", "publish": "reference.json"}.get(command)
                if written and paths[written].is_file():
                    run.add(written.removesuffix(".json"), paths[written].read_text())
        run.add("manifest", s3.written("manifests/"))
    return run


def rewrite_manifest(s3: FakeS3, reference_path: Path, change: Callable[[Any, Markers], Any], repair: bool, markers: Markers) -> None:
    """Store a changed manifest at the key of its own digest and point the reference at it, so the change reaches the checks behind the reference."""
    reference = json.loads(reference_path.read_text())
    stored = next(item for item in s3.objects[reference["key"]] if item["VersionId"] == reference["version_id"])
    document = change(json.loads(stored["data"]), markers)
    if repair:
        document = repaired_manifest(document)
    data = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
    sha256 = digest(data)
    name = reference["key"].split("/")[1]
    key = f"manifests/{name}/{sha256}.json"
    entry = s3.put(key, data)
    reference_path.write_text(json.dumps({"bucket": reference["bucket"], "key": key, "version_id": entry["VersionId"], "sha256": sha256}))


def reply_edit(call: str, change: Callable[[Any, Markers], Any], markers: Markers) -> Callable[[str, Any], Any]:
    def edit(name: str, reply: Any) -> Any:
        return change(copy.deepcopy(reply), markers) if name == call else reply

    return edit


def first_landed_entry(change: Callable[[dict[str, Any], Markers], None]) -> Callable[[Any, Markers], Any]:
    def apply(reply: Any, markers: Markers) -> Any:
        for entry in reply["Versions"]:
            if entry["Key"].startswith("landing/"):
                change(entry, markers)
                break
        return reply

    return apply


def ingestion_cases() -> Iterator[IngestionCase]:
    selection, list_text, pairings = ingestion_baseline()
    for name, change, edit in top_edits(selection, "selection"):
        if name in RANGE_FIELDS and change == "number":
            yield IngestionCase(
                name,
                "one-file range",
                selection=lambda text, m: json.dumps(
                    json.loads(text) | {"case_files": {**selection["case_files"], **dict.fromkeys(("first", "last"), m.number())}}
                ),
            )
            continue
        if name in RANGE_FIELDS and change == "negative number":
            yield IngestionCase(name, change, selection=edit)
            continue
        yield IngestionCase(name, change, selection=edit)
    for name, change, edit in line_edits([line.split(" ") for line in list_text.splitlines()], "checksums", ("sha256", "path"), " "):
        yield IngestionCase(name, change, checksum_list=edit)
        yield IngestionCase(name, f"{change}, pin rewritten", checksum_list=edit, repin_list=True)
    yield IngestionCase("checksums", "replaced, pin rewritten", checksum_list=lambda text, m: m.text(), repin_list=True)
    for name, change, edit in top_edits(pairings, "pairings"):
        yield IngestionCase(name, change, pairings=edit)
    yield from published_input_cases()
    for key in CONFIGURATION:
        yield IngestionCase(f"config.{key}", "text", configuration=functools.partial(set_configuration, key))
    yield IngestionCase("config", "configuration error with text", raise_on_configuration=True)
    yield IngestionCase("aws", "error text", store=lambda s3, m: setattr(s3, "fail_call", AwsError(f"aws s3api list-object-versions failed: {m.text()}")))
    for entry_field in ("Key", "VersionId", "IsLatest", "Size", "LastModified"):
        for label, make in (("text", lambda m: m.text()), ("number", lambda m: m.number())):
            yield IngestionCase(
                f"reply.list.{entry_field}",
                label,
                store=functools.partial(edit_landed_entry, entry_field, make),
            )
    for part in ("Versions", "DeleteMarkers"):
        for label, make in (("text", lambda m: m.text()), ("number", lambda m: m.number()), ("object", lambda m: {m.text(): m.text()})):
            yield IngestionCase(
                f"reply.list.{part}",
                label,
                store=functools.partial(edit_reply_field, "list-object-versions", part, make),
            )
    for call, reply_field in (("get-object", "VersionId"), ("get-object", "ContentLength"), ("get-bucket-versioning", "Status"), ("put-object", "VersionId")):
        for label, make in (("text", lambda m: m.text()), ("number", lambda m: m.number())):
            yield IngestionCase(
                f"reply.{call}.{reply_field}",
                label,
                store=functools.partial(edit_reply_field, call, reply_field, make),
            )
    yield IngestionCase("store.landed_version_id", "text", store=lambda s3, m: rename_first_landed(s3, "VersionId", m.text()))
    yield IngestionCase("store.landed_time", "time", store=lambda s3, m: rename_first_landed(s3, "LastModified", m.time()))
    yield IngestionCase("store.manifest_version_id", "text", store=lambda s3, m: setattr(s3, "next_ids", [m.text()]))
    yield IngestionCase("store.landed_object", "substituted after the manifest", published_store=lambda s3, m: substitute_first(s3, m))
    yield IngestionCase("paths", "folder", folder=lambda m: m.text())
    yield IngestionCase(
        "paths", "absent input", arguments=lambda command, arguments, m: [m.text() if value.endswith("selection.json") else value for value in arguments]
    )
    yield IngestionCase(
        "paths",
        "unwritable result",
        arguments=lambda command, arguments, m: [f"{value}/{m.text()}/x" if value.endswith("result.json") else value for value in arguments],
    )
    yield IngestionCase("arguments.bucket", "text", arguments=lambda command, arguments, m: ["--bucket", m.text(), *arguments])
    yield IngestionCase(
        "arguments.source_key", "text", arguments=lambda command, arguments, m: [*arguments, "--source-key", m.text()] if command == "verify" else arguments
    )
    yield IngestionCase(
        "arguments.source_key",
        "prefixed",
        arguments=lambda command, arguments, m: [*arguments, "--source-key", m.text() + PREFIX + "clinical_data.csv"] if command == "verify" else arguments,
    )
    yield IngestionCase("arguments", "unknown word", arguments=lambda command, arguments, m: [*arguments, m.text()])
    yield IngestionCase("arguments", "unknown option", arguments=lambda command, arguments, m: [*arguments, f"--{m.text()}"])


def set_configuration(key: str, configuration: dict[str, str], markers: Markers) -> dict[str, str]:
    return configuration | {key: markers.text()}


def edit_landed_entry(entry_field: str, make: Callable[[Markers], Any], s3: FakeS3, markers: Markers) -> None:
    def change(entry: dict[str, Any], m: Markers) -> None:
        entry[entry_field] = make(m)

    s3.reply_edit = reply_edit("list-object-versions", first_landed_entry(change), markers)


def edit_reply_field(call: str, reply_field: str, make: Callable[[Markers], Any], s3: FakeS3, markers: Markers) -> None:
    def change(reply: Any, m: Markers) -> Any:
        return reply | {reply_field: make(m)}

    s3.reply_edit = reply_edit(call, change, markers)


def edit_with_finding(edit: Callable[[str, Markers], str], text: str, markers: Markers) -> str:
    return edit(json.dumps(json.loads(text) | {"findings": [BASELINE_FINDING]}), markers)


def rename_first_landed(s3: FakeS3, name: str, value: str) -> None:
    key = sorted(key for key in s3.objects if key.startswith("landing/"))[0]
    s3.objects[key][-1][name] = value


def substitute_first(s3: FakeS3, markers: Markers) -> None:
    key = sorted(key for key in s3.objects if key.startswith("landing/"))[0]
    s3.put(key, markers.text().encode())


def published_input_cases() -> Iterator[IngestionCase]:
    """The changes of the check result, the manifest reference and the stored manifest, from documents of a baseline run."""
    result, reference, manifest = baseline_records()
    for name, change, edit in top_edits(result, "result"):
        yield IngestionCase(name, change, result=edit)
    with_finding = result | {"findings": [BASELINE_FINDING]}
    for name, change, edit in top_edits(with_finding, "result"):
        if name.startswith("result.findings[]"):
            yield IngestionCase(name, change, result=functools.partial(edit_with_finding, edit))
    for name, change, edit in top_edits(reference, "reference"):
        yield IngestionCase(name, change, reference=edit)
    for name, change, edit in json_edits(manifest, "manifest"):
        yield IngestionCase(name, change, manifest=edit)
        yield IngestionCase(name, f"{change}, digests rewritten", manifest=edit, repair_manifest=True)


def baseline_records() -> tuple[Any, Any, Any]:
    captured: dict[str, Any] = {}

    def keep(s3: FakeS3, markers: Markers) -> None:
        manifests = [item for key, items in s3.objects.items() if key.startswith("manifests/") for item in items]
        captured["manifest"] = json.loads(manifests[-1]["data"])

    case = IngestionCase("baseline", "none", published_store=keep)
    run = run_ingestion(case, Markers())
    return json.loads(run.outputs["result"]), json.loads(run.outputs["reference"]), captured["manifest"]


BASELINE_FINDING = {"position": 1, "cause": "missing", "source_key": ""}


class FakePublisher:
    """The three reads of the publisher's files: the key listing, a key of the public bucket and the track list address."""

    def __init__(self, files: dict[str, bytes], listing: list[dict[str, Any]]) -> None:
        self.files = files
        self.listing = listing

    def __call__(self, program: str, arguments: list[str], **_: Any) -> CompletedProcess[str]:
        if program == "aws" and arguments[:2] == ["s3api", "list-objects-v2"]:
            return CompletedProcess([program], 0, json.dumps({"Contents": self.listing}), "")
        if program == "aws":
            Path(arguments[3]).write_bytes(self.files[arguments[2].removeprefix(f"s3://{publisher.BUCKET}/")])
            return CompletedProcess([program], 0, "", "")
        Path(arguments[arguments.index("--output") + 1]).write_bytes(self.files[arguments[-1]])
        return CompletedProcess([program], 0, "", "")


def sample_cases_table() -> list[tuple[int, int, bool, int]]:
    """(case, patient, arterial waveform, duration) for a release in which every rule of ``SLOTS`` finds its cases."""
    development = [patient for patient in range(1, 400) if is_development(patient)]
    test = [patient for patient in range(1, 400) if not is_development(patient)]
    plan = [
        (development[0], True, 2),
        (development[1], True, 2),
        (test[0], True, 2),
        (development[2], False, 1),
        (development[3], False, 1),
        (test[1], False, 1),
        (test[2], True, 1),
        (development[4], True, 1),
        (development[5], True, 1),
        (development[6], True, 1),
    ]
    rows: list[tuple[int, int, bool, int]] = []
    for patient, arterial, count in plan:
        for _ in range(count):
            rows.append((len(rows) + 1, patient, arterial, 600 + 37 * len(rows)))
    return rows


@dataclass
class SampleCase:
    field: str
    change: str
    pin: Callable[[str, Markers], str] | None = None
    tracked: Callable[[str, Markers], str] | None = None
    download: str | None = None
    content: Callable[[str, Markers], str] | None = None
    repin: bool = False
    listing: Callable[[list[dict[str, Any]], Markers], list[dict[str, Any]]] | None = None
    folder: Callable[[Markers], str] | None = None
    arguments: Callable[[str, list[str], Markers], list[str]] | None = None


DOWNLOADS = {"checksums": publisher.CHECKSUM_LIST_KEY, "clinical": f"{publisher.RELEASE_PREFIX}clinical_data.csv", "tracks": publisher.TRACK_LIST_URL}


def sample_baseline() -> tuple[dict[str, str], list[dict[str, Any]]]:
    rows = sample_cases_table()
    vital = {f"{publisher.CASE_PREFIX}{case:04d}.vital": f"recording {case}".encode() * (case + 2) for case, _, _, _ in rows}
    lab = b"caseid,dt,name,result\n1,0,cr,0.9\n"
    clinical = "caseid,subjectid,casestart,caseend\n" + "".join(f"{case},{patient},0,{duration}\n" for case, patient, _, duration in rows)
    tracks = "caseid,tname,tid\n" + "".join(f"{case},{candidates.ARTERIAL_TRACK if arterial else 'Solar8000/HR'},t{case}\n" for case, _, arterial, _ in rows)
    sums = {f"{publisher.RELEASE_PREFIX}clinical_data.csv": digest(clinical.encode()), f"{publisher.RELEASE_PREFIX}lab_data.csv": digest(lab)}
    sums |= {key: digest(data) for key, data in vital.items()}
    checksums = "".join(f"{value} {key.removeprefix(publisher.RELEASE_PREFIX)}\n" for key, value in sorted(sums.items()))
    listing = [{"Key": key, "Size": len(data)} for key, data in sorted(vital.items())]
    listing += [
        {"Key": f"{publisher.RELEASE_PREFIX}clinical_data.csv", "Size": len(clinical)},
        {"Key": f"{publisher.RELEASE_PREFIX}lab_data.csv", "Size": len(lab)},
    ]
    return {"checksums": checksums, "clinical": clinical, "tracks": tracks, "lab": lab.decode()}, listing


def sample_pin(contents: dict[str, str]) -> dict[str, Any]:
    pin: dict[str, Any] = copy.deepcopy(PIN)
    pin["checksum_list"].update(size_bytes=len(contents["checksums"].encode()), sha256=digest(contents["checksums"].encode()))
    for table in pin["tables"]:
        name = "clinical" if table["key"].endswith("clinical_data.csv") else "lab"
        table.update(size_bytes=len(contents[name].encode()), sha256=digest(contents[name].encode()))
    pin["track_list"].update(size_bytes=len(contents["tracks"].encode()), sha256=digest(contents["tracks"].encode()))
    return pin


def run_sample(case: SampleCase, markers: Markers) -> Run:
    run = Run()
    contents, listing = sample_baseline()
    baseline_pin = sample_pin(contents)
    if case.download and case.content:
        before = contents[case.download]
        contents[case.download] = case.content(before, markers)
        if case.repin and case.download == "clinical":
            contents["checksums"] = contents["checksums"].replace(digest(before.encode()), digest(contents["clinical"].encode()))
    pin = sample_pin(contents) if case.repin else baseline_pin
    if case.listing:
        listing = case.listing(listing, markers)
    files = {DOWNLOADS[name]: contents[name].encode() for name in DOWNLOADS}
    with tempfile.TemporaryDirectory(prefix="boundary_sample_") as root:
        folder = Path(root) / (case.folder(markers) if case.folder else "work")
        folder.mkdir()
        pin_path, tracked_path, output_path = folder / "release.json", folder / "candidates.tsv", folder / "built.tsv"
        pin_text = json.dumps(pin)
        pin_path.write_text(case.pin(pin_text, markers) if case.pin else pin_text)
        tracked_path.write_text(baseline_tracked())
        if case.tracked:
            tracked_path.write_text(case.tracked(tracked_path.read_text(), markers))
        commands = {
            "build": ["candidates", "build", "--release", str(pin_path), "--output", str(output_path)],
            "check": ["candidates", "check", "--release", str(pin_path), "--candidates", str(tracked_path)],
        }
        with mock.patch.object(publisher, "run_command", FakePublisher(files, listing)):
            for command, arguments in commands.items():
                if case.arguments:
                    arguments = case.arguments(command, arguments, markers)
                with mock.patch("sys.argv", arguments):
                    call_main(run, candidates.main)
        if output_path.is_file():
            run.add("output", output_path.read_text())
    return run


@functools.cache
def baseline_tracked() -> str:
    """The candidate file that a build of the baseline release writes."""
    contents, listing = sample_baseline()
    files = {DOWNLOADS[name]: contents[name].encode() for name in DOWNLOADS}
    with tempfile.TemporaryDirectory(prefix="boundary_sample_") as root:
        pin_path, output_path = Path(root) / "release.json", Path(root) / "built.tsv"
        pin_path.write_text(json.dumps(sample_pin(contents)))
        with (
            mock.patch.object(publisher, "run_command", FakePublisher(files, listing)),
            mock.patch("sys.argv", ["candidates", "build", "--release", str(pin_path), "--output", str(output_path)]),
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            candidates.main()
        return output_path.read_text()


def sample_cases() -> Iterator[SampleCase]:
    contents, listing = sample_baseline()
    for name, change, edit in top_edits(sample_pin(contents), "pin"):
        yield SampleCase(name, change, pin=edit)
    tracked = baseline_tracked()
    for name, change, edit in line_edits([line.split("\t") for line in tracked.splitlines()], "tracked", candidates.HEADER, "\t"):
        yield SampleCase(name, change, tracked=edit)
    yield SampleCase("tracked", "header replaced", tracked=lambda text, m: m.text() + "\n" + "\n".join(text.splitlines()[1:]) + "\n")
    columns = {"checksums": ("sha256", "path"), "clinical": ("caseid", "subjectid", "casestart", "caseend"), "tracks": ("caseid", "tname", "tid")}
    separators = {"checksums": " ", "clinical": ",", "tracks": ","}
    for download, names in columns.items():
        lines = [line.split(separators[download]) for line in contents[download].splitlines()]
        for name, change, edit in line_edits(lines, download, names, separators[download]):
            yield SampleCase(name, change, download=download, content=edit)
            yield SampleCase(name, f"{change}, pin rewritten", download=download, content=edit, repin=True)
    for name, change, edit in json_edits(listing, "listing"):
        yield SampleCase(name.replace("listing[].", "listing."), change, listing=edit)
    yield SampleCase("paths", "folder", folder=lambda m: m.text())
    yield SampleCase(
        "paths",
        "absent pin",
        arguments=lambda command, arguments, m: [f"{value}.{m.text()}" if value.endswith("release.json") else value for value in arguments],
    )
    yield SampleCase(
        "paths",
        "absent tracked file",
        arguments=lambda command, arguments, m: [f"{value}.{m.text()}" if value.endswith("candidates.tsv") else value for value in arguments],
    )
    yield SampleCase(
        "paths",
        "unwritable output",
        arguments=lambda command, arguments, m: [f"{value}/{m.text()}/x" if value.endswith("built.tsv") else value for value in arguments],
    )
    yield SampleCase("arguments", "unknown word", arguments=lambda command, arguments, m: [*arguments, m.text()])
    yield SampleCase("arguments", "unknown option", arguments=lambda command, arguments, m: [*arguments, f"--{m.text()}"])


def leaks(field_name: str, change: str, run: Run, markers: Markers) -> list[str]:
    allowed = next((outputs for prefix, outputs in ALLOWED.items() if field_name == prefix or field_name.startswith(f"{prefix}.")), frozenset())
    found = []
    for output, text in sorted(run.outputs.items()):
        if output in allowed:
            continue
        for marker in markers.found(text):
            line = next((line for line in text.splitlines() if marker[:10] in line), "")[:160]
            found.append(f"{field_name} ({change}): marker {marker[:16]} reached {output}: {line}")
    return found


class OutputBoundaryTest(unittest.TestCase):
    def test_control_the_baselines_pass_every_command(self) -> None:
        run = run_ingestion(IngestionCase("baseline", "none"), Markers())
        self.assertIn("check passed", run.outputs["stdout"])
        self.assertIn("verified: 3 object versions", run.outputs["stdout"])
        self.assertNotIn("exception", run.outputs)
        sample = run_sample(SampleCase("baseline", "none"), Markers())
        self.assertIn("wrote 13 candidates", sample.outputs["stdout"])
        self.assertIn("13 tracked candidates equal the rebuild", sample.outputs["stdout"])
        self.assertEqual("", sample.outputs["stderr"])

    def test_the_baselines_hold_every_field_of_each_schema(self) -> None:
        selection, list_text, pairings = ingestion_baseline()
        result, reference, manifest = baseline_records()
        contents, listing = sample_baseline()
        pin = sample_pin(contents)
        held = {
            "selection": (set(selection), {*SELECTION_FIELDS, *SELECTION_OPTIONAL}),
            "selection.checksum_list": (set(selection["checksum_list"]), set(LIST_PIN_FIELDS)),
            "selection.case_files": (set(selection["case_files"]), set(CASE_FILES_FIELDS)),
            "pairings[]": (set(pairings[0]), set(PAIRING_FIELDS)),
            "result": (set(result), set(RESULT_FIELDS)),
            "result.files[]": (set(result["files"][0]), set(CHECKED_FILE_FIELDS)),
            "result.findings[]": (set(BASELINE_FINDING), set(FINDING_FIELDS)),
            "reference": (set(reference), set(REFERENCE_FIELDS)),
            "manifest": (set(manifest), set(MANIFEST_FIELDS)),
            "manifest.selection": (set(manifest["selection"]), set(MANIFEST_SELECTION_FIELDS)),
            "manifest.checksum_list": (set(manifest["checksum_list"]), set(MANIFEST_LIST_FIELDS)),
            "manifest.entries[]": (set(manifest["entries"][0]), set(CHECKED_FILE_FIELDS)),
            "pin": (set(pin), set(candidates.RELEASE_FIELDS)),
            "pin.source": (set(pin["source"]), set(candidates.SOURCE_FIELDS)),
            "pin.checksum_list": (set(pin["checksum_list"]), set(candidates.FILE_PIN_FIELDS)),
            "pin.tables[]": (set(pin["tables"][0]), set(candidates.FILE_PIN_FIELDS)),
            "pin.track_list": (set(pin["track_list"]), set(candidates.TRACK_LIST_FIELDS)),
            "pin.credit": (set(pin["credit"]), set(candidates.CREDIT_FIELDS)),
            "tracked": (set(baseline_tracked().splitlines()[0].split("\t")), set(candidates.HEADER)),
            "tracks": (set(contents["tracks"].splitlines()[0].split(",")), set(publisher.TRACK_LIST_COLUMNS)),
            "clinical": (set(contents["clinical"].splitlines()[0].split(",")), set(publisher.CLINICAL_COLUMNS)),
            "listing[]": (set(listing[0]), {"Key", "Size"}),
        }
        for name, (baseline, schema) in held.items():
            with self.subTest(name):
                self.assertEqual(schema, baseline)
        self.assertEqual(2, len(list_text.splitlines()[0].split(" ")))

    def test_no_marker_of_an_ingestion_input_reaches_an_output_it_is_not_allowed_in(self) -> None:
        found: list[str] = []
        count = 0
        for case in ingestion_cases():
            markers = Markers()
            found += leaks(case.field, case.change, run_ingestion(case, markers), markers)
            count += 1
        self.assertGreater(count, 150)
        self.assertEqual([], found, f"{len(found)} leaks in {count} runs:\n" + "\n".join(found))

    def test_no_marker_of_a_sample_input_reaches_an_output_it_is_not_allowed_in(self) -> None:
        found: list[str] = []
        count = 0
        for case in sample_cases():
            markers = Markers()
            found += leaks(case.field, case.change, run_sample(case, markers), markers)
            count += 1
        self.assertGreater(count, 150)
        self.assertEqual([], found, f"{len(found)} leaks in {count} runs:\n" + "\n".join(found))


if __name__ == "__main__":
    unittest.main()
