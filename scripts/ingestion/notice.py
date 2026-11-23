"""The one output boundary of the ingestion and fixed-sample commands: every finding, refusal and error is a ``Notice``, and ``render`` makes its text.

A notice holds a ``Code`` of the fixed enumeration below, whose value is the fixed text of the message, and only these values besides:

- a ``Subject``, from a fixed enumeration of the inputs, outputs and calls that a message names by their role;
- field names of the closed input schemas, each in ``FIELD_NAMES``;
- numbers that the code computed: the place of a file in the sorted selection, a line number, a count, a deadline, a limit or an exit status;
- a ``Cause`` of a finding, from a fixed enumeration;
- a source key that the check verified: the tracked pin's release path and a path that the pinned checksum list names;
- a SHA-256 that the code computed.

No notice holds a value of an input that nothing verified, so no message can show one. ``ClosedError`` is the base of every error the two packages
raise: it holds notices only, and its text is their rendering. The commands write each line of standard error and standard output with ``render``.
"""

from __future__ import annotations

import re
import string
from dataclasses import dataclass
from enum import StrEnum, unique

SHA256 = re.compile(r"[0-9a-f]{64}")
VERIFIED_KEY = re.compile(r"[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)+")
FIELD_NAMES = frozenset(
    {
        "bucket",
        "case_files",
        "cause",
        "changes",
        "check_result_sha256",
        "checksum_list",
        "checksum_list_key",
        "checksum_list_sha256",
        "citation",
        "credit",
        "destination_tag",
        "entries",
        "file_count",
        "files",
        "findings",
        "first",
        "format",
        "key",
        "keys_sha256",
        "landed_key",
        "last",
        "licence",
        "list_sha256",
        "measurement_period",
        "name",
        "page",
        "pattern",
        "position",
        "region",
        "release",
        "release_path",
        "result_sha256",
        "retrieved_utc",
        "rows",
        "rows_sha256",
        "selection",
        "selection_name",
        "selection_sha256",
        "sha256",
        "size",
        "size_bytes",
        "source",
        "source_bucket",
        "source_key",
        "source_prefix",
        "source_tag",
        "status",
        "tables",
        "track_list",
        "url",
        "version_id",
    }
)


@unique
class Subject(StrEnum):
    """What a message names by its role, never by its path or value."""

    SELECTION = "the selection"
    SELECTION_LIST = "the selection's checksum_list"
    SELECTION_CASES = "the selection's case_files"
    CHECKSUM_LIST = "the checksum list"
    PAIRINGS = "the pairing records"
    PAIRING = "a pairing record"
    RESULT = "the check result"
    RESULT_FILE = "a file of the check result"
    RESULT_FINDING = "a finding of the check result"
    REFERENCE = "the manifest reference"
    MANIFEST = "the manifest"
    MANIFEST_SELECTION = "the manifest's selection"
    MANIFEST_LIST = "the manifest's checksum list"
    MANIFEST_ENTRY = "an entry of the manifest"
    TRACKED_PIN = "the tracked release pin"
    RELEASE_PIN = "the release pin"
    PIN_TOP = "release pin"
    PIN_SOURCE = "source"
    PIN_LIST = "checksum_list"
    PIN_TABLE = "tables"
    PIN_TABLE_ENTRY = "table"
    PIN_TRACKS = "track_list"
    PIN_CREDIT = "credit"
    CANDIDATE_FILE = "the candidate file"
    CANDIDATE_LINE = "candidate file line"
    REBUILT_CANDIDATE = "rebuilt candidate"
    CLINICAL_TABLE = "the clinical table"
    LAB_TABLE = "the lab table"
    TRACK_LIST = "the track list"
    LISTING_RELEASE = "listing the release"
    READING_LIST = "reading the checksum list"
    READING_CLINICAL = "reading the clinical table"
    READING_TRACKS = "reading the track list"
    LIST_CALL = "list-object-versions"
    GET_CALL = "get-object"
    PUT_CALL = "put-object"
    VERSIONING_CALL = "get-bucket-versioning"
    JOB_RECORD = "the job record"
    JOB_LOG = "the job log"
    STAGING_SAMPLES = "the staging samples"
    RUN_RECORD = "the run record"
    ROUTES_FILE = "the routes file"
    STAGING_PEAK = "the staging peak record"
    SAMPLE_PIN = "the Platform sample pin"


@unique
class Cause(StrEnum):
    """Why one selected file failed the check, or why the gate refused one key."""

    MISSING = "missing"
    DUPLICATE_PAIRING = "duplicate_pairing"
    LANDED_OBJECT_MISSING = "landed_object_missing"
    PAIRING_SIZE_DIFFERS = "pairing_size_differs"
    CHECKSUM_DIFFERS = "checksum_differs"
    NOT_IN_CHECKSUM_LIST = "not_in_checksum_list"
    NULL_VERSION_ID = "null_version_id"
    NOT_IN_MANIFEST = "not_in_manifest"
    BUCKET_DIFFERS = "bucket_differs"
    VERSION_SUBSTITUTED = "version_substituted"
    SIZE_DIFFERS = "size_differs"
    LANDED_SHARED = "landed_shared"


@unique
class Code(StrEnum):
    """Every message of the two packages. The value is the fixed text; each ``{slot}`` is filled from the notice's own closed values."""

    NOT_JSON = "{subject} is not valid JSON"
    REPEATED_FIELD = "{subject} names a field more than once"
    OBJECT_FIELDS = "{subject} must be an object with the fields: {fields}"
    OBJECT_FIELDS_OPTIONAL = "{subject} must be an object with the fields: {fields}; optional: {optional}"
    WRONG_TYPE = "{subject} has a field of the wrong type: {fields}"
    USAGE = "the command line is not valid; run it with --help for its commands and options"
    CANNOT_READ = "cannot read {subject}"
    CANNOT_WRITE = "cannot write {subject}"
    OUTPUT_IS_INPUT = "an output file of the command is also one of its input files; name another file for the output"
    CONFIGURATION_INVALID = "the configuration in .env is missing or holds a value that is not valid; .env.example lists every key"
    AWS_FAILED = "an AWS call failed; its full error text is kept in an owner-only file under runs/raw_errors"
    AWS_FAILED_NOT_KEPT = "an AWS call failed; its full error text could not be kept"
    AWS_TIMED_OUT = "an AWS call passed its deadline"
    BUCKET_FORM = "--bucket must be S3_BUCKET of .env or a test bucket of the form <S3_BUCKET>-test-<8 digits><6 hex digits>"
    CASE_PATTERN = "the selection's case_files pattern is not a path with one {{:0Nd}} place for the number"
    CASE_RANGE = "the selection's case_files must have 1 <= first <= last <= {total}, the case numbers of the release"
    PIN_UNREADABLE = "the tracked release pin cannot be read"
    PIN_INVALID = "the tracked release pin is not valid"
    NAME_FORM = "name must be lower-case letters, digits and underscores"
    PERIOD_EMPTY = "measurement_period must not be empty"
    NO_FILES = "the selection names no file"
    FILE_TWICE = "a file is named twice"
    FILE_NOT_RELATIVE = "files[{position}] is not a relative path"
    FILES_NOT_TEXT = "the selection's files must all be text"
    SOURCE_NOT_PIN = "the selection's {fields} is not the tracked pin's"
    FULL_RELEASE_FILES = "the full release's files are not exactly clinical_data.csv, lab_data.csv and {total} case files"
    PAIRINGS_NOT_LIST = "the pairing records must be a list"
    PAIRING_EMPTY = "a pairing record needs a source key, a landed key and a size of zero or more"
    LIST_NOT_PINNED = "the checksum list's SHA-256 differs from the pinned SHA-256"
    LIST_NOT_TEXT = "the checksum list is not UTF-8 text"
    LIST_LINE_FORM = "line {position} of the checksum list is not a SHA-256, one space and a path"
    LIST_LINE_REPEAT = "line {position} of the checksum list repeats a path"
    LIST_EMPTY = "the checksum list is empty"
    RESULT_EARLIER = "the check result is not of this format: an earlier check wrote it; rerun check to write one of this format"
    FINDING_CAUSE = "a finding of the check result names no known cause"
    FINDING_KEY = "{key}: {cause}"
    FINDING_POSITION = "selected file {position}: {cause}"
    PROGRESS = "checked {count} of {total} files"
    CHECK_FAILED = "check failed: {count} faults in {total} selected files; no manifest may be published"
    CHECK_PASSED = "check passed: {count} of {total} selected files verified; result {digest}"
    NOT_PASSED = "the check did not pass"
    NOT_THIS_SELECTION = "the check result is not for this selection and its pinned checksum list"
    INCOMPLETE = "the check result does not hold every selected file once"
    EDITED = "the check result was edited after the check"
    RESULT_NULL_VERSION = "the check result binds a null version ID, which versioning does not keep apart from the next write"
    OTHER_BUCKET = "the check result is for another bucket"
    GONE_AFTER_CHECK = "checked file {position}: no longer present after the check"
    NEW_VERSION_AFTER_CHECK = "checked file {position}: a new version landed after the check"
    MANIFEST_NULL = "the manifest is a null version, which versioning does not keep apart from the next write"
    KEY_OTHER_CONTENT = "the manifest key holds other content than its name says"
    READ_BACK_DIFFERS = "the published manifest reads back with other content"
    PUBLISHED = "manifest published, SHA-256 {digest}; the reference file names its key and version"
    REFERENCE_BUCKET = "the manifest reference names another bucket than the one in use"
    REFERENCE_NULL = "the manifest reference names a null version"
    KEY_NOT_DIGEST = "the manifest key does not hold the SHA-256 of the reference"
    MANIFEST_VERSION_MISSING = "the manifest version of the reference does not exist"
    MANIFEST_UNREADABLE = "the manifest cannot be read"
    MANIFEST_DIGEST_DIFFERS = "the manifest's SHA-256 differs from its reference"
    MANIFEST_EARLIER = "the manifest is of an earlier format that this code does not read; rerun check and publish to make one of this format"
    MANIFEST_NOT_PASSED = "the manifest is not a passed manifest of this format"
    KEY_OTHER_SELECTION = "the manifest key names another selection than the manifest does"
    MANIFEST_LIST_NOT_PIN = "the manifest's checksum list is not the tracked pin's"
    MANIFEST_NULL_ENTRY = "the manifest binds an object with a null version ID"
    ENTRIES_DIGEST = "the manifest's entries do not match the digest of the check that produced it"
    SELECTION_DIGEST = "the manifest's selection, period, list or entries do not match its selection digest"
    GATE_REFUSED = "refused before any payload was read"
    REQUESTED_POSITION = "requested key {position}: {cause}"
    ENTRY_POSITION = "manifest entry {position}: {cause}"
    VERIFIED = "verified: {count} object versions are the versions of the passed manifest"
    REPLY_FORM = "the reply of {subject} is not in the documented form"
    LISTING_ENTRY_FORM = "an entry of the version listing is not in the documented form"
    LISTING_FORM = "the version listing is not in the documented form"
    VERSION_MISSING = "the version does not exist"
    OTHER_VERSION = "the reply was for another version than the one asked for"
    NO_BYTE_COUNT = "the reply gave no byte count"
    SHORT_REPLY = "the reply held another number of bytes than the range asked for"
    TOO_LARGE = "the object is larger than {count} bytes"
    NO_DATA = "the reply wrote no data"
    VERSIONING_OFF = "versioning is not enabled on the bucket, so nothing was written"
    NO_VERSION_ID = "the write returned no version ID; versioning is not enabled on the bucket"
    NOT_LISTED = "the written version is not in the listing"
    EXISTS_UNLISTED = "the write was refused because the key exists, but no current version is listed"
    NULL_HELD = "the key holds a null version, which versioning does not keep apart from the next write"
    OTHER_CONTENT = "the key already holds other content"
    READ_ONLY_STORE = "this store is read only, so nothing was written"
    TIMED_OUT = "{subject} timed out after {count} seconds"
    NEEDS_AWS = "{subject} needs aws, which is not installed or not on PATH"
    NEEDS_CURL = "{subject} needs curl, which is not installed or not on PATH"
    EXIT_STATUS = "{subject} failed with exit status {count}"
    RELEASE_LISTING_FORM = "the release listing is not JSON with a Contents list of entries that each hold a Key and a Size"
    PINNED_SIZE = "{subject} does not have the pinned size"
    PINNED_DIGEST = "{subject} does not match the pinned SHA-256"
    SUMS_LINE_FORM = "checksum list line {position} is malformed"
    SUMS_LINE_REPEAT = "checksum list line {position} repeats a path"
    TRACK_COLUMNS = "the track list does not have the columns caseid, tname, tid"
    TRACK_FIELDS = "track list line {position} does not have {count} fields"
    TRACK_CASE = "track list line {position}: the case number is not an integer"
    CLINICAL_COLUMNS = "the clinical table does not have each of the columns caseid, subjectid, casestart, caseend once"
    CLINICAL_FIELDS = "clinical table line {position} does not have {count} fields"
    CLINICAL_INTEGERS = "clinical table line {position}: a case, patient or time value is not an integer"
    CLINICAL_ORDER = "clinical table line {position}: the case ends before it starts"
    CLINICAL_TWICE = "clinical table line {position}: its case is listed twice"
    STRAY_TRACK = "the track list names a case that the clinical table does not list"
    CASE_UNLISTED = "case {position} of the clinical table is missing from the publisher's checksum list or key listing"
    TABLE_NOT_IN_LIST = "the pinned checksum of {subject} is not the one in the checksum list"
    NO_ARTERIAL_DEVELOPMENT = "the release has no development patient with one case and the arterial waveform, which three rules need"
    OVER_LIMIT = "the candidates total is over the limit of {count} bytes"
    PIN_NOT_OBJECT = "release pin: not a JSON object"
    PIN_TOP_FIELDS = "release pin: must have exactly the fields {fields}"
    SHAPE_MISSING = "{subject}{index}: missing"
    SHAPE = "{subject}{index}: must be an object with exactly the fields {fields}, each of its type"
    PIN_KEY_OUTSIDE = "{subject}{index}: the key is outside the release path vitaldb/1.0.0/"
    PIN_CHECKSUM_FORM = "{subject}{index}: checksum is not 64 hex characters in lower case"
    PIN_SIZE = "{subject}{index}: size is not a positive number"
    SOURCE_VALUE = "source: the {fields} is not the one that every read of this package uses"
    LIST_KEY_PIN = "checksum_list: the key must be vitaldb/1.0.0/SHA256SUMS.txt"
    TABLES_NOT_LIST = "tables: not a list"
    TABLES_NAMES = "tables: the pin must name clinical_data.csv and lab_data.csv of the release once each"
    TRACK_URL = "track_list: the address must be https://api.vitaldb.net/trks"
    PIN_ROWS = "{subject}{index}: rows is not a positive number or rows_sha256 is not 64 hex characters in lower case"
    CANDIDATE_HEADER = "the candidate file: the first line is not the header case, patient, key, size_bytes, sha256, arterial_waveform, slot"
    CANDIDATE_LINE = "the candidate file, line {position}: not a candidate line"
    CANDIDATE_KEY_OUTSIDE = "{subject} {position}: the key is outside the release path vitaldb/1.0.0/"
    CANDIDATE_KEY_OTHER_CASE = "{subject} {position}: the key names another case"
    CANDIDATE_CHECKSUM_FORM = "{subject} {position}: checksum is not 64 hex characters in lower case"
    CANDIDATE_SIZE = "{subject} {position}: the size is not positive"
    CANDIDATE_TWICE = "{subject} {position}: its case is listed twice"
    LISTING_CHECKSUM = "{subject} {position}: the checksum is not the one the publisher's checksum list gives for its key"
    LISTING_SIZE = "{subject} {position}: the size is not the one the publisher lists for its key"
    TRACKED_NOT_CHOSEN = "{subject} {position}: tracked but not chosen by the rules"
    CHOSEN_NOT_TRACKED = "{subject} {position}: chosen by the rules but not tracked"
    LINE_DIFFERS = "{subject} {position}: the tracked line differs from the rebuilt line"
    BUILD_NEEDS_OUTPUT = "build needs --output"
    WROTE = "wrote {count} candidates; the checksum list, the clinical table and the track list match their pins"
    CHECKED = "{count} tracked candidates equal the rebuild and match the pinned checksum list (seed {total})"
    CONNECTION_SELECTION = "the selection names a file that is not a payload of the pinned release; only the two tables and the case files may be copied"
    LISTED_OUTSIDE = "a listed key is outside the landing prefix, so nothing was read"
    PAIRING_LANDED_TWICE = "metadata record {position} pairs a landed file that an earlier record pairs with another source"
    CONNECTION_BUCKET = "the bucket is not a valid bucket name of the project, or it is the publisher's bucket"
    CONNECTION_REGION = "the region is not an AWS region name"
    METADATA_LINE = "line {position} of a metadata file is not a record of a file name, a byte count and a file path"
    NO_METADATA = "the landing prefix holds no metadata file"
    PAIRINGS_WRITTEN = "wrote {count} pairing records from the metadata of the copy"
    LIST_FETCHED = "the checksum list was read and matches the pinned size and SHA-256"
    LIST_PRESENT = "the checksum list is already present and matches the pinned size and SHA-256; nothing was read"
    CONNECTION_TAG = "the {fields} is not a full version of the form N.N.N"
    PROBE_CASE_NOT_CANDIDATE = "case {position} of the copy probe is not a candidate of the tracked file"
    PROBE_CASE_NOT_DEVELOPMENT = "case {position} of the copy probe is not in the development data"
    PROBE_KEYS = "the landed payloads are not exactly the copy probe's {total} selected keys: {count} are missing, repeated or not selected"
    PROBE_NOT_THIS = "the manifest is not the passed manifest of the copy probe's selection"
    PROBE_NULL_VERSION = "landed payload {position} has no object version ID, or a null one"
    PROBE_PIN_DIFFERS = "landed payload {position} differs from the tracked pin in its size or its SHA-256, or no tracked pin names it"
    PROBE_LANDING_EXTRA = "the landing prefix holds {count} current payloads that the manifest does not bind"
    PROBE_LANDING_MISSING = "{count} payloads that the manifest binds are not current objects of the landing prefix"
    PROBE_REFUSED = "the landed payloads of the copy probe are refused; no payload was read"
    PROBE_PASSED = "the {total} landed payloads of the copy probe are the selected keys, each at its pinned size and SHA-256 with a version ID that is not null"
    JOB_NOT_RECORD = "{subject} is not a job in the documented form: a text status, a whole number rowsSynced and a whole number bytesSynced"
    JOB_STATUS = "the copy job's status is not succeeded"
    JOB_ROWS = "the copy job synced {count} records, not the {total} files of the selection"
    JOB_BYTES = "the copy job reports no synced bytes"
    LOG_EMPTY = "the copy job's log is empty"
    LOG_ERROR_LINE = "line {position} of the copy job's log is an error line"
    JOB_PASSED = "the copy job succeeded, synced one record for each of the {total} files and reports synced bytes, and its log has no error line"
    SAMPLE_NOT_CANDIDATE = "landed case {position} is not a candidate of the tracked file"
    SAMPLE_NOT_DEVELOPMENT = "landed case {position} is not in the development data"
    SAMPLE_NO_REPEAT = "no patient of the landed cases has two of them"
    SAMPLE_NO_ARTERIAL = "no landed case has the arterial waveform"
    SAMPLE_NO_NON_ARTERIAL = "every landed case has the arterial waveform"
    SAMPLE_NO_SHORT = "the short development case is not among the landed cases"
    SAMPLE_WROTE = "wrote the Platform sample of {count} cases from the passed manifest of the copy probe"
    STAGING_NO_SAMPLES = "the staging samples hold no sample line"
    STAGING_CUT_OFF = "the staging samples have no header, no end line or an end line that counts other samples than the file holds: the file was cut off"
    STAGING_MISSING = "{count} sample lines have a failed reading, so the peak could not be seen in them"
    STAGING_FORM = "line {position} of the staging samples is not in the documented form"
    STAGING_PEAK_WROTE = "wrote the staging peak record from {count} samples"


NUMBER_SLOTS = ("position", "index", "count", "total")


def slots_of(code: Code) -> frozenset[str]:
    return frozenset(name for _, name, _, _ in string.Formatter().parse(code.value) if name)


@dataclass(frozen=True, slots=True)
class Notice:
    """One message as closed values. Raise ``TypeError`` for any value outside its kind, or a slot that its code's text does not have or lacks."""

    code: Code
    subject: Subject | None = None
    fields: tuple[str, ...] = ()
    optional: tuple[str, ...] = ()
    position: int | None = None
    index: int | None = None
    count: int | None = None
    total: int | None = None
    key: str | None = None
    cause: Cause | None = None
    digest: str | None = None

    def __post_init__(self) -> None:
        given = {name for name in ("subject", "position", "count", "total", "key", "cause", "digest") if getattr(self, name) is not None}
        given |= {name for name in ("fields", "optional") if getattr(self, name)}
        wanted = slots_of(self.code) if type(self.code) is Code else frozenset()
        if (
            type(self.code) is not Code
            or given != wanted - {"index"}
            or (self.index is not None and "index" not in wanted)
            or not (self.subject is None or type(self.subject) is Subject)
            or not (self.cause is None or type(self.cause) is Cause)
            or any(type(names) is not tuple or not set(names) <= FIELD_NAMES for names in (self.fields, self.optional))
            or any(not (getattr(self, name) is None or type(getattr(self, name)) is int) for name in NUMBER_SLOTS)
            or not (self.key is None or (type(self.key) is str and VERIFIED_KEY.fullmatch(self.key)))
            or not (self.digest is None or (type(self.digest) is str and SHA256.fullmatch(self.digest)))
        ):
            raise TypeError("a notice holds a known code and only the closed values its text names")


def render(notice: Notice) -> str:
    """The text of one notice: its code's fixed text with its closed values in their places."""
    return notice.code.value.format(
        subject=notice.subject.value if notice.subject else "",
        fields=", ".join(notice.fields),
        optional=", ".join(notice.optional),
        position=notice.position,
        index="" if notice.index is None else f"[{notice.index}]",
        count=notice.count,
        total=notice.total,
        key=notice.key,
        cause=notice.cause.value if notice.cause else "",
        digest=notice.digest,
    )


class ClosedError(Exception):
    """An error that holds notices only. Its text is their rendering, so it can name no value of an input."""

    def __init__(self, *notices: Notice) -> None:
        if not notices or any(type(notice) is not Notice for notice in notices):
            raise TypeError("a closed error holds one or more notices and nothing else")
        super().__init__(*notices)
        self.notices = notices

    def __str__(self) -> str:
        return "; ".join(render(notice) for notice in self.notices)
