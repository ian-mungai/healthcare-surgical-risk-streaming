"""The state format of a checkpoint folder, checked before the query starts.

Spark keeps the keyed state of each case occurrence in the checkpoint as JSON of one format (``spark.streaming.state.STATE_FORMAT``), and the job reads the
state of a key only when an event of that key arrives. A checkpoint of another format would start, commit batches and fail at the first old key. So the job
writes a marker file with the format into a new checkpoint folder, and refuses a folder that holds a checkpoint with no marker (every checkpoint that the
earlier jobs wrote) or with another format. The refusal has a fixed code in its text and a fixed exit code. The job makes it before it calls Nessie, the
registry or Spark.

The folder is a local path (a volume). A path that starts with a URI scheme (``s3a://``, ``file:/``, ``file:///``) cannot be read here, and Spark would read
it as another place than the folder this check marks, so it is refused too, before any marker is checked or made.
"""

from __future__ import annotations

import re
from pathlib import Path

from scripts.ingestion.closed import ClosedInputError
from scripts.ingestion.folder import list_folder
from scripts.ingestion.notice import FixedTextError
from scripts.ingestion.output_file import write_new_file
from spark.streaming.state import STATE_FORMAT

STATE_FORMAT_FILE = "state_format"
PART_FILE = re.compile(r"\.[0-9a-f]{8}\.part")
REFUSAL_CODE = "checkpoint_state_format_refused"
EXIT_CODE = 3
URI_SCHEME = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*:")


class CheckpointFormatError(FixedTextError, RuntimeError):
    """The checkpoint folder holds a state of another format, or one whose format is not known. The text is fixed: it names no path and no value that was
    read from the folder, and the job shows it as it is (``describe_error``)."""

    code = REFUSAL_CODE

    def __init__(self, message: str) -> None:
        super().__init__(f"{REFUSAL_CODE}: {message}")


def write_marker(folder: Path, expected: str) -> None:
    """Write the marker as a new file of the checkpoint folder: whole or not at all. It is written only when the folder has none, so it never replaces one."""
    folder.mkdir(parents=True, exist_ok=True)
    try:
        write_new_file(folder / STATE_FORMAT_FILE, expected + "\n")
    except ClosedInputError:
        raise CheckpointFormatError("the state format marker cannot be written to the checkpoint folder") from None


def check_checkpoint(checkpoint: str | Path, expected: str = STATE_FORMAT) -> None:
    """Mark a new checkpoint folder with ``expected`` and pass, pass a folder marked with ``expected``, and refuse any other folder. A folder that cannot be
    listed, made or marked (a file-system error of any kind) is the same fixed refusal: the error text holds the path, so it is never shown."""
    try:
        check_folder(checkpoint, expected)
    except OSError:
        raise CheckpointFormatError("the checkpoint folder cannot be read or marked, so its state format cannot be checked before the query starts") from None


def check_folder(checkpoint: str | Path, expected: str) -> None:
    text = str(checkpoint)
    if URI_SCHEME.match(text):
        raise CheckpointFormatError("the checkpoint is not a local folder, so its state format cannot be read before the query starts")
    folder = Path(checkpoint)
    marker = folder / STATE_FORMAT_FILE
    if marker.is_symlink() or marker.exists():
        try:
            found = marker.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeDecodeError) as error:
            raise CheckpointFormatError(f"the state format marker of the checkpoint cannot be read ({type(error).__name__})") from None
        if found != expected:
            raise CheckpointFormatError(f"the checkpoint holds another state format and this job reads {expected}; use a new checkpoint folder")
        return
    if folder.exists() and (not folder.is_dir() or any(not PART_FILE.fullmatch(path.name) for path in list_folder(folder))):
        raise CheckpointFormatError(
            f"the checkpoint holds a checkpoint with no state format marker, as an older job wrote it, and this job reads {expected}; "
            "use a new checkpoint folder"
        )
    write_marker(folder, expected)
