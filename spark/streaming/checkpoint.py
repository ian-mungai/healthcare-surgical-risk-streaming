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

import os
import re
from pathlib import Path

from spark.streaming.state import STATE_FORMAT

STATE_FORMAT_FILE = "state_format"
TEMPORARY_FILE = f".{STATE_FORMAT_FILE}.tmp"
REFUSAL_CODE = "checkpoint_state_format_refused"
EXIT_CODE = 3
URI_SCHEME = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*:")


class CheckpointFormatError(RuntimeError):
    """The checkpoint folder holds a state of another format, or one whose format is not known."""

    code = REFUSAL_CODE

    def __init__(self, message: str) -> None:
        super().__init__(f"{REFUSAL_CODE}: {message}")


def write_marker(folder: Path, expected: str) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    temporary = folder / TEMPORARY_FILE
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(expected + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, folder / STATE_FORMAT_FILE)


def check_checkpoint(checkpoint: str | Path, expected: str = STATE_FORMAT) -> None:
    """Mark a new checkpoint folder with ``expected`` and pass, pass a folder marked with ``expected``, and refuse any other folder."""
    text = str(checkpoint)
    if URI_SCHEME.match(text):
        raise CheckpointFormatError(f"the checkpoint {text} is not a local folder, so its state format cannot be read before the query starts")
    folder = Path(checkpoint)
    marker = folder / STATE_FORMAT_FILE
    if marker.is_symlink() or marker.exists():
        try:
            found = marker.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeDecodeError) as error:
            raise CheckpointFormatError(f"the state format marker of {text} cannot be read ({type(error).__name__})") from None
        if found != expected:
            raise CheckpointFormatError(
                f"the checkpoint {text} holds the state format {found or '(empty)'} and this job reads {expected}; use a new checkpoint folder"
            )
        return
    if folder.exists() and (not folder.is_dir() or any(path.name != TEMPORARY_FILE for path in folder.iterdir())):
        raise CheckpointFormatError(
            f"the checkpoint {text} holds a checkpoint with no state format marker, as an older job wrote it, and this job reads {expected}; "
            "use a new checkpoint folder"
        )
    write_marker(folder, expected)
