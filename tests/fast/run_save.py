"""The one order in which a Silver end-to-end run saves its run folder, shared by the runners (``tests/e2e/silver_cases.py``, ``silver_signals.py``,
``silver_patient_split.py``, ``silver_hypotension.py`` and ``silver_reconcile.py``).

The order is fixed, as in the Gold dimensions runner: every attachment (the log and each file of ``attachments``, such as the environment file and the copy of
the script) is written first and read back whole, and compared with the text that was written, also by a runner that has no scan; the scan for a fixed-sample
number beside a set name runs on what the folder holds, and on the record and its feature text; the saving and the scan are two of the record's checks; the
retention label follows and is read back too; and ``record.json`` is written last, from the final verdict. A failed write or read of an attachment, or of the
label, therefore gives a failing record. The label and the record are then written from the final verdict by one guarded path, which the reporter also uses
when a print fails: no file of the folder says pass beside a record that says fail. When ``record.json`` itself cannot be written, there is no record: the
folder is removed, the reason goes to the error output and the exit code is 1. Each removal is checked: a folder or a file that cannot be removed is said to
stay, and the earlier label and record are removed before a rewrite, so that a stop in the middle leaves no earlier pass. The runners read the fixed-sample
candidate list once, through ``candidate_numbers``: a list that cannot be read is a failing observation, and the run does not start.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import shutil
import sys
import tempfile
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tests.e2e.run_record import FAILED_END, PASSED_END, ROOT, RUN_PAYLOAD_CLASS, Record, remove_verified
from tests.fast.sealed_record import sealed_observed

SET_WORD = re.compile(r"\b(training|validation|test)\b")
LABEL, RECORD = "retention.json", "record.json"


class ReadBackError(OSError):
    """A saved file that holds another text than the one that was written: it is cut short, or it was never written."""


def write_text(path: Path, text: str) -> None:
    path.write_text(text)


def leaks(texts: Iterable[str], numbers: set[str]) -> int:
    """The number of lines that hold both a fixed-sample case or patient number (alone, not inside a longer number or word) and a set name."""
    patterns = [re.compile(rf"(?<![0-9A-Za-z_.]){number}(?![0-9])") for number in sorted(numbers)]
    hits = 0
    for text in texts:
        for line in text.splitlines():
            if SET_WORD.search(line) and any(pattern.search(line) for pattern in patterns):
                hits += 1
    return hits


def candidate_numbers(record: Record, read: Callable[[], Iterable[Any]]) -> set[str] | None:
    """The fixed-sample case and patient numbers of the candidate list, read once for the whole run (``read`` gives its candidates). A list that cannot be read
    is a failing observation of the record, never an error that stops the runner before it saves: the result is then None, and the run is not started, because
    its saved texts cannot be scanned for these numbers. Only the class name of the error is recorded."""
    try:
        candidates = list(read())
    except (RuntimeError, ValueError, OSError) as error:
        record.expect(
            "the fixed-sample candidate list is read",
            "setup",
            "the tracked candidate list is read once, before the run, so that the saved texts are scanned for its numbers",
            sealed_observed(error=error),
            False,
        )
        return None
    return {str(item.case) for item in candidates} | {str(item.patient) for item in candidates}


def read_back(path: Path, text: str) -> str:
    """The text that ``path`` holds, which must be the text that was written."""
    found = path.read_text()
    if found != text:
        raise ReadBackError("a saved file differs from the text that was written")
    return found


def label_text(record: Record, outcome: str) -> str:
    label = {
        "class": RUN_PAYLOAD_CLASS,
        "purpose": record.feature,
        "created": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_condition": PASSED_END if outcome == "pass" else FAILED_END,
    }
    return json.dumps(label, indent=2) + "\n"


def stage(path: Path, text: str, put: Callable[[Path, str], None]) -> None:
    """Write ``text`` to a part file beside ``path`` and replace ``path`` with it whole. A failed or interrupted write removes the part file and raises."""
    staged = path.with_name(f"{path.name}.part")
    try:
        put(staged, text)
        os.replace(staged, path)
    except BaseException:
        with contextlib.suppress(OSError):
            staged.unlink()
        raise


def commit(record: Record, folder: Path, put: Callable[[Path, str], None], *, label_file: bool = True, record_file: bool = True) -> None:
    """The one path that writes the final files: the retention label of the record's outcome, then ``record.json``, each unless its flag is false. The reporter
    calls it with both when a print fails, so the label and the record say fail together. The earlier file is removed and checked first, so that a stop in the
    middle leaves no earlier pass. It raises ``OSError`` at the first file that cannot be written."""
    names = [name for name, wanted in ((LABEL, label_file), (RECORD, record_file)) if wanted]
    left = remove_verified(folder, tuple(name for wanted in names for name in (wanted, f"{wanted}.part")))
    if left:
        raise OSError(f"cannot remove {', '.join(left)}")
    if label_file:
        stage(folder / LABEL, label_text(record, record.outcome), put)
    if record_file:
        stage(folder / RECORD, json.dumps(asdict(record), indent=2) + "\n", put)


def remove_run(folder: Path, record: Record, reason: str) -> int:
    """Remove the run folder, check that it is gone and say what is true: that there is no record, or that the folder stays and may still say pass."""
    shutil.rmtree(folder, ignore_errors=True)
    if folder.exists():
        sys.stderr.write(f"{record.slug}: {reason}, and the run folder cannot be removed: its files may still say pass; outcome would be {record.outcome}\n")
    else:
        sys.stderr.write(f"{record.slug}: {reason}, so there is no record; outcome would be {record.outcome}\n")
    return 1


def save_run(
    record: Record,
    log: list[str],
    attachments: Mapping[str, Callable[[], str]],
    *,
    scan: Callable[[Iterable[str]], int] | None = None,
    runs: Path | None = None,
    write: Callable[[Path, str], None] | None = None,
    echo: bool = True,
) -> int:
    """Save the run in a new folder under ``runs`` (the project's ``runs`` folder when not given) and return 0 only when the record passes and is saved.

    ``attachments`` maps a file name to a function that gives its text, called inside the save so that a text that cannot be read fails the record too. Every
    file (the log and each attachment) is read back and must hold the text that was written. ``scan`` counts the lines of the texts that hold a fixed-sample
    number beside a set name; a runner without such a scan gives none. ``write`` writes one file (a test breaks it); it is read when the function runs, so that
    a test can replace ``write_text``."""
    put = write or write_text
    stamp = record.started_utc.replace(":", "").replace("-", "")
    mode = re.sub(r"[^a-z0-9]+", "_", record.mode.lower()).strip("_")
    try:
        folder_of_runs = runs if runs is not None else ROOT / "runs"
        folder_of_runs.mkdir(parents=True, exist_ok=True)
        folder = Path(tempfile.mkdtemp(prefix=f"{stamp}_{record.slug}_{mode}_", dir=folder_of_runs))
    except OSError as error:
        sys.stderr.write(f"{record.slug}: no run folder, so no record: {type(error).__name__}\n")
        return 1
    held, failure = 0, None
    try:
        written = {"log.txt": "\n".join(log) + "\n"}
        put(folder / "log.txt", written["log.txt"])
        for name, text_of in attachments.items():
            written[name] = text_of()
            put(folder / name, written[name])
        for name, text in written.items():
            read_back(folder / name, text)
        if scan is not None:
            held = scan([record.feature, json.dumps(asdict(record), indent=2), *(path.read_text() for path in sorted(folder.iterdir()) if path.is_file())])
    except (OSError, UnicodeError) as error:
        failure = error
    record.expect(
        "the run folder is saved and scanned",
        "control",
        "the log and every attachment (the environment file and the copy of this script) are written into the run folder and read back whole, and scanned "
        "when the runner has a scan, before the record is written",
        sealed_observed({"written and read back": failure is None}, error=failure),
        failure is None,
    )
    if failure is None and scan is not None:
        record.expect(
            "no identifier beside a set name",
            "control",
            "no text that is saved in the run folder (the log, the record, the attachments and the copy of this script) holds a fixed-sample case or patient "
            "number and a set name",
            sealed_observed({"no line holds one": held == 0}),
            held == 0,
        )

    def verdict() -> str:
        return "pass" if record.observations and all(item.passed for item in record.observations) else "fail"

    try:
        first = verdict()
        text = label_text(record, first)
        try:
            put(folder / LABEL, text)
            read = read_back(folder / LABEL, text)
            label_held = scan([read]) if scan is not None else 0
        except (OSError, UnicodeError) as error:
            label_ok = record.expect(
                "the retention label is written", "control", "the label is written and read back before the record", sealed_observed(error=error), False
            )
        else:
            label_ok = record.expect(
                "the retention label is written",
                "control",
                "the label is written, read back and holds no fixed-sample number beside a set name, before the record",
                sealed_observed({"no line holds one": label_held == 0}),
                label_held == 0,
            )
        record.outcome = verdict()
        if not label_ok or record.outcome != first:
            # The label on disk was made before its own check and the later observations: it is written again for the final verdict, and a label that cannot be
            # written again is removed, so that no label says pass beside a record that says fail.
            try:
                commit(record, folder, put, record_file=False)
            except OSError:
                with contextlib.suppress(OSError):
                    (folder / LABEL).unlink()
                if (folder / LABEL).exists():
                    return remove_run(folder, record, "the retention label cannot be made to say fail")
        try:
            commit(record, folder, put, label_file=False)
        except OSError as error:
            return remove_run(folder, record, f"record.json cannot be written ({type(error).__name__})")
    except KeyboardInterrupt:
        # An interrupt before the record is written leaves no label of a pass beside no record.
        remove_verified(folder, (LABEL, RECORD, f"{LABEL}.part", f"{RECORD}.part"))
        raise
    if echo and folder.is_relative_to(ROOT):
        record.report(folder, lambda again: commit(record, again, put))
    return 0 if record.outcome == "pass" else 1
