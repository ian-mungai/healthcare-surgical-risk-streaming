"""The one order in which a Silver end-to-end run saves its run folder, shared by the runners (``tests/e2e/silver_cases.py``, ``silver_signals.py``).

The order is fixed, as in the Gold dimensions runner: every attachment (the log and each file of ``attachments``, such as the environment file and the copy of
the script) is written first; the scan for a fixed-sample number beside a set name runs on what the folder holds, and on the record and its feature text; the
saving and the scan are two of the record's checks; the retention label follows; and ``record.json`` is written last, from the final verdict. A failed write
or read of an attachment, or of the label, therefore gives a failing record. When ``record.json`` itself cannot be written, there is no record: the folder is
removed, the reason goes to the error output and the exit code is 1.
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

from tests.e2e.run_record import FAILED_END, PASSED_END, ROOT, RUN_PAYLOAD_CLASS, Record
from tests.fast.sealed_record import sealed_observed


def write_text(path: Path, text: str) -> None:
    path.write_text(text)


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

    ``attachments`` maps a file name to a function that gives its text, called inside the save so that a text that cannot be read fails the record too.
    ``scan`` counts the lines of the texts that hold a fixed-sample number beside a set name; a runner without such a scan gives none. ``write`` writes one
    file (a test breaks it); it is read when the function runs, so that a test can replace ``write_text``."""
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
        put(folder / "log.txt", "\n".join(log) + "\n")
        for name, text in attachments.items():
            put(folder / name, text())
        if scan is not None:
            held = scan([record.feature, json.dumps(asdict(record), indent=2), *(path.read_text() for path in sorted(folder.iterdir()) if path.is_file())])
    except (OSError, UnicodeError) as error:
        failure = error
    record.expect(
        "the run folder is saved and scanned",
        "control",
        "the log and every attachment (the environment file and the copy of this script) are written into the run folder and read back for the scan, before "
        "the record is written",
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

    label = {
        "class": RUN_PAYLOAD_CLASS,
        "purpose": record.feature,
        "created": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_condition": PASSED_END if verdict() == "pass" else FAILED_END,
    }
    try:
        put(folder / "retention.json", json.dumps(label, indent=2) + "\n")
        label_held = scan([(folder / "retention.json").read_text()]) if scan is not None else 0
    except (OSError, UnicodeError) as error:
        record.expect("the retention label is written", "control", "the label is written and read back before the record", sealed_observed(error=error), False)
    else:
        record.expect(
            "the retention label is written",
            "control",
            "the label is written, read back and holds no fixed-sample number beside a set name, before the record",
            sealed_observed({"no line holds one": label_held == 0}),
            label_held == 0,
        )
    record.outcome = verdict()
    staged = folder / "record.json.part"
    try:
        put(staged, json.dumps(asdict(record), indent=2) + "\n")
        os.replace(staged, folder / "record.json")
    except OSError as error:
        with contextlib.suppress(OSError):
            staged.unlink()
        shutil.rmtree(folder, ignore_errors=True)
        sys.stderr.write(f"{record.slug}: record.json cannot be written, so there is no record ({type(error).__name__}); outcome would be {record.outcome}\n")
        return 1
    if echo and folder.is_relative_to(ROOT):
        record.report(folder)
    return 0 if record.outcome == "pass" else 1
