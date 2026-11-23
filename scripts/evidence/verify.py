"""Verify an evidence export on the owner's computer and sign its receipt.

Run from the repository root on the owner's computer: ``make evidence-verify``. It unpacks the archive into a new
readback folder, checks the complete file list and every hash against the manifest and, only when all agree, writes
``export/receipt.json`` and signs it with the owner's verifier key. The receipt binds the run ID, the commit and the
manifest's hash. The key's path comes from ``EVIDENCE_VERIFIER_KEY`` in ``.env`` and the key never leaves the owner's computer.
"""

from __future__ import annotations

import json
import shutil
import sys
import tarfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from scripts.evidence.manifest import archive_problems, manifest_problems, manifest_sha256, readback_problems
from scripts.evidence.receipt import NAMESPACE, SIGNATURE_TIMEOUT_SECONDS
from scripts.infra.state_bootstrap import DEFAULT_BINDING, RunBinding
from scripts.infra.terraform_wrapper import WrapperError, load_run
from scripts.ingestion.closed import QuietParser
from scripts.ingestion.notice import describe_error
from scripts.process import run_command
from scripts.project_config import ROOT, ConfigurationError, load_configuration


def invalidate_receipt(folder: Path) -> None:
    """Remove any earlier receipt and its signature. Every verification starts here, so none that stops early leaves a receipt that passes."""
    receipt = folder / "export" / "receipt.json"
    receipt.unlink(missing_ok=True)
    receipt.with_name("receipt.json.sig").unlink(missing_ok=True)


def export_problem(folder: Path) -> str | None:
    """Remove the earlier receipt, then say why there is nothing to verify, or None when the export is there."""
    invalidate_receipt(folder)
    if not (folder / "export" / "manifest.json").is_file() or not (folder / "export" / "evidence.tar.gz").is_file():
        return "no export to verify: run make evidence-export first"
    return None


def verify_export(folder: Path, run: RunBinding, sign: Callable[[Path], bool]) -> list[str]:
    """Check the export in ``folder`` and write its signed receipt, or return each reason it was not written.

    Any earlier receipt is removed first, so a failed verification never leaves a receipt that passes.
    """
    export = folder / "export"
    receipt = export / "receipt.json"
    invalidate_receipt(folder)
    manifest = json.loads((export / "manifest.json").read_text())
    problems = manifest_problems(manifest)
    if manifest.get("run_id") != run.run_id or manifest.get("commit") != run.commit:
        problems.append("the manifest belongs to another run or commit")
    if problems:
        return problems
    partial = folder / "readback.partial"
    shutil.rmtree(partial, ignore_errors=True)
    try:
        with tarfile.open(export / "evidence.tar.gz", "r:gz") as archive:
            members = archive.getmembers()
            problems = [f"archive member is not a regular file: {member.name}" for member in members if not member.isfile()]
            problems += archive_problems(manifest, [member.name for member in members])
            if problems:
                return problems
            partial.mkdir()
            archive.extractall(partial, filter="data")
    except tarfile.TarError, OSError:
        shutil.rmtree(partial, ignore_errors=True)
        return ["the archive cannot be read"]
    problems = readback_problems(manifest, partial)
    if problems:
        shutil.rmtree(partial)
        return problems
    readback = folder / "readback"
    shutil.rmtree(readback, ignore_errors=True)
    partial.replace(readback)
    record = {"run_id": run.run_id, "commit": run.commit, "manifest_sha256": manifest_sha256(manifest), "files": len(manifest["files"])}
    receipt.write_text(json.dumps(record | {"verified_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")}, indent=2) + "\n")
    if not sign(receipt):
        receipt.unlink()
        receipt.with_name("receipt.json.sig").unlink(missing_ok=True)
        return ["the receipt could not be signed"]
    return []


def main() -> int:
    """Check the readback, sign the receipt and return a non-zero code when the readback is incomplete or the signing fails."""
    parser = QuietParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--binding", type=Path, default=DEFAULT_BINDING, help="the run binding file; the default is runs/run_binding.json")
    try:
        run = load_run(parser.parse_args().binding)
    except WrapperError as error:
        sys.stderr.write(f"{describe_error(error)}\n")
        return 1
    folder = ROOT / "runs" / run.run_id
    invalidate_receipt(folder)
    try:
        key = load_configuration().get("EVIDENCE_VERIFIER_KEY", "")
    except ConfigurationError as error:
        sys.stderr.write(f"{describe_error(error)}\n")
        return 1
    missing = export_problem(folder)
    if missing:
        sys.stderr.write(f"{missing}\n")
        return 1
    if not key or not Path(key).expanduser().is_file():
        sys.stderr.write(".env: EVIDENCE_VERIFIER_KEY does not name the owner's verifier key file\n")
        return 1

    def sign(receipt: Path) -> bool:
        arguments = ["-Y", "sign", "-f", str(Path(key).expanduser()), "-n", NAMESPACE, str(receipt)]
        return run_command("ssh-keygen", arguments, timeout=SIGNATURE_TIMEOUT_SECONDS).returncode == 0

    problems = verify_export(folder, run, sign)
    if problems:
        sys.stderr.write("".join(f"{problem}\n" for problem in problems) + "no receipt was written\n")
        return 1
    sys.stdout.write(f"run {run.run_id}: the export is complete and verified; signed receipt written\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
