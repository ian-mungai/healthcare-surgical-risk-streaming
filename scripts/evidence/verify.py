"""Verify an evidence export on the owner's computer and sign its receipt.

Run from the repository root on the owner's computer: ``make evidence-verify``. It unpacks the archive into a new
readback folder, checks the complete file list and every hash against the manifest and, only when all agree, writes
``export/receipt.json`` and signs it with the owner's verifier key. The receipt binds the run ID, the commit and the
manifest's hash. The key's path comes from ``EVIDENCE_VERIFIER_KEY`` in ``.env`` and the key never leaves the owner's computer.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tarfile
from datetime import UTC, datetime
from pathlib import Path

from scripts.evidence.manifest import manifest_sha256, readback_problems
from scripts.evidence.receipt import NAMESPACE, SIGNATURE_TIMEOUT_SECONDS
from scripts.infra.state_bootstrap import DEFAULT_BINDING
from scripts.infra.terraform_wrapper import WrapperError, load_run
from scripts.process import run_command
from scripts.project_config import ROOT, ConfigurationError, load_configuration


def main() -> int:
    """Check the readback, sign the receipt and return a non-zero code when the readback is incomplete or the signing fails."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--binding", type=Path, default=DEFAULT_BINDING, help="the run binding file; the default is runs/run_binding.json")
    try:
        run = load_run(parser.parse_args().binding)
        key = load_configuration().get("EVIDENCE_VERIFIER_KEY", "")
    except (WrapperError, ConfigurationError) as error:
        sys.stderr.write(f"{error}\n")
        return 1
    folder = ROOT / "runs" / run.run_id
    export = folder / "export"
    if not (export / "manifest.json").is_file() or not (export / "evidence.tar.gz").is_file():
        sys.stderr.write("no export to verify: run make evidence-export first\n")
        return 1
    if not key or not Path(key).expanduser().is_file():
        sys.stderr.write(".env: EVIDENCE_VERIFIER_KEY does not name the owner's verifier key file\n")
        return 1
    manifest = json.loads((export / "manifest.json").read_text())
    readback = folder / "readback"
    shutil.rmtree(readback, ignore_errors=True)
    readback.mkdir()
    with tarfile.open(export / "evidence.tar.gz", "r:gz") as archive:
        archive.extractall(readback, filter="data")
    problems = readback_problems(manifest, readback)
    if manifest.get("run_id") != run.run_id or manifest.get("commit") != run.commit:
        problems.append("the manifest belongs to another run or commit")
    if problems:
        sys.stderr.write("".join(f"{problem}\n" for problem in problems) + "no receipt was written\n")
        return 1
    receipt = export / "receipt.json"
    manifest_hash = manifest_sha256(manifest)
    record = {"run_id": run.run_id, "commit": run.commit, "manifest_sha256": manifest_hash, "files": len(manifest["files"])}
    receipt.write_text(json.dumps(record | {"verified_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")}, indent=2) + "\n")
    receipt.with_name("receipt.json.sig").unlink(missing_ok=True)
    signed = run_command("ssh-keygen", ["-Y", "sign", "-f", str(Path(key).expanduser()), "-n", NAMESPACE, str(receipt)], timeout=SIGNATURE_TIMEOUT_SECONDS)
    if signed.returncode != 0:
        receipt.unlink()
        sys.stderr.write(f"the receipt could not be signed: {signed.stderr.strip()}\n")
        return 1
    sys.stdout.write(f"run {run.run_id}: {len(manifest['files'])} files verified against manifest {manifest_hash[:12]}; signed receipt written\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
