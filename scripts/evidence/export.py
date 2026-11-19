"""Export a run's evidence as one archive with its manifest.

Run from the repository root: ``make evidence-export``. It lists every evidence file of ``runs/<RUN_ID>/`` with its
SHA-256 in ``runs/<RUN_ID>/export/manifest.json`` and packs exactly those files into ``export/evidence.tar.gz``. A run
started on a GitHub runner hands these two files to the owner, who verifies them with ``make evidence-verify``.
"""

from __future__ import annotations

import argparse
import json
import sys
import tarfile
from pathlib import Path

from scripts.evidence.manifest import build_manifest, manifest_sha256
from scripts.infra.state_bootstrap import DEFAULT_BINDING
from scripts.infra.terraform_wrapper import WrapperError, load_run
from scripts.project_config import ROOT


def main() -> int:
    """Write the manifest and the archive, print their size and return a non-zero code on a failure."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--binding", type=Path, default=DEFAULT_BINDING, help="the run binding file; the default is runs/run_binding.json")
    try:
        run = load_run(parser.parse_args().binding)
    except WrapperError as error:
        sys.stderr.write(f"{error}\n")
        return 1
    folder = ROOT / "runs" / run.run_id
    export = folder / "export"
    export.mkdir(parents=True, exist_ok=True)
    manifest = build_manifest(folder, run.run_id, run.commit)
    if not manifest["files"]:
        sys.stderr.write(f"run {run.run_id}: the run folder holds no evidence to export\n")
        return 1
    (export / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    with tarfile.open(export / "evidence.tar.gz", "w:gz") as archive:
        for item in manifest["files"]:
            archive.add(folder / item["path"], arcname=item["path"])
    sys.stdout.write(f"run {run.run_id}: {len(manifest['files'])} files exported to {export.relative_to(ROOT)}; manifest {manifest_sha256(manifest)[:12]}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
