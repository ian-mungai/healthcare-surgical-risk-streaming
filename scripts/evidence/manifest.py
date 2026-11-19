"""The export manifest of a run folder: every evidence file with its hash, and the readback check against it."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

EXCLUDED_TOP_LEVEL = ("export", "readback", "removal", "known_hosts")


def evidence_files(folder: Path) -> list[str]:
    """Every file of the run folder that is evidence, as sorted relative paths."""
    paths = [path.relative_to(folder).as_posix() for path in folder.rglob("*") if path.is_file() and not path.is_symlink()]
    return sorted(path for path in paths if path.split("/")[0] not in EXCLUDED_TOP_LEVEL)


def build_manifest(folder: Path, run_id: str, commit: str) -> dict[str, Any]:
    files = [
        {"path": path, "sha256": hashlib.sha256((folder / path).read_bytes()).hexdigest(), "bytes": (folder / path).stat().st_size}
        for path in evidence_files(folder)
    ]
    return {"run_id": run_id, "commit": commit, "files": files}


def manifest_sha256(manifest: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def readback_problems(manifest: dict[str, Any], folder: Path) -> list[str]:
    """Each file that is missing from the folder, differs from its manifest hash or is not in the manifest."""
    problems = []
    listed = {item["path"]: item["sha256"] for item in manifest["files"]}
    for path, expected in listed.items():
        target = folder / path
        if not target.is_file():
            problems.append(f"missing from the readback: {path}")
        elif hashlib.sha256(target.read_bytes()).hexdigest() != expected:
            problems.append(f"differs from its manifest hash: {path}")
    return problems + [f"not in the manifest: {path}" for path in evidence_files(folder) if path not in listed]
