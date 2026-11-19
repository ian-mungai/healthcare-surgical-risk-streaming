"""The export manifest of a run folder: every evidence file with its hash, and the readback check against it."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

EXCLUDED_TOP_LEVEL = ("export", "readback", "removal", "known_hosts")
REQUIRED_EVIDENCE = ("inventory.json", "plans/destroy_binding.json", "teardown/approved_inventory.json")


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


def manifest_problems(manifest: dict[str, Any]) -> list[str]:
    """Each path that is not a plain relative path or is listed twice, and each required file that is missing."""
    problems = []
    seen: set[str] = set()
    for item in manifest["files"]:
        path = str(item["path"])
        parts = path.split("/")
        if not path or path.startswith("/") or any(part in ("", ".", "..") for part in parts) or "\\" in path:
            problems.append(f"manifest path is not a plain relative path: {path!r}")
        elif path in seen:
            problems.append(f"manifest lists a path twice: {path}")
        seen.add(path)
    return problems + [f"required evidence is not in the manifest: {path}" for path in REQUIRED_EVIDENCE if path not in seen]


def archive_problems(manifest: dict[str, Any], members: list[str]) -> list[str]:
    """Each archive member the manifest does not list and each listed file the archive does not hold."""
    listed = [str(item["path"]) for item in manifest["files"]]
    extra = [f"in the archive and not in the manifest: {name}" for name in sorted(set(members) - set(listed))]
    return extra + [f"in the manifest and not in the archive: {path}" for path in listed if path not in members]
