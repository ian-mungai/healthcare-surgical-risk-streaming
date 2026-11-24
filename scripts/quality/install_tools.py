"""Install the pinned command-line tools into ``.tools``: gitleaks, Terraform, tflint, the tflint AWS ruleset and Trivy, and the working-tree privacy hook.

Run from the repository root: ``.venv/bin/python -m scripts.quality.install_tools``. ``--check`` verifies each installed
binary against its pinned archive without downloading or writing anything. Each archive is checked against its pinned
SHA-256 before anything is extracted, and the checked archive is kept in ``.tools/downloads``. A rerun with the kept
archives downloads nothing and rewrites a binary only when its bytes differ. The checksums come from each release's
published checksum file.
"""

from __future__ import annotations

import hashlib
import io
import platform
import sys
import tarfile
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from scripts.ingestion.closed import QuietParser
from scripts.ingestion.notice import describe_error
from scripts.ingestion.output_file import replace_owned_file
from scripts.process import run_command
from scripts.quality import worktree_scan_hook

ROOT = Path(__file__).resolve().parents[2]
TOOLS = ROOT / ".tools"
DOWNLOADS = TOOLS / "downloads"
DOWNLOAD_TIMEOUT_SECONDS = 300
BINARY_MODE = 0o700
Platform = tuple[str, str]


@dataclass(frozen=True)
class Tool:
    name: str
    version: str
    url: str
    member: str
    target: str
    assets: dict[Platform, tuple[str, str]]

    @property
    def folder(self) -> Path:
        """The folder of the installed file: the tools folder constant plus the folder part of the pinned target."""
        return TOOLS / PurePosixPath(self.target).parent

    @property
    def path(self) -> Path:
        return self.folder / PurePosixPath(self.target).name


GITLEAKS = Tool(
    name="gitleaks",
    version="8.30.1",
    url="https://github.com/gitleaks/gitleaks/releases/download/v8.30.1",
    member="gitleaks",
    target="bin/gitleaks",
    assets={
        ("Darwin", "arm64"): ("gitleaks_8.30.1_darwin_arm64.tar.gz", "b40ab0ae55c505963e365f271a8d3846efbc170aa17f2607f13df610a9aeb6a5"),
        ("Linux", "x86_64"): ("gitleaks_8.30.1_linux_x64.tar.gz", "551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb"),
    },
)
TERRAFORM = Tool(
    name="terraform",
    version="1.16.4",
    url="https://releases.hashicorp.com/terraform/1.16.4",
    member="terraform",
    target="bin/terraform",
    assets={
        ("Darwin", "arm64"): ("terraform_1.16.4_darwin_arm64.zip", "42cfdf97ad722f79085fe2279b06d4b8680172de3534b22eeddd9a0fbbe7b8f1"),
        ("Linux", "x86_64"): ("terraform_1.16.4_linux_amd64.zip", "dc94af0eef1147718ad7c8daea792ed199e3e0492eec180d0adafa2a65a879df"),
    },
)
TFLINT = Tool(
    name="tflint",
    version="0.66.0",
    url="https://github.com/terraform-linters/tflint/releases/download/v0.66.0",
    member="tflint",
    target="bin/tflint",
    assets={
        ("Darwin", "arm64"): ("tflint_darwin_arm64.zip", "859b26af07ead7e65cee7eec9d050b2e2571dc28395b7a39191cae088cd67803"),
        ("Linux", "x86_64"): ("tflint_linux_amd64.zip", "22be11cadc25cd02401e94c0ccc91aa63467debb8485c2a755d98f4c0a9d1e2e"),
    },
)
TFLINT_AWS = Tool(
    name="tflint-ruleset-aws",
    version="0.50.0",
    url="https://github.com/terraform-linters/tflint-ruleset-aws/releases/download/v0.50.0",
    member="tflint-ruleset-aws",
    target="tflint_plugins/github.com/terraform-linters/tflint-ruleset-aws/0.50.0/tflint-ruleset-aws",
    assets={
        ("Darwin", "arm64"): ("tflint-ruleset-aws_darwin_arm64.zip", "0117f75027cbc1bc452db1defd59d40b4a579ef3e7bb3a8b3079eaf6d372a9a6"),
        ("Linux", "x86_64"): ("tflint-ruleset-aws_linux_amd64.zip", "cdd433fdbc0929e7b9f9709dd9e72237fdfe1fc235674445c284b1e4b94a5c26"),
    },
)
TRIVY = Tool(
    name="trivy",
    version="0.76.2",
    url="https://github.com/aquasecurity/trivy/releases/download/v0.76.2",
    member="trivy",
    target="bin/trivy",
    assets={
        ("Darwin", "arm64"): ("trivy_0.76.2_macOS-ARM64.tar.gz", "ae7ea358326960406b703c77b2ac490190797d80b306b5b50c2a27221da0a5b8"),
        ("Linux", "x86_64"): ("trivy_0.76.2_Linux-64bit.tar.gz", "bc0fc726e017b8927a62b2d3531d7a84ee01769daf5c99ed866dced54089fdbf"),
    },
)
TOOL_LIST = (GITLEAKS, TERRAFORM, TFLINT, TFLINT_AWS, TRIVY)


def pinned_asset(tool: Tool) -> tuple[str, str]:
    key = (platform.system(), platform.machine())
    if key not in tool.assets:
        raise SystemExit(f"no pinned {tool.name} build for {key[0]} {key[1]}; add its checksum to scripts/quality/install_tools.py")
    return tool.assets[key]


def verified_archive(tool: Tool, *, download: bool) -> bytes | None:
    asset, expected = pinned_asset(tool)
    kept = DOWNLOADS / f"{tool.name}_{tool.version}_{asset}"
    if kept.is_file() and hashlib.sha256(kept.read_bytes()).hexdigest() == expected:
        return kept.read_bytes()
    if not download:
        return None
    DOWNLOADS.mkdir(parents=True, exist_ok=True)
    partial = kept.with_name(kept.name + ".partial")
    arguments = ["--fail", "--silent", "--show-error", "--location", "--proto", "=https", "--max-time", str(DOWNLOAD_TIMEOUT_SECONDS), "--output", str(partial)]
    run_command("curl", [*arguments, f"{tool.url}/{asset}"], timeout=DOWNLOAD_TIMEOUT_SECONDS + 10, check=True)
    data = partial.read_bytes()
    if hashlib.sha256(data).hexdigest() != expected:
        partial.unlink()
        raise SystemExit(f"{tool.name} {tool.version}: the download does not match its pinned SHA-256; nothing was installed")
    partial.replace(kept)
    return data


def pinned_binary(tool: Tool, archive: bytes) -> bytes:
    if pinned_asset(tool)[0].endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
            return bundle.read(tool.member)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tarball:
        member = tarball.extractfile(tool.member)
        if member is None:
            raise SystemExit(f"the {tool.name} archive holds no {tool.member} file")
        return member.read()


def settle(tool: Tool, *, check: bool) -> bool:
    label = f"{tool.name} {tool.version}"
    archive = verified_archive(tool, download=not check)
    if archive is None:
        sys.stderr.write(f"{label}: no verified archive in .tools/downloads; run the installer without --check\n")
        return False
    binary = pinned_binary(tool, archive)
    target = tool.path
    if target.is_file() and target.read_bytes() == binary and (target.stat().st_mode & 0o777) == BINARY_MODE:
        sys.stdout.write(f"{label}: installed and verified\n")
        return True
    if check:
        sys.stderr.write(f"{label}: the installed binary is missing or differs from the pinned archive\n")
        return False
    tool.folder.mkdir(parents=True, exist_ok=True)
    replace_owned_file(target, binary, state_root=tool.folder, mode=BINARY_MODE)
    sys.stdout.write(f"{label}: installed from the verified archive\n")
    return True


def settle_hook(*, check: bool) -> bool:
    if worktree_scan_hook.installed(ROOT):
        sys.stdout.write("working-tree privacy hook: installed and verified\n")
        return True
    if check:
        sys.stderr.write("working-tree privacy hook: missing or changed; run the installer without --check\n")
        return False
    try:
        worktree_scan_hook.install(ROOT)
    except worktree_scan_hook.KeptHookError as error:
        sys.stderr.write(f"working-tree privacy hook: {describe_error(error)}\n")
        return False
    sys.stdout.write("working-tree privacy hook: installed\n")
    return True


def main() -> int:
    """Install or check every pinned tool and return 1 when one is missing or differs."""
    parser = QuietParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--check", action="store_true", help="verify the installed binaries without downloading or writing")
    check = parser.parse_args().check
    results = [settle(tool, check=check) for tool in TOOL_LIST]
    results.append(settle_hook(check=check))
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
