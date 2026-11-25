"""The pinned Terraform on a copy of ``infra/`` with no cloud access, shared by the slow tests that run Terraform."""

from __future__ import annotations

import shutil
from pathlib import Path

from scripts.infra.terraform_wrapper import pinned_terraform
from scripts.process import CompletedProcess, run_command
from scripts.project_config import ROOT

INIT_SECONDS = 300
PROVIDERS = ROOT / ".tools" / "terraform" / "static" / "providers"


class Terraform:
    """The pinned Terraform on a copy of ``infra/`` with no cloud access: an unreadable profile, empty credential files and a data folder of its own."""

    def __init__(self, scratch: Path) -> None:
        self.scratch = scratch
        self.config = scratch / "infra"
        shutil.copytree(ROOT / "infra", self.config, ignore=shutil.ignore_patterns(".terraform", "probe_tests"))
        (scratch / "empty").write_text("")
        self.env = {
            "PATH": "/usr/bin:/bin",
            "HOME": str(scratch),
            "TF_DATA_DIR": str(scratch / "data"),
            "TF_IN_AUTOMATION": "1",
            "AWS_PROFILE": "no_such_profile",
            "AWS_CONFIG_FILE": str(scratch / "empty"),
            "AWS_SHARED_CREDENTIALS_FILE": str(scratch / "empty"),
        }

    def run(self, *arguments: str, seconds: int) -> CompletedProcess[str]:
        return run_command(str(pinned_terraform()), [f"-chdir={self.config}", *arguments], cwd=self.scratch, env=self.env, timeout=seconds)

    def init(self) -> None:
        plugin = [f"-plugin-dir={PROVIDERS}"] if PROVIDERS.is_dir() else []
        result = self.run("init", "-backend=false", "-input=false", "-no-color", *plugin, seconds=INIT_SECONDS)
        if result.returncode != 0:
            raise AssertionError("terraform init failed:\n" + (result.stdout + result.stderr)[-2000:])
