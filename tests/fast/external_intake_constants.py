"""The names that the tests of the external secrets intake share: the script, the one declared secret and a synthetic marker in the form of its check."""

from __future__ import annotations

from scripts.project_config import ROOT

SCRIPT = ROOT / "ansible" / "roles" / "openbao" / "files" / "external_secret_intake.py"
NAME = "dockerhub_read"
VALUE = "dckr_pat_" + "MarkerValueThatMustNotLeak01"
