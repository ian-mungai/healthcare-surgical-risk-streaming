"""Paths of the Ansible roles and the shared module utilities, and the group variables, read without running Ansible.

The quick group imports these; the runners that start ``ansible-playbook`` are in ``tests/slow/ansible_support.py``.
"""

from __future__ import annotations

import importlib
from types import ModuleType
from typing import Any

from scripts.project_config import ROOT
from scripts.stack.yaml_text import load_yaml

ROLES = ROOT / "ansible" / "roles"
SHARED_MODULE_UTILS = ROLES / "secret_files" / "module_utils"


def shared_module(name: str) -> ModuleType:
    """A module of the shared role under ``ansible.module_utils``, found the way a module of a role finds it when Ansible runs it."""
    package = importlib.import_module("ansible.module_utils")
    if str(SHARED_MODULE_UTILS) not in package.__path__:
        package.__path__.append(str(SHARED_MODULE_UTILS))
    return importlib.import_module(f"ansible.module_utils.{name}")


GROUP_VARIABLES = ROOT / "ansible" / "group_vars" / "all.yml"


def group_variables() -> dict[str, Any]:
    """The variables of ``ansible/group_vars/all.yml`` as a mapping."""
    document = load_yaml(GROUP_VARIABLES.read_text())
    if not isinstance(document, dict):
        raise TypeError("expected a mapping")
    return document
