"""Run one Ansible task file of a role on this computer with given variables, through the real ``ansible-playbook``.

The role's checks are task files that read the variables a role registers from the host. These runs give them the
registered values a real host would give and show the check's verdict, with no host and no network.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from scripts.process import CompletedProcess, run_command
from scripts.project_config import ROOT

PLAYBOOK_SECONDS = 120
ROLES = ROOT / "ansible" / "roles"


def run_task_file(role: str, task_file: str, variables: dict[str, object]) -> CompletedProcess[str]:
    """Run ``ansible/roles/<role>/tasks/<task_file>`` on localhost and return the playbook's exit code and output."""
    with tempfile.TemporaryDirectory() as scratch:
        folder = Path(scratch)
        playbook = folder / "check.yml"
        playbook.write_text(
            "- hosts: localhost\n  connection: local\n  gather_facts: false\n  tasks:\n"
            f"    - ansible.builtin.include_tasks: {ROLES / role / 'tasks' / task_file}\n"
        )
        environment = {
            "PATH": "/usr/bin:/bin",
            "ANSIBLE_HOME": str(folder / "home"),
            "ANSIBLE_LOCAL_TEMP": str(folder / "tmp"),
            "ANSIBLE_PYTHON_INTERPRETER": str(ROOT / ".venv" / "bin" / "python"),
            "ANSIBLE_NOCOLOR": "1",
            "ANSIBLE_FORCE_COLOR": "0",
        }
        return run_command(
            str(ROOT / ".venv" / "bin" / "ansible-playbook"),
            ["-i", "localhost,", str(playbook), "--extra-vars", json.dumps(variables)],
            cwd=folder,
            env=environment,
            timeout=PLAYBOOK_SECONDS,
        )
