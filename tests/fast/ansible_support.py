"""Run one Ansible task file of a role on this computer with given variables, through the real ``ansible-playbook``.

The role's checks are task files that read the variables a role registers from the host. These runs give them the
registered values a real host would give and show the check's verdict, with no host and no network.
"""

from __future__ import annotations

import base64
import json
import re
import tempfile
from pathlib import Path

from scripts.process import CompletedProcess, run_command
from scripts.project_config import ROOT

PLAYBOOK_SECONDS = 120
ROLES = ROOT / "ansible" / "roles"


def environment(folder: Path) -> dict[str, str]:
    return {
        "PATH": "/usr/bin:/bin",
        "ANSIBLE_HOME": str(folder / "home"),
        "ANSIBLE_LOCAL_TEMP": str(folder / "tmp"),
        "ANSIBLE_REMOTE_TMP": str(folder / "remote_tmp"),
        "ANSIBLE_PYTHON_INTERPRETER": str(ROOT / ".venv" / "bin" / "python"),
        "ANSIBLE_NOCOLOR": "1",
        "ANSIBLE_FORCE_COLOR": "0",
    }


def run_playbook(folder: Path, playbook: Path, extra: list[str], cwd: Path | None = None) -> CompletedProcess[str]:
    return run_command(
        str(ROOT / ".venv" / "bin" / "ansible-playbook"), [*extra, str(playbook)], cwd=cwd or folder, env=environment(folder), timeout=PLAYBOOK_SECONDS
    )


def run_task_files(role: str, task_files: list[str], variables: dict[str, object], *, verbose: bool = False) -> CompletedProcess[str]:
    """Run ``ansible/roles/<role>/tasks/<file>`` for each file in order on localhost and return the playbook's exit code and output."""
    with tempfile.TemporaryDirectory() as scratch:
        work = Path(scratch)
        playbook = work / "check.yml"
        includes = "".join(f"    - ansible.builtin.include_tasks: {ROLES / role / 'tasks' / name}\n" for name in task_files)
        playbook.write_text(f"- hosts: localhost\n  connection: local\n  gather_facts: false\n  tasks:\n{includes}")
        return run_playbook(work, playbook, [*(["-v"] if verbose else []), "-i", "localhost,", "--extra-vars", json.dumps(variables)])


def run_task_file(role: str, task_file: str, variables: dict[str, object], *, verbose: bool = False) -> CompletedProcess[str]:
    """Run ``ansible/roles/<role>/tasks/<task_file>`` on localhost and return the playbook's exit code and output."""
    return run_task_files(role, [task_file], variables, verbose=verbose)


def load_list(variables_file: Path, name: str) -> list[str]:
    """The named list variable of a variables file, as Ansible loads it."""
    with tempfile.TemporaryDirectory() as scratch:
        folder = Path(scratch)
        playbook = folder / "load.yml"
        playbook.write_text(
            f"- hosts: localhost\n  connection: local\n  gather_facts: false\n  vars_files: [{variables_file}]\n  tasks:\n"
            f'    - ansible.builtin.copy:\n        content: "{{{{ {name} | to_json | b64encode }}}}"\n        dest: {folder / "value.json"}\n'
        )
        result = run_playbook(folder, playbook, ["-i", "localhost,"])
        if result.returncode != 0:
            raise RuntimeError(result.stdout + result.stderr)
        values: list[str] = json.loads(base64.b64decode((folder / "value.json").read_text()))
        return values


def role_order(playbook: str) -> list[str]:
    """The roles of ``ansible/<playbook>`` in the order that Ansible would run them."""
    with tempfile.TemporaryDirectory() as scratch:
        result = run_playbook(Path(scratch), ROOT / "ansible" / playbook, ["--list-tasks"], cwd=ROOT / "ansible")
    if result.returncode != 0:
        raise RuntimeError(result.stdout + result.stderr)
    order: list[str] = []
    for role in re.findall(r"^\s+(\w+) : ", result.stdout, flags=re.MULTILINE):
        if role not in order:
            order.append(role)
    return order
