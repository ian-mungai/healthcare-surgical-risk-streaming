"""Ansible module: the check of a runner checkout's environment against the lock files that it was built from.

``state: check`` runs the folder's own ``.venv/bin/python`` and reports ``intact: true`` only when all of these hold: the interpreter runs and says the pinned
version; and the packages that the environment holds (but ``pip``, which the interpreter's own ``venv`` brings) are exactly the pins of the lock files that
apply to this interpreter, with their versions. A pin with an environment marker (``; platform_python_implementation != 'PyPy'``) applies when the marker is
true for the folder's own interpreter, as pip decides it: the markers are evaluated by that interpreter (one program, with the listing of its packages, through
the ``packaging`` that the environment holds or, at the build, the one that its own pip brings), so that ``cffi`` is required on CPython and a pin of another
platform is not. It never fails on a bad environment: it reports ``intact: false`` and fixed problem lines (at most twenty and a count of the rest) with a
distribution's name and never a file's content, so that the role can build the folder again. Each program that it starts runs in a session of its own and is
killed, with every process of the session, when its deadline ends; the check then fails, and the program is gone when it does. It uses only the standard library
and runs on the host's Python.
"""

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
from pathlib import Path
from typing import Any

from ansible.module_utils.basic import AnsibleModule

ENVIRONMENT = ".venv"
IGNORED = frozenset({"pip"})
SHOWN_PROBLEMS = 20
LOCK_ENTRY = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)(?:\[[^\]]*\])?==([^\s;\\]+)[ \t]*(;[^\\\n]*)?", re.MULTILINE)
# The one program that the folder's interpreter runs besides --version: its installed packages, and each marker of the lock files evaluated for it.
REPORT = "\n".join(
    (
        "import importlib.metadata as m, json, sys",
        "try:",
        "    from packaging.markers import Marker",
        "except ImportError:",
        "    from pip._vendor.packaging.markers import Marker",
        "installed = {d.metadata['Name']: d.version for d in m.distributions()}",
        "print(json.dumps({'installed': installed, 'applies': [Marker(text).evaluate() for text in json.loads(sys.argv[1])]}))",
    )
)
REQUIREMENT_FORM = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]*(/[A-Za-z0-9_][A-Za-z0-9_.-]*)*")


class EnvironmentCheckError(RuntimeError):
    """The check itself could not finish: a program passed its deadline, or a lock file cannot be read."""


def normalized(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def run_bounded(argv: list[str], seconds: int) -> tuple[int, str]:
    """Run a program in a session of its own with a deadline: after the deadline the whole session is killed and waited for, and the check fails."""
    process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, start_new_session=True, text=True)  # noqa: S603
    try:
        output, _ = process.communicate(timeout=seconds)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.communicate()
        raise EnvironmentCheckError(f"a program of the check did not finish within {seconds} seconds and was stopped") from None
    return process.returncode, output


def locked(folder: Path, requirements: list[str]) -> list[tuple[str, str, str]]:
    """Each pin of the lock files as (name, version, marker); the marker is the empty text for a pin that has none."""
    found: list[tuple[str, str, str]] = []
    for name in requirements:
        if REQUIREMENT_FORM.fullmatch(name) is None:
            raise EnvironmentCheckError("a lock file is not a plain relative path")
        text = (folder / name).read_text()
        found += [(normalized(match.group(1)), match.group(2), (match.group(3) or "").lstrip(";").strip()) for match in LOCK_ENTRY.finditer(text)]
    if not found:
        raise EnvironmentCheckError("the lock files pin no requirement")
    return found


def applicable(pins: list[tuple[str, str, str]], applies: dict[str, bool]) -> dict[str, set[str]]:
    """The versions of each name that pip installs on this interpreter: the pins with no marker and those whose marker is true for it."""
    chosen: dict[str, set[str]] = {}
    for name, version, marker in pins:
        if not marker or applies[marker]:
            chosen.setdefault(name, set()).add(version)
    return chosen


def problems(folder: Path, version: str, requirements: list[str], seconds: int) -> list[str]:
    pins = locked(folder, requirements)
    python = folder / ENVIRONMENT / "bin" / "python"
    if not os.access(python, os.X_OK):
        return ["the environment's interpreter is not there"]
    code, said = run_bounded([str(python), "--version"], seconds)
    if code != 0 or said.strip() != f"Python {version}":
        return ["the environment's interpreter does not run or is not the pinned version"]
    markers = sorted({marker for _, _, marker in pins if marker})
    code, listing = run_bounded([str(python), "-I", "-c", REPORT, json.dumps(markers)], seconds)
    try:
        document: Any = json.loads(listing) if code == 0 else None
    except ValueError:
        document = None
    if (
        not isinstance(document, dict)
        or not isinstance(document.get("installed"), dict)
        or document.get("applies") is None
        or len(document["applies"]) != len(markers)
    ):
        return ["the environment's installed packages or the lock files' markers cannot be read"]
    expected = applicable(pins, dict(zip(markers, document["applies"], strict=True)))
    installed = {normalized(name): found for name, found in document["installed"].items() if normalized(name) not in IGNORED}
    found: list[str] = []
    for name, version_found in sorted(installed.items()):
        if name not in expected:
            found.append(f"installed but not in the lock files: {name}")
        elif version_found not in expected[name]:
            found.append(f"another version than the lock files: {name}")
    found += [f"in the lock files but not installed: {name}" for name in sorted(expected) if name not in installed]
    if len(found) > SHOWN_PROBLEMS:
        return [*found[:SHOWN_PROBLEMS], f"and {len(found) - SHOWN_PROBLEMS} more"]
    return found


def main() -> None:
    module = AnsibleModule(
        argument_spec={
            "folder": {"type": "path", "required": True},
            "version": {"type": "str", "required": True},
            "requirements": {"type": "list", "elements": "str", "required": True},
            "deadline": {"type": "int", "default": 60},
            "state": {"type": "str", "required": True, "choices": ["check"]},
        },
        supports_check_mode=False,
    )
    try:
        found = problems(Path(module.params["folder"]), module.params["version"], module.params["requirements"], module.params["deadline"])
    except (EnvironmentCheckError, OSError) as error:
        module.fail_json(msg=str(error))
    module.exit_json(changed=False, intact=not found, problems=found)


if __name__ == "__main__":
    main()
