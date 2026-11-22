"""The replay image holds every project module the replay imports, and the tracked release pin the gate reads, at the paths the code looks for them.

The replay calls the ingestion gate, which imports further project modules and reads ``tests/fixtures/vitaldb/release.json`` through ``ROOT``. A module
or the pin left out of the image stops the replay at start, which only a build and a run would show. The imports are followed from the replay's own
sources with ``ast``; the Dockerfile's ``COPY`` lines and the build's ignore file are read as the build reads them. The planted cases remove one copy each.
"""

from __future__ import annotations

import ast
import fnmatch
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = ROOT / "replay" / "Dockerfile"
IGNORE_FILE = ROOT / "replay" / "Dockerfile.dockerignore"
APP = "/opt/replay/app"
READ_FILES = ("tests/fixtures/vitaldb/release.json",)


def module_file(name: str) -> str | None:
    """The repository path of a project module, or None for a module outside the repository."""
    base = ROOT.joinpath(*name.split("."))
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.is_file():
            return str(candidate.relative_to(ROOT))
    return None


def imported_files(start: list[str]) -> set[str]:
    """Every repository file that the files in ``start`` import, directly or through each other, with the package files on the way."""
    found: set[str] = set()
    pending = list(start)
    while pending:
        path = pending.pop()
        if path in found:
            continue
        found.add(path)
        parts = Path(path).parts[:-1]
        for depth in range(1, len(parts) + 1):
            package = Path(*parts[:depth], "__init__.py")
            if (ROOT / package).is_file():
                pending.append(str(package))
        for node in ast.walk(ast.parse((ROOT / path).read_text())):
            names = [alias.name for alias in node.names] if isinstance(node, ast.Import) else []
            if isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                names = [node.module, *(f"{node.module}.{alias.name}" for alias in node.names)]
            pending += [file for name in names if (file := module_file(name)) is not None]
    return found


def copies(text: str) -> list[tuple[str, str]]:
    """Each source and destination of the ``COPY`` lines that copy from the build context."""
    pairs = []
    for line in text.replace("\\\n", " ").splitlines():
        words = line.split()
        if words[:1] == ["COPY"] and not any(word.startswith("--from=") for word in words):
            pairs += [(source.rstrip("/"), words[-1]) for source in words[1:-1]]
    return pairs


def ignored(path: str, rules: list[str]) -> bool:
    """The build's decision for ``path``: the last rule that matches it or one of its folders wins, and a ``!`` rule takes the path back in."""
    decision = False
    folders = [str(Path(*Path(path).parts[: depth + 1])) for depth in range(len(Path(path).parts))]
    for rule in rules:
        pattern = rule.removeprefix("!")
        matched = any(fnmatch.fnmatchcase(folder, pattern) or fnmatch.fnmatchcase(folder, pattern.removeprefix("**/")) for folder in folders)
        if pattern.startswith("**/"):
            matched = matched or any(fnmatch.fnmatchcase(part, pattern.removeprefix("**/")) for part in Path(path).parts)
        if matched:
            decision = not rule.startswith("!")
    return decision


def landed_at(path: str, source: str, destination: str) -> str:
    """Where a ``COPY`` of ``source`` to ``destination`` puts ``path``: a folder's contents go below the destination, a file into a destination folder."""
    if path != source:
        return destination.rstrip("/") + path.removeprefix(source)
    return destination + Path(source).name if destination.endswith("/") else destination


def problems(dockerfile: str, ignore_rules: list[str], needed: set[str]) -> list[str]:
    found = []
    pairs = copies(dockerfile)
    for path in sorted(needed):
        destinations = [landed_at(path, source, destination) for source, destination in pairs if path == source or path.startswith(source + "/")]
        if f"{APP}/{path}" not in destinations:
            found.append(f"{path}: no COPY line puts it at {APP}/{path}")
        elif ignored(path, ignore_rules):
            found.append(f"{path}: the build's ignore file leaves it out")
    return found


def ignore_rules(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if line.strip() and not line.startswith("#")]


def replay_sources() -> list[str]:
    return sorted(str(path.relative_to(ROOT)) for folder in ("replay/service", "replay/events") for path in (ROOT / folder).glob("*.py"))


class RealImageTest(unittest.TestCase):
    def test_every_imported_module_and_the_pin_are_in_the_image(self) -> None:
        needed = imported_files(replay_sources()) | set(READ_FILES)
        self.assertIn("scripts/ingestion/gate.py", needed)
        self.assertIn("scripts/sample/candidates.py", needed)
        self.assertEqual([], problems(DOCKERFILE.read_text(), ignore_rules(IGNORE_FILE.read_text()), needed))


def service_users() -> dict[str, str]:
    """The numeric user of each Compose service file that names one."""
    users = {}
    for path in sorted((ROOT / "compose" / "services").glob("*.yml")):
        match = re.search(r'^\s+user: "([0-9]+:[0-9]+)"', path.read_text(), re.MULTILINE)
        if match:
            users[path.stem] = match.group(1)
    return users


def user_problems(dockerfile: str, users: dict[str, str]) -> list[str]:
    """The image's last user must be the replay service's user, and no other service may run as that user ID."""
    found = []
    last = [line.split()[1] for line in dockerfile.splitlines() if line.startswith("USER ")][-1:]
    if last != [users.get("replay")]:
        found.append(f"the image's last USER {last} is not the replay service's user {users.get('replay')}")
    uid = (users.get("replay") or ":").split(":")[0]
    found += [f"{name} runs as the replay's user ID {uid}" for name, user in users.items() if name != "replay" and user.split(":")[0] == uid]
    return found


class UserTest(unittest.TestCase):
    def test_the_image_runs_as_the_replay_service_user_that_no_other_service_has(self) -> None:
        users = service_users()
        self.assertEqual("10003:10003", users.get("replay"))
        self.assertEqual([], user_problems(DOCKERFILE.read_text(), users))

    def test_another_user_in_the_image_or_a_shared_user_is_named(self) -> None:
        users, text = service_users(), DOCKERFILE.read_text()
        self.assertEqual(1, len(user_problems(text.replace("USER 10003:10003", "USER 10002:10002"), users)))
        self.assertEqual(["apicurio runs as the replay's user ID 10003"], user_problems(text, {**users, "apicurio": "10003:10003"}))


class PlantedDefectTest(unittest.TestCase):
    def test_a_module_or_the_pin_left_out_is_named(self) -> None:
        needed = imported_files(replay_sources()) | set(READ_FILES)
        text, rules = DOCKERFILE.read_text(), ignore_rules(IGNORE_FILE.read_text())
        for path in ("scripts/sample/candidates.py", "scripts/project_config.py", "tests/fixtures/vitaldb/release.json"):
            with self.subTest(left_out=path):
                self.assertIn(f"{path}: the build's ignore file leaves it out", problems(text, [*rules, path], needed))
        without = "\n".join(line for line in text.splitlines() if "scripts/ingestion" not in line)
        self.assertIn(f"scripts/ingestion/gate.py: no COPY line puts it at {APP}/scripts/ingestion/gate.py", problems(without, rules, needed))


if __name__ == "__main__":
    unittest.main()
