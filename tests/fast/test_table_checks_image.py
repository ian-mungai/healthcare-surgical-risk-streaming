"""The table checks job is deployable: the code location loads it, the image holds every module it imports and the code location has the staging volume.

The text of the Dockerfile, of the Compose file and of the definitions is the contract here: the build and the code location read these lines. A planted
change shows that each check names what it finds missing.
"""

from __future__ import annotations

import ast
import re
import unittest
from pathlib import Path
from typing import Any

import yaml

from scripts.ingestion import table_checks as run

ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = ROOT / "orchestration" / "Dockerfile"
DEFINITIONS = ROOT / "orchestration" / "surgical_risk" / "definitions.py"
CODE_LOCATION = ROOT / "compose" / "services" / "dagster_code_location.yml"
ENTRY = "orchestration/surgical_risk/table_checks.py"
CONTEXT_FOLDERS = {"stack_scripts": "scripts", "stack_spark": "spark"}
IMAGE_APP = "/opt/dagster/app/"


def module_file(name: str) -> str | None:
    path = name.replace(".", "/")
    for candidate in (f"{path}.py", f"{path}/__init__.py"):
        if (ROOT / candidate).is_file():
            return candidate
    return None


def imported_files(entry: str) -> set[str]:
    """Every file of the ``scripts`` and ``spark`` packages that ``entry`` imports, directly or through another, with the ``__init__.py`` of each package."""
    found: set[str] = set()
    pending = [entry]
    while pending:
        tree = ast.parse((ROOT / pending.pop()).read_text())
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                names = [node.module, *(f"{node.module}.{alias.name}" for alias in node.names)]
            elif isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            for name in names:
                path = module_file(name) if name.split(".")[0] in CONTEXT_FOLDERS.values() else None
                if path is not None and path not in found:
                    found.add(path)
                    pending.append(path)
    for path in list(found):
        parts = path.split("/")
        found.update("/".join(parts[:depth]) + "/__init__.py" for depth in range(1, len(parts)))
    return {path for path in found if (ROOT / path).is_file()}


def copied(text: str) -> set[str]:
    """The files that ``COPY --from=<context>`` takes from the repository's ``scripts`` and ``spark`` folders, as paths of the repository."""
    found: set[str] = set()
    for line in text.replace("\\\n", " ").splitlines():
        words = line.split()
        if len(words) > 3 and words[0] == "COPY" and words[1].startswith("--from=") and words[1].removeprefix("--from=") in CONTEXT_FOLDERS:
            folder = CONTEXT_FOLDERS[words[1].removeprefix("--from=")]
            found.update(f"{folder}/{source}" for source in words[2:-1])
    return found


class ImageTest(unittest.TestCase):
    def test_every_module_the_job_imports_is_copied_into_the_image(self) -> None:
        needed = imported_files(ENTRY)
        for path in (
            "scripts/ingestion/table_checks.py",
            "scripts/ingestion/gate.py",
            "scripts/stack/nessie.py",
            "scripts/aws_cli.py",
            "spark/table_checks.py",
            "spark/__init__.py",
        ):
            self.assertIn(path, needed)
        self.assertEqual([], sorted(needed - copied(DOCKERFILE.read_text())))

    def test_a_missing_module_is_named_by_the_check(self) -> None:
        needed = imported_files(ENTRY)
        for old, new in (
            ("ingestion/table_checks.py", ""),
            ("COPY --from=stack_spark __init__.py table_checks.py", "COPY --from=stack_spark __init__.py"),
            ("ingestion/gate.py", ""),
            (
                "COPY --from=stack_scripts __init__.py aws_cli.py process.py project_config.py",
                "COPY --from=stack_scripts __init__.py process.py project_config.py",
            ),
        ):
            text = DOCKERFILE.read_text()
            self.assertIn(old, text)
            self.assertTrue(needed - copied(text.replace(old, new, 1)), old)

    def test_the_release_pin_the_check_opens_is_copied_to_where_the_code_looks(self) -> None:
        self.assertIn(f"COPY --from=stack_fixtures release.json {IMAGE_APP}tests/fixtures/vitaldb/release.json", DOCKERFILE.read_text())

    def test_the_named_build_contexts_are_the_ones_the_dockerfile_documents(self) -> None:
        text = DOCKERFILE.read_text()
        used = set(re.findall(r"--from=(stack_\w+)", text))
        self.assertLessEqual({"stack_scripts", "stack_spark", "stack_fixtures"}, used)
        self.assertIn("`stack_spark`", text.split("FROM ", 1)[0])

    def test_the_staging_folder_exists_in_the_image_for_the_container_user(self) -> None:
        text = DOCKERFILE.read_text().replace("\\\n", " ")
        line = next(line for line in text.splitlines() if line.startswith("RUN useradd"))
        self.assertIn(run.STAGING_DIRECTORY, line.split("&&")[1])
        self.assertIn("chown -R dagster:dagster /opt/dagster", line)


class RegistrationTest(unittest.TestCase):
    def test_the_definitions_the_code_location_loads_merge_the_table_checks_job(self) -> None:
        tree = ast.parse(DEFINITIONS.read_text())
        imported = {alias.name for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.level == 1 for alias in node.names}
        self.assertIn("table_checks", imported)
        merge = next(
            node for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "merge" and node.args
        )
        self.assertIn("table_checks.defs", [ast.unparse(argument) for argument in merge.args])

    def test_the_job_is_named_in_the_module_the_definitions_import(self) -> None:
        source = (ROOT / ENTRY).read_text()
        self.assertIn("def table_checks_job", source)
        self.assertIn("Definitions(jobs=[table_checks_job])", source)


class StagingVolumeTest(unittest.TestCase):
    def document(self) -> dict[str, Any]:
        document: dict[str, Any] = yaml.safe_load(CODE_LOCATION.read_text())
        return document

    def test_the_code_location_mounts_a_declared_named_volume_where_the_code_writes_the_staged_files(self) -> None:
        document = self.document()
        self.assertIn(f"{run.STAGING_VOLUME}:{run.STAGING_DIRECTORY}", document["services"]["dagster_code_location"]["volumes"])
        self.assertIn(run.STAGING_VOLUME, document["volumes"])

    def test_the_state_directory_is_a_folder_of_a_named_volume_that_the_code_location_mounts_so_it_survives_a_restart(self) -> None:
        document = self.document()
        mounts = {entry.split(":")[0]: entry.split(":")[1] for entry in document["services"]["dagster_code_location"]["volumes"] if not entry.startswith("/")}
        self.assertIn(run.STATE_DIRECTORY, [Path(mounts["dagster_data"]) / "table_checks"])
        self.assertIn("dagster_data", document["volumes"])
        self.assertNotIn("/run/", str(run.STATE_DIRECTORY))

    def test_the_staging_folder_is_a_folder_of_the_image_and_of_the_mount(self) -> None:
        self.assertEqual("/opt/dagster/staging", run.STAGING_DIRECTORY)
        self.assertTrue(run.STAGING_DIRECTORY.startswith("/opt/dagster/"))

    def test_the_job_names_the_volume_of_the_compose_project_from_the_constants(self) -> None:
        source = (ROOT / ENTRY).read_text()
        self.assertIn("checks.STAGING_VOLUME", source)
        self.assertIn("checks.STAGING_DIRECTORY", source)
        self.assertNotIn("TABLE_CHECKS_STAGING", source)


if __name__ == "__main__":
    unittest.main()
