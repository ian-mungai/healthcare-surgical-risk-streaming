"""No secret value is put into a process environment, except the OpenBao unlock key that the owner's recorded exception allows.

A container's start script or entrypoint can export a secret that it read from a file, and an image's own entrypoint can export the content of a
``<NAME>__FILE`` or ``<NAME>_FILE`` variable. Either puts the value in the environment of the server process, where every process of the container user and
the host's root can read it. These checks read every Compose service and every start script of the project. A variable that names a secret file must be on the
reviewed list, with the reason that its reader does not export it. The acceptance test's environment search is the proof on the running containers.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path
from typing import Any

from scripts.stack.yaml_text import load_yaml

ROOT = Path(__file__).resolve().parents[2]
SERVICES = ROOT / "compose" / "services"
START_SCRIPTS = (ROOT / "compose" / "openbao" / "start.sh", ROOT / "orchestration" / "entrypoint.sh")
EXPORT_LINE = re.compile(r"\bexport\s+([^\n;&|]+)")
ALLOWED_EXPORTS = {ROOT / "compose" / "openbao" / "start.sh": {"OPENBAO_UNLOCK_KEY"}}
# A variable that names a secret file, with the reason that its reader never puts the file's content in the environment.
FILE_VARIABLES: dict[tuple[str, str], str] = {
    (
        "postgres",
        "POSTGRES_PASSWORD_FILE",
    ): "the image's entrypoint unsets every POSTGRES_ variable before it starts the server (docker-entrypoint.sh of postgres 18.7)",
    (
        "apicurio",
        "APICURIO_DATASOURCE_PASSWORD_FILE",
    ): "holds a path only; Apicurio 3.3.5's own interceptor reads the mounted password file itself (confirmed in the recheck of the slice)",
    (
        "aistor",
        "MINIO_ROOT_USER_FILE",
    ): "owner decision pending: the server reads the file itself and may keep the root user in its own environment (heavy run: docker exec)",
    (
        "aistor",
        "MINIO_ROOT_PASSWORD_FILE",
    ): "owner decision pending: the server reads the file itself and may keep the root password in its own environment (heavy run: docker exec)",
    **{
        (
            "dagster_code_location",
            f"AIRBYTE_{name}_FILE",
        ): "the project's own code reads the file (scripts/ingestion/record_run.py) and writes no environment variable"
        for name in ("API_URL", "NESSIE_URL", "CLIENT_ID", "CLIENT_SECRET", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
    },
}


def exported_names(text: str) -> list[str]:
    """Every variable name that an ``export`` command names, with any ``=value`` cut off, for each word after the command."""
    return [word.split("=", 1)[0] for line in EXPORT_LINE.findall(text) for word in line.split()]


def services() -> dict[str, dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    for path in sorted(SERVICES.glob("*.yml")):
        document = load_yaml(path.read_text())
        if not isinstance(document, dict):
            raise TypeError(f"{path.name} is not a mapping")
        found |= document["services"]
    return found


def script_text(service: dict[str, Any]) -> str:
    parts = [*(service.get("entrypoint") or []), *(service.get("command") or [])]
    return "\n".join(str(part) for part in parts)


def secret_values(found: dict[str, dict[str, Any]]) -> list[str]:
    """Each environment variable of a service whose name says password, token, key or licence and whose value is neither a secret file nor under /run."""
    return [
        f"{name}: {key}"
        for name, service in found.items()
        for key, value in (service.get("environment") or {}).items()
        if re.search(r"PASSWORD|SECRET|TOKEN|PRIVATE_KEY|ACCESS_KEY|LICEN[CS]E", key)
        and not str(value).startswith(("/run/secrets/", "/run/"))
        and not key.endswith("_FILE")
    ]


class EnvironmentTest(unittest.TestCase):
    def test_no_compose_start_script_exports_a_value(self) -> None:
        exporting = {name: exported_names(script_text(service)) for name, service in services().items() if exported_names(script_text(service))}
        self.assertEqual({}, exporting)

    def test_the_start_scripts_export_only_what_is_allowed(self) -> None:
        exported = {path.name: set(exported_names(path.read_text())) for path in START_SCRIPTS}
        allowed = {path.name: ALLOWED_EXPORTS.get(path, set()) for path in START_SCRIPTS}
        self.assertEqual(allowed, exported)

    def test_every_variable_that_names_a_secret_file_is_reviewed_and_none_is_the_grafana_form(self) -> None:
        unreviewed = []
        for name, service in services().items():
            for key, value in (service.get("environment") or {}).items():
                if str(value).startswith("/run/secrets/") and (name, key) not in FILE_VARIABLES:
                    unreviewed.append(f"{name}: {key}")
                if key.endswith("__FILE"):
                    unreviewed.append(f"{name}: {key} (the form that Grafana's entrypoint exports)")
        self.assertEqual([], unreviewed)

    def test_every_reviewed_variable_exists_and_names_a_reason(self) -> None:
        present = {(name, key) for name, service in services().items() for key in (service.get("environment") or {})}
        self.assertEqual([], [item for item in FILE_VARIABLES if item not in present])
        self.assertTrue(all(len(reason) > 20 for reason in FILE_VARIABLES.values()))

    def test_no_environment_value_of_a_service_is_a_password_a_token_a_key_or_a_licence(self) -> None:
        self.assertEqual([], secret_values(services()))

    def test_a_planted_value_is_found_in_each_spelling_and_a_file_reference_is_not(self) -> None:
        for key in (
            "PASSWORD",
            "MINIO_ROOT_PASSWORD",
            "API_TOKEN",
            "CLIENT_SECRET",
            "SOME_PRIVATE_KEY",
            "AWS_ACCESS_KEY_ID",
            "MINIO_LICENSE",
            "AISTOR_LICENCE",
        ):
            with self.subTest(key=key):
                self.assertEqual([f"s: {key}"], secret_values({"s": {"environment": {key: "a-value"}}}))
        for key, value in (("MINIO_LICENSE_FILE", "/run/secrets/x"), ("MINIO_LICENSE", "/run/secrets/x"), ("OTHER_NAME", "a-value")):
            with self.subTest(key=key):
                self.assertEqual([], secret_values({"s": {"environment": {key: value}}}))


if __name__ == "__main__":
    unittest.main()
