"""Every secret input of the stack, the harmless marker for each and the scanner that looks for a marker in every sink.

The inventory is data. ``derived_files`` reads the secret files out of the code (the Compose files, the two tracked lists, the Ansible modules and the key
script), and a fast test refuses a file that has no line here, so a new secret cannot be added without a case. The acceptance test and the slow sink tests take
their inputs from this module. A marker is random for each run and has the form that its entry point requires, so the real entry point accepts it.
"""

from __future__ import annotations

import ast
import base64
import re
import secrets
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from scripts.project_config import OPTIONAL_PATTERNS, ROOT
from scripts.stack.yaml_text import load_yaml

MINIMUM_MARKER_CHARACTERS = 16
FORMS = ("hex64", "hex20", "urlsafe", "aws_key_id", "aws_secret", "plain")
SECRETS_REFERENCE = re.compile(r"\$\{SECRETS_DIR[^}]*\}/(.+)")
UNLOCK_REFERENCE = re.compile(r"\$\{OPENBAO_UNLOCK_KEY_FILE[^}]*\}")
LICENCE_REFERENCE = re.compile(r"\$\{AISTOR_LICENCE_FILE[^}]*\}")


@dataclass(frozen=True)
class Marker:
    label: str
    value: str


@dataclass(frozen=True)
class Hit:
    label: str
    form: str
    path: str = ""


def make_marker(form: str) -> Marker:
    """A new harmless marker in the form that the entry point of ``form`` takes."""
    if form == "hex64":
        return Marker(form, secrets.token_hex(32))
    if form == "hex20":
        return Marker(form, secrets.token_hex(10))
    if form == "urlsafe":
        return Marker(form, "sinkmark_" + secrets.token_urlsafe(32))
    if form == "aws_key_id":
        return Marker(form, "SINKMARK" + secrets.token_hex(6).upper())
    if form == "aws_secret":
        return Marker(form, "SinkMark/" + secrets.token_urlsafe(24).replace("-", "+").replace("_", "/"))
    if form == "plain":
        return Marker(form, "sinkmark-" + secrets.token_hex(12))
    raise ValueError(f"unknown marker form {form!r}")


def base64_forms(value: str) -> list[str]:
    """The Base64 texts (standard and URL-safe) that hold ``value`` at each byte offset, cut to the characters that do not depend on the neighbours."""
    found: list[str] = []
    for offset in range(3):
        raw = b"\0" * offset + value.encode()
        encoded = base64.b64encode(raw).decode().rstrip("=")
        start = -(offset * 8 // -6)
        tail = {0: 0, 1: 2, 2: 1}[len(raw) % 3]
        stable = encoded[start : len(encoded) - tail]
        found += [stable, stable.replace("+", "-").replace("/", "_")]
    return [form for form in dict.fromkeys(found) if form]


def lower_escapes(text: str) -> list[str]:
    """The text with the hexadecimal digits of each percent escape in lower case, which some encoders write."""
    return [re.sub(r"%[0-9A-F]{2}", lambda found: found[0].lower(), text)]


def search_forms(value: str) -> dict[str, list[str]]:
    """Each form that a log or a file may hold for ``value``."""
    forms = {
        "plain": [value],
        "percent": [quote(value, safe=""), quote(value), *lower_escapes(quote(value, safe="")), *lower_escapes(quote(value))],
        "hex": [value.encode().hex(), value.encode().hex().upper()],
        "base64": base64_forms(value),
    }
    if "/" in value:
        forms["json"] = [value.replace("/", "\\/")]
    return forms


def scan_text(data: str | bytes, markers: Iterable[Marker]) -> list[Hit]:
    """Each marker found in ``data``, once for each form it appears in. A marker that is too short to rule out a chance match is refused."""
    text = data.decode("latin-1") if isinstance(data, bytes) else data
    hits: list[Hit] = []
    for marker in markers:
        if len(marker.value) < MINIMUM_MARKER_CHARACTERS:
            raise ValueError(f"marker {marker.label!r} is shorter than {MINIMUM_MARKER_CHARACTERS} characters")
        for form, candidates in search_forms(marker.value).items():
            if any(candidate in text for candidate in candidates if form == "plain" or candidate != marker.value):
                hits.append(Hit(marker.label, form))
    return hits


def scan_tree(root: Path, markers: list[Marker], allowed: Iterable[Path] = ()) -> list[Hit]:
    """Each marker found in a file under ``root``, with the file's path relative to ``root``. Links are not followed and the ``allowed`` files are skipped."""
    skipped = {path.resolve() for path in allowed}
    hits: list[Hit] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or not path.is_file() or path.resolve() in skipped:
            continue
        relative = path.relative_to(root).as_posix()
        hits += [Hit(hit.label, hit.form, relative) for hit in scan_text(path.read_bytes(), markers)]
    return hits


def listed_names(path: Path) -> list[str]:
    """The names of a tracked list: the first word of each line, comments and blank lines ignored, a tool's databases after its colon ignored."""
    words = [line.split("#", 1)[0].partition(":")[0].split() for line in path.read_text().splitlines()]
    return [line[0] for line in words if line]


def string_constants(path: Path, names: Iterable[str]) -> dict[str, str]:
    """The module-level string constants of a Python file that has the given names, read without importing the file."""
    wanted = set(names)
    found: dict[str, str] = {}
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            found |= {target.id: node.value.value for target in node.targets if isinstance(target, ast.Name) and target.id in wanted}
    return found


def generated_names(path: Path) -> list[str]:
    """The keys of the ``GENERATED`` dictionary of the Airbyte secrets module."""
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == "GENERATED" and isinstance(node.value, ast.Dict):
            return [key.value for key in node.value.keys if isinstance(key, ast.Constant) and isinstance(key.value, str)]
    raise ValueError(f"{path} has no GENERATED dictionary")


def compose_secret_files(root: Path) -> set[str]:
    """The file of every Compose secret, relative to the secrets folder (the unlock key file is ``.env``, the AIStor licence file ``aistor/licence``)."""
    files: set[str] = set()
    for service_file in sorted((root / "compose" / "services").glob("*.yml")):
        document = load_yaml(service_file.read_text())
        if not isinstance(document, dict):
            raise ValueError(f"{service_file.name} is not a mapping")
        for definition in (document.get("secrets") or {}).values():
            reference = str(definition["file"])
            if UNLOCK_REFERENCE.fullmatch(reference):
                files.add(".env")
            elif LICENCE_REFERENCE.fullmatch(reference):
                files.add("aistor/licence")
            elif (found := SECRETS_REFERENCE.fullmatch(reference)) is not None:
                files.add(found[1])
            else:
                raise ValueError(f"{service_file.name}: the secret file {reference!r} is not under the secrets folder")
    return files


def derived_files(root: Path = ROOT) -> set[str]:
    """Every secret file or input that the code names: a path under the secrets folder, ``config/<KEY>`` or ``workload_access/credentials.json``."""
    roles = root / "ansible" / "roles"
    files = compose_secret_files(root)
    files |= {f"postgres/{tool}.password" for tool in listed_names(root / "compose" / "postgres_tools.txt")}
    files |= {f"openbao/{service}.secret_id" for service in listed_names(root / "compose" / "openbao_services.txt")}
    files |= {f"airbyte/{name}" for name in generated_names(roles / "airbyte" / "library" / "airbyte_secrets.py")}
    script = string_constants(roles / "airbyte" / "files" / "airbyte_key_install.py", ["FOLDER", "CONTAINER_FOLDER", "ID_FILE", "VALUE_FILE"])
    files |= {f"{folder}/{name}" for folder in (script["FOLDER"], script["CONTAINER_FOLDER"]) for name in (script["ID_FILE"], script["VALUE_FILE"])}
    files |= {f"airbyte_api/{name}" for name in re.findall(r"\{name: (\w+), value:", (roles / "airbyte" / "tasks" / "record_client_files.yml").read_text())}
    files |= {f"k3s/{name}" for name in re.findall(r"\{name: ([\w.]+), value:", (roles / "airbyte" / "tasks" / "group_access.yml").read_text())}
    credentials = roles / "workload_access" / "files" / "workload_credentials.py"
    if "credentials.json" in credentials.read_text():
        files.add("workload_access/credentials.json")
    if "iam/security-credentials" in credentials.read_text():
        files.add("instance_metadata/host_role_credentials")
    files |= {f"config/{key}" for key, pattern in OPTIONAL_PATTERNS.items() if pattern.pattern.startswith("(?!tskey)")}
    return files


@dataclass(frozen=True)
class SecretInput:
    name: str
    patterns: tuple[str, ...]
    form: str
    where: str
    entry_point: str
    consumer: str
    control: str

    def paths(self, root: Path = ROOT) -> set[str]:
        """The files this line claims, with ``{tool}`` and ``{service}`` replaced by each name of the tracked lists."""
        tools = listed_names(root / "compose" / "postgres_tools.txt")
        services = listed_names(root / "compose" / "openbao_services.txt")
        claimed: set[str] = set()
        for pattern in self.patterns:
            if "{tool}" in pattern:
                claimed |= {pattern.format(tool=tool) for tool in tools}
            elif "{service}" in pattern:
                claimed |= {pattern.format(service=service) for service in services}
            else:
                claimed.add(pattern)
        return claimed

    def claims(self, path: str) -> bool:
        return path in self.paths()


INPUTS: tuple[SecretInput, ...] = (
    SecretInput(
        "openbao_unlock_key",
        (".env",),
        "hex64",
        "local",
        "the OPENBAO_UNLOCK_KEY line of the host's key file; the owner puts the key there and the unlock_key role makes a random one only for a missing file",
        "OpenBao's static seal, through the process environment of the container's start script",
        "after a restart OpenBao unseals with the marker key and a start with another key stays sealed",
    ),
    SecretInput(
        "openbao_admin_secret_id",
        ("openbao/bootstrap.secret_id",),
        "urlsafe",
        "local",
        "the file under the secrets folder; the openbao role's prepare step keeps an existing file",
        "OpenBao's first start (the initialize block) and the bootstrap step's login",
        "a login as the bootstrap role with the marker succeeds",
    ),
    SecretInput(
        "postgres_admin_password",
        ("postgres/postgres.password",),
        "urlsafe",
        "local",
        "the file under the secrets folder; the accounts step keeps an existing file",
        "the PostgreSQL server (POSTGRES_PASSWORD_FILE) and the accounts step's psql",
        "a login as postgres over TCP with the marker succeeds and a login with another password fails",
    ),
    SecretInput(
        "postgres_tool_password",
        ("postgres/{tool}.password",),
        "urlsafe",
        "local",
        "the file under the secrets folder; the accounts step keeps an existing file",
        "the tool's PostgreSQL account (as a SCRAM verifier), the tool's path in OpenBao and the tool's container",
        "a login as the tool with the marker succeeds, and the tool's secret ID reads the marker from OpenBao",
    ),
    SecretInput(
        "openbao_service_secret_id",
        ("openbao/{service}.secret_id",),
        "urlsafe",
        "local",
        "issued by OpenBao and written by the bootstrap step, so it is tracked after it is issued",
        "the service's own AppRole login",
        "a login as the service with the file's value gets a token that reads its own path and not another service's",
    ),
    SecretInput(
        "grafana_admin_password",
        ("grafana/admin.password",),
        "urlsafe",
        "local",
        "the file under the secrets folder, written from the host's OpenBao, which keeps the password (an existing file's password is taken into OpenBao once)",
        "Grafana's administrator sign-in, through the $__file{} provider in its mounted configuration (compose/monitoring/grafana/grafana.ini)",
        "the Grafana API accepts the marker for the administrator and refuses another password",
    ),
    SecretInput(
        "aistor_root_user",
        ("aistor/root_user",),
        "urlsafe",
        "local",
        "the file under the secrets folder; the secret_file module keeps an existing file",
        "AIStor's root user, through MINIO_ROOT_USER_FILE, and the AIStor step, which reads the file inside the container for one mc call",
        "the S3 API accepts the marker as the root user and refuses another name",
    ),
    SecretInput(
        "aistor_root_password",
        ("aistor/root_password",),
        "urlsafe",
        "local",
        "the file under the secrets folder; the secret_file module keeps an existing file",
        "AIStor's root password, through MINIO_ROOT_PASSWORD_FILE, and the AIStor step, which reads the file inside the container for one mc call",
        "the S3 API accepts the marker as the root password and refuses another",
    ),
    SecretInput(
        "aistor_licence",
        ("aistor/licence",),
        "plain",
        "local",
        "standard input of the licence script, which the launcher feeds from this computer's OpenBao (secret/aistor_licence)",
        "AIStor's server, through its --license flag with the path of the mounted file",
        "the script writes the one root-only file and answers one code; a missing file is a recorded skip of AIStor, an empty one a fixed refusal",
    ),
    SecretInput(
        "project_key_id",
        ("airbyte/aws_access_key_id", "airbyte_api/aws_access_key_id"),
        "aws_key_id",
        "local",
        "standard input of the key script, which the launcher feeds from the project profile",
        "Airbyte's connectors (root file) and the code location (copy for its user)",
        "the four files hold the marker, each for its own owner, and the script's answer is one code",
    ),
    SecretInput(
        "project_key_secret",
        ("airbyte/aws_secret_access_key", "airbyte_api/aws_secret_access_key"),
        "aws_secret",
        "local",
        "standard input of the key script, which the launcher feeds from the project profile",
        "Airbyte's connectors (root file) and the code location (copy for its user)",
        "the four files hold the marker, each for its own owner, and the script's answer is one code",
    ),
    SecretInput(
        "airbyte_admin_password",
        ("airbyte/admin.password",),
        "urlsafe",
        "host",
        "the file under the secrets folder; the airbyte_secrets module keeps an existing file",
        "Airbyte's instance administrator, through the secret airbyte-auth-secrets in k3s",
        "the secret in k3s holds the marker and Airbyte's pod reads it",
    ),
    SecretInput(
        "airbyte_storage_access_key_id",
        ("airbyte/storage_access_key_id",),
        "hex20",
        "host",
        "the file under the secrets folder; the airbyte_secrets module keeps an existing file",
        "Airbyte's own object store, through the secret airbyte-storage in k3s",
        "the secret in k3s holds the marker and the object store accepts it",
    ),
    SecretInput(
        "airbyte_storage_secret_access_key",
        ("airbyte/storage_secret_access_key",),
        "urlsafe",
        "host",
        "the file under the secrets folder; the airbyte_secrets module keeps an existing file",
        "Airbyte's own object store, through the secret airbyte-storage in k3s",
        "the secret in k3s holds the marker and the object store accepts it",
    ),
    SecretInput(
        "airbyte_client_id",
        ("airbyte_api/client_id",),
        "plain",
        "host",
        "Airbyte writes it to airbyte-auth-secrets at its first start and the airbyte role copies it for the code location",
        "the code location's call of Airbyte's API",
        "the code location's token request with the pair succeeds",
    ),
    SecretInput(
        "airbyte_client_secret",
        ("airbyte_api/client_secret",),
        "plain",
        "host",
        "Airbyte writes it to airbyte-auth-secrets at its first start and the airbyte role copies it for the code location",
        "the code location's call of Airbyte's API",
        "the code location's token request with the pair succeeds",
    ),
    SecretInput(
        "k3s_token",
        ("k3s/dagster.token",),
        "plain",
        "host",
        "k3s issues it for the code location's service account and the airbyte role writes the file",
        "the code location's call of the k3s API",
        "the code location's call is permitted for its own verbs and refused for another namespace",
    ),
    SecretInput(
        "workload_credentials",
        ("workload_access/credentials.json",),
        "plain",
        "host",
        "the credential helper's call of AWS STS with the host role, one file for each workload",
        "each workload's own AWS access, read through the credential process of its AWS configuration",
        "a permitted operation, a refused operation and a renewal for each workload",
    ),
    SecretInput(
        "host_role_credentials",
        ("instance_metadata/host_role_credentials",),
        "plain",
        "host",
        "the instance metadata service, which gives the host role's temporary credentials to the credential helper only",
        "the credential helper (workload-credentials.service), which assumes each workload's role with them; the metadata guard refuses every other process",
        "a container on the host network, a container on the bridge network and an Airbyte pod cannot reach the metadata address",
    ),
    SecretInput(
        "tailscale_client_id",
        ("config/TAILSCALE_CLIENT_ID",),
        "plain",
        "local",
        "the TAILSCALE_CLIENT_ID line of the owner's .env file; an auth key given there is refused",
        "Terraform's host module (the federated identity), which stores no key",
        "the configuration loader and Terraform each refuse a key in its place and name no value",
    ),
)

NOT_SECRET: dict[str, str] = {
    "k3s/ca.crt": "the certificate authority of the cluster, a public certificate",
    "airbyte_api/api_url": "an address on the bridge network",
    "airbyte_api/nessie_url": "an address on the bridge network",
}
