#!/usr/bin/python3 -I
"""Put one external secret into the host's OpenBao when its path is empty. Run as root on the host by the launcher.

The launcher sends one JSON document on standard input: ``{"name": <a name of compose/external_secrets.txt>, "value": <text or null>}``. A null value
asks whether the path is empty. A text value is written when the path is empty. The script takes no secret in an argument or the environment, and no
Ansible task or module ever holds the value. It prints one code and nothing else. It exits 0 for the codes ``empty``, ``present``, ``written`` and
``unchanged`` and 3 for the others: ``oversized`` (input over 4 KiB), ``not_json``, ``bad_keys`` (not exactly ``name`` and ``value``),
``unknown_name``, ``bad_form`` (a value that is not text or fails the check of its name), ``stored_bad_form`` (the stored value fails the check),
``no_login`` (the login of the intake role was refused or its secret ID file cannot be read), ``not_allowed`` (the policy of the intake role
forbids the call: the host playbook writes that policy from the list at every deploy, so run it first), ``no_answer`` (the server did not
answer), ``stored_mismatch`` (the stored text does not match the SHA-256 stored beside it, or has none), ``mismatch`` (the read-back text did not equal the sent
one: the stored version is removed, and a read shows the path empty), ``mismatch_kept`` (the same, but no read showed the path empty) and ``cannot_write``.

The write is one check-and-set call that stores the value and its SHA-256 together, so it creates them only while no value is stored. The call is read back
and compared by SHA-256 with the sent value, and every other read checks the stored text against the SHA-256 beside it: a mismatch is a refusal, never
``present``. A call that never arrived leaves the path empty and the next run writes. A call whose read-back was lost leaves the whole value with its
SHA-256 (the form was checked before it was sent, and one call stores all of it), and the next run finds it present. A removal is reported only after a read
shows the path empty. It uses only the standard library and the helpers that
the role installs beside it, and runs on the host's Python. The launcher starts it as ``sudo /usr/bin/python3 -I <this file>``; isolated mode drops
the script's folder from the import path, so the script loads the helpers by their path beside itself."""

from __future__ import annotations

import argparse
import http.client
import importlib.util
import json
import re
import sys
from pathlib import Path
from types import ModuleType
from typing import NoReturn
from urllib.parse import urlsplit


def load_helper(name: str) -> ModuleType:
    path = Path(__file__).resolve().parent / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(name)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


external = load_helper("external_secrets")

DEFAULT_DIRECTORY = "/opt/platform/secrets"
DEFAULT_LIST = "/opt/platform/compose/external_secrets.txt"
DEFAULT_ADDRESS = "http://127.0.0.1:8200"
ROLE = external.INTAKE_ROLE
ROLE_FILE = Path("openbao") / f"{ROLE}.secret_id"
MAX_INPUT_BYTES = 4096
HTTP_SECONDS = 10
TOKEN_FORM = re.compile(r"[A-Za-z0-9\-._~+/]+=*")
GOOD = ("empty", "present", "written", "unchanged")


class Refusal(RuntimeError):
    """The document cannot be used or the server refused. The error holds a code and no other text."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class Server:
    def __init__(self, address: str) -> None:
        parts = urlsplit(address)
        if parts.scheme != "http" or not parts.hostname:
            raise Refusal("no_answer")
        self.host, self.port = parts.hostname, parts.port or 80

    def call(self, method: str, path: str, token: str | None = None, body: dict[str, object] | None = None) -> tuple[int, dict[str, object]]:
        connection = http.client.HTTPConnection(self.host, self.port, timeout=HTTP_SECONDS)
        try:
            connection.request(method, f"/v1/{path}", json.dumps(body) if body is not None else None, {"X-Vault-Token": token} if token else {})
            response = connection.getresponse()
            raw = response.read().decode(errors="replace")
        except (OSError, http.client.HTTPException):
            raise Refusal("no_answer") from None
        finally:
            connection.close()
        try:
            document = json.loads(raw) if raw else {}
        except ValueError:
            document = {}
        return response.status, document if isinstance(document, dict) else {}


def parse_document(data: bytes) -> tuple[str, str | None]:
    """The name and the value (text or None) of the document, or ``Refusal`` with a code."""
    if len(data) > MAX_INPUT_BYTES:
        raise Refusal("oversized")
    try:
        document = json.loads(data.decode())
    except (ValueError, UnicodeDecodeError):
        raise Refusal("not_json") from None
    if not isinstance(document, dict):
        raise Refusal("not_json")
    if set(document) != {"name", "value"}:
        raise Refusal("bad_keys")
    name, value = document["name"], document["value"]
    if not isinstance(name, str) or not (value is None or isinstance(value, str)):
        raise Refusal("bad_form")
    return name, value


def login_token(server: Server, directory: Path) -> str:
    """The token of the intake role, which the host playbook's reconcile step wrote from the list (its policy holds the declared paths and nothing else)."""
    try:
        secret_id = (directory / ROLE_FILE).read_text().rstrip("\n")
    except OSError:
        raise Refusal("no_login") from None
    status, document = server.call("POST", "auth/approle/login", body={"role_id": ROLE, "secret_id": secret_id})
    auth = document.get("auth")
    token = auth.get("client_token") if isinstance(auth, dict) else None
    if status != 200 or not isinstance(token, str) or TOKEN_FORM.fullmatch(token) is None:
        raise Refusal("no_login")
    return token


def stored_document(server: Server, token: str, name: str) -> tuple[str | None, object]:
    """The value that the path holds and the SHA-256 stored beside it, or ``(None, None)`` when the path is empty (absent or removed). Nothing is checked."""
    status, document = server.call("GET", f"secret/data/external/{name}", token)
    if status == 404:
        return None, None
    if status == 403:
        raise Refusal("not_allowed")
    data = document.get("data")
    inner = data.get("data") if isinstance(data, dict) else None
    value = inner.get("value") if isinstance(inner, dict) else None
    if status != 200 or not isinstance(value, str):
        raise Refusal("cannot_write")
    return value, inner.get("sha256")


def stored_value(server: Server, token: str, name: str) -> str | None:
    """The value that the path holds, or None when it is empty. A text that does not match the SHA-256 beside it, or has none, is refused."""
    value, digest = stored_document(server, token, name)
    if value is not None and digest != external.value_digest(value):
        raise Refusal("stored_mismatch")
    return value


def removed(server: Server, token: str, name: str) -> bool:
    """Remove the stored version and return True only when a read afterwards shows the path empty."""
    try:
        server.call("DELETE", f"secret/data/external/{name}", token)
        return stored_document(server, token, name)[0] is None
    except Refusal:
        return False


def current_version(server: Server, token: str, name: str) -> int:
    status, document = server.call("GET", f"secret/metadata/external/{name}", token)
    if status == 404:
        return 0
    data = document.get("data")
    version = data.get("current_version") if isinstance(data, dict) else None
    if status == 403:
        raise Refusal("not_allowed")
    if status != 200 or not isinstance(version, int):
        raise Refusal("cannot_write")
    return version


def run(name: str, value: str | None, directory: Path, list_path: Path, address: str) -> str:
    try:
        declared = {item.name: item for item in external.parse_external(list_path.read_text())}
    except (OSError, external.ExternalSecretError):
        raise Refusal("cannot_write") from None
    if name not in declared:
        raise Refusal("unknown_name")
    check = declared[name].check
    if value is not None and not external.form_ok(check, value):
        raise Refusal("bad_form")
    server = Server(address)
    token = login_token(server, directory)
    stored = stored_value(server, token, name)
    if stored is not None and not external.form_ok(check, stored):
        raise Refusal("stored_bad_form")
    if value is None:
        return "empty" if stored is None else "present"
    if stored is not None:
        return "unchanged"
    version = current_version(server, token, name)
    sent = external.value_digest(value)
    status, _ = server.call("POST", f"secret/data/external/{name}", token, {"options": {"cas": version}, "data": {"value": value, "sha256": sent}})
    if status == 403:
        raise Refusal("not_allowed")
    if status == 400:
        raise Refusal("cannot_write") if stored_value(server, token, name) is None else Refusal("unchanged")
    if status != 200:
        raise Refusal("cannot_write")
    read_back, stored_digest = stored_document(server, token, name)
    if read_back is None or external.value_digest(read_back) != sent or stored_digest != sent:
        raise Refusal("mismatch" if removed(server, token, name) else "mismatch_kept")
    return "written"


class QuietParser(argparse.ArgumentParser):
    """An argument parser whose usage error names no word of the command line, so a mistyped value is never repeated on standard error."""

    def error(self, message: str) -> NoReturn:
        self.exit(2, f"{self.prog}: the command line is not valid\n")


def main(argv: list[str] | None = None) -> int:
    parser = QuietParser(description="Write one external secret from standard input into OpenBao when its path is empty.")
    parser.add_argument("--directory", default=DEFAULT_DIRECTORY, help="the secrets folder")
    parser.add_argument("--list", default=DEFAULT_LIST, help="the list of external secrets")
    parser.add_argument("--address", default=DEFAULT_ADDRESS, help="the address of OpenBao")
    arguments = parser.parse_args(argv)
    try:
        name, value = parse_document(sys.stdin.buffer.read(MAX_INPUT_BYTES + 1))
        code = run(name, value, Path(arguments.directory), Path(arguments.list), arguments.address)
    except Refusal as error:
        code = error.code
    sys.stdout.write(f"{code}\n")
    return 0 if code in GOOD else 3


if __name__ == "__main__":
    sys.exit(main())
