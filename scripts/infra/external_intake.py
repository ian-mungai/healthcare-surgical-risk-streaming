"""Give the host's OpenBao each external secret that is still empty there, from the source that ``.env`` names (``EXTERNAL_SECRETS_SOURCE``).

``compose/external_secrets.txt`` declares each external secret by name, consumer, OpenBao path and form check, never its value. For each one, the
launcher asks the host's intake script whether the path is empty. A path that holds a value is left alone and the source is not touched, so a second
run asks for nothing and writes nothing. For an empty path, the value comes from the source, is checked against its form here, and goes to the script
on standard input only (never in an argument, the environment, a file, an output or an error text). The script writes it when the path is still empty,
reads it back and compares it by SHA-256.

The two sources: ``openbao`` reads the value from this computer's local OpenBao (``references/local_machine.md``: the loopback server, the Keychain
item ``openbao-owner-token`` and the secret ``secret/<name>`` with the value in the field ``value``); ``prompt`` asks once on the terminal with no
echo and keeps nothing. Every refusal is a fixed message that names the secret and, at most, the source kind.
"""

from __future__ import annotations

import getpass
import http.client
import importlib.util
import json
import re
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any
from urllib.parse import urlsplit

from scripts.process import TimeoutExpired, run_command
from scripts.project_config import ROOT

HELPER = ROOT / "ansible" / "roles" / "secret_files" / "module_utils" / "external_secrets.py"
LIST_FILE = ROOT / "compose" / "external_secrets.txt"
LOCAL_ADDRESS = "http://127.0.0.1:8200"
KEYCHAIN_ITEM = "openbao-owner-token"
KEYCHAIN_SECONDS = 30
HTTP_SECONDS = 10
LOOPBACK = ("127.0.0.1", "localhost", "::1")
TOKEN_FORM = re.compile(r"[A-Za-z0-9\-._~+/]+=*")
TIMED_OUT = 124
GOOD_CODES = ("empty", "present", "written", "unchanged")
MESSAGES = {
    "no_terminal": (
        "external secret {name} is empty in the host's OpenBao and EXTERNAL_SECRETS_SOURCE is prompt, but this command has no "
        "terminal to ask on: run it in a terminal or set EXTERNAL_SECRETS_SOURCE=openbao in .env"
    ),
    "no_answer": "no value was given for external secret {name}",
    "bad_form": "the value for external secret {name} is not in the form of its check, so nothing was sent",
    "not_found": (
        "external secret {name} is empty in the host's OpenBao and this computer's OpenBao holds none under that name: store it "
        "there or set EXTERNAL_SECRETS_SOURCE=prompt in .env"
    ),
    "denied": "this computer's OpenBao refused the owner's login when asked for external secret {name}",
    "no_server": ("this computer's OpenBao did not answer when asked for external secret {name}: start it or set EXTERNAL_SECRETS_SOURCE=prompt in .env"),
    "no_keychain": "the owner's OpenBao login is not in the Keychain, so external secret {name} cannot be read from this computer's OpenBao",
    "bad_stored": "this computer's OpenBao holds a value for external secret {name} that is not text or is empty",
    "ssh_timeout": "the intake of external secret {name} did not finish in time; run the command again, which completes the write",
    "ssh_failed": "the intake of external secret {name} could not reach the host (exit status {status})",
    "script_unknown": "the intake script on the host answered with something that this program does not know, for external secret {name}",
    "script_oversized": "the intake script on the host refused the document for external secret {name}: it is larger than the script accepts",
    "script_not_json": "the intake script on the host refused the document for external secret {name}: it is not a JSON object",
    "script_bad_keys": "the intake script on the host refused the document for external secret {name}: it does not hold exactly a name and a value",
    "script_unknown_name": ("the host's copy of the list of external secrets does not declare {name}: the host playbook places the list, so run it first"),
    "script_bad_form": "the intake script on the host refused the value of external secret {name}: it is not in the form of its check",
    "script_stored_bad_form": "the value that the host's OpenBao holds for external secret {name} is not in the form of its check",
    "script_no_admin": "the intake script on the host could not log in to its OpenBao to store external secret {name}",
    "script_not_allowed": (
        "the admin policy of the host's OpenBao does not allow external secrets ({name}): the host was made before they existed, "
        "so recreate it or reset OpenBao's data volume"
    ),
    "script_no_answer": ("the host's OpenBao did not answer the intake script for external secret {name}; run the command again, which completes the write"),
    "script_mismatch": (
        "the host's OpenBao stored another text than the one sent for external secret {name}; the stored version was removed, so run the command again"
    ),
    "script_cannot_write": "the intake script on the host could not store external secret {name}",
    "written": "external secret {name} is stored in the host's OpenBao",
    "present": "external secret {name} is in the host's OpenBao and unchanged",
}
Source = Callable[[Any], str]
Ask = Callable[[str], tuple[int, str]]


class IntakeError(RuntimeError):
    """A refusal with a fixed message that names the secret and never a value."""


def helper() -> ModuleType:
    """The shared list module of the role ``secret_files``, loaded by its path: it is the one parser that the launcher, the host script and the module use."""
    spec = importlib.util.spec_from_file_location("external_secrets", HELPER)
    if spec is None or spec.loader is None:
        raise ImportError(str(HELPER))
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("external_secrets", module)
    spec.loader.exec_module(module)
    return module


def declared_secrets(path: Path = LIST_FILE) -> list[Any]:
    try:
        return list(helper().parse_external(path.read_text()))
    except (OSError, ValueError) as error:
        raise IntakeError(f"the list of external secrets cannot be used: {error}") from None


def write_line(text: str) -> None:
    sys.stdout.write(f"{text}\n")


def prompt_source(read: Callable[[str], str] | None = None, is_terminal: Callable[[], bool] | None = None) -> Source:
    """A source that asks on the terminal with no echo. It keeps nothing: the answer is returned and forgotten."""

    def ask(item: Any) -> str:
        if not (is_terminal or sys.stdin.isatty)():
            raise IntakeError(MESSAGES["no_terminal"].format(name=item.name))
        answer = (read or getpass.getpass)(f"Value of external secret {item.name} (hidden, kept in no file): ").strip()
        if not answer:
            raise IntakeError(MESSAGES["no_answer"].format(name=item.name))
        return answer

    return ask


def keychain_token() -> str:
    """The owner's login token for this computer's OpenBao, from the login Keychain. The token is never printed."""
    try:
        result = run_command("security", ["find-generic-password", "-a", getpass.getuser(), "-s", KEYCHAIN_ITEM, "-w"], timeout=KEYCHAIN_SECONDS)
    except TimeoutExpired, FileNotFoundError:
        raise IntakeError("no_keychain") from None
    token = result.stdout.strip()
    if result.returncode != 0 or not token:
        raise IntakeError("no_keychain")
    return token


def openbao_source(address: str = LOCAL_ADDRESS, token: Callable[[], str] = keychain_token) -> Source:
    """A source that reads ``secret/<name>`` (field ``value``) from this computer's local OpenBao. The address must be plain HTTP on the loopback interface."""
    parts = urlsplit(address)
    host = parts.hostname or ""
    if parts.scheme != "http" or host not in LOOPBACK:
        raise IntakeError("the address of this computer's OpenBao must be plain HTTP on the loopback interface, so the login never leaves the computer")

    def read(item: Any) -> str:
        name = item.name
        try:
            login = token()
        except IntakeError:
            raise IntakeError(MESSAGES["no_keychain"].format(name=name)) from None
        if TOKEN_FORM.fullmatch(login) is None:
            raise IntakeError(MESSAGES["denied"].format(name=name))
        connection = http.client.HTTPConnection(host, parts.port or 80, timeout=HTTP_SECONDS)
        try:
            connection.request("GET", f"/v1/secret/data/{name}", headers={"X-Vault-Token": login})
            response = connection.getresponse()
            raw = response.read().decode(errors="replace")
        except OSError, http.client.HTTPException:
            raise IntakeError(MESSAGES["no_server"].format(name=name)) from None
        finally:
            connection.close()
        if response.status == 404:
            raise IntakeError(MESSAGES["not_found"].format(name=name))
        if response.status != 200:
            raise IntakeError(MESSAGES["denied"].format(name=name))
        try:
            value = json.loads(raw)["data"]["data"]["value"]
        except ValueError, KeyError, TypeError:
            value = None
        if not isinstance(value, str) or not value.strip():
            raise IntakeError(MESSAGES["bad_stored"].format(name=name))
        return value.strip()

    return read


def reply_code(name: str, reply: tuple[int, str]) -> str:
    """The code of one answer of the host's script: a good code, or a refusal as ``IntakeError`` with its fixed message."""
    status, output = reply
    code = output.strip()
    if status == 0 and code in GOOD_CODES:
        return code
    if status == 3 and f"script_{code}" in MESSAGES:
        raise IntakeError(MESSAGES[f"script_{code}"].format(name=name))
    if status == TIMED_OUT:
        raise IntakeError(MESSAGES["ssh_timeout"].format(name=name))
    if status not in (0, 3):
        raise IntakeError(MESSAGES["ssh_failed"].format(name=name, status=status))
    raise IntakeError(MESSAGES["script_unknown"].format(name=name))


def run_intake(declared: list[Any], ask_host: Ask, source: Source, write: Callable[[str], None]) -> None:
    """For each declared secret, in list order: leave a filled path alone, and fill an empty one from the source."""
    form_ok = helper().form_ok
    for item in declared:
        if reply_code(item.name, ask_host(json.dumps({"name": item.name, "value": None}))) == "present":
            write(MESSAGES["present"].format(name=item.name))
            continue
        value = source(item)
        if not form_ok(item.check, value):
            raise IntakeError(MESSAGES["bad_form"].format(name=item.name))
        code = reply_code(item.name, ask_host(json.dumps({"name": item.name, "value": value})))
        write(MESSAGES["written" if code == "written" else "present"].format(name=item.name))
