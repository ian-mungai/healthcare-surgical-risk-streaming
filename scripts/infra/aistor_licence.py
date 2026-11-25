"""Read AIStor's licence from this computer's OpenBao, for the launcher to place on the host. The licence is never printed, logged or put in a message.

The owner saves the licence in the local OpenBao at ``secret/aistor_licence``, field ``value`` (``references/local_machine.md``: the loopback server), as
for every secret that a third party issues. This module reads it with a token of its own over plain HTTP on the loopback interface: the Keychain item
``openbao-aistor-licence-token``, whose OpenBao policy (``aistor-licence-read``) allows ``read`` on that one path and nothing else. It never asks the
Keychain for the owner's full token. The owner makes the item once (README.md, AIStor's licence). It gives the text with its edges trimmed, or None when
the server says by its 404 that the path holds nothing (the deploy then skips AIStor with a notice). A computer without that item, or without the
``security`` command, has no licence source: that raises ``NoLicenceSource``, which names the owner's setup step, and the deploy skips AIStor with a
notice. Every other failure is a fixed message that names the failed step, at most a status, and never an answer, a token or a licence."""

from __future__ import annotations

import getpass
import http.client
import json
import re
from urllib.parse import urlsplit

from scripts.process import TimeoutExpired, run_command

LOCAL_ADDRESS = "http://127.0.0.1:8200"
LOCAL_PATH = "secret/data/aistor_licence"
KEYCHAIN_ITEM = "openbao-aistor-licence-token"
ITEM_NOT_FOUND_STATUS = 44
KEYCHAIN_SECONDS = 30
HTTP_SECONDS = 10
LOOPBACK = ("127.0.0.1", "localhost", "::1")
TOKEN_FORM = re.compile(r"[A-Za-z0-9\-._~+/]+=*")
MAX_CHARACTERS = 16384
SETUP_MESSAGE = (
    f"this computer has no Keychain item {KEYCHAIN_ITEM} (or no security command), so the AIStor licence cannot be read from its OpenBao: the owner makes "
    "the token once with the setup step in README.md (AIStor's licence: the policy aistor-licence-read and a periodic token with only that policy)"
)


class LicenceError(RuntimeError):
    """The licence cannot be read. The message is fixed and holds no answer, token or licence."""


class NoLicenceSource(LicenceError):
    """This computer has no Keychain item for the licence token, or no ``security`` command. The deploy skips AIStor; the message names the owner's step."""


def keychain_token() -> str:
    """The token for the licence path, from the login Keychain. It is never printed.

    An item that is not there (``security`` exits with 44) and a computer without the ``security`` command are no licence source. A locked Keychain, a
    timeout, an empty value and a value that is not a token are failures."""
    try:
        result = run_command("security", ["find-generic-password", "-a", getpass.getuser(), "-s", KEYCHAIN_ITEM, "-w"], timeout=KEYCHAIN_SECONDS)
    except FileNotFoundError:
        raise NoLicenceSource(SETUP_MESSAGE) from None
    except TimeoutExpired:
        raise LicenceError("the Keychain did not answer within the deadline when asked for the AIStor licence token") from None
    if result.returncode == ITEM_NOT_FOUND_STATUS:
        raise NoLicenceSource(SETUP_MESSAGE)
    token = result.stdout.strip() if result.returncode == 0 else ""
    if not token or TOKEN_FORM.fullmatch(token) is None:
        raise LicenceError(
            "the Keychain item for the AIStor licence token cannot be read or is not a token, so the licence cannot be read from this computer's OpenBao"
        )
    return token


def usable(value: object) -> str | None:
    """The licence text with its edges trimmed when it has the form of one (ASCII text without a control character but a line break or a tab), else None."""
    if not isinstance(value, str) or not value.strip() or len(value) > MAX_CHARACTERS:
        return None
    if not value.isascii() or any((ord(character) < 32 or ord(character) == 127) and character not in "\n\r\t" for character in value):
        return None
    return value.strip()


def read_licence(address: str = LOCAL_ADDRESS) -> str | None:
    """The licence in this computer's OpenBao, or None when the path holds nothing (a 404).

    No licence source raises ``NoLicenceSource``; any other failure raises ``LicenceError``."""
    parts = urlsplit(address)
    host = parts.hostname or ""
    if parts.scheme != "http" or host not in LOOPBACK:
        raise LicenceError("the address of this computer's OpenBao must be plain HTTP on the loopback interface, so the login never leaves the computer")
    token = keychain_token()
    connection = http.client.HTTPConnection(host, parts.port or 80, timeout=HTTP_SECONDS)
    answer = None
    try:
        connection.request("GET", f"/v1/{LOCAL_PATH}", headers={"X-Vault-Token": token})
        response = connection.getresponse()
        answer = (response.status, response.read().decode(errors="replace"))
    except OSError, http.client.HTTPException:
        answer = None
    finally:
        connection.close()
    if answer is None:
        # Raised outside the handler, so the library's text (which can repeat the request) is not chained to the message.
        raise LicenceError("this computer's OpenBao did not answer when asked for the AIStor licence: start it or store the licence at secret/aistor_licence")
    status, raw = answer
    if status == 404:
        return None
    if status != 200:
        raise LicenceError(f"this computer's OpenBao refused the read of the AIStor licence (HTTP {status})")
    try:
        value = json.loads(raw)["data"]["data"]["value"]
    except ValueError, KeyError, TypeError:
        value = None
    licence = usable(value)
    if licence is None:
        raise LicenceError("this computer's OpenBao holds a value at secret/aistor_licence that is not a licence: it is not text of the form of a licence key")
    return licence
