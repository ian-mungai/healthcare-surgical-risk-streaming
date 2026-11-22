"""Ansible module: place the project profile's access key for Airbyte's two connectors as two root-only files on the host.

The deployment reads the key on the owner's computer with the AWS command line (``aws configure export-credentials --format process``) and hands the
command's answer to this module as one hidden argument. The module reads the answer in memory and refuses a profile that yields a session token, because the
connectors take an IAM user's own key. It writes ``airbyte/aws_access_key_id`` and ``airbyte/aws_secret_access_key`` under the secrets folder, owned like
every secret file that no container reads, with mode 0400. Each file is written through a fresh temporary file that is flushed and synced before it takes its
place, so a file is whole or absent and never empty. The leftovers of an interrupted write are removed at the next run, an empty or short file is replaced like
a changed one (a rotated key), a file with another mode or owner is given the right ones, and a second run changes nothing.

The module never fails with text that could hold a value. It always returns ``reason``, one of a fixed set of codes (``REASONS``), and the playbook prints the
message that belongs to the code. It uses only the standard library and runs on the host's Python.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.secret_files import SecretFileError, access_differs, parse_owner, remove_leftovers, set_access, write_private_file

FOLDER = "airbyte"
ID_FILE = "aws_access_key_id"
VALUE_FILE = "aws_secret_access_key"
KEY_ID_FORM = re.compile(r"[A-Z0-9]{16,128}")
KEY_VALUE_FORM = re.compile(r"[A-Za-z0-9/+=]{20,128}")
REASONS = ("ok", "session_token", "not_a_static_key", "empty", "export_failed", "cannot_write")


class KeyFormError(RuntimeError):
    """The key cannot be used. The error holds a reason code and no other text, so nothing of an answer can be in it."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def parse_export(text: str, exit_code: int) -> tuple[str, str]:
    """The access key ID and the secret from the answer of the AWS command line. Raise ``KeyFormError`` with a code for anything that is not a static key."""
    if exit_code != 0:
        raise KeyFormError("export_failed")
    if not text.strip():
        raise KeyFormError("empty")
    try:
        document = json.loads(text)
    except ValueError:
        document = None
    if not isinstance(document, dict):
        raise KeyFormError("not_a_static_key")
    if "SessionToken" in document:
        raise KeyFormError("session_token")
    key_id, value = document.get("AccessKeyId"), document.get("SecretAccessKey")
    if not isinstance(key_id, str) or not isinstance(value, str):
        raise KeyFormError("not_a_static_key")
    if not key_id or not value:
        raise KeyFormError("empty")
    if KEY_ID_FORM.fullmatch(key_id) is None or KEY_VALUE_FORM.fullmatch(value) is None:
        raise KeyFormError("not_a_static_key")
    return key_id, value


def current_value(path: Path) -> str | None:
    """What the file holds, or ``None`` when it is absent. An empty file holds the empty text, which no key equals."""
    try:
        return path.read_text().rstrip("\n")
    except FileNotFoundError:
        return None
    except (OSError, UnicodeDecodeError):
        raise KeyFormError("cannot_write") from None


def place_key(directory: Path, owner: tuple[int, int], key_id: str, value: str, *, check_only: bool = False) -> list[str]:
    """Make the two files hold the key and return the names of the files that changed. The secret goes first, so the ID changes last."""
    changed: list[str] = []
    for name, wanted in ((VALUE_FILE, value), (ID_FILE, key_id)):
        path = directory / FOLDER / name
        try:
            if not check_only and path.parent.exists():
                remove_leftovers(path)
            held = current_value(path)
            if held != wanted:
                if not check_only:
                    write_private_file(path, wanted, replace=True, owner=owner)
            elif access_differs(path, owner):
                if not check_only:
                    set_access(path, owner)
            else:
                continue
        except (SecretFileError, OSError):
            raise KeyFormError("cannot_write") from None
        changed.append(name)
    return changed


def main() -> None:
    module = AnsibleModule(
        argument_spec={
            "exported": {"type": "str", "required": True, "no_log": True},
            "exit_code": {"type": "int", "required": True},
            "secrets_directory": {"type": "path", "required": True},
            "root_owner": {"type": "str", "default": "0:0"},
        },
        supports_check_mode=True,
    )
    params = module.params
    try:
        key_id, value = parse_export(params["exported"], params["exit_code"])
        changed = place_key(Path(params["secrets_directory"]), parse_owner(params["root_owner"], allow_root=True), key_id, value, check_only=module.check_mode)
    except KeyFormError as error:
        module.exit_json(changed=False, reason=error.code, files=[])
    except SecretFileError:
        module.exit_json(changed=False, reason="cannot_write", files=[])
    module.exit_json(changed=bool(changed), reason="ok", files=changed)


if __name__ == "__main__":
    main()
