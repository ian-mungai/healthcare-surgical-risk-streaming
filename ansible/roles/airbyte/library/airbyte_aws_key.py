"""Ansible module: place the project profile's access key for Airbyte's two connectors as two root-only files on the host.

The deployment reads the key on the owner's computer with the AWS command line (``aws configure export-credentials --format process``) and hands the
command's answer to this module as one hidden argument. The module reads the answer in memory and refuses a profile that yields a session token, because the
connectors take an IAM user's own key. It writes ``airbyte/aws_access_key_id`` and ``airbyte/aws_secret_access_key`` under the secrets folder, owned like
every secret file that no container reads, with mode 0400 from the moment each exists. A file that holds another value is replaced atomically (a rotated
key), a file with another mode or owner is given the right ones, and a second run changes nothing. No value is in the module's output, its messages or its
logs. It uses only the standard library and runs on the host's Python.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.secret_files import SecretFileError, access_differs, parse_owner, read_file, set_access, write_private_file

FOLDER = "airbyte"
ID_FILE = "aws_access_key_id"
VALUE_FILE = "aws_secret_access_key"
KEY_ID_FORM = re.compile(r"[A-Z0-9]{16,128}")
KEY_VALUE_FORM = re.compile(r"[A-Za-z0-9/+=]{20,128}")


class KeyFormError(RuntimeError):
    """The key cannot be used. The message names a cause and the profile and never a value."""


def parse_export(text: str, exit_code: int, profile: str) -> tuple[str, str]:
    """The access key ID and the secret from the answer of the AWS command line. Raise ``KeyFormError`` for anything that is not a static key."""
    if exit_code != 0:
        raise KeyFormError(
            f"the AWS command line could not export the credentials of the profile {profile} (exit status {exit_code}); run it by hand to see why"
        )
    try:
        document = json.loads(text)
    except ValueError:
        document = None
    if not isinstance(document, dict) or not isinstance(document.get("AccessKeyId"), str) or not isinstance(document.get("SecretAccessKey"), str):
        raise KeyFormError(f"the answer for the profile {profile} is not the process credential form with an AccessKeyId and a SecretAccessKey")
    if "SessionToken" in document:
        raise KeyFormError(
            f"the profile {profile} yields a session token; Airbyte's connectors need the own access key of an IAM user, not a temporary credential"
        )
    key_id, value = document["AccessKeyId"], document["SecretAccessKey"]
    if KEY_ID_FORM.fullmatch(key_id) is None or KEY_VALUE_FORM.fullmatch(value) is None:
        raise KeyFormError(f"the access key of the profile {profile} is not in the form of an AWS access key")
    return key_id, value


def place_key(directory: Path, owner: tuple[int, int], key_id: str, value: str, *, check_only: bool = False) -> list[str]:
    """Make the two files hold the key and return the names of the files that changed. The secret goes first, so the ID changes last."""
    changed: list[str] = []
    for name, wanted in ((VALUE_FILE, value), (ID_FILE, key_id)):
        path = directory / FOLDER / name
        try:
            if not path.exists():
                if not check_only:
                    write_private_file(path, wanted, replace=False, owner=owner)
            elif read_file(path, f"key file {name}") != wanted:
                if not check_only:
                    write_private_file(path, wanted, replace=True, owner=owner)
            elif access_differs(path, owner):
                if not check_only:
                    set_access(path, owner)
            else:
                continue
        except SecretFileError as error:
            raise KeyFormError(str(error)) from None
        changed.append(name)
    return changed


def main() -> None:
    module = AnsibleModule(
        argument_spec={
            "exported": {"type": "str", "required": True, "no_log": True},
            "exit_code": {"type": "int", "required": True},
            "profile": {"type": "str", "required": True},
            "secrets_directory": {"type": "path", "required": True},
            "root_owner": {"type": "str", "default": "0:0"},
        },
        supports_check_mode=True,
    )
    params = module.params
    try:
        key_id, value = parse_export(params["exported"], params["exit_code"], params["profile"])
        changed = place_key(Path(params["secrets_directory"]), parse_owner(params["root_owner"], allow_root=True), key_id, value, check_only=module.check_mode)
        module.exit_json(changed=bool(changed), files=changed)
    except (KeyFormError, SecretFileError) as error:
        module.fail_json(msg=str(error))


if __name__ == "__main__":
    main()
