"""Ansible module: one secret file with a random value, made once, for the numeric user that reads it.

A missing file is created with a random value, owned by ``owner`` (``uid:gid``) and with mode 0400 from the moment it exists. An existing file keeps its
value and is given that owner and mode when it has others. The value is never in the module's output. It uses only the standard library and runs on the host's
Python.
"""

from __future__ import annotations

import secrets
from pathlib import Path

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.secret_files import SecretFileError, access_differs, parse_owner, read_file, set_access, write_private_file

VALUE_BYTES = 32


def main() -> None:
    module = AnsibleModule(argument_spec={"path": {"type": "path", "required": True}, "owner": {"type": "str", "required": True}}, supports_check_mode=True)
    path = Path(module.params["path"])
    try:
        owner = parse_owner(module.params["owner"], allow_root=True)
        if not path.exists():
            if not module.check_mode:
                write_private_file(path, secrets.token_urlsafe(VALUE_BYTES), replace=False, owner=owner)
            module.exit_json(changed=True, created=True, protected=False)
        read_file(path, f"secret file {path.name}")
        protected = access_differs(path, owner)
        if protected and not module.check_mode:
            set_access(path, owner)
        module.exit_json(changed=protected, created=False, protected=protected)
    except SecretFileError as error:
        module.fail_json(msg=str(error))


if __name__ == "__main__":
    main()
