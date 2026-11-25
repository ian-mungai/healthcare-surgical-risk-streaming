"""The list of external secrets and the named form checks. It is a module of the role ``secret_files``, beside ``secret_files.py``.

An external secret is one that a third party issues (the first is the host's read-only Docker Hub token). ``compose/external_secrets.txt`` declares each
by name, consumer, OpenBao path and form check, and never its value. Three programs read the same file: the launcher on the owner's computer (before it
sends a value), the intake script on the host (before it writes a value) and the OpenBao module (which gives each consumer's policy its own paths and the
intake role's policy all of them). It uses only the standard library.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

NAME_FORM = re.compile(r"[a-z][a-z0-9_]{0,30}")
PATH_PREFIX = "secret/data/external/"
# The AppRole of the intake script that runs on the host: its policy writes the declared paths, and it is written at every deploy from the list, so a host whose
# OpenBao was made before the list existed gets it too.
INTAKE_ROLE = "external_intake"
# The closed set of named checks. A value is one line: no white space and no character outside the set.
FORM_CHECKS = {"dockerhub_token": re.compile(r"dckr_pat_[A-Za-z0-9_-]{20,100}")}


class ExternalSecretError(ValueError):
    """The list or a check name is not usable. The message never holds a secret value."""


@dataclass(frozen=True)
class ExternalSecret:
    name: str
    consumer: str
    path: str
    check: str


def parse_external(text: str) -> list[ExternalSecret]:
    """The declarations of the list: one for each line of four words (name, consumer, path, check), with comments and blank lines ignored."""
    found: list[ExternalSecret] = []
    for line in text.splitlines():
        words = line.split("#", 1)[0].split()
        if not words:
            continue
        if len(words) != 4:
            raise ExternalSecretError(f"the line of {words[0]!r} must hold four words: name, consumer, path and check")
        name, consumer, path, check = words
        if not NAME_FORM.fullmatch(name):
            raise ExternalSecretError(f"external secret name {name!r} must be 1 to 31 lower case letters, digits or underscores and start with a letter")
        if not NAME_FORM.fullmatch(consumer):
            raise ExternalSecretError(f"the consumer of {name!r} is not a service name")
        if path != f"{PATH_PREFIX}{name}":
            raise ExternalSecretError(f"the path of {name!r} must be {PATH_PREFIX}{name}")
        if check not in FORM_CHECKS:
            raise ExternalSecretError(f"the check of {name!r} is not one of the named checks: {', '.join(sorted(FORM_CHECKS))}")
        if name in [item.name for item in found]:
            raise ExternalSecretError(f"external secret {name!r} is listed more than once")
        found.append(ExternalSecret(name, consumer, path, check))
    if not found:
        raise ExternalSecretError("the list names no external secret")
    return found


def form_ok(check: str, value: str) -> bool:
    """Whether ``value`` has the form that the named check asks for."""
    if check not in FORM_CHECKS:
        raise ExternalSecretError(f"{check!r} is not a named check")
    return FORM_CHECKS[check].fullmatch(value) is not None


def value_digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()
