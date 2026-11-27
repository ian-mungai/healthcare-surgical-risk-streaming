"""Read and validate the project configuration from the ignored ``.env`` file.

``.env.example`` lists every key. A missing or malformed value stops the caller with the key's name and never its value.
"""

from __future__ import annotations

import re
from pathlib import Path

from scripts.ingestion.notice import FixedTextError

ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT / ".env"
ENVIRONMENTS = ("dev", "prod")
PATTERNS = {
    "AWS_REGION": re.compile(r"[a-z]{2}(?:-[a-z]+)+-\d"),
    "AWS_PROFILE": re.compile(r"[A-Za-z0-9_-]+"),
    "AWS_ACCOUNT_ID": re.compile(r"\d{12}"),
    "PROJECT_NAME": re.compile(r"[a-z][a-z0-9_]*"),
    "ENVIRONMENT": re.compile("|".join(ENVIRONMENTS)),
    "S3_BUCKET": re.compile(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]"),
}


# One Docker Hub ``<namespace>/<repository>``. The namespace is an account name: lowercase letters and digits, which holds no dot, so no registry host (and not
# ``localhost``). The repository is one path component of Docker's reference grammar: lowercase letters and digits, joined by ``.``, ``_``, ``__`` or ``-``.
# A tag, a digest and a second slash are not part of it.
IMAGE_ACCOUNT_PATTERN = r"(?!localhost/)[a-z0-9]+"
IMAGE_REPOSITORY_PATTERN = IMAGE_ACCOUNT_PATTERN + r"/[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*"
OPTIONAL_PATTERNS = {
    "IMAGE_REPOSITORY": re.compile(IMAGE_REPOSITORY_PATTERN),
    "TAILSCALE_CLIENT_ID": re.compile(r"(?!tskey)\S+"),
    "TAILSCALE_AUDIENCE": re.compile(r"\S+"),
    "EVIDENCE_VERIFIER_KEY": re.compile(r"\S+"),
    "EXTERNAL_SECRETS_SOURCE": re.compile(r"openbao|prompt"),
}


class ConfigurationError(ValueError, FixedTextError):
    """The configuration file is missing or one of its values is absent or malformed. Its text names the file ``.env`` and the settings by their names, and
    never a path or a value."""


def load_configuration(path: Path = ENV_FILE) -> dict[str, str]:
    """Return the validated configuration."""
    if not path.is_file():
        raise ConfigurationError(f"{ENV_FILE.name} is missing; copy .env.example to .env and fill it in")
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        key, separator, value = line.partition("=")
        if separator and not line.lstrip().startswith("#"):
            values[key.strip()] = value.strip().strip("\"'")
    problems = sorted(key for key, pattern in PATTERNS.items() if pattern.fullmatch(values.get(key, "")) is None)
    problems += sorted(key for key, pattern in OPTIONAL_PATTERNS.items() if values.get(key) and pattern.fullmatch(values[key]) is None)
    if problems:
        raise ConfigurationError(f"{ENV_FILE.name}: missing or malformed values for: {', '.join(problems)}")
    configuration = {key: values[key] for key in PATTERNS} | {key: values[key] for key in OPTIONAL_PATTERNS if values.get(key)}
    return configuration
