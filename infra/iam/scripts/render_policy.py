"""Render the per-service IAM policy templates in ``infra/iam/policies/`` with the values of ``.env``.

A template holds ``${AWS_REGION}``, ``${AWS_ACCOUNT_ID}``, ``${PROJECT_NAME}``, ``${ENVIRONMENT}``,
``${S3_BUCKET}`` and ``${TAILSCALE_AUDIENCE}``. IAM policy variables such as ``${aws:username}`` stay. An unknown placeholder, a missing value, a
document that is not a policy and a policy over the managed policy size limit stop the render.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

POLICIES_DIRECTORY = Path(__file__).resolve().parents[1] / "policies"
BOUNDARY_DIRECTORY = Path(__file__).resolve().parents[1] / "boundary"
PLACEHOLDER = re.compile(r"\$\{([A-Z][A-Z0-9_]*)\}")
PLACEHOLDER_NAMES = ("AWS_REGION", "AWS_ACCOUNT_ID", "PROJECT_NAME", "ENVIRONMENT", "S3_BUCKET", "TAILSCALE_AUDIENCE")
TEMPLATE_PREFIX = "healthcare_surgical_risk_streaming_"
MAX_POLICY_CHARACTERS = 6144
READ_ACTION = re.compile(r"[a-z0-9-]+:(?:Describe|List|Get)[A-Za-z]*")


class PolicyRenderError(ValueError):
    """A template holds an unknown placeholder or a missing value, is not a policy document or is over the size limit."""


def render_template(text: str, values: Mapping[str, str]) -> dict[str, Any]:
    """Replace every upper-case placeholder in the template text and return the parsed policy document."""
    found = set(PLACEHOLDER.findall(text))
    unknown = sorted(found - set(PLACEHOLDER_NAMES))
    if unknown:
        raise PolicyRenderError(f"unknown placeholders: {', '.join(unknown)}")
    missing = sorted(name for name in found if not values.get(name))
    if missing:
        raise PolicyRenderError(f"missing values for: {', '.join(missing)}")
    document = json.loads(PLACEHOLDER.sub(lambda match: values[match.group(1)], text))
    if not isinstance(document, dict) or not isinstance(document.get("Statement"), list):
        raise PolicyRenderError("a policy document must be a JSON object with a Statement list")
    size = len(re.sub(r"\s", "", json.dumps(document, separators=(",", ":"))))
    if size > MAX_POLICY_CHARACTERS:
        raise PolicyRenderError(f"the rendered policy has {size} characters; IAM allows {MAX_POLICY_CHARACTERS}")
    return document


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else [value]


def scope_problems(document: Mapping[str, Any]) -> list[str]:
    """List each statement with no Sid and each action other than a read that is allowed on every resource with no condition."""
    problems = []
    for number, statement in enumerate(document["Statement"], 1):
        sid = statement.get("Sid")
        if not sid:
            problems.append(f"statement {number}: no Sid")
            continue
        if statement.get("Effect") != "Allow" or "*" not in as_list(statement.get("Resource")) or statement.get("Condition"):
            continue
        problems += [
            f"{sid}: {action} is allowed on every resource with no condition"
            for action in as_list(statement.get("Action"))
            if not READ_ACTION.fullmatch(action)
        ]
    return problems


def load_documents(values: Mapping[str, str], directory: Path = POLICIES_DIRECTORY, *, suffix: str = "_policy") -> dict[str, dict[str, Any]]:
    """Render every template in the directory whose name ends with the suffix, keyed by the name between the project prefix and the suffix."""
    documents = {}
    for path in sorted(directory.glob(f"{TEMPLATE_PREFIX}*{suffix}.json")):
        try:
            documents[path.stem.removeprefix(TEMPLATE_PREFIX).removesuffix(suffix)] = render_template(path.read_text(), values)
        except PolicyRenderError as error:
            raise PolicyRenderError(f"{path.name}: {error}") from None
    if not documents:
        raise PolicyRenderError(f"no policy templates in {directory.name}")
    return documents
