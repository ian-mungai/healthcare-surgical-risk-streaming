"""Parse the stack's YAML files, refusing a key that a mapping repeats.

PyYAML is pinned in ``requirements.txt`` as a dependency of ansible-core. A repeated key would silently keep only its last value, which in the
limits file would be a limit that is typed twice, so it is refused with its line number.
"""

from __future__ import annotations

import importlib
from typing import Any


class YamlError(ValueError):
    """The text is not valid YAML or a mapping repeats a key."""


def refuse_repeated_keys(node: Any) -> None:
    value = node.value
    if isinstance(value, str):
        return
    seen: set[str] = set()
    for item in value:
        if isinstance(item, tuple):
            key, child = item
            if node.tag == "tag:yaml.org,2002:map":
                if key.value in seen:
                    raise YamlError(f"repeated key {key.value!r} at line {key.start_mark.line + 1}")
                seen.add(key.value)
            refuse_repeated_keys(key)
            refuse_repeated_keys(child)
        else:
            refuse_repeated_keys(item)


def load_yaml(text: str) -> object:
    """The parsed document, or ``YamlError`` for text that is not YAML or repeats a mapping key."""
    yaml = importlib.import_module("yaml")
    try:
        node = yaml.compose(text)
        if node is not None:
            refuse_repeated_keys(node)
        return yaml.safe_load(text)
    except yaml.YAMLError as error:
        raise YamlError(str(error).replace("\n", " ")) from error
