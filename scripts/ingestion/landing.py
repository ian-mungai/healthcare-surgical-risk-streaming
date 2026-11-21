"""Where a source key lands in the project's bucket: the one rule that pairs the two.

The rule below is the rule of the tests: the landing prefix, then the source key unchanged. It is a stand-in. Replace it, together with
``LANDING_PREFIX``, when Airbyte's naming of the objects it writes is verified. Nothing else names a landed key: the check reads it from this
function, and the manifest and the gate use the key the manifest holds.
"""

from __future__ import annotations

LANDING_PREFIX = "landing/"


def landed_key(source_key: str) -> str:
    return LANDING_PREFIX + source_key
