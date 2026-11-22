"""What a table must be before maintenance touches it: a table that Nessie made, which Iceberg cannot delete files from.

Two table properties decide whether Iceberg could remove a file that a tag needs. The catalog sets the first to false when it makes a table, and
Iceberg's deleting procedures refuse a table where it is false. The second, when true, makes every commit delete the oldest metadata file, which a tag
can still point to. Maintenance only reads the two properties, and refuses a table where the first is not false or the second is true, before it
rewrites anything. It never sets either.
"""

from __future__ import annotations

from collections.abc import Mapping

PROPERTY_KEYS = ("gc.enabled", "write.metadata.delete-after-commit.enabled")


def precondition_problems(table: str, properties: Mapping[str, str | None]) -> list[str]:
    """One message for each property of ``table`` that stops maintenance, naming the table. An empty list means the table may be compacted."""
    found = []
    first, second = PROPERTY_KEYS
    if (properties.get(first) or "").strip().lower() != "false":
        found.append(f"{table}: {first} is {properties.get(first)!r}, not 'false', so Iceberg could delete files that a tag needs: nothing was rewritten")
    if (properties.get(second) or "").strip().lower() == "true":
        found.append(f"{table}: {second} is true, so every commit deletes the oldest metadata file, which a tag may point to: nothing was rewritten")
    return found
