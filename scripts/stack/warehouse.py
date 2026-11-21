"""The Iceberg warehouse location: the ``warehouse`` prefix of the project's one S3 bucket.

The bucket's name comes from the project configuration (``S3_BUCKET`` in the ignored ``.env`` file), so no bucket name is typed in a tracked file.
Spark and Trino read the location from here when they open the Nessie catalog.

``.venv/bin/python -m scripts.stack.warehouse`` prints the location.
"""

from __future__ import annotations

import sys

from scripts.project_config import PATTERNS, ConfigurationError, load_configuration

WAREHOUSE_PREFIX = "warehouse"
BUCKET_NAME = PATTERNS["S3_BUCKET"]


class WarehouseError(ValueError):
    """The bucket name is not one that S3 accepts."""


def warehouse_location(bucket: str) -> str:
    """The warehouse location in ``bucket``, ending in a slash, or ``WarehouseError`` when ``bucket`` is not a valid bucket name."""
    if BUCKET_NAME.fullmatch(bucket) is None:
        raise WarehouseError(
            "the bucket name is not a valid S3 bucket name: 3 to 63 lower case letters, digits and dashes, starting and ending with a letter or digit"
        )
    return f"s3://{bucket}/{WAREHOUSE_PREFIX}/"


def configured_warehouse_location() -> str:
    """The warehouse location of the project's bucket, from the project configuration."""
    return warehouse_location(load_configuration()["S3_BUCKET"])


def main() -> int:
    try:
        sys.stdout.write(configured_warehouse_location() + "\n")
    except (ConfigurationError, WarehouseError) as error:
        sys.stderr.write(f"warehouse: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
