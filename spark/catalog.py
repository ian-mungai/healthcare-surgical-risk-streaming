"""Opening Nessie catalogs from the environment of a job: the settings that the Compose service sets, and a catalog added to a running session.

``NESSIE_URI``, ``WAREHOUSE_LOCATION`` and, for an s3 warehouse, ``AWS_REGION`` come from the environment, so a job never types an address or a bucket.
This module imports nothing from Spark, so the settings are proved without a Spark install.
"""

from __future__ import annotations

import os
from typing import Any

from spark.settings import CATALOG, SettingsError, catalog_options


def required_environment(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise SettingsError(f"the environment variable {name} is not set")
    return value


def environment_catalog_options(ref: str, catalog: str = CATALOG, ref_hash: str | None = None) -> dict[str, str]:
    """The options of the catalog ``catalog`` on the Nessie reference ``ref`` (as of ``ref_hash`` when given), from the environment of the job."""
    return catalog_options(
        nessie_uri=required_environment("NESSIE_URI"),
        warehouse=required_environment("WAREHOUSE_LOCATION"),
        ref=ref,
        region=os.environ.get("AWS_REGION", ""),
        catalog=catalog,
        ref_hash=ref_hash,
    )


def add_catalog(session: Any, catalog: str, ref: str) -> None:
    """Open one more catalog, named ``catalog``, on the Nessie reference ``ref`` in a running session, to read a tag beside the branch that the job works on.

    The catalog has no table cache, so every read loads the table's metadata from storage again.
    A cached table would answer a read after its metadata file is gone.
    """
    for key, value in environment_catalog_options(ref, catalog).items():
        session.conf.set(key, value)
    session.conf.set(f"spark.sql.catalog.{catalog}.cache-enabled", "false")
