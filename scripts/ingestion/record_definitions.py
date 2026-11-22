"""The record connections as data: what Airbyte reads, where it writes and how each connection runs.

Three connections load three tables into Iceberg on the Nessie catalog, in the namespace ``raw_records``: ``clinical_data`` and ``lab_data`` from the
publisher's two CSV tables, and ``track_list`` from the publisher's track list. A table's name is its stream name and so its Iceberg table name.

- The two CSV tables are read by Airbyte's S3 source from the run's own read prefix of the project's bucket, ``records_input/<run ID>/``, where the Dagster step
  copied exactly the object versions that the passed manifest binds. The source cannot name an object version, so a copy of the bound version under a key that
  holds one version is how it reads only those. Every column is a string (``inference_type`` None) and an empty cell stays an empty string, so an identifier
  keeps its published form.
- The track list is read by Airbyte's File source from the publisher's address, with every column a string.
- The destination is the S3 Data Lake with the Nessie catalog. Its Main Branch Name is the run's load branch ``load_<run ID>``, so a sync commits to that branch
  and never to ``main``. Column names stay as the CSV header has them. The project's Nessie has no sign-in, so the destination has no access token.
- Each connection has no schedule and syncs as a full refresh that overwrites, so only the orchestrator starts it and a second sync of a run leaves one
  state of the table.

No document here holds a secret. The destination connector requires an AWS access key ID and secret access key for the Nessie catalog; the caller adds them
from files when it applies the document (``record_connections.py``), and they are never a value here.

Connector versions are pinned by full version. The sources and the date are in ``PINNED_ON`` and the comments of each pin.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from scripts.ingestion.landing import LANDING_PREFIX
from scripts.ingestion.selection import pinned_release
from scripts.ingestion.track_list import pinned_track_list

RUN_ID = re.compile(r"[a-z0-9][a-z0-9_]{0,63}")
BRANCH = re.compile(r"load_[a-z0-9][a-z0-9_]{0,63}")
BUCKET_NAME = re.compile(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]")
REGION_NAME = re.compile(r"[a-z]{2}(?:-[a-z]+)+-[0-9]")
WAREHOUSE_PATH = re.compile(r"[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*")
NAMESPACE = "raw_records"
READ_PREFIX = "records_input/"
MAIN_BRANCH = "main"
# Read on Nov 22 2026 (PST) from each connector's metadata.yaml on the master branch of github.com/airbytehq/airbyte and from the S3 Data Lake page of
# docs.airbyte.com/integrations/destinations/s3-data-lake: source-s3 4.17.2, source-file 0.6.2, destination-s3-data-lake 0.4.2.
PINNED_ON = "2026-11-22"


class DefinitionError(ValueError):
    """An argument of a definition is not valid: a run ID, a branch, a bucket, a region or a path."""


@dataclass(frozen=True, slots=True)
class Connector:
    repository: str
    tag: str

    @property
    def image(self) -> str:
        return f"{self.repository}:{self.tag}"


SOURCE_S3 = Connector("airbyte/source-s3", "4.17.2")
SOURCE_FILE = Connector("airbyte/source-file", "0.6.2")
DESTINATION = Connector("airbyte/destination-s3-data-lake", "0.4.2")


@dataclass(frozen=True, slots=True)
class Table:
    """A table and where its rows come from: the source key of a CSV table in the release, or none for the track list, which has its own address."""

    name: str
    source_key: str | None


@dataclass(frozen=True, slots=True)
class Environment:
    """The settings of the host that the definitions need. ``nessie_uri`` is the Nessie address as the Airbyte pods reach it."""

    bucket: str
    region: str
    warehouse_path: str
    nessie_uri: str


def table_key(file_name: str) -> str:
    return f"{pinned_release().source_prefix}{file_name}"


CSV_TABLES = (Table("clinical_data", table_key("clinical_data.csv")), Table("lab_data", table_key("lab_data.csv")))
TRACK_LIST = Table("track_list", None)
TABLES = (*CSV_TABLES, TRACK_LIST)
RECORD_SOURCE_KEYS = tuple(table.source_key for table in CSV_TABLES if table.source_key)


def check_run(run_id: str) -> str:
    if RUN_ID.fullmatch(run_id) is None:
        raise DefinitionError("the run ID must be lower-case letters, digits and underscores, from 1 to 64 characters")
    return run_id


def read_prefix(run_id: str) -> str:
    return f"{READ_PREFIX}{check_run(run_id)}/"


def read_key(run_id: str, table: Table) -> str:
    """The key of the run's copy of a CSV table, which is where the source reads it."""
    return f"{read_prefix(run_id)}{table.name}.csv"


def branch_name(run_id: str) -> str:
    return f"load_{check_run(run_id)}"


def check_environment(environment: Environment) -> None:
    if BUCKET_NAME.fullmatch(environment.bucket) is None or environment.bucket == pinned_release().source_bucket:
        raise DefinitionError("the bucket is not a valid bucket name of the project, or it is the publisher's bucket")
    if REGION_NAME.fullmatch(environment.region) is None:
        raise DefinitionError("the region is not an AWS region name")
    if WAREHOUSE_PATH.fullmatch(environment.warehouse_path) is None or environment.warehouse_path.split("/")[0] in (
        LANDING_PREFIX.strip("/"),
        READ_PREFIX.strip("/"),
    ):
        raise DefinitionError("the warehouse path must be a relative path outside the landing and read prefixes")
    if not environment.nessie_uri.startswith("http://"):
        raise DefinitionError("the Nessie address must be a plain http URL")


def source_definition(table: Table) -> Connector:
    return SOURCE_FILE if table.source_key is None else SOURCE_S3


def source_name(table: Table) -> str:
    return f"vitaldb_{table.name}_source"


def source_configuration(table: Table, run_id: str, environment: Environment) -> dict[str, Any]:
    """The configuration of the table's source for this run."""
    check_run(run_id)
    if table.source_key is None:
        return {
            "dataset_name": table.name,
            "format": "csv",
            "url": pinned_track_list().url,
            "provider": {"storage": "HTTPS"},
            "reader_options": json.dumps({"dtype": "str", "keep_default_na": False, "na_filter": False}, sort_keys=True),
        }
    check_environment(environment)
    return {
        "bucket": environment.bucket,
        "region_name": environment.region,
        "credentials": {"auth_type": "default_credentials"},
        "delivery_method": {"delivery_type": "use_records_transfer"},
        "streams": [
            {
                "name": table.name,
                "globs": [read_key(run_id, table)],
                "schemaless": False,
                "format": {
                    "filetype": "csv",
                    "header_definition": {"header_definition_type": "From CSV"},
                    "inference_type": "None",
                    "strings_can_be_null": False,
                },
            }
        ],
    }


DESTINATION_NAME = "vitaldb_records_destination"


def destination_configuration(environment: Environment, branch: str) -> dict[str, Any]:
    """The destination without its secrets, writing to ``branch``. Raise ``DefinitionError`` for ``main`` or any name that is not a load branch."""
    check_environment(environment)
    if BRANCH.fullmatch(branch) is None:
        raise DefinitionError("the destination branch must be a load branch, load_ and the run ID")
    return {
        "s3_bucket_name": environment.bucket,
        "s3_bucket_region": environment.region,
        "warehouse_location": f"s3://{environment.bucket}/{environment.warehouse_path}",
        "main_branch_name": branch,
        "catalog_type": {"catalog_type": "NESSIE", "server_uri": environment.nessie_uri, "namespace": NAMESPACE},
        "normalize_column_names": False,
    }


def connection_name(table: Table) -> str:
    return f"vitaldb_{table.name}_records"


def connection_configuration(table: Table) -> dict[str, Any]:
    """The connection of one table: no schedule, a full refresh that overwrites, in the raw namespace."""
    return {
        "name": connection_name(table),
        "status": "active",
        "scheduleType": "manual",
        "namespaceDefinition": "customformat",
        "namespaceFormat": NAMESPACE,
        "streams": [{"stream": table.name, "syncMode": "full_refresh", "destinationSyncMode": "overwrite"}],
    }
