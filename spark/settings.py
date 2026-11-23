"""Settings of the Spark jobs: the memory taken from the container's limit and the Nessie catalog opened on the warehouse location.

The container's memory limit is set once, in ``compose/limits.yml``. A job reads the limit that its cgroup enforces and sets its driver and
executor memory from it, so no memory figure is typed in a job. The catalog is Iceberg's built-in Nessie catalog. The warehouse location decides
the file system: ``s3://`` opens Iceberg's S3 file system and ``file:`` its Hadoop one, and nothing else in the catalog changes.
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlsplit

CATALOG = "lake"
CGROUP_V2_LIMIT = Path("/sys/fs/cgroup/memory.max")
CGROUP_V1_LIMIT = Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")
BYTES_PER_MIB = 1024 * 1024
UNLIMITED_BYTES = 2**62
OVERHEAD_FLOOR_MIB = 384
MINIMUM_HEAP_MIB = 512
FILE_IO = {"s3": "org.apache.iceberg.aws.s3.S3FileIO", "file": "org.apache.iceberg.hadoop.HadoopFileIO"}


class SettingsError(ValueError):
    """A setting is missing or cannot be used, and the message names it and never holds a secret."""


def driver_memory_mib(limit_mib: int) -> int:
    """The JVM heap for a container limit: the heap plus an overhead of 40% of the heap, at least 384 MiB, never exceeds the limit.

    The overhead is Spark's own rule for a PySpark application. It pays for the JVM's non-heap memory and the Python driver.
    """
    heap = min(limit_mib - OVERHEAD_FLOOR_MIB, limit_mib * 10 // 14)
    if heap < MINIMUM_HEAP_MIB:
        raise SettingsError(
            f"the container's memory limit of {limit_mib} MiB leaves a heap under {MINIMUM_HEAP_MIB} MiB: raise its limit in compose/limits.yml"
        )
    return heap


def read_memory_limit_mib(path: Path | None = None) -> int:
    """The memory limit of this container in whole MiB. A container with no limit is refused."""
    paths = (path,) if path is not None else (CGROUP_V2_LIMIT, CGROUP_V1_LIMIT)
    for candidate in paths:
        try:
            text = candidate.read_text().strip()
        except OSError:
            continue
        if not text.isascii() or not text.isdigit() or int(text) >= UNLIMITED_BYTES:
            raise SettingsError(f"the container has no memory limit ({candidate.name} holds {text[:24]!r}): a job refuses to run without one")
        return int(text) // BYTES_PER_MIB
    raise SettingsError(f"cannot read the container's memory limit from {paths[0].name}: the job refuses to run without one")


def catalog_options(*, nessie_uri: str, warehouse: str, ref: str, region: str, catalog: str = CATALOG) -> dict[str, str]:
    """The Spark options that open the catalog ``catalog`` (``lake`` unless named) on the Nessie reference ``ref``, or ``SettingsError`` naming one."""
    parts = urlsplit(nessie_uri)
    if parts.scheme != "http" or not parts.hostname or parts.path != "/api/v2" or parts.query or parts.fragment:
        raise SettingsError("the Nessie address must be a plain http URL ending in /api/v2, such as http://nessie:19120/api/v2")
    scheme = urlsplit(warehouse).scheme
    if scheme not in FILE_IO or not urlsplit(warehouse).path.strip("/") or (scheme == "s3" and not urlsplit(warehouse).netloc):
        raise SettingsError("the warehouse location must be s3://<bucket>/<prefix>/ or file:///<folder>/")
    if not ref:
        raise SettingsError("the Nessie reference is empty: a job names the branch it writes to")
    if scheme == "s3" and not region:
        raise SettingsError("the AWS region is empty: an s3 warehouse needs it")
    prefix = f"spark.sql.catalog.{catalog}"
    options = {
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
        f"{prefix}.uri": nessie_uri,
        f"{prefix}.client-api-version": "2",
        f"{prefix}.ref": ref,
        f"{prefix}.warehouse": warehouse,
        f"{prefix}.io-impl": FILE_IO[scheme],
    }
    if scheme == "s3":
        options[f"{prefix}.client.region"] = region
    return options
