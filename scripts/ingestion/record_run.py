"""Run a record load from the settings of the code location container.

The container sets the settings below and mounts the six files. A missing or blank setting stops the load before any call and is named. A file that is
missing or empty is named by its setting, never by its path, and no message holds a secret value.

Settings that the host settings file already carries: ``WAREHOUSE_LOCATION`` (``s3://<bucket>/<path>``: the project's bucket and the warehouse path) and
``AWS_REGION``. ``NESSIE_URI`` is Nessie as this container reaches it. Others, all with defaults: ``RECORD_AWS_PROFILE`` (``default``, the profile whose
``credential_process`` gives this workload its short-lived access), ``RECORD_LOAD_DIR`` (``/opt/dagster/runs/record_loads``), ``RECORD_SYNC_DEADLINE_SECONDS``
(3600), ``RECORD_POLL_SECONDS`` (10) and ``RECORD_CANCEL_CONFIRM_SECONDS`` (120).

Files that the deploy writes in ``<secrets folder>/airbyte_api/``, owned by the container's user with mode 0400, which the code location mounts as Compose
secret files at ``/run/secrets/airbyte_<name>``. The Airbyte role (``record_client_files.yml``) writes the first four; the launcher's key script
(``airbyte_key_install``) writes the last two, so that no Ansible task holds the key:

- ``api_url``, named by ``AIRBYTE_API_URL_FILE``: the forwarder on the k3s bridge address, ``http://<bridge address>:<airbyte_api_route_port>``. The Airbyte
  address is not taken from the environment. It is checked before any secret file is read, because both credentials go to it:
  ``http://<private IPv4 literal>:<port>``, no host name, no user information, no path.
- ``nessie_url``, named by ``AIRBYTE_NESSIE_URL_FILE``: the Nessie forwarder as the pods reach it,
  ``http://<bridge address>:<airbyte_nessie_route_port>/api/v2``.
- ``client_id`` and ``client_secret`` (``AIRBYTE_CLIENT_ID_FILE``, ``AIRBYTE_CLIENT_SECRET_FILE``): the instance's client credentials.
- ``aws_access_key_id`` and ``aws_secret_access_key`` (``AIRBYTE_AWS_ACCESS_KEY_ID_FILE``, ``AIRBYTE_AWS_SECRET_ACCESS_KEY_FILE``): the access key that the S3
  Data Lake destination needs for the Nessie catalog and that the two S3 sources of the CSV tables sign in with.

``.venv/bin/python -m scripts.ingestion.record_run load --run-id RUN --reference reference.json`` runs the load by hand and prints its record.
"""

from __future__ import annotations

import math
import os
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from scripts.aws_cli import Aws
from scripts.ingestion.airbyte_api import AirbyteError, HttpAirbyteApi, check_api_origin
from scripts.ingestion.closed import QuietParser
from scripts.ingestion.manifest import ManifestRef
from scripts.ingestion.notice import ClosedError, describe_error
from scripts.ingestion.record_catalog import NessieCatalog
from scripts.ingestion.record_copy import CopyError, S3CopyStore
from scripts.ingestion.record_definitions import DefinitionError, Environment, check_environment
from scripts.ingestion.record_load import Deadlines, LoadError, LoadResult, load_records
from scripts.ingestion.store import S3Store
from scripts.stack.nessie import NessieClient, NessieError

DEFAULT_PROFILE = "default"
DEFAULT_RECORD_DIR = "/opt/dagster/runs/record_loads"
SECRET_FILES = ("AIRBYTE_CLIENT_ID_FILE", "AIRBYTE_CLIENT_SECRET_FILE", "AIRBYTE_AWS_ACCESS_KEY_ID_FILE", "AIRBYTE_AWS_SECRET_ACCESS_KEY_FILE")
WAREHOUSE_LOCATION = re.compile(r"s3://([a-z0-9][a-z0-9-]{1,61}[a-z0-9])/([A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*)/?")


class SettingsError(RuntimeError):
    """A setting is missing, blank or not valid. The message names the setting and never its value."""


@dataclass(frozen=True, slots=True)
class RecordSettings:
    environment: Environment
    nessie_uri: str
    airbyte_url: str
    aws_profile: str
    record_dir: Path
    deadlines: Deadlines
    client_id: str
    client_secret: str
    aws_access_key_id: str
    aws_secret_access_key: str


def setting(environment: Mapping[str, str], name: str, default: str | None = None) -> str:
    value = environment.get(name, default if default is not None else "").strip()
    if not value:
        raise SettingsError(f"the setting {name} is missing or blank")
    return value


def seconds(environment: Mapping[str, str], name: str, default: float) -> float:
    text = environment.get(name, "").strip()
    try:
        value = float(text) if text else default
    except ValueError:
        value = math.nan
    if not math.isfinite(value) or value <= 0:
        raise SettingsError(f"the setting {name} must be a positive number of seconds")
    return value


def secret_file(environment: Mapping[str, str], name: str) -> str:
    path = Path(setting(environment, name))
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError, UnicodeDecodeError:
        raise SettingsError(f"the file named by {name} cannot be read") from None
    if not value:
        raise SettingsError(f"the file named by {name} is empty")
    return value


def plain_http(name: str, value: str) -> str:
    try:
        parts = urlsplit(value)
    except ValueError:
        raise SettingsError(f"the setting {name} must be a plain http URL with a host") from None
    if parts.scheme != "http" or not parts.hostname:
        raise SettingsError(f"the setting {name} must be a plain http URL with a host")
    return value


def api_address(environment: Mapping[str, str]) -> str:
    """The Airbyte address from the file that the deploy wrote, and nowhere else. ``SettingsError`` unless it is ``http://<private IPv4>:<port>``."""
    try:
        text = Path(setting(environment, "AIRBYTE_API_URL_FILE")).read_text(encoding="utf-8").strip()
    except OSError, UnicodeDecodeError:
        raise SettingsError("the file named by AIRBYTE_API_URL_FILE cannot be read") from None
    try:
        check_api_origin(text)
    except AirbyteError:
        raise SettingsError("the address in the file named by AIRBYTE_API_URL_FILE must be http://<a private IPv4 address>:<port>, with no host name") from None
    return text


def nessie_address(environment: Mapping[str, str]) -> str:
    """The Nessie address as Airbyte's pods reach it, from the file that the deploy wrote beside the credentials. It carries no credential."""
    try:
        text = Path(setting(environment, "AIRBYTE_NESSIE_URL_FILE")).read_text(encoding="utf-8").strip()
    except OSError, UnicodeDecodeError:
        raise SettingsError("the file named by AIRBYTE_NESSIE_URL_FILE cannot be read") from None
    try:
        return plain_http("AIRBYTE_NESSIE_URL_FILE", text)
    except SettingsError:
        raise SettingsError("the address in the file named by AIRBYTE_NESSIE_URL_FILE must be a plain http URL with a host") from None


def warehouse(environment: Mapping[str, str]) -> tuple[str, str]:
    """The project's bucket and the warehouse path in it, from ``WAREHOUSE_LOCATION`` (``s3://<bucket>/<path>``), which the host settings already carry."""
    found = WAREHOUSE_LOCATION.fullmatch(setting(environment, "WAREHOUSE_LOCATION"))
    if found is None:
        raise SettingsError("the setting WAREHOUSE_LOCATION must be s3://<bucket>/<path>")
    return found.group(1), found.group(2)


def project_store(environment: Mapping[str, str]) -> S3Store:
    """The project's bucket as the code location reaches it, for a job that only reads the bucket: the bucket is the one of ``WAREHOUSE_LOCATION``, the
    region is ``AWS_REGION`` and the profile is ``RECORD_AWS_PROFILE`` (``default``, the workload's own). None of the three is a setting of the owner's
    ``.env``, which the container does not have."""
    bucket, _ = warehouse(environment)
    return S3Store(Aws(setting(environment, "RECORD_AWS_PROFILE", DEFAULT_PROFILE), setting(environment, "AWS_REGION")), bucket)


def settings_from_environment(environment: Mapping[str, str]) -> RecordSettings:
    """The settings of a load from the container's environment. Raise ``SettingsError`` for the first setting that is missing, blank or not valid.

    The Airbyte address is read and checked before any secret file is read, because both credentials go to it.
    """
    airbyte_url = api_address(environment)
    nessie_for_airbyte = nessie_address(environment)
    nessie_here = plain_http("NESSIE_URI", setting(environment, "NESSIE_URI"))
    bucket, path = warehouse(environment)
    record_environment = Environment(bucket, setting(environment, "AWS_REGION"), path, nessie_for_airbyte)
    try:
        check_environment(record_environment)
    except DefinitionError:
        raise SettingsError("the setting WAREHOUSE_LOCATION or AWS_REGION is not valid for the project's bucket") from None
    secrets = {name: secret_file(environment, name) for name in SECRET_FILES}
    return RecordSettings(
        record_environment,
        nessie_here,
        airbyte_url,
        setting(environment, "RECORD_AWS_PROFILE", DEFAULT_PROFILE),
        Path(setting(environment, "RECORD_LOAD_DIR", DEFAULT_RECORD_DIR)),
        Deadlines(
            seconds(environment, "RECORD_SYNC_DEADLINE_SECONDS", 3600.0),
            seconds(environment, "RECORD_POLL_SECONDS", 10.0),
            seconds(environment, "RECORD_CANCEL_CONFIRM_SECONDS", 120.0),
        ),
        secrets["AIRBYTE_CLIENT_ID_FILE"],
        secrets["AIRBYTE_CLIENT_SECRET_FILE"],
        secrets["AIRBYTE_AWS_ACCESS_KEY_ID_FILE"],
        secrets["AIRBYTE_AWS_SECRET_ACCESS_KEY_FILE"],
    )


def run_load(settings: RecordSettings, run_id: str, reference: ManifestRef) -> LoadResult:
    """Build the store, the catalog and the Airbyte client from the settings and run the load for the run."""
    return load_records(
        store=S3CopyStore(Aws(settings.aws_profile, settings.environment.region), settings.environment.bucket),
        reference=reference,
        run_id=run_id,
        environment=settings.environment,
        catalog=NessieCatalog(NessieClient(settings.nessie_uri)),
        airbyte=HttpAirbyteApi(settings.airbyte_url, settings.client_id, settings.client_secret, settings.aws_access_key_id, settings.aws_secret_access_key),
        deadlines=settings.deadlines,
        record_dir=settings.record_dir,
    )


def main(argv: list[str] | None = None, environment: Mapping[str, str] | None = None) -> int:
    parser = QuietParser(prog="scripts.ingestion.record_run", description="Run a record load by hand.")
    commands = parser.add_subparsers(dest="command", required=True)
    loader = commands.add_parser("load")
    loader.add_argument("--run-id", required=True)
    loader.add_argument("--reference", type=Path, required=True, help="the manifest reference file of the passed check")
    arguments = parser.parse_args(argv)
    try:
        reference = ManifestRef.from_json(arguments.reference.read_text(encoding="utf-8"))
        result = run_load(settings_from_environment(os.environ if environment is None else environment), arguments.run_id, reference)
    except (SettingsError, LoadError, CopyError, AirbyteError, NessieError, DefinitionError, ClosedError) as error:
        sys.stderr.write(f"{describe_error(error)}\n")
        return 1
    except (OSError, ValueError) as error:
        sys.stderr.write(f"{type(error).__name__}\n")
        return 1
    sys.stdout.write(result.to_json())
    return 0


if __name__ == "__main__":
    sys.exit(main())
