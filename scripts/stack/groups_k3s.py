"""Scale Airbyte's workloads in k3s for a service group, through the Kubernetes API with a narrow service account.

The Docker socket does not reach k3s, so the group commands call the API on the host with the token of one service account. That account may read the
deployments, stateful sets and pods of Airbyte's namespace and change their scale, and nothing else (``ansible/roles/airbyte/templates/group_access.yaml.j2``
holds the role and a fast test holds the role to that list). The token is read from a file for each call, sent only in the authorization header and never
written to a message. A message names the workload and a status or a ready count, never the text of an answer.

``SCALED`` lists the limits-file entries that are Deployments or StatefulSets of the chart and the kind of each. ``ONE_OFF`` lists the entries that are jobs
and pods that Airbyte starts for itself (the bootloader, the bucket hook, the admin tools and the pods of a sync). A fast test holds the two lists to the
k3s entries of the limits file. An object is named by the release and the entry without its ``airbyte_`` prefix, with dashes.
"""

from __future__ import annotations

import http.client
import json
import re
import socket
import ssl
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit

from scripts.stack.groups import GroupError

SCALED = {
    "airbyte_server": "deployments",
    "airbyte_worker": "deployments",
    "airbyte_workload_launcher": "deployments",
    "airbyte_workload_api_server": "deployments",
    "airbyte_cron": "deployments",
    "airbyte_manifest_server": "deployments",
    "airbyte_temporal": "deployments",
    "airbyte_minio": "statefulsets",
}
ONE_OFF = frozenset(
    {
        "airbyte_bootloader",
        "airbyte_temporal_admin",
        "airbyte_minio_create_bucket",
        "airbyte_job_connector",
        "airbyte_job_check",
        "airbyte_job_discover",
        "airbyte_job_replication",
        "airbyte_job_sidecar",
    }
)
NAMESPACE_VARIABLE = "STACK_K3S_NAMESPACE"
RELEASE_VARIABLE = "STACK_K3S_RELEASE"
API_VARIABLE = "STACK_K3S_API"
ACCESS_FILE_VARIABLE = "STACK_K3S_TOKEN_FILE"
CA_FILE_VARIABLE = "STACK_K3S_CA_FILE"
DEFAULT_API = "https://k3s_api:6443"
DEFAULT_TOKEN_FILE = Path("/run/secrets/k3s_token")
DEFAULT_CA_FILE = Path("/run/secrets/k3s_ca")
TLS_NAME = "kubernetes"
NAME = re.compile(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?")
API_ADDRESS = re.compile(r"https://[A-Za-z0-9._-]+(:[0-9]{1,5})?")
REQUEST_SECONDS = 30
POLL_SECONDS = 3
LIVE_PHASES = frozenset({"Running", "Pending"})
APPS = "/apis/apps/v1/namespaces"
Clock = Callable[[], float]
Sleep = Callable[[float], None]


class Transport(Protocol):
    def request(self, method: str, path: str, body: Mapping[str, Any] | None = None) -> tuple[int, Any]: ...


@dataclass(frozen=True)
class KubernetesConfig:
    api: str
    namespace: str
    release: str
    token_file: Path
    ca_file: Path


def object_name(release: str, entry: str) -> str:
    """The name of the chart's object for a limits-file entry."""
    return f"{release}-{entry.removeprefix('airbyte_').replace('_', '-')}"


def config_from_environment(environment: Mapping[str, str]) -> KubernetesConfig | None:
    """The configuration from the environment, or None when no namespace is named, which means the k3s workloads are not managed here."""
    namespace = environment.get(NAMESPACE_VARIABLE)
    if not namespace:
        return None
    release = environment.get(RELEASE_VARIABLE, "airbyte")
    api = environment.get(API_VARIABLE, DEFAULT_API)
    for key, value, pattern in ((NAMESPACE_VARIABLE, namespace, NAME), (RELEASE_VARIABLE, release, NAME), (API_VARIABLE, api, API_ADDRESS)):
        if not pattern.fullmatch(value):
            raise GroupError(f"{key} is not valid: a name of lower case letters, digits and dashes, or an https address")
    access_file = Path(environment.get(ACCESS_FILE_VARIABLE, str(DEFAULT_TOKEN_FILE)))
    return KubernetesConfig(api, namespace, release, access_file, Path(environment.get(CA_FILE_VARIABLE, str(DEFAULT_CA_FILE))))


class PinnedConnection(http.client.HTTPSConnection):
    """An HTTPS connection to an address that checks the server's certificate against a fixed name, as ``--tls-server-name`` does."""

    def __init__(self, host: str, port: int, *, context: ssl.SSLContext, tls_name: str, timeout: float) -> None:
        super().__init__(host, port, timeout=timeout)
        self.pinned_context = context
        self.tls_name = tls_name

    def connect(self) -> None:
        raw = socket.create_connection((self.host, self.port), self.timeout)
        self.sock = self.pinned_context.wrap_socket(raw, server_hostname=self.tls_name)


class HttpsTransport:
    """The Kubernetes API over HTTPS, trusting the cluster's own certificate authority and sending the service account's token."""

    def __init__(self, config: KubernetesConfig) -> None:
        self.config = config

    def authorization(self) -> str:
        try:
            token = self.config.token_file.read_text().strip()
        except OSError as error:
            raise GroupError(f"cannot read the token file {self.config.token_file.name}: {type(error).__name__}") from error
        if not token:
            raise GroupError(f"the token file {self.config.token_file.name} is empty")
        return f"Bearer {token}"

    def context(self) -> ssl.SSLContext:
        try:
            return ssl.create_default_context(cafile=str(self.config.ca_file))
        except (OSError, ssl.SSLError) as error:
            raise GroupError(f"cannot read the certificate authority file {self.config.ca_file.name}: {type(error).__name__}") from error

    def request(self, method: str, path: str, body: Mapping[str, Any] | None = None) -> tuple[int, Any]:
        address = urlsplit(self.config.api)
        headers = {"Authorization": self.authorization(), "Accept": "application/json"}
        payload = None
        if body is not None:
            payload = json.dumps(body)
            headers["Content-Type"] = "application/merge-patch+json"
        connection = PinnedConnection(address.hostname or "", address.port or 443, context=self.context(), tls_name=TLS_NAME, timeout=REQUEST_SECONDS)
        try:
            connection.request(method, path, payload, headers)
            response = connection.getresponse()
            raw = response.read()
        except (OSError, ssl.SSLError, http.client.HTTPException) as error:
            raise GroupError(f"the Kubernetes API did not answer: {type(error).__name__}") from error
        finally:
            connection.close()
        try:
            return response.status, json.loads(raw)
        except ValueError:
            return response.status, None


class KubernetesControl:
    """The scale of a group's workloads in one namespace: ``running``, ``start`` and ``stop`` for the entries of the limits file."""

    def __init__(self, config: KubernetesConfig, transport: Transport, *, clock: Clock = time.monotonic, sleep: Sleep = time.sleep) -> None:
        self.config = config
        self.transport = transport
        self.clock = clock
        self.sleep = sleep

    def object_path(self, entry: str, suffix: str = "") -> str:
        return f"{APPS}/{self.config.namespace}/{SCALED[entry]}/{object_name(self.config.release, entry)}{suffix}"

    def scaled(self, workloads: Sequence[str]) -> list[str]:
        return [entry for entry in workloads if entry in SCALED]

    def state(self, entry: str) -> tuple[int, int] | None:
        """The desired and the ready replicas of a workload, or None when the namespace has no such object."""
        name = object_name(self.config.release, entry)
        status, answer = self.transport.request("GET", self.object_path(entry))
        if status == 404:
            return None
        if status != 200:
            raise GroupError(f"{name}: HTTP {status}")
        try:
            desired = answer["spec"]["replicas"]
            ready = answer.get("status", {}).get("readyReplicas", 0)
        except (KeyError, TypeError, AttributeError) as error:
            raise GroupError(f"{name}: the answer is malformed") from error
        if not isinstance(desired, int) or not isinstance(ready, int):
            raise GroupError(f"{name}: the answer is malformed")
        return desired, ready

    def running(self, workloads: Sequence[str]) -> frozenset[str]:
        found = {entry: self.state(entry) for entry in self.scaled(workloads)}
        return frozenset(entry for entry, state in found.items() if state is not None and state[1] > 0)

    def scale(self, entries: Sequence[str], replicas: int) -> list[str]:
        """Set the scale of each workload and return the entries that exist. A failure for one workload does not stop the others, and is raised at the end."""
        existing: list[str] = []
        errors: list[str] = []
        for entry in entries:
            status, _ = self.transport.request("PATCH", self.object_path(entry, "/scale"), {"spec": {"replicas": replicas}})
            if status == 200:
                existing.append(entry)
            elif status != 404:
                errors.append(f"{object_name(self.config.release, entry)}: HTTP {status}")
        if errors:
            raise GroupError(f"scaling in the namespace {self.config.namespace} failed: {'; '.join(errors)}")
        return existing

    def wait(self, entries: Sequence[str], deadline_seconds: float, *, ready: bool) -> None:
        started = self.clock()
        while True:
            pending: list[str] = []
            for entry in entries:
                state = self.state(entry)
                name = object_name(self.config.release, entry)
                if state is not None and (state[1] < 1 if ready else state[1] > 0):
                    pending.append(f"{name}: ready {state[1]} of {state[0]}")
            if not pending:
                return
            if self.clock() - started >= deadline_seconds:
                wanted = "ready" if ready else "stopped"
                raise GroupError(f"in the namespace {self.config.namespace} not {wanted} after {deadline_seconds:g} seconds: {'; '.join(pending)}")
            self.sleep(POLL_SECONDS)

    def live_pods(self, entries: Sequence[str]) -> list[str]:
        """The pods that run or wait and belong to the workloads or to a sync, as ``name (phase)``. A finished pod is not listed."""
        status, answer = self.transport.request("GET", f"/api/v1/namespaces/{self.config.namespace}/pods")
        if status != 200 or not isinstance(answer, dict):
            raise GroupError(f"listing the pods of the namespace {self.config.namespace}: HTTP {status}")
        prefixes = tuple(f"{object_name(self.config.release, entry)}-" for entry in entries)
        found: list[str] = []
        for item in answer.get("items", []):
            name, phase = item["metadata"]["name"], item.get("status", {}).get("phase", "")
            ours = name.startswith(prefixes) or not name.startswith(f"{self.config.release}-")
            if ours and phase in LIVE_PHASES:
                found.append(f"{name} ({phase})")
        return found

    def start(self, workloads: Sequence[str], deadline_seconds: float) -> None:
        entries = self.scaled(workloads)
        if not entries:
            return
        existing = self.scale(entries, 1)
        if not existing:
            raise GroupError(f"none of the workloads of the group exist in the namespace {self.config.namespace}: {', '.join(entries)}")
        self.wait(existing, deadline_seconds, ready=True)

    def stop(self, workloads: Sequence[str], deadline_seconds: float) -> None:
        entries = self.scaled(workloads)
        if not entries:
            return
        existing = self.scale(entries, 0)
        self.wait(existing, deadline_seconds, ready=False)
        started = self.clock()
        while True:
            left = self.live_pods(existing)
            if not left:
                return
            if self.clock() - started >= deadline_seconds:
                raise GroupError(f"in the namespace {self.config.namespace} still running after {deadline_seconds:g} seconds: {', '.join(left)}")
            self.sleep(POLL_SECONDS)


def control_from_environment(environment: Mapping[str, str]) -> KubernetesControl | None:
    config = config_from_environment(environment)
    return None if config is None else KubernetesControl(config, HttpsTransport(config))
