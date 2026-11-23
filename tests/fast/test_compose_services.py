"""Every Compose service file holds the properties that the design requires, and the limits file and the Compose files name the same services.

The first group reads the real files, so a later service is held to the same rules. The second group plants one defect at a time in a copy of
the PostgreSQL service and expects the checker to name that defect, so a check that stopped looking would fail here.
"""

from __future__ import annotations

import copy
import posixpath
import re
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.infra.host import PROJECT_WORKLOAD_GIDS
from scripts.stack.limits import load_limits, variable_name
from scripts.stack.yaml_text import load_yaml

COMPOSE = Path(__file__).resolve().parents[2] / "compose"
ROOT_FILE = COMPOSE / "compose.yml"
SERVICES = COMPOSE / "services"
IMAGE = re.compile(r"^[a-z0-9./_-]+:(?!latest@)[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}$")
PROJECT_IMAGE = re.compile(r"^surgical-risk-[a-z0-9_-]+:(?!latest(?:@|$))[0-9][A-Za-z0-9._-]*(?:@sha256:[0-9a-f]{64})?$")
BATCH_IMAGE = re.compile(r"^[a-z0-9./_-]+:(?!latest$)[A-Za-z0-9._-]+$")
RESTART_POLICIES = {"always", "unless-stopped"}
HEALTHCHECK_FIELDS = {"test", "interval", "timeout", "retries", "start_period"}
HOST_NETWORK_SERVICES = {"caddy"}
# The host paths that a collector may read, by the source on the host and always read-only: the runtime sockets of Docker and containerd for container
# metrics and logs, and the kernel's process and system folders. No other service has one, and the target in the container is free.
READ_ONLY_HOST_SOURCES = {"alloy": {"/var/run/docker.sock", "/run/containerd/containerd.sock", "/proc", "/sys"}}
DOCKER_SOCKET = "/var/run/docker.sock:/var/run/docker.sock"
# The exact host paths that the design names for a service: the Tailscale socket folder for the gateway and, by the owner's decision, the Docker socket for the
# code location that runs the jobs.
HOST_PATH_VOLUMES = {"caddy": {"/var/run/tailscale:/var/run/tailscale:ro"}, "dagster_code_location": {DOCKER_SOCKET}}


def load(path: Path) -> dict[str, Any]:
    document = load_yaml(path.read_text())
    if not isinstance(document, dict):
        raise TypeError(f"{path.name} is not a mapping")
    return document


def host_mount_allowed(name: str, volume: object) -> bool:
    """Whether a volume entry is a host path that the design names for this service: an exact entry, or a source of the read-only list with the mode ro."""
    text = str(volume)
    parts = text.split(":")
    return text in HOST_PATH_VOLUMES.get(name, set()) or (len(parts) == 3 and parts[2] == "ro" and parts[0] in READ_ONLY_HOST_SOURCES.get(name, set()))


def service_problems(name: str, document: dict[str, Any]) -> list[str]:
    """Each way that one service file departs from the design, as a sentence that names the property."""
    problems: list[str] = []
    services = document.get("services")
    if not isinstance(services, dict) or list(services) != [name]:
        return [f"{name}: the file must define exactly one service, named like the file"]
    service = services[name]
    image = str(service.get("image", ""))
    batch = service.get("profiles") == ["batch"]
    build = service.get("build")
    if batch and not (isinstance(build, dict) and build.get("dockerfile") and BATCH_IMAGE.match(image)):
        problems.append(f"{name}: a batch job's image must be built from a tracked Dockerfile and named by a tag other than latest")
    elif not batch and not (PROJECT_IMAGE if image.startswith("surgical-risk-") else IMAGE).match(image):
        problems.append(
            f"{name}: image must be pinned by a tag other than latest and by a sha256 digest (a project-built image: a version tag, the digest once pushed)"
        )
    if service.get("platform") != "linux/amd64":
        problems.append(f"{name}: platform must be linux/amd64")
    if not re.fullmatch(rf"\$\{{{variable_name(name)}:\?[^}}]+\}}", str(service.get("mem_limit", ""))):
        problems.append(f"{name}: mem_limit must read {variable_name(name)} from the rendered limits and fail when it is unset")
    health = service.get("healthcheck")
    if not batch and (not isinstance(health, dict) or not set(health) >= HEALTHCHECK_FIELDS):
        problems.append(f"{name}: healthcheck must set {', '.join(sorted(HEALTHCHECK_FIELDS))}")
    if batch and service.get("restart") != "no":
        problems.append(f"{name}: a batch job's restart must be no")
    if not batch and service.get("restart") not in RESTART_POLICIES:
        problems.append(f"{name}: restart must be one of {', '.join(sorted(RESTART_POLICIES))}")
    volumes = service.get("volumes") or []
    declared = document.get("volumes") or {}
    named = [str(volume).split(":")[0] for volume in volumes if not host_mount_allowed(name, volume)]
    if not volumes or any(not re.fullmatch(r"[a-z][a-z0-9_]*", source) or source not in declared for source in named):
        problems.append(f"{name}: data must live in declared named volumes, with no host path")
    if service.get("network_mode") == "host" and (name not in HOST_NETWORK_SERVICES or service.get("ports")):
        problems.append(f"{name}: network_mode host would listen on every address")
    for port in service.get("ports") or []:
        if not isinstance(port, str) or not re.match(r"127\.0\.0\.1:", port):
            problems.append(f"{name}: every published port must bind 127.0.0.1")
    for key, value in (service.get("environment") or {}).items():
        if re.search(r"PASSWORD|SECRET|TOKEN|KEY", str(key)) and not str(key).endswith("_FILE") and str(value):
            problems.append(f"{name}: {key} must come from a secrets file, not a value in the file")
    for secret in service.get("secrets") or []:
        source = secret if isinstance(secret, str) else secret.get("source")
        if source not in (document.get("secrets") or {}):
            problems.append(f"{name}: secret {source} is not declared in the file")
    return problems


class ServiceFilesTest(unittest.TestCase):
    def test_the_root_file_names_the_project_and_includes_every_service_file_and_nothing_else(self) -> None:
        root = load(ROOT_FILE)
        self.assertRegex(str(root.get("name")), r"^[a-z][a-z0-9_]*$")
        self.assertEqual(sorted(f"services/{path.name}" for path in SERVICES.glob("*.yml")), sorted(root["include"]))
        self.assertEqual({"name", "include"}, set(root))

    def test_every_service_file_holds_the_designed_properties(self) -> None:
        files = sorted(SERVICES.glob("*.yml"))
        self.assertTrue(files)
        for path in files:
            with self.subTest(service=path.stem):
                self.assertEqual([], service_problems(path.stem, load(path)))

    def test_the_compose_services_and_the_limits_file_name_the_same_services(self) -> None:
        in_compose = {path.stem for path in SERVICES.glob("*.yml")}
        in_limits = {name for name, entry in load_limits().services.items() if entry.runtime == "compose"}
        self.assertEqual([], sorted(in_compose - in_limits), "a Compose service has no entry in the limits file")
        self.assertEqual([], sorted(in_limits - in_compose), "a Compose entry in the limits file has no service")

    def test_every_published_port_variable_has_a_default_so_a_service_starts_without_one(self) -> None:
        for path in SERVICES.glob("*.yml"):
            for port in load(path)["services"][path.stem].get("ports") or []:
                with self.subTest(service=path.stem, port=port):
                    self.assertRegex(port, r"^127\.0\.0\.1:(\d+|\$\{[A-Z_]+:-\d+\}):\d+$")


class HostNetworkExceptionTest(unittest.TestCase):
    def caddy(self) -> dict[str, Any]:
        return copy.deepcopy(load(SERVICES / "caddy.yml"))

    def test_the_real_caddy_file_has_no_problem(self) -> None:
        self.assertEqual([], service_problems("caddy", self.caddy()))

    def test_caddy_with_a_published_port_is_named(self) -> None:
        document = self.caddy()
        document["services"]["caddy"]["ports"] = ["127.0.0.1:9440:9440"]
        self.assertIn("network_mode", service_problems("caddy", document)[0])

    def test_caddy_with_another_host_path_is_named(self) -> None:
        document = self.caddy()
        document["services"]["caddy"]["volumes"].append("/etc:/etc:ro")
        self.assertIn("named volumes", service_problems("caddy", document)[0])

    def test_another_service_on_the_host_network_or_with_the_socket_folder_is_named(self) -> None:
        document = copy.deepcopy(load(SERVICES / "postgres.yml"))
        document["services"]["postgres"]["network_mode"] = "host"
        self.assertIn("network_mode", service_problems("postgres", document)[0])
        document = copy.deepcopy(load(SERVICES / "postgres.yml"))
        document["services"]["postgres"]["volumes"].append("/var/run/tailscale:/var/run/tailscale:ro")
        self.assertIn("named volumes", service_problems("postgres", document)[0])


PUBLISHED_PORT = re.compile(r"127\.0\.0\.1:\$\{[A-Z_]+:-(\d+)\}:\d+")


def port_clashes(documents: dict[str, dict[str, Any]]) -> list[str]:
    """Each default host port that more than one service publishes, with the services that publish it."""
    publishers: dict[int, list[str]] = {}
    for name, document in documents.items():
        for entry in document["services"][name].get("ports") or []:
            if match := PUBLISHED_PORT.fullmatch(str(entry)):
                publishers.setdefault(int(match[1]), []).append(name)
    return [f"port {port}: {', '.join(names)}" for port, names in sorted(publishers.items()) if len(names) > 1]


class PublishedPortTest(unittest.TestCase):
    """Two services that publish the same default host port cannot both start on one host."""

    def documents(self) -> dict[str, dict[str, Any]]:
        return {path.stem: copy.deepcopy(load(path)) for path in sorted(SERVICES.glob("*.yml"))}

    def test_no_two_services_publish_the_same_default_host_port(self) -> None:
        documents = self.documents()
        self.assertGreaterEqual(sum(1 for document in documents.values() if (next(iter(document["services"].values())).get("ports"))), 6)
        self.assertEqual([], port_clashes(documents))

    def test_a_planted_second_service_on_a_published_port_is_named_with_both_services(self) -> None:
        documents = self.documents()
        documents["loki"]["services"]["loki"]["ports"] = ["127.0.0.1:${LOKI_HOST_PORT:-5432}:3100"]
        self.assertEqual(["port 5432: loki, postgres"], port_clashes(documents))


class CollectorMountTest(unittest.TestCase):
    """Alloy reads the container runtimes' sockets and the kernel's folders from the host, read-only, and nothing else of the host."""

    def alloy(self) -> dict[str, Any]:
        return copy.deepcopy(load(SERVICES / "alloy.yml"))

    def test_the_real_alloy_file_has_no_problem_and_mounts_both_sockets_read_only_as_volumes_not_as_secrets(self) -> None:
        document = self.alloy()
        service = document["services"]["alloy"]
        self.assertEqual([], service_problems("alloy", document))
        host_volumes = sorted(str(volume) for volume in service["volumes"] if str(volume).startswith("/"))
        self.assertEqual(["/run/containerd/containerd.sock:/run/containerd/containerd.sock:ro", "/var/run/docker.sock:/var/run/docker.sock:ro"], host_volumes)
        self.assertEqual({"postgres_alloy_password"}, set(service["secrets"]))
        self.assertEqual({"postgres_alloy_password"}, set(document["secrets"]))

    def test_each_named_host_path_is_allowed_read_only_and_a_control_passes(self) -> None:
        for source in ("/var/run/docker.sock", "/run/containerd/containerd.sock", "/proc", "/sys"):
            with self.subTest(source=source):
                document = self.alloy()
                document["services"]["alloy"]["volumes"].append(f"{source}:/host{source}:ro")
                self.assertEqual([], service_problems("alloy", document))

    def test_an_extra_host_mount_or_one_that_is_not_read_only_is_named(self) -> None:
        for volume in (
            "/etc:/etc:ro",
            "/:/rootfs:ro",
            "/var/lib/docker:/var/lib/docker:ro",
            "/proc:/host/proc",
            "/sys:/host/sys:rw",
            "/proc:/host/proc:ro,z",
            "/proc",
        ):
            with self.subTest(volume=volume):
                document = self.alloy()
                document["services"]["alloy"]["volumes"].append(volume)
                self.assertIn("named volumes", service_problems("alloy", document)[0])

    def test_the_docker_socket_without_the_read_only_mode_is_named(self) -> None:
        document = self.alloy()
        volumes = document["services"]["alloy"]["volumes"]
        volumes[:] = [str(volume).removesuffix(":ro") for volume in volumes]
        self.assertIn("named volumes", service_problems("alloy", document)[0])

    def test_no_other_service_may_read_a_host_path_that_alloy_may(self) -> None:
        for name in ("postgres", "grafana", "loki"):
            with self.subTest(service=name):
                document = copy.deepcopy(load(SERVICES / f"{name}.yml"))
                document["services"][name]["volumes"].append("/proc:/host/proc:ro")
                self.assertIn("named volumes", service_problems(name, document)[0])


class DockerSocketExceptionTest(unittest.TestCase):
    """The owner's decision: the Docker socket is mounted into the Dagster container that runs the jobs, and into no other container."""

    def code_location(self) -> dict[str, Any]:
        return copy.deepcopy(load(SERVICES / "dagster_code_location.yml"))

    def test_the_real_code_location_file_mounts_the_socket_and_has_no_problem(self) -> None:
        document = self.code_location()
        self.assertIn(DOCKER_SOCKET, document["services"]["dagster_code_location"]["volumes"])
        self.assertEqual([], service_problems("dagster_code_location", document))

    def test_no_other_service_file_mounts_the_socket(self) -> None:
        """The collector is the one other holder, by the owner's decision, and reads it read-only."""
        expected = {"dagster_code_location": [DOCKER_SOCKET], "alloy": ["/var/run/docker.sock:/var/run/docker.sock:ro"]}
        for path in SERVICES.glob("*.yml"):
            mounted = [volume for volume in load(path)["services"][path.stem].get("volumes") or [] if "docker.sock" in str(volume)]
            with self.subTest(service=path.stem):
                self.assertEqual(expected.get(path.stem, []), mounted)

    def test_the_code_location_reaches_the_socket_through_its_group_by_number_and_never_runs_as_root(self) -> None:
        service = self.code_location()["services"]["dagster_code_location"]
        self.assertEqual("10001:10001", service["user"])
        self.assertEqual(["${DOCKER_SOCKET_GID:?write the host settings first}", "20105"], service["group_add"])
        self.assertEqual([{"path": "../host.env", "required": False}, {"path": "../heavy.env", "required": True}], service["env_file"])

    def test_the_code_location_reaches_the_k3s_api_on_the_host_with_the_service_account_files_as_secrets(self) -> None:
        document = self.code_location()
        service = document["services"]["dagster_code_location"]
        self.assertEqual(["k3s_api:host-gateway"], service["extra_hosts"])
        self.assertEqual({"k3s_token", "k3s_ca"}, set(service["secrets"]) & {"k3s_token", "k3s_ca"})
        self.assertEqual("${SECRETS_DIR:?render the host settings first}/k3s/dagster.token", document["secrets"]["k3s_token"]["file"])
        self.assertEqual("${SECRETS_DIR:?render the host settings first}/k3s/ca.crt", document["secrets"]["k3s_ca"]["file"])

    def test_the_socket_in_another_service_is_named(self) -> None:
        document = copy.deepcopy(load(SERVICES / "postgres.yml"))
        document["services"]["postgres"]["volumes"].append(DOCKER_SOCKET)
        self.assertIn("named volumes", service_problems("postgres", document)[0])

    def test_any_other_host_path_or_a_changed_socket_mount_in_the_code_location_is_named(self) -> None:
        for mount in ("/etc:/etc:ro", "/var/run:/var/run", "/var/run/docker.sock:/var/run/docker.sock:ro", "/var/run/docker.sock:/run/docker.sock"):
            with self.subTest(mount=mount):
                document = self.code_location()
                document["services"]["dagster_code_location"]["volumes"].append(mount)
                self.assertIn("named volumes", service_problems("dagster_code_location", document)[0])


CREDENTIAL_READERS = {
    "spark": ("spark", 20102),
    "trino": ("trino", 20103),
    "replay": ("replay", 20104),
    "dagster_code_location": ("orchestrator", 20105),
}
EXTRA_GROUPS = {"dagster_code_location": ["<DOCKER_SOCKET_GID>"]}
"""The services that read a credential folder, each bound to its folder and its workload group. The table is fixed here, not derived from the mount, so a
change of folder and group together is still a change this test names. A service has exactly its own entry and nothing else; a service that is not in the
table has no credential folder and no workload group. ``EXTRA_GROUPS`` is the one group, as the check resolves it, that a reader may have besides its
workload's: the code location also reaches the Docker socket through the socket's group (project standard)."""
ACCESS_ROOT = "/run/workload_access"
OWN_MOUNT = re.compile(r"[a-z][a-z0-9_]*:/run/workload_access:ro")


class Unresolvable(ValueError):
    """A field that the credential checks read holds a variable whose value the test cannot know."""


DEFAULTED = re.compile(r"\$\{[A-Za-z_][A-Za-z0-9_]*(?::-|-)([^${}]*)\}")
REQUIRED = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?:(?::\?|\?)[^${}]*)?\}|\$([A-Za-z_][A-Za-z0-9_]*)")
HOST_RENDERED_GROUPS = {"dagster_code_location": {"DOCKER_SOCKET_GID"}}
"""The variables that a service's user and group fields may name without a default, each rendered into the host settings file by the Ansible role from the
host itself (the group of the Docker socket). Their value is the host's, so each stands for one group that is not a workload group. No other field and no
other service may name a variable without a default."""
HOST_SETTINGS_RENDERER = COMPOSE.parent / "ansible" / "roles" / "compose_stack" / "tasks" / "render_environment.yml"


def resolve(text: str, rendered: frozenset[str] | set[str] = frozenset()) -> str:
    """The text with every Compose interpolation resolved as Compose resolves it: ``${VAR:-default}`` and ``${VAR-default}`` take their default (the innermost
    first), and ``${VAR}``, ``$VAR``, ``${VAR:?message}`` and ``${VAR?message}`` are refused unless the variable is one of ``rendered``, which stands for the
    host's own value. Any other use of ``$`` is refused."""
    previous = None
    while previous != text:
        previous, text = text, DEFAULTED.sub(lambda match: match.group(1), text)

    def required(match: re.Match[str]) -> str:
        name = match.group(1) or match.group(2)
        if name not in rendered:
            raise Unresolvable(text)
        return f"<{name}>"

    text = REQUIRED.sub(required, text)
    if "$" in text:
        raise Unresolvable(text)
    return text


def normal_path(source: str) -> str:
    """A bind source as Linux reads it: any run of slashes is one, ``.`` segments and a trailing slash are dropped, ``..`` is resolved lexically and
    ``/var/run`` is ``/run`` (the first is a link to the second on the host)."""
    normal = posixpath.normpath(re.sub(r"/+", "/", source.strip()))
    return "/run" + normal.removeprefix("/var/run") if normal == "/var/run" or normal.startswith("/var/run/") else normal


def credential_folder(source: str) -> str | None:
    """The folder under ``/run/workload_access/`` that a resolved bind source names: the empty text for the root itself and None for a path outside it."""
    normal = normal_path(source)
    if normal == ACCESS_ROOT:
        return ""
    return normal.removeprefix(f"{ACCESS_ROOT}/") if normal.startswith(f"{ACCESS_ROOT}/") else None


def includes_credential_root(source: str) -> bool:
    """Whether a resolved bind source is a folder above ``/run/workload_access``, such as ``/run`` or ``/``, which holds the credential root inside it."""
    normal = normal_path(source)
    return ACCESS_ROOT.startswith(normal.rstrip("/") + "/")


def volume_source(entry: object) -> str:
    """The source of a volume entry, short or long form, as written. The short form is cut at its first colon outside a variable."""
    if isinstance(entry, dict):
        return str(entry.get("source", ""))
    depth, text = 0, str(entry)
    for index, character in enumerate(text):
        depth += character == "{"
        depth -= character == "}"
        if character == ":" and depth == 0:
            return text[:index]
    return text


def credential_mounts(
    service: dict[str, Any], document: dict[str, Any], literal: Callable[[str, str], str | None]
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Each volume entry of the service whose bind source is at or under ``/run/workload_access/``, as the folder and the entry as written, and each whose
    source is a folder above it, as the path and the entry. A named volume counts by the device that the file declares for it. ``literal`` resolves a field's
    text, or says it cannot and returns None."""
    declared = document.get("volumes") or {}
    inside, above = [], []
    for entry in service.get("volumes") or []:
        source = volume_source(entry)
        if source in declared:
            source = str(((declared[source] or {}).get("driver_opts") or {}).get("device", ""))
        resolved = literal(source, "a volume source")
        if resolved is None:
            continue
        if (folder := credential_folder(resolved)) is not None:
            inside.append((folder, str(entry)))
        elif includes_credential_root(resolved):
            above.append((normal_path(resolved), str(entry)))
    return inside, above


def access_problems(name: str, document: dict[str, Any]) -> list[str]:
    """Each way that one service departs from the table of credential readers: a folder or a workload group that is not its own, or its own written wrongly,
    or a field that the check needs and cannot resolve."""
    service = document["services"][name]
    own = CREDENTIAL_READERS.get(name)
    found: list[str] = []

    def literal(text: str, field: str, rendered: set[str] = frozenset()) -> str | None:  # type: ignore[assignment]
        try:
            return resolve(text, rendered)
        except Unresolvable:
            found.append(f"{name}: {field} `{text}`: this field must be a literal or a resolvable default")
            return None

    mounts, above = credential_mounts(service, document, literal)
    found.extend(f"{name}: mounts {path}, which includes the credential root {ACCESS_ROOT}" for path, _ in above)
    for folder, entry in mounts:
        if own is None:
            found.append(f"{name}: is not in the table of services that may read a credential folder and mounts {folder or 'the whole folder'}")
        elif folder != own[0]:
            found.append(f"{name}: may read only the credential folder of {own[0]}, not {folder or 'the whole folder'}")
        elif not OWN_MOUNT.fullmatch(entry):
            found.append(f"{name}: the credential folder of {folder} must be mounted read-only at /run/workload_access")
    if own is not None and own[0] not in [folder for folder, _ in mounts]:
        found.append(f"{name}: must mount the credential folder of {own[0]}")
    rendered = HOST_RENDERED_GROUPS.get(name, set())
    user = literal(str(service.get("user", "")), "the user", rendered)
    primary = (user or "").partition(":")[2]
    supplementary = [group for group in (literal(str(entry), "a group_add entry", rendered) for entry in service.get("group_add") or []) if group is not None]
    workload_groups = {str(group) for group in PROJECT_WORKLOAD_GIDS.values()}
    if own is None:
        if workload_groups & {primary, *supplementary}:
            found.append(f"{name}: is not in the table of services that may read a credential folder and runs in a workload group")
    else:
        if sorted(supplementary) != sorted([str(own[1]), *EXTRA_GROUPS.get(name, [])]):
            found.append(f"{name}: must run in the group {own[1]} of the workload {own[0]} and in no other group but {EXTRA_GROUPS.get(name, 'none')}")
        if primary in workload_groups:
            found.append(f"{name}: must not take a workload group as its primary group")
    return found


class WorkloadAccessTest(unittest.TestCase):
    """A service that reads a workload's short-lived AWS credentials mounts that workload's folder read-only and runs in its group: the helper writes the
    folder for root and the workload's group, mode 0750, and the credentials 0640, so a container outside the group cannot read them."""

    def replay(self) -> dict[str, Any]:
        return copy.deepcopy(load(SERVICES / "replay.yml"))

    def test_every_service_with_a_credential_folder_mounts_it_read_only_and_runs_in_its_group_and_no_other_does_either(self) -> None:
        readers = []
        for path in sorted(SERVICES.glob("*.yml")):
            with self.subTest(service=path.stem):
                self.assertEqual([], access_problems(path.stem, load(path)))
            if "/run/workload_access/" in path.read_text():
                readers.append(path.stem)
        self.assertEqual(sorted(CREDENTIAL_READERS), readers)

    def test_the_replay_runs_under_an_init_process_so_a_stop_before_its_handler_is_set_ends_it_at_once(self) -> None:
        self.assertIs(True, load(SERVICES / "replay.yml")["services"]["replay"].get("init"))

    def test_the_fixed_table_agrees_with_the_project_workload_groups(self) -> None:
        for name, (workload, group) in CREDENTIAL_READERS.items():
            self.assertEqual(group, PROJECT_WORKLOAD_GIDS[workload], name)

    def test_the_code_location_reads_the_orchestrator_credentials_through_the_same_interface_as_the_other_readers(self) -> None:
        document = load(SERVICES / "dagster_code_location.yml")
        service = document["services"]["dagster_code_location"]
        self.assertEqual("/etc/workload_access/client_config", service["environment"]["AWS_CONFIG_FILE"])
        self.assertEqual("true", service["environment"]["AWS_EC2_METADATA_DISABLED"])
        self.assertIn("workload_access_orchestrator:/run/workload_access:ro", service["volumes"])
        self.assertEqual({"source": "dagster_aws_client_config", "target": "/etc/workload_access/client_config"}, service["configs"][-1])
        self.assertEqual("${AWS_CLIENT_CONFIG_FILE:-/etc/workload_access/client_config}", document["configs"]["dagster_aws_client_config"]["file"])
        self.assertEqual(
            "${ORCHESTRATOR_ACCESS_DIR:-/run/workload_access/orchestrator}", document["volumes"]["workload_access_orchestrator"]["driver_opts"]["device"]
        )

    def test_the_socket_group_is_the_only_group_besides_the_workload_s_that_a_reader_may_have(self) -> None:
        document = copy.deepcopy(load(SERVICES / "dagster_code_location.yml"))
        service = document["services"]["dagster_code_location"]
        service["group_add"] = [*service["group_add"], "20102"]
        self.assertEqual(1, len(access_problems("dagster_code_location", document)))
        service["group_add"] = ["${DOCKER_SOCKET_GID:?write the host settings first}"]
        self.assertEqual(1, len(access_problems("dagster_code_location", document)))

    def test_a_folder_and_group_changed_together_is_named(self) -> None:
        document = self.replay()
        service = document["services"]["replay"]
        service["group_add"] = [str(PROJECT_WORKLOAD_GIDS["spark"])]
        service["volumes"] = [str(entry).replace("workload_access_replay", "workload_access_spark") for entry in service["volumes"]]
        document["volumes"] = {"workload_access_spark": {"driver_opts": {"device": "${SPARK_ACCESS_DIR:-/run/workload_access/spark}"}}}
        problems = access_problems("replay", document)
        self.assertEqual(3, len(problems), problems)
        self.assertIn("may read only the credential folder of replay, not spark", problems[0])

    def test_a_missing_group_another_group_or_a_writable_mount_is_named(self) -> None:
        for change in ("no group", "other group", "extra group", "writable", "no mount"):
            document = self.replay()
            service = document["services"]["replay"]
            if change == "no group":
                del service["group_add"]
            elif change == "other group":
                service["group_add"] = [str(PROJECT_WORKLOAD_GIDS["spark"])]
            elif change == "extra group":
                service["group_add"] = [*service["group_add"], str(PROJECT_WORKLOAD_GIDS["spark"])]
            elif change == "writable":
                service["volumes"] = [str(entry).removesuffix(":ro") for entry in service["volumes"]]
            else:
                service["volumes"] = [entry for entry in service["volumes"] if "workload_access" not in entry]
            with self.subTest(change=change):
                self.assertEqual(1, len(access_problems("replay", document)))

    def test_the_reviewers_case_a_second_credential_folder_and_another_workload_group_as_the_primary_group_is_named(self) -> None:
        document = self.replay()
        service = document["services"]["replay"]
        service["user"] = f"10003:{PROJECT_WORKLOAD_GIDS['spark']}"
        service["volumes"] = [*service["volumes"], "/run/workload_access/spark/:/run/workload_access/spark:ro"]
        problems = access_problems("replay", document)
        self.assertEqual(2, len(problems), problems)
        self.assertIn("not spark", problems[0])
        self.assertIn("primary group", problems[1])

    def test_a_second_credential_folder_in_any_written_form_is_named(self) -> None:
        long_form = {"type": "bind", "source": "/run/workload_access/spark/", "target": "/elsewhere", "read_only": True}
        for label, entry in (
            ("short form", "/run/workload_access/spark:/elsewhere:ro"),
            ("trailing slash", "/run/workload_access/spark/:/elsewhere:ro"),
            ("two trailing slashes", "/run/workload_access/spark//:/elsewhere:ro"),
            ("dots", "/run/workload_access/replay/../spark:/elsewhere:ro"),
            ("variable default", "${SPARK_ACCESS_DIR:-/run/workload_access/spark/}:/elsewhere:ro"),
            ("long form", long_form),
            ("the whole folder", "/run/workload_access:/elsewhere:ro"),
            ("the whole folder with a slash", "/run/workload_access/:/elsewhere"),
        ):
            with self.subTest(form=label):
                document = self.replay()
                document["services"]["replay"]["volumes"].append(entry)
                problems = access_problems("replay", document)
                self.assertEqual(1, len(problems), problems)
                self.assertIn("may read only the credential folder of replay", problems[0])

    def test_a_second_credential_folder_as_a_named_volume_with_a_trailing_slash_is_named(self) -> None:
        document = self.replay()
        document["services"]["replay"]["volumes"].append("workload_access_other:/elsewhere:ro")
        document["volumes"]["workload_access_other"] = {
            "driver_opts": {"type": "none", "o": "bind", "device": "${OTHER_ACCESS_DIR:-/run/workload_access/trino/}"}
        }
        problems = access_problems("replay", document)
        self.assertEqual(1, len(problems), problems)
        self.assertIn("not trino", problems[0])

    def test_the_own_folder_written_with_a_trailing_slash_or_dots_is_still_its_own_folder(self) -> None:
        for device in ("${REPLAY_ACCESS_DIR:-/run/workload_access/replay/}", "/run/workload_access/replay//", "/run/workload_access/spark/../replay"):
            with self.subTest(device=device):
                document = self.replay()
                document["volumes"]["workload_access_replay"]["driver_opts"]["device"] = device
                self.assertEqual([], access_problems("replay", document))

    def test_the_own_folder_mounted_a_second_time_is_named_and_the_control_passes(self) -> None:
        document = self.replay()
        self.assertEqual([], access_problems("replay", document))
        document["services"]["replay"]["volumes"].append("/run/workload_access/replay/:/elsewhere:ro")
        self.assertEqual(1, len(access_problems("replay", document)))

    def test_the_reviewers_two_plants_a_double_slash_source_and_a_primary_group_given_as_a_default_are_named(self) -> None:
        document = self.replay()
        document["services"]["replay"]["volumes"].append("//run/workload_access/spark/:/elsewhere:ro")
        problems = access_problems("replay", document)
        self.assertEqual(1, len(problems), problems)
        self.assertIn("not spark", problems[0])
        document = self.replay()
        document["services"]["replay"]["user"] = "10003:${TEST_PRIMARY_GID:-20104}"
        problems = access_problems("replay", document)
        self.assertEqual(1, len(problems), problems)
        self.assertIn("primary group", problems[0])

    def test_a_bind_source_is_read_as_linux_reads_a_path(self) -> None:
        for label, source in (
            ("slashes and a dot", "/run//workload_access/./spark"),
            ("three slashes", "///run/workload_access///spark//"),
            ("dots and a parent", "/run/workload_access/replay/../spark/."),
            ("a parent above the root", "/run/workload_access/../workload_access/spark"),
            ("a variable default with slashes", "${X:-//run/workload_access//spark}"),
            ("a default in the middle", "/run/workload_access/${X-spark}"),
            ("a nested default", "${A:-${B:-/run/workload_access/spark}}"),
            ("a default followed by a path", "${DIR:-/run/workload_access}/spark"),
        ):
            for form in ("short", "long", "named"):
                with self.subTest(source=label, form=form):
                    document = self.replay()
                    if form == "short":
                        document["services"]["replay"]["volumes"].append(f"{source}:/elsewhere:ro")
                    elif form == "long":
                        document["services"]["replay"]["volumes"].append({"type": "bind", "source": source, "target": "/elsewhere"})
                    else:
                        document["services"]["replay"]["volumes"].append("other:/elsewhere:ro")
                        document["volumes"]["other"] = {"driver_opts": {"type": "none", "o": "bind", "device": source}}
                    problems = access_problems("replay", document)
                    self.assertEqual(1, len(problems), problems)
                    self.assertIn("may read only the credential folder of replay, not spark", problems[0])

    def test_the_own_folder_in_these_forms_is_still_its_own_folder_and_a_path_outside_the_root_is_no_folder(self) -> None:
        for source in (
            "//run//workload_access/./replay/",
            "${X:-/run/workload_access/spark/../replay}",
            "/run/workload_access_other/spark",
            "/run/workload_accessspark",
        ):
            with self.subTest(source=source):
                document = self.replay()
                document["volumes"]["workload_access_replay"]["driver_opts"]["device"] = source
                expected = [] if "replay" in source else ["must mount the credential folder of replay"]
                self.assertEqual(expected, [problem.split(": ", 1)[1] for problem in access_problems("replay", document)])

    def test_a_group_given_as_a_default_is_read_as_its_default_in_user_and_group_add(self) -> None:
        for label, field, value, expected in (
            ("a default in group_add", "group_add", ["20104", "${X-20102}"], "must run in the group 20104"),
            ("a colon default in group_add", "group_add", ["20104", "${X:-20102}"], "must run in the group 20104"),
            ("a nested default", "group_add", ["20104", "${X:-${Y:-20102}}"], "must run in the group 20104"),
            ("the other group in place of its own", "group_add", ["${X-20102}"], "must run in the group 20104"),
            ("a primary group", "user", "10003:${X-20102}", "must not take a workload group as its primary group"),
            ("a primary group with a colon default", "user", "${U:-10003}:${G:-20104}", "must not take a workload group as its primary group"),
        ):
            with self.subTest(case=label):
                document = self.replay()
                document["services"]["replay"][field] = value
                problems = access_problems("replay", document)
                self.assertEqual(1, len(problems), problems)
                self.assertIn(expected, problems[0])

    def test_a_default_that_is_the_own_value_passes_so_the_forms_are_not_refused_for_being_forms(self) -> None:
        document = self.replay()
        document["services"]["replay"]["user"] = "${U:-10003}:${G-10003}"
        document["services"]["replay"]["group_add"] = ["${X:-20104}"]
        self.assertEqual([], access_problems("replay", document))

    def test_a_variable_with_no_default_in_user_group_add_or_a_bind_source_is_refused_as_not_resolvable(self) -> None:
        forms = ("${X}", "$X", "${X:?no value}", "${X?no value}", "${X:+20104}", "$${X}", "20${X}")
        for form in forms:
            for field in ("user", "group_add", "volume source"):
                with self.subTest(form=form, field=field):
                    document = self.replay()
                    service = document["services"]["replay"]
                    if field == "user":
                        service["user"] = f"10003:{form}"
                    elif field == "group_add":
                        service["group_add"] = [form]
                    else:
                        service["volumes"].append(f"{form}/spark:/elsewhere:ro")
                    problems = access_problems("replay", document)
                    unresolvable = [problem for problem in problems if "this field must be a literal or a resolvable default" in problem]
                    self.assertEqual(1, len(unresolvable), problems)

    def test_only_the_host_rendered_group_variable_of_its_own_service_may_have_no_default(self) -> None:
        code_location = copy.deepcopy(load(SERVICES / "dagster_code_location.yml"))
        self.assertEqual([], access_problems("dagster_code_location", code_location))
        for variable in HOST_RENDERED_GROUPS["dagster_code_location"]:
            self.assertIn(f"{variable}=", HOST_SETTINGS_RENDERER.read_text())
        code_location["services"]["dagster_code_location"]["group_add"] = ["20105", "${OTHER_GID:?no value}"]
        problems = access_problems("dagster_code_location", code_location)
        self.assertEqual(1, len([problem for problem in problems if "this field must be a literal or a resolvable default" in problem]), problems)
        document = self.replay()
        document["services"]["replay"]["group_add"] = ["20104", "${DOCKER_SOCKET_GID:?no value}"]
        problems = access_problems("replay", document)
        self.assertEqual(1, len(problems), problems)
        self.assertIn("this field must be a literal or a resolvable default", problems[0])

    def test_a_bind_of_a_folder_that_includes_the_credential_root_is_named_for_every_service_in_every_form(self) -> None:
        for source in (
            "/run",
            "/run/",
            "//run//",
            "/",
            "//",
            "/run/.",
            "/run/workload_access/..",
            "/run/workload_access/replay/../..",
            "${X:-/run}",
            "/var/run",
            "/var/run/",
        ):
            for name, form in (("replay", "short"), ("replay", "long"), ("replay", "named"), ("postgres", "short"), ("postgres", "long")):
                with self.subTest(source=source, service=name, form=form):
                    document = copy.deepcopy(load(SERVICES / f"{name}.yml"))
                    if form == "short":
                        document["services"][name]["volumes"].append(f"{source}:/elsewhere:ro")
                    elif form == "long":
                        document["services"][name]["volumes"].append({"type": "bind", "source": source, "target": "/elsewhere", "read_only": True})
                    else:
                        document["services"][name]["volumes"].append("other:/elsewhere:ro")
                        document["volumes"]["other"] = {"driver_opts": {"type": "none", "o": "bind", "device": source}}
                    problems = access_problems(name, document)
                    self.assertEqual(1, len(problems), problems)
                    self.assertIn("includes the credential root", problems[0])

    def test_var_run_is_the_same_folder_as_run_so_a_credential_folder_under_it_is_named(self) -> None:
        document = self.replay()
        document["services"]["replay"]["volumes"].append("/var/run/workload_access/spark:/elsewhere:ro")
        problems = access_problems("replay", document)
        self.assertEqual(1, len(problems), problems)
        self.assertIn("not spark", problems[0])
        document = self.replay()
        document["volumes"]["workload_access_replay"]["driver_opts"]["device"] = "/var/run/workload_access/replay"
        self.assertEqual([], access_problems("replay", document))

    def test_a_bind_beside_the_credential_root_or_elsewhere_under_run_is_not_named(self) -> None:
        for source in (
            "/runner",
            "/run/workload_accessible",
            "/run/containerd/containerd.sock",
            "/var/run/docker.sock",
            "/var/run/tailscale",
            "/proc",
            "/etc",
            "./run",
        ):
            with self.subTest(source=source):
                document = copy.deepcopy(load(SERVICES / "postgres.yml"))
                document["services"]["postgres"]["volumes"].append(f"{source}:/elsewhere:ro")
                self.assertEqual([], access_problems("postgres", document))

    def test_a_service_that_is_not_in_the_table_has_no_credential_folder_and_no_workload_group(self) -> None:
        changes: tuple[tuple[str, Callable[[dict[str, Any]], object]], ...] = (
            ("a bind of a folder", lambda service: service["volumes"].append("/run/workload_access/replay/:/elsewhere:ro")),
            ("the whole folder", lambda service: service["volumes"].append({"type": "bind", "source": "/run/workload_access", "target": "/elsewhere"})),
            ("a supplementary group", lambda service: service.update(group_add=[str(PROJECT_WORKLOAD_GIDS["replay"])])),
            ("a primary group", lambda service: service.update(user=f"999:{PROJECT_WORKLOAD_GIDS['trino']}")),
        )
        for label, change in changes:
            with self.subTest(change=label):
                document = copy.deepcopy(load(SERVICES / "postgres.yml"))
                self.assertEqual([], access_problems("postgres", document))
                change(document["services"]["postgres"])
                problems = access_problems("postgres", document)
                self.assertEqual(1, len(problems), problems)
                self.assertIn("not in the table", problems[0])


class PlantedDefectTest(unittest.TestCase):
    def good(self) -> dict[str, Any]:
        return copy.deepcopy(load(SERVICES / "postgres.yml"))

    def defect(self, mutate: Any) -> list[str]:
        document = self.good()
        mutate(document, document["services"]["postgres"])
        return service_problems("postgres", document)

    def test_the_real_postgres_file_has_no_problem(self) -> None:
        self.assertEqual([], service_problems("postgres", self.good()))

    def test_a_missing_mem_limit_is_named(self) -> None:
        problems = self.defect(lambda _document, service: service.pop("mem_limit"))
        self.assertEqual(1, len(problems))
        self.assertIn("mem_limit", problems[0])

    def test_a_typed_mem_limit_is_refused_so_a_limit_is_never_typed_twice(self) -> None:
        for value in ("1g", "1024m", "${MEM_LIMIT_POSTGRES:-1024m}", "${MEM_LIMIT_OTHER:?x}", "${MEM_LIMIT_POSTGRES}"):
            with self.subTest(value=value):
                problems = self.defect(lambda _document, service, value=value: service.update(mem_limit=value))
                self.assertEqual(1, len(problems))
                self.assertIn("mem_limit", problems[0])

    def test_a_project_built_image_needs_a_version_tag_and_a_digest_is_optional_until_it_is_pushed(self) -> None:
        digest = "@sha256:" + "a" * 64
        for image in ("surgical-risk-dagster:1.13.31", f"surgical-risk-dagster:1.13.31{digest}"):
            with self.subTest(image=image):
                self.assertEqual([], self.defect(lambda _document, service, image=image: service.update(image=image)))
        for image in ("surgical-risk-dagster", "surgical-risk-dagster:latest", f"surgical-risk-dagster:latest{digest}", "other-dagster:1.13.31"):
            with self.subTest(image=image):
                problems = self.defect(lambda _document, service, image=image: service.update(image=image))
                self.assertEqual(1, len(problems))
                self.assertIn("image", problems[0])

    def test_an_image_without_a_digest_or_with_latest_is_named(self) -> None:
        for image in ("postgres:18.7", "postgres@sha256:" + "a" * 64, "postgres:latest@sha256:" + "a" * 64, "postgres:18.7@sha256:abc"):
            with self.subTest(image=image):
                problems = self.defect(lambda _document, service, image=image: service.update(image=image))
                self.assertEqual(1, len(problems))
                self.assertIn("image", problems[0])

    def test_a_missing_or_other_platform_is_named(self) -> None:
        for mutate in (lambda _d, s: s.pop("platform"), lambda _d, s: s.update(platform="linux/arm64")):
            problems = self.defect(mutate)
            self.assertEqual(1, len(problems))
            self.assertIn("platform", problems[0])

    def test_a_healthcheck_missing_any_field_is_named(self) -> None:
        for field in HEALTHCHECK_FIELDS:
            with self.subTest(field=field):
                problems = self.defect(lambda _document, service, field=field: service["healthcheck"].pop(field))
                self.assertEqual(1, len(problems))
                self.assertIn("healthcheck", problems[0])

    def test_a_restart_policy_that_does_not_restart_is_named(self) -> None:
        for mutate in (lambda _d, s: s.pop("restart"), lambda _d, s: s.update(restart="no")):
            problems = self.defect(mutate)
            self.assertEqual(1, len(problems))
            self.assertIn("restart", problems[0])

    def test_a_port_on_every_address_or_the_host_network_is_named(self) -> None:
        for port in ("5432:5432", "0.0.0.0:5432:5432", "${POSTGRES_HOST_PORT:-5432}:5432"):
            with self.subTest(port=port):
                problems = self.defect(lambda _document, service, port=port: service.update(ports=[port]))
                self.assertEqual(1, len(problems))
                self.assertIn("127.0.0.1", problems[0])
        problems = self.defect(lambda _document, service: service.update(network_mode="host"))
        self.assertIn("network_mode", problems[0])

    def test_data_outside_a_declared_named_volume_is_named(self) -> None:
        for volumes in ([], ["/srv/data:/var/lib/postgresql"], ["undeclared:/var/lib/postgresql"]):
            with self.subTest(volumes=volumes):
                problems = self.defect(lambda _document, service, volumes=volumes: service.update(volumes=volumes))
                self.assertEqual(1, len(problems))
                self.assertIn("named volumes", problems[0])

    def test_a_password_value_in_the_environment_is_named_and_a_file_reference_is_not(self) -> None:
        problems = self.defect(lambda _document, service: service["environment"].update({"POSTGRES_PASSWORD": "x"}))
        self.assertEqual(1, len(problems))
        self.assertIn("POSTGRES_PASSWORD", problems[0])
        self.assertEqual([], self.defect(lambda _document, service: service["environment"].update({"OTHER_PASSWORD_FILE": "/run/secrets/x"})))

    def test_a_secret_that_the_file_does_not_declare_is_named(self) -> None:
        problems = self.defect(lambda document, _service: document.pop("secrets"))
        self.assertEqual(1, len(problems))
        self.assertIn("not declared", problems[0])

    def test_a_service_named_differently_from_its_file_is_named(self) -> None:
        document = self.good()
        document["services"]["other"] = document["services"].pop("postgres")
        self.assertIn("exactly one service", service_problems("postgres", document)[0])


if __name__ == "__main__":
    unittest.main()
