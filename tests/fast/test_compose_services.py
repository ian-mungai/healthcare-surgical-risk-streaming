"""Every Compose service file holds the properties that the design requires, and the limits file and the Compose files name the same services.

The first group reads the real files, so a later service is held to the same rules. The second group plants one defect at a time in a copy of
the PostgreSQL service and expects the checker to name that defect, so a check that stopped looking would fail here.
"""

from __future__ import annotations

import copy
import re
import unittest
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
        self.assertEqual(["${DOCKER_SOCKET_GID:?write the host settings first}"], service["group_add"])
        self.assertEqual([{"path": "../host.env", "required": False}], service["env_file"])

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


CREDENTIAL_READERS = {"spark": ("spark", 20102), "trino": ("trino", 20103), "replay": ("replay", 20104)}
"""The services that read a credential folder, each bound to its folder and its workload group. The table is fixed here, not derived from the mount, so a
change of folder and group together is still a change this test names."""


class WorkloadAccessTest(unittest.TestCase):
    """A service that reads a workload's short-lived AWS credentials mounts that workload's folder read-only and runs in its group: the helper writes the
    folder for root and the workload's group, mode 0750, and the credentials 0640, so a container outside the group cannot read them."""

    def problems(self, name: str, document: dict[str, Any]) -> list[str]:
        service = document["services"][name]
        found = []
        folders = []
        for volume_name, volume in (document.get("volumes") or {}).items():
            device = str(((volume or {}).get("driver_opts") or {}).get("device", ""))
            match = re.search(r"/run/workload_access/([a-z_]+)\}?$", device)
            if not match:
                continue
            workload = match.group(1)
            folders.append(workload)
            if f"{volume_name}:/run/workload_access:ro" not in [str(entry) for entry in service.get("volumes") or []]:
                found.append(f"{name}: the credential folder of {workload} must be mounted read-only at /run/workload_access")
        if folders and name not in CREDENTIAL_READERS:
            found.append(f"{name}: is not in the table of services that may read a credential folder")
        for workload in folders:
            if name in CREDENTIAL_READERS and workload != CREDENTIAL_READERS[name][0]:
                found.append(f"{name}: may read only the credential folder of {CREDENTIAL_READERS[name][0]}, not {workload}")
        if folders and name in CREDENTIAL_READERS and [str(CREDENTIAL_READERS[name][1])] != [str(group) for group in service.get("group_add") or []]:
            found.append(f"{name}: must run in the group {CREDENTIAL_READERS[name][1]} of the workload {CREDENTIAL_READERS[name][0]} and in no other group")
        return found

    def test_every_service_with_a_credential_folder_mounts_it_read_only_and_runs_in_its_group(self) -> None:
        readers = []
        for path in sorted(SERVICES.glob("*.yml")):
            with self.subTest(service=path.stem):
                self.assertEqual([], self.problems(path.stem, load(path)))
            if "/run/workload_access/" in path.read_text():
                readers.append(path.stem)
        self.assertEqual(sorted(CREDENTIAL_READERS), readers)

    def test_the_fixed_table_agrees_with_the_project_workload_groups(self) -> None:
        for name, (workload, group) in CREDENTIAL_READERS.items():
            self.assertEqual(group, PROJECT_WORKLOAD_GIDS[workload], name)

    def test_a_folder_and_group_changed_together_is_named(self) -> None:
        document = copy.deepcopy(load(SERVICES / "replay.yml"))
        service = document["services"]["replay"]
        service["group_add"] = [str(PROJECT_WORKLOAD_GIDS["spark"])]
        document["services"]["replay"]["volumes"] = [str(entry).replace("workload_access_replay", "workload_access_spark") for entry in service["volumes"]]
        document["volumes"] = {"workload_access_spark": {"driver_opts": {"device": "${SPARK_ACCESS_DIR:-/run/workload_access/spark}"}}}
        problems = self.problems("replay", document)
        self.assertEqual(2, len(problems), problems)
        self.assertIn("may read only the credential folder of replay", problems[0])

    def test_a_missing_group_another_group_or_a_writable_mount_is_named(self) -> None:
        for change in ("no group", "other group", "writable"):
            document = copy.deepcopy(load(SERVICES / "replay.yml"))
            service = document["services"]["replay"]
            if change == "no group":
                del service["group_add"]
            elif change == "other group":
                service["group_add"] = [str(PROJECT_WORKLOAD_GIDS["spark"])]
            else:
                service["volumes"] = [str(entry).removesuffix(":ro") for entry in service["volumes"]]
            with self.subTest(change=change):
                self.assertEqual(1, len(self.problems("replay", document)))


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
