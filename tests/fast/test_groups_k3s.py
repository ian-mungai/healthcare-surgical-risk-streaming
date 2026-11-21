"""The control of Airbyte's workloads through the Kubernetes API, held to its failure modes without a cluster.

Isolation is necessary here: k3s runs on the host only, so these cases use an in-memory stand-in for the API and the real client logic. The real cases are
in ``tests/e2e/stack_groups.py host``. The failures listed before the code was written: the API does not answer, the token is refused (401 or 403), a workload
does not exist (404), the answer is malformed, a workload never becomes ready, a stopped workload leaves a pod, one workload of several fails to scale, and a
token or an answer's text reaching a message.
"""

from __future__ import annotations

import tempfile
import unittest
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from scripts.stack.groups import GroupError
from scripts.stack.groups_k3s import (
    ONE_OFF,
    SCALED,
    HttpsTransport,
    KubernetesConfig,
    KubernetesControl,
    config_from_environment,
    object_name,
)
from scripts.stack.limits import parse_limits

NAMESPACE = "airbyte_ns"
PLANTED_VALUE = "value-that-must-never-appear"
BODY_TEXT = "body-text-that-must-never-appear"
LIMITS_OF_THE_PLATFORM = """\
host_reserve_mib: 4096
services:
  airbyte_server: {runtime: k3s, group: ingestion, memory_limit_mib: 1024}
  airbyte_worker: {runtime: k3s, group: ingestion, memory_limit_mib: 1024}
  airbyte_workload_launcher: {runtime: k3s, group: ingestion, memory_limit_mib: 512}
  airbyte_workload_api_server: {runtime: k3s, group: ingestion, memory_limit_mib: 512}
  airbyte_cron: {runtime: k3s, group: ingestion, memory_limit_mib: 512}
  airbyte_manifest_server: {runtime: k3s, group: ingestion, memory_limit_mib: 512}
  airbyte_bootloader: {runtime: k3s, group: ingestion, memory_limit_mib: 512}
  airbyte_temporal: {runtime: k3s, group: ingestion, memory_limit_mib: 1024}
  airbyte_temporal_admin: {runtime: k3s, group: ingestion, memory_limit_mib: 256}
  airbyte_minio: {runtime: k3s, group: ingestion, memory_limit_mib: 1024}
  airbyte_minio_create_bucket: {runtime: k3s, group: ingestion, memory_limit_mib: 128}
  airbyte_job_connector: {runtime: k3s, group: ingestion, memory_limit_mib: 1024}
  airbyte_job_check: {runtime: k3s, group: ingestion, memory_limit_mib: 512}
  airbyte_job_discover: {runtime: k3s, group: ingestion, memory_limit_mib: 512}
  airbyte_job_replication: {runtime: k3s, group: ingestion, memory_limit_mib: 1024}
  airbyte_job_sidecar: {runtime: k3s, group: ingestion, memory_limit_mib: 256}
"""


class Cluster:
    """A stand-in for the Kubernetes API: workloads with a desired and a ready count, pods with a phase, and planted failures by path."""

    def __init__(self, release: str = "airbyte", workloads: Mapping[str, str] | None = None) -> None:
        kinds = workloads if workloads is not None else {f"{release}-{name.removeprefix('airbyte_').replace('_', '-')}": kind for name, kind in SCALED.items()}
        self.objects: dict[tuple[str, str], dict[str, Any]] = {(kind, name): {"desired": 1, "ready": 1} for name, kind in kinds.items()}
        self.pods: dict[str, str] = {}
        self.fail: dict[str, int] = {}
        self.ready_after_scale = True
        self.pods_end_after_scale = True
        self.calls: list[tuple[str, str]] = []

    def request(self, method: str, path: str, body: Mapping[str, Any] | None = None) -> tuple[int, Any]:
        self.calls.append((method, path))
        for part, status in self.fail.items():
            if part in path:
                return status, {"message": BODY_TEXT}
        parts = path.split("/")
        if path.endswith("/pods"):
            return 200, {"items": [{"metadata": {"name": name}, "status": {"phase": phase}} for name, phase in sorted(self.pods.items())]}
        kind, name = parts[6], parts[7]
        found = self.objects.get((kind, name))
        if found is None:
            return 404, {"message": BODY_TEXT}
        if method == "PATCH":
            if body is None or parts[-1] != "scale":
                raise ValueError("a patch must carry a body and address the scale")
            found["desired"] = body["spec"]["replicas"]
            if self.ready_after_scale:
                found["ready"] = found["desired"]
            if found["desired"] == 0 and self.pods_end_after_scale:
                self.pods = {pod: phase for pod, phase in self.pods.items() if not pod.startswith(f"{name}-")}
            return 200, {"spec": {"replicas": found["desired"]}}
        report: dict[str, Any] = {"replicas": found["desired"]}
        if found["ready"]:
            report["readyReplicas"] = found["ready"]
        return 200, {"spec": {"replicas": found["desired"]}, "status": report}


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def control(cluster: Cluster, release: str = "airbyte") -> tuple[KubernetesControl, Clock]:
    clock = Clock()
    config = KubernetesConfig("https://k3s_api:6443", NAMESPACE, release, Path("/run/secrets/k3s_token"), Path("/run/secrets/k3s_ca"))
    return KubernetesControl(config, cluster, clock=clock, sleep=clock.sleep), clock


GROUP = tuple(sorted(parse_limits(LIMITS_OF_THE_PLATFORM).services))
SCALED_ENTRIES = tuple(name for name in GROUP if name in SCALED)


class MappingTest(unittest.TestCase):
    def test_every_k3s_entry_of_the_platform_is_scaled_or_left_alone_and_never_both(self) -> None:
        self.assertEqual(set(GROUP), set(SCALED) | ONE_OFF)
        self.assertEqual(set(), set(SCALED) & ONE_OFF)

    def test_an_object_is_named_by_the_release_and_the_entry_with_dashes(self) -> None:
        self.assertEqual("airbyte-workload-api-server", object_name("airbyte", "airbyte_workload_api_server"))
        self.assertEqual("stage-server", object_name("stage", "airbyte_server"))

    def test_the_object_store_is_a_stateful_set_and_the_other_scaled_workloads_are_deployments(self) -> None:
        self.assertEqual("statefulsets", SCALED["airbyte_minio"])
        self.assertEqual({"deployments"}, {kind for name, kind in SCALED.items() if name != "airbyte_minio"})


class ConfigTest(unittest.TestCase):
    def test_no_namespace_means_no_control(self) -> None:
        self.assertIsNone(config_from_environment({}))

    def test_the_defaults_are_the_release_airbyte_and_the_secret_files_of_the_container(self) -> None:
        config = config_from_environment({"STACK_K3S_NAMESPACE": "airbyte"})
        if config is None:
            self.fail("the namespace is named, so there must be a configuration")
        self.assertEqual(("https://k3s_api:6443", "airbyte", "airbyte"), (config.api, config.namespace, config.release))
        self.assertEqual((Path("/run/secrets/k3s_token"), Path("/run/secrets/k3s_ca")), (config.token_file, config.ca_file))

    def test_a_value_that_is_not_a_name_or_an_https_address_is_refused(self) -> None:
        for key, value in (
            ("STACK_K3S_NAMESPACE", "a/b"),
            ("STACK_K3S_NAMESPACE", "A"),
            ("STACK_K3S_RELEASE", "a b"),
            ("STACK_K3S_API", "http://k3s_api:6443"),
        ):
            with self.subTest(key=key, value=value), self.assertRaisesRegex(GroupError, key):
                config_from_environment({"STACK_K3S_NAMESPACE": "airbyte", key: value})


class StartTest(unittest.TestCase):
    def test_start_scales_each_scaled_workload_to_one_and_leaves_the_one_off_entries_alone(self) -> None:
        cluster = Cluster()
        for found in cluster.objects.values():
            found.update(desired=0, ready=0)
        kubernetes, _ = control(cluster)
        kubernetes.start(GROUP, 60)
        self.assertEqual({1}, {found["desired"] for found in cluster.objects.values()})
        patched = [path for method, path in cluster.calls if method == "PATCH"]
        self.assertEqual(len(SCALED), len(patched))
        self.assertTrue(all(path.endswith("/scale") for path in patched))

    def test_a_workload_that_does_not_exist_is_skipped_and_the_others_are_started(self) -> None:
        cluster = Cluster()
        del cluster.objects[("statefulsets", "airbyte-minio")]
        for found in cluster.objects.values():
            found.update(desired=0, ready=0)
        kubernetes, _ = control(cluster)
        kubernetes.start(GROUP, 60)
        self.assertEqual({1}, {found["desired"] for found in cluster.objects.values()})

    def test_a_group_with_none_of_its_workloads_in_the_cluster_is_refused_by_name(self) -> None:
        kubernetes, _ = control(Cluster(workloads={}))
        with self.assertRaisesRegex(GroupError, "none of the workloads.*airbyte_ns"):
            kubernetes.start(GROUP, 60)

    def test_a_workload_that_never_becomes_ready_fails_at_the_deadline_naming_it(self) -> None:
        cluster = Cluster()
        cluster.ready_after_scale = False
        for found in cluster.objects.values():
            found.update(desired=0, ready=0)
        kubernetes, clock = control(cluster)
        with self.assertRaisesRegex(GroupError, r"airbyte-cron: ready 0 of 1") as caught:
            kubernetes.start(GROUP, 30)
        self.assertGreaterEqual(clock.now, 30)
        self.assertIn("after 30 seconds", str(caught.exception))

    def test_a_refused_token_fails_naming_the_workload_and_the_status_and_no_text_of_the_answer(self) -> None:
        for status in (401, 403, 500):
            cluster = Cluster()
            cluster.fail["airbyte-worker/scale"] = status
            kubernetes, _ = control(cluster)
            with self.subTest(status=status), self.assertRaises(GroupError) as caught:
                kubernetes.start(GROUP, 60)
            self.assertIn(f"airbyte-worker: HTTP {status}", str(caught.exception))
            self.assertNotIn(BODY_TEXT, str(caught.exception))


class StopTest(unittest.TestCase):
    def test_stop_scales_to_zero_and_waits_until_the_pods_are_gone(self) -> None:
        cluster = Cluster()
        cluster.pods = {"airbyte-worker-abc": "Running", "airbyte-server-def": "Running"}
        kubernetes, _ = control(cluster)
        kubernetes.stop(GROUP, 60)
        self.assertEqual({0}, {found["desired"] for found in cluster.objects.values()})
        self.assertEqual({}, cluster.pods)

    def test_a_pod_that_does_not_end_fails_at_the_deadline_naming_the_pod_and_its_phase(self) -> None:
        cluster = Cluster()
        cluster.pods = {"airbyte-worker-abc": "Running"}
        cluster.pods_end_after_scale = False
        kubernetes, clock = control(cluster)
        with self.assertRaisesRegex(GroupError, r"airbyte-worker-abc \(Running\)"):
            kubernetes.stop(GROUP, 30)
        self.assertGreaterEqual(clock.now, 30)

    def test_a_job_pod_of_a_sync_is_waited_for_and_a_finished_pod_is_not(self) -> None:
        cluster = Cluster()
        cluster.pods = {"replication-job-1-attempt-0": "Running", "airbyte-bootloader": "Succeeded", "replication-job-0-attempt-0": "Failed"}
        cluster.pods_end_after_scale = False
        kubernetes, _ = control(cluster)
        with self.assertRaises(GroupError) as caught:
            kubernetes.stop(GROUP, 30)
        self.assertIn("replication-job-1-attempt-0 (Running)", str(caught.exception))
        self.assertNotIn("bootloader", str(caught.exception))
        self.assertNotIn("replication-job-0", str(caught.exception))

    def test_a_scale_that_fails_for_one_workload_still_scales_the_others_and_then_fails(self) -> None:
        cluster = Cluster()
        cluster.fail["airbyte-cron/scale"] = 403
        kubernetes, _ = control(cluster)
        with self.assertRaisesRegex(GroupError, "airbyte-cron: HTTP 403"):
            kubernetes.stop(GROUP, 60)
        stopped = sorted(name for (_, name), found in cluster.objects.items() if found["desired"] == 0)
        self.assertEqual(len(SCALED) - 1, len(stopped))


class RunningTest(unittest.TestCase):
    def test_a_workload_runs_when_it_has_a_ready_replica_and_the_one_off_entries_never_run(self) -> None:
        cluster = Cluster()
        cluster.objects[("deployments", "airbyte-cron")].update(desired=0, ready=0)
        del cluster.objects[("statefulsets", "airbyte-minio")]
        kubernetes, _ = control(cluster)
        found = kubernetes.running(GROUP)
        self.assertNotIn("airbyte_cron", found)
        self.assertNotIn("airbyte_minio", found)
        self.assertEqual(set(SCALED) - {"airbyte_cron", "airbyte_minio"}, set(found))

    def test_an_answer_that_fails_is_an_error_and_not_an_empty_set(self) -> None:
        cluster = Cluster()
        cluster.fail["airbyte-server"] = 500
        kubernetes, _ = control(cluster)
        with self.assertRaisesRegex(GroupError, "airbyte-server: HTTP 500"):
            kubernetes.running(GROUP)

    def test_an_answer_with_a_wrong_type_is_an_error(self) -> None:
        cluster = Cluster()
        cluster.objects[("deployments", "airbyte-server")]["ready"] = "1"
        kubernetes, _ = control(cluster)
        with self.assertRaisesRegex(GroupError, "airbyte-server: the answer is malformed"):
            kubernetes.running(GROUP)


class TokenTest(unittest.TestCase):
    def test_the_token_file_is_read_into_the_bearer_header_and_no_message_holds_it(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            token_file = Path(folder) / "token"
            token_file.write_text(f"{PLANTED_VALUE}\n")
            config = KubernetesConfig("https://k3s_api:6443", NAMESPACE, "airbyte", token_file, Path(folder) / "ca.crt")
            self.assertEqual(f"Bearer {PLANTED_VALUE}", HttpsTransport(config).authorization())
            token_file.write_text("")
            with self.assertRaises(GroupError) as empty:
                HttpsTransport(config).authorization()
            self.assertNotIn(PLANTED_VALUE, str(empty.exception))
            with self.assertRaises(GroupError) as missing:
                HttpsTransport(KubernetesConfig(config.api, NAMESPACE, "airbyte", Path(folder) / "none", config.ca_file)).authorization()
            self.assertIn("none", str(missing.exception))


if __name__ == "__main__":
    unittest.main()
