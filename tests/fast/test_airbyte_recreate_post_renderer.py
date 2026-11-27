"""The Helm post-renderer that gives the Airbyte deployments holding database connections the Recreate strategy in the manifests themselves.

The script is run as a process with the manifests on its standard input, as Helm runs it. The documents below have the shapes that chart 2.5.0 renders for each
deployment (tests/slow/test_airbyte_rendered_environment.py renders the real release and parses the result): no strategy, a rolling update with a comment inside
it, Recreate with a trailing comment, and a deployment that the script must leave alone.
"""

from __future__ import annotations

import unittest
from pathlib import Path
from typing import Any

import yaml

from scripts.process import run_command
from scripts.project_config import ROOT

SCRIPT = ROOT / "ansible" / "roles" / "airbyte" / "files" / "recreate_post_renderer.py"
PYTHON = str(ROOT / ".venv" / "bin" / "python")
NAMES = "temporal,cron,worker,workload-api-server"
PLAIN = """\
---
# Source: airbyte/templates/airbyte-{name}/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  namespace: airbyte
  name: airbyte-{name}
  annotations:

    {{}}
spec:
  replicas: 1
  selector:
    matchLabels:

      airbyte: {name}
  template:
    metadata:
      labels:
        airbyte: {name}
    spec:
      containers:
      - name: main
        image: example/{name}:1
"""
ROLLING = """\
---
# Source: airbyte/templates/airbyte-workload-api-server/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  namespace: airbyte
  name: airbyte-workload-api-server
spec:
  minReadySeconds: 30
  replicas: 1
  selector:
    matchLabels:
      airbyte: workload-api-server
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 0
      # maxSurge: 100% doubles the pod count during a rollout, which pulls in
      # new nodes that the cluster-autoscaler then reclaims ~10min later,
      # evicting every replica at once. Surge one pod at a time instead.
      maxSurge: 1
  template:
    metadata:
      labels:
        airbyte: workload-api-server
    spec:
      strategy: not-a-deployment-strategy
      containers:
      - name: main
"""
PLAIN_ROLLING = """\
---
# Source: airbyte/templates/airbyte-manifest-server/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  namespace: airbyte
  name: airbyte-manifest-server
spec:
  replicas: 1
  strategy:
    type: RollingUpdate
  template:
    spec:
      containers:
      - name: main
"""
RECREATE_COMMENT = """\
---
# Source: airbyte/templates/airbyte-server/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  namespace: airbyte
  name: airbyte-server
spec:
  minReadySeconds: 30
  replicas: 1
  strategy:
    type: Recreate # Needed due to volume claims
  template:
    spec:
      containers:
      - name: main
"""
CONFIG_MAP = """\
---
# Source: airbyte/templates/config.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  namespace: airbyte
  name: airbyte-temporal
data:
  note: |
    kind: Deployment
    spec:
      strategy:
        type: RollingUpdate
"""
FLOW_STYLE = PLAIN.format(name="worker").replace(
    "spec:\n  replicas: 1\n", "spec:\n  replicas: 1\n  strategy: {type: RollingUpdate, rollingUpdate: {maxSurge: 1}}\n"
)
CHART = CONFIG_MAP + PLAIN.format(name="temporal") + PLAIN.format(name="cron") + PLAIN.format(name="worker") + ROLLING + PLAIN_ROLLING + RECREATE_COMMENT
REFUSAL = "recreate_post_renderer: the manifests could not be given the Recreate strategy"


def render(text: str, release: str = "airbyte", names: str = NAMES) -> tuple[int, str, str]:
    result = run_command(PYTHON, ["-I", str(SCRIPT), release, names], cwd=ROOT, input_text=text, timeout=30)
    return result.returncode, result.stdout, result.stderr


def documents(text: str) -> dict[str, Any]:
    return {item["metadata"]["name"]: item for item in yaml.safe_load_all(text) if item}


class PostRendererTest(unittest.TestCase):
    def test_each_listed_deployment_gets_exactly_the_recreate_strategy_whatever_the_chart_stated(self) -> None:
        code, output, error = render(CHART)
        self.assertEqual((0, ""), (code, error))
        found = documents(output)
        for name in ("temporal", "cron", "worker", "workload-api-server"):
            with self.subTest(name=name):
                self.assertEqual({"type": "Recreate"}, found[f"airbyte-{name}"]["spec"]["strategy"])

    def test_nothing_else_changes_in_a_deployment_that_is_changed_and_a_deployment_or_a_config_map_that_is_not_listed_is_left_byte_for_byte(self) -> None:
        _, output, _ = render(CHART)
        before, after = documents(CHART), documents(output)
        for name, document in before.items():
            expected = {key: value for key, value in document.items()}
            if name in {f"airbyte-{item}" for item in NAMES.split(",")}:
                spec = dict(document["spec"])
                spec.pop("strategy", None)
                after_spec = dict(after[name]["spec"])
                self.assertEqual({"type": "Recreate"}, after_spec.pop("strategy"))
                self.assertEqual(spec, after_spec, name)
                expected["spec"] = after[name]["spec"]
            self.assertEqual(expected, after[name], name)
        for untouched in (CONFIG_MAP, PLAIN_ROLLING, RECREATE_COMMENT):
            self.assertIn(untouched, output)

    def test_a_strategy_inside_a_pod_template_or_a_comment_is_not_taken_for_the_deployments_strategy(self) -> None:
        _, output, _ = render(ROLLING)
        found = documents(output)["airbyte-workload-api-server"]
        self.assertEqual("not-a-deployment-strategy", found["spec"]["template"]["spec"]["strategy"])
        self.assertNotIn("maxSurge: 1", output)
        self.assertNotIn("cluster-autoscaler", output, "the comment that belonged to the removed rolling update goes with it")

    def test_a_strategy_written_as_one_flow_line_is_replaced(self) -> None:
        _, output, _ = render(FLOW_STYLE, names="worker")
        self.assertEqual({"type": "Recreate"}, documents(output)["airbyte-worker"]["spec"]["strategy"])

    def test_the_output_is_the_same_when_it_is_rendered_again_so_a_second_pass_changes_nothing(self) -> None:
        _, once, _ = render(CHART)
        code, twice, _ = render(once)
        self.assertEqual((0, once), (code, twice))

    def test_the_release_name_is_part_of_the_deployments_name_and_a_deployment_that_the_chart_lacks_is_skipped(self) -> None:
        _, output, _ = render(CHART, release="other")
        self.assertEqual(CHART, output, "no deployment is named other-<name>, so nothing changes")
        code, output, _ = render(CHART, names="temporal,does-not-exist")
        self.assertEqual(0, code)
        found = documents(output)
        self.assertEqual({"type": "Recreate"}, found["airbyte-temporal"]["spec"]["strategy"])
        self.assertNotIn("strategy", found["airbyte-cron"]["spec"])

    def test_a_listed_deployment_with_no_spec_or_an_unreadable_metadata_stops_with_fixed_text_and_no_manifest(self) -> None:
        broken = PLAIN.format(name="temporal").replace("spec:\n", "specification:\n", 1)
        for label, text in (("no spec", broken), ("tab indent", PLAIN.format(name="temporal").replace("  replicas: 1", "\treplicas: 1", 1))):
            with self.subTest(case=label):
                code, output, error = render(text, names="temporal")
                self.assertEqual((3, "", REFUSAL), (code, output, error.strip()))
                self.assertNotIn("temporal", error)

    def test_wrong_arguments_stop_with_fixed_text_and_no_manifest(self) -> None:
        for arguments in ([], ["airbyte"], ["airbyte", "a", "b"], ["Air Byte", "temporal"], ["airbyte", "temporal,;rm"], ["airbyte", ""]):
            with self.subTest(arguments=arguments):
                result = run_command(PYTHON, ["-I", str(SCRIPT), *arguments], cwd=ROOT, input_text=CHART, timeout=30)
                self.assertEqual((2, ""), (result.returncode, result.stdout))
                self.assertEqual("recreate_post_renderer: usage: recreate_post_renderer.py <release> <deployment>[,<deployment>...]", result.stderr.strip())

    def test_the_script_uses_only_the_standard_library_and_runs_in_isolated_mode_as_the_host_python(self) -> None:
        text = Path(SCRIPT).read_text()
        self.assertTrue(text.startswith("#!/usr/bin/python3 -I\n"))
        for forbidden in ("import yaml", "from yaml", "import ruamel"):
            self.assertNotIn(forbidden, text)
