"""The three project images keep the vulnerability fixes that sit on top of their official base images.

A fix that a later edit drops shows only at the next scan, so these checks read the tracked Dockerfiles: the Debian security upgrades, the removal of pip (whose
bundled packages carry findings and which no container runs), the Spark jars that are replaced or removed before they enter the final image, and the Compose
plugin that is built from its pinned source with fixed Go modules.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOCKERFILES = {name: ROOT / folder / "Dockerfile" for name, folder in (("dagster", "orchestration"), ("replay", "replay"), ("spark", "spark"))}
# The jars that the Spark distribution holds and the jobs do not need: no job uses a Hive metastore, Kubernetes, YARN, Spark Connect's REPL or a Scala shell.
REMOVED_JARS = (
    "hive-jdbc-",
    "hive-exec-",
    "hive-metastore-",
    "spark-hive_",
    "spark-hive-thriftserver_",
    "derby",
    "libthrift-",
    "kubernetes-",
    "spark-kubernetes_",
    "vertx-",
    "spark-yarn_",
    "jline-3.",
    "netty-codec-classes-quic-",
    "netty-codec-native-quic-",
    "netty-codec-http3-",
)
# Each replaced library with the lowest version that the findings of Nov 25 2026 name as fixed.
FIXED_JARS = {
    "netty-handler": "4.2.17",
    "netty-codec-http": "4.2.17",
    "jackson-core": "2.21.7",
    "jackson-databind": "2.21.7",
    "log4j-core": "2.25.5",
    "guava": "33.7.2",
    "zookeeper": "3.9.5",
    "snappy-java": "1.1.10.9",
    "lz4-java": "1.8.1",
    "parquet-jackson": "1.18.2",
    "hadoop-client-runtime": "3.4.3",
}


def instructions(path: Path) -> list[str]:
    joined = path.read_text().replace("\\\n", " ")
    return [" ".join(line.split()) for line in joined.splitlines() if line.strip() and not line.lstrip().startswith("#")]


def final_stage(path: Path) -> list[str]:
    found = instructions(path)
    start = max(index for index, line in enumerate(found) if line.startswith("FROM "))
    return found[start:]


def version_key(text: str) -> tuple[int, ...]:
    return tuple(int(part) for part in re.findall(r"\d+", text))


class EveryImageTest(unittest.TestCase):
    def test_the_final_stage_installs_the_debian_upgrades_and_removes_pip_and_its_bundled_wheel(self) -> None:
        for name, path in DOCKERFILES.items():
            text = "\n".join(final_stage(path))
            with self.subTest(image=name):
                self.assertRegex(text, r"apt-get update && apt-get upgrade --yes")
                self.assertIn("rm -rf /var/lib/apt/lists/*", text)
                self.assertIn("python -m pip uninstall --yes pip", text)
                self.assertIn("/usr/local/lib/python3.14/ensurepip/_bundled/pip-", text)
                self.assertLess(text.index("--requirement"), text.index("pip uninstall"), "the requirements are installed before pip goes")


class SparkJarTest(unittest.TestCase):
    def test_each_unneeded_jar_is_removed_in_the_stage_before_the_final_one(self) -> None:
        text = DOCKERFILES["spark"].read_text()
        final = final_stage(DOCKERFILES["spark"])
        before = text[: text.rindex("\nFROM ")]
        for prefix in REMOVED_JARS:
            with self.subTest(jar=prefix):
                self.assertIn(f"/opt/spark/jars/{prefix}", before, "a removal in the final stage would leave the jar in an earlier layer")
                self.assertFalse([line for line in final if line.startswith("ADD ") and f"/{prefix}" in line])
        for folder in ("/opt/spark/jars/connect-repl", "/opt/spark/yarn", "/opt/spark/kubernetes"):
            with self.subTest(folder=folder):
                self.assertIn(folder, before)

    def test_each_replaced_library_is_added_at_a_fixed_version_with_its_checksum(self) -> None:
        downloads = [line for line in final_stage(DOCKERFILES["spark"]) if line.startswith("ADD ")]
        for artifact, lowest in FIXED_JARS.items():
            with self.subTest(artifact=artifact):
                found = [match for line in downloads if (match := re.search(rf"/{re.escape(artifact)}/([0-9][^/]*)/{re.escape(artifact)}-", line))]
                self.assertEqual(1, len(found), artifact)
                self.assertGreaterEqual(version_key(found[0].group(1)), version_key(lowest))
        for line in downloads:
            self.assertRegex(line, r"^ADD --checksum=sha256:(\$\{\w+\}|[0-9a-f]{64}) https://|^ADD --checksum=sha256:\$\{\w+\} \$\{")


class ComposePluginTest(unittest.TestCase):
    def test_the_plugin_is_built_from_the_checksum_pinned_source_with_the_fixed_modules(self) -> None:
        text = "\n".join(instructions(DOCKERFILES["dagster"]))
        self.assertRegex(text, r"FROM --platform=\$BUILDPLATFORM golang:[0-9.]+-alpine[0-9.]+@sha256:[0-9a-f]{64} AS compose_build")
        self.assertRegex(text, r"ADD --checksum=sha256:[0-9a-f]{64} https://github\.com/docker/compose/archive/refs/tags/v[0-9.]+\.tar\.gz ")
        for module in (
            "golang.org/x/net@v0.62.0",
            "golang.org/x/crypto@v0.59.0",
            "github.com/klauspost/compress@v1.20.2",
            "go.opentelemetry.io/otel@v1.47.0",
            "go.opentelemetry.io/otel/sdk@v1.47.0",
            "go.opentelemetry.io/otel/trace@v1.47.0",
        ):
            with self.subTest(module=module):
                self.assertIn(module, text)
        self.assertIn("GOOS=linux GOARCH=amd64", text)
        self.assertIn("COPY --from=compose_build /out/docker-compose /usr/local/libexec/docker/cli-plugins/docker-compose", text)


if __name__ == "__main__":
    unittest.main()
