"""The three project images keep the vulnerability fixes that sit on top of their official base images.

A fix that a later edit drops shows only at the next scan, so these checks read the tracked Dockerfiles: the Debian security upgrades, the removal of pip (whose
bundled packages carry findings and which no container runs), the Spark jars that are replaced or removed before they enter the final image, and the Compose
plugin that is built from its pinned source with fixed Go modules. The step that installs the Debian upgrades also runs here: its shell text runs in /bin/sh
on a copy of the base image's APT files, with a stand-in for apt-get. No test runs Docker or reaches a network, so what the real apt does with those options
shows only at a build.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from scripts.process import run_command

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
# Netty's modules come from one release, with the netty-tcnative release that its POM names: a module left at another release keeps that release's findings.
NETTY_VERSION = "4.2.20.Final"
TCNATIVE_VERSION = "2.0.85.Final"
NETTY_MODULES = (
    *("netty-buffer", "netty-codec", "netty-codec-base", "netty-codec-compression", "netty-codec-dns", "netty-codec-http", "netty-codec-http2"),
    *("netty-codec-marshalling", "netty-codec-protobuf", "netty-codec-socks", "netty-common", "netty-handler", "netty-handler-proxy", "netty-resolver"),
    *("netty-resolver-dns", "netty-transport", "netty-transport-classes-epoll", "netty-transport-classes-io_uring", "netty-transport-classes-kqueue"),
    *("netty-transport-native-unix-common", "netty-transport-native-epoll", "netty-transport-native-io_uring"),
    *("netty-tcnative-classes", "netty-tcnative-boringssl-static"),
)
# Each replaced library with the lowest version that the findings of Nov 25 2026 name as fixed.
FIXED_JARS = {
    "jackson-core": "2.21.7",
    "jackson-databind": "2.21.7",
    "log4j-core": "2.25.5",
    "guava": "33.7.2",
    "zookeeper": "3.9.5",
    "snappy-java": "1.1.10.9",
    "lz4-java": "1.8.1",
    "parquet-jackson": "1.18.2",
    "hadoop-client-api": "3.5.0",
    "hadoop-client-runtime": "3.5.0",
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
                self.assertRegex(text, r"apt-get update --error-on=any .*&& apt-get upgrade --yes")
                self.assertIn("rm -rf /var/lib/apt/lists/*", text)
                self.assertIn("python -m pip uninstall --yes pip", text)
                self.assertIn("/usr/local/lib/python3.14/ensurepip/_bundled/pip-", text)
                self.assertLess(text.index("--requirement"), text.index("pip uninstall"), "the requirements are installed before pip goes")


KEYRING = "/usr/share/keyrings/debian-archive-keyring.pgp"
# /etc/apt/sources.list.d/debian.sources of the pinned base image of the three final stages, as that image holds it (read on Nov 25 2026).
BASE_SOURCES = f"""Types: deb
# http://snapshot.debian.org/archive/debian/20261123T000000Z
URIs: http://deb.debian.org/debian
Suites: trixie trixie-updates
Components: main
Signed-By: {KEYRING}

Types: deb
# http://snapshot.debian.org/archive/debian-security/20261123T000000Z
URIs: http://deb.debian.org/debian-security
Suites: trixie-security
Components: main
Signed-By: {KEYRING}
"""
MAIN_STANZA, SECURITY_STANZA = BASE_SOURCES.split("\n\n")
# The one source file that both apt commands must read.
PINNED_SOURCES = """Types: deb
URIs: https://snapshot.debian.org/archive/debian/{snapshot}
Suites: trixie trixie-updates
Components: main
Signed-By: {keyring}
Check-Valid-Until: no

Types: deb
URIs: https://snapshot.debian.org/archive/debian-security/{snapshot}
Suites: trixie-security
Components: main
Signed-By: {keyring}
Check-Valid-Until: no
"""
# The lines that apt-get indextargets prints for that file: each configured package index, by its address, suite and component (read from apt 3.0.4).
TARGETS = (
    "https://snapshot.debian.org/archive/debian-security/{snapshot}/ trixie-security main",
    "https://snapshot.debian.org/archive/debian/{snapshot}/ trixie main",
    "https://snapshot.debian.org/archive/debian/{snapshot}/ trixie-updates main",
)
# A stand-in for apt-get: it records its arguments and the source file that they name, prints the planted index targets and fails the planted command.
APT_STAND_IN = """#!{python}
import json, os, pathlib, sys

here = pathlib.Path(os.environ["APT_RECORD"])
named = [one.partition("=")[2] for one in sys.argv[1:] if one.startswith("Dir::Etc::sourcelist=")]
read = pathlib.Path(named[-1]).read_text() if named else None
with (here / "calls").open("a") as calls:
    calls.write(json.dumps({{"arguments": sys.argv[1:], "sources": read}}) + "\\n")
if sys.argv[1] == "indextargets":
    sys.stdout.write((here / "targets").read_text())
failing = here / "failing"
sys.exit(100 if failing.exists() and failing.read_text() == sys.argv[1] else 0)
"""


def run_text(path: Path) -> str:
    """The shell text of the one RUN that calls apt-get, with its continued lines joined as Docker joins them."""
    (found,) = [line for line in path.read_text().replace("\\\n", "").splitlines() if line.startswith("RUN ") and "apt-get" in line]
    return found.removeprefix("RUN ")


class DebianSnapshotTest(unittest.TestCase):
    def snapshot_of(self, name: str, path: Path) -> str:
        final = final_stage(path)
        (declared,) = [line for line in final if line.startswith("ARG DEBIAN_SNAPSHOT")]
        self.assertRegex(declared, r"^ARG DEBIAN_SNAPSHOT=20[0-9]{6}T[0-9]{6}Z$", name)
        return declared.partition("=")[2]

    def test_the_three_images_name_one_snapshot_timestamp_and_run_one_apt_step(self) -> None:
        self.assertEqual(1, len({self.snapshot_of(name, path) for name, path in DOCKERFILES.items()}))
        self.assertEqual(1, len({run_text(path) for path in DOCKERFILES.values()}))

    def test_no_other_stage_installs_a_debian_package(self) -> None:
        for name, path in DOCKERFILES.items():
            with self.subTest(image=name):
                self.assertEqual(1, len([line for line in instructions(path) if re.search(r"\bapt(-get)?\b", line)]))


class AptStepTest(unittest.TestCase):
    """The apt step of each image, run in /bin/sh with the absolute paths of the APT folders moved below a temporary folder."""

    def run_step(
        self,
        name: str,
        files: dict[str, str] | None = None,
        targets: tuple[str, ...] = TARGETS,
        failing: str | None = None,
    ) -> tuple[int, list[dict[str, Any]], Path]:
        path = DOCKERFILES[name]
        snapshot = DebianSnapshotTest().snapshot_of(name, path)
        folder = tempfile.TemporaryDirectory(prefix="apt_step_")
        self.addCleanup(folder.cleanup)
        root = Path(folder.name)
        etc, lists, record, programs = root / "etc/apt", root / "var/lib/apt/lists", root / "record", root / "bin"
        for one in (etc / "sources.list.d", lists, record, programs):
            one.mkdir(parents=True)
        (lists / "left_over").write_text("")
        for relative, content in ({"sources.list.d/debian.sources": BASE_SOURCES} if files is None else files).items():
            (etc / relative).write_text(content)
        (record / "targets").write_text("".join(f"{line.format(snapshot=snapshot)}\n" for line in targets))
        if failing is not None:
            (record / "failing").write_text(failing)
        stand_in = programs / "apt-get"
        stand_in.write_text(APT_STAND_IN.format(python=sys.executable))
        stand_in.chmod(0o755)
        script = run_text(path).replace("/etc/apt/", f"{etc}/").replace("/var/lib/apt/lists/", f"{lists}/")
        environment = {"PATH": f"{programs}:{os.environ['PATH']}", "APT_RECORD": str(record), "DEBIAN_SNAPSHOT": snapshot, "LANG": "C"}
        done = run_command("/bin/sh", ["-c", script], env=environment, timeout=60)
        calls = record / "calls"
        return done.returncode, [json.loads(line) for line in calls.read_text().splitlines()] if calls.exists() else [], root

    def test_both_apt_commands_read_one_explicit_file_of_the_three_suites_at_the_snapshot_and_no_other_source(self) -> None:
        for name, path in DOCKERFILES.items():
            with self.subTest(image=name):
                code, calls, root = self.run_step(name)
                self.assertEqual(0, code)
                self.assertEqual(["update", "indextargets", "upgrade"], [call["arguments"][0] for call in calls])
                expected = PINNED_SOURCES.format(snapshot=DebianSnapshotTest().snapshot_of(name, path), keyring=KEYRING)
                for call in calls:
                    arguments = call["arguments"]
                    self.assertIn(f"Dir::Etc::sourcelist={root}/etc/apt/sources.list.d/debian.sources", arguments)
                    self.assertIn("Dir::Etc::sourceparts=-", arguments)
                    self.assertEqual(expected, call["sources"])
                self.assertIn("--error-on=any", calls[0]["arguments"])
                self.assertIn("--yes", calls[2]["arguments"])
                self.assertIn("--no-install-recommends", calls[2]["arguments"])
                self.assertEqual([], list((root / "var/lib/apt/lists").iterdir()))

    def test_a_base_source_set_that_lacks_an_archive_or_holds_an_extra_source_stops_the_build_before_any_apt_call(self) -> None:
        base = "sources.list.d/debian.sources"
        live = "Types: deb\nURIs: http://deb.debian.org/debian\nSuites: sid\nComponents: main\n"
        line = "deb http://deb.debian.org/debian sid main\n"
        cases: dict[str, dict[str, str]] = {
            "no security archive": {base: MAIN_STANZA + "\n"},
            "two main entries and no security archive": {base: MAIN_STANZA + "\n\n" + MAIN_STANZA + "\n"},
            "no main archive": {base: SECURITY_STANZA},
            "an extra live source in debian.sources": {base: f"{BASE_SOURCES}\n{live}"},
            "an extra suite": {base: BASE_SOURCES.replace("Suites: trixie trixie-updates", "Suites: trixie trixie-updates sid")},
            "a source that apt would not verify": {base: BASE_SOURCES + "Trusted: yes\n"},
            "an extra .sources file": {base: BASE_SOURCES, "sources.list.d/extra.sources": live},
            "an extra .list file": {base: BASE_SOURCES, "sources.list.d/extra.list": line},
            "a sources.list": {base: BASE_SOURCES, "sources.list": line},
            "no debian.sources": {},
        }
        for name in DOCKERFILES:
            for case, files in cases.items():
                with self.subTest(image=name, case=case):
                    code, calls, _ = self.run_step(name, files=files)
                    self.assertNotEqual(0, code)
                    self.assertEqual([], calls)

    def test_indexes_other_than_the_three_pinned_ones_stop_the_build_before_the_upgrade(self) -> None:
        security, main, updates = TARGETS
        cases: dict[str, tuple[str, ...]] = {
            "none": (),
            "no security archive": (main, updates),
            "no updates suite": (security, main),
            "no main suite": (security, updates),
            "an extra live source": (*TARGETS, "http://deb.debian.org/debian/ sid main"),
            "plain HTTP": tuple(line.replace("https://", "http://") for line in TARGETS),
            "the live archive for security": (
                security.replace("snapshot.debian.org/archive/debian-security/{snapshot}", "deb.debian.org/debian-security"),
                main,
                updates,
            ),
            "another snapshot": (security.replace("{snapshot}", "20261101T000000Z"), main, updates),
        }
        for name in DOCKERFILES:
            for case, targets in cases.items():
                with self.subTest(image=name, case=case):
                    code, calls, _ = self.run_step(name, targets=targets)
                    self.assertNotEqual(0, code)
                    self.assertEqual(["update", "indextargets"], [call["arguments"][0] for call in calls])

    def test_a_failed_update_runs_nothing_more_and_a_failed_upgrade_fails_the_build(self) -> None:
        for failing, expected in (("update", ["update"]), ("upgrade", ["update", "indextargets", "upgrade"])):
            with self.subTest(failing=failing):
                code, calls, _ = self.run_step("replay", failing=failing)
                self.assertNotEqual(0, code)
                self.assertEqual(expected, [call["arguments"][0] for call in calls])


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


class HadoopTest(unittest.TestCase):
    def test_the_two_hadoop_client_jars_come_from_one_release(self) -> None:
        form = re.compile(r"/org/apache/hadoop/(hadoop-client-(?:api|runtime))/([^/]+)/\1-\2\.jar ")
        found = [match.groups() for line in final_stage(DOCKERFILES["spark"]) if (match := form.search(line))]
        self.assertEqual(["hadoop-client-api", "hadoop-client-runtime"], sorted(name for name, _ in found))
        self.assertEqual(1, len({version for _, version in found}), "the runtime jar is built against the api jar of its own release")


class NettyTest(unittest.TestCase):
    def test_the_whole_netty_set_is_added_from_one_release_with_the_tcnative_release_that_it_names(self) -> None:
        form = re.compile(r"https://repo1\.maven\.org/maven2/io/netty/([^/]+)/([^/]+)/\1-\2(?:-linux-x86_64)?\.jar ")
        lines = [line for line in final_stage(DOCKERFILES["spark"]) if "/io/netty/" in line]
        matches = [form.search(line) for line in lines]
        self.assertNotIn(None, matches, "each Netty jar is added by its Maven Central address")
        found = {match.group(1): match.group(2) for match in matches if match}
        self.assertEqual(len(found), len(lines), "no module is added twice")
        self.assertEqual({name: TCNATIVE_VERSION if name.startswith("netty-tcnative-") else NETTY_VERSION for name in NETTY_MODULES}, found)


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
