"""The step of each project image that installs the Debian upgrades, run for real in /bin/sh.

Its shell text, taken from the tracked Dockerfile, runs on a copy of the base image's APT files with a stand-in for apt-get on the search path, so these cases
start a stand-in program and belong to the slow group. The static checks of the same Dockerfiles are in ``tests/fast/test_image_hardening.py``. No test runs
Docker or reaches a network, so what the real apt does with those options shows only at a build.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from scripts.process import run_command
from tests.fast.test_image_hardening import DOCKERFILES, run_text, snapshot_line

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


class AptStepTest(unittest.TestCase):
    """The apt step of each image, run in /bin/sh with the absolute paths of the APT folders moved below a temporary folder."""

    def run_step(
        self,
        name: str,
        files: dict[str, str] | None = None,
        targets: tuple[str, ...] = TARGETS,
        failing: str | None = None,
        links: dict[str, str] | None = None,
    ) -> tuple[int, list[dict[str, Any]], Path]:
        path = DOCKERFILES[name]
        snapshot = snapshot_line(path).partition("=")[2]
        folder = tempfile.TemporaryDirectory(prefix="apt_step_")
        self.addCleanup(folder.cleanup)
        root = Path(folder.name)
        etc, lists, record, programs = root / "etc/apt", root / "var/lib/apt/lists", root / "record", root / "bin"
        for one in (etc / "sources.list.d", lists, record, programs):
            one.mkdir(parents=True)
        (lists / "left_over").write_text("")
        for relative, content in ({"sources.list.d/debian.sources": BASE_SOURCES} if files is None else files).items():
            (etc / relative).write_text(content)
        for relative, destination in (links or {}).items():
            (etc / relative).symlink_to(destination)
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
                expected = PINNED_SOURCES.format(snapshot=snapshot_line(path).partition("=")[2], keyring=KEYRING)
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

    def test_a_symbolic_link_where_the_base_image_holds_none_stops_the_build_before_any_apt_call(self) -> None:
        base = "sources.list.d/debian.sources"
        # Each case: the regular files of the APT folder, then the links with their destinations.
        cases: dict[str, tuple[dict[str, str], dict[str, str]]] = {
            "a dangling sources.list": ({base: BASE_SOURCES}, {"sources.list": "no_such_file"}),
            "a dangling extra .list link": ({base: BASE_SOURCES}, {"sources.list.d/extra.list": "no_such_file"}),
            "a dangling debian.sources": ({}, {base: "no_such_file"}),
            "debian.sources as a link to a file with the base content": ({"base_copy": BASE_SOURCES}, {base: "../base_copy"}),
        }
        for name in DOCKERFILES:
            for case, (files, links) in cases.items():
                with self.subTest(image=name, case=case):
                    code, calls, _ = self.run_step(name, files=files, links=links)
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

    def test_a_failed_apt_command_runs_nothing_more_and_fails_the_build(self) -> None:
        # The stand-in prints the three pinned indexes before it fails indextargets: only the exit status can stop that build.
        commands = ["update", "indextargets", "upgrade"]
        for name in DOCKERFILES:
            for position, failing in enumerate(commands):
                with self.subTest(image=name, failing=failing):
                    code, calls, _ = self.run_step(name, failing=failing)
                    self.assertNotEqual(0, code)
                    self.assertEqual(commands[: position + 1], [call["arguments"][0] for call in calls])


if __name__ == "__main__":
    unittest.main()
