"""The publish command builds each image from the tracked files of HEAD, pushes it when the pin file lacks its tag and pins the digest that the push reported.

Docker is a stand-in program that logs its arguments, so no image is built and no registry is reached. The repository is a temporary one. The stand-in knows
three commands, ``buildx build``, ``scout cves`` and ``push``. Its scan writes the report that a case planted for the image, or a report with no finding.
Any other command (a read of the registry) is logged and fails, and the cases prove that none is ever run.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import re
import sys
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.images import pins, publish
from scripts.images.publish import PublishError, Settings
from tests.fast.git_fixture import git_in

REPOSITORY = "example/project_images"
ACCOUNT = "1" * 12
CANARY_TEXT = "TOKEN-THAT-MUST-NOT-APPEAR"
STAND_IN = f"""#!{sys.executable}
import hashlib, json, os, sys
from pathlib import Path

here = Path(__file__).resolve().parent
args = sys.argv[1:]
with (here / "calls.log").open("a") as log:
    log.write(" ".join(args) + "\\n")


def files(folder):
    return sorted(str(p.relative_to(folder)) for p in Path(folder).rglob("*") if p.is_file())


if args[:2] == ["buildx", "build"]:
    paths = [args[-1]] + [a.split("=", 2)[2] for a in args if a.startswith("--build-context=")]
    with (here / "contexts.log").open("a") as out:
        out.write(json.dumps({{"paths": {{p: files(p) for p in paths}}, "args": args}}) + "\\n")
    sys.exit(0)
if args[:2] == ["scout", "cves"]:
    image = args[-1].rsplit(":", 1)[1].split("-")[0]
    if (here / ("scan_fails_" + image)).exists():
        sys.stderr.write("{CANARY_TEXT} scan failed\\n")
        sys.exit(int((here / ("scan_fails_" + image)).read_text()))
    if not (here / ("no_report_" + image)).exists():
        planted = here / ("report_" + image)
        run = {{"tool": {{"driver": {{"name": "docker scout"}}}}, "results": []}}
        clean = {{"version": "2.1.0", "runs": [run]}}
        Path(args[args.index("--output") + 1]).write_text(planted.read_text() if planted.exists() else json.dumps(clean))
    sys.exit(0)
if args[:1] == ["push"]:
    tag = args[-1]
    deny = here / "deny"
    if deny.exists() and deny.read_text().strip() in tag:
        sys.stderr.write("{CANARY_TEXT} denied: requested access to the resource is denied\\n")
        code = here / "deny_code"
        sys.exit(int(code.read_text()) if code.exists() else 1)
    digest = (here / "digest").read_text().strip() if (here / "digest").exists() else "sha256:" + hashlib.sha256(tag.encode()).hexdigest()
    name = tag.rsplit(":", 1)[1]
    custom = here / "push_output"
    if custom.exists():
        sys.stdout.write(custom.read_text().replace("TAG", name).replace("DIGEST", digest))
        sys.exit(0)
    sys.stdout.write("The push refers to repository [docker.io/" + tag.rsplit(":", 1)[0] + "]\\n")
    sys.stdout.write("5f70bf18a086: Layer already exists\\n")
    sys.stdout.write(name + ": digest: " + digest + " size: 856\\n")
    sys.exit(0)
sys.exit(3)
"""
PUSH = "push --platform linux/amd64 "
IMAGES = [
    {
        "tag": "x-app:1.2",
        "name": "app",
        "sources": ["appsrc", "libsrc"],
        "context": ".",
        "dockerfile": "appsrc/Dockerfile",
        "build_contexts": {"lib": "libsrc"},
    },
    {"tag": "x-tool:0.4", "name": "tool", "sources": ["toolsrc", "compose"], "build_contexts": {"stack": "compose"}},
]


def digest_of(tag: str) -> str:
    import hashlib

    return "sha256:" + hashlib.sha256(tag.encode()).hexdigest()


class Case(unittest.TestCase):
    def setUp(self) -> None:
        home = tempfile.TemporaryDirectory(prefix="publish_")
        self.addCleanup(home.cleanup)
        base = Path(home.name)
        self.repo = base / "repo"
        self.bin = base / "bin"
        self.bin.mkdir()
        self.docker = self.bin / "docker"
        self.docker.write_text(STAND_IN)
        self.docker.chmod(0o755)
        for relative, text in {
            "appsrc/Dockerfile": "FROM scratch\n",
            "appsrc/app.py": "print(1)\n",
            "libsrc/lib.py": "x = 1\n",
            "toolsrc/Dockerfile": "FROM scratch\n",
            "toolsrc/tool.py": "y = 2\n",
            "compose/services.yml": "services: {}\n",
            ".gitignore": ".env\nhandoff.md\n",
        }.items():
            self.write(relative, text)
        self.accepted_file = base / "accepted_findings.yml"
        self.accepted_file.write_text("findings: []\n")
        self.git("init", "--quiet", "--initial-branch", "main")
        self.git("config", "user.name", "Fixture")
        self.git("config", "user.email", "fixture@example.invalid")
        self.git("add", "--all")
        self.git("commit", "--quiet", "--message", "feat: first")
        self.pin_file = self.repo / "compose" / "image_pins.yml"
        self.pin_file.write_text("images: {}\n")
        self.git("add", "--all")
        self.git("commit", "--quiet", "--message", "chore: pins")

    def write(self, relative: str, text: str) -> None:
        path = self.repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def git(self, *arguments: str) -> None:
        git_in(self.repo, *arguments)

    def commit_all(self) -> None:
        self.git("add", "--all")
        self.git("commit", "--quiet", "--message", "fix: change")

    def settings(self, **more: object) -> Settings:
        values: dict[str, object] = {
            "root": self.repo,
            "docker": str(self.docker),
            "repository": REPOSITORY,
            "pin_file": self.pin_file,
            "images": IMAGES,
            "accepted_file": self.accepted_file,
        }
        return Settings(**{**values, **more})  # type: ignore[arg-type]

    def run_publish(self, **more: object) -> list[str]:
        return publish.publish(self.settings(**more))

    def calls(self) -> list[str]:
        log = self.bin / "calls.log"
        return log.read_text().splitlines() if log.exists() else []

    def builds(self) -> list[dict[str, Any]]:
        log = self.bin / "contexts.log"
        return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []

    def pushes(self) -> list[str]:
        return [line for line in self.calls() if line.startswith("push ")]


class RefusalTest(Case):
    def test_a_modified_tracked_file_or_an_untracked_file_stops_the_command_before_any_docker_call(self) -> None:
        changes: list[Callable[[], None]] = [lambda: self.write("appsrc/app.py", "print(2)\n"), lambda: self.write("appsrc/new.py", "z\n")]
        for change in changes:
            with self.subTest():
                change()
                with self.assertRaises(PublishError) as raised:
                    self.run_publish()
                self.assertEqual("the working tree is not clean: commit or remove every change first", str(raised.exception))
                self.assertEqual([], self.calls())
                self.git("checkout", "--", ".")
                self.git("clean", "--force", "--quiet")

    def test_a_staged_but_uncommitted_file_is_refused_because_the_image_would_come_from_head(self) -> None:
        self.write("appsrc/app.py", "print(3)\n")
        self.git("add", "appsrc/app.py")
        with self.assertRaisesRegex(PublishError, "not clean"):
            self.run_publish()

    def test_a_source_that_head_does_not_hold_is_refused_by_name_of_the_image(self) -> None:
        images = [{"tag": "x-app:1.2", "name": "app", "sources": ["appsrc", "missing"]}]
        with self.assertRaises(PublishError) as raised:
            self.run_publish(images=images)
        self.assertEqual("app: a source is not a tracked path of HEAD", str(raised.exception))
        self.assertEqual([], self.calls())

    def test_a_tracked_symbolic_link_in_a_source_is_refused(self) -> None:
        os.symlink("../libsrc/lib.py", self.repo / "appsrc" / "link.py")
        self.commit_all()
        with self.assertRaises(PublishError) as raised:
            self.run_publish()
        self.assertEqual("app: a source holds a symbolic link or another file that is not a plain file", str(raised.exception))
        self.assertEqual([], self.calls())


class StagingTest(Case):
    def test_each_build_gets_the_tracked_files_of_head_and_no_untracked_or_ignored_file(self) -> None:
        self.write(".env", "SECRET=1\n")
        self.write("appsrc/handoff.md", "notes\n")
        self.run_publish()
        app, tool = (build["paths"] for build in self.builds())
        self.assertEqual({"appsrc/Dockerfile", "appsrc/app.py", "libsrc/lib.py", "lib.py"}, {name for names in app.values() for name in names})
        self.assertEqual({"Dockerfile", "tool.py", "services.yml"}, {name for names in tool.values() for name in names})

    def test_the_pin_file_is_left_out_of_a_folder_that_holds_its_folder_so_a_pin_never_changes_a_tag(self) -> None:
        self.run_publish()
        first = pins.load(self.pin_file)
        self.commit_all()
        self.assertEqual(["app: already pinned", "tool: already pinned"], self.run_publish())
        self.assertEqual(first, pins.load(self.pin_file))

    def test_a_change_in_the_pin_file_alone_does_not_stop_the_command_and_a_change_beside_it_does(self) -> None:
        self.pin_file.write_text(pins.HEADER + "images: {}\n")
        self.run_publish()
        self.write("appsrc/app.py", "print(5)\n")
        with self.assertRaisesRegex(PublishError, "not clean"):
            self.run_publish()

    def test_a_file_that_is_changed_on_disk_but_not_committed_never_reaches_an_image(self) -> None:
        self.write("appsrc/app.py", "print(99)\n")
        with self.assertRaises(PublishError):
            self.run_publish()

    def test_the_build_command_has_the_platform_the_load_flag_the_tag_the_dockerfile_the_named_contexts_and_the_context_last(self) -> None:
        self.run_publish()
        app, tool = (build["args"] for build in self.builds())
        tag = json.loads(json.dumps(pins.load(self.pin_file)))["app"]["tag"]
        self.assertEqual(["buildx", "build", "--platform", "linux/amd64", "--load", "--tag", tag], app[:7])
        self.assertTrue(app[7].startswith("--file=") and app[7].endswith("/appsrc/Dockerfile"))
        self.assertTrue(app[8].startswith("--build-context=lib=") and app[8].endswith("/libsrc"))
        self.assertEqual(9 + 1, len(app))
        self.assertEqual(7 + 1 + 1, len(tool), "no Dockerfile is given when the list names none, and the context is the first source")
        self.assertTrue(tool[-1].endswith("/toolsrc"))
        self.assertEqual([PUSH + tag], [line for line in self.pushes() if line.endswith(tag)])

    def test_the_tag_is_the_repository_the_name_the_version_of_the_list_and_12_hex_digits_of_the_folder_hash_and_it_is_stable_until_a_tracked_file_changes(
        self,
    ) -> None:
        self.run_publish()
        first = pins.load(self.pin_file)
        self.assertRegex(first["app"]["tag"], rf"^{REPOSITORY}:app-1\.2-[0-9a-f]{{12}}$")
        self.assertRegex(first["tool"]["tag"], rf"^{REPOSITORY}:tool-0\.4-[0-9a-f]{{12}}$")
        self.git("checkout", "--", "compose/image_pins.yml")
        self.run_publish()
        self.assertEqual({name: value["tag"] for name, value in first.items()}, {name: value["tag"] for name, value in pins.load(self.pin_file).items()})
        self.write("libsrc/lib.py", "x = 2\n")
        self.commit_all()
        self.run_publish()
        changed = pins.load(self.pin_file)
        self.assertNotEqual(first["app"]["tag"], changed["app"]["tag"])
        self.assertEqual(first["tool"]["tag"], changed["tool"]["tag"], "a change outside the image's sources leaves its tag as it was")

    def test_the_file_mode_is_part_of_the_hash(self) -> None:
        self.run_publish()
        first = pins.load(self.pin_file)["app"]["tag"]
        self.pin_file.write_text("images: {}\n")
        (self.repo / "appsrc" / "app.py").chmod(0o755)
        self.commit_all()
        self.run_publish()
        self.assertNotEqual(first, pins.load(self.pin_file)["app"]["tag"])


class PushTest(Case):
    def test_the_digest_in_the_pushs_own_line_is_written_to_the_pin_file_and_no_other_docker_command_runs(self) -> None:
        self.run_publish()
        found = pins.load(self.pin_file)
        for name in ("app", "tool"):
            self.assertEqual(f"{REPOSITORY}@{digest_of(found[name]['tag'])}", found[name]["digest"])
        allowed = re.compile(rf"buildx build |scout cves |{re.escape(PUSH)}")
        self.assertEqual(
            [], [line for line in self.calls() if not allowed.match(line)], "the registry is never read: only a build, a scan of the local image and a push run"
        )
        self.assertEqual(
            [PUSH + found["app"]["tag"], PUSH + found["tool"]["tag"]], self.pushes(), "each push asks for the one linux/amd64 manifest, not an index"
        )

    def test_a_second_run_with_the_pins_in_place_runs_no_docker_command_and_writes_nothing(self) -> None:
        self.run_publish()
        text = self.pin_file.read_text()
        before = self.calls()
        with mock.patch.object(pins, "write") as written:
            self.assertEqual(["app: already pinned", "tool: already pinned"], self.run_publish())
        written.assert_not_called()
        self.assertEqual(before, self.calls())
        self.assertEqual(text, self.pin_file.read_text())

    def test_a_tag_that_the_pin_file_holds_keeps_its_pin_whatever_the_digest_is_and_whatever_the_registry_would_say(self) -> None:
        self.run_publish()
        found = pins.load(self.pin_file)
        edited = {**found, "tool": {**found["tool"], "digest": f"{REPOSITORY}@sha256:{'e' * 64}"}}
        pins.write(self.pin_file, edited)
        text = self.pin_file.read_text()
        before = self.calls()
        self.assertEqual(["app: already pinned", "tool: already pinned"], self.run_publish())
        self.assertEqual(text, self.pin_file.read_text())
        self.assertEqual(before, self.calls())
        self.assertEqual(f"{REPOSITORY}@sha256:{'e' * 64}", pins.load(self.pin_file)["tool"]["digest"])

    def test_a_tag_that_the_pin_file_lacks_is_built_and_pushed_again_even_when_an_earlier_run_pushed_it(self) -> None:
        """The accepted behavior: the pin file is the authority and the registry is never read, so a pushed tag whose pin was not saved is pushed again."""
        self.run_publish()
        found = pins.load(self.pin_file)
        self.pin_file.write_text("images: {}\n")
        (self.bin / "calls.log").unlink()
        (self.bin / "contexts.log").unlink()
        self.assertEqual(["app: pushed and pinned", "tool: pushed and pinned"], self.run_publish())
        self.assertEqual(2, len(self.builds()))
        self.assertEqual([PUSH + found["app"]["tag"], PUSH + found["tool"]["tag"]], self.pushes())
        self.assertEqual(found, pins.load(self.pin_file))

    def test_the_module_text_says_what_the_accepted_behavior_is_and_that_the_registry_is_never_read(self) -> None:
        text = " ".join((publish.__doc__ or "").split())
        self.assertIn("The pin file is the authority and the registry is never read", text)
        self.assertIn("a push that succeeded but whose pin was not saved is pushed again on the next run and may overwrite that unpinned tag", text)
        self.assertIn("Nothing pins it, so no host pulls it", text)

    def test_the_module_holds_no_read_of_the_registry_and_no_login_check(self) -> None:
        text = Path(publish.__file__).read_text()
        for word in ("manifest inspect", '"manifest"', "inspect", "imagetools", "remote_digest", "platform_digest", "refused_login", "LOGIN_WORDS"):
            self.assertTrue(word not in text, word)

    def test_the_pin_is_the_digest_of_the_tags_own_line_and_not_of_another_line_of_the_output(self) -> None:
        (self.bin / "push_output").write_text(
            f"other-tag: digest: sha256:{'a' * 64} size: 1\nlatest: digest: sha256:{'b' * 64} size: 2\nTAG: digest: DIGEST size: 856\n"
            f"x TAG: digest: sha256:{'c' * 64} size: 3\n"
        )
        self.run_publish(images=IMAGES[:1])
        tag = pins.load(self.pin_file)["app"]["tag"]
        self.assertEqual(f"{REPOSITORY}@{digest_of(tag)}", pins.load(self.pin_file)["app"]["digest"])

    def test_a_push_output_without_exactly_one_exact_digest_line_for_the_tag_is_refused_with_no_pin(self) -> None:
        good = "sha256:" + "d" * 64
        cases = {
            "no line": "The push refers to repository [docker.io/x]\n",
            "empty": "",
            "another tag": f"other-tag: digest: {good} size: 856\n",
            "the repository with the tag": f"{REPOSITORY}:TAG: digest: {good} size: 856\n",
            "two lines": f"TAG: digest: {good} size: 856\nTAG: digest: sha256:{'e' * 64} size: 856\n",
            "short digest": "TAG: digest: sha256:short size: 856\n",
            "63 digits": f"TAG: digest: sha256:{'d' * 63} size: 856\n",
            "65 digits": f"TAG: digest: sha256:{'d' * 65} size: 856\n",
            "upper case": f"TAG: digest: sha256:{'D' * 64} size: 856\n",
            "another algorithm": f"TAG: digest: sha512:{'d' * 64} size: 856\n",
            "no size": f"TAG: digest: {good}\n",
            "size not a number": f"TAG: digest: {good} size: big\n",
            "text after the size": f"TAG: digest: {good} size: 856 extra\n",
            "text before the tag": f" TAG: digest: {good} size: 856\n",
            "one space less": f"TAG:digest: {good} size: 856\n",
        }
        for label, output in cases.items():
            with self.subTest(case=label):
                self.setUp()
                (self.bin / "push_output").write_text(output)
                with self.assertRaises(PublishError) as raised:
                    self.run_publish(images=IMAGES[:1])
                self.assertEqual(
                    "app: the push output holds no digest line of the form <tag>: digest: sha256:<64 hex digits> size: <number> for the pushed tag",
                    str(raised.exception),
                )
                self.assertEqual("images: {}\n", self.pin_file.read_text())

    def test_a_digest_line_with_a_windows_line_ending_is_read_as_the_same_line(self) -> None:
        (self.bin / "push_output").write_text("TAG: digest: DIGEST size: 856\r\n")
        self.run_publish(images=IMAGES[:1])
        tag = pins.load(self.pin_file)["app"]["tag"]
        self.assertEqual(f"{REPOSITORY}@{digest_of(tag)}", pins.load(self.pin_file)["app"]["digest"])

    def test_the_digest_line_on_standard_error_is_not_read(self) -> None:
        """The CLI writes its push report to standard output, so the same line elsewhere proves nothing."""
        self.docker.write_text(self.docker.read_text().replace('sys.stdout.write(name + ": digest: "', 'sys.stderr.write(name + ": digest: "'))
        with self.assertRaisesRegex(PublishError, "holds no digest line"):
            self.run_publish(images=IMAGES[:1])
        self.assertEqual("images: {}\n", self.pin_file.read_text())

    def test_a_push_that_fails_stops_with_the_cli_s_own_exit_status_in_a_fixed_text_that_holds_no_output_of_docker_and_changes_no_pin(self) -> None:
        (self.bin / "deny").write_text("")
        (self.bin / "deny_code").write_text("7")
        with self.assertRaises(PublishError) as raised:
            self.run_publish()
        self.assertEqual(
            "app: the push failed with the exit status 7 of docker: check docker login on this computer, then run the command again", str(raised.exception)
        )
        self.assertEqual(7, raised.exception.exit_status)
        self.assertNotIn(CANARY_TEXT, str(raised.exception))
        self.assertEqual("images: {}\n", self.pin_file.read_text())

    def test_an_image_that_fails_changes_no_pin_and_the_retry_pushes_the_unpinned_image_again(self) -> None:
        before = self.pin_file.read_text()
        (self.bin / "deny").write_text("tool")
        with self.assertRaises(PublishError):
            self.run_publish()
        self.assertEqual(before, self.pin_file.read_text())
        self.assertEqual(1, len([line for line in self.pushes() if "images:app-" in line]))
        (self.bin / "deny").unlink()
        self.assertEqual(["app: pushed and pinned", "tool: pushed and pinned"], self.run_publish())
        self.assertEqual(["app", "tool"], list(pins.load(self.pin_file)))
        self.assertEqual(2, len([line for line in self.pushes() if "images:app-" in line]), "the unpinned app tag is pushed again, as the docstring says")

    def test_a_pin_that_was_saved_is_never_pushed_again_after_a_later_image_failed(self) -> None:
        self.run_publish(images=IMAGES[:1])
        (self.bin / "deny").write_text("tool")
        with self.assertRaises(PublishError):
            self.run_publish()
        self.assertEqual(
            1, len([line for line in self.pushes() if "images:app-" in line]), "app was pinned by the first run, so the second run never touched it"
        )

    def test_the_pin_file_is_written_once_after_every_image_has_resolved(self) -> None:
        with mock.patch.object(pins, "write", wraps=pins.write) as written:
            self.run_publish()
        self.assertEqual(1, written.call_count)
        self.assertEqual({"app", "tool"}, set(written.call_args.args[1]))

    def test_a_pin_file_that_the_run_did_not_change_is_not_written(self) -> None:
        self.run_publish()
        with mock.patch.object(pins, "write") as written:
            self.run_publish()
        written.assert_not_called()

    def test_the_build_and_the_push_name_the_platform_of_the_host(self) -> None:
        self.run_publish(images=IMAGES[:1])
        self.assertIn("--platform linux/amd64 --load", [line for line in self.calls() if line.startswith("buildx build")][0])
        self.assertEqual(PUSH, self.pushes()[0][: len(PUSH)])
        self.assertEqual("linux/amd64", publish.PLATFORM)


FINDING_PACKAGE = "pkg:maven/io.netty/netty-handler@4.2.7"
HOLDER = "/opt/jars/holder-1.0.jar"


def accepted_entry(image: str = "tool", artifact: str = HOLDER, package: str = FINDING_PACKAGE) -> str:
    fields = f"  - image: {image}\n    artifact: {artifact}\n    cve: CVE-2026-1111\n    package: {package}\n"
    return fields + '    reason: No release holds the fix.\n    date: "2026-11-25"\n'


class ScanGateTest(Case):
    def plant(self, image: str, cve: str) -> None:
        text = f"Vulnerability    :{cve}  \nPackage          :{FINDING_PACKAGE}  \nFixed version    :9  \n"
        location = {"physicalLocation": {"artifactLocation": {"uri": f"{HOLDER}:io.netty:netty-handler"}}}
        self.plant_report(image, [{"ruleId": cve, "message": {"text": text}, "locations": [location]}])

    def plant_report(self, image: str, results: list[dict[str, Any]], invocations: list[dict[str, Any]] | None = None) -> None:
        run: dict[str, Any] = {"tool": {"driver": {"name": "docker scout"}}, "results": results}
        if invocations is not None:
            run["invocations"] = invocations
        (self.bin / f"report_{image}").write_text(json.dumps({"version": "2.1.0", "runs": [run]}))

    def refusal(self) -> PublishError:
        with self.assertRaises(PublishError) as raised:
            self.run_publish()
        return raised.exception

    def assert_nothing_pushed_or_pinned(self) -> None:
        self.assertEqual([], self.pushes())
        self.assertEqual("images: {}\n", self.pin_file.read_text())

    def test_every_built_image_is_scanned_before_the_first_push_and_each_scan_is_of_the_local_tag(self) -> None:
        self.run_publish()
        found = pins.load(self.pin_file)
        kinds = [line.split()[0] for line in self.calls()]
        self.assertEqual(["buildx", "scout", "buildx", "scout", "push", "push"], kinds)
        scans = [line for line in self.calls() if line.startswith("scout ")]
        self.assertEqual([f"local://{found[name]['tag']}" for name in ("app", "tool")], [line.split()[-1] for line in scans])
        self.assertTrue(all(line.startswith("scout cves --platform linux/amd64 --only-fixed --format sarif --output ") for line in scans))

    def test_a_finding_in_the_last_image_stops_the_push_of_every_image_and_each_finding_of_each_image_is_named(self) -> None:
        for planted in (["tool"], ["app", "tool"]):
            with self.subTest(images=planted):
                for image in planted:
                    self.plant(image, "CVE-2026-1111")
                error = self.refusal()
                self.assertEqual(tuple(f"{image}: CVE-2026-1111 {FINDING_PACKAGE} in {HOLDER}" for image in planted), error.findings)
                self.assertIn("nothing was pushed", str(error))
                self.assert_nothing_pushed_or_pinned()

    def test_a_finding_passes_only_by_an_entry_for_that_image_artifact_and_package_version(self) -> None:
        self.plant("tool", "CVE-2026-1111")
        others = {
            "another version": accepted_entry(package=FINDING_PACKAGE.replace("4.2.7", "4.2.6")),
            "another image": accepted_entry(image="app"),
            "another artifact": accepted_entry(artifact="/opt/jars/other-2.0.jar"),
        }
        for label, other in others.items():
            with self.subTest(case=label):
                self.accepted_file.write_text("findings:\n" + other)
                self.assertEqual((f"tool: CVE-2026-1111 {FINDING_PACKAGE} in {HOLDER}",), self.refusal().findings)
                self.assert_nothing_pushed_or_pinned()
        self.accepted_file.write_text("findings:\n" + accepted_entry())
        self.assertEqual(["app: pushed and pinned", "tool: pushed and pinned"], self.run_publish())

    def test_an_entry_that_is_only_pending_lets_nothing_pass(self) -> None:
        self.plant("tool", "CVE-2026-1111")
        self.accepted_file.write_text("findings: []\npending:\n" + accepted_entry())
        self.assertEqual((f"tool: CVE-2026-1111 {FINDING_PACKAGE} in {HOLDER}",), self.refusal().findings)
        self.assert_nothing_pushed_or_pinned()

    def test_a_scan_that_fails_or_writes_no_report_or_an_incomplete_one_pushes_nothing_and_shows_no_output_of_docker(self) -> None:
        cases = (
            ("scan_fails_tool", "3", "tool: the scan failed with the exit status 3"),
            ("no_report_tool", "", "tool: the scan wrote no readable report"),
            ("report_tool", '{"version": "2.1.0"}', "tool: the scan report is not one complete SARIF run"),
        )
        for marker, content, message in cases:
            with self.subTest(case=marker):
                (self.bin / marker).write_text(content)
                text = str(self.refusal())
                self.assertIn(message, text)
                self.assertNotIn(CANARY_TEXT, text)
                self.assert_nothing_pushed_or_pinned()
                (self.bin / marker).unlink()

    def test_a_clean_real_shaped_report_without_invocations_pushes_only_when_the_scan_exits_with_0(self) -> None:
        self.plant_report("tool", [])
        self.assertNotIn("invocations", (self.bin / "report_tool").read_text())
        (self.bin / "scan_fails_tool").write_text("1")
        self.assertIn("tool: the scan failed with the exit status 1", str(self.refusal()))
        self.assert_nothing_pushed_or_pinned()
        (self.bin / "scan_fails_tool").unlink()
        self.assertEqual(["app: pushed and pinned", "tool: pushed and pinned"], self.run_publish())

    def test_a_clean_report_whose_invocations_do_not_all_say_that_the_execution_succeeded_pushes_nothing(self) -> None:
        cases: dict[str, list[dict[str, Any]]] = {"failed": [{"executionSuccessful": False}], "no invocation": [], "no statement": [{}]}
        for label, invocations in cases.items():
            with self.subTest(case=label):
                self.plant_report("tool", [], invocations)
                self.assertIn("tool: the scan report does not say that its execution succeeded", str(self.refusal()))
                self.assert_nothing_pushed_or_pinned()

    def test_a_report_that_holds_a_second_empty_result_list_after_a_finding_pushes_nothing(self) -> None:
        self.plant("tool", "CVE-2026-1111")
        path = self.bin / "report_tool"
        text = path.read_text()
        changed = text.removesuffix("}]}") + ', "results": []}]}'
        self.assertNotEqual(text, changed)
        path.write_text(changed)
        self.assertIn("tool: the scan report holds a key twice in one object", str(self.refusal()))
        self.assert_nothing_pushed_or_pinned()

    def test_an_unreadable_accepted_list_stops_the_command_before_any_docker_call(self) -> None:
        self.accepted_file.write_text("findings:\n")
        self.refusal()
        self.assertEqual([], self.calls())

    def test_the_command_prints_each_refused_finding_on_standard_error(self) -> None:
        self.plant("app", "CVE-2026-1111")
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            self.assertEqual(1, publish.main([], self.settings()))
        self.assertEqual("", out.getvalue())
        self.assertIn(f"publish: app: CVE-2026-1111 {FINDING_PACKAGE} in {HOLDER}\n", err.getvalue())
        self.assertNotIn(CANARY_TEXT, err.getvalue())


class MissingToolTest(Case):
    def test_a_missing_docker_is_a_fixed_message_and_not_a_traceback(self) -> None:
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            code = publish.main([], settings=self.settings(docker=str(self.bin / "no_such_docker")))
        self.assertEqual(1, code)
        self.assertEqual("publish: docker could not be started: it is not installed, not on the path or not executable\n", err.getvalue())

    def test_a_missing_git_is_a_fixed_message_and_not_a_traceback(self) -> None:
        err = io.StringIO()
        with mock.patch.object(publish, "run_command", side_effect=FileNotFoundError("git")), contextlib.redirect_stderr(err):
            code = publish.main([], settings=self.settings())
        self.assertEqual(1, code)
        self.assertEqual("publish: git could not be started: it is not installed, not on the path or not executable\n", err.getvalue())

    def test_a_pin_file_that_cannot_be_written_is_a_fixed_message_and_the_old_file_stays(self) -> None:
        before = self.pin_file.read_text()
        err = io.StringIO()
        with mock.patch("scripts.images.pins.tempfile.mkstemp", side_effect=PermissionError("no")), contextlib.redirect_stderr(err):
            code = publish.main([], settings=self.settings())
        self.assertEqual(1, code)
        self.assertEqual("publish: the pin file could not be written\n", err.getvalue())
        self.assertEqual(before, self.pin_file.read_text())


class OutputTest(Case):
    def test_the_command_prints_only_fixed_text_and_the_names_of_images(self) -> None:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = publish.main([], settings=self.settings())
        self.assertEqual(0, code)
        self.assertEqual("app: pushed and pinned\ntool: pushed and pinned\n", out.getvalue())
        self.assertEqual("", err.getvalue())

    def test_a_failed_push_prints_one_fixed_line_to_standard_error_and_exits_with_the_cli_s_own_status(self) -> None:
        (self.bin / "deny").write_text("")
        (self.bin / "deny_code").write_text("17")
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = publish.main([], settings=self.settings())
        self.assertEqual(17, code)
        self.assertEqual(
            "publish: app: the push failed with the exit status 17 of docker: check docker login on this computer, then run the command again\n", err.getvalue()
        )
        self.assertNotIn(CANARY_TEXT, out.getvalue() + err.getvalue())

    def test_a_failure_of_another_kind_exits_with_one_and_so_does_an_exit_status_that_is_no_status_of_a_process(self) -> None:
        for status in (None, -9, 0, 256, 1000):
            with self.subTest(status=status), mock.patch.object(publish, "publish", side_effect=PublishError("fixed text", status)):
                err = io.StringIO()
                with contextlib.redirect_stderr(err):
                    self.assertEqual(1, publish.main([], settings=self.settings()))
                self.assertEqual("publish: fixed text\n", err.getvalue())

    def test_no_environment_variable_that_may_hold_a_token_is_read_by_the_module(self) -> None:
        text = Path(publish.__file__).read_text()
        for word in ("getenv", "TOKEN", "PASSWORD", "DOCKER_CONFIG", "docker/config", "config.json", "input_text"):
            self.assertNotIn(word, text)
        self.assertEqual([], [line for line in text.splitlines() if "os.environ" in line and "GIT_" not in line], "only the GIT_ variables are filtered out")

    def test_the_default_settings_name_the_one_list_the_pin_file_and_the_repository_of_the_configuration_file(self) -> None:
        env = self.repo.parent / "env"
        keys = f"AWS_REGION=us-west-2\nAWS_PROFILE=p\nAWS_ACCOUNT_ID={ACCOUNT}\nPROJECT_NAME=p\nENVIRONMENT=dev\nS3_BUCKET=bucket-one\n"
        env.write_text(keys + f"IMAGE_REPOSITORY={REPOSITORY}\n")
        settings = publish.default_settings(env)
        self.assertEqual(pins.load_images(), settings.images)
        self.assertEqual((REPOSITORY, pins.PIN_FILE, "docker"), (settings.repository, settings.pin_file, settings.docker))

    def test_a_missing_or_malformed_repository_key_is_refused_by_its_name_and_never_its_value(self) -> None:
        env = self.repo.parent / "env"
        keys = f"AWS_REGION=us-west-2\nAWS_PROFILE=p\nAWS_ACCOUNT_ID={ACCOUNT}\nPROJECT_NAME=p\nENVIRONMENT=dev\nS3_BUCKET=bucket-one\n"
        env.write_text(keys)
        with self.assertRaisesRegex(PublishError, "IMAGE_REPOSITORY is missing"):
            publish.default_settings(env)
        env.write_text(keys + "IMAGE_REPOSITORY=Bad Value\n")
        with self.assertRaises(PublishError) as raised:
            publish.default_settings(env)
        self.assertIn("IMAGE_REPOSITORY", str(raised.exception))
        self.assertNotIn("Bad Value", str(raised.exception))

    def test_a_pin_file_of_another_repository_than_the_configured_one_stops_the_command(self) -> None:
        self.run_publish()
        with self.assertRaisesRegex(PublishError, "another repository"):
            self.run_publish(repository="example/other")


if __name__ == "__main__":
    unittest.main()
