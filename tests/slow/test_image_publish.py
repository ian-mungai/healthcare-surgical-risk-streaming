"""The publish command builds each image from the tracked files of HEAD, pushes it once and pins the digest that the repository gave it.

Docker is a stand-in program that logs its arguments, so no image is built and no registry is reached. The repository is a temporary one.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
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
remote = here / "remote.json"
state = json.loads(remote.read_text()) if remote.exists() else {{}}


def files(folder):
    return sorted(str(p.relative_to(folder)) for p in Path(folder).rglob("*") if p.is_file())


if args[:2] == ["buildx", "build"]:
    paths = [args[-1]] + [a.split("=", 2)[2] for a in args if a.startswith("--build-context=")]
    with (here / "contexts.log").open("a") as out:
        out.write(json.dumps({{"paths": {{p: files(p) for p in paths}}, "args": args}}) + "\\n")
    sys.exit(0)
if args[:1] == ["push"]:
    deny = here / "deny"
    if deny.exists() and deny.read_text().strip() in args[1]:
        sys.stderr.write("{CANARY_TEXT} denied: requested access to the resource is denied\\n")
        sys.exit(1)
    override = here / "digest"
    state[args[1]] = override.read_text().strip() if override.exists() else "sha256:" + hashlib.sha256(args[1].encode()).hexdigest()
    remote.write_text(json.dumps(state))
    sys.exit(0)
if args[:3] == ["buildx", "imagetools", "inspect"]:
    tag = args[-1]
    if tag in state:
        print(state[tag])
        sys.exit(0)
    broken = here / "inspect_fails"
    if broken.exists():
        sys.stderr.write(broken.read_text().replace("TAG", tag) or "ERROR: {CANARY_TEXT} the registry is unreachable\\n")
        sys.exit(1)
    sys.stderr.write("ERROR: docker.io/" + tag + ": not found\\n")
    sys.exit(1)
sys.exit(3)
"""
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
        values: dict[str, object] = {"root": self.repo, "docker": str(self.docker), "repository": REPOSITORY, "pin_file": self.pin_file, "images": IMAGES}
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
        self.assertEqual(["push " + tag], [line for line in self.pushes() if line.endswith(tag)])

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
    def test_the_pushed_digest_is_read_from_the_repository_and_written_to_the_pin_file(self) -> None:
        self.run_publish()
        found = pins.load(self.pin_file)
        for name in ("app", "tool"):
            self.assertEqual(f"{REPOSITORY}@{digest_of(found[name]['tag'])}", found[name]["digest"])
        self.assertTrue(any(line.startswith("buildx imagetools inspect --format {{.Manifest.Digest}} ") for line in self.calls()))

    def test_a_second_run_with_the_pins_in_place_reads_each_digest_but_builds_pushes_and_writes_nothing(self) -> None:
        self.run_publish()
        text = self.pin_file.read_text()
        before = len(self.calls())
        with mock.patch.object(pins, "write") as written:
            self.assertEqual(["app: already pinned", "tool: already pinned"], self.run_publish())
        written.assert_not_called()
        added = self.calls()[before:]
        self.assertEqual(2, len(added))
        self.assertTrue(all(line.startswith("buildx imagetools inspect ") for line in added), added)
        self.assertEqual(text, self.pin_file.read_text())

    def test_a_pin_that_was_edited_by_hand_to_another_digest_with_the_same_tag_is_refused_and_no_pin_changes(self) -> None:
        self.run_publish()
        found = pins.load(self.pin_file)
        edited = {**found, "tool": {**found["tool"], "digest": f"{REPOSITORY}@sha256:{'e' * 64}"}}
        pins.write(self.pin_file, edited)
        text = self.pin_file.read_text()
        before = self.calls()
        with self.assertRaises(PublishError) as raised:
            self.run_publish()
        self.assertEqual("tool: the tag already has a different digest in the pin file", str(raised.exception))
        self.assertEqual(text, self.pin_file.read_text())
        self.assertEqual([], [line for line in self.calls()[len(before) :] if not line.startswith("buildx imagetools inspect ")])

    def test_a_pinned_tag_that_the_repository_no_longer_holds_is_refused_and_not_pushed_again(self) -> None:
        self.run_publish()
        text = self.pin_file.read_text()
        (self.bin / "remote.json").write_text("{}")
        before = len(self.pushes())
        with self.assertRaises(PublishError) as raised:
            self.run_publish()
        self.assertEqual(
            "app: the repository does not hold the pinned tag: remove its entry from the pin file and run the command again", str(raised.exception)
        )
        self.assertEqual(before, len(self.pushes()))
        self.assertEqual(text, self.pin_file.read_text())

    def test_a_tag_that_the_repository_holds_but_the_pins_lack_is_pinned_without_a_build_or_a_push(self) -> None:
        self.run_publish()
        found = pins.load(self.pin_file)
        self.pin_file.write_text("images: {}\n")
        (self.bin / "calls.log").unlink()
        (self.bin / "contexts.log").unlink()
        self.assertEqual(["app: pinned from the repository", "tool: pinned from the repository"], self.run_publish())
        self.assertEqual([], self.builds())
        self.assertEqual([], self.pushes())
        self.assertEqual(found, pins.load(self.pin_file))

    def test_a_digest_that_differs_from_the_pinned_digest_of_the_same_tag_is_refused_and_the_pins_stay(self) -> None:
        self.run_publish()
        found = pins.load(self.pin_file)
        other = "sha256:" + "f" * 64
        with self.assertRaises(PublishError) as raised:
            publish.pin_image(found, "app", found["app"]["tag"], other, REPOSITORY)
        self.assertEqual("app: the tag already has a different digest in the pin file", str(raised.exception))
        same = publish.pin_image(found, "app", found["app"]["tag"], found["app"]["digest"].partition("@")[2], REPOSITORY)
        self.assertEqual(found, same)

    def test_a_malformed_digest_from_the_repository_is_refused_and_not_pinned(self) -> None:
        (self.bin / "digest").write_text("sha256:short\n")
        with self.assertRaises(PublishError) as raised:
            self.run_publish(images=IMAGES[:1])
        self.assertEqual("app: the repository returned a digest that is not sha256 and 64 hex digits", str(raised.exception))
        self.assertEqual("images: {}\n", self.pin_file.read_text())

    def test_a_push_that_the_repository_refuses_stops_with_the_fixed_login_text_that_holds_no_output_of_docker(self) -> None:
        (self.bin / "deny").write_text("")
        with self.assertRaises(PublishError) as raised:
            self.run_publish()
        self.assertEqual("the push was refused: run docker login on this computer, then run the command again", str(raised.exception))
        self.assertNotIn(CANARY_TEXT, str(raised.exception))
        self.assertEqual("images: {}\n", self.pin_file.read_text())

    def test_an_image_that_fails_changes_no_pin_and_the_retry_pins_the_pushed_image_from_the_repository_without_a_second_push(self) -> None:
        before = self.pin_file.read_text()
        (self.bin / "deny").write_text("tool")
        with self.assertRaises(PublishError):
            self.run_publish()
        self.assertEqual(before, self.pin_file.read_text())
        self.assertEqual(1, len([line for line in self.pushes() if " app-" in line or "images:app-" in line]))
        (self.bin / "deny").unlink()
        self.assertEqual(["app: pinned from the repository", "tool: pushed and pinned"], self.run_publish())
        self.assertEqual(["app", "tool"], list(pins.load(self.pin_file)))
        self.assertEqual(1, len([line for line in self.pushes() if "images:app-" in line]), "the app tag was pushed once in all")

    def test_the_pin_file_is_written_once_after_every_image_has_resolved(self) -> None:
        with mock.patch.object(pins, "write", wraps=pins.write) as written:
            self.run_publish()
        self.assertEqual(1, written.call_count)
        self.assertEqual({"app", "tool"}, set(written.call_args.args[1]))


class InspectFailureTest(Case):
    """Only the registry's own answer that the tag is absent lets a push follow. Any other failure of the read is a fixed refusal, with no build and no push."""

    def refuse(self, text: str) -> str:
        (self.bin / "inspect_fails").write_text(text)
        with self.assertRaises(PublishError) as raised:
            self.run_publish()
        self.assertEqual([], self.pushes())
        self.assertEqual([], self.builds())
        self.assertNotIn(CANARY_TEXT, str(raised.exception))
        self.assertEqual("images: {}\n", self.pin_file.read_text())
        return str(raised.exception)

    def test_an_unreachable_registry_or_a_timeout_or_any_other_failure_of_the_read_is_not_read_as_an_absent_tag(self) -> None:
        for text in (
            "",
            f"ERROR: {CANARY_TEXT} Get https://registry-1.docker.io/v2/: dial tcp: i/o timeout\n",
            f"ERROR: {CANARY_TEXT} 500 Internal Server Error\n",
            f"docker: 'buildx' is not a docker command: {CANARY_TEXT} not found\n",
            f"ERROR: {CANARY_TEXT} command not found\n",
            f"ERROR: {CANARY_TEXT} repository not found\n",
            f"ERROR: {CANARY_TEXT} manifest unknown\n",
            f"ERROR: {CANARY_TEXT} name unknown\n",
            "ERROR: docker.io/example/other_images:app-1.2-000000000000: not found\n",
            "ERROR: docker.io/TAG: not found, and the registry also said: x\n",
            "WARNING: slow\nERROR: docker.io/TAG: not found\n",
        ):
            with self.subTest(text=text):
                self.assertEqual("app: the repository could not be read", self.refuse(text))

    def test_a_refused_login_in_the_read_gives_the_login_text(self) -> None:
        self.assertEqual(
            "the push was refused: run docker login on this computer, then run the command again",
            self.refuse(f"ERROR: {CANARY_TEXT} requested access to the resource is denied\n"),
        )

    def test_the_registrys_own_not_found_answer_for_the_exact_tag_lets_the_push_follow_with_or_without_the_docker_io_prefix(self) -> None:
        for answer in ("ERROR: docker.io/TAG: not found\n", "ERROR: TAG: not found\r\n", "error: docker.io/TAG: Not Found\n\n"):
            with self.subTest(answer=answer):
                self.setUp()
                (self.bin / "inspect_fails").write_text(answer)
                self.assertEqual(["app: pushed and pinned"], self.run_publish(images=IMAGES[:1]))
                self.assertEqual(1, len(self.pushes()))


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

    def test_a_failure_prints_one_fixed_line_to_standard_error_and_exits_with_one(self) -> None:
        (self.bin / "deny").write_text("")
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = publish.main([], settings=self.settings())
        self.assertEqual(1, code)
        self.assertEqual("publish: the push was refused: run docker login on this computer, then run the command again\n", err.getvalue())
        self.assertNotIn(CANARY_TEXT, out.getvalue() + err.getvalue())

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
