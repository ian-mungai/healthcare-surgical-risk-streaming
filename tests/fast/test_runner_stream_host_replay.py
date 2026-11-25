"""The host batch reads what the replay container wrote, runs its parity probe in an image that holds everything the probe imports, and proves the replay's
restart by the exact IDs that were sent and not yet saved.

The runners are untracked, so the tests run only where they are on disk. Nothing starts: stand-ins answer every command.

Failure modes covered here:

- The replay container writes its manifest and observation as another user with mode 0640, so the host's user cannot read them, or reads them through a path
  that is not exactly one of those files.
- The parity probe imports ``replay.service.decode``, which the deployed Spark image does not hold, or imports a module that the replay image does not hold, or
  a package that the replay image does not install: the probe stops at its first import.
- The replay's restart is tested by a kill at a random instant, so the IDs that are sent again are not known: the check only says that some are repeated.
- The saved position moved on after the fault (the unsaved batch was saved), so the restart sends nothing again and the case passes with nothing proved.
- The restart case reads the topics a second time, which the replay refuses.
- The late-event probe takes its input topic from its arguments and writes to the deployed ``replay_events`` (or to any topic that the sink would take) on the
  host, where only the topic of the run may be written.
"""

from __future__ import annotations

import ast
import importlib
import importlib.util
import inspect
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock

from scripts.process import CompletedProcess
from scripts.stack.yaml_text import load_yaml
from tests.e2e.run_record import Record
from tests.fast.runner_import import import_runner

ROOT = Path(__file__).resolve().parents[2]
RUNNERS = (ROOT / "tests" / "e2e" / "stream_host.py", ROOT / "tests" / "e2e" / "stream_features.py", ROOT / "tests" / "e2e" / "stack_replay.py")
PROBES = ROOT / "tests" / "e2e" / "stream_stack"
OVERLAY = PROBES / "compose.host.yml"
FAULT = PROBES / "replay_fault.py"


def done(code: int = 0, text: str = "", error: str = "") -> CompletedProcess[str]:
    return CompletedProcess(["docker"], code, text, error)


@unittest.skipUnless(all(path.is_file() for path in RUNNERS), "the temporary runners are not in this checkout")
class ReplayCase(unittest.TestCase):
    def setUp(self) -> None:
        self.host: ModuleType = import_runner("stream_host")
        self.features: ModuleType = import_runner("stream_features")
        self.replay: ModuleType = import_runner("stack_replay")
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.scratch = Path(scratch.name)
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    def stack(self, answers: dict[str, CompletedProcess[str]] | None = None) -> Any:
        record = Record(feature="stand-in", slug="stand_in", rerun="none")
        work = self.scratch / "work"
        (work / "run").mkdir(parents=True, exist_ok=True)

        def runner(program: str, args: list[str], **options: Any) -> CompletedProcess[str]:
            self.calls.append((program, tuple(args)))
            for word, answer in (answers or {}).items():
                if word in args or word in " ".join(args):
                    return answer
            return done()

        patched = mock.patch.object(self.host, "run_command", runner)
        patched.start()
        self.addCleanup(patched.stop)
        return self.host.HostStack(record, [], "replay:test", "spark:test", work / "landed", work / "run", work)


class ReadAccess(ReplayCase):
    def test_a_file_of_the_replay_is_read_with_sudo_cat_of_exactly_that_path(self) -> None:
        stack = self.stack({"cat": done(0, '{"a": 1}')})
        text = stack.read_run_file("control_manifest.json")
        self.assertEqual('{"a": 1}', text)
        self.assertEqual(("sudo", ("-n", "cat", str(stack.run_dir / "control_manifest.json"))), self.calls[-1])

    def test_a_name_that_is_not_a_json_file_of_the_run_folder_is_refused_before_anything_is_sent(self) -> None:
        stack = self.stack()
        for name in ("../etc/passwd", "a/b.json", "x.txt", "X.json", "/etc/shadow", "", ".json", "a b.json"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                stack.read_run_file(name)
        self.assertEqual([], self.calls)

    def test_a_read_that_fails_blocks_the_run_with_the_name_of_the_file(self) -> None:
        stack = self.stack({"cat": done(1, "", "denied")})
        with self.assertRaisesRegex(self.replay.Blocked, "control_manifest.json"):
            stack.read_run_file("control_manifest.json")

    def test_no_step_reads_a_file_of_the_replay_straight_from_the_run_folder(self) -> None:
        sources = [
            inspect.getsource(self.replay.Case.build_manifest),
            inspect.getsource(self.replay.Case.observe),
            inspect.getsource(self.features.replay_resent),
        ]
        for source in sources:
            self.assertNotIn("run_dir /", source)
        self.assertIn("read_run_file", sources[0] + sources[1])


class ProbeImage(unittest.TestCase):
    """The probe runs in the replay image. What it imports must be copied by the replay's Dockerfile or mounted over its ``features`` folder."""

    def setUp(self) -> None:
        self.document: Any = load_yaml(OVERLAY.read_text()) if OVERLAY.is_file() else None
        if self.document is None:
            self.skipTest("the temporary overlay is not in this checkout")

    def test_the_batch_probe_runs_in_the_replay_image_as_the_replay_user(self) -> None:
        batch = self.document["services"]["batch"]
        self.assertEqual("${REPLAY_IMAGE:?name the replay image}", batch["image"])
        self.assertEqual("10003:10003", batch["user"])
        self.assertEqual(["python", "/probe/batch_features.py"], batch["entrypoint"])
        self.assertEqual("/opt/replay/app:/probe", batch["environment"]["PYTHONPATH"])

    def test_the_feature_code_is_mounted_read_only_over_the_images_features_folder(self) -> None:
        mounts = self.document["services"]["batch"]["volumes"]
        self.assertIn("${FEATURES_DIR:?name the feature code folder}:/opt/replay/app/features:ro", mounts)

    def test_the_spark_image_is_not_the_probes_image(self) -> None:
        self.assertNotIn("SPARK_IMAGE", json.dumps(self.document["services"]["batch"]))

    def dockerfile_sources(self) -> list[str]:
        sources = []
        for line in (ROOT / "replay" / "Dockerfile").read_text().splitlines():
            if line.startswith("COPY "):
                parts = line.split()[1:]
                sources += [part for part in parts[:-1] if not part.startswith("--")]
        return sources

    def covered(self, relative: str, sources: list[str]) -> bool:
        return any(relative == source.rstrip("/") or relative.startswith(source.rstrip("/") + "/") for source in sources)

    def first_party(self, name: str, folder: Path) -> Path | None:
        for base in (ROOT, folder):
            candidate = base / (name.replace(".", "/") + ".py")
            if candidate.is_file():
                return candidate
            package = base / name.replace(".", "/") / "__init__.py"
            if package.is_file():
                return package
        return None

    def is_first_party(self, top: str) -> bool:
        return (ROOT / top).is_dir() or (ROOT / f"{top}.py").is_file() or (PROBES / f"{top}.py").is_file()

    def closure(self) -> tuple[set[Path], set[str]]:
        """The first-party files that the probe imports, followed through their own imports, and the names of the packages that are not first-party."""
        seen: set[Path] = set()
        outside: set[str] = set()
        pending = [PROBES / "batch_features.py"]
        while pending:
            path = pending.pop()
            if path in seen:
                continue
            seen.add(path)
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.Import):
                    named = [alias.name for alias in node.names]
                    wanted = list(named)
                elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                    named = [node.module]
                    wanted = [node.module, *(f"{node.module}.{alias.name}" for alias in node.names)]
                else:
                    continue
                for name in named:
                    top = name.split(".")[0]
                    if top not in sys.stdlib_module_names and not self.is_first_party(top):
                        outside.add(top)
                for name in wanted:
                    found = self.first_party(name, PROBES)
                    if found is not None:
                        pending.append(found)
                        if found.parent != PROBES:
                            pending += [parent / "__init__.py" for parent in found.parents if ROOT in parent.parents and (parent / "__init__.py").is_file()]
        return seen, outside

    def test_every_module_the_probe_imports_is_in_the_replay_image_or_in_the_mounted_features_folder(self) -> None:
        seen, _ = self.closure()
        sources = self.dockerfile_sources()
        missing = []
        for path in sorted(seen):
            if path.parent == PROBES:
                continue
            relative = path.relative_to(ROOT).as_posix()
            if relative.startswith("features/"):
                continue
            if not self.covered(relative, sources):
                missing.append(relative)
        self.assertEqual([], missing)

    def test_the_probe_does_not_need_the_spark_image_only_decoder_path(self) -> None:
        seen, _ = self.closure()
        self.assertIn(ROOT / "replay" / "service" / "decode.py", seen)
        self.assertTrue(self.covered("replay/service/decode.py", self.dockerfile_sources()))

    def test_every_package_the_probe_imports_is_installed_in_the_replay_image(self) -> None:
        _, outside = self.closure()
        locked = {
            line.split("==")[0].strip().lower().replace("_", "-") for line in (ROOT / "replay" / "requirements.txt").read_text().splitlines() if "==" in line
        }
        self.assertIn("vitaldb", outside)
        self.assertEqual(set(), {name.lower().replace("_", "-") for name in outside} - locked)


@unittest.skipUnless(all(path.is_file() for path in RUNNERS), "the temporary runners are not in this checkout")
class Folders(ReplayCase):
    def test_the_features_folder_is_copied_for_the_probe_and_readable_by_the_replay_user(self) -> None:
        work = self.scratch / "prep"
        work.mkdir()
        target = self.host.copy_feature_code(work)
        names = {path.name for path in target.glob("*.py")}
        self.assertTrue({"batch.py", "row.py", "sampling.py", "__init__.py"} <= names)
        for path in (target, *target.glob("*.py")):
            self.assertTrue(path.stat().st_mode & 0o004, path)
        self.assertFalse(list(target.rglob("__pycache__")))

    def test_the_env_file_names_the_features_folder(self) -> None:
        stack = self.stack()
        stack.write_env_file()
        self.assertIn("FEATURES_DIR", self.host.parse_environment_file(stack.env_file.read_text()))


def load_fault() -> ModuleType:
    spec = importlib.util.spec_from_file_location("replay_fault_probe", FAULT)
    if spec is None or spec.loader is None:
        raise ImportError(str(FAULT))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@unittest.skipUnless(FAULT.is_file(), "the temporary fault probe is not in this checkout")
class FaultHook(unittest.TestCase):
    def setUp(self) -> None:
        self.module = load_fault()
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.path = Path(scratch.name) / "fault.json"

    def test_the_run_stops_at_the_save_after_a_batch_was_sent_and_before_that_batch_is_saved(self) -> None:
        fault = self.module.Fault(self.path, after_saves=1)
        for event in ("a", "b"):
            fault.sent(event)
        self.assertFalse(fault.before_save())
        fault.saved("state one")
        for event in ("c", "d", "e"):
            fault.sent(event)
        self.assertTrue(fault.before_save())
        written = json.loads(self.path.read_text())
        self.assertEqual(["c", "d", "e"], written["unsaved_ids"])
        self.assertEqual(1, written["saves_before"])
        self.assertEqual(self.module.digest("state one"), written["saved_state_sha256"])

    def test_the_ids_of_a_batch_that_was_saved_are_never_in_the_fault_file(self) -> None:
        fault = self.module.Fault(self.path, after_saves=1)
        fault.sent("a")
        fault.before_save()
        fault.saved("s")
        fault.sent("b")
        fault.before_save()
        self.assertNotIn("a", json.loads(self.path.read_text())["unsaved_ids"])

    def test_a_save_with_nothing_sent_since_the_last_one_does_not_stop_the_run(self) -> None:
        fault = self.module.Fault(self.path, after_saves=0)
        self.assertFalse(fault.before_save())
        self.assertFalse(self.path.exists())

    def test_with_no_saves_required_the_first_save_of_a_sent_batch_stops_the_run(self) -> None:
        fault = self.module.Fault(self.path, after_saves=0)
        fault.sent("a")
        self.assertTrue(fault.before_save())

    def test_the_file_is_complete_before_the_run_is_told_to_stop(self) -> None:
        fault = self.module.Fault(self.path, after_saves=0)
        fault.sent("a")
        seen: list[bool] = []
        original = fault.before_save

        def checking() -> bool:
            stop = bool(original())
            seen.append(self.path.is_file())
            return stop

        self.assertTrue(checking())
        self.assertEqual([True], seen)

    def test_a_run_that_never_reaches_the_fault_leaves_no_file(self) -> None:
        fault = self.module.Fault(self.path, after_saves=3)
        fault.sent("a")
        fault.before_save()
        fault.saved("s")
        self.assertFalse(self.path.exists())


PLANT = PROBES / "plant_late.py"
LIBRARIES = ("confluent_kafka", "confluent_kafka.admin", "fastavro", "fastavro.schema")


def plant_arguments(topic: str) -> list[str]:
    """The arguments of the late-event probe as the replay image runs it: ``python plant_late.py <bootstrap> <registry> <run> <topic> <second run>``."""
    return ["plant_late.py", "kafka:9092", "http://registry", "run_a", topic, "run_b"]


def load_plant(topic: str) -> ModuleType:
    """The late-event probe module, loaded with its arguments in ``sys.argv`` (it reads them when it is imported)."""
    stand_ins = {name: mock.MagicMock() for name in LIBRARIES if importlib.util.find_spec(name.split(".")[0]) is None}
    spec = importlib.util.spec_from_file_location("plant_late_probe", PLANT)
    if spec is None or spec.loader is None:
        raise ImportError(str(PLANT))
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, stand_ins), mock.patch.object(sys, "argv", plant_arguments(topic)):
        spec.loader.exec_module(module)
    return module


@unittest.skipUnless(PLANT.is_file(), "the temporary probe is not in this checkout")
class PlantTopic(unittest.TestCase):
    def run_main(self, topic: str, required: str | None) -> tuple[int, mock.Mock]:
        module = load_plant(topic)
        environment = {} if required is None else {"REQUIRE_RUN_TOPIC": required}
        with (
            mock.patch.dict(os.environ, environment),
            mock.patch.object(sys, "argv", plant_arguments(topic)),
            mock.patch.object(module, "KafkaSink") as sink,
            mock.patch.object(module, "RegistryClient"),
            mock.patch.object(module, "Writer"),
        ):
            sink.return_value.send.side_effect = RuntimeError("stop after the first message")
            try:
                code = module.main()
            except RuntimeError:
                code = 0
        return code, sink

    def test_on_the_host_the_deployed_input_topic_is_refused_before_a_sink_exists(self) -> None:
        code, sink = self.run_main("replay_events", "true")
        self.assertEqual(2, code)
        sink.assert_not_called()

    def test_on_the_host_a_name_that_is_not_a_run_topic_is_refused_before_a_sink_exists(self) -> None:
        for topic in ("feature_ready", "warnings", "replay_events_", "Replay_events_x", "other"):
            with self.subTest(topic=topic):
                code, sink = self.run_main(topic, "true")
                self.assertEqual(2, code)
                sink.assert_not_called()

    def test_on_the_host_a_topic_of_a_run_is_taken(self) -> None:
        code, sink = self.run_main("replay_events_p8h_0123abcd_late", "true")
        self.assertEqual(0, code)
        self.assertEqual("replay_events_p8h_0123abcd_late", sink.call_args.kwargs["input_topic"])

    def test_the_local_stack_may_still_use_the_deployed_name_of_its_own_throwaway_broker(self) -> None:
        code, sink = self.run_main("replay_events", None)
        self.assertEqual(0, code)
        self.assertEqual("replay_events", sink.call_args.kwargs["input_topic"])

    def test_the_host_overlay_requires_a_run_topic_for_every_probe_of_its_replay_service(self) -> None:
        document: Any = load_yaml(OVERLAY.read_text())
        self.assertEqual("true", document["services"]["replay"]["environment"]["REQUIRE_RUN_TOPIC"])


@unittest.skipUnless(all(path.is_file() for path in RUNNERS), "the temporary runners are not in this checkout")
class PlantGuard(ReplayCase):
    def plant(self, topic: str) -> tuple[str, ...]:
        return ("run", "--rm", "-T", "--no-deps", "--entrypoint", "python", "replay", "/probe/plant_late.py", "kafka:9092", "http://r", "run_a", topic, "run_b")

    def test_the_host_sends_the_probe_only_with_a_topic_of_its_own_run(self) -> None:
        stack = self.stack()
        for topic in ("replay_events", "feature_ready", "replay_events_p8h_ffffffff_late", f"feature_ready_p8h_{stack.token}_late"):
            with self.subTest(topic=topic), self.assertRaises(self.host.PlatformRefusal):
                stack.compose(*self.plant(topic))
        stack.compose(*self.plant(f"replay_events_p8h_{stack.token}_late"))

    def test_another_probe_is_not_held_to_the_rule(self) -> None:
        stack = self.stack()
        stack.compose("run", "--rm", "-T", "--no-deps", "--entrypoint", "python", "replay", "/probe/read_ready.py", "kafka:9092", "http://r", "t")


@unittest.skipUnless(all(path.is_file() for path in RUNNERS), "the temporary runners are not in this checkout")
class ResentIds(ReplayCase):
    def test_the_repeated_ids_must_be_exactly_the_ids_sent_and_not_saved(self) -> None:
        observed = {"a": ["x"], "b": ["y", "y"], "c": ["z", "z"], "d": ["w"]}
        self.assertEqual([], self.host.resent_problems({"b", "c"}, observed, {"a", "b", "c", "d"}))

    def test_an_id_that_is_repeated_but_was_not_unsaved_is_named(self) -> None:
        observed = {"a": ["x", "x"], "b": ["y", "y"]}
        found = self.host.resent_problems({"b"}, observed, {"a", "b"})
        self.assertEqual(1, len(found))
        self.assertIn("a", found[0])

    def test_an_unsaved_id_that_was_not_sent_again_is_named(self) -> None:
        observed = {"a": ["x"], "b": ["y"]}
        found = self.host.resent_problems({"a", "b"}, observed, {"a", "b"})
        self.assertTrue(any("not sent again" in item for item in found))

    def test_a_repeat_with_another_payload_is_named(self) -> None:
        found = self.host.resent_problems({"a"}, {"a": ["x", "y"]}, {"a"})
        self.assertTrue(any("another payload" in item for item in found))

    def test_a_missing_or_unexpected_id_is_named(self) -> None:
        found = self.host.resent_problems(set(), {"a": ["x"], "z": ["q"]}, {"a", "b"})
        self.assertTrue(any("missing" in item and "b" in item for item in found))
        self.assertTrue(any("unexpected" in item and "z" in item for item in found))

    def test_a_fault_that_sent_nothing_proves_nothing(self) -> None:
        self.assertTrue(self.host.resent_problems(set(), {"a": ["x"]}, {"a"}))

    def test_the_saved_state_must_be_the_one_before_the_unsaved_batch(self) -> None:
        self.assertEqual([], self.host.saved_state_problems("abc", "abc"))
        self.assertTrue(self.host.saved_state_problems("abc", "def"))
        self.assertTrue(self.host.saved_state_problems("abc", None))

    def test_the_case_reads_the_topics_once(self) -> None:
        tree = ast.parse(inspect.getsource(self.host.replay_resent_host))
        observes = [node for node in ast.walk(tree) if isinstance(node, ast.Call) and ast.unparse(node.func).endswith(".observe")]
        self.assertEqual(1, len(observes))

    def test_the_main_run_uses_the_host_restart_case(self) -> None:
        source = inspect.getsource(self.host.run_batch)
        self.assertIn("replay_resent_host(stack", source)
        self.assertNotRegex(source, r"\breplay_resent\(")


if __name__ == "__main__":
    unittest.main()
