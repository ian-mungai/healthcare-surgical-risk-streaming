"""The teardown of a kept host batch checks its inventory before it removes anything and reports success only when every object of the run is gone.

The runner is untracked, so the tests run only where it is on disk. Nothing starts: a stand-in answers every Docker command and every Nessie call.

Failure modes covered here:

- The inventory is read after the containers and volumes were removed, so a missing or foreign inventory leaves a half-done teardown.
- A Kafka topic that the run made is still on the broker (the run's own deletion failed) and the teardown reports success, or the teardown deletes a topic that
  the inventory does not hold for this run.
- A Nessie reference is still there after its deletion, and the teardown counts it as removed.
- The inventory says that the run could not read its references (``references_error``), and the teardown reports success without knowing which exist.
- The teardown cannot ask the broker (the listing fails) and reports success.
- A tag whose creation gave no clear answer (or was cut by an interrupt) may be the run's, and the teardown deletes the lake volume that its snapshots are
  read from, or says "every resource is gone" and exits 0 while it keeps the object.
"""

from __future__ import annotations

import contextlib
import importlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock

from scripts.process import CompletedProcess
from scripts.stack.nessie import NessieError, Reference

ROOT = Path(__file__).resolve().parents[2]
RUNNERS = (ROOT / "tests" / "e2e" / "stream_host.py", ROOT / "tests" / "e2e" / "stream_features.py")
SUFFIX = "0123abcd"
PROJECT = f"p8-host-{SUFFIX}"
HASH_A = "a" * 64
INPUT = f"replay_events_p8h_{SUFFIX}_control"
OUTPUT = f"feature_ready_p8h_{SUFFIX}_control"


def done(code: int = 0, text: str = "", error: str = "") -> CompletedProcess[str]:
    return CompletedProcess(["docker"], code, text, error)


class FakeNessie:
    def __init__(self, references: dict[str, Reference], *, keep: bool = False) -> None:
        self.references = dict(references)
        self.keep = keep
        self.deleted: list[tuple[str, str, str | None]] = []

    def reference(self, name: str) -> Reference:
        if name not in self.references:
            raise NessieError("not found", status=404)
        return self.references[name]

    def delete_reference(self, name: str, hash_value: str, kind: str | None = None) -> None:
        self.deleted.append((name, hash_value, kind))
        if not self.keep:
            self.references.pop(name, None)


class Broker:
    """The Docker commands of a teardown: the project's resources (none left), the deployed broker's topics and the host file."""

    def __init__(self, topics: set[str], *, keep_topics: bool = False, listing_fails: bool = False, volumes: tuple[str, ...] = ()) -> None:
        self.topics = set(topics)
        self.volumes = set(volumes)
        self.keep_topics = keep_topics
        self.listing_fails = listing_fails
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, program: str, args: list[str], **options: Any) -> CompletedProcess[str]:
        self.calls.append(tuple(args))
        if "cat" in args:
            return done(0, "APICURIO_HOST_PORT=18081\nNESSIE_HOST_PORT=19120\n")
        if "compose" in args and "exec" in args:
            if "--delete" in args:
                if not self.keep_topics:
                    self.topics.discard(args[args.index("--topic") + 1])
                return done(0, "")
            if "--list" in args:
                return done(1, "", "no broker") if self.listing_fails else done(0, "".join(f"{name}\n" for name in sorted(self.topics)))
        if args[2:4] == ["volume", "ls"]:
            return done(0, "".join(f"{name}\n" for name in sorted(self.volumes)))
        if args[2:4] == ["volume", "rm"]:
            self.volumes -= set(args[4:])
        return done(0, "")

    def removals(self) -> list[tuple[str, ...]]:
        return [call for call in self.calls if "rm" in call or call[:2] == ("volume", "rm")]

    def deletions(self) -> list[str]:
        return [call[call.index("--topic") + 1] for call in self.calls if "--delete" in call]


@unittest.skipUnless(all(path.is_file() for path in RUNNERS), "the temporary runners are not in this checkout")
class TeardownCase(unittest.TestCase):
    def setUp(self) -> None:
        self.host: ModuleType = importlib.import_module("tests.e2e.stream_host")
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        self.output = io.StringIO()
        self.enterContext(contextlib.redirect_stdout(self.output))
        self.errors = io.StringIO()
        self.enterContext(contextlib.redirect_stderr(self.errors))

    def inventory(self, **changes: Any) -> dict[str, Any]:
        document = {
            "project": PROJECT,
            "references": [{"name": f"stream_control_{SUFFIX}", "type": "BRANCH", "hash": HASH_A}],
            "topics": [INPUT, OUTPUT],
            "references_error": "",
            "unconfirmed": [],
            **changes,
        }
        folder = self.root / f"stream_host_{SUFFIX}"
        folder.mkdir(exist_ok=True)
        (folder / "inventory.json").write_text(json.dumps(document))
        return document

    def tear_down(self, broker: Broker, nessie: FakeNessie) -> int:
        with mock.patch.object(self.host, "run_command", broker), mock.patch.object(self.host, "NessieClient", lambda *args, **options: nessie):
            return int(self.host.teardown_host(PROJECT, self.root))

    def branch(self) -> FakeNessie:
        return FakeNessie({f"stream_control_{SUFFIX}": Reference("BRANCH", f"stream_control_{SUFFIX}", HASH_A)})


class InventoryIsCheckedFirst(TeardownCase):
    def test_a_missing_inventory_refuses_before_anything_is_removed(self) -> None:
        broker = Broker(set())
        code = self.tear_down(broker, self.branch())
        self.assertEqual(2, code)
        self.assertEqual([], broker.removals())
        self.assertEqual([], broker.calls)

    def test_an_inventory_that_is_not_this_projects_refuses_before_anything_is_removed(self) -> None:
        for change in (
            {"project": "p8-host-ffffffff"},
            {"topics": ["replay_events"]},
            {"topics": ["replay_events_p8h_ffffffff_control"]},
            {"topics": "none"},
            {"references_error": 5},
            {"unconfirmed": "none"},
            {"references": [{"name": "main", "type": "BRANCH", "hash": HASH_A}]},
        ):
            with self.subTest(change=change):
                self.inventory(**change)
                broker = Broker(set())
                self.assertEqual(2, self.tear_down(broker, self.branch()))
                self.assertEqual([], broker.calls)

    def test_a_key_that_the_inventory_always_holds_must_be_there(self) -> None:
        for key in ("topics", "references_error", "unconfirmed", "references"):
            with self.subTest(key=key):
                document = self.inventory()
                del document[key]
                (self.root / f"stream_host_{SUFFIX}" / "inventory.json").write_text(json.dumps(document))
                broker = Broker(set())
                self.assertEqual(2, self.tear_down(broker, self.branch()))
                self.assertEqual([], broker.calls)


class TopicsAreVerified(TeardownCase):
    def test_a_run_that_deleted_its_topics_is_checked_with_one_listing_and_passes(self) -> None:
        self.inventory()
        broker = Broker({"replay_events", "feature_ready", "warnings"})
        self.assertEqual(0, self.tear_down(broker, self.branch()), self.output.getvalue())
        self.assertEqual([], broker.deletions())

    def test_a_topic_that_is_still_on_the_broker_is_deleted_by_name_and_checked_again(self) -> None:
        self.inventory()
        broker = Broker({"replay_events", "feature_ready", INPUT})
        self.assertEqual(0, self.tear_down(broker, self.branch()), self.output.getvalue())
        self.assertEqual([INPUT], broker.deletions())
        self.assertEqual({"replay_events", "feature_ready"}, broker.topics)

    def test_a_topic_that_cannot_be_deleted_fails_the_teardown_and_names_the_topic(self) -> None:
        self.inventory()
        broker = Broker({INPUT}, keep_topics=True)
        self.assertEqual(1, self.tear_down(broker, self.branch()))
        self.assertIn(INPUT, self.output.getvalue())

    def test_a_listing_that_fails_fails_the_teardown(self) -> None:
        self.inventory()
        broker = Broker(set(), listing_fails=True)
        self.assertEqual(1, self.tear_down(broker, self.branch()))
        self.assertIn("topics", self.output.getvalue())

    def test_only_the_topics_of_the_inventory_are_ever_deleted(self) -> None:
        self.inventory()
        broker = Broker({INPUT, OUTPUT, "replay_events", "replay_events_p8h_ffffffff_control"})
        self.tear_down(broker, self.branch())
        self.assertEqual({INPUT, OUTPUT}, set(broker.deletions()))

    def test_the_broker_is_asked_with_the_platform_command_and_the_topic_tool_only(self) -> None:
        self.inventory()
        broker = Broker({INPUT})
        self.tear_down(broker, self.branch())
        asked = [call for call in broker.calls if "compose" in call]
        self.assertTrue(asked)
        for call in asked:
            self.assertEqual("platform", self.host.compose_target(call[call.index("--project-name") + 2 :]))


class ReferencesAreVerified(TeardownCase):
    def test_a_reference_that_is_still_there_after_its_deletion_is_a_failure(self) -> None:
        self.inventory()
        nessie = FakeNessie({f"stream_control_{SUFFIX}": Reference("BRANCH", f"stream_control_{SUFFIX}", HASH_A)}, keep=True)
        self.assertEqual(1, self.tear_down(Broker(set()), nessie))
        self.assertIn(f"stream_control_{SUFFIX}", self.output.getvalue())

    def test_the_removal_helper_reads_each_reference_again_after_its_deletion(self) -> None:
        entries = [{"name": f"stream_control_{SUFFIX}", "type": "BRANCH", "hash": HASH_A}]
        client = FakeNessie({f"stream_control_{SUFFIX}": Reference("BRANCH", f"stream_control_{SUFFIX}", HASH_A)}, keep=True)
        removed, failures = self.host.teardown_references(client, entries)
        self.assertEqual(0, removed)
        self.assertEqual(1, len(failures))

    def test_a_references_error_in_the_inventory_fails_the_teardown_even_when_everything_else_is_gone(self) -> None:
        self.inventory(references_error="NessieError: down")
        self.assertEqual(1, self.tear_down(Broker(set()), FakeNessie({})))
        self.assertIn("references", self.output.getvalue())

    def test_a_clean_run_passes(self) -> None:
        self.inventory()
        nessie = self.branch()
        self.assertEqual(0, self.tear_down(Broker(set()), nessie), self.output.getvalue())
        self.assertEqual({}, nessie.references)

    def test_a_topic_of_unconfirmed_ownership_is_left_alone_and_the_teardown_is_incomplete(self) -> None:
        self.inventory(unconfirmed=[f"topic replay_events_p8h_{SUFFIX}_late"])
        broker = Broker({f"replay_events_p8h_{SUFFIX}_late"})
        self.assertEqual(1, self.tear_down(broker, self.branch()))
        self.assertEqual([], broker.deletions())
        text = self.output.getvalue()
        self.assertIn(f"replay_events_p8h_{SUFFIX}_late", text)
        self.assertIn("INCOMPLETE", text)
        self.assertNotIn("every resource is gone", text)


class UnresolvedTags(TeardownCase):
    VOLUME = f"{PROJECT}_lake_warehouse"
    TAG = f"tag stream_features_{SUFFIX}"

    def test_a_tag_of_unconfirmed_ownership_keeps_the_lake_volume_and_the_teardown_is_incomplete(self) -> None:
        self.inventory(unconfirmed=[self.TAG])
        broker = Broker(set(), volumes=(self.VOLUME,))
        self.assertEqual(1, self.tear_down(broker, self.branch()))
        self.assertEqual({self.VOLUME}, broker.volumes)
        self.assertNotIn(("volume", "rm"), [call[2:4] for call in broker.calls])
        text = self.output.getvalue()
        self.assertIn("INCOMPLETE", text)
        self.assertIn(self.TAG, text)
        self.assertIn(self.VOLUME, text)
        self.assertNotIn("every resource is gone", text)

    def test_the_other_resources_of_the_project_are_still_removed(self) -> None:
        self.inventory(unconfirmed=[self.TAG])
        broker = Broker({INPUT}, volumes=(self.VOLUME,))
        nessie = self.branch()
        self.assertEqual(1, self.tear_down(broker, nessie))
        self.assertEqual([INPUT], broker.deletions())
        self.assertEqual({}, nessie.references)

    def test_a_second_teardown_keeps_the_volume_again(self) -> None:
        self.inventory(unconfirmed=[self.TAG])
        broker = Broker(set(), volumes=(self.VOLUME,))
        for _ in range(2):
            self.assertEqual(1, self.tear_down(broker, self.branch()))
        self.assertEqual({self.VOLUME}, broker.volumes)

    def test_a_tag_that_is_both_recorded_and_unconfirmed_is_never_deleted(self) -> None:
        name = f"stream_features_{SUFFIX}"
        self.inventory(references=[{"name": name, "type": "TAG", "hash": HASH_A}], unconfirmed=[self.TAG])
        broker = Broker(set(), volumes=(self.VOLUME,))
        nessie = FakeNessie({name: Reference("TAG", name, HASH_A)})
        self.assertEqual(1, self.tear_down(broker, nessie))
        self.assertEqual([], nessie.deleted)
        self.assertIn(name, nessie.references)
        self.assertEqual({self.VOLUME}, broker.volumes)
        text = self.output.getvalue()
        self.assertIn("INCOMPLETE", text)
        self.assertIn(f"never deleted: ['{self.TAG}']", text)
        self.assertNotIn("every resource is gone", text)

    def test_the_other_references_of_that_inventory_are_still_deleted(self) -> None:
        name = f"stream_features_{SUFFIX}"
        branch = f"stream_control_{SUFFIX}"
        entries = [{"name": branch, "type": "BRANCH", "hash": HASH_A}, {"name": name, "type": "TAG", "hash": HASH_A}]
        self.inventory(references=entries, unconfirmed=[self.TAG])
        nessie = FakeNessie({branch: Reference("BRANCH", branch, HASH_A), name: Reference("TAG", name, HASH_A)})
        self.assertEqual(1, self.tear_down(Broker(set(), volumes=(self.VOLUME,)), nessie))
        self.assertEqual([(branch, HASH_A, "BRANCH")], nessie.deleted)
        self.assertEqual({name}, set(nessie.references))

    def test_a_topic_alone_does_not_keep_the_volume(self) -> None:
        self.inventory(unconfirmed=[f"topic replay_events_p8h_{SUFFIX}_late"])
        broker = Broker(set(), volumes=(self.VOLUME,))
        self.assertEqual(1, self.tear_down(broker, self.branch()))
        self.assertEqual(set(), broker.volumes)

    def test_a_run_with_nothing_unresolved_removes_the_volume_and_says_so(self) -> None:
        self.inventory()
        broker = Broker(set(), volumes=(self.VOLUME,))
        self.assertEqual(0, self.tear_down(broker, self.branch()), self.output.getvalue())
        self.assertEqual(set(), broker.volumes)
        self.assertIn("every resource is gone", self.output.getvalue())


if __name__ == "__main__":
    unittest.main()
