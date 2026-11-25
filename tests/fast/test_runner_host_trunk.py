"""The host mode of the Nessie runner and of the catalog proof never commits to the live ``main`` and gives its private trunk a first commit of its own.

The runners stay untracked, so each test skips when its runner is not on this machine. The Nessie is a stand-in that answers as the live host did on
2026-11-24: ``main`` is the default branch and has no commit. No Docker, Spark or host runs here.

Failure modes covered here:

- The Nessie runner calls ``initialise`` on its private trunk, and Nessie's client refuses any branch but the default one, so the run stops with "is not
  Nessie's default branch". On the private trunk the call must be a refusal check, and the trunk gets its namespace by a commit of its own.
- The catalog proof branches from a private trunk that has no commit and its merge into that trunk is refused with 404 "No common ancestor". The trunk needs
  its first commit before the first branch is made from it.
- Either runner adds a commit to the live ``main``, so the next run starts on a changed catalog.
- A refusal check of the host mode passes on a call that was accepted, or on a refusal with another text.
- The local mode of the Nessie runner changes with the host mode's code: its cases must still pass on ``main`` with every initialise case.
"""

from __future__ import annotations

import ast
import unittest
from pathlib import Path
from types import ModuleType

from scripts.stack.nessie import Initialised, NessieError, put_namespace
from tests.e2e.run_record import Record
from tests.fast.host_nessie_stand_in import EMPTY, HostNessie
from tests.fast.runner_import import import_runner

E2E = Path(__file__).resolve().parents[1] / "e2e"


def runner(name: str) -> ModuleType:
    return import_runner(name)


def record() -> Record:
    return Record(feature="host trunk", slug="host_trunk", rerun="none")


def writes_to_main(client: HostNessie) -> list[str]:
    return [target for method, target in client.writes() if "/trees/main" in target]


class NessieRunnerHostTrunk(unittest.TestCase):
    def private_run(self) -> tuple[HostNessie, Record, str]:
        nessie = runner("stack_nessie")
        client, run = HostNessie(), record()
        names = nessie.Names()
        trunk = names("stack_nessie_trunk")
        client.create_branch(trunk, "main")
        made: list[tuple[str, str]] = [("branch", trunk)]
        nessie.empty_trunk_cases(client, run, trunk, names, made, client.config()["noAncestorHash"], private=True)
        return client, run, trunk

    def test_every_case_passes_and_the_live_main_is_not_written(self) -> None:
        client, run, _ = self.private_run()
        failed = [item.case for item in run.observations if not item.passed]
        self.assertEqual(failed, [])
        self.assertEqual(writes_to_main(client), [])
        self.assertEqual(client.head("main"), EMPTY)

    def test_initialise_on_the_private_trunk_is_a_refusal_check(self) -> None:
        _, run, trunk = self.private_run()
        cases = {item.case: item for item in run.observations}
        self.assertIn("initialise on a private trunk", cases)
        self.assertTrue(cases["initialise on a private trunk"].passed)
        self.assertIn(f"the branch {trunk} is not Nessie's default branch main", cases["initialise on a private trunk"].observed)
        self.assertNotIn("default branch given by name", cases)
        self.assertNotIn("first commit of the trunk", cases)

    def test_the_private_trunk_has_its_own_first_commit_and_takes_a_merge(self) -> None:
        client, run, trunk = self.private_run()
        self.assertNotEqual(client.head(trunk), EMPTY)
        self.assertEqual(client.entries(trunk), ["probe"])
        self.assertIn("first commit of the private trunk", [item.case for item in run.observations])
        branch = client.create_branch("work", trunk)
        client.commit(branch.name, branch.hash, "write", [put_namespace("probe", "one")])
        merged = client.merge("work", trunk, client.head(trunk), "merge", source_hash=client.head("work"))
        self.assertEqual(client.head(trunk), merged)
        self.assertEqual(client.entries(trunk), ["probe", "probe.one"])

    def test_the_stand_in_refuses_a_merge_into_a_trunk_with_no_commit(self) -> None:
        client = HostNessie()
        client.create_branch("trunk", "main")
        branch = client.create_branch("work", "trunk")
        client.commit(branch.name, branch.hash, "write", [put_namespace("probe")])
        with self.assertRaises(NessieError) as caught:
            client.merge("work", "trunk", EMPTY, "merge", source_hash=client.head("work"))
        self.assertEqual((caught.exception.status, "No common ancestor" in str(caught.exception)), (404, True))

    def test_a_refusal_check_fails_when_the_call_is_accepted(self) -> None:
        nessie = runner("stack_nessie")
        client, run = HostNessie(), record()
        client.initialise = lambda branch, namespace: Initialised(branch or "", False)  # type: ignore[method-assign]
        names = nessie.Names()
        trunk = names("t")
        client.create_branch(trunk, "main")
        nessie.empty_trunk_cases(client, run, trunk, names, [], EMPTY, private=True)
        self.assertIn("initialise on a private trunk", [item.case for item in run.observations if not item.passed])

    def test_a_refusal_with_another_text_fails_the_check(self) -> None:
        nessie = runner("stack_nessie")
        client, run = HostNessie(), record()

        def refuse_otherwise(branch: str | None, namespace: str) -> Initialised:
            raise NessieError("the branch is not usable")

        client.initialise = refuse_otherwise  # type: ignore[method-assign]
        names = nessie.Names()
        trunk = names("t")
        client.create_branch(trunk, "main")
        nessie.empty_trunk_cases(client, run, trunk, names, [], EMPTY, private=True)
        self.assertIn("initialise on a private trunk", [item.case for item in run.observations if not item.passed])

    def test_the_first_commit_check_fails_when_the_commit_changes_nothing(self) -> None:
        nessie = runner("stack_nessie")

        class Dropping(HostNessie):
            def serve_commit(self, path: str, payload: bytes | None) -> tuple[int, bytes]:
                name, _ = self.split(path, "/history/commit")
                return self.answer(200, {"targetBranch": {"type": "BRANCH", "name": name, "hash": self.head(name)}})

        client, run = Dropping(), record()
        names = nessie.Names()
        trunk = names("t")
        client.create_branch(trunk, "main")
        nessie.empty_trunk_cases(client, run, trunk, names, [], EMPTY, private=True)
        self.assertIn("first commit of the private trunk", [item.case for item in run.observations if not item.passed])

    def test_the_host_mode_runs_its_cases_as_private_and_the_local_mode_does_not(self) -> None:
        runner("stack_nessie")
        source = ast.parse((E2E / "stack_nessie.py").read_text())
        calls = {
            function.name: [call for call in ast.walk(function) if isinstance(call, ast.Call) and getattr(call.func, "id", "") == "api_cases"]
            for function in source.body
            if isinstance(function, ast.FunctionDef) and function.name in ("run_host", "run_local")
        }
        private = {
            name: [keyword.value.value for call in found for keyword in call.keywords if keyword.arg == "private" and isinstance(keyword.value, ast.Constant)]
            for name, found in calls.items()
        }
        self.assertEqual(private, {"run_host": [True], "run_local": []})

    def test_the_local_mode_keeps_every_initialise_case_on_main(self) -> None:
        nessie = runner("stack_nessie")
        client, run = HostNessie(), record()
        nessie.empty_trunk_cases(client, run, "main", nessie.Names(), [], EMPTY, private=False)
        cases = [item.case for item in run.observations]
        self.assertEqual([item.case for item in run.observations if not item.passed], [])
        for wanted in ("first commit of the trunk", "default branch given by name", "entries for another hash", "head without the namespace"):
            self.assertIn(wanted, cases)
        self.assertNotIn("initialise on a private trunk", cases)


class CatalogProofHostTrunk(unittest.TestCase):
    def test_the_trunk_gets_a_first_commit_before_the_first_branch(self) -> None:
        proof = runner("catalog_proof")
        client = HostNessie()
        trunk = proof.start_private_trunk(client, "proof_trunk_x")
        self.assertEqual(trunk.name, "proof_trunk_x")
        self.assertNotEqual(client.head("proof_trunk_x"), EMPTY)
        self.assertEqual(client.entries("proof_trunk_x"), [proof.NAMESPACE])
        branch = client.create_branch("work", "proof_trunk_x")
        client.commit(branch.name, branch.hash, "write", [put_namespace(proof.NAMESPACE, "numbers")])
        merged = client.merge("work", "proof_trunk_x", client.head("proof_trunk_x"), "merge", source_hash=client.head("work"))
        self.assertEqual(client.head("proof_trunk_x"), merged)

    def test_the_host_mode_makes_its_trunk_with_the_helper_and_not_from_main_alone(self) -> None:
        runner("catalog_proof")
        source = ast.parse((E2E / "catalog_proof.py").read_text())
        host = next(item for item in source.body if isinstance(item, ast.FunctionDef) and item.name == "run_host")
        called = [getattr(call.func, "id", getattr(call.func, "attr", "")) for call in ast.walk(host) if isinstance(call, ast.Call)]
        self.assertIn("start_private_trunk", called)
        self.assertNotIn("create_branch", called)

    def test_the_live_main_is_not_written(self) -> None:
        proof = runner("catalog_proof")
        client = HostNessie()
        proof.start_private_trunk(client, "proof_trunk_x")
        self.assertEqual(writes_to_main(client), [])
        self.assertEqual(client.head("main"), EMPTY)

    def test_a_trunk_that_already_has_a_commit_is_not_given_another(self) -> None:
        proof = runner("catalog_proof")
        client = HostNessie()
        first = client.create_branch("seed", "main")
        client.commit(first.name, first.hash, "seed", [put_namespace(proof.NAMESPACE)])
        client.references["main"] = client.references["seed"]
        before = client.head("main")
        proof.start_private_trunk(client, "proof_trunk_x")
        self.assertEqual(client.head("proof_trunk_x"), before)


if __name__ == "__main__":
    unittest.main()
