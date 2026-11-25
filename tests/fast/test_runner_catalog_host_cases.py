"""The cases of the catalog proof that the host mode ran on 2026-11-24 are shaped by what the host answered, not by what the local mode answers.

The runner stays untracked, so each test skips when it is not on this machine. The stand-ins answer as the live host did (run 20261125T000359Z): Trino's
``lake`` catalog follows ``main``, which holds no proof table, and ``lake_pinned`` holds it at the run's tag. The job prints the fixed line
``<job>: timeout`` after other lines. The AWS CLI refuses ``--body /dev/null`` with exit 252. No Docker, Spark, SSH or host runs here.

Failure modes covered here:

- The read-only case sends its insert and its drop to ``lake``, where the table does not exist on the host, and reads TABLE_NOT_FOUND where it needs
  PERMISSION_DENIED. The host-transport case reuses the drop and fails with it.
- The deadline case waits for a message with a duration that the job never prints, or for a line that is not the job's own, or for another exit code.
- The Trino workload's write is sent with a body that the CLI refuses before it sends anything, so the refusal is not the bucket's. The probe key may
  also exist after the write without the case noticing.
- The host mode reads "the table on main" and the later write's row count from a constant: it substitutes the rows it wrote, or 25, and passes on a main
  that moved.
- A private trunk whose seed commit fails is not in the list of references to delete, and stays in Nessie.
"""

from __future__ import annotations

import ast
import importlib
import re
import unittest
from collections.abc import Callable
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock
from urllib.parse import unquote, urlsplit

from scripts.project_config import ROOT
from scripts.stack.nessie import NessieError
from scripts.stack.trino import TrinoError
from tests.e2e.run_record import Record
from tests.fast.host_nessie_stand_in import HostNessie

E2E = Path(__file__).resolve().parents[1] / "e2e"
BUCKET = "bucket-x"
TAG_STATE = "ready"


def runner() -> ModuleType:
    if not (E2E / "catalog_proof.py").is_file():
        raise unittest.SkipTest("the untracked runners are not on this machine")
    return importlib.import_module("tests.e2e.catalog_proof")


def record() -> Record:
    return Record(feature="catalog host cases", slug="catalog_host_cases", rerun="none")


def failed(run: Record) -> list[str]:
    return [item.case for item in run.observations if not item.passed]


def observation(run: Record, case: str) -> Any:
    return next(item for item in run.observations if item.case == case)


class HostTrino:
    """The Trino of the live host: ``lake`` follows main (no proof table there), ``lake_pinned`` is pinned to the tag (the table is there)."""

    def __init__(self, rows: int = 10, pinned_answer: str = "PERMISSION_DENIED") -> None:
        self.rows = rows
        self.pinned_answer = pinned_answer
        self.queries: list[str] = []

    def query(self, sql: str) -> list[list[Any]]:
        self.queries.append(sql)
        catalog = re.search(r"\b(lake_pinned|lake)\.", sql)
        if catalog is None:
            raise AssertionError(sql)
        pinned = catalog.group(1) == "lake_pinned"
        if sql.startswith(("CREATE", "INSERT", "DROP")):
            name = self.pinned_answer if pinned else ("PERMISSION_DENIED" if sql.startswith("CREATE") else "TABLE_NOT_FOUND")
            if name == "accepted":
                return []
            raise TrinoError("refused", error_name=name)
        if not pinned:
            raise TrinoError("table not found", error_name="TABLE_NOT_FOUND")
        if sql.startswith("SHOW TABLES"):
            return [["numbers"]]
        if "snapshot_id" in sql:
            return [[7]]
        if sql.startswith("SELECT count"):
            return [[self.rows]]
        return [[n, n * n] for n in range(1, self.rows + 1)]


class Stack:
    """The parts of the runner's ``Stack`` that these cases call. ``jobs`` answers the Spark job, ``aws`` answers each ``docker`` call."""

    def __init__(self, jobs: Callable[[list[str], dict[str, str] | None], tuple[int, str, str, float]] | None = None, aws: Any = None) -> None:
        self.jobs = jobs
        self.aws = aws
        self.left: list[str] = []
        self.job_calls: list[tuple[list[str], dict[str, str] | None]] = []
        self.docker_calls: list[tuple[str, ...]] = []
        self.target = SimpleNamespace(remove_file=lambda path: None, run=lambda *a, **k: (0, "", ""))

    def job(self, arguments: list[str], environment: dict[str, str] | None = None) -> tuple[int, str, str, float]:
        self.job_calls.append((arguments, environment))
        if self.jobs is None:
            raise AssertionError("no job answers")
        return self.jobs(arguments, environment)

    def spark_containers(self) -> list[str]:
        return self.left

    def docker(self, *arguments: str, timeout: float = 0) -> tuple[int, str]:
        self.docker_calls.append(arguments)
        if self.aws is None:
            raise AssertionError("no docker answers")
        answer: tuple[int, str] = self.aws(arguments)
        return answer

    def container(self, service: str) -> str:
        return ""

    def pin_trino(self, reference: str | None, scratch_file: str) -> float | None:
        return 2.0


def proof_of(module: ModuleType, stack: Any, trino: Any = None, nessie: Any = None, run: Record | None = None, trunk: str = "proof_trunk_x") -> Any:
    return module.Proof(stack, nessie or HostNessie(), trino or HostTrino(), run or record(), trunk, "pin.env")


class ReadOnlyOnTheHost(unittest.TestCase):
    def test_every_write_targets_the_catalog_that_is_pinned_to_the_tag(self) -> None:
        module = runner()
        self.assertEqual(set(module.WRITES), {"create table", "insert", "drop table"})
        for label, sql in module.WRITES.items():
            self.assertRegex(sql, r"\blake_pinned\.catalog_proof\.", label)
            self.assertNotRegex(sql, r"\blake\.", label)

    def test_the_read_only_case_passes_for_the_answers_of_the_host(self) -> None:
        module = runner()
        proof = proof_of(module, Stack())
        proof.facts["rows_at_tag"] = 10
        proof.read_only()
        self.assertEqual(failed(proof.record), [])
        self.assertIn("'insert': 'PERMISSION_DENIED'", observation(proof.record, "Trino is read-only").observed)

    def test_the_read_only_case_fails_when_the_pinned_catalog_accepts_a_write(self) -> None:
        module = runner()
        proof = proof_of(module, Stack(), HostTrino(pinned_answer="accepted"))
        proof.facts["rows_at_tag"] = 10
        proof.read_only()
        self.assertEqual(failed(proof.record), ["Trino is read-only"])

    def test_the_read_only_case_fails_when_a_write_is_refused_for_another_reason(self) -> None:
        module = runner()
        proof = proof_of(module, Stack(), HostTrino(pinned_answer="TABLE_NOT_FOUND"))
        proof.facts["rows_at_tag"] = 10
        proof.read_only()
        self.assertEqual(failed(proof.record), ["Trino is read-only"])

    def test_the_read_only_case_fails_when_the_rows_changed(self) -> None:
        module = runner()
        proof = proof_of(module, Stack(), HostTrino(rows=9))
        proof.facts["rows_at_tag"] = 10
        proof.read_only()
        self.assertEqual(failed(proof.record), ["Trino is read-only"])

    def test_the_host_transport_case_meets_the_refusal_of_the_pinned_catalog(self) -> None:
        module = runner()
        trino = HostTrino()
        proof = proof_of(module, Stack(), trino)
        with mock.patch.object(module, "ExecTrino", lambda stack: trino):
            proof.exec_transport()
        self.assertEqual(failed(proof.record), [])
        self.assertIn("refusal: PERMISSION_DENIED", observation(proof.record, "host transport").observed)

    def test_the_host_transport_case_fails_when_its_rows_differ(self) -> None:
        module = runner()
        proof = proof_of(module, Stack(), HostTrino())
        with mock.patch.object(module, "ExecTrino", lambda stack: HostTrino(rows=3)):
            proof.exec_transport()
        self.assertEqual(failed(proof.record), ["host transport"])


def spark_name(module: ModuleType) -> str:
    """The job name that the runner passes: the module that the Spark service's entrypoint runs."""
    name: str = module.job_name()
    return name


class DeadlineLine(unittest.TestCase):
    def answers(self, module: ModuleType, late: tuple[int, str, str, float]) -> Stack:
        def jobs(arguments: list[str], environment: dict[str, str] | None) -> tuple[int, str, str, float]:
            if environment and "NESSIE_URI" in environment:
                return 1, "", "catalog_proof: cannot work with Nessie\n", 6.0
            return late

        return Stack(jobs)

    def run_case(self, module: ModuleType, late: tuple[int, str, str, float], left: list[str] | None = None) -> Record:
        stack = self.answers(module, late)
        stack.left = left or []
        proof = proof_of(module, stack)
        proof.bounded_failures()
        record: Record = proof.record
        return record

    def test_the_job_name_is_the_one_of_the_spark_service_and_of_the_job(self) -> None:
        module = runner()
        name = spark_name(module)
        self.assertEqual(name, "catalog_proof")
        entrypoint = (ROOT / "compose" / "services" / "spark.yml").read_text()
        self.assertIn(f"spark.jobs.{name}", entrypoint)
        self.assertIn(f'JOB = "{name}"', (ROOT / "spark" / "jobs" / f"{name}.py").read_text())

    def test_the_fixed_line_after_other_stderr_lines_passes(self) -> None:
        module = runner()
        noise = "WARNING: Using incubator modules\nSet by the cluster manager (via SPARK_LOCAL_DIRS)\n"
        run = self.run_case(module, (124, "", f"{noise}{spark_name(module)}: timeout\n", 4.0))
        self.assertEqual(failed(run), [])

    def test_the_old_message_with_a_duration_does_not_pass(self) -> None:
        module = runner()
        run = self.run_case(module, (124, "", f"{spark_name(module)}: did not finish within 3 seconds\n", 4.0))
        self.assertEqual(failed(run), ["a job past its deadline"])

    def test_a_line_with_a_duration_added_does_not_pass(self) -> None:
        module = runner()
        run = self.run_case(module, (124, "", f"{spark_name(module)}: timeout after 3 seconds\n", 4.0))
        self.assertEqual(failed(run), ["a job past its deadline"])

    def test_the_line_of_another_job_does_not_pass(self) -> None:
        module = runner()
        run = self.run_case(module, (124, "", "table_checks: timeout\n", 4.0))
        self.assertEqual(failed(run), ["a job past its deadline"])

    def test_another_exit_code_does_not_pass(self) -> None:
        module = runner()
        run = self.run_case(module, (1, "", f"{spark_name(module)}: timeout\n", 4.0))
        self.assertEqual(failed(run), ["a job past its deadline"])

    def test_a_container_left_behind_does_not_pass(self) -> None:
        module = runner()
        run = self.run_case(module, (124, "", f"{spark_name(module)}: timeout\n", 4.0), left=["abc"])
        self.assertEqual(failed(run), ["no route to Nessie", "a job past its deadline"])

    def test_the_case_no_longer_expects_a_duration_in_the_message(self) -> None:
        runner()
        self.assertNotIn("did not finish within", (E2E / "catalog_proof.py").read_text())


class AwsOfTheHost:
    """The AWS CLI image as the host ran it: ``--body /dev/null`` is refused with exit 252, and a write from the Trino role is refused with AccessDenied."""

    def __init__(self, *, accept_write: bool = False, probe_listed: bool = False, spark_write_denied: bool = True) -> None:
        self.accept_write = accept_write
        self.probe_listed = probe_listed
        self.written = False
        self.spark_write_denied = spark_write_denied

    def __call__(self, arguments: tuple[str, ...]) -> tuple[int, str]:
        words = list(arguments)
        text = " ".join(words)
        workload = re.search(r"/run/workload_access/(spark|trino):", text)
        if workload is None:
            raise AssertionError(text)
        after = words[words.index("public.ecr.aws/aws-cli/aws-cli:2.37.10") + 1 :]
        script = " ".join(after[after.index("-c") + 1 :]) if "sh" in words and "-c" in after else ""
        if "--entrypoint" in words:
            if "sh" not in words or after[:1] != ["-c"]:
                raise AssertionError(text)
            return self.shell(script, workload.group(1))
        return self.cli(after, workload.group(1))

    def shell(self, script: str, workload: str) -> tuple[int, str]:
        put = re.search(r"aws s3api put-object .*--body (\S+)", script)
        if put is None:
            return 0, ""
        if put.group(1) == "/dev/null" or "printf" not in script or f"> {put.group(1)}" not in script:
            return 252, "aws: [ERROR]: An error occurred (ParamValidation): Error parsing parameter '--body': Blob values must be a path to a file.\n"
        if self.accept_write:
            self.written = True
            return 0, '{"ETag": "x"}\n'
        return 254, "An error occurred (AccessDenied) when calling the PutObject operation: User is not authorized to perform: s3:PutObject\n"

    def cli(self, after: list[str], workload: str) -> tuple[int, str]:
        if after[:2] == ["sts", "get-caller-identity"]:
            return 0, f"arn:aws:sts::1:assumed-role/workload_{workload}/x\n"
        if after[:2] == ["s3api", "put-object"]:
            return 252, "aws: [ERROR]: An error occurred (ParamValidation): Error parsing parameter '--body': Blob values must be a path to a file.\n"
        if after[:2] == ["s3api", "list-objects-v2"]:
            prefix = after[after.index("--prefix") + 1]
            if prefix.startswith("state/"):
                return 254, "An error occurred (AccessDenied) when calling the ListObjectsV2 operation\n"
            if "probe" in prefix and "--query" in after:
                return 0, ("1\n" if self.written or self.probe_listed else "0\n")
            return 0, '{"KeyCount": 1}\n'
        raise AssertionError(after)


class TrinoWorkloadWrite(unittest.TestCase):
    def run_case(self, aws: AwsOfTheHost) -> tuple[Record, Stack]:
        module = runner()
        stack = Stack(aws=aws)
        proof = proof_of(module, stack)
        module.workload_cases(proof, BUCKET)
        return proof.record, stack

    def test_the_cli_would_refuse_the_old_body_before_sending(self) -> None:
        self.assertEqual(AwsOfTheHost().shell("aws s3api put-object --body /dev/null", "trino")[0], 252)

    def test_the_write_is_made_from_a_regular_file_inside_the_container_and_refused_by_the_bucket(self) -> None:
        run, stack = self.run_case(AwsOfTheHost())
        self.assertEqual(failed(run), [])
        write = next(call for call in stack.docker_calls if "put-object" in " ".join(call))
        self.assertNotIn("/dev/null", " ".join(write))
        self.assertIn("--entrypoint", write)
        observed = observation(run, "the Trino workload's own access").observed
        self.assertIn("write exit 254", observed)
        self.assertIn("refusal is AccessDenied: True", observed)

    def test_the_probe_key_is_listed_by_its_own_prefix_and_found_absent(self) -> None:
        run, stack = self.run_case(AwsOfTheHost())
        listed = [call for call in stack.docker_calls if "list-objects-v2" in " ".join(call) and "catalog_proof_trino_write_probe_" in " ".join(call)]
        self.assertEqual(len(listed), 1)
        self.assertIn("probe key absent: True", observation(run, "the Trino workload's own access").observed)

    def test_the_case_fails_when_the_write_is_accepted(self) -> None:
        run, _ = self.run_case(AwsOfTheHost(accept_write=True))
        self.assertEqual(failed(run), ["the Trino workload's own access"])

    def test_the_case_fails_when_the_probe_key_exists_after_a_refusal(self) -> None:
        run, _ = self.run_case(AwsOfTheHost(probe_listed=True))
        self.assertEqual(failed(run), ["the Trino workload's own access"])
        self.assertIn("probe key absent: False", observation(run, "the Trino workload's own access").observed)

    def test_the_observation_holds_exit_codes_and_flags_only(self) -> None:
        run, _ = self.run_case(AwsOfTheHost())
        observed = observation(run, "the Trino workload's own access").observed
        self.assertNotIn("probe_", observed)
        self.assertNotIn(BUCKET, observed)
        self.assertRegex(observed, r"^list exit \d+, write exit \d+, refusal is AccessDenied: (True|False), probe key absent: (True|False)$")


class MainAndLaterWrite(unittest.TestCase):
    def reading(self, module: ModuleType, trunk: str, *, move_main: bool = False) -> tuple[Any, HostNessie]:
        nessie = HostNessie()
        proof = proof_of(module, Stack(), HostTrino(rows=10), nessie, trunk=trunk)
        proof.main_start = nessie.reference("main").hash
        if move_main:
            first = nessie.create_branch("other", "main")
            nessie.commit(first.name, first.hash, "write", [module.put_namespace("x")])
            nessie.references["main"] = nessie.references["other"]
        proof.table_snapshot = lambda reference: 7
        return proof, nessie

    def read(self, proof: Any) -> None:
        proof.read_at_tag("tag", "m", {"rows": 10, "snapshot_id": 7})

    def test_the_host_reads_main_as_unchanged_and_never_substitutes_the_rows_it_wrote(self) -> None:
        module = runner()
        proof, _ = self.reading(module, "proof_trunk_x")
        self.read(proof)
        self.assertEqual(failed(proof.record), [])
        case = observation(proof.record, "main is unchanged")
        self.assertIn("main unchanged: True", case.observed)
        self.assertNotIn("the table on main", [item.case for item in proof.record.observations])

    def test_the_host_fails_when_main_moved_during_the_run(self) -> None:
        module = runner()
        proof, _ = self.reading(module, "proof_trunk_x", move_main=True)
        self.read(proof)
        self.assertEqual(failed(proof.record), ["main is unchanged"])
        self.assertIn("main unchanged: False", observation(proof.record, "main is unchanged").observed)

    def test_the_local_mode_still_reads_the_table_on_main(self) -> None:
        module = runner()
        nessie = HostNessie()
        proof = proof_of(module, Stack(), HostTrino(rows=10), nessie, trunk="main")
        proof.table_snapshot = lambda reference: 7
        trino = proof.trino
        trino.query = lambda sql: [[7]] if "snapshot_id" in sql else [[n, n * n] for n in range(1, 11)] if "SELECT n" in sql else [["numbers"]]
        self.read(proof)
        cases = [item.case for item in proof.record.observations]
        self.assertIn("the table on main", cases)
        self.assertNotIn("main is unchanged", cases)

    def later(self, module: ModuleType, rows: int, trunk: str = "proof_trunk_x") -> Any:
        nessie = HostNessie()
        module.start_private_trunk(nessie, trunk)

        def jobs(arguments: list[str], environment: dict[str, str] | None) -> tuple[int, str, str, float]:
            branch = arguments[arguments.index("--branch") + 1]
            made = nessie.create_branch(branch, trunk)
            nessie.commit(branch, made.hash, "write", [module.put_namespace("x", branch)])
            return 0, f'RESULT {{"rows": {rows}, "snapshot_id": 22, "branch": "{branch}", "branch_hash": "h"}}\n', "", 1.0

        proof = proof_of(module, Stack(jobs), HostTrino(rows=10), nessie, trunk=trunk)
        proof.facts["merged_hash"] = "1" * 64
        proof.table_snapshot = lambda reference: 22
        proof.trino_snapshot = lambda catalog: 7
        proof.later_write("tag", {"rows": 10, "snapshot_id": 7})
        return proof

    def test_the_host_takes_the_later_row_count_from_the_job_result_and_says_so(self) -> None:
        module = runner()
        proof = self.later(module, 25)
        self.assertEqual(failed(proof.record), [])
        observed = observation(proof.record, "a later write to the trunk").observed
        self.assertIn("rows source: job result", observed)
        self.assertIn("rows: 25", observed)

    def test_the_host_fails_when_the_job_reports_another_count(self) -> None:
        module = runner()
        proof = self.later(module, 24)
        self.assertEqual(failed(proof.record), ["a later write to the trunk"])
        self.assertIn("rows: 24", observation(proof.record, "a later write to the trunk").observed)

    def test_the_host_mode_reads_no_trino_catalog_that_follows_main(self) -> None:
        module = runner()
        proof = self.later(module, 25)
        self.assertFalse([sql for sql in proof.trino.queries if re.search(r"\blake\.", sql)])


class FailingSeed(unittest.TestCase):
    class Refusing(HostNessie):
        def serve_commit(self, path: str, payload: bytes | None) -> tuple[int, bytes]:
            return self.answer(503, {"errorCode": "UNAVAILABLE", "message": "service unavailable"})

        def send(self, method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
            if method == "DELETE":
                name = unquote(urlsplit(target).path).rsplit("/", 1)[1].partition("@")[0]
                self.references.pop(name, None)
                return 200, b""
            return super().send(method, target, payload)

    def test_the_trunk_is_listed_for_deletion_before_its_seed_commit(self) -> None:
        module = runner()
        client, made = self.Refusing(), list[str]()
        with self.assertRaises(NessieError):
            module.start_private_trunk(client, "proof_trunk_x", made)
        self.assertEqual(made, ["proof_trunk_x"])
        self.assertIn("proof_trunk_x", client.references)

    def test_the_cleanup_removes_a_trunk_whose_seed_failed(self) -> None:
        module = runner()
        client = self.Refusing()
        proof = proof_of(module, Stack(), HostTrino(), client)
        with self.assertRaises(NessieError):
            module.start_private_trunk(client, proof.trunk, proof.made)
        note = module.finish(proof, proof.record)
        self.assertNotIn("proof_trunk_x", client.references)
        self.assertIn("deleted: 1", note)

    def test_the_trunk_that_seeds_well_is_listed_once(self) -> None:
        module = runner()
        client, made = HostNessie(), list[str]()
        module.start_private_trunk(client, "proof_trunk_x", made)
        self.assertEqual(made, ["proof_trunk_x"])

    def test_the_host_mode_passes_its_list_and_does_not_append_the_trunk_afterwards(self) -> None:
        runner()
        source = ast.parse((E2E / "catalog_proof.py").read_text())
        host = next(item for item in source.body if isinstance(item, ast.FunctionDef) and item.name == "run_host")
        starts = [call for call in ast.walk(host) if isinstance(call, ast.Call) and getattr(call.func, "id", "") == "start_private_trunk"]
        self.assertEqual(len(starts), 1)
        self.assertEqual(ast.unparse(starts[0].args[-1]), "proof.made")
        appended = [
            call
            for call in ast.walk(host)
            if isinstance(call, ast.Call) and ast.unparse(call.func) == "proof.made.append" and ast.unparse(call.args[0]) == "proof.trunk"
        ]
        self.assertEqual(appended, [])

    def test_the_host_mode_records_the_hash_of_main_before_it_makes_the_trunk(self) -> None:
        runner()
        source = ast.parse((E2E / "catalog_proof.py").read_text())
        host = next(item for item in source.body if isinstance(item, ast.FunctionDef) and item.name == "run_host")
        lines = {
            "start": next(node.lineno for node in ast.walk(host) if isinstance(node, ast.Assign) and ast.unparse(node.targets[0]) == "proof.main_start"),
            "trunk": next(node.lineno for node in ast.walk(host) if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "start_private_trunk"),
        }
        self.assertLess(lines["start"], lines["trunk"])


if __name__ == "__main__":
    unittest.main()
