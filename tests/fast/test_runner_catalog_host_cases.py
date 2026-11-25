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
- A private trunk whose seed commit fails is not in the list of references to delete, and stays in Nessie. The trunk is made outside the ``try`` whose
  ``finally`` runs the cleanup, so a seed that answers 503 leaves it behind.
- The refusal of a workload is read as AccessDenied from any text that holds the word, so another code (``InvalidRequest``) or another operation passes.
- The job gets no deadline of 3 seconds, or the workload checks mount the folder of the other workload, or the probe key that is listed is not the one that
  was written, and the case still passes.
- An SSH warning on the standard error of a call is read as part of an answer (the key count, the identity).
- The message of the job that cannot work with Nessie is any text with those words, and not the job's own line.
"""

from __future__ import annotations

import ast
import contextlib
import importlib
import json
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

    def docker_split(self, *arguments: str, timeout: float = 0) -> tuple[int, str, str]:
        self.docker_calls.append(arguments)
        if self.aws is None:
            raise AssertionError("no docker answers")
        answer: tuple[int, str, str] = self.aws(arguments)
        return answer

    def docker(self, *arguments: str, timeout: float = 0) -> tuple[int, str]:
        code, out, err = self.docker_split(*arguments, timeout=timeout)
        return code, out + err

    def docker_out(self, *arguments: str, timeout: float = 0) -> tuple[int, str]:
        code, out, _ = self.docker_split(*arguments, timeout=timeout)
        return code, out

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
                return 1, "", f"WARNING: noise\n{spark_name(module)}: cannot work with Nessie: connection refused\n", 6.0
            if (environment or {}).get("JOB_DEADLINE_SECONDS") != "3":
                return 0, "RESULT {}\n", "", 30.0
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

    def test_the_late_job_is_given_a_deadline_of_three_seconds_and_nothing_else(self) -> None:
        module = runner()
        stack = self.answers(module, (124, "", f"{spark_name(module)}: timeout\n", 4.0))
        proof = proof_of(module, stack)
        proof.bounded_failures()
        late = [environment for _, environment in stack.job_calls if environment and "JOB_DEADLINE_SECONDS" in environment]
        self.assertEqual(late, [{"JOB_DEADLINE_SECONDS": "3"}])
        self.assertEqual(failed(proof.record), [])

    def test_the_message_of_the_job_that_cannot_work_with_nessie_is_its_own_line(self) -> None:
        module = runner()
        for message in (
            "at org.apache.spark: cannot work with Nessie: trace\n",
            "table_checks: cannot work with Nessie: x\n",
            f"{spark_name(module)}: could not reach the catalog; cannot work with Nessie\n",
            f"{spark_name(module)}: cannot work with Nessie\n",
        ):
            with self.subTest(message=message):

                def jobs(arguments: list[str], environment: dict[str, str] | None, message: str = message) -> tuple[int, str, str, float]:
                    if environment and "NESSIE_URI" in environment:
                        return 1, "", message, 6.0
                    return 124, "", f"{spark_name(module)}: timeout\n", 4.0

                proof = proof_of(module, Stack(jobs))
                proof.bounded_failures()
                self.assertEqual(failed(proof.record), ["no route to Nessie"])

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


def rendered_config(trino: str | None, spark: str | None) -> str:
    """The standard output of ``docker compose config --format json``: the WAREHOUSE_LOCATION of each engine's service, or none when its value is None."""
    services = {
        name: {"environment": {"WAREHOUSE_LOCATION": value, "AWS_REGION": "us-east-1"} if value is not None else {"AWS_REGION": "us-east-1"}}
        for name, value in (("trino", trino), ("spark", spark))
    }
    return json.dumps({"name": "p", "services": services})


DENIED_LIST = "An error occurred (AccessDenied) when calling the ListObjectsV2 operation: User is not authorized to perform: s3:ListBucket\n"
DENIED_PUT = "An error occurred (AccessDenied) when calling the PutObject operation: User is not authorized to perform: s3:PutObject\n"
PARAMETER = "aws: [ERROR]: An error occurred (ParamValidation): Error parsing parameter '--body': Blob values must be a path to a file.\n"


class AwsOfTheHost:
    """The AWS CLI image as the host ran it: ``--body /dev/null`` is refused with exit 252, and a write from the Trino role is refused with AccessDenied.

    Each answer is ``(exit code, stdout, stderr)``: the CLI writes its errors to stderr, and ssh adds ``warning`` to the stderr of every call. ``bucket``
    holds the keys that exist, and a test adds the probe key of the run to it before the write. ``calls`` holds what each call asked for, with the workload
    whose folder it mounted.
    """

    def __init__(
        self,
        *,
        accept_write: bool = False,
        state_error: str = DENIED_LIST,
        write_error: str = DENIED_PUT,
        warning: str = "",
        empty_listing: str = "0\n",
        listing_exit: int = 0,
        listing_error: str = "",
    ) -> None:
        self.empty_listing = empty_listing
        self.listing_exit = listing_exit
        self.listing_error = listing_error
        self.accept_write = accept_write
        self.bucket: set[str] = set()
        self.state_error = state_error
        self.write_error = write_error
        self.warning = warning
        self.calls: list[tuple[str, str]] = []
        self.put_key = ""
        self.listed_keys: list[str] = []

    def __call__(self, arguments: tuple[str, ...]) -> tuple[int, str, str]:
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
            code, out, err = self.shell(script, workload.group(1))
        else:
            code, out, err = self.cli(after, workload.group(1))
        return code, out, err + self.warning

    def shell(self, script: str, workload: str) -> tuple[int, str, str]:
        put = re.search(r"aws s3api put-object .*--bucket (\S+) --key (\S+) --body (\S+)", script)
        if put is None:
            return 0, "", ""
        self.calls.append((workload, "put"))
        self.put_key = put.group(2)
        if put.group(3) == "/dev/null" or "printf" not in script or f"> {put.group(3)}" not in script:
            return 252, "", PARAMETER
        if self.accept_write:
            self.bucket.add(self.put_key)
            return 0, '{"ETag": "x"}\n', ""
        return 254, "", self.write_error

    def cli(self, after: list[str], workload: str) -> tuple[int, str, str]:
        if after[:2] == ["sts", "get-caller-identity"]:
            self.calls.append((workload, "identity"))
            return 0, f"arn:aws:sts::1:assumed-role/workload_{workload}/x\n", ""
        if after[:2] == ["s3api", "put-object"]:
            return 252, "", PARAMETER
        if after[:2] == ["s3api", "list-objects-v2"]:
            prefix = after[after.index("--prefix") + 1]
            if prefix.startswith("state/"):
                self.calls.append((workload, "state list"))
                return 254, "", self.state_error
            if "--query" in after:
                self.calls.append((workload, "probe list"))
                self.listed_keys.append(prefix)
                if self.listing_exit:
                    return self.listing_exit, "", self.listing_error
                return 0, ("1\n" if prefix in self.bucket else self.empty_listing), ""
            self.calls.append((workload, "warehouse list"))
            return 0, '{"KeyCount": 1}\n', ""
        raise AssertionError(after)


class TrinoWorkloadWrite(unittest.TestCase):
    def run_case(self, aws: AwsOfTheHost, *, leftover: bool = False) -> tuple[Record, Stack]:
        module = runner()
        stack = Stack(aws=aws)
        proof = proof_of(module, stack)
        if leftover:
            aws.bucket.add(f"warehouse/catalog_proof_trino_write_probe_{proof.suffix}")
        module.workload_cases(proof, BUCKET)
        return proof.record, stack

    def test_the_cli_would_refuse_the_old_body_before_sending(self) -> None:
        self.assertEqual(AwsOfTheHost().shell("aws s3api put-object --bucket b --key k --body /dev/null", "trino")[0], 252)

    def test_the_write_is_made_from_a_regular_file_inside_the_container_and_refused_by_the_bucket(self) -> None:
        run, stack = self.run_case(AwsOfTheHost())
        self.assertEqual(failed(run), [])
        write = next(call for call in stack.docker_calls if "put-object" in " ".join(call))
        self.assertNotIn("/dev/null", " ".join(write))
        self.assertIn("--entrypoint", write)
        observed = observation(run, "the Trino workload's own access").observed
        self.assertIn("write exit 254", observed)
        self.assertIn("refusal is AccessDenied: True", observed)

    def test_the_probe_key_that_is_listed_is_the_key_that_was_written(self) -> None:
        aws = AwsOfTheHost()
        run, _ = self.run_case(aws)
        self.assertTrue(aws.put_key.startswith("warehouse/catalog_proof_trino_write_probe_"))
        self.assertEqual(aws.listed_keys, [aws.put_key])
        self.assertIn("probe key absent: True", observation(run, "the Trino workload's own access").observed)

    def test_the_cli_prints_none_for_the_key_count_of_an_empty_listing_and_that_is_absent(self) -> None:
        for empty in ("0\n", "None\n"):
            with self.subTest(empty=empty):
                run, _ = self.run_case(AwsOfTheHost(empty_listing=empty))
                self.assertEqual(failed(run), [])
                self.assertIn("probe key absent: True", observation(run, "the Trino workload's own access").observed)

    def test_any_other_answer_of_the_probe_listing_is_not_absent(self) -> None:
        for answer in ("1\n", "5\n", "10\n", "\n", "none\n", "null\n", "None\nNone\n", "0 0\n", " 0\n", "00\n", "-0\n", "KeyCount\n"):
            with self.subTest(answer=answer):
                run, _ = self.run_case(AwsOfTheHost(empty_listing=answer))
                self.assertEqual(failed(run), ["the Trino workload's own access"])
                self.assertIn("probe key absent: False", observation(run, "the Trino workload's own access").observed)

    def test_a_probe_listing_that_fails_is_not_absent_whatever_it_printed(self) -> None:
        for code, out in ((1, "0\n"), (254, "None\n"), (255, "0\n")):
            with self.subTest(code=code):
                run, _ = self.run_case(AwsOfTheHost(listing_exit=code, empty_listing=out))
                self.assertEqual(failed(run), ["the Trino workload's own access"])
                self.assertIn("probe key absent: False", observation(run, "the Trino workload's own access").observed)

    def test_a_probe_listing_that_is_an_error_is_not_absent(self) -> None:
        run, _ = self.run_case(AwsOfTheHost(empty_listing=DENIED_LIST))
        self.assertEqual(failed(run), ["the Trino workload's own access"])

    def test_the_probe_key_that_exists_prints_its_count_and_is_not_absent_even_when_none_is_the_empty_form(self) -> None:
        run, _ = self.run_case(AwsOfTheHost(empty_listing="None\n"), leftover=True)
        self.assertEqual(failed(run), ["the Trino workload's own access"])

    def test_the_absent_forms_are_the_two_of_an_empty_listing_and_nothing_else(self) -> None:
        module = runner()
        self.assertTrue(all(module.probe_absent(0, text) for text in ("0\n", "None\n", "0", "None")))
        self.assertFalse(any(module.probe_absent(0, text) for text in ("1\n", "5\n", "", "\n", "none", "None None", "0\n0\n", "00", "Error")))
        self.assertFalse(any(module.probe_absent(code, text) for code in (1, 2, 252, 254, 255) for text in ("0\n", "None\n")))

    def test_each_call_mounts_the_folder_of_its_own_workload(self) -> None:
        aws = AwsOfTheHost()
        self.run_case(aws)
        self.assertEqual(
            aws.calls,
            [
                ("spark", "identity"),
                ("spark", "warehouse list"),
                ("spark", "state list"),
                ("trino", "warehouse list"),
                ("trino", "put"),
                ("trino", "probe list"),
            ],
        )

    def test_the_case_fails_when_the_write_is_accepted(self) -> None:
        run, _ = self.run_case(AwsOfTheHost(accept_write=True))
        self.assertEqual(failed(run), ["the Trino workload's own access"])

    def test_the_case_fails_when_the_probe_key_exists_after_a_refusal(self) -> None:
        run, _ = self.run_case(AwsOfTheHost(), leftover=True)
        self.assertEqual(failed(run), ["the Trino workload's own access"])
        self.assertIn("probe key absent: False", observation(run, "the Trino workload's own access").observed)

    def test_a_warning_of_ssh_on_the_error_output_changes_no_answer(self) -> None:
        run, _ = self.run_case(AwsOfTheHost(warning="Warning: Permanently added '100.64.0.9' (ED25519) to the list of known hosts.\n"))
        self.assertEqual(failed(run), [])
        self.assertIn("probe key absent: True", observation(run, "the Trino workload's own access").observed)

    def test_a_refusal_with_another_code_does_not_pass_for_the_state_prefix(self) -> None:
        for text in (
            "An error occurred (InvalidRequest) when calling the ListObjectsV2 operation: AccessDenied was the cause\n",
            "An error occurred (AccessDenied) when calling the GetObject operation: Access Denied\n",
            "An error occurred (AccessDeniedException) when calling the ListObjectsV2 operation: x\n",
            "AccessDenied\n",
        ):
            with self.subTest(text=text):
                run, _ = self.run_case(AwsOfTheHost(state_error=text))
                self.assertEqual(failed(run), ["the Spark workload's own access"])

    def test_a_refusal_with_another_code_does_not_pass_for_the_trino_write(self) -> None:
        for text in (
            "An error occurred (InvalidRequest) when calling the PutObject operation: AccessDenied was the cause\n",
            "An error occurred (AccessDenied) when calling the DeleteObject operation: Access Denied\n",
            "An error occurred (AccessDeniedException) when calling the PutObject operation: x\n",
            "AccessDenied\n",
        ):
            with self.subTest(text=text):
                run, _ = self.run_case(AwsOfTheHost(write_error=text))
                self.assertEqual(failed(run), ["the Trino workload's own access"])
                self.assertIn("refusal is AccessDenied: False", observation(run, "the Trino workload's own access").observed)

    def test_the_refusal_on_the_standard_output_does_not_pass(self) -> None:
        class OnStdout(AwsOfTheHost):
            def cli(self, after: list[str], workload: str) -> tuple[int, str, str]:
                code, out, err = super().cli(after, workload)
                return code, out + err, ""

        run, _ = self.run_case(OnStdout())
        self.assertEqual(failed(run), ["the Spark workload's own access"])

    def test_the_observation_holds_exit_codes_and_flags_only(self) -> None:
        run, _ = self.run_case(AwsOfTheHost())
        observed = observation(run, "the Trino workload's own access").observed
        self.assertNotIn("probe_", observed)
        self.assertNotIn(BUCKET, observed)
        self.assertRegex(observed, r"^list exit \d+, write exit \d+, refusal is AccessDenied: (True|False), probe key absent: (True|False)$")


JOB_LINE = "Nessie refused the branch: the reference or its content changed (a conflict)"


class SecondJobOnTheSameBranch(unittest.TestCase):
    """The job's refusal of a branch that exists is one fixed line of its own: ``<job>: <fixed text>``, and the case asks for exactly that line."""

    def case(self, module: ModuleType, code: int, err: str, *, moved: bool = False) -> Record:
        nessie = HostNessie()
        trunk = module.start_private_trunk(nessie, "proof_trunk_x")
        branch = nessie.create_branch("proof_b", trunk.name)
        at = branch.hash
        if moved:
            nessie.commit(branch.name, branch.hash, "write", [module.put_namespace("x")])
        proof = proof_of(module, Stack(lambda arguments, environment: (code, "", err, 1.0)), HostTrino(), nessie, trunk="proof_trunk_x")
        proof.second_job("proof_b", {"branch_hash": at})
        record_: Record = proof.record
        return record_

    def test_the_fixed_line_of_the_job_passes_also_after_other_lines(self) -> None:
        module = runner()
        name = spark_name(module)
        for err in (f"{name}: {JOB_LINE}\n", f"WARNING: noise\n{name}: {JOB_LINE}\nmore noise\n", f"{name}: {JOB_LINE}"):
            with self.subTest(err=err):
                self.assertEqual(failed(self.case(module, 1, err)), [])

    def test_the_old_text_that_names_the_branch_does_not_pass(self) -> None:
        module = runner()
        name = spark_name(module)
        run = self.case(module, 1, f"{name}: Nessie refused the branch proof_b: Nessie refused the reference\n")
        self.assertEqual(failed(run), ["a second job on the same branch"])

    def test_another_line_does_not_pass(self) -> None:
        module = runner()
        name = spark_name(module)
        for err in (
            "",
            f"{name}: cannot work with Nessie: boom\n",
            f"table_checks: {JOB_LINE}\n",
            f"{JOB_LINE}\n",
            f"{name}: {JOB_LINE} extra\n",
            f"{name}: {JOB_LINE.replace('(a conflict)', '(conflict)')}\n",
            f"{name}: {JOB_LINE.lower()}\n",
            f"at x: {name}: {JOB_LINE}\n",
            f" {name}: {JOB_LINE}\n",
            f"{name}: Nessie refused the branch proof_b\n",
        ):
            with self.subTest(err=err):
                self.assertEqual(failed(self.case(module, 1, err)), ["a second job on the same branch"])

    def test_another_exit_code_or_a_moved_branch_does_not_pass(self) -> None:
        module = runner()
        line = f"{spark_name(module)}: {JOB_LINE}\n"
        for code in (0, 2, 124):
            with self.subTest(code=code):
                self.assertEqual(failed(self.case(module, code, line)), ["a second job on the same branch"])
        self.assertEqual(failed(self.case(module, 1, line, moved=True)), ["a second job on the same branch"])

    def test_the_text_is_the_one_that_the_job_source_holds_when_the_checkout_holds_the_fixed_text(self) -> None:
        module = runner()
        nessie_source = (ROOT / "scripts" / "stack" / "nessie.py").read_text()
        found = re.search(r'^CONFLICT_TEXT = "(.+)"$', nessie_source, re.MULTILINE)
        if found is None:
            self.skipTest("this checkout's job still names the branch: the fixed text is in the integration branch")
        self.assertEqual(module.CONFLICT_LINE, found.group(1))
        self.assertEqual(module.CONFLICT_LINE, JOB_LINE)


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

    def test_the_host_mode_removes_a_trunk_whose_seed_answers_503_and_leaves_main_as_it_was(self) -> None:
        module = runner()
        client = self.Refusing()
        main_before = client.reference("main").hash
        run, log = record(), list[str]()

        class HostStack(Stack):
            def health(self, service: str) -> str:
                return "healthy"

            def docker(self, *arguments: str, timeout: float = 0) -> tuple[int, str]:
                return 0, ""

            def compose_out(self, *arguments: str, timeout: float = 0) -> tuple[int, str]:
                return 0, rendered_config("s3://wh/", "s3://wh/")

        with (
            mock.patch.multiple(
                module,
                Remote=lambda: SimpleNamespace(remove_file=lambda path: None),
                Stack=lambda target, rec, lg: HostStack(),
                load_configuration=lambda: {"S3_BUCKET": BUCKET},
                configured_warehouse_location=lambda: "s3://wh/",
                ExecNessie=lambda stack: client,
                ExecTrino=lambda stack: HostTrino(),
            ),
            self.assertRaises(NessieError),
        ):
            module.run_host(run, log)
        self.assertEqual([name for name in client.references if name.startswith("proof_trunk_")], [])
        self.assertEqual(client.reference("main").hash, main_before)
        self.assertIn("deleted: 1", run.cleanup)
        self.assertIn("no commit was added to main", run.cleanup)

    def test_the_location_of_the_warehouse_is_read_from_the_standard_output_of_the_configuration(self) -> None:
        module = runner()
        client = HostNessie()
        run = record()

        class HostStack(Stack):
            def health(self, service: str) -> str:
                return "healthy"

            def docker(self, *arguments: str, timeout: float = 0) -> tuple[int, str]:
                return 0, ""

            def compose_out(self, *arguments: str, timeout: float = 0) -> tuple[int, str]:
                return 0, "{}"

            def compose_run(self, *arguments: str, stdin: str | None = None, timeout: float = 0) -> tuple[int, str]:
                return 0, "warning: the location s3://wh/ is not set"

        with mock.patch.multiple(
            module,
            Remote=lambda: SimpleNamespace(remove_file=lambda path: None),
            Stack=lambda target, rec, lg: HostStack(),
            load_configuration=lambda: {"S3_BUCKET": BUCKET},
            configured_warehouse_location=lambda: "s3://wh/",
            ExecNessie=lambda stack: client,
            ExecTrino=lambda stack: HostTrino(),
        ):
            module.run_host(run, [])
        self.assertEqual(failed(run), ["deployed stack"])

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


class WarehouseSettings(unittest.TestCase):
    """The host mode reads the warehouse location that Compose renders for Trino and for Spark, from a read that succeeded, and compares both."""

    def locations(self, code: int, output: str) -> Any:
        module = runner()
        calls: list[tuple[str, ...]] = []

        def compose_out(*arguments: str, timeout: float = 0) -> tuple[int, str]:
            calls.append(arguments)
            return code, output

        found = module.engine_locations(SimpleNamespace(compose_out=compose_out))
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][:2], ("--profile", "batch"))
        self.assertIn("config", calls[0])
        return found

    def test_the_settings_of_both_engines_are_read_from_a_read_that_exited_0(self) -> None:
        self.assertEqual(self.locations(0, rendered_config("s3://a/", "s3://b/")), {"trino": "s3://a/", "spark": "s3://b/"})

    def test_a_read_that_failed_is_a_failure_whatever_it_printed(self) -> None:
        unread = runner().Unread
        for code in (1, 255):
            with self.subTest(code=code):
                self.assertIsInstance(self.locations(code, rendered_config("s3://wh/", "s3://wh/")), unread)

    def test_an_empty_or_unparseable_or_incomplete_document_is_a_failure(self) -> None:
        unread = runner().Unread
        for text in ("", "not json", "[]", "{}", '{"services": {}}', rendered_config(None, "s3://wh/"), rendered_config("s3://wh/", None), '{"services": 1}'):
            with self.subTest(text=text):
                self.assertIsInstance(self.locations(0, text), unread)

    def run_host(self, trino: str | None, spark: str | None, code: int = 0, text: str | None = None) -> Record:
        module = runner()
        run = record()

        class HostStack(Stack):
            def health(self, service: str) -> str:
                return "healthy"

            def docker(self, *arguments: str, timeout: float = 0) -> tuple[int, str]:
                return 0, ""

            def compose_out(self, *arguments: str, timeout: float = 0) -> tuple[int, str]:
                return code, rendered_config(trino, spark) if text is None else text

        class Refusing(HostNessie):
            def reference(self, name: str, *args: Any, **keywords: Any) -> Any:
                raise NessieError("stop after the setup case")

        with (
            mock.patch.multiple(
                module,
                Remote=lambda: SimpleNamespace(remove_file=lambda path: None),
                Stack=lambda target, rec, lg: HostStack(),
                load_configuration=lambda: {"S3_BUCKET": BUCKET},
                configured_warehouse_location=lambda: "s3://wh/",
                ExecNessie=lambda stack: Refusing(),
                ExecTrino=lambda stack: HostTrino(),
            ),
            contextlib.suppress(NessieError),
        ):
            module.run_host(run, [])
        return run

    def test_both_engines_on_the_project_location_pass_the_setup_case(self) -> None:
        run = self.run_host("s3://wh/", "s3://wh/")
        self.assertEqual(failed(run), [])

    def test_a_read_that_failed_fails_the_setup_case_although_it_printed_the_location(self) -> None:
        run = self.run_host("s3://wh/", "s3://wh/", code=255)
        self.assertEqual(failed(run), ["deployed stack"])

    def test_an_engine_on_another_location_fails_the_setup_case(self) -> None:
        for trino, spark in (("s3://wh/", "s3://other/"), ("s3://other/", "s3://wh/"), ("s3://wh/", "s3://wh"), ("s3://wh/", "")):
            with self.subTest(trino=trino, spark=spark):
                self.assertEqual(failed(self.run_host(trino, spark)), ["deployed stack"])

    def test_an_engine_with_no_setting_fails_the_setup_case_although_the_location_is_in_the_text(self) -> None:
        self.assertEqual(failed(self.run_host("s3://wh/", None)), ["deployed stack"])
        text = json.dumps({"services": {"trino": {"environment": {"WAREHOUSE_LOCATION": "s3://wh/"}}, "spark": {"labels": {"x": "s3://wh/"}}}})
        self.assertEqual(failed(self.run_host("s3://wh/", "s3://wh/", text=text)), ["deployed stack"])

    def test_the_observation_shows_flags_and_no_location(self) -> None:
        run = self.run_host("s3://wh/", "s3://other/")
        observed = observation(run, "deployed stack").observed
        self.assertNotIn("s3://", observed)
        self.assertNotIn("other", observed)

    def test_running_services_that_cannot_be_listed_is_a_failure_and_not_none_running(self) -> None:
        module = runner()
        unread = module.Unread
        for code, out in ((1, ""), (255, "postgres\n")):
            with self.subTest(code=code):
                stack = SimpleNamespace(compose=["docker", "compose"], target=SimpleNamespace(run=lambda *a, code=code, out=out, **k: (code, out, "")))
                self.assertIsInstance(module.running_services(stack), unread)
        stack = SimpleNamespace(compose=["docker", "compose"], target=SimpleNamespace(run=lambda *a, **k: (0, "postgres\nnessie\n", "")))
        self.assertEqual(module.running_services(stack), {"postgres", "nessie"})
        stack = SimpleNamespace(compose=["docker", "compose"], target=SimpleNamespace(run=lambda *a, **k: (0, "", "")))
        self.assertEqual(module.running_services(stack), set())

    def test_the_spark_job_is_stopped_only_when_the_listing_succeeded_and_does_not_name_it(self) -> None:
        module = runner()

        def stack(code: int, out: str) -> Any:
            return SimpleNamespace(compose=["docker", "compose"], target=SimpleNamespace(run=lambda *a, **k: (code, out, "")))

        self.assertTrue(module.spark_stopped(stack(0, "postgres\nnessie\n")))
        self.assertTrue(module.spark_stopped(stack(0, "")))
        self.assertFalse(module.spark_stopped(stack(0, "postgres\nspark\n")))
        self.assertFalse(module.spark_stopped(stack(1, "")))
        self.assertFalse(module.spark_stopped(stack(255, "postgres\n")))


if __name__ == "__main__":
    unittest.main()
