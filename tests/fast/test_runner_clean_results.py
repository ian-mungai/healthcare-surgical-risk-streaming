"""Every clean-up step of a host runner records its outcome as an assertion, and the test runs the runner's own clean-up and reads the final record.

The runners stay untracked, so each test skips when its runner is not on this machine. Docker and Nessie are stand-ins: no cloud, Docker or Spark runs here.

Failure modes covered here:

- A step of the clean-up (the removal of the Compose project, of the test network, of the scratch folder, of the Nessie references the run made, of the pinned
  Trino file) fails and only the text of the record says so, so the run passes with something left behind.
- The scratch folder is tested with ``exists``, which is false for a link that points nowhere, so a link left in its place reads as removed.
- A failed step stops the steps after it, so a second leftover is not reported.
"""

from __future__ import annotations

import contextlib
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from tests.e2e.run_record import Record, Unread
from tests.fast.test_runner_failed_reads import DockerOfThisComputer, failed, record
from tests.fast.test_runner_stdout_alone import runner

NETWORK = "p2_caddy_net"
LEFT = {"containers": ["c1"], "volumes": [], "networks": []}


class NetworkRefused(DockerOfThisComputer):
    """The Docker of this computer, which refuses to remove the test network that Caddy's run made."""

    def __call__(self, program: str, arguments: list[str], **keywords: object) -> SimpleNamespace:
        if arguments[:3] == ["network", "rm", NETWORK]:
            self.calls.append(arguments)
            return SimpleNamespace(returncode=1, stdout="", stderr="network has active endpoints")
        return super().__call__(program, arguments, **keywords)


def clean(name: str, docker: DockerOfThisComputer, *, scratch_stays: bool = False) -> tuple[Record, Path]:
    """Run the clean-up of the local run of ``name`` on a scratch folder. ``scratch_stays`` makes the removal of the folder do nothing."""
    module = runner(name)
    run = record()
    scratch = Path(tempfile.mkdtemp(prefix="clean_results_"))
    (scratch / "inner").mkdir()
    removal = mock.patch("shutil.rmtree") if scratch_stays else contextlib.nullcontext()
    with mock.patch.object(module, "run_command", docker), removal:
        if name == "stack_caddy":
            module.clean_local(run, [], scratch, NETWORK)
        elif name == "stack_openbao":
            module.clean_local(run, [], scratch, "p2_openbao_x")
        elif name == "catalog_proof":
            module.clean_local(run, [], scratch, [])
        else:
            module.clean_local(run, [], scratch)
    return run, scratch


def refused_network(name: str) -> list[str]:
    """The Docker that refuses every removal also refuses the test network that only Caddy's run makes."""
    return ["network removed"] if name == "stack_caddy" else []


LOCAL_RUNNERS = ("stack_caddy", "stack_nessie", "stack_openbao", "catalog_proof")


class LocalCleanup(unittest.TestCase):
    def test_the_four_runners_with_a_local_cleanup_are_all_covered(self) -> None:
        self.assertEqual(LOCAL_RUNNERS, ("stack_caddy", "stack_nessie", "stack_openbao", "catalog_proof"))

    def test_a_clean_run_has_no_failed_assertion_and_each_step_is_one(self) -> None:
        for name in LOCAL_RUNNERS:
            with self.subTest(runner=name):
                run, scratch = clean(name, DockerOfThisComputer())
                self.assertEqual(failed(run), [])
                cases = [item.case for item in run.observations]
                self.assertIn("project removed", cases)
                self.assertIn("scratch folder removed", cases)
                self.assertEqual("network removed" in cases, name == "stack_caddy")
                self.assertIn("scratch folder removed: True", run.cleanup)
                self.assertFalse(scratch.exists())

    def test_a_project_resource_that_is_left_fails_the_run(self) -> None:
        for name in LOCAL_RUNNERS:
            with self.subTest(runner=name):
                run, _ = clean(name, DockerOfThisComputer(dict(LEFT), stubborn=True))
                self.assertEqual(failed(run), ["project removed", *refused_network(name)])

    def test_a_project_listing_that_cannot_be_read_fails_the_run(self) -> None:
        for name in LOCAL_RUNNERS:
            with self.subTest(runner=name):
                run, _ = clean(name, DockerOfThisComputer(fails=("containers",)))
                self.assertEqual(failed(run), ["project removed"])

    def test_a_scratch_folder_that_is_left_fails_the_run(self) -> None:
        for name in LOCAL_RUNNERS:
            with self.subTest(runner=name):
                run, scratch = clean(name, DockerOfThisComputer(), scratch_stays=True)
                self.assertEqual(failed(run), ["scratch folder removed"])
                self.assertIn("scratch folder removed: False", run.cleanup)
                (scratch / "inner").rmdir()
                scratch.rmdir()

    def test_a_link_that_points_nowhere_in_the_place_of_the_folder_is_left(self) -> None:
        for name in LOCAL_RUNNERS:
            with self.subTest(runner=name):
                module = runner(name)
                run = record()
                with tempfile.TemporaryDirectory() as folder:
                    link = Path(folder) / "scratch"
                    link.symlink_to(Path(folder) / "missing")
                    with mock.patch.object(module, "run_command", DockerOfThisComputer()), mock.patch("shutil.rmtree"):
                        arguments: dict[str, tuple[Any, ...]] = {
                            "stack_caddy": (NETWORK,),
                            "stack_openbao": ("p2_openbao_x",),
                            "catalog_proof": ([],),
                            "stack_nessie": (),
                        }
                        module.clean_local(run, [], link, *arguments[name])
                self.assertEqual(failed(run), ["scratch folder removed"])

    def test_two_leftovers_are_both_reported(self) -> None:
        for name in LOCAL_RUNNERS:
            with self.subTest(runner=name):
                run, scratch = clean(name, DockerOfThisComputer(dict(LEFT), stubborn=True), scratch_stays=True)
                self.assertEqual(failed(run), ["project removed", *refused_network(name), "scratch folder removed"])
                (scratch / "inner").rmdir()
                scratch.rmdir()

    def test_a_network_that_cannot_be_removed_fails_the_caddy_run(self) -> None:
        run, _ = clean("stack_caddy", NetworkRefused())
        self.assertEqual(failed(run), ["network removed"])
        self.assertIn("network removed: False", run.cleanup)

    def test_a_network_that_cannot_be_removed_does_not_skip_the_scratch_folder(self) -> None:
        run, scratch = clean("stack_caddy", NetworkRefused(), scratch_stays=True)
        self.assertEqual(failed(run), ["network removed", "scratch folder removed"])
        (scratch / "inner").rmdir()
        scratch.rmdir()


class NessieHostCleanup(unittest.TestCase):
    def clean(self, *, refuses: tuple[str, ...] = ()) -> Record:
        module = runner("stack_nessie")
        run = record()
        calls: list[str] = []

        def delete(name: str, hash_: str) -> None:
            calls.append(name)
            if name in refuses:
                raise module.NessieError("refused")

        client = SimpleNamespace(reference=lambda name: SimpleNamespace(hash="h"), delete_reference=delete)
        module.clean_host(run, [], client, [("branch", "a"), ("tag", "b")])
        self.assertEqual(calls, ["b", "a"])
        return run

    def test_references_that_are_all_deleted_pass(self) -> None:
        run = self.clean()
        self.assertEqual(failed(run), [])
        self.assertEqual([item.case for item in run.observations], ["references deleted"])
        self.assertIn("deleted: 2", run.cleanup)

    def test_a_reference_that_cannot_be_deleted_fails_the_run(self) -> None:
        for refused in ("a", "b"):
            with self.subTest(refused=refused):
                run = self.clean(refuses=(refused,))
                self.assertEqual(failed(run), ["references deleted"])
                self.assertIn("deleted: 1", run.cleanup)

    def test_a_reference_that_cannot_be_read_fails_the_run(self) -> None:
        module = runner("stack_nessie")
        run = record()

        def read(name: str) -> object:
            raise module.NessieError("no such reference")

        module.clean_host(run, [], SimpleNamespace(reference=read, delete_reference=lambda *a: None), [("branch", "a")])
        self.assertEqual(failed(run), ["references deleted"])


class CatalogFinish(unittest.TestCase):
    def setUp(self) -> None:
        self.pins: list[str | None] = []
        self.deleted: list[str] = []

    def finish(
        self,
        *,
        restored: bool = True,
        trino: bool = True,
        listing: bool = True,
        refuses: tuple[str, ...] = (),
        file_removed: bool = True,
        pin_error: Exception | None = None,
        delete_error: Exception | None = None,
        remove_error: Exception | None = None,
        never_made: tuple[str, ...] = (),
        unreadable: tuple[str, ...] = (),
    ) -> tuple[Record, str]:
        module = runner("catalog_proof")
        run = record()

        def delete(name: str, hash_: str) -> None:
            self.deleted.append(name)
            if delete_error is not None:
                raise delete_error
            if name in refuses:
                raise module.NessieError("refused")

        def pin(reference: str | None, scratch_file: str) -> float | None:
            self.pins.append(reference)
            if pin_error is not None:
                raise pin_error
            return 1.0 if restored else None

        def remove(path: str) -> bool:
            if remove_error is not None:
                raise remove_error
            return file_removed

        stack = SimpleNamespace(
            listed_container=lambda service: ("abc" if trino else "") if listing else Unread("exit 1"),
            pin_trino=pin,
            target=SimpleNamespace(remove_file=remove),
        )

        def reference(name: str) -> SimpleNamespace:
            if name in never_made:
                raise module.NessieError("missing", status=404, error_code="REFERENCE_NOT_FOUND")
            if name in unreadable:
                raise module.NessieError("unavailable", status=503, error_code=None)
            return SimpleNamespace(hash="h")

        nessie = SimpleNamespace(reference=reference, delete_reference=delete)
        proof = SimpleNamespace(stack=stack, nessie=nessie, made=["a", "b"], pin_file="/pin.env", facts={})
        return run, module.finish(proof, run)

    def test_a_finish_that_put_trino_back_deleted_the_references_and_removed_the_file_passes(self) -> None:
        run, text = self.finish()
        self.assertEqual(failed(run), [])
        self.assertEqual([item.case for item in run.observations], ["Trino back on main", "Trino container listed", "references deleted", "pin file removed"])
        self.assertIn("deleted: 2", text)

    def test_trino_that_did_not_come_back_fails_the_run(self) -> None:
        self.assertEqual(failed(self.finish(restored=False)[0]), ["Trino back on main", "references deleted", "pin file removed"])

    def test_a_stack_with_no_trino_container_in_the_final_listing_fails_the_run(self) -> None:
        self.assertEqual(failed(self.finish(trino=False)[0]), ["Trino container listed"])

    def test_a_reference_that_cannot_be_deleted_fails_the_run(self) -> None:
        for refused in ("a", "b"):
            with self.subTest(refused=refused):
                self.assertEqual(failed(self.finish(refuses=(refused,))[0]), ["references deleted"])

    def test_a_final_listing_that_fails_fails_the_run_and_the_restoration_still_runs(self) -> None:
        run, text = self.finish(listing=False)
        self.assertEqual(failed(run), ["Trino container listed"])
        self.assertEqual(self.pins, [None])
        self.assertTrue(text.startswith("cleanup failed (Trino container listed)"), text)
        self.assertEqual(sorted(self.deleted), ["a", "b"])
        self.assertEqual([item.case for item in run.observations], ["Trino back on main", "Trino container listed", "references deleted", "pin file removed"])
        run.settle()
        self.assertEqual(run.outcome, "fail")

    def test_the_restoration_runs_whatever_the_final_listing_holds(self) -> None:
        cases: tuple[tuple[str, dict[str, Any]], ...] = (("readable", {}), ("empty", {"trino": False}), ("failed", {"listing": False}))
        for kind, arguments in cases:
            with self.subTest(listing=kind):
                self.pins.clear()
                self.finish(**arguments)
                self.assertEqual(self.pins, [None])

    def test_a_restoration_that_fails_fails_the_run_and_names_the_step(self) -> None:
        run, text = self.finish(restored=False, listing=False)
        self.assertEqual(failed(run), ["Trino back on main", "Trino container listed", "references deleted", "pin file removed"])
        self.assertTrue(text.startswith("cleanup failed (Trino back on main, Trino container listed, references deleted, pin file removed)"), text)

    def test_a_restoration_that_times_out_fails_the_run_and_keeps_what_trino_may_still_read(self) -> None:
        run, text = self.finish(pin_error=subprocess.TimeoutExpired("docker", 600))
        self.assertEqual(failed(run), ["Trino back on main", "references deleted", "pin file removed"])
        self.assertIn("TimeoutExpired", next(item.observed for item in run.observations if not item.passed))
        self.assertEqual(self.deleted, [])
        self.assertTrue(text.startswith("cleanup failed (Trino back on main, references deleted, pin file removed)"), text)
        self.assertIn("were kept", text)

    def test_a_restoration_that_fails_before_it_starts_keeps_the_references_and_the_pin_file(self) -> None:
        for error in (BlockingIOError("resource busy"), OSError("ssh failed")):
            with self.subTest(error=type(error).__name__):
                self.deleted.clear()
                run, text = self.finish(pin_error=error)
                self.assertEqual(self.deleted, [])
                self.assertEqual(failed(run), ["Trino back on main", "references deleted", "pin file removed"])
                self.assertIn("were kept", text)

    def test_a_restoration_that_returns_no_health_keeps_the_references(self) -> None:
        run, text = self.finish(restored=False)
        self.assertEqual(self.deleted, [])
        self.assertIn("were kept", text)

    def test_a_reference_that_fails_to_travel_fails_the_run_and_the_pin_file_is_still_removed(self) -> None:
        for error in (OSError("curl exit 7"), TimeoutError("timed out")):
            with self.subTest(error=type(error).__name__):
                run, _ = self.finish(delete_error=error)
                self.assertEqual(failed(run), ["references deleted"])
                self.assertEqual([item.case for item in run.observations][-1], "pin file removed")

    def test_a_pin_file_removal_that_raises_fails_the_run(self) -> None:
        run, _ = self.finish(remove_error=OSError("ssh failed"))
        self.assertEqual(failed(run), ["pin file removed"])

    def test_a_pin_file_that_cannot_be_removed_fails_the_run(self) -> None:
        self.assertEqual(failed(self.finish(file_removed=False)[0]), ["pin file removed"])

    def test_a_reference_that_nessie_reports_as_not_found_is_absent_not_left(self) -> None:
        run, text = self.finish(never_made=("a",))
        self.assertEqual(failed(run), [])
        self.assertEqual(self.deleted, ["b"])
        self.assertIn("deleted: 1", text)
        self.assertIn("already absent: 1", text)

    def test_a_reference_that_cannot_be_read_is_left_not_absent(self) -> None:
        run, text = self.finish(unreadable=("a",))
        self.assertEqual(failed(run), ["references deleted"])
        self.assertIn("already absent: 0", text)

    def test_every_leftover_is_reported_at_once(self) -> None:
        run, _ = self.finish(restored=False, refuses=("a",), file_removed=False)
        self.assertEqual(failed(run), ["Trino back on main", "references deleted", "pin file removed"])


class RemoveFile(unittest.TestCase):
    """The targets of the catalog runner say whether the file is gone."""

    def test_the_local_target_reports_a_removed_file_and_a_file_that_is_left(self) -> None:
        module = runner("catalog_proof")
        target = module.Local.__new__(module.Local)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "pin.env"
            path.write_text("x")
            self.assertTrue(target.remove_file(str(path)))
            self.assertFalse(path.exists())
            self.assertTrue(target.remove_file(str(path)))
            with mock.patch.object(Path, "unlink"):
                path.write_text("x")
                self.assertFalse(target.remove_file(str(path)))

    def test_the_remote_target_reports_the_exit_of_the_removal(self) -> None:
        module = runner("catalog_proof")
        target = module.Remote.__new__(module.Remote)
        for code in (0, 1):
            with self.subTest(code=code), mock.patch.object(module.Remote, "run", return_value=(code, "")):
                self.assertEqual(target.remove_file("/pin.env"), code == 0)


if __name__ == "__main__":
    unittest.main()
