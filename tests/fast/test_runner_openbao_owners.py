"""The OpenBao runner expects each secret ID file to be owned by the user that the deploy gives it, read the way the deploy reads it.

The runner stays untracked, so each test skips when it is not on this machine. The target is a stand-in that answers ``stat`` as the live host did on
2026-11-24: every file mode 400, nessie 10000:10001, dagster 10001:10001, apicurio 10002:10002. No Docker, SSH or host runs here.

Failure modes covered here:

- The runner cuts each owner at its colon (the cut that a tool line needs for its databases), expects ``10000`` for ``10000:10001`` and reports nessie, dagster
  and apicurio as wrong on a host where all three are right.
- The runner keeps its own list of owners, so an owner that the deploy changes is not the owner it expects.
- A file with a wrong mode or a wrong owner passes, or the observation does not say which of the two is wrong.
- A service with no owner on its line (it runs outside Compose) is not checked against root, which the deploy gives it.
- The observation shows more than the mode and the numeric ids.
- The runner expects a file for each service that has a PostgreSQL account and not for each deployed service, so a missing file of a service with no
  account (the list of the deploy has more services than the list of the accounts) passes.
- A ``stat`` answer that is not ``<octal mode> <uid>:<gid>`` (an error text, extra words, letters in an id) is shown as it is in the observation, or reads as
  a mode or an owner. It must read as unreadable, with none of its text.
- The folder's mode is cut from the first word of any answer, so an error text on the folder shows in the observation.
- The Nessie runner prints the raw ``stat`` answer and the raw answer of ``id`` in its observations.
"""

from __future__ import annotations

import importlib
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace

from scripts.project_config import ROOT
from tests.e2e.run_record import Record

E2E = Path(__file__).resolve().parents[1] / "e2e"
WARNING = "Warning: Permanently added '100.64.0.9' (ED25519) to the list of known hosts.\n"
HOST_MODES = {
    "bootstrap": "400 100:1000",
    "nessie": "400 10000:10001",
    "airbyte": "400 0:0",
    "dagster": "400 10001:10001",
    "alloy": "400 0:0",
    "apicurio": "400 10002:10002",
}


def runner() -> ModuleType:
    if not (E2E / "stack_openbao.py").is_file():
        raise unittest.SkipTest("the untracked runners are not on this machine")
    return importlib.import_module("tests.e2e.stack_openbao")


class HostTarget:
    secrets_directory = "/opt/platform/secrets"
    compose_directory = "/opt/platform/compose"
    project = "surgical_risk"

    def __init__(self, modes: dict[str, str]) -> None:
        self.modes = modes

    folder = "700 0:0"

    def mode(self, path: str) -> str:
        if path.endswith("/openbao"):
            return self.folder
        return self.modes.get(path.rsplit("/", 1)[1].removesuffix(".secret_id"), "unreadable")


class Owners(unittest.TestCase):
    def check(self, modes: dict[str, str], without: str = "", extra_service: str = "", folder: str = "700 0:0") -> tuple[bool, str]:
        module = runner()
        record = Record(feature="x", slug="x", rerun="x")
        owners = {name: owner for name, owner in module.deployed_owners().items() if name != without}
        services = [*module.listed_names(ROOT / "compose" / "openbao_services.txt"), *([extra_service] if extra_service else [])]
        tools = module.listed_names(ROOT / "compose" / "postgres_tools.txt")
        target = HostTarget(modes)
        target.folder = folder
        module.private_files(module.Stack(target, record, [], services, tools, owners))
        observation = record.observations[-1]
        return observation.passed, observation.observed

    def test_the_owners_of_the_deploy_are_the_ones_of_the_host(self) -> None:
        owners = runner().deployed_owners()
        self.assertEqual(owners, {name: mode.split()[1] for name, mode in HOST_MODES.items()})

    def test_the_files_of_the_host_pass(self) -> None:
        passed, observed = self.check(HOST_MODES)
        self.assertTrue(passed, observed)
        self.assertIn("wrong: []", observed)

    def test_a_wrong_mode_is_named_as_a_mode_and_not_as_an_owner(self) -> None:
        passed, observed = self.check({**HOST_MODES, "nessie": "440 10000:10001"})
        self.assertFalse(passed)
        self.assertIn("nessie: mode 440", observed)
        self.assertNotIn("nessie: owner", observed)

    def test_a_wrong_owner_is_named_as_an_owner_with_both_ids_and_not_as_a_mode(self) -> None:
        passed, observed = self.check({**HOST_MODES, "dagster": "400 10002:10002"})
        self.assertFalse(passed)
        self.assertIn("dagster: owner 10002:10002, expected 10001:10001", observed)
        self.assertNotIn("dagster: mode", observed)

    def test_a_wrong_mode_and_a_wrong_owner_are_both_named(self) -> None:
        passed, observed = self.check({**HOST_MODES, "apicurio": "644 0:0"})
        self.assertFalse(passed)
        self.assertIn("apicurio: mode 644, owner 0:0, expected 10002:10002", observed)

    def test_a_service_outside_compose_must_be_owned_by_root(self) -> None:
        passed, observed = self.check({**HOST_MODES, "airbyte": "400 1000:1000"})
        self.assertFalse(passed)
        self.assertIn("airbyte: owner 1000:1000, expected 0:0", observed)

    def test_the_admin_file_must_be_owned_by_the_openbao_user(self) -> None:
        passed, observed = self.check({**HOST_MODES, "bootstrap": "400 0:0"})
        self.assertFalse(passed)
        self.assertIn("bootstrap: owner 0:0, expected 100:1000", observed)

    def test_a_file_with_no_expected_owner_fails(self) -> None:
        passed, observed = self.check(HOST_MODES, without="alloy")
        self.assertFalse(passed)
        self.assertIn("alloy: owner 0:0, expected none listed", observed)

    def test_an_unreadable_file_fails(self) -> None:
        modes = {name: mode for name, mode in HOST_MODES.items() if name != "alloy"}
        passed, observed = self.check(modes)
        self.assertFalse(passed)
        self.assertIn("alloy", observed)

    def test_the_observation_holds_only_modes_and_numeric_ids(self) -> None:
        _, observed = self.check({**HOST_MODES, "nessie": "440 10000:10001", "dagster": "400 1:2"})
        problems = observed.partition("wrong: ")[2].translate(str.maketrans("", "", "[]'"))
        for word in problems.replace(",", " ").split():
            self.assertRegex(word, r"^(mode|owner|expected|[a-z_]+:|[0-9]+(:[0-9]+)?)$")
        self.assertNotIn("/", observed.partition("wrong: ")[2])

    def test_a_missing_file_of_a_service_with_no_postgresql_account_fails(self) -> None:
        modes = {**HOST_MODES}
        passed, observed = self.check(modes, extra_service="probe_service")
        self.assertFalse(passed)
        self.assertIn("probe_service: unreadable", observed)

    def test_every_deployed_service_is_checked_and_counted(self) -> None:
        module = runner()
        services = module.listed_names(ROOT / "compose" / "openbao_services.txt")
        _, observed = self.check(HOST_MODES)
        self.assertIn(f"files checked: {len(services) + 1},", observed)

    def test_an_answer_that_does_not_parse_reads_as_unreadable_and_shows_none_of_its_text(self) -> None:
        for text in (
            "stat: cannot statx: Permission denied",
            "400 10000:10001 extra",
            "400 ten:10001",
            "400 10000:",
            "4x0 10000:10001",
            "400 10000 10001",
            "-1 10000:10001",
            "",
            "400 10000:10001\n400 1:1",
        ):
            with self.subTest(text=text):
                passed, observed = self.check({**HOST_MODES, "nessie": text})
                self.assertFalse(passed)
                self.assertIn("nessie: unreadable", observed)
                for word in ("stat", "Permission", "extra", "ten", "4x0"):
                    self.assertNotIn(word, observed.replace("nessie: unreadable", ""))

    def test_an_answer_for_the_folder_that_does_not_parse_fails_and_shows_none_of_its_text(self) -> None:
        passed, observed = self.check(HOST_MODES, folder="stat: cannot statx: No such file")
        self.assertFalse(passed)
        self.assertIn("folder unreadable", observed)
        self.assertNotIn("cannot", observed)

    def test_a_folder_that_is_not_700_fails(self) -> None:
        passed, observed = self.check(HOST_MODES, folder="755 0:0")
        self.assertFalse(passed)
        self.assertIn("folder 755", observed)

    def test_the_parse_takes_the_mode_and_the_two_ids_and_nothing_else(self) -> None:
        module = runner()
        self.assertEqual(module.parse_stat("400 10000:10001"), ("400", "10000:10001"))
        self.assertEqual(module.parse_stat("700 0:0\n"), ("700", "0:0"))
        for text in ("", "400", "400 1", "400 1:2:3", "400 a:b", "8 1:2", "400 1:2 x", "unreadable"):
            self.assertIsNone(module.parse_stat(text), text)


class NessieStatAnswers(unittest.TestCase):
    """The Nessie runner shows the owner, the group and the mode as numbers, or says unreadable."""

    def stack(self, module: ModuleType, process: tuple[int, str], stat: tuple[int, str]) -> tuple[object, Record]:
        """A stack whose ``run`` and ``compose_run`` answer with the SSH warning after the answer, as the live host's merged output did, and whose ``read`` and
        ``compose_out`` answer with standard output alone."""
        record = Record(feature="x", slug="x", rerun="x")
        target = SimpleNamespace(
            secrets_directory="/opt/platform/secrets",
            read=lambda args, **_: stat,
            run=lambda args, **_: (stat[0], stat[1] + WARNING),
        )
        stack = SimpleNamespace(
            record=record,
            target=target,
            compose_out=lambda *args, **_: process,
            compose_run=lambda *args, **_: (process[0], process[1] + WARNING),
        )
        return stack, record

    def cases(self, process: tuple[int, str], stat: tuple[int, str]) -> dict[str, tuple[bool, str]]:
        if not (E2E / "stack_nessie.py").is_file():
            raise unittest.SkipTest("the untracked runners are not on this machine")
        module = importlib.import_module("tests.e2e.stack_nessie")
        stack, record = self.stack(module, process, stat)
        module.container_user(stack, True)
        return {item.case: (item.passed, item.observed) for item in record.observations}

    def test_the_answers_of_the_host_pass(self) -> None:
        cases = self.cases((0, "10000\n10001\n10000\n"), (0, "10000:10001:400\n"))
        self.assertTrue(all(passed for passed, _ in cases.values()), cases)

    def test_a_warning_on_the_merged_output_is_not_part_of_an_answer(self) -> None:
        for name, (passed, observed) in self.cases((0, "10000\n10001\n10000\n"), (0, "10000:10001:400\n")).items():
            with self.subTest(case=name):
                self.assertTrue(passed, observed)
                self.assertNotIn("Warning", observed)

    def test_a_wrong_owner_fails_and_shows_the_numbers(self) -> None:
        cases = self.cases((0, "10000\n10001\n10000\n"), (0, "0:0:400\n"))
        passed, observed = cases["owner of the password file"]
        self.assertFalse(passed)
        self.assertIn("0:0:400", observed)

    def test_text_that_is_not_numbers_reads_as_unreadable_and_is_not_shown(self) -> None:
        cases = self.cases((0, "OCI runtime exec failed: unable to start\n"), (0, "stat: cannot statx: Permission denied\n"))
        for passed, observed in cases.values():
            self.assertFalse(passed)
            self.assertIn("unreadable", observed)
            for word in ("OCI", "runtime", "cannot", "Permission"):
                self.assertNotIn(word, observed)


if __name__ == "__main__":
    unittest.main()
