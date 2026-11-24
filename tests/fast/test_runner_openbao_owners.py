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
"""

from __future__ import annotations

import importlib
import unittest
from pathlib import Path
from types import ModuleType

from scripts.project_config import ROOT
from tests.e2e.run_record import Record

E2E = Path(__file__).resolve().parents[1] / "e2e"
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

    def mode(self, path: str) -> str:
        if path.endswith("/openbao"):
            return "700 0:0"
        return self.modes.get(path.rsplit("/", 1)[1].removesuffix(".secret_id"), "unreadable")


class Owners(unittest.TestCase):
    def check(self, modes: dict[str, str], without: str = "") -> tuple[bool, str]:
        module = runner()
        record = Record(feature="x", slug="x", rerun="x")
        owners = {name: owner for name, owner in module.deployed_owners().items() if name != without}
        services = module.listed_names(ROOT / "compose" / "openbao_services.txt")
        tools = module.listed_names(ROOT / "compose" / "postgres_tools.txt")
        module.private_files(module.Stack(HostTarget(modes), record, [], services, tools, owners))
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


if __name__ == "__main__":
    unittest.main()
