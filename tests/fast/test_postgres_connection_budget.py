"""The PostgreSQL connection budget: the clients' maxima fit under the usable slots, every client has an entry and the tracked files cap each one.

The first group reads the real files. The second group changes one thing in a copy of the budget or of a configuration file and expects the checker to name it,
so a check that stopped looking would fail here.
"""

from __future__ import annotations

import unittest
from collections.abc import Callable, Mapping

from scripts.project_config import ROOT
from scripts.stack.connection_budget import (
    BUDGET_FILE,
    Budget,
    BudgetError,
    budget_problems,
    coverage_problems,
    load_budget,
    parse_budget,
    postgres_problems,
    setting_problems,
)
from scripts.stack.limits import load_limits

COMPOSE_SERVICES = ROOT / "compose" / "services"
TOOLS_FILE = ROOT / "compose" / "postgres_tools.txt"
POSTGRES_FILE = "compose/services/postgres.yml"

GOOD = """\
postgres:
  max_connections: 100
  superuser_reserved_connections: 3
  margin: 10
clients:
  nessie:
    account: nessie
    services: [nessie]
    max: 4
    allotment: 0
    settings:
      - {file: compose/services/nessie.yml, path: services.nessie.environment.QUARKUS_DATASOURCE_POSTGRESQL_JDBC_MAX_SIZE, value: 4, count: 1}
    source: Quarkus Agroal
  temporal:
    account: airbyte
    services: [airbyte_temporal]
    max: 20
    allotment: 4
    settings:
      - {file: ansible/roles/airbyte/files/values.yaml, path: temporal.env_vars.SQL_MAX_CONNS, value: 4, count: 4}
    source: Temporal config template
not_clients: [postgres, kafka]
"""

NESSIE_FILE = """\
services:
  nessie:
    environment:
      QUARKUS_DATASOURCE_POSTGRESQL_JDBC_MAX_SIZE: "4"
"""
VALUES_FILE = """\
temporal:
  env_vars:
    SQL_MAX_CONNS: "4"
"""
FILES = {"compose/services/nessie.yml": NESSIE_FILE, "ansible/roles/airbyte/files/values.yaml": VALUES_FILE}
FILE_TEXTS = {"nessie": "x: /postgres/nessie.password\n", "kafka": "x: 1\n"}


def refused(text: str) -> str:
    try:
        parse_budget(text)
    except BudgetError as error:
        return str(error)
    raise AssertionError("the loader accepted a file that it must refuse")


def reader(files: Mapping[str, str]) -> Callable[[str], str]:
    def read(name: str) -> str:
        return files[name]

    return read


class TrackedBudgetTest(unittest.TestCase):
    budget: Budget

    @classmethod
    def setUpClass(cls) -> None:
        cls.budget = load_budget(BUDGET_FILE)

    def test_the_tracked_file_loads_and_states_the_servers_slots(self) -> None:
        self.assertGreater(self.budget.max_connections, self.budget.superuser_reserved_connections)

    def test_the_clients_maxima_and_the_margin_fit_under_the_usable_slots(self) -> None:
        self.assertEqual([], budget_problems(self.budget))

    def test_every_service_account_and_compose_file_that_uses_the_server_has_an_entry(self) -> None:
        limits = load_limits()
        accounts = [line.split(":")[0].split("#")[0].strip() for line in TOOLS_FILE.read_text().splitlines() if line.split("#")[0].strip()]
        texts = {path.stem: path.read_text() for path in COMPOSE_SERVICES.glob("*.yml")}
        self.assertEqual([], coverage_problems(self.budget, list(limits.services), accounts, texts))

    def test_the_tracked_configuration_caps_each_client_at_its_stated_figure(self) -> None:
        self.assertEqual([], setting_problems(self.budget, lambda name: (ROOT / name).read_text()))

    def test_the_server_is_started_with_the_slots_that_the_budget_states(self) -> None:
        self.assertEqual([], postgres_problems(self.budget, (ROOT / POSTGRES_FILE).read_text()))


class LoaderTest(unittest.TestCase):
    def test_a_good_file_loads_every_field(self) -> None:
        budget = parse_budget(GOOD)
        self.assertEqual((100, 3, 10), (budget.max_connections, budget.superuser_reserved_connections, budget.margin))
        self.assertEqual(["nessie", "temporal"], list(budget.clients))
        self.assertEqual(("airbyte", 20, 4), (budget.clients["temporal"].account, budget.clients["temporal"].max, budget.clients["temporal"].allotment))
        self.assertEqual(("postgres", "kafka"), budget.not_clients)

    def test_a_missing_file_is_refused_by_name(self) -> None:
        with self.assertRaisesRegex(BudgetError, "does_not_exist.yml"):
            load_budget(BUDGET_FILE.parent / "does_not_exist.yml")

    def test_a_missing_key_a_wrong_number_or_an_unknown_key_is_refused_by_name(self) -> None:
        self.assertIn("margin", refused(GOOD.replace("  margin: 10\n", "")))
        self.assertIn("max_connections", refused(GOOD.replace("max_connections: 100", "max_connections: lots")))
        self.assertIn("margin", refused(GOOD.replace("margin: 10", "margin: -1")))
        self.assertIn("speed", refused(GOOD.replace("not_clients:", "speed: 1\nnot_clients:")))
        self.assertIn("nessie", refused(GOOD.replace("    allotment: 0\n", "", 1)))
        self.assertIn("nessie", refused(GOOD.replace("    source: Quarkus Agroal\n", "")))

    def test_a_client_with_no_service_or_a_setting_with_a_missing_field_is_refused(self) -> None:
        self.assertIn("nessie", refused(GOOD.replace("services: [nessie]", "services: []")))
        self.assertIn("path", refused(GOOD.replace("path: services.nessie.environment.QUARKUS_DATASOURCE_POSTGRESQL_JDBC_MAX_SIZE, ", "")))

    def test_a_repeated_client_is_refused_not_merged(self) -> None:
        self.assertIn("nessie", refused(GOOD.replace("  temporal:", "  nessie:\n    account: nessie\n  temporal:")))


class SumTest(unittest.TestCase):
    def test_a_good_budget_has_no_problem(self) -> None:
        self.assertEqual([], budget_problems(parse_budget(GOOD)))

    def test_a_client_over_its_cap_pushes_the_sum_over_the_usable_slots_and_the_total_is_named(self) -> None:
        problems = budget_problems(parse_budget(GOOD.replace("    max: 20\n    allotment: 4\n", "    max: 100\n    allotment: 84\n")))
        self.assertTrue(any("97" in problem and "slots" in problem for problem in problems), problems)

    def test_the_margin_counts_against_the_usable_slots(self) -> None:
        self.assertEqual([], budget_problems(parse_budget(GOOD.replace("margin: 10", "margin: 73"))))
        self.assertTrue(budget_problems(parse_budget(GOOD.replace("margin: 10", "margin: 74"))))

    def test_a_client_whose_maximum_is_not_its_settings_plus_its_allotment_is_named(self) -> None:
        problems = budget_problems(parse_budget(GOOD.replace("    max: 4\n", "    max: 5\n")))
        self.assertTrue(any("nessie" in problem and "max" in problem for problem in problems), problems)

    def test_a_server_whose_reserve_is_not_below_its_slots_is_named(self) -> None:
        self.assertTrue(
            any("superuser_reserved_connections" in problem for problem in budget_problems(parse_budget(GOOD.replace("connections: 3", "connections: 100"))))
        )


class CoverageTest(unittest.TestCase):
    budget: Budget = parse_budget(GOOD)
    services = ("postgres", "kafka", "nessie", "airbyte_temporal")

    def coverage(
        self,
        budget: Budget | None = None,
        services: tuple[str, ...] | None = None,
        accounts: tuple[str, ...] = ("nessie", "airbyte"),
        texts: Mapping[str, str] | None = None,
    ) -> list[str]:
        return coverage_problems(budget or self.budget, list(services or self.services), list(accounts), FILE_TEXTS if texts is None else texts)

    def test_a_complete_budget_has_no_problem(self) -> None:
        self.assertEqual([], self.coverage())

    def test_a_service_that_is_neither_a_client_nor_listed_as_no_client_is_named(self) -> None:
        problems = self.coverage(services=(*self.services, "airbyte_cron"))
        self.assertTrue(any("airbyte_cron" in problem for problem in problems), problems)

    def test_a_client_that_names_a_service_that_does_not_exist_and_leaves_a_real_one_without_an_entry_is_named_twice(self) -> None:
        problems = self.coverage(budget=parse_budget(GOOD.replace("services: [nessie]", "services: [nessie_x]")))
        self.assertTrue(any("nessie_x" in problem for problem in problems), problems)
        self.assertTrue(any("nessie" in problem and "no entry" in problem for problem in problems), problems)

    def test_a_service_that_is_both_a_client_and_listed_as_no_client_is_named(self) -> None:
        problems = self.coverage(budget=parse_budget(GOOD.replace("not_clients: [postgres, kafka]", "not_clients: [postgres, kafka, nessie]")))
        self.assertTrue(any("nessie" in problem and "both" in problem for problem in problems), problems)

    def test_a_compose_file_that_reads_a_postgres_password_and_is_listed_as_no_client_is_named(self) -> None:
        problems = self.coverage(texts={"nessie": FILE_TEXTS["nessie"], "kafka": "x: /postgres/kafka.password\n"})
        self.assertTrue(any("kafka" in problem and "password" in problem for problem in problems), problems)

    def test_an_account_of_the_tool_list_with_no_client_is_named(self) -> None:
        problems = self.coverage(accounts=("nessie", "airbyte", "dagster"))
        self.assertTrue(any("dagster" in problem for problem in problems), problems)

    def test_a_client_whose_account_is_not_on_the_tool_list_is_named(self) -> None:
        problems = self.coverage(accounts=("nessie",))
        self.assertTrue(any("temporal" in problem and "airbyte" in problem for problem in problems), problems)


class SettingTest(unittest.TestCase):
    budget = parse_budget(GOOD)

    def problems(self, **changes: str) -> list[str]:
        return setting_problems(self.budget, reader({**FILES, **changes}))

    def test_files_that_carry_the_figures_have_no_problem(self) -> None:
        self.assertEqual([], self.problems())

    def test_a_removed_cap_is_named_with_its_client_and_file(self) -> None:
        problems = self.problems(**{"compose/services/nessie.yml": "services:\n  nessie:\n    environment:\n      OTHER: x\n"})
        self.assertTrue(any("nessie" in problem and "nessie.yml" in problem and "no setting" in problem for problem in problems), problems)

    def test_a_cap_that_differs_from_the_budget_is_named_with_both_figures(self) -> None:
        problems = self.problems(**{"ansible/roles/airbyte/files/values.yaml": VALUES_FILE.replace('"4"', '"20"')})
        self.assertTrue(any("temporal" in problem and "20" in problem and "4" in problem for problem in problems), problems)

    def test_a_flag_in_a_command_list_is_read_from_the_item_after_it(self) -> None:
        text = GOOD.replace(
            "path: services.nessie.environment.QUARKUS_DATASOURCE_POSTGRESQL_JDBC_MAX_SIZE, value: 4", "path: services.nessie.command, flag: --pool, value: 4"
        )
        budget = parse_budget(text)
        files = {**FILES, "compose/services/nessie.yml": "services:\n  nessie:\n    command: [run, --pool, '4']\n"}
        self.assertEqual([], setting_problems(budget, reader(files)))
        files["compose/services/nessie.yml"] = "services:\n  nessie:\n    command: [run]\n"
        self.assertTrue(any("--pool" in problem for problem in setting_problems(budget, reader(files))))

    def test_a_file_that_cannot_be_read_is_named(self) -> None:
        def read(name: str) -> str:
            raise OSError(name)

        self.assertTrue(any("nessie.yml" in problem for problem in setting_problems(self.budget, read)))


class ServerTest(unittest.TestCase):
    budget = parse_budget(GOOD)
    SERVICE = "services:\n  postgres:\n    command: [postgres, -c, max_connections=100, -c, superuser_reserved_connections=3]\n"

    def test_a_server_started_with_the_budgeted_slots_has_no_problem(self) -> None:
        self.assertEqual([], postgres_problems(self.budget, self.SERVICE))

    def test_a_server_that_leaves_a_figure_to_the_image_default_is_named(self) -> None:
        problems = postgres_problems(self.budget, "services:\n  postgres:\n    image: x\n")
        self.assertTrue(any("max_connections" in problem for problem in problems), problems)

    def test_a_server_started_with_other_figures_is_named_with_both(self) -> None:
        problems = postgres_problems(self.budget, self.SERVICE.replace("max_connections=100", "max_connections=200"))
        self.assertTrue(any("max_connections" in problem and "200" in problem and "100" in problem for problem in problems), problems)
        problems = postgres_problems(self.budget, self.SERVICE.replace("reserved_connections=3", "reserved_connections=5"))
        self.assertTrue(any("superuser_reserved_connections" in problem and "5" in problem for problem in problems), problems)


if __name__ == "__main__":
    unittest.main()
