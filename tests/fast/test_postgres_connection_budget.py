"""The PostgreSQL connection budget: the clients' maxima fit under the usable slots, every client has an entry and the tracked files cap each one.

The first group reads the real files. The second group changes one thing in a copy of the budget or of a configuration file and expects the checker to name it,
so a check that stopped looking would fail here.
"""

from __future__ import annotations

import re
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
from scripts.stack.yaml_text import load_yaml

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

VALUES_FILE_PATH = "ansible/roles/airbyte/files/values.yaml"
DATABASE_USE = re.compile(r"/postgres/[a-z_]+\.password|postgres:\d+")

# The two lists below are typed from the services and the Helm release, never read from the budget file, so that a client moved to not_clients or dropped
# from clients is a difference. A Compose service is a client when its file reads a PostgreSQL password or address (checked below from the files). A pod of
# chart 2.5.0 is a client when its environment reaches DATABASE_URL, which `helm template` with the tracked values shows (tests/slow/
# test_airbyte_rendered_environment.py renders the release and compares), and Temporal's two containers read the database through SQL_* settings.
DATABASE_CLIENT_SERVICES = frozenset(
    {
        "airbyte_temporal",
        "airbyte_temporal_admin",
        "airbyte_server",
        "airbyte_workload_api_server",
        "airbyte_worker",
        "airbyte_cron",
        "airbyte_workload_launcher",
        "airbyte_bootloader",
        "apicurio",
        "nessie",
        "alloy",
        "dagster_webserver",
        "dagster_daemon",
        "dagster_code_location",
    }
)
NON_CLIENT_SERVICES = frozenset(
    {
        "postgres",
        "openbao",
        "caddy",
        "prometheus",
        "grafana",
        "loki",
        "aistor",
        "spark",
        "trino",
        "kafka",
        "replay",
        "airbyte_manifest_server",
        "airbyte_minio",
        "airbyte_minio_create_bucket",
        "airbyte_job_connector",
        "airbyte_job_check",
        "airbyte_job_discover",
        "airbyte_job_replication",
        "airbyte_job_sidecar",
    }
)

DATASOURCE = "datasources.{}.maximum-pool-size"
# Each client with its account, maximum, allotment and every setting as (file, path, flag or Java property, value, count). The names are typed here once more on
# purpose: a name that is misspelled in the budget and in the file together still differs from this table, and an unknown name sets nothing in the service.
COMPOSE = "compose/services/"
EXPECTED_CLIENTS: dict[str, tuple[str, int, int, frozenset[tuple[str, str, str | None, int, int]]]] = {
    "temporal": (
        "airbyte",
        24,
        0,
        frozenset(
            {
                (VALUES_FILE_PATH, "temporal.env_vars.SQL_MAX_CONNS", None, 4, 4),
                (VALUES_FILE_PATH, "temporal.env_vars.SQL_MAX_IDLE_CONNS", None, 4, 0),
                (VALUES_FILE_PATH, "temporal.env_vars.SQL_VIS_MAX_CONNS", None, 2, 4),
                (VALUES_FILE_PATH, "temporal.env_vars.SQL_VIS_MAX_IDLE_CONNS", None, 2, 0),
            }
        ),
    ),
    "airbyte_pods": (
        "airbyte",
        27,
        0,
        frozenset(
            {
                (VALUES_FILE_PATH, "server.configDbMaxPoolSize", None, 4, 2),
                (VALUES_FILE_PATH, "workloadApiServer.env_vars.CONFIG_DB_MAX_POOL_SIZE", None, 4, 1),
                (VALUES_FILE_PATH, "server.env_vars.CONFIG_DB_REPLICA_MAX_POOL_SIZE", None, 1, 1),
                (VALUES_FILE_PATH, "server.env_vars.JAVA_TOOL_OPTIONS", DATASOURCE.format("local-secrets"), 1, 1),
                (VALUES_FILE_PATH, "worker.env_vars.JAVA_TOOL_OPTIONS", DATASOURCE.format("local-secrets"), 1, 1),
                (VALUES_FILE_PATH, "workloadLauncher.env_vars.JAVA_TOOL_OPTIONS", DATASOURCE.format("local-secrets"), 1, 1),
                (VALUES_FILE_PATH, "cron.env_vars.JAVA_TOOL_OPTIONS", DATASOURCE.format("config"), 2, 1),
                (VALUES_FILE_PATH, "cron.env_vars.JAVA_TOOL_OPTIONS", DATASOURCE.format("jobs"), 2, 1),
                (VALUES_FILE_PATH, "cron.env_vars.JAVA_TOOL_OPTIONS", DATASOURCE.format("local-secrets"), 1, 1),
                (VALUES_FILE_PATH, "airbyteBootloader.env_vars.JAVA_TOOL_OPTIONS", DATASOURCE.format("config"), 3, 1),
                (VALUES_FILE_PATH, "airbyteBootloader.env_vars.JAVA_TOOL_OPTIONS", DATASOURCE.format("jobs"), 3, 1),
            }
        ),
    ),
    "apicurio": (
        "apicurio",
        4,
        0,
        frozenset(
            {
                (COMPOSE + "apicurio.yml", "services.apicurio.environment.APICURIO_DATASOURCE_JDBC_MAX_SIZE", None, 4, 1),
                (COMPOSE + "apicurio.yml", "services.apicurio.environment.APICURIO_DATASOURCE_JDBC_MIN_SIZE", None, 1, 0),
                (COMPOSE + "apicurio.yml", "services.apicurio.environment.APICURIO_DATASOURCE_JDBC_INITIAL_SIZE", None, 1, 0),
            }
        ),
    ),
    "nessie": (
        "nessie",
        4,
        0,
        frozenset({(COMPOSE + "nessie.yml", "services.nessie.environment.QUARKUS_DATASOURCE_POSTGRESQL_JDBC_MAX_SIZE", None, 4, 1)}),
    ),
    "alloy": ("alloy", 2, 2, frozenset()),
    "dagster_webserver": (
        "dagster",
        9,
        3,
        frozenset({(COMPOSE + "dagster_webserver.yml", "services.dagster_webserver.command", "--db-pool-max-overflow", 2, 3)}),
    ),
    "dagster_daemon": ("dagster", 9, 9, frozenset({("orchestration/dagster.yaml", "backfills.use_threads", None, False, 0)})),
    "dagster_code_location": (
        "dagster",
        8,
        2,
        frozenset({("orchestration/dagster.yaml", "concurrency.runs.max_concurrent_runs", None, 3, 2)}),
    ),
}
EXPECTED_CLIENT_SERVICES = {
    "temporal": {"airbyte_temporal", "airbyte_temporal_admin"},
    "airbyte_pods": {
        "airbyte_server",
        "airbyte_workload_api_server",
        "airbyte_worker",
        "airbyte_cron",
        "airbyte_workload_launcher",
        "airbyte_bootloader",
    },
    "apicurio": {"apicurio"},
    "nessie": {"nessie"},
    "alloy": {"alloy"},
    "dagster_webserver": {"dagster_webserver"},
    "dagster_daemon": {"dagster_daemon"},
    "dagster_code_location": {"dagster_code_location"},
}
LOCAL_SECRETS = "-Ddatasources.local-secrets.maximum-pool-size=1"
# The environment that each Airbyte component of the tracked values file gives its pods: the whole of its env_vars, so that a name that is misspelled, added or
# removed differs. The chart renders each entry as an environment value of the component's container.
EXPECTED_AIRBYTE_ENV_VARS: dict[str, dict[str, str]] = {
    "temporal": {"SQL_MAX_CONNS": "4", "SQL_MAX_IDLE_CONNS": "4", "SQL_VIS_MAX_CONNS": "2", "SQL_VIS_MAX_IDLE_CONNS": "2"},
    "workloadApiServer": {"CONFIG_DB_MAX_POOL_SIZE": "4"},
    "server": {"CONFIG_DB_REPLICA_MAX_POOL_SIZE": "1", "JAVA_TOOL_OPTIONS": LOCAL_SECRETS},
    "worker": {"JAVA_TOOL_OPTIONS": LOCAL_SECRETS},
    "workloadLauncher": {"JAVA_TOOL_OPTIONS": LOCAL_SECRETS},
    "cron": {"JAVA_TOOL_OPTIONS": "-Ddatasources.config.maximum-pool-size=2 -Ddatasources.jobs.maximum-pool-size=2 " + LOCAL_SECRETS},
    "airbyteBootloader": {"JAVA_TOOL_OPTIONS": "-Ddatasources.config.maximum-pool-size=3 -Ddatasources.jobs.maximum-pool-size=3"},
}
POSTGRES_COMMAND = ["postgres", "-c", "max_connections=100", "-c", "superuser_reserved_connections=3"]


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

    def test_the_dagster_instance_runs_the_backfills_in_the_daemons_own_thread(self) -> None:
        """Dagster 1.13.31 (daemon.py, BackfillDaemon) starts a pool of threads when ``backfills.use_threads`` is not set, and each can hold a connection."""
        document = mapping(load_yaml((ROOT / "orchestration" / "dagster.yaml").read_text()))
        self.assertEqual({"use_threads": False}, document.get("backfills"))

    def test_the_server_is_started_with_the_slots_that_the_budget_states(self) -> None:
        self.assertEqual([], postgres_problems(self.budget, (ROOT / POSTGRES_FILE).read_text()))

    def test_the_server_command_is_exactly_the_postgres_binary_with_the_two_figures_each_after_its_own_c(self) -> None:
        service = mapping(mapping(load_yaml((ROOT / POSTGRES_FILE).read_text()))["services"])["postgres"]
        self.assertEqual(POSTGRES_COMMAND, mapping(service)["command"])


def mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError("expected a mapping")
    return value


def membership_problems(budget: Budget) -> list[str]:
    """Each difference between the budget's clients and the fixed lists, which are not read from the budget."""
    problems = []
    clients = {service for client in budget.clients.values() for service in client.services}
    for service in sorted(DATABASE_CLIENT_SERVICES - clients):
        problems.append(f"{service}: opens connections and no client lists it")
    for service in sorted(clients - DATABASE_CLIENT_SERVICES):
        problems.append(f"{service}: a client lists it and it is not a database client")
    for service in sorted(DATABASE_CLIENT_SERVICES & set(budget.not_clients)):
        problems.append(f"{service}: opens connections and is listed as no client")
    for service in sorted(set(budget.not_clients) - NON_CLIENT_SERVICES):
        problems.append(f"{service}: listed as no client and not in the fixed list of non-clients")
    for service in sorted(NON_CLIENT_SERVICES - set(budget.not_clients)):
        problems.append(f"{service}: a non-client of the fixed list that the budget does not list")
    return problems


def figure_problems(budget: Budget) -> list[str]:
    """Each difference between the budget's clients and the fixed table of accounts, maxima, allotments, setting names, values and counts."""
    problems = []
    if set(budget.clients) != set(EXPECTED_CLIENTS):
        problems.append(f"the clients are {sorted(budget.clients)}, not {sorted(EXPECTED_CLIENTS)}")
    for name, (account, maximum, allotment, settings) in EXPECTED_CLIENTS.items():
        client = budget.clients.get(name)
        if client is None:
            continue
        if (client.account, client.max, client.allotment) != (account, maximum, allotment):
            problems.append(f"{name}: account, max, allotment are {(client.account, client.max, client.allotment)}, not {(account, maximum, allotment)}")
        found = frozenset((item.file, item.path, item.flag or item.java_property, item.value, item.count) for item in client.settings)
        if found != settings:
            problems.append(
                f"{name}: settings differ: only in the budget {sorted(found - settings, key=str)}, only in the table {sorted(settings - found, key=str)}"
            )
        if set(client.services) != EXPECTED_CLIENT_SERVICES[name]:
            problems.append(f"{name}: services are {sorted(client.services)}, not {sorted(EXPECTED_CLIENT_SERVICES[name])}")
    return problems


class FixedTableTest(unittest.TestCase):
    """The budget against lists and figures that this file types by itself, so that the budget and the files cannot agree on a wrong name."""

    budget: Budget
    budget_text: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.budget_text = BUDGET_FILE.read_text()
        cls.budget = parse_budget(cls.budget_text)

    def test_every_service_of_the_limits_file_is_in_exactly_one_of_the_two_fixed_lists(self) -> None:
        self.assertEqual(set(load_limits().services), DATABASE_CLIENT_SERVICES | NON_CLIENT_SERVICES)
        self.assertEqual(frozenset(), DATABASE_CLIENT_SERVICES & NON_CLIENT_SERVICES)

    def test_the_compose_services_that_read_the_server_are_the_compose_part_of_the_fixed_client_list(self) -> None:
        reading = {path.stem for path in COMPOSE_SERVICES.glob("*.yml") if path.stem != "postgres" and DATABASE_USE.search(path.read_text())}
        compose = {name for name, service in load_limits().services.items() if service.runtime == "compose"}
        self.assertEqual(reading, DATABASE_CLIENT_SERVICES & compose)

    def test_the_budget_lists_exactly_the_database_clients_and_the_non_clients_of_the_fixed_lists(self) -> None:
        self.assertEqual([], membership_problems(self.budget))

    def test_the_budget_states_the_fixed_accounts_maxima_allotments_and_setting_names(self) -> None:
        self.assertEqual([], figure_problems(self.budget))

    def test_a_client_moved_to_no_client_is_named(self) -> None:
        moved = self.budget_text.replace("    services: [apicurio]\n", "    services: [apicurio_gone]\n").replace(
            "not_clients:\n", "not_clients:\n  - apicurio\n"
        )
        problems = membership_problems(parse_budget(moved))
        self.assertTrue(any("apicurio" in problem and "no client" in problem for problem in problems), problems)

    def test_a_client_dropped_from_the_budget_is_named(self) -> None:
        problems = membership_problems(parse_budget(self.budget_text.replace("services: [alloy]", "services: [alloy_gone]")))
        self.assertTrue(any("alloy:" in problem and "no client lists it" in problem for problem in problems), problems)

    def test_a_setting_name_that_is_misspelled_in_the_budget_and_in_the_file_together_is_named_by_the_table(self) -> None:
        wrong = self.budget_text.replace("temporal.env_vars.SQL_MAX_CONNS", "temporal.env_vars.SQL_MAX_CONN")
        budget = parse_budget(wrong)
        values_text = (ROOT / VALUES_FILE_PATH).read_text().replace("    SQL_MAX_CONNS:", "    SQL_MAX_CONN:")
        self.assertEqual([], setting_problems(budget, lambda name: values_text if name == VALUES_FILE_PATH else (ROOT / name).read_text()))
        problems = figure_problems(budget)
        self.assertTrue(any("temporal" in problem and "SQL_MAX_CONN" in problem for problem in problems), problems)

    def test_a_changed_maximum_or_a_changed_count_is_named_by_the_table(self) -> None:
        self.assertTrue(
            figure_problems(
                parse_budget(
                    self.budget_text.replace(
                        "    max: 24\n    allotment: 0\n    settings:\n      - {file: ansible",
                        "    max: 25\n    allotment: 1\n    settings:\n      - {file: ansible",
                        1,
                    )
                )
            )
        )


class RenderedEnvironmentTest(unittest.TestCase):
    """What the Helm release gives each component: the tracked values file with the three files of the release (the other two hold no env_vars)."""

    values: dict[str, object]

    @classmethod
    def setUpClass(cls) -> None:
        cls.values = mapping(load_yaml((ROOT / VALUES_FILE_PATH).read_text()))

    def test_each_component_gives_its_pods_exactly_the_fixed_environment_and_no_other_component_sets_one(self) -> None:
        found = {name: mapping(component["env_vars"]) for name, component in self.values.items() if isinstance(component, dict) and "env_vars" in component}
        self.assertEqual(EXPECTED_AIRBYTE_ENV_VARS, found)

    def test_the_server_gives_both_of_its_pools_the_chart_setting_that_reaches_the_shared_configuration(self) -> None:
        self.assertEqual("4", mapping(self.values["server"])["configDbMaxPoolSize"])

    def test_a_misspelled_name_in_the_values_file_is_a_difference_from_the_fixed_environment(self) -> None:
        wrong = (ROOT / VALUES_FILE_PATH).read_text().replace("CONFIG_DB_REPLICA_MAX_POOL_SIZE", "CONFIG_DB_REPLICA_MAX_POOL_SZIE")
        document = mapping(load_yaml(wrong))
        self.assertNotEqual(EXPECTED_AIRBYTE_ENV_VARS["server"], mapping(mapping(document["server"])["env_vars"]))


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

    def test_a_switch_that_must_stay_off_is_a_setting_with_the_value_false_and_no_connection(self) -> None:
        text = GOOD.replace("count: 4}", "count: 4}\n      - {file: orchestration/dagster.yaml, path: backfills.use_threads, value: false, count: 0}", 1)
        budget = parse_budget(text)
        switch = budget.clients["temporal"].settings[-1]
        self.assertEqual((False, 0), (switch.value, switch.count))
        self.assertEqual([], budget_problems(budget), "a switch adds no connection to the client's settings")

    def test_a_switch_that_holds_a_connection_or_is_on_is_refused(self) -> None:
        switch = "- {file: orchestration/dagster.yaml, path: backfills.use_threads, value: %s, count: %s}"
        for value, count in (("false", "1"), ("true", "0"), ("true", "1")):
            with self.subTest(value=value, count=count):
                text = GOOD.replace("count: 4}", "count: 4}\n      " + switch % (value, count), 1)
                self.assertIn("value", refused(text))

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

    def test_a_java_property_inside_a_text_of_options_is_read_by_its_name(self) -> None:
        text = GOOD.replace(
            "path: services.nessie.environment.QUARKUS_DATASOURCE_POSTGRESQL_JDBC_MAX_SIZE, value: 4",
            "path: services.nessie.environment.JAVA_TOOL_OPTIONS, java_property: datasources.config.maximum-pool-size, value: 4",
        )
        budget = parse_budget(text)
        files = {
            **FILES,
            "compose/services/nessie.yml": (
                "services:\n  nessie:\n    environment:\n      JAVA_TOOL_OPTIONS: -Xmx1g -Ddatasources.config.maximum-pool-size=4 -Dx=1\n"
            ),
        }
        self.assertEqual([], setting_problems(budget, reader(files)))
        for options in ("-Ddatasources.config.maximum-pool-size=20", "-Ddatasources.jobs.maximum-pool-size=4", "-Ddatasources.config.maximum-pool-size4", ""):
            files["compose/services/nessie.yml"] = f"services:\n  nessie:\n    environment:\n      JAVA_TOOL_OPTIONS: '{options}'\n"
            problems = setting_problems(budget, reader(files))
            self.assertTrue(any("datasources.config.maximum-pool-size" in problem for problem in problems), (options, problems))

    def test_a_switch_is_read_from_the_file_and_a_file_that_turns_it_on_or_leaves_it_out_is_named(self) -> None:
        """Dagster starts a thread pool for backfills when ``backfills`` is unset, so an absent switch is as wrong as one that is on."""
        text = GOOD.replace("count: 4}", "count: 4}\n      - {file: orchestration/dagster.yaml, path: backfills.use_threads, value: false, count: 0}", 1)
        budget = parse_budget(text)
        files = {**FILES, "orchestration/dagster.yaml": "backfills:\n  use_threads: false\n"}
        self.assertEqual([], setting_problems(budget, reader(files)))
        for document in ("backfills:\n  use_threads: true\n", "backfills:\n  num_workers: 4\n", "telemetry:\n  enabled: false\n"):
            with self.subTest(document=document):
                problems = setting_problems(budget, reader({**FILES, "orchestration/dagster.yaml": document}))
                self.assertTrue(any("backfills.use_threads" in problem for problem in problems), problems)

    def test_a_setting_that_names_both_a_flag_and_a_java_property_is_refused(self) -> None:
        both = GOOD.replace("count: 1}", "count: 1, flag: --a, java_property: b}", 1)
        self.assertIn("flag", refused(both))

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

    def test_a_figure_that_is_not_the_item_after_a_c_is_named_as_if_it_were_missing(self) -> None:
        for command in (
            "[postgres, max_connections=100, -c, superuser_reserved_connections=3]",
            "[postgres, -c, max_connections=100, superuser_reserved_connections=3]",
            "[postgres, max_connections=100, superuser_reserved_connections=3]",
            "[postgres, -c, max_connections=100, -c]",
        ):
            problems = postgres_problems(self.budget, f"services:\n  postgres:\n    command: {command}\n")
            self.assertTrue(problems, command)
        problems = postgres_problems(
            self.budget, "services:\n  postgres:\n    command: [postgres, -c, max_connections=100, superuser_reserved_connections=3]\n"
        )
        self.assertTrue(any("superuser_reserved_connections" in problem for problem in problems), problems)
        self.assertFalse(any("max_connections:" in problem for problem in problems), problems)

    def test_a_figure_given_twice_with_different_values_is_named(self) -> None:
        problems = postgres_problems(self.budget, self.SERVICE.replace("]", ", -c, max_connections=200]"))
        self.assertTrue(any("max_connections" in problem and "twice" in problem for problem in problems), problems)

    def test_a_server_started_with_other_figures_is_named_with_both(self) -> None:
        problems = postgres_problems(self.budget, self.SERVICE.replace("max_connections=100", "max_connections=200"))
        self.assertTrue(any("max_connections" in problem and "200" in problem and "100" in problem for problem in problems), problems)
        problems = postgres_problems(self.budget, self.SERVICE.replace("reserved_connections=3", "reserved_connections=5"))
        self.assertTrue(any("superuser_reserved_connections" in problem and "5" in problem for problem in problems), problems)


if __name__ == "__main__":
    unittest.main()
