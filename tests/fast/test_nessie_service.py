"""The Nessie service file keeps its version store in the PostgreSQL account and database that the tool list provisions, and reads the password from its file.

The generic checks of ``test_compose_services`` hold every service to the shared rules. These checks hold what only this service can get wrong:
a name that differs from the tool list gives an account that PostgreSQL never created, so Nessie would not become healthy.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path
from typing import Any

from scripts.stack.yaml_text import load_yaml
from tests.fast.test_postgres_accounts import TOOLS_FILE, accounts

COMPOSE = Path(__file__).resolve().parents[2] / "compose"
TOOLS: dict[str, list[str]] = accounts.parse_tools(TOOLS_FILE.read_text())


def nessie() -> dict[str, Any]:
    document = load_yaml((COMPOSE / "services" / "nessie.yml").read_text())
    if not isinstance(document, dict):
        raise TypeError("nessie.yml is not a mapping")
    return document


class NessieServiceTest(unittest.TestCase):
    def test_nessie_is_a_tool_of_the_list_so_its_account_and_its_own_database_exist(self) -> None:
        self.assertEqual(["nessie"], TOOLS.get("nessie"))

    def test_the_version_store_is_the_tools_own_database_and_account_on_the_postgres_service(self) -> None:
        environment = nessie()["services"]["nessie"]["environment"]
        self.assertEqual("JDBC2", environment["NESSIE_VERSION_STORE_TYPE"])
        self.assertEqual("postgresql", environment["NESSIE_VERSION_STORE_PERSIST_JDBC_DATASOURCE"])
        self.assertEqual("jdbc:postgresql://postgres:5432/nessie", environment["QUARKUS_DATASOURCE_POSTGRESQL_JDBC_URL"])
        self.assertEqual("nessie", environment["QUARKUS_DATASOURCE_POSTGRESQL_USERNAME"])

    def test_the_password_is_the_tools_password_file_and_no_value_is_in_the_file(self) -> None:
        document = nessie()
        service = document["services"]["nessie"]
        self.assertEqual("${SECRETS_DIR:?render the host settings first}/postgres/nessie.password", document["secrets"]["nessie_database_password"]["file"])
        self.assertEqual(["nessie_database_password"], service["secrets"])
        self.assertIn("/run/secrets/nessie_database_password", " ".join(service["command"]))
        self.assertFalse([key for key in service["environment"] if "PASSWORD" in key])

    def test_nessie_runs_as_the_numeric_user_of_its_image_and_never_as_root_with_all_capabilities_dropped(self) -> None:
        service = nessie()["services"]["nessie"]
        self.assertEqual("10000:10001", service["user"])
        self.assertEqual(["ALL"], service["cap_drop"])
        self.assertEqual(["no-new-privileges:true"], service["security_opt"])

    def test_nessie_waits_for_a_healthy_postgres_before_it_starts(self) -> None:
        self.assertEqual({"postgres": {"condition": "service_healthy"}}, nessie()["services"]["nessie"]["depends_on"])

    def test_only_the_api_is_published_and_only_on_the_loopback_address(self) -> None:
        ports = nessie()["services"]["nessie"]["ports"]
        self.assertEqual(1, len(ports))
        self.assertRegex(ports[0], r"^127\.0\.0\.1:\$\{NESSIE_HOST_PORT:-\d+\}:19120$")

    def test_the_healthcheck_asks_the_readiness_endpoint_of_the_management_port(self) -> None:
        test = " ".join(nessie()["services"]["nessie"]["healthcheck"]["test"])
        self.assertRegex(test, re.escape("http://127.0.0.1:9000/q/health/ready"))


if __name__ == "__main__":
    unittest.main()
