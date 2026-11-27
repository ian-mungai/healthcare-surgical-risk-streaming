"""The limits file is the one source of every memory limit: its loader refuses a malformed file, and the rendered values are what Compose reads.

Each refusal case changes one thing in a good file and expects the loader's message to name that thing.
"""

from __future__ import annotations

import unittest

from scripts.stack.limits import LIMITS_FILE, LimitsError, load_limits, parse_limits, render_environment, variable_name

GOOD = """\
host_reserve_mib: 4096
services:
  postgres:
    runtime: compose
    group: supporting
    memory_limit_mib: 1024
  airbyte_server:
    runtime: k3s
    group: ingestion
    memory_limit_mib: 2048
"""


def refused(text: str) -> str:
    try:
        parse_limits(text)
    except LimitsError as error:
        return str(error)
    raise AssertionError("the loader accepted a file that it must refuse")


class LoadTest(unittest.TestCase):
    def test_the_tracked_file_loads_and_holds_postgres_as_a_supporting_compose_service(self) -> None:
        limits = load_limits(LIMITS_FILE)
        self.assertGreater(limits.host_reserve_mib, 0)
        self.assertEqual(("compose", "supporting"), (limits.services["postgres"].runtime, limits.services["postgres"].group))
        self.assertGreater(limits.services["postgres"].memory_limit_mib, 0)

    def test_a_good_file_loads_every_field(self) -> None:
        limits = parse_limits(GOOD)
        self.assertEqual(4096, limits.host_reserve_mib)
        self.assertEqual(["postgres", "airbyte_server"], list(limits.services))
        self.assertEqual(("k3s", "ingestion", 2048), tuple(vars(limits.services["airbyte_server"]).values()))

    def test_a_missing_file_is_refused_by_name(self) -> None:
        with self.assertRaisesRegex(LimitsError, "does_not_exist.yml"):
            load_limits(LIMITS_FILE.parent / "does_not_exist.yml")


class RefusalTest(unittest.TestCase):
    def test_a_file_that_is_not_a_mapping_is_refused(self) -> None:
        self.assertIn("mapping", refused("- postgres\n"))
        self.assertIn("mapping", refused(""))

    def test_a_missing_or_unknown_top_level_key_is_refused(self) -> None:
        self.assertIn("host_reserve_mib", refused(GOOD.replace("host_reserve_mib: 4096\n", "")))
        self.assertIn("reserve", refused(GOOD + "reserve: 1\n"))

    def test_a_reserve_that_is_not_a_positive_whole_number_is_refused(self) -> None:
        for value in ("0", "-1", "1.5", "lots", "true"):
            with self.subTest(value=value):
                self.assertIn("host_reserve_mib", refused(GOOD.replace("host_reserve_mib: 4096", f"host_reserve_mib: {value}")))

    def test_no_services_is_refused(self) -> None:
        self.assertIn("services", refused("host_reserve_mib: 1\nservices: {}\n"))

    def test_a_memory_limit_that_is_not_a_positive_whole_number_is_refused(self) -> None:
        for value in ("0", "-512", "1.5", "big", "true", "'1024'"):
            with self.subTest(value=value):
                message = refused(GOOD.replace("memory_limit_mib: 1024", f"memory_limit_mib: {value}"))
                self.assertIn("postgres", message)
                self.assertIn("memory_limit_mib", message)

    def test_an_unknown_runtime_is_refused_with_the_service_named(self) -> None:
        message = refused(GOOD.replace("runtime: compose", "runtime: swarm"))
        self.assertIn("postgres", message)
        self.assertIn("runtime", message)

    def test_a_missing_field_or_an_unknown_field_of_a_service_is_refused(self) -> None:
        for field in ("runtime: compose", "group: supporting", "memory_limit_mib: 1024"):
            with self.subTest(missing=field):
                self.assertIn(field.split(":")[0], refused(GOOD.replace(f"    {field}\n", "", 1)))
        self.assertIn("mem_limit", refused(GOOD.replace("    group: supporting\n", "    group: supporting\n    mem_limit: 1g\n")))

    def test_a_service_name_that_cannot_make_a_variable_is_refused(self) -> None:
        for name in ("Postgres", "post-gres", "1postgres", "post gres"):
            with self.subTest(name=name):
                self.assertIn(name, refused(GOOD.replace("  postgres:", f"  '{name}':")))

    def test_an_empty_group_is_refused(self) -> None:
        self.assertIn("group", refused(GOOD.replace("group: supporting", "group: ''")))

    def test_a_repeated_service_is_refused_not_merged(self) -> None:
        self.assertIn("postgres", refused(GOOD + "  postgres:\n    runtime: compose\n    group: supporting\n    memory_limit_mib: 1\n"))


class RenderTest(unittest.TestCase):
    def test_a_compose_service_gets_one_variable_in_mebibytes(self) -> None:
        self.assertEqual("MEM_LIMIT_POSTGRES=1024m\n", render_environment(parse_limits(GOOD)))

    def test_a_k3s_workload_gets_no_compose_variable(self) -> None:
        self.assertNotIn("AIRBYTE", render_environment(parse_limits(GOOD)))

    def test_every_compose_service_is_rendered_in_name_order(self) -> None:
        text = GOOD + "  kafka:\n    runtime: compose\n    group: stream\n    memory_limit_mib: 3072\n"
        self.assertEqual("MEM_LIMIT_KAFKA=3072m\nMEM_LIMIT_POSTGRES=1024m\n", render_environment(parse_limits(text)))

    def test_the_variable_name_is_the_upper_case_service_name(self) -> None:
        self.assertEqual("MEM_LIMIT_OPEN_BAO", variable_name("open_bao"))


if __name__ == "__main__":
    unittest.main()
