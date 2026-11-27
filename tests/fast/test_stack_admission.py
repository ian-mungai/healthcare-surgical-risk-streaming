"""The admission check decides from stated figures: which services run, the host's available memory and the limits file.

Each case changes one figure of a good setup and expects the decision, its four figures or its refusal to follow from that figure alone.
"""

from __future__ import annotations

import contextlib
import io
import re
import tempfile
import unittest
from pathlib import Path

from scripts.ingestion.notice import Code, Notice, render
from scripts.stack.admission import (
    COMPOSE_PROJECT,
    EXIT_ADMITTED,
    EXIT_REFUSED,
    EXIT_WAIT,
    AdmissionError,
    decide,
    describe,
    main,
    parse_available_mib,
    parse_running,
)
from scripts.stack.limits import LIMITS_FILE, Limits, parse_limits

GOOD = """\
host_reserve_mib: 4096
services:
  postgres:
    runtime: compose
    group: supporting
    memory_limit_mib: 1024
  heavy_one:
    runtime: compose
    group: heavy
    memory_limit_mib: 4096
  heavy_two:
    runtime: compose
    group: heavy
    memory_limit_mib: 2048
  airbyte_worker:
    runtime: k3s
    group: ingestion
    memory_limit_mib: 3072
  airbyte_server:
    runtime: k3s
    group: ingestion
    memory_limit_mib: 1024
  stream_broker:
    runtime: compose
    group: ingestion
    memory_limit_mib: 512
  chart_registry:
    runtime: k3s
    group: supporting
    memory_limit_mib: 256
"""
HEAVY_NEED = 4096 + 2048
SUPPORTING_NEED = 1024 + 256
ALL_IN = 4096 + SUPPORTING_NEED + HEAVY_NEED


def limits() -> Limits:
    return parse_limits(GOOD)


def decided(group: str, available: int, running: frozenset[str] = frozenset(), text: str = GOOD):  # type: ignore[no-untyped-def]
    return decide(parse_limits(text), group, running, available)


def refused(group: str, text: str = GOOD) -> str:
    try:
        decide(parse_limits(text), group, frozenset(), 100000)
    except AdmissionError as error:
        return str(error)
    raise AssertionError("the check accepted a group that it must refuse")


class DecisionTest(unittest.TestCase):
    def test_a_group_that_fits_is_admitted_with_its_four_figures(self) -> None:
        decision = decided("heavy", ALL_IN + 1000)
        self.assertTrue(decision.admitted)
        self.assertEqual(
            (4096, SUPPORTING_NEED, HEAVY_NEED, ALL_IN + 1000), (decision.reserve_mib, decision.supporting_mib, decision.group_mib, decision.available_mib)
        )

    def test_a_group_that_exceeds_the_available_memory_waits_with_the_same_figures(self) -> None:
        decision = decided("heavy", ALL_IN - 1000)
        self.assertFalse(decision.admitted)
        self.assertEqual(
            (4096, SUPPORTING_NEED, HEAVY_NEED, ALL_IN - 1000), (decision.reserve_mib, decision.supporting_mib, decision.group_mib, decision.available_mib)
        )

    def test_the_edge_where_the_need_equals_what_is_left_after_the_reserve_is_admitted(self) -> None:
        self.assertTrue(decided("heavy", ALL_IN).admitted)
        self.assertFalse(decided("heavy", ALL_IN - 1).admitted)

    def test_a_supporting_service_that_runs_is_not_counted_again(self) -> None:
        running = frozenset({"postgres", "chart_registry"})
        decision = decided("heavy", 4096 + HEAVY_NEED, running)
        self.assertEqual(0, decision.supporting_mib)
        self.assertTrue(decision.admitted)
        self.assertFalse(decided("heavy", 4096 + HEAVY_NEED - 1, running).admitted)

    def test_only_the_supporting_services_that_do_not_run_are_counted(self) -> None:
        self.assertEqual(256, decided("heavy", 100000, frozenset({"postgres"})).supporting_mib)
        self.assertEqual(1024, decided("heavy", 100000, frozenset({"chart_registry"})).supporting_mib)

    def test_a_service_of_the_group_that_runs_is_not_counted_again(self) -> None:
        self.assertEqual(2048, decided("heavy", 100000, frozenset({"heavy_one"})).group_mib)

    def test_a_group_whose_services_all_run_is_admitted_with_nothing_to_start(self) -> None:
        running = frozenset({"postgres", "chart_registry", "heavy_one", "heavy_two"})
        decision = decided("heavy", 100, running)
        self.assertEqual((0, 0), (decision.supporting_mib, decision.group_mib))
        self.assertTrue(decision.admitted)

    def test_a_running_name_that_the_limits_file_does_not_hold_is_ignored(self) -> None:
        self.assertEqual(decided("heavy", 20000), decided("heavy", 20000, frozenset({"grafana", "heavy"})))

    def test_both_runtimes_are_counted_in_the_group_and_in_the_supporting_services(self) -> None:
        decision = decided("ingestion", 100000)
        self.assertEqual(3072 + 1024 + 512, decision.group_mib)
        self.assertEqual(SUPPORTING_NEED, decision.supporting_mib)

    def test_a_k3s_workload_that_runs_is_left_out_like_a_compose_service(self) -> None:
        self.assertEqual(1024 + 512, decided("ingestion", 100000, frozenset({"airbyte_worker"})).group_mib)

    def test_the_supporting_group_does_not_count_its_services_twice(self) -> None:
        decision = decided("supporting", 100000)
        self.assertEqual((0, SUPPORTING_NEED), (decision.supporting_mib, decision.group_mib))
        self.assertTrue(decided("supporting", 4096 + SUPPORTING_NEED).admitted)
        self.assertFalse(decided("supporting", 4096 + SUPPORTING_NEED - 1).admitted)

    def test_the_reserve_comes_from_the_limits_file(self) -> None:
        decision = decided("heavy", 100000, text=GOOD.replace("host_reserve_mib: 4096", "host_reserve_mib: 512"))
        self.assertEqual(512, decision.reserve_mib)

    def test_the_decision_does_not_change_its_inputs(self) -> None:
        running = frozenset({"postgres"})
        parsed = limits()
        decide(parsed, "heavy", running, 5000)
        self.assertEqual((limits(), frozenset({"postgres"})), (parsed, running))


class MessageTest(unittest.TestCase):
    def test_a_waiting_message_names_and_gives_the_four_figures_in_mebibytes(self) -> None:
        message = describe(decided("heavy", 7000))
        for name, figure in (("reserve", 4096), ("supporting services", SUPPORTING_NEED), ("group", HEAVY_NEED), ("available", 7000)):
            with self.subTest(name=name):
                self.assertRegex(message, rf"{name} {figure} MiB")
        self.assertIn("waits", message)
        self.assertIn("heavy", message)

    def test_a_waiting_message_gives_the_shortfall(self) -> None:
        self.assertIn(f"{ALL_IN - 7000} MiB short", describe(decided("heavy", 7000)))

    def test_an_admitted_message_says_admitted_and_gives_the_four_figures_and_the_spare(self) -> None:
        message = describe(decided("heavy", ALL_IN + 500))
        self.assertIn("admitted", message)
        self.assertNotIn("waits", message)
        self.assertIn("500 MiB spare", message)
        for figure in (4096, SUPPORTING_NEED, HEAVY_NEED, ALL_IN + 500):
            self.assertIn(f"{figure} MiB", message)


class RefusalTest(unittest.TestCase):
    def test_an_unknown_group_is_refused_and_the_known_groups_are_named(self) -> None:
        message = refused("batch")
        self.assertIn("batch", message)
        self.assertRegex(message, r"heavy.*ingestion.*supporting")

    def test_a_group_name_that_cannot_be_a_group_is_refused_apart_from_an_unknown_one(self) -> None:
        for name in ("", "Heavy", "heavy one", "heavy;"):
            with self.subTest(name=name):
                message = refused(name)
                self.assertIn("not a valid group name", message)
                self.assertNotIn("unknown", message)
        self.assertIn("unknown group", refused("batch"))


class HostReadingTest(unittest.TestCase):
    MEMINFO = "MemTotal:       32768000 kB\nMemFree:         1000000 kB\nMemAvailable:   20971520 kB\nBuffers:           10000 kB\n"

    def test_the_available_memory_is_read_in_whole_mebibytes_rounded_down(self) -> None:
        self.assertEqual(20480, parse_available_mib(self.MEMINFO))
        self.assertEqual(1, parse_available_mib("MemAvailable:   2047 kB\n"))

    def test_a_meminfo_without_the_available_figure_is_refused_and_not_guessed_from_free_memory(self) -> None:
        with self.assertRaisesRegex(AdmissionError, "MemAvailable"):
            parse_available_mib("MemTotal: 1 kB\nMemFree: 1 kB\n")

    def test_the_running_services_are_the_distinct_names_of_the_lines(self) -> None:
        self.assertEqual(frozenset({"postgres", "kafka"}), parse_running("postgres\nkafka\npostgres\n\n"))
        self.assertEqual(frozenset(), parse_running(""))

    def test_the_compose_project_is_the_one_the_compose_file_names(self) -> None:
        text = (LIMITS_FILE.parent / "compose.yml").read_text()
        found = re.search(r"^name: (\S+)$", text, re.MULTILINE)
        if found is None:
            self.fail("the Compose file names no project")
        self.assertEqual(COMPOSE_PROJECT, found.group(1))


def run(*arguments: str) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = main(list(arguments))
        except SystemExit as stop:
            code = int(stop.code or 0)
    return code, out.getvalue(), err.getvalue()


class CommandTest(unittest.TestCase):
    def setUp(self) -> None:
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.limits_path = Path(self.folder.name) / "limits.yml"
        self.limits_path.write_text(GOOD)

    def command(self, group: str, available: int, running: str = "") -> tuple[int, str, str]:
        return run(group, "--limits", str(self.limits_path), "--available-mib", str(available), "--running", running)

    def test_the_tracked_limits_file_gives_an_admitted_supporting_group_on_a_large_host(self) -> None:
        code, out, _ = run("supporting", "--available-mib", "30000", "--running", "")
        self.assertEqual(EXIT_ADMITTED, code)
        self.assertIn("admitted", out)

    def test_an_admitted_group_exits_zero_and_a_waiting_group_exits_with_the_wait_status_and_a_message_on_standard_output(self) -> None:
        self.assertEqual(EXIT_ADMITTED, self.command("heavy", ALL_IN)[0])
        code, out, err = self.command("heavy", ALL_IN - 1)
        self.assertEqual((EXIT_WAIT, ""), (code, err))
        self.assertIn("waits", out)

    def test_the_three_statuses_differ(self) -> None:
        self.assertEqual(3, len({EXIT_ADMITTED, EXIT_REFUSED, EXIT_WAIT}))

    def test_an_unknown_group_exits_with_the_refusal_status_and_not_the_wait_status(self) -> None:
        code, out, err = self.command("batch", 100000)
        self.assertEqual((EXIT_REFUSED, ""), (code, out))
        self.assertIn("AdmissionError", err)

    def test_a_limits_file_that_fails_its_checks_exits_with_the_refusal_status_and_the_loaders_message(self) -> None:
        self.limits_path.write_text(GOOD.replace("memory_limit_mib: 4096", "memory_limit_mib: 0"))
        code, out, err = self.command("heavy", 100000)
        self.assertEqual((EXIT_REFUSED, ""), (code, out))
        self.assertIn("LimitsError", err)
        self.assertNotIn("heavy_one", err)

    def test_a_missing_limits_file_exits_with_the_refusal_status(self) -> None:
        code, _, err = run("heavy", "--limits", str(Path(self.folder.name) / "none.yml"), "--available-mib", "1", "--running", "")
        self.assertEqual(EXIT_REFUSED, code)
        self.assertIn("LimitsError", err)
        self.assertNotIn("none.yml", err)

    def test_the_running_names_are_a_comma_list_and_the_empty_text_is_none(self) -> None:
        code, out, _ = self.command("heavy", 4096 + HEAVY_NEED, "postgres,chart_registry")
        self.assertEqual(EXIT_ADMITTED, code)
        self.assertIn("supporting services 0 MiB", out)
        self.assertIn(f"supporting services {SUPPORTING_NEED} MiB", self.command("heavy", 100000, "")[1])

    def test_a_figure_that_is_not_a_whole_number_is_a_usage_error(self) -> None:
        for text in ("lots", "-1", "1.5", "", "\u0663\u0660\u0660\u0660\u0660", "\u00b2"):
            with self.subTest(text=text):
                code, out, err = run("heavy", "--available-mib", text, "--running", "")
                self.assertEqual((2, ""), (code, out))
                self.assertIn(render(Notice(Code.USAGE)), err)
                self.assertNotIn(text or "\0", err)


if __name__ == "__main__":
    unittest.main()
