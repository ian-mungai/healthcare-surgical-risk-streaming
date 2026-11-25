"""The host runners accept a read only in its exact expected form: any other reply is an ``Unread`` and fails the case (fix round 4, recheck 3 of 5f85871).

The runners stay untracked, so each test skips when its runner is not on this machine. Docker, SSH, Prometheus, Loki and the connections are stand-ins:
no cloud, Docker or Spark runs here.

Failure modes covered here:

- A Prometheus or Loki answer whose status is not ``success``, or whose ``result`` is not a list of objects with the fields used (a mapping, a text, null,
  items of another shape), reads as "no series" or "no lines", so an absence is concluded from a reply that held no result.
- The wait for a metric that must stay missing ends at its deadline with a message although some poll was unread, so the absence was never read.
- A start time is any text, so two reads that differ only in their last fractional digit, or that are not timestamps, read as equal or as read. The monitoring
  snapshot takes any word as a container ID.
- A refusal on a port is decided from the TLS exchange or from any error of the connect: a timeout, an unreachable network or any error reads as "refused", and
  an open port whose TLS fails reads as closed (Caddy's public address and outsider cases, and the probes of the other addresses of this computer in Nessie,
  OpenBao and the catalog proof).
"""

from __future__ import annotations

import json
import unittest
from collections.abc import Callable, Iterator
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

from tests.e2e.run_record import DROPPED, OPEN, REFUSED, Record, Unread
from tests.fast.test_runner_failed_reads import GOOD, ID, OTHER, UNREAD_CASES, failed, inspect_answers, only, record, shaped
from tests.fast.test_runner_stdout_alone import Noisy, runner

OTHER_ID = "fedcba9876543210" * 4
NANOS = "2026-11-25T00:00:00.123456789Z"
NANOS_NEXT = "2026-11-25T00:00:00.123456788Z"
STARTED = ("stack_caddy", "stack_nessie", "stack_monitoring")


class Series(unittest.TestCase):
    """The parsers of Prometheus and Loki answers: a status of success and a list of objects with the fields used, or an ``Unread``."""

    def stack(self, status: int, text: str) -> Any:
        module = runner("stack_monitoring")
        target = SimpleNamespace(
            http=lambda port, method, path, **keywords: (status, text),
            ports={"prometheus": 9090, "loki": 3100},
            project="p",
            compose_directory="/c",
            secrets_directory="/s",
            env_files=(),
        )
        return module.Stack(target, record(), [])

    def instant(self, result: Any, status: Any = "success") -> str:
        document: dict[str, Any] = {"data": {"resultType": "vector", "result": result}}
        if status is not None:
            document["status"] = status
        return json.dumps(document)

    def streams(self, result: Any, status: Any = "success") -> str:
        document: dict[str, Any] = {"data": {"resultType": "streams", "result": result}}
        if status is not None:
            document["status"] = status
        return json.dumps(document)

    SERIES = {"metric": {"a": "b"}, "value": [1.0, "1"]}
    STREAM = {"stream": {"a": "b"}, "values": [["1", "line"]]}

    def test_a_prometheus_answer_in_the_exact_form_is_read(self) -> None:
        self.assertEqual(self.stack(200, self.instant([self.SERIES])).prometheus_read("x"), [self.SERIES])
        self.assertEqual(self.stack(200, self.instant([])).prometheus_read("x"), [])

    def test_a_prometheus_answer_in_another_form_is_unread(self) -> None:
        for label, text in {
            "an error status": self.instant([], "error"),
            "no status": self.instant([], None),
            "a status that is not text": self.instant([], True),
            "a result that is a mapping": self.instant({}),
            "a result that is a text": self.instant("[]"),
            "a result that is null": self.instant(None),
            "a result with a number": self.instant([1]),
            "a series that is a list": self.instant([[1, 2]]),
            "a series without a metric": self.instant([{"value": [1.0, "1"]}]),
            "a series without a value": self.instant([{"metric": {}}]),
            "a value that is not a pair": self.instant([{"metric": {}, "value": [1.0]}]),
            "a value that is a text": self.instant([{"metric": {}, "value": "1"}]),
            "a sample that is not text": self.instant([{"metric": {}, "value": [1.0, 1]}]),
            "a metric that is a list": self.instant([{"metric": [], "value": [1.0, "1"]}]),
            "data that is a list": json.dumps({"status": "success", "data": []}),
            "no data": json.dumps({"status": "success"}),
            "a document that is a list": "[]",
            "a document that is null": "null",
            "no document": "",
        }.items():
            with self.subTest(case=label):
                self.assertIsInstance(self.stack(200, text).prometheus_read("x"), Unread)
                self.assertFalse(self.stack(200, text).no_series("x"))
                self.assertEqual(self.stack(200, text).prometheus("x"), [])

    def test_a_prometheus_range_answer_needs_the_same_exact_form(self) -> None:
        series = {"metric": {}, "values": [[1.0, "1"], [2.0, "1"]]}
        self.assertEqual(self.stack(200, self.instant([series])).prometheus_range("x", 1.0, 2.0), [series])
        for label, text in {
            "an error status": self.instant([series], "error"),
            "a result that is a mapping": self.instant({"metric": {}, "values": []}),
            "a series without values": self.instant([{"metric": {}}]),
            "values that are a text": self.instant([{"metric": {}, "values": "ab"}]),
            "a pair of the wrong length": self.instant([{"metric": {}, "values": [[1.0]]}]),
        }.items():
            with self.subTest(case=label):
                self.assertEqual(self.stack(200, text).prometheus_range("x", 1.0, 2.0), [])
                self.assertIsInstance(self.stack(200, text).prometheus_range_read("x", 1.0, 2.0), Unread)

    def test_a_loki_answer_in_the_exact_form_is_read(self) -> None:
        self.assertEqual(self.stack(200, self.streams([self.STREAM])).loki_read('{a="b"}', 1, 2), [(1, {"a": "b"}, "line")])
        self.assertEqual(self.stack(200, self.streams([])).loki_read('{a="b"}', 1, 2), [])

    def test_a_loki_answer_in_another_form_is_unread(self) -> None:
        for label, text in {
            "an error status": self.streams([], "error"),
            "no status": self.streams([], None),
            "a result that is a mapping": self.streams({}),
            "a result that is a text": self.streams("[]"),
            "a result that is null": self.streams(None),
            "a stream that is a list": self.streams([[1]]),
            "a stream without labels": self.streams([{"values": [["1", "line"]]}]),
            "a stream without values": self.streams([{"stream": {}}]),
            "values that are a mapping": self.streams([{"stream": {}, "values": {}}]),
            "a value that is a text": self.streams([{"stream": {}, "values": ["ab"]}]),
            "a value of three items": self.streams([{"stream": {}, "values": [["1", "line", "x"]]}]),
            "a stamp that is not digits": self.streams([{"stream": {}, "values": [["now", "line"]]}]),
            "a line that is not text": self.streams([{"stream": {}, "values": [["1", 2]]}]),
            "labels that are a list": self.streams([{"stream": [], "values": [["1", "line"]]}]),
            "a document that is null": "null",
        }.items():
            with self.subTest(case=label):
                self.assertIsInstance(self.stack(200, text).loki_read('{a="b"}', 1, 2), Unread)
                self.assertFalse(self.stack(200, text).no_lines('{a="b"}', 1, 2))
                self.assertEqual(self.stack(200, text).loki_lines('{a="b"}', 1, 2), [])


class Clock:
    """A clock that moves one second at each reading and never sleeps, so a bounded wait ends at once."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        self.now += 1.0
        return self.now

    def sleep(self, seconds: float) -> None:
        return None


class MissingMetric(unittest.TestCase):
    """The case of a metric that must stay missing until its deadline fails when any read of the wait is unread."""

    GOOD_EMPTY = (200, json.dumps({"status": "success", "data": {"resultType": "vector", "result": []}}))
    UNREAD = (200, json.dumps({"status": "error", "data": {"resultType": "vector", "result": []}}))
    PRESENT = (200, json.dumps({"status": "success", "data": {"resultType": "vector", "result": [{"metric": {}, "value": [1.0, "1"]}]}}))

    def case(self, *answers: tuple[int, str]) -> tuple[Record, list[str]]:
        module = runner("stack_monitoring")
        queue: Iterator[tuple[int, str]] = iter(answers)
        last = {"answer": answers[-1]}
        paths: list[str] = []

        def http(port: int, method: str, path: str, **keywords: object) -> tuple[int, str]:
            paths.append(path)
            last["answer"] = next(queue, last["answer"])
            return last["answer"]

        target = SimpleNamespace(http=http, ports={"prometheus": 9090, "loki": 3100}, project="p", compose_directory="/c", secrets_directory="/s", env_files=())
        run = record()
        stack = module.Stack(target, run, [])
        clock = Clock()
        with (
            mock.patch.object(module, "SHORT_DEADLINE_SECONDS", 4),
            mock.patch.object(module, "time", SimpleNamespace(monotonic=clock.monotonic, sleep=clock.sleep)),
        ):
            module.missing_metric_case(stack)
        return run, paths

    NAME = "metric of a service that is not running"

    def test_every_read_in_the_exact_form_and_empty_to_the_deadline_passes(self) -> None:
        run, paths = self.case(self.GOOD_EMPTY)
        self.assertEqual(failed(run), [])
        self.assertGreaterEqual(len(paths), 3)

    def test_one_unread_poll_in_the_middle_of_the_wait_fails_the_case(self) -> None:
        run, paths = self.case(self.GOOD_EMPTY, self.GOOD_EMPTY, self.UNREAD, self.GOOD_EMPTY)
        self.assertEqual(failed(run), [self.NAME])
        self.assertIn("unread polls: 1", only(run, self.NAME).observed)

    def test_an_unread_first_poll_or_last_poll_fails_the_case(self) -> None:
        self.assertEqual(failed(self.case(self.GOOD_EMPTY, self.UNREAD, self.GOOD_EMPTY)[0]), [self.NAME])
        run, _ = self.case(self.GOOD_EMPTY, self.GOOD_EMPTY, self.GOOD_EMPTY, self.GOOD_EMPTY, self.UNREAD)
        self.assertEqual(failed(run), [self.NAME])

    def test_an_unread_read_before_the_wait_fails_the_case(self) -> None:
        run, _ = self.case(self.UNREAD, self.GOOD_EMPTY)
        self.assertEqual(failed(run), [self.NAME])

    def test_a_poll_that_answers_with_no_http_status_fails_the_case(self) -> None:
        self.assertEqual(failed(self.case(self.GOOD_EMPTY, self.GOOD_EMPTY, (0, "no answer"), self.GOOD_EMPTY)[0]), [self.NAME])

    def test_a_series_that_appears_during_the_wait_fails_the_case(self) -> None:
        run, _ = self.case(self.GOOD_EMPTY, self.GOOD_EMPTY, self.PRESENT)
        self.assertEqual(failed(run), [self.NAME])


class StartTimes(unittest.TestCase):
    """A start time is an RFC 3339 timestamp with every fractional digit, and the monitoring snapshot is a 64 digit container ID and a timestamp."""

    def second_start(self, name: str, first: tuple[int, str], second: tuple[int, str]) -> Record:
        module = runner(name)
        run = record()
        target = Noisy(inspect_answers(shaped(name, first), shaped(name, second)))
        stack = module.Stack(target, run, [], ["docker", "compose"]) if name == "stack_caddy" else module.Stack(target, run, [])
        if name == "stack_caddy":
            module.second_start_case(stack, stack.container())
        elif name == "stack_nessie":
            module.idempotence(stack)
        else:
            module.idempotence_case(stack)
        return run

    def case_name(self, name: str) -> str:
        return "second start" if name == "stack_caddy" else "second start of the stack"

    def test_the_three_runners_with_a_start_time_are_all_covered(self) -> None:
        self.assertEqual(STARTED, ("stack_caddy", "stack_nessie", "stack_monitoring"))

    def test_a_start_time_with_nanoseconds_that_is_the_same_passes(self) -> None:
        for name in STARTED:
            with self.subTest(runner=name):
                self.assertEqual(failed(self.second_start(name, (0, NANOS + "\n"), (0, NANOS + "\n"))), [])

    def test_a_start_time_that_differs_in_the_last_fractional_digit_fails(self) -> None:
        for name in STARTED:
            with self.subTest(runner=name):
                self.assertEqual(failed(self.second_start(name, (0, NANOS + "\n"), (0, NANOS_NEXT + "\n"))), [self.case_name(name)])

    def test_a_text_that_is_not_a_timestamp_is_unread_on_either_side(self) -> None:
        for name in STARTED:
            for text in (
                "started",
                "2026-11-25",
                "2026-11-25T00:00:00",
                "2026-13-45T00:00:00Z",
                "0001-01-01T00:00:00Z",
                "Warning: Permanently added to the list",
            ):
                with self.subTest(runner=name, text=text):
                    self.assertEqual(failed(self.second_start(name, (0, text + "\n"), (0, text + "\n"))), [self.case_name(name)])
                    self.assertEqual(failed(self.second_start(name, (0, text + "\n"), GOOD)), [self.case_name(name)])
                    self.assertEqual(failed(self.second_start(name, GOOD, (0, text + "\n"))), [self.case_name(name)])

    def test_the_failed_reads_of_the_earlier_rounds_stay_unread(self) -> None:
        for name in STARTED:
            for label, answer in UNREAD_CASES.items():
                with self.subTest(runner=name, answer=label):
                    self.assertEqual(failed(self.second_start(name, answer, answer)), [self.case_name(name)])
                    self.assertEqual(failed(self.second_start(name, answer, OTHER)), [self.case_name(name)])
        self.assertEqual(failed(self.second_start("stack_caddy", GOOD, GOOD)), [])

    def test_the_monitoring_snapshot_needs_a_full_container_id(self) -> None:
        for label, text in {
            "a short ID": f"abc123 {NANOS}",
            "an ID of 63 digits": f"{ID[:63]} {NANOS}",
            "an ID of 65 digits": f"{ID}0 {NANOS}",
            "an upper case ID": f"{ID.upper()} {NANOS}",
            "an ID that is not hexadecimal": f"{'z' * 64} {NANOS}",
            "no ID": NANOS,
            "an ID only": ID,
            "a third word": f"{ID} {NANOS} extra",
            "a warning in front": f"Warning: Permanently added {ID} {NANOS}",
        }.items():
            with self.subTest(case=label):
                run = self.monitoring_second_start((0, text + "\n"), (0, text + "\n"))
                self.assertEqual(failed(run), ["second start of the stack"])

    def monitoring_second_start(self, first: tuple[int, str], second: tuple[int, str]) -> Record:
        module = runner("stack_monitoring")
        run = record()
        module.idempotence_case(module.Stack(Noisy(inspect_answers(first, second)), run, []))
        return run

    def test_the_monitoring_snapshot_that_is_the_same_passes_and_one_with_another_id_or_start_fails(self) -> None:
        same = (0, f"{ID} {NANOS}\n")
        self.assertEqual(failed(self.monitoring_second_start(same, same)), [])
        self.assertEqual(failed(self.monitoring_second_start(same, (0, f"{OTHER_ID} {NANOS}\n"))), ["second start of the stack"])
        self.assertEqual(failed(self.monitoring_second_start(same, (0, f"{ID} {NANOS_NEXT}\n"))), ["second start of the stack"])


def always(state: str | Unread) -> Callable[[str, int, float], str | Unread]:
    """A ``tcp_state`` that gives ``state`` for every address and port."""
    return lambda address, port, timeout: state


def unread_first(error: str) -> Callable[[str, int, float], str | Unread]:
    """A ``tcp_state`` that is unread for the first of ``ADDRESSES`` and refused for the others."""
    return lambda address, port, timeout: Unread(error) if address == ADDRESSES[0] else REFUSED


class CaddyProbes(unittest.TestCase):
    """The public address and the outsider cases decide from the TCP connect alone and name the state each port gave."""

    def routes(self, module: ModuleType) -> list[Any]:
        return list(module.load_routes())

    def public(self, probe: Callable[[str, int, float], str | Unread], control: Callable[[str, int, float], str | Unread] | None = None) -> Record:
        module = runner("stack_caddy")
        run = record()

        def both(address: str, port: int, timeout: float) -> str | Unread:
            return (control or probe)(address, port, timeout) if address == "100.64.0.1" else probe(address, port, timeout)

        with mock.patch.object(module, "tcp_state", both):
            module.public_address_cases(run, "192.0.2.7", "100.64.0.1", self.routes(module))
        return run

    def test_a_public_address_that_refuses_or_drops_every_port_and_a_tailscale_address_that_is_open_pass(self) -> None:
        for state in (REFUSED, DROPPED):
            with self.subTest(state=state):
                run = self.public(always(state), always(OPEN))
                self.assertEqual(failed(run), [])
                self.assertTrue(all(state in item.observed for item in run.observations if item.case.startswith("public address, ")))

    def test_a_port_that_is_open_on_the_public_address_fails_whatever_tls_does_after(self) -> None:
        run = self.public(lambda a, p, t: OPEN if p == 443 else REFUSED, lambda a, p, t: OPEN)
        self.assertEqual(failed(run), ["public address, https on port 443"])
        self.assertIn(OPEN, only(run, "public address, https on port 443").observed)

    def test_a_port_that_is_unread_on_the_public_address_fails(self) -> None:
        run = self.public(lambda a, p, t: Unread("ENETUNREACH") if p == 80 else REFUSED, lambda a, p, t: OPEN)
        self.assertEqual(failed(run), ["public address, http on port 80"])
        self.assertIn("unread", only(run, "public address, http on port 80").observed)

    def test_the_control_fails_when_the_tailscale_address_is_not_open_or_not_read(self) -> None:
        for state in (REFUSED, DROPPED, Unread("EHOSTUNREACH")):
            with self.subTest(state=str(state)):
                run = self.public(always(REFUSED), always(state))
                self.assertEqual(failed(run), ["public address control"])

    def test_the_control_is_read_once_and_names_its_state(self) -> None:
        reads: list[str] = []
        module = runner("stack_caddy")

        def probe(address: str, port: int, timeout: float) -> str | Unread:
            reads.append(address)
            return OPEN if address == "100.64.0.1" else REFUSED

        with mock.patch.object(module, "tcp_state", probe):
            module.public_address_cases(record(), "192.0.2.7", "100.64.0.1", self.routes(module))
        self.assertEqual(reads.count("100.64.0.1"), 1)

    def outsider(self, probe: Callable[[str, int, float], str | Unread], answer: object = "no answer (SSLError)") -> Record:
        module = runner("stack_caddy")
        run = record()
        with mock.patch.object(module, "tcp_state", probe), mock.patch.object(module, "try_get", return_value=answer):
            module.reach_checks(run, "n", "100.64.0.1", self.routes(module), set(), expect="refused", verify=False, where="outside")
        return run

    def test_an_outsider_that_is_refused_or_dropped_on_every_port_passes(self) -> None:
        for state in (REFUSED, DROPPED):
            with self.subTest(state=state):
                run = self.outsider(always(state))
                self.assertEqual(failed(run), [])
                self.assertTrue(all(state in item.observed for item in run.observations))

    def test_an_outsider_whose_connect_succeeds_fails_although_tls_fails_after_it(self) -> None:
        run = self.outsider(lambda a, p, t: OPEN if p == 9440 else DROPPED)
        self.assertEqual(len(failed(run)), 1)
        self.assertIn(OPEN, only(run, failed(run)[0]).observed)

    def test_an_outsider_whose_connect_is_unread_fails_although_the_tls_attempt_failed(self) -> None:
        run = self.outsider(lambda a, p, t: Unread("ENETUNREACH"))
        self.assertEqual(len(failed(run)), len(run.observations))
        self.assertTrue(all("unread" in item.observed for item in run.observations))

    def test_the_refusal_case_makes_no_tls_request(self) -> None:
        module = runner("stack_caddy")
        refuse = mock.Mock(side_effect=AssertionError("a TLS request was made"))
        with mock.patch.object(module, "tcp_state", lambda a, p, t: REFUSED), mock.patch.object(module, "try_get", refuse):
            module.reach_checks(record(), "n", "100.64.0.1", self.routes(module), set(), expect="refused", verify=False, where="outside")
        refuse.assert_not_called()


ADDRESSES = ["10.0.0.5", "192.168.1.9"]


class OtherAddresses(unittest.TestCase):
    """No other address of this computer may accept a connection on the service's port: each is read from the TCP connect alone."""

    def nessie(self, probe: Callable[[str, int, float], str | Unread], addresses: list[str]) -> Record:
        module = runner("stack_nessie")
        run = record()
        with mock.patch.object(module, "local_addresses", return_value=addresses), mock.patch.object(module, "tcp_state", probe):
            module.other_address_case(run, 19120)
        return run

    def openbao(self, probe: Callable[[str, int, float], str | Unread], addresses: list[str]) -> Record:
        module = runner("stack_openbao")
        run = record()
        stack = SimpleNamespace(record=run, seal_state=lambda: "sealed=false")
        with mock.patch.object(module, "local_addresses", return_value=addresses), mock.patch.object(module, "tcp_state", probe):
            module.other_addresses(stack, 8200)
        return run

    def catalog(self, probe: Callable[[str, int, float], str | Unread], addresses: list[str]) -> Record:
        module = runner("catalog_proof")
        run = record()

        def run_target(args: list[str], *, stdin: str | None = None, timeout: float = 0) -> tuple[int, str, str]:
            if "ps" in args and "compose" in args:
                return 0, "abc123\n", ""
            return 0, json.dumps({"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8080"}]}) + "\n", ""

        target = SimpleNamespace(compose_directory="/c", secrets_directory="/s", project="p", run=run_target)
        with mock.patch.object(module, "local_addresses", return_value=addresses), mock.patch.object(module, "tcp_state", probe):
            module.ports_case(module.Stack(target, run, []), run, 8080)
        return run

    CASES = (("nessie", "API on another address of the machine"), ("openbao", "port on another address of the machine"), ("catalog", "web interface address"))

    def test_the_three_runners_with_probes_of_other_addresses_are_all_covered(self) -> None:
        self.assertEqual([kind for kind, _ in self.CASES], ["nessie", "openbao", "catalog"])

    def of(self, kind: str, probe: Callable[[str, int, float], str | Unread], addresses: list[str]) -> Record:
        run: Record = getattr(self, kind)(probe, addresses)
        return run

    def test_addresses_that_refuse_or_drop_pass_and_the_observation_names_each_state(self) -> None:
        for kind, case in self.CASES:
            for state in (REFUSED, DROPPED):
                with self.subTest(runner=kind, state=state):
                    run = self.of(kind, always(state), ADDRESSES)
                    self.assertEqual(failed(run), [])
                    self.assertIn(f"{state}: {len(ADDRESSES)}", only(run, case).observed)

    def test_an_address_that_accepts_the_connection_fails_whatever_it_answers_after(self) -> None:
        for kind, case in self.CASES:
            with self.subTest(runner=kind):
                run = self.of(kind, lambda a, p, t: OPEN if a == ADDRESSES[1] else REFUSED, ADDRESSES)
                self.assertEqual(failed(run), [case])
                self.assertIn(f"{OPEN}: 1", only(run, case).observed)

    def test_an_address_that_is_unread_fails(self) -> None:
        for kind, case in self.CASES:
            for error in ("ENETUNREACH", "EHOSTUNREACH", "EPERM"):
                with self.subTest(runner=kind, error=error):
                    run = self.of(kind, unread_first(error), ADDRESSES)
                    self.assertEqual(failed(run), [case])
                    self.assertIn("unread: 1", only(run, case).observed)

    def test_no_address_to_try_fails(self) -> None:
        for kind, case in self.CASES:
            with self.subTest(runner=kind):
                self.assertEqual(failed(self.of(kind, lambda a, p, t: REFUSED, [])), [case])


if __name__ == "__main__":
    unittest.main()
