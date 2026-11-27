"""The wiring of one call of the Silver cases job: a verify reads at one hash and ends when the branch moved.

The Spark session and Nessie are stand-ins, so these tests prove the calls and not Spark: they hold the two guards that no other fast test reaches. Removing
the pin of the session, or the check that the branch still has the hash, would leave a verify that reads a moving branch and reports a hash it did not read.

Failure modes covered here:

- A verify whose Spark session is not pinned to the hash that the job resolved first.
- A verify that reports success although the branch moved while it ran.
- A build that is pinned, although it writes on a branch that only it moves.
"""

from __future__ import annotations

import unittest
from argparse import Namespace
from dataclasses import dataclass
from typing import Any
from unittest import mock

from spark.jobs import silver_cases
from spark.silver_cases import SilverCasesError
from spark.split_table import BRANCH_MOVED

FIRST, SECOND = "a" * 64, "b" * 64


@dataclass
class Reference:
    hash: str


class Client:
    """A Nessie stand-in whose branch answers each ask for its head with the next hash of ``heads``, the last one repeating."""

    def __init__(self, heads: list[str]) -> None:
        self.heads = list(heads)
        self.asked = 0

    def config(self) -> dict[str, Any]:
        return {}

    def reference(self, name: str) -> Reference:
        head = self.heads[min(self.asked, len(self.heads) - 1)]
        self.asked += 1
        return Reference(head)

    def create_branch(self, name: str, base: str) -> Reference:
        return Reference(FIRST)


class Session:
    def __init__(self) -> None:
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True


class Outcome:
    def facts(self) -> dict[str, Any]:
        return {"passed": {"cases": True}}


def call(mode: str, client: Client, sessions: list[Session]) -> tuple[dict[str, Any], list[tuple[Any, ...]]]:
    calls: list[tuple[Any, ...]] = []

    def build_session(name: str, ref: str, ref_hash: str | None = None) -> Session:
        calls.append((name, ref, ref_hash))
        sessions.append(Session())
        return sessions[-1]

    settings = mock.Mock(nessie_uri="http://nessie/api/v2", nessie_timeout_seconds=1.0, deadline_seconds=60.0)
    with (
        mock.patch.multiple(
            silver_cases,
            read_settings=mock.Mock(return_value=settings),
            open_client=mock.Mock(return_value=client),
            start_deadline=mock.Mock(),
            build_session=build_session,
            run_verify=mock.Mock(return_value=Outcome()),
            run_build=mock.Mock(return_value=Outcome()),
        ),
        mock.patch.object(silver_cases.Lake, "check_sources"),
    ):
        arguments = Namespace(mode=mode, branch="run_b", base="main", pinned_release=False)
        return silver_cases.execute(arguments), calls


class PinnedVerifyTest(unittest.TestCase):
    def test_a_verify_resolves_the_hash_first_and_pins_its_session_and_its_result_to_it(self) -> None:
        sessions: list[Session] = []
        result, calls = call("verify", Client([FIRST]), sessions)
        self.assertEqual([("silver_cases", "run_b", FIRST)], calls)
        self.assertEqual(FIRST, result["branch_hash"])
        self.assertTrue(sessions[0].stopped)

    def test_a_verify_that_finds_the_branch_moved_fails_with_the_moved_code_and_still_stops_its_session(self) -> None:
        sessions: list[Session] = []
        with self.assertRaises(SilverCasesError) as raised:
            call("verify", Client([FIRST, SECOND]), sessions)
        self.assertEqual([BRANCH_MOVED], raised.exception.problems)
        self.assertTrue(sessions[0].stopped)


class BuildTest(unittest.TestCase):
    def test_a_build_is_not_pinned_and_reports_the_hash_that_the_branch_has_at_the_end(self) -> None:
        result, calls = call("build", Client([SECOND]), [])
        self.assertEqual([("silver_cases", "run_b", None)], calls)
        self.assertEqual((FIRST, SECOND), (result["base_hash"], result["branch_hash"]))


if __name__ == "__main__":
    unittest.main()
