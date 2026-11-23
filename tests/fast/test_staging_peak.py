"""The staging sampler and the peak of its file: what the copy's pods held on the node's disk, as the kubelet reports it, and the lowest free space.

The sampler's kubectl call is a stand-in that returns the kubelet's summary document, so each case sets the pods and the node's disk. A reading that fails
is named in the line and the peak refuses such a file: a sample that is missing could have held the peak. Each refusal has a passing control.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from scripts.ingestion import staging_peak
from scripts.ingestion.notice import Code, Notice
from scripts.ingestion.staging_peak import StagingRefused, peak_record, run_sampler

NAMESPACE = "airbyte"
NODE = "host-node"


def summary(pods: dict[str, int], *, available: int = 90_000, capacity: int = 100_000, other: dict[str, int] | None = None) -> str:
    """The kubelet's summary document: the pods' ephemeral storage, those of another namespace too, and the node's file system."""
    rows = [{"podRef": {"name": name, "namespace": NAMESPACE, "uid": name}, "ephemeral-storage": {"usedBytes": used}} for name, used in pods.items()]
    rows += [
        {"podRef": {"name": name, "namespace": "kube-system", "uid": name}, "ephemeral-storage": {"usedBytes": used}} for name, used in (other or {}).items()
    ]
    return json.dumps(
        {"node": {"nodeName": NODE, "fs": {"availableBytes": available, "capacityBytes": capacity, "usedBytes": capacity - available}}, "pods": rows}
    )


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def pause(self, seconds: float) -> None:
        self.now += seconds


class FakeKubectl:
    """A stand-in for the launcher: it records each call and returns the next answer, the last one again when they run out."""

    def __init__(self, answers: Sequence[tuple[int, str]]) -> None:
        self.answers = list(answers)
        self.calls: list[tuple[str, list[str]]] = []
        self.timeouts: list[float] = []

    def __call__(self, program: str, arguments: list[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        self.calls.append((program, list(arguments)))
        self.timeouts.append(timeout)
        code, text = self.answers[min(len(self.calls) - 1, len(self.answers) - 1)]
        return subprocess.CompletedProcess([program, *arguments], code, text, "")


def sample_file(answers: Sequence[tuple[int, str]], *, max_seconds: float = 20.0, interval: float = 5.0) -> tuple[int, list[dict[str, Any]], FakeKubectl, Path]:
    folder = Path(tempfile.mkdtemp(prefix="staging_peak_test_"))
    output = folder / "samples.jsonl"
    clock = FakeClock()
    kubectl = FakeKubectl(answers)
    code = run_sampler(
        output,
        interval_seconds=interval,
        max_seconds=max_seconds,
        kubectl=("kubectl",),
        node=NODE,
        namespace=NAMESPACE,
        runner=kubectl,
        clock=clock,
        pause=clock.pause,
        say=lambda text: None,
    )
    lines = [json.loads(line) for line in output.read_text().splitlines()] if output.exists() else []
    return code, lines, kubectl, output


def lines_for(totals: Sequence[dict[str, int]], *, available: Sequence[int] | None = None) -> str:
    """A sample file as the sampler writes it, from the pods' bytes at each sample."""
    free = list(available or [90_000] * len(totals))
    rows = [
        {"kind": "header", "schema": staging_peak.SCHEMA, "started_utc": "2026-11-24T00:00:00Z", "interval_seconds": 5.0, "namespace": NAMESPACE, "node": NODE}
    ]
    for seq, pods in enumerate(totals):
        body = {"node_fs_available_bytes": free[seq], "node_fs_capacity_bytes": 100_000, "pods": pods}
        rows.append({"kind": "sample", "seq": seq, "summary": body, "missing": []})
    rows.append({"kind": "end", "reason": "stop_flag", "samples": len(totals)})
    return "".join(json.dumps(row) + "\n" for row in rows)


def codes(notices: Sequence[Notice]) -> list[Code]:
    return [notice.code for notice in notices]


class SamplerTest(unittest.TestCase):
    def test_control_each_sample_holds_the_pods_of_the_namespace_and_the_node_disk_and_the_file_ends_with_its_end_line(self) -> None:
        answer = summary({"replication-job-1": 5_000, "airbyte-server": 100}, available=88_000, other={"coredns": 777})
        code, lines, kubectl, output = sample_file([(0, answer)])
        self.assertEqual(0, code)
        self.assertEqual(["header", "sample", "sample", "sample", "sample", "end"], [line["kind"] for line in lines])
        self.assertEqual({"replication-job-1": 5_000, "airbyte-server": 100}, lines[1]["summary"]["pods"])
        self.assertEqual((88_000, 100_000), (lines[1]["summary"]["node_fs_available_bytes"], lines[1]["summary"]["node_fs_capacity_bytes"]))
        self.assertEqual({"kind": "end", "reason": "max_seconds", "samples": 4}, lines[-1])
        self.assertEqual(("kubectl", ["get", "--raw", f"/api/v1/nodes/{NODE}/proxy/stats/summary"]), kubectl.calls[0])
        self.assertEqual(0o600, stat.S_IMODE(os.stat(output).st_mode))

    def test_a_failed_call_is_named_in_its_line_and_holds_no_reading(self) -> None:
        code, lines, _, _ = sample_file([(1, ""), (0, summary({"p": 1}))], max_seconds=10.0)
        self.assertEqual(0, code)
        self.assertEqual([{"reading": "summary", "cause": "exit_status"}], lines[1]["missing"])
        self.assertNotIn("summary", lines[1])
        self.assertEqual([], lines[2]["missing"])

    def test_a_pod_with_no_storage_statistics_names_the_reading_failed_and_is_not_counted_as_zero(self) -> None:
        bare = json.loads(summary({"job": 5_000}))
        bare["pods"].append({"podRef": {"name": "starting", "namespace": NAMESPACE, "uid": "s"}})
        empty = json.loads(summary({"job": 5_000}))
        empty["pods"].append({"podRef": {"name": "starting", "namespace": NAMESPACE, "uid": "s"}, "ephemeral-storage": {"time": "now"}})
        for answer in (bare, empty):
            _, lines, _, _ = sample_file([(0, json.dumps(answer))], max_seconds=5.0)
            self.assertEqual([{"reading": "summary", "cause": "unparsable"}], lines[1]["missing"])
            self.assertNotIn("summary", lines[1])

    def test_a_pod_of_another_namespace_with_no_statistics_is_not_a_failed_reading(self) -> None:
        answer = json.loads(summary({"job": 5_000}))
        answer["pods"].append({"podRef": {"name": "elsewhere", "namespace": "kube-system", "uid": "e"}})
        _, lines, _, _ = sample_file([(0, json.dumps(answer))], max_seconds=5.0)
        self.assertEqual([], lines[1]["missing"])

    def test_no_reading_is_given_more_time_than_the_run_has_left(self) -> None:
        _, _, kubectl, _ = sample_file([(0, summary({"p": 1}))], max_seconds=7.0, interval=5.0)
        self.assertEqual([5.0, 2.0], kubectl.timeouts)

    def test_an_answer_that_is_not_the_summary_form_is_named_unparsable(self) -> None:
        for text in (
            "not json",
            "{}",
            json.dumps({"node": {"fs": {}}, "pods": []}),
            json.dumps({"node": {"fs": {"availableBytes": "1", "capacityBytes": 2}}, "pods": []}),
        ):
            with self.subTest(text=text):
                _, lines, _, _ = sample_file([(0, text)], max_seconds=5.0)
                self.assertEqual([{"reading": "summary", "cause": "unparsable"}], lines[1]["missing"])

    def test_an_output_that_exists_is_refused_and_left_as_it_was(self) -> None:
        _, _, _, output = sample_file([(0, summary({}))])
        before = output.read_text()
        clock = FakeClock()
        kubectl = FakeKubectl([(0, summary({}))])
        code = run_sampler(
            output,
            interval_seconds=5.0,
            max_seconds=5.0,
            kubectl=("kubectl",),
            node=NODE,
            namespace=NAMESPACE,
            runner=kubectl,
            clock=clock,
            pause=clock.pause,
            say=lambda text: None,
        )
        self.assertEqual(1, code)
        self.assertEqual(before, output.read_text())
        self.assertEqual([], kubectl.calls)

    def test_a_node_name_that_is_not_a_node_name_is_refused_before_any_call(self) -> None:
        clock = FakeClock()
        kubectl = FakeKubectl([(0, summary({}))])
        output = Path(tempfile.mkdtemp(prefix="staging_peak_test_")) / "samples.jsonl"
        code = run_sampler(
            output,
            interval_seconds=5.0,
            max_seconds=5.0,
            kubectl=("kubectl",),
            node="../x",
            namespace=NAMESPACE,
            runner=kubectl,
            clock=clock,
            pause=clock.pause,
            say=lambda text: None,
        )
        self.assertEqual(1, code)
        self.assertEqual([], kubectl.calls)
        self.assertFalse(output.exists())


class PeakTest(unittest.TestCase):
    def test_control_the_peak_is_the_most_the_pods_held_together_and_the_lowest_free_space(self) -> None:
        text = lines_for(
            [{"a": 100, "b": 50}, {"a": 100, "b": 4_000}, {"a": 100, "b": 9_000}, {"a": 100, "b": 200}],
            available=[90_000, 80_000, 60_000, 85_000],
        )
        record = peak_record(text)
        self.assertEqual("staging_peak_1", record["format"])
        self.assertEqual(9_100, record["peak_pod_bytes_total"])
        self.assertEqual(2, record["peak_at_seq"])
        self.assertEqual(("b", 9_000), (record["peak_pod"], record["peak_pod_bytes"]))
        self.assertEqual(150, record["first_sample_pod_bytes_total"])
        self.assertEqual(8_950, record["peak_increase_bytes"])
        self.assertEqual(60_000, record["min_node_free_bytes"])
        self.assertEqual(100_000, record["node_capacity_bytes"])
        self.assertEqual(4, record["samples"])

    def test_a_pod_that_is_gone_at_a_sample_counts_as_nothing_there(self) -> None:
        record = peak_record(lines_for([{"a": 10}, {"b": 30}, {}]))
        self.assertEqual(30, record["peak_pod_bytes_total"])

    def test_a_file_with_no_end_line_was_cut_off_and_is_refused_for_that_cause(self) -> None:
        text = "".join(lines_for([{"a": 1}, {"a": 2}]).splitlines(keepends=True)[:-1])
        with self.assertRaises(StagingRefused) as refused:
            peak_record(text)
        self.assertEqual([Code.STAGING_CUT_OFF], codes(refused.exception.notices))

    def test_an_end_line_that_counts_other_samples_than_the_file_holds_is_refused(self) -> None:
        rows = lines_for([{"a": 1}, {"a": 2}]).replace('"samples": 2', '"samples": 3')
        with self.assertRaises(StagingRefused) as refused:
            peak_record(rows)
        self.assertEqual([Code.STAGING_CUT_OFF], codes(refused.exception.notices))

    def test_an_end_record_must_be_an_end_with_a_whole_number_that_equals_the_samples_read(self) -> None:
        base = lines_for([{"a": 1}, {"a": 2}]).splitlines()
        counted_sample = json.dumps(json.loads(base[2]) | {"samples": 1})
        one = lines_for([{"a": 1}]).splitlines()
        cases = (
            ("a boolean", [*one[:-1], json.dumps({"kind": "end", "reason": "stop_flag", "samples": True})]),
            ("a float", [*one[:-1], json.dumps({"kind": "end", "reason": "stop_flag", "samples": 1.0})]),
            ("a string", [*one[:-1], json.dumps({"kind": "end", "reason": "stop_flag", "samples": "1"})]),
            ("no count", [*one[:-1], json.dumps({"kind": "end", "reason": "stop_flag"})]),
            ("a last sample line that holds the count of the one before", [*base[:2], counted_sample]),
        )
        for label, lines in cases:
            with self.subTest(label), self.assertRaises(StagingRefused) as refused:
                peak_record("\n".join(lines) + "\n")
            self.assertEqual([Code.STAGING_CUT_OFF], codes(refused.exception.notices))

    def test_a_file_with_no_sample_is_refused_for_that_cause(self) -> None:
        with self.assertRaises(StagingRefused) as refused:
            peak_record(lines_for([]))
        self.assertEqual([Code.STAGING_NO_SAMPLES], codes(refused.exception.notices))

    def test_a_sample_with_a_failed_reading_is_refused_because_it_could_have_held_the_peak(self) -> None:
        rows = [json.loads(line) for line in lines_for([{"a": 1}, {"a": 2}, {"a": 3}]).splitlines()]
        rows[2] = {"kind": "sample", "seq": 1, "missing": [{"reading": "summary", "cause": "timeout"}]}
        with self.assertRaises(StagingRefused) as refused:
            peak_record("".join(json.dumps(row) + "\n" for row in rows))
        self.assertEqual([Code.STAGING_MISSING], codes(refused.exception.notices))
        self.assertEqual(1, refused.exception.notices[0].count)

    def test_a_line_that_is_not_in_the_form_is_named_by_its_number(self) -> None:
        lines = lines_for([{"a": 1}]).splitlines()
        body = {"node_fs_available_bytes": 9, "node_fs_capacity_bytes": 10}
        bodies: list[dict[str, Any]] = [{**body, "pods": {"a": "1"}}, {**body, "pods": {"a": -1}}, {"pods": {}}]
        for summary_body in bodies:
            lines[1] = json.dumps({"kind": "sample", "seq": 0, "summary": summary_body, "missing": []})
            with self.subTest(summary=summary_body), self.assertRaises(StagingRefused) as refused:
                peak_record("\n".join(lines) + "\n")
            self.assertEqual([Code.STAGING_FORM], codes(refused.exception.notices))
            self.assertEqual(2, refused.exception.notices[0].position)

    def test_a_sample_out_of_its_place_in_the_sequence_is_named_by_its_number(self) -> None:
        lines = lines_for([{"a": 1}, {"a": 2}, {"a": 3}]).replace('"seq": 1', '"seq": 5').splitlines()
        with self.assertRaises(StagingRefused) as refused:
            peak_record("\n".join(lines) + "\n")
        self.assertEqual([Code.STAGING_FORM], codes(refused.exception.notices))
        self.assertEqual(3, refused.exception.notices[0].position)

    def test_a_file_that_does_not_start_with_its_header_is_refused(self) -> None:
        lines = lines_for([{"a": 1}]).splitlines()
        with self.assertRaises(StagingRefused) as refused:
            peak_record("\n".join(lines[1:]) + "\n")
        self.assertEqual([Code.STAGING_FORM], codes(refused.exception.notices))

    def test_the_sampler_file_and_the_peak_agree_end_to_end(self) -> None:
        answers = [(0, summary({"job": size}, available=90_000 - size)) for size in (100, 7_000, 300)]
        _, _, _, output = sample_file(answers, max_seconds=15.0)
        record = peak_record(output.read_text())
        self.assertEqual((7_000, 83_000, 3), (record["peak_pod_bytes_total"], record["min_node_free_bytes"], record["samples"]))


if __name__ == "__main__":
    unittest.main()
