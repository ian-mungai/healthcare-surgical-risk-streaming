"""The scan gate reads Docker Scout's SARIF report of the fixable findings and the committed list of accepted findings, and fails closed on anything else.

The reports here are fixtures in the shape that Docker Scout writes (read from real reports on Nov 25 2026). No test runs Docker or reaches a network.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from scripts.images import pins, publish, scan
from scripts.images.scan import Accepted, Finding, ScanError
from scripts.stack.yaml_text import load_yaml

NETTY = "pkg:maven/io.netty/netty-handler@4.2.7"
JAR = "/opt/spark/jars/holder-1.0.jar"
OTHER_JAR = "/opt/spark/jars/other-2.0.jar"


def location(uri: str) -> dict[str, Any]:
    return {"physicalLocation": {"artifactLocation": {"uri": uri}}}


def result(cve: str = "CVE-2026-1111", purl: str = NETTY, uris: tuple[str, ...] = (JAR, JAR + ":io.netty:netty-handler")) -> dict[str, Any]:
    text = (
        f"Vulnerability    :{cve}   \nSeverity         :HIGH   \nPackage          :{purl}   \nAffected range   :<9   \nFixed version    :9   \n"
        "EPSS Score       :0.001900   \n"
    )
    locations = [location(uri) for uri in uris]
    return {"ruleId": cve, "ruleIndex": 0, "kind": "fail", "level": "error", "message": {"text": text}, "locations": locations}


def report(*results: dict[str, Any]) -> dict[str, Any]:
    """A report as Docker Scout 1.28.0 writes it: the schema, the version and one run of a tool and its results, with no invocations."""
    driver = {
        "fullName": "Docker Scout",
        "informationUri": "https://docker.com/products/docker-scout",
        "name": "docker scout",
        "rules": [],
        "version": "1.28.0",
    }
    schema = "https://raw.githubusercontent.com/oasis-tcs/sarif-spec/main/sarif-2.1/schema/sarif-schema-2.1.0.json"
    return {"version": "2.1.0", "$schema": schema, "runs": [{"tool": {"driver": driver}, "results": list(results)}]}


def stated(document: dict[str, Any], invocations: object) -> str:
    """The text of a report whose one run holds these invocations."""
    document["runs"][0]["invocations"] = invocations
    return json.dumps(document)


def read(text: str) -> frozenset[Finding]:
    """The findings of a report that a scan with the exit status 0 wrote."""
    return scan.read_report(text, 0)


def accepted_text(*entries: str) -> str:
    return "findings:\n" + "".join(entries) if entries else "findings: []\n"


def entry(
    cve: str = "CVE-2026-1111",
    package: str = NETTY,
    reason: str = "No release holds the fix.",
    date: str = '"2026-11-25"',
    image: str = "spark",
    artifact: str = JAR,
) -> str:
    return f"  - image: {image}\n    artifact: {artifact}\n    cve: {cve}\n    package: {package}\n    reason: {reason}\n    date: {date}\n"


class ReportTest(unittest.TestCase):
    def test_each_result_is_one_finding_of_its_cve_and_its_package_without_qualifiers_and_with_percent_codes_read(self) -> None:
        debian = "pkg:deb/debian/zlib@1%3A1.3.dfsg%2Breally1.3.1-1?os_distro=trixie&os_name=debian&os_version=13"
        found = read(json.dumps(report(result(), result("GHSA-6v7p-g79w-8964", debian), result())))
        zlib = Finding("GHSA-6v7p-g79w-8964", "pkg:deb/debian/zlib@1:1.3.dfsg+really1.3.1-1", (JAR,))
        self.assertEqual({Finding("CVE-2026-1111", NETTY, (JAR,)), zlib}, found)

    def test_the_artifacts_are_each_location_once_and_a_library_inside_a_jar_is_located_at_the_jar(self) -> None:
        uris = (OTHER_JAR + ":io.netty:netty-handler", JAR, JAR + ":io.netty:netty-handler", "/var/lib/dpkg/info/libzstd1:amd64.md5sums", "/usr/bin/[")
        (found,) = read(json.dumps(report(result(uris=uris))))
        self.assertEqual((JAR, OTHER_JAR, "/usr/bin/[", "/var/lib/dpkg/info/libzstd1:amd64.md5sums"), found.artifacts)

    def test_a_result_that_does_not_say_where_the_package_is_refuses_the_report(self) -> None:
        cases: dict[str, Any] = {
            "none": None,
            "an empty list": [],
            "not a list": location(JAR),
            "a location that is text": ["x"],
            "no uri": [{"physicalLocation": {"artifactLocation": {}}}],
            "a uri that is no text": [{"physicalLocation": {"artifactLocation": {"uri": 7}}}],
            "an empty uri": [location("")],
            "a uri with a space": [location("/opt/a b.jar")],
            "a uri with a newline": [location(JAR + "\nforged")],
            "one good and one without a uri": [location(JAR), {}],
        }
        for name, locations in cases.items():
            changed = result()
            changed["locations"] = locations
            with self.subTest(case=name), self.assertRaisesRegex(ScanError, "location|in its form"):
                read(json.dumps(report(changed)))
        missing = result()
        del missing["locations"]
        with self.assertRaisesRegex(ScanError, "location"):
            read(json.dumps(report(missing)))

    def test_a_report_with_no_result_holds_no_finding(self) -> None:
        self.assertEqual(frozenset(), read(json.dumps(report())))

    def test_anything_that_is_not_a_complete_scout_report_is_refused_and_never_read_as_clean(self) -> None:
        no_package = result()
        no_package["message"]["text"] = "Vulnerability    :CVE-2026-1111\n"
        two_packages = result()
        two_packages["message"]["text"] += f"Package          :{NETTY}\n"
        other_tool = report()
        other_tool["runs"][0]["tool"]["driver"]["name"] = "another tool"
        no_results = report()
        del no_results["runs"][0]["results"]
        cases: dict[str, str] = {
            "empty": "",
            "truncated": json.dumps(report(result()))[:-9],
            "a list": "[]",
            "no runs": json.dumps({"version": "2.1.0", "runs": []}),
            "two runs": json.dumps({"version": "2.1.0", "runs": report()["runs"] * 2}),
            "another version": json.dumps({**report(), "version": "9"}),
            "another tool": json.dumps(other_tool),
            "no results": json.dumps(no_results),
            "results is a mapping": json.dumps({**report(), "runs": [{"tool": {"driver": {"name": "docker scout"}}, "results": {}}]}),
            "a result that is text": json.dumps({**report(), "runs": [{"tool": {"driver": {"name": "docker scout"}}, "results": ["x"]}]}),
            "no package line": json.dumps(report(no_package)),
            "two package lines": json.dumps(report(two_packages)),
            "a cve with a newline": json.dumps(report(result("CVE-1\nforged"))),
            "a package with a space": json.dumps(report(result(purl="pkg:maven/a/b@1 forged"))),
            "a package that is no package URL": json.dumps(report(result(purl="netty"))),
        }
        for name, text in cases.items():
            with self.subTest(report=name), self.assertRaises(ScanError):
                read(text)


class CompletionTest(unittest.TestCase):
    def refused(self, text: str, status: object = 0) -> None:
        with self.assertRaisesRegex(ScanError, "does not say that its execution succeeded|exit status"):
            scan.read_report(text, status)

    def test_a_real_shaped_report_without_invocations_passes_on_the_exit_status_0_and_is_refused_on_1(self) -> None:
        for results in ((), (result(),)):
            with self.subTest(results=len(results)):
                text = json.dumps(report(*results))
                self.assertNotIn("invocations", text)
                self.assertEqual(len(results), len(scan.read_report(text, 0)))
                self.refused(text, 1)

    def test_only_the_exit_status_0_counts_and_a_status_that_cannot_be_read_refuses(self) -> None:
        cases: dict[str, object] = {
            "another failure": 2,
            "killed by a signal": -9,
            "no status": None,
            "the text 0": "0",
            "the number 0.0": 0.0,
            "false": False,
        }
        good = [{"executionSuccessful": True}]
        for name, status in cases.items():
            for text in (json.dumps(report()), stated(report(), good)):
                with self.subTest(case=name, invocations="invocations" in text):
                    self.refused(text, status)

    def test_a_run_with_invocations_passes_only_when_every_one_says_that_its_execution_succeeded(self) -> None:
        good = {"executionSuccessful": True}
        cases: dict[str, Any] = {
            "no invocation in the list": [],
            "not a list": good,
            "null": None,
            "an invocation that is text": ["x"],
            "no statement": [{}],
            "failed": [{"executionSuccessful": False}],
            "the text true": [{"executionSuccessful": "true"}],
            "the number 1": [{"executionSuccessful": 1}],
            "the second one failed": [good, {"executionSuccessful": False}],
        }
        for name, invocations in cases.items():
            for results in ((), (result(),)):
                with self.subTest(case=name, results=len(results)):
                    self.refused(stated(report(*results), invocations))
        self.assertEqual(frozenset(), read(stated(report(), [good, good])))

    def test_a_constant_that_standard_json_does_not_have_refuses_the_report_wherever_it_stands(self) -> None:
        whole = json.dumps(report(result()))
        invoked = stated(report(), [{"executionSuccessful": True}])
        for constant in ("NaN", "Infinity", "-Infinity"):
            cases = {
                "a property of the report": whole.replace('"version": "2.1.0", ', f'"version": "2.1.0", "extra": {constant}, ', 1),
                "a number in a result": whole.replace('"ruleIndex": 0', f'"ruleIndex": {constant}'),
                "a value in a list": whole.replace('"rules": []', f'"rules": [{constant}]'),
                "a property of an invocation": invoked.replace('"executionSuccessful": true', f'"executionSuccessful": true, "exitCode": {constant}'),
            }
            for name, changed in cases.items():
                with self.subTest(case=name, constant=constant):
                    self.assertNotIn(changed, (whole, invoked), "the case changed the report")
                    with self.assertRaisesRegex(ScanError, "not standard JSON"):
                        read(changed)
        self.assertEqual(1, len(read(json.dumps(report(result(uris=("/opt/NaN/Infinity",)))))), "the same letters inside a text are no constant")

    def test_a_key_that_one_object_holds_twice_is_refused_because_the_last_value_would_hide_the_first(self) -> None:
        text = stated(report(), [{"executionSuccessful": True}])
        finding = json.dumps([result()])
        cases = {
            "two result lists, the last one empty": text.replace('"results": []', f'"results": {finding}, "results": []'),
            "two statements of success": text.replace('"executionSuccessful": true', '"executionSuccessful": false, "executionSuccessful": true'),
            "two run lists": text.replace('"runs": ', '"runs": [], "runs": '),
            "two rules in one result": json.dumps(report(result())).replace('"ruleId": ', '"ruleId": "CVE-2026-2222", "ruleId": '),
        }
        for name, changed in cases.items():
            with self.subTest(case=name):
                self.assertNotIn(changed, (text, json.dumps(report(result()))), "the case changed the report")
                with self.assertRaisesRegex(ScanError, "a key twice"):
                    read(changed)


class AcceptedListTest(unittest.TestCase):
    def load(self, text: str) -> frozenset[Accepted]:
        with tempfile.TemporaryDirectory(prefix="accepted_") as folder:
            path = Path(folder) / "accepted.yml"
            path.write_text(text)
            return scan.load_accepted(path)

    def test_an_empty_list_accepts_nothing_and_an_entry_accepts_its_image_artifact_cve_and_package(self) -> None:
        self.assertEqual(frozenset(), self.load(accepted_text()))
        self.assertEqual({Accepted("spark", JAR, "CVE-2026-1111", NETTY)}, self.load(accepted_text(entry())))
        two = self.load(accepted_text(entry(), entry(artifact=OTHER_JAR), entry(image="replay")))
        self.assertEqual(3, len(two), "the same vulnerability in another artifact or image is another entry")

    def test_the_gate_never_reads_the_pending_list_whatever_it_holds(self) -> None:
        pending = "pending:\n" + entry()
        self.assertEqual(frozenset(), self.load("findings: []\n" + pending))
        self.assertEqual(frozenset(), self.load("findings: []\npending: not a list\n"))
        listed = self.load(accepted_text(entry(artifact=OTHER_JAR)) + pending)
        self.assertEqual({Accepted("spark", OTHER_JAR, "CVE-2026-1111", NETTY)}, listed)
        with self.assertRaises(ScanError):
            self.load(pending)

    def test_a_missing_file_is_refused(self) -> None:
        with self.assertRaises(ScanError):
            scan.load_accepted(Path("/nonexistent/accepted.yml"))

    def test_an_entry_names_exactly_a_cve_a_package_with_its_version_a_reason_and_a_date(self) -> None:
        cases = {
            "no reason": accepted_text(entry().replace("    reason: No release holds the fix.\n", "")),
            "no image": accepted_text(entry().replace("image: spark\n    ", "")),
            "no artifact": accepted_text(entry().replace(f"    artifact: {JAR}\n", "")),
            "a wildcard image": accepted_text(entry(image='"*"')),
            "a wildcard artifact": accepted_text(entry(artifact='"/opt/spark/jars/*.jar"')),
            "an empty artifact": accepted_text(entry(artifact='""')),
            "an artifact that is no text": accepted_text(entry(artifact="7")),
            "an extra field": accepted_text(entry() + "    note: x\n"),
            "an empty reason": accepted_text(entry(reason='"  "')),
            "a package without a version": accepted_text(entry(package="pkg:maven/io.netty/netty-handler")),
            "a wildcard cve": accepted_text(entry(cve='"*"')),
            "a date in another form": accepted_text(entry(date='"25-11-2026"')),
            "a date that is no day": accepted_text(entry(date='"2026-02-30"')),
            "an unquoted date, which YAML reads as a date and not as text": accepted_text(entry(date="2026-11-25")),
            "the same finding twice": accepted_text(entry(), entry(reason="Again.")),
            "another top key": "findings: []\nall: true\n",
            "no list": "findings:\n",
            "a repeated key": "findings: []\nfindings: []\n",
            "not YAML": "findings: [\n",
        }
        for name, text in cases.items():
            with self.subTest(case=name), self.assertRaises(ScanError):
                self.load(text)

    def test_each_committed_entry_active_or_pending_is_a_valid_entry_for_a_library_that_a_jar_or_a_program_holds_inside_it(self) -> None:
        document: Any = load_yaml(scan.ACCEPTED_FILE.read_text())
        self.assertLessEqual(set(document), {"findings", "pending"})
        names = {image["name"] for image in pins.load_images()}
        pending = [scan.accepted_entry(value) for value in document["pending"]]
        active = scan.load_accepted(scan.ACCEPTED_FILE)
        self.assertEqual(len(pending), len(set(pending)), "no entry is pending twice")
        self.assertEqual(set(), set(pending) & active, "no entry is both pending and active")
        for accepted in [*pending, *active]:
            with self.subTest(entry=accepted):
                self.assertIn(accepted.image, names)
                self.assertRegex(accepted.package, r"^pkg:(maven|golang)/")
                self.assertNotRegex(accepted.artifact, r"\.jar:", "the artifact is the jar and not a library inside it")


class DecisionTest(unittest.TestCase):
    def test_only_a_finding_that_the_list_names_for_that_image_artifact_cve_and_package_version_passes(self) -> None:
        listed = frozenset({Accepted("spark", JAR, "CVE-2026-1111", NETTY)})
        found = Finding("CVE-2026-1111", NETTY, (JAR,))
        self.assertEqual([], scan.unaccepted("spark", frozenset({found}), listed))
        self.assertEqual([found], scan.unaccepted("replay", frozenset({found}), listed), "another image")
        others = [
            Finding("CVE-2026-2222", NETTY, (JAR,)),
            Finding("CVE-2026-1111", NETTY.replace("4.2.7", "4.2.8"), (JAR,)),
            Finding("CVE-2026-1111", NETTY, (OTHER_JAR,)),
        ]
        self.assertEqual(sorted(others), scan.unaccepted("spark", frozenset({*others, found}), listed))

    def test_a_finding_in_two_artifacts_needs_an_entry_for_each_and_names_the_ones_without_one(self) -> None:
        both = frozenset({Finding("CVE-2026-1111", NETTY, (JAR, OTHER_JAR))})
        one = frozenset({Accepted("spark", JAR, "CVE-2026-1111", NETTY)})
        self.assertEqual([Finding("CVE-2026-1111", NETTY, (OTHER_JAR,))], scan.unaccepted("spark", both, one))
        self.assertEqual([], scan.unaccepted("spark", both, one | {Accepted("spark", OTHER_JAR, "CVE-2026-1111", NETTY)}))
        split = one | {Accepted("replay", OTHER_JAR, "CVE-2026-1111", NETTY)}
        self.assertEqual([Finding("CVE-2026-1111", NETTY, (OTHER_JAR,))], scan.unaccepted("spark", both, split))

    def test_a_refusal_shows_at_most_three_artifacts_and_counts_the_others(self) -> None:
        self.assertEqual(f"CVE-2026-1111 {NETTY} in /a /b /c", scan.shown(Finding("CVE-2026-1111", NETTY, ("/a", "/b", "/c"))))
        self.assertEqual(f"CVE-2026-1111 {NETTY} in /a /b /c and 2 more", scan.shown(Finding("CVE-2026-1111", NETTY, ("/a", "/b", "/c", "/d", "/e"))))

    def test_the_scan_asks_scout_for_the_fixable_findings_of_the_local_linux_image_in_sarif(self) -> None:
        self.assertEqual(
            ["scout", "cves", "--platform", "linux/amd64", "--only-fixed", "--format", "sarif", "--output", "scratch/report.sarif", "local://repo:tag"],
            scan.scan_arguments("repo:tag", Path("scratch/report.sarif")),
        )

    def test_the_accepted_list_is_in_no_image_source_so_an_entry_never_changes_an_image_tag(self) -> None:
        relative = scan.ACCEPTED_FILE.relative_to(pins.ROOT).as_posix()
        for image in pins.load_images():
            for source in image["sources"]:
                with self.subTest(image=image["name"], source=source):
                    self.assertFalse(relative.startswith(f"{source}/"))

    def test_the_publisher_gives_its_default_settings_the_committed_list(self) -> None:
        self.assertEqual(scan.ACCEPTED_FILE, publish.Settings.__dataclass_fields__["accepted_file"].default)


if __name__ == "__main__":
    unittest.main()
