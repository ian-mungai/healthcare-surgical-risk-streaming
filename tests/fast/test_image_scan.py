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
from scripts.images.scan import Finding, ScanError

NETTY = "pkg:maven/io.netty/netty-handler@4.2.7"


def result(cve: str = "CVE-2026-1111", purl: str = NETTY) -> dict[str, Any]:
    text = (
        f"Vulnerability    :{cve}   \nSeverity         :HIGH   \nPackage          :{purl}   \nAffected range   :<9   \nFixed version    :9   \n"
        "EPSS Score       :0.001900   \n"
    )
    return {"ruleId": cve, "ruleIndex": 0, "kind": "fail", "level": "error", "message": {"text": text}}


def report(*results: dict[str, Any]) -> dict[str, Any]:
    return {"version": "2.1.0", "runs": [{"tool": {"driver": {"name": "docker scout", "rules": []}}, "results": list(results)}]}


def accepted_text(*entries: str) -> str:
    return "findings:\n" + "".join(entries) if entries else "findings: []\n"


def entry(cve: str = "CVE-2026-1111", package: str = NETTY, reason: str = "No release holds the fix.", date: str = '"2026-11-25"') -> str:
    return f"  - cve: {cve}\n    package: {package}\n    reason: {reason}\n    date: {date}\n"


class ReportTest(unittest.TestCase):
    def test_each_result_is_one_finding_of_its_cve_and_its_package_without_qualifiers_and_with_percent_codes_read(self) -> None:
        debian = "pkg:deb/debian/zlib@1%3A1.3.dfsg%2Breally1.3.1-1?os_distro=trixie&os_name=debian&os_version=13"
        found = scan.read_report(json.dumps(report(result(), result("GHSA-6v7p-g79w-8964", debian), result())))
        self.assertEqual({Finding("CVE-2026-1111", NETTY), Finding("GHSA-6v7p-g79w-8964", "pkg:deb/debian/zlib@1:1.3.dfsg+really1.3.1-1")}, found)

    def test_a_report_with_no_result_holds_no_finding(self) -> None:
        self.assertEqual(frozenset(), scan.read_report(json.dumps(report())))

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
                scan.read_report(text)


class AcceptedListTest(unittest.TestCase):
    def load(self, text: str) -> frozenset[Finding]:
        with tempfile.TemporaryDirectory(prefix="accepted_") as folder:
            path = Path(folder) / "accepted.yml"
            path.write_text(text)
            return scan.load_accepted(path)

    def test_an_empty_list_accepts_nothing_and_an_entry_accepts_its_cve_and_package(self) -> None:
        self.assertEqual(frozenset(), self.load(accepted_text()))
        self.assertEqual({Finding("CVE-2026-1111", NETTY)}, self.load(accepted_text(entry())))

    def test_a_missing_file_is_refused(self) -> None:
        with self.assertRaises(ScanError):
            scan.load_accepted(Path("/nonexistent/accepted.yml"))

    def test_an_entry_names_exactly_a_cve_a_package_with_its_version_a_reason_and_a_date(self) -> None:
        cases = {
            "no reason": "findings:\n  - cve: CVE-2026-1111\n    package: " + NETTY + '\n    date: "2026-11-25"\n',
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

    def test_the_committed_list_is_valid_and_each_entry_is_for_a_library_that_another_artifact_holds_inside_it(self) -> None:
        for finding in scan.load_accepted(scan.ACCEPTED_FILE):
            with self.subTest(finding=finding):
                self.assertRegex(finding.package, r"^pkg:(maven|golang)/")


class DecisionTest(unittest.TestCase):
    def test_only_a_finding_that_the_list_names_with_the_same_cve_and_the_same_package_version_passes(self) -> None:
        listed = frozenset({Finding("CVE-2026-1111", NETTY)})
        self.assertEqual([], scan.unaccepted(frozenset({Finding("CVE-2026-1111", NETTY)}), listed))
        others = [Finding("CVE-2026-2222", NETTY), Finding("CVE-2026-1111", NETTY.replace("4.2.7", "4.2.8"))]
        self.assertEqual(sorted(others), scan.unaccepted(frozenset({*others, *listed}), listed))

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
