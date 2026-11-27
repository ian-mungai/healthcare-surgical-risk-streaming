"""The second scanner of the image gate reads Trivy's JSON report and its pinned databases, and fails closed on anything else.

The reports here are fixtures in the shape that the pinned Trivy writes (real reports of Nov 26 2026). No test runs Docker or Trivy or reaches a network.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.images import pins, publish, scan, trivy_scan
from scripts.images.scan import Accepted, Finding, ScanError
from scripts.quality import install_tools

TAG = "example/project_images:spark-4.1.4-0123456789ab"
JAR = "opt/spark/jars/holder-1.0.jar"
JACKSON = "pkg:maven/com.fasterxml.jackson.core/jackson-databind@2.18.6"
DEBIAN_WITH_EPOCH = "pkg:deb/debian/passwd@4.17.4-2%2Bb1?arch=amd64&distro=debian-13.7&epoch=1"
DIGEST = "a" * 64


def vulnerability(cve: str = "CVE-2026-1111", purl: str = JACKSON, path: str | None = JAR, severity: str = "HIGH") -> dict[str, Any]:
    found: dict[str, Any] = {
        "VulnerabilityID": cve,
        "PkgName": "a-package",
        "PkgIdentifier": {"PURL": purl, "UID": "1"},
        "InstalledVersion": "1",
        "FixedVersion": "2",
        "Status": "fixed",
        "Severity": severity,
    }
    if path is not None:
        found["PkgPath"] = path
    return found


def result(kind: str = "lang-pkgs", type_: str = "jar", target: str = "Java", *found: dict[str, Any]) -> dict[str, Any]:
    made: dict[str, Any] = {"Target": target, "Class": kind, "Type": type_, "Packages": [{"Name": "a-package"}]}
    if found:
        made["Vulnerabilities"] = list(found)
    return made


def os_result(*found: dict[str, Any]) -> dict[str, Any]:
    return result("os-pkgs", "debian", f"{TAG} (debian 13.7)", *found)


def report(*results: dict[str, Any]) -> dict[str, Any]:
    """A report as the pinned Trivy writes it: its own version, the scanned tag, the detected OS and one result for the OS packages."""
    return {
        "SchemaVersion": 2,
        "Trivy": {"Version": "0.76.2"},
        "ArtifactName": TAG,
        "ArtifactType": "container_image",
        "Metadata": {"OS": {"Family": "debian", "Name": "13.7"}},
        "Results": list(results) or [os_result()],
    }


def read(document: dict[str, Any] | str) -> frozenset[Finding]:
    return trivy_scan.read_report(document if isinstance(document, str) else json.dumps(document), 0, TAG)


class ReportTest(unittest.TestCase):
    def test_a_library_in_a_jar_is_a_finding_at_the_jar_with_a_leading_slash_and_the_package_url_that_scout_writes(self) -> None:
        found = read(report(os_result(), result("lang-pkgs", "jar", "Java", vulnerability())))
        self.assertEqual({Finding("CVE-2026-1111", JACKSON, ("/" + JAR,))}, found)

    def test_a_jar_inside_a_jar_keeps_its_whole_path_so_an_entry_for_the_outer_jar_never_covers_it(self) -> None:
        nested = f"{JAR}/BOOT-INF/lib/inner-2.0.jar"
        (found,) = read(report(os_result(), result("lang-pkgs", "jar", "Java", vulnerability(path=nested))))
        self.assertEqual(("/" + nested,), found.artifacts)
        outer = frozenset({Accepted("spark", "/" + JAR, "CVE-2026-1111", JACKSON)})
        self.assertEqual([found], scan.unaccepted("spark", frozenset({found}), outer))

    def test_a_path_of_the_form_that_scout_uses_for_a_library_inside_a_jar_is_not_folded_into_the_jar(self) -> None:
        inside = f"{JAR}:com.fasterxml.jackson.core:jackson-databind"
        (found,) = read(report(os_result(), result("lang-pkgs", "jar", "Java", vulnerability(path=inside))))
        self.assertEqual(("/" + inside,), found.artifacts)

    def test_an_os_package_has_no_reported_path_so_no_entry_accepts_it_and_it_keeps_its_binary_package_url_with_the_epoch_in_the_version(self) -> None:
        (found,) = read(report(os_result(vulnerability(purl=DEBIAN_WITH_EPOCH, path=None))))
        self.assertEqual(Finding("CVE-2026-1111", "pkg:deb/debian/passwd@1:4.17.4-2+b1", (trivy_scan.NO_PATH,)), found)
        dpkg = frozenset({Accepted("spark", "/var/lib/dpkg/status", "CVE-2026-1111", found.package)})
        self.assertEqual([found], scan.unaccepted("spark", frozenset({found}), dpkg))

    def test_an_os_package_without_an_epoch_keeps_its_version(self) -> None:
        (found,) = read(report(os_result(vulnerability(purl="pkg:deb/debian/adduser@3.152?arch=all&distro=debian-13.7", path=None))))
        self.assertEqual("pkg:deb/debian/adduser@3.152", found.package)

    def test_a_program_file_that_is_its_own_target_is_the_artifact_of_its_findings(self) -> None:
        go = vulnerability(purl="pkg:golang/stdlib@v1.27.4", path=None)
        (found,) = read(report(os_result(), result("lang-pkgs", "gobinary", "usr/local/bin/docker", go)))
        self.assertEqual(("/usr/local/bin/docker",), found.artifacts)

    def test_a_library_without_a_path_can_never_be_accepted(self) -> None:
        pypi = "pkg:pypi/msgpack@1.1.2"
        for type_, target in (("python-pkg", "Python"), ("a-new-type", "opt/a-file")):
            with self.subTest(type=type_):
                (found,) = read(report(os_result(), result("lang-pkgs", type_, target, vulnerability(purl=pypi, path=None))))
                self.assertEqual((trivy_scan.NO_PATH,), found.artifacts)
                with self.assertRaises(ScanError):
                    scan.accepted_entry(
                        {"image": "spark", "artifact": trivy_scan.NO_PATH, "cve": "CVE-2026-1111", "package": pypi, "reason": "r", "date": "2026-11-26"}
                    )
                self.assertEqual([found], scan.unaccepted("spark", frozenset({found}), frozenset()))

    def test_every_vulnerability_that_the_report_holds_is_a_finding_whatever_its_severity_or_status(self) -> None:
        low = vulnerability("CVE-2026-2222", severity="LOW")
        affected = {**vulnerability("CVE-2026-3333"), "Status": "affected"}
        self.assertEqual(3, len(read(report(os_result(), result("lang-pkgs", "jar", "Java", vulnerability(), low, affected)))))

    def test_one_package_in_two_jars_is_two_findings_each_at_its_own_jar(self) -> None:
        two = result("lang-pkgs", "jar", "Java", vulnerability(), vulnerability(path="opt/other.jar"), vulnerability())
        self.assertEqual({("/" + JAR,), ("/opt/other.jar",)}, {found.artifacts for found in read(report(os_result(), two))})

    def test_a_complete_report_with_no_vulnerability_holds_no_finding(self) -> None:
        self.assertEqual(frozenset(), read(report(os_result(), result())))


class RefusalTest(unittest.TestCase):
    def refused(self, text: str, pattern: str = "", status: object = 0, tag: str = TAG) -> None:
        with self.assertRaisesRegex(ScanError, pattern):
            trivy_scan.read_report(text, status, tag)

    def test_anything_that_is_not_one_complete_report_of_the_pinned_trivy_for_the_scanned_tag_is_refused(self) -> None:
        def changed(**more: object) -> str:
            return json.dumps({**report(), **more})

        def without(key: str) -> str:
            document = report()
            del document[key]
            return json.dumps(document)

        cases = {
            "empty": "",
            "truncated": json.dumps(report())[:-3],
            "a list": "[]",
            "text": '"report"',
            "no schema version": without("SchemaVersion"),
            "another schema version": changed(SchemaVersion=3),
            "the schema version as text": changed(SchemaVersion="2"),
            "the schema version as 2.0": changed(SchemaVersion=2.0),
            "no trivy": without("Trivy"),
            "another trivy": changed(Trivy={"Version": "0.76.1"}),
            "a trivy that is text": changed(Trivy="0.76.2"),
            "a scout report": json.dumps({"version": "2.1.0", "runs": [{"tool": {"driver": {"name": "docker scout"}}, "results": []}]}),
            "another artifact type": changed(ArtifactType="filesystem"),
            "another image": changed(ArtifactName=TAG + "x"),
            "no metadata": without("Metadata"),
            "metadata that is a list": changed(Metadata=[]),
            "no results": without("Results"),
            "results as a mapping": changed(Results={}),
            "empty results": changed(Results=[]),
        }
        for name, text in cases.items():
            with self.subTest(report=name):
                self.refused(text)

    def test_an_os_that_was_not_detected_is_unknown_or_is_past_its_end_of_life_refuses_the_report(self) -> None:
        cases: dict[str, Any] = {
            "none": None,
            "no family": {"Name": "13.7"},
            "no name": {"Family": "debian"},
            "an empty name": {"Family": "debian", "Name": ""},
            "another family": {"Family": "alpine", "Name": "3.20"},
            "a family that is a list": {"Family": ["debian"], "Name": "13.7"},
            "a family that is a mapping": {"Family": {"debian": "13"}, "Name": "13.7"},
            "a family that is a number": {"Family": 13, "Name": "13.7"},
            "end of life": {"Family": "debian", "Name": "13.7", "EOSL": True},
            "an end of life that is not true or false": {"Family": "debian", "Name": "13.7", "EOSL": "true"},
        }
        for name, found in cases.items():
            with self.subTest(os=name):
                document = report()
                document["Metadata"] = {"OS": found} if found is not None else {}
                self.refused(json.dumps(document), "operating system")

    def test_a_report_without_exactly_one_result_of_the_os_packages_or_with_a_result_that_lists_no_package_is_refused(self) -> None:
        no_packages = os_result()
        del no_packages["Packages"]
        cases = {
            "no os result": [result()],
            "two os results": [os_result(), os_result()],
            "an os result of another family": [result("os-pkgs", "ubuntu", "x")],
            "an os result without packages": [no_packages],
            "an os result with an empty package list": [{**os_result(), "Packages": []}],
            "a library result without packages": [os_result(), {key: value for key, value in result().items() if key != "Packages"}],
            "a class that was not asked for": [os_result(), result("secret", "secret", "x")],
            "a result that is text": [os_result(), "x"],
            "a result without a type": [os_result(), {key: value for key, value in result().items() if key != "Type"}],
            "a result without a target": [os_result(), {key: value for key, value in result().items() if key != "Target"}],
        }
        for name, results in cases.items():
            with self.subTest(case=name):
                self.refused(json.dumps({**report(), "Results": results}))

    def test_a_package_record_that_is_not_a_mapping_with_a_name_refuses_the_report_in_fixed_text(self) -> None:
        cases: dict[str, Any] = {
            "null": None,
            "text": "forged",
            "a number": 7,
            "a list": ["forged"],
            "an empty mapping": {},
            "no name": {"Version": "forged"},
            "an empty name": {"Name": ""},
            "a name that is a number": {"Name": 7},
            "a name that is null": {"Name": None},
        }
        for name, record in cases.items():
            for packages in ([record], [{"Name": "a-package"}, record]):
                for results in ([{**os_result(), "Packages": packages}], [os_result(), {**result(), "Packages": packages}]):
                    with self.subTest(case=name, records=len(packages), results=len(results)):
                        with self.assertRaisesRegex(ScanError, "list of checked packages") as raised:
                            read({**report(), "Results": results})
                        self.assertNotIn("forged", str(raised.exception))

    def test_a_vulnerability_that_does_not_name_its_id_package_and_artifact_in_their_forms_refuses_the_report_and_is_never_shown(self) -> None:
        def library(**more: object) -> dict[str, Any]:
            return {**vulnerability(), **more}

        cases: dict[str, Any] = {
            "not a mapping": "CVE-2026-1111",
            "no id": {key: value for key, value in vulnerability().items() if key != "VulnerabilityID"},
            "an id with a newline": library(VulnerabilityID="CVE-1\nforged"),
            "no package identifier": library(PkgIdentifier=None),
            "no package url": library(PkgIdentifier={"UID": "1"}),
            "a package url with a space": library(PkgIdentifier={"PURL": "pkg:maven/a/b@1 forged"}),
            "a package url without a version": library(PkgIdentifier={"PURL": "pkg:maven/a/b"}),
            "a path with a newline": library(PkgPath=JAR + "\nforged"),
            "a path that is a number": library(PkgPath=7),
            "an empty path": library(PkgPath=""),
            "an absolute path, which trivy does not write": library(PkgPath="/" + JAR),
        }
        for name, found in cases.items():
            with self.subTest(case=name), self.assertRaises(ScanError) as raised:
                read(report(os_result(), {**result(), "Vulnerabilities": [found]}))
            self.assertNotIn("forged", str(raised.exception))
        with self.assertRaisesRegex(ScanError, "operating system package"):
            read(report(os_result(vulnerability(purl="pkg:deb/debian/passwd@1", path=JAR))))
        with self.assertRaisesRegex(ScanError, "epoch"):
            read(report(os_result(vulnerability(purl="pkg:deb/debian/passwd@1?epoch=one", path=None))))
        with self.assertRaises(ScanError):
            read(report(os_result(), {**result(), "Vulnerabilities": {}}))
        with self.assertRaises(ScanError):
            read(report(os_result(), {**result(), "Vulnerabilities": None}))

    def test_only_the_exit_status_0_counts(self) -> None:
        for status in (1, 2, -9, None, "0", 0.0, False):
            with self.subTest(status=status):
                self.refused(json.dumps(report()), "exit status", status)

    def test_a_key_held_twice_or_a_number_that_standard_json_does_not_have_refuses_the_report(self) -> None:
        text = json.dumps(report(os_result(), result("lang-pkgs", "jar", "Java", vulnerability())))
        twice = text.replace('"Results": [', '"Results": [], "Results": [', 1)
        self.assertNotEqual(text, twice)
        self.refused(twice, "a key twice")
        for constant in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(constant=constant):
                self.refused(text.replace('"UID": "1"', f'"UID": {constant}', 1), "not standard JSON")


class ArgumentTest(unittest.TestCase):
    def test_the_scan_reads_only_the_local_docker_image_with_the_pinned_databases_and_no_file_that_could_hide_a_finding(self) -> None:
        arguments = trivy_scan.scan_arguments("repo:tag", Path("/w/report.json"), Path("/cache/pinned"), Path("/w/trivy"))
        self.assertEqual(
            [
                "image",
                "--config=/w/trivy/trivy.yaml",
                "--module-dir=/w/trivy/modules",
                "--cache-dir=/cache/pinned",
                "--cache-backend=memory",
                "--skip-db-update",
                "--skip-java-db-update",
                "--offline-scan",
                "--skip-version-check",
                "--disable-telemetry",
                "--image-src=docker",
                "--platform=linux/amd64",
                "--scanners=vuln",
                "--pkg-types=os,library",
                "--severity=HIGH,CRITICAL",
                "--ignore-unfixed",
                "--list-all-pkgs",
                "--ignorefile=",
                "--secret-config=",
                "--no-progress",
                "--format=json",
                "--output=/w/report.json",
                "repo:tag",
            ],
            arguments,
        )

    def test_no_trivy_setting_of_the_caller_s_environment_reaches_trivy(self) -> None:
        with mock.patch.dict("os.environ", {"TRIVY_IGNOREFILE": "x", "TRIVY_SEVERITY": "CRITICAL", "trivy_vex": "y", "HOME": "/h", "DOCKER_HOST": "u"}):
            environment = trivy_scan.clean_environment()
        self.assertEqual([], [name for name in environment if name.upper().startswith("TRIVY")])
        self.assertEqual(("/h", "u"), (environment["HOME"], environment["DOCKER_HOST"]))

    def test_the_publisher_gives_its_default_settings_the_pinned_trivy_its_committed_database_pins_and_the_cache_under_tools(self) -> None:
        fields = publish.Settings.__dataclass_fields__
        self.assertEqual(str(install_tools.TRIVY.path), fields["trivy"].default)
        self.assertEqual(trivy_scan.DATABASE_FILE, fields["trivy_databases"].default)
        self.assertEqual(install_tools.TOOLS / "trivy_db", fields["trivy_cache"].default)
        self.assertEqual(install_tools.TRIVY.version, trivy_scan.VERSION)


class DatabasePinTest(unittest.TestCase):
    def load(self, text: str) -> trivy_scan.Databases:
        with tempfile.TemporaryDirectory(prefix="databases_") as folder:
            path = Path(folder) / "databases.yml"
            path.write_text(text)
            return trivy_scan.load_databases(path)

    def test_the_committed_pins_name_both_databases_by_digest_and_are_in_no_image_source(self) -> None:
        found = trivy_scan.load_databases(trivy_scan.DATABASE_FILE)
        self.assertRegex(found.db, r"^mirror\.gcr\.io/aquasec/trivy-db@sha256:[0-9a-f]{64}$")
        self.assertRegex(found.java_db, r"^mirror\.gcr\.io/aquasec/trivy-java-db@sha256:[0-9a-f]{64}$")
        relative = trivy_scan.DATABASE_FILE.relative_to(pins.ROOT).as_posix()
        self.assertEqual([], [source for image in pins.load_images() for source in image["sources"] if relative.startswith(f"{source}/")])

    def test_a_pin_that_is_not_exactly_two_references_by_digest_is_refused(self) -> None:
        good = f"db: r.io/a/db@sha256:{DIGEST}\njava_db: r.io/a/java@sha256:{DIGEST}\n"
        self.assertEqual(f"r.io/a/db@sha256:{DIGEST}", self.load(good).db)
        cases = {
            "a tag": good.replace(f"db@sha256:{DIGEST}", "db:2", 1),
            "a tag and a digest": good.replace("a/db@", "a/db:2@"),
            "a short digest": good.replace(DIGEST, "a" * 63, 1),
            "only one": f"db: r.io/a/db@sha256:{DIGEST}\n",
            "another key": good + "extra: x\n",
            "a repeated key": good + f"db: r.io/a/db@sha256:{'b' * 64}\n",
            "not text": "db: 1\njava_db: 2\n",
            "a space": good.replace("r.io/a/db", "r.io/a /db"),
            "not YAML": "db: [\n",
            "a list": "- a\n",
        }
        for name, text in cases.items():
            with self.subTest(case=name), self.assertRaises(ScanError):
                self.load(text)
        with self.assertRaises(ScanError):
            trivy_scan.load_databases(Path("/nonexistent/databases.yml"))

    def test_a_download_folder_s_name_changes_with_either_digest(self) -> None:
        first = trivy_scan.Databases(f"r/db@sha256:{DIGEST}", f"r/java@sha256:{DIGEST}")
        others = [trivy_scan.Databases(f"r/db@sha256:{'b' * 64}", first.java_db), trivy_scan.Databases(first.db, f"r/java@sha256:{'c' * 64}")]
        self.assertEqual(3, len({trivy_scan.folder_prefix(databases) for databases in [first, *others]}))

    def test_a_cache_that_cannot_be_listed_is_an_error_and_never_read_as_empty(self) -> None:
        pinned = trivy_scan.Databases(f"r/db@sha256:{DIGEST}", f"r/java@sha256:{DIGEST}")
        with tempfile.TemporaryDirectory(prefix="databases_") as scratch:
            cache = Path(scratch) / "cache"
            cache.write_text("a file in place of the folder")
            with self.assertRaisesRegex(ScanError, "cache cannot be read"):
                trivy_scan.require_databases(cache, pinned)


def synthetic_download(trivy: str, arguments: list[str], work: Path, seconds: int, label: str) -> int:
    """In place of ``run_trivy``: write the two files of the database that the arguments ask for."""
    cache = Path(next(argument for argument in arguments if argument.startswith("--cache-dir=")).partition("=")[2])
    folder, name = ("java-db", "trivy-java.db") if "--download-java-db-only" in arguments else ("db", "trivy.db")
    (cache / folder).mkdir(parents=True, exist_ok=True)
    (cache / folder / name).write_text(f"the {folder} database")
    (cache / folder / "metadata.json").write_text("{}")
    return 0


class DownloadedFilesTest(unittest.TestCase):
    PINNED = trivy_scan.Databases(f"r/db@sha256:{DIGEST}", f"r/java@sha256:{DIGEST}")

    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="databases_")
        self.addCleanup(scratch.cleanup)
        self.cache = Path(scratch.name) / "cache"
        stand_in = mock.patch.object(trivy_scan, "run_trivy", side_effect=synthetic_download)
        self.download = stand_in.start()
        self.addCleanup(stand_in.stop)

    def fetch(self) -> bool:
        return trivy_scan.fetch_databases("trivy", self.cache, self.PINNED)

    def fetched(self) -> Path:
        self.assertTrue(self.fetch())
        return trivy_scan.require_databases(self.cache, self.PINNED)

    def refused(self) -> None:
        with self.assertRaisesRegex(ScanError, "run make trivy-db"):
            trivy_scan.require_databases(self.cache, self.PINNED)

    def test_the_mark_of_a_complete_download_records_the_size_and_the_sha256_of_each_database_file(self) -> None:
        folder = self.fetched()
        expected = {}
        for name in trivy_scan.DATABASE_FILES:
            content = (folder / name).read_bytes()
            expected[name] = {"size": len(content), "sha256": hashlib.sha256(content).hexdigest()}
        self.assertEqual(expected, json.loads((folder / trivy_scan.COMPLETE).read_text()))

    def test_a_database_file_that_is_missing_empty_shorter_or_changed_at_the_same_size_is_never_read_and_is_downloaded_again(self) -> None:
        for name in trivy_scan.DATABASE_FILES:
            folder = self.fetched()
            path = folder / name
            good = path.read_bytes()
            changes = {"empty": b"", "shorter": good[:-1], "longer": good + b"x", "changed at the same size": bytes(len(good)), "missing": None}
            for change, content in changes.items():
                with self.subTest(file=name, change=change):
                    if content is None:
                        path.unlink()
                    else:
                        path.write_bytes(content)
                    self.refused()
                    path.write_bytes(good)
                    self.assertEqual(folder, trivy_scan.require_databases(self.cache, self.PINNED))
            path.write_bytes(b"")
            calls = self.download.call_count
            self.assertTrue(self.fetch(), "make trivy-db downloads again instead of reusing the damaged folder")
            self.assertEqual(calls + 2, self.download.call_count)
            self.assertNotEqual(folder, trivy_scan.require_databases(self.cache, self.PINNED))
            for left in self.cache.iterdir():
                shutil.rmtree(left)

    def test_a_download_that_writes_an_empty_database_file_fails_and_leaves_no_folder(self) -> None:
        def empty_java(trivy: str, arguments: list[str], work: Path, seconds: int, label: str) -> int:
            synthetic_download(trivy, arguments, work, seconds, label)
            for path in Path(next(a for a in arguments if a.startswith("--cache-dir=")).partition("=")[2]).glob("java-db/trivy-java.db"):
                path.write_bytes(b"")
            return 0

        self.download.side_effect = empty_java
        with self.assertRaisesRegex(ScanError, "every database file"):
            self.fetch()
        self.assertEqual([], list(self.cache.iterdir()))

    def test_a_mark_that_does_not_record_a_nonzero_size_and_a_sha256_for_exactly_the_database_files_is_never_trusted(self) -> None:
        folder = self.fetched()
        mark = folder / trivy_scan.COMPLETE
        good = json.loads(mark.read_text())
        first = trivy_scan.DATABASE_FILES[2]
        empty = {"size": 0, "sha256": hashlib.sha256(b"").hexdigest()}
        cases: dict[str, str] = {
            "empty, as an earlier download wrote it": "",
            "not JSON": "{",
            "a list": "[]",
            "text": '"complete"',
            "no file": "{}",
            "a file less": json.dumps({name: value for name, value in good.items() if name != first}),
            "a file more": json.dumps({**good, "other": good[first]}),
            "a record that is a number": json.dumps({**good, first: 7}),
            "no size": json.dumps({**good, first: {"sha256": good[first]["sha256"]}}),
            "no sha256": json.dumps({**good, first: {"size": good[first]["size"]}}),
            "the size as text": json.dumps({**good, first: {**good[first], "size": str(good[first]["size"])}}),
            "a key twice": mark.read_text().replace("{", f'{{"{first}": 1, ', 1),
            "nested too deep": "[" * 100_000,
        }
        for name, text in cases.items():
            with self.subTest(mark=name):
                mark.write_text(text)
                self.refused()
        with self.subTest(mark="an empty file recorded as empty"):
            (folder / first).write_bytes(b"")
            mark.write_text(json.dumps({**good, first: empty}))
            self.refused()
        with self.subTest(mark="bytes that are not UTF-8"):
            mark.write_bytes(b"\xff\xfe")
            self.refused()


if __name__ == "__main__":
    unittest.main()
