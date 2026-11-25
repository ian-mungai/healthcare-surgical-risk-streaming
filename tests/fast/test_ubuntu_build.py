"""One tracked file pins the Canonical Ubuntu 24.04 build, and nothing else in the repository names a build.

The same build serial runs as the AMI on EC2 and as the cloud image on a home KVM virtual machine, so the serial, the AMI of each region, the cloud
image's URL and its SHA-256 sum live in ``infra/ubuntu_build.json`` only. Terraform reads the AMI from it (``infra/main.tf``); the host module no longer
looks the image up in the AWS parameter store, so the project profile needs no Systems Manager grant.

Limit of these checks: they run offline. That an AMI's name carries the pinned serial (``ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-<serial>``
in the account that Canonical owns) and that the cloud image's bytes have the pinned SHA-256 sum cannot be proven here: the first needs the EC2 API, the
second a download of the image. The owner's heavy run does both.

Failure modes covered here:

- The pin file is missing, is not JSON or holds a key beyond the four it defines: refused.
- The serial is not eight digits, an AMI is not ``ami-`` and 17 lower-case hex digits, a region name is not a region name, the sum is not 64 lower-case hex
  digits, or the URL is not the image of the pinned serial under ``https://cloud-images.ubuntu.com/releases/noble/``: each refused for its own cause.
- No region has an AMI: refused, so the host would have no image.
- A tracked or new file other than the pin names an Ubuntu AMI, the pinned serial or a build's release folder: refused, named by file and line.
- A Terraform file looks the image up in the parameter store again, or a policy template grants Systems Manager again: refused.
"""

from __future__ import annotations

import copy
import json
import re
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any

from tests.fast.git_fixture import ROOT, git_in

PIN = "infra/ubuntu_build.json"
SERIAL = re.compile(r"\d{8}")
AMI = re.compile(r"ami-[0-9a-f]{17}")
SUM = re.compile(r"[0-9a-f]{64}")
REGION = re.compile(r"[a-z]{2}(?:-[a-z]+)+-[0-9]")
URL_FOLDER = "https://cloud-images.ubuntu.com/releases/noble/release-{serial}/"
IMAGE_FILE = "ubuntu-24.04-server-cloudimg-amd64.img"
KEYS = {"serial", "amis", "cloud_image"}
IMAGE_KEYS = {"url", "sha256"}
# What names a build outside the pin: an AMI ID, a build's folder or an AMI name with a serial. The pinned serial itself is added at run time.
BUILD_NAMES = (re.compile(r"\bami-[0-9a-f]{17}\b"), re.compile(r"\brelease-\d{8}\b"), re.compile(r"ubuntu-noble-24\.04-[a-z0-9]+-server-\d{8}"))
SSM_NAMES = (re.compile(r"aws_ssm_parameter"), re.compile(r"\bssm:", re.IGNORECASE), re.compile(r"/aws/service/canonical"))


def pin_problems(pin: Any) -> list[str]:
    """Each way in which the pin's form is wrong, empty for a good pin. Offline: the AMI's name and the image's bytes are not checked."""
    if not isinstance(pin, dict):
        return ["the pin is not a JSON object"]
    if set(pin) != KEYS:
        return [f"the pin's keys are {sorted(pin)}, not {sorted(KEYS)}"]
    problems = []
    serial = pin["serial"]
    if not isinstance(serial, str) or not SERIAL.fullmatch(serial):
        problems.append("the serial is not eight digits (YYYYMMDD)")
    amis = pin["amis"]
    if not isinstance(amis, dict) or not amis:
        problems.append("amis is not a mapping of at least one region")
    else:
        for region, ami in amis.items():
            if not REGION.fullmatch(str(region)):
                problems.append(f"{region!r} is not a region name")
            if not isinstance(ami, str) or not AMI.fullmatch(ami):
                problems.append(f"the AMI of {region} is not 'ami-' and 17 lower-case hex digits")
    image = pin["cloud_image"]
    if not isinstance(image, dict) or set(image) != IMAGE_KEYS:
        problems.append(f"cloud_image's keys are not {sorted(IMAGE_KEYS)}")
    else:
        if not isinstance(image["sha256"], str) or not SUM.fullmatch(image["sha256"]):
            problems.append("the sha256 is not 64 lower-case hex digits")
        if not isinstance(serial, str) or image["url"] != URL_FOLDER.format(serial=serial) + IMAGE_FILE:
            problems.append(f"the URL is not {URL_FOLDER.format(serial='<serial>')}{IMAGE_FILE} for the pinned serial")
    return problems


def readable_files() -> list[str]:
    """The repository files that Git tracks or that are new and not ignored, as text. The pin itself and any file that is not UTF-8 text are left out."""
    listed = git_in(ROOT, "ls-files", "-z", "--cached", "--others", "--exclude-standard", keep_newlines=True).stdout
    return sorted({name for name in listed.split("\0") if name and name != PIN and (ROOT / name).is_file()})


def build_patterns(serial: str) -> tuple[re.Pattern[str], ...]:
    """The names of a build that no file but the pin may hold: an AMI ID, a build's folder, an AMI name with a serial and the pinned serial."""
    return (*BUILD_NAMES, re.compile(re.escape(serial)))


def hits(files: dict[str, str], patterns: tuple[re.Pattern[str], ...]) -> list[str]:
    return [
        f"{name}:{number}" for name, text in sorted(files.items()) for number, line in enumerate(text.splitlines(), 1) if any(p.search(line) for p in patterns)
    ]


def read_text(name: str) -> str | None:
    try:
        return (ROOT / name).read_text()
    except UnicodeDecodeError, OSError:
        return None


class PinFormTest(unittest.TestCase):
    pin: dict[str, Any]

    @classmethod
    def setUpClass(cls) -> None:
        cls.pin = json.loads((ROOT / PIN).read_text())

    def test_the_pin_has_a_good_form(self) -> None:
        self.assertEqual([], pin_problems(self.pin))

    def test_the_pin_holds_the_region_that_the_environment_uses(self) -> None:
        self.assertIn("us-west-2", self.pin["amis"])

    def test_the_cloud_image_comes_from_the_pinned_release_folder(self) -> None:
        self.assertEqual(URL_FOLDER.format(serial=self.pin["serial"]) + IMAGE_FILE, self.pin["cloud_image"]["url"])


class PinFormDefectsTest(unittest.TestCase):
    """Each planted defect in a good pin is refused for its own cause. The good pin is built here, so a change of the real pin cannot hide a defect."""

    GOOD = {
        "serial": "20260101",
        "amis": {"us-west-2": "ami-" + "0123456789abcdef0"},
        "cloud_image": {"url": URL_FOLDER.format(serial="20260101") + IMAGE_FILE, "sha256": "ab" * 32},
    }

    def planted(self, change: Callable[[dict[str, Any]], None]) -> list[str]:
        pin = copy.deepcopy(self.GOOD)
        change(pin)
        return pin_problems(pin)

    def test_the_good_pin_is_accepted(self) -> None:
        self.assertEqual([], pin_problems(self.GOOD))

    def test_each_defect_is_refused_for_its_own_cause(self) -> None:
        cases: dict[str, tuple[Callable[[dict[str, Any]], None], str]] = {
            "a serial of seven digits": (lambda p: p.update(serial="2026101"), "serial is not eight digits"),
            "a serial with a letter": (lambda p: p.update(serial="2026101a"), "serial is not eight digits"),
            "a serial that is a number": (lambda p: p.update(serial=20260101), "serial is not eight digits"),
            "an AMI of 16 hex digits": (lambda p: p["amis"].update({"us-west-2": "ami-" + "0" * 16}), "AMI of us-west-2"),
            "an AMI with an upper-case digit": (lambda p: p["amis"].update({"us-west-2": "ami-" + "0" * 16 + "A"}), "AMI of us-west-2"),
            "an AMI with a non-hex letter": (lambda p: p["amis"].update({"us-west-2": "ami-" + "0" * 16 + "g"}), "AMI of us-west-2"),
            "an AMI of 18 hex digits": (lambda p: p["amis"].update({"us-west-2": "ami-" + "0" * 18}), "AMI of us-west-2"),
            "an AMI with text around it": (lambda p: p["amis"].update({"us-west-2": " ami-" + "0" * 17}), "AMI of us-west-2"),
            "a region that is not a region": (lambda p: p["amis"].update({"west": "ami-" + "0" * 17}), "not a region name"),
            "no region": (lambda p: p.update(amis={}), "at least one region"),
            "a sum of 63 digits": (lambda p: p["cloud_image"].update(sha256="a" * 63), "sha256"),
            "a sum of 65 digits": (lambda p: p["cloud_image"].update(sha256="a" * 65), "sha256"),
            "an upper-case sum": (lambda p: p["cloud_image"].update(sha256="A" * 64), "sha256"),
            "a URL of another host": (
                lambda p: p["cloud_image"].update(url=URL_FOLDER.format(serial="20260101").replace("cloud-images.ubuntu.com", "example.org") + IMAGE_FILE),
                "URL",
            ),
            "a URL over http": (lambda p: p["cloud_image"].update(url=URL_FOLDER.format(serial="20260101").replace("https:", "http:") + IMAGE_FILE), "URL"),
            "a URL of another serial": (lambda p: p["cloud_image"].update(url=URL_FOLDER.format(serial="20260102") + IMAGE_FILE), "URL"),
            "a URL of the rolling folder": (
                lambda p: p["cloud_image"].update(url="https://cloud-images.ubuntu.com/releases/noble/release/" + IMAGE_FILE),
                "URL",
            ),
            "a URL of another release": (
                lambda p: p["cloud_image"].update(url=URL_FOLDER.format(serial="20260101").replace("noble", "jammy") + IMAGE_FILE),
                "URL",
            ),
            "a URL of another file": (lambda p: p["cloud_image"].update(url=URL_FOLDER.format(serial="20260101") + "other.img"), "URL"),
            "an extra key": (lambda p: p.update(name="x"), "keys"),
            "a missing key": (lambda p: p.pop("serial"), "keys"),
            "an extra image key": (lambda p: p["cloud_image"].update(size=1), "cloud_image's keys"),
        }
        for label, (change, cause) in cases.items():
            with self.subTest(defect=label):
                problems = self.planted(change)
                self.assertTrue(problems, f"{label} was accepted")
                self.assertTrue(any(cause in problem for problem in problems), f"{label} was refused for another cause: {problems}")

    def test_a_pin_that_is_not_an_object_is_refused(self) -> None:
        not_objects: list[Any] = [[], "x", None, 3]
        for pin in not_objects:
            with self.subTest(pin=pin):
                self.assertEqual(["the pin is not a JSON object"], pin_problems(pin))


class NothingElseNamesABuildTest(unittest.TestCase):
    def test_no_other_file_names_an_ami_a_serial_or_a_release_folder(self) -> None:
        pin: dict[str, Any] = json.loads((ROOT / PIN).read_text())
        patterns = build_patterns(pin["serial"])
        files = {name: text for name in readable_files() if (text := read_text(name)) is not None}
        self.assertIn("infra/main.tf", files, "the scan reads the repository's files")
        self.assertEqual([], hits(files, patterns))

    def test_the_scan_finds_each_kind_of_name(self) -> None:
        files = {
            "a": "ami = " + '"ami-' + "0123456789abcdef0" + '"\n',
            "b": "x\nhttps://cloud-images.ubuntu.com/releases/noble/release-" + "20260101/\n",
            "c": "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-" + "20260101\n",
            "d": "the serial 20260101 here\n",
            "e": "no build is named here: ami-too-short, release-notes\n",
        }
        self.assertEqual(["a:1", "b:2", "c:1", "d:1"], hits(files, build_patterns("20260101")))
        self.assertEqual(["a:1", "b:2", "c:1"], hits(files, build_patterns("20260102")), "the pinned serial is the one that the fourth file's line names")


class NoParameterStoreLookupTest(unittest.TestCase):
    def test_the_scan_finds_each_kind_of_parameter_store_use(self) -> None:
        files = {
            "a": 'data "aws_ssm_parameter" "ubuntu" {\n',
            "b": '"Action": "ssm:GetParameter"\n',
            "c": 'name = "/aws/service/canonical/ubuntu/server/24.04"\n',
            "d": "a dessmount and a classmethod name no grant\n",
        }
        self.assertEqual(["a:1", "b:1", "c:1"], hits(files, SSM_NAMES))

    def test_no_terraform_file_and_no_policy_template_uses_the_parameter_store(self) -> None:
        names = [name for name in readable_files() if name.startswith("infra/") and name.endswith((".tf", ".hcl", ".json", ".tftpl"))]
        files = {name: text for name in names if (text := read_text(name)) is not None}
        self.assertIn("infra/modules/host/main.tf", files)
        self.assertEqual([], hits(files, SSM_NAMES))

    def test_no_policy_template_is_named_for_the_parameter_store(self) -> None:
        self.assertEqual([], sorted(path.name for path in Path(ROOT, "infra/iam/policies").glob("*ssm*")))

    def test_the_host_reads_its_image_from_the_pin_for_the_run_region(self) -> None:
        main = (ROOT / "infra/main.tf").read_text()
        self.assertRegex(main, r'ubuntu_build\s*=\s*jsondecode\(file\("\$\{path\.module\}/ubuntu_build\.json"\)\)')
        self.assertRegex(main, r'\bubuntu_ami\s*=\s*lookup\(local\.ubuntu_build\.amis, var\.aws_region, ""\)')
        self.assertRegex(main, r"\bami\s*=\s*local\.ubuntu_ami\b")


if __name__ == "__main__":
    unittest.main()
