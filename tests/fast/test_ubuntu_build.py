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
- A tracked or new text file of any folder other than the pin names an Ubuntu AMI, the pinned serial or a build's release folder: refused, named by file
  and line.
- Any tracked or new text file of any folder (Terraform, IAM policy, script, YAML, shell) uses the parameter store in any form, or names its public parameter
  paths: refused, named by file and line. Only this file is left out of that scan, because its patterns and fixtures are the forms.
"""

from __future__ import annotations

import copy
import json
import re
import tempfile
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
# What uses the parameter store in any form: the name as a word or as a part of an identifier (``aws_ssm_parameter``, ``ssm:GetParameter``, ``aws ssm``,
# ``client("ssm")``, ``ssm_client``), the managed policies and clients named for it, and the paths of the public parameters. A run of letters or digits on
# either side (``classmethod``, ``dessmount``) is no use.
SSM_NAMES = (
    re.compile(r"(?<![A-Za-z0-9])ssm(?![A-Za-z0-9])", re.IGNORECASE),
    re.compile(r"AmazonSSM|SSMClient"),
    re.compile(r"/aws/service/"),
)
GUARD = "tests/fast/test_ubuntu_build.py"


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


def scanned_files(root: Path, skip: tuple[str, ...] = ()) -> dict[str, str]:
    """Every file of the repository at ``root`` that Git tracks or that is new and not ignored, as text by name. The pin itself, the files of ``skip`` and
    any file that is not UTF-8 text are left out."""
    listed = git_in(root, "ls-files", "-z", "--cached", "--others", "--exclude-standard", keep_newlines=True).stdout
    names = sorted({name for name in listed.split("\0") if name and name != PIN and name not in skip and (root / name).is_file()})
    return {name: text for name in names if (text := read_text(root, name)) is not None}


def pinned_serial(root: Path) -> str:
    serial: str = json.loads((root / PIN).read_text())["serial"]
    return serial


def build_patterns(serial: str) -> tuple[re.Pattern[str], ...]:
    """The names of a build that no file but the pin may hold: an AMI ID, a build's folder, an AMI name with a serial and the pinned serial."""
    return (*BUILD_NAMES, re.compile(re.escape(serial)))


def hits(files: dict[str, str], patterns: tuple[re.Pattern[str], ...]) -> list[str]:
    return [
        f"{name}:{number}" for name, text in sorted(files.items()) for number, line in enumerate(text.splitlines(), 1) if any(p.search(line) for p in patterns)
    ]


def read_text(root: Path, name: str) -> str | None:
    try:
        return (root / name).read_text()
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
        files = scanned_files(ROOT)
        self.assertIn("infra/main.tf", files, "the scan reads the repository's files")
        self.assertIn("scripts/infra/host.py", files, "the scan reads every folder, not infra alone")
        self.assertEqual([], hits(files, build_patterns(pinned_serial(ROOT))))

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
    def test_the_scan_finds_each_form_of_parameter_store_use(self) -> None:
        forms = {
            "a Terraform data source": 'data "aws_ssm_' + 'parameter" "ubuntu" {',
            "a Terraform resource": 'resource "aws_ssm_' + 'document" "x" {',
            "an IAM action": '"Action": "ssm' + ':GetParameter"',
            "an IAM action in YAML": "- ssm" + ":GetParameters",
            "a managed policy": "arn:aws:iam::aws:policy/AmazonSS" + "MManagedInstanceCore",
            "the AWS CLI service": "aws ss" + "m get-parameter --name x",
            "the AWS CLI service in an argument list": '["aws", "ss' + 'm", "get-parameter"]',
            "a boto3 client": 'boto3.client("ss' + 'm")',
            "a boto3 client in single quotes": "session.client(service_name='ss" + "m')",
            "a client name": "ss" + "m_client = make()",
            "the parameter path": 'name = "/aws/serv' + 'ice/canonical/ubuntu/server/24.04"',
            "another parameter path": "/aws/serv" + "ice/ami-amazon-linux-latest/x",
            "a capital name": "SS" + "M_PARAMETER = 1",
        }
        for label, line in forms.items():
            with self.subTest(form=label):
                self.assertEqual(["f:2"], hits({"f": "first\n" + line + "\n"}, SSM_NAMES))

    def test_words_that_only_contain_the_letters_name_no_use(self) -> None:
        files = {"a": "a dessmount, a classmethod, a MESSMATE and a bypassmark name no grant\n", "b": "assumed role, assembly, ssh and ssl\n"}
        self.assertEqual([], hits(files, SSM_NAMES))

    def test_no_text_file_of_the_repository_uses_the_parameter_store(self) -> None:
        files = scanned_files(ROOT, skip=(GUARD,))
        self.assertIn("infra/modules/host/main.tf", files)
        self.assertIn("scripts/infra/host.py", files)
        self.assertEqual([], hits(files, SSM_NAMES))

    def test_no_policy_template_is_named_for_the_parameter_store(self) -> None:
        self.assertEqual([], sorted(path.name for path in Path(ROOT, "infra/iam/policies").glob("*ssm*")))

    def test_the_host_reads_its_image_from_the_pin_for_the_run_region(self) -> None:
        main = (ROOT / "infra/main.tf").read_text()
        self.assertRegex(main, r'ubuntu_build\s*=\s*jsondecode\(file\("\$\{path\.module\}/ubuntu_build\.json"\)\)')
        self.assertRegex(main, r'\bubuntu_ami\s*=\s*lookup\(local\.ubuntu_build\.amis, var\.aws_region, ""\)')
        self.assertRegex(main, r"\bami\s*=\s*local\.ubuntu_ami\b")


class PlantedUseIsFoundTest(unittest.TestCase):
    """The scan runs over a real Git repository: a plant in a launcher script or a YAML file, tracked or new, names its file and line. The plants sit in a
    temporary repository, so the real one is never touched."""

    def setUp(self) -> None:
        home = tempfile.TemporaryDirectory(prefix="ubuntu_build_plants_")
        self.addCleanup(home.cleanup)
        self.root = Path(home.name)
        git_in(self.root, "init", "--quiet", "--initial-branch", "main")
        self.write(PIN, '{"serial": "20260101", "amis": {"us-west-2": "ami-' + '0123456789abcdef0"}}\n')
        self.write("scripts/infra/launcher.py", "import boto3\n\nclient = boto3.client('ssm')\n")
        self.write("ansible/group_vars/all.yml", "image: ami-" + "0aaaaaaaaaaaaaaaa\nserial: '20260101'\nlookup: aws ssm get-parameter\n")
        git_in(self.root, "add", "--", PIN, "scripts/infra/launcher.py")
        self.write("scripts/new_script.sh", "aws ssm get-parameter --name /x\n")

    def write(self, name: str, text: str) -> None:
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def test_a_parameter_store_use_in_a_tracked_script_a_new_script_or_a_yaml_file_is_found_by_file_and_line(self) -> None:
        self.assertEqual(
            ["ansible/group_vars/all.yml:3", "scripts/infra/launcher.py:3", "scripts/new_script.sh:1"],
            hits(scanned_files(self.root), SSM_NAMES),
        )

    def test_a_build_name_in_a_yaml_file_is_found_and_the_pin_itself_is_left_out(self) -> None:
        self.assertEqual(
            ["ansible/group_vars/all.yml:1", "ansible/group_vars/all.yml:2"], hits(scanned_files(self.root), build_patterns(pinned_serial(self.root)))
        )

    def test_a_file_of_the_skip_list_is_left_out_and_a_binary_or_ignored_file_is_not_read(self) -> None:
        (self.root / "blob.bin").write_bytes(b"\xff\xfe aws ssm \x00")
        self.write(".gitignore", "ignored.txt\n")
        self.write("ignored.txt", "aws ssm get-parameter\n")
        files = scanned_files(self.root, skip=("scripts/new_script.sh",))
        self.assertNotIn("scripts/new_script.sh", files)
        self.assertNotIn("blob.bin", files)
        self.assertNotIn("ignored.txt", files)
        self.assertNotIn(PIN, files)
        self.assertIn("scripts/infra/launcher.py", files)


if __name__ == "__main__":
    unittest.main()
