"""The Spark image holds the packages and the project modules that the Bronze decoder and the streaming job import, and installs the packages only from a lock.

Failure modes covered here:

- A module that the decoder imports, directly or through another, or the tracked release pin that the ingestion gate reads, is left out of the image or of the
  build context, so the job stops at start and only a build and a run would show it. The imports are followed with ``ast`` from the decoder's own sources.
- The image installs a package from an unpinned name, without the hash of each file, from a source file that needs a build step or with the dependencies chosen
  at build time, so the platform changes with the day of the build.
- The Spark image's lock holds another version of a package than the replay's lock, so the decoder reads a file with a library that the replay did not run on.
- A package that the replay also pins (the decoder's two and the Avro reader) is pinned at another version than the replay's, which its acceptance run proved.
- The streaming job's two Spark packages (pyarrow and protobuf) are left out of the lock or not pinned.

The wheels for Python 3.14 on linux/amd64 and the hashes are proved by the build of the image in ``tests/e2e/bronze_decode.py``.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

from tests.fast.test_replay_image import ignore_rules, ignored, imported_files, landed_at

ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = ROOT / "spark" / "Dockerfile"
IGNORE_FILE = ROOT / "spark" / "Dockerfile.dockerignore"
REQUIREMENTS_IN = ROOT / "spark" / "requirements.in"
LOCK = ROOT / "spark" / "requirements.txt"
REPLAY_IN = ROOT / "replay" / "requirements.in"
REPLAY_LOCK = ROOT / "replay" / "requirements.txt"
JOBS = "/opt/jobs"
SCHEMAS = ("numeric", "waveform", "string_event", "case_lifecycle", "feature_ready")
READ_FILES = ("tests/fixtures/vitaldb/release.json", *(f"replay/events/schemas/{name}.avsc" for name in SCHEMAS))
PACKAGES = ("vitaldb", "boto3", "fastavro")
STREAM_ONLY_PACKAGES = ("pyarrow", "protobuf")
CHECK_PACKAGES = {"great-expectations": "1.24.1", "marshmallow": "4.3.1", "narwhals": "2.28.0", "pydantic": "2.14.3", "pydantic-core": "2.50.4"}
INSTALL_FLAGS = ("--require-hashes", "--only-binary :all:", "--no-deps")
LOCK_ENTRY = re.compile(r"^([A-Za-z0-9_.-]+)==(\S+) \\$", re.MULTILINE)


def bronze_sources() -> list[str]:
    return sorted(str(path.relative_to(ROOT)) for path in (ROOT / "spark" / "bronze").glob("*.py")) + ["spark/jobs/bronze_decode.py"]


def stream_sources() -> list[str]:
    return sorted(str(path.relative_to(ROOT)) for path in (ROOT / "spark" / "streaming").glob("*.py")) + ["spark/jobs/stream_features.py"]


def copied_pairs(dockerfile: str) -> list[tuple[str, str]]:
    pairs = []
    for line in dockerfile.replace("\\\n", " ").splitlines():
        words = line.split()
        if words[:1] == ["COPY"] and not any(word.startswith("--from=") for word in words):
            pairs += [(source.rstrip("/"), words[-1]) for source in words[1:-1]]
    return pairs


def image_problems(dockerfile: str, rules: list[str], needed: set[str]) -> list[str]:
    """Each needed file that no ``COPY`` line puts at its own path below ``/opt/jobs``, or that the build's ignore file leaves out."""
    found = []
    pairs = copied_pairs(dockerfile)
    for path in sorted(needed):
        destinations = [landed_at(path, source, destination) for source, destination in pairs if path == source or path.startswith(source + "/")]
        if f"{JOBS}/{path}" not in destinations:
            found.append(f"{path}: no COPY line puts it at {JOBS}/{path}")
        elif ignored(path, rules):
            found.append(f"{path}: the build's ignore file leaves it out")
    return found


def install_problems(dockerfile: str) -> list[str]:
    """What is wrong with the line that installs the locked packages: it must exist, name the lock, carry each flag and run before the image's user is set."""
    lines = [line.strip() for line in dockerfile.replace("\\\n", " ").splitlines() if line.strip() and not line.lstrip().startswith("#")]
    installs = [number for number, line in enumerate(lines) if line.startswith("RUN") and "pip install" in line]
    if len(installs) != 1:
        return [f"the image has {len(installs)} pip install lines, not one"]
    line = lines[installs[0]]
    found = [f"the pip install line lacks {flag}" for flag in INSTALL_FLAGS if flag not in line]
    if "--requirement /tmp/requirements.txt" not in line:
        found.append("the pip install line does not install /tmp/requirements.txt")
    if "COPY spark/requirements.txt /tmp/requirements.txt" not in lines[: installs[0]]:
        found.append("the lock is not copied to /tmp/requirements.txt before it is installed")
    if any(line.startswith("USER ") for line in lines[: installs[0]]):
        found.append("the packages are installed after the user is set")
    return found


def lock_problems(text: str) -> list[str]:
    """Each package of a lock that is not pinned to one version or has no hash."""
    entries: list[list[object]] = []
    for line in text.splitlines():
        if line and not line[0].isspace() and not line.startswith("#"):
            entries.append([line, False])
        elif entries and "--hash=sha256:" in line:
            entries[-1][1] = True
    found = []
    for head, hashed in entries:
        name = re.match(r"[A-Za-z0-9_.-]+", str(head))
        label = name.group(0) if name else str(head)
        if not re.fullmatch(r"[A-Za-z0-9_.-]+==\S+( \\)?", str(head)):
            found.append(f"{label}: not pinned to one version")
        elif not hashed:
            found.append(f"{label}: no hash")
    return found


def locked(text: str) -> dict[str, str]:
    return {name.lower(): version for name, version in re.findall(r"^([A-Za-z0-9_.-]+)==(\S+)", text, re.MULTILINE)}


def requested(text: str) -> dict[str, str]:
    return {name.lower(): version for name, version in re.findall(r"^([A-Za-z0-9_.-]+)==(\S+)\s*$", text, re.MULTILINE)}


class Image(unittest.TestCase):
    def test_every_module_that_the_decoder_imports_and_the_pin_are_in_the_image(self) -> None:
        needed = imported_files(bronze_sources() + stream_sources()) | set(READ_FILES)
        for path in (
            "replay/service/decode.py",
            "replay/events/waveform.py",
            "scripts/ingestion/gate.py",
            "scripts/sample/candidates.py",
            "scripts/stack/nessie.py",
            "replay/service/wire_codec.py",
            "replay/service/registry.py",
            "replay/events/formats.py",
            "replay/events/wire.py",
            "features/row.py",
        ):
            self.assertIn(path, needed)
        self.assertEqual([], image_problems(DOCKERFILE.read_text(), ignore_rules(IGNORE_FILE.read_text()), needed))

    def test_a_module_or_the_pin_left_out_is_named(self) -> None:
        needed = imported_files(bronze_sources() + stream_sources()) | set(READ_FILES)
        text, rules = DOCKERFILE.read_text(), ignore_rules(IGNORE_FILE.read_text())
        for path in ("replay/service/decode.py", "replay/events/stream.py", "scripts/process.py", "tests/fixtures/vitaldb/release.json"):
            with self.subTest(ignored=path):
                self.assertIn(f"{path}: the build's ignore file leaves it out", image_problems(text, [*rules, path], needed))
            with self.subTest(not_copied=path):
                without = "\n".join(line for line in text.splitlines() if f" {path} " not in line + " ")
                self.assertIn(f"{path}: no COPY line puts it at {JOBS}/{path}", image_problems(without, rules, needed))

    def test_the_image_holds_every_file_of_the_feature_package_so_its_version_is_the_hosts(self) -> None:
        """``spark.streaming.version.feature_version`` is the digest of every ``features/*.py`` file, and the batch path and the acceptance run compute it on
        the host: an image with fewer files gives the job another version and other feature IDs."""
        every = {str(path.relative_to(ROOT)) for path in (ROOT / "features").glob("*.py")}
        self.assertEqual([], image_problems(DOCKERFILE.read_text(), ignore_rules(IGNORE_FILE.read_text()), every))
        text = DOCKERFILE.read_text()
        for path in sorted(every):
            with self.subTest(left_out=path):
                without = "\n".join(line for line in text.splitlines() if f" {path} " not in line + " ")
                self.assertIn(f"{path}: no COPY line puts it at {JOBS}/{path}", image_problems(without, ignore_rules(IGNORE_FILE.read_text()), every))

    def test_the_image_copies_no_replay_module_that_no_job_imports(self) -> None:
        needed = imported_files(bronze_sources() + stream_sources()) | set(READ_FILES)
        for source, _ in copied_pairs(DOCKERFILE.read_text()):
            if source.startswith("replay/"):
                self.assertTrue(any(path == source or path.startswith(source + "/") for path in needed), source)


class Packages(unittest.TestCase):
    def test_the_packages_are_installed_from_the_lock_with_hashes_wheels_only_and_no_dependency_choice(self) -> None:
        self.assertEqual([], install_problems(DOCKERFILE.read_text()))

    def test_a_missing_flag_a_missing_copy_or_a_late_install_is_named(self) -> None:
        text = DOCKERFILE.read_text()
        for flag in INSTALL_FLAGS:
            self.assertIn(f"the pip install line lacks {flag}", install_problems(text.replace(flag, "")))
        self.assertIn(
            "the lock is not copied to /tmp/requirements.txt before it is installed",
            install_problems(text.replace("COPY spark/requirements.txt /tmp/requirements.txt", "")),
        )
        self.assertIn(
            "the packages are installed after the user is set", install_problems(text.replace("RUN pip install", "USER 10100:10100\nRUN pip install"))
        )

    def test_every_package_of_the_lock_is_pinned_and_has_a_hash(self) -> None:
        self.assertEqual([], lock_problems(LOCK.read_text()))
        self.assertGreaterEqual(len(locked(LOCK.read_text())), len(PACKAGES))

    def test_an_unpinned_or_unhashed_package_is_named(self) -> None:
        good = (
            "# comment\nvitaldb==1.7.2 \\\n    --hash=sha256:aa \\\n    --hash=sha256:bb\n    # via -r spark/requirements.in\n"
            "numpy==2.5.5 \\\n    --hash=sha256:cc\n"
        )
        self.assertEqual([], lock_problems(good))
        self.assertEqual(["numpy: no hash"], lock_problems(good.replace("    --hash=sha256:cc\n", "")))
        self.assertEqual(["numpy: not pinned to one version"], lock_problems(good.replace("numpy==2.5.5", "numpy>=2")))
        self.assertEqual(["vitaldb: not pinned to one version"], lock_problems(good.replace("vitaldb==1.7.2", "vitaldb")))

    def test_the_replays_packages_are_pinned_where_the_replay_pins_them(self) -> None:
        source, replay = requested(REQUIREMENTS_IN.read_text()), requested(REPLAY_IN.read_text())
        self.assertEqual({*PACKAGES, *STREAM_ONLY_PACKAGES, *CHECK_PACKAGES}, set(source))
        for name in STREAM_ONLY_PACKAGES:
            self.assertEqual(source[name], locked(LOCK.read_text()).get(name), f"{name} is pinned in the requirements and not at that version in the lock")
        for name in PACKAGES:
            self.assertEqual(replay[name], source[name], name)
            self.assertEqual(replay[name], locked(LOCK.read_text())[name], name)

    def test_the_checks_package_and_the_dependencies_held_back_are_pinned_in_the_input_and_the_lock_at_one_version(self) -> None:
        source, lock = requested(REQUIREMENTS_IN.read_text()), locked(LOCK.read_text())
        for name, version in CHECK_PACKAGES.items():
            self.assertEqual(version, source[name], name)
            self.assertEqual(version, lock[name], name)

    def test_the_checks_packages_are_locked_for_the_hash_checked_wheel_install_and_the_job_imports_them_by_name_only(self) -> None:
        text = LOCK.read_text()
        for name in ("great-expectations", "pydantic", "pydantic-core", "narwhals", "marshmallow"):
            self.assertIn(f"\n{name}=={CHECK_PACKAGES[name]} \\\n    --hash=sha256:", text)
        quality = (ROOT / "spark" / "bronze" / "quality.py").read_text()
        self.assertNotIn("\nimport great_expectations", quality)
        self.assertNotIn("\nfrom great_expectations", quality)

    def test_a_package_that_both_locks_hold_has_one_version(self) -> None:
        ours, replay = locked(LOCK.read_text()), locked(REPLAY_LOCK.read_text())
        shared = ours.keys() & replay.keys()
        self.assertTrue({"vitaldb", "boto3", "numpy", "fastavro"} <= shared)
        self.assertEqual({}, {name: (ours[name], replay[name]) for name in shared if ours[name] != replay[name]})


if __name__ == "__main__":
    unittest.main()
