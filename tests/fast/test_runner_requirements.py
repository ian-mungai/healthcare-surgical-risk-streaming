"""The Python packages of the native host runners are in the lock files that the runner checkout installs.

The runner folder's environment is built from ``runner_checkout_requirements`` (``ansible/group_vars/all.yml``) with ``--require-hashes``. A runner that imports
a package that no listed lock file pins fails on the host, after the deploy has finished. The four host runners (``tests/e2e/silver_signals.py``,
``silver_hypotension.py``, ``gold_dimensions.py`` and ``stream_host.py``) are untracked, so the check of them runs where they are on disk and is skipped
elsewhere; the tracked modules that every runner shares are checked everywhere.

The chosen set is the root ``requirements.txt`` alone. The host process of these runners imports PyYAML (``scripts/stack/yaml_text.py``) and, through the topic
parser that ``stream_host.py`` loads by path, ansible-core (``kafka_topics.py`` imports ``AnsibleModule``). Both are in the root lock. The packages of the other
lock files (``spark/``, ``replay/`` and ``orchestration/``: vitaldb, confluent-kafka, fastavro, boto3, pyarrow, Dagster) belong to the code that the runners
start in containers, and no module of the host process imports them, so the host does not install them and needs no lock file besides the root's.

Failure modes covered here:

- A runner or a module that it imports takes a package that no installed lock file pins (a Spark, Avro or Kafka client added to the host process).
- An import inside a function, an ``if`` or a ``try`` is not seen because only the top of a file is read.
- An import through a package's ``__init__`` or a name that is a submodule (``from spark.silver import tables``) is not followed.
- A module that is loaded by path or by name at run time is not found by the static read, so a package it needs is missed.
- A repository module that has no file is not reported, so the read stops silently.
- A lock file that is listed for the host does not exist, or pins a requirement without a hash.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tests.fast.ansible_paths import ROLES, group_variables
from tests.fast.runner_imports import closure, locked_distributions, unlocked

ROOT = Path(__file__).resolve().parents[2]
HOST_RUNNERS = ("tests/e2e/silver_signals.py", "tests/e2e/silver_hypotension.py", "tests/e2e/gold_dimensions.py", "tests/e2e/stream_host.py")
# The tracked modules that the host runners share, and the module that stream_host.py loads by its path.
SHARED = ("tests/e2e/run_record.py", "tests/e2e/commit_guards.py", "tests/e2e/deadline.py")
LOADED_BY_PATH = {"tests/e2e/stream_host.py": ("ansible/roles/compose_stack/library/kafka_topics.py",)}
LOCK_PACKAGES = {"yaml": "pyyaml", "ansible": "ansible-core"}
# The files that load a module at run time by a path or a name, so that a new one is read by a person.
DYNAMIC = {"tests/e2e/stream_host.py", "ansible/roles/compose_stack/library/kafka_topics.py"} - {"ansible/roles/compose_stack/library/kafka_topics.py"}


def installed_locks() -> list[str]:
    """The text of each lock file that the role installs; a listed file that is missing fails here."""
    return [(ROOT / name).read_text() for name in group_variables()["runner_checkout_requirements"]]


def entry_files() -> list[Path]:
    paths = [ROOT / name for name in SHARED]
    paths += [ROOT / name for names in LOADED_BY_PATH.values() for name in names]
    return paths


class RealRunnersTest(unittest.TestCase):
    def test_every_third_party_package_of_the_shared_modules_and_the_loaded_topic_parser_is_pinned_by_an_installed_lock_file(self) -> None:
        found = closure(ROOT, entry_files())
        self.assertEqual([], found.missing)
        self.assertEqual({"yaml", "ansible"}, set(found.third_party), "the host process's packages; a new one is a decision about the lock files")
        self.assertEqual([], unlocked(found.third_party, locked_distributions(installed_locks())))

    def test_every_third_party_package_of_each_host_runner_that_is_on_disk_is_pinned_by_an_installed_lock_file(self) -> None:
        present = [ROOT / name for name in HOST_RUNNERS if (ROOT / name).is_file()]
        if not present:
            self.skipTest("the host runners are untracked and none is on disk here")
        locked = locked_distributions(installed_locks())
        for runner in present:
            with self.subTest(runner=runner.name):
                found = closure(ROOT, [runner, *entry_files()])
                self.assertEqual([], found.missing)
                self.assertEqual([], unlocked(found.third_party, locked))
                self.assertLessEqual(set(found.third_party), set(LOCK_PACKAGES), "only PyYAML and ansible-core are expected in the host process")

    def test_a_host_runner_that_loads_a_module_at_run_time_is_one_that_this_test_lists_with_the_module_it_loads(self) -> None:
        for name in HOST_RUNNERS:
            if not (ROOT / name).is_file():
                continue
            with self.subTest(runner=name):
                found = closure(ROOT, [ROOT / name])
                self.assertEqual({name} if name in LOADED_BY_PATH else set(), {item for item in found.dynamic if item.startswith("tests/e2e/")})


class ChosenSetTest(unittest.TestCase):
    def test_the_host_environment_is_built_from_the_root_lock_alone_and_the_container_locks_are_not_installed(self) -> None:
        self.assertEqual(["requirements.txt"], group_variables()["runner_checkout_requirements"])

    def test_every_pinned_requirement_of_an_installed_lock_file_has_a_hash(self) -> None:
        for name in group_variables()["runner_checkout_requirements"]:
            with self.subTest(name=name):
                text = (ROOT / name).read_text()
                entries = [line for line in text.splitlines() if line and not line.startswith((" ", "#"))]
                self.assertGreater(len(entries), 20)
                for line in entries:
                    start = text.index(line)
                    block = text[start : text.find("\n", text.find("--hash=", start))]
                    self.assertIn("--hash=sha256:", block, line)

    def test_the_container_lock_files_pin_the_packages_that_the_host_process_does_not_import(self) -> None:
        host = locked_distributions(installed_locks())
        containers = locked_distributions([(ROOT / folder / "requirements.txt").read_text() for folder in ("spark", "replay")])
        self.assertTrue({"vitaldb", "confluent-kafka", "fastavro", "boto3", "pyarrow"} <= containers)
        self.assertTrue({"pyyaml", "ansible-core"} <= host)
        self.assertEqual(set(), {"vitaldb", "fastavro", "pyarrow", "great-expectations"} & host)

    def test_the_role_installs_every_file_it_is_given(self) -> None:
        text = (ROLES / "runner_checkout" / "tasks" / "environment.yml").read_text()
        self.assertIn("runner_checkout_requirements", text)


class ReaderTest(unittest.TestCase):
    """The read itself, on a small tree: each failure mode above has a file that breaks it and one that does not."""

    def tree(self, files: dict[str, str]) -> Path:
        folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="imports_")))
        for name, text in files.items():
            (folder / name).parent.mkdir(parents=True, exist_ok=True)
            (folder / name).write_text(text)
        return folder

    def third(self, files: dict[str, str], entry: str = "tests/run.py") -> dict[str, set[str]]:
        root = self.tree(files)
        found = closure(root, [root / entry])
        self.assertEqual([], found.missing)
        return {name: set(places) for name, places in found.third_party.items()}

    BASE = {"scripts/__init__.py": "", "tests/__init__.py": ""}

    def test_a_package_that_a_runner_takes_at_the_top_is_found_with_its_file_and_the_standard_library_and_the_repository_are_not(self) -> None:
        files = {**self.BASE, "tests/run.py": "import os\nimport vitaldb\nfrom scripts import tool\n", "scripts/tool.py": "import json\n"}
        self.assertEqual({"vitaldb": {"tests/run.py"}}, self.third(files))

    def test_a_package_that_a_module_takes_inside_a_function_an_if_or_a_try_is_found_through_the_chain_of_imports(self) -> None:
        files = {
            **self.BASE,
            "tests/run.py": "from scripts.tool import go\n",
            "scripts/tool.py": "def go():\n    if True:\n        try:\n            import boto3\n        except ImportError:\n            import fastavro\n",
        }
        self.assertEqual({"boto3": {"scripts/tool.py"}, "fastavro": {"scripts/tool.py"}}, self.third(files))

    def test_a_package_that_a_submodule_or_a_package_init_takes_is_found(self) -> None:
        files = {
            **self.BASE,
            "tests/run.py": "from scripts import inner\n",
            "scripts/inner.py": "import fastavro\n",
            "spark/__init__.py": "import pyarrow\n",
            "scripts/other.py": "import spark.silver.tables\n",
            "spark/silver/__init__.py": "",
            "spark/silver/tables.py": "",
        }
        self.assertEqual({"fastavro": {"scripts/inner.py"}}, self.third(files))
        self.assertEqual({"pyarrow": {"spark/__init__.py"}}, self.third(files, "scripts/other.py"))

    def test_a_module_that_is_loaded_by_path_or_by_name_is_marked_dynamic(self) -> None:
        for call in ("importlib.util.spec_from_file_location('n', p)", "importlib.import_module(name)", "__import__(name)"):
            with self.subTest(call=call):
                root = self.tree({**self.BASE, "tests/run.py": f"import importlib.util\nname = p = 'x'\n{call}\n"})
                self.assertEqual({"tests/run.py"}, closure(root, [root / "tests/run.py"]).dynamic)

    def test_a_repository_module_with_no_file_is_reported(self) -> None:
        root = self.tree({**self.BASE, "tests/run.py": "from scripts.gone import x\n"})
        self.assertEqual(["tests/run.py: scripts.gone has no file"], closure(root, [root / "tests/run.py"]).missing)

    def test_a_package_is_unlocked_when_no_lock_file_pins_its_distribution_or_no_distribution_is_known(self) -> None:
        found = {"yaml": {"a.py"}, "vitaldb": {"b.py"}, "mystery": {"c.py"}}
        providers = {"yaml": ["PyYAML"], "vitaldb": ["vitaldb"]}
        locked = locked_distributions(["PyYAML==6.0.3 \\\n    --hash=sha256:00\n", "other[extra]==1.0 \\\n    --hash=sha256:11\n"])
        self.assertEqual({"pyyaml", "other"}, locked)
        problems = unlocked(found, locked, providers)
        self.assertEqual(2, len(problems))
        self.assertRegex(problems[0], r"^mystery: no distribution is known")
        self.assertRegex(problems[1], r"^vitaldb: .*not in the lock files \(imported by b\.py\)")

    def test_the_names_of_the_lock_are_compared_in_the_form_that_pip_compares(self) -> None:
        self.assertEqual({"ansible-core", "typing-extensions"}, locked_distributions(["Ansible_Core==2.22.0 \\\n", "typing.extensions==4.0\n"]))


if __name__ == "__main__":
    unittest.main()
