"""No workload in the environment's variable file is granted more than the design declares for it: its prefixes, its modes and the publisher's dataset.

Terraform builds each workload's role from ``workloads`` in ``infra/environments/dev.tfvars``, and its tests check how a role is built from an entry, not
which entries the file holds. So a widened entry, such as a write mode for a reader, would reach the plan unnoticed. ``DECLARED`` is the design's least
privilege for each workload. The file is read by a small reader of the one form it uses, which refuses any other form, so a grant written another way fails
too. The planted cases widen one grant at a time.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path
from typing import Any

from scripts.infra.host import PROJECT_WORKLOAD_GIDS

TFVARS = Path(__file__).resolve().parents[2] / "infra" / "environments" / "dev.tfvars"
# The design's grants: Airbyte lands the copy and reads the publisher; the checksum check reads the copy and writes the manifests; Spark builds and maintains
# the tables; Trino reads them; the replay reads the copy and the manifests; the orchestrator's record load reads the copy and the manifests and makes, reads
# and deletes the versions of its own read copies under records_input.
DECLARED: dict[str, dict[str, Any]] = {
    "airbyte": {"access": {"landing": {"write"}}, "publisher_read": True},
    "ingestion_check": {"access": {"landing": {"read"}, "manifests": {"read", "write"}}, "publisher_read": False},
    "spark": {"access": {"warehouse": {"read", "write", "delete"}}, "publisher_read": False},
    "trino": {"access": {"warehouse": {"read"}}, "publisher_read": False},
    "replay": {"access": {"landing": {"read"}, "manifests": {"read"}}, "publisher_read": False},
    "orchestrator": {"access": {"landing": {"read"}, "manifests": {"read"}, "records_input": {"read", "write", "delete"}}, "publisher_read": False},
}
TOKEN = re.compile(r'\s*(?:(?P<name>[a-z_][a-z0-9_]*)|(?P<text>"[^"\\\n]*")|(?P<mark>[{}\[\]=,]))')


class FormError(ValueError):
    """The text is not the one form of the variable file that the reader knows."""


def tokens(text: str) -> list[str]:
    found, position = [], 0
    text = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
    while position < len(text.rstrip()):
        match = TOKEN.match(text, position)
        if match is None:
            raise FormError(f"unknown text at character {position}")
        found.append(match.group(match.lastgroup or ""))
        position = match.end()
    return found


def value(items: list[str]) -> Any:
    head = items.pop(0)
    if head == "{":
        document: dict[str, Any] = {}
        while items[0] != "}":
            name, equals = items.pop(0), items.pop(0)
            if equals != "=" or not re.fullmatch(r"[a-z_][a-z0-9_]*", name) or name in document:
                raise FormError(f"not one name = value at {name}")
            document[name] = value(items)
            if items[0] == ",":
                items.pop(0)
        items.pop(0)
        return document
    if head == "[":
        listed = []
        while items[0] != "]":
            entry = items.pop(0)
            if not entry.startswith('"'):
                raise FormError("a list holds text only")
            listed.append(entry.strip('"'))
            if items[0] == ",":
                items.pop(0)
        items.pop(0)
        return listed
    if head in ("true", "false"):
        return head == "true"
    if head.startswith('"'):
        return head.strip('"')
    raise FormError(f"unknown value {head}")


def read_workloads(text: str) -> dict[str, Any]:
    """The ``workloads`` setting of a variable file whose every line is one ``name = value`` of the known form, read to its end."""
    items = ["{", *tokens(text), "}"]
    settings = value(items)
    if items:
        raise FormError("text after the last setting")
    document = settings.get("workloads")
    if not isinstance(document, dict):
        raise FormError("the file sets no workloads mapping")
    return document


def excess(workloads: dict[str, Any]) -> list[str]:
    """Each grant of the file beyond the declared one, as ``workload: what``."""
    found = []
    for name, entry in sorted(workloads.items()):
        declared = DECLARED.get(name)
        if declared is None:
            found.append(f"{name}: a workload the design does not declare")
            continue
        unknown = set(entry) - {"access", "publisher_read"}
        if unknown:
            found.append(f"{name}: unknown fields {sorted(unknown)}")
        for prefix, modes in sorted((entry.get("access") or {}).items()):
            wider = set(modes) - declared["access"].get(prefix, set())
            if wider:
                found.append(f"{name}: {prefix} {sorted(wider)} beyond the declared {sorted(declared['access'].get(prefix, set()))}")
        if entry.get("publisher_read", False) and not declared["publisher_read"]:
            found.append(f"{name}: reads the publisher's dataset, which the design does not declare")
    return found


class RealFileTest(unittest.TestCase):
    def test_no_workload_is_granted_more_than_the_design_declares(self) -> None:
        workloads = read_workloads(TFVARS.read_text())
        self.assertEqual(sorted(DECLARED), sorted(workloads))
        self.assertEqual([], excess(workloads))


class HostFolderTest(unittest.TestCase):
    def test_every_declared_workload_has_its_own_fixed_group_so_the_host_makes_its_credential_folder(self) -> None:
        """The host makes ``/run/workload_access/<name>/`` for each workload that Terraform gives a role, with the workload's own group."""
        workloads = read_workloads(TFVARS.read_text())
        self.assertEqual(sorted(workloads), sorted(PROJECT_WORKLOAD_GIDS))
        self.assertEqual(len(PROJECT_WORKLOAD_GIDS), len(set(PROJECT_WORKLOAD_GIDS.values())))


class PlantedGrantTest(unittest.TestCase):
    def planted(self, old: str, new: str) -> list[str]:
        text = TFVARS.read_text()
        self.assertEqual(1, text.count(old))
        return excess(read_workloads(text.replace(old, new)))

    def test_a_wider_mode_another_prefix_the_publisher_or_another_workload_is_named(self) -> None:
        replay = 'replay = {\n    access = { landing = ["read"], manifests = ["read"] }\n  }'
        cases = {
            "a write mode for a reader": (replay, replay.replace('landing = ["read"]', 'landing = ["read", "write"]'), "replay: landing ['write']"),
            "another prefix": (replay, replay.replace('manifests = ["read"]', 'manifests = ["read"], warehouse = ["read"]'), "replay: warehouse ['read']"),
            "the publisher": (replay, replay.replace("}\n  }", "}\n    publisher_read = true\n  }"), "replay: reads the publisher's"),
            "another workload": (replay, replay + '\n  extra = {\n    access = { landing = ["read"] }\n  }', "extra: a workload the design"),
        }
        for label, (old, new, named) in cases.items():
            with self.subTest(label):
                found = self.planted(old, new)
                self.assertEqual(1, len(found), found)
                self.assertTrue(found[0].startswith(named), found)

    def test_a_wider_grant_of_the_orchestrator_is_named(self) -> None:
        block = 'orchestrator = {\n    access = { landing = ["read"], manifests = ["read"], records_input = ["read", "write", "delete"] }\n  }'
        cases = {
            "a write mode on landing": (block.replace('landing = ["read"]', 'landing = ["read", "write"]'), "orchestrator: landing ['write']"),
            "a delete mode on landing": (block.replace('landing = ["read"]', 'landing = ["read", "delete"]'), "orchestrator: landing ['delete']"),
            "a write mode on manifests": (block.replace('manifests = ["read"]', 'manifests = ["read", "write"]'), "orchestrator: manifests ['write']"),
            "the warehouse": (block.replace("records_input =", 'warehouse = ["read"], records_input ='), "orchestrator: warehouse ['read']"),
            "the publisher": (block.replace("}\n  }", "}\n    publisher_read = true\n  }"), "orchestrator: reads the publisher's"),
        }
        for label, (new, named) in cases.items():
            with self.subTest(label):
                found = self.planted(block, new)
                self.assertEqual(1, len(found), found)
                self.assertTrue(found[0].startswith(named), found)

    def test_a_grant_written_in_another_form_is_refused(self) -> None:
        for old, new in (('landing = ["read"], manifests', "landing = local.read, manifests"), ('manifests = ["read"] }\n  }', 'manifests = ["read"] }\n  }}')):
            with self.subTest(new=new), self.assertRaises((FormError, IndexError)):
                text = TFVARS.read_text()
                read_workloads(text.replace(old, new, 1))


if __name__ == "__main__":
    unittest.main()
