"""The record of what a run created, written when an approved plan is applied, and the inventory built from it.

Only a resource that an applied plan of this run created is recorded. A resource that was already in the state, or a
state bucket this run did not create, is never listed as created, so the teardown cannot be given it as a target.
"""

from __future__ import annotations

from typing import Any

HOST_PARTS = (("root volume", "aws_ebs_volume"), ("network interface", "aws_network_interface"))


def module_resources(module: dict[str, Any]) -> list[dict[str, Any]]:
    found = list(module.get("resources", []))
    for child in module.get("child_modules", []):
        found += module_resources(child)
    return found


def managed_resources(state: dict[str, Any]) -> list[dict[str, Any]]:
    return [item for item in module_resources(state.get("values", {}).get("root_module", {})) if item["mode"] == "managed"]


def entry(address: str, kind: str, service_id: object) -> dict[str, str]:
    return {"address": address, "type": kind, "service_id": str(service_id)}


def created_entries(plan: dict[str, Any], state: dict[str, Any]) -> list[dict[str, str]]:
    """One entry for each resource the applied plan created, with the host's root volume and network interface."""
    created = {change["address"] for change in plan.get("resource_changes", []) if "create" in change["change"]["actions"]}
    entries = []
    for item in managed_resources(state):
        if item["address"] not in created:
            continue
        values = item["values"]
        entries.append(entry(item["address"], item["type"], values["id"]))
        if item["type"] == "aws_instance":
            parts = (values["root_block_device"][0]["volume_id"], values["primary_network_interface_id"])
            entries += [entry(f"{item['address']} {name}", kind, part) for (name, kind), part in zip(HOST_PARTS, parts, strict=True)]
    return sorted(entries, key=lambda found: found["address"])


def merge_created(recorded: list[dict[str, str]], new: list[dict[str, str]]) -> list[dict[str, str]]:
    """The record after another apply: a new entry replaces a recorded one with the same address."""
    by_address = {found["address"]: found for found in [*recorded, *new]}
    return [by_address[address] for address in sorted(by_address)]


def run_inventory(recorded: list[dict[str, str]], state: dict[str, Any], run: Any) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """What the run created and still holds, with its own state bucket, and the managed resources it did not create."""
    in_state = {(item["address"], str(item["values"]["id"])) for item in managed_resources(state)}
    parents = {item["address"] for item in managed_resources(state)}
    kept = [
        found
        for found in recorded
        if (found["address"], found["service_id"]) in in_state or any(found["address"] == f"{parent} {name}" for parent in parents for name, _ in HOST_PARTS)
    ]
    recorded_addresses = {found["address"] for found in recorded}
    not_created = [entry(item["address"], item["type"], item["values"]["id"]) for item in managed_resources(state) if item["address"] not in recorded_addresses]
    if run.backend == "s3" and run.state_bucket_creation == "created":
        kept.append(entry("state bucket", "aws_s3_bucket", run.state_bucket))
    return kept, sorted(not_created, key=lambda found: found["address"])
