"""The parts of the host commands that decide before any call: which Tailscale peer is the host and what a run owns.

The commands' calls to AWS, Tailscale, Terraform and Ansible are proven on the live host. These cases cover choices a
live run cannot readily force: a stale peer with the host's name, an offline peer, a peer without the host tag and a
Terraform state with data sources and nested modules.
"""

from __future__ import annotations

import unittest

from scripts.infra.host import HostError, instance_id, select_peer, workload_variables
from scripts.infra.inventory import inventory_entries


def peer(
    name: str, *, online: bool = True, tags: tuple[str, ...] = ("tag:surgical-host",), created: str = "2026-01-02T00:00:00Z", dns: str = ""
) -> dict[str, object]:
    return {"HostName": name, "Online": online, "Tags": list(tags), "Created": created, "DNSName": dns or f"{name}.example.ts.net."}


class SelectPeerTest(unittest.TestCase):
    def test_online_tagged_peer_is_selected(self) -> None:
        status = {"Peer": {"a": peer("platform-dev"), "b": peer("laptop", tags=())}}
        self.assertEqual("platform-dev.example.ts.net", select_peer(status, "platform-dev", "tag:surgical-host"))

    def test_newest_of_two_peers_with_the_name_is_selected(self) -> None:
        old = peer("platform-dev", created="2026-01-01T00:00:00Z", dns="platform-dev.example.ts.net.")
        new = peer("platform-dev", created="2026-01-03T00:00:00Z", dns="platform-dev-1.example.ts.net.")
        self.assertEqual("platform-dev-1.example.ts.net", select_peer({"Peer": {"a": old, "b": new}}, "platform-dev", "tag:surgical-host"))

    def test_offline_peer_is_not_the_host(self) -> None:
        with self.assertRaisesRegex(HostError, "no online peer"):
            select_peer({"Peer": {"a": peer("platform-dev", online=False)}}, "platform-dev", "tag:surgical-host")

    def test_untagged_peer_with_the_name_is_not_the_host(self) -> None:
        with self.assertRaisesRegex(HostError, "no online peer"):
            select_peer({"Peer": {"a": peer("platform-dev", tags=())}}, "platform-dev", "tag:surgical-host")

    def test_empty_status_is_refused(self) -> None:
        with self.assertRaisesRegex(HostError, "no online peer"):
            select_peer({}, "platform-dev", "tag:surgical-host")


STATE = {
    "values": {
        "root_module": {
            "child_modules": [
                {
                    "resources": [
                        {
                            "address": "module.host.aws_instance.host",
                            "mode": "managed",
                            "type": "aws_instance",
                            "values": {"id": "i-0123456789abcdef0", "arn": "arn:aws:ec2:us-west-2:<ACCOUNT>:instance/i-0123456789abcdef0"},
                        },
                        {"address": "module.host.data.aws_vpc.default", "mode": "data", "type": "aws_vpc", "values": {"id": "vpc-0123456789abcdef0"}},
                    ]
                },
                {
                    "resources": [
                        {
                            "address": 'module.storage.aws_s3_bucket.this["landing"]',
                            "mode": "managed",
                            "type": "aws_s3_bucket",
                            "values": {"id": "example-landing"},
                        }
                    ]
                },
            ]
        }
    }
}


class WorkloadVariablesTest(unittest.TestCase):
    def test_each_workload_role_gets_a_group_id_fixed_by_name_order(self) -> None:
        state = {"values": {"outputs": {"workload_role_arns": {"value": {"writer": "arn:writer", "reader": "arn:reader"}}}}}
        expected = [{"name": "reader", "role_arn": "arn:reader", "gid": 20000}, {"name": "writer", "role_arn": "arn:writer", "gid": 20001}]
        self.assertEqual({"workload_access_region": "us-west-2", "workload_access_workloads": expected}, workload_variables(state, "us-west-2"))

    def test_state_without_workloads_gives_an_empty_list(self) -> None:
        self.assertEqual({"workload_access_region": "us-west-2", "workload_access_workloads": []}, workload_variables({"values": {}}, "us-west-2"))


class InventoryTest(unittest.TestCase):
    def test_managed_resources_and_the_state_bucket_are_listed(self) -> None:
        entries = inventory_entries(STATE, "example-state")
        expected = [
            {"address": "module.host.aws_instance.host", "type": "aws_instance", "service_id": "i-0123456789abcdef0"},
            {"address": 'module.storage.aws_s3_bucket.this["landing"]', "type": "aws_s3_bucket", "service_id": "example-landing"},
            {"address": "state bucket", "type": "aws_s3_bucket", "service_id": "example-state"},
        ]
        self.assertEqual(expected, entries)

    def test_the_host_instance_is_read_from_the_state(self) -> None:
        self.assertEqual("i-0123456789abcdef0", instance_id(STATE))

    def test_state_without_a_host_is_refused(self) -> None:
        with self.assertRaisesRegex(HostError, "exactly one host instance"):
            instance_id({"values": {}})

    def test_run_with_local_state_lists_no_state_bucket(self) -> None:
        self.assertEqual([], inventory_entries({"values": {}}, ""))
        self.assertNotIn("state bucket", [entry["address"] for entry in inventory_entries(STATE, "")])

    def test_empty_state_lists_only_the_state_bucket(self) -> None:
        self.assertEqual(
            [{"address": "state bucket", "type": "aws_s3_bucket", "service_id": "example-state"}], inventory_entries({"values": {}}, "example-state")
        )


if __name__ == "__main__":
    unittest.main()
