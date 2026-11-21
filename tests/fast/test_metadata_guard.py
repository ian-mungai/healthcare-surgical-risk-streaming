"""The metadata guard: the table that the host loads, its refresh when an allowed unit starts again and the playbook's check that it is loaded.

The script runs here as a real process, with a fake ``nft`` that records the ruleset it is given and a folder tree that
stands in for the cgroup tree. The role's check runs through the real ``ansible-playbook``. Whether the kernel drops a
workload's packets, and whether Docker, k3s and Tailscale still work, is proven on the live host.
"""

from __future__ import annotations

import stat
import tempfile
import unittest
from pathlib import Path

from scripts.process import run_command
from scripts.project_config import ROOT
from tests.fast.ansible_support import load_list, role_order, run_task_file

ROLE = ROOT / "ansible" / "roles" / "metadata_guard"
SCRIPT = ROLE / "files" / "metadata_guard.sh"
GROUP_VARIABLES = ROOT / "ansible" / "group_vars" / "all.yml"
ADDRESS = "169.254.169.254"
NAMED_UNITS = {
    "workload-credentials.service",
    "tailscaled.service",
    "tailscale-join.service",
    "cloud-init-local.service",
    "cloud-init.service",
    "cloud-init-main.service",
    "cloud-config.service",
    "cloud-final.service",
}
FAKE_NFT = (
    '#!/bin/sh\ncat > "$FAKE_NFT_INPUT"\necho call >> "$FAKE_NFT_CALLS"\n'
    'if [ -n "${FAKE_NEW_CGROUP:-}" ]; then mkdir -p "$FAKE_NEW_CGROUP"; fi\nexit "${FAKE_NFT_EXIT:-0}"\n'
)


def load_script(units_lines: list[str], existing: list[str], nft_exit: int = 0, starts_during_load: str = "") -> tuple[int, str, str]:
    """Run the script with the given allowed-units file and the given unit cgroups present. Return its exit code, the ruleset it gave nft and its stderr."""
    with tempfile.TemporaryDirectory() as scratch:
        folder = Path(scratch)
        (folder / "nft").write_text(FAKE_NFT)
        (folder / "nft").chmod(stat.S_IRWXU)
        units_file = folder / "allowed_units"
        units_file.write_text("\n".join(units_lines) + "\n")
        cgroups = folder / "cgroup"
        for unit in existing:
            (cgroups / "system.slice" / unit).mkdir(parents=True)
        environment = {
            "PATH": f"{folder}:/usr/bin:/bin",
            "FAKE_NFT_INPUT": str(folder / "ruleset"),
            "FAKE_NFT_CALLS": str(folder / "calls"),
            "FAKE_NFT_EXIT": str(nft_exit),
            "FAKE_NEW_CGROUP": str(cgroups / "system.slice" / starts_during_load) if starts_during_load else "",
        }
        result = run_command("bash", [str(SCRIPT), str(units_file), str(cgroups)], env=environment, timeout=30)
        ruleset = (folder / "ruleset").read_text() if (folder / "ruleset").exists() else ""
        return result.returncode, ruleset, result.stderr


def chain(ruleset: str, name: str) -> list[str]:
    """The lines of one chain, without its braces."""
    start = ruleset.index(f"chain {name} {{")
    return [line.strip() for line in ruleset[start : ruleset.index("}", start)].splitlines()[1:] if line.strip()]


def accept_line(unit: str) -> str:
    return f'ip daddr {ADDRESS} socket cgroupv2 level 2 "system.slice/{unit}" accept'


class RulesetTest(unittest.TestCase):
    def test_each_running_allowed_unit_is_accepted_in_the_output_hook_before_the_drop(self) -> None:
        code, ruleset, _ = load_script(["tailscaled.service", "workload-credentials.service"], ["tailscaled.service", "workload-credentials.service"])
        self.assertEqual(0, code)
        output = chain(ruleset, "output")
        self.assertEqual("type filter hook output priority filter; policy accept;", output[0])
        self.assertEqual([accept_line("tailscaled.service"), accept_line("workload-credentials.service"), f"ip daddr {ADDRESS} drop"], output[1:])

    def test_forwarded_traffic_to_the_address_is_dropped_with_no_exception(self) -> None:
        _, ruleset, _ = load_script(["tailscaled.service"], ["tailscaled.service"])
        forward = chain(ruleset, "forward")
        self.assertEqual(["type filter hook forward priority filter; policy accept;", f"ip daddr {ADDRESS} drop"], forward)

    def test_a_unit_that_is_not_running_gets_no_rule(self) -> None:
        _, ruleset, _ = load_script(["tailscaled.service", "workload-credentials.service"], ["tailscaled.service"])
        self.assertNotIn("workload-credentials.service", ruleset)
        self.assertIn(accept_line("tailscaled.service"), ruleset)

    def test_with_no_allowed_unit_running_everything_is_dropped(self) -> None:
        code, ruleset, _ = load_script(["tailscaled.service"], [])
        self.assertEqual(0, code)
        self.assertEqual([f"ip daddr {ADDRESS} drop"], chain(ruleset, "output")[1:])

    def test_a_running_unit_that_is_not_allowed_gets_no_rule(self) -> None:
        _, ruleset, _ = load_script(["tailscaled.service"], ["tailscaled.service", "docker.service"])
        self.assertNotIn("docker.service", ruleset)

    def test_the_table_is_replaced_whole_so_a_reload_leaves_no_old_rule(self) -> None:
        _, ruleset, _ = load_script(["tailscaled.service"], ["tailscaled.service"])
        lines = ruleset.splitlines()
        self.assertEqual(["table inet metadata_guard", "delete table inet metadata_guard", "table inet metadata_guard {"], lines[:3])

    def test_comments_and_blank_lines_in_the_units_file_are_ignored(self) -> None:
        code, ruleset, _ = load_script(["# the credential helper", "", "tailscaled.service"], ["tailscaled.service"])
        self.assertEqual(0, code)
        self.assertEqual([accept_line("tailscaled.service"), f"ip daddr {ADDRESS} drop"], chain(ruleset, "output")[1:])

    def test_a_name_that_could_change_the_ruleset_is_refused_before_nft_runs(self) -> None:
        for name in ('x.service" accept #', "a b.service", "../docker.service", "tailscaled.socket", "x.service;drop"):
            with self.subTest(name=name):
                code, ruleset, stderr = load_script([name], [])
                self.assertEqual(2, code)
                self.assertEqual("", ruleset)
                self.assertIn("refusing the unit name", stderr)

    def test_a_unit_that_starts_while_the_table_loads_is_in_the_table_that_stays(self) -> None:
        code, ruleset, _ = load_script(
            ["tailscaled.service", "workload-credentials.service"], ["tailscaled.service"], starts_during_load="workload-credentials.service"
        )
        self.assertEqual(0, code)
        self.assertEqual(
            [accept_line("tailscaled.service"), accept_line("workload-credentials.service"), f"ip daddr {ADDRESS} drop"], chain(ruleset, "output")[1:]
        )

    def test_a_failure_of_nft_is_a_failure_of_the_script(self) -> None:
        code, _, _ = load_script(["tailscaled.service"], ["tailscaled.service"], nft_exit=1)
        self.assertNotEqual(0, code)

    def test_a_missing_units_file_is_a_failure(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            result = run_command("bash", [str(SCRIPT), str(Path(scratch) / "absent")], env={"PATH": "/usr/bin:/bin"}, timeout=30)
        self.assertNotEqual(0, result.returncode)


class RoleContractTest(unittest.TestCase):
    """The allowed units are the contract: the credential helper, Tailscale and cloud-init, and the units that the project's other files create."""

    def test_the_allowed_units_are_exactly_the_named_services(self) -> None:
        units = load_list(GROUP_VARIABLES, "metadata_guard_allowed_units")
        self.assertEqual(sorted(NAMED_UNITS), sorted(units))

    def test_the_credential_helper_and_the_join_unit_exist_under_the_allowed_names(self) -> None:
        helper = (ROOT / "ansible" / "roles" / "workload_access" / "tasks" / "main.yml").read_text()
        join = (ROOT / "infra" / "modules" / "host" / "tailscale_join.sh.tftpl").read_text()
        self.assertIn("/etc/systemd/system/workload-credentials.service", helper)
        self.assertIn("/etc/systemd/system/tailscale-join.service", join)

    def test_the_guard_runs_before_the_roles_that_start_workloads(self) -> None:
        roles = role_order("host.yml")
        self.assertEqual(["metadata_guard", "docker", "k3s", "workload_access", "unlock_key", "compose_stack"], roles)


RUNTIME_UNITS = ["docker.service", "containerd.service", "k3s.service"]
DROPIN_NAME = "metadata_guard_required.conf"
DROPIN_TEXT = "[Unit]\nRequires=metadata-guard.service\nAfter=metadata-guard.service\n"


class RuntimeDependencyTest(unittest.TestCase):
    """A runtime starts only after the guard loaded: a guard that failed at boot leaves Docker, containerd and k3s stopped.

    The drop-ins are written by the role's own task file through the real ``ansible-playbook``, into a scratch folder. That systemd
    refuses to start the runtimes when the guard unit fails is proven on the live host.
    """

    def write_dropins(self, folder: Path) -> tuple[int, str]:
        units = load_list(GROUP_VARIABLES, "metadata_guard_runtime_units")
        result = run_task_file(
            "metadata_guard", "runtime_dependency.yml", {"metadata_guard_unit_directory": str(folder), "metadata_guard_runtime_units": units}
        )
        return result.returncode, result.stdout + result.stderr

    def test_the_runtimes_are_docker_containerd_and_k3s(self) -> None:
        self.assertEqual(RUNTIME_UNITS, load_list(GROUP_VARIABLES, "metadata_guard_runtime_units"))

    def test_each_runtime_gets_a_drop_in_that_requires_and_follows_the_guard(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            code, output = self.write_dropins(Path(scratch))
            self.assertEqual(0, code, output)
            for unit in RUNTIME_UNITS:
                with self.subTest(unit=unit):
                    dropin = Path(scratch) / f"{unit}.d" / DROPIN_NAME
                    self.assertEqual(DROPIN_TEXT, dropin.read_text())
                    self.assertEqual(0o644, stat.S_IMODE(dropin.stat().st_mode))
            self.assertEqual(sorted(f"{unit}.d" for unit in RUNTIME_UNITS), sorted(path.name for path in Path(scratch).iterdir()))

    def test_a_second_run_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            self.write_dropins(Path(scratch))
            code, output = self.write_dropins(Path(scratch))
        self.assertEqual(0, code, output)
        self.assertRegex(output, r"changed=0\b")

    def test_the_role_writes_them_and_reloads_systemd_when_they_change(self) -> None:
        tasks = (ROLE / "tasks" / "main.yml").read_text()
        self.assertIn("ansible.builtin.include_tasks: runtime_dependency.yml", tasks)
        self.assertIn("metadata_guard_runtime_dropins is changed", tasks)
        self.assertLess(tasks.index("runtime_dependency.yml"), tasks.index("Enable the metadata guard at boot"))


LOADED = """table inet metadata_guard {
\tchain output {
\t\ttype filter hook output priority filter; policy accept;
\t\tip daddr 169.254.169.254 socket cgroupv2 level 2 "system.slice/tailscaled.service" accept
\t\tip daddr 169.254.169.254 drop
\t}

\tchain forward {
\t\ttype filter hook forward priority filter; policy accept;
\t\tip daddr 169.254.169.254 drop
\t}
}
"""


def drop_other_address(chain_name: str, other: str) -> str:
    """The loaded table with the drop of one chain aimed at another address."""
    start = LOADED.index(f"chain {chain_name} {{")
    end = LOADED.index("}", start)
    changed = LOADED[start:end].replace("ip daddr 169.254.169.254 drop", f"ip daddr {other} drop")
    return LOADED[:start] + changed + LOADED[end:]


def verdict(listing: str, *, rc: int = 0) -> tuple[int, str]:
    result = run_task_file("metadata_guard", "verify_loaded.yml", {"metadata_guard_listing": {"rc": rc, "stdout": listing}})
    return result.returncode, result.stdout + result.stderr


class LoadedCheckTest(unittest.TestCase):
    MESSAGE = "The metadata guard is not loaded"

    def test_a_table_with_both_drops_passes(self) -> None:
        code, output = verdict(LOADED)
        self.assertEqual(0, code, output)

    def test_an_absent_table_is_refused(self) -> None:
        code, output = verdict("", rc=1)
        self.assertNotEqual(0, code)
        self.assertIn(self.MESSAGE, output)

    def test_a_table_without_the_forward_drop_is_refused(self) -> None:
        code, output = verdict(
            LOADED.replace(
                "\t\ttype filter hook forward priority filter; policy accept;\n\t\tip daddr 169.254.169.254 drop\n",
                "\t\ttype filter hook forward priority filter; policy accept;\n",
            )
        )
        self.assertNotEqual(0, code)
        self.assertIn(self.MESSAGE, output)

    def test_a_table_without_the_output_drop_is_refused(self) -> None:
        code, output = verdict(LOADED.replace("\t\tip daddr 169.254.169.254 drop\n\t}\n\n\tchain forward", "\t}\n\n\tchain forward"))
        self.assertNotEqual(0, code)
        self.assertIn(self.MESSAGE, output)

    def test_a_listing_that_failed_is_refused_whatever_it_holds(self) -> None:
        code, output = verdict(LOADED, rc=1)
        self.assertNotEqual(0, code)
        self.assertIn(self.MESSAGE, output)

    def test_a_chain_that_is_not_hooked_is_refused(self) -> None:
        for hook in ("output", "forward"):
            with self.subTest(chain=hook):
                code, output = verdict(LOADED.replace(f"type filter hook {hook} priority filter; policy accept;", ""))
                self.assertNotEqual(0, code)
                self.assertIn(self.MESSAGE, output)

    def test_a_chain_that_drops_another_address_is_refused(self) -> None:
        for chain_name in ("output", "forward"):
            for other in ("169.254.169.253", "169x254x169x254", "169.254.169.2540"):
                with self.subTest(chain=chain_name, address=other):
                    code, output = verdict(drop_other_address(chain_name, other))
                    self.assertNotEqual(0, code)
                    self.assertIn(self.MESSAGE, output)


if __name__ == "__main__":
    unittest.main()
