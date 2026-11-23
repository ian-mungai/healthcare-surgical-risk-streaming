"""Where a secret goes after its real entry point: a harmless marker is placed, the intended consumer must receive it and no sink may hold it.

Each case runs the real Ansible modules through the real ``ansible-playbook`` against stand-ins for the consumers that need a daemon: a program for ``docker
compose exec`` (PostgreSQL), a program for ``kubectl`` (the cluster) and a small HTTP server for OpenBao. A marker is random for each run and has the form that
its entry point requires. The passing control comes first in each case: the stand-in received the marker through the intended channel (a SCRAM verifier on the
standard input of the exec, a request body to OpenBao, a manifest on the standard input of kubectl), so an empty search cannot pass. Then every sink is searched
for the marker in its plain, encoded and Base64 forms: the playbook's own output, which these cases read raw and before any sanitising, every argument list and
environment that a stand-in saw, and every file under the run's folder but the marker's own file. A second case for each consumer makes it fail with an error
text that repeats its input, and the emitted message must still hold no marker.

Failure modes covered here:

- A module puts a password in a command line, an environment variable, a message or a rendered file.
- A failed call shows the statement or the manifest that it carried, with the password or its Base64 form.
- A refused login shows the secret ID that was sent.
- A file that already holds the owner's value is replaced by a new random one, so the consumer never gets the owner's value.
"""

from __future__ import annotations

import base64
import grp
import importlib.util
import json
import os
import pwd
import re
import sys
import tarfile
import tempfile
import threading
import traceback
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.evidence.manifest import build_manifest
from scripts.process import CompletedProcess, run_command
from scripts.project_config import ROOT, ConfigurationError, load_configuration
from tests.fast.secret_inputs import Marker, listed_names, make_marker, scan_text, scan_tree
from tests.slow.ansible_support import ROLES, SHARED_MODULE_UTILS, environment, shared_module

PLAYBOOK_SECONDS = 180
OWNER = f"{os.getuid()}:{os.getgid()}"
LIBRARIES = [ROLES / name / "library" for name in ("compose_stack", "openbao", "airbyte", "secret_files")]
TOOLS_FILE = ROOT / "compose" / "postgres_tools.txt"
SERVICES_FILE = ROOT / "compose" / "openbao_services.txt"
PYTHON = str(ROOT / ".venv" / "bin" / "python")
VERIFIER = re.compile(r"CREATE ROLE \"(\w+)\" [A-Z ]+ PASSWORD '(SCRAM-SHA-256\$[^']+)'")

DOCKER_STAND_IN = """#!{python}
import json, os, sys
from pathlib import Path
calls = Path(__file__).resolve().parent.parent / "calls"
standard_input = sys.stdin.read()
number = len(list(calls.glob("*.stdin")))
(calls / f"{{number}}.stdin").write_text(standard_input)
with open(calls / "calls.jsonl", "a") as handle:
    handle.write(json.dumps({{"argv": sys.argv, "env": dict(os.environ)}}) + "\\n")
mode = json.loads((calls / "mode.json").read_text())
if mode.get("fail_on") and mode["fail_on"] in standard_input:
    sys.stderr.write("psql:<stdin>:1: ERROR: could not run the statement\\nLINE 1: " + standard_input + "\\n")
    sys.exit(1)
if "pg_authid" in standard_input:
    sys.stdout.write(mode.get("state", ""))
sys.exit(0)
"""

KUBECTL_STAND_IN = """#!{python}
import json, os, sys
from pathlib import Path
calls = Path(__file__).resolve().parent.parent / "calls"
arguments = sys.argv[1:]
standard_input = sys.stdin.read() if "-" in arguments else ""
number = len(list(calls.glob("*.stdin")))
(calls / f"{{number}}.stdin").write_text(standard_input)
with open(calls / "calls.jsonl", "a") as handle:
    handle.write(json.dumps({{"argv": sys.argv, "env": dict(os.environ)}}) + "\\n")
mode = json.loads((calls / "mode.json").read_text())
if arguments[0] == "get":
    sys.stderr.write('Error from server (NotFound): secrets "x" not found\\n')
    sys.exit(1)
if mode.get("fail"):
    sys.stderr.write("error: failed to apply the object: " + standard_input + "\\n")
    sys.exit(1)
sys.exit(0)
"""


class Run:
    """One scratch folder with the secrets folder, the stand-in programs and the record of what they were given."""

    def __init__(self, work: Path) -> None:
        self.work = work
        self.secrets = work / "secrets"
        self.calls = work / "calls"
        self.bin = work / "bin"
        for folder in (self.secrets, self.calls, self.bin):
            folder.mkdir(mode=0o700)
        self.set_mode({})
        for name, body in (("docker", DOCKER_STAND_IN), ("kubectl", KUBECTL_STAND_IN)):
            (self.bin / name).write_text(body.format(python=PYTHON))
            (self.bin / name).chmod(0o755)
        self.owned: dict[str, set[Path]] = {}

    def set_mode(self, mode: dict[str, Any]) -> None:
        (self.calls / "mode.json").write_text(json.dumps(mode))

    def place(self, marker: Marker, relative: str) -> Path:
        """Put the marker in its secret file, as the owner's value, before the deploy runs. Only this file may hold it."""
        path = self.secrets / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(marker.value + "\n")
        path.chmod(0o600)
        self.owned.setdefault(marker.label, set()).add(path)
        return path

    def play(self, tasks: list[dict[str, Any]], variables: dict[str, Any] | None = None) -> CompletedProcess[str]:
        """Run the tasks on localhost through the real ``ansible-playbook`` with the stand-ins first on the path, and return the raw output."""
        playbook = self.work / "play.yml"
        playbook.write_text(json.dumps([{"hosts": "localhost", "connection": "local", "gather_facts": False, "vars": variables or {}, "tasks": tasks}]))
        env = environment(self.work)
        env["PATH"] = f"{self.bin}:{env['PATH']}"
        env["ANSIBLE_LIBRARY"] = ":".join(str(path) for path in LIBRARIES)
        env["ANSIBLE_MODULE_UTILS"] = str(SHARED_MODULE_UTILS)
        return run_command(
            str(ROOT / ".venv" / "bin" / "ansible-playbook"), ["-vvv", "-i", "localhost,", str(playbook)], cwd=self.work, env=env, timeout=PLAYBOOK_SECONDS
        )

    def stand_in_calls(self) -> list[dict[str, Any]]:
        path = self.calls / "calls.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def stand_in_input(self, number: int) -> str:
        return (self.calls / f"{number}.stdin").read_text()

    def inputs(self) -> list[str]:
        return [path.read_text() for path in sorted(self.calls.glob("*.stdin"), key=lambda item: int(item.stem))]

    def sinks(self, markers: list[Marker], output: str, *, extra_text: tuple[str, ...] = ()) -> list[str]:
        """Each sink that holds a marker: the raw playbook output, the stand-ins' arguments and environment, other texts and every file but its own."""
        found: list[str] = []
        calls = json.dumps(self.stand_in_calls())
        for label, text in (("playbook output", output), ("stand-in arguments and environment", calls), *[("text", item) for item in extra_text]):
            found += [f"{label}: {hit.label} ({hit.form})" for hit in scan_text(text, markers)]
        for marker in markers:
            allowed = self.owned.get(marker.label, set()) | set(self.calls.glob("*.stdin"))
            found += [f"file {hit.path}: {hit.label} ({hit.form})" for hit in scan_tree(self.work, [marker], allowed)]
        return found


def rewritten_lists(run: Run) -> tuple[Path, Path]:
    """The tracked tool and service lists with no container owner, so every file is the test user's own."""
    tools = run.work / "postgres_tools.txt"
    services = run.work / "openbao_services.txt"
    tools.write_text(TOOLS_FILE.read_text())
    services.write_text("".join(f"{name}\n" for name in listed_names(SERVICES_FILE)))
    return tools, services


def load_module(name: str, path: Path) -> Any:
    shared_module("secret_files")
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(str(path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class PostgresSinks(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.run_ = Run(Path(self.directory.name))
        self.tools_file, self.services_file = rewritten_lists(self.run_)
        self.tools = listed_names(TOOLS_FILE)
        self.markers = {name: Marker(f"password of {name}", make_marker("urlsafe").value) for name in ["postgres", *self.tools]}
        for name, marker in self.markers.items():
            self.run_.place(marker, f"postgres/{name}.password")

    def tasks(self) -> list[dict[str, Any]]:
        common = {"tools_file": str(self.tools_file), "secrets_directory": str(self.run_.secrets)}
        return [
            {"postgres_accounts": {"step": "passwords", "services_file": str(self.services_file), "root_owner": OWNER, **common}},
            {"postgres_accounts": {"step": "provision", "compose_command": [str(self.run_.bin / "docker"), "compose"], **common}},
        ]

    def test_each_password_reaches_the_server_as_a_verifier_and_no_sink_holds_the_password(self) -> None:
        result = self.run_.play(self.tasks())
        output = result.stdout + result.stderr
        self.assertEqual(0, result.returncode, output[-3000:])
        sent = self.run_.inputs()
        verifiers = dict(VERIFIER.findall("\n".join(sent)))
        accounts = load_module("postgres_accounts_for_sinks", ROLES / "compose_stack" / "library" / "postgres_accounts.py")
        self.assertEqual(sorted(self.tools), sorted(verifiers), "control: every tool's account was created")
        for name in self.tools:
            self.assertTrue(accounts.verifier_matches(verifiers[name], self.markers[name].value), f"control: the verifier of {name} belongs to its marker")
        self.assertEqual([], self.run_.sinks(list(self.markers.values()), output, extra_text=tuple(sent)))
        for name, marker in self.markers.items():
            path = self.run_.secrets / "postgres" / f"{name}.password"
            self.assertEqual(marker.value + "\n", path.read_text(), "control: the owner's value was kept")
            self.assertEqual(0o400, path.stat().st_mode & 0o777)

    def test_a_server_error_that_repeats_the_statement_shows_no_password_and_no_verifier(self) -> None:
        self.run_.set_mode({"fail_on": "CREATE ROLE"})
        result = self.run_.play(self.tasks())
        output = result.stdout + result.stderr
        self.assertNotEqual(0, result.returncode)
        self.assertIn("psql exited with 1", output, "control: the failure reached the message")
        self.assertIn("SCRAM-SHA-256$4096", "".join(self.run_.inputs()), "control: the failed statement carried verifiers, so the search is not empty")
        self.assertEqual([], self.run_.sinks(list(self.markers.values()), output))
        self.assertNotIn("SCRAM-SHA-256$4096", output)


class OpenBaoStandIn:
    """A small in-memory OpenBao: logins by AppRole, policies, roles, issued secret IDs and the key-value paths of the tools. It keeps every request it saw."""

    def __init__(self, admin_secret_id: str) -> None:
        self.admin = admin_secret_id
        self.policies: dict[str, str] = {}
        self.roles: dict[str, dict[str, Any]] = {}
        self.issued: dict[str, str] = {}
        self.tokens: dict[str, str] = {}
        self.kv: dict[str, str] = {}
        self.requests: list[tuple[str, str, dict[str, Any]]] = []
        state = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *arguments: object) -> None:
                return

            def reply(self, status: int, document: dict[str, Any]) -> None:
                body = json.dumps(document).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def handle_any(self, method: str) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"{}")
                path = self.path.split("?")[0].removeprefix("/v1/")
                state.requests.append((method, path, body))
                if path == "sys/seal-status":
                    return self.reply(200, {"initialized": True, "sealed": False})
                if path == "auth/approle/login":
                    return self.login(body)
                if self.headers.get("X-Vault-Token") not in state.tokens:
                    return self.reply(403, {"errors": ["permission denied"]})
                self.reply(*state.route(method, path, body, "list=true" in self.path))

            def login(self, body: dict[str, Any]) -> None:
                role, secret_id = body.get("role_id"), body.get("secret_id")
                valid = secret_id == state.admin if role == "bootstrap" else state.issued.get(str(role)) == secret_id
                if not valid:
                    return self.reply(400, {"errors": ["invalid role or secret ID"]})
                token = f"token_{len(state.tokens)}"
                state.tokens[token] = str(role)
                self.reply(200, {"auth": {"client_token": token}})

            def do_GET(self) -> None:
                self.handle_any("GET")

            def do_PUT(self) -> None:
                self.handle_any("PUT")

            def do_POST(self) -> None:
                self.handle_any("POST")

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def address(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)

    def route(self, method: str, path: str, body: dict[str, Any], listing: bool) -> tuple[int, dict[str, Any]]:
        if listing and path == "auth/approle/role":
            return 200, {"data": {"keys": sorted(self.roles)}}
        parts = path.split("/")
        if path.startswith("sys/policies/acl/"):
            name = parts[-1]
            if method == "PUT":
                self.policies[name] = body["policy"]
                return 204, {}
            return (200, {"data": {"policy": self.policies[name]}}) if name in self.policies else (404, {"errors": []})
        if path.startswith("auth/approle/role/"):
            name = parts[3]
            rest = "/".join(parts[4:])
            if rest == "" and method == "POST":
                self.roles[name] = body
                return 204, {}
            if rest == "":
                return (200, {"data": self.roles[name]}) if name in self.roles else (404, {"errors": []})
            if rest == "role-id":
                return 200, {"data": {"role_id": name}}
            if rest == "secret-id":
                self.issued[name] = "issued_" + base64.urlsafe_b64encode(os.urandom(24)).decode()
                return 200, {"data": {"secret_id": self.issued[name]}}
            if rest == "secret-id/lookup":
                return (200, {"data": {"secret_id_accessor": "a"}}) if self.issued.get(name) == body.get("secret_id") else (404, {"errors": []})
        if path.startswith("secret/data/services/"):
            name = parts[3]
            if method == "POST":
                self.kv[name] = body["data"]["password"]
                return 200, {}
            return (200, {"data": {"data": {"password": self.kv[name]}}}) if name in self.kv else (404, {"errors": []})
        return 404, {"errors": ["unsupported path"]}


class OpenBaoSinks(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.run_ = Run(Path(self.directory.name))
        self.tools_file, self.services_file = rewritten_lists(self.run_)
        self.tools = listed_names(TOOLS_FILE)
        self.unlock = self.run_.work / "keys" / ".env"
        self.unlock.parent.mkdir()
        self.unlock.write_text("OPENBAO_UNLOCK_KEY=" + "0" * 64 + "\n")
        self.admin = Marker("admin secret ID", make_marker("urlsafe").value)
        self.run_.place(self.admin, "openbao/bootstrap.secret_id")
        self.passwords = {name: Marker(f"password of {name}", make_marker("urlsafe").value) for name in self.tools}
        for name, marker in self.passwords.items():
            self.run_.place(marker, f"postgres/{name}.password")
        self.bao = OpenBaoStandIn(self.admin.value)
        self.bao.start()
        self.addCleanup(self.bao.stop)

    def tasks(self) -> list[dict[str, Any]]:
        return [
            {"openbao_bootstrap": {"step": "prepare", "secrets_directory": str(self.run_.secrets), "unlock_key_file": str(self.unlock), "file_owner": OWNER}},
            {
                "openbao_bootstrap": {
                    "step": "reconcile",
                    "secrets_directory": str(self.run_.secrets),
                    "services_file": str(self.services_file),
                    "tools_file": str(self.tools_file),
                    "address": self.bao.address,
                    "file_owner": OWNER,
                    "root_owner": OWNER,
                    "ready_deadline_seconds": 10,
                }
            },
        ]

    def markers(self) -> list[Marker]:
        issued = [
            Marker(f"issued secret ID of {path.name}", path.read_text().strip())
            for path in sorted((self.run_.secrets / "openbao").glob("*.secret_id"))
            if path.name != "bootstrap.secret_id"
        ]
        for marker in issued:
            self.run_.owned[marker.label] = {self.run_.secrets / "openbao" / marker.label.removeprefix("issued secret ID of ")}
        return [self.admin, *self.passwords.values(), *issued]

    def test_the_admin_secret_id_and_each_password_reach_the_server_in_a_request_body_and_no_sink_holds_them(self) -> None:
        result = self.run_.play(self.tasks())
        output = result.stdout + result.stderr
        self.assertEqual(0, result.returncode, output[-3000:])
        logins = [body for method, path, body in self.bao.requests if path == "auth/approle/login"]
        self.assertEqual({"role_id": "bootstrap", "secret_id": self.admin.value}, logins[0], "control: the server got the owner's admin secret ID")
        for name, marker in self.passwords.items():
            self.assertEqual(marker.value, self.bao.kv.get(name), f"control: OpenBao holds the owner's password of {name}")
        markers = self.markers()
        self.assertGreaterEqual(len(markers), 1 + 2 * len(self.tools), "control: the issued secret IDs are searched for too")
        self.assertEqual([], self.run_.sinks(markers, output))

    def test_a_refused_login_shows_no_secret_id(self) -> None:
        wrong = Marker("wrong admin secret ID", make_marker("urlsafe").value)
        self.bao.admin = "another-value"
        result = self.run_.play(self.tasks())
        output = result.stdout + result.stderr
        self.assertNotEqual(0, result.returncode)
        self.assertIn("login of role bootstrap was refused (HTTP 400)", output, "control: the refusal reached the message")
        self.assertEqual([], self.run_.sinks([self.admin, wrong], output))


class AirbyteSinks(unittest.TestCase):
    NAMES = {
        "admin.password": "urlsafe",
        "storage_access_key_id": "hex20",
        "storage_secret_access_key": "urlsafe",
    }

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.run_ = Run(Path(self.directory.name))
        self.markers = {name: Marker(f"airbyte {name}", make_marker(form).value) for name, form in self.NAMES.items()}
        for name, marker in self.markers.items():
            self.run_.place(marker, f"airbyte/{name}")
        self.database = Marker("airbyte database password", make_marker("urlsafe").value)
        self.run_.place(self.database, "postgres/airbyte.password")

    def tasks(self) -> list[dict[str, Any]]:
        common = {"secrets_directory": str(self.run_.secrets)}
        return [
            {"airbyte_secrets": {"step": "files", "root_owner": OWNER, **common}},
            {"airbyte_secrets": {"step": "secrets", "kubectl": [str(self.run_.bin / "kubectl")], **common}},
        ]

    def all_markers(self) -> list[Marker]:
        return [self.database, *self.markers.values()]

    def test_each_value_reaches_the_cluster_on_standard_input_and_no_sink_holds_it(self) -> None:
        result = self.run_.play(self.tasks())
        output = result.stdout + result.stderr
        self.assertEqual(0, result.returncode, output[-3000:])
        manifests = [json.loads(text) for text in self.run_.inputs() if text]
        data = {item["metadata"]["name"]: {key: base64.b64decode(value).decode() for key, value in item["data"].items()} for item in manifests}
        self.assertEqual(
            {"airbyte-auth-secrets", "airbyte-database", "airbyte-storage"}, set(data), "control: the three secrets were applied on the standard input"
        )
        self.assertEqual(self.markers["admin.password"].value, data["airbyte-auth-secrets"]["instance-admin-password"])
        self.assertEqual(self.database.value, data["airbyte-database"]["database-password"])
        self.assertEqual(self.markers["storage_access_key_id"].value, data["airbyte-storage"]["AWS_ACCESS_KEY_ID"])
        self.assertEqual(self.markers["storage_secret_access_key"].value, data["airbyte-storage"]["AWS_SECRET_ACCESS_KEY"])
        self.assertEqual([], self.run_.sinks(self.all_markers(), output))

    def test_a_failed_apply_that_repeats_the_manifest_shows_no_value_in_either_form(self) -> None:
        self.run_.set_mode({"fail": True})
        result = self.run_.play(self.tasks())
        output = result.stdout + result.stderr
        self.assertNotEqual(0, result.returncode)
        self.assertIn("cannot apply the secret", output, "control: the failure reached the message")
        self.assertEqual([], self.run_.sinks(self.all_markers(), output))


class FileSinks(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.run_ = Run(Path(self.directory.name))

    def test_the_unlock_key_the_owner_placed_is_kept_checked_and_never_shown(self) -> None:
        marker = Marker("unlock key", make_marker("hex64").value)
        key_file = self.run_.work / "keys" / ".env"
        key_file.parent.mkdir()
        key_file.write_text(f"OPENBAO_UNLOCK_KEY={marker.value}\n")
        key_file.chmod(0o600)
        self.run_.owned[marker.label] = {key_file}
        variables = {
            "unlock_key_directory": str(key_file.parent),
            "unlock_key_file": str(key_file),
            "unlock_key_owner": pwd.getpwuid(os.getuid()).pw_name,
            "unlock_key_group": grp.getgrgid(os.getgid()).gr_name,
            "openbao_user": OWNER,
        }
        tasks = [
            {"ansible.builtin.include_tasks": str(ROLES / "unlock_key" / "tasks" / name)} for name in ("create_file.yml", "protect_file.yml", "verify_file.yml")
        ]
        result = self.run_.play(tasks, variables)
        output = result.stdout + result.stderr
        self.assertEqual(0, result.returncode, output[-3000:])
        self.assertEqual(f"OPENBAO_UNLOCK_KEY={marker.value}\n", key_file.read_text(), "control: the owner's key was kept and passed the shape check")
        self.assertEqual(0o400, key_file.stat().st_mode & 0o777)
        self.assertEqual([], self.run_.sinks([marker], output))

    def test_the_grafana_password_the_owner_placed_is_kept_and_never_shown(self) -> None:
        marker = Marker("grafana password", make_marker("urlsafe").value)
        path = self.run_.place(marker, "grafana/admin.password")
        result = self.run_.play([{"secret_file": {"path": str(path), "owner": OWNER}, "no_log": True}])
        output = result.stdout + result.stderr
        self.assertEqual(0, result.returncode, output[-3000:])
        self.assertEqual(marker.value + "\n", path.read_text(), "control: the owner's value was kept")
        self.assertEqual(0o400, path.stat().st_mode & 0o777)
        self.assertEqual([], self.run_.sinks([marker], output))


class DiagnosticSinks(unittest.TestCase):
    def test_a_key_given_as_the_tailscale_client_id_is_refused_by_the_name_of_the_setting_and_never_shown(self) -> None:
        marker = Marker("auth key", "tskey-auth-" + make_marker("plain").value)
        values = {
            "AWS_REGION": "us-west-2",
            "AWS_PROFILE": "example_project_dev",
            "AWS_ACCOUNT_ID": "1" * 12,
            "PROJECT_NAME": "example_project",
            "ENVIRONMENT": "dev",
            "S3_BUCKET": "example-bucket",
            "TAILSCALE_CLIENT_ID": marker.value,
        }
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / ".env"
            path.write_text("".join(f"{key}={value}\n" for key, value in values.items()))
            with self.assertRaises(ConfigurationError) as caught:
                load_configuration(path)
        shown = "".join(traceback.format_exception(caught.exception))
        self.assertIn("TAILSCALE_CLIENT_ID", shown, "control: the refusal names the setting")
        self.assertEqual([], scan_text(shown, [marker]))


class EvidenceSinks(unittest.TestCase):
    def test_a_value_in_a_plan_in_raw_output_or_in_a_log_does_not_reach_the_export(self) -> None:
        marker = Marker("secret in the run folder", make_marker("urlsafe").value)
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            for relative in ("raw/terraform.txt", "plans/plan.txt", "plans/plan.json", "logs/deploy.log", "notes.txt"):
                (folder / relative).parent.mkdir(parents=True, exist_ok=True)
                (folder / relative).write_text(f"value {marker.value}\n")
            for relative, text in (("inventory.json", "{}"), ("plans/a_binding.json", "{}"), ("acceptance/case.json", '{"passed": true}')):
                (folder / relative).parent.mkdir(parents=True, exist_ok=True)
                (folder / relative).write_text(text)
            manifest = build_manifest(folder, "20260101t0000-abcdef", "c0ffee")
            archive_path = folder / "evidence.tar.gz"
            with tarfile.open(archive_path, "w:gz") as archive:
                for item in manifest["files"]:
                    archive.add(folder / item["path"], arcname=item["path"])
            with tarfile.open(archive_path) as archive:
                members = {name: (archive.extractfile(name) or mock.Mock(read=lambda: b"")).read() for name in archive.getnames()}
        self.assertEqual({"inventory.json", "plans/a_binding.json", "acceptance/case.json"}, set(members), "control: the named evidence is in the archive")
        self.assertEqual([], scan_text(json.dumps(manifest), [marker]))
        self.assertEqual([], [name for name, content in members.items() if scan_text(content, [marker])])


STANDARD_BAO = """#!/bin/sh
env > "$SINK_OUT/environment"
printf '%s\\n' "$@" > "$SINK_OUT/arguments"
"""


class OpenBaoStartSinks(unittest.TestCase):
    """The real start script of the OpenBao container, run with a stand-in server program: what the server process receives."""

    START = ROOT / "compose" / "openbao" / "start.sh"
    CONFIG = ROOT / "compose" / "openbao" / "openbao.hcl"

    def run_start(self, scratch: Path, unlock: Marker, admin: Marker) -> tuple[int, str, str, str]:
        secrets_folder = scratch / "run_secrets"
        secrets_folder.mkdir()
        (secrets_folder / "openbao_unlock_key_file").write_text(f"OPENBAO_UNLOCK_KEY={unlock.value}\n")
        (secrets_folder / "openbao_bootstrap_secret_id").write_text(admin.value)
        script = scratch / "start.sh"
        script.write_text(self.START.read_text().replace("/run/secrets/", f"{secrets_folder}/"))
        bin_folder = scratch / "bin"
        bin_folder.mkdir()
        (bin_folder / "bao").write_text(STANDARD_BAO)
        (bin_folder / "bao").chmod(0o755)
        out = scratch / "out"
        out.mkdir()
        result = run_command("/bin/sh", [str(script)], env={"PATH": f"{bin_folder}:/usr/bin:/bin", "SINK_OUT": str(out)}, timeout=60)
        environment = (out / "environment").read_text() if (out / "environment").exists() else ""
        arguments = (out / "arguments").read_text() if (out / "arguments").exists() else ""
        return result.returncode, environment, arguments, result.stdout + result.stderr

    def test_the_server_process_gets_the_unlock_key_and_no_bootstrap_secret_id(self) -> None:
        unlock, admin = Marker("unlock key", make_marker("hex64").value), Marker("admin secret ID", make_marker("urlsafe").value)
        with tempfile.TemporaryDirectory() as scratch:
            code, environment, arguments, output = self.run_start(Path(scratch), unlock, admin)
        self.assertEqual(0, code, output)
        self.assertIn(
            f"OPENBAO_UNLOCK_KEY={unlock.value}", environment, "control: the unlock key reaches the server's environment, as the owner's exception allows"
        )
        self.assertIn("server", arguments, "control: the stand-in was started as the server")
        self.assertEqual([], scan_text(environment, [admin]), "the bootstrap secret ID is in the server's environment")
        self.assertEqual([], scan_text(arguments + output, [admin, unlock]))

    def test_the_first_start_reads_the_bootstrap_secret_id_from_its_own_file_and_not_from_the_environment(self) -> None:
        config = self.CONFIG.read_text()
        self.assertRegex(config, r'eval_source\s*=\s*"file"[^}]*path\s*=\s*"/run/secrets/openbao_bootstrap_secret_id"')
        self.assertNotIn("OPENBAO_BOOTSTRAP_SECRET_ID", config + self.START.read_text())

    def test_a_missing_or_empty_secret_id_file_stops_the_start_by_name(self) -> None:
        unlock = Marker("unlock key", make_marker("hex64").value)
        with tempfile.TemporaryDirectory() as scratch:
            code, environment, _, output = self.run_start(Path(scratch), unlock, Marker("empty", ""))
        self.assertNotEqual(0, code)
        self.assertEqual("", environment, "the server was not started")
        self.assertIn("bootstrap secret ID", output)


if __name__ == "__main__":
    unittest.main()
