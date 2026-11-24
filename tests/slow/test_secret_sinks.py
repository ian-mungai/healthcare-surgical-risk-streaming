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
import contextlib
import grp
import http.client
import importlib.util
import io
import json
import os
import pwd
import re
import sys
import tarfile
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast
from unittest import mock

from scripts.evidence import export
from scripts.evidence.manifest import build_manifest
from scripts.infra.state_bootstrap import RunBinding
from scripts.process import CompletedProcess, run_command
from scripts.project_config import ROOT, ConfigurationError, load_configuration
from scripts.stack.yaml_text import load_yaml
from tests.fast.secret_inputs import Marker, listed_names, make_marker, scan_text, scan_tree
from tests.fast.traceback_support import raised_text
from tests.slow.ansible_support import ROLES, SHARED_MODULE_UTILS, environment, shared_module

PLAYBOOK_SECONDS = 180
OWNER = f"{os.getuid()}:{os.getgid()}"
LIBRARIES = [ROLES / name / "library" for name in ("compose_stack", "openbao", "airbyte", "secret_files")]
TOOLS_FILE = ROOT / "compose" / "postgres_tools.txt"
SERVICES_FILE = ROOT / "compose" / "openbao_services.txt"
PYTHON = str(ROOT / ".venv" / "bin" / "python")
VERIFIER = re.compile(r"CREATE ROLE \"(\w+)\" [A-Z ]+ PASSWORD '(SCRAM-SHA-256\$[^']+)'")

DOCKER_STAND_IN = r"""#!__PYTHON__
import json, os, subprocess, sys
from pathlib import Path
calls = Path(__file__).resolve().parent.parent / "calls"
standard_input = sys.stdin.read()
number = len(list(calls.glob("*.stdin")))
(calls / f"{number}.stdin").write_text(standard_input)
with open(calls / "calls.jsonl", "a") as handle:
    handle.write(json.dumps({"program": "docker", "argv": sys.argv, "env": dict(os.environ)}) + "\n")
mode = json.loads((calls / "mode.json").read_text())
script = sys.argv[sys.argv.index("-c") + 1]
for container_path, host_path in mode["files"].items():
    script = script.replace(container_path, host_path)
script = script.replace("/dev/shm", str(calls.parent / "shm"))
environment = {"PATH": f"{calls.parent / 'bin' / 'psql_bin'}:/usr/bin:/bin"}
result = subprocess.run(["/bin/sh", "-c", script], input=standard_input, capture_output=True, text=True, env=environment)
sys.stdout.write(result.stdout)
sys.stderr.write(result.stderr)
sys.exit(result.returncode)
"""

PSQL_STAND_IN = r"""#!__PYTHON__
import json, os, sys
from pathlib import Path
calls = Path(__file__).resolve().parent.parent.parent / "calls"
standard_input = sys.stdin.read()
number = len(list(calls.glob("*.stdin")))
(calls / f"{number}.stdin").write_text(standard_input)
with open(calls / "calls.jsonl", "a") as handle:
    handle.write(json.dumps({"program": "psql", "argv": sys.argv, "env": dict(os.environ)}) + "\n")
password_file = os.environ.get("PGPASSFILE")
if password_file:
    status = os.stat(password_file)
    line = Path(password_file).read_text().splitlines()[0]
    password = line.split(":", 4)[4].replace("\\:", ":").replace("\\\\", "\\")
    with open(calls / "consumer.jsonl", "a") as handle:
        handle.write(json.dumps({"PGPASSFILE_PASSWORD": password, "mode": oct(status.st_mode & 0o777)}) + "\n")
mode = json.loads((calls / "mode.json").read_text())
if "PGPASSWORD" in os.environ:
    with open(calls / "consumer.jsonl", "a") as handle:
        handle.write(json.dumps({"PGPASSWORD": os.environ["PGPASSWORD"]}) + "\n")
if mode.get("fail_on") and mode["fail_on"] in standard_input:
    sys.stderr.write("psql:<stdin>:1: ERROR: could not run the statement\nLINE 1: " + standard_input + "\n")
    sys.exit(1)
if "pg_authid" in standard_input:
    sys.stdout.write(mode.get("state", ""))
sys.exit(0)
"""

KUBECTL_STAND_IN = r"""#!__PYTHON__
import json, os, sys
from pathlib import Path
calls = Path(__file__).resolve().parent.parent / "calls"
arguments = sys.argv[1:]
standard_input = sys.stdin.read() if "-" in arguments else ""
number = len(list(calls.glob("*.stdin")))
(calls / f"{number}.stdin").write_text(standard_input)
with open(calls / "calls.jsonl", "a") as handle:
    handle.write(json.dumps({"argv": sys.argv, "env": dict(os.environ)}) + "\n")
mode = json.loads((calls / "mode.json").read_text())
if arguments[0] == "get":
    sys.stderr.write('Error from server (NotFound): secrets "x" not found\n')
    sys.exit(1)
if mode.get("fail"):
    sys.stderr.write("error: failed to apply the object: " + standard_input + "\n")
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
        (self.bin / "psql_bin").mkdir()
        (self.work / "shm").mkdir(mode=0o700)
        for name, body in (("docker", DOCKER_STAND_IN), ("kubectl", KUBECTL_STAND_IN), ("psql_bin/psql", PSQL_STAND_IN)):
            (self.bin / name).write_text(body.replace("__PYTHON__", PYTHON))
            (self.bin / name).chmod(0o755)
        self.owned: dict[str, set[Path]] = {}

    def set_mode(self, mode: dict[str, Any]) -> None:
        (self.calls / "mode.json").write_text(json.dumps(mode))

    def place(self, marker: Marker, relative: str, *, line_break: bool = True) -> Path:
        """Put the marker in its secret file, as the owner's value, before the deploy runs. Only this file may hold it."""
        path = self.secrets / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(marker.value + ("\n" if line_break else ""))
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

    def consumer(self) -> list[dict[str, str]]:
        """What the consumer inside a stand-in received through its intended channel, one record for each call that read a mounted secret."""
        path = self.calls / "consumer.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def inputs(self) -> list[str]:
        return [path.read_text() for path in sorted(self.calls.glob("*.stdin"), key=lambda item: int(item.stem))]

    def sinks(self, markers: list[Marker], output: str, *, extra_text: tuple[str, ...] = ()) -> list[str]:
        """Each sink that holds a marker: the raw playbook output, the stand-ins' arguments and environment, other texts and every file but its own."""
        found: list[str] = []
        calls = json.dumps(self.stand_in_calls())
        for label, text in (("playbook output", output), ("stand-in arguments and environment", calls), *[("text", item) for item in extra_text]):
            found += [f"{label}: {hit.label} ({hit.form})" for hit in scan_text(text, markers)]
        for marker in markers:
            allowed = self.owned.get(marker.label, set()) | set(self.calls.glob("*.stdin")) | set(self.calls.glob("consumer.jsonl"))
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
    """Provisioning runs the real script of the accounts module, and no provisioning process has the password in its environment.

    The pinned PostgreSQL 18.7 entrypoint exports the administrator password on every container start, before its PG_VERSION check, and clears it before the
    final server starts. That is an accepted limit (owner, Nov 23 2026, Q55 "A", extended by Q60 "A"), and no process of these cases belongs to it.
    """

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.run_ = Run(Path(self.directory.name))
        self.tools_file, self.services_file = rewritten_lists(self.run_)
        self.tools = listed_names(TOOLS_FILE)
        self.mode: dict[str, Any] = {}
        self.markers = {name: Marker(f"password of {name}", make_marker("urlsafe").value) for name in ["postgres", *self.tools]}
        for name, marker in self.markers.items():
            self.run_.place(marker, f"postgres/{name}.password")

    def tasks(self) -> list[dict[str, Any]]:
        self.run_.set_mode({"files": {"/run/secrets/postgres_admin_password": str(self.run_.secrets / "postgres" / "postgres.password")}, **self.mode})
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
        programs = {call.get("program") for call in self.run_.stand_in_calls()}
        self.assertEqual(
            {"docker", "psql"}, programs, "control: the environment of the exec and of psql, the provisioning processes, were recorded and are searched"
        )
        records = self.run_.consumer()
        self.assertEqual(
            {self.markers["postgres"].value},
            {item.get("PGPASSFILE_PASSWORD") for item in records},
            "control: psql read the owner's admin password from its password file",
        )
        self.assertEqual({"0o600"}, {item["mode"] for item in records}, "the password file is private")
        self.assertEqual([], [item for item in records if "PGPASSWORD" in item], "psql's environment holds no password")
        self.assertEqual([], list((self.run_.work / "shm").iterdir()), "the password file is removed after use")
        self.assertEqual([], self.run_.sinks(list(self.markers.values()), output, extra_text=tuple(sent)))
        for name, marker in self.markers.items():
            path = self.run_.secrets / "postgres" / f"{name}.password"
            self.assertEqual(marker.value + "\n", path.read_text(), "control: the owner's value was kept")
            self.assertEqual(0o400, path.stat().st_mode & 0o777)

    def test_a_server_error_that_repeats_the_statement_shows_no_password_and_no_verifier(self) -> None:
        self.mode = {"fail_on": "CREATE ROLE"}
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
                    return self.reply(400, {"errors": [f"invalid role or secret ID {secret_id}"]})
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

    def login_over_http(self, role: str, secret_id: str) -> tuple[int, dict[str, Any]]:
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_address[1], timeout=10)
        try:
            connection.request("POST", "/v1/auth/approle/login", json.dumps({"role_id": role, "secret_id": secret_id}))
            response = connection.getresponse()
            return response.status, json.loads(response.read() or b"{}")
        finally:
            connection.close()

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
        self.run_.place(self.admin, "openbao/bootstrap.secret_id", line_break=False)
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
        for marker in markers[1 + len(self.tools) :]:
            role = marker.label.removeprefix("issued secret ID of ").removesuffix(".secret_id")
            status, _ = self.bao.login_over_http(role, marker.value)
            self.assertEqual(200, status, f"control: the issued secret ID of {role} logs in against the server that issued it")
            self.assertEqual(400, self.bao.login_over_http(role, marker.value + "x")[0])
        self.assertEqual([], self.run_.sinks(markers, output))

    def test_a_refused_login_shows_no_secret_id_although_the_server_repeats_it(self) -> None:
        self.bao.admin = "another-value"
        result = self.run_.play(self.tasks())
        output = result.stdout + result.stderr
        self.assertNotEqual(0, result.returncode)
        self.assertIn("login of role bootstrap was refused (HTTP 400)", output, "control: the refusal reached the message")
        refused = [body for method, path, body in self.bao.requests if path == "auth/approle/login"]
        self.assertEqual(self.admin.value, refused[0]["secret_id"], "control: the server received the secret ID and answered with a text that repeats it")
        self.assertEqual([], self.run_.sinks([self.admin], output))

    def test_an_admin_secret_id_file_that_ends_with_a_line_break_is_refused_before_any_start(self) -> None:
        self.run_.place(self.admin, "openbao/bootstrap.secret_id", line_break=True)
        result = self.run_.play(self.tasks())
        output = result.stdout + result.stderr
        self.assertNotEqual(0, result.returncode)
        self.assertIn("admin secret ID file ends with a line break", output, "control: the refusal reached the message")
        self.assertEqual([], [request for request in self.bao.requests if request[1] != "sys/seal-status"], "no request reached the server")
        self.assertEqual([], self.run_.sinks([self.admin], output))


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

    def test_the_grafana_password_the_owner_placed_is_kept_and_reaches_grafana_through_its_own_file_provider(self) -> None:
        marker = Marker("grafana password", make_marker("urlsafe").value)
        path = self.run_.place(marker, "grafana/admin.password")
        result = self.run_.play([{"secret_file": {"path": str(path), "owner": OWNER}, "no_log": True}])
        output = result.stdout + result.stderr
        self.assertEqual(0, result.returncode, output[-3000:])
        self.assertEqual(marker.value + "\n", path.read_text(), "control: the owner's value was kept")
        self.assertEqual(0o400, path.stat().st_mode & 0o777)
        compose = cast("dict[str, Any]", load_yaml((ROOT / "compose" / "services" / "grafana.yml").read_text()))
        service = compose["services"]["grafana"]
        mount = next(item for item in service["configs"] if item["target"] == "/etc/grafana/grafana.ini")
        configuration = (ROOT / "compose" / "services" / compose["configs"][mount["source"]]["file"]).resolve()
        provider = re.search(r"^admin_password\s*=\s*\$__file\{([^}]+)\}", configuration.read_text(), re.MULTILINE)
        self.assertIsNotNone(provider)
        provider = cast("re.Match[str]", provider)
        secret_name = Path(provider[1]).name
        self.assertIn(secret_name, service["secrets"], "the service mounts the secret that the provider names")
        mounted = re.fullmatch(r"\$\{SECRETS_DIR[^}]*\}/(.+)", str(compose["secrets"][secret_name]["file"]))
        self.assertIsNotNone(mounted)
        mounted = cast("re.Match[str]", mounted)
        received = (self.run_.secrets / mounted[1]).read_text().strip()
        self.assertEqual(marker.value, received, "control: the file that Grafana's provider reads, by the configuration and the mount, holds the owner's value")
        self.assertEqual([], self.run_.sinks([marker], output))

    def grafana_play(self, content: str) -> tuple[int, str, Path]:
        folder = self.run_.secrets
        path = folder / "grafana" / "admin.password"
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.chmod(0o600)
        path.write_text(content)
        path.chmod(0o600)
        variables = {"compose_stack_secrets_directory": str(folder), "grafana_user": OWNER}
        result = self.run_.play([{"ansible.builtin.include_tasks": str(ROLES / "compose_stack" / "tasks" / "grafana_password.yml")}], variables)
        return result.returncode, result.stdout + result.stderr, path

    def test_a_grafana_password_with_leading_or_trailing_whitespace_is_refused_by_name_and_never_shown(self) -> None:
        for label, content in (
            ("a leading space", " lead-" + "x" * 20),
            ("a trailing space", "x" * 20 + "-trail "),
            ("a leading tab", "\t" + "x" * 20),
            ("a trailing tab", "x" * 20 + "\t\n"),
        ):
            with self.subTest(label):
                marker = Marker("grafana password", content.strip())
                code, output, path = self.grafana_play(content)
                self.assertNotEqual(0, code, output[-2000:])
                self.assertIn("grafana/admin.password", output, "control: the refusal names the file")
                self.assertIn("whitespace", output)
                self.assertEqual(content, path.read_text(), "the file is left as it was")
                self.assertEqual([], scan_text(output, [marker]))

    def test_a_grafana_password_with_no_edge_whitespace_or_with_one_final_line_break_passes(self) -> None:
        for label, content in (("none", "x" * 24), ("a final line break", "x" * 24 + "\n"), ("inner spaces", "two words " + "x" * 12)):
            with self.subTest(label):
                code, output, path = self.grafana_play(content)
                self.assertEqual(0, code, output[-2000:])
                self.assertEqual(content, path.read_text(), "control: the value was kept")


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
            _, shown = raised_text(lambda: load_configuration(path), ConfigurationError, "load_configuration")
        self.assertIn("TAILSCALE_CLIENT_ID", shown, "control: the refusal names the setting")
        self.assertEqual([], scan_text(shown, [marker]))


class EvidenceSinks(unittest.TestCase):
    def test_a_value_in_a_plan_in_raw_output_or_in_a_log_does_not_reach_the_export_made_by_the_real_exporter(self) -> None:
        marker = Marker("secret in the run folder", make_marker("urlsafe").value)
        run_id = "20260101t0000-abcdef"
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            folder = root / "runs" / run_id
            for relative in ("raw/terraform.txt", "plans/plan.txt", "plans/plan.json", "logs/deploy.log", "notes.txt"):
                (folder / relative).parent.mkdir(parents=True, exist_ok=True)
                (folder / relative).write_text(f"value {marker.value}\n")
            for relative, text in (("inventory.json", "{}"), ("plans/a_binding.json", "{}"), ("acceptance/case.json", '{"passed": true}')):
                (folder / relative).parent.mkdir(parents=True, exist_ok=True)
                (folder / relative).write_text(text)
            binding = root / "binding.json"
            RunBinding(run_id, "c0ffee", "s3", "us-west-2", "example-project", "default", ["a", "b"], "verified").write(binding)
            with (
                mock.patch.object(export, "ROOT", root),
                mock.patch.object(export, "build_manifest", wraps=build_manifest) as built,
                mock.patch.object(sys, "argv", ["export", "--binding", str(binding)]),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                code = export.main()
            manifest_text = (folder / "export" / "manifest.json").read_text()
            with tarfile.open(folder / "export" / "evidence.tar.gz") as archive:
                members = {name: (archive.extractfile(name) or io.BytesIO()).read() for name in archive.getnames()}
        self.assertEqual(0, code)
        built.assert_called_once()
        self.assertEqual({"inventory.json", "plans/a_binding.json", "acceptance/case.json"}, set(members), "control: the named evidence is in the archive")
        self.assertEqual([], scan_text(manifest_text, [marker]))
        self.assertEqual([], [name for name, content in members.items() if scan_text(content, [marker])])


def parse_properties(text: str) -> dict[str, str]:
    """The entries of a Java properties file as ``java.util.Properties.load`` reads them (SmallRye Config 3.17.2 uses it for properties files)."""
    entries: dict[str, str] = {}
    escapes = {"n": "\n", "r": "\r", "t": "\t", "f": "\f"}
    for line in re.split(r"\r\n|\r|\n", text):
        stripped = line.lstrip(" \t\f")
        if not stripped or stripped[0] in "#!":
            continue
        key, index = "", 0
        while index < len(stripped) and stripped[index] not in "=: \t\f":
            if stripped[index] == "\\":
                index += 1
                key += escapes.get(stripped[index], stripped[index])
            else:
                key += stripped[index]
            index += 1
        rest = stripped[index:].lstrip(" \t\f")
        if rest[:1] in ("=", ":"):
            rest = rest[1:].lstrip(" \t\f")
        value, index = "", 0
        while index < len(rest):
            if rest[index] == "\\":
                index += 1
                value += escapes.get(rest[index], rest[index])
            else:
                value += rest[index]
            index += 1
        entries[key] = value
    return entries


class NessieStartSinks(unittest.TestCase):
    """The real start command of the Nessie service with its two paths moved into a scratch folder and the server replaced by a command that does nothing."""

    SERVICE = ROOT / "compose" / "services" / "nessie.yml"
    PASSWORDS = {
        "a plain password": make_marker("urlsafe").value,
        "a leading space": "  two leading spaces",
        "a line break": "first line\nsecond line",
        "a tab and a carriage return": "tab\there\rcarriage",
        "properties characters": "#hash !bang =equals :colon trailing  ",
    }

    def command(self) -> str:
        document = load_yaml(self.SERVICE.read_text())
        self.assertIsInstance(document, dict)
        script = str(cast("dict[str, Any]", document)["services"]["nessie"]["command"][2])
        return script.replace("$$", "$")

    def started(self, password: str) -> tuple[int, str, str]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            (folder / "config").mkdir()
            secret = folder / "password"
            secret.write_bytes(password.encode())
            script = self.command().replace("/run/secrets/nessie_database_password", str(secret)).replace("/run/nessie_config", str(folder / "config"))
            script = script.replace("exec /opt/jboss/container/java/run/run-java.sh", "true")
            result = run_command("/bin/bash", ["-c", script], env={"PATH": "/usr/bin:/bin"}, timeout=60)
            written = folder / "config" / "datasource.properties"
            mode = oct(written.stat().st_mode & 0o777) if written.exists() else "missing"
            return result.returncode, written.read_bytes().decode() if written.exists() else "", mode + result.stdout + result.stderr

    def test_each_password_is_read_back_exactly_from_the_properties_file(self) -> None:
        for label, password in self.PASSWORDS.items():
            with self.subTest(label):
                code, text, detail = self.started(password)
                self.assertEqual(0, code, detail)
                self.assertEqual(
                    {"quarkus.datasource.postgresql.password": password}, parse_properties(text), f"the parsed value differs from the file's bytes: {text!r}"
                )
                self.assertTrue(detail.startswith("0o600"), detail)

    def test_an_empty_password_file_stops_the_start_by_name(self) -> None:
        code, text, detail = self.started("")
        self.assertNotEqual(0, code)
        self.assertEqual("", text)
        self.assertIn("password file is empty", detail)


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
