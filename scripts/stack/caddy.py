"""Render the Caddy configuration from the list of web interfaces and refuse a configuration that could listen on any other address.

``compose/caddy/routes.txt`` has one line for each web interface: its name, the port Caddy answers on (from the gateway port 9440 to 9459, the ports that
the Tailscale grant in ``infra/tailscale/policy_additions.hujson`` admits), the local address and port behind it and the path that the access test reads
as its health check. A later phase adds an interface with one line. The configuration holds the Tailscale host
name and address only as the placeholders ``{$TS_HOST_NAME}`` and ``{$TS_ADDRESS}``, which the container reads from the host's own settings.

``.venv/bin/python -m scripts.stack.caddy render`` prints the configuration, for another list when a file is given. ``write`` replaces the tracked
file and ``check`` fails when the tracked file differs from the list.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

from scripts.project_config import ROOT

CADDY_FOLDER = ROOT / "compose" / "caddy"
ROUTES_FILE = CADDY_FOLDER / "routes.txt"
CADDYFILE = CADDY_FOLDER / "Caddyfile"
GATEWAY_PORT = 9440
GATEWAY_LAST_PORT = 9459
LOCAL_ADDRESS = "127.0.0.1"
NAME = re.compile(r"[a-z][a-z0-9_]*")
UPSTREAM = re.compile(rf"{re.escape(LOCAL_ADDRESS)}:([1-9][0-9]{{0,4}})")
PLAIN_PORT = re.compile(r"[1-9][0-9]{0,4}")
HEALTH_PATH = re.compile(r"/[A-Za-z0-9._~/-]*")
SITE_LINE = re.compile(r"^(?P<address>\S.*) \{$")
SITE_ADDRESS = re.compile(r"\{\$TS_HOST_NAME\}:(?P<port>\d+)")
ADDRESS_LITERAL = re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b")
BIND_LINE = "bind {$TS_ADDRESS}"
FORBIDDEN = ("local_certs", "skip_install_trust", "tls ", "tls\t", "http://", "acme_ca", "default_sni")
HEADER = "# Generated from routes.txt by `python -m scripts.stack.caddy write`. Edit the list, not this file.\n"
GLOBAL_OPTIONS = "{\n\tadmin off\n\tauto_https disable_redirects\n\tservers {\n\t\tprotocols h1 h2\n\t}\n}\n"
REQUIRED_GLOBAL = (("admin off", "admin"), ("auto_https disable_redirects", "redirect"), ("protocols h1 h2", "protocols"))


class RoutesError(ValueError):
    """The route list is malformed or repeats a name or a port."""


class RenderError(ValueError):
    """The configuration could listen on an address other than the Tailscale address or opens a listener the design forbids."""


@dataclass(frozen=True)
class Route:
    name: str
    port: int
    upstream: str
    health_path: str


def valid_port(text: str) -> bool:
    return bool(PLAIN_PORT.fullmatch(text)) and 1024 <= int(text) <= 65535


def parse_route(number: int, line: str) -> Route:
    fields = line.split()
    if len(fields) != 4:
        raise RoutesError(f"line {number}: a route has four fields, name, port, upstream and health path")
    name, port, upstream, health = fields
    if not NAME.fullmatch(name):
        raise RoutesError(f"line {number}: name {name!r} must be lower case letters, digits and underscores, starting with a letter")
    if not valid_port(port):
        raise RoutesError(f"line {number}: port {port!r} must be a whole number from 1024 to 65535")
    if not GATEWAY_PORT <= int(port) <= GATEWAY_LAST_PORT:
        raise RoutesError(
            f"line {number}: port {port} must be inside the gateway's range, {GATEWAY_PORT} to {GATEWAY_LAST_PORT}, which the Tailscale grant admits"
        )
    match = UPSTREAM.fullmatch(upstream)
    if match is None or not valid_port(match[1]):
        raise RoutesError(f"line {number}: upstream {upstream!r} must be {LOCAL_ADDRESS} and a port from 1024 to 65535")
    if not HEALTH_PATH.fullmatch(health):
        raise RoutesError(f"line {number}: health path {health!r} must start with a slash and hold only letters, digits and . _ ~ / -")
    return Route(name, int(port), upstream, health)


def parse_routes(text: str) -> list[Route]:
    """The checked routes in ``text`` in file order, or ``RoutesError`` naming the line and the fault."""
    routes: list[Route] = []
    for number, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        route = parse_route(number, line)
        if route.name in {item.name for item in routes}:
            raise RoutesError(f"line {number}: name {route.name} is used twice")
        if route.port in {item.port for item in routes} | {GATEWAY_PORT}:
            raise RoutesError(f"line {number}: port {route.port} is used twice or is the gateway port {GATEWAY_PORT}")
        routes.append(route)
    if not routes:
        raise RoutesError("the list holds no route")
    return routes


def load_routes(path: Path = ROUTES_FILE) -> list[Route]:
    try:
        return parse_routes(path.read_text())
    except OSError as error:
        raise RoutesError(f"cannot read {path.name}: {error.strerror}") from error


def site(address_port: int, body: list[str]) -> str:
    lines = [f"{{$TS_HOST_NAME}}:{address_port} {{", f"\t{BIND_LINE}", *(f"\t{line}" for line in body), "}"]
    return "\n".join(lines) + "\n"


def route_site(route: Route) -> str:
    message = f"Gateway error {{err.status_code}}: the service behind {route.name} is not running or does not answer."
    return site(route.port, [f"reverse_proxy {route.upstream}", "handle_errors {", f'\trespond "{message}" {{err.status_code}}', "}"])


def render(routes: list[Route]) -> str:
    """The Caddy configuration for ``routes``, checked by ``verify_rendered`` before it is returned."""
    sites = [site(GATEWAY_PORT, ['respond "gateway ok"']), *(route_site(route) for route in routes)]
    text = HEADER + GLOBAL_OPTIONS + "\n" + "\n".join(sites)
    verify_rendered(text)
    return text


def split_blocks(text: str) -> tuple[str, list[tuple[str, str]]]:
    """The global options block and each site as its address and its body, read by brace depth."""
    blocks: list[tuple[str, list[str]]] = []
    depth = 0
    for line in text.splitlines():
        if depth == 0 and line.strip() and not line.startswith("#"):
            blocks.append((line, []))
        elif blocks:
            blocks[-1][1].append(line)
        depth += line.count("{") - line.count("}")
    heads = [(head, "\n".join(body)) for head, body in blocks]
    if not heads or heads[0][0] != "{":
        raise RenderError("the configuration must start with its global options block")
    return heads[0][1], heads[1:]


def verify_rendered(text: str) -> None:
    """Refuse a configuration that is not bound to the Tailscale address in every site or that opens a listener of any other kind."""
    for forbidden in FORBIDDEN:
        if forbidden in text:
            raise RenderError(f"the configuration holds {forbidden.strip()!r}, which only a test may use and no permanent configuration may hold")
    global_block, sites = split_blocks(text)
    for needle, label in REQUIRED_GLOBAL:
        if needle not in global_block:
            raise RenderError(f"the global options must hold {needle!r} ({label} off or restricted)")
    if not sites:
        raise RenderError("the configuration holds no site")
    for head, body in sites:
        address = SITE_LINE.match(head)
        if address is None or SITE_ADDRESS.fullmatch(address["address"]) is None:
            raise RenderError(f"site {head!r} must be written as {{$TS_HOST_NAME}}:<port>, the Tailscale name form")
        port = SITE_ADDRESS.fullmatch(address["address"])
        label = port["port"] if port else head
        bind_lines = [line.strip() for line in body.splitlines() if line.strip().startswith("bind")]
        if bind_lines != [BIND_LINE]:
            raise RenderError(f"site on port {label} must hold exactly one bind to {{$TS_ADDRESS}}, found {bind_lines or 'none'}")
        for line in body.splitlines():
            stripped = line.strip()
            if stripped.startswith("reverse_proxy") and UPSTREAM.fullmatch(stripped.removeprefix("reverse_proxy").strip()) is None:
                raise RenderError(f"site on port {label}: reverse_proxy must name {LOCAL_ADDRESS} and a port, found {stripped!r}")
            if ADDRESS_LITERAL.search(line) and not stripped.startswith("reverse_proxy"):
                raise RenderError(f"site on port {label} holds an address literal in {stripped!r}")
    if ADDRESS_LITERAL.search(global_block):
        raise RenderError("the global options hold an address literal")


def main() -> int:
    arguments = sys.argv[1:]
    verb = arguments[0] if arguments else ""
    if verb not in ("render", "write", "check") or len(arguments) > (2 if verb == "render" else 1):
        sys.stderr.write("usage: python -m scripts.stack.caddy render [ROUTES_FILE] | write | check\n")
        return 2
    try:
        text = render(load_routes(Path(arguments[1]) if len(arguments) == 2 else ROUTES_FILE))
    except (RoutesError, RenderError) as error:
        sys.stderr.write(f"caddy: {error}\n")
        return 1
    if verb == "render":
        sys.stdout.write(text)
    elif verb == "write":
        CADDYFILE.write_text(text)
    elif not CADDYFILE.exists() or CADDYFILE.read_text() != text:
        sys.stderr.write("caddy: compose/caddy/Caddyfile differs from the list; run `python -m scripts.stack.caddy write`\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
