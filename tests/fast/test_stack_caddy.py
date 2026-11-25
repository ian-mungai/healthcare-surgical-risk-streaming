"""The Caddy route list is data, the rendered configuration follows from it and a configuration that could listen on another address is refused.

The renderer and its guard are held by planted defects: each case changes one thing and expects the message to name that thing. The tracked
configuration must equal what the renderer writes for the tracked list, hold the Tailscale name form only and carry a bind in every site.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path
from typing import Any

from scripts.project_config import ROOT
from scripts.stack.caddy import (
    CADDYFILE,
    GATEWAY_LAST_PORT,
    GATEWAY_PORT,
    ROUTES_FILE,
    RenderError,
    Route,
    RoutesError,
    load_routes,
    parse_routes,
    render,
    verify_rendered,
)
from scripts.stack.yaml_text import load_yaml

SERVICE_FILE = Path(__file__).resolve().parents[2] / "compose" / "services" / "caddy.yml"
SERVICES = ROOT / "compose" / "services"
TAILSCALE_GRANT = ROOT / "infra" / "tailscale" / "policy_additions.hujson"
PUBLISHED = re.compile(r"127\.0\.0\.1:\$\{[A-Z_]+:-(\d+)\}:\d+")
GOOD = """\
# name  caddy_port  upstream  health_path
grafana 9442 127.0.0.1:3000 /api/health
openbao 9441 127.0.0.1:8200 /v1/sys/health
"""


def refused_routes(text: str) -> str:
    try:
        parse_routes(text)
    except RoutesError as error:
        return str(error)
    raise AssertionError("the parser accepted a list that it must refuse")


def refused_render(text: str) -> str:
    try:
        verify_rendered(text)
    except RenderError as error:
        return str(error)
    raise AssertionError("the guard accepted a configuration that it must refuse")


def published_ports(service_file: Path) -> set[int]:
    """The host ports on the local address that a Compose service file publishes by default."""
    document: Any = load_yaml(service_file.read_text())
    (service,) = document["services"].values()
    return {int(match[1]) for entry in service.get("ports", []) if (match := PUBLISHED.fullmatch(str(entry)))}


# A route named for an interface whose service file has another name.
ROUTE_SERVICES = {"dagster": "dagster_webserver"}


def routes_off_their_service(routes: list[Route], services: Path = SERVICES) -> list[str]:
    """The routes whose service file exists and does not publish the port that the route sends to."""
    wrong = []
    for route in routes:
        service_file = services / f"{ROUTE_SERVICES.get(route.name, route.name)}.yml"
        if service_file.exists() and int(route.upstream.rsplit(":", 1)[1]) not in published_ports(service_file):
            wrong.append(route.name)
    return wrong


class GatewayRangeTest(unittest.TestCase):
    """The Tailscale grant admits the owner's devices to the gateway's ports and to nothing else of the host's web ports."""

    def test_the_ports_the_gateway_may_use_are_the_ports_the_tailscale_grant_admits(self) -> None:
        self.assertIn((str(GATEWAY_PORT), str(GATEWAY_LAST_PORT)), re.findall(r"tcp:(\d+)-(\d+)", TAILSCALE_GRANT.read_text()))

    def test_every_port_of_the_tracked_list_is_inside_the_range_that_the_grant_admits(self) -> None:
        ports = [route.port for route in load_routes(ROUTES_FILE)]
        self.assertTrue(ports)
        self.assertEqual([], [port for port in ports if not GATEWAY_PORT < port <= GATEWAY_LAST_PORT])

    def test_a_route_port_outside_the_range_is_refused_with_the_port_and_the_range_named_and_the_two_ends_pass(self) -> None:
        self.assertEqual(
            [GATEWAY_PORT + 1, GATEWAY_LAST_PORT],
            [route.port for route in parse_routes(f"a {GATEWAY_PORT + 1} 127.0.0.1:3000 /\nb {GATEWAY_LAST_PORT} 127.0.0.1:3001 /\n")],
        )
        for port in (GATEWAY_PORT - 1, GATEWAY_LAST_PORT + 1, 9499, 15600, 65535, 1024):
            with self.subTest(port=port):
                message = refused_routes(f"x {port} 127.0.0.1:3000 /\n")
                self.assertIn(str(port), message)
                self.assertIn(f"{GATEWAY_PORT}", message)
                self.assertIn(f"{GATEWAY_LAST_PORT}", message)


class RouteTargetTest(unittest.TestCase):
    """A route sends to the host port that its service really publishes, and to its health path only where the service answers there."""

    def test_each_route_whose_service_file_exists_sends_to_the_port_that_file_publishes(self) -> None:
        routes = load_routes(ROUTES_FILE)
        self.assertEqual([], routes_off_their_service(routes))
        self.assertTrue({"openbao", "nessie"} <= {route.name for route in routes if (SERVICES / f"{route.name}.yml").exists()})

    def test_a_route_that_sends_to_another_port_than_its_service_publishes_is_found(self) -> None:
        planted = parse_routes("openbao 9441 127.0.0.1:8201 /v1/sys/health\nnessie 9445 127.0.0.1:19120 /api/v2/config\n")
        self.assertEqual(["openbao"], routes_off_their_service(planted))

    def test_a_route_for_a_service_with_no_file_yet_is_not_judged(self) -> None:
        self.assertEqual([], routes_off_their_service(parse_routes("grafana 9442 127.0.0.1:3000 /api/health\n")))

    def test_nessie_is_checked_on_the_port_of_its_api_because_its_health_path_of_the_management_port_is_not_published(self) -> None:
        routes = {route.name: route for route in load_routes(ROUTES_FILE)}
        self.assertEqual(("127.0.0.1:19120", "/api/v2/config"), (routes["nessie"].upstream, routes["nessie"].health_path))
        self.assertEqual({19120}, published_ports(SERVICES / "nessie.yml"))


class RouteListTest(unittest.TestCase):
    def test_the_tracked_list_loads_and_holds_the_phase_2_interfaces(self) -> None:
        names = [route.name for route in load_routes(ROUTES_FILE)]
        self.assertEqual(["openbao", "grafana", "prometheus", "trino", "nessie", "airbyte", "dagster", "apicurio", "aistor"], names)

    def test_dagster_and_the_registry_are_served_on_their_ports_with_the_health_paths_of_their_tools(self) -> None:
        routes = {route.name: (route.port, route.upstream, route.health_path) for route in load_routes(ROUTES_FILE)}
        self.assertEqual((9447, "127.0.0.1:3001", "/server_info"), routes["dagster"])
        self.assertEqual((9448, "127.0.0.1:8081", "/apis/registry/v3/system/info"), routes["apicurio"])

    def test_the_storage_route_is_the_s3_port_with_the_live_health_path_and_is_held_to_the_port_its_file_publishes(self) -> None:
        routes = {route.name: (route.port, route.upstream, route.health_path) for route in load_routes(ROUTES_FILE)}
        self.assertEqual((9449, "127.0.0.1:9000", "/minio/health/live"), routes["aistor"])
        self.assertEqual({9000}, published_ports(SERVICES / "aistor.yml"))
        self.assertEqual(["aistor"], routes_off_their_service(parse_routes("aistor 9449 127.0.0.1:9001 /minio/health/live\n")))

    def test_the_dagster_route_is_held_to_the_port_that_the_web_servers_file_publishes(self) -> None:
        planted = parse_routes("dagster 9447 127.0.0.1:3000 /server_info\n")
        self.assertEqual(["dagster"], routes_off_their_service(planted))
        self.assertEqual({3001}, published_ports(SERVICES / "dagster_webserver.yml"))

    def test_the_airbyte_route_sends_to_the_port_of_airbytes_forwarder(self) -> None:
        routes = {route.name: route for route in load_routes(ROUTES_FILE)}
        group_variables: Any = load_yaml((ROOT / "ansible" / "group_vars" / "all.yml").read_text())
        self.assertEqual(f"127.0.0.1:{group_variables['airbyte_web_port']}", routes["airbyte"].upstream)

    def test_a_good_list_loads_each_field_in_file_order(self) -> None:
        routes = parse_routes(GOOD)
        self.assertEqual(["grafana", "openbao"], [route.name for route in routes])
        self.assertEqual((9442, "127.0.0.1:3000", "/api/health"), (routes[0].port, routes[0].upstream, routes[0].health_path))

    def test_a_repeated_name_or_port_is_refused_with_the_name_or_port_named(self) -> None:
        self.assertIn("openbao", refused_routes(GOOD + "openbao 9443 127.0.0.1:3000 /\n"))
        self.assertIn("9441", refused_routes(GOOD + "other 9441 127.0.0.1:3000 /\n"))

    def test_the_gateway_port_cannot_be_a_route_port(self) -> None:
        self.assertIn(str(GATEWAY_PORT), refused_routes(f"x {GATEWAY_PORT} 127.0.0.1:3000 /\n"))

    def test_a_port_outside_the_unprivileged_range_is_refused(self) -> None:
        for port in ("0", "80", "1023", "65536", "x", "-1", "9441.5"):
            with self.subTest(port=port):
                self.assertIn("port", refused_routes(f"x {port} 127.0.0.1:3000 /\n"))

    def test_an_upstream_that_is_not_the_local_address_is_refused_so_a_route_cannot_reach_another_host(self) -> None:
        for upstream in ("10.0.0.5:80", "0.0.0.0:80", "example.com:80", "localhost:80", "127.0.0.1", "127.0.0.1:0", "127.0.0.1:99999", "http://127.0.0.1:80"):
            with self.subTest(upstream=upstream):
                self.assertIn("upstream", refused_routes(f"x 9441 {upstream} /\n"))

    def test_a_name_that_is_not_a_lower_case_word_is_refused(self) -> None:
        for name in ("Grafana", "1grafana", "gra-fana", "gra.fana"):
            with self.subTest(name=name):
                self.assertIn(name, refused_routes(f"{name} 9441 127.0.0.1:3000 /\n"))

    def test_a_health_path_that_does_not_start_with_a_slash_is_refused(self) -> None:
        self.assertIn("health", refused_routes("x 9441 127.0.0.1:3000 health\n"))

    def test_a_hash_inside_a_line_is_refused_and_never_cuts_the_line_short(self) -> None:
        self.assertIn("health", refused_routes("x 9441 127.0.0.1:3000 /a#b\n"))
        self.assertIn("line 1", refused_routes("x 9441 127.0.0.1:3000 /a # a note\n"))

    def test_a_whole_line_comment_is_the_only_comment(self) -> None:
        self.assertEqual(["x"], [route.name for route in parse_routes("   # a note\nx 9441 127.0.0.1:3000 /a\n")])

    def test_digits_that_are_not_ascii_or_that_start_with_zero_are_refused_in_a_port_or_an_upstream(self) -> None:
        for line in (
            "x \u0669\u0664\u0664\u0661 127.0.0.1:3000 /\n",
            "x 9441 127.0.0.1:\u0663\u0660\u0660\u0660 /\n",
            "x 09441 127.0.0.1:3000 /\n",
            "x 9441 127.0.0.1:03000 /\n",
        ):
            with self.subTest(line=line):
                self.assertIn("line 1", refused_routes(line))

    def test_a_health_path_with_a_character_outside_the_closed_set_is_refused(self) -> None:
        for path in ('/a"b', "/a{b}", "/a?b=1", "/a\\b", "/a;b", "/a%b"):
            with self.subTest(path=path):
                self.assertIn("health", refused_routes(f"x 9441 127.0.0.1:3000 {path}\n"))

    def test_the_health_paths_of_the_tracked_list_are_in_the_closed_set(self) -> None:
        self.assertEqual(
            [
                "/v1/sys/health",
                "/api/health",
                "/-/healthy",
                "/v1/info",
                "/api/v2/config",
                "/api/v1/health",
                "/server_info",
                "/apis/registry/v3/system/info",
                "/minio/health/live",
            ],
            [route.health_path for route in load_routes(ROUTES_FILE)],
        )

    def test_a_line_with_the_wrong_number_of_fields_is_refused_with_its_line_number(self) -> None:
        self.assertIn("line 2", refused_routes(GOOD.splitlines()[0] + "\nx 9441 127.0.0.1:3000\n"))
        self.assertIn("line 2", refused_routes(GOOD.splitlines()[0] + "\nx 9441 127.0.0.1:3000 / extra\n"))

    def test_an_empty_list_is_refused(self) -> None:
        self.assertIn("no route", refused_routes("# nothing\n"))


class RenderTest(unittest.TestCase):
    def test_a_good_list_renders_one_site_for_each_route_and_one_for_the_gateway_and_passes_the_guard(self) -> None:
        text = render(parse_routes(GOOD))
        verify_rendered(text)
        self.assertEqual(3, len(re.findall(r"^\{\$TS_HOST_NAME\}:\d+ \{$", text, re.MULTILINE)))
        self.assertEqual(2, text.count("reverse_proxy 127.0.0.1:"))

    def test_each_route_forwards_to_its_own_upstream_and_names_itself_in_its_gateway_error(self) -> None:
        text = render(parse_routes(GOOD))
        for route in parse_routes(GOOD):
            block = text.split(f"{{$TS_HOST_NAME}}:{route.port} {{")[1].split("\n}\n")[0]
            self.assertIn(f"reverse_proxy {route.upstream}\n", block)
            self.assertIn(f"behind {route.name} ", block)

    def test_the_rendering_is_the_same_each_time(self) -> None:
        self.assertEqual(render(parse_routes(GOOD)), render(parse_routes(GOOD)))

    def test_a_site_without_a_bind_is_refused_with_its_port_named(self) -> None:
        self.assertIn("9441", refused_render(remove_bind(render(parse_routes(GOOD)), 9441)))

    def test_a_site_bound_to_an_address_other_than_the_tailscale_setting_is_refused(self) -> None:
        text = render(parse_routes(GOOD)).replace("bind {$TS_ADDRESS}", "bind 0.0.0.0", 1)
        self.assertIn("bind", refused_render(text))

    def test_a_site_that_names_a_host_other_than_the_tailscale_name_form_is_refused(self) -> None:
        text = render(parse_routes(GOOD)).replace("{$TS_HOST_NAME}:9441 {", "host.example.net:9441 {")
        self.assertIn("9441", refused_render(text))

    def test_a_plain_http_site_is_refused_because_it_would_open_a_listener_without_a_certificate(self) -> None:
        text = render(parse_routes(GOOD)).replace("{$TS_HOST_NAME}:9441 {", "http://{$TS_HOST_NAME}:9441 {")
        self.assertIn("http", refused_render(text))

    def test_a_test_only_certificate_setting_or_a_certificate_directive_is_refused(self) -> None:
        text = render(parse_routes(GOOD))
        for setting in ("local_certs", "skip_install_trust", "tls internal", "tls /certs/a.pem /certs/a.key"):
            with self.subTest(setting=setting):
                self.assertIn(setting.split()[0], refused_render(text.replace("\tadmin off\n", f"\tadmin off\n\t{setting}\n", 1)))

    def test_the_admin_endpoint_left_on_is_refused(self) -> None:
        text = render(parse_routes(GOOD)).replace("\tadmin off\n", "")
        self.assertIn("admin", refused_render(text))

    def test_the_automatic_redirect_listener_and_http3_left_on_are_refused(self) -> None:
        text = render(parse_routes(GOOD))
        self.assertIn("redirect", refused_render(text.replace("\tauto_https disable_redirects\n", "")))
        self.assertIn("protocols", refused_render(text.replace("\t\tprotocols h1 h2\n", "")))

    def test_a_proxy_to_an_address_other_than_the_local_one_is_refused(self) -> None:
        text = render(parse_routes(GOOD)).replace("reverse_proxy 127.0.0.1:3000", "reverse_proxy 10.0.0.5:3000")
        self.assertIn("reverse_proxy", refused_render(text))

    def test_an_address_literal_anywhere_else_is_refused(self) -> None:
        text = render(parse_routes(GOOD)).replace('respond "gateway ok"', 'respond "gateway ok 100.64.0.1"', 1)
        self.assertIn("address", refused_render(text))


def remove_bind(text: str, port: int) -> str:
    head, tail = text.split(f"{{$TS_HOST_NAME}}:{port} {{\n", 1)
    return head + f"{{$TS_HOST_NAME}}:{port} {{\n" + tail.replace("\tbind {$TS_ADDRESS}\n", "", 1)


class TrackedFilesTest(unittest.TestCase):
    def test_the_tracked_configuration_is_what_the_renderer_writes_for_the_tracked_list(self) -> None:
        self.assertEqual(render(load_routes(ROUTES_FILE)), CADDYFILE.read_text())

    def test_the_tracked_configuration_holds_the_tailscale_name_form_and_no_address_or_certificate_setting(self) -> None:
        text = CADDYFILE.read_text()
        verify_rendered(text)
        sites = re.findall(r"^(\S.*) \{$", text, re.MULTILINE)
        self.assertEqual(["{$TS_HOST_NAME}:" + str(GATEWAY_PORT)] + [f"{{$TS_HOST_NAME}}:{route.port}" for route in load_routes(ROUTES_FILE)], sites)
        self.assertNotRegex(text, r"\.ts\.net|local_certs|tls |skip_install_trust")
        self.assertEqual(["127.0.0.1"], sorted(set(re.findall(r"\d+\.\d+\.\d+\.\d+", text))))


class ServiceFileTest(unittest.TestCase):
    def service(self) -> dict[str, Any]:
        document = load_yaml(SERVICE_FILE.read_text())
        if not isinstance(document, dict):
            raise TypeError("caddy.yml is not a mapping")
        service: dict[str, Any] = document["services"]["caddy"]
        return service

    def test_the_service_uses_the_host_network_and_publishes_no_port_because_the_bind_decides_what_listens(self) -> None:
        service = self.service()
        self.assertEqual("host", service["network_mode"])
        self.assertNotIn("ports", service)

    def test_the_service_refuses_to_start_without_a_usable_tailscale_address_and_the_name_before_caddy_runs(self) -> None:
        command = " ".join(str(part) for part in self.service()["command"])
        self.assertLess(command.index("TS_ADDRESS"), command.index("exec caddy run"))
        self.assertLess(command.index("TS_HOST_NAME"), command.index("exec caddy run"))

    def test_the_health_check_connects_to_the_gateway_port_on_the_tailscale_address(self) -> None:
        test = " ".join(str(part) for part in self.service()["healthcheck"]["test"])
        self.assertRegex(test, rf"\$\$TS_ADDRESS {GATEWAY_PORT}\b")

    def test_the_tailscale_socket_folder_is_the_only_host_path_and_is_read_only(self) -> None:
        paths = [volume for volume in self.service()["volumes"] if volume.startswith("/")]
        self.assertEqual(["/var/run/tailscale:/var/run/tailscale:ro"], paths)


if __name__ == "__main__":
    unittest.main()
