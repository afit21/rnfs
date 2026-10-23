"""AUD-H2 (0.2.1 audit): the gateway's *.localhost site origins could be squatted on [::1] while the daemon ran.

LocalServer bound 127.0.0.1 only, but the gateway's sites are http://<k-cid>.rnfs.localhost:<port>/ and
http://<name>.name.localhost:<port>/ (rnfs url, README), and browsers and curl try a *.localhost name on ::1 first.
[::1]:<port> was free, so any other local user could listen there while the daemon ran and serve its own HTML in
every site's real origin (reading the site's localStorage, forging any CID's or name's content), and relay the API
proof and token of a client dialing "localhost" rather than 127.0.0.1.

The test rebuilds the squat as the audit's probe did: with an ApiServer or a GatewayServer listening on port P, a
second socket binds [::1]:P. Now LocalServer listens on [::1]:P too (IPV6_V6ONLY, the same handler, connection slots
and Windows exclusive binding), so that bind fails, and a request over ::1 reaches the daemon's own handler: a site
host gets the real page with its CSP, the API answers the proof. A daemon whose [::1]:P is already taken refuses to
start, as for 127.0.0.1:P, leaving 127.0.0.1:P free again; only a host without IPv6 runs on 127.0.0.1 alone, with a
log line (forced here by refusing the bind, so it runs on every OS). Port 0 binds both families on the same port,
picking another when [::1] has the one 127.0.0.1 got. The tests that need a real ::1 skip on a host without one.
"""
import errno
import http.client
import json
import socket
import unittest
from unittest import mock

from rnfs import api, fsutil
from rnfs.api import ApiServer, BoundedHTTPServer, LocalServer
from rnfs.apiclient import CHALLENGE_HEADER, PROOF_HEADER, api_proof
from rnfs.config import Config
from rnfs.content.cid import Cid, to_base36
from rnfs.gateway import GatewayServer

from tests.support import wait_for
from tests.test_api import FakeNode, free_slots, raw_request

LocalServer.poll_interval = 0.01
CHALLENGE = "0123456789abcdef" * 2


def _has_ipv6_loopback() -> bool:
    """Whether this host can listen on ::1 (some CI runners and containers have no IPv6)."""
    if not socket.has_ipv6:
        return False
    try:
        with socket.socket(socket.AF_INET6) as probe:
            probe.bind(("::1", 0))
    except OSError:
        return False
    return True


HAS_IPV6 = _has_ipv6_loopback()


def squat(port: int, reuse: bool = False) -> socket.socket:
    """Another local user's listener on [::1]:port (with SO_REUSEADDR if reuse). Raises OSError if it can't."""
    sock = socket.socket(socket.AF_INET6)
    try:
        if reuse:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("::1", port))
        sock.listen()
    except OSError:
        sock.close()
        raise
    return sock


def v6_request(port: int, path: str, host: str, headers: dict | None = None):
    """(status, headers, body) of a GET over [::1]:port with exactly this Host."""
    connection = http.client.HTTPConnection("::1", port, timeout=10)
    try:
        connection.putrequest("GET", path, skip_host=True, skip_accept_encoding=True)
        connection.putheader("Host", host)
        for name, value in (headers or {}).items():
            connection.putheader(name, value)
        connection.endheaders()
        response = connection.getresponse()
        return response.status, response.headers, response.read()
    finally:
        connection.close()


def refuse_ipv6(error: int, times: int | None = None):
    """Patch BoundedHTTPServer.server_bind to fail with error for ::1 (every time, or the first times); the list
    it returns counts the attempts."""
    original = BoundedHTTPServer.server_bind
    attempts = []

    def server_bind(server):
        """Fail the ::1 bind as a host would; bind 127.0.0.1 as usual."""
        if server.address_family == socket.AF_INET6:
            attempts.append(server.server_address)
            if times is None or len(attempts) <= times:
                raise OSError(error, f"forced {errno.errorcode.get(error, error)}")
        original(server)

    return mock.patch.object(BoundedHTTPServer, "server_bind", server_bind), attempts


@unittest.skipUnless(HAS_IPV6, "this host has no IPv6 loopback")
class SquatTest(unittest.TestCase):
    """A real ::1: the daemon holds it, so nobody else can."""

    def api_server(self, port: int = 0) -> ApiServer:
        """A started ApiServer on port."""
        server = ApiServer(FakeNode(), port)
        server.start()
        self.addCleanup(server.stop)
        return server

    def test_api_port_cant_be_squatted_on_ipv6(self):
        server = self.api_server()
        for reuse in (False, True):
            with self.subTest(reuse=reuse), self.assertRaises(OSError):
                squat(server.port, reuse).close()

    def test_gateway_site_is_served_over_ipv6(self):
        node = FakeNode()
        root = node.put_dir({"index.html": node.put_file(b"<p>the real site</p>")})
        gateway = GatewayServer(node, Config(gateway_fetches=2), 0)
        gateway.start()
        self.addCleanup(gateway.stop)
        with self.assertRaises(OSError):
            squat(gateway.port).close()
        host = f"{to_base36(Cid.parse(root))}.rnfs.localhost:{gateway.port}"
        status, headers, body = v6_request(gateway.port, "/", host)
        self.assertEqual((status, body), (200, b"<p>the real site</p>"))
        self.assertIn("allow-same-origin", headers["Content-Security-Policy"])
        # The path gateway as on 127.0.0.1; an IP literal is no host of its, on either family
        self.assertEqual(v6_request(gateway.port, f"/rnfs/{root}/", f"localhost:{gateway.port}")[0], 200)
        self.assertEqual(v6_request(gateway.port, f"/rnfs/{root}/", f"[::1]:{gateway.port}")[0], 403)

    def test_api_answers_its_proof_over_ipv6(self):
        server = self.api_server()
        status, headers, body = v6_request(server.port, "/api/status", f"localhost:{server.port}",
                                           {CHALLENGE_HEADER: CHALLENGE})
        self.assertEqual(status, 200)
        self.assertEqual(headers[PROOF_HEADER], api_proof(server.token, server.port, CHALLENGE))
        self.assertEqual(set(json.loads(body)), {"version"})
        # The Host rule is unchanged: 127.0.0.1 or localhost only, whichever loopback the request came over
        status, _, body = v6_request(server.port, "/api/status", f"[::1]:{server.port}")
        self.assertEqual((status, json.loads(body)["code"]), (403, "bad_host"))

    def test_both_loopbacks_share_the_connection_slots(self):
        with mock.patch.object(ApiServer, "max_connections", 2):
            server = self.api_server()
        held = socket.create_connection(("::1", server.port))
        self.addCleanup(held.close)
        self.assertTrue(wait_for(lambda: free_slots(server) == 1, what="the ::1 connection holds a slot"))
        other = socket.create_connection(("127.0.0.1", server.port))
        self.addCleanup(other.close)
        self.assertTrue(wait_for(lambda: free_slots(server) == 0, what="both connections hold slots"))
        status = v6_request(server.port, "/api/status", f"localhost:{server.port}")[0]
        self.assertEqual(status, 503)

    def test_a_squatted_ipv6_port_stops_the_daemon(self):
        """[::1]:P taken before the daemon starts: it refuses, as for 127.0.0.1:P, and doesn't keep that one."""
        squatter = squat(0)
        self.addCleanup(squatter.close)
        port = squatter.getsockname()[1]
        with self.assertRaises(OSError) as raised:
            ApiServer(FakeNode(), port)
        self.assertIn(raised.exception.errno, api.PORT_TAKEN)
        self.assertIn("[::1]", str(raised.exception))
        with socket.socket() as again:
            again.bind(("127.0.0.1", port))  # released

    def test_windows_binds_ipv6_exclusively(self):
        with mock.patch.object(fsutil, "IS_WINDOWS", True):
            server = ApiServer(FakeNode(), 0)
        self.addCleanup(server.stop)
        [v6] = [each for each in server.servers if each.address_family == socket.AF_INET6]
        self.assertFalse(v6.socket.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR))
        self.assertTrue(v6.socket.getsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY))
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            self.assertTrue(v6.socket.getsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE))


class NoIpv6Test(unittest.TestCase):
    """What a host without IPv6, or a taken [::1] port, does (the bind refused by a patch: runs on every OS)."""

    def test_no_ipv6_runs_on_127_0_0_1_alone_and_says_so(self):
        for error in sorted(api.NO_IPV6):
            patch, _ = refuse_ipv6(error)
            with self.subTest(error=errno.errorcode.get(error, error)), patch, mock.patch.object(api, "log") as log:
                server = ApiServer(FakeNode(), 0)
                server.start()
                self.addCleanup(server.stop)
                self.assertEqual([each.address_family for each in server.servers], [socket.AF_INET])
                self.assertIn("::1", log.call_args.args[0])
                self.assertEqual(raw_request(server.port, "GET", "/api/status")[0], 200)

    def test_ipv6_port_in_use_is_fatal_for_a_set_port(self):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        patch, attempts = refuse_ipv6(errno.EADDRINUSE)
        with patch, self.assertRaises(OSError) as raised:
            ApiServer(FakeNode(), port)
        self.assertEqual((raised.exception.errno, len(attempts)), (errno.EADDRINUSE, 1))
        with socket.socket() as again:
            again.bind(("127.0.0.1", port))  # 127.0.0.1:port was released

    def test_port_zero_picks_another_port_when_ipv6_has_it(self):
        patch, attempts = refuse_ipv6(errno.EADDRINUSE, times=1)
        with patch:
            server = ApiServer(FakeNode(), 0)
        self.addCleanup(server.stop)
        self.assertEqual({each.server_address[1] for each in server.servers}, {server.port})
        if HAS_IPV6:  # else the socket can't even be made, and the patch is never reached
            self.assertEqual(len(attempts), 2)
            self.assertNotEqual(attempts[0][1], server.port)
            self.assertEqual(len(server.servers), 2)


if __name__ == "__main__":
    unittest.main()
