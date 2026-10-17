"""The peers feature: `rnfs peers` and `rnfs stats` — the nodes this one knows and the traffic it exchanged.

Counters live in a mesh.Traffic shared by the node's PeerClient (requests and verified bytes received per
provider) and RequestServer (/blocks bytes sent, per requester where it identified, and airtime refusals);
this feature reads them. It also registers an rnfs.dht announce handler of its own, so the DHT mode and the
last-heard time of client-mode nodes (which never enter the routing table) are known too.

Nothing is persisted: counts are since the daemon started. Memory is bounded by mesh.MAX_PEERS.
"""
import json
import math
import time

from rnfs import mesh
from rnfs.features import Feature, _common
from rnfs.fsutil import printable

NAME = "peers"


class PeersFeature(Feature):
    """Known peers with their path, link and traffic, and the node's traffic totals."""

    name = NAME

    def __init__(self, node):
        """Use the client's counters (the server shares them); an offline node's fake client has none."""
        super().__init__(node)
        traffic = getattr(node.client, "traffic", None)
        self.traffic: mesh.Traffic = traffic if isinstance(traffic, mesh.Traffic) else mesh.Traffic()
        self._handler = None

    def start(self):
        """Listen to rnfs.dht announces for modes and last-heard times."""
        if getattr(self.node, "reticulum", None) is None:
            return
        from rnfs.network.announces import DhtAnnounceHandler
        self._handler = DhtAnnounceHandler(lambda node_id: self.traffic.heard(node_id, "server"),
                                           lambda node_id: self.traffic.heard(node_id, "client"),
                                           self.node.routing.is_banned, self.node.node_id)
        self._handler.register()

    def stop(self):
        """Stop listening to announces."""
        if self._handler is not None:
            self._handler.deregister()
            self._handler = None

    def status(self) -> dict:
        """Traffic totals and airtime refusals for `rnfs status`."""
        snapshot = self.traffic.snapshot()
        fields = {"bytes_sent": snapshot["totals"]["sent"], "bytes_received": snapshot["totals"]["received"]}
        fields.update({f"blocks_refused_{reason}": count for reason, count in snapshot["refused"].items()})
        remaining = self._budget_remaining()
        if remaining is not None:
            fields["upload_budget_left"] = remaining
        return fields

    def _budget_remaining(self) -> int | None:
        """Bytes left in upload_budget, or None without one (or without a server)."""
        airtime = getattr(getattr(self.node, "server", None), "airtime", None)
        return airtime.remaining() if airtime is not None else None

    # Queries

    def peers(self) -> list[dict]:
        """Every known node: in the routing table, with an open link, or in the traffic counters; nearest first."""
        node = self.node
        routing = node.routing
        paths = getattr(node, "paths", None)
        path_info = paths.info if paths is not None else mesh.no_path_info
        counters = self.traffic.peers()
        try:
            linked = set(node.client.open_links())
        except Exception:  # noqa: BLE001 - a listing shows what it can
            linked = set()
        ids = [n for n in dict.fromkeys([*routing.nodes(), *linked, *reversed(list(counters))]) if n != node.node_id]
        peers = []
        for node_id in ids[:mesh.MAX_PEERS]:
            counts = counters.get(node_id) or {}
            seniority = routing.seniority(node_id)
            path = path_info(node_id)
            rate = None
            if node_id in linked:
                try:
                    rate = node.client.rate_estimate(node_id)
                except Exception:  # noqa: BLE001 - a listing shows what it can
                    rate = None
            peers.append({
                "node_id": node_id.hex(),
                "mode": counts.get("mode") or ("server" if seniority is not None else self._recalled_mode(node_id)),
                "in_table": seniority is not None,
                "seniority": seniority,
                "senior": seniority is not None and seniority >= node.config.provider_seniority,
                "hops": path.hops, "interface": path.interface, "bitrate": path.bitrate,
                "link": node_id in linked, "rate": rate,
                "last_heard": counts.get("last_heard") or self._recalled_heard(node_id),
                "last_active": counts.get("last_active"),
                "banned": routing.is_banned(node_id),
                **{key: counts.get(key, 0) for key in ("sent", "received", "requests", "answered", "refused",
                                                      "failed", "bad")},
            })
        peers.sort(key=lambda p: (p["hops"] if p["hops"] is not None else math.inf, p["node_id"]))
        return peers

    def stats(self) -> dict:
        """Traffic since start: totals, airtime refusals, per-interface bytes and the budget left."""
        snapshot = self.traffic.snapshot()
        snapshot["upload_budget"] = self.node.config.upload_budget
        snapshot["upload_budget_left"] = self._budget_remaining()
        return snapshot

    def _recalled_mode(self, node_id: bytes) -> str | None:
        """The DHT mode in the announce RNS last cached for node_id, or None (or without Reticulum)."""
        if getattr(self.node, "reticulum", None) is None:
            return None
        try:
            import RNS
            from rnfs.constants import MODE_SERVER
            from rnfs.network.announces import parse_mode
            app_data = RNS.Identity.recall_app_data(node_id)
            if app_data is None:
                return None
            return "server" if parse_mode(app_data) == MODE_SERVER else "client"
        except Exception:  # noqa: BLE001 - RNS internals: unknown if they fail
            return None

    def _recalled_heard(self, node_id: bytes) -> float | None:
        """When RNS last heard node_id's announce (an RNS internal: Identity.known_destinations), or None."""
        if getattr(self.node, "reticulum", None) is None:
            return None
        try:
            import RNS
            entry = RNS.Identity.known_destinations.get(node_id)
            return float(entry[0]) if entry else None
        except Exception:  # noqa: BLE001 - RNS internals: unknown if they fail
            return None

    # API

    @classmethod
    def api_routes(cls) -> dict:
        """GET /api/peers and GET /api/stats."""
        return {("GET", "/api/peers"): _api_peers, ("GET", "/api/stats"): _api_stats}

    # CLI

    @classmethod
    def cli_commands(cls, commands, command):
        """rnfs peers [--json] and rnfs stats [--json]."""
        peers = command(commands, "peers", run_peers,
                        "known nodes: mode, seniority, hops, interface, link rate, last heard, traffic")
        peers.add_argument("--json", action="store_true", help="print JSON")
        stats = command(commands, "stats", run_stats, "traffic since start: totals, refusals, per interface")
        stats.add_argument("--json", action="store_true", help="print JSON")


def _api_peers(handler):
    """GET /api/peers: {"peers": [...]}."""
    handler.send_json(200, {"peers": _common.feature(handler, NAME).peers()})


def _api_stats(handler):
    """GET /api/stats: traffic totals, refusals and per-interface bytes."""
    handler.send_json(200, _common.feature(handler, NAME).stats())


# Output (no RNS here: the CLI must not load it)

def human_size(num_bytes: float | None) -> str:
    """A short size like "1.5 MB", or "-" for None."""
    if num_bytes is None:
        return "-"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num_bytes) < 1000 or unit == "TB":
            return f"{num_bytes:.0f} {unit}" if unit == "B" else f"{num_bytes:.1f} {unit}"
        num_bytes /= 1000
    return f"{num_bytes:.1f} TB"


def human_rate(bits: float | None) -> str:
    """A bitrate like "1.2 kbps", or "-"."""
    if not bits:
        return "-"
    for unit in ("bps", "kbps", "Mbps", "Gbps"):
        if bits < 1000 or unit == "Gbps":
            return f"{bits:.0f} {unit}" if unit == "bps" else f"{bits:.1f} {unit}"
        bits /= 1000
    return f"{bits:.1f} Gbps"


def human_age(seconds: float | None) -> str:
    """A duration like "5m", "3h", "2d", or "-"."""
    if seconds is None:
        return "-"
    seconds = max(0.0, seconds)
    for limit, divisor, unit in ((60, 1, "s"), (3600, 60, "m"), (86400, 3600, "h")):
        if seconds < limit:
            return f"{seconds // divisor:.0f}{unit}"
    return f"{seconds // 86400:.0f}d"


def format_peers(peers: list[dict], now: float) -> list[str]:
    """The `rnfs peers` table, one line per node."""
    lines = [f"{'NODE':<32} {'MODE':<7} {'TABLE':>6} {'HOPS':>4} {'INTERFACE':<20} {'RATE':>10} {'HEARD':>6} "
             f"{'SENT':>9} {'RECV':>9} {'FAIL':>5}"]
    for peer in peers:
        table = human_age(peer["seniority"]) if peer["in_table"] else "-"
        heard = human_age(now - peer["last_heard"]) if peer.get("last_heard") else "-"
        rate = human_rate(peer["rate"]) if peer["link"] else "-"
        interface = (peer.get("interface") or "-")[:20]
        flags = (" banned" if peer["banned"] else "") + (f" bad:{peer['bad']}" if peer.get("bad") else "")
        lines.append(f"{printable(peer['node_id']):<32} {printable(peer['mode'] or '?'):<7} {table:>6} "
                     f"{peer['hops'] if peer['hops'] is not None else '?':>4} {printable(interface):<20} "
                     f"{rate:>10} {heard:>6} {human_size(peer['sent']):>9} {human_size(peer['received']):>9} "
                     f"{peer['failed']:>5}{flags}")
    return lines


def format_stats(stats: dict) -> list[str]:
    """The `rnfs stats` report."""
    totals = stats["totals"]
    lines = [f"since: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(stats['since']))}",
             f"sent: {human_size(totals['sent'])} in {totals['responses']} /blocks responses",
             f"received: {human_size(totals['received'])} from {totals['requests']} requests "
             f"({totals['answered']} answered, {totals['refused']} refused, {totals['failed']} failed)"]
    for reason, count in stats["refused"].items():
        lines.append(f"/blocks refused ({reason.replace('_', ' ')}): {count}")
    if stats.get("upload_budget"):
        lines.append(f"upload budget: {human_size(stats.get('upload_budget_left'))} left of "
                     f"{human_size(stats['upload_budget'])} per hour")
    for name, counters in sorted(stats["interfaces"].items()):
        lines.append(f"interface {printable(name)}: sent {human_size(counters['sent'])}, "
                     f"received {human_size(counters['received'])}")
    return lines


def run_peers(args):
    """Print the known nodes."""
    import rnfs.cli
    peers = rnfs.cli.client(args).get("/api/peers")["peers"]
    if args.json:
        print(json.dumps(peers, indent=2))
        return
    for line in format_peers(peers, time.time()):
        print(line)


def run_stats(args):
    """Print the traffic report."""
    import rnfs.cli
    stats = rnfs.cli.client(args).get("/api/stats")
    if args.json:
        print(json.dumps(stats, indent=2))
        return
    for line in format_stats(stats):
        print(line)


FEATURE = PeersFeature
