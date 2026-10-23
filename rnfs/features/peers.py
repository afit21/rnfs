"""The peers feature: `rnfs peers` and `rnfs stats` — the nodes this one knows and the traffic it exchanged.

Counters live in a mesh.Traffic shared by the node's PeerClient (requests and verified bytes received per
provider) and RequestServer (/blocks bytes sent, per requester where it identified, and airtime refusals);
this feature reads them. The node's rnfs.dht announce handler counts every announce in them too (Traffic.heard),
so the DHT mode and the last-heard time of client-mode nodes (which never enter the routing table) are known.

Per peer the listing also gives:
- first_heard: the earliest of when this run first counted the peer, when it entered the routing table (kept
  across a short absence, and across restarts) and when RNS's announce cache (Identity.known_destinations, which
  RNS persists) last heard it; null if none knows.
- rtt, rtt_at: the round trip RNS measured on the last link established to it (a shared link, ping's, or an
  identified one for /provide or /pin; LinkPool.on_rtt), in seconds, and when; null if none was.
- online: mesh.online(), the one rule: an rnfs link to it is open, or it showed it was up within
  mesh.ONLINE_WINDOW (15 minutes, on the monotonic clock): an answer to a request (a refusal too), a request of its
  own served, a link established, or an announce of its own emitted within the window (its signed emission time,
  allowing mesh.EMISSION_SKEW of clock difference, and later than the last counted: Traffic.announced). A path
  response, or an announce held in a queue or passed on late by a shared instance, says nothing of now. No network
  I/O.
- last_seen (0.2.1): that last sign it was up, as wall time (Unix seconds), or null if none was seen this run.
  last_heard is no such sign: a path response for the node (which any node with a path may send) refreshes it.

"peer" events (rnfs.events): {"node_id", "change": "new" | "online" | "offline", "online"}. A thread checks every
CHECK_INTERVAL (offline comes with time passing, so it can't be a hook) and diffs the online states of the peers
Traffic counts and those with an open link, both bounded (not the listing, which the routing table may fill: a peer
only in the routing table is never online). Its first check, at start, only records them, and "new" is for a peer
first counted in Traffic (first heard or contacted this run) after that, so neither the start nor the routing table
seeded from RNS's announce cache floods the log: a client lists the peers once, then follows the events, taking
"new" as add-or-update (a peer listed from the routing table gets "new" once it is first heard). A new peer's event
says whether it is online and no "online" follows for it; one online that is no longer counted nor linked gets
"offline"; a peer forgotten beyond mesh.MAX_PEERS and heard again is "new" again.

Nothing is persisted: counts are since the daemon started. Memory is bounded by mesh.MAX_PEERS.
"""
import json
import math
import threading
import time

from rnfs import events, mesh, units
from rnfs.features import Feature, _common
from rnfs.fsutil import printable

NAME = "peers"
# Seconds between checks of the peers' online states for "peer" events: an offline event is at most this late after
# mesh.ONLINE_WINDOW has passed, and a check reads only counters and the pool's open links
CHECK_INTERVAL = 30.0


class PeersFeature(Feature):
    """Known peers with their path, link and traffic, and the node's traffic totals."""

    name = NAME

    def __init__(self, node):
        """Use the client's counters (the server shares them); an offline node's fake client has none."""
        super().__init__(node)
        traffic = getattr(node.client, "traffic", None)
        self.traffic: mesh.Traffic = traffic if isinstance(traffic, mesh.Traffic) else mesh.Traffic()
        from rnfs.log import ThrottledLog  # here, not at module level: it loads RNS, and the CLI imports features
        # A listing that can't read something is a bug or an RNS change, met per peer: a line a minute
        self._failure_log = ThrottledLog()
        self._stop = threading.Event()
        # The online state of each peer at the last check (None before the first), and that check's time
        # (Traffic.monotonic): only a peer counted after the first is "new". Only the check thread (or a test) uses them
        self._states: dict[bytes, bool] | None = None
        self._since: float | None = None

    # Lifecycle

    def start(self):
        """Start the thread publishing "peer" events (an offline test node calls check() itself)."""
        self._spawn(self._run, "rnfs-peers")

    def stop(self, deadline: float | None = None):
        """Stop the events thread, waiting for it until deadline (as Feature.stop)."""
        self._stop.set()
        self._join(deadline)

    def _run(self):
        """check() now (recording the states without events), then every CHECK_INTERVAL until stop."""
        while True:
            try:
                self.check()
            except Exception as e:  # noqa: BLE001 - the thread keeps running; logged
                self._failed(f"couldn't check the peers' online states: {e!r}")
            if self._stop.wait(CHECK_INTERVAL):
                return

    def check(self):
        """Publish a "peer" event for each peer new since the last check (first counted after the first check) or
        whose online state changed; the first check only records the states. See the module docstring.

        Over the peers Traffic counts and those with an open link (both bounded), not the listing, which the
        routing table may fill to mesh.MAX_PEERS: a peer cut from it isn't offline. A peer only in the routing table
        is never online (nothing of it was seen), so leaving it out loses no event. Decided by Traffic.monotonic."""
        now = self.traffic.monotonic()
        counters = self.traffic.peers()
        linked = self._linked()
        watched = [node_id for node_id in dict.fromkeys([*linked, *counters]) if node_id != self.node.node_id]
        states = {node_id: mesh.online(node_id in linked, (counters.get(node_id) or {}).get("last_seen"), now)
                  for node_id in watched}
        previous, self._states = self._states, states
        if previous is None:
            self._since = now
            return
        for node_id, online in states.items():
            before = previous.get(node_id)
            if before is None:
                first = (counters.get(node_id) or {}).get("first_counted")
                if first is not None and first >= self._since:
                    change = "new"
                elif online:  # counted before the first check, without a link then (forgotten, counted again)
                    change = "online"
                else:
                    continue
            elif before != online:
                change = "online" if online else "offline"
            else:
                continue
            events.publish(self.node, "peer", node_id=node_id.hex(), change=change, online=online)
        # One no longer watched (its link closed with nothing counted, or forgotten beyond mesh.MAX_PEERS) is offline
        for node_id, before in previous.items():
            if before and node_id not in states:
                events.publish(self.node, "peer", node_id=node_id.hex(), change="offline", online=False)

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

    def _linked(self) -> set[bytes]:
        """The nodes with an open shared link (empty, logged, if they can't be listed)."""
        try:
            return set(self.node.client.open_links())
        except Exception as e:  # noqa: BLE001 - a listing shows what it can; logged
            self._failed(f"couldn't list the open links: {e!r}")
            return set()

    def _known(self, counters: dict, linked: set[bytes]) -> list[bytes]:
        """The peers listed: in the routing table, with an open link, or in counters (most recent first); this node
        left out, at most mesh.MAX_PEERS."""
        node = self.node
        ids = [n for n in dict.fromkeys([*node.routing.nodes(), *linked, *reversed(list(counters))])
               if n != node.node_id]
        return ids[:mesh.MAX_PEERS]

    def peers(self) -> list[dict]:
        """Every known node: in the routing table, with an open link, or in the traffic counters; nearest first."""
        node = self.node
        routing = node.routing
        paths = getattr(node, "paths", None)
        counters = self.traffic.peers()
        linked = self._linked()
        ids = self._known(counters, linked)
        now, monotonic = self.traffic.clock(), self.traffic.monotonic()  # wall time to show, monotonic to decide
        # All at once: attached to a shared instance, RnsPaths reads the interfaces from one RPC, not one each
        known = paths.infos(ids) if paths is not None else {}
        peers = []
        for node_id in ids:
            counts = counters.get(node_id) or {}
            seniority = routing.seniority(node_id)
            path = known.get(node_id, mesh.NO_PATH)
            recalled = self._recalled_heard(node_id)  # RNS's announce cache: one lookup for both times
            rate = None
            if node_id in linked:
                try:
                    rate = node.client.rate_estimate(node_id)
                except Exception as e:  # noqa: BLE001 - a listing shows what it can; logged
                    self._failed(f"couldn't read the rate estimate of {node_id.hex()}: {e!r}")
                    rate = None
            peers.append({
                "node_id": node_id.hex(),
                "mode": counts.get("mode") or ("server" if seniority is not None else self._recalled_mode(node_id)),
                "in_table": seniority is not None,
                "seniority": seniority,
                "senior": seniority is not None and seniority >= node.config.provider_seniority,
                "hops": path.hops, "interface": path.interface, "bitrate": path.bitrate,
                "link": node_id in linked, "rate": rate,
                "last_heard": counts.get("last_heard") or recalled,
                "last_active": counts.get("last_active"),
                "banned": routing.is_banned(node_id),
                **{key: counts.get(key, 0) for key in ("sent", "received", "requests", "answered", "refused",
                                                      "failed", "bad")},
                "first_heard": _earliest(counts.get("first_heard"),
                                         None if seniority is None else now - seniority, recalled),
                "rtt": counts.get("rtt"), "rtt_at": counts.get("rtt_at"),
                "online": mesh.online(node_id in linked, counts.get("last_seen"), monotonic),
                "last_seen": _wall(counts.get("last_seen"), now, monotonic),
            })
        peers.sort(key=lambda p: (p["hops"] if p["hops"] is not None else math.inf, p["node_id"]))
        return peers

    def stats(self) -> dict:
        """Traffic since start: totals, airtime refusals, per-interface bytes and the budget left; and (0.2.1)
        Reticulum's interfaces, read-only (mesh.interface_states; [] if they can't be read, logged)."""
        snapshot = self.traffic.snapshot()
        snapshot["upload_budget"] = self.node.config.upload_budget
        snapshot["upload_budget_left"] = self._budget_remaining()
        try:
            snapshot["reticulum_interfaces"] = mesh.interface_states(getattr(self.node, "reticulum", None))
        except Exception as e:  # noqa: BLE001 - RNS internals or the instance's RPC: a listing shows what it can
            self._failed(f"couldn't read Reticulum's interfaces: {e!r}")
            snapshot["reticulum_interfaces"] = []
        return snapshot

    def _recalled_mode(self, node_id: bytes) -> str | None:
        """The DHT mode in the announce RNS last cached for node_id, or None (or without Reticulum)."""
        if getattr(self.node, "reticulum", None) is None:
            return None
        try:
            import RNS
            from rnfs.constants import MODE_SERVER
            from rnfs.network.announces import parse_mode
            app_data = RNS.Identity.recall_app_data(node_id, _no_use=True)  # a listing doesn't mark it used
            if app_data is None:
                return None
            return "server" if parse_mode(app_data) == MODE_SERVER else "client"
        except Exception as e:  # noqa: BLE001 - RNS internals: unknown if they fail; logged
            self._failed(f"couldn't read {node_id.hex()}'s last announce from RNS: {e!r}")
            return None

    def _recalled_heard(self, node_id: bytes) -> float | None:
        """When RNS last heard node_id's announce (an RNS internal: Identity.known_destinations), or None."""
        if getattr(self.node, "reticulum", None) is None:
            return None
        try:
            import RNS
            entry = RNS.Identity.known_destinations.get(node_id)
            return float(entry[0]) if entry else None
        except Exception as e:  # noqa: BLE001 - RNS internals: unknown if they fail; logged
            self._failed(f"couldn't read when RNS last heard {node_id.hex()}: {e!r}")
            return None

    def _failed(self, message: str):
        """Log, at most once a minute, that a listing couldn't read something. Called inside the except, so the
        debug log has the traceback."""
        self._failure_log(f"peers: {message}", _common.LOG_WARNING)

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
                        "known nodes: mode, seniority, hops, interface, link rate, last heard, traffic, "
                        "round trip, online")
        peers.add_argument("--json", action="store_true", help="print JSON")
        stats = command(commands, "stats", run_stats, "traffic since start: totals, refusals, per interface")
        stats.add_argument("--json", action="store_true", help="print JSON")


def _earliest(*times: float | None) -> float | None:
    """The earliest of times that aren't None, or None. first_heard: each source (this run's counters, the routing
    table, RNS's announce cache) knows of a time the peer had been heard by, so the earliest is the best answer."""
    known = [value for value in times if value is not None]
    return min(known) if known else None


def _wall(seen: float | None, now: float, monotonic: float) -> float | None:
    """A Traffic.monotonic time as wall time (now and monotonic read together), or None. Never later than now: a
    sign of life dated ahead by an announce's allowed skew is shown as now."""
    return None if seen is None else now - max(0.0, monotonic - seen)


def _api_peers(handler):
    """GET /api/peers: {"peers": [...]}."""
    handler.send_json(200, {"peers": _common.feature(handler, NAME).peers()})


def _api_stats(handler):
    """GET /api/stats: traffic totals, refusals and per-interface bytes."""
    handler.send_json(200, _common.feature(handler, NAME).stats())


# Output (no RNS here: the CLI must not load it)

def human_size(num_bytes: float | None) -> str:
    """A size like "1.50 MB" (units.human_size), or "-" for None."""
    return units.human_size(num_bytes, unknown="-")


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


def human_rtt(seconds) -> str:
    """A round-trip time like "85ms" or "2.4s", or "-" for None (or anything but a number)."""
    if not isinstance(seconds, (int, float)) or isinstance(seconds, bool) or not 0 <= seconds < math.inf:
        return "-"
    return f"{seconds * 1000:.0f}ms" if seconds < 1 else f"{seconds:.1f}s"


def format_peers(peers: list[dict], now: float) -> list[str]:
    """The `rnfs peers` table, one line per node, escaped (the daemon's fields mustn't drive the terminal). RTT and
    ONLINE come last, so the older columns keep their places; a daemon before 0.2.0 sends neither ("-")."""
    lines = [f"{'NODE':<32} {'MODE':<7} {'TABLE':>6} {'HOPS':>4} {'INTERFACE':<20} {'RATE':>10} {'HEARD':>6} "
             f"{'SENT':>9} {'RECV':>9} {'FAIL':>5} {'RTT':>7} ONLINE"]
    for peer in peers:
        table = human_age(peer["seniority"]) if peer["in_table"] else "-"
        heard = human_age(now - peer["last_heard"]) if peer.get("last_heard") else "-"
        rate = human_rate(peer["rate"]) if peer["link"] else "-"
        interface = (peer.get("interface") or "-")[:20]
        online = {True: "yes", False: "no"}.get(peer.get("online"), "-")
        flags = (" banned" if peer["banned"] else "") + (f" bad:{peer['bad']}" if peer.get("bad") else "")
        # Each text field escaped before it is padded, so the columns line up; the whole line again for the rest
        lines.append(printable(
            f"{printable(peer['node_id']):<32} {printable(peer['mode'] or '?'):<7} {table:>6} "
            f"{peer['hops'] if peer['hops'] is not None else '?':>4} {printable(interface):<20} "
            f"{rate:>10} {heard:>6} {human_size(peer['sent']):>9} {human_size(peer['received']):>9} "
            f"{peer['failed']:>5} {human_rtt(peer.get('rtt')):>7} {online}{flags}"))
    return lines


def format_stats(stats: dict) -> list[str]:
    """The `rnfs stats` report, escaped (the daemon's fields mustn't drive the terminal)."""
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
        lines.append(f"interface {name}: sent {human_size(counters['sent'])}, "
                     f"received {human_size(counters['received'])}")
    for interface in stats.get("reticulum_interfaces") or []:  # 0.2.1
        state = {True: "up", False: "down"}.get(interface.get("online"), "?")
        details = ", ".join(part for part in (
            interface.get("mode"), f"{interface['bitrate'] / 1000:g} kbit/s" if interface.get("bitrate") else None,
            f"rx {human_size(interface.get('rx_bytes'))}, tx {human_size(interface.get('tx_bytes'))}") if part)
        lines.append(f"reticulum {interface.get('name')} ({interface.get('type') or '?'}): {state}; {details}")
    return [printable(line) for line in lines]


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
