"""rnfs.dht announces carrying the node's DHT mode, and the handler that reads them (section 3).

The announce's app_data is the DHT mode as a msgpack integer: 1 server, 0 client; anything other than
msgpack 1 means client. It is set with Destination.set_default_app_data(), so path responses carry it too.
Ratchets are not enabled: links are already forward secret.
"""
import io
import threading
import time
from typing import Callable

import RNS
from RNS.vendor import umsgpack

from rnfs.config import Config
from rnfs.constants import APP_NAME, DHT_ASPECT, MODE_CLIENT, MODE_SERVER
from rnfs.log import LOG_VERBOSE, LOG_WARNING, log, node_name


def encode_mode(server: bool) -> bytes:
    """app_data for a node in server (True) or client mode."""
    return umsgpack.packb(MODE_SERVER if server else MODE_CLIENT)


def parse_mode(app_data) -> int:
    """MODE_SERVER if app_data is exactly one msgpack integer 1, else MODE_CLIENT."""
    if not isinstance(app_data, (bytes, bytearray)) or not app_data:
        return MODE_CLIENT
    stream = io.BytesIO(bytes(app_data))
    try:
        value = umsgpack.unpack(stream)
    except Exception:  # noqa: BLE001 - umsgpack raises several types for hostile bytes
        return MODE_CLIENT
    # Booleans are a separate msgpack type, and trailing bytes make it something other than msgpack 1
    if type(value) is int and value == MODE_SERVER and not stream.read(1):
        return MODE_SERVER
    return MODE_CLIENT


REACH_CHECK = 60.0  # seconds between checks of whether a client has started to need announcing


class DhtAnnouncer:
    """Announces rnfs.dht at startup, after startup_announce_delay, then every announce_interval.

    A client announces periodically only while needs_reach() is True (something needs other nodes to reach it:
    it provides, keeps or owns names, accepts remote pins, or is in private mode), and at once when that starts:
    a transport node forwards a path request only for a destination it has a path to (on its default interface
    mode), so a node that never announces can't be reached beyond one hop. One that only fetches needs no path to
    it, so it stops announcing after the startup announces (section 3). Servers always announce.
    """

    def __init__(self, destination: RNS.Destination, config: Config, needs_reach: Callable[[], bool] | None = None,
                 clock: Callable[[], float] = time.monotonic):
        """Set destination's app_data to the mode (config.dht_mode); start() announces. needs_reach: the client's
        test (None: always announce)."""
        self.destination = destination
        self.config = config
        self.server = config.dht_mode == "server"
        self.needs_reach = needs_reach
        self.clock = clock
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        destination.set_default_app_data(encode_mode(self.server))

    def announce(self):
        """Announce now, with the current mode as app_data."""
        try:
            self.destination.announce()
            log(f"announced as a DHT {'server' if self.server else 'client'}", LOG_VERBOSE)
        except Exception as e:  # noqa: BLE001 - announcing is retried at the next interval; logged
            log(f"could not announce: {e}", LOG_WARNING)

    def start(self):
        """Start the announce schedule on a background thread."""
        if self.thread is None:
            self.thread = threading.Thread(target=self._run, name="rnfs-announcer", daemon=True)
            self.thread.start()

    def stop(self):
        """Stop announcing."""
        self.stop_event.set()

    def _needed(self) -> bool:
        """True if the periodic announces are due to go out: always on a server, while needs_reach() on a client."""
        if self.server or self.needs_reach is None:
            return True
        try:
            return bool(self.needs_reach())
        except Exception as e:  # noqa: BLE001 - a failing test must not stop announcing: announce, logged
            log(f"could not tell whether to announce: {e!r}", LOG_WARNING)
            return True

    def _run(self):
        """Announce now, again after startup_announce_delay (interfaces may not have been up), then periodically
        while needed, and at once when a client comes to need it."""
        self.announce()
        if self.stop_event.wait(self.config.startup_announce_delay):
            return
        self.announce()
        last = self.clock()
        needed = True  # the startup announces count: a client that already needs reaching isn't announced again
        while not self.stop_event.wait(min(REACH_CHECK, self.config.announce_interval)):
            was_needed, needed = needed, self._needed()
            if needed and (not was_needed or self.clock() - last >= self.config.announce_interval):
                self.announce()
                last = self.clock()


class DhtAnnounceHandler:
    """RNS announce handler for rnfs.dht, including path responses, reporting each node's DHT mode."""

    def __init__(self, on_server: Callable[[bytes], None], on_client: Callable[[bytes], None],
                 is_banned: Callable[[bytes], bool] = lambda node_id: False, own_id: bytes | None = None,
                 on_announced: Callable[[bytes], None] | None = None):
        """Handler calling on_server(node_id) or on_client(node_id) for each announce or path response not from a
        banned node, then on_announced(node_id), if given, for an announce only: a transport node may answer a
        path request from its path table long after the node went away. Not enough on its own: a shared instance
        passes path answers on as announces, and announces can arrive late, so on_announced (RnfsNode._announced)
        goes by the announce's emission time."""
        # RNS only calls us for announces whose destination hash matches this full name
        self.aspect_filter = f"{APP_NAME}.{DHT_ASPECT}"
        self.receive_path_responses = True
        self.on_server = on_server
        self.on_client = on_client
        self.is_banned = is_banned
        self.own_id = own_id
        self.on_announced = on_announced

    def received_announce(self, destination_hash, announced_identity, app_data, announce_packet_hash=None,
                          is_path_response=False):
        """RNS calls this (on a thread of its own) for each rnfs.dht announce or path response; with five
        parameters, it says which (is_path_response)."""
        node_id = bytes(destination_hash)
        if node_id == self.own_id or self.is_banned(node_id):
            return
        try:
            if parse_mode(app_data) == MODE_SERVER:
                self.on_server(node_id)
            else:
                self.on_client(node_id)
            if not is_path_response and self.on_announced is not None:
                self.on_announced(node_id)
        except Exception as e:  # noqa: BLE001 - runs on RNS's announce thread; logged
            log(f"error handling the announce of {node_name(node_id)}: {e}", LOG_WARNING)

    def register(self):
        """Register with RNS Transport."""
        RNS.Transport.register_announce_handler(self)

    def deregister(self):
        """Deregister from RNS Transport."""
        RNS.Transport.deregister_announce_handler(self)


def read_mode(node_id: bytes, timeout: float, refresh: bool = True) -> int | None:
    """A node's DHT mode from the app_data RNS last heard for it, after a path request.

    With refresh, asks for a path (whose response carries the app_data) and waits up to timeout for it.
    None if RNS has never heard from the node. Blocks, so don't call it from an RNS callback.
    """
    if refresh or not RNS.Transport.has_path(node_id):
        RNS.Transport.request_path(node_id)
        deadline = time.monotonic() + timeout
        # A path response updates the stored app_data when it arrives; poll until it's there
        while time.monotonic() < deadline:
            if RNS.Transport.has_path(node_id) and RNS.Identity.recall_app_data(node_id) is not None:
                break
            time.sleep(0.1)
    app_data = RNS.Identity.recall_app_data(node_id)
    if app_data is None:
        return None
    return parse_mode(app_data)
