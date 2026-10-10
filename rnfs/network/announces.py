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
    except Exception:
        return MODE_CLIENT
    # Booleans are a separate msgpack type, and trailing bytes make it something other than msgpack 1
    if type(value) is int and value == MODE_SERVER and not stream.read(1):
        return MODE_SERVER
    return MODE_CLIENT


class DhtAnnouncer:
    """Announces rnfs.dht at startup, after startup_announce_delay, then every announce_interval."""

    def __init__(self, destination: RNS.Destination, config: Config, server: bool | None = None):
        """Set destination's app_data to the mode (config.dht_mode unless server is given); start() announces."""
        self.destination = destination
        self.config = config
        self.server = (config.dht_mode == "server") if server is None else server
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.thread: threading.Thread | None = None
        destination.set_default_app_data(encode_mode(self.server))

    def announce(self):
        """Announce now, with the current mode as app_data."""
        try:
            self.destination.announce()
            log(f"announced as a DHT {'server' if self.server else 'client'}", LOG_VERBOSE)
        except Exception as e:
            log(f"could not announce: {e}", LOG_WARNING)

    def set_server(self, server: bool):
        """Change the DHT mode, re-announcing if it changed."""
        with self.lock:
            if server == self.server:
                return
            self.server = server
            self.destination.set_default_app_data(encode_mode(server))
        self.announce()

    def start(self):
        """Start the announce schedule on a background thread."""
        if self.thread is None:
            self.thread = threading.Thread(target=self._run, name="rnfs-announcer", daemon=True)
            self.thread.start()

    def stop(self):
        """Stop announcing."""
        self.stop_event.set()

    def _run(self):
        """Announce now, again after startup_announce_delay (interfaces may not have been up), then periodically."""
        self.announce()
        if self.stop_event.wait(self.config.startup_announce_delay):
            return
        self.announce()
        while not self.stop_event.wait(self.config.announce_interval):
            self.announce()


class DhtAnnounceHandler:
    """RNS announce handler for rnfs.dht, including path responses, reporting each node's DHT mode."""

    def __init__(self, on_server: Callable[[bytes], None], on_client: Callable[[bytes], None],
                 is_banned: Callable[[bytes], bool] = lambda node_id: False, own_id: bytes | None = None):
        """Handler calling on_server(node_id) or on_client(node_id) for each announce not from a banned node."""
        # RNS only calls us for announces whose destination hash matches this full name
        self.aspect_filter = f"{APP_NAME}.{DHT_ASPECT}"
        self.receive_path_responses = True
        self.on_server = on_server
        self.on_client = on_client
        self.is_banned = is_banned
        self.own_id = own_id

    def received_announce(self, destination_hash, announced_identity, app_data):
        """RNS calls this (on a thread of its own) for each rnfs.dht announce or path response."""
        node_id = bytes(destination_hash)
        if node_id == self.own_id or self.is_banned(node_id):
            return
        try:
            if parse_mode(app_data) == MODE_SERVER:
                self.on_server(node_id)
            else:
                self.on_client(node_id)
        except Exception as e:
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
