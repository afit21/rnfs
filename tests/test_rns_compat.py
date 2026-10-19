"""The installed RNS still has every internal rnfs relies on.

rnfs reads private attributes of RNS objects, replaces some of their methods, passes undocumented keywords and
depends on data layouts RNS doesn't document, none of which its version numbers promise to keep. INTERNALS lists
each one with the rnfs module relying on it, so a dependency bump (Dependabot, a lock update, a user's own
environment) fails here, naming the module to update, rather than at runtime. Written against RNS 1.5.6 (LXMF's
internals left with the LXMF feature, in 0.2.0).

Nothing here starts Reticulum: objects are built offline (Transport.register_destination patched out), and
the process-wide state touched (Identity.known_destinations) is restored.
"""
import ast
import functools
import inspect
import os
import re
import sys
import tempfile
import textwrap
import time
import types
import unittest
from collections.abc import Callable
from typing import NamedTuple
from unittest import mock

import RNS
from RNS.vendor import umsgpack

from rnfs import mesh, names
from rnfs.constants import ID_LENGTH
from rnfs.content import seal
from rnfs.content.cid import RAW, Cid
from rnfs.features import keepers
from rnfs.network import guards, protocol, rates


class Internal(NamedTuple):
    module: str  # the rnfs modules that rely on it, under rnfs/ and separated by ", "
    surface: str  # what of RNS it relies on
    check: Callable[[], None]  # raises if the installed RNS doesn't provide it


INTERNALS = [
    # Resource advertisements and the guard on them
    Internal("network/guards.py", "ResourceAdvertisement fields t d u l i q p x c, after a pack/unpack round "
             "trip", lambda: _advertisement_round_trip()),
    Internal("network/guards.py", "Resource.accept(advertisement_packet, callback, progress_callback, request_id) "
             "and Resource.reject(advertisement_packet), called by Link as RNS.Resource.accept/reject",
             lambda: (_params(_original_accept(), "advertisement_packet", "callback", "progress_callback",
                              "request_id"),
                      _params(RNS.Resource.reject, "advertisement_packet"),
                      _source(RNS.Link, "packet.plaintext = self.decrypt(packet.data)",
                              "RNS.Resource.accept(packet, callback=self.response_resource_concluded, "
                              "progress_callback=pending_request.response_resource_progress, request_id=request_id)",
                              "RNS.Resource.reject(packet)"))),
    Internal("network/guards.py", "Resource data overhead (RANDOM_HASH_SIZE, Identity.TOKEN_OVERHEAD, one AES "
             "block) within TRANSFER_OVERHEAD",
             lambda: _true(RNS.Resource.RANDOM_HASH_SIZE + RNS.Identity.TOKEN_OVERHEAD + 16
                           <= guards.TRANSFER_OVERHEAD, "overhead grew")),
    Internal("network/guards.py", "Link.pending_requests of RequestReceipts with request_id and response_rejected()",
             lambda: (_true(_incoming_link().pending_requests == [], "no pending_requests list"),
                      _assigns(RNS.RequestReceipt.__init__, "request_id"),
                      _has(RNS.RequestReceipt, "response_rejected"))),
    Internal("network/guards.py", "Link.destination (outgoing), Link.owner (incoming) and Destination.name as "
             "expand_name builds it", lambda: _dht_link_names()),

    # Responses: the server follows the Resources RNS sends them in
    Internal("network/guards.py", "Resource.AUTO_COMPRESS_MAX_SIZE, copied by every Resource at creation into "
             "max_decompressed_size and auto_compress_limit; Resource.accept returning the Resource, whose "
             "max_decompressed_size assemble() decompresses at most",
             lambda: (_has(RNS.Resource, "AUTO_COMPRESS_MAX_SIZE"),
                      _assigns(RNS.Resource.__init__, max_decompressed_size="Resource.AUTO_COMPRESS_MAX_SIZE",
                               auto_compress_limit="Resource.AUTO_COMPRESS_MAX_SIZE"),
                      _source(_original_accept(), "return resource"),
                      _source(RNS.Resource.assemble, "max_length=self.max_decompressed_size"))),
    Internal("network/server.py", "Destination.register_request_handler(path, response_generator, allow, "
             "auto_compress), and Link.handle_request passing the handler's auto_compress to both response "
             "Resources",
             lambda: (_params(RNS.Destination.register_request_handler, "path", "response_generator", "allow",
                              "auto_compress"),
                      _source(RNS.Link.handle_request, "auto_compress = request_handler[4]",
                              "RNS.Resource(file_handle, self, metadata=metadata, request_id=request_id, "
                              "is_response=True, auto_compress=auto_compress)",
                              "RNS.Resource(packed_response, self, request_id=request_id, is_response=True, "
                              "auto_compress=auto_compress)"))),
    Internal("network/server.py", "Link.register_outgoing_resource(resource), looked up on the link (and so "
             "replaceable per link) when a Resource is advertised",
             lambda: (_params(RNS.Link.register_outgoing_resource, "resource"),
                      _source(RNS.Resource, "self.link.register_outgoing_resource(self)"),
                      setattr(_incoming_link(), "register_outgoing_resource", None))),
    Internal("network/server.py, network/links.py", "Link.outgoing_resources and Link.incoming_resources lists",
             lambda: _true(_incoming_link().outgoing_resources == [] == _incoming_link().incoming_resources,
                           "no resource lists")),
    Internal("network/server.py, network/links.py", "Resource request_id, status, callback (called as it "
             "concludes) and link; get_progress() and cancel()",
             lambda: (_assigns(RNS.Resource.__init__, "request_id", "status", "callback", "link"),
                      _source(RNS.Resource, "self.callback(self)"),
                      _has(RNS.Resource, "get_progress", "cancel"))),
    Internal("network/server.py", "Resource status order: NONE..TRANSFERRING < AWAITING_PROOF < ASSEMBLING < "
             "COMPLETE, and every other conclusion above COMPLETE",
             lambda: _true(max(RNS.Resource.NONE, RNS.Resource.QUEUED, RNS.Resource.ADVERTISED,
                               RNS.Resource.TRANSFERRING) < RNS.Resource.AWAITING_PROOF < RNS.Resource.ASSEMBLING
                           < RNS.Resource.COMPLETE < min(RNS.Resource.FAILED, RNS.Resource.CORRUPT,
                                                         RNS.Resource.REJECTED), "statuses reordered")),
    Internal("network/server.py", "request handlers with six parameters get (path, data, request_id, link_id, "
             "remote_identity, requested_at)",
             lambda: _source(RNS.Link.handle_request,
                             "len(inspect.signature(response_generator).parameters) == 6",
                             "response_generator(path, request_data, request_id, self.link_id, "
                             "self.__remote_identity, requested_at)")),
    Internal("network/server.py", "Destination.links: the destination's incoming links",
             lambda: _true(_destination("rnfs", "dht").links == [], "no links list")),
    Internal("network/server.py, network/links.py", "Link status, rtt, traffic_timeout_factor, attached_interface "
             "and link_id (set_link_id); Link.ACTIVE, CLOSED, TRAFFIC_TIMEOUT_FACTOR",
             lambda: (_assigns(RNS.Link.__init__, "status", "rtt", "traffic_timeout_factor", "attached_interface"),
                      _has(RNS.Link, "ACTIVE", "CLOSED", "TRAFFIC_TIMEOUT_FACTOR", "set_link_id"))),
    Internal("network/links.py", "Link.rtt measured on a link this node opens, from its request to the proof, "
             "before the established callback runs (LinkPool.on_rtt reads it once established, for `rnfs peers`)",
             lambda: _source(RNS.Link.validate_proof, "self.rtt = time.time() - self.request_time",
                             "self.callbacks.link_established")),

    # Requests
    Internal("network/links.py, network/server.py", "Link.request's own timeout, rtt × traffic_timeout_factor + "
             "Resource.RESPONSE_MAX_GRACE_TIME × 1.125 (RESPONSE_GRACE, MAX_PACING_WAIT)",
             lambda: (_params(RNS.Link.request, "path", "data", "response_callback", "failed_callback",
                              "progress_callback", "timeout", "max_response_size"),
                      _source(RNS.Link.request, "timeout = self.rtt * self.traffic_timeout_factor + "
                              "RNS.Resource.RESPONSE_MAX_GRACE_TIME * 1.125"))),
    Internal("network/links.py", "Link.request sending a request as one packet if it packs to at most Link.mdu "
             "bytes (else as a Resource), and Link.get_mdu() returning mdu on an active link",
             lambda: (_source(RNS.Link.request, "if len(packed_request) <= self.mdu:",
                              "RNS.Packet(self, packed_request, RNS.Packet.DATA, context=RNS.Packet.REQUEST)"),
                      _source(RNS.Link.get_mdu, "if self.status == Link.ACTIVE: return self.mdu"))),
    Internal("network/links.py", "RequestReceipt link, response_size, get_response() and get_progress()",
             lambda: (_assigns(RNS.RequestReceipt.__init__, "link", "response_size"),
                      _has(RNS.RequestReceipt, "get_response", "get_progress"))),
    Internal("network/protocol.py", "request framing [time.time(), truncated path hash, data] (umsgpack packs a "
             "float in 9 bytes), response framing [request_id, response] with truncated-hash request IDs",
             lambda: (_source(RNS.Link.request, "unpacked_request = [time.time(), request_path_hash, data]",
                              "request_path_hash = RNS.Identity.truncated_hash(path.encode('utf-8'))"),
                      _source(RNS.Link.handle_request, "packed_response = umsgpack.packb([request_id, response])"),
                      _true(len(umsgpack.packb(0.0)) == 9, "a float isn't packed in 9 bytes"),
                      _true(protocol.REQUEST_ID_LENGTH == RNS.Identity.TRUNCATED_HASHLENGTH // 8 == ID_LENGTH,
                            "hash length changed"))),

    # Identities and announces RNS remembers
    Internal("node.py, names.py, features/peers.py", "Identity.known_destinations entries: [time heard, packet "
             "hash, public key, app_data, ...]", lambda: _known_destinations()),
    Internal("node.py", "announce handlers called only for an announce that takes the path (should_add), and only "
             "after its path table entry is written, so the path's newest emission is that announce's",
             lambda: _handlers_after_path()),
    Internal("names.py, node.py", "Transport.path_table entries holding, at IDX_PT_RANDBLOBS, the random blobs of "
             "the announces the path was taken from, a blob added only for an announce not heard before; "
             "Transport.timebase_from_random_blobs, their newest emission time",
             lambda: (_true(sys.modules["RNS.Transport"].IDX_PT_RANDBLOBS == names._PATH_RANDOM_BLOBS,
                            "IDX_PT_RANDBLOBS moved"),
                      _true(isinstance(RNS.Transport.path_table, dict), "path_table isn't a dict"),
                      _true(RNS.Transport.timebase_from_random_blobs([os.urandom(5) + (1234).to_bytes(5, "big"),
                                                                     os.urandom(5) + (99).to_bytes(5, "big")])
                            == 1234, "timebase_from_random_blobs isn't the newest emission"),
                      _source(RNS.Transport, "if not random_blob in random_blobs:",
                              "random_blobs.append(random_blob)",
                              "path_table_entry = [now, received_from, announce_hops, expires, random_blobs, "
                              "packet.receiving_interface, packet.packet_hash]"))),
    Internal("names.py", "Transport.path_table entries holding, at IDX_PT_PACKET, the packet hash of the announce "
             "the path was taken from; Identity.remember storing each announce's packet hash at entry[1], a "
             "known destination's too; the announce cache written, in Transport._inbound, only for an announce "
             "that takes the path (should_add)",
             lambda: (_true(sys.modules["RNS.Transport"].IDX_PT_PACKET == names._PATH_PACKET, "IDX_PT_PACKET moved"),
                      _source(RNS.Identity.remember, "entry[1] = packet_hash"),
                      _announce_cache_on_path())),
    Internal("node_content.py, node.py, features/peers.py, network/links.py",
             "Identity.recall(hash, from_identity_hash=, _no_use=), raising AttributeError without a Reticulum "
             "instance unless _no_use (read-only checks pass _no_use=True so they don't mark the destination used)",
             lambda: _recall()),
    Internal("features/peers.py", "Identity.recall_app_data(hash, _no_use=)",
             lambda: _params(RNS.Identity.recall_app_data, "_no_use")),
    Internal("features/keepers.py, names.py", "Transport.get_cached_packet(packet_hash, packet_type=\"announce\") and the "
             "signed announce in it: public key, name hash, random hash (emission time in its last 5 bytes), "
             "ratchet, signature, app_data", lambda: _cached_announce()),
    Internal("names.py, network/announces.py", "announce handlers with five parameters get announce_packet_hash and "
             "is_path_response",
             lambda: _source(RNS.Transport, "len(inspect.signature(handler.received_announce).parameters) == 5",
                             "announce_packet_hash=packet.packet_hash",
                             "is_path_response=packet.context == RNS.Packet.PATH_RESPONSE")),
    Internal("names.py", "an identity's private key is Identity.KEYSIZE // 8 (KEY_LENGTH) bytes for from_bytes",
             lambda: _true(len(RNS.Identity().get_private_key()) == names.KEY_LENGTH, "key size changed")),

    # Paths and interfaces
    Internal("mesh.py", "Transport.PATHFINDER_M (what hops_to returns for an unknown path); next_hop_interface, "
             "Reticulum.get_next_hop_if_name, is_connected_to_shared_instance",
             lambda: (_true(RNS.Transport.PATHFINDER_M == mesh.PATHFINDER_M, "PATHFINDER_M changed"),
                      _has(RNS.Transport, "hops_to", "next_hop_interface"),
                      _has(RNS.Reticulum, "get_next_hop_if_name", "get_instance", "configdir"),
                      _assigns(RNS.Reticulum.__init__, "is_connected_to_shared_instance"))),
    Internal("mesh.py", "Reticulum.get_path_table() entries {hash, interface}, interface being the str() of the "
             "interface that get_next_hop_if_name gives (RnsPaths.infos)",
             lambda: (_source(RNS.Reticulum.get_path_table, "'hash': dst_hash",
                              "'interface': str(RNS.Transport.path_table[dst_hash][5])"),
                      _source(RNS.Reticulum.get_next_hop_if_name,
                              "return str(RNS.Transport.next_hop_interface(destination))"))),
    Internal("mesh.py", "Transport.path_table entries holding the hop count at IDX_PT_HOPS and the interface at "
             "IDX_PT_RVCD_IF; get_path_table(max_hops) entries {hash, hops, interface}, within max_hops (RnsPaths.scan)",
             lambda: (_true(sys.modules["RNS.Transport"].IDX_PT_HOPS == mesh.PATH_TABLE_HOPS, "IDX_PT_HOPS moved"),
                      _true(sys.modules["RNS.Transport"].IDX_PT_RVCD_IF == mesh.PATH_TABLE_INTERFACE,
                            "IDX_PT_RVCD_IF moved"),
                      _params(RNS.Reticulum.get_path_table, "max_hops"),
                      _source(RNS.Reticulum.get_path_table, "'hops': path_hops",
                              "if max_hops == None or path_hops <= max_hops:",
                              "rpc_connection.send_bytes(mp.packb({'get': 'path_table', 'max_hops': max_hops}))"))),
    Internal("mesh.py, network/rates.py", "Reticulum.get_interface_stats() {\"interfaces\": [{name, short_name, "
             "type, status, bitrate}]}, with LocalServerInterface and LocalClientInterface types",
             lambda: (_source(RNS.Reticulum.get_interface_stats, "ifstats['name'] = str(interface)",
                              "ifstats['short_name'] = str(interface.name)",
                              "ifstats['type'] = str(type(interface).__name__)",
                              "ifstats['status'] = interface.online", "ifstats['bitrate'] = interface.bitrate",
                              "stats['interfaces'] = interfaces"),
                      _has(RNS.Interfaces.LocalInterface, *rates.LOCAL_INTERFACE_TYPES))),

    # Sealed files
    Internal("content/seal.py", "RNS.Cryptography.hkdf(length, derive_from, salt, context), "
             "AES.AES_256_CBC.encrypt/decrypt(data, key, iv), and Identity.encrypt of a content key being "
             "WRAPPED_KEY_SIZE bytes", lambda: _crypto()),

]


class RnsCompatTest(unittest.TestCase):
    """INTERNALS against the installed RNS."""

    def test_internals(self):
        versions = f"RNS {RNS.__version__}"
        for internal in INTERNALS:
            with self.subTest(internal.surface):
                try:
                    internal.check()
                except Exception as e:
                    modules = " and ".join(f"rnfs/{module}" for module in internal.module.split(", "))
                    self.fail(f"{versions} doesn't provide what {modules} relies on (update it, or hold the "
                              f"dependency back): {internal.surface}\n  {type(e).__name__}: {e}")

    def test_inventory_names_real_modules(self):
        root = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "rnfs")
        for internal in INTERNALS:
            for module in internal.module.split(", "):
                self.assertTrue(os.path.isfile(os.path.join(root, module)), module)


# Checks


def _true(condition, message: str):
    """Raise AssertionError(message) unless condition."""
    if not condition:
        raise AssertionError(message)


def _has(owner, *names: str):
    """Every name is an attribute of owner."""
    missing = [name for name in names if not hasattr(owner, name)]
    _true(not missing, f"{owner.__name__} has no {', '.join(missing)}")


def _params(function, *names: str):
    """function takes parameters with every name."""
    parameters = inspect.signature(function).parameters
    missing = [name for name in names if name not in parameters]
    _true(not missing, f"{function.__qualname__} takes no {', '.join(missing)}")


def _normal(code: str) -> str:
    """code without whitespace and with one kind of quotes, so source can be matched however it's laid out."""
    return re.sub(r"\s+", "", code).replace('"', "'")


@functools.lru_cache(maxsize=None)
def _tree(owner) -> ast.AST:
    """The parsed source of a function, class or module."""
    return ast.parse(textwrap.dedent(inspect.getsource(owner)))


def _source(owner, *snippets: str):
    """owner's source (a function, class or module) contains every snippet, ignoring layout."""
    code = _normal(ast.unparse(_tree(owner)))
    missing = [snippet for snippet in snippets if _normal(snippet) not in code]
    _true(not missing, f"{getattr(owner, '__qualname__', owner.__name__)} no longer does {missing}")


def _assigns(function, *names: str, **values: str):
    """function assigns self.<name> for every name, and self.<key> = value for every value (as source)."""
    assigned = {}
    for node in ast.walk(_tree(function)):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name) \
                        and target.value.id == "self":
                    assigned.setdefault(target.attr, []).append(_normal(ast.unparse(node.value)))
    missing = [name for name in (*names, *values) if name not in assigned]
    _true(not missing, f"{function.__qualname__} sets no self.{', self.'.join(missing)}")
    changed = [f"{key} = {value}" for key, value in values.items() if _normal(value) not in assigned[key]]
    _true(not changed, f"{function.__qualname__} no longer sets {changed}")


# Offline RNS objects


def _destination(app: str, *aspects: str, identity=None, direction=RNS.Destination.IN):
    """A Destination that isn't registered with Transport."""
    with mock.patch.object(RNS.Transport, "register_destination"):
        return RNS.Destination(identity or RNS.Identity(), direction, RNS.Destination.SINGLE, app, *aspects)


def _incoming_link():
    """An incoming Link to an rnfs.dht destination, as RNS makes one for a link request (not established)."""
    return RNS.Link(owner=_destination("rnfs", "dht"))


def _original_accept():
    """RNS's Resource.accept, even with the guard installed."""
    accept = RNS.Resource.__dict__["accept"].__func__
    return guards._original_accept if accept is guards.guarded_accept else accept


class _Remembered:
    """Destination hashes added to Identity.known_destinations, removed on exit."""

    def __init__(self):
        self.hashes = []

    def __enter__(self):
        return self

    def remember(self, destination, app_data: bytes | None = None) -> bytes:
        """Remember destination's announce as RNS does for a valid one; its packet hash."""
        packet_hash = os.urandom(32)
        _true(destination.hash not in RNS.Identity.known_destinations, "already known")
        self.hashes.append(destination.hash)
        RNS.Identity.remember(packet_hash, destination.hash, destination.identity.get_public_key(), app_data)
        return packet_hash

    def __exit__(self, *exc):
        for destination_hash in self.hashes:
            RNS.Identity.known_destinations.pop(destination_hash, None)


# Round trips


def _advertisement_round_trip():
    """Advertisements as Resource.advertise packs them and Link.receive unpacks them, through guards.refusal:
    in the daemon's scope with no link, and on an rnfs.dht link for the compressed and metadata flags."""
    dht = types.SimpleNamespace(destination=_destination("rnfs", "dht", direction=RNS.Destination.OUT))
    cases = (({"is_response": True}, None, None),
             ({"is_response": False}, None, "requests are never sent as Resources"),
             ({"compressed": True}, dht, "rnfs Resources are never compressed"),
             ({"has_metadata": True}, dht, "rnfs responses carry no metadata"))
    for overrides, link, refused in cases:
        resource = types.SimpleNamespace(
            size=100_052, total_size=100_000, parts=[None], hash=os.urandom(32), random_hash=os.urandom(4),
            original_hash=os.urandom(32), hashmap=os.urandom(4), compressed=False, encrypted=True, split=True,
            has_metadata=False, segment_index=2, total_segments=3, request_id=os.urandom(16), is_response=True)
        if link is not None:  # one segment, so only the flag under test is refused
            resource.split, resource.segment_index, resource.total_segments = False, 1, 1
        vars(resource).update(overrides)
        adv = RNS.ResourceAdvertisement.unpack(RNS.ResourceAdvertisement(resource).pack())
        fields = {name: getattr(adv, name, None) for name in "tdulipqxc"}
        expected = {"t": 100_052, "d": 100_000, "u": not resource.is_response, "l": resource.total_segments,
                    "i": resource.segment_index, "p": resource.is_response, "q": resource.request_id,
                    "x": resource.has_metadata, "c": resource.compressed}
        _true(fields == expected, f"unpacked as {fields}")
        with mock.patch.object(guards, "_scope", guards.PROCESS):  # whatever scope another test left
            reason = guards.refusal(adv, link)
        _true(reason == refused, f"guards.refusal: {reason}")


def _dht_link_names():
    """guards.is_dht_link on real Destinations and an incoming Link."""
    identity = RNS.Identity()
    dht = _destination("rnfs", "dht", identity=identity, direction=RNS.Destination.OUT)
    _true(dht.name == RNS.Destination.expand_name(identity, "rnfs", "dht"), f"Destination.name is {dht.name}")
    _true(guards.is_dht_link(types.SimpleNamespace(destination=dht)), "outgoing rnfs.dht link not recognised")
    _true(guards.is_dht_link(_incoming_link()), "incoming rnfs.dht link not recognised")
    delivery = _destination("lxmf", "delivery", identity=identity, direction=RNS.Destination.OUT)
    _true(not guards.is_dht_link(types.SimpleNamespace(destination=delivery)), "lxmf.delivery taken for rnfs.dht")
    _true(not guards.is_dht_link(RNS.Link(owner=_destination("lxmf", "delivery"))), "lxmf.delivery taken for rnfs.dht")


def _known_destinations():
    """An announce RNS remembers, read back as node._seed_routing, names.heard_time and peers do."""
    destination = _destination("rnfs", "dht")
    with _Remembered() as known:
        before = time.time()
        packet_hash = known.remember(destination, b"app data")
        entry = RNS.Identity.known_destinations.get(destination.hash)
        _true(before <= entry[0] <= time.time(), f"entry[0] is {entry[0]!r}, not when it was heard")
        _true(entry[1:4] == [packet_hash, destination.identity.get_public_key(), b"app data"],
              f"entry is {entry!r}")
        identity = RNS.Identity(create_keys=False)
        identity.load_public_key(entry[2])
        _true(identity.pub is not None and RNS.Destination.hash(identity, "rnfs", "dht") == destination.hash,
              "the public key doesn't make the destination hash")


def _announce_cache_on_path():
    """Transport._inbound caches announces (packet_type "announce") only under `if should_add:`."""
    tree = _tree(RNS.Transport._inbound)
    guarded = {id(node) for branch in ast.walk(tree)
               if isinstance(branch, ast.If) and ast.unparse(branch.test) == "should_add"
               for statement in branch.body for node in ast.walk(statement)}
    caches = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
              and ast.unparse(node.func) == "Transport.cache"
              and any(keyword.arg == "packet_type" and ast.unparse(keyword.value) == "'announce'"
                      for keyword in node.keywords)]
    _true(caches, "Transport._inbound no longer caches announces")
    _true(all(id(node) in guarded for node in caches), "Transport._inbound caches an announce outside should_add")


def _recall():
    """Identity.recall with _no_use, as node_content does without a Reticulum instance."""
    destination = _destination("rnfs", "dht")
    public_key = destination.identity.get_public_key()
    with _Remembered() as known:
        known.remember(destination)
        for target, kwargs in ((destination.hash, {}), (destination.identity.hash, {"from_identity_hash": True})):
            found = RNS.Identity.recall(target, _no_use=True, **kwargs)
            _true(found is not None and found.get_public_key() == public_key, f"recall({kwargs}) found {found}")
            if RNS.Reticulum.get_instance() is None:
                try:
                    RNS.Identity.recall(target, **kwargs)
                except AttributeError:
                    pass
                else:
                    raise AssertionError("recall without _no_use and without Reticulum didn't raise AttributeError")


def _handlers_after_path():
    """Transport._inbound writes an announce's path table entry, inside `if should_add:`, before it calls the
    announce handlers, which are inside that block too."""
    written, handlers = (_normal("Transport.path_table[packet.destination_hash] = path_table_entry"),
                         _normal("for handler in Transport.announce_handlers:"))
    for node in ast.walk(_tree(RNS.Transport._inbound)):
        if isinstance(node, ast.If) and ast.unparse(node.test) == "should_add":
            code = _normal(ast.unparse(node))
            if -1 < code.find(written) < code.find(handlers):
                return
    raise AssertionError("announce handlers no longer run inside `if should_add:`, after the path is written")


def _cached_announce():
    """An rnfs.name announce, cached as Transport caches it, read back as names reads it and verified as keepers
    does."""
    _true((keepers.PUBLIC_KEY_LENGTH, keepers.NAME_HASH_LENGTH, keepers.RATCHET_LENGTH, keepers.SIGNATURE_LENGTH)
          == (RNS.Identity.KEYSIZE // 8, RNS.Identity.NAME_HASH_LENGTH // 8, RNS.Identity.RATCHETSIZE // 8,
              RNS.Identity.SIGLENGTH // 8), "announce field sizes changed")
    destination = _destination("rnfs", "name")
    _true(destination.name_hash == keepers.NAME_HASH, "name hash changed")
    cid = Cid.for_block(RAW, b"kept")
    app_data = names.encode_record(7, cid)
    packet = destination.announce(app_data=app_data, send=False)
    packet.pack()
    with tempfile.TemporaryDirectory() as cache, mock.patch.object(RNS.Reticulum, "cachepath", cache):
        os.mkdir(os.path.join(cache, "announces"))
        RNS.Transport.cache(packet, force_cache=True, packet_type="announce")
        cached = RNS.Transport.get_cached_packet(packet.get_hash(), packet_type="announce")
        # As RnsNetwork.recalled_emission reads it
        emitted, app_data_read = names.RnsNetwork._cached_announce(destination.hash, packet.get_hash())
    _true(app_data_read == app_data and abs(emitted - time.time()) < 60, f"names read {emitted}, {app_data_read}")
    # As KeeperFeature._from_rns_cache reads it
    _true(cached is not None and cached.unpack() is not False, "the cached packet doesn't unpack")
    _true(cached.destination_hash == destination.hash and cached.packet_type == RNS.Packet.ANNOUNCE,
          "the cached packet isn't the announce")
    record = keepers.from_announce_data(destination.hash, bytes(cached.data),
                                        cached.context_flag == RNS.Packet.FLAG_SET)
    _true(record is not None and (record.sequence, record.cid) == (7, cid), f"keepers read {record}")
    _true(abs(record.emitted - time.time()) < 60, f"emission time {record.emitted}")


def _crypto():
    """seal's primitives: HKDF, AES-256-CBC, and Identity.encrypt wrapping a content key."""
    from RNS.Cryptography import hkdf
    from RNS.Cryptography.AES import AES_256_CBC
    _true(len(hkdf(length=64, derive_from=bytes(32), salt=None, context=b"info")) == 64, "hkdf length")
    key, iv, data = os.urandom(32), os.urandom(16), os.urandom(32)
    _true(AES_256_CBC.decrypt(AES_256_CBC.encrypt(data, key, iv), key, iv) == data, "AES round trip")
    identity = RNS.Identity()
    wrapped = RNS.Identity(create_keys=False)
    wrapped.load_public_key(identity.get_public_key())
    token = wrapped.encrypt(key)
    _true(len(token) == seal.WRAPPED_KEY_SIZE, f"a wrapped key is {len(token)} bytes")
    _true(identity.decrypt(token) == key, "Identity.decrypt round trip")


if __name__ == "__main__":
    unittest.main()
