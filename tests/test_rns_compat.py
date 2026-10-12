"""The installed RNS and LXMF still have every internal rnfs relies on.

rnfs reads private attributes of RNS and LXMF objects, replaces some of their methods, passes undocumented
keywords and depends on data layouts neither documents, none of which their version numbers promise to keep.
INTERNALS lists each one with the rnfs module relying on it, so a dependency bump (Dependabot, a lock update, a
user's own environment) fails here, naming the module to update, rather than at runtime. Written against RNS
1.5.6 and LXMF 1.2.0.

Nothing here starts Reticulum: objects are built offline (Transport.register_destination patched out), and
the process-wide state touched (Identity.known_destinations) is restored.
"""
import ast
import functools
import inspect
import os
import re
import tempfile
import textwrap
import threading
import time
import types
import unittest
from collections.abc import Callable
from typing import NamedTuple
from unittest import mock

import LXMF
import RNS
from LXMF import LXMRouter
from RNS.vendor import umsgpack

from rnfs import mesh, names
from rnfs.constants import ID_LENGTH
from rnfs.content import seal
from rnfs.content.cid import RAW, Cid
from rnfs.features import keepers, lxmf
from rnfs.network import guards, protocol, rates


class Internal(NamedTuple):
    module: str  # the rnfs modules that rely on it, under rnfs/ and separated by ", "
    surface: str  # what of RNS or LXMF it relies on
    check: Callable[[], None]  # raises if the installed RNS or LXMF doesn't provide it


INTERNALS = [
    # Resource advertisements and the guard on them
    Internal("network/guards.py", "ResourceAdvertisement fields t d u l i q p, after a pack/unpack round trip",
             lambda: _advertisement_round_trip()),
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
    Internal("network/server.py, node.py", "Resource.AUTO_COMPRESS_MAX_SIZE, copied by every Resource at creation "
             "into max_decompressed_size and auto_compress_limit",
             lambda: (_has(RNS.Resource, "AUTO_COMPRESS_MAX_SIZE"),
                      _assigns(RNS.Resource.__init__, max_decompressed_size="Resource.AUTO_COMPRESS_MAX_SIZE",
                               auto_compress_limit="Resource.AUTO_COMPRESS_MAX_SIZE"),
                      _source(RNS.Resource.assemble, "max_length=self.max_decompressed_size"))),
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

    # Requests
    Internal("network/links.py, network/server.py", "Link.request's own timeout, rtt × traffic_timeout_factor + "
             "Resource.RESPONSE_MAX_GRACE_TIME × 1.125 (RESPONSE_GRACE, MAX_PACING_WAIT)",
             lambda: (_params(RNS.Link.request, "path", "data", "response_callback", "failed_callback",
                              "progress_callback", "timeout", "max_response_size"),
                      _source(RNS.Link.request, "timeout = self.rtt * self.traffic_timeout_factor + "
                              "RNS.Resource.RESPONSE_MAX_GRACE_TIME * 1.125"))),
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
    Internal("node_content.py", "Identity.recall(hash, from_identity_hash=, _no_use=), raising AttributeError "
             "without a Reticulum instance unless _no_use", lambda: _recall()),
    Internal("features/keepers.py", "Transport.get_cached_packet(packet_hash, packet_type=\"announce\") and the "
             "signed announce in it: public key, name hash, random hash (emission time in its last 5 bytes), "
             "ratchet, signature, app_data", lambda: _cached_announce()),
    Internal("names.py", "announce handlers with five parameters get announce_packet_hash and is_path_response",
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

    # LXMF
    Internal("features/lxmf.py", "LXMRouter(identity, storagepath, delivery_limit, autopeer), keeping its state "
             "in storagepath/lxmf and setting signal handlers through signal.signal",
             lambda: (_params(LXMRouter.__init__, "identity", "storagepath", "delivery_limit", "autopeer"),
                      _assigns(LXMRouter.__init__, storagepath="storagepath + '/lxmf'"),
                      _source(LXMRouter.__init__, "signal.signal(signal.SIGINT, self.sigint_handler)",
                              "signal.signal(signal.SIGTERM, self.sigterm_handler)"))),
    Internal("features/lxmf.py", "LXMRouter.get_announce_app_data(destination_hash), looked up on the router "
             "when announcing, and its layout [display name, stamp cost, supported functions]",
             lambda: _announce_app_data()),
    Internal("features/lxmf.py", "LXMRouter.delivery_resource_transfer_began(resource), looked up on the router "
             "when a delivery link is established; Link.set_resource_started_callback",
             lambda: (_params(LXMRouter.delivery_resource_transfer_began, "resource"),
                      _source(LXMRouter.delivery_link_established,
                              "link.set_resource_started_callback(self.delivery_resource_transfer_began)"),
                      _has(RNS.Link, "set_resource_started_callback"))),
    Internal("features/lxmf.py", "LXMRouter.request_messages_from_propagation_node(identity, max_messages), "
             "called again through the router once the propagation node link is up",
             lambda: (_params(LXMRouter.request_messages_from_propagation_node, "identity", "max_messages"),
                      _source(LXMRouter.request_messages_from_propagation_node,
                              "self.request_messages_from_propagation_node(identity, "
                              "self.propagation_transfer_max_messages)"))),
    Internal("features/lxmf.py", "LXMRouter propagation_transfer_state, outbound_propagation_link, "
             "delivery_destinations; PR_* order: PR_IDLE < PR_PATH_REQUESTED..PR_RESPONSE_RECEIVED < PR_COMPLETE "
             "< every failure",
             lambda: (_assigns(LXMRouter.__init__, "propagation_transfer_state", "outbound_propagation_link",
                               delivery_destinations="{}"),
                      _true(LXMRouter.PR_IDLE < LXMRouter.PR_PATH_REQUESTED < LXMRouter.PR_LINK_ESTABLISHING
                            < LXMRouter.PR_LINK_ESTABLISHED < LXMRouter.PR_REQUEST_SENT < LXMRouter.PR_RECEIVING
                            < LXMRouter.PR_RESPONSE_RECEIVED < LXMRouter.PR_COMPLETE
                            < min(LXMRouter.PR_NO_PATH, LXMRouter.PR_LINK_FAILED, LXMRouter.PR_TRANSFER_FAILED,
                                  LXMRouter.PR_NO_IDENTITY_RCVD, LXMRouter.PR_NO_ACCESS, LXMRouter.PR_FAILED),
                            "PR_* states reordered"))),
    Internal("features/lxmf.py", "LXMRouter cancel_propagation_node_requests(), acknowledge_sync_completion("
             "reset_state=), set_outbound_propagation_node, register_delivery_identity(identity, display_name=), "
             "register_delivery_callback, handle_outbound, announce, exit_handler",
             lambda: (_params(LXMRouter.acknowledge_sync_completion, "reset_state"),
                      _params(LXMRouter.register_delivery_identity, "identity", "display_name"),
                      _has(LXMRouter, "cancel_propagation_node_requests", "set_outbound_propagation_node",
                           "register_delivery_callback", "handle_outbound", "announce", "exit_handler"))),
    Internal("features/lxmf.py", "propagation node announce data: pn_announce_data_is_valid, enabled flag at "
             "index 2", lambda: _propagation_announce()),
    Internal("features/lxmf.py", "LXMessage(destination, source, content, title=, fields=, desired_method=); "
             "packed as destination hash, source hash, ...; unpack_from_bytes; signature_validated, "
             "unverified_reason SOURCE_UNKNOWN, packed, hash, timestamp, title and content (bytes), fields, "
             "method, state, progress", lambda: _message_round_trip()),
    Internal("features/lxmf.py", "LXMessage state and method values (STATE_NAMES, METHOD_NAMES); "
             "FIELD_CUSTOM_TYPE, FIELD_CUSTOM_DATA",
             lambda: (_true({getattr(LXMF.LXMessage, name.upper()): name for name in lxmf.STATE_NAMES.values()}
                            == lxmf.STATE_NAMES, "states renumbered"),
                      _true({getattr(LXMF.LXMessage, name.upper()): name for name in lxmf.METHOD_NAMES.values()}
                            == lxmf.METHOD_NAMES, "methods renumbered"),
                      _true((LXMF.FIELD_CUSTOM_TYPE, LXMF.FIELD_CUSTOM_DATA)
                            == (lxmf.FIELD_CUSTOM_TYPE, lxmf.FIELD_CUSTOM_DATA), "custom fields renumbered"))),
    Internal("features/lxmf.py", "Resource get_data_size(), get_transfer_size(), get_segments(), cancel() and "
             "link on the propagation node link's Resources; a foreign Link takes rnfs_bounded",
             lambda: (_has(RNS.Resource, "get_data_size", "get_transfer_size", "get_segments", "cancel"),
                      setattr(_incoming_link(), "rnfs_bounded", True))),
]


class RnsCompatTest(unittest.TestCase):
    """INTERNALS against the installed RNS and LXMF."""

    def test_internals(self):
        versions = f"RNS {RNS.__version__}, LXMF {LXMF.__version__}"
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
    """Advertisements as Resource.advertise packs them and Link.receive unpacks them, through guards.refusal."""
    for is_response in (True, False):
        resource = types.SimpleNamespace(
            size=100_052, total_size=100_000, parts=[None], hash=os.urandom(32), random_hash=os.urandom(4),
            original_hash=os.urandom(32), hashmap=os.urandom(4), compressed=False, encrypted=True, split=True,
            has_metadata=False, segment_index=2, total_segments=3, request_id=os.urandom(16), is_response=is_response)
        adv = RNS.ResourceAdvertisement.unpack(RNS.ResourceAdvertisement(resource).pack())
        fields = {name: getattr(adv, name, None) for name in "tdulipq"}
        _true(fields == {"t": 100_052, "d": 100_000, "u": not is_response, "l": 3, "i": 2, "p": is_response,
                         "q": resource.request_id}, f"unpacked as {fields}")
        _true(guards.refusal(adv) == (None if is_response else "requests are never sent as Resources"),
              f"guards.refusal: {guards.refusal(adv)}")


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


def _cached_announce():
    """An rnfs.name announce, cached as Transport caches it, read back and verified as keepers does."""
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


# LXMF round trips


def _announce_app_data():
    """rnfs's announce data reads back in LXMF as LXMF's own does, minus compression support."""
    for name in ("display_name_from_app_data", "stamp_cost_from_app_data", "compression_support_from_app_data"):
        _has(LXMF, name)
    _params(LXMRouter.get_announce_app_data, "destination_hash")
    _source(LXMRouter.announce, "app_data=self.get_announce_app_data(destination_hash)")
    _source(LXMRouter.register_delivery_identity, "self.get_announce_app_data(delivery_destination.hash)")
    destination_hash = os.urandom(16)
    router = types.SimpleNamespace(delivery_destinations={
        destination_hash: types.SimpleNamespace(display_name="rnfs node", stamp_cost=8)})
    ours = lxmf.LxmfNetwork._announce_app_data(types.SimpleNamespace(router=router, msgpack=umsgpack),
                                               destination_hash)
    theirs = LXMRouter.get_announce_app_data(router, destination_hash)
    _true(umsgpack.unpackb(ours)[:2] == umsgpack.unpackb(theirs)[:2], f"{ours!r} isn't laid out as {theirs!r}")
    _true((LXMF.display_name_from_app_data(ours), LXMF.stamp_cost_from_app_data(ours),
           LXMF.compression_support_from_app_data(ours)) == ("rnfs node", 8, False), "LXMF reads it differently")


def _propagation_announce():
    """LXMF's propagation node announce data, enabled and not, as heard_propagation_node reads it."""
    node = types.SimpleNamespace(LXMF=LXMF, msgpack=umsgpack, _lock=threading.Lock(), _propagation_nodes={})
    destination_hash = os.urandom(16)
    for enabled in (True, False):
        router = types.SimpleNamespace(
            propagation_node=enabled, from_static_only=False, propagation_stamp_cost=16,
            propagation_stamp_cost_flexibility=3, peering_cost=18, propagation_per_transfer_limit=256,
            propagation_per_sync_limit=10240, get_propagation_node_announce_metadata=lambda: {})
        app_data = LXMRouter.get_propagation_node_app_data(router)
        _true(LXMF.pn_announce_data_is_valid(app_data), "LXMF's own announce data isn't valid")
        lxmf.LxmfNetwork.heard_propagation_node(node, destination_hash, app_data)
        _true((destination_hash in node._propagation_nodes) == enabled, f"enabled {enabled} not read as such")


def _message_round_trip():
    """An LXMessage packed and unpacked, its sender unknown and then known, as LxmfNetwork reads it."""
    sender = _destination("lxmf", "delivery")
    recipient = _destination("lxmf", "delivery", direction=RNS.Destination.OUT)
    fields = {lxmf.FIELD_CUSTOM_TYPE: lxmf.CUSTOM_TYPE}
    message = LXMF.LXMessage(recipient, sender, "text", title="title", fields=fields,
                             desired_method=LXMF.LXMessage.DIRECT)
    _true((message.state, message.progress) == (LXMF.LXMessage.GENERATING, 0.0), "a new message's state")
    message.pack()
    packed = bytes(message.packed)
    _true(packed[ID_LENGTH:2 * ID_LENGTH] == sender.hash, "the source hash isn't at packed[16:32]")
    instance = types.SimpleNamespace(is_blackholed=lambda identity: False, _used_destination_data=lambda h: None)
    with mock.patch.object(RNS.Transport, "register_destination"), _Remembered() as known:
        unknown = LXMF.LXMessage.unpack_from_bytes(packed)
        _true(not unknown.signature_validated and unknown.unverified_reason == LXMF.LXMessage.SOURCE_UNKNOWN
              and bytes(unknown.packed) == packed, "a message from an unknown sender")
        known.remember(sender)
        with mock.patch.object(RNS.Reticulum, "get_instance", return_value=instance):
            received = LXMF.LXMessage.unpack_from_bytes(packed)
    _true(received.signature_validated, "a signed message from a known sender doesn't validate")
    _true((bytes(received.source_hash), bytes(received.hash), received.title, received.content, received.fields)
          == (sender.hash, bytes(message.hash), b"title", b"text", fields), "the received message's fields")
    _true(isinstance(received.timestamp, float) and hasattr(received, "method"), "timestamp and method")


if __name__ == "__main__":
    unittest.main()
