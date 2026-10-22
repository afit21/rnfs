"""The wire known-answer cases (specification sections 3, 4, 7, 9 and 13), shared by generate.py, which runs them on
the v0.1.3 and v0.1.4 trees, and tests/test_wire.py, which runs them on the current code.

Each case is (name, kind, inputs). encode() turns a kind's inputs into what goes on the wire, with that tree's own
encoders, and parse() turns it back with that tree's own parsers; parse(encode(inputs)) must give inputs again.
For a request or response, the vector is umsgpack.packb of the request or response data only (RNS owns the
envelope); an announce's app_data (the DHT mode, a name record) is the bytes themselves.

Inputs are fixed: node IDs, keys, CIDs (raw and DAG) and a name key. A name record's signature is Ed25519, which
is deterministic, so every run signs the same bytes. Imports nothing from rnfs itself: the caller passes the
modules of the tree under test (modules()).
"""
import hashlib
import types

# Kinds whose wire form is bytes (announce app_data), not msgpack-packed data
RAW_KINDS = ("announce_mode", "name_record")

KEY = bytes.fromhex("00112233445566778899aabbccddeeff")
OTHER_KEY = bytes.fromhex("ffeeddccbbaa99887766554433221100")
NODES = [bytes([0x10 + i]) * 16 for i in range(10)]  # K = 10
PROVIDERS = [bytes([0x40 + i]) * 16 for i in range(10)]
RAW_BLOCK = b"a raw block for the wire vectors\n"
DAG_BLOCK = b"\x92\x02\x90"  # msgpack [2, []]: the empty directory
# The name key: 32 bytes of X25519 private key, then a 32-byte Ed25519 seed. A throwaway key for these vectors only
NAME_KEY = bytes(range(64))
EMITTED = 1792054800  # the emission time in the record's random hash
RANDOM_HASH = bytes.fromhex("a1a2a3a4a5") + EMITTED.to_bytes(5, "big")
NAME_HASH = hashlib.sha256(b"rnfs.name").digest()[:10]


def modules(rnfs_root: str | None = None) -> types.SimpleNamespace:
    """The modules of the rnfs tree on sys.path that the cases use (rnfs_root: check they come from there)."""
    import RNS
    from RNS.vendor import umsgpack

    import rnfs
    from rnfs import constants, names
    from rnfs.content import cid
    from rnfs.features import keepers
    from rnfs.network import announces, protocol
    try:
        from rnfs import uris as lxmf  # where the LXMF field is defined since 0.2.0
    except ImportError:
        from rnfs.features import lxmf  # the trees generate.py runs on: 0.1.3 and 0.1.4
    if rnfs_root is not None:
        assert rnfs.__file__.startswith(rnfs_root), rnfs.__file__
    return types.SimpleNamespace(RNS=RNS, umsgpack=umsgpack, rnfs=rnfs, constants=constants, names=names, cid=cid,
                                 keepers=keepers, lxmf=lxmf, announces=announces, protocol=protocol)


def cases(m) -> list[tuple[str, str, dict]]:
    """Every case, with typed inputs (bytes, the tree's Cid)."""
    Cid = m.cid.Cid
    raw, dag = Cid.for_block(m.cid.RAW, RAW_BLOCK), Cid.for_block(m.cid.DAG, DAG_BLOCK)
    identity = m.RNS.Identity.from_bytes(NAME_KEY)
    name = m.RNS.Destination.hash(identity, "rnfs", "name")
    public_key = identity.get_public_key()

    def signed(sequence: int, target) -> dict:
        """A signed name record's fields, as a /name response carries them (no ratchet, as rnfs announces)."""
        app_data = m.names.encode_record(sequence, target)
        signature = identity.sign(name + public_key + NAME_HASH + RANDOM_HASH + app_data)
        return {"public_key": public_key, "random_hash": RANDOM_HASH, "signature": signature, "app_data": app_data,
                "sequence": sequence, "cid": target}

    def link(**fields) -> dict:
        """An LXMF link's fields, every one given."""
        return {"cid": None, "name": None, "path": [], "size": None, "from": None, **fields}

    return [
        # /find
        ("find request", "find_request", {"key": KEY}),
        ("find response, empty", "find_response", {"providers": [], "nodes": []}),
        ("find response, K nodes", "find_response", {"providers": [], "nodes": NODES}),
        ("find response, K providers and K nodes", "find_response", {"providers": PROVIDERS, "nodes": NODES}),
        # /provide
        ("provide request", "provide_request", {"keys": [KEY, OTHER_KEY], "withdrawn": []}),
        ("provide request, with withdrawn", "provide_request", {"keys": [KEY], "withdrawn": [OTHER_KEY]}),
        ("provide request, only withdrawn", "provide_request", {"keys": [], "withdrawn": [KEY, OTHER_KEY]}),
        ("provide or pin reply, accepted", "accept_response", {"accepted": True}),
        ("provide or pin reply, refused", "accept_response", {"accepted": False}),
        # /blocks
        ("blocks request, raw and DAG", "blocks_request", {"max_bytes": 262144, "cids": [raw, dag]}),
        ("blocks request, first block only", "blocks_request", {"max_bytes": 0, "cids": [dag]}),
        ("blocks request, 4 MiB budget", "blocks_request", {"max_bytes": 4 * 1024 * 1024, "cids": [raw]}),
        ("blocks response, every block", "blocks_response", {"requested": 2, "entries": [RAW_BLOCK, DAG_BLOCK]}),
        ("blocks response, not held entries", "blocks_response",
         {"requested": 3, "entries": [None, RAW_BLOCK, None]}),
        ("blocks response, cut by the budget", "blocks_response", {"requested": 3, "entries": [DAG_BLOCK]}),
        # /pin
        ("pin request, pin", "pin_request", {"cid": raw, "pin": True}),
        ("pin request, release", "pin_request", {"cid": dag, "pin": False}),
        # keepers' /name
        ("name request", "name_request", {"name": name}),
        ("name response, a record", "name_response", {"name": name, "record": signed(7, dag)}),
        ("name response, none kept", "name_response", {"name": name, "record": None}),
        # rnfs.dht announce app_data
        ("announce mode, server", "announce_mode", {"server": True}),
        ("announce mode, client", "announce_mode", {"server": False}),
        # rnfs.name announce app_data
        ("name record, sequence 0", "name_record", {"sequence": 0, "cid": raw}),
        ("name record, sequence 1", "name_record", {"sequence": 1, "cid": dag}),
        ("name record, sequence 2^64 - 1", "name_record", {"sequence": 2 ** 64 - 1, "cid": dag}),
        # LXMF field
        ("lxmf field, one link", "lxmf_field", {"links": [link(cid=raw)]}),
        ("lxmf field, several links", "lxmf_field", {"links": [
            link(cid=dag, path=["photos", "été.jpg"], size=123456, **{"from": NODES[0]}),
            link(name=name, cid=dag, path=["2026"]),
            link(name=name),
            link(cid=raw, size=0),
        ]}),
    ]


def encode(m, kind: str, inputs: dict):
    """What goes on the wire for a case: the request or response data (packed by the caller), or app_data bytes."""
    p = m.protocol
    if kind == "find_request":
        return p.encode_find(inputs["key"])
    if kind == "find_response":
        return p.encode_find_response(inputs["providers"], inputs["nodes"])
    if kind == "provide_request":
        return p.encode_provide(inputs["keys"], inputs["withdrawn"])
    if kind == "accept_response":
        return bool(inputs["accepted"])  # /provide and /pin handlers answer True or False
    if kind == "blocks_request":
        return p.encode_blocks(inputs["max_bytes"], inputs["cids"])
    if kind == "blocks_response":
        return p.encode_blocks_response(inputs["entries"])
    if kind == "pin_request":
        return p.encode_pin(inputs["cid"], inputs["pin"])
    if kind == "name_request":
        return m.keepers.encode_request(inputs["name"])
    if kind == "name_response":
        r = inputs["record"]
        record = None if r is None else m.keepers.SignedRecord(
            inputs["name"], r["public_key"], r["random_hash"], b"", r["signature"], r["app_data"], r["sequence"],
            r["cid"])
        return m.keepers.encode_response(record)
    if kind == "announce_mode":
        return m.announces.encode_mode(inputs["server"])
    if kind == "name_record":
        return m.names.encode_record(inputs["sequence"], inputs["cid"])
    if kind == "lxmf_field":
        return m.lxmf.encode_field([m.lxmf.Link(cid=f["cid"], name=f["name"], path=tuple(f["path"]), size=f["size"],
                                                from_node=f["from"]) for f in inputs["links"]])
    raise ValueError(kind)


def parse(m, kind: str, data, inputs: dict) -> dict:
    """A case's inputs, back from its wire data with the tree's parsers. inputs supplies only what a parser is
    given besides the data (the CIDs requested, the name asked for)."""
    p = m.protocol
    if kind == "find_request":
        return {"key": p.parse_find_request(data)}
    if kind == "find_response":
        providers, nodes = p.parse_find_response(data)
        return {"providers": providers, "nodes": nodes}
    if kind == "provide_request":
        keys, withdrawn = p.parse_provide_request(data)
        return {"keys": keys, "withdrawn": withdrawn}
    if kind == "accept_response":
        return {"accepted": p.is_accepted(data)}
    if kind == "blocks_request":
        max_bytes, cids = p.parse_blocks_request(data)
        return {"max_bytes": max_bytes, "cids": cids}
    if kind == "blocks_response":
        return {"requested": inputs["requested"], "entries": p.parse_blocks_response(data, inputs["requested"])}
    if kind == "pin_request":
        cid, pin = p.parse_pin_request(data)
        return {"cid": cid, "pin": pin}
    if kind == "name_request":
        return {"name": m.keepers.parse_request(data)}
    if kind == "name_response":
        record = m.keepers.parse_response(inputs["name"], data)
        return {"name": inputs["name"], "record": None if record is None else {
            "public_key": record.public_key, "random_hash": record.random_hash, "signature": record.signature,
            "app_data": record.app_data, "sequence": record.sequence, "cid": record.cid}}
    if kind == "announce_mode":
        return {"server": m.announces.parse_mode(data) == m.constants.MODE_SERVER}
    if kind == "name_record":
        sequence, cid = m.names.parse_record(data)
        return {"sequence": sequence, "cid": cid}
    if kind == "lxmf_field":
        return {"links": [{"cid": link.cid, "name": link.name, "path": list(link.path), "size": link.size,
                           "from": link.from_node} for link in m.lxmf.decode_field(data)]}
    raise ValueError(kind)


def wire(m, kind: str, inputs: dict) -> bytes:
    """The vector's bytes: app_data as is, anything else packed as RNS packs request and response data."""
    data = encode(m, kind, inputs)
    return data if kind in RAW_KINDS else m.umsgpack.packb(data)


def unwire(m, kind: str, data: bytes):
    """wire()'s inverse, before parse()."""
    return data if kind in RAW_KINDS else m.umsgpack.unpackb(data)


def to_json(value):
    """Inputs as JSON: bytes as {"hex": ...}, a CID as {"cid": its text}."""
    if isinstance(value, bytes):
        return {"hex": value.hex()}
    if isinstance(value, dict):
        return {key: to_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_json(item) for item in value]
    if value is None or isinstance(value, (bool, int, str)):
        return value
    return {"cid": str(value)}  # a Cid (the only other type the cases hold)


def from_json(m, value):
    """to_json()'s inverse, with the tree's Cid."""
    if isinstance(value, dict):
        if set(value) == {"hex"}:
            return bytes.fromhex(value["hex"])
        if set(value) == {"cid"}:
            return m.cid.Cid.parse(value["cid"])
        return {key: from_json(m, item) for key, item in value.items()}
    if isinstance(value, list):
        return [from_json(m, item) for item in value]
    return value
