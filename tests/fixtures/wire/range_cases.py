"""The /range known-answer cases (specification section 7, `/range`): the one request rnfs 0.2.0 added to the wire, so
they can't come from 0.1.3 and 0.1.4 as wire_cases.py's do. generate.py --range writes range_vectors.json from the tree
that introduced the request (0.2.0); every later tree must encode the same inputs to the same bytes and parse them
back, as tests/test_wire.py checks.

Each case is (name, kind, inputs), as in wire_cases.py, whose fixed blocks, modules(), to_json() and from_json() these
use. The bz2 stream is fixed bytes (BZ2_STREAM, what bz2.compress(BZ2_TEXT, 9) gave), not compressed at each run, so a
compressor giving other bytes for the same text couldn't change the vectors: what is fixed is the answer's framing and
that the stream decodes to BZ2_TEXT.
"""
import importlib.util
import os

_spec = importlib.util.spec_from_file_location("wire_cases", os.path.join(os.path.dirname(__file__), "wire_cases.py"))
wire_cases = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(wire_cases)

KINDS = ("range_request", "range_response", "range_unserved")
BZ2_TEXT = b"rnfs /range vector: " * 50  # 1,000 bytes
BZ2_STREAM = bytes.fromhex("425a6839314159265359a9174fb40001a89980400080102b819d0020006a0a551a01a014aa00d0d328b08b8458"
                           "45922d51768be45922dd16116516517a8b08b28b28b545f8bb9229c2848548ba7da0")


def cases(m) -> list[tuple[str, str, dict]]:
    """Every /range case, with typed inputs (bytes, the tree's Cid)."""
    Cid = m.cid.Cid
    raw = Cid.for_block(m.cid.RAW, wire_cases.RAW_BLOCK)
    dag = Cid.for_block(m.cid.DAG, wire_cases.DAG_BLOCK)
    bz2 = m.constants.RANGE_BZ2
    block = wire_cases.RAW_BLOCK
    return [
        ("range request, from the start", "range_request", {"cid": raw, "offset": 0, "max_bytes": 16384, "flags": 0}),
        ("range request, compressed", "range_request", {"cid": dag, "offset": 65536, "max_bytes": 131072,
                                                        "flags": bz2}),
        ("range request, at the largest block's end", "range_request",
         {"cid": raw, "offset": 131072, "max_bytes": 1, "flags": 0}),
        ("range answer, as is", "range_response",
         {"size": len(block), "encoding": 0, "data": block[5:15], "offset": 5, "max_bytes": 10, "raw": block[5:15]}),
        ("range answer, the rest of the block", "range_response",
         {"size": len(block), "encoding": 0, "data": block[20:], "offset": 20, "max_bytes": 4096, "raw": block[20:]}),
        ("range answer, bz2", "range_response",
         {"size": 70000, "encoding": bz2, "data": BZ2_STREAM, "offset": 65536, "max_bytes": 1000, "raw": BZ2_TEXT}),
        ("range answer, at the end", "range_response",
         {"size": len(block), "encoding": 0, "data": b"", "offset": len(block), "max_bytes": 10, "raw": b""}),
        ("range answer, not served", "range_unserved", {"unserved": True}),
    ]


def encode(m, kind: str, inputs: dict):
    """A case's request or answer data, with the tree's encoders."""
    p = m.protocol
    if kind == "range_request":
        return p.encode_range(inputs["cid"], inputs["offset"], inputs["max_bytes"], inputs["flags"])
    if kind == "range_response":
        return p.encode_range_response(inputs["size"], inputs["encoding"], inputs["data"])
    if kind == "range_unserved":
        return []  # what the responder answers for a block it doesn't serve (RNS sends nothing for None)
    raise ValueError(kind)


def parse(m, kind: str, data, inputs: dict) -> dict:
    """A case's inputs, back from its data with the tree's parsers (an answer given the request's offset and
    max_bytes, which it doesn't carry, and its data as sent)."""
    p = m.protocol
    if kind == "range_request":
        cid, offset, max_bytes, flags = p.parse_range_request(data)
        return {"cid": cid, "offset": offset, "max_bytes": max_bytes, "flags": flags}
    if kind == "range_response":
        size, raw = p.parse_range_response(data, inputs["offset"], inputs["max_bytes"], True, inputs["size"])
        return {"size": size, "encoding": data[1], "data": data[2], "offset": inputs["offset"],
                "max_bytes": inputs["max_bytes"], "raw": raw}
    if kind == "range_unserved":
        return {"unserved": p.is_range_unserved(data)}
    raise ValueError(kind)


def wire(m, kind: str, inputs: dict) -> bytes:
    """The vector's bytes: the data packed as RNS packs request and response data."""
    return m.umsgpack.packb(encode(m, kind, inputs))


def unwire(m, kind: str, data: bytes):
    """wire()'s inverse, before parse()."""
    return m.umsgpack.unpackb(data)
