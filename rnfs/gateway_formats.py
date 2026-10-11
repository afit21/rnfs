"""Trustless gateway responses (rnfs/gateway.py calls serve_format for any /rnfs/ request with ?format=):

GET/HEAD /rnfs/<cid>?format=raw              the CID's own block, re-verified; application/vnd.rnfs.raw
GET/HEAD /rnfs/<cid>[/path]?format=archive   an archive (spec section 8) of the target's DAG;
                                             application/vnd.rnfs.archive, Content-Disposition attachment

The client can verify either without trusting the gateway: a raw block hashes to its CID, and an archive's
blocks are checked by `rnfs import` (or any reader) from the root down. Every gateway header rule still
applies (the handler's end_headers adds nosniff, the CSP and the CORS rule); both are immutable, with an
ETag naming the target and the format ("<cid>.raw", "<cid>.archive") so they never collide with the plain
response's "<cid>". Fetches count against gateway_fetches like any other, and archives keep
max_download_size and max_output_entries. /name/ paths don't take ?format= (the result must be named by
the CID it is checked against).

An archive needs the target's whole DAG before its first byte (its length is in the headers), so a web
page's request (Sec-Fetch-Site present and not "none") or a HEAD gets one only of a target already stored
completely, else 409; nobody waiting for a body shouldn't make the node fetch a whole DAG. The user's own
GET of one not stored fetches it, and that fetch is cancelled once the client disconnects.
"""
import select
import socket

from rnfs.content.cid import Cid
from rnfs.fetcher import Cancelled, Observer, observing

RAW_TYPE = "application/vnd.rnfs.raw"
ARCHIVE_TYPE = "application/vnd.rnfs.archive"
FORMATS = ("raw", "archive")


def serve_format(handler, segments: list[str], names: list[str]):
    """Answer a request with ?format=: segments are the URL path's, names its decoded path names."""
    from rnfs.gateway import IMMUTABLE, GatewayError, etag_matches

    fmt = handler.param("format")
    if fmt not in FORMATS:
        raise GatewayError(400, f"unknown format {fmt!r}: use raw or archive")
    if segments[1] != "rnfs":
        raise GatewayError(400, "?format= is only for /rnfs/<cid> paths")
    try:
        root = str(Cid.parse(segments[2]))
    except ValueError as e:
        raise GatewayError(400, str(e)) from None
    if fmt == "raw" and names:
        raise GatewayError(400, "?format=raw is only for a bare CID: request the CID inside the path itself")
    target = "/".join([root, *names])

    if fmt == "raw":
        etag = f'"{root}.raw"'
        if etag_matches(handler.headers.get("If-None-Match"), etag):
            return _not_modified(handler, etag, IMMUTABLE)
        if handler.limited() and not handler.node.is_local(root):
            handler.need_network()
        block = handler.node.block(root, for_gateway=True)
        return handler.send_body(200, RAW_TYPE, block, {"ETag": etag, "Cache-Control": IMMUTABLE})

    if not names and etag_matches(handler.headers.get("If-None-Match"), f'"{root}.archive"'):
        return _not_modified(handler, f'"{root}.archive"', IMMUTABLE)  # a revalidation needs nothing stored
    if (handler.limited() or handler.command == "HEAD") and not handler.node.is_local(target):
        raise GatewayError(409, "an archive of content not completely stored on this node is only made for a GET "
                                "of the user's own (not a web page's request or a HEAD): fetching it takes the "
                                "whole DAG; open the URL directly or `rnfs pin` it first")
    if names:
        cid = handler.node.info(target, for_gateway=True)["cid"]  # the ETag names the target, not the root
    else:
        cid = root
    etag = f'"{cid}.archive"'
    if etag_matches(handler.headers.get("If-None-Match"), etag):
        return _not_modified(handler, etag, IMMUTABLE)
    headers = {"ETag": etag, "Cache-Control": IMMUTABLE,
               "Content-Disposition": f'attachment; filename="{cid}.rnfs-archive"'}

    def start(length: int):
        """Send the headers once the archive's length is known; where to write it (None for HEAD)."""
        handler.send_response(200)
        handler.send_header("Content-Type", ARCHIVE_TYPE)
        handler.send_header("Content-Length", str(length))
        for name, value in headers.items():
            handler.send_header(name, value)
        handler.end_headers()
        return None if handler.command == "HEAD" else handler.wfile

    try:
        # The fetch runs on this thread, so the observer sees it and cancels it once the client has gone
        with observing(ClientWatch(handler.connection)):
            handler.node.export_stream(target, start, for_gateway=True)
    except Cancelled:
        handler.close_connection = True  # nobody to answer


def client_gone(connection) -> bool:
    """True if the client closed its connection (or it failed): readable with nothing left to read."""
    try:
        readable, _, _ = select.select([connection], [], [], 0)
        if not readable:
            return False
        # A GET has no body, so anything readable is end of stream or a pipelined request (still there)
        return connection.recv(1, socket.MSG_PEEK) == b""
    except BlockingIOError:
        return False
    except (OSError, ValueError):  # reset, or already closed
        return True


class ClientWatch(Observer):
    """Cancels the fetches of a request whose client has disconnected (see rnfs.fetcher.observing)."""

    def __init__(self, connection):
        """Watch connection (the request's socket)."""
        self.connection = connection

    def cancelled(self) -> bool:
        """True once the client has gone away."""
        return client_gone(self.connection)


def _not_modified(handler, etag: str, cache_control: str):
    """304 for a revalidated format response."""
    handler.send_response(304)
    handler.send_header("ETag", etag)
    handler.send_header("Cache-Control", cache_control)
    handler.end_headers()
