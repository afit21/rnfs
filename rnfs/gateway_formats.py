"""Trustless gateway responses (rnfs/gateway.py calls serve_format for any /rnfs/ request with ?format=):

GET /rnfs/<cid>?format=raw              the CID's own block, re-verified; application/vnd.rnfs.raw
GET /rnfs/<cid>[/path]?format=archive   an archive (spec section 8) of the target's DAG;
                                        application/vnd.rnfs.archive, Content-Disposition attachment

The client can verify either without trusting the gateway: a raw block hashes to its CID, and an archive's
blocks are checked by `rnfs import` (or any reader) from the root down. Every gateway header rule still
applies (the handler's end_headers adds nosniff and the CSP, and no CORS header); both are immutable, with
an ETag naming the target and the format ("<cid>.raw", "<cid>.archive") so they never collide with the
plain response's "<cid>". Archives keep max_download_size and max_output_entries. /name/ paths don't take
?format= (the result must be named by the CID it is checked against).

Every format is answered only to the user's own GET (handler.limited() false: Sec-Fetch-Site "none", or none sent
by a client whose User-Agent doesn't start with "Mozilla/"), which fetches what isn't stored (an archive's fetch is
cancelled once the client disconnects). A web page's request or a HEAD always gets 409, stored or not, before the
node is asked anything, so no format ever takes a gateway_fetches slot. Neither type is one a browser shows, so a
format's 200 is a download, and an iframe of a download fires no load event while an error page does: answering a
page 200 for stored content but a fetch (an error once nobody has it, or 503 once the page holds every
gateway_fetches slot) for the rest would tell web pages what the node stores (L6). A bare-CID If-None-Match
revalidation, which needs nothing stored, is answered 304 first. An archive also needs the target's whole DAG
before its first byte (its length is in the headers), and a request whose body nobody reads shouldn't make the node
fetch a whole DAG.
"""
import select
import socket

from rnfs.content.cid import Cid
from rnfs.fetcher import Cancelled, Observer, observing

RAW_TYPE = "application/vnd.rnfs.raw"
ARCHIVE_TYPE = "application/vnd.rnfs.archive"
FORMATS = ("raw", "archive")
TOOLS = {"raw": "`rnfs cat --block`", "archive": "`rnfs export`"}  # for the 409's message


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

    if not names and etag_matches(handler.headers.get("If-None-Match"), f'"{root}.{fmt}"'):
        return _not_modified(handler, f'"{root}.{fmt}"', IMMUTABLE)  # a revalidation needs nothing stored
    if handler.limited() or handler.command == "HEAD":
        # Whether the target is stored or not: the answer must not tell a web page which it is (L6)
        raise GatewayError(409, f"?format={fmt} is only answered to a GET of the user's own, never to a web page's "
                                "request or a HEAD, whether the content is stored or not: open the URL directly, "
                                f"or use {TOOLS[fmt]}")

    if fmt == "raw":
        block = handler.node.block(root, for_gateway=True)
        return handler.send_body(200, RAW_TYPE, block, {"ETag": f'"{root}.raw"', "Cache-Control": IMMUTABLE})

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
        """Send the headers once the archive's length is known; where to write it."""
        handler.send_response(200)
        handler.send_header("Content-Type", ARCHIVE_TYPE)
        handler.send_header("Content-Length", str(length))
        for name, value in headers.items():
            handler.send_header(name, value)
        handler.end_headers()
        return handler.wfile

    try:
        # The fetch runs on this thread, so the observer sees it and cancels it once the client has gone
        with observing(ClientWatch(handler.connection)):
            handler.node.export_stream(target, start, for_gateway=True)
    except Cancelled:
        handler.close_connection = True  # nobody to answer
    except OSError as e:
        # Once the headers are out, a write failing or timing out means the client went away or stopped reading
        # (a web page can make that happen at will, so it isn't an error to log); a read error here is one
        if not handler.headers_sent or not (isinstance(e, TimeoutError) or client_gone(handler.connection)):
            raise
        handler.close_connection = True


def client_gone(connection) -> bool:
    """True if the client closed its connection (or it failed): readable with nothing left to read."""
    try:
        if hasattr(select, "poll"):
            # Not select.select, which can't take a file descriptor of 1024 or more
            poller = select.poll()
            poller.register(connection, select.POLLIN)
            readable = poller.poll(0)
        else:  # Windows, whose select() takes any socket
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
