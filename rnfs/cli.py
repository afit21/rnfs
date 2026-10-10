"""rnfs command line (specification section 11).

`daemon` runs a node with its API and gateway; every other command is one request to a running daemon's
local API (see rnfs/api.py). Client commands never import RNS, so they start quickly; paths given to them
are resolved to absolute paths here, since the daemon reads and writes them itself.
"""
import argparse
import http.client
import json
import os
import stat
import sys
from urllib.parse import urlencode

from rnfs.config import DEFAULT_API_PORT, Config

DEFAULT_DIR = Config.rnfs_dir
COPY_SIZE = 64 * 1024


def printable(text) -> str:
    """text with control characters escaped, so names from other people's DAGs can't drive the terminal."""
    text = str(text)
    if text.isprintable():
        return text
    return "".join(c if c.isprintable() else c.encode("unicode_escape").decode("ascii") for c in text)


def warn(message: str):
    """Print a warning to stderr."""
    print(f"rnfs: warning: {printable(message)}", file=sys.stderr)


def fail(message: str):
    """Exit with status 1 and an error message on stderr."""
    sys.exit(f"rnfs: {printable(message)}")


class ApiClient:
    """Talks to the daemon's API on 127.0.0.1:port, exiting with a friendly message on any error."""

    def __init__(self, port: int):
        self.port = port

    def request(self, method: str, path: str, params: dict | None = None, json_body: dict | None = None,
                body=None, length: int | None = None) -> http.client.HTTPResponse:
        """Send a request and return the successful response; exits on errors.

        body may be bytes or a file; a file without length is sent chunked.
        """
        url = path + ("?" + urlencode({k: v for k, v in params.items() if v is not None}) if params else "")
        # Host is set explicitly: the API only accepts 127.0.0.1:<port>, even for port 80
        headers = {"Host": f"127.0.0.1:{self.port}", "X-Rnfs-Api": "1"}
        if json_body is not None:
            body = json.dumps(json_body).encode()
            headers["Content-Type"] = "application/json"
        elif body is not None:
            headers["Content-Type"] = "application/octet-stream"
            if length is not None:
                headers["Content-Length"] = str(length)
        connection = http.client.HTTPConnection("127.0.0.1", self.port)  # no timeout: fetches can take hours
        try:
            connection.request(method, url, body=body, headers=headers)
            response = connection.getresponse()
        except ConnectionRefusedError:
            fail(f"no daemon is running on API port {self.port}. Start one with: rnfs daemon")
        except (http.client.HTTPException, ConnectionError) as e:
            fail(f"the daemon dropped the connection ({e!r}); see its log")
        except OSError as e:
            fail(f"can't reach the daemon on API port {self.port}: {e}")
        if response.status >= 400:
            data = response.read()
            try:
                message = json.loads(data)["error"]
            except (ValueError, KeyError, TypeError):
                message = f"HTTP {response.status} {response.reason}"
            fail(message)
        return response

    def call(self, method: str, path: str, params: dict | None = None, json_body: dict | None = None,
             body=None, length: int | None = None):
        """Send a request and return its decoded JSON response."""
        response = self.request(method, path, params, json_body, body, length)
        try:
            return json.loads(read_all(response))
        except ValueError:
            fail("the daemon sent an invalid response")

    def get(self, path: str, /, **params):
        """GET path with query parameters; JSON response."""
        return self.call("GET", path, params=params)

    def post(self, path: str, /, **fields):
        """POST path with a JSON body; JSON response."""
        return self.call("POST", path, json_body=fields)


def read_all(response: http.client.HTTPResponse) -> bytes:
    """A response's whole body, failing if it is shorter than its Content-Length (the daemon hit an error)."""
    try:
        data = response.read()
    except http.client.IncompleteRead:
        fail("the response was cut short; see the daemon's log")
    return data


def copy_response(response: http.client.HTTPResponse, out):
    """Copy a response body to out, failing if it ends before its Content-Length."""
    expected = response.getheader("Content-Length")
    copied = 0
    while True:
        try:
            chunk = response.read(COPY_SIZE)
        except http.client.IncompleteRead:
            chunk = b""
        if not chunk:
            break
        out.write(chunk)
        copied += len(chunk)
    out.flush()
    if expected is not None and copied != int(expected):
        fail(f"the transfer stopped after {copied} of {expected} bytes; see the daemon's log")


def client(args) -> ApiClient:
    """An ApiClient for --api-port, or the api_port in <dir>/rnfs.toml, or the default port."""
    port = args.api_port
    if port is None:
        try:
            port = Config.load(args.dir).api_port
        except (ValueError, OSError) as e:
            fail(f"can't read the settings in {args.dir}: {e}")
    return ApiClient(port if port is not None else DEFAULT_API_PORT)


def absolute(path: str) -> str:
    """path made absolute here, since the daemon may run in another directory."""
    return os.path.abspath(os.path.expanduser(path))


def print_warnings(result: dict):
    """Print the warnings a daemon response carries."""
    for warning in result.get("warnings") or []:
        warn(warning)


# Daemon


def run_daemon(args):
    """Run a node with its API and gateway until SIGINT or SIGTERM, then shut everything down cleanly."""
    # Imported here so client commands don't pay for loading RNS
    import faulthandler
    import signal
    import threading

    import RNS

    from rnfs.api import ApiServer
    from rnfs.gateway import GatewayServer
    from rnfs.node import RnfsNode

    try:
        config = Config.load(args.dir, reticulum_dir=args.rnsconfig,
                             api_port=args.api_port, gateway_port=args.gateway_port,
                             bootstrap_nodes=args.bootstrap, verbosity=args.verbose or None,
                             dht_mode=args.dht_mode, slow=args.slow, private=args.private,
                             provide_strategy=args.provide_strategy,
                             gateway_scripts=False if args.no_gateway_scripts else None)
    except (ValueError, OSError) as e:
        fail(f"invalid settings: {e}")

    node = RnfsNode(config)
    servers = []
    try:
        servers.append(ApiServer(node, config.api_port))
        servers.append(GatewayServer(node, config, config.gateway_port))
    except OSError as e:
        for server in servers:
            server.stop()
        node.stop()
        RNS.exit()
        fail(f"can't listen on 127.0.0.1: {e}. Is another daemon running? (--api-port, --gateway-port)")
    for server in servers:
        server.start()
    api, gateway = servers
    print(f"rnfs node {node.node_id.hex()}, API on http://127.0.0.1:{api.port}, "
          f"gateway on http://127.0.0.1:{gateway.port}", flush=True)
    # `kill -USR1 <pid>` prints every thread's stack, for debugging hangs
    faulthandler.register(signal.SIGUSR1, all_threads=True)

    # Replace RNS's handlers, which exit immediately, so the node shuts down first
    stopping = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stopping.set())
    signal.signal(signal.SIGTERM, lambda *_: stopping.set())
    while not stopping.wait(1):
        pass

    for server in servers:
        server.stop()
    node.stop()
    # RNS.exit() ends with os._exit, which would lose log lines still buffered when stdout is a file
    sys.stdout.flush()
    sys.stderr.flush()
    RNS.exit()  # saves Reticulum's path table and known destinations


# Content


def run_add(args):
    """Add a file, directory or stdin and print its CID."""
    api = client(args)
    if args.path == "-":
        if args.wrap and not args.name:
            fail("--wrap with stdin needs --name, the entry's name")
        stream = sys.stdin.buffer
        try:
            info = os.fstat(stream.fileno())
            length = info.st_size if stat.S_ISREG(info.st_mode) else None  # else sent chunked
        except (OSError, ValueError, AttributeError):
            length = None
        result = api.call("POST", "/api/add/stream", params={"name": args.name, "only_hash": int(args.only_hash)},
                          body=stream, length=length)
    else:
        if args.name:
            fail("--name is only for stdin (add -)")
        path = absolute(args.path)
        if not os.path.lexists(path):
            fail(f"no such file or directory: {args.path}")
        result = api.post("/api/add", path=path, wrap=args.wrap, only_hash=args.only_hash)
    print_warnings(result)
    print(result["cid"])


def default_output(target: str) -> str:
    """Where `get` writes without -o: the target's last path name, else its CID, in the current directory."""
    name = target.rstrip("/").rsplit("/", 1)[-1]
    if name in ("", ".", "..") or "\x00" in name:
        name = target.split("/", 1)[0]
    return name


def run_get(args):
    """Fetch a file or directory and have the daemon write it out."""
    output = absolute(args.output or default_output(args.target))
    if os.path.lexists(output):
        fail(f"{output} already exists; remove it or choose another with -o")
    result = client(args).post("/api/get", target=args.target, output=output, **{"from": args.from_node})
    print_warnings(result)
    print(output)


def run_cat(args):
    """Write a file, or part of one, to stdout."""
    response = client(args).request("GET", "/api/cat", params={"target": args.target, "offset": args.offset,
                                                                "length": args.length})
    try:
        copy_response(response, sys.stdout.buffer)
    except BrokenPipeError:
        # The reader (e.g. `head`) has gone; exit quietly without a second error at interpreter shutdown
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        sys.exit(1)


def run_ls(args):
    """List a directory: kind, size, CID and name (last, since names can contain spaces)."""
    for entry in client(args).get("/api/ls", target=args.target)["entries"]:
        name = printable(entry["name"]) + ("/" if entry["kind"] == "directory" else "")
        print(f"{entry['kind']:<10} {entry['size']:>12} {entry['cid']} {name}")


def print_fields(result: dict):
    """Print a JSON object as "name: value" lines."""
    for name, value in result.items():
        if isinstance(value, list):
            value = ", ".join(str(item) for item in value) if value else "none"
        print(f"{name}: {printable(value)}")


def run_stat(args):
    """Print a target's size, block count and local holdings."""
    print_fields(client(args).get("/api/stat", target=args.target))


def run_dir_set(args):
    """Make a directory with an entry added or replaced; print its CID."""
    print(client(args).post("/api/dir/set", target=args.target, name=args.name, cid=args.cid,
                            executable=args.exec)["cid"])


def run_dir_rm(args):
    """Make a directory with an entry removed; print its CID."""
    print(client(args).post("/api/dir/rm", target=args.target)["cid"])


# Pins


def run_pin_add(args):
    """Fetch and pin a target; print its CID once complete."""
    print(client(args).post("/api/pin/add", target=args.target)["cid"])


def run_pin_rm(args):
    """Remove the user pin of a CID."""
    client(args).post("/api/pin/rm", cid=args.cid)


def run_pin_ls(args):
    """List pins with their sources."""
    for pin in client(args).get("/api/pin/ls", cid=args.cid)["pins"]:
        state = "" if pin["complete"] else " (incomplete)"
        print(f"{pin['cid']} {','.join(printable(s) for s in pin['sources'])}{state}")


def run_pin_remote(args):
    """Ask a node to pin or release a CID."""
    result = client(args).post("/api/pin/remote", node=args.node, cid=args.cid, pin=args.action == "add")
    if not result["accepted"]:
        fail(f"{args.node} refused the request")
    print("accepted")


def run_gc(args):
    """Evict every cached fetch."""
    result = client(args).post("/api/gc")
    print(f"removed {result['removed_blocks']} blocks, freed {result['freed_bytes']} bytes")


def run_verify(args):
    """Re-check every stored block; exits with status 1 if any were bad (they are deleted)."""
    result = client(args).post("/api/verify")
    print(f"checked {result['checked']} blocks")
    for cid in result["bad"]:
        print(f"bad block deleted: {cid}")
    if result["bad"]:
        sys.exit(1)


# Network


def run_providers(args):
    """Print the IDs of nodes providing a CID, one per line."""
    providers = client(args).get("/api/providers", cid=args.cid)["providers"]
    if not providers:
        fail("no providers found")
    print("\n".join(providers))


def run_ping(args):
    """Print the round-trip time to a node."""
    rtt = client(args).get("/api/ping", node=args.node)["rtt"]
    if rtt is None:
        fail(f"no answer from {args.node}")
    print(f"round-trip time {rtt * 1000:.0f} ms")


# Names


def run_name_create(args):
    """Create a name key; print the name."""
    print(client(args).post("/api/name/create", alias=args.alias)["name"])


def run_name_rm(args):
    """Delete a name key, releasing its pin."""
    client(args).post("/api/name/rm", alias=args.alias)


def run_name_export(args):
    """Write a name's key to -o (created with mode 0600) or stdout."""
    if not args.output and sys.stdout.isatty():
        fail("refusing to write a private key to the terminal; use -o <file> or redirect stdout")
    response = client(args).request("POST", "/api/name/export", json_body={"alias": args.alias})
    key = read_all(response)
    if args.output:
        try:
            fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            fail(f"{args.output} already exists")
        except OSError as e:
            fail(f"can't write {args.output}: {e}")
        with os.fdopen(fd, "wb") as out:
            out.write(key)
    else:
        sys.stdout.buffer.write(key)
        sys.stdout.buffer.flush()


def run_name_import(args):
    """Import a name key from a file or stdin; print the name."""
    if args.file == "-":
        key = sys.stdin.buffer.read()
    else:
        try:
            with open(args.file, "rb") as f:
                key = f.read()
        except OSError as e:
            fail(f"can't read {args.file}: {e}")
    result = client(args).call("POST", "/api/name/import", params={"alias": args.alias}, body=key)
    print(result["name"])


def run_name_list(args):
    """List this node's names: alias, name, CID and sequence."""
    for entry in client(args).get("/api/name/list")["names"]:
        print(f"{printable(entry['alias'])}\t{entry['name']}\t{entry.get('cid') or '-'}\t{entry.get('sequence', 0)}")


def run_name_publish(args):
    """Point a name at a target."""
    result = client(args).post("/api/name/publish", alias=args.alias, target=args.target)
    print(f"published {result['cid']} as {result['name']} (sequence {result['sequence']})")


def run_name_resolve(args):
    """Print the CID a name points at."""
    print(client(args).get("/api/name/resolve", name=args.name)["cid"])


# Archives and status


def run_export(args):
    """Write a target and its DAG to an archive file."""
    output = absolute(args.output)
    result = client(args).post("/api/export", target=args.target, output=output)
    print(f"exported {result['blocks']} blocks of {', '.join(result['roots'])} to {output}")


def run_import(args):
    """Import an archive, pinning its complete roots."""
    path = absolute(args.file)
    if not os.path.isfile(path):
        fail(f"no such file: {args.file}")
    result = client(args).post("/api/import", path=path)
    for root in result["roots"]:
        print(root)
    for root in result.get("incomplete") or []:
        warn(f"{root} is incomplete in the archive, so it wasn't pinned")


def run_status(args):
    """Print the daemon's status, one "name: value" per line."""
    print_fields(client(args).get("/api/status"))


# Parser


def build_parser() -> argparse.ArgumentParser:
    """The argument parser for every command."""
    parser = argparse.ArgumentParser(prog="rnfs", description="Content-addressed file sharing over Reticulum")
    parser.add_argument("--api-port", type=int, default=None, help=f"daemon's API port (default {DEFAULT_API_PORT})")
    parser.add_argument("--dir", default=DEFAULT_DIR, help="rnfs directory (settings, identity, storage)")
    # Repeated on every subcommand so they can also go after the command name. SUPPRESS keeps a
    # subcommand's unset default from overwriting a value given before the command
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--api-port", type=int, default=argparse.SUPPRESS, help="daemon's API port")
    common.add_argument("--dir", default=argparse.SUPPRESS, help="rnfs directory")
    commands = parser.add_subparsers(dest="command", required=True, metavar="command")

    def command(subparsers, name: str, run, help: str) -> argparse.ArgumentParser:
        """Add a subcommand that runs run(args)."""
        sub = subparsers.add_parser(name, parents=[common], help=help, description=help)
        sub.set_defaults(run=run)
        return sub

    daemon = command(commands, "daemon", run_daemon, "run a node, its API and its gateway")
    daemon.add_argument("--bootstrap", nargs="+", action="extend", metavar="NODE_ID",
                        help="node IDs (hex) to join the DHT through")
    daemon.add_argument("--gateway-port", type=int, help="gateway port (default 8789)")
    daemon.add_argument("--rnsconfig", metavar="DIR",
                        help="Reticulum config directory (default: RNS's own, the one rnsd uses)")
    daemon.add_argument("--dht-mode", choices=["server", "client"])
    daemon.add_argument("--slow", choices=["yes", "no", "auto"])
    daemon.add_argument("--private", action="store_true", default=None, help="private mode: members only")
    daemon.add_argument("--provide-strategy", choices=["pinned", "roots"])
    daemon.add_argument("--no-gateway-scripts", action="store_true", help="don't let gateway pages run scripts")
    daemon.add_argument("-v", "--verbose", action="count", default=0,
                        help="log more detail: -v for every DHT query, -vv for RNS debug output")

    add = command(commands, "add", run_add, "add a file, directory or stdin (-), pin it and print its CID")
    add.add_argument("path", help="file or directory, or - for stdin")
    add.add_argument("--wrap", action="store_true", help="put it in a directory")
    add.add_argument("--only-hash", action="store_true", help="just print the CID; store nothing")
    add.add_argument("--name", help="with stdin: wrap it in a directory as this entry")

    get = command(commands, "get", run_get, "fetch and write out a file or directory")
    get.add_argument("target", metavar="CID[/PATH]")
    get.add_argument("-o", "--output", help="where to write (default: the last path name or the CID, here)")
    get.add_argument("--from", dest="from_node", metavar="NODE_ID", help="fetch from this node, bypassing the DHT")

    cat = command(commands, "cat", run_cat, "write a file, or part of one, to stdout")
    cat.add_argument("target", metavar="CID[/PATH]")
    cat.add_argument("--offset", type=int, default=None, help="first byte to write")
    cat.add_argument("--length", type=int, default=None, help="number of bytes to write")

    ls = command(commands, "ls", run_ls, "list a directory: kind, size, CID, name")
    ls.add_argument("target", metavar="CID[/PATH]")

    stat_ = command(commands, "stat", run_stat, "size, block count, and how much is held locally")
    stat_.add_argument("target", metavar="CID[/PATH]")

    dir_ = commands.add_parser("dir", help="edit directories (making new ones)").add_subparsers(
        dest="dir_command", required=True, metavar="command")
    dir_set = command(dir_, "set", run_dir_set, "make a directory with an entry added or replaced")
    dir_set.add_argument("target", metavar="CID[/PATH]", help="the directory")
    dir_set.add_argument("name", help="entry name")
    dir_set.add_argument("cid", help="the entry's CID")
    dir_set.add_argument("--exec", action="store_true", help="mark the entry executable")
    dir_rm = command(dir_, "rm", run_dir_rm, "make a directory with an entry removed")
    dir_rm.add_argument("target", metavar="CID/PATH", help="the entry to remove")

    pin = commands.add_parser("pin", help="manage pins").add_subparsers(dest="pin_command", required=True,
                                                                        metavar="command")
    pin_add = command(pin, "add", run_pin_add, "fetch and pin")
    pin_add.add_argument("target", metavar="CID[/PATH]")
    pin_rm = command(pin, "rm", run_pin_rm, "remove the user pin")
    pin_rm.add_argument("cid")
    pin_ls = command(pin, "ls", run_pin_ls, "list pins and their sources")
    pin_ls.add_argument("cid", nargs="?")
    remote = pin.add_parser("remote", help="remote pinning").add_subparsers(dest="action", required=True,
                                                                            metavar="command")
    for action in ("add", "rm"):
        sub = command(remote, action, run_pin_remote, f"ask a node to {'pin' if action == 'add' else 'unpin'} a CID")
        sub.add_argument("node", metavar="NODE_ID")
        sub.add_argument("cid")

    command(commands, "gc", run_gc, "evict all cached fetches")
    command(commands, "verify", run_verify, "re-check every stored block against its CID")

    providers = command(commands, "providers", run_providers, "find nodes providing a CID")
    providers.add_argument("cid")
    ping = command(commands, "ping", run_ping, "report the round-trip time to a node")
    ping.add_argument("node", metavar="NODE_ID")

    name = commands.add_parser("name", help="mutable names").add_subparsers(dest="name_command", required=True,
                                                                            metavar="command")
    for action, run, help in (("create", run_name_create, "create a name key"),
                              ("rm", run_name_rm, "delete a name key and release its pin")):
        command(name, action, run, help).add_argument("alias")
    name_export = command(name, "export", run_name_export, "write a name's key to stdout or -o")
    name_export.add_argument("alias")
    name_export.add_argument("-o", "--output", help="file to create (mode 0600)")
    name_import = command(name, "import", run_name_import, "import a name key from a file or stdin")
    name_import.add_argument("alias")
    name_import.add_argument("file", nargs="?", default="-", help="key file (default: stdin)")
    command(name, "list", run_name_list, "list this node's names")
    publish = command(name, "publish", run_name_publish, "point a name at a CID")
    publish.add_argument("alias")
    publish.add_argument("target", metavar="CID[/PATH]")
    resolve = command(name, "resolve", run_name_resolve, "print the CID a name points at")
    resolve.add_argument("name")

    export = command(commands, "export", run_export, "write an archive of a target")
    export.add_argument("target", metavar="CID[/PATH]")
    export.add_argument("-o", "--output", required=True, help="archive file to write")
    import_ = command(commands, "import", run_import, "import an archive, pinning its roots")
    import_.add_argument("file")

    command(commands, "status", run_status, "node ID, known nodes, records held, storage use")
    return parser


def main(argv: list[str] | None = None):
    """Parse the command line and run the chosen command."""
    args = build_parser().parse_args(argv)
    args.run(args)


if __name__ == "__main__":
    main()
