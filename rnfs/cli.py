"""rnfs command line (specification section 11).

`daemon` runs a node with its API and gateway, holding a lock on <rnfs dir>/daemon.lock from before it touches
anything there, so a second daemon on the same rnfs dir stops at once, and describes itself in
<rnfs dir>/daemon.json (its PID, ports, version, start time and Reticulum config) while it runs, so clients find
it whatever rnfs.toml holds; every other command but `url` (which only prints a gateway site's URL) is one request
to a running daemon's local API (see rnfs/api.py). Client commands never import RNS, so they start quickly; paths
given to them are resolved to absolute paths here, since the daemon reads and writes them itself.

Client commands talk to the daemon through rnfs.apiclient, the library companion apps use too: it reads the token
the daemon writes to <rnfs dir>/api_token at each start (specification section 11), so only users who can read
the rnfs directory can use the daemon, and sends it only once the program on the API port has proved it knows it
(api_proof), since while the daemon is down any local user could listen there. Any error it raises exits with its
message.
"""
import argparse
import errno
import http.client
import json
import os
import stat
import sys
import time

from rnfs import __version__, apiclient, fsutil
from rnfs.apiclient import CHALLENGE_HEADER, PROOF_HEADER, TOKEN_FILE, api_proof, token_path  # noqa: F401
from rnfs.apiclient import daemon_info_path
from rnfs.config import DEFAULT_API_PORT, LOCK_FILE, Config

DEFAULT_DIR = apiclient.DEFAULT_DIR
COPY_SIZE = apiclient.COPY_SIZE


# text with control characters escaped, so names from other people's DAGs can't drive the terminal
printable = fsutil.printable


def warn(message: str):
    """Print a warning to stderr."""
    print(f"rnfs: warning: {printable(message)}", file=sys.stderr)


def fail(message: str):
    """Exit with status 1 and an error message on stderr."""
    sys.exit(f"rnfs: {printable(message)}")


def write_token(path: str, token: str):
    """Write the API token to path atomically, created with mode 0600 so other users can't read it (on Windows,
    with an access list for this user and SYSTEM only)."""
    # A new file (O_EXCL, O_NOFOLLOW) in the same directory, then a rename: the token is never readable with
    # wider permissions, nor half-written, and an existing file or symlink at path is replaced, not written
    # through. Ended with os.linesep, as the text-mode write before 0.1.2 did, so the file is unchanged
    fsutil.write_atomic(path, (token + os.linesep).encode("ascii"), mode=0o600, private=True)


def remove_token(path: str, token: str):
    """Delete the token file at path if it still holds token (another daemon may have replaced it)."""
    try:
        with open(path, encoding="ascii", errors="replace") as f:
            if f.read().strip() != token:
                return
        fsutil.remove(path)  # on Windows, one a client still holds open is left, and replaced at the next start
    except OSError:
        pass  # already gone, or unreadable: nothing of ours to clean up


def write_daemon_info(path: str, info: dict):
    """Write the running daemon's description (apiclient.DAEMON_FILE) to path atomically, owner-only like the
    token beside it."""
    fsutil.write_atomic(path, (json.dumps(info, indent=2) + "\n").encode("utf-8"), mode=0o600, private=True)


def remove_daemon_info(path: str, pid: int):
    """Delete the daemon description at path if it is still this process's (pid): another daemon may have
    replaced it."""
    try:
        with open(path, encoding="utf-8") as f:
            if json.load(f).get("pid") != pid:
                return
        fsutil.remove(path)
    except (OSError, ValueError, AttributeError):
        pass  # already gone, or not ours to clean up


def read_token(path: str) -> str:
    """The daemon's API token from path (apiclient.read_token); exits with an explanation if it can't be used."""
    try:
        return apiclient.read_token(path)
    except apiclient.ApiError as e:
        fail(str(e))


class ApiClient(apiclient.ApiConnection):
    """The library's client for the CLI: talks to the daemon's API on 127.0.0.1:port, exiting with a friendly
    message on any error.

    token_file is read on the first request: every request carries the token but the proof check's status
    request (status without the token says only the daemon's version, for health checks).
    """

    def __init__(self, port: int, token_file: str):
        """A client of the daemon on port whose token is in token_file."""
        super().__init__(port=port, token_file=token_file)

    def request(self, *args, **kwargs) -> http.client.HTTPResponse:
        """ApiConnection.request (the successful response), exiting on errors."""
        try:
            return super().request(*args, **kwargs)
        except apiclient.ApiError as e:
            fail(str(e))

    def call(self, *args, **kwargs):
        """ApiConnection.call (the decoded JSON response), exiting on errors."""
        try:
            return super().call(*args, **kwargs)
        except apiclient.ApiError as e:
            fail(str(e))


def read_all(response: http.client.HTTPResponse) -> bytes:
    """A response's whole body, failing if it is shorter than its Content-Length (the daemon hit an error)."""
    try:
        return apiclient.read_all(response)
    except apiclient.ApiError as e:
        fail(str(e))


def copy_response(stream: apiclient.Stream, out, progress=None):
    """Copy a streamed response body to out, failing if it ends before its Content-Length.

    progress (jobs.StreamProgress) is called with the bytes copied so far and Content-Length, and closed at the end.
    """
    expected = stream.response.getheader("Content-Length")
    error = None
    while True:
        try:
            chunk = stream.read(COPY_SIZE)
        except apiclient.ApiError as e:
            error = e
            break
        if not chunk:
            break
        out.write(chunk)
        if progress is not None:
            progress(stream.received, expected)
    if progress is not None:
        progress.close()
    out.flush()
    if error is not None:
        fail(str(error))


def client(args) -> ApiClient:
    """An ApiClient for --api-port, or the port in the running daemon's <dir>/daemon.json, or the api_port in
    <dir>/rnfs.toml, or the default port; token from <dir>."""
    port = args.api_port
    if port is None:
        running = apiclient.read_daemon_info(args.dir)
        if running is not None:
            return ApiClient(running["api_port"], token_path(args.dir))
        try:
            config = apiclient.load_settings(args.dir)
        except apiclient.ApiError as e:
            fail(str(e))
        for warning in config.warnings:
            warn(warning)
        port = config.api_port
    return ApiClient(port, token_path(args.dir))


def port_number(text: str, minimum: int = 1) -> int:
    """A TCP port from the command line (argparse type): 1-65535, or from minimum (0: the OS picks one)."""
    try:
        port = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid port {text!r}") from None
    if not minimum <= port <= 65535:
        raise argparse.ArgumentTypeError(f"invalid port {text!r}: must be {minimum}-65535")
    return port


def positive_seconds(text: str) -> float:
    """A number of seconds above 0 from the command line (argparse type)."""
    try:
        value = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid number of seconds {text!r}") from None
    if not 0 < value < float("inf"):
        raise argparse.ArgumentTypeError(f"invalid number of seconds {text!r}: must be above 0")
    return value


def absolute(path: str) -> str:
    """path made absolute here, since the daemon may run in another directory."""
    return os.path.abspath(os.path.expanduser(path))


def print_warnings(result: dict):
    """Print the warnings a daemon response carries."""
    for warning in result.get("warnings") or []:
        warn(warning)


# Daemon


def wait_until_stopping(stopping, interval: float = 0.0, task=None):
    """Return once stopping is set (by a signal handler), calling task every interval seconds meanwhile, if given.

    Python runs signal handlers in the main thread. On POSIX a signal interrupts the wait, so this sleeps until the
    task is due, or without one until stopped: an idle daemon's main thread doesn't wake. On Windows a wait isn't
    interrupted for them, so it wakes each second there."""
    due = time.monotonic() + interval if task is not None else None
    while True:
        timeout = None if due is None else max(0.0, due - time.monotonic())
        if fsutil.IS_WINDOWS:
            timeout = 1.0 if timeout is None else min(timeout, 1.0)
        if stopping.wait(timeout):
            return
        if due is not None and time.monotonic() >= due:
            due = time.monotonic() + interval
            task()


def run_daemon(args):
    """Run a node with its API and gateway until SIGINT, SIGTERM or SIGBREAK, then shut everything down cleanly."""
    # Imported here so client commands don't pay for loading RNS
    import faulthandler
    import signal
    import threading
    import traceback

    import RNS

    from rnfs import debuglog
    from rnfs.api import ApiServer
    from rnfs.errors import RnfsError
    from rnfs.gateway import GatewayServer
    from rnfs.log import LOG_ERROR, LOG_NOTICE
    from rnfs.node import DirInUse, RnfsNode, lock_rnfs_dir, make_private_root

    # The rnfs dir is made private first, then the settings in it are checked and read, then its subdirectories
    # are made private and it is locked (lock_rnfs_dir): nobody else can change what each step relies on
    root = os.path.expanduser(args.dir)
    try:
        make_private_root(root)
    except (RnfsError, OSError) as e:
        fail(f"can't use the rnfs dir {root}: {e}")
    try:
        config = Config.load(args.dir, reticulum_dir=args.rnsconfig,
                             api_port=args.api_port, gateway_port=args.gateway_port,
                             bootstrap_nodes=args.bootstrap, verbosity=args.verbose or None,
                             dht_mode=args.dht_mode, slow=args.slow, private=args.private,
                             provide_strategy=args.provide_strategy,
                             gateway_scripts=False if args.no_gateway_scripts else None,
                             debug_log=args.debug_log)
    except (ValueError, TypeError, OSError) as e:
        fail(f"invalid settings: {e}")
    for warning in config.warnings:
        warn(warning)

    # Before anything uses the rnfs dir (made private first, as the node would make it): a second daemon on it
    # would load and save the same state, announce the same node ID and rotate the first one's debug log
    lock_path = os.path.join(config.root, LOCK_FILE)
    try:
        lock = lock_rnfs_dir(config)  # held until the process exits
    except DirInUse as e:
        fail(f"another rnfs daemon{f' (process {e.holder})' if e.holder else ''} is using {config.root}; stop it "
             "first, or give this one another --dir")
    except (RnfsError, OSError) as e:
        fail(f"can't lock {lock_path}: {e}")

    if config.debug_log:
        # Before the node, so it has Reticulum starting (interfaces connecting or not)
        try:
            debug = debuglog.start(config)
        except (RnfsError, OSError) as e:
            fsutil.unlock(lock)  # fail() raises SystemExit, which an in-process caller (a test) may catch
            fail(f"can't write the debug log: {e}")
        print(f"rnfs: writing the debug log {debug.path}", flush=True)

    try:
        node = RnfsNode(config)
    except (RnfsError, OSError) as e:  # e.g. an rnfs dir that isn't private, or an unreadable state file
        if debuglog.current() is not None:  # inside the except: with its traceback
            debuglog.current().write(LOG_ERROR, "rnfs", f"couldn't start the node: {e!r}", traceback.format_exc())
        if RNS.Reticulum.get_instance() is None:
            fsutil.unlock(lock)  # as above
            fail(str(e))
        print(f"rnfs: {printable(str(e))}", file=sys.stderr, flush=True)
        RNS.exit(1)  # Reticulum has started: ends with os._exit, so its threads don't keep the process
    servers = []
    # Set by a signal (below) or POST /api/shutdown (Windows has no clean way to stop a background daemon otherwise)
    stopping = threading.Event()

    def abort(message: str):
        """Shut down what has started and exit with status 1 and message."""
        for server in servers:
            server.stop()
        node.stop()
        print(f"rnfs: {printable(message)}", file=sys.stderr, flush=True)
        if debuglog.current() is not None:
            debuglog.current().note(f"stopped: {message}", LOG_ERROR)
        RNS.exit(1)  # ends with os._exit, so the message is printed first

    try:
        servers.append(ApiServer(node, config.api_port, shutdown=stopping.set))
        servers.append(GatewayServer(node, config, config.gateway_port))
    except (OSError, OverflowError) as e:  # OverflowError: a port over 65535, which validate() should have refused
        abort(f"can't listen on 127.0.0.1: {e}. Is another daemon running? (--api-port, --gateway-port)")
    api, gateway = servers
    # A new token each start, so one copied while an old daemon ran is useless now
    token_file = token_path(config.root)
    try:
        write_token(token_file, api.token)
    except OSError as e:
        abort(f"can't write the API token {token_file}: {e}")
    for server in servers:
        server.start()
    # After the token and the servers: a client finding it finds a daemon to talk to
    info_file = daemon_info_path(config.root)
    try:
        write_daemon_info(info_file, {
            "pid": os.getpid(), "api_port": api.port, "gateway_port": gateway.port, "version": __version__,
            "started": time.time(), "rnfs_dir": config.root, "reticulum_config": RNS.Reticulum.configdir,
            "shared_instance": bool(node.reticulum.is_connected_to_shared_instance), "node_id": node.node_id.hex()})
    except OSError as e:
        abort(f"can't write {info_file}: {e}")
    print(f"rnfs node {node.node_id.hex()}, API on http://127.0.0.1:{api.port}, "
          f"gateway on http://127.0.0.1:{gateway.port}", flush=True)
    debug = debuglog.current()
    if debug is not None:
        debug.note(f"started: API on port {api.port}, gateway on port {gateway.port}", LOG_NOTICE)
        debug.note(debuglog.status_line(node), LOG_NOTICE)
    # `kill -USR1 <pid>` prints every thread's stack, for debugging hangs. Windows has no SIGUSR1
    if hasattr(signal, "SIGUSR1"):
        faulthandler.register(signal.SIGUSR1, all_threads=True)

    # Replace RNS's handlers, which exit immediately, so the node shuts down first. On Windows, Ctrl+C is
    # SIGINT and Ctrl+Break (or CTRL_BREAK_EVENT sent to the daemon's process group) is SIGBREAK, whose
    # default would end the process at once; SIGTERM never comes from another process there
    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), lambda *_: stopping.set())

    def status():
        """Write the debug log's periodic status line."""
        try:
            debug.note(debuglog.status_line(node), LOG_NOTICE)
        except Exception as e:  # noqa: BLE001 - a status line mustn't stop the daemon
            debug.note(f"status line failed: {e!r}", LOG_ERROR)

    wait_until_stopping(stopping, debuglog.STATUS_INTERVAL, status if debug is not None else None)

    for server in servers:
        server.stop()
    remove_daemon_info(info_file, os.getpid())
    remove_token(token_file, api.token)
    node.stop()
    if debug is not None:
        debug.note("stopped", LOG_NOTICE)
        debuglog.stop()
    # RNS.exit() ends with os._exit, which would lose log lines still buffered when stdout is a file
    sys.stdout.flush()
    sys.stderr.flush()
    RNS.exit()  # saves Reticulum's path table and known destinations


# Content


def add_options(args, query: bool = False) -> dict:
    """add's --no-pin, --to, --to-self, --no-sealed-name and --chunker as API parameters (only those given);
    query: for the stream endpoint's query string."""
    options: dict = {}
    if getattr(args, "no_pin", False):
        options["pin"] = 0 if query else False
    if getattr(args, "to", None):
        options["to"] = ",".join(args.to) if query else args.to
    if getattr(args, "to_self", False):
        options["to_self"] = 1 if query else True
    if getattr(args, "chunker", None):
        options["chunker"] = args.chunker
    if options.get("to") or options.get("to_self"):
        if query and args.name:
            options["seal_name"] = args.name
        elif not query and getattr(args, "no_sealed_name", False):
            options["seal_name"] = False
    return options


def wants_sealed_name(args) -> bool:
    """True if get, without -o, should write a sealed file it opens under its sealed name: a bare CID's
    output name would be the CID, while a path's last name is already a name."""
    return not args.output and not getattr(args, "raw", False) and "/" not in args.target.strip("/")


def sealed_output(args, output: str) -> str:
    """get's default output for a sealed file this node can open: its sealed name, if it has one.

    Only for a daemon without the job manager: a get job picks the sealed name itself (sealed_name), which
    doesn't cost a separate fetch of the root before the job, ignoring --from.
    """
    if not wants_sealed_name(args):
        return output
    info = client(args).get("/api/info", target=args.target, **{"from": getattr(args, "from_node", None)})
    name = info.get("name") if info.get("sealed") else None
    # The sender chose the name: no dotfiles, control characters or path tricks in the current directory
    return os.path.join(os.path.dirname(output), fsutil.safe_default_name(name, os.path.basename(output)))


def print_added(result: dict):
    """Print an add's warnings (on stderr) and CID, as `rnfs add` does (an add job's too)."""
    print_warnings(result)
    print(printable(result["cid"]))


def print_imported(result: dict):
    """Print an import's roots, and warn of those it didn't pin, as `rnfs import` does (an import job's too)."""
    for root in result["roots"]:
        print(printable(root))
    for root in result.get("incomplete") or []:
        warn(f"{root} is incomplete in the archive, so it wasn't pinned")


def run_add(args):
    """Add a file, directory or stdin and print its CID; with --background, start an add job and print its ID.

    In the foreground an add stays one request, not a job: a job would wait behind max_jobs transfers (one on slow
    nodes), and a local add mustn't queue behind an hour-long LoRa download."""
    if getattr(args, "background", False):
        if args.path == "-":
            fail("--background adds a path; stdin is added in the foreground")
        if args.name:
            fail("--name is only for stdin (add -)")
        path = absolute(args.path)
        if not os.path.lexists(path):
            fail(f"no such file or directory: {args.path}")
        from rnfs.features import jobs
        fields = {"kind": "add", "path": path, **{name: True for name in ("wrap", "only_hash") if getattr(args, name)},
                  **add_options(args)}
        jobs.run_transfer(args, fields, None)  # prints the job's ID (and fails on a daemon without jobs)
        return
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
        options = add_options(args, query=True)
        # Sealing stdin: --name is sealed inside, and is a visible wrapper entry too only with --wrap
        name = args.name if args.wrap or not (options.get("to") or options.get("to_self")) else None
        result = api.call("POST", "/api/add/stream", params={"name": name, "only_hash": int(args.only_hash), **options},
                          body=stream, length=length)
    else:
        if args.name:
            fail("--name is only for stdin (add -)")
        path = absolute(args.path)
        if not os.path.lexists(path):
            fail(f"no such file or directory: {args.path}")
        result = api.post("/api/add", path=path, wrap=args.wrap, only_hash=args.only_hash, **add_options(args))
    print_added(result)


def default_output(target: str) -> str:
    """Where `get` writes without -o: the target's last path name, else its CID, in the current directory."""
    root = target.split("/", 1)[0]
    # The last name is whoever made the DAG's choice: falls back to the CID if it is hidden (.bash_login),
    # unprintable (terminal escapes) or not one plain path component here (fsutil.safe_default_name)
    return fsutil.safe_default_name(target.rstrip("/").rsplit("/", 1)[-1], root)


def run_get(args):
    """Fetch a file or directory and have the daemon write it out, as a job (rnfs/features/jobs.py)."""
    output = absolute(args.output or default_output(args.target))
    if os.path.lexists(output):
        fail(f"{output} already exists; remove it or choose another with -o")
    from rnfs.features.contacts import node_arg  # a node contact's nick works for --from
    args.from_node = node_arg(args, args.from_node)
    if getattr(args, "raw", False):  # a sealed file's ciphertext: one request (jobs decrypt as plain get does)
        result = client(args).post("/api/get", target=args.target, output=output, raw=True, **{"from": args.from_node})
        print_warnings(result)
        print(printable(output))
        return

    def without_jobs(api) -> dict:
        """The get in one request, for a daemon without the job manager (asking for a sealed name first)."""
        final = sealed_output(args, output)
        if final != output and os.path.lexists(final):
            fail(f"{final} already exists; remove it or choose another with -o")
        return {"output": final, **api.post("/api/get", target=args.target, output=final, **{"from": args.from_node})}

    from rnfs.features import jobs
    fields = {"kind": "get", "target": args.target, "output": output, "from": args.from_node}
    if wants_sealed_name(args):
        fields["sealed_name"] = True  # the job renames a sealed file it opens to its sealed name
    result = jobs.run_transfer(args, fields, without_jobs)
    if result is not None:  # None: started in the background, its job ID printed
        print_warnings(result)
        print(printable(result.get("output") or output))


def run_cat(args):
    """Write a file, or part of one, to stdout."""
    stream = client(args).cat(args.target, offset=args.offset, length=args.length, raw=getattr(args, "raw", False),
                              block=getattr(args, "block", False))
    progress = None
    if sys.stderr.isatty() and not sys.stdout.isatty():  # into a file or pipe, watched from a terminal
        from rnfs.features import jobs
        progress = jobs.StreamProgress(sys.stderr)
    try:
        copy_response(stream, sys.stdout.buffer, progress)
    except OSError as e:
        # Windows reports a pipe whose reader has gone as EINVAL rather than EPIPE
        if not (isinstance(e, BrokenPipeError) or (fsutil.IS_WINDOWS and e.errno == errno.EINVAL)):
            raise
        # The reader (e.g. `head`) has gone; exit quietly without a second error at interpreter shutdown
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        sys.exit(1)


def run_ls(args):
    """List a directory: kind, size, CID and name (last, since names can contain spaces)."""
    for entry in client(args).ls(args.target):
        name = printable(entry["name"]) + ("/" if entry["kind"] == "directory" else "")
        print(f"{printable(entry['kind']):<10} {printable(entry['size']):>12} {printable(entry['cid'])} {name}")


def print_fields(result: dict):
    """Print a JSON object as "name: value" lines."""
    for name, value in result.items():
        if isinstance(value, list):
            value = ", ".join(str(item) for item in value) if value else "none"
        print(f"{name}: {printable(value)}")


def run_stat(args):
    """Print a target's size, block count and local holdings."""
    print_fields(client(args).stat(args.target))


def site_url(target: str, port: int) -> str:
    """The gateway URL of target (<cid>[/path] or <name>[/path], a name as 32 hex characters) on its own site
    (specification section 11): http://<k-cid>.rnfs.localhost:<port>/<path> or
    http://<name>.name.localhost:<port>/<path>, each path name percent-encoded. Raises ValueError for anything
    else, contact nicks included: the gateway never expands them."""
    from urllib.parse import quote

    from rnfs import ids
    from rnfs.content.cid import Cid, to_base36
    root, _, path = target.partition("/")
    if ids.is_hex16(root):
        host = f"{ids.parse_hex16(root, 'name').hex()}.name.localhost"
    else:
        try:
            host = f"{to_base36(Cid.parse(root))}.rnfs.localhost"
        except ValueError:
            raise ValueError(f"{root!r} is neither a CID nor a name (32 hex characters)") from None
    names = path.split("/") if path else []
    if "" in names[:-1]:
        raise ValueError(f"empty name in path {path!r}")
    return f"http://{host}:{port}/" + "/".join(quote(name, safe="") for name in names)


def run_url(args):
    """Print a target's URL on its gateway site, for the gateway port of the running daemon or else rnfs.toml's."""
    running = apiclient.read_daemon_info(args.dir)
    if running is not None and type(running.get("gateway_port")) is int and running["gateway_port"]:
        port = running["gateway_port"]
    else:
        try:
            config = apiclient.load_settings(args.dir)
        except apiclient.ApiError as e:
            fail(str(e))
        port = config.gateway_port
        if not port:
            fail("gateway_port is 0 (any free port) and no daemon is running to say which it took")
    try:
        print(printable(site_url(args.target, port)))
    except ValueError as e:
        fail(str(e))


def run_dir_set(args):
    """Make a directory with an entry added or replaced; print its CID."""
    print(printable(client(args).post("/api/dir/set", target=args.target, name=args.name, cid=args.cid,
                                      executable=args.exec)["cid"]))


def run_dir_rm(args):
    """Make a directory with an entry removed; print its CID."""
    print(printable(client(args).post("/api/dir/rm", target=args.target)["cid"]))


# Pins


def run_pin_add(args):
    """Fetch and pin a target, as a job; print its CID once complete."""
    from rnfs.features import jobs
    result = jobs.run_transfer(args, {"kind": "pin", "target": args.target},
                               lambda api: api.post("/api/pin/add", target=args.target))
    if result is not None:
        print(printable(result["cid"]))


def run_pin_rm(args):
    """Remove the user pin of a CID."""
    client(args).pin_rm(args.cid)


def display_name(name: str) -> str:
    """A pin's local name for printing: the home directory shortened to ~, non-printables escaped."""
    home = os.path.expanduser("~")
    if home not in ("", "~", os.sep) and (name == home or name.startswith(home + os.sep)):
        name = "~" + name[len(home):]
    return printable(name)


def run_pin_ls(args):
    """List pins with their sources and, when known, where each came from (or what it holds)."""
    pins = client(args).pin_ls(args.cid)
    if args.json:
        print(json.dumps(pins, indent=2))
        return
    for pin in pins:
        state = ("" if pin["complete"] else " (incomplete)") + (" (sealed)" if pin.get("sealed") else "")
        name = f"  {display_name(pin['name'])}" if pin.get("name") else ""
        print(f"{printable(pin['cid'])} {','.join(printable(s) for s in pin['sources'])}{state}{name}")


def run_pin_label(args):
    """Label a pin, or clear its label (no label given)."""
    pin = client(args).pin_label(args.cid, args.label)
    if pin.get("label"):
        print(printable(f"{pin['cid']}  {pin['label']}"))
    else:
        print(printable(f"{pin['cid']}: label cleared"))


def run_cache_ls(args):
    """List the cached fetches: downloads kept until evicted, not pinned."""
    from rnfs.features.peers import human_size  # no RNS there
    cache = client(args).cache_ls()
    if args.json:
        print(json.dumps(cache, indent=2))
        return
    for entry in cache:
        target = "/".join([entry["root"], *entry["path"]])
        state = ("" if entry["complete"] else " (incomplete)") + (" (pinned)" if entry["pinned"] else "")
        line = f"{printable(entry['cid'])} {human_size(entry.get('size'))}{state}"
        if target != entry["cid"]:
            line += f"  {printable(target)}"  # names from other people's DAGs: escaped
        print(line)


def run_cache_rm(args):
    """Evict cached fetches now, by CID."""
    api = client(args)
    for cid in args.cids:
        result = api.cache_rm(cid)
        print(printable(f"{cid}: removed {result['removed_blocks']} blocks, freed {result['freed_bytes']} bytes"))


def run_pin_remote(args):
    """Ask a node to pin or release a CID."""
    from rnfs.features.contacts import node_arg  # a node contact's nick works as a node ID
    args.node = node_arg(args, args.node)
    result = client(args).post("/api/pin/remote", node=args.node, cid=args.cid, pin=args.action == "add")
    if not result["accepted"]:
        fail(f"{args.node} refused the request")
    print("accepted")


def run_gc(args):
    """Evict every cached fetch."""
    result = client(args).post("/api/gc")
    print(printable(f"removed {result['removed_blocks']} blocks, freed {result['freed_bytes']} bytes"))


def run_verify(args):
    """Re-check every stored block; exits with status 1 if any were bad (they are deleted)."""
    result = client(args).post("/api/verify")
    print(printable(f"checked {result['checked']} blocks"))
    for cid in result["bad"]:
        print(f"bad block deleted: {printable(cid)}")
    if result["bad"]:
        sys.exit(1)


# Network


def run_providers(args):
    """Print the IDs of nodes providing a CID, one per line."""
    providers = client(args).get("/api/providers", cid=args.cid)["providers"]
    if not providers:
        fail("no providers found")
    print("\n".join(printable(node_id) for node_id in providers))


def run_ping(args):
    """Print the round-trip time to a node."""
    from rnfs.features.contacts import node_arg  # a node contact's nick works as a node ID
    args.node = node_arg(args, args.node)
    rtt = client(args).get("/api/ping", node=args.node)["rtt"]
    if rtt is None:
        fail(f"no answer from {args.node}")
    print(f"round-trip time {rtt * 1000:.0f} ms")


# Names


def run_name_create(args):
    """Create a name key; print the name, and on stderr how to back up its key."""
    result = client(args).name_create(args.alias)
    print(printable(result["name"]))
    if result.get("warning"):  # an older daemon gives none
        warn(result["warning"])


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
            fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | fsutil.O_BINARY, 0o600)
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
    print(printable(result["name"]))
    if result.get("warning"):
        warn(result["warning"])


def run_name_list(args):
    """List this node's names: alias, name, CID and sequence."""
    for entry in client(args).name_list():
        print("\t".join(printable(value) for value in (entry["alias"], entry["name"], entry.get("cid") or "-",
                                                          entry.get("sequence", 0))))


def run_name_publish(args):
    """Point a name at a target."""
    result = client(args).name_publish(args.alias, args.target)
    print(printable(f"published {result['cid']} as {result['name']} (sequence {result['sequence']})"))
    if result.get("warning"):  # private mode: the name's announce still reaches outsiders
        warn(result["warning"])


def run_name_resolve(args):
    """Print the CID a name points at."""
    print(printable(client(args).name_resolve(args.name)["cid"]))


# Archives and status


def run_export(args):
    """Write a target and its DAG to an archive file, as a job."""
    output = absolute(args.output)
    from rnfs.features import jobs
    result = jobs.run_transfer(args, {"kind": "export", "target": args.target, "output": output},
                               lambda api: api.post("/api/export", target=args.target, output=output))
    if result is not None:
        print(printable(f"exported {result['blocks']} blocks of {', '.join(result['roots'])} to {output}"))


def run_import(args):
    """Import an archive, pinning its complete roots; with --background, as an import job (one request otherwise,
    as run_add explains)."""
    path = absolute(args.file)
    if not os.path.isfile(path):
        fail(f"no such file: {args.file}")
    if getattr(args, "background", False):
        from rnfs.features import jobs
        jobs.run_transfer(args, {"kind": "import", "path": path}, None)  # prints the job's ID
        return
    print_imported(client(args).post("/api/import", path=path))


def run_stop(args):
    """Ask the daemon to stop, and wait (up to --wait seconds) until it has."""
    found = client(args)
    # The library's own connection: the CLI's would exit on the error that means the daemon has stopped
    api = apiclient.ApiConnection(args.dir, port=found.port, token_file=found.token_file)
    try:
        stopped = api.shutdown(wait=args.wait)
    except apiclient.NoSuchEndpoint:
        fail(f"the daemon (rnfs {api.daemon_version or '?'}) can't be stopped through its API; stop its process "
             f"instead")
    except apiclient.ApiError as e:
        fail(str(e))
    if not stopped:
        fail(f"the daemon hasn't stopped after {args.wait:g} s; it may still be saving its state")
    print("rnfs: the daemon stopped")


def run_status(args):
    """Print the daemon's status, one "name: value" per line. Without a readable API token (a health check by
    another user), only the version the daemon says without it."""
    api = client(args)
    if not os.access(api.token_file, os.R_OK):
        print_fields(api.call("GET", "/api/status", authenticated=False))
        warn(f"no readable API token at {api.token_file}: the daemon only says its version without it")
        return
    print_fields(api.status())


# Parser


def build_parser() -> argparse.ArgumentParser:
    """The argument parser for every command."""
    parser = argparse.ArgumentParser(prog="rnfs", description="Content-addressed file sharing over Reticulum")
    parser.add_argument("--version", action="version", version=f"rnfs {__version__}")
    parser.add_argument("--api-port", type=port_number, default=None,
                        help=f"daemon's API port (default {DEFAULT_API_PORT})")
    parser.add_argument("--dir", default=DEFAULT_DIR, help="rnfs directory (settings, identity, storage)")
    # Repeated on every subcommand so they can also go after the command name. SUPPRESS keeps a
    # subcommand's unset default from overwriting a value given before the command
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--api-port", type=port_number, default=argparse.SUPPRESS, help="daemon's API port")
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
    daemon.add_argument("--gateway-port", type=lambda text: port_number(text, minimum=0),
                        help="gateway port (default 8789; 0: any free port)")
    daemon.add_argument("--rnsconfig", metavar="DIR",
                        help="Reticulum config directory (default: RNS's own, the one rnsd uses)")
    daemon.add_argument("--dht-mode", choices=["server", "client"])
    daemon.add_argument("--slow", choices=["yes", "no", "auto"])
    daemon.add_argument("--private", action="store_true", default=None, help="private mode: members only")
    daemon.add_argument("--provide-strategy", choices=["pinned", "roots"])
    daemon.add_argument("--no-gateway-scripts", action="store_true", help="don't let gateway pages run scripts")
    daemon.add_argument("--debug-log", action="store_true", default=None,
                        help="also write every message, gateway request and RNS message to <rnfs dir>/debug.log")
    daemon.add_argument("-v", "--verbose", action="count", default=0,
                        help="log more detail: -v for every DHT query, -vv for RNS debug output")

    add = command(commands, "add", run_add, "add a file, directory or stdin (-), pin it and print its CID")
    add.add_argument("path", help="file or directory, or - for stdin")
    add.add_argument("--wrap", action="store_true", help="put it in a directory")
    add.add_argument("--only-hash", action="store_true", help="just print the CID; store nothing")
    add.add_argument("--name", help="with stdin: wrap it in a directory as this entry")
    add.add_argument("--background", action="store_true",
                     help="start an add job (a path only), print its ID and return (rnfs jobs)")
    # --- content: unpinned adds, sealed files, chunkers ---
    add.add_argument("--no-pin", action="store_true", help="keep it as evictable cache instead of pinning it")
    add.add_argument("--to", nargs="+", action="extend", metavar="RECIPIENT",
                     help="seal it so only these can read it: rnfs node IDs, LXMF addresses or identity hashes "
                          "RNS has heard, 128-hex public keys, or contact nicks")
    add.add_argument("--to-self", action="store_true", help="seal it for this node's identity too")
    add.add_argument("--no-sealed-name", action="store_true", help="with --to: don't seal the file name inside")
    add.add_argument("--chunker", choices=["fixed", "fastcdc"], help="fixed 64 KiB chunks (default) or FastCDC "
                     "content-defined chunks, which keep most chunks of an edited file unchanged")

    get = command(commands, "get", run_get, "fetch and write out a file or directory")
    get.add_argument("target", metavar="CID[/PATH]")
    get.add_argument("-o", "--output", help="where to write (default: the last path name or the CID, here)")
    get.add_argument("--from", dest="from_node", metavar="NODE_ID", help="fetch from this node, bypassing the DHT")
    get.add_argument("--background", action="store_true", help="start a job, print its ID and return (rnfs jobs)")
    get.add_argument("--raw", action="store_true", help="a sealed file as stored (its ciphertext), not decrypted")

    cat = command(commands, "cat", run_cat, "write a file, or part of one, to stdout")
    cat.add_argument("target", metavar="CID[/PATH]")
    cat.add_argument("--offset", type=int, default=None, help="first byte to write")
    cat.add_argument("--length", type=int, default=None, help="number of bytes to write")
    cat.add_argument("--raw", action="store_true", help="a sealed file as stored (its ciphertext), not decrypted")
    cat.add_argument("--block", action="store_true", help="the CID's own block (a bare CID), verified")

    ls = command(commands, "ls", run_ls, "list a directory: kind, size, CID, name")
    ls.add_argument("target", metavar="CID[/PATH]")

    stat_ = command(commands, "stat", run_stat, "size, block count, and how much is held locally")
    stat_.add_argument("target", metavar="CID[/PATH]")
    url = command(commands, "url", run_url, "print a target's gateway URL on its own site (<cid>.rnfs.localhost)")
    url.add_argument("target", metavar="CID|NAME[/PATH]")

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
    pin_add.add_argument("--background", action="store_true", help="start a job, print its ID and return (rnfs jobs)")
    pin_rm = command(pin, "rm", run_pin_rm, "remove the user pin")
    pin_rm.add_argument("cid")
    pin_ls = command(pin, "ls", run_pin_ls, "list pins, their sources and where each came from")
    pin_ls.add_argument("cid", nargs="?")
    pin_ls.add_argument("--json", action="store_true", help="print the full listing as JSON")
    pin_label = command(pin, "label", run_pin_label, "give a pin a label (none given: clear it)")
    pin_label.add_argument("cid")
    pin_label.add_argument("label", nargs="?", default=None)
    remote = pin.add_parser("remote", help="remote pinning").add_subparsers(dest="action", required=True,
                                                                            metavar="command")
    for action in ("add", "rm"):
        sub = command(remote, action, run_pin_remote, f"ask a node to {'pin' if action == 'add' else 'unpin'} a CID")
        sub.add_argument("node", metavar="NODE_ID")
        sub.add_argument("cid")

    command(commands, "gc", run_gc, "evict all cached fetches")
    cache = commands.add_parser("cache", help="cached fetches (downloads not pinned)").add_subparsers(
        dest="cache_command", required=True, metavar="command")
    cache_ls = command(cache, "ls", run_cache_ls, "list cached fetches, most recently used first")
    cache_ls.add_argument("--json", action="store_true", help="print the full listing as JSON")
    cache_rm = command(cache, "rm", run_cache_rm, "evict cached fetches now")
    cache_rm.add_argument("cids", nargs="+", metavar="CID")
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
    resolve.add_argument("name", help="the name (32 hex characters) or a name contact's nick")

    export = command(commands, "export", run_export, "write an archive of a target")
    export.add_argument("target", metavar="CID[/PATH]")
    export.add_argument("-o", "--output", required=True, help="archive file to write")
    export.add_argument("--background", action="store_true", help="start a job, print its ID and return (rnfs jobs)")
    import_ = command(commands, "import", run_import, "import an archive, pinning its roots")
    import_.add_argument("file")
    import_.add_argument("--background", action="store_true",
                         help="start an import job, print its ID and return (rnfs jobs)")

    command(commands, "status", run_status, "node ID, known nodes, records held, storage use")
    stop = command(commands, "stop", run_stop, "stop the daemon (as Ctrl+C or SIGTERM would) and wait until it has")
    stop.add_argument("--wait", type=positive_seconds, default=30.0, metavar="SECONDS",
                      help="how long to wait for it to stop (default 30)")

    from rnfs import features
    for feature in features.feature_classes():
        feature.cli_commands(commands, command)
    return parser


def main(argv: list[str] | None = None):
    """Parse the command line and run the chosen command."""
    # Redirected output on Windows uses the ANSI code page, which can't encode every name: escape what it
    # can't rather than fail part-way through a listing
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="backslashreplace")
    args = build_parser().parse_args(argv)
    args.run(args)


if __name__ == "__main__":
    main()
