"""A minimal NomadNet node over real RNS, for tests/test_nomadnet_e2e.py: run as its own process,
`python nomadnet_node.py <rns config dir> <tcp port> <pages dir>`.

It answers as NomadNet's Node.py (1.4) does, on a `nomadnetwork.node` destination listening on 127.0.0.1:<port>:
pages as bytes, with NomadNet's "Request Not Allowed" page for a visitor a `<page>.allowed` file (identity hashes
in hex, one per line, read at each request) doesn't name; files as `[open file, {"name": <bytes>}]`, which RNS
sends as a Resource with metadata, compressed (handlers registered with auto_compress=32_000_000, as NomadNet's) and
in several segments when over 1 MiB. Pages and files are those in <pages dir>: `<name>.mu` as /page/<name>.mu,
anything else as /file/<name>, plus one hostile handler, /file/nested, whose metadata is a deeply nested list.

Prints its destination hash on the first line of stdout, announces every few seconds, and runs until stdin closes.
Nothing here imports rnfs.
"""
import os
import sys
import threading
import time

import RNS

CONFIG = """[reticulum]
  enable_transport = No
  share_instance = No
  panic_on_interface_error = No
[logging]
  loglevel = 2
[interfaces]
  [[TCP Server]]
    type = TCPServerInterface
    enabled = yes
    listen_ip = 127.0.0.1
    listen_port = {port}
"""

# NomadNet's Node.py, DEFAULT_NOTALLOWED
NOT_ALLOWED = '''>Request Not Allowed

You are not authorised to carry out the request.
'''


def allowed(path: str, remote_identity) -> bool:
    """NomadNet's request_allowed: no .allowed file, or one naming remote_identity's hash."""
    list_path = path + ".allowed"
    if not os.path.isfile(list_path):
        return True
    with open(list_path, "rb") as f:
        hashes = [line.strip() for line in f.read().splitlines()]
    return remote_identity is not None and remote_identity.hash.hex().encode() in hashes


def main():
    """Serve until stdin closes."""
    config_dir, port, pages = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    os.makedirs(config_dir, exist_ok=True)
    with open(os.path.join(config_dir, "config"), "w", encoding="utf-8") as f:
        f.write(CONFIG.format(port=port))
    RNS.Reticulum(configdir=config_dir)
    destination = RNS.Destination(RNS.Identity(), RNS.Destination.IN, RNS.Destination.SINGLE, "nomadnetwork", "node")

    def serve_page(path, data, request_id, link_id, remote_identity, requested_at):
        """A page's bytes, or NomadNet's refusal page."""
        file_path = os.path.join(pages, path.replace("/page/", "", 1))
        if not allowed(file_path, remote_identity):
            return NOT_ALLOWED.encode("utf-8")
        with open(file_path, "rb") as f:
            return f.read()

    def serve_file(path, data, request_id, remote_identity, requested_at):
        """A file with its name as metadata, as NomadNet sends it."""
        file_path = os.path.join(pages, path.replace("/file/", "", 1))
        if not allowed(file_path, remote_identity):
            return NOT_ALLOWED.encode("utf-8")
        return [open(file_path, "rb"), {"name": os.path.basename(file_path).encode("utf-8")}]

    def serve_nested(path, data, request_id, remote_identity, requested_at):
        """Hostile: metadata nested 200 lists deep, which RNS would decode before anyone could look at it."""
        nested = []
        for _ in range(200):
            nested = [nested]
        return [open(os.path.join(pages, "index.mu"), "rb"), {"name": nested}]

    for name in sorted(os.listdir(pages)):
        if name.endswith(".allowed"):
            continue
        if name.endswith(".mu"):
            destination.register_request_handler("/page/" + name, response_generator=serve_page,
                                                 allow=RNS.Destination.ALLOW_ALL)
        else:
            destination.register_request_handler("/file/" + name, response_generator=serve_file,
                                                 allow=RNS.Destination.ALLOW_ALL, auto_compress=32_000_000)
    destination.register_request_handler("/file/nested", response_generator=serve_nested,
                                         allow=RNS.Destination.ALLOW_ALL, auto_compress=32_000_000)
    print(destination.hash.hex(), flush=True)

    def announce():
        """Announce every few seconds, so the rnfs node finds a path whenever it asks."""
        while True:
            destination.announce(app_data=b"fake NomadNet node")
            time.sleep(3)

    threading.Thread(target=announce, daemon=True).start()
    sys.stdin.read()
    RNS.exit()


if __name__ == "__main__":
    main()
