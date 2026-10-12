"""Tests for the local JSON API (rnfs/api.py), run against an in-memory FakeNode with no RNS.

FakeNode is shared with test_gateway.py and test_cli.py.
"""
import hashlib
import http.client
import json
import os
import socket
import tempfile
import threading
import time
import unittest
from unittest import mock

from rnfs import cli, fsutil
from rnfs.api import ApiHandler, ApiServer, BodyReader, LocalServer, loggable
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.errors import Busy, InvalidDag, NotFound, RnfsError, StorageFull, TooLarge

LocalServer.poll_interval = 0.01  # so each test's server stops at once


class FakeNode:
    """The RnfsNode facade (design.md) in memory: files, directories, pins and names, no network.

    CIDs of files are real raw CIDs; directories get a DAG CID over a JSON listing. CIDs in `remote` count as
    not stored: touching them is a "network fetch", which blocks while `gate` is an unset Event, so tests
    can hold fetches open. Every facade call is recorded in `calls` as (method, args, kwargs).
    """

    def __init__(self):
        self.node_id = bytes(range(16))
        self.objects: dict[str, tuple] = {}  # cid → ("file", bytes, executable) or ("dir", {name: cid})
        self.remote: set[str] = set()
        self.gate: threading.Event | None = None
        self.fetching = 0
        self.lock = threading.Lock()
        self.calls: list[tuple] = []
        self.pins: dict[str, set[str]] = {}
        self.keys: dict[str, bytes] = {}  # alias → key
        self.records: dict[str, dict] = {}  # name → {"cid", "sequence"}
        self.provider_map: dict[str, list[bytes]] = {}
        self.piece = 7  # read() yields pieces this big, to exercise streaming

    def record(self, method, *args, **kwargs):
        """Remember a facade call."""
        with self.lock:
            self.calls.append((method, args, kwargs))

    def called(self, method) -> list[tuple]:
        """The (args, kwargs) of every call to method."""
        return [(args, kwargs) for name, args, kwargs in self.calls if name == method]

    # Building content

    def put_file(self, data: bytes, executable=False, remote=False) -> str:
        """Store a file; returns its CID."""
        cid = str(Cid.for_block(RAW, data))
        self.objects[cid] = ("file", data, executable)
        if remote:
            self.remote.add(cid)
        return cid

    def put_dir(self, entries: dict[str, str], remote=False) -> str:
        """Store a directory of name → CID; returns its CID."""
        block = json.dumps(sorted(entries.items())).encode()
        cid = str(Cid.for_block(DAG, block))
        self.objects[cid] = ("dir", dict(entries))
        if remote:
            self.remote.add(cid)
        return cid

    def _fetch(self, cid: str):
        """Simulate fetching cid if it isn't stored: wait for the gate, then it is cached."""
        if cid not in self.remote:
            return
        with self.lock:
            self.fetching += 1
        try:
            if self.gate is not None:
                self.gate.wait(10)
        finally:
            with self.lock:
                self.fetching -= 1
            self.remote.discard(cid)

    def _object(self, cid: str):
        """The object for cid, fetched if remote. NotFound if unknown."""
        if cid not in self.objects:
            raise NotFound(f"no provider found for {cid}")
        self._fetch(cid)
        return self.objects[cid]

    def _path(self, target: str) -> list[str]:
        """The CIDs from the root to the target, without fetching."""
        if not isinstance(target, str) or not target:
            raise ValueError("bad target")
        parts = target.split("/")
        root = str(Cid.parse(parts[0]))
        path = [root]
        for name in parts[1:]:
            if not name:
                raise ValueError("empty name in path")
            obj = self.objects.get(path[-1])
            if obj is None:
                raise NotFound(f"no provider found for {path[-1]}")
            if obj[0] != "dir":
                raise NotFound(f"{name}: not a directory")
            if name not in obj[1]:
                raise NotFound(f"no such entry: {name}")
            path.append(obj[1][name])
        return path

    def _resolve(self, target: str) -> str:
        """The target's CID, fetching the path's nodes."""
        path = self._path(target)
        for cid in path:
            self._object(cid)
        return path[-1]

    def _size(self, cid: str) -> int:
        obj = self.objects[cid]
        return len(obj[1]) if obj[0] == "file" else sum(self._size(child) for child in obj[1].values())

    def _dag(self, cid: str) -> set[str]:
        obj = self.objects.get(cid)
        if obj is None or obj[0] == "file":
            return {cid}
        return {cid}.union(*(self._dag(child) for child in obj[1].values()))

    def _kind(self, cid: str) -> str:
        obj = self.objects[cid]
        if obj[0] == "dir":
            return "directory"
        return "executable" if obj[2] else "file"

    def _add_tree(self, path: str) -> str:
        if os.path.isdir(path):
            return self.put_dir({name: self._add_tree(os.path.join(path, name)) for name in os.listdir(path)})
        with open(path, "rb") as f:
            return self.put_file(f.read(), executable=os.name != "nt" and os.access(path, os.X_OK))  # as add does

    # Facade

    def add_path(self, path, wrap=False, only_hash=False):
        self.record("add_path", path, wrap=wrap, only_hash=only_hash)
        if not os.path.isabs(path):
            raise ValueError("path must be absolute")
        if not os.path.exists(path):
            raise NotFound(f"no such file: {path}")
        cid = self._add_tree(path)
        if wrap:
            cid = self.put_dir({os.path.basename(path): cid})
        if not only_hash:
            self.pins.setdefault(cid, set()).add("user")
        return {"cid": cid, "size": self._size(cid), "warnings": ["skipped a symlink"] if wrap else []}

    def add_stream(self, stream, size=None, name=None, only_hash=False):
        data = stream.read()
        self.record("add_stream", data, size=size, name=name, only_hash=only_hash)
        if size is not None and len(data) != size:
            raise ValueError("stream shorter than its size")
        cid = self.put_file(data)
        if name:
            cid = self.put_dir({name: cid})
        if not only_hash:
            self.pins.setdefault(cid, set()).add("user")
        return {"cid": cid, "size": len(data), "warnings": []}

    def get(self, target, output, from_node=None):
        self.record("get", target, output, from_node=from_node)
        cid = self._resolve(target)

        def write(cid, dest):
            obj = self._object(cid)
            if obj[0] == "file":
                with open(dest, "wb") as f:
                    f.write(obj[1])
            else:
                os.mkdir(dest)
                for name, child in obj[1].items():
                    write(child, os.path.join(dest, name))
        write(cid, output)
        return {"cid": cid, "warnings": []}

    def info(self, target, for_gateway=False, from_node=None):
        self.record("info", target, for_gateway=for_gateway, from_node=from_node)
        cid = self._resolve(target)
        obj = self.objects[cid]
        return {"cid": cid, "type": "file" if obj[0] == "file" else "directory", "size": self._size(cid),
                "executable": obj[0] == "file" and obj[2]}

    def read(self, target, offset=0, length=None, for_gateway=False, from_node=None):
        self.record("read", target, offset=offset, length=length, for_gateway=for_gateway, from_node=from_node)
        cid = self._resolve(target)
        obj = self.objects[cid]
        if obj[0] != "file":
            raise ValueError("not a file")
        data = obj[1][offset:] if length is None else obj[1][offset:offset + length]

        def pieces():
            for i in range(0, len(data), self.piece):
                yield data[i:i + self.piece]
        return pieces()

    def ls(self, target, for_gateway=False):
        self.record("ls", target, for_gateway=for_gateway)
        cid = self._resolve(target)
        obj = self.objects[cid]
        if obj[0] != "dir":
            raise ValueError("not a directory")
        return [{"name": name, "kind": self._kind(child), "size": self._size(child), "cid": child}
                for name, child in sorted(obj[1].items(), key=lambda item: item[0].encode())]

    def stat(self, target):
        self.record("stat", target)
        cid = self._resolve(target)
        blocks = self._dag(cid)
        return {"cid": cid, "size": self._size(cid), "blocks": len(blocks),
                "local_blocks": len(blocks - self.remote), "local_bytes": self._size(cid)}

    def is_local(self, target):
        self.record("is_local", target)
        path = self._path(target)
        return not (set(path) | self._dag(path[-1])) & self.remote

    def dir_set(self, target, name, child_cid, executable=False):
        self.record("dir_set", target, name, child_cid, executable=executable)
        obj = self._object(self._resolve(target))
        if obj[0] != "dir":
            raise ValueError("not a directory")
        cid = self.put_dir({**obj[1], name: child_cid})
        self.pins.setdefault(cid, set()).add("user")
        return {"cid": cid}

    def dir_rm(self, target):
        self.record("dir_rm", target)
        parent, name = target.rsplit("/", 1)
        obj = self._object(self._resolve(parent))
        if name not in obj[1]:
            raise NotFound(f"no such entry: {name}")
        cid = self.put_dir({k: v for k, v in obj[1].items() if k != name})
        self.pins.setdefault(cid, set()).add("user")
        return {"cid": cid}

    def pin_add(self, target):
        self.record("pin_add", target)
        cid = self._resolve(target)
        self.pins.setdefault(cid, set()).add("user")
        return {"cid": cid}

    def pin_rm(self, cid):
        self.record("pin_rm", cid)
        if "user" not in self.pins.get(cid, set()):
            raise NotFound(f"{cid} is not pinned")
        self.pins[cid].discard("user")
        if not self.pins[cid]:
            del self.pins[cid]

    def pin_ls(self, cid=None):
        self.record("pin_ls", cid)
        return [{"cid": c, "sources": sorted(s), "complete": c not in self.remote}
                for c, s in sorted(self.pins.items()) if cid is None or c == cid]

    def pin_remote(self, node_id, cid, pin):
        self.record("pin_remote", node_id, cid, pin)
        return node_id != b"\xff" * 16

    def gc(self):
        self.record("gc")
        return {"removed_blocks": 3, "freed_bytes": 300}

    def verify(self):
        self.record("verify")
        return {"checked": len(self.objects), "bad": []}

    def providers(self, cid):
        self.record("providers", cid)
        return self.provider_map.get(cid, [])

    def ping(self, node_id):
        self.record("ping", node_id)
        return None if node_id == b"\xff" * 16 else 0.25

    def _name_of(self, key: bytes) -> str:
        return hashlib.sha256(key).digest()[:16].hex()

    def name_create(self, alias):
        self.record("name_create", alias)
        if alias in self.keys:
            raise ValueError(f"alias {alias} exists")
        self.keys[alias] = os.urandom(64)
        return {"alias": alias, "name": self._name_of(self.keys[alias])}

    def name_import(self, alias, key_bytes):
        self.record("name_import", alias, key_bytes)
        if len(key_bytes) != 64:
            raise ValueError("not a name key")
        self.keys[alias] = bytes(key_bytes)
        return {"alias": alias, "name": self._name_of(key_bytes)}

    def name_rm(self, alias):
        self.record("name_rm", alias)
        if alias not in self.keys:
            raise NotFound(f"no name {alias}")
        del self.keys[alias]

    def name_export(self, alias):
        self.record("name_export", alias)
        if alias not in self.keys:
            raise NotFound(f"no name {alias}")
        return self.keys[alias]

    def name_list(self):
        self.record("name_list")
        result = []
        for alias, key in sorted(self.keys.items()):
            record = self.records.get(self._name_of(key), {})
            result.append({"alias": alias, "name": self._name_of(key), "cid": record.get("cid"),
                           "sequence": record.get("sequence", 0)})
        return result

    def name_publish(self, alias, target):
        self.record("name_publish", alias, target)
        if alias not in self.keys:
            raise NotFound(f"no name {alias}")
        name = self._name_of(self.keys[alias])
        sequence = self.records.get(name, {}).get("sequence", 0) + 1
        self.records[name] = {"cid": self._resolve(target), "sequence": sequence}
        return {"name": name, **self.records[name]}

    def name_resolve(self, name, for_gateway=False):
        self.record("name_resolve", name, for_gateway=for_gateway)
        if name not in self.records:
            raise NotFound(f"name {name} not resolved")
        return {"name": name, **self.records[name], "age": 5.0}

    def export(self, target, output):
        self.record("export", target, output)
        cid = self._resolve(target)
        with open(output, "wb") as f:
            f.write(b"archive")
        return {"roots": [cid], "blocks": len(self._dag(cid))}

    def import_archive(self, path):
        self.record("import_archive", path)
        if not os.path.isfile(path):
            raise NotFound(f"no such file: {path}")
        return {"roots": [self.put_file(b"from archive")], "incomplete": []}

    def status(self):
        self.record("status")
        return {"node_id": self.node_id.hex(), "dht_mode": "server", "slow": False, "known_nodes": 4,
                "records": 2, "pins": len(self.pins), "used_bytes": 1000, "max_storage": 10 ** 10}


def raw_request(port, method, path, body=None, headers=None, host="default"):
    """Send exactly these headers (plus Host unless host is None) and return (status, headers, body)."""
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        connection.putrequest(method, path, skip_host=True, skip_accept_encoding=True)
        if host is not None:
            connection.putheader("Host", f"127.0.0.1:{port}" if host == "default" else host)
        for name, value in (headers or {}).items():
            connection.putheader(name, value)
        if body is not None and not any(name.lower() in ("content-length", "transfer-encoding")
                                        for name in (headers or {})):
            connection.putheader("Content-Length", str(len(body)))
        connection.endheaders(body)
        response = connection.getresponse()
        return response.status, response.headers, response.read()
    finally:
        connection.close()


class ApiTestCase(unittest.TestCase):
    """Base: an ApiServer on a free port for a FakeNode."""

    def setUp(self):
        self.node = FakeNode()
        self.server = ApiServer(self.node, 0)  # port 0: the OS picks a free one
        self.server.start()
        self.addCleanup(self.server.stop)
        self.port = self.server.port

    def call(self, method, path, body=None, headers=None, api=True, host="default"):
        """(status, headers, body) of a request, with the server's token in X-Rnfs-Api unless api is False."""
        headers = dict(headers or {})
        if api:
            headers["X-Rnfs-Api"] = self.server.token
        if isinstance(body, dict):
            body = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        return raw_request(self.port, method, path, body, headers, host)

    def ok(self, method, path, body=None, **kwargs):
        """The JSON of a request that must succeed."""
        status, _, data = self.call(method, path, body, **kwargs)
        self.assertEqual(status, 200, data)
        return json.loads(data)


class SecurityTest(ApiTestCase):

    def test_host_check(self):
        for host in (f"127.0.0.1:{self.port}", f"localhost:{self.port}", f"LocalHost:{self.port}"):
            self.assertEqual(self.call("GET", "/api/status", host=host)[0], 200, host)
        for host in ("evil.example", f"evil.example:{self.port}", "127.0.0.1", f"127.0.0.1:{self.port + 1}",
                     f"localhost.evil.example:{self.port}", None):
            status, _, data = self.call("GET", "/api/status", host=host)
            self.assertEqual(status, 403, host)
            self.assertIn("Host", json.loads(data)["error"])
        # Checked before anything else, including state-changing requests
        self.assertEqual(self.call("POST", "/api/gc", {}, host="evil.example")[0], 403)
        self.assertEqual(self.node.called("gc"), [])

    def test_token_required(self):
        # Every route but status, with valid parameters, so only the token can be what refuses them
        file_cid = self.node.put_file(b"x")
        with tempfile.TemporaryDirectory() as tmp:
            secret = os.path.join(tmp, "secret")
            with open(secret, "wb") as f:
                f.write(b"private key")
            self.node.keys["k"] = b"key"
            parameters = {"target": file_cid, "cid": file_cid, "node": "00" * 16, "name": "00" * 16,
                          "path": secret, "output": os.path.join(tmp, "out"), "alias": "k", "pin": True}
            query = "&".join(f"{name}={value}" for name, value in parameters.items() if name != "pin")
            wrong = ["1", "0", "", self.server.token[:-1], self.server.token + "x", "é" + self.server.token[1:]]
            for (method, path) in ApiHandler.ROUTES:
                if path == "/api/status":
                    continue
                body = parameters if method == "POST" else None
                status, _, data = self.call(method, f"{path}?{query}", body, api=False)
                self.assertEqual(status, 403, path)
                self.assertIn("X-Rnfs-Api", json.loads(data)["error"])
                for token in wrong:
                    self.assertEqual(self.call(method, f"{path}?{query}", body, {"X-Rnfs-Api": token},
                                               api=False)[0], 403, (path, token))
            self.assertEqual(self.node.calls, [])
            self.assertFalse(os.path.exists(parameters["output"]))
            # The right token works, with surrounding whitespace too
            self.assertEqual(self.call("GET", f"/api/cat?target={file_cid}", None,
                                       {"X-Rnfs-Api": f" {self.server.token} "}, api=False)[2], b"x")
        # Status stays open for health checks
        self.assertEqual(self.call("GET", "/api/status", api=False)[0], 200)

    def test_tokens(self):
        self.assertGreaterEqual(len(self.server.token), 43)  # 32 random bytes
        other = ApiServer(self.node, 0)
        self.addCleanup(other.stop)
        self.assertNotEqual(other.token, self.server.token)
        chosen = ApiServer(self.node, 0, token="chosen")
        self.addCleanup(chosen.stop)
        self.assertEqual(chosen.token, "chosen")
        with self.assertRaises(ValueError):
            ApiServer(self.node, 0, token="")

    def test_proof_of_the_token(self):
        # A challenge is answered with the proof the CLI checks before it sends the token (rnfs.cli.api_proof)
        challenge = "0123456789abcdef" * 2
        status, headers, _ = self.call("GET", "/api/status", headers={cli.CHALLENGE_HEADER: challenge}, api=False)
        self.assertEqual(status, 200)
        self.assertEqual(headers[cli.PROOF_HEADER], cli.api_proof(self.server.token, self.port, challenge))
        for bad in ("short", "zz" * 16, challenge + "00"):
            _, headers, _ = self.call("GET", "/api/status", headers={cli.CHALLENGE_HEADER: bad}, api=False)
            self.assertIsNone(headers[cli.PROOF_HEADER], bad)
        self.assertIsNone(self.call("GET", "/api/status", api=False)[1][cli.PROOF_HEADER])

    def test_http_0_9_is_refused_with_headers(self):
        # http.server would answer HTTP/0.9 with a bare body; every answer here has a status line and headers
        for line in (b"GET /api/status\r\n", b"GET /api/status HTTP/0.9\r\n", b"GARBAGE\r\n"):
            with socket.create_connection(("127.0.0.1", self.port), timeout=10) as sock:
                sock.sendall(line + f"Host: 127.0.0.1:{self.port}\r\n\r\n".encode())
                data, _ = read_until_closed(sock)
            self.assertTrue(data.startswith(b"HTTP/1.0 400 "), (line, data[:80]))

    def test_no_cors_headers(self):
        file_cid = self.node.put_file(b"x")
        headers = {"Origin": "http://evil.example", "Access-Control-Request-Method": "POST"}
        for method, path in (("GET", "/api/status"), ("OPTIONS", "/api/gc"), ("GET", "/nope"),
                             ("GET", f"/api/cat?target={file_cid}"), ("POST", "/api/gc")):
            status, response_headers, _ = self.call(method, path, b"{}" if method == "POST" else None, headers)
            self.assertFalse([h for h in response_headers if h.lower().startswith("access-control")], path)


class EndpointTest(ApiTestCase):

    def test_routing_errors(self):
        self.assertEqual(self.call("GET", "/nope")[0], 404)
        status, headers, _ = self.call("GET", "/api/gc")
        self.assertEqual(status, 405)
        self.assertEqual(headers["Allow"], "POST")
        self.assertEqual(self.call("DELETE", "/api/status")[0], 405)

    def test_add_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "f.txt")
            with open(path, "wb") as f:
                f.write(b"hello")
            result = self.ok("POST", "/api/add", {"path": path, "wrap": True, "only_hash": False})
            self.assertEqual(self.node.called("add_path"), [((path,), {"wrap": True, "only_hash": False})])
            self.assertEqual(result["warnings"], ["skipped a symlink"])
            self.assertEqual(self.ok("POST", "/api/add", {"path": path})["cid"], str(Cid.for_block(RAW, b"hello")))
        self.assertEqual(self.call("POST", "/api/add", {"path": "relative/path"})[0], 400)
        self.assertEqual(self.call("POST", "/api/add", b"not json")[0], 400)
        self.assertEqual(self.call("POST", "/api/add", b"[1]")[0], 400)

    def test_add_stream_content_length(self):
        result = self.ok("POST", "/api/add/stream?name=a.txt&only_hash=1", b"hello world")
        (data,), kwargs = self.node.called("add_stream")[0]
        self.assertEqual(data, b"hello world")
        self.assertEqual(kwargs, {"size": 11, "name": "a.txt", "only_hash": True})
        self.assertEqual(self.node.objects[result["cid"]][0], "dir")

    def test_add_stream_chunked(self):
        body = b"5\r\nhello\r\n1;ext=1\r\n \r\n5\r\nworld\r\n0\r\nTrailer: x\r\n\r\n"
        result = self.ok("POST", "/api/add/stream", body, headers={"Transfer-Encoding": "chunked"})
        self.assertEqual(result["cid"], str(Cid.for_block(RAW, b"hello world")))
        self.assertEqual(self.node.called("add_stream")[0][1]["size"], None)

    def test_bad_uploads(self):
        self.assertEqual(self.call("POST", "/api/add/stream", b"x", {"Content-Length": "abc"})[0], 411)
        self.assertEqual(self.call("POST", "/api/add/stream")[0], 411)
        self.assertEqual(self.call("POST", "/api/add/stream", b"x", {"Content-Length": "-1"})[0], 400)
        self.assertEqual(self.call("POST", "/api/add/stream", b"zz\r\nab", {"Transfer-Encoding": "chunked"})[0], 400)

    def test_short_upload_is_rejected(self):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        self.addCleanup(connection.close)
        connection.putrequest("POST", "/api/add/stream")
        connection.putheader("Content-Length", "10")
        connection.putheader("X-Rnfs-Api", self.server.token)
        connection.endheaders(b"abc")
        connection.sock.shutdown(socket.SHUT_WR)  # end the body early
        self.assertEqual(connection.getresponse().status, 400)
        self.assertEqual(self.node.objects, {})

    def test_cat(self):
        cid = self.node.put_file(b"0123456789" * 5)
        status, headers, data = self.call("GET", f"/api/cat?target={cid}")
        self.assertEqual((status, data), (200, b"0123456789" * 5))
        self.assertEqual(headers["Content-Length"], "50")
        self.assertEqual(headers["Content-Type"], "application/octet-stream")
        self.assertEqual(self.call("GET", f"/api/cat?target={cid}&offset=12&length=5")[2], b"23456")
        self.assertEqual(self.call("GET", f"/api/cat?target={cid}&offset=48")[2], b"89")
        self.assertEqual(self.call("GET", f"/api/cat?target={cid}&offset=60")[2], b"")
        self.assertEqual(self.node.called("read")[1], ((cid,), {"offset": 12, "length": 5, "for_gateway": False,
                                                               "from_node": None}))
        self.assertEqual(self.call("GET", f"/api/cat?target={cid}&offset=-1")[0], 400)
        self.assertEqual(self.call("GET", f"/api/cat?target={cid}&length=x")[0], 400)
        directory = self.node.put_dir({"a": cid})
        self.assertEqual(self.call("GET", f"/api/cat?target={directory}")[0], 400)
        self.assertEqual(self.call("GET", f"/api/cat?target={directory}/a")[2], b"0123456789" * 5)
        self.assertEqual(self.call("GET", f"/api/cat?target={directory}/missing")[0], 404)

    def test_cat_failing_mid_stream_is_cut_short(self):
        cid = self.node.put_file(b"x" * 100)

        def failing(*args, **kwargs):
            yield b"x" * 10
            raise NotFound("provider vanished")
        self.node.read = failing
        logged = mock.patch("rnfs.api.log")
        logged.start()
        self.addCleanup(logged.stop)
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        self.addCleanup(connection.close)
        connection.request("GET", f"/api/cat?target={cid}", headers={"X-Rnfs-Api": self.server.token})
        response = connection.getresponse()
        self.assertEqual(response.status, 200)
        with self.assertRaises(http.client.IncompleteRead):
            response.read()

    def test_read_only_endpoints(self):
        file_cid = self.node.put_file(b"abc")
        directory = self.node.put_dir({"<b>": file_cid})
        self.assertEqual(self.ok("GET", f"/api/info?target={directory}")["type"], "directory")
        self.assertEqual(self.ok("GET", f"/api/ls?target={directory}")["entries"],
                         [{"name": "<b>", "kind": "file", "size": 3, "cid": file_cid}])
        self.assertEqual(self.ok("GET", f"/api/stat?target={directory}/%3Cb%3E")["size"], 3)
        self.node.provider_map[file_cid] = [bytes.fromhex("ab" * 16)]
        self.assertEqual(self.ok("GET", f"/api/providers?cid={file_cid}"), {"providers": ["ab" * 16]})
        self.assertEqual(self.ok("GET", f"/api/ping?node={'cd' * 16}"), {"rtt": 0.25})
        self.assertEqual(self.node.called("ping")[0][0], (bytes.fromhex("cd" * 16),))
        self.assertEqual(self.ok("GET", f"/api/ping?node={'ff' * 16}"), {"rtt": None})
        self.assertEqual(self.ok("GET", "/api/status")["node_id"], bytes(range(16)).hex())

    def test_validation(self):
        file_cid = self.node.put_file(b"abc")
        for path in ("/api/ls?target=not-a-cid", "/api/ls", f"/api/ls?target={file_cid.upper()}",
                     "/api/providers?cid=bnope", "/api/ping?node=abc", f"/api/ping?node={'zz' * 16}",
                     "/api/name/resolve?name=short"):
            status, _, data = self.call("GET", path)
            self.assertEqual(status, 400, path)
            self.assertIn("error", json.loads(data))
        self.assertEqual(self.call("POST", "/api/get", {"target": file_cid, "output": "rel"})[0], 400)
        self.assertEqual(self.call("POST", "/api/get", {"target": file_cid, "output": "/x", "from": "bad"})[0], 400)
        self.assertEqual(self.call("POST", "/api/pin/remote", {"node": "ab" * 16, "cid": file_cid, "pin": "yes"})[0],
                         400)
        self.assertEqual(self.call("POST", "/api/dir/rm", {"target": file_cid})[0], 400)

    def test_error_mapping(self):
        file_cid = self.node.put_file(b"abc")
        for error, status in ((ValueError("v"), 400), (NotFound("n"), 404), (TooLarge("t"), 413),
                              (InvalidDag("i"), 422), (Busy("b"), 503), (StorageFull("s"), 507),
                              (RnfsError("r"), 500), (RuntimeError("boom"), 500)):
            with mock.patch.object(self.node, "stat", side_effect=error), mock.patch("rnfs.api.log"):
                code, headers, data = self.call("GET", f"/api/stat?target={file_cid}")
            self.assertEqual(code, status, error)
            self.assertEqual(json.loads(data), {"error": str(error)})
            self.assertEqual(headers["Content-Type"], "application/json")
            if status == 503:
                self.assertIn("Retry-After", headers)

    def test_state_changing_endpoints(self):
        file_cid = self.node.put_file(b"abc")
        directory = self.node.put_dir({"a": file_cid})
        new_dir = self.ok("POST", "/api/dir/set", {"target": directory, "name": "b", "cid": file_cid,
                                                   "executable": True})["cid"]
        self.assertEqual(self.node.called("dir_set")[0], ((directory, "b", file_cid), {"executable": True}))
        self.assertEqual(self.ok("POST", "/api/dir/rm", {"target": f"{new_dir}/b"})["cid"], directory)
        self.assertEqual(self.ok("POST", "/api/pin/add", {"target": f"{directory}/a"}), {"cid": file_cid})
        self.assertEqual(self.ok("GET", f"/api/pin/ls?cid={file_cid}")["pins"],
                         [{"cid": file_cid, "sources": ["user"], "complete": True}])
        self.assertEqual(self.ok("POST", "/api/pin/rm", {"cid": file_cid}), {})
        self.assertEqual(self.call("POST", "/api/pin/rm", {"cid": file_cid})[0], 404)
        self.assertEqual(self.ok("POST", "/api/pin/remote", {"node": "ab" * 16, "cid": file_cid, "pin": False}),
                         {"accepted": True})
        self.assertEqual(self.node.called("pin_remote")[0][0], (bytes.fromhex("ab" * 16), file_cid, False))
        self.assertEqual(self.ok("POST", "/api/gc"), {"removed_blocks": 3, "freed_bytes": 300})
        self.assertEqual(self.ok("POST", "/api/verify")["bad"], [])
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "out")
            self.assertEqual(self.ok("POST", "/api/get", {"target": directory, "output": out, "from": "ab" * 16})
                             ["cid"], directory)
            self.assertEqual(self.node.called("get")[0][1], {"from_node": bytes.fromhex("ab" * 16)})
            with open(os.path.join(out, "a"), "rb") as f:
                self.assertEqual(f.read(), b"abc")
            archive = os.path.join(tmp, "archive")
            self.assertEqual(self.ok("POST", "/api/export", {"target": directory, "output": archive})["roots"],
                             [directory])
            self.assertEqual(len(self.ok("POST", "/api/import", {"path": archive})["roots"]), 1)

    def test_names(self):
        file_cid = self.node.put_file(b"abc")
        created = self.ok("POST", "/api/name/create", {"alias": "site"})
        self.assertEqual(self.call("POST", "/api/name/create", {"alias": "site"})[0], 400)
        status, headers, key = self.call("POST", "/api/name/export", {"alias": "site"})
        self.assertEqual((status, headers["Content-Type"]), (200, "application/octet-stream"))
        self.assertEqual(key, self.node.keys["site"])
        self.assertEqual(self.ok("POST", "/api/name/import?alias=copy", key)["name"], created["name"])
        self.assertEqual(self.call("POST", "/api/name/import?alias=copy2", b"")[0], 400)
        self.assertEqual(self.call("POST", "/api/name/import?alias=big", b"x" * 100_000)[0], 413)
        published = self.ok("POST", "/api/name/publish", {"alias": "site", "target": file_cid})
        self.assertEqual(published, {"name": created["name"], "cid": file_cid, "sequence": 1})
        self.assertEqual(self.ok("GET", f"/api/name/resolve?name={created['name']}")["cid"], file_cid)
        self.assertEqual(self.node.called("name_resolve")[0][1], {"for_gateway": False})
        self.assertEqual(len(self.ok("GET", "/api/name/list")["names"]), 2)
        self.assertEqual(self.ok("POST", "/api/name/rm", {"alias": "copy"}), {})
        self.assertEqual(self.call("POST", "/api/name/export", {"alias": "copy"})[0], 404)


def read_until_closed(sock: socket.socket, limit: float = 5.0) -> tuple[bytes, float]:
    """(everything sock receives until the server closes it, seconds that took). Fails after limit."""
    sock.settimeout(limit)
    start = time.monotonic()
    data = b""
    while True:
        try:
            chunk = sock.recv(65536)
        except ConnectionResetError:
            chunk = b""
        if not chunk:
            return data, time.monotonic() - start
        data += chunk


def held_connections(port: int, count: int, data: bytes = b"GET /") -> list[socket.socket]:
    """count connections that have sent data (an unfinished request) and wait; closed at cleanup."""
    connections = [socket.create_connection(("127.0.0.1", port)) for _ in range(count)]
    for connection in connections:
        connection.sendall(data)
    return connections


class ConnectionTest(ApiTestCase):
    """Time limits and the connection cap, with short limits so the tests are quick."""

    LIMITS = {"request_timeout": 0.5, "body_timeout": 0.5, "max_connections": 3}

    def setUp(self):
        patcher = mock.patch.multiple(ApiServer, **self.LIMITS)
        patcher.start()
        self.addCleanup(patcher.stop)
        super().setUp()

    def connect(self) -> socket.socket:
        """A raw connection to the API, closed at cleanup."""
        sock = socket.create_connection(("127.0.0.1", self.port))
        self.addCleanup(sock.close)
        return sock

    def request_head(self, method: str, path: str, **headers) -> bytes:
        """A request line and headers carrying the token and a valid Host."""
        lines = [f"{method} {path} HTTP/1.1", f"Host: 127.0.0.1:{self.port}", f"X-Rnfs-Api: {self.server.token}",
                 *(f"{name.replace('_', '-')}: {value}" for name, value in headers.items())]
        return ("\r\n".join(lines) + "\r\n\r\n").encode()

    def test_idle_connection_is_closed(self):
        data, elapsed = read_until_closed(self.connect())
        self.assertEqual(data, b"")
        self.assertGreater(elapsed, 0.3)
        self.assertLess(elapsed, 3)

    def test_slow_headers_hit_the_deadline(self):
        # Trickling bytes doesn't extend the time allowed: the whole request has one deadline
        sock = self.connect()
        start = time.monotonic()
        with self.assertRaises(OSError):
            for _ in range(50):
                sock.sendall(b"G")
                time.sleep(0.1)
        self.assertLess(time.monotonic() - start, 3)

    def test_short_json_body(self):
        sock = self.connect()
        sock.sendall(self.request_head("POST", "/api/gc", Content_Length=10) + b"{}")
        data, elapsed = read_until_closed(sock)
        self.assertTrue(data.startswith(b"HTTP/1.0 408"), data)
        self.assertLess(elapsed, 3)
        self.assertEqual(self.node.called("gc"), [])

    def test_short_upload(self):
        sock = self.connect()
        sock.sendall(self.request_head("POST", "/api/add/stream", Content_Length=10) + b"abc")
        data, elapsed = read_until_closed(sock)
        self.assertTrue(data.startswith(b"HTTP/1.0 408"), data)
        self.assertLess(elapsed, 3)
        self.assertEqual(self.node.objects, {})

    def test_slow_upload_within_body_timeout(self):
        # Each pause is under body_timeout, and together they exceed request_timeout
        sock = self.connect()
        sock.sendall(self.request_head("POST", "/api/add/stream", Content_Length=6))
        for piece in (b"ab", b"cd", b"ef"):
            time.sleep(0.3)
            sock.sendall(piece)
        data, _ = read_until_closed(sock)
        self.assertTrue(data.startswith(b"HTTP/1.0 200"), data)
        self.assertEqual(self.node.called("add_stream")[0][0], (b"abcdef",))

    def test_node_work_is_not_timed(self):
        # A fetch longer than every limit still gets its answer
        cid = self.node.put_file(b"remote", remote=True)
        self.node.gate = threading.Event()
        threading.Timer(1.2, self.node.gate.set).start()
        self.assertEqual(self.ok("POST", "/api/pin/add", {"target": cid})["cid"], cid)
        self.assertEqual(self.call("GET", f"/api/cat?target={cid}")[2], b"remote")

    def test_connection_cap(self):
        held = held_connections(self.port, 3)
        for sock in held:
            self.addCleanup(sock.close)
        time.sleep(0.1)  # let the server accept them
        status, headers, data = self.call("GET", "/api/status")
        self.assertEqual(status, 503)
        self.assertIn("too many", json.loads(data)["error"])
        self.assertEqual(headers["Retry-After"], "10")
        self.assertEqual(self.node.called("status"), [])
        # The held connections time out, freeing their slots
        for sock in held:
            read_until_closed(sock)
        time.sleep(0.1)
        self.assertEqual(self.call("GET", "/api/status")[0], 200)

    def test_log_escapes_the_target(self):
        def failing(*args, **kwargs):
            yield b"x"
            raise RuntimeError("boom")
        cid = self.node.put_file(b"0123456789")
        with mock.patch.object(self.node, "info", return_value={"type": "file", "size": 10, "cid": cid}), \
                mock.patch.object(self.node, "read", failing), mock.patch("rnfs.api.log") as log:
            with self.assertRaises(http.client.IncompleteRead):  # cut short, as the failure happened mid-stream
                self.call("GET", f"/api/cat?target={cid}/%1b%5b2J%0a")
        (message, _), = [call.args for call in log.call_args_list]
        self.assertIn(f"{cid}/\\x1b[2J\\n", message)
        self.assertNotIn("\x1b", message)
        self.assertEqual(loggable("plain/path"), "plain/path")


class BindTest(unittest.TestCase):
    """The listening socket."""

    def test_windows_binds_exclusively(self):
        # On Windows SO_REUSEADDR would let another socket bind the API's port while the daemon listens on it
        with mock.patch.object(fsutil, "IS_WINDOWS", True):
            server = ApiServer(FakeNode(), 0)
        self.addCleanup(server.stop)
        self.assertFalse(server.server.socket.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR))
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            self.assertTrue(server.server.socket.getsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE))
        with self.assertRaises(OSError):
            ApiServer(FakeNode(), server.port)  # a second daemon can't share the port


class BodyReaderTest(unittest.TestCase):

    def test_reads_in_pieces(self):
        import io
        reader = BodyReader(io.BytesIO(b"3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n"), 0, chunked=True)
        self.assertEqual(reader.read(2), b"ab")
        self.assertEqual(reader.read(2), b"cd")
        self.assertEqual(reader.read(), b"e")
        self.assertEqual(reader.read(), b"")
        reader = BodyReader(io.BytesIO(b"abcdef"), 4, chunked=False)
        self.assertEqual(reader.read(3) + reader.read(3) + reader.read(), b"abcd")
        with self.assertRaises(ValueError):
            BodyReader(io.BytesIO(b"ab"), 4, chunked=False).read()

    def test_chunk_sizes_are_plain_hex(self):
        import io
        self.assertEqual(BodyReader(io.BytesIO(b"A;ext=1\r\n" + b"x" * 10 + b"\r\n0\r\n\r\n"), 0, True).read(),
                         b"x" * 10)
        for size in (b"0x3", b"+3", b"0_3", b"-3", b"3 3", b"", b"1" * 17):
            with self.assertRaises(ValueError, msg=size):
                BodyReader(io.BytesIO(size + b"\r\nabc\r\n0\r\n\r\n"), 0, chunked=True).read()

    def test_trailers_are_capped(self):
        import io
        trailers = b"".join(b"X-T: %d\r\n" % i for i in range(32))
        self.assertEqual(BodyReader(io.BytesIO(b"1\r\na\r\n0\r\n" + trailers + b"\r\n"), 0, True).read(), b"a")
        with self.assertRaises(ValueError):
            BodyReader(io.BytesIO(b"1\r\na\r\n0\r\n" + trailers * 2 + b"\r\n"), 0, chunked=True).read()


if __name__ == "__main__":
    unittest.main()


class EarlyResponseTest(ApiTestCase):
    """A response sent before the request body is read still reaches the client (no connection reset)."""

    def test_refusal_before_reading_the_body_is_received(self):
        # Without the token the request is refused before its body is read. Closing the socket with that body
        # unread would make the kernel send a reset, and the client (on Windows always) would lose the 403
        body = b"x" * 128 * 1024
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=10)
        self.addCleanup(sock.close)
        sock.sendall(f"POST /api/add/stream HTTP/1.1\r\nHost: 127.0.0.1:{self.port}\r\n"
                     f"Content-Length: {len(body)}\r\n\r\n".encode() + body[:16384])
        time.sleep(0.3)  # the server answers and closes its side meanwhile
        try:
            sock.sendall(body[16384:])  # the rest of the body arrives after the response was sent
        except OSError:
            pass  # a reset here already means the response may be lost; the read below decides
        response = b""
        while True:
            data = sock.recv(65536)
            if not data:
                break
            response += data
        self.assertTrue(response.startswith(b"HTTP/1.0 403") or response.startswith(b"HTTP/1.1 403"), response[:80])
