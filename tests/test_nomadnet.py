"""`rnfs add <destination>:/page/<path>` and `:/file/<path>` (rnfs/nomadnet_links.py, rnfs/node_content.py add_nomadnet, the
API, add jobs and the CLI), with the fetch from the NomadNet node faked (FakeNomadnet); the fetch's own bounds are in
tests/security/test_nomadnet_link_bounds.py, a real one over RNS in tests/test_nomadnet_e2e.py."""
import contextlib
import hashlib
import io
import json
import os
import secrets
import stat
import tempfile
import sys
import unittest
from unittest import mock

import RNS

from rnfs import api, cli, nomadnet_links
from rnfs.api import ApiServer
from rnfs.apiclient import ApiConnection
from rnfs.config import Config
from rnfs.content.cid import Cid
from rnfs.content.storage import USER
from rnfs.errors import Denied, NotFound, TooLarge
from rnfs.features import jobs
from rnfs.network import nomadnet as fetching
from rnfs.network.identity import load_or_create_identity
from rnfs.node_content import nomadnet_identity_path
from rnfs.node import RnfsNode
from tests.support import wait_for
from tests.test_jobs import ApiBase, JobsBase
from tests.test_links import FakeLink, RacingLink
from tests.test_names import FakeNetwork
from tests.test_node import NetClient, NodeTest

DEST = "e166ecdbc405bcc9762b6767617fc949"
PAGE = f"{DEST}:/page/index.mu"
FILE = f"{DEST}:/file/music/song.mp3"
PAGE_BYTES = b">Welcome\n\nA NomadNet page, as the node renders it.\n"
FILE_BYTES = os.urandom(300_000)
_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the nodes' log (the CLI tests read stdout)."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class FakeNomadnet:
    """Stands in for rnfs.network.nomadnet.fetch: answers each path from answers ({path: (bytes, sent name or None,
    sent as a file)}) as the real fetch hands its Snapshot over, reporting progress, and records each call."""

    def __init__(self):
        """A node serving PAGE and FILE."""
        self.answers = {"/page/index.mu": (PAGE_BYTES, None, False),
                        "/file/music/song.mp3": (FILE_BYTES, b"song.mp3", True)}
        self.calls: list[dict] = []

    def __call__(self, target, config, directory, identity=None, max_size=None, progress=None, stop_event=None,
                 paths=None):
        """The fetch."""
        self.calls.append({"target": target, "directory": directory, "identity": identity, "max_size": max_size})
        if target.path not in self.answers:
            raise NotFound(f"NomadNet node didn't answer {target.path}: no answer")
        data, sent_name, file = self.answers[target.path]
        if max_size is not None and len(data) > max_size:
            raise TooLarge(f"{len(data)} bytes, over {max_size}")
        if progress is not None:
            progress(len(data) // 2, len(data))
            progress(len(data), len(data))
        path = os.path.join(directory, f"{fetching.PARTIAL_PREFIX}{secrets.token_hex(4)}{fetching.PARTIAL_SUFFIX}")
        with open(path, "xb") as f:
            f.write(data)
        return fetching.Snapshot(path, len(data), sent_name, file)

    @contextlib.contextmanager
    def installed(self):
        """Fetch with this fake while inside."""
        with mock.patch.object(fetching, "fetch", self):
            yield self


def partials(node) -> list[str]:
    """The copies of NomadNet answers left in node's state directory."""
    return [name for name in os.listdir(node.config.state_dir) if name.startswith(fetching.PARTIAL_PREFIX)]


class ParseTest(unittest.TestCase):
    """The links' syntax, the snapshot's name and NomadNet's "Request Not Allowed" page."""

    def test_links(self):
        link = nomadnet_links.parse(PAGE)
        self.assertEqual((link.destination.hex(), link.path, link.kind, link.last_name),
                         (DEST, "/page/index.mu", nomadnet_links.PAGE, "index.mu"))
        self.assertEqual(str(nomadnet_links.parse(DEST.upper() + ":/file/a b/c.txt")), DEST + ":/file/a b/c.txt")
        self.assertEqual(nomadnet_links.parse(FILE).kind, nomadnet_links.FILE)

    def test_malformed_links(self):
        for text in (DEST[:-1] + ":/page/x", DEST + ":/pages/x", DEST + ":/page/", DEST + ":/page//x",
                     DEST + ":/page/a/../b", DEST + ":/page/./x", DEST + ":/file/a/", DEST + "/page/x",
                     DEST + ":/page/index.mu`var_x=1", DEST + ":/page/a\x1bb", DEST + ":/page/" + "a" * 1100,
                     DEST + ":/page/a\x00", "", None, 5):
            with self.subTest(text=repr(text)[:60]), self.assertRaises(ValueError):
                nomadnet_links.parse(text)

    def test_local_files_named_like_links(self):
        self.assertTrue(nomadnet_links.looks_like(PAGE))
        self.assertTrue(nomadnet_links.looks_like(DEST + ":/page/"))  # malformed, but a link: refused, not read as a file
        for text in ("./" + PAGE, "/abs/" + PAGE, "-", "notes.txt", DEST, DEST + ":/other/x"):
            self.assertFalse(nomadnet_links.looks_like(text), text)

    def test_snapshot_names(self):
        page, file = nomadnet_links.parse(PAGE), nomadnet_links.parse(FILE)
        self.assertEqual(nomadnet_links.snapshot_name(page, b"other.txt"), "index.mu")  # a page's metadata never counts
        for sent, expected in ((b"song.mp3", "song.mp3"), ("Song é.mp3", "Song é.mp3"), (None, "song.mp3"),
                               (b"../etc/passwd", "song.mp3"), (b"a/b", "song.mp3"), (b"..", "song.mp3"),
                               (b"\xff\xfe", "song.mp3"), (b"x\x1b[2J", "song.mp3"), (b"x" * 300, "song.mp3"),
                               (b"", "song.mp3"), (5, "song.mp3")):
            with self.subTest(sent=sent):
                self.assertEqual(nomadnet_links.snapshot_name(file, sent), expected)

    def test_request_not_allowed_page(self):
        self.assertTrue(nomadnet_links.is_not_allowed(nomadnet_links.NOT_ALLOWED))
        self.assertTrue(nomadnet_links.is_not_allowed(nomadnet_links.NOT_ALLOWED.replace(b"\n", b"\r\n") + b"\n\n"))
        for page in (PAGE_BYTES, b"", nomadnet_links.NOT_ALLOWED + b"\nbut a real page goes on " * 20,
                     b">Request Not Allowed\n"):
            self.assertFalse(nomadnet_links.is_not_allowed(page), page[:40])


class FetchFlowTest(unittest.TestCase):
    """fetch's handling of NomadNet's "Request Not Allowed" page and of identifying, over a faked link."""

    def setUp(self):
        self.tmp = self.enterContext(tempfile.TemporaryDirectory())
        self.link = mock.Mock()
        self.answers: list[bytes] = []
        self.enterContext(mock.patch.object(fetching, "open_link", lambda target, timeout, paths=None: self.link))
        self.enterContext(mock.patch.object(fetching, "_request", self.request))
        self.config = mock.Mock(path_timeout=1)

    def request(self, link, target, config, directory, max_size, progress, stop_event, early=None):
        """The next answer, copied as _request copies one."""
        path = os.path.join(directory, f"answer{len(os.listdir(directory))}")
        with open(path, "wb") as f:
            f.write(self.answers.pop(0))
        return fetching.Snapshot(path, os.path.getsize(path), None, False)

    def fetch(self, identity=None):
        """fetching.fetch of PAGE into the temporary directory."""
        return fetching.fetch(nomadnet_links.parse(PAGE), self.config, self.tmp, identity=identity)

    def test_anonymous_refusal_fails_and_says_how(self):
        self.answers = [nomadnet_links.NOT_ALLOWED]
        with self.assertRaises(NotFound) as caught:
            self.fetch()
        self.assertIn("Request Not Allowed", str(caught.exception))
        self.assertIn("--identify", str(caught.exception))
        self.assertEqual(os.listdir(self.tmp), [])  # never added
        self.link.identify.assert_not_called()
        self.link.teardown.assert_called()

    def test_identified_refusal_is_retried_once(self):
        identity = mock.Mock(hash=bytes(range(16)))
        self.answers = [nomadnet_links.NOT_ALLOWED, PAGE_BYTES]
        snapshot = self.fetch(identity)
        with open(snapshot.path, "rb") as f:
            self.assertEqual(f.read(), PAGE_BYTES)
        self.assertEqual(self.link.identify.call_args_list, [mock.call(identity)] * 2)
        self.answers = [nomadnet_links.NOT_ALLOWED, nomadnet_links.NOT_ALLOWED]
        with self.assertRaises(NotFound) as caught:
            self.fetch(identity)
        self.assertIn("nomadnet_identity", str(caught.exception))
        self.assertIn(bytes(range(16)).hex(), str(caught.exception))  # the hash to ask the node's operator to list

    def test_pages_pass(self):
        for page in (PAGE_BYTES, b"", nomadnet_links.NOT_ALLOWED * 5):
            self.answers = [page]
            self.assertEqual(self.fetch().size, len(page))


class RacingNomadnetLink(RacingLink):
    """RacingLink (RNS 1.5.6's Link.request, its answer arriving before the request is registered) with what fetch
    sets on a NomadNet link."""

    def set_resource_started_callback(self, callback):
        """Keep the callback: these answers are packets, not Resources."""
        self.resource_started = callback


class EarlyAnswerTest(unittest.TestCase):
    """A NomadNet answer sent as a packet that arrives before RNS has registered its request (links.EarlyResponses):
    RNS would drop it, and the fetch fail once RNS's timeout (RacingLink's, shortened) has passed."""

    def setUp(self):
        self.tmp = self.enterContext(tempfile.TemporaryDirectory())
        saved = FakeLink.responder, FakeLink.created
        self.addCleanup(setattr, FakeLink, "created", saved[1])
        self.addCleanup(setattr, FakeLink, "responder", saved[0])
        self.answers: list[bytes] = []
        FakeLink.responder = staticmethod(lambda link, path, data: self.answers.pop(0))
        self.link = RacingNomadnetLink(None, established_callback=lambda link: None,
                                       closed_callback=lambda link: None)
        wait_for(lambda: self.link.status == RNS.Link.ACTIVE, what="the fake link established")
        self.enterContext(mock.patch.object(fetching, "open_link", lambda target, timeout, paths=None: self.link))

    def fetch(self, identity=None) -> bytes:
        """What fetching.fetch of PAGE copied, read back."""
        snapshot = fetching.fetch(nomadnet_links.parse(PAGE), Config(), self.tmp, identity=identity)
        with open(snapshot.path, "rb") as f:
            return f.read()

    def test_an_early_page_is_handed_over(self):
        self.answers = [PAGE_BYTES]
        self.assertEqual(self.fetch(), PAGE_BYTES)
        self.assertEqual(self.link.pending_requests, [])
        self.assertEqual(self.link.status, RNS.Link.CLOSED)  # the fetch's link closed as it ended

    def test_an_identified_retry_too(self):
        # "Request Not Allowed", then the page after identifying again: two requests on the link, both early
        self.answers = [nomadnet_links.NOT_ALLOWED, PAGE_BYTES]
        identity = object()
        self.assertEqual(self.fetch(identity), PAGE_BYTES)
        self.assertEqual(self.link.identities, [identity, identity])
        self.assertEqual(len(self.link.requests), 2)


class NodeAddTest(NodeTest):
    """RnfsNode.add_nomadnet on an offline node, with the fetch faked."""

    def setUp(self):
        super().setUp()
        self.fake = FakeNomadnet()
        patcher = mock.patch.object(fetching, "fetch", self.fake)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_page_added_as_a_file_and_pinned_with_its_link(self):
        result = self.a.add_nomadnet(PAGE)
        self.assertEqual(result["cid"], self.a.add_stream(io.BytesIO(PAGE_BYTES), only_hash=True)["cid"])
        self.assertEqual((result["size"], result["name"], result["link"]), (len(PAGE_BYTES), "index.mu", PAGE))
        cid = Cid.parse(result["cid"])
        self.assertIn(USER, self.a.storage.pins()[cid])
        [pin] = self.a.pin_ls(result["cid"])
        self.assertEqual((pin["origin"], pin["name"], pin["label"]), (PAGE, PAGE, None))
        self.assertEqual(partials(self.a), [])  # the copy is gone
        self.assertEqual(self.fake.calls[0]["directory"], self.a.config.state_dir)

    def test_a_users_label_is_kept(self):
        cid = self.a.add_nomadnet(PAGE)["cid"]
        self.a.pin_label(cid, "home page")
        self.a.add_nomadnet(PAGE)  # the same snapshot again
        self.assertEqual(self.a.pin_ls(cid)[0]["label"], "home page")

    def test_each_add_takes_a_new_snapshot(self):
        first = self.a.add_nomadnet(PAGE)["cid"]
        self.fake.answers["/page/index.mu"] = (b">Changed\n", None, False)
        second = self.a.add_nomadnet(PAGE)["cid"]
        self.assertEqual(len(self.fake.calls), 2)
        self.assertNotEqual(first, second)
        self.assertEqual({first, second} <= {pin["cid"] for pin in self.a.pin_ls()}, True)

    def test_names_and_wrapping(self):
        wrapped = self.a.add_nomadnet(FILE, wrap=True)
        self.assertEqual([entry["name"] for entry in self.a.ls(wrapped["cid"])], ["song.mp3"])
        self.assertEqual(wrapped["name"], "song.mp3")
        named = self.a.add_nomadnet(FILE, name="tune.mp3")  # as `add - --name`: wrapped under that name
        self.assertEqual([entry["name"] for entry in self.a.ls(named["cid"])], ["tune.mp3"])
        self.fake.answers["/file/music/song.mp3"] = (FILE_BYTES, b"../../.bashrc/x", True)
        fallback = self.a.add_nomadnet(FILE, wrap=True)
        self.assertEqual([entry["name"] for entry in self.a.ls(fallback["cid"])], ["song.mp3"])
        plain = self.a.add_nomadnet(FILE)
        self.assertEqual(self.a.stat(plain["cid"])["size"], len(FILE_BYTES))
        with self.assertRaises(ValueError):
            self.a.add_nomadnet(FILE, name="a/b")

    def test_add_options(self):
        cached = self.a.add_nomadnet(FILE, pin=False)
        self.assertNotIn(Cid.parse(cached["cid"]), self.a.storage.pins())
        self.assertIn(Cid.parse(cached["cid"]), self.a._fetched)  # kept as cache, like add --no-pin
        fastcdc = self.a.add_nomadnet(FILE, chunker="fastcdc", only_hash=True)
        self.assertEqual(fastcdc["cid"], self.a.add_stream(io.BytesIO(FILE_BYTES), chunker="fastcdc",
                                                           only_hash=True)["cid"])
        self.assertFalse(self.a.storage.has(Cid.parse(fastcdc["cid"])))
        calls = len(self.fake.calls)
        for options in ({"chunker": "nope"}, {"to": ["not-a-recipient"]}):
            with self.subTest(options), self.assertRaises(ValueError):
                self.a.add_nomadnet(FILE, **options)
        self.assertEqual(len(self.fake.calls), calls)  # refused before anything was fetched

    def test_deny_list_checked(self):
        self.a.deny.add(["sha256:" + hashlib.sha256(FILE_BYTES).hexdigest()])
        with self.assertRaises(Denied):
            self.a.add_nomadnet(FILE)
        self.assertEqual(partials(self.a), [])

    def test_failures(self):
        with self.assertRaises(ValueError):
            self.a.add_nomadnet(DEST + ":/page/../x")
        self.assertEqual(self.fake.calls, [])
        with self.assertRaises(NotFound):
            self.a.add_nomadnet(DEST + ":/page/missing.mu")
        self.assertEqual(partials(self.a), [])

    def test_bound_is_max_download_size_and_free_space(self):
        self.a.add_nomadnet(PAGE)
        self.assertLess(0, self.fake.calls[-1]["max_size"])
        node = self.make_node("small", bytes(16), max_download_size=1000)
        node.add_nomadnet(PAGE)
        self.assertEqual(self.fake.calls[-1]["max_size"], 1000)
        with self.assertRaises(TooLarge):
            node.add_nomadnet(FILE)

    def test_anonymous_unless_identify_then_its_own_identity(self):
        self.a.add_nomadnet(PAGE)
        self.assertIsNone(self.fake.calls[-1]["identity"])
        path = nomadnet_identity_path(self.a.config)
        self.assertFalse(os.path.exists(path))  # made on first use only
        self.a.add_nomadnet(PAGE, identify=True)
        identity = self.fake.calls[-1]["identity"]
        self.assertTrue(os.path.isfile(path))
        if os.name != "nt":
            self.assertEqual(stat.S_IMODE(os.stat(path).st_mode) & 0o077, 0)  # private, like the node's
        node_identity = load_or_create_identity(self.a.config.identity_path)
        self.assertNotEqual(identity.hash, node_identity.hash)  # never the node's
        self.a.add_nomadnet(PAGE, identify=True)
        self.assertEqual(self.fake.calls[-1]["identity"].hash, identity.hash)  # the same each time
        self.assertEqual(self.a.status()["nomadnet_identity_hash"], identity.hash.hex())

    def test_status_makes_the_identity_to_list_before_the_first_identified_add(self):
        path = nomadnet_identity_path(self.a.config)
        listed = self.a.status()["nomadnet_identity_hash"]
        self.assertTrue(os.path.isfile(path))
        self.assertNotEqual(listed, load_or_create_identity(self.a.config.identity_path).hash.hex())
        self.assertEqual(self.a.status()["nomadnet_identity_hash"], listed)
        self.a.add_nomadnet(PAGE, identify=True)
        self.assertEqual(self.fake.calls[-1]["identity"].hash.hex(), listed)

    def test_leftover_copies_removed_at_start(self):
        state = os.path.join(self.tmp.name, "restarted", "state")
        os.makedirs(state)
        leftover = os.path.join(state, fetching.PARTIAL_PREFIX + "abcd" + fetching.PARTIAL_SUFFIX)
        with open(leftover, "wb") as f:
            f.write(b"interrupted")
        self.make_node("restarted", bytes(16))
        self.assertFalse(os.path.exists(leftover))


class ApiAndJobsTest(ApiBase):
    """POST /api/add and POST /api/jobs with nomadnet, on node B behind its API."""

    def setUp(self):
        super().setUp()
        self.fake = FakeNomadnet()
        patcher = mock.patch.object(fetching, "fetch", self.fake)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_post_add(self):
        status, result = self.call("POST", "/api/add", {"nomadnet": FILE, "wrap": True, "pin": False})
        self.assertEqual(status, 200, result)
        self.assertEqual((result["name"], result["link"]), ("song.mp3", FILE))
        self.assertEqual(result["cid"], self.b.add_nomadnet(FILE, wrap=True, only_hash=True)["cid"])
        status, result = self.call("POST", "/api/add", {"nomadnet": PAGE, "name": "home.mu", "identify": True})
        self.assertEqual(status, 200, result)
        self.assertIsNotNone(self.fake.calls[-1]["identity"])
        for body, expected in (({"nomadnet": PAGE, "path": "/tmp/x"}, 400), ({"nomadnet": DEST + ":/page/"}, 400),
                               ({"nomadnet": PAGE, "name": "a/b"}, 400), ({"nomadnet": 5}, 400),
                               ({"nomadnet": DEST + ":/page/missing"}, 404)):
            with self.subTest(body):
                self.assertEqual(self.call("POST", "/api/add", body)[0], expected)

    def test_capability(self):
        self.assertIn("add_nomadnet", self.call("GET", "/api/capabilities")[1]["capabilities"])

    def test_add_job(self):
        status, job = self.call("POST", "/api/jobs", {"kind": "add", "nomadnet": FILE, "wrap": True, "name": "t.mp3"})
        self.assertEqual(status, 200, job)
        self.assertEqual((job["nomadnet"], job["path"], job["options"]), (FILE, None, {"wrap": True, "name": "t.mp3"}))
        job = self.finished(job["id"])
        self.assertEqual(job["state"], "done", job)
        self.assertEqual(job["result"], self.b.add_nomadnet(FILE, wrap=True, name="t.mp3"))
        self.assertEqual((job["total_bytes"], job["done_bytes"]), (len(FILE_BYTES), len(FILE_BYTES)))
        [saved] = [record for record in self.saved() if record["id"] == job["id"]]
        self.assertEqual(jobs.Job.from_record(saved).nomadnet, FILE)
        for bad in ({"kind": "add", "nomadnet": FILE, "path": "/x"}, {"kind": "add", "nomadnet": "nope"},
                    {"kind": "import", "nomadnet": FILE}, {"kind": "add", "nomadnet": FILE, "name": "../x"},
                    {"kind": "add", "nomadnet": FILE, "identify": "maybe", "chunker": "x"}):
            self.assertEqual(self.call("POST", "/api/jobs", bad)[0], 400, bad)

    def test_job_params_checked_when_loaded(self):
        for params in ({"nomadnet": FILE, "path": "/x"}, {"nomadnet": "nope"},
                       {"nomadnet": FILE, "options": {"name": 5}}, {"path": "/x", "options": {"identify": True}}):
            with self.subTest(params), self.assertRaises(ValueError):
                jobs.check_job_params("add", None, None, None, path=params.get("path"),
                                      options=params.get("options"), nomadnet=params.get("nomadnet"))
        jobs.check_job_params("add", None, None, None, nomadnet=FILE, options={"name": "x", "identify": True})

    def test_failed_job_says_why(self):
        status, job = self.call("POST", "/api/jobs", {"kind": "add", "nomadnet": DEST + ":/page/missing.mu"})
        self.assertEqual(status, 200, job)
        job = self.finished(job["id"])
        self.assertEqual(job["state"], "failed")
        self.assertIn("didn't answer", job["error"])

    def test_cancelled_job_stops_its_fetch(self):
        def held(target, config, directory, progress=None, **kwargs):
            """A fetch that reports progress until it is told to stop."""
            while True:
                progress(0, 1000)
        with mock.patch.object(fetching, "fetch", held):
            status, job = self.call("POST", "/api/jobs", {"kind": "add", "nomadnet": PAGE})
            self.jobs.cancel(job["id"])
            self.assertEqual(self.finished(job["id"])["state"], "cancelled")


class CliTest(ApiBase):
    """`rnfs add <link>` through the CLI, against node B's API."""

    def setUp(self):
        super().setUp()
        self.fake = FakeNomadnet()
        patcher = mock.patch.object(fetching, "fetch", self.fake)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.cli_dir = os.path.join(self.tmp.name, "cli")
        os.makedirs(self.cli_dir)
        cli.write_token(cli.token_path(self.cli_dir), self.server.token)

    def run_cli(self, *argv) -> tuple[int, str, str]:
        """(exit code, stdout, stderr) of cli.main(argv)."""
        stdout, stderr = io.StringIO(), io.StringIO()
        code = 0
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            try:
                cli.main([*argv, "--api-port", str(self.server.port), "--dir", self.cli_dir])
            except SystemExit as e:
                if isinstance(e.code, str):
                    print(e.code, file=sys.stderr)
                    code = 1
                else:
                    code = e.code or 0
        return code, stdout.getvalue(), stderr.getvalue()

    def test_add_a_link(self):
        code, out, err = self.run_cli("add", FILE, "--wrap", "--no-pin")
        self.assertEqual((code, out.strip()), (0, self.b.add_nomadnet(FILE, wrap=True, only_hash=True)["cid"]), err)
        [job] = [job for job in self.jobs.list() if job.get("nomadnet") == FILE]  # run as a job, like get
        self.assertEqual(job["options"], {"wrap": True, "pin": False})
        code, out, err = self.run_cli("add", PAGE, "--background", "--identify", "--name", "home.mu")
        self.assertEqual(code, 0, err)
        code, out, err = self.run_cli("jobs", "wait", out.strip())
        self.assertEqual(code, 0, err)
        self.assertIsNotNone(self.fake.calls[-1]["identity"])

    def test_refusals(self):
        code, _, err = self.run_cli("add", DEST + ":/page/a/../b")
        self.assertEqual(code, 1)
        self.assertIn("./" + DEST, err)  # how to add a local file named like that
        self.assertEqual(self.run_cli("add", DEST + ":/page/missing.mu")[0], 1)
        self.assertEqual(self.run_cli("add", "-", "--identify")[0], 1)

    @unittest.skipIf(os.name == "nt", "Windows reads ':' in a file name as a stream, so no file is named like a link")
    def test_local_file_named_like_a_link(self):
        os.makedirs(os.path.join(self.tmp.name, DEST + ":", "page"))
        local = os.path.join(self.tmp.name, DEST + ":", "page", "index.mu")
        with open(local, "wb") as f:
            f.write(b"a local file")
        cwd = os.getcwd()
        os.chdir(self.tmp.name)
        self.addCleanup(os.chdir, cwd)
        code, out, err = self.run_cli("add", "./" + PAGE)
        self.assertEqual((code, out.strip()), (0, self.b.add_path(local, only_hash=True)["cid"]), err)
        self.assertEqual(self.fake.calls, [])

    def test_daemon_without_the_capability(self):
        with mock.patch.object(api, "CAPABILITIES", tuple(c for c in api.CAPABILITIES if c != "add_nomadnet")):
            code, _, err = self.run_cli("add", PAGE)
        self.assertEqual(code, 1)
        self.assertIn("update it", err)
        self.assertEqual(self.fake.calls, [])

    def test_job_progress_line(self):
        job = {"state": "running", "kind": "add", "nomadnet": PAGE, "total_bytes": None, "done_bytes": 0}
        self.assertIn("NomadNet", jobs.format_progress(job))


class AppSourceTest(unittest.TestCase):
    """A NomadNet add made with an app's token pins with that app's source, as every other add does (app:<name>,
    app:<name>:<key> inside pin_key), in one request or as a job; the CLI's pins user."""

    def setUp(self):
        """An offline node behind its API, its jobs started; connections with the CLI's token and as app "wiki"."""
        self.tmp = self.enterContext(tempfile.TemporaryDirectory())
        config = Config(rnfs_dir=os.path.join(self.tmp, "node"), slow="no", max_jobs=2)
        config.validate()
        self.node = RnfsNode.offline(config, bytes(range(16)), NetClient(), FakeNetwork())
        self.addCleanup(self.node.stop)
        self.node.feature("jobs").start()
        self.server = ApiServer(self.node, 0)
        self.server.start()
        self.addCleanup(self.server.stop)
        token_file = os.path.join(self.tmp, "api_token")
        cli.write_token(token_file, self.server.token)
        self.cli = ApiConnection(self.tmp, port=self.server.port, token_file=token_file)
        self.app = ApiConnection(self.tmp, port=self.server.port, app="wiki", token=self.node.app_add("wiki")["token"])
        self.enterContext(FakeNomadnet().installed())

    def sources(self, cid: str) -> set[str]:
        """The sources pinning cid now."""
        return self.node.storage.pins().get(Cid.parse(cid), set())

    def test_app_pins_with_its_source(self):
        self.assertEqual(self.sources(self.app.add_nomadnet(PAGE)["cid"]), {"app:wiki"})
        with self.app.pin_key("pages"):
            keyed = self.app.add_nomadnet(FILE)["cid"]
        self.assertEqual(self.sources(keyed), {"app:wiki:pages"})
        self.assertEqual(self.sources(self.cli.add_nomadnet(FILE, wrap=True)["cid"]), {USER})

    def test_app_job_pins_with_its_source(self):
        job = self.app.submit_job("add", nomadnet=FILE, options={"wrap": True})
        self.assertEqual(job["source"], "app:wiki")
        job = self.app.job(job["id"], wait=10)
        self.assertEqual(job["state"], "done", job)
        self.assertEqual(self.sources(job["result"]["cid"]), {"app:wiki"})
        self.node.app_rm("wiki")  # removing the app releases what its NomadNet add pinned too
        self.assertEqual(self.sources(job["result"]["cid"]), set())


class LocalOnlyTest(JobsBase):
    """An add job of a path stays as it was: no nomadnet in its record."""

    def test_path_add_record_unchanged(self):
        path = os.path.join(self.tmp.name, "f")
        with open(path, "wb") as f:
            f.write(b"x")
        job = self.finished(self.jobs.submit("add", path=path)["id"])
        self.assertNotIn("nomadnet", job)
        self.assertNotIn("nomadnet", json.dumps(self.saved()))


if __name__ == "__main__":
    unittest.main()
