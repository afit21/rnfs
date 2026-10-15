"""H2: one anonymous view of the display-mu-file page made the node permanently pin (source "user") and
provide content the visitor chose. The CID comes from var_cid; the page ran stat, ls, `pin add` and cat on it
with no allowlist and no per-visitor limit. `pin add` pins USER (node.py:821-833), which the node then
provides; and with the default provide_strategy="roots", stat/cat alone already make a cached complete fetch
be provided, so dropping only the pin would not stop the seeding. That part is documented behaviour of
"roots" (kept as the default by the user's decision; "pinned" avoids it), tested as such below.

The fixed page shows only the operator's own complete pins (source user or folder) and CIDs in the operator's
allowlist file; for anything else it runs nothing but `rnfs pin ls`, which never fetches. Allowlisted CIDs are
read with a bounded `rnfs cat` (no pin, no daemon job). Requests run one at a time under a lock file and a
rate limit. The in-process tests run the page's real script against an offline victim node that could fetch
the CID from an offline provider (the NetClient double). The real-daemon variants are gated behind RNFS_E2E.
"""
import io
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

import RNS

from rnfs.content.cid import Cid
from rnfs.content.storage import USER
from tests.security import _pages_helpers as helpers

CONTENT = b"hello from the visitor's chosen content\n"
MICRON = b">Allowlisted page\nA `!bold`! word and a `[link`:/page/index.mu]\n"

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the node log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class PageTestCase(unittest.TestCase):
    """A provider A and a victim B that serves the page, and a data dir for the page."""

    def setUp(self):
        """Two offline nodes joined by a NetClient, and the page's data dir."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        _, self.provider, self.victim = helpers.make_nodes(self.tmp.name)
        self.addCleanup(self.provider.stop)
        self.addCleanup(self.victim.stop)
        self.data_dir = os.path.join(self.tmp.name, "page-data")
        os.makedirs(self.data_dir)

    def provide(self, content: bytes) -> str:
        """Add content on the provider and advertise it to the victim; its CID."""
        cid = self.provider.add_stream(io.BytesIO(content))["cid"]
        helpers.advertise(self.provider, self.victim, cid)
        return cid

    def view(self, cid, **kwargs):
        """Run the page for cid against the victim, with this test's data dir; (output, calls)."""
        return helpers.run_display_page(self.victim, cid, data_dir=self.data_dir, **kwargs)

    def page(self, name):
        """One of the page's module-level names (a constant or function), for this test's data dir."""
        return helpers.display_page_namespace(self.data_dir)[name]

    def stored_blocks(self, cid: str) -> int:
        """How many blocks of cid's DAG the victim stores."""
        return self.victim.storage.stat(Cid.parse(cid))["local_blocks"]


class DisplayPagePinTest(PageTestCase):
    """A visitor asks for a CID the operator never chose."""

    def setUp(self):
        """A small file on the provider, which the victim could fetch."""
        super().setUp()
        self.cid = self.provide(CONTENT)
        self.key = Cid.parse(self.cid)

    def test_page_view_does_not_pin_a_non_allowlisted_cid(self):
        """A single page view for a CID the operator never chose must not create a permanent user pin."""
        output, calls = self.view(self.cid)
        self.assertNotIn(CONTENT.decode(), output)  # nor serve it
        self.assertIn("isn't shared on this node", output)
        self.assertNotIn(USER, self.victim.storage.pins().get(self.key, set()))

    def test_page_view_does_not_make_the_node_provide(self):
        """After a page view, the node must not advertise itself as a provider of the visitor's CID."""
        self.view(self.cid)
        self.assertNotIn(self.key, self.victim.publisher.provided())

    def test_page_runs_nothing_that_fetches_a_cid_it_does_not_share(self):
        """The page only lists the CID's pins (which never fetches): no stat, ls, cat or pin add, so not
        even the root block is fetched, and the "roots" strategy has nothing to provide."""
        _, calls = self.view(self.cid)
        self.assertEqual(calls, [["pin", "ls", "--json", self.cid]])
        self.assertEqual(self.stored_blocks(self.cid), 0)

    def test_stat_and_cat_of_remote_content_are_provided_under_roots(self):
        """Documented behaviour (specification section 6, "Provide strategy", and section 7, "What a node
        serves"; README `provide_strategy`): under the default "roots", every complete download is provided,
        including reads through `rnfs stat` and `rnfs cat`, so anyone can learn what the node fetched. That is
        why the page runs neither for CIDs it doesn't share (the test above)."""
        self.victim.stat(self.cid)  # the old page's `rnfs stat`
        b"".join(self.victim.read(self.cid))  # the old page's `rnfs cat`
        self.assertEqual(self.victim.config.provide_strategy, "roots")
        self.assertIn(self.key, self.victim.publisher.provided())

    def test_pinned_strategy_does_not_provide_reads(self):
        """With provide_strategy = "pinned", the same reads are cache that is never provided (section 6)."""
        self.victim.config.provide_strategy = "pinned"
        self.victim.stat(self.cid)
        b"".join(self.victim.read(self.cid))
        self.victim.publisher.refresh()
        self.assertNotIn(self.key, self.victim.publisher.provided())

    def test_contact_pin_is_not_shown(self):
        """A complete pin that a contact asked for (a remote: source), not the operator, isn't shown."""
        b"".join(self.victim.read(self.cid))  # fetched and cached...
        self.victim.storage.pin(self.key, "remote:" + "ab" * 16)  # ...and pinned for a contact
        output, calls = self.view(self.cid)
        self.assertNotIn(CONTENT.decode(), output)
        self.assertIn("isn't shared on this node", output)
        self.assertEqual(calls, [["pin", "ls", "--json", self.cid]])


class OperatorPinTest(PageTestCase):
    """CIDs the operator pinned on the victim itself."""

    def test_operator_pin_is_shown_without_fetching_or_pinning(self):
        """A complete user pin is shown, read with a bounded raw cat; no pin add, ls or other command runs (status
        says whether the node is in private mode)."""
        cid = self.victim.add_stream(io.BytesIO(b"the operator's notes\n"))["cid"]
        output, calls = self.view(cid)
        self.assertIn("the operator's notes", output)
        self.assertEqual(calls, [["pin", "ls", "--json", cid], ["status"], ["stat", cid],
                                 ["cat", cid, "--raw", "--length", str(self.page("MAX_SIZE"))]])

    def test_sealed_operator_pin_is_not_shown(self):
        """A file sealed to this node is never decrypted for a visitor (cat --raw) and never shown."""
        helpers.give_identity(self.victim)
        cid = self.victim.add_stream(io.BytesIO(b"private plaintext\n"), to_self=True)["cid"]
        output, _ = self.view(cid)
        self.assertNotIn("private plaintext", output)
        self.assertIn("sealed", output)

    def test_folder_pin_is_refused_as_a_folder(self):
        """A pinned directory isn't a file to show, and nothing more than its pin listing (and the node's
        status) is read."""
        folder = os.path.join(self.tmp.name, "shared")
        os.makedirs(folder)
        with open(os.path.join(folder, "a.txt"), "wb") as f:
            f.write(b"a")
        cid = self.victim.add_path(folder)["cid"]
        output, calls = self.view(cid)
        self.assertIn("is a folder", output)
        self.assertEqual(calls, [["pin", "ls", "--json", cid], ["status"]])


class AllowlistTest(PageTestCase):
    """CIDs listed in the operator's allowlist file may be fetched, and only they are rendered as Micron."""

    def test_allowlisted_cid_is_fetched_and_rendered_as_micron(self):
        """An allowlisted CID the node doesn't have is read from the network and printed as Micron, as
        written; it is not pinned and no pin add (a daemon job) is started."""
        cid = self.provide(MICRON)
        helpers.allow(self.data_dir, cid)
        output, calls = self.view(cid)
        self.assertEqual(output, MICRON.decode())
        self.assertNotIn(["pin", "add", cid], calls)
        self.assertIn(["cat", cid, "--raw", "--length", str(self.page("MAX_SIZE"))], calls)
        self.assertNotIn(Cid.parse(cid), self.victim.storage.pins())

    def test_allowlist_ignores_comments_and_junk(self):
        """Comments, blank lines and words that aren't CIDs are skipped; a CID with a comment after it counts."""
        cid = self.provide(MICRON)
        with open(os.path.join(self.data_dir, "allowlist.txt"), "w", encoding="utf-8") as f:
            f.write(f"# pages I vouch for\n\n../etc/passwd\n--help\n{cid}  # my front page\n")
        self.assertEqual(self.page("allowlist")(), {cid})
        output, _ = self.view(cid)
        self.assertEqual(output, MICRON.decode())

    def test_allowlisted_cid_over_max_size_is_not_read(self):
        """MAX_SIZE still holds for allowlisted CIDs: a larger file is refused after stat (its root only)."""
        cid = self.provide(os.urandom(self.page("MAX_SIZE") + 1))
        helpers.allow(self.data_dir, cid)
        output, calls = self.view(cid)
        self.assertIn("only files up to", output)
        self.assertEqual([call[0] for call in calls], ["pin", "stat"])
        self.assertEqual(self.stored_blocks(cid), 1)  # the root, which stat needs for the size

    def test_allowlisted_timeout_shows_a_generic_message_and_logs_it(self):
        """If the read outlives the page's deadline the visitor gets a short message; the log says why."""
        cid = self.provide(MICRON)
        helpers.allow(self.data_dir, cid)
        output, _ = self.view(cid, timeout_on=("cat",))
        self.assertIn("can't be shown right now", output)
        with open(os.path.join(self.data_dir, "page.log"), encoding="utf-8") as f:
            self.assertIn("took longer than", f.read())

    def test_page_deadline_is_shared_by_its_commands(self):
        """The page gives all its rnfs commands together at most TIMEOUT seconds, not TIMEOUT each."""
        cid = self.victim.add_stream(io.BytesIO(b"notes\n"))["cid"]
        timeouts = []
        run = helpers.fake_rnfs(self.victim, [])
        clock = [1000.0]

        def slow_run(argv, *args, **kwargs):
            """Record the timeout the page passes, and let 30 seconds pass for each command."""
            timeouts.append(kwargs["timeout"])
            clock[0] += 30
            return run(argv, *args, **kwargs)

        with mock.patch.object(time, "monotonic", lambda: clock[0]):
            helpers.exec_display_page(slow_run, cid, self.data_dir)
        timeout = self.page("TIMEOUT")
        self.assertEqual(timeouts, [timeout, timeout - 30, timeout - 60, timeout - 90])


class LockAndRateLimitTest(PageTestCase):
    """Page requests run one at a time and at most RATE_LIMIT per RATE_WINDOW; both fail closed."""

    def setUp(self):
        """A CID the victim doesn't share: a view of it only lists pins."""
        super().setUp()
        self.cid = self.provide(CONTENT)

    def rate_file(self, stamps=None):
        """Write the rate state (if stamps is given); the stamps saved there."""
        path = os.path.join(self.data_dir, "rate.json")
        if stamps is not None:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(stamps, f)
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    def test_a_request_in_progress_makes_the_next_one_busy(self):
        """While another request holds the lock, the page runs no rnfs command and says it is busy."""
        lock = self.page("take_lock")()  # as a request in progress holds it
        self.assertIsNotNone(lock)
        with lock:
            output, calls = self.view(self.cid)
        self.assertIn("busy", output)
        self.assertEqual(calls, [])
        _, calls = self.view(self.cid)  # released: the next request runs
        self.assertEqual(calls, [["pin", "ls", "--json", self.cid]])

    def test_views_past_the_rate_limit_are_refused(self):
        """RATE_LIMIT views run; the next one, within RATE_WINDOW, runs no rnfs command."""
        for _ in range(self.page("RATE_LIMIT")):
            _, calls = self.view(self.cid)
            self.assertEqual(len(calls), 1)
        output, calls = self.view(self.cid)
        self.assertIn("too many requests", output)
        self.assertEqual(calls, [])

    def test_old_views_no_longer_count(self):
        """Views older than RATE_WINDOW are forgotten; junk and future stamps don't count either."""
        now = time.time()
        old = now - self.page("RATE_WINDOW") - 1
        self.rate_file([old] * self.page("RATE_LIMIT") + ["junk", now + 3600])
        _, calls = self.view(self.cid)
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(self.rate_file()), 1)  # only this view

    def test_a_full_window_is_refused(self):
        """RATE_LIMIT views within the window (e.g. from earlier requests) make the next one wait."""
        self.rate_file([time.time() - 1] * self.page("RATE_LIMIT"))
        output, calls = self.view(self.cid)
        self.assertIn("too many requests", output)
        self.assertEqual(calls, [])

    def test_an_unusable_data_dir_fails_closed(self):
        """If the page can't use its data dir (here a file is in the way), it runs nothing."""
        blocked = os.path.join(self.tmp.name, "not-a-dir")
        with open(blocked, "w", encoding="utf-8"):
            pass
        output, calls = helpers.run_display_page(self.victim, self.cid, data_dir=blocked)
        self.assertIn("isn't available right now", output)
        self.assertEqual(calls, [])

    def test_unsaved_rate_state_fails_closed(self):
        """If the rate state can't be saved, the page doesn't run without its limit."""
        os.makedirs(os.path.join(self.data_dir, "rate.json.tmp"))  # in the way of the atomic write
        output, calls = self.view(self.cid)
        self.assertIn("can't be shown right now", output)
        self.assertEqual(calls, [])

    def test_an_invalid_cid_runs_nothing_and_is_not_counted(self):
        """A var_cid that isn't a bare CID never reaches rnfs, the lock or the rate state."""
        output, calls = self.view("--help")
        self.assertIn("No valid RNFS CID", output)
        self.assertEqual(calls, [])
        self.assertFalse(os.path.exists(os.path.join(self.data_dir, "rate.json")))


# --- full-stack variant: a real daemon, driven through the real CLI ---

E2E = unittest.skipUnless(os.environ.get("RNFS_E2E") == "1", "set RNFS_E2E=1 to run")


@E2E
class DisplayPageDaemonTest(unittest.TestCase):
    """Run the actual page script against a real victim daemon served by a real provider."""

    def setUp(self):
        """A hub (transport), a provider and a victim daemon over loopback TCP, and the page's data dir."""
        from tests import test_e2e
        self.e2e = test_e2e
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.tmp.cleanup)
        self.daemons = []
        self.addCleanup(self._stop_all)
        tcp_port = helpers_free_port()
        self.hub = self._start("hub", test_e2e.HUB_CONFIG, tcp_port)
        bootstrap = ("--bootstrap", self.hub.node_id)
        self.provider = self._start("prov", test_e2e.NODE_CONFIG, tcp_port, *bootstrap)
        self.victim = self._start("victim", test_e2e.NODE_CONFIG, tcp_port, *bootstrap)
        self.data_dir = os.path.join(self.tmp.name, "page-data")
        os.makedirs(self.data_dir)
        self.commands = os.path.join(self.tmp.name, "commands.log")  # what the page ran, one per line

    def _start(self, name, config, tcp_port, *extra):
        """Start one daemon and remember it for cleanup."""
        daemon = self.e2e.Daemon(name, self.tmp.name, config, tcp_port, *extra)
        self.daemons.append(daemon)
        return daemon

    def _stop_all(self):
        """Stop every daemon."""
        for daemon in self.daemons:
            daemon.interrupt()
        for daemon in self.daemons:
            daemon.stop()

    def _shim(self, daemon):
        """A directory holding an `rnfs` executable that logs its arguments and drives daemon through the
        worktree's CLI."""
        bindir = os.path.join(self.tmp.name, f"bin-{daemon.name}")
        os.makedirs(bindir, exist_ok=True)
        shim = os.path.join(bindir, "rnfs")
        with open(shim, "w", encoding="utf-8") as f:
            f.write(f'#!/bin/sh\necho "$*" >> "{self.commands}"\nexec "{sys.executable}" "{self.e2e.MAIN}" '
                    f'--dir "{daemon.dir}" --api-port {daemon.api_port} "$@"\n')
        os.chmod(shim, 0o755)
        return bindir

    def _path(self, daemon):
        """PATH with daemon's shim first."""
        return self._shim(daemon) + os.pathsep + os.environ.get("PATH", "")

    def _run_page(self, daemon, cid):
        """Run the display-mu-file page as NomadNet would, against daemon, for var_cid=cid; its stdout."""
        env = {**os.environ, "PATH": self._path(daemon), "var_cid": cid, "RNFS_PAGE_DIR": self.data_dir}
        page = subprocess.run([sys.executable, helpers.DISPLAY_PAGE], env=env, capture_output=True, timeout=300)
        self.assertEqual(page.returncode, 0, page.stderr.decode("utf-8", "replace"))
        return page.stdout

    def _provide(self, content: bytes) -> str:
        """Add content on the provider and wait until the victim finds it; its CID."""
        path = os.path.join(self.tmp.name, "content.txt")
        with open(path, "wb") as f:
            f.write(content)
        cid = self.provider.out("add", path)
        self.e2e.wait_for(lambda: self.provider.node_id in self._providers(self.victim, cid), 60,
                          "victim to find the provider")
        return cid

    def _pinned(self, daemon):
        """CIDs daemon has pinned."""
        return [pin["cid"] for pin in json.loads(daemon.out("pin", "ls", "--json"))]

    def _ran(self):
        """The rnfs commands the page ran, as argument strings."""
        if not os.path.exists(self.commands):
            return []
        with open(self.commands, encoding="utf-8") as f:
            return f.read().splitlines()

    def _jobs(self, daemon):
        """daemon's job list lines (none if it has no jobs)."""
        return [line for line in daemon.out("jobs", "list").splitlines() if line.strip() and line != "no jobs"]

    def _providers(self, daemon, cid):
        """Provider node IDs daemon finds for cid."""
        result = daemon.cli("providers", cid, check=False)
        return result.stdout.decode("utf-8", "replace").split() if result.returncode == 0 else []

    def test_page_view_does_not_fetch_on_a_real_daemon(self):
        """The real page, run against a real daemon for a CID the operator never chose, only lists pins:
        it shows nothing, pins nothing and fetches nothing."""
        cid = self._provide(b"visitor chosen bytes\n")
        out = self._run_page(self.victim, cid)
        self.assertNotIn(b"visitor chosen bytes", out)
        self.assertIn(b"isn't shared on this node", out)
        self.assertEqual(self._ran(), [f"pin ls --json {cid}"])
        self.assertNotIn(cid, self._pinned(self.victim), "the page left a permanent pin for a visitor-chosen CID")

    def test_allowlisted_view_leaves_no_pin_or_job_on_a_real_daemon(self):
        """An allowlisted CID is fetched and rendered as Micron, with no pin and no daemon job left behind."""
        cid = self._provide(MICRON)
        helpers.allow(self.data_dir, cid)
        out = self._run_page(self.victim, cid)
        self.assertEqual(out, MICRON)
        self.assertNotIn(cid, self._pinned(self.victim))
        self.assertEqual(self._jobs(self.victim), [])
        self.assertFalse(any(line.startswith("pin add") for line in self._ran()))

    def test_page_timeout_leaves_no_job_on_the_daemon(self):
        """The old page SIGKILLed its foreground `rnfs pin add` on timeout and the daemon finished the job
        anyway. The page's read is now a plain `rnfs cat`: when the page's deadline kills it, no job is left
        queued or running and the CID is never pinned."""
        cid = self._provide(os.urandom(75 * 1024).hex().encode())  # 150 KB of text: several chunks
        helpers.allow(self.data_dir, cid)
        real_run = subprocess.run

        def run(argv, *args, **kwargs):
            """The real CLI, with the cat's deadline cut so short that the page kills it, as on a slow link."""
            if argv[1:2] == ["cat"]:
                kwargs["timeout"] = 0.05
            return real_run(argv, *args, **kwargs)

        with mock.patch.dict(os.environ, {"PATH": self._path(self.victim)}):
            output = helpers.exec_display_page(run, cid, self.data_dir)
        self.assertIn("can't be shown right now", output)
        self.assertTrue(any(line.startswith("cat ") for line in self._ran()))
        self.assertEqual(self._jobs(self.victim), [])
        # No `what`, so a timeout returns False (never pinned) instead of raising
        self.assertFalse(self.e2e.wait_for(lambda: cid in self._pinned(self.victim), 10))


def helpers_free_port():
    """A free local TCP port (support.free_port, imported lazily so the module loads without RNFS_E2E)."""
    from tests.support import free_port
    return free_port()


if __name__ == "__main__":
    unittest.main()
