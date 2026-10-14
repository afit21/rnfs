"""H2: one anonymous view of the display-mu-file page makes the node permanently pin (source "user") and
provide content the visitor chose. The CID comes from var_cid; the page runs stat, ls, `pin add` and cat on
it with no allowlist and no per-visitor limit (index.mu:70-100). `pin add` pins USER (node.py:821-833), which
the node then provides; and with the default provide_strategy="roots", the page's stat/cat alone already make
a cached complete fetch be provided, so dropping only the pin would not stop the seeding.

The in-process tests run the page's real script against an offline victim node that fetches from an offline
provider (the NetClient double). The job-leak variant (the daemon job surviving the page's SIGKILL) needs the
real daemon/CLI process split and is gated behind RNFS_E2E.
"""
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest

import RNS

from rnfs.content.cid import Cid
from rnfs.content.storage import USER
from tests.security import _pages_helpers as helpers


_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the node log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class DisplayPagePinTest(unittest.TestCase):
    """The display page driven against an in-process victim that can fetch the CID from a provider."""

    def setUp(self):
        """A provider A holding a small file, and a victim B that serves the page."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        _, self.provider, self.victim = helpers.make_nodes(self.tmp.name)
        self.addCleanup(self.provider.stop)
        self.addCleanup(self.victim.stop)
        self.cid = self.provider.add_stream(io.BytesIO(b"hello from the visitor's chosen content\n"))["cid"]
        helpers.advertise(self.provider, self.victim, self.cid)
        self.key = Cid.parse(self.cid)

    @unittest.expectedFailure  # H2: fails until the page only pins CIDs the operator allowlisted
    def test_page_view_does_not_pin_a_non_allowlisted_cid(self):
        """A single page view for a CID the operator never chose must not create a permanent user pin."""
        output, calls = helpers.run_display_page(self.victim, self.cid)
        self.assertIn(b"hello from the visitor's chosen content\n", output.encode())  # the page served it
        self.assertNotIn(USER, self.victim.storage.pins().get(self.key, set()))

    @unittest.expectedFailure  # H2: fails until a page view for a non-allowlisted CID never makes the node provide
    def test_page_view_does_not_make_the_node_provide(self):
        """After a page view, the node must not advertise itself as a provider of the visitor's CID."""
        helpers.run_display_page(self.victim, self.cid)
        self.assertNotIn(self.key, self.victim.publisher.provided())

    @unittest.expectedFailure  # H2: fails until the page's read path (stat/cat) does not provide either
    def test_stat_and_cat_alone_make_the_node_provide(self):
        """Removing only the `pin add` isn't enough: with provide_strategy="roots", the page's remaining
        stat and cat calls make the complete cached fetch be provided."""
        self.victim.stat(self.cid)  # the page's `rnfs stat`
        b"".join(self.victim.read(self.cid))  # the page's `rnfs cat`
        self.assertNotIn(self.key, self.victim.publisher.provided())


# --- full-stack variant: a real daemon, driven through the real CLI, with the page's SIGKILL ---

E2E = unittest.skipUnless(os.environ.get("RNFS_E2E") == "1", "set RNFS_E2E=1 to run")


@E2E
class DisplayPageDaemonTest(unittest.TestCase):
    """Run the actual page script as a subprocess against a real victim daemon served by a real provider."""

    def setUp(self):
        """A hub (transport), a provider and a victim daemon over loopback TCP."""
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
        """A directory holding an `rnfs` executable that drives daemon through the worktree's CLI."""
        bindir = os.path.join(self.tmp.name, f"bin-{daemon.name}")
        os.makedirs(bindir, exist_ok=True)
        shim = os.path.join(bindir, "rnfs")
        with open(shim, "w", encoding="utf-8") as f:
            f.write(f'#!/bin/sh\nexec "{sys.executable}" "{self.e2e.MAIN}" '
                    f'--dir "{daemon.dir}" --api-port {daemon.api_port} "$@"\n')
        os.chmod(shim, 0o755)
        return bindir

    def _run_page(self, daemon, cid, env_extra=None):
        """Run the display-mu-file page as NomadNet would, against daemon, for var_cid=cid."""
        env = {**os.environ, "PATH": self._shim(daemon) + os.pathsep + os.environ.get("PATH", ""),
               "var_cid": cid, **(env_extra or {})}
        return subprocess.Popen([sys.executable, helpers.DISPLAY_PAGE], env=env,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    @unittest.expectedFailure  # H2: fails until the page does not permanently pin non-allowlisted CIDs
    def test_page_view_pins_as_user_on_a_real_daemon(self):
        """The real page, run against a real daemon, leaves a permanent user pin for the visitor's CID."""
        path = os.path.join(self.tmp.name, "content.txt")
        with open(path, "wb") as f:
            f.write(b"visitor chosen bytes\n")
        cid = self.provider.out("add", path)
        self.e2e.wait_for(lambda: self.provider.node_id in self._providers(self.victim, cid), 60,
                          "victim to find the provider")
        page = self._run_page(self.victim, cid)
        out, err = page.communicate(timeout=180)
        self.assertEqual(page.returncode, 0, err.decode("utf-8", "replace"))
        self.assertIn(b"visitor chosen bytes", out)
        pins = {pin["cid"]: pin for pin in json.loads(self.victim.out("pin", "ls", "--json"))}
        self.assertNotIn(cid, pins, "the page left a permanent pin for a visitor-chosen CID")

    @unittest.expectedFailure  # H2: fails until killing the page's pin add cancels the daemon job
    def test_killed_pin_add_still_pins_on_the_daemon(self):
        """The page pins with `rnfs pin add` and SIGKILLs it on timeout; the daemon finishes the pin anyway.

        The page runs `rnfs pin add` as a subprocess and SIGKILLs it when it times out. follow() cancels the
        daemon job only on KeyboardInterrupt, so SIGKILL leaves the job running: the daemon fetches and
        permanently pins the content even though the page's CLI is gone. Here the CLI is SIGKILLed as soon as
        its pin job is running, and the victim still ends up with the CID pinned.
        """
        path = os.path.join(self.tmp.name, "big.bin")
        with open(path, "wb") as f:
            f.write(os.urandom(3 * 1024 * 1024))  # large enough that the fetch is still running when killed
        cid = self.provider.out("add", path)
        self.e2e.wait_for(lambda: self.provider.node_id in self._providers(self.victim, cid), 60,
                          "victim to find the provider")
        proc = subprocess.Popen(
            [sys.executable, self.e2e.MAIN, "--dir", self.victim.dir, "--api-port", str(self.victim.api_port),
             "pin", "add", cid],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, **self.e2e.POPEN_OPTIONS)
        try:
            self.e2e.wait_for(lambda: self._active_targets(self.victim), 30, "the pin job to start running")
            proc.kill()  # SIGKILL, as subprocess.run(timeout=...) does on timeout
            proc.wait(timeout=20)
            # No `what`, so a timeout returns False (not pinned) instead of raising
            pinned = self.e2e.wait_for(
                lambda: cid in [pin["cid"] for pin in json.loads(self.victim.out("pin", "ls", "--json"))], 60)
            self.assertFalse(pinned, "the daemon pinned the content after the page's CLI was killed")
        finally:
            if proc.poll() is None:
                proc.kill()

    def _active_targets(self, daemon):
        """Targets of the daemon's queued or running jobs."""
        lines = daemon.out("jobs", "list").splitlines()
        return [line.split()[-1] for line in lines
                if len(line.split()) >= 3 and line.split()[1] in ("queued", "running")]

    def _providers(self, daemon, cid):
        """Provider node IDs daemon finds for cid."""
        result = daemon.cli("providers", cid, check=False)
        return result.stdout.decode("utf-8", "replace").split() if result.returncode == 0 else []


def helpers_free_port():
    """A free local TCP port (support.free_port, imported lazily so the module loads without RNFS_E2E)."""
    from tests.support import free_port
    return free_port()


if __name__ == "__main__":
    unittest.main()
