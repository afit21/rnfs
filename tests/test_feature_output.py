"""Feature CLI commands escape what the daemon sends before printing it (follow, keepers, contacts, peers, stats,
jobs, deny): whatever answers on the API port mustn't drive the terminal. So do examples/rnfs_share.py and
rnfs.Client's log (FEAT-7; tests/security/test_feat7_example_and_client_escaping.py has the details)."""
import contextlib
import io
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

from rnfs import remote
from rnfs.features import contacts, deny, follow, jobs, keepers, peers

from tests.security.test_feat7_example_and_client_escaping import StandInNode, load_example

EVIL = "ab\x1b]0;owned\x07\x1b[2J"


class FeatureCliOutputTest(unittest.TestCase):
    """Every field of a response is printed escaped."""

    def output(self, run, response, args=None, tty=False) -> str:
        """What run prints (stdout, then stderr) when the daemon answers response to any request; tty: whether
        stderr is a terminal."""
        client = mock.Mock()
        client.get.return_value = response
        client.post.return_value = response
        out, err = io.StringIO(), io.StringIO()
        err.isatty = lambda: tty
        with mock.patch("rnfs.cli.client", return_value=client), mock.patch("sys.stdout", out), \
                mock.patch("sys.stderr", err):
            run(args or SimpleNamespace(name="x", nick="x", id="x", kind=None))
        self.assertTrue(out.getvalue())
        return out.getvalue() + err.getvalue()

    def assertEscaped(self, text: str):
        """No control character reaches the terminal."""
        self.assertNotIn("\x1b", text)
        self.assertNotIn("\x07", text)

    def test_follow(self):
        self.assertEscaped(self.output(follow.run_follow, {"name": EVIL}))
        self.assertEscaped(self.output(follow.run_following, {"following": [
            {"name": EVIL, "pinned": EVIL, "latest": EVIL, "state": EVIL}]}))

    def test_keepers(self):
        self.assertEscaped(self.output(keepers.run_keep, {"name": EVIL, "sequence": EVIL, "cid": EVIL}))
        self.assertEscaped(self.output(keepers.run_kept, {"kept": [
            {"name": EVIL, "sequence": EVIL, "cid": EVIL, "reasons": [EVIL]}]}))

    def test_contacts(self):
        self.assertEscaped(self.output(contacts.run_add, {"nick": EVIL, "kind": EVIL, "id": EVIL}))

    def test_peers(self):
        peer = {"node_id": EVIL, "mode": EVIL, "seniority": 0, "in_table": False, "last_heard": None, "rate": 0,
                "link": False, "interface": EVIL, "banned": False, "bad": 0, "hops": None, "sent": 0,
                "received": 0, "failed": 0}
        self.assertEscaped("\n".join(peers.format_peers([peer], 0.0)))
        # Every field the table prints as it came, numbers too
        peer.update(hops=EVIL, failed=EVIL, bad=EVIL)
        self.assertEscaped("\n".join(peers.format_peers([peer], 0.0)))

    def test_stats(self):
        totals = {"sent": 0, "responses": EVIL, "received": 0, "requests": EVIL, "answered": EVIL, "refused": EVIL,
                  "failed": EVIL}
        stats = {"since": 0, "totals": totals, "refused": {EVIL: EVIL}, "upload_budget": 1, "upload_budget_left": 1,
                 "interfaces": {EVIL: {"sent": 0, "received": 0}}}
        self.assertEscaped("\n".join(peers.format_stats(stats)))
        self.assertEscaped(self.output(peers.run_stats, stats, SimpleNamespace(json=False)))

    def test_jobs(self):
        job = {"id": EVIL, "kind": EVIL, "state": jobs.RUNNING, "target": EVIL, "output": EVIL, "from": EVIL,
               "blocks": EVIL, "error": EVIL, "result": {"cid": EVIL}}
        args = SimpleNamespace(id="0123abcd", background=True)
        self.assertEscaped(self.output(jobs.run_list, {"jobs": [job, {**job, "state": EVIL}]}))
        self.assertEscaped(self.output(jobs.run_show, {**job, "blocks_total": EVIL}, args))
        for state in (jobs.RUNNING, jobs.CANCELLED, EVIL):
            self.assertEscaped(self.output(jobs.run_cancel, {**job, "state": state}, args))
        self.assertEscaped(self.output(jobs.run_clear, {"removed": EVIL}, args))
        with mock.patch.object(jobs, "has_jobs", return_value=True):
            printed = self.output(lambda args: jobs.run_transfer(args, {}, None), job, args, tty=True)
        self.assertIn("follow it with", printed)
        self.assertEscaped(printed)
        for kind in ("get", "pin", "export"):
            self.assertEscaped(self.output(jobs.run_wait, {**job, "kind": kind, "state": jobs.DONE,
                                                            "result": {"output": EVIL, "cid": EVIL, "roots": [EVIL],
                                                                       "warnings": [EVIL]}}, args))
        with self.assertRaises(SystemExit) as failed:
            self.output(jobs.run_wait, {**job, "state": jobs.FAILED}, args)
        self.assertEscaped(str(failed.exception.code))

    def test_jobs_progress_line(self):
        """The progress line a foreground transfer shows on a terminal, and the line Ctrl+C prints."""
        api = mock.Mock()
        api.get.side_effect = [{"state": jobs.RUNNING, "blocks": EVIL}, KeyboardInterrupt]
        err = io.StringIO()
        err.isatty = lambda: True
        with mock.patch("sys.stderr", err), self.assertRaises(SystemExit):
            jobs.follow(api, EVIL, cancel_on_interrupt=True)
        self.assertIn("finding the target", err.getvalue())
        self.assertEscaped(err.getvalue())

    def test_deny(self):
        args = SimpleNamespace(entries=["x"])
        self.assertEscaped(self.output(deny.run_add, {"added": [EVIL]}, args))
        self.assertEscaped(self.output(deny.run_rm, {"removed": [EVIL]}, args))
        self.assertEscaped(self.output(deny.run_ls, {"entries": [EVIL], "lists": [
            {"source": EVIL, "cid": EVIL, "entries": EVIL, "state": EVIL}]}, args))


class ExampleAndClientOutputTest(FeatureCliOutputTest.__base__):
    """FEAT-7: examples/rnfs_share.py prints names escaped, and rnfs.Client logs the daemon's messages escaped."""

    assertEscaped = FeatureCliOutputTest.assertEscaped

    def test_example(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as folder, contextlib.redirect_stdout(out):
            load_example().share(StandInNode(), folder)
        self.assertIn("evil\\x1b", out.getvalue())
        self.assertEscaped(out.getvalue())

    def test_client_log(self):
        with self.assertLogs("rnfs", "ERROR") as logged:
            remote.Client._report(None, EVIL)
        self.assertEscaped("\n".join(logged.output))


if __name__ == "__main__":
    unittest.main()
