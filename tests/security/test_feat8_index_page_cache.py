"""FEAT-8: the file-index-node page had no lock or rate limit, unlike the display page.

Every visit ran `rnfs pin ls --json` (and a sealed check for each new file pin), however many visitors came at
once, so anyone reaching the NomadNet node could keep the daemon busy listing pins.

These tests run the whole page script with the CLI stood in for by a fake subprocess.run that records each
call. Fixed: the page renders at most once per CACHE_SECONDS (60 s), one request at a time (the display page's
lock); it serves the cached page, written atomically, while it is fresh or while another request holds the lock.
"""
import json
import os
import tempfile
import time
import unittest

from tests.security import _pages_helpers as helpers

CID_A = "bafkreiailvfecuuvt6tctx3owax2jic7y5af2ndriwo6xymcq2uwyut34a"
CID_B = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"


def pin(cid, name):
    """A complete file pin the operator made, as `rnfs pin ls --json` lists it."""
    return {"cid": cid, "name": name, "sources": ["user"], "complete": True, "type": "file"}


class IndexPageCacheTest(unittest.TestCase):
    """The whole file-index-node script, run repeatedly over one page data dir."""

    def setUp(self):
        """A page data dir, the pins the fake CLI lists, and the calls it records."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.data_dir = tmp.name
        self.pins = [pin(CID_A, "field-guide.pdf")]
        self.calls = []
        self.daemon_down = False

    def run_cli(self, argv, *args, **kwargs):
        """The fake CLI: pin ls lists self.pins, cat answers with a file's (unsealed) first bytes."""
        words = list(argv[1:])
        self.calls.append(words)
        if self.daemon_down:
            return helpers._Completed(1, stderr="rnfs: the daemon isn't running")
        if words[:3] == ["pin", "ls", "--json"]:
            return helpers._Completed(0, json.dumps(self.pins).encode("utf-8"))
        if words[0] == "cat":
            return helpers._Completed(0, b"%PDF-1.7\n")
        return helpers._Completed(1, stderr="unknown command")

    def page(self) -> str:
        """What the whole page prints now."""
        return helpers.exec_index_page(self.run_cli, self.data_dir)

    def cache_path(self) -> str:
        """The cached page's path in the data dir."""
        return os.path.join(self.data_dir, "page.mu")

    def age_cache(self, seconds):
        """Make the cached page seconds old."""
        then = time.time() - seconds
        os.utime(self.cache_path(), (then, then))

    def test_fresh_cache_is_served_without_running_rnfs(self):
        """A second visit within the cache time gets the same page, and the CLI isn't run again."""
        first = self.page()
        self.assertIn("`!field-guide.pdf`!", first)
        self.assertIn(["pin", "ls", "--json"], self.calls)
        self.calls.clear()
        self.pins.append(pin(CID_B, "notes.txt"))
        self.assertEqual(self.page(), first)
        self.assertEqual(self.calls, [])
        with open(self.cache_path(), encoding="utf-8") as f:
            self.assertEqual(f.read(), first)
        self.assertEqual(sorted(os.listdir(self.data_dir)), ["lock", "page.mu", "sealed.json"])  # no temp file

    def test_stale_cache_is_rendered_again(self):
        """Once the cached page is 60 s old, the next visit lists the pins afresh."""
        self.page()
        self.pins.append(pin(CID_B, "notes.txt"))
        self.age_cache(59)
        self.assertNotIn("notes.txt", self.page())
        self.age_cache(61)
        self.calls.clear()
        page = self.page()
        self.assertIn("`!notes.txt`!", page)
        self.assertIn(["pin", "ls", "--json"], self.calls)
        # A cache dated in the future (the clock went back) isn't trusted either
        self.pins.append(pin("bafkreie5737gdxlw5i64vzichcalba3z2v5mzd7mo7kzkwqvzrmxl5ofwu", "later.txt"))
        then = time.time() + 3600
        os.utime(self.cache_path(), (then, then))
        self.assertIn("`!later.txt`!", self.page())

    @unittest.skipIf(os.name == "nt", "holds the lock with fcntl")
    def test_busy_lock_serves_the_cache(self):
        """While another request holds the lock, a visit gets the cached page however old, without running the
        CLI; with no cached page at all it is asked to try again."""
        import fcntl  # noqa: TID251 - skipped on Windows
        first = self.page()
        self.age_cache(3600)
        self.calls.clear()
        with open(os.path.join(self.data_dir, "lock"), "a+") as held:
            fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertEqual(self.page(), first)
            os.remove(self.cache_path())
            busy = self.page()
        self.assertEqual(self.calls, [])
        self.assertIn("busy", busy)
        self.assertNotIn("field-guide.pdf", busy)

    def test_failure_is_not_cached(self):
        """With the daemon down, visitors get the generic message, and the next visit tries again."""
        self.daemon_down = True
        self.assertIn("The file list isn't available right now", self.page())
        self.assertFalse(os.path.exists(self.cache_path()))
        self.daemon_down = False
        self.assertIn("`!field-guide.pdf`!", self.page())


if __name__ == "__main__":
    unittest.main()
