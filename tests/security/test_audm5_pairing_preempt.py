"""AUD-M5 (0.2.1 audit): pairing could be pre-empted, so another local user got the token meant for an app.

POST /api/apps/request needs no token (an app asking has none), and PairingRequests.request allowed one request
waiting per name and 8 in all. User B asked under the name "rnfs-desktop" first; the real RNFS Desktop was then
refused (400), so it had no code to show, and the user, seeing a request named as their app, approved it with
`rnfs app approve <id>`, which never asked for the code: B collected an unscoped app token. B could also fill the 8
places with junk names, and every app was refused pairing (503) for as long as B kept asking every 10 minutes.

The tests rebuild both, on PairingRequests and end to end through an offline node's API and the CLI. Now:
- a name may have several requests waiting, each with its own id and code, so the real app always gets one and
  shows its code; approving one denies the others under that name (their requesters learn "denied");
- approve takes the code the app shows (POST /api/apps/approve's `code`, ApiConnection.app_approve(code=),
  `rnfs app approve <id> --code`, and asked for at a terminal): a code that isn't the request's is refused (400)
  and the request stays waiting;
- a request is never refused for want of room: once MAX_PENDING wait, a new one takes the place of the oldest,
  so filling the table once no longer shuts pairing out (it takes a flood faster than the user answers, which the
  user sees in rnfs app requests).
"""
import contextlib
import io
import os
import sys
import tempfile
import unittest
from unittest import mock

from rnfs import apps, cli
from rnfs.apiclient import ApiConnection, BadRequest
from rnfs.apps import AppRegistry, PairingRequests
from rnfs.errors import NotFound
from tests import test_apps


class PreemptTest(unittest.TestCase):
    """PairingRequests on its own, with a clock the test moves."""

    def setUp(self):
        self.tmp = self.enterContext(tempfile.TemporaryDirectory())
        self.registry = AppRegistry(os.path.join(self.tmp, "state"))
        self.addCleanup(self.registry.close)
        self.now = 1000.0
        self.pairing = PairingRequests(self.registry, clock=lambda: self.now)
        quiet = mock.patch("rnfs.apps.log")
        quiet.start()
        self.addCleanup(quiet.stop)

    def test_the_real_app_still_gets_a_request_and_its_code(self):
        evil = self.pairing.request("rnfs-desktop", "RNFS Desktop")
        real = self.pairing.request("rnfs-desktop", "RNFS Desktop")  # refused (400) before the fix
        self.assertNotEqual(evil["id"], real["id"])
        self.assertEqual({r["id"] for r in self.pairing.pending()}, {evil["id"], real["id"]})

    def test_approve_needs_the_apps_code_and_denies_the_rest(self):
        with mock.patch.object(apps.secrets, "randbelow", side_effect=[111111, 222222]):
            evil = self.pairing.request("rnfs-desktop")
            real = self.pairing.request("rnfs-desktop")
        # The user reads the code off the real app and types it; the squatter's request doesn't match it
        with self.assertRaises(ValueError):
            self.pairing.approve(evil["id"], code=real["code"])
        self.assertEqual(self.pairing.state(evil["id"], evil["secret"])["state"], "pending")
        self.assertNotIn("rnfs-desktop", self.registry)
        for bad in ("", "22222", "2222222", " 222222", 222222):
            with self.subTest(code=bad), self.assertRaises(ValueError):
                self.pairing.approve(real["id"], code=bad)
        self.assertEqual(self.pairing.approve(real["id"], code=real["code"])["name"], "rnfs-desktop")
        answer = self.pairing.state(real["id"], real["secret"])
        self.assertTrue(self.registry.verify("rnfs-desktop", answer["token"]))
        # The squatter's request under the same name is denied with it, and can't be approved any more
        self.assertEqual(self.pairing.state(evil["id"], evil["secret"]), {"name": "rnfs-desktop", "state": "denied",
                                                                          "expires": self.now + apps.PICKUP_LIFE})
        with self.assertRaises(NotFound):
            self.pairing.approve(evil["id"], code=evil["code"])

    def test_filling_the_table_doesnt_shut_pairing_out(self):
        junk = [self.pairing.request(f"junk{n}") for n in range(apps.MAX_PENDING)]
        self.now += 1
        real = self.pairing.request("rnfs-desktop")  # Busy (503) before the fix
        waiting = [r["id"] for r in self.pairing.pending()]
        self.assertEqual(len(waiting), apps.MAX_PENDING)
        self.assertIn(real["id"], waiting)
        self.assertNotIn(junk[0]["id"], waiting)  # the oldest made room
        with self.assertRaises(NotFound):
            self.pairing.state(junk[0]["id"], junk[0]["secret"])
        self.assertEqual(self.pairing.approve(real["id"], code=real["code"])["name"], "rnfs-desktop")


class PreemptApiTest(unittest.TestCase):
    """The attack through an offline node's API (no token for the requests) and the user's CLI."""

    setUp = test_apps.AppPinsTest.setUp

    def run_cli(self, *argv, typed: str | None = None):
        """(exit code, stdout, stderr) of cli.main(argv); typed: what the user types at a terminal."""
        stdout, stderr = io.StringIO(), io.StringIO()
        stdin = io.StringIO(typed or "")
        stdin.isatty = lambda: typed is not None
        code = 0
        with mock.patch("sys.stdout", stdout), mock.patch("sys.stdin", stdin), contextlib.redirect_stderr(stderr):
            try:
                cli.main([*argv, "--api-port", str(self.server.port), "--dir", self.tmp.name])
            except SystemExit as e:
                if isinstance(e.code, str):
                    print(e.code, file=sys.stderr)
                    code = 1
                else:
                    code = e.code or 0
        return code, stdout.getvalue(), stderr.getvalue()

    def test_squatter_cant_get_the_apps_token(self):
        squatter = ApiConnection(self.tmp.name, port=self.server.port)
        real_app = ApiConnection(self.tmp.name, port=self.server.port)
        with mock.patch.object(apps.secrets, "randbelow", side_effect=[111111, 222222, 333333]):
            evil = squatter.app_request("rnfs-desktop", "RNFS Desktop")
            real = real_app.app_request("rnfs-desktop", "RNFS Desktop")
            other = squatter.app_request("rnfs-desktop", "RNFS Desktop")
        self.assertEqual(len(self.cli.app_requests()), 3)
        # Approving the squatter's request with the code the real app shows is refused, through the API and the CLI
        with self.assertRaises(BadRequest):
            self.cli.app_approve(evil["id"], code=real["code"])
        code, _, err = self.run_cli("app", "approve", evil["id"], "--code", real["code"])
        self.assertEqual(code, 1)
        self.assertIn("code", err)
        # At a terminal the CLI asks for the code the app shows
        code, _, err = self.run_cli("app", "approve", other["id"], typed=real["code"] + "\n")
        self.assertEqual(code, 1, err)
        self.assertIn("code", err)
        self.assertNotIn(other["code"], err)  # it asks for the code, rather than showing the one to type
        self.assertNotIn("rnfs-desktop", self.node.apps)
        code, out, err = self.run_cli("app", "approve", real["id"], typed=real["code"] + "\n")
        self.assertEqual(code, 0, err)
        self.assertIn("approved app rnfs-desktop", out)
        token = real_app.app_request_state(real["id"], real["secret"])["token"]
        self.assertTrue(self.node.apps.verify("rnfs-desktop", token))
        for request in (evil, other):
            self.assertEqual(squatter.app_request_state(request["id"], request["secret"])["state"], "denied")


if __name__ == "__main__":
    unittest.main()
