"""AUD-L1 (0.2.1 audit): pairing brought back the REV9 app-name oracle.

POST /api/apps/request needs no token, and PairingRequests.request refused a name an app already had (400, "there is
an app called <name> already") while it gave any other valid name a request (200). Any local process could so try
names and list the installed apps, which REV9 had stopped the proof from telling (tests/security/
test_rev9_app_name_oracle.py).

The test rebuilds it through an offline node's API holding the app "wiki": requests without a token for "wiki" and
for names no app has must be answered alike (status, header names, body fields and their shapes). Now request never
looks at the apps; a name already taken is refused only when the user approves (PairingRequests.approve, through the
CLI's token), with a message saying it is a name clash, and the app holding the name keeps its token.
"""
import json
import re
import unittest

from rnfs.apiclient import ApiConnection, BadRequest
from tests import test_apps
from tests.test_api import raw_request

ID = re.compile(r"[0-9a-f]{16}")
CODE = re.compile(r"[0-9]{6}")


class PairingNameOracleTest(unittest.TestCase):
    """An offline node behind its API, holding the app "wiki" (AppPinsTest's)."""

    setUp = test_apps.AppPinsTest.setUp

    def ask(self, name: str):
        """(status, header names, body fields, the body) of an app asking for a token under name, with no token."""
        status, headers, data = raw_request(self.server.port, "POST", "/api/apps/request", b"{}",
                                            {"X-Rnfs-Pairing": name})
        body = json.loads(data)
        return status, sorted(n.lower() for n in headers.keys()), sorted(body), body

    def test_installed_and_missing_names_answer_alike(self):
        status, names, fields, body = self.ask("wiki")
        self.assertEqual(status, 200, body)  # 400 "there is an app called wiki already" before the fix
        for other in ("ghost", "notes", "wiki2"):
            with self.subTest(name=other):
                other_status, other_names, other_fields, other_body = self.ask(other)
                self.assertEqual((other_status, other_names, other_fields), (status, names, fields))
                for answer in (body, other_body):
                    self.assertRegex(answer["id"], ID)
                    self.assertRegex(answer["code"], CODE)
                    self.assertEqual(len(answer["secret"]), len(body["secret"]))

    def test_a_taken_name_is_refused_at_approval_as_a_clash(self):
        asked = ApiConnection(self.tmp.name, port=self.server.port).app_request("wiki")
        with self.assertRaises(BadRequest) as refused:
            self.cli.app_approve(asked["id"], code=asked["code"])
        self.assertIn("there is an app called wiki already", str(refused.exception))
        self.assertIn("name", str(refused.exception))
        self.assertTrue(self.node.apps.verify("wiki", self.token))  # the installed app keeps its token
        self.assertEqual([r["id"] for r in self.cli.app_requests()], [asked["id"]])


if __name__ == "__main__":
    unittest.main()
