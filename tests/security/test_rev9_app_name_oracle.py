"""REV9: the proof told anyone who could reach the API port which app names exist. A request carrying
X-Rnfs-Challenge and X-Rnfs-App: <name> got X-Rnfs-Proof keyed by that app's verifier when there was an app called
name, and no proof at all when there wasn't (AppRegistry.proof_key answered None, ApiHandler.proof_key passed it on,
send_common_headers left the header out). The proof needs no token, so any local process could try names against
GET /api/status (or any route: every response of the API's own carries the proof, the 403, 404 and 405 ones too) and
read the installed apps off the header's presence.

The test rebuilds it with a real AppRegistry holding "wiki" behind an ApiServer, asking with no token or a wrong one,
for an existing name and for names there aren't, on the open status route, a route needing the token (403), an
unknown path (404) and a wrong method (405). Now a name there isn't gets a proof too, keyed by an HMAC of a per-start
random secret and the name (AppRegistry.proof_key): the same status, header names, body and proof shape as for an
existing app, the same proof each time it is asked within a start (so asking twice doesn't tell them apart), a
different one for each name, and one no client can check (it isn't keyed by the API token or any verifier).
"""
import json
import os
import re
import tempfile
import unittest

from rnfs.api import ApiServer
from rnfs.apiclient import APP_HEADER, CHALLENGE_HEADER, PROOF_HEADER, api_proof, app_proof_key
from rnfs.apps import AppRegistry
from tests.test_api import FakeNode, raw_request

CHALLENGE = "0123456789abcdef" * 2
PROOF = re.compile(r"[0-9a-f]{64}")
MISSING = ("ghost", "Wiki", "a:b", "wïki", "")  # no such app: valid names, invalid ones, non-ASCII, empty


class AppNameOracleTest(unittest.TestCase):
    """An ApiServer whose node has a real AppRegistry holding the app "wiki"."""

    def setUp(self):
        """The registry, the app and the server."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.state = os.path.join(tmp.name, "state")
        self.node = FakeNode()
        self.node.apps = AppRegistry(self.state)
        self.addCleanup(lambda: self.node.apps.close())
        self.wiki = self.node.apps.add("wiki")["token"]
        self.server = ApiServer(self.node, 0)
        self.server.start()
        self.addCleanup(self.server.stop)
        self.port = self.server.port

    def ask(self, method: str, path: str, app: str, token: str | None = None, **headers):
        """(status, header names, body, proof) of a request naming app, with the challenge and token if given."""
        headers = {CHALLENGE_HEADER: CHALLENGE, APP_HEADER: app, **headers}
        if token is not None:
            headers["X-Rnfs-Api"] = token
        body = b"{}" if method == "POST" else None
        status, response, data = raw_request(self.port, method, path, body, headers)
        return status, sorted(name.lower() for name in response.keys()), data, response[PROOF_HEADER]

    def requests(self):
        """(what, method, path, token, extra headers) of the requests an attacker without a token can make."""
        return [("status, no token", "GET", "/api/status", None, {}),
                ("status, wrong token", "GET", "/api/status", "wrong", {}),
                ("status, kept open", "GET", "/api/status", None, {"Connection": "keep-alive"}),
                ("a route needing the token (403)", "POST", "/api/gc", "wrong", {}),
                ("an unknown path (404)", "GET", "/api/nothing-here", None, {}),
                ("a wrong method (405)", "PUT", "/api/status", None, {})]

    def test_existing_and_missing_names_answer_alike(self):
        """For every response path, a name there isn't gets the same status, header names and body as "wiki", and
        a proof of the same shape: the response doesn't say whether the app exists."""
        for what, method, path, token, extra in self.requests():
            status, names, body, proof = self.ask(method, path, "wiki", token, **extra)
            self.assertIsNotNone(proof, what)
            self.assertRegex(proof, PROOF, what)
            for name in MISSING:
                with self.subTest(what=what, name=name):
                    other_status, other_names, other_body, other_proof = self.ask(method, path, name, token, **extra)
                    self.assertEqual((other_status, other_names, other_body), (status, names, body))
                    self.assertIsNotNone(other_proof, "no proof for an app there isn't: the name's absence shows")
                    self.assertRegex(other_proof, PROOF)

    def test_missing_names_proof_is_stable_within_a_start(self):
        """Asked again with the same challenge, a missing name's proof is the same, as an existing app's is, so
        asking twice tells them apart no more than asking once; each name gets its own."""
        proofs = {}
        for name in ("wiki", *MISSING):
            first = self.ask("GET", "/api/status", name)[3]
            self.assertIsNotNone(first, name)
            self.assertEqual(self.ask("GET", "/api/status", name)[3], first, name)
            proofs[name] = first
        self.assertEqual(len(set(proofs.values())), len(proofs))
        self.assertEqual(proofs["wiki"], api_proof(app_proof_key(self.wiki), self.port, CHALLENGE))

    def test_missing_names_proof_is_unverifiable(self):
        """No key a client could hold makes a missing name's proof: not the API token, not a verifier of the empty
        token or of the name itself; and it changes at the next start, while a real app's doesn't."""
        proof = self.ask("GET", "/api/status", "ghost")[3]
        for key in (self.server.token, app_proof_key(""), app_proof_key("ghost"), "ghost", ""):
            self.assertNotEqual(proof, api_proof(key, self.port, CHALLENGE), key)
        wiki = self.ask("GET", "/api/status", "wiki")[3]
        self.node.apps.close()
        self.node.apps = AppRegistry(self.state)  # a restart: the same apps, a new secret
        self.assertEqual(self.ask("GET", "/api/status", "wiki")[3], wiki)
        self.assertNotEqual(self.ask("GET", "/api/status", "ghost")[3], proof)

    def test_registry_answers_every_name(self):
        """AppRegistry.proof_key gives a key for every name; an existing app's is its verifier in hex."""
        self.assertEqual(self.node.apps.proof_key("wiki"), app_proof_key(self.wiki))
        for name in MISSING:
            self.assertRegex(self.node.apps.proof_key(name) or "", PROOF, name)
        self.assertEqual(json.loads(self.ask("GET", "/api/status", "ghost")[2]), json.loads(
            self.ask("GET", "/api/status", "wiki")[2]))


if __name__ == "__main__":
    unittest.main()
