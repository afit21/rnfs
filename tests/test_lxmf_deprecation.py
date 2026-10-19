"""Tests for LXMF's deprecation in 0.2.0 (rnfs/features/lxmf.py moves to an app of its own; roadmap "LXMF moves out
of core"): LXMF keeps working exactly as before, and says it is deprecated wherever it is used. The API's routes
carry a Deprecation header and capabilities list the feature. No Reticulum: offline nodes and a stand-in for
LxmfNetwork, as in tests/test_lxmf.py."""
import json
import unittest
from unittest import mock

from rnfs import api, features
from rnfs.api import ApiServer
from rnfs.config import LXMF_DEPRECATION
from rnfs.features import lxmf

from tests.test_api import raw_request
from tests.test_lxmf import SENDER, FeatureTestCase, setUpModule, tearDownModule  # noqa: F401 - the log's level


class ApiTest(FeatureTestCase):
    """Every LXMF route answers as before, with a Deprecation header (RFC 9745); no other route has one."""

    def setUp(self):
        """Node B with the lxmf feature on a stand-in network, served by an ApiServer."""
        super().setUp()
        self.feature = self.feature_node()
        self.server = ApiServer(self.b, 0)
        self.server.start()
        self.addCleanup(self.server.stop)

    def call(self, method: str, path: str, body: dict | None = None) -> tuple[int, object, dict]:
        """(status, headers, JSON body) of a request with the token."""
        headers = {"X-Rnfs-Api": self.server.token}
        data = None
        if method == "POST":
            data = json.dumps(body or {}).encode()
            headers["Content-Type"] = "application/json"
        status, response_headers, raw = raw_request(self.server.port, method, path, data, headers)
        return status, response_headers, json.loads(raw)

    def test_lxmf_routes_carry_the_header_and_answer_as_before(self):
        """Each route (successes and errors alike) carries `Deprecation: @<the date>`; bodies are unchanged."""
        since = f"@{lxmf.LxmfFeature.deprecated_since}"
        requests = {("GET", "/api/lxmf/address"): (None, 200, self.feature.address()),
                    ("POST", "/api/lxmf/send"): ({"to": SENDER.hex(), "target": str(self.cid)}, 200, None),
                    ("GET", "/api/lxmf/inbox"): (None, 200, {"messages": []}),
                    ("POST", "/api/lxmf/inbox/get"): ({"id": 7}, 404, None),  # no such message: an error has it too
                    ("POST", "/api/lxmf/inbox/rm"): ({"all": True}, 200, {"removed": 0}),
                    ("GET", "/api/lxmf/outbox"): (None, 200, None),
                    ("POST", "/api/lxmf/sync"): ({}, 200, {"propagation_node": None, "started": False})}
        self.assertEqual(set(requests), set(lxmf.LxmfFeature.api_routes()))  # every route is tried
        for (method, path), (body, status, expected) in requests.items():
            with self.subTest(path=path):
                got, headers, answer = self.call(method, path, body)
                self.assertEqual(got, status, answer)
                self.assertEqual(headers.get_all("Deprecation"), [since])
                if expected is not None:
                    self.assertEqual(answer, expected)
        sent, = self.feature.outbox_list()
        self.assertEqual(self.call("GET", "/api/lxmf/outbox")[2], {"messages": [sent]})
        self.assertEqual(set(self.call("POST", "/api/lxmf/inbox/get", {"id": 7})[2]), {"error", "code"})

    def test_other_routes_have_no_header(self):
        """Core routes, other features' routes, an LXMF path asked with the wrong method and an unknown path."""
        for method, path in (("GET", "/api/status"), ("GET", "/api/capabilities"), ("GET", "/api/config"),
                             ("GET", "/api/peers"), ("GET", "/api/contacts"), ("POST", "/api/lxmf/address"),
                             ("GET", "/api/lxmf/nope")):
            with self.subTest(path=path):
                self.assertIsNone(self.call(method, path)[1].get("Deprecation"))

    def test_capabilities_list_lxmf_as_deprecated(self):
        """capabilities' "deprecated" names the lxmf feature, which is still loaded and offers every route."""
        result = self.call("GET", "/api/capabilities")[2]
        self.assertEqual(result["deprecated"], ["lxmf"])
        self.assertIn("lxmf", result["features"])
        self.assertLessEqual({tuple(route) for route in lxmf.LxmfFeature.api_routes()},
                             {tuple(route) for route in result["routes"]})


class FeatureDeclarationTest(unittest.TestCase):
    """Feature.deprecated: the lxmf feature declares it with a date; nothing else is deprecated."""

    def test_lxmf_is_the_deprecated_feature(self):
        self.assertEqual(features.deprecated_features(), ["lxmf"])
        self.assertEqual(lxmf.LxmfFeature.deprecated, LXMF_DEPRECATION)
        self.assertEqual(features.deprecated_routes(),
                         dict.fromkeys(lxmf.LxmfFeature.api_routes(), lxmf.LxmfFeature.deprecated_since))
        self.assertEqual(api.deprecated_routes(), features.deprecated_routes())

    def test_a_malformed_declaration_is_refused(self):
        """A feature whose deprecated isn't a reason, or whose date isn't whole seconds, doesn't load."""
        for deprecated, since in (("", 1), (True, 1), ("why", -1), ("why", 1.5), ("why", True)):
            with self.subTest(deprecated=deprecated, since=since):
                bad = type("Bad", (features.Feature,), {"name": "bad", "deprecated": deprecated,
                                                        "deprecated_since": since})
                with mock.patch.object(features, "feature_classes", return_value=[bad]), \
                        self.assertRaisesRegex(features.FeatureError, "deprecated"):
                    features.checked_classes()


if __name__ == "__main__":
    unittest.main()
