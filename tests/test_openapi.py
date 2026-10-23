"""docs/openapi.json, the local API's OpenAPI reference, checked against the code as tests/test_api.py's
RouteTableTest checks rnfs/api.py's route tables: the same routes and nothing else, every error code, event kind and
capability, the security and headers routes need, and, for routes with simple shapes, the fields a live API over an
offline node actually answers. Standard library only: no OpenAPI or JSON Schema validator is used."""
import inspect
import json
import os
import re
import tempfile
import unittest

import RNS

import rnfs
from rnfs import api, errors, events, features
from rnfs.api import ApiHandler, ApiServer
from rnfs.config import Config
from tests.test_api import raw_request

DOCUMENT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs", "openapi.json")
METHODS = ("get", "put", "post", "delete", "options", "head", "patch", "trace")  # OpenAPI's operation keys
PIN_KEY = {"$ref": "#/components/parameters/PinKey"}
NODE_ID = bytes([0xB] * 16)
PEER_ID = bytes([0xA] * 16)

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the offline node's log."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


def load() -> dict:
    """The document, parsed."""
    with open(DOCUMENT, encoding="utf-8") as f:
        return json.load(f)


def operations(document: dict) -> dict[tuple[str, str], dict]:
    """(METHOD, path) -> operation, for every operation in the document."""
    return {(method.upper(), path): operation for path, item in document["paths"].items()
            for method, operation in item.items() if method in METHODS}


def resolve(document: dict, schema: dict) -> dict:
    """schema with any $ref (to this document) followed."""
    while "$ref" in schema:
        node = document
        for part in schema["$ref"].removeprefix("#/").split("/"):
            node = node[part]
        schema = node
    return schema


def fields(document: dict, schema: dict) -> tuple[dict, set]:
    """(properties, required) of an object schema, the parts of an allOf merged."""
    schema = resolve(document, schema)
    properties, required = dict(schema.get("properties", {})), set(schema.get("required", ()))
    for part in schema.get("allOf", ()):
        more, needed = fields(document, part)
        properties.update(more)
        required |= needed
    return properties, required


JSON_TYPES = {"object": dict, "array": list, "string": str, "boolean": bool, "null": type(None)}


def type_matches(value, types) -> bool:
    """Whether value is of one of the JSON Schema types (bool is no integer or number here, as in JSON)."""
    for name in [types] if isinstance(types, str) else types:
        if name in ("integer", "number"):
            if isinstance(value, (int, float)) and not isinstance(value, bool) and (
                    name == "number" or isinstance(value, int) or float(value).is_integer()):
                return True
        elif isinstance(value, JSON_TYPES[name]):
            return True
    return False


def mismatches(document: dict, value, schema: dict, where: str) -> list[str]:
    """What in value the schema doesn't document: undocumented fields, required fields missing and wrong types, in
    objects and in the objects inside arrays (oneOf and anyOf aren't followed: give the alternative)."""
    schema = resolve(document, schema)
    found = []
    if "type" in schema and not type_matches(value, schema["type"]):
        return [f"{where}: {type(value).__name__} {value!r} isn't {schema['type']}"]
    if isinstance(value, dict) and ("properties" in schema or "allOf" in schema):
        properties, required = fields(document, schema)
        found += [f"{where}: field {key!r} isn't documented" for key in sorted(value.keys() - properties.keys())]
        found += [f"{where}: required field {key!r} is missing" for key in sorted(required - value.keys())]
        for key in sorted(value.keys() & properties.keys()):
            found += mismatches(document, value[key], properties[key], f"{where}.{key}")
    elif isinstance(value, list) and "items" in schema:
        for index, item in enumerate(value):
            found += mismatches(document, item, schema["items"], f"{where}[{index}]")
    return found


class DocumentTest(unittest.TestCase):
    """The document against the code, statically."""

    @classmethod
    def setUpClass(cls):
        """The document and its operations."""
        cls.document = load()
        cls.operations = operations(cls.document)

    def assertSameSet(self, documented, actual, what: str):
        """documented equals actual, else a failure naming what is missing from the document and what is extra."""
        missing, extra = set(actual) - set(documented), set(documented) - set(actual)
        message = []
        if missing:
            message.append(f"{what} missing from docs/openapi.json: {sorted(missing)}")
        if extra:
            message.append(f"{what} in docs/openapi.json that the code doesn't have: {sorted(extra)}")
        if message:
            self.fail("; ".join(message))

    def test_version_and_status(self):
        """It describes this version, provisionally."""
        self.assertTrue(self.document["openapi"].startswith("3.1."))
        info = self.document["info"]
        self.assertEqual(info["version"], rnfs.__version__, "docs/openapi.json's info.version must be "
                                                            "rnfs.__version__")
        self.assertEqual(info["x-api-status"], rnfs.API_STATUS)
        self.assertIn("provisional", info["description"])

    def test_every_route_and_nothing_else(self):
        """The operations are exactly ApiHandler.ROUTES and the features' routes, each with a unique operationId."""
        self.assertSameSet(self.operations, set(ApiHandler.ROUTES) | set(features.api_routes()), "routes")
        ids = [operation["operationId"] for operation in self.operations.values()]
        self.assertEqual(len(ids), len(set(ids)), "an operationId is used twice")

    def test_feature_routes_name_their_feature(self):
        """x-rnfs-feature names the feature whose api_routes() has the route; core routes have none."""
        owner = {route: cls.name for cls in features.feature_classes() for route in cls.api_routes()}
        for route, operation in self.operations.items():
            with self.subTest(route=route):
                self.assertEqual(operation.get("x-rnfs-feature"), owner.get(route))
                if route in owner:
                    self.assertIn(owner[route], operation["tags"])

    def test_security(self):
        """Status is open, the app routes take the CLI's token only, and every other route the default (a token)."""
        self.assertEqual(self.document["security"], [{"token": []}, {"token": [], "app": []}])
        schemes = self.document["components"]["securitySchemes"]
        self.assertEqual((schemes["token"]["in"], schemes["token"]["name"]), ("header", "X-Rnfs-Api"))
        self.assertEqual((schemes["app"]["in"], schemes["app"]["name"]), ("header", "X-Rnfs-App"))
        for route, operation in self.operations.items():
            with self.subTest(route=route):
                if route in ApiHandler.OPEN_ROUTES:
                    self.assertIn({}, operation["security"])
                elif route in ApiHandler.CLI_ROUTES:
                    self.assertEqual(operation["security"], [{"token": []}])
                else:
                    self.assertNotIn("security", operation)
                self.assertIn("403", operation["responses"])
                self.assertIn("default", operation["responses"])

    def test_pin_key_on_the_routes_that_pin_for_the_caller(self):
        """X-Rnfs-Pin-Key is a parameter of exactly the routes whose handler pins with the caller's source."""
        handlers = {route: getattr(ApiHandler, name) for route, name in ApiHandler.ROUTES.items()}
        handlers.update(features.api_routes())
        for route, handler in handlers.items():
            with self.subTest(route=route):
                uses = "pin_source()" in inspect.getsource(handler)
                self.assertEqual(PIN_KEY in self.operations.get(route, {}).get("parameters", []), uses,
                                 "documented X-Rnfs-Pin-Key" if not uses else "X-Rnfs-Pin-Key isn't documented")

    def test_no_route_is_deprecated(self):
        """No operation is marked deprecated or documents a Deprecation header: rnfs deprecates nothing (0.2.0
        removed LXMF, the one feature it had deprecated, and the mechanism with it)."""
        for route, operation in self.operations.items():
            with self.subTest(route=route):
                self.assertNotIn("deprecated", operation)
                for status, response in operation["responses"].items():
                    self.assertNotIn("Deprecation", response.get("headers", {}), status)
        self.assertNotIn("Deprecation", self.document["components"].get("headers", {}))

    def test_error_codes(self):
        """ErrorCode lists every code of rnfs.errors.CODES, and nothing else, each described."""
        codes = self.document["components"]["schemas"]["ErrorCode"]["oneOf"]
        self.assertSameSet([code["const"] for code in codes], errors.CODES, "error codes")
        for code in codes:
            self.assertTrue(code.get("description"), code["const"])
        properties, required = fields(self.document, {"$ref": "#/components/schemas/Error"})
        self.assertEqual(set(properties), {"error", "code"})
        self.assertEqual(required, {"error", "code"})

    def test_event_kinds(self):
        """EventKind and the Event schemas cover rnfs.events.EVENT_KINDS, and nothing else."""
        schemas = self.document["components"]["schemas"]
        self.assertSameSet(schemas["EventKind"]["enum"], events.EVENT_KINDS, "event kinds")
        mapping = schemas["Event"]["discriminator"]["mapping"]
        self.assertSameSet(mapping, events.EVENT_KINDS, "Event schemas")
        for kind, target in mapping.items():
            with self.subTest(kind=kind):
                self.assertEqual(resolve(self.document, {"$ref": target})["properties"]["kind"], {"const": kind})
                self.assertIn({"$ref": target}, schemas["Event"]["oneOf"])

    def test_capabilities(self):
        """Capabilities' items list api.CAPABILITIES, and nothing else."""
        items = self.document["components"]["schemas"]["Capabilities"]["properties"]["capabilities"]["items"]
        self.assertSameSet(items["enum"], api.CAPABILITIES, "capabilities")

    def test_job_retry_is_opt_in(self):
        """POST /api/jobs' retry defaults to false (0.1.6's behaviour), and a job's retry is there only when true."""
        request = self.operations[("POST", "/api/jobs")]["requestBody"]["content"]["application/json"]["schema"]
        self.assertIs(resolve(self.document, request)["properties"]["retry"]["default"], False)
        self.assertIs(self.document["components"]["schemas"]["Job"]["properties"]["retry"]["const"], True)

    def test_every_reference_resolves(self):
        """Every $ref names something in the document."""
        def walk(node, where):
            """Check the references in node."""
            if isinstance(node, dict):
                if "$ref" in node:
                    try:
                        resolve(self.document, node)
                    except (KeyError, TypeError):
                        self.fail(f"{where}: {node['$ref']} names nothing")
                for key, value in node.items():
                    walk(value, f"{where}/{key}")
            elif isinstance(node, list):
                for index, value in enumerate(node):
                    walk(value, f"{where}/{index}")
        walk(self.document, "#")


class LiveTest(unittest.TestCase):
    """An ApiServer over an offline node (as tests/test_settings.py serves one): the fields its answers carry are
    the documented ones, required fields included, with the documented JSON types."""

    @classmethod
    def setUpClass(cls):
        """The document."""
        cls.document = load()
        cls.operations = operations(cls.document)

    def setUp(self):
        """An offline node with every feature, served by an ApiServer."""
        from rnfs.node import RnfsNode
        from tests.test_names import FakeNetwork
        from tests.test_node import NetClient
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.net = NetClient()
        self.net.sender = NODE_ID
        config = Config.load(os.path.join(self.tmp.name, "node"), slow="no", lookup_timeout=5, path_timeout=0.5)
        self.node = RnfsNode.offline(config, NODE_ID, self.net, FakeNetwork())
        self.addCleanup(self.node.stop)
        self.server = ApiServer(self.node, 0)
        self.server.start()
        self.addCleanup(self.server.stop)

    def call(self, method: str, path: str, body: dict | None = None, token: bool = True) -> object:
        """The JSON a request is answered with, checked to be a 200."""
        headers = {"X-Rnfs-Api": self.server.token} if token else {}
        data = None
        if method == "POST":
            data = json.dumps(body or {}).encode()
            headers["Content-Type"] = "application/json"
        status, _, raw = raw_request(self.server.port, method, path, data, headers)
        answer = json.loads(raw)
        self.assertEqual(status, 200, answer)
        return answer

    def check(self, method: str, path: str, answer, schema: dict | None = None):
        """answer has the fields the document gives route's 200 (or schema), and no others."""
        if schema is None:
            schema = self.operations[(method, path.split("?")[0])]["responses"]["200"]["content"]["application/json"][
                "schema"]
        # Each problem once, not once per list item: [3] becomes []
        problems = sorted({re.sub(r"\[\d+\]", "[]", problem)
                           for problem in mismatches(self.document, answer, schema, f"{method} {path}")})
        self.assertFalse(problems, "the answer doesn't match docs/openapi.json:\n" + "\n".join(problems))

    def test_status(self):
        """GET /api/status with the token (every feature's fields too), and without: only the version."""
        alternatives = self.operations[("GET", "/api/status")]["responses"]["200"]["content"]["application/json"][
            "schema"]["anyOf"]
        self.check("GET", "/api/status", self.call("GET", "/api/status"), alternatives[0])
        self.check("GET", "/api/status", self.call("GET", "/api/status", token=False), alternatives[1])

    def test_capabilities(self):
        """GET /api/capabilities."""
        self.check("GET", "/api/capabilities", self.call("GET", "/api/capabilities"))

    def test_pins_and_names(self):
        """GET /api/pin/ls and GET /api/name/list, with a pin and a name in them, and the calls making them."""
        source = os.path.join(self.tmp.name, "file.txt")
        with open(source, "wb") as f:
            f.write(b"hello")
        added = self.call("POST", "/api/add", {"path": source})
        self.check("POST", "/api/add", added)
        self.check("GET", "/api/pin/ls", pins := self.call("GET", "/api/pin/ls"))
        self.assertEqual(len(pins["pins"]), 1)
        self.check("POST", "/api/pin/label", self.call("POST", "/api/pin/label", {"cid": added["cid"], "label": "x"}))
        self.check("POST", "/api/name/create", self.call("POST", "/api/name/create", {"alias": "site"}))
        self.check("POST", "/api/name/publish", self.call("POST", "/api/name/publish",
                                                          {"alias": "site", "target": added["cid"]}))
        self.check("GET", "/api/name/list", names := self.call("GET", "/api/name/list"))
        self.assertEqual(len(names["names"]), 1)

    def test_remote_pins(self):
        """POST and GET /api/pin/remote, and GET /api/pin/remote/served."""
        cid = self.call("POST", "/api/add", {"path": os.path.abspath(__file__)})["cid"]
        self.check("POST", "/api/pin/remote", self.call("POST", "/api/pin/remote",
                                                        {"node": PEER_ID.hex(), "cid": cid, "pin": True}))
        self.check("GET", "/api/pin/remote", asked := self.call("GET", "/api/pin/remote"))
        self.assertEqual(len(asked["pins"]), 1)
        self.check("GET", "/api/pin/remote/served", self.call("GET", "/api/pin/remote/served"))

    def test_jobs(self):
        """POST /api/jobs with and without retry, and GET /api/jobs: retry is in a job only when submitted true."""
        cid = self.call("POST", "/api/add", {"path": os.path.abspath(__file__)})["cid"]
        job = self.call("POST", "/api/jobs", {"kind": "pin", "target": cid})
        self.check("POST", "/api/jobs", job)
        self.assertNotIn("retry", job)
        job = self.call("POST", "/api/jobs", {"kind": "pin", "target": cid, "retry": True})
        self.check("POST", "/api/jobs", job)
        self.assertIs(job["retry"], True)
        self.check("GET", "/api/jobs", self.call("GET", "/api/jobs"))

    def test_peers(self):
        """GET /api/peers with a peer in it, and GET /api/stats."""
        self.node.routing.heard(PEER_ID)
        self.check("GET", "/api/peers", peers := self.call("GET", "/api/peers"))
        self.assertEqual([peer["node_id"] for peer in peers["peers"]], [PEER_ID.hex()])
        self.check("GET", "/api/stats", self.call("GET", "/api/stats"))

    def test_config(self):
        """GET /api/config, every setting's entry; POST /api/config/set's dry run (0.2.1)."""
        self.check("GET", "/api/config", self.call("GET", "/api/config"))
        self.check("POST", "/api/config/set", self.call("POST", "/api/config/set",
                                                        {"settings": {"max_jobs": 3}, "dry_run": True}))

    def test_routing_ping_and_gc_preview(self):
        """GET /api/routing with a node and a ban in it, GET /api/ping, and POST /api/gc's dry run (0.2.1)."""
        self.node.routing.heard(PEER_ID)
        self.node.routing.ban(bytes([0xC] * 16), "a test")
        self.check("GET", "/api/routing", routing := self.call("GET", "/api/routing"))
        self.assertEqual((routing["nodes"], routing["bans"][0]["reason"]), (1, "a test"))
        self.check("GET", "/api/ping", self.call("GET", f"/api/ping?node={PEER_ID.hex()}"),
                   {"$ref": "#/components/schemas/PingResult"})
        self.call("POST", "/api/add", {"path": os.path.abspath(__file__)})
        self.check("POST", "/api/gc", self.call("POST", "/api/gc", {"dry_run": True}),
                   {"$ref": "#/components/schemas/GcPreview"})

    def test_apps(self):
        """POST /api/apps/add, GET /api/apps with the app in it, POST /api/apps/rm."""
        self.check("POST", "/api/apps/add", self.call("POST", "/api/apps/add", {"name": "notes"}))
        self.check("GET", "/api/apps", apps := self.call("GET", "/api/apps"))
        self.assertEqual(len(apps["apps"]), 1)
        self.check("POST", "/api/apps/rm", self.call("POST", "/api/apps/rm", {"name": "notes"}))

    def test_events(self):
        """GET /api/events after a pin and a cached fetch were made."""
        source = os.path.join(self.tmp.name, "file.txt")
        with open(source, "wb") as f:
            f.write(b"hello")
        self.call("POST", "/api/add", {"path": source})
        batch = self.call("GET", "/api/events?after=0")
        self.assertTrue(batch["events"])
        schemas = self.document["components"]["schemas"]
        for event in batch["events"]:
            target = schemas["Event"]["discriminator"]["mapping"][event["kind"]]
            self.check("GET", "/api/events", event, {"$ref": target})
        self.check("GET", "/api/events", batch)
