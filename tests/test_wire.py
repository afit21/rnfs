"""Wire known-answer tests: the current code's encoders give, byte for byte, what rnfs 0.1.3 and 0.1.4 put on the
wire, and its parsers read those bytes back (tests/fixtures/wire: /find, /provide, /blocks, /pin, keepers' /name,
announce app_data, name records, the LXMF field). The vectors were made by those releases' own code, so a change
that still round-trips with itself (an int width, a list for a tuple, a field order) fails here. /range, which
0.2.0 added, is checked the same way against vectors 0.2.0 made (RangeVectorTest)."""
import importlib.util
import json
import os
import unittest

FIXTURE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "wire")


def _cases():
    """tests/fixtures/wire/wire_cases.py, which generate.py ran on the old trees."""
    spec = importlib.util.spec_from_file_location("wire_cases", os.path.join(FIXTURE, "wire_cases.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


wire_cases = _cases()


def _vectors() -> dict:
    """vectors.json."""
    with open(os.path.join(FIXTURE, "vectors.json"), encoding="utf-8") as f:
        return json.load(f)


class WireVectorTest(unittest.TestCase):
    """Every vector, encoded and parsed by the current code."""

    @classmethod
    def setUpClass(cls):
        """The vectors, and the current code's modules."""
        cls.vectors = _vectors()
        cls.m = wire_cases.modules()

    def test_vectors_come_from_released_code(self):
        """The vectors were made by 0.1.3 and 0.1.4, which agreed, and cover every case and kind."""
        self.assertEqual(self.vectors["generated_by"], ["0.1.3", "0.1.4"])
        names = [vector["name"] for vector in self.vectors["vectors"]]
        self.assertEqual(names, [name for name, _, _ in wire_cases.cases(self.m)])
        kinds = {vector["kind"] for vector in self.vectors["vectors"]}
        self.assertEqual(kinds, {"find_request", "find_response", "provide_request", "accept_response",
                                 "blocks_request", "blocks_response", "pin_request", "name_request",
                                 "name_response", "announce_mode", "name_record", "lxmf_field"})

    def test_encode_gives_the_released_bytes(self):
        """packb(encode(inputs)) is the vector's bytes (app_data: encode(inputs) is)."""
        for vector in self.vectors["vectors"]:
            with self.subTest(vector["name"]):
                inputs = wire_cases.from_json(self.m, vector["inputs"])
                self.assertEqual(wire_cases.wire(self.m, vector["kind"], inputs).hex(), vector["hex"])

    def test_parse_reads_the_released_bytes(self):
        """parse(unpackb(bytes)) is the vector's inputs."""
        for vector in self.vectors["vectors"]:
            with self.subTest(vector["name"]):
                inputs = wire_cases.from_json(self.m, vector["inputs"])
                data = wire_cases.unwire(self.m, vector["kind"], bytes.fromhex(vector["hex"]))
                self.assertEqual(wire_cases.parse(self.m, vector["kind"], data, inputs), inputs)

    def test_inputs_are_the_fixed_ones(self):
        """The inputs in vectors.json are those wire_cases builds now (so a changed case can't pass silently)."""
        built = {name: wire_cases.to_json(inputs) for name, _, inputs in wire_cases.cases(self.m)}
        for vector in self.vectors["vectors"]:
            with self.subTest(vector["name"]):
                self.assertEqual(vector["inputs"], built[vector["name"]])


def _range_cases():
    """tests/fixtures/wire/range_cases.py: the /range cases (rnfs 0.2.0)."""
    spec = importlib.util.spec_from_file_location("range_cases", os.path.join(FIXTURE, "range_cases.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


range_cases = _range_cases()


class RangeVectorTest(unittest.TestCase):
    """The /range vectors (range_vectors.json), which 0.2.0, the release adding /range, made: every later tree gives
    the same bytes and reads them back, as the 0.1.3 and 0.1.4 vectors above hold for the rest of the wire."""

    @classmethod
    def setUpClass(cls):
        """The vectors, and the current code's modules."""
        with open(os.path.join(FIXTURE, "range_vectors.json"), encoding="utf-8") as f:
            cls.vectors = json.load(f)
        cls.m = wire_cases.modules()

    def test_vectors_cover_every_case_and_kind(self):
        self.assertEqual(self.vectors["introduced_in"], "0.2.0")
        names = [vector["name"] for vector in self.vectors["vectors"]]
        self.assertEqual(names, [name for name, _, _ in range_cases.cases(self.m)])
        self.assertEqual({vector["kind"] for vector in self.vectors["vectors"]}, set(range_cases.KINDS))

    def test_encode_gives_the_bytes(self):
        for vector in self.vectors["vectors"]:
            with self.subTest(vector["name"]):
                inputs = wire_cases.from_json(self.m, vector["inputs"])
                self.assertEqual(range_cases.wire(self.m, vector["kind"], inputs).hex(), vector["hex"])

    def test_parse_reads_the_bytes(self):
        for vector in self.vectors["vectors"]:
            with self.subTest(vector["name"]):
                inputs = wire_cases.from_json(self.m, vector["inputs"])
                data = range_cases.unwire(self.m, vector["kind"], bytes.fromhex(vector["hex"]))
                self.assertEqual(range_cases.parse(self.m, vector["kind"], data, inputs), inputs)

    def test_inputs_are_the_fixed_ones(self):
        built = {name: wire_cases.to_json(inputs) for name, _, inputs in range_cases.cases(self.m)}
        for vector in self.vectors["vectors"]:
            with self.subTest(vector["name"]):
                self.assertEqual(vector["inputs"], built[vector["name"]])

    def test_answers_pass_the_response_precheck(self):
        """Every /range answer passes the check requesters since 0.1.4 run on rnfs.dht responses (a 0.2.0 requester
        asks only nodes that answer /range, but the check is the same code)."""
        for vector in self.vectors["vectors"]:
            if vector["kind"] != "range_request":
                with self.subTest(vector["name"]):
                    framed = self.m.umsgpack.packb([bytes(16), self.m.umsgpack.unpackb(bytes.fromhex(vector["hex"]))])
                    self.assertIsNone(self.m.protocol.response_refusal(framed))


if __name__ == "__main__":
    unittest.main()
