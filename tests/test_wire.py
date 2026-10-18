"""Wire known-answer tests: the current code's encoders give, byte for byte, what rnfs 0.1.3 and 0.1.4 put on the
wire, and its parsers read those bytes back (tests/fixtures/wire: /find, /provide, /blocks, /pin, keepers' /name,
announce app_data, name records, the LXMF field). The vectors were made by those releases' own code, so a change
that still round-trips with itself (an int width, a list for a tuple, a field order) fails here."""
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


if __name__ == "__main__":
    unittest.main()
