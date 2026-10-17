"""Write tests/fixtures/wire/vectors.json: the wire known-answer vectors (see README.md), from old releases' own
encoders.

Each tree is run in a subprocess with that tree first on sys.path (and checked to be where rnfs comes from); the
runs must agree byte for byte, which also shows the wire didn't change between those releases. From the
repository root:

    for v in v0.1.3 v0.1.4; do mkdir /tmp/rnfs-$v && git archive $v | tar -x -C /tmp/rnfs-$v; done
    python3 tests/fixtures/wire/generate.py /tmp/rnfs-v0.1.3 /tmp/rnfs-v0.1.4

Run with "--one TREE", it prints one tree's vectors as JSON instead.
"""
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def vectors(tree: str) -> list[dict]:
    """Every case's vector from tree's own code (which must be first on sys.path)."""
    import wire_cases
    m = wire_cases.modules(tree + os.sep)
    out = []
    for name, kind, inputs in wire_cases.cases(m):
        data = wire_cases.wire(m, kind, inputs)
        assert wire_cases.parse(m, kind, wire_cases.unwire(m, kind, data), inputs) == inputs, name
        out.append({"name": name, "kind": kind, "inputs": wire_cases.to_json(inputs), "hex": data.hex()})
    return out


def main():
    """Run every tree, check they agree, write vectors.json with the first tree's version noted."""
    if sys.argv[1] == "--one":
        tree = os.path.abspath(sys.argv[2])
        sys.path.insert(0, tree)
        sys.path.insert(1, HERE)
        import RNS
        RNS.loglevel = RNS.LOG_CRITICAL
        import rnfs
        print(json.dumps({"version": rnfs.__version__, "vectors": vectors(tree)}))
        return
    runs = []
    for tree in sys.argv[1:]:
        result = subprocess.run([sys.executable, os.path.abspath(__file__), "--one", tree], check=True,
                                capture_output=True, text=True)
        runs.append(json.loads(result.stdout))
    for run in runs[1:]:
        assert run["vectors"] == runs[0]["vectors"], f"{run['version']} differs from {runs[0]['version']}"
    out = {"generated_by": [run["version"] for run in runs], "vectors": runs[0]["vectors"]}
    with open(os.path.join(HERE, "vectors.json"), "w", encoding="utf-8") as f:  # one vector per line
        f.write('{"generated_by": %s, "vectors": [\n' % json.dumps(out["generated_by"]))
        f.write(",\n".join(json.dumps(vector, ensure_ascii=False) for vector in out["vectors"]))
        f.write("\n]}\n")
    print(f"{len(out['vectors'])} vectors, identical from {', '.join(out['generated_by'])}")


if __name__ == "__main__":
    main()
