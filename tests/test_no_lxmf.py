"""rnfs runs without the LXMF package: LXMF moved out of rnfs in 0.2.0 into an app of its own, and LXMF is no longer
one of its dependencies (roadmap "LXMF moves out of core").

The package may still be installed (an older rnfs needed it, and an LXMF app does), so a stray `import LXMF` would
pass unnoticed: no module of rnfs imports it, and a fresh interpreter where importing it fails imports every rnfs
module, builds the CLI and runs a node with an rnfs.toml still holding an lxmf_* setting."""
import ast
import os
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Run in a fresh interpreter: LXMF can't be imported there, so anything that tries fails
WITHOUT_LXMF = """
import importlib, io, os, pkgutil, sys, tempfile
sys.modules["LXMF"] = sys.modules["lxmf"] = None  # `import LXMF` raises ImportError
import rnfs
for module in pkgutil.walk_packages(rnfs.__path__, "rnfs."):
    if module.name == "rnfs.__main__":  # it runs the CLI
        continue
    importlib.import_module(module.name)
from rnfs import cli
parser = cli.build_parser()
for removed in ("send", "inbox", "outbox", "lxmf"):
    try:
        parser.parse_args([removed])
    except SystemExit:
        continue
    raise AssertionError(f"rnfs {removed} is still a command")
from rnfs.config import Config
from rnfs.node import RnfsNode
from tests.test_names import FakeNetwork
from tests.test_node import NetClient
with tempfile.TemporaryDirectory() as root:
    with open(os.path.join(root, "rnfs.toml"), "w", encoding="utf-8") as f:
        f.write("lxmf_enabled = true\\n")
    config = Config.load(root, slow="no")
    node = RnfsNode.offline(config, bytes([7]) * 16, NetClient(), FakeNetwork())
    try:
        node.add_stream(io.BytesIO(b"x" * 3000), name="f")
        assert "lxmf" not in node.features and "lxmf_address" not in node.status()
    finally:
        node.stop()
print([warning.split(" in ")[0] for warning in config.warnings if warning.startswith("ignoring")])
"""


def python_files(top: str):
    """Every .py file under top."""
    for directory, _, files in os.walk(top):
        for name in files:
            if name.endswith(".py"):
                yield os.path.join(directory, name)


class NoLxmfTest(unittest.TestCase):
    """rnfs neither imports nor needs LXMF."""

    def test_no_module_imports_lxmf(self):
        """No import statement in rnfs/ (or rnfs.py) names the LXMF package, at module level or in a function."""
        for path in [os.path.join(ROOT, "rnfs.py"), *python_files(os.path.join(ROOT, "rnfs"))]:
            with open(path, encoding="utf-8") as f:
                tree = ast.parse(f.read(), path)
            for node in ast.walk(tree):
                names = []
                if isinstance(node, ast.Import):
                    names = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom) and node.level == 0:
                    names = [node.module or ""]
                for name in names:
                    self.assertNotIn(name.split(".")[0].lower(), ("lxmf",), f"{path}:{node.lineno} imports {name}")

    def test_runs_without_the_package(self):
        """Where LXMF can't be imported: every module imports, the LXMF commands are gone, and a node starts,
        adds and stops, ignoring the lxmf_* setting with a warning."""
        result = subprocess.run([sys.executable, "-c", WITHOUT_LXMF], capture_output=True, encoding="utf-8",
                                cwd=ROOT, env={**os.environ, "PYTHONPATH": ROOT}, timeout=120)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip().splitlines()[-1], "['ignoring lxmf_enabled']")


if __name__ == "__main__":
    unittest.main()
