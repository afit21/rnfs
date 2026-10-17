"""Tests for dir set / dir rm (specification section 11): edits give the CID of the edited tree built afresh."""
import unittest
from unittest import mock

from rnfs import constants
from rnfs.content import builder, dag, edit, walker
from rnfs.content.cid import Cid
from rnfs.content.dag import Entry
from rnfs.errors import NotFound, TooLarge


class Blocks(dict):
    """In-memory blocks with put and a walker get."""

    def put(self, cid: Cid, block: bytes):
        """Store a block."""
        self[cid] = block

    def get(self, expect: walker.Expect) -> bytes:
        """The block, or NotFound."""
        if expect.cid not in self:
            raise NotFound(str(expect.cid))
        return self[expect.cid]


class EditTest(unittest.TestCase):
    """Adding, replacing and removing entries at a path."""

    def setUp(self):
        """A tree: root/{readme, docs/{a, b}}."""
        self.blocks = Blocks()
        self.readme = self.file(b"readme")
        self.a = self.file(b"a")
        self.b = self.file(b"b")
        self.docs = self.directory({"a": self.a, "b": self.b})
        self.root = self.directory({"readme": self.readme, "docs": self.docs})

    def file(self, data: bytes, kind: int = dag.KIND_FILE) -> builder.Built:
        """Build a file."""
        return builder.build_bytes(data, self.blocks.put, kind)

    def directory(self, children: dict[str, builder.Built]) -> builder.Built:
        """Build a directory from name -> built child."""
        entries = [Entry(name, child.cid, child.size, child.kind) for name, child in children.items()]
        return builder.build_directory(entries, self.blocks.put)

    def test_add_at_root(self):
        new = self.file(b"new")
        result = edit.dir_set(self.root.cid, [], "new", new.cid, False, self.blocks.get, self.blocks.put)
        expected = self.directory({"readme": self.readme, "docs": self.docs, "new": new})
        self.assertEqual(result, expected.cid)

    def test_replace_nested(self):
        c = self.file(b"c")
        result = edit.dir_set(self.root.cid, ["docs"], "a", c.cid, True, self.blocks.get, self.blocks.put)
        exec_c = builder.Built(c.cid, c.size, dag.KIND_EXECUTABLE, 0)
        expected = self.directory({"readme": self.readme, "docs": self.directory({"a": exec_c, "b": self.b})})
        self.assertEqual(result, expected.cid)
        entry = walker.resolve(result, ["docs", "a"], self.blocks.get)
        self.assertEqual((entry.cid, entry.kind), (c.cid, dag.KIND_EXECUTABLE))

    def test_set_directory(self):
        result = edit.dir_set(self.root.cid, ["docs"], "copy", self.docs.cid, False, self.blocks.get, self.blocks.put)
        docs = self.directory({"a": self.a, "b": self.b, "copy": self.docs})
        self.assertEqual(result, self.directory({"readme": self.readme, "docs": docs}).cid)
        with self.assertRaises(ValueError):
            edit.dir_set(self.root.cid, [], "x", self.docs.cid, True, self.blocks.get, self.blocks.put)

    def test_remove(self):
        result = edit.dir_rm(self.root.cid, ["docs", "a"], self.blocks.get, self.blocks.put)
        expected = self.directory({"readme": self.readme, "docs": self.directory({"b": self.b})})
        self.assertEqual(result, expected.cid)
        result = edit.dir_rm(self.root.cid, ["docs"], self.blocks.get, self.blocks.put)
        self.assertEqual(result, self.directory({"readme": self.readme}).cid)

    def test_errors(self):
        """A missing path is NotFound, naming it; a non-directory target, an invalid name or no path is ValueError."""
        get, put = self.blocks.get, self.blocks.put
        with self.assertRaises(NotFound):
            edit.dir_rm(self.root.cid, ["docs", "missing"], get, put)
        with self.assertRaises(NotFound) as caught:
            edit.dir_set(self.root.cid, ["docs", "missing"], "x", self.a.cid, False, get, put)
        self.assertEqual(str(caught.exception), f"{self.root.cid}/docs/missing does not exist")
        with self.assertRaises(ValueError):
            edit.dir_set(self.root.cid, ["readme"], "x", self.a.cid, False, get, put)  # not a directory
        with self.assertRaises(ValueError):
            edit.dir_set(self.readme.cid, [], "x", self.a.cid, False, get, put)
        with self.assertRaises(ValueError):
            edit.dir_set(self.root.cid, [], "a/b", self.a.cid, False, get, put)
        with self.assertRaises(ValueError):
            edit.dir_rm(self.root.cid, [], get, put)

    def test_through_index_nodes(self):
        """Edits inside a directory split into index nodes, and paths that step through them."""
        with mock.patch.multiple(constants, CHUNK_SIZE=4, MAX_CHILDREN=3):
            files = {f"f{i:02d}": self.file(f"{i}".encode()) for i in range(20)}
            big = self.directory(files)
            root = self.directory({"big": big})
            self.assertIsInstance(dag.decode(self.blocks[big.cid]), dag.IndexNode)
            new = self.file(b"new")
            result = edit.dir_set(root.cid, ["big"], "f05", new.cid, False, self.blocks.get, self.blocks.put)
            self.assertEqual(result, self.directory({"big": self.directory({**files, "f05": new})}).cid)
            result = edit.dir_rm(root.cid, ["big", "f10"], self.blocks.get, self.blocks.put)
            remaining = {k: v for k, v in files.items() if k != "f10"}
            self.assertEqual(result, self.directory({"big": self.directory(remaining)}).cid)
            # Paths stepping through big's index nodes: to a directory in it, a file, and a name it doesn't hold
            sub = self.directory({"a": self.a})
            big = self.directory({**files, "f07": sub})
            root = self.directory({"big": big})
            result = edit.dir_set(root.cid, ["big", "f07"], "b", self.b.cid, False, self.blocks.get, self.blocks.put)
            edited = self.directory({"a": self.a, "b": self.b})
            expected = self.directory({"big": self.directory({**files, "f07": edited})})
            self.assertEqual(result, expected.cid)
            with self.assertRaises(ValueError) as caught:
                edit.dir_set(root.cid, ["big", "f05"], "x", new.cid, False, self.blocks.get, self.blocks.put)
            self.assertEqual(str(caught.exception), f"{root.cid}/big/f05 is not a directory")
            with self.assertRaises(NotFound) as caught:
                edit.dir_set(root.cid, ["big", "f99", "x"], "y", new.cid, False, self.blocks.get, self.blocks.put)
            self.assertEqual(str(caught.exception), f"{root.cid}/big/f99 does not exist")

    def test_depth_limit(self):
        with mock.patch.object(constants, "MAX_DEPTH", 3):
            inner = self.directory({"x": self.directory({"y": self.a})})  # 2 deep
            shallow = self.directory({})
            edit.dir_set(shallow.cid, [], "inner", inner.cid, False, self.blocks.get, self.blocks.put)  # 3 deep
            outer = self.directory({"o": shallow})
            with self.assertRaises(TooLarge):
                edit.dir_set(outer.cid, ["o"], "inner", inner.cid, False, self.blocks.get, self.blocks.put)


if __name__ == "__main__":
    unittest.main()
