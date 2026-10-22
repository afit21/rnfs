"""REL-4 (0.2.0 release audit): a tar member's size can be negative, and only some interpreters refused it.

The attack: a sealed folder's author (or a local API client adding a tar stream) writes a member whose size is
negative, in a base-256 size field or a pax `size=` record. Python's tarfile parses both; only interpreters with its
offset check (3.12.3 and later security fixes) then refuse the stream, while rnfs supports 3.11, where a negative size
would reach the space estimate (scan_tar) and the file builder.

The test feeds both forms to scan_tar and build_tar: each is refused as a malformed tar, the base-256 one by rnfs's own
check. The fix (rnfs/content/tarstream.py, the Header class): a negative size is refused in _proc_member, and again once
pax records are applied, in fromtarfile.
"""
import io
import tarfile
import unittest

from rnfs.content import tarstream


def negative_base256() -> bytes:
    """A GNU tar whose one member's size field is base-256 -1."""
    info = tarfile.TarInfo("a.txt")
    info.size = -1
    return info.tobuf(tarfile.GNU_FORMAT) + b"\0" * 1024


def negative_pax() -> bytes:
    """A pax tar whose one member's size is -5 by its pax record."""
    info = tarfile.TarInfo("a.txt")
    info.pax_headers = {"size": "-5"}
    return info.tobuf(tarfile.PAX_FORMAT) + b"\0" * 1024


class NegativeTarSizeTest(unittest.TestCase):

    def readers(self):
        """scan_tar and build_tar, each as a function of the stream's bytes."""
        blocks = {}
        return (lambda data: tarstream.scan_tar(io.BytesIO(data), 100),
                lambda data: tarstream.build_tar(io.BytesIO(data), blocks.__setitem__, lambda text: None, 100))

    def test_a_negative_size_is_refused(self):
        for make in (negative_base256, negative_pax):
            for read in self.readers():
                with self.subTest(form=make.__name__), self.assertRaises(ValueError):
                    read(make())

    def test_a_negative_size_field_is_refused_by_rnfs_itself(self):
        for read in self.readers():
            with self.assertRaisesRegex(ValueError, "a tar member of -1 bytes"):
                read(negative_base256())


if __name__ == "__main__":
    unittest.main()
