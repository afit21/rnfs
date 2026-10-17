"""The files the rnfs-0.1.5 fixture adds on top of the rnfs-0.1.4 fixture's (tests/fixtures/rnfs-0.1.4/
fixture_content.py), shared by generate.py (which adds them with rnfs 0.1.5) and tests/test_upgrade_015.py (which
adds them again with the current code, to make the blocks: the fixture keeps no blocks/ of its own). Data only
(and hashlib), so either version of rnfs can import it.
"""
import hashlib

# Local deny-list entries (rnfs deny add): a CID, and a file hash. Neither matches anything the fixture holds,
# so the upgrade test sees every pin and record as 0.1.5 kept it.
DENIED_CID = "bafkreig5r66w4f7qbtvtsqwwlfvfql6cmsnhp42vymrhjfiwhtestirgie"  # raw block b"denied on this node only"
DENIED_HASH = "sha256:b4c04cf45324f820d1061ae8f3955b3c5f9d4079553d52e6624dcc9386879bcb"  # b"a file this node refuses"

# A deny list (the deny_lists setting names its CID), added with no name and pinned "deny" once accepted
DENY_LIST = (b"# a list a node operator follows\n"
             b"bafkreibdtcf2jfx2vilaswpa3bmfkbli5w3qw3jnxz23mnpqzflu4y3dzm\n"  # raw block b"denied by a deny list"
             b"sha256:ab79b95e5c7747cd86f28d0d91b8339922032445112f1b6a48713847858bdaee\n", None)

# A file of several blocks, pinned and read once while file hashes are denied, so 0.1.5 remembers its content
# hash (the deny_hashes table): a one-block file's CID is its hash, so only a larger one is remembered
LARGE = (b"".join(hashlib.sha256(b"%d" % i).digest() for i in range(20000)), "large.bin")

STREAMS = (DENY_LIST, LARGE)
