"""The files the rnfs-0.1.4 fixture adds, shared by generate.py (which adds them with rnfs 0.1.4) and
tests/test_upgrade_014.py (which adds them again with the current code, to make the blocks: the fixture keeps no
blocks/ of its own). Data only, so either version of rnfs can import it.
"""

# Added from a stream (rnfs add -): (data, the name it is wrapped as, or None)
USER = (b"Bonjour \xc3\xa0 tous\n", "Résumé – 2026.txt")
PLAIN = (b"plain bytes from stdin\n", None)
PICTURE = (b"\xff\xd8\xff\xe0 not really a jpeg", "photo.jpg")
ODD = (b"a file named in Latin-1\n", None)  # pin_names.json records it under a path that isn't UTF-8
REMOTE_PIN = (b"pinned for a remote identity\n", None)
KEPT_TARGET = (b"a kept name's target\n", None)
FOLLOWED_TARGET = (b"the latest issue of a followed name\n", None)
RESOLVED_TARGET = (b"resolved through the CLI\n", None)
FEED = (b"auto-fetched from a trusted sender\n", None)
DROPPED = (b"pinned, published, then released\n", None)
STREAMS = (USER, PLAIN, PICTURE, ODD, REMOTE_PIN, KEPT_TARGET, FOLLOWED_TARGET, RESOLVED_TARGET, FEED, DROPPED)

# Added from a directory (rnfs add <dir>): {name: bytes, or a subdirectory's dict}
ALBUM = {"2026": {"sommer.jpg": b"jpeg bytes", "été.jpg": b"more jpeg"}}
DOCS = {"日本語": {"メモ.md": b"# memo\n"}, "café.txt": b"coffee\n", "Ελληνικά": {"αρχείο.txt": b"greek\n"}}
TREES = (ALBUM, DOCS)

# The shared folder's one entry, files/notes.txt (checked in, as the folder scan imports it)
NOTES = ("notes.txt", b"shared folder notes\n")
