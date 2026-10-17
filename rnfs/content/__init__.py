"""The content layer: CIDs, DAG nodes, chunking, building and walking trees, sealed files, and storage. No
networking. It uses RNS's bundled msgpack and RNS logging (so importing it loads RNS), and seal.py also RNS's
cryptography and identities; nothing else from RNS.

cid.py            CIDs (section 4)
dag.py            DAG node encoding, strict decoding, names (section 5)
chunker.py        fixed and FastCDC chunkers (section 5)
builder.py        building trees: files, directories, wrap, only-hash (section 5)
seal.py           sealed files (section 5, Sealed files)
walker.py         expectations, reader checks, path lookup, byte ranges, listing, writing out (sections 5, 7)
edit.py           dir set / dir rm (section 11)
blockstore.py     one file per CID, atomic, verified (section 8)
storage.py        blocks + pins + cached fetches + protection + eviction/GC (section 8)
archive.py        export/import streams (section 8)
shared_folder.py  importing <rnfs dir>/files/ with pin source "folder" (section 8)
"""
