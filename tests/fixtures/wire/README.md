# Wire known-answer vectors

`vectors.json` holds what rnfs **0.1.3** and **0.1.4** put on the wire for fixed inputs, made by those releases'
own code: `/find`, `/provide`, `/blocks` and `/pin` requests and responses (and the `True`/`False` reply of
`/provide` and `/pin`), keepers' `/name` request and response, the `rnfs.dht` announce's mode, `rnfs.name`
records (sequence 0, 1 and 2^64 - 1), and the LXMF field carrying links. `tests/test_wire.py` checks that the
current code encodes the same inputs to the same bytes, and parses those bytes back to the same inputs.

- `wire_cases.py`: the cases, and how each kind is encoded and parsed. It imports the rnfs tree it is given, so
  the same file runs on the old releases and on the current code.
- `generate.py`: runs `wire_cases.py` on each tree given, in a subprocess with that tree first on `sys.path`,
  checks the runs agree byte for byte, and writes `vectors.json` (one vector per line).

Each vector is `{name, kind, inputs, hex}`. `hex` is `umsgpack.packb` of the request or response data only (RNS
owns the envelope); for announce app_data (the mode and name records) it is the bytes themselves. In `inputs`, a
byte string is `{"hex": ...}` and a CID `{"cid": <text>}`. A name record's signature is Ed25519, which is
deterministic, so a fixed key (`NAME_KEY`, a throwaway for these vectors only) signs the same bytes each run.

## Regenerating

Only when adding cases: the bytes must never change. From the repository root:

    for v in v0.1.3 v0.1.4; do mkdir /tmp/rnfs-$v && git archive $v | tar -x -C /tmp/rnfs-$v; done
    python3 tests/fixtures/wire/generate.py /tmp/rnfs-v0.1.3 /tmp/rnfs-v0.1.4

A new kind that 0.1.3 doesn't have can't be generated this way; give it its own generator run on the release that
introduced it.
