"""Entry names (specification section 5): what a directory entry may be called. The one rule, which
rnfs.content.dag re-exports for the DAG and its builders.

Stdlib only: rnfs.uris and rnfs.nomadnet_links check names with it, and the CLI and rnfs.Client import those without RNS
(rnfs.content.dag loads RNS's bundled msgpack).
"""

MAX_NAME_BYTES = 255


def check_name(name) -> None:
    """Raise ValueError unless name is a valid entry name: UTF-8, 1-255 bytes, no / or NUL, not . or .."""
    if not isinstance(name, str):
        raise ValueError("name must be a string")
    try:
        encoded = name.encode("utf-8")
    except UnicodeEncodeError:
        # Lone surrogates, e.g. from a filename that wasn't valid UTF-8 (os.fsdecode's surrogateescape)
        raise ValueError(f"name {name!r} is not valid UTF-8") from None
    if not 1 <= len(encoded) <= MAX_NAME_BYTES:
        raise ValueError(f"name {name!r} must be 1-{MAX_NAME_BYTES} bytes of UTF-8")
    if "/" in name or "\0" in name:
        raise ValueError(f"name {name!r} contains / or NUL")
    if name in (".", ".."):
        raise ValueError(f"name {name!r} is not allowed")


def is_valid_name(name) -> bool:
    """True if check_name accepts name."""
    try:
        check_name(name)
        return True
    except ValueError:
        return False
