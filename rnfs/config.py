"""Local settings (specification section 12) and where a node keeps its files.

Each node may change these, in `<rnfs dir>/rnfs.toml` or on the command line. Times are in seconds,
sizes in bytes and rates in bits/s. A setting left as None takes a default that depends on whether
the node (or link) is slow; use the `effective_*` helpers rather than reading those fields directly.
"""
import errno
import math
import os
import re
import stat
import tomllib
from dataclasses import dataclass, field, fields

from rnfs import fsutil
from rnfs.constants import DAY, GIB, HOUR, KIB, MIB, PROVIDER_TTL

DEFAULT_API_PORT = 8788
DEFAULT_GATEWAY_PORT = 8789
SETTINGS_FILE = "rnfs.toml"
MAX_SETTINGS_SIZE = 1 * MIB  # bytes of rnfs.toml read; a larger file is refused
# In the rnfs dir, locked by the daemon or embedded node using it (node.lock_rnfs_dir), so a second can't start
LOCK_FILE = "daemon.lock"

# Core settings' types and ranges, checked by Config.validate (feature settings are checked by their features)
WHOLE_NUMBERS = {  # name: (minimum, maximum or None). gateway_port 0: any free port (printed at start)
    "api_port": (1, 65535), "gateway_port": (0, 65535), "verbosity": (0, None), "min_rate_sample": (1, None),
    "beta": (1, None), "max_failures": (1, None), "max_incoming_links": (1, None), "max_blocks_responses": (2, None),
    "fast_blocks_responses": (1, None), "max_records": (0, None), "max_storage": (0, None),
    "max_output_entries": (0, None), "name_table_size": (0, None), "remote_pin_quota": (0, None),
    "gateway_fetches": (0, None),
}
OPTIONAL_WHOLE_NUMBERS = {"alpha": 1, "max_parallel_providers": 1, "max_download_size": 0}  # name: minimum
RATES = ("slow_bitrate", "unmeasured_bitrate", "max_upload_rate")  # bits/s, above 0
DURATIONS = {  # seconds: name → True if it must be above 0 (else 0 is allowed too)
    "republish_interval": True, "announce_interval": True, "startup_announce_delay": False,
    "provider_seniority": False, "response_target": True, "deadline_slack": False, "stall_timeout": True,
    "blocks_cooldown": False, "lookup_timeout": True, "path_timeout": True, "link_idle_timeout": True,
    "ban_time": False, "scan_interval": True,
}
OPTIONAL = ("republish_interval", "max_upload_rate")  # these may also be unset (None)
# Some durations are waited on, and Windows can wait at most ~49 days at once (threading.TIMEOUT_MAX)
MAX_DURATION = 30 * DAY
FLAGS = ("private", "gateway_scripts", "default_bootstrap", "debug_log")
IDENTITY_LISTS = ("members", "remote_pin_identities")  # RNS identity hashes: 32 hex digits, optionally <bracketed>
_HEX_ID = re.compile(r"[0-9a-fA-F]{32}")


def _is_number(value) -> bool:
    """True if value is a finite int or float; not a bool, which Python counts as an int."""
    return type(value) in (int, float) and math.isfinite(value)


@dataclass
class Config:
    """Settings for an RnfsNode.

    warnings: what Config.load changed to trust the settings, for the caller to show (a plain attribute, not a
    field, so rnfs.toml can't set it).
    """

    warnings = ()  # unannotated, so not a dataclass field; Config.load sets a list

    rnfs_dir: str = "~/.rnfs/"  # identity, blocks, pins, names and shared folder live here
    # Reticulum config directory. None uses RNS's own default (/etc/reticulum, ~/.config/reticulum or
    # ~/.reticulum), which is the one rnsd uses, so rnfs attaches to the shared instance with its RPC key
    reticulum_dir: str | None = None
    api_port: int = DEFAULT_API_PORT
    gateway_port: int = DEFAULT_GATEWAY_PORT
    bootstrap_nodes: list[str] = field(default_factory=list)  # node IDs (hex) contacted first when joining
    default_bootstrap: bool = True  # also contact DEFAULT_BOOTSTRAP_NODES when joining (never in private mode)
    verbosity: int = 0  # added to Reticulum's loglevel: 1 shows per-query detail, 2 shows RNS debug output
    debug_log: bool = False  # the daemon also writes <rnfs dir>/debug.log, every message at every level (debuglog.py)

    slow: str = "auto"  # "yes", "no" or "auto" (section 2)
    slow_bitrate: float = 100_000
    min_rate_sample: int = 16 * KIB
    unmeasured_bitrate: float = 100
    dht_mode: str = "server"  # "server" or "client"
    alpha: int | None = None  # 3; 1 on slow nodes
    beta: int = 3
    republish_interval: float | None = None  # 24 h; 3 days on slow nodes. MUST be < PROVIDER_TTL / 2
    announce_interval: float = 6 * HOUR
    startup_announce_delay: float = 30
    max_parallel_providers: int | None = None  # 3; 1 on slow nodes
    provider_seniority: float = 1 * DAY
    response_target: float = 60
    deadline_slack: float = 30
    stall_timeout: float = 120
    # /blocks refused to a link (and identity) whose response was displaced or cancelled for making no progress
    blocks_cooldown: float = 300
    lookup_timeout: float = 600
    path_timeout: float = 30
    max_failures: int = 3
    link_idle_timeout: float = 300
    max_incoming_links: int = 128
    max_blocks_responses: int = 4  # at least 2: half of them for slow links, the rest for others
    # Responses in progress at once on links that aren't slow, if more than the rest of max_blocks_responses
    fast_blocks_responses: int = 16
    max_upload_rate: float | None = None  # none; half the rate estimate on slow links that have one
    max_records: int = 100_000
    ban_time: float = 1 * HOUR
    max_storage: int = 10 * GIB
    max_download_size: int | None = None
    max_output_entries: int = 100_000
    name_table_size: int = 10_000
    remote_pin_quota: int = 1 * GIB  # per remote identity
    remote_pin_identities: list[str] = field(default_factory=list)  # identity hashes (hex) allowed to send /pin
    members: list[str] = field(default_factory=list)  # private mode's member identity hashes (hex)
    private: bool = False  # private mode (section 3)
    scan_interval: float = 60
    provide_strategy: str = "roots"  # "roots" (pins and complete cached downloads) or "pinned" (pins only)
    gateway_scripts: bool = True
    gateway_fetches: int = 2

    # Feature settings. Each feature adds its settings under its own marker (keep this order)
    # --- transfers (rnfs/features/jobs.py) ---
    max_jobs: int | None = None  # background jobs (get, pin, export) running at once: 2; 1 on slow nodes
    max_finished_jobs: int = 100  # finished jobs kept for `rnfs jobs`; the oldest beyond this are forgotten
    # --- mesh (provider choice, airtime, peers) ---
    upload_budget: int | None = None  # bytes per hour of /blocks responses, node-wide; None: unlimited
    serve_interfaces: list[str] = field(default_factory=list)  # serve /blocks only on these (names or prefixes)
    no_serve_interfaces: list[str] = field(default_factory=list)  # never serve /blocks on these
    quiet_hours: str = ""  # local time, e.g. "22:00-06:00": /blocks is refused then; "" for none
    # --- names (keepers, follow, contacts) ---
    # Names kept (keep and follow) for others; this node's own names don't count. Each is a DHT key republished
    # (at least every 3 days) and, if followed, a pin: 100 keeps a slow channel's republish airtime small
    keep_max_names: int = 100
    keeper_query: str = "fallback"  # "fallback": ask keepers when a path request heard nothing; "always"
    # Bytes a followed name's new target may hold (its size and its DAG's blocks); 0: no follow fetches. A swap
    # briefly holds the old target and the new one, so up to twice this per followed name
    follow_quota: int = 1 * GIB
    # --- content (pin=false, sealing, chunker, gateway formats) ---
    # --- lxmf (rnfs/features/lxmf.py) ---
    lxmf_enabled: bool | None = None  # None: on if the LXMF package is installed
    lxmf_display_name: str | None = None  # announced name; None: "rnfs node" (nothing linking it to the node ID)
    lxmf_propagation_node: str | None = None  # LXMF propagation node (hex); None: none for automatic use
    lxmf_send_from_hint: bool = True  # sent links carry from=<node ID>: links the LXMF address to the node ID
    lxmf_sync_interval: float = 3600  # seconds between downloads from the propagation node; 0: never
    lxmf_accept_from: list[str] = field(default_factory=list)  # LXMF addresses (hex); empty: everyone
    lxmf_auto_fetch_from: list[str] = field(default_factory=list)  # senders whose links are fetched and pinned
    lxmf_auto_fetch_quota: int = 1 * GIB  # per sender in lxmf_auto_fetch_from
    # --- deny lists (rnfs/features/deny.py) ---
    deny_lists: list[str] = field(default_factory=list)  # deny-list sources: <cid>[/path] or name/<name>[/path]

    # Paths

    @property
    def root(self) -> str:
        """rnfs_dir with ~ expanded."""
        return os.path.expanduser(self.rnfs_dir)

    @property
    def reticulum_root(self) -> str | None:
        """reticulum_dir with ~ expanded, or None for RNS's default config directory."""
        return os.path.expanduser(self.reticulum_dir) if self.reticulum_dir else None

    @property
    def identity_path(self) -> str:
        return os.path.join(self.root, "storage", "identity")

    @property
    def blocks_dir(self) -> str:
        return os.path.join(self.root, "blocks")

    @property
    def files_dir(self) -> str:
        """The shared folder (section 8)."""
        return os.path.join(self.root, "files")

    @property
    def names_dir(self) -> str:
        """Name keys, one RNS identity file per alias (section 9)."""
        return os.path.join(self.root, "names")

    @property
    def state_dir(self) -> str:
        """rnfs's own persistent state: pins, cache records, provider records, sequences."""
        return os.path.join(self.root, "state")

    # Values that depend on whether the node is slow (section 2)

    def effective_alpha(self, slow_node: bool) -> int:
        if self.alpha is not None:
            return self.alpha
        return 1 if slow_node else 3

    def effective_republish_interval(self, slow_node: bool) -> float:
        if self.republish_interval is not None:
            return self.republish_interval
        return 3 * DAY if slow_node else 1 * DAY

    def effective_max_parallel_providers(self, slow_node: bool) -> int:
        if self.max_parallel_providers is not None:
            return self.max_parallel_providers
        return 1 if slow_node else 3

    def effective_max_upload_rate(self, slow_link: bool, rate_estimate: float | None) -> float | None:
        """The upload cap for a link in bits/s, or None for no cap."""
        if self.max_upload_rate is not None:
            return self.max_upload_rate
        if slow_link and rate_estimate is not None:
            return rate_estimate / 2
        return None

    def validate(self):
        """Raise ValueError for settings the spec forbids, or of the wrong type or out of range."""
        self._validate_core_types()
        if self.slow not in ("yes", "no", "auto"):
            raise ValueError("slow must be yes, no or auto")
        if self.dht_mode not in ("server", "client"):
            raise ValueError("dht_mode must be server or client")
        if self.provide_strategy not in ("pinned", "roots"):
            raise ValueError("provide_strategy must be pinned or roots")
        # transfers
        # type() rather than isinstance(): true is an int to Python, but not a count anyone means
        if self.max_jobs is not None and (type(self.max_jobs) is not int or self.max_jobs < 1):
            raise ValueError("max_jobs must be a whole number of at least 1")
        if type(self.max_finished_jobs) is not int or self.max_finished_jobs < 1:
            raise ValueError("max_finished_jobs must be a whole number of at least 1")
        # mesh
        from rnfs import mesh  # here: rnfs.mesh is only needed once settings are checked
        mesh.validate(self)
        # names
        # type(): bool is an int subclass, and keep_max_names = true must not mean 1
        if type(self.keep_max_names) is not int or self.keep_max_names < 0:
            raise ValueError("keep_max_names must be a whole number, not negative")
        if self.keeper_query not in ("fallback", "always"):
            raise ValueError("keeper_query must be fallback or always")
        if type(self.follow_quota) is not int or self.follow_quota < 0:
            raise ValueError("follow_quota must be a whole number of bytes, not negative")
        # lxmf
        from rnfs.features import lxmf  # here, like mesh: only needed once settings are checked
        lxmf.validate_settings(self)
        # deny lists
        from rnfs.features import deny
        deny.validate_settings(self)
        for slow_node in (False, True):
            if self.effective_republish_interval(slow_node) >= PROVIDER_TTL / 2:
                raise ValueError("republish_interval must be less than PROVIDER_TTL / 2")

    def _validate_core_types(self):
        """Raise ValueError for a core setting of the wrong type or out of range.

        type() rather than isinstance(): true is an int to Python, but not a count or a port anyone means; and
        a string such as "false" or "no" in rnfs.toml must not read as a true flag.
        """
        for name, (minimum, maximum) in WHOLE_NUMBERS.items():
            value = getattr(self, name)
            if type(value) is not int or value < minimum or (maximum is not None and value > maximum):
                limits = f"{minimum}-{maximum}" if maximum is not None else f"at least {minimum}"
                raise ValueError(f"{name} must be a whole number, {limits}")
        for name, minimum in OPTIONAL_WHOLE_NUMBERS.items():
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < minimum):
                raise ValueError(f"{name} must be a whole number of at least {minimum}, or unset")
        unset = " (or unset)"
        for name in RATES:
            value = getattr(self, name)
            if not (value is None and name in OPTIONAL) and (not _is_number(value) or value <= 0):
                raise ValueError(f"{name} must be a number of bits/s above 0{unset if name in OPTIONAL else ''}")
        for name, positive in DURATIONS.items():
            value = getattr(self, name)
            if value is None and name in OPTIONAL:
                continue
            if not _is_number(value) or value < 0 or (positive and value == 0) or value > MAX_DURATION:
                raise ValueError(f"{name} must be a number of seconds, {'above' if positive else 'at least'} 0 "
                                 f"and at most {MAX_DURATION}{unset if name in OPTIONAL else ''}")
        for name in FLAGS:
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be true or false")
        for name in IDENTITY_LISTS:
            values = getattr(self, name)
            if not isinstance(values, list) or not all(
                    isinstance(value, str) and _HEX_ID.fullmatch(value.strip().strip("<>")) for value in values):
                raise ValueError(f"{name} must be a list of RNS identity hashes (32 hex characters each)")
        # Node IDs that don't parse are skipped with a warning when joining, but a string isn't a list of them
        if not isinstance(self.bootstrap_nodes, list) or not all(isinstance(v, str) for v in self.bootstrap_nodes):
            raise ValueError("bootstrap_nodes must be a list of node IDs (hex)")
        if self.reticulum_dir is not None and not isinstance(self.reticulum_dir, str):
            raise ValueError("reticulum_dir must be a directory path")

    @classmethod
    def load(cls, rnfs_dir: str = "~/.rnfs/", defaults: dict | None = None, **overrides) -> "Config":
        """Settings from `<rnfs_dir>/rnfs.toml` if it exists, then overrides (None values ignored), validated.
        defaults (an embedded node's) apply where neither sets a value.

        rnfs.toml, and RNS's `config` when reticulum_dir is the rnfs dir, must be regular files of this user's;
        one others may access is made private (open_checked). What was changed is in the result's warnings, for
        the caller to show: this module doesn't log, as rnfs.log imports RNS, which the CLI and rnfs.Client never
        load. Raises PermissionError if either file can't be trusted, ValueError for settings that aren't valid
        (also a settings file over MAX_SETTINGS_SIZE), OSError if the file can't be read.
        """
        values: dict = dict(defaults or {})
        warnings: list[str] = []
        root = os.path.expanduser(rnfs_dir)
        path = os.path.join(root, SETTINGS_FILE)
        # Its settings choose the Reticulum config, and with it programs rnfs starts, and the API port the CLI
        # sends its token to: whoever could change it could choose them
        opened = open_checked(path, "the settings file")
        if opened is not None:
            fd, warning = opened
            if warning is not None:
                warnings.append(f"{warning}. Check that its settings are yours")
            with os.fdopen(fd, "rb") as f:
                data = f.read(MAX_SETTINGS_SIZE + 1)
            if len(data) > MAX_SETTINGS_SIZE:
                raise ValueError(f"{path} is larger than {MAX_SETTINGS_SIZE // MIB} MiB")
            values.update(tomllib.loads(data.decode("utf-8")))
        known = {f.name for f in fields(cls)}
        unknown = values.keys() - known
        if unknown:
            raise ValueError(f"unknown settings in {path}: {', '.join(sorted(unknown))}")
        if "rnfs_dir" in values:
            # The file lives in the rnfs dir, so it can't move it; --dir does
            raise ValueError(f"rnfs_dir can't be set in {path}; use --dir")
        values.update({k: v for k, v in overrides.items() if v is not None})
        values["rnfs_dir"] = rnfs_dir
        config = cls(**values)
        config.validate()
        reticulum_root = config.reticulum_root
        if reticulum_root is not None and os.path.realpath(reticulum_root) == os.path.realpath(root):
            # Earlier versions' layout: RNS reads its config (interfaces, and their commands) from the rnfs dir
            opened = open_checked(os.path.join(root, "config"), "the Reticulum config")
            if opened is not None:
                fd, warning = opened
                os.close(fd)
                if warning is not None:
                    warnings.append(f"{warning}. Check that its interfaces are yours")
        config.warnings = warnings
        return config


def open_checked(path: str, what: str) -> tuple[int, str | None] | None:
    """Open the file at path for reading, once checked through its descriptor: (the descriptor, a warning if it
    was made private) or None if nothing is at path. what (e.g. "the settings file") names it in messages.

    It must be a regular file (a FIFO would block the start, a device be read without end), not a symlink
    (whoever can write to the directory could point it at a file of theirs), and the user's own; one others may
    access is made private (fsutil.check_owned_fd). Opened with O_NOFOLLOW and O_NONBLOCK, so neither a symlink
    nor a FIFO is followed or waited on. Raises PermissionError if it can't be trusted, OSError if it can't be
    opened.
    """
    try:
        # On Windows, which has no O_NOFOLLOW, a symlink or junction is found here (O_NOFOLLOW catches a swap)
        listed = os.lstat(path)
    except FileNotFoundError:
        return None
    if fsutil.is_link(listed):
        raise PermissionError(f"{what} {path} is a symlink; rnfs only reads a regular file there (put the file "
                              f"itself there)")
    try:
        fd = os.open(path, os.O_RDONLY | fsutil.O_NOFOLLOW | fsutil.O_BINARY | getattr(os, "O_NONBLOCK", 0)
                     | getattr(os, "O_CLOEXEC", 0))
    except FileNotFoundError:
        return None  # removed since the lstat
    except OSError as e:
        if e.errno in (errno.ELOOP, errno.EMLINK):  # O_NOFOLLOW's answer for a symlink (EMLINK on FreeBSD)
            raise PermissionError(f"{what} {path} is a symlink; rnfs only reads a regular file there") from None
        raise
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise PermissionError(f"{what} {path} is not a regular file; rnfs only reads a regular file there")
        try:
            warning = fsutil.check_owned_fd(path, fd, info, what)
        except OSError as e:
            if not fsutil.IS_WINDOWS:
                raise  # its message says what and why
            raise PermissionError(f"can't trust {what}: {e}") from None
    except BaseException:
        os.close(fd)
        raise
    return fd, warning
