"""Local settings (specification section 12) and where a node keeps its files.

Each node may change these, in `<rnfs dir>/rnfs.toml` or on the command line. Times are in seconds,
sizes in bytes and rates in bits/s. A setting left as None takes a default that depends on whether
the node (or link) is slow; use the `effective_*` helpers rather than reading those fields directly.
"""
import errno
import math
import os
import stat
import tomllib
from dataclasses import MISSING, dataclass, field, fields
from types import MappingProxyType

from rnfs import fsutil, ids
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
    "fast_blocks_responses": (1, None), "record_budget": (0, None), "max_storage": (0, None),
    "max_output_entries": (0, None), "name_table_size": (0, None), "remote_pin_quota": (0, None),
    "gateway_fetches": (0, None), "provide_entries_max": (0, None),
}
OPTIONAL_WHOLE_NUMBERS = {"alpha": 1, "max_parallel_providers": 1, "max_download_size": 0}  # name: minimum
RATES = ("slow_bitrate", "unmeasured_bitrate", "max_upload_rate")  # bits/s, above 0
DURATIONS = {  # seconds: name → True if it must be above 0 (else 0 is allowed too)
    "republish_interval": True, "announce_interval": True, "startup_announce_delay": False,
    "provider_seniority": False, "response_target": True, "deadline_slack": False, "stall_timeout": True,
    "blocks_cooldown": False, "lookup_timeout": True, "path_timeout": True, "link_idle_timeout": True,
    "ban_time": False, "scan_interval": True,
}
# The least a duration that sets how often something is sent (or a folder read) may be: 1e-300 passed "above 0", and a
# typo would have announced or republished in a loop, flooding a radio channel
MIN_DURATIONS = {"announce_interval": 60.0, "republish_interval": 600.0, "scan_interval": 1.0}
OPTIONAL = ("republish_interval", "max_upload_rate")  # these may also be unset (None)
# Some durations are waited on, and Windows can wait at most ~49 days at once (threading.TIMEOUT_MAX)
MAX_DURATION = 30 * DAY
FLAGS = ("private", "gateway_scripts", "default_bootstrap", "debug_log")
OPTIONAL_FLAGS = ("provide_entries",)  # true, false, or unset (None: a default that depends on the node)
IDENTITY_LISTS = ("members", "remote_pin_identities")  # RNS identity hashes: 32 hex digits, optionally <bracketed>
# Settings that take one of a few words (Config.validate), core and feature ones alike
CHOICES = {"slow": ("yes", "no", "auto"), "dht_mode": ("server", "client"), "provide_strategy": ("pinned", "roots", "auto"),
           "keeper_query": ("fallback", "always")}
_HEX_ID = ids.HEX16  # the name other modules used before rnfs.ids

# The settings schema (settings_schema): what an app needs to show and edit the settings without a copy of their
# metadata. Types and ranges come from the tables above and the fields' annotations and defaults; what those
# can't say is below. tests/test_settings.py fails for a field none of this describes

# Groups, as the markers in Config mark them: (group, its first field), in field order; a group runs to the next
SECTIONS = (("core", "reticulum_dir"), ("transfers", "max_jobs"), ("mesh", "upload_budget"),
            ("names", "keep_max_names"), ("deny lists", "deny_lists"))
# Feature settings' limits (their features check them): name -> (minimum, maximum or None)
FEATURE_LIMITS = {"max_jobs": (1, None), "max_finished_jobs": (1, None), "upload_budget": (1, None),
                  "local_hops": (1, None), "local_replicas": (0, 5),
                  "keep_max_names": (0, None), "follow_quota": (0, None)}
# Units of what the tables don't give one (DURATIONS are seconds, RATES bits/s)
UNITS = {"min_rate_sample": "bytes", "record_budget": "bytes", "max_storage": "bytes", "max_download_size": "bytes",
         "remote_pin_quota": "bytes", "follow_quota": "bytes", "upload_budget": "bytes/hour", "local_hops": "hops",
         "local_replicas": "servers", "provide_entries_max": "keys"}
# Settings a running node applies at once when changed through the API (rnfs/features/settings.py, whose LIVE table
# must name exactly these); every other one applies at the next start
LIVE_SETTINGS = frozenset({
    "max_storage", "max_download_size", "max_output_entries", "max_upload_rate", "remote_pin_quota",
    "scan_interval", "provide_entries", "provide_entries_max", "max_jobs", "max_finished_jobs", "serve_interfaces", "no_serve_interfaces",
    "quiet_hours", "keep_max_names", "keeper_query", "follow_quota"})
# One line per setting (specification section 12 has the full notes)
DESCRIPTIONS = {
    "reticulum_dir": "Reticulum config directory; unset: RNS's own, the one rnsd uses",
    "api_port": "Local API port, on 127.0.0.1",
    "gateway_port": "Gateway port, on 127.0.0.1; 0: any free port",
    "bootstrap_nodes": "Node IDs (hex) contacted first when joining the DHT",
    "default_bootstrap": "Also contact the default bootstrap nodes when joining (never in private mode)",
    "verbosity": "Extra logging: 1 per-query detail, 2 RNS debug output",
    "debug_log": "Also write every message to <rnfs dir>/debug.log, readable by its owner only",
    "slow": "Whether this node is slow: yes, no, or auto (from its interfaces)",
    "slow_bitrate": "Below this, links and nodes are slow",
    "min_rate_sample": "Smallest response used for a rate estimate",
    "unmeasured_bitrate": "Rate assumed for deadlines before a link has a rate estimate",
    "dht_mode": "server (store records and answer queries) or client (only ask)",
    "alpha": "Parallel lookup queries; unset: 3, or 1 on slow nodes",
    "beta": "Closest nodes that must answer before a lookup is complete",
    "republish_interval": "How often provider records are republished; unset: 24 h, or 3 days on slow nodes",
    "announce_interval": "How often the node announces itself",
    "startup_announce_delay": "Delay of the second announce after startup",
    "max_parallel_providers": "Providers fetched from at once; unset: 3, or 1 on slow nodes",
    "provider_seniority": "Providers in the routing table this long are tried first",
    "response_target": "Transfer time a /blocks response aims for",
    "deadline_slack": "Added to response deadlines; minimum age before a response can be displaced",
    "stall_timeout": "A response with no progress for this long is cancelled",
    "blocks_cooldown": "A link whose response made no progress is refused /blocks this long",
    "lookup_timeout": "Longest a DHT lookup runs",
    "path_timeout": "Longest to wait for an RNS path",
    "max_failures": "Unanswered queries in a row before a node is dropped",
    "link_idle_timeout": "Idle links are closed after this long at most (sooner without a rate estimate)",
    "max_incoming_links": "Links other nodes may have open to this one",
    "max_blocks_responses": "/blocks responses in progress at once (half of them for slow links); at least 2",
    "fast_blocks_responses": "/blocks responses in progress on links that aren't slow, if more than the rest",
    "max_upload_rate": "Upload cap of every /blocks response; unset: half the rate estimate on slow links",
    "record_budget": "Disk the provider records stored for others may take; past it, storage tiers decide who stays",
    "ban_time": "How long a misbehaving node is ignored",
    "max_storage": "Storage used before cached fetches are evicted (pins are never evicted)",
    "max_download_size": "Largest target fetched; unset: no limit",
    "max_output_entries": "Most entries a fetch, listing or write-out may meet",
    "name_table_size": "Records kept for names this node hasn't resolved itself",
    "remote_pin_quota": "Storage the remote pins of one identity may use",
    "remote_pin_identities": "Identity hashes (hex) allowed to ask this node to pin (/pin)",
    "members": "Private mode's member identity hashes (hex)",
    "private": "Private mode: talk to members only",
    "scan_interval": "How often the shared folder and the folders synced to names are scanned",
    "provide_strategy": "roots (pins and complete cached downloads) or pinned (pins only) are provided; auto: pinned "
                        "on a slow node, roots on others",
    "provide_entries": "Also provide every file and folder inside the user's own complete pins (not remote pins or "
                       "followed names'); unset: on, but on slow nodes only while republishing stays under 2% of "
                       "the channel",
    "provide_entries_max": "Most entry keys provide_entries provides: pins oldest first, each pin's entries all or "
                           "none",
    "gateway_scripts": "Let gateway pages run scripts",
    "gateway_fetches": "Gateway network fetches at a time for requests that aren't the user's own navigation",
    "max_jobs": "Background jobs running at once; unset: 2, or 1 on slow nodes",
    "max_finished_jobs": "Finished jobs kept for rnfs jobs (each at least 60 s)",
    "upload_budget": "Bytes per hour of /blocks responses, node-wide; unset: unlimited",
    "serve_interfaces": "Serve /blocks only on these RNS interfaces (names or prefixes); empty: all",
    "no_serve_interfaces": "Never serve /blocks on these RNS interfaces (names or prefixes)",
    "quiet_hours": "Local times when /blocks is refused, e.g. 22:00-06:00; empty: none",
    "local_interfaces": "Servers reached through these RNS interfaces (names or prefixes) are local; empty: those "
                        "within local_hops over a slow interface",
    "local_hops": "With local_interfaces empty, servers within this many hops over a slow interface are local",
    "local_replicas": "Local servers each provided key is also stored on (at most 5); 0: none",
    "keep_max_names": "Names kept (keep and follow) for others; this node's own names don't count",
    "keeper_query": "fallback: ask keepers when a path request heard nothing; always: on every network resolve",
    "follow_quota": "Bytes a followed name's new target may hold; 0: followed names aren't fetched",
    "deny_lists": "Deny-list sources: <cid>[/path] or name/<name>[/path]",
}
# Settings earlier versions had and this one doesn't: name -> what took its place, for the warning. One left in
# rnfs.toml (or given to an embedded node) is dropped with a warning instead of refused as unknown, so settings an
# older version accepted still start the node. The settings API and `rnfs config set` know it no more than any other
# name that isn't a setting (`rnfs config unset` removes it from rnfs.toml)
# LXMF messaging left rnfs in 0.2.0 for an app of its own, built on the local API and the library
_LXMF_RETIRED = "since rnfs 0.2.0 LXMF messaging is no longer part of rnfs, but an app of its own"
RETIRED_SETTINGS: MappingProxyType = MappingProxyType({
    "max_records": "since rnfs 0.2.0 provider records are bounded by record_budget, in bytes of disk",
    **{name: _LXMF_RETIRED for name in ("lxmf_enabled", "lxmf_display_name", "lxmf_propagation_node",
                                        "lxmf_send_from_hint", "lxmf_sync_interval", "lxmf_accept_from",
                                        "lxmf_auto_fetch_from", "lxmf_auto_fetch_quota")},
})
_TYPE_NAMES = {int: "int", float: "float", bool: "bool", str: "string", list[str]: "list[string]"}
MAX_TOML_INT = 2 ** 63 - 1  # TOML integers are 64-bit signed


def _is_number(value) -> bool:
    """True if value is a finite float or an int TOML can hold (64-bit); not a bool, which Python counts as an int. A
    larger int made math.isfinite raise OverflowError (REL-17): an API or embedded setting got that, not ValueError."""
    if type(value) is int:
        return -MAX_TOML_INT - 1 <= value <= MAX_TOML_INT
    return type(value) is float and math.isfinite(value)


@dataclass
class Config:
    """Settings for an RnfsNode.

    warnings: what Config.load changed to trust the settings, and the retired settings it ignored, for the caller to
    show (a plain attribute, not a field, so rnfs.toml can't set it). overrides and given_defaults: what Config.load
    was given over and under rnfs.toml (the daemon's command line, an embedded node's settings and defaults), so the
    settings feature can tell what a restart would run with.
    """

    # Unannotated, so not dataclass fields; Config.load sets them (never changed in place: a copy is shared)
    warnings = ()
    overrides = MappingProxyType({})
    given_defaults = MappingProxyType({})

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
    record_budget: int = 1 * GIB  # bytes of the state database provider records may take (spec section 6)
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
    # "roots" (pins and complete cached downloads), "pinned" (pins only), or "auto": pinned on a slow node, where
    # publishing each download's root costs about the airtime of a small download, roots elsewhere
    provide_strategy: str = "auto"
    # Also provide the CID of every file and directory inside the user's own complete pins (spec section 6). None:
    # on, up to provide_entries_max; on slow nodes only while the estimated republish airtime stays small
    provide_entries: bool | None = None
    provide_entries_max: int = 100_000  # entry keys provided at most (a 70k-file folder fits)
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
    # Local servers (spec section 6, "LoRa islands"): those whose path's next-hop interface is named here (names or
    # prefixes; through a shared instance, its interfaces as rnsd's config names them), or with none named, those
    # within local_hops over an interface below slow_bitrate. Each key is also stored on local_replicas of them
    local_interfaces: list[str] = field(default_factory=list)
    local_hops: int = 3
    local_replicas: int = 3  # at most mesh.MAX_LOCAL_REPLICAS (5); 0: none
    # --- names (keepers, follow, contacts) ---
    # Names kept (keep and follow) for others; this node's own names don't count. Each is a DHT key republished
    # (at least every 3 days) and, if followed, a pin: 100 keeps a slow channel's republish airtime small
    keep_max_names: int = 100
    keeper_query: str = "fallback"  # "fallback": ask keepers when a path request heard nothing; "always"
    # Bytes a followed name's new target may hold (its size and its DAG's blocks); 0: no follow fetches. A swap
    # briefly holds the old target and the new one, so up to twice this per followed name
    follow_quota: int = 1 * GIB
    # --- content (pin=false, sealing, chunker, gateway formats) ---
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

    # Identity lists, each normalised once here for its users: two sets differently normalised could disagree
    # about who is a member

    @property
    def member_set(self) -> frozenset[str]:
        """members as lowercase hex (as identity.hash.hex() writes them), for private mode's membership checks."""
        return frozenset(ids.normal_hex16(value) for value in self.members)

    @property
    def remote_pin_set(self) -> frozenset[str]:
        """remote_pin_identities as lowercase hex (as identity.hash.hex() writes them): who may send /pin."""
        return frozenset(ids.normal_hex16(value) for value in self.remote_pin_identities)

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
        """The upload cap for a link in bits/s, or None for no cap: max_upload_rate if set, else the default
        (rates.default_upload_cap: a slow link's rate estimate halved)."""
        if self.max_upload_rate is not None:
            return self.max_upload_rate
        # Here: rnfs.network.rates imports this module, and RNS (through rnfs.log), which settings alone don't need
        from rnfs.network import rates
        return rates.default_upload_cap(slow_link, rate_estimate)

    def validate(self):
        """Raise ValueError for settings the spec forbids, or of the wrong type or out of range."""
        self._validate_core_types()
        for name, choices in CHOICES.items():
            if getattr(self, name) not in choices:
                raise ValueError(f"{name} must be {', '.join(choices[:-1])} or {choices[-1]}")
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
        if type(self.follow_quota) is not int or self.follow_quota < 0:
            raise ValueError("follow_quota must be a whole number of bytes, not negative")
        # deny lists
        from rnfs.features import deny
        deny.validate_settings(self)
        for slow_node in (False, True):
            if self.effective_republish_interval(slow_node) >= PROVIDER_TTL / 2:
                raise ValueError("republish_interval must be less than PROVIDER_TTL / 2")
        # Both are bound on 127.0.0.1 at start, so one port for both stops the daemon then
        if self.gateway_port != 0 and self.api_port == self.gateway_port:
            raise ValueError("api_port and gateway_port must differ")

    def _validate_core_types(self):
        """Raise ValueError for a core setting of the wrong type or out of range.

        type() rather than isinstance(): true is an int to Python, but not a count or a port anyone means; and
        a string such as "false" or "no" in rnfs.toml must not read as a true flag.
        """
        for name, (minimum, maximum) in WHOLE_NUMBERS.items():
            value = getattr(self, name)
            if type(value) is not int or value < minimum or value > (maximum if maximum is not None else MAX_TOML_INT):
                limits = f"{minimum}-{maximum}" if maximum is not None else f"at least {minimum}"
                raise ValueError(f"{name} must be a whole number, {limits}")
        for name, minimum in OPTIONAL_WHOLE_NUMBERS.items():
            value = getattr(self, name)
            if value is not None and (type(value) is not int or not minimum <= value <= MAX_TOML_INT):
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
            least = MIN_DURATIONS.get(name)
            if least is not None:
                if not _is_number(value) or value < least or value > MAX_DURATION:
                    raise ValueError(f"{name} must be a number of seconds, at least {least:g} and at most "
                                     f"{MAX_DURATION}{unset if name in OPTIONAL else ''}")
                continue
            if not _is_number(value) or value < 0 or (positive and value == 0) or value > MAX_DURATION:
                raise ValueError(f"{name} must be a number of seconds, {'above' if positive else 'at least'} 0 "
                                 f"and at most {MAX_DURATION}{unset if name in OPTIONAL else ''}")
        for name in FLAGS:
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be true or false")
        for name in OPTIONAL_FLAGS:
            if getattr(self, name) is not None and type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be true or false, or unset")
        for name in IDENTITY_LISTS:
            values = getattr(self, name)
            if not isinstance(values, list) or not all(ids.is_hex16(value, brackets=True) for value in values):
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
        path = settings_path(rnfs_dir)
        data, warnings = read_settings_file(path)
        config = cls.combine(rnfs_dir, parse_settings(data, path), defaults, overrides, path)
        warnings.extend(config.warnings)  # the retired settings left out
        warnings.extend(config.check_reticulum_config())
        config.warnings = warnings
        return config

    def check_reticulum_config(self) -> list[str]:
        """Check RNS's `config` when reticulum_dir is the rnfs dir (earlier versions' layout, where RNS reads its
        interfaces, and the commands they run, from there) as open_checked does: the warnings of making it
        private. Raises PermissionError if it can't be trusted, OSError if it can't be opened; nothing to check
        for another reticulum_dir."""
        root = self.root
        reticulum_root = self.reticulum_root
        if reticulum_root is None or os.path.realpath(reticulum_root) != os.path.realpath(root):
            return []
        opened = open_checked(os.path.join(root, "config"), "the Reticulum config")
        if opened is None:
            return []
        fd, warning = opened
        os.close(fd)
        return [f"{warning}. Check that its interfaces are yours"] if warning is not None else []

    @classmethod
    def combine(cls, rnfs_dir: str, file_values: dict, defaults: dict | None = None, overrides: dict | None = None,
                path: str = SETTINGS_FILE) -> "Config":
        """The settings of rnfs_dir from rnfs.toml's values (file_values, as parse_settings gives them, read from
        path), over defaults and under overrides (None values ignored), validated as Config.load does, with
        overrides and given_defaults recorded. Retired settings (RETIRED_SETTINGS) are left out, wherever they come
        from, each with a warning in the result's warnings. Raises ValueError (TypeError for an override that isn't
        a field).
        """
        # An embedded node's defaults are the app's, not the user's: left out without a word
        retired = sorted((file_values.keys() | (overrides or {}).keys()) & RETIRED_SETTINGS.keys())
        warnings = tuple(f"ignoring {name}{f' in {path}' if name in file_values else ''}: it is no longer a setting "
                         f"({RETIRED_SETTINGS[name]}); remove it" for name in retired)
        defaults, file_values, overrides = _current(defaults), _current(file_values), _current(overrides)
        values: dict = dict(defaults)
        values.update(file_values)
        known = {f.name for f in fields(cls)}
        unknown = values.keys() - known
        if unknown:
            raise ValueError(f"unknown settings in {path}: {', '.join(sorted(unknown))}")
        if "rnfs_dir" in values:
            # The file lives in the rnfs dir, so it can't move it; --dir does
            raise ValueError(f"rnfs_dir can't be set in {path}; use --dir")
        given = {k: v for k, v in overrides.items() if v is not None}
        values.update(given)
        values["rnfs_dir"] = rnfs_dir
        config = cls(**values)
        config.validate()
        config.overrides = MappingProxyType(given)
        config.given_defaults = MappingProxyType(defaults)
        config.warnings = warnings
        return config


def _current(values: dict | None) -> dict:
    """values (setting name -> value; None: none) without the retired settings."""
    return {name: value for name, value in (values or {}).items() if name not in RETIRED_SETTINGS}


def settings_path(rnfs_dir: str) -> str:
    """Where the settings of rnfs_dir are: <rnfs dir>/rnfs.toml."""
    return os.path.join(os.path.expanduser(rnfs_dir), SETTINGS_FILE)


def read_settings_file(path: str) -> tuple[bytes | None, list[str]]:
    """rnfs.toml's bytes at path (None if there is none) and the warnings of making it private, once checked as
    open_checked checks it. Raises PermissionError if it can't be trusted, ValueError if it is larger than
    MAX_SETTINGS_SIZE, OSError if it can't be read."""
    # Its settings choose the Reticulum config, and with it programs rnfs starts, and the API port the CLI
    # sends its token to: whoever could change it could choose them
    opened = open_checked(path, "the settings file")
    if opened is None:
        return None, []
    fd, warning = opened
    warnings = [f"{warning}. Check that its settings are yours"] if warning is not None else []
    with os.fdopen(fd, "rb") as f:
        data = f.read(MAX_SETTINGS_SIZE + 1)
    if len(data) > MAX_SETTINGS_SIZE:
        raise ValueError(f"{path} is larger than {MAX_SETTINGS_SIZE // MIB} MiB")
    return data, warnings


def parse_settings(data: bytes | None, path: str = SETTINGS_FILE) -> dict:
    """The values rnfs.toml's bytes data set ({} for None), not yet checked. Raises ValueError (naming path) if
    they aren't UTF-8 TOML, or are nested too deeply to parse."""
    if data is None:
        return {}
    try:
        # utf-8-sig: a byte-order mark at the start, as some Windows editors save, is no reason to refuse the file
        return tomllib.loads(data.decode("utf-8-sig"))
    except ValueError as e:  # UnicodeDecodeError and tomllib.TOMLDecodeError are ValueErrors
        raise ValueError(f"{path} isn't valid TOML: {e}") from None
    except RecursionError:
        # tomllib recurses per nested array or table, so a few hundred levels exhaust Python's stack: no setting nests
        # at all, so the file is as unusable as one that isn't TOML, and is said so rather than shown as a traceback
        raise ValueError(f"{path} isn't valid TOML: it is nested too deeply to parse") from None


# The schema


def settings_schema() -> list[dict]:
    """Every setting rnfs.toml may hold (rnfs_dir, which it can't, is left out), in Config's order, each a new
    JSON-ready dict: {"name", "group" (SECTIONS), "type" ("int", "float", "bool", "string", "list[string]" or
    "choice"), "choices" (a list for "choice", else None), "nullable" (whether it may be unset, taking the default
    its description says), "default", "unit" ("seconds", "bytes", "bits/s", "bytes/hour" or None), "minimum",
    "maximum" (None for no bound), "exclusive_minimum" (true: the value must be above minimum), "description",
    "live" (a change through the API applies without a restart)}. Standard library only, so apps and the CLI can
    use it without a daemon."""
    starts = dict((first, group) for group, first in SECTIONS)
    group = "core"
    schema = []
    for f in fields(Config):
        group = starts.get(f.name, group)
        if f.name == "rnfs_dir":
            continue
        schema.append(_schema_entry(f, group))
    return schema


def _schema_entry(f, group: str) -> dict:
    """The schema entry of dataclass field f of Config, in group (see settings_schema)."""
    name = f.name
    nullable = type(None) in getattr(f.type, "__args__", ())
    base = next((t for t in getattr(f.type, "__args__", ()) if t is not type(None)), f.type) if nullable else f.type
    kind = "choice" if name in CHOICES else _TYPE_NAMES[base]
    minimum = maximum = None
    exclusive = False
    if name in WHOLE_NUMBERS:
        minimum, maximum = WHOLE_NUMBERS[name]
    elif name in OPTIONAL_WHOLE_NUMBERS:
        minimum = OPTIONAL_WHOLE_NUMBERS[name]
    elif name in RATES:
        minimum, exclusive = 0, True
    elif name in MIN_DURATIONS:
        minimum, maximum = MIN_DURATIONS[name], MAX_DURATION
    elif name in DURATIONS:
        minimum, maximum, exclusive = 0, MAX_DURATION, DURATIONS[name]
    elif name in FEATURE_LIMITS:
        minimum, maximum = FEATURE_LIMITS[name]
    unit = "seconds" if name in DURATIONS else "bits/s" if name in RATES else UNITS.get(name)
    default = f.default_factory() if f.default is MISSING else f.default
    return {"name": name, "group": group, "type": kind, "choices": list(CHOICES[name]) if name in CHOICES else None,
            "nullable": nullable, "default": default, "unit": unit, "minimum": minimum, "maximum": maximum,
            "exclusive_minimum": exclusive, "description": DESCRIPTIONS[name], "live": name in LIVE_SETTINGS}


# Editing rnfs.toml


def toml_value(value) -> str:
    """value written as a TOML value: a bool, a whole number (64-bit), a finite float, a string, or a list of
    these. Raises ValueError for anything else, which rnfs.toml can't hold."""
    if type(value) is bool:
        return "true" if value else "false"
    if type(value) is int:
        if not -MAX_TOML_INT - 1 <= value <= MAX_TOML_INT:
            raise ValueError(f"{value} is too large for rnfs.toml (64-bit whole numbers)")
        return str(value)
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("rnfs.toml can't hold inf or nan")
        return repr(value)  # "0.5", "1e+16": both TOML floats
    if type(value) is str:
        return _toml_string(value)
    if type(value) is list:
        return "[" + ", ".join(toml_value(item) for item in value) + "]"
    raise ValueError(f"rnfs.toml can't hold a {type(value).__name__} value")


def _toml_string(text: str) -> str:
    """text as a TOML basic string: quote, backslash and control characters escaped, and the characters other
    tools take for line breaks (NEL, U+2028, U+2029: TOML allows them raw, but an editor may split the line
    there). ValueError for text that isn't Unicode text (a lone surrogate, which JSON can carry)."""
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError("a setting's text must be valid Unicode") from None
    escapes = {'"': '\\"', "\\": "\\\\", "\b": "\\b", "\t": "\\t", "\n": "\\n", "\f": "\\f", "\r": "\\r"}
    out = []
    for c in text:
        if c in escapes:
            out.append(escapes[c])
        elif ord(c) < 0x20 or ord(c) == 0x7F or c in "\x85  ":
            out.append(f"\\u{ord(c):04x}")
        else:
            out.append(c)
    return '"' + "".join(out) + '"'


def edit_settings(text: str, changes: dict) -> str:
    """rnfs.toml's text with each setting in changes (name -> value, or None to remove it) changed, and every
    other line kept: comments, blank lines and the other settings as they were written.

    text must be flat TOML (settings only, no tables), as Config.load accepts. A setting already there has its
    assignment replaced where it is, all the lines its value spans (a multi-line array), keeping a comment that
    ends its last line; one that isn't is added at the end; None removes the assignment. Every statement setting
    a name is replaced (a dotted key, `name.x = 1`, or an inline table too): the first by the new assignment, the
    others removed. Comments inside a replaced multi-line value go with it. Lines end at "\\n" only, as TOML's
    do (NEL, U+2028 and U+2029 may be in its strings and comments). Linear in text's size: each statement's
    extent is found by _statement_extent and parsed once. Raises ValueError if text isn't flat TOML or a value
    can't be written (toml_value).
    """
    try:
        tomllib.loads(text)  # TOMLDecodeError (a ValueError) here: every statement below is complete
    except RecursionError:  # as parse_settings: no setting nests, and a statement below can't nest deeper
        raise ValueError("rnfs.toml isn't valid TOML: it is nested too deeply to parse") from None
    first_line = text.split("\n", 1)[0]
    newline = "\r\n" if first_line.endswith("\r") and len(first_line) < len(text) else "\n"
    out: list[str] = []
    seen: set[str] = set()
    position = 0
    while position < len(text):
        line_break = text.find("\n", position)
        line_end = len(text) if line_break < 0 else line_break + 1
        line = text[position:line_end]
        stripped = line.strip(" \t\r\n")
        if not stripped or stripped.startswith("#"):
            out.append(line)
            position = line_end
            continue
        where = f"line {text.count(chr(10), 0, position) + 1} of rnfs.toml"
        if stripped.startswith("["):
            raise ValueError(f"{where} starts a table, which rnfs settings never use; edit it by hand")
        value_end, end = _statement_extent(text, position)
        try:
            parsed = tomllib.loads(text[position:end])
        except tomllib.TOMLDecodeError:
            raise ValueError(f"{where} isn't one setting; edit it by hand") from None
        if len(parsed) != 1:
            raise ValueError(f"{where} isn't one setting; edit it by hand")
        name = next(iter(parsed))
        if name in changes:
            if changes[name] is not None and name not in seen:
                indent = line[:len(line) - len(line.lstrip(" \t"))]
                comment = _trailing_comment(text, position, value_end, end, parsed)
                out.append(f"{indent}{name} = {toml_value(changes[name])}{comment}{newline}")
            seen.add(name)
        else:
            out.append(text[position:end])
        position = end
    if out and not out[-1].endswith("\n"):
        out[-1] += newline
    for name, value in changes.items():
        if name not in seen and value is not None:
            out.append(f"{name} = {toml_value(value)}{newline}")
    return "".join(out)


def _statement_extent(text: str, start: int) -> tuple[int, int]:
    """The statement (key = value) starting at text[start]: (the index after the last character of its value,
    before any spaces and comment that end its line; the index after the line break ending it, or len(text)).

    A small lexer, linear in the statement's length: it skips TOML's strings (basic with escapes, literal, and
    their multi-line forms, whose closing delimiter may take up to two more quotes) and counts brackets and
    braces, so a line break or # ends the statement only outside them. Only an extent: the caller parses it."""
    depth = 0
    last = start  # after the last character that isn't a space, a line break or a comment
    i, n = start, len(text)
    while i < n:
        c = text[i]
        if c in "\"'":
            i = _string_end(text, i)
            last = i
            continue
        if c == "#":  # a comment, to the end of its line
            line_break = text.find("\n", i)
            if line_break < 0:
                return last, n
            if depth == 0:
                return last, line_break + 1
            i = line_break
            continue
        if c == "\n":
            if depth == 0:
                return last, i + 1
        elif c in "[{":
            depth += 1
        elif c in "]}":
            depth -= 1
        if c not in " \t\r\n":
            last = i + 1
        i += 1
    return last, n


def _string_end(text: str, start: int) -> int:
    """The index after the TOML string starting at text[start] (a quote), or where a single-line one meets a line
    break or text ends (not TOML: the caller's parse refuses it)."""
    quote = text[start]
    escapes = quote == '"'  # literal strings have none
    if text.startswith(quote * 3, start):
        i = start + 3
        while i < len(text):
            if escapes and text[i] == "\\":
                i += 2
            elif text.startswith(quote * 3, i):
                i += 3
                for _ in range(2):  # """a"""" is a" and a closing """
                    if i < len(text) and text[i] == quote:
                        i += 1
                return i
            else:
                i += 1
        return len(text)
    i = start + 1
    while i < len(text) and text[i] != "\n":
        if escapes and text[i] == "\\":
            i += 2
        elif text[i] == quote:
            return i + 1
        else:
            i += 1
    return min(i, len(text))


def _trailing_comment(text: str, start: int, value_end: int, end: int, parsed: dict) -> str:
    """The comment ending the statement text[start:end] (which parses as parsed, its value ending at value_end),
    with the spaces before it, or "". Checked: the statement without it parses the same."""
    tail = text[value_end:end].rstrip("\r\n")
    if not tail.lstrip(" \t").startswith("#"):
        return ""
    try:
        if tomllib.loads(text[start:value_end]) != parsed:
            return ""
    except tomllib.TOMLDecodeError:
        return ""
    return tail


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
