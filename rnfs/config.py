"""Local settings (specification section 12) and where a node keeps its files.

Each node may change these, in `<rnfs dir>/rnfs.toml` or on the command line. Times are in seconds,
sizes in bytes and rates in bits/s. A setting left as None takes a default that depends on whether
the node (or link) is slow; use the `effective_*` helpers rather than reading those fields directly.
"""
import os
import tomllib
from dataclasses import dataclass, field, fields

from rnfs.constants import DAY, GIB, HOUR, KIB, PROVIDER_TTL

DEFAULT_API_PORT = 8788
DEFAULT_GATEWAY_PORT = 8789
SETTINGS_FILE = "rnfs.toml"


@dataclass
class Config:
    """Settings for an RnfsNode."""

    rnfs_dir: str = "~/.rnfs/"  # Reticulum config, identity, blocks, pins, names and shared folder live here
    api_port: int = DEFAULT_API_PORT
    gateway_port: int = DEFAULT_GATEWAY_PORT
    bootstrap_nodes: list[str] = field(default_factory=list)  # node IDs (hex) queried at startup
    verbosity: int = 0  # added to Reticulum's loglevel: 1 shows per-query detail, 2 shows RNS debug output

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
    lookup_timeout: float = 600
    path_timeout: float = 30
    max_failures: int = 3
    link_idle_timeout: float = 300
    max_incoming_links: int = 128
    max_blocks_responses: int = 4  # at least 2
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
    provide_strategy: str = "pinned"  # "pinned" or "roots"
    gateway_scripts: bool = True
    gateway_fetches: int = 2

    # Paths

    @property
    def root(self) -> str:
        """rnfs_dir with ~ expanded."""
        return os.path.expanduser(self.rnfs_dir)

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
        """Raise ValueError for settings the spec forbids."""
        if self.slow not in ("yes", "no", "auto"):
            raise ValueError("slow must be yes, no or auto")
        if self.dht_mode not in ("server", "client"):
            raise ValueError("dht_mode must be server or client")
        if self.provide_strategy not in ("pinned", "roots"):
            raise ValueError("provide_strategy must be pinned or roots")
        if self.max_blocks_responses < 2:
            raise ValueError("max_blocks_responses must be at least 2")
        for slow_node in (False, True):
            if self.effective_republish_interval(slow_node) >= PROVIDER_TTL / 2:
                raise ValueError("republish_interval must be less than PROVIDER_TTL / 2")

    @classmethod
    def load(cls, rnfs_dir: str = "~/.rnfs/", **overrides) -> "Config":
        """Settings from `<rnfs_dir>/rnfs.toml` if it exists, then overrides (None values ignored), validated."""
        values: dict = {}
        path = os.path.join(os.path.expanduser(rnfs_dir), SETTINGS_FILE)
        if os.path.isfile(path):
            with open(path, "rb") as f:
                values.update(tomllib.load(f))
        known = {f.name for f in fields(cls)}
        unknown = values.keys() - known
        if unknown:
            raise ValueError(f"unknown settings in {path}: {', '.join(sorted(unknown))}")
        values.update({k: v for k, v in overrides.items() if v is not None})
        values["rnfs_dir"] = rnfs_dir
        config = cls(**values)
        config.validate()
        return config
