"""Protocol constants (specification section 12). Every node MUST use these values.

CHUNK_SIZE and MAX_CHILDREN decide CIDs; the rest decide validity and interoperability.
"""

KIB = 1024
MIB = 1024 * KIB
GIB = 1024 * MIB
HOUR = 3600
DAY = 24 * HOUR

CHUNK_SIZE = 64 * KIB
MAX_CHILDREN = 256
MAX_BLOCK_SIZE = 128 * KIB
MAX_DEPTH = 64
K = 10  # bucket size, /find reply size and replication
PROVIDER_TTL = 7 * DAY
MAX_REQUEST_SIZE = 1 * KIB  # including RNS request framing
MAX_REQUESTS_PER_LINK = 2  # at most one of them /blocks
MAX_RESPONSE_SIZE = 1 * MIB - 1  # encoded response; one RNS Resource segment

# Not tunable, but fixed by the spec's text rather than its constants table
ID_LENGTH = 16  # node IDs and DHT keys: RNS destination hashes, 128 bits
BUCKET_COUNT = 128
RESPONSE_OVERHEAD = 1 * KIB  # room left for msgpack framing in a /blocks response (section 7)
MAX_PROVIDE_KEYS = 55  # keys in one /provide, both lists: the most that fit MAX_REQUEST_SIZE (section 4)

# Local, not protocol: a file being written out (.rnfs-get-<random>.partial, .rnfs-job-<id>.partial), then renamed
PARTIAL_SUFFIX = ".partial"

APP_NAME = "rnfs"
DHT_ASPECT = "dht"  # rnfs.dht: every node's request destination. A breaking change uses a new aspect
NAME_ASPECT = "name"  # rnfs.name: a name's announce destination

# Request paths (section 3)
FIND_PATH = "/find"
PROVIDE_PATH = "/provide"
BLOCKS_PATH = "/blocks"
PIN_PATH = "/pin"

# DHT mode carried in the rnfs.dht announce's app_data (section 3)
MODE_CLIENT = 0
MODE_SERVER = 1

# Archive header (section 8)
ARCHIVE_MAGIC = "rnfs-archive"
ARCHIVE_VERSION = 1

# Local, not protocol: the project's always-on DHT servers, which a join contacts after bootstrap_nodes unless
# default_bootstrap is off or the node is in private mode (section 6). Node IDs, changed only by a release
DEFAULT_BOOTSTRAP_NODES = (
    "d8be7a8a1a2c3beb2cdd0d33e7225c21",  # Broken Hill, Australia
    "5e395b3045b68c881d5db62c146033eb",  # Sydney, Australia (AWS)
    "bddad1ec2f4a4a6112d3d42afabfd94a",  # Broken Hill, Australia
    "24770a7a9221b97d583905a839bc4fce",  # Central US (Google Cloud)
    "4a8dec339ac23f9a6f423b6ee0a84c16",  # Madrid, Spain (Google Cloud)
    "4ac0af1cb64587d1e71103a1733e91de",  # Johannesburg, South Africa (Google Cloud)
    "8ce2903ad6aaee3128c6494647b5496f",  # Montreal, Canada (Google Cloud)
    "3c7a4a76eec780fbe56ed6e13184981c",  # Stockholm, Sweden (Google Cloud)
)
