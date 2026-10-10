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
REQUEST_FRAMING = 28  # approximate bytes RNS adds around a request's data (section 3)

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
