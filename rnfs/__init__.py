"""rnfs: content-addressed file sharing over Reticulum (RNS).

content/     CIDs, DAGs, storage, pins, archives, shared folder (no RNS)
dht/         Kademlia routing table, provider records, iterative lookups (no RNS)
network/     everything that talks to RNS: identity, links, request client and server, announces
names.py     mutable names
fetcher.py   fetching targets from providers
publisher.py provide strategy, publishing and withdrawal in the DHT
node.py      RnfsNode, which ties the above together into a running node and offers the facade
api.py       local HTTP API; gateway.py the HTTP gateway
cli.py       command line: run a daemon, or talk to one through its API
"""

__version__ = "0.1.4"
