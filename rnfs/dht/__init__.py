"""The provider DHT (specification section 6): routing table, provider records and iterative lookups.

Network access, identity knowledge and clocks are injected, so nothing here reaches the network itself
(routing and lookups don't import RNS; provider records load it only through rnfs.log).
"""
