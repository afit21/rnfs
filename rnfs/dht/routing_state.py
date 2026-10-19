"""The routing table's saved state (specification section 6, "Routing table"): one row per node the table holds
something for, in the state database's routing table (rnfs.statedb), so a restart rejoins from the servers it
knew, they keep their seniority, a node dropped shortly before keeps its seniority to regain, and bans last their
time.

Kept apart from rnfs.dht.routing so that module (which lookups import) stays free of the state database. The
table tells this store of each change (RoutingTable.track); save() writes the rows changed since the last save, in
one transaction, not durably: the node calls it with its other batched saves (every 30 s) and at stop. A power
loss costs at most the last changes, which the announces and answers that follow make again.
"""
import os

from rnfs import statedb
from rnfs.dht.routing import RoutingTable
from rnfs.log import LOG_VERBOSE, log, plural

TABLE = "routing"


class RoutingState:
    """A node.Store saving table in the state database of state_dir."""

    def __init__(self, table: RoutingTable, state_dir: str):
        """Load table's saved rows into it (which must be unused yet), then track its changes."""
        self.table = table
        self._rows: statedb.RowStore | None = None
        self._db = statedb.open_store(os.path.expanduser(state_dir), self._load)

    def _load(self, db: statedb.StateDb):
        """Read the saved rows into the table; those it doesn't take as saved are written again (or deleted) by the
        next save."""
        self._rows = statedb.RowStore(db, TABLE, ("node",), durable=False, mode=statedb.SNAPSHOT,
                                      lock=self.table.lock, row=self.table.row)
        rows = db.query(f"SELECT node, state, first_seen, at, failures FROM {TABLE}")
        changed = self.table.restore(rows)
        with self.table.lock:
            self._rows.changed(*changed)
        self.table.track(self._rows.changed)
        if self.table.node_count():
            log(f"{plural(self.table.node_count(), 'DHT server')} known from the saved routing table", LOG_VERBOSE)

    def save(self):
        """Write the rows changed since the last save. Raises OSError (StateDbError) if the database fails; they are
        written by the next save."""
        if self._rows is not None:
            self._rows.save()

    def close(self):
        """Release the state database (idempotent)."""
        self._db.close()
