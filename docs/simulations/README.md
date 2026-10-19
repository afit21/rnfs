# Simulations behind the 0.2.0 plan

The studies whose measured parameters `roadmap.md`'s 0.2.0 section uses. Each folder's `RESULTS.md` is the
study's report; the scripts beside it are the code that produced it, kept as it was run (they are research code:
`ruff` leaves this folder out, and nothing in rnfs or its tests imports them).

| Folder | Question | Used for |
|---|---|---|
| [load](load/RESULTS.md) | What republishing costs at scale; sweep providing; slow nodes' capacity; server load and `K` | Sweep providing, the record budget's headroom, `provide_entries` on slow nodes |
| [storage](storage/RESULTS.md) | Which record-storage policy keeps honest publishers' records under a flood | Storage tiers, their floors, Earned, the 4.5-day newcomer expiry |
| [eclipse](eclipse/RESULTS.md) | How to find content when identities are generated next to its key | The density check, disjoint paths, the announce first-seen log |
| [topology](topology/RESULTS.md) | Lookups across LoRa islands and the internet backbone | Slow-path demotion, path-request budgets, two-level records, partition mode |

The load study's scripts and full tables were lost (they lived in a temporary directory cleared by a reboot);
its folder holds the study's own summary.

Running them: Python 3.11 or later, from the folder of the study (each imports its `sim.py` from there), for
example `cd docs/simulations/topology && python3 experiments.py connected 300`; see each `RESULTS.md` for the
experiments and their arguments. They write their raw output next to themselves. `eclipse/validate.py` checks the
eclipse model against rnfs's own `RoutingTable` and `Lookup` and imports them from this tree: it was written
against rnfs as it was before 0.2.0 (the planning session, commit 23a8c5b), so run it on a checkout of that
commit (`git worktree add`), since 0.2.0 changed both classes. The other scripts don't use rnfs.
