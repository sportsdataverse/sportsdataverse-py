- **Crosswalk history:** a per-game fetch that fails on two runs (while the host answers other games) is recorded in
  `_missing.tsv`, and a retry of an already-failed game does not count toward the 10-in-a-row stop. Before, a run of
  broken games (Yahoo CFB 2020-10-24 answers HTTP 500 for every game) stopped every re-run at the same spot.
