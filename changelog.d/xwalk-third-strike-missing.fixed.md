- **Crosswalk history:** a per-game key that failed on three separate runs is recorded in `_missing.tsv` even when no
  later game in the run answers. A finished season's re-run requests only its broken games (Yahoo CFB 2020-10-24), so
  the "a later game answered" rule from #1182 never fired for them. `_failed.tsv` now gets one line per failed run.
