- **Crosswalk history:** a per-game key that failed on three separate runs is recorded in `_missing.tsv` even when
  no later game in the run answers: a finished season's re-run requests only its broken games (Yahoo CFB 2020-10-24),
  so #1182's "a later game answered" rule never fired for them. `_failed.tsv` gets one line per failed run. (#1189)
