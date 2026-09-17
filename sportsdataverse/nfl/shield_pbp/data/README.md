# Vendored data — `native_pbp`

## `scramble_fix.csv`

**Source:** nflfastR's internal `R/sysdata.rda` object `scramble_fix` — a plain
character vector of 5,830 `"{game_id}_{play_id}"` keys, generated once at
nflfastR package-build time by `data-raw/build_scramble_fix.R` from three
vendored Football-Outsiders / Aaron-Schatz charting-data spreadsheets checked
into nflfastR's own `data-raw/`:

- `data-raw/scrambles_2005.xlsx` (2005 season)
- `data-raw/Scrambles 1999-2004 UPDATE for NFLfastR.xlsx` (1999-2004, filtered
  to `type %in% c("scramble", "assume scramble")`)
- `data-raw/Scrambles.1999-2003.FURTHER.UPDATE.for.NFLfastR.xlsx` — a
  correction list; plays in this file are **excluded** from the final vector
  (they were miscoded as scrambles and are actually rushes; see nflfastR
  issue #475), plus one hardcoded exclusion
  (`scramble_id != "2005_09_CIN_BAL_1725"`).

`scramble_fix` marks scrambles in the 1999-2005 seasons that the NFL's raw
play-by-play feed did not flag as scrambles in the play description text.

**Retrieval:** the full, complete 5,830-row list was extracted from
`R/sysdata.rda` and committed to this repo's
`docs/superpowers/plans/2026-07-03-nflfastr-scramble-fix.csv` as part of the
nflfastR-parity reference extraction (2026-07-03). This file
(`scramble_fix.csv`) is a byte-identical copy of that vendored CSV, placed
under `native_pbp/data/` so `native_pbp.repairs` can load it via
`Path(__file__).parent / "data"` without reaching back into `docs/`.

**Format:** single column `scramble_id` (header row + 5,830 data rows), one
`"{game_id}_{play_id}"` key per row — e.g. `1999_01_MIN_ATL_133`.

**Do NOT** attempt to regenerate this file from the original Excel charting
spreadsheets — the correction-exclusion pass (`s3` in
`build_scramble_fix.R`) makes an independent re-derivation NOT equivalent
unless that exact exclusion logic is reproduced. Treat this CSV as the
canonical, complete artifact.

**No network access** is required to use or refresh this file — it is a
static, pre-extracted vendor copy checked into the repo.
