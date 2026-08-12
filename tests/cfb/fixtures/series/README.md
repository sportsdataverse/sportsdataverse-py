<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Series / first-down decomposition parity fixtures](#series--first-down-decomposition-parity-fixtures)
  - [Provenance](#provenance)
  - [Known, documented divergence](#known-documented-divergence)
  - [Regenerating](#regenerating)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Series / first-down decomposition parity fixtures

R-oracle golden fixtures for the `CFBPlayProcess.__add_series_data` port
(`firstD_by_kickoff` / `firstD_by_poss` / `firstD_by_penalty` /
`firstD_by_yards` + `new_series`).

## Provenance

- **Input CSVs** (`series_input_<gid>.csv`): real ESPN plays dumped from the
  offline pipeline (captured `tests/cfb/fixtures/summary_<gid>.json` payloads)
  by `dev/boxscore_parity/make_series_fixture.py`. Column names are renamed to
  cfbfastR's (`play_type`, `yards_gained`, `distance`, `down`, `id_drive`, ...)
  so the oracle script can run cfbfastR's code verbatim. `down` is the
  kickoff-normalized down (the pipeline's `__process_epa` kick-mask
  substitution, mirroring the semantics cfbfastR sees natively on CFBD input).
  The `py_*` columns are the Python pipeline's own series outputs at dump time
  (report convenience only; the tests recompute from the inputs).
- **Expected CSVs** (`series_expected_<gid>.csv`): produced by
  `dev/boxscore_parity/series_oracle.R` under **R 4.6.1 + dplyr**, applying
  cfbfastR's series logic verbatim:
  - `R/pbp_prep_epa_df_after.R` L194-298
  - `R/pbp_clean_drive_dat.R` L18-66, L300-312
  - `R/pbp_clean_pbp_dat.R` L388-390
- Games: 400869270 (2016), 401135269 (2019), 401309854 (2021),
  401754598 (2024).

## Known, documented divergence

R leaves `firstD_by_yards` / `new_series` `NA` where an unfilled lag2/lag3
feeds the condition (rows 1-2 of a half); the Python port emits `False`. The
tests fold R `NA` to 0 before comparing. Everything else is exact 0/1
equality.

## Regenerating

```sh
uv run python dev/boxscore_parity/make_series_fixture.py
"/c/Program Files/R/R-4.6.1/bin/Rscript.exe" dev/boxscore_parity/series_oracle.R
```
