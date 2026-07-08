<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [nfl_scheme oracle fixture corpus](#nfl_scheme-oracle-fixture-corpus)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# nfl_scheme oracle fixture corpus

Committed offline validation corpus for the NFL scheme/special-teams model
spine (`nfl_playcall`, `nfl_gamescript`, `nfl_kicker_rating`,
`nfl_special_teams`, `nfl_line_grades`).

**Capture:** `dev/nfl_scheme/capture_fixtures.py`, run 2026-07-08 against the
live nflverse data releases via the `sportsdataverse.nfl` loaders.
`xpass` / `pass_oe` are recomputed with the shipped
`sportsdataverse.nfl.ep_wp.calculate_xpass` (bundled `xpass_model.ubj`), so
the "beat shipped xpass" oracle compares against this package's own model.

**Dtype policy:** `game_id`/`posteam`/`defteam`/`kicker_player_id`/
`punter_player_id`/`home_team`/`stadium`/`play_type` → Utf8;
`play_id`/`season`/`week` → Int64.

| File | Source | Seasons | Shape |
|---|---|---|---|
| `pbp_2021_2023_slice.parquet` | `load_nfl_pbp` + `calculate_xpass`, 42-column subset | 2021–2023 | 149021 x 42 |
| `fg_attempts_2019_2023.parquet` | same pipeline, `play_type == "field_goal"` | 2019–2023 | 5321 x 42 |
| `participation_2021_2023.parquet` | `load_nfl_pbp_participation` (per-season + `diagonal_relaxed` concat — the multi-season loader call crashes on a cross-season schema drift, width 20 vs 26) | 2021–2023 | 147032 x 7 |
| `pfr_advstats_2023.parquet` | `load_nfl_pfr_advstats([2023], stat_type="def", summary_level="season")` | 2023 | 926 x 30 |

**Actual column names vs the plan contract** (recorded per Task 0.3 step 2):

- participation keys on `nflverse_game_id` (not `game_id`); consumers rename.
- PFR def/season columns are PFR-abbreviated: player id = `pfr_id` (not
  `pfr_player_id`), team = `tm` (not `team`), pressures = `prss` (not
  `def_pressures`), sacks = `sk` (not `def_sacks`), plus `bltz`/`hrry`/`qbkd`.
- pbp slice carries every plan-contract column (none missing) plus
  `touchback` for punter net computation.
