<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NBA tracking-value fixtures (2023-24)](#nba-tracking-value-fixtures-2023-24)
  - [Design-doc corrections found during capture](#design-doc-corrections-found-during-capture)
  - [Regenerating](#regenerating)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NBA tracking-value fixtures (2023-24)

Real captures backing `tests/nba/test_nba_tracking_value_*` — no synthetic data
for the offline oracle gates. Captured 2026-07-08 from a residential IP via
`SDV_PY_NBA_STATS_LIVE=1 uv run python dev/nba_tracking_value/capture_fixtures.py`.

All from `nba_stats_leaguedashptstats(season="2023-24", league_id="00",
per_mode_simple="Totals", player_or_team="Player", return_parsed=False)`,
one file per `pt_measure_type`:

| File | `pt_measure_type` | Rows |
|---|---|---|
| `leaguedashptstats_rebounding_2324.json` | `Rebounding` | 560 |
| `leaguedashptstats_possessions_2324.json` | `Possessions` | 572 |
| `leaguedashptstats_drives_2324.json` | `Drives` | 572 |
| `leaguedashptstats_catchshoot_2324.json` | `CatchShoot` | 572 |
| `leaguedashptstats_pullupshot_2324.json` | `PullUpShot` | 572 |
| `leaguedashptstats_defense_2324.json` | `Defense` | 572 |
| `leaguedashptstats_passing_2324.json` | `Passing` | 572 |

`player_positions_2324.parquet` — columns `player_id:Utf8, position_bucket:Utf8`,
built from `nba_player_positions("2023-24", league_id="00")` (numeric 1-5 scale
via `nba_stats_playerindex`) bucketed guard/wing/big by
`nba_tracking_value._position_num_to_bucket` (`<2.5` guard, `<3.75` wing, else
big). 5199 rows — `nba_stats_playerindex` is not season-scoped, so it returns
the full historical player universe; harmless, since every join is a left-join
FROM the (season-scoped) tracking frame.

## Design-doc corrections found during capture

The 2026-07-07 design/plan assumed column names later found not to match the
live payload. `nba_tracking_value_constants.MEASURE_SPECS` reflects the real
names; corrections:

- **Assists live on `pt_measure_type="Passing"`, not `"Possessions"`.**
  `Possessions` carries no `ast`/`passes` column at all. `Passing` supplies
  `ast`, `passes_made`, `potential_ast`, `ast_pts_created` (already computed —
  no need to hand-derive `ast*2+...`).
- **`Defense` exposes only rim-band defended shooting** (`def_rim_fgm`,
  `def_rim_fga`, `def_rim_fg_pct`) — there is no separate overall `d_fga`/
  `d_fg_pct`. This actually simplifies model ⑥: it is rim-only by
  construction, no extra `shotdefend` filtering needed to get a "rim" figure.
  There is also no `normal_fg_pct` (shooters'-average) column, so
  `rim_protect_pts_saved` always uses the bucket-mean-defended-rate baseline
  form noted in the plan's Σ=0 test note.
- **`Rebounding` has no contested/uncontested CHANCE columns** — only
  `reb_contest`/`reb_uncontest` (a split of made rebounds) and `reb_chances`
  (total opportunities, undifferentiated by difficulty). The
  contest-difficulty-adjusted expected value therefore always degrades to the
  plain `(reb, reb_chances)` rate on this endpoint — anticipated in the plan
  ("when the difficulty columns are absent it falls back to the single
  (actual, denom) rate").
- **Touch model's scoring column is `points`, not `pts`.**

## Regenerating

`dev/nba_tracking_value/capture_fixtures.py` (gitignored `dev/`, not
committed) reproduces the six `leaguedashptstats` captures + the positions
parquet; the `Passing` measure was captured separately with an equivalent
one-off call (same wrapper, `pt_measure_type="Passing"`).
