<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Box prior v2 and REML season rating corpus (BB-D38)](#box-prior-v2-and-reml-season-rating-corpus-bb-d38)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Box prior v2 and REML season rating corpus (BB-D38)

Real NBA data for `tests/nba/test_nba_box_prior.py`, `tests/nba/test_nba_reml_rapm.py` and
`tests/tools/test_fit_box_prior_v2.py`. Nothing is synthetic or edited: rows are only filtered and columns
only dropped. Parquet files are zstd level 19 unless noted. Seasons are named by their end year
(2024 = 2023-24). Every id is Int64, as in the sources.

Inputs read on 2026-11-10:

- `nba_stats_possessions` release assets 1997-2026 (`nba_possessions_{yr}.parquet`, pulled 2026-11-08);
- the hoopR-nba-stats-raw checkout at `5da1f96bf5`: `leaguegamelog/{yr}/regular-season{,_p}.json` (box
  logs, game dates), `leaguedashplayerbiostats/{yr}/regular-season_totals.json` (age) and
  `playerindex/2026.json` (the historical index: one height and listed position per player, used for every season).

| File | Rows | What |
|---|---:|---|
| `nba_possessions_2024_slice.parquet` | 19,939 | The first 100 regular-season games of 2023-24 (`game_id` order). Cols: `game_id, points, off_player_1..5, def_player_1..5`. |
| `nba_player_logs_2024_slice.parquet` | 2,145 | Those games' leaguegamelog `P` rows. Cols: `game_id, game_date, player_id, team_id, min, pts, fg3_m, fga, fta, ast, oreb, dreb, stl, blk, tov, pf`. |
| `nba_team_logs_2024_slice.parquet` | 200 | Those games' leaguegamelog `T` rows. Cols: `game_id, team_id, min, fga, oreb, tov, fta`. |
| `nba_bio_2023_2024.parquet` (snappy) | 839 | The slice's players in 2022-23 and 2023-24: `player_id, season, age, height_in, position_num` (age from biostats, the nearest captured season +/- the gap when a season is missing; height in inches and position 1-5 from the playerindex). |
| `nba_panel_2022_2023.parquet` | 1,138 | The 2021-22 and 2022-23 rows of the panel the 2023-24 prior trained on: the `feat_{yr}` features of `tools/fit_box_prior_v2.py panel` (`player_id, poss, def_poss`, the 22 `BOX_PRIOR_FEATURES`, `season`) with `o_target, d_target` from `career_rapm_target` over 1997-2023 centred on 2024. |
| `gate.parquet` | 1,044 | `tools/fit_box_prior_v2.py gate` for every regular season 1998-2026: one row per season, split (`fold0..4`: five game-grouped folds; `chrono`: fit before the date by which 60% of the games were played, score the rest) and method (`adj`, `rapm`, `v2`, `v2_fixed`, `adj_reml`, `prior`), with the held-out `sse` and `n` (possessions), and for the REML methods `lambda_o, lambda_d` and the possession-weighted prior weights. |
| `report.json` | | `tools/fit_box_prior_v2.py report`: pooled MSE, paired differences against `adj` (basis points; mean, SE, seasons, seasons `v2` wins), the REML penalties and prior weights per season, and the long-window oracle summary. |

The long-window oracle block compares pooled 2020-2026 ratings with an outside 8-season RAPM shared for SPM
work under no license. Only its summary numbers (Spearman, standard deviation, counts) are committed;
the oracle file itself is not, and neither is any per-player value from it. The current SPM, BPM, RAPM
and adjusted RAPM in that block are the `nba_player_impact_{2020..2026}` regular-season rows built with
sportsdataverse-py #1150.

Regenerate (about 15 minutes, local inputs; nothing reaches the Data API):

```sh
uv run --frozen python tools/fit_box_prior_v2.py panel OUT --poss-dir POSS --raw RAW
uv run --frozen python tools/fit_box_prior_v2.py prior OUT
uv run --frozen python tools/fit_box_prior_v2.py gate OUT --poss-dir POSS --raw RAW --adj-prior ADJ
uv run --frozen python tools/fit_box_prior_v2.py report OUT --oracle ORACLE.csv --impact-dir IMPACT
```

`ADJ` is the `curves` output of `tools/fit_adj_rapm_penalty.py` (its `nba_{yr}_prior_next.parquet`, the
previous season's SPM that today's adjusted RAPM shrinks toward). The slices and the panel were cut from
`OUT` and the inputs with a throwaway script; the filters are the ones in the table.
