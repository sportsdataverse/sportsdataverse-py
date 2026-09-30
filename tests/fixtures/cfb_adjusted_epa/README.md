<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [cfb_adjusted_epa fixtures — provenance](#cfb_adjusted_epa-fixtures--provenance)
  - [`pre598_*.parquet` — expected output of the pre-#598 method](#pre598_parquet--expected-output-of-the-pre-598-method)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# cfb_adjusted_epa fixtures — provenance

| file | rows | source | notes |
|---|---|---|---|
| `fbs_plays_2026_wk1_4.parquet` | 22,779 | `cfbfastR-cfb-data` @ `8fca03a91` (2026-09-26): `cfb/pbp/parquet/play_by_play_2026.parquet` + `load_cfb_schedule(seasons=[2026])`, passed through that repo's own `cfb_data_build.summaries_input.prepare_plays_input` (FBS vs FBS, pass/rush, kneels dropped) | Down-selected to the columns `cfb_adjusted_epa_by_game` reads plus `wp_before` (12 in all; `wp_before_naive` has no nulls). Weeks 1-4, 185 games, 138 teams. `cfb_adjusted_epa` on it reproduces the published `cfb_team_summaries_2026` adjusted columns exactly (max abs diff 0) on sdv-py `80e287c57`. `pos_team_id` is Utf8 and `def_pos_team_id` Int64, as the build input ships them. |

Why this snapshot: on 2026-09-26 the live gameonpaper.com team board ranked
Utah State and Washington State 1st and 2nd by net adjusted EPA/play while
their raw net ranked 124th and 123rd. The test suite pins that regression.

## `pre598_*.parquet` — expected output of the pre-#598 method

| file | rows | input |
|---|---|---|
| `pre598_season_fbs2026.parquet` | 135 | `fbs_plays_2026_wk1_4.parquet` |
| `pre598_by_game_fbs2026.parquet` | 369 | `fbs_plays_2026_wk1_4.parquet` |
| `pre598_season_nfl_synthetic.parquet` | 6 | `_nfl_shaped_pbp()` in `tests/cfb/test_cfb_adjusted_epa.py` (300 rows, `wp_before` only) |
| `pre598_by_game_nfl_synthetic.parquet` | 30 | same |

Each is `cfb_adjusted_epa(frame)` / `cfb_adjusted_epa_by_game(frame)` with default
arguments, run on sdv-py `b05cb2112` (`bd1987493^`, the parent of #598) in its own
worktree and venv (same `uv.lock` as the commit that added these files). The tests
assert `method="pre598"` reproduces them to 1e-12; the observed gap is 0 to 1.1e-16,
the same last-ULP noise the pre-#598 code shows between two of its own runs
(polars' threaded group-by means).
