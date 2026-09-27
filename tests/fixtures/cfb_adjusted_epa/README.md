# cfb_adjusted_epa fixtures — provenance

| file | rows | source | notes |
|---|---|---|---|
| `fbs_plays_2026_wk1_4.parquet` | 22,779 | `cfbfastR-cfb-data` @ `8fca03a91` (2026-09-26): `cfb/pbp/parquet/play_by_play_2026.parquet` + `load_cfb_schedule(seasons=[2026])`, passed through that repo's own `cfb_data_build.summaries_input.prepare_plays_input` (FBS vs FBS, pass/rush, kneels dropped) | Down-selected to the columns `cfb_adjusted_epa_by_game` reads plus `wp_before` (12 in all; `wp_before_naive` has no nulls). Weeks 1-4, 185 games, 138 teams. `cfb_adjusted_epa` on it reproduces the published `cfb_team_summaries_2026` adjusted columns exactly (max abs diff 0) on sdv-py `80e287c57`. `pos_team_id` is Utf8 and `def_pos_team_id` Int64, as the build input ships them. |

Why this snapshot: on 2026-09-26 the live gameonpaper.com team board ranked
Utah State and Washington State 1st and 2nd by net adjusted EPA/play while
their raw net ranked 124th and 123rd. The test suite pins that regression.
