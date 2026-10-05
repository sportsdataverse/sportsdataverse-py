---
title: "NFL — nflpro — Fantasy"
sidebar_label: "Fantasy"
sidebar_position: 2
description: "NFL — nflpro — Fantasy — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — nflpro — Fantasy

## nfl_pro_fantasy_season

GET /api/secured/stats/fantasy/season — one row per player for the season — fantasy points, opportunity and usage.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/fantasy/season`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/fantasy/season?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/fantasy/season?season=2024&seasonType=REG&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `nflId` | `nfl_id` |  |  | `Y` | Optional player filter. |
| `positionGroup` | `position_group` |  |  | `Y` | Optional position-group filter, e.g. ``QB``. Optional on this season scope — it is the ``game`` scope that requires it. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``fpHalfPPR``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |

### Returns {#nfl_pro_fantasy_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `nfl_id` | character | NFL ID of player (this is used in Big Data Bowl Data) |
| `display_name` | character | Full name of player |
| `short_name` | character | Player short name (i.e. "F.Last") |
| `headshot` | character | NFL headshot url for player |
| `team_id` | character | ESPN team id. |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `position` | character | Primary position as reported by NFL.com |
| `position_group` | character | Postion group of player as listed by NFL |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
| `o_snap` | integer |  |
| `o_snap3rd` | integer |  |
| `o_tm_snap` | integer |  |
| `o_snap_pg` | double |  |
| `pt_pct` | double |  |
| `pt_pct3rd` | double |  |
| `pass_cmp` | integer |  |
| `pass_att` | integer |  |
| `pass_yd` | integer |  |
| `pass_td` | integer | Binary flag for a passing touchdown. |
| `pass_int` | integer |  |
| `pass_two_pt_conv` | integer |  |
| `pass_db` | integer |  |
| `pass_cmp_pct` | double |  |
| `pass_exp_cmp_pct` | double |  |
| `pass_cpoe` | double |  |
| `pass_rating` | double |  |
| `pass_avg_ttt` | double |  |
| `pass_ay_pa` | double |  |
| `pass_deep_att_pct` | double |  |
| `pass_yac_pct` | double |  |
| `pass_qbp` | integer |  |
| `pass_qbp_pct` | double |  |
| `pass_sack` | integer |  |
| `pass_sack_pg` | double |  |
| `pass_cmp_pg` | double |  |
| `pass_att_pg` | double |  |
| `pass_yd_pg` | double |  |
| `pass_td_pg` | double |  |
| `pass_int_pg` | double |  |
| `pass_db_pg` | double |  |
| `rush_att` | integer |  |
| `rush_yd` | integer |  |
| `rush_td` | integer | Binary flag for a rushing touchdown. |
| `rush_two_pt_conv` | integer |  |
| `rush_exp_yd` | integer |  |
| `rush_ryoe` | integer |  |
| `rush_att_pg` | double |  |
| `rush_yd_pg` | double |  |
| `rush_yd_pa` | double |  |
| `rush_yaco` | double |  |
| `rush_ybco` | double |  |
| `rush_yaco_pa` | double |  |
| `rush_ybco_pa` | double |  |
| `rush_stuffed` | integer |  |
| `rush_td_pg` | double |  |
| `scr_rush_att` | integer |  |
| `scr_rush_yd` | integer |  |
| `scr_rush_td` | integer |  |
| `scr_rush_pct` | double |  |
| `design_rush_att` | integer |  |
| `design_rush_yd` | integer |  |
| `design_rush_td` | integer |  |
| `rush_rz_att` | integer |  |
| `rush_gl_att` | integer |  |
| `rush10_plus_yd` | integer |  |
| `rec_rt` | integer |  |
| `rec_tgt` | integer |  |
| `rec_rec` | integer |  |
| `rec_yd` | integer |  |
| `rec_td` | integer |  |
| `rec_two_pt_conv` | integer |  |
| `rec_rt_pg` | double |  |
| `rec_tgt_pg` | double |  |
| `rec_rec_pg` | double |  |
| `rec_yd_pg` | double |  |
| `rec_td_pg` | double |  |
| `rec_catch_pct` | double |  |
| `rec_ay_share` | double |  |
| `rec_tgt_rate` | double |  |
| `rec_tgt_share` | double |  |
| `rec_rt_part_pct` | double |  |
| `rec_tgt_quick` | integer |  |
| `rec_tgt_play_act` | integer |  |
| `rec_ez_tgt` | integer |  |
| `rec_ez_rec` | integer |  |
| `rec_rz_tgt` | integer |  |
| `rec_ay_tgt` | integer |  |
| `rec_ay_rec` | integer |  |
| `rec_ay_unrealized` | integer |  |
| `rec_ay_pt` | double |  |
| `rec_tgt_ay10_plus` | integer |  |
| `rec_yd_p_rt` | double |  |
| `rec_yd_pt` | double |  |
| `rec_yd_pr` | double |  |
| `rec_yac` | integer |  |
| `rec_exp_yac` | integer |  |
| `rec_yacoe` | integer |  |
| `kick_xp_att` | integer |  |
| `kick_xp_made` | integer |  |
| `kick_fg_att` | integer |  |
| `kick_fg_made` | integer |  |
| `kick_fg_miss` | integer |  |
| `kick_fg_made_less40` | integer |  |
| `kick_fg_made40_to49` | integer |  |
| `kick_fg_made50_to59` | integer |  |
| `kick_fg_made60_plus` | integer |  |
| `misc_kickoff_ret_td` | integer |  |
| `misc_punt_ret_td` | integer |  |
| `misc_fum_rec_td` | integer |  |
| `misc_fum_lost` | integer |  |
| `misc_fum` | integer |  |
| `misc_int_ret_td` | integer |  |
| `misc_fum_ret_td` | integer |  |
| `misc_blk_punt_fg_ret_td` | integer |  |
| `misc_two_pt_ret` | integer |  |
| `misc_one_pt_safety` | integer |  |
| `o_touch` | integer |  |
| `o_opp` | integer |  |
| `o_opp_pg` | double |  |
| `o_miss_tkl_forced` | integer |  |
| `o_miss_tkl_forced_pct` | double |  |
| `o_tm_db` | integer |  |
| `o_tm_pass_pct` | double |  |
| `o_tm_ppg` | double |  |
| `o_tm_yd_pg` | double |  |
| `rz_opp` | integer |  |
| `fp_std` | double |  |
| `fp_half_ppr` | double |  |
| `fp_ppr` | double |  |
| `fp_pass` | double |  |
| `fp_rush` | double |  |
| `fp_rec_std` | double |  |
| `fp_rec_half_ppr` | double |  |
| `fp_rec_ppr` | double |  |
| `fp_kick` | integer |  |
| `fp_misc` | integer |  |
| `fp_pg_std` | double |  |
| `fp_pg_half_ppr` | double |  |
| `fp_pgppr` | double |  |
| `fp_pos_rk_std` | integer |  |
| `fp_pos_rk_half_ppr` | integer |  |
| `fp_pos_rk_ppr` | integer |  |
| `fp_pos_rk_lbl_std` | character |  |
| `fp_pos_rk_lbl_half_ppr` | character |  |
| `fp_pos_rk_lbl_ppr` | character |  |
| `fp_ps_std` | double |  |
| `fp_ps_half_ppr` | double |  |
| `fp_psppr` | double |  |
| `fp_p_rt_std` | double |  |
| `fp_p_rt_half_ppr` | double |  |
| `fp_p_rt_ppr` | double |  |
| `fp_pt_std` | double |  |
| `fp_pt_half_ppr` | double |  |
| `fp_ptppr` | double |  |
| `fp_po_std` | double |  |
| `fp_po_half_ppr` | double |  |
| `fp_poppr` | double |  |
| `top5_qb_wk_std` | integer |  |
| `top12_qb_wk_std` | integer |  |
| `top12_rb_wk_std` | integer |  |
| `top24_rb_wk_std` | integer |  |
| `top12_wr_wk_std` | integer |  |
| `top24_wr_wk_std` | integer |  |
| `top36_wr_wk_std` | integer |  |
| `top5_te_wk_std` | integer |  |
| `top12_te_wk_std` | integer |  |
| `top5_k_wk_std` | integer |  |
| `top12_k_wk_std` | integer |  |
| `top5_qb_wk_half_ppr` | integer |  |
| `top12_qb_wk_half_ppr` | integer |  |
| `top12_rb_wk_half_ppr` | integer |  |
| `top24_rb_wk_half_ppr` | integer |  |
| `top12_wr_wk_half_ppr` | integer |  |
| `top24_wr_wk_half_ppr` | integer |  |
| `top36_wr_wk_half_ppr` | integer |  |
| `top5_te_wk_half_ppr` | integer |  |
| `top12_te_wk_half_ppr` | integer |  |
| `top5_k_wk_half_ppr` | integer |  |
| `top12_k_wk_half_ppr` | integer |  |
| `top5_qb_wk_ppr` | integer |  |
| `top12_qb_wk_ppr` | integer |  |
| `top12_rb_wk_ppr` | integer |  |
| `top24_rb_wk_ppr` | integer |  |
| `top12_wr_wk_ppr` | integer |  |
| `top24_wr_wk_ppr` | integer |  |
| `top36_wr_wk_ppr` | integer |  |
| `top5_te_wk_ppr` | integer |  |
| `top12_te_wk_ppr` | integer |  |
| `top5_k_wk_ppr` | integer |  |
| `top12_k_wk_ppr` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_pro_fantasy_season-example}

```python
nfl_pro_fantasy_season(season=2024, season_type='REG')
```

_Last validated n/a._

## nfl_pro_fantasy_game

GET /api/secured/stats/fantasy/game — one row per player-game — fantasy scoring by game. Requires `position_group`.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/fantasy/game`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/fantasy/game?season=2024&seasonType=REG&limit=500&positionGroup=QB](https://pro.nfl.com/api/secured/stats/fantasy/game?season=2024&seasonType=REG&limit=500&positionGroup=QB)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `nflId` | `nfl_id` |  |  | `Y` | Optional player filter. |
| `positionGroup` | `position_group` |  | `Y` |  | Position group, e.g. ``QB``. **Required**: this scope returns HTTP 500 without it, so it is a required argument rather than an optional filter. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``fpHalfPPR``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |

### Returns {#nfl_pro_fantasy_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `nfl_id` | character | NFL ID of player (this is used in Big Data Bowl Data) |
| `display_name` | character | Full name of player |
| `short_name` | character | Player short name (i.e. "F.Last") |
| `headshot` | character | NFL headshot url for player |
| `team_id` | character | ESPN team id. |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `position` | character | Primary position as reported by NFL.com |
| `position_group` | character | Postion group of player as listed by NFL |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
| `week_slug` | character |  |
| `game_id` | integer | Ten digit identifier for NFL game. |
| `fapi_game_id` | character |  |
| `opponent_team_id` | character | Unique identifier for the opponent team. |
| `is_home` | logical | Whether the subject team was the home team. |
| `final_score` | character |  |
| `game_result` | character | Game result for the player's team (`W`/`L`). |
| `o_snap` | integer |  |
| `o_snap3rd` | integer |  |
| `o_tm_snap` | integer |  |
| `o_snap_pg` | integer |  |
| `pt_pct` | double |  |
| `pt_pct3rd` | double |  |
| `pass_cmp` | integer |  |
| `pass_att` | integer |  |
| `pass_yd` | integer |  |
| `pass_td` | integer | Binary flag for a passing touchdown. |
| `pass_int` | integer |  |
| `pass_two_pt_conv` | integer |  |
| `pass_db` | integer |  |
| `pass_cmp_pct` | double |  |
| `pass_exp_cmp_pct` | double |  |
| `pass_cpoe` | double |  |
| `pass_rating` | double |  |
| `pass_avg_ttt` | double |  |
| `pass_ay_pa` | double |  |
| `pass_deep_att_pct` | double |  |
| `pass_yac_pct` | double |  |
| `pass_qbp` | integer |  |
| `pass_qbp_pct` | double |  |
| `pass_sack` | integer |  |
| `pass_sack_pg` | integer |  |
| `pass_cmp_pg` | integer |  |
| `pass_att_pg` | integer |  |
| `pass_yd_pg` | integer |  |
| `pass_td_pg` | integer |  |
| `pass_int_pg` | integer |  |
| `pass_db_pg` | integer |  |
| `rush_att` | integer |  |
| `rush_yd` | integer |  |
| `rush_td` | integer | Binary flag for a rushing touchdown. |
| `rush_two_pt_conv` | integer |  |
| `rush_exp_yd` | integer |  |
| `rush_ryoe` | integer |  |
| `rush_att_pg` | integer |  |
| `rush_yd_pg` | integer |  |
| `rush_yd_pa` | double |  |
| `rush_yaco` | double |  |
| `rush_ybco` | double |  |
| `rush_yaco_pa` | double |  |
| `rush_ybco_pa` | double |  |
| `rush_stuffed` | integer |  |
| `rush_td_pg` | integer |  |
| `scr_rush_att` | integer |  |
| `scr_rush_yd` | integer |  |
| `scr_rush_td` | integer |  |
| `scr_rush_pct` | double |  |
| `design_rush_att` | integer |  |
| `design_rush_yd` | integer |  |
| `design_rush_td` | integer |  |
| `rush_rz_att` | integer |  |
| `rush_gl_att` | integer |  |
| `rush10_plus_yd` | integer |  |
| `rec_rt` | integer |  |
| `rec_tgt` | integer |  |
| `rec_rec` | integer |  |
| `rec_yd` | integer |  |
| `rec_td` | integer |  |
| `rec_two_pt_conv` | integer |  |
| `rec_rt_pg` | integer |  |
| `rec_tgt_pg` | integer |  |
| `rec_rec_pg` | integer |  |
| `rec_yd_pg` | integer |  |
| `rec_td_pg` | integer |  |
| `rec_catch_pct` | integer |  |
| `rec_ay_share` | integer |  |
| `rec_tgt_rate` | integer |  |
| `rec_tgt_share` | integer |  |
| `rec_rt_part_pct` | double |  |
| `rec_tgt_quick` | integer |  |
| `rec_tgt_play_act` | integer |  |
| `rec_ez_tgt` | integer |  |
| `rec_ez_rec` | integer |  |
| `rec_rz_tgt` | integer |  |
| `rec_ay_tgt` | integer |  |
| `rec_ay_rec` | integer |  |
| `rec_ay_unrealized` | integer |  |
| `rec_ay_pt` | integer |  |
| `rec_tgt_ay10_plus` | integer |  |
| `rec_yd_p_rt` | integer |  |
| `rec_yd_pt` | integer |  |
| `rec_yd_pr` | integer |  |
| `rec_yac` | integer |  |
| `rec_exp_yac` | integer |  |
| `rec_yacoe` | integer |  |
| `kick_xp_att` | integer |  |
| `kick_xp_made` | integer |  |
| `kick_fg_att` | integer |  |
| `kick_fg_made` | integer |  |
| `kick_fg_miss` | integer |  |
| `kick_fg_made_less40` | integer |  |
| `kick_fg_made40_to49` | integer |  |
| `kick_fg_made50_to59` | integer |  |
| `kick_fg_made60_plus` | integer |  |
| `misc_kickoff_ret_td` | integer |  |
| `misc_punt_ret_td` | integer |  |
| `misc_fum_rec_td` | integer |  |
| `misc_fum_lost` | integer |  |
| `misc_fum` | integer |  |
| `misc_int_ret_td` | integer |  |
| `misc_fum_ret_td` | integer |  |
| `misc_blk_punt_fg_ret_td` | integer |  |
| `misc_two_pt_ret` | integer |  |
| `misc_one_pt_safety` | integer |  |
| `o_touch` | integer |  |
| `o_opp` | integer |  |
| `o_opp_pg` | integer |  |
| `o_miss_tkl_forced` | integer |  |
| `o_miss_tkl_forced_pct` | integer |  |
| `o_tm_db` | integer |  |
| `o_tm_pass_pct` | double |  |
| `o_tm_ppg` | integer |  |
| `o_tm_yd_pg` | integer |  |
| `rz_opp` | integer |  |
| `fp_std` | double |  |
| `fp_half_ppr` | double |  |
| `fp_ppr` | double |  |
| `fp_pass` | double |  |
| `fp_rush` | double |  |
| `fp_rec_std` | integer |  |
| `fp_rec_half_ppr` | integer |  |
| `fp_rec_ppr` | integer |  |
| `fp_kick` | integer |  |
| `fp_misc` | integer |  |
| `fp_pg_std` | double |  |
| `fp_pg_half_ppr` | double |  |
| `fp_pgppr` | double |  |
| `fp_pos_rk_std` | integer |  |
| `fp_pos_rk_half_ppr` | integer |  |
| `fp_pos_rk_ppr` | integer |  |
| `fp_pos_rk_lbl_std` | character |  |
| `fp_pos_rk_lbl_half_ppr` | character |  |
| `fp_pos_rk_lbl_ppr` | character |  |
| `fp_ps_std` | double |  |
| `fp_ps_half_ppr` | double |  |
| `fp_psppr` | double |  |
| `fp_p_rt_std` | double |  |
| `fp_p_rt_half_ppr` | double |  |
| `fp_p_rt_ppr` | double |  |
| `fp_pt_std` | integer |  |
| `fp_pt_half_ppr` | integer |  |
| `fp_ptppr` | integer |  |
| `fp_po_std` | double |  |
| `fp_po_half_ppr` | double |  |
| `fp_poppr` | double |  |
| `top5_qb_wk_std` | integer |  |
| `top12_qb_wk_std` | integer |  |
| `top12_rb_wk_std` | integer |  |
| `top24_rb_wk_std` | integer |  |
| `top12_wr_wk_std` | integer |  |
| `top24_wr_wk_std` | integer |  |
| `top36_wr_wk_std` | integer |  |
| `top5_te_wk_std` | integer |  |
| `top12_te_wk_std` | integer |  |
| `top5_k_wk_std` | integer |  |
| `top12_k_wk_std` | integer |  |
| `top5_qb_wk_half_ppr` | integer |  |
| `top12_qb_wk_half_ppr` | integer |  |
| `top12_rb_wk_half_ppr` | integer |  |
| `top24_rb_wk_half_ppr` | integer |  |
| `top12_wr_wk_half_ppr` | integer |  |
| `top24_wr_wk_half_ppr` | integer |  |
| `top36_wr_wk_half_ppr` | integer |  |
| `top5_te_wk_half_ppr` | integer |  |
| `top12_te_wk_half_ppr` | integer |  |
| `top5_k_wk_half_ppr` | integer |  |
| `top12_k_wk_half_ppr` | integer |  |
| `top5_qb_wk_ppr` | integer |  |
| `top12_qb_wk_ppr` | integer |  |
| `top12_rb_wk_ppr` | integer |  |
| `top24_rb_wk_ppr` | integer |  |
| `top12_wr_wk_ppr` | integer |  |
| `top24_wr_wk_ppr` | integer |  |
| `top36_wr_wk_ppr` | integer |  |
| `top5_te_wk_ppr` | integer |  |
| `top12_te_wk_ppr` | integer |  |
| `top5_k_wk_ppr` | integer |  |
| `top12_k_wk_ppr` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_pro_fantasy_game-example}

```python
nfl_pro_fantasy_game(season=2024, season_type='REG', position_group='QB')
```

_Last validated n/a._
