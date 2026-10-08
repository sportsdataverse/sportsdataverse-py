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
| `nfl_id` | character | NFL player id (`nflId`) as a string; the key the wrappers accept as `nfl_id`. |
| `display_name` | character | Player's full display name (e.g. 'Aaron Rodgers'). |
| `short_name` | character | Player's abbreviated name, first initial and surname (e.g. 'A.Rodgers'). |
| `headshot` | character | NFL headshot image URL template; the `{formatInstructions}` token must be replaced with a Cloudinary transform (e.g. `t_headshot_desktop`) before use. |
| `team_id` | character | NFL team id as a zero-padded string (e.g. '0200', '3430'); casting it to a number drops the leading zero. |
| `jersey_number` | integer | Jersey number the player wore in the season. |
| `position` | character | Roster position abbreviation (e.g. QB, WR, CB). |
| `position_group` | character | Position group the roster position rolls up to (e.g. QB, RB, WR, TE, DB). |
| `gp` | integer | Games played in the season (and season type) -- the denominator of every `*_pg` per-game column. |
| `gs` | integer | Games started in the season. |
| `o_snap` | integer | Offensive snaps played. |
| `o_snap3rd` | integer |  |
| `o_tm_snap` | integer | Offensive snaps the player's team played; the denominator of pt_pct. |
| `o_snap_pg` | double | Offensive snaps per game (o_snap / gp). |
| `pt_pct` | double | Playing-time share: offensive snaps played over the team's offensive snaps (o_snap / o_tm_snap). |
| `pt_pct3rd` | double |  |
| `pass_cmp` | integer | Pass completions. |
| `pass_att` | integer | Pass attempts, season total. |
| `pass_yd` | integer | Passing yards, season total. |
| `pass_td` | integer | Passing touchdowns. |
| `pass_int` | integer | Interceptions thrown. |
| `pass_two_pt_conv` | integer | Two-point conversions passed. |
| `pass_db` | integer | Dropbacks, season total. |
| `pass_cmp_pct` | double | Completion percentage as a fraction (pass_cmp / pass_att). |
| `pass_exp_cmp_pct` | double | Expected completion percentage as a fraction, per Next Gen Stats; pass_cmp_pct = pass_exp_cmp_pct + pass_cpoe. |
| `pass_cpoe` | double | Completion percentage over expected as a fraction (pass_cmp_pct - pass_exp_cmp_pct). |
| `pass_rating` | double | NFL passer rating (the standard 0-158.3 formula). |
| `pass_avg_ttt` | double | Average time to throw in seconds, per Next Gen Stats. |
| `pass_ay_pa` | double | Average intended air yards per pass attempt. |
| `pass_deep_att_pct` | double | Share of pass attempts NGS classifies as deep throws. |
| `pass_yac_pct` | double | Share of passing yards gained after the catch. |
| `pass_qbp` | integer | Dropbacks on which the passer was pressured. |
| `pass_qbp_pct` | double | Pressure rate: share of dropbacks with pressure (pass_qbp / pass_db). |
| `pass_sack` | integer | Times sacked (a count, not a per-play flag). |
| `pass_sack_pg` | double | Sacks taken per game (pass_sack / gp). |
| `pass_cmp_pg` | double | Completions per game (pass_cmp / gp). |
| `pass_att_pg` | double | Pass attempts per game (pass_att / gp). |
| `pass_yd_pg` | double | Passing yards per game (pass_yd / gp). |
| `pass_td_pg` | double | Passing touchdowns per game (pass_td / gp). |
| `pass_int_pg` | double | Interceptions thrown per game (pass_int / gp). |
| `pass_db_pg` | double | Dropbacks per game (pass_db / gp). |
| `rush_att` | integer | Rush attempts, season total. |
| `rush_yd` | integer | Rushing yards (rush_exp_yd + rush_ryoe). |
| `rush_td` | integer | Rushing touchdowns. |
| `rush_two_pt_conv` | integer | Two-point conversions rushed. |
| `rush_exp_yd` | integer | Expected rushing yards, per Next Gen Stats. |
| `rush_ryoe` | integer | Rushing yards over expected (rush_yd - rush_exp_yd). |
| `rush_att_pg` | double | Rush attempts per game (rush_att / gp). |
| `rush_yd_pg` | double | Rushing yards per game (rush_yd / gp). |
| `rush_yd_pa` | double | Rushing yards per attempt (rush_yd / rush_att). |
| `rush_yaco` | double | Rushing yards after contact. |
| `rush_ybco` | double | Rushing yards before contact. |
| `rush_yaco_pa` | double | Yards after contact per rush attempt (rush_yaco / rush_att). |
| `rush_ybco_pa` | double | Yards before contact per rush attempt (rush_ybco / rush_att). |
| `rush_stuffed` | integer | Rush attempts stuffed (stopped at or behind the line of scrimmage). |
| `rush_td_pg` | double | Rushing touchdowns per game (rush_td / gp). |
| `scr_rush_att` | integer | Scramble rush attempts (quarterback runs off a dropback). |
| `scr_rush_yd` | integer | Scramble rushing yards. |
| `scr_rush_td` | integer | Scramble rushing touchdowns. |
| `scr_rush_pct` | double | Scramble rate: scrambles per dropback (scr_rush_att / pass_db). |
| `design_rush_att` | integer | Designed rush attempts (called runs, excluding scrambles). |
| `design_rush_yd` | integer | Designed-rush yards. |
| `design_rush_td` | integer | Designed-rush touchdowns. |
| `rush_rz_att` | integer | Rush attempts inside the red zone. |
| `rush_gl_att` | integer | Rush attempts at the goal line. |
| `rush10_plus_yd` | integer | Rush attempts that gained 10 or more yards. |
| `rec_rt` | integer | Routes run, season total. |
| `rec_tgt` | integer | Targets, season total. |
| `rec_rec` | integer | Receptions, season total. |
| `rec_yd` | integer | Receiving yards. |
| `rec_td` | integer | Receiving touchdowns. |
| `rec_two_pt_conv` | integer | Two-point conversions received. |
| `rec_rt_pg` | double | Routes run per game (rec_rt / gp). |
| `rec_tgt_pg` | double | Targets per game (rec_tgt / gp). |
| `rec_rec_pg` | double | Receptions per game (rec_rec / gp). |
| `rec_yd_pg` | double | Receiving yards per game (rec_yd / gp). |
| `rec_td_pg` | double | Receiving touchdowns per game (rec_td / gp). |
| `rec_catch_pct` | double | Catch rate as a fraction (rec_rec / rec_tgt). |
| `rec_ay_share` | double | Share of the team's intended air yards thrown to the player. |
| `rec_tgt_rate` | double | Target rate: targets per route run (rec_tgt / rec_rt). |
| `rec_tgt_share` | double | Share of the team's targets thrown to the player. |
| `rec_rt_part_pct` | double | Route participation: share of the team's dropbacks on which the player ran a route. |
| `rec_tgt_quick` | integer |  |
| `rec_tgt_play_act` | integer | Targets on play-action dropbacks. |
| `rec_ez_tgt` | integer | End-zone targets. |
| `rec_ez_rec` | integer | End-zone receptions. |
| `rec_rz_tgt` | integer | Targets inside the red zone. |
| `rec_ay_tgt` | integer | Total intended air yards on targets (rec_ay_rec + rec_ay_unrealized). |
| `rec_ay_rec` | integer | Air yards on completed targets (realized air yards). |
| `rec_ay_unrealized` | integer | Air yards on incomplete targets (unrealized air yards). |
| `rec_ay_pt` | double | Average intended air yards per target. |
| `rec_tgt_ay10_plus` | integer | Targets of 10 or more intended air yards. |
| `rec_yd_p_rt` | double | Receiving yards per route run (rec_yd / rec_rt). |
| `rec_yd_pt` | double | Receiving yards per target (rec_yd / rec_tgt). |
| `rec_yd_pr` | double | Receiving yards per reception (rec_yd / rec_rec). |
| `rec_yac` | integer | Yards after the catch. |
| `rec_exp_yac` | integer | Expected yards after the catch, per Next Gen Stats. |
| `rec_yacoe` | integer | Yards after the catch over expected (rec_yac - rec_exp_yac). |
| `kick_xp_att` | integer | Extra-point attempts. |
| `kick_xp_made` | integer | Extra points made. |
| `kick_fg_att` | integer | Field-goal attempts. |
| `kick_fg_made` | integer | Field goals made. |
| `kick_fg_miss` | integer | Field goals missed. |
| `kick_fg_made_less40` | integer | Field goals made from under 40 yards. |
| `kick_fg_made40_to49` | integer | Field goals made from 40-49 yards. |
| `kick_fg_made50_to59` | integer | Field goals made from 50-59 yards. |
| `kick_fg_made60_plus` | integer | Field goals made from 60 yards or more. |
| `misc_kickoff_ret_td` | integer | Kickoff-return touchdowns. |
| `misc_punt_ret_td` | integer | Punt-return touchdowns. |
| `misc_fum_rec_td` | integer | Fumble-recovery touchdowns. |
| `misc_fum_lost` | integer | Fumbles lost, season total. |
| `misc_fum` | integer | Fumbles, season total. |
| `misc_int_ret_td` | integer | Interception-return touchdowns. |
| `misc_fum_ret_td` | integer | Fumble-return touchdowns. |
| `misc_blk_punt_fg_ret_td` | integer | Touchdowns on blocked-punt or blocked-field-goal returns. |
| `misc_two_pt_ret` | integer | Two-point conversion returns (defensive two-point scores). |
| `misc_one_pt_safety` | integer | One-point safeties. |
| `o_touch` | integer | Touches: rush attempts plus receptions (rush_att + rec_rec). |
| `o_opp` | integer | Opportunities: rush attempts plus targets (rush_att + rec_tgt). |
| `o_opp_pg` | double | Opportunities per game (o_opp / gp). |
| `o_miss_tkl_forced` | integer | Missed tackles forced on touches. |
| `o_miss_tkl_forced_pct` | double | Missed tackles forced per touch (o_miss_tkl_forced / o_touch). |
| `o_tm_db` | integer | Team dropbacks. |
| `o_tm_pass_pct` | double | Team pass rate: team dropbacks over team offensive snaps (o_tm_db / o_tm_snap). |
| `o_tm_ppg` | double | Team points per game. |
| `o_tm_yd_pg` | double | Team yards per game. |
| `rz_opp` | integer | Red-zone opportunities: red-zone rushes plus red-zone targets (rush_rz_att + rec_rz_tgt). |
| `fp_std` | double | Fantasy points, standard (non-PPR) scoring. |
| `fp_half_ppr` | double | Fantasy points, half-PPR scoring. |
| `fp_ppr` | double | Fantasy points, full-PPR scoring. |
| `fp_pass` | double | Fantasy points from passing. |
| `fp_rush` | double | Fantasy points from rushing. |
| `fp_rec_std` | double | Fantasy points from receiving, standard scoring. |
| `fp_rec_half_ppr` | double | Fantasy points from receiving, half-PPR scoring. |
| `fp_rec_ppr` | double | Fantasy points from receiving, full-PPR scoring. |
| `fp_kick` | integer | Fantasy points from kicking. |
| `fp_misc` | integer | Fantasy points from return, fumble-recovery and other miscellaneous scoring. |
| `fp_pg_std` | double | Fantasy points per game, standard scoring (fp_std / gp). |
| `fp_pg_half_ppr` | double | Fantasy points per game, half-PPR scoring (fp_half_ppr / gp). |
| `fp_pgppr` | double | Fantasy points per game, full-PPR scoring (fp_ppr / gp). |
| `fp_pos_rk_std` | integer | Positional fantasy rank, standard scoring (1 = top scorer at the position). |
| `fp_pos_rk_half_ppr` | integer | Positional fantasy rank, half-PPR scoring (1 = top scorer at the position). |
| `fp_pos_rk_ppr` | integer | Positional fantasy rank, full-PPR scoring (1 = top scorer at the position). |
| `fp_pos_rk_lbl_std` | character | Positional rank label, standard scoring (e.g. 'QB1', 'RB12'). |
| `fp_pos_rk_lbl_half_ppr` | character | Positional rank label, half-PPR scoring (e.g. 'QB1', 'RB12'). |
| `fp_pos_rk_lbl_ppr` | character | Positional rank label, full-PPR scoring (e.g. 'QB1', 'RB12'). |
| `fp_ps_std` | double | Fantasy points per offensive snap, standard scoring (fp_std / o_snap). |
| `fp_ps_half_ppr` | double | Fantasy points per offensive snap, half-PPR scoring (fp_half_ppr / o_snap). |
| `fp_psppr` | double | Fantasy points per offensive snap, full-PPR scoring (fp_ppr / o_snap). |
| `fp_p_rt_std` | double | Fantasy points per route run, standard scoring. |
| `fp_p_rt_half_ppr` | double | Fantasy points per route run, half-PPR scoring. |
| `fp_p_rt_ppr` | double | Fantasy points per route run, full-PPR scoring. |
| `fp_pt_std` | double | Fantasy points per touch, standard scoring. |
| `fp_pt_half_ppr` | double | Fantasy points per touch, half-PPR scoring. |
| `fp_ptppr` | double | Fantasy points per touch, full-PPR scoring. |
| `fp_po_std` | double | Fantasy points per opportunity, standard scoring (fp_std / o_opp). |
| `fp_po_half_ppr` | double | Fantasy points per opportunity, half-PPR scoring (fp_half_ppr / o_opp). |
| `fp_poppr` | double | Fantasy points per opportunity, full-PPR scoring (fp_ppr / o_opp). |
| `top5_qb_wk_std` | integer | Weeks the player finished as a top-5 QB by fantasy points, standard scoring, season count. |
| `top12_qb_wk_std` | integer | Weeks the player finished as a top-12 QB by fantasy points, standard scoring, season count. |
| `top12_rb_wk_std` | integer | Weeks the player finished as a top-12 RB by fantasy points, standard scoring, season count. |
| `top24_rb_wk_std` | integer | Weeks the player finished as a top-24 RB by fantasy points, standard scoring, season count. |
| `top12_wr_wk_std` | integer | Weeks the player finished as a top-12 WR by fantasy points, standard scoring, season count. |
| `top24_wr_wk_std` | integer | Weeks the player finished as a top-24 WR by fantasy points, standard scoring, season count. |
| `top36_wr_wk_std` | integer | Weeks the player finished as a top-36 WR by fantasy points, standard scoring, season count. |
| `top5_te_wk_std` | integer | Weeks the player finished as a top-5 TE by fantasy points, standard scoring, season count. |
| `top12_te_wk_std` | integer | Weeks the player finished as a top-12 TE by fantasy points, standard scoring, season count. |
| `top5_k_wk_std` | integer | Weeks the player finished as a top-5 K by fantasy points, standard scoring, season count. |
| `top12_k_wk_std` | integer | Weeks the player finished as a top-12 K by fantasy points, standard scoring, season count. |
| `top5_qb_wk_half_ppr` | integer | Weeks the player finished as a top-5 QB by fantasy points, half-PPR scoring, season count. |
| `top12_qb_wk_half_ppr` | integer | Weeks the player finished as a top-12 QB by fantasy points, half-PPR scoring, season count. |
| `top12_rb_wk_half_ppr` | integer | Weeks the player finished as a top-12 RB by fantasy points, half-PPR scoring, season count. |
| `top24_rb_wk_half_ppr` | integer | Weeks the player finished as a top-24 RB by fantasy points, half-PPR scoring, season count. |
| `top12_wr_wk_half_ppr` | integer | Weeks the player finished as a top-12 WR by fantasy points, half-PPR scoring, season count. |
| `top24_wr_wk_half_ppr` | integer | Weeks the player finished as a top-24 WR by fantasy points, half-PPR scoring, season count. |
| `top36_wr_wk_half_ppr` | integer | Weeks the player finished as a top-36 WR by fantasy points, half-PPR scoring, season count. |
| `top5_te_wk_half_ppr` | integer | Weeks the player finished as a top-5 TE by fantasy points, half-PPR scoring, season count. |
| `top12_te_wk_half_ppr` | integer | Weeks the player finished as a top-12 TE by fantasy points, half-PPR scoring, season count. |
| `top5_k_wk_half_ppr` | integer | Weeks the player finished as a top-5 K by fantasy points, half-PPR scoring, season count. |
| `top12_k_wk_half_ppr` | integer | Weeks the player finished as a top-12 K by fantasy points, half-PPR scoring, season count. |
| `top5_qb_wk_ppr` | integer | Weeks the player finished as a top-5 QB by fantasy points, full-PPR scoring, season count. |
| `top12_qb_wk_ppr` | integer | Weeks the player finished as a top-12 QB by fantasy points, full-PPR scoring, season count. |
| `top12_rb_wk_ppr` | integer | Weeks the player finished as a top-12 RB by fantasy points, full-PPR scoring, season count. |
| `top24_rb_wk_ppr` | integer | Weeks the player finished as a top-24 RB by fantasy points, full-PPR scoring, season count. |
| `top12_wr_wk_ppr` | integer | Weeks the player finished as a top-12 WR by fantasy points, full-PPR scoring, season count. |
| `top24_wr_wk_ppr` | integer | Weeks the player finished as a top-24 WR by fantasy points, full-PPR scoring, season count. |
| `top36_wr_wk_ppr` | integer | Weeks the player finished as a top-36 WR by fantasy points, full-PPR scoring, season count. |
| `top5_te_wk_ppr` | integer | Weeks the player finished as a top-5 TE by fantasy points, full-PPR scoring, season count. |
| `top12_te_wk_ppr` | integer | Weeks the player finished as a top-12 TE by fantasy points, full-PPR scoring, season count. |
| `top5_k_wk_ppr` | integer | Weeks the player finished as a top-5 K by fantasy points, full-PPR scoring, season count. |
| `top12_k_wk_ppr` | integer | Weeks the player finished as a top-12 K by fantasy points, full-PPR scoring, season count. |

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
| `nfl_id` | character | NFL player id (`nflId`) as a string; the key the wrappers accept as `nfl_id`. |
| `display_name` | character | Player's full display name (e.g. 'Aaron Rodgers'). |
| `short_name` | character | Player's abbreviated name, first initial and surname (e.g. 'A.Rodgers'). |
| `headshot` | character | NFL headshot image URL template; the `{formatInstructions}` token must be replaced with a Cloudinary transform (e.g. `t_headshot_desktop`) before use. |
| `team_id` | character | NFL team id as a zero-padded string (e.g. '0200', '3430'); casting it to a number drops the leading zero. |
| `jersey_number` | integer | Jersey number the player wore in the season. |
| `position` | character | Roster position abbreviation (e.g. QB, WR, CB). |
| `position_group` | character | Position group the roster position rolls up to (e.g. QB, RB, WR, TE, DB). |
| `gp` | integer | Games played in the season (and season type) -- the denominator of every `*_pg` per-game column. |
| `gs` | integer | Games started in the season. |
| `week_slug` | character | Week slug of the game (e.g. 'WEEK_1', 'WEEK_18'); the week scope of the row. |
| `game_id` | integer | NFL game id as an integer (e.g. 2024090900, the date followed by a two-digit sequence). |
| `fapi_game_id` | character | NFL Football API (FAPI) UUID of the game, the id api.nfl.com uses for the same game. |
| `opponent_team_id` | character | Opponent's NFL team id as a zero-padded string. |
| `is_home` | logical | Whether the player's team was the home team in the game. |
| `final_score` | character | Final score as 'own-opponent' (e.g. '19-32' for a 32-19 loss). |
| `game_result` | character | Result from the player's team's side: 'W', 'L' or 'T'. |
| `o_snap` | integer | Offensive snaps played. |
| `o_snap3rd` | integer |  |
| `o_tm_snap` | integer | Offensive snaps the player's team played; the denominator of pt_pct. |
| `o_snap_pg` | integer | Offensive snaps per game (o_snap / gp). |
| `pt_pct` | double | Playing-time share: offensive snaps played over the team's offensive snaps (o_snap / o_tm_snap). |
| `pt_pct3rd` | double |  |
| `pass_cmp` | integer | Pass completions. |
| `pass_att` | integer | Pass attempts in the game. |
| `pass_yd` | integer | Passing yards in the game. |
| `pass_td` | integer | Passing touchdowns. |
| `pass_int` | integer | Interceptions thrown. |
| `pass_two_pt_conv` | integer | Two-point conversions passed. |
| `pass_db` | integer | Dropbacks in the game. |
| `pass_cmp_pct` | double | Completion percentage as a fraction (pass_cmp / pass_att). |
| `pass_exp_cmp_pct` | double | Expected completion percentage as a fraction, per Next Gen Stats; pass_cmp_pct = pass_exp_cmp_pct + pass_cpoe. |
| `pass_cpoe` | double | Completion percentage over expected as a fraction (pass_cmp_pct - pass_exp_cmp_pct). |
| `pass_rating` | double | NFL passer rating (the standard 0-158.3 formula). |
| `pass_avg_ttt` | double | Average time to throw in seconds, per Next Gen Stats. |
| `pass_ay_pa` | double | Average intended air yards per pass attempt. |
| `pass_deep_att_pct` | double | Share of pass attempts NGS classifies as deep throws. |
| `pass_yac_pct` | double | Share of passing yards gained after the catch. |
| `pass_qbp` | integer | Dropbacks on which the passer was pressured. |
| `pass_qbp_pct` | double | Pressure rate: share of dropbacks with pressure (pass_qbp / pass_db). |
| `pass_sack` | integer | Times sacked (a count, not a per-play flag). |
| `pass_sack_pg` | integer | Sacks taken per game (pass_sack / gp). |
| `pass_cmp_pg` | integer | Completions per game (pass_cmp / gp). |
| `pass_att_pg` | integer | Pass attempts per game (pass_att / gp). |
| `pass_yd_pg` | integer | Passing yards per game (pass_yd / gp). |
| `pass_td_pg` | integer | Passing touchdowns per game (pass_td / gp). |
| `pass_int_pg` | integer | Interceptions thrown per game (pass_int / gp). |
| `pass_db_pg` | integer | Dropbacks per game (pass_db / gp). |
| `rush_att` | integer | Rush attempts in the game. |
| `rush_yd` | integer | Rushing yards (rush_exp_yd + rush_ryoe). |
| `rush_td` | integer | Rushing touchdowns. |
| `rush_two_pt_conv` | integer | Two-point conversions rushed. |
| `rush_exp_yd` | integer | Expected rushing yards, per Next Gen Stats. |
| `rush_ryoe` | integer | Rushing yards over expected (rush_yd - rush_exp_yd). |
| `rush_att_pg` | integer | Rush attempts per game (rush_att / gp). |
| `rush_yd_pg` | integer | Rushing yards per game (rush_yd / gp). |
| `rush_yd_pa` | double | Rushing yards per attempt (rush_yd / rush_att). |
| `rush_yaco` | double | Rushing yards after contact. |
| `rush_ybco` | double | Rushing yards before contact. |
| `rush_yaco_pa` | double | Yards after contact per rush attempt (rush_yaco / rush_att). |
| `rush_ybco_pa` | double | Yards before contact per rush attempt (rush_ybco / rush_att). |
| `rush_stuffed` | integer | Rush attempts stuffed (stopped at or behind the line of scrimmage). |
| `rush_td_pg` | integer | Rushing touchdowns per game (rush_td / gp). |
| `scr_rush_att` | integer | Scramble rush attempts (quarterback runs off a dropback). |
| `scr_rush_yd` | integer | Scramble rushing yards. |
| `scr_rush_td` | integer | Scramble rushing touchdowns. |
| `scr_rush_pct` | double | Scramble rate: scrambles per dropback (scr_rush_att / pass_db). |
| `design_rush_att` | integer | Designed rush attempts (called runs, excluding scrambles). |
| `design_rush_yd` | integer | Designed-rush yards. |
| `design_rush_td` | integer | Designed-rush touchdowns. |
| `rush_rz_att` | integer | Rush attempts inside the red zone. |
| `rush_gl_att` | integer | Rush attempts at the goal line. |
| `rush10_plus_yd` | integer | Rush attempts that gained 10 or more yards. |
| `rec_rt` | integer | Routes run in the game. |
| `rec_tgt` | integer | Targets in the game. |
| `rec_rec` | integer | Receptions in the game. |
| `rec_yd` | integer | Receiving yards. |
| `rec_td` | integer | Receiving touchdowns. |
| `rec_two_pt_conv` | integer | Two-point conversions received. |
| `rec_rt_pg` | integer | Routes run per game (rec_rt / gp). |
| `rec_tgt_pg` | integer | Targets per game (rec_tgt / gp). |
| `rec_rec_pg` | integer | Receptions per game (rec_rec / gp). |
| `rec_yd_pg` | integer | Receiving yards per game (rec_yd / gp). |
| `rec_td_pg` | integer | Receiving touchdowns per game (rec_td / gp). |
| `rec_catch_pct` | integer | Catch rate as a fraction (rec_rec / rec_tgt). |
| `rec_ay_share` | integer | Share of the team's intended air yards thrown to the player. |
| `rec_tgt_rate` | integer | Target rate: targets per route run (rec_tgt / rec_rt). |
| `rec_tgt_share` | integer | Share of the team's targets thrown to the player. |
| `rec_rt_part_pct` | double | Route participation: share of the team's dropbacks on which the player ran a route. |
| `rec_tgt_quick` | integer |  |
| `rec_tgt_play_act` | integer | Targets on play-action dropbacks. |
| `rec_ez_tgt` | integer | End-zone targets. |
| `rec_ez_rec` | integer | End-zone receptions. |
| `rec_rz_tgt` | integer | Targets inside the red zone. |
| `rec_ay_tgt` | integer | Total intended air yards on targets (rec_ay_rec + rec_ay_unrealized). |
| `rec_ay_rec` | integer | Air yards on completed targets (realized air yards). |
| `rec_ay_unrealized` | integer | Air yards on incomplete targets (unrealized air yards). |
| `rec_ay_pt` | integer | Average intended air yards per target. |
| `rec_tgt_ay10_plus` | integer | Targets of 10 or more intended air yards. |
| `rec_yd_p_rt` | integer | Receiving yards per route run (rec_yd / rec_rt). |
| `rec_yd_pt` | integer | Receiving yards per target (rec_yd / rec_tgt). |
| `rec_yd_pr` | integer | Receiving yards per reception (rec_yd / rec_rec). |
| `rec_yac` | integer | Yards after the catch. |
| `rec_exp_yac` | integer | Expected yards after the catch, per Next Gen Stats. |
| `rec_yacoe` | integer | Yards after the catch over expected (rec_yac - rec_exp_yac). |
| `kick_xp_att` | integer | Extra-point attempts. |
| `kick_xp_made` | integer | Extra points made. |
| `kick_fg_att` | integer | Field-goal attempts. |
| `kick_fg_made` | integer | Field goals made. |
| `kick_fg_miss` | integer | Field goals missed. |
| `kick_fg_made_less40` | integer | Field goals made from under 40 yards. |
| `kick_fg_made40_to49` | integer | Field goals made from 40-49 yards. |
| `kick_fg_made50_to59` | integer | Field goals made from 50-59 yards. |
| `kick_fg_made60_plus` | integer | Field goals made from 60 yards or more. |
| `misc_kickoff_ret_td` | integer | Kickoff-return touchdowns. |
| `misc_punt_ret_td` | integer | Punt-return touchdowns. |
| `misc_fum_rec_td` | integer | Fumble-recovery touchdowns. |
| `misc_fum_lost` | integer | Fumbles lost in the game. |
| `misc_fum` | integer | Fumbles in the game. |
| `misc_int_ret_td` | integer | Interception-return touchdowns. |
| `misc_fum_ret_td` | integer | Fumble-return touchdowns. |
| `misc_blk_punt_fg_ret_td` | integer | Touchdowns on blocked-punt or blocked-field-goal returns. |
| `misc_two_pt_ret` | integer | Two-point conversion returns (defensive two-point scores). |
| `misc_one_pt_safety` | integer | One-point safeties. |
| `o_touch` | integer | Touches: rush attempts plus receptions (rush_att + rec_rec). |
| `o_opp` | integer | Opportunities: rush attempts plus targets (rush_att + rec_tgt). |
| `o_opp_pg` | integer | Opportunities per game (o_opp / gp). |
| `o_miss_tkl_forced` | integer | Missed tackles forced on touches. |
| `o_miss_tkl_forced_pct` | integer | Missed tackles forced per touch (o_miss_tkl_forced / o_touch). |
| `o_tm_db` | integer | Team dropbacks. |
| `o_tm_pass_pct` | double | Team pass rate: team dropbacks over team offensive snaps (o_tm_db / o_tm_snap). |
| `o_tm_ppg` | integer | Team points per game. |
| `o_tm_yd_pg` | integer | Team yards per game. |
| `rz_opp` | integer | Red-zone opportunities: red-zone rushes plus red-zone targets (rush_rz_att + rec_rz_tgt). |
| `fp_std` | double | Fantasy points, standard (non-PPR) scoring. |
| `fp_half_ppr` | double | Fantasy points, half-PPR scoring. |
| `fp_ppr` | double | Fantasy points, full-PPR scoring. |
| `fp_pass` | double | Fantasy points from passing. |
| `fp_rush` | double | Fantasy points from rushing. |
| `fp_rec_std` | integer | Fantasy points from receiving, standard scoring. |
| `fp_rec_half_ppr` | integer | Fantasy points from receiving, half-PPR scoring. |
| `fp_rec_ppr` | integer | Fantasy points from receiving, full-PPR scoring. |
| `fp_kick` | integer | Fantasy points from kicking. |
| `fp_misc` | integer | Fantasy points from return, fumble-recovery and other miscellaneous scoring. |
| `fp_pg_std` | double | Fantasy points per game, standard scoring (fp_std / gp). |
| `fp_pg_half_ppr` | double | Fantasy points per game, half-PPR scoring (fp_half_ppr / gp). |
| `fp_pgppr` | double | Fantasy points per game, full-PPR scoring (fp_ppr / gp). |
| `fp_pos_rk_std` | integer | Positional fantasy rank, standard scoring (1 = top scorer at the position). |
| `fp_pos_rk_half_ppr` | integer | Positional fantasy rank, half-PPR scoring (1 = top scorer at the position). |
| `fp_pos_rk_ppr` | integer | Positional fantasy rank, full-PPR scoring (1 = top scorer at the position). |
| `fp_pos_rk_lbl_std` | character | Positional rank label, standard scoring (e.g. 'QB1', 'RB12'). |
| `fp_pos_rk_lbl_half_ppr` | character | Positional rank label, half-PPR scoring (e.g. 'QB1', 'RB12'). |
| `fp_pos_rk_lbl_ppr` | character | Positional rank label, full-PPR scoring (e.g. 'QB1', 'RB12'). |
| `fp_ps_std` | double | Fantasy points per offensive snap, standard scoring (fp_std / o_snap). |
| `fp_ps_half_ppr` | double | Fantasy points per offensive snap, half-PPR scoring (fp_half_ppr / o_snap). |
| `fp_psppr` | double | Fantasy points per offensive snap, full-PPR scoring (fp_ppr / o_snap). |
| `fp_p_rt_std` | double | Fantasy points per route run, standard scoring. |
| `fp_p_rt_half_ppr` | double | Fantasy points per route run, half-PPR scoring. |
| `fp_p_rt_ppr` | double | Fantasy points per route run, full-PPR scoring. |
| `fp_pt_std` | integer | Fantasy points per touch, standard scoring. |
| `fp_pt_half_ppr` | integer | Fantasy points per touch, half-PPR scoring. |
| `fp_ptppr` | integer | Fantasy points per touch, full-PPR scoring. |
| `fp_po_std` | double | Fantasy points per opportunity, standard scoring (fp_std / o_opp). |
| `fp_po_half_ppr` | double | Fantasy points per opportunity, half-PPR scoring (fp_half_ppr / o_opp). |
| `fp_poppr` | double | Fantasy points per opportunity, full-PPR scoring (fp_ppr / o_opp). |
| `top5_qb_wk_std` | integer | Whether the player finished the week as a top-5 QB by fantasy points, standard scoring (1/0). |
| `top12_qb_wk_std` | integer | Whether the player finished the week as a top-12 QB by fantasy points, standard scoring (1/0). |
| `top12_rb_wk_std` | integer | Whether the player finished the week as a top-12 RB by fantasy points, standard scoring (1/0). |
| `top24_rb_wk_std` | integer | Whether the player finished the week as a top-24 RB by fantasy points, standard scoring (1/0). |
| `top12_wr_wk_std` | integer | Whether the player finished the week as a top-12 WR by fantasy points, standard scoring (1/0). |
| `top24_wr_wk_std` | integer | Whether the player finished the week as a top-24 WR by fantasy points, standard scoring (1/0). |
| `top36_wr_wk_std` | integer | Whether the player finished the week as a top-36 WR by fantasy points, standard scoring (1/0). |
| `top5_te_wk_std` | integer | Whether the player finished the week as a top-5 TE by fantasy points, standard scoring (1/0). |
| `top12_te_wk_std` | integer | Whether the player finished the week as a top-12 TE by fantasy points, standard scoring (1/0). |
| `top5_k_wk_std` | integer | Whether the player finished the week as a top-5 K by fantasy points, standard scoring (1/0). |
| `top12_k_wk_std` | integer | Whether the player finished the week as a top-12 K by fantasy points, standard scoring (1/0). |
| `top5_qb_wk_half_ppr` | integer | Whether the player finished the week as a top-5 QB by fantasy points, half-PPR scoring (1/0). |
| `top12_qb_wk_half_ppr` | integer | Whether the player finished the week as a top-12 QB by fantasy points, half-PPR scoring (1/0). |
| `top12_rb_wk_half_ppr` | integer | Whether the player finished the week as a top-12 RB by fantasy points, half-PPR scoring (1/0). |
| `top24_rb_wk_half_ppr` | integer | Whether the player finished the week as a top-24 RB by fantasy points, half-PPR scoring (1/0). |
| `top12_wr_wk_half_ppr` | integer | Whether the player finished the week as a top-12 WR by fantasy points, half-PPR scoring (1/0). |
| `top24_wr_wk_half_ppr` | integer | Whether the player finished the week as a top-24 WR by fantasy points, half-PPR scoring (1/0). |
| `top36_wr_wk_half_ppr` | integer | Whether the player finished the week as a top-36 WR by fantasy points, half-PPR scoring (1/0). |
| `top5_te_wk_half_ppr` | integer | Whether the player finished the week as a top-5 TE by fantasy points, half-PPR scoring (1/0). |
| `top12_te_wk_half_ppr` | integer | Whether the player finished the week as a top-12 TE by fantasy points, half-PPR scoring (1/0). |
| `top5_k_wk_half_ppr` | integer | Whether the player finished the week as a top-5 K by fantasy points, half-PPR scoring (1/0). |
| `top12_k_wk_half_ppr` | integer | Whether the player finished the week as a top-12 K by fantasy points, half-PPR scoring (1/0). |
| `top5_qb_wk_ppr` | integer | Whether the player finished the week as a top-5 QB by fantasy points, full-PPR scoring (1/0). |
| `top12_qb_wk_ppr` | integer | Whether the player finished the week as a top-12 QB by fantasy points, full-PPR scoring (1/0). |
| `top12_rb_wk_ppr` | integer | Whether the player finished the week as a top-12 RB by fantasy points, full-PPR scoring (1/0). |
| `top24_rb_wk_ppr` | integer | Whether the player finished the week as a top-24 RB by fantasy points, full-PPR scoring (1/0). |
| `top12_wr_wk_ppr` | integer | Whether the player finished the week as a top-12 WR by fantasy points, full-PPR scoring (1/0). |
| `top24_wr_wk_ppr` | integer | Whether the player finished the week as a top-24 WR by fantasy points, full-PPR scoring (1/0). |
| `top36_wr_wk_ppr` | integer | Whether the player finished the week as a top-36 WR by fantasy points, full-PPR scoring (1/0). |
| `top5_te_wk_ppr` | integer | Whether the player finished the week as a top-5 TE by fantasy points, full-PPR scoring (1/0). |
| `top12_te_wk_ppr` | integer | Whether the player finished the week as a top-12 TE by fantasy points, full-PPR scoring (1/0). |
| `top5_k_wk_ppr` | integer | Whether the player finished the week as a top-5 K by fantasy points, full-PPR scoring (1/0). |
| `top12_k_wk_ppr` | integer | Whether the player finished the week as a top-12 K by fantasy points, full-PPR scoring (1/0). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_pro_fantasy_game-example}

```python
nfl_pro_fantasy_game(season=2024, season_type='REG', position_group='QB')
```

_Last validated n/a._
