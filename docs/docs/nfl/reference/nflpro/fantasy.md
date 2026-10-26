---
title: "NFL — NFL Pro API (pro.nfl.com, NFL+ Premium account) — Fantasy"
sidebar_label: "Fantasy"
sidebar_position: 2
description: "NFL — NFL Pro API (pro.nfl.com, NFL+ Premium account) — Fantasy — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — NFL Pro API (pro.nfl.com, NFL+ Premium account) — Fantasy

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
| `nfl_id` | String | NFL player id (`nflId`) as a string; the key the wrappers accept as `nfl_id`. |
| `display_name` | String | Player's full display name (e.g. 'Aaron Rodgers'). |
| `short_name` | String | Player's abbreviated name, first initial and surname (e.g. 'A.Rodgers'). |
| `headshot` | String | NFL headshot image URL template; the `{formatInstructions}` token must be replaced with a Cloudinary transform (e.g. `t_headshot_desktop`) before use. |
| `team_id` | String | NFL team id as a zero-padded string (e.g. '0200', '3430'); casting it to a number drops the leading zero. |
| `jersey_number` | Int64 | Jersey number the player wore in the season. |
| `position` | String | Roster position abbreviation (e.g. QB, WR, CB). |
| `position_group` | String | Position group the roster position rolls up to (e.g. QB, RB, WR, TE, DB). |
| `gp` | Int64 | Games played in the season (and season type) -- the denominator of every `*_pg` per-game column. |
| `gs` | Int64 | Games started in the season. |
| `o_snap` | Int64 | Offensive snaps played. |
| `o_snap3rd` | Int64 |  |
| `o_tm_snap` | Int64 | Offensive snaps the player's team played; the denominator of pt_pct. |
| `o_snap_pg` | Float64 | Offensive snaps per game (o_snap / gp). |
| `pt_pct` | Float64 | Playing-time share: offensive snaps played over the team's offensive snaps (o_snap / o_tm_snap). |
| `pt_pct3rd` | Float64 |  |
| `pass_cmp` | Int64 | Pass completions. |
| `pass_att` | Int64 | Pass attempts, season total. |
| `pass_yd` | Int64 | Passing yards, season total. |
| `pass_td` | Int64 | Passing touchdowns. |
| `pass_int` | Int64 | Interceptions thrown. |
| `pass_two_pt_conv` | Int64 | Two-point conversions passed. |
| `pass_db` | Int64 | Dropbacks, season total. |
| `pass_cmp_pct` | Float64 | Completion percentage as a fraction (pass_cmp / pass_att). |
| `pass_exp_cmp_pct` | Float64 | Expected completion percentage as a fraction, per Next Gen Stats; pass_cmp_pct = pass_exp_cmp_pct + pass_cpoe. |
| `pass_cpoe` | Float64 | Completion percentage over expected as a fraction (pass_cmp_pct - pass_exp_cmp_pct). |
| `pass_rating` | Float64 | NFL passer rating (the standard 0-158.3 formula). |
| `pass_avg_ttt` | Float64 | Average time to throw in seconds, per Next Gen Stats. |
| `pass_ay_pa` | Float64 | Average intended air yards per pass attempt. |
| `pass_deep_att_pct` | Float64 | Share of pass attempts NGS classifies as deep throws. |
| `pass_yac_pct` | Float64 | Share of passing yards gained after the catch. |
| `pass_qbp` | Int64 | Dropbacks on which the passer was pressured. |
| `pass_qbp_pct` | Float64 | Pressure rate: share of dropbacks with pressure (pass_qbp / pass_db). |
| `pass_sack` | Int64 | Times sacked (a count, not a per-play flag). |
| `pass_sack_pg` | Float64 | Sacks taken per game (pass_sack / gp). |
| `pass_cmp_pg` | Float64 | Completions per game (pass_cmp / gp). |
| `pass_att_pg` | Float64 | Pass attempts per game (pass_att / gp). |
| `pass_yd_pg` | Float64 | Passing yards per game (pass_yd / gp). |
| `pass_td_pg` | Float64 | Passing touchdowns per game (pass_td / gp). |
| `pass_int_pg` | Float64 | Interceptions thrown per game (pass_int / gp). |
| `pass_db_pg` | Float64 | Dropbacks per game (pass_db / gp). |
| `rush_att` | Int64 | Rush attempts, season total. |
| `rush_yd` | Int64 | Rushing yards (rush_exp_yd + rush_ryoe). |
| `rush_td` | Int64 | Rushing touchdowns. |
| `rush_two_pt_conv` | Int64 | Two-point conversions rushed. |
| `rush_exp_yd` | Int64 | Expected rushing yards, per Next Gen Stats. |
| `rush_ryoe` | Int64 | Rushing yards over expected (rush_yd - rush_exp_yd). |
| `rush_att_pg` | Float64 | Rush attempts per game (rush_att / gp). |
| `rush_yd_pg` | Float64 | Rushing yards per game (rush_yd / gp). |
| `rush_yd_pa` | Float64 | Rushing yards per attempt (rush_yd / rush_att). |
| `rush_yaco` | Float64 | Rushing yards after contact. |
| `rush_ybco` | Float64 | Rushing yards before contact. |
| `rush_yaco_pa` | Float64 | Yards after contact per rush attempt (rush_yaco / rush_att). |
| `rush_ybco_pa` | Float64 | Yards before contact per rush attempt (rush_ybco / rush_att). |
| `rush_stuffed` | Int64 | Rush attempts stuffed (stopped at or behind the line of scrimmage). |
| `rush_td_pg` | Float64 | Rushing touchdowns per game (rush_td / gp). |
| `scr_rush_att` | Int64 | Scramble rush attempts (quarterback runs off a dropback). |
| `scr_rush_yd` | Int64 | Scramble rushing yards. |
| `scr_rush_td` | Int64 | Scramble rushing touchdowns. |
| `scr_rush_pct` | Float64 | Scramble rate: scrambles per dropback (scr_rush_att / pass_db). |
| `design_rush_att` | Int64 | Designed rush attempts (called runs, excluding scrambles). |
| `design_rush_yd` | Int64 | Designed-rush yards. |
| `design_rush_td` | Int64 | Designed-rush touchdowns. |
| `rush_rz_att` | Int64 | Rush attempts inside the red zone. |
| `rush_gl_att` | Int64 | Rush attempts at the goal line. |
| `rush10_plus_yd` | Int64 | Rush attempts that gained 10 or more yards. |
| `rec_rt` | Int64 | Routes run, season total. |
| `rec_tgt` | Int64 | Targets, season total. |
| `rec_rec` | Int64 | Receptions, season total. |
| `rec_yd` | Int64 | Receiving yards. |
| `rec_td` | Int64 | Receiving touchdowns. |
| `rec_two_pt_conv` | Int64 | Two-point conversions received. |
| `rec_rt_pg` | Float64 | Routes run per game (rec_rt / gp). |
| `rec_tgt_pg` | Float64 | Targets per game (rec_tgt / gp). |
| `rec_rec_pg` | Float64 | Receptions per game (rec_rec / gp). |
| `rec_yd_pg` | Float64 | Receiving yards per game (rec_yd / gp). |
| `rec_td_pg` | Float64 | Receiving touchdowns per game (rec_td / gp). |
| `rec_catch_pct` | Float64 | Catch rate as a fraction (rec_rec / rec_tgt). |
| `rec_ay_share` | Float64 | Share of the team's intended air yards thrown to the player. |
| `rec_tgt_rate` | Float64 | Target rate: targets per route run (rec_tgt / rec_rt). |
| `rec_tgt_share` | Float64 | Share of the team's targets thrown to the player. |
| `rec_rt_part_pct` | Float64 | Route participation: share of the team's dropbacks on which the player ran a route. |
| `rec_tgt_quick` | Int64 |  |
| `rec_tgt_play_act` | Int64 | Targets on play-action dropbacks. |
| `rec_ez_tgt` | Int64 | End-zone targets. |
| `rec_ez_rec` | Int64 | End-zone receptions. |
| `rec_rz_tgt` | Int64 | Targets inside the red zone. |
| `rec_ay_tgt` | Int64 | Total intended air yards on targets (rec_ay_rec + rec_ay_unrealized). |
| `rec_ay_rec` | Int64 | Air yards on completed targets (realized air yards). |
| `rec_ay_unrealized` | Int64 | Air yards on incomplete targets (unrealized air yards). |
| `rec_ay_pt` | Float64 | Average intended air yards per target. |
| `rec_tgt_ay10_plus` | Int64 | Targets of 10 or more intended air yards. |
| `rec_yd_p_rt` | Float64 | Receiving yards per route run (rec_yd / rec_rt). |
| `rec_yd_pt` | Float64 | Receiving yards per target (rec_yd / rec_tgt). |
| `rec_yd_pr` | Float64 | Receiving yards per reception (rec_yd / rec_rec). |
| `rec_yac` | Int64 | Yards after the catch. |
| `rec_exp_yac` | Int64 | Expected yards after the catch, per Next Gen Stats. |
| `rec_yacoe` | Int64 | Yards after the catch over expected (rec_yac - rec_exp_yac). |
| `kick_xp_att` | Int64 | Extra-point attempts. |
| `kick_xp_made` | Int64 | Extra points made. |
| `kick_fg_att` | Int64 | Field-goal attempts. |
| `kick_fg_made` | Int64 | Field goals made. |
| `kick_fg_miss` | Int64 | Field goals missed. |
| `kick_fg_made_less40` | Int64 | Field goals made from under 40 yards. |
| `kick_fg_made40_to49` | Int64 | Field goals made from 40-49 yards. |
| `kick_fg_made50_to59` | Int64 | Field goals made from 50-59 yards. |
| `kick_fg_made60_plus` | Int64 | Field goals made from 60 yards or more. |
| `misc_kickoff_ret_td` | Int64 | Kickoff-return touchdowns. |
| `misc_punt_ret_td` | Int64 | Punt-return touchdowns. |
| `misc_fum_rec_td` | Int64 | Fumble-recovery touchdowns. |
| `misc_fum_lost` | Int64 | Fumbles lost, season total. |
| `misc_fum` | Int64 | Fumbles, season total. |
| `misc_int_ret_td` | Int64 | Interception-return touchdowns. |
| `misc_fum_ret_td` | Int64 | Fumble-return touchdowns. |
| `misc_blk_punt_fg_ret_td` | Int64 | Touchdowns on blocked-punt or blocked-field-goal returns. |
| `misc_two_pt_ret` | Int64 | Two-point conversion returns (defensive two-point scores). |
| `misc_one_pt_safety` | Int64 | One-point safeties. |
| `o_touch` | Int64 | Touches: rush attempts plus receptions (rush_att + rec_rec). |
| `o_opp` | Int64 | Opportunities: rush attempts plus targets (rush_att + rec_tgt). |
| `o_opp_pg` | Float64 | Opportunities per game (o_opp / gp). |
| `o_miss_tkl_forced` | Int64 | Missed tackles forced on touches. |
| `o_miss_tkl_forced_pct` | Float64 | Missed tackles forced per touch (o_miss_tkl_forced / o_touch). |
| `o_tm_db` | Int64 | Team dropbacks. |
| `o_tm_pass_pct` | Float64 | Team pass rate: team dropbacks over team offensive snaps (o_tm_db / o_tm_snap). |
| `o_tm_ppg` | Float64 | Team points per game. |
| `o_tm_yd_pg` | Float64 | Team yards per game. |
| `rz_opp` | Int64 | Red-zone opportunities: red-zone rushes plus red-zone targets (rush_rz_att + rec_rz_tgt). |
| `fp_std` | Float64 | Fantasy points, standard (non-PPR) scoring. |
| `fp_half_ppr` | Float64 | Fantasy points, half-PPR scoring. |
| `fp_ppr` | Float64 | Fantasy points, full-PPR scoring. |
| `fp_pass` | Float64 | Fantasy points from passing. |
| `fp_rush` | Float64 | Fantasy points from rushing. |
| `fp_rec_std` | Float64 | Fantasy points from receiving, standard scoring. |
| `fp_rec_half_ppr` | Float64 | Fantasy points from receiving, half-PPR scoring. |
| `fp_rec_ppr` | Float64 | Fantasy points from receiving, full-PPR scoring. |
| `fp_kick` | Int64 | Fantasy points from kicking. |
| `fp_misc` | Int64 | Fantasy points from return, fumble-recovery and other miscellaneous scoring. |
| `fp_pg_std` | Float64 | Fantasy points per game, standard scoring (fp_std / gp). |
| `fp_pg_half_ppr` | Float64 | Fantasy points per game, half-PPR scoring (fp_half_ppr / gp). |
| `fp_pgppr` | Float64 | Fantasy points per game, full-PPR scoring (fp_ppr / gp). |
| `fp_pos_rk_std` | Int64 | Positional fantasy rank, standard scoring (1 = top scorer at the position). |
| `fp_pos_rk_half_ppr` | Int64 | Positional fantasy rank, half-PPR scoring (1 = top scorer at the position). |
| `fp_pos_rk_ppr` | Int64 | Positional fantasy rank, full-PPR scoring (1 = top scorer at the position). |
| `fp_pos_rk_lbl_std` | String | Positional rank label, standard scoring (e.g. 'QB1', 'RB12'). |
| `fp_pos_rk_lbl_half_ppr` | String | Positional rank label, half-PPR scoring (e.g. 'QB1', 'RB12'). |
| `fp_pos_rk_lbl_ppr` | String | Positional rank label, full-PPR scoring (e.g. 'QB1', 'RB12'). |
| `fp_ps_std` | Float64 | Fantasy points per offensive snap, standard scoring (fp_std / o_snap). |
| `fp_ps_half_ppr` | Float64 | Fantasy points per offensive snap, half-PPR scoring (fp_half_ppr / o_snap). |
| `fp_psppr` | Float64 | Fantasy points per offensive snap, full-PPR scoring (fp_ppr / o_snap). |
| `fp_p_rt_std` | Float64 | Fantasy points per route run, standard scoring. |
| `fp_p_rt_half_ppr` | Float64 | Fantasy points per route run, half-PPR scoring. |
| `fp_p_rt_ppr` | Float64 | Fantasy points per route run, full-PPR scoring. |
| `fp_pt_std` | Float64 | Fantasy points per touch, standard scoring. |
| `fp_pt_half_ppr` | Float64 | Fantasy points per touch, half-PPR scoring. |
| `fp_ptppr` | Float64 | Fantasy points per touch, full-PPR scoring. |
| `fp_po_std` | Float64 | Fantasy points per opportunity, standard scoring (fp_std / o_opp). |
| `fp_po_half_ppr` | Float64 | Fantasy points per opportunity, half-PPR scoring (fp_half_ppr / o_opp). |
| `fp_poppr` | Float64 | Fantasy points per opportunity, full-PPR scoring (fp_ppr / o_opp). |
| `top5_qb_wk_std` | Int64 | Weeks the player finished as a top-5 QB by fantasy points, standard scoring, season count. |
| `top12_qb_wk_std` | Int64 | Weeks the player finished as a top-12 QB by fantasy points, standard scoring, season count. |
| `top12_rb_wk_std` | Int64 | Weeks the player finished as a top-12 RB by fantasy points, standard scoring, season count. |
| `top24_rb_wk_std` | Int64 | Weeks the player finished as a top-24 RB by fantasy points, standard scoring, season count. |
| `top12_wr_wk_std` | Int64 | Weeks the player finished as a top-12 WR by fantasy points, standard scoring, season count. |
| `top24_wr_wk_std` | Int64 | Weeks the player finished as a top-24 WR by fantasy points, standard scoring, season count. |
| `top36_wr_wk_std` | Int64 | Weeks the player finished as a top-36 WR by fantasy points, standard scoring, season count. |
| `top5_te_wk_std` | Int64 | Weeks the player finished as a top-5 TE by fantasy points, standard scoring, season count. |
| `top12_te_wk_std` | Int64 | Weeks the player finished as a top-12 TE by fantasy points, standard scoring, season count. |
| `top5_k_wk_std` | Int64 | Weeks the player finished as a top-5 K by fantasy points, standard scoring, season count. |
| `top12_k_wk_std` | Int64 | Weeks the player finished as a top-12 K by fantasy points, standard scoring, season count. |
| `top5_qb_wk_half_ppr` | Int64 | Weeks the player finished as a top-5 QB by fantasy points, half-PPR scoring, season count. |
| `top12_qb_wk_half_ppr` | Int64 | Weeks the player finished as a top-12 QB by fantasy points, half-PPR scoring, season count. |
| `top12_rb_wk_half_ppr` | Int64 | Weeks the player finished as a top-12 RB by fantasy points, half-PPR scoring, season count. |
| `top24_rb_wk_half_ppr` | Int64 | Weeks the player finished as a top-24 RB by fantasy points, half-PPR scoring, season count. |
| `top12_wr_wk_half_ppr` | Int64 | Weeks the player finished as a top-12 WR by fantasy points, half-PPR scoring, season count. |
| `top24_wr_wk_half_ppr` | Int64 | Weeks the player finished as a top-24 WR by fantasy points, half-PPR scoring, season count. |
| `top36_wr_wk_half_ppr` | Int64 | Weeks the player finished as a top-36 WR by fantasy points, half-PPR scoring, season count. |
| `top5_te_wk_half_ppr` | Int64 | Weeks the player finished as a top-5 TE by fantasy points, half-PPR scoring, season count. |
| `top12_te_wk_half_ppr` | Int64 | Weeks the player finished as a top-12 TE by fantasy points, half-PPR scoring, season count. |
| `top5_k_wk_half_ppr` | Int64 | Weeks the player finished as a top-5 K by fantasy points, half-PPR scoring, season count. |
| `top12_k_wk_half_ppr` | Int64 | Weeks the player finished as a top-12 K by fantasy points, half-PPR scoring, season count. |
| `top5_qb_wk_ppr` | Int64 | Weeks the player finished as a top-5 QB by fantasy points, full-PPR scoring, season count. |
| `top12_qb_wk_ppr` | Int64 | Weeks the player finished as a top-12 QB by fantasy points, full-PPR scoring, season count. |
| `top12_rb_wk_ppr` | Int64 | Weeks the player finished as a top-12 RB by fantasy points, full-PPR scoring, season count. |
| `top24_rb_wk_ppr` | Int64 | Weeks the player finished as a top-24 RB by fantasy points, full-PPR scoring, season count. |
| `top12_wr_wk_ppr` | Int64 | Weeks the player finished as a top-12 WR by fantasy points, full-PPR scoring, season count. |
| `top24_wr_wk_ppr` | Int64 | Weeks the player finished as a top-24 WR by fantasy points, full-PPR scoring, season count. |
| `top36_wr_wk_ppr` | Int64 | Weeks the player finished as a top-36 WR by fantasy points, full-PPR scoring, season count. |
| `top5_te_wk_ppr` | Int64 | Weeks the player finished as a top-5 TE by fantasy points, full-PPR scoring, season count. |
| `top12_te_wk_ppr` | Int64 | Weeks the player finished as a top-12 TE by fantasy points, full-PPR scoring, season count. |
| `top5_k_wk_ppr` | Int64 | Weeks the player finished as a top-5 K by fantasy points, full-PPR scoring, season count. |
| `top12_k_wk_ppr` | Int64 | Weeks the player finished as a top-12 K by fantasy points, full-PPR scoring, season count. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_pro_fantasy_season-raises}

| Exception | Description |
|---|---|
| `NFLProAuthError` | No user-bound NFL Pro token could be resolved (pass `token=` or `email=`/`password=`, or set `NFLPRO_TOKEN`, or `NFLPRO_EMAIL`/`NFLPRO_PW`), or the resolved token carries no active NFL_PLUS_* plan, or pro.nfl.com answered 401 / 403 (an `AssetFetchError`). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. Pro.nfl.com answered 400 / 422, or ignored `offset`. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other non-2xx or a connection failure after retries, or a 2xx whose body is not a JSON object or array -- including the HTTP 200 with an empty body that is how pro.nfl.com rejects unsupported query params. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |

### Example {#nfl_pro_fantasy_season-example}

```python
nfl_pro_fantasy_season(season=2024, season_type='REG')
```

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
| `nfl_id` | String | NFL player id (`nflId`) as a string; the key the wrappers accept as `nfl_id`. |
| `display_name` | String | Player's full display name (e.g. 'Aaron Rodgers'). |
| `short_name` | String | Player's abbreviated name, first initial and surname (e.g. 'A.Rodgers'). |
| `headshot` | String | NFL headshot image URL template; the `{formatInstructions}` token must be replaced with a Cloudinary transform (e.g. `t_headshot_desktop`) before use. |
| `team_id` | String | NFL team id as a zero-padded string (e.g. '0200', '3430'); casting it to a number drops the leading zero. |
| `jersey_number` | Int64 | Jersey number the player wore in the season. |
| `position` | String | Roster position abbreviation (e.g. QB, WR, CB). |
| `position_group` | String | Position group the roster position rolls up to (e.g. QB, RB, WR, TE, DB). |
| `gp` | Int64 | Games played in the season (and season type) -- the denominator of every `*_pg` per-game column. |
| `gs` | Int64 | Games started in the season. |
| `week_slug` | String | Week slug of the game (e.g. 'WEEK_1', 'WEEK_18'); the week scope of the row. |
| `game_id` | Int64 | NFL game id as an integer (e.g. 2024090900, the date followed by a two-digit sequence). |
| `fapi_game_id` | String | NFL Football API (FAPI) UUID of the game, the id api.nfl.com uses for the same game. |
| `opponent_team_id` | String | Opponent's NFL team id as a zero-padded string. |
| `is_home` | Boolean | Whether the player's team was the home team in the game. |
| `final_score` | String | Final score as 'own-opponent' (e.g. '19-32' for a 32-19 loss). |
| `game_result` | String | Result from the player's team's side: 'W', 'L' or 'T'. |
| `o_snap` | Int64 | Offensive snaps played. |
| `o_snap3rd` | Int64 |  |
| `o_tm_snap` | Int64 | Offensive snaps the player's team played; the denominator of pt_pct. |
| `o_snap_pg` | Int64 | Offensive snaps per game (o_snap / gp). |
| `pt_pct` | Float64 | Playing-time share: offensive snaps played over the team's offensive snaps (o_snap / o_tm_snap). |
| `pt_pct3rd` | Float64 |  |
| `pass_cmp` | Int64 | Pass completions. |
| `pass_att` | Int64 | Pass attempts in the game. |
| `pass_yd` | Int64 | Passing yards in the game. |
| `pass_td` | Int64 | Passing touchdowns. |
| `pass_int` | Int64 | Interceptions thrown. |
| `pass_two_pt_conv` | Int64 | Two-point conversions passed. |
| `pass_db` | Int64 | Dropbacks in the game. |
| `pass_cmp_pct` | Float64 | Completion percentage as a fraction (pass_cmp / pass_att). |
| `pass_exp_cmp_pct` | Float64 | Expected completion percentage as a fraction, per Next Gen Stats; pass_cmp_pct = pass_exp_cmp_pct + pass_cpoe. |
| `pass_cpoe` | Float64 | Completion percentage over expected as a fraction (pass_cmp_pct - pass_exp_cmp_pct). |
| `pass_rating` | Float64 | NFL passer rating (the standard 0-158.3 formula). |
| `pass_avg_ttt` | Float64 | Average time to throw in seconds, per Next Gen Stats. |
| `pass_ay_pa` | Float64 | Average intended air yards per pass attempt. |
| `pass_deep_att_pct` | Float64 | Share of pass attempts NGS classifies as deep throws. |
| `pass_yac_pct` | Float64 | Share of passing yards gained after the catch. |
| `pass_qbp` | Int64 | Dropbacks on which the passer was pressured. |
| `pass_qbp_pct` | Float64 | Pressure rate: share of dropbacks with pressure (pass_qbp / pass_db). |
| `pass_sack` | Int64 | Times sacked (a count, not a per-play flag). |
| `pass_sack_pg` | Int64 | Sacks taken per game (pass_sack / gp). |
| `pass_cmp_pg` | Int64 | Completions per game (pass_cmp / gp). |
| `pass_att_pg` | Int64 | Pass attempts per game (pass_att / gp). |
| `pass_yd_pg` | Int64 | Passing yards per game (pass_yd / gp). |
| `pass_td_pg` | Int64 | Passing touchdowns per game (pass_td / gp). |
| `pass_int_pg` | Int64 | Interceptions thrown per game (pass_int / gp). |
| `pass_db_pg` | Int64 | Dropbacks per game (pass_db / gp). |
| `rush_att` | Int64 | Rush attempts in the game. |
| `rush_yd` | Int64 | Rushing yards (rush_exp_yd + rush_ryoe). |
| `rush_td` | Int64 | Rushing touchdowns. |
| `rush_two_pt_conv` | Int64 | Two-point conversions rushed. |
| `rush_exp_yd` | Int64 | Expected rushing yards, per Next Gen Stats. |
| `rush_ryoe` | Int64 | Rushing yards over expected (rush_yd - rush_exp_yd). |
| `rush_att_pg` | Int64 | Rush attempts per game (rush_att / gp). |
| `rush_yd_pg` | Int64 | Rushing yards per game (rush_yd / gp). |
| `rush_yd_pa` | Float64 | Rushing yards per attempt (rush_yd / rush_att). |
| `rush_yaco` | Float64 | Rushing yards after contact. |
| `rush_ybco` | Float64 | Rushing yards before contact. |
| `rush_yaco_pa` | Float64 | Yards after contact per rush attempt (rush_yaco / rush_att). |
| `rush_ybco_pa` | Float64 | Yards before contact per rush attempt (rush_ybco / rush_att). |
| `rush_stuffed` | Int64 | Rush attempts stuffed (stopped at or behind the line of scrimmage). |
| `rush_td_pg` | Int64 | Rushing touchdowns per game (rush_td / gp). |
| `scr_rush_att` | Int64 | Scramble rush attempts (quarterback runs off a dropback). |
| `scr_rush_yd` | Int64 | Scramble rushing yards. |
| `scr_rush_td` | Int64 | Scramble rushing touchdowns. |
| `scr_rush_pct` | Float64 | Scramble rate: scrambles per dropback (scr_rush_att / pass_db). |
| `design_rush_att` | Int64 | Designed rush attempts (called runs, excluding scrambles). |
| `design_rush_yd` | Int64 | Designed-rush yards. |
| `design_rush_td` | Int64 | Designed-rush touchdowns. |
| `rush_rz_att` | Int64 | Rush attempts inside the red zone. |
| `rush_gl_att` | Int64 | Rush attempts at the goal line. |
| `rush10_plus_yd` | Int64 | Rush attempts that gained 10 or more yards. |
| `rec_rt` | Int64 | Routes run in the game. |
| `rec_tgt` | Int64 | Targets in the game. |
| `rec_rec` | Int64 | Receptions in the game. |
| `rec_yd` | Int64 | Receiving yards. |
| `rec_td` | Int64 | Receiving touchdowns. |
| `rec_two_pt_conv` | Int64 | Two-point conversions received. |
| `rec_rt_pg` | Int64 | Routes run per game (rec_rt / gp). |
| `rec_tgt_pg` | Int64 | Targets per game (rec_tgt / gp). |
| `rec_rec_pg` | Int64 | Receptions per game (rec_rec / gp). |
| `rec_yd_pg` | Int64 | Receiving yards per game (rec_yd / gp). |
| `rec_td_pg` | Int64 | Receiving touchdowns per game (rec_td / gp). |
| `rec_catch_pct` | Int64 | Catch rate as a fraction (rec_rec / rec_tgt). |
| `rec_ay_share` | Int64 | Share of the team's intended air yards thrown to the player. |
| `rec_tgt_rate` | Int64 | Target rate: targets per route run (rec_tgt / rec_rt). |
| `rec_tgt_share` | Int64 | Share of the team's targets thrown to the player. |
| `rec_rt_part_pct` | Float64 | Route participation: share of the team's dropbacks on which the player ran a route. |
| `rec_tgt_quick` | Int64 |  |
| `rec_tgt_play_act` | Int64 | Targets on play-action dropbacks. |
| `rec_ez_tgt` | Int64 | End-zone targets. |
| `rec_ez_rec` | Int64 | End-zone receptions. |
| `rec_rz_tgt` | Int64 | Targets inside the red zone. |
| `rec_ay_tgt` | Int64 | Total intended air yards on targets (rec_ay_rec + rec_ay_unrealized). |
| `rec_ay_rec` | Int64 | Air yards on completed targets (realized air yards). |
| `rec_ay_unrealized` | Int64 | Air yards on incomplete targets (unrealized air yards). |
| `rec_ay_pt` | Int64 | Average intended air yards per target. |
| `rec_tgt_ay10_plus` | Int64 | Targets of 10 or more intended air yards. |
| `rec_yd_p_rt` | Int64 | Receiving yards per route run (rec_yd / rec_rt). |
| `rec_yd_pt` | Int64 | Receiving yards per target (rec_yd / rec_tgt). |
| `rec_yd_pr` | Int64 | Receiving yards per reception (rec_yd / rec_rec). |
| `rec_yac` | Int64 | Yards after the catch. |
| `rec_exp_yac` | Int64 | Expected yards after the catch, per Next Gen Stats. |
| `rec_yacoe` | Int64 | Yards after the catch over expected (rec_yac - rec_exp_yac). |
| `kick_xp_att` | Int64 | Extra-point attempts. |
| `kick_xp_made` | Int64 | Extra points made. |
| `kick_fg_att` | Int64 | Field-goal attempts. |
| `kick_fg_made` | Int64 | Field goals made. |
| `kick_fg_miss` | Int64 | Field goals missed. |
| `kick_fg_made_less40` | Int64 | Field goals made from under 40 yards. |
| `kick_fg_made40_to49` | Int64 | Field goals made from 40-49 yards. |
| `kick_fg_made50_to59` | Int64 | Field goals made from 50-59 yards. |
| `kick_fg_made60_plus` | Int64 | Field goals made from 60 yards or more. |
| `misc_kickoff_ret_td` | Int64 | Kickoff-return touchdowns. |
| `misc_punt_ret_td` | Int64 | Punt-return touchdowns. |
| `misc_fum_rec_td` | Int64 | Fumble-recovery touchdowns. |
| `misc_fum_lost` | Int64 | Fumbles lost in the game. |
| `misc_fum` | Int64 | Fumbles in the game. |
| `misc_int_ret_td` | Int64 | Interception-return touchdowns. |
| `misc_fum_ret_td` | Int64 | Fumble-return touchdowns. |
| `misc_blk_punt_fg_ret_td` | Int64 | Touchdowns on blocked-punt or blocked-field-goal returns. |
| `misc_two_pt_ret` | Int64 | Two-point conversion returns (defensive two-point scores). |
| `misc_one_pt_safety` | Int64 | One-point safeties. |
| `o_touch` | Int64 | Touches: rush attempts plus receptions (rush_att + rec_rec). |
| `o_opp` | Int64 | Opportunities: rush attempts plus targets (rush_att + rec_tgt). |
| `o_opp_pg` | Int64 | Opportunities per game (o_opp / gp). |
| `o_miss_tkl_forced` | Int64 | Missed tackles forced on touches. |
| `o_miss_tkl_forced_pct` | Int64 | Missed tackles forced per touch (o_miss_tkl_forced / o_touch). |
| `o_tm_db` | Int64 | Team dropbacks. |
| `o_tm_pass_pct` | Float64 | Team pass rate: team dropbacks over team offensive snaps (o_tm_db / o_tm_snap). |
| `o_tm_ppg` | Int64 | Team points per game. |
| `o_tm_yd_pg` | Int64 | Team yards per game. |
| `rz_opp` | Int64 | Red-zone opportunities: red-zone rushes plus red-zone targets (rush_rz_att + rec_rz_tgt). |
| `fp_std` | Float64 | Fantasy points, standard (non-PPR) scoring. |
| `fp_half_ppr` | Float64 | Fantasy points, half-PPR scoring. |
| `fp_ppr` | Float64 | Fantasy points, full-PPR scoring. |
| `fp_pass` | Float64 | Fantasy points from passing. |
| `fp_rush` | Float64 | Fantasy points from rushing. |
| `fp_rec_std` | Int64 | Fantasy points from receiving, standard scoring. |
| `fp_rec_half_ppr` | Int64 | Fantasy points from receiving, half-PPR scoring. |
| `fp_rec_ppr` | Int64 | Fantasy points from receiving, full-PPR scoring. |
| `fp_kick` | Int64 | Fantasy points from kicking. |
| `fp_misc` | Int64 | Fantasy points from return, fumble-recovery and other miscellaneous scoring. |
| `fp_pg_std` | Float64 | Fantasy points per game, standard scoring (fp_std / gp). |
| `fp_pg_half_ppr` | Float64 | Fantasy points per game, half-PPR scoring (fp_half_ppr / gp). |
| `fp_pgppr` | Float64 | Fantasy points per game, full-PPR scoring (fp_ppr / gp). |
| `fp_pos_rk_std` | Int64 | Positional fantasy rank, standard scoring (1 = top scorer at the position). |
| `fp_pos_rk_half_ppr` | Int64 | Positional fantasy rank, half-PPR scoring (1 = top scorer at the position). |
| `fp_pos_rk_ppr` | Int64 | Positional fantasy rank, full-PPR scoring (1 = top scorer at the position). |
| `fp_pos_rk_lbl_std` | String | Positional rank label, standard scoring (e.g. 'QB1', 'RB12'). |
| `fp_pos_rk_lbl_half_ppr` | String | Positional rank label, half-PPR scoring (e.g. 'QB1', 'RB12'). |
| `fp_pos_rk_lbl_ppr` | String | Positional rank label, full-PPR scoring (e.g. 'QB1', 'RB12'). |
| `fp_ps_std` | Float64 | Fantasy points per offensive snap, standard scoring (fp_std / o_snap). |
| `fp_ps_half_ppr` | Float64 | Fantasy points per offensive snap, half-PPR scoring (fp_half_ppr / o_snap). |
| `fp_psppr` | Float64 | Fantasy points per offensive snap, full-PPR scoring (fp_ppr / o_snap). |
| `fp_p_rt_std` | Float64 | Fantasy points per route run, standard scoring. |
| `fp_p_rt_half_ppr` | Float64 | Fantasy points per route run, half-PPR scoring. |
| `fp_p_rt_ppr` | Float64 | Fantasy points per route run, full-PPR scoring. |
| `fp_pt_std` | Int64 | Fantasy points per touch, standard scoring. |
| `fp_pt_half_ppr` | Int64 | Fantasy points per touch, half-PPR scoring. |
| `fp_ptppr` | Int64 | Fantasy points per touch, full-PPR scoring. |
| `fp_po_std` | Float64 | Fantasy points per opportunity, standard scoring (fp_std / o_opp). |
| `fp_po_half_ppr` | Float64 | Fantasy points per opportunity, half-PPR scoring (fp_half_ppr / o_opp). |
| `fp_poppr` | Float64 | Fantasy points per opportunity, full-PPR scoring (fp_ppr / o_opp). |
| `top5_qb_wk_std` | Int64 | Whether the player finished the week as a top-5 QB by fantasy points, standard scoring (1/0). |
| `top12_qb_wk_std` | Int64 | Whether the player finished the week as a top-12 QB by fantasy points, standard scoring (1/0). |
| `top12_rb_wk_std` | Int64 | Whether the player finished the week as a top-12 RB by fantasy points, standard scoring (1/0). |
| `top24_rb_wk_std` | Int64 | Whether the player finished the week as a top-24 RB by fantasy points, standard scoring (1/0). |
| `top12_wr_wk_std` | Int64 | Whether the player finished the week as a top-12 WR by fantasy points, standard scoring (1/0). |
| `top24_wr_wk_std` | Int64 | Whether the player finished the week as a top-24 WR by fantasy points, standard scoring (1/0). |
| `top36_wr_wk_std` | Int64 | Whether the player finished the week as a top-36 WR by fantasy points, standard scoring (1/0). |
| `top5_te_wk_std` | Int64 | Whether the player finished the week as a top-5 TE by fantasy points, standard scoring (1/0). |
| `top12_te_wk_std` | Int64 | Whether the player finished the week as a top-12 TE by fantasy points, standard scoring (1/0). |
| `top5_k_wk_std` | Int64 | Whether the player finished the week as a top-5 K by fantasy points, standard scoring (1/0). |
| `top12_k_wk_std` | Int64 | Whether the player finished the week as a top-12 K by fantasy points, standard scoring (1/0). |
| `top5_qb_wk_half_ppr` | Int64 | Whether the player finished the week as a top-5 QB by fantasy points, half-PPR scoring (1/0). |
| `top12_qb_wk_half_ppr` | Int64 | Whether the player finished the week as a top-12 QB by fantasy points, half-PPR scoring (1/0). |
| `top12_rb_wk_half_ppr` | Int64 | Whether the player finished the week as a top-12 RB by fantasy points, half-PPR scoring (1/0). |
| `top24_rb_wk_half_ppr` | Int64 | Whether the player finished the week as a top-24 RB by fantasy points, half-PPR scoring (1/0). |
| `top12_wr_wk_half_ppr` | Int64 | Whether the player finished the week as a top-12 WR by fantasy points, half-PPR scoring (1/0). |
| `top24_wr_wk_half_ppr` | Int64 | Whether the player finished the week as a top-24 WR by fantasy points, half-PPR scoring (1/0). |
| `top36_wr_wk_half_ppr` | Int64 | Whether the player finished the week as a top-36 WR by fantasy points, half-PPR scoring (1/0). |
| `top5_te_wk_half_ppr` | Int64 | Whether the player finished the week as a top-5 TE by fantasy points, half-PPR scoring (1/0). |
| `top12_te_wk_half_ppr` | Int64 | Whether the player finished the week as a top-12 TE by fantasy points, half-PPR scoring (1/0). |
| `top5_k_wk_half_ppr` | Int64 | Whether the player finished the week as a top-5 K by fantasy points, half-PPR scoring (1/0). |
| `top12_k_wk_half_ppr` | Int64 | Whether the player finished the week as a top-12 K by fantasy points, half-PPR scoring (1/0). |
| `top5_qb_wk_ppr` | Int64 | Whether the player finished the week as a top-5 QB by fantasy points, full-PPR scoring (1/0). |
| `top12_qb_wk_ppr` | Int64 | Whether the player finished the week as a top-12 QB by fantasy points, full-PPR scoring (1/0). |
| `top12_rb_wk_ppr` | Int64 | Whether the player finished the week as a top-12 RB by fantasy points, full-PPR scoring (1/0). |
| `top24_rb_wk_ppr` | Int64 | Whether the player finished the week as a top-24 RB by fantasy points, full-PPR scoring (1/0). |
| `top12_wr_wk_ppr` | Int64 | Whether the player finished the week as a top-12 WR by fantasy points, full-PPR scoring (1/0). |
| `top24_wr_wk_ppr` | Int64 | Whether the player finished the week as a top-24 WR by fantasy points, full-PPR scoring (1/0). |
| `top36_wr_wk_ppr` | Int64 | Whether the player finished the week as a top-36 WR by fantasy points, full-PPR scoring (1/0). |
| `top5_te_wk_ppr` | Int64 | Whether the player finished the week as a top-5 TE by fantasy points, full-PPR scoring (1/0). |
| `top12_te_wk_ppr` | Int64 | Whether the player finished the week as a top-12 TE by fantasy points, full-PPR scoring (1/0). |
| `top5_k_wk_ppr` | Int64 | Whether the player finished the week as a top-5 K by fantasy points, full-PPR scoring (1/0). |
| `top12_k_wk_ppr` | Int64 | Whether the player finished the week as a top-12 K by fantasy points, full-PPR scoring (1/0). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_pro_fantasy_game-raises}

| Exception | Description |
|---|---|
| `NFLProAuthError` | No user-bound NFL Pro token could be resolved (pass `token=` or `email=`/`password=`, or set `NFLPRO_TOKEN`, or `NFLPRO_EMAIL`/`NFLPRO_PW`), or the resolved token carries no active NFL_PLUS_* plan, or pro.nfl.com answered 401 / 403 (an `AssetFetchError`). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. Pro.nfl.com answered 400 / 422, or ignored `offset`. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other non-2xx or a connection failure after retries, or a 2xx whose body is not a JSON object or array -- including the HTTP 200 with an empty body that is how pro.nfl.com rejects unsupported query params. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |

### Example {#nfl_pro_fantasy_game-example}

```python
nfl_pro_fantasy_game(season=2024, season_type='REG', position_group='QB')
```
