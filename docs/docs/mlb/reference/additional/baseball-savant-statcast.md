---
title: "MLB — additional Python functions — Baseball Savant (Statcast)"
sidebar_label: "Baseball Savant (Statcast)"
sidebar_position: 3
description: "MLB — additional Python functions — Baseball Savant (Statcast) — function reference in sdv-py, the SportsDataverse Python package."
---
# MLB — additional Python functions — Baseball Savant (Statcast)

### mlb_statcast_player {#mlb_statcast_player}

`mlb_statcast_player(player_id: 'int', stats: 'Optional[str]' = None, *, section: 'str' = 'statcast', raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "'Union[pl.DataFrame, pd.DataFrame, str]'"`

GET /savant-player/{player_id} and parse one embedded table into a tidy frame.

Returns a tidy frame **by default** (the parsed Statcast page); pass
`raw=True` to get the underlying HTML string instead (the page embeds ~12
other tables you can mine yourself, or feed to
`sportsdataverse.mlb.parse_mlb_statcast_player` with a different
`section`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_id` | `int` |  | MLBAM player id (shared with the Stats API `personId`). |
| `stats` | `Optional[str]` | `None` | optional `stats` query value to scope the embedded payload. |
| `section` | `str` | `'statcast'` | which embedded `serverVals` table to flatten (default `"statcast"`, the seasonal aggregate; e.g. `"statcastGameLogs"`). |
| `raw` | `bool` | `False` | return the raw page HTML string instead of a parsed frame. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame of the player's Statcast metrics by default (zero rows when the page/section is absent); the raw HTML `str` when `raw=True`.

| col_name | type | description |
|---|---|---|
| `aggregate` | Int64 | Aggregate. |
| `year` | Int64 | Season year. |
| `yearhidden` | Int64 | Yearhidden. |
| `player_id` | Int64 | MLBAM player id. |
| `age` | Int64 | Player age. |
| `bat_side` | String | Batter side (R/L/S). |
| `pitch_hand` | String | Pitcher handedness (R/L). |
| `month` | String | Month. |
| `grouping_code` | String | Grouping code. |
| `grouping_cat` | String | Grouping cat. |
| `pitch_count` | Int64 | Pitch count. |
| `in_zone_percent` | Float64 | In zone rate. |
| `out_zone_percent` | Float64 | Out zone rate. |
| `edge_percent` | Float64 | Edge rate. |
| `z_swing_percent` | Float64 | Z swing rate. |
| `oz_swing_percent` | Float64 | Oz swing rate. |
| `iz_contact_percent` | Float64 | Iz contact rate. |
| `oz_contact_percent` | Float64 | Oz contact rate. |
| `whiff_percent` | Float64 | Whiff rate (swings and misses / swings). |
| `f_strike_percent` | Float64 | F strike rate. |
| `f_swing_percent` | Float64 | F swing rate. |
| `swing_percent` | Float64 | Swing rate. |
| `meatball_swing_percent` | Int64 | Meatball swing rate. |
| `meatball_percent` | Float64 | Meatball rate. |
| `z_swing_miss_percent` | Float64 | Z swing miss rate. |
| `oz_swing_miss_percent` | Float64 | Oz swing miss rate. |
| `in_zone` | Int64 | In zone. |
| `out_zone` | Int64 | Out zone. |
| `edge` | Int64 | Edge. |
| `popups` | Int64 | Popups. |
| `flyballs` | Int64 | Flyballs. |
| `linedrives` | Int64 | Linedrives. |
| `groundballs` | Int64 | Groundballs. |
| `airballs` | Int64 | Airballs. |
| `popups_percent` | Float64 | Popups rate. |
| `flyballs_percent` | Float64 | Flyballs rate. |
| `linedrives_percent` | Float64 | Linedrives rate. |
| `groundballs_percent` | Float64 | Groundballs rate. |
| `airballs_percent` | Float64 | Airballs rate. |
| `pull_percent` | Float64 | Pull rate. |
| `straightaway_percent` | Float64 | Straightaway rate. |
| `opposite_percent` | Float64 | Opposite rate. |
| `pull_percent_airballs` | Float64 | Pull percent airballs. |
| `straightaway_percent_airballs` | Float64 | Straightaway percent airballs. |
| `opposite_percent_airballs` | Int64 | Opposite percent airballs. |
| `pull_percent_groundballs` | Float64 | Pull percent groundballs. |
| `straightaway_percent_groundballs` | Float64 | Straightaway percent groundballs. |
| `opposite_percent_groundballs` | Float64 | Opposite percent groundballs. |
| `pull_percent_popups` | Float64 | Pull percent popups. |
| `straightaway_percent_popups` | Float64 | Straightaway percent popups. |
| `opposite_percent_popups` | Float64 | Opposite percent popups. |
| `pull_percent_flyballs` | Float64 | Pull percent flyballs. |
| `straightaway_percent_flyballs` | Float64 | Straightaway percent flyballs. |
| `opposite_percent_flyballs` | Float64 | Opposite percent flyballs. |
| `pull_percent_linedrives` | Int64 | Pull percent linedrives. |
| `straightaway_percent_linedrives` | Int64 | Straightaway percent linedrives. |
| `opposite_percent_linedrives` | Int64 | Opposite percent linedrives. |
| `poorlyweak_percent` | Int64 | Poorlyweak rate. |
| `poorlytopped_percent` | Float64 | Poorlytopped rate. |
| `poorlyunder_percent` | Float64 | Poorlyunder rate. |
| `flareburner_percent` | Float64 | Flareburner rate. |
| `solidcontact_percent` | Float64 | Solidcontact rate. |
| `hr_flyballs_percent` | Float64 | Hr flyballs rate. |
| `in_zone_swing` | Int64 | In zone swing. |
| `out_zone_swing` | Int64 | Out zone swing. |
| `in_zone_swing_miss` | Int64 | In zone swing miss. |
| `out_zone_swing_miss` | Int64 | Out zone swing miss. |
| `pitch_count_fastball` | Int64 | Pitch count fastball. |
| `pitch_count_offspeed` | Int64 | Pitch count offspeed. |
| `pitch_count_breaking` | Int64 | Pitch count breaking. |
| `pa` | Int64 | Plate appearances. |
| `ab` | Int64 | At-bats. |
| `hit` | Int64 | Hit. |
| `single` | Int64 | Singles. |
| `double` | Int64 | Doubles. |
| `triple` | Int64 | Triples. |
| `home_run` | Int64 | Home run. |
| `walk` | Int64 | Walk. |
| `strikeout` | Int64 | Strikeout. |
| `hbp` | Int64 | Hbp. |
| `k_percent` | Float64 | Strikeout rate. |
| `bb_percent` | Float64 | Walk rate. |
| `sz_judge` | Float64 | Sz judge. |
| `batted_ball` | Int64 | Batted ball. |
| `barrel` | Int64 | Barrel. |
| `barrel_batted_rate` | Float64 | Barrels per batted ball. |
| `barrels_per_pa` | Float64 | Barrels per pa. |
| `launch_angle_avg` | Float64 | Launch angle avg. |
| `exit_velocity_avg` | Float64 | Exit velocity avg. |
| `exit_velocity_max` | Float64 | Exit velocity max. |
| `hard_hit_percent` | Float64 | Hard-hit rate (95+ mph EV). |
| `sweet_spot_percent` | Float64 | Sweet-spot rate (8-32 deg launch angle). |
| `ba` | Float64 | Batting average. |
| `xba` | Float64 | Expected batting average. |
| `bacon` | Float64 | Bacon. |
| `xbacon` | Float64 | Expected batting average on contact. |
| `babip` | Float64 | BABIP. |
| `obp` | Float64 | On-base percentage. |
| `slg` | Float64 | Slugging percentage. |
| `xobp` | Float64 | Expected on-base percentage. |
| `xslg` | Float64 | Expected slugging. |
| `iso` | Float64 | Isolated power. |
| `xiso` | Float64 | Expected isolated power. |
| `woba` | Float64 | Weighted on-base average. |
| `xwoba` | Float64 | Expected wOBA. |
| `wobacon` | Float64 | Wobacon. |
| `xwobacon` | Float64 | Xwobacon. |
| `xbadiff` | Float64 | Xbadiff. |
| `xslgdiff` | Float64 | Xslgdiff. |
| `wobadiff` | Float64 | Wobadiff. |
| `player_type` | String | Player type. |
| `era` | String | Era. |
| `xera` | String | Expected ERA. |
| `avg_hyper_speed` | Float64 | Avg hyper speed. |
| `avg_best_speed` | Float64 | Avg best speed. |
| `distance_hr_avg` | Int64 | Distance hr avg. |
| `sprint_speed` | Float64 | Sprint speed (ft/sec, top 50% of competitive runs). |
| `pop_2b` | String | Pop 2b. |
| `arm_cs_2b` | String | Arm cs 2b. |
| `strike_rate` | String | Called-strike rate. |
| `outs_above_average` | Int64 | Outs Above Average. |
| `jump_v_avg` | Int64 | Jump v avg. |
| `max_arm_strength` | String | Max arm strength (mph). |
| `arm_overall` | String | Arm overall. |
| `xhr` | Float64 | Xhr. |
| `swing_take_run_value` | Int64 | Swing take run value. |
| `blocks_above_average` | String | Blocks above average. |
| `cs_above_average` | String | Cs above average. |
| `fastball_velo` | String | Fastball velo. |
| `fastball_spin` | String | Fastball spin. |
| `fastball_extension` | String | Fastball extension. |
| `curveball_spin` | String | Curveball spin. |
| `pitch_run_value_fastball` | Float64 | Pitch run value fastball. |
| `pitch_run_value_breaking` | Float64 | Pitch run value breaking. |
| `pitch_run_value_offspeed` | Float64 | Pitch run value offspeed. |
| `group_fastball_velo` | Float64 | Group fastball velo. |
| `group_breaking_velo` | Float64 | Group breaking velo. |
| `group_offspeed_velo` | Float64 | Group offspeed velo. |
| `pitch_usage_fastball` | Float64 | Pitch usage fastball. |
| `pitch_usage_breaking` | Float64 | Pitch usage breaking. |
| `pitch_usage_offspeed` | Float64 | Pitch usage offspeed. |
| `fielding_run_value` | Int64 | Fielding run value. |
| `runner_run_value` | Int64 | Runner run value. |
| `fielding_run_value_arm` | Int64 | Fielding run value arm. |
| `fielding_run_value_framing` | String | Fielding run value framing. |
| `runner_runs_sb` | Int64 | Runner runs sb. |
| `runner_runs_xb` | Int64 | Runner runs xb. |
| `net_bases_runner` | Int64 | Net bases runner. |
| `net_bases_pitcher` | String | Net bases pitcher. |
| `fast_swing_rate` | String | Fast-swing rate (>=75 mph). |
| `squared_up_contact` | String | Squared up contact. |
| `squared_up_swing` | String | Squared up swing. |
| `blasts_contact` | String | Blasts contact. |
| `blasts_swing` | String | Blasts swing. |
| `swords` | String | Swords. |
| `avg_swing_speed` | String | Avg swing speed. |
| `avg_swing_length` | String | Avg swing length. |
| `attack_angle` | String | Attack angle (deg, bat path at contact). |
| `vertical_swing_path` | String | Vertical swing path. |
| `acceleration` | String | Acceleration. |
| `horizontal_swing_path` | String | Horizontal swing path. |
| `attack_direction` | String | Attack direction (deg, pull/oppo). |
| `ideal_angle_rate` | String | Ideal angle rate. |
| `n_squared_up` | String | Number of squared up. |
| `n_blasts` | String | Number of blasts. |
| `arm_angle` | String | Arm angle. |
| `is_qualified` | Int64 | Is qualified. |
| `percent_rank_barrel_unrounded` | String | Percent rank barrel unrounded. |
| `percent_rank_barrel_batted_rate_unrounded` | String | Percent rank barrel batted rate unrounded. |
| `percent_rank_exit_velocity_avg_unrounded` | String | Percent rank exit velocity avg unrounded. |
| `percent_rank_exit_velocity_max_unrounded` | Float64 | Percent rank exit velocity max unrounded. |
| `percent_rank_launch_angle_avg_unrounded` | String | Percent rank launch angle avg unrounded. |
| `percent_rank_xba_unrounded` | String | Percent rank xba unrounded. |
| `percent_rank_xslg_unrounded` | String | Percent rank xslg unrounded. |
| `percent_rank_xwoba_unrounded` | String | Percent rank xwoba unrounded. |
| `percent_rank_woba_unrounded` | String | Percent rank woba unrounded. |
| `percent_rank_hard_hit_percent_unrounded` | String | Percent rank hard hit percent unrounded. |
| `percent_rank_xwobacon_unrounded` | String | Percent rank xwobacon unrounded. |
| `percent_rank_wobacon_unrounded` | String | Percent rank wobacon unrounded. |
| `percent_rank_k_percent_unrounded` | String | Percent rank k percent unrounded. |
| `percent_rank_bb_percent_unrounded` | String | Percent rank bb percent unrounded. |
| `percent_rank_sz_judge_unrounded` | String | Percent rank sz judge unrounded. |
| `percent_rank_whiff_percent_unrounded` | String | Percent rank whiff percent unrounded. |
| `percent_rank_chase_percent_unrounded` | String | Percent rank chase percent unrounded. |
| `percent_rank_ba_unrounded` | String | Percent rank ba unrounded. |
| `percent_rank_bacon_unrounded` | String | Percent rank bacon unrounded. |
| `percent_rank_xbacon_unrounded` | String | Percent rank xbacon unrounded. |
| `percent_rank_babip_unrounded` | String | Percent rank babip unrounded. |
| `percent_rank_obp_unrounded` | String | Percent rank obp unrounded. |
| `percent_rank_slg_unrounded` | String | Percent rank slg unrounded. |
| `percent_rank_xobp_unrounded` | String | Percent rank xobp unrounded. |
| `percent_rank_iso_unrounded` | String | Percent rank iso unrounded. |
| `percent_rank_xiso_unrounded` | String | Percent rank xiso unrounded. |
| `percent_rank_sweet_spot_percent_unrounded` | String | Percent rank sweet spot percent unrounded. |
| `percent_rank_distance_hr_avg_unrounded` | String | Percent rank distance hr avg unrounded. |
| `percent_rank_groundballs_percent_unrounded` | String | Percent rank groundballs percent unrounded. |
| `percent_rank_airballs_percent_unrounded` | String | Percent rank airballs percent unrounded. |
| `percent_rank_avg_hyper_speed_unrounded` | String | Percent rank avg hyper speed unrounded. |
| `percent_rank_avg_best_speed_unrounded` | String | Percent rank avg best speed unrounded. |
| `percent_rank_pitch_run_value_fastball_unrounded` | String | Percent rank pitch run value fastball unrounded. |
| `percent_rank_pitch_run_value_breaking_unrounded` | String | Percent rank pitch run value breaking unrounded. |
| `percent_rank_pitch_run_value_offspeed_unrounded` | String | Percent rank pitch run value offspeed unrounded. |
| `percent_rank_barrel` | String | Percent rank barrel. |
| `percent_rank_barrel_batted_rate` | String | Percent rank barrel batted rate. |
| `percent_rank_exit_velocity_avg` | String | Percent rank exit velocity avg. |
| `percent_rank_exit_velocity_max` | Int64 | Percent rank exit velocity max. |
| `percent_rank_launch_angle_avg` | String | Percent rank launch angle avg. |
| `percent_rank_xba` | String | Percent rank xba. |
| `percent_rank_xslg` | String | Percent rank xslg. |
| `percent_rank_xwoba` | String | Percent rank xwoba. |
| `percent_rank_woba` | String | Percent rank woba. |
| `percent_rank_hard_hit_percent` | String | Percent rank hard hit rate. |
| `percent_rank_xwobacon` | String | Percent rank xwobacon. |
| `percent_rank_wobacon` | String | Percent rank wobacon. |
| `percent_rank_k_percent` | String | Percent rank k rate. |
| `percent_rank_bb_percent` | String | Percent rank bb rate. |
| `percent_rank_sz_judge` | String | Percent rank sz judge. |
| `percent_rank_whiff_percent` | String | Percent rank whiff rate. |
| `percent_rank_chase_percent` | String | Percent rank chase rate. |
| `percent_rank_ba` | String | Percent rank ba. |
| `percent_rank_bacon` | String | Percent rank bacon. |
| `percent_rank_xbacon` | String | Percent rank xbacon. |
| `percent_rank_babip` | String | Percent rank babip. |
| `percent_rank_obp` | String | Percent rank obp. |
| `percent_rank_slg` | String | Percent rank slg. |
| `percent_rank_xobp` | String | Percent rank xobp. |
| `percent_rank_iso` | String | Percent rank iso. |
| `percent_rank_xiso` | String | Percent rank xiso. |
| `percent_rank_sweet_spot_percent` | String | Percent rank sweet spot rate. |
| `percent_rank_distance_hr_avg` | String | Percent rank distance hr avg. |
| `percent_rank_groundballs_percent` | String | Percent rank groundballs rate. |
| `percent_rank_airballs_percent` | String | Percent rank airballs rate. |
| `percent_rank_avg_hyper_speed` | String | Percent rank avg hyper speed. |
| `percent_rank_avg_best_speed` | String | Percent rank avg best speed. |
| `percent_rank_pitch_run_value_fastball` | String | Percent rank pitch run value fastball. |
| `percent_rank_pitch_run_value_breaking` | String | Percent rank pitch run value breaking. |
| `percent_rank_pitch_run_value_offspeed` | String | Percent rank pitch run value offspeed. |
| `percent_speed_order` | Int64 | Percent speed order. |
| `percent_rank_speed_order` | Int64 | Percent rank speed order. |
| `percent_rank_pop_2b` | String | Percent rank pop 2b. |
| `percent_rank_arm_cs_2b` | String | Percent rank arm cs 2b. |
| `percent_rank_oaa` | String | Percent rank oaa. |
| `percent_rank_framing` | String | Percent rank framing. |
| `percent_rank_jump` | String | Percent rank jump. |
| `percent_rank_fastball_velo` | String | Percent rank fastball velo. |
| `percent_rank_fastball_spin` | String | Percent rank fastball spin. |
| `percent_rank_fastball_extension` | String | Percent rank fastball extension. |
| `percent_rank_cu_spin` | String | Percent rank cu spin. |
| `percent_rank_xera` | String | Percent rank xera. |
| `percent_rank_arm_max` | String | Percent rank arm max. |
| `percent_rank_arm_overall` | String | Percent rank arm overall. |
| `percent_rank_xhr` | String | Percent rank xhr. |
| `percent_rank_swing_take_run_value` | String | Percent rank swing take run value. |
| `percent_rank_blocks_above_average` | String | Percent rank blocks above average. |
| `percent_rank_cs_above_average` | String | Percent rank cs above average. |
| `percent_rank_fielding_run_value` | String | Percent rank fielding run value. |
| `percent_rank_runner_run_value` | String | Percent rank runner run value. |
| `percent_rank_fielding_run_value_arm` | String | Percent rank fielding run value arm. |
| `percent_rank_fielding_run_value_framing` | String | Percent rank fielding run value framing. |
| `percent_rank_swing_speed` | String | Percent rank swing speed. |
| `percent_rank_swing_length` | String | Percent rank swing length. |
| `percent_rank_squared_up_swing` | String | Percent rank squared up swing. |
| `percent_rank_attack_angle` | String | Percent rank attack angle. |
| `percent_rank_vertical_swing_path` | String | Percent rank vertical swing path. |
| `percent_rank_acceleration` | String | Percent rank acceleration. |
| `percent_rank_ideal_angle_rate` | String | Percent rank ideal angle rate. |

**Example**

```python
from sportsdataverse.mlb import mlb_statcast_player
df = mlb_statcast_player(592450)
html = mlb_statcast_player(592450, raw=True)
```

### mlb_statcast_search {#mlb_statcast_search}

`mlb_statcast_search(start_dt: 'str', end_dt: 'str', *, player_type: 'str' = 'batter', chunk_days: 'int' = 7, return_as_pandas: 'bool' = False, **filters: 'Any') -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Pitch-by-pitch MLB Statcast search (`/statcast_search/csv`), date-chunked.

A window starting before `STATCAST_SEARCH_FLOOR` (2008-03-25, the first
PITCHf/x regular-season day) is clamped to the floor with a `UserWarning`;
Savant answers earlier dates with empty CSVs, which used to come back as a
silent zero-row frame.

Savant caps a single `/statcast_search/csv` response at **25,000 rows with
no pagination**. This splits the date range into `chunk_days` windows,
halving any window that hits the cap, and stitches the chunks back together.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `start_dt` | `str` |  |  |
| `end_dt` | `str` |  |  |
| `player_type` | `str` | `'batter'` | `"batter"` (default) or `"pitcher"`. |
| `chunk_days` | `int` | `7` | initial window size in days. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per pitch. A window with no games returns zero rows with the documented columns and dtypes (MLBAM ids `Int64`), so it stacks onto a populated one with either relaxed concat.

| col_name | type | description |
|---|---|---|
| `pitch_type` | String | Abbreviation of the pitch type thrown (e.g. FF, SL, CH). |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `release_speed` | Float64 | Pitch velocity out of the hand (mph). |
| `release_pos_x` | Float64 | Horizontal release position of the ball, catcher's perspective (feet). |
| `release_pos_z` | Float64 | Vertical release position of the ball, catcher's perspective (feet). |
| `player_name` | String | Name of the `player_type`-side player, `Last, First` -- the batter under the default `player_type="batter"`, the pitcher under `player_type="pitcher"`. |
| `batter` | Int64 | MLBAM player id of the batter. |
| `pitcher` | Int64 | MLBAM player id of the pitcher. |
| `events` | String | Nested list of non-game events. |
| `description` | String | Long-form description text. |
| `spin_dir` | Float64 | Deprecated spin direction field, no longer populated. |
| `spin_rate_deprecated` | Float64 | Deprecated legacy spin-rate field, no longer populated. |
| `break_angle_deprecated` | Float64 | Deprecated legacy break-angle field, no longer populated. |
| `break_length_deprecated` | Float64 | Deprecated legacy break-length field, no longer populated. |
| `zone` | Int64 | Strike-zone region the pitch crossed (1-14 Gameday zone). |
| `des` | String | Full text description of the play. |
| `game_type` | String | Game type code (R, P, etc.). |
| `stand` | String | Side of the plate the batter is standing (L or R). |
| `p_throws` | String | Hand the pitcher throws with (L or R). |
| `home_team` | String | Home team name. |
| `away_team` | String | Away team name. |
| `type` | String | Record type / category. |
| `hit_location` | Float64 | Fielder position number that fielded the ball. |
| `bb_type` | String | Batted-ball type (ground_ball, line_drive, fly_ball, popup). |
| `balls` | Int64 | Ball count before the pitch. |
| `strikes` | Int64 | Strike count before the pitch. |
| `game_year` | Int64 | Season year of the game. |
| `pfx_x` | Float64 | Horizontal pitch movement from the catcher's perspective (feet). |
| `pfx_z` | Float64 | Vertical pitch movement from the catcher's perspective (feet). |
| `plate_x` | Float64 | Horizontal position of the pitch crossing the plate (feet from center). |
| `plate_z` | Float64 | Vertical position of the pitch crossing the plate (feet above ground). |
| `on_3b` | Int64 | MLBAM ID of the runner on third base, if any. |
| `on_2b` | Int64 | MLBAM ID of the runner on second base, if any. |
| `on_1b` | Int64 | MLBAM ID of the runner on first base, if any. |
| `outs_when_up` | Int64 | Number of outs when the batter came to the plate. |
| `inning` | Int64 | Inning number. |
| `inning_topbot` | String | Half of the inning (Top or Bot). |
| `hc_x` | Float64 | Hit coordinate X on the field diagram. |
| `hc_y` | Float64 | Hit coordinate Y on the field diagram. |
| `tfs_deprecated` | Float64 | Deprecated time-from-start field, no longer populated. |
| `tfs_zulu_deprecated` | Float64 | Deprecated Zulu time-from-start field, no longer populated. |
| `umpire` | Float64 | Deprecated umpire field, no longer populated. |
| `sv_id` | Float64 | Deprecated Sportvision/Statcast pitch identifier, no longer populated. |
| `vx0` | Float64 | Velocity of the pitch in the x-direction at y=50 ft (ft/s). |
| `vy0` | Float64 | Velocity of the pitch in the y-direction at y=50 ft (ft/s). |
| `vz0` | Float64 | Velocity of the pitch in the z-direction at y=50 ft (ft/s). |
| `ax` | Float64 | Acceleration of the pitch in the x-direction at y=50 ft (ft/s^2). |
| `ay` | Float64 | Acceleration of the pitch in the y-direction at y=50 ft (ft/s^2). |
| `az` | Float64 | Acceleration of the pitch in the z-direction at y=50 ft (ft/s^2). |
| `sz_top` | Float64 | Top of the batter's strike zone for the pitch (feet). |
| `sz_bot` | Float64 | Bottom of the batter's strike zone for the pitch (feet). |
| `hit_distance_sc` | Float64 | Statcast-measured projected distance of the batted ball (feet). |
| `launch_speed` | Float64 | Exit velocity of the batted ball (mph). |
| `launch_angle` | Float64 | Vertical launch angle of the batted ball (degrees). |
| `effective_speed` | Float64 | Perceived velocity adjusted for release extension (mph). |
| `release_spin_rate` | Int64 | Spin rate of the pitch at release (rpm). |
| `release_extension` | Float64 | Distance toward the plate at release (feet). |
| `game_pk` | Int64 | Unique game identifier. |
| `fielder_2` | Int64 | MLBAM ID of the catcher. |
| `fielder_3` | Int64 | MLBAM ID of the first baseman. |
| `fielder_4` | Int64 | MLBAM ID of the second baseman. |
| `fielder_5` | Int64 | MLBAM ID of the third baseman. |
| `fielder_6` | Int64 | MLBAM ID of the shortstop. |
| `fielder_7` | Int64 | MLBAM ID of the left fielder. |
| `fielder_8` | Int64 | MLBAM ID of the center fielder. |
| `fielder_9` | Int64 | MLBAM ID of the right fielder. |
| `release_pos_y` | Float64 | Release position of the ball toward the plate (feet). |
| `estimated_ba_using_speedangle` | Float64 | Expected batting average based on exit velocity and launch angle. |
| `estimated_woba_using_speedangle` | Float64 | Expected wOBA based on exit velocity and launch angle. |
| `woba_value` | Float64 | wOBA value assigned to the event. |
| `woba_denom` | Float64 | wOBA denominator (plate-appearance weight) for the event. |
| `babip_value` | Float64 | BABIP value assigned to the event (0 or 1). |
| `iso_value` | Float64 | Isolated power value assigned to the event. |
| `launch_speed_angle` | Float64 | Batted-ball classification code (1-6) from exit velocity and angle. |
| `at_bat_number` | Int64 | Sequential plate-appearance number within the game. |
| `pitch_number` | Int64 | Pitch number within the plate appearance. |
| `pitch_name` | String | Full name of the pitch type (e.g. 4-Seam Fastball, Slider). |
| `home_score` | Int64 | Home team score before the pitch (see post_home_score). |
| `away_score` | Int64 | Away team score before the pitch (see post_away_score). |
| `bat_score` | Int64 | Batting team score before the pitch. |
| `fld_score` | Int64 | Fielding team score before the pitch. |
| `post_away_score` | Int64 | Away team score after the pitch. |
| `post_home_score` | Int64 | Home team score after the pitch. |
| `post_bat_score` | Int64 | Batting team score after the pitch. |
| `post_fld_score` | Int64 | Fielding team score after the pitch. |
| `if_fielding_alignment` | String | Infield defensive alignment (Standard, Strategic, Infield shift). |
| `of_fielding_alignment` | String | Outfield defensive alignment (Standard, Strategic, 4th outfielder). |
| `spin_axis` | Int64 | Spin axis of the pitch as a clock-face angle (degrees). |
| `delta_home_win_exp` | Float64 | Change in home team win expectancy on the play. |
| `delta_run_exp` | Float64 | Change in run expectancy on the play. |
| `bat_speed` | Float64 | Bat speed at the point of contact (mph). |
| `swing_length` | Float64 | Length of the swing path to contact (feet). |
| `miss_distance` | Float64 | Distance between the bat and the ball on a swing-and-miss, in inches (Savant bat-tracking field); populated only on swinging-strike pitches and null on every other pitch. |
| `estimated_slg_using_speedangle` | Float64 | Expected slugging based on exit velocity and launch angle. |
| `delta_pitcher_run_exp` | Float64 | Change in run expectancy credited to the pitcher. |
| `hyper_speed` | Float64 | Adjusted (90th-percentile) exit velocity (mph). |
| `home_score_diff` | Int64 | Home team score minus away team score before the pitch. |
| `bat_score_diff` | Int64 | Batting team score minus fielding team score before the pitch. |
| `home_win_exp` | Float64 | Home team win expectancy before the play. |
| `bat_win_exp` | Float64 | Batting team win expectancy before the play. |
| `age_pit_legacy` | Int64 | Pitcher age using the legacy calculation. |
| `age_bat_legacy` | Int64 | Batter age using the legacy calculation. |
| `age_pit` | Int64 | Pitcher age for the season. |
| `age_bat` | Int64 | Batter age for the season. |
| `n_thruorder_pitcher` | Int64 | Times through the order the pitcher is facing the lineup. |
| `n_priorpa_thisgame_player_at_bat` | Int64 | Number of prior plate appearances by the batter in the game. |
| `pitcher_days_since_prev_game` | Int64 | Days since the pitcher's previous game appearance. |
| `batter_days_since_prev_game` | Int64 | Days since the batter's previous game appearance. |
| `pitcher_days_until_next_game` | Int64 | Days until the pitcher's next game appearance. |
| `batter_days_until_next_game` | Int64 | Days until the batter's next game appearance. |
| `api_break_z_with_gravity` | Float64 | Vertical pitch break including gravity (inches). |
| `api_break_x_arm` | Float64 | Horizontal pitch break to the pitcher's arm side (inches). |
| `api_break_x_batter_in` | Float64 | Horizontal pitch break toward/away from the batter (inches). |
| `arm_angle` | Float64 | Pitcher's arm angle at release (degrees). |
| `attack_angle` | Float64 | Angle of the bat's path at contact (degrees). |
| `attack_direction` | Float64 | Horizontal direction of the swing at contact (degrees). |
| `swing_path_tilt` | Float64 | Vertical tilt of the swing path (degrees). |
| `intercept_ball_minus_batter_pos_x_inches` | Float64 | Horizontal offset of ball-bat intercept from batter position (inches). |
| `intercept_ball_minus_batter_pos_y_inches` | Float64 | Depth offset of ball-bat intercept from batter position (inches). |

**Raises**

| Exception | Description |
|---|---|
| `AssetFetchError` | When Savant answers an error status for a chunk after `download`'s retries, or a 200 body that is not a Statcast CSV (an HTML or error page); an outage is never reported as an empty window. |

**Example**

```python
from sportsdataverse.mlb import mlb_statcast_search
df = mlb_statcast_search("2024-06-15", "2024-06-16", batters_lookup=592450)
```

### mlb_statcast_search_minors {#mlb_statcast_search_minors}

`mlb_statcast_search_minors(start_dt: 'str', end_dt: 'str', *, player_type: 'str' = 'batter', chunk_days: 'int' = 7, return_as_pandas: 'bool' = False, **filters: 'Any') -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Minor-league Statcast search (`/statcast-search-minors/csv`), date-chunked.

Same shape, columns, and 25,000-row chunking as `mlb_statcast_search`,
against the MiLB CSV route with Savant's `minors=true&wbc=false` population
flags sent for you (the route path alone returns MLB games). The route pins those
two flags and **overrides** a `minors=`/`wbc=` passed through `**filters`;
every other filter rides along. Narrow further with `hfLevel` (`"AAA|"`,
`"AA|"`, `"A+|"`, `"A|"`) and `hfSea` filters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `start_dt` | `str` |  |  |
| `end_dt` | `str` |  |  |
| `player_type` | `str` | `'batter'` | `"batter"` (default) or `"pitcher"`. |
| `chunk_days` | `int` | `7` | initial window size in days. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per minor-league pitch. A window with no games returns zero rows with the documented columns and dtypes (MLBAM ids `Int64`), so it stacks onto a populated one with either relaxed concat.

| col_name | type | description |
|---|---|---|
| `pitch_type` | String | Abbreviation of the pitch type thrown (e.g. FF, SL, CH). |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `release_speed` | Float64 | Pitch velocity out of the hand (mph). |
| `release_pos_x` | Float64 | Horizontal release position of the ball, catcher's perspective (feet). |
| `release_pos_z` | Float64 | Vertical release position of the ball, catcher's perspective (feet). |
| `player_name` | String | Name of the `player_type`-side player, `Last, First` -- the batter under the default `player_type="batter"`, the pitcher under `player_type="pitcher"`. |
| `batter` | Int64 | MLBAM player id of the batter. |
| `pitcher` | Int64 | MLBAM player id of the pitcher. |
| `events` | String | Nested list of non-game events. |
| `description` | String | Long-form description text. |
| `spin_dir` | Float64 | Deprecated spin direction field, no longer populated. |
| `spin_rate_deprecated` | Float64 | Deprecated legacy spin-rate field, no longer populated. |
| `break_angle_deprecated` | Float64 | Deprecated legacy break-angle field, no longer populated. |
| `break_length_deprecated` | Float64 | Deprecated legacy break-length field, no longer populated. |
| `zone` | Int64 | Strike-zone region the pitch crossed (1-14 Gameday zone). |
| `des` | String | Full text description of the play. |
| `game_type` | String | Game type code (R, P, etc.). |
| `stand` | String | Side of the plate the batter is standing (L or R). |
| `p_throws` | String | Hand the pitcher throws with (L or R). |
| `home_team` | String | Home team name. |
| `away_team` | String | Away team name. |
| `type` | String | Record type / category. |
| `hit_location` | Float64 | Fielder position number that fielded the ball. |
| `bb_type` | String | Batted-ball type (ground_ball, line_drive, fly_ball, popup). |
| `balls` | Int64 | Ball count before the pitch. |
| `strikes` | Int64 | Strike count before the pitch. |
| `game_year` | Int64 | Season year of the game. |
| `pfx_x` | Float64 | Horizontal pitch movement from the catcher's perspective (feet). |
| `pfx_z` | Float64 | Vertical pitch movement from the catcher's perspective (feet). |
| `plate_x` | Float64 | Horizontal position of the pitch crossing the plate (feet from center). |
| `plate_z` | Float64 | Vertical position of the pitch crossing the plate (feet above ground). |
| `on_3b` | Int64 | MLBAM ID of the runner on third base, if any. |
| `on_2b` | Int64 | MLBAM ID of the runner on second base, if any. |
| `on_1b` | Int64 | MLBAM ID of the runner on first base, if any. |
| `outs_when_up` | Int64 | Number of outs when the batter came to the plate. |
| `inning` | Int64 | Inning number. |
| `inning_topbot` | String | Half of the inning (Top or Bot). |
| `hc_x` | Float64 | Hit coordinate X on the field diagram. |
| `hc_y` | Float64 | Hit coordinate Y on the field diagram. |
| `tfs_deprecated` | Float64 | Deprecated time-from-start field, no longer populated. |
| `tfs_zulu_deprecated` | Float64 | Deprecated Zulu time-from-start field, no longer populated. |
| `umpire` | Float64 | Deprecated umpire field, no longer populated. |
| `sv_id` | Float64 | Deprecated Sportvision/Statcast pitch identifier, no longer populated. |
| `vx0` | Float64 | Velocity of the pitch in the x-direction at y=50 ft (ft/s). |
| `vy0` | Float64 | Velocity of the pitch in the y-direction at y=50 ft (ft/s). |
| `vz0` | Float64 | Velocity of the pitch in the z-direction at y=50 ft (ft/s). |
| `ax` | Float64 | Acceleration of the pitch in the x-direction at y=50 ft (ft/s^2). |
| `ay` | Float64 | Acceleration of the pitch in the y-direction at y=50 ft (ft/s^2). |
| `az` | Float64 | Acceleration of the pitch in the z-direction at y=50 ft (ft/s^2). |
| `sz_top` | Float64 | Top of the batter's strike zone for the pitch (feet). |
| `sz_bot` | Float64 | Bottom of the batter's strike zone for the pitch (feet). |
| `hit_distance_sc` | Float64 | Statcast-measured projected distance of the batted ball (feet). |
| `launch_speed` | Float64 | Exit velocity of the batted ball (mph). |
| `launch_angle` | Float64 | Vertical launch angle of the batted ball (degrees). |
| `effective_speed` | Float64 | Perceived velocity adjusted for release extension (mph). |
| `release_spin_rate` | Int64 | Spin rate of the pitch at release (rpm). |
| `release_extension` | Float64 | Distance toward the plate at release (feet). |
| `game_pk` | Int64 | Unique game identifier. |
| `fielder_2` | Int64 | MLBAM ID of the catcher. |
| `fielder_3` | Int64 | MLBAM ID of the first baseman. |
| `fielder_4` | Int64 | MLBAM ID of the second baseman. |
| `fielder_5` | Int64 | MLBAM ID of the third baseman. |
| `fielder_6` | Int64 | MLBAM ID of the shortstop. |
| `fielder_7` | Int64 | MLBAM ID of the left fielder. |
| `fielder_8` | Int64 | MLBAM ID of the center fielder. |
| `fielder_9` | Int64 | MLBAM ID of the right fielder. |
| `release_pos_y` | Float64 | Release position of the ball toward the plate (feet). |
| `estimated_ba_using_speedangle` | Float64 | Expected batting average based on exit velocity and launch angle. |
| `estimated_woba_using_speedangle` | Float64 | Expected wOBA based on exit velocity and launch angle. |
| `woba_value` | Float64 | wOBA value assigned to the event. |
| `woba_denom` | Float64 | wOBA denominator (plate-appearance weight) for the event. |
| `babip_value` | Float64 | BABIP value assigned to the event (0 or 1). |
| `iso_value` | Float64 | Isolated power value assigned to the event. |
| `launch_speed_angle` | Float64 | Batted-ball classification code (1-6) from exit velocity and angle. |
| `at_bat_number` | Int64 | Sequential plate-appearance number within the game. |
| `pitch_number` | Int64 | Pitch number within the plate appearance. |
| `pitch_name` | String | Full name of the pitch type (e.g. 4-Seam Fastball, Slider). |
| `home_score` | Int64 | Home team score before the pitch (see post_home_score). |
| `away_score` | Int64 | Away team score before the pitch (see post_away_score). |
| `bat_score` | Int64 | Batting team score before the pitch. |
| `fld_score` | Int64 | Fielding team score before the pitch. |
| `post_away_score` | Int64 | Away team score after the pitch. |
| `post_home_score` | Int64 | Home team score after the pitch. |
| `post_bat_score` | Int64 | Batting team score after the pitch. |
| `post_fld_score` | Int64 | Fielding team score after the pitch. |
| `if_fielding_alignment` | String | Infield defensive alignment (Standard, Strategic, Infield shift). |
| `of_fielding_alignment` | String | Outfield defensive alignment (Standard, Strategic, 4th outfielder). |
| `spin_axis` | Int64 | Spin axis of the pitch as a clock-face angle (degrees). |
| `delta_home_win_exp` | Float64 | Change in home team win expectancy on the play. |
| `delta_run_exp` | Float64 | Change in run expectancy on the play. |
| `bat_speed` | Float64 | Bat speed at the point of contact (mph). |
| `swing_length` | Float64 | Length of the swing path to contact (feet). |
| `miss_distance` | Float64 | Distance between the bat and the ball on a swing-and-miss, in inches (Savant bat-tracking field); null on pitches without a tracked swing-and-miss (about 90% of sampled rows). |
| `estimated_slg_using_speedangle` | Float64 | Expected slugging based on exit velocity and launch angle. |
| `delta_pitcher_run_exp` | Float64 | Change in run expectancy credited to the pitcher. |
| `hyper_speed` | Float64 | Adjusted (90th-percentile) exit velocity (mph). |
| `home_score_diff` | Int64 | Home team score minus away team score before the pitch. |
| `bat_score_diff` | Int64 | Batting team score minus fielding team score before the pitch. |
| `home_win_exp` | Float64 | Home team win expectancy before the play. |
| `bat_win_exp` | Float64 | Batting team win expectancy before the play. |
| `age_pit_legacy` | Int64 | Pitcher age using the legacy calculation. |
| `age_bat_legacy` | Int64 | Batter age using the legacy calculation. |
| `age_pit` | Int64 | Pitcher age for the season. |
| `age_bat` | Int64 | Batter age for the season. |
| `n_thruorder_pitcher` | Int64 | Times through the order the pitcher is facing the lineup. |
| `n_priorpa_thisgame_player_at_bat` | Int64 | Number of prior plate appearances by the batter in the game. |
| `pitcher_days_since_prev_game` | Int64 | Days since the pitcher's previous game appearance. |
| `batter_days_since_prev_game` | Int64 | Days since the batter's previous game appearance. |
| `pitcher_days_until_next_game` | Int64 | Days until the pitcher's next game appearance. |
| `batter_days_until_next_game` | Int64 | Days until the batter's next game appearance. |
| `api_break_z_with_gravity` | Float64 | Vertical pitch break including gravity (inches). |
| `api_break_x_arm` | Float64 | Horizontal pitch break to the pitcher's arm side (inches). |
| `api_break_x_batter_in` | Float64 | Horizontal pitch break toward/away from the batter (inches). |
| `arm_angle` | Float64 | Pitcher's arm angle at release (degrees). |
| `attack_angle` | Float64 | Angle of the bat's path at contact (degrees). |
| `attack_direction` | Float64 | Horizontal direction of the swing at contact (degrees). |
| `swing_path_tilt` | Float64 | Vertical tilt of the swing path (degrees). |
| `intercept_ball_minus_batter_pos_x_inches` | Float64 | Horizontal offset of ball-bat intercept from batter position (inches). |
| `intercept_ball_minus_batter_pos_y_inches` | Float64 | Depth offset of ball-bat intercept from batter position (inches). |

**Raises**

| Exception | Description |
|---|---|
| `AssetFetchError` | When Savant answers an error status for a chunk after `download`'s retries, or a 200 body that is not a Statcast CSV (an HTML or error page); an outage is never reported as an empty window. |

**Example**

```python
from sportsdataverse.mlb import mlb_statcast_search_minors
df = mlb_statcast_search_minors("2024-06-01", "2024-06-02")
```

### mlb_statcast_search_wbc {#mlb_statcast_search_wbc}

`mlb_statcast_search_wbc(start_dt: 'str', end_dt: 'str', *, player_type: 'str' = 'batter', chunk_days: 'int' = 7, return_as_pandas: 'bool' = False, **filters: 'Any') -> "'Union[pl.DataFrame, pd.DataFrame]'"`

World Baseball Classic Statcast search (`/statcast-search-world-baseball-classic/csv`).

Same shape, columns, and 25,000-row chunking as `mlb_statcast_search`,
against the WBC CSV route with Savant's `minors=false&wbc=true` population
flags sent for you (the route path alone returns MLB spring training). The route
pins those two flags and **overrides** a `minors=`/`wbc=` passed through
`**filters`; every other filter rides along. Pass WBC date windows (e.g. March
of a WBC year); `game_type` is the tournament round (`F` pool play, `D`
quarterfinals, `L` semifinals, `W` championship).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `start_dt` | `str` |  |  |
| `end_dt` | `str` |  |  |
| `player_type` | `str` | `'batter'` | `"batter"` (default) or `"pitcher"`. |
| `chunk_days` | `int` | `7` | initial window size in days. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per WBC pitch. A window with no games returns zero rows with the documented columns and dtypes (MLBAM ids `Int64`), so it stacks onto a populated one with either relaxed concat.

| col_name | type | description |
|---|---|---|
| `pitch_type` | String | Abbreviation of the pitch type thrown (e.g. FF, SL, CH). |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `release_speed` | Float64 | Pitch velocity out of the hand (mph). |
| `release_pos_x` | Float64 | Horizontal release position of the ball, catcher's perspective (feet). |
| `release_pos_z` | Float64 | Vertical release position of the ball, catcher's perspective (feet). |
| `player_name` | String | Name of the `player_type`-side player, `Last, First` -- the batter under the default `player_type="batter"`, the pitcher under `player_type="pitcher"`. |
| `batter` | Int64 | MLBAM player id of the batter. |
| `pitcher` | Int64 | MLBAM player id of the pitcher. |
| `events` | String | Nested list of non-game events. |
| `description` | String | Long-form description text. |
| `spin_dir` | Float64 | Deprecated spin direction field, no longer populated. |
| `spin_rate_deprecated` | Float64 | Deprecated legacy spin-rate field, no longer populated. |
| `break_angle_deprecated` | Float64 | Deprecated legacy break-angle field, no longer populated. |
| `break_length_deprecated` | Float64 | Deprecated legacy break-length field, no longer populated. |
| `zone` | Int64 | Strike-zone region the pitch crossed (1-14 Gameday zone). |
| `des` | String | Full text description of the play. |
| `game_type` | String | Game type code (R, P, etc.). |
| `stand` | String | Side of the plate the batter is standing (L or R). |
| `p_throws` | String | Hand the pitcher throws with (L or R). |
| `home_team` | String | Home team name. |
| `away_team` | String | Away team name. |
| `type` | String | Record type / category. |
| `hit_location` | Float64 | Fielder position number that fielded the ball. |
| `bb_type` | String | Batted-ball type (ground_ball, line_drive, fly_ball, popup). |
| `balls` | Int64 | Ball count before the pitch. |
| `strikes` | Int64 | Strike count before the pitch. |
| `game_year` | Int64 | Season year of the game. |
| `pfx_x` | Float64 | Horizontal pitch movement from the catcher's perspective (feet). |
| `pfx_z` | Float64 | Vertical pitch movement from the catcher's perspective (feet). |
| `plate_x` | Float64 | Horizontal position of the pitch crossing the plate (feet from center). |
| `plate_z` | Float64 | Vertical position of the pitch crossing the plate (feet above ground). |
| `on_3b` | Int64 | MLBAM ID of the runner on third base, if any. |
| `on_2b` | Int64 | MLBAM ID of the runner on second base, if any. |
| `on_1b` | Int64 | MLBAM ID of the runner on first base, if any. |
| `outs_when_up` | Int64 | Number of outs when the batter came to the plate. |
| `inning` | Int64 | Inning number. |
| `inning_topbot` | String | Half of the inning (Top or Bot). |
| `hc_x` | Float64 | Hit coordinate X on the field diagram. |
| `hc_y` | Float64 | Hit coordinate Y on the field diagram. |
| `tfs_deprecated` | Float64 | Deprecated time-from-start field, no longer populated. |
| `tfs_zulu_deprecated` | Float64 | Deprecated Zulu time-from-start field, no longer populated. |
| `umpire` | Float64 | Deprecated umpire field, no longer populated. |
| `sv_id` | Float64 | Deprecated Sportvision/Statcast pitch identifier, no longer populated. |
| `vx0` | Float64 | Velocity of the pitch in the x-direction at y=50 ft (ft/s). |
| `vy0` | Float64 | Velocity of the pitch in the y-direction at y=50 ft (ft/s). |
| `vz0` | Float64 | Velocity of the pitch in the z-direction at y=50 ft (ft/s). |
| `ax` | Float64 | Acceleration of the pitch in the x-direction at y=50 ft (ft/s^2). |
| `ay` | Float64 | Acceleration of the pitch in the y-direction at y=50 ft (ft/s^2). |
| `az` | Float64 | Acceleration of the pitch in the z-direction at y=50 ft (ft/s^2). |
| `sz_top` | Float64 | Top of the batter's strike zone for the pitch (feet). |
| `sz_bot` | Float64 | Bottom of the batter's strike zone for the pitch (feet). |
| `hit_distance_sc` | Float64 | Statcast-measured projected distance of the batted ball (feet). |
| `launch_speed` | Float64 | Exit velocity of the batted ball (mph). |
| `launch_angle` | Float64 | Vertical launch angle of the batted ball (degrees). |
| `effective_speed` | Float64 | Perceived velocity adjusted for release extension (mph). |
| `release_spin_rate` | Int64 | Spin rate of the pitch at release (rpm). |
| `release_extension` | Float64 | Distance toward the plate at release (feet). |
| `game_pk` | Int64 | Unique game identifier. |
| `fielder_2` | Int64 | MLBAM ID of the catcher. |
| `fielder_3` | Int64 | MLBAM ID of the first baseman. |
| `fielder_4` | Int64 | MLBAM ID of the second baseman. |
| `fielder_5` | Int64 | MLBAM ID of the third baseman. |
| `fielder_6` | Int64 | MLBAM ID of the shortstop. |
| `fielder_7` | Int64 | MLBAM ID of the left fielder. |
| `fielder_8` | Int64 | MLBAM ID of the center fielder. |
| `fielder_9` | Int64 | MLBAM ID of the right fielder. |
| `release_pos_y` | Float64 | Release position of the ball toward the plate (feet). |
| `estimated_ba_using_speedangle` | Float64 | Expected batting average based on exit velocity and launch angle. |
| `estimated_woba_using_speedangle` | Float64 | Expected wOBA based on exit velocity and launch angle. |
| `woba_value` | Float64 | wOBA value assigned to the event. |
| `woba_denom` | Float64 | wOBA denominator (plate-appearance weight) for the event. |
| `babip_value` | Float64 | BABIP value assigned to the event (0 or 1). |
| `iso_value` | Float64 | Isolated power value assigned to the event. |
| `launch_speed_angle` | Float64 | Batted-ball classification code (1-6) from exit velocity and angle. |
| `at_bat_number` | Int64 | Sequential plate-appearance number within the game. |
| `pitch_number` | Int64 | Pitch number within the plate appearance. |
| `pitch_name` | String | Full name of the pitch type (e.g. 4-Seam Fastball, Slider). |
| `home_score` | Int64 | Home team score before the pitch (see post_home_score). |
| `away_score` | Int64 | Away team score before the pitch (see post_away_score). |
| `bat_score` | Int64 | Batting team score before the pitch. |
| `fld_score` | Int64 | Fielding team score before the pitch. |
| `post_away_score` | Int64 | Away team score after the pitch. |
| `post_home_score` | Int64 | Home team score after the pitch. |
| `post_bat_score` | Int64 | Batting team score after the pitch. |
| `post_fld_score` | Int64 | Fielding team score after the pitch. |
| `if_fielding_alignment` | String | Infield defensive alignment (Standard, Strategic, Infield shift). |
| `of_fielding_alignment` | String | Outfield defensive alignment (Standard, Strategic, 4th outfielder). |
| `spin_axis` | Int64 | Spin axis of the pitch as a clock-face angle (degrees). |
| `delta_home_win_exp` | Float64 | Change in home team win expectancy on the play. |
| `delta_run_exp` | Float64 | Change in run expectancy on the play. |
| `bat_speed` | Float64 | Bat speed at the point of contact (mph). |
| `swing_length` | Float64 | Length of the swing path to contact (feet). |
| `miss_distance` | Float64 | Distance between the bat and the ball on a swing-and-miss, in inches (Savant bat-tracking field); all null in the sampled WBC pitches (2023-03-08), and coverage for later WBC tournaments is unverified. |
| `estimated_slg_using_speedangle` | Float64 | Expected slugging based on exit velocity and launch angle. |
| `delta_pitcher_run_exp` | Float64 | Change in run expectancy credited to the pitcher. |
| `hyper_speed` | Float64 | Adjusted (90th-percentile) exit velocity (mph). |
| `home_score_diff` | Int64 | Home team score minus away team score before the pitch. |
| `bat_score_diff` | Int64 | Batting team score minus fielding team score before the pitch. |
| `home_win_exp` | Float64 | Home team win expectancy before the play. |
| `bat_win_exp` | Float64 | Batting team win expectancy before the play. |
| `age_pit_legacy` | Int64 | Pitcher age using the legacy calculation. |
| `age_bat_legacy` | Int64 | Batter age using the legacy calculation. |
| `age_pit` | Int64 | Pitcher age for the season. |
| `age_bat` | Int64 | Batter age for the season. |
| `n_thruorder_pitcher` | Int64 | Times through the order the pitcher is facing the lineup. |
| `n_priorpa_thisgame_player_at_bat` | Int64 | Number of prior plate appearances by the batter in the game. |
| `pitcher_days_since_prev_game` | Int64 | Days since the pitcher's previous game appearance. |
| `batter_days_since_prev_game` | Int64 | Days since the batter's previous game appearance. |
| `pitcher_days_until_next_game` | Int64 | Days until the pitcher's next game appearance. |
| `batter_days_until_next_game` | Int64 | Days until the batter's next game appearance. |
| `api_break_z_with_gravity` | Float64 | Vertical pitch break including gravity (inches). |
| `api_break_x_arm` | Float64 | Horizontal pitch break to the pitcher's arm side (inches). |
| `api_break_x_batter_in` | Float64 | Horizontal pitch break toward/away from the batter (inches). |
| `arm_angle` | Float64 | Pitcher's arm angle at release (degrees). |
| `attack_angle` | Float64 | Angle of the bat's path at contact (degrees). |
| `attack_direction` | Float64 | Horizontal direction of the swing at contact (degrees). |
| `swing_path_tilt` | Float64 | Vertical tilt of the swing path (degrees). |
| `intercept_ball_minus_batter_pos_x_inches` | Float64 | Horizontal offset of ball-bat intercept from batter position (inches). |
| `intercept_ball_minus_batter_pos_y_inches` | Float64 | Depth offset of ball-bat intercept from batter position (inches). |

**Raises**

| Exception | Description |
|---|---|
| `AssetFetchError` | When Savant answers an error status for a chunk after `download`'s retries, or a 200 body that is not a Statcast CSV (an HTML or error page); an outage is never reported as an empty window. |

**Example**

```python
from sportsdataverse.mlb import mlb_statcast_search_wbc
df = mlb_statcast_search_wbc("2023-03-08", "2023-03-22")
```
