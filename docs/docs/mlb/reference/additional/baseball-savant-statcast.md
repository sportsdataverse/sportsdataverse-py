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
| `aggregate` | integer | Aggregate. |
| `year` | integer | Season year. |
| `yearhidden` | integer | Yearhidden. |
| `player_id` | integer | MLBAM player id. |
| `age` | integer | Player age. |
| `bat_side` | character | Batter side (R/L/S). |
| `pitch_hand` | character | Pitcher handedness (R/L). |
| `month` | character | Month. |
| `grouping_code` | character | Grouping code. |
| `grouping_cat` | character | Grouping cat. |
| `pitch_count` | integer | Pitch count. |
| `in_zone_percent` | numeric | In zone rate. |
| `out_zone_percent` | numeric | Out zone rate. |
| `edge_percent` | numeric | Edge rate. |
| `z_swing_percent` | numeric | Z swing rate. |
| `oz_swing_percent` | numeric | Oz swing rate. |
| `iz_contact_percent` | numeric | Iz contact rate. |
| `oz_contact_percent` | numeric | Oz contact rate. |
| `whiff_percent` | numeric | Whiff rate (swings and misses / swings). |
| `f_strike_percent` | numeric | F strike rate. |
| `f_swing_percent` | numeric | F swing rate. |
| `swing_percent` | numeric | Swing rate. |
| `meatball_swing_percent` | integer | Meatball swing rate. |
| `meatball_percent` | numeric | Meatball rate. |
| `z_swing_miss_percent` | numeric | Z swing miss rate. |
| `oz_swing_miss_percent` | numeric | Oz swing miss rate. |
| `in_zone` | integer | In zone. |
| `out_zone` | integer | Out zone. |
| `edge` | integer | Edge. |
| `popups` | integer | Popups. |
| `flyballs` | integer | Flyballs. |
| `linedrives` | integer | Linedrives. |
| `groundballs` | integer | Groundballs. |
| `airballs` | integer | Airballs. |
| `popups_percent` | numeric | Popups rate. |
| `flyballs_percent` | numeric | Flyballs rate. |
| `linedrives_percent` | numeric | Linedrives rate. |
| `groundballs_percent` | numeric | Groundballs rate. |
| `airballs_percent` | numeric | Airballs rate. |
| `pull_percent` | numeric | Pull rate. |
| `straightaway_percent` | numeric | Straightaway rate. |
| `opposite_percent` | numeric | Opposite rate. |
| `pull_percent_airballs` | numeric | Pull percent airballs. |
| `straightaway_percent_airballs` | numeric | Straightaway percent airballs. |
| `opposite_percent_airballs` | integer | Opposite percent airballs. |
| `pull_percent_groundballs` | numeric | Pull percent groundballs. |
| `straightaway_percent_groundballs` | numeric | Straightaway percent groundballs. |
| `opposite_percent_groundballs` | numeric | Opposite percent groundballs. |
| `pull_percent_popups` | numeric | Pull percent popups. |
| `straightaway_percent_popups` | numeric | Straightaway percent popups. |
| `opposite_percent_popups` | numeric | Opposite percent popups. |
| `pull_percent_flyballs` | numeric | Pull percent flyballs. |
| `straightaway_percent_flyballs` | numeric | Straightaway percent flyballs. |
| `opposite_percent_flyballs` | numeric | Opposite percent flyballs. |
| `pull_percent_linedrives` | integer | Pull percent linedrives. |
| `straightaway_percent_linedrives` | integer | Straightaway percent linedrives. |
| `opposite_percent_linedrives` | integer | Opposite percent linedrives. |
| `poorlyweak_percent` | integer | Poorlyweak rate. |
| `poorlytopped_percent` | numeric | Poorlytopped rate. |
| `poorlyunder_percent` | numeric | Poorlyunder rate. |
| `flareburner_percent` | numeric | Flareburner rate. |
| `solidcontact_percent` | numeric | Solidcontact rate. |
| `hr_flyballs_percent` | numeric | Hr flyballs rate. |
| `in_zone_swing` | integer | In zone swing. |
| `out_zone_swing` | integer | Out zone swing. |
| `in_zone_swing_miss` | integer | In zone swing miss. |
| `out_zone_swing_miss` | integer | Out zone swing miss. |
| `pitch_count_fastball` | integer | Pitch count fastball. |
| `pitch_count_offspeed` | integer | Pitch count offspeed. |
| `pitch_count_breaking` | integer | Pitch count breaking. |
| `pa` | integer | Plate appearances. |
| `ab` | integer | At-bats. |
| `hit` | integer | Hit. |
| `single` | integer | Singles. |
| `double` | integer | Doubles. |
| `triple` | integer | Triples. |
| `home_run` | integer | Home run. |
| `walk` | integer | Walk. |
| `strikeout` | integer | Strikeout. |
| `hbp` | integer | Hbp. |
| `k_percent` | numeric | Strikeout rate. |
| `bb_percent` | numeric | Walk rate. |
| `sz_judge` | numeric | Sz judge. |
| `batted_ball` | integer | Batted ball. |
| `barrel` | integer | Barrel. |
| `barrel_batted_rate` | numeric | Barrels per batted ball. |
| `barrels_per_pa` | numeric | Barrels per pa. |
| `launch_angle_avg` | numeric | Launch angle avg. |
| `exit_velocity_avg` | numeric | Exit velocity avg. |
| `exit_velocity_max` | numeric | Exit velocity max. |
| `hard_hit_percent` | numeric | Hard-hit rate (95+ mph EV). |
| `sweet_spot_percent` | numeric | Sweet-spot rate (8-32 deg launch angle). |
| `ba` | numeric | Batting average. |
| `xba` | numeric | Expected batting average. |
| `bacon` | numeric | Bacon. |
| `xbacon` | numeric | Expected batting average on contact. |
| `babip` | numeric | BABIP. |
| `obp` | numeric | On-base percentage. |
| `slg` | numeric | Slugging percentage. |
| `xobp` | numeric | Expected on-base percentage. |
| `xslg` | numeric | Expected slugging. |
| `iso` | numeric | Isolated power. |
| `xiso` | numeric | Expected isolated power. |
| `woba` | numeric | Weighted on-base average. |
| `xwoba` | numeric | Expected wOBA. |
| `wobacon` | numeric | Wobacon. |
| `xwobacon` | numeric | Xwobacon. |
| `xbadiff` | numeric | Xbadiff. |
| `xslgdiff` | numeric | Xslgdiff. |
| `wobadiff` | numeric | Wobadiff. |
| `player_type` | character | Player type. |
| `era` | character | Era. |
| `xera` | character | Expected ERA. |
| `avg_hyper_speed` | numeric | Avg hyper speed. |
| `avg_best_speed` | numeric | Avg best speed. |
| `distance_hr_avg` | integer | Distance hr avg. |
| `sprint_speed` | numeric | Sprint speed (ft/sec, top 50% of competitive runs). |
| `pop_2b` | character | Pop 2b. |
| `arm_cs_2b` | character | Arm cs 2b. |
| `strike_rate` | character | Called-strike rate. |
| `outs_above_average` | integer | Outs Above Average. |
| `jump_v_avg` | integer | Jump v avg. |
| `max_arm_strength` | character | Max arm strength (mph). |
| `arm_overall` | character | Arm overall. |
| `xhr` | numeric | Xhr. |
| `swing_take_run_value` | integer | Swing take run value. |
| `blocks_above_average` | character | Blocks above average. |
| `cs_above_average` | character | Cs above average. |
| `fastball_velo` | character | Fastball velo. |
| `fastball_spin` | character | Fastball spin. |
| `fastball_extension` | character | Fastball extension. |
| `curveball_spin` | character | Curveball spin. |
| `pitch_run_value_fastball` | numeric | Pitch run value fastball. |
| `pitch_run_value_breaking` | numeric | Pitch run value breaking. |
| `pitch_run_value_offspeed` | numeric | Pitch run value offspeed. |
| `group_fastball_velo` | numeric | Group fastball velo. |
| `group_breaking_velo` | numeric | Group breaking velo. |
| `group_offspeed_velo` | numeric | Group offspeed velo. |
| `pitch_usage_fastball` | numeric | Pitch usage fastball. |
| `pitch_usage_breaking` | numeric | Pitch usage breaking. |
| `pitch_usage_offspeed` | numeric | Pitch usage offspeed. |
| `fielding_run_value` | integer | Fielding run value. |
| `runner_run_value` | integer | Runner run value. |
| `fielding_run_value_arm` | integer | Fielding run value arm. |
| `fielding_run_value_framing` | character | Fielding run value framing. |
| `runner_runs_sb` | integer | Runner runs sb. |
| `runner_runs_xb` | integer | Runner runs xb. |
| `net_bases_runner` | integer | Net bases runner. |
| `net_bases_pitcher` | character | Net bases pitcher. |
| `fast_swing_rate` | character | Fast-swing rate (>=75 mph). |
| `squared_up_contact` | character | Squared up contact. |
| `squared_up_swing` | character | Squared up swing. |
| `blasts_contact` | character | Blasts contact. |
| `blasts_swing` | character | Blasts swing. |
| `swords` | character | Swords. |
| `avg_swing_speed` | character | Avg swing speed. |
| `avg_swing_length` | character | Avg swing length. |
| `attack_angle` | character | Attack angle (deg, bat path at contact). |
| `vertical_swing_path` | character | Vertical swing path. |
| `acceleration` | character | Acceleration. |
| `horizontal_swing_path` | character | Horizontal swing path. |
| `attack_direction` | character | Attack direction (deg, pull/oppo). |
| `ideal_angle_rate` | character | Ideal angle rate. |
| `n_squared_up` | character | Number of squared up. |
| `n_blasts` | character | Number of blasts. |
| `arm_angle` | character | Arm angle. |
| `is_qualified` | integer | Is qualified. |
| `percent_rank_barrel_unrounded` | character | Percent rank barrel unrounded. |
| `percent_rank_barrel_batted_rate_unrounded` | character | Percent rank barrel batted rate unrounded. |
| `percent_rank_exit_velocity_avg_unrounded` | character | Percent rank exit velocity avg unrounded. |
| `percent_rank_exit_velocity_max_unrounded` | numeric | Percent rank exit velocity max unrounded. |
| `percent_rank_launch_angle_avg_unrounded` | character | Percent rank launch angle avg unrounded. |
| `percent_rank_xba_unrounded` | character | Percent rank xba unrounded. |
| `percent_rank_xslg_unrounded` | character | Percent rank xslg unrounded. |
| `percent_rank_xwoba_unrounded` | character | Percent rank xwoba unrounded. |
| `percent_rank_woba_unrounded` | character | Percent rank woba unrounded. |
| `percent_rank_hard_hit_percent_unrounded` | character | Percent rank hard hit percent unrounded. |
| `percent_rank_xwobacon_unrounded` | character | Percent rank xwobacon unrounded. |
| `percent_rank_wobacon_unrounded` | character | Percent rank wobacon unrounded. |
| `percent_rank_k_percent_unrounded` | character | Percent rank k percent unrounded. |
| `percent_rank_bb_percent_unrounded` | character | Percent rank bb percent unrounded. |
| `percent_rank_sz_judge_unrounded` | character | Percent rank sz judge unrounded. |
| `percent_rank_whiff_percent_unrounded` | character | Percent rank whiff percent unrounded. |
| `percent_rank_chase_percent_unrounded` | character | Percent rank chase percent unrounded. |
| `percent_rank_ba_unrounded` | character | Percent rank ba unrounded. |
| `percent_rank_bacon_unrounded` | character | Percent rank bacon unrounded. |
| `percent_rank_xbacon_unrounded` | character | Percent rank xbacon unrounded. |
| `percent_rank_babip_unrounded` | character | Percent rank babip unrounded. |
| `percent_rank_obp_unrounded` | character | Percent rank obp unrounded. |
| `percent_rank_slg_unrounded` | character | Percent rank slg unrounded. |
| `percent_rank_xobp_unrounded` | character | Percent rank xobp unrounded. |
| `percent_rank_iso_unrounded` | character | Percent rank iso unrounded. |
| `percent_rank_xiso_unrounded` | character | Percent rank xiso unrounded. |
| `percent_rank_sweet_spot_percent_unrounded` | character | Percent rank sweet spot percent unrounded. |
| `percent_rank_distance_hr_avg_unrounded` | character | Percent rank distance hr avg unrounded. |
| `percent_rank_groundballs_percent_unrounded` | character | Percent rank groundballs percent unrounded. |
| `percent_rank_airballs_percent_unrounded` | character | Percent rank airballs percent unrounded. |
| `percent_rank_avg_hyper_speed_unrounded` | character | Percent rank avg hyper speed unrounded. |
| `percent_rank_avg_best_speed_unrounded` | character | Percent rank avg best speed unrounded. |
| `percent_rank_pitch_run_value_fastball_unrounded` | character | Percent rank pitch run value fastball unrounded. |
| `percent_rank_pitch_run_value_breaking_unrounded` | character | Percent rank pitch run value breaking unrounded. |
| `percent_rank_pitch_run_value_offspeed_unrounded` | character | Percent rank pitch run value offspeed unrounded. |
| `percent_rank_barrel` | character | Percent rank barrel. |
| `percent_rank_barrel_batted_rate` | character | Percent rank barrel batted rate. |
| `percent_rank_exit_velocity_avg` | character | Percent rank exit velocity avg. |
| `percent_rank_exit_velocity_max` | integer | Percent rank exit velocity max. |
| `percent_rank_launch_angle_avg` | character | Percent rank launch angle avg. |
| `percent_rank_xba` | character | Percent rank xba. |
| `percent_rank_xslg` | character | Percent rank xslg. |
| `percent_rank_xwoba` | character | Percent rank xwoba. |
| `percent_rank_woba` | character | Percent rank woba. |
| `percent_rank_hard_hit_percent` | character | Percent rank hard hit rate. |
| `percent_rank_xwobacon` | character | Percent rank xwobacon. |
| `percent_rank_wobacon` | character | Percent rank wobacon. |
| `percent_rank_k_percent` | character | Percent rank k rate. |
| `percent_rank_bb_percent` | character | Percent rank bb rate. |
| `percent_rank_sz_judge` | character | Percent rank sz judge. |
| `percent_rank_whiff_percent` | character | Percent rank whiff rate. |
| `percent_rank_chase_percent` | character | Percent rank chase rate. |
| `percent_rank_ba` | character | Percent rank ba. |
| `percent_rank_bacon` | character | Percent rank bacon. |
| `percent_rank_xbacon` | character | Percent rank xbacon. |
| `percent_rank_babip` | character | Percent rank babip. |
| `percent_rank_obp` | character | Percent rank obp. |
| `percent_rank_slg` | character | Percent rank slg. |
| `percent_rank_xobp` | character | Percent rank xobp. |
| `percent_rank_iso` | character | Percent rank iso. |
| `percent_rank_xiso` | character | Percent rank xiso. |
| `percent_rank_sweet_spot_percent` | character | Percent rank sweet spot rate. |
| `percent_rank_distance_hr_avg` | character | Percent rank distance hr avg. |
| `percent_rank_groundballs_percent` | character | Percent rank groundballs rate. |
| `percent_rank_airballs_percent` | character | Percent rank airballs rate. |
| `percent_rank_avg_hyper_speed` | character | Percent rank avg hyper speed. |
| `percent_rank_avg_best_speed` | character | Percent rank avg best speed. |
| `percent_rank_pitch_run_value_fastball` | character | Percent rank pitch run value fastball. |
| `percent_rank_pitch_run_value_breaking` | character | Percent rank pitch run value breaking. |
| `percent_rank_pitch_run_value_offspeed` | character | Percent rank pitch run value offspeed. |
| `percent_speed_order` | integer | Percent speed order. |
| `percent_rank_speed_order` | integer | Percent rank speed order. |
| `percent_rank_pop_2b` | character | Percent rank pop 2b. |
| `percent_rank_arm_cs_2b` | character | Percent rank arm cs 2b. |
| `percent_rank_oaa` | character | Percent rank oaa. |
| `percent_rank_framing` | character | Percent rank framing. |
| `percent_rank_jump` | character | Percent rank jump. |
| `percent_rank_fastball_velo` | character | Percent rank fastball velo. |
| `percent_rank_fastball_spin` | character | Percent rank fastball spin. |
| `percent_rank_fastball_extension` | character | Percent rank fastball extension. |
| `percent_rank_cu_spin` | character | Percent rank cu spin. |
| `percent_rank_xera` | character | Percent rank xera. |
| `percent_rank_arm_max` | character | Percent rank arm max. |
| `percent_rank_arm_overall` | character | Percent rank arm overall. |
| `percent_rank_xhr` | character | Percent rank xhr. |
| `percent_rank_swing_take_run_value` | character | Percent rank swing take run value. |
| `percent_rank_blocks_above_average` | character | Percent rank blocks above average. |
| `percent_rank_cs_above_average` | character | Percent rank cs above average. |
| `percent_rank_fielding_run_value` | character | Percent rank fielding run value. |
| `percent_rank_runner_run_value` | character | Percent rank runner run value. |
| `percent_rank_fielding_run_value_arm` | character | Percent rank fielding run value arm. |
| `percent_rank_fielding_run_value_framing` | character | Percent rank fielding run value framing. |
| `percent_rank_swing_speed` | character | Percent rank swing speed. |
| `percent_rank_swing_length` | character | Percent rank swing length. |
| `percent_rank_squared_up_swing` | character | Percent rank squared up swing. |
| `percent_rank_attack_angle` | character | Percent rank attack angle. |
| `percent_rank_vertical_swing_path` | character | Percent rank vertical swing path. |
| `percent_rank_acceleration` | character | Percent rank acceleration. |
| `percent_rank_ideal_angle_rate` | character | Percent rank ideal angle rate. |

**Example**

```python
from sportsdataverse.mlb import mlb_statcast_player
df = mlb_statcast_player(592450)
html = mlb_statcast_player(592450, raw=True)
```

### mlb_statcast_search {#mlb_statcast_search}

`mlb_statcast_search(start_dt: 'str', end_dt: 'str', *, player_type: 'str' = 'batter', chunk_days: 'int' = 7, return_as_pandas: 'bool' = False, **filters: 'Any') -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Pitch-by-pitch MLB Statcast search (`/statcast_search/csv`), date-chunked.

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

A polars (or pandas) DataFrame, one row per pitch. A window with no games returns zero rows keeping the documented columns -- MLBAM ids `Int64`, the rest `Null` (no values to infer a dtype from), so the frame widens cleanly into a populated one.

| col_name | type | description |
|---|---|---|
| `pitch_type` | character | Abbreviation of the pitch type thrown (e.g. FF, SL, CH). |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `release_speed` | double | Pitch velocity out of the hand (mph). |
| `release_pos_x` | double | Horizontal release position of the ball, catcher's perspective (feet). |
| `release_pos_z` | double | Vertical release position of the ball, catcher's perspective (feet). |
| `player_name` | character | Player name. |
| `batter` | integer | MLBAM player id of the batter. |
| `pitcher` | integer | Whether the position is a pitcher. |
| `events` | character | Nested list of non-game events. |
| `description` | character | Long-form description text. |
| `spin_dir` | double | Deprecated spin direction field, no longer populated. |
| `spin_rate_deprecated` | double | Deprecated legacy spin-rate field, no longer populated. |
| `break_angle_deprecated` | double | Deprecated legacy break-angle field, no longer populated. |
| `break_length_deprecated` | double | Deprecated legacy break-length field, no longer populated. |
| `zone` | double | Strike-zone region the pitch crossed (1-14 Gameday zone). |
| `des` | character | Full text description of the play. |
| `game_type` | character | Game type code (R, P, etc.). |
| `stand` | character | Side of the plate the batter is standing (L or R). |
| `p_throws` | character | Hand the pitcher throws with (L or R). |
| `home_team` | character | Home team name. |
| `away_team` | character | Away team name. |
| `type` | character | Record type / category. |
| `hit_location` | double | Fielder position number that fielded the ball. |
| `bb_type` | character | Batted-ball type (ground_ball, line_drive, fly_ball, popup). |
| `balls` | integer | Ball count before the pitch. |
| `strikes` | integer | Strike count before the pitch. |
| `game_year` | integer | Season year of the game. |
| `pfx_x` | double | Horizontal pitch movement from the catcher's perspective (feet). |
| `pfx_z` | double | Vertical pitch movement from the catcher's perspective (feet). |
| `plate_x` | double | Horizontal position of the pitch crossing the plate (feet from center). |
| `plate_z` | double | Vertical position of the pitch crossing the plate (feet above ground). |
| `on_3b` | integer | MLBAM ID of the runner on third base, if any. |
| `on_2b` | integer | MLBAM ID of the runner on second base, if any. |
| `on_1b` | integer | MLBAM ID of the runner on first base, if any. |
| `outs_when_up` | integer | Number of outs when the batter came to the plate. |
| `inning` | integer | Inning number. |
| `inning_topbot` | character | Half of the inning (Top or Bot). |
| `hc_x` | double | Hit coordinate X on the field diagram. |
| `hc_y` | double | Hit coordinate Y on the field diagram. |
| `tfs_deprecated` | double | Deprecated time-from-start field, no longer populated. |
| `tfs_zulu_deprecated` | double | Deprecated Zulu time-from-start field, no longer populated. |
| `umpire` | double | Deprecated umpire field, no longer populated. |
| `sv_id` | double | Deprecated Sportvision/Statcast pitch identifier, no longer populated. |
| `vx0` | double | Velocity of the pitch in the x-direction at y=50 ft (ft/s). |
| `vy0` | double | Velocity of the pitch in the y-direction at y=50 ft (ft/s). |
| `vz0` | double | Velocity of the pitch in the z-direction at y=50 ft (ft/s). |
| `ax` | double | Acceleration of the pitch in the x-direction at y=50 ft (ft/s^2). |
| `ay` | double | Acceleration of the pitch in the y-direction at y=50 ft (ft/s^2). |
| `az` | double | Acceleration of the pitch in the z-direction at y=50 ft (ft/s^2). |
| `sz_top` | double | Top of the batter's strike zone for the pitch (feet). |
| `sz_bot` | double | Bottom of the batter's strike zone for the pitch (feet). |
| `hit_distance_sc` | double | Statcast-measured projected distance of the batted ball (feet). |
| `launch_speed` | double | Exit velocity of the batted ball (mph). |
| `launch_angle` | double | Vertical launch angle of the batted ball (degrees). |
| `effective_speed` | double | Perceived velocity adjusted for release extension (mph). |
| `release_spin_rate` | double | Spin rate of the pitch at release (rpm). |
| `release_extension` | double | Distance toward the plate at release (feet). |
| `game_pk` | integer | Unique game identifier. |
| `fielder_2` | integer | MLBAM ID of the catcher. |
| `fielder_3` | integer | MLBAM ID of the first baseman. |
| `fielder_4` | integer | MLBAM ID of the second baseman. |
| `fielder_5` | integer | MLBAM ID of the third baseman. |
| `fielder_6` | integer | MLBAM ID of the shortstop. |
| `fielder_7` | integer | MLBAM ID of the left fielder. |
| `fielder_8` | integer | MLBAM ID of the center fielder. |
| `fielder_9` | integer | MLBAM ID of the right fielder. |
| `release_pos_y` | double | Release position of the ball toward the plate (feet). |
| `estimated_ba_using_speedangle` | double | Expected batting average based on exit velocity and launch angle. |
| `estimated_woba_using_speedangle` | double | Expected wOBA based on exit velocity and launch angle. |
| `woba_value` | double | wOBA value assigned to the event. |
| `woba_denom` | double | wOBA denominator (plate-appearance weight) for the event. |
| `babip_value` | double | BABIP value assigned to the event (0 or 1). |
| `iso_value` | double | Isolated power value assigned to the event. |
| `launch_speed_angle` | double | Batted-ball classification code (1-6) from exit velocity and angle. |
| `at_bat_number` | integer | Sequential plate-appearance number within the game. |
| `pitch_number` | integer | Pitch number within the plate appearance. |
| `pitch_name` | character | Full name of the pitch type (e.g. 4-Seam Fastball, Slider). |
| `home_score` | integer | Home team run total after the play. |
| `away_score` | integer | Away team run total after the play. |
| `bat_score` | integer | Batting team score before the pitch. |
| `fld_score` | integer | Fielding team score before the pitch. |
| `post_away_score` | integer | Away team score after the pitch. |
| `post_home_score` | integer | Home team score after the pitch. |
| `post_bat_score` | integer | Batting team score after the pitch. |
| `post_fld_score` | integer | Fielding team score after the pitch. |
| `if_fielding_alignment` | character | Infield defensive alignment (Standard, Strategic, Infield shift). |
| `of_fielding_alignment` | character | Outfield defensive alignment (Standard, Strategic, 4th outfielder). |
| `spin_axis` | double | Spin axis of the pitch as a clock-face angle (degrees). |
| `delta_home_win_exp` | double | Change in home team win expectancy on the play. |
| `delta_run_exp` | double | Change in run expectancy on the play. |
| `bat_speed` | double | Bat speed at the point of contact (mph). |
| `swing_length` | double | Length of the swing path to contact (feet). |
| `miss_distance` | double | Distance between the bat and the ball on a swing-and-miss, in inches (Savant bat-tracking field); populated only on swinging-strike pitches and null on every other pitch. |
| `estimated_slg_using_speedangle` | double | Expected slugging based on exit velocity and launch angle. |
| `delta_pitcher_run_exp` | double | Change in run expectancy credited to the pitcher. |
| `hyper_speed` | double | Adjusted (90th-percentile) exit velocity (mph). |
| `home_score_diff` | integer | Home team score minus away team score before the pitch. |
| `bat_score_diff` | integer | Batting team score minus fielding team score before the pitch. |
| `home_win_exp` | double | Home team win expectancy before the play. |
| `bat_win_exp` | double | Batting team win expectancy before the play. |
| `age_pit_legacy` | integer | Pitcher age using the legacy calculation. |
| `age_bat_legacy` | integer | Batter age using the legacy calculation. |
| `age_pit` | integer | Pitcher age for the season. |
| `age_bat` | integer | Batter age for the season. |
| `n_thruorder_pitcher` | integer | Times through the order the pitcher is facing the lineup. |
| `n_priorpa_thisgame_player_at_bat` | integer | Number of prior plate appearances by the batter in the game. |
| `pitcher_days_since_prev_game` | double | Days since the pitcher's previous game appearance. |
| `batter_days_since_prev_game` | double | Days since the batter's previous game appearance. |
| `pitcher_days_until_next_game` | integer | Days until the pitcher's next game appearance. |
| `batter_days_until_next_game` | integer | Days until the batter's next game appearance. |
| `api_break_z_with_gravity` | double | Vertical pitch break including gravity (inches). |
| `api_break_x_arm` | double | Horizontal pitch break to the pitcher's arm side (inches). |
| `api_break_x_batter_in` | double | Horizontal pitch break toward/away from the batter (inches). |
| `arm_angle` | double | Pitcher's arm angle at release (degrees). |
| `attack_angle` | double | Angle of the bat's path at contact (degrees). |
| `attack_direction` | double | Horizontal direction of the swing at contact (degrees). |
| `swing_path_tilt` | double | Vertical tilt of the swing path (degrees). |
| `intercept_ball_minus_batter_pos_x_inches` | double | Horizontal offset of ball-bat intercept from batter position (inches). |
| `intercept_ball_minus_batter_pos_y_inches` | double | Depth offset of ball-bat intercept from batter position (inches). |

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

A polars (or pandas) DataFrame, one row per minor-league pitch. A window with no games returns zero rows keeping the documented columns -- MLBAM ids `Int64`, the rest `Null` (no values to infer a dtype from), so the frame widens cleanly into a populated one.

| col_name | type | description |
|---|---|---|
| `pitch_type` | character | Abbreviation of the pitch type thrown (e.g. FF, SL, CH). |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `release_speed` | double | Pitch velocity out of the hand (mph). |
| `release_pos_x` | double | Horizontal release position of the ball, catcher's perspective (feet). |
| `release_pos_z` | double | Vertical release position of the ball, catcher's perspective (feet). |
| `player_name` | character | Player name. |
| `batter` | integer | MLBAM player id of the batter. |
| `pitcher` | integer | Whether the position is a pitcher. |
| `events` | character | Nested list of non-game events. |
| `description` | character | Long-form description text. |
| `spin_dir` | double | Deprecated spin direction field, no longer populated. |
| `spin_rate_deprecated` | double | Deprecated legacy spin-rate field, no longer populated. |
| `break_angle_deprecated` | double | Deprecated legacy break-angle field, no longer populated. |
| `break_length_deprecated` | double | Deprecated legacy break-length field, no longer populated. |
| `zone` | double | Strike-zone region the pitch crossed (1-14 Gameday zone). |
| `des` | character | Full text description of the play. |
| `game_type` | character | Game type code (R, P, etc.). |
| `stand` | character | Side of the plate the batter is standing (L or R). |
| `p_throws` | character | Hand the pitcher throws with (L or R). |
| `home_team` | character | Home team name. |
| `away_team` | character | Away team name. |
| `type` | character | Record type / category. |
| `hit_location` | double | Fielder position number that fielded the ball. |
| `bb_type` | character | Batted-ball type (ground_ball, line_drive, fly_ball, popup). |
| `balls` | integer | Ball count before the pitch. |
| `strikes` | integer | Strike count before the pitch. |
| `game_year` | integer | Season year of the game. |
| `pfx_x` | double | Horizontal pitch movement from the catcher's perspective (feet). |
| `pfx_z` | double | Vertical pitch movement from the catcher's perspective (feet). |
| `plate_x` | double | Horizontal position of the pitch crossing the plate (feet from center). |
| `plate_z` | double | Vertical position of the pitch crossing the plate (feet above ground). |
| `on_3b` | integer | MLBAM ID of the runner on third base, if any. |
| `on_2b` | integer | MLBAM ID of the runner on second base, if any. |
| `on_1b` | integer | MLBAM ID of the runner on first base, if any. |
| `outs_when_up` | integer | Number of outs when the batter came to the plate. |
| `inning` | integer | Inning number. |
| `inning_topbot` | character | Half of the inning (Top or Bot). |
| `hc_x` | double | Hit coordinate X on the field diagram. |
| `hc_y` | double | Hit coordinate Y on the field diagram. |
| `tfs_deprecated` | double | Deprecated time-from-start field, no longer populated. |
| `tfs_zulu_deprecated` | double | Deprecated Zulu time-from-start field, no longer populated. |
| `umpire` | double | Deprecated umpire field, no longer populated. |
| `sv_id` | double | Deprecated Sportvision/Statcast pitch identifier, no longer populated. |
| `vx0` | double | Velocity of the pitch in the x-direction at y=50 ft (ft/s). |
| `vy0` | double | Velocity of the pitch in the y-direction at y=50 ft (ft/s). |
| `vz0` | double | Velocity of the pitch in the z-direction at y=50 ft (ft/s). |
| `ax` | double | Acceleration of the pitch in the x-direction at y=50 ft (ft/s^2). |
| `ay` | double | Acceleration of the pitch in the y-direction at y=50 ft (ft/s^2). |
| `az` | double | Acceleration of the pitch in the z-direction at y=50 ft (ft/s^2). |
| `sz_top` | double | Top of the batter's strike zone for the pitch (feet). |
| `sz_bot` | double | Bottom of the batter's strike zone for the pitch (feet). |
| `hit_distance_sc` | double | Statcast-measured projected distance of the batted ball (feet). |
| `launch_speed` | double | Exit velocity of the batted ball (mph). |
| `launch_angle` | double | Vertical launch angle of the batted ball (degrees). |
| `effective_speed` | double | Perceived velocity adjusted for release extension (mph). |
| `release_spin_rate` | double | Spin rate of the pitch at release (rpm). |
| `release_extension` | double | Distance toward the plate at release (feet). |
| `game_pk` | integer | Unique game identifier. |
| `fielder_2` | integer | MLBAM ID of the catcher. |
| `fielder_3` | integer | MLBAM ID of the first baseman. |
| `fielder_4` | integer | MLBAM ID of the second baseman. |
| `fielder_5` | integer | MLBAM ID of the third baseman. |
| `fielder_6` | integer | MLBAM ID of the shortstop. |
| `fielder_7` | integer | MLBAM ID of the left fielder. |
| `fielder_8` | integer | MLBAM ID of the center fielder. |
| `fielder_9` | integer | MLBAM ID of the right fielder. |
| `release_pos_y` | double | Release position of the ball toward the plate (feet). |
| `estimated_ba_using_speedangle` | double | Expected batting average based on exit velocity and launch angle. |
| `estimated_woba_using_speedangle` | double | Expected wOBA based on exit velocity and launch angle. |
| `woba_value` | double | wOBA value assigned to the event. |
| `woba_denom` | double | wOBA denominator (plate-appearance weight) for the event. |
| `babip_value` | double | BABIP value assigned to the event (0 or 1). |
| `iso_value` | double | Isolated power value assigned to the event. |
| `launch_speed_angle` | double | Batted-ball classification code (1-6) from exit velocity and angle. |
| `at_bat_number` | integer | Sequential plate-appearance number within the game. |
| `pitch_number` | integer | Pitch number within the plate appearance. |
| `pitch_name` | character | Full name of the pitch type (e.g. 4-Seam Fastball, Slider). |
| `home_score` | integer | Home team run total after the play. |
| `away_score` | integer | Away team run total after the play. |
| `bat_score` | integer | Batting team score before the pitch. |
| `fld_score` | integer | Fielding team score before the pitch. |
| `post_away_score` | integer | Away team score after the pitch. |
| `post_home_score` | integer | Home team score after the pitch. |
| `post_bat_score` | integer | Batting team score after the pitch. |
| `post_fld_score` | integer | Fielding team score after the pitch. |
| `if_fielding_alignment` | character | Infield defensive alignment (Standard, Strategic, Infield shift). |
| `of_fielding_alignment` | character | Outfield defensive alignment (Standard, Strategic, 4th outfielder). |
| `spin_axis` | double | Spin axis of the pitch as a clock-face angle (degrees). |
| `delta_home_win_exp` | double | Change in home team win expectancy on the play. |
| `delta_run_exp` | double | Change in run expectancy on the play. |
| `bat_speed` | double | Bat speed at the point of contact (mph). |
| `swing_length` | double | Length of the swing path to contact (feet). |
| `miss_distance` | double | Distance between the bat and the ball on a swing-and-miss, in inches (Savant bat-tracking field); null on pitches without a tracked swing-and-miss (about 90% of sampled rows). |
| `estimated_slg_using_speedangle` | double | Expected slugging based on exit velocity and launch angle. |
| `delta_pitcher_run_exp` | double | Change in run expectancy credited to the pitcher. |
| `hyper_speed` | double | Adjusted (90th-percentile) exit velocity (mph). |
| `home_score_diff` | integer | Home team score minus away team score before the pitch. |
| `bat_score_diff` | integer | Batting team score minus fielding team score before the pitch. |
| `home_win_exp` | double | Home team win expectancy before the play. |
| `bat_win_exp` | double | Batting team win expectancy before the play. |
| `age_pit_legacy` | integer | Pitcher age using the legacy calculation. |
| `age_bat_legacy` | integer | Batter age using the legacy calculation. |
| `age_pit` | integer | Pitcher age for the season. |
| `age_bat` | integer | Batter age for the season. |
| `n_thruorder_pitcher` | integer | Times through the order the pitcher is facing the lineup. |
| `n_priorpa_thisgame_player_at_bat` | integer | Number of prior plate appearances by the batter in the game. |
| `pitcher_days_since_prev_game` | double | Days since the pitcher's previous game appearance. |
| `batter_days_since_prev_game` | double | Days since the batter's previous game appearance. |
| `pitcher_days_until_next_game` | double | Days until the pitcher's next game appearance. |
| `batter_days_until_next_game` | double | Days until the batter's next game appearance. |
| `api_break_z_with_gravity` | double | Vertical pitch break including gravity (inches). |
| `api_break_x_arm` | double | Horizontal pitch break to the pitcher's arm side (inches). |
| `api_break_x_batter_in` | double | Horizontal pitch break toward/away from the batter (inches). |
| `arm_angle` | double | Pitcher's arm angle at release (degrees). |
| `attack_angle` | double | Angle of the bat's path at contact (degrees). |
| `attack_direction` | double | Horizontal direction of the swing at contact (degrees). |
| `swing_path_tilt` | double | Vertical tilt of the swing path (degrees). |
| `intercept_ball_minus_batter_pos_x_inches` | double | Horizontal offset of ball-bat intercept from batter position (inches). |
| `intercept_ball_minus_batter_pos_y_inches` | double | Depth offset of ball-bat intercept from batter position (inches). |

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

A polars (or pandas) DataFrame, one row per WBC pitch. A window with no games returns zero rows keeping the documented columns -- MLBAM ids `Int64`, the rest `Null` (no values to infer a dtype from), so the frame widens cleanly into a populated one.

| col_name | type | description |
|---|---|---|
| `pitch_type` | character | Abbreviation of the pitch type thrown (e.g. FF, SL, CH). |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `release_speed` | double | Pitch velocity out of the hand (mph). |
| `release_pos_x` | double | Horizontal release position of the ball, catcher's perspective (feet). |
| `release_pos_z` | double | Vertical release position of the ball, catcher's perspective (feet). |
| `player_name` | character | Player name. |
| `batter` | integer | MLBAM player id of the batter. |
| `pitcher` | integer | Whether the position is a pitcher. |
| `events` | character | Nested list of non-game events. |
| `description` | character | Long-form description text. |
| `spin_dir` | double | Deprecated spin direction field, no longer populated. |
| `spin_rate_deprecated` | double | Deprecated legacy spin-rate field, no longer populated. |
| `break_angle_deprecated` | double | Deprecated legacy break-angle field, no longer populated. |
| `break_length_deprecated` | double | Deprecated legacy break-length field, no longer populated. |
| `zone` | double | Strike-zone region the pitch crossed (1-14 Gameday zone). |
| `des` | character | Full text description of the play. |
| `game_type` | character | Game type code (R, P, etc.). |
| `stand` | character | Side of the plate the batter is standing (L or R). |
| `p_throws` | character | Hand the pitcher throws with (L or R). |
| `home_team` | character | Home team name. |
| `away_team` | character | Away team name. |
| `type` | character | Record type / category. |
| `hit_location` | double | Fielder position number that fielded the ball. |
| `bb_type` | character | Batted-ball type (ground_ball, line_drive, fly_ball, popup). |
| `balls` | integer | Ball count before the pitch. |
| `strikes` | integer | Strike count before the pitch. |
| `game_year` | integer | Season year of the game. |
| `pfx_x` | double | Horizontal pitch movement from the catcher's perspective (feet). |
| `pfx_z` | double | Vertical pitch movement from the catcher's perspective (feet). |
| `plate_x` | double | Horizontal position of the pitch crossing the plate (feet from center). |
| `plate_z` | double | Vertical position of the pitch crossing the plate (feet above ground). |
| `on_3b` | integer | MLBAM ID of the runner on third base, if any. |
| `on_2b` | integer | MLBAM ID of the runner on second base, if any. |
| `on_1b` | integer | MLBAM ID of the runner on first base, if any. |
| `outs_when_up` | integer | Number of outs when the batter came to the plate. |
| `inning` | integer | Inning number. |
| `inning_topbot` | character | Half of the inning (Top or Bot). |
| `hc_x` | double | Hit coordinate X on the field diagram. |
| `hc_y` | double | Hit coordinate Y on the field diagram. |
| `tfs_deprecated` | double | Deprecated time-from-start field, no longer populated. |
| `tfs_zulu_deprecated` | double | Deprecated Zulu time-from-start field, no longer populated. |
| `umpire` | double | Deprecated umpire field, no longer populated. |
| `sv_id` | double | Deprecated Sportvision/Statcast pitch identifier, no longer populated. |
| `vx0` | double | Velocity of the pitch in the x-direction at y=50 ft (ft/s). |
| `vy0` | double | Velocity of the pitch in the y-direction at y=50 ft (ft/s). |
| `vz0` | double | Velocity of the pitch in the z-direction at y=50 ft (ft/s). |
| `ax` | double | Acceleration of the pitch in the x-direction at y=50 ft (ft/s^2). |
| `ay` | double | Acceleration of the pitch in the y-direction at y=50 ft (ft/s^2). |
| `az` | double | Acceleration of the pitch in the z-direction at y=50 ft (ft/s^2). |
| `sz_top` | double | Top of the batter's strike zone for the pitch (feet). |
| `sz_bot` | double | Bottom of the batter's strike zone for the pitch (feet). |
| `hit_distance_sc` | double | Statcast-measured projected distance of the batted ball (feet). |
| `launch_speed` | double | Exit velocity of the batted ball (mph). |
| `launch_angle` | double | Vertical launch angle of the batted ball (degrees). |
| `effective_speed` | double | Perceived velocity adjusted for release extension (mph). |
| `release_spin_rate` | double | Spin rate of the pitch at release (rpm). |
| `release_extension` | double | Distance toward the plate at release (feet). |
| `game_pk` | integer | Unique game identifier. |
| `fielder_2` | integer | MLBAM ID of the catcher. |
| `fielder_3` | integer | MLBAM ID of the first baseman. |
| `fielder_4` | integer | MLBAM ID of the second baseman. |
| `fielder_5` | integer | MLBAM ID of the third baseman. |
| `fielder_6` | integer | MLBAM ID of the shortstop. |
| `fielder_7` | integer | MLBAM ID of the left fielder. |
| `fielder_8` | integer | MLBAM ID of the center fielder. |
| `fielder_9` | integer | MLBAM ID of the right fielder. |
| `release_pos_y` | double | Release position of the ball toward the plate (feet). |
| `estimated_ba_using_speedangle` | double | Expected batting average based on exit velocity and launch angle. |
| `estimated_woba_using_speedangle` | double | Expected wOBA based on exit velocity and launch angle. |
| `woba_value` | double | wOBA value assigned to the event. |
| `woba_denom` | double | wOBA denominator (plate-appearance weight) for the event. |
| `babip_value` | double | BABIP value assigned to the event (0 or 1). |
| `iso_value` | double | Isolated power value assigned to the event. |
| `launch_speed_angle` | double | Batted-ball classification code (1-6) from exit velocity and angle. |
| `at_bat_number` | integer | Sequential plate-appearance number within the game. |
| `pitch_number` | integer | Pitch number within the plate appearance. |
| `pitch_name` | character | Full name of the pitch type (e.g. 4-Seam Fastball, Slider). |
| `home_score` | integer | Home team run total after the play. |
| `away_score` | integer | Away team run total after the play. |
| `bat_score` | integer | Batting team score before the pitch. |
| `fld_score` | integer | Fielding team score before the pitch. |
| `post_away_score` | integer | Away team score after the pitch. |
| `post_home_score` | integer | Home team score after the pitch. |
| `post_bat_score` | integer | Batting team score after the pitch. |
| `post_fld_score` | integer | Fielding team score after the pitch. |
| `if_fielding_alignment` | character | Infield defensive alignment (Standard, Strategic, Infield shift). |
| `of_fielding_alignment` | character | Outfield defensive alignment (Standard, Strategic, 4th outfielder). |
| `spin_axis` | double | Spin axis of the pitch as a clock-face angle (degrees). |
| `delta_home_win_exp` | double | Change in home team win expectancy on the play. |
| `delta_run_exp` | double | Change in run expectancy on the play. |
| `bat_speed` | double | Bat speed at the point of contact (mph). |
| `swing_length` | double | Length of the swing path to contact (feet). |
| `miss_distance` | double | Distance between the bat and the ball on a swing-and-miss, in inches (Savant bat-tracking field); all null in the sampled WBC pitches (2023-03-08), and coverage for later WBC tournaments is unverified. |
| `estimated_slg_using_speedangle` | double | Expected slugging based on exit velocity and launch angle. |
| `delta_pitcher_run_exp` | double | Change in run expectancy credited to the pitcher. |
| `hyper_speed` | double | Adjusted (90th-percentile) exit velocity (mph). |
| `home_score_diff` | integer | Home team score minus away team score before the pitch. |
| `bat_score_diff` | integer | Batting team score minus fielding team score before the pitch. |
| `home_win_exp` | double | Home team win expectancy before the play. |
| `bat_win_exp` | double | Batting team win expectancy before the play. |
| `age_pit_legacy` | integer | Pitcher age using the legacy calculation. |
| `age_bat_legacy` | integer | Batter age using the legacy calculation. |
| `age_pit` | integer | Pitcher age for the season. |
| `age_bat` | integer | Batter age for the season. |
| `n_thruorder_pitcher` | integer | Times through the order the pitcher is facing the lineup. |
| `n_priorpa_thisgame_player_at_bat` | integer | Number of prior plate appearances by the batter in the game. |
| `pitcher_days_since_prev_game` | double | Days since the pitcher's previous game appearance. |
| `batter_days_since_prev_game` | double | Days since the batter's previous game appearance. |
| `pitcher_days_until_next_game` | double | Days until the pitcher's next game appearance. |
| `batter_days_until_next_game` | double | Days until the batter's next game appearance. |
| `api_break_z_with_gravity` | double | Vertical pitch break including gravity (inches). |
| `api_break_x_arm` | double | Horizontal pitch break to the pitcher's arm side (inches). |
| `api_break_x_batter_in` | double | Horizontal pitch break toward/away from the batter (inches). |
| `arm_angle` | double | Pitcher's arm angle at release (degrees). |
| `attack_angle` | double | Angle of the bat's path at contact (degrees). |
| `attack_direction` | double | Horizontal direction of the swing at contact (degrees). |
| `swing_path_tilt` | double | Vertical tilt of the swing path (degrees). |
| `intercept_ball_minus_batter_pos_x_inches` | double | Horizontal offset of ball-bat intercept from batter position (inches). |
| `intercept_ball_minus_batter_pos_y_inches` | double | Depth offset of ball-bat intercept from batter position (inches). |

**Example**

```python
from sportsdataverse.mlb import mlb_statcast_search_wbc
df = mlb_statcast_search_wbc("2023-03-08", "2023-03-22")
```
