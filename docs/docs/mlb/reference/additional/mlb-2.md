---
title: "MLB — additional Python functions — Mlb (2)"
sidebar_label: "Mlb (2)"
sidebar_position: 3
description: "MLB — additional Python functions — Mlb (2) — function reference in sdv-py, the SportsDataverse Python package."
---
# MLB — additional Python functions — Mlb (2)

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
| `batter_days_since_prev_game` | integer | Days since the batter's previous game appearance. |
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

### mlb_stats {#mlb_stats}

`mlb_stats(stats: 'str', group: 'str', season: 'Optional[Union[int, str]]' = None, sport_id: 'int' = 1, league_id: 'Optional[Union[int, str]]' = None, team_id: 'Optional[int]' = None, player_pool: 'Optional[str]' = None, game_type: 'Optional[str]' = None, limit: 'int' = 50, offset: 'int' = 0, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/stats — generic stats query.

`stats` selects the slice (`season`, `career`, `yearByYear`, …) and
`group` selects the stat group (`hitting`, `pitching`, `fielding`).
Filters: `season`, `team_id`, `league_id`, `game_type`, `player_pool`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stats` | `str` |  |  |
| `group` | `str` |  |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `sport_id` | `int` | `1` |  |
| `league_id` | `Optional[Union[int, str]]` | `None` |  |
| `team_id` | `Optional[int]` | `None` |  |
| `player_pool` | `Optional[str]` | `None` |  |
| `game_type` | `Optional[str]` | `None` |  |
| `limit` | `int` | `50` |  |
| `offset` | `int` | `0` |  |
| `fields` | `Optional[str]` | `None` |  |

### mlb_stats_leaders {#mlb_stats_leaders}

`mlb_stats_leaders(leader_categories: 'str', season: 'Optional[Union[int, str]]' = None, leader_game_types: 'Optional[str]' = None, stat_group: 'Optional[str]' = None, league_id: 'Optional[Union[int, str]]' = None, sport_id: 'int' = 1, limit: 'int' = 10, **kwargs) -> 'Dict'`

GET /api/v1/stats/leaders — top-N leaders for a stat category.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `leader_categories` | `str` |  |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `leader_game_types` | `Optional[str]` | `None` |  |
| `stat_group` | `Optional[str]` | `None` |  |
| `league_id` | `Optional[Union[int, str]]` | `None` |  |
| `sport_id` | `int` | `1` |  |
| `limit` | `int` | `10` |  |

### mlb_stats_streaks {#mlb_stats_streaks}

`mlb_stats_streaks(streak_type: 'str', streak_threshold: 'int' = 1, season: 'Optional[Union[int, str]]' = None, stat_group: 'Optional[str]' = None, active_streak: 'Optional[bool]' = None, sport_id: 'int' = 1, **kwargs) -> 'Dict'`

GET /api/v1/stats/streaks — active or historical streaks.

`streak_type` e.g. `hittingStreakOverall`, `onBaseOverall`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `streak_type` | `str` |  |  |
| `streak_threshold` | `int` | `1` |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `stat_group` | `Optional[str]` | `None` |  |
| `active_streak` | `Optional[bool]` | `None` |  |
| `sport_id` | `int` | `1` |  |

### mlb_stolen_base_value {#mlb_stolen_base_value}

`mlb_stolen_base_value(sb_attempts: "'pl.DataFrame'", sprint_speed: "'pl.DataFrame'", poptime: "'pl.DataFrame'", *, speed_bin: 'float' = 0.5, pop_bin: 'float' = 0.05, pop_col: 'str' = 'pop_2b_sba', alpha: 'float' = 2.0, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-runner stolen-base run value: realized-vs-expected run contribution.

`sb_run_value = sum(p_success * RUN_VALUES["sb"] + (1 - p_success) *
RUN_VALUES["cs"])` per attempt -- the documented fallback constants
(see module docstring for why, not
`sportsdataverse.mlb.mlb_run_values.event_run_value` on these
bundled-`des` rows), weighted by the surface's modeled success
probability for that attempt's bin.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sb_attempts` | `DataFrame` |  | One row per attempt (see `sb_attempts_from_pitches`). MiLB feeds run through the same function -- there is no Savant basestealing leaderboard oracle for MiLB. |
| `sprint_speed` | `DataFrame` |  | Sprint-speed leaderboard frame (`runner_id`, `sprint_speed`). |
| `poptime` | `DataFrame` |  | Pop-time leaderboard frame (`catcher_id`, `pop_col`). |
| `speed_bin` | `float` | `0.5` | Sprint-speed bin width. Defaults to `0.5`. |
| `pop_bin` | `float` | `0.05` | Pop-time bin width. Defaults to `0.05`. |
| `pop_col` | `str` | `'pop_2b_sba'` | Pop-time column name in `poptime`. Defaults to `"pop_2b_sba"`. |
| `alpha` | `float` | `2.0` | Laplace smoothing strength for the surface. Defaults to `2.0`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

one row per runner. | Column | Type | Description | |---|---|---| | runner_id | Utf8 | Runner MLBAM id | | attempts | Int64 | Stolen-base attempts | | p_success_mean | Float64 | Mean modeled success probability across attempts | | sb_run_value | Float64 | Sum of realized-vs-expected run contribution |

**Example**

```python
from sportsdataverse.mlb.mlb_stolen_base import mlb_stolen_base_value
sb_value = mlb_stolen_base_value(sb_attempts, sprint_speed, poptime)
```

### mlb_stuff_plus {#mlb_stuff_plus}

`mlb_stuff_plus(pitches: 'pl.DataFrame', *, level: 'str' = 'pitch', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Score pitches with the bundled Stuff+ (①) run-value model.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Output of `sportsdataverse.mlb.mlb_pitch_features.pitch_features` (needs `velo_z`, `spin_z`, `pfx_x_z`, `pfx_z_z`, `release_pos_x_z`, `release_pos_z_z`, `extension_z`). |
| `level` | `str` | `'pitch'` | `"pitch"` (default) for per-pitch output, or `"arsenal"` for a per `(pitcher, pitch_type)` mean. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`pitcher`, `pitch_type`, `stuff_rv_hat`, `stuff_plus` — one row per pitch (`level="pitch"`) or per pitcher-pitchtype (`level="arsenal"`). Empty input returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | integer | MLB Advanced Media (MLBAM) id for the pitcher. |
| `pitch_type` | character | Statcast pitch-type abbreviation. |
| `stuff_rv_hat` | double | Predicted per-pitch run value from the bundled Stuff+ xgboost model (physics + fastball-relative features only). |
| `stuff_plus` | double | Plus-scale Stuff+ score, 100 = league average, higher = better (sign-inverted from stuff_rv_hat). |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_features import pitch_features
from sportsdataverse.mlb.mlb_stuff_plus import mlb_stuff_plus
feats = pitch_features(raw_pitches)
out = mlb_stuff_plus(feats, level="arsenal")
print(out.sort("stuff_plus", descending=True).head())

# Pipeline next step

out.filter(pl.col("pitch_type") == "FF").sort("stuff_plus", descending=True)
```

### mlb_swing_decision {#mlb_swing_decision}

`mlb_swing_decision(start_dt: 'str', end_dt: 'str', *, puller: 'Optional[Callable[..., pl.DataFrame]]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Per player-season swing/take run value + selective-aggression (SEAGER analog).

Pulls pitches via `puller(start_dt, end_dt, player_type="batter")`,
builds the RV(swing)/RV(take) zone x count surfaces (and the league
swing-rate table) from the pull itself, then per batter:

* `swing_take_runs` = sum of the **actual** per-pitch `delta_run_exp`
  credited to the batter's swing/take decisions (matching Savant's
  swing/take run-value definition -- the run value of what actually
  happened on each pitch, not a league-average lookup).
* `selective_agg` = sum of `rv_chosen - rv_neutral`, where
  `rv_neutral = swing_rate * rv_swing + (1 - swing_rate) * rv_take` uses
  the **league** swing rate for that zone x count cell -- positive means
  the batter swings at hittable pitches and takes bad ones more than a
  league-average decision-maker would.
* `chase_rate` = swings / pitches seen in the waste/chase zones
  (`zone in {11,12,13,14}`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `start_dt` | `str` |  | Pull start date, `YYYY-MM-DD`. |
| `end_dt` | `str` |  | Pull end date, `YYYY-MM-DD`. |
| `puller` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable Statcast search callable -- defaults to `sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (`batter`, `season`): `pitches`, `swing_take_runs`, `selective_agg`, `chase_rate`, `n_swings`. Empty pull returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `batter` | integer | MLBAM batter id (join key into Savant's swing/take leaderboard as `player_id`). |
| `season` | integer | Four-digit season year derived from `game_year`/`game_date`. |
| `pitches` | integer | Total pitches seen with a non-null zone/decision in the pulled window. |
| `swing_take_runs` | double | Sum of the run value of the batter's actual swing/take decisions (delta_run_exp of the chosen decision at that zone x count). |
| `selective_agg` | double | SEAGER-analog selective-aggression score -- sum of (chosen run value minus the league-neutral-rate run value) per pitch. |
| `chase_rate` | double | Share of pitches in the waste/chase attack zones (11-14) that the batter swung at. |
| `n_swings` | integer | Count of pitches the batter swung at. |

**Example**

```python
from sportsdataverse.mlb.mlb_swing_decision import mlb_swing_decision

df = mlb_swing_decision("2024-06-01", "2024-06-21")
print(df.shape)

# Pipeline next step (one line)

df.sort("selective_agg", descending=True).head()
```

### mlb_team_elo {#mlb_team_elo}

`mlb_team_elo(results: 'pl.DataFrame', *, k: 'float' = 4.0, hfa: 'float' = 24.0, init: 'float' = 1500.0, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

As-of-date iterative Elo run-differential rating.

Games are folded **in date order** (ties broken by `game_id`); each
team's rating updates only *after* its game is scored, so the
`home_rating`/`away_rating` columns are strictly as-of-date (no
leakage from later games). `home_win_prob_elo` uses the standard
logistic Elo formula with a home-field-advantage offset.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `results` | `DataFrame` |  | Game-level results (`game_id`, `date`, `home_team_id`, `away_team_id`, `home_score`, `away_score`). |
| `k` | `float` | `4.0` | Elo K-factor (rating-update step size). |
| `hfa` | `float` | `24.0` | Home-field-advantage Elo-point offset. |
| `init` | `float` | `1500.0` | Initial rating for a team with no prior games. |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per game, in date order. | Column | Type | Description | |---|---|---| | game_id | Utf8 | Game identifier | | date | Date | Game date | | home_team_id | Utf8 | Home team identifier | | away_team_id | Utf8 | Away team identifier | | home_rating | Float64 | Home team's rating **before** this game | | away_rating | Float64 | Away team's rating **before** this game | | home_win_prob_elo | Float64 | Elo-implied P(home wins) before this game | | home_rating_post | Float64 | Home team's rating **after** this game | | away_rating_post | Float64 | Away team's rating **after** this game |

| col_name | type | description |
|---|---|---|
| `game_id` | character | Game identifier (statsapi gamePk, stringified). |
| `date` | date | Calendar date of the game (YYYY-MM-DD). |
| `home_team_id` | character | Home team identifier. |
| `away_team_id` | character | Away team identifier. |
| `home_rating` | double | Home team's Elo rating before this game (as-of-date). |
| `away_rating` | double | Away team's Elo rating before this game (as-of-date). |
| `home_win_prob_elo` | double | Elo-implied P(home team wins) before this game. |
| `home_rating_post` | double | Home team's Elo rating after this game. |
| `away_rating_post` | double | Away team's Elo rating after this game. |

**Example**

```python
from sportsdataverse.mlb.mlb_team_projection import mlb_team_elo
elo = mlb_team_elo(results)

# Pipeline next step (one line)

elo.group_by("home_team_id").agg(pl.col("home_rating_post").last())
```

### mlb_team_leaders {#mlb_team_leaders}

`mlb_team_leaders(team_id: 'int', leader_categories: 'str', season: 'Optional[Union[int, str]]' = None, leader_game_types: 'Optional[str]' = None, limit: 'int' = 10, **kwargs) -> 'Dict'`

GET /api/v1/teams/{teamId}/leaders — team leaders.

`leader_categories` e.g. `homeRuns`, `battingAverage`, `wins`,
`earnedRunAverage` (comma-separated for multi).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  |  |
| `leader_categories` | `str` |  |  |
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `leader_game_types` | `Optional[str]` | `None` |  |
| `limit` | `int` | `10` |  |

### mlb_team_projection {#mlb_team_projection}

`mlb_team_projection(seasons: 'Union[int, List[int], None]' = None, *, results: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Combined pythagenpat + Elo team projection.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, List[int], None]` | `None` | Reserved for a future network-collector path (currently unused -- pass `results` directly; see `sportsdataverse.mlb.mlb_run_expectancy.mlb_run_expectancy_matrix` for the collector pattern this will follow once wired). |
| `results` | `Optional[DataFrame]` | `None` | Game-level results (see `mlb_pythagenpat_table` and `mlb_team_elo` for the required columns). |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per (season, team). | Column | Type | Description | |---|---|---| | season | Int64 | Season | | team_id | Utf8 | Team identifier | | win_pct | Float64 | Realized win percentage | | pythag_win_pct | Float64 | Pythagenpat expected win percentage | | rating | Float64 | Final (as of the last observed game) Elo rating | | exp_margin | Float64 | Elo-implied expected run margin vs a league-average opponent |

| col_name | type | description |
|---|---|---|
| `season` | integer | MLB season (4-digit start year). |
| `team_id` | character | Team identifier (statsapi team id, stringified). |
| `win_pct` | double | Realized win percentage. |
| `pythag_win_pct` | double | Pythagenpat expected win percentage. |
| `rating` | double | Final (as of the last observed game) Elo rating. |
| `exp_margin` | double | Elo-implied expected run margin vs a league-average opponent. |

**Example**

```python
from sportsdataverse.mlb.mlb_team_projection import mlb_team_projection
projection = mlb_team_projection(results=results)
```

### mlb_team_stats {#mlb_team_stats}

`mlb_team_stats(team_id: 'int', season: 'Union[int, str]', stats: 'str' = 'season', group: 'str' = 'hitting', sport_ids: 'Optional[Union[int, List[int]]]' = None, game_type: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/teams/{teamId}/stats — team-level stats.

`stats`: `season`, `career`, `yearByYear`, `byMonth`, `byDayOfWeek`, …
`group`: `hitting`, `pitching`, `fielding`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `int` |  |  |
| `season` | `Union[int, str]` |  |  |
| `stats` | `str` | `'season'` |  |
| `group` | `str` | `'hitting'` |  |
| `sport_ids` | `Optional[Union[int, List[int]]]` | `None` |  |
| `game_type` | `Optional[str]` | `None` |  |
| `fields` | `Optional[str]` | `None` |  |

### mlb_teams {#mlb_teams}

`mlb_teams(season: 'Optional[Union[int, str]]' = None, sport_id: 'int' = 1, league_ids: 'Optional[Union[int, List[int], str]]' = None, active_status: 'Optional[str]' = None, all_star_statuses: 'Optional[str]' = None, hydrate: 'Optional[str]' = None, fields: 'Optional[str]' = None, **kwargs) -> 'Dict'`

GET /api/v1/teams — list teams. `sport_id=1` = MLB.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[Union[int, str]]` | `None` |  |
| `sport_id` | `int` | `1` |  |
| `league_ids` | `Optional[Union[int, List[int], str]]` | `None` |  |
| `active_status` | `Optional[str]` | `None` |  |
| `all_star_statuses` | `Optional[str]` | `None` |  |
| `hydrate` | `Optional[str]` | `None` |  |
| `fields` | `Optional[str]` | `None` |  |

### mlb_times_through_order {#mlb_times_through_order}

`mlb_times_through_order(pitches: 'pl.DataFrame', *, season: 'int' = 2024, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-pitch fitted times-through-order fatigue adjustment.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Output of `sportsdataverse.mlb.mlb_pitch_features.add_sequence_features` (needs `times_through_order`). |
| `season` | `int` | `2024` | Season year, selects the fitted `tto_penalty` coefficients via `sportsdataverse.mlb.mlb_pitching_constants.get_baselines`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

`pitcher`, `game_pk`, `at_bat_number`, `pitch_number`, `times_through_order`, `fatigue_rv_adj` (the fitted marginal penalty for that TTO level). Empty input returns a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `pitcher` | integer | MLB Advanced Media (MLBAM) id for the pitcher. |
| `game_pk` | integer | Game identifier. |
| `at_bat_number` | integer | Game-level plate-appearance sequence number. |
| `pitch_number` | integer | Pitch sequence number within the plate appearance. |
| `times_through_order` | integer | Times through the batting order (1-3). |
| `fatigue_rv_adj` | double | Fitted per-TTO run-value marginal (mlb_pitching_constants.tto_penalty) for this pitch's TTO level. |

**Example**

```python
from sportsdataverse.mlb.mlb_pitch_features import pitch_features, add_sequence_features
from sportsdataverse.mlb.mlb_pitch_fatigue import mlb_times_through_order
feats = add_sequence_features(pitch_features(raw_pitches))
out = mlb_times_through_order(feats, season=2024)
print(out.select("times_through_order", "fatigue_rv_adj").unique())
```

### mlb_umpire_bias {#mlb_umpire_bias}

`mlb_umpire_bias(pitches: 'pl.DataFrame', *, model: 'Optional[Dict[str, Any]]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Per-umpire called-strike bias residual (observed minus expected).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Called pitches with `umpire_id`, `description`, `plate_x`, `plate_z`, `sz_top`, `sz_bot`. |
| `model` | `Optional[Dict[str, Any]]` | `None` | Pre-fit model dict from `fit_zone_model`; fits on `pitches` itself when `None`. |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per umpire. | Column | Type | Description | |---|---|---| | umpire_id | Utf8 | Umpire identifier | | n_called | Int64 | Called pitches observed for this umpire | | obs_strike_rate | Float64 | Realized called-strike rate | | exp_strike_rate | Float64 | Mean model-predicted called-strike probability | | bias | Float64 | obs_strike_rate - exp_strike_rate (positive = strike-generous) |

| col_name | type | description |
|---|---|---|
| `umpire_id` | character | Umpire identifier (statsapi people id, stringified). |
| `n_called` | integer | Called pitches (strike or ball) observed for this umpire. |
| `obs_strike_rate` | double | Realized called-strike rate for this umpire. |
| `exp_strike_rate` | double | Mean model-predicted called-strike probability for this umpire's pitches. |
| `bias` | double | obs_strike_rate minus exp_strike_rate (positive = strike-generous). |

**Example**

```python
from sportsdataverse.mlb.mlb_umpire_zone import mlb_umpire_bias
bias = mlb_umpire_bias(pitches)
```

### mlb_umpire_called_strike_prob {#mlb_umpire_called_strike_prob}

`mlb_umpire_called_strike_prob(pitches: 'pl.DataFrame', *, model: 'Optional[Dict[str, Any]]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

P(called strike) per pitch from the zone logistic.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pitches` | `DataFrame` |  | Frame with `plate_x`, `plate_z`, `sz_top`, `sz_bot` (one row per pitch, not required to be called pitches only). |
| `model` | `Optional[Dict[str, Any]]` | `None` | Pre-fit model dict from `fit_zone_model`; fits on `pitches` itself when `None` (using only its called pitches). |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per input pitch. | Column | Type | Description | |---|---|---| | called_strike_prob | Float64 | P(called strike \| pitch location) |

| col_name | type | description |
|---|---|---|
| `called_strike_prob` | double | P(called strike \| pitch location) from the standardized zone-coordinate logistic. |

**Example**

```python
from sportsdataverse.mlb.mlb_umpire_zone import mlb_umpire_called_strike_prob
prob = mlb_umpire_called_strike_prob(pitches)
```

### mlb_win_expectancy {#mlb_win_expectancy}

`mlb_win_expectancy(pbp: 'pl.DataFrame', results: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Per-play home win expectancy from the empirical state table.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Parsed `mlb_play_by_play` frame (see `sportsdataverse.mlb.mlb_run_expectancy.pbp_base_out_states`). |
| `results` | `DataFrame` |  | Game-level results (`game_id`, `home_score`, `away_score`). |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per plate appearance, **plus one terminal "game over" row per game** (`at_bat_index` = last real PA's index + 1, `home_win_exp` pinned to the actual final outcome: 1.0 if home won, 0.0 otherwise). Without this anchor, the last real play's own WPA swing (e.g. a walk-off) would never be captured by `mlb_win_probability_added`'s per-game diff, and the game-level WPA sum would not telescope to the exact +-0.5 identity. | Column | Type | Description | |---|---|---| | game_id | Utf8 | Game identifier | | at_bat_index | Int64 | Game-global sequential PA index (last row is a synthetic terminal marker) | | half | Utf8 | `"top"` or `"bottom"` (offense side); the terminal row repeats the last real half | | home_win_exp | Float64 | P(home team wins \| state before the play); 1.0/0.0 on the terminal row |

| col_name | type | description |
|---|---|---|
| `game_id` | character | Game identifier (statsapi gamePk, stringified). |
| `at_bat_index` | integer | Game-global sequential plate-appearance index. |
| `half` | character | Half-inning ("top" or "bottom") -- which side is on offense. |
| `home_win_exp` | double | Empirical P(home team wins \| base-out-score-inning state before the play). |

**Example**

```python
from sportsdataverse.mlb.mlb_win_expectancy import mlb_win_expectancy
we = mlb_win_expectancy(pbp, results)

# Pipeline next step (one line)

we.filter(pl.col("game_id") == "716390").sort("at_bat_index")
```

### mlb_win_probability_added {#mlb_win_probability_added}

`mlb_win_probability_added(we: 'pl.DataFrame', *, perspective: 'str' = 'home', return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Per-play win-probability-added from a `mlb_win_expectancy` frame.

`wpa_i = home_win_exp_i - home_win_exp_{i-1}` within each game (the
first play of a game is measured against the neutral 0.5 baseline).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `we` | `DataFrame` |  | Output of `mlb_win_expectancy` (needs `game_id`, `at_bat_index`, `home_win_exp`). |
| `perspective` | `str` | `'home'` | `"home"` (default) returns home-team WPA; any other value (e.g. `"away"`) returns the sign-flipped (away-team) WPA. |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

one row per plate appearance. | Column | Type | Description | |---|---|---| | game_id | Utf8 | Game identifier | | at_bat_index | Int64 | Game-global sequential PA index | | wpa | Float64 | Win-probability added, from `perspective` |

| col_name | type | description |
|---|---|---|
| `game_id` | character | Game identifier (statsapi gamePk, stringified). |
| `at_bat_index` | integer | Game-global sequential plate-appearance index. |
| `wpa` | double | Win-probability added on this play, from the requested perspective. |

**Example**

```python
from sportsdataverse.mlb.mlb_win_expectancy import mlb_win_probability_added
wpa = mlb_win_probability_added(we)
```
