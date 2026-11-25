---
title: "NFL — additional Python functions — Play-by-play processing: build_nfl–espn_nfl"
sidebar_label: "Play-by-play processing: build_nfl–espn_nfl"
sidebar_position: 13
description: "NFL — additional Python functions — Play-by-play processing: build_nfl–espn_nfl — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Play-by-play processing: build_nfl–espn_nfl

### build_nfl_team_stats {#build_nfl_team_stats}

`build_nfl_team_stats(seasons: 'List[int]', *, summary_level: 'str' = 'week', season_type: 'str' = 'REG', source: 'str' = 'sdv', return_as_pandas: 'bool' = False) -> "pl.DataFrame | 'pd.DataFrame'"`

Build nflverse **team_stats** by aggregating SDV-native play-by-play.

A faithful polars port of nflfastR's `calculate_stats(stat_type = "team")`
(the `aggregate_game_stats*` family). Offense is keyed on `posteam`,
defense on the tackler's team (per-play `*_team` slot tags -- NOT
`defteam`, which double-counts on return plays), kicking on `posteam`,
and returns / penalties / timeouts on the relevant play team tag. See the
module docstring for the full grouping + SDV-PBP gap notes (`passing_epa`
uses the exact `qb_epa`; `gwfg_*` derive from `fixed_drive`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | Four-digit NFL seasons to aggregate (e.g. `[2023]`). |
| `summary_level` | `str` | `'week'` | `"week"` (group on season + week + team, with `opponent_team`) or `"season"` (group on season + team, with a `games` distinct-game count replacing week / season_type / opponent_team). |
| `season_type` | `str` | `'REG'` | `"REG"`, `"POST"`, or `"REG+POST"`. Pre-filters the play-by-play before aggregation. |
| `source` | `str` | `'sdv'` | Play-by-play release passed to `load_nfl_pbp`. Defaults to `"sdv"` (the SDV-native enriched release). |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; else polars. |

**Returns**

A polars (or pandas) DataFrame in the published `load_nfl_team_stats` schema (~102 columns). At `summary_level="season"` the `week` / `season_type` / `opponent_team` columns are replaced by a `games` column.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | Int64 | Season week. |
| `team` | String | NFL team. Uses official abbreviations as per NFL.com |
| `season_type` | String | REG or POST indicating if the timeframe belongs to regular or post season. |
| `opponent_team` | String |  |
| `completions` | Int64 | The number of completed passes. |
| `attempts` | Int64 | The number of pass attempts as defined by the NFL. |
| `passing_yards` | Float64 | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `passing_tds` | Int64 | The number of passing touchdowns. |
| `passing_interceptions` | Int64 |  |
| `sacks_suffered` | Int64 |  |
| `sack_yards_lost` | Float64 |  |
| `sack_fumbles` | Int64 | The number of sacks with a fumble. |
| `sack_fumbles_lost` | Int64 | The number of sacks with a lost fumble. |
| `passing_air_yards` | Float64 | Passing air yards (includes incomplete passes). |
| `passing_yards_after_catch` | Float64 | Yards after the catch gained on plays in which player was the passer (this is an unofficial stat and may differ slightly between different sources). |
| `passing_first_downs` | Int64 | First downs on pass attempts. |
| `passing_epa` | Float64 | Total expected points added on pass attempts and sacks. NOTE: this uses the variable `qb_epa`, which gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `passing_cpoe` | Float64 |  |
| `passing_2pt_conversions` | Int64 | Two-point conversion passes. |
| `carries` | Int64 | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushing_yards` | Float64 | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `rushing_tds` | Int64 | The number of rushing touchdowns (incl. scrambles). Also includes touchdowns after obtaining a lateral on a play that started with a rushing attempt. |
| `rushing_fumbles` | Int64 | The number of rushes with a fumble. |
| `rushing_fumbles_lost` | Int64 | The number of rushes with a lost fumble. |
| `rushing_first_downs` | Int64 | First downs on rush attempts (incl. scrambles). |
| `rushing_epa` | Float64 | Expected points added on rush attempts (incl. scrambles and kneel downs). |
| `rushing_2pt_conversions` | Int64 | Two-point conversion rushes |
| `receptions` | Int64 | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | Int64 | The number of pass plays where the player was the targeted receiver. |
| `receiving_yards` | Float64 | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `receiving_tds` | Int64 | The number of touchdowns following a pass reception. Also includes touchdowns after receiving a lateral on a play that started as a pass play. |
| `receiving_fumbles` | Int64 | The number of fumbles after a pass reception. |
| `receiving_fumbles_lost` | Int64 | The number of fumbles lost after a pass reception. |
| `receiving_air_yards` | Float64 | Receiving air yards (incl. incomplete passes). |
| `receiving_yards_after_catch` | Float64 | Yards after the catch gained on plays in which player was receiver (this is an unofficial stat and may differ slightly between different sources). |
| `receiving_first_downs` | Int64 | Total number of first downs gained on receptions |
| `receiving_epa` | Float64 | Total EPA on plays where this receiver was targeted |
| `receiving_2pt_conversions` | Int64 | Two-point conversion receptions |
| `special_teams_tds` | Int64 | Total number of kick/punt return touchdowns |
| `def_tackles_solo` | Int64 | Total number of solo tackles for this player |
| `def_tackles_with_assist` | Int64 | Number of tackles this player had with an assisted tackle |
| `def_tackle_assists` | Int64 | Number of assisted tackles for this player |
| `def_tackles_for_loss` | Int64 | Number of tackles for loss (TFL) for this player |
| `def_tackles_for_loss_yards` | Int64 | Yards lost from TFLs involving this player |
| `def_fumbles_forced` | Int64 | Number of times a fumble was forced from this player |
| `def_sacks` | Float64 | Number of sacks form this player |
| `def_sack_yards` | Float64 | Yards lost from sacks forced by this player |
| `def_qb_hits` | Int64 | Number of QB hits from this player (should not include plays where the QB was sacked) |
| `def_interceptions` | Int64 | Number of interceptions forced by this player |
| `def_interception_yards` | Float64 | yards gained/lost by interception returns from this player |
| `def_pass_defended` | Int64 | Number of passes defended/broken up by this player |
| `def_tds` | Int64 | Number of defensive touchdowns scored by this player |
| `def_fumbles` | Int64 | Number of fumbles by this player |
| `def_safeties` | Int64 |  |
| `misc_yards` | Int64 |  |
| `fumble_recovery_own` | Int64 |  |
| `fumble_recovery_yards_own` | Int64 |  |
| `fumble_recovery_opp` | Int64 |  |
| `fumble_recovery_yards_opp` | Int64 |  |
| `fumble_recovery_tds` | Int64 |  |
| `penalties` | Int64 |  |
| `penalty_yards` | Int64 | Yards gained (or lost) by the posteam from the penalty. |
| `timeouts` | Int64 |  |
| `punt_returns` | Int64 |  |
| `punt_return_yards` | Int64 |  |
| `kickoff_returns` | Int64 |  |
| `kickoff_return_yards` | Int64 |  |
| `fg_made` | Int64 |  |
| `fg_att` | Int64 |  |
| `fg_missed` | Int64 |  |
| `fg_blocked` | Int64 |  |
| `fg_long` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg_made_0_19` | Int64 |  |
| `fg_made_20_29` | Int64 |  |
| `fg_made_30_39` | Int64 |  |
| `fg_made_40_49` | Int64 |  |
| `fg_made_50_59` | Int64 |  |
| `fg_made_60_` | Int64 |  |
| `fg_missed_0_19` | Int64 |  |
| `fg_missed_20_29` | Int64 |  |
| `fg_missed_30_39` | Int64 |  |
| `fg_missed_40_49` | Int64 |  |
| `fg_missed_50_59` | Int64 |  |
| `fg_missed_60_` | Int64 |  |
| `fg_made_list` | String |  |
| `fg_missed_list` | String |  |
| `fg_blocked_list` | String |  |
| `fg_made_distance` | Int64 |  |
| `fg_missed_distance` | Int64 |  |
| `fg_blocked_distance` | Int64 |  |
| `pat_made` | Int64 |  |
| `pat_att` | Int64 |  |
| `pat_missed` | Int64 |  |
| `pat_blocked` | Int64 |  |
| `pat_pct` | Float64 |  |
| `gwfg_made` | Int64 |  |
| `gwfg_att` | Int64 |  |
| `gwfg_missed` | Int64 |  |
| `gwfg_blocked` | Int64 |  |
| `gwfg_distance` | Int64 |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `summary_level` is not `"week"`/`"season"` or `season_type` is not `"REG"`/`"POST"`/`"REG+POST"`. |

**Example**

```python
from sportsdataverse.nfl import build_nfl_team_stats
wk = build_nfl_team_stats([2023], summary_level="week")
print(wk.shape)

# Season totals as pandas

df_pd = build_nfl_team_stats([2023], summary_level="season",
                             return_as_pandas=True)

# Pipeline next step (one line)

wk.sort("def_sacks", descending=True).head()
```

### calculate_nfl_series_conversion_rates {#calculate_nfl_series_conversion_rates}

`calculate_nfl_series_conversion_rates(pbp: 'pl.DataFrame', *, weekly: 'bool' = False, return_as_pandas: 'bool' = False) -> "pl.DataFrame | 'pd.DataFrame'"`

Compute per-team offense + defense series conversion rates.

A faithful polars port of nflfastR's `calculate_series_conversion_rates`.
Series where `down` is null (kickoffs, PAT/2pt attempts, non-plays, no
`posteam`) and series ending in a `"QB kneel"` are excluded from the
series count before rates are computed, matching the R source.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame carrying `season`, `week`, `posteam`, `defteam`, `down`, `series`, `series_success`, and `series_result` (added by the `add_series_data` port). Rows must already be in play order within each series so the internal `first()`/`last()` series collapse is correct. |
| `weekly` | `bool` | `False` | If `True`, group on `(season, team, week)`; if `False` (default), group on `(season, team)` -- collapsing every week into one season-level rate. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; else polars. |

**Returns**

A polars (or pandas) DataFrame with one row per team (per week when `weekly=True`), `off_n`/`def_n` (series count) plus the `off_*`/`def_*` rate columns documented in reference Sec 11. A team with offensive series but zero defensive series in a group (or vice versa -- effectively never happens in real data) carries nulls in the missing side rather than being dropped (full outer join).

| col_name | type | description |
|---|---|---|
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `team` | String | NFL team. Uses official abbreviations as per NFL.com |
| `off_n` | Int64 |  |
| `off_scr` | Float64 |  |
| `off_scr_1st` | Float64 |  |
| `off_scr_2nd` | Float64 |  |
| `off_scr_3rd` | Float64 |  |
| `off_scr_4th` | Float64 |  |
| `off_1st` | Float64 |  |
| `off_td` | Float64 |  |
| `off_fg` | Float64 |  |
| `off_punt` | Float64 |  |
| `off_to` | Float64 |  |
| `def_n` | Int64 |  |
| `def_scr` | Float64 |  |
| `def_scr_1st` | Float64 |  |
| `def_scr_2nd` | Float64 |  |
| `def_scr_3rd` | Float64 |  |
| `def_scr_4th` | Float64 |  |
| `def_1st` | Float64 |  |
| `def_td` | Float64 |  |
| `def_fg` | Float64 |  |
| `def_punt` | Float64 |  |
| `def_to` | Float64 |  |

**Example**

```python
from sportsdataverse.nfl import calculate_nfl_series_conversion_rates
rates = calculate_nfl_series_conversion_rates(pbp)
rates.filter(pl.col("team") == "KC").select("off_scr", "def_scr")

# Weekly grain

weekly = calculate_nfl_series_conversion_rates(pbp, weekly=True)

# Pipeline next step (one line)

rates.sort("off_scr", descending=True).head()
```

### clean_nfl_pbp {#clean_nfl_pbp}

`clean_nfl_pbp(df: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Canonicalize names/ids/teams on a play-by-play frame (nflfastR `clean_pbp` port).

See the module docstring for the full column set added, the
compute-if-absent scope note on `pass`/`rush`, and the lookaround ->
capture-group regex rewrites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | An nflverse-shape (or ESPN/native) play-by-play `polars.DataFrame`. Required columns: `desc`, `epa`, `game_id`, `play_id`, `season`, `posteam`. See the module docstring for the full optional-column-with-default list. |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame`; otherwise a `polars.DataFrame` (default). |

**Returns**

The input frame with every §6 column added/overwritten (idempotent -- pre-existing values of those columns, except `pass`/`rush`, are dropped and recomputed). A zero-row input yields a zero-row frame carrying the full documented schema rather than raising.

| col_name | type | description |
|---|---|---|
| `play_id` | Float64 | Numeric play id that when used with game_id and drive provides the unique identifier for a single play. |
| `game_id` | String | Ten digit identifier for NFL game. |
| `old_game_id` | String | Legacy NFL game ID. |
| `home_team` | String | The home team. Note that this contains the designated home team for games which no team is playing at home such as Super Bowls or NFL International games. |
| `away_team` | String | String abbreviation for the away team. |
| `season_type` | String | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | Int64 | Season week. |
| `posteam` | String | String abbreviation for the team with possession. |
| `posteam_type` | String | String indicating whether the posteam team is home or away. |
| `defteam` | String | String abbreviation for the team on defense. |
| `side_of_field` | String | String abbreviation for which team's side of the field the team with possession is currently on. |
| `yardline_100` | Float64 | Numeric distance in the number of yards from the opponent's endzone for the posteam. |
| `game_date` | String | Date of the game. |
| `quarter_seconds_remaining` | Float64 | Numeric seconds remaining in the quarter. |
| `half_seconds_remaining` | Float64 | Numeric seconds remaining in the half. |
| `game_seconds_remaining` | Float64 | Numeric seconds remaining in the game. |
| `game_half` | String | String indicating which half the play is in, either Half1, Half2, or Overtime. |
| `quarter_end` | Float64 | Binary indicator for whether or not the row of the data is marking the end of a quarter. |
| `drive` | Float64 | Numeric drive number in the game. |
| `sp` | Float64 | Binary indicator for whether or not a score occurred on the play. |
| `qtr` | Float64 | Quarter of the game (5 is overtime). |
| `down` | Float64 | The down for the given play. |
| `goal_to_go` | Float64 | Binary indicator for whether or not the posteam is in a goal down situation. |
| `time` | String | Time at start of play provided in string format as minutes:seconds remaining in the quarter. |
| `yrdln` | String | String indicating the current field position for a given play. |
| `ydstogo` | Float64 | Numeric yards in distance from either the first down marker or the endzone in goal down situations. |
| `ydsnet` | Float64 | Numeric value for total yards gained on the given drive. |
| `desc` | String | Detailed string description for the given play. |
| `play_type` | String | String indicating the type of play: pass (includes sacks), run (includes scrambles), punt, field_goal, kickoff, extra_point, qb_kneel, qb_spike, no_play (timeouts and penalties), and missing for rows indicating end of play. |
| `yards_gained` | Float64 | Numeric yards gained (or lost) by the possessing team, excluding yards gained via fumble recoveries and laterals. |
| `shotgun` | Float64 | Binary indicator for whether or not the play was in shotgun formation. |
| `no_huddle` | Float64 | Binary indicator for whether or not the play was in no_huddle formation. |
| `qb_dropback` | Float64 | Binary indicator for whether or not the QB dropped back on the play (pass attempt, sack, or scrambled). |
| `qb_kneel` | Float64 | Binary indicator for whether or not the QB took a knee. |
| `qb_spike` | Float64 | Binary indicator for whether or not the QB spiked the ball. |
| `qb_scramble` | Float64 | Binary indicator for whether or not the QB scrambled. |
| `pass_length` | String | String indicator for pass length: short or deep. |
| `pass_location` | String | String indicator for pass location: left, middle, or right. |
| `air_yards` | Float64 | Numeric value for distance in yards perpendicular to the line of scrimmage at where the targeted receiver either caught or didn't catch the ball. |
| `yards_after_catch` | Float64 | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `run_location` | String | String indicator for location of run: left, middle, or right. |
| `run_gap` | String | String indicator for line gap of run: end, guard, or tackle |
| `field_goal_result` | String | String indicator for result of field goal attempt: made, missed, or blocked. |
| `kick_distance` | Float64 | Numeric distance in yards for kickoffs, field goals, and punts. |
| `extra_point_result` | String | String indicator for the result of the extra point attempt: good, failed, blocked, safety (touchback in defensive endzone is 1 point apparently), or aborted. |
| `two_point_conv_result` | String | String indicator for result of two point conversion attempt: success, failure, safety (touchback in defensive endzone is 1 point apparently), or return. |
| `home_timeouts_remaining` | Float64 | Numeric timeouts remaining in the half for the home team. |
| `away_timeouts_remaining` | Float64 | Numeric timeouts remaining in the half for the away team. |
| `timeout` | Float64 | Binary indicator for whether or not a timeout was called by either team. |
| `timeout_team` | String | String abbreviation for which team called the timeout. |
| `td_team` | String | String abbreviation for which team scored the touchdown. |
| `td_player_name` | String | String name of the player who scored a touchdown. |
| `td_player_id` | String | Unique identifier of the player who scored a touchdown. |
| `posteam_timeouts_remaining` | Float64 | Number of timeouts remaining for the possession team. |
| `defteam_timeouts_remaining` | Float64 | Number of timeouts remaining for the team on defense. |
| `total_home_score` | Float64 | Score for the home team at the start of the play. |
| `total_away_score` | Float64 | Score for the away team at the start of the play. |
| `posteam_score` | Float64 | Score the posteam at the start of the play. |
| `defteam_score` | Float64 | Score the defteam at the start of the play. |
| `score_differential` | Float64 | Score differential between the posteam and defteam at the start of the play. |
| `posteam_score_post` | Float64 | Score for the posteam at the end of the play. |
| `defteam_score_post` | Float64 | Score for the defteam at the end of the play. |
| `score_differential_post` | Float64 | Score differential between the posteam and defteam at the end of the play. |
| `no_score_prob` | Float64 | Predicted probability of no score occurring for the rest of the half based on the expected points model. |
| `opp_fg_prob` | Float64 | Predicted probability of the defteam scoring a FG next. 'Next' in this context means the next score in the same game half. |
| `opp_safety_prob` | Float64 | Predicted probability of the defteam scoring a safety next. 'Next' in this context means the next score in the same game half. |
| `opp_td_prob` | Float64 | Predicted probability of the defteam scoring a TD next. 'Next' in this context means the next score in the same game half. |
| `fg_prob` | Float64 | Predicted probability of the posteam scoring a FG next. 'Next' in this context means the next score in the same game half. |
| `safety_prob` | Float64 | Predicted probability of the posteam scoring a safety next. 'Next' in this context means the next score in the same game half. |
| `td_prob` | Float64 | Predicted probability of the posteam scoring a TD next. 'Next' in this context means the next score in the same game half. |
| `extra_point_prob` | Float64 | Predicted probability of the posteam scoring an extra point. |
| `two_point_conversion_prob` | Float64 | Predicted probability of the posteam scoring the two point conversion. |
| `ep` | Float64 | Using the scoring event probabilities, the estimated expected points with respect to the possession team for the given play. |
| `epa` | Float64 | Expected points added (EPA) by the posteam for the given play. |
| `total_home_epa` | Float64 | Cumulative total EPA for the home team in the game so far. |
| `total_away_epa` | Float64 | Cumulative total EPA for the away team in the game so far. |
| `total_home_rush_epa` | Float64 | Cumulative total rushing EPA for the home team in the game so far. |
| `total_away_rush_epa` | Float64 | Cumulative total rushing EPA for the away team in the game so far. |
| `total_home_pass_epa` | Float64 | Cumulative total passing EPA for the home team in the game so far. |
| `total_away_pass_epa` | Float64 | Cumulative total passing EPA for the away team in the game so far. |
| `air_epa` | Float64 | EPA from the air yards alone. For completions this represents the actual value provided through the air. For incompletions this represents the hypothetical value that could've been added through the air if the pass was completed. |
| `yac_epa` | Float64 | EPA from the yards after catch alone. For completions this represents the actual value provided after the catch. For incompletions this represents the difference between the hypothetical air_epa and the play's raw observed EPA (how much the incomplete pass cost the posteam). |
| `comp_air_epa` | Float64 | EPA from the air yards alone only for completions. |
| `comp_yac_epa` | Float64 | EPA from the yards after catch alone only for completions. |
| `total_home_comp_air_epa` | Float64 | Cumulative total completions air EPA for the home team in the game so far. |
| `total_away_comp_air_epa` | Float64 | Cumulative total completions air EPA for the away team in the game so far. |
| `total_home_comp_yac_epa` | Float64 | Cumulative total completions yac EPA for the home team in the game so far. |
| `total_away_comp_yac_epa` | Float64 | Cumulative total completions yac EPA for the away team in the game so far. |
| `total_home_raw_air_epa` | Float64 | Cumulative total raw air EPA for the home team in the game so far. |
| `total_away_raw_air_epa` | Float64 | Cumulative total raw air EPA for the away team in the game so far. |
| `total_home_raw_yac_epa` | Float64 | Cumulative total raw yac EPA for the home team in the game so far. |
| `total_away_raw_yac_epa` | Float64 | Cumulative total raw yac EPA for the away team in the game so far. |
| `wp` | Float64 | Estimated win probability for the posteam given the current situation at the start of the given play. |
| `def_wp` | Float64 | Estimated win probability for the defteam. |
| `home_wp` | Float64 | Estimated win probability for the home team. |
| `away_wp` | Float64 | Estimated win probability for the away team. |
| `wpa` | Float64 | Win probability added (WPA) for the posteam. |
| `vegas_wpa` | Float64 | Win probability added (WPA) for the posteam: spread_adjusted model. |
| `vegas_home_wpa` | Float64 | Win probability added (WPA) for the home team: spread_adjusted model. |
| `home_wp_post` | Float64 | Estimated win probability for the home team at the end of the play. |
| `away_wp_post` | Float64 | Estimated win probability for the away team at the end of the play. |
| `vegas_wp` | Float64 | Estimated win probability for the posteam given the current situation at the start of the given play, incorporating pre-game Vegas line. |
| `vegas_home_wp` | Float64 | Estimated win probability for the home team incorporating pre-game Vegas line. |
| `total_home_rush_wpa` | Float64 | Cumulative total rushing WPA for the home team in the game so far. |
| `total_away_rush_wpa` | Float64 | Cumulative total rushing WPA for the away team in the game so far. |
| `total_home_pass_wpa` | Float64 | Cumulative total passing WPA for the home team in the game so far. |
| `total_away_pass_wpa` | Float64 | Cumulative total passing WPA for the away team in the game so far. |
| `air_wpa` | Float64 | WPA through the air (same logic as air_epa). |
| `yac_wpa` | Float64 | WPA from yards after the catch (same logic as yac_epa). |
| `comp_air_wpa` | Float64 | The air_wpa for completions only. |
| `comp_yac_wpa` | Float64 | The yac_wpa for completions only. |
| `total_home_comp_air_wpa` | Float64 | Cumulative total completions air WPA for the home team in the game so far. |
| `total_away_comp_air_wpa` | Float64 | Cumulative total completions air WPA for the away team in the game so far. |
| `total_home_comp_yac_wpa` | Float64 | Cumulative total completions yac WPA for the home team in the game so far. |
| `total_away_comp_yac_wpa` | Float64 | Cumulative total completions yac WPA for the away team in the game so far. |
| `total_home_raw_air_wpa` | Float64 | Cumulative total raw air WPA for the home team in the game so far. |
| `total_away_raw_air_wpa` | Float64 | Cumulative total raw air WPA for the away team in the game so far. |
| `total_home_raw_yac_wpa` | Float64 | Cumulative total raw yac WPA for the home team in the game so far. |
| `total_away_raw_yac_wpa` | Float64 | Cumulative total raw yac WPA for the away team in the game so far. |
| `punt_blocked` | Float64 | Binary indicator for if the punt was blocked. |
| `first_down_rush` | Float64 | Binary indicator for if a running play converted the first down. |
| `first_down_pass` | Float64 | Binary indicator for if a passing play converted the first down. |
| `first_down_penalty` | Float64 | Binary indicator for if a penalty converted the first down. |
| `third_down_converted` | Float64 | Binary indicator for if the first down was converted on third down. |
| `third_down_failed` | Float64 | Binary indicator for if the posteam failed to convert first down on third down. |
| `fourth_down_converted` | Float64 | Binary indicator for if the first down was converted on fourth down. |
| `fourth_down_failed` | Float64 | Binary indicator for if the posteam failed to convert first down on fourth down. |
| `incomplete_pass` | Float64 | Binary indicator for if the pass was incomplete. |
| `touchback` | Float64 | Binary indicator for if a touchback occurred on the play. |
| `interception` | Float64 | Binary indicator for if the pass was intercepted. |
| `punt_inside_twenty` | Float64 | Binary indicator for if the punt ended inside the twenty yard line. |
| `punt_in_endzone` | Float64 | Binary indicator for if the punt was in the endzone. |
| `punt_out_of_bounds` | Float64 | Binary indicator for if the punt went out of bounds. |
| `punt_downed` | Float64 | Binary indicator for if the punt was downed. |
| `punt_fair_catch` | Float64 | Binary indicator for if the punt was caught with a fair catch. |
| `kickoff_inside_twenty` | Float64 | Binary indicator for if the kickoff ended inside the twenty yard line. |
| `kickoff_in_endzone` | Float64 | Binary indicator for if the kickoff was in the endzone. |
| `kickoff_out_of_bounds` | Float64 | Binary indicator for if the kickoff went out of bounds. |
| `kickoff_downed` | Float64 | Binary indicator for if the kickoff was downed. |
| `kickoff_fair_catch` | Float64 | Binary indicator for if the kickoff was caught with a fair catch. |
| `fumble_forced` | Float64 | Binary indicator for if the fumble was forced. |
| `fumble_not_forced` | Float64 | Binary indicator for if the fumble was not forced. |
| `fumble_out_of_bounds` | Float64 | Binary indicator for if the fumble went out of bounds. |
| `solo_tackle` | Float64 | Binary indicator if the play had a solo tackle (could be multiple due to fumbles). |
| `safety` | Float64 | Binary indicator for whether or not a safety occurred. |
| `penalty` | Float64 | Binary indicator for whether or not a penalty occurred. |
| `tackled_for_loss` | Float64 | Binary indicator for whether or not a tackle for loss on a run play occurred. |
| `fumble_lost` | Float64 | Binary indicator for if the fumble was lost. |
| `own_kickoff_recovery` | Float64 | Binary indicator for if the kicking team recovered the kickoff. |
| `own_kickoff_recovery_td` | Float64 | Binary indicator for if the kicking team recovered the kickoff and scored a TD. |
| `qb_hit` | Float64 | Binary indicator if the QB was hit on the play. |
| `rush_attempt` | Float64 | Binary indicator for if the play was a run. |
| `pass_attempt` | Float64 | Binary indicator for if the play was a pass attempt (includes sacks). |
| `sack` | Float64 | Binary indicator for if the play ended in a sack. |
| `touchdown` | Float64 | Binary indicator for if the play resulted in a TD. |
| `pass_touchdown` | Float64 | Binary indicator for if the play resulted in a passing TD. |
| `rush_touchdown` | Float64 | Binary indicator for if the play resulted in a rushing TD. |
| `return_touchdown` | Float64 | Binary indicator for if the play resulted in a return TD. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `extra_point_attempt` | Float64 | Binary indicator for extra point attempt. |
| `two_point_attempt` | Float64 | Binary indicator for two point conversion attempt. |
| `field_goal_attempt` | Float64 | Binary indicator for field goal attempt. |
| `kickoff_attempt` | Float64 | Binary indicator for kickoff. |
| `punt_attempt` | Float64 | Binary indicator for punts. |
| `fumble` | Float64 | Binary indicator for if a fumble occurred. |
| `complete_pass` | Float64 | Binary indicator for if the pass was completed. |
| `assist_tackle` | Float64 | Binary indicator for if an assist tackle occurred. |
| `lateral_reception` | Float64 | Binary indicator for if a lateral occurred on the reception. |
| `lateral_rush` | Float64 | Binary indicator for if a lateral occurred on a run. |
| `lateral_return` | Float64 | Binary indicator for if a lateral occurred on a return. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `lateral_recovery` | Float64 | Binary indicator for if a lateral occurred on a fumble recovery. |
| `passer_player_id` | String | Unique identifier for the player that attempted the pass. |
| `passer_player_name` | String | String name for the player that attempted the pass. |
| `passing_yards` | Float64 | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `receiver_player_id` | String | Unique identifier for the receiver that was targeted on the pass. |
| `receiver_player_name` | String | String name for the targeted receiver. |
| `receiving_yards` | Float64 | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `rusher_player_id` | String | Unique identifier for the player that attempted the run. |
| `rusher_player_name` | String | String name for the player that attempted the run. |
| `rushing_yards` | Float64 | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `lateral_receiver_player_id` | String | Unique identifier for the player that received the last(!) lateral on a pass play. |
| `lateral_receiver_player_name` | String | String name for the player that received the last(!) lateral on a pass play. If there were multiple laterals in the same play, this will only be the last player who received a lateral. Please see <https://github.com/mrcaseb/nfl-data/tree/master/data/lateral_yards> for a list of plays where multiple players recorded lateral receiving yards. |
| `lateral_receiving_yards` | Float64 | Numeric yards by the `lateral_receiver_player_name` in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `lateral_rusher_player_id` | String | Unique identifier for the player that received the last(!) lateral on a run play. |
| `lateral_rusher_player_name` | String | String name for the player that received the last(!) lateral on a run play. If there were multiple laterals in the same play, this will only be the last player who received a lateral. Please see <https://github.com/mrcaseb/nfl-data/tree/master/data/lateral_yards> for a list of plays where multiple players recorded lateral rushing yards. |
| `lateral_rushing_yards` | Float64 | Numeric yards by the `lateral_rusher_player_name` in run plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `lateral_sack_player_id` | String | Unique identifier for the player that received the lateral on a sack. |
| `lateral_sack_player_name` | String | String name for the player that received the lateral on a sack. |
| `interception_player_id` | String | Unique identifier for the player that intercepted the pass. |
| `interception_player_name` | String | String name for the player that intercepted the pass. |
| `lateral_interception_player_id` | String | Unique identifier for the player that received the lateral on an interception. |
| `lateral_interception_player_name` | String | String name for the player that received the lateral on an interception. |
| `punt_returner_player_id` | String | Unique identifier for the punt returner. |
| `punt_returner_player_name` | String | String name for the punt returner. |
| `lateral_punt_returner_player_id` | String | Unique identifier for the player that received the lateral on a punt return. |
| `lateral_punt_returner_player_name` | String | String name for the player that received the lateral on a punt return. |
| `kickoff_returner_player_name` | String | String name for the kickoff returner. |
| `kickoff_returner_player_id` | String | Unique identifier for the kickoff returner. |
| `lateral_kickoff_returner_player_id` | String | Unique identifier for the player that received the lateral on a kickoff return. |
| `lateral_kickoff_returner_player_name` | String | String name for the player that received the lateral on a kickoff return. |
| `punter_player_id` | String | Unique identifier for the punter. |
| `punter_player_name` | String | String name for the punter. |
| `kicker_player_name` | String | String name for the kicker on FG or kickoff. |
| `kicker_player_id` | String | Unique identifier for the kicker on FG or kickoff. |
| `own_kickoff_recovery_player_id` | String | Unique identifier for the player that recovered their own kickoff. |
| `own_kickoff_recovery_player_name` | String | String name for the player that recovered their own kickoff. |
| `blocked_player_id` | String | Unique identifier for the player that blocked the punt or FG. |
| `blocked_player_name` | String | String name for the player that blocked the punt or FG. |
| `tackle_for_loss_1_player_id` | String | Unique identifier for one of the potential players with the tackle for loss. |
| `tackle_for_loss_1_player_name` | String | String name for one of the potential players with the tackle for loss. |
| `tackle_for_loss_2_player_id` | String | Unique identifier for one of the potential players with the tackle for loss. |
| `tackle_for_loss_2_player_name` | String | String name for one of the potential players with the tackle for loss. |
| `qb_hit_1_player_id` | String | Unique identifier for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_1_player_name` | String | String name for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_2_player_id` | String | Unique identifier for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_2_player_name` | String | String name for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `forced_fumble_player_1_team` | String | Team of one of the players with a forced fumble. |
| `forced_fumble_player_1_player_id` | String | Unique identifier of one of the players with a forced fumble. |
| `forced_fumble_player_1_player_name` | String | String name of one of the players with a forced fumble. |
| `forced_fumble_player_2_team` | String | Team of one of the players with a forced fumble. |
| `forced_fumble_player_2_player_id` | String | Unique identifier of one of the players with a forced fumble. |
| `forced_fumble_player_2_player_name` | String | String name of one of the players with a forced fumble. |
| `solo_tackle_1_team` | String | Team of one of the players with a solo tackle. |
| `solo_tackle_2_team` | String | Team of one of the players with a solo tackle. |
| `solo_tackle_1_player_id` | String | Unique identifier of one of the players with a solo tackle. |
| `solo_tackle_2_player_id` | String | Unique identifier of one of the players with a solo tackle. |
| `solo_tackle_1_player_name` | String | String name of one of the players with a solo tackle. |
| `solo_tackle_2_player_name` | String | String name of one of the players with a solo tackle. |
| `assist_tackle_1_player_id` | String | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_1_player_name` | String | String name of one of the players with a tackle assist. |
| `assist_tackle_1_team` | String | Team of one of the players with a tackle assist. |
| `assist_tackle_2_player_id` | String | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_2_player_name` | String | String name of one of the players with a tackle assist. |
| `assist_tackle_2_team` | String | Team of one of the players with a tackle assist. |
| `assist_tackle_3_player_id` | String | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_3_player_name` | String | String name of one of the players with a tackle assist. |
| `assist_tackle_3_team` | String | Team of one of the players with a tackle assist. |
| `assist_tackle_4_player_id` | String | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_4_player_name` | String | String name of one of the players with a tackle assist. |
| `assist_tackle_4_team` | String | Team of one of the players with a tackle assist. |
| `tackle_with_assist` | Float64 | Binary indicator for if there has been a tackle with assist. |
| `tackle_with_assist_1_player_id` | String | Unique identifier of one of the players with a tackle with assist. |
| `tackle_with_assist_1_player_name` | String | String name of one of the players with a tackle with assist. |
| `tackle_with_assist_1_team` | String | Team of one of the players with a tackle with assist. |
| `tackle_with_assist_2_player_id` | String | Unique identifier of one of the players with a tackle with assist. |
| `tackle_with_assist_2_player_name` | String | String name of one of the players with a tackle with assist. |
| `tackle_with_assist_2_team` | String | Team of one of the players with a tackle with assist. |
| `pass_defense_1_player_id` | String | Unique identifier of one of the players with a pass defense. |
| `pass_defense_1_player_name` | String | String name of one of the players with a pass defense. |
| `pass_defense_2_player_id` | String | Unique identifier of one of the players with a pass defense. |
| `pass_defense_2_player_name` | String | String name of one of the players with a pass defense. |
| `fumbled_1_team` | String | Team of one of the first player with a fumble. |
| `fumbled_1_player_id` | String | Unique identifier of the first player who fumbled on the play. |
| `fumbled_1_player_name` | String | String name of one of the first player who fumbled on the play. |
| `fumbled_2_player_id` | String | Unique identifier of the second player who fumbled on the play. |
| `fumbled_2_player_name` | String | String name of one of the second player who fumbled on the play. |
| `fumbled_2_team` | String | Team of one of the second player with a fumble. |
| `fumble_recovery_1_team` | String | Team of one of the players with a fumble recovery. |
| `fumble_recovery_1_yards` | Float64 | Yards gained by one of the players with a fumble recovery. |
| `fumble_recovery_1_player_id` | String | Unique identifier of one of the players with a fumble recovery. |
| `fumble_recovery_1_player_name` | String | String name of one of the players with a fumble recovery. |
| `fumble_recovery_2_team` | String | Team of one of the players with a fumble recovery. |
| `fumble_recovery_2_yards` | Float64 | Yards gained by one of the players with a fumble recovery. |
| `fumble_recovery_2_player_id` | String | Unique identifier of one of the players with a fumble recovery. |
| `fumble_recovery_2_player_name` | String | String name of one of the players with a fumble recovery. |
| `sack_player_id` | String | Unique identifier of the player who recorded a solo sack. |
| `sack_player_name` | String | String name of the player who recorded a solo sack. |
| `half_sack_1_player_id` | String | Unique identifier of the first player who recorded half a sack. |
| `half_sack_1_player_name` | String | String name of the first player who recorded half a sack. |
| `half_sack_2_player_id` | String | Unique identifier of the second player who recorded half a sack. |
| `half_sack_2_player_name` | String | String name of the second player who recorded half a sack. |
| `return_team` | String | String abbreviation of the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `return_yards` | Float64 | Yards gained by the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `penalty_team` | String | String abbreviation of the team with the penalty. |
| `penalty_player_id` | String | Unique identifier for the player with the penalty. |
| `penalty_player_name` | String | String name for the player with the penalty. |
| `penalty_yards` | Float64 | Yards gained (or lost) by the posteam from the penalty. |
| `replay_or_challenge` | Float64 | Binary indicator for whether or not a replay or challenge. |
| `replay_or_challenge_result` | String | String indicating the result of the replay or challenge. |
| `penalty_type` | String | String indicating the penalty type of the first penalty in the given play. Will be `NA` if `desc` is missing the type. |
| `defensive_two_point_attempt` | Float64 | Binary indicator whether or not the defense was able to have an attempt on a two point conversion, this results following a turnover. |
| `defensive_two_point_conv` | Float64 | Binary indicator whether or not the defense successfully scored on the two point conversion. |
| `defensive_extra_point_attempt` | Float64 | Binary indicator whether or not the defense was able to have an attempt on an extra point attempt, this results following a blocked attempt that the defense recovers the ball. |
| `defensive_extra_point_conv` | Float64 | Binary indicator whether or not the defense successfully scored on an extra point attempt. |
| `safety_player_name` | String | String name for the player who scored a safety. |
| `safety_player_id` | String | Unique identifier for the player who scored a safety. |
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `cp` | Float64 | Numeric value indicating the probability for a complete pass based on comparable game situations. |
| `cpoe` | Float64 | For a single pass play this is 1 - cp when the pass was completed or 0 - cp when the pass was incomplete. Analyzed for a whole game or season an indicator for the passer how much over or under expectation his completion percentage was. |
| `series` | Float64 | Starts at 1, each new first down increments, numbers shared across both teams NA: kickoffs, extra point/two point conversion attempts, non-plays, no posteam |
| `series_success` | Float64 | 1: scored touchdown, gained enough yards for first down. |
| `series_result` | String | Possible values: First down, Touchdown, Opp touchdown, Field goal, Missed field goal, Safety, Turnover, Punt, Turnover on downs, QB kneel, End of half |
| `order_sequence` | Float64 | Column provided by NFL to fix out-of-order plays. Available 2011 and beyond with source "nfl". |
| `start_time` | String | Kickoff time in eastern time zone. |
| `time_of_day` | String | Time of day of play in UTC "HH:MM:SS" format. Available 2011 and beyond with source "nfl". |
| `stadium` | String | Name of the stadium |
| `weather` | String | String describing the weather including temperature, humidity and wind (direction and speed). Doesn't change during the game! |
| `nfl_api_id` | String | UUID of the game in the new NFL API. |
| `play_clock` | String | Time on the playclock when the ball was snapped. |
| `play_deleted` | Float64 | Binary indicator for deleted plays. |
| `play_type_nfl` | String | Play type as listed in the NFL source. Slightly different to the regular play_type variable. |
| `special_teams_play` | Float64 | Binary indicator for whether play is special teams play from NFL source. Available 2011 and beyond with source "nfl". |
| `st_play_type` | String | Type of special teams play from NFL source. Available 2011 and beyond with source "nfl". |
| `end_clock_time` | String | Game time at the end of a given play. |
| `end_yard_line` | String | String indicating the yardline at the end of the given play consisting of team half and yard line number. |
| `fixed_drive` | Float64 | Manually created drive number in a game. |
| `fixed_drive_result` | String | Manually created drive result. |
| `drive_real_start_time` | String | Local day time when the drive started (currently not used by the NFL and therefore mostly 'NA'). |
| `drive_play_count` | Float64 | Numeric value of how many regular plays happened in a given drive. |
| `drive_time_of_possession` | String | Time of possession in a given drive. |
| `drive_first_downs` | Float64 | Number of first downs in a given drive. |
| `drive_inside20` | Float64 | Binary indicator if the offense was able to get inside the opponents 20 yard line. |
| `drive_ended_with_score` | Float64 | Binary indicator the drive ended with a score. |
| `drive_quarter_start` | Float64 | Numeric value indicating in which quarter the given drive has started. |
| `drive_quarter_end` | Float64 | Numeric value indicating in which quarter the given drive has ended. |
| `drive_yards_penalized` | Float64 | Numeric value of how many yards the offense gained or lost through penalties in the given drive. |
| `drive_start_transition` | String | String indicating how the offense got the ball. |
| `drive_end_transition` | String | String indicating how the offense lost the ball. |
| `drive_game_clock_start` | String | Game time at the beginning of a given drive. |
| `drive_game_clock_end` | String | Game time at the end of a given drive. |
| `drive_start_yard_line` | String | String indicating where a given drive started consisting of team half and yard line number. |
| `drive_end_yard_line` | String | String indicating where a given drive ended consisting of team half and yard line number. |
| `drive_play_id_started` | Float64 | Play_id of the first play in the given drive. |
| `drive_play_id_ended` | Float64 | Play_id of the last play in the given drive. |
| `away_score` | Int64 | The number of points the away team scored. Is NA for games which haven't yet been played. |
| `home_score` | Int64 | The number of points the home team scored. Is NA for games which haven't yet been played. |
| `location` | String | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `result` | Int64 | The number of points the home team scored minus the number of points the visiting team scored. Equals h_score - v_score. Is NA for games which haven't yet been played. Convenient for evaluating against the spread bets. |
| `total` | Int64 | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `spread_line` | Float64 | The closing spread line for the game. A positive number means the home team was favored by that many points, a negative number means the away team was favored by that many points. (Source: Pro-Football-Reference) |
| `total_line` | Float64 | The closing total line for the game. (Source: Pro-Football-Reference) |
| `div_game` | Int64 | Binary indicator of whether or not game was played by 2 teams in the same division. |
| `roof` | String | One of 'dome', 'outdoors', 'closed', 'open' indicating indicating the roof status of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `surface` | String | What type of ground the game was played on. (Source: Pro-Football-Reference) |
| `temp` | Int64 | The temperature at the stadium only for 'roof' = 'outdoors' or 'open'.(Source: Pro-Football-Reference) |
| `wind` | Int64 | The speed of the wind in miles/hour only for 'roof' = 'outdoors' or 'open'. (Source: Pro-Football-Reference) |
| `home_coach` | String | First and last name of the home team coach. (Source: Pro-Football-Reference) |
| `away_coach` | String | First and last name of the away team coach. (Source: Pro-Football-Reference) |
| `stadium_id` | String | ID of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `game_stadium` | String | Name of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `pass` | Float64 | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `rush` | Float64 | Binary indicator if the play was a rushing play. |
| `home_opening_kickoff` | Int64 | 1 if the home team received the opening kickoff, 0 otherwise. |
| `qb_epa` | Float64 | Gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `xyac_epa` | Float64 | Expected value of EPA gained after the catch, starting from where the catch was made. Zero yards after the catch would be listed as zero EPA. |
| `xyac_mean_yardage` | Float64 | Average expected yards after the catch based on where the ball was caught. |
| `xyac_median_yardage` | Int64 | Median expected yards after the catch based on where the ball was caught. |
| `xyac_success` | Float64 | Probability play earns positive EPA (relative to where play started) based on where ball was caught. |
| `xyac_fd` | Float64 | Probability play earns a first down based on where the ball was caught. |
| `xpass` | Float64 | Probability of dropback scaled from 0 to 1. |
| `pass_oe` | Float64 | Dropback percent over expected on a given play scaled from 0 to 100. |
| `aborted_play` | Int64 | Binary indicator if the play description indicates "Aborted". |
| `success` | Float64 | Binary indicator whether epa > 0 in the given play. |
| `passer_jersey_number` | Int64 | Jersey number of the passer. |
| `rusher_jersey_number` | Int64 | Jersey number of the rusher. |
| `receiver_jersey_number` | Int64 | Jersey number of the receiver. |
| `first_down` | Int64 | Binary indicator if the play ended in a first down. |
| `special` | Int64 | Binary indicator if "play_type" is one of "extra_point", "field_goal", "kickoff", or "punt". |
| `play` | Int64 | Binary indicator: 1 if the play was a 'normal' play (including penalties), 0 otherwise. |
| `passer_id` | String | ID of the player in the 'passer' column. |
| `passer` | String | Name of the dropback player (scrambles included) including plays with penalties. |
| `rusher_id` | String | ID of the player in the 'rusher' column. |
| `rusher` | String | Name of the rusher (no scrambles) including plays with penalties. |
| `receiver_id` | String | ID of the player in the 'receiver' column. |
| `receiver` | String | Name of the receiver including plays with penalties. |
| `name` | String | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `jersey_number` | Int64 | Jersey number. Often useful for joins by name/team/jersey. |
| `id` | String | ID of the player in the 'name' column. |
| `fantasy_player_name` | String | Name of the rusher on rush plays or receiver on pass plays (from official stats). |
| `fantasy_player_id` | String | ID of the rusher on rush plays or receiver on pass plays (from official stats). |
| `fantasy` | String | Name of the rusher on rush plays or receiver on pass plays. |
| `fantasy_id` | String | ID of the rusher on rush plays or receiver on pass plays. |
| `out_of_bounds` | Int64 | 1 if play description contains ran ob, pushed ob, or sacked ob; 0 otherwise. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_clean import clean_nfl_pbp

pbp = load_nfl_pbp([2023])
cleaned = clean_nfl_pbp(pbp)
print(cleaned.select("name", "id", "fantasy").head())

# Pandas output

cleaned_pd = clean_nfl_pbp(pbp, return_as_pandas=True)

# Pipeline next step (one line)

import polars as pl
cleaned.filter(pl.col("play") == 1).group_by("passer").len()
```

### espn_nfl_pbp_nflverse {#espn_nfl_pbp_nflverse}

`espn_nfl_pbp_nflverse(plays: 'pl.DataFrame', schedule: 'Optional[pl.DataFrame]' = None, *, split_pats: 'bool' = True) -> 'pl.DataFrame'`

ESPN NFL play-by-play in nflverse `play_by_play` names, conventions and dtypes.

Turns `~sportsdataverse.nfl.NFLPlayProcess` output (`plays_frame`, one game or many, or the
published `espn_nfl_pbp` release) into nflverse's base play-by-play: the keys, teams, field position,
clock, down and distance, scores, timeouts, play types, the 0/1 play flags, yardage and drive detail,
and, with `schedule`, nflverse's game columns (nflfastR's `add_game_data`). It is additive: the
cfbfastR-shape ESPN frame is not changed, and nothing is fetched.

Conventions follow nflverse, not ESPN: `game_id` is nflverse's `YYYY_WW_AWAY_HOME` and `play_id` the
GSIS play id ESPN embeds in its play `id` (2006 on), so the frame joins nflverse `play_by_play` on
`(game_id, play_id)`. Teams come from the team ids and the game id, never from ESPN's abbreviations, in
the franchise's current abbreviation as nflverse writes them (`LA` for the 2007 Rams). A kickoff
belongs to the receiving team; a timeout row has no possession, down or field position; an offensive
touchdown counts as a first down; a declined penalty is no penalty. With `split_pats` the try ESPN
folds into a touchdown row (2014 on) gets its own `extra_point` / two-point row
(`~sportsdataverse.nfl.split_pat_rows`) and the touchdown row's scores leave the try's points out.

Agreement with nflverse on whole seasons (2007, 2014, 2019, 2024) is 99%+ for most columns. The known
gaps are ESPN's: `desc` is ESPN's text, not the NFL feed's; ESPN stamps some plays (scoring plays,
whole 2024 games) with the clock after the snap; before 2009 many plays carry a stub text ("Rush",
"Pass Completion"), so the text-derived columns (pass / run direction, completion, yards) are thin
there; the timeouts are the processor's own count; a split PAT row's `play_id` is the touchdown's
plus one (ESPN gives the try no id), so it does not join nflverse's PAT row.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | `NFLPlayProcess(...).plays_frame` or the `espn_nfl_pbp` release (one or many games). |
| `schedule` | `Optional[DataFrame]` | `None` | The nflverse schedule (`~sportsdataverse.nfl.load_nfl_schedule`). Adds the game columns, and supplies `nflverse_game_id` (from its `espn` id) when `plays` has none. |
| `split_pats` | `bool` | `True` | Give each folded PAT / two-point try its own row, as nflverse does (default `True`). |

**Returns**

One row per ESPN play (plus one per split try) in nflverse's column names, flags as `Int64` 0/1, measures as `Float64`, plus `espn_game_id` / `espn_play_id` to join back to ESPN (participants are keyed on the ESPN play id). The schedule columns are present only when `schedule` is given.

| col_name | type | description |
|---|---|---|
| `play_id` | Int64 | GSIS play id, the digits ESPN's play id carries after its game id (2006 on); a split PAT row is its touchdown's play_id plus 1. Joins nflverse play_by_play with game_id. |
| `game_id` | String | nflverse game id, YYYY_WW_AWAY_HOME (the old abbreviation, e.g. 2007_14_SD_TEN). |
| `espn_game_id` | Int64 | ESPN event id of the game. |
| `espn_play_id` | Int64 | ESPN play id (participants and the ESPN frame are keyed on it); a split PAT row's is the touchdown's plus 1. |
| `home_team` | String | Home team, nflverse's current franchise abbreviation (LA, LAC, LV for the Rams, Chargers, Raiders of every era). |
| `away_team` | String | Away team, nflverse's current franchise abbreviation. |
| `season_type` | String | REG, POST or PRE. |
| `week` | Int64 | nflverse week (playoff weeks continue the regular-season count), from game_id. |
| `season` | Int64 | Season, from game_id. |
| `posteam` | String | Team with possession (a kickoff belongs to the receiving team); null on a timeout row. |
| `posteam_type` | String | home or away: which side posteam is. |
| `defteam` | String | Team on defense; null on a timeout row. |
| `side_of_field` | String | Abbreviation of the team whose half the ball is in, or 50 at midfield. |
| `yardline_100` | Float64 | Yards from the opponent's end zone at the snap (a kickoff from the receiving team's side). |
| `yrdln` | String | Field position as nflverse writes it (e.g. KC 35, MID 50). |
| `qtr` | Float64 | Quarter (5 = overtime). |
| `quarter_seconds_remaining` | Float64 | Seconds left in the quarter at the snap: the clock the NFL text opens with where it has one, else ESPN's clock. |
| `half_seconds_remaining` | Float64 | Seconds left in the half at the snap (overtime counts only its own period). |
| `game_seconds_remaining` | Float64 | Seconds left in regulation at the snap (overtime counts only its own period). |
| `game_half` | String | Half1, Half2 or Overtime. |
| `time` | String | Game clock at the snap, MM:SS. |
| `down` | Float64 | Down; null on kickoffs, tries and timeouts. |
| `ydstogo` | Float64 | Yards to go for a first down (0 on kickoffs and timeouts). |
| `posteam_score` | Float64 | posteam's score before the play. |
| `defteam_score` | Float64 | defteam's score before the play. |
| `score_differential` | Float64 | posteam_score minus defteam_score. |
| `posteam_score_post` | Float64 | posteam's score after the play (a touchdown row leaves out its split try's points). |
| `defteam_score_post` | Float64 | defteam's score after the play. |
| `score_differential_post` | Float64 | posteam_score_post minus defteam_score_post. |
| `total_home_score` | Float64 | Home score after the play (a touchdown row leaves out its split try's points). |
| `total_away_score` | Float64 | Away score after the play (a touchdown row leaves out its split try's points). |
| `home_timeouts_remaining` | Float64 | Home timeouts left before the play (after it on a timeout row), from the ESPN processor's count. |
| `away_timeouts_remaining` | Float64 | Away timeouts left before the play (after it on a timeout row), from the ESPN processor's count. |
| `posteam_timeouts_remaining` | Float64 | posteam's timeouts left before the play; null on a timeout row. |
| `defteam_timeouts_remaining` | Float64 | defteam's timeouts left before the play; null on a timeout row. |
| `complete_pass` | Int64 | 0/1 flag: completed pass. |
| `interception` | Int64 | 0/1 flag: intercepted pass. |
| `sack` | Int64 | 0/1 flag: sack. |
| `rush_attempt` | Int64 | 0/1 flag: rushing attempt (scrambles and kneels included). |
| `touchdown` | Int64 | 0/1 flag: touchdown scored on the play. |
| `pass_touchdown` | Int64 | 0/1 flag: passing touchdown. |
| `rush_touchdown` | Int64 | 0/1 flag: rushing touchdown. |
| `safety` | Int64 | 0/1 flag: safety. |
| `fumble` | Int64 | 0/1 flag: fumble (or muff) on the play. |
| `fumble_lost` | Int64 | 0/1 flag: fumble recovered by the other team. |
| `first_down_penalty` | Int64 | 0/1 flag: first down awarded by penalty. |
| `kickoff_attempt` | Int64 | 0/1 flag: kickoff. |
| `kickoff_out_of_bounds` | Int64 | 0/1 flag: kickoff out of bounds. |
| `kickoff_downed` | Int64 | 0/1 flag: kickoff downed. |
| `kickoff_fair_catch` | Int64 | 0/1 flag: kickoff fair catch. |
| `punt_attempt` | Int64 | 0/1 flag: punt. |
| `punt_blocked` | Int64 | 0/1 flag: blocked punt. |
| `punt_out_of_bounds` | Int64 | 0/1 flag: punt out of bounds. |
| `punt_downed` | Int64 | 0/1 flag: punt downed. |
| `punt_fair_catch` | Int64 | 0/1 flag: punt fair catch. |
| `field_goal_attempt` | Int64 | 0/1 flag: field goal attempt. |
| `extra_point_attempt` | Int64 | 0/1 flag: extra point attempt. |
| `two_point_attempt` | Int64 | 0/1 flag: two-point conversion attempt. |
| `qb_kneel` | Int64 | 0/1 flag: quarterback kneel. |
| `special` | Int64 | 0/1 flag: special-teams play (kickoff, punt, field goal, extra point). |
| `tackled_for_loss` | Int64 | 0/1 flag: run tackled for a loss. |
| `goal_to_go` | Int64 | 0/1 flag: goal-to-go situation. |
| `pass_attempt` | Int64 | 0/1 flag: pass attempt, sacks included (nflverse's convention). |
| `incomplete_pass` | Int64 | 0/1 flag: incomplete pass (not intercepted, not a sack). |
| `fumble_forced` | Int64 | 0/1 flag: forced fumble (the text names the forcer, FUMBLES (J.Doe)). |
| `special_teams_play` | Int64 | 0/1 flag: special-teams play as nflverse flags it (a made field goal is left out). |
| `fumble_not_forced` | Int64 | 0/1 flag: fumble with no forcer named. |
| `penalty` | Int64 | 0/1 flag: accepted penalty (a declined one is 0, as in nflverse). |
| `return_touchdown` | Int64 | 0/1 flag: touchdown that is neither a passing nor a rushing one (returns, recoveries). |
| `touchback` | Int64 | 0/1 flag: kickoff or punt touchback. |
| `qb_spike` | Int64 | 0/1 flag: spike (the text says spiked). |
| `qb_scramble` | Int64 | 0/1 flag: quarterback scramble (the text says scrambles). |
| `qb_dropback` | Int64 | 0/1 flag: dropback: pass attempt, sack, scramble or spike. |
| `first_down` | Int64 | 0/1 flag: first down by yards, penalty or an offensive touchdown. |
| `first_down_rush` | Int64 | 0/1 flag: rushing first down (a rushing touchdown counts). |
| `first_down_pass` | Int64 | 0/1 flag: passing first down (a passing touchdown counts). |
| `third_down_converted` | Int64 | 0/1 flag: third down converted (first down or offensive touchdown). |
| `third_down_failed` | Int64 | 0/1 flag: third-down pass or run that did not convert. |
| `fourth_down_converted` | Int64 | 0/1 flag: fourth down converted (first down or offensive touchdown). |
| `fourth_down_failed` | Int64 | 0/1 flag: fourth-down pass or run that did not convert. |
| `aborted_play` | Int64 | 0/1 flag: aborted snap (the text contains Aborted, nflfastR's rule). |
| `drive_ended_with_score` | Int64 | 0/1 flag: the play's drive ended in a score. |
| `desc` | String | ESPN's play text (close to, but not, the NFL feed's description nflverse carries). |
| `field_goal_result` | String | made, missed or blocked. |
| `extra_point_result` | String | good, failed or blocked. |
| `two_point_conv_result` | String | success or failure. |
| `run_location` | String | left, middle or right, from the text. |
| `pass_location` | String | left, middle or right, from the text. |
| `pass_length` | String | short or deep, from the text. |
| `drive_play_count` | Float64 | Offensive plays in the play's ESPN drive. |
| `drive_time_of_possession` | String | Time of possession of the play's ESPN drive, M:SS. |
| `drive_quarter_start` | Float64 | Quarter the play's ESPN drive started in. |
| `drive_quarter_end` | Float64 | Quarter the play's ESPN drive ended in. |
| `ydsnet` | Float64 | Net yards of the play's ESPN drive. |
| `play_type_nfl` | String | nflverse's NFL play type (PASS, RUSH, SACK, INTERCEPTION, KICK_OFF, PUNT, FIELD_GOAL, XP_KICK, PAT2, PENALTY, TIMEOUT, UNSPECIFIED). |
| `play_type` | String | nflverse's play type: pass, run, punt, field_goal, kickoff, extra_point, qb_kneel, qb_spike, no_play, or null. |
| `yards_gained` | Float64 | Yards gained on the play itself (an accepted penalty's yards excluded; 0 on kicks and no-play penalties). |
| `passing_yards` | Float64 | Yards on a completed pass; null otherwise. |
| `receiving_yards` | Float64 | Yards on a completed pass; null otherwise. |
| `rushing_yards` | Float64 | Yards on a run; null otherwise. |
| `penalty_yards` | Float64 | Yards of an accepted penalty; null otherwise. |
| `penalty_team` | String | Team of an accepted penalty, from ESPN's penalized team id. |
| `td_team` | String | Team that scored the touchdown. |
| `spread_line` | Float64 | Home team's point spread (positive when the home team is favored): the schedule's when given, else ESPN's. |
| `total_line` | Float64 | Game total (over/under): the schedule's when given, else ESPN's. |
| `drive` | Float64 | ESPN's drive number within the game (the digits of ESPN's drive id after the game id). |
| `old_game_id` | String | NFL's old game id (YYYYMMDDGG), from the schedule. |
| `game_date` | String | Game date (YYYY-MM-DD), the schedule's gameday. |
| `location` | String | Home or Neutral, from the schedule. |
| `result` | Int64 | Final home score minus away score, from the schedule. |
| `total` | Int64 | Final combined score, from the schedule. |
| `home_score` | Int64 | Final home score, from the schedule. |
| `away_score` | Int64 | Final away score, from the schedule. |
| `div_game` | Int64 | 1 for a divisional game, from the schedule. |
| `roof` | String | outdoors, dome, closed or open, from the schedule. |
| `surface` | String | Playing surface, from the schedule. |
| `temp` | Int64 | Temperature (F), from the schedule; null indoors. |
| `wind` | Int64 | Wind speed (mph), from the schedule; null indoors. |
| `home_coach` | String | Home head coach, from the schedule. |
| `away_coach` | String | Away head coach, from the schedule. |
| `game_stadium` | String | Stadium name, from the schedule (nflverse's stadium column is the feed's own name). |
| `stadium_id` | String | nflverse stadium id, from the schedule. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `plays` lacks the processor's columns, or carries no `nflverse_game_id` and no `schedule` (with `game_id` and `espn`) is given or the schedule lacks one of its games, or an id column is a float, or a split try's synthetic id is already a play's (`~sportsdataverse.nfl.split_pat_rows`). |

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess, espn_nfl_pbp_nflverse, load_nfl_schedule
proc = NFLPlayProcess(gameId=401671740)
proc.espn_nfl_pbp()
proc.run_processing_pipeline()
pbp = espn_nfl_pbp_nflverse(proc.plays_frame, load_nfl_schedule([2024]))
```
