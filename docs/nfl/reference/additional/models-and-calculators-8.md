# NFL — additional Python functions — Models and calculators: enrich_nfl–get_2pt

> NFL — additional Python functions — Models and calculators: enrich_nfl–get_2pt — function reference in sdv-py, the SportsDataverse Python package.

### enrich_nfl_pbp {#enrich_nfl_pbp}

`enrich_nfl_pbp(df: 'pl.DataFrame', *, method: 'str' = 'lead_diff', models_dir: 'Union[str, None]' = None, add_fourth_down: 'bool' = True, league: 'str' = 'nfl', touchback_yardline: 'Union[int, None]' = None, ep_point_values: 'Union[np.ndarray, None]' = None, return_as_pandas: 'bool' = False, strict: 'bool | None' = None) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Enrich an nflverse-shape PBP frame with EP/EPA/WP/WPA/CP/CPOE/xYAC.

`method="lead_diff"` (the default and only implemented method) is a
NFLVERSE-NATIVE derivation that mirrors nflfastR's `helper_add_ep_wp.R`.
It runs EP -> EPA -> WP -> WPA -> CP/CPOE -> xYAC, deriving `epa` / `wpa`
with grouped lead-differences (over `game_id`) and the nflfastR scoring /
end-of-half / OT overlays — it does **not** call `calculate_epa` /
`calculate_wpa` (those consume ESPN-internal columns absent from a
nflverse frame).  Kickoffs have their EP model features substituted
(the season's rule touchback `yardline_100`, `down=1`, `ydstogo=10`)
before EP scoring and down-NA plays (PATs, two-point conversions) are
scored on their own spot as 1st-and-10 — see
`_apply_feature_substitution`.  Overtime `wp` comes from the
closed form of the season's overtime rule format
(`_apply_ot_wp_overlay`).

The orchestrator is idempotent: stale `ep` / `epa` / `wp` / `wpa` /
`cp` / `cpoe` / xYAC columns are recomputed on each call.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | Play-by-play DataFrame in nflverse shape. Must satisfy the `NFLVERSE_FRAME_CONTRACT` minimum (`game_id`, `season`, `posteam`, `home_team`, `yardline_100`, `ydstogo`, `down`, `half_seconds_remaining`, the timeout columns, plus the WP inputs `score_differential` / `game_seconds_remaining` / `spread_line` / `receive_2h_ko`). |
| `method` | `str` | `'lead_diff'` | `"lead_diff"` (default) — the native, nflfastR-faithful EPA/WPA derivation and the canonical parity path. It is the only supported method; any other value raises `ValueError`. (The param is retained for forward extensibility / API stability.) |
| `models_dir` | `Union[str, None]` | `None` | Optional directory to load the xYAC model (`xyac_model.ubj`) from instead of downloading/caching it — for offline use or a custom-trained model. When `None` (default) the bundled EP/WP/CP models load from `sportsdataverse/nfl/models/` and the large xYAC model is resolved bundled → cache → downloaded-from-release. |
| `add_fourth_down` | `bool` | `True` | When `True` (default), attach the nfl4th 4th-down decision columns (`go_wp` / `fg_wp` / `punt_wp` / `go_boost` / `fourth_down_recommendation` etc.) — scored only on the qualifying 4th-down rows and null elsewhere. The 4th-down models are download-on-demand; when they are unavailable offline the columns are emitted as all-null (with a `RuntimeWarning`). Set `False` to skip the step entirely. |
| `league` | `str` | `'nfl'` | `"nfl"` (default) applies the NFL overtime rule formats. The spring-football port passes its league (`"xfl"` / `"ufl"` / `"cfl"`), whose shootout overtime has no NFL closed form: overtime `wp` / `vegas_wp` are left null there. |
| `touchback_yardline` | `Union[int, None]` | `None` | Fixed kickoff-touchback yards-to-endzone for the EP feature substitution and the overtime re-score. `None` (default) resolves the NFL rule spot per season (`~sportsdataverse.nfl.model_vars.touchback_yardline`); the spring-football port passes its league-season constant. |
| `ep_point_values` | `Union[ndarray, None]` | `None` | 7-class point vector collapsing the EP class probabilities into `ep` (see `calculate_expected_points`). `None` (default) is the NFL vector. The air-yards / qb_epa re-scores keep the NFL vector; today every league's vector equals it, so nothing diverges. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |
| `strict` | `bool \| None` | `None` | When `True`, a model that cannot be obtained (the download-on-demand xYAC / nfl4th boosters offline with an empty cache) raises instead of warning and nulling the columns it feeds. `None` (default) reads `SDV_PY_MODELS_STRICT` (`1` = strict); a producer that must not publish null model columns turns it on. |

**Returns**

polars.DataFrame (or pandas.DataFrame when *return_as_pandas* is `True`) — the input frame with the following columns added or recomputed: * `ep` — start-of-play expected points (float64, clipped to [-10, 10]). * `epa` — expected points added (float64; null on terminal/timeout rows). * `qb_epa` — EPA crediting the QB (float64). Equals `epa` on every play except completed passes the receiver then loses to a fumble (`complete_pass == 1 & fumble_lost == 1`), which are re-spotted at the fumble spot (no turnover) and re-scored so the QB keeps completion + YAC credit and is not penalised for the turnover (nflfastR `add_qb_epa`). * `wp` — naive win probability from the `wp_naive` model (float64). * `vegas_wp` — spread-adjusted win probability from `wp_spread` (falls back to `wp` when `spread_line` is null). * `def_wp` — defensive team win probability (`1 - wp`). * `home_wp` — home-team win probability (possession-team frame flip). * `away_wp` — away-team win probability (`1 - home_wp`). * `wpa` — win probability added (float64; null on kneel/terminal rows). * `vegas_wpa` — spread-adjusted WPA. * `vegas_home_wpa` — home-frame spread-adjusted WPA lead difference (kept as a real column per nflfastR; NOT null'd on kneel rows). * `total_home_epa`, `total_away_epa`, `total_home_rush_epa`, `total_away_rush_epa`, `total_home_pass_epa`, `total_away_pass_epa`, `total_home_rush_wpa`, `total_away_rush_wpa`, `total_home_pass_wpa`, `total_away_pass_wpa` — per-game cumulative home/away-signed EPA/WPA running totals (nflfastR `add_ep_variables` / `add_wp_variables` tails; every cumsum is within `game_id`). * `air_epa`, `yac_epa`, `comp_air_epa`, `comp_yac_epa`, `total_home_comp_air_epa`, `total_away_comp_air_epa`, `total_home_comp_yac_epa`, `total_away_comp_yac_epa`, `total_home_raw_air_epa`, `total_away_raw_air_epa`, `total_home_raw_yac_epa`, `total_away_raw_yac_epa` — the air/YAC EPA family (nflfastR `add_air_yac_ep_variables`): an EP re-score on the air-yards-projected state splits `epa` into its air and yards-after-catch components; `comp_*` gate to completions; the totals are per-game signed cumulative sums. Null on non-qualifying plays (all float64). * `air_wpa`, `yac_wpa`, `comp_air_wpa`, `comp_yac_wpa`, and the eight `total_{home,away}_{comp,raw}_{air,yac}_wpa` mirrors — the air/YAC WPA family (`add_air_yac_wp_variables`), same structure on `wp` / `wpa`. * `cp` — completion probability for intended pass plays (null otherwise; float64). * `cpoe` — completion probability over expected, percentage-point scale (null when `complete_pass` absent; float64). * `xpass` — expected dropback (pass) probability from the `binary:logistic` xpass model (faithful to nflfastR's `add_xpass`; null outside the `valid_play` filter — `season < 2006` or any required input null; float64). `xpass_model.ubj` is **not** bundled — on first use it is downloaded from the `nfl_model_artifacts` GitHub release and cached under `<cache_dir>/models/`. If the model is unavailable offline the xpass step is skipped with a `RuntimeWarning` and `xpass` / `pass_oe` stay null. * `pass_oe` — dropback percent over expected, `100 * (pass - xpass)` (null when `xpass` is null and when `rush == 0 & pass == 0`; float64). * `xyac_epa`, `xyac_mean_yardage`, `xyac_median_yardage`, `xyac_success`, `xyac_fd` — expected yards after catch, derived from the single multinomial xYAC model (faithful to nflfastR's `add_xyac`; null on non-qualifying plays). `xyac_model.ubj` (~34 MB) is **not** bundled in the wheel — on first use it is downloaded from the `nfl_model_artifacts` GitHub release and cached under `<cache_dir>/models/` (preserved across `clear_cache()`). If the model is unavailable offline (no cache + no network) the xYAC step is skipped with a `RuntimeWarning` and the five columns stay null while the rest of the enrichment proceeds. * `qbr_epa`, `weight`, `non_fumble_sack`, `sack_epa`, `pass_epa`, `rush_epa`, `pen_epa`, `sack_weight`, `pass_weight`, `rush_weight`, `pen_weight`, `action_play` — the per-play Total-QBR EPA components (nflverse port of the ESPN `__process_qbr` step): `qb_epa` clamped at `-5`/`-3.5` (fumble), the win-probability leverage `weight`, and the play-family EPA / weight partitions. These are the inputs the QBR rating aggregates downstream (the rating itself is not computed here). * `go_wp`, `first_down_prob`, `wp_succeed`, `wp_fail`, `fg_make_prob`, `make_fg_wp`, `miss_fg_wp`, `fg_wp`, `punt_wp`, `go_boost`, `go_wp_diff`, `punt_wp_diff`, `fg_wp_diff`, `fourth_down_recommendation` — the nfl4th 4th-down decision surface (default-on; see *add_fourth_down*). Scored only on 4th-down rows (`down == 4` with a non-null `yardline_100`); null on every other play. Omitted / all-null when *add_fourth_down* is `False` or the 4th-down models are unavailable offline.

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
| `extra_point_prob` | Float64 | Predicted probability of the posteam scoring an extra point. |
| `two_point_conversion_prob` | Float64 | Predicted probability of the posteam scoring the two point conversion. |
| `epa` | Float64 | Expected points added (EPA) by the posteam for the given play. |
| `vegas_home_wpa` | Float64 | Win probability added (WPA) for the home team: spread_adjusted model. |
| `home_wp_post` | Float64 | Estimated win probability for the home team at the end of the play. |
| `away_wp_post` | Float64 | Estimated win probability for the away team at the end of the play. |
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
| `aborted_play` | Float64 | Binary indicator if the play description indicates "Aborted". |
| `success` | Float64 | Binary indicator whether epa > 0 in the given play. |
| `passer` | String | Name of the dropback player (scrambles included) including plays with penalties. |
| `passer_jersey_number` | Int64 | Jersey number of the passer. |
| `rusher` | String | Name of the rusher (no scrambles) including plays with penalties. |
| `rusher_jersey_number` | Int64 | Jersey number of the rusher. |
| `receiver` | String | Name of the receiver including plays with penalties. |
| `receiver_jersey_number` | Int64 | Jersey number of the receiver. |
| `pass` | Float64 | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `rush` | Float64 | Binary indicator if the play was a rushing play. |
| `first_down` | Float64 | Binary indicator if the play ended in a first down. |
| `special` | Float64 | Binary indicator if "play_type" is one of "extra_point", "field_goal", "kickoff", or "punt". |
| `play` | Float64 | Binary indicator: 1 if the play was a 'normal' play (including penalties), 0 otherwise. |
| `passer_id` | String | ID of the player in the 'passer' column. |
| `rusher_id` | String | ID of the player in the 'rusher' column. |
| `receiver_id` | String | ID of the player in the 'receiver' column. |
| `name` | String | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `jersey_number` | Int64 | Jersey number. Often useful for joins by name/team/jersey. |
| `id` | String | ID of the player in the 'name' column. |
| `fantasy_player_name` | String | Name of the rusher on rush plays or receiver on pass plays (from official stats). |
| `fantasy_player_id` | String | ID of the rusher on rush plays or receiver on pass plays (from official stats). |
| `fantasy` | String | Name of the rusher on rush plays or receiver on pass plays. |
| `fantasy_id` | String | ID of the rusher on rush plays or receiver on pass plays. |
| `out_of_bounds` | Float64 | 1 if play description contains ran ob, pushed ob, or sacked ob; 0 otherwise. |
| `home_opening_kickoff` | Float64 | 1 if the home team received the opening kickoff, 0 otherwise. |
| `ep` | Float64 | Using the scoring event probabilities, the estimated expected points with respect to the possession team for the given play. |
| `td_prob` | Float64 | Predicted probability of the posteam scoring a TD next. 'Next' in this context means the next score in the same game half. |
| `opp_td_prob` | Float64 | Predicted probability of the defteam scoring a TD next. 'Next' in this context means the next score in the same game half. |
| `fg_prob` | Float64 | Predicted probability of the posteam scoring a FG next. 'Next' in this context means the next score in the same game half. |
| `opp_fg_prob` | Float64 | Predicted probability of the defteam scoring a FG next. 'Next' in this context means the next score in the same game half. |
| `safety_prob` | Float64 | Predicted probability of the posteam scoring a safety next. 'Next' in this context means the next score in the same game half. |
| `opp_safety_prob` | Float64 | Predicted probability of the defteam scoring a safety next. 'Next' in this context means the next score in the same game half. |
| `no_score_prob` | Float64 | Predicted probability of no score occurring for the rest of the half based on the expected points model. |
| `total_home_epa` | Float64 | Cumulative total EPA for the home team in the game so far. |
| `total_away_epa` | Float64 | Cumulative total EPA for the away team in the game so far. |
| `total_home_rush_epa` | Float64 | Cumulative total rushing EPA for the home team in the game so far. |
| `total_away_rush_epa` | Float64 | Cumulative total rushing EPA for the away team in the game so far. |
| `total_home_pass_epa` | Float64 | Cumulative total passing EPA for the home team in the game so far. |
| `total_away_pass_epa` | Float64 | Cumulative total passing EPA for the away team in the game so far. |
| `qb_epa` | Float64 | Gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
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
| `era0` | Int64 | 1 for seasons through 2001, else 0 (model era feature). |
| `era1` | Int64 | 1 for seasons 2002-2005, else 0 (model era feature). |
| `era2` | Int64 | 1 for seasons 2006-2013, else 0 (model era feature). |
| `era3` | Int64 | 1 for seasons 2014-2017, else 0 (model era feature). |
| `era4` | Int64 | 1 for seasons from 2018 on, else 0 (model era feature). |
| `down1` | Int64 | 1 on a first-down play, else 0 (expected-points model feature). |
| `down2` | Int64 | 1 on a second-down play, else 0 (expected-points model feature). |
| `down3` | Int64 | 1 on a third-down play, else 0 (expected-points model feature). |
| `down4` | Int64 | 1 on a fourth-down play, else 0 (expected-points model feature). |
| `home` | Int64 | 1 when the possession team is the home team, else 0 (model feature). |
| `retractable` | Int64 | 1 only when the roof is the literal value "retractable", which nflverse does not ship, else 0 (model feature). |
| `dome` | Int64 | 1 when the game is played under a dome or a closed roof, else 0 (model feature; a null roof counts as outdoors). |
| `outdoors` | Int64 | 1 when the game is played outdoors or under an open roof, else 0 (model feature; a null roof counts as outdoors). |
| `receive_2h_ko` | Int64 | 1 on a first-half play when the possession team receives the second-half kickoff, else 0 (win-probability model feature). |
| `posteam_spread` | Float64 | Point spread from the possession team's side, nflverse spread_line for the home team and its negation for the away team. |
| `elapsed_share` | Float64 | Share of regulation elapsed at the snap, (3600 - game_seconds_remaining) / 3600 clipped to [0, 1]. |
| `spread_time` | Float64 | Win-probability model feature, posteam_spread * exp(-4 * elapsed_share), so the spread fades as the game runs down; 0 when spread_line is missing. |
| `Diff_Time_Ratio` | Float64 | Win-probability model feature, score_differential divided by exp(-4 * elapsed_share), so a lead weighs more as the game runs down. |
| `wp` | Float64 | Estimated win probability for the posteam given the current situation at the start of the given play. |
| `vegas_wp` | Float64 | Estimated win probability for the posteam given the current situation at the start of the given play, incorporating pre-game Vegas line. |
| `home_wp` | Float64 | Estimated win probability for the home team. |
| `away_wp` | Float64 | Estimated win probability for the away team. |
| `def_wp` | Float64 | Estimated win probability for the defteam. |
| `vegas_wpa` | Float64 | Win probability added (WPA) for the posteam: spread_adjusted model. |
| `wpa` | Float64 | Win probability added (WPA) for the posteam. |
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
| `cp` | Float64 | Numeric value indicating the probability for a complete pass based on comparable game situations. |
| `cpoe` | Float64 | For a single pass play this is 1 - cp when the pass was completed or 0 - cp when the pass was incomplete. Analyzed for a whole game or season an indicator for the passer how much over or under expectation his completion percentage was. |
| `xpass` | Float64 | Probability of dropback scaled from 0 to 1. |
| `pass_oe` | Float64 | Dropback percent over expected on a given play scaled from 0 to 100. |
| `xyac_epa` | Float64 | Expected value of EPA gained after the catch, starting from where the catch was made. Zero yards after the catch would be listed as zero EPA. |
| `xyac_mean_yardage` | Float64 | Average expected yards after the catch based on where the ball was caught. |
| `xyac_median_yardage` | Float64 | Median expected yards after the catch based on where the ball was caught. |
| `xyac_success` | Float64 | Probability play earns positive EPA (relative to where play started) based on where ball was caught. |
| `xyac_fd` | Float64 | Probability play earns a first down based on where the ball was caught. |
| `qbr_epa` | Float64 | The quarterback's EPA on the play as the QBR model reads it, floored at -5 and set to -3.5 on a fumble. |
| `weight` | Float64 | Official weight, in pounds |
| `non_fumble_sack` | Boolean | True for a sack with no fumble on the play (QBR model input). |
| `sack_epa` | Float64 | qbr_epa on sacks without a fumble, null otherwise (QBR model input). |
| `pass_epa` | Float64 | qbr_epa on pass plays, null otherwise (QBR model input). |
| `rush_epa` | Float64 | qbr_epa on rushing plays, null otherwise (QBR model input). |
| `pen_epa` | Float64 | qbr_epa on plays with a penalty, null otherwise (QBR model input). |
| `sack_weight` | Float64 | The play's QBR weight on sacks without a fumble, null otherwise. |
| `pass_weight` | Float64 | The play's QBR weight on pass plays, null otherwise. |
| `rush_weight` | Float64 | The play's QBR weight on rushing plays, null otherwise. |
| `pen_weight` | Float64 | The play's QBR weight on plays with a penalty, null otherwise. |
| `action_play` | Boolean | True when the play's qb_epa is non-zero, i.e. the play counts toward the QBR model's inputs. |
| `go_wp` | Float64 | Fourth downs only. Possession team's win probability if it goes for it, weighted over the distribution of yards gained. |
| `first_down_prob` | Float64 | Fourth downs only. Probability that going for it gains a first down or a touchdown. |
| `wp_succeed` | Float64 | Fourth downs only. Possession team's win probability if going for it converts. |
| `wp_fail` | Float64 | Fourth downs only. Possession team's win probability if going for it fails to convert. |
| `fg_make_prob` | Float64 | Fourth downs only. Probability a field-goal attempt from this spot is good, from the bundled fg_model. |
| `make_fg_wp` | Float64 | Fourth downs only. Possession team's win probability if the field goal is good. |
| `miss_fg_wp` | Float64 | Fourth downs only. Possession team's win probability if the field goal is missed. |
| `fg_wp` | Float64 | Fourth downs only. Possession team's win probability if it attempts a field goal, the make and miss outcomes weighted by fg_make_prob. |
| `punt_wp` | Float64 | Fourth downs only. Possession team's win probability if it punts, weighted over the punt landing distribution. |
| `go_boost` | Float64 | Fourth downs only. Percentage points of win probability gained by going for it over the better of kicking and punting, 100 * (go_wp - max(fg_wp, punt_wp)) (nfl4th's headline number). |
| `go_wp_diff` | Float64 | Fourth downs only. go_wp minus the recommended option's win probability; 0 when going for it is recommended, negative otherwise. |
| `punt_wp_diff` | Float64 | Fourth downs only. punt_wp minus the recommended option's win probability; 0 when punting is recommended, negative otherwise. |
| `fg_wp_diff` | Float64 | Fourth downs only. fg_wp minus the recommended option's win probability; 0 when kicking is recommended, negative otherwise. |
| `fourth_down_recommendation` | String | Fourth downs only. The option with the highest win probability: "go", "field_goal" or "punt". |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | When `method` is not `"lead_diff"`, or required contract columns are absent from `df`. |
| `FileNotFoundError` | In strict mode, when a model cannot be obtained (the message names the artifact). |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import enrich_nfl_pbp

pbp = load_nfl_pbp([2023])
enriched = enrich_nfl_pbp(pbp)
print(enriched.select("ep", "epa", "wp", "wpa").head())

# Pandas output

from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import enrich_nfl_pbp

enriched_pd = enrich_nfl_pbp(load_nfl_pbp([2023]), return_as_pandas=True)
print(enriched_pd[["ep", "epa", "wp", "wpa"]].head())

# Pipeline next step

enriched.filter(pl.col("play_type") == "pass").select("posteam", "epa", "cp", "cpoe").head()
```

### env_adjusted_make_prob {#env_adjusted_make_prob}

`env_adjusted_make_prob(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Add `base_make_prob` + environment-adjusted `exp_make_prob`.

`exp_make_prob = sigmoid(logit(base) + b_wind*wind + b_temp*(temp-baseline)
+ b_alt*altitude_kft)` with coefficients from
`sportsdataverse.nfl.nfl_scheme_constants.ENVIRONMENT_FG_COEF` and
altitude from `STADIUM_ALTITUDE[home_team]`.  Dome / closed-roof kicks
(and missing readings) are treated as neutral (wind 0, temp = baseline).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | FG-attempt rows with `yardline_100` / `roof` / `temp` / `wind` / `home_team` (+ `season` or `era0..era4` / `fg_roof`). |

**Returns**

The input plus `base_make_prob` and `exp_make_prob` (Float64).

| col_name | type | description |
|---|---|---|
| `base_make_prob` | Float64 | Shipped fg_model make probability (with nfl4th long-kick clamps applied). |
| `exp_make_prob` | Float64 | Environment-adjusted make probability (logit shift for long-kick clamp correction, wind, temperature and altitude). |

**Example**

```python
import polars as pl
from sportsdataverse.nfl.nfl_kicker_rating import env_adjusted_make_prob
fg = pl.read_parquet("tests/fixtures/nfl_scheme/fg_attempts_2019_2023.parquet")
out = env_adjusted_make_prob(fg)
print(out.select("base_make_prob", "exp_make_prob").describe())
```

### fg_make_probability {#fg_make_probability}

`fg_make_probability(yardline_100: 'np.ndarray', fg_roof: 'np.ndarray', era: 'np.ndarray') -> 'Optional[np.ndarray]'`

Predict FG make probability from the bundled `fg_model` (public wrapper).

Thin supported alias over the private underscore-prefixed helper so downstream
consumers (e.g. the kicker-rating spine) reuse the shipped model through a
public import instead of a private reach.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `yardline_100` | `ndarray` |  | Kick spot (yards from the opponent end zone); the attempt distance is `yardline_100 + 18`. |
| `fg_roof` | `ndarray` |  | 1.0 when `roof == "outdoors"` else 0.0, per kick. |
| `era` | `ndarray` |  | `(n, 5)` one-hot era matrix (`era0`..`era4`, season cuts 2001/2005/2013/2017). |

**Returns**

Make probabilities (with nfl4th's long-kick clamps), or `None` when the bundled model is unavailable.

**Example**

```python
import numpy as np
from sportsdataverse.nfl.nfl_fourth_down import fg_make_probability
p = fg_make_probability(
    np.array([30.0]), np.array([1.0]),
    np.array([[0.0, 0.0, 0.0, 0.0, 1.0]]),
)
print(p)
```

### fit_nfl_field_position_ep {#fit_nfl_field_position_ep}

`fit_nfl_field_position_ep(pbp: 'pl.DataFrame', *, exclude_garbage: 'bool' = True) -> 'pl.DataFrame'`

Fit the NFL EP-by-starting-yardline curve from released `espn_nfl_pbp` plays.

Extracts one row per drive (starting yard line from the offense's own
goal, realized drive points) and fits the monotone curve with
`sportsdataverse.cfb.cfb_field_position.fit_field_position_ep` --
the same estimator and target the college curve uses. This is how the
bundled artifact was produced; re-run it on newer seasons to refresh it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | plays in the released `espn_nfl_pbp` shape (any number of seasons concatenated). Needs the drive fields (`drive.id`, `drive.result`, `drive.start.yardLine`), `homeTeamId`, `period` and `start.pos_team.id` / `start.def_pos_team.id`. |
| `exclude_garbage` | `bool` | `True` | drop drives that start in garbage time. |

**Returns**

`yardline_own: Int64 (1..99), ep: Float64` -- monotone non-decreasing. Empty input returns a zero-row frame.

No returns table is published for this function: no capture: it raises DuplicateError ('down') on real play-by-play, because it renames start.down to down even when down already exists.

**Example**

```python
import polars as pl
from sportsdataverse.nfl import fit_nfl_field_position_ep
pbp = pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")
curve = fit_nfl_field_position_ep(pbp)
curve.write_parquet("nfl_field_position_ep.parquet")
```

### get_2pt_probs {#get_2pt_probs}

`get_2pt_probs(pbp_df: "Union[pl.DataFrame, 'pd.DataFrame']") -> 'pd.DataFrame'`

The PAT-vs-2pt decision surface for post-touchdown states (CFB-shaped).

The NFL twin of `sportsdataverse.cfb.cfb_two_point.get_2pt_probs`. It
runs the same three-outcome enumeration `get_2pt_wp` uses, but returns
the decision columns instead of folding them into `wp_td`. The two option

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Union[DataFrame, 'DataFrame']` |  | Post-touchdown states in nflverse column space (the same inputs `get_4th_down_probs` takes; `score_differential` is the scoring team's lead **after** the six points). Prepared frames are accepted as-is. |

**Returns**

A pandas frame with `go_index` plus `two_pt_wp`, `xp_wp`, `prob_2pt`, `two_pt_recommendation` (`"go_for_2"` iff `two_pt_wp > xp_wp` else `"kick_xp"`) and `two_pt_wp_diff` (`two_pt_wp - xp_wp`). All NaN / null when the models are unavailable.

| col_name | type | description |
|---|---|---|
| `go_index` | Int64 |  |
| `two_pt_wp` | Float64 |  |
| `xp_wp` | Float64 |  |
| `prob_2pt` | Float64 |  |
| `two_pt_wp_diff` | Float64 |  |
| `two_pt_recommendation` | String |  |

**Raises**

| Exception | Description |
|---|---|
| `FileNotFoundError` | `SDV_PY_MODELS_STRICT=1` and an nfl4th model cannot be obtained. |
| `XGBoostError` | A cached nfl4th booster is corrupt. |
| `ValueError` | A model card beside a booster disagrees with the applier's fitted constants. |

**Example**

```python
from sportsdataverse.nfl.nfl_fourth_down import get_2pt_probs
out = get_2pt_probs(touchdown_states)
print(out[["two_pt_wp", "xp_wp", "two_pt_recommendation"]].head())
```

### get_2pt_wp {#get_2pt_wp}

`get_2pt_wp(pbp_df: "Union[pl.DataFrame, 'pd.DataFrame']") -> 'pd.DataFrame'`

Win probability of the PAT-vs-2pt choice after a touchdown (nfl4th `get_2pt_wp`).

For each row, scores the post-touchdown state under three scoring outcomes
(0 / 1 / 2 added points) from the kicking-off team's ensuing-drive WP, and
combines them with the 2-pt conversion probability (`two_pt_model`) and the
PAT make probability (the FG model at `yardline_100 = 15`) into `wp_td` —
the better of go-for-2 and kick-the-PAT.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Union[DataFrame, 'DataFrame']` |  | Play-by-play frame (polars or pandas) of post-touchdown states, already carrying the prepared state columns (see module docstring). |

**Returns**

A pandas frame with `go_index`, `yardline_100` (always 0) and `wp_td`. `wp_td` is NaN when the WP / 2-pt models are unavailable.

No returns table is published for this function: no capture: it never prepares its input, so it raises KeyError (era3) on real play-by-play.

**Raises**

| Exception | Description |
|---|---|
| `FileNotFoundError` | `SDV_PY_MODELS_STRICT=1` and an nfl4th model cannot be obtained. |
| `XGBoostError` | A cached nfl4th booster is corrupt. |
| `ValueError` | A model card beside a booster disagrees with the applier's fitted constants. |

**Example**

```python
from sportsdataverse.nfl.nfl_fourth_down import get_2pt_wp
out = get_2pt_wp(touchdown_states)
print(out[["go_index", "wp_td"]].head())
```
