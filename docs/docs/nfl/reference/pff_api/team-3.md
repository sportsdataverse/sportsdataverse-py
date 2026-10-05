---
title: "NFL — PFF Developer API (api.pff.com, API key) — Team: report"
sidebar_label: "Team: report"
sidebar_position: 16
description: "NFL — PFF Developer API (api.pff.com, API key) — Team: report — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — PFF Developer API (api.pff.com, API key) — Team: report

## pff_api_team_report

One of nineteen player reports for a team, one row per player

**Endpoint URL:** `GET https://api.pff.com/v2/{league}/teams/{team}/reports/{report}`

**Valid URL:** [https://api.pff.com/v2/nfl/teams/cincinnati-bengals/reports/offense?season=2022](https://api.pff.com/v2/nfl/teams/cincinnati-bengals/reports/offense?season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug: nfl or ncaa. |
| `team` | `team` |  | `Y` |  | The team, as its slug (los-angeles-rams) or its numeric franchise id (26) — both resolve through the league's team directory for the season, and both produce the same answer. |
| `report` | `report` |  | `Y` |  | Which report: offense, passing, passing-depth, passing-pressure, receiving, receiving-depth, rushing, blocking, pass-blocking, run-blocking, defense, run-defense, pass-rush, coverage, special-teams, kick-returns, field-goals, punting, kickoffs. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year: 2006 or later, and at most one year past the current season. |
| `weekGroup` | `week_group` |  |  | `Y` | Which part of the season to cover: REG (regular season), PO (playoffs) or REGPO (both, the default). |
| `week` | `week` |  |  | `Y` | Narrow the report to one week of the weekGroup — or, with weekTo, to a span of weeks. |
| `weekTo` | `week_to` |  |  | `Y` | The last week of a span that starts at week; requires week. |

### Returns {#pff_api_team_report-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` whose columns depend on `report` (one table per value below); pass `return_as_pandas=True` for a `pandas.DataFrame`.

**offense**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `player` | character | Player's display name as PFF lists it (e.g. "Tom Brady"). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P, LS). |
| `declined_penalties` | integer | Penalties committed by the player that were declined. |
| `draft_season` | integer | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | integer | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | integer | PFF franchise (team) id (integer join key). |
| `grades_offense` | numeric | PFF overall offense grade (0-100). |
| `grades_offense_penalty` | numeric | PFF offensive penalty grade, 0-100. |
| `grades_pass_block` | numeric | PFF pass-blocking grade (0-100). |
| `grades_run_block` | numeric | PFF run-blocking grade (0-100). |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `penalties` | integer | Penalties charged to the player over the covered span (see declined_penalties for the ones that were declined). |
| `player_game_count` | integer | Number of games the player appeared in over the covered span. |
| `snap_counts_pass` | integer | Pass-play snaps spent as the passer, rather than blocking or running a route. |
| `snap_counts_pass_block` | integer | Pass-blocking snaps played. |
| `snap_counts_pass_route` | integer | Snaps spent running a pass route. |
| `snap_counts_run` | integer | Run-play snaps: on the offense report, run plays on which the player was the runner rather than a run blocker; on the run-defense report, run-defense snaps played. |
| `snap_counts_run_block` | integer | Run-blocking snaps played. |
| `snap_counts_total` | integer | Total offensive snaps played. |
| `snap_counts_total_pass` | integer | Total pass-play snaps across passing, pass blocking, and route running. |
| `snap_counts_total_run` | integer | Total run-play snaps across rushing and run blocking. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `team_name` | character | Team name/abbreviation the player is credited to for the range. |
| `grades_hands_fumble` | numeric | PFF ball-security (hands/fumble) grade, 0-100. |
| `grades_pass` | numeric | PFF passing grade (0-100). |
| `grades_pass_route` | numeric | PFF receiving/route grade (0-100). |
| `grades_run` | numeric | PFF rushing grade (0-100). |

**passing**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `player` | character | Player's display name as PFF lists it (e.g. "Tom Brady"). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P, LS). |
| `grades_offense` | numeric | PFF overall offense grade (0-100). |
| `twp_rate` | numeric | Turnover-worthy-play rate. |
| `btt_rate` | numeric | Big-time-throw rate. |
| `spikes` | integer | Clock-stopping spike plays. |
| `dropbacks` | integer | Total quarterback dropbacks. |
| `thrown_aways` | integer | Passes intentionally thrown away. |
| `draft_season` | integer | Season of the player's NFL draft class, per PFF. |
| `team_name` | character | Team name/abbreviation the player is credited to for the range. |
| `grades_pass` | numeric | PFF passing grade (0-100). |
| `hit_as_threw` | integer | Plays where the quarterback was hit as he threw. |
| `first_downs` | integer | First downs gained: passing first downs on the passing report, first-down receptions on receiving, rushing first downs on rushing. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `sack_percent` | numeric | Sack rate (sacks per dropback). |
| `bats` | integer | Passes batted at the line. |
| `sacks` | integer | Sacks: recorded by the player on the defense and pass-rush reports; times the passer was sacked on the passing report. |
| `player_game_count` | integer | Number of games the player appeared in over the covered span. |
| `eligible_season` | integer | Season of the player's NFL draft eligibility, per PFF. |
| `completions` | integer | Completed passes by the passer. |
| `yards` | integer | Yards in the report's play family: passing yards (passing), receiving yards (receiving), rushing yards (rushing), gross punt yards (punting), and receiving yards allowed into the player's coverage (coverage, defense). |
| `accuracy_percent` | numeric | Charted accuracy percentage. |
| `scrambles` | integer | Quarterback scrambles (pass plays on which the passer ran with the ball). |
| `interceptions` | integer | Interceptions: thrown by the passer on the passing report, made by the player on the coverage and defense reports, and on passes targeting the player on the receiving report. |
| `drop_rate` | numeric | Drop rate, in percent: share of catchable passes dropped by the passer's receivers (passing report) or by the player (receiving report). |
| `grades_run` | numeric | PFF rushing grade (0-100). |
| `qb_rating` | numeric | NFL passer rating. |
| `completion_percent` | integer | Completion percentage. |
| `penalties` | integer | Penalties charged to the player over the covered span (see declined_penalties for the ones that were declined). |
| `attempts` | integer | Attempts in the report's play family: pass attempts on the passing report, carries on rushing, punts on punting and kickoffs on the kickoffs report. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | integer | Penalties committed by the player that were declined. |
| `passing_snaps` | integer | Number of passing snaps played. |
| `pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack. |
| `ypa` | numeric | Yards per attempt: passing yards per pass attempt on the passing report, rushing yards per carry on the rushing report. |
| `drops` | integer | Dropped passes: drops by the passer's receivers on the passing report, drops by the player on the receiving and rushing reports. |
| `grades_hands_fumble` | numeric | PFF ball-security (hands/fumble) grade, 0-100. |
| `avg_time_to_throw` | numeric | Average time to throw, in seconds from snap to release, on the passer's attempts. |
| `big_time_throws` | integer | Number of big-time throws, per PFF's highest-value, highest-difficulty throw designation. |
| `positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added. |
| `franchise_id` | integer | PFF franchise (team) id (integer join key). |
| `avg_depth_of_target` | numeric | Average depth of target, in yards downfield: of the passer's throws (passing), of passes to the player (receiving), or of targets into the player's coverage (coverage). |
| `turnover_worthy_plays` | integer | Number of turnover-worthy plays, plays PFF charts as deserving of a turnover. |
| `epa` | numeric | Expected points added per play on the passer's dropbacks, as computed by PFF (an average such as 0.04, not a season total). |
| `aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways), as charted by PFF. |
| `touchdowns` | integer | Touchdowns: passing touchdowns thrown (passing), receiving and rushing touchdowns scored (receiving, rushing), or touchdowns allowed into the player's coverage (coverage, defense). |
| `def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks, as charted by PFF. |
| `npa_epa` | numeric | Expected points added per play on the passer's non-play-action dropbacks, as computed by PFF (npa_ = non-play-action). |
| `npa_positive_epa_percent` | numeric | Percentage of the passer's non-play-action dropbacks with positive expected points added. |
| `no_screen_epa` | numeric | Expected points added per play on the passer's non-screen dropbacks, as computed by PFF. |
| `no_screen_positive_epa_percent` | numeric | Percentage of the passer's non-screen dropbacks with positive expected points added. |
| `pass_rate_oe` | numeric | Pass rate over expectation on the passer's plays, in percentage points (actual minus PFF-expected pass rate). |
| `completion_oe` | numeric | Completion percentage over expectation, in percentage points (actual minus PFF-expected completion rate). |
| `accuracy_oe` | numeric | Accuracy percentage over expectation, in percentage points (PFF-charted accuracy minus its expected value). |
| `offense_pos_graded_rate` | numeric | Percentage of the player's offensive plays that PFF graded positively. |
| `offense_neg_graded_rate` | numeric | Percentage of the player's offensive plays that PFF graded negatively. |

**passing-depth**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `player` | character | Player's display name as PFF lists it (e.g. "Tom Brady"). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P, LS). |
| `left_behind_los_accuracy_percent` | integer | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the left side of the field. |
| `left_short_scrambles` | integer | Number of scrambles on short (0-9 air yards) throws to the left side of the field. |
| `center_short_first_downs` | integer | Number of passing first downs gained on short (0-9 air yards) throws to the center of the field. |
| `right_short_bats` | integer | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the right side of the field. |
| `left_behind_los_completions` | integer | Number of completed passes on throws behind the line of scrimmage to the left side of the field. |
| `left_medium_dropbacks` | integer | Number of dropbacks on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the right side of the field. |
| `right_medium_hit_as_threw` | integer | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage, expressed as a percentage. |
| `right_short_turnover_worthy_plays` | integer | Number of turnover-worthy plays on short (0-9 air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `right_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the right side of the field. |
| `deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws, per PFF charting. |
| `center_medium_sacks` | integer | Number of sacks taken on medium (10-19 air yards) throws to the center of the field. |
| `medium_interceptions` | integer | Interceptions on medium (10-19 air yards) throws: thrown by the passer (passing-depth) or on passes targeting the player (receiving-depth). |
| `left_deep_completions` | integer | Number of completed passes on deep (20+ air yards) throws to the left side of the field. |
| `behind_los_spikes` | integer | Number of clock-stopping spikes on throws behind the line of scrimmage. |
| `medium_aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws, as charted by PFF. |
| `center_behind_los_drops` | integer | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_interceptions` | integer | Number of passes intercepted on throws behind the line of scrimmage to the center of the field. |
| `behind_los_dropbacks` | integer | Number of dropbacks on throws behind the line of scrimmage. |
| `left_behind_los_interceptions` | integer | Number of passes intercepted on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_first_downs` | integer | Number of passing first downs gained on deep (20+ air yards) throws to the left side of the field. |
| `deep_passing_snaps` | integer | Number of passing snaps played on deep (20+ air yards) throws. |
| `center_short_btt_rate` | integer | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the center of the field, per PFF charting. |
| `center_medium_thrown_aways` | integer | Number of intentional throwaways on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_avg_depth_of_target` | integer | Average depth of target in air yards on deep (20+ air yards) throws to the center of the field. |
| `center_short_drops` | integer | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the center of the field. |
| `center_deep_first_downs` | integer | Number of passing first downs gained on deep (20+ air yards) throws to the center of the field. |
| `left_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_attempts` | integer | Number of pass attempts on deep (20+ air yards) throws to the center of the field. |
| `center_behind_los_attempts` | integer | Number of pass attempts on throws behind the line of scrimmage to the center of the field. |
| `right_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the right side of the field. |
| `center_deep_aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `right_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the right side of the field. |
| `right_medium_big_time_throws` | integer | Number of big-time throws on medium (10-19 air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_sack_percent` | integer | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws. |
| `center_medium_pressure_to_sack_rate` | integer | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the center of the field. |
| `deep_sacks` | integer | Number of sacks taken on deep (20+ air yards) throws. |
| `right_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the right side of the field. |
| `left_deep_drop_rate` | integer | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the left side of the field. |
| `right_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the right side of the field, expressed as a percentage. |
| `right_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the right side of the field, per PFF charting. |
| `center_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the center of the field, expressed as a percentage. |
| `deep_drops` | integer | Catchable passes dropped on deep (20+ air yards) throws: by the passer's receivers (passing-depth) or by the player (receiving-depth). |
| `center_behind_los_big_time_throws` | integer | Number of big-time throws on throws behind the line of scrimmage to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_behind_los_touchdowns` | integer | Number of passing touchdowns thrown on throws behind the line of scrimmage to the center of the field. |
| `right_behind_los_twp_rate` | integer | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the right side of the field, per PFF charting. |
| `center_deep_completions` | integer | Number of completed passes on deep (20+ air yards) throws to the center of the field. |
| `center_short_attempts` | integer | Number of pass attempts on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_sack_percent` | integer | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the left side of the field. |
| `center_short_pressure_to_sack_rate` | integer | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the center of the field. |
| `deep_touchdowns` | integer | Touchdowns on deep (20+ air yards) throws: thrown by the passer (passing-depth) or scored by the player (receiving-depth). |
| `center_behind_los_accuracy_percent` | integer | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the center of the field. |
| `center_deep_drop_rate` | integer | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the center of the field. |
| `right_short_first_downs` | integer | Number of passing first downs gained on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_first_downs` | integer | Number of passing first downs gained on throws behind the line of scrimmage to the right side of the field. |
| `short_interceptions` | integer | Interceptions on short (0-9 air yards) throws: thrown by the passer (passing-depth) or on passes targeting the player (receiving-depth). |
| `center_medium_scrambles` | integer | Number of scrambles on medium (10-19 air yards) throws to the center of the field. |
| `left_behind_los_sacks` | integer | Number of sacks taken on throws behind the line of scrimmage to the left side of the field. |
| `left_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the left side of the field. |
| `left_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the left side of the field. |
| `right_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the right side of the field. |
| `draft_season` | integer | Season of the player's NFL draft class, per PFF. |
| `right_behind_los_dropbacks` | integer | Number of dropbacks on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage, as charted by PFF. |
| `right_short_thrown_aways` | integer | Number of intentional throwaways on short (0-9 air yards) throws to the right side of the field. |
| `deep_def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws, as charted by PFF. |
| `right_short_btt_rate` | integer | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the right side of the field, per PFF charting. |
| `center_behind_los_positive_epa_percent` | integer | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the left side of the field. |
| `deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws. |
| `center_short_turnover_worthy_plays` | integer | Number of turnover-worthy plays on short (0-9 air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `center_behind_los_scrambles` | integer | Number of scrambles on throws behind the line of scrimmage to the center of the field. |
| `right_short_aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `center_short_dropbacks` | integer | Number of dropbacks on short (0-9 air yards) throws to the center of the field. |
| `medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws. |
| `right_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the center of the field. |
| `medium_attempts` | integer | Number of pass attempts on medium (10-19 air yards) throws. |
| `right_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_scrambles` | integer | Number of scrambles on throws behind the line of scrimmage. |
| `center_behind_los_completion_percent` | integer | Percentage of pass attempts completed on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_btt_rate` | integer | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the left side of the field, per PFF charting. |
| `right_behind_los_btt_rate` | integer | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the right side of the field, per PFF charting. |
| `team_name` | character | Team name/abbreviation the player is credited to for the range. |
| `short_touchdowns` | integer | Touchdowns on short (0-9 air yards) throws: thrown by the passer (passing-depth) or scored by the player (receiving-depth). |
| `center_medium_drops` | integer | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the center of the field. |
| `left_behind_los_aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `deep_attempts` | integer | Number of pass attempts on deep (20+ air yards) throws. |
| `right_behind_los_sack_percent` | integer | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the right side of the field. |
| `center_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the center of the field. |
| `deep_avg_depth_of_target` | numeric | Average depth of target, in air yards, on deep (20+ air yards) throws: of the passer's throws (passing-depth) or the player's targets (receiving-depth). |
| `left_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the left side of the field, per PFF charting. |
| `medium_scrambles` | integer | Number of scrambles on medium (10-19 air yards) throws. |
| `center_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the center of the field. |
| `short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws, expressed as a percentage. |
| `right_behind_los_sacks` | integer | Number of sacks taken on throws behind the line of scrimmage to the right side of the field. |
| `center_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the center of the field. |
| `left_short_pressure_to_sack_rate` | integer | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the left side of the field. |
| `center_deep_sack_percent` | integer | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the center of the field. |
| `center_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the center of the field. |
| `left_behind_los_hit_as_threw` | integer | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `medium_big_time_throws` | integer | Number of big-time throws on medium (10-19 air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_thrown_aways` | integer | Number of intentional throwaways on deep (20+ air yards) throws. |
| `right_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the right side of the field. |
| `left_deep_turnover_worthy_plays` | integer | Number of turnover-worthy plays on deep (20+ air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_medium_bats` | integer | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the center of the field. |
| `right_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the right side of the field. |
| `right_deep_spikes` | integer | Number of clock-stopping spikes on deep (20+ air yards) throws to the right side of the field. |
| `left_deep_passing_snaps` | integer | Number of passing snaps played on deep (20+ air yards) throws to the left side of the field. |
| `center_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the center of the field, per PFF charting. |
| `right_behind_los_passing_snaps` | integer | Number of passing snaps played on throws behind the line of scrimmage to the right side of the field. |
| `left_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the left side of the field. |
| `right_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the right side of the field. |
| `left_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the left side of the field. |
| `left_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_attempts` | integer | Number of pass attempts on deep (20+ air yards) throws to the right side of the field. |
| `left_deep_spikes` | integer | Number of clock-stopping spikes on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_dropbacks` | integer | Number of dropbacks on throws behind the line of scrimmage to the center of the field. |
| `right_deep_big_time_throws` | integer | Number of big-time throws on deep (20+ air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `medium_hit_as_threw` | integer | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws, as charted by PFF. |
| `right_short_dropbacks` | integer | Number of dropbacks on short (0-9 air yards) throws to the right side of the field. |
| `medium_def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws, as charted by PFF. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `left_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the left side of the field. |
| `left_short_attempts` | integer | Number of pass attempts on short (0-9 air yards) throws to the left side of the field. |
| `center_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the center of the field, per PFF charting. |
| `right_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the left side of the field. |
| `left_behind_los_dropbacks` | integer | Number of dropbacks on throws behind the line of scrimmage to the left side of the field. |
| `deep_dropbacks` | integer | Number of dropbacks on deep (20+ air yards) throws. |
| `right_behind_los_scrambles` | integer | Number of scrambles on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_interceptions` | integer | Interceptions on throws behind the line of scrimmage: thrown by the passer (passing-depth) or on passes targeting the player (receiving-depth). |
| `right_deep_drops` | integer | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the right side of the field. |
| `right_deep_yards` | integer | Passing yards gained on deep (20+ air yards) throws to the right side of the field. |
| `right_short_hit_as_threw` | integer | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `right_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the right side of the field. |
| `short_bats` | integer | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws. |
| `right_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the right side of the field, per PFF charting. |
| `right_behind_los_pressure_to_sack_rate` | integer | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the right side of the field. |
| `deep_spikes` | integer | Number of clock-stopping spikes on deep (20+ air yards) throws. |
| `center_medium_spikes` | integer | Number of clock-stopping spikes on medium (10-19 air yards) throws to the center of the field. |
| `behind_los_aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage, as charted by PFF. |
| `right_behind_los_touchdowns` | integer | Number of passing touchdowns thrown on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_pressure_to_sack_rate` | integer | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the right side of the field. |
| `medium_thrown_aways` | integer | Number of intentional throwaways on medium (10-19 air yards) throws. |
| `short_sacks` | integer | Number of sacks taken on short (0-9 air yards) throws. |
| `center_behind_los_twp_rate` | integer | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the center of the field, per PFF charting. |
| `left_behind_los_attempts` | integer | Number of pass attempts on throws behind the line of scrimmage to the left side of the field. |
| `short_aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws, as charted by PFF. |
| `short_completions` | integer | Number of completed passes on short (0-9 air yards) throws. |
| `right_short_pressure_to_sack_rate` | integer | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the right side of the field. |
| `medium_avg_depth_of_target` | numeric | Average depth of target, in air yards, on medium (10-19 air yards) throws: of the passer's throws (passing-depth) or the player's targets (receiving-depth). |
| `left_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the left side of the field. |
| `center_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the center of the field, expressed as a percentage. |
| `left_medium_touchdowns` | integer | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the left side of the field. |
| `center_short_bats` | integer | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the center of the field. |
| `left_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_passing_snaps` | integer | Number of passing snaps played on throws behind the line of scrimmage to the center of the field. |
| `center_short_yards` | integer | Passing yards gained on short (0-9 air yards) throws to the center of the field. |
| `right_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the right side of the field, per PFF charting. |
| `right_medium_scrambles` | integer | Number of scrambles on medium (10-19 air yards) throws to the right side of the field. |
| `medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws, per PFF charting. |
| `center_behind_los_btt_rate` | integer | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the center of the field, per PFF charting. |
| `center_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the center of the field. |
| `left_medium_sacks` | integer | Number of sacks taken on medium (10-19 air yards) throws to the left side of the field. |
| `right_short_yards` | integer | Passing yards gained on short (0-9 air yards) throws to the right side of the field. |
| `left_medium_first_downs` | integer | Number of passing first downs gained on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the right side of the field. |
| `left_short_big_time_throws` | integer | Number of big-time throws on short (0-9 air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_turnover_worthy_plays` | integer | Number of turnover-worthy plays on deep (20+ air yards) throws, plays PFF charts as deserving of a turnover. |
| `left_deep_bats` | integer | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the left side of the field. |
| `left_medium_turnover_worthy_plays` | integer | Number of turnover-worthy plays on medium (10-19 air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `base_dropbacks` | integer | Number of dropbacks across all splits, the baseline total for this facet. |
| `center_deep_drops` | integer | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the center of the field. |
| `center_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the center of the field. |
| `right_short_attempts` | integer | Number of pass attempts on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the center of the field. |
| `right_short_sack_percent` | integer | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the right side of the field. |
| `left_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the left side of the field. |
| `center_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_sack_percent` | integer | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the center of the field. |
| `medium_completions` | integer | Number of completed passes on medium (10-19 air yards) throws. |
| `medium_sack_percent` | integer | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws. |
| `left_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the left side of the field. |
| `deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws, per PFF charting. |
| `left_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the left side of the field. |
| `short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws, per PFF charting. |
| `short_passing_snaps` | integer | Number of passing snaps played on short (0-9 air yards) throws. |
| `left_deep_sacks` | integer | Number of sacks taken on deep (20+ air yards) throws to the left side of the field. |
| `short_scrambles` | integer | Number of scrambles on short (0-9 air yards) throws. |
| `center_medium_turnover_worthy_plays` | integer | Number of turnover-worthy plays on medium (10-19 air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `behind_los_drop_rate` | numeric | Percentage of catchable passes dropped on throws behind the line of scrimmage: by the passer's receivers (passing-depth) or by the player (receiving-depth). |
| `right_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws. |
| `player_game_count` | integer | Number of games the player appeared in over the covered span. |
| `short_btt_rate` | integer | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws, per PFF charting. |
| `right_short_touchdowns` | integer | Number of passing touchdowns thrown on short (0-9 air yards) throws to the right side of the field. |
| `center_short_completions` | integer | Number of completed passes on short (0-9 air yards) throws to the center of the field. |
| `right_short_spikes` | integer | Number of clock-stopping spikes on short (0-9 air yards) throws to the right side of the field. |
| `behind_los_first_downs` | integer | First downs on throws behind the line of scrimmage: passing first downs (passing-depth) or first-down receptions by the player (receiving-depth). |
| `right_medium_def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `deep_pressure_to_sack_rate` | integer | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws. |
| `medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws. |
| `short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws. |
| `right_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the right side of the field, expressed as a percentage. |
| `eligible_season` | integer | Season of the player's NFL draft eligibility, per PFF. |
| `short_big_time_throws` | integer | Number of big-time throws on short (0-9 air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `behind_los_attempts` | integer | Number of pass attempts on throws behind the line of scrimmage. |
| `center_behind_los_yards` | integer | Passing yards gained on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_bats` | integer | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the center of the field. |
| `right_deep_pressure_to_sack_rate` | integer | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the right side of the field. |
| `center_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_bats` | integer | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the left side of the field. |
| `center_deep_passing_snaps` | integer | Number of passing snaps played on deep (20+ air yards) throws to the center of the field. |
| `right_behind_los_hit_as_threw` | integer | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `medium_bats` | integer | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws. |
| `right_medium_first_downs` | integer | Number of passing first downs gained on medium (10-19 air yards) throws to the right side of the field. |
| `medium_spikes` | integer | Number of clock-stopping spikes on medium (10-19 air yards) throws. |
| `left_deep_thrown_aways` | integer | Number of intentional throwaways on deep (20+ air yards) throws to the left side of the field. |
| `right_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the right side of the field. |
| `center_medium_hit_as_threw` | integer | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `left_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the left side of the field. |
| `center_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the center of the field. |
| `right_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the right side of the field. |
| `left_short_yards` | integer | Passing yards gained on short (0-9 air yards) throws to the left side of the field. |
| `deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws. |
| `right_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the right side of the field. |
| `center_behind_los_thrown_aways` | integer | Number of intentional throwaways on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the center of the field, expressed as a percentage. |
| `right_deep_first_downs` | integer | Number of passing first downs gained on deep (20+ air yards) throws to the right side of the field. |
| `left_short_completions` | integer | Number of completed passes on short (0-9 air yards) throws to the left side of the field. |
| `deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws. |
| `left_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the left side of the field. |
| `right_deep_sack_percent` | integer | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_pressure_to_sack_rate` | integer | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage. |
| `right_deep_touchdowns` | integer | Number of passing touchdowns thrown on deep (20+ air yards) throws to the right side of the field. |
| `left_behind_los_def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `left_behind_los_drops` | integer | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the left side of the field. |
| `deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws. |
| `left_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the left side of the field. |
| `medium_touchdowns` | integer | Touchdowns on medium (10-19 air yards) throws: thrown by the passer (passing-depth) or scored by the player (receiving-depth). |
| `left_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the left side of the field. |
| `right_behind_los_spikes` | integer | Number of clock-stopping spikes on throws behind the line of scrimmage to the right side of the field. |
| `medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws. |
| `behind_los_passing_snaps` | integer | Number of passing snaps played on throws behind the line of scrimmage. |
| `left_short_first_downs` | integer | Number of passing first downs gained on short (0-9 air yards) throws to the left side of the field. |
| `center_short_passing_snaps` | integer | Number of passing snaps played on short (0-9 air yards) throws to the center of the field. |
| `center_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the center of the field. |
| `right_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the right side of the field. |
| `right_medium_sack_percent` | integer | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the right side of the field. |
| `right_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the right side of the field. |
| `right_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_interceptions` | integer | Number of passes intercepted on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_yards` | integer | Yards on throws behind the line of scrimmage: passing yards (passing-depth) or the player's receiving yards (receiving-depth). |
| `center_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the center of the field. |
| `left_deep_dropbacks` | integer | Number of dropbacks on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_spikes` | integer | Number of clock-stopping spikes on throws behind the line of scrimmage to the center of the field. |
| `right_behind_los_aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `left_short_interceptions` | integer | Number of passes intercepted on short (0-9 air yards) throws to the left side of the field. |
| `right_medium_interceptions` | integer | Number of passes intercepted on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_yards` | integer | Passing yards gained on throws behind the line of scrimmage to the left side of the field. |
| `center_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the center of the field, per PFF charting. |
| `medium_first_downs` | integer | First downs on medium (10-19 air yards) throws: passing first downs (passing-depth) or first-down receptions by the player (receiving-depth). |
| `left_short_avg_depth_of_target` | integer | Average depth of target in air yards on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_ypa` | integer | Yards gained per pass attempt on throws behind the line of scrimmage to the left side of the field. |
| `short_attempts` | integer | Number of pass attempts on short (0-9 air yards) throws. |
| `right_medium_bats` | integer | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_touchdowns` | integer | Number of passing touchdowns thrown on throws behind the line of scrimmage to the left side of the field. |
| `right_medium_dropbacks` | integer | Number of dropbacks on medium (10-19 air yards) throws to the right side of the field. |
| `short_turnover_worthy_plays` | integer | Number of turnover-worthy plays on short (0-9 air yards) throws, plays PFF charts as deserving of a turnover. |
| `right_deep_thrown_aways` | integer | Number of intentional throwaways on deep (20+ air yards) throws to the right side of the field. |
| `right_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the right side of the field. |
| `right_short_qb_rating` | integer | Traditional NFL passer rating on short (0-9 air yards) throws to the right side of the field. |
| `medium_sacks` | integer | Number of sacks taken on medium (10-19 air yards) throws. |
| `center_deep_attempts_percent` | integer | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the center of the field, expressed as a percentage. |
| `left_short_dropbacks` | integer | Number of dropbacks on short (0-9 air yards) throws to the left side of the field. |
| `behind_los_drops` | integer | Catchable passes dropped on throws behind the line of scrimmage: by the passer's receivers (passing-depth) or by the player (receiving-depth). |
| `short_sack_percent` | integer | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws. |
| `left_medium_spikes` | integer | Number of clock-stopping spikes on medium (10-19 air yards) throws to the left side of the field. |
| `left_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the left side of the field. |
| `behind_los_completions` | integer | Number of completed passes on throws behind the line of scrimmage. |
| `behind_los_sack_percent` | integer | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage. |
| `left_deep_yards` | integer | Passing yards gained on deep (20+ air yards) throws to the left side of the field. |
| `left_short_aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `center_behind_los_completions` | integer | Number of completed passes on throws behind the line of scrimmage to the center of the field. |
| `left_short_thrown_aways` | integer | Number of intentional throwaways on short (0-9 air yards) throws to the left side of the field. |
| `left_short_sack_percent` | integer | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the left side of the field. |
| `center_short_thrown_aways` | integer | Number of intentional throwaways on short (0-9 air yards) throws to the center of the field. |
| `center_short_interceptions` | integer | Number of passes intercepted on short (0-9 air yards) throws to the center of the field. |
| `short_avg_depth_of_target` | numeric | Average depth of target, in air yards, on short (0-9 air yards) throws: of the passer's throws (passing-depth) or the player's targets (receiving-depth). |
| `left_deep_touchdowns` | integer | Number of passing touchdowns thrown on deep (20+ air yards) throws to the left side of the field. |
| `deep_yards` | integer | Yards on deep (20+ air yards) throws: passing yards (passing-depth) or the player's receiving yards (receiving-depth). |
| `center_behind_los_turnover_worthy_plays` | integer | Number of turnover-worthy plays on throws behind the line of scrimmage to the center of the field, plays PFF charts as deserving of a turnover. |
| `medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws. |
| `left_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the left side of the field. |
| `left_deep_avg_depth_of_target` | integer | Average depth of target in air yards on deep (20+ air yards) throws to the left side of the field. |
| `deep_first_downs` | integer | First downs on deep (20+ air yards) throws: passing first downs (passing-depth) or first-down receptions by the player (receiving-depth). |
| `short_drop_rate` | integer | Percentage of catchable passes dropped on short (0-9 air yards) throws: by the passer's receivers (passing-depth) or by the player (receiving-depth). |
| `left_medium_bats` | integer | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_spikes` | integer | Number of clock-stopping spikes on deep (20+ air yards) throws to the center of the field. |
| `center_short_hit_as_threw` | integer | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `left_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_touchdowns` | integer | Number of passing touchdowns thrown on deep (20+ air yards) throws to the center of the field. |
| `right_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the right side of the field. |
| `right_short_big_time_throws` | integer | Number of big-time throws on short (0-9 air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_short_big_time_throws` | integer | Number of big-time throws on short (0-9 air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws. |
| `left_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the left side of the field. |
| `right_deep_passing_snaps` | integer | Number of passing snaps played on deep (20+ air yards) throws to the right side of the field. |
| `right_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the right side of the field, per PFF charting. |
| `center_medium_attempts` | integer | Number of pass attempts on medium (10-19 air yards) throws to the center of the field. |
| `right_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the right side of the field. |
| `center_deep_def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage. |
| `right_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the right side of the field, per PFF charting. |
| `right_deep_aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `right_deep_scrambles` | integer | Number of scrambles on deep (20+ air yards) throws to the right side of the field. |
| `deep_big_time_throws` | integer | Number of big-time throws on deep (20+ air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `left_short_turnover_worthy_plays` | integer | Number of turnover-worthy plays on short (0-9 air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_short_touchdowns` | integer | Number of passing touchdowns thrown on short (0-9 air yards) throws to the center of the field. |
| `right_medium_drops` | integer | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the right side of the field. |
| `left_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the left side of the field. |
| `short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws. |
| `medium_pressure_to_sack_rate` | integer | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws. |
| `left_medium_thrown_aways` | integer | Number of intentional throwaways on medium (10-19 air yards) throws to the left side of the field. |
| `right_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_btt_rate` | integer | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage, per PFF charting. |
| `medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws. |
| `center_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the center of the field. |
| `behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage. |
| `right_medium_touchdowns` | integer | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the right side of the field. |
| `center_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the center of the field. |
| `left_deep_aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `left_medium_yards` | integer | Passing yards gained on medium (10-19 air yards) throws to the left side of the field. |
| `center_medium_touchdowns` | integer | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the center of the field. |
| `center_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the center of the field. |
| `left_short_twp_rate` | integer | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the left side of the field, per PFF charting. |
| `penalties` | integer | Penalties charged to the player over the covered span (see declined_penalties for the ones that were declined). |
| `right_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the right side of the field, expressed as a percentage. |
| `right_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_sacks` | integer | Number of sacks taken on throws behind the line of scrimmage. |
| `center_deep_yards` | integer | Passing yards gained on deep (20+ air yards) throws to the center of the field. |
| `short_dropbacks` | integer | Number of dropbacks on short (0-9 air yards) throws. |
| `left_deep_sack_percent` | integer | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the center of the field. |
| `behind_los_avg_depth_of_target` | numeric | Average depth of target, in air yards, on throws behind the line of scrimmage: of the passer's throws (passing-depth) or the player's targets (receiving-depth). |
| `left_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the left side of the field. |
| `medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws, per PFF charting. |
| `left_short_def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `medium_turnover_worthy_plays` | integer | Number of turnover-worthy plays on medium (10-19 air yards) throws, plays PFF charts as deserving of a turnover. |
| `behind_los_twp_rate` | integer | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage, per PFF charting. |
| `left_behind_los_thrown_aways` | integer | Number of intentional throwaways on throws behind the line of scrimmage to the left side of the field. |
| `left_short_bats` | integer | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the left side of the field. |
| `center_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the center of the field. |
| `right_deep_bats` | integer | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the right side of the field. |
| `medium_yards` | integer | Yards on medium (10-19 air yards) throws: passing yards (passing-depth) or the player's receiving yards (receiving-depth). |
| `center_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the center of the field. |
| `center_medium_passing_snaps` | integer | Number of passing snaps played on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_first_downs` | integer | Number of passing first downs gained on throws behind the line of scrimmage to the center of the field. |
| `center_medium_interceptions` | integer | Number of passes intercepted on medium (10-19 air yards) throws to the center of the field. |
| `behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage. |
| `left_short_hit_as_threw` | integer | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws. |
| `center_deep_bats` | integer | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the center of the field. |
| `behind_los_ypa` | integer | Yards gained per pass attempt on throws behind the line of scrimmage. |
| `right_short_interceptions` | integer | Number of passes intercepted on short (0-9 air yards) throws to the right side of the field. |
| `left_deep_interceptions` | integer | Number of passes intercepted on deep (20+ air yards) throws to the left side of the field. |
| `right_medium_sacks` | integer | Number of sacks taken on medium (10-19 air yards) throws to the right side of the field. |
| `right_behind_los_turnover_worthy_plays` | integer | Number of turnover-worthy plays on throws behind the line of scrimmage to the right side of the field, plays PFF charts as deserving of a turnover. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `right_medium_spikes` | integer | Number of clock-stopping spikes on medium (10-19 air yards) throws to the right side of the field. |
| `behind_los_hit_as_threw` | integer | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage, as charted by PFF. |
| `left_medium_pressure_to_sack_rate` | integer | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the left side of the field. |
| `medium_dropbacks` | integer | Number of dropbacks on medium (10-19 air yards) throws. |
| `deep_hit_as_threw` | integer | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws, as charted by PFF. |
| `right_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the right side of the field. |
| `short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws. |
| `deep_drop_rate` | numeric | Percentage of catchable passes dropped on deep (20+ air yards) throws: by the passer's receivers (passing-depth) or by the player (receiving-depth). |
| `declined_penalties` | integer | Penalties committed by the player that were declined. |
| `deep_scrambles` | integer | Number of scrambles on deep (20+ air yards) throws. |
| `left_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the left side of the field. |
| `right_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the right side of the field. |
| `medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws, expressed as a percentage. |
| `right_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the right side of the field. |
| `left_medium_hit_as_threw` | integer | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `left_behind_los_pressure_to_sack_rate` | integer | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the left side of the field. |
| `right_short_def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `right_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the right side of the field, expressed as a percentage. |
| `center_short_aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `short_hit_as_threw` | integer | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws, as charted by PFF. |
| `right_medium_turnover_worthy_plays` | integer | Number of turnover-worthy plays on medium (10-19 air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `left_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the left side of the field. |
| `center_behind_los_pressure_to_sack_rate` | integer | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the center of the field. |
| `right_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the right side of the field. |
| `deep_interceptions` | integer | Interceptions on deep (20+ air yards) throws: thrown by the passer (passing-depth) or on passes targeting the player (receiving-depth). |
| `left_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the left side of the field, per PFF charting. |
| `right_behind_los_big_time_throws` | integer | Number of big-time throws on throws behind the line of scrimmage to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_medium_sack_percent` | integer | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_thrown_aways` | integer | Number of intentional throwaways on deep (20+ air yards) throws to the center of the field. |
| `short_thrown_aways` | integer | Number of intentional throwaways on short (0-9 air yards) throws. |
| `left_short_btt_rate` | integer | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the left side of the field, per PFF charting. |
| `right_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the right side of the field. |
| `short_def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws, as charted by PFF. |
| `center_deep_dropbacks` | integer | Number of dropbacks on deep (20+ air yards) throws to the center of the field. |
| `center_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the center of the field. |
| `right_behind_los_yards` | integer | Passing yards gained on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_attempts` | integer | Number of pass attempts on medium (10-19 air yards) throws to the right side of the field. |
| `left_medium_attempts` | integer | Number of pass attempts on medium (10-19 air yards) throws to the left side of the field. |
| `medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws. |
| `left_medium_drops` | integer | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the left side of the field. |
| `left_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the left side of the field. |
| `medium_passing_snaps` | integer | Number of passing snaps played on medium (10-19 air yards) throws. |
| `center_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_passing_snaps` | integer | Number of passing snaps played on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_pressure_to_sack_rate` | integer | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the left side of the field. |
| `deep_bats` | integer | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws. |
| `left_medium_passing_snaps` | integer | Number of passing snaps played on medium (10-19 air yards) throws to the left side of the field. |
| `left_medium_scrambles` | integer | Number of scrambles on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the right side of the field. |
| `left_short_drops` | integer | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the left side of the field. |
| `left_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the left side of the field. |
| `medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws. |
| `behind_los_turnover_worthy_plays` | integer | Number of turnover-worthy plays on throws behind the line of scrimmage, plays PFF charts as deserving of a turnover. |
| `right_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the right side of the field. |
| `right_deep_dropbacks` | integer | Number of dropbacks on deep (20+ air yards) throws to the right side of the field. |
| `deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws. |
| `center_medium_completions` | integer | Number of completed passes on medium (10-19 air yards) throws to the center of the field. |
| `left_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the left side of the field. |
| `left_short_sacks` | integer | Number of sacks taken on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_spikes` | integer | Number of clock-stopping spikes on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `center_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the center of the field. |
| `center_deep_interceptions` | integer | Number of passes intercepted on deep (20+ air yards) throws to the center of the field. |
| `right_deep_completions` | integer | Number of completed passes on deep (20+ air yards) throws to the right side of the field. |
| `center_behind_los_sacks` | integer | Number of sacks taken on throws behind the line of scrimmage to the center of the field. |
| `deep_completions` | integer | Number of completed passes on deep (20+ air yards) throws. |
| `left_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the left side of the field, expressed as a percentage. |
| `short_yards` | integer | Yards on short (0-9 air yards) throws: passing yards (passing-depth) or the player's receiving yards (receiving-depth). |
| `behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage. |
| `right_short_sacks` | integer | Number of sacks taken on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_thrown_aways` | integer | Number of intentional throwaways on throws behind the line of scrimmage to the right side of the field. |
| `base_attempts` | integer | Number of pass attempts across all splits, the baseline total for this facet. |
| `center_short_spikes` | integer | Number of clock-stopping spikes on short (0-9 air yards) throws to the center of the field. |
| `center_behind_los_hit_as_threw` | integer | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `center_deep_turnover_worthy_plays` | integer | Number of turnover-worthy plays on deep (20+ air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `left_medium_sack_percent` | integer | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the left side of the field. |
| `behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage. |
| `left_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the left side of the field, expressed as a percentage. |
| `center_deep_big_time_throws` | integer | Number of big-time throws on deep (20+ air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `left_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the left side of the field. |
| `behind_los_thrown_aways` | integer | Number of intentional throwaways on throws behind the line of scrimmage. |
| `center_medium_def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws. |
| `left_deep_twp_rate` | integer | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the left side of the field, per PFF charting. |
| `center_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_big_time_throws` | integer | Number of big-time throws on throws behind the line of scrimmage to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `right_medium_yards` | integer | Passing yards gained on medium (10-19 air yards) throws to the right side of the field. |
| `right_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the right side of the field. |
| `center_medium_big_time_throws` | integer | Number of big-time throws on medium (10-19 air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `franchise_id` | integer | PFF franchise (team) id (integer join key). |
| `left_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the left side of the field. |
| `left_short_drop_rate` | integer | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_scrambles` | integer | Number of scrambles on throws behind the line of scrimmage to the left side of the field. |
| `left_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the left side of the field, per PFF charting. |
| `right_deep_hit_as_threw` | integer | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `behind_los_bats` | integer | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage. |
| `short_first_downs` | integer | First downs on short (0-9 air yards) throws: passing first downs (passing-depth) or first-down receptions by the player (receiving-depth). |
| `left_deep_attempts` | integer | Number of pass attempts on deep (20+ air yards) throws to the left side of the field. |
| `right_behind_los_attempts` | integer | Number of pass attempts on throws behind the line of scrimmage to the right side of the field. |
| `right_deep_sacks` | integer | Number of sacks taken on deep (20+ air yards) throws to the right side of the field. |
| `left_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the left side of the field, expressed as a percentage. |
| `behind_los_big_time_throws` | integer | Number of big-time throws on throws behind the line of scrimmage, per PFF's highest-value, highest-difficulty throw designation. |
| `center_deep_pressure_to_sack_rate` | integer | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the center of the field. |
| `left_medium_big_time_throws` | integer | Number of big-time throws on medium (10-19 air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `right_behind_los_bats` | integer | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the right side of the field. |
| `left_medium_def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `right_deep_turnover_worthy_plays` | integer | Number of turnover-worthy plays on deep (20+ air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `left_short_spikes` | integer | Number of clock-stopping spikes on short (0-9 air yards) throws to the left side of the field. |
| `left_medium_aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `left_behind_los_twp_rate` | integer | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the left side of the field, per PFF charting. |
| `short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws. |
| `right_short_drops` | integer | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the right side of the field. |
| `right_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the right side of the field. |
| `right_medium_thrown_aways` | integer | Number of intentional throwaways on medium (10-19 air yards) throws to the right side of the field. |
| `center_short_sack_percent` | integer | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the center of the field. |
| `right_short_passing_snaps` | integer | Number of passing snaps played on short (0-9 air yards) throws to the right side of the field. |
| `center_short_def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `center_medium_positive_epa_percent` | integer | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the center of the field. |
| `medium_drops` | integer | Catchable passes dropped on medium (10-19 air yards) throws: by the passer's receivers (passing-depth) or by the player (receiving-depth). |
| `center_medium_aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `center_short_sacks` | integer | Number of sacks taken on short (0-9 air yards) throws to the center of the field. |
| `deep_attempts_percent` | integer | Share of the player's total pass attempts that came on deep (20+ air yards) throws, expressed as a percentage. |
| `center_behind_los_def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `left_short_passing_snaps` | integer | Number of passing snaps played on short (0-9 air yards) throws to the left side of the field. |
| `center_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the center of the field. |
| `behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage. |
| `center_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the center of the field. |
| `left_deep_hit_as_threw` | integer | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `center_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the center of the field. |
| `left_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the left side of the field, expressed as a percentage. |
| `left_deep_scrambles` | integer | Number of scrambles on deep (20+ air yards) throws to the left side of the field. |
| `right_short_completions` | integer | Number of completed passes on short (0-9 air yards) throws to the right side of the field. |
| `center_behind_los_aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `right_short_scrambles` | integer | Number of scrambles on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the center of the field. |
| `deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws. |
| `left_deep_drops` | integer | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the left side of the field. |
| `right_behind_los_def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `center_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the center of the field. |
| `center_deep_hit_as_threw` | integer | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `short_spikes` | integer | Number of clock-stopping spikes on short (0-9 air yards) throws. |
| `right_behind_los_completions` | integer | Number of completed passes on throws behind the line of scrimmage to the right side of the field. |
| `center_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the center of the field. |
| `short_drops` | integer | Catchable passes dropped on short (0-9 air yards) throws: by the passer's receivers (passing-depth) or by the player (receiving-depth). |
| `deep_aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws, as charted by PFF. |
| `right_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the right side of the field. |
| `short_pressure_to_sack_rate` | integer | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws. |
| `center_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the center of the field. |
| `left_medium_completions` | integer | Number of completed passes on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_sacks` | integer | Number of sacks taken on deep (20+ air yards) throws to the center of the field. |
| `left_deep_big_time_throws` | integer | Number of big-time throws on deep (20+ air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_medium_first_downs` | integer | Number of passing first downs gained on medium (10-19 air yards) throws to the center of the field. |
| `center_medium_dropbacks` | integer | Number of dropbacks on medium (10-19 air yards) throws to the center of the field. |
| `right_deep_def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `left_short_touchdowns` | integer | Number of passing touchdowns thrown on short (0-9 air yards) throws to the left side of the field. |
| `medium_drop_rate` | numeric | Percentage of catchable passes dropped on medium (10-19 air yards) throws: by the passer's receivers (passing-depth) or by the player (receiving-depth). |
| `left_medium_ypa` | integer | Yards gained per pass attempt on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_passing_snaps` | integer | Number of passing snaps played on medium (10-19 air yards) throws to the right side of the field. |
| `center_short_scrambles` | integer | Number of scrambles on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_turnover_worthy_plays` | integer | Number of turnover-worthy plays on throws behind the line of scrimmage to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_medium_yards` | integer | Passing yards gained on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the center of the field. |
| `left_medium_interceptions` | integer | Number of passes intercepted on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_interceptions` | integer | Number of passes intercepted on deep (20+ air yards) throws to the right side of the field. |
| `medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws. |
| `right_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the right side of the field. |
| `deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws. |
| `behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage. |
| `center_deep_scrambles` | integer | Number of scrambles on deep (20+ air yards) throws to the center of the field. |
| `center_short_twp_rate` | integer | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the center of the field, per PFF charting. |
| `behind_los_touchdowns` | integer | Touchdowns on throws behind the line of scrimmage: thrown by the passer (passing-depth) or scored by the player (receiving-depth). |
| `left_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_completions` | integer | Number of completed passes on medium (10-19 air yards) throws to the right side of the field. |
| `right_behind_los_drops` | integer | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the right side of the field. |
| `left_behind_los_first_downs` | integer | Number of passing first downs gained on throws behind the line of scrimmage to the left side of the field. |
| `short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws. |
| `center_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the center of the field, per PFF charting. |
| `left_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the left side of the field. |

**passing-pressure**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `player` | character | Player's display name as PFF lists it (e.g. "Tom Brady"). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P, LS). |
| `no_blitz_completion_percent` | numeric | Percentage of pass attempts completed when not blitzed. |
| `grades_offense` | numeric | PFF overall offense grade (0-100). |
| `no_pressure_scrambles` | integer | Number of scrambles from a clean pocket (no pressure). |
| `blitz_touchdowns` | integer | Number of passing touchdowns thrown when blitzed. |
| `pressure_yards` | integer | Passing yards gained when under pressure. |
| `no_pressure_spikes` | integer | Number of clock-stopping spikes from a clean pocket (no pressure). |
| `blitz_ypa` | numeric | Yards gained per pass attempt when blitzed. |
| `no_blitz_grades_run` | numeric | PFF rushing grade for the player (0-100) when not blitzed. |
| `blitz_qb_rating` | numeric | Traditional NFL passer rating when blitzed. |
| `no_pressure_thrown_aways` | integer | Number of intentional throwaways from a clean pocket (no pressure). |
| `no_blitz_bats` | integer | Number of pass attempts batted down at the line of scrimmage when not blitzed. |
| `no_blitz_drops` | integer | Number of catchable passes dropped by receivers when not blitzed. |
| `no_pressure_completion_percent` | numeric | Percentage of pass attempts completed from a clean pocket (no pressure). |
| `blitz_aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways) when blitzed, as charted by PFF. |
| `pressure_grades_run` | numeric | PFF rushing grade for the player (0-100) when under pressure. |
| `no_blitz_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when not blitzed. |
| `no_pressure_bats` | integer | Number of pass attempts batted down at the line of scrimmage from a clean pocket (no pressure). |
| `pressure_completions` | integer | Number of completed passes when under pressure. |
| `blitz_big_time_throws` | integer | Number of big-time throws when blitzed, per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when not blitzed. |
| `blitz_spikes` | integer | Number of clock-stopping spikes when blitzed. |
| `no_pressure_completions` | integer | Number of completed passes from a clean pocket (no pressure). |
| `draft_season` | integer | Season of the player's NFL draft class, per PFF. |
| `no_pressure_passing_snaps` | integer | Number of passing snaps played from a clean pocket (no pressure). |
| `no_blitz_first_downs` | integer | Number of passing first downs gained when not blitzed. |
| `blitz_avg_time_to_throw` | numeric | Average time from snap to release in seconds when blitzed. |
| `no_pressure_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) from a clean pocket (no pressure). |
| `pressure_aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways) when under pressure, as charted by PFF. |
| `blitz_sacks` | integer | Number of sacks taken when blitzed. |
| `no_pressure_interceptions` | integer | Number of passes intercepted from a clean pocket (no pressure). |
| `team_name` | character | Team name/abbreviation the player is credited to for the range. |
| `pressure_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when under pressure. |
| `blitz_completions` | integer | Number of completed passes when blitzed. |
| `blitz_attempts` | integer | Number of pass attempts when blitzed. |
| `pressure_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when under pressure, per PFF charting. |
| `pressure_sacks` | integer | Number of sacks taken when under pressure. |
| `no_blitz_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack when not blitzed. |
| `no_pressure_ypa` | numeric | Yards gained per pass attempt from a clean pocket (no pressure). |
| `pressure_passing_snaps` | integer | Number of passing snaps played when under pressure. |
| `grades_pass` | numeric | PFF passing grade (0-100). |
| `pressure_bats` | integer | Number of pass attempts batted down at the line of scrimmage when under pressure. |
| `blitz_thrown_aways` | integer | Number of intentional throwaways when blitzed. |
| `no_pressure_drops` | integer | Number of catchable passes dropped by receivers from a clean pocket (no pressure). |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `pressure_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when under pressure. |
| `pressure_scrambles` | integer | Number of scrambles when under pressure. |
| `blitz_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when blitzed. |
| `pressure_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when under pressure. |
| `blitz_completion_percent` | numeric | Percentage of pass attempts completed when blitzed. |
| `no_pressure_first_downs` | integer | Number of passing first downs gained from a clean pocket (no pressure). |
| `blitz_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when blitzed. |
| `no_blitz_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when not blitzed. |
| `blitz_interceptions` | integer | Number of passes intercepted when blitzed. |
| `no_blitz_dropbacks` | integer | Number of dropbacks when not blitzed. |
| `no_blitz_grades_pass` | numeric | PFF passing grade (0-100) when not blitzed. |
| `no_blitz_scrambles` | integer | Number of scrambles when not blitzed. |
| `pressure_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when under pressure. |
| `no_blitz_yards` | integer | Passing yards gained when not blitzed. |
| `base_dropbacks` | integer | Number of dropbacks across all splits, the baseline total for this facet. |
| `pressure_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack, reported within the pressure split. |
| `no_blitz_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when not blitzed. |
| `no_pressure_grades_offense` | numeric | PFF overall offense grade for the player (0-100) from a clean pocket (no pressure). |
| `no_pressure_avg_time_to_throw` | numeric | Average time from snap to release in seconds from a clean pocket (no pressure). |
| `pressure_dropbacks` | integer | Number of dropbacks when under pressure. |
| `no_blitz_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when not blitzed. |
| `no_pressure_grades_run` | numeric | PFF rushing grade for the player (0-100) from a clean pocket (no pressure). |
| `player_game_count` | integer | Number of games the player appeared in over the covered span. |
| `blitz_turnover_worthy_plays` | integer | Number of turnover-worthy plays when blitzed, plays PFF charts as deserving of a turnover. |
| `no_blitz_touchdowns` | integer | Number of passing touchdowns thrown when not blitzed. |
| `no_blitz_avg_depth_of_target` | numeric | Average depth of target in air yards when not blitzed. |
| `no_pressure_dropbacks_percent` | numeric | Share of the player's total dropbacks that came from a clean pocket (no pressure), expressed as a percentage. |
| `blitz_grades_pass` | numeric | PFF passing grade (0-100) when blitzed. |
| `eligible_season` | integer | Season of the player's NFL draft eligibility, per PFF. |
| `blitz_avg_depth_of_target` | numeric | Average depth of target in air yards when blitzed. |
| `no_blitz_spikes` | integer | Number of clock-stopping spikes when not blitzed. |
| `no_pressure_dropbacks` | integer | Number of dropbacks from a clean pocket (no pressure). |
| `blitz_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when blitzed, per PFF charting. |
| `blitz_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when blitzed. |
| `no_pressure_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts from a clean pocket (no pressure), per PFF charting. |
| `no_pressure_drop_rate` | numeric | Percentage of catchable passes dropped by receivers from a clean pocket (no pressure). |
| `no_blitz_turnover_worthy_plays` | integer | Number of turnover-worthy plays when not blitzed, plays PFF charts as deserving of a turnover. |
| `pressure_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when under pressure. |
| `blitz_first_downs` | integer | Number of passing first downs gained when blitzed. |
| `no_blitz_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when not blitzed, expressed as a percentage. |
| `pressure_turnover_worthy_plays` | integer | Number of turnover-worthy plays when under pressure, plays PFF charts as deserving of a turnover. |
| `no_pressure_epa` | numeric | Total expected points added (EPA) on the player's dropbacks from a clean pocket (no pressure). |
| `no_blitz_avg_time_to_throw` | numeric | Average time from snap to release in seconds when not blitzed. |
| `no_blitz_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when not blitzed. |
| `pressure_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when under pressure, per PFF charting. |
| `no_pressure_aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways) from a clean pocket (no pressure), as charted by PFF. |
| `pressure_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when under pressure, expressed as a percentage. |
| `grades_run` | numeric | PFF rushing grade (0-100). |
| `no_pressure_def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks from a clean pocket (no pressure), as charted by PFF. |
| `pressure_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when under pressure. |
| `pressure_completion_percent` | numeric | Percentage of pass attempts completed when under pressure. |
| `pressure_avg_depth_of_target` | integer | Average depth of target in air yards when under pressure. |
| `blitz_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when blitzed. |
| `pressure_drops` | integer | Number of catchable passes dropped by receivers when under pressure. |
| `no_blitz_def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks when not blitzed, as charted by PFF. |
| `pressure_attempts` | integer | Number of pass attempts when under pressure. |
| `pressure_big_time_throws` | integer | Number of big-time throws when under pressure, per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_thrown_aways` | integer | Number of intentional throwaways when not blitzed. |
| `blitz_drops` | integer | Number of catchable passes dropped by receivers when blitzed. |
| `no_pressure_touchdowns` | integer | Number of passing touchdowns thrown from a clean pocket (no pressure). |
| `no_pressure_sacks` | integer | Number of sacks taken from a clean pocket (no pressure). |
| `blitz_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when blitzed. |
| `pressure_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when under pressure. |
| `penalties` | integer | Penalties charged to the player over the covered span (see declined_penalties for the ones that were declined). |
| `no_pressure_attempts` | integer | Number of pass attempts from a clean pocket (no pressure). |
| `no_blitz_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when not blitzed. |
| `blitz_scrambles` | integer | Number of scrambles when blitzed. |
| `blitz_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when blitzed, expressed as a percentage. |
| `no_pressure_qb_rating` | numeric | Traditional NFL passer rating from a clean pocket (no pressure). |
| `no_blitz_passing_snaps` | integer | Number of passing snaps played when not blitzed. |
| `no_blitz_aimed_passes` | integer | Number of aimed passes (attempts excluding spikes and throwaways) when not blitzed, as charted by PFF. |
| `blitz_yards` | integer | Passing yards gained when blitzed. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `no_blitz_attempts` | integer | Number of pass attempts when not blitzed. |
| `pressure_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when under pressure. |
| `no_blitz_hit_as_threw` | integer | Number of attempts on which the passer was hit as he threw when not blitzed, as charted by PFF. |
| `pressure_hit_as_threw` | integer | Number of attempts on which the passer was hit as he threw when under pressure, as charted by PFF. |
| `blitz_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack when blitzed. |
| `declined_penalties` | integer | Penalties committed by the player that were declined. |
| `no_blitz_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when not blitzed, per PFF charting. |
| `no_blitz_ypa` | numeric | Yards gained per pass attempt when not blitzed. |
| `blitz_grades_run` | numeric | PFF rushing grade for the player (0-100) when blitzed. |
| `pressure_interceptions` | integer | Number of passes intercepted when under pressure. |
| `blitz_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when blitzed. |
| `no_pressure_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) from a clean pocket (no pressure). |
| `blitz_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when blitzed. |
| `grades_hands_fumble` | numeric | PFF ball-security (hands/fumble) grade, 0-100. |
| `pressure_def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks when under pressure, as charted by PFF. |
| `pressure_grades_pass` | numeric | PFF passing grade (0-100) when under pressure. |
| `no_blitz_qb_rating` | numeric | Traditional NFL passer rating when not blitzed. |
| `blitz_hit_as_threw` | integer | Number of attempts on which the passer was hit as he threw when blitzed, as charted by PFF. |
| `pressure_ypa` | numeric | Yards gained per pass attempt when under pressure. |
| `blitz_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when blitzed. |
| `pressure_avg_time_to_throw` | numeric | Average time from snap to release in seconds when under pressure. |
| `no_blitz_completions` | integer | Number of completed passes when not blitzed. |
| `no_pressure_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) from a clean pocket (no pressure). |
| `no_pressure_sack_percent` | integer | Percentage of dropbacks that ended in a sack from a clean pocket (no pressure). |
| `no_blitz_accuracy_percent` | integer | Percentage of aimed passes charted as accurate by PFF when not blitzed. |
| `no_pressure_pressure_to_sack_rate` | character | Pressure-to-sack rate as reported within the no-pressure split of the PFF passing-pressure facet. |
| `no_pressure_turnover_worthy_plays` | integer | Number of turnover-worthy plays from a clean pocket (no pressure), plays PFF charts as deserving of a turnover. |
| `pressure_first_downs` | integer | Number of passing first downs gained when under pressure. |
| `blitz_passing_snaps` | integer | Number of passing snaps played when blitzed. |
| `franchise_id` | integer | PFF franchise (team) id (integer join key). |
| `no_pressure_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts from a clean pocket (no pressure), per PFF charting. |
| `no_blitz_sacks` | integer | Number of sacks taken when not blitzed. |
| `no_blitz_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when not blitzed. |
| `no_pressure_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF from a clean pocket (no pressure). |
| `no_blitz_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when not blitzed, per PFF charting. |
| `no_pressure_hit_as_threw` | integer | Number of attempts on which the passer was hit as he threw from a clean pocket (no pressure), as charted by PFF. |
| `no_pressure_grades_pass` | numeric | PFF passing grade (0-100) from a clean pocket (no pressure). |
| `no_pressure_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added from a clean pocket (no pressure). |
| `pressure_spikes` | integer | Number of clock-stopping spikes when under pressure. |
| `pressure_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when under pressure. |
| `pressure_qb_rating` | numeric | Traditional NFL passer rating when under pressure. |
| `blitz_bats` | integer | Number of pass attempts batted down at the line of scrimmage when blitzed. |
| `pressure_touchdowns` | integer | Number of passing touchdowns thrown when under pressure. |
| `pressure_thrown_aways` | integer | Number of intentional throwaways when under pressure. |
| `no_blitz_interceptions` | integer | Number of passes intercepted when not blitzed. |
| `blitz_dropbacks` | integer | Number of dropbacks when blitzed. |
| `blitz_def_gen_pressures` | integer | Number of defense-generated pressures on the player's dropbacks when blitzed, as charted by PFF. |
| `no_blitz_big_time_throws` | integer | Number of big-time throws when not blitzed, per PFF's highest-value, highest-difficulty throw designation. |
| `no_pressure_avg_depth_of_target` | numeric | Average depth of target in air yards from a clean pocket (no pressure). |
| `no_pressure_yards` | integer | Passing yards gained from a clean pocket (no pressure). |
| `blitz_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when blitzed, per PFF charting. |
| `blitz_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when blitzed. |
| `no_pressure_big_time_throws` | integer | Number of big-time throws from a clean pocket (no pressure), per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when not blitzed. |
| `blitz_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when blitzed. |
| `no_pressure_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `blitz_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when blitzed. |
| `no_pressure_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_pass_block` | character | PFF pass-blocking grade for the player (0-100) when under pressure. |
| `no_blitz_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when not blitzed. |
| `blitz_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when blitzed. |
| `pressure_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when under pressure. |
| `no_blitz_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when not blitzed. |
| `no_pressure_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) from a clean pocket (no pressure). |
| `blitz_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when blitzed. |
| `pressure_grades_run_block` | integer | PFF run-blocking grade for the player (0-100) when under pressure. |
| `no_pressure_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when under pressure. |
| `no_blitz_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when not blitzed. |

**receiving**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `player` | character | Player's display name as PFF lists it (e.g. "Tom Brady"). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P, LS). |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `franchise_id` | integer | PFF franchise (team) id (integer join key). |
| `team_abbreviation` | character | Abbreviation of the team the player is credited to for the range (the /v2 counterpart of /v1's team). |
| `games_played` | integer | Number of games the player appeared in over the requested span (the /v2 counterpart of /v1's player_game_count). |
| `draft_season` | integer | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | integer | Season of the player's NFL draft eligibility, per PFF. |
| `targets` | integer | Targets: passes thrown to the player (receiving, rushing reports) or into the player's coverage (coverage, defense reports). |
| `team_targets_percent` | numeric | Percentage of the team's targets that went to the player (target share). |
| `receptions` | integer | Receptions: passes caught by the player (receiving, rushing reports) or completions allowed into the player's coverage (coverage, defense reports). |
| `caught_percent` | numeric | Percentage of targets caught. |
| `yards` | integer | Yards in the report's play family: passing yards (passing), receiving yards (receiving), rushing yards (rushing), gross punt yards (punting), and receiving yards allowed into the player's coverage (coverage, defense). |
| `yards_per_reception` | numeric | Average yards per reception: the player's own on the receiving report, allowed per reception in the player's coverage on the coverage and defense reports. |
| `touchdowns` | integer | Touchdowns: passing touchdowns thrown (passing), receiving and rushing touchdowns scored (receiving, rushing), or touchdowns allowed into the player's coverage (coverage, defense). |
| `grades_offense` | numeric | PFF overall offense grade (0-100). |
| `grades_pass_route` | numeric | PFF receiving/route grade (0-100). |
| `grades_hands_drop` | numeric | PFF hands/drop grade (0-100). |
| `grades_hands_fumble` | numeric | PFF ball-security (hands/fumble) grade, 0-100. |
| `pass_plays` | integer | Pass-play snaps. |
| `routes` | integer | Pass routes run by the player. |
| `route_rate` | numeric | Share of pass-play snaps on which the player ran a route. |
| `pass_blocks` | integer | Pass-play snaps spent pass blocking. |
| `pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking. |
| `slot_snaps` | integer | Receiving snaps aligned in the slot. |
| `slot_rate` | numeric | Share of receiving snaps aligned in the slot. |
| `wide_snaps` | integer | Receiving snaps aligned out wide. |
| `wide_rate` | numeric | Share of receiving snaps aligned out wide. |
| `inline_snaps` | integer | Receiving snaps aligned inline, tight to the formation. |
| `inline_rate` | numeric | Share of receiving snaps aligned inline, tight to the formation. |
| `yards_after_catch` | integer | Yards after the catch: gained by the player on the receiving report, allowed after the catch in the player's coverage on the coverage and defense reports. |
| `yards_after_catch_per_reception` | numeric | Average yards after the catch per reception. |
| `yprr` | numeric | Yards per route run. |
| `avg_depth_of_target` | numeric | Average depth of target, in yards downfield: of the passer's throws (passing), of passes to the player (receiving), or of targets into the player's coverage (coverage). |
| `longest` | integer | Longest play in yards: longest reception (receiving), longest run (rushing), or longest completion allowed in coverage (coverage, defense). |
| `drops` | integer | Dropped passes: drops by the passer's receivers on the passing report, drops by the player on the receiving and rushing reports. |
| `drop_rate` | numeric | Drop rate, in percent: share of catchable passes dropped by the passer's receivers (passing report) or by the player (receiving report). |
| `contested_targets` | integer | Contested targets. |
| `contested_receptions` | integer | Contested catches made. |
| `contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught. |
| `interceptions` | integer | Interceptions: thrown by the passer on the passing report, made by the player on the coverage and defense reports, and on passes targeting the player on the receiving report. |
| `fumbles` | integer | Fumbles by the player: after the catch on the receiving report, as a ball carrier on the rushing report. |
| `avoided_tackles` | integer | Missed tackles forced (tackles avoided) by the player: after the catch on the receiving report, as a runner on rushing. |
| `first_downs` | integer | First downs gained: passing first downs on the passing report, first-down receptions on receiving, rushing first downs on rushing. |
| `targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player. |
| `penalties` | integer | Penalties charged to the player over the covered span (see declined_penalties for the ones that were declined). |
| `receptions_oe` | numeric | Catch rate over expectation, in percentage points (receptions per target minus PFF's expected rate). |
| `receptions_oe_total` | numeric | Receptions over expectation in total: receptions minus PFF-expected receptions on the player's targets. |
| `lined_up_vs_cb` | numeric | Percentage of the player's charted matchups in which he lined up across from a cornerback; with lined_up_vs_lb and lined_up_vs_s it sums to 100 where populated (null for many players). |
| `lined_up_vs_s` | numeric | Percentage of the player's charted matchups in which he lined up across from a safety; with lined_up_vs_cb and lined_up_vs_lb it sums to 100 where populated (null for many players). |
| `lined_up_vs_lb` | numeric | Percentage of the player's charted matchups in which he lined up across from a linebacker; with lined_up_vs_cb and lined_up_vs_s it sums to 100 where populated (null for many players). |
| `position_target_share` | numeric | Percentage of the team's targets that went to players at the player's position; the same value repeats on every row at that position (e.g. the whole WR group's share). |
| `offense_pos_graded_rate` | numeric | Percentage of the player's offensive plays that PFF graded positively. |
| `offense_neg_graded_rate` | numeric | Percentage of the player's offensive plays that PFF graded negatively. |

**receiving-depth**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `player` | character | Player's display name as PFF lists it (e.g. "Tom Brady"). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P, LS). |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `franchise_id` | integer | PFF franchise (team) id (integer join key). |
| `team_abbreviation` | character | Abbreviation of the team the player is credited to for the range (the /v2 counterpart of /v1's team). |
| `games_played` | integer | Number of games the player appeared in over the requested span (the /v2 counterpart of /v1's player_game_count). |
| `draft_season` | integer | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | integer | Season of the player's NFL draft eligibility, per PFF. |
| `base_targets` | integer | Total targets from the facet's unsplit base row, across all depths and directions. |
| `deep_targets_percent` | numeric | Share of the team's targets thrown to the player on deep passes (20 or more yards downfield). |
| `deep_targets` | integer | Pass targets to the player on deep passes (20 or more yards downfield). |
| `deep_receptions` | integer | Receptions made on deep passes (20 or more yards downfield). |
| `deep_caught_percent` | numeric | Percentage of targets caught on deep passes (20 or more yards downfield). |
| `deep_yards` | integer | Yards on deep (20+ air yards) throws: passing yards (passing-depth) or the player's receiving yards (receiving-depth). |
| `deep_yards_per_reception` | numeric | Average yards per reception on deep passes (20 or more yards downfield). |
| `deep_touchdowns` | integer | Touchdowns on deep (20+ air yards) throws: thrown by the passer (passing-depth) or scored by the player (receiving-depth). |
| `deep_grades_pass_route` | numeric | PFF route-running (receiving) grade on deep passes (20 or more yards downfield), 0-100. |
| `deep_grades_hands_drop` | numeric | PFF hands/drop grade on deep passes (20 or more yards downfield), 0-100. |
| `deep_yards_after_catch` | integer | Yards gained after the catch on deep passes (20 or more yards downfield). |
| `deep_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on deep passes (20 or more yards downfield). |
| `deep_yprr` | numeric | Yards per route run on deep passes (20 or more yards downfield). |
| `deep_avg_depth_of_target` | numeric | Average depth of target, in air yards, on deep (20+ air yards) throws: of the passer's throws (passing-depth) or the player's targets (receiving-depth). |
| `deep_drops` | integer | Catchable passes dropped on deep (20+ air yards) throws: by the passer's receivers (passing-depth) or by the player (receiving-depth). |
| `deep_drop_rate` | numeric | Percentage of catchable passes dropped on deep (20+ air yards) throws: by the passer's receivers (passing-depth) or by the player (receiving-depth). |
| `deep_contested_targets` | integer | PFF-charted contested targets on deep passes (20 or more yards downfield). |
| `deep_contested_receptions` | integer | Catches made on PFF-charted contested targets on deep passes (20 or more yards downfield). |
| `deep_contested_catch_rate` | integer | Percentage of PFF-charted contested targets caught on deep passes (20 or more yards downfield). |
| `deep_interceptions` | integer | Interceptions on deep (20+ air yards) throws: thrown by the passer (passing-depth) or on passes targeting the player (receiving-depth). |
| `deep_fumbles` | integer | Fumbles by the player after the catch on deep passes (20 or more yards downfield). |
| `deep_avoided_tackles` | integer | Tackles avoided after the catch on deep passes (20 or more yards downfield). |
| `deep_first_downs` | integer | First downs on deep (20+ air yards) throws: passing first downs (passing-depth) or first-down receptions by the player (receiving-depth). |
| `deep_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on deep passes (20 or more yards downfield). |
| `medium_targets_percent` | numeric | Share of the team's targets thrown to the player on medium passes (10-19 yards downfield). |
| `medium_targets` | integer | Pass targets to the player on medium passes (10-19 yards downfield). |
| `medium_receptions` | integer | Receptions made on medium passes (10-19 yards downfield). |
| `medium_caught_percent` | numeric | Percentage of targets caught on medium passes (10-19 yards downfield). |
| `medium_yards` | integer | Yards on medium (10-19 air yards) throws: passing yards (passing-depth) or the player's receiving yards (receiving-depth). |
| `medium_yards_per_reception` | numeric | Average yards per reception on medium passes (10-19 yards downfield). |
| `medium_touchdowns` | integer | Touchdowns on medium (10-19 air yards) throws: thrown by the passer (passing-depth) or scored by the player (receiving-depth). |
| `medium_grades_pass_route` | numeric | PFF route-running (receiving) grade on medium passes (10-19 yards downfield), 0-100. |
| `medium_grades_hands_drop` | numeric | PFF hands/drop grade on medium passes (10-19 yards downfield), 0-100. |
| `medium_yards_after_catch` | integer | Yards gained after the catch on medium passes (10-19 yards downfield). |
| `medium_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on medium passes (10-19 yards downfield). |
| `medium_yprr` | numeric | Yards per route run on medium passes (10-19 yards downfield). |
| `medium_avg_depth_of_target` | numeric | Average depth of target, in air yards, on medium (10-19 air yards) throws: of the passer's throws (passing-depth) or the player's targets (receiving-depth). |
| `medium_drops` | integer | Catchable passes dropped on medium (10-19 air yards) throws: by the passer's receivers (passing-depth) or by the player (receiving-depth). |
| `medium_drop_rate` | numeric | Percentage of catchable passes dropped on medium (10-19 air yards) throws: by the passer's receivers (passing-depth) or by the player (receiving-depth). |
| `medium_contested_targets` | integer | PFF-charted contested targets on medium passes (10-19 yards downfield). |
| `medium_contested_receptions` | integer | Catches made on PFF-charted contested targets on medium passes (10-19 yards downfield). |
| `medium_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on medium passes (10-19 yards downfield). |
| `medium_interceptions` | integer | Interceptions on medium (10-19 air yards) throws: thrown by the passer (passing-depth) or on passes targeting the player (receiving-depth). |
| `medium_fumbles` | integer | Fumbles by the player after the catch on medium passes (10-19 yards downfield). |
| `medium_avoided_tackles` | integer | Tackles avoided after the catch on medium passes (10-19 yards downfield). |
| `medium_first_downs` | integer | First downs on medium (10-19 air yards) throws: passing first downs (passing-depth) or first-down receptions by the player (receiving-depth). |
| `medium_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on medium passes (10-19 yards downfield). |
| `short_targets_percent` | numeric | Share of the team's targets thrown to the player on short passes (0-9 yards downfield). |
| `short_targets` | integer | Pass targets to the player on short passes (0-9 yards downfield). |
| `short_receptions` | integer | Receptions made on short passes (0-9 yards downfield). |
| `short_caught_percent` | numeric | Percentage of targets caught on short passes (0-9 yards downfield). |
| `short_yards` | integer | Yards on short (0-9 air yards) throws: passing yards (passing-depth) or the player's receiving yards (receiving-depth). |
| `short_yards_per_reception` | numeric | Average yards per reception on short passes (0-9 yards downfield). |
| `short_touchdowns` | integer | Touchdowns on short (0-9 air yards) throws: thrown by the passer (passing-depth) or scored by the player (receiving-depth). |
| `short_grades_pass_route` | numeric | PFF route-running (receiving) grade on short passes (0-9 yards downfield), 0-100. |
| `short_grades_hands_drop` | numeric | PFF hands/drop grade on short passes (0-9 yards downfield), 0-100. |
| `short_yards_after_catch` | integer | Yards gained after the catch on short passes (0-9 yards downfield). |
| `short_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on short passes (0-9 yards downfield). |
| `short_yprr` | numeric | Yards per route run on short passes (0-9 yards downfield). |
| `short_avg_depth_of_target` | numeric | Average depth of target, in air yards, on short (0-9 air yards) throws: of the passer's throws (passing-depth) or the player's targets (receiving-depth). |
| `short_drops` | integer | Catchable passes dropped on short (0-9 air yards) throws: by the passer's receivers (passing-depth) or by the player (receiving-depth). |
| `short_drop_rate` | numeric | Percentage of catchable passes dropped on short (0-9 air yards) throws: by the passer's receivers (passing-depth) or by the player (receiving-depth). |
| `short_contested_targets` | integer | PFF-charted contested targets on short passes (0-9 yards downfield). |
| `short_contested_receptions` | integer | Catches made on PFF-charted contested targets on short passes (0-9 yards downfield). |
| `short_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on short passes (0-9 yards downfield). |
| `short_interceptions` | integer | Interceptions on short (0-9 air yards) throws: thrown by the passer (passing-depth) or on passes targeting the player (receiving-depth). |
| `short_fumbles` | integer | Fumbles by the player after the catch on short passes (0-9 yards downfield). |
| `short_avoided_tackles` | integer | Tackles avoided after the catch on short passes (0-9 yards downfield). |
| `short_first_downs` | integer | First downs on short (0-9 air yards) throws: passing first downs (passing-depth) or first-down receptions by the player (receiving-depth). |
| `short_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on short passes (0-9 yards downfield). |
| `behind_los_targets_percent` | numeric | Share of the team's targets thrown to the player on passes thrown behind the line of scrimmage. |
| `behind_los_targets` | integer | Pass targets to the player on passes thrown behind the line of scrimmage. |
| `behind_los_receptions` | integer | Receptions made on passes thrown behind the line of scrimmage. |
| `behind_los_caught_percent` | numeric | Percentage of targets caught on passes thrown behind the line of scrimmage. |
| `behind_los_yards` | integer | Yards on throws behind the line of scrimmage: passing yards (passing-depth) or the player's receiving yards (receiving-depth). |
| `behind_los_yards_per_reception` | numeric | Average yards per reception on passes thrown behind the line of scrimmage. |
| `behind_los_touchdowns` | integer | Touchdowns on throws behind the line of scrimmage: thrown by the passer (passing-depth) or scored by the player (receiving-depth). |
| `behind_los_grades_pass_route` | numeric | PFF route-running (receiving) grade on passes thrown behind the line of scrimmage, 0-100. |
| `behind_los_grades_hands_drop` | numeric | PFF hands/drop grade on passes thrown behind the line of scrimmage, 0-100. |
| `behind_los_yards_after_catch` | integer | Yards gained after the catch on passes thrown behind the line of scrimmage. |
| `behind_los_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on passes thrown behind the line of scrimmage. |
| `behind_los_yprr` | numeric | Yards per route run on passes thrown behind the line of scrimmage. |
| `behind_los_avg_depth_of_target` | numeric | Average depth of target, in air yards, on throws behind the line of scrimmage: of the passer's throws (passing-depth) or the player's targets (receiving-depth). |
| `behind_los_drops` | integer | Catchable passes dropped on throws behind the line of scrimmage: by the passer's receivers (passing-depth) or by the player (receiving-depth). |
| `behind_los_drop_rate` | numeric | Percentage of catchable passes dropped on throws behind the line of scrimmage: by the passer's receivers (passing-depth) or by the player (receiving-depth). |
| `behind_los_contested_targets` | integer | PFF-charted contested targets on passes thrown behind the line of scrimmage. |
| `behind_los_contested_receptions` | integer | Catches made on PFF-charted contested targets on passes thrown behind the line of scrimmage. |
| `behind_los_contested_catch_rate` | integer | Percentage of PFF-charted contested targets caught on passes thrown behind the line of scrimmage. |
| `behind_los_interceptions` | integer | Interceptions on throws behind the line of scrimmage: thrown by the passer (passing-depth) or on passes targeting the player (receiving-depth). |
| `behind_los_fumbles` | integer | Fumbles by the player after the catch on passes thrown behind the line of scrimmage. |
| `behind_los_avoided_tackles` | integer | Tackles avoided after the catch on passes thrown behind the line of scrimmage. |
| `behind_los_first_downs` | integer | First downs on throws behind the line of scrimmage: passing first downs (passing-depth) or first-down receptions by the player (receiving-depth). |
| `behind_los_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on passes thrown behind the line of scrimmage. |

**rushing**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `player` | character | Player's display name as PFF lists it (e.g. "Tom Brady"). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P, LS). |
| `targets` | integer | Targets: passes thrown to the player (receiving, rushing reports) or into the player's coverage (coverage, defense reports). |
| `grades_pass_block` | numeric | PFF pass-blocking grade (0-100). |
| `grades_offense` | numeric | PFF overall offense grade (0-100). |
| `yards_after_contact` | integer | Yards after contact. |
| `explosive` | integer | Runs PFF designates as explosive. |
| `grades_pass_route` | numeric | PFF receiving/route grade (0-100). |
| `draft_season` | integer | Season of the player's NFL draft class, per PFF. |
| `elu_rush_mtf` | integer | Missed tackles forced as a rusher, an input to PFF's elusive rating. |
| `breakaway_attempts` | integer | Runs of 15 or more yards, PFF's breakaway designation. |
| `designed_yards` | integer | Rushing yards gained on designed runs, excluding scrambles. |
| `team_name` | character | Team name/abbreviation the player is credited to for the range. |
| `yprr` | numeric | Yards per route run. |
| `breakaway_percent` | numeric | Share of rushing yards gained on breakaway runs of 15 or more yards. |
| `fumbles` | integer | Fumbles by the player: after the catch on the receiving report, as a ball carrier on the rushing report. |
| `first_downs` | integer | First downs gained: passing first downs on the passing report, first-down receptions on receiving, rushing first downs on rushing. |
| `elusive_rating` | numeric | PFF elusive rating. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `breakaway_yards` | integer | Breakaway (long-run) yards. |
| `player_game_count` | integer | Number of games the player appeared in over the covered span. |
| `eligible_season` | integer | Season of the player's NFL draft eligibility, per PFF. |
| `total_touches` | integer | Combined carries and receptions. |
| `scramble_yards` | integer | Rushing yards gained on scrambles. |
| `yco_attempt` | numeric | Average yards after contact per rushing attempt. |
| `yards` | integer | Yards in the report's play family: passing yards (passing), receiving yards (receiving), rushing yards (rushing), gross punt yards (punting), and receiving yards allowed into the player's coverage (coverage, defense). |
| `grades_run_block` | numeric | PFF run-blocking grade (0-100). |
| `receptions` | integer | Receptions: passes caught by the player (receiving, rushing reports) or completions allowed into the player's coverage (coverage, defense reports). |
| `zone_attempts` | integer | Rushing attempts on zone-scheme runs. |
| `scrambles` | integer | Quarterback scrambles (pass plays on which the passer ran with the ball). |
| `grades_run` | numeric | PFF rushing grade (0-100). |
| `penalties` | integer | Penalties charged to the player over the covered span (see declined_penalties for the ones that were declined). |
| `attempts` | integer | Attempts in the report's play family: pass attempts on the passing report, carries on rushing, punts on punting and kickoffs on the kickoffs report. |
| `elu_yco` | integer | Yards-after-contact component used in PFF's elusive rating. |
| `elu_recv_mtf` | integer | Missed tackles forced as a receiver, an input to PFF's elusive rating. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | integer | Penalties committed by the player that were declined. |
| `ypa` | numeric | Yards per attempt: passing yards per pass attempt on the passing report, rushing yards per carry on the rushing report. |
| `drops` | integer | Dropped passes: drops by the passer's receivers on the passing report, drops by the player on the receiving and rushing reports. |
| `grades_hands_fumble` | numeric | PFF ball-security (hands/fumble) grade, 0-100. |
| `longest` | integer | Longest play in yards: longest reception (receiving), longest run (rushing), or longest completion allowed in coverage (coverage, defense). |
| `routes` | integer | Pass routes run by the player. |
| `franchise_id` | integer | PFF franchise (team) id (integer join key). |
| `rec_yards` | integer | Receiving yards gained by the player (the rushing report also carries the ball carrier's receiving line). |
| `gap_attempts` | integer | Rushing attempts on gap-scheme runs. |
| `run_plays` | integer | Run-play snaps. |
| `avoided_tackles` | integer | Missed tackles forced (tackles avoided) by the player: after the catch on the receiving report, as a runner on rushing. |
| `grades_offense_penalty` | numeric | PFF offensive penalty grade, 0-100. |
| `touchdowns` | integer | Touchdowns: passing touchdowns thrown (passing), receiving and rushing touchdowns scored (receiving, rushing), or touchdowns allowed into the player's coverage (coverage, defense). |
| `carry_share` | numeric | Percentage of the team's carries taken by the player. |
| `offense_pos_graded_rate` | numeric | Percentage of the player's offensive plays that PFF graded positively. |
| `offense_neg_graded_rate` | numeric | Percentage of the player's offensive plays that PFF graded negatively. |
| `grades_pass` | numeric | PFF passing grade (0-100). |

**blocking**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `player` | character | Player's display name as PFF lists it (e.g. "Tom Brady"). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P, LS). |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `franchise_id` | integer | PFF franchise (team) id (integer join key). |
| `team_abbreviation` | character | Abbreviation of the team the player is credited to for the range (the /v2 counterpart of /v1's team). |
| `games_played` | integer | Number of games the player appeared in over the requested span (the /v2 counterpart of /v1's player_game_count). |
| `draft_season` | integer | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | integer | Season of the player's NFL draft eligibility, per PFF. |
| `snap_counts_offense` | integer | Offensive snaps played. |
| `snap_counts_block` | integer | Total blocking snaps played. |
| `block_percent` | numeric | Share of offensive snaps spent blocking. |
| `snap_counts_run_block` | integer | Run-blocking snaps played. |
| `snap_counts_pass_block` | integer | Pass-blocking snaps played. |
| `pass_block_percent` | numeric | Share of pass-play snaps spent pass blocking. |
| `grades_offense` | numeric | PFF overall offense grade (0-100). |
| `grades_run_block` | numeric | PFF run-blocking grade (0-100). |
| `grades_pass_block` | numeric | PFF pass-blocking grade (0-100). |
| `non_spike_pass_block` | integer | Pass-blocking snaps excluding spike plays. |
| `non_spike_pass_block_percentage` | numeric | Share of non-spike pass-play snaps spent pass blocking. |
| `sacks_allowed` | integer | Sacks allowed by the player in pass protection. |
| `hits_allowed` | integer | Quarterback hits allowed. |
| `hurries_allowed` | integer | Quarterback hurries allowed. |
| `pressures_allowed` | integer | Total pressures allowed (sacks, hits, and hurries). |
| `pbe` | numeric | PFF Pass Blocking Efficiency rating, pressures allowed per pass-blocking snap weighted toward sacks. |
| `penalties` | integer | Penalties charged to the player over the covered span (see declined_penalties for the ones that were declined). |
| `declined_penalties` | integer | Penalties committed by the player that were declined. |
| `snap_counts_lt` | integer | Snaps aligned at left tackle. |
| `snap_counts_lg` | integer | Snaps aligned at left guard. |
| `snap_counts_ce` | integer | Snaps aligned at center. |
| `snap_counts_rg` | integer | Snaps aligned at right guard. |
| `snap_counts_rt` | integer | Snaps aligned at right tackle. |
| `snap_counts_te` | integer | Snaps aligned at tight end. |

**pass-blocking**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `player` | character | Player's display name as PFF lists it (e.g. "Tom Brady"). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P, LS). |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `franchise_id` | integer | PFF franchise (team) id (integer join key). |
| `team_abbreviation` | character | Abbreviation of the team the player is credited to for the range (the /v2 counterpart of /v1's team). |
| `games_played` | integer | Number of games the player appeared in over the requested span (the /v2 counterpart of /v1's player_game_count). |
| `draft_season` | integer | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | integer | Season of the player's NFL draft eligibility, per PFF. |
| `snap_counts_lt` | integer | Snaps aligned at left tackle. |
| `snap_counts_lg` | integer | Snaps aligned at left guard. |
| `snap_counts_ce` | integer | Snaps aligned at center. |
| `snap_counts_rg` | integer | Snaps aligned at right guard. |
| `snap_counts_rt` | integer | Snaps aligned at right tackle. |
| `snap_counts_te` | integer | Snaps aligned at tight end. |
| `snap_counts_pass_play` | integer | Pass-play snaps. |
| `snap_counts_pass_block` | integer | Pass-blocking snaps played. |
| `pass_block_percent` | numeric | Share of pass-play snaps spent pass blocking. |
| `grades_pass_block` | numeric | PFF pass-blocking grade (0-100). |
| `penalties` | integer | Penalties charged to the player over the covered span (see declined_penalties for the ones that were declined). |
| `declined_penalties` | integer | Penalties committed by the player that were declined. |
| `non_spike_pass_block` | integer | Pass-blocking snaps excluding spike plays. |
| `non_spike_pass_block_percentage` | numeric | Share of non-spike pass-play snaps spent pass blocking. |
| `sacks_allowed` | integer | Sacks allowed by the player in pass protection. |
| `hits_allowed` | integer | Quarterback hits allowed. |
| `hurries_allowed` | integer | Quarterback hurries allowed. |
| `pressures_allowed` | integer | Total pressures allowed (sacks, hits, and hurries). |
| `pressure_rate_allowed` | numeric | Percentage of the player's pass-blocking snaps on which he allowed a pressure. |
| `sack_rate_allowed` | numeric | Percentage of the player's pass-blocking snaps on which he allowed a sack. |
| `pbe` | numeric | PFF Pass Blocking Efficiency rating, pressures allowed per pass-blocking snap weighted toward sacks. |
| `true_pass_set_snap_counts_pass_play` | integer | Pass-play snaps on PFF-designated true pass sets. |
| `true_pass_set_snap_counts_pass_block` | integer | Pass-blocking snaps played on PFF-designated true pass sets. |
| `true_pass_set_pass_block_percent` | numeric | Share of pass-play snaps spent pass blocking on PFF-designated true pass sets. |
| `true_pass_set_grades_pass_block` | numeric | PFF pass-blocking grade on PFF-designated true pass sets, 0-100. |
| `true_pass_set_non_spike_pass_block` | integer | Pass-blocking snaps excluding spike plays on PFF-designated true pass sets. |
| `true_pass_set_non_spike_pass_block_percentage` | numeric | Share of non-spike pass-play snaps spent pass blocking on PFF-designated true pass sets. |
| `true_pass_set_sacks_allowed` | integer | Sacks allowed on PFF-designated true pass sets. |
| `true_pass_set_hits_allowed` | integer | Quarterback hits allowed on PFF-designated true pass sets. |
| `true_pass_set_hurries_allowed` | integer | Quarterback hurries allowed on PFF-designated true pass sets. |
| `true_pass_set_pressures_allowed` | integer | Total pressures allowed (sacks, hits, and hurries) on PFF-designated true pass sets. |
| `true_pass_set_pressure_rate_allowed` | numeric | Percentage of the player's true-pass-set pass-blocking snaps on which he allowed a pressure. |
| `true_pass_set_sack_rate_allowed` | numeric | Percentage of the player's true-pass-set pass-blocking snaps on which he allowed a sack. |
| `true_pass_set_pbe` | numeric | PFF Pass Blocking Efficiency rating, pressures allowed per pass-blocking snap weighted toward sacks on PFF-designated true pass sets. |
| `pbwr` | numeric | Pass-block win rate, in percent: share of the player's pass-blocking snaps PFF charted as won. |
| `true_pass_set_pbwr` | numeric | Pass-block win rate, in percent, on PFF-designated true pass sets. |
| `pass_block_grade_oe_percentile` | numeric | Percentile (0-100) of the player's pass-blocking grade over expectation, as reported by PFF; null for many players. |
| `island_rate` | numeric | Percentage of the player's pass-blocking snaps PFF charted as island (isolated, one-on-one) blocks. |
| `island_pass_block_win_rate` | numeric | Pass-block win rate, in percent, on the player's island (isolated, one-on-one) pass-blocking snaps. |
| `offense_pos_graded_rate` | numeric | Percentage of the player's offensive plays that PFF graded positively. |
| `offense_neg_graded_rate` | numeric | Percentage of the player's offensive plays that PFF graded negatively. |

**run-blocking**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `player` | character | Player's display name as PFF lists it (e.g. "Tom Brady"). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P, LS). |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `franchise_id` | integer | PFF franchise (team) id (integer join key). |
| `team_abbreviation` | character | Abbreviation of the team the player is credited to for the range (the /v2 counterpart of /v1's team). |
| `games_played` | integer | Number of games the player appeared in over the requested span (the /v2 counterpart of /v1's player_game_count). |
| `draft_season` | integer | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | integer | Season of the player's NFL draft eligibility, per PFF. |
| `snap_counts_lt` | integer | Snaps aligned at left tackle. |
| `snap_counts_lg` | integer | Snaps aligned at left guard. |
| `snap_counts_ce` | integer | Snaps aligned at center. |
| `snap_counts_rg` | integer | Snaps aligned at right guard. |
| `snap_counts_rt` | integer | Snaps aligned at right tackle. |
| `snap_counts_te` | integer | Snaps aligned at tight end. |
| `snap_counts_run_play` | integer | Run-play snaps. |
| `snap_counts_run_block` | integer | Run-blocking snaps played. |
| `run_block_percent` | numeric | Share of run-play snaps spent run blocking. |
| `grades_run_block` | numeric | PFF run-blocking grade (0-100). |
| `penalties` | integer | Penalties charged to the player over the covered span (see declined_penalties for the ones that were declined). |
| `declined_penalties` | integer | Penalties committed by the player that were declined. |
| `zone_snap_counts_run_play` | integer | Run-play snaps on zone-scheme runs. |
| `zone_snap_counts_run_block` | integer | Run-blocking snaps played on zone-scheme runs. |
| `zone_snap_counts_run_block_percent` | numeric | Share of the player's run-blocking snaps on zone-scheme runs. |
| `zone_run_block_percent` | numeric | Share of run-play snaps spent run blocking on zone-scheme runs. |
| `zone_grades_run_block` | numeric | PFF run-blocking grade on zone-scheme runs, 0-100. |
| `gap_snap_counts_run_play` | integer | Run-play snaps on gap-scheme runs. |
| `gap_snap_counts_run_block` | integer | Run-blocking snaps played on gap-scheme runs. |
| `gap_snap_counts_run_block_percent` | numeric | Share of the player's run-blocking snaps on gap-scheme runs. |
| `gap_run_block_percent` | numeric | Share of run-play snaps spent run blocking on gap-scheme runs. |
| `gap_grades_run_block` | numeric | PFF run-blocking grade on gap-scheme runs, 0-100. |
| `pos_graded_rate` | numeric | Percentage of the player's plays in the report's phase (run blocking or run defense) that PFF graded positively. |
| `neg_graded_rate` | numeric | Percentage of the player's plays in the report's phase (run blocking or run defense) that PFF graded negatively. |
| `zone_pos_graded_rate` | numeric | Percentage of the player's zone-scheme run-blocking plays that PFF graded positively. |
| `zone_neg_graded_rate` | numeric | Percentage of the player's zone-scheme run-blocking plays that PFF graded negatively. |
| `gap_pos_graded_rate` | numeric | Percentage of the player's gap-scheme run-blocking plays that PFF graded positively. |
| `gap_neg_graded_rate` | numeric | Percentage of the player's gap-scheme run-blocking plays that PFF graded negatively. |
| `offense_pos_graded_rate` | numeric | Percentage of the player's offensive plays that PFF graded positively. |
| `offense_neg_graded_rate` | numeric | Percentage of the player's offensive plays that PFF graded negatively. |

**defense**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `player` | character | Player's display name as PFF lists it (e.g. "Tom Brady"). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P, LS). |
| `targets` | integer | Targets: passes thrown to the player (receiving, rushing reports) or into the player's coverage (coverage, defense reports). |
| `interception_touchdowns` | integer | Touchdowns scored on interception returns. |
| `draft_season` | integer | Season of the player's NFL draft class, per PFF. |
| `forced_fumbles` | integer | Fumbles forced by the player. |
| `missed_tackles` | integer | Missed tackles by the player (on special-teams plays in the special-teams report). |
| `catch_rate` | numeric | Completion percentage allowed on targets into the player's coverage. |
| `team_name` | character | Team name/abbreviation the player is credited to for the range. |
| `tackles` | integer | Tackles made by the player, as charted by PFF (assisted tackles are reported in assists). |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `snap_counts_offball` | integer | Snaps aligned as an off-ball linebacker. |
| `snap_counts_box` | integer | Snaps aligned in the box. |
| `sacks` | integer | Sacks: recorded by the player on the defense and pass-rush reports; times the passer was sacked on the passing report. |
| `snap_counts_pass_rush` | integer | Pass-rush snaps played. |
| `player_game_count` | integer | Number of games the player appeared in over the covered span. |
| `eligible_season` | integer | Season of the player's NFL draft eligibility, per PFF. |
| `snap_counts_dl` | integer | Snaps aligned on the defensive line. |
| `grades_tackle` | numeric | PFF tackling grade, 0-100. |
| `yards` | integer | Yards in the report's play family: passing yards (passing), receiving yards (receiving), rushing yards (rushing), gross punt yards (punting), and receiving yards allowed into the player's coverage (coverage, defense). |
| `receptions` | integer | Receptions: passes caught by the player (receiving, rushing reports) or completions allowed into the player's coverage (coverage, defense reports). |
| `grades_coverage_defense` | numeric | PFF coverage grade (0-100). |
| `hurries` | integer | Quarterback hurries recorded. |
| `interceptions` | integer | Interceptions: thrown by the passer on the passing report, made by the player on the coverage and defense reports, and on passes targeting the player on the receiving report. |
| `snap_counts_coverage` | integer | Coverage snaps played. |
| `snap_counts_dl_over_t` | integer | Defensive-line snaps aligned head-up over the offensive tackle. |
| `snap_counts_dl_a_gap` | integer | Defensive-line snaps aligned in the A gap. |
| `fumble_recoveries` | integer | Opponent fumbles recovered by the player. |
| `grades_run_defense` | numeric | PFF run-defense grade (0-100). |
| `snap_counts_corner` | integer | Snaps aligned at outside cornerback. |
| `hits` | integer | Quarterback hits recorded by the player, as charted by PFF. |
| `penalties` | integer | Penalties charged to the player over the covered span (see declined_penalties for the ones that were declined). |
| `batted_passes` | integer | Passes batted down at the line of scrimmage. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `stops` | integer | Stops, PFF's tackles that constitute a failed play for the offense. |
| `declined_penalties` | integer | Penalties committed by the player that were declined. |
| `total_pressures` | integer | Total quarterback pressures generated (sacks, hits, and hurries). |
| `fumble_recovery_touchdowns` | integer | Touchdowns scored on fumble recoveries. |
| `longest` | integer | Longest play in yards: longest reception (receiving), longest run (rushing), or longest completion allowed in coverage (coverage, defense). |
| `snap_counts_slot` | integer | Snaps aligned in the slot. |
| `missed_tackle_rate` | numeric | Share of tackle attempts the player missed. |
| `grades_defense` | numeric | PFF overall defense grade (0-100). |
| `yards_per_reception` | numeric | Average yards per reception: the player's own on the receiving report, allowed per reception in the player's coverage on the coverage and defense reports. |
| `grades_defense_penalty` | numeric | PFF defensive penalty grade, 0-100. |
| `safeties` | integer | Safeties recorded by the player. |
| `franchise_id` | integer | PFF franchise (team) id (integer join key). |
| `snap_counts_defense` | integer | Total defensive snaps played. |
| `yards_after_catch` | integer | Yards after the catch: gained by the player on the receiving report, allowed after the catch in the player's coverage on the coverage and defense reports. |
| `snap_counts_dl_b_gap` | integer | Defensive-line snaps aligned in the B gap. |
| `pass_break_ups` | integer | Passes broken up. |
| `qb_rating_against` | numeric | NFL passer rating allowed on targets into the player's coverage. |
| `snap_counts_run_defense` | integer | Run-defense snaps played. |
| `tackles_for_loss` | integer | Tackles for loss made by the player. |
| `assists` | integer | Assisted tackles credited to the player. |
| `grades_pass_rush_defense` | numeric | PFF pass-rush grade, 0-100. |
| `snap_counts_fs` | integer | Snaps aligned at free safety. |
| `touchdowns` | integer | Touchdowns: passing touchdowns thrown (passing), receiving and rushing touchdowns scored (receiving, rushing), or touchdowns allowed into the player's coverage (coverage, defense). |
| `snap_counts_dl_outside_t` | integer | Defensive-line snaps aligned outside the offensive tackle. |

**run-defense**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `player` | character | Player's display name as PFF lists it (e.g. "Tom Brady"). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P, LS). |
| `assists` | integer | Assisted tackles credited to the player. |
| `avg_depth_of_tackle` | numeric | Average depth downfield, in yards, at which the player made his tackles on run plays. |
| `declined_penalties` | integer | Penalties committed by the player that were declined. |
| `draft_season` | integer | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | integer | Season of the player's NFL draft eligibility, per PFF. |
| `forced_fumbles` | integer | Fumbles forced by the player. |
| `franchise_id` | integer | PFF franchise (team) id (integer join key). |
| `grades_coverage_defense` | numeric | PFF coverage grade (0-100). |
| `grades_defense` | numeric | PFF overall defense grade (0-100). |
| `grades_defense_penalty` | numeric | PFF defensive penalty grade, 0-100. |
| `grades_pass_rush_defense` | numeric | PFF pass-rush grade, 0-100. |
| `grades_run_defense` | numeric | PFF run-defense grade (0-100). |
| `grades_tackle` | numeric | PFF tackling grade, 0-100. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `missed_tackle_rate` | numeric | Share of tackle attempts the player missed. |
| `missed_tackles` | integer | Missed tackles by the player (on special-teams plays in the special-teams report). |
| `penalties` | integer | Penalties charged to the player over the covered span (see declined_penalties for the ones that were declined). |
| `player_game_count` | integer | Number of games the player appeared in over the covered span. |
| `run_stop_opp` | integer | Run-defense snaps PFF counts as run-stop opportunities. |
| `snap_counts_run` | integer | Run-play snaps: on the offense report, run plays on which the player was the runner rather than a run blocker; on the run-defense report, run-defense snaps played. |
| `stop_percent` | numeric | Percentage of run-stop opportunities converted into stops. |
| `stops` | integer | Stops, PFF's tackles that constitute a failed play for the offense. |
| `tackles` | integer | Tackles made by the player, as charted by PFF (assisted tackles are reported in assists). |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `team_name` | character | Team name/abbreviation the player is credited to for the range. |
| `pos_graded_rate` | numeric | Percentage of the player's plays in the report's phase (run blocking or run defense) that PFF graded positively. |
| `neg_graded_rate` | numeric | Percentage of the player's plays in the report's phase (run blocking or run defense) that PFF graded negatively. |

**pass-rush**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `player` | character | Player's display name as PFF lists it (e.g. "Tom Brady"). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P, LS). |
| `true_pass_set_total_pressures` | integer | Total pressures generated (sacks, hits, and hurries) on PFF-designated true pass sets. |
| `draft_season` | integer | Season of the player's NFL draft class, per PFF. |
| `true_pass_set_prp` | numeric | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks on PFF-designated true pass sets. |
| `true_pass_set_hurries` | integer | Quarterback hurries recorded on PFF-designated true pass sets. |
| `team_name` | character | Team name/abbreviation the player is credited to for the range. |
| `prp` | numeric | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks. |
| `true_pass_set_sacks` | integer | Sacks recorded on PFF-designated true pass sets. |
| `pass_rush_win_rate` | numeric | Percentage of pass-rush snaps with a PFF-charted pass-rush win. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `sacks` | integer | Sacks: recorded by the player on the defense and pass-rush reports; times the passer was sacked on the passing report. |
| `snap_counts_pass_rush` | integer | Pass-rush snaps played. |
| `player_game_count` | integer | Number of games the player appeared in over the covered span. |
| `eligible_season` | integer | Season of the player's NFL draft eligibility, per PFF. |
| `true_pass_set_snap_counts_pass_play` | integer | Pass-play snaps on PFF-designated true pass sets. |
| `pass_rush_wins` | integer | PFF-charted pass-rush wins. |
| `hurries` | integer | Quarterback hurries recorded. |
| `pass_rush_opp` | integer | Pass-rush snaps PFF counts as pressure opportunities. |
| `snap_counts_pass_play` | integer | Pass-play snaps. |
| `hits` | integer | Quarterback hits recorded by the player, as charted by PFF. |
| `true_pass_set_pass_rush_win_rate` | numeric | Percentage of pass-rush snaps with a PFF-charted pass-rush win on PFF-designated true pass sets. |
| `penalties` | integer | Penalties charged to the player over the covered span (see declined_penalties for the ones that were declined). |
| `batted_passes` | integer | Passes batted down at the line of scrimmage. |
| `true_pass_set_hits` | integer | Quarterback hits recorded on PFF-designated true pass sets. |
| `true_pass_set_snap_counts_pass_rush` | integer | Pass-rush snaps played on PFF-designated true pass sets. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | integer | Penalties committed by the player that were declined. |
| `true_pass_set_pass_rush_wins` | integer | PFF-charted pass-rush wins on PFF-designated true pass sets. |
| `total_pressures` | integer | Total quarterback pressures generated (sacks, hits, and hurries). |
| `true_pass_set_grades_pass_rush_defense` | numeric | PFF pass-rush grade on PFF-designated true pass sets, 0-100. |
| `true_pass_set_batted_passes` | integer | Passes batted down at the line of scrimmage on PFF-designated true pass sets. |
| `franchise_id` | integer | PFF franchise (team) id (integer join key). |
| `pass_rush_percent` | numeric | Share of pass-play snaps spent rushing the passer. |
| `true_pass_set_pass_rush_opp` | integer | Pass-rush snaps PFF counts as pressure opportunities on PFF-designated true pass sets. |
| `true_pass_set_pass_rush_percent` | numeric | Share of pass-play snaps spent rushing the passer on PFF-designated true pass sets. |
| `grades_pass_rush_defense` | numeric | PFF pass-rush grade, 0-100. |
| `knockdowns` | integer | Quarterback knockdowns credited to the pass rusher, as charted by PFF. |
| `knockdown_rate` | numeric | Knockdowns as a percentage of the player's pass-rush snaps. |
| `pressure_rate` | numeric | Percentage of the player's pass-rush snaps that produced a pressure. |
| `sack_rate` | numeric | Percentage of the player's pass-rush snaps that produced a sack. |
| `true_pass_set_knockdowns` | integer | Quarterback knockdowns credited to the pass rusher on PFF-designated true pass sets. |
| `true_pass_set_knockdown_rate` | numeric | Knockdowns as a percentage of the player's true-pass-set pass-rush snaps. |
| `true_pass_set_pressure_rate` | numeric | Percentage of the player's true-pass-set pass-rush snaps that produced a pressure. |
| `true_pass_set_sack_rate` | numeric | Percentage of the player's true-pass-set pass-rush snaps that produced a sack. |
| `lhs_pass_rush_snaps` | integer | Pass-rush snaps the player took from the left-hand side (LHS), as PFF charts the rusher's alignment. |
| `lhs_pass_rush_percent` | numeric | Percentage of the player's side-charted pass-rush snaps taken from the left-hand side (LHS); lhs_pass_rush_percent and rhs_pass_rush_percent sum to 100. |
| `lhs_sacks` | integer | Sacks recorded while rushing from the left-hand side (LHS). |
| `lhs_hits` | integer | Quarterback hits recorded while rushing from the left-hand side (LHS). |
| `lhs_hurries` | integer | Quarterback hurries recorded while rushing from the left-hand side (LHS). |
| `lhs_pressures` | integer | Total pressures (sacks, hits, and hurries) generated while rushing from the left-hand side (LHS). |
| `lhs_prp` | numeric | PFF Pass Rush Productivity rating on rushes from the left-hand side (LHS), pressure generated per pass-rush snap weighted toward sacks. |
| `lhs_stops` | integer | Stops (tackles that constitute a failed play for the offense) made on snaps rushing from the left-hand side (LHS). |
| `lhs_tackles` | integer | Tackles made on snaps rushing from the left-hand side (LHS). |
| `lhs_assists` | integer | Assisted tackles on snaps rushing from the left-hand side (LHS). |
| `lhs_misses` | integer | Missed tackles on snaps rushing from the left-hand side (LHS). |
| `rhs_pass_rush_snaps` | integer | Pass-rush snaps the player took from the right-hand side (RHS), as PFF charts the rusher's alignment. |
| `rhs_pass_rush_percent` | numeric | Percentage of the player's side-charted pass-rush snaps taken from the right-hand side (RHS); lhs_pass_rush_percent and rhs_pass_rush_percent sum to 100. |
| `rhs_sacks` | integer | Sacks recorded while rushing from the right-hand side (RHS). |
| `rhs_hits` | integer | Quarterback hits recorded while rushing from the right-hand side (RHS). |
| `rhs_hurries` | integer | Quarterback hurries recorded while rushing from the right-hand side (RHS). |
| `rhs_pressures` | integer | Total pressures (sacks, hits, and hurries) generated while rushing from the right-hand side (RHS). |
| `rhs_prp` | numeric | PFF Pass Rush Productivity rating on rushes from the right-hand side (RHS), pressure generated per pass-rush snap weighted toward sacks. |
| `rhs_stops` | integer | Stops (tackles that constitute a failed play for the offense) made on snaps rushing from the right-hand side (RHS). |
| `rhs_tackles` | integer | Tackles made on snaps rushing from the right-hand side (RHS). |
| `rhs_assists` | integer | Assisted tackles on snaps rushing from the right-hand side (RHS). |
| `rhs_misses` | integer | Missed tackles on snaps rushing from the right-hand side (RHS). |
| `pass_rush_grade_oe_percentile` | numeric | Percentile (0-100) of the player's pass-rush grade over expectation, as reported by PFF. |
| `double_team_rate` | numeric | Percentage of the player's pass-rush snaps on which he was double-teamed. |

**coverage**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `player` | character | Player's display name as PFF lists it (e.g. "Tom Brady"). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P, LS). |
| `targets` | integer | Targets: passes thrown to the player (receiving, rushing reports) or into the player's coverage (coverage, defense reports). |
| `yards_per_coverage_snap` | numeric | Receiving yards allowed per coverage snap. |
| `draft_season` | integer | Season of the player's NFL draft class, per PFF. |
| `missed_tackles` | integer | Missed tackles by the player (on special-teams plays in the special-teams report). |
| `catch_rate` | numeric | Completion percentage allowed on targets into the player's coverage. |
| `team_name` | character | Team name/abbreviation the player is credited to for the range. |
| `tackles` | integer | Tackles made by the player, as charted by PFF (assisted tackles are reported in assists). |
| `coverage_percent` | numeric | Share of pass-play snaps spent in coverage. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `dropped_ints` | integer | Interception chances PFF charted as dropped. |
| `player_game_count` | integer | Number of games the player appeared in over the covered span. |
| `eligible_season` | integer | Season of the player's NFL draft eligibility, per PFF. |
| `grades_tackle` | numeric | PFF tackling grade, 0-100. |
| `yards` | integer | Yards in the report's play family: passing yards (passing), receiving yards (receiving), rushing yards (rushing), gross punt yards (punting), and receiving yards allowed into the player's coverage (coverage, defense). |
| `receptions` | integer | Receptions: passes caught by the player (receiving, rushing reports) or completions allowed into the player's coverage (coverage, defense reports). |
| `forced_incompletion_rate` | numeric | Share of targets into the player's coverage with a PFF-charted forced incompletion. |
| `grades_coverage_defense` | numeric | PFF coverage grade (0-100). |
| `interceptions` | integer | Interceptions: thrown by the passer on the passing report, made by the player on the coverage and defense reports, and on passes targeting the player on the receiving report. |
| `snap_counts_coverage` | integer | Coverage snaps played. |
| `grades_run_defense` | numeric | PFF run-defense grade (0-100). |
| `snap_counts_pass_play` | integer | Pass-play snaps. |
| `penalties` | integer | Penalties charged to the player over the covered span (see declined_penalties for the ones that were declined). |
| `forced_incompletes` | integer | Incompletions forced by the player's coverage, per PFF charting. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `coverage_snaps_per_target` | numeric | Coverage snaps played per target into the player's coverage. |
| `stops` | integer | Stops, PFF's tackles that constitute a failed play for the offense. |
| `declined_penalties` | integer | Penalties committed by the player that were declined. |
| `longest` | integer | Longest play in yards: longest reception (receiving), longest run (rushing), or longest completion allowed in coverage (coverage, defense). |
| `missed_tackle_rate` | numeric | Share of tackle attempts the player missed. |
| `grades_defense` | numeric | PFF overall defense grade (0-100). |
| `yards_per_reception` | numeric | Average yards per reception: the player's own on the receiving report, allowed per reception in the player's coverage on the coverage and defense reports. |
| `grades_defense_penalty` | numeric | PFF defensive penalty grade, 0-100. |
| `franchise_id` | integer | PFF franchise (team) id (integer join key). |
| `yards_after_catch` | integer | Yards after the catch: gained by the player on the receiving report, allowed after the catch in the player's coverage on the coverage and defense reports. |
| `avg_depth_of_target` | numeric | Average depth of target, in yards downfield: of the passer's throws (passing), of passes to the player (receiving), or of targets into the player's coverage (coverage). |
| `pass_break_ups` | integer | Passes broken up. |
| `qb_rating_against` | numeric | NFL passer rating allowed on targets into the player's coverage. |
| `coverage_snaps_per_reception` | numeric | Coverage snaps played per reception allowed. |
| `assists` | integer | Assisted tackles credited to the player. |
| `grades_pass_rush_defense` | numeric | PFF pass-rush grade, 0-100. |
| `touchdowns` | integer | Touchdowns: passing touchdowns thrown (passing), receiving and rushing touchdowns scored (receiving, rushing), or touchdowns allowed into the player's coverage (coverage, defense). |

**special-teams**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `player` | character | Player's display name as PFF lists it (e.g. "Tom Brady"). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P, LS). |
| `assists` | integer | Assisted tackles credited to the player. |
| `declined_penalties` | integer | Penalties committed by the player that were declined. |
| `draft_season` | integer | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | integer | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | integer | PFF franchise (team) id (integer join key). |
| `grades_fgep_offense` | numeric | PFF grade on the field-goal and extra-point protection unit, 0-100. |
| `grades_long_snap` | numeric | PFF long-snapping grade, 0-100. |
| `grades_misc_st` | numeric | PFF miscellaneous special-teams grade, 0-100. |
| `grades_special_teams_penalty` | numeric | PFF special-teams penalty grade, 0-100. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `missed_tackles` | integer | Missed tackles by the player (on special-teams plays in the special-teams report). |
| `penalties` | integer | Penalties charged to the player over the covered span (see declined_penalties for the ones that were declined). |
| `player_game_count` | integer | Number of games the player appeared in over the covered span. |
| `snap_counts_field_goal` | integer | Snaps on the field-goal and extra-point unit. |
| `snap_counts_field_goal_blocking` | integer | Snaps on the field-goal and extra-point block unit. |
| `snap_counts_kickoff` | integer | Snaps on the kickoff coverage unit. |
| `snap_counts_kickoff_return` | integer | Snaps on the kickoff return unit. |
| `snap_counts_punt_coverage` | integer | Snaps on the punt coverage unit. |
| `snap_counts_punt_return` | integer | Snaps on the punt return unit. |
| `tackles` | integer | Tackles made by the player, as charted by PFF (assisted tackles are reported in assists). |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `team_name` | character | Team name/abbreviation the player is credited to for the range. |
| `total_snaps` | integer | Total special-teams snaps the player played across all kicking-game units. |
| `grades_punter` | numeric | PFF punting grade, 0-100. |
| `grades_fgep_defense` | numeric | PFF grade on field-goal and extra-point defense, 0-100. |
| `grades_kick_return` | numeric | PFF kickoff-return grade, 0-100. |
| `grades_punt_return` | numeric | PFF punt-return grade, 0-100. |
| `grades_fgep_kicker` | numeric | PFF field-goal and extra-point kicking grade, 0-100. |
| `grades_kickoff_kicker` | numeric | PFF kickoff kicking grade, 0-100. |

**kick-returns**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `player` | character | Player's display name as PFF lists it (e.g. "Tom Brady"). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P, LS). |
| `declined_penalties` | integer | Penalties committed by the player that were declined. |
| `draft_season` | integer | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | integer | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | integer | PFF franchise (team) id (integer join key). |
| `grades_kick_return` | numeric | PFF kickoff-return grade, 0-100. |
| `grades_return` | numeric | PFF overall return grade, 0-100. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `kickoff_attempts` | integer | Kickoff returns attempted. |
| `kickoff_fair_catches` | integer | Kickoffs fair-caught by the player. |
| `kickoff_long` | integer | Longest kickoff return in yards. |
| `kickoff_muffed_returns` | integer | Kickoff returns the player muffed. |
| `kickoff_touchdowns` | integer | Kickoff returns scoring a touchdown. |
| `kickoff_yards` | integer | Total kickoff-return yards. |
| `kickoff_ypa` | numeric | Average yards per kickoff return. |
| `penalties` | integer | Penalties charged to the player over the covered span (see declined_penalties for the ones that were declined). |
| `player_game_count` | integer | Number of games the player appeared in over the covered span. |
| `punt_attempts` | integer | Punt returns attempted. |
| `punt_fair_catches` | integer | Punts fair-caught by the player. |
| `punt_long` | integer | Longest punt return in yards. |
| `punt_muffed_returns` | integer | Punt returns the player muffed. |
| `punt_touchdowns` | integer | Punt returns scoring a touchdown. |
| `punt_yards` | integer | Total punt-return yards. |
| `punt_ypa` | integer | Average yards per punt return. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `team_name` | character | Team name/abbreviation the player is credited to for the range. |
| `total_attempts` | integer | Total attempts: field goals attempted across all distances on the field-goals report, kick and punt returns combined on the kick-returns report. |
| `grades_punt_return` | numeric | PFF punt-return grade, 0-100. |

**field-goals**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `player` | character | Player's display name as PFF lists it (e.g. "Tom Brady"). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P, LS). |
| `twenty_attempts` | integer | Field goals attempted from 20-29 yards. |
| `pat_percent` | numeric | Extra-point percentage. |
| `draft_season` | integer | Season of the player's NFL draft class, per PFF. |
| `forty_made` | integer | Field goals made from 40-49 yards. |
| `team_name` | character | Team name/abbreviation the player is credited to for the range. |
| `fifty_percent` | integer | Field-goal percentage from 50 or more yards. |
| `total_made` | integer | Total field goals made across all distances. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `one_made` | integer | Field goals made from 1-19 yards. |
| `fifty_attempts` | integer | Field goals attempted from 50 or more yards. |
| `forty_attempts` | integer | Field goals attempted from 40-49 yards. |
| `player_game_count` | integer | Number of games the player appeared in over the covered span. |
| `eligible_season` | integer | Season of the player's NFL draft eligibility, per PFF. |
| `thirty_percent` | integer | Field-goal percentage from 30-39 yards. |
| `total_attempts` | integer | Total attempts: field goals attempted across all distances on the field-goals report, kick and punt returns combined on the kick-returns report. |
| `pat_attempts` | integer | Extra points attempted. |
| `twenty_made` | integer | Field goals made from 20-29 yards. |
| `one_attempts` | integer | Field goals attempted from 1-19 yards. |
| `grades_fgep_kicker` | numeric | PFF field-goal and extra-point kicking grade, 0-100. |
| `thirty_attempts` | integer | Field goals attempted from 30-39 yards. |
| `penalties` | integer | Penalties charged to the player over the covered span (see declined_penalties for the ones that were declined). |
| `pat_made` | integer | Extra points made. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | integer | Penalties committed by the player that were declined. |
| `one_percent` | integer | Field-goal percentage from 1-19 yards. |
| `total_percent` | numeric | Overall field-goal percentage. |
| `twenty_percent` | numeric | Field-goal percentage from 20-29 yards. |
| `forty_percent` | numeric | Field-goal percentage from 40-49 yards. |
| `franchise_id` | integer | PFF franchise (team) id (integer join key). |
| `fifty_made` | integer | Field goals made from 50 or more yards. |
| `thirty_made` | integer | Field goals made from 30-39 yards. |
| `field_goal_oe` | numeric | Field-goal percentage over expectation, in percentage points (made rate minus PFF's expected make rate). |
| `field_goal_oe_total` | numeric | Field goals made over expectation in total (makes minus PFF-expected makes). |

**punting**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `player` | character | Player's display name as PFF lists it (e.g. "Tom Brady"). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P, LS). |
| `touchbacks` | integer | Kicks resulting in touchbacks: kickoffs on the kickoffs report, punts on the punting report. |
| `attempts_with_hangtime` | integer | Kicks with a PFF-recorded hangtime: kickoffs on the kickoffs report, punts on the punting report. |
| `draft_season` | integer | Season of the player's NFL draft class, per PFF. |
| `percent_returned` | numeric | Percentage of the player's kicks that were returned: kickoffs on the kickoffs report, punts on the punting report. |
| `fair_catches` | integer | Kicks fair-caught by the return team: kickoffs on the kickoffs report, punts on the punting report. |
| `team_name` | character | Team name/abbreviation the player is credited to for the range. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `player_game_count` | integer | Number of games the player appeared in over the covered span. |
| `eligible_season` | integer | Season of the player's NFL draft eligibility, per PFF. |
| `average_net_yards` | numeric | Average net punting yards per attempt. |
| `yards` | integer | Yards in the report's play family: passing yards (passing), receiving yards (receiving), rushing yards (rushing), gross punt yards (punting), and receiving yards allowed into the player's coverage (coverage, defense). |
| `average_hangtime` | numeric | Average hangtime in seconds of the player's kickoffs (kickoffs report) or punts (punting report). |
| `total_net_yards` | integer | Total net punting yards. |
| `penalties` | integer | Penalties charged to the player over the covered span (see declined_penalties for the ones that were declined). |
| `attempts` | integer | Attempts in the report's play family: pass attempts on the passing report, carries on rushing, punts on punting and kickoffs on the kickoffs report. |
| `inside_twenties` | integer | Punts downed inside the opponent 20-yard line. |
| `out_of_bounds` | integer | Punts that went out of bounds. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `average_yards_per_return` | numeric | Average return yards allowed per kick returned: kickoffs on the kickoffs report, punts on the punting report. |
| `total_hangtime` | numeric | Total hangtime in seconds summed over the player's kickoffs (kickoffs report) or punts (punting report). |
| `declined_penalties` | integer | Penalties committed by the player that were declined. |
| `returns` | integer | Punts returned by the opponent. |
| `long` | integer | Longest punt in yards. |
| `blocks` | integer | Punts that were blocked. |
| `average_yards_per_attempt` | numeric | Average gross punting yards per attempt. |
| `franchise_id` | integer | PFF franchise (team) id (integer join key). |
| `grades_punter` | numeric | PFF punting grade, 0-100. |
| `return_yards` | integer | Return yards gained by the return team on the player's kicks: kickoffs on the kickoffs report, punts on the punting report. |
| `downeds` | integer | Punts downed by the coverage unit. |
| `snaps` | integer | Punting snaps played. |

**kickoffs**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `player` | character | Player's display name as PFF lists it (e.g. "Tom Brady"). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P, LS). |
| `attempts` | integer | Attempts in the report's play family: pass attempts on the passing report, carries on rushing, punts on punting and kickoffs on the kickoffs report. |
| `attempts_with_hangtime` | integer | Kicks with a PFF-recorded hangtime: kickoffs on the kickoffs report, punts on the punting report. |
| `average_distance` | numeric | Average kickoff distance in yards. |
| `average_hangtime` | numeric | Average hangtime in seconds of the player's kickoffs (kickoffs report) or punts (punting report). |
| `average_starting_field_position` | numeric | Average opponent starting field position following the player's kickoffs. |
| `average_yards_per_return` | numeric | Average return yards allowed per kick returned: kickoffs on the kickoffs report, punts on the punting report. |
| `declined_penalties` | integer | Penalties committed by the player that were declined. |
| `draft_season` | integer | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | integer | Season of the player's NFL draft eligibility, per PFF. |
| `fair_catches` | integer | Kicks fair-caught by the return team: kickoffs on the kickoffs report, punts on the punting report. |
| `franchise_id` | integer | PFF franchise (team) id (integer join key). |
| `grades_kickoff_kicker` | numeric | PFF kickoff kicking grade, 0-100. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `kicked_yards` | integer | Total kickoff yards. |
| `kicks_returned` | integer | Kickoffs returned by the opponent. |
| `onside_kicks` | integer | Onside kicks attempted. |
| `penalties` | integer | Penalties charged to the player over the covered span (see declined_penalties for the ones that were declined). |
| `percent_returned` | numeric | Percentage of the player's kicks that were returned: kickoffs on the kickoffs report, punts on the punting report. |
| `player_game_count` | integer | Number of games the player appeared in over the covered span. |
| `return_yards` | integer | Return yards gained by the return team on the player's kicks: kickoffs on the kickoffs report, punts on the punting report. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `team_name` | character | Team name/abbreviation the player is credited to for the range. |
| `total_hangtime` | numeric | Total hangtime in seconds summed over the player's kickoffs (kickoffs report) or punts (punting report). |
| `touchbacks` | integer | Kicks resulting in touchbacks: kickoffs on the kickoffs report, punts on the punting report. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_team_report-example}

```python
pff_api_team_report(league='nfl', report='offense', season=2022, team='cincinnati-bengals')
```

_Last validated n/a._
