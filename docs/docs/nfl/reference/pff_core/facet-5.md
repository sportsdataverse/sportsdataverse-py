---
title: "NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet (5)"
sidebar_label: "Facet (5)"
sidebar_position: 5
description: "NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet (5) — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet (5)

## pff_facet_passing_depth

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/passing/depth`

**Valid URL:** [https://premium.pff.com/api/v1/facet/passing/depth](https://premium.pff.com/api/v1/facet/passing/depth)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns {#pff_facet_passing_depth-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `left_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the left side of the field. |
| `left_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the left side of the field. |
| `center_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the center of the field. |
| `right_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the right side of the field. |
| `left_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the left side of the field. |
| `left_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the right side of the field. |
| `right_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage, expressed as a percentage. |
| `right_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `right_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the right side of the field. |
| `deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws, per PFF charting. |
| `center_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the center of the field. |
| `medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws. |
| `left_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the left side of the field. |
| `behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage. |
| `medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws, as charted by PFF. |
| `center_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the center of the field. |
| `behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage. |
| `left_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the left side of the field. |
| `deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws. |
| `center_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the center of the field, per PFF charting. |
| `center_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the center of the field. |
| `center_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the center of the field. |
| `center_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the center of the field. |
| `left_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the center of the field. |
| `center_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the center of the field. |
| `right_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the right side of the field. |
| `center_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `right_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the right side of the field. |
| `right_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws. |
| `center_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the center of the field. |
| `deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws. |
| `right_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the right side of the field. |
| `left_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the left side of the field. |
| `right_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the right side of the field, expressed as a percentage. |
| `right_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the right side of the field, per PFF charting. |
| `center_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the center of the field, expressed as a percentage. |
| `deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws. |
| `center_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the center of the field. |
| `right_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the right side of the field, per PFF charting. |
| `center_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the center of the field. |
| `center_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the left side of the field. |
| `center_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the center of the field. |
| `deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws. |
| `center_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the center of the field. |
| `center_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the center of the field. |
| `right_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the right side of the field. |
| `short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws. |
| `center_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the center of the field. |
| `left_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the left side of the field. |
| `left_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the left side of the field. |
| `left_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the left side of the field. |
| `right_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the right side of the field. |
| `draft_season` | numeric | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `right_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage, as charted by PFF. |
| `right_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the right side of the field. |
| `deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws, as charted by PFF. |
| `right_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the right side of the field, per PFF charting. |
| `center_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the left side of the field. |
| `deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws. |
| `center_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `center_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the center of the field. |
| `right_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `center_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the center of the field. |
| `medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws. |
| `right_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the center of the field. |
| `medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws. |
| `right_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage. |
| `center_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the left side of the field, per PFF charting. |
| `right_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the right side of the field, per PFF charting. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws. |
| `center_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the center of the field. |
| `left_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws. |
| `right_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the right side of the field. |
| `center_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the center of the field. |
| `deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws. |
| `left_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the left side of the field, per PFF charting. |
| `medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws. |
| `center_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the center of the field. |
| `short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws, expressed as a percentage. |
| `right_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the right side of the field. |
| `center_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the center of the field. |
| `left_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the left side of the field. |
| `center_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the center of the field. |
| `center_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the center of the field. |
| `left_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws. |
| `right_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the right side of the field. |
| `left_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the center of the field. |
| `right_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the right side of the field. |
| `right_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the right side of the field. |
| `left_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the left side of the field. |
| `center_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the center of the field, per PFF charting. |
| `right_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the right side of the field. |
| `left_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the left side of the field. |
| `right_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the right side of the field. |
| `left_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the left side of the field. |
| `left_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the right side of the field. |
| `left_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the center of the field. |
| `right_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws, as charted by PFF. |
| `right_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the right side of the field. |
| `medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws, as charted by PFF. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `left_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the left side of the field. |
| `left_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the left side of the field. |
| `center_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the center of the field, per PFF charting. |
| `right_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the left side of the field. |
| `left_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the left side of the field. |
| `deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws. |
| `right_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage. |
| `right_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the right side of the field. |
| `right_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the right side of the field. |
| `right_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `right_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the right side of the field. |
| `short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws. |
| `right_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the right side of the field, per PFF charting. |
| `right_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the right side of the field. |
| `deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws. |
| `center_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the center of the field. |
| `behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage, as charted by PFF. |
| `right_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the right side of the field. |
| `medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws. |
| `short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws. |
| `center_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the center of the field, per PFF charting. |
| `left_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the left side of the field. |
| `short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws, as charted by PFF. |
| `short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws. |
| `right_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the right side of the field. |
| `medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws. |
| `left_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the left side of the field. |
| `center_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the center of the field, expressed as a percentage. |
| `left_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the left side of the field. |
| `center_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the center of the field. |
| `left_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the center of the field. |
| `center_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the center of the field. |
| `right_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the right side of the field, per PFF charting. |
| `right_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the right side of the field. |
| `medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws, per PFF charting. |
| `center_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the center of the field, per PFF charting. |
| `center_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the center of the field. |
| `left_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the left side of the field. |
| `right_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the right side of the field. |
| `left_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the right side of the field. |
| `left_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws, plays PFF charts as deserving of a turnover. |
| `left_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the left side of the field. |
| `left_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `base_dropbacks` | numeric | Number of dropbacks across all splits, the baseline total for this facet. |
| `center_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the center of the field. |
| `center_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the center of the field. |
| `right_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the center of the field. |
| `right_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the right side of the field. |
| `left_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the left side of the field. |
| `center_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the center of the field. |
| `medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws. |
| `medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws. |
| `left_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the left side of the field. |
| `deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws, per PFF charting. |
| `left_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the left side of the field. |
| `short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws, per PFF charting. |
| `short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws. |
| `left_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the left side of the field. |
| `short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws. |
| `center_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage. |
| `right_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws. |
| `player_game_count` | numeric | Number of games the player appeared in during the period covered. |
| `short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws, per PFF charting. |
| `right_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the right side of the field. |
| `center_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the center of the field. |
| `right_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the right side of the field. |
| `behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage. |
| `right_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws. |
| `medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws. |
| `short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws. |
| `right_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the right side of the field, expressed as a percentage. |
| `eligible_season` | numeric | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage. |
| `center_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the center of the field. |
| `right_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the right side of the field. |
| `center_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the left side of the field. |
| `center_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the center of the field. |
| `right_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws. |
| `right_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the right side of the field. |
| `medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws. |
| `left_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the left side of the field. |
| `right_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the right side of the field. |
| `center_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `left_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the left side of the field. |
| `center_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the center of the field. |
| `right_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the right side of the field. |
| `left_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the left side of the field. |
| `deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws. |
| `right_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the right side of the field. |
| `center_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the center of the field, expressed as a percentage. |
| `right_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the right side of the field. |
| `left_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the left side of the field. |
| `deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws. |
| `left_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the left side of the field. |
| `right_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage. |
| `right_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the right side of the field. |
| `left_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `left_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the left side of the field. |
| `deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws. |
| `left_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the left side of the field. |
| `medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws. |
| `left_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the left side of the field. |
| `right_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the right side of the field. |
| `medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws. |
| `behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage. |
| `left_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the left side of the field. |
| `center_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the center of the field. |
| `center_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the center of the field. |
| `right_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the right side of the field. |
| `right_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the right side of the field. |
| `right_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the right side of the field. |
| `right_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage. |
| `center_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the center of the field. |
| `left_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the center of the field. |
| `right_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `left_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the left side of the field. |
| `right_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the left side of the field. |
| `center_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the center of the field, per PFF charting. |
| `medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws. |
| `left_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the left side of the field. |
| `short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws. |
| `right_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the left side of the field. |
| `right_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the right side of the field. |
| `short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws, plays PFF charts as deserving of a turnover. |
| `right_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the right side of the field. |
| `right_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the right side of the field. |
| `right_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the right side of the field. |
| `medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws. |
| `center_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the center of the field, expressed as a percentage. |
| `left_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the left side of the field. |
| `behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage. |
| `short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws. |
| `left_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the left side of the field. |
| `left_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the left side of the field. |
| `behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage. |
| `behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage. |
| `left_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the left side of the field. |
| `left_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `center_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the center of the field. |
| `left_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the left side of the field. |
| `left_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the left side of the field. |
| `center_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the center of the field. |
| `center_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the center of the field. |
| `short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws. |
| `left_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the left side of the field. |
| `deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws. |
| `center_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the center of the field, plays PFF charts as deserving of a turnover. |
| `medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws. |
| `left_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the left side of the field. |
| `left_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the left side of the field. |
| `deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws. |
| `short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws. |
| `left_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the center of the field. |
| `center_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `left_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the center of the field. |
| `right_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the right side of the field. |
| `right_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws. |
| `left_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the left side of the field. |
| `right_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the right side of the field. |
| `right_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the right side of the field, per PFF charting. |
| `center_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the center of the field. |
| `right_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the right side of the field. |
| `center_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage. |
| `right_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the right side of the field, per PFF charting. |
| `right_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `right_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the right side of the field. |
| `deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `left_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the center of the field. |
| `right_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the right side of the field. |
| `left_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the left side of the field. |
| `short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws. |
| `medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws. |
| `left_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the left side of the field. |
| `right_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage, per PFF charting. |
| `medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws. |
| `center_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the center of the field. |
| `behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage. |
| `right_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the right side of the field. |
| `center_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the center of the field. |
| `left_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `left_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the left side of the field. |
| `center_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the center of the field. |
| `center_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the center of the field. |
| `left_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the left side of the field, per PFF charting. |
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `right_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the right side of the field, expressed as a percentage. |
| `right_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage. |
| `center_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the center of the field. |
| `short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws. |
| `left_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the center of the field. |
| `behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage. |
| `left_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the left side of the field. |
| `medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws, per PFF charting. |
| `left_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws, plays PFF charts as deserving of a turnover. |
| `behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage, per PFF charting. |
| `left_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the left side of the field. |
| `left_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the left side of the field. |
| `center_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the center of the field. |
| `right_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the right side of the field. |
| `medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws. |
| `center_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the center of the field. |
| `center_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the center of the field. |
| `center_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the center of the field. |
| `behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage. |
| `left_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws. |
| `center_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the center of the field. |
| `behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage. |
| `right_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the right side of the field. |
| `left_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the left side of the field. |
| `right_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the right side of the field. |
| `right_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the right side of the field, plays PFF charts as deserving of a turnover. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `right_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the right side of the field. |
| `behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage, as charted by PFF. |
| `left_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the left side of the field. |
| `medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws. |
| `deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws, as charted by PFF. |
| `right_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the right side of the field. |
| `short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws. |
| `deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws. |
| `declined_penalties` | numeric | Number of declined penalties committed by the player. |
| `deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws. |
| `left_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the left side of the field. |
| `right_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the right side of the field. |
| `medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws, expressed as a percentage. |
| `right_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the right side of the field. |
| `left_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `left_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the left side of the field. |
| `right_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `right_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the right side of the field, expressed as a percentage. |
| `center_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws, as charted by PFF. |
| `right_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `left_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the left side of the field. |
| `center_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the center of the field. |
| `right_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the right side of the field. |
| `deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws. |
| `left_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the left side of the field, per PFF charting. |
| `right_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the center of the field. |
| `short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws. |
| `left_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the left side of the field, per PFF charting. |
| `right_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the right side of the field. |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws, as charted by PFF. |
| `center_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the center of the field. |
| `center_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the center of the field. |
| `right_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the right side of the field. |
| `left_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the left side of the field. |
| `medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws. |
| `left_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the left side of the field. |
| `left_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the left side of the field. |
| `medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws. |
| `center_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the left side of the field. |
| `deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws. |
| `left_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the left side of the field. |
| `left_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the right side of the field. |
| `left_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the left side of the field. |
| `left_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the left side of the field. |
| `medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws. |
| `behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage, plays PFF charts as deserving of a turnover. |
| `right_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the right side of the field. |
| `right_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the right side of the field. |
| `deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws. |
| `center_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the center of the field. |
| `left_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the left side of the field. |
| `left_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `center_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the center of the field. |
| `center_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the center of the field. |
| `right_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the right side of the field. |
| `center_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the center of the field. |
| `deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws. |
| `left_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the left side of the field, expressed as a percentage. |
| `short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws. |
| `behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage. |
| `right_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the right side of the field. |
| `base_attempts` | numeric | Number of pass attempts across all splits, the baseline total for this facet. |
| `center_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the center of the field. |
| `center_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `center_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `left_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the left side of the field. |
| `behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage. |
| `left_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the left side of the field, expressed as a percentage. |
| `center_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `player` | character | Player's display name as PFF lists it. |
| `left_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the left side of the field. |
| `behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage. |
| `center_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws. |
| `left_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the left side of the field, per PFF charting. |
| `center_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `right_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the right side of the field. |
| `right_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the right side of the field. |
| `center_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `left_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the left side of the field. |
| `left_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the left side of the field. |
| `left_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the left side of the field, per PFF charting. |
| `right_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage. |
| `short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws. |
| `left_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the left side of the field. |
| `right_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the right side of the field. |
| `right_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the right side of the field. |
| `left_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the left side of the field, expressed as a percentage. |
| `behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage, per PFF's highest-value, highest-difficulty throw designation. |
| `center_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the center of the field. |
| `left_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `right_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the right side of the field. |
| `left_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `right_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `left_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the left side of the field. |
| `left_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `left_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the left side of the field, per PFF charting. |
| `short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws. |
| `right_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the right side of the field. |
| `right_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the right side of the field. |
| `right_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the right side of the field. |
| `center_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the center of the field. |
| `right_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the right side of the field. |
| `center_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `center_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the center of the field. |
| `medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws. |
| `center_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `center_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the center of the field. |
| `deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws, expressed as a percentage. |
| `center_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `left_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the left side of the field. |
| `center_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the center of the field. |
| `behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage. |
| `center_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the center of the field. |
| `left_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `center_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the center of the field. |
| `left_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the left side of the field, expressed as a percentage. |
| `left_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the left side of the field. |
| `right_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the right side of the field. |
| `center_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `right_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the center of the field. |
| `deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws. |
| `left_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the left side of the field. |
| `right_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `center_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the center of the field. |
| `center_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws. |
| `right_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the right side of the field. |
| `center_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the center of the field. |
| `short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws. |
| `deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws, as charted by PFF. |
| `right_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the right side of the field. |
| `short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws. |
| `center_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the center of the field. |
| `left_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the center of the field. |
| `left_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the center of the field. |
| `center_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the center of the field. |
| `right_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `left_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the left side of the field. |
| `medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `left_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the right side of the field. |
| `center_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the center of the field. |
| `left_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the right side of the field. |
| `medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws. |
| `right_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the right side of the field. |
| `deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws. |
| `behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage. |
| `center_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the center of the field. |
| `center_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the center of the field, per PFF charting. |
| `behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage. |
| `left_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the right side of the field. |
| `right_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the right side of the field. |
| `left_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the left side of the field. |
| `short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws. |
| `center_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the center of the field, per PFF charting. |
| `left_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the left side of the field. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_facet_passing_depth-example}

```python
pff_facet_passing_depth()
```

_Last validated n/a._
