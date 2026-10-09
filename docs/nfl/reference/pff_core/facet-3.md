# NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: passing

> NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: passing — function reference in sdv-py, the SportsDataverse Python package.

## pff_facet_passing_detail_stats

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/detail (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/passing/detail`

**Valid URL:** [https://premium.pff.com/api/v1/facet/passing/detail](https://premium.pff.com/api/v1/facet/passing/detail)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns {#pff_facet_passing_detail_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `left_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the left side of the field. |
| `left_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the left side of the field. |
| `center_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the center of the field. |
| `no_blitz_completion_percent` | numeric | Percentage of pass attempts completed when not blitzed. |
| `right_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the right side of the field. |
| `left_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the left side of the field. |
| `comp_pct_diff` | numeric | Difference in completion percentage between play-action and non-play-action attempts (PA minus non-PA), from the PFF passing-concept split. |
| `pa_grades_pass` | numeric | PFF passing grade (0-100) on play-action dropbacks. |
| `left_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the right side of the field. |
| `right_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage, expressed as a percentage. |
| `grades_offense` | numeric | PFF overall offense grade for the player (0-100). |
| `no_screen_grades_offense` | numeric | PFF overall offense grade for the player (0-100) excluding screen passes. |
| `pa_qb_rating` | numeric | Traditional NFL passer rating on play-action dropbacks. |
| `right_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `no_screen_qb_rating` | numeric | Traditional NFL passer rating excluding screen passes. |
| `pa_completions` | numeric | Number of completed passes on play-action dropbacks. |
| `right_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the right side of the field. |
| `deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws, per PFF charting. |
| `center_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the center of the field. |
| `medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws. |
| `left_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the left side of the field. |
| `no_pressure_scrambles` | numeric | Number of scrambles from a clean pocket (no pressure). |
| `twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts, per PFF charting. |
| `behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage. |
| `medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on medium (10-19 air yards) throws, as charted by PFF. |
| `pa_thrown_aways` | numeric | Number of intentional throwaways on play-action dropbacks. |
| `pa_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on play-action dropbacks, expressed as a percentage. |
| `ypa_diff` | numeric | Difference in yards per attempt between play-action and non-play-action attempts (PA minus non-PA), from the PFF passing-concept split. |
| `blitz_touchdowns` | numeric | Number of passing touchdowns thrown when blitzed. |
| `center_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the center of the field. |
| `pressure_yards` | numeric | Passing yards gained when under pressure. |
| `no_pressure_spikes` | numeric | Number of clock-stopping spikes from a clean pocket (no pressure). |
| `blitz_ypa` | numeric | Yards gained per pass attempt when blitzed. |
| `center_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the center of the field. |
| `no_screen_drops` | numeric | Number of catchable passes dropped by receivers excluding screen passes. |
| `behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage. |
| `no_blitz_grades_run` | numeric | PFF rushing grade for the player (0-100) when not blitzed. |
| `screen_completion_percent` | numeric | Percentage of pass attempts completed on screen passes. |
| `npa_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on non-play-action dropbacks. |
| `left_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the left side of the field. |
| `deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws. |
| `btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts, per PFF charting. |
| `center_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the center of the field, per PFF charting. |
| `center_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the center of the field. |
| `blitz_qb_rating` | numeric | Traditional NFL passer rating when blitzed. |
| `center_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the center of the field. |
| `no_pressure_thrown_aways` | numeric | Number of intentional throwaways from a clean pocket (no pressure). |
| `no_screen_thrown_aways` | numeric | Number of intentional throwaways excluding screen passes. |
| `no_blitz_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when not blitzed. |
| `center_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the center of the field. |
| `left_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the center of the field. |
| `pa_grades_run` | numeric | PFF rushing grade for the player (0-100) on play-action dropbacks. |
| `center_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the center of the field. |
| `right_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the right side of the field. |
| `no_blitz_drops` | numeric | Number of catchable passes dropped by receivers when not blitzed. |
| `center_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `right_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the right side of the field. |
| `right_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws. |
| `no_pressure_completion_percent` | numeric | Percentage of pass attempts completed from a clean pocket (no pressure). |
| `blitz_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) when blitzed, as charted by PFF. |
| `screen_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on screen passes. |
| `pa_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on play-action dropbacks. |
| `center_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the center of the field. |
| `deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws. |
| `right_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the right side of the field. |
| `spikes` | numeric | Clock-stopping spike plays. |
| `left_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the left side of the field. |
| `screen_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on screen passes. |
| `right_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the right side of the field, expressed as a percentage. |
| `right_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the right side of the field, per PFF charting. |
| `center_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the center of the field, expressed as a percentage. |
| `deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws. |
| `center_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the center of the field. |
| `dropbacks` | numeric | Number of dropbacks. |
| `right_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the right side of the field, per PFF charting. |
| `center_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the center of the field. |
| `npa_thrown_aways` | numeric | Number of intentional throwaways on non-play-action dropbacks. |
| `center_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the center of the field. |
| `pressure_grades_run` | numeric | PFF rushing grade for the player (0-100) when under pressure. |
| `left_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the left side of the field. |
| `center_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the center of the field. |
| `deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws. |
| `no_blitz_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when not blitzed. |
| `no_pressure_bats` | numeric | Number of pass attempts batted down at the line of scrimmage from a clean pocket (no pressure). |
| `center_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the center of the field. |
| `pressure_completions` | numeric | Number of completed passes when under pressure. |
| `blitz_big_time_throws` | numeric | Number of big-time throws when blitzed, per PFF's highest-value, highest-difficulty throw designation. |
| `center_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the center of the field. |
| `right_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the right side of the field. |
| `no_screen_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks excluding screen passes, as charted by PFF. |
| `right_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the right side of the field. |
| `screen_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on screen passes. |
| `pa_touchdowns` | numeric | Number of passing touchdowns thrown on play-action dropbacks. |
| `short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws. |
| `center_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the center of the field. |
| `left_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the left side of the field. |
| `npa_ypa` | numeric | Yards gained per pass attempt on non-play-action dropbacks. |
| `no_blitz_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when not blitzed. |
| `blitz_spikes` | numeric | Number of clock-stopping spikes when blitzed. |
| `left_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the left side of the field. |
| `no_pressure_completions` | numeric | Number of completed passes from a clean pocket (no pressure). |
| `left_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the left side of the field. |
| `thrown_aways` | numeric | Number of intentional throwaways. |
| `right_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the right side of the field. |
| `draft_season` | numeric | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `screen_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on screen passes. |
| `right_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the right side of the field. |
| `screen_thrown_aways` | numeric | Number of intentional throwaways on screen passes. |
| `behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage, as charted by PFF. |
| `npa_sacks` | numeric | Number of sacks taken on non-play-action dropbacks. |
| `no_pressure_passing_snaps` | numeric | Number of passing snaps played from a clean pocket (no pressure). |
| `npa_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on non-play-action dropbacks, as charted by PFF. |
| `no_blitz_first_downs` | numeric | Number of passing first downs gained when not blitzed. |
| `right_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the right side of the field. |
| `deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws, as charted by PFF. |
| `right_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the right side of the field, per PFF charting. |
| `center_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the center of the field. |
| `blitz_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, when blitzed. |
| `left_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the left side of the field. |
| `deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws. |
| `no_pressure_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) from a clean pocket (no pressure). |
| `center_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `center_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the center of the field. |
| `pressure_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) when under pressure, as charted by PFF. |
| `no_screen_completions` | numeric | Number of completed passes excluding screen passes. |
| `right_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `no_screen_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added excluding screen passes. |
| `center_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the center of the field. |
| `medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws. |
| `right_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the right side of the field. |
| `blitz_sacks` | numeric | Number of sacks taken when blitzed. |
| `center_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the center of the field. |
| `screen_spikes` | numeric | Number of clock-stopping spikes on screen passes. |
| `pa_first_downs` | numeric | Number of passing first downs gained on play-action dropbacks. |
| `medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws. |
| `no_pressure_interceptions` | numeric | Number of passes intercepted from a clean pocket (no pressure). |
| `right_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage. |
| `center_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the left side of the field, per PFF charting. |
| `right_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the right side of the field, per PFF charting. |
| `pa_big_time_throws` | numeric | Number of big-time throws on play-action dropbacks, per PFF's highest-value, highest-difficulty throw designation. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws. |
| `pa_spikes` | numeric | Number of clock-stopping spikes on play-action dropbacks. |
| `center_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the center of the field. |
| `left_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `pa_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on play-action dropbacks. |
| `deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws. |
| `right_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the right side of the field. |
| `pressure_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when under pressure. |
| `center_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the center of the field. |
| `deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws. |
| `left_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the left side of the field, per PFF charting. |
| `medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws. |
| `blitz_completions` | numeric | Number of completed passes when blitzed. |
| `center_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the center of the field. |
| `blitz_attempts` | numeric | Number of pass attempts when blitzed. |
| `short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws, expressed as a percentage. |
| `right_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the right side of the field. |
| `screen_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on screen passes, expressed as a percentage. |
| `center_medium_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on medium (10-19 air yards) throws to the center of the field. |
| `pressure_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when under pressure, per PFF charting. |
| `pressure_sacks` | numeric | Number of sacks taken when under pressure. |
| `left_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the left side of the field. |
| `no_blitz_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack when not blitzed. |
| `no_pressure_ypa` | numeric | Yards gained per pass attempt from a clean pocket (no pressure). |
| `no_screen_epa` | numeric | Total expected points added (EPA) on the player's dropbacks excluding screen passes. |
| `center_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the center of the field. |
| `center_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the center of the field. |
| `left_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `pressure_passing_snaps` | numeric | Number of passing snaps played when under pressure. |
| `medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `grades_pass` | numeric | PFF passing grade (0-100). |
| `npa_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on non-play-action dropbacks. |
| `deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws. |
| `right_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the right side of the field. |
| `left_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the center of the field. |
| `right_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the right side of the field. |
| `hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw, as charted by PFF. |
| `right_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the right side of the field. |
| `left_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the left side of the field. |
| `center_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the center of the field, per PFF charting. |
| `right_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the right side of the field. |
| `left_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the left side of the field. |
| `screen_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on screen passes. |
| `right_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the right side of the field. |
| `first_downs` | numeric | Passing first downs. |
| `screen_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) on screen passes. |
| `left_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the left side of the field. |
| `pressure_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when under pressure. |
| `left_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the right side of the field. |
| `left_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the center of the field. |
| `blitz_thrown_aways` | numeric | Number of intentional throwaways when blitzed. |
| `right_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws, as charted by PFF. |
| `right_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the right side of the field. |
| `no_pressure_drops` | numeric | Number of catchable passes dropped by receivers from a clean pocket (no pressure). |
| `medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws, as charted by PFF. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `left_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the left side of the field. |
| `left_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the left side of the field. |
| `pressure_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when under pressure. |
| `center_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the center of the field, per PFF charting. |
| `right_medium_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the left side of the field. |
| `no_screen_bats` | numeric | Number of pass attempts batted down at the line of scrimmage excluding screen passes. |
| `left_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the left side of the field. |
| `pressure_scrambles` | numeric | Number of scrambles when under pressure. |
| `blitz_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when blitzed. |
| `deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws. |
| `right_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the right side of the field. |
| `sack_percent` | numeric | Percentage of dropbacks that ended in a sack. |
| `behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage. |
| `right_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the right side of the field. |
| `right_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the right side of the field. |
| `right_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `npa_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on non-play-action dropbacks. |
| `right_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the right side of the field. |
| `short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws. |
| `right_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the right side of the field, per PFF charting. |
| `right_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the right side of the field. |
| `screen_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on screen passes, as charted by PFF. |
| `deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws. |
| `pa_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on play-action dropbacks. |
| `center_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the center of the field. |
| `screen_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on screen passes. |
| `blitz_completion_percent` | numeric | Percentage of pass attempts completed when blitzed. |
| `behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on throws behind the line of scrimmage, as charted by PFF. |
| `right_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the right side of the field. |
| `bats` | numeric | Number of pass attempts batted down at the line of scrimmage. |
| `right_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the right side of the field. |
| `no_screen_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts excluding screen passes, per PFF charting. |
| `medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws. |
| `short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws. |
| `center_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the center of the field, per PFF charting. |
| `left_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the left side of the field. |
| `short_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on short (0-9 air yards) throws, as charted by PFF. |
| `short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws. |
| `right_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the right side of the field. |
| `npa_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on non-play-action dropbacks, per PFF charting. |
| `medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws. |
| `left_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the left side of the field. |
| `screen_interceptions` | numeric | Number of passes intercepted on screen passes. |
| `center_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the center of the field, expressed as a percentage. |
| `left_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the left side of the field. |
| `center_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the center of the field. |
| `left_deep_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on deep (20+ air yards) throws to the left side of the field. |
| `no_screen_passing_snaps` | numeric | Number of passing snaps played excluding screen passes. |
| `no_pressure_first_downs` | numeric | Number of passing first downs gained from a clean pocket (no pressure). |
| `center_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the center of the field. |
| `center_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the center of the field. |
| `blitz_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when blitzed. |
| `right_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the right side of the field, per PFF charting. |
| `right_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the right side of the field. |
| `no_screen_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) excluding screen passes. |
| `medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws, per PFF charting. |
| `no_blitz_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when not blitzed. |
| `center_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the center of the field, per PFF charting. |
| `blitz_interceptions` | numeric | Number of passes intercepted when blitzed. |
| `no_blitz_dropbacks` | numeric | Number of dropbacks when not blitzed. |
| `center_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the center of the field. |
| `left_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the left side of the field. |
| `right_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the right side of the field. |
| `left_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the left side of the field. |
| `no_blitz_grades_pass` | numeric | PFF passing grade (0-100) when not blitzed. |
| `right_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the right side of the field. |
| `screen_scrambles` | numeric | Number of scrambles on screen passes. |
| `left_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws, plays PFF charts as deserving of a turnover. |
| `no_blitz_scrambles` | numeric | Number of scrambles when not blitzed. |
| `pressure_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when under pressure. |
| `left_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the left side of the field. |
| `no_blitz_yards` | numeric | Passing yards gained when not blitzed. |
| `left_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `screen_grades_pass` | numeric | PFF passing grade (0-100) on screen passes. |
| `center_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the center of the field. |
| `center_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the center of the field. |
| `sacks` | numeric | Times the passer was sacked. |
| `pressure_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack, reported within the pressure split. |
| `center_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the center of the field. |
| `right_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the center of the field. |
| `no_blitz_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when not blitzed. |
| `right_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the right side of the field. |
| `left_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the left side of the field. |
| `center_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the center of the field. |
| `npa_qb_rating` | numeric | Traditional NFL passer rating on non-play-action dropbacks. |
| `no_pressure_grades_offense` | numeric | PFF overall offense grade for the player (0-100) from a clean pocket (no pressure). |
| `medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws. |
| `no_screen_grades_pass` | numeric | PFF passing grade (0-100) excluding screen passes. |
| `medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws. |
| `left_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the left side of the field. |
| `deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws, per PFF charting. |
| `pa_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on play-action dropbacks. |
| `left_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the left side of the field. |
| `no_pressure_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, from a clean pocket (no pressure). |
| `screen_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on screen passes, per PFF charting. |
| `pressure_dropbacks` | numeric | Number of dropbacks when under pressure. |
| `short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws, per PFF charting. |
| `short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws. |
| `left_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the left side of the field. |
| `short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws. |
| `center_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `no_blitz_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when not blitzed. |
| `behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage. |
| `right_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `no_pressure_grades_run` | numeric | PFF rushing grade for the player (0-100) from a clean pocket (no pressure). |
| `short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws. |
| `player_game_count` | numeric | Number of games the player appeared in during the period covered. |
| `short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws, per PFF charting. |
| `right_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the right side of the field. |
| `center_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the center of the field. |
| `right_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the right side of the field. |
| `behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage. |
| `right_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `blitz_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when blitzed, plays PFF charts as deserving of a turnover. |
| `deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws. |
| `npa_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on non-play-action dropbacks. |
| `no_blitz_touchdowns` | numeric | Number of passing touchdowns thrown when not blitzed. |
| `no_blitz_avg_depth_of_target` | numeric | Average depth of target in air yards when not blitzed. |
| `medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws. |
| `short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws. |
| `right_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the right side of the field, expressed as a percentage. |
| `no_pressure_dropbacks_percent` | numeric | Share of the player's total dropbacks that came from a clean pocket (no pressure), expressed as a percentage. |
| `blitz_grades_pass` | numeric | PFF passing grade (0-100) when blitzed. |
| `eligible_season` | numeric | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage. |
| `blitz_avg_depth_of_target` | numeric | Average depth of target in air yards when blitzed. |
| `no_blitz_spikes` | numeric | Number of clock-stopping spikes when not blitzed. |
| `center_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the center of the field. |
| `npa_drops` | numeric | Number of catchable passes dropped by receivers on non-play-action dropbacks. |
| `center_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the center of the field. |
| `right_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the right side of the field. |
| `center_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the left side of the field. |
| `no_pressure_dropbacks` | numeric | Number of dropbacks from a clean pocket (no pressure). |
| `center_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the center of the field. |
| `right_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws. |
| `right_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the right side of the field. |
| `completions` | numeric | Completed passes by the passer. |
| `medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws. |
| `left_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the left side of the field. |
| `screen_yards` | numeric | Passing yards gained on screen passes. |
| `right_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the right side of the field. |
| `center_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `left_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the left side of the field. |
| `blitz_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when blitzed, per PFF charting. |
| `no_screen_drop_rate` | numeric | Percentage of catchable passes dropped by receivers excluding screen passes. |
| `center_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the center of the field. |
| `right_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the right side of the field. |
| `blitz_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when blitzed. |
| `no_pressure_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts from a clean pocket (no pressure), per PFF charting. |
| `left_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the left side of the field. |
| `deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws. |
| `right_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the right side of the field. |
| `center_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the center of the field, expressed as a percentage. |
| `no_screen_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) excluding screen passes. |
| `right_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the right side of the field. |
| `left_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the left side of the field. |
| `deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws. |
| `left_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the left side of the field. |
| `npa_passing_snaps` | numeric | Number of passing snaps played on non-play-action dropbacks. |
| `screen_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on screen passes. |
| `screen_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on screen passes. |
| `no_pressure_drop_rate` | numeric | Percentage of catchable passes dropped by receivers from a clean pocket (no pressure). |
| `no_blitz_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when not blitzed, plays PFF charts as deserving of a turnover. |
| `yards` | numeric | Total passing yards gained. |
| `right_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage. |
| `screen_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on screen passes. |
| `right_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the right side of the field. |
| `npa_spikes` | numeric | Number of clock-stopping spikes on non-play-action dropbacks. |
| `pressure_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when under pressure. |
| `screen_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on screen passes, as charted by PFF. |
| `left_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `left_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the left side of the field. |
| `deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws. |
| `left_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the left side of the field. |
| `medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws. |
| `no_screen_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) excluding screen passes, as charted by PFF. |
| `screen_drops` | numeric | Number of catchable passes dropped by receivers on screen passes. |
| `left_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the left side of the field. |
| `accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF. |
| `right_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the right side of the field. |
| `screen_ypa` | numeric | Yards gained per pass attempt on screen passes. |
| `medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws. |
| `blitz_first_downs` | numeric | Number of passing first downs gained when blitzed. |
| `npa_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on non-play-action dropbacks, per PFF charting. |
| `behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage. |
| `left_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the left side of the field. |
| `center_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the center of the field. |
| `center_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the center of the field. |
| `right_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the right side of the field. |
| `right_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the right side of the field. |
| `right_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the right side of the field. |
| `no_blitz_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when not blitzed, expressed as a percentage. |
| `right_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the right side of the field. |
| `screen_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on screen passes. |
| `right_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage. |
| `center_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the center of the field. |
| `left_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the left side of the field. |
| `npa_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on non-play-action dropbacks, as charted by PFF. |
| `center_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the center of the field. |
| `scrambles` | numeric | Number of scrambles. |
| `right_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `left_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the left side of the field. |
| `right_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the left side of the field. |
| `pa_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on play-action dropbacks. |
| `center_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the center of the field, per PFF charting. |
| `pressure_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when under pressure, plays PFF charts as deserving of a turnover. |
| `medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws. |
| `no_pressure_epa` | numeric | Total expected points added (EPA) on the player's dropbacks from a clean pocket (no pressure). |
| `left_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the left side of the field. |
| `short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws. |
| `right_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the right side of the field. |
| `no_blitz_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, when not blitzed. |
| `left_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the left side of the field. |
| `pa_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on play-action dropbacks, as charted by PFF. |
| `right_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the right side of the field. |
| `interceptions` | numeric | Interceptions thrown. |
| `screen_avg_depth_of_target` | numeric | Average depth of target in air yards on screen passes. |
| `pa_sacks` | numeric | Number of sacks taken on play-action dropbacks. |
| `short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws, plays PFF charts as deserving of a turnover. |
| `screen_passing_snaps` | numeric | Number of passing snaps played on screen passes. |
| `right_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the right side of the field. |
| `drop_rate` | numeric | Percentage of catchable passes dropped by receivers. |
| `right_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the right side of the field. |
| `no_screen_grades_run` | numeric | PFF rushing grade for the player (0-100) excluding screen passes. |
| `right_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the right side of the field. |
| `medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws. |
| `center_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the center of the field, expressed as a percentage. |
| `left_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the left side of the field. |
| `behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage. |
| `short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws. |
| `no_screen_first_downs` | numeric | Number of passing first downs gained excluding screen passes. |
| `no_blitz_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when not blitzed. |
| `left_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the left side of the field. |
| `left_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the left side of the field. |
| `pressure_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when under pressure, per PFF charting. |
| `pa_ypa` | numeric | Yards gained per pass attempt on play-action dropbacks. |
| `behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage. |
| `npa_scrambles` | numeric | Number of scrambles on non-play-action dropbacks. |
| `no_pressure_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) from a clean pocket (no pressure), as charted by PFF. |
| `pressure_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when under pressure, expressed as a percentage. |
| `grades_run` | numeric | PFF rushing grade for the player (0-100). |
| `behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage. |
| `left_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the left side of the field. |
| `left_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `npa_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on non-play-action dropbacks. |
| `center_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the center of the field. |
| `pa_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on play-action dropbacks, per PFF charting. |
| `no_pressure_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks from a clean pocket (no pressure), as charted by PFF. |
| `left_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the left side of the field. |
| `pressure_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when under pressure. |
| `left_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the left side of the field. |
| `center_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the center of the field. |
| `center_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the center of the field. |
| `short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws. |
| `pressure_completion_percent` | numeric | Percentage of pass attempts completed when under pressure. |
| `left_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the left side of the field. |
| `deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws. |
| `center_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the center of the field, plays PFF charts as deserving of a turnover. |
| `medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws. |
| `left_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the left side of the field. |
| `left_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the left side of the field. |
| `deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws. |
| `pressure_avg_depth_of_target` | numeric | Average depth of target in air yards when under pressure. |
| `short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws. |
| `left_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the left side of the field. |
| `blitz_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when blitzed. |
| `center_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the center of the field. |
| `center_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `left_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the center of the field. |
| `right_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the right side of the field. |
| `right_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws. |
| `no_screen_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts excluding screen passes, per PFF charting. |
| `left_behind_los_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on throws behind the line of scrimmage to the left side of the field. |
| `right_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the right side of the field. |
| `pressure_drops` | numeric | Number of catchable passes dropped by receivers when under pressure. |
| `right_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the right side of the field, per PFF charting. |
| `center_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the center of the field. |
| `right_deep_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on deep (20+ air yards) throws to the right side of the field. |
| `no_blitz_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when not blitzed, as charted by PFF. |
| `center_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `pressure_attempts` | numeric | Number of pass attempts when under pressure. |
| `npa_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on non-play-action dropbacks. |
| `behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage. |
| `right_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the right side of the field, per PFF charting. |
| `right_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `right_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the right side of the field. |
| `deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `left_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `qb_rating` | numeric | Traditional NFL passer rating. |
| `center_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the center of the field. |
| `right_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the right side of the field. |
| `left_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the left side of the field. |
| `pressure_big_time_throws` | numeric | Number of big-time throws when under pressure, per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_thrown_aways` | numeric | Number of intentional throwaways when not blitzed. |
| `short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws. |
| `medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws. |
| `left_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the left side of the field. |
| `right_behind_los_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on throws behind the line of scrimmage to the right side of the field. |
| `completion_percent` | numeric | Percentage of pass attempts completed. |
| `blitz_drops` | numeric | Number of catchable passes dropped by receivers when blitzed. |
| `behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage, per PFF charting. |
| `screen_completions` | numeric | Number of completed passes on screen passes. |
| `screen_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on screen passes, per PFF charting. |
| `npa_grades_run` | numeric | PFF rushing grade for the player (0-100) on non-play-action dropbacks. |
| `no_pressure_touchdowns` | numeric | Number of passing touchdowns thrown from a clean pocket (no pressure). |
| `no_screen_interceptions` | numeric | Number of passes intercepted excluding screen passes. |
| `medium_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on medium (10-19 air yards) throws. |
| `center_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the center of the field. |
| `behind_los_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on throws behind the line of scrimmage. |
| `right_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the right side of the field. |
| `no_pressure_sacks` | numeric | Number of sacks taken from a clean pocket (no pressure). |
| `center_short_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on short (0-9 air yards) throws to the center of the field. |
| `npa_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `left_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `left_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the left side of the field. |
| `center_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the center of the field. |
| `blitz_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when blitzed. |
| `no_screen_sacks` | numeric | Number of sacks taken excluding screen passes. |
| `center_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the center of the field. |
| `left_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the left side of the field, per PFF charting. |
| `pressure_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when under pressure. |
| `no_pressure_attempts` | numeric | Number of pass attempts from a clean pocket (no pressure). |
| `right_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the right side of the field, expressed as a percentage. |
| `no_blitz_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when not blitzed. |
| `attempts` | numeric | Pass attempts thrown by the passer. |
| `blitz_scrambles` | numeric | Number of scrambles when blitzed. |
| `right_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage. |
| `center_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the center of the field. |
| `short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws. |
| `no_screen_big_time_throws` | numeric | Number of big-time throws excluding screen passes, per PFF's highest-value, highest-difficulty throw designation. |
| `left_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on throws behind the line of scrimmage to the center of the field. |
| `blitz_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when blitzed, expressed as a percentage. |
| `behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage. |
| `left_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the left side of the field. |
| `medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws, per PFF charting. |
| `left_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `npa_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on non-play-action dropbacks. |
| `medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws, plays PFF charts as deserving of a turnover. |
| `behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage, per PFF charting. |
| `left_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the left side of the field. |
| `left_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the left side of the field. |
| `center_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the center of the field. |
| `npa_attempts` | numeric | Number of pass attempts on non-play-action dropbacks. |
| `right_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the right side of the field. |
| `screen_qb_rating` | numeric | Traditional NFL passer rating on screen passes. |
| `medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws. |
| `center_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the center of the field. |
| `center_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the center of the field. |
| `npa_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on non-play-action dropbacks. |
| `no_pressure_qb_rating` | numeric | Traditional NFL passer rating from a clean pocket (no pressure). |
| `center_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the center of the field. |
| `pa_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on play-action dropbacks. |
| `pa_attempts` | numeric | Number of pass attempts on play-action dropbacks. |
| `behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage. |
| `no_blitz_passing_snaps` | numeric | Number of passing snaps played when not blitzed. |
| `npa_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on non-play-action dropbacks, as charted by PFF. |
| `left_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws. |
| `no_blitz_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) when not blitzed, as charted by PFF. |
| `blitz_yards` | numeric | Passing yards gained when blitzed. |
| `no_screen_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, excluding screen passes. |
| `center_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the center of the field. |
| `behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage. |
| `pa_yards` | numeric | Passing yards gained on play-action dropbacks. |
| `right_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the right side of the field. |
| `left_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the left side of the field. |
| `right_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the right side of the field. |
| `npa_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on non-play-action dropbacks. |
| `right_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the right side of the field, plays PFF charts as deserving of a turnover. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `no_screen_scrambles` | numeric | Number of scrambles excluding screen passes. |
| `pa_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on play-action dropbacks, plays PFF charts as deserving of a turnover. |
| `right_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the right side of the field. |
| `behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage, as charted by PFF. |
| `no_blitz_attempts` | numeric | Number of pass attempts when not blitzed. |
| `pressure_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when under pressure. |
| `left_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the left side of the field. |
| `medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws. |
| `deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws, as charted by PFF. |
| `no_blitz_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when not blitzed, as charted by PFF. |
| `right_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the right side of the field. |
| `pressure_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when under pressure, as charted by PFF. |
| `short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws. |
| `deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws. |
| `blitz_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack when blitzed. |
| `pa_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on play-action dropbacks, as charted by PFF. |
| `deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws. |
| `pa_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on play-action dropbacks. |
| `screen_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on screen passes. |
| `left_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the left side of the field. |
| `no_screen_dropbacks` | numeric | Number of dropbacks excluding screen passes. |
| `right_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the right side of the field. |
| `no_screen_dropbacks_percent` | numeric | Share of the player's total dropbacks that came excluding screen passes, expressed as a percentage. |
| `no_blitz_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when not blitzed, per PFF charting. |
| `medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws, expressed as a percentage. |
| `right_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the right side of the field. |
| `left_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `left_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the left side of the field. |
| `right_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `no_blitz_ypa` | numeric | Yards gained per pass attempt when not blitzed. |
| `right_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the right side of the field, expressed as a percentage. |
| `center_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `npa_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on non-play-action dropbacks. |
| `short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws, as charted by PFF. |
| `blitz_grades_run` | numeric | PFF rushing grade for the player (0-100) when blitzed. |
| `right_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `left_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the left side of the field. |
| `center_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the center of the field. |
| `pa_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on play-action dropbacks. |
| `pressure_interceptions` | numeric | Number of passes intercepted when under pressure. |
| `right_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the right side of the field. |
| `deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws. |
| `left_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the left side of the field, per PFF charting. |
| `blitz_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when blitzed. |
| `passing_snaps` | numeric | Number of passing snaps played. |
| `pa_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) on play-action dropbacks. |
| `pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack. |
| `ypa` | numeric | Yards gained per pass attempt. |
| `right_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the center of the field. |
| `drops` | numeric | Passes dropped by the passer's receivers. |
| `center_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the center of the field. |
| `short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws. |
| `left_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the left side of the field, per PFF charting. |
| `no_screen_turnover_worthy_plays` | numeric | Number of turnover-worthy plays excluding screen passes, plays PFF charts as deserving of a turnover. |
| `no_pressure_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) from a clean pocket (no pressure). |
| `right_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the right side of the field. |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws, as charted by PFF. |
| `center_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the center of the field. |
| `center_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the center of the field. |
| `blitz_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when blitzed. |
| `right_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the right side of the field. |
| `grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100). |
| `left_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the left side of the field. |
| `npa_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on non-play-action dropbacks. |
| `no_screen_avg_depth_of_target` | numeric | Average depth of target in air yards excluding screen passes. |
| `medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws. |
| `left_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the left side of the field. |
| `pa_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on play-action dropbacks. |
| `no_screen_completion_percent` | numeric | Percentage of pass attempts completed excluding screen passes. |
| `left_short_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on short (0-9 air yards) throws to the left side of the field. |
| `pa_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on play-action dropbacks, as charted by PFF. |
| `medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws. |
| `center_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the left side of the field. |
| `avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release. |
| `pa_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on play-action dropbacks. |
| `deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws. |
| `npa_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on non-play-action dropbacks, expressed as a percentage. |
| `left_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the left side of the field. |
| `pressure_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when under pressure, as charted by PFF. |
| `left_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the left side of the field. |
| `npa_grades_pass` | numeric | PFF passing grade (0-100) on non-play-action dropbacks. |
| `pressure_grades_pass` | numeric | PFF passing grade (0-100) when under pressure. |
| `right_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the right side of the field. |
| `big_time_throws` | numeric | Number of big-time throws, per PFF's highest-value, highest-difficulty throw designation. |
| `screen_grades_run` | numeric | PFF rushing grade for the player (0-100) on screen passes. |
| `left_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the left side of the field. |
| `screen_first_downs` | numeric | Number of passing first downs gained on screen passes. |
| `npa_completion_percent` | numeric | Percentage of pass attempts completed on non-play-action dropbacks. |
| `left_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the left side of the field. |
| `medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws. |
| `no_blitz_qb_rating` | numeric | Traditional NFL passer rating when not blitzed. |
| `blitz_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when blitzed, as charted by PFF. |
| `behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage, plays PFF charts as deserving of a turnover. |
| `right_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the right side of the field. |
| `right_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the right side of the field. |
| `deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws. |
| `center_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the center of the field. |
| `left_medium_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on medium (10-19 air yards) throws to the left side of the field. |
| `left_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the left side of the field. |
| `no_screen_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) excluding screen passes. |
| `pressure_ypa` | numeric | Yards gained per pass attempt when under pressure. |
| `left_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `no_screen_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw excluding screen passes, as charted by PFF. |
| `center_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the center of the field. |
| `center_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the center of the field. |
| `right_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the right side of the field. |
| `center_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the center of the field. |
| `screen_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on screen passes, plays PFF charts as deserving of a turnover. |
| `deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws. |
| `left_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the left side of the field, expressed as a percentage. |
| `pressure_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, when under pressure. |
| `short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws. |
| `behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage. |
| `right_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the right side of the field. |
| `center_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the center of the field. |
| `center_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `no_blitz_completions` | numeric | Number of completed passes when not blitzed. |
| `center_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `left_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the left side of the field. |
| `behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage. |
| `left_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the left side of the field, expressed as a percentage. |
| `no_pressure_sack_percent` | numeric | Percentage of dropbacks that ended in a sack from a clean pocket (no pressure). |
| `center_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `npa_avg_depth_of_target` | numeric | Average depth of target in air yards on non-play-action dropbacks. |
| `npa_dropbacks` | numeric | Number of dropbacks on non-play-action dropbacks. |
| `player` | character | Player's display name as PFF lists it. |
| `left_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the left side of the field. |
| `no_blitz_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when not blitzed. |
| `behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage. |
| `center_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `short_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on short (0-9 air yards) throws. |
| `left_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the left side of the field, per PFF charting. |
| `center_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `pa_drops` | numeric | Number of catchable passes dropped by receivers on play-action dropbacks. |
| `no_pressure_pressure_to_sack_rate` | character | Pressure-to-sack rate as reported within the no-pressure split of the PFF passing-pressure facet. |
| `right_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the right side of the field. |
| `pa_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on play-action dropbacks, per PFF charting. |
| `no_pressure_turnover_worthy_plays` | numeric | Number of turnover-worthy plays from a clean pocket (no pressure), plays PFF charts as deserving of a turnover. |
| `pressure_first_downs` | numeric | Number of passing first downs gained when under pressure. |
| `positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added. |
| `right_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the right side of the field. |
| `blitz_passing_snaps` | numeric | Number of passing snaps played when blitzed. |
| `center_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `left_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the left side of the field. |
| `screen_big_time_throws` | numeric | Number of big-time throws on screen passes, per PFF's highest-value, highest-difficulty throw designation. |
| `left_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the left side of the field. |
| `left_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the left side of the field, per PFF charting. |
| `screen_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on screen passes, as charted by PFF. |
| `right_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage. |
| `no_pressure_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts from a clean pocket (no pressure), per PFF charting. |
| `npa_touchdowns` | numeric | Number of passing touchdowns thrown on non-play-action dropbacks. |
| `no_blitz_sacks` | numeric | Number of sacks taken when not blitzed. |
| `short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws. |
| `left_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the left side of the field. |
| `right_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the right side of the field. |
| `screen_attempts` | numeric | Number of pass attempts on screen passes. |
| `screen_dropbacks` | numeric | Number of dropbacks on screen passes. |
| `right_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the right side of the field. |
| `left_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the left side of the field, expressed as a percentage. |
| `behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage, per PFF's highest-value, highest-difficulty throw designation. |
| `center_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the center of the field. |
| `left_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `right_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the right side of the field. |
| `left_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `no_screen_yards` | numeric | Passing yards gained excluding screen passes. |
| `right_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `left_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the left side of the field. |
| `left_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `left_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the left side of the field, per PFF charting. |
| `short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws. |
| `right_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the right side of the field. |
| `right_short_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on short (0-9 air yards) throws to the right side of the field. |
| `pa_scrambles` | numeric | Number of scrambles on play-action dropbacks. |
| `right_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the right side of the field. |
| `no_pressure_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF from a clean pocket (no pressure). |
| `center_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the center of the field. |
| `right_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the right side of the field. |
| `center_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `center_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the center of the field. |
| `medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws. |
| `center_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `center_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the center of the field. |
| `pa_completion_percent` | numeric | Percentage of pass attempts completed on play-action dropbacks. |
| `deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws, expressed as a percentage. |
| `center_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `left_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the left side of the field. |
| `center_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on deep (20+ air yards) throws to the center of the field. |
| `pa_avg_depth_of_target` | numeric | Average depth of target in air yards on play-action dropbacks. |
| `pa_interceptions` | numeric | Number of passes intercepted on play-action dropbacks. |
| `no_screen_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF excluding screen passes. |
| `behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage. |
| `no_screen_spikes` | numeric | Number of clock-stopping spikes excluding screen passes. |
| `center_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the center of the field. |
| `pa_dropbacks` | numeric | Number of dropbacks on play-action dropbacks. |
| `left_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `center_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the center of the field. |
| `left_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the left side of the field, expressed as a percentage. |
| `left_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the left side of the field. |
| `no_blitz_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when not blitzed, per PFF charting. |
| `no_pressure_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw from a clean pocket (no pressure), as charted by PFF. |
| `right_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the right side of the field. |
| `center_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `right_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the center of the field. |
| `no_pressure_grades_pass` | numeric | PFF passing grade (0-100) from a clean pocket (no pressure). |
| `npa_big_time_throws` | numeric | Number of big-time throws on non-play-action dropbacks, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on deep (20+ air yards) throws. |
| `no_screen_sack_percent` | numeric | Percentage of dropbacks that ended in a sack excluding screen passes. |
| `avg_depth_of_target` | numeric | Average depth of target in air yards. |
| `no_pressure_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added from a clean pocket (no pressure). |
| `turnover_worthy_plays` | numeric | Number of turnover-worthy plays, plays PFF charts as deserving of a turnover. |
| `epa` | numeric | Expected points added per play on the passer's dropbacks, as computed by PFF (an average such as 0.14, not a total). |
| `pressure_spikes` | numeric | Number of clock-stopping spikes when under pressure. |
| `pressure_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when under pressure. |
| `left_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the left side of the field. |
| `right_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `pa_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on play-action dropbacks. |
| `pressure_qb_rating` | numeric | Traditional NFL passer rating when under pressure. |
| `center_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the center of the field. |
| `center_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `no_screen_attempts` | numeric | Number of pass attempts excluding screen passes. |
| `pa_passing_snaps` | numeric | Number of passing snaps played on play-action dropbacks. |
| `aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit), as charted by PFF. |
| `blitz_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when blitzed. |
| `pressure_touchdowns` | numeric | Number of passing touchdowns thrown when under pressure. |
| `npa_completions` | numeric | Number of completed passes on non-play-action dropbacks. |
| `short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws. |
| `pressure_thrown_aways` | numeric | Number of intentional throwaways when under pressure. |
| `right_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the right side of the field. |
| `no_blitz_interceptions` | numeric | Number of passes intercepted when not blitzed. |
| `center_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the center of the field. |
| `short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws. |
| `deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on deep (20+ air yards) throws, as charted by PFF. |
| `right_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the right side of the field. |
| `short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws. |
| `screen_touchdowns` | numeric | Number of passing touchdowns thrown on screen passes. |
| `center_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the center of the field. |
| `left_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the center of the field. |
| `left_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `blitz_dropbacks` | numeric | Number of dropbacks when blitzed. |
| `center_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the center of the field. |
| `center_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the center of the field. |
| `blitz_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when blitzed, as charted by PFF. |
| `npa_first_downs` | numeric | Number of passing first downs gained on non-play-action dropbacks. |
| `no_screen_touchdowns` | numeric | Number of passing touchdowns thrown excluding screen passes. |
| `right_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `left_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the left side of the field. |
| `medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `npa_yards` | numeric | Passing yards gained on non-play-action dropbacks. |
| `left_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the right side of the field. |
| `no_screen_ypa` | numeric | Yards gained per pass attempt excluding screen passes. |
| `no_blitz_big_time_throws` | numeric | Number of big-time throws when not blitzed, per PFF's highest-value, highest-difficulty throw designation. |
| `center_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the center of the field. |
| `npa_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on non-play-action dropbacks, plays PFF charts as deserving of a turnover. |
| `left_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the center of the field. |
| `npa_interceptions` | numeric | Number of passes intercepted on non-play-action dropbacks. |
| `left_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the right side of the field. |
| `no_pressure_avg_depth_of_target` | numeric | Average depth of target in air yards from a clean pocket (no pressure). |
| `touchdowns` | numeric | Number of passing touchdowns thrown. |
| `medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws. |
| `right_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the right side of the field. |
| `deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws. |
| `no_screen_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack excluding screen passes. |
| `behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage. |
| `center_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the center of the field. |
| `center_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the center of the field, per PFF charting. |
| `behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage. |
| `left_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the left side of the field. |
| `no_pressure_yards` | numeric | Passing yards gained from a clean pocket (no pressure). |
| `screen_sacks` | numeric | Number of sacks taken on screen passes. |
| `def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks, as charted by PFF. |
| `right_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the right side of the field. |
| `right_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the right side of the field. |
| `left_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the left side of the field. |
| `blitz_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when blitzed, per PFF charting. |
| `short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws. |
| `blitz_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when blitzed. |
| `center_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the center of the field, per PFF charting. |
| `no_pressure_big_time_throws` | numeric | Number of big-time throws from a clean pocket (no pressure), per PFF's highest-value, highest-difficulty throw designation. |
| `left_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the left side of the field. |
| `no_blitz_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when not blitzed. |
| `screen_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) on screen passes. |
| `screen_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) on screen passes. |
| `no_screen_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) excluding screen passes. |
| `pressure_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when under pressure. |
| `no_screen_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) excluding screen passes. |
| `pa_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) on play-action dropbacks. |
| `blitz_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when blitzed. |
| `screen_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) on screen passes. |
| `no_pressure_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `npa_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `pressure_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when under pressure. |
| `pa_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) on play-action dropbacks. |
| `no_pressure_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) from a clean pocket (no pressure). |
| `npa_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) on non-play-action dropbacks. |
| `npa_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) on play-action dropbacks. |
| `blitz_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when blitzed. |
| `blitz_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when blitzed. |
| `no_screen_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) excluding screen passes. |
| `no_pressure_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) from a clean pocket (no pressure). |
| `no_blitz_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when not blitzed. |
| `pressure_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when under pressure. |
| `no_blitz_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when not blitzed. |
| `pa_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) on play-action dropbacks. |
| `blitz_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when blitzed. |
| `screen_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) on screen passes. |
| `no_pressure_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `no_screen_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) excluding screen passes. |
| `no_blitz_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when not blitzed. |
| `blitz_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when blitzed. |
| `pressure_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when under pressure. |
| `no_blitz_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when not blitzed. |
| `npa_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `pressure_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when under pressure. |
| `no_pressure_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `pa_grades_defense_penalty` | character | PFF defensive penalty grade for the player (0-100) on play-action dropbacks. |
| `screen_grades_run_defense` | character | PFF run-defense grade for the player (0-100) on screen passes. |
| `npa_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_run_defense` | character | PFF run-defense grade for the player (0-100) on play-action dropbacks. |
| `pa_grades_defense` | character | PFF overall defense grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_defense` | numeric | PFF overall defense grade for the player (0-100) on non-play-action dropbacks. |
| `screen_grades_defense` | character | PFF overall defense grade for the player (0-100) on screen passes. |
| `screen_grades_defense_penalty` | character | PFF defensive penalty grade for the player (0-100) on screen passes. |
| `no_screen_grades_run_defense` | numeric | PFF run-defense grade for the player (0-100) excluding screen passes. |
| `no_screen_grades_defense` | numeric | PFF overall defense grade for the player (0-100) excluding screen passes. |
| `no_screen_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) excluding screen passes. |
| `npa_grades_run_defense` | numeric | PFF run-defense grade for the player (0-100) on non-play-action dropbacks. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_facet_passing_detail_stats-example}

```python
pff_facet_passing_detail_stats()
```

_Last validated n/a._
