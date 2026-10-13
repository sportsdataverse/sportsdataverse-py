# NFL — NFL.com API — Live

> NFL — NFL.com API — Live — function reference in sdv-py, the SportsDataverse Python package.

## nfl_live_team_statistics

GET /football/v2/stats/live/team-statistics/{game_id} — one row per side (away, home): the live team box score.

**Endpoint URL:** `GET https://api.nfl.com/football/v2/stats/live/team-statistics/{game_id}`

**Valid URL:** [https://api.nfl.com/football/v2/stats/live/team-statistics/a9a890ed-4feb-11f1-abca-2c54536568a9](https://api.nfl.com/football/v2/stats/live/team-statistics/a9a890ed-4feb-11f1-abca-2c54536568a9)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Shield uuid game id -- the ``id`` column of the week games and weekly game details listings. |

### Returns {#nfl_live_team_statistics-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | character | NFL.com Shield GUID for the game. |
| `offset` | integer | Live-feed sequence position the statistics reflect; advances as the game is played. |
| `side` | character | Which side of the game the row belongs to: away or home. |
| `team_id` | character | NFL.com Shield GUID of the team. |
| `defensive_fumbles_forced` | integer | Defense: fumbles forced. |
| `defensive_fumbles_recovered` | integer | Defense: fumbles recovered. |
| `defensive_interceptions` | integer | Defense: interceptions. |
| `defensive_passes_defended` | integer | Defense: passes defended. |
| `defensive_quarterback_hits` | integer | Defense: quarterback hits. |
| `defensive_sacks` | integer | Defense: sacks. |
| `defensive_safeties` | integer | Defense: safeties. |
| `defensive_tackles_combined` | numeric | Defense: tackles combined. |
| `defensive_tackles_for_loss` | numeric | Defense: tackles for loss. |
| `defensive_touchdowns` | integer | Defense: touchdowns. |
| `extra_point_kick_attempts` | integer | Extra points: kick attempts. |
| `extra_point_kick_blocked` | integer | Extra points: kick blocked. |
| `extra_point_kick_made` | integer | Extra points: kick made. |
| `field_goals_attempts` | integer | Field goals: attempts. |
| `field_goals_blocked` | integer | Field goals: blocked. |
| `field_goals_longest_made` | integer | Field goals: longest made. |
| `field_goals_made` | integer | Field goals: made. |
| `first_downs_passing` | integer | First downs: passing. |
| `first_downs_penalty` | integer | First downs: penalty. |
| `first_downs_rushing` | integer | First downs: rushing. |
| `first_downs_total` | integer | First downs: total. |
| `fourth_down_attempts` | integer | Fourth down: attempts. |
| `fourth_down_conversions` | integer | Fourth down: conversions. |
| `fumbles_lost` | integer | Fumbles: lost. |
| `fumbles_made` | integer | Fumbles: made. |
| `fumbles_own_recoveries` | integer | Fumbles: own recoveries. |
| `fumbles_returned_touchdowns` | integer | Fumbles: returned touchdowns. |
| `goal_to_go_attempts` | integer | Goal-to-go: attempts. |
| `goal_to_go_successes` | integer | Goal-to-go: successes. |
| `interceptions_longest_touchdown` | integer | Interceptions: longest touchdown. |
| `interceptions_made` | integer | Interceptions made by the defense. |
| `interceptions_returned` | integer | Interceptions: returned. |
| `interceptions_returned_touchdowns` | integer | Interceptions: returned touchdowns. |
| `interceptions_returned_yards` | integer | Interceptions: returned yards. |
| `kick_returns_longest` | integer | Kickoff returns: longest. |
| `kick_returns_yards_average` | numeric | Kickoff returns: yards average. |
| `kickoffs_in_end_zone` | integer | Kickoffs: in end zone. |
| `kickoffs_made` | integer | Kickoffs: made. |
| `kickoffs_returned` | integer | Kickoffs: returned. |
| `kickoffs_returned_touchdowns` | integer | Kickoffs: returned touchdowns. |
| `kickoffs_returned_yards` | integer | Kickoffs: returned yards. |
| `kickoffs_touchbacks` | integer | Kickoffs: touchbacks. |
| `passing_attempts` | integer | Passing: attempts. |
| `passing_completions` | integer | Passing: completions. |
| `passing_completion_percent` | numeric | Passing: completion percent. |
| `passing_interceptions` | integer | Passing: interceptions. |
| `passing_rating` | numeric | Passing: rating. |
| `passing_sacks` | integer | Passing: sacks. |
| `passing_sack_yards_lost` | numeric | Passing: sack yards lost. |
| `passing_touchdowns` | integer | Passing: touchdowns. |
| `passing_yards` | integer | Passing: yards. |
| `passing_yards_average` | numeric | Passing: yards average. |
| `passing_yards_per_attempt` | numeric | Passing: yards per attempt. |
| `penalties_made` | integer | Penalties: made. |
| `penalties_yards` | integer | Penalties: yards. |
| `punt_returns_longest` | integer | Punt returns: longest. |
| `punt_returns_yards_average` | numeric | Punt returns: yards average. |
| `punts_attempts` | integer | Punting: attempts. |
| `punts_blocked` | integer | Punting: blocked. |
| `punts_inside20` | integer | Punting: inside the 20. |
| `punts_longest` | integer | Punting: longest. |
| `punts_returned` | integer | Punting: returned. |
| `punts_returned_touchdowns` | integer | Punting: returned touchdowns. |
| `punts_returned_yards` | integer | Punting: returned yards. |
| `punts_touchbacks` | integer | Punting: touchbacks. |
| `punts_yards` | integer | Punting: yards. |
| `punts_yards_average_gross` | numeric | Gross punting average (yards per punt). |
| `punts_yards_average_net` | numeric | Net punting average (yards per punt, after returns and touchbacks). |
| `receptions` | integer | Receptions. |
| `receptions_long` | integer | Receiving: longest. |
| `receptions_pass_target` | integer | Pass targets. |
| `receptions_touchdowns` | integer | Receiving: touchdowns. |
| `receptions_yards` | integer | Receiving: yards. |
| `receptions_yards_after_catch` | integer | Receiving yards after the catch. |
| `red_zone_attempts` | integer | Red zone: attempts. |
| `red_zone_successes` | integer | Red zone: successes. |
| `rushing_long` | integer | Rushing: longest. |
| `rushing_plays` | integer | Rushing: plays. |
| `rushing_tackles_for_loss` | integer | Rushing: tackles for loss. |
| `rushing_tackles_for_loss_yards` | integer | Rushing: tackles for loss yards. |
| `rushing_touchdowns` | integer | Rushing: touchdowns. |
| `rushing_yards` | integer | Rushing: yards. |
| `rushing_yards_average` | numeric | Rushing: yards average. |
| `safeties_one_point` | integer | Safeties: one point. |
| `safeties_two_point` | integer | Safeties: two point. |
| `score_q1` | integer | Points scored in the 1st quarter. |
| `score_q2` | integer | Points scored in the 2nd quarter. |
| `score_q3` | integer | Points scored in the 3rd quarter. |
| `score_q4` | integer | Points scored in the 4th quarter. |
| `score_ot` | integer | Points scored in overtime. |
| `score_total` | integer | Total points scored. |
| `third_down_attempts` | integer | Third down: attempts. |
| `third_down_conversions` | integer | Third down: conversions. |
| `time_of_possession` | character | Time of possession (MM:SS). |
| `timeouts_remaining` | integer | Timeouts: remaining. |
| `timeouts_used` | integer | Timeouts: used. |
| `total_plays` | integer | Totals: plays. |
| `total_yards` | integer | Totals: yards. |
| `touchdowns_all_other` | integer | Touchdowns: all other. |
| `turnovers` | integer | Total turnovers. |
| `two_point_conversions_defensive_returns` | integer | Two-point conversions: defensive returns. |
| `two_point_conversions_passing_attempts` | integer | Two-point conversions: passing attempts. |
| `two_point_conversions_passing_successes` | integer | Two-point conversions: passing successes. |
| `two_point_conversions_rushing_attempts` | integer | Two-point conversions: rushing attempts. |
| `two_point_conversions_rushing_successes` | integer | Two-point conversions: rushing successes. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_live_team_statistics-example}

```python
nfl_live_team_statistics(game_id='a9a890ed-4feb-11f1-abca-2c54536568a9')
```

_Last validated n/a._

## nfl_live_player_statistics

GET /football/v2/stats/live/player-statistics/{game_id} — one row per player per side: the live player box score.

**Endpoint URL:** `GET https://api.nfl.com/football/v2/stats/live/player-statistics/{game_id}`

**Valid URL:** [https://api.nfl.com/football/v2/stats/live/player-statistics/a9a890ed-4feb-11f1-abca-2c54536568a9](https://api.nfl.com/football/v2/stats/live/player-statistics/a9a890ed-4feb-11f1-abca-2c54536568a9)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Shield uuid game id -- the ``id`` column of the week games and weekly game details listings. |

### Returns {#nfl_live_player_statistics-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | character | NFL.com Shield GUID for the game. |
| `offset` | integer | Live-feed sequence position the statistics reflect; advances as the game is played. |
| `side` | character | Which side of the game the row belongs to: away or home. |
| `team_id` | character | NFL.com Shield GUID of the team. |
| `gsis_player_id` | character | NFL GSIS player id (00-00xxxxx), the key nflverse data joins on. |
| `gsis_player_jersey_number` | character | Player's jersey number as recorded in GSIS. |
| `gsis_player_name` | character | Player name as recorded in GSIS (e.g. P.Mahomes). |
| `person_id` | character | NFL.com Shield GUID of the player (person). |
| `defensive_fumbles_forced` | integer | Defense: fumbles forced. |
| `defensive_fumbles_recovered` | integer | Defense: fumbles recovered. |
| `defensive_interceptions` | integer | Defense: interceptions. |
| `defensive_miscellaneous_fumbles_forced` | integer | Defense (miscellaneous): fumbles forced. |
| `defensive_miscellaneous_fumbles_recovered` | integer | Defense (miscellaneous): fumbles recovered. |
| `defensive_miscellaneous_tackles` | numeric | Defense (miscellaneous): tackles. |
| `defensive_miscellaneous_tackles_assists` | integer | Defense (miscellaneous): tackles assists. |
| `defensive_passes_defended` | integer | Defense: passes defended. |
| `defensive_quarterback_hits` | integer | Defense: quarterback hits. |
| `defensive_sacks` | numeric | Defense: sacks. |
| `defensive_sack_yards` | numeric | Defense: sack yards. |
| `defensive_safeties` | integer | Defense: safeties. |
| `defensive_special_teams_fumbles_forced` | integer | Defense: special teams fumbles forced. |
| `defensive_special_teams_fumbles_recovered` | integer | Defense: special teams fumbles recovered. |
| `defensive_special_teams_tackles` | numeric | Defense: special teams tackles. |
| `defensive_special_teams_tackles_assists` | integer | Defense: special teams tackles assists. |
| `defensive_special_teams_blocks` | integer | Defense: special teams blocks. |
| `defensive_tackles` | numeric | Defense: tackles. |
| `defensive_tackles_assists` | integer | Defense: tackles assists. |
| `defensive_tackles_combined` | numeric | Defense: tackles combined. |
| `defensive_tackles_for_loss` | numeric | Defense: tackles for loss. |
| `defensive_tackles_for_loss_yards` | numeric | Defense: tackles for loss yards. |
| `extra_points_attempted` | integer | Extra points: attempted. |
| `extra_points_made` | integer | Extra points: made. |
| `extra_points_missed` | integer | Extra points: missed. |
| `extra_points_blocked` | integer | Extra points: blocked. |
| `field_goals_attempted` | integer | Field goals: attempted. |
| `field_goals_average_length` | numeric | Field goals: average length. |
| `field_goals_blocked` | integer | Field goals: blocked. |
| `field_goals_longest_made` | integer | Field goals: longest made. |
| `field_goals_made` | integer | Field goals: made. |
| `field_goals_missed` | integer | Field goals: missed. |
| `field_goals_total_yards` | integer | Field goals: total yards. |
| `fumbles` | integer | Fumbles. |
| `fumbles_forced` | integer | Fumbles: forced. |
| `fumbles_lost` | integer | Fumbles: lost. |
| `fumbles_recovered_in_end_zone_for_touchdown` | integer | Fumbles: recovered in end zone for touchdown. |
| `fumbles_opponent_recoveries` | integer | Fumbles: opponent recoveries. |
| `fumbles_opponent_recovery_touchdowns` | integer | Fumbles: opponent recovery touchdowns. |
| `fumbles_opponent_recovery_yards` | integer | Fumbles: opponent recovery yards. |
| `fumbles_out_of_bounds` | integer | Fumbles: out of bounds. |
| `fumbles_own_recoveries` | integer | Fumbles: own recoveries. |
| `fumbles_own_recovery_touchdowns` | integer | Fumbles: own recovery touchdowns. |
| `fumbles_own_recovery_yards` | integer | Fumbles: own recovery yards. |
| `interceptions` | integer | Interceptions thrown. |
| `interceptions_long` | integer | Interceptions: longest. |
| `interceptions_longest_touchdown` | integer | Interceptions: longest touchdown. |
| `interceptions_touchdowns` | integer | Interceptions: touchdowns. |
| `interceptions_yards` | integer | Interceptions: yards. |
| `kickoffs` | integer | Kickoffs. |
| `kickoffs_inside20` | integer | Kickoffs: inside the 20. |
| `kickoffs_out_of_bounds` | integer | Kickoffs: out of bounds. |
| `kickoffs_return_yards` | integer | Kickoffs: return yards. |
| `kickoffs_to_end_zone` | integer | Kickoffs: to end zone. |
| `kickoffs_touchbacks` | integer | Kickoffs: touchbacks. |
| `kickoffs_yards` | integer | Kickoffs: yards. |
| `kick_returns` | integer | Kickoff returns. |
| `kick_returns_fair_catches` | integer | Kickoff returns: fair catches. |
| `kick_returns_longest` | integer | Kickoff returns: longest. |
| `kick_returns_longest_touchdown` | integer | Kickoff returns: longest touchdown. |
| `kick_returns_touchdowns` | integer | Kickoff returns: touchdowns. |
| `kick_returns_yards` | integer | Kickoff returns: yards. |
| `kick_returns_yards_average` | numeric | Kickoff returns: yards average. |
| `passing_attempts` | integer | Passing: attempts. |
| `passing_completions` | integer | Passing: completions. |
| `passing_completion_percent` | numeric | Passing: completion percent. |
| `passing_interceptions` | integer | Passing: interceptions. |
| `passing_long` | integer | Passing: longest. |
| `passing_longest_touchdown_pass` | integer | Passing: longest touchdown pass. |
| `passing_rating` | numeric | Passing: rating. |
| `passing_sack_yards_lost` | numeric | Passing: sack yards lost. |
| `passing_times_sacked` | integer | Passing: times sacked. |
| `passing_touchdowns` | integer | Passing: touchdowns. |
| `passing_yards` | integer | Passing: yards. |
| `passing_yards_average` | numeric | Passing: yards average. |
| `passing_yards_per_attempt` | numeric | Passing: yards per attempt. |
| `punts` | integer | Punts. |
| `punts_blocked` | integer | Punting: blocked. |
| `punts_inside20` | integer | Punting: inside the 20. |
| `punts_longest` | integer | Punting: longest. |
| `punts_return_yards` | integer | Punt return yards allowed on this player's punts. |
| `punts_touchbacks` | integer | Punting: touchbacks. |
| `punts_yards` | integer | Punting: yards. |
| `punts_yards_average_gross` | numeric | Gross punting average (yards per punt). |
| `punts_yards_average_net` | numeric | Net punting average (yards per punt, after returns and touchbacks). |
| `punt_returns` | integer | Punt returns. |
| `punt_returns_fair_catches` | integer | Punt returns: fair catches. |
| `punt_returns_longest` | integer | Punt returns: longest. |
| `punt_returns_longest_touchdown` | integer | Punt returns: longest touchdown. |
| `punt_returns_touchdowns` | integer | Punt returns: touchdowns. |
| `punt_returns_yards` | integer | Punt returns: yards. |
| `punt_returns_yards_average` | numeric | Punt returns: yards average. |
| `receptions` | integer | Receptions. |
| `receptions_average` | numeric | Receiving: average. |
| `receptions_long` | integer | Receiving: longest. |
| `receptions_longest_touchdown` | integer | Receiving: longest touchdown. |
| `receptions_pass_target` | integer | Pass targets. |
| `receptions_touchdowns` | integer | Receiving: touchdowns. |
| `receptions_yards` | integer | Receiving: yards. |
| `receptions_yards_after_catch` | integer | Receiving yards after the catch. |
| `rushing_attempts` | integer | Rushing: attempts. |
| `rushing_average` | numeric | Rushing: average. |
| `rushing_long` | integer | Rushing: longest. |
| `rushing_longest_touchdown` | integer | Rushing: longest touchdown. |
| `rushing_touchdowns` | integer | Rushing: touchdowns. |
| `rushing_yards` | integer | Rushing: yards. |
| `two_point_defensive_attempts` | integer | Two-point conversions: defensive attempts. |
| `two_point_defensive_successes` | integer | Two-point conversions: defensive successes. |
| `two_point_passing_attempts` | integer | Two-point conversions: passing attempts. |
| `two_point_passing_successes` | integer | Two-point conversions: passing successes. |
| `two_point_reception_attempts` | integer | Two-point conversions: reception attempts. |
| `two_point_reception_successes` | integer | Two-point conversions: reception successes. |
| `two_point_rushing_attempts` | integer | Two-point conversions: rushing attempts. |
| `two_point_rushing_successes` | integer | Two-point conversions: rushing successes. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_live_player_statistics-example}

```python
nfl_live_player_statistics(game_id='a9a890ed-4feb-11f1-abca-2c54536568a9')
```

_Last validated n/a._
