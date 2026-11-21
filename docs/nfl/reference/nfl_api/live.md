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
| `game_id` | String | NFL.com Shield GUID for the game. |
| `offset` | Int64 | Live-feed sequence position the statistics reflect; advances as the game is played. |
| `side` | String | Which side of the game the row belongs to: away or home. |
| `team_id` | String | NFL.com Shield GUID of the team. |
| `defensive_fumbles_forced` | Int64 | Defense: fumbles forced. |
| `defensive_fumbles_recovered` | Int64 | Defense: fumbles recovered. |
| `defensive_interceptions` | Int64 | Defense: interceptions. |
| `defensive_passes_defended` | Int64 | Defense: passes defended. |
| `defensive_quarterback_hits` | Int64 | Defense: quarterback hits. |
| `defensive_sacks` | Int64 | Defense: sacks. |
| `defensive_safeties` | Int64 | Defense: safeties. |
| `defensive_tackles_combined` | Float64 | Defense: tackles combined. |
| `defensive_tackles_for_loss` | Float64 | Defense: tackles for loss. |
| `defensive_touchdowns` | Int64 | Defense: touchdowns. |
| `extra_point_kick_attempts` | Int64 | Extra points: kick attempts. |
| `extra_point_kick_blocked` | Int64 | Extra points: kick blocked. |
| `extra_point_kick_made` | Int64 | Extra points: kick made. |
| `field_goals_attempts` | Int64 | Field goals: attempts. |
| `field_goals_blocked` | Int64 | Field goals: blocked. |
| `field_goals_longest_made` | Int64 | Field goals: longest made. |
| `field_goals_made` | Int64 | Field goals: made. |
| `first_downs_passing` | Int64 | First downs: passing. |
| `first_downs_penalty` | Int64 | First downs: penalty. |
| `first_downs_rushing` | Int64 | First downs: rushing. |
| `first_downs_total` | Int64 | First downs: total. |
| `fourth_down_attempts` | Int64 | Fourth down: attempts. |
| `fourth_down_conversions` | Int64 | Fourth down: conversions. |
| `fumbles_lost` | Int64 | Fumbles: lost. |
| `fumbles_made` | Int64 | Fumbles: made. |
| `fumbles_own_recoveries` | Int64 | Fumbles: own recoveries. |
| `fumbles_returned_touchdowns` | Int64 | Fumbles: returned touchdowns. |
| `goal_to_go_attempts` | Int64 | Goal-to-go: attempts. |
| `goal_to_go_successes` | Int64 | Goal-to-go: successes. |
| `interceptions_longest_touchdown` | Int64 | Interceptions: longest touchdown. |
| `interceptions_made` | Int64 | Interceptions made by the defense. |
| `interceptions_returned` | Int64 | Interceptions: returned. |
| `interceptions_returned_touchdowns` | Int64 | Interceptions: returned touchdowns. |
| `interceptions_returned_yards` | Int64 | Interceptions: returned yards. |
| `kick_returns_longest` | Int64 | Kickoff returns: longest. |
| `kick_returns_yards_average` | Float64 | Kickoff returns: yards average. |
| `kickoffs_in_end_zone` | Int64 | Kickoffs: in end zone. |
| `kickoffs_made` | Int64 | Kickoffs: made. |
| `kickoffs_returned` | Int64 | Kickoffs: returned. |
| `kickoffs_returned_touchdowns` | Int64 | Kickoffs: returned touchdowns. |
| `kickoffs_returned_yards` | Int64 | Kickoffs: returned yards. |
| `kickoffs_touchbacks` | Int64 | Kickoffs: touchbacks. |
| `passing_attempts` | Int64 | Passing: attempts. |
| `passing_completions` | Int64 | Passing: completions. |
| `passing_completion_percent` | Float64 | Passing: completion percent. |
| `passing_interceptions` | Int64 | Passing: interceptions. |
| `passing_rating` | Float64 | Passing: rating. |
| `passing_sacks` | Int64 | Passing: sacks. |
| `passing_sack_yards_lost` | Float64 | Passing: sack yards lost. |
| `passing_touchdowns` | Int64 | Passing: touchdowns. |
| `passing_yards` | Int64 | Passing: yards. |
| `passing_yards_average` | Float64 | Passing: yards average. |
| `passing_yards_per_attempt` | Float64 | Passing: yards per attempt. |
| `penalties_made` | Int64 | Penalties: made. |
| `penalties_yards` | Int64 | Penalties: yards. |
| `punt_returns_longest` | Int64 | Punt returns: longest. |
| `punt_returns_yards_average` | Float64 | Punt returns: yards average. |
| `punts_attempts` | Int64 | Punting: attempts. |
| `punts_blocked` | Int64 | Punting: blocked. |
| `punts_inside20` | Int64 | Punting: inside the 20. |
| `punts_longest` | Int64 | Punting: longest. |
| `punts_returned` | Int64 | Punting: returned. |
| `punts_returned_touchdowns` | Int64 | Punting: returned touchdowns. |
| `punts_returned_yards` | Int64 | Punting: returned yards. |
| `punts_touchbacks` | Int64 | Punting: touchbacks. |
| `punts_yards` | Int64 | Punting: yards. |
| `punts_yards_average_gross` | Float64 | Gross punting average (yards per punt). |
| `punts_yards_average_net` | Float64 | Net punting average (yards per punt, after returns and touchbacks). |
| `receptions` | Int64 | Receptions. |
| `receptions_long` | Int64 | Receiving: longest. |
| `receptions_pass_target` | Int64 | Pass targets. |
| `receptions_touchdowns` | Int64 | Receiving: touchdowns. |
| `receptions_yards` | Int64 | Receiving: yards. |
| `receptions_yards_after_catch` | Int64 | Receiving yards after the catch. |
| `red_zone_attempts` | Int64 | Red zone: attempts. |
| `red_zone_successes` | Int64 | Red zone: successes. |
| `rushing_long` | Int64 | Rushing: longest. |
| `rushing_plays` | Int64 | Rushing: plays. |
| `rushing_tackles_for_loss` | Int64 | Rushing: tackles for loss. |
| `rushing_tackles_for_loss_yards` | Int64 | Rushing: tackles for loss yards. |
| `rushing_touchdowns` | Int64 | Rushing: touchdowns. |
| `rushing_yards` | Int64 | Rushing: yards. |
| `rushing_yards_average` | Float64 | Rushing: yards average. |
| `safeties_one_point` | Int64 | Safeties: one point. |
| `safeties_two_point` | Int64 | Safeties: two point. |
| `score_q1` | Int64 | Points scored in the 1st quarter. |
| `score_q2` | Int64 | Points scored in the 2nd quarter. |
| `score_q3` | Int64 | Points scored in the 3rd quarter. |
| `score_q4` | Int64 | Points scored in the 4th quarter. |
| `score_ot` | Int64 | Points scored in overtime. |
| `score_total` | Int64 | Total points scored. |
| `third_down_attempts` | Int64 | Third down: attempts. |
| `third_down_conversions` | Int64 | Third down: conversions. |
| `time_of_possession` | String | Time of possession (MM:SS). |
| `timeouts_remaining` | Int64 | Timeouts: remaining. |
| `timeouts_used` | Int64 | Timeouts: used. |
| `total_plays` | Int64 | Totals: plays. |
| `total_yards` | Int64 | Totals: yards. |
| `touchdowns_all_other` | Int64 | Touchdowns: all other. |
| `turnovers` | Int64 | Total turnovers. |
| `two_point_conversions_defensive_returns` | Int64 | Two-point conversions: defensive returns. |
| `two_point_conversions_passing_attempts` | Int64 | Two-point conversions: passing attempts. |
| `two_point_conversions_passing_successes` | Int64 | Two-point conversions: passing successes. |
| `two_point_conversions_rushing_attempts` | Int64 | Two-point conversions: rushing attempts. |
| `two_point_conversions_rushing_successes` | Int64 | Two-point conversions: rushing successes. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_live_team_statistics-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nfl_live_team_statistics-example}

```python
nfl_live_team_statistics(game_id='a9a890ed-4feb-11f1-abca-2c54536568a9')
```

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
| `game_id` | String | NFL.com Shield GUID for the game. |
| `offset` | Int64 | Live-feed sequence position the statistics reflect; advances as the game is played. |
| `side` | String | Which side of the game the row belongs to: away or home. |
| `team_id` | String | NFL.com Shield GUID of the team. |
| `gsis_player_id` | String | NFL GSIS player id (00-00xxxxx), the key nflverse data joins on. |
| `gsis_player_jersey_number` | String | Player's jersey number as recorded in GSIS. |
| `gsis_player_name` | String | Player name as recorded in GSIS (e.g. P.Mahomes). |
| `person_id` | String | NFL.com Shield GUID of the player (person). |
| `defensive_fumbles_forced` | Int64 | Defense: fumbles forced. |
| `defensive_fumbles_recovered` | Int64 | Defense: fumbles recovered. |
| `defensive_interceptions` | Int64 | Defense: interceptions. |
| `defensive_miscellaneous_fumbles_forced` | Int64 | Defense (miscellaneous): fumbles forced. |
| `defensive_miscellaneous_fumbles_recovered` | Int64 | Defense (miscellaneous): fumbles recovered. |
| `defensive_miscellaneous_tackles` | Float64 | Defense (miscellaneous): tackles. |
| `defensive_miscellaneous_tackles_assists` | Int64 | Defense (miscellaneous): tackles assists. |
| `defensive_passes_defended` | Int64 | Defense: passes defended. |
| `defensive_quarterback_hits` | Int64 | Defense: quarterback hits. |
| `defensive_sacks` | Float64 | Defense: sacks. |
| `defensive_sack_yards` | Float64 | Defense: sack yards. |
| `defensive_safeties` | Int64 | Defense: safeties. |
| `defensive_special_teams_fumbles_forced` | Int64 | Defense: special teams fumbles forced. |
| `defensive_special_teams_fumbles_recovered` | Int64 | Defense: special teams fumbles recovered. |
| `defensive_special_teams_tackles` | Float64 | Defense: special teams tackles. |
| `defensive_special_teams_tackles_assists` | Int64 | Defense: special teams tackles assists. |
| `defensive_special_teams_blocks` | Int64 | Defense: special teams blocks. |
| `defensive_tackles` | Float64 | Defense: tackles. |
| `defensive_tackles_assists` | Int64 | Defense: tackles assists. |
| `defensive_tackles_combined` | Float64 | Defense: tackles combined. |
| `defensive_tackles_for_loss` | Float64 | Defense: tackles for loss. |
| `defensive_tackles_for_loss_yards` | Float64 | Defense: tackles for loss yards. |
| `extra_points_attempted` | Int64 | Extra points: attempted. |
| `extra_points_made` | Int64 | Extra points: made. |
| `extra_points_missed` | Int64 | Extra points: missed. |
| `extra_points_blocked` | Int64 | Extra points: blocked. |
| `field_goals_attempted` | Int64 | Field goals: attempted. |
| `field_goals_average_length` | Float64 | Field goals: average length. |
| `field_goals_blocked` | Int64 | Field goals: blocked. |
| `field_goals_longest_made` | Int64 | Field goals: longest made. |
| `field_goals_made` | Int64 | Field goals: made. |
| `field_goals_missed` | Int64 | Field goals: missed. |
| `field_goals_total_yards` | Int64 | Field goals: total yards. |
| `fumbles` | Int64 | Fumbles. |
| `fumbles_forced` | Int64 | Fumbles: forced. |
| `fumbles_lost` | Int64 | Fumbles: lost. |
| `fumbles_recovered_in_end_zone_for_touchdown` | Int64 | Fumbles: recovered in end zone for touchdown. |
| `fumbles_opponent_recoveries` | Int64 | Fumbles: opponent recoveries. |
| `fumbles_opponent_recovery_touchdowns` | Int64 | Fumbles: opponent recovery touchdowns. |
| `fumbles_opponent_recovery_yards` | Int64 | Fumbles: opponent recovery yards. |
| `fumbles_out_of_bounds` | Int64 | Fumbles: out of bounds. |
| `fumbles_own_recoveries` | Int64 | Fumbles: own recoveries. |
| `fumbles_own_recovery_touchdowns` | Int64 | Fumbles: own recovery touchdowns. |
| `fumbles_own_recovery_yards` | Int64 | Fumbles: own recovery yards. |
| `interceptions` | Int64 | Interceptions thrown. |
| `interceptions_long` | Int64 | Interceptions: longest. |
| `interceptions_longest_touchdown` | Int64 | Interceptions: longest touchdown. |
| `interceptions_touchdowns` | Int64 | Interceptions: touchdowns. |
| `interceptions_yards` | Int64 | Interceptions: yards. |
| `kickoffs` | Int64 | Kickoffs. |
| `kickoffs_inside20` | Int64 | Kickoffs: inside the 20. |
| `kickoffs_out_of_bounds` | Int64 | Kickoffs: out of bounds. |
| `kickoffs_return_yards` | Int64 | Kickoffs: return yards. |
| `kickoffs_to_end_zone` | Int64 | Kickoffs: to end zone. |
| `kickoffs_touchbacks` | Int64 | Kickoffs: touchbacks. |
| `kickoffs_yards` | Int64 | Kickoffs: yards. |
| `kick_returns` | Int64 | Kickoff returns. |
| `kick_returns_fair_catches` | Int64 | Kickoff returns: fair catches. |
| `kick_returns_longest` | Int64 | Kickoff returns: longest. |
| `kick_returns_longest_touchdown` | Int64 | Kickoff returns: longest touchdown. |
| `kick_returns_touchdowns` | Int64 | Kickoff returns: touchdowns. |
| `kick_returns_yards` | Int64 | Kickoff returns: yards. |
| `kick_returns_yards_average` | Float64 | Kickoff returns: yards average. |
| `passing_attempts` | Int64 | Passing: attempts. |
| `passing_completions` | Int64 | Passing: completions. |
| `passing_completion_percent` | Float64 | Passing: completion percent. |
| `passing_interceptions` | Int64 | Passing: interceptions. |
| `passing_long` | Int64 | Passing: longest. |
| `passing_longest_touchdown_pass` | Int64 | Passing: longest touchdown pass. |
| `passing_rating` | Float64 | Passing: rating. |
| `passing_sack_yards_lost` | Float64 | Passing: sack yards lost. |
| `passing_times_sacked` | Int64 | Passing: times sacked. |
| `passing_touchdowns` | Int64 | Passing: touchdowns. |
| `passing_yards` | Int64 | Passing: yards. |
| `passing_yards_average` | Float64 | Passing: yards average. |
| `passing_yards_per_attempt` | Float64 | Passing: yards per attempt. |
| `punts` | Int64 | Punts. |
| `punts_blocked` | Int64 | Punting: blocked. |
| `punts_inside20` | Int64 | Punting: inside the 20. |
| `punts_longest` | Int64 | Punting: longest. |
| `punts_return_yards` | Int64 | Punt return yards allowed on this player's punts. |
| `punts_touchbacks` | Int64 | Punting: touchbacks. |
| `punts_yards` | Int64 | Punting: yards. |
| `punts_yards_average_gross` | Float64 | Gross punting average (yards per punt). |
| `punts_yards_average_net` | Float64 | Net punting average (yards per punt, after returns and touchbacks). |
| `punt_returns` | Int64 | Punt returns. |
| `punt_returns_fair_catches` | Int64 | Punt returns: fair catches. |
| `punt_returns_longest` | Int64 | Punt returns: longest. |
| `punt_returns_longest_touchdown` | Int64 | Punt returns: longest touchdown. |
| `punt_returns_touchdowns` | Int64 | Punt returns: touchdowns. |
| `punt_returns_yards` | Int64 | Punt returns: yards. |
| `punt_returns_yards_average` | Float64 | Punt returns: yards average. |
| `receptions` | Int64 | Receptions. |
| `receptions_average` | Float64 | Receiving: average. |
| `receptions_long` | Int64 | Receiving: longest. |
| `receptions_longest_touchdown` | Int64 | Receiving: longest touchdown. |
| `receptions_pass_target` | Int64 | Pass targets. |
| `receptions_touchdowns` | Int64 | Receiving: touchdowns. |
| `receptions_yards` | Int64 | Receiving: yards. |
| `receptions_yards_after_catch` | Int64 | Receiving yards after the catch. |
| `rushing_attempts` | Int64 | Rushing: attempts. |
| `rushing_average` | Float64 | Rushing: average. |
| `rushing_long` | Int64 | Rushing: longest. |
| `rushing_longest_touchdown` | Int64 | Rushing: longest touchdown. |
| `rushing_touchdowns` | Int64 | Rushing: touchdowns. |
| `rushing_yards` | Int64 | Rushing: yards. |
| `two_point_defensive_attempts` | Int64 | Two-point conversions: defensive attempts. |
| `two_point_defensive_successes` | Int64 | Two-point conversions: defensive successes. |
| `two_point_passing_attempts` | Int64 | Two-point conversions: passing attempts. |
| `two_point_passing_successes` | Int64 | Two-point conversions: passing successes. |
| `two_point_reception_attempts` | Int64 | Two-point conversions: reception attempts. |
| `two_point_reception_successes` | Int64 | Two-point conversions: reception successes. |
| `two_point_rushing_attempts` | Int64 | Two-point conversions: rushing attempts. |
| `two_point_rushing_successes` | Int64 | Two-point conversions: rushing successes. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_live_player_statistics-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nfl_live_player_statistics-example}

```python
nfl_live_player_statistics(game_id='a9a890ed-4feb-11f1-abca-2c54536568a9')
```
