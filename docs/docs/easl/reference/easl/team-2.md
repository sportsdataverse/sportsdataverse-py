---
title: "EASL — EASL sync server (easl.joshricha.com, third-party over Sportradar DataCore) — Team: stats–leaders"
sidebar_label: "Team: stats–leaders"
sidebar_position: 3
description: "EASL — EASL sync server (easl.joshricha.com, third-party over Sportradar DataCore) — Team: stats–leaders — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# EASL — EASL sync server (easl.joshricha.com, third-party over Sportradar DataCore) — Team: stats–leaders

## easl_team_stats

A team's cumulative season statistics: totals, per-game rates and the opponent's totals (*_against).

**Endpoint URL:** `GET https://easl.joshricha.com/api/v1/teams/{team_id}/stats`

**Valid URL:** [https://easl.joshricha.com/api/v1/teams/865489e8-b0f5-11ed-8b3f-c199a9dea184/stats?season=2026](https://easl.joshricha.com/api/v1/teams/865489e8-b0f5-11ed-8b3f-c199a9dea184/stats?season=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | DataCore team (entity) UUID v1, e.g. '865489e8-b0f5-11ed-8b3f-c199a9dea184' (Utsunomiya Brex): the team_id of easl_teams, easl_standings and the box scores. easl_team resolves the 12 historical teams easl_teams does not list. |
| `season` | `season` |  |  | `Y` | Season END year (2026 = EASL 25-26; the four editions are 2024 to 2027). easl_runtime reads easl_seasons and maps it to the DataCore seasonId. Default: the ACTIVE season. |

### Returns {#easl_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | String | DataCore team UUID v1, from the request (the body carries none). |
| `season` | Int64 | Season END year (2026 = EASL 25-26), from the request. |
| `season_id` | String | DataCore season UUID v1, from the request. |
| `assists` | Int64 | Assists: passes that directly lead to a made field goal. Season total of the team's finished games. |
| `assists_against` | Int64 | Opponent's figure in the same games: assists: passes that directly lead to a made field goal. Season total of the team's finished games. |
| `assists_against_per_game` | Float64 | Opponent's figure in the same games: assists: passes that directly lead to a made field goal. Per-game average: the total divided by `games`. |
| `assists_defensive` | Int64 | Defensive assists (DataCore column; meaning unverified). Season total of the team's finished games. |
| `assists_per_game` | Float64 | Assists: passes that directly lead to a made field goal. Per-game average: the total divided by `games`. |
| `assists_turnover_ratio` | Float64 | Assists divided by turnovers (a ratio, not a percentage). Season total of the team's finished games. |
| `biggest_lead` | Int64 | Largest lead the team held during the game, in points. A season total sums the per-game maxima. Season total of the team's finished games. |
| `biggest_scoring_run` | Int64 | Longest unanswered scoring run of the team, in points. A season total sums the per-game maxima. Season total of the team's finished games. |
| `blocks` | Int64 | Shots blocked (made by this player or team). Season total of the team's finished games. |
| `blocks_per_game` | Float64 | Shots blocked (made by this player or team). Per-game average: the total divided by `games`. |
| `blocks_received` | Int64 | The team's or player's own shots that were blocked by an opponent. Season total of the team's finished games. |
| `blocks_received_per_game` | Float64 | The team's or player's own shots that were blocked by an opponent. Per-game average: the total divided by `games`. |
| `draws` | Int64 | Drawn games (basketball has none). Season total of the team's finished games. |
| `dunks` | Int64 | Dunks made. Season total of the team's finished games. |
| `dunks_per_game` | Float64 | Dunks made. Per-game average: the total divided by `games`. |
| `efficiency` | Int64 | NBA-style efficiency: points + rebounds + assists + steals + blocks - missed field goals - missed free throws - turnovers (verified on all 2,143 played player-games). Season total of the team's finished games. |
| `efficiency_custom` | Int64 | Custom efficiency slot (DataCore column; meaning unverified). Season total of the team's finished games. |
| `field_goals_against_percentage` | Float64 | Field-goal percentage allowed to opponents over the season (0-100 scale). Season total of the team's finished games. |
| `field_goals_attempted` | Int64 | Field goals attempted (2- and 3-point shots; free throws excluded). Season total of the team's finished games. |
| `field_goals_attempted_against` | Int64 | Opponent's figure in the same games: field goals attempted (2- and 3-point shots; free throws excluded). Season total of the team's finished games. |
| `field_goals_attempted_against_per_game` | Float64 | Opponent's figure in the same games: field goals attempted (2- and 3-point shots; free throws excluded). Per-game average: the total divided by `games`. |
| `field_goals_effective_percentage` | Float64 | Effective field-goal percentage on a 0-100 scale: (FGM + 0.5 x 3PM) / FGA x 100; can exceed 100 on small samples. Season total of the team's finished games. |
| `field_goals_made` | Int64 | Field goals made (2- and 3-point shots). Season total of the team's finished games. |
| `field_goals_made_against` | Int64 | Opponent's figure in the same games: field goals made (2- and 3-point shots). Season total of the team's finished games. |
| `field_goals_made_against_per_game` | Float64 | Opponent's figure in the same games: field goals made (2- and 3-point shots). Per-game average: the total divided by `games`. |
| `field_goals_percentage` | Float64 | Field-goal percentage on a 0-100 scale (made / attempted x 100). Season total of the team's finished games. |
| `fouls_bench_technical` | Int64 | Technical fouls called on the bench (not a player). Season total of the team's finished games. |
| `fouls_coach_disqualifying` | Int64 | Disqualifying fouls on the coach. Season total of the team's finished games. |
| `fouls_coach_technical` | Int64 | Technical fouls on the coach. Season total of the team's finished games. |
| `fouls_disqualifying` | Int64 | Disqualifying fouls. Season total of the team's finished games. |
| `fouls_disruptive` | Int64 | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Int64). |
| `fouls_drawn` | Int64 | Fouls drawn (committed against this player or team by an opponent). Season total of the team's finished games. |
| `fouls_drawn_per_game` | Float64 | Fouls drawn (committed against this player or team by an opponent). Per-game average: the total divided by `games`. |
| `fouls_flagrant` | Int64 | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Int64). |
| `fouls_offensive` | Int64 | Offensive fouls committed. Counted separately from foulsPersonal. Season total of the team's finished games. |
| `fouls_personal` | Int64 | Personal fouls committed (offensive, technical and unsportsmanlike fouls are separate columns). Season total of the team's finished games. |
| `fouls_personal_against` | Int64 | Opponent's figure in the same games: personal fouls committed (offensive, technical and unsportsmanlike fouls are separate columns). Season total of the team's finished games. |
| `fouls_personal_against_per_game` | Float64 | Opponent's figure in the same games: personal fouls committed (offensive, technical and unsportsmanlike fouls are separate columns). Per-game average: the total divided by `games`. |
| `fouls_personal_per_game` | Float64 | Personal fouls committed (offensive, technical and unsportsmanlike fouls are separate columns). Per-game average: the total divided by `games`. |
| `fouls_technical` | Int64 | Technical fouls committed by the player or team. Season total of the team's finished games. |
| `fouls_technical_against` | Int64 | Opponent's figure in the same games: technical fouls committed by the player or team. Season total of the team's finished games. |
| `fouls_technical_against_per_game` | Float64 | Opponent's figure in the same games: technical fouls committed by the player or team. Per-game average: the total divided by `games`. |
| `fouls_technical_cat1` | Int64 | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Int64). |
| `fouls_technical_cat2` | Int64 | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Int64). |
| `fouls_total` | Int64 | Fouls charged in total: personal + technical + unsportsmanlike + offensive (matches 2,142 of 2,143 played player-games; the exception is unexplained). Season total of the team's finished games. |
| `fouls_unsportsmanlike` | Int64 | Unsportsmanlike fouls committed. Season total of the team's finished games. |
| `free_throws_against_percentage` | Float64 | Free-throw percentage allowed to opponents over the season (0-100 scale). Season total of the team's finished games. |
| `free_throws_attempted` | Int64 | Free throws attempted. Season total of the team's finished games. |
| `free_throws_attempted_against` | Int64 | Opponent's figure in the same games: free throws attempted. Season total of the team's finished games. |
| `free_throws_attempted_against_per_game` | Float64 | Opponent's figure in the same games: free throws attempted. Per-game average: the total divided by `games`. |
| `free_throws_attempted_per_game` | Float64 | Free throws attempted. Per-game average: the total divided by `games`. |
| `free_throws_made` | Int64 | Free throws made. Season total of the team's finished games. |
| `free_throws_made_against` | Int64 | Opponent's figure in the same games: free throws made. Season total of the team's finished games. |
| `free_throws_made_against_per_game` | Float64 | Opponent's figure in the same games: free throws made. Per-game average: the total divided by `games`. |
| `free_throws_made_per_game` | Float64 | Free throws made. Per-game average: the total divided by `games`. |
| `free_throws_percentage` | Float64 | Free-throw percentage on a 0-100 scale. Season total of the team's finished games. |
| `games` | Int64 | Finished games counted in this line. Season total of the team's finished games. |
| `index_of_success` | Int64 | Index of success: efficiency + fouls drawn - personal fouls - technical fouls - blocks received (verified on all 2,143 played player-games). It differs from pir only in which fouls it subtracts. Season total of the team's finished games. |
| `losses` | Int64 | Games lost (for a player: by his team while he is in the line). Season total of the team's finished games. |
| `minutes` | String | Playing time as an ISO 8601 duration PT{m}M{s}S with the minutes NOT wrapped at 60 (PT27M53S; a team's is the sum of its players', PT200M for a 40-minute game, PT250M with one overtime). Season total of the team's finished games. |
| `offensive_rating` | Int64 | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Int64). |
| `pir` | Int64 | Performance index rating: efficiency + fouls drawn - total fouls - blocks received (verified on all 2,143 played player-games). Season total of the team's finished games. |
| `points` | Int64 | Points scored. Season total of the team's finished games. |
| `points_against` | Int64 | Opponent's figure in the same games: points scored. Season total of the team's finished games. |
| `points_against_per_game` | Float64 | Opponent's figure in the same games: points scored. Per-game average: the total divided by `games`. |
| `points_fast_break` | Int64 | Points scored on fast breaks. Season total of the team's finished games. |
| `points_fast_break_against` | Int64 | Opponent's figure in the same games: points scored on fast breaks. Season total of the team's finished games. |
| `points_fast_break_against_per_game` | Float64 | Opponent's figure in the same games: points scored on fast breaks. Per-game average: the total divided by `games`. |
| `points_fast_break_attempted` | Int64 | Fast-break field goals attempted. Season total of the team's finished games. |
| `points_fast_break_made` | Int64 | Fast-break field goals made. Season total of the team's finished games. |
| `points_fast_break_per_game` | Float64 | Points scored on fast breaks. Per-game average: the total divided by `games`. |
| `points_four_attempted` | Int64 | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Int64). |
| `points_four_attempted_against` | Int64 | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Int64). |
| `points_four_attempted_against_per_game` | Float64 | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Float64). |
| `points_four_attempted_per_game` | Float64 | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Float64). |
| `points_four_made` | Int64 | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Int64). |
| `points_four_made_against` | Int64 | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Int64). |
| `points_four_made_against_per_game` | Float64 | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Float64). |
| `points_four_made_per_game` | Float64 | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Float64). |
| `points_four_percentage` | Float64 | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Float64). |
| `points_four_percentage_against` | Int64 | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Int64). |
| `points_from_bench` | Int64 | Points scored by players who did not start (a team figure). Season total of the team's finished games. |
| `points_from_bench_against` | Int64 | Opponent's figure in the same games: points scored by players who did not start (a team figure). Season total of the team's finished games. |
| `points_from_bench_against_per_game` | Float64 | Opponent's figure in the same games: points scored by players who did not start (a team figure). Per-game average: the total divided by `games`. |
| `points_from_bench_per_game` | Float64 | Points scored by players who did not start (a team figure). Per-game average: the total divided by `games`. |
| `points_from_turnover` | Int64 | Points scored off opponent turnovers. Season total of the team's finished games. |
| `points_from_turnover_against` | Int64 | Opponent's figure in the same games: points scored off opponent turnovers. Season total of the team's finished games. |
| `points_from_turnover_against_per_game` | Float64 | Opponent's figure in the same games: points scored off opponent turnovers. Per-game average: the total divided by `games`. |
| `points_in_the_paint` | Int64 | Points scored in the paint. Season total of the team's finished games. |
| `points_in_the_paint_against` | Int64 | Opponent's figure in the same games: points scored in the paint. Season total of the team's finished games. |
| `points_in_the_paint_against_per_game` | Float64 | Opponent's figure in the same games: points scored in the paint. Per-game average: the total divided by `games`. |
| `points_in_the_paint_attempted` | Int64 | Field goals attempted in the paint. Season total of the team's finished games. |
| `points_in_the_paint_made` | Int64 | Field goals made in the paint. Season total of the team's finished games. |
| `points_in_the_paint_per_game` | Float64 | Points scored in the paint. Per-game average: the total divided by `games`. |
| `points_per_game` | Float64 | Points scored. Per-game average: the total divided by `games`. |
| `points_second_chance` | Int64 | Points scored on second-chance possessions (after an offensive rebound). Season total of the team's finished games. |
| `points_second_chance_against` | Int64 | Opponent's figure in the same games: points scored on second-chance possessions (after an offensive rebound). Season total of the team's finished games. |
| `points_second_chance_against_per_game` | Float64 | Opponent's figure in the same games: points scored on second-chance possessions (after an offensive rebound). Per-game average: the total divided by `games`. |
| `points_second_chance_attempted` | Int64 | Second-chance field goals attempted. Season total of the team's finished games. |
| `points_second_chance_made` | Int64 | Second-chance field goals made. Season total of the team's finished games. |
| `points_second_chance_per_game` | Float64 | Points scored on second-chance possessions (after an offensive rebound). Per-game average: the total divided by `games`. |
| `points_three_attempted` | Int64 | 3-point field goals attempted. Season total of the team's finished games. |
| `points_three_attempted_against` | Int64 | Opponent's figure in the same games: 3-point field goals attempted. Season total of the team's finished games. |
| `points_three_attempted_against_per_game` | Float64 | Opponent's figure in the same games: 3-point field goals attempted. Per-game average: the total divided by `games`. |
| `points_three_attempted_per_game` | Float64 | 3-point field goals attempted. Per-game average: the total divided by `games`. |
| `points_three_made` | Int64 | 3-point field goals made. Season total of the team's finished games. |
| `points_three_made_against` | Int64 | Opponent's figure in the same games: 3-point field goals made. Season total of the team's finished games. |
| `points_three_made_against_per_game` | Float64 | Opponent's figure in the same games: 3-point field goals made. Per-game average: the total divided by `games`. |
| `points_three_made_per_game` | Float64 | 3-point field goals made. Per-game average: the total divided by `games`. |
| `points_three_percentage` | Float64 | 3-point percentage on a 0-100 scale. Season total of the team's finished games. |
| `points_three_percentage_against` | Float64 | Opponent's figure in the same games: 3-point percentage on a 0-100 scale. Season total of the team's finished games. |
| `points_two_attempted` | Int64 | 2-point field goals attempted. Season total of the team's finished games. |
| `points_two_attempted_against` | Int64 | Opponent's figure in the same games: 2-point field goals attempted. Season total of the team's finished games. |
| `points_two_attempted_against_per_game` | Float64 | Opponent's figure in the same games: 2-point field goals attempted. Per-game average: the total divided by `games`. |
| `points_two_attempted_per_game` | Float64 | 2-point field goals attempted. Per-game average: the total divided by `games`. |
| `points_two_made` | Int64 | 2-point field goals made. Season total of the team's finished games. |
| `points_two_made_against` | Int64 | Opponent's figure in the same games: 2-point field goals made. Season total of the team's finished games. |
| `points_two_made_against_per_game` | Float64 | Opponent's figure in the same games: 2-point field goals made. Per-game average: the total divided by `games`. |
| `points_two_made_per_game` | Float64 | 2-point field goals made. Per-game average: the total divided by `games`. |
| `points_two_percentage` | Float64 | 2-point percentage on a 0-100 scale. Season total of the team's finished games. |
| `points_two_percentage_against` | Float64 | Opponent's figure in the same games: 2-point percentage on a 0-100 scale. Season total of the team's finished games. |
| `power_play_points` | Int64 | Power-play points (an ice-hockey column; meaning in basketball unverified). Season total of the team's finished games. |
| `power_play_points_awarded` | Int64 | Power-play points awarded (an ice-hockey column; meaning in basketball unverified). Season total of the team's finished games. |
| `rebounds` | Int64 | Total rebounds: offensive + defensive. Team's totals include team rebounds. Season total of the team's finished games. |
| `rebounds_against` | Int64 | Opponent's figure in the same games: total rebounds: offensive + defensive. Team's totals include team rebounds. Season total of the team's finished games. |
| `rebounds_against_per_game` | Float64 | Opponent's figure in the same games: total rebounds: offensive + defensive. Team's totals include team rebounds. Per-game average: the total divided by `games`. |
| `rebounds_defensive` | Int64 | Defensive rebounds. Season total of the team's finished games. |
| `rebounds_defensive_against` | Int64 | Opponent's figure in the same games: defensive rebounds. Season total of the team's finished games. |
| `rebounds_defensive_against_per_game` | Float64 | Opponent's figure in the same games: defensive rebounds. Per-game average: the total divided by `games`. |
| `rebounds_defensive_per_game` | Float64 | Defensive rebounds. Per-game average: the total divided by `games`. |
| `rebounds_offensive` | Int64 | Offensive rebounds. Season total of the team's finished games. |
| `rebounds_offensive_against` | Int64 | Opponent's figure in the same games: offensive rebounds. Season total of the team's finished games. |
| `rebounds_offensive_against_per_game` | Float64 | Opponent's figure in the same games: offensive rebounds. Per-game average: the total divided by `games`. |
| `rebounds_offensive_per_game` | Float64 | Offensive rebounds. Per-game average: the total divided by `games`. |
| `rebounds_per_game` | Float64 | Total rebounds: offensive + defensive. Team's totals include team rebounds. Per-game average: the total divided by `games`. |
| `rebounds_team_defensive` | Int64 | Defensive rebounds credited to the team as a whole (not to a player). Season total of the team's finished games. |
| `rebounds_team_defensive_against` | Int64 | Opponent's figure in the same games: defensive rebounds credited to the team as a whole (not to a player). Season total of the team's finished games. |
| `rebounds_team_defensive_against_per_game` | Float64 | Opponent's figure in the same games: defensive rebounds credited to the team as a whole (not to a player). Per-game average: the total divided by `games`. |
| `rebounds_team_offensive` | Int64 | Offensive rebounds credited to the team as a whole. Season total of the team's finished games. |
| `rebounds_team_offensive_against` | Int64 | Opponent's figure in the same games: offensive rebounds credited to the team as a whole. Season total of the team's finished games. |
| `rebounds_team_offensive_against_per_game` | Float64 | Opponent's figure in the same games: offensive rebounds credited to the team as a whole. Per-game average: the total divided by `games`. |
| `rebounds_team_total` | Int64 | Team rebounds credited to the team as a whole (offensive + defensive). Season total of the team's finished games. |
| `rebounds_team_total_against` | Int64 | Opponent's figure in the same games: team rebounds credited to the team as a whole (offensive + defensive). Season total of the team's finished games. |
| `rebounds_team_total_against_per_game` | Float64 | Opponent's figure in the same games: team rebounds credited to the team as a whole (offensive + defensive). Per-game average: the total divided by `games`. |
| `steals` | Int64 | Steals. Season total of the team's finished games. |
| `steals_against` | Int64 | Opponent's figure in the same games: steals. Season total of the team's finished games. |
| `steals_against_per_game` | Float64 | Opponent's figure in the same games: steals. Per-game average: the total divided by `games`. |
| `steals_per_game` | Float64 | Steals. Per-game average: the total divided by `games`. |
| `true_shooting_attempts` | Float64 | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Float64). |
| `true_shooting_percentage` | Float64 | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Float64). |
| `turnovers` | Int64 | Turnovers committed by players. Season total of the team's finished games. |
| `turnovers_against` | Int64 | Opponent's figure in the same games: turnovers committed by players. Season total of the team's finished games. |
| `turnovers_against_per_game` | Float64 | Opponent's figure in the same games: turnovers committed by players. Per-game average: the total divided by `games`. |
| `turnovers_per_game` | Float64 | Turnovers committed by players. Per-game average: the total divided by `games`. |
| `turnovers_percentage` | Float64 | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Float64). |
| `turnovers_team` | Int64 | Team turnovers not charged to a player (shot-clock and similar violations). Season total of the team's finished games. |
| `turnovers_team_against` | Int64 | Opponent's figure in the same games: team turnovers not charged to a player (shot-clock and similar violations). Season total of the team's finished games. |
| `turnovers_team_against_per_game` | Float64 | Opponent's figure in the same games: team turnovers not charged to a player (shot-clock and similar violations). Per-game average: the total divided by `games`. |
| `wins` | Int64 | Games won (for a player: by his team while he is in the line). Season total of the team's finished games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#easl_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The server has nothing for the request (an empty list or leaders object, a team or player without games, a 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown fixture is the server's HTTP 500), an error envelope, a game that is not finished or whose box score or play-by-play is incomplete, a stalled sync on the active season, or a body for another game, season, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed id or a season that is not one of the server's END years (raised before the request that would use it). |

### Example {#easl_team_stats-example}

```python
easl_team_stats(season=2026, team_id='865489e8-b0f5-11ed-8b3f-c199a9dea184')
```

## easl_team_leaders

A team's top-5 player leaders per category in one season, shaped like easl_leaders.

**Endpoint URL:** `GET https://easl.joshricha.com/api/v1/teams/{team_id}/leaders`

**Valid URL:** [https://easl.joshricha.com/api/v1/teams/865489e8-b0f5-11ed-8b3f-c199a9dea184/leaders?season=2026](https://easl.joshricha.com/api/v1/teams/865489e8-b0f5-11ed-8b3f-c199a9dea184/leaders?season=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | DataCore team (entity) UUID v1, e.g. '865489e8-b0f5-11ed-8b3f-c199a9dea184' (Utsunomiya Brex): the team_id of easl_teams, easl_standings and the box scores. easl_team resolves the 12 historical teams easl_teams does not list. |
| `season` | `season` |  |  | `Y` | Season END year (2026 = EASL 25-26; the four editions are 2024 to 2027). easl_runtime reads easl_seasons and maps it to the DataCore seasonId. Default: the ACTIVE season. |

### Returns {#easl_team_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = EASL 25-26): stamped by easl_runtime from the request, null when the body is parsed bare. |
| `season_id` | String | DataCore season UUID v1 of the leader row. |
| `category` | String | Leader category, the wire key of the group: pointsPerGame, reboundsPerGame, assistsPerGame, stealsPerGame, blocksPerGame, pointsThreeMade, pointsThreePercentage, fieldGoalsPercentage or freeThrowsPercentage. |
| `rank` | Int64 | Rank inside the category, 1 = best, in the delivered order (the server lists the top 5). |
| `value` | Float64 | The leader's value in the category: a per-game average, a count (pointsThreeMade) or a percentage (0-100). Float64. |
| `player_id` | String | playerId: DataCore person UUID v1 (key space: EASL persons, the same id as the box score's `personId`; joins /players, leaders and play-by-play players). Not a team id. |
| `player_name` | String | Player display name in 'FAMILY Given' form with the family name upper-cased (OSAKABE Taiki), in Latin script. |
| `player_name_latin` | String | Alternative name: equal to nameFullLocal for 257 of 552 players, otherwise null; never non-Latin in the observed data. |
| `jersey_number` | String | Jersey number as a string ('00', '0' and '75' are distinct). |
| `position` | String | Playing position: G, F, C, PG, SG, SF, PF or GF (guard-forward). |
| `player_status` | String | Registration state of the player in the database: ACTIVE, INACTIVE or UNREGISTERED. |
| `team_id` | String | teamId: DataCore team (entity) UUID v1 (key space: EASL teams, the same id as `entityId`; joins /teams, /standings team.id, fixture competitors, box and play-by-play teams). Not a player id. |
| `team_name` | String | Display name. Despite the field name this is the English / Latin-script name used on the league site. |
| `team_name_native` | String | Alternative name: the native-script name of some Japanese clubs (for example 宇都宮ブレックス for Utsunomiya Brex), blank for most clubs; for players it usually equals nameFullLocal. Despite the field name it is not always Latin script. |
| `team_code_latin` | String | Three-letter club code (for example BRE); null or empty for some clubs. |
| `team_code_local` | String | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (String). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#easl_team_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The server has nothing for the request (an empty list or leaders object, a team or player without games, a 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown fixture is the server's HTTP 500), an error envelope, a game that is not finished or whose box score or play-by-play is incomplete, a stalled sync on the active season, or a body for another game, season, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed id or a season that is not one of the server's END years (raised before the request that would use it). |

### Example {#easl_team_leaders-example}

```python
easl_team_leaders(season=2026, team_id='865489e8-b0f5-11ed-8b3f-c199a9dea184')
```
