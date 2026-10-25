---
title: "EASL — EASL sync server (easl.joshricha.com, third-party over Sportradar DataCore) — Team: box–schedule"
sidebar_label: "Team: box–schedule"
sidebar_position: 2
description: "EASL — EASL sync server (easl.joshricha.com, third-party over Sportradar DataCore) — Team: box–schedule — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# EASL — EASL sync server (easl.joshricha.com, third-party over Sportradar DataCore) — Team: box–schedule

## easl_team_box

Team totals of a finished game, one row per team, with the opponent's totals as *_against columns.

**Endpoint URL:** `GET https://easl.joshricha.com/api/v1/matches/{fixture_id}/teamstats`

**Valid URL:** [https://easl.joshricha.com/api/v1/matches/10af8fd4-106e-11f1-960b-5798087c7aa3/teamstats](https://easl.joshricha.com/api/v1/matches/10af8fd4-106e-11f1-960b-5798087c7aa3/teamstats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `fixture_id` | `fixture_id` |  | `Y` |  | DataCore fixture (game) UUID v1, e.g. '10af8fd4-106e-11f1-960b-5798087c7aa3' (the 2025-26 EASL Championship): the 'id' of easl_schedule rows. Only finished (CONFIRMED) games are served; a malformed id is a ValueError. |

### Returns {#easl_team_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `fixture_id` | character | fixtureId: DataCore fixture (game) UUID v1 (key space: EASL games; the {fixtureId} of every /matches route). |
| `team_id` | character | DataCore team (entity) UUID v1 (the wire's entityId). |
| `team_name` | character | Display name. Despite the field name this is the English / Latin-script name used on the league site. |
| `team_name_native` | character | Alternative name: the native-script name of some Japanese clubs (for example 宇都宮ブレックス for Utsunomiya Brex), blank for most clubs; for players it usually equals nameFullLocal. Despite the field name it is not always Latin script. Null on some rows. |
| `assists` | integer | Assists: passes that directly lead to a made field goal. Team total for the game. |
| `assists_against` | integer | Opponent's figure in the same games: assists: passes that directly lead to a made field goal. Team total for the game. Absent on some rows. |
| `assists_defensive` | integer | Defensive assists (DataCore column; meaning unverified). Team total for the game. |
| `assists_turnover_ratio` | numeric | Assists divided by turnovers (a ratio, not a percentage). Team total for the game. |
| `biggest_lead` | integer | Largest lead the team held during the game, in points. A season total sums the per-game maxima. Team total for the game. |
| `biggest_scoring_run` | integer | Longest unanswered scoring run of the team, in points. A season total sums the per-game maxima. Team total for the game. |
| `blocks` | integer | Shots blocked (made by this player or team). Team total for the game. |
| `blocks_received` | integer | The team's or player's own shots that were blocked by an opponent. Team total for the game. |
| `dunks` | integer | Dunks made. Team total for the game. |
| `efficiency` | integer | NBA-style efficiency: points + rebounds + assists + steals + blocks - missed field goals - missed free throws - turnovers (verified on all 2,143 played player-games). Team total for the game. |
| `efficiency_custom` | integer | Custom efficiency slot (DataCore column; meaning unverified). Team total for the game. |
| `field_goals_attempted` | integer | Field goals attempted (2- and 3-point shots; free throws excluded). Team total for the game. |
| `field_goals_attempted_against` | integer | Opponent's figure in the same games: field goals attempted (2- and 3-point shots; free throws excluded). Team total for the game. Absent on some rows. |
| `field_goals_effective_percentage` | numeric | Effective field-goal percentage on a 0-100 scale: (FGM + 0.5 x 3PM) / FGA x 100; can exceed 100 on small samples. Team total for the game. |
| `field_goals_made` | integer | Field goals made (2- and 3-point shots). Team total for the game. |
| `field_goals_made_against` | integer | Opponent's figure in the same games: field goals made (2- and 3-point shots). Team total for the game. Absent on some rows. |
| `field_goals_percentage` | numeric | Field-goal percentage on a 0-100 scale (made / attempted x 100). Team total for the game. |
| `field_goals_percentage_against` | numeric | Opponent's figure in the same games: field-goal percentage on a 0-100 scale (made / attempted x 100). Team total for the game. Absent on some rows. |
| `fouls_bench_technical` | integer | Technical fouls called on the bench (not a player). Team total for the game. |
| `fouls_coach_disqualifying` | integer | Disqualifying fouls on the coach. Team total for the game. |
| `fouls_coach_technical` | integer | Technical fouls on the coach. Team total for the game. |
| `fouls_disqualifying` | integer | Disqualifying fouls. Team total for the game. |
| `fouls_disruptive` | integer | Disruptive fouls (DataCore column; filled from 2026-27). Team total for the game. Absent on some rows. |
| `fouls_drawn` | integer | Fouls drawn (committed against this player or team by an opponent). Team total for the game. |
| `fouls_flagrant` | integer | Flagrant fouls (DataCore column; filled from 2026-27). Team total for the game. Absent on some rows. |
| `fouls_offensive` | integer | Offensive fouls committed. Counted separately from foulsPersonal. Team total for the game. |
| `fouls_personal` | integer | Personal fouls committed (offensive, technical and unsportsmanlike fouls are separate columns). Team total for the game. |
| `fouls_personal_against` | integer | Opponent's figure in the same games: personal fouls committed (offensive, technical and unsportsmanlike fouls are separate columns). Team total for the game. Absent on some rows. |
| `fouls_technical` | integer | Technical fouls committed by the player or team. Team total for the game. |
| `fouls_technical_against` | integer | Opponent's figure in the same games: technical fouls committed by the player or team. Team total for the game. Absent on some rows. |
| `fouls_technical_cat1` | integer | Technical fouls of category 1 (DataCore column; filled from 2026-27). Team total for the game. Absent on some rows. |
| `fouls_technical_cat2` | integer | Technical fouls of category 2 (DataCore column; filled from 2026-27). Team total for the game. Absent on some rows. |
| `fouls_total` | integer | Fouls charged in total: personal + technical + unsportsmanlike + offensive (matches 2,142 of 2,143 played player-games; the exception is unexplained). Team total for the game. |
| `fouls_unsportsmanlike` | integer | Unsportsmanlike fouls committed. Team total for the game. |
| `free_throws_attempted` | integer | Free throws attempted. Team total for the game. |
| `free_throws_attempted_against` | integer | Opponent's figure in the same games: free throws attempted. Team total for the game. Absent on some rows. |
| `free_throws_made` | integer | Free throws made. Team total for the game. |
| `free_throws_made_against` | integer | Opponent's figure in the same games: free throws made. Team total for the game. Absent on some rows. |
| `free_throws_percentage` | numeric | Free-throw percentage on a 0-100 scale. Team total for the game. |
| `free_throws_percentage_against` | numeric | Opponent's figure in the same games: free-throw percentage on a 0-100 scale. Team total for the game. Absent on some rows. |
| `index_of_success` | integer | Index of success: efficiency + fouls drawn - personal fouls - technical fouls - blocks received (verified on all 2,143 played player-games). It differs from pir only in which fouls it subtracts. Team total for the game. |
| `lead_changes` | integer | Times the lead changed hands during the game (a season total sums the games). Team total for the game. |
| `minutes` | character | Playing time as an ISO 8601 duration PT{m}M{s}S with the minutes NOT wrapped at 60 (PT27M53S; a team's is the sum of its players', PT200M for a 40-minute game, PT250M with one overtime). Team total for the game. |
| `pir` | integer | Performance index rating: efficiency + fouls drawn - total fouls - blocks received (verified on all 2,143 played player-games). Team total for the game. |
| `points` | integer | Points scored. Team total for the game. |
| `points_against` | integer | Opponent's figure in the same games: points scored. Team total for the game. |
| `points_fast_break` | integer | Points scored on fast breaks. Team total for the game. |
| `points_fast_break_against` | integer | Opponent's figure in the same games: points scored on fast breaks. Team total for the game. Absent on some rows. |
| `points_fast_break_attempted` | integer | Fast-break field goals attempted. Team total for the game. |
| `points_fast_break_made` | integer | Fast-break field goals made. Team total for the game. |
| `points_four_attempted` | integer | 4-point shots attempted (DataCore column; EASL has no 4-point line; filled from 2026-27). Team total for the game. Absent on some rows. |
| `points_four_attempted_against` | integer | Opponent's figure in the same games: 4-point shots attempted (DataCore column; EASL has no 4-point line; filled from 2026-27). Team total for the game. Absent on some rows. |
| `points_four_made` | integer | 4-point shots made (see pointsFourAttempted). Team total for the game. Absent on some rows. |
| `points_four_made_against` | integer | Opponent's figure in the same games: 4-point shots made (see pointsFourAttempted). Team total for the game. Absent on some rows. |
| `points_from_bench` | integer | Points scored by players who did not start (a team figure). Team total for the game. |
| `points_from_bench_against` | integer | Opponent's figure in the same games: points scored by players who did not start (a team figure). Team total for the game. Absent on some rows. |
| `points_from_turnover` | integer | Points scored off opponent turnovers. Team total for the game. |
| `points_from_turnover_against` | integer | Opponent's figure in the same games: points scored off opponent turnovers. Team total for the game. Absent on some rows. |
| `points_in_the_paint` | integer | Points scored in the paint. Team total for the game. |
| `points_in_the_paint_against` | integer | Opponent's figure in the same games: points scored in the paint. Team total for the game. Absent on some rows. |
| `points_in_the_paint_attempted` | integer | Field goals attempted in the paint. Team total for the game. |
| `points_in_the_paint_made` | integer | Field goals made in the paint. Team total for the game. |
| `points_second_chance` | integer | Points scored on second-chance possessions (after an offensive rebound). Team total for the game. |
| `points_second_chance_against` | integer | Opponent's figure in the same games: points scored on second-chance possessions (after an offensive rebound). Team total for the game. Absent on some rows. |
| `points_second_chance_attempted` | integer | Second-chance field goals attempted. Team total for the game. |
| `points_second_chance_made` | integer | Second-chance field goals made. Team total for the game. |
| `points_three_attempted` | integer | 3-point field goals attempted. Team total for the game. |
| `points_three_attempted_against` | integer | Opponent's figure in the same games: 3-point field goals attempted. Team total for the game. Absent on some rows. |
| `points_three_made` | integer | 3-point field goals made. Team total for the game. |
| `points_three_made_against` | integer | Opponent's figure in the same games: 3-point field goals made. Team total for the game. Absent on some rows. |
| `points_three_percentage` | numeric | 3-point percentage on a 0-100 scale. Team total for the game. |
| `points_three_percentage_against` | numeric | Opponent's figure in the same games: 3-point percentage on a 0-100 scale. Team total for the game. Absent on some rows. |
| `points_two_attempted` | integer | 2-point field goals attempted. Team total for the game. |
| `points_two_attempted_against` | integer | Opponent's figure in the same games: 2-point field goals attempted. Team total for the game. Absent on some rows. |
| `points_two_made` | integer | 2-point field goals made. Team total for the game. |
| `points_two_made_against` | integer | Opponent's figure in the same games: 2-point field goals made. Team total for the game. Absent on some rows. |
| `points_two_percentage` | numeric | 2-point percentage on a 0-100 scale. Team total for the game. |
| `points_two_percentage_against` | numeric | Opponent's figure in the same games: 2-point percentage on a 0-100 scale. Team total for the game. Absent on some rows. |
| `power_play_points` | integer | Power-play points (an ice-hockey column; meaning in basketball unverified). Team total for the game. |
| `power_play_points_awarded` | integer | Power-play points awarded (an ice-hockey column; meaning in basketball unverified). Team total for the game. |
| `rebounds` | integer | Total rebounds: offensive + defensive. Team's totals include team rebounds. Team total for the game. |
| `rebounds_against` | integer | Opponent's figure in the same games: total rebounds: offensive + defensive. Team's totals include team rebounds. Team total for the game. Absent on some rows. |
| `rebounds_defensive` | integer | Defensive rebounds. Team total for the game. |
| `rebounds_defensive_against` | integer | Opponent's figure in the same games: defensive rebounds. Team total for the game. Absent on some rows. |
| `rebounds_offensive` | integer | Offensive rebounds. Team total for the game. |
| `rebounds_offensive_against` | integer | Opponent's figure in the same games: offensive rebounds. Team total for the game. Absent on some rows. |
| `rebounds_team_defensive` | integer | Defensive rebounds credited to the team as a whole (not to a player). Team total for the game. |
| `rebounds_team_defensive_against` | integer | Opponent's figure in the same games: defensive rebounds credited to the team as a whole (not to a player). Team total for the game. Absent on some rows. |
| `rebounds_team_offensive` | integer | Offensive rebounds credited to the team as a whole. Team total for the game. |
| `rebounds_team_offensive_against` | integer | Opponent's figure in the same games: offensive rebounds credited to the team as a whole. Team total for the game. Absent on some rows. |
| `rebounds_team_total` | integer | Team rebounds credited to the team as a whole (offensive + defensive). Team total for the game. |
| `rebounds_team_total_against` | integer | Opponent's figure in the same games: team rebounds credited to the team as a whole (offensive + defensive). Team total for the game. Absent on some rows. |
| `steals` | integer | Steals. Team total for the game. |
| `steals_against` | integer | Opponent's figure in the same games: steals. Team total for the game. Absent on some rows. |
| `time_in_lead` | character | Time the team led, as an ISO 8601 duration PT{m}M{s}S (unwrapped minutes). Team total for the game. |
| `timeouts_used` | integer | Timeouts taken by the team (present from 2025-26). Team total for the game. Absent on some rows. |
| `times_score_level` | integer | Times the score was tied during the game. Team total for the game. |
| `turnovers` | integer | Turnovers committed by players. Team total for the game. |
| `turnovers_against` | integer | Opponent's figure in the same games: turnovers committed by players. Team total for the game. Absent on some rows. |
| `turnovers_team` | integer | Team turnovers not charged to a player (shot-clock and similar violations). Team total for the game. |
| `turnovers_team_against` | integer | Opponent's figure in the same games: team turnovers not charged to a player (shot-clock and similar violations). Team total for the game. Absent on some rows. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#easl_team_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The server has nothing for the request (an empty list or leaders object, a team or player without games, a 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown fixture is the server's HTTP 500), an error envelope, a game that is not finished or whose box score or play-by-play is incomplete, a stalled sync on the active season, or a body for another game, season, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed id or a season that is not one of the server's END years (raised before the request that would use it). |

### Example {#easl_team_box-example}

```python
easl_team_box(fixture_id='10af8fd4-106e-11f1-960b-5798087c7aa3')
```

## easl_team_box_by_period

Team totals of a finished game by period, one row per team and period (overtimes after the fourth).

**Endpoint URL:** `GET https://easl.joshricha.com/api/v1/matches/{fixture_id}/teamstats/by/period`

**Valid URL:** [https://easl.joshricha.com/api/v1/matches/10af8fd4-106e-11f1-960b-5798087c7aa3/teamstats/by/period](https://easl.joshricha.com/api/v1/matches/10af8fd4-106e-11f1-960b-5798087c7aa3/teamstats/by/period)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `fixture_id` | `fixture_id` |  | `Y` |  | DataCore fixture (game) UUID v1, e.g. '10af8fd4-106e-11f1-960b-5798087c7aa3' (the 2025-26 EASL Championship): the 'id' of easl_schedule rows. Only finished (CONFIRMED) games are served; a malformed id is a ValueError. |

### Returns {#easl_team_box_by_period-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `fixture_id` | character | DataCore fixture (game) UUID v1 from the request. |
| `team_id` | character | DataCore team UUID v1 (the wire's team.teamId). |
| `team_name` | character | Team display name (English / Latin script, the wire's nameFullLocal). |
| `team_name_native` | character | Team name in native script (the wire's nameFullLatin; blank for most clubs). |
| `period` | integer | Period number: 1-4 for regulation, overtimes after (the wire's period_{n} key). |
| `period_label` | character | Q1-Q4 for periods 1-4; OT1, OT2 ... for overtimes (the server numbers the first overtime 11 in team totals; periods 5-10 are read as OT{n-4}, unverified). |
| `assists` | integer | Assists: passes that directly lead to a made field goal. Team total for the period. |
| `assists_against` | integer | Opponent's figure in the same games: assists: passes that directly lead to a made field goal. Team total for the period. Absent on some rows. |
| `assists_defensive` | integer | Defensive assists (DataCore column; meaning unverified). Team total for the period. |
| `assists_turnover_ratio` | numeric | Assists divided by turnovers (a ratio, not a percentage). Team total for the period. Absent on some rows. |
| `biggest_lead` | integer | Largest lead the team held during the game, in points. A season total sums the per-game maxima. Team total for the period. |
| `biggest_scoring_run` | integer | Longest unanswered scoring run of the team, in points. A season total sums the per-game maxima. Team total for the period. |
| `blocks` | integer | Shots blocked (made by this player or team). Team total for the period. |
| `blocks_received` | integer | The team's or player's own shots that were blocked by an opponent. Team total for the period. |
| `dunks` | integer | Dunks made. Team total for the period. |
| `efficiency` | integer | NBA-style efficiency: points + rebounds + assists + steals + blocks - missed field goals - missed free throws - turnovers (verified on all 2,143 played player-games). Team total for the period. |
| `efficiency_custom` | integer | Custom efficiency slot (DataCore column; meaning unverified). Team total for the period. |
| `field_goals_attempted` | integer | Field goals attempted (2- and 3-point shots; free throws excluded). Team total for the period. |
| `field_goals_attempted_against` | integer | Opponent's figure in the same games: field goals attempted (2- and 3-point shots; free throws excluded). Team total for the period. Absent on some rows. |
| `field_goals_effective_percentage` | numeric | Effective field-goal percentage on a 0-100 scale: (FGM + 0.5 x 3PM) / FGA x 100; can exceed 100 on small samples. Team total for the period. |
| `field_goals_made` | integer | Field goals made (2- and 3-point shots). Team total for the period. |
| `field_goals_made_against` | integer | Opponent's figure in the same games: field goals made (2- and 3-point shots). Team total for the period. Absent on some rows. |
| `field_goals_percentage` | numeric | Field-goal percentage on a 0-100 scale (made / attempted x 100). Team total for the period. |
| `field_goals_percentage_against` | numeric | Opponent's figure in the same games: field-goal percentage on a 0-100 scale (made / attempted x 100). Team total for the period. Absent on some rows. |
| `fouls_bench_technical` | integer | Technical fouls called on the bench (not a player). Team total for the period. |
| `fouls_coach_disqualifying` | integer | Disqualifying fouls on the coach. Team total for the period. |
| `fouls_coach_technical` | integer | Technical fouls on the coach. Team total for the period. |
| `fouls_disqualifying` | integer | Disqualifying fouls. Team total for the period. |
| `fouls_disruptive` | integer | Disruptive fouls (DataCore column; filled from 2026-27). Team total for the period. Absent on some rows. |
| `fouls_drawn` | integer | Fouls drawn (committed against this player or team by an opponent). Team total for the period. |
| `fouls_flagrant` | integer | Flagrant fouls (DataCore column; filled from 2026-27). Team total for the period. Absent on some rows. |
| `fouls_offensive` | integer | Offensive fouls committed. Counted separately from foulsPersonal. Team total for the period. |
| `fouls_personal` | integer | Personal fouls committed (offensive, technical and unsportsmanlike fouls are separate columns). Team total for the period. |
| `fouls_personal_against` | integer | Opponent's figure in the same games: personal fouls committed (offensive, technical and unsportsmanlike fouls are separate columns). Team total for the period. Absent on some rows. |
| `fouls_technical` | integer | Technical fouls committed by the player or team. Team total for the period. |
| `fouls_technical_against` | integer | Opponent's figure in the same games: technical fouls committed by the player or team. Team total for the period. Absent on some rows. |
| `fouls_technical_cat1` | integer | Technical fouls of category 1 (DataCore column; filled from 2026-27). Team total for the period. Absent on some rows. |
| `fouls_technical_cat2` | integer | Technical fouls of category 2 (DataCore column; filled from 2026-27). Team total for the period. Absent on some rows. |
| `fouls_total` | integer | Fouls charged in total: personal + technical + unsportsmanlike + offensive (matches 2,142 of 2,143 played player-games; the exception is unexplained). Team total for the period. |
| `fouls_unsportsmanlike` | integer | Unsportsmanlike fouls committed. Team total for the period. |
| `free_throws_attempted` | integer | Free throws attempted. Team total for the period. |
| `free_throws_attempted_against` | integer | Opponent's figure in the same games: free throws attempted. Team total for the period. Absent on some rows. |
| `free_throws_made` | integer | Free throws made. Team total for the period. |
| `free_throws_made_against` | integer | Opponent's figure in the same games: free throws made. Team total for the period. Absent on some rows. |
| `free_throws_percentage` | numeric | Free-throw percentage on a 0-100 scale. Team total for the period. Absent on some rows. |
| `free_throws_percentage_against` | numeric | Opponent's figure in the same games: free-throw percentage on a 0-100 scale. Team total for the period. Absent on some rows. |
| `index_of_success` | integer | Index of success: efficiency + fouls drawn - personal fouls - technical fouls - blocks received (verified on all 2,143 played player-games). It differs from pir only in which fouls it subtracts. Team total for the period. |
| `lead_changes` | integer | Times the lead changed hands during the game (a season total sums the games). Team total for the period. |
| `minutes` | character | Playing time as an ISO 8601 duration PT{m}M{s}S with the minutes NOT wrapped at 60 (PT27M53S; a team's is the sum of its players', PT200M for a 40-minute game, PT250M with one overtime). Team total for the period. |
| `pir` | integer | Performance index rating: efficiency + fouls drawn - total fouls - blocks received (verified on all 2,143 played player-games). Team total for the period. |
| `points` | integer | Points scored. Team total for the period. |
| `points_against` | integer | Opponent's figure in the same games: points scored. Team total for the period. |
| `points_fast_break` | integer | Points scored on fast breaks. Team total for the period. |
| `points_fast_break_against` | integer | Opponent's figure in the same games: points scored on fast breaks. Team total for the period. Absent on some rows. |
| `points_fast_break_attempted` | integer | Fast-break field goals attempted. Team total for the period. |
| `points_fast_break_made` | integer | Fast-break field goals made. Team total for the period. |
| `points_four_attempted` | integer | 4-point shots attempted (DataCore column; EASL has no 4-point line; filled from 2026-27). Team total for the period. Absent on some rows. |
| `points_four_attempted_against` | integer | Opponent's figure in the same games: 4-point shots attempted (DataCore column; EASL has no 4-point line; filled from 2026-27). Team total for the period. Absent on some rows. |
| `points_four_made` | integer | 4-point shots made (see pointsFourAttempted). Team total for the period. Absent on some rows. |
| `points_four_made_against` | integer | Opponent's figure in the same games: 4-point shots made (see pointsFourAttempted). Team total for the period. Absent on some rows. |
| `points_from_bench` | integer | Points scored by players who did not start (a team figure). Team total for the period. |
| `points_from_bench_against` | integer | Opponent's figure in the same games: points scored by players who did not start (a team figure). Team total for the period. Absent on some rows. |
| `points_from_turnover` | integer | Points scored off opponent turnovers. Team total for the period. |
| `points_from_turnover_against` | integer | Opponent's figure in the same games: points scored off opponent turnovers. Team total for the period. Absent on some rows. |
| `points_in_the_paint` | integer | Points scored in the paint. Team total for the period. |
| `points_in_the_paint_against` | integer | Opponent's figure in the same games: points scored in the paint. Team total for the period. Absent on some rows. |
| `points_in_the_paint_attempted` | integer | Field goals attempted in the paint. Team total for the period. |
| `points_in_the_paint_made` | integer | Field goals made in the paint. Team total for the period. |
| `points_second_chance` | integer | Points scored on second-chance possessions (after an offensive rebound). Team total for the period. |
| `points_second_chance_against` | integer | Opponent's figure in the same games: points scored on second-chance possessions (after an offensive rebound). Team total for the period. Absent on some rows. |
| `points_second_chance_attempted` | integer | Second-chance field goals attempted. Team total for the period. |
| `points_second_chance_made` | integer | Second-chance field goals made. Team total for the period. |
| `points_three_attempted` | integer | 3-point field goals attempted. Team total for the period. |
| `points_three_attempted_against` | integer | Opponent's figure in the same games: 3-point field goals attempted. Team total for the period. Absent on some rows. |
| `points_three_made` | integer | 3-point field goals made. Team total for the period. |
| `points_three_made_against` | integer | Opponent's figure in the same games: 3-point field goals made. Team total for the period. Absent on some rows. |
| `points_three_percentage` | numeric | 3-point percentage on a 0-100 scale. Team total for the period. |
| `points_three_percentage_against` | numeric | Opponent's figure in the same games: 3-point percentage on a 0-100 scale. Team total for the period. Absent on some rows. |
| `points_two_attempted` | integer | 2-point field goals attempted. Team total for the period. |
| `points_two_attempted_against` | integer | Opponent's figure in the same games: 2-point field goals attempted. Team total for the period. Absent on some rows. |
| `points_two_made` | integer | 2-point field goals made. Team total for the period. |
| `points_two_made_against` | integer | Opponent's figure in the same games: 2-point field goals made. Team total for the period. Absent on some rows. |
| `points_two_percentage` | numeric | 2-point percentage on a 0-100 scale. Team total for the period. |
| `points_two_percentage_against` | numeric | Opponent's figure in the same games: 2-point percentage on a 0-100 scale. Team total for the period. Absent on some rows. |
| `power_play_points` | integer | Power-play points (an ice-hockey column; meaning in basketball unverified). Team total for the period. |
| `power_play_points_awarded` | integer | Power-play points awarded (an ice-hockey column; meaning in basketball unverified). Team total for the period. |
| `rebounds` | integer | Total rebounds: offensive + defensive. Team's totals include team rebounds. Team total for the period. |
| `rebounds_against` | integer | Opponent's figure in the same games: total rebounds: offensive + defensive. Team's totals include team rebounds. Team total for the period. Absent on some rows. |
| `rebounds_defensive` | integer | Defensive rebounds. Team total for the period. |
| `rebounds_defensive_against` | integer | Opponent's figure in the same games: defensive rebounds. Team total for the period. Absent on some rows. |
| `rebounds_offensive` | integer | Offensive rebounds. Team total for the period. |
| `rebounds_offensive_against` | integer | Opponent's figure in the same games: offensive rebounds. Team total for the period. Absent on some rows. |
| `rebounds_team_defensive` | integer | Defensive rebounds credited to the team as a whole (not to a player). Team total for the period. |
| `rebounds_team_defensive_against` | integer | Opponent's figure in the same games: defensive rebounds credited to the team as a whole (not to a player). Team total for the period. Absent on some rows. |
| `rebounds_team_offensive` | integer | Offensive rebounds credited to the team as a whole. Team total for the period. |
| `rebounds_team_offensive_against` | integer | Opponent's figure in the same games: offensive rebounds credited to the team as a whole. Team total for the period. Absent on some rows. |
| `rebounds_team_total` | integer | Team rebounds credited to the team as a whole (offensive + defensive). Team total for the period. |
| `rebounds_team_total_against` | integer | Opponent's figure in the same games: team rebounds credited to the team as a whole (offensive + defensive). Team total for the period. Absent on some rows. |
| `steals` | integer | Steals. Team total for the period. |
| `steals_against` | integer | Opponent's figure in the same games: steals. Team total for the period. Absent on some rows. |
| `time_in_lead` | character | Time the team led, as an ISO 8601 duration PT{m}M{s}S (unwrapped minutes). Team total for the period. |
| `timeouts_used` | integer | Timeouts taken by the team (present from 2025-26). Team total for the period. Absent on some rows. |
| `times_score_level` | integer | Times the score was tied during the game. Team total for the period. |
| `turnovers` | integer | Turnovers committed by players. Team total for the period. |
| `turnovers_against` | integer | Opponent's figure in the same games: turnovers committed by players. Team total for the period. Absent on some rows. |
| `turnovers_team` | integer | Team turnovers not charged to a player (shot-clock and similar violations). Team total for the period. |
| `turnovers_team_against` | integer | Opponent's figure in the same games: team turnovers not charged to a player (shot-clock and similar violations). Team total for the period. Absent on some rows. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#easl_team_box_by_period-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The server has nothing for the request (an empty list or leaders object, a team or player without games, a 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown fixture is the server's HTTP 500), an error envelope, a game that is not finished or whose box score or play-by-play is incomplete, a stalled sync on the active season, or a body for another game, season, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed id or a season that is not one of the server's END years (raised before the request that would use it). |

### Example {#easl_team_box_by_period-example}

```python
easl_team_box_by_period(fixture_id='10af8fd4-106e-11f1-960b-5798087c7aa3')
```

## easl_teams

The 12 ACTIVE EASL teams (historical teams resolve only through easl_team).

**Endpoint URL:** `GET https://easl.joshricha.com/api/v1/teams`

**Valid URL:** [https://easl.joshricha.com/api/v1/teams](https://easl.joshricha.com/api/v1/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#easl_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_row_id` | integer | The sync server's own integer row id of the team (not a DataCore id; inactive teams interleave, so ids of /teams rows have gaps). |
| `team_id` | character | teamId: DataCore team (entity) UUID v1 (key space: EASL teams, the same id as `entityId`; joins /teams, /standings team.id, fixture competitors, box and play-by-play teams). Not a player id. |
| `name` | character | Display name. Despite the field name this is the English / Latin-script name used on the league site. |
| `name_native` | character | Alternative name: the native-script name of some Japanese clubs (for example 宇都宮ブレックス for Utsunomiya Brex), blank for most clubs; for players it usually equals nameFullLocal. Despite the field name it is not always Latin script. Null on some rows. |
| `code_latin` | character | Three-letter club code (for example BRE); null or empty for some clubs. |
| `code_local` | character | Alternative three-letter club code (for example TPP); null for most clubs. A club may have only one of codeLatin / codeLocal. |
| `standard` | character | Competition standard: ELITE. |
| `age_group` | character | Age group: SENIOR; null on some inactive teams. |
| `gender` | character | Competition gender of the team: MALE (the EASL is a men's competition). |
| `status` | character | ACTIVE for teams in the current season (the only ones /teams lists), INACTIVE for historical teams (only via /teams/{teamId}). |
| `primary_color` | character | Club colour as a 6-digit hex string without '#'; '000000' is a placeholder on several clubs. |
| `secondary_color` | character | Club colour as a 6-digit hex string without '#'; '000000' is a placeholder on several clubs. |
| `tertiary_color` | character | Club colour as a 6-digit hex string without '#'; '000000' is a placeholder on several clubs. |
| `logo` | character | Club logo URL on the Sportradar DataCore asset CDN (images.dc.connect.sportradar.com/b1e0z/...). |
| `website` | character | Club website URL; most clubs have none. Null on some rows. |
| `facebook_username` | character | Club Facebook handle; most clubs have none. Null on some rows. |
| `twitter_username` | character | Club X / Twitter handle; most clubs have none. Null on some rows. |
| `instagram_username` | character | Club Instagram handle; some clubs have none. Null on some rows. |
| `wikipedia_url` | character | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (String). |
| `organization_id` | character | DataCore organisation id: b1e0z (EASL). |
| `created_at` | character | When the sync server inserted the row, 'YYYY-MM-DD HH:MM:SS' (a different format from updatedAt; zone unstated, UTC assumed). |
| `updated_at` | character | When the sync server last synced the row, ISO 8601 with milliseconds and Z (UTC). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#easl_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The server has nothing for the request (an empty list or leaders object, a team or player without games, a 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown fixture is the server's HTTP 500), an error envelope, a game that is not finished or whose box score or play-by-play is incomplete, a stalled sync on the active season, or a body for another game, season, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed id or a season that is not one of the server's END years (raised before the request that would use it). |

### Example {#easl_teams-example}

```python
easl_teams()
```

## easl_team

One team's directory entry; resolves the historical teams easl_teams does not list.

**Endpoint URL:** `GET https://easl.joshricha.com/api/v1/teams/{team_id}`

**Valid URL:** [https://easl.joshricha.com/api/v1/teams/865489e8-b0f5-11ed-8b3f-c199a9dea184](https://easl.joshricha.com/api/v1/teams/865489e8-b0f5-11ed-8b3f-c199a9dea184)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | DataCore team (entity) UUID v1, e.g. '865489e8-b0f5-11ed-8b3f-c199a9dea184' (Utsunomiya Brex): the team_id of easl_teams, easl_standings and the box scores. easl_team resolves the 12 historical teams easl_teams does not list. |

### Returns {#easl_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_row_id` | integer | The sync server's own integer row id of the team (not a DataCore id; inactive teams interleave, so ids of /teams rows have gaps). |
| `team_id` | character | teamId: DataCore team (entity) UUID v1 (key space: EASL teams, the same id as `entityId`; joins /teams, /standings team.id, fixture competitors, box and play-by-play teams). Not a player id. |
| `name` | character | Display name. Despite the field name this is the English / Latin-script name used on the league site. |
| `name_native` | character | Alternative name: the native-script name of some Japanese clubs (for example 宇都宮ブレックス for Utsunomiya Brex), blank for most clubs; for players it usually equals nameFullLocal. Despite the field name it is not always Latin script. |
| `code_latin` | character | Three-letter club code (for example BRE); null or empty for some clubs. |
| `code_local` | character | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (String). |
| `standard` | character | Competition standard: ELITE. |
| `age_group` | character | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (String). |
| `gender` | character | Competition gender of the team: MALE (the EASL is a men's competition). |
| `status` | character | ACTIVE for teams in the current season (the only ones /teams lists), INACTIVE for historical teams (only via /teams/{teamId}). |
| `primary_color` | character | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (String). |
| `secondary_color` | character | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (String). |
| `tertiary_color` | character | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (String). |
| `logo` | character | Club logo URL on the Sportradar DataCore asset CDN (images.dc.connect.sportradar.com/b1e0z/...). |
| `website` | character | Club website URL; most clubs have none. |
| `facebook_username` | character | Club Facebook handle; most clubs have none. |
| `twitter_username` | character | Club X / Twitter handle; most clubs have none. |
| `instagram_username` | character | Club Instagram handle; some clubs have none. |
| `wikipedia_url` | character | Club Wikipedia URL (DataCore column); empty for every ACTIVE team. |
| `organization_id` | character | DataCore organisation id: b1e0z (EASL). |
| `created_at` | character | When the sync server inserted the row, 'YYYY-MM-DD HH:MM:SS' (a different format from updatedAt; zone unstated, UTC assumed). |
| `updated_at` | character | When the sync server last synced the row, ISO 8601 with milliseconds and Z (UTC). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#easl_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The server has nothing for the request (an empty list or leaders object, a team or player without games, a 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown fixture is the server's HTTP 500), an error envelope, a game that is not finished or whose box score or play-by-play is incomplete, a stalled sync on the active season, or a body for another game, season, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed id or a season that is not one of the server's END years (raised before the request that would use it). |

### Example {#easl_team-example}

```python
easl_team(team_id='865489e8-b0f5-11ed-8b3f-c199a9dea184')
```

## easl_team_schedule

A team's fixtures in one season, shaped like easl_schedule.

**Endpoint URL:** `GET https://easl.joshricha.com/api/v1/teams/{team_id}/matches`

**Valid URL:** [https://easl.joshricha.com/api/v1/teams/865489e8-b0f5-11ed-8b3f-c199a9dea184/matches?season=2026](https://easl.joshricha.com/api/v1/teams/865489e8-b0f5-11ed-8b3f-c199a9dea184/matches?season=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | DataCore team (entity) UUID v1, e.g. '865489e8-b0f5-11ed-8b3f-c199a9dea184' (Utsunomiya Brex): the team_id of easl_teams, easl_standings and the box scores. easl_team resolves the 12 historical teams easl_teams does not list. |
| `season` | `season` |  |  | `Y` | Season END year (2026 = EASL 25-26; the four editions are 2024 to 2027). easl_runtime reads easl_seasons and maps it to the DataCore seasonId. Default: the ACTIVE season. |

### Returns {#easl_team_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = EASL 25-26): stamped by easl_runtime from the request. |
| `season_id` | character | Season UUID: the rows carry no seasonId (the season sits in the first half of stage_id), so this is the request's. |
| `fixture_id` | character | fixtureId: DataCore fixture (game) UUID v1 (key space: EASL games; the {fixtureId} of every /matches route). |
| `fixture_number` | integer | Fixture number YYSSNNN: two-digit start year, two-digit end year, three-digit sequence (2627001 = first game of 2026-27, 2526003 = third of 2025-26). Null on every 2023-24 fixture. |
| `fixture_type` | character | REGULAR (group stage), PLAYOFF (quarter- and semifinals) or FINAL (championship and third-place games). Missing from the DRAFT placeholders. |
| `status` | character | CONFIRMED = finished with a final box score; SCHEDULED = future; DRAFT = a placeholder with no teams or time. |
| `fixture_name` | character | Fixture name for special games ('EASL Championship', 'Semifinals 2', 'EASL Tip-Off 2026 Cebu'); null for ordinary group games. |
| `start_time_utc` | character | Scheduled tip-off in UTC, 'YYYY-MM-DDTHH:MM:SS' with no zone marker. The /matches list holds null here on DRAFT fixtures; the single-fixture sample is a finished game. |
| `start_time_local` | character | Scheduled tip-off in the venue's local time, 'YYYY-MM-DDTHH:MM:SS' with no zone (19:10 local = 10:10 UTC in Tokyo). DRAFT fixtures carry a date at 00:00:00. |
| `attendance` | integer | Announced attendance of the game; null when not recorded. |
| `live_data_available` | logical | Whether live stats were collected for the game (present on a minority of fixtures; meaning unverified). Absent on some rows. |
| `live_video_available` | logical | Whether live video was available (false on all 146 fixtures of the four seasons). |
| `pool_code` | character | Pool (group) id: a 30-character hex string, equal to pool.id; null on playoff and DRAFT fixtures. |
| `pool_id` | character | Pool id: a 30-character hex string; null for playoff games ('Unknown Pool'). |
| `pool_name` | character | Pool name: 'Group A', 'Group B', 'Group C' or the placeholder 'Unknown Pool'. |
| `pool_abb` | character | Pool abbreviation (A, B, C). Playoff and DRAFT fixtures of the /matches list carry the placeholder pool, whose abb is empty or 'N/A'. |
| `stage_id` | character | Stage reference on /teams/{teamId}/matches rows only: '<seasonId UUID>:<stage code>' (the season and stage the game belongs to; replaces the nameLocal / stageCode / stageOrder trio of /matches rows). |
| `stage_code` | character | Stage id: a 30-character hex string. Absent on some rows. |
| `stage_name` | character | Stage name: 'Group Stage', 'Final Four' or 'EASL Playoffs'. Absent on some rows. |
| `stage_order` | integer | Ordinal of the stage within the season (1 = Group Stage, 2 = EASL Playoffs). Absent on some rows. |
| `venue_name` | character | Arena name. Absent on some rows. |
| `venue_name_latin` | character | Venue name in Latin script; null on most rows. |
| `venue_abbreviation_latin` | character | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (String). |
| `venue_abbreviation_local` | character | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (String). |
| `venue_alternate_name_latin` | character | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (String). |
| `venue_country_code` | character | ISO 3166-1 alpha-3 country code of the arena: JPN, TWN, PHL, KOR, MAC, HKG, MNG, CHN. The /matches list holds null here on DRAFT fixtures (empty venue). |
| `venue_city` | character | City of the arena. Absent on some rows. |
| `venue_state` | character | State or province of the arena (Philippine games, for example Cebu). Absent on some rows. |
| `home_team_id` | character | Home team: teamId: DataCore team (entity) UUID v1 (key space: EASL teams, the same id as `entityId`; joins /teams, /standings team.id, fixture competitors, box and play-by-play teams). Not a player id. |
| `home_name` | character | Home team: display name. Despite the field name this is the English / Latin-script name used on the league site. |
| `home_name_native` | character | Home team: alternative name: the native-script name of some Japanese clubs (for example 宇都宮ブレックス for Utsunomiya Brex), blank for most clubs; for players it usually equals nameFullLocal. Despite the field name it is not always Latin script. Null on some rows. |
| `home_code_latin` | character | Home team: three-letter club code (for example BRE); null or empty for some clubs. |
| `home_code_local` | character | Home team: alternative three-letter club code (for example TPP); null for most clubs. A club may have only one of codeLatin / codeLocal. |
| `home_score` | integer | Home team final points (the wire sends a numeric string; cast to Int64); null before the game is played. |
| `home_result_place` | integer | Home team: result place: 1 = winner, 2 = loser. The /matches list holds null here for fixtures not yet played; /matches/{fixtureId} is sampled for finished games only. |
| `home_draw` | logical | Home team: true when the game has no winner YET or ever: true on every unplayed competitor, false on every finished one (a tie has never happened). |
| `away_team_id` | character | Away team: teamId: DataCore team (entity) UUID v1 (key space: EASL teams, the same id as `entityId`; joins /teams, /standings team.id, fixture competitors, box and play-by-play teams). Not a player id. |
| `away_name` | character | Away team: display name. Despite the field name this is the English / Latin-script name used on the league site. |
| `away_name_native` | character | Away team: alternative name: the native-script name of some Japanese clubs (for example 宇都宮ブレックス for Utsunomiya Brex), blank for most clubs; for players it usually equals nameFullLocal. Despite the field name it is not always Latin script. Null on some rows. |
| `away_code_latin` | character | Away team: three-letter club code (for example BRE); null or empty for some clubs. |
| `away_code_local` | character | Away team: alternative three-letter club code (for example TPP); null for most clubs. A club may have only one of codeLatin / codeLocal. |
| `away_score` | integer | Away team final points (the wire sends a numeric string; cast to Int64); null before the game is played. |
| `away_result_place` | integer | Away team: result place: 1 = winner, 2 = loser. The /matches list holds null here for fixtures not yet played; /matches/{fixtureId} is sampled for finished games only. |
| `away_draw` | logical | Away team: true when the game has no winner YET or ever: true on every unplayed competitor, false on every finished one (a tie has never happened). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#easl_team_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The server has nothing for the request (an empty list or leaders object, a team or player without games, a 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown fixture is the server's HTTP 500), an error envelope, a game that is not finished or whose box score or play-by-play is incomplete, a stalled sync on the active season, or a body for another game, season, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed id or a season that is not one of the server's END years (raised before the request that would use it). |

### Example {#easl_team_schedule-example}

```python
easl_team_schedule(season=2026, team_id='865489e8-b0f5-11ed-8b3f-c199a9dea184')
```
