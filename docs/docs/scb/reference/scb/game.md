---
title: "SCB — SCB basketball API (BBL and DBBL, Germany) — Game"
sidebar_label: "Game"
sidebar_position: 1
description: "SCB — SCB basketball API (BBL and DBBL, Germany) — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# SCB — SCB basketball API (BBL and DBBL, Germany) — Game

## scb_games_recent

Recent, live and upcoming games, one row per game with its past / present / future bucket.

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/games/recent`

**Valid URL:** [https://api.basketball-bundesliga.de/games/recent](https://api.basketball-bundesliga.de/games/recent)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |
| `slotSize` | `slot_size` |  |  | `Y` | Games per bucket (past, present, future); the API's default is 9. |

### Returns {#scb_games_recent-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `season` | integer | Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one. |
| `season_id` | integer | Season, START year (2026 = 2026-27). |
| `game_id` | character | Game id, a string; unique within a league only (BBL and DBBL overlap), and not chronological. |
| `competition` | character | Competition: BBL, BBL_CUP (Pokal), ALLSTAR, CHAMPIONS_CUP on the BBL host; DBBL, DBBL_CUP on the DBBL host. |
| `stage` | character | Stage: MAIN_ROUND (Hauptrunde, regular season), PLAYINS, FIRST_ROUND, SECOND_ROUND, ROUND_OF_16 (Achtelfinale), ROUND_OF_8 (Viertelfinale), SEMI_FINALS, FINALS, THIRD_PLACE_GAME, QUALIFICATION, GROUP_STAGE. |
| `status` | character | Game status: PRE (scheduled), LIVE, POST (ended, not yet official), OFFICIAL (final). |
| `bucket` | character | Which list of `/games/recent` the game came from: `past` (finished), `present` (today or live) or `future` (scheduled). |
| `game_progress` | character | Live progress code: E (ended), empty or null before the game. Meaning inferred. |
| `scheduled_time` | character | Scheduled tip-off, ISO 8601 UTC. |
| `match_day` | integer | Round number within the stage (Spieltag). |
| `is_tbd` | logical | Date or time not yet fixed. |
| `is_hidden` | logical | True when the API hides the game (0/1 on /games, a boolean on the header). |
| `home_team_id` | character | Club id; equal to `seasonTeamId` in every sample. Joins to /teams/{teamId}. |
| `home_season_team_id` | character | Season-team id; equal to the club `teamId` in every sample (BBL 2005-2026, DBBL 2024-2026). |
| `home_team_name` | character | Team name as listed that season (sponsor names included, e.g. 'EWE Baskets Oldenburg'). |
| `home_tlc` | character | Three-letter club code. |
| `home_is_foreign` | logical | The team is not in this league (cup opponents from the ProA / ProB). |
| `home_rank` | integer | The team's current table position. |
| `home_streak` | character | Current streak: W or L; empty when none. |
| `home_streak_count` | integer | Length of the current streak. |
| `home_last10_victories` | integer | Wins in the last 10 games. |
| `home_last10_losses` | integer | Losses in the last 10 games. |
| `home_total_games` | integer | Games the home team has played so far (the API sends it as a string). |
| `guest_team_id` | character | Club id; equal to `seasonTeamId` in every sample. Joins to /teams/{teamId}. |
| `guest_season_team_id` | character | Season-team id; equal to the club `teamId` in every sample (BBL 2005-2026, DBBL 2024-2026). |
| `guest_team_name` | character | Team name as listed that season (sponsor names included, e.g. 'EWE Baskets Oldenburg'). |
| `guest_tlc` | character | Three-letter club code. |
| `guest_is_foreign` | logical | The team is not in this league (cup opponents from the ProA / ProB). |
| `guest_rank` | integer | The team's current table position. |
| `guest_streak` | character | Current streak: W or L; empty when none. |
| `guest_streak_count` | integer | Length of the current streak. |
| `guest_last10_victories` | integer | Wins in the last 10 games. |
| `guest_last10_losses` | integer | Losses in the last 10 games. |
| `guest_total_games` | integer | Games the guest team has played so far (the API sends it as a string). |
| `home_score` | integer | Home team final score. |
| `guest_score` | integer | Guest team final score. |
| `latest_period` | integer | Number of the last period played (4 = Q4, 5 = OT1, ...); the API sends `Q4` on /games and 4 on /games/recent. Null before the game. |
| `spectators` | integer | Attendance; 0 when not recorded. |
| `home_q1_score` | integer | Home team points in quarter 1; 0 when quarter scores were not recorded. |
| `home_q2_score` | integer | Home team points in quarter 2; 0 when quarter scores were not recorded. |
| `home_q3_score` | integer | Home team points in quarter 3; 0 when quarter scores were not recorded. |
| `home_q4_score` | integer | Home team points in quarter 4; 0 when quarter scores were not recorded. |
| `home_ot1_score` | integer | Home team points in overtime 1; 0 when not played (null from OT5 on in old seasons). |
| `home_ot2_score` | integer | Home team points in overtime 2; 0 when not played (null from OT5 on in old seasons). |
| `home_ot3_score` | integer | Home team points in overtime 3; 0 when not played (null from OT5 on in old seasons). |
| `home_ot4_score` | integer | Home team points in overtime 4; 0 when not played (null from OT5 on in old seasons). |
| `home_ot5_score` | integer | Home team points in overtime 5; 0 when not played (null from OT5 on in old seasons). |
| `home_ot6_score` | integer | Home team points in overtime 6; 0 when not played (null from OT5 on in old seasons). |
| `home_ot7_score` | integer | Home team points in overtime 7; 0 when not played (null from OT5 on in old seasons). |
| `home_ot8_score` | integer | Home team points in overtime 8; 0 when not played (null from OT5 on in old seasons). |
| `home_ot9_score` | integer | Home team points in overtime 9; 0 when not played (null from OT5 on in old seasons). |
| `guest_q1_score` | integer | Guest team points in quarter 1; 0 when quarter scores were not recorded. |
| `guest_q2_score` | integer | Guest team points in quarter 2; 0 when quarter scores were not recorded. |
| `guest_q3_score` | integer | Guest team points in quarter 3; 0 when quarter scores were not recorded. |
| `guest_q4_score` | integer | Guest team points in quarter 4; 0 when quarter scores were not recorded. |
| `guest_ot1_score` | integer | Guest team points in overtime 1; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot2_score` | integer | Guest team points in overtime 2; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot3_score` | integer | Guest team points in overtime 3; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot4_score` | integer | Guest team points in overtime 4; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot5_score` | integer | Guest team points in overtime 5; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot6_score` | integer | Guest team points in overtime 6; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot7_score` | integer | Guest team points in overtime 7; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot8_score` | integer | Guest team points in overtime 8; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot9_score` | integer | Guest team points in overtime 9; 0 when not played (null from OT5 on in old seasons). |
| `referee1_id` | character | Official id; joins to /referees `refId` (= its `id`). /referees lists only the current officials (44), so older games' officials can be missing there: all 40 of 2026-27's matched, 6 of 10 of 2008-09's. |
| `referee1_first_name` | character | Official's given name. |
| `referee1_last_name` | character | Official's family name. |
| `referee2_id` | character | Official id; joins to /referees `refId` (= its `id`). /referees lists only the current officials (44), so older games' officials can be missing there: all 40 of 2026-27's matched, 6 of 10 of 2008-09's. |
| `referee2_first_name` | character | Official's given name. |
| `referee2_last_name` | character | Official's family name. |
| `referee3_id` | character | Official id; joins to /referees `refId` (= its `id`). /referees lists only the current officials (44), so older games' officials can be missing there: all 40 of 2026-27's matched, 6 of 10 of 2008-09's. |
| `referee3_first_name` | character | Official's given name. |
| `referee3_last_name` | character | Official's family name. |
| `commissioner_id` | character | Official id; joins to /referees `refId` (= its `id`). /referees lists only the current officials (44), so older games' officials can be missing there: all 40 of 2026-27's matched, 6 of 10 of 2008-09's. |
| `commissioner_first_name` | character | Official's given name. |
| `commissioner_last_name` | character | Official's family name. |
| `venue_id` | character | Arena id, stable across seasons (Uber Arena = 2 in both 2025-26 and 2026-27); a different key space from /teams `seasonVenues.id`, and no captured route is keyed by it. A string in /games and /games/{gameId}, an integer in /games/recent. |
| `venue_name` | character | Name of the arena the game is played in; `venue_id` is its stable arena id. |
| `venue_address` | character | Postal address, German. |
| `venue_capacity` | integer | Arena capacity, spectators. |
| `venue_is_main` | logical | The club's main arena. |
| `tv_channel` | character | TV channel(s) showing the game, comma-joined (a list on some routes, an empty string when none). |
| `tv_type` | character | TV coverage type; empty or null in every sample. |
| `is_free_tv` | logical | Shown on free-to-air TV. |
| `live_stream_url` | character | Live-stream link (DBBL games link YouTube); empty when none. |
| `top_score` | integer | Points of the game's top scorer; null before the game. |
| `top_scorer_season_player_id` | character | Season-player id. BBL: a string equal to `id`. DBBL: an INTEGER that matches /seasons/{seasonId} `seasonPlayerId` (5 of 5 rows checked, 2026-27). Joins to /season-players/{seasonPlayerId}. |
| `top_scorer_real_player_id` | character | Global person id: the same across clubs and seasons. Joins to `realPlayerId` everywhere. |
| `top_scorer_first_name` | character | Player's given name. |
| `top_scorer_last_name` | character | Player's family name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#scb_games_recent-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a game not yet started, an empty table, a playoff bracket not yet drawn); a game before 2002-03 is zero rows. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#scb_games_recent-example}

```python
scb_games_recent()
```

_Last validated n/a._

## scb_game_players

Box score of a finished game: one row per player line of both teams (2002-03 on).

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/games/{game_id}/stats`

**Valid URL:** [https://api.basketball-bundesliga.de/games/2006920/stats](https://api.basketball-bundesliga.de/games/2006920/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Game id (`game_id` in `scb_games`), e.g. 2006920 (BBL) or 10130 (DBBL). One sequence per league. |
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |

### Returns {#scb_game_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `game_id` | character | Game id, a string. Joins to /games/{gameId}. |
| `season` | integer | Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one. |
| `side` | character | `home` or `guest` (the away side), for the team the row belongs to. |
| `team_id` | character | Club id; equal to `seasonTeamId` in every sample. Joins to /teams/{teamId}. |
| `season_player_id` | character | Season-player id: one player's engagement with one club (kept across seasons while he stays, new after a move; see README). Joins to box-score `seasonPlayer.id`, action `seasonPlayerId` and /season-players/{seasonPlayerId}. |
| `real_player_id` | integer | Global person id: the same across clubs and seasons. Joins to `realPlayerId` everywhere. |
| `first_name` | character | Player's given name. |
| `last_name` | character | Player's family name. |
| `shirt_number` | character | Jersey number. A number or a numeric string across rows. |
| `position` | character | Position: POINT_GUARD, SHOOTING_GUARD, SMALL_FORWARD, POWER_FORWARD, CENTER. |
| `season_team_id` | character | Season-team id; equal to the club `teamId` in every sample (BBL 2005-2026, DBBL 2024-2026). |
| `team_name` | character | Team name as listed that season (sponsor names included, e.g. 'EWE Baskets Oldenburg'). |
| `team_tlc` | character | Three-letter club code. |
| `season_id` | integer | Season, START year (2026 = 2026-27). |
| `seconds_played` | integer | The player's playing time in SECONDS in the game. |
| `points` | integer | The player's points in the game. |
| `two_point_shots_made` | integer | The player's 2-point field goals made in the game. |
| `two_point_shots_attempted` | integer | The player's 2-point field goals attempted in the game. |
| `two_point_shot_success_percent` | numeric | The player's 2-point field-goal percentage in the game, 0-100 (one decimal); 0 when no attempts. |
| `three_point_shots_made` | integer | The player's 3-point field goals made in the game. |
| `three_point_shots_attempted` | integer | The player's 3-point field goals attempted in the game. |
| `three_point_shot_success_percent` | numeric | The player's 3-point field-goal percentage in the game, 0-100 (one decimal); 0 when no attempts. |
| `field_goals_made` | integer | The player's field goals made in the game. |
| `field_goals_attempted` | integer | The player's field goals attempted in the game. |
| `field_goals_success_percent` | numeric | The player's field-goal percentage in the game, 0-100 (one decimal); 0 when no attempts. |
| `free_throws_made` | integer | The player's free throws made in the game. |
| `free_throws_attempted` | integer | The player's free throws attempted in the game. |
| `free_throws_success_percent` | numeric | The player's free-throw percentage in the game, 0-100 (one decimal); 0 when no attempts. |
| `offensive_rebounds` | integer | The player's offensive rebounds in the game. |
| `defensive_rebounds` | integer | The player's defensive rebounds in the game. |
| `total_rebounds` | integer | The player's rebounds (offensive + defensive) in the game. |
| `assists` | integer | The player's assists in the game. |
| `steals` | integer | The player's steals in the game. |
| `turnovers` | integer | The player's turnovers in the game. |
| `blocks` | integer | The player's blocked shots in the game. |
| `fouls_committed` | integer | The player's fouls committed in the game. |
| `efficiency` | integer | The player's efficiency (EFF = PTS + REB + AST + STL + BLK - missed FG - missed FT - TO; checked on a 2026 box score) in the game. |
| `plus_minus` | integer | The player's plus-minus (point margin while on court); null when the API sends an empty string (2005-06). |
| `is_starting_five` | logical | Started the game. |
| `double_double` | integer | The player's double-doubles in the game. |
| `triple_double` | integer | The player's triple-doubles in the game. |
| `fouls_received` | integer | The player's fouls drawn in the game. |
| `opponent_team_name` | character | Opponent's name. |
| `field_goals_recomputed` | logical | True when the API's field-goal totals disagreed with its 2-point plus 3-point parts (2005-06 box scores) and were recomputed from the parts. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#scb_game_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a game not yet started, an empty table, a playoff bracket not yet drawn); a game before 2002-03 is zero rows. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#scb_game_players-example}

```python
scb_game_players(game_id='2006920')
```

_Last validated n/a._

## scb_game_teams

Box score of a finished game: one row per team, with the head coach and the team line.

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/games/{game_id}/stats`

**Valid URL:** [https://api.basketball-bundesliga.de/games/2006920/stats](https://api.basketball-bundesliga.de/games/2006920/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Game id (`game_id` in `scb_games`), e.g. 2006920 (BBL) or 10130 (DBBL). One sequence per league. |
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |

### Returns {#scb_game_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `game_id` | character | Game id, a string. Joins to /games/{gameId}. |
| `season` | integer | Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one. |
| `side` | character | `home` or `guest` (the away side), for the team the row belongs to. |
| `team_id` | character | Club id; equal to `seasonTeamId` in every sample. Joins to /teams/{teamId}. |
| `head_coach_name` | character | Name of the team's head coach. |
| `status` | character | Game status: PRE (scheduled), LIVE, POST (ended, not yet official) or OFFICIAL (final). |
| `scheduled_time` | character | Scheduled tip-off, ISO 8601 UTC. |
| `season_team_id` | character | Season-team id; equal to the club `teamId` in every sample (BBL 2005-2026, DBBL 2024-2026). |
| `season_id` | integer | Season, START year (2026 = 2026-27). |
| `team_name` | character | Team name as listed that season (sponsor names included, e.g. 'EWE Baskets Oldenburg'). |
| `team_tlc` | character | Three-letter club code. |
| `competition` | character | Competition: BBL, BBL_CUP (Pokal), ALLSTAR, CHAMPIONS_CUP on the BBL host; DBBL, DBBL_CUP on the DBBL host. |
| `stage` | character | Stage: MAIN_ROUND (Hauptrunde, regular season), PLAYINS, FIRST_ROUND, SECOND_ROUND, ROUND_OF_16 (Achtelfinale), ROUND_OF_8 (Viertelfinale), SEMI_FINALS, FINALS, THIRD_PLACE_GAME, QUALIFICATION, GROUP_STAGE. |
| `seconds_played` | integer | The team's playing time in SECONDS in the game (players plus the team line) (12000 = 5 x 40 min). |
| `points` | integer | The team's points in the game (players plus the team line). |
| `two_point_shots_made` | integer | The team's 2-point field goals made in the game (players plus the team line). |
| `two_point_shots_attempted` | integer | The team's 2-point field goals attempted in the game (players plus the team line). |
| `two_point_shot_success_percent` | numeric | The team's 2-point field-goal percentage in the game (players plus the team line), 0-100 (one decimal); 0 when no attempts. |
| `three_point_shots_made` | integer | The team's 3-point field goals made in the game (players plus the team line). |
| `three_point_shots_attempted` | integer | The team's 3-point field goals attempted in the game (players plus the team line). |
| `three_point_shot_success_percent` | numeric | The team's 3-point field-goal percentage in the game (players plus the team line), 0-100 (one decimal); 0 when no attempts. |
| `field_goals_made` | integer | The team's field goals made in the game (players plus the team line). |
| `field_goals_attempted` | integer | The team's field goals attempted in the game (players plus the team line). |
| `field_goals_success_percent` | numeric | The team's field-goal percentage in the game (players plus the team line), 0-100 (one decimal); 0 when no attempts. |
| `free_throws_made` | integer | The team's free throws made in the game (players plus the team line). |
| `free_throws_attempted` | integer | The team's free throws attempted in the game (players plus the team line). |
| `free_throws_success_percent` | numeric | The team's free-throw percentage in the game (players plus the team line), 0-100 (one decimal); 0 when no attempts. |
| `offensive_rebounds` | integer | The team's offensive rebounds in the game (players plus the team line). |
| `defensive_rebounds` | integer | The team's defensive rebounds in the game (players plus the team line). |
| `total_rebounds` | integer | The team's rebounds (offensive + defensive) in the game (players plus the team line). |
| `assists` | integer | The team's assists in the game (players plus the team line). |
| `steals` | integer | The team's steals in the game (players plus the team line). |
| `turnovers` | integer | The team's turnovers in the game (players plus the team line). |
| `blocks` | integer | The team's blocked shots in the game (players plus the team line). |
| `fouls_committed` | integer | The team's fouls committed in the game (players plus the team line). |
| `efficiency` | integer | The team's efficiency (EFF = PTS + REB + AST + STL + BLK - missed FG - missed FT - TO; checked on a 2026 box score) in the game (players plus the team line). |
| `plus_minus` | integer | The team's point margin in the game. |
| `team_line_seconds_played` | integer | Team-line playing time in SECONDS (not credited to a player). |
| `team_line_points` | integer | Team-line points (not credited to a player). |
| `team_line_two_point_shots_made` | integer | Team-line 2-point field goals made (not credited to a player). |
| `team_line_two_point_shots_attempted` | integer | Team-line 2-point field goals attempted (not credited to a player). |
| `team_line_two_point_shot_success_percent` | numeric | Team-line 2-point field-goal percentage (not credited to a player), 0-100 (one decimal); 0 when no attempts. |
| `team_line_three_point_shots_made` | integer | Team-line 3-point field goals made (not credited to a player). |
| `team_line_three_point_shots_attempted` | integer | Team-line 3-point field goals attempted (not credited to a player). |
| `team_line_three_point_shot_success_percent` | numeric | Team-line 3-point field-goal percentage (not credited to a player), 0-100 (one decimal); 0 when no attempts. |
| `team_line_field_goals_made` | integer | Team-line field goals made (not credited to a player). |
| `team_line_field_goals_attempted` | integer | Team-line field goals attempted (not credited to a player). |
| `team_line_field_goals_success_percent` | numeric | Team-line field-goal percentage (not credited to a player), 0-100 (one decimal); 0 when no attempts. |
| `team_line_free_throws_made` | integer | Team-line free throws made (not credited to a player). |
| `team_line_free_throws_attempted` | integer | Team-line free throws attempted (not credited to a player). |
| `team_line_free_throws_success_percent` | numeric | Team-line free-throw percentage (not credited to a player), 0-100 (one decimal); 0 when no attempts. |
| `team_line_offensive_rebounds` | integer | Team-line offensive rebounds (not credited to a player). |
| `team_line_defensive_rebounds` | integer | Team-line defensive rebounds (not credited to a player). |
| `team_line_total_rebounds` | integer | Team-line rebounds (offensive + defensive) (not credited to a player). |
| `team_line_assists` | integer | Team-line assists (not credited to a player). |
| `team_line_steals` | integer | Team-line steals (not credited to a player). |
| `team_line_turnovers` | integer | Team-line turnovers (not credited to a player). |
| `team_line_blocks` | integer | Team-line blocked shots (not credited to a player). |
| `team_line_fouls_committed` | integer | Team-line fouls committed (not credited to a player). |
| `team_line_efficiency` | integer | Team-line efficiency (EFF = PTS + REB + AST + STL + BLK - missed FG - missed FT - TO; checked on a 2026 box score) (not credited to a player). |
| `team_line_plus_minus` | integer | Team-line plus-minus: the team line has no court time, so this is not a margin. |
| `field_goals_recomputed` | logical | True when the API's field-goal totals disagreed with its 2-point plus 3-point parts (2005-06 box scores) and were recomputed from the parts. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#scb_game_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a game not yet started, an empty table, a playoff bracket not yet drawn); a game before 2002-03 is zero rows. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#scb_game_teams-example}

```python
scb_game_teams(game_id='2006920')
```

_Last validated n/a._

## scb_game_actions

Play-by-play of a finished game with shot x/y (BBL 2008-09 on, DBBL 2024-25 on).

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/games/{game_id}/stats`

**Valid URL:** [https://api.basketball-bundesliga.de/games/2006920/stats](https://api.basketball-bundesliga.de/games/2006920/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Game id (`game_id` in `scb_games`), e.g. 2006920 (BBL) or 10130 (DBBL). One sequence per league. |
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |

### Returns {#scb_game_actions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `game_id` | character | Game id, a string; unique within a league only (BBL and DBBL overlap), and not chronological. |
| `action_id` | integer | Action id, an integer unique across games of a league. |
| `order_id` | integer | Chronological sequence of the action within the game. |
| `action_identifier` | character | Provider action key `<n>:<seq>` (opaque; the first part is not the period). |
| `period` | integer | Period: 1-4 quarters, 5+ overtimes. |
| `game_time` | character | Game clock as HH:MM:SS: in an action, the time LEFT in the period (00:10:00 at the start of a 10-minute quarter); in a game header, 00:00:00 in every sample. |
| `seconds_left` | integer | Seconds left in the period at the action: `game_time` as seconds. |
| `real_time` | character | Wall-clock time of the action, ISO 8601 UTC. Pre-2011 games carry a placeholder: local midnight of the game day (23:00 UTC the day before) on every row. |
| `type` | character | Action type; see the action-type table. Records and videos use their own `type` (see those routes). |
| `qualifiers` | character | Action qualifiers (shot kind, rebound side, foul kind, turnover kind); see the qualifier table. |
| `is_successful` | logical | Made / successful (shots, free throws); for other types see the action-type table. |
| `season_team_id` | character | Season-team id; equal to the club `teamId` in every sample (BBL 2005-2026, DBBL 2024-2026). |
| `side` | character | `home` or `guest` (the away side), for the team the row belongs to. |
| `season_player_id` | character | Season-player id: one player's engagement with one club (kept across seasons while he stays, new after a move; see README). Joins to box-score `seasonPlayer.id`, action `seasonPlayerId` and /season-players/{seasonPlayerId}. Null on team and period rows. |
| `assisting_season_player_id` | character | Season-player id of the assisting player; only on assisted made shots. Joins to box-score `seasonPlayer.id` and action `seasonPlayerId`. |
| `shot_x` | integer | Shot x on the site's half-court chart (0-280 across, 140 through the rim); null without a location. Scale and orientation unverified. |
| `shot_y` | integer | Shot y along the court from behind the baseline (rim near 45, arc top near 147); null without a location. Scale and orientation unverified. |
| `home_team_points` | integer | Home score after this action; only on scoring actions. |
| `guest_team_points` | integer | Guest score after this action; only on scoring actions. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#scb_game_actions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a game not yet started, an empty table, a playoff bracket not yet drawn); a game before 2002-03 is zero rows. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#scb_game_actions-example}

```python
scb_game_actions(game_id='2006920')
```

_Last validated n/a._
