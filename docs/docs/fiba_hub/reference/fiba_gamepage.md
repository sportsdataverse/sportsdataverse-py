---
title: FIBA (digital API and game pages) — FIBA game pages (fiba.basketball, RSC payload parsed)
sidebar_label: FIBA game pages (fiba.basketball, RSC payload parsed)
description: "FIBA (digital API and game pages) — FIBA game pages (fiba.basketball, RSC payload parsed) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 11
toc_max_heading_level: 2
---
# FIBA (digital API and game pages) — FIBA game pages (fiba.basketball, RSC payload parsed)

`sportsdataverse.basketball.fiba_hub` — 6 endpoints.

> **Note:** FIBA's terms restrict this source. The GDAP developer-portal terms limit API use to 'Authorized Users' (federations, organisers, FIBA partners) and say any broader use 'is not authorised'; the fiba.basketball terms say no portion of the site may be duplicated, redistributed or manipulated. The package owner reviewed both clauses on 2026-10-12, accepted them and approved the full intake, published data releases included. Users who are not themselves authorised by FIBA should check the terms before using the wrappers.

> **Note:** The GDAP subscription key is public (fiba.basketball ships it in every page); the runtime reads it from the live page at run time and never logs it. Officials' contact details and coaches' and officials' dates of birth are dropped from every body.

> **Note:** Id key spaces: competition_id is one per edition (2023 World Cup = 208182) and game_id is global across competitions (the <gameId> of a game-page URL). team_id is per team per competition (Germany is 279569 at the 2023 World Cup and 281545 at Paris 2024), while organisation_id is the stable federation or club (64 = Germany, every year); the game page's T_<id> and the play-by-play oId are organisation ids, not team ids.

> **Note:** The wrapper takes the game id and its competition (comp) and builds the real page URL from GDAP (the competition's marketing name gives the history slug, its schedule gives the two team codes; a competition with an event page is tried at its event URL when the archive URL answers the not-found shell, and the Champions League host last). The 'Example URL' above is the wrapper's route, not a page FIBA serves.

> **Note:** Box scores start about 2006; play-by-play with shot x/y about 2012 (OVR events) and 2017-18 (FLS club competitions). A game with no box score raises NoDataError; a game still live raises AssetFetchError. fiba.basketball answers plain requests with HTTP 403, so the page transport is curl_cffi with Chrome TLS (extra: pip install sportsdataverse[all]). Shot x/y are image pixels with an unverified scale.

## fiba_gamepage_box_players

Player box score of one FIBA game.

**Endpoint URL:** `GET https://www.fiba.basketball/en/games/{game_id}`

**Valid URL:** [https://www.fiba.basketball/en/games/100720?comp=208182](https://www.fiba.basketball/en/games/100720?comp=208182)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | FIBA game id (global): gameId of a gdap schedule row, e.g. 100720 = the World Cup 2023 final. |
| `comp` | `comp` |  | `Y` |  | Required. FIBA competition id (GDAP key space), one per edition: competition.competitionId of any game row (208182 = World Cup 2023, 208722 = Paris 2024 men, 208850 = BCL 2024-25). The game must belong to it. Find it with fiba_gdap_editions or fiba_gdap_games_between. |

### Returns {#fiba_gamepage_box_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | FIBA game id (Int64), global across competitions: gdapGameId, the <gameId> of a game-page URL. |
| `competition_id` | Int64 | FIBA competition id (Int64) of the game's edition; the comp argument. |
| `team_side` | String | A or B: whether the row is the schedule's team A or team B (null when the page does not say). |
| `organisation_id` | Int64 | Stable federation or club id (Int64) of the team, the same in every competition (64 = Germany); the page's T_<id> and the play-by-play oId. NOT the per-competition team_id. |
| `player_id` | Int64 | FIBA person id (Int64), global across competitions and roles: the page's P_<id>, play-by-play pId, roster personId and fiba_gdap_roster.player_id. |
| `first_name` | String | Given name(s) as registered with FIBA. |
| `last_name` | String | Last name as registered with FIBA. |
| `jersey` | String | Jersey number as a string. |
| `position` | String | Playing position code from the lineup card (PG, SG, SF, PF, C). |
| `is_captain` | Boolean | True for the team captain. |
| `is_starter` | Boolean | True for the starting five. |
| `has_played` | Boolean | True when the player got on the court. |
| `minutes` | String | Minutes played as an 'm:ss' string ('0:0' = did not play). |
| `seconds_played` | Int64 | Seconds played, derived from minutes. |
| `points` | Int64 | Points scored in the game. |
| `fg_made` | Int64 | Field goals made (2 + 3 points). |
| `fg_attempted` | Int64 | Field goals attempted (2 + 3 points). |
| `fg_pct` | Float64 | Field-goal percentage, 0-100. |
| `fg2_made` | Int64 | Two-point field goals made. |
| `fg2_attempted` | Int64 | Two-point field goals attempted. |
| `fg2_pct` | Float64 | Two-point field-goal percentage, 0-100. |
| `fg3_made` | Int64 | Three-point field goals made. |
| `fg3_attempted` | Int64 | Three-point field goals attempted. |
| `fg3_pct` | Float64 | Three-point field-goal percentage, 0-100. |
| `ft_made` | Int64 | Free throws made. |
| `ft_attempted` | Int64 | Free throws attempted. |
| `ft_pct` | Float64 | Free-throw percentage, 0-100. |
| `reb_offensive` | Int64 | Offensive rebounds. |
| `reb_defensive` | Int64 | Defensive rebounds. |
| `reb_total` | Int64 | Total rebounds (a team row includes the team rebounds team_reb_total). |
| `assists` | Int64 | Assists credited in the game. |
| `steals` | Int64 | Steals credited in the game. |
| `blocks` | Int64 | Blocked shots (blocks made). |
| `blocks_received` | Int64 | Own shots blocked by the opponent (blocks received). |
| `turnovers` | Int64 | Turnovers (a team row includes the team turnovers team_turnovers). |
| `fouls` | Int64 | Personal fouls (a team row includes the team fouls team_fouls). |
| `fouls_drawn` | Int64 | Fouls drawn in the game. |
| `paint_fg_made` | Int64 | Field goals made in the paint (team points_in_paint = 2 x paint_fg_made on every sampled game). |
| `paint_fg_attempted` | Int64 | Field goals attempted in the paint. |
| `paint_fg_pct` | Float64 | Field-goal percentage in the paint, 0-100. |
| `efficiency` | Int64 | Efficiency (FIBA EFF: points + rebounds + assists + steals + blocks - missed field goals - missed free throws - turnovers). |
| `plus_minus` | Int64 | Plus/minus: the team's point differential while the player was on the court. |
| `cb` | Boolean | Boolean flag, false on every sampled player; meaning unverified. |

**`return_parsed=False`** — the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved.

### Raises {#fiba_gamepage_box_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, GDAP answered {} or [] for an unknown id, a listed game whose teams are not drawn yet, or a game page with no box score). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season, a game of another competition than comp, the HTTP 200 'not-found shell' of a wrong game page, a live game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, a malformed id, season or date, or a game page requested without comp. |

### Example {#fiba_gamepage_box_players-example}

```python
fiba_gamepage_box_players(comp=208182, game_id=100720)
```

## fiba_gamepage_box_teams

Team box score of one FIBA game, with the game-level figures.

**Endpoint URL:** `GET https://www.fiba.basketball/en/games/{game_id}`

**Valid URL:** [https://www.fiba.basketball/en/games/100720?comp=208182](https://www.fiba.basketball/en/games/100720?comp=208182)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | FIBA game id (global): gameId of a gdap schedule row, e.g. 100720 = the World Cup 2023 final. |
| `comp` | `comp` |  | `Y` |  | Required. FIBA competition id (GDAP key space), one per edition: competition.competitionId of any game row (208182 = World Cup 2023, 208722 = Paris 2024 men, 208850 = BCL 2024-25). The game must belong to it. Find it with fiba_gdap_editions or fiba_gdap_games_between. |

### Returns {#fiba_gamepage_box_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | FIBA game id (Int64), global across competitions: gdapGameId, the <gameId> of a game-page URL. |
| `competition_id` | Int64 | FIBA competition id (Int64) of the game's edition; the comp argument. |
| `statistic_system` | String | Live-stats system of the game: OVR (World Cup, Olympics) or FLS (club competitions, most qualifiers); they differ in the play-by-play action ids, order and text. |
| `team_side` | String | A or B: whether the row is the schedule's team A or team B (null when the page does not say). |
| `organisation_id` | Int64 | Stable federation or club id (Int64) of the team, the same in every competition (64 = Germany); the page's T_<id> and the play-by-play oId. NOT the per-competition team_id. |
| `score` | Int64 | Team final score. |
| `points` | Int64 | Points scored in the game. |
| `fg_made` | Int64 | Field goals made (2 + 3 points). |
| `fg_attempted` | Int64 | Field goals attempted (2 + 3 points). |
| `fg_pct` | Float64 | Field-goal percentage, 0-100. |
| `fg2_made` | Int64 | Two-point field goals made. |
| `fg2_attempted` | Int64 | Two-point field goals attempted. |
| `fg2_pct` | Float64 | Two-point field-goal percentage, 0-100. |
| `fg3_made` | Int64 | Three-point field goals made. |
| `fg3_attempted` | Int64 | Three-point field goals attempted. |
| `fg3_pct` | Float64 | Three-point field-goal percentage, 0-100. |
| `ft_made` | Int64 | Free throws made. |
| `ft_attempted` | Int64 | Free throws attempted. |
| `ft_pct` | Float64 | Free-throw percentage, 0-100. |
| `reb_offensive` | Int64 | Offensive rebounds. |
| `reb_defensive` | Int64 | Defensive rebounds. |
| `reb_total` | Int64 | Total rebounds (a team row includes the team rebounds team_reb_total). |
| `assists` | Int64 | Assists credited in the game. |
| `steals` | Int64 | Steals credited in the game. |
| `blocks` | Int64 | Blocked shots (blocks made). |
| `blocks_received` | Int64 | Own shots blocked by the opponent (blocks received). |
| `turnovers` | Int64 | Turnovers (a team row includes the team turnovers team_turnovers). |
| `fouls` | Int64 | Personal fouls (a team row includes the team fouls team_fouls). |
| `fouls_drawn` | Int64 | Fouls drawn in the game. |
| `paint_fg_made` | Int64 | Field goals made in the paint (team points_in_paint = 2 x paint_fg_made on every sampled game). |
| `paint_fg_attempted` | Int64 | Field goals attempted in the paint. |
| `paint_fg_pct` | Float64 | Field-goal percentage in the paint, 0-100. |
| `efficiency` | Int64 | Efficiency (FIBA EFF: points + rebounds + assists + steals + blocks - missed field goals - missed free throws - turnovers). |
| `biggest_lead` | Int64 | Biggest lead, points. |
| `biggest_lead_score` | String | Score when the biggest lead was reached, 'A-B' string (spacing varies; empty when the team never led). |
| `biggest_run` | Int64 | Biggest scoring run, points. |
| `biggest_run_score` | String | Score of the biggest scoring run, 'x-y' string; exact meaning unverified. |
| `fast_break_points` | Int64 | Fast-break points. |
| `points_off_turnovers` | Int64 | Points scored after turnovers. |
| `bench_points` | Int64 | Points from the bench (the sum of the non-starters' points). |
| `points_in_paint` | Int64 | Points in the paint (2 x paint_fg_made). |
| `second_chance_points` | Int64 | Second-chance points. |
| `a_act` | Int64 | Always 0 in the samples; meaning unverified (FIBA stat code A_ACT). |
| `a_foul` | Int64 | Always 0 in the samples; meaning unverified (FIBA stat code A_FOUL). |
| `a_tod` | Int64 | Always 0 in the samples; meaning unverified (FIBA stat code A_TOD). |
| `a_tot` | Int64 | Always 0 in the samples; meaning unverified (FIBA stat code A_TOT). |
| `team_reb_offensive` | Int64 | Team offensive rebounds, credited to no player. |
| `team_reb_defensive` | Int64 | Team defensive rebounds, credited to no player. |
| `team_reb_total` | Int64 | Team rebounds (total rebounds minus the players' sum). |
| `team_fouls` | Int64 | Team (bench/coach) fouls (personal fouls minus the players' sum). |
| `team_turnovers` | Int64 | Team turnovers (turnovers minus the players' sum). |
| `team_bench_points_flag` | Int64 | Bench points in FLS games (equal to bench_points); 0 in OVR games (FIBA stat code T_PFB). |
| `time_led` | String | Time the team led, 'mm:ss' string. |
| `lead_changes` | Int64 | Lead changes in the game (game-level; repeats on both team rows). |
| `times_tied` | Int64 | Times the score was tied (game-level; repeats on both team rows). |
| `duration` | String | Game duration as an 'h:mm' string (game-level; repeats on both team rows). |
| `attendance` | Int64 | Attendance (game-level; repeats on both team rows). |

**`return_parsed=False`** — the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved.

### Raises {#fiba_gamepage_box_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, GDAP answered {} or [] for an unknown id, a listed game whose teams are not drawn yet, or a game page with no box score). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season, a game of another competition than comp, the HTTP 200 'not-found shell' of a wrong game page, a live game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, a malformed id, season or date, or a game page requested without comp. |

### Example {#fiba_gamepage_box_teams-example}

```python
fiba_gamepage_box_teams(comp=208182, game_id=100720)
```

## fiba_gamepage_lineup

Lineup card (the players who were registered for the game) of both teams of one FIBA game.

**Endpoint URL:** `GET https://www.fiba.basketball/en/games/{game_id}`

**Valid URL:** [https://www.fiba.basketball/en/games/100720?comp=208182](https://www.fiba.basketball/en/games/100720?comp=208182)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | FIBA game id (global): gameId of a gdap schedule row, e.g. 100720 = the World Cup 2023 final. |
| `comp` | `comp` |  | `Y` |  | Required. FIBA competition id (GDAP key space), one per edition: competition.competitionId of any game row (208182 = World Cup 2023, 208722 = Paris 2024 men, 208850 = BCL 2024-25). The game must belong to it. Find it with fiba_gdap_editions or fiba_gdap_games_between. |

### Returns {#fiba_gamepage_lineup-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | FIBA game id (Int64), global across competitions: gdapGameId, the <gameId> of a game-page URL. |
| `competition_id` | Int64 | FIBA competition id (Int64) of the game's edition; the comp argument. |
| `team_side` | String | A or B: whether the row is the schedule's team A or team B (null when the page does not say). |
| `organisation_id` | Int64 | Stable federation or club id (Int64) of the team, the same in every competition (64 = Germany); the page's T_<id> and the play-by-play oId. NOT the per-competition team_id. |
| `player_id` | Int64 | FIBA person id (Int64), global across competitions and roles: the page's P_<id>, play-by-play pId, roster personId and fiba_gdap_roster.player_id. |
| `first_name` | String | Given name(s) as registered with FIBA. |
| `last_name` | String | Last name as registered with FIBA. |
| `jersey` | String | Jersey number as a string. |
| `position` | String | Playing position code from the lineup card (PG, SG, SF, PF, C). |
| `is_captain` | Boolean | True for the team captain. |

**`return_parsed=False`** — the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved.

### Raises {#fiba_gamepage_lineup-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, GDAP answered {} or [] for an unknown id, a listed game whose teams are not drawn yet, or a game page with no box score). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season, a game of another competition than comp, the HTTP 200 'not-found shell' of a wrong game page, a live game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, a malformed id, season or date, or a game page requested without comp. |

### Example {#fiba_gamepage_lineup-example}

```python
fiba_gamepage_lineup(comp=208182, game_id=100720)
```

## fiba_gamepage_pbp

Play-by-play of one FIBA game: every action with period, clock, running score, actor and shot x/y.

**Endpoint URL:** `GET https://www.fiba.basketball/en/games/{game_id}`

**Valid URL:** [https://www.fiba.basketball/en/games/100720?comp=208182](https://www.fiba.basketball/en/games/100720?comp=208182)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | FIBA game id (global): gameId of a gdap schedule row, e.g. 100720 = the World Cup 2023 final. |
| `comp` | `comp` |  | `Y` |  | Required. FIBA competition id (GDAP key space), one per edition: competition.competitionId of any game row (208182 = World Cup 2023, 208722 = Paris 2024 men, 208850 = BCL 2024-25). The game must belong to it. Find it with fiba_gdap_editions or fiba_gdap_games_between. |

### Returns {#fiba_gamepage_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | FIBA game id (Int64), global across competitions: gdapGameId, the <gameId> of a game-page URL. |
| `competition_id` | Int64 | FIBA competition id (Int64) of the game's edition; the comp argument. |
| `statistic_system` | String | Live-stats system of the game: OVR (World Cup, Olympics) or FLS (club competitions, most qualifiers); they differ in the play-by-play action ids, order and text. |
| `period` | String | Period name: Q1..Q4, then OT1, OT2... |
| `period_number` | Int64 | 1-based position of the period in the game. |
| `action_id` | String | Action id (String): '<n>:<seq>' in OVR games (3:0015), a sequence number in FLS games; unique within the game. |
| `order` | Int64 | Sort key within the period (OVR: 1, 2, ...; FLS: 1000, 2000, ...); null on some period events. |
| `clock` | String | Game clock: time REMAINING in the period, 'm:ss', counting down from 10:00 (5:00 in overtime). |
| `clock_seconds` | Int64 | Seconds remaining in the period, derived from clock. |
| `wall_time_ms` | Int64 | Wall-clock time of the action, Unix epoch milliseconds. |
| `score_a` | Int64 | Team A score after the action. |
| `score_b` | Int64 | Team B score after the action. |
| `action_code` | String | Action code: P2 two-point shot, P3 three-point shot, FT free throw, REB rebound, TREB team rebound, ASS assist, ST steal, BS block, TO turnover, TTO team turnover, FOUL foul, RFOUL foul drawn, SUBST substitution, TIMO timeout, plus period events (STARTG, STARTP, ENDP, ENDG). |
| `action_family` | String | Action family: shot, subst, timeout, periods, and 'unknwown' (FIBA's spelling) for every other action. |
| `description` | String | English action text ('3pt jump shot missed', 'defensive rebound'); null on most OVR actions. |
| `organisation_id` | Int64 | Stable federation or club id (Int64) of the team, the same in every competition (64 = Germany); the page's T_<id> and the play-by-play oId. NOT the per-competition team_id. |
| `team_side` | String | A or B: whether the row is the schedule's team A or team B (null when the page does not say). |
| `player_id` | Int64 | FIBA person id (Int64), global across competitions and roles: the page's P_<id>, play-by-play pId, roster personId and fiba_gdap_roster.player_id. |
| `player2_id` | Int64 | FIBA person id (Int64) of the second player on a substitution; null when the game does not record it (the refs note FLS only, but the Paris 2024 final, an OVR game, carries it and the FLS games sampled leave it null). |
| `sub_direction` | String | Substitution direction: IN or OUT; null on other actions. |
| `made` | Boolean | True when the shot went in (two-pointers, three-pointers and free throws only; null otherwise). |
| `points_attempted` | Int64 | Points the attempt was worth (2, 3 or 1), made or not; null on other actions. |
| `shot_type` | String | Shot type derived from action_code: 2PT, 3PT or FT; null on other actions. |
| `shot_x` | Int64 | Shot x, integer pixels on a half-court image across the court width (observed 0-278, basket near x = 139). Present only on shots; free throws are 0. Which sideline is x = 0 and the scale are UNVERIFIED. |
| `shot_y` | Int64 | Shot y, integer pixels from the baseline (y = 0) towards midcourt (observed 0-224, basket near y = 25); both teams' shots are drawn on one basket. Free throws are 0. Scale UNVERIFIED. |
| `has_location` | Boolean | True when shot_x / shot_y locate the shot (two- and three-pointers); False for free throws (always 0, 0); null on other actions. |

**`return_parsed=False`** — the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved.

### Raises {#fiba_gamepage_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, GDAP answered {} or [] for an unknown id, a listed game whose teams are not drawn yet, or a game page with no box score). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season, a game of another competition than comp, the HTTP 200 'not-found shell' of a wrong game page, a live game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, a malformed id, season or date, or a game page requested without comp. |

### Example {#fiba_gamepage_pbp-example}

```python
fiba_gamepage_pbp(comp=208182, game_id=100720)
```

## fiba_gamepage_period_scores

Points by period of both teams of one FIBA game.

**Endpoint URL:** `GET https://www.fiba.basketball/en/games/{game_id}`

**Valid URL:** [https://www.fiba.basketball/en/games/100720?comp=208182](https://www.fiba.basketball/en/games/100720?comp=208182)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | FIBA game id (global): gameId of a gdap schedule row, e.g. 100720 = the World Cup 2023 final. |
| `comp` | `comp` |  | `Y` |  | Required. FIBA competition id (GDAP key space), one per edition: competition.competitionId of any game row (208182 = World Cup 2023, 208722 = Paris 2024 men, 208850 = BCL 2024-25). The game must belong to it. Find it with fiba_gdap_editions or fiba_gdap_games_between. |

### Returns {#fiba_gamepage_period_scores-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | FIBA game id (Int64), global across competitions: gdapGameId, the <gameId> of a game-page URL. |
| `competition_id` | Int64 | FIBA competition id (Int64) of the game's edition; the comp argument. |
| `team_side` | String | A or B: whether the row is the schedule's team A or team B (null when the page does not say). |
| `organisation_id` | Int64 | Stable federation or club id (Int64) of the team, the same in every competition (64 = Germany); the page's T_<id> and the play-by-play oId. NOT the per-competition team_id. |
| `period` | String | Period name: Q1..Q4, then OT1, OT2... |
| `period_number` | Int64 | 1-based position of the period in the game. |
| `points` | Int64 | Points the team scored in the period. |
| `points_first_five_min` | Int64 | Points the team scored in the first five minutes of the period (matches the play-by-play except free throws on the 5:00 boundary); null when absent. |

**`return_parsed=False`** — the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved.

### Raises {#fiba_gamepage_period_scores-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, GDAP answered {} or [] for an unknown id, a listed game whose teams are not drawn yet, or a game page with no box score). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season, a game of another competition than comp, the HTTP 200 'not-found shell' of a wrong game page, a live game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, a malformed id, season or date, or a game page requested without comp. |

### Example {#fiba_gamepage_period_scores-example}

```python
fiba_gamepage_period_scores(comp=208182, game_id=100720)
```

## fiba_gamepage_shots

Shot attempts of one FIBA game (two- and three-pointers and free throws) with their x/y.

**Endpoint URL:** `GET https://www.fiba.basketball/en/games/{game_id}`

**Valid URL:** [https://www.fiba.basketball/en/games/100720?comp=208182](https://www.fiba.basketball/en/games/100720?comp=208182)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | FIBA game id (global): gameId of a gdap schedule row, e.g. 100720 = the World Cup 2023 final. |
| `comp` | `comp` |  | `Y` |  | Required. FIBA competition id (GDAP key space), one per edition: competition.competitionId of any game row (208182 = World Cup 2023, 208722 = Paris 2024 men, 208850 = BCL 2024-25). The game must belong to it. Find it with fiba_gdap_editions or fiba_gdap_games_between. |

### Returns {#fiba_gamepage_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | FIBA game id (Int64), global across competitions: gdapGameId, the <gameId> of a game-page URL. |
| `competition_id` | Int64 | FIBA competition id (Int64) of the game's edition; the comp argument. |
| `statistic_system` | String | Live-stats system of the game: OVR (World Cup, Olympics) or FLS (club competitions, most qualifiers); they differ in the play-by-play action ids, order and text. |
| `period` | String | Period name: Q1..Q4, then OT1, OT2... |
| `period_number` | Int64 | 1-based position of the period in the game. |
| `action_id` | String | Action id (String): '<n>:<seq>' in OVR games (3:0015), a sequence number in FLS games; unique within the game. |
| `order` | Int64 | Sort key within the period (OVR: 1, 2, ...; FLS: 1000, 2000, ...); null on some period events. |
| `clock` | String | Game clock: time REMAINING in the period, 'm:ss', counting down from 10:00 (5:00 in overtime). |
| `clock_seconds` | Int64 | Seconds remaining in the period, derived from clock. |
| `action_code` | String | Action code: P2 two-point shot, P3 three-point shot, FT free throw, REB rebound, TREB team rebound, ASS assist, ST steal, BS block, TO turnover, TTO team turnover, FOUL foul, RFOUL foul drawn, SUBST substitution, TIMO timeout, plus period events (STARTG, STARTP, ENDP, ENDG). |
| `organisation_id` | Int64 | Stable federation or club id (Int64) of the team, the same in every competition (64 = Germany); the page's T_<id> and the play-by-play oId. NOT the per-competition team_id. |
| `team_side` | String | A or B: whether the row is the schedule's team A or team B (null when the page does not say). |
| `player_id` | Int64 | FIBA person id (Int64), global across competitions and roles: the page's P_<id>, play-by-play pId, roster personId and fiba_gdap_roster.player_id. |
| `shot_type` | String | Shot type derived from action_code: 2PT, 3PT or FT; null on other actions. |
| `points_attempted` | Int64 | Points the attempt was worth (2, 3 or 1), made or not; null on other actions. |
| `made` | Boolean | True when the shot went in (two-pointers, three-pointers and free throws only; null otherwise). |
| `shot_x` | Int64 | Shot x, integer pixels on a half-court image across the court width (observed 0-278, basket near x = 139). Present only on shots; free throws are 0. Which sideline is x = 0 and the scale are UNVERIFIED. |
| `shot_y` | Int64 | Shot y, integer pixels from the baseline (y = 0) towards midcourt (observed 0-224, basket near y = 25); both teams' shots are drawn on one basket. Free throws are 0. Scale UNVERIFIED. |
| `has_location` | Boolean | True when shot_x / shot_y locate the shot (two- and three-pointers); False for free throws (always 0, 0); null on other actions. |

**`return_parsed=False`** — the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved.

### Raises {#fiba_gamepage_shots-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, GDAP answered {} or [] for an unknown id, a listed game whose teams are not drawn yet, or a game page with no box score). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season, a game of another competition than comp, the HTTP 200 'not-found shell' of a wrong game page, a live game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, a malformed id, season or date, or a game page requested without comp. |

### Example {#fiba_gamepage_shots-example}

```python
fiba_gamepage_shots(comp=208182, game_id=100720)
```
