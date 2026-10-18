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
| `game_id` | integer | FIBA game id (Int64), global across competitions: gdapGameId, the <gameId> of a game-page URL. |
| `competition_id` | integer | FIBA competition id (Int64) of the game's edition; the comp argument. |
| `team_side` | character | A or B: whether the row is the schedule's team A or team B (null when the page does not say). |
| `organisation_id` | integer | Stable federation or club id (Int64) of the team, the same in every competition (64 = Germany); the page's T_<id> and the play-by-play oId. NOT the per-competition team_id. |
| `player_id` | integer | FIBA person id (Int64), global across competitions and roles: the page's P_<id>, play-by-play pId, roster personId and fiba_gdap_roster.player_id. |
| `first_name` | character | Given name(s) as registered with FIBA. |
| `last_name` | character | Last name as registered with FIBA. |
| `jersey` | character | Jersey number as a string. |
| `position` | character | Playing position code from the lineup card (PG, SG, SF, PF, C). |
| `is_captain` | logical | True for the team captain. |
| `is_starter` | logical | True for the starting five. |
| `has_played` | logical | True when the player got on the court. |
| `minutes` | character | Minutes played as an 'm:ss' string ('0:0' = did not play). |
| `seconds_played` | integer | Seconds played, derived from minutes. |
| `points` | integer | Points scored in the game. |
| `fg_made` | integer | Field goals made (2 + 3 points). |
| `fg_attempted` | integer | Field goals attempted (2 + 3 points). |
| `fg_pct` | numeric | Field-goal percentage, 0-100. |
| `fg2_made` | integer | Two-point field goals made. |
| `fg2_attempted` | integer | Two-point field goals attempted. |
| `fg2_pct` | numeric | Two-point field-goal percentage, 0-100. |
| `fg3_made` | integer | Three-point field goals made. |
| `fg3_attempted` | integer | Three-point field goals attempted. |
| `fg3_pct` | numeric | Three-point field-goal percentage, 0-100. |
| `ft_made` | integer | Free throws made. |
| `ft_attempted` | integer | Free throws attempted. |
| `ft_pct` | numeric | Free-throw percentage, 0-100. |
| `reb_offensive` | integer | Offensive rebounds. |
| `reb_defensive` | integer | Defensive rebounds. |
| `reb_total` | integer | Total rebounds (a team row includes the team rebounds team_reb_total). |
| `assists` | integer | Assists credited in the game. |
| `steals` | integer | Steals credited in the game. |
| `blocks` | integer | Blocked shots (blocks made). |
| `blocks_received` | integer | Own shots blocked by the opponent (blocks received). |
| `turnovers` | integer | Turnovers (a team row includes the team turnovers team_turnovers). |
| `fouls` | integer | Personal fouls (a team row includes the team fouls team_fouls). |
| `fouls_drawn` | integer | Fouls drawn in the game. |
| `paint_fg_made` | integer | Field goals made in the paint (team points_in_paint = 2 x paint_fg_made on every sampled game). |
| `paint_fg_attempted` | integer | Field goals attempted in the paint. |
| `paint_fg_pct` | numeric | Field-goal percentage in the paint, 0-100. |
| `efficiency` | integer | Efficiency (FIBA EFF: points + rebounds + assists + steals + blocks - missed field goals - missed free throws - turnovers). |
| `plus_minus` | integer | Plus/minus: the team's point differential while the player was on the court. |
| `cb` | logical | Boolean flag, false on every sampled player; meaning unverified. |

**`return_parsed=False`** — the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved.

### Raises {#fiba_gamepage_box_players-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | FIBA has nothing for the request (HTTP 404, GDAP answered {} or [] for an unknown id, a game the competition does not list, or a game page with no box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season, the HTTP 200 'not-found shell' of a wrong game page, a live game). |
| `ValueError` | HTTP 400 / 422, a malformed id, season or date, or a game page requested without comp. |

### Example {#fiba_gamepage_box_players-example}

```python
fiba_gamepage_box_players(comp=208182, game_id=100720)
```

_Last validated n/a._

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
| `game_id` | integer | FIBA game id (Int64), global across competitions: gdapGameId, the <gameId> of a game-page URL. |
| `competition_id` | integer | FIBA competition id (Int64) of the game's edition; the comp argument. |
| `statistic_system` | character | Live-stats system of the game: OVR (World Cup, Olympics) or FLS (club competitions, most qualifiers); they differ in the play-by-play action ids, order and text. |
| `team_side` | character | A or B: whether the row is the schedule's team A or team B (null when the page does not say). |
| `organisation_id` | integer | Stable federation or club id (Int64) of the team, the same in every competition (64 = Germany); the page's T_<id> and the play-by-play oId. NOT the per-competition team_id. |
| `score` | integer | Team final score. |
| `points` | integer | Points scored in the game. |
| `fg_made` | integer | Field goals made (2 + 3 points). |
| `fg_attempted` | integer | Field goals attempted (2 + 3 points). |
| `fg_pct` | numeric | Field-goal percentage, 0-100. |
| `fg2_made` | integer | Two-point field goals made. |
| `fg2_attempted` | integer | Two-point field goals attempted. |
| `fg2_pct` | numeric | Two-point field-goal percentage, 0-100. |
| `fg3_made` | integer | Three-point field goals made. |
| `fg3_attempted` | integer | Three-point field goals attempted. |
| `fg3_pct` | numeric | Three-point field-goal percentage, 0-100. |
| `ft_made` | integer | Free throws made. |
| `ft_attempted` | integer | Free throws attempted. |
| `ft_pct` | numeric | Free-throw percentage, 0-100. |
| `reb_offensive` | integer | Offensive rebounds. |
| `reb_defensive` | integer | Defensive rebounds. |
| `reb_total` | integer | Total rebounds (a team row includes the team rebounds team_reb_total). |
| `assists` | integer | Assists credited in the game. |
| `steals` | integer | Steals credited in the game. |
| `blocks` | integer | Blocked shots (blocks made). |
| `blocks_received` | character | Own shots blocked by the opponent (blocks received). |
| `turnovers` | integer | Turnovers (a team row includes the team turnovers team_turnovers). |
| `fouls` | integer | Personal fouls (a team row includes the team fouls team_fouls). |
| `fouls_drawn` | integer | Fouls drawn in the game. |
| `paint_fg_made` | integer | Field goals made in the paint (team points_in_paint = 2 x paint_fg_made on every sampled game). |
| `paint_fg_attempted` | integer | Field goals attempted in the paint. |
| `paint_fg_pct` | numeric | Field-goal percentage in the paint, 0-100. |
| `efficiency` | integer | Efficiency (FIBA EFF: points + rebounds + assists + steals + blocks - missed field goals - missed free throws - turnovers). |
| `biggest_lead` | integer | Biggest lead, points. |
| `biggest_lead_score` | character | Score when the biggest lead was reached, 'A-B' string (spacing varies; empty when the team never led). |
| `biggest_run` | integer | Biggest scoring run, points. |
| `biggest_run_score` | character | Score of the biggest scoring run, 'x-y' string; exact meaning unverified. |
| `fast_break_points` | integer | Fast-break points. |
| `points_off_turnovers` | integer | Points scored after turnovers. |
| `bench_points` | integer | Points from the bench (the sum of the non-starters' points). |
| `points_in_paint` | integer | Points in the paint (2 x paint_fg_made). |
| `second_chance_points` | integer | Second-chance points. |
| `a_act` | integer | Always 0 in the samples; meaning unverified (FIBA stat code A_ACT). |
| `a_foul` | integer | Always 0 in the samples; meaning unverified (FIBA stat code A_FOUL). |
| `a_tod` | integer | Always 0 in the samples; meaning unverified (FIBA stat code A_TOD). |
| `a_tot` | integer | Always 0 in the samples; meaning unverified (FIBA stat code A_TOT). |
| `team_reb_offensive` | integer | Team offensive rebounds, credited to no player. |
| `team_reb_defensive` | integer | Team defensive rebounds, credited to no player. |
| `team_reb_total` | integer | Team rebounds (total rebounds minus the players' sum). |
| `team_fouls` | integer | Team (bench/coach) fouls (personal fouls minus the players' sum). |
| `team_turnovers` | integer | Team turnovers (turnovers minus the players' sum). |
| `team_bench_points_flag` | integer | Bench points in FLS games (equal to bench_points); 0 in OVR games (FIBA stat code T_PFB). |
| `time_led` | character | Time the team led, 'mm:ss' string. |
| `lead_changes` | integer | Lead changes in the game (game-level; repeats on both team rows). |
| `times_tied` | integer | Times the score was tied (game-level; repeats on both team rows). |
| `duration` | character | Game duration as an 'h:mm' string (game-level; repeats on both team rows). |
| `attendance` | integer | Attendance (game-level; repeats on both team rows). |

**`return_parsed=False`** — the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved.

### Raises {#fiba_gamepage_box_teams-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | FIBA has nothing for the request (HTTP 404, GDAP answered {} or [] for an unknown id, a game the competition does not list, or a game page with no box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season, the HTTP 200 'not-found shell' of a wrong game page, a live game). |
| `ValueError` | HTTP 400 / 422, a malformed id, season or date, or a game page requested without comp. |

### Example {#fiba_gamepage_box_teams-example}

```python
fiba_gamepage_box_teams(comp=208182, game_id=100720)
```

_Last validated n/a._

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
| `game_id` | integer | FIBA game id (Int64), global across competitions: gdapGameId, the <gameId> of a game-page URL. |
| `competition_id` | integer | FIBA competition id (Int64) of the game's edition; the comp argument. |
| `team_side` | character | A or B: whether the row is the schedule's team A or team B (null when the page does not say). |
| `organisation_id` | integer | Stable federation or club id (Int64) of the team, the same in every competition (64 = Germany); the page's T_<id> and the play-by-play oId. NOT the per-competition team_id. |
| `player_id` | integer | FIBA person id (Int64), global across competitions and roles: the page's P_<id>, play-by-play pId, roster personId and fiba_gdap_roster.player_id. |
| `first_name` | character | Given name(s) as registered with FIBA. |
| `last_name` | character | Last name as registered with FIBA. |
| `jersey` | character | Jersey number as a string. |
| `position` | character | Playing position code from the lineup card (PG, SG, SF, PF, C). |
| `is_captain` | logical | True for the team captain. |

**`return_parsed=False`** — the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved.

### Raises {#fiba_gamepage_lineup-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | FIBA has nothing for the request (HTTP 404, GDAP answered {} or [] for an unknown id, a game the competition does not list, or a game page with no box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season, the HTTP 200 'not-found shell' of a wrong game page, a live game). |
| `ValueError` | HTTP 400 / 422, a malformed id, season or date, or a game page requested without comp. |

### Example {#fiba_gamepage_lineup-example}

```python
fiba_gamepage_lineup(comp=208182, game_id=100720)
```

_Last validated n/a._

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
| `game_id` | integer | FIBA game id (Int64), global across competitions: gdapGameId, the <gameId> of a game-page URL. |
| `competition_id` | integer | FIBA competition id (Int64) of the game's edition; the comp argument. |
| `statistic_system` | character | Live-stats system of the game: OVR (World Cup, Olympics) or FLS (club competitions, most qualifiers); they differ in the play-by-play action ids, order and text. |
| `period` | character | Period name: Q1..Q4, then OT1, OT2... |
| `period_number` | integer | 1-based position of the period in the game. |
| `action_id` | character | Action id (String): '<n>:<seq>' in OVR games (3:0015), a sequence number in FLS games; unique within the game. |
| `order` | integer | Sort key within the period (OVR: 1, 2, ...; FLS: 1000, 2000, ...); null on some period events. |
| `clock` | character | Game clock: time REMAINING in the period, 'm:ss', counting down from 10:00 (5:00 in overtime). |
| `clock_seconds` | integer | Seconds remaining in the period, derived from clock. |
| `wall_time_ms` | integer | Wall-clock time of the action, Unix epoch milliseconds. |
| `score_a` | integer | Team A score after the action. |
| `score_b` | integer | Team B score after the action. |
| `action_code` | character | Action code: P2 two-point shot, P3 three-point shot, FT free throw, REB rebound, TREB team rebound, ASS assist, ST steal, BS block, TO turnover, TTO team turnover, FOUL foul, RFOUL foul drawn, SUBST substitution, TIMO timeout, plus period events (STARTG, STARTP, ENDP, ENDG). |
| `action_family` | character | Action family: shot, subst, timeout, periods, and 'unknwown' (FIBA's spelling) for every other action. |
| `description` | character | English action text ('3pt jump shot missed', 'defensive rebound'); null on most OVR actions. |
| `organisation_id` | integer | Stable federation or club id (Int64) of the team, the same in every competition (64 = Germany); the page's T_<id> and the play-by-play oId. NOT the per-competition team_id. |
| `team_side` | character | A or B: whether the row is the schedule's team A or team B (null when the page does not say). |
| `player_id` | integer | FIBA person id (Int64), global across competitions and roles: the page's P_<id>, play-by-play pId, roster personId and fiba_gdap_roster.player_id. |
| `player2_id` | integer | FIBA person id (Int64) of the second player on a substitution; null when the game does not record it (the refs note FLS only, but the Paris 2024 final, an OVR game, carries it and the FLS games sampled leave it null). |
| `sub_direction` | character | Substitution direction: IN or OUT; null on other actions. |
| `made` | logical | True when the shot went in (two-pointers, three-pointers and free throws only; null otherwise). |
| `points_attempted` | integer | Points the attempt was worth (2, 3 or 1), made or not; null on other actions. |
| `shot_type` | character | Shot type derived from action_code: 2PT, 3PT or FT; null on other actions. |
| `shot_x` | integer | Shot x, integer pixels on a half-court image across the court width (observed 0-278, basket near x = 139). Present only on shots; free throws are 0. Which sideline is x = 0 and the scale are UNVERIFIED. |
| `shot_y` | integer | Shot y, integer pixels from the baseline (y = 0) towards midcourt (observed 0-224, basket near y = 25); both teams' shots are drawn on one basket. Free throws are 0. Scale UNVERIFIED. |
| `has_location` | logical | True when shot_x / shot_y locate the shot (two- and three-pointers); False for free throws (always 0, 0); null on other actions. |

**`return_parsed=False`** — the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved.

### Raises {#fiba_gamepage_pbp-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | FIBA has nothing for the request (HTTP 404, GDAP answered {} or [] for an unknown id, a game the competition does not list, or a game page with no box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season, the HTTP 200 'not-found shell' of a wrong game page, a live game). |
| `ValueError` | HTTP 400 / 422, a malformed id, season or date, or a game page requested without comp. |

### Example {#fiba_gamepage_pbp-example}

```python
fiba_gamepage_pbp(comp=208182, game_id=100720)
```

_Last validated n/a._

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
| `game_id` | integer | FIBA game id (Int64), global across competitions: gdapGameId, the <gameId> of a game-page URL. |
| `competition_id` | integer | FIBA competition id (Int64) of the game's edition; the comp argument. |
| `team_side` | character | A or B: whether the row is the schedule's team A or team B (null when the page does not say). |
| `organisation_id` | integer | Stable federation or club id (Int64) of the team, the same in every competition (64 = Germany); the page's T_<id> and the play-by-play oId. NOT the per-competition team_id. |
| `period` | character | Period name: Q1..Q4, then OT1, OT2... |
| `period_number` | integer | 1-based position of the period in the game. |
| `points` | integer | Points the team scored in the period. |
| `points_first_five_min` | integer | Points the team scored in the first five minutes of the period (matches the play-by-play except free throws on the 5:00 boundary); null when absent. |

**`return_parsed=False`** — the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved.

### Raises {#fiba_gamepage_period_scores-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | FIBA has nothing for the request (HTTP 404, GDAP answered {} or [] for an unknown id, a game the competition does not list, or a game page with no box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season, the HTTP 200 'not-found shell' of a wrong game page, a live game). |
| `ValueError` | HTTP 400 / 422, a malformed id, season or date, or a game page requested without comp. |

### Example {#fiba_gamepage_period_scores-example}

```python
fiba_gamepage_period_scores(comp=208182, game_id=100720)
```

_Last validated n/a._

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
| `game_id` | integer | FIBA game id (Int64), global across competitions: gdapGameId, the <gameId> of a game-page URL. |
| `competition_id` | integer | FIBA competition id (Int64) of the game's edition; the comp argument. |
| `statistic_system` | character | Live-stats system of the game: OVR (World Cup, Olympics) or FLS (club competitions, most qualifiers); they differ in the play-by-play action ids, order and text. |
| `period` | character | Period name: Q1..Q4, then OT1, OT2... |
| `period_number` | integer | 1-based position of the period in the game. |
| `action_id` | character | Action id (String): '<n>:<seq>' in OVR games (3:0015), a sequence number in FLS games; unique within the game. |
| `order` | integer | Sort key within the period (OVR: 1, 2, ...; FLS: 1000, 2000, ...); null on some period events. |
| `clock` | character | Game clock: time REMAINING in the period, 'm:ss', counting down from 10:00 (5:00 in overtime). |
| `clock_seconds` | integer | Seconds remaining in the period, derived from clock. |
| `action_code` | character | Action code: P2 two-point shot, P3 three-point shot, FT free throw, REB rebound, TREB team rebound, ASS assist, ST steal, BS block, TO turnover, TTO team turnover, FOUL foul, RFOUL foul drawn, SUBST substitution, TIMO timeout, plus period events (STARTG, STARTP, ENDP, ENDG). |
| `organisation_id` | integer | Stable federation or club id (Int64) of the team, the same in every competition (64 = Germany); the page's T_<id> and the play-by-play oId. NOT the per-competition team_id. |
| `team_side` | character | A or B: whether the row is the schedule's team A or team B (null when the page does not say). |
| `player_id` | integer | FIBA person id (Int64), global across competitions and roles: the page's P_<id>, play-by-play pId, roster personId and fiba_gdap_roster.player_id. |
| `shot_type` | character | Shot type derived from action_code: 2PT, 3PT or FT; null on other actions. |
| `points_attempted` | integer | Points the attempt was worth (2, 3 or 1), made or not; null on other actions. |
| `made` | logical | True when the shot went in (two-pointers, three-pointers and free throws only; null otherwise). |
| `shot_x` | integer | Shot x, integer pixels on a half-court image across the court width (observed 0-278, basket near x = 139). Present only on shots; free throws are 0. Which sideline is x = 0 and the scale are UNVERIFIED. |
| `shot_y` | integer | Shot y, integer pixels from the baseline (y = 0) towards midcourt (observed 0-224, basket near y = 25); both teams' shots are drawn on one basket. Free throws are 0. Scale UNVERIFIED. |
| `has_location` | logical | True when shot_x / shot_y locate the shot (two- and three-pointers); False for free throws (always 0, 0); null on other actions. |

**`return_parsed=False`** — the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved.

### Raises {#fiba_gamepage_shots-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | FIBA has nothing for the request (HTTP 404, GDAP answered {} or [] for an unknown id, a game the competition does not list, or a game page with no box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season, the HTTP 200 'not-found shell' of a wrong game page, a live game). |
| `ValueError` | HTTP 400 / 422, a malformed id, season or date, or a game page requested without comp. |

### Example {#fiba_gamepage_shots-example}

```python
fiba_gamepage_shots(comp=208182, game_id=100720)
```

_Last validated n/a._
