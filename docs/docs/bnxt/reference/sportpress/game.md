---
title: "BNXT League — BNXT League Sportpress API (bnxt.sportpress.info, Belgium and the Netherlands) — Game"
sidebar_label: "Game"
sidebar_position: 1
description: "BNXT League — BNXT League Sportpress API (bnxt.sportpress.info, Belgium and the Netherlands) — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# BNXT League — BNXT League Sportpress API (bnxt.sportpress.info, Belgium and the Netherlands) — Game

## sportpress_game_slider

The latest 14 finished games of a competition plus every upcoming game, with the Genius match id.

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/schedule/game-slider/{competition_id}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/schedule/game-slider/25](https://bnxt.sportpress.info/api/v1/schedule/game-slider/25)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_id` | `competition_id` |  | `Y` |  | Required. Competition id (integer), ONE per season of the league: 24 = 2025-26, 25 = 2026-27 (``sportpress_phases`` maps a season to it). |

### Returns {#sportpress_game_slider-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id (key space: sportpress games), unique across seasons: the key of every game route. |
| `genius_id` | Int64 | Genius Sports LiveStats match id (organisation BNXT): the join to the genius family. Upcoming games already carry one; null for the 2020-21 competition. |
| `game_time` | String | Tip-off as a naive datetime. The source carries no offset; the league's local time is assumed and unverified. |
| `status` | String | Game status: finished, or ready for an upcoming game. |
| `competition_id` | Int64 | Competition id (key space: sportpress competitions): ONE season of the league (24 = 2025-26, 25 = 2026-27). Not the season number. The key of the standings, ranking, slider and box routes. |
| `season` | Int64 | Season END year (2026 = 2025-26), the sdv convention and the API's own season key. |
| `phase_id` | Int64 | Phase id (key space: sportpress phases), a stage of one competition such as the regular season or the playoffs; new every season. |
| `phase_name` | String | Phase name as entered in the CMS ('Regular season 2026-27'). |
| `home_competition_team_id` | Int64 | Home side. Competition-team id (key space: sportpress competition teams): a team in ONE competition, so the same club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). Joins competitors, rosters, box scores, film and standings. The stable club key across seasons is `uu_team_id`. |
| `home_team` | String | Home side. Display name incl. the sponsor. Teams of the playoff phases carry a ' playoff' suffix (2025-26), and standings names vary with X-Localization ('Partena Professional Okapi Aalst Playoff' (en) vs 'Okapi Aalst Playoff' (nl, fr)): never join on it. |
| `home_team_short_name` | String | Home side. Three-letter club code (CHA, OOS, ANT, MON, LIM, ...), stable across seasons in the samples. |
| `home_score` | Int64 | Home side. Final points of the side, including overtime. |
| `away_competition_team_id` | Int64 | Away side. Competition-team id (key space: sportpress competition teams): a team in ONE competition, so the same club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). Joins competitors, rosters, box scores, film and standings. The stable club key across seasons is `uu_team_id`. |
| `away_team` | String | Away side. Display name incl. the sponsor. Teams of the playoff phases carry a ' playoff' suffix (2025-26), and standings names vary with X-Localization ('Partena Professional Okapi Aalst Playoff' (en) vs 'Okapi Aalst Playoff' (nl, fr)): never join on it. |
| `away_team_short_name` | String | Away side. Three-letter club code (CHA, OOS, ANT, MON, LIM, ...), stable across seasons in the samples. |
| `away_score` | Int64 | Away side. Final points of the side, including overtime. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_game_slider-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_game_slider-example}

```python
sportpress_game_slider(competition_id=25)
```

## sportpress_game_find

One game: teams, scores, phase, venue and the neighbouring game ids.

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/schedule/find/{game_id}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/schedule/find/11295](https://bnxt.sportpress.info/api/v1/schedule/find/11295)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. Game id (integer), unique across seasons; from ``sportpress_schedule`` (``game_id``). |

### Returns {#sportpress_game_find-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id (key space: sportpress games), unique across seasons: the key of every game route. |
| `game_time` | String | Tip-off as a naive datetime. The source carries no offset; the league's local time is assumed and unverified. |
| `status` | String | Game status: finished, or ready for an upcoming game. |
| `competition_id` | Int64 | Competition id (key space: sportpress competitions): ONE season of the league (24 = 2025-26, 25 = 2026-27). Not the season number. The key of the standings, ranking, slider and box routes. |
| `season` | Int64 | Season END year (2026 = 2025-26), the sdv convention and the API's own season key. |
| `phase_id` | Int64 | Phase id (key space: sportpress phases), a stage of one competition such as the regular season or the playoffs; new every season. |
| `phase_name` | String | Phase name as entered in the CMS ('Regular season 2026-27'). |
| `round_name` | String | Round label as entered in the CMS; often empty. |
| `round_priority` | Int64 | Sort order of the round within the phase (provider's priority); meaning of the values is unverified. |
| `period_name` | String | Name of the period the game is in or ended in; 'Extra2' means the second overtime. |
| `arena` | String | Name of the arena where the game is played. |
| `home_competition_team_id` | Int64 | Home side. Competition-team id (key space: sportpress competition teams): a team in ONE competition, so the same club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). Joins competitors, rosters, box scores, film and standings. The stable club key across seasons is `uu_team_id`. |
| `home_team` | String | Home side. Display name incl. the sponsor. Teams of the playoff phases carry a ' playoff' suffix (2025-26), and standings names vary with X-Localization ('Partena Professional Okapi Aalst Playoff' (en) vs 'Okapi Aalst Playoff' (nl, fr)): never join on it. |
| `home_team_short_name` | String | Home side. Three-letter club code (CHA, OOS, ANT, MON, LIM, ...), stable across seasons in the samples. |
| `home_score` | Int64 | Home side. Final points of the side, including overtime. |
| `away_competition_team_id` | Int64 | Away side. Competition-team id (key space: sportpress competition teams): a team in ONE competition, so the same club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). Joins competitors, rosters, box scores, film and standings. The stable club key across seasons is `uu_team_id`. |
| `away_team` | String | Away side. Display name incl. the sponsor. Teams of the playoff phases carry a ' playoff' suffix (2025-26), and standings names vary with X-Localization ('Partena Professional Okapi Aalst Playoff' (en) vs 'Okapi Aalst Playoff' (nl, fr)): never join on it. |
| `away_team_short_name` | String | Away side. Three-letter club code (CHA, OOS, ANT, MON, LIM, ...), stable across seasons in the samples. |
| `away_score` | Int64 | Away side. Final points of the side, including overtime. |
| `last_game_id` | Int64 | Latest finished meeting of the same two teams in the competition, which can be LATER than this game; null when none. |
| `next_game_id` | Int64 | Next meeting of the same two teams; null in every sample. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_game_find-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_game_find-example}

```python
sportpress_game_find(game_id=11295)
```

## sportpress_game_periods

Points each side scored in each period of a game (periods 5 and up are overtimes).

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/schedule/find/{game_id}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/schedule/find/11295](https://bnxt.sportpress.info/api/v1/schedule/find/11295)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. Game id (integer), unique across seasons; from ``sportpress_schedule`` (``game_id``). |

### Returns {#sportpress_game_periods-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id (key space: sportpress games), unique across seasons: the key of every game route. |
| `team_side` | String | Side of the game the row belongs to: home or away (the provider's is_home, or side 1 / 2). |
| `competition_team_id` | Int64 | Competition-team id of the side. |
| `period` | Int64 | Period number: 1-4 are the quarters, 5 and up are overtimes (5 = first overtime). |
| `score` | Int64 | Points the side scored IN that period (not cumulative). |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_game_periods-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_game_periods-example}

```python
sportpress_game_periods(game_id=11295)
```

## sportpress_game_overview

Team totals of both sides of a finished game.

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/schedule/overview/{game_id}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/schedule/overview/11295](https://bnxt.sportpress.info/api/v1/schedule/overview/11295)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. Game id (integer), unique across seasons; from ``sportpress_schedule`` (``game_id``). |

### Returns {#sportpress_game_overview-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id the overview was asked for (the body does not echo it). |
| `team_side` | String | Side of the game the row belongs to: home or away (the provider's is_home, or side 1 / 2). |
| `competition_team_id` | Int64 | Competition-team id (key space: sportpress competition teams): a team in ONE competition, so a club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). uu_team_id is the stable club key. |
| `team` | String | Team display name including the sponsor; varies with language and playoff suffixes: never join on it. |
| `team_short_name` | String | Three-letter club code (ANT, MON, ...), stable across seasons in the samples. |
| `score` | Int64 | Final score of the side. |
| `minute` | Int64 | The team's total: minutes played; for a team total, the sum over its players (200 + 25 per overtime). |
| `foul` | Int64 | The team's total: personal fouls committed. |
| `defensive_foul` | Int64 | The team's total: fouls DRAWN (suffered), despite the name: equals the opponent's foul count up to scorer corrections. |
| `dunk_made` | Int64 | The team's total: dunks made (a subset of the two-point field goals). |
| `dunk_all` | Int64 | The team's total: dunks attempted. |
| `dunk_percent` | Float64 | The team's total: dunk percentage 0-100; 0 when none were attempted. |
| `two_point_made` | Int64 | The team's total: two-point field goals made. |
| `two_point_all` | Int64 | The team's total: two-point field goals attempted. |
| `two_point_percent` | Float64 | The team's total: two-point percentage 0-100. |
| `three_point_made` | Int64 | The team's total: three-point field goals made. |
| `three_point_all` | Int64 | The team's total: three-point field goals attempted. |
| `three_point_percent` | Float64 | The team's total: three-point percentage 0-100. |
| `throw_made` | Int64 | The team's total: all field goals made, two- plus three-pointers (the provider calls them 'throw'). |
| `throw_all` | Int64 | The team's total: all field goals attempted, two- plus three-pointers (the provider calls them 'throw'). |
| `throw_percent` | Float64 | The team's total: field-goal percentage 0-100. |
| `free_throw_made` | Int64 | The team's total: free throws made. |
| `free_throw_all` | Int64 | The team's total: free throws attempted. |
| `free_throw_percent` | Float64 | The team's total: free-throw percentage 0-100. |
| `offensive_rebound` | Int64 | The team's total: offensive rebounds. |
| `defensive_rebound` | Int64 | The team's total: defensive rebounds. |
| `total_rebound` | Int64 | The team's total: total rebounds (offensive plus defensive). |
| `block` | Int64 | The team's total: shots blocked by the player. |
| `block_against` | Int64 | The team's total: own shots blocked by an opponent. |
| `turnover` | Int64 | The team's total: turnovers committed. |
| `steal` | Int64 | The team's total: steals (balls taken from an opponent). |
| `assist` | Int64 | The team's total: assists (passes leading directly to a made basket). |
| `points` | Int64 | The team's total: points scored (2 x two-pointers + 3 x three-pointers + free throws). |
| `value` | Int64 | The team's total: efficiency rating (PIR with fouls drawn): points + rebounds + assists + steals + blocks + fouls drawn - missed field goals - missed free throws - turnovers. |
| `plusminus` | Int64 | The team's total: plus/minus: the score margin while the player (or team) was on court; for a team total it is the game margin. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_game_overview-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_game_overview-example}

```python
sportpress_game_overview(game_id=11295)
```

## sportpress_game_top_performers

The top performer of each side of a finished game, with his full line.

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/schedule/overview/{game_id}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/schedule/overview/11295](https://bnxt.sportpress.info/api/v1/schedule/overview/11295)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. Game id (integer), unique across seasons; from ``sportpress_schedule`` (``game_id``). |

### Returns {#sportpress_game_top_performers-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id the overview was asked for (the body does not echo it). |
| `team_side` | String | Side of the game the row belongs to: home or away (the provider's is_home, or side 1 / 2). |
| `competition_team_id` | Int64 | Competition-team id (key space: sportpress competition teams): a team in ONE competition, so a club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). uu_team_id is the stable club key. |
| `team` | String | Team display name including the sponsor; varies with language and playoff suffixes: never join on it. |
| `team_short_name` | String | Three-letter club code (ANT, MON, ...), stable across seasons in the samples. |
| `player_id` | Int64 | Player id (key space: sportpress persons): stable across seasons and teams, distinct from roster_id. Not a Genius or FIBA person id. |
| `first_name` | String | Given name of the player as entered in the CMS. |
| `last_name` | String | Family name of the player as entered in the CMS. |
| `nationality_code` | String | Nationality code in FIBA style (USA, BEL, NED): DIRTY, with lower-case variants, '/'-joined pairs (USA/BEL), the empty string and 'HOL'; normalise before joining. |
| `birth_date` | String | Date of birth as shown on the public player page; null when unknown. |
| `jersey` | String | Shirt number as text. |
| `starter` | Boolean | True when the player started the game. |
| `minute` | Int64 | Minutes played; for a team total, the sum over its players (200 + 25 per overtime). |
| `foul` | Int64 | Personal fouls committed. |
| `defensive_foul` | Int64 | Fouls DRAWN (suffered), despite the name: equals the opponent's foul count up to scorer corrections. |
| `dunk_made` | Int64 | Dunks made (a subset of the two-point field goals). |
| `dunk_all` | Int64 | Dunks attempted. |
| `dunk_percent` | Float64 | Dunk percentage 0-100; 0 when none were attempted. |
| `two_point_made` | Int64 | Two-point field goals made. |
| `two_point_all` | Int64 | Two-point field goals attempted. |
| `two_point_percent` | Float64 | Two-point percentage 0-100. |
| `three_point_made` | Int64 | Three-point field goals made. |
| `three_point_all` | Int64 | Three-point field goals attempted. |
| `three_point_percent` | Float64 | Three-point percentage 0-100. |
| `throw_made` | Int64 | All field goals made, two- plus three-pointers (the provider calls them 'throw'). |
| `throw_all` | Int64 | All field goals attempted, two- plus three-pointers (the provider calls them 'throw'). |
| `throw_percent` | Float64 | Field-goal percentage 0-100. |
| `free_throw_made` | Int64 | Free throws made. |
| `free_throw_all` | Int64 | Free throws attempted. |
| `free_throw_percent` | Float64 | Free-throw percentage 0-100. |
| `offensive_rebound` | Int64 | Offensive rebounds. |
| `defensive_rebound` | Int64 | Defensive rebounds. |
| `total_rebound` | Int64 | Total rebounds (offensive plus defensive). |
| `block` | Int64 | Shots blocked by the player. |
| `block_against` | Int64 | Own shots blocked by an opponent. |
| `turnover` | Int64 | Turnovers committed. |
| `steal` | Int64 | Steals (balls taken from an opponent). |
| `assist` | Int64 | Assists (passes leading directly to a made basket). |
| `points` | Int64 | Points scored (2 x two-pointers + 3 x three-pointers + free throws). |
| `value` | Int64 | Efficiency rating (PIR with fouls drawn): points + rebounds + assists + steals + blocks + fouls drawn - missed field goals - missed free throws - turnovers. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_game_top_performers-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_game_top_performers-example}

```python
sportpress_game_top_performers(game_id=11295)
```

## sportpress_game_leaders

The game leader of each side in every statistic (one row per side and statistic).

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/schedule/overview/{game_id}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/schedule/overview/11295](https://bnxt.sportpress.info/api/v1/schedule/overview/11295)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. Game id (integer), unique across seasons; from ``sportpress_schedule`` (``game_id``). |

### Returns {#sportpress_game_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id the overview was asked for (the body does not echo it). |
| `team_side` | String | Side of the game the row belongs to: home or away (the provider's is_home, or side 1 / 2). |
| `competition_team_id` | Int64 | Competition-team id (key space: sportpress competition teams): a team in ONE competition, so a club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). uu_team_id is the stable club key. |
| `team` | String | Team display name including the sponsor; varies with language and playoff suffixes: never join on it. |
| `team_short_name` | String | Three-letter club code (ANT, MON, ...), stable across seasons in the samples. |
| `stat` | String | Statistic the leader leads, in the provider's column name (points, total_rebound, value, ...). |
| `player_id` | Int64 | Player id (key space: sportpress persons): stable across seasons and teams, distinct from roster_id. Not a Genius or FIBA person id. |
| `first_name` | String | Given name of the player as entered in the CMS. |
| `last_name` | String | Family name of the player as entered in the CMS. |
| `nationality_code` | String | Nationality code in FIBA style (USA, BEL, NED): DIRTY, with lower-case variants, '/'-joined pairs (USA/BEL), the empty string and 'HOL'; normalise before joining. |
| `birth_date` | String | Date of birth as shown on the public player page; null when unknown. |
| `value` | Float64 | The leader's count (or percentage) in that statistic in this game. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_game_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_game_leaders-example}

```python
sportpress_game_leaders(game_id=11295)
```

## sportpress_game_box

Per-player box score of a finished game (one row per player line).

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/boxscore/game/{competition_id}/{game_id}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/boxscore/game/25/11295](https://bnxt.sportpress.info/api/v1/boxscore/game/25/11295)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_id` | `competition_id` |  | `Y` |  | Required. Competition id (integer), ONE per season of the league: 24 = 2025-26, 25 = 2026-27 (``sportpress_phases`` maps a season to it). |
| `game_id` | `game_id` |  | `Y` |  | Required. Game id (integer), unique across seasons; from ``sportpress_schedule`` (``game_id``). |

### Returns {#sportpress_game_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id the box score was asked for (the body does not echo it). |
| `competition_id` | Int64 | Competition id the box score was asked for; checked against the game. |
| `team_side` | String | Side of the game the row belongs to: home or away (the provider's is_home, or side 1 / 2). |
| `competition_team_id` | Int64 | Competition-team id (key space: sportpress competition teams): a team in ONE competition, so a club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). uu_team_id is the stable club key. |
| `team` | String | Team display name including the sponsor; varies with language and playoff suffixes: never join on it. |
| `team_short_name` | String | Three-letter club code (ANT, MON, ...), stable across seasons in the samples. |
| `roster_id` | Int64 | Roster id of the line; null when the line has no player (15 of 14,142 sampled lines). |
| `player_id` | Int64 | Player id (key space: sportpress persons): stable across seasons and teams, distinct from roster_id. Not a Genius or FIBA person id. |
| `first_name` | String | Given name of the player as entered in the CMS. |
| `last_name` | String | Family name of the player as entered in the CMS. |
| `nationality_code` | String | Nationality code in FIBA style (USA, BEL, NED): DIRTY, with lower-case variants, '/'-joined pairs (USA/BEL), the empty string and 'HOL'; normalise before joining. |
| `birth_date` | String | Date of birth as shown on the public player page; null when unknown. |
| `position_id` | Int64 | Playing position id (key space: sportpress positions); null on 21 of 14,142 sampled lines. |
| `position` | String | Playing position: point_guard, shooting_guard, small_forward, power_forward or center. |
| `starter` | Boolean | True when the player was in the starting five. |
| `jersey` | String | Shirt number as text; the box score sends it as an integer, so a zero-padded '00' is already lost there. |
| `minute` | Int64 | Minutes played; for a team total, the sum over its players (200 + 25 per overtime). |
| `foul` | Int64 | Personal fouls committed. |
| `defensive_foul` | Int64 | Fouls DRAWN (suffered), despite the name: equals the opponent's foul count up to scorer corrections. |
| `dunk_made` | Int64 | Dunks made (a subset of the two-point field goals). |
| `dunk_all` | Int64 | Dunks attempted. |
| `dunk_percent` | Float64 | Dunk percentage 0-100; 0 when none were attempted. |
| `two_point_made` | Int64 | Two-point field goals made. |
| `two_point_all` | Int64 | Two-point field goals attempted. |
| `two_point_percent` | Float64 | Two-point percentage 0-100. |
| `three_point_made` | Int64 | Three-point field goals made. |
| `three_point_all` | Int64 | Three-point field goals attempted. |
| `three_point_percent` | Float64 | Three-point percentage 0-100. |
| `throw_made` | Int64 | All field goals made, two- plus three-pointers (the provider calls them 'throw'). |
| `throw_all` | Int64 | All field goals attempted, two- plus three-pointers (the provider calls them 'throw'). |
| `throw_percent` | Float64 | Field-goal percentage 0-100. |
| `free_throw_made` | Int64 | Free throws made. |
| `free_throw_all` | Int64 | Free throws attempted. |
| `free_throw_percent` | Float64 | Free-throw percentage 0-100. |
| `offensive_rebound` | Int64 | Offensive rebounds. |
| `defensive_rebound` | Int64 | Defensive rebounds. |
| `total_rebound` | Int64 | Total rebounds (offensive plus defensive). |
| `block` | Int64 | Shots blocked by the player. |
| `block_against` | Int64 | Own shots blocked by an opponent. |
| `turnover` | Int64 | Turnovers committed. |
| `steal` | Int64 | Steals (balls taken from an opponent). |
| `assist` | Int64 | Assists (passes leading directly to a made basket). |
| `points` | Int64 | Points scored (2 x two-pointers + 3 x three-pointers + free throws). |
| `value` | Int64 | Efficiency rating (PIR with fouls drawn): points + rebounds + assists + steals + blocks + fouls drawn - missed field goals - missed free throws - turnovers. |
| `plusminus` | Int64 | Plus/minus: the score margin while the player (or team) was on court; for a team total it is the game margin. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_game_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_game_box-example}

```python
sportpress_game_box(competition_id=25, game_id=11295)
```

## sportpress_game_team_box

Team totals of both sides from the box score of a finished game.

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/boxscore/game/{competition_id}/{game_id}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/boxscore/game/25/11295](https://bnxt.sportpress.info/api/v1/boxscore/game/25/11295)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_id` | `competition_id` |  | `Y` |  | Required. Competition id (integer), ONE per season of the league: 24 = 2025-26, 25 = 2026-27 (``sportpress_phases`` maps a season to it). |
| `game_id` | `game_id` |  | `Y` |  | Required. Game id (integer), unique across seasons; from ``sportpress_schedule`` (``game_id``). |

### Returns {#sportpress_game_team_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id the box score was asked for (the body does not echo it). |
| `competition_id` | Int64 | Competition id the box score was asked for; checked against the game. |
| `team_side` | String | Side of the game the row belongs to: home or away (the provider's is_home, or side 1 / 2). |
| `competition_team_id` | Int64 | Competition-team id (key space: sportpress competition teams): a team in ONE competition, so a club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). uu_team_id is the stable club key. |
| `team` | String | Team display name including the sponsor; varies with language and playoff suffixes: never join on it. |
| `team_short_name` | String | Three-letter club code (ANT, MON, ...), stable across seasons in the samples. |
| `minute` | Int64 | The team's total: minutes played; for a team total, the sum over its players (200 + 25 per overtime). |
| `foul` | Int64 | The team's total: personal fouls committed. |
| `defensive_foul` | Int64 | The team's total: fouls DRAWN (suffered), despite the name: equals the opponent's foul count up to scorer corrections. |
| `dunk_made` | Int64 | The team's total: dunks made (a subset of the two-point field goals). |
| `dunk_all` | Int64 | The team's total: dunks attempted. |
| `dunk_percent` | Float64 | The team's total: dunk percentage 0-100; 0 when none were attempted. |
| `two_point_made` | Int64 | The team's total: two-point field goals made. |
| `two_point_all` | Int64 | The team's total: two-point field goals attempted. |
| `two_point_percent` | Float64 | The team's total: two-point percentage 0-100. |
| `three_point_made` | Int64 | The team's total: three-point field goals made. |
| `three_point_all` | Int64 | The team's total: three-point field goals attempted. |
| `three_point_percent` | Float64 | The team's total: three-point percentage 0-100. |
| `throw_made` | Int64 | The team's total: all field goals made, two- plus three-pointers (the provider calls them 'throw'). |
| `throw_all` | Int64 | The team's total: all field goals attempted, two- plus three-pointers (the provider calls them 'throw'). |
| `throw_percent` | Float64 | The team's total: field-goal percentage 0-100. |
| `free_throw_made` | Int64 | The team's total: free throws made. |
| `free_throw_all` | Int64 | The team's total: free throws attempted. |
| `free_throw_percent` | Float64 | The team's total: free-throw percentage 0-100. |
| `offensive_rebound` | Int64 | The team's total: offensive rebounds. |
| `defensive_rebound` | Int64 | The team's total: defensive rebounds. |
| `total_rebound` | Int64 | The team's total: total rebounds (offensive plus defensive). |
| `block` | Int64 | The team's total: shots blocked by the player. |
| `block_against` | Int64 | The team's total: own shots blocked by an opponent. |
| `turnover` | Int64 | The team's total: turnovers committed. |
| `steal` | Int64 | The team's total: steals (balls taken from an opponent). |
| `assist` | Int64 | The team's total: assists (passes leading directly to a made basket). |
| `points` | Int64 | Team points in the box score; can differ from the schedule final score (about 18 of 1,246 sampled team boxes, by 2-21 points). |
| `value` | Int64 | The team's total: efficiency rating (PIR with fouls drawn): points + rebounds + assists + steals + blocks + fouls drawn - missed field goals - missed free throws - turnovers. |
| `plusminus` | Int64 | The team's total: plus/minus: the score margin while the player (or team) was on court; for a team total it is the game margin. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_game_team_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_game_team_box-example}

```python
sportpress_game_team_box(competition_id=25, game_id=11295)
```

## sportpress_game_film

Play-by-play ('film') of a finished game with court x/y (2021-22 and 2026-27 only).

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/film/game/{competition_id}/{game_id}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/film/game/25/11295](https://bnxt.sportpress.info/api/v1/film/game/25/11295)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_id` | `competition_id` |  | `Y` |  | Required. Competition id (integer), ONE per season of the league: 24 = 2025-26, 25 = 2026-27 (``sportpress_phases`` maps a season to it). |
| `game_id` | `game_id` |  | `Y` |  | Required. Game id (integer), unique across seasons; from ``sportpress_schedule`` (``game_id``). |

### Returns {#sportpress_game_film-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id the film was asked for (the body does not echo it). |
| `competition_id` | Int64 | Competition id the film was asked for; checked against the game. |
| `order` | Int64 | Event sequence number in the game, strictly increasing from 0. |
| `period` | Int64 | Period of the event: 1-4 quarters, 5 and up overtimes. |
| `minute` | Int64 | Whole minutes ELAPSED in the period (not clock remaining). |
| `second` | Int64 | Seconds elapsed within that minute. |
| `team_side` | String | Side of the game the row belongs to: home or away (the provider's is_home, or side 1 / 2). |
| `competition_team_id` | Int64 | Competition-team id (key space: sportpress competition teams): a team in ONE competition, so a club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). uu_team_id is the stable club key. |
| `team` | String | Team display name including the sponsor; varies with language and playoff suffixes: never join on it. |
| `team_short_name` | String | Three-letter club code (ANT, MON, ...), stable across seasons in the samples. |
| `event_code` | Int64 | Provider event code: 1000 made shot, 1001 missed shot, 1002 defensive rebound, 1003 offensive rebound, 1004 steal, 1005 turnover, 1006 foul drawn, 1007 foul committed, 1008 assist, 1009 block, 1011 substitution, 1101 period marker; 2xxx team events (meaning unverified). |
| `event_subcode` | Int64 | Provider sub-type of the event: for shots 1, 2 and 5 = two-pointer, 3 = three-pointer, 4 = free throw; turnover and foul sub-types are unnamed by the provider. |
| `x` | Int64 | Court x position of the event in the provider's integer court coordinates (0 on events with none); the coordinate system and orientation are unverified. |
| `y` | Int64 | Court y position of the event in the provider's integer court coordinates (0 on events with none); the coordinate system and orientation are unverified. |
| `home_score` | Int64 | Running home score AFTER the event; the maximum is the final score. |
| `away_score` | Int64 | Running away score AFTER the event; the maximum is the final score. |
| `player_id` | Int64 | Player id of the actor; null on team events and period markers. |
| `first_name` | String | Given name of the player as entered in the CMS. |
| `last_name` | String | Family name of the player as entered in the CMS. |
| `nationality_code` | String | Nationality code in FIBA style (USA, BEL, NED): DIRTY, with lower-case variants, '/'-joined pairs (USA/BEL), the empty string and 'HOL'; normalise before joining. |
| `birth_date` | String | Date of birth as shown on the public player page; null when unknown. |
| `player2_id` | Int64 | Player id of the second player; filled on substitutions only. |
| `player2_first_name` | String | Given name of the second player; substitutions only. |
| `player2_last_name` | String | Family name of the second player; substitutions only. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_game_film-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_game_film-example}

```python
sportpress_game_film(competition_id=25, game_id=11295)
```
