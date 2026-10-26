---
title: "BNXT League — BNXT League Sportpress API (bnxt.sportpress.info, Belgium and the Netherlands) — Team"
sidebar_label: "Team"
sidebar_position: 3
description: "BNXT League — BNXT League Sportpress API (bnxt.sportpress.info, Belgium and the Netherlands) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# BNXT League — BNXT League Sportpress API (bnxt.sportpress.info, Belgium and the Netherlands) — Team

## sportpress_team_rankings

Team per-game leaders in nine categories (one row per team and category).

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/competition-team/average-rankings/{competition_id}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/competition-team/average-rankings/24](https://bnxt.sportpress.info/api/v1/competition-team/average-rankings/24)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_id` | `competition_id` |  | `Y` |  | Required. Competition id (integer), ONE per season of the league: 24 = 2025-26, 25 = 2026-27 (``sportpress_phases`` maps a season to it). |
| `phase_id` | `phase_id` |  |  | `Y` | Restrict to one phase (integer), from ``sportpress_phases``. Omitted = every phase. |
| `limit` | `limit` |  |  | `Y` | Cap each ranking list at this many rows. Omitted = the complete lists. |

### Returns {#sportpress_team_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | Competition id (key space: sportpress competitions): ONE season of the league (24 = 2025-26, 25 = 2026-27). Not the season number. The key of the standings, ranking, slider and box routes. |
| `category` | String | Ranking category: efficiency, points, total_rebound, block, assist, steal, two_point_percent, three_point_percent or free_throw_percent. |
| `rank` | Int64 | Place in the category list, 1 = the leader (the list is sorted best first). |
| `competition_team_id` | Int64 | Competition-team id (key space: sportpress competition teams): a team in ONE competition, so a club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). uu_team_id is the stable club key. |
| `team` | String | Team display name including the sponsor; varies with language and playoff suffixes: never join on it. |
| `team_short_name` | String | Three-letter club code (ANT, MON, ...), stable across seasons in the samples. |
| `value` | Float64 | The team's per-game mean (or percentage 0-100) in the row's category. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_team_rankings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_team_rankings-example}

```python
sportpress_team_rankings(competition_id=24)
```

## sportpress_team_roster

Roster of one competition team (current entries only).

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/roster/team-players/{competition_team_id}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/roster/team-players/541](https://bnxt.sportpress.info/api/v1/roster/team-players/541)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_team_id` | `competition_team_id` |  | `Y` |  | Required. Competition-team id (integer): a team in ONE competition, so new every season (e.g. 541 = Antwerp in 2026-27); from ``sportpress_competition_teams``. |

### Returns {#sportpress_team_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `roster_id` | Int64 | Roster id (key space: sportpress roster entries): one player on one competition team in one season, so a player has a new roster_id for every team and season. player_id is the person. |
| `competition_team_id` | Int64 | Competition-team id (key space: sportpress competition teams): a team in ONE competition, so a club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). uu_team_id is the stable club key. |
| `jersey` | String | Shirt number as text; the box score sends it as an integer, so a zero-padded '00' is already lost there. |
| `weight` | Int64 | Weight in kg; null when unknown. |
| `height` | Int64 | Height in cm; null when unknown. |
| `active` | Boolean | True for a current roster entry; the lists hold current entries only. |
| `position_id` | Int64 | Position id (key space: sportpress positions; 3 = small_forward). |
| `position` | String | Position code: point_guard, shooting_guard, small_forward, power_forward, center, and the hybrids point_guard-shooting_guard, shooting_guard-small_forward, small_forward-power_forward, power_forward-center. |
| `player_id` | Int64 | Player id (key space: sportpress persons): stable across seasons and teams, distinct from roster_id. Not a Genius or FIBA person id. |
| `first_name` | String | Given name of the player as entered in the CMS. |
| `last_name` | String | Family name of the player as entered in the CMS. |
| `nationality_code` | String | Nationality code in FIBA style (USA, BEL, NED): DIRTY, with lower-case variants, '/'-joined pairs (USA/BEL), the empty string and 'HOL'; normalise before joining. |
| `birth_date` | String | Date of birth as shown on the public player page; null when unknown. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_team_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_team_roster-example}

```python
sportpress_team_roster(competition_team_id=541)
```

## sportpress_team_find

One competition team with its season scoring averages.

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/competition-team/find/{competition_team_id}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/competition-team/find/541](https://bnxt.sportpress.info/api/v1/competition-team/find/541)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_team_id` | `competition_team_id` |  | `Y` |  | Required. Competition-team id (integer): a team in ONE competition, so new every season (e.g. 541 = Antwerp in 2026-27); from ``sportpress_competition_teams``. |

### Returns {#sportpress_team_find-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | Competition id (key space: sportpress competitions): ONE season of the league (24 = 2025-26, 25 = 2026-27). Not the season number. The key of the standings, ranking, slider and box routes. |
| `season` | Int64 | Season END year of the team's competition. |
| `competition_name` | String | Competition name, 'BNXT League 2026-2027'. |
| `uu_team_id` | Int64 | Stable club key across seasons (key space: sportpress clubs-as-teams); competition_team_id changes every season, this does not. |
| `competition_team_id` | Int64 | Competition-team id (key space: sportpress competition teams): a team in ONE competition, so a club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). uu_team_id is the stable club key. |
| `team` | String | Team display name including the sponsor; varies with language and playoff suffixes: never join on it. |
| `team_short_name` | String | Three-letter club code (ANT, MON, ...), stable across seasons in the samples. |
| `club_id` | Int64 | Country of the team (key space: sportpress clubs): 1 = Belgium, 2 = Netherlands. The provider calls the country a club. |
| `club_name` | String | Country name in English, Belgium or Netherlands. |
| `average_score_for` | Float64 | Points scored per game by the team over the season; null for a team with no statistics. |
| `average_score_against` | Float64 | Points conceded per game; null for a team with no statistics. |
| `field_goal_percent` | Float64 | Team field-goal percentage 0-100; null for a team with no statistics. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_team_find-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_team_find-example}

```python
sportpress_team_find(competition_team_id=541)
```

## sportpress_team_player_averages

Per-game averages of each player of a competition team.

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/competition-team/player-averages/{competition_team_id}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/competition-team/player-averages/541](https://bnxt.sportpress.info/api/v1/competition-team/player-averages/541)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_team_id` | `competition_team_id` |  | `Y` |  | Required. Competition-team id (integer): a team in ONE competition, so new every season (e.g. 541 = Antwerp in 2026-27); from ``sportpress_competition_teams``. |

### Returns {#sportpress_team_player_averages-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_team_id` | Int64 | Competition-team id (key space: sportpress competition teams): a team in ONE competition, so a club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). uu_team_id is the stable club key. |
| `roster_id` | Int64 | Roster id (key space: sportpress roster entries): one player on one competition team in one season, so a player has a new roster_id for every team and season. player_id is the person. |
| `player_id` | Int64 | Player id (key space: sportpress persons): stable across seasons and teams, distinct from roster_id. Not a Genius or FIBA person id. |
| `first_name` | String | Given name of the player as entered in the CMS. |
| `last_name` | String | Family name of the player as entered in the CMS. |
| `nationality_code` | String | Nationality code in FIBA style (USA, BEL, NED): DIRTY, with lower-case variants, '/'-joined pairs (USA/BEL), the empty string and 'HOL'; normalise before joining. |
| `birth_date` | String | Date of birth as shown on the public player page; null when unknown. |
| `played_games` | Int64 | Games the player played in the season. |
| `games_started` | Int64 | Games started (a count, not a mean). |
| `minute` | Float64 | Minutes played; for a team total, the sum over its players (200 + 25 per overtime). Per-game mean over the games played (fractional). |
| `foul` | Float64 | Personal fouls committed. Per-game mean over the games played (fractional). |
| `defensive_foul` | Float64 | Fouls DRAWN (suffered), despite the name: equals the opponent's foul count up to scorer corrections. Per-game mean over the games played (fractional). |
| `dunk_made` | Float64 | Dunks made (a subset of the two-point field goals). Per-game mean over the games played (fractional). |
| `dunk_all` | Float64 | Dunks attempted. Per-game mean over the games played (fractional). |
| `dunk_percent` | Float64 | Dunk percentage 0-100; 0 when none were attempted. Per-game mean over the games played (fractional). |
| `two_point_made` | Float64 | Two-point field goals made. Per-game mean over the games played (fractional). |
| `two_point_all` | Float64 | Two-point field goals attempted. Per-game mean over the games played (fractional). |
| `two_point_percent` | Float64 | Two-point percentage 0-100. Per-game mean over the games played (fractional). |
| `three_point_made` | Float64 | Three-point field goals made. Per-game mean over the games played (fractional). |
| `three_point_all` | Float64 | Three-point field goals attempted. Per-game mean over the games played (fractional). |
| `three_point_percent` | Float64 | Three-point percentage 0-100. Per-game mean over the games played (fractional). |
| `throw_made` | Float64 | All field goals made, two- plus three-pointers (the provider calls them 'throw'). Per-game mean over the games played (fractional). |
| `throw_all` | Float64 | All field goals attempted, two- plus three-pointers (the provider calls them 'throw'). Per-game mean over the games played (fractional). |
| `throw_percent` | Float64 | Field-goal percentage 0-100. Per-game mean over the games played (fractional). |
| `free_throw_made` | Float64 | Free throws made. Per-game mean over the games played (fractional). |
| `free_throw_all` | Float64 | Free throws attempted. Per-game mean over the games played (fractional). |
| `free_throw_percent` | Float64 | Free-throw percentage 0-100. Per-game mean over the games played (fractional). |
| `offensive_rebound` | Float64 | Offensive rebounds. Per-game mean over the games played (fractional). |
| `defensive_rebound` | Float64 | Defensive rebounds. Per-game mean over the games played (fractional). |
| `total_rebound` | Float64 | Total rebounds (offensive plus defensive). Per-game mean over the games played (fractional). |
| `block` | Float64 | Shots blocked by the player. Per-game mean over the games played (fractional). |
| `block_against` | Float64 | Own shots blocked by an opponent. Per-game mean over the games played (fractional). |
| `turnover` | Float64 | Turnovers committed. Per-game mean over the games played (fractional). |
| `steal` | Float64 | Steals (balls taken from an opponent). Per-game mean over the games played (fractional). |
| `assist` | Float64 | Assists (passes leading directly to a made basket). Per-game mean over the games played (fractional). |
| `points` | Float64 | Points scored (2 x two-pointers + 3 x three-pointers + free throws). Per-game mean over the games played (fractional). |
| `value` | Float64 | Efficiency rating (PIR with fouls drawn): points + rebounds + assists + steals + blocks + fouls drawn - missed field goals - missed free throws - turnovers. Per-game mean over the games played (fractional). |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_team_player_averages-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_team_player_averages-example}

```python
sportpress_team_player_averages(competition_team_id=541)
```

## sportpress_team_games

Every finished game of a competition team with that team's totals.

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/competition-team/statistics-by-game/total/{competition_team_id}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/competition-team/statistics-by-game/total/541](https://bnxt.sportpress.info/api/v1/competition-team/statistics-by-game/total/541)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_team_id` | `competition_team_id` |  | `Y` |  | Required. Competition-team id (integer): a team in ONE competition, so new every season (e.g. 541 = Antwerp in 2026-27); from ``sportpress_competition_teams``. |

### Returns {#sportpress_team_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_team_id` | Int64 | Competition-team id the games were asked for (the team the totals belong to). |
| `game_id` | Int64 | Game id (key space: sportpress games), unique across seasons: the key of every game route. |
| `game_time` | String | Tip-off as a naive datetime. The source carries no offset; the league's local time is assumed and unverified. |
| `status` | String | Game status: finished, or ready for an upcoming game. |
| `competition_id` | Int64 | Competition id (key space: sportpress competitions): ONE season of the league (24 = 2025-26, 25 = 2026-27). Not the season number. The key of the standings, ranking, slider and box routes. |
| `season` | Int64 | Season END year of the game's competition. |
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
| `minute` | Int64 | The team's own minutes played; for a team total, the sum over its players (200 + 25 per overtime). |
| `foul` | Int64 | The team's own personal fouls committed. |
| `defensive_foul` | Int64 | The team's own fouls DRAWN (suffered), despite the name: equals the opponent's foul count up to scorer corrections. |
| `dunk_made` | Int64 | The team's own dunks made (a subset of the two-point field goals). |
| `dunk_all` | Int64 | The team's own dunks attempted. |
| `dunk_percent` | Float64 | The team's own dunk percentage 0-100; 0 when none were attempted. |
| `two_point_made` | Int64 | The team's own two-point field goals made. |
| `two_point_all` | Int64 | The team's own two-point field goals attempted. |
| `two_point_percent` | Float64 | The team's own two-point percentage 0-100. |
| `three_point_made` | Int64 | The team's own three-point field goals made. |
| `three_point_all` | Int64 | The team's own three-point field goals attempted. |
| `three_point_percent` | Float64 | The team's own three-point percentage 0-100. |
| `throw_made` | Int64 | The team's own all field goals made, two- plus three-pointers (the provider calls them 'throw'). |
| `throw_all` | Int64 | The team's own all field goals attempted, two- plus three-pointers (the provider calls them 'throw'). |
| `throw_percent` | Float64 | The team's own field-goal percentage 0-100. |
| `free_throw_made` | Int64 | The team's own free throws made. |
| `free_throw_all` | Int64 | The team's own free throws attempted. |
| `free_throw_percent` | Float64 | The team's own free-throw percentage 0-100. |
| `offensive_rebound` | Int64 | The team's own offensive rebounds. |
| `defensive_rebound` | Int64 | The team's own defensive rebounds. |
| `total_rebound` | Int64 | The team's own total rebounds (offensive plus defensive). |
| `block` | Int64 | The team's own shots blocked by the player. |
| `block_against` | Int64 | The team's own own shots blocked by an opponent. |
| `turnover` | Int64 | The team's own turnovers committed. |
| `steal` | Int64 | The team's own steals (balls taken from an opponent). |
| `assist` | Int64 | The team's own assists (passes leading directly to a made basket). |
| `points` | Int64 | The team's own points scored (2 x two-pointers + 3 x three-pointers + free throws). |
| `value` | Int64 | The team's own efficiency rating (PIR with fouls drawn): points + rebounds + assists + steals + blocks + fouls drawn - missed field goals - missed free throws - turnovers. |
| `plusminus` | Int64 | The team's own plus/minus: the score margin while the player (or team) was on court; for a team total it is the game margin. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_team_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_team_games-example}

```python
sportpress_team_games(competition_team_id=541)
```
