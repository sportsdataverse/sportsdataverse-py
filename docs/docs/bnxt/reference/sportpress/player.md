---
title: "BNXT — BNXT League Sportpress API (bnxt.sportpress.info, Belgium and the Netherlands) — Player"
sidebar_label: "Player"
sidebar_position: 2
description: "BNXT — BNXT League Sportpress API (bnxt.sportpress.info, Belgium and the Netherlands) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# BNXT — BNXT League Sportpress API (bnxt.sportpress.info, Belgium and the Netherlands) — Player

## sportpress_player_rankings

Qualified-player per-game leaders in nine categories (one row per player and category).

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/player/average-rankings/{competition_id}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/player/average-rankings/24](https://bnxt.sportpress.info/api/v1/player/average-rankings/24)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_id` | `competition_id` |  | `Y` |  | Required. Competition id (integer), ONE per season of the league: 24 = 2025-26, 25 = 2026-27 (``sportpress_phases`` maps a season to it). |
| `phase_id` | `phase_id` |  |  | `Y` | Restrict to one phase (integer), from ``sportpress_phases``. Omitted = every phase. |
| `limit` | `limit` |  |  | `Y` | Cap each ranking list at this many rows. Omitted = the complete lists. |
| `type` | `type` |  |  | `Y` | Return one ranking category only (e.g. ``points``); ``all`` or omitted = the nine categories. |

### Returns {#sportpress_player_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | integer | Competition id (key space: sportpress competitions): ONE season of the league (24 = 2025-26, 25 = 2026-27). Not the season number. The key of the standings, ranking, slider and box routes. |
| `category` | character | Ranking category: efficiency, points, total_rebound, block, assist, steal, two_point_percent, three_point_percent or free_throw_percent. |
| `rank` | integer | Place in the category list, 1 = the leader (the list is sorted best first). |
| `roster_id` | integer | Roster id (key space: sportpress roster entries): one player on one competition team in one season, so a player has a new roster_id for every team and season. player_id is the person. |
| `player_id` | integer | Player id (key space: sportpress persons): stable across seasons and teams, distinct from roster_id. Not a Genius or FIBA person id. |
| `first_name` | character | Given name of the player as entered in the CMS. |
| `last_name` | character | Family name of the player as entered in the CMS. |
| `nationality_code` | character | Nationality code in FIBA style (USA, BEL, NED): DIRTY, with lower-case variants, '/'-joined pairs (USA/BEL), the empty string and 'HOL'; normalise before joining. |
| `birth_date` | character | Date of birth as shown on the public player page; null when unknown. |
| `competition_team_id` | integer | Competition-team id (key space: sportpress competition teams): a team in ONE competition, so a club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). uu_team_id is the stable club key. |
| `team` | character | Team display name including the sponsor; varies with language and playoff suffixes: never join on it. |
| `team_short_name` | character | Three-letter club code (ANT, MON, ...), stable across seasons in the samples. |
| `value` | numeric | The player's per-game mean (or percentage 0-100) in the row's category. |
| `throw_made` | numeric | Field goals made per game; filled in the three percentage categories only. |
| `throw_attempted` | numeric | Field goals attempted per game; filled in the three percentage categories only. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_player_rankings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_player_rankings-example}

```python
sportpress_player_rankings(competition_id=24)
```

## sportpress_player_career

Season-by-season per-game averages of the player of a roster entry.

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/player/career-averages/{roster_id}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/player/career-averages/11193](https://bnxt.sportpress.info/api/v1/player/career-averages/11193)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `roster_id` | `roster_id` |  | `Y` |  | Required. Roster id (integer): one player on one team in one season; from the roster frames. |

### Returns {#sportpress_player_career-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `roster_id` | integer | Roster id the career was asked for; the rows are the person's seasons, each with its own roster. |
| `competition_id` | integer | Competition id of the season row. |
| `season` | integer | Season END year of the row. |
| `competition_name` | character | Competition name of the season row. |
| `competition_team_id` | integer | Competition-team id (key space: sportpress competition teams): a team in ONE competition, so a club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). uu_team_id is the stable club key. |
| `team` | character | Team display name including the sponsor; varies with language and playoff suffixes: never join on it. |
| `team_short_name` | character | Three-letter club code (ANT, MON, ...), stable across seasons in the samples. |
| `club_id` | integer | Country of the team (key space: sportpress clubs): 1 = Belgium, 2 = Netherlands. The provider calls the country a club. |
| `club_name` | character | Country name in English, Belgium or Netherlands. |
| `position` | character | Playing position; null in every sample. |
| `jersey` | character | Shirt number as text; the box score sends it as an integer, so a zero-padded '00' is already lost there. |
| `played_games` | integer | Games played in the season. |
| `games_started` | integer | Games started (a count, not a mean). |
| `minute` | numeric | Minutes played; for a team total, the sum over its players (200 + 25 per overtime). Per-game mean over the games played (fractional). |
| `foul` | numeric | Personal fouls committed. Per-game mean over the games played (fractional). |
| `defensive_foul` | numeric | Fouls DRAWN (suffered), despite the name: equals the opponent's foul count up to scorer corrections. Per-game mean over the games played (fractional). |
| `dunk_made` | numeric | Dunks made (a subset of the two-point field goals). Per-game mean over the games played (fractional). |
| `dunk_all` | numeric | Dunks attempted. Per-game mean over the games played (fractional). |
| `dunk_percent` | numeric | Dunk percentage 0-100; 0 when none were attempted. Per-game mean over the games played (fractional). |
| `two_point_made` | numeric | Two-point field goals made. Per-game mean over the games played (fractional). |
| `two_point_all` | numeric | Two-point field goals attempted. Per-game mean over the games played (fractional). |
| `two_point_percent` | numeric | Two-point percentage 0-100. Per-game mean over the games played (fractional). |
| `three_point_made` | numeric | Three-point field goals made. Per-game mean over the games played (fractional). |
| `three_point_all` | numeric | Three-point field goals attempted. Per-game mean over the games played (fractional). |
| `three_point_percent` | numeric | Three-point percentage 0-100. Per-game mean over the games played (fractional). |
| `throw_made` | numeric | All field goals made, two- plus three-pointers (the provider calls them 'throw'). Per-game mean over the games played (fractional). |
| `throw_all` | numeric | All field goals attempted, two- plus three-pointers (the provider calls them 'throw'). Per-game mean over the games played (fractional). |
| `throw_percent` | numeric | Field-goal percentage 0-100. Per-game mean over the games played (fractional). |
| `free_throw_made` | numeric | Free throws made. Per-game mean over the games played (fractional). |
| `free_throw_all` | numeric | Free throws attempted. Per-game mean over the games played (fractional). |
| `free_throw_percent` | numeric | Free-throw percentage 0-100. Per-game mean over the games played (fractional). |
| `offensive_rebound` | numeric | Offensive rebounds. Per-game mean over the games played (fractional). |
| `defensive_rebound` | numeric | Defensive rebounds. Per-game mean over the games played (fractional). |
| `total_rebound` | numeric | Total rebounds (offensive plus defensive). Per-game mean over the games played (fractional). |
| `block` | numeric | Shots blocked by the player. Per-game mean over the games played (fractional). |
| `block_against` | numeric | Own shots blocked by an opponent. Per-game mean over the games played (fractional). |
| `turnover` | numeric | Turnovers committed. Per-game mean over the games played (fractional). |
| `steal` | numeric | Steals (balls taken from an opponent). Per-game mean over the games played (fractional). |
| `assist` | numeric | Assists (passes leading directly to a made basket). Per-game mean over the games played (fractional). |
| `points` | numeric | Points scored (2 x two-pointers + 3 x three-pointers + free throws). Per-game mean over the games played (fractional). |
| `value` | numeric | Efficiency rating (PIR with fouls drawn): points + rebounds + assists + steals + blocks + fouls drawn - missed field goals - missed free throws - turnovers. Per-game mean over the games played (fractional). |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_player_career-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_player_career-example}

```python
sportpress_player_career(roster_id=11193)
```

## sportpress_player_games

Game-by-game box lines of a roster entry (including games with 0 minutes).

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/boxscore/player/season-games/{roster_id}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/boxscore/player/season-games/11193](https://bnxt.sportpress.info/api/v1/boxscore/player/season-games/11193)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `roster_id` | `roster_id` |  | `Y` |  | Required. Roster id (integer): one player on one team in one season; from the roster frames. |

### Returns {#sportpress_player_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `roster_id` | integer | Roster id the lines were asked for. |
| `game_id` | integer | Game id (key space: sportpress games), unique across seasons: the key of every game route. |
| `game_time` | character | Tip-off as a naive datetime. The source carries no offset; the league's local time is assumed and unverified. |
| `status` | character | Game status: finished, or ready for an upcoming game. |
| `competition_id` | integer | Competition id (key space: sportpress competitions): ONE season of the league (24 = 2025-26, 25 = 2026-27). Not the season number. The key of the standings, ranking, slider and box routes. |
| `season` | integer | Season END year of the game's competition. |
| `phase_id` | integer | Phase id (key space: sportpress phases), a stage of one competition such as the regular season or the playoffs; new every season. |
| `phase_name` | character | Phase name as entered in the CMS ('Regular season 2026-27'). |
| `round_name` | character | Round label as entered in the CMS; often empty. |
| `round_priority` | integer | Sort order of the round within the phase (provider's priority); meaning of the values is unverified. |
| `period_name` | character | Name of the period the game is in or ended in; 'Extra2' means the second overtime. |
| `arena` | character | Name of the arena where the game is played. |
| `home_competition_team_id` | integer | Home side. Competition-team id (key space: sportpress competition teams): a team in ONE competition, so the same club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). Joins competitors, rosters, box scores, film and standings. The stable club key across seasons is `uu_team_id`. |
| `home_team` | character | Home side. Display name incl. the sponsor. Teams of the playoff phases carry a ' playoff' suffix (2025-26), and standings names vary with X-Localization ('Partena Professional Okapi Aalst Playoff' (en) vs 'Okapi Aalst Playoff' (nl, fr)): never join on it. |
| `home_team_short_name` | character | Home side. Three-letter club code (CHA, OOS, ANT, MON, LIM, ...), stable across seasons in the samples. |
| `home_score` | integer | Home side. Final points of the side, including overtime. |
| `away_competition_team_id` | integer | Away side. Competition-team id (key space: sportpress competition teams): a team in ONE competition, so the same club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). Joins competitors, rosters, box scores, film and standings. The stable club key across seasons is `uu_team_id`. |
| `away_team` | character | Away side. Display name incl. the sponsor. Teams of the playoff phases carry a ' playoff' suffix (2025-26), and standings names vary with X-Localization ('Partena Professional Okapi Aalst Playoff' (en) vs 'Okapi Aalst Playoff' (nl, fr)): never join on it. |
| `away_team_short_name` | character | Away side. Three-letter club code (CHA, OOS, ANT, MON, LIM, ...), stable across seasons in the samples. |
| `away_score` | integer | Away side. Final points of the side, including overtime. |
| `competition_team_id` | integer | Competition-team id the player played for in the game. |
| `starter` | logical | True when the player started the game. |
| `jersey` | character | Shirt number as text; the box score sends it as an integer, so a zero-padded '00' is already lost there. |
| `minute` | integer | Minutes played; for a team total, the sum over its players (200 + 25 per overtime). |
| `foul` | integer | Personal fouls committed. |
| `defensive_foul` | integer | Fouls DRAWN (suffered), despite the name: equals the opponent's foul count up to scorer corrections. |
| `dunk_made` | integer | Dunks made (a subset of the two-point field goals). |
| `dunk_all` | integer | Dunks attempted. |
| `dunk_percent` | numeric | Dunk percentage 0-100; 0 when none were attempted. |
| `two_point_made` | integer | Two-point field goals made. |
| `two_point_all` | integer | Two-point field goals attempted. |
| `two_point_percent` | numeric | Two-point percentage 0-100. |
| `three_point_made` | integer | Three-point field goals made. |
| `three_point_all` | integer | Three-point field goals attempted. |
| `three_point_percent` | numeric | Three-point percentage 0-100. |
| `throw_made` | integer | All field goals made, two- plus three-pointers (the provider calls them 'throw'). |
| `throw_all` | integer | All field goals attempted, two- plus three-pointers (the provider calls them 'throw'). |
| `throw_percent` | numeric | Field-goal percentage 0-100. |
| `free_throw_made` | integer | Free throws made. |
| `free_throw_all` | integer | Free throws attempted. |
| `free_throw_percent` | numeric | Free-throw percentage 0-100. |
| `offensive_rebound` | integer | Offensive rebounds. |
| `defensive_rebound` | integer | Defensive rebounds. |
| `total_rebound` | integer | Total rebounds (offensive plus defensive). |
| `block` | integer | Shots blocked by the player. |
| `block_against` | integer | Own shots blocked by an opponent. |
| `turnover` | integer | Turnovers committed. |
| `steal` | integer | Steals (balls taken from an opponent). |
| `assist` | integer | Assists (passes leading directly to a made basket). |
| `points` | integer | Points scored (2 x two-pointers + 3 x three-pointers + free throws). |
| `value` | integer | Efficiency rating (PIR with fouls drawn): points + rebounds + assists + steals + blocks + fouls drawn - missed field goals - missed free throws - turnovers. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_player_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_player_games-example}

```python
sportpress_player_games(roster_id=11193)
```
