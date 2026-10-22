---
title: "TBF — TBF app API (miniappapi.tbf.org.tr, Turkey: BSL, TBL, KBSL) — Game"
sidebar_label: "Game"
sidebar_position: 1
description: "TBF — TBF app API (miniappapi.tbf.org.tr, Turkey: BSL, TBL, KBSL) — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# TBF — TBF app API (miniappapi.tbf.org.tr, Turkey: BSL, TBL, KBSL) — Game

## tbf_game_header

Game header of a finished game: teams, coaches, final score, officials, venue and the Genius LiveStats id.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-header`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-header?match_id=346279](https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-header?match_id=346279)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Required. TBF game id (integer), e.g. 346279; match_id in tbf_schedule. One global sequence across leagues and seasons. Only a finished game is served. |

### Returns {#tbf_game_header-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, 346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the runtime stamps the integer asked for. |
| `match_code` | character | TBF short game code ('1EA4917'). |
| `genius_id` | integer | Genius Sports LiveStats match id (TBF spells it genuisId); joins the genius family. Null when TBF stored none: every game before 2018-19, many 2021-25 games and games not yet assigned one. |
| `activity_id` | integer | Activity id (faaliyet = activity; key space: TBF league-SEASONS, new every season: BSL 2026-27 = 22214, BSL 2025-26 = 20728, TBL 2026-27 = 22215, KBSL 2026-27 = 22209). Cast to an integer from the 22214 / 22214.0 the API sends. |
| `activity_name` | character | Activity name ('BSL 2026-2027'). |
| `season_id` | integer | Internal TBF season id (key space: TBF seasons, global across leagues: 174 = 2026-2027, 172 = 2025-2026; the odd ids are international UA- seasons). Never an sdv key: use season (END year). |
| `season_label` | character | Season label 'YYYY-YYYY' (seasonName). |
| `season` | integer | Season END year, read from the season label (seasonName). |
| `tipoff` | character | Tip-off as a naive timestamp in Turkey local time (Europe/Istanbul, UTC+3). |
| `status_id` | integer | Game status code: 1 = played (Oynandi), 4 = not played (Oynanmadi), 19 = fixture without a date and time. Cast to an integer from the 1 / 1.0 the API sends. |
| `status` | character | Game status text in Turkish: Oynandi = played. Only finished games are served. |
| `venue` | character | Venue name (location) as TBF prints it. |
| `attendance` | integer | Spectator count (matchSpectatorCount); 0 when not recorded. |
| `has_overtime` | logical | Overtime flag (uzatma = overtime). Reliability unverified: false on every one of 64 BSL 2025-26 games checked. |
| `broadcast_channel` | character | Broadcaster (matchBroadcastChannel), e.g. beINSports, or null. |
| `home_team_process_id` | integer | Home team-season id (key space: TBF team registrations; the header calls it homeTeamProcessId). |
| `home_team` | character | Home team name as registered for that season, uppercase with Turkish letters. |
| `home_coach` | character | Home head coach name, uppercase. |
| `home_score` | integer | Home final score. |
| `away_team_process_id` | integer | Away team-season id (key space: TBF team registrations). |
| `away_team` | character | Away team name as registered for that season, uppercase with Turkish letters. |
| `away_coach` | character | Away head coach name, uppercase. |
| `away_score` | integer | Away final score. |
| `referee` | character | Main referee's name (names only: no contact or birth data is returned). |
| `first_assistant_referee` | character | First assistant referee's name. |
| `second_assistant_referee` | character | Second assistant referee's name. |
| `commissioner` | character | Commissioner's (temsilci = representative) name. |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_game_header-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_game_header-example}

```python
tbf_game_header(match_id=346279)
```

## tbf_game_players

Game roster from the header: every listed player of both sides (names and shirt numbers, no usable ids).

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-header`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-header?match_id=346279](https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-header?match_id=346279)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Required. TBF game id (integer), e.g. 346279; match_id in tbf_schedule. One global sequence across leagues and seasons. Only a finished game is served. |

### Returns {#tbf_game_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, 346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the runtime stamps the integer asked for. |
| `side` | character | Side of the game the row belongs to: home or away (from the route's isHome flag or home / away block). |
| `team` | character | Team name for the roster row; empty in the samples, so null. |
| `player_name` | character | Player name as the header roster lists it. |
| `jersey_number` | character | Shirt number as a string, so '0' and '00' stay distinct. |
| `is_starter` | logical | True for a starter in the header roster (isStarter). |
| `is_active` | logical | True when the player is flagged active in the header (isActive). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_game_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_game_players-example}

```python
tbf_game_players(match_id=346279)
```

## tbf_game_leaders

Game leaders of a finished game per statistic category (top five each).

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-leader`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-leader?match_id=346279](https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-leader?match_id=346279)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Required. TBF game id (integer), e.g. 346279; match_id in tbf_schedule. One global sequence across leagues and seasons. Only a finished game is served. |

### Returns {#tbf_game_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, 346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the runtime stamps the integer asked for. |
| `category` | character | Leader category code (leaderCategory), e.g. VerimlilikPuani = efficiency rating. |
| `category_label` | character | Leader category label in Turkish (leaderCategoryDescription), e.g. Verimlilik Puani. |
| `rank` | integer | Position in the category's list, 1 = best (top five per category). |
| `side` | character | Side of the leader's team (isHome). |
| `player_id` | integer | TBF person id of the leader (key space: TBF persons; equals the box score player_id). |
| `player_name` | character | Player name as TBF prints it for that route (title case, Turkish letters kept). |
| `jersey_number` | character | Shirt number as a string, so '0' and '00' stay distinct. |
| `value` | numeric | The leader's value in the category (points, rebounds, ...). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_game_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_game_leaders-example}

```python
tbf_game_leaders(match_id=346279)
```
