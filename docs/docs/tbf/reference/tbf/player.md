---
title: "TBF — TBF app API (miniappapi.tbf.org.tr, Turkey: BSL, TBL, KBSL) — Player"
sidebar_label: "Player"
sidebar_position: 3
description: "TBF — TBF app API (miniappapi.tbf.org.tr, Turkey: BSL, TBL, KBSL) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# TBF — TBF app API (miniappapi.tbf.org.tr, Turkey: BSL, TBL, KBSL) — Player

## tbf_player_box

Player box score of a finished game: minutes, shooting splits, rebounds, assists, +/- and efficiency.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-istatistik`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-istatistik?match_id=346279](https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-istatistik?match_id=346279)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Required. TBF game id (integer), e.g. 346279; match_id in tbf_schedule. One global sequence across leagues and seasons. Only a finished game is served. |

### Returns {#tbf_player_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, 346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the runtime stamps the integer asked for. |
| `side` | character | Side of the game the row belongs to: home or away (from the route's isHome flag or home / away block). |
| `team_process_id` | integer | Team-season id of the player's side (from the game header; key space: TBF team registrations). |
| `player_id` | integer | TBF person id (key space: TBF persons, stable across seasons and clubs: 196413). The same value as person_id in the player and team tables and the player_id argument of tbf_player_career. |
| `player_name` | character | Player name as TBF prints it for that route (title case, Turkish letters kept). |
| `jersey_number` | character | Shirt number as a string, so '0' and '00' stay distinct. |
| `is_starter` | logical | True for the starting five (homeTopFive / awayTopFive block); none are flagged in 2006-07. |
| `seconds_played` | integer | Time on court in seconds, parsed from the 'mm:ss' or 'hh:mm:ss' text the API sends (team totals sum their players, so 200:00 = 12000). |
| `points` | integer | Points scored in the game (or season) the row covers. |
| `fg_made` | integer | Field goals made, 2PT + 3PT (successfulShots). |
| `fg_attempted` | integer | Field goals attempted, 2PT + 3PT (shotAttempts). |
| `fg_pct` | integer | Field-goal percentage 0-100, rounded to an integer by the API. |
| `fg2_made` | integer | Two-point field goals MADE (the API calls it twoPointShots; attempts are fg2_attempted). |
| `fg2_attempted` | integer | Two-point field goals attempted (twoPointShotAttempts). |
| `fg2_pct` | integer | Two-point percentage 0-100, an integer. |
| `fg3_made` | integer | Three-point field goals MADE (the API calls it threePointShots). |
| `fg3_attempted` | integer | Three-point field goals attempted (threePointShotAttempts). |
| `fg3_pct` | integer | Three-point percentage 0-100, an integer. |
| `ft_made` | integer | Free throws MADE (serbest atis; the API calls it freeThrows). |
| `ft_attempted` | integer | Free throws attempted (freeThrowAttempts). |
| `ft_pct` | integer | Free-throw percentage 0-100, an integer. |
| `off_rebounds` | integer | Offensive rebounds (hücum ribaundu). |
| `def_rebounds` | integer | Defensive rebounds (savunma ribaundu). |
| `rebounds` | integer | Total rebounds (toplam ribaund). |
| `assists` | integer | Assists in this game (asist). |
| `steals` | integer | Steals in this game (top çalma). |
| `blocks` | integer | Shots this player blocked in this game (blok). |
| `turnovers` | integer | Turnovers committed in this game (top kaybı). |
| `fouls` | integer | Personal fouls committed (faul). |
| `fouls_drawn` | integer | Fouls drawn (rivalFoul = foul committed by the opponent). |
| `blocks_received` | integer | Own shots blocked by an opponent (blocksReceived). |
| `plus_minus` | integer | Plus/minus while on court (plusMinus); null when the API sends ''. |
| `efficiency` | integer | Efficiency rating (verimlilik puani, PIR): points + rebounds + assists + steals + blocks + fouls drawn - missed shots - turnovers - fouls. An integer on player rows. |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_player_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_player_box-example}

```python
tbf_player_box(match_id=346279)
```

## tbf_players

Every player registered in one league-season (birth dates are not returned).

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-player-list`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-player-list?season=2027&competition=bsl](https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-player-list?season=2027&competition=bsl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the list lacks raises NoDataError. |
| `competition` | `competition` |  |  | `Y` | TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was named TBL until 2007-08 and is listed under bsl. |

### Returns {#tbf_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year of the request. |
| `competition` | character | League prefix the row reports (bsl, tbl or kbsl); equals the request's competition. |
| `person_id` | integer | TBF person id (kisiID; key space: TBF persons, stable across seasons and clubs). |
| `player_name` | character | Player name as TBF prints it for that route (title case, Turkish letters kept). |
| `nationality` | character | Nationality as a TURKISH country name (ABD = United States); see the nationality_code. |
| `nationality_code` | character | Nationality code (tabiyetKisa), ISO 3166-1 alpha-3 except GB for the United Kingdom. |
| `club_id` | integer | Stable club id (takimID / teamId of the team lists; key space: TBF clubs, the same across seasons and leagues: Tofas = 13). Not the team-season id (team_process_id). |
| `team_name` | character | Team name for that season, uppercase with Turkish letters. |
| `jersey_number` | character | Shirt number as a string, so '0' and '00' stay distinct. |
| `prefix` | character | League prefix of the row (bsl, tbl, kbsl); the runtime refuses any other than the request's. |
| `season_id` | integer | Internal TBF season id (key space: TBF seasons, global across leagues: 174 = 2026-2027, 172 = 2025-2026; the odd ids are international UA- seasons). Never an sdv key: use season (END year). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_players-example}

```python
tbf_players(competition='bsl', season=2027)
```

## tbf_player_career

One player's career: a row per season and competition with totals (birth date is not returned).

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-player-detail-profile-and-career-info/{player_id}`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-player-detail-profile-and-career-info/196413](https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-player-detail-profile-and-career-info/196413)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Required. TBF person id, e.g. 196413; person_id in tbf_players and the box score. |

### Returns {#tbf_player_career-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `person_id` | integer | TBF person id of the player asked for (the player_id argument). |
| `player_name` | character | Player's full name from the profile (the same on every row). |
| `nationality` | character | Nationality as a TURKISH country name from the profile (country). |
| `height_cm` | integer | Height in centimetres from the profile; null when TBF has none. |
| `season_label` | character | Career season as TBF writes it (seasonCode), e.g. 2025-2026. |
| `season` | integer | Career season END year, read from the season label. |
| `team_name` | character | Team the player represented that season, uppercase with Turkish letters (teamName). |
| `activity_name` | character | League-season name (activityName), e.g. 'BSL 2025-2026'. |
| `activity_type_id` | integer | Competition type id (faaliyetTuruId): 1 = BSL, 4 = TBL, 79 = BGL. |
| `activity_id` | integer | Activity id of the league-season (faaliyetAltAlaniId; key space: TBF league-seasons). |
| `games` | integer | Games played (macSayisi = game count). |
| `seconds_played` | integer | Total time on court that season in seconds (formattedTotalSeconds 'hh:mm:ss'). |
| `points` | integer | Points scored in the game (or season) the row covers. |
| `assists` | integer | Assists in that season and competition. |
| `fg_made` | integer | Field goals made (fieldGoalsMade), 2PT + 3PT. |
| `fg_attempted` | integer | Field goals attempted (fieldGoalsAttempted), 2PT + 3PT. |
| `fg3_made` | integer | Three-point field goals made (threePointsMade). |
| `fg3_attempted` | integer | Three-point field goals attempted (threePointsAttempted). |
| `ft_made` | integer | Free throws made (the API spells the field freeThows). |
| `ft_attempted` | integer | Free throws attempted (freeThowsAttempted). |
| `off_rebounds` | integer | Offensive rebounds in that season and competition. |
| `def_rebounds` | integer | Defensive rebounds in that season and competition. |
| `rebounds` | integer | Total rebounds in that season and competition. |
| `steals` | integer | Steals in that season and competition. |
| `turnovers` | integer | Turnovers (the API spells the field tournovers). |
| `blocks` | integer | Blocks in that season and competition. |
| `fouls` | integer | Personal fouls (the API spells the field fauls). |
| `efficiency` | integer | Efficiency rating total (playerEfficiency, PIR) for that season and competition. |
| `fouls_drawn` | integer | Fouls drawn (yapilanFaul). |
| `blocks_received` | integer | Own shots blocked by an opponent (yenilenBlok). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_player_career-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_player_career-example}

```python
tbf_player_career(player_id=196413)
```
