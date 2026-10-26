---
title: "BBL / DBBL (SCB) — SCB basketball API (BBL and DBBL, Germany) — Other: records_per–records_season"
sidebar_label: "Other: records_per–records_season"
sidebar_position: 4
description: "BBL / DBBL (SCB) — SCB basketball API (BBL and DBBL, Germany) — Other: records_per–records_season — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# BBL / DBBL (SCB) — SCB basketball API (BBL and DBBL, Germany) — Other: records_per–records_season

## scb_records_per_game

Single-game records: the season's and the all-time ones, one row per record.

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/records/per-game/{season}`

**Valid URL:** [https://api.basketball-bundesliga.de/records/per-game/2025](https://api.basketball-bundesliga.de/records/per-game/2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26, the sdv key). Sent to the API as its START-year seasonId (season - 1). |
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |

### Returns {#scb_records_per_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | String | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `season` | Int64 | Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one. |
| `season_id` | Int64 | The API's own season key, the START year (2025 = 2025-26); `season` minus one. |
| `scope` | String | `season` for the requested season's records, `all_time` for the all-time list. |
| `stat` | String | The record's stat: points, rebounds, assists, steals, blocks, turnovers, efficiency, free_throws_made, three_point_shots_made or free_throws_success_percentage. |
| `place` | Int64 | Rank within the list (1 = best); 0 on some rows. |
| `all_time_place` | Int64 | Rank on the all-time list; null when not on it. |
| `order_id` | Int64 | Sort order within the list. |
| `all_time_order_id` | Int64 | Sort order on the all-time list; null when not on it. |
| `value` | Float64 | The record value. A STRING in /records/per-game ('52'); a number in /records/season-averages. |
| `ratio` | Float64 | [made, attempted] as strings, on percentage records only; null otherwise. |
| `season_player_id` | String | Season-player id: one player's engagement with one club (kept across seasons while he stays, new after a move; see README). Joins to box-score `seasonPlayer.id`, action `seasonPlayerId` and /season-players/{seasonPlayerId}. |
| `real_player_id` | Int64 | Global person id: the same across clubs and seasons. Joins to `realPlayerId` everywhere. |
| `first_name` | String | Player's given name. |
| `last_name` | String | Player's family name. |
| `team_name` | String | Team name as listed that season (sponsor names included). |
| `team_tlc` | String | The player's team code. |
| `home_tlc` | String | Three-letter code of the home team of the record game. |
| `guest_tlc` | String | Three-letter code of the guest team of the record game. |
| `game_id` | String | Game id, a string. Joins to /games/{gameId}. |
| `scheduled_time` | String | Scheduled tip-off, ISO 8601 UTC. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#scb_records_per_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a game not yet started, an empty table, a playoff bracket not yet drawn); a game before 2002-03 is zero rows. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#scb_records_per_game-example}

```python
scb_records_per_game(season=2026)
```

## scb_records_season_averages

All-time season-average records (points, rebounds, assists, ...), one row per record.

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/records/season-averages`

**Valid URL:** [https://api.basketball-bundesliga.de/records/season-averages](https://api.basketball-bundesliga.de/records/season-averages)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |

### Returns {#scb_records_season_averages-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | String | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `stat` | String | The record's stat list: assist_average, block_average, efficiency_average, foul_average, free_throws_success_percentage, points_average, rebound_average, steal_average, three_point_shot_average or turnover_average. |
| `season` | Int64 | Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one. |
| `season_id` | Int64 | Season, START year (2026 = 2026-27). |
| `place` | Int64 | Rank within the list (1 = best); 0 on some rows. |
| `order_id` | Int64 | Sort order within the list. |
| `value` | Float64 | The record value. A STRING in /records/per-game ('52'); a number in /records/season-averages. |
| `ratio` | Float64 | [made, attempted] as strings, on percentage records only; null otherwise. |
| `season_player_id` | String | Season-player id: one player's engagement with one club (kept across seasons while he stays, new after a move; see README). Joins to box-score `seasonPlayer.id`, action `seasonPlayerId` and /season-players/{seasonPlayerId}. |
| `real_player_id` | Int64 | Global person id: the same across clubs and seasons. Joins to `realPlayerId` everywhere. |
| `first_name` | String | Player's given name. |
| `last_name` | String | Player's family name. |
| `team_name` | String | Team name as listed that season (sponsor names included). |
| `team_tlc` | String | The player's team code. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#scb_records_season_averages-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a game not yet started, an empty table, a playoff bracket not yet drawn); a game before 2002-03 is zero rows. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#scb_records_season_averages-example}

```python
scb_records_season_averages()
```
