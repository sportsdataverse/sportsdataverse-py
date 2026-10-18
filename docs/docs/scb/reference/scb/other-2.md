---
title: "SCB — SCB basketball API (BBL and DBBL, Germany) — Other: records_season"
sidebar_label: "Other: records_season"
sidebar_position: 4
description: "SCB — SCB basketball API (BBL and DBBL, Germany) — Other: records_season — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# SCB — SCB basketball API (BBL and DBBL, Germany) — Other: records_season

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
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `stat` | character | The record's stat list: assist_average, block_average, efficiency_average, foul_average, free_throws_success_percentage, points_average, rebound_average, steal_average, three_point_shot_average or turnover_average. |
| `season` | integer | Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one. |
| `season_id` | integer | Season, START year (2026 = 2026-27). |
| `place` | integer | Rank within the list (1 = best); 0 on some rows. |
| `order_id` | integer | Sort order within the list. |
| `value` | numeric | The record value. A STRING in /records/per-game ('52'); a number in /records/season-averages. |
| `ratio` | numeric | [made, attempted] as strings, on percentage records only; null otherwise. |
| `season_player_id` | character | Season-player id: one player's engagement with one club (kept across seasons while he stays, new after a move; see README). Joins to box-score `seasonPlayer.id`, action `seasonPlayerId` and /season-players/{seasonPlayerId}. |
| `real_player_id` | integer | Global person id: the same across clubs and seasons. Joins to `realPlayerId` everywhere. |
| `first_name` | character | Player's given name. |
| `last_name` | character | Player's family name. |
| `team_name` | character | Team name as listed that season (sponsor names included). |
| `team_tlc` | character | The player's team code. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#scb_records_season_averages-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the API has nothing for the request (an unknown id, a game before 2002-03 or not yet started, an empty table, a playoff bracket not yet drawn). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#scb_records_season_averages-example}

```python
scb_records_season_averages()
```

_Last validated n/a._
