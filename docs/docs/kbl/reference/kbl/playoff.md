---
title: "KBL — KBL API (api.kbl.or.kr, Korea) — Playoff"
sidebar_label: "Playoff"
sidebar_position: 2
description: "KBL — KBL API (api.kbl.or.kr, Korea) — Playoff — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# KBL — KBL API (api.kbl.or.kr, Korea) — Playoff

## kbl_playoff_seeds

The regular-season finish that seeds the playoffs (the top six).

**Endpoint URL:** `GET https://api.kbl.or.kr/league/playoff/{season}`

**Valid URL:** [https://api.kbl.or.kr/league/playoff/2025](https://api.kbl.or.kr/league/playoff/2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26; the inaugural 1997 season is 1997, the latest 2027). kbl_runtime maps it to the KBL seasonCode (47 = 2025-26). |

### Returns {#kbl_playoff_seeds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26), derived from season_code through the crosswalk (kbl_codes.SEASONS). |
| `season_code` | Int64 | KBL season code: sequential over every season and D-League edition (2 = 1997-98, 47 = 2025-26); the crosswalk to the sdv END year is in the README. |
| `game_code` | String | Competition type of the standings: '01' regular season. |
| `seed` | Int64 | Final regular-season standing, i.e. the playoff seed (1-6). |
| `team_code` | String | KBL team code (2-digit string, e.g. '50' = Changwon LG). Joins to /common/teamList `teamCode`, game `tcodeH`/`tcodeA`, `t` in text-cast. Not stable across ownership changes: the Anyang club is '40' (SBS) in 1997-98 and '70' today. D-League sides have their own codes (`teamCodeD`). |
| `team_name` | String | Team short name (Korean, e.g. '원주 DB' = Wonju DB). English names: /common/teamList `teamNameEn` / `teamNameFullEn`, joined on the team code. |
| `team_name_full` | String | Team full name (Korean). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kbl_playoff_seeds-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a final game with no box score, no playoff bracket, no schedule rows in a window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown id is the API's HTTP 500), the KBL error envelope, an empty or non-JSON body, a game that is not final, or a body for another game, season or window than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed gmkey, a season that is not an END year between 1997 and 2027, an unknown division or game_code, or bad schedule dates (raised before any request). |

### Example {#kbl_playoff_seeds-example}

```python
kbl_playoff_seeds(season=2025)
```

## kbl_playoff_series

The playoff series (quarterfinals, semifinals, final): wins and losses of each team in each series.

**Endpoint URL:** `GET https://api.kbl.or.kr/league/playoff/{season}`

**Valid URL:** [https://api.kbl.or.kr/league/playoff/2025](https://api.kbl.or.kr/league/playoff/2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26; the inaugural 1997 season is 1997, the latest 2027). kbl_runtime maps it to the KBL seasonCode (47 = 2025-26). |

### Returns {#kbl_playoff_series-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26), derived from season_code through the crosswalk (kbl_codes.SEASONS). |
| `season_code` | Int64 | KBL season code: one sequential number over every season and D-League edition (47 = 2025-26, 48 = the 2025 D-League). |
| `bracket_format` | String | Playoff bracket format: old (seasons through 2023-24, per-team series results) or new (2024-25 on, bracket numbers and games). |
| `round` | String | Playoff round: quarterfinal (6강, seeds 3-6), semifinal (4강) or final; on the round-MVP and rounds frames the round number of the season. |
| `series_no` | Int64 | Series number inside the round (the wire's gameHoi, '1' or '2'; 1 for the final). |
| `team_code` | String | KBL team code (2-digit string, e.g. '50' = Changwon LG). Joins to /common/teamList `teamCode`, game `tcodeH`/`tcodeA`, `t` in text-cast. Not stable across ownership changes: the Anyang club is '40' (SBS) in 1997-98 and '70' today. D-League sides have their own codes (`teamCodeD`). |
| `opponent_code` | String | Team code of the series opponent; null when the opponent cannot be told from the body. |
| `seed` | Int64 | Regular-season finish that seeded the team (1-6), from the bracket's seed table; null for a team outside it. |
| `wins` | Int64 | Wins of the team in the series. |
| `losses` | Int64 | Losses of the team in the series. |
| `game_date` | Date | Date of a game of the series, KST (which one is unverified); old bracket format only. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kbl_playoff_series-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a final game with no box score, no playoff bracket, no schedule rows in a window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown id is the API's HTTP 500), the KBL error envelope, an empty or non-JSON body, a game that is not final, or a body for another game, season or window than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed gmkey, a season that is not an END year between 1997 and 2027, an unknown division or game_code, or bad schedule dates (raised before any request). |

### Example {#kbl_playoff_series-example}

```python
kbl_playoff_series(season=2025)
```

## kbl_playoff_games

The games of the playoff bracket (2024-25 on; the older bracket format lists no games).

**Endpoint URL:** `GET https://api.kbl.or.kr/league/playoff/{season}`

**Valid URL:** [https://api.kbl.or.kr/league/playoff/2025](https://api.kbl.or.kr/league/playoff/2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26; the inaugural 1997 season is 1997, the latest 2027). kbl_runtime maps it to the KBL seasonCode (47 = 2025-26). |

### Returns {#kbl_playoff_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26), derived from season_code through the crosswalk (kbl_codes.SEASONS). |
| `season_code` | Int64 | KBL season code of the game (from the bracket's gmkey object). |
| `gmkey` | String | Game key S{seasonCode}G{gameCode}N{gameNo} (e.g. S47G01N49): the id of every game route and game frame. |
| `game_code` | String | Competition type of the game: 03 playoffs, 04 championship final. |
| `game_no` | Int64 | Game number inside the competition, from the bracket's gmkey object. |
| `round` | String | Playoff round: quarterfinal (6강, seeds 3-6), semifinal (4강) or final; on the round-MVP and rounds frames the round number of the season. |
| `series_no` | Int64 | Series number inside the round (1 or 2 for the quarterfinals and semifinals, 1 for the final). |
| `game_date` | Date | Game date, KST. |
| `home_team_code` | String | Home team code. Team code: the KBL club in that season (two-digit string with a leading zero, e.g. '50' = Changwon LG). Joins kbl_teams team_code; not stable across ownership changes (the Anyang club is '40' in 1997-98, '70' today). |
| `away_team_code` | String | Away team code. Team code: the KBL club in that season (two-digit string with a leading zero, e.g. '50' = Changwon LG). Joins kbl_teams team_code; not stable across ownership changes (the Anyang club is '40' in 1997-98, '70' today). |
| `home_team_name` | String | Home team short name (Korean). |
| `away_team_name` | String | Away team short name (Korean). |
| `home_score` | Int64 | Home team points. |
| `away_score` | Int64 | Away team points. |
| `home_win` | Boolean | 1 when the home team won, else 0. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kbl_playoff_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a final game with no box score, no playoff bracket, no schedule rows in a window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown id is the API's HTTP 500), the KBL error envelope, an empty or non-JSON body, a game that is not final, or a body for another game, season or window than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed gmkey, a season that is not an END year between 1997 and 2027, an unknown division or game_code, or bad schedule dates (raised before any request). |

### Example {#kbl_playoff_games-example}

```python
kbl_playoff_games(season=2025)
```
