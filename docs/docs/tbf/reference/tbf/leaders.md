---
title: "TBF — TBF app API (miniappapi.tbf.org.tr, Turkey: BSL, TBL, KBSL) — Leaders"
sidebar_label: "Leaders"
sidebar_position: 2
description: "TBF — TBF app API (miniappapi.tbf.org.tr, Turkey: BSL, TBL, KBSL) — Leaders — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# TBF — TBF app API (miniappapi.tbf.org.tr, Turkey: BSL, TBL, KBSL) — Leaders

## tbf_leaders_points

Season leaders in points: the server's top 20 by season total and by per-game average.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-point-leaders`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-point-leaders?season=2027&competition=bsl](https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-point-leaders?season=2027&competition=bsl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the list lacks raises NoDataError. |
| `competition` | `competition` |  |  | `Y` | TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was named TBL until 2007-08 and is listed under bsl. |

### Returns {#tbf_leaders_points-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `competition` | character | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `category` | character | Statistic of the wrapper: points, assists, blocks, rebounds, steals, minutes, threes, turnovers or efficiency. |
| `kind` | character | total = the server's top 20 by season TOTAL; average = the top 20 by PER-GAME average. |
| `rank` | integer | Rank in the list, 1 = best (the API's order). |
| `person_id` | integer | TBF person id (oyuncuKodu, sent as a string; key space: TBF persons). |
| `player_name` | character | Player name as TBF prints it for that route (title case, Turkish letters kept). |
| `team_process_id` | integer | Team-season id (takimKodu, sent as a string; key space: TBF team registrations; despite the name NOT the club id). |
| `team_name` | character | Team name as registered for that season, uppercase with Turkish letters (sponsor name: 'PİZZABULLS BORDO BANDIRMA'). |
| `games` | integer | Games played (macSayisi, sent as a string). |
| `value` | numeric | The ranked statistic: the season total for kind total, the per-game average for kind average (the API names the field grup and sends it as a string). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_leaders_points-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_leaders_points-example}

```python
tbf_leaders_points(competition='bsl', season=2027)
```

_Last validated n/a._

## tbf_leaders_assists

Season leaders in assists: the server's top 20 by season total and by per-game average.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-asist-leaders`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-asist-leaders?season=2027&competition=bsl](https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-asist-leaders?season=2027&competition=bsl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the list lacks raises NoDataError. |
| `competition` | `competition` |  |  | `Y` | TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was named TBL until 2007-08 and is listed under bsl. |

### Returns {#tbf_leaders_assists-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `competition` | character | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `category` | character | Statistic of the wrapper: points, assists, blocks, rebounds, steals, minutes, threes, turnovers or efficiency. |
| `kind` | character | total = the server's top 20 by season TOTAL; average = the top 20 by PER-GAME average. |
| `rank` | integer | Rank in the list, 1 = best (the API's order). |
| `person_id` | integer | TBF person id (oyuncuKodu, sent as a string; key space: TBF persons). |
| `player_name` | character | Player name as TBF prints it for that route (title case, Turkish letters kept). |
| `team_process_id` | integer | Team-season id (takimKodu, sent as a string; key space: TBF team registrations; despite the name NOT the club id). |
| `team_name` | character | Team name as registered for that season, uppercase with Turkish letters (sponsor name: 'PİZZABULLS BORDO BANDIRMA'). |
| `games` | integer | Games played (macSayisi, sent as a string). |
| `value` | numeric | The ranked statistic: the season total for kind total, the per-game average for kind average (the API names the field grup and sends it as a string). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_leaders_assists-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_leaders_assists-example}

```python
tbf_leaders_assists(competition='bsl', season=2027)
```

_Last validated n/a._

## tbf_leaders_blocks

Season leaders in blocks: the server's top 20 by season total and by per-game average.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-blok-leaders`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-blok-leaders?season=2027&competition=bsl](https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-blok-leaders?season=2027&competition=bsl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the list lacks raises NoDataError. |
| `competition` | `competition` |  |  | `Y` | TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was named TBL until 2007-08 and is listed under bsl. |

### Returns {#tbf_leaders_blocks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `competition` | character | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `category` | character | Statistic of the wrapper: points, assists, blocks, rebounds, steals, minutes, threes, turnovers or efficiency. |
| `kind` | character | total = the server's top 20 by season TOTAL; average = the top 20 by PER-GAME average. |
| `rank` | integer | Rank in the list, 1 = best (the API's order). |
| `person_id` | integer | TBF person id (oyuncuKodu, sent as a string; key space: TBF persons). |
| `player_name` | character | Player name as TBF prints it for that route (title case, Turkish letters kept). |
| `team_process_id` | integer | Team-season id (takimKodu, sent as a string; key space: TBF team registrations; despite the name NOT the club id). |
| `team_name` | character | Team name as registered for that season, uppercase with Turkish letters (sponsor name: 'PİZZABULLS BORDO BANDIRMA'). |
| `games` | integer | Games played (macSayisi, sent as a string). |
| `value` | numeric | The ranked statistic: the season total for kind total, the per-game average for kind average (the API names the field grup and sends it as a string). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_leaders_blocks-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_leaders_blocks-example}

```python
tbf_leaders_blocks(competition='bsl', season=2027)
```

_Last validated n/a._

## tbf_leaders_rebounds

Season leaders in rebounds: the server's top 20 by season total and by per-game average.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-ribound-leaders`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-ribound-leaders?season=2027&competition=bsl](https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-ribound-leaders?season=2027&competition=bsl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the list lacks raises NoDataError. |
| `competition` | `competition` |  |  | `Y` | TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was named TBL until 2007-08 and is listed under bsl. |

### Returns {#tbf_leaders_rebounds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `competition` | character | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `category` | character | Statistic of the wrapper: points, assists, blocks, rebounds, steals, minutes, threes, turnovers or efficiency. |
| `kind` | character | total = the server's top 20 by season TOTAL; average = the top 20 by PER-GAME average. |
| `rank` | integer | Rank in the list, 1 = best (the API's order). |
| `person_id` | integer | TBF person id (oyuncuKodu, sent as a string; key space: TBF persons). |
| `player_name` | character | Player name as TBF prints it for that route (title case, Turkish letters kept). |
| `team_process_id` | integer | Team-season id (takimKodu, sent as a string; key space: TBF team registrations; despite the name NOT the club id). |
| `team_name` | character | Team name as registered for that season, uppercase with Turkish letters (sponsor name: 'PİZZABULLS BORDO BANDIRMA'). |
| `games` | integer | Games played (macSayisi, sent as a string). |
| `value` | numeric | The ranked statistic: the season total for kind total, the per-game average for kind average (the API names the field grup and sends it as a string). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_leaders_rebounds-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_leaders_rebounds-example}

```python
tbf_leaders_rebounds(competition='bsl', season=2027)
```

_Last validated n/a._

## tbf_leaders_steals

Season leaders in steals: the server's top 20 by season total and by per-game average.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-top-calma-leaders`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-top-calma-leaders?season=2027&competition=bsl](https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-top-calma-leaders?season=2027&competition=bsl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the list lacks raises NoDataError. |
| `competition` | `competition` |  |  | `Y` | TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was named TBL until 2007-08 and is listed under bsl. |

### Returns {#tbf_leaders_steals-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `competition` | character | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `category` | character | Statistic of the wrapper: points, assists, blocks, rebounds, steals, minutes, threes, turnovers or efficiency. |
| `kind` | character | total = the server's top 20 by season TOTAL; average = the top 20 by PER-GAME average. |
| `rank` | integer | Rank in the list, 1 = best (the API's order). |
| `person_id` | integer | TBF person id (oyuncuKodu, sent as a string; key space: TBF persons). |
| `player_name` | character | Player name as TBF prints it for that route (title case, Turkish letters kept). |
| `team_process_id` | integer | Team-season id (takimKodu, sent as a string; key space: TBF team registrations; despite the name NOT the club id). |
| `team_name` | character | Team name as registered for that season, uppercase with Turkish letters (sponsor name: 'PİZZABULLS BORDO BANDIRMA'). |
| `games` | integer | Games played (macSayisi, sent as a string). |
| `value` | numeric | The ranked statistic: the season total for kind total, the per-game average for kind average (the API names the field grup and sends it as a string). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_leaders_steals-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_leaders_steals-example}

```python
tbf_leaders_steals(competition='bsl', season=2027)
```

_Last validated n/a._

## tbf_leaders_minutes

Season leaders in minutes played: the server's top 20 by season total and by per-game average.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-duration-leaders`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-duration-leaders?season=2027&competition=bsl](https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-duration-leaders?season=2027&competition=bsl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the list lacks raises NoDataError. |
| `competition` | `competition` |  |  | `Y` | TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was named TBL until 2007-08 and is listed under bsl. |

### Returns {#tbf_leaders_minutes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `competition` | character | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `category` | character | Statistic of the wrapper: points, assists, blocks, rebounds, steals, minutes, threes, turnovers or efficiency. |
| `kind` | character | total = the server's top 20 by season TOTAL; average = the top 20 by PER-GAME average. |
| `rank` | integer | Rank in the list, 1 = best (the API's order). |
| `person_id` | integer | TBF person id (oyuncuKodu, sent as a string; key space: TBF persons). |
| `player_name` | character | Player name as TBF prints it for that route (title case, Turkish letters kept). |
| `team_process_id` | integer | Team-season id (takimKodu, sent as a string; key space: TBF team registrations; despite the name NOT the club id). |
| `team_name` | character | Team name as registered for that season, uppercase with Turkish letters (sponsor name: 'PİZZABULLS BORDO BANDIRMA'). |
| `games` | integer | Games played (macSayisi, sent as a string). |
| `value` | numeric | The ranked statistic: the season total for kind total, the per-game average for kind average (the API names the field grup and sends it as a string). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_leaders_minutes-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_leaders_minutes-example}

```python
tbf_leaders_minutes(competition='bsl', season=2027)
```

_Last validated n/a._

## tbf_leaders_threes

Season leaders in three-point field goals made: the server's top 20 by season total and by per-game average.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-3Point-leaders`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-3Point-leaders?season=2027&competition=bsl](https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-3Point-leaders?season=2027&competition=bsl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the list lacks raises NoDataError. |
| `competition` | `competition` |  |  | `Y` | TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was named TBL until 2007-08 and is listed under bsl. |

### Returns {#tbf_leaders_threes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `competition` | character | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `category` | character | Statistic of the wrapper: points, assists, blocks, rebounds, steals, minutes, threes, turnovers or efficiency. |
| `kind` | character | total = the server's top 20 by season TOTAL; average = the top 20 by PER-GAME average. |
| `rank` | integer | Rank in the list, 1 = best (the API's order). |
| `person_id` | integer | TBF person id (oyuncuKodu, sent as a string; key space: TBF persons). |
| `player_name` | character | Player name as TBF prints it for that route (title case, Turkish letters kept). |
| `team_process_id` | integer | Team-season id (takimKodu, sent as a string; key space: TBF team registrations; despite the name NOT the club id). |
| `team_name` | character | Team name as registered for that season, uppercase with Turkish letters (sponsor name: 'PİZZABULLS BORDO BANDIRMA'). |
| `games` | integer | Games played (macSayisi, sent as a string). |
| `value` | numeric | The ranked statistic: the season total for kind total, the per-game average for kind average (the API names the field grup and sends it as a string). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_leaders_threes-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_leaders_threes-example}

```python
tbf_leaders_threes(competition='bsl', season=2027)
```

_Last validated n/a._

## tbf_leaders_turnovers

Season leaders in turnovers: the server's top 20 by season total and by per-game average.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-top-kaybi-leaders`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-top-kaybi-leaders?season=2027&competition=bsl](https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-top-kaybi-leaders?season=2027&competition=bsl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the list lacks raises NoDataError. |
| `competition` | `competition` |  |  | `Y` | TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was named TBL until 2007-08 and is listed under bsl. |

### Returns {#tbf_leaders_turnovers-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `competition` | character | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `category` | character | Statistic of the wrapper: points, assists, blocks, rebounds, steals, minutes, threes, turnovers or efficiency. |
| `kind` | character | total = the server's top 20 by season TOTAL; average = the top 20 by PER-GAME average. |
| `rank` | integer | Rank in the list, 1 = best (the API's order). |
| `person_id` | integer | TBF person id (oyuncuKodu, sent as a string; key space: TBF persons). |
| `player_name` | character | Player name as TBF prints it for that route (title case, Turkish letters kept). |
| `team_process_id` | integer | Team-season id (takimKodu, sent as a string; key space: TBF team registrations; despite the name NOT the club id). |
| `team_name` | character | Team name as registered for that season, uppercase with Turkish letters (sponsor name: 'PİZZABULLS BORDO BANDIRMA'). |
| `games` | integer | Games played (macSayisi, sent as a string). |
| `value` | numeric | The ranked statistic: the season total for kind total, the per-game average for kind average (the API names the field grup and sends it as a string). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_leaders_turnovers-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_leaders_turnovers-example}

```python
tbf_leaders_turnovers(competition='bsl', season=2027)
```

_Last validated n/a._

## tbf_leaders_efficiency

Season leaders in efficiency rating: the server's top 20 by season total and by per-game average.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-mvp-leaders`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-mvp-leaders?season=2027&competition=bsl](https://miniappapi.tbf.org.tr/webapi-service/api/Player/get-mvp-leaders?season=2027&competition=bsl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the list lacks raises NoDataError. |
| `competition` | `competition` |  |  | `Y` | TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was named TBL until 2007-08 and is listed under bsl. |

### Returns {#tbf_leaders_efficiency-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `competition` | character | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `category` | character | Statistic of the wrapper: points, assists, blocks, rebounds, steals, minutes, threes, turnovers or efficiency. |
| `kind` | character | total = the server's top 20 by season TOTAL; average = the top 20 by PER-GAME average. |
| `rank` | integer | Rank in the list, 1 = best (the API's order). |
| `person_id` | integer | TBF person id (oyuncuKodu, sent as a string; key space: TBF persons). |
| `player_name` | character | Player name as TBF prints it for that route (title case, Turkish letters kept). |
| `team_process_id` | integer | Team-season id (takimKodu, sent as a string; key space: TBF team registrations; despite the name NOT the club id). |
| `team_name` | character | Team name as registered for that season, uppercase with Turkish letters (sponsor name: 'PİZZABULLS BORDO BANDIRMA'). |
| `games` | integer | Games played (macSayisi, sent as a string). |
| `value` | numeric | The ranked statistic: the season total for kind total, the per-game average for kind average (the API names the field grup and sends it as a string). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_leaders_efficiency-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_leaders_efficiency-example}

```python
tbf_leaders_efficiency(competition='bsl', season=2027)
```

_Last validated n/a._
