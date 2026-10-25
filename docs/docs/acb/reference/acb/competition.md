---
title: "ACB — ACB API (api2.acb.com, Spain) — Competition"
sidebar_label: "Competition"
sidebar_position: 1
description: "ACB — ACB API (api2.acb.com, Spain) — Competition — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# ACB — ACB API (api2.acb.com, Spain) — Competition

## acb_competition_editions

Every competition edition (season) the API knows, one row per edition.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/Menu/competition-data`

**Valid URL:** [https://api2.acb.com/api/matchdata/Menu/competition-data](https://api2.acb.com/api/matchdata/Menu/competition-data)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionIds` | `competition_ids` |  |  | `Y` | Competition id (1, 2, 3, 10) or a list of ids for several. Omitted = the API's default competition. |

### Returns {#acb_competition_editions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | integer | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa. |
| `competition_name` | character | The API's competition name ('Liga Nacional' for the Liga). |
| `edition_id` | integer | Season id (START year - 1935). |
| `season_start_year` | integer | Calendar year the season starts (2026 for 2026-27). |
| `season` | integer | Season END year (2027 for 2026-27) = seasonEndYear. |
| `n_rounds` | integer | Rounds the edition lists. |
| `n_weeks` | integer | Weeks the edition lists. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_competition_editions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_competition_editions-example}

```python
acb_competition_editions()
```

## acb_competition_rounds

The rounds of every competition edition: the round ids to crawl.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/Menu/competition-data`

**Valid URL:** [https://api2.acb.com/api/matchdata/Menu/competition-data](https://api2.acb.com/api/matchdata/Menu/competition-data)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionIds` | `competition_ids` |  |  | `Y` | Competition id (1, 2, 3, 10) or a list of ids for several. Omitted = the API's default competition. |

### Returns {#acb_competition_rounds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | integer | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa. |
| `edition_id` | integer | Season id the round belongs to (START year - 1935). |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `round_id` | integer | Round id: global across seasons, not the round number (6017 = Liga 2026-27 round 3; 2600 = 2016-17 round 1) |
| `round_number` | integer | Round number within the phase |
| `subphase_id` | integer | Sub-phase id inside a phase (a playoff round or a cup round, e.g. 291, 292, 293); null in the regular season |
| `subphase_number` | integer | Sub-phase number; null in the regular season |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_competition_rounds-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_competition_rounds-example}

```python
acb_competition_rounds()
```

## acb_competition_weeks

The weeks of every competition edition: the week ids to crawl.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/Menu/competition-data`

**Valid URL:** [https://api2.acb.com/api/matchdata/Menu/competition-data](https://api2.acb.com/api/matchdata/Menu/competition-data)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionIds` | `competition_ids` |  |  | `Y` | Competition id (1, 2, 3, 10) or a list of ids for several. Omitted = the API's default competition. |

### Returns {#acb_competition_weeks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | integer | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa. |
| `edition_id` | integer | Season id the week belongs to (START year - 1935). |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `week_id` | integer | Week id: global across seasons (2989 = the week of 2026-10-10) |
| `description` | character | Spanish week label ('Semana 1'). |
| `start_date` | character | Monday the week starts (UTC midnight). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_competition_weeks-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_competition_weeks-example}

```python
acb_competition_weeks()
```

## acb_competition_current

The API's current pointers: competition, edition, week, round and match.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/Menu/competition-data`

**Valid URL:** [https://api2.acb.com/api/matchdata/Menu/competition-data](https://api2.acb.com/api/matchdata/Menu/competition-data)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionIds` | `competition_ids` |  |  | `Y` | Competition id (1, 2, 3, 10) or a list of ids for several. Omitted = the API's default competition. |

### Returns {#acb_competition_current-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | integer | Competition the site opens by default. |
| `edition_id` | integer | Edition (season) the site opens by default. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `week_id` | integer | Week the site opens by default. |
| `round_id` | integer | Round the site opens by default. |
| `match_grouping` | character | round or week (current pointers) |
| `match_id` | integer | The current match (the featured game). |
| `status` | character | State of the current match. |
| `start` | character | Tip-off instant (UTC) of the current match. |
| `home_team_id` | integer | Home season-team entry id of the current match. |
| `home_club_id` | integer | Home stable club id of the current match. |
| `home_team` | character | Home team name of the current match. |
| `home_team_abbr` | character | Home team abbreviation of the current match. |
| `away_team_id` | integer | Away season-team entry id of the current match. |
| `away_club_id` | integer | Away stable club id of the current match. |
| `away_team` | character | Away team name of the current match. |
| `away_team_abbr` | character | Away team abbreviation of the current match. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_competition_current-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_competition_current-example}

```python
acb_competition_current()
```
