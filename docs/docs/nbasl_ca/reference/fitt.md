---
title: NBASL_CA — ESPN FPI API (fitt v3)
sidebar_label: ESPN FPI API (fitt v3)
description: "NBASL_CA — ESPN FPI API (fitt v3) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 23
toc_max_heading_level: 2
---
# NBASL_CA — ESPN FPI API (fitt v3)

`sportsdataverse.nbasl_ca` — 1 endpoint.

## espn_nbasl_ca_fpi

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/fitt/v3/sports/basketball/nba-summer-california/powerindex`

**Valid URL:** [https://site.web.api.espn.com/apis/fitt/v3/sports/basketball/nba-summer-california/powerindex?season=2024](https://site.web.api.espn.com/apis/fitt/v3/sports/basketball/nba-summer-california/powerindex?season=2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season (4-digit year) whose FPI table to return; defaults to the current season. |
| `limit` | `limit` |  |  | `Y` | Page size. The response is a single page for every league observed, so the default suffices. |
| `page` | `page` |  |  | `Y` | Page number, for the paginated envelope. |

### Returns {#espn_nbasl_ca_fpi-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | character |  |
| `team_uid` | character |  |
| `team_abbreviation` | character |  |
| `team_display_name` | character |  |
| `team_short_display_name` | character |  |
| `team_nickname` | character |  |
| `bpi` | double |  |
| `bpirank` | double |  |
| `bpioffense` | double |  |
| `bpidefense` | double |  |
| `playoffbpi` | double |  |
| `offtalent` | character |  |
| `deftalent` | character |  |
| `numwins` | double |  |
| `numlosses` | double |  |
| `projectedw` | double |  |
| `projectedl` | double |  |
| `probwindiv` | double |  |
| `probmakeplayoffs` | double |  |
| `top6seed` | double |  |
| `playinchance` | double |  |
| `playoffseed` | double |  |
| `projdraftslot` | double |  |
| `probno1draftpick` | double |  |
| `playoffbpi_playoffs` | double |  |
| `playoffoff` | double |  |
| `playoffdef` | double |  |
| `probmakeplayoffs_playoffs` | double |  |
| `probmakeconfsemi` | double |  |
| `probmakeconfchamp` | double |  |
| `probmaketitlegame` | double |  |
| `probwintitle` | double |  |
| `season.displayName` | character |  |
| `season.endDate` | character |  |
| `season.startDate` | character |  |
| `season.type.endDate` | character |  |
| `season.type.id` | character |  |
| `season.type.name` | character |  |
| `season.type.startDate` | character |  |
| `season.type.type` | integer |  |
| `season.year` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nbasl_ca_fpi-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nbasl_ca_fpi-example}

```python
espn_nbasl_ca_fpi(season=2024)
```

_Last validated n/a._
