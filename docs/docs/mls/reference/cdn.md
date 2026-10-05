---
title: MLS — ESPN CDN API (cdn.espn.com)
sidebar_label: ESPN CDN API (cdn.espn.com)
description: "MLS — ESPN CDN API (cdn.espn.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 24
toc_max_heading_level: 2
---
# MLS — ESPN CDN API (cdn.espn.com)

`sportsdataverse.mls` — 1 endpoint.

## espn_mls_cdn_scoreboard

espn.com scoreboard page data for one day (one week for football), one row per game. The page's sbData block is a Site v2 scoreboard payload.

**Endpoint URL:** `GET https://cdn.espn.com/core/usa.1/scoreboard`

**Valid URL:** [https://cdn.espn.com/core/usa.1/scoreboard?xhr=1&date=20250115](https://cdn.espn.com/core/usa.1/scoreboard?xhr=1&date=20250115)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | Single date (YYYYMMDD). Ignored by cfb and nfl, which are week-oriented. Defaults to today. |
| `week` | `week` |  |  | `Y` | Week number (cfb and nfl). |
| `year` | `season` |  |  | `Y` | Season year that `week` belongs to (cfb and nfl). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase for `week`: 1=preseason, 2=regular season, 3=postseason (cfb and nfl). |

### Returns {#espn_mls_cdn_scoreboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | integer | ESPN event id. |
| `season` | integer | Four-digit season year. |
| `game_date` | character | ISO 8601 kickoff timestamp (UTC). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mls_cdn_scoreboard-example}

```python
espn_mls_cdn_scoreboard(date='20250115')
```

_Last validated n/a._
