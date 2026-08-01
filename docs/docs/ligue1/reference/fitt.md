---
title: LIGUE1 — ESPN FPI API (fitt v3)
sidebar_label: ESPN FPI API (fitt v3)
sidebar_position: 23
---
# LIGUE1 — ESPN FPI API (fitt v3)

`sportsdataverse.ligue1` — 1 endpoint.

## `espn_ligue1_fpi`

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/fitt/v3/sports/soccer/fra.1/powerindex`

**Valid URL:** [https://site.web.api.espn.com/apis/fitt/v3/sports/soccer/fra.1/powerindex?season=2024](https://site.web.api.espn.com/apis/fitt/v3/sports/soccer/fra.1/powerindex?season=2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season (4-digit year) whose FPI table to return; defaults to the current season. |
| `limit` | `limit` |  |  | `Y` | Page size. The response is a single page for every league observed, so the default suffices. |
| `page` | `page` |  |  | `Y` | Page number, for the paginated envelope. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fpi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_ligue1_fpi(season=2024)
```

_Last validated n/a._
