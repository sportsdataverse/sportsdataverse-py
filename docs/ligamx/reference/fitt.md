# Liga MX — ESPN FPI API (fitt v3)

> Liga MX — ESPN FPI API (fitt v3) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.soccer.ligamx` — 1 endpoint.

## espn_ligamx_fpi

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/fitt/v3/sports/soccer/mex.1/powerindex`

**Valid URL:** [https://site.web.api.espn.com/apis/fitt/v3/sports/soccer/mex.1/powerindex?season=2024](https://site.web.api.espn.com/apis/fitt/v3/sports/soccer/mex.1/powerindex?season=2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season (4-digit year) whose FPI table to return; defaults to the current season. |
| `limit` | `limit` |  |  | `Y` | Page size. The response is a single page for every league observed, so the default suffices. |
| `page` | `page` |  |  | `Y` | Page number, for the paginated envelope. |

### Returns {#espn_ligamx_fpi-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 |  |
| `team_uid` | String |  |
| `team_abbreviation` | String |  |
| `team_display_name` | String |  |
| `team_short_display_name` | String |  |
| `team_nickname` | String |  |
| `bpi` | Float64 |  |
| `bpirank` | Float64 |  |
| `bpioffense` | Float64 |  |
| `bpidefense` | Float64 |  |
| `playoffbpi` | Float64 |  |
| `offtalent` | String |  |
| `deftalent` | String |  |
| `numwins` | Float64 |  |
| `numlosses` | Float64 |  |
| `projectedw` | Float64 |  |
| `projectedl` | Float64 |  |
| `probwindiv` | Float64 |  |
| `probmakeplayoffs` | Float64 |  |
| `top6seed` | Float64 |  |
| `playinchance` | Float64 |  |
| `playoffseed` | Float64 |  |
| `projdraftslot` | Float64 |  |
| `probno1draftpick` | Float64 |  |
| `playoffbpi_playoffs` | Float64 |  |
| `playoffoff` | Float64 |  |
| `playoffdef` | Float64 |  |
| `probmakeplayoffs_playoffs` | Float64 |  |
| `probmakeconfsemi` | Float64 |  |
| `probmakeconfchamp` | Float64 |  |
| `probmaketitlegame` | Float64 |  |
| `probwintitle` | Float64 |  |
| `season.displayName` | String |  |
| `season.endDate` | String |  |
| `season.startDate` | String |  |
| `season.type.endDate` | String |  |
| `season.type.id` | String |  |
| `season.type.name` | String |  |
| `season.type.startDate` | String |  |
| `season.type.type` | Int64 |  |
| `season.year` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_ligamx_fpi-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_ligamx_fpi-example}

```python
espn_ligamx_fpi(season=2024)
```
