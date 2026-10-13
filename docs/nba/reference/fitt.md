# NBA — ESPN FPI API (fitt v3)

> NBA — ESPN FPI API (fitt v3) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.nba` — 1 endpoint.

## espn_nba_fpi

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/fitt/v3/sports/basketball/nba/powerindex`

**Valid URL:** [https://site.web.api.espn.com/apis/fitt/v3/sports/basketball/nba/powerindex?season=2024](https://site.web.api.espn.com/apis/fitt/v3/sports/basketball/nba/powerindex?season=2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season (4-digit year) whose FPI table to return; defaults to the current season. |
| `limit` | `limit` |  |  | `Y` | Page size. The response is a single page for every league observed, so the default suffices. |
| `page` | `page` |  |  | `Y` | Page number, for the paginated envelope. |

### Returns {#espn_nba_fpi-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | character | Unique team identifier. |
| `team_uid` | character | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | character | Full team display name. |
| `team_short_display_name` | character | Short team display name (e.g. 'Aces'). |
| `team_nickname` | character | Team nickname. |
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
| `playoffseed` | double | Playoffseed. |
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

### Example {#espn_nba_fpi-example}

```python
espn_nba_fpi(season=2024)
```

_Last validated n/a._
