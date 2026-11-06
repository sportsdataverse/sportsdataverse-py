# Fox Sports — Fox Sports API (api.foxsports.com) — Team

> Fox Sports — Fox Sports API (api.foxsports.com) — Team — function reference in sdv-py, the SportsDataverse Python package.

## fox_api_team_gamelog

GET /bifrost/v1/{sport}/team/{team_id}/gamelog -- Fox Sports API team gamelog.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/team/{team_id}/gamelog`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/team/25/gamelog?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/team/25/gamelog?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `team_id` | `team_id` |  | `Y` |  | Numeric Fox team id, e.g. ``25``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_team_gamelog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `emphasized` | Boolean |  |
| `index` | Int64 |  |
| `priority` | Int64 |  |
| `sortable` | String |  |
| `template` | String |  |
| `text` | String |  |
| `weight` | Float64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_team_gamelog-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_team_gamelog-example}

```python
fox_api_team_gamelog(sport='nfl', team_id='25')
```

## fox_api_team_header

GET /bifrost/v1/{sport}/team/{team_id}/header -- Fox Sports API team header.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/team/{team_id}/header`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/team/25/header?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/team/25/header?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `team_id` | `team_id` |  | `Y` |  | Numeric Fox team id, e.g. ``25``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_team_header-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `template` | String |  |
| `title` | String |  |
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `content_uri` | String |  |
| `content_type` | String |  |
| `color` | String |  |
| `logo_url` | String |  |
| `image_alt_text` | String |  |
| `rank` | String |  |
| `details` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_team_header-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_team_header-example}

```python
fox_api_team_header(sport='nfl', team_id='25')
```

## fox_api_team_roster

GET /bifrost/v1/{sport}/team/{team_id}/roster -- Fox Sports API team roster.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/team/{team_id}/roster`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/team/25/roster?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/team/25/roster?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `team_id` | `team_id` |  | `Y` |  | Numeric Fox team id, e.g. ``25``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_team_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `position_group` | String |  |
| `player` | String |  |
| `pos` | String |  |
| `age` | String | Athlete age in years. |
| `ht` | String |  |
| `wt` | String |  |
| `college` | String |  |
| `athlete_id` | String | ESPN numeric identifier for the athlete. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_team_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_team_roster-example}

```python
fox_api_team_roster(sport='nfl', team_id='25')
```

## fox_api_team_standings

GET /bifrost/v1/{sport}/team/{team_id}/standings -- Fox Sports API team standings.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/team/{team_id}/standings`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/team/25/standings?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/team/25/standings?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `team_id` | `team_id` |  | `Y` |  | Numeric Fox team id, e.g. ``25``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_team_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `section` | String |  |
| `nfc_south` | String |  |
| `v1` | String |  |
| `w_l_t` | String |  |
| `pct` | String |  |
| `pf` | String |  |
| `pa` | String |  |
| `home` | String |  |
| `away` | String |  |
| `conf` | String |  |
| `div` | String |  |
| `strk` | String |  |
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `nfc_east` | String |  |
| `nfc_north` | String |  |
| `nfc_west` | String |  |
| `afc_east` | String |  |
| `afc_north` | String |  |
| `afc_south` | String |  |
| `afc_west` | String |  |
| `national_football_conference` | String |  |
| `american_football_conference` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_team_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_team_standings-example}

```python
fox_api_team_standings(sport='nfl', team_id='25')
```

## fox_api_team_stats

GET /bifrost/v1/{sport}/team/{team_id}/stats -- Fox Sports API team stats.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/team/{team_id}/stats`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/team/25/stats?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/team/25/stats?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `team_id` | `team_id` |  | `Y` |  | Numeric Fox team id, e.g. ``25``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `condensed_uri` | String |  |
| `image_alt_text` | String |  |
| `image_type` | String |  |
| `image_url` | String |  |
| `name` | String |  |
| `selection_id` | String |  |
| `short_name` | String |  |
| `stat_abbreviation` | String |  |
| `stat_value` | String |  |
| `template` | String |  |
| `title` | String |  |
| `web_url` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_team_stats-example}

```python
fox_api_team_stats(sport='nfl', team_id='25')
```
