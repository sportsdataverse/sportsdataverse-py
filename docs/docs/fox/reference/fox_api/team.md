---
title: "FOX — Fox Sports API (api.foxsports.com) — Team"
sidebar_label: "Team"
sidebar_position: 3
description: "FOX — Fox Sports API (api.foxsports.com) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# FOX — Fox Sports API (api.foxsports.com) — Team

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
| `emphasized` | logical |  |
| `index` | integer |  |
| `priority` | integer |  |
| `sortable` | character |  |
| `template` | character |  |
| `text` | character |  |
| `weight` | double |  |

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

_Last validated n/a._

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
| `template` | character |  |
| `title` | character |  |
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `content_uri` | character |  |
| `content_type` | character |  |
| `color` | character |  |
| `logo_url` | character |  |
| `image_alt_text` | character |  |
| `rank` | character |  |
| `details` | character |  |

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

_Last validated n/a._

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
| `position_group` | character |  |
| `player` | character |  |
| `pos` | character |  |
| `age` | character | Athlete age in years. |
| `ht` | character |  |
| `wt` | character |  |
| `college` | character |  |
| `athlete_id` | character | ESPN numeric identifier for the athlete. |

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

_Last validated n/a._

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
| `section` | character |  |
| `nfc_south` | character |  |
| `v1` | character |  |
| `w_l_t` | character |  |
| `pct` | character |  |
| `pf` | character |  |
| `pa` | character |  |
| `home` | character |  |
| `away` | character |  |
| `conf` | character |  |
| `div` | character |  |
| `strk` | character |  |
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `nfc_east` | character |  |
| `nfc_north` | character |  |
| `nfc_west` | character |  |
| `afc_east` | character |  |
| `afc_north` | character |  |
| `afc_south` | character |  |
| `afc_west` | character |  |
| `national_football_conference` | character |  |
| `american_football_conference` | character |  |

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

_Last validated n/a._

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
| `condensed_uri` | character |  |
| `image_alt_text` | character |  |
| `image_type` | character |  |
| `image_url` | character |  |
| `name` | character |  |
| `selection_id` | character |  |
| `short_name` | character |  |
| `stat_abbreviation` | character |  |
| `stat_value` | character |  |
| `template` | character |  |
| `title` | character |  |
| `web_url` | character |  |

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

_Last validated n/a._
