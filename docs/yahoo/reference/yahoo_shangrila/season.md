# Yahoo Sports — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Season

> Yahoo Sports — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Season — function reference in sdv-py, the SportsDataverse Python package.

## yahoo_season_stats_football_defense_ncaaf

Legacy player season Defense leaders (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballDefenseNcaaf`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballDefenseNcaaf](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballDefenseNcaaf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns {#yahoo_season_stats_football_defense_ncaaf-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | String |  |
| `abbreviation` | String |  |
| `sort_order` | String |  |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | String |  |
| `player_player_id` | String | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | String |  |
| `player_positions` | String | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | String | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | String | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_season_stats_football_defense_ncaaf-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_season_stats_football_defense_ncaaf-example}

```python
yahoo_season_stats_football_defense_ncaaf()
```

## yahoo_season_stats_football_kicking_ncaaf

Legacy player season Kicking leaders (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballKickingNcaaf`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballKickingNcaaf](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballKickingNcaaf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns {#yahoo_season_stats_football_kicking_ncaaf-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | String |  |
| `abbreviation` | String |  |
| `sort_order` | String |  |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | String |  |
| `player_player_id` | String | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | String |  |
| `player_positions` | String | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | String | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | String | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_season_stats_football_kicking_ncaaf-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_season_stats_football_kicking_ncaaf-example}

```python
yahoo_season_stats_football_kicking_ncaaf()
```

## yahoo_season_stats_football_passing_ncaaf

Legacy player season Passing leaders (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballPassingNcaaf`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballPassingNcaaf](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballPassingNcaaf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns {#yahoo_season_stats_football_passing_ncaaf-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | String |  |
| `abbreviation` | String |  |
| `sort_order` | String |  |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | String |  |
| `player_player_id` | String | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | String |  |
| `player_positions` | String | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | String | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | String | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_season_stats_football_passing_ncaaf-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_season_stats_football_passing_ncaaf-example}

```python
yahoo_season_stats_football_passing_ncaaf()
```

## yahoo_season_stats_football_punting_ncaaf

Legacy player season Punting leaders (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballPuntingNcaaf`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballPuntingNcaaf](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballPuntingNcaaf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns {#yahoo_season_stats_football_punting_ncaaf-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | String |  |
| `abbreviation` | String |  |
| `sort_order` | String |  |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | String |  |
| `player_player_id` | String | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | String |  |
| `player_positions` | String | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | String | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | String | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_season_stats_football_punting_ncaaf-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_season_stats_football_punting_ncaaf-example}

```python
yahoo_season_stats_football_punting_ncaaf()
```

## yahoo_season_stats_football_receiving_ncaaf

Legacy player season Receiving leaders (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballReceivingNcaaf`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballReceivingNcaaf](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballReceivingNcaaf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns {#yahoo_season_stats_football_receiving_ncaaf-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | String |  |
| `abbreviation` | String |  |
| `sort_order` | String |  |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | String |  |
| `player_player_id` | String | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | String |  |
| `player_positions` | String | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | String | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | String | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_season_stats_football_receiving_ncaaf-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_season_stats_football_receiving_ncaaf-example}

```python
yahoo_season_stats_football_receiving_ncaaf()
```

## yahoo_season_stats_football_returns_ncaaf

Legacy player season Returns leaders (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballReturnsNcaaf`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballReturnsNcaaf](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballReturnsNcaaf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns {#yahoo_season_stats_football_returns_ncaaf-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | String |  |
| `abbreviation` | String |  |
| `sort_order` | String |  |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | String |  |
| `player_player_id` | String | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | String |  |
| `player_positions` | String | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | String | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | String | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_season_stats_football_returns_ncaaf-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_season_stats_football_returns_ncaaf-example}

```python
yahoo_season_stats_football_returns_ncaaf()
```

## yahoo_season_stats_football_rushing_ncaaf

Legacy player season Rushing leaders (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballRushingNcaaf`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballRushingNcaaf](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballRushingNcaaf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns {#yahoo_season_stats_football_rushing_ncaaf-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | String |  |
| `abbreviation` | String |  |
| `sort_order` | String |  |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | String |  |
| `player_player_id` | String | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | String |  |
| `player_positions` | String | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | String | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | String | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_season_stats_football_rushing_ncaaf-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_season_stats_football_rushing_ncaaf-example}

```python
yahoo_season_stats_football_rushing_ncaaf()
```

## yahoo_season_team_stats_football_defense

Legacy team season Defense (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballDefense`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballDefense](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballDefense)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns {#yahoo_season_team_stats_football_defense-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | String |  |
| `abbreviation` | String |  |
| `sort_order` | String |  |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | String |  |
| `player_player_id` | String | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | String |  |
| `player_positions` | String | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | String | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | String | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_season_team_stats_football_defense-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_season_team_stats_football_defense-example}

```python
yahoo_season_team_stats_football_defense()
```

## yahoo_season_team_stats_football_kicking

Legacy team season Kicking (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballKicking`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballKicking](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballKicking)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns {#yahoo_season_team_stats_football_kicking-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | String |  |
| `abbreviation` | String |  |
| `sort_order` | String |  |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | String |  |
| `player_player_id` | String | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | String |  |
| `player_positions` | String | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | String | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | String | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_season_team_stats_football_kicking-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_season_team_stats_football_kicking-example}

```python
yahoo_season_team_stats_football_kicking()
```

## yahoo_season_team_stats_football_kickoffs

Legacy team season Kickoffs (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballKickoffs`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballKickoffs](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballKickoffs)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns {#yahoo_season_team_stats_football_kickoffs-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | String |  |
| `abbreviation` | String |  |
| `sort_order` | String |  |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | String |  |
| `player_player_id` | String | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | String |  |
| `player_positions` | String | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | String | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | String | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_season_team_stats_football_kickoffs-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_season_team_stats_football_kickoffs-example}

```python
yahoo_season_team_stats_football_kickoffs()
```

## yahoo_season_team_stats_football_offense

Legacy team season Offense (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballOffense`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballOffense](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballOffense)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns {#yahoo_season_team_stats_football_offense-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | String |  |
| `abbreviation` | String |  |
| `sort_order` | String |  |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | String |  |
| `player_player_id` | String | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | String |  |
| `player_positions` | String | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | String | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | String | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_season_team_stats_football_offense-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_season_team_stats_football_offense-example}

```python
yahoo_season_team_stats_football_offense()
```

## yahoo_season_team_stats_football_passing

Legacy team season Passing (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballPassing`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballPassing](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballPassing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns {#yahoo_season_team_stats_football_passing-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | String |  |
| `abbreviation` | String |  |
| `sort_order` | String |  |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | String |  |
| `player_player_id` | String | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | String |  |
| `player_positions` | String | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | String | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | String | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_season_team_stats_football_passing-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_season_team_stats_football_passing-example}

```python
yahoo_season_team_stats_football_passing()
```

## yahoo_season_team_stats_football_passing_defense

Legacy team Passing defense allowed (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballPassingDefense`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballPassingDefense](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballPassingDefense)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns {#yahoo_season_team_stats_football_passing_defense-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | String |  |
| `abbreviation` | String |  |
| `sort_order` | String |  |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | String |  |
| `player_player_id` | String | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | String |  |
| `player_positions` | String | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | String | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | String | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_season_team_stats_football_passing_defense-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_season_team_stats_football_passing_defense-example}

```python
yahoo_season_team_stats_football_passing_defense()
```

## yahoo_season_team_stats_football_punting

Legacy team season Punting (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballPunting`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballPunting](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballPunting)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns {#yahoo_season_team_stats_football_punting-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | String |  |
| `abbreviation` | String |  |
| `sort_order` | String |  |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | String |  |
| `player_player_id` | String | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | String |  |
| `player_positions` | String | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | String | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | String | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_season_team_stats_football_punting-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_season_team_stats_football_punting-example}

```python
yahoo_season_team_stats_football_punting()
```

## yahoo_season_team_stats_football_receiving

Legacy team season Receiving (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballReceiving`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballReceiving](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballReceiving)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns {#yahoo_season_team_stats_football_receiving-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | String |  |
| `abbreviation` | String |  |
| `sort_order` | String |  |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | String |  |
| `player_player_id` | String | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | String |  |
| `player_positions` | String | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | String | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | String | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_season_team_stats_football_receiving-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_season_team_stats_football_receiving-example}

```python
yahoo_season_team_stats_football_receiving()
```

## yahoo_season_team_stats_football_receiving_defense

Legacy team Receiving defense allowed (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballReceivingDefense`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballReceivingDefense](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballReceivingDefense)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns {#yahoo_season_team_stats_football_receiving_defense-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | String |  |
| `abbreviation` | String |  |
| `sort_order` | String |  |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | String |  |
| `player_player_id` | String | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | String |  |
| `player_positions` | String | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | String | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | String | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_season_team_stats_football_receiving_defense-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_season_team_stats_football_receiving_defense-example}

```python
yahoo_season_team_stats_football_receiving_defense()
```

## yahoo_season_team_stats_football_returns

Legacy team season Returns (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballReturns`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballReturns](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballReturns)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns {#yahoo_season_team_stats_football_returns-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | String |  |
| `abbreviation` | String |  |
| `sort_order` | String |  |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | String |  |
| `player_player_id` | String | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | String |  |
| `player_positions` | String | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | String | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | String | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_season_team_stats_football_returns-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_season_team_stats_football_returns-example}

```python
yahoo_season_team_stats_football_returns()
```

## yahoo_season_team_stats_football_rushing

Legacy team season Rushing (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballRushing`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballRushing](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballRushing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns {#yahoo_season_team_stats_football_rushing-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | String |  |
| `abbreviation` | String |  |
| `sort_order` | String |  |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | String |  |
| `player_player_id` | String | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | String |  |
| `player_positions` | String | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | String | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | String | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_season_team_stats_football_rushing-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_season_team_stats_football_rushing-example}

```python
yahoo_season_team_stats_football_rushing()
```

## yahoo_season_team_stats_football_rushing_defense

Legacy team Rushing defense allowed (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballRushingDefense`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballRushingDefense](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballRushingDefense)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns {#yahoo_season_team_stats_football_rushing_defense-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | String |  |
| `abbreviation` | String |  |
| `sort_order` | String |  |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | String |  |
| `player_player_id` | String | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | String |  |
| `player_positions` | String | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | String | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | String | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_season_team_stats_football_rushing_defense-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_season_team_stats_football_rushing_defense-example}

```python
yahoo_season_team_stats_football_rushing_defense()
```
