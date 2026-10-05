---
title: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Season"
sidebar_label: "Season"
sidebar_position: 8
description: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Season — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Season

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
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_season_stats_football_defense_ncaaf-example}

```python
yahoo_season_stats_football_defense_ncaaf()
```

_Last validated n/a._

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
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_season_stats_football_kicking_ncaaf-example}

```python
yahoo_season_stats_football_kicking_ncaaf()
```

_Last validated n/a._

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
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_season_stats_football_passing_ncaaf-example}

```python
yahoo_season_stats_football_passing_ncaaf()
```

_Last validated n/a._

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
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_season_stats_football_punting_ncaaf-example}

```python
yahoo_season_stats_football_punting_ncaaf()
```

_Last validated n/a._

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
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_season_stats_football_receiving_ncaaf-example}

```python
yahoo_season_stats_football_receiving_ncaaf()
```

_Last validated n/a._

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
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_season_stats_football_returns_ncaaf-example}

```python
yahoo_season_stats_football_returns_ncaaf()
```

_Last validated n/a._

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
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_season_stats_football_rushing_ncaaf-example}

```python
yahoo_season_stats_football_rushing_ncaaf()
```

_Last validated n/a._

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
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_season_team_stats_football_defense-example}

```python
yahoo_season_team_stats_football_defense()
```

_Last validated n/a._

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
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_season_team_stats_football_kicking-example}

```python
yahoo_season_team_stats_football_kicking()
```

_Last validated n/a._

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
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_season_team_stats_football_kickoffs-example}

```python
yahoo_season_team_stats_football_kickoffs()
```

_Last validated n/a._

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
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_season_team_stats_football_offense-example}

```python
yahoo_season_team_stats_football_offense()
```

_Last validated n/a._

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
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_season_team_stats_football_passing-example}

```python
yahoo_season_team_stats_football_passing()
```

_Last validated n/a._

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
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_season_team_stats_football_passing_defense-example}

```python
yahoo_season_team_stats_football_passing_defense()
```

_Last validated n/a._

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
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_season_team_stats_football_punting-example}

```python
yahoo_season_team_stats_football_punting()
```

_Last validated n/a._

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
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_season_team_stats_football_receiving-example}

```python
yahoo_season_team_stats_football_receiving()
```

_Last validated n/a._

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
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_season_team_stats_football_receiving_defense-example}

```python
yahoo_season_team_stats_football_receiving_defense()
```

_Last validated n/a._

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
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_season_team_stats_football_returns-example}

```python
yahoo_season_team_stats_football_returns()
```

_Last validated n/a._

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
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_season_team_stats_football_rushing-example}

```python
yahoo_season_team_stats_football_rushing()
```

_Last validated n/a._

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
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_season_team_stats_football_rushing_defense-example}

```python
yahoo_season_team_stats_football_rushing_defense()
```

_Last validated n/a._
