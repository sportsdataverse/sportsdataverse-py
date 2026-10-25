---
title: MLB — ESPN web API (v3)
sidebar_label: ESPN web API (v3)
description: "MLB — ESPN web API (v3) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 21
toc_max_heading_level: 2
---
# MLB — ESPN web API (v3)

`sportsdataverse.mlb` — 5 endpoints.

## espn_mlb_player_overview

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/{athlete_id}/overview`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/4239/overview](https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/4239/overview)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_mlb_player_overview-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `athlete_id` | integer | Unique ESPN athlete identifier. |
| `athlete_display_name` | character | Athlete display name (full). |
| `athlete_short_name` | character | Athlete short display name. |
| `athlete_position` | character |  |
| `athlete_jersey` | character | Athlete jersey. |
| `athlete_team_id` | integer |  |
| `athlete_team_abbreviation` | character |  |
| `split_name` | character | Split name (typically "All Splits"). |
| `split_category` | character |  |
| `games_played` | character | Games played. |
| `avg_minutes` | character |  |
| `field_goal_pct` | character |  |
| `three_point_pct` | character |  |
| `free_throw_pct` | character |  |
| `avg_rebounds` | character |  |
| `avg_assists` | character |  |
| `avg_blocks` | character |  |
| `avg_steals` | character |  |
| `avg_fouls` | character |  |
| `avg_turnovers` | character |  |
| `avg_points` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_player_overview-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_player_overview-example}

```python
espn_mlb_player_overview(athlete_id='4239')
```

_Last validated n/a._

## espn_mlb_player_stats_v3

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/{athlete_id}/stats`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/39832/stats](https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/39832/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_mlb_player_stats_v3-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `category` | character | ESPN stat category the row belongs to (e.g., basketball "averages" / "totals" / "miscellaneous", NFL "passing", MLB "career-batting"). The stat columns after `position` are named from each category's own `names` list, so they differ by sport, and the other categories' columns are null on a row. |
| `split_name` | character | Season label of the row (e.g., "2010-11") on the Web v3 payload, or "Career" on the row built from the category's career totals; the split's name on payloads that ship named splits. |
| `split_category` | character | Category of a named split; null on the Web v3 per-season rows. |
| `split_value` | character | Value of a named split; null on the Web v3 per-season rows. |
| `season` | integer | Season end year of the row (e.g., 2011 for 2010-11); null on the "Career" row. |
| `team_id` | integer | ESPN team id (Int64) the athlete played that season for; a traded player has one row per team. Null on the "Career" row. |
| `position` | character | Position abbreviation the athlete was listed at for that season and team (e.g., "G"); null on the "Career" row. |
| `games_played` | character | Games played (GP), as ESPN ships it (a string). |
| `at_bats` | character | At bats (AB), plate appearances minus walks, hit-by-pitches and sacrifices. |
| `runs` | character | Runs scored (R). |
| `hits` | character | Hits (H), singles through home runs. |
| `doubles` | character | Doubles (2B), two-base hits. |
| `triples` | character | Triples (3B), three-base hits. |
| `home_runs` | character | Home runs (HR). |
| `rb_is` | character | Runs batted in (RBI); ESPN's stat key `RBIs`, snake-cased. |
| `walks` | character | Walks (BB), bases on balls. |
| `hit_by_pitch` | character | Times hit by a pitch (HBP). |
| `strikeouts` | character | Strikeouts (SO). |
| `stolen_bases` | character | Stolen bases (SB). |
| `caught_stealing` | character | Times caught stealing (CS). |
| `avg` | character | Batting average (AVG), hits / at bats. |
| `on_base_pct` | character | On-base percentage (OBP), (hits + walks + hit-by-pitches) / plate appearances. |
| `slug_avg` | character | Slugging percentage (SLG), total bases / at bats. |
| `ops` | character | On-base plus slugging (OPS). |
| `warbr` | character | Wins above replacement (WAR); ESPN's stat key `WARBR`, snake-cased. |
| `plate_appearances` | character | Plate appearances (PA). |
| `pitches` | character | Pitches seen (P). |
| `pitches_per_plate_appearance` | character | Pitches seen per plate appearance (P/PA). |
| `extra_base_hits` | character | Extra-base hits (XBH), doubles plus triples plus home runs. |
| `total_bases` | character | Total bases (TB), 1 per single, 2 per double, 3 per triple, 4 per home run. |
| `intentional_walks` | character | Intentional walks (IBB). |
| `gid_ps` | character | Grounded into double play (GIDP); ESPN's stat key `GIDPs`, snake-cased. |
| `sac_hits` | character | Sacrifice bunts (SH). |
| `sac_flies` | character | Sacrifice flies (SF). |
| `stolen_base_pct` | character | Stolen-base success percentage (SB%). |
| `off_warbr` | character | Offensive wins above replacement (OWAR); ESPN's stat key `offWARBR`, snake-cased. |
| `runs_created` | character | Runs created (RC), an estimate of the runs a batter produced. |
| `runs_created_per27_outs` | character | Runs created per 27 outs (RC/27). |
| `isolated_power` | character | Isolated power (ISOP), slugging percentage minus batting average. |
| `secondary_avg` | character | Secondary average (SECA), extra bases from hits plus walks and stolen bases, per at bat. |
| `at_bats_per_home_run` | character | At bats per home run (AB/HR). |
| `walks_per_plate_appearance` | character | Walks per plate appearance (BB/PA). |
| `walk_to_strikeout_ratio` | character | Walks per strikeout (BB/K). |
| `ground_balls` | character | Ground-ball outs (GO). |
| `fly_balls` | character | Fly-ball outs (FO). |
| `ground_to_fly_ratio` | character | Ground outs per fly out (GO/FO). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_player_stats_v3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_player_stats_v3-example}

```python
espn_mlb_player_stats_v3(athlete_id='39832')
```

_Last validated n/a._

## espn_mlb_player_gamelog

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/{athlete_id}/gamelog`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/4239/gamelog](https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/4239/gamelog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_mlb_player_gamelog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_type_id` | integer |  |
| `season_type_name` | character |  |
| `category` | character | Category label. |
| `event_id` | integer | Unique ESPN event/game identifier. |
| `event_date` | character | Event date-time in ISO 8601 (e.g. '2017-07-11T00:00:00Z'). |
| `home_away` | character | Venue label for the team ('home' or 'away'). |
| `score` | character | Running score (away-home) after the play. |
| `opponent_id` | integer | Unique identifier for opponent. |
| `opponent_abbreviation` | character | Opponent abbreviation. |
| `opponent_display_name` | character | Display name. |
| `game_result` | character | Game result (W/L). |
| `game_processed` | character |  |
| `stat_0` | character |  |
| `stat_1` | character |  |
| `stat_2` | character |  |
| `stat_3` | character |  |
| `stat_4` | character |  |
| `stat_5` | character |  |
| `stat_6` | character |  |
| `stat_7` | character |  |
| `stat_8` | character |  |
| `stat_9` | character |  |
| `stat_10` | character |  |
| `stat_11` | character |  |
| `stat_12` | character |  |
| `stat_13` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_player_gamelog-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_player_gamelog-example}

```python
espn_mlb_player_gamelog(athlete_id='4239')
```

_Last validated n/a._

## espn_mlb_player_splits

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/{athlete_id}/splits`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/4239/splits](https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/4239/splits)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_mlb_player_splits-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `descriptions` | character |  |
| `display_name` | character | Display name. |
| `display_names` | character |  |
| `filters` | character |  |
| `labels` | character |  |
| `names` | character | Associated detail names for the type. |
| `split_categories` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_player_splits-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_player_splits-example}

```python
espn_mlb_player_splits(athlete_id='4239')
```

_Last validated n/a._

## espn_mlb_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/statistics/byathlete`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/statistics/byathlete?limit=50&page=1](https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/statistics/byathlete?limit=50&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `category` | `category` |  |  | `Y` | category query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `sort` | `sort` |  |  | `Y` | sort query parameter. |

### Returns {#espn_mlb_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `athletes` | character | Athletes. |
| `glossary` | character | Glossary. |
| `categories` | character | Categories. |
| `pagination_count` | integer | Pagination count. |
| `pagination_limit` | integer | Pagination limit. |
| `pagination_page` | integer | Pagination page. |
| `pagination_pages` | integer | Pagination pages. |
| `pagination_first` | character | Pagination first. |
| `pagination_next` | character | Pagination next. |
| `pagination_last` | character | Pagination last. |
| `league_id` | integer | League id. |
| `league_uid` | character | League uid. |
| `league_name` | character | League name. |
| `league_abbreviation` | character | League abbreviation. |
| `league_slug` | character | League slug. |
| `league_midsize_name` | character | Medium-length display name for the league, between the full name and the short abbreviation. |
| `league_short_name` | character | League short name. |
| `current_season_year` | integer | Current season year. |
| `current_season_display_name` | character | Current season display name. |
| `current_season_start_date` | character | Current season start date. |
| `current_season_end_date` | character | Current season end date. |
| `current_season_type_id` | integer | Current season type id. |
| `current_season_type_type` | integer | Current season type type. |
| `current_season_type_name` | character | Current season type name. |
| `current_season_type_start_date` | character | Current season type start date. |
| `current_season_type_end_date` | character | Current season type end date. |
| `current_season_type_week_number` | integer | Ordinal week number within the current season type. |
| `current_season_type_week_start_date` | character | ISO 8601 start date of the current week within the active season type. |
| `current_season_type_week_end_date` | character | ISO 8601 end date of the current week within the active season type (e.g., regular season, playoffs). |
| `current_season_type_week_text` | character | Human-readable label for the current week within the active season type (e.g., 'Week 14'). |
| `requested_season_year` | integer | Requested season year. |
| `requested_season_display_name` | character | Requested season display name. |
| `requested_season_start_date` | character | Requested season start date. |
| `requested_season_end_date` | character | Requested season end date. |
| `requested_season_type_id` | integer | Requested season type id. |
| `requested_season_type_type` | integer | Requested season type type. |
| `requested_season_type_name` | character | Requested season type name. |
| `requested_season_type_start_date` | character | Requested season type start date. |
| `requested_season_type_end_date` | character | Requested season type end date. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_leaders-example}

```python
espn_mlb_leaders()
```

_Last validated n/a._
