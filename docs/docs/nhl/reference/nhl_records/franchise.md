---
title: "NHL — NHL Records API — Franchise"
sidebar_label: "Franchise"
sidebar_position: 4
description: "NHL — NHL Records API — Franchise — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NHL — NHL Records API — Franchise

## nhl_records_franchises

List all NHL franchises (historical and active).

**Endpoint URL:** `GET https://records.nhl.com/site/api/franchise`

**Valid URL:** [https://records.nhl.com/site/api/franchise](https://records.nhl.com/site/api/franchise)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_franchises-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `first_season_id` | Int64 | Season identifier of the first season. |
| `full_name` | String | Player full name. |
| `last_season_id` | Int64 | Season ID of the franchise's last season. |
| `most_recent_team_id` | Int64 | Most recent team identifier. |
| `team_abbrev` | String | Team abbreviation. |
| `team_common_name` | String | Team common (nickname) name. |
| `team_place_name` | String | Team place (city/location) name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_franchises-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_franchises-example}

```python
nhl_records_franchises()
```

## nhl_records_franchise_detail

Franchise detail records (extended metadata per franchise).

**Endpoint URL:** `GET https://records.nhl.com/site/api/franchise-detail`

**Valid URL:** [https://records.nhl.com/site/api/franchise-detail](https://records.nhl.com/site/api/franchise-detail)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_franchise_detail-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `active` | Boolean | Whether athlete is currently active. |
| `captain_history` | String | Franchise captain history text. |
| `coaching_history` | String | Franchise coaching history text. |
| `date_awarded` | String | Date the franchise was awarded. |
| `directory_url` | String | Franchise directory URL. |
| `first_season_id` | Int64 | Season identifier of the first season. |
| `general_manager_history` | String | Franchise general manager history text. |
| `hero_image_url` | String | Franchise hero image URL. |
| `most_recent_team_id` | Int64 | Most recent team identifier. |
| `retired_numbers_summary` | String | Summary of retired jersey numbers. |
| `team_abbrev` | String | Team abbreviation. |
| `team_full_name` | String | Full team name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_franchise_detail-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_franchise_detail-example}

```python
nhl_records_franchise_detail()
```

## nhl_records_franchise_team_totals

All-time team totals per franchise (regular season).

**Endpoint URL:** `GET https://records.nhl.com/site/api/franchise-team-totals`

**Valid URL:** [https://records.nhl.com/site/api/franchise-team-totals](https://records.nhl.com/site/api/franchise-team-totals)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_franchise_team_totals-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `active_franchise` | Int64 | Indicator of whether the franchise is active. |
| `active_team` | Boolean | Indicator of whether the team is active. |
| `cups` | Int64 | Number of Stanley Cup championships. |
| `first_season_id` | Int64 | Season identifier of the first season. |
| `franchise_id` | Int64 | Unique franchise identifier. |
| `game_type_id` | Int64 | Game type identifier (regular/playoffs). |
| `game_win_pctg` | Float64 | Game-winning percentage. |
| `games_played` | Float64 | Games played. |
| `goals_against` | Float64 | Goals against. |
| `goals_for` | Float64 | Goals for. |
| `home_losses` | Float64 | Losses at home. |
| `home_overtime_losses` | Float64 | Overtime losses at home. |
| `home_ties` | Float64 | Ties at home. |
| `home_wins` | Float64 | Wins at home. |
| `last_season_id` | Int64 | Season ID of the franchise's last season. |
| `losses` | Float64 | Losses. |
| `overtime_losses` | Float64 | Total overtime losses. |
| `penalty_minutes` | Float64 | Penalty minutes. |
| `playoff_seasons` | Float64 | Number of playoff seasons. |
| `point_pctg` | Float64 | Points percentage. |
| `points` | Float64 | Total points (goals + assists). |
| `road_losses` | Float64 | Losses on the road. |
| `road_overtime_losses` | Float64 | Overtime losses on the road. |
| `road_ties` | Float64 | Ties on the road. |
| `road_wins` | Float64 | Wins on the road. |
| `series_losses` | Int64 | Playoff series losses. |
| `series_played` | Float64 | Playoff series played. |
| `series_win_pctg` | Float64 | Playoff series win percentage. |
| `series_wins` | Int64 | Playoff series wins. |
| `shootout_losses` | Float64 | Shootout losses. |
| `shootout_wins` | Float64 | Shootout wins. |
| `shutouts` | Float64 | Shutouts recorded. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Team name. |
| `ties` | Float64 | Total ties. |
| `tri_code` | String | Team three-letter code. |
| `wins` | Float64 | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_franchise_team_totals-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_franchise_team_totals-example}

```python
nhl_records_franchise_team_totals()
```

## nhl_records_franchise_season_results

Season-by-season results for each franchise.

**Endpoint URL:** `GET https://records.nhl.com/site/api/franchise-season-results`

**Valid URL:** [https://records.nhl.com/site/api/franchise-season-results](https://records.nhl.com/site/api/franchise-season-results)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_franchise_season_results-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `conference_abbrev` | String | Conference abbreviation. |
| `conference_name` | String | Conference name. |
| `conference_sequence` | Int64 | Team's seeding position within the conference. |
| `decision` | String | Goalie decision (W/L/O). |
| `division_abbrev` | String | Division abbreviation. |
| `division_name` | String | Division name. |
| `division_sequence` | Int64 | Team's seeding position within the division. |
| `final_playoff_round` | Int64 | Final playoff round reached. |
| `franchise_id` | Int64 | Unique franchise identifier. |
| `game_type_id` | Int64 | Game type identifier (regular/playoffs). |
| `games_played` | Int64 | Games played. |
| `goals` | Int64 | Goals scored. |
| `goals_against` | Int64 | Goals against. |
| `home_losses` | Int64 | Losses at home. |
| `home_overtime_losses` | String | Overtime losses at home. |
| `home_ties` | Int64 | Ties at home. |
| `home_wins` | Int64 | Wins at home. |
| `in_playoffs` | Boolean | Whether the season reached the playoffs. |
| `league_sequence` | Int64 | Team's seeding position within the league. |
| `losses` | Int64 | Losses. |
| `overtime_losses` | String | Total overtime losses. |
| `penalty_minutes` | Int64 | Penalty minutes. |
| `playoff_round` | Float64 | Playoff round identifier. |
| `points` | Int64 | Total points (goals + assists). |
| `road_losses` | Int64 | Losses on the road. |
| `road_overtime_losses` | String | Overtime losses on the road. |
| `road_ties` | Int64 | Ties on the road. |
| `road_wins` | Int64 | Wins on the road. |
| `season_id` | Int64 | Season identifier. |
| `series_abbrev` | String | Playoff series abbreviation. |
| `series_title` | String | Playoff series title. |
| `shutouts` | Int64 | Shutouts recorded. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Team name. |
| `ties` | Int64 | Total ties. |
| `tri_code` | String | Team three-letter code. |
| `wins` | Int64 | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_franchise_season_results-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_franchise_season_results-example}

```python
nhl_records_franchise_season_results()
```

## nhl_records_franchise_playoff_appearances

Franchise playoff appearance counts and streak information.

**Endpoint URL:** `GET https://records.nhl.com/site/api/franchise-playoff-appearances`

**Valid URL:** [https://records.nhl.com/site/api/franchise-playoff-appearances](https://records.nhl.com/site/api/franchise-playoff-appearances)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_franchise_playoff_appearances-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `first_season_id` | Int64 | Season identifier of the first season. |
| `franchise_id` | Int64 | Unique franchise identifier. |
| `franchise_name` | String | Franchise name. |
| `playoff_seasons` | Int64 | Number of playoff seasons. |
| `stanley_cup_appearances` | Int64 | Number of Stanley Cup Final appearances. |
| `stanley_cup_wins` | Int64 | Number of Stanley Cup championships. |
| `years` | Int64 | Number of years the franchise existed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_franchise_playoff_appearances-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_franchise_playoff_appearances-example}

```python
nhl_records_franchise_playoff_appearances()
```

## nhl_records_franchise_totals

League-wide franchise totals (all-time aggregate per franchise).

**Endpoint URL:** `GET https://records.nhl.com/site/api/franchise-totals`

**Valid URL:** [https://records.nhl.com/site/api/franchise-totals](https://records.nhl.com/site/api/franchise-totals)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_franchise_totals-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `active_franchise` | Int64 | Indicator of whether the franchise is active. |
| `cups` | Int64 | Number of Stanley Cup championships. |
| `first_season_id` | Int64 | Season identifier of the first season. |
| `franchise_id` | Int64 | Unique franchise identifier. |
| `game_type_id` | Int64 | Game type identifier (regular/playoffs). |
| `game_win_pctg` | Float64 | Game-winning percentage. |
| `games_played` | Int64 | Games played. |
| `goals_against` | Int64 | Goals against. |
| `goals_for` | Int64 | Goals for. |
| `home_losses` | Int64 | Losses at home. |
| `home_overtime_losses` | Float64 | Overtime losses at home. |
| `home_ties` | Float64 | Ties at home. |
| `home_wins` | Int64 | Wins at home. |
| `last_season_id` | Int64 | Season ID of the franchise's last season. |
| `losses` | Int64 | Losses. |
| `overtime_losses` | Float64 | Total overtime losses. |
| `penalty_minutes` | Int64 | Penalty minutes. |
| `playoff_seasons` | Float64 | Number of playoff seasons. |
| `point_pctg` | Float64 | Points percentage. |
| `points` | Int64 | Total points (goals + assists). |
| `road_losses` | Int64 | Losses on the road. |
| `road_overtime_losses` | Float64 | Overtime losses on the road. |
| `road_ties` | Float64 | Ties on the road. |
| `road_wins` | Int64 | Wins on the road. |
| `series_losses` | Float64 | Playoff series losses. |
| `series_played` | Float64 | Playoff series played. |
| `series_win_pctg` | Float64 | Playoff series win percentage. |
| `series_wins` | Float64 | Playoff series wins. |
| `shootout_losses` | Int64 | Shootout losses. |
| `shootout_wins` | Int64 | Shootout wins. |
| `shutouts` | Int64 | Shutouts recorded. |
| `team_abbrev` | String | Team abbreviation. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Team name. |
| `ties` | Float64 | Total ties. |
| `wins` | Int64 | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_franchise_totals-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_franchise_totals-example}

```python
nhl_records_franchise_totals()
```
