---
title: "NHL — NHL Records API — Franchise"
sidebar_label: "Franchise"
sidebar_position: 3
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
| `id` | integer | Unique player identifier. |
| `first_season_id` | integer | Season identifier of the first season. |
| `full_name` | character | Player full name. |
| `last_season_id` | double | Season ID of the franchise's last season. |
| `most_recent_team_id` | integer | Most recent team identifier. |
| `team_abbrev` | character | Team abbreviation. |
| `team_common_name` | character | Team common (nickname) name. |
| `team_place_name` | character | Team place (city/location) name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_franchises-example}

```python
nhl_records_franchises()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `active` | logical | Whether athlete is currently active. |
| `captain_history` | character | Franchise captain history text. |
| `coaching_history` | character | Franchise coaching history text. |
| `date_awarded` | character | Date the franchise was awarded. |
| `directory_url` | character | Franchise directory URL. |
| `first_season_id` | integer | Season identifier of the first season. |
| `general_manager_history` | character | Franchise general manager history text. |
| `hero_image_url` | character | Franchise hero image URL. |
| `most_recent_team_id` | integer | Most recent team identifier. |
| `retired_numbers_summary` | character | Summary of retired jersey numbers. |
| `team_abbrev` | character | Team abbreviation. |
| `team_full_name` | character | Full team name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_franchise_detail-example}

```python
nhl_records_franchise_detail()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `active_franchise` | integer | Indicator of whether the franchise is active. |
| `active_team` | logical | Indicator of whether the team is active. |
| `cups` | integer | Number of Stanley Cup championships. |
| `first_season_id` | integer | Season identifier of the first season. |
| `franchise_id` | integer | Unique franchise identifier. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `game_win_pctg` | double | Game-winning percentage. |
| `games_played` | double | Games played. |
| `goals_against` | double | Goals against. |
| `goals_for` | double | Goals for. |
| `home_losses` | double | Losses at home. |
| `home_overtime_losses` | double | Overtime losses at home. |
| `home_ties` | double | Ties at home. |
| `home_wins` | double | Wins at home. |
| `last_season_id` | double | Season ID of the franchise's last season. |
| `losses` | double | Losses. |
| `overtime_losses` | double | Total overtime losses. |
| `penalty_minutes` | double | Penalty minutes. |
| `playoff_seasons` | double | Number of playoff seasons. |
| `point_pctg` | double | Points percentage. |
| `points` | double | Total points (goals + assists). |
| `road_losses` | double | Losses on the road. |
| `road_overtime_losses` | double | Overtime losses on the road. |
| `road_ties` | double | Ties on the road. |
| `road_wins` | double | Wins on the road. |
| `series_losses` | integer | Playoff series losses. |
| `series_played` | double | Playoff series played. |
| `series_win_pctg` | double | Playoff series win percentage. |
| `series_wins` | integer | Playoff series wins. |
| `shootout_losses` | double | Shootout losses. |
| `shootout_wins` | double | Shootout wins. |
| `shutouts` | double | Shutouts recorded. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Team name. |
| `ties` | double | Total ties. |
| `tri_code` | character | Team three-letter code. |
| `wins` | double | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_franchise_team_totals-example}

```python
nhl_records_franchise_team_totals()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `conference_abbrev` | character | Conference abbreviation. |
| `conference_name` | character | Conference name. |
| `conference_sequence` | integer | Team's seeding position within the conference. |
| `decision` | character | Goalie decision (W/L/O). |
| `division_abbrev` | character | Division abbreviation. |
| `division_name` | character | Division name. |
| `division_sequence` | integer | Team's seeding position within the division. |
| `final_playoff_round` | integer | Final playoff round reached. |
| `franchise_id` | integer | Unique franchise identifier. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `games_played` | integer | Games played. |
| `goals` | integer | Goals scored. |
| `goals_against` | integer | Goals against. |
| `home_losses` | integer | Losses at home. |
| `home_overtime_losses` | character | Overtime losses at home. |
| `home_ties` | integer | Ties at home. |
| `home_wins` | integer | Wins at home. |
| `in_playoffs` | logical | Whether the season reached the playoffs. |
| `league_sequence` | integer | Team's seeding position within the league. |
| `losses` | integer | Losses. |
| `overtime_losses` | character | Total overtime losses. |
| `penalty_minutes` | integer | Penalty minutes. |
| `playoff_round` | double | Playoff round identifier. |
| `points` | integer | Total points (goals + assists). |
| `road_losses` | integer | Losses on the road. |
| `road_overtime_losses` | character | Overtime losses on the road. |
| `road_ties` | integer | Ties on the road. |
| `road_wins` | integer | Wins on the road. |
| `season_id` | integer | Season identifier. |
| `series_abbrev` | character | Playoff series abbreviation. |
| `series_title` | character | Playoff series title. |
| `shutouts` | integer | Shutouts recorded. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Team name. |
| `ties` | integer | Total ties. |
| `tri_code` | character | Team three-letter code. |
| `wins` | integer | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_franchise_season_results-example}

```python
nhl_records_franchise_season_results()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `first_season_id` | integer | Season identifier of the first season. |
| `franchise_id` | integer | Unique franchise identifier. |
| `franchise_name` | character | Franchise name. |
| `playoff_seasons` | integer | Number of playoff seasons. |
| `stanley_cup_appearances` | integer | Number of Stanley Cup Final appearances. |
| `stanley_cup_wins` | integer | Number of Stanley Cup championships. |
| `years` | integer | Number of years the franchise existed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_franchise_playoff_appearances-example}

```python
nhl_records_franchise_playoff_appearances()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `active_franchise` | integer | Indicator of whether the franchise is active. |
| `cups` | integer | Number of Stanley Cup championships. |
| `first_season_id` | integer | Season identifier of the first season. |
| `franchise_id` | integer | Unique franchise identifier. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `game_win_pctg` | double | Game-winning percentage. |
| `games_played` | integer | Games played. |
| `goals_against` | integer | Goals against. |
| `goals_for` | integer | Goals for. |
| `home_losses` | integer | Losses at home. |
| `home_overtime_losses` | double | Overtime losses at home. |
| `home_ties` | double | Ties at home. |
| `home_wins` | integer | Wins at home. |
| `last_season_id` | double | Season ID of the franchise's last season. |
| `losses` | integer | Losses. |
| `overtime_losses` | double | Total overtime losses. |
| `penalty_minutes` | integer | Penalty minutes. |
| `playoff_seasons` | double | Number of playoff seasons. |
| `point_pctg` | double | Points percentage. |
| `points` | integer | Total points (goals + assists). |
| `road_losses` | integer | Losses on the road. |
| `road_overtime_losses` | double | Overtime losses on the road. |
| `road_ties` | double | Ties on the road. |
| `road_wins` | integer | Wins on the road. |
| `series_losses` | double | Playoff series losses. |
| `series_played` | double | Playoff series played. |
| `series_win_pctg` | double | Playoff series win percentage. |
| `series_wins` | double | Playoff series wins. |
| `shootout_losses` | integer | Shootout losses. |
| `shootout_wins` | integer | Shootout wins. |
| `shutouts` | integer | Shutouts recorded. |
| `team_abbrev` | character | Team abbreviation. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Team name. |
| `ties` | double | Total ties. |
| `wins` | integer | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_franchise_totals-example}

```python
nhl_records_franchise_totals()
```

_Last validated n/a._
