---
title: "NHL — NHL Records API — Coach"
sidebar_label: "Coach"
sidebar_position: 2
description: "NHL — NHL Records API — Coach — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NHL — NHL Records API — Coach

## nhl_records_coach_career

Coach career-records (regular season).

**Endpoint URL:** `GET https://records.nhl.com/site/api/coach-career-records/{coach_id}`

**Valid URL:** [https://records.nhl.com/site/api/coach-career-records](https://records.nhl.com/site/api/coach-career-records)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  |  | `Y` | coach_id path parameter. |

### Returns {#nhl_records_coach_career-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `active_coach` | Boolean | Indicates whether the coach is currently active as an NHL head coach. |
| `coach_name` | String | Full display name of the NHL head coach. |
| `end_season` | Int64 | The most recent season the coach held a head-coaching position, encoded as an eight-digit season ID. |
| `first_name` | String | Player first name. |
| `game_type_id` | Int64 | Game type identifier (regular/playoffs). |
| `games` | Int64 | Games played. |
| `home_games` | Int64 |  |
| `home_losses` | Int64 | Losses at home. |
| `home_ot_losses` | Float64 | Home overtime losses. |
| `home_ties` | Float64 | Ties at home. |
| `home_win_pctg` | Float64 | Win percentage for all regular-season home games coached. |
| `home_wins` | Int64 | Wins at home. |
| `jack_adams` | Int64 | Number of Jack Adams Award trophies won by the coach as NHL coach of the year. |
| `last_coached_date` | String | Date of the coach's most recent game on the bench, in ISO 8601 format. |
| `last_name` | String | Player last name. |
| `losses` | Int64 | Losses. |
| `losses_in_ot` | Int64 | Total regular-season games the coach's team lost in overtime. |
| `losses_in_ot_plus_shootout` | Int64 | Total regular-season games the coach's team lost in overtime or a shootout combined. |
| `losses_in_shootout` | Float64 | Total regular-season games the coach's team lost via shootout. |
| `ot_losses` | Float64 | Overtime losses. |
| `road_games` | Int64 | Total regular-season road games coached. |
| `road_losses` | Int64 | Losses on the road. |
| `road_ot_losses` | Float64 | Road overtime losses. |
| `road_ties` | Float64 | Ties on the road. |
| `road_win_pctg` | Float64 | Win percentage for all regular-season road games coached. |
| `road_wins` | Int64 | Wins on the road. |
| `seasons` | Int64 | Number of NHL seasons the coach has served as a head coach. |
| `stanley_cup_final_appearances` | Int64 | Number of times the coach has led a team to the Stanley Cup Final. |
| `stanley_cups` | Int64 | Number of Stanley Cup championships won as head coach. |
| `start_season` | Int64 | The first season the coach served as an NHL head coach, encoded as an eight-digit season ID. |
| `team_abbrevs` | String | Team abbreviation(s). |
| `ties` | Float64 | Total ties. |
| `ties_in_ot` | Int64 | Total regular-season overtime-period ties recorded under pre-shootout rules. |
| `win_pctg` | Float64 | Overall regular-season win percentage across the coach's entire career. |
| `wins` | Int64 | Wins. |
| `wins_in_ot` | Int64 | Total regular-season games the coach's team won in overtime. |
| `wins_in_ot_plus_shootout` | Int64 | Total regular-season games the coach's team won in overtime or a shootout combined. |
| `wins_in_shootout` | Float64 | Wins in shootout. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_coach_career-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_coach_career-example}

```python
nhl_records_coach_career()
```

## nhl_records_coach_career_with_playoffs

Coach career records inclusive of regular season + playoffs.

**Endpoint URL:** `GET https://records.nhl.com/site/api/coach-career-records-regular-plus-playoffs`

**Valid URL:** [https://records.nhl.com/site/api/coach-career-records-regular-plus-playoffs](https://records.nhl.com/site/api/coach-career-records-regular-plus-playoffs)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_coach_career_with_playoffs-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `active_coach` | Boolean | Indicates whether the coach is currently active as an NHL head coach. |
| `coach_id` | Int64 | ESPN coach id parsed from the `$ref` URL. |
| `coach_name` | String | Full display name of the NHL head coach. |
| `end_season` | Int64 | The most recent season the coach held a head-coaching position, encoded as an eight-digit season ID. |
| `games` | Int64 | Games played. |
| `losses` | Int64 | Losses. |
| `ot_losses` | Float64 | Overtime losses. |
| `seasons` | Int64 | Number of NHL seasons the coach has served as a head coach, including playoff appearances. |
| `start_season` | Int64 | The first season the coach served as an NHL head coach, encoded as an eight-digit season ID. |
| `team_abbrevs` | String | Team abbreviation(s). |
| `ties` | Float64 | Total ties. |
| `wins` | Int64 | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_coach_career_with_playoffs-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_coach_career_with_playoffs-example}

```python
nhl_records_coach_career_with_playoffs()
```

## nhl_records_coach_franchise

Coach records scoped to individual franchise stints.

**Endpoint URL:** `GET https://records.nhl.com/site/api/coach-franchise-records/{coach_id}`

**Valid URL:** [https://records.nhl.com/site/api/coach-franchise-records](https://records.nhl.com/site/api/coach-franchise-records)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  |  | `Y` | coach_id path parameter. |

### Returns {#nhl_records_coach_franchise-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `active_coach` | Boolean | Indicates whether the coach is currently active with the franchise. |
| `coach_name` | String | Full display name of the coach as recorded in NHL records. |
| `end_season` | Int64 | Last season (in YYYYYYYY format) the coach was behind the bench for this franchise. |
| `first_coached_date` | String | Calendar date on which the coach first handled a game for this franchise. |
| `first_name` | String | Player first name. |
| `franchise_id` | Int64 | Unique franchise identifier. |
| `franchise_name` | String | Franchise name. |
| `game_type_id` | Int64 | Game type identifier (regular/playoffs). |
| `games` | Int64 | Games played. |
| `home_games` | Int64 |  |
| `home_losses` | Int64 | Losses at home. |
| `home_ot_losses` | Float64 | Home overtime losses. |
| `home_ties` | Float64 | Ties at home. |
| `home_win_pctg` | Float64 | Fraction of home games the coach's franchise won during their tenure. |
| `home_wins` | Int64 | Wins at home. |
| `jack_adams` | Int64 | Number of Jack Adams Awards (NHL coach of the year) won by the coach during this franchise tenure. |
| `last_coached_date` | String | Calendar date of the coach's most recent game on the bench for this franchise. |
| `last_name` | String | Player last name. |
| `losses` | Int64 | Losses. |
| `losses_in_ot` | Int64 | Number of games the coach's franchise lost in overtime during the tenure. |
| `losses_in_ot_plus_shootout` | Int64 | Combined losses in overtime and shootout during the coach's franchise tenure. |
| `losses_in_shootout` | Float64 | Number of games the coach's franchise lost in a shootout during the tenure. |
| `ot_losses` | Float64 | Overtime losses. |
| `point_pctg` | Float64 | Points percentage. |
| `points` | Int64 | Total points (goals + assists). |
| `road_games` | Int64 | Total regular-season road games coached with this franchise. |
| `road_losses` | Int64 | Losses on the road. |
| `road_ot_losses` | Float64 | Road overtime losses. |
| `road_ties` | Float64 | Ties on the road. |
| `road_win_pctg` | Float64 | Fraction of away games the coach's franchise won during their tenure. |
| `road_wins` | Int64 | Wins on the road. |
| `seasons` | Int64 | Number of NHL seasons the coach spent with this franchise. |
| `stanley_cup_final_appearances` | Int64 | Number of Stanley Cup Final appearances made while coaching this franchise. |
| `stanley_cups` | Int64 | Number of Stanley Cup championships won while coaching this franchise. |
| `start_season` | Int64 | First season (in YYYYYYYY format) the coach served with this franchise. |
| `team_abbrev` | String | Team abbreviation. |
| `team_name` | String | Team name. |
| `ties` | Float64 | Total ties. |
| `ties_in_ot` | Int64 | Number of overtime ties recorded under this coach for this franchise (pre-shootout era). |
| `win_pctg` | Float64 | Overall win percentage across all regular-season games coached with this franchise. |
| `wins` | Int64 | Wins. |
| `wins_in_ot` | Int64 | Number of games the coach's franchise won in overtime during the tenure. |
| `wins_in_ot_plus_shootout` | Int64 | Combined wins in overtime and shootout during the coach's franchise tenure. |
| `wins_in_shootout` | Float64 | Wins in shootout. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_coach_franchise-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_coach_franchise-example}

```python
nhl_records_coach_franchise()
```

## nhl_records_coach_stanley_cup

Coach Stanley Cup Final win streak and consecutive-cup records.

**Endpoint URL:** `GET https://records.nhl.com/site/api/coach-stanley-cup-streak`

**Valid URL:** [https://records.nhl.com/site/api/coach-stanley-cup-streak](https://records.nhl.com/site/api/coach-stanley-cup-streak)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_coach_stanley_cup-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `active_coach` | Boolean | Boolean flag indicating whether the coach is currently active as an NHL head coach. |
| `coach_id` | Int64 | ESPN coach id parsed from the `$ref` URL. |
| `coach_name` | String | Full name of the head coach in this Stanley Cup championship record. |
| `franchise_id` | Int64 | Unique franchise identifier. |
| `franchise_name` | String | Franchise name. |
| `longest_streak` | Int64 | Maximum number of consecutive seasons in which the coach won the Stanley Cup. |
| `longest_streak_description` | String | Human-readable description of the coach's longest consecutive Stanley Cup winning streak. |
| `seasons_won` | String | Comma-separated list of NHL seasons in which the coach won the Stanley Cup as head coach. |
| `stanley_cups` | Int64 | Total number of Stanley Cup championships won by this coach as head coach. |
| `team_abbrevs` | String | Team abbreviation(s). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_coach_stanley_cup-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_coach_stanley_cup-example}

```python
nhl_records_coach_stanley_cup()
```
