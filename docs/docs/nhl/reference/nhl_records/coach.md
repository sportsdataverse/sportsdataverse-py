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
| `id` | integer | Unique player identifier. |
| `active_coach` | logical | Indicates whether the coach is currently active as an NHL head coach. |
| `coach_name` | character | Full display name of the NHL head coach. |
| `end_season` | integer | The most recent season the coach held a head-coaching position, encoded as an eight-digit season ID. |
| `first_name` | character | Player first name. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `games` | integer | Games played. |
| `home_games` | integer | Total home games. |
| `home_losses` | integer | Losses at home. |
| `home_ot_losses` | double | Home overtime losses. |
| `home_ties` | double | Ties at home. |
| `home_win_pctg` | double | Win percentage for all regular-season home games coached. |
| `home_wins` | integer | Wins at home. |
| `jack_adams` | integer | Number of Jack Adams Award trophies won by the coach as NHL coach of the year. |
| `last_coached_date` | character | Date of the coach's most recent game on the bench, in ISO 8601 format. |
| `last_name` | character | Player last name. |
| `losses` | integer | Losses. |
| `losses_in_ot` | integer | Total regular-season games the coach's team lost in overtime. |
| `losses_in_ot_plus_shootout` | integer | Total regular-season games the coach's team lost in overtime or a shootout combined. |
| `losses_in_shootout` | double | Total regular-season games the coach's team lost via shootout. |
| `ot_losses` | double | Overtime losses. |
| `road_games` | integer | Total regular-season road games coached. |
| `road_losses` | integer | Losses on the road. |
| `road_ot_losses` | double | Road overtime losses. |
| `road_ties` | double | Ties on the road. |
| `road_win_pctg` | double | Win percentage for all regular-season road games coached. |
| `road_wins` | integer | Wins on the road. |
| `seasons` | integer | Number of NHL seasons the coach has served as a head coach. |
| `stanley_cup_final_appearances` | integer | Number of times the coach has led a team to the Stanley Cup Final. |
| `stanley_cups` | integer | Number of Stanley Cup championships won as head coach. |
| `start_season` | integer | The first season the coach served as an NHL head coach, encoded as an eight-digit season ID. |
| `team_abbrevs` | character | Team abbreviation(s). |
| `ties` | double | Total ties. |
| `ties_in_ot` | integer | Total regular-season overtime-period ties recorded under pre-shootout rules. |
| `win_pctg` | double | Overall regular-season win percentage across the coach's entire career. |
| `wins` | integer | Wins. |
| `wins_in_ot` | integer | Total regular-season games the coach's team won in overtime. |
| `wins_in_ot_plus_shootout` | integer | Total regular-season games the coach's team won in overtime or a shootout combined. |
| `wins_in_shootout` | double | Wins in shootout. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_coach_career-example}

```python
nhl_records_coach_career()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `active_coach` | logical | Indicates whether the coach is currently active as an NHL head coach. |
| `coach_id` | integer | ESPN coach id parsed from the `$ref` URL. |
| `coach_name` | character | Full display name of the NHL head coach. |
| `end_season` | integer | The most recent season the coach held a head-coaching position, encoded as an eight-digit season ID. |
| `games` | integer | Games played. |
| `losses` | integer | Losses. |
| `ot_losses` | double | Overtime losses. |
| `seasons` | integer | Number of NHL seasons the coach has served as a head coach, including playoff appearances. |
| `start_season` | integer | The first season the coach served as an NHL head coach, encoded as an eight-digit season ID. |
| `team_abbrevs` | character | Team abbreviation(s). |
| `ties` | double | Total ties. |
| `wins` | integer | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_coach_career_with_playoffs-example}

```python
nhl_records_coach_career_with_playoffs()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `active_coach` | logical | Indicates whether the coach is currently active with the franchise. |
| `coach_name` | character | Full display name of the coach as recorded in NHL records. |
| `end_season` | integer | Last season (in YYYYYYYY format) the coach was behind the bench for this franchise. |
| `first_coached_date` | character | Calendar date on which the coach first handled a game for this franchise. |
| `first_name` | character | Player first name. |
| `franchise_id` | integer | Unique franchise identifier. |
| `franchise_name` | character | Franchise name. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `games` | integer | Games played. |
| `home_games` | integer | Total home games. |
| `home_losses` | integer | Losses at home. |
| `home_ot_losses` | double | Home overtime losses. |
| `home_ties` | double | Ties at home. |
| `home_win_pctg` | double | Fraction of home games the coach's franchise won during their tenure. |
| `home_wins` | integer | Wins at home. |
| `jack_adams` | integer | Number of Jack Adams Awards (NHL coach of the year) won by the coach during this franchise tenure. |
| `last_coached_date` | character | Calendar date of the coach's most recent game on the bench for this franchise. |
| `last_name` | character | Player last name. |
| `losses` | integer | Losses. |
| `losses_in_ot` | integer | Number of games the coach's franchise lost in overtime during the tenure. |
| `losses_in_ot_plus_shootout` | integer | Combined losses in overtime and shootout during the coach's franchise tenure. |
| `losses_in_shootout` | double | Number of games the coach's franchise lost in a shootout during the tenure. |
| `ot_losses` | double | Overtime losses. |
| `point_pctg` | double | Points percentage. |
| `points` | integer | Total points (goals + assists). |
| `road_games` | integer | Total regular-season road games coached with this franchise. |
| `road_losses` | integer | Losses on the road. |
| `road_ot_losses` | double | Road overtime losses. |
| `road_ties` | double | Ties on the road. |
| `road_win_pctg` | double | Fraction of away games the coach's franchise won during their tenure. |
| `road_wins` | integer | Wins on the road. |
| `seasons` | integer | Number of NHL seasons the coach spent with this franchise. |
| `stanley_cup_final_appearances` | integer | Number of Stanley Cup Final appearances made while coaching this franchise. |
| `stanley_cups` | integer | Number of Stanley Cup championships won while coaching this franchise. |
| `start_season` | integer | First season (in YYYYYYYY format) the coach served with this franchise. |
| `team_abbrev` | character | Team abbreviation. |
| `team_name` | character | Team name. |
| `ties` | double | Total ties. |
| `ties_in_ot` | integer | Number of overtime ties recorded under this coach for this franchise (pre-shootout era). |
| `win_pctg` | double | Overall win percentage across all regular-season games coached with this franchise. |
| `wins` | integer | Wins. |
| `wins_in_ot` | integer | Number of games the coach's franchise won in overtime during the tenure. |
| `wins_in_ot_plus_shootout` | integer | Combined wins in overtime and shootout during the coach's franchise tenure. |
| `wins_in_shootout` | double | Wins in shootout. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_coach_franchise-example}

```python
nhl_records_coach_franchise()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `active_coach` | logical | Boolean flag indicating whether the coach is currently active as an NHL head coach. |
| `coach_id` | integer | ESPN coach id parsed from the `$ref` URL. |
| `coach_name` | character | Full name of the head coach in this Stanley Cup championship record. |
| `franchise_id` | double | Unique franchise identifier. |
| `franchise_name` | character | Franchise name. |
| `longest_streak` | integer | Maximum number of consecutive seasons in which the coach won the Stanley Cup. |
| `longest_streak_description` | character | Human-readable description of the coach's longest consecutive Stanley Cup winning streak. |
| `seasons_won` | character | Comma-separated list of NHL seasons in which the coach won the Stanley Cup as head coach. |
| `stanley_cups` | integer | Total number of Stanley Cup championships won by this coach as head coach. |
| `team_abbrevs` | character | Team abbreviation(s). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_coach_stanley_cup-example}

```python
nhl_records_coach_stanley_cup()
```

_Last validated n/a._
