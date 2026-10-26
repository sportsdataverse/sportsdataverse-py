---
title: "ACB — ACB API (api2.acb.com, Spain) — Other: period_scores–bracket_games"
sidebar_label: "Other: period_scores–bracket_games"
sidebar_position: 5
description: "ACB — ACB API (api2.acb.com, Spain) — Other: period_scores–bracket_games — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# ACB — ACB API (api2.acb.com, Spain) — Other: period_scores–bracket_games

## acb_period_scores

Points each team scored in each period of a game.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/MatchHeader/match-header`

**Valid URL:** [https://api2.acb.com/api/matchdata/MatchHeader/match-header?matchId=105401](https://api2.acb.com/api/matchdata/MatchHeader/match-header?matchId=105401)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `matchId` | `match_id` |  | `Y` |  | Required. ACB match id (integer), one global sequence for every competition: legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = the 2005-06 final); ids from 2020-21 start around 100000 (105401 = Barca 89-86 Valencia Basket, Liga 2026-27 round 3). |

### Returns {#acb_period_scores-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `quarter` | Int64 | Period number: 1-4 quarters, 5+ overtimes (a finished game lists every period played). |
| `home_score` | Int64 | Points the home team scored IN this period (not cumulative). |
| `away_score` | Int64 | Points the away team scored IN this period (not cumulative). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_period_scores-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_period_scores-example}

```python
acb_period_scores(match_id=105401)
```

## acb_box_info

Coaches and game facts per team: head coach, assistants, arena, attendance, referees.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/Result/boxscores`

**Valid URL:** [https://api2.acb.com/api/matchdata/Result/boxscores?matchId=105401](https://api2.acb.com/api/matchdata/Result/boxscores?matchId=105401)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `matchId` | `match_id` |  | `Y` |  | Required. ACB match id (integer), one global sequence for every competition: legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = the 2005-06 final); ids from 2020-21 start around 100000 (105401 = Barca 89-86 Valencia Basket, Liga 2026-27 round 3). |

### Returns {#acb_box_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `edition_id` | Int64 | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `team_side` | String | Side of the game the team played: home or away (the feed lists the home team first). |
| `team_id` | Int64 | Season-team entry id: one club in one competition edition, NEW every season (Barca = 4467 in Liga 2026-27). Not the club: use club_id to follow a club across seasons. |
| `club_id` | Int64 | Stable ACB club id (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket): the same across seasons and sponsor names, and the club_id request argument. |
| `team` | String | Team name as registered for that season (sponsor names change; the club id does not). |
| `team_abbr` | String | Three-letter team abbreviation (BAR, RMB, VBC). |
| `head_coach` | String | Head coach's name. |
| `assistant_coaches` | String | Assistant coaches' names joined with '; '. |
| `arena` | String | Arena where the game was played; repeats on both team rows. |
| `attendance` | Int64 | Announced attendance; repeats on both team rows. |
| `referees` | String | Referees' names joined with '; '; repeats on both team rows. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_box_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_box_info-example}

```python
acb_box_info(match_id=105401)
```

## acb_pbp

Play-by-play of a game (2016-17 onward): every shot, rebound, foul, turnover, substitution and time-out.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/PlayByPlay/play-by-play`

**Valid URL:** [https://api2.acb.com/api/matchdata/PlayByPlay/play-by-play?matchId=105401](https://api2.acb.com/api/matchdata/PlayByPlay/play-by-play?matchId=105401)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `matchId` | `match_id` |  | `Y` |  | Required. ACB match id (integer), one global sequence for every competition: legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = the 2005-06 final); ids from 2020-21 start around 100000 (105401 = Barca 89-86 Valencia Basket, Liga 2026-27 round 3). |

### Returns {#acb_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `edition_id` | Int64 | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `play_index` | Int64 | 1-based position of the event in the feed (the feed's order key is not unique). |
| `order` | Int64 | Event order key, ascending through the game (10, 301, 401, ...); not contiguous and not unique (the starting five share one). |
| `quarter` | Int64 | Period of the event: 1-4 quarters, 5+ overtimes. |
| `minute` | Int64 | Game clock left in the period, minutes part (10 at tip-off; overtimes are 5 minutes). |
| `second` | Int64 | Game clock left in the period, seconds part. |
| `clock_seconds` | Int64 | Game clock REMAINING in the period as whole seconds (minute x 60 + second; the clock counts down from 600, overtimes from 300). |
| `play_type` | Int64 | Event code (acb_codes.PLAY_TYPES decodes it; 599 = starting lineup, one row per starter at 10:00). |
| `play_name` | String | Bundle name of play_type (acb_codes.PLAY_TYPES, e.g. TWO_POINTER_MADE); null for a code outside the enum such as the minute marker 600. |
| `play_tag` | Int64 | Turnover detail code on turnovers (acb_codes.PLAY_TAGS); null otherwise. |
| `turnover_type` | String | Name of the turnover detail play_tag (acb_codes.PLAY_TAGS, e.g. BAD_PASS); null when there is no tag or the tag is unmapped. |
| `is_home` | Boolean | True when the event belongs to the home team (the API's local); false for the away team and for neutral events. |
| `score_home` | Int64 | Home score after the event. |
| `score_away` | Int64 | Away score after the event. |
| `license_type` | Int64 | Licence class of the player (202 on every player event); meaning unverified; null on team and clock events. |
| `player_id` | Int64 | ACB licence number of the player; null on team and clock events. |
| `player_name` | String | Player display name; null on team and clock events. |
| `shirt_number` | String | Shirt number as a string; null on starting-lineup rows and on team events. |
| `run_points` | Int64 | The acting player's running points AFTER this event (cumulative in the game). |
| `run_free_throws_made` | Int64 | The acting player's running free throws made AFTER this event (cumulative in the game). |
| `run_free_throws_attempted` | Int64 | The acting player's running free throws attempted AFTER this event (cumulative in the game). |
| `run_two_pointers_made` | Int64 | The acting player's running two pointers made AFTER this event (cumulative in the game). |
| `run_two_pointers_attempted` | Int64 | The acting player's running two pointers attempted AFTER this event (cumulative in the game). |
| `run_three_pointers_made` | Int64 | The acting player's running three pointers made AFTER this event (cumulative in the game). |
| `run_three_pointers_attempted` | Int64 | The acting player's running three pointers attempted AFTER this event (cumulative in the game). |
| `run_assists` | Int64 | The acting player's running assists AFTER this event (cumulative in the game). |
| `run_off_rebounds` | Int64 | The acting player's running off rebounds AFTER this event (cumulative in the game). |
| `run_def_rebounds` | Int64 | The acting player's running def rebounds AFTER this event (cumulative in the game). |
| `run_total_rebounds` | Int64 | The acting player's running total rebounds AFTER this event (cumulative in the game). |
| `run_steals` | Int64 | The acting player's running steals AFTER this event (cumulative in the game). |
| `run_turnovers` | Int64 | The acting player's running turnovers AFTER this event (cumulative in the game). |
| `run_blocks` | Int64 | The acting player's running blocks AFTER this event (cumulative in the game). |
| `run_personal_fouls` | Int64 | The acting player's running personal fouls AFTER this event (cumulative in the game). |
| `run_fouls_drawn` | Int64 | The acting player's running fouls drawn AFTER this event (cumulative in the game). |
| `series_wins` | Int64 | Home team's series wins, on the END_OF_GAME event of a 2016-17 playoff game only. |
| `series_losses` | Int64 | Home team's series losses, on the same event. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_pbp-example}

```python
acb_pbp(match_id=105401)
```

## acb_shots

Every field-goal and free-throw attempt of a game (2016-17 onward) with its court position.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/MatchShots/match-shots`

**Valid URL:** [https://api2.acb.com/api/matchdata/MatchShots/match-shots?matchId=105401](https://api2.acb.com/api/matchdata/MatchShots/match-shots?matchId=105401)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `matchId` | `match_id` |  | `Y` |  | Required. ACB match id (integer), one global sequence for every competition: legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = the 2005-06 final); ids from 2020-21 start around 100000 (105401 = Barca 89-86 Valencia Basket, Liga 2026-27 round 3). |

### Returns {#acb_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `edition_id` | Int64 | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `shot_id` | Int64 | Event id of the attempt within one match; joins nothing outside this body. |
| `pos_x` | Int64 | Court x as sent. mm system: millimetres from the attacked basket towards midcourt (never negative; 0 for free throws and dunks); grid system: 0-100 percent of the court length, not mirrored. |
| `pos_y` | Int64 | Court y as sent. mm system: millimetres across the court (sidelines at -7500 and +7500, negative = the shooter's left); grid system: 0-100 percent of the width (which sideline is 0 is unverified). Free throws are (0, 0). |
| `coord_system` | String | Shot coordinate system, told by the match id: mm = millimetres from the attacked basket (match ids from 2020-21), grid = 0-100 percent of the full court (up to 2019-20). Null when the match id is unknown. |
| `x_basket_m` | Float64 | Shot x in metres from the centre of the attacked basket towards midcourt, when coord_system is mm and the attempt is a field goal; null otherwise. Units and origin are inferred by the refs (arc and sideline fit), not documented by ACB. |
| `y_basket_m` | Float64 | Shot y in metres across the court from the basket line, negative = the shooter's left, when coord_system is mm and the attempt is a field goal; null otherwise (inferred by the refs, not documented by ACB). |
| `quarter` | Int64 | Period of the attempt: 1-4 quarters, 5+ overtimes. |
| `minute` | Int64 | Game clock left in the period, minutes |
| `second` | Int64 | Game clock left in the period, seconds part |
| `clock_seconds` | Int64 | Game clock REMAINING in the period as whole seconds (minute x 60 + second; the clock counts down from 600, overtimes from 300). |
| `play_type` | Int64 | Event code (92 free throw made, 93 two made, 94 three made, 96 free throw missed, 97 two missed, 98 three missed, 100 dunk, 533 dunk missed). |
| `play_name` | String | Bundle name of play_type (acb_codes.PLAY_TYPES, e.g. TWO_POINTER_MADE); null for a code outside the enum such as the minute marker 600. |
| `is_home` | Boolean | True when the shooter's team is the home team (the API's local). |
| `score_home` | Int64 | Home score after the attempt. |
| `score_away` | Int64 | Away score after the attempt. |
| `player_id` | Int64 | ACB licence number of the shooter. |
| `player_name` | String | Shooter's short name from the game's shooter list (e.g. 'T. Martin'). |
| `shirt_number` | String | Shooter's shirt number as a string, from the game's shooter list. |
| `zone` | String | Shot zone (acb_codes.SHOT_ZONES: restricted_area, paint, mid_range_*, corner_three_*, wing_three_*, top_three); null on free throws; legacy zones (up to 2019-20) are unreliable. |
| `made` | Boolean | True when the shot went in; null on some free throws. |
| `points` | Int64 | Points the attempt scored (2 or 3 on made field goals); null on free throws and misses. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_shots-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_shots-example}

```python
acb_shots(match_id=105401)
```

## acb_lead_tracker

Score through a game, one row per scoring moment, with the margin.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/Overview/lead-tracker`

**Valid URL:** [https://api2.acb.com/api/matchdata/Overview/lead-tracker?matchId=105401](https://api2.acb.com/api/matchdata/Overview/lead-tracker?matchId=105401)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `matchId` | `match_id` |  | `Y` |  | Required. ACB match id (integer), one global sequence for every competition: legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = the 2005-06 final); ids from 2020-21 start around 100000 (105401 = Barca 89-86 Valencia Basket, Liga 2026-27 round 3). |

### Returns {#acb_lead_tracker-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `edition_id` | Int64 | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `quarter` | Int64 | Period: 1-4, 5+ overtime |
| `minute` | Int64 | Game clock left in the period, minutes part. |
| `second` | Int64 | Game clock left in the period, seconds part. |
| `clock_seconds` | Int64 | Game clock REMAINING in the period as whole seconds (minute x 60 + second; the clock counts down from 600, overtimes from 300). |
| `home_score` | Int64 | Home score at that moment. |
| `away_score` | Int64 | Away score at that moment. |
| `margin` | Int64 | Home score minus away score at that moment (positive = the home team leads). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_lead_tracker-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_lead_tracker-example}

```python
acb_lead_tracker(match_id=105401)
```

## acb_lead_stats

Lead statistics of a game (best run, maximum lead, time ahead, lead changes) per side.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/Overview/lead-tracker`

**Valid URL:** [https://api2.acb.com/api/matchdata/Overview/lead-tracker?matchId=105401](https://api2.acb.com/api/matchdata/Overview/lead-tracker?matchId=105401)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `matchId` | `match_id` |  | `Y` |  | Required. ACB match id (integer), one global sequence for every competition: legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = the 2005-06 final); ids from 2020-21 start around 100000 (105401 = Barca 89-86 Valencia Basket, Liga 2026-27 round 3). |

### Returns {#acb_lead_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `stat` | String | Statistic: best_score_run, max_difference, time_ahead or lead_changes. |
| `side` | String | Side the value belongs to: home or away. |
| `value` | String | Display value as sent: a count, or 'mm:ss' for time_ahead. |
| `bar_progress` | Float64 | Length of the site's comparison bar (a drawing value, not a statistic). |
| `is_highlighted` | Boolean | True for the side whose bar the site highlights as the better value. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_lead_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_lead_stats-example}

```python
acb_lead_stats(match_id=105401)
```

## acb_lineup

Players of a game with their game line, starters first.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/Overview/lineup`

**Valid URL:** [https://api2.acb.com/api/matchdata/Overview/lineup?matchId=105401](https://api2.acb.com/api/matchdata/Overview/lineup?matchId=105401)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `matchId` | `match_id` |  | `Y` |  | Required. ACB match id (integer), one global sequence for every competition: legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = the 2005-06 final); ids from 2020-21 start around 100000 (105401 = Barca 89-86 Valencia Basket, Liga 2026-27 round 3). |

### Returns {#acb_lineup-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `edition_id` | Int64 | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `team_side` | String | Side of the game the team played: home or away (the feed lists the home team first). |
| `team_id` | Int64 | Season-team entry id: one club in one competition edition, NEW every season (Barca = 4467 in Liga 2026-27). Not the club: use club_id to follow a club across seasons. |
| `club_id` | Int64 | Stable ACB club id (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket): the same across seasons and sponsor names, and the club_id request argument. |
| `team` | String | Team name as registered for that season (sponsor names change; the club id does not). |
| `team_abbr` | String | Three-letter team abbreviation (BAR, RMB, VBC). |
| `lineup_order` | Int64 | 1-based position of the player in the team's list (the feed lists the starters first, then the bench). |
| `player_id` | Int64 | ACB licence number of the player. |
| `player_name` | String | Display (sporting) name ('Edy Tavares') |
| `short_name` | String | Short display name ('T. Martin'). |
| `shirt_number` | String | Shirt number, a string ('' or null when unknown) |
| `play_time` | String | Time on court in the game as 'mm:ss'. |
| `seconds_played` | Int64 | Time on court in whole seconds (play_time parsed). |
| `is_playing` | Boolean | True while the player is on court (false once the game is over). |
| `points` | Int64 | Points scored in the game. |
| `rebounds` | Int64 | Total rebounds in the game. |
| `assists` | Int64 | Assists in the game. |
| `fouls` | Int64 | Personal fouls committed in the game. |
| `differential` | Int64 | Point differential while the player was on court (plus/minus). |
| `rating` | Int64 | ACB rating (valoracion) of the game. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_lineup-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_lineup-example}

```python
acb_lineup(match_id=105401)
```

## acb_serie

The playoff series a game belongs to, one row per game of the series (zero rows outside a series: HTTP 204).

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/Overview/serie`

**Valid URL:** [https://api2.acb.com/api/matchdata/Overview/serie?matchId=17718](https://api2.acb.com/api/matchdata/Overview/serie?matchId=17718)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `matchId` | `match_id` |  | `Y` |  | Required. ACB match id (integer), one global sequence for every competition: legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = the 2005-06 final); ids from 2020-21 start around 100000 (105401 = Barca 89-86 Valencia Basket, Liga 2026-27 round 3). |

### Returns {#acb_serie-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `requested_match_id` | Int64 | The match asked for (any game of the series); every row of the series repeats it. |
| `phase_id` | Int64 | Phase id of the series (111 = Play Off Titulo). |
| `subphase_id` | Int64 | Sub-phase id of the series (the playoff stage). |
| `series_home_team_id` | Int64 | Season-team entry of the series' home-court team (new every season). |
| `series_home_club_id` | Int64 | Stable club id of the series' home-court team. |
| `series_away_team_id` | Int64 | Season-team entry of the other team of the series. |
| `series_away_club_id` | Int64 | Stable club id of the other team of the series. |
| `series_home_wins` | Int64 | Games won by the home-court team in the series (the API's homeScore). |
| `series_away_wins` | Int64 | Games won by the other team in the series (the API's awayScore). |
| `series_finished` | Boolean | True once the series is decided. |
| `best_of` | Int64 | Series length (best of N) as sent; 0 in the 2016-17 final sample, so read 0 as unknown. |
| `edition_id` | Int64 | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `match_id` | Int64 | ACB match id of this game of the series. |
| `home_team_id` | Int64 | Home season-team entry of THIS game (the series' home-court team is series_home_team_id). |
| `away_team_id` | Int64 | Away season-team entry of THIS game. |
| `home_score` | Int64 | Home points of this game. |
| `away_score` | Int64 | Away points of this game. |
| `start` | String | Tip-off instant (UTC). |
| `status` | String | Game state: FINALIZED once over. |
| `is_on_half_time` | Boolean | True during the half-time break of a live game. |
| `current_quarter` | Int64 | Current or last period of the game (4 for a finished regulation game). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_serie-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_serie-example}

```python
acb_serie(match_id=17718)
```

## acb_quickfacts

Pre-match facts per team: standing, record, recent results.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/Preview/quickfacts`

**Valid URL:** [https://api2.acb.com/api/matchdata/Preview/quickfacts?matchId=105401](https://api2.acb.com/api/matchdata/Preview/quickfacts?matchId=105401)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `matchId` | `match_id` |  | `Y` |  | Required. ACB match id (integer), one global sequence for every competition: legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = the 2005-06 final); ids from 2020-21 start around 100000 (105401 = Barca 89-86 Valencia Basket, Liga 2026-27 round 3). |

### Returns {#acb_quickfacts-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `team_side` | String | Side of the game the team played: home or away (the feed lists the home team first). |
| `team_id` | Int64 | Season-team entry id (new every season). |
| `club_id` | Int64 | Stable club id (2 = Barca, 9 = Real Madrid). |
| `team` | String | Team name for the season. |
| `team_abbr` | String | Three-letter team abbreviation. |
| `edition_id` | Int64 | Season id (START year - 1935). |
| `season` | Int64 | Season END year (edition_id + 1936). |
| `standing` | Int64 | League position before the game. |
| `wins` | Int64 | Wins before the game. |
| `losses` | Int64 | Losses before the game (the API's currentLoses). |
| `last_results` | String | Recent results joined with '; ' (WIN / LOSS), in the order the API sends them. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_quickfacts-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_quickfacts-example}

```python
acb_quickfacts(match_id=105401)
```

## acb_season_leaders

Each team's season leaders before a game, by category.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/Preview/season-leaders`

**Valid URL:** [https://api2.acb.com/api/matchdata/Preview/season-leaders?matchId=105401](https://api2.acb.com/api/matchdata/Preview/season-leaders?matchId=105401)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `matchId` | `match_id` |  | `Y` |  | Required. ACB match id (integer), one global sequence for every competition: legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = the 2005-06 final); ids from 2020-21 start around 100000 (105401 = Barca 89-86 Valencia Basket, Liga 2026-27 round 3). |

### Returns {#acb_season_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `edition_id` | Int64 | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `team_side` | String | Side of the game the team played: home or away (the feed lists the home team first). |
| `team_id` | Int64 | Season-team entry id: one club in one competition edition, NEW every season (Barca = 4467 in Liga 2026-27). Not the club: use club_id to follow a club across seasons. |
| `club_id` | Int64 | Stable ACB club id (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket): the same across seasons and sponsor names, and the club_id request argument. |
| `team` | String | Team name as registered for that season (sponsor names change; the club id does not). |
| `team_abbr` | String | Three-letter team abbreviation (BAR, RMB, VBC). |
| `metric` | String | Leader category in snake_case (the API's key, e.g. rating, points, rebounds, assists). |
| `rank` | Int64 | Place in the category list, 1 = the leader, in the order the feed lists them. |
| `value` | Float64 | The player's season value in the category before this game (a total or an average depending on the category). |
| `player_id` | Int64 | ACB licence number: the person id everywhere. A player's box-score `player.id`, his play-by-play and shot `playerLicenseId` and the `playerLicenceId` / `playerLicense` request params hold the same number (all 24 players of match 105401 match). 20209407 = Edy Tavares. Older licences start 2020..., recent ones 3000.... |
| `player_name` | String | Display (sporting) name ('Edy Tavares') |
| `first_name` | String | Legal first name(s) |
| `last_name` | String | Legal last name(s) |
| `shirt_number` | String | Shirt number, a string ('' or null when unknown) |
| `game_role` | String | Position or staff role, in Spanish: Base (point guard), Escolta (shooting guard), Alero (small forward), Ala-pivot (power forward), Pivot (centre); Entrenador (head coach), Segundo Entrenador / Entrenador Ayudante (assistant coach). Null or '' in many rows |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_season_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_season_leaders-example}

```python
acb_season_leaders(match_id=105401)
```

## acb_matchlist

The games of the round (or week) around a game.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/Menu/matchlist`

**Valid URL:** [https://api2.acb.com/api/matchdata/Menu/matchlist?matchId=105401](https://api2.acb.com/api/matchdata/Menu/matchlist?matchId=105401)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `matchId` | `match_id` |  | `Y` |  | Required. ACB match id (integer), one global sequence for every competition: legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = the 2005-06 final); ids from 2020-21 start around 100000 (105401 = Barca 89-86 Valencia Basket, Liga 2026-27 round 3). |
| `roundId` | `round_id` |  |  | `Y` | Round id (integer): global across seasons, not the round number (6017 = Liga 2026-27 round 3). List them with ``acb_competition_rounds`` or ``acb_calendar``. Omitted = the current (or last) round. |
| `weekId` | `week_id` |  |  | `Y` | Week id (integer): global across seasons (2989 = the week of 2026-10-10). List them with ``acb_competition_weeks``. |

### Returns {#acb_matchlist-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | Match id. Legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = 2005-06 final, 17718 = 2016-17 final); from 2020-21 they start at about 100000 (102920 in 2021-22, 105401 in 2026-27). One key space across competitions: the matchId param of every matchdata route. |
| `round_type` | String | Phase code: LR = Liga Regular (regular season), PO = playoffs (a bundle value, not in the samples); null on every row of /Menu/competition-data and /TeamProfile/next-matches |
| `round_id` | Int64 | Round id: global across seasons, not the round number (6017 = Liga 2026-27 round 3; 2600 = 2016-17 round 1) |
| `round_number` | Int64 | Round number within the phase |
| `group_id` | Int64 | Group id in group-stage competitions |
| `subphase_id` | Int64 | Sub-phase id inside a phase (a playoff round or a cup round, e.g. 291, 292, 293); null in the regular season |
| `subphase_number` | Int64 | Sub-phase number |
| `week_id` | Int64 | Week id: global across seasons (2989 = the week of 2026-10-10) |
| `status` | String | Match state (x-match-statuses) |
| `is_on_half_time` | Boolean | At half time (live) |
| `current_quarter` | Int64 | Current or last quarter (0 before the match and in some finished rows) |
| `home_score` | Int64 | Home points (null before the match) |
| `away_score` | Int64 | Away points (null before the match) |
| `start` | String | Tip-off, ISO 8601 UTC |
| `home_wins` | Int64 | Home team's series wins before the game (playoff series only). |
| `home_losses` | Int64 | Home team's series losses before the game (playoff series only). |
| `away_wins` | Int64 | Away team's series wins before the game (playoff series only). |
| `away_losses` | Int64 | Away team's series losses before the game (playoff series only). |
| `home_team_id` | Int64 | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `home_club_id` | Int64 | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `home_team` | String | Team name that season (sponsor names change) |
| `home_team_abbr` | String | Three-letter abbreviation (BAR, RMB) |
| `away_team_id` | Int64 | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `away_club_id` | Int64 | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `away_team` | String | Team name that season (sponsor names change) |
| `away_team_abbr` | String | Three-letter abbreviation (BAR, RMB) |
| `competition_id` | Int64 | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (x-competitions) |
| `edition_id` | Int64 | Season id (`editionId`) = season START year - 1935 (91 = 2026-27, 81 = 2016-17, 70 = 2005-06, 48 = 1983-84, the first). The sdv season (END year) is editionId + 1936. Season lists also carry `seasonStartYear` / `seasonEndYear` |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `is_selected_local` | Boolean | Null in this list (set only in a club's game list). |
| `match_grouping` | String | How the list was grouped: round or week. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_matchlist-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_matchlist-example}

```python
acb_matchlist(match_id=105401)
```

## acb_round_matches

Games of one round (or week) with scores, status and broadcasters.

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/Competition/matches`

**Valid URL:** [https://api2.acb.com/api/seasondata/Competition/matches?season=2027&roundId=6017](https://api2.acb.com/api/seasondata/Competition/matches?season=2027&roundId=6017)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionId` | `competition_id` |  |  | `Y` | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (youth). Omitted = the API's default competition. |
| `season` | `season` |  |  | `Y` | Season END year (2027 = 2026-27; the first ACB season, 1983-84, is 1984). ``acb_runtime`` sends the API's ``editionId`` = season - 1936. Omitted = the current season. |
| `roundId` | `round_id` |  |  | `Y` | Round id (integer): global across seasons, not the round number (6017 = Liga 2026-27 round 3). List them with ``acb_competition_rounds`` or ``acb_calendar``. Omitted = the current (or last) round. |
| `weekId` | `week_id` |  |  | `Y` | Week id (integer): global across seasons (2989 = the week of 2026-10-10). List them with ``acb_competition_weeks``. |
| `isRoundSelected` | `is_round_selected` |  |  | `Y` | With week_id: false = the games played that week across rounds; true resolves the week to one round. |

### Returns {#acb_round_matches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa. |
| `edition_id` | Int64 | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `round_id` | Int64 | Global round id (6017 = Liga 2026-27 round 3); not the round number. |
| `round_number` | Int64 | Round number within the phase, looked up from the body's round list. |
| `subphase_id` | Int64 | Sub-phase id of the round (playoff stages); null in the regular season. |
| `subphase_number` | Int64 | Sub-phase number of the round; null in the regular season. |
| `week_id` | Int64 | Week selected in the request (the API's selectedFilters.week); null when none. |
| `match_id` | Int64 | Match id. Legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = 2005-06 final, 17718 = 2016-17 final); from 2020-21 they start at about 100000 (102920 in 2021-22, 105401 in 2026-27). One key space across competitions: the matchId param of every matchdata route. |
| `home_team_id` | Int64 | Home season-team entry id (new every season; joins the body's team list). |
| `away_team_id` | Int64 | Away season-team entry id (new every season; joins the body's team list). |
| `home_score` | Int64 | Home points (null before the match) |
| `away_score` | Int64 | Away points (null before the match) |
| `start` | String | Tip-off, ISO 8601 UTC |
| `status` | String | Match state (x-match-statuses) |
| `is_on_half_time` | Boolean | At half time (live) |
| `current_quarter` | Int64 | Current or last quarter (5+ = overtime); not reliable after the match (1 in the 2017 series rows) |
| `home_club_id` | Int64 | Stable club id of the home team (looked up in the body's team list). |
| `home_team` | String | Home team name for the season (looked up in the body's team list). |
| `home_team_abbr` | String | Home team three-letter abbreviation (looked up in the body's team list). |
| `away_club_id` | Int64 | Stable club id of the away team (looked up in the body's team list). |
| `away_team` | String | Away team name for the season (looked up in the body's team list). |
| `away_team_abbr` | String | Away team three-letter abbreviation (looked up in the body's team list). |
| `ticketing_link` | String | Ticket shop URL; null when none. |
| `broadcasters` | String | Broadcast channel names of the game joined with '; ' (the first is the primary). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_round_matches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_round_matches-example}

```python
acb_round_matches(round_id=6017, season=2027)
```

## acb_standings

League table after a round (default: the current round).

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/Competition/standings`

**Valid URL:** [https://api2.acb.com/api/seasondata/Competition/standings?season=2027](https://api2.acb.com/api/seasondata/Competition/standings?season=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionId` | `competition_id` |  |  | `Y` | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (youth). Omitted = the API's default competition. |
| `season` | `season` |  |  | `Y` | Season END year (2027 = 2026-27; the first ACB season, 1983-84, is 1984). ``acb_runtime`` sends the API's ``editionId`` = season - 1936. Omitted = the current season. |
| `roundId` | `round_id` |  |  | `Y` | Standings after this round. Round id (integer): global across seasons, not the round number (6017 = Liga 2026-27 round 3). List them with ``acb_competition_rounds`` or ``acb_calendar``. Omitted = the current (or last) round. |

### Returns {#acb_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa. |
| `edition_id` | Int64 | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `round_id` | Int64 | Round the table is as of (the selected round). |
| `total_rounds` | Int64 | Rounds in the phase. |
| `team_id` | Int64 | Season-team entry id (new every season). |
| `club_id` | Int64 | Stable ACB club id (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket): the same across seasons and sponsor names, and the club_id request argument. |
| `team` | String | Team name as registered for that season (sponsor names change; the club id does not). |
| `team_abbr` | String | Three-letter team abbreviation (BAR, RMB, VBC). |
| `position` | Int64 | Table position (standings row) |
| `position_trend` | Int64 | Change in position since the previous round: positive = up (meaning of the sign unverified). |
| `played` | Int64 | Games played (standings row) |
| `wins` | Int64 | Wins (standings row) |
| `losses` | Int64 | Losses (the key is misspelt by the API) |
| `win_pct` | Float64 | Win percentage, 0-100. |
| `points_for` | Int64 | Points scored (standings row) |
| `points_against` | Int64 | Points allowed (standings row) |
| `plus_minus` | Int64 | Point differential (points for minus points against). |
| `home_wins` | Int64 | Wins at home (the API's localBalanceWins). |
| `home_losses` | Int64 | Losses at home. |
| `away_wins` | Int64 | Wins away from home (the API's awayBalanceWins). |
| `away_losses` | Int64 | Losses away from home (the API's awayBalanceLosses). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_standings-example}

```python
acb_standings(season=2027)
```

## acb_standings_rounds

Standings history: each team's position after each round, with the round's game.

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/Competition/standings`

**Valid URL:** [https://api2.acb.com/api/seasondata/Competition/standings?season=2027](https://api2.acb.com/api/seasondata/Competition/standings?season=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionId` | `competition_id` |  |  | `Y` | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (youth). Omitted = the API's default competition. |
| `season` | `season` |  |  | `Y` | Season END year (2027 = 2026-27; the first ACB season, 1983-84, is 1984). ``acb_runtime`` sends the API's ``editionId`` = season - 1936. Omitted = the current season. |
| `roundId` | `round_id` |  |  | `Y` | Standings after this round. Round id (integer): global across seasons, not the round number (6017 = Liga 2026-27 round 3). List them with ``acb_competition_rounds`` or ``acb_calendar``. Omitted = the current (or last) round. |

### Returns {#acb_standings_rounds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa. |
| `edition_id` | Int64 | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `team_id` | Int64 | Season-team entry id: one club in one competition edition, NEW every season (Barca = 4467 in Liga 2026-27). Not the club: use club_id to follow a club across seasons. |
| `club_id` | Int64 | Stable ACB club id (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket): the same across seasons and sponsor names, and the club_id request argument. |
| `team` | String | Team name as registered for that season (sponsor names change; the club id does not). |
| `team_abbr` | String | Three-letter team abbreviation (BAR, RMB, VBC). |
| `round_id` | Int64 | Global round id of the standing. |
| `round_number` | Int64 | Round number within the phase |
| `subphase_id` | Int64 | Sub-phase id inside a phase (a playoff round or a cup round, e.g. 291, 292, 293); null in the regular season |
| `subphase_number` | Int64 | Sub-phase number; null in the regular season |
| `position` | Int64 | The team's league position after this round. |
| `match_id` | Int64 | The team's game in this round. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_standings_rounds-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_standings_rounds-example}

```python
acb_standings_rounds(season=2027)
```

## acb_bracket

Playoff bracket: one row per series (quarter-final, semifinal, final).

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/Competition/bracket`

**Valid URL:** [https://api2.acb.com/api/seasondata/Competition/bracket?season=2026](https://api2.acb.com/api/seasondata/Competition/bracket?season=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionId` | `competition_id` |  |  | `Y` | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (youth). Omitted = the API's default competition. |
| `season` | `season` |  |  | `Y` | Season END year (2027 = 2026-27; the first ACB season, 1983-84, is 1984). ``acb_runtime`` sends the API's ``editionId`` = season - 1936. Omitted = the current season. |

### Returns {#acb_bracket-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `stage` | String | Bracket stage: quarter_finals, semifinals, final (other_positions when present). |
| `phase_id` | Int64 | Phase id of the series (111 = Play Off Titulo). |
| `subphase_id` | Int64 | Sub-phase id (the playoff stage). |
| `home_team_id` | Int64 | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `home_club_id` | Int64 | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `home_team` | String | Team name that season (sponsor names change) |
| `home_team_abbr` | String | Three-letter abbreviation (BAR, RMB) |
| `away_team_id` | Int64 | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `away_club_id` | Int64 | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `away_team` | String | Team name that season (sponsor names change) |
| `away_team_abbr` | String | Three-letter abbreviation (BAR, RMB) |
| `home_wins` | Int64 | Games won by the home-court team in the series. |
| `away_wins` | Int64 | Games won by the other team in the series. |
| `finished` | Boolean | True once the series is decided. |
| `best_of` | Int64 | Series length (best of N). |
| `n_games` | Int64 | Games listed for the series. |
| `edition_id` | Int64 | Season id (`editionId`) = season START year - 1935 (91 = 2026-27, 81 = 2016-17, 70 = 2005-06, 48 = 1983-84, the first). The sdv season (END year) is editionId + 1936. Season lists also carry `seasonStartYear` / `seasonEndYear` |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_bracket-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_bracket-example}

```python
acb_bracket(season=2026)
```

## acb_bracket_games

The games of every playoff series of a bracket, one row per game.

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/Competition/bracket`

**Valid URL:** [https://api2.acb.com/api/seasondata/Competition/bracket?season=2026](https://api2.acb.com/api/seasondata/Competition/bracket?season=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionId` | `competition_id` |  |  | `Y` | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (youth). Omitted = the API's default competition. |
| `season` | `season` |  |  | `Y` | Season END year (2027 = 2026-27; the first ACB season, 1983-84, is 1984). ``acb_runtime`` sends the API's ``editionId`` = season - 1936. Omitted = the current season. |

### Returns {#acb_bracket_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `stage` | String | Bracket stage: quarter_finals, semifinals, final, other_positions. |
| `phase_id` | Int64 | Phase id of the series (111 = Play Off Titulo). |
| `subphase_id` | Int64 | Sub-phase id of the series (the playoff stage). |
| `series_home_team_id` | Int64 | Season-team entry of the series' home-court team (new every season). |
| `series_home_club_id` | Int64 | Stable club id of the series' home-court team. |
| `series_away_team_id` | Int64 | Season-team entry of the other team of the series. |
| `series_away_club_id` | Int64 | Stable club id of the other team of the series. |
| `series_home_wins` | Int64 | Games won by the home-court team in the series (the API's homeScore). |
| `series_away_wins` | Int64 | Games won by the other team in the series (the API's awayScore). |
| `series_finished` | Boolean | True once the series is decided. |
| `best_of` | Int64 | Series length (best of N) as sent; 0 in the 2016-17 final sample, so read 0 as unknown. |
| `edition_id` | Int64 | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `match_id` | Int64 | Match id. Legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = 2005-06 final, 17718 = 2016-17 final); from 2020-21 they start at about 100000 (102920 in 2021-22, 105401 in 2026-27). One key space across competitions: the matchId param of every matchdata route. |
| `home_team_id` | Int64 | Home team. Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `away_team_id` | Int64 | Away team. Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `home_score` | Int64 | Home points (null before the match) |
| `away_score` | Int64 | Away points (null before the match) |
| `start` | String | Tip-off, ISO 8601 UTC |
| `status` | String | Match state (x-match-statuses) |
| `is_on_half_time` | Boolean | At half time (live) |
| `current_quarter` | Int64 | Current or last quarter (5+ = overtime); not reliable after the match (1 in the 2017 series rows) |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_bracket_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_bracket_games-example}

```python
acb_bracket_games(season=2026)
```
