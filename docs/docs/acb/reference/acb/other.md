---
title: "ACB — ACB API (api2.acb.com, Spain) — Other: period_scores–league_zones"
sidebar_label: "Other: period_scores–league_zones"
sidebar_position: 4
description: "ACB — ACB API (api2.acb.com, Spain) — Other: period_scores–league_zones — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# ACB — ACB API (api2.acb.com, Spain) — Other: period_scores–league_zones

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
| `match_id` | integer | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `quarter` | integer | Period number: 1-4 quarters, 5+ overtimes (a finished game lists every period played). |
| `home_score` | integer | Points the home team scored IN this period (not cumulative). |
| `away_score` | integer | Points the away team scored IN this period (not cumulative). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_period_scores-example}

```python
acb_period_scores(match_id=105401)
```

_Last validated n/a._

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
| `match_id` | integer | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `edition_id` | integer | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `team_side` | character | Side of the game the team played: home or away (the feed lists the home team first). |
| `team_id` | integer | Season-team entry id: one club in one competition edition, NEW every season (Barca = 4467 in Liga 2026-27). Not the club: use club_id to follow a club across seasons. |
| `club_id` | integer | Stable ACB club id (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket): the same across seasons and sponsor names, and the club_id request argument. |
| `team` | character | Team name as registered for that season (sponsor names change; the club id does not). |
| `team_abbr` | character | Three-letter team abbreviation (BAR, RMB, VBC). |
| `head_coach` | character | Head coach's name. |
| `assistant_coaches` | character | Assistant coaches' names joined with '; '. |
| `arena` | character | Arena where the game was played; repeats on both team rows. |
| `attendance` | integer | Announced attendance; repeats on both team rows. |
| `referees` | character | Referees' names joined with '; '; repeats on both team rows. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_box_info-example}

```python
acb_box_info(match_id=105401)
```

_Last validated n/a._

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
| `match_id` | integer | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `edition_id` | integer | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `play_index` | integer | 1-based position of the event in the feed (the feed's order key is not unique). |
| `order` | integer | Event order key, ascending through the game (10, 301, 401, ...); not contiguous and not unique (the starting five share one). |
| `quarter` | integer | Period of the event: 1-4 quarters, 5+ overtimes. |
| `minute` | integer | Game clock left in the period, minutes part (10 at tip-off; overtimes are 5 minutes). |
| `second` | integer | Game clock left in the period, seconds part. |
| `clock_seconds` | integer | Game clock REMAINING in the period as whole seconds (minute x 60 + second; the clock counts down from 600, overtimes from 300). |
| `play_type` | integer | Event code (acb_codes.PLAY_TYPES decodes it; 599 = starting lineup, one row per starter at 10:00). |
| `play_name` | character | Bundle name of play_type (acb_codes.PLAY_TYPES, e.g. TWO_POINTER_MADE); null for a code outside the enum such as the minute marker 600. |
| `play_tag` | integer | Turnover detail code on turnovers (acb_codes.PLAY_TAGS); null otherwise. |
| `turnover_type` | character | Name of the turnover detail play_tag (acb_codes.PLAY_TAGS, e.g. BAD_PASS); null when there is no tag or the tag is unmapped. |
| `is_home` | logical | True when the event belongs to the home team (the API's local); false for the away team and for neutral events. |
| `score_home` | integer | Home score after the event. |
| `score_away` | integer | Away score after the event. |
| `license_type` | integer | Licence class of the player (202 on every player event); meaning unverified; null on team and clock events. |
| `player_id` | integer | ACB licence number of the player; null on team and clock events. |
| `player_name` | character | Player display name; null on team and clock events. |
| `shirt_number` | character | Shirt number as a string; null on starting-lineup rows and on team events. |
| `run_points` | integer | The acting player's running points AFTER this event (cumulative in the game). |
| `run_free_throws_made` | integer | The acting player's running free throws made AFTER this event (cumulative in the game). |
| `run_free_throws_attempted` | integer | The acting player's running free throws attempted AFTER this event (cumulative in the game). |
| `run_two_pointers_made` | integer | The acting player's running two pointers made AFTER this event (cumulative in the game). |
| `run_two_pointers_attempted` | integer | The acting player's running two pointers attempted AFTER this event (cumulative in the game). |
| `run_three_pointers_made` | integer | The acting player's running three pointers made AFTER this event (cumulative in the game). |
| `run_three_pointers_attempted` | integer | The acting player's running three pointers attempted AFTER this event (cumulative in the game). |
| `run_assists` | integer | The acting player's running assists AFTER this event (cumulative in the game). |
| `run_off_rebounds` | integer | The acting player's running off rebounds AFTER this event (cumulative in the game). |
| `run_def_rebounds` | integer | The acting player's running def rebounds AFTER this event (cumulative in the game). |
| `run_total_rebounds` | integer | The acting player's running total rebounds AFTER this event (cumulative in the game). |
| `run_steals` | integer | The acting player's running steals AFTER this event (cumulative in the game). |
| `run_turnovers` | integer | The acting player's running turnovers AFTER this event (cumulative in the game). |
| `run_blocks` | integer | The acting player's running blocks AFTER this event (cumulative in the game). |
| `run_personal_fouls` | integer | The acting player's running personal fouls AFTER this event (cumulative in the game). |
| `run_fouls_drawn` | integer | The acting player's running fouls drawn AFTER this event (cumulative in the game). |
| `series_wins` | integer | Home team's series wins, on the END_OF_GAME event of a 2016-17 playoff game only. |
| `series_losses` | integer | Home team's series losses, on the same event. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_pbp-example}

```python
acb_pbp(match_id=105401)
```

_Last validated n/a._

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
| `match_id` | integer | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `edition_id` | integer | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `shot_id` | integer | Event id of the attempt within one match; joins nothing outside this body. |
| `pos_x` | integer | Court x as sent. mm system: millimetres from the attacked basket towards midcourt (never negative; 0 for free throws and dunks); grid system: 0-100 percent of the court length, not mirrored. |
| `pos_y` | integer | Court y as sent. mm system: millimetres across the court (sidelines at -7500 and +7500, negative = the shooter's left); grid system: 0-100 percent of the width (which sideline is 0 is unverified). Free throws are (0, 0). |
| `coord_system` | character | Shot coordinate system, told by the match id: mm = millimetres from the attacked basket (match ids from 2020-21), grid = 0-100 percent of the full court (up to 2019-20). Null when the match id is unknown. |
| `x_basket_m` | numeric | Shot x in metres from the centre of the attacked basket towards midcourt, when coord_system is mm and the attempt is a field goal; null otherwise. Units and origin are inferred by the refs (arc and sideline fit), not documented by ACB. |
| `y_basket_m` | numeric | Shot y in metres across the court from the basket line, negative = the shooter's left, when coord_system is mm and the attempt is a field goal; null otherwise (inferred by the refs, not documented by ACB). |
| `quarter` | integer | Period of the attempt: 1-4 quarters, 5+ overtimes. |
| `minute` | integer | Game clock left in the period, minutes |
| `second` | integer | Game clock left in the period, seconds part |
| `clock_seconds` | integer | Game clock REMAINING in the period as whole seconds (minute x 60 + second; the clock counts down from 600, overtimes from 300). |
| `play_type` | integer | Event code (92 free throw made, 93 two made, 94 three made, 96 free throw missed, 97 two missed, 98 three missed, 100 dunk, 533 dunk missed). |
| `play_name` | character | Bundle name of play_type (acb_codes.PLAY_TYPES, e.g. TWO_POINTER_MADE); null for a code outside the enum such as the minute marker 600. |
| `is_home` | logical | True when the shooter's team is the home team (the API's local). |
| `score_home` | integer | Home score after the attempt. |
| `score_away` | integer | Away score after the attempt. |
| `player_id` | integer | ACB licence number of the shooter. |
| `player_name` | character | Shooter's short name from the game's shooter list (e.g. 'T. Martin'). |
| `shirt_number` | character | Shooter's shirt number as a string, from the game's shooter list. |
| `zone` | character | Shot zone (acb_codes.SHOT_ZONES: restricted_area, paint, mid_range_*, corner_three_*, wing_three_*, top_three); null on free throws; legacy zones (up to 2019-20) are unreliable. |
| `made` | logical | True when the shot went in; null on some free throws. |
| `points` | integer | Points the attempt scored (2 or 3 on made field goals); null on free throws and misses. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_shots-example}

```python
acb_shots(match_id=105401)
```

_Last validated n/a._

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
| `match_id` | integer | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `edition_id` | integer | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `quarter` | integer | Period: 1-4, 5+ overtime |
| `minute` | integer | Game clock left in the period, minutes part. |
| `second` | integer | Game clock left in the period, seconds part. |
| `clock_seconds` | integer | Game clock REMAINING in the period as whole seconds (minute x 60 + second; the clock counts down from 600, overtimes from 300). |
| `home_score` | integer | Home score at that moment. |
| `away_score` | integer | Away score at that moment. |
| `margin` | integer | Home score minus away score at that moment (positive = the home team leads). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_lead_tracker-example}

```python
acb_lead_tracker(match_id=105401)
```

_Last validated n/a._

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
| `match_id` | integer | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `stat` | character | Statistic: best_score_run, max_difference, time_ahead or lead_changes. |
| `side` | character | Side the value belongs to: home or away. |
| `value` | character | Display value as sent: a count, or 'mm:ss' for time_ahead. |
| `bar_progress` | numeric | Length of the site's comparison bar (a drawing value, not a statistic). |
| `is_highlighted` | logical | True for the side whose bar the site highlights as the better value. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_lead_stats-example}

```python
acb_lead_stats(match_id=105401)
```

_Last validated n/a._

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
| `match_id` | integer | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `edition_id` | integer | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `team_side` | character | Side of the game the team played: home or away (the feed lists the home team first). |
| `team_id` | integer | Season-team entry id: one club in one competition edition, NEW every season (Barca = 4467 in Liga 2026-27). Not the club: use club_id to follow a club across seasons. |
| `club_id` | integer | Stable ACB club id (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket): the same across seasons and sponsor names, and the club_id request argument. |
| `team` | character | Team name as registered for that season (sponsor names change; the club id does not). |
| `team_abbr` | character | Three-letter team abbreviation (BAR, RMB, VBC). |
| `lineup_order` | integer | 1-based position of the player in the team's list (the feed lists the starters first, then the bench). |
| `player_id` | integer | ACB licence number of the player. |
| `player_name` | character | Display (sporting) name ('Edy Tavares') |
| `short_name` | character | Short display name ('T. Martin'). |
| `shirt_number` | character | Shirt number, a string ('' or null when unknown) |
| `play_time` | character | Time on court in the game as 'mm:ss'. |
| `seconds_played` | integer | Time on court in whole seconds (play_time parsed). |
| `is_playing` | logical | True while the player is on court (false once the game is over). |
| `points` | integer | Points scored in the game. |
| `rebounds` | integer | Total rebounds in the game. |
| `assists` | integer | Assists in the game. |
| `fouls` | integer | Personal fouls committed in the game. |
| `differential` | integer | Point differential while the player was on court (plus/minus). |
| `rating` | integer | ACB rating (valoracion) of the game. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_lineup-example}

```python
acb_lineup(match_id=105401)
```

_Last validated n/a._

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
| `requested_match_id` | integer | The match asked for (any game of the series); every row of the series repeats it. |
| `phase_id` | integer | Phase id of the series (111 = Play Off Titulo). |
| `subphase_id` | integer | Sub-phase id of the series (the playoff stage). |
| `series_home_team_id` | integer | Season-team entry of the series' home-court team (new every season). |
| `series_home_club_id` | integer | Stable club id of the series' home-court team. |
| `series_away_team_id` | integer | Season-team entry of the other team of the series. |
| `series_away_club_id` | integer | Stable club id of the other team of the series. |
| `series_home_wins` | integer | Games won by the home-court team in the series (the API's homeScore). |
| `series_away_wins` | integer | Games won by the other team in the series (the API's awayScore). |
| `series_finished` | logical | True once the series is decided. |
| `best_of` | integer | Series length (best of N) as sent; 0 in the 2016-17 final sample, so read 0 as unknown. |
| `edition_id` | integer | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `match_id` | integer | ACB match id of this game of the series. |
| `home_team_id` | integer | Home season-team entry of THIS game (the series' home-court team is series_home_team_id). |
| `away_team_id` | integer | Away season-team entry of THIS game. |
| `home_score` | integer | Home points of this game. |
| `away_score` | integer | Away points of this game. |
| `start` | character | Tip-off instant (UTC). |
| `status` | character | Game state: FINALIZED once over. |
| `is_on_half_time` | logical | True during the half-time break of a live game. |
| `current_quarter` | integer | Current or last period of the game (4 for a finished regulation game). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_serie-example}

```python
acb_serie(match_id=17718)
```

_Last validated n/a._

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
| `match_id` | integer | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `team_side` | character | Side of the game the team played: home or away (the feed lists the home team first). |
| `team_id` | integer | Season-team entry id (new every season). |
| `club_id` | integer | Stable club id (2 = Barca, 9 = Real Madrid). |
| `team` | character | Team name for the season. |
| `team_abbr` | character | Three-letter team abbreviation. |
| `edition_id` | integer | Season id (START year - 1935). |
| `season` | integer | Season END year (edition_id + 1936). |
| `standing` | integer | League position before the game. |
| `wins` | integer | Wins before the game. |
| `losses` | integer | Losses before the game (the API's currentLoses). |
| `last_results` | character | Recent results joined with '; ' (WIN / LOSS), in the order the API sends them. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_quickfacts-example}

```python
acb_quickfacts(match_id=105401)
```

_Last validated n/a._

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
| `match_id` | integer | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `edition_id` | integer | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `team_side` | character | Side of the game the team played: home or away (the feed lists the home team first). |
| `team_id` | integer | Season-team entry id: one club in one competition edition, NEW every season (Barca = 4467 in Liga 2026-27). Not the club: use club_id to follow a club across seasons. |
| `club_id` | integer | Stable ACB club id (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket): the same across seasons and sponsor names, and the club_id request argument. |
| `team` | character | Team name as registered for that season (sponsor names change; the club id does not). |
| `team_abbr` | character | Three-letter team abbreviation (BAR, RMB, VBC). |
| `metric` | character | Leader category in snake_case (the API's key, e.g. rating, points, rebounds, assists). |
| `rank` | integer | Place in the category list, 1 = the leader, in the order the feed lists them. |
| `value` | numeric | The player's season value in the category before this game (a total or an average depending on the category). |
| `player_id` | integer | ACB licence number: the person id everywhere. A player's box-score `player.id`, his play-by-play and shot `playerLicenseId` and the `playerLicenceId` / `playerLicense` request params hold the same number (all 24 players of match 105401 match). 20209407 = Edy Tavares. Older licences start 2020..., recent ones 3000.... |
| `player_name` | character | Display (sporting) name ('Edy Tavares') |
| `first_name` | character | Legal first name(s) |
| `last_name` | character | Legal last name(s) |
| `shirt_number` | character | Shirt number, a string ('' or null when unknown) |
| `game_role` | character | Position or staff role, in Spanish: Base (point guard), Escolta (shooting guard), Alero (small forward), Ala-pivot (power forward), Pivot (centre); Entrenador (head coach), Segundo Entrenador / Entrenador Ayudante (assistant coach). Null or '' in many rows |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_season_leaders-example}

```python
acb_season_leaders(match_id=105401)
```

_Last validated n/a._

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
| `match_id` | integer | Match id. Legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = 2005-06 final, 17718 = 2016-17 final); from 2020-21 they start at about 100000 (102920 in 2021-22, 105401 in 2026-27). One key space across competitions: the matchId param of every matchdata route. |
| `round_type` | character | Phase code: LR = Liga Regular (regular season), PO = playoffs (a bundle value, not in the samples); null on every row of /Menu/competition-data and /TeamProfile/next-matches |
| `round_id` | integer | Round id: global across seasons, not the round number (6017 = Liga 2026-27 round 3; 2600 = 2016-17 round 1) |
| `round_number` | integer | Round number within the phase |
| `group_id` | integer | Group id in group-stage competitions |
| `subphase_id` | integer | Sub-phase id inside a phase (a playoff round or a cup round, e.g. 291, 292, 293); null in the regular season |
| `subphase_number` | integer | Sub-phase number |
| `week_id` | integer | Week id: global across seasons (2989 = the week of 2026-10-10) |
| `status` | character | Match state (x-match-statuses) |
| `is_on_half_time` | logical | At half time (live) |
| `current_quarter` | integer | Current or last quarter (0 before the match and in some finished rows) |
| `home_score` | integer | Home points (null before the match) |
| `away_score` | integer | Away points (null before the match) |
| `start` | character | Tip-off, ISO 8601 UTC |
| `home_wins` | integer | Home team's series wins before the game (playoff series only). |
| `home_losses` | integer | Home team's series losses before the game (playoff series only). |
| `away_wins` | integer | Away team's series wins before the game (playoff series only). |
| `away_losses` | integer | Away team's series losses before the game (playoff series only). |
| `home_team_id` | integer | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `home_club_id` | integer | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `home_team` | character | Team name that season (sponsor names change) |
| `home_team_abbr` | character | Three-letter abbreviation (BAR, RMB) |
| `away_team_id` | integer | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `away_club_id` | integer | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `away_team` | character | Team name that season (sponsor names change) |
| `away_team_abbr` | character | Three-letter abbreviation (BAR, RMB) |
| `competition_id` | integer | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (x-competitions) |
| `edition_id` | integer | Season id (`editionId`) = season START year - 1935 (91 = 2026-27, 81 = 2016-17, 70 = 2005-06, 48 = 1983-84, the first). The sdv season (END year) is editionId + 1936. Season lists also carry `seasonStartYear` / `seasonEndYear` |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `is_selected_local` | logical | Null in this list (set only in a club's game list). |
| `match_grouping` | character | How the list was grouped: round or week. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_matchlist-example}

```python
acb_matchlist(match_id=105401)
```

_Last validated n/a._

## acb_competition_editions

Every competition edition (season) the API knows, one row per edition.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/Menu/competition-data`

**Valid URL:** [https://api2.acb.com/api/matchdata/Menu/competition-data](https://api2.acb.com/api/matchdata/Menu/competition-data)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionIds` | `competition_ids` |  |  | `Y` | Competition id (1, 2, 3, 10) or a list of ids for several. Omitted = the API's default competition. |

### Returns {#acb_competition_editions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | integer | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa. |
| `competition_name` | character | The API's competition name ('Liga Nacional' for the Liga). |
| `edition_id` | integer | Season id (START year - 1935). |
| `season_start_year` | integer | Calendar year the season starts (2026 for 2026-27). |
| `season` | integer | Season END year (2027 for 2026-27) = seasonEndYear. |
| `n_rounds` | integer | Rounds the edition lists. |
| `n_weeks` | integer | Weeks the edition lists. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_competition_editions-example}

```python
acb_competition_editions()
```

_Last validated n/a._

## acb_competition_rounds

The rounds of every competition edition: the round ids to crawl.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/Menu/competition-data`

**Valid URL:** [https://api2.acb.com/api/matchdata/Menu/competition-data](https://api2.acb.com/api/matchdata/Menu/competition-data)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionIds` | `competition_ids` |  |  | `Y` | Competition id (1, 2, 3, 10) or a list of ids for several. Omitted = the API's default competition. |

### Returns {#acb_competition_rounds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | integer | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa. |
| `edition_id` | integer | Season id the round belongs to (START year - 1935). |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `round_id` | integer | Round id: global across seasons, not the round number (6017 = Liga 2026-27 round 3; 2600 = 2016-17 round 1) |
| `round_number` | integer | Round number within the phase |
| `subphase_id` | integer | Sub-phase id inside a phase (a playoff round or a cup round, e.g. 291, 292, 293); null in the regular season |
| `subphase_number` | integer | Sub-phase number; null in the regular season |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_competition_rounds-example}

```python
acb_competition_rounds()
```

_Last validated n/a._

## acb_competition_weeks

The weeks of every competition edition: the week ids to crawl.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/Menu/competition-data`

**Valid URL:** [https://api2.acb.com/api/matchdata/Menu/competition-data](https://api2.acb.com/api/matchdata/Menu/competition-data)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionIds` | `competition_ids` |  |  | `Y` | Competition id (1, 2, 3, 10) or a list of ids for several. Omitted = the API's default competition. |

### Returns {#acb_competition_weeks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | integer | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa. |
| `edition_id` | integer | Season id the week belongs to (START year - 1935). |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `week_id` | integer | Week id: global across seasons (2989 = the week of 2026-10-10) |
| `description` | character | Spanish week label ('Semana 1'). |
| `start_date` | character | Monday the week starts (UTC midnight). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_competition_weeks-example}

```python
acb_competition_weeks()
```

_Last validated n/a._

## acb_competition_current

The API's current pointers: competition, edition, week, round and match.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/Menu/competition-data`

**Valid URL:** [https://api2.acb.com/api/matchdata/Menu/competition-data](https://api2.acb.com/api/matchdata/Menu/competition-data)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionIds` | `competition_ids` |  |  | `Y` | Competition id (1, 2, 3, 10) or a list of ids for several. Omitted = the API's default competition. |

### Returns {#acb_competition_current-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | integer | Competition the site opens by default. |
| `edition_id` | integer | Edition (season) the site opens by default. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `week_id` | integer | Week the site opens by default. |
| `round_id` | integer | Round the site opens by default. |
| `match_grouping` | character | round or week (current pointers) |
| `match_id` | integer | The current match (the featured game). |
| `status` | character | State of the current match. |
| `start` | character | Tip-off instant (UTC) of the current match. |
| `home_team_id` | integer | Home season-team entry id of the current match. |
| `home_club_id` | integer | Home stable club id of the current match. |
| `home_team` | character | Home team name of the current match. |
| `home_team_abbr` | character | Home team abbreviation of the current match. |
| `away_team_id` | integer | Away season-team entry id of the current match. |
| `away_club_id` | integer | Away stable club id of the current match. |
| `away_team` | character | Away team name of the current match. |
| `away_team_abbr` | character | Away team abbreviation of the current match. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_competition_current-example}

```python
acb_competition_current()
```

_Last validated n/a._

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
| `competition_id` | integer | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa. |
| `edition_id` | integer | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `round_id` | integer | Global round id (6017 = Liga 2026-27 round 3); not the round number. |
| `round_number` | integer | Round number within the phase, looked up from the body's round list. |
| `subphase_id` | integer | Sub-phase id of the round (playoff stages); null in the regular season. |
| `subphase_number` | integer | Sub-phase number of the round; null in the regular season. |
| `week_id` | integer | Week selected in the request (the API's selectedFilters.week); null when none. |
| `match_id` | integer | Match id. Legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = 2005-06 final, 17718 = 2016-17 final); from 2020-21 they start at about 100000 (102920 in 2021-22, 105401 in 2026-27). One key space across competitions: the matchId param of every matchdata route. |
| `home_team_id` | integer | Home season-team entry id (new every season; joins the body's team list). |
| `away_team_id` | integer | Away season-team entry id (new every season; joins the body's team list). |
| `home_score` | integer | Home points (null before the match) |
| `away_score` | integer | Away points (null before the match) |
| `start` | character | Tip-off, ISO 8601 UTC |
| `status` | character | Match state (x-match-statuses) |
| `is_on_half_time` | logical | At half time (live) |
| `current_quarter` | integer | Current or last quarter (5+ = overtime); not reliable after the match (1 in the 2017 series rows) |
| `home_club_id` | integer | Stable club id of the home team (looked up in the body's team list). |
| `home_team` | character | Home team name for the season (looked up in the body's team list). |
| `home_team_abbr` | character | Home team three-letter abbreviation (looked up in the body's team list). |
| `away_club_id` | integer | Stable club id of the away team (looked up in the body's team list). |
| `away_team` | character | Away team name for the season (looked up in the body's team list). |
| `away_team_abbr` | character | Away team three-letter abbreviation (looked up in the body's team list). |
| `ticketing_link` | character | Ticket shop URL; null when none. |
| `broadcasters` | character | Broadcast channel names of the game joined with '; ' (the first is the primary). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_round_matches-example}

```python
acb_round_matches(round_id=6017, season=2027)
```

_Last validated n/a._

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
| `competition_id` | integer | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa. |
| `edition_id` | integer | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `round_id` | integer | Round the table is as of (the selected round). |
| `total_rounds` | integer | Rounds in the phase. |
| `team_id` | integer | Season-team entry id (new every season). |
| `club_id` | integer | Stable ACB club id (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket): the same across seasons and sponsor names, and the club_id request argument. |
| `team` | character | Team name as registered for that season (sponsor names change; the club id does not). |
| `team_abbr` | character | Three-letter team abbreviation (BAR, RMB, VBC). |
| `position` | integer | Table position (standings row) |
| `position_trend` | integer | Change in position since the previous round: positive = up (meaning of the sign unverified). |
| `played` | integer | Games played (standings row) |
| `wins` | integer | Wins (standings row) |
| `losses` | integer | Losses (the key is misspelt by the API) |
| `win_pct` | numeric | Win percentage, 0-100. |
| `points_for` | integer | Points scored (standings row) |
| `points_against` | integer | Points allowed (standings row) |
| `plus_minus` | integer | Point differential (points for minus points against). |
| `home_wins` | integer | Wins at home (the API's localBalanceWins). |
| `home_losses` | integer | Losses at home. |
| `away_wins` | integer | Wins away from home (the API's awayBalanceWins). |
| `away_losses` | integer | Losses away from home (the API's awayBalanceLosses). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_standings-example}

```python
acb_standings(season=2027)
```

_Last validated n/a._

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
| `competition_id` | integer | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa. |
| `edition_id` | integer | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `team_id` | integer | Season-team entry id: one club in one competition edition, NEW every season (Barca = 4467 in Liga 2026-27). Not the club: use club_id to follow a club across seasons. |
| `club_id` | integer | Stable ACB club id (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket): the same across seasons and sponsor names, and the club_id request argument. |
| `team` | character | Team name as registered for that season (sponsor names change; the club id does not). |
| `team_abbr` | character | Three-letter team abbreviation (BAR, RMB, VBC). |
| `round_id` | integer | Global round id of the standing. |
| `round_number` | integer | Round number within the phase |
| `subphase_id` | integer | Sub-phase id inside a phase (a playoff round or a cup round, e.g. 291, 292, 293); null in the regular season |
| `subphase_number` | integer | Sub-phase number; null in the regular season |
| `position` | integer | The team's league position after this round. |
| `match_id` | integer | The team's game in this round. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_standings_rounds-example}

```python
acb_standings_rounds(season=2027)
```

_Last validated n/a._

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
| `stage` | character | Bracket stage: quarter_finals, semifinals, final (other_positions when present). |
| `phase_id` | integer | Phase id of the series (111 = Play Off Titulo). |
| `subphase_id` | integer | Sub-phase id (the playoff stage). |
| `home_team_id` | integer | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `home_club_id` | integer | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `home_team` | character | Team name that season (sponsor names change) |
| `home_team_abbr` | character | Three-letter abbreviation (BAR, RMB) |
| `away_team_id` | integer | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `away_club_id` | integer | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `away_team` | character | Team name that season (sponsor names change) |
| `away_team_abbr` | character | Three-letter abbreviation (BAR, RMB) |
| `home_wins` | integer | Games won by the home-court team in the series. |
| `away_wins` | integer | Games won by the other team in the series. |
| `finished` | logical | True once the series is decided. |
| `best_of` | integer | Series length (best of N). |
| `n_games` | integer | Games listed for the series. |
| `edition_id` | integer | Season id (`editionId`) = season START year - 1935 (91 = 2026-27, 81 = 2016-17, 70 = 2005-06, 48 = 1983-84, the first). The sdv season (END year) is editionId + 1936. Season lists also carry `seasonStartYear` / `seasonEndYear` |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_bracket-example}

```python
acb_bracket(season=2026)
```

_Last validated n/a._

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
| `stage` | character | Bracket stage: quarter_finals, semifinals, final, other_positions. |
| `phase_id` | integer | Phase id of the series (111 = Play Off Titulo). |
| `subphase_id` | integer | Sub-phase id of the series (the playoff stage). |
| `series_home_team_id` | integer | Season-team entry of the series' home-court team (new every season). |
| `series_home_club_id` | integer | Stable club id of the series' home-court team. |
| `series_away_team_id` | integer | Season-team entry of the other team of the series. |
| `series_away_club_id` | integer | Stable club id of the other team of the series. |
| `series_home_wins` | integer | Games won by the home-court team in the series (the API's homeScore). |
| `series_away_wins` | integer | Games won by the other team in the series (the API's awayScore). |
| `series_finished` | logical | True once the series is decided. |
| `best_of` | integer | Series length (best of N) as sent; 0 in the 2016-17 final sample, so read 0 as unknown. |
| `edition_id` | integer | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `match_id` | integer | Match id. Legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = 2005-06 final, 17718 = 2016-17 final); from 2020-21 they start at about 100000 (102920 in 2021-22, 105401 in 2026-27). One key space across competitions: the matchId param of every matchdata route. |
| `home_team_id` | integer | Home team. Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `away_team_id` | integer | Away team. Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `home_score` | integer | Home points (null before the match) |
| `away_score` | integer | Away points (null before the match) |
| `start` | character | Tip-off, ISO 8601 UTC |
| `status` | character | Match state (x-match-statuses) |
| `is_on_half_time` | logical | At half time (live) |
| `current_quarter` | integer | Current or last quarter (5+ = overtime); not reliable after the match (1 in the 2017 series rows) |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_bracket_games-example}

```python
acb_bracket_games(season=2026)
```

_Last validated n/a._

## acb_calendar

Every round of a season with its games: one call lists every game id.

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/Calendar/season-calendar`

**Valid URL:** [https://api2.acb.com/api/seasondata/Calendar/season-calendar?season=2027](https://api2.acb.com/api/seasondata/Calendar/season-calendar?season=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionId` | `competition_id` |  |  | `Y` | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (youth). Omitted = the API's default competition. |
| `season` | `season` |  |  | `Y` | Season END year (2027 = 2026-27; the first ACB season, 1983-84, is 1984). ``acb_runtime`` sends the API's ``editionId`` = season - 1936. Omitted = the current season. |

### Returns {#acb_calendar-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | integer | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa. |
| `edition_id` | integer | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `round_id` | integer | Global round id (6017 = Liga 2026-27 round 3); not the round number. |
| `round_number` | integer | Round number within its phase. |
| `subphase_id` | integer | Sub-phase id of the round (playoff stages); null in the regular season. |
| `subphase_number` | integer | Sub-phase number of the round; null in the regular season. |
| `week_id` | integer | Not sent by the calendar route (always null). |
| `match_id` | integer | Match id. Legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = 2005-06 final, 17718 = 2016-17 final); from 2020-21 they start at about 100000 (102920 in 2021-22, 105401 in 2026-27). One key space across competitions: the matchId param of every matchdata route. |
| `home_team_id` | integer | Home season-team entry id (new every season; joins the body's team list). |
| `away_team_id` | integer | Away season-team entry id (new every season; joins the body's team list). |
| `home_score` | integer | Home points (null before the match) |
| `away_score` | integer | Away points (null before the match) |
| `start` | character | Tip-off, ISO 8601 UTC |
| `status` | character | Match state (x-match-statuses) |
| `is_on_half_time` | logical | At half time (live) |
| `current_quarter` | integer | Current or last quarter (5+ = overtime); not reliable after the match (1 in the 2017 series rows) |
| `home_club_id` | integer | Stable club id of the home team (looked up in the body's team list). |
| `home_team` | character | Home team name for the season (looked up in the body's team list). |
| `home_team_abbr` | character | Home team three-letter abbreviation (looked up in the body's team list). |
| `away_club_id` | integer | Stable club id of the away team (looked up in the body's team list). |
| `away_team` | character | Away team name for the season (looked up in the body's team list). |
| `away_team_abbr` | character | Away team three-letter abbreviation (looked up in the body's team list). |
| `ticketing_link` | character | Ticket shop URL; null when none. |
| `broadcasters` | character | Broadcast channel names of the game joined with '; ' (the first is the primary). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_calendar-example}

```python
acb_calendar(season=2027)
```

_Last validated n/a._

## acb_league_zones

League-wide shooting by court zone for a season.

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/ShotChart/league-zones`

**Valid URL:** [https://api2.acb.com/api/seasondata/ShotChart/league-zones?season=2027](https://api2.acb.com/api/seasondata/ShotChart/league-zones?season=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionId` | `competition_id` |  |  | `Y` | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (youth). Omitted = the API's default competition. |
| `season` | `season` |  |  | `Y` | Season END year (2027 = 2026-27; the first ACB season, 1983-84, is 1984). ``acb_runtime`` sends the API's ``editionId`` = season - 1936. Omitted = the current season. |

### Returns {#acb_league_zones-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | integer | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa. |
| `edition_id` | integer | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `phase_id` | integer | Phase the statistics cover; null in the sample (read as all phases, unverified). |
| `zone` | character | Court zone (acb_codes.SHOT_ZONES): restricted_area, paint, mid_range_*, corner_three_*, wing_three_*, top_three. |
| `attempts` | integer | Field-goal attempts in the zone, league-wide. |
| `made` | integer | Field goals made in the zone, league-wide. |
| `points_scored` | integer | Points scored from the zone, league-wide. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_league_zones-example}

```python
acb_league_zones(season=2027)
```

_Last validated n/a._
