---
title: "LBA — Lega Basket Serie A site API (www.legabasket.it/api, Italy) — Other"
sidebar_label: "Other"
sidebar_position: 3
description: "LBA — Lega Basket Serie A site API (www.legabasket.it/api, Italy) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# LBA — Lega Basket Serie A site API (www.legabasket.it/api, Italy) — Other

## lba_championships

Championships of a season, one per phase (Regular Season, Play Off); every season when none is given.

**Endpoint URL:** `GET https://www.legabasket.it/api/championships/get-championships`

**Valid URL:** [https://www.legabasket.it/api/championships/get-championships?cs_id=1&items=1000&s=2027](https://www.legabasket.it/api/championships/get-championships?cs_id=1&items=1000&s=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `s` | `season` |  |  | `Y` | Season END year (2027 = 2026-27). Omitted = every season (310 championships). |

### Returns {#lba_championships-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = 2026-27): the row's START year + 1. |
| `championship_id` | integer | Championship id (one season and phase). |
| `year` | integer | Season START year (2026 = 2026-27); sdv END year = year + 1. |
| `name` | character | Championship name, e.g. `Regular Season A`, `Play Off A1`. |
| `status` | integer | Championship status: 1 = in progress, 2 = finished (inferred from the 2026-27 and past samples). |
| `championship_type_id` | integer | Phase type id: 4 = Regular Season, 2 = Play Off. |
| `championship_series_id` | integer | Series id, 1 = Serie A. |
| `full_name` | character | Name with the season, e.g. `Regular Season A 2017`. |
| `focus_on` | logical | Highlighted on the site. |
| `ctype_code` | character | Phase type code: `RS` / `PO`. |
| `ctype_name` | character | Phase type name, as sent in English: `Regular Season`, `Play Off`. |
| `cserie_code` | character | Series code, `A1` = Serie A. |
| `cserie_name` | character | Name of the championship series, e.g. Serie A. |
| `logo_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#lba_championships-example}

```python
lba_championships(season=2027)
```

_Last validated n/a._

## lba_calendar

Games of one round of a championship: teams, score, venue, referees and broadcasters.

**Endpoint URL:** `GET https://www.legabasket.it/api/championships/get-championships-calendar-by-id`

**Valid URL:** [https://www.legabasket.it/api/championships/get-championships-calendar-by-id?id=602&d=1](https://www.legabasket.it/api/championships/get-championships-calendar-by-id?id=602&d=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `championship_id` |  | `Y` |  | Championship id (championship_id in lba_championships): one season AND one phase. |
| `d` | `round` |  |  | `Y` | Round number (day_serial, 1, 2, ...). Omitted = the current round (the last played for a finished championship). |

### Returns {#lba_calendar-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = 2026-27): the row's START year + 1. |
| `game_id` | integer | Game id (global across seasons); joins lba_calendar and lba_game. |
| `game_status` | character | Game status, number sent as a string: `0` scheduled, `1` live, `2` final (per the OSS client; only `2` sampled). |
| `championship_id` | integer | Championship id (season + phase) of the game. |
| `championships_day_id` | integer | Round id (`championships_day_id` key space: one round of one championship; joins `filters.days[].id` and standings `championships_day_id`; not the `d` param). |
| `year` | integer | Season START year (2026 = 2026-27); sdv END year = year + 1. |
| `number` | integer | Round number of the game, equal to `day_serial` (the `d` echo the capture checks). |
| `match_datetime` | character | Tip-off, ISO-8601 with the Italian offset (1995-96: midnight, no time kept). |
| `match_hh` | integer | Tip-off hour, local time. |
| `match_mm` | integer | Tip-off minute, local time. |
| `home_final_score` | integer | Final points of the home team. |
| `visitor_final_score` | integer | Final points of the away team. |
| `target_score` | character | Null in every sample (meaning unverified). |
| `additional_time` | integer | Overtime periods played (0 in every sample; reading inferred). |
| `plant_id` | integer | Arena id (arena key space; joins nothing specced). |
| `websocket_match_id` | integer | Live-feed game id, number sent as a string; separate from the game id. |
| `ticketing_url` | character | Ticket shop URL (null for old games). |
| `has_streaming` | integer | 1 when a stream exists, else 0. |
| `updated_at` | character | Timestamp, ISO-8601 with the Italian offset. Last update of the game row. |
| `h_team_id` | integer | Home team id (team-season). |
| `h_team_name` | character | Home team name (sponsor name of that season). |
| `h_club_code` | character | Home club code, e.g. `TVB`; null on the 2017-18 and 1995-96 calendar rows. |
| `h_team_enabled` | logical | Home team page enabled on the site. |
| `home_logo_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `v_team_id` | integer | Away team id (team-season). |
| `v_team_name` | character | Visitor team name (the sponsor name of that season). |
| `v_club_code` | character | Away club code (`BCR`); null on the 2017-18 and 1995-96 calendar rows. |
| `v_team_enabled` | logical | Away team page enabled on the site. |
| `v_logo_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `town_id` | integer | Town id of the arena (town key space; joins nothing specced). |
| `plant_name` | character | Name of the arena where the game is played. |
| `town_name` | character | Town of the arena. |
| `day_code` | character | Round code (`1`, `2`, ...; string in game rows, integer in standings). |
| `day_serial` | integer | Round number (`event_serial`, the `d` param). |
| `day_name` | character | Round name, Italian: `1° Giornata` = matchday 1. |
| `match_serie_key` | character | The two team ids joined low_high (`1761_1764`). |
| `referees` | character | JSON list of the game's referees (id, name, surname); null when the site lists none. |
| `broadcasters` | character | JSON list of the game's broadcasters (name, logo key); null when the site lists none. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#lba_calendar-example}

```python
lba_calendar(championship_id=602, round=1)
```

_Last validated n/a._

## lba_game

Game header: teams, venue, quarter scores, coaches, referees and broadcasters.

**Endpoint URL:** `GET https://www.legabasket.it/api/championships/get-championships-matches-by-id`

**Valid URL:** [https://www.legabasket.it/api/championships/get-championships-matches-by-id?sc=1&id=25602](https://www.legabasket.it/api/championships/get-championships-matches-by-id?sc=1&id=25602)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `game_id` |  | `Y` |  | Game id (game_id in lba_calendar), e.g. 25602. Global across seasons. |

### Returns {#lba_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = 2026-27): the row's START year + 1. |
| `game_id` | integer | Game id (global across seasons); joins lba_calendar and lba_game. |
| `game_status` | character | Game status, number sent as a string: `0` scheduled, `1` live, `2` final (per the OSS client; only `2` sampled). |
| `championship_id` | integer | Championship id (season + phase) of the game. |
| `competition_name` | character | Championship full name, e.g. `Regular Season 2026/27`. |
| `championships_day_id` | integer | Round id (`championships_day_id` key space: one round of one championship; joins `filters.days[].id` and standings `championships_day_id`; not the `d` param). |
| `year` | integer | Season START year (2026 = 2026-27); sdv END year = year + 1. |
| `number` | integer | Round number of the game, equal to `day_serial` (the `d` echo the capture checks). |
| `match_datetime` | character | Tip-off, ISO-8601 with the Italian offset (1995-96: midnight, no time kept). |
| `match_hh` | integer | Tip-off hour, local time. |
| `match_mm` | integer | Tip-off minute, local time. |
| `target_score` | character | Null in every sample (meaning unverified). |
| `home_final_score` | integer | Final points of the home team. |
| `visitor_final_score` | integer | Final points of the away team. |
| `q1_hs` | integer | Home points in the 1st quarter (null in the 1995-96 sample: no period scores then). |
| `q1_vs` | integer | Away points in the 1st quarter. |
| `q2_hs` | integer | Home points in the 2nd quarter. |
| `q2_vs` | integer | Away points in the 2nd quarter. |
| `q3_hs` | integer | Home points in the 3rd quarter (0 in the 1995-96 sample: a placeholder, not a score). |
| `q3_vs` | integer | Away points in the 3rd quarter (0 placeholder in 1995-96). |
| `q4_hs` | integer | Home points in the 4th quarter (0 placeholder in 1995-96). |
| `q4_vs` | integer | Away points in the 4th quarter (0 placeholder in 1995-96). |
| `ot_hs` | integer | Home overtime points (0 without overtime). |
| `ot_vs` | integer | Away overtime points. |
| `home_coach_id` | integer | Home head coach id. |
| `home_coach_code` | character | Home head coach code `SURNAME3-NAME3`. |
| `home_coach_fullname` | character | Home head coach, `Surname Name`. |
| `visitor_coach_id` | integer | Away head coach id. |
| `visitor_coach_code` | character | Away head coach code. |
| `visitor_coach_fullname` | character | Away head coach, `Surname Name`. |
| `additional_time` | integer | Overtime periods played (0 in every sample; reading inferred). |
| `plant_id` | integer | Arena id (arena key space; joins nothing specced). |
| `websocket_match_id` | integer | Live-feed game id, number sent as a string; separate from the game id. |
| `has_streaming` | integer | 1 when a stream exists, else 0. |
| `live_match_data` | character | Null in every sample (meaning unverified). |
| `quarter` | character | Last period played, number sent as a string (`4`; `5` = overtime). |
| `ticketing_url` | character | Ticket shop URL (null for old games). |
| `match_serie` | character | All-time head-to-head as `home wins-away wins` (`6-18`; inferred from `match_hw` / `match_vw`). |
| `match_hw` | integer | All-time head-to-head wins of the home club (inferred). |
| `match_vw` | integer | All-time head-to-head wins of the away club (inferred). |
| `updated_at` | character | Timestamp, ISO-8601 with the Italian offset. Last update of the game row. |
| `h_team_id` | integer | Home team id (team-season). |
| `h_team_name` | character | Home team name (sponsor name of that season). |
| `h_team_enabled` | logical | Home team page enabled on the site. |
| `home_logo_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `v_team_id` | integer | Away team id (team-season). |
| `v_team_name` | character | Visitor team name (the sponsor name of that season). |
| `v_team_enabled` | logical | Away team page enabled on the site. |
| `v_logo_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `plant_name` | character | Name of the arena where the game is played. |
| `day_code` | character | Round code (`1`, `2`, ...; string in game rows, integer in standings). |
| `day_serial` | integer | Round number (`event_serial`, the `d` param). |
| `day_name` | character | Round name, Italian: `1° Giornata` = matchday 1. |
| `competition_type_id` | integer | Phase type id: 4 = Regular Season, 2 = Play Off. |
| `competition_type_code` | character | Phase type code: `RS` / `PO`. |
| `competition_type` | character | Phase type name: `Regular Season` / `Play Off`. |
| `competition_serie_id` | integer | Series id, 1 = Serie A. |
| `competition_serie` | character | Series name, `Serie A`. |
| `day_phase_id` | integer | Phase id: 1 = `Andata` (first half of the season), 2 = `Ritorno` (second half). |
| `spectators` | integer | Attendance: spectators at the game. |
| `referees` | character | JSON list of the game's referees (id, name, surname); null when the site lists none. |
| `broadcasters` | character | JSON list of the game's broadcasters (name, logo key); null when the site lists none. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#lba_game-example}

```python
lba_game(game_id=25602)
```

_Last validated n/a._

## lba_box_score

Box score of a final game: one row per player of both teams (Italian-named stat columns).

**Endpoint URL:** `GET https://www.legabasket.it/api/championships/get-championships-matches-scores-by-id`

**Valid URL:** [https://www.legabasket.it/api/championships/get-championships-matches-scores-by-id?info=true&id=25602](https://www.legabasket.it/api/championships/get-championships-matches-scores-by-id?info=true&id=25602)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `game_id` |  | `Y` |  | Game id (game_id in lba_calendar), e.g. 25602. Global across seasons. |

### Returns {#lba_box_score-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | Game id (global across seasons); joins lba_calendar and lba_game. |
| `season` | integer | Season END year (2027 = 2026-27): the row's START year + 1. |
| `side` | character | Which team of the game the row belongs to: home or visitor. |
| `team_id` | integer | Team-season id (re-issued every season); join lba_teams for the stable club_id. |
| `player_id` | integer | Player id (global). |
| `player_num` | integer | Jersey number the player wore in the game. |
| `player_surname` | character | Family name, upper case. |
| `player_name` | character | Player's given name. |
| `player_code` | character | Player code `SURNAME3-NAME3-YY`. |
| `player_p_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `pun` | integer | Points (`punti`). |
| `min` | integer | Minutes played, whole minutes (`minuti`); 200 in `totals` = 5 × 40. |
| `sec` | integer | Seconds played (`secondi`) = `min` × 60 in every sample. |
| `sf` | character | Starter flag (`starting five`), number sent as a string: `1` = started. |
| `falli_c` | integer | Fouls committed (`falli commessi`). |
| `falli_sf` | integer | Fouls drawn (`falli subiti`). |
| `falli_tc1` | integer | Technical fouls (`falli tecnici`; reading `tc1` as category-1 technicals is inferred). |
| `falli_fla` | integer | Unsportsmanlike / flagrant fouls (`falli antisportivi`). |
| `t2_r` | integer | 2-point field goals made (`tiri da 2 realizzati`). |
| `t2_t` | integer | 2-point field goals attempted (`tiri da 2 tentati`). |
| `t2_perc` | numeric | 2-point field-goal percentage, 0-100 (number; 0 when no attempt). |
| `sc` | integer | Dunks (`schiacciate`). |
| `t3_r` | integer | 3-point field goals made. |
| `t3_t` | integer | 3-point field goals attempted. |
| `t3_perc` | numeric | 3-point field-goal percentage, 0-100. |
| `tl_r` | integer | Free throws made (`tiri liberi realizzati`). |
| `tl_t` | integer | Free throws attempted (`tiri liberi tentati`). |
| `tl_perc` | numeric | Free-throw percentage, 0-100. |
| `rimbalzi_o` | integer | Offensive rebounds (`rimbalzi offensivi`). |
| `rimbalzi_d` | integer | Defensive rebounds (`rimbalzi difensivi`). |
| `rimbalzi_t` | integer | Total rebounds (`rimbalzi totali`). |
| `stoppate_dat` | integer | Blocks (`stoppate date`). |
| `stoppate_sub` | integer | Own shots blocked (`stoppate subite`). |
| `palle_p` | integer | Turnovers (`palle perse`). |
| `palle_r` | integer | Steals (`palle recuperate`). |
| `ass` | integer | Assists credited to the player in the game. |
| `val_lega` | integer | LBA efficiency rating (`valutazione Lega`). |
| `val_oer` | numeric | OER = points / (FGA + turnovers + FTA / 2); verified on every 2026-27 sample row. |
| `plus_minus` | integer | Plus-minus: point margin while on court. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#lba_box_score-example}

```python
lba_box_score(game_id=25602)
```

_Last validated n/a._

## lba_pbp

Play-by-play of a final game (2017-18 on), one row per action with x/y; coordinate frame by era.

**Endpoint URL:** `GET https://www.legabasket.it/api/championships/get-championships-matches-play-by-play`

**Valid URL:** [https://www.legabasket.it/api/championships/get-championships-matches-play-by-play?info=true&sort=asc&id=25602](https://www.legabasket.it/api/championships/get-championships-matches-play-by-play?info=true&sort=asc&id=25602)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `game_id` |  | `Y` |  | Game id (game_id in lba_calendar), e.g. 25602. Global across seasons. |

### Returns {#lba_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | Game id (global across seasons); joins lba_calendar and lba_game. |
| `season` | integer | Season END year (2027 = 2026-27): the row's START year + 1. |
| `home_team_id` | integer | Home team-season id of the game (the play-by-play ht_id). |
| `visitor_team_id` | integer | Visitor team-season id of the game (the play-by-play vt_id). |
| `team_side` | character | Side of the acting team, home or visitor; null for a clock action without a team. |
| `home_score` | integer | Home points after the action, parsed from the score text (2021-22 on; else null). |
| `visitor_score` | integer | Visitor points after the action, parsed from the score text (2021-22 on; else null). |
| `coord_frame` | character | Frame of x and y: half_court_px (2017-18 and 2018-19 image pixels) or court_pct (2019-20 on, 0-100). |
| `action_id` | integer | Action id within the game (ascending in time from 2021-22; not ordered before). |
| `description` | character | Action type in Italian, e.g. `2 punti segnato` (2-pointer made); full translation table in the Divergence notes. |
| `player_id` | integer | Player id (null for team and clock actions). |
| `team_id` | integer | Team id (team-season) of the actor. |
| `home_club` | integer | 1 = the home team acted, 0 = the away team; null for clock actions, and on every action in 2018-19 and 2019-20 (use `side` there). |
| `in_area` | logical | True on shots from the paint (zone `3`). |
| `dunk` | integer | 1 = dunk, 0 otherwise. |
| `seconds` | integer | Elapsed seconds within `minute` (0-59); 0 before 2019-20. Elapsed = minute × 60 + seconds; `print_time` = period length minus that. |
| `minute` | integer | Elapsed minute of the period (2019-20 on: 0-9, with `seconds`); 2017-18 / 2018-19: 1-10, the minute in progress, `seconds` 0. |
| `period` | integer | Period: 1-4, 5+ = overtime. |
| `order` | integer | Sort key; `sort=asc` orders by it. |
| `side` | integer | Which team of the game the row belongs to: home or visitor. |
| `x` | integer | Court x; frame by era (see the coordinates section): 2019-20 on, 0-100 % of the 28 m length; 2017-18 / 2018-19, half-court image pixels; null (2019-20 on) or 0 (earlier) when unlocated. |
| `y` | integer | Court y; 2019-20 on, 0-100 % of the 15 m width; 2017-18 / 2018-19, image pixels (see the coordinates section). |
| `score` | character | Score after the action, `home - away` string (`0 - 0`); 2020-21 on scoring actions only, null before. |
| `linked_action_id` | integer | `action_id` of the related action: the shot an assist or rebound belongs to, the foul a drawn foul answers, the player swapped in a substitution. |
| `print_time` | character | Game clock remaining, `MM:SS` (10:00 at a period start); null before 2021-22. |
| `action_1_qualifier_code` | character | First qualifier code, number sent as a string (shot type, turnover type, foul type; see `legend`). |
| `action_2_qualifier_code` | character | Second qualifier code (fast break `500`, blocked `1005`, tip-ins `1001`-`1004`, ...). |
| `action_1_qualifier_description` | character | `legend` label of `action_1_qualifier_code`, Italian. |
| `action_2_qualifier_description` | character | `legend` label of `action_2_qualifier_code`, Italian. |
| `side_area_zone` | character | `A` = in the actor's attacking half, `D` = its defensive half (2021-22 on). |
| `side_area_code` | character | Court zone `1`-`10`, number sent as a string (see the coordinates section; 2021-22 on). |
| `player_name` | character | Given name of the acting player. |
| `player_surname` | character | Family name of the acting player. |
| `player_number` | character | Jersey number, number sent as a string. |
| `team_name` | character | Team name in that season, with its sponsor (`Nutribullet Treviso Basket`); changes with the sponsor. |
| `id` | integer | Always null (not a key). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#lba_pbp-example}

```python
lba_pbp(game_id=25602)
```

_Last validated n/a._

## lba_pbp_legend

The play-by-play qualifier code table (59 codes) with its Italian labels.

**Endpoint URL:** `GET https://www.legabasket.it/api/championships/get-championships-matches-play-by-play`

**Valid URL:** [https://www.legabasket.it/api/championships/get-championships-matches-play-by-play?info=true&sort=asc&id=25602](https://www.legabasket.it/api/championships/get-championships-matches-play-by-play?info=true&sort=asc&id=25602)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `game_id` |  | `Y` |  | Game id (game_id in lba_calendar), e.g. 25602. Global across seasons. |

### Returns {#lba_pbp_legend-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `code` | integer | Qualifier code, the value of action_1_qualifier_code / action_2_qualifier_code on the actions. |
| `label` | character | Italian label of the qualifier code, as sent (translated in sdv-internal-refs legabasket-returns.md). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#lba_pbp_legend-example}

```python
lba_pbp_legend(game_id=25602)
```

_Last validated n/a._

## lba_shots

Shooting by court zone per team for a final game (made / attempted aggregates; zone 7 includes free throws).

**Endpoint URL:** `GET https://www.legabasket.it/api/matches/get-championship-matches-shots-by-id`

**Valid URL:** [https://www.legabasket.it/api/matches/get-championship-matches-shots-by-id?info=1&id=25602](https://www.legabasket.it/api/matches/get-championship-matches-shots-by-id?info=1&id=25602)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `game_id` |  | `Y` |  | Game id (game_id in lba_calendar), e.g. 25602. Global across seasons. |

### Returns {#lba_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | Game id (global across seasons); joins lba_calendar and lba_game. |
| `season` | integer | Season END year (2027 = 2026-27): the row's START year + 1. |
| `side` | character | Which team of the game the row belongs to: home or visitor. |
| `team_id` | integer | Team-season id (re-issued every season); join lba_teams for the stable club_id. |
| `zone` | integer | Court zone 1-10 (the play-by-play side_area_code); zone 7 also holds the free throws. |
| `made` | integer | Shots made in the zone, parsed from num. |
| `attempts` | integer | Shots attempted in the zone, parsed from num. |
| `perc` | numeric | Made / attempted × 100. |
| `precision` | integer | Decimal places the site shows (1). |
| `num` | character | Made/attempted as a string (`17/21`); zone `7` also counts free throws. |
| `opacity` | integer | Heat-map shading for the site: `perc` floored, minimum 5. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#lba_shots-example}

```python
lba_shots(game_id=25602)
```

_Last validated n/a._

## lba_standings

Standings of a championship after a round, one row per team.

**Endpoint URL:** `GET https://www.legabasket.it/api/championships/get-championship-ranking`

**Valid URL:** [https://www.legabasket.it/api/championships/get-championship-ranking?id=602&d=3](https://www.legabasket.it/api/championships/get-championship-ranking?id=602&d=3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `championship_id` |  | `Y` |  | Championship id (championship_id in lba_championships; a Regular Season id carries standings). |
| `d` | `round` |  |  | `Y` | Standings after this round. Required in practice for a finished championship: without it the site sends none (NoDataError). |

### Returns {#lba_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = 2026-27): the row's START year + 1. |
| `standing_id` | integer | Standings row id (one team after one round; joins nothing else). |
| `championships_day_id` | integer | Round id the standings are after. |
| `team_id` | integer | Team id (team-season). |
| `position` | integer | Rank in the standings. |
| `wins` | integer | Games the team has won through the round. |
| `defeats` | integer | Games the team has lost through the round. |
| `points_made` | integer | Points the team has scored through the round. |
| `points_suffered` | integer | Points allowed. |
| `qp` | integer | Tie-break quotient (`quoziente punti`?); 1 in the samples (unverified). |
| `lv` | character | Null in every sample (meaning unverified). |
| `lp` | character | Null in every sample (meaning unverified). |
| `lpf` | character | Null in every sample (meaning unverified). |
| `lps` | character | Null in every sample (meaning unverified). |
| `lqp` | character | Null in every sample (meaning unverified). |
| `updated_at` | character | Timestamp, ISO-8601 with the Italian offset. |
| `points` | integer | Standings points (2 per win). |
| `penalty_points` | integer | Points deducted (0 or null). |
| `game_played` | integer | Games the team has played through the round. |
| `home_game_played` | integer | Home games played. |
| `home_wins` | integer | Home games won. |
| `home_defeats` | integer | Home games the team has lost through the round. |
| `away_game_played` | integer | Away games played. |
| `away_wins` | integer | Away games won. |
| `away_defeats` | integer | Away games the team has lost through the round. |
| `avg_points_made` | numeric | Points scored per game. |
| `avg_points_suffered` | numeric | Points allowed per game. |
| `home_points_made` | integer | Points scored at home. |
| `home_points_suffered` | integer | Points allowed at home. |
| `home_avg_points_made` | numeric | Points scored per home game. |
| `home_avg_points_suffered` | numeric | Points allowed per home game. |
| `away_points_made` | integer | Points scored away. |
| `away_points_suffered` | integer | Points allowed away. |
| `away_avg_points_made` | numeric | Points scored per away game. |
| `away_avg_points_suffered` | numeric | Points allowed per away game. |
| `year` | integer | Season START year (2026 = 2026-27); sdv END year = year + 1. |
| `ties` | integer | Ties (0 or null; basketball has none). |
| `home_ties` | integer | Home ties (null). |
| `away_ties` | integer | Away ties (null). |
| `play_off_qualified` | logical | Clinched a play-off place (null in old seasons). |
| `relegation` | logical | Relegated (null in old seasons). |
| `movement` | character | Position change since the previous round, signed string (`+2`, `-1`; null for finished seasons). |
| `wins_percent` | numeric | Win percentage as a string with `%` (`100%`). |
| `home_wins_percent` | numeric | Home win percentage, string with `%`. |
| `away_wins_percent` | numeric | Away win percentage, string with `%`. |
| `max_points_made` | integer | Most points scored in a game. |
| `home_max_points_made` | integer | Most points scored in a home game. |
| `away_max_points_made` | integer | Most points scored in an away game. |
| `day_code` | integer | Round code the standings are after (integer). |
| `day_name` | character | Round name, Italian (`4ª Giornata - 2° fase` = matchday 4 of the 2nd phase). |
| `team_name` | character | Team name in that season, with its sponsor (`Nutribullet Treviso Basket`); changes with the sponsor. |
| `club_code` | character | Club code (`TVB`, `TS`); whether it is stable across seasons is unverified. |
| `logo_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `ctp` | integer | Classification marker; 0 or null in the samples (unverified). |
| `ctp_symbol` | character | Null in every sample (meaning unverified). |
| `ctp_description` | character | Null in every sample (meaning unverified). |
| `season_start_year` | integer | The wire season field: the season START year (2026 = 2026-27), same as year. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#lba_standings-example}

```python
lba_standings(championship_id=602, round=3)
```

_Last validated n/a._

## lba_roster

Players of one team-season with bio, number and role.

**Endpoint URL:** `GET https://www.legabasket.it/api/teams/get-team-roster`

**Valid URL:** [https://www.legabasket.it/api/teams/get-team-roster?id=1764](https://www.legabasket.it/api/teams/get-team-roster?id=1764)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `team_id` |  | `Y` |  | Team id (team_id in lba_teams), e.g. 1764. One team-season: ids are re-issued every season. |

### Returns {#lba_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Team-season id (re-issued every season); join lba_teams for the stable club_id. |
| `club_id` | integer | Club id: the cross-season join key for team ids (only here and in the `team` headers); stability across seasons is unverified. |
| `club_code` | character | Club code (`TVB`, `TS`); whether it is stable across seasons is unverified. |
| `season` | integer | Season END year (2027 = 2026-27): the row's START year + 1. |
| `player_id` | integer | Player id (global). |
| `name` | character | Player's given name. |
| `surname` | character | Player's family name. |
| `code` | character | Player code `SURNAME3-NAME3-YY`. |
| `place_of_birth` | character | Birthplace (Italian spelling). |
| `birth_date` | character | Birth date, Date `YYYY-MM-DD`. |
| `player_number` | character | Jersey number, number sent as a string. |
| `country` | character | Nationality, ISO alpha-3. |
| `uefa_ratio` | character | Eligibility class for the foreign-player quota: `I`, `E` or `C` (7 / 6 / 1 on the Treviso roster; reading as Italian / EU / non-EU is inferred). |
| `height` | integer | Player's height in centimetres. |
| `weight` | integer | Player's weight in kilograms. |
| `year` | integer | Season START year of the player entry (2026 = 2026-27). |
| `start_date` | character | Contract start with the team, Date `YYYY-MM-DD`. |
| `end_date` | character | Contract end with the team, Date `YYYY-MM-DD`. |
| `player_role_id` | integer | Position id (position key space: the id behind `player_role`; joins the `r_id` filter). |
| `player_role` | character | Position, Italian: `Playmaker` (point guard), `Guardia` (guard), `Ala` (forward), `Centro` (center), `-` unknown. |
| `team_name` | character | Team name in that season, with its sponsor (`Nutribullet Treviso Basket`); changes with the sponsor. |
| `player_picture_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `team_logo_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#lba_roster-example}

```python
lba_roster(team_id=1764)
```

_Last validated n/a._

## lba_roster_coach

Head coach of one team-season.

**Endpoint URL:** `GET https://www.legabasket.it/api/teams/get-team-roster`

**Valid URL:** [https://www.legabasket.it/api/teams/get-team-roster?id=1764](https://www.legabasket.it/api/teams/get-team-roster?id=1764)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `team_id` |  | `Y` |  | Team id (team_id in lba_teams), e.g. 1764. One team-season: ids are re-issued every season. |

### Returns {#lba_roster_coach-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Team-season id (re-issued every season); join lba_teams for the stable club_id. |
| `club_id` | integer | Club id: the cross-season join key for team ids (only here and in the `team` headers); stability across seasons is unverified. |
| `club_code` | character | Club code (`TVB`, `TS`); whether it is stable across seasons is unverified. |
| `season` | integer | Season END year (2027 = 2026-27): the row's START year + 1. |
| `coach_id` | integer | Coach id (global coach key space; joins the game header `home_coach_id` / `visitor_coach_id`). |
| `name` | character | Coach's given name. |
| `surname` | character | Coach's family name. |
| `birth_date` | character | Birth date, Date `YYYY-MM-DD`. |
| `place_of_birth` | character | Birthplace (Italian spelling, e.g. `Londra`). |
| `coach_type_id` | integer | Coach role id (1 = head coach). |
| `coach_type_description` | character | Coach role, Italian: `Allenatore` = head coach. |
| `coach_type_code` | character | Coach role code, number sent as a string. |
| `team_name` | character | Team, short name. |
| `coach_picture_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `team_logo_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#lba_roster_coach-example}

```python
lba_roster_coach(team_id=1764)
```

_Last validated n/a._

## lba_game_log

A player's game log for one season, one row per game.

**Endpoint URL:** `GET https://www.legabasket.it/api/players/get-player-matches`

**Valid URL:** [https://www.legabasket.it/api/players/get-player-matches?cs_id=1&id=969&s=2027](https://www.legabasket.it/api/players/get-player-matches?cs_id=1&id=969&s=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `player_id` |  | `Y` |  | Player id (player_id in lba_players or a box score), e.g. 969. Global: one id for a whole career. |
| `s` | `season` |  |  | `Y` | Season END year (2027 = 2026-27). Omitted = the site's default season (unverified). |

### Returns {#lba_game_log-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = 2026-27): the row's START year + 1. |
| `stat_line_id` | integer | Id of the player's stat line for the game (one per player and game). |
| `championship_id` | integer | Championship id of the game. |
| `championships_match_id` | integer | Game id (global across seasons); joins lba_calendar and lba_game. |
| `player_id` | integer | Player id (global, one per career). |
| `team_id` | integer | Team id (team-season) of the player. |
| `number` | integer | Jersey number the player wore in the game. |
| `played_minutes` | integer | Minutes played, in that game. |
| `points` | integer | Points, in that game. |
| `suffered_fouls` | integer | Fouls drawn, in that game. |
| `done_fouls` | integer | Fouls committed, in that game. |
| `shots_2p_realized` | integer | 2-point field goals made, in that game. |
| `shots_2p_total` | integer | 2-point field goals attempted, in that game. |
| `shots_3p_realized` | integer | 3-point field goals made, in that game. |
| `shots_3p_total` | integer | 3-point field goals attempted, in that game. |
| `free_throws_realized` | integer | Free throws made, in that game. |
| `free_throws_total` | integer | Free throws attempted, in that game. |
| `restricted_area_p1` | character | Null in every sample (meaning unverified). |
| `restricted_area_p2` | character | Null in every sample (meaning unverified). |
| `restricted_area_p3` | character | Null in every sample (meaning unverified). |
| `slam_dunk` | integer | Dunks, in that game. |
| `offensive_rebound` | integer | Offensive rebounds, in that game. |
| `defensive_rebound` | integer | Defensive rebounds, in that game. |
| `ball_stop_given` | integer | Blocks, in that game. |
| `ball_stop_received` | integer | Own shots blocked, in that game. |
| `lost_balls` | integer | Turnovers, in that game. |
| `regain_balls` | integer | Steals, in that game. |
| `assists` | integer | Assists, in that game. |
| `technical_fouls` | character | Technical fouls (null in old seasons). |
| `minute5_fouls` | integer | Minute of the fifth foul (0 = did not foul out; inferred, unverified). |
| `time5_fouls` | integer | Game time of the fifth foul (0 = did not foul out; inferred, unverified). |
| `rating_lega` | integer | LBA efficiency rating (`valutazione`), in that game. |
| `quintet` | integer | Starts (in the starting five), in that game. |
| `rating_oer` | numeric | OER (points / (FGA + turnovers + FTA / 2)), in that game. |
| `avg_points` | integer | The player's plus-minus in the game (the site's name is misleading). |
| `wp_id` | integer | Null in every sample (meaning unverified). |
| `deleted_at` | character | Null in every sample (meaning unverified). |
| `created_at` | character | Timestamp, ISO-8601 with the Italian offset. |
| `updated_at` | character | Timestamp, ISO-8601 with the Italian offset. |
| `player_role_id` | integer | Position id (position key space: the id behind `player_role`; joins the `r_id` filter). |
| `order` | integer | Row order in the box score. |
| `captain` | logical | Team captain in that game. |
| `shots_2p_perc` | integer | 2-point field-goal percentage, 0-100, in that game. |
| `shots_3p_perc` | integer | 3-point field-goal percentage, 0-100, in that game. |
| `free_throws_perc` | integer | Free-throw percentage, 0-100, in that game. |
| `total_rebound` | integer | Total rebounds, in that game. |
| `ext_code` | character | `<websocket_match_id>\\|<team code>\\|<player code>` (`1674778\\|TVU\\|CAP-ALE-95`). |
| `fouls_first_category` | integer | Category-1 technical fouls (null in old seasons; reading inferred). |
| `fouls_flagrant` | integer | Unsportsmanlike / flagrant fouls. |
| `championship_day_name` | character | Round code, number sent as a string. |
| `home_score` | integer | Home points after the action, parsed from the score text (2021-22 on; else null). |
| `visitor_score` | integer | Visitor points after the action, parsed from the score text (2021-22 on; else null). |
| `match_date` | character | Tip-off in UTC, ISO-8601 `Z` (local time in `match_hh` / `match_mm`). |
| `match_hh` | integer | Tip-off hour, local time. |
| `match_mm` | integer | Tip-off minute, local time. |
| `championship_match_id` | integer | Game id (repeated). |
| `player_team_id` | integer | Team id (team-season) of the player (repeated). |
| `player_team_name` | character | Short name of the player's team in the game (without the season sponsor). |
| `player_team_code` | character | `<season>\\|<team code>` (`1995\\|BOF`). |
| `visitor_team_id` | integer | Visitor team-season id of the game (the play-by-play vt_id). |
| `visitor_team_name` | character | Away team short name. |
| `visitor_team_logo_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `home_team_id` | integer | Home team-season id of the game (the play-by-play ht_id). |
| `home_team_name` | character | Home team short name. |
| `home_team_logo_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `phase_description` | character | Phase code: `And` (Andata, 1st half of the season) / `Rit` (Ritorno). |
| `match_adp` | integer | The player's plus-minus in the game (the site's name is misleading). |
| `home_match_result` | character | `w` / `l` for the home team when the player was on it, else null. |
| `visitor_match_result` | character | `w` / `l` for the away team when the player was on it, else null. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#lba_game_log-example}

```python
lba_game_log(player_id=969, season=2027)
```

_Last validated n/a._
