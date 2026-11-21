# Lega Basket Serie A — Lega Basket Serie A site API (www.legabasket.it/api, Italy) — Other

> Lega Basket Serie A — Lega Basket Serie A site API (www.legabasket.it/api, Italy) — Other — function reference in sdv-py, the SportsDataverse Python package.

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
| `season` | Int64 | Season END year (2027 = 2026-27): the row's START year + 1. |
| `championship_id` | Int64 | Championship id (one season and phase). |
| `year` | Int64 | Season START year (2026 = 2026-27); sdv END year = year + 1. |
| `name` | String | Championship name, e.g. `Regular Season A`, `Play Off A1`. |
| `status` | Int64 | Championship status: 1 = in progress, 2 = finished (inferred from the 2026-27 and past samples). |
| `championship_type_id` | Int64 | Phase type id: 4 = Regular Season, 2 = Play Off. |
| `championship_series_id` | Int64 | Series id, 1 = Serie A. |
| `full_name` | String | Name with the season, e.g. `Regular Season A 2017`. |
| `focus_on` | Boolean | Highlighted on the site. |
| `ctype_code` | String | Phase type code: `RS` / `PO`. |
| `ctype_name` | String | Phase type name, as sent in English: `Regular Season`, `Play Off`. |
| `cserie_code` | String | Series code, `A1` = Serie A. |
| `cserie_name` | String | Name of the championship series, e.g. Serie A. |
| `logo_key` | String | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#lba_championships-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (an unknown season, a play-by-play before 2017-18, standings of a finished championship without a round, no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an unknown id (the site answers HTTP 500), a game that is not final, a truncated page, or a body for another season, championship, game, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lba_championships-example}

```python
lba_championships(season=2027)
```

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
| `season` | Int64 | Season END year (2027 = 2026-27): the row's START year + 1. |
| `game_id` | Int64 | Game id (global across seasons); joins lba_calendar and lba_game. |
| `game_status` | String | Game status, number sent as a string: `0` scheduled, `1` live, `2` final (per the OSS client; only `2` sampled). |
| `championship_id` | Int64 | Championship id (season + phase) of the game. |
| `championships_day_id` | Int64 | Round id (`championships_day_id` key space: one round of one championship; joins `filters.days[].id` and standings `championships_day_id`; not the `d` param). |
| `year` | Int64 | Season START year (2026 = 2026-27); sdv END year = year + 1. |
| `number` | Int64 | Round number of the game, equal to `day_serial` (the `d` echo the capture checks). |
| `match_datetime` | String | Tip-off, ISO-8601 with the Italian offset (1995-96: midnight, no time kept). |
| `match_hh` | Int64 | Tip-off hour, local time. |
| `match_mm` | Int64 | Tip-off minute, local time. |
| `home_final_score` | Int64 | Final points of the home team. |
| `visitor_final_score` | Int64 | Final points of the away team. |
| `target_score` | String | Null in every sample (meaning unverified). |
| `additional_time` | Int64 | Overtime periods played (0 in every sample; reading inferred). |
| `plant_id` | Int64 | Arena id (arena key space; joins nothing specced). |
| `websocket_match_id` | Int64 | Live-feed game id, number sent as a string; separate from the game id. |
| `ticketing_url` | String | Ticket shop URL (null for old games). |
| `has_streaming` | Int64 | 1 when a stream exists, else 0. |
| `updated_at` | String | Timestamp, ISO-8601 with the Italian offset. Last update of the game row. |
| `h_team_id` | Int64 | Home team id (team-season). |
| `h_team_name` | String | Home team name (sponsor name of that season). |
| `h_club_code` | String | Home club code, e.g. `TVB`; null on the 2017-18 and 1995-96 calendar rows. |
| `h_team_enabled` | Boolean | Home team page enabled on the site. |
| `home_logo_key` | String | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `v_team_id` | Int64 | Away team id (team-season). |
| `v_team_name` | String | Visitor team name (the sponsor name of that season). |
| `v_club_code` | String | Away club code (`BCR`); null on the 2017-18 and 1995-96 calendar rows. |
| `v_team_enabled` | Boolean | Away team page enabled on the site. |
| `v_logo_key` | String | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `town_id` | Int64 | Town id of the arena (town key space; joins nothing specced). |
| `plant_name` | String | Name of the arena where the game is played. |
| `town_name` | String | Town of the arena. |
| `day_code` | String | Round code (`1`, `2`, ...; string in game rows, integer in standings). |
| `day_serial` | Int64 | Round number (`event_serial`, the `d` param). |
| `day_name` | String | Round name, Italian: `1° Giornata` = matchday 1. |
| `match_serie_key` | String | The two team ids joined low_high (`1761_1764`). |
| `referees` | String | JSON list of the game's referees (id, name, surname); null when the site lists none. |
| `broadcasters` | String | JSON list of the game's broadcasters (name, logo key); null when the site lists none. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#lba_calendar-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (an unknown season, a play-by-play before 2017-18, standings of a finished championship without a round, no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an unknown id (the site answers HTTP 500), a game that is not final, a truncated page, or a body for another season, championship, game, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lba_calendar-example}

```python
lba_calendar(championship_id=602, round=1)
```

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
| `season` | Int64 | Season END year (2027 = 2026-27): the row's START year + 1. |
| `game_id` | Int64 | Game id (global across seasons); joins lba_calendar and lba_game. |
| `game_status` | String | Game status, number sent as a string: `0` scheduled, `1` live, `2` final (per the OSS client; only `2` sampled). |
| `championship_id` | Int64 | Championship id (season + phase) of the game. |
| `competition_name` | String | Championship full name, e.g. `Regular Season 2026/27`. |
| `championships_day_id` | Int64 | Round id (`championships_day_id` key space: one round of one championship; joins `filters.days[].id` and standings `championships_day_id`; not the `d` param). |
| `year` | Int64 | Season START year (2026 = 2026-27); sdv END year = year + 1. |
| `number` | Int64 | Round number of the game, equal to `day_serial` (the `d` echo the capture checks). |
| `match_datetime` | String | Tip-off, ISO-8601 with the Italian offset (1995-96: midnight, no time kept). |
| `match_hh` | Int64 | Tip-off hour, local time. |
| `match_mm` | Int64 | Tip-off minute, local time. |
| `target_score` | String | Null in every sample (meaning unverified). |
| `home_final_score` | Int64 | Final points of the home team. |
| `visitor_final_score` | Int64 | Final points of the away team. |
| `q1_hs` | Int64 | Home points in the 1st quarter (null in the 1995-96 sample: no period scores then). |
| `q1_vs` | Int64 | Away points in the 1st quarter. |
| `q2_hs` | Int64 | Home points in the 2nd quarter. |
| `q2_vs` | Int64 | Away points in the 2nd quarter. |
| `q3_hs` | Int64 | Home points in the 3rd quarter (0 in the 1995-96 sample: a placeholder, not a score). |
| `q3_vs` | Int64 | Away points in the 3rd quarter (0 placeholder in 1995-96). |
| `q4_hs` | Int64 | Home points in the 4th quarter (0 placeholder in 1995-96). |
| `q4_vs` | Int64 | Away points in the 4th quarter (0 placeholder in 1995-96). |
| `ot_hs` | Int64 | Home overtime points (0 without overtime). |
| `ot_vs` | Int64 | Away overtime points. |
| `home_coach_id` | Int64 | Home head coach id. |
| `home_coach_code` | String | Home head coach code `SURNAME3-NAME3`. |
| `home_coach_fullname` | String | Home head coach, `Surname Name`. |
| `visitor_coach_id` | Int64 | Away head coach id. |
| `visitor_coach_code` | String | Away head coach code. |
| `visitor_coach_fullname` | String | Away head coach, `Surname Name`. |
| `additional_time` | Int64 | Overtime periods played (0 in every sample; reading inferred). |
| `plant_id` | Int64 | Arena id (arena key space; joins nothing specced). |
| `websocket_match_id` | Int64 | Live-feed game id, number sent as a string; separate from the game id. |
| `has_streaming` | Int64 | 1 when a stream exists, else 0. |
| `live_match_data` | String | Null in every sample (meaning unverified). |
| `quarter` | String | Last period played, number sent as a string (`4`; `5` = overtime). |
| `ticketing_url` | String | Ticket shop URL (null for old games). |
| `match_serie` | String | All-time head-to-head as `home wins-away wins` (`6-18`; inferred from `match_hw` / `match_vw`). |
| `match_hw` | Int64 | All-time head-to-head wins of the home club (inferred). |
| `match_vw` | Int64 | All-time head-to-head wins of the away club (inferred). |
| `updated_at` | String | Timestamp, ISO-8601 with the Italian offset. Last update of the game row. |
| `h_team_id` | Int64 | Home team id (team-season). |
| `h_team_name` | String | Home team name (sponsor name of that season). |
| `h_team_enabled` | Boolean | Home team page enabled on the site. |
| `home_logo_key` | String | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `v_team_id` | Int64 | Away team id (team-season). |
| `v_team_name` | String | Visitor team name (the sponsor name of that season). |
| `v_team_enabled` | Boolean | Away team page enabled on the site. |
| `v_logo_key` | String | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `plant_name` | String | Name of the arena where the game is played. |
| `day_code` | String | Round code (`1`, `2`, ...; string in game rows, integer in standings). |
| `day_serial` | Int64 | Round number (`event_serial`, the `d` param). |
| `day_name` | String | Round name, Italian: `1° Giornata` = matchday 1. |
| `competition_type_id` | Int64 | Phase type id: 4 = Regular Season, 2 = Play Off. |
| `competition_type_code` | String | Phase type code: `RS` / `PO`. |
| `competition_type` | String | Phase type name: `Regular Season` / `Play Off`. |
| `competition_serie_id` | Int64 | Series id, 1 = Serie A. |
| `competition_serie` | String | Series name, `Serie A`. |
| `day_phase_id` | Int64 | Phase id: 1 = `Andata` (first half of the season), 2 = `Ritorno` (second half). |
| `spectators` | Int64 | Attendance: spectators at the game. |
| `referees` | String | JSON list of the game's referees (id, name, surname); null when the site lists none. |
| `broadcasters` | String | JSON list of the game's broadcasters (name, logo key); null when the site lists none. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#lba_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (an unknown season, a play-by-play before 2017-18, standings of a finished championship without a round, no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an unknown id (the site answers HTTP 500), a game that is not final, a truncated page, or a body for another season, championship, game, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lba_game-example}

```python
lba_game(game_id=25602)
```

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
| `game_id` | Int64 | Game id (global across seasons); joins lba_calendar and lba_game. |
| `season` | Int64 | Season END year (2027 = 2026-27): the row's START year + 1. |
| `side` | String | Which team of the game the row belongs to: home or visitor. |
| `team_id` | Int64 | Team-season id (re-issued every season); join lba_teams for the stable club_id. |
| `player_id` | Int64 | Player id (global). |
| `player_num` | Int64 | Jersey number the player wore in the game. |
| `player_surname` | String | Family name, upper case. |
| `player_name` | String | Player's given name. |
| `player_code` | String | Player code `SURNAME3-NAME3-YY`. |
| `player_p_key` | String | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `pun` | Int64 | Points (`punti`). |
| `min` | Int64 | Minutes played, whole minutes (`minuti`); 200 in `totals` = 5 × 40. |
| `sec` | Int64 | Seconds played (`secondi`) = `min` × 60 in every sample. |
| `sf` | String | Starter flag (`starting five`), number sent as a string: `1` = started. |
| `falli_c` | Int64 | Fouls committed (`falli commessi`). |
| `falli_sf` | Int64 | Fouls drawn (`falli subiti`). |
| `falli_tc1` | Int64 | Technical fouls (`falli tecnici`; reading `tc1` as category-1 technicals is inferred). |
| `falli_fla` | Int64 | Unsportsmanlike / flagrant fouls (`falli antisportivi`). |
| `t2_r` | Int64 | 2-point field goals made (`tiri da 2 realizzati`). |
| `t2_t` | Int64 | 2-point field goals attempted (`tiri da 2 tentati`). |
| `t2_perc` | Float64 | 2-point field-goal percentage, 0-100 (number; 0 when no attempt). |
| `sc` | Int64 | Dunks (`schiacciate`). |
| `t3_r` | Int64 | 3-point field goals made. |
| `t3_t` | Int64 | 3-point field goals attempted. |
| `t3_perc` | Float64 | 3-point field-goal percentage, 0-100. |
| `tl_r` | Int64 | Free throws made (`tiri liberi realizzati`). |
| `tl_t` | Int64 | Free throws attempted (`tiri liberi tentati`). |
| `tl_perc` | Float64 | Free-throw percentage, 0-100. |
| `rimbalzi_o` | Int64 | Offensive rebounds (`rimbalzi offensivi`). |
| `rimbalzi_d` | Int64 | Defensive rebounds (`rimbalzi difensivi`). |
| `rimbalzi_t` | Int64 | Total rebounds (`rimbalzi totali`). |
| `stoppate_dat` | Int64 | Blocks (`stoppate date`). |
| `stoppate_sub` | Int64 | Own shots blocked (`stoppate subite`). |
| `palle_p` | Int64 | Turnovers (`palle perse`). |
| `palle_r` | Int64 | Steals (`palle recuperate`). |
| `ass` | Int64 | Assists credited to the player in the game. |
| `val_lega` | Int64 | LBA efficiency rating (`valutazione Lega`). |
| `val_oer` | Float64 | OER = points / (FGA + turnovers + FTA / 2); verified on every 2026-27 sample row. |
| `plus_minus` | Int64 | Plus-minus: point margin while on court. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#lba_box_score-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (an unknown season, a play-by-play before 2017-18, standings of a finished championship without a round, no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an unknown id (the site answers HTTP 500), a game that is not final, a truncated page, or a body for another season, championship, game, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lba_box_score-example}

```python
lba_box_score(game_id=25602)
```

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
| `game_id` | Int64 | Game id (global across seasons); joins lba_calendar and lba_game. |
| `season` | Int64 | Season END year (2027 = 2026-27): the row's START year + 1. |
| `home_team_id` | Int64 | Home team-season id of the game (the play-by-play ht_id). |
| `visitor_team_id` | Int64 | Visitor team-season id of the game (the play-by-play vt_id). |
| `team_side` | String | Side of the acting team, home or visitor; null for a clock action without a team. |
| `home_score` | Int64 | Home points after the action, parsed from the score text (2021-22 on; else null). |
| `visitor_score` | Int64 | Visitor points after the action, parsed from the score text (2021-22 on; else null). |
| `coord_frame` | String | Frame of x and y: half_court_px (2017-18 and 2018-19 image pixels) or court_pct (2019-20 on, 0-100). |
| `action_id` | Int64 | Action id within the game (ascending in time from 2021-22; not ordered before). |
| `description` | String | Action type in Italian, e.g. `2 punti segnato` (2-pointer made); full translation table in the Divergence notes. |
| `player_id` | Int64 | Player id (null for team and clock actions). |
| `team_id` | Int64 | Team id (team-season) of the actor. |
| `home_club` | Int64 | 1 = the home team acted, 0 = the away team; null for clock actions, and on every action in 2018-19 and 2019-20 (use `side` there). |
| `in_area` | Boolean | True on shots from the paint (zone `3`). |
| `dunk` | Int64 | 1 = dunk, 0 otherwise. |
| `seconds` | Int64 | Elapsed seconds within `minute` (0-59); 0 before 2019-20. Elapsed = minute × 60 + seconds; `print_time` = period length minus that. |
| `minute` | Int64 | Elapsed minute of the period (2019-20 on: 0-9, with `seconds`); 2017-18 / 2018-19: 1-10, the minute in progress, `seconds` 0. |
| `period` | Int64 | Period: 1-4, 5+ = overtime. |
| `order` | Int64 | Sort key; `sort=asc` orders by it. |
| `side` | Int64 | Basket the row's side_area_zone is measured from, as sent: 0 = the basket at x = 0, 1 = the one at x = 100 (zone A is that basket's half: x < 50 for 0, x >= 50 for 1). It is NOT the team of the game (that is team_side): on shots, assists, fouls committed, turnovers and offensive rebounds it is the actor's attacking basket, which changes at half-time; on defensive rebounds, steals, blocks and fouls drawn it is the opponent's. |
| `x` | Int64 | Court x; frame by era (see the coordinates section): 2019-20 on, 0-100 % of the 28 m length; 2017-18 / 2018-19, half-court image pixels; null (2019-20 on) or 0 (earlier) when unlocated. |
| `y` | Int64 | Court y; 2019-20 on, 0-100 % of the 15 m width; 2017-18 / 2018-19, image pixels (see the coordinates section). |
| `score` | String | Score after the action, `home - away` string (`0 - 0`); 2020-21 on scoring actions only, null before. |
| `linked_action_id` | Int64 | `action_id` of the related action: the shot an assist or rebound belongs to, the foul a drawn foul answers, the player swapped in a substitution. |
| `print_time` | String | Game clock remaining, `MM:SS` (10:00 at a period start); null before 2021-22. |
| `action_1_qualifier_code` | String | First qualifier code, number sent as a string (shot type, turnover type, foul type; see `legend`). |
| `action_2_qualifier_code` | String | Second qualifier code (fast break `500`, blocked `1005`, tip-ins `1001`-`1004`, ...). |
| `action_1_qualifier_description` | String | `legend` label of `action_1_qualifier_code`, Italian. |
| `action_2_qualifier_description` | String | `legend` label of `action_2_qualifier_code`, Italian. |
| `side_area_zone` | String | `A` = in the actor's attacking half, `D` = its defensive half (2021-22 on). |
| `side_area_code` | String | Court zone `1`-`10`, number sent as a string (see the coordinates section; 2021-22 on). |
| `player_name` | String | Given name of the acting player. |
| `player_surname` | String | Family name of the acting player. |
| `player_number` | String | Jersey number, number sent as a string. |
| `team_name` | String | Team name in that season, with its sponsor (`Nutribullet Treviso Basket`); changes with the sponsor. |
| `id` | Int64 | Always null (not a key). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#lba_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (an unknown season, a play-by-play before 2017-18, standings of a finished championship without a round, no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an unknown id (the site answers HTTP 500), a game that is not final, a truncated page, or a body for another season, championship, game, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lba_pbp-example}

```python
lba_pbp(game_id=25602)
```

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
| `code` | Int64 | Qualifier code, the value of action_1_qualifier_code / action_2_qualifier_code on the actions. |
| `label` | String | Italian label of the qualifier code, as sent. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#lba_pbp_legend-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (an unknown season, a play-by-play before 2017-18, standings of a finished championship without a round, no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an unknown id (the site answers HTTP 500), a game that is not final, a truncated page, or a body for another season, championship, game, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lba_pbp_legend-example}

```python
lba_pbp_legend(game_id=25602)
```

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
| `game_id` | Int64 | Game id (global across seasons); joins lba_calendar and lba_game. |
| `season` | Int64 | Season END year (2027 = 2026-27): the row's START year + 1. |
| `side` | String | Which team of the game the row belongs to: home or visitor. |
| `team_id` | Int64 | Team-season id (re-issued every season); join lba_teams for the stable club_id. |
| `zone` | Int64 | Court zone 1-10 (the play-by-play side_area_code); zone 7 also holds the free throws. |
| `made` | Int64 | Shots made in the zone, parsed from num. |
| `attempts` | Int64 | Shots attempted in the zone, parsed from num. |
| `perc` | Float64 | Made / attempted × 100. |
| `precision` | Int64 | Decimal places the site shows (1). |
| `num` | String | Made/attempted as a string (`17/21`); zone `7` also counts free throws. |
| `opacity` | Int64 | Heat-map shading for the site: `perc` floored, minimum 5. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#lba_shots-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (an unknown season, a play-by-play before 2017-18, standings of a finished championship without a round, no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an unknown id (the site answers HTTP 500), a game that is not final, a truncated page, or a body for another season, championship, game, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lba_shots-example}

```python
lba_shots(game_id=25602)
```

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
| `season` | Int64 | Season END year (2027 = 2026-27): the row's START year + 1. |
| `standing_id` | Int64 | Standings row id (one team after one round; joins nothing else). |
| `championships_day_id` | Int64 | Round id the standings are after. |
| `team_id` | Int64 | Team id (team-season). |
| `position` | Int64 | Rank in the standings. |
| `wins` | Int64 | Games the team has won through the round. |
| `defeats` | Int64 | Games the team has lost through the round. |
| `points_made` | Int64 | Points the team has scored through the round. |
| `points_suffered` | Int64 | Points allowed. |
| `qp` | Int64 | Tie-break quotient (`quoziente punti`?); 1 in the samples (unverified). |
| `lv` | String | Null in every sample (meaning unverified). |
| `lp` | String | Null in every sample (meaning unverified). |
| `lpf` | String | Null in every sample (meaning unverified). |
| `lps` | String | Null in every sample (meaning unverified). |
| `lqp` | String | Null in every sample (meaning unverified). |
| `updated_at` | String | Timestamp, ISO-8601 with the Italian offset. |
| `points` | Int64 | Standings points (2 per win). |
| `penalty_points` | Int64 | Points deducted (0 or null). |
| `game_played` | Int64 | Games the team has played through the round. |
| `home_game_played` | Int64 | Home games played. |
| `home_wins` | Int64 | Home games won. |
| `home_defeats` | Int64 | Home games the team has lost through the round. |
| `away_game_played` | Int64 | Away games played. |
| `away_wins` | Int64 | Away games won. |
| `away_defeats` | Int64 | Away games the team has lost through the round. |
| `avg_points_made` | Float64 | Points scored per game. |
| `avg_points_suffered` | Float64 | Points allowed per game. |
| `home_points_made` | Int64 | Points scored at home. |
| `home_points_suffered` | Int64 | Points allowed at home. |
| `home_avg_points_made` | Float64 | Points scored per home game. |
| `home_avg_points_suffered` | Float64 | Points allowed per home game. |
| `away_points_made` | Int64 | Points scored away. |
| `away_points_suffered` | Int64 | Points allowed away. |
| `away_avg_points_made` | Float64 | Points scored per away game. |
| `away_avg_points_suffered` | Float64 | Points allowed per away game. |
| `year` | Int64 | Season START year (2026 = 2026-27); sdv END year = year + 1. |
| `ties` | Int64 | Ties (0 or null; basketball has none). |
| `home_ties` | Int64 | Home ties (null). |
| `away_ties` | Int64 | Away ties (null). |
| `play_off_qualified` | Boolean | Clinched a play-off place (null in old seasons). |
| `relegation` | Boolean | Relegated (null in old seasons). |
| `movement` | String | Position change since the previous round, signed string (`+2`, `-1`; null for finished seasons). |
| `wins_percent` | Float64 | Win percentage as a string with `%` (`100%`). |
| `home_wins_percent` | Float64 | Home win percentage, string with `%`. |
| `away_wins_percent` | Float64 | Away win percentage, string with `%`. |
| `max_points_made` | Int64 | Most points scored in a game. |
| `home_max_points_made` | Int64 | Most points scored in a home game. |
| `away_max_points_made` | Int64 | Most points scored in an away game. |
| `day_code` | Int64 | Round code the standings are after (integer). |
| `day_name` | String | Round name, Italian (`4ª Giornata - 2° fase` = matchday 4 of the 2nd phase). |
| `team_name` | String | Team name in that season, with its sponsor (`Nutribullet Treviso Basket`); changes with the sponsor. |
| `club_code` | String | Club code (`TVB`, `TS`); whether it is stable across seasons is unverified. |
| `logo_key` | String | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `ctp` | Int64 | Classification marker; 0 or null in the samples (unverified). |
| `ctp_symbol` | String | Null in every sample (meaning unverified). |
| `ctp_description` | String | Null in every sample (meaning unverified). |
| `season_start_year` | Int64 | The wire season field: the season START year (2026 = 2026-27), same as year. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#lba_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (an unknown season, a play-by-play before 2017-18, standings of a finished championship without a round, no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an unknown id (the site answers HTTP 500), a game that is not final, a truncated page, or a body for another season, championship, game, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lba_standings-example}

```python
lba_standings(championship_id=602, round=3)
```

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
| `team_id` | Int64 | Team-season id (re-issued every season); join lba_teams for the stable club_id. |
| `club_id` | Int64 | Club id: the cross-season join key for team ids (only here and in the `team` headers); stability across seasons is unverified. |
| `club_code` | String | Club code (`TVB`, `TS`); whether it is stable across seasons is unverified. |
| `season` | Int64 | Season END year (2027 = 2026-27): the row's START year + 1. |
| `player_id` | Int64 | Player id (global). |
| `name` | String | Player's given name. |
| `surname` | String | Player's family name. |
| `code` | String | Player code `SURNAME3-NAME3-YY`. |
| `place_of_birth` | String | Birthplace (Italian spelling). |
| `birth_date` | String | Birth date, Date `YYYY-MM-DD`. |
| `player_number` | String | Jersey number, number sent as a string. |
| `country` | String | Nationality, ISO alpha-3. |
| `uefa_ratio` | String | Eligibility class for the foreign-player quota: `I`, `E` or `C` (7 / 6 / 1 on the Treviso roster; reading as Italian / EU / non-EU is inferred). |
| `height` | Int64 | Player's height in centimetres. |
| `weight` | Int64 | Player's weight in kilograms. |
| `year` | Int64 | Season START year of the player entry (2026 = 2026-27). |
| `start_date` | String | Contract start with the team, Date `YYYY-MM-DD`. |
| `end_date` | String | Contract end with the team, Date `YYYY-MM-DD`. |
| `player_role_id` | Int64 | Position id (position key space: the id behind `player_role`; joins the `r_id` filter). |
| `player_role` | String | Position, Italian: `Playmaker` (point guard), `Guardia` (guard), `Ala` (forward), `Centro` (center), `-` unknown. |
| `team_name` | String | Team name in that season, with its sponsor (`Nutribullet Treviso Basket`); changes with the sponsor. |
| `player_picture_key` | String | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `team_logo_key` | String | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#lba_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (an unknown season, a play-by-play before 2017-18, standings of a finished championship without a round, no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an unknown id (the site answers HTTP 500), a game that is not final, a truncated page, or a body for another season, championship, game, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lba_roster-example}

```python
lba_roster(team_id=1764)
```

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
| `team_id` | Int64 | Team-season id (re-issued every season); join lba_teams for the stable club_id. |
| `club_id` | Int64 | Club id: the cross-season join key for team ids (only here and in the `team` headers); stability across seasons is unverified. |
| `club_code` | String | Club code (`TVB`, `TS`); whether it is stable across seasons is unverified. |
| `season` | Int64 | Season END year (2027 = 2026-27): the row's START year + 1. |
| `coach_id` | Int64 | Coach id (global coach key space; joins the game header `home_coach_id` / `visitor_coach_id`). |
| `name` | String | Coach's given name. |
| `surname` | String | Coach's family name. |
| `birth_date` | String | Birth date, Date `YYYY-MM-DD`. |
| `place_of_birth` | String | Birthplace (Italian spelling, e.g. `Londra`). |
| `coach_type_id` | Int64 | Coach role id (1 = head coach). |
| `coach_type_description` | String | Coach role, Italian: `Allenatore` = head coach. |
| `coach_type_code` | String | Coach role code, number sent as a string. |
| `team_name` | String | Team, short name. |
| `coach_picture_key` | String | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `team_logo_key` | String | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#lba_roster_coach-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (an unknown season, a play-by-play before 2017-18, standings of a finished championship without a round, no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an unknown id (the site answers HTTP 500), a game that is not final, a truncated page, or a body for another season, championship, game, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lba_roster_coach-example}

```python
lba_roster_coach(team_id=1764)
```

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
| `season` | Int64 | Season END year (2027 = 2026-27): the row's START year + 1. |
| `stat_line_id` | Int64 | Id of the player's stat line for the game (one per player and game). |
| `championship_id` | Int64 | Championship id of the game. |
| `championships_match_id` | Int64 | Game id (global across seasons); joins lba_calendar and lba_game. |
| `player_id` | Int64 | Player id (global, one per career). |
| `team_id` | Int64 | Team id (team-season) of the player. |
| `number` | Int64 | Jersey number the player wore in the game. |
| `played_minutes` | Int64 | Minutes played, in that game. |
| `points` | Int64 | Points, in that game. |
| `suffered_fouls` | Int64 | Fouls drawn, in that game. |
| `done_fouls` | Int64 | Fouls committed, in that game. |
| `shots_2p_realized` | Int64 | 2-point field goals made, in that game. |
| `shots_2p_total` | Int64 | 2-point field goals attempted, in that game. |
| `shots_3p_realized` | Int64 | 3-point field goals made, in that game. |
| `shots_3p_total` | Int64 | 3-point field goals attempted, in that game. |
| `free_throws_realized` | Int64 | Free throws made, in that game. |
| `free_throws_total` | Int64 | Free throws attempted, in that game. |
| `restricted_area_p1` | String | Null in every sample (meaning unverified). |
| `restricted_area_p2` | String | Null in every sample (meaning unverified). |
| `restricted_area_p3` | String | Null in every sample (meaning unverified). |
| `slam_dunk` | Int64 | Dunks, in that game. |
| `offensive_rebound` | Int64 | Offensive rebounds, in that game. |
| `defensive_rebound` | Int64 | Defensive rebounds, in that game. |
| `ball_stop_given` | Int64 | Blocks, in that game. |
| `ball_stop_received` | Int64 | Own shots blocked, in that game. |
| `lost_balls` | Int64 | Turnovers, in that game. |
| `regain_balls` | Int64 | Steals, in that game. |
| `assists` | Int64 | Assists, in that game. |
| `technical_fouls` | String | Technical fouls (null in old seasons). |
| `minute5_fouls` | Int64 | Minute of the fifth foul (0 = did not foul out; inferred, unverified). |
| `time5_fouls` | Int64 | Game time of the fifth foul (0 = did not foul out; inferred, unverified). |
| `rating_lega` | Int64 | LBA efficiency rating (`valutazione`), in that game. |
| `quintet` | Int64 | Starts (in the starting five), in that game. |
| `rating_oer` | Float64 | OER (points / (FGA + turnovers + FTA / 2)), in that game. |
| `avg_points` | Int64 | The player's plus-minus in the game (the site's name is misleading). |
| `wp_id` | Int64 | Null in every sample (meaning unverified). |
| `deleted_at` | String | Null in every sample (meaning unverified). |
| `created_at` | String | Timestamp, ISO-8601 with the Italian offset. |
| `updated_at` | String | Timestamp, ISO-8601 with the Italian offset. |
| `player_role_id` | Int64 | Position id (position key space: the id behind `player_role`; joins the `r_id` filter). |
| `order` | Int64 | Row order in the box score. |
| `captain` | Boolean | Team captain in that game. |
| `shots_2p_perc` | Int64 | 2-point field-goal percentage, 0-100, in that game. |
| `shots_3p_perc` | Int64 | 3-point field-goal percentage, 0-100, in that game. |
| `free_throws_perc` | Int64 | Free-throw percentage, 0-100, in that game. |
| `total_rebound` | Int64 | Total rebounds, in that game. |
| `ext_code` | String | `<websocket_match_id>\\|<team code>\\|<player code>` (`1674778\\|TVU\\|CAP-ALE-95`). |
| `fouls_first_category` | Int64 | Category-1 technical fouls (null in old seasons; reading inferred). |
| `fouls_flagrant` | Int64 | Unsportsmanlike / flagrant fouls. |
| `championship_day_name` | String | Round code, number sent as a string. |
| `home_score` | Int64 | Home points after the action, parsed from the score text (2021-22 on; else null). |
| `visitor_score` | Int64 | Visitor points after the action, parsed from the score text (2021-22 on; else null). |
| `match_date` | String | Tip-off in UTC, ISO-8601 `Z` (local time in `match_hh` / `match_mm`). |
| `match_hh` | Int64 | Tip-off hour, local time. |
| `match_mm` | Int64 | Tip-off minute, local time. |
| `championship_match_id` | Int64 | Game id (repeated). |
| `player_team_id` | Int64 | Team id (team-season) of the player (repeated). |
| `player_team_name` | String | Short name of the player's team in the game (without the season sponsor). |
| `player_team_code` | String | `<season>\\|<team code>` (`1995\\|BOF`). |
| `visitor_team_id` | Int64 | Visitor team-season id of the game (the play-by-play vt_id). |
| `visitor_team_name` | String | Away team short name. |
| `visitor_team_logo_key` | String | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `home_team_id` | Int64 | Home team-season id of the game (the play-by-play ht_id). |
| `home_team_name` | String | Home team short name. |
| `home_team_logo_key` | String | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `phase_description` | String | Phase code: `And` (Andata, 1st half of the season) / `Rit` (Ritorno). |
| `match_adp` | Int64 | The player's plus-minus in the game (the site's name is misleading). |
| `home_match_result` | String | `w` / `l` for the home team when the player was on it, else null. |
| `visitor_match_result` | String | `w` / `l` for the away team when the player was on it, else null. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#lba_game_log-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (an unknown season, a play-by-play before 2017-18, standings of a finished championship without a round, no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an unknown id (the site answers HTTP 500), a game that is not final, a truncated page, or a body for another season, championship, game, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lba_game_log-example}

```python
lba_game_log(player_id=969, season=2027)
```
