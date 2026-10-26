---
title: "VTB — VTB United League Infobasket (Russia) — Other"
sidebar_label: "Other"
sidebar_position: 5
description: "VTB — VTB United League Infobasket (Russia) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# VTB — VTB United League Infobasket (Russia) — Other

## vtb_infobasket_seasons

Season END year -> season-level CompID for a competition tag (the table every season-keyed wrapper maps with).

**Endpoint URL:** `GET https://org.infobasket.su/Comp/GetSeasonsForTag`

**Valid URL:** [https://org.infobasket.su/Comp/GetSeasonsForTag?format=json&tag=vtb](https://org.infobasket.su/Comp/GetSeasonsForTag?format=json&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `comp_id` | Int64 | Season-level CompID of that season (50714 = VTB 2025-26). Pass it as `comps` to /Comp/GetCalendar/. |
| `name_ru` | String | Season name in Russian ('Сезон 2025/2026' = 'Season 2025/2026'). |
| `name_en` | String | Season name in English. |
| `db` | String | Infobasket database the competition lives in ('org'). |
| `tag` | String | Competition tag of the request (vtb is the VTB United League; vtbyouth, wbc, wpremier and rfb also answer). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_seasons-example}

```python
vtb_infobasket_seasons()
```

## vtb_infobasket_calendar

Every game of a season (finished, live and planned): teams, scores, date, venue, attendance.

**Endpoint URL:** `GET https://org.infobasket.su/Comp/GetCalendar/`

**Valid URL:** [https://org.infobasket.su/Comp/GetCalendar/?format=json&season=2026&tag=vtb](https://org.infobasket.su/Comp/GetCalendar/?format=json&season=2026&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). Required. |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_calendar-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `comp_id` | Int64 | Season-level Infobasket CompID of the season (50714 = 2025-26): the id every season-keyed route takes. |
| `game_id` | Int64 | Infobasket game id (GameID); equals the v2 API `matchId`. Joins to /Widget/GetOnline/{gameId} and /Widget/GamePage/{gameId}. |
| `game_number` | String | Game number within the competition, e.g. 'RS-002' (RS = regular season); playoff games use the series code. |
| `game_date` | String | Game date as an ISO 8601 date (YYYY-MM-DD); the wire sends DD.MM.YYYY. |
| `has_time` | Boolean | True when a tip-off time is set. |
| `game_time` | String | Tip-off local time, 'HH:MM'. |
| `game_time_msk` | String | Tip-off time in Moscow time ('HH:MM' on calendar rows, 'HH.MM' in GetOnline). |
| `game_date_time` | String | Tip-off instant as an ISO 8601 string in Moscow time with its +03:00 offset (the wire sends a .NET /Date(ms)/ value). |
| `game_date_time_moscow` | String | Tip-off instant as an ISO 8601 string in Moscow time with its +03:00 offset (the wire's second .NET date; equal to game_date_time in every sample). |
| `game_status` | Int64 | Game status in the calendar code set: 0 planned, 1 completed, 2 live. |
| `team_a_id` | Int64 | Team A (home) TeamID. Infobasket team id (TeamID): one club's entry in a competition family (Lokomotiv Kuban = 2650). Joins to /Widget/Team*/{teamId} and equals the v2 API `teamId`. |
| `team_b_id` | Int64 | Team B (away) TeamID. Infobasket team id (TeamID): one club's entry in a competition family (Lokomotiv Kuban = 2650). Joins to /Widget/Team*/{teamId} and equals the v2 API `teamId`. |
| `short_team_name_aru` | String | Team short name of team A (home), Russian. |
| `short_team_name_bru` | String | Team short name of team B (away), Russian. |
| `team_name_aru` | String | Team name with city in brackets of team A (home), Russian. |
| `team_name_bru` | String | Team name with city in brackets of team B (away), Russian. |
| `comp_team_name_aru` | String | Team name as registered in the competition of team A (home), Russian. |
| `comp_team_name_bru` | String | Team name as registered in the competition of team B (away), Russian. |
| `region_team_name_aru` | String | Team's city or region of team A (home), Russian. |
| `region_team_name_bru` | String | Team's city or region of team B (away), Russian. |
| `short_team_name_aen` | String | Team short name of team A (home), English. |
| `short_team_name_ben` | String | Team short name of team B (away), English. |
| `team_name_aen` | String | Team name with city in brackets of team A (home), English. |
| `team_name_ben` | String | Team name with city in brackets of team B (away), English. |
| `comp_team_name_aen` | String | Team name as registered in the competition of team A (home), English. |
| `comp_team_name_ben` | String | Team name as registered in the competition of team B (away), English. |
| `region_team_name_aen` | String | Team's city or region of team A (home), English. |
| `region_team_name_ben` | String | Team's city or region of team B (away), English. |
| `score_a` | Int64 | Final score of team A (home). |
| `score_b` | Int64 | Final score of team B (away). |
| `tv_ru` | String | TV channels, in Russian. |
| `tv_en` | String | TV channels, in English. |
| `region_ru` | String | City or region of the game's venue in Russian. |
| `region_en` | String | City or region of the game's venue in English. |
| `arena_en` | String | Arena name, English. |
| `arena_ru` | String | Arena, Russian ('СК' = sports complex). |
| `arena_id` | Int64 | Infobasket arena id; equals the v2 API `venueId` (11687 = SK Basket-Hall, Krasnodar). |
| `game_attendance` | Int64 | Spectators at the game (null when not recorded). |
| `comp_name_ru` | String | Stage name, Russian ('Регулярный чемпионат' = 'Regular Season', 'Группа А' = 'Group A'). |
| `comp_name_en` | String | Stage name (e.g. 'Regular Season'), English. |
| `league_name_ru` | String | League name, Russian ('Единая лига ВТБ' / 'Чемпионат Единой лиги ВТБ' = 'VTB United League' / 'VTB United League Championship'). |
| `league_name_en` | String | League name, English. |
| `league_short_name_ru` | String | League short name, Russian ('ВТБ' = 'VTB'). |
| `league_short_name_en` | String | League short name, English. |
| `gender` | Int64 | Competition gender: 0 on every VTB sample (men). Other values unverified. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_calendar-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_calendar-example}

```python
vtb_infobasket_calendar(season=2026)
```

## vtb_infobasket_game

Header of a finished game: stage, venue, rules, the two teams and their scores.

**Endpoint URL:** `GET https://org.infobasket.su/Widget/GetOnline/{game_id}`

**Valid URL:** [https://org.infobasket.su/Widget/GetOnline/921391?format=json](https://org.infobasket.su/Widget/GetOnline/921391?format=json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Infobasket GameID (game_id in vtb_infobasket_calendar; the v2 matches' match_id), e.g. 921391. Global across seasons. |

### Returns {#vtb_infobasket_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `is_online` | Boolean | True while the game is live; False for a completed game. |
| `game_status` | Int64 | Game status: 0 planned, 1 completed, 2 live (the site's widget code). Samples are completed games. |
| `max_period` | Int64 | Last period played (4 = regulation; 5+ = overtime). |
| `from_date` | String | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `game_date` | String | Game date (ISO), local. |
| `has_time` | Boolean | True when a tip-off time is set. |
| `game_time` | String | Tip-off time of day as 'HH.MM' (a dot, local time). |
| `game_time_msk` | String | Tip-off time of day as 'HH.MM' in Moscow time. |
| `has_video` | Boolean | True when a video embed is attached. |
| `tv_ru` | String | TV channels, in Russian ('Матч ТВ' = Match TV). |
| `tv_en` | String | TV channels, in English. |
| `comp_name_ru` | String | Stage name, Russian ('Регулярный чемпионат' = 'Regular Season'). |
| `comp_name_en` | String | Stage name (e.g. 'Regular Season'), English. |
| `league_name_ru` | String | League name, Russian ('Единая лига ВТБ' = 'VTB United League'). |
| `league_name_en` | String | League name, English. |
| `league_short_name_ru` | String | League short name, Russian. |
| `league_short_name_en` | String | League short name, English. |
| `gender` | Int64 | Competition gender code from the host (1 male, 0 unspecified). |
| `comp_id` | Int64 | Stage CompID of the game (50720 = the 2025-26 regular season): a stage below the season. |
| `is3x3` | Boolean | True for a 3x3 competition (False in every sample). |
| `season_comp_id` | Int64 | Season-level CompID of the game (the body calls it LeagueID). |
| `game_id` | Int64 | Infobasket GameID of the game (the v2 matches' match_id); global across seasons. |
| `game_number` | String | Game number within the competition, e.g. 'RS-002' (RS = regular season); playoff games use the series code. |
| `game_date_time` | String | Tip-off as an ISO 8601 string in Moscow time (+03:00), from Online.GameTime. |
| `region_id` | Int64 | Infobasket region (city) id of the venue. |
| `arena_id` | Int64 | Infobasket arena id; equals the v2 API `venueId` (11687 = SK Basket-Hall, Krasnodar). |
| `venue_en` | String | Venue, 'City, Arena', in English. |
| `venue_ru` | String | Venue, 'City, Arena', in Russian. |
| `attendance` | Int64 | Spectators at the game (null when not recorded). |
| `period_count` | Int64 | Regulation periods (4). |
| `period_extra_seconds` | Int64 | Overtime length in seconds (300 = 5 minutes). |
| `period_minutes` | String | Period lengths in minutes, comma separated ('10,10,10,10'). |
| `intervals_minutes` | String | Break lengths in minutes after each period, comma separated ('2,15,2,2'). |
| `personal_fouls` | Int64 | Personal fouls that disqualify a player (5). |
| `team_fouls` | String | Team-foul setting per period, comma separated (4 or 5 seen; exact meaning unverified). |
| `players_on_court` | Int64 | Players per team on the court (5). |
| `free_throw_value` | Int64 | Points for a made free throw (1). |
| `field_goal_value` | Int64 | Points for a made 2-point shot (2). |
| `long_shot_value` | Int64 | Points for a made 3-point shot (3). |
| `left_team_number` | Int64 | Team number drawn on the left in the widget (1 in every sample). |
| `team_a_id` | Int64 | Infobasket TeamID of team A, the team listed first (the home side in the calendar). |
| `score_a` | Int64 | Final points of team A (listed first). |
| `team_a_short_name_ru` | String | The team A (the home side in the calendar)'s short name in Russian (as the competition names it). |
| `team_a_short_name_en` | String | The team A (the home side in the calendar)'s short name in English (as the competition names it). |
| `team_a_name_ru` | String | The team A (the home side in the calendar)'s name in Russian (as the competition names it). |
| `team_a_name_en` | String | The team A (the home side in the calendar)'s name in English (as the competition names it). |
| `team_a_region_name_ru` | String | The team A (the home side in the calendar)'s region or city in Russian (as the competition names it). |
| `team_a_region_name_en` | String | The team A (the home side in the calendar)'s region or city in English (as the competition names it). |
| `team_b_id` | Int64 | Infobasket TeamID of team B, the team listed second (the away side). |
| `score_b` | Int64 | Final points of team B. |
| `team_b_short_name_ru` | String | The team B (the away side)'s short name in Russian (as the competition names it). |
| `team_b_short_name_en` | String | The team B (the away side)'s short name in English (as the competition names it). |
| `team_b_name_ru` | String | The team B (the away side)'s name in Russian (as the competition names it). |
| `team_b_name_en` | String | The team B (the away side)'s name in English (as the competition names it). |
| `team_b_region_name_ru` | String | The team B (the away side)'s region or city in Russian (as the competition names it). |
| `team_b_region_name_en` | String | The team B (the away side)'s region or city in English (as the competition names it). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_game-example}

```python
vtb_infobasket_game(game_id=921391)
```

## vtb_infobasket_box_score

Player box score of a finished game (one row per player of each team, DNPs included).

**Endpoint URL:** `GET https://org.infobasket.su/Widget/GetOnline/{game_id}`

**Valid URL:** [https://org.infobasket.su/Widget/GetOnline/921391?format=json](https://org.infobasket.su/Widget/GetOnline/921391?format=json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Infobasket GameID (game_id in vtb_infobasket_calendar; the v2 matches' match_id), e.g. 921391. Global across seasons. |

### Returns {#vtb_infobasket_box_score-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Infobasket GameID of the game (the v2 matches' match_id); global across seasons. |
| `team_id` | Int64 | Infobasket TeamID (2650 = Lokomotiv Kuban); the v2 teamId. Not a club-record id and not a registration id. |
| `is_home` | Boolean | True for team 1, the team listed first (team A, the home side in the calendar); the body has no explicit home flag (unverified). |
| `person_id` | Int64 | Infobasket person id (PersonID): one person (player, coach or official) across clubs and seasons. Joins to /Widget/PlayerPage/{personId} (players) and /Widget/CoachPage/{personId} (coaches). |
| `team_number` | Int64 | Side of the player's team: 1 team A (home), 2 team B (away). |
| `player_number` | Int64 | Shirt number as an integer (-1 when none). |
| `display_number` | String | Shirt number as displayed ('0' and '00' differ). |
| `last_name_ru` | String | Last name in Russian. |
| `last_name_en` | String | Last name in English. |
| `first_name_ru` | String | First name in Russian. |
| `first_name_en` | String | First name in English. |
| `person_name_ru` | String | Name 'Last First' in Russian. |
| `person_name_en` | String | Name 'Last First' in English. |
| `is_captain` | Boolean | 1 for the team captain, else 0. |
| `birth_date` | String | Birth date (ISO 8601 date) of a player: public bio, kept; coaches' and officials' birth data is never output. |
| `pos_id` | Int64 | Position code: players 1 point guard, 2 shooting guard, 3 small forward, 4 power forward, 5 centre; coaches 1000 head coach, 1001 assistant; officials 101-103 referees, 104 commissioner, 111-123 table officials (exact roles unverified). |
| `country_code_ioc` | String | Nationality code: ISO 3166 alpha-2 in most rows ('RU', 'US'), IOC alpha-3 in some ('BLR'). |
| `country_name_ru` | String | Nationality code (same as CountryCodeIOC), not a name. |
| `country_name_en` | String | Nationality code (same as CountryCodeIOC), not a name. |
| `rank_ru` | String | Sporting title, Russian; empty or null in the samples. |
| `rank_en` | String | Sporting title, English; empty or null in the samples. |
| `height` | Int64 | Height in centimetres. |
| `weight` | Int64 | Weight in kilograms. |
| `points` | Int64 | Points. The wire's null is 0 here (null counts are filled). |
| `shot1` | Int64 | Free throws attempted. The wire's null is 0 here (null counts are filled). |
| `goal1` | Int64 | Free throws made. The wire's null is 0 here (null counts are filled). |
| `shot1_percent` | Float64 | Free-throw percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot2` | Int64 | 2-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal2` | Int64 | 2-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot2_percent` | Float64 | 2-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot3` | Int64 | 3-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal3` | Int64 | 3-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot3_percent` | Float64 | 3-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `assist` | Int64 | Assists. The wire's null is 0 here (null counts are filled). |
| `blocks` | Int64 | Blocked shots. The wire's null is 0 here (null counts are filled). |
| `def_rebound` | Int64 | Defensive rebounds. The wire's null is 0 here (null counts are filled). |
| `off_rebound` | Int64 | Offensive rebounds. The wire's null is 0 here (null counts are filled). |
| `rebound` | Int64 | Total rebounds. The wire's null is 0 here (null counts are filled). |
| `steal` | Int64 | Steals. The wire's null is 0 here (null counts are filled). |
| `turnover` | Int64 | Turnovers. The wire's null is 0 here (null counts are filled). |
| `foul` | Int64 | Personal fouls committed. The wire's null is 0 here (null counts are filled). |
| `opponent_foul` | Int64 | Fouls drawn. The wire's null is 0 here (null counts are filled). |
| `plus_minus` | Int64 | Plus-minus: point differential while on the court. Null where the host sends none. |
| `seconds` | Int64 | Playing time in seconds. |
| `played_time` | String | Playing time as 'minutes:seconds' (team rows sum the five players: '200:00' = 40 minutes x 5). |
| `is_start` | Boolean | True for a starter. |
| `start_mark` | String | '+' for a starter, else empty. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_box_score-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_box_score-example}

```python
vtb_infobasket_box_score(game_id=921391)
```

## vtb_infobasket_pbp

Play-by-play of a finished game with shot x/y (2022-23 onwards; earlier games have no plays).

**Endpoint URL:** `GET https://org.infobasket.su/Widget/GetOnline/{game_id}`

**Valid URL:** [https://org.infobasket.su/Widget/GetOnline/921391?format=json](https://org.infobasket.su/Widget/GetOnline/921391?format=json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Infobasket GameID (game_id in vtb_infobasket_calendar; the v2 matches' match_id), e.g. 921391. Global across seasons. |

### Returns {#vtb_infobasket_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Infobasket GameID of the game (the v2 matches' match_id); global across seasons. |
| `play_id` | Int64 | Play id within the game (the row-0 header play included); parent_play_id points at another play_id. |
| `parent_play_id` | Int64 | play_id of the parent play (a made shot's children: shot type, assist, possession change). |
| `play_type_id` | Int64 | Play type code (see play_type; the full table is in vtb-returns.md). |
| `play_type` | String | English label of play_type_id from the site widget's table (null for a code the table lacks). |
| `play_period` | Int64 | Period: 1-4 quarters, 5+ overtimes. |
| `play_second` | Int64 | Time ELAPSED in the period, in tenths of a second (6000 = 600 s = end of a 10-minute quarter). Game clock = period length - PlaySecond/10. |
| `play_sort_order` | Int64 | Order among plays sharing the same play_second. |
| `play_flag` | Int64 | Play flag; 0 in every sample (meaning unverified). |
| `start_id` | Int64 | Line-up slot id within this game's line-up (0 = the placeholder row); not a person id. |
| `team_number` | Int64 | Side (1 or 2) of the line-up slot the play belongs to; null for a play with no player. |
| `team_id` | Int64 | Infobasket TeamID of the team the play belongs to; null for a play with no player. |
| `person_id` | Int64 | Infobasket PersonID of the player of the line-up slot; null for a play with no player. |
| `play_zone` | Int64 | Court zone code of a shot: 1-3 count as paint in the site's widget, 3-pointers sit in zones 7-12; the full map is unverified. |
| `play_x` | Int64 | Shot x in decimetres across the court, -75..75, origin at the basket centre (orientation from the renderer, unverified). |
| `play_y` | Int64 | Shot y in decimetres from the basket toward midcourt (about -16 baseline, 124 midcourt). |
| `video_from` | Int64 | Video clip start, Unix epoch seconds; 0 when none. |
| `video_to` | Int64 | Video clip end, Unix epoch seconds; 0 when none. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_pbp-example}

```python
vtb_infobasket_pbp(game_id=921391)
```

## vtb_infobasket_game_staff

The coaches and officials of a finished game (names and roles only).

**Endpoint URL:** `GET https://org.infobasket.su/Widget/GetOnline/{game_id}`

**Valid URL:** [https://org.infobasket.su/Widget/GetOnline/921391?format=json](https://org.infobasket.su/Widget/GetOnline/921391?format=json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Infobasket GameID (game_id in vtb_infobasket_calendar; the v2 matches' match_id), e.g. 921391. Global across seasons. |

### Returns {#vtb_infobasket_game_staff-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Infobasket GameID of the game (the v2 matches' match_id); global across seasons. |
| `team_number` | Int64 | Side: 1 team A (home), 2 team B (away), 0 neither (officials, the placeholder row). |
| `team_id` | Int64 | Infobasket TeamID (2650 = Lokomotiv Kuban); the v2 teamId. Not a club-record id and not a registration id. |
| `start_id` | Int64 | Line-up slot id within this game; not a person id. |
| `start_type` | Int64 | Line-up row type: 3 coach, 4 official. |
| `role` | String | English role of the person: head coach, assistant coach, referee, commissioner, table official (from start_type and pos_id). |
| `pos_id` | Int64 | Position code of the coach or official (coach 1000 head / 1001 assistant; referees 101-103, commissioner 104, table officials 111-123). |
| `person_id` | Int64 | Infobasket person id (PersonID): one person (player, coach or official) across clubs and seasons. Joins to /Widget/PlayerPage/{personId} (players) and /Widget/CoachPage/{personId} (coaches). 0 on team and placeholder rows. |
| `person_name_en` | String | Name in English, as 'Last First'. |
| `person_name_ru` | String | Name in Russian, as 'Last First'. |
| `country_code_ioc` | String | Country code as the host sends it (ISO alpha-2 in most rows, IOC alpha-3 in some). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_game_staff-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_game_staff-example}

```python
vtb_infobasket_game_staff(game_id=921391)
```

## vtb_infobasket_game_page

Thin header of a finished game (use vtb_infobasket_game for teams, box score and plays).

**Endpoint URL:** `GET https://org.infobasket.su/Widget/GamePage/{game_id}`

**Valid URL:** [https://org.infobasket.su/Widget/GamePage/921391?format=json](https://org.infobasket.su/Widget/GamePage/921391?format=json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Infobasket GameID (game_id in vtb_infobasket_calendar; the v2 matches' match_id), e.g. 921391. Global across seasons. |

### Returns {#vtb_infobasket_game_page-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `comp_name_ru` | String | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `comp_name_en` | String | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `league_name_ru` | String | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `league_name_en` | String | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `league_short_name_ru` | String | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `league_short_name_en` | String | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `gender` | Int64 | Competition gender: 0 on every VTB sample (men). Other values unverified. |
| `comp_id` | Int64 | Always 0 on this thin header; use vtb_infobasket_game for the stage. |
| `league_id` | Int64 | Season-level competition id of the game (50714 = VTB 2025-26); 0 in GamePage. |
| `game_id` | Int64 | Infobasket game id (GameID); equals the v2 API `matchId`. Joins to /Widget/GetOnline/{gameId} and /Widget/GamePage/{gameId}. The scope check compares it with the request. |
| `game_date` | String | Game date as an ISO 8601 string in Moscow time (+03:00), midnight. |
| `game_time` | String | Tip-off as an ISO 8601 string in Moscow time (+03:00). |
| `game_date_time` | String | Tip-off as an ISO 8601 string in Moscow time (+03:00). |
| `game_number` | String | Game number within the competition, e.g. 'RS-002' (RS = regular season); playoff games use the series code. |
| `game_region` | Int64 | Region (city) id of the game. |
| `game_arena` | Int64 | Arena id of the game. Infobasket arena id; equals the v2 API `venueId` (11687 = SK Basket-Hall, Krasnodar). |
| `game_rules` | Int64 | Rules code (1 in the samples; unverified). |
| `game_forfeit` | Int64 | 1 when the game was forfeited, else 0. |
| `game_attendance` | Int64 | Spectators at the game (null when not recorded). |
| `game_status` | Int64 | Game status: 0 planned, 1 completed, 2 live (the site's widget code). Samples are completed games. |
| `game_stats_status` | Int64 | Stats status of the game; 0 in the samples. |
| `game_time_moscow_string` | String | Tip-off time in Moscow time, 'HH:MM'. |
| `region_name_ru` | String | City or region in Russian. |
| `region_name_en` | String | City or region in English. |
| `arena_name_ru` | String | Arena name in Russian. |
| `arena_name_en` | String | Arena name in English. |
| `arena_full_name_ru` | String | Arena full name in Russian. |
| `arena_full_name_en` | String | Arena full name in English. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_game_page-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_game_page-example}

```python
vtb_infobasket_game_page(game_id=921391)
```

## vtb_infobasket_comp_issue

Stage tree of a season (regular season, playoff, series): the stage ids the standings wrappers take.

**Endpoint URL:** `GET https://org.infobasket.su/Widget/CompIssue/{season}`

**Valid URL:** [https://org.infobasket.su/Widget/CompIssue/2026?format=json&tag=vtb](https://org.infobasket.su/Widget/CompIssue/2026?format=json&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_comp_issue-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `season_comp_id` | Int64 | Season-level Infobasket CompID of the season (50714 = 2025-26): the id every season-keyed route takes. |
| `level` | Int64 | Depth in the stage tree: 1 = a stage of the season, 2 = a series or group below it. |
| `comp_id` | Int64 | Stage CompID (the id vtb_infobasket_standings, _cross_table and _playoff take). |
| `parent_comp_id` | Int64 | CompID of the parent stage (0 at the top). |
| `comp_type` | Int64 | Stage type code: 0 season, regular season or group; 10 playoff container; 5 and 7 playoff series; -5 seen in season lists. Meanings inferred from the stage names, unverified. |
| `comp_sort` | Int64 | Sort order among sibling stages. |
| `comp_round` | Int64 | Playoff round as the number of series in it (4 = quarter-finals, 2 = semi-finals, 1 = final); 0 outside the playoff. |
| `comp_short_name_ru` | String | Stage name, Russian. |
| `comp_short_name_en` | String | Stage name, English. |
| `comp_abc_name_ru` | String | Stage abbreviation ('1/4-1' = quarter-final 1), Russian. |
| `comp_abc_name_en` | String | Stage abbreviation ('1/4-1' = quarter-final 1), English. |
| `comp_gender` | Int64 | Competition gender: 0 on every sample (men). |
| `comp_state` | Int64 | Stage state code: 0 or 2 seen (2 on finished stages, but some finished stages show 0); unverified. |
| `is_public` | Boolean | True when the site shows the stage. |
| `note_ru` | String | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `note_en` | String | Unobserved: null in every sample, so its type and meaning are unconfirmed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_comp_issue-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_comp_issue-example}

```python
vtb_infobasket_comp_issue(season=2026)
```

## vtb_infobasket_standings

Standings of one stage (a round-robin stage such as the regular season), one row per team.

**Endpoint URL:** `GET https://org.infobasket.su/Widget/CompTeamResults/{comp_id}`

**Valid URL:** [https://org.infobasket.su/Widget/CompTeamResults/50720?format=json](https://org.infobasket.su/Widget/CompTeamResults/50720?format=json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `comp_id` | `comp_id` |  | `Y` |  | Stage CompID (comp_id in vtb_infobasket_comp_issue), not a season id and not an END year, e.g. 50720 (Regular Season 2025-26) or 54281 (Playoff 2025-26). |

### Returns {#vtb_infobasket_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `comp_id` | Int64 | Stage CompID the request asked for (50720 = the 2025-26 regular season): a stage below the season, not an END year. |
| `comp_team_id` | Int64 | Infobasket competition-team id: one team's registration in one competition stage (a different key space from TeamID). |
| `team_id` | Int64 | Infobasket team id (TeamID): one club's entry in a competition family (Lokomotiv Kuban = 2650). Joins to /Widget/Team*/{teamId} and equals the v2 API `teamId`. |
| `place` | Int64 | Current place in the standings. |
| `start_place` | Int64 | Place before the latest round (unverified). |
| `games` | Int64 | Games played in the stage (top-level copy of standing_game). |
| `won` | Int64 | Games won in the stage (top-level copy of standing_win). |
| `lost` | Int64 | Games lost in the stage (top-level copy of standing_lose). |
| `standing_game` | Int64 | Games played in the stage. |
| `standing_win` | Int64 | Games won in the stage. |
| `standing_draw` | String | Draws; null in basketball. |
| `standing_lose` | Int64 | Games lost in the stage. |
| `standing_forfeit` | Int64 | Games lost by forfeit in the stage. |
| `standing_points` | Int64 | League points (2 per win, 1 per loss). |
| `standing_goal_plus` | Int64 | Points scored in the stage. |
| `standing_goal_minus` | Int64 | Points conceded in the stage. |
| `standing_goal_difference` | Int64 | Point difference (scored minus conceded). |
| `standing_penalty` | Int64 | Points deducted. |
| `standing_is_play` | Boolean | True while the team is still playing in the stage. |
| `victory_percent` | Float64 | Win percentage as a number (83 = 83%). |
| `avg_goal_plus` | Float64 | Average points scored per game. |
| `avg_goal_minus` | Float64 | Average points conceded per game. |
| `team_short_name_ru` | String | The the team's short name in Russian (as the competition names it). |
| `team_short_name_en` | String | The the team's short name in English (as the competition names it). |
| `team_name_ru` | String | The the team's name in Russian (as the competition names it). |
| `team_name_en` | String | The the team's name in English (as the competition names it). |
| `team_region_name_ru` | String | The the team's region or city in Russian (as the competition names it). |
| `team_region_name_en` | String | The the team's region or city in English (as the competition names it). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_standings-example}

```python
vtb_infobasket_standings(comp_id=50720)
```

## vtb_infobasket_cross_table

Head-to-head results grid of a round-robin stage (every game twice, once from each side).

**Endpoint URL:** `GET https://org.infobasket.su/Widget/CrossTable/{comp_id}`

**Valid URL:** [https://org.infobasket.su/Widget/CrossTable/50720?format=json](https://org.infobasket.su/Widget/CrossTable/50720?format=json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `comp_id` | `comp_id` |  | `Y` |  | Stage CompID (comp_id in vtb_infobasket_comp_issue), not a season id and not an END year, e.g. 50720 (Regular Season 2025-26) or 54281 (Playoff 2025-26). |

### Returns {#vtb_infobasket_cross_table-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `comp_id` | Int64 | Stage CompID the request asked for (50720 = the 2025-26 regular season): a stage below the season, not an END year. |
| `game_id` | Int64 | Infobasket game id (GameID); equals the v2 API `matchId`. Joins to /Widget/GetOnline/{gameId} and /Widget/GamePage/{gameId}. |
| `game_status` | Int64 | Game status in the game-record code set: 21 on every completed game sampled (not the calendar's 0/1/2). |
| `score` | String | Final score 'row team:opponent'. Every game appears twice, once from each side. |
| `plus_minus` | Int64 | Point difference from the row team's side. |
| `game_date` | String | Day and month of the game as the host sends them ('04.04.'): there is no year in this route. |
| `team1_id` | Int64 | TeamID of the row team. Infobasket team id (TeamID): one club's entry in a competition family (Lokomotiv Kuban = 2650). Joins to /Widget/Team*/{teamId} and equals the v2 API `teamId`. |
| `team2_id` | Int64 | TeamID of the opponent. Infobasket team id (TeamID): one club's entry in a competition family (Lokomotiv Kuban = 2650). Joins to /Widget/Team*/{teamId} and equals the v2 API `teamId`. |
| `team1_place` | Int64 | Standings place of the row team. |
| `team2_place` | Int64 | Standings place of the opponent. |
| `home_team` | Int64 | Which side was at home: 1 the row team, 2 the opponent. |
| `win_team` | Int64 | Which side won: 1 the row team, 2 the opponent. |
| `score1` | Int64 | Points of team 1, parsed from the 'a:b' score string. |
| `score2` | Int64 | Points of team 2, parsed from the 'a:b' score string. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_cross_table-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_cross_table-example}

```python
vtb_infobasket_cross_table(comp_id=50720)
```

## vtb_infobasket_playoff

Playoff bracket of a playoff stage, one row per series.

**Endpoint URL:** `GET https://org.infobasket.su/Widget/PlayOff/{comp_id}`

**Valid URL:** [https://org.infobasket.su/Widget/PlayOff/54281?format=json](https://org.infobasket.su/Widget/PlayOff/54281?format=json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `comp_id` | `comp_id` |  | `Y` |  | Stage CompID (comp_id in vtb_infobasket_comp_issue), not a season id and not an END year, e.g. 50720 (Regular Season 2025-26) or 54281 (Playoff 2025-26). |

### Returns {#vtb_infobasket_playoff-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `comp_id` | Int64 | Stage CompID the request asked for (50720 = the 2025-26 regular season): a stage below the season, not an END year. |
| `tree` | Int64 | Bracket tree number (1 = the main bracket). |
| `round` | Int64 | Round as the number of series in it (4 = quarter-finals, 2 = semi-finals, 1 = final). |
| `is_current_round` | Boolean | True for the round in progress. |
| `sort` | Int64 | Series order within the round. |
| `round_name_ru` | String | Round name, Russian. |
| `round_name_en` | String | Round name, English. |
| `score1` | Int64 | Series wins of side 1. |
| `score2` | Int64 | Series wins of side 2. |
| `is_final` | Boolean | True for a final (decided) result. |
| `winner` | Int64 | Series winner: 1 side 1, 2 side 2. |
| `team1_id` | Int64 | Infobasket TeamID of team 1. |
| `team1_short_name_ru` | String | The team 1 of the series's short name in Russian (as the competition names it). |
| `team1_short_name_en` | String | The team 1 of the series's short name in English (as the competition names it). |
| `team1_name_ru` | String | The team 1 of the series's name in Russian (as the competition names it). |
| `team1_name_en` | String | The team 1 of the series's name in English (as the competition names it). |
| `team1_region_name_ru` | String | The team 1 of the series's region or city in Russian (as the competition names it). |
| `team1_region_name_en` | String | The team 1 of the series's region or city in English (as the competition names it). |
| `team2_id` | Int64 | Infobasket TeamID of team 2. |
| `team2_short_name_ru` | String | The team 2 of the series's short name in Russian (as the competition names it). |
| `team2_short_name_en` | String | The team 2 of the series's short name in English (as the competition names it). |
| `team2_name_ru` | String | The team 2 of the series's name in Russian (as the competition names it). |
| `team2_name_en` | String | The team 2 of the series's name in English (as the competition names it). |
| `team2_region_name_ru` | String | The team 2 of the series's region or city in Russian (as the competition names it). |
| `team2_region_name_en` | String | The team 2 of the series's region or city in English (as the competition names it). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_playoff-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_playoff-example}

```python
vtb_infobasket_playoff(comp_id=54281)
```

## vtb_infobasket_playoff_games

The games of every series of a playoff stage.

**Endpoint URL:** `GET https://org.infobasket.su/Widget/PlayOff/{comp_id}`

**Valid URL:** [https://org.infobasket.su/Widget/PlayOff/54281?format=json](https://org.infobasket.su/Widget/PlayOff/54281?format=json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `comp_id` | `comp_id` |  | `Y` |  | Stage CompID (comp_id in vtb_infobasket_comp_issue), not a season id and not an END year, e.g. 50720 (Regular Season 2025-26) or 54281 (Playoff 2025-26). |

### Returns {#vtb_infobasket_playoff_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `comp_id` | Int64 | Stage CompID the request asked for (50720 = the 2025-26 regular season): a stage below the season, not an END year. |
| `tree` | Int64 | Bracket tree number (1 = the main bracket). |
| `round` | Int64 | Round as the number of series in it (4 = quarter-finals, 2 = semi-finals, 1 = final). |
| `sort` | Int64 | Series order within the round. |
| `game_id` | Int64 | Infobasket game id (GameID); equals the v2 API `matchId`. Joins to /Widget/GetOnline/{gameId} and /Widget/GamePage/{gameId}. |
| `game_status` | Int64 | Game status in the game-record code set: 21 on every completed game sampled (not the calendar's 0/1/2). |
| `score` | String | Final score 'side1:side2'. |
| `plus_minus` | Int64 | Point difference from side 1's view. |
| `game_date` | String | Game date as an ISO 8601 date (YYYY-MM-DD); the wire sends DD.MM.YYYY. |
| `team1_id` | Int64 | TeamID of side 1. |
| `team2_id` | Int64 | TeamID of side 2. |
| `team1_place` | Int64 | Seed of side 1. |
| `team2_place` | Int64 | Seed of side 2. |
| `home_team` | Int64 | Home side of that game (1 or 2). |
| `win_team` | Int64 | Winning side of that game (1 or 2). |
| `score1` | Int64 | Series wins of side 1. |
| `score2` | Int64 | Series wins of side 2. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_playoff_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_playoff_games-example}

```python
vtb_infobasket_playoff_games(comp_id=54281)
```

## vtb_infobasket_coach

Coach profile: names, height, weight (no birth data).

**Endpoint URL:** `GET https://org.infobasket.su/Widget/CoachPage/{person_id}`

**Valid URL:** [https://org.infobasket.su/Widget/CoachPage/131047?format=json](https://org.infobasket.su/Widget/CoachPage/131047?format=json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_id` | `person_id` |  | `Y` |  | Infobasket PersonID (person_id in a box score or roster): one person across clubs and seasons, e.g. 423011. |

### Returns {#vtb_infobasket_coach-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `person_id` | Int64 | Infobasket person id (PersonID): one person (player, coach or official) across clubs and seasons. Joins to /Widget/PlayerPage/{personId} (players) and /Widget/CoachPage/{personId} (coaches). The scope check compares it with the request. |
| `person_last_name_ru` | String | Last name, Russian. |
| `person_first_name_ru` | String | First name, Russian. |
| `person_last_name_en` | String | Last name, English. |
| `person_first_name_en` | String | First name, English. |
| `person_gender` | Int64 | Person gender: 1 on every sampled (male) person. |
| `person_height` | Int64 | Height in centimetres. |
| `person_weight` | Int64 | Weight in kilograms. |
| `person_status` | Int64 | Person record status (1 active, 0 otherwise); meaning unverified. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_coach-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_coach-example}

```python
vtb_infobasket_coach(person_id=131047)
```

## vtb_infobasket_roster

A club's player roster in one season.

**Endpoint URL:** `GET https://org.infobasket.su/Widget/TeamRoster/{team_id}`

**Valid URL:** [https://org.infobasket.su/Widget/TeamRoster/2650?format=json&season=2026&lang=en&tag=vtb](https://org.infobasket.su/Widget/TeamRoster/2650?format=json&season=2026&lang=en&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Infobasket TeamID (team_id in the calendar, a box score or the v2 matches), e.g. 2650 (Lokomotiv Kuban). |
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). Required. |
| `lang` | `lang` |  |  | `Y` | Label language (en or ru). Most bodies carry both languages whatever is asked; the leader categories' labels follow it. |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Infobasket TeamID (2650 = Lokomotiv Kuban); the v2 teamId. Not a club-record id and not a registration id. |
| `season` | Int64 | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `person_id` | Int64 | Infobasket person id (PersonID): one person (player, coach or official) across clubs and seasons. Joins to /Widget/PlayerPage/{personId} (players) and /Widget/CoachPage/{personId} (coaches). |
| `player_number` | Int64 | Shirt number as an integer (-1 when none). |
| `display_number` | String | Shirt number as displayed ('0' and '00' differ). |
| `is_captain` | Boolean | 1 for the team captain, else 0. |
| `birth_date` | String | Birth date (ISO 8601 date) of a player: public bio, kept; coaches' and officials' birth data is never output. |
| `pos_id` | Int64 | Position code: players 1 point guard, 2 shooting guard, 3 small forward, 4 power forward, 5 centre; coaches 1000 head coach, 1001 assistant; officials 101-103 referees, 104 commissioner, 111-123 table officials (exact roles unverified). |
| `position` | String | Playing position: a record (PlayerPage) or its English name (TeamRoster). |
| `nationality_code` | String | Nationality, ISO 3166 alpha-2. |
| `country_code` | String | Nationality, ISO 3166 alpha-2. |
| `country_code_ioc` | String | Nationality code: ISO 3166 alpha-2 in most rows ('RU', 'US'), IOC alpha-3 in some ('BLR'). |
| `country_name` | String | Nationality, English country name. |
| `rank` | Int64 | Unobserved: the roster sends an empty string for every player and coach (null here); the field belongs to the leader lists (best_players), so its meaning on the roster is unconfirmed. |
| `height` | Int64 | Height in centimetres. |
| `weight` | Int64 | Weight in kilograms. |
| `is_active` | Boolean | True while the person is on the current roster. |
| `active_status` | String | Roster status text ('Accepted'), empty otherwise. |
| `person_last_name_ru` | String | Last name, Russian. |
| `person_last_name_en` | String | Last name, English. |
| `person_first_name_ru` | String | First name, Russian. |
| `person_first_name_en` | String | First name, English. |
| `person_full_name_ru` | String | Full name, 'Last First', Russian. |
| `person_full_name_en` | String | Full name, 'Last First', English. |
| `person_gender` | Int64 | Person gender: 1 on every sampled (male) person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_roster-example}

```python
vtb_infobasket_roster(season=2026, team_id=2650)
```

## vtb_infobasket_roster_coaches

A club's coaches in one season (no birth data).

**Endpoint URL:** `GET https://org.infobasket.su/Widget/TeamRoster/{team_id}`

**Valid URL:** [https://org.infobasket.su/Widget/TeamRoster/2650?format=json&season=2026&lang=en&tag=vtb](https://org.infobasket.su/Widget/TeamRoster/2650?format=json&season=2026&lang=en&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Infobasket TeamID (team_id in the calendar, a box score or the v2 matches), e.g. 2650 (Lokomotiv Kuban). |
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). Required. |
| `lang` | `lang` |  |  | `Y` | Label language (en or ru). Most bodies carry both languages whatever is asked; the leader categories' labels follow it. |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_roster_coaches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Infobasket TeamID (2650 = Lokomotiv Kuban); the v2 teamId. Not a club-record id and not a registration id. |
| `season` | Int64 | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `person_id` | Int64 | Infobasket person id (PersonID): one person (player, coach or official) across clubs and seasons. Joins to /Widget/PlayerPage/{personId} (players) and /Widget/CoachPage/{personId} (coaches). |
| `coach_number` | Int64 | Coach order on the team sheet (1 = head coach). |
| `display_number` | String | Shirt number as displayed ('0' and '00' differ). |
| `post_id` | Int64 | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `post` | String | Coaching post ('Head Coach', 'Assistant Coach'). |
| `country_code_ioc` | String | Nationality code: ISO 3166 alpha-2 in most rows ('RU', 'US'), IOC alpha-3 in some ('BLR'). |
| `country_name` | String | Nationality, English country name. |
| `rank` | Int64 | Unobserved: the roster sends an empty string for every player and coach (null here); the field belongs to the leader lists (best_players), so its meaning on the roster is unconfirmed. |
| `is_active` | Boolean | True while the person is on the current roster. |
| `active_status` | String | Roster status text ('Accepted'), empty otherwise. |
| `person_last_name_ru` | String | Last name, Russian. |
| `person_last_name_en` | String | Last name, English. |
| `person_first_name_ru` | String | First name, Russian. |
| `person_first_name_en` | String | First name, English. |
| `person_full_name_ru` | String | Full name, 'Last First', Russian. |
| `person_full_name_en` | String | Full name, 'Last First', English. |
| `person_gender` | Int64 | Person gender: 1 on every sampled (male) person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_roster_coaches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_roster_coaches-example}

```python
vtb_infobasket_roster_coaches(season=2026, team_id=2650)
```
