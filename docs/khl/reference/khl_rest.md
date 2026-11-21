# KHL — KHL site REST (www.khl.ru/rest)

> KHL — KHL site REST (www.khl.ru/rest) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.hockey.khl` — 14 endpoints.

> **Note:** Owner flag: the KHL and its webcaster platform are run by Russian operators, some clubs by sanctioned state companies. Fetching on your own machine is fine; any published data release, hosted mirror or redistribution needs owner (sanctions / legal) sign-off first.

> **Note:** The documented curl request is schematic: the runtime primes a session (adds the form field `sessid`), maps `season` / `stage` to the form field `values[tournament]` (the tournament id) and sends the X-Requested-With, Origin and Referer headers.

## khl_schedule

Season schedule and results of a tournament, one row per game (All-Star games dropped).

**Endpoint URL:** `POST https://www.khl.ru/rest/calendar/list/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/calendar/list/' -d 'season=2026&stage=regular'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `season` | `season` | body |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). None = the site's current season. |
| `stage` | `stage` | body |  |  | `Y` | Which stage of the season: regular or playoff. |
| `values[tournament]` | `tournament_id` | body |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |

### Returns {#khl_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | khl.ru game id; equals webcaster event khl_id / match_id |
| `tournament_id` | Int64 | khl.ru tournament (stage) id, tnId: 1369 = 2025-26 regular season, 1370 its playoff; equals webcaster stage khl_id |
| `date` | String | Game date YYYY-MM-DD (Moscow time) |
| `time` | String | Start time hh:mm (Moscow time); not a game clock |
| `home_club_id` | Int64 | Home CLUB id (stable across seasons; joins webcaster team khl_id) |
| `home_name` | String | Home team name (Russian on www.khl.ru) |
| `away_club_id` | Int64 | Visitor CLUB id (stable across seasons; joins webcaster team khl_id) |
| `away_name` | String | Visitor team name (Russian on www.khl.ru) |
| `home_score` | Int64 | Home goals (final, shootout winner counted) |
| `away_score` | Int64 | Visitor goals (final, shootout winner counted) |
| `decision` | String | OT or SO when decided in overtime / a shootout; null in regulation |
| `period_scores` | String | Per-period home:away scores, comma-joined |
| `winner_club_id` | Int64 | Club id of the winner |
| `approved` | Int64 | 1 = result confirmed by the league |
| `arena_id` | Int64 | khl.ru arena id |
| `arena_name` | String | Arena (city) name, from data.ARENAS |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Raises {#khl_schedule-raises}

| Exception | Description |
|---|---|
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Khl.ru answered an error envelope (status != success), a 307 again after re-priming the session, or a page for another game / player / tournament than the one requested. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Khl.ru answered no rows, or the season has no such stage. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_schedule-example}

```python
khl_schedule(season=2026)
```

## khl_roster

Club roster (current season only): one row per player.

**Endpoint URL:** `POST https://www.khl.ru/rest/clubs/team/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/clubs/team/' -d 'page=team&values%5Bclub_id%5D=7'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `values[club_id]` | `club_id` | body |  | `Y` |  | Required. khl.ru CLUB id (7 = Spartak; schedule home_club_id), not a team-season id. |

### Returns {#khl_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | String | List the row comes from (position group / roster section) |
| `player_id` | Int64 | khl.ru player id; equals webcaster player khl_id |
| `photo` | String | Player photo URL on khl.ru (protocol-relative) |
| `country` | String | Citizenship (Russian) |
| `lastname` | String | Last name in Russian |
| `firstname` | String | First name in Russian |
| `jn` | Int64 | Jersey number (not an id; 0 on a team event) |
| `age` | Int64 | Player age in years |
| `favorite` | Boolean | Front-end favourite flag of the signed-in user (always False here) |
| `link` | String | Relative khl.ru page path |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Raises {#khl_roster-raises}

| Exception | Description |
|---|---|
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Khl.ru answered an error envelope (status != success), a 307 again after re-priming the session, or a page for another game / player / tournament than the one requested. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Khl.ru answered no rows, or the season has no such stage. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_roster-example}

```python
khl_roster(club_id=7)
```

## khl_stat_filters

Stats filter options: seasons, tournaments, position groups and clubs (one row per option).

**Endpoint URL:** `POST https://www.khl.ru/rest/filters/statplayers/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/filters/statplayers/' -d 'season=2026&stage=regular'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `season` | `season` | body |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). None = the site's current season. |
| `stage` | `stage` | body |  |  | `Y` | Which stage of the season: regular or playoff. |
| `values[tournament]` | `tournament_id` | body |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |

### Returns {#khl_stat_filters-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `filter` | String | Filter code: season, tournament, amplua_full or clubs_full |
| `filter_name` | String | Filter label as the site prints it |
| `value` | String | Option code to send for the filter (a tournament id, a season code such as s19, a club slug) |
| `name` | String | Option label (Russian) |
| `def` | Int64 | Default tournament id (tnId) of a season option |
| `active` | Int64 | 1 when the option is selectable |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Raises {#khl_stat_filters-raises}

| Exception | Description |
|---|---|
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Khl.ru answered an error envelope (status != success), a 307 again after re-priming the session, or a page for another game / player / tournament than the one requested. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Khl.ru answered no rows, or the season has no such stage. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_stat_filters-example}

```python
khl_stat_filters(season=2026)
```

## khl_game_header

Game header: status, teams, score, arena (one row).

**Endpoint URL:** `POST https://www.khl.ru/rest/game/header/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/game/header/' -d 'page=resume&values%5Bgameid%5D=898236&season=2026&stage=regular'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `values[gameid]` | `game_id` | body |  | `Y` |  | Required. khl.ru game id (the schedule's game_id; equals webcaster event khl_id). |
| `season` | `season` | body |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). The game's season; required unless tournament_id is given. |
| `stage` | `stage` | body |  |  | `Y` | Which stage of the season: regular or playoff. |
| `values[tournament]` | `tournament_id` | body |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |

### Returns {#khl_game_header-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | khl.ru game id; equals webcaster event khl_id / match_id |
| `tournament_id` | Int64 | khl.ru tournament (stage) id, tnId: 1369 = 2025-26 regular season, 1370 its playoff; equals webcaster stage khl_id |
| `home_club_id` | Int64 | Home CLUB id (stable across seasons; joins webcaster team khl_id) |
| `home_team_season_id` | Int64 | Home team-season id (one per tournament; joins nothing in the webcaster API) |
| `away_club_id` | Int64 | Visitor CLUB id (stable across seasons; joins webcaster team khl_id) |
| `away_team_season_id` | Int64 | Visitor team-season id (one per tournament; joins nothing in the webcaster API) |
| `arena_id` | Int64 | khl.ru arena id |
| `home_score` | Int64 | Home goals (final, shootout winner counted) |
| `away_score` | Int64 | Visitor goals (final, shootout winner counted) |
| `round` | Int64 | Playoff round number |
| `roundname` | String | Round label in Russian ('Неделя 29' = Week 29) |
| `roundname_en` | String | Round label in English |
| `date` | String | Game date YYYY-MM-DD (Moscow time) |
| `dayofweek` | Int64 | Day of week, 1 = Sunday (6 = Friday 2026-03-20) |
| `time` | String | Start time hh:mm:ss (Moscow time); not a game clock |
| `changes` | Int64 | Count of changes to the game record (0 in the captured bodies; unverified) |
| `number` | Int64 | Game number within the tournament |
| `home_name_l3` | String | Home club 3-letter code in Russian |
| `home_name_en` | String | Home club name in English |
| `home_name_l3_en` | String | Home club 3-letter code in English |
| `home_city_en` | String | Home club city in English |
| `visitor_name_l3` | String | Visiting club 3-letter code in Russian |
| `visitor_name_en` | String | Visiting club name in English |
| `visitor_name_l3_en` | String | Visiting club 3-letter code in English |
| `visitor_city_en` | String | Visiting club city in English |
| `arena` | String | Arena name in Russian |
| `arena_en` | String | Arena name in English |
| `arena_city` | String | Arena city in Russian |
| `arena_city_en` | String | Arena city in English |
| `msktime_sft` | Int64 | Offset of local time from Moscow time, hours (unverified) |
| `gmttime_sft` | Int64 | Offset of Moscow time from GMT, hours (unverified) |
| `temp` | String |  |
| `score` | String | Final score 'home:visitor' |
| `ots` | String | How the game ended: empty = regulation, OT = overtime, SO = shootout |
| `attendance` | Int64 | Attendance (numeric string) |
| `approved` | Int64 | 1 = result confirmed by the league |
| `format_date_day` | String | Game date YYYY-MM-DD (Moscow time), as the site formats it |
| `time_format` | String | Start time hh:mm (Moscow time) |
| `full_date` | Int64 | Start time, unix seconds (UTC) |
| `date_type` | String | Front-end date bucket of the game relative to today ('last' = already played; other values unverified) |
| `type` | String | Game state/type code (sogame = finished in a shootout) |
| `winner` | String | Winning side: A = home, B = visitor |
| `visible_date` | String | Short display date in Russian ('20 мар' = 20 March) |
| `visible_date_full` | String | Display date in Russian without the year ('20 марта') |
| `visible_date_full2` | String | Display date in Russian with the year ('20 марта 2026') |
| `visible_date_day` | String | Abbreviated weekday in Russian ('пт' = Friday) |
| `win` | Int64 | True when this club won |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Raises {#khl_game_header-raises}

| Exception | Description |
|---|---|
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Khl.ru answered an error envelope (status != success), a 307 again after re-priming the session, or a page for another game / player / tournament than the one requested. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Khl.ru answered no rows, or the season has no such stage. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_game_header-example}

```python
khl_game_header(game_id=898236, season=2026)
```

## khl_game_protocol

Game protocol: one row per goal with scorer, assistants, strength and the jerseys on the ice.

**Endpoint URL:** `POST https://www.khl.ru/rest/game/protocol/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/game/protocol/' -d 'values%5Bgameid%5D=898236&season=2026&stage=regular'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `values[gameid]` | `game_id` | body |  | `Y` |  | Required. khl.ru game id (the schedule's game_id; equals webcaster event khl_id). |
| `season` | `season` | body |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). The game's season; required unless tournament_id is given. |
| `stage` | `stage` | body |  |  | `Y` | Which stage of the season: regular or playoff. |
| `values[tournament]` | `tournament_id` | body |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |

### Returns {#khl_game_protocol-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | khl.ru game id; equals webcaster event khl_id / match_id |
| `tournament_id` | Int64 | khl.ru tournament (stage) id, tnId: 1369 = 2025-26 regular season, 1370 its playoff; equals webcaster stage khl_id |
| `period` | Int64 | Period number (box score: 0 = the whole game; 4+ = overtime) |
| `goal_number` | Int64 | Goal number in the game |
| `time` | String | Game clock mm:ss (game-elapsed); period marker rows keep their wall-clock HH:MM |
| `team_side` | String | A = home, B = visitor |
| `score_a` | Int64 | Home goals after the goal |
| `score_b` | Int64 | Visitor goals after the goal |
| `strength` | String | Strength code as the site prints it (Russian: РАВ even, БОЛ power play, МЕН short-handed) |
| `strength_text` | String | Strength label as the site prints it |
| `scorer_player_id` | Int64 | Scorer's khl.ru player id |
| `scorer_name` | String | Scorer's name with the season count in brackets |
| `scorer_jn` | Int64 | Scorer's jersey |
| `assist_1_player_id` | Int64 | First assistant's khl.ru player id |
| `assist_1_name` | String | First assistant's name with the season count in brackets |
| `assist_1_jn` | Int64 | First assistant's jersey |
| `assist_2_player_id` | Int64 | Second assistant's khl.ru player id |
| `assist_2_name` | String | Second assistant's name with the season count in brackets |
| `assist_2_jn` | Int64 | Second assistant's jersey |
| `on_ice_a` | String | Home jerseys on the ice, comma-separated |
| `on_ice_b` | String | Visitor jerseys on the ice, comma-separated |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Raises {#khl_game_protocol-raises}

| Exception | Description |
|---|---|
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Khl.ru answered an error envelope (status != success), a 307 again after re-priming the session, or a page for another game / player / tournament than the one requested. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Khl.ru answered no rows, or the season has no such stage. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_game_protocol-example}

```python
khl_game_protocol(game_id=898236, season=2026)
```

## khl_game_info

Game info: teams, score, arena, officials, starting goalies (one row).

**Endpoint URL:** `POST https://www.khl.ru/rest/game/text/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/game/text/' -d 'values%5Bgameid%5D=898236&season=2026&stage=regular'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `values[gameid]` | `game_id` | body |  | `Y` |  | Required. khl.ru game id (the schedule's game_id; equals webcaster event khl_id). |
| `season` | `season` | body |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). The game's season; required unless tournament_id is given. |
| `stage` | `stage` | body |  |  | `Y` | Which stage of the season: regular or playoff. |
| `values[tournament]` | `tournament_id` | body |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |

### Returns {#khl_game_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | khl.ru game id; equals webcaster event khl_id / match_id |
| `tournament_id` | Int64 | khl.ru tournament (stage) id, tnId: 1369 = 2025-26 regular season, 1370 its playoff; equals webcaster stage khl_id |
| `home_club_id` | Int64 | Home CLUB id (stable across seasons; joins webcaster team khl_id) |
| `home_team_season_id` | Int64 | Home team-season id (one per tournament; joins nothing in the webcaster API) |
| `away_club_id` | Int64 | Visitor CLUB id (stable across seasons; joins webcaster team khl_id) |
| `away_team_season_id` | Int64 | Visitor team-season id (one per tournament; joins nothing in the webcaster API) |
| `arena_id` | Int64 | khl.ru arena id |
| `home_score` | Int64 | Home goals (final, shootout winner counted) |
| `away_score` | Int64 | Visitor goals (final, shootout winner counted) |
| `round` | Int64 | Playoff round number |
| `roundname` | String | Round label in Russian ('Неделя 29' = Week 29) |
| `roundname_en` | String | Round label in English |
| `date` | String | Game date YYYY-MM-DD (Moscow time) |
| `dayofweek` | Int64 | Day of week, 1 = Sunday (6 = Friday 2026-03-20) |
| `time` | String | Start time hh:mm:ss (Moscow time); not a game clock |
| `changes` | Int64 | Count of changes to the game record (0 in the captured bodies; unverified) |
| `number` | Int64 | Game number within the tournament |
| `home_name_l3` | String | Home club 3-letter code in Russian |
| `home_name_en` | String | Home club name in English |
| `home_name_l3_en` | String | Home club 3-letter code in English |
| `home_city_en` | String | Home club city in English |
| `visitor_name_l3` | String | Visiting club 3-letter code in Russian |
| `visitor_name_en` | String | Visiting club name in English |
| `visitor_name_l3_en` | String | Visiting club 3-letter code in English |
| `visitor_city_en` | String | Visiting club city in English |
| `arena` | String | Arena name in Russian |
| `arena_en` | String | Arena name in English |
| `arena_city` | String | Arena city in Russian |
| `arena_city_en` | String | Arena city in English |
| `msktime_sft` | Int64 | Offset of local time from Moscow time, hours (unverified) |
| `gmttime_sft` | Int64 | Offset of Moscow time from GMT, hours (unverified) |
| `temp` | String |  |
| `score` | String | Final score 'home:visitor' |
| `ots` | String | How the game ended: empty = regulation, OT = overtime, SO = shootout |
| `sc_p1` | String | 1st-period score 'home:visitor' |
| `sc_p2` | String | 2nd-period score 'home:visitor' |
| `sc_p3` | String | 3rd-period score 'home:visitor' |
| `sc_p4` | String | Overtime or shootout score 'home:visitor' |
| `attendance` | Int64 | Attendance (numeric string) |
| `approved` | Int64 | 1 = result confirmed by the league |
| `format_date_day` | String | Game date YYYY-MM-DD (Moscow time), as the site formats it |
| `time_format` | String | Start time hh:mm (Moscow time) |
| `full_date` | Int64 | Start time, unix seconds (UTC) |
| `date_type` | String | Front-end date bucket of the game relative to today ('last' = already played; other values unverified) |
| `type` | String | Game state/type code (sogame = finished in a shootout) |
| `winner` | String | Winning side: A = home, B = visitor |
| `visible_date` | String | Short display date in Russian ('20 мар' = 20 March) |
| `visible_date_full` | String | Display date in Russian without the year ('20 марта') |
| `visible_date_full2` | String | Display date in Russian with the year ('20 марта 2026') |
| `visible_date_day` | String | Abbreviated weekday in Russian ('пт' = Friday) |
| `statusvalue` | String | Game status in Russian (матч завершен = match finished) |
| `pernum` | Int64 | Number of regulation periods played |
| `numlines` | Int64 | Number of lines on the line-up card |
| `gk_a_jn` | Int64 | Home starting goalie's JERSEY number (gameItem.gkA), not an id |
| `gk_b_jn` | Int64 | Visitor starting goalie's JERSEY number (gameItem.gkB), not an id |
| `is_events_have_multi_ovt` | Boolean | True when the game has more than one overtime period (playoff) |
| `idmainref1` | Int64 | Referee 1: khl.ru referee id |
| `idmainref2` | Int64 | Referee 2: khl.ru referee id |
| `idlineref1` | Int64 | Linesman 1: khl.ru referee id |
| `idlineref2` | Int64 | Linesman 2: khl.ru referee id |
| `jnmainref1` | String | Referee 1 sweater number |
| `jnmainref2` | String | Referee 2 sweater number |
| `jnlineref1` | String | Linesman 1 sweater number |
| `jnlineref2` | String | Linesman 2 sweater number |
| `refm1` | String | Referee 1, name in Russian |
| `refm2` | String | Referee 2, name in Russian |
| `refl1` | String | Linesman 1, name in Russian |
| `refl2` | String | Linesman 2, name in Russian |
| `status` | Int64 | Game status code (90 = finished; other codes UNVERIFIED) |
| `gk_a_player_id` | Int64 | khl.ru player id of the home starting goalie (gk_a_jn resolved through players) |
| `gk_b_player_id` | Int64 | khl.ru player id of the visitor starting goalie (gk_b_jn resolved through players) |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Raises {#khl_game_info-raises}

| Exception | Description |
|---|---|
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Khl.ru answered an error envelope (status != success), a 307 again after re-priming the session, or a page for another game / player / tournament than the one requested. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Khl.ru answered no rows, or the season has no such stage. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_game_info-example}

```python
khl_game_info(game_id=898236, season=2026)
```

## khl_player_box

Skater box score by period: one row per period, side and jersey, with the player id.

**Endpoint URL:** `POST https://www.khl.ru/rest/game/text/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/game/text/' -d 'values%5Bgameid%5D=898236&season=2026&stage=regular'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `values[gameid]` | `game_id` | body |  | `Y` |  | Required. khl.ru game id (the schedule's game_id; equals webcaster event khl_id). |
| `season` | `season` | body |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). The game's season; required unless tournament_id is given. |
| `stage` | `stage` | body |  |  | `Y` | Which stage of the season: regular or playoff. |
| `values[tournament]` | `tournament_id` | body |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |

### Returns {#khl_player_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | khl.ru game id; equals webcaster event khl_id / match_id |
| `tournament_id` | Int64 | khl.ru tournament (stage) id, tnId: 1369 = 2025-26 regular season, 1370 its playoff; equals webcaster stage khl_id |
| `period` | Int64 | Period number (box score: 0 = the whole game; 4+ = overtime) |
| `team_side` | String | A = home, B = visitor |
| `jn` | Int64 | Jersey number (not an id; 0 on a team event) |
| `player_id` | Int64 | khl.ru player id; equals webcaster player khl_id |
| `player_name` | String | Player name as the site prints it (Russian on www.khl.ru) |
| `position` | String | Position code (Russian letter on www.khl.ru) |
| `g` | Int64 | Goals scored in the period block |
| `a` | Int64 | Home roster keyed by jersey number |
| `p` | String | Points (empty string when none) |
| `shots` | Int64 | Shots on goal in the period block |
| `pim` | Int64 | Penalty minutes |
| `fo` | Int64 | Faceoffs taken in the period block |
| `fow` | Int64 | Faceoffs won in the period block |
| `fow_pct` | Float64 | Stat code the bar is computed from |
| `bls` | Int64 | Shots blocked in the period block |
| `hits` | Int64 | Hits (body checks) |
| `fa` | Int64 | Takeaways (the site labels this column 'Отборы', takeaways) |
| `p_intc` | Int64 | Pass interceptions |
| `sft` | String | Time on ice 'mm:ss(shifts)' |
| `sfte` | String | Even-strength time on ice 'mm:ss(shifts)' |
| `sftpp` | String | Power-play time on ice 'mm:ss(shifts)' |
| `sftsh` | String | Short-handed time on ice 'mm:ss(shifts)' |
| `sften` | String | Empty-net time on ice 'mm:ss(shifts)' |
| `tiams` | String | Time in the attacking zone (mm:ss) |
| `ts` | Float64 | Top speed, km/h (tracking; present by 2019/20) |
| `dt` | Float64 | Distance skated, km (tracking) |
| `as` | Float64 | Average speed, km/h (tracking) |
| `pcd` | Float64 | Puck-control duration (tracking; unit unverified) |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Raises {#khl_player_box-raises}

| Exception | Description |
|---|---|
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Khl.ru answered an error envelope (status != success), a 307 again after re-priming the session, or a page for another game / player / tournament than the one requested. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Khl.ru answered no rows, or the season has no such stage. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_player_box-example}

```python
khl_player_box(game_id=898236, season=2026)
```

## khl_pbp

Play-by-play: shots, faceoffs, hits, blocks, giveaways, takeaways, penalties and goals (2008-09 on).

**Endpoint URL:** `POST https://www.khl.ru/rest/game/text/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/game/text/' -d 'values%5Bgameid%5D=898236&season=2026&stage=regular'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `values[gameid]` | `game_id` | body |  | `Y` |  | Required. khl.ru game id (the schedule's game_id; equals webcaster event khl_id). |
| `season` | `season` | body |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). The game's season; required unless tournament_id is given. |
| `stage` | `stage` | body |  |  | `Y` | Which stage of the season: regular or playoff. |
| `values[tournament]` | `tournament_id` | body |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |

### Returns {#khl_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | khl.ru game id; equals webcaster event khl_id / match_id |
| `tournament_id` | Int64 | khl.ru tournament (stage) id, tnId: 1369 = 2025-26 regular season, 1370 its playoff; equals webcaster stage khl_id |
| `period` | Int64 | Period number (box score: 0 = the whole game; 4+ = overtime) |
| `event_index` | Int64 | 0-based order of the event in the game, oldest first |
| `event_type` | String | period (start / end marker), faceoff, shot, block, action, penalty or goal |
| `time` | String | Game clock mm:ss (game-elapsed); period marker rows keep their wall-clock HH:MM |
| `time_seconds` | Int64 | Game-elapsed seconds (null on period marker rows) |
| `team_side` | String | A = home, B = visitor |
| `jn` | Int64 | Jersey number (not an id; 0 on a team event) |
| `player_id` | Int64 | khl.ru player id; equals webcaster player khl_id |
| `result` | String | Event result code (shots: goal / saved / miss; actions: hit, gva = giveaway, tka = takeaway, intc = interception) |
| `result_text` | String | Event result label |
| `penalty_minutes` | Int64 | Penalty minutes (penalty events) |
| `score_a` | Int64 | Home goals after the goal |
| `score_b` | Int64 | Visitor goals after the goal |
| `won_side` | String | Faceoff winner's side (A / B) |
| `won_jn` | Int64 | Faceoff winner's jersey |
| `won_player_id` | Int64 | Faceoff winner's khl.ru player id |
| `lost_side` | String | Faceoff loser's side (A / B) |
| `lost_jn` | Int64 | Faceoff loser's jersey |
| `lost_player_id` | Int64 | Faceoff loser's khl.ru player id |
| `won_zone` | String | Zone of the faceoff from the winner's view: offensive, neutral or defensive |
| `shooter_side` | String | Blocked shooter's side (A / B) |
| `shooter_jn` | Int64 | Blocked shooter's jersey |
| `shooter_player_id` | Int64 | Blocked shooter's khl.ru player id |
| `period_action` | String | Period marker: 0 = start, 1 = end (inferred) |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Raises {#khl_pbp-raises}

| Exception | Description |
|---|---|
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Khl.ru answered an error envelope (status != success), a 307 again after re-priming the session, or a page for another game / player / tournament than the one requested. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Khl.ru answered no rows, or the season has no such stage. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_pbp-example}

```python
khl_pbp(game_id=898236, season=2026)
```

## khl_shots

Shot map: one row per shot with its x / y on the 1000 x 500 rink grid (2024-25 on).

**Endpoint URL:** `POST https://www.khl.ru/rest/game/text/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/game/text/' -d 'values%5Bgameid%5D=898236&season=2026&stage=regular'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `values[gameid]` | `game_id` | body |  | `Y` |  | Required. khl.ru game id (the schedule's game_id; equals webcaster event khl_id). |
| `season` | `season` | body |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). The game's season; required unless tournament_id is given. |
| `stage` | `stage` | body |  |  | `Y` | Which stage of the season: regular or playoff. |
| `values[tournament]` | `tournament_id` | body |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |

### Returns {#khl_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | khl.ru game id; equals webcaster event khl_id / match_id |
| `tournament_id` | Int64 | khl.ru tournament (stage) id, tnId: 1369 = 2025-26 regular season, 1370 its playoff; equals webcaster stage khl_id |
| `period` | Int64 | Period number (box score: 0 = the whole game; 4+ = overtime) |
| `time` | String | Game clock mm:ss (game-elapsed); period marker rows keep their wall-clock HH:MM |
| `time_seconds` | Int64 | Game-elapsed seconds (null on period marker rows) |
| `team_side` | String | A = home, B = visitor |
| `jn` | Int64 | Jersey number (not an id; 0 on a team event) |
| `player_id` | Int64 | khl.ru player id; equals webcaster player khl_id |
| `shot_type` | Int64 | 1 = goal, 2 = saved (inferred upstream; unverified) |
| `x` | Int64 | Shot x on the site's 1000 x 500 rink grid, origin top-left (indent_left = x/10 %); goal lines near x = 55 and 945. Team A (home) attacks +x and team B -x in every period, overtime included |
| `y` | Int64 | Shot y on the site's 1000 x 500 grid, origin top-left, increasing downward (indent_top = y/5 %); the grid is stretched in y, about 1.18x against a 200 x 85 ft rink |
| `indent_left` | Float64 | Shot position as percent from the left edge |
| `indent_top` | Float64 | Shot position as percent from the top edge |
| `indent_right` | Float64 | Shot position as percent from the right edge |
| `indent_bottom` | Float64 | Shot position as percent from the bottom edge |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Raises {#khl_shots-raises}

| Exception | Description |
|---|---|
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Khl.ru answered an error envelope (status != success), a 307 again after re-priming the session, or a page for another game / player / tournament than the one requested. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Khl.ru answered no rows, or the season has no such stage. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_shots-example}

```python
khl_shots(game_id=898236, season=2026)
```

## khl_player_profile

Player profile: bio, contract, club (one row).

**Endpoint URL:** `POST https://www.khl.ru/rest/players/profile/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/players/profile/' -d 'values%5Bid%5D=41928'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `values[id]` | `player_id` | body |  | `Y` |  | Required. khl.ru player id (the box score / roster player_id). |

### Returns {#khl_player_profile-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | khl.ru player id; equals webcaster player khl_id |
| `firstname` | String | First name in Russian |
| `lastname` | String | Last name in Russian |
| `photo` | String | Player photo URL on khl.ru (protocol-relative) |
| `position` | String | Position code (Russian letter on www.khl.ru) |
| `position_code` | String | Position key in English (goaltender, defender, forward) |
| `biography` | String | Biography text in Russian (HTML) |
| `is_hero` | Boolean | Site 'hero' flag (unverified meaning) |
| `is_rip` | Boolean | True for a deceased player |
| `links_stats` | String | Relative khl.ru page path |
| `pagelink` | String | Relative khl.ru page path |
| `summary_stats_gp` | Int64 | Career KHL games played (profile summary) |
| `summary_stats_a` | Int64 | Career KHL assists (profile summary) |
| `summary_stats_pm` | Int64 | Career KHL plus-minus (profile summary) |
| `summary_stats_g` | Int64 | Career KHL goals (profile summary) |
| `summary_stats_pts` | Int64 | Career KHL points (profile summary) |
| `jn` | Int64 | Jersey number (not an id; 0 on a team event) |
| `club_name` | String | Name in Russian (Cyrillic); the *_en / *en sibling holds the English transliteration |
| `club_logo` | String | Current club logo URL on khl.ru (protocol-relative) |
| `club_code` | String | Club slug (/clubs/<slug>/) |
| `height` | String | Height with its Russian unit ('183 см' = 183 cm) |
| `weight` | String | Weight with its Russian unit ('85 кг' = 85 kg) |
| `birthdate` | String | Birth date (Russian display format) |
| `age` | String | Age with its Russian unit ('27 лет' = 27 years) |
| `contract_name` | String | Contract label in Russian |
| `contract_date` | String | Contract end date |
| `country_name` | String | Name in Russian (Cyrillic); the *_en / *en sibling holds the English transliteration |
| `country_flag` | String | Flag image path of the player's country (citizenship) |
| `sport_country_name` | String | Name in Russian (Cyrillic); the *_en / *en sibling holds the English transliteration |
| `sport_country_flag` | String | Flag image path of the country the player represents in sport |
| `stick` | String | Shooting hand: l = left, r = right |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Raises {#khl_player_profile-raises}

| Exception | Description |
|---|---|
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Khl.ru answered an error envelope (status != success), a 307 again after re-priming the session, or a page for another game / player / tournament than the one requested. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Khl.ru answered no rows, or the season has no such stage. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_player_profile-example}

```python
khl_player_profile(player_id=41928)
```

## khl_player_career

Player career: one row per season tournament.

**Endpoint URL:** `POST https://www.khl.ru/rest/players/stats/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/players/stats/' -d 'values%5Bid%5D=41928'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `values[id]` | `player_id` | body |  | `Y` |  | Required. khl.ru player id (the box score / roster player_id). |

### Returns {#khl_player_career-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | khl.ru player id; equals webcaster player khl_id |
| `season_label` | String | Season label as sent (26/27, 2025/2026) |
| `tournament_id` | Int64 | khl.ru tournament (stage) id, tnId: 1369 = 2025-26 regular season, 1370 its playoff; equals webcaster stage khl_id |
| `team_season_id` | Int64 | Team-season id (tid; one per tournament; joins nothing in the webcaster API) |
| `club_id` | Int64 | khl.ru CLUB id (stable across seasons; joins webcaster team khl_id) |
| `team_name` | String | Team name as the site prints it |
| `jn` | Int64 | Jersey number (not an id; 0 on a team event) |
| `gp` | Int64 | Games played by the player |
| `g` | Int64 | Goals scored by the player |
| `a` | Int64 | Assists credited to the player |
| `pts` | Int64 | Points (goals plus assists) |
| `pm` | Int64 | Plus-minus rating of the player |
| `plus` | Int64 | Plus (on ice for a goal for) |
| `minus` | Int64 | Minus (on ice for a goal against) |
| `pim` | Int64 | Penalty minutes |
| `eqg` | Int64 | Even-strength goals |
| `ppg` | Int64 | Power-play goals |
| `shg` | Int64 | Short-handed goals |
| `otg` | Int64 | Overtime goals scored by the player |
| `gwg` | Int64 | Game-winning goals |
| `sds` | Int64 | Shootout-deciding goals |
| `sog` | Int64 | Shots on goal by the player |
| `sog_pct` | Float64 | Shooting percentage |
| `sog_avg` | Float64 | Shots on goal per game |
| `fo` | Int64 | Faceoffs taken by the player |
| `fow` | Int64 | Faceoffs won by the player |
| `fo_pct` | Float64 | Faceoff win percentage |
| `toi_avg` | String | Average time on ice per game (mm:ss) |
| `sft_avg` | Float64 | Average shifts per game |
| `tie_avg` | String | Average even-strength time on ice per game |
| `sfte_avg` | Float64 | Average even-strength shifts per game |
| `tipp_avg` | String | Average power-play time on ice per game |
| `sftpp_avg` | Float64 | Average power-play shifts per game |
| `tish_avg` | String | Average short-handed time on ice per game |
| `sftsh_avg` | Float64 | Average short-handed shifts per game |
| `hits` | Int64 | Hits (body checks) |
| `bls` | Int64 | Shots blocked by the player |
| `foa` | Int64 | Faceoffs lost by the player |
| `tka` | Int64 | Takeaways by the player |
| `p_intc` | Int64 | Pass interceptions |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Raises {#khl_player_career-raises}

| Exception | Description |
|---|---|
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Khl.ru answered an error envelope (status != success), a 307 again after re-priming the session, or a page for another game / player / tournament than the one requested. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Khl.ru answered no rows, or the season has no such stage. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_player_career-example}

```python
khl_player_career(player_id=41928)
```

## khl_playoff_bracket

Playoff bracket: one row per team per series.

**Endpoint URL:** `POST https://www.khl.ru/rest/standings/playoff/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/standings/playoff/' -d 'stage=playoff&values%5Btype%5D=playoff&season=2026'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `season` | `season` | body |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). Required unless tournament_id is given. |
| `values[tournament]` | `tournament_id` | body |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |

### Returns {#khl_playoff_bracket-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `round` | Int64 | Playoff round number |
| `wins_to_advance` | Int64 | Wins needed to win a series of the round |
| `pair` | Int64 | Series number within the round |
| `conference` | String | Conference (west / east) |
| `seed` | Int64 | Conference seed |
| `club_id` | Int64 | khl.ru CLUB id (stable across seasons; joins webcaster team khl_id) |
| `name` | String | Name in Russian (Cyrillic); the *_en / *en sibling holds the English transliteration |
| `series_wins` | Int64 | Wins in the series so far |
| `game_ids` | List | khl.ru game ids of the series games, a List(Int64) in feed order (unplayed slots skipped) |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Raises {#khl_playoff_bracket-raises}

| Exception | Description |
|---|---|
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Khl.ru answered an error envelope (status != success), a 307 again after re-priming the session, or a page for another game / player / tournament than the one requested. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Khl.ru answered no rows, or the season has no such stage. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_playoff_bracket-example}

```python
khl_playoff_bracket(season=2026)
```

## khl_standings

Regular-season standings: one row per team per table (league, conferences, divisions).

**Endpoint URL:** `POST https://www.khl.ru/rest/standings/regular/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/standings/regular/' -d 'values%5Btype%5D=regular&season=2026'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `season` | `season` | body |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). None = the site's current season. |
| `values[tournament]` | `tournament_id` | body |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |

### Returns {#khl_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `table` | String | Standings table: league, a conference (west / east) or a division key |
| `rank` | Int64 | 1-based position in its table / list |
| `club_id` | Int64 | khl.ru CLUB id (stable across seasons; joins webcaster team khl_id) |
| `name` | String | Name in Russian (Cyrillic); the *_en / *en sibling holds the English transliteration |
| `gp` | Int64 | Games played (gp = w + otw + sow + sol + otl + l) |
| `w` | Int64 | Regulation wins only: overtime (otw) and shootout (sow) wins are separate columns |
| `otw` | Int64 | Overtime wins (not counted in w) |
| `sow` | Int64 | Shootout wins (not counted in w) |
| `sol` | Int64 | Shootout losses |
| `otl` | Int64 | Overtime losses |
| `otl_en` | Int64 | Overtime losses with the net empty |
| `l` | Int64 | Regulation losses only: overtime (otl) and shootout (sol) losses are separate columns |
| `pts` | Int64 | Points: 2 per win of any kind, 1 per overtime or shootout loss (3-2-1 in early seasons such as 2013-14) |
| `goals_for` | Int64 | Goals scored by the team |
| `goals_against` | Int64 | Goals allowed by the team |
| `is_out_playoff` | Boolean | Eliminated from playoff contention |
| `link` | String | Relative khl.ru page path |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Raises {#khl_standings-raises}

| Exception | Description |
|---|---|
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Khl.ru answered an error envelope (status != success), a 307 again after re-priming the session, or a page for another game / player / tournament than the one requested. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Khl.ru answered no rows, or the season has no such stage. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_standings-example}

```python
khl_standings(season=2026)
```

## khl_player_stats

League player stats of a tournament, one page (goalies: every page) (one row per player).

**Endpoint URL:** `POST https://www.khl.ru/rest/stat/players/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/stat/players/' -d 'season=2026&stage=regular&values%5Bamplua_full%5D=forwards&nav%5Bpage%5D=1&nav%5Bcount%5D=20'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `season` | `season` | body |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). None = the site's current season. |
| `stage` | `stage` | body |  |  | `Y` | Which stage of the season: regular or playoff. |
| `values[tournament]` | `tournament_id` | body |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |
| `values[amplua_full]` | `position` | body |  |  | `Y` | Position group: goalies, defenses, forwards, skaters or all. None = the site's default. goalies fetches every page and returns one list sorted by games played, then wins (both descending), then name; page is ignored for it. A partial list raises AssetFetchError. |
| `values[clubs_full]` | `club` | body |  |  | `Y` | Club slug (khl_stat_filters, filter clubs_full) or all. None = every club. |
| `nav[page]` | `page` | body |  |  | `Y` | 1-based page (skaters); ignored for goalies. Loop until a page comes back shorter than per_page. |
| `nav[count]` | `per_page` | body |  |  | `Y` | Rows per page: 12, 20, 24 or 36. Keep 20: the site computes page counts and ranks for 20. |

### Returns {#khl_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | String | List the row comes from (position group / roster section) |
| `rank` | Int64 | 1-based position in its table / list |
| `player_id` | Int64 | khl.ru player id; equals webcaster player khl_id |
| `player_name` | String | Player name as the site prints it (Russian on www.khl.ru) |
| `image` | String | Player photo URL on khl.ru (team-season specific) |
| `jn` | Int64 | Jersey number (not an id; 0 on a team event) |
| `gp` | Int64 | Games played by the player |
| `g` | Int64 | Goals scored by the player |
| `a` | Int64 | Assists credited to the player |
| `pts` | Int64 | Points (goals plus assists) |
| `pm` | Int64 | Plus-minus rating of the player |
| `plus` | Int64 | Plus (on ice for a goal for) |
| `minus` | Int64 | Minus (on ice for a goal against) |
| `pim` | Int64 | Penalty minutes |
| `eqg` | Int64 | Even-strength goals |
| `ppg` | Int64 | Power-play goals |
| `shg` | Int64 | Short-handed goals |
| `otg` | Int64 | Overtime goals scored by the player |
| `gwg` | Int64 | Game-winning goals |
| `sds` | Int64 | Shootout-deciding goals |
| `sog` | Int64 | Shots on goal by the player (shots faced for a goalie) |
| `sog_pct` | Float64 | Shooting percentage |
| `sog_avg` | Float64 | Shots on goal per game |
| `fo` | Int64 | Faceoffs taken by the player |
| `fow` | Int64 | Faceoffs won by the player |
| `fo_pct` | Float64 | Faceoff win percentage |
| `toi_avg` | String | Average time on ice per game (mm:ss) |
| `sft_avg` | Float64 | Average shifts per game |
| `tie_avg` | String | Average even-strength time on ice per game |
| `sfte_avg` | Float64 | Average even-strength shifts per game |
| `tipp_avg` | String | Average power-play time on ice per game |
| `sftpp_avg` | Float64 | Average power-play shifts per game |
| `tish_avg` | String | Average short-handed time on ice per game |
| `sftsh_avg` | Float64 | Average short-handed shifts per game |
| `hits` | Int64 | Hits (body checks) |
| `bls` | Int64 | Shots blocked by the player |
| `w` | Int64 | Wins (goalies only) |
| `l` | Int64 | Losses (goalies only) |
| `sop` | Int64 | Games that went to a shootout (goalies only) |
| `ga` | Int64 | Goals against (goalies only) |
| `sv` | Int64 | Saves, shots on goal stopped (goalies only) |
| `sv_pct` | Float64 | Save percentage on a 0-100 scale, saves / shots on goal x 100 (92.2 = .922; goalies only) |
| `gaa` | Float64 | Goals-against average, goals against per 60 minutes of ice time (goalies only) |
| `so` | Int64 | Shutouts, games without a goal against (goalies only) |
| `toi` | String | Total time on the ice over the stage, text "minutes:seconds" (e.g. "3521:21"; goalies only, null when none) |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Raises {#khl_player_stats-raises}

| Exception | Description |
|---|---|
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Khl.ru answered an error envelope (status != success), a 307 again after re-priming the session, or a page for another game / player / tournament than the one requested. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Khl.ru answered no rows, or the season has no such stage. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#khl_player_stats-example}

```python
khl_player_stats(position='forwards', season=2026)
```
