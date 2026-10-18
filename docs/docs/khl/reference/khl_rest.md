---
title: KHL — KHL site REST (www.khl.ru/rest)
sidebar_label: KHL site REST (www.khl.ru/rest)
description: "KHL — KHL site REST (www.khl.ru/rest) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# KHL — KHL site REST (www.khl.ru/rest)

`sportsdataverse.hockey.khl` — 14 endpoints.

> **Note:** Owner flag (sdv-internal-refs/khl/README.md): the KHL and its webcaster platform are run by Russian operators, some clubs by sanctioned state companies. Fetching on your own machine is fine; any published data release, hosted mirror or redistribution needs owner (sanctions / legal) sign-off first.

> **Note:** The documented curl request is schematic: the runtime primes a session (adds the form field `sessid`), maps `season` / `stage` to the form field `values[tournament]` (the tournament id) and sends the X-Requested-With, Origin and Referer headers.

## khl_schedule

Season schedule and results of a tournament, one row per game (All-Star games dropped).

**Endpoint URL:** `POST https://www.khl.ru/rest/calendar/list/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/calendar/list/' -d 'season=2026&stage=regular'`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). None = the site's current season. |
| `stage` | `stage` |  |  | `Y` | Which stage of the season: regular or playoff. |
| `values[tournament]` | `tournament_id` |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |

### Returns {#khl_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | khl.ru game id; equals webcaster event khl_id / match_id |
| `tournament_id` | integer | khl.ru tournament (stage) id, tnId: 1369 = 2025-26 regular season, 1370 its playoff; equals webcaster stage khl_id |
| `date` | character | Game date YYYY-MM-DD (Moscow time) |
| `time` | character | Start time hh:mm (Moscow time); not a game clock |
| `home_club_id` | integer | Home CLUB id (stable across seasons; joins webcaster team khl_id) |
| `home_name` | character | Home team name (Russian on www.khl.ru) |
| `away_club_id` | integer | Visitor CLUB id (stable across seasons; joins webcaster team khl_id) |
| `away_name` | character | Visitor team name (Russian on www.khl.ru) |
| `home_score` | integer | Home goals (final, shootout winner counted) |
| `away_score` | integer | Visitor goals (final, shootout winner counted) |
| `decision` | character | OT or SO when decided in overtime / a shootout; null in regulation |
| `period_scores` | character | Per-period home:away scores, comma-joined |
| `winner_club_id` | integer | Club id of the winner |
| `approved` | integer | 1 = result confirmed by the league |
| `arena_id` | integer | khl.ru arena id |
| `arena_name` | character | Arena (city) name, from data.ARENAS |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Example {#khl_schedule-example}

```python
khl_schedule(season=2026)
```

_Last validated n/a._

## khl_roster

Club roster (current season only): one row per player.

**Endpoint URL:** `POST https://www.khl.ru/rest/clubs/team/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/clubs/team/' -d 'page=team&values%5Bclub_id%5D=7'`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `values[club_id]` | `club_id` |  | `Y` |  | Required. khl.ru CLUB id (7 = Spartak; schedule home_club_id), not a team-season id. |

### Returns {#khl_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | character | List the row comes from (position group / roster section) |
| `player_id` | integer | khl.ru player id; equals webcaster player khl_id |
| `photo` | character | Player photo URL on khl.ru (protocol-relative) |
| `country` | character | Citizenship (Russian) |
| `lastname` | character | Last name in Russian |
| `firstname` | character | First name in Russian |
| `jn` | integer | Jersey number (not an id; 0 on a team event) |
| `age` | integer | Player age in years |
| `favorite` | logical | Front-end favourite flag of the signed-in user (always False here) |
| `link` | character | Relative khl.ru page path |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Example {#khl_roster-example}

```python
khl_roster(club_id=7)
```

_Last validated n/a._

## khl_stat_filters

Stats filter options: seasons, tournaments, position groups and clubs (one row per option).

**Endpoint URL:** `POST https://www.khl.ru/rest/filters/statplayers/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/filters/statplayers/' -d 'season=2026&stage=regular'`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). None = the site's current season. |
| `stage` | `stage` |  |  | `Y` | Which stage of the season: regular or playoff. |
| `values[tournament]` | `tournament_id` |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |

### Returns {#khl_stat_filters-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `filter` | character | Filter code: season, tournament, amplua_full or clubs_full |
| `filter_name` | character | Filter label as the site prints it |
| `value` | character | Option code to send for the filter (a tournament id, a season code such as s19, a club slug) |
| `name` | character | Option label (Russian) |
| `def` | integer | Default tournament id (tnId) of a season option |
| `active` | integer | 1 when the option is selectable |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Example {#khl_stat_filters-example}

```python
khl_stat_filters(season=2026)
```

_Last validated n/a._

## khl_game_header

Game header: status, teams, score, arena (one row).

**Endpoint URL:** `POST https://www.khl.ru/rest/game/header/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/game/header/' -d 'page=resume&values%5Bgameid%5D=898236&season=2026&stage=regular'`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `values[gameid]` | `game_id` |  | `Y` |  | Required. khl.ru game id (the schedule's game_id; equals webcaster event khl_id). |
| `season` | `season` |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). The game's season; required unless tournament_id is given. |
| `stage` | `stage` |  |  | `Y` | Which stage of the season: regular or playoff. |
| `values[tournament]` | `tournament_id` |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |

### Returns {#khl_game_header-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | khl.ru game id; equals webcaster event khl_id / match_id |
| `tournament_id` | integer | khl.ru tournament (stage) id, tnId: 1369 = 2025-26 regular season, 1370 its playoff; equals webcaster stage khl_id |
| `home_club_id` | integer | Home CLUB id (stable across seasons; joins webcaster team khl_id) |
| `home_team_season_id` | integer | Home team-season id (one per tournament; joins nothing in the webcaster API) |
| `away_club_id` | integer | Visitor CLUB id (stable across seasons; joins webcaster team khl_id) |
| `away_team_season_id` | integer | Visitor team-season id (one per tournament; joins nothing in the webcaster API) |
| `arena_id` | integer | khl.ru arena id |
| `home_score` | integer | Home goals (final, shootout winner counted) |
| `away_score` | integer | Visitor goals (final, shootout winner counted) |
| `round` | integer | Playoff round number |
| `roundname` | character | Round label in Russian ('Неделя 29' = Week 29) |
| `roundname_en` | character | Round label in English |
| `date` | character | Game date YYYY-MM-DD (Moscow time) |
| `dayofweek` | integer | Day of week, 1 = Sunday (6 = Friday 2026-03-20) |
| `time` | character | Start time hh:mm:ss (Moscow time); not a game clock |
| `changes` | integer | Count of changes to the game record (0 in the captured bodies; unverified) |
| `number` | integer | Game number within the tournament |
| `home_name_l3` | character | Home club 3-letter code in Russian |
| `home_name_en` | character | Home club name in English |
| `home_name_l3_en` | character | Home club 3-letter code in English |
| `home_city_en` | character | Home club city in English |
| `visitor_name_l3` | character | Visiting club 3-letter code in Russian |
| `visitor_name_en` | character | Visiting club name in English |
| `visitor_name_l3_en` | character | Visiting club 3-letter code in English |
| `visitor_city_en` | character | Visiting club city in English |
| `arena` | character | Arena name in Russian |
| `arena_en` | character | Arena name in English |
| `arena_city` | character | Arena city in Russian |
| `arena_city_en` | character | Arena city in English |
| `msktime_sft` | integer | Offset of local time from Moscow time, hours (unverified) |
| `gmttime_sft` | integer | Offset of Moscow time from GMT, hours (unverified) |
| `temp` | character |  |
| `score` | character | Final score 'home:visitor' |
| `ots` | character | How the game ended: empty = regulation, OT = overtime, SO = shootout |
| `attendance` | integer | Attendance (numeric string) |
| `approved` | integer | 1 = result confirmed by the league |
| `format_date_day` | character | Game date YYYY-MM-DD (Moscow time), as the site formats it |
| `time_format` | character | Start time hh:mm (Moscow time) |
| `full_date` | integer | Start time, unix seconds (UTC) |
| `date_type` | character | Front-end date bucket of the game relative to today ('last' = already played; other values unverified) |
| `type` | character | Game state/type code (sogame = finished in a shootout) |
| `winner` | character | Winning side: A = home, B = visitor |
| `visible_date` | character | Short display date in Russian ('20 мар' = 20 March) |
| `visible_date_full` | character | Display date in Russian without the year ('20 марта') |
| `visible_date_full2` | character | Display date in Russian with the year ('20 марта 2026') |
| `visible_date_day` | character | Abbreviated weekday in Russian ('пт' = Friday) |
| `win` | integer | True when this club won |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Example {#khl_game_header-example}

```python
khl_game_header(game_id=898236, season=2026)
```

_Last validated n/a._

## khl_game_protocol

Game protocol: one row per goal with scorer, assistants, strength and the jerseys on the ice.

**Endpoint URL:** `POST https://www.khl.ru/rest/game/protocol/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/game/protocol/' -d 'values%5Bgameid%5D=898236&season=2026&stage=regular'`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `values[gameid]` | `game_id` |  | `Y` |  | Required. khl.ru game id (the schedule's game_id; equals webcaster event khl_id). |
| `season` | `season` |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). The game's season; required unless tournament_id is given. |
| `stage` | `stage` |  |  | `Y` | Which stage of the season: regular or playoff. |
| `values[tournament]` | `tournament_id` |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |

### Returns {#khl_game_protocol-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | khl.ru game id; equals webcaster event khl_id / match_id |
| `tournament_id` | integer | khl.ru tournament (stage) id, tnId: 1369 = 2025-26 regular season, 1370 its playoff; equals webcaster stage khl_id |
| `period` | integer | Period number (box score: 0 = the whole game; 4+ = overtime) |
| `goal_number` | integer | Goal number in the game |
| `time` | character | Game clock mm:ss (game-elapsed); period marker rows keep their wall-clock HH:MM |
| `team_side` | character | A = home, B = visitor |
| `score_a` | integer | Home goals after the goal |
| `score_b` | integer | Visitor goals after the goal |
| `strength` | character | Strength code as the site prints it (Russian: РАВ even, БОЛ power play, МЕН short-handed) |
| `strength_text` | character | Strength label as the site prints it |
| `scorer_player_id` | integer | Scorer's khl.ru player id |
| `scorer_name` | character | Scorer's name with the season count in brackets |
| `scorer_jn` | integer | Scorer's jersey |
| `assist_1_player_id` | integer | First assistant's khl.ru player id |
| `assist_1_name` | character | First assistant's name with the season count in brackets |
| `assist_1_jn` | integer | First assistant's jersey |
| `assist_2_player_id` | integer | Second assistant's khl.ru player id |
| `assist_2_name` | character | Second assistant's name with the season count in brackets |
| `assist_2_jn` | integer | Second assistant's jersey |
| `on_ice_a` | character | Home jerseys on the ice, comma-separated |
| `on_ice_b` | character | Visitor jerseys on the ice, comma-separated |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Example {#khl_game_protocol-example}

```python
khl_game_protocol(game_id=898236, season=2026)
```

_Last validated n/a._

## khl_game_info

Game info: teams, score, arena, officials, starting goalies (one row).

**Endpoint URL:** `POST https://www.khl.ru/rest/game/text/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/game/text/' -d 'values%5Bgameid%5D=898236&season=2026&stage=regular'`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `values[gameid]` | `game_id` |  | `Y` |  | Required. khl.ru game id (the schedule's game_id; equals webcaster event khl_id). |
| `season` | `season` |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). The game's season; required unless tournament_id is given. |
| `stage` | `stage` |  |  | `Y` | Which stage of the season: regular or playoff. |
| `values[tournament]` | `tournament_id` |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |

### Returns {#khl_game_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | khl.ru game id; equals webcaster event khl_id / match_id |
| `tournament_id` | integer | khl.ru tournament (stage) id, tnId: 1369 = 2025-26 regular season, 1370 its playoff; equals webcaster stage khl_id |
| `home_club_id` | integer | Home CLUB id (stable across seasons; joins webcaster team khl_id) |
| `home_team_season_id` | integer | Home team-season id (one per tournament; joins nothing in the webcaster API) |
| `away_club_id` | integer | Visitor CLUB id (stable across seasons; joins webcaster team khl_id) |
| `away_team_season_id` | integer | Visitor team-season id (one per tournament; joins nothing in the webcaster API) |
| `arena_id` | integer | khl.ru arena id |
| `home_score` | integer | Home goals (final, shootout winner counted) |
| `away_score` | integer | Visitor goals (final, shootout winner counted) |
| `round` | integer | Playoff round number |
| `roundname` | character | Round label in Russian ('Неделя 29' = Week 29) |
| `roundname_en` | character | Round label in English |
| `date` | character | Game date YYYY-MM-DD (Moscow time) |
| `dayofweek` | integer | Day of week, 1 = Sunday (6 = Friday 2026-03-20) |
| `time` | character | Start time hh:mm:ss (Moscow time); not a game clock |
| `changes` | integer | Count of changes to the game record (0 in the captured bodies; unverified) |
| `number` | integer | Game number within the tournament |
| `home_name_l3` | character | Home club 3-letter code in Russian |
| `home_name_en` | character | Home club name in English |
| `home_name_l3_en` | character | Home club 3-letter code in English |
| `home_city_en` | character | Home club city in English |
| `visitor_name_l3` | character | Visiting club 3-letter code in Russian |
| `visitor_name_en` | character | Visiting club name in English |
| `visitor_name_l3_en` | character | Visiting club 3-letter code in English |
| `visitor_city_en` | character | Visiting club city in English |
| `arena` | character | Arena name in Russian |
| `arena_en` | character | Arena name in English |
| `arena_city` | character | Arena city in Russian |
| `arena_city_en` | character | Arena city in English |
| `msktime_sft` | integer | Offset of local time from Moscow time, hours (unverified) |
| `gmttime_sft` | integer | Offset of Moscow time from GMT, hours (unverified) |
| `temp` | character |  |
| `score` | character | Final score 'home:visitor' |
| `ots` | character | How the game ended: empty = regulation, OT = overtime, SO = shootout |
| `sc_p1` | character | 1st-period score 'home:visitor' |
| `sc_p2` | character | 2nd-period score 'home:visitor' |
| `sc_p3` | character | 3rd-period score 'home:visitor' |
| `sc_p4` | character | Overtime or shootout score 'home:visitor' |
| `attendance` | integer | Attendance (numeric string) |
| `approved` | integer | 1 = result confirmed by the league |
| `format_date_day` | character | Game date YYYY-MM-DD (Moscow time), as the site formats it |
| `time_format` | character | Start time hh:mm (Moscow time) |
| `full_date` | integer | Start time, unix seconds (UTC) |
| `date_type` | character | Front-end date bucket of the game relative to today ('last' = already played; other values unverified) |
| `type` | character | Game state/type code (sogame = finished in a shootout) |
| `winner` | character | Winning side: A = home, B = visitor |
| `visible_date` | character | Short display date in Russian ('20 мар' = 20 March) |
| `visible_date_full` | character | Display date in Russian without the year ('20 марта') |
| `visible_date_full2` | character | Display date in Russian with the year ('20 марта 2026') |
| `visible_date_day` | character | Abbreviated weekday in Russian ('пт' = Friday) |
| `statusvalue` | character | Game status in Russian (матч завершен = match finished) |
| `pernum` | integer | Number of regulation periods played |
| `numlines` | integer | Number of lines on the line-up card |
| `gk_a_jn` | integer | Home starting goalie's JERSEY number (gameItem.gkA), not an id |
| `gk_b_jn` | integer | Visitor starting goalie's JERSEY number (gameItem.gkB), not an id |
| `is_events_have_multi_ovt` | logical | True when the game has more than one overtime period (playoff) |
| `idmainref1` | integer | Referee 1: khl.ru referee id |
| `idmainref2` | integer | Referee 2: khl.ru referee id |
| `idlineref1` | integer | Linesman 1: khl.ru referee id |
| `idlineref2` | integer | Linesman 2: khl.ru referee id |
| `jnmainref1` | character | Referee 1 sweater number |
| `jnmainref2` | character | Referee 2 sweater number |
| `jnlineref1` | character | Linesman 1 sweater number |
| `jnlineref2` | character | Linesman 2 sweater number |
| `refm1` | character | Referee 1, name in Russian |
| `refm2` | character | Referee 2, name in Russian |
| `refl1` | character | Linesman 1, name in Russian |
| `refl2` | character | Linesman 2, name in Russian |
| `status` | integer | Game status code (90 = finished; other codes UNVERIFIED) |
| `gk_a_player_id` | integer | khl.ru player id of the home starting goalie (gk_a_jn resolved through players) |
| `gk_b_player_id` | integer | khl.ru player id of the visitor starting goalie (gk_b_jn resolved through players) |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Example {#khl_game_info-example}

```python
khl_game_info(game_id=898236, season=2026)
```

_Last validated n/a._

## khl_player_box

Skater box score by period: one row per period, side and jersey, with the player id.

**Endpoint URL:** `POST https://www.khl.ru/rest/game/text/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/game/text/' -d 'values%5Bgameid%5D=898236&season=2026&stage=regular'`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `values[gameid]` | `game_id` |  | `Y` |  | Required. khl.ru game id (the schedule's game_id; equals webcaster event khl_id). |
| `season` | `season` |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). The game's season; required unless tournament_id is given. |
| `stage` | `stage` |  |  | `Y` | Which stage of the season: regular or playoff. |
| `values[tournament]` | `tournament_id` |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |

### Returns {#khl_player_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | khl.ru game id; equals webcaster event khl_id / match_id |
| `tournament_id` | integer | khl.ru tournament (stage) id, tnId: 1369 = 2025-26 regular season, 1370 its playoff; equals webcaster stage khl_id |
| `period` | integer | Period number (box score: 0 = the whole game; 4+ = overtime) |
| `team_side` | character | A = home, B = visitor |
| `jn` | integer | Jersey number (not an id; 0 on a team event) |
| `player_id` | integer | khl.ru player id; equals webcaster player khl_id |
| `player_name` | character | Player name as the site prints it (Russian on www.khl.ru) |
| `position` | character | Position code (Russian letter on www.khl.ru) |
| `g` | integer | Goals scored in the period block |
| `a` | integer | Home roster keyed by jersey number |
| `p` | character | Points (empty string when none) |
| `shots` | integer | Shots on goal in the period block |
| `pim` | integer | Penalty minutes |
| `fo` | integer | Faceoffs taken in the period block |
| `fow` | integer | Faceoffs won in the period block |
| `fow_pct` | numeric | Stat code the bar is computed from |
| `bls` | integer | Shots blocked in the period block |
| `hits` | integer | Hits (body checks) |
| `fa` | integer | Takeaways (the site labels this column 'Отборы', takeaways) |
| `p_intc` | integer | Pass interceptions |
| `sft` | character | Time on ice 'mm:ss(shifts)' |
| `sfte` | character | Even-strength time on ice 'mm:ss(shifts)' |
| `sftpp` | character | Power-play time on ice 'mm:ss(shifts)' |
| `sftsh` | character | Short-handed time on ice 'mm:ss(shifts)' |
| `sften` | character | Empty-net time on ice 'mm:ss(shifts)' |
| `tiams` | character | Time in the attacking zone (mm:ss) |
| `ts` | numeric | Top speed, km/h (tracking; present by 2019/20) |
| `dt` | numeric | Distance skated, km (tracking) |
| `as` | numeric | Average speed, km/h (tracking) |
| `pcd` | numeric | Puck-control duration (tracking; unit unverified) |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Example {#khl_player_box-example}

```python
khl_player_box(game_id=898236, season=2026)
```

_Last validated n/a._

## khl_pbp

Play-by-play: shots, faceoffs, hits, blocks, giveaways, takeaways, penalties and goals (2008-09 on).

**Endpoint URL:** `POST https://www.khl.ru/rest/game/text/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/game/text/' -d 'values%5Bgameid%5D=898236&season=2026&stage=regular'`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `values[gameid]` | `game_id` |  | `Y` |  | Required. khl.ru game id (the schedule's game_id; equals webcaster event khl_id). |
| `season` | `season` |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). The game's season; required unless tournament_id is given. |
| `stage` | `stage` |  |  | `Y` | Which stage of the season: regular or playoff. |
| `values[tournament]` | `tournament_id` |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |

### Returns {#khl_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | khl.ru game id; equals webcaster event khl_id / match_id |
| `tournament_id` | integer | khl.ru tournament (stage) id, tnId: 1369 = 2025-26 regular season, 1370 its playoff; equals webcaster stage khl_id |
| `period` | integer | Period number (box score: 0 = the whole game; 4+ = overtime) |
| `event_index` | integer | 0-based order of the event in the game, oldest first |
| `event_type` | character | period (start / end marker), faceoff, shot, block, action, penalty or goal |
| `time` | character | Game clock mm:ss (game-elapsed); period marker rows keep their wall-clock HH:MM |
| `time_seconds` | integer | Game-elapsed seconds (null on period marker rows) |
| `team_side` | character | A = home, B = visitor |
| `jn` | integer | Jersey number (not an id; 0 on a team event) |
| `player_id` | integer | khl.ru player id; equals webcaster player khl_id |
| `result` | character | Event result code (shots: goal / saved / miss; actions: hit, gva = giveaway, tka = takeaway, intc = interception) |
| `result_text` | character | Event result label |
| `penalty_minutes` | integer | Penalty minutes (penalty events) |
| `score_a` | integer | Home goals after the goal |
| `score_b` | integer | Visitor goals after the goal |
| `won_side` | character | Faceoff winner's side (A / B) |
| `won_jn` | integer | Faceoff winner's jersey |
| `won_player_id` | integer | Faceoff winner's khl.ru player id |
| `lost_side` | character | Faceoff loser's side (A / B) |
| `lost_jn` | integer | Faceoff loser's jersey |
| `lost_player_id` | integer | Faceoff loser's khl.ru player id |
| `won_zone` | character | Zone of the faceoff from the winner's view: offensive, neutral or defensive |
| `shooter_side` | character | Blocked shooter's side (A / B) |
| `shooter_jn` | integer | Blocked shooter's jersey |
| `shooter_player_id` | integer | Blocked shooter's khl.ru player id |
| `period_action` | character | Period marker: 0 = start, 1 = end (inferred) |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Example {#khl_pbp-example}

```python
khl_pbp(game_id=898236, season=2026)
```

_Last validated n/a._

## khl_shots

Shot map: one row per shot with its x / y on the 1000 x 500 rink grid (2024-25 on).

**Endpoint URL:** `POST https://www.khl.ru/rest/game/text/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/game/text/' -d 'values%5Bgameid%5D=898236&season=2026&stage=regular'`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `values[gameid]` | `game_id` |  | `Y` |  | Required. khl.ru game id (the schedule's game_id; equals webcaster event khl_id). |
| `season` | `season` |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). The game's season; required unless tournament_id is given. |
| `stage` | `stage` |  |  | `Y` | Which stage of the season: regular or playoff. |
| `values[tournament]` | `tournament_id` |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |

### Returns {#khl_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | khl.ru game id; equals webcaster event khl_id / match_id |
| `tournament_id` | integer | khl.ru tournament (stage) id, tnId: 1369 = 2025-26 regular season, 1370 its playoff; equals webcaster stage khl_id |
| `period` | integer | Period number (box score: 0 = the whole game; 4+ = overtime) |
| `time` | character | Game clock mm:ss (game-elapsed); period marker rows keep their wall-clock HH:MM |
| `time_seconds` | integer | Game-elapsed seconds (null on period marker rows) |
| `team_side` | character | A = home, B = visitor |
| `jn` | integer | Jersey number (not an id; 0 on a team event) |
| `player_id` | integer | khl.ru player id; equals webcaster player khl_id |
| `shot_type` | integer | 1 = goal, 2 = saved (inferred upstream; unverified) |
| `x` | integer | Shot x on the site's 1000 x 500 rink grid, origin top-left (indent_left = x/10 %); goal lines near x = 55 and 945. Team A (home) attacks +x and team B -x in every period, overtime included |
| `y` | integer | Shot y on the site's 1000 x 500 grid, origin top-left, increasing downward (indent_top = y/5 %); the grid is stretched in y, about 1.18x against a 200 x 85 ft rink |
| `indent_left` | numeric | Shot position as percent from the left edge |
| `indent_top` | numeric | Shot position as percent from the top edge |
| `indent_right` | numeric | Shot position as percent from the right edge |
| `indent_bottom` | numeric | Shot position as percent from the bottom edge |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Example {#khl_shots-example}

```python
khl_shots(game_id=898236, season=2026)
```

_Last validated n/a._

## khl_player_profile

Player profile: bio, contract, club (one row).

**Endpoint URL:** `POST https://www.khl.ru/rest/players/profile/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/players/profile/' -d 'values%5Bid%5D=41928'`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `values[id]` | `player_id` |  | `Y` |  | Required. khl.ru player id (the box score / roster player_id). |

### Returns {#khl_player_profile-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | khl.ru player id; equals webcaster player khl_id |
| `firstname` | character | First name in Russian |
| `lastname` | character | Last name in Russian |
| `photo` | character | Player photo URL on khl.ru (protocol-relative) |
| `position` | character | Position code (Russian letter on www.khl.ru) |
| `position_code` | character | Position key in English (goaltender, defender, forward) |
| `biography` | character | Biography text in Russian (HTML) |
| `is_hero` | logical | Site 'hero' flag (unverified meaning) |
| `is_rip` | logical | True for a deceased player |
| `links_stats` | character | Relative khl.ru page path |
| `pagelink` | character | Relative khl.ru page path |
| `summary_stats_gp` | integer | Career KHL games played (profile summary) |
| `summary_stats_a` | integer | Career KHL assists (profile summary) |
| `summary_stats_pm` | integer | Career KHL plus-minus (profile summary) |
| `summary_stats_g` | integer | Career KHL goals (profile summary) |
| `summary_stats_pts` | integer | Career KHL points (profile summary) |
| `jn` | integer | Jersey number (not an id; 0 on a team event) |
| `club_name` | character | Name in Russian (Cyrillic); the *_en / *en sibling holds the English transliteration |
| `club_logo` | character | Current club logo URL on khl.ru (protocol-relative) |
| `club_code` | character | Club slug (/clubs/<slug>/) |
| `height` | character | Height with its Russian unit ('183 см' = 183 cm) |
| `weight` | character | Weight with its Russian unit ('85 кг' = 85 kg) |
| `birthdate` | character | Birth date (Russian display format) |
| `age` | character | Age with its Russian unit ('27 лет' = 27 years) |
| `contract_name` | character | Contract label in Russian |
| `contract_date` | character | Contract end date |
| `country_name` | character | Name in Russian (Cyrillic); the *_en / *en sibling holds the English transliteration |
| `country_flag` | character | Flag image path of the player's country (citizenship) |
| `sport_country_name` | character | Name in Russian (Cyrillic); the *_en / *en sibling holds the English transliteration |
| `sport_country_flag` | character | Flag image path of the country the player represents in sport |
| `stick` | character | Shooting hand: l = left, r = right |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Example {#khl_player_profile-example}

```python
khl_player_profile(player_id=41928)
```

_Last validated n/a._

## khl_player_career

Player career: one row per season tournament.

**Endpoint URL:** `POST https://www.khl.ru/rest/players/stats/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/players/stats/' -d 'values%5Bid%5D=41928'`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `values[id]` | `player_id` |  | `Y` |  | Required. khl.ru player id (the box score / roster player_id). |

### Returns {#khl_player_career-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | khl.ru player id; equals webcaster player khl_id |
| `season_label` | character | Season label as sent (26/27, 2025/2026) |
| `tournament_id` | integer | khl.ru tournament (stage) id, tnId: 1369 = 2025-26 regular season, 1370 its playoff; equals webcaster stage khl_id |
| `team_season_id` | integer | Team-season id (tid; one per tournament; joins nothing in the webcaster API) |
| `club_id` | integer | khl.ru CLUB id (stable across seasons; joins webcaster team khl_id) |
| `team_name` | character | Team name as the site prints it |
| `jn` | integer | Jersey number (not an id; 0 on a team event) |
| `gp` | integer | Games played by the player |
| `g` | integer | Goals scored by the player |
| `a` | integer | Assists credited to the player |
| `pts` | integer | Points (goals plus assists) |
| `pm` | integer | Plus-minus rating of the player |
| `plus` | integer | Plus (on ice for a goal for) |
| `minus` | integer | Minus (on ice for a goal against) |
| `pim` | integer | Penalty minutes |
| `eqg` | integer | Even-strength goals |
| `ppg` | integer | Power-play goals |
| `shg` | integer | Short-handed goals |
| `otg` | integer | Overtime goals scored by the player |
| `gwg` | integer | Game-winning goals |
| `sds` | integer | Shootout-deciding goals |
| `sog` | integer | Shots on goal by the player |
| `sog_pct` | numeric | Shooting percentage |
| `sog_avg` | numeric | Shots on goal per game |
| `fo` | integer | Faceoffs taken by the player |
| `fow` | integer | Faceoffs won by the player |
| `fo_pct` | numeric | Faceoff win percentage |
| `toi_avg` | character | Average time on ice per game (mm:ss) |
| `sft_avg` | numeric | Average shifts per game |
| `tie_avg` | character | Average even-strength time on ice per game |
| `sfte_avg` | numeric | Average even-strength shifts per game |
| `tipp_avg` | character | Average power-play time on ice per game |
| `sftpp_avg` | numeric | Average power-play shifts per game |
| `tish_avg` | character | Average short-handed time on ice per game |
| `sftsh_avg` | numeric | Average short-handed shifts per game |
| `hits` | integer | Hits (body checks) |
| `bls` | integer | Shots blocked by the player |
| `foa` | integer | Faceoffs lost by the player |
| `tka` | integer | Takeaways by the player |
| `p_intc` | integer | Pass interceptions |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Example {#khl_player_career-example}

```python
khl_player_career(player_id=41928)
```

_Last validated n/a._

## khl_playoff_bracket

Playoff bracket: one row per team per series.

**Endpoint URL:** `POST https://www.khl.ru/rest/standings/playoff/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/standings/playoff/' -d 'stage=playoff&values%5Btype%5D=playoff&season=2026'`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). Required unless tournament_id is given. |
| `values[tournament]` | `tournament_id` |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |

### Returns {#khl_playoff_bracket-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `round` | integer | Playoff round number |
| `wins_to_advance` | integer | Wins needed to win a series of the round |
| `pair` | integer | Series number within the round |
| `conference` | character | Conference (west / east) |
| `seed` | integer | Conference seed |
| `club_id` | integer | khl.ru CLUB id (stable across seasons; joins webcaster team khl_id) |
| `name` | character | Name in Russian (Cyrillic); the *_en / *en sibling holds the English transliteration |
| `series_wins` | integer | Wins in the series so far |
| `game_ids` | list | khl.ru game ids of the series games, a List(Int64) in feed order (unplayed slots skipped) |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Example {#khl_playoff_bracket-example}

```python
khl_playoff_bracket(season=2026)
```

_Last validated n/a._

## khl_standings

Regular-season standings: one row per team per table (league, conferences, divisions).

**Endpoint URL:** `POST https://www.khl.ru/rest/standings/regular/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/standings/regular/' -d 'values%5Btype%5D=regular&season=2026'`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). None = the site's current season. |
| `values[tournament]` | `tournament_id` |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |

### Returns {#khl_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `table` | character | Standings table: league, a conference (west / east) or a division key |
| `rank` | integer | 1-based position in its table / list |
| `club_id` | integer | khl.ru CLUB id (stable across seasons; joins webcaster team khl_id) |
| `name` | character | Name in Russian (Cyrillic); the *_en / *en sibling holds the English transliteration |
| `gp` | integer | Games played (gp = w + otw + sow + sol + otl + l) |
| `w` | integer | Regulation wins only: overtime (otw) and shootout (sow) wins are separate columns |
| `otw` | integer | Overtime wins (not counted in w) |
| `sow` | integer | Shootout wins (not counted in w) |
| `sol` | integer | Shootout losses |
| `otl` | integer | Overtime losses |
| `otl_en` | integer | Overtime losses with the net empty |
| `l` | integer | Regulation losses only: overtime (otl) and shootout (sol) losses are separate columns |
| `pts` | integer | Points: 2 per win of any kind, 1 per overtime or shootout loss (3-2-1 in early seasons such as 2013-14) |
| `goals_for` | integer | Goals scored by the team |
| `goals_against` | integer | Goals allowed by the team |
| `is_out_playoff` | logical | Eliminated from playoff contention |
| `link` | character | Relative khl.ru page path |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Example {#khl_standings-example}

```python
khl_standings(season=2026)
```

_Last validated n/a._

## khl_player_stats

League player stats of a tournament, one page (goalies: every page) (one row per player).

**Endpoint URL:** `POST https://www.khl.ru/rest/stat/players/`

**Example request:** `curl -X POST 'https://www.khl.ru/rest/stat/players/' -d 'season=2026&stage=regular&values%5Bamplua_full%5D=forwards&nav%5Bpage%5D=1&nav%5Bcount%5D=20'`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season by its END year (2026 = 2025-26), mapped to the KHL tournament id (tnId) through the webcaster season table (khl_webcaster_stages). None = the site's current season. |
| `stage` | `stage` |  |  | `Y` | Which stage of the season: regular or playoff. |
| `values[tournament]` | `tournament_id` |  |  | `Y` | khl.ru tournament id (tnId), sent as is instead of season / stage (1369 = 2025-26 regular season, 1370 its playoff; the schedule's tournament_id column). The All-Star games have their own. |
| `values[amplua_full]` | `position` |  |  | `Y` | Position group: goalies, defenses, forwards, skaters or all. None = the site's default. goalies fetches every page and returns one list sorted by games played, then wins (both descending), then name; page is ignored for it. A partial list raises AssetFetchError. |
| `values[clubs_full]` | `club` |  |  | `Y` | Club slug (khl_stat_filters, filter clubs_full) or all. None = every club. |
| `nav[page]` | `page` |  |  | `Y` | 1-based page (skaters); ignored for goalies. Loop until a page comes back shorter than per_page. |
| `nav[count]` | `per_page` |  |  | `Y` | Rows per page: 12, 20, 24 or 36. Keep 20: the site computes page counts and ranks for 20. |

### Returns {#khl_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | character | List the row comes from (position group / roster section) |
| `rank` | integer | 1-based position in its table / list |
| `player_id` | integer | khl.ru player id; equals webcaster player khl_id |
| `player_name` | character | Player name as the site prints it (Russian on www.khl.ru) |
| `image` | character | Player photo URL on khl.ru (team-season specific) |
| `jn` | integer | Jersey number (not an id; 0 on a team event) |
| `gp` | integer | Games played by the player |
| `g` | integer | Goals scored by the player |
| `a` | integer | Assists credited to the player |
| `pts` | integer | Points (goals plus assists) |
| `pm` | integer | Plus-minus rating of the player |
| `plus` | integer | Plus (on ice for a goal for) |
| `minus` | integer | Minus (on ice for a goal against) |
| `pim` | integer | Penalty minutes |
| `eqg` | integer | Even-strength goals |
| `ppg` | integer | Power-play goals |
| `shg` | integer | Short-handed goals |
| `otg` | integer | Overtime goals scored by the player |
| `gwg` | integer | Game-winning goals |
| `sds` | integer | Shootout-deciding goals |
| `sog` | integer | Shots on goal by the player (shots faced for a goalie) |
| `sog_pct` | numeric | Shooting percentage |
| `sog_avg` | numeric | Shots on goal per game |
| `fo` | integer | Faceoffs taken by the player |
| `fow` | integer | Faceoffs won by the player |
| `fo_pct` | numeric | Faceoff win percentage |
| `toi_avg` | character | Average time on ice per game (mm:ss) |
| `sft_avg` | numeric | Average shifts per game |
| `tie_avg` | character | Average even-strength time on ice per game |
| `sfte_avg` | numeric | Average even-strength shifts per game |
| `tipp_avg` | character | Average power-play time on ice per game |
| `sftpp_avg` | numeric | Average power-play shifts per game |
| `tish_avg` | character | Average short-handed time on ice per game |
| `sftsh_avg` | numeric | Average short-handed shifts per game |
| `hits` | integer | Hits (body checks) |
| `bls` | integer | Shots blocked by the player |
| `w` | integer | Wins (goalies only) |
| `l` | integer | Losses (goalies only) |
| `sop` | integer | Games that went to a shootout (goalies only) |
| `ga` | integer | Goals against (goalies only) |
| `sv` | integer | Saves, shots on goal stopped (goalies only) |
| `sv_pct` | numeric | Save percentage on a 0-100 scale, saves / shots on goal x 100 (92.2 = .922; goalies only) |
| `gaa` | numeric | Goals-against average, goals against per 60 minutes of ice time (goalies only) |
| `so` | integer | Shutouts, games without a goal against (goalies only) |
| `toi` | character | Total time on the ice over the stage, text "minutes:seconds" (e.g. "3521:21"; goalies only, null when none) |

**`return_parsed=False`** — the raw khl.ru envelope `Dict` (`{status, data, errors}`).

### Example {#khl_player_stats-example}

```python
khl_player_stats(position='forwards', season=2026)
```

_Last validated n/a._
