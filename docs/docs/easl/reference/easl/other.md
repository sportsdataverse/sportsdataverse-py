---
title: "EASL — EASL sync server (easl.joshricha.com, third-party over Sportradar DataCore) — Other"
sidebar_label: "Other"
sidebar_position: 4
description: "EASL — EASL sync server (easl.joshricha.com, third-party over Sportradar DataCore) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# EASL — EASL sync server (easl.joshricha.com, third-party over Sportradar DataCore) — Other

## easl_health

Liveness probe of the unofficial sync server: status, message and the server's own timestamp.

**Endpoint URL:** `GET https://easl.joshricha.com/api/v1/health`

**Valid URL:** [https://easl.joshricha.com/api/v1/health](https://easl.joshricha.com/api/v1/health)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#easl_health-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `status` | character | Server status: 'OK'. |
| `message` | character | Human-readable status text ('EASL Basketball API is running'). |
| `timestamp` | character | Server time, ISO 8601 with milliseconds and Z (UTC). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#easl_health-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The server has nothing for the request (an empty list or leaders object, a team or player without games, a 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown fixture is the server's HTTP 500), an error envelope, a game that is not finished or whose box score or play-by-play is incomplete, a stalled sync on the active season, or a body for another game, season, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed id or a season that is not one of the server's END years (raised before the request that would use it). |

### Example {#easl_health-example}

```python
easl_health()
```

## easl_seasons

Every configured EASL season (the four editions 2023-24 to 2026-27) with its DataCore season id and END year.

**Endpoint URL:** `GET https://easl.joshricha.com/api/v1/metadata/seasons`

**Valid URL:** [https://easl.joshricha.com/api/v1/metadata/seasons](https://easl.joshricha.com/api/v1/metadata/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#easl_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = EASL 25-26), derived as year + 1. |
| `season_id` | character | seasonId: DataCore season UUID v1 (key space: EASL seasons; one per edition: 57cefc32... is 2024-25). Joins the `seasonId` query parameter of /matches, /standings, /leaders, /players and the /teams/{teamId} routes, the `seasonId` of every fixture and leader row, and the season half of a team-fixture `stage.id`. |
| `season_row_id` | integer | The sync server's own integer row id (1 = 2024-25, 2 = 2025-26, 3 = 2023-24, 4 = 2026-27: insertion order, not chronological). Not a DataCore id. |
| `year` | integer | START year of the season (2024 = EASL 24-25, which runs 2024-10 to 2025-03). sdv season (END year) = year + 1. |
| `name` | character | Season label ('EASL 24-25' = 2024-25). |
| `name_latin` | character | Season label in Latin script; null on some seasons. |
| `status` | character | COMPLETE for a finished season, ACTIVE for the current one (the default of every season route). |
| `start_date` | date | First day of the season, local date YYYY-MM-DD. |
| `end_date` | date | Last scheduled day of the season, local date YYYY-MM-DD. |
| `competition_id` | character | DataCore competition UUID v1 (key space: competitions); the same 77a790d8-ad01-11ed-8355-01b87a3904c0 on all four seasons: EASL is one competition with one season per edition. |
| `profile_id` | character | DataCore profile UUID v1 of the competition (joins nothing in this API). |
| `organization_id` | character | DataCore organisation id of the league: b1e0z (EASL) on every row. |
| `age_group` | character | Age group of the competition: SENIOR. |
| `event_type` | character | DataCore event type: FIXTURE (a game schedule). |
| `gender` | character | MALE (EASL is a men's competition). |
| `representation` | character | CLUB (club teams, not national teams). |
| `season_type` | character | DataCore season type: SEASON. |
| `standard` | character | Competition standard: ELITE. |
| `created_at` | character | When the sync server inserted the row, 'YYYY-MM-DD HH:MM:SS' (a different format from updatedAt; zone unstated, UTC assumed). |
| `updated_at` | character | When the sync server last synced the row, ISO 8601 with milliseconds and Z (UTC). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#easl_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The server has nothing for the request (an empty list or leaders object, a team or player without games, a 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown fixture is the server's HTTP 500), an error envelope, a game that is not finished or whose box score or play-by-play is incomplete, a stalled sync on the active season, or a body for another game, season, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed id or a season that is not one of the server's END years (raised before the request that would use it). |

### Example {#easl_seasons-example}

```python
easl_seasons()
```

## easl_schedule

Every fixture of one season: status, tip-off, stage, pool, venue and both teams with their scores.

**Endpoint URL:** `GET https://easl.joshricha.com/api/v1/matches`

**Valid URL:** [https://easl.joshricha.com/api/v1/matches?season=2026](https://easl.joshricha.com/api/v1/matches?season=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season END year (2026 = EASL 25-26; the four editions are 2024 to 2027). easl_runtime reads easl_seasons and maps it to the DataCore seasonId. Default: the ACTIVE season. |

### Returns {#easl_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = EASL 25-26): stamped by easl_runtime from the request, null when parsed bare. |
| `season_id` | character | DataCore season UUID v1 of the fixture (the row's own seasonId). |
| `fixture_id` | character | fixtureId: DataCore fixture (game) UUID v1 (key space: EASL games; the {fixtureId} of every /matches route). |
| `fixture_number` | integer | Fixture number YYSSNNN: two-digit start year, two-digit end year, three-digit sequence (2627001 = first game of 2026-27, 2526003 = third of 2025-26). Null on every 2023-24 fixture. |
| `fixture_type` | character | REGULAR (group stage), PLAYOFF (quarter- and semifinals) or FINAL (championship and third-place games). Missing from the DRAFT placeholders. Absent on some rows. |
| `status` | character | CONFIRMED for a finished game, SCHEDULED for a future one, DRAFT for an unscheduled placeholder. |
| `fixture_name` | character | Fixture name for special games ('EASL Championship', 'Semifinals 2', 'EASL Tip-Off 2026 Cebu'); null for ordinary group games. |
| `start_time_utc` | character | Scheduled tip-off in UTC, 'YYYY-MM-DDTHH:MM:SS' with no zone marker. The /matches list holds null here on DRAFT fixtures; the single-fixture sample is a finished game. |
| `start_time_local` | character | Scheduled tip-off in the venue's local time, 'YYYY-MM-DDTHH:MM:SS' with no zone (19:10 local = 10:10 UTC in Tokyo). DRAFT fixtures carry a date at 00:00:00. |
| `attendance` | integer | Announced attendance of the game; null when not recorded. |
| `live_data_available` | logical | Whether live stats were collected for the game (present on a minority of fixtures; meaning unverified). Absent on some rows. |
| `live_video_available` | logical | Whether live video was available (false on all 146 fixtures of the four seasons). |
| `pool_code` | character | Pool (group) id: a 30-character hex string, equal to pool.id; null on playoff and DRAFT fixtures. |
| `pool_id` | character | Pool id: a 30-character hex string; null for playoff games ('Unknown Pool'). |
| `pool_name` | character | Pool name: 'Group A', 'Group B', 'Group C' or the placeholder 'Unknown Pool'. |
| `pool_abb` | character | Pool abbreviation (A, B, C). Playoff and DRAFT fixtures of the /matches list carry the placeholder pool, whose abb is empty or 'N/A'. Null on some rows. |
| `stage_id` | character | Stage reference '<season UUID>:<stage code>' on team-fixture rows only (its first half scopes the fixture to a season); null on the other fixture routes, which send the stage_code, stage_name and stage_order trio. |
| `stage_code` | character | Stage id: a 30-character hex string. Absent on some rows. |
| `stage_name` | character | Stage name: 'Group Stage', 'Final Four' or 'EASL Playoffs'. Absent on some rows. |
| `stage_order` | integer | Ordinal of the stage within the season (1 = Group Stage, 2 = EASL Playoffs). Absent on some rows. |
| `venue_name` | character | Arena name. Absent on some rows. |
| `venue_name_latin` | character | Venue name in Latin script; null on most rows. Absent on some rows. |
| `venue_abbreviation_latin` | character | Venue abbreviation in the Latin field; null except the native-script name 沖縄アリーナ (Okinawa Arena) on 12 fixtures. Absent on some rows. |
| `venue_abbreviation_local` | character | Venue abbreviation in the local field; null except 'F-PICO' on 2 fixtures. Absent on some rows. |
| `venue_alternate_name_latin` | character | Alternative venue name; null on most rows. Absent on some rows. |
| `venue_country_code` | character | ISO 3166-1 alpha-3 country code of the arena: JPN, TWN, PHL, KOR, MAC, HKG, MNG, CHN. The /matches list holds null here on DRAFT fixtures (empty venue). Absent on some rows. |
| `venue_city` | character | City of the arena. Absent on some rows. |
| `venue_state` | character | State or province of the arena (Philippine games, for example Cebu). Absent on some rows. |
| `home_team_id` | character | Home team: teamId: DataCore team (entity) UUID v1 (key space: EASL teams, the same id as `entityId`; joins /teams, /standings team.id, fixture competitors, box and play-by-play teams). Not a player id. |
| `home_name` | character | Home team: display name. Despite the field name this is the English / Latin-script name used on the league site. |
| `home_name_native` | character | Home team: alternative name: the native-script name of some Japanese clubs (for example 宇都宮ブレックス for Utsunomiya Brex), blank for most clubs; for players it usually equals nameFullLocal. Despite the field name it is not always Latin script. Null on some rows. |
| `home_code_latin` | character | Home team: three-letter club code (for example BRE); null or empty for some clubs. |
| `home_code_local` | character | Home team: alternative three-letter club code (for example TPP); null for most clubs. A club may have only one of codeLatin / codeLocal. |
| `home_score` | integer | Home team final points (the wire sends a numeric string; cast to Int64); null before the game is played. |
| `home_result_place` | integer | Home team: result place: 1 = winner, 2 = loser. The /matches list holds null here for fixtures not yet played; /matches/{fixtureId} is sampled for finished games only. |
| `home_draw` | logical | Home team: true when the game has no winner YET or ever: true on every unplayed competitor, false on every finished one (a tie has never happened). |
| `away_team_id` | character | Away team: teamId: DataCore team (entity) UUID v1 (key space: EASL teams, the same id as `entityId`; joins /teams, /standings team.id, fixture competitors, box and play-by-play teams). Not a player id. |
| `away_name` | character | Away team: display name. Despite the field name this is the English / Latin-script name used on the league site. |
| `away_name_native` | character | Away team: alternative name: the native-script name of some Japanese clubs (for example 宇都宮ブレックス for Utsunomiya Brex), blank for most clubs; for players it usually equals nameFullLocal. Despite the field name it is not always Latin script. Null on some rows. |
| `away_code_latin` | character | Away team: three-letter club code (for example BRE); null or empty for some clubs. |
| `away_code_local` | character | Away team: alternative three-letter club code (for example TPP); null for most clubs. A club may have only one of codeLatin / codeLocal. |
| `away_score` | integer | Away team final points (the wire sends a numeric string; cast to Int64); null before the game is played. |
| `away_result_place` | integer | Away team: result place: 1 = winner, 2 = loser. The /matches list holds null here for fixtures not yet played; /matches/{fixtureId} is sampled for finished games only. |
| `away_draw` | logical | Away team: true when the game has no winner YET or ever: true on every unplayed competitor, false on every finished one (a tie has never happened). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#easl_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The server has nothing for the request (an empty list or leaders object, a team or player without games, a 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown fixture is the server's HTTP 500), an error envelope, a game that is not finished or whose box score or play-by-play is incomplete, a stalled sync on the active season, or a body for another game, season, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed id or a season that is not one of the server's END years (raised before the request that would use it). |

### Example {#easl_schedule-example}

```python
easl_schedule(season=2026)
```

## easl_game

Header of one finished fixture: teams, final score, status, stage, pool, venue and attendance.

**Endpoint URL:** `GET https://easl.joshricha.com/api/v1/matches/{fixture_id}`

**Valid URL:** [https://easl.joshricha.com/api/v1/matches/10af8fd4-106e-11f1-960b-5798087c7aa3](https://easl.joshricha.com/api/v1/matches/10af8fd4-106e-11f1-960b-5798087c7aa3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `fixture_id` | `fixture_id` |  | `Y` |  | DataCore fixture (game) UUID v1, e.g. '10af8fd4-106e-11f1-960b-5798087c7aa3' (the 2025-26 EASL Championship): the 'id' of easl_schedule rows. Only finished (CONFIRMED) games are served; a malformed id is a ValueError. |

### Returns {#easl_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Always null here: the game route carries no season; join season_id to easl_seasons. |
| `season_id` | character | Always null here: the game route carries no season. |
| `fixture_id` | character | fixtureId: DataCore fixture (game) UUID v1 (key space: EASL games; the {fixtureId} of every /matches route). |
| `fixture_number` | integer | Fixture number YYSSNNN: two-digit start year, two-digit end year, three-digit sequence (2627001 = first game of 2026-27, 2526003 = third of 2025-26). Null on every 2023-24 fixture. |
| `fixture_type` | character | REGULAR (group stage), PLAYOFF (quarter- and semifinals) or FINAL (championship and third-place games). Missing from the DRAFT placeholders. |
| `status` | character | CONFIRMED = finished with a final box score; SCHEDULED = future; DRAFT = a placeholder with no teams or time. |
| `fixture_name` | character | Fixture name for special games ('EASL Championship', 'Semifinals 2', 'EASL Tip-Off 2026 Cebu'); null for ordinary group games. |
| `start_time_utc` | character | Scheduled tip-off in UTC, 'YYYY-MM-DDTHH:MM:SS' with no zone marker. The /matches list holds null here on DRAFT fixtures; the single-fixture sample is a finished game. |
| `start_time_local` | character | Scheduled tip-off in the venue's local time, 'YYYY-MM-DDTHH:MM:SS' with no zone (19:10 local = 10:10 UTC in Tokyo). DRAFT fixtures carry a date at 00:00:00. |
| `attendance` | integer | Announced attendance of the game; null when not recorded. |
| `live_data_available` | logical | Whether live stats were collected for the game (present on a minority of fixtures; meaning unverified). Absent on some rows. |
| `live_video_available` | logical | Whether live video was available (false on all 146 fixtures of the four seasons). |
| `pool_code` | character | Pool (group) id: a 30-character hex string, equal to pool.id; null on playoff and DRAFT fixtures. |
| `pool_id` | character | Pool id: a 30-character hex string; null for playoff games ('Unknown Pool'). |
| `pool_name` | character | Pool name: 'Group A', 'Group B', 'Group C' or the placeholder 'Unknown Pool'. |
| `pool_abb` | character | Pool abbreviation (A, B, C). Playoff and DRAFT fixtures of the /matches list carry the placeholder pool, whose abb is empty or 'N/A'. |
| `stage_id` | character | Stage reference '<season UUID>:<stage code>' on team-fixture rows only (its first half scopes the fixture to a season); null on the other fixture routes, which send the stage_code, stage_name and stage_order trio. |
| `stage_code` | character | Stage id: a 30-character hex string. Absent on some rows. |
| `stage_name` | character | Stage name: 'Group Stage', 'Final Four' or 'EASL Playoffs'. Absent on some rows. |
| `stage_order` | integer | Ordinal of the stage within the season (1 = Group Stage, 2 = EASL Playoffs). Absent on some rows. |
| `venue_name` | character | Arena name. Absent on some rows. |
| `venue_name_latin` | character | Venue name in Latin script; null on most rows. |
| `venue_abbreviation_latin` | character | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (String). |
| `venue_abbreviation_local` | character | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (String). |
| `venue_alternate_name_latin` | character | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (String). |
| `venue_country_code` | character | ISO 3166-1 alpha-3 country code of the arena: JPN, TWN, PHL, KOR, MAC, HKG, MNG, CHN. The /matches list holds null here on DRAFT fixtures (empty venue). |
| `venue_city` | character | City of the arena. |
| `venue_state` | character | State or province of the arena (Philippine games, for example Cebu). |
| `home_team_id` | character | Home team: teamId: DataCore team (entity) UUID v1 (key space: EASL teams, the same id as `entityId`; joins /teams, /standings team.id, fixture competitors, box and play-by-play teams). Not a player id. |
| `home_name` | character | Home team: display name. Despite the field name this is the English / Latin-script name used on the league site. |
| `home_name_native` | character | Home team: alternative name: the native-script name of some Japanese clubs (for example 宇都宮ブレックス for Utsunomiya Brex), blank for most clubs; for players it usually equals nameFullLocal. Despite the field name it is not always Latin script. Null on some rows. |
| `home_code_latin` | character | Home team: three-letter club code (for example BRE); null or empty for some clubs. |
| `home_code_local` | character | Home team: alternative three-letter club code (for example TPP); null for most clubs. A club may have only one of codeLatin / codeLocal. |
| `home_score` | integer | Home team final points (the wire sends a numeric string; cast to Int64); null before the game is played. |
| `home_result_place` | integer | Home team: result place: 1 = winner, 2 = loser. The /matches list holds null here for fixtures not yet played; /matches/{fixtureId} is sampled for finished games only. |
| `home_draw` | logical | Home team: true when the game has no winner YET or ever: true on every unplayed competitor, false on every finished one (a tie has never happened). |
| `away_team_id` | character | Away team: teamId: DataCore team (entity) UUID v1 (key space: EASL teams, the same id as `entityId`; joins /teams, /standings team.id, fixture competitors, box and play-by-play teams). Not a player id. |
| `away_name` | character | Away team: display name. Despite the field name this is the English / Latin-script name used on the league site. |
| `away_name_native` | character | Away team: alternative name: the native-script name of some Japanese clubs (for example 宇都宮ブレックス for Utsunomiya Brex), blank for most clubs; for players it usually equals nameFullLocal. Despite the field name it is not always Latin script. Null on some rows. |
| `away_code_latin` | character | Away team: three-letter club code (for example BRE); null or empty for some clubs. |
| `away_code_local` | character | Away team: alternative three-letter club code (for example TPP); null for most clubs. A club may have only one of codeLatin / codeLocal. |
| `away_score` | integer | Away team final points (the wire sends a numeric string; cast to Int64); null before the game is played. |
| `away_result_place` | integer | Away team: result place: 1 = winner, 2 = loser. The /matches list holds null here for fixtures not yet played; /matches/{fixtureId} is sampled for finished games only. |
| `away_draw` | logical | Away team: true when the game has no winner YET or ever: true on every unplayed competitor, false on every finished one (a tie has never happened). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#easl_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The server has nothing for the request (an empty list or leaders object, a team or player without games, a 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown fixture is the server's HTTP 500), an error envelope, a game that is not finished or whose box score or play-by-play is incomplete, a stalled sync on the active season, or a body for another game, season, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed id or a season that is not one of the server's END years (raised before the request that would use it). |

### Example {#easl_game-example}

```python
easl_game(fixture_id='10af8fd4-106e-11f1-960b-5798087c7aa3')
```

## easl_pbp

Play-by-play of a finished game in delivered order, with the running score and the points each event scored.

**Endpoint URL:** `GET https://easl.joshricha.com/api/v1/matches/{fixture_id}/playbyplay`

**Valid URL:** [https://easl.joshricha.com/api/v1/matches/10af8fd4-106e-11f1-960b-5798087c7aa3/playbyplay](https://easl.joshricha.com/api/v1/matches/10af8fd4-106e-11f1-960b-5798087c7aa3/playbyplay)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `fixture_id` | `fixture_id` |  | `Y` |  | DataCore fixture (game) UUID v1, e.g. '10af8fd4-106e-11f1-960b-5798087c7aa3' (the 2025-26 EASL Championship): the 'id' of easl_schedule rows. Only finished (CONFIRMED) games are served; a malformed id is a ValueError. |

### Returns {#easl_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `fixture_id` | character | DataCore fixture (game) UUID v1 from the request. |
| `event_number` | integer | 1-based position of the event in the delivered array, which is the only usable order (sequence is not monotonic). |
| `sequence` | integer | Event sequence number from the scorer's system: NOT in game order (the delivered array order is), and not always unique within a game (35 of 106 games repeat values). |
| `period_id` | integer | Period: 1-4 regulation, 11 = first overtime (12 = second overtime appears in team totals). |
| `period_label` | character | Q1-Q4 for periods 1-4; OT1, OT2 ... for overtimes (the server numbers the first overtime 11 in team totals; periods 5-10 are read as OT{n-4}, unverified). |
| `event_time` | character | When the event was logged, wall-clock 'YYYY-MM-DDTHH:MM:SS.mmm' in UTC with no zone marker. There is NO game clock (time remaining) in this feed. |
| `received_time` | integer | When the event was received upstream, Unix epoch MILLISECONDS (UTC): a median 1.6 s after eventTime (5th to 95th percentile -0.6 s to +55 s over 30,874 events); meaning unverified. |
| `description` | character | Event text. Shots read '<2pt\|3pt> <shot type> missed[ (modifier)]' for EVERY field-goal attempt, made or not: the word 'missed' is always there and the result is the change of `scores` (see the divergence notes). Shot types: Jump shot, Layup, Driving layup, Pull-up jump shot, Floating jump shot, Tip-in, Fadeaway, Step-back jump shot, Turnaround jump shot, Hook shot, Dunk, Reverse layup, Alley-oop, Alley-oop dunk, Euro step (none on 23 rows: '3pt missed'). Modifiers: in the paint, second chance, from turnover, fast break, blocked. Free throws read 'Free throw K of M made' (M = 1, 2 or 3) for every attempt, also always 'made'. Other events (complete list): Assist; Block; Steal; Substitution in; Substitution out; Defensive rebound; Defensive rebound (team); Offensive rebound; Offensive rebound (team); Foul; Foul (N FT); Foul drawn; Foul drawn (N FT); Personal foul; Personal foul (N FT); Personal foul (shooting); Personal foul (shooting) (N FT); Offensive foul; Offensive foul (charge); Technical foul; Technical foul (N FT); Bench technical foul; Bench technical foul (N FT); Coach technical foul; Coach technical foul (N FT); Unsportsmanlike foul; Unsportsmanlike foul (N FT); Disqualifying foul (N FT); Jump ball won; Jump ball won (held ball); Jump ball lost; Full timeout; Short timeout; Media timeout; Commercial timeout; Officials timeout; Officials timeout (mandatory); Coach's challenge (pending); Coach's challenge (complete) - upheld; Coach's challenge (complete) - overturned; Instant replay review (complete) - upheld; Instant replay review (complete) - overturned; Turnover - Bad pass; Ball handling; Traveling; Out of bounds; Offensive; Backcourt violation; Double dribble; Carrying; Offensive goaltending; 3 second violation; 5 second violation; 5 second violation (team); 8 second violation (team); Shot clock violation (team); Other; Other (team). N FT = free throws awarded. |
| `team_id` | character | DataCore team UUID v1 of the acting team; null on events with no team. |
| `player_id` | character | DataCore person UUID v1 of the acting player; null for team events (team rebounds, timeouts). |
| `player_name` | character | Player display name in 'FAMILY Given' form with the family name upper-cased (OSAKABE Taiki), in Latin script. Absent on some rows. |
| `player_name_latin` | character | Alternative name: equal to nameFullLocal for 257 of 552 players, otherwise null; never non-Latin in the observed data. Absent on some rows. |
| `jersey_number` | character | Jersey number as a string; null on one event. Absent on some rows. |
| `team_score` | integer | The event team's running score as of this event (the running score lags the true score on a minority of events); null on events with no team. |
| `opponent_score` | integer | The other team's running score as of this event; null on events with no team. |
| `points_scored` | integer | Points the event team gained since the previous event (change of its running score): the made shot's value. The description always says 'missed', so count points here, not strings; the running score lags on some games, which shifts points to a later event. |
| `team1_id` | character | First team of the body's teams array (the fixture's competitors order, arbitrary home/away); team1_score belongs to it. |
| `team1_score` | integer | Running score of team1_id as of this event. |
| `team2_id` | character | Second team of the body's teams array; team2_score belongs to it. |
| `team2_score` | integer | Running score of team2_id as of this event. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#easl_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The server has nothing for the request (an empty list or leaders object, a team or player without games, a 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown fixture is the server's HTTP 500), an error envelope, a game that is not finished or whose box score or play-by-play is incomplete, a stalled sync on the active season, or a body for another game, season, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed id or a season that is not one of the server's END years (raised before the request that would use it). |

### Example {#easl_pbp-example}

```python
easl_pbp(fixture_id='10af8fd4-106e-11f1-960b-5798087c7aa3')
```

## easl_standings

Group-stage standings of one season: rank within the pool, record, splits, points and the last five results.

**Endpoint URL:** `GET https://easl.joshricha.com/api/v1/standings`

**Valid URL:** [https://easl.joshricha.com/api/v1/standings?season=2026](https://easl.joshricha.com/api/v1/standings?season=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season END year (2026 = EASL 25-26; the four editions are 2024 to 2027). easl_runtime reads easl_seasons and maps it to the DataCore seasonId. Default: the ACTIVE season. |

### Returns {#easl_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = EASL 25-26), from the request. |
| `season_id` | character | DataCore season UUID v1, from the request (the standings rows carry none). |
| `standing_id` | character | Standings row id: a DataCore UUID v1 for the (season, pool, team) row (joins nothing else). |
| `position` | integer | Rank within the pool, 1-4. |
| `team_id` | character | teamId: DataCore team (entity) UUID v1 (key space: EASL teams, the same id as `entityId`; joins /teams, /standings team.id, fixture competitors, box and play-by-play teams). Not a player id. |
| `team_name` | character | Display name. Despite the field name this is the English / Latin-script name used on the league site. |
| `team_code` | character | Three-letter club code; null for some clubs. |
| `pool_id` | character | Pool id: a 30-character hex string (equals a fixture's poolCode). |
| `pool_name` | character | Pool name: 'Group A', 'Group B' or 'Group C'. |
| `pool_abb` | character | Pool abbreviation: A, B or C. Null on some rows. |
| `live` | logical | True while the standing is recomputed live during games (false on all six snapshots checked). |
| `latest` | logical | True on the newest standings snapshot (false on all six snapshots checked, including the active season's). |
| `game_played` | integer | Games played (group stage only: playoff games are not counted, although lastFiveMatches includes them). |
| `game_played_home` | integer | Games played at home. |
| `game_played_away` | integer | Games played away. |
| `wins` | integer | Wins: all group-stage games on the standings route, all games on the season statistics routes. |
| `wins_home` | integer | Wins in the team's home games. |
| `wins_away` | integer | Wins in the team's away games. |
| `losses` | integer | Losses: all group-stage games on the standings route, all games on the season statistics routes. |
| `losses_home` | integer | Losses in the team's home games. |
| `losses_away` | integer | Losses in the team's away games. |
| `draws` | integer | Draws (0: basketball has none). |
| `draws_home` | integer | Home draws (0). |
| `draws_away` | integer | Away draws (0). |
| `win_percentage` | numeric | Winning share as a 0-1 ratio rounded to 2 decimals (0.83 = 5-1), NOT a percentage. |
| `win_percentage_home` | numeric | Home winning share, 0-1 ratio. |
| `win_percentage_away` | numeric | Away winning share, 0-1 ratio. |
| `points_for` | integer | Points the team scored over its group-stage games (points_against is the points it conceded). |
| `points_against` | integer | Points allowed. |
| `point_difference` | integer | Points for minus points against. |
| `point_difference_home` | integer | Point difference in home games. |
| `point_difference_away` | integer | Point difference in away games. |
| `steak` | integer | Current winning streak in games (the key is misspelled 'steak'); 0 after a loss. Never negative: losing streaks are not tracked. |
| `streak_home` | integer | Current winning streak in home games; 0 after a loss. |
| `streak_away` | integer | Current winning streak in away games; 0 after a loss. |
| `last_five` | character | Results of the team's last five games as a comma-separated string, most recent first (W win, L loss). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#easl_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The server has nothing for the request (an empty list or leaders object, a team or player without games, a 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown fixture is the server's HTTP 500), an error envelope, a game that is not finished or whose box score or play-by-play is incomplete, a stalled sync on the active season, or a body for another game, season, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed id or a season that is not one of the server's END years (raised before the request that would use it). |

### Example {#easl_standings-example}

```python
easl_standings(season=2026)
```

## easl_leaders

Tournament top-5 player leaders in nine categories (points, rebounds, assists, steals, blocks, shooting).

**Endpoint URL:** `GET https://easl.joshricha.com/api/v1/leaders`

**Valid URL:** [https://easl.joshricha.com/api/v1/leaders?season=2026](https://easl.joshricha.com/api/v1/leaders?season=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season END year (2026 = EASL 25-26; the four editions are 2024 to 2027). easl_runtime reads easl_seasons and maps it to the DataCore seasonId. Default: the ACTIVE season. |

### Returns {#easl_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = EASL 25-26): stamped by easl_runtime from the request, null when the body is parsed bare. |
| `season_id` | character | DataCore season UUID v1 of the leader row. |
| `category` | character | Leader category, the wire key of the group: pointsPerGame, reboundsPerGame, assistsPerGame, stealsPerGame, blocksPerGame, pointsThreeMade, pointsThreePercentage, fieldGoalsPercentage or freeThrowsPercentage. |
| `rank` | integer | Rank inside the category, 1 = best, in the delivered order (the server lists the top 5). |
| `value` | numeric | The leader's value in the category: a per-game average, a count (pointsThreeMade) or a percentage (0-100). Float64. |
| `player_id` | character | playerId: DataCore person UUID v1 (key space: EASL persons, the same id as the box score's `personId`; joins /players, leaders and play-by-play players). Not a team id. |
| `player_name` | character | Player display name in 'FAMILY Given' form with the family name upper-cased (OSAKABE Taiki), in Latin script. |
| `player_name_latin` | character | Alternative name: equal to nameFullLocal for 257 of 552 players, otherwise null; never non-Latin in the observed data. |
| `jersey_number` | character | Jersey number as a string ('00', '0' and '75' are distinct). |
| `position` | character | Playing position: G, F, C, PG, SG, SF, PF or GF (guard-forward). |
| `player_status` | character | Registration state of the player in the database: ACTIVE, INACTIVE or UNREGISTERED. |
| `team_id` | character | teamId: DataCore team (entity) UUID v1 (key space: EASL teams, the same id as `entityId`; joins /teams, /standings team.id, fixture competitors, box and play-by-play teams). Not a player id. |
| `team_name` | character | Display name. Despite the field name this is the English / Latin-script name used on the league site. |
| `team_name_native` | character | Alternative name: the native-script name of some Japanese clubs (for example 宇都宮ブレックス for Utsunomiya Brex), blank for most clubs; for players it usually equals nameFullLocal. Despite the field name it is not always Latin script. Null on some rows. |
| `team_code_latin` | character | Three-letter club code (for example BRE); null or empty for some clubs. |
| `team_code_local` | character | Alternative three-letter club code (for example TPP); null for most clubs. A club may have only one of codeLatin / codeLocal. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#easl_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The server has nothing for the request (an empty list or leaders object, a team or player without games, a 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown fixture is the server's HTTP 500), an error envelope, a game that is not finished or whose box score or play-by-play is incomplete, a stalled sync on the active season, or a body for another game, season, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed id or a season that is not one of the server's END years (raised before the request that would use it). |

### Example {#easl_leaders-example}

```python
easl_leaders(season=2026)
```
