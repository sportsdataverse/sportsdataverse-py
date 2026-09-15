---
title: NFL — NFL.com API
sidebar_label: NFL.com API
description: "NFL — NFL.com API — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
---
# NFL — NFL.com API

`sportsdataverse.nfl` — 15 endpoints.

## `nfl_standings`

GET /football/v2/standings — one row per team standing across the returned week(s).

**Endpoint URL:** `GET https://api.nfl.com/football/v2/standings`

**Valid URL:** [https://api.nfl.com/football/v2/standings?season=2024&seasonType=REG&week=18](https://api.nfl.com/football/v2/standings?season=2024&seasonType=REG&week=18)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `week` | `week` |  |  | `Y` | Week number within the season. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | character | NFL.com Shield GUID for the team. |
| `team_current_logo` | character | Templated URL of the team's current logo. |
| `team_full_name` | character | Full team name (e.g. "Arizona Cardinals"). |
| `clinched_bye` | logical | Whether the team has clinched a first-round playoff bye. |
| `clinched_division` | logical | Whether the team has clinched its division. |
| `clinched_eliminated` | logical | Whether the team has been mathematically eliminated from playoff contention. |
| `clinched_home_field` | logical | Whether the team has clinched home-field advantage throughout the playoffs. |
| `clinched_playoff` | logical | Whether the team has clinched a playoff berth. |
| `clinched_wild_card` | logical | Whether the team has clinched a wild-card playoff berth. |
| `close_games_wins` | integer | Wins in close games (decided by one score / 8 points or fewer). |
| `close_games_losses` | integer | Losses in close games (decided by one score / 8 points or fewer). |
| `close_games_ties` | integer | Ties in close games. |
| `conference_wins` | integer | Wins against conference (AFC/NFC) opponents. |
| `conference_win_pct` | numeric | Win percentage against conference opponents. |
| `conference_losses` | integer | Losses against conference opponents. |
| `conference_ties` | integer | Ties against conference opponents. |
| `conference_rank` | integer | Standings rank within the conference. |
| `conference_points_for` | integer | Points scored in conference games. |
| `conference_points_against` | integer | Points allowed in conference games. |
| `division_wins` | integer | Wins against division opponents. |
| `division_win_pct` | numeric | Win percentage against division opponents. |
| `division_losses` | integer | Losses against division opponents. |
| `division_ties` | integer | Ties against division opponents. |
| `division_rank` | integer | Standings rank within the division. |
| `division_points_for` | integer | Points scored in division games. |
| `division_points_against` | integer | Points allowed in division games. |
| `home_wins` | integer | Wins in home games. |
| `home_win_pct` | numeric | Win percentage in home games. |
| `home_losses` | integer | Losses in home games. |
| `home_ties` | integer | Ties in home games. |
| `home_points_for` | integer | Points scored in home games. |
| `home_points_against` | integer | Points allowed in home games. |
| `last5_wins` | integer | Wins over the last five games. |
| `last5_win_pct` | numeric | Win percentage over the last five games. |
| `last5_losses` | integer | Losses over the last five games. |
| `last5_ties` | integer | Ties over the last five games. |
| `last5_points_for` | integer | Points scored over the last five games. |
| `last5_points_against` | integer | Points allowed over the last five games. |
| `overall_games` | integer | Total games played. |
| `overall_wins` | integer | Total wins. |
| `overall_win_pct` | numeric | Overall win percentage. |
| `overall_losses` | integer | Total losses. |
| `overall_ties` | integer | Total ties. |
| `overall_points_for` | integer | Total points scored. |
| `overall_points_against` | integer | Total points allowed. |
| `overall_streak_type` | character | Current streak type ("W" for winning, "L" for losing). |
| `overall_streak_length` | integer | Length of the current win/loss streak. |
| `road_wins` | integer | Wins in road (away) games. |
| `road_win_pct` | numeric | Win percentage in road games. |
| `road_losses` | integer | Losses in road games. |
| `road_ties` | integer | Ties in road games. |
| `road_points_for` | integer | Points scored in road games. |
| `road_points_against` | integer | Points allowed in road games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_standings(season=2024, season_type='REG', week=18)
```

_Last validated n/a._

## `nfl_rosters`

GET /football/v2/rosters — one row per team roster for the season.

**Endpoint URL:** `GET https://api.nfl.com/football/v2/rosters`

**Valid URL:** [https://api.nfl.com/football/v2/rosters?season=2024](https://api.nfl.com/football/v2/rosters?season=2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `teamId` | `team_id` |  |  | `Y` | Shield team uuid (the ``id`` of a team in the teams history listing). Returns just that team's roster -- one roster (~37 KB) instead of all 32 (~1.2 MB). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season` | integer | Season (year) of the roster. |
| `season_type` | character | Season type code (PRE, REG, or POST). |
| `team_id` | character | NFL.com Shield GUID for the team. |
| `team_abbreviation` | character | Team abbreviation (e.g. "ARI"). |
| `team_full_name` | character | Full team name (e.g. "Arizona Cardinals"). |
| `team_conference_abbr` | character | Conference abbreviation (AFC or NFC). |
| `team_conference_full_name` | character | Full conference name (e.g. "National Football Conference"). |
| `team_current_logo` | character | Templated URL of the team's current logo. |
| `team_division_full_name` | character | Full division name (e.g. "NFC West"). |
| `team_league` | character | League name ("National Football League"). |
| `team_location` | character | Team location / city (e.g. "Arizona"). |
| `team_nick_name` | character | Team nickname (e.g. "Cardinals"). |
| `team_venues` | character | JSON-stringified array of the team's venue objects (id, name, etc.). |
| `persons` | character | JSON-stringified array of roster player objects for the team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_rosters(season=2024)
```

_Last validated n/a._

## `nfl_teams_history`

GET /football/v2/teams/history — one row per team for a season.

**Endpoint URL:** `GET https://api.nfl.com/football/v2/teams/history`

**Valid URL:** [https://api.nfl.com/football/v2/teams/history?season=2024](https://api.nfl.com/football/v2/teams/history?season=2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | character | NFL.com Shield GUID for the team (or Pro Bowl team entry). |
| `season` | integer | Season (year) the team record applies to. |
| `abbreviation` | character | Team abbreviation (e.g. "AFC", "ARI"). |
| `full_name` | character | Full team name (e.g. "AFC Pro Bowl Team", "Arizona Cardinals"). |
| `team_type` | character | Team type classification (e.g. "TEAM", "PRO" for Pro Bowl teams). |
| `conference_abbr` | character | Conference abbreviation (AFC or NFC). |
| `conference_full_name` | character | Full conference name (e.g. "American Football Conference"). |
| `current_logo` | character | Templated URL of the team's current logo. |
| `division_full_name` | character | Full division name (e.g. "NFC West"). |
| `league` | character | League name ("National Football League"). |
| `location` | character | Team location / city. |
| `nick_name` | character | Team nickname. |
| `venues` | character | JSON-stringified array of the team's venue objects (empty for non-club entries). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_teams_history(season=2024)
```

_Last validated n/a._

## `nfl_team`

GET /football/v2/teams/{team_id} — single-team detail (one row).

**Endpoint URL:** `GET https://api.nfl.com/football/v2/teams/{team_id}`

**Valid URL:** [https://api.nfl.com/football/v2/teams/10403800-517c-7b8c-65a3-c61b95d86123](https://api.nfl.com/football/v2/teams/10403800-517c-7b8c-65a3-c61b95d86123)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | character | NFL.com Shield GUID for the team. |
| `bio` | character | Team biography / description text (may be null). |
| `current_background` | character | Templated URL of the team's current background image. |
| `current_coach` | character | Name of the team's current head coach. |
| `current_logo` | character | Templated URL of the team's current logo. |
| `primary_color` | character | Team primary color (hex code). |
| `secondary_color` | character | Team secondary color (hex code). |
| `year_established` | integer | Year the franchise was established. |
| `full_name` | character | Full team name (e.g. "Arizona Cardinals"). |
| `nfl_shop_url` | character | URL to the team's NFL Shop storefront. |
| `official_website_url` | character | URL of the team's official website. |
| `owners` | character | Name(s) of the team's owner(s). |
| `team_type` | character | Team type classification (e.g. "TEAM"). |
| `socials` | character | JSON-stringified array of the team's social-media links (platform, link). |
| `vll_channel_callsign` | character | Verizon Live League (VLL) channel call sign for the team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_team(team_id='10403800-517c-7b8c-65a3-c61b95d86123')
```

_Last validated n/a._

## `nfl_weeks`

GET /football/v2/weeks/season/{season}/seasonType/{season_type} — week calendar (one row per week).

**Endpoint URL:** `GET https://api.nfl.com/football/v2/weeks/season/{season}/seasonType/{season_type}`

**Valid URL:** [https://api.nfl.com/football/v2/weeks/season/2024/seasonType/REG](https://api.nfl.com/football/v2/weeks/season/2024/seasonType/REG)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | season path parameter. |
| `season_type` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season` | integer | Season (year) of the week. |
| `season_type` | character | Season type code (PRE, REG, or POST). |
| `week` | integer | Week number within the season type. |
| `bye_teams` | character | JSON-stringified array of teams on bye during the week (empty when none). |
| `date_begin` | character | Start date of the week (YYYY-MM-DD). |
| `date_end` | character | End date of the week (YYYY-MM-DD). |
| `week_type` | character | Week type code (e.g. PRE, REG, WC, DIV, CONF, SB). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_weeks(season=2024, season_type='REG')
```

_Last validated n/a._

## `nfl_weeks_by_date`

GET /football/v2/weeks/date/{YYYY-MM-DD} — the week containing a date (one row).

**Endpoint URL:** `GET https://api.nfl.com/football/v2/weeks/date/{date}`

**Valid URL:** [https://api.nfl.com/football/v2/weeks/date/2024-09-08](https://api.nfl.com/football/v2/weeks/date/2024-09-08)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  | `Y` |  | date path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season` | integer | Season (year) the date falls in. |
| `season_type` | character | Season type code (PRE, REG, or POST). |
| `week` | integer | Week number that the queried date falls within. |
| `bye_teams` | character | JSON-stringified array of teams on bye during the week (empty when none). |
| `date_begin` | character | Start date of the week (YYYY-MM-DD). |
| `date_end` | character | End date of the week (YYYY-MM-DD). |
| `week_type` | character | Week type code (e.g. PRE, REG, WC, DIV, CONF, SB). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_weeks_by_date(date='2024-09-08')
```

_Last validated n/a._

## `nfl_combine_profiles`

GET /football/v2/combine/profiles — one row per combine prospect.

**Endpoint URL:** `GET https://api.nfl.com/football/v2/combine/profiles`

**Valid URL:** [https://api.nfl.com/football/v2/combine/profiles?year=2024](https://api.nfl.com/football/v2/combine/profiles?year=2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | character | NFL.com Shield GUID for the combine profile. |
| `year` | integer | Combine / draft class year. |
| `person_id` | character | NFL.com Shield GUID of the prospect. |
| `person_display_name` | character | Prospect's full display name. |
| `person_esb_id` | character | Prospect's Elias Sports Bureau (ESB) identifier. |
| `person_first_name` | character | Prospect's first name. |
| `person_last_name` | character | Prospect's last name. |
| `person_hometown` | character | Prospect's hometown (city, state). |
| `person_college_names` | character | JSON-stringified array of the prospect's college(s). |
| `arm_length` | numeric | Arm length measured at the combine, in inches. |
| `athleticism_score` | numeric | Composite athleticism score. |
| `bench_press` | numeric | Bench-press measurement object (flattened; null when no measurement). |
| `broad_jump` | numeric | Broad-jump measurement object (flattened; null when no measurement). |
| `bio` | character | HTML biography / career summary for the prospect. |
| `college_class` | character | Prospect's college class (e.g. Senior, Junior). |
| `draft_grade` | numeric | Numeric draft grade assigned to the prospect. |
| `draft_projection` | character | Draft projection text (e.g. "Priority free agent"). |
| `forty_yard_dash` | numeric | 40-yard-dash measurement object (flattened; null when no measurement). |
| `grade` | numeric | Overall prospect grade. |
| `hand_size` | integer | Hand size measured at the combine, in inches. |
| `headshot` | character | Templated URL of the prospect's headshot image. |
| `height` | integer | Prospect height, in inches. |
| `nfl_comparison` | character | Player the prospect is most comparable to. |
| `overview` | character | HTML scouting overview of the prospect. |
| `production_score` | numeric | Composite college-production score. |
| `profile_author` | character | Author of the scouting profile (e.g. "Lance Zierlein"). |
| `pro_forty_yard_dash` | numeric | Pro-day 40-yard-dash measurement object (flattened; null when no measurement). |
| `sixty_yard_shuttle` | numeric | 60-yard-shuttle measurement object (flattened; null when no measurement). |
| `size_score` | numeric | Composite size score (flattened; null when not scored). |
| `sources_tell_us` | character | "Sources tell us" scouting commentary text. |
| `strengths` | character | HTML list of the prospect's strengths. |
| `ten_yard_split` | numeric | 10-yard-split measurement object (flattened; null when no measurement). |
| `three_cone_drill` | numeric | Three-cone-drill measurement object (flattened; null when no measurement). |
| `twenty_yard_shuttle` | numeric | 20-yard-shuttle measurement object (flattened; null when no measurement). |
| `weaknesses` | character | HTML list of the prospect's weaknesses. |
| `combine_attendance` | logical | Whether the prospect attended the combine. |
| `position` | character | Prospect's position abbreviation (e.g. "CB"). |
| `position_group` | character | Prospect's position group (e.g. "DB"). |
| `vertical_jump` | numeric | Vertical-jump measurement object (flattened; null when no measurement). |
| `weight` | integer | Prospect weight, in pounds. |
| `pro_forty_yard_dash_designation` | character | Designation for the pro-day 40-yard dash (OFFICIAL or UNOFFICIAL). |
| `pro_forty_yard_dash_seconds` | numeric | Pro-day 40-yard-dash time, in seconds. |
| `bench_press_designation` | character | Designation for the bench-press result (OFFICIAL or UNOFFICIAL). |
| `bench_press_repetitions` | integer | Number of 225 lb bench-press repetitions. |
| `broad_jump_designation` | character | Designation for the broad-jump result (OFFICIAL or UNOFFICIAL). |
| `broad_jump_inches` | integer | Broad-jump distance, in inches. |
| `forty_yard_dash_designation` | character | Designation for the 40-yard-dash result (OFFICIAL or UNOFFICIAL). |
| `forty_yard_dash_seconds` | numeric | 40-yard-dash time, in seconds. |
| `ten_yard_split_designation` | character | Designation for the 10-yard-split result (OFFICIAL or UNOFFICIAL). |
| `ten_yard_split_seconds` | numeric | 10-yard-split time, in seconds. |
| `vertical_jump_designation` | character | Designation for the vertical-jump result (OFFICIAL or UNOFFICIAL). |
| `vertical_jump_inches` | integer | Vertical-jump height, in inches. |
| `three_cone_drill_designation` | character | Designation for the three-cone-drill result (OFFICIAL or UNOFFICIAL). |
| `three_cone_drill_seconds` | numeric | Three-cone-drill time, in seconds. |
| `twenty_yard_shuttle_designation` | character | Designation for the 20-yard-shuttle result (OFFICIAL or UNOFFICIAL). |
| `twenty_yard_shuttle_seconds` | numeric | 20-yard-shuttle time, in seconds. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_combine_profiles(year=2024)
```

_Last validated n/a._

## `nfl_draft_picks`

GET /football/v2/draft/picks/report — one row per draft pick.

**Endpoint URL:** `GET https://api.nfl.com/football/v2/draft/picks/report`

**Valid URL:** [https://api.nfl.com/football/v2/draft/picks/report?year=2024](https://api.nfl.com/football/v2/draft/picks/report?year=2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `year` | integer | Draft year. |
| `draft_round` | integer | Round of the draft in which the pick was made. |
| `draft_position` | integer | Pick position within the round. |
| `draft_number_overall` | integer | Overall pick number across the entire draft. |
| `person_id` | character | NFL.com Shield GUID of the drafted player (may be empty until the pick is announced). |
| `pick_is_in` | logical | Whether the pick has officially been submitted / announced. |
| `team_id` | character | NFL.com Shield GUID of the team making the pick. |
| `trade_note` | character | Trade annotation for the pick (e.g. "CAR>CHI" denoting a traded selection). |
| `tweet_sent` | logical | Whether the announcement tweet has been sent for the pick. |
| `tweets_sent` | character | JSON-stringified array of per-account tweet-sent status objects. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_draft_picks(year=2024)
```

_Last validated n/a._

## `nfl_injuries`

GET /football/v2/injuries — one row per injured player.

**Endpoint URL:** `GET https://api.nfl.com/football/v2/injuries`

**Valid URL:** [https://api.nfl.com/football/v2/injuries?season=2024&seasonType=REG&week=1](https://api.nfl.com/football/v2/injuries?season=2024&seasonType=REG&week=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `week` | `week` |  |  | `Y` | Week number within the season. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season` | integer | Season (year) of the injury report. |
| `season_type` | character | Season type code (PRE, REG, or POST). |
| `week` | integer | Week number of the injury report. |
| `team_id` | character | NFL.com Shield GUID of the player's team. |
| `team_current_logo` | character | Templated URL of the team's current logo. |
| `team_full_name` | character | Full team name (e.g. "Atlanta Falcons"). |
| `person_id` | character | NFL.com Shield GUID of the injured player. |
| `person_first_name` | character | Player's first name. |
| `person_common_first_name` | character | Player's common / preferred first name. |
| `person_last_name` | character | Player's last name. |
| `person_display_name` | character | Player's full display name. |
| `person_gsis_id` | character | Player's NFL GSIS identifier (e.g. "00-0036948"). |
| `person_headshot` | character | Templated URL of the player's headshot image. |
| `injuries` | character | JSON-stringified array of detailed injury objects (empty when not specified). |
| `injury_status` | character | Game-status designation (e.g. OUT, DOUBTFUL, QUESTIONABLE). |
| `practices` | character | JSON-stringified array of practice-participation notes. |
| `practice_days` | character | JSON-stringified array of per-day practice status objects (date, status). |
| `practice_status` | character | Most recent practice-participation status (e.g. FULL, LIMITED, DNP). |
| `position` | character | Player's position abbreviation (e.g. "DE"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_injuries(season=2024, season_type='REG', week=1)
```

_Last validated n/a._

## `nfl_game_summaries`

GET /football/v2/stats/live/game-summaries — one row per game (live state).

**Endpoint URL:** `GET https://api.nfl.com/football/v2/stats/live/game-summaries`

**Valid URL:** [https://api.nfl.com/football/v2/stats/live/game-summaries?season=2024&seasonType=REG&week=1](https://api.nfl.com/football/v2/stats/live/game-summaries?season=2024&seasonType=REG&week=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `week` | `week` |  |  | `Y` | Week number within the season. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | NFL.com Shield GUID for the game. |
| `offset` | integer | Live-feed sequence offset for the summary snapshot. |
| `attendance` | integer | Announced game attendance. |
| `clock` | character | Game clock at the snapshot (MM:SS). |
| `distance` | integer | Yards to gain for a first down at the snapshot. |
| `down` | integer | Current down (1-4) at the snapshot. |
| `game_book_url` | character | URL of the official game book image. |
| `is_goal_to_go` | logical | Whether the current situation is goal-to-go. |
| `is_red_zone` | logical | Whether the ball is in the red zone. |
| `phase` | character | Game phase (e.g. PREGAME, INGAME, HALFTIME, FINAL). |
| `quarter` | character | Current period descriptor (e.g. Q1, HALFTIME, END_OF_GAME). |
| `start_time` | character | ISO 8601 kickoff timestamp. |
| `weather` | character | Weather summary string (temperature, humidity, wind). |
| `yard_line` | character | Current line of scrimmage (e.g. "KC 10"). |
| `away_team_team_id` | character | NFL.com Shield GUID of the away team. |
| `away_team_has_possession` | logical | Whether the away team has possession at the snapshot. |
| `away_team_score_q1` | integer | Away team points scored in the first quarter. |
| `away_team_score_q2` | integer | Away team points scored in the second quarter. |
| `away_team_score_q3` | integer | Away team points scored in the third quarter. |
| `away_team_score_q4` | integer | Away team points scored in the fourth quarter. |
| `away_team_score_ot` | integer | Away team points scored in overtime. |
| `away_team_score_total` | integer | Away team total points. |
| `away_team_timeouts_remaining` | integer | Away team timeouts remaining at the snapshot. |
| `away_team_timeouts_used` | integer | Away team timeouts used at the snapshot. |
| `home_team_team_id` | character | NFL.com Shield GUID of the home team. |
| `home_team_has_possession` | logical | Whether the home team has possession at the snapshot. |
| `home_team_score_q1` | integer | Home team points scored in the first quarter. |
| `home_team_score_q2` | integer | Home team points scored in the second quarter. |
| `home_team_score_q3` | integer | Home team points scored in the third quarter. |
| `home_team_score_q4` | integer | Home team points scored in the fourth quarter. |
| `home_team_score_ot` | integer | Home team points scored in overtime. |
| `home_team_score_total` | integer | Home team total points. |
| `home_team_timeouts_remaining` | integer | Home team timeouts remaining at the snapshot. |
| `home_team_timeouts_used` | integer | Home team timeouts used at the snapshot. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_game_summaries(season=2024, season_type='REG', week=1)
```

_Last validated n/a._

## `nfl_weekly_game_details`

GET /football/v2/experience/weekly-game-details — one row per game (bare list).

**Endpoint URL:** `GET https://api.nfl.com/football/v2/experience/weekly-game-details`

**Valid URL:** [https://api.nfl.com/football/v2/experience/weekly-game-details?season=2024&type=REG&week=1](https://api.nfl.com/football/v2/experience/weekly-game-details?season=2024&type=REG&week=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `type` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST (sent as the `type` query param) -- not ESPN's numeric 1/2/3. |
| `week` | `week` |  |  | `Y` | Week number within the season. |
| `includeDriveChart` | `include_drive_chart` |  |  | `Y` | includeDriveChart query parameter. |
| `includeReplays` | `include_replays` |  |  | `Y` | includeReplays query parameter. |
| `includeStandings` | `include_standings` |  |  | `Y` | includeStandings query parameter. |
| `includeTaggedVideos` | `include_tagged_videos` |  |  | `Y` | includeTaggedVideos query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | character | NFL.com Shield GUID for the game. |
| `home_team_id` | character | NFL.com Shield GUID of the home team. |
| `home_team_current_logo` | character | Templated URL of the home team's current logo. |
| `home_team_full_name` | character | Full home-team name (e.g. "Kansas City Chiefs"). |
| `away_team_id` | character | NFL.com Shield GUID of the away team. |
| `away_team_current_logo` | character | Templated URL of the away team's current logo. |
| `away_team_full_name` | character | Full away-team name (e.g. "Baltimore Ravens"). |
| `category` | character | Game category / window (e.g. SNF, MNF, TNF). |
| `date` | character | Game date (YYYY-MM-DD). |
| `time` | character | ISO 8601 kickoff timestamp. |
| `broadcast_info_home_network_channels` | character | JSON-stringified array of broadcast channels in the home market. |
| `broadcast_info_away_network_channels` | character | JSON-stringified array of broadcast channels in the away market. |
| `broadcast_info_international_watch_options` | character | JSON-stringified array of international broadcaster options by country. |
| `broadcast_info_streaming_networks` | character | JSON-stringified array of streaming-network objects. |
| `broadcast_info_territory` | character | Broadcast territory designation (e.g. NATIONAL, REGIONAL). |
| `broadcast_info_audio_networks` | character | JSON-stringified array of audio-broadcast network objects. |
| `game_type` | character | Game type classification (e.g. UNSPECIFIED, REG, WC). |
| `international` | logical | Whether the game is played at an international venue. |
| `neutral_site` | logical | Whether the game is played at a neutral site. |
| `venue_id` | character | NFL.com Shield GUID of the venue. |
| `venue_name` | character | Venue name (e.g. "GEHA Field at Arrowhead Stadium"). |
| `venue_city` | character | Venue city. |
| `venue_country` | character | Venue country. |
| `season` | integer | Season (year) of the game. |
| `season_type` | character | Season type code (PRE, REG, or POST). |
| `status` | character | Game status (e.g. SCHEDULED, INGAME, FINAL). |
| `week` | integer | Week number of the game. |
| `week_type` | character | Week type code (e.g. PRE, REG, WC, DIV, CONF, SB). |
| `external_ids` | character | JSON-stringified array of external game identifiers (elias, gsis, etc.). |
| `ticket_url` | character | Primary ticket-purchase URL for the game. |
| `ticket_vendors` | character | JSON-stringified array of ticket-vendor objects (vendor name, URL). |
| `extensions` | character | JSON-stringified array of extension objects (empty when none). |
| `version` | integer | Record version number. |
| `summary_game_id` | character | NFL.com Shield GUID for the game (from the embedded live summary). |
| `summary_offset` | integer | Live-feed sequence offset for the embedded summary snapshot. |
| `summary_attendance` | integer | Announced game attendance (from the embedded summary). |
| `summary_clock` | character | Game clock at the summary snapshot (MM:SS). |
| `summary_distance` | integer | Yards to gain for a first down at the summary snapshot. |
| `summary_down` | integer | Current down (1-4) at the summary snapshot. |
| `summary_game_book_url` | character | URL of the official game book image (from the embedded summary). |
| `summary_is_goal_to_go` | logical | Whether the situation is goal-to-go at the summary snapshot. |
| `summary_is_red_zone` | logical | Whether the ball is in the red zone at the summary snapshot. |
| `summary_phase` | character | Game phase (e.g. PREGAME, INGAME, HALFTIME, FINAL). |
| `summary_quarter` | character | Current period descriptor (e.g. Q1, HALFTIME, END_OF_GAME). |
| `summary_start_time` | character | ISO 8601 kickoff timestamp (from the embedded summary). |
| `summary_weather` | character | Weather summary string (temperature, humidity, wind). |
| `summary_yard_line` | character | Current line of scrimmage at the summary snapshot (e.g. "KC 10"). |
| `summary_away_team_team_id` | character | NFL.com Shield GUID of the away team (from the embedded summary). |
| `summary_away_team_has_possession` | logical | Whether the away team has possession at the summary snapshot. |
| `summary_away_team_score_q1` | integer | Away team points scored in the first quarter. |
| `summary_away_team_score_q2` | integer | Away team points scored in the second quarter. |
| `summary_away_team_score_q3` | integer | Away team points scored in the third quarter. |
| `summary_away_team_score_q4` | integer | Away team points scored in the fourth quarter. |
| `summary_away_team_score_ot` | integer | Away team points scored in overtime. |
| `summary_away_team_score_total` | integer | Away team total points. |
| `summary_away_team_timeouts_remaining` | integer | Away team timeouts remaining at the summary snapshot. |
| `summary_away_team_timeouts_used` | integer | Away team timeouts used at the summary snapshot. |
| `summary_home_team_team_id` | character | NFL.com Shield GUID of the home team (from the embedded summary). |
| `summary_home_team_has_possession` | logical | Whether the home team has possession at the summary snapshot. |
| `summary_home_team_score_q1` | integer | Home team points scored in the first quarter. |
| `summary_home_team_score_q2` | integer | Home team points scored in the second quarter. |
| `summary_home_team_score_q3` | integer | Home team points scored in the third quarter. |
| `summary_home_team_score_q4` | integer | Home team points scored in the fourth quarter. |
| `summary_home_team_score_ot` | integer | Home team points scored in overtime. |
| `summary_home_team_score_total` | integer | Home team total points. |
| `summary_home_team_timeouts_remaining` | integer | Home team timeouts remaining at the summary snapshot. |
| `summary_home_team_timeouts_used` | integer | Home team timeouts used at the summary snapshot. |
| `drive_chart_game_id` | character | NFL.com Shield GUID for the game (from the drive chart, present when include_drive_chart=true). |
| `drive_chart_offset` | integer | Live-feed sequence offset for the drive-chart snapshot. |
| `drive_chart_drives` | character | JSON-stringified array of drive objects (sequence, team, result, etc.). |
| `drive_chart_plays` | character | JSON-stringified array of play objects within the drive chart. |
| `drive_chart_scoring_summaries` | character | JSON-stringified array of scoring-summary objects (sequence, scores, clock). |
| `replays` | character | JSON-stringified array of replay objects (populated only when include_replays=true). |
| `tagged_videos` | character | JSON-stringified array of tagged-video objects (populated only when include_tagged_videos=true). |
| `away_team_standings` | character | JSON-stringified away-team standings object (populated only when include_standings=true). |
| `home_team_standings` | character | JSON-stringified home-team standings object (populated only when include_standings=true). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_weekly_game_details(season=2024, season_type='REG', week=1)
```

_Last validated n/a._

## `nfl_live_team_statistics`

GET /football/v2/stats/live/team-statistics/{game_id} — one row per side (away, home): the live team box score.

**Endpoint URL:** `GET https://api.nfl.com/football/v2/stats/live/team-statistics/{game_id}`

**Valid URL:** [https://api.nfl.com/football/v2/stats/live/team-statistics/a9a890ed-4feb-11f1-abca-2c54536568a9](https://api.nfl.com/football/v2/stats/live/team-statistics/a9a890ed-4feb-11f1-abca-2c54536568a9)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Shield uuid game id -- the ``id`` column of the week games and weekly game details listings. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | NFL.com Shield GUID for the game. |
| `offset` | integer | Live-feed sequence position the statistics reflect; advances as the game is played. |
| `side` | character | Which side of the game the row belongs to: away or home. |
| `team_id` | character | NFL.com Shield GUID of the team. |
| `defensive_fumbles_forced` | integer | Defense: fumbles forced. |
| `defensive_fumbles_recovered` | integer | Defense: fumbles recovered. |
| `defensive_interceptions` | integer | Defense: interceptions. |
| `defensive_passes_defended` | integer | Defense: passes defended. |
| `defensive_quarterback_hits` | integer | Defense: quarterback hits. |
| `defensive_sacks` | integer | Defense: sacks. |
| `defensive_safeties` | integer | Defense: safeties. |
| `defensive_tackles_combined` | numeric | Defense: tackles combined. |
| `defensive_tackles_for_loss` | numeric | Defense: tackles for loss. |
| `defensive_touchdowns` | integer | Defense: touchdowns. |
| `extra_point_kick_attempts` | integer | Extra points: kick attempts. |
| `extra_point_kick_blocked` | integer | Extra points: kick blocked. |
| `extra_point_kick_made` | integer | Extra points: kick made. |
| `field_goals_attempts` | integer | Field goals: attempts. |
| `field_goals_blocked` | integer | Field goals: blocked. |
| `field_goals_longest_made` | integer | Field goals: longest made. |
| `field_goals_made` | integer | Field goals: made. |
| `first_downs_passing` | integer | First downs: passing. |
| `first_downs_penalty` | integer | First downs: penalty. |
| `first_downs_rushing` | integer | First downs: rushing. |
| `first_downs_total` | integer | First downs: total. |
| `fourth_down_attempts` | integer | Fourth down: attempts. |
| `fourth_down_conversions` | integer | Fourth down: conversions. |
| `fumbles_lost` | integer | Fumbles: lost. |
| `fumbles_made` | integer | Fumbles: made. |
| `fumbles_own_recoveries` | integer | Fumbles: own recoveries. |
| `fumbles_returned_touchdowns` | integer | Fumbles: returned touchdowns. |
| `goal_to_go_attempts` | integer | Goal-to-go: attempts. |
| `goal_to_go_successes` | integer | Goal-to-go: successes. |
| `interceptions_longest_touchdown` | integer | Interceptions: longest touchdown. |
| `interceptions_made` | integer | Interceptions made by the defense. |
| `interceptions_returned` | integer | Interceptions: returned. |
| `interceptions_returned_touchdowns` | integer | Interceptions: returned touchdowns. |
| `interceptions_returned_yards` | integer | Interceptions: returned yards. |
| `kick_returns_longest` | integer | Kickoff returns: longest. |
| `kick_returns_yards_average` | numeric | Kickoff returns: yards average. |
| `kickoffs_in_end_zone` | integer | Kickoffs: in end zone. |
| `kickoffs_made` | integer | Kickoffs: made. |
| `kickoffs_returned` | integer | Kickoffs: returned. |
| `kickoffs_returned_touchdowns` | integer | Kickoffs: returned touchdowns. |
| `kickoffs_returned_yards` | integer | Kickoffs: returned yards. |
| `kickoffs_touchbacks` | integer | Kickoffs: touchbacks. |
| `passing_attempts` | integer | Passing: attempts. |
| `passing_completions` | integer | Passing: completions. |
| `passing_completion_percent` | numeric | Passing: completion percent. |
| `passing_interceptions` | integer | Passing: interceptions. |
| `passing_rating` | numeric | Passing: rating. |
| `passing_sacks` | integer | Passing: sacks. |
| `passing_sack_yards_lost` | numeric | Passing: sack yards lost. |
| `passing_touchdowns` | integer | Passing: touchdowns. |
| `passing_yards` | integer | Passing: yards. |
| `passing_yards_average` | numeric | Passing: yards average. |
| `passing_yards_per_attempt` | numeric | Passing: yards per attempt. |
| `penalties_made` | integer | Penalties: made. |
| `penalties_yards` | integer | Penalties: yards. |
| `punt_returns_longest` | integer | Punt returns: longest. |
| `punt_returns_yards_average` | numeric | Punt returns: yards average. |
| `punts_attempts` | integer | Punting: attempts. |
| `punts_blocked` | integer | Punting: blocked. |
| `punts_inside20` | integer | Punting: inside the 20. |
| `punts_longest` | integer | Punting: longest. |
| `punts_returned` | integer | Punting: returned. |
| `punts_returned_touchdowns` | integer | Punting: returned touchdowns. |
| `punts_returned_yards` | integer | Punting: returned yards. |
| `punts_touchbacks` | integer | Punting: touchbacks. |
| `punts_yards` | integer | Punting: yards. |
| `punts_yards_average_gross` | numeric | Gross punting average (yards per punt). |
| `punts_yards_average_net` | numeric | Net punting average (yards per punt, after returns and touchbacks). |
| `receptions` | integer | Receptions. |
| `receptions_long` | integer | Receiving: longest. |
| `receptions_pass_target` | integer | Pass targets. |
| `receptions_touchdowns` | integer | Receiving: touchdowns. |
| `receptions_yards` | integer | Receiving: yards. |
| `receptions_yards_after_catch` | integer | Receiving yards after the catch. |
| `red_zone_attempts` | integer | Red zone: attempts. |
| `red_zone_successes` | integer | Red zone: successes. |
| `rushing_long` | integer | Rushing: longest. |
| `rushing_plays` | integer | Rushing: plays. |
| `rushing_tackles_for_loss` | integer | Rushing: tackles for loss. |
| `rushing_tackles_for_loss_yards` | integer | Rushing: tackles for loss yards. |
| `rushing_touchdowns` | integer | Rushing: touchdowns. |
| `rushing_yards` | integer | Rushing: yards. |
| `rushing_yards_average` | numeric | Rushing: yards average. |
| `safeties_one_point` | integer | Safeties: one point. |
| `safeties_two_point` | integer | Safeties: two point. |
| `score_q1` | integer | Points scored in the 1st quarter. |
| `score_q2` | integer | Points scored in the 2nd quarter. |
| `score_q3` | integer | Points scored in the 3rd quarter. |
| `score_q4` | integer | Points scored in the 4th quarter. |
| `score_ot` | integer | Points scored in overtime. |
| `score_total` | integer | Total points scored. |
| `third_down_attempts` | integer | Third down: attempts. |
| `third_down_conversions` | integer | Third down: conversions. |
| `time_of_possession` | character | Time of possession (MM:SS). |
| `timeouts_remaining` | integer | Timeouts: remaining. |
| `timeouts_used` | integer | Timeouts: used. |
| `total_plays` | integer | Totals: plays. |
| `total_yards` | integer | Totals: yards. |
| `touchdowns_all_other` | integer | Touchdowns: all other. |
| `turnovers` | integer | Total turnovers. |
| `two_point_conversions_defensive_returns` | integer | Two-point conversions: defensive returns. |
| `two_point_conversions_passing_attempts` | integer | Two-point conversions: passing attempts. |
| `two_point_conversions_passing_successes` | integer | Two-point conversions: passing successes. |
| `two_point_conversions_rushing_attempts` | integer | Two-point conversions: rushing attempts. |
| `two_point_conversions_rushing_successes` | integer | Two-point conversions: rushing successes. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_live_team_statistics(game_id='a9a890ed-4feb-11f1-abca-2c54536568a9')
```

_Last validated n/a._

## `nfl_live_player_statistics`

GET /football/v2/stats/live/player-statistics/{game_id} — one row per player per side: the live player box score.

**Endpoint URL:** `GET https://api.nfl.com/football/v2/stats/live/player-statistics/{game_id}`

**Valid URL:** [https://api.nfl.com/football/v2/stats/live/player-statistics/a9a890ed-4feb-11f1-abca-2c54536568a9](https://api.nfl.com/football/v2/stats/live/player-statistics/a9a890ed-4feb-11f1-abca-2c54536568a9)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Shield uuid game id -- the ``id`` column of the week games and weekly game details listings. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | NFL.com Shield GUID for the game. |
| `offset` | integer | Live-feed sequence position the statistics reflect; advances as the game is played. |
| `side` | character | Which side of the game the row belongs to: away or home. |
| `team_id` | character | NFL.com Shield GUID of the team. |
| `gsis_player_id` | character | NFL GSIS player id (00-00xxxxx), the key nflverse data joins on. |
| `gsis_player_jersey_number` | character | Player's jersey number as recorded in GSIS. |
| `gsis_player_name` | character | Player name as recorded in GSIS (e.g. P.Mahomes). |
| `person_id` | character | NFL.com Shield GUID of the player (person). |
| `defensive_fumbles_forced` | integer | Defense: fumbles forced. |
| `defensive_fumbles_recovered` | integer | Defense: fumbles recovered. |
| `defensive_interceptions` | integer | Defense: interceptions. |
| `defensive_miscellaneous_fumbles_forced` | integer | Defense (miscellaneous): fumbles forced. |
| `defensive_miscellaneous_fumbles_recovered` | integer | Defense (miscellaneous): fumbles recovered. |
| `defensive_miscellaneous_tackles` | numeric | Defense (miscellaneous): tackles. |
| `defensive_miscellaneous_tackles_assists` | integer | Defense (miscellaneous): tackles assists. |
| `defensive_passes_defended` | integer | Defense: passes defended. |
| `defensive_quarterback_hits` | integer | Defense: quarterback hits. |
| `defensive_sacks` | numeric | Defense: sacks. |
| `defensive_sack_yards` | numeric | Defense: sack yards. |
| `defensive_safeties` | integer | Defense: safeties. |
| `defensive_special_teams_fumbles_forced` | integer | Defense: special teams fumbles forced. |
| `defensive_special_teams_fumbles_recovered` | integer | Defense: special teams fumbles recovered. |
| `defensive_special_teams_tackles` | numeric | Defense: special teams tackles. |
| `defensive_special_teams_tackles_assists` | integer | Defense: special teams tackles assists. |
| `defensive_special_teams_blocks` | integer | Defense: special teams blocks. |
| `defensive_tackles` | numeric | Defense: tackles. |
| `defensive_tackles_assists` | integer | Defense: tackles assists. |
| `defensive_tackles_combined` | numeric | Defense: tackles combined. |
| `defensive_tackles_for_loss` | numeric | Defense: tackles for loss. |
| `defensive_tackles_for_loss_yards` | numeric | Defense: tackles for loss yards. |
| `extra_points_attempted` | integer | Extra points: attempted. |
| `extra_points_made` | integer | Extra points: made. |
| `extra_points_missed` | integer | Extra points: missed. |
| `extra_points_blocked` | integer | Extra points: blocked. |
| `field_goals_attempted` | integer | Field goals: attempted. |
| `field_goals_average_length` | numeric | Field goals: average length. |
| `field_goals_blocked` | integer | Field goals: blocked. |
| `field_goals_longest_made` | integer | Field goals: longest made. |
| `field_goals_made` | integer | Field goals: made. |
| `field_goals_missed` | integer | Field goals: missed. |
| `field_goals_total_yards` | integer | Field goals: total yards. |
| `fumbles` | integer | Fumbles. |
| `fumbles_forced` | integer | Fumbles: forced. |
| `fumbles_lost` | integer | Fumbles: lost. |
| `fumbles_recovered_in_end_zone_for_touchdown` | integer | Fumbles: recovered in end zone for touchdown. |
| `fumbles_opponent_recoveries` | integer | Fumbles: opponent recoveries. |
| `fumbles_opponent_recovery_touchdowns` | integer | Fumbles: opponent recovery touchdowns. |
| `fumbles_opponent_recovery_yards` | integer | Fumbles: opponent recovery yards. |
| `fumbles_out_of_bounds` | integer | Fumbles: out of bounds. |
| `fumbles_own_recoveries` | integer | Fumbles: own recoveries. |
| `fumbles_own_recovery_touchdowns` | integer | Fumbles: own recovery touchdowns. |
| `fumbles_own_recovery_yards` | integer | Fumbles: own recovery yards. |
| `interceptions` | integer | Interceptions thrown. |
| `interceptions_long` | integer | Interceptions: longest. |
| `interceptions_longest_touchdown` | integer | Interceptions: longest touchdown. |
| `interceptions_touchdowns` | integer | Interceptions: touchdowns. |
| `interceptions_yards` | integer | Interceptions: yards. |
| `kickoffs` | integer | Kickoffs. |
| `kickoffs_inside20` | integer | Kickoffs: inside the 20. |
| `kickoffs_out_of_bounds` | integer | Kickoffs: out of bounds. |
| `kickoffs_return_yards` | integer | Kickoffs: return yards. |
| `kickoffs_to_end_zone` | integer | Kickoffs: to end zone. |
| `kickoffs_touchbacks` | integer | Kickoffs: touchbacks. |
| `kickoffs_yards` | integer | Kickoffs: yards. |
| `kick_returns` | integer | Kickoff returns. |
| `kick_returns_fair_catches` | integer | Kickoff returns: fair catches. |
| `kick_returns_longest` | integer | Kickoff returns: longest. |
| `kick_returns_longest_touchdown` | integer | Kickoff returns: longest touchdown. |
| `kick_returns_touchdowns` | integer | Kickoff returns: touchdowns. |
| `kick_returns_yards` | integer | Kickoff returns: yards. |
| `kick_returns_yards_average` | numeric | Kickoff returns: yards average. |
| `passing_attempts` | integer | Passing: attempts. |
| `passing_completions` | integer | Passing: completions. |
| `passing_completion_percent` | numeric | Passing: completion percent. |
| `passing_interceptions` | integer | Passing: interceptions. |
| `passing_long` | integer | Passing: longest. |
| `passing_longest_touchdown_pass` | integer | Passing: longest touchdown pass. |
| `passing_rating` | numeric | Passing: rating. |
| `passing_sack_yards_lost` | numeric | Passing: sack yards lost. |
| `passing_times_sacked` | integer | Passing: times sacked. |
| `passing_touchdowns` | integer | Passing: touchdowns. |
| `passing_yards` | integer | Passing: yards. |
| `passing_yards_average` | numeric | Passing: yards average. |
| `passing_yards_per_attempt` | numeric | Passing: yards per attempt. |
| `punts` | integer | Punts. |
| `punts_blocked` | integer | Punting: blocked. |
| `punts_inside20` | integer | Punting: inside the 20. |
| `punts_longest` | integer | Punting: longest. |
| `punts_return_yards` | integer | Punt return yards allowed on this player's punts. |
| `punts_touchbacks` | integer | Punting: touchbacks. |
| `punts_yards` | integer | Punting: yards. |
| `punts_yards_average_gross` | numeric | Gross punting average (yards per punt). |
| `punts_yards_average_net` | numeric | Net punting average (yards per punt, after returns and touchbacks). |
| `punt_returns` | integer | Punt returns. |
| `punt_returns_fair_catches` | integer | Punt returns: fair catches. |
| `punt_returns_longest` | integer | Punt returns: longest. |
| `punt_returns_longest_touchdown` | integer | Punt returns: longest touchdown. |
| `punt_returns_touchdowns` | integer | Punt returns: touchdowns. |
| `punt_returns_yards` | integer | Punt returns: yards. |
| `punt_returns_yards_average` | numeric | Punt returns: yards average. |
| `receptions` | integer | Receptions. |
| `receptions_average` | numeric | Receiving: average. |
| `receptions_long` | integer | Receiving: longest. |
| `receptions_longest_touchdown` | integer | Receiving: longest touchdown. |
| `receptions_pass_target` | integer | Pass targets. |
| `receptions_touchdowns` | integer | Receiving: touchdowns. |
| `receptions_yards` | integer | Receiving: yards. |
| `receptions_yards_after_catch` | integer | Receiving yards after the catch. |
| `rushing_attempts` | integer | Rushing: attempts. |
| `rushing_average` | numeric | Rushing: average. |
| `rushing_long` | integer | Rushing: longest. |
| `rushing_longest_touchdown` | integer | Rushing: longest touchdown. |
| `rushing_touchdowns` | integer | Rushing: touchdowns. |
| `rushing_yards` | integer | Rushing: yards. |
| `two_point_defensive_attempts` | integer | Two-point conversions: defensive attempts. |
| `two_point_defensive_successes` | integer | Two-point conversions: defensive successes. |
| `two_point_passing_attempts` | integer | Two-point conversions: passing attempts. |
| `two_point_passing_successes` | integer | Two-point conversions: passing successes. |
| `two_point_reception_attempts` | integer | Two-point conversions: reception attempts. |
| `two_point_reception_successes` | integer | Two-point conversions: reception successes. |
| `two_point_rushing_attempts` | integer | Two-point conversions: rushing attempts. |
| `two_point_rushing_successes` | integer | Two-point conversions: rushing successes. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_live_player_statistics(game_id='a9a890ed-4feb-11f1-abca-2c54536568a9')
```

_Last validated n/a._

## `nfl_game_details_v2`

GET /experience/v2/gamedetails/{game_id} — one row: the flat v2 game detail (game, summary, optional drive chart / replays / standings).

**Endpoint URL:** `GET https://api.nfl.com/experience/v2/gamedetails/{game_id}`

**Valid URL:** [https://api.nfl.com/experience/v2/gamedetails/a9a890ed-4feb-11f1-abca-2c54536568a9](https://api.nfl.com/experience/v2/gamedetails/a9a890ed-4feb-11f1-abca-2c54536568a9)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Shield uuid game id -- the ``id`` column of the week games and weekly game details listings. |
| `includeDriveChart` | `include_drive_chart` |  |  | `Y` | includeDriveChart query parameter. |
| `includeReplays` | `include_replays` |  |  | `Y` | includeReplays query parameter. |
| `includeStandings` | `include_standings` |  |  | `Y` | includeStandings query parameter. |
| `includeTaggedVideos` | `include_tagged_videos` |  |  | `Y` | includeTaggedVideos query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | character | NFL.com Shield GUID for the combine profile. |
| `category` | character | Game category / window (e.g. SNF, MNF, TNF). |
| `date` | character | Game date (YYYY-MM-DD). |
| `time` | character | ISO 8601 kickoff timestamp. |
| `game_type` | character | Game type classification (e.g. UNSPECIFIED, REG, WC). |
| `international` | logical | Whether the game is played at an international venue. |
| `neutral_site` | logical | Whether the game is played at a neutral site. |
| `season` | integer | Season (year) the date falls in. |
| `season_type` | character | Season type code (PRE, REG, or POST). |
| `status` | character | Game status (e.g. SCHEDULED, INGAME, FINAL). |
| `week` | integer | Week number that the queried date falls within. |
| `week_type` | character | Week type code (e.g. PRE, REG, WC, DIV, CONF, SB). |
| `external_ids` | character | JSON-stringified array of external game identifiers (elias, gsis, etc.). |
| `ticket_url` | character | Primary ticket-purchase URL for the game. |
| `ticket_vendors` | character | JSON-stringified array of ticket-vendor objects (vendor name, URL). |
| `extensions` | character | JSON-stringified array of extension objects (empty when none). |
| `version` | integer | Record version number. |
| `replays` | character | JSON-stringified array of replay objects (populated only when include_replays=true). |
| `tagged_videos` | character | JSON-stringified array of tagged-video objects (populated only when include_tagged_videos=true). |
| `home_team_id` | character | NFL.com Shield GUID of the home team. |
| `home_team_current_logo` | character | Templated URL of the home team's current logo. |
| `home_team_full_name` | character | Full home-team name (e.g. "Kansas City Chiefs"). |
| `away_team_id` | character | NFL.com Shield GUID of the away team. |
| `away_team_current_logo` | character | Templated URL of the away team's current logo. |
| `away_team_full_name` | character | Full away-team name (e.g. "Baltimore Ravens"). |
| `broadcast_info_home_network_channels` | character | JSON-stringified array of broadcast channels in the home market. |
| `broadcast_info_away_network_channels` | character | JSON-stringified array of broadcast channels in the away market. |
| `broadcast_info_international_watch_options` | character | JSON-stringified array of international broadcaster options by country. |
| `broadcast_info_streaming_networks` | character | JSON-stringified array of streaming-network objects. |
| `broadcast_info_territory` | character | Broadcast territory designation (e.g. NATIONAL, REGIONAL). |
| `broadcast_info_audio_networks` | character | JSON-stringified array of audio-broadcast network objects. |
| `venue_id` | character | NFL.com Shield GUID of the venue. |
| `venue_name` | character | Venue name (e.g. "GEHA Field at Arrowhead Stadium"). |
| `venue_city` | character | Venue city. |
| `venue_country` | character | Venue country. |
| `summary_game_id` | character | NFL.com Shield GUID for the game (from the embedded live summary). |
| `summary_offset` | integer | Live-feed sequence offset for the embedded summary snapshot. |
| `summary_attendance` | character | Announced game attendance (from the embedded summary). |
| `summary_clock` | character | Game clock at the summary snapshot (MM:SS). |
| `summary_distance` | integer | Yards to gain for a first down at the summary snapshot. |
| `summary_down` | integer | Current down (1-4) at the summary snapshot. |
| `summary_game_book_url` | character | URL of the official game book image (from the embedded summary). |
| `summary_is_goal_to_go` | logical | Whether the situation is goal-to-go at the summary snapshot. |
| `summary_is_red_zone` | logical | Whether the ball is in the red zone at the summary snapshot. |
| `summary_phase` | character | Game phase (e.g. PREGAME, INGAME, HALFTIME, FINAL). |
| `summary_quarter` | character | Current period descriptor (e.g. Q1, HALFTIME, END_OF_GAME). |
| `summary_start_time` | character | ISO 8601 kickoff timestamp (from the embedded summary). |
| `summary_weather` | character | Weather summary string (temperature, humidity, wind). |
| `summary_yard_line` | character | Current line of scrimmage at the summary snapshot (e.g. "KC 10"). |
| `summary_away_team_team_id` | character | NFL.com Shield GUID of the away team (from the embedded summary). |
| `summary_away_team_has_possession` | logical | Whether the away team has possession at the summary snapshot. |
| `summary_away_team_score_q1` | integer | Away team points scored in the first quarter. |
| `summary_away_team_score_q2` | integer | Away team points scored in the second quarter. |
| `summary_away_team_score_q3` | integer | Away team points scored in the third quarter. |
| `summary_away_team_score_q4` | integer | Away team points scored in the fourth quarter. |
| `summary_away_team_score_ot` | integer | Away team points scored in overtime. |
| `summary_away_team_score_total` | integer | Away team total points. |
| `summary_away_team_timeouts_remaining` | integer | Away team timeouts remaining at the summary snapshot. |
| `summary_away_team_timeouts_used` | integer | Away team timeouts used at the summary snapshot. |
| `summary_home_team_team_id` | character | NFL.com Shield GUID of the home team (from the embedded summary). |
| `summary_home_team_has_possession` | logical | Whether the home team has possession at the summary snapshot. |
| `summary_home_team_score_q1` | integer | Home team points scored in the first quarter. |
| `summary_home_team_score_q2` | integer | Home team points scored in the second quarter. |
| `summary_home_team_score_q3` | integer | Home team points scored in the third quarter. |
| `summary_home_team_score_q4` | integer | Home team points scored in the fourth quarter. |
| `summary_home_team_score_ot` | integer | Home team points scored in overtime. |
| `summary_home_team_score_total` | integer | Home team total points. |
| `summary_home_team_timeouts_remaining` | integer | Home team timeouts remaining at the summary snapshot. |
| `summary_home_team_timeouts_used` | integer | Home team timeouts used at the summary snapshot. |
| `away_team_standings_team_id` | character | Away team standings: nFL.com Shield GUID for the team. |
| `away_team_standings_team_current_logo` | character | Away team standings: templated URL of the team's current logo. |
| `away_team_standings_team_full_name` | character | Away team standings: full team name (e.g. "Arizona Cardinals"). |
| `away_team_standings_clinched_bye` | logical | Away team standings: whether the team has clinched a first-round playoff bye. |
| `away_team_standings_clinched_division` | logical | Away team standings: whether the team has clinched its division. |
| `away_team_standings_clinched_eliminated` | logical | Away team standings: whether the team has been mathematically eliminated from playoff contention. |
| `away_team_standings_clinched_home_field` | logical | Away team standings: whether the team has clinched home-field advantage throughout the playoffs. |
| `away_team_standings_clinched_playoff` | logical | Away team standings: whether the team has clinched a playoff berth. |
| `away_team_standings_clinched_wild_card` | logical | Away team standings: whether the team has clinched a wild-card playoff berth. |
| `away_team_standings_close_games_wins` | integer | Away team standings: wins in close games (decided by one score / 8 points or fewer). |
| `away_team_standings_close_games_losses` | integer | Away team standings: losses in close games (decided by one score / 8 points or fewer). |
| `away_team_standings_close_games_ties` | integer | Away team standings: ties in close games. |
| `away_team_standings_conference_wins` | integer | Away team standings: wins against conference (AFC/NFC) opponents. |
| `away_team_standings_conference_win_pct` | numeric | Away team standings: win percentage against conference opponents. |
| `away_team_standings_conference_losses` | integer | Away team standings: losses against conference opponents. |
| `away_team_standings_conference_ties` | integer | Away team standings: ties against conference opponents. |
| `away_team_standings_conference_rank` | integer | Away team standings: standings rank within the conference. |
| `away_team_standings_conference_points_for` | integer | Away team standings: points scored in conference games. |
| `away_team_standings_conference_points_against` | integer | Away team standings: points allowed in conference games. |
| `away_team_standings_division_wins` | integer | Away team standings: wins against division opponents. |
| `away_team_standings_division_win_pct` | numeric | Away team standings: win percentage against division opponents. |
| `away_team_standings_division_losses` | integer | Away team standings: losses against division opponents. |
| `away_team_standings_division_ties` | integer | Away team standings: ties against division opponents. |
| `away_team_standings_division_rank` | integer | Away team standings: standings rank within the division. |
| `away_team_standings_division_points_for` | integer | Away team standings: points scored in division games. |
| `away_team_standings_division_points_against` | integer | Away team standings: points allowed in division games. |
| `away_team_standings_home_wins` | integer | Away team standings: wins in home games. |
| `away_team_standings_home_win_pct` | numeric | Away team standings: win percentage in home games. |
| `away_team_standings_home_losses` | integer | Away team standings: losses in home games. |
| `away_team_standings_home_ties` | integer | Away team standings: ties in home games. |
| `away_team_standings_home_points_for` | integer | Away team standings: points scored in home games. |
| `away_team_standings_home_points_against` | integer | Away team standings: points allowed in home games. |
| `away_team_standings_last5_wins` | integer | Away team standings: wins over the last five games. |
| `away_team_standings_last5_win_pct` | numeric | Away team standings: win percentage over the last five games. |
| `away_team_standings_last5_losses` | integer | Away team standings: losses over the last five games. |
| `away_team_standings_last5_ties` | integer | Away team standings: ties over the last five games. |
| `away_team_standings_last5_points_for` | integer | Away team standings: points scored over the last five games. |
| `away_team_standings_last5_points_against` | integer | Away team standings: points allowed over the last five games. |
| `away_team_standings_overall_games` | integer | Away team standings: total games played. |
| `away_team_standings_overall_wins` | integer | Away team standings: total wins. |
| `away_team_standings_overall_win_pct` | numeric | Away team standings: overall win percentage. |
| `away_team_standings_overall_losses` | integer | Away team standings: total losses. |
| `away_team_standings_overall_ties` | integer | Away team standings: total ties. |
| `away_team_standings_overall_points_for` | integer | Away team standings: total points scored. |
| `away_team_standings_overall_points_against` | integer | Away team standings: total points allowed. |
| `away_team_standings_overall_streak_type` | character | Away team standings: current streak type ("W" for winning, "L" for losing). |
| `away_team_standings_overall_streak_length` | integer | Away team standings: length of the current win/loss streak. |
| `away_team_standings_road_wins` | integer | Away team standings: wins in road (away) games. |
| `away_team_standings_road_win_pct` | numeric | Away team standings: win percentage in road games. |
| `away_team_standings_road_losses` | integer | Away team standings: losses in road games. |
| `away_team_standings_road_ties` | integer | Away team standings: ties in road games. |
| `away_team_standings_road_points_for` | integer | Away team standings: points scored in road games. |
| `away_team_standings_road_points_against` | integer | Away team standings: points allowed in road games. |
| `home_team_standings_team_id` | character | Home team standings: nFL.com Shield GUID for the team. |
| `home_team_standings_team_current_logo` | character | Home team standings: templated URL of the team's current logo. |
| `home_team_standings_team_full_name` | character | Home team standings: full team name (e.g. "Arizona Cardinals"). |
| `home_team_standings_clinched_bye` | logical | Home team standings: whether the team has clinched a first-round playoff bye. |
| `home_team_standings_clinched_division` | logical | Home team standings: whether the team has clinched its division. |
| `home_team_standings_clinched_eliminated` | logical | Home team standings: whether the team has been mathematically eliminated from playoff contention. |
| `home_team_standings_clinched_home_field` | logical | Home team standings: whether the team has clinched home-field advantage throughout the playoffs. |
| `home_team_standings_clinched_playoff` | logical | Home team standings: whether the team has clinched a playoff berth. |
| `home_team_standings_clinched_wild_card` | logical | Home team standings: whether the team has clinched a wild-card playoff berth. |
| `home_team_standings_close_games_wins` | integer | Home team standings: wins in close games (decided by one score / 8 points or fewer). |
| `home_team_standings_close_games_losses` | integer | Home team standings: losses in close games (decided by one score / 8 points or fewer). |
| `home_team_standings_close_games_ties` | integer | Home team standings: ties in close games. |
| `home_team_standings_conference_wins` | integer | Home team standings: wins against conference (AFC/NFC) opponents. |
| `home_team_standings_conference_win_pct` | numeric | Home team standings: win percentage against conference opponents. |
| `home_team_standings_conference_losses` | integer | Home team standings: losses against conference opponents. |
| `home_team_standings_conference_ties` | integer | Home team standings: ties against conference opponents. |
| `home_team_standings_conference_rank` | integer | Home team standings: standings rank within the conference. |
| `home_team_standings_conference_points_for` | integer | Home team standings: points scored in conference games. |
| `home_team_standings_conference_points_against` | integer | Home team standings: points allowed in conference games. |
| `home_team_standings_division_wins` | integer | Home team standings: wins against division opponents. |
| `home_team_standings_division_win_pct` | numeric | Home team standings: win percentage against division opponents. |
| `home_team_standings_division_losses` | integer | Home team standings: losses against division opponents. |
| `home_team_standings_division_ties` | integer | Home team standings: ties against division opponents. |
| `home_team_standings_division_rank` | integer | Home team standings: standings rank within the division. |
| `home_team_standings_division_points_for` | integer | Home team standings: points scored in division games. |
| `home_team_standings_division_points_against` | integer | Home team standings: points allowed in division games. |
| `home_team_standings_home_wins` | integer | Home team standings: wins in home games. |
| `home_team_standings_home_win_pct` | numeric | Home team standings: win percentage in home games. |
| `home_team_standings_home_losses` | integer | Home team standings: losses in home games. |
| `home_team_standings_home_ties` | integer | Home team standings: ties in home games. |
| `home_team_standings_home_points_for` | integer | Home team standings: points scored in home games. |
| `home_team_standings_home_points_against` | integer | Home team standings: points allowed in home games. |
| `home_team_standings_last5_wins` | integer | Home team standings: wins over the last five games. |
| `home_team_standings_last5_win_pct` | numeric | Home team standings: win percentage over the last five games. |
| `home_team_standings_last5_losses` | integer | Home team standings: losses over the last five games. |
| `home_team_standings_last5_ties` | integer | Home team standings: ties over the last five games. |
| `home_team_standings_last5_points_for` | integer | Home team standings: points scored over the last five games. |
| `home_team_standings_last5_points_against` | integer | Home team standings: points allowed over the last five games. |
| `home_team_standings_overall_games` | integer | Home team standings: total games played. |
| `home_team_standings_overall_wins` | integer | Home team standings: total wins. |
| `home_team_standings_overall_win_pct` | numeric | Home team standings: overall win percentage. |
| `home_team_standings_overall_losses` | integer | Home team standings: total losses. |
| `home_team_standings_overall_ties` | integer | Home team standings: total ties. |
| `home_team_standings_overall_points_for` | integer | Home team standings: total points scored. |
| `home_team_standings_overall_points_against` | integer | Home team standings: total points allowed. |
| `home_team_standings_overall_streak_type` | character | Home team standings: current streak type ("W" for winning, "L" for losing). |
| `home_team_standings_overall_streak_length` | integer | Home team standings: length of the current win/loss streak. |
| `home_team_standings_road_wins` | integer | Home team standings: wins in road (away) games. |
| `home_team_standings_road_win_pct` | numeric | Home team standings: win percentage in road games. |
| `home_team_standings_road_losses` | integer | Home team standings: losses in road games. |
| `home_team_standings_road_ties` | integer | Home team standings: ties in road games. |
| `home_team_standings_road_points_for` | integer | Home team standings: points scored in road games. |
| `home_team_standings_road_points_against` | integer | Home team standings: points allowed in road games. |
| `drive_chart_game_id` | character | NFL.com Shield GUID for the game (from the drive chart, present when include_drive_chart=true). |
| `drive_chart_offset` | integer | Live-feed sequence offset for the drive-chart snapshot. |
| `drive_chart_drives` | character | JSON-stringified array of drive objects (sequence, team, result, etc.). |
| `drive_chart_plays` | character | JSON-stringified array of play objects within the drive chart. |
| `drive_chart_scoring_summaries` | character | JSON-stringified array of scoring-summary objects (sequence, scores, clock). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_game_details_v2(game_id='a9a890ed-4feb-11f1-abca-2c54536568a9')
```

_Last validated n/a._

## `nfl_game_details_by_slug`

GET /experience/v1/gamedetailsbyslug/{slug} — one row: the flat game detail looked up by nfl.com slug.

**Endpoint URL:** `GET https://api.nfl.com/experience/v1/gamedetailsbyslug/{slug}`

**Valid URL:** [https://api.nfl.com/experience/v1/gamedetailsbyslug/broncos-at-chiefs-2026-reg-1](https://api.nfl.com/experience/v1/gamedetailsbyslug/broncos-at-chiefs-2026-reg-1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `slug` | `slug` |  | `Y` |  | nfl.com game slug, e.g. ``broncos-at-chiefs-2026-reg-1`` -- the last segment of the nfl.com game page URL and the ``slug`` external id. |
| `includeReplays` | `include_replays` |  |  | `Y` | includeReplays query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | character | NFL.com Shield GUID for the combine profile. |
| `category` | character | Game category / window (e.g. SNF, MNF, TNF). |
| `date` | character | Game date (YYYY-MM-DD). |
| `time` | character | ISO 8601 kickoff timestamp. |
| `game_type` | character | Game type classification (e.g. UNSPECIFIED, REG, WC). |
| `international` | logical | Whether the game is played at an international venue. |
| `neutral_site` | logical | Whether the game is played at a neutral site. |
| `season` | integer | Season (year) the date falls in. |
| `season_type` | character | Season type code (PRE, REG, or POST). |
| `status` | character | Game status (e.g. SCHEDULED, INGAME, FINAL). |
| `week` | integer | Week number that the queried date falls within. |
| `week_type` | character | Week type code (e.g. PRE, REG, WC, DIV, CONF, SB). |
| `external_ids` | character | JSON-stringified array of external game identifiers (elias, gsis, etc.). |
| `ticket_url` | character | Primary ticket-purchase URL for the game. |
| `ticket_vendors` | character | JSON-stringified array of ticket-vendor objects (vendor name, URL). |
| `extensions` | character | JSON-stringified array of extension objects (empty when none). |
| `version` | integer | Record version number. |
| `replays` | character | JSON-stringified array of replay objects (populated only when include_replays=true). |
| `home_team_id` | character | NFL.com Shield GUID of the home team. |
| `home_team_current_logo` | character | Templated URL of the home team's current logo. |
| `home_team_full_name` | character | Full home-team name (e.g. "Kansas City Chiefs"). |
| `away_team_id` | character | NFL.com Shield GUID of the away team. |
| `away_team_current_logo` | character | Templated URL of the away team's current logo. |
| `away_team_full_name` | character | Full away-team name (e.g. "Baltimore Ravens"). |
| `broadcast_info_home_network_channels` | character | JSON-stringified array of broadcast channels in the home market. |
| `broadcast_info_away_network_channels` | character | JSON-stringified array of broadcast channels in the away market. |
| `broadcast_info_international_watch_options` | character | JSON-stringified array of international broadcaster options by country. |
| `broadcast_info_streaming_networks` | character | JSON-stringified array of streaming-network objects. |
| `broadcast_info_territory` | character | Broadcast territory designation (e.g. NATIONAL, REGIONAL). |
| `broadcast_info_audio_networks` | character | JSON-stringified array of audio-broadcast network objects. |
| `venue_id` | character | NFL.com Shield GUID of the venue. |
| `venue_name` | character | Venue name (e.g. "GEHA Field at Arrowhead Stadium"). |
| `venue_city` | character | Venue city. |
| `venue_country` | character | Venue country. |
| `summary_game_id` | character | NFL.com Shield GUID for the game (from the embedded live summary). |
| `summary_offset` | integer | Live-feed sequence offset for the embedded summary snapshot. |
| `summary_attendance` | character | Announced game attendance (from the embedded summary). |
| `summary_clock` | character | Game clock at the summary snapshot (MM:SS). |
| `summary_distance` | integer | Yards to gain for a first down at the summary snapshot. |
| `summary_down` | integer | Current down (1-4) at the summary snapshot. |
| `summary_game_book_url` | character | URL of the official game book image (from the embedded summary). |
| `summary_is_goal_to_go` | logical | Whether the situation is goal-to-go at the summary snapshot. |
| `summary_is_red_zone` | logical | Whether the ball is in the red zone at the summary snapshot. |
| `summary_phase` | character | Game phase (e.g. PREGAME, INGAME, HALFTIME, FINAL). |
| `summary_quarter` | character | Current period descriptor (e.g. Q1, HALFTIME, END_OF_GAME). |
| `summary_start_time` | character | ISO 8601 kickoff timestamp (from the embedded summary). |
| `summary_weather` | character | Weather summary string (temperature, humidity, wind). |
| `summary_yard_line` | character | Current line of scrimmage at the summary snapshot (e.g. "KC 10"). |
| `summary_away_team_team_id` | character | NFL.com Shield GUID of the away team (from the embedded summary). |
| `summary_away_team_has_possession` | logical | Whether the away team has possession at the summary snapshot. |
| `summary_away_team_score_q1` | integer | Away team points scored in the first quarter. |
| `summary_away_team_score_q2` | integer | Away team points scored in the second quarter. |
| `summary_away_team_score_q3` | integer | Away team points scored in the third quarter. |
| `summary_away_team_score_q4` | integer | Away team points scored in the fourth quarter. |
| `summary_away_team_score_ot` | integer | Away team points scored in overtime. |
| `summary_away_team_score_total` | integer | Away team total points. |
| `summary_away_team_timeouts_remaining` | integer | Away team timeouts remaining at the summary snapshot. |
| `summary_away_team_timeouts_used` | integer | Away team timeouts used at the summary snapshot. |
| `summary_home_team_team_id` | character | NFL.com Shield GUID of the home team (from the embedded summary). |
| `summary_home_team_has_possession` | logical | Whether the home team has possession at the summary snapshot. |
| `summary_home_team_score_q1` | integer | Home team points scored in the first quarter. |
| `summary_home_team_score_q2` | integer | Home team points scored in the second quarter. |
| `summary_home_team_score_q3` | integer | Home team points scored in the third quarter. |
| `summary_home_team_score_q4` | integer | Home team points scored in the fourth quarter. |
| `summary_home_team_score_ot` | integer | Home team points scored in overtime. |
| `summary_home_team_score_total` | integer | Home team total points. |
| `summary_home_team_timeouts_remaining` | integer | Home team timeouts remaining at the summary snapshot. |
| `summary_home_team_timeouts_used` | integer | Home team timeouts used at the summary snapshot. |
| `away_team_standings_team_id` | character | Away team standings: nFL.com Shield GUID for the team. |
| `away_team_standings_team_current_logo` | character | Away team standings: templated URL of the team's current logo. |
| `away_team_standings_team_full_name` | character | Away team standings: full team name (e.g. "Arizona Cardinals"). |
| `away_team_standings_clinched_bye` | logical | Away team standings: whether the team has clinched a first-round playoff bye. |
| `away_team_standings_clinched_division` | logical | Away team standings: whether the team has clinched its division. |
| `away_team_standings_clinched_eliminated` | logical | Away team standings: whether the team has been mathematically eliminated from playoff contention. |
| `away_team_standings_clinched_home_field` | logical | Away team standings: whether the team has clinched home-field advantage throughout the playoffs. |
| `away_team_standings_clinched_playoff` | logical | Away team standings: whether the team has clinched a playoff berth. |
| `away_team_standings_clinched_wild_card` | logical | Away team standings: whether the team has clinched a wild-card playoff berth. |
| `away_team_standings_close_games_wins` | integer | Away team standings: wins in close games (decided by one score / 8 points or fewer). |
| `away_team_standings_close_games_losses` | integer | Away team standings: losses in close games (decided by one score / 8 points or fewer). |
| `away_team_standings_close_games_ties` | integer | Away team standings: ties in close games. |
| `away_team_standings_conference_wins` | integer | Away team standings: wins against conference (AFC/NFC) opponents. |
| `away_team_standings_conference_win_pct` | numeric | Away team standings: win percentage against conference opponents. |
| `away_team_standings_conference_losses` | integer | Away team standings: losses against conference opponents. |
| `away_team_standings_conference_ties` | integer | Away team standings: ties against conference opponents. |
| `away_team_standings_conference_rank` | integer | Away team standings: standings rank within the conference. |
| `away_team_standings_conference_points_for` | integer | Away team standings: points scored in conference games. |
| `away_team_standings_conference_points_against` | integer | Away team standings: points allowed in conference games. |
| `away_team_standings_division_wins` | integer | Away team standings: wins against division opponents. |
| `away_team_standings_division_win_pct` | numeric | Away team standings: win percentage against division opponents. |
| `away_team_standings_division_losses` | integer | Away team standings: losses against division opponents. |
| `away_team_standings_division_ties` | integer | Away team standings: ties against division opponents. |
| `away_team_standings_division_rank` | integer | Away team standings: standings rank within the division. |
| `away_team_standings_division_points_for` | integer | Away team standings: points scored in division games. |
| `away_team_standings_division_points_against` | integer | Away team standings: points allowed in division games. |
| `away_team_standings_home_wins` | integer | Away team standings: wins in home games. |
| `away_team_standings_home_win_pct` | numeric | Away team standings: win percentage in home games. |
| `away_team_standings_home_losses` | integer | Away team standings: losses in home games. |
| `away_team_standings_home_ties` | integer | Away team standings: ties in home games. |
| `away_team_standings_home_points_for` | integer | Away team standings: points scored in home games. |
| `away_team_standings_home_points_against` | integer | Away team standings: points allowed in home games. |
| `away_team_standings_last5_wins` | integer | Away team standings: wins over the last five games. |
| `away_team_standings_last5_win_pct` | numeric | Away team standings: win percentage over the last five games. |
| `away_team_standings_last5_losses` | integer | Away team standings: losses over the last five games. |
| `away_team_standings_last5_ties` | integer | Away team standings: ties over the last five games. |
| `away_team_standings_last5_points_for` | integer | Away team standings: points scored over the last five games. |
| `away_team_standings_last5_points_against` | integer | Away team standings: points allowed over the last five games. |
| `away_team_standings_overall_games` | integer | Away team standings: total games played. |
| `away_team_standings_overall_wins` | integer | Away team standings: total wins. |
| `away_team_standings_overall_win_pct` | numeric | Away team standings: overall win percentage. |
| `away_team_standings_overall_losses` | integer | Away team standings: total losses. |
| `away_team_standings_overall_ties` | integer | Away team standings: total ties. |
| `away_team_standings_overall_points_for` | integer | Away team standings: total points scored. |
| `away_team_standings_overall_points_against` | integer | Away team standings: total points allowed. |
| `away_team_standings_overall_streak_type` | character | Away team standings: current streak type ("W" for winning, "L" for losing). |
| `away_team_standings_overall_streak_length` | integer | Away team standings: length of the current win/loss streak. |
| `away_team_standings_road_wins` | integer | Away team standings: wins in road (away) games. |
| `away_team_standings_road_win_pct` | numeric | Away team standings: win percentage in road games. |
| `away_team_standings_road_losses` | integer | Away team standings: losses in road games. |
| `away_team_standings_road_ties` | integer | Away team standings: ties in road games. |
| `away_team_standings_road_points_for` | integer | Away team standings: points scored in road games. |
| `away_team_standings_road_points_against` | integer | Away team standings: points allowed in road games. |
| `home_team_standings_team_id` | character | Home team standings: nFL.com Shield GUID for the team. |
| `home_team_standings_team_current_logo` | character | Home team standings: templated URL of the team's current logo. |
| `home_team_standings_team_full_name` | character | Home team standings: full team name (e.g. "Arizona Cardinals"). |
| `home_team_standings_clinched_bye` | logical | Home team standings: whether the team has clinched a first-round playoff bye. |
| `home_team_standings_clinched_division` | logical | Home team standings: whether the team has clinched its division. |
| `home_team_standings_clinched_eliminated` | logical | Home team standings: whether the team has been mathematically eliminated from playoff contention. |
| `home_team_standings_clinched_home_field` | logical | Home team standings: whether the team has clinched home-field advantage throughout the playoffs. |
| `home_team_standings_clinched_playoff` | logical | Home team standings: whether the team has clinched a playoff berth. |
| `home_team_standings_clinched_wild_card` | logical | Home team standings: whether the team has clinched a wild-card playoff berth. |
| `home_team_standings_close_games_wins` | integer | Home team standings: wins in close games (decided by one score / 8 points or fewer). |
| `home_team_standings_close_games_losses` | integer | Home team standings: losses in close games (decided by one score / 8 points or fewer). |
| `home_team_standings_close_games_ties` | integer | Home team standings: ties in close games. |
| `home_team_standings_conference_wins` | integer | Home team standings: wins against conference (AFC/NFC) opponents. |
| `home_team_standings_conference_win_pct` | numeric | Home team standings: win percentage against conference opponents. |
| `home_team_standings_conference_losses` | integer | Home team standings: losses against conference opponents. |
| `home_team_standings_conference_ties` | integer | Home team standings: ties against conference opponents. |
| `home_team_standings_conference_rank` | integer | Home team standings: standings rank within the conference. |
| `home_team_standings_conference_points_for` | integer | Home team standings: points scored in conference games. |
| `home_team_standings_conference_points_against` | integer | Home team standings: points allowed in conference games. |
| `home_team_standings_division_wins` | integer | Home team standings: wins against division opponents. |
| `home_team_standings_division_win_pct` | numeric | Home team standings: win percentage against division opponents. |
| `home_team_standings_division_losses` | integer | Home team standings: losses against division opponents. |
| `home_team_standings_division_ties` | integer | Home team standings: ties against division opponents. |
| `home_team_standings_division_rank` | integer | Home team standings: standings rank within the division. |
| `home_team_standings_division_points_for` | integer | Home team standings: points scored in division games. |
| `home_team_standings_division_points_against` | integer | Home team standings: points allowed in division games. |
| `home_team_standings_home_wins` | integer | Home team standings: wins in home games. |
| `home_team_standings_home_win_pct` | numeric | Home team standings: win percentage in home games. |
| `home_team_standings_home_losses` | integer | Home team standings: losses in home games. |
| `home_team_standings_home_ties` | integer | Home team standings: ties in home games. |
| `home_team_standings_home_points_for` | integer | Home team standings: points scored in home games. |
| `home_team_standings_home_points_against` | integer | Home team standings: points allowed in home games. |
| `home_team_standings_last5_wins` | integer | Home team standings: wins over the last five games. |
| `home_team_standings_last5_win_pct` | numeric | Home team standings: win percentage over the last five games. |
| `home_team_standings_last5_losses` | integer | Home team standings: losses over the last five games. |
| `home_team_standings_last5_ties` | integer | Home team standings: ties over the last five games. |
| `home_team_standings_last5_points_for` | integer | Home team standings: points scored over the last five games. |
| `home_team_standings_last5_points_against` | integer | Home team standings: points allowed over the last five games. |
| `home_team_standings_overall_games` | integer | Home team standings: total games played. |
| `home_team_standings_overall_wins` | integer | Home team standings: total wins. |
| `home_team_standings_overall_win_pct` | numeric | Home team standings: overall win percentage. |
| `home_team_standings_overall_losses` | integer | Home team standings: total losses. |
| `home_team_standings_overall_ties` | integer | Home team standings: total ties. |
| `home_team_standings_overall_points_for` | integer | Home team standings: total points scored. |
| `home_team_standings_overall_points_against` | integer | Home team standings: total points allowed. |
| `home_team_standings_overall_streak_type` | character | Home team standings: current streak type ("W" for winning, "L" for losing). |
| `home_team_standings_overall_streak_length` | integer | Home team standings: length of the current win/loss streak. |
| `home_team_standings_road_wins` | integer | Home team standings: wins in road (away) games. |
| `home_team_standings_road_win_pct` | numeric | Home team standings: win percentage in road games. |
| `home_team_standings_road_losses` | integer | Home team standings: losses in road games. |
| `home_team_standings_road_ties` | integer | Home team standings: ties in road games. |
| `home_team_standings_road_points_for` | integer | Home team standings: points scored in road games. |
| `home_team_standings_road_points_against` | integer | Home team standings: points allowed in road games. |
| `drive_chart_game_id` | character | NFL.com Shield GUID for the game (from the drive chart, present when include_drive_chart=true). |
| `drive_chart_offset` | integer | Live-feed sequence offset for the drive-chart snapshot. |
| `drive_chart_drives` | character | JSON-stringified array of drive objects (sequence, team, result, etc.). |
| `drive_chart_plays` | character | JSON-stringified array of play objects within the drive chart. |
| `drive_chart_scoring_summaries` | character | JSON-stringified array of scoring-summary objects (sequence, scores, clock). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_game_details_by_slug(slug='broncos-at-chiefs-2026-reg-1')
```

_Last validated n/a._
