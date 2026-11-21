# NFL — NFL.com API — Other

> NFL — NFL.com API — Other — function reference in sdv-py, the SportsDataverse Python package.

## nfl_standings

GET /football/v2/standings — one row per team standing across the returned week(s).

**Endpoint URL:** `GET https://api.nfl.com/football/v2/standings`

**Valid URL:** [https://api.nfl.com/football/v2/standings?season=2024&seasonType=REG&week=18&limit=40](https://api.nfl.com/football/v2/standings?season=2024&seasonType=REG&week=18&limit=40)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `week` | `week` |  |  | `Y` | Week number within the season. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#nfl_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | String | NFL.com Shield GUID for the team. |
| `team_current_logo` | String | Templated URL of the team's current logo. |
| `team_full_name` | String | Full team name (e.g. "Arizona Cardinals"). |
| `clinched_bye` | Boolean | Whether the team has clinched a first-round playoff bye. |
| `clinched_division` | Boolean | Whether the team has clinched its division. |
| `clinched_eliminated` | Boolean | Whether the team has been mathematically eliminated from playoff contention. |
| `clinched_home_field` | Boolean | Whether the team has clinched home-field advantage throughout the playoffs. |
| `clinched_playoff` | Boolean | Whether the team has clinched a playoff berth. |
| `clinched_wild_card` | Boolean | Whether the team has clinched a wild-card playoff berth. |
| `close_games_wins` | Int64 | Wins in close games (decided by one score / 8 points or fewer). |
| `close_games_losses` | Int64 | Losses in close games (decided by one score / 8 points or fewer). |
| `close_games_ties` | Int64 | Ties in close games. |
| `conference_wins` | Int64 | Wins against conference (AFC/NFC) opponents. |
| `conference_win_pct` | Float64 | Win percentage against conference opponents. |
| `conference_losses` | Int64 | Losses against conference opponents. |
| `conference_ties` | Int64 | Ties against conference opponents. |
| `conference_rank` | Int64 | Standings rank within the conference. |
| `conference_points_for` | Int64 | Points scored in conference games. |
| `conference_points_against` | Int64 | Points allowed in conference games. |
| `division_wins` | Int64 | Wins against division opponents. |
| `division_win_pct` | Float64 | Win percentage against division opponents. |
| `division_losses` | Int64 | Losses against division opponents. |
| `division_ties` | Int64 | Ties against division opponents. |
| `division_rank` | Int64 | Standings rank within the division. |
| `division_points_for` | Int64 | Points scored in division games. |
| `division_points_against` | Int64 | Points allowed in division games. |
| `home_wins` | Int64 | Wins in home games. |
| `home_win_pct` | Float64 | Win percentage in home games. |
| `home_losses` | Int64 | Losses in home games. |
| `home_ties` | Int64 | Ties in home games. |
| `home_points_for` | Int64 | Points scored in home games. |
| `home_points_against` | Int64 | Points allowed in home games. |
| `last5_wins` | Int64 | Wins over the last five games. |
| `last5_win_pct` | Float64 | Win percentage over the last five games. |
| `last5_losses` | Int64 | Losses over the last five games. |
| `last5_ties` | Int64 | Ties over the last five games. |
| `last5_points_for` | Int64 | Points scored over the last five games. |
| `last5_points_against` | Int64 | Points allowed over the last five games. |
| `overall_games` | Int64 | Total games played. |
| `overall_wins` | Int64 | Total wins. |
| `overall_win_pct` | Float64 | Overall win percentage. |
| `overall_losses` | Int64 | Total losses. |
| `overall_ties` | Int64 | Total ties. |
| `overall_points_for` | Int64 | Total points scored. |
| `overall_points_against` | Int64 | Total points allowed. |
| `overall_streak_type` | String | Current streak type ("W" for winning, "L" for losing). |
| `overall_streak_length` | Int64 | Length of the current win/loss streak. |
| `road_wins` | Int64 | Wins in road (away) games. |
| `road_win_pct` | Float64 | Win percentage in road games. |
| `road_losses` | Int64 | Losses in road games. |
| `road_ties` | Int64 | Ties in road games. |
| `road_points_for` | Int64 | Points scored in road games. |
| `road_points_against` | Int64 | Points allowed in road games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nfl_standings-example}

```python
nfl_standings(season=2024, season_type='REG', week=18)
```

## nfl_rosters

GET /football/v2/rosters — one row per team roster for the season.

**Endpoint URL:** `GET https://api.nfl.com/football/v2/rosters`

**Valid URL:** [https://api.nfl.com/football/v2/rosters?season=2024&limit=40](https://api.nfl.com/football/v2/rosters?season=2024&limit=40)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `teamId` | `team_id` |  |  | `Y` | Shield team uuid (the ``id`` of a team in the teams history listing). Returns just that team's roster -- one roster (~37 KB) instead of all 32 (~1.2 MB). |

### Returns {#nfl_rosters-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (year) of the roster. |
| `season_type` | String | Season type code (PRE, REG, or POST). |
| `team_id` | String | NFL.com Shield GUID for the team. |
| `team_abbreviation` | String | Team abbreviation (e.g. "ARI"). |
| `team_full_name` | String | Full team name (e.g. "Arizona Cardinals"). |
| `team_conference_abbr` | String | Conference abbreviation (AFC or NFC). |
| `team_conference_full_name` | String | Full conference name (e.g. "National Football Conference"). |
| `team_current_logo` | String | Templated URL of the team's current logo. |
| `team_division_full_name` | String | Full division name (e.g. "NFC West"). |
| `team_league` | String | League name ("National Football League"). |
| `team_location` | String | Team location / city (e.g. "Arizona"). |
| `team_nick_name` | String | Team nickname (e.g. "Cardinals"). |
| `team_venues` | String | JSON-stringified array of the team's venue objects (id, name, etc.). |
| `persons` | String | JSON-stringified array of roster player objects for the team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_rosters-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nfl_rosters-example}

```python
nfl_rosters(season=2024)
```

## nfl_teams_history

GET /football/v2/teams/history — one row per team for a season.

**Endpoint URL:** `GET https://api.nfl.com/football/v2/teams/history`

**Valid URL:** [https://api.nfl.com/football/v2/teams/history?season=2024&limit=40](https://api.nfl.com/football/v2/teams/history?season=2024&limit=40)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#nfl_teams_history-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String | NFL.com Shield GUID for the team (or Pro Bowl team entry). |
| `season` | Int64 | Season (year) the team record applies to. |
| `abbreviation` | String | Team abbreviation (e.g. "AFC", "ARI"). |
| `full_name` | String | Full team name (e.g. "AFC Pro Bowl Team", "Arizona Cardinals"). |
| `team_type` | String | Team type classification (e.g. "TEAM", "PRO" for Pro Bowl teams). |
| `conference_abbr` | String | Conference abbreviation (AFC or NFC). |
| `conference_full_name` | String | Full conference name (e.g. "American Football Conference"). |
| `current_logo` | String | Templated URL of the team's current logo. |
| `division_full_name` | String | Full division name (e.g. "NFC West"). |
| `league` | String | League name ("National Football League"). |
| `location` | String | Team location / city. |
| `nick_name` | String | Team nickname. |
| `venues` | String | JSON-stringified array of the team's venue objects (empty for non-club entries). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_teams_history-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nfl_teams_history-example}

```python
nfl_teams_history(season=2024)
```

## nfl_team

GET /football/v2/teams/{team_id} — single-team detail (one row).

**Endpoint URL:** `GET https://api.nfl.com/football/v2/teams/{team_id}`

**Valid URL:** [https://api.nfl.com/football/v2/teams/10403800-517c-7b8c-65a3-c61b95d86123](https://api.nfl.com/football/v2/teams/10403800-517c-7b8c-65a3-c61b95d86123)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#nfl_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String | NFL.com Shield GUID for the team. |
| `bio` | String | Team biography / description text (may be null). |
| `current_background` | String | Templated URL of the team's current background image. |
| `current_coach` | String | Name of the team's current head coach. |
| `current_logo` | String | Templated URL of the team's current logo. |
| `primary_color` | String | Team primary color (hex code). |
| `secondary_color` | String | Team secondary color (hex code). |
| `year_established` | Int64 | Year the franchise was established. |
| `full_name` | String | Full team name (e.g. "Arizona Cardinals"). |
| `nfl_shop_url` | String | URL to the team's NFL Shop storefront. |
| `official_website_url` | String | URL of the team's official website. |
| `owners` | String | Name(s) of the team's owner(s). |
| `team_type` | String | Team type classification (e.g. "TEAM"). |
| `socials` | String | JSON-stringified array of the team's social-media links (platform, link). |
| `vll_channel_callsign` | String | Verizon Live League (VLL) channel call sign for the team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nfl_team-example}

```python
nfl_team(team_id='10403800-517c-7b8c-65a3-c61b95d86123')
```

## nfl_weeks

GET /football/v2/weeks/season/{season}/seasonType/{season_type} — week calendar (one row per week).

**Endpoint URL:** `GET https://api.nfl.com/football/v2/weeks/season/{season}/seasonType/{season_type}`

**Valid URL:** [https://api.nfl.com/football/v2/weeks/season/2024/seasonType/REG](https://api.nfl.com/football/v2/weeks/season/2024/seasonType/REG)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | season path parameter. |
| `season_type` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |

### Returns {#nfl_weeks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (year) of the week. |
| `season_type` | String | Season type code (PRE, REG, or POST). |
| `week` | Int64 | Week number within the season type. |
| `bye_teams` | String | JSON-stringified array of teams on bye during the week (empty when none). |
| `date_begin` | String | Start date of the week (YYYY-MM-DD). |
| `date_end` | String | End date of the week (YYYY-MM-DD). |
| `week_type` | String | Week type code (e.g. PRE, REG, WC, DIV, CONF, SB). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_weeks-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nfl_weeks-example}

```python
nfl_weeks(season=2024, season_type='REG')
```

## nfl_weeks_by_date

GET /football/v2/weeks/date/{YYYY-MM-DD} — the week containing a date (one row).

**Endpoint URL:** `GET https://api.nfl.com/football/v2/weeks/date/{date}`

**Valid URL:** [https://api.nfl.com/football/v2/weeks/date/2024-09-08](https://api.nfl.com/football/v2/weeks/date/2024-09-08)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  | `Y` |  | date path parameter. |

### Returns {#nfl_weeks_by_date-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (year) the date falls in. |
| `season_type` | String | Season type code (PRE, REG, or POST). |
| `week` | Int64 | Week number that the queried date falls within. |
| `bye_teams` | String | JSON-stringified array of teams on bye during the week (empty when none). |
| `date_begin` | String | Start date of the week (YYYY-MM-DD). |
| `date_end` | String | End date of the week (YYYY-MM-DD). |
| `week_type` | String | Week type code (e.g. PRE, REG, WC, DIV, CONF, SB). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_weeks_by_date-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nfl_weeks_by_date-example}

```python
nfl_weeks_by_date(date='2024-09-08')
```

## nfl_combine_profiles

GET /football/v2/combine/profiles — one row per combine prospect.

**Endpoint URL:** `GET https://api.nfl.com/football/v2/combine/profiles`

**Valid URL:** [https://api.nfl.com/football/v2/combine/profiles?year=2024&limit=40](https://api.nfl.com/football/v2/combine/profiles?year=2024&limit=40)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#nfl_combine_profiles-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String | NFL.com Shield GUID for the combine profile. |
| `year` | Int64 | Combine / draft class year. |
| `person_id` | String | NFL.com Shield GUID of the prospect. |
| `person_display_name` | String | Prospect's full display name. |
| `person_esb_id` | String | Prospect's Elias Sports Bureau (ESB) identifier. |
| `person_first_name` | String | Prospect's first name. |
| `person_last_name` | String | Prospect's last name. |
| `person_hometown` | String | Prospect's hometown (city, state). |
| `person_college_names` | String | JSON-stringified array of the prospect's college(s). |
| `arm_length` | Float64 | Arm length measured at the combine, in inches. |
| `athleticism_score` | Float64 | Composite athleticism score. |
| `bench_press` | Float64 | Bench-press measurement object (flattened; null when no measurement). |
| `broad_jump` | Float64 | Broad-jump measurement object (flattened; null when no measurement). |
| `bio` | String | HTML biography / career summary for the prospect. |
| `college_class` | String | Prospect's college class (e.g. Senior, Junior). |
| `draft_grade` | Float64 | Numeric draft grade assigned to the prospect. |
| `draft_projection` | String | Draft projection text (e.g. "Priority free agent"). |
| `forty_yard_dash` | Float64 | 40-yard-dash measurement object (flattened; null when no measurement). |
| `grade` | Float64 | Overall prospect grade. |
| `hand_size` | Int64 | Hand size measured at the combine, in inches. |
| `headshot` | String | Templated URL of the prospect's headshot image. |
| `height` | Int64 | Prospect height, in inches. |
| `nfl_comparison` | String | Player the prospect is most comparable to. |
| `overview` | String | HTML scouting overview of the prospect. |
| `production_score` | Float64 | Composite college-production score. |
| `profile_author` | String | Author of the scouting profile (e.g. "Lance Zierlein"). |
| `pro_forty_yard_dash` | Float64 | Pro-day 40-yard-dash measurement object (flattened; null when no measurement). |
| `sixty_yard_shuttle` | Float64 | 60-yard-shuttle measurement object (flattened; null when no measurement). |
| `size_score` | Float64 | Composite size score (flattened; null when not scored). |
| `sources_tell_us` | String | "Sources tell us" scouting commentary text. |
| `strengths` | String | HTML list of the prospect's strengths. |
| `ten_yard_split` | Float64 | 10-yard-split measurement object (flattened; null when no measurement). |
| `three_cone_drill` | Float64 | Three-cone-drill measurement object (flattened; null when no measurement). |
| `twenty_yard_shuttle` | Float64 | 20-yard-shuttle measurement object (flattened; null when no measurement). |
| `weaknesses` | String | HTML list of the prospect's weaknesses. |
| `combine_attendance` | Boolean | Whether the prospect attended the combine. |
| `position` | String | Prospect's position abbreviation (e.g. "CB"). |
| `position_group` | String | Prospect's position group (e.g. "DB"). |
| `vertical_jump` | Float64 | Vertical-jump measurement object (flattened; null when no measurement). |
| `weight` | Int64 | Prospect weight, in pounds. |
| `pro_forty_yard_dash_designation` | String | Designation for the pro-day 40-yard dash (OFFICIAL or UNOFFICIAL). |
| `pro_forty_yard_dash_seconds` | Float64 | Pro-day 40-yard-dash time, in seconds. |
| `bench_press_designation` | String | Designation for the bench-press result (OFFICIAL or UNOFFICIAL). |
| `bench_press_repetitions` | Int64 | Number of 225 lb bench-press repetitions. |
| `broad_jump_designation` | String | Designation for the broad-jump result (OFFICIAL or UNOFFICIAL). |
| `broad_jump_inches` | Int64 | Broad-jump distance, in inches. |
| `forty_yard_dash_designation` | String | Designation for the 40-yard-dash result (OFFICIAL or UNOFFICIAL). |
| `forty_yard_dash_seconds` | Float64 | 40-yard-dash time, in seconds. |
| `ten_yard_split_designation` | String | Designation for the 10-yard-split result (OFFICIAL or UNOFFICIAL). |
| `ten_yard_split_seconds` | Float64 | 10-yard-split time, in seconds. |
| `vertical_jump_designation` | String | Designation for the vertical-jump result (OFFICIAL or UNOFFICIAL). |
| `vertical_jump_inches` | Int64 | Vertical-jump height, in inches. |
| `three_cone_drill_designation` | String | Designation for the three-cone-drill result (OFFICIAL or UNOFFICIAL). |
| `three_cone_drill_seconds` | Float64 | Three-cone-drill time, in seconds. |
| `twenty_yard_shuttle_designation` | String | Designation for the 20-yard-shuttle result (OFFICIAL or UNOFFICIAL). |
| `twenty_yard_shuttle_seconds` | Float64 | 20-yard-shuttle time, in seconds. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_combine_profiles-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nfl_combine_profiles-example}

```python
nfl_combine_profiles(year=2024)
```

## nfl_draft_picks

GET /football/v2/draft/picks/report — one row per draft pick.

**Endpoint URL:** `GET https://api.nfl.com/football/v2/draft/picks/report`

**Valid URL:** [https://api.nfl.com/football/v2/draft/picks/report?year=2024&limit=40](https://api.nfl.com/football/v2/draft/picks/report?year=2024&limit=40)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#nfl_draft_picks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `year` | Int64 | Draft year. |
| `draft_round` | Int64 | Round of the draft in which the pick was made. |
| `draft_position` | Int64 | Pick position within the round. |
| `draft_number_overall` | Int64 | Overall pick number across the entire draft. |
| `person_id` | String | NFL.com Shield GUID of the drafted player (may be empty until the pick is announced). |
| `pick_is_in` | Boolean | Whether the pick has officially been submitted / announced. |
| `team_id` | String | NFL.com Shield GUID of the team making the pick. |
| `trade_note` | String | Trade annotation for the pick (e.g. "CAR>CHI" denoting a traded selection). |
| `tweet_sent` | Boolean | Whether the announcement tweet has been sent for the pick. |
| `tweets_sent` | String | JSON-stringified array of per-account tweet-sent status objects. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_draft_picks-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nfl_draft_picks-example}

```python
nfl_draft_picks(year=2024)
```

## nfl_injuries

GET /football/v2/injuries — one row per injured player.

**Endpoint URL:** `GET https://api.nfl.com/football/v2/injuries`

**Valid URL:** [https://api.nfl.com/football/v2/injuries?season=2024&seasonType=REG&week=1](https://api.nfl.com/football/v2/injuries?season=2024&seasonType=REG&week=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `week` | `week` |  |  | `Y` | Week number within the season. |

### Returns {#nfl_injuries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (year) of the injury report. |
| `season_type` | String | Season type code (PRE, REG, or POST). |
| `week` | Int64 | Week number of the injury report. |
| `team_id` | String | NFL.com Shield GUID of the player's team. |
| `team_current_logo` | String | Templated URL of the team's current logo. |
| `team_full_name` | String | Full team name (e.g. "Atlanta Falcons"). |
| `person_id` | String | NFL.com Shield GUID of the injured player. |
| `person_first_name` | String | Player's first name. |
| `person_common_first_name` | String | Player's common / preferred first name. |
| `person_last_name` | String | Player's last name. |
| `person_display_name` | String | Player's full display name. |
| `person_gsis_id` | String | Player's NFL GSIS identifier (e.g. "00-0036948"). |
| `person_headshot` | String | Templated URL of the player's headshot image. |
| `injuries` | String | JSON-stringified array of detailed injury objects (empty when not specified). |
| `injury_status` | String | Game-status designation (e.g. OUT, DOUBTFUL, QUESTIONABLE). |
| `practices` | String | JSON-stringified array of practice-participation notes. |
| `practice_days` | String | JSON-stringified array of per-day practice status objects (date, status). |
| `practice_status` | String | Most recent practice-participation status (e.g. FULL, LIMITED, DNP). |
| `position` | String | Player's position abbreviation (e.g. "DE"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_injuries-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nfl_injuries-example}

```python
nfl_injuries(season=2024, season_type='REG', week=1)
```

## nfl_weekly_game_details

GET /football/v2/experience/weekly-game-details — one row per game (bare list).

**Endpoint URL:** `GET https://api.nfl.com/football/v2/experience/weekly-game-details`

**Valid URL:** [https://api.nfl.com/football/v2/experience/weekly-game-details?season=2024&type=REG&week=1&includeDriveChart=true&includeReplays=false&includeStandings=false&includeTaggedVideos=false](https://api.nfl.com/football/v2/experience/weekly-game-details?season=2024&type=REG&week=1&includeDriveChart=true&includeReplays=false&includeStandings=false&includeTaggedVideos=false)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `type` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST (sent as the `type` query param) -- not ESPN's numeric 1/2/3. |
| `week` | `week` |  |  | `Y` | Week number within the season. |
| `includeDriveChart` | `include_drive_chart` |  |  | `Y` | includeDriveChart query parameter. |
| `includeReplays` | `include_replays` |  |  | `Y` | includeReplays query parameter. |
| `includeStandings` | `include_standings` |  |  | `Y` | includeStandings query parameter. |
| `includeTaggedVideos` | `include_tagged_videos` |  |  | `Y` | includeTaggedVideos query parameter. |

### Returns {#nfl_weekly_game_details-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String | NFL.com Shield GUID for the game. |
| `home_team_id` | String | NFL.com Shield GUID of the home team. |
| `home_team_current_logo` | String | Templated URL of the home team's current logo. |
| `home_team_full_name` | String | Full home-team name (e.g. "Kansas City Chiefs"). |
| `away_team_id` | String | NFL.com Shield GUID of the away team. |
| `away_team_current_logo` | String | Templated URL of the away team's current logo. |
| `away_team_full_name` | String | Full away-team name (e.g. "Baltimore Ravens"). |
| `category` | String | Game category / window (e.g. SNF, MNF, TNF). |
| `date` | String | Game date (YYYY-MM-DD). |
| `time` | String | ISO 8601 kickoff timestamp. |
| `broadcast_info_home_network_channels` | String | JSON-stringified array of broadcast channels in the home market. |
| `broadcast_info_away_network_channels` | String | JSON-stringified array of broadcast channels in the away market. |
| `broadcast_info_international_watch_options` | String | JSON-stringified array of international broadcaster options by country. |
| `broadcast_info_streaming_networks` | String | JSON-stringified array of streaming-network objects. |
| `broadcast_info_territory` | String | Broadcast territory designation (e.g. NATIONAL, REGIONAL). |
| `broadcast_info_audio_networks` | String | JSON-stringified array of audio-broadcast network objects. |
| `game_type` | String | Game type classification (e.g. UNSPECIFIED, REG, WC). |
| `international` | Boolean | Whether the game is played at an international venue. |
| `neutral_site` | Boolean | Whether the game is played at a neutral site. |
| `venue_id` | String | NFL.com Shield GUID of the venue. |
| `venue_name` | String | Venue name (e.g. "GEHA Field at Arrowhead Stadium"). |
| `venue_city` | String | Venue city. |
| `venue_country` | String | Venue country. |
| `season` | Int64 | Season (year) of the game. |
| `season_type` | String | Season type code (PRE, REG, or POST). |
| `status` | String | Game status (e.g. SCHEDULED, INGAME, FINAL). |
| `week` | Int64 | Week number of the game. |
| `week_type` | String | Week type code (e.g. PRE, REG, WC, DIV, CONF, SB). |
| `external_ids` | String | JSON-stringified array of external game identifiers (elias, gsis, etc.). |
| `ticket_url` | String | Primary ticket-purchase URL for the game. |
| `ticket_vendors` | String | JSON-stringified array of ticket-vendor objects (vendor name, URL). |
| `extensions` | String | JSON-stringified array of extension objects (empty when none). |
| `version` | Int64 | Record version number. |
| `summary_game_id` | String | NFL.com Shield GUID for the game (from the embedded live summary). |
| `summary_offset` | Int64 | Live-feed sequence offset for the embedded summary snapshot. |
| `summary_attendance` | Int64 | Announced game attendance (from the embedded summary). |
| `summary_clock` | String | Game clock at the summary snapshot (MM:SS). |
| `summary_distance` | Int64 | Yards to gain for a first down at the summary snapshot. |
| `summary_down` | Int64 | Current down (1-4) at the summary snapshot. |
| `summary_game_book_url` | String | URL of the official game book image (from the embedded summary). |
| `summary_is_goal_to_go` | Boolean | Whether the situation is goal-to-go at the summary snapshot. |
| `summary_is_red_zone` | Boolean | Whether the ball is in the red zone at the summary snapshot. |
| `summary_phase` | String | Game phase (e.g. PREGAME, INGAME, HALFTIME, FINAL). |
| `summary_quarter` | String | Current period descriptor (e.g. Q1, HALFTIME, END_OF_GAME). |
| `summary_start_time` | String | ISO 8601 kickoff timestamp (from the embedded summary). |
| `summary_weather` | String | Weather summary string (temperature, humidity, wind). |
| `summary_yard_line` | String | Current line of scrimmage at the summary snapshot (e.g. "KC 10"). |
| `summary_away_team_team_id` | String | NFL.com Shield GUID of the away team (from the embedded summary). |
| `summary_away_team_has_possession` | Boolean | Whether the away team has possession at the summary snapshot. |
| `summary_away_team_score_q1` | Int64 | Away team points scored in the first quarter. |
| `summary_away_team_score_q2` | Int64 | Away team points scored in the second quarter. |
| `summary_away_team_score_q3` | Int64 | Away team points scored in the third quarter. |
| `summary_away_team_score_q4` | Int64 | Away team points scored in the fourth quarter. |
| `summary_away_team_score_ot` | Int64 | Away team points scored in overtime. |
| `summary_away_team_score_total` | Int64 | Away team total points. |
| `summary_away_team_timeouts_remaining` | Int64 | Away team timeouts remaining at the summary snapshot. |
| `summary_away_team_timeouts_used` | Int64 | Away team timeouts used at the summary snapshot. |
| `summary_home_team_team_id` | String | NFL.com Shield GUID of the home team (from the embedded summary). |
| `summary_home_team_has_possession` | Boolean | Whether the home team has possession at the summary snapshot. |
| `summary_home_team_score_q1` | Int64 | Home team points scored in the first quarter. |
| `summary_home_team_score_q2` | Int64 | Home team points scored in the second quarter. |
| `summary_home_team_score_q3` | Int64 | Home team points scored in the third quarter. |
| `summary_home_team_score_q4` | Int64 | Home team points scored in the fourth quarter. |
| `summary_home_team_score_ot` | Int64 | Home team points scored in overtime. |
| `summary_home_team_score_total` | Int64 | Home team total points. |
| `summary_home_team_timeouts_remaining` | Int64 | Home team timeouts remaining at the summary snapshot. |
| `summary_home_team_timeouts_used` | Int64 | Home team timeouts used at the summary snapshot. |
| `drive_chart_game_id` | String | NFL.com Shield GUID for the game (from the drive chart, present when include_drive_chart=true). |
| `drive_chart_offset` | Int64 | Live-feed sequence offset for the drive-chart snapshot. |
| `drive_chart_drives` | String | JSON-stringified array of drive objects (sequence, team, result, etc.). |
| `drive_chart_plays` | String | JSON-stringified array of play objects within the drive chart. |
| `drive_chart_scoring_summaries` | String | JSON-stringified array of scoring-summary objects (sequence, scores, clock). |
| `replays` | String | JSON-stringified array of replay objects (populated only when include_replays=true). |
| `tagged_videos` | String | JSON-stringified array of tagged-video objects (populated only when include_tagged_videos=true). |
| `away_team_standings` | String | JSON-stringified away-team standings object (populated only when include_standings=true). |
| `home_team_standings` | String | JSON-stringified home-team standings object (populated only when include_standings=true). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_weekly_game_details-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nfl_weekly_game_details-example}

```python
nfl_weekly_game_details(season=2024, season_type='REG', week=1)
```
