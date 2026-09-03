---
title: SOCCER — American Soccer Analysis (app.americansocceranalysis.com)
sidebar_label: American Soccer Analysis (app.americansocceranalysis.com)
description: "SOCCER — American Soccer Analysis (app.americansocceranalysis.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
---
# SOCCER — American Soccer Analysis (app.americansocceranalysis.com)

`sportsdataverse.soccer` — 15 endpoints.

## `asa_games`

Games/fixtures with final scores, venue/official/manager FKs.

**Endpoint URL:** `GET https://app.americansocceranalysis.com/api/v1/{league_slug}/games`

**Valid URL:** [https://app.americansocceranalysis.com/api/v1/mls/games](https://app.americansocceranalysis.com/api/v1/mls/games)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | ASA game id (base62 string; Utf8 join key). |
| `date_time_utc` | character | Kickoff timestamp (UTC, ISO 8601). |
| `home_score` | integer | Home goals (regulation + extra time). |
| `away_score` | integer | Away goals (regulation + extra time). |
| `home_team_id` | character | FK -> Team (home side). |
| `away_team_id` | character | FK -> Team (away side). |
| `referee_id` | character | ASA referee id (base62 string; Utf8 join key). |
| `stadium_id` | character | ASA stadium id (base62 string; Utf8 join key). |
| `home_manager_id` | character | FK -> Manager (home side; nullable). |
| `away_manager_id` | character | FK -> Manager (away side; nullable). |
| `expanded_minutes` | integer | Total match minutes incl. stoppage (data coverage window). |
| `season_name` | character | Season(s) the player appears in; may serialize as a scalar, a list, or an object across the leagues. |
| `matchday` | integer | Round/matchday number. |
| `knockout_game` | logical | True if a knockout/playoff fixture. |
| `status` | character | Game status (e.g. `final`). |
| `last_updated_utc` | character | Last-updated timestamp (UTC, ISO 8601). |
| `attendance` | numeric | Reported attendance (nullable). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
asa_games(league_slug='mls')
```

_Last validated n/a._

## `asa_games_xgoals`

Per-game expected-goals + expected points for both sides.

**Endpoint URL:** `GET https://app.americansocceranalysis.com/api/v1/{league_slug}/games/xgoals`

**Valid URL:** [https://app.americansocceranalysis.com/api/v1/mls/games/xgoals](https://app.americansocceranalysis.com/api/v1/mls/games/xgoals)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |
| `season_name` | `season_name` |  |  | `Y` | Filter to one or more seasons. Comma-list accepted (`2022,2023`). |
| `stage_name` | `stage_name` |  |  | `Y` | Filter to a competition stage, e.g. `Regular Season`. **URL-encode spaces.** |
| `minimum_minutes` | `minimum_minutes` |  |  | `Y` | Drop players/teams below this minutes-played threshold. |
| `general_position` | `general_position` |  |  | `Y` | Filter by general position code: GK, CB, FB, DM, CM, AM, W, ST (player/GK routes). |
| `split_by_teams` | `split_by_teams` |  |  | `Y` | `true` => one row per entity per team (splits traded players). |
| `split_by_seasons` | `split_by_seasons` |  |  | `Y` | `true` => one row per entity per season. |
| `split_by_games` | `split_by_games` |  |  | `Y` | `true` => one row per entity per game. |
| `start_date` | `start_date` |  |  | `Y` | Lower date bound (`YYYY-MM-DD`), where the route supports date windows. |
| `end_date` | `end_date` |  |  | `Y` | Upper date bound (`YYYY-MM-DD`), where the route supports date windows. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | ASA game id (base62 string; Utf8 join key). |
| `date_time_utc` | character | Kickoff timestamp (UTC, ISO 8601). |
| `home_team_id` | character | FK -> Team (home side). |
| `home_goals` | integer | Home goals scored. |
| `home_team_xgoals` | numeric | Home expected goals (team model). |
| `home_player_xgoals` | numeric | Home expected goals (player-shot model). |
| `away_team_id` | character | FK -> Team (away side). |
| `away_goals` | integer | Away goals scored. |
| `away_team_xgoals` | numeric | Away expected goals (team model). |
| `away_player_xgoals` | numeric | Away expected goals (player-shot model). |
| `goal_difference` | integer | goals_for - goals_against. |
| `team_xgoal_difference` | numeric | Home minus away team xGoals. |
| `player_xgoal_difference` | numeric | Home minus away player xGoals. |
| `final_score_difference` | integer | Final goal margin (home perspective). |
| `home_xpoints` | numeric | Home expected points from the match. |
| `away_xpoints` | numeric | Away expected points from the match. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
asa_games_xgoals(league_slug='mls')
```

_Last validated n/a._

## `asa_goalkeepers_goals_added`

Per-goalkeeper Goals Added with a per-action-type data[] breakdown.

**Endpoint URL:** `GET https://app.americansocceranalysis.com/api/v1/{league_slug}/goalkeepers/goals-added`

**Valid URL:** [https://app.americansocceranalysis.com/api/v1/mls/goalkeepers/goals-added](https://app.americansocceranalysis.com/api/v1/mls/goalkeepers/goals-added)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |
| `season_name` | `season_name` |  |  | `Y` | Filter to one or more seasons. Comma-list accepted (`2022,2023`). |
| `stage_name` | `stage_name` |  |  | `Y` | Filter to a competition stage, e.g. `Regular Season`. **URL-encode spaces.** |
| `minimum_minutes` | `minimum_minutes` |  |  | `Y` | Drop players/teams below this minutes-played threshold. |
| `general_position` | `general_position` |  |  | `Y` | Filter by general position code: GK, CB, FB, DM, CM, AM, W, ST (player/GK routes). |
| `split_by_teams` | `split_by_teams` |  |  | `Y` | `true` => one row per entity per team (splits traded players). |
| `split_by_seasons` | `split_by_seasons` |  |  | `Y` | `true` => one row per entity per season. |
| `split_by_games` | `split_by_games` |  |  | `Y` | `true` => one row per entity per game. |
| `start_date` | `start_date` |  |  | `Y` | Lower date bound (`YYYY-MM-DD`), where the route supports date windows. |
| `end_date` | `end_date` |  |  | `Y` | Upper date bound (`YYYY-MM-DD`), where the route supports date windows. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | character | ASA player id (base62 string; Utf8 join key). |
| `team_id` | character | ASA team id (base62 string; Utf8 join key, never numeric). |
| `minutes_played` | integer | Minutes played in the filtered window. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
asa_goalkeepers_goals_added(league_slug='mls')
```

_Last validated n/a._

## `asa_goalkeepers_xgoals`

Per-goalkeeper shot-stopping vs post-shot expected goals.

**Endpoint URL:** `GET https://app.americansocceranalysis.com/api/v1/{league_slug}/goalkeepers/xgoals`

**Valid URL:** [https://app.americansocceranalysis.com/api/v1/mls/goalkeepers/xgoals](https://app.americansocceranalysis.com/api/v1/mls/goalkeepers/xgoals)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |
| `season_name` | `season_name` |  |  | `Y` | Filter to one or more seasons. Comma-list accepted (`2022,2023`). |
| `stage_name` | `stage_name` |  |  | `Y` | Filter to a competition stage, e.g. `Regular Season`. **URL-encode spaces.** |
| `minimum_minutes` | `minimum_minutes` |  |  | `Y` | Drop players/teams below this minutes-played threshold. |
| `general_position` | `general_position` |  |  | `Y` | Filter by general position code: GK, CB, FB, DM, CM, AM, W, ST (player/GK routes). |
| `split_by_teams` | `split_by_teams` |  |  | `Y` | `true` => one row per entity per team (splits traded players). |
| `split_by_seasons` | `split_by_seasons` |  |  | `Y` | `true` => one row per entity per season. |
| `split_by_games` | `split_by_games` |  |  | `Y` | `true` => one row per entity per game. |
| `start_date` | `start_date` |  |  | `Y` | Lower date bound (`YYYY-MM-DD`), where the route supports date windows. |
| `end_date` | `end_date` |  |  | `Y` | Upper date bound (`YYYY-MM-DD`), where the route supports date windows. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | character | ASA player id (base62 string; Utf8 join key). |
| `team_id` | character | ASA team id (base62 string; Utf8 join key, never numeric). |
| `minutes_played` | integer | Minutes played in the filtered window. |
| `shots_faced` | integer | Shots faced. |
| `goals_conceded` | integer | Goals conceded. |
| `saves` | integer | Saves made. |
| `share_headed_shots` | numeric | Share of faced shots that were headers. |
| `xgoals_gk_faced` | numeric | Post-shot expected goals faced. |
| `goals_minus_xgoals_gk` | numeric | Goals conceded minus post-shot xG (negative = shots saved above expectation). |
| `goals_divided_by_xgoals_gk` | numeric | Goals conceded / post-shot xG faced. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
asa_goalkeepers_xgoals(league_slug='mls')
```

_Last validated n/a._

## `asa_managers`

Managers/head coaches.

**Endpoint URL:** `GET https://app.americansocceranalysis.com/api/v1/{league_slug}/managers`

**Valid URL:** [https://app.americansocceranalysis.com/api/v1/mls/managers](https://app.americansocceranalysis.com/api/v1/mls/managers)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `manager_id` | character | ASA manager id (base62 string; Utf8 join key). |
| `manager_name` | character | Manager display name. |
| `nationality` | character | Player nationality (country name). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
asa_managers(league_slug='mls')
```

_Last validated n/a._

## `asa_players`

Players in the league (identity + biometrics + positions).

**Endpoint URL:** `GET https://app.americansocceranalysis.com/api/v1/{league_slug}/players`

**Valid URL:** [https://app.americansocceranalysis.com/api/v1/mls/players](https://app.americansocceranalysis.com/api/v1/mls/players)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | character | ASA player id (base62 string; Utf8 join key). |
| `player_name` | character | Player display name. |
| `birth_date` | character | Date of birth (`YYYY-MM-DD`; may be null). |
| `height_ft` | integer | Listed height, feet component. |
| `height_in` | integer | Listed height, inches component. |
| `weight_lb` | integer | Listed weight in pounds. |
| `nationality` | character | Player nationality (country name). |
| `primary_broad_position` | character | Broad position bucket (Goalkeeper/Defender/Midfielder/Forward). |
| `primary_general_position` | character | General position code (GK/CB/FB/DM/CM/AM/W/ST). |
| `season_name` | character | Season(s) the player appears in; may serialize as a scalar, a list, or an object across the leagues. |
| `secondary_general_position` | character | Secondary general position code (nullable). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
asa_players(league_slug='mls')
```

_Last validated n/a._

## `asa_players_goals_added`

Per-player Goals Added (g+) with a per-action-type data[] breakdown.

**Endpoint URL:** `GET https://app.americansocceranalysis.com/api/v1/{league_slug}/players/goals-added`

**Valid URL:** [https://app.americansocceranalysis.com/api/v1/mls/players/goals-added](https://app.americansocceranalysis.com/api/v1/mls/players/goals-added)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |
| `season_name` | `season_name` |  |  | `Y` | Filter to one or more seasons. Comma-list accepted (`2022,2023`). |
| `stage_name` | `stage_name` |  |  | `Y` | Filter to a competition stage, e.g. `Regular Season`. **URL-encode spaces.** |
| `minimum_minutes` | `minimum_minutes` |  |  | `Y` | Drop players/teams below this minutes-played threshold. |
| `general_position` | `general_position` |  |  | `Y` | Filter by general position code: GK, CB, FB, DM, CM, AM, W, ST (player/GK routes). |
| `split_by_teams` | `split_by_teams` |  |  | `Y` | `true` => one row per entity per team (splits traded players). |
| `split_by_seasons` | `split_by_seasons` |  |  | `Y` | `true` => one row per entity per season. |
| `split_by_games` | `split_by_games` |  |  | `Y` | `true` => one row per entity per game. |
| `start_date` | `start_date` |  |  | `Y` | Lower date bound (`YYYY-MM-DD`), where the route supports date windows. |
| `end_date` | `end_date` |  |  | `Y` | Upper date bound (`YYYY-MM-DD`), where the route supports date windows. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | character | ASA player id (base62 string; Utf8 join key). |
| `team_id` | character | ASA team id (base62 string; Utf8 join key, never numeric). |
| `general_position` | character | General position code (GK/CB/FB/DM/CM/AM/W/ST). |
| `minutes_played` | integer | Minutes played in the filtered window. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
asa_players_goals_added(league_slug='mls')
```

_Last validated n/a._

## `asa_players_salaries`

Player salaries (MLS only; server caps the response at 10000 rows).

**Endpoint URL:** `GET https://app.americansocceranalysis.com/api/v1/{league_slug}/players/salaries`

**Valid URL:** [https://app.americansocceranalysis.com/api/v1/mls/players/salaries](https://app.americansocceranalysis.com/api/v1/mls/players/salaries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |
| `season_name` | `season_name` |  |  | `Y` | Filter to one or more seasons. Comma-list accepted (`2022,2023`). |
| `stage_name` | `stage_name` |  |  | `Y` | Filter to a competition stage, e.g. `Regular Season`. **URL-encode spaces.** |
| `minimum_minutes` | `minimum_minutes` |  |  | `Y` | Drop players/teams below this minutes-played threshold. |
| `general_position` | `general_position` |  |  | `Y` | Filter by general position code: GK, CB, FB, DM, CM, AM, W, ST (player/GK routes). |
| `split_by_teams` | `split_by_teams` |  |  | `Y` | `true` => one row per entity per team (splits traded players). |
| `split_by_seasons` | `split_by_seasons` |  |  | `Y` | `true` => one row per entity per season. |
| `split_by_games` | `split_by_games` |  |  | `Y` | `true` => one row per entity per game. |
| `start_date` | `start_date` |  |  | `Y` | Lower date bound (`YYYY-MM-DD`), where the route supports date windows. |
| `end_date` | `end_date` |  |  | `Y` | Upper date bound (`YYYY-MM-DD`), where the route supports date windows. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | character | ASA player id (base62 string; Utf8 join key). |
| `team_id` | character | ASA team id (base62 string; Utf8 join key, never numeric). |
| `season_name` | integer | Season(s) the player appears in; may serialize as a scalar, a list, or an object across the leagues. |
| `position` | character | Roster position designation (string; distinct from `general_position`). |
| `base_salary` | integer | Base salary (USD). |
| `guaranteed_compensation` | integer | Guaranteed compensation (USD). |
| `mlspa_release` | character | MLSPA salary-release label/date the row is sourced from. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
asa_players_salaries(league_slug='mls')
```

_Last validated n/a._

## `asa_players_xgoals`

Per-player expected-goals + attacking production.

**Endpoint URL:** `GET https://app.americansocceranalysis.com/api/v1/{league_slug}/players/xgoals`

**Valid URL:** [https://app.americansocceranalysis.com/api/v1/mls/players/xgoals](https://app.americansocceranalysis.com/api/v1/mls/players/xgoals)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |
| `season_name` | `season_name` |  |  | `Y` | Filter to one or more seasons. Comma-list accepted (`2022,2023`). |
| `stage_name` | `stage_name` |  |  | `Y` | Filter to a competition stage, e.g. `Regular Season`. **URL-encode spaces.** |
| `minimum_minutes` | `minimum_minutes` |  |  | `Y` | Drop players/teams below this minutes-played threshold. |
| `general_position` | `general_position` |  |  | `Y` | Filter by general position code: GK, CB, FB, DM, CM, AM, W, ST (player/GK routes). |
| `split_by_teams` | `split_by_teams` |  |  | `Y` | `true` => one row per entity per team (splits traded players). |
| `split_by_seasons` | `split_by_seasons` |  |  | `Y` | `true` => one row per entity per season. |
| `split_by_games` | `split_by_games` |  |  | `Y` | `true` => one row per entity per game. |
| `start_date` | `start_date` |  |  | `Y` | Lower date bound (`YYYY-MM-DD`), where the route supports date windows. |
| `end_date` | `end_date` |  |  | `Y` | Upper date bound (`YYYY-MM-DD`), where the route supports date windows. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | character | ASA player id (base62 string; Utf8 join key). |
| `team_id` | character | ASA team id (base62 string; Utf8 join key, never numeric). |
| `general_position` | character | General position code (GK/CB/FB/DM/CM/AM/W/ST). |
| `minutes_played` | integer | Minutes played in the filtered window. |
| `shots` | integer | Shots taken. |
| `shots_on_target` | integer | Shots on target. |
| `goals` | integer | Goals scored. |
| `xgoals` | numeric | Expected goals (pre-shot xG). |
| `xplace` | numeric | Expected goals added by shot placement (post-shot minus pre-shot). |
| `goals_minus_xgoals` | numeric | Finishing over expectation (goals - xG). |
| `key_passes` | integer | Passes that led to a shot. |
| `primary_assists` | integer | Primary assists. |
| `xassists` | numeric | Expected assists. |
| `primary_assists_minus_xassists` | numeric | Assists over expectation. |
| `goals_plus_primary_assists` | integer | Goals + primary assists (G+A). |
| `xgoals_plus_xassists` | numeric | xGoals + xAssists (xG+xA). |
| `points_added` | numeric | Team points added by the player's attacking output. |
| `xpoints_added` | numeric | Expected team points added. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
asa_players_xgoals(league_slug='mls')
```

_Last validated n/a._

## `asa_referees`

Match referees.

**Endpoint URL:** `GET https://app.americansocceranalysis.com/api/v1/{league_slug}/referees`

**Valid URL:** [https://app.americansocceranalysis.com/api/v1/mls/referees](https://app.americansocceranalysis.com/api/v1/mls/referees)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `referee_id` | character | ASA referee id (base62 string; Utf8 join key). |
| `referee_name` | character | Referee display name. |
| `birth_date` | character | Date of birth (`YYYY-MM-DD`; may be null). |
| `nationality` | character | Player nationality (country name). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
asa_referees(league_slug='mls')
```

_Last validated n/a._

## `asa_stadia`

Stadia (venue metadata incl. coordinates + pitch dimensions).

**Endpoint URL:** `GET https://app.americansocceranalysis.com/api/v1/{league_slug}/stadia`

**Valid URL:** [https://app.americansocceranalysis.com/api/v1/mls/stadia](https://app.americansocceranalysis.com/api/v1/mls/stadia)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `stadium_id` | character | ASA stadium id (base62 string; Utf8 join key). |
| `stadium_name` | character | Venue name. |
| `capacity` | integer | Seating capacity. |
| `year_built` | integer | Year the venue opened. |
| `roof` | logical | Whether the venue has a roof. |
| `turf` | logical | Whether the playing surface is artificial turf. |
| `street` | character | Street address. |
| `city` | character | City. |
| `province` | character | State/province. |
| `country` | character | Country. |
| `postal_code` | character | Postal/ZIP code. |
| `latitude` | numeric | Latitude (decimal degrees). |
| `longitude` | numeric | Longitude (decimal degrees). |
| `field_x` | integer | Pitch length (venue-reported field dimension). |
| `field_y` | integer | Pitch width (venue-reported field dimension). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
asa_stadia(league_slug='mls')
```

_Last validated n/a._

## `asa_teams`

Teams in the league (full table, no filter params).

**Endpoint URL:** `GET https://app.americansocceranalysis.com/api/v1/{league_slug}/teams`

**Valid URL:** [https://app.americansocceranalysis.com/api/v1/mls/teams](https://app.americansocceranalysis.com/api/v1/mls/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | character | ASA team id (base62 string; Utf8 join key, never numeric). |
| `team_name` | character | Full club name. |
| `team_short_name` | character | Short club name. |
| `team_abbreviation` | character | Short (2-4 char) club abbreviation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
asa_teams(league_slug='mls')
```

_Last validated n/a._

## `asa_teams_goals_added`

Per-team Goals Added for/against with a per-action-type data[] breakdown.

**Endpoint URL:** `GET https://app.americansocceranalysis.com/api/v1/{league_slug}/teams/goals-added`

**Valid URL:** [https://app.americansocceranalysis.com/api/v1/mls/teams/goals-added](https://app.americansocceranalysis.com/api/v1/mls/teams/goals-added)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |
| `season_name` | `season_name` |  |  | `Y` | Filter to one or more seasons. Comma-list accepted (`2022,2023`). |
| `stage_name` | `stage_name` |  |  | `Y` | Filter to a competition stage, e.g. `Regular Season`. **URL-encode spaces.** |
| `minimum_minutes` | `minimum_minutes` |  |  | `Y` | Drop players/teams below this minutes-played threshold. |
| `general_position` | `general_position` |  |  | `Y` | Filter by general position code: GK, CB, FB, DM, CM, AM, W, ST (player/GK routes). |
| `split_by_teams` | `split_by_teams` |  |  | `Y` | `true` => one row per entity per team (splits traded players). |
| `split_by_seasons` | `split_by_seasons` |  |  | `Y` | `true` => one row per entity per season. |
| `split_by_games` | `split_by_games` |  |  | `Y` | `true` => one row per entity per game. |
| `start_date` | `start_date` |  |  | `Y` | Lower date bound (`YYYY-MM-DD`), where the route supports date windows. |
| `end_date` | `end_date` |  |  | `Y` | Upper date bound (`YYYY-MM-DD`), where the route supports date windows. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | character | ASA team id (base62 string; Utf8 join key, never numeric). |
| `minutes` | integer | Team minutes in the window. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
asa_teams_goals_added(league_slug='mls')
```

_Last validated n/a._

## `asa_teams_xgoals`

Per-team expected-goals for/against + expected points.

**Endpoint URL:** `GET https://app.americansocceranalysis.com/api/v1/{league_slug}/teams/xgoals`

**Valid URL:** [https://app.americansocceranalysis.com/api/v1/mls/teams/xgoals](https://app.americansocceranalysis.com/api/v1/mls/teams/xgoals)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |
| `season_name` | `season_name` |  |  | `Y` | Filter to one or more seasons. Comma-list accepted (`2022,2023`). |
| `stage_name` | `stage_name` |  |  | `Y` | Filter to a competition stage, e.g. `Regular Season`. **URL-encode spaces.** |
| `minimum_minutes` | `minimum_minutes` |  |  | `Y` | Drop players/teams below this minutes-played threshold. |
| `general_position` | `general_position` |  |  | `Y` | Filter by general position code: GK, CB, FB, DM, CM, AM, W, ST (player/GK routes). |
| `split_by_teams` | `split_by_teams` |  |  | `Y` | `true` => one row per entity per team (splits traded players). |
| `split_by_seasons` | `split_by_seasons` |  |  | `Y` | `true` => one row per entity per season. |
| `split_by_games` | `split_by_games` |  |  | `Y` | `true` => one row per entity per game. |
| `start_date` | `start_date` |  |  | `Y` | Lower date bound (`YYYY-MM-DD`), where the route supports date windows. |
| `end_date` | `end_date` |  |  | `Y` | Upper date bound (`YYYY-MM-DD`), where the route supports date windows. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | character | ASA team id (base62 string; Utf8 join key, never numeric). |
| `count_games` | integer | Games included in the window. |
| `shots_for` | integer | Shots taken. |
| `shots_against` | integer | Shots faced. |
| `goals_for` | integer | Goals scored. |
| `goals_against` | integer | Goals conceded. |
| `goal_difference` | integer | goals_for - goals_against. |
| `xgoals_for` | numeric | Expected goals created. |
| `xgoals_against` | numeric | Expected goals conceded. |
| `xgoal_difference` | numeric | xgoals_for - xgoals_against. |
| `goal_difference_minus_xgoal_difference` | numeric | Finishing/keeping over expectation. |
| `points` | integer | Actual league points earned. |
| `xpoints` | numeric | Expected league points. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
asa_teams_xgoals(league_slug='mls')
```

_Last validated n/a._

## `asa_teams_xpass`

Per-team expected-passing (completion over expected, vertical distance).

**Endpoint URL:** `GET https://app.americansocceranalysis.com/api/v1/{league_slug}/teams/xpass`

**Valid URL:** [https://app.americansocceranalysis.com/api/v1/mls/teams/xpass](https://app.americansocceranalysis.com/api/v1/mls/teams/xpass)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |
| `season_name` | `season_name` |  |  | `Y` | Filter to one or more seasons. Comma-list accepted (`2022,2023`). |
| `stage_name` | `stage_name` |  |  | `Y` | Filter to a competition stage, e.g. `Regular Season`. **URL-encode spaces.** |
| `minimum_minutes` | `minimum_minutes` |  |  | `Y` | Drop players/teams below this minutes-played threshold. |
| `general_position` | `general_position` |  |  | `Y` | Filter by general position code: GK, CB, FB, DM, CM, AM, W, ST (player/GK routes). |
| `split_by_teams` | `split_by_teams` |  |  | `Y` | `true` => one row per entity per team (splits traded players). |
| `split_by_seasons` | `split_by_seasons` |  |  | `Y` | `true` => one row per entity per season. |
| `split_by_games` | `split_by_games` |  |  | `Y` | `true` => one row per entity per game. |
| `start_date` | `start_date` |  |  | `Y` | Lower date bound (`YYYY-MM-DD`), where the route supports date windows. |
| `end_date` | `end_date` |  |  | `Y` | Upper date bound (`YYYY-MM-DD`), where the route supports date windows. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | character | ASA team id (base62 string; Utf8 join key, never numeric). |
| `count_games` | integer | Games included in the window. |
| `attempted_passes_for` | integer | Passes attempted by the team. |
| `pass_completion_percentage_for` | numeric | Actual pass completion % (for). |
| `xpass_completion_percentage_for` | numeric | Expected pass completion % (for). |
| `passes_completed_over_expected_for` | numeric | Passes completed over expected (for). |
| `passes_completed_over_expected_p100_for` | numeric | Passes completed over expected per 100 passes (for). |
| `avg_vertical_distance_for` | numeric | Average vertical (goalward) pass distance (for). |
| `attempted_passes_against` | integer | Passes attempted by opponents. |
| `pass_completion_percentage_against` | numeric | Actual pass completion % (against). |
| `xpass_completion_percentage_against` | numeric | Expected pass completion % (against). |
| `passes_completed_over_expected_against` | numeric | Passes completed over expected (against). |
| `passes_completed_over_expected_p100_against` | numeric | Passes completed over expected per 100 passes (against). |
| `avg_vertical_distance_against` | numeric | Average vertical pass distance (against). |
| `passes_completed_over_expected_difference` | numeric | For minus against (passes over expected). |
| `avg_vertical_distance_difference` | numeric | For minus against (vertical distance). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
asa_teams_xpass(league_slug='mls')
```

_Last validated n/a._
