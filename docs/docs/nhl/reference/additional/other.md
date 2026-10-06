---
title: "NHL — additional Python functions — Other"
sidebar_label: "Other"
sidebar_position: 5
description: "NHL — additional Python functions — Other — function reference in sdv-py, the SportsDataverse Python package."
---
# NHL — additional Python functions — Other

### load_nhl_games {#load_nhl_games}

`load_nhl_games(return_as_pandas: 'bool' = False)`

Load the NHL games-in-data-repo manifest (no `seasons` argument).

Mirrors fastRhockey (R) `load_nhl_games()` which reads a manifest of every
NHL game that has processed data in the data repository.

Tries the sportsdataverse-data release asset first; falls back to the raw
fastRhockey-data GitHub path.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame of all games in the data repository.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | Unique game identifier. |
| `season_full` | character | Full season label (e.g. 20212022). |
| `game_type` | character | Game type the row belongs to. |
| `game_date` | character | Game date. |
| `game_time` | character | Scheduled start time of the game. |
| `home_team_abbr` | character | Home team abbreviation. |
| `away_team_abbr` | character | Away team abbreviation. |
| `home_team_name` | character | Home team name. |
| `away_team_name` | character | Away team name. |
| `home_score` | integer | Home team final score. |
| `away_score` | integer | Away team final score. |
| `game_state` | character | Game state (e.g., FINAL, LIVE). |
| `venue` | character | Venue where the game was played. |
| `series_letter` | character | Single-letter identifier for the playoff series to which this game belongs, used to group games within the same bracket matchup in the NHL games dataset. |
| `playoff_round` | integer | Playoff round identifier. |
| `series_game_number` | integer | Series game number. |
| `season` | integer | Season year (echoed from arg). |
| `game_json` | logical | Whether processed game JSON is available. |
| `game_json_url` | character | URL to the processed game JSON. |
| `PBP` | logical | Whether play-by-play data is available. |
| `team_box` | logical | Whether team box score data is available. |
| `player_box` | logical | Whether player box score data is available. |
| `skater_box` | logical | Whether skater box data is available. |
| `goalie_box` | logical | Whether goalie box data is available. |
| `game_info` | logical | Whether game info data is available. |
| `game_rosters` | logical | Whether game rosters data is available. |
| `scoring` | logical | TRUE when the play results in a score (TD, FG, safety, two-point conversion). |
| `penalties` | logical | Penalty count. |
| `scratches` | logical | Logical flag indicating whether a scratches list (players healthy-scratched and not dressing) is available for this game in the NHL games loader output. |
| `linescore` | logical | Logical flag indicating whether linescore data (period-by-period scoring breakdown) is available for this game in the NHL games loader output. |
| `three_stars` | logical | Whether three stars data is available. |
| `shifts` | logical | Number of shifts. |
| `officials` | logical | Whether officials data is available. |
| `shots_by_period` | logical | Whether shots-by-period data is available. |
| `shootout` | logical | Whether shootout data is available. |

**Example**

```python
load_nhl_games()
```

### load_nhl_goalie_box {#load_nhl_goalie_box}

`load_nhl_goalie_box(seasons, return_as_pandas: 'bool' = False)`

Alias of load_nhl_goalie_boxscores() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### load_nhl_player_box {#load_nhl_player_box}

`load_nhl_player_box(seasons, return_as_pandas: 'bool' = False)`

Alias of load_nhl_player_boxscore() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### load_nhl_skater_box {#load_nhl_skater_box}

`load_nhl_skater_box(seasons, return_as_pandas: 'bool' = False)`

Alias of load_nhl_skater_boxscores() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### load_nhl_team_box {#load_nhl_team_box}

`load_nhl_team_box(seasons, return_as_pandas: 'bool' = False)`

Alias of load_nhl_team_boxscore() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### nhl_scoreboard {#nhl_scoreboard}

`nhl_scoreboard(date: 'Optional[str]' = None, team: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Dict'`

In-game scoreboard payload (renamed from `nhl_web_scoreboard`).

Picks among three mutually-exclusive NHL api-web forms (kept hand-written
because the URL-builder codegen can't represent the 3-way branch):

* `GET /v1/scoreboard/{team}/now` -- team-scoped now (when `team` set),
* `GET /v1/scoreboard/{date}` -- league-wide on a date,
* `GET /v1/scoreboard/now` -- league-wide now (both args None).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `Optional[str]` | `None` | `YYYY-MM-DD`; `None` -> `/now`. Mutually exclusive with `team`. |
| `team` | `Optional[str]` | `None` | 3-letter abbreviation; takes precedence over `date`. |
| `return_parsed` | `bool` | `True` | dispatch the raw payload through `parse_nhl_web_scoreboard`. |
| `return_as_pandas` | `bool` | `False` | with `return_parsed`, return pandas instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `scoreboard_date` | character | Calendar date (YYYY-MM-DD) for which this scoreboard snapshot was retrieved from the NHL api-web feed. |
| `id` | integer | Unique player identifier. |
| `season` | integer | Season year (echoed from arg). |
| `game_type` | integer | Game type the row belongs to. |
| `game_date` | character | Game date. |
| `game_center_link` | character | Link to the NHL game center page. |
| `start_time_utc` | character | Scheduled start time in UTC. |
| `eastern_utc_offset` | character | Eastern time UTC offset. |
| `venue_utc_offset` | character | Venue UTC offset. |
| `tv_broadcasts` | character | Nested list of TV broadcast details. |
| `game_state` | character | Game state (e.g., FINAL, LIVE). |
| `game_schedule_state` | character | Schedule state of the game. |
| `tickets_link` | character | URL to the English-language ticket purchase page for the game, as provided by the NHL api-web scoreboard. |
| `tickets_link_fr` | character | URL to the French-language ticket purchase page for the game, as provided by the NHL api-web scoreboard. |
| `period` | double | Period number. |
| `three_min_recap` | character | Link to the three-minute recap. |
| `three_min_recap_fr` | character | Link to the French three-minute recap. |
| `venue_default` | character | Venue name (default language). |
| `away_team_id` | integer | Away team identifier. |
| `away_team_name_default` | character | Full English-language team name for the away team, as returned by the NHL api-web scoreboard feed. |
| `away_team_name_fr` | character | Full French-language team name for the away team, as returned by the NHL api-web scoreboard feed. |
| `away_team_common_name_default` | character | Away team common name (default language). |
| `away_team_place_name_with_preposition_default` | character | Away team place name with preposition (default). |
| `away_team_place_name_with_preposition_fr` | character | Away team place name with preposition (French). |
| `away_team_abbrev` | character | Away team abbreviation. |
| `away_team_score` | double | Away team final score. |
| `away_team_logo` | character | URL to the away team logo. |
| `home_team_id` | integer | Home team identifier. |
| `home_team_name_default` | character | Full English-language team name for the home team, as returned by the NHL api-web scoreboard feed. |
| `home_team_name_fr` | character | Full French-language team name for the home team, as returned by the NHL api-web scoreboard feed. |
| `home_team_common_name_default` | character | Home team common name (default language). |
| `home_team_place_name_with_preposition_default` | character | Home team place name with preposition (default). |
| `home_team_place_name_with_preposition_fr` | character | Home team place name with preposition (French). |
| `home_team_abbrev` | character | Home team abbreviation. |
| `home_team_score` | double | Home team final score. |
| `home_team_logo` | character | URL to the home team logo. |
| `period_descriptor_number` | double | Period number. |
| `period_descriptor_period_type` | character | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | double | Maximum number of regulation periods. |
| `series_status_round` | integer | Playoff round number for this game's series (1 = first round, 2 = second round, etc.). |
| `series_status_series_abbrev` | character | Short abbreviation string identifying the specific playoff series matchup (e.g., 'A1' for a particular bracket slot). |
| `series_status_game` | integer | Game number within the current playoff series (e.g., 1 through 7) for the game represented in this scoreboard row. |
| `series_status_top_seed_team_abbrev` | character | Three-letter abbreviation for the higher-seeded team in the playoff series context embedded in the scoreboard game entry. |
| `series_status_top_seed_wins` | integer | Number of wins accumulated by the higher-seeded team in the current playoff series as of this scoreboard snapshot. |
| `series_status_bottom_seed_team_abbrev` | character | Three-letter abbreviation for the lower-seeded team in the playoff series context embedded in the scoreboard game entry. |
| `series_status_bottom_seed_wins` | integer | Number of wins accumulated by the lower-seeded team in the current playoff series as of this scoreboard snapshot. |
| `period_descriptor_ot_periods` | double | Number of overtime periods played when the game extended beyond regulation, as reported in the scoreboard period descriptor. |
| `away_team_record` | character | Away team's win-loss record. |
| `home_team_record` | character | Home team's win-loss record. |
| `away_team_common_name_fr` | character | Away team common name (French). |
| `home_team_common_name_fr` | character | Home team common name (French). |

**Example**

```python
nhl_scoreboard(date="2024-03-01")
```

### nhl_records_coach_milestone_wins {#nhl_records_coach_milestone_wins}

`nhl_records_coach_milestone_wins(wins: 'int', playoffs: 'bool' = False, **filters) -> 'Dict'`

Coaches who reached a wins milestone in fewest games.

Wraps one of the `/coach-fewest-games-to-{N}-wins` or
`/coach-fewest-games-to-{N}-playoff-wins` paths.

Supported *wins* values: `50, 100, 150, 200, 300, 400, 500, 600, 700,
800, 900, 1000` (regular season); `50, 100, 150` (playoffs).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `wins` | `int` |  | Milestone win total (e.g. `100`). |
| `playoffs` | `bool` | `False` | If `True`, use the playoff-wins path. |

**Returns**

Coaches who hit the milestone, sorted by games needed.

### nhl_records_comeback_wins {#nhl_records_comeback_wins}

`nhl_records_comeback_wins(scope: 'str' = 'league', **filters) -> 'Dict'`

Comeback wins from a multi-goal deficit.

Wraps:
  * `GET /comeback-league-wins` when *scope* is `"league"`.
  * `GET /comeback-franchise-wins` when *scope* is `"franchise"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scope` | `str` | `'league'` | `"league"` (default) or `"franchise"`. |

**Returns**

Games where the team overcame a deficit to win.

### nhl_records_consecutive_goal_seasons {#nhl_records_consecutive_goal_seasons}

`nhl_records_consecutive_goal_seasons(goals: 'int' = 50, **filters) -> 'Dict'`

Skaters with the most consecutive N-goal seasons.

Wraps one of:
  * `GET /consecutive-20-goal-seasons`
  * `GET /consecutive-30-goal-seasons`
  * `GET /consecutive-40-goal-seasons`
  * `GET /consecutive-50-goal-seasons`
  * `GET /consecutive-60-goal-seasons`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `goals` | `int` | `50` | Goal threshold — one of `20, 30, 40, 50, 60`. |

**Returns**

Skaters sorted by consecutive-season streak.

### nhl_records_fastest_goals {#nhl_records_fastest_goals}

`nhl_records_fastest_goals(n_goals: 'int' = 2, **filters) -> 'Dict'`

Fastest N goals by one team in a single game.

Wraps one of:
  * `GET /fastest-2-goals-one-team`
  * `GET /fastest-3-goals-one-team`
  * `GET /fastest-4-goals-one-team`
  * `GET /fastest-5-goals-one-team`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `n_goals` | `int` | `2` | Goal count — one of `2, 3, 4, 5`. |

**Returns**

Games where the milestone was set, sorted by elapsed time (fastest first).

### nhl_records_fastest_goals_both_teams {#nhl_records_fastest_goals_both_teams}

`nhl_records_fastest_goals_both_teams(n_goals: 'int' = 2, **filters) -> 'Dict'`

Fastest N goals combined (both teams) in a single game.

Wraps one of:
  * `GET /fastest-2-goals-both-teams`
  * `GET /fastest-3-goals-both-teams`
  * `GET /fastest-4-goals-both-teams`
  * `GET /fastest-5-goals-both-teams`
  * `GET /fastest-6-goals-both-teams`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `n_goals` | `int` | `2` | Combined goal count — one of `2, 3, 4, 5, 6`. |

**Returns**

Sorted by elapsed time (fastest first).

### nhl_records_games_played_streak_skaters {#nhl_records_games_played_streak_skaters}

`nhl_records_games_played_streak_skaters(active_only: 'bool' = False, **filters) -> 'Dict'`

Consecutive games-played streaks for skaters.

Wraps `GET /games-played-streak-skaters` (career) or
`GET /games-played-active-streak-skaters` (currently active streaks).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `active_only` | `bool` | `False` | If `True`, return only active streaks. |

**Returns**

Skaters sorted by streak length.

### build_design {#build_design}

`build_design(stints: 'pl.DataFrame') -> "tuple['sp.csr_matrix', np.ndarray, np.ndarray, list[int]]"`

Build the sparse RAPM design matrix -- two rows per stint (one per attacking team).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stints` | `DataFrame` |  | a `build_stints`-shaped frame. |

**Returns**

`(X, y, w, player_index)` where `X` is a `scipy.sparse.csr_matrix` with columns `off_<player>` (all on-ice attackers), `def_<player>` (all on-ice defenders), then a trailing home-ice indicator and intercept column; `y` is the attacking team's xGF per 60; `w` is stint duration (seconds); `player_index` maps each `off_`/`def_` column pair's position to a `player_id` (so column `j` is `off_<player_index[j]>` and column `j + n_players` is `def_<player_index[j]>`).

**Example**

```python
from sportsdataverse.nhl.nhl_rapm import build_design
X, y, w, player_index = build_design(stints)
```

### build_stints {#build_stints}

`build_stints(shifts: 'pl.DataFrame', scored: 'pl.DataFrame', *, as_of: 'int | None' = None) -> 'pl.DataFrame'`

Fold `load_nhl_shifts` CHANGE events into contiguous constant-personnel intervals.

Per game: resolves each shift row's full team name (`event_team`) to home/away via
`team_fullname_to_abbr` + the game's `home_abbr`/`away_abbr` (from `scored`),
then folds `ids_on`/`ids_off` deltas chronologically into a running on-ice set per
side. A new interval begins at every distinct `game_seconds` boundary; the final
interval is closed at the last `scored` event's `game_seconds` + 1 for that game
(there is no explicit "end of game" CHANGE row in the shift-chart feed).

Known simplification: shift-chart id lists do not distinguish position, so
`home_ids`/`away_ids` may include the on-ice goalie's id alongside skaters;
`home_goalie`/`away_goalie` are instead sourced from the overlapping `scored`
events' `home_goalie_id`/`away_goalie_id` (the modal value in the interval).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shifts` | `DataFrame` |  | a `load_nhl_shifts`-shaped frame. |
| `scored` | `DataFrame` |  | an `nhl_xg`-scored frame (for the game's `home_abbr`/`away_abbr` and each interval's on-ice xG-for and goalie). |
| `as_of` | `int \| None` | `None` | an optional per-game `game_seconds` cutoff -- intervals starting at or after `as_of` are dropped. This is the leakage boundary for any forward-looking use: features for a game/date must use only stints strictly before that game's cutoff. |

**Returns**

one row per interval -- `game_id:Int64, period:Int64, start_s:Int64, end_s:Int64, duration:Int64, home_ids:List(Int64), away_ids:List(Int64), home_goalie:Int64, away_goalie:Int64, strength_state:Utf8, xgf_home:Float64, xgf_away:Float64`. Empty/malformed `shifts` returns a zero-row frame with this schema.

**Example**

```python
import polars as pl
from sportsdataverse.nhl.nhl_xg import nhl_xg
from sportsdataverse.nhl.nhl_rapm import build_stints
pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
shifts = pl.read_parquet("tests/fixtures/nhl_player_impact/shifts_sample.parquet")
scored = nhl_xg(pbp, model_dir="tests/fixtures/nhl_player_impact/xg_models")
stints = build_stints(shifts, scored)
```

### espn_nhl_pbp {#espn_nhl_pbp}

`espn_nhl_pbp(game_id: 'int', raw=False, **kwargs) -> 'Dict'`

espn_nhl_pbp() - Pull the game by id. Data from API endpoints - `nhl/playbyplay`, `nhl/summary`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique ESPN event id (NOT the NHL native game id), can be obtained from nhl_schedule(). |
| `raw` |  | `False` |  |

**Returns**

Dictionary of game data with keys - "gameId", "plays", "boxscore", "header", "broadcasts", "videos", "playByPlaySource", "standings", "leaders", "seasonseries", "pickcenter", "againstTheSpread", "odds", "onIce", "gameInfo", "season"

**Example**

```python
from sportsdataverse.nhl import espn_nhl_pbp
game = espn_nhl_pbp(game_id=401559395)
list(game.keys())  # 'gameId', 'plays', 'boxscore', ...

# Inspect parsed plays and a quick filter on goal events

import polars as pl
plays = pl.DataFrame(game["plays"])
print(plays.shape)
goals = plays.filter(pl.col("type.text") == "Goal")
goals.select(["period", "time", "text"]).head()

# Pull the unparsed payload for custom downstream parsing

raw = espn_nhl_pbp(game_id=401559395, raw=True)
sorted(raw.keys())[:5]
```

### nhl_pbp_disk {#nhl_pbp_disk}

`nhl_pbp_disk(game_id, path_to_json)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  |  |
| `path_to_json` |  |  |  |

### most_recent_nhl_season {#most_recent_nhl_season}

`most_recent_nhl_season()`

most_recent_nhl_season - return the season year for "today".

NHL seasons are labeled by the year they end in. October flips the
label to next calendar year (the new season just started), otherwise
the current calendar year is returned.

**Returns**

A season year suitable for season-aware loaders / schedule helpers.

**Example**

```python
from sportsdataverse.nhl import most_recent_nhl_season, espn_nhl_calendar
season = most_recent_nhl_season()
cal = espn_nhl_calendar(season=season)
print(season, cal.height)
```

### year_to_season {#year_to_season}

`year_to_season(year)`

year_to_season - format a starting year as the canonical `YYYY-YY` season string.

NHL season strings (used by `statsapi` / `api-web.nhle.com`) are of the form
`"2023-24"`. This helper converts a starting year (`2023`) into that string.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` |  |  | Starting calendar year of the season (e.g. `2023`). |

**Returns**

Season string formatted as `"YYYY-YY"`.

**Example**

```python
from sportsdataverse.nhl import year_to_season
year_to_season(2023)  # '2023-24'
year_to_season(2009)  # '2009-10'
year_to_season(1999)  # '1999-00'
```
