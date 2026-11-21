---
title: "College football — additional Python functions — Yahoo Sports Shangrila"
sidebar_label: "Yahoo Sports Shangrila"
sidebar_position: 5
description: "College football — additional Python functions — Yahoo Sports Shangrila — function reference in sdv-py, the SportsDataverse Python package."
---
# College football — additional Python functions — Yahoo Sports Shangrila

### yahoo_cfb_boxscore {#yahoo_cfb_boxscore}

`yahoo_cfb_boxscore(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

Yahoo CFB box score: player stats from the game lineups, one row per player stat.

Wraps the shangrila `playbookBoxscore` query. Each side's lineup lists
the players Yahoo features, each with up to 12 headline stats (passing,
rushing and receiving yards and touchdowns, receptions, tackles, sacks,
field goals and extra points made); names come from the payload's
`footballStatTypes`. Pivot on `stat_type_id` for a wide box.

Yahoo retired the editorial `boxscore` this used to wrap on 2026-11-20.
Shangrila has no full box score: there are no team-stat rows, a lineup
covers about a dozen players per side, and stat ids are shangrila names
(`PASSING_YARDS`) rather than `ncaaf.stat_type.<n>`. Pass
`return_parsed=False` for the raw payload, which also carries drives,
play-by-play and the win-probability timeline.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Dotted Yahoo game id (e.g. `"ncaaf.g.202509200023"`). |
| `return_parsed` | `bool` | `True` | If `True` (default) decode the box score into a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame by default (pandas when `return_as_pandas=True`) with one row per player stat, every column `Utf8`, and zero rows (same columns) for an empty payload; the raw `playbookBoxscore` JSON `dict` when `return_parsed=False`: | Column | Type | Description | |---|---|---| | `game_id` | Utf8 | Dotted Yahoo game id (`ncaaf.g.<date><n>`). | | `team_id` | Utf8 | Dotted Yahoo team id (`ncaaf.t.<n>`). | | `home_away` | Utf8 | `"home"` or `"away"`. | | `player_id` | Utf8 | Dotted Yahoo player id (`ncaaf.p.<n>`). | | `stat_category` | Utf8 | `Passing`, `Rushing`, `Receiving`, `Defense` or `Kicking`. | | `stat_type_id` | Utf8 | Shangrila stat id (`PASSING_YARDS`). | | `stat_name` | Utf8 | Stat name (`Passing Yards`). | | `stat_abbreviation` | Utf8 | Short stat label. | | `stat_variation` | Utf8 | Always `Game`. | | `value` | Utf8 | Stat value as Yahoo sends it (`"188"`). |

| col_name | type | description |
|---|---|---|
| `game_id` | String | ESPN game identifier. |
| `team_id` | String | ESPN team id. |
| `home_away` | String | `home` or `away`. |
| `player_id` | String | ESPN player id from the roster entry. |
| `stat_category` | String |  |
| `stat_type_id` | String |  |
| `stat_name` | String | Internal stat key (e.g. `passingYards`). |
| `stat_abbreviation` | String |  |
| `stat_variation` | String |  |
| `value` | String | Metric value. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | Yahoo answered 404. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_boxscore
box = yahoo_cfb_boxscore("ncaaf.g.202509200023")

# Raw JSON (includes drives and play-by-play)

raw = yahoo_cfb_boxscore("ncaaf.g.202509200023", return_parsed=False)

# Wide player box (one line)

box.pivot("stat_type_id", index="player_id", values="value")
```

### yahoo_cfb_player_season_stats {#yahoo_cfb_player_season_stats}

`yahoo_cfb_player_season_stats(season: 'int' = 2024, *, league_structure: 'str' = 'ncaaf.struct.div.1', count: 'int' = 200, qualified: 'bool' = False, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

Yahoo CFB player season stats (modern; one wide row per player).

Wraps the shangrila `leagueStatsIndividual` query, which returns every
stat group (passing/rushing/receiving/...) in one call, pivoted wide with
one column per `statId`. NCAAF data is available 2013-present.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | Season year (2013-present). Defaults to `2024`. |
| `league_structure` | `str` | `'ncaaf.struct.div.1'` | Yahoo league-structure id (division filter). Defaults to `"ncaaf.struct.div.1"` (FBS). |
| `count` | `int` | `200` | Maximum number of players to request. Defaults to `200`. |
| `qualified` | `bool` | `False` | Restrict to qualified leaders only. Defaults to `False`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A wide polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`. Includes a self-describing `season` column.

| col_name | type | description |
|---|---|---|
| `player_id` | String | ESPN player id from the roster entry. |
| `display_name` | String | Human-readable metric name. |
| `team` | String | Team name. |
| `team_abbreviation` | String | Team abbreviation. |
| `passing_completions` | String | Pass completions (split from CFBD's `C/ATT` field). |
| `rushing_attempts_per_game` | String |  |
| `sacks_yards_lost` | String |  |
| `solo_tackles` | String |  |
| `kickoff_return_touchdowns` | String |  |
| `field_goals_30_to_39` | String |  |
| `longest_punt` | String |  |
| `passing_touchdowns` | String |  |
| `field_goals_20_to_29` | String |  |
| `kickoff_returns` | String |  |
| `field_goals_made_20_29` | String |  |
| `games_rushing` | String |  |
| `games_defense` | String |  |
| `forced_fumbles` | String |  |
| `interception_return_yards` | String |  |
| `punt_return_touchdowns` | String |  |
| `longest_field_goal` | String |  |
| `sacks_taken` | String |  |
| `receiving_yards_per_reception` | String |  |
| `receiving_yards` | String |  |
| `field_goals_made_30_39` | String |  |
| `field_goals_made_50_plus` | String |  |
| `games_offense` | String |  |
| `punts` | String |  |
| `field_goals_made` | String |  |
| `field_goals_50_plus` | String |  |
| `all_purpose_yards` | String |  |
| `interception_return_touchdowns` | String |  |
| `field_goal_percentage` | String |  |
| `field_goals_made_0_19` | String |  |
| `rushing_touchdowns` | String |  |
| `punt_yards_per_punt` | String |  |
| `field_goal_attempts_40_49` | String |  |
| `sacks` | String | Team sacks. |
| `field_goals_made_40_49` | String |  |
| `longest_rush` | String |  |
| `games_returns` | String |  |
| `return_yards_per_kickoff` | String |  |
| `rushing_yards_per_game` | String |  |
| `longest_reception` | String |  |
| `rushing_yards` | String | Team rushing yards. |
| `games_kicking` | String |  |
| `passing_yards_per_attempt` | String |  |
| `rushing_yards_per_attempt` | String |  |
| `completion_percentage` | String |  |
| `total_tackles` | String |  |
| `games_punting` | String |  |
| `passing_yards_per_game` | String |  |
| `passes_defended` | String |  |
| `longest_pass` | String |  |
| `points_scored_kicking` | String |  |
| `passing_yards` | String |  |
| `receiving_yards_per_game` | String |  |
| `rushing_attempts` | String | Team rushing attempts. |
| `games_passing` | String |  |
| `field_goal_attempts_50_plus` | String |  |
| `targets` | String |  |
| `qb_rating` | String |  |
| `games_receiving` | String |  |
| `longest_kickoff_return` | String |  |
| `safeties` | String |  |
| `tackle_assists` | String |  |
| `passing_attempts` | String | Pass attempts (split from CFBD's `C/ATT` field). |
| `extra_point_attempts` | String |  |
| `interceptions_forced` | String |  |
| `extra_points_made` | String |  |
| `punt_yards` | String |  |
| `receptions` | String |  |
| `field_goal_attempts_30_39` | String |  |
| `field_goals_40_to_49` | String |  |
| `punt_return_yards` | String | Team punt return yards. |
| `field_goal_attempts` | String |  |
| `longest_punt_return` | String |  |
| `field_goal_attempts_20_29` | String |  |
| `field_goal_attempts_0_19` | String |  |
| `kickoff_return_yards` | String |  |
| `punt_returns` | String | Number of punt returns. |
| `sacks_yards` | String |  |
| `field_goals_0_to_19` | String |  |
| `receiving_touchdowns` | String |  |
| `extra_point_percentage` | String |  |
| `tackles_for_loss` | String | Team tackles for a loss. |
| `passing_interceptions` | String |  |
| `return_yards_per_punt` | String |  |
| `season` | Int64 | Season (4-digit year). |

**Raises**

| Exception | Description |
|---|---|
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_player_season_stats
df = yahoo_cfb_player_season_stats(season=2024)
```

### yahoo_cfb_player_season_stats_legacy {#yahoo_cfb_player_season_stats_legacy}

`yahoo_cfb_player_season_stats_legacy(season: 'int' = 2024, category: 'str' = 'Passing', sort_stat: 'str' = 'PASSING_YARDS', *, league_structure: 'str' = 'ncaaf.struct.div.1', count: 'int' = 200, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

Yahoo CFB legacy per-category player leaders (one wide row per player).

Wraps the legacy `seasonStatsFootball{Category}Ncaaf` query (one stat
category per call), pivoted wide with one column per `statId`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | Season year (2013-present). Defaults to `2024`. |
| `category` | `str` | `'Passing'` | Stat category, one of `{"Passing", "Rushing", "Receiving", "Defense", "Kicking", "Punting", "Returns"}`. Defaults to `"Passing"`. |
| `sort_stat` | `str` | `'PASSING_YARDS'` | Required `FootballStatId` to sort by (see the catalog vocab). Defaults to `"PASSING_YARDS"`. |
| `league_structure` | `str` | `'ncaaf.struct.div.1'` | Yahoo league-structure id (division filter). Defaults to `"ncaaf.struct.div.1"` (FBS). |
| `count` | `int` | `200` | Maximum number of players to request. Defaults to `200`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A wide polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`. Includes self-describing `season` and `category` columns.

| col_name | type | description |
|---|---|---|
| `player_id` | String | ESPN player id from the roster entry. |
| `display_name` | String | Human-readable metric name. |
| `team` | String | Team name. |
| `team_abbreviation` | String | Team abbreviation. |
| `games_rushing` | String |  |
| `rushing_attempts` | String | Team rushing attempts. |
| `rushing_yards` | String | Team rushing yards. |
| `rushing_yards_per_game` | String |  |
| `rushing_yards_per_attempt` | String |  |
| `rushing_touchdowns` | String |  |
| `longest_rush` | String |  |
| `season` | Int64 | Season (4-digit year). |
| `category` | String | CFBD stats category name (e.g. passing, rushing, defensive). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `category` is not one of `LEGACY_PLAYER_CATEGORIES`. |

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_player_season_stats_legacy
df = yahoo_cfb_player_season_stats_legacy(
    season=2024, category="Rushing", sort_stat="RUSHING_YARDS"
)
```

### yahoo_cfb_scoreboard {#yahoo_cfb_scoreboard}

`yahoo_cfb_scoreboard(season: 'int', week: 'int' = 1, *, count: 'int' = 500, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

Yahoo CFB scoreboard (one row per game).

Two shangrila calls: `leagueGameIdsByDate` lists the week's game ids
(every division Yahoo carries, FBS through NAIA), then `scoreboardGame`
fetches those games in one comma-joined batch. `season` is required --
there is no meaningful default for a weekly scoreboard. Postseason games
carry on the week count (bowls and the CFP run to about week 21); a week
with no games returns zero rows. Unscheduled postseason slots are listed
too, as `TBD @ TBD` games whose team ids are both `ncaaf.t.0`.

Yahoo retired the editorial `scoreboard` this used to wrap on
2026-11-20. Shangrila has no counterpart for its `global_gameid`, and
the columns are new: they are listed below.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season year (required). |
| `week` | `int` | `1` | Schedule week number. Defaults to `1`. |
| `count` | `int` | `500` | Ignored. Kept so existing calls still work; `leagueGameIdsByDate` returns the whole week. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the games to a DataFrame; if `False` return `{"data": {"games": [...]}}`, the merged raw `scoreboardGame` entries (odds, records and venue included). |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default; pandas when `return_as_pandas=True`) with one row per game, and zero rows with the same columns for an empty week: | Column | Type | Description | |---|---|---| | `game_id` | Utf8 | Dotted Yahoo game id (`ncaaf.g.202508230021`). | | `display_name` | Utf8 | `"Away @ Home"` label. | | `start_time` | Utf8 | Kickoff, ISO-8601 with the US Central offset; null while the time is TBA. | | `start_date` | Utf8 | Kickoff date with offset (`2025-08-23-05:00`). | | `status` | Utf8 | `PREGAME`, `FINAL`, or an in-game status while live. | | `status_display_name` | Utf8 | Display status (`Final`). | | `season` | Int64 | Season year. | | `season_phase` | Utf8 | `REGULAR_SEASON` or `POSTSEASON`. | | `week` | Int64 | Yahoo week number. | | `home_team_id` | Utf8 | Dotted home team id (`ncaaf.t.<n>`). | | `home_team` | Utf8 | Home team full name (`Kansas State Wildcats`). | | `home_abbreviation` | Utf8 | Home team abbreviation. | | `home_score` | Int64 | Home points; null before kickoff. | | `away_team_id` | Utf8 | Dotted away team id. | | `away_team` | Utf8 | Away team full name. | | `away_abbreviation` | Utf8 | Away team abbreviation. | | `away_score` | Int64 | Away points; null before kickoff. | | `winning_team_id` | Utf8 | Dotted id of the winner; null until final. | | `display_result` | Utf8 | Score line (`24-21`). | | `venue` | Utf8 | Stadium name. |

| col_name | type | description |
|---|---|---|
| `game_id` | String | ESPN game identifier. |
| `display_name` | String | Human-readable metric name. |
| `start_time` | String | Game start time. |
| `start_date` | String | Season start timestamp (ISO 8601, UTC). |
| `status` | String | Game status (e.g. "scheduled", "in_progress", "completed"). |
| `status_display_name` | String | Short game or event status as shown on the scoreboard (e.g., "Final", "12:00 pm ET"). |
| `season` | Int64 | Season (4-digit year). |
| `season_phase` | String | Phase of the season the game falls in (e.g., "season.phase.season"). |
| `week` | Int64 | Game week of the season. |
| `home_team_id` | String | ESPN home team id (parsed from `home_team_ref`). |
| `home_team` | String | Home team name. |
| `home_abbreviation` | String |  |
| `home_score` | Int64 | Home-team score after the play. |
| `away_team_id` | String | ESPN away team id (parsed from `away_team_ref`). |
| `away_team` | String | Away team name. |
| `away_abbreviation` | String |  |
| `away_score` | Int64 | Away-team score after the play. |
| `winning_team_id` | String | Composite Yahoo team id of the side that won the game (e.g., "ncaaf.t.29"). |
| `display_result` | String | Drive-result label (e.g. `Punt`, `Touchdown`). |
| `venue` | String | Venue name. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | Yahoo answered 404. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_scoreboard
df = yahoo_cfb_scoreboard(season=2024, week=1)
```

### yahoo_cfb_team_season_stats {#yahoo_cfb_team_season_stats}

`yahoo_cfb_team_season_stats(season: 'int' = 2024, *, league_structure: 'str' = 'ncaaf.struct.div.1', count: 'int' = 200, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

Yahoo CFB team season stats (modern; one wide row per team).

Wraps the shangrila `leagueStatsByTeam` query (all stat groups in one
call, pivoted wide with one column per `statId`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | Season year (2013-present). Defaults to `2024`. |
| `league_structure` | `str` | `'ncaaf.struct.div.1'` | Yahoo league-structure id (division filter). Defaults to `"ncaaf.struct.div.1"` (FBS). |
| `count` | `int` | `200` | Maximum number of teams to request. Defaults to `200`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A wide polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`. Includes a self-describing `season` column.

| col_name | type | description |
|---|---|---|
| `team` | String | Team name. |
| `team_abbreviation` | String | Team abbreviation. |
| `receiving_yards_rank` | String |  |
| `passing_touchdowns` | String |  |
| `rushing_attempts` | String | Team rushing attempts. |
| `rushing_yards_allowed_per_attempt` | String |  |
| `points_per_game` | String |  |
| `receiving_first_downs_allowed` | String |  |
| `receiving_yards_allowed_per_game` | String |  |
| `passing_yards_per_game` | String |  |
| `total_offensive_yards_per_game` | String |  |
| `interceptions_forced_rank` | String |  |
| `rushing_yards_allowed_per_game` | String |  |
| `total_offensive_yards_per_game_rank` | String |  |
| `receiving_yards` | String |  |
| `points_per_game_rank` | String |  |
| `rushing_yards_rank` | String |  |
| `receiving_yards_allowed` | String |  |
| `rushing_touchdowns` | String |  |
| `points_allowed_per_game_rank` | String |  |
| `third_down_attempts` | String |  |
| `sacks_taken` | String |  |
| `total_yards_allowed_per_game_rank` | String |  |
| `rushing_yards_per_attempt` | String |  |
| `rushing_touchdowns_allowed_per_game` | String |  |
| `longest_pass` | String |  |
| `completion_percentage` | String |  |
| `receptions` | String |  |
| `receiving_yards_per_reception` | String |  |
| `interceptions_forced` | String |  |
| `passing_completions_per_game` | String |  |
| `team_penalties` | String |  |
| `rushing_first_downs_allowed` | String |  |
| `receiving_yards_per_game` | String |  |
| `points_allowed` | String | Points for the opponent. |
| `points_allowed_rank` | String |  |
| `total_yards_allowed_per_game` | String |  |
| `rushing_attempts_allowed` | String | Opponent rushing attempts. |
| `games_kicking` | String |  |
| `completion_percentage_allowed` | String |  |
| `rushing_yards_allowed_per_game_rank` | String |  |
| `receiving_touchdowns` | String |  |
| `games_punting` | String |  |
| `total_offensive_yards` | String |  |
| `points` | String | Total points accumulated by the school in the poll's weighted voting. |
| `rushing_yards_per_game_rank` | String |  |
| `passing_yards_per_attempt` | String |  |
| `receptions_per_game` | String |  |
| `rushing_first_downs` | String |  |
| `receiving_first_downs` | String |  |
| `passing_first_downs` | String |  |
| `points_rank` | String |  |
| `passing_yards_allowed_per_game` | String |  |
| `fourth_down_conversions` | String |  |
| `receptions_allowed` | String |  |
| `passing_attempts_per_game` | String |  |
| `passing_yards` | String |  |
| `rushing_touchdowns_allowed` | String |  |
| `receiving_touchdowns_allowed` | String |  |
| `receptions_allowed_per_game` | String |  |
| `rushing_yards_allowed` | String | Opponent rushing yards. |
| `games_receiving` | String |  |
| `sacks_rank` | String |  |
| `points_allowed_per_game` | String |  |
| `rushing_yards` | String | Team rushing yards. |
| `games_rushing` | String |  |
| `longest_rush` | String |  |
| `games_offense` | String |  |
| `fourth_down_conversion_percentage` | String |  |
| `receiving_touchdowns_allowed_per_game` | String |  |
| `passing_attempts_allowed_per_game` | String |  |
| `passing_first_downs_allowed` | String |  |
| `passing_completions` | String | Pass completions (split from CFBD's `C/ATT` field). |
| `total_offensive_yards_rank` | String |  |
| `time_of_possession_per_game_rank` | String |  |
| `offensive_penalty_yards_lost` | String |  |
| `team_penalty_yards_lost` | String |  |
| `passing_yards_per_game_rank` | String |  |
| `interception_return_touchdowns` | String |  |
| `passing_yards_allowed_per_game_rank` | String |  |
| `sacks_yards_lost` | String |  |
| `games_defense` | String |  |
| `passing_yards_allowed` | String |  |
| `passing_touchdowns_allowed` | String |  |
| `games_passing` | String |  |
| `passing_completions_allowed_per_game` | String |  |
| `passing_touchdowns_allowed_per_game` | String |  |
| `rushing_yards_per_game` | String |  |
| `fourth_down_attempts` | String |  |
| `passing_yards_rank` | String |  |
| `rushing_attempts_per_game` | String |  |
| `third_down_conversion_percentage` | String |  |
| `passing_attempts` | String | Pass attempts (split from CFBD's `C/ATT` field). |
| `passing_interceptions` | String |  |
| `first_downs` | String | First downs earned by the team. |
| `first_downs_per_game` | String |  |
| `rushing_attempts_allowed_per_game` | String |  |
| `third_down_conversions` | String |  |
| `time_of_possession_per_game` | String |  |
| `offensive_penalties` | String |  |
| `season` | Int64 | Season (4-digit year). |

**Raises**

| Exception | Description |
|---|---|
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_team_season_stats
df = yahoo_cfb_team_season_stats(season=2024)
```

### yahoo_cfb_team_season_stats_legacy {#yahoo_cfb_team_season_stats_legacy}

`yahoo_cfb_team_season_stats_legacy(season: 'int' = 2024, category: 'str' = 'Passing', sort_stat: 'str' = 'PASSING_YARDS', *, league_structure: 'str' = 'ncaaf.struct.div.1', count: 'int' = 200, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

Yahoo CFB legacy per-category team stats (one wide row per team).

Wraps the legacy `seasonTeamStatsFootball{Category}` query (one stat
category per call), pivoted wide with one column per `statId`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | Season year (2013-present). Defaults to `2024`. |
| `category` | `str` | `'Passing'` | Stat category, one of `{"Passing", "Rushing", "Receiving", "Defense", "Kicking", "Punting", "Returns", "Kickoffs", "Offense"}`. Defaults to `"Passing"`. |
| `sort_stat` | `str` | `'PASSING_YARDS'` | Required `FootballStatId` to sort by. Defaults to `"PASSING_YARDS"`. |
| `league_structure` | `str` | `'ncaaf.struct.div.1'` | Yahoo league-structure id (division filter). Defaults to `"ncaaf.struct.div.1"` (FBS). |
| `count` | `int` | `200` | Maximum number of teams to request. Defaults to `200`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A wide polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`. Includes self-describing `season` and `category` columns.

| col_name | type | description |
|---|---|---|
| `team` | String | Team name. |
| `team_abbreviation` | String | Team abbreviation. |
| `games_rushing` | String |  |
| `rushing_attempts` | String | Team rushing attempts. |
| `rushing_yards` | String | Team rushing yards. |
| `rushing_yards_per_attempt` | String |  |
| `rushing_attempts_per_game` | String |  |
| `rushing_yards_per_game` | String |  |
| `rushing_touchdowns` | String |  |
| `rushing_first_downs` | String |  |
| `longest_rush` | String |  |
| `rushing_fumbles` | String |  |
| `rushing_fumbles_lost` | String |  |
| `season` | Int64 | Season (4-digit year). |
| `category` | String | CFBD stats category name (e.g. passing, rushing, defensive). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `category` is not one of `LEGACY_TEAM_CATEGORIES`. |

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_team_season_stats_legacy
df = yahoo_cfb_team_season_stats_legacy(
    season=2024, category="Rushing", sort_stat="RUSHING_YARDS"
)
```

### yahoo_cfb_teams {#yahoo_cfb_teams}

`yahoo_cfb_teams(season: 'int', week: 'int' = 1, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

Yahoo CFB team directory (one row per team).

Wraps the shangrila `leagueTeams` query (`league=ncaaf`): every team
Yahoo carries, about 540 across FBS, FCS, D2, D3, NAIA and USCAA, keyed by
the dotted `ncaaf.t.<n>` id. It is the Yahoo side of
`sportsdataverse.cfb.cfb_teams_crosswalk`.

Until 2026-11-20 this read the team map embedded in the editorial
scoreboard (about 265 FBS + FCS teams). Yahoo retired that feed. Shangrila
has no conference abbreviation and no SeatGeek id, so
`conference_abbreviation` and `seatgeek_id` stay as all-null columns,
and `conference_id` / `division_id` are shangrila's own ids (ACC is
`"1"`), not the editorial ones.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season year. Sent to Yahoo, but `leagueTeams` answers with the current directory for any season (2004 and 2025 return the same ids). |
| `week` | `int` | `1` | Ignored. Kept so existing calls still work. |
| `return_parsed` | `bool` | `True` | If `True` (default) project the teams to a DataFrame; if `False` return the raw `leagueTeams` JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default; pandas when `return_as_pandas=True`) with one row per team, every column `Utf8`: | Column | Type | Description | |---|---|---| | `team_id` | Utf8 | Dotted Yahoo team id (`ncaaf.t.1`). | | `abbreviation` | Utf8 | Team abbreviation (`CLEM`). | | `display_name` | Utf8 | Short name (`Clemson`). | | `full_name` | Utf8 | School and nickname (`Clemson Tigers`). | | `location` | Utf8 | School / place name. | | `nickname` | Utf8 | Team nickname (`Tigers`). | | `conference` | Utf8 | Conference name (`Atlantic Coast`). | | `conference_abbreviation` | Utf8 | Always null: shangrila carries none. | | `conference_id` | Utf8 | Shangrila conference id. | | `division` | Utf8 | `FBS`, `FCS`, `D2`, `D3`, `NAIA` or `USCAA`. | | `division_id` | Utf8 | Shangrila division id (`"1"` is FBS). | | `seatgeek_id` | Utf8 | Always null: shangrila carries none. |

| col_name | type | description |
|---|---|---|
| `team_id` | String | ESPN team id. |
| `abbreviation` | String | Metric abbreviation. |
| `display_name` | String | Human-readable metric name. |
| `full_name` | String | Venue full name (e.g. `Tenney Stadium`). |
| `location` | String | Team location / school name. |
| `nickname` | String | Team nickname / location label. |
| `conference` | String | Conference of the team. |
| `conference_abbreviation` | String |  |
| `conference_id` | String | Referencing conference id. |
| `division` | String | Division in the conference for the team. |
| `division_id` | String |  |
| `seatgeek_id` | String | SeatGeek performer or event identifier used to build the ticket-purchase link. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | Yahoo answered 404. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_teams
teams = yahoo_cfb_teams(season=2024)
abbr = dict(zip(teams["team_id"], teams["abbreviation"]))
```
