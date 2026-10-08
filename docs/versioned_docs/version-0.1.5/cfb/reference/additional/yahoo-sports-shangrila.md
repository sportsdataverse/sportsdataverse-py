---
title: "CFB — additional Python functions — Yahoo Sports Shangrila"
sidebar_label: "Yahoo Sports Shangrila"
sidebar_position: 5
description: "CFB — additional Python functions — Yahoo Sports Shangrila — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — Yahoo Sports Shangrila

### yahoo_cfb_boxscore {#yahoo_cfb_boxscore}

`yahoo_cfb_boxscore(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

Yahoo CFB box score: team and player stats, one row per entity stat.

Wraps the editorial `boxscore/{game_id}` resource. Its box score is a
decoder-dictionary schema
(`player_stats[playerId][variation][stat_type] = value`, same for
`team_stats`) that this decodes against the payload's `stat_types` /
`stat_categories` dictionaries into a long frame: one row per team stat
and per player stat. Pivot on `stat_type_id` for a wide box. The editorial
payload carries no player names; a player's team comes from the game's
home/away lineups. Pass `return_parsed=False` for the raw payload, which
also carries play-by-play and drives.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Dotted Yahoo game id (e.g. `"ncaaf.g.202509200023"`). |
| `return_parsed` | `bool` | `True` | If `True` (default) decode the box score into a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame by default (pandas when `return_as_pandas=True`) with one row per team or player stat, every column `Utf8`, and zero rows (same columns) for an empty payload; the raw editorial boxscore JSON `dict` when `return_parsed=False`: | Column | Type | Description | |---|---|---| | `game_id` | Utf8 | Dotted Yahoo game id (`ncaaf.g.<date><n>`). | | `team_id` | Utf8 | Dotted Yahoo team id (`ncaaf.t.<n>`); null for a player missing from the lineups. | | `home_away` | Utf8 | `"home"` or `"away"`. | | `player_id` | Utf8 | Dotted Yahoo player id (`ncaaf.p.<n>`); null on team-stat rows. | | `stat_category` | Utf8 | `Passing`, `Rushing`, `Receiving`, `Kicking`, `Returns`, `Punting`, `Defense` or `Team`. | | `stat_type_id` | Utf8 | Yahoo stat type id (`ncaaf.stat_type.105`). | | `stat_name` | Utf8 | Stat name (`Yards`, `Third Down Efficiency`). | | `stat_abbreviation` | Utf8 | Short stat label (`Yds`, `3DE`). | | `stat_variation` | Utf8 | Stat variation name (`Game`). | | `value` | Utf8 | Stat value as Yahoo sends it (`"188"`, `"73.2"`, `"1-14"`). |

| col_name | type | description |
|---|---|---|
| `game_id` | character | ESPN game identifier. |
| `team_id` | character | ESPN team id. |
| `home_away` | character | `home` or `away`. |
| `player_id` | character | ESPN player id from the roster entry. |
| `stat_category` | character |  |
| `stat_type_id` | character |  |
| `stat_name` | character | Internal stat key (e.g. `passingYards`). |
| `stat_abbreviation` | character |  |
| `stat_variation` | character |  |
| `value` | character | Metric value. |

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_boxscore
box = yahoo_cfb_boxscore("ncaaf.g.202509200023")

# Raw JSON (includes play-by-play and drives)

raw = yahoo_cfb_boxscore("ncaaf.g.202509200023", return_parsed=False)

# Wide team box (one line)

box.filter(pl.col("player_id").is_null()).pivot("stat_name", index="team_id", values="value")
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
| `player_id` | character | ESPN player id from the roster entry. |
| `display_name` | character | Human-readable metric name. |
| `team` | character | Team name. |
| `team_abbreviation` | character | Team abbreviation. |
| `passing_completions` | character | Pass completions (split from CFBD's `C/ATT` field). |
| `rushing_attempts_per_game` | character |  |
| `sacks_yards_lost` | character |  |
| `solo_tackles` | character |  |
| `kickoff_return_touchdowns` | character |  |
| `field_goals_30_to_39` | character |  |
| `longest_punt` | character |  |
| `passing_touchdowns` | character |  |
| `field_goals_20_to_29` | character |  |
| `kickoff_returns` | character |  |
| `field_goals_made_20_29` | character |  |
| `games_rushing` | character |  |
| `games_defense` | character |  |
| `forced_fumbles` | character |  |
| `interception_return_yards` | character |  |
| `punt_return_touchdowns` | character |  |
| `longest_field_goal` | character |  |
| `sacks_taken` | character |  |
| `receiving_yards_per_reception` | character |  |
| `receiving_yards` | character |  |
| `field_goals_made_30_39` | character |  |
| `field_goals_made_50_plus` | character |  |
| `games_offense` | character |  |
| `punts` | character |  |
| `field_goals_made` | character |  |
| `field_goals_50_plus` | character |  |
| `all_purpose_yards` | character |  |
| `interception_return_touchdowns` | character |  |
| `field_goal_percentage` | character |  |
| `field_goals_made_0_19` | character |  |
| `rushing_touchdowns` | character |  |
| `punt_yards_per_punt` | character |  |
| `field_goal_attempts_40_49` | character |  |
| `sacks` | character | Team sacks. |
| `field_goals_made_40_49` | character |  |
| `longest_rush` | character |  |
| `games_returns` | character |  |
| `return_yards_per_kickoff` | character |  |
| `rushing_yards_per_game` | character |  |
| `longest_reception` | character |  |
| `rushing_yards` | character | Team rushing yards. |
| `games_kicking` | character |  |
| `passing_yards_per_attempt` | character |  |
| `rushing_yards_per_attempt` | character |  |
| `completion_percentage` | character |  |
| `total_tackles` | character |  |
| `games_punting` | character |  |
| `passing_yards_per_game` | character |  |
| `passes_defended` | character |  |
| `longest_pass` | character |  |
| `points_scored_kicking` | character |  |
| `passing_yards` | character |  |
| `receiving_yards_per_game` | character |  |
| `rushing_attempts` | character | Team rushing attempts. |
| `games_passing` | character |  |
| `field_goal_attempts_50_plus` | character |  |
| `targets` | character |  |
| `qb_rating` | character |  |
| `games_receiving` | character |  |
| `longest_kickoff_return` | character |  |
| `safeties` | character |  |
| `tackle_assists` | character |  |
| `passing_attempts` | character | Pass attempts (split from CFBD's `C/ATT` field). |
| `extra_point_attempts` | character |  |
| `interceptions_forced` | character |  |
| `extra_points_made` | character |  |
| `punt_yards` | character |  |
| `receptions` | character |  |
| `field_goal_attempts_30_39` | character |  |
| `field_goals_40_to_49` | character |  |
| `punt_return_yards` | character | Team punt return yards. |
| `field_goal_attempts` | character |  |
| `longest_punt_return` | character |  |
| `field_goal_attempts_20_29` | character |  |
| `field_goal_attempts_0_19` | character |  |
| `kickoff_return_yards` | character |  |
| `punt_returns` | character | Number of punt returns. |
| `sacks_yards` | character |  |
| `field_goals_0_to_19` | character |  |
| `receiving_touchdowns` | character |  |
| `extra_point_percentage` | character |  |
| `tackles_for_loss` | character | Team tackles for a loss. |
| `passing_interceptions` | character |  |
| `return_yards_per_punt` | character |  |
| `season` | integer | Season (4-digit year). |

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
| `player_id` | character | ESPN player id from the roster entry. |
| `display_name` | character | Human-readable metric name. |
| `team` | character | Team name. |
| `team_abbreviation` | character | Team abbreviation. |
| `games_rushing` | character |  |
| `rushing_attempts` | character | Team rushing attempts. |
| `rushing_yards` | character | Team rushing yards. |
| `rushing_yards_per_game` | character |  |
| `rushing_yards_per_attempt` | character |  |
| `rushing_touchdowns` | character |  |
| `longest_rush` | character |  |
| `season` | integer | Season (4-digit year). |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |

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

Wraps the editorial `scoreboard` resource and flattens the `games` map.
`season` is required — there is no meaningful default for a weekly
scoreboard and the API has no concept of "current season". The full raw
payload also carries teams/leagues/odds maps (use `return_parsed=False`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season year (required). |
| `week` | `int` | `1` | Schedule week number. Defaults to `1`. |
| `count` | `int` | `500` | Maximum number of games to request. Defaults to `500`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the games map to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default) with one row per game, a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`. Includes self-describing `season` and `week` columns.

| col_name | type | description |
|---|---|---|
| `gameid` | character | Date-encoded Yahoo composite game id for this row (e.g., "ncaaf.g.202509200023"). |
| `global_gameid` | character | Yahoo cross-provider game id, distinct from the date-encoded gameid (e.g., "ncaaf.g.13556882"). |
| `start_time` | character | Game start time. |
| `is_time_tba` | logical | Flag indicating that the scheduled start time has not yet been announced. |
| `season_phase_id` | character | Identifier of the season phase the game falls in (e.g., "season.phase.season"). |
| `game_type` | character |  |
| `winning_team_id` | character | Composite Yahoo team id of the side that won the game (e.g., "ncaaf.t.29"). |
| `is_rank_upset` | logical | Flag indicating that the lower-ranked side won, judged against the teams' poll rankings. |
| `is_spread_upset` | logical | Flag indicating that the winning side was the betting underdog against the closing spread. |
| `outcome_type` | character | Outcome classification for a completed game (e.g., "outcome.type.won", "outcome.type.tied"). |
| `home_team_id` | character | ESPN home team id (parsed from `home_team_ref`). |
| `away_team_id` | character | ESPN away team id (parsed from `away_team_ref`). |
| `week_number` | character |  |
| `navigation_links` | character |  |
| `sportacular_url` | character | Deep link into the Yahoo Sportacular mobile app for this game (a "ysportacular://" URL). |
| `status_display_name` | character | Short game or event status as shown on the scoreboard (e.g., "Final", "12:00 pm ET"). |
| `status_description` | character |  |
| `status_type` | character | Status type. |
| `total_away_points` | character | Points scored by the away team in the game. |
| `current_period_id` | character | Ordinal number of the period currently in progress, counting from 1. |
| `total_home_points` | character | Points scored by the home team in the game. |
| `total_away_shootout_points` | character | Shootout goals converted by the away team, populated only for sports that break ties by shootout. |
| `total_home_shootout_points` | character | Shootout goals converted by the home team, populated only for sports that break ties by shootout. |
| `home_team_stats` | character | JSON-encoded team-stat block for the home team, populated once the game is under way. |
| `away_team_stats` | character | JSON-encoded team-stat block for the away team, populated once the game is under way. |
| `game_period_balls` | character | Balls in the count for the at-bat in progress; baseball only. |
| `game_period_strikes` | character | Strikes in the count for the at-bat in progress; baseball only. |
| `game_period_outs` | character | Outs recorded so far in the current half-inning; baseball only. |
| `yards_to_endzone` | character | Distance from the current ball spot to the opponent's goal line, in yards. |
| `start_yardline` | character | Yard line at the drive start. |
| `distance` | character | Yards to gain for a first down (or to the goal line in goal-to-go situations). |
| `down` | character | Down of the play (1-4). |
| `team_in_possession` | character | Yahoo team id of the side currently in possession of the ball. |
| `power_play_strength_home` | character | Number of skaters the home team has on the ice during special-teams play; hockey only. |
| `power_play_strength_away` | character | Number of skaters the away team has on the ice during special-teams play; hockey only. |
| `game_time_elapsed` | character | Playing time elapsed in the game, in seconds. |
| `game_time_elapsed_display` | character | Playing time elapsed formatted for display (e.g., "67:12"), used by sports whose clock counts up. |
| `inning_status` | character | Half-inning indicator for a game in progress (e.g., "Top", "Bottom"); baseball only. |
| `away_timeouts` | character | Away-team timeouts remaining. |
| `home_timeouts` | character | Home-team timeouts remaining. |
| `is_halftime` | character | Flag indicating that the game is currently stopped at halftime. |
| `minimum_periods` | integer | Number of periods a game of this sport runs before overtime is required (4 for football, 9 for baseball). |
| `game_periods` | integer | JSON-encoded list of the game's period nodes, each carrying a period number and its display names. |
| `baserunners` | character | JSON-encoded baserunner occupancy for the game in progress; baseball only. |
| `season` | integer | Season (4-digit year). |
| `subleague` | character | Sub-league the game belongs to, for leagues split into constituent circuits. |
| `subleague_display_name` | character | Display name of the sub-league the game belongs to. |
| `agg_score` | character | Aggregate score across the legs of a two-leg tie, populated only for competitions decided on aggregate. |
| `leg_number` | character | Ordinal of this leg within a multi-leg tie, counting from 1. |
| `tv_coverage` | character | Network carrying the game, as a short broadcast abbreviation (e.g., "CBS", "ESPN"). |
| `provider_coverage` | character |  |
| `seatgeek_id` | character | SeatGeek performer or event identifier used to build the ticket-purchase link. |
| `last_updated` | character | Timestamp ESPN last refreshed the power index. |
| `teams` | character |  |
| `play_by_play` | character | JSON-encoded data-island pointer to the game's play-by-play collection in the same editorial payload. |
| `pitches` | character | JSON-encoded data-island pointer to the game's pitch-level feed; baseball only. |
| `at_bat` | character | JSON-encoded data-island pointer to the game's current at-bat feed; baseball only. |
| `penalty_summary` | character |  |
| `scoring_summary` | character |  |
| `stat_categories` | character | JSON-encoded pointer to the stat-category dictionary that groups this feed's statistics. |
| `stadium` | character |  |
| `stadium_id` | character |  |
| `stadium_image` | character | JSON-encoded data-island pointer to the venue photograph used on the game page. |
| `attendance` | character | Reported attendance at the game. |
| `lineups` | character | JSON-encoded data-island pointer to the game's lineup collection. |
| `top_performer` | character | JSON-encoded data-island pointer to the game's top-performing players. |
| `players` | character |  |
| `byline` | character |  |
| `highlight` | character | JSON-encoded data-island pointer to the game's highlight video. |
| `highlights` | character | Game highlight urls. |
| `live_video` | character | JSON-encoded data-island pointer to the live video stream for the game. |
| `odds` | character | JSON-encoded data-island pointer to the game's odds collection. |
| `current_players` | character | JSON-encoded data-island pointer to the players currently on the field, ice or court. |
| `last_play` | character | Free-text description of the most recent play. |
| `series_type` | character | JSON-encoded data-island pointer to the kind of series the game belongs to. |
| `series_status` | character | JSON-encoded data-island pointer to the current state of the series the game belongs to. |
| `games` | character | Number of games included in the ATS summary. |
| `series_games` | character | JSON-encoded data-island pointer to the games making up the series. |
| `game_details` | character | JSON-encoded data-island pointer to supplementary detail notes for the game. |
| `section_notes` | character | JSON-encoded data-island pointer to editorial section notes attached to the game page. |
| `articles` | character | JSON-encoded data-island pointer to the editorial articles attached to the game. |
| `tweets` | character | JSON-encoded data-island pointer to the social posts attached to the game page. |
| `playoff_round` | character |  |
| `media_stream` | character | JSON-encoded data-island pointer to the game's media-stream collection. |
| `playoff_series_status` | character | JSON-encoded data-island pointer to the current state of the playoff series the game belongs to. |
| `playoff_series_details` | character | JSON-encoded data-island pointer to detail about the playoff series the game belongs to. |
| `drives` | character | JSON-encoded data-island pointer to the game's drive collection; football only. |
| `user_teams_game` | character | JSON-encoded data-island pointer to the viewer's followed-team context for the game. |
| `page_metadata` | character | JSON-encoded data-island pointer to the SEO and page metadata for the entity. |
| `penalty_box` | character | JSON-encoded data-island pointer to the game's penalty-box feed; hockey only. |
| `starting_pitchers` | character | JSON-encoded data-island pointer to the game's announced starting pitchers; baseball only. |
| `unrestricted_streams` | character | JSON-encoded data-island pointer to the streams viewable without a subscription. |
| `tv_details` | character | JSON-encoded list of broadcast entries for the game, each carrying a network abbreviation and full channel name (e.g., [{"abbr": "NBC", "name": "NBC/Peacock"}]). |
| `away_seed` | character |  |
| `home_seed` | character |  |
| `week` | integer | Game week of the season. |

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
| `team` | character | Team name. |
| `team_abbreviation` | character | Team abbreviation. |
| `receiving_yards_rank` | character |  |
| `passing_touchdowns` | character |  |
| `rushing_attempts` | character | Team rushing attempts. |
| `rushing_yards_allowed_per_attempt` | character |  |
| `points_per_game` | character |  |
| `receiving_first_downs_allowed` | character |  |
| `receiving_yards_allowed_per_game` | character |  |
| `passing_yards_per_game` | character |  |
| `total_offensive_yards_per_game` | character |  |
| `interceptions_forced_rank` | character |  |
| `rushing_yards_allowed_per_game` | character |  |
| `total_offensive_yards_per_game_rank` | character |  |
| `receiving_yards` | character |  |
| `points_per_game_rank` | character |  |
| `rushing_yards_rank` | character |  |
| `receiving_yards_allowed` | character |  |
| `rushing_touchdowns` | character |  |
| `points_allowed_per_game_rank` | character |  |
| `third_down_attempts` | character |  |
| `sacks_taken` | character |  |
| `total_yards_allowed_per_game_rank` | character |  |
| `rushing_yards_per_attempt` | character |  |
| `rushing_touchdowns_allowed_per_game` | character |  |
| `longest_pass` | character |  |
| `completion_percentage` | character |  |
| `receptions` | character |  |
| `receiving_yards_per_reception` | character |  |
| `interceptions_forced` | character |  |
| `passing_completions_per_game` | character |  |
| `team_penalties` | character |  |
| `rushing_first_downs_allowed` | character |  |
| `receiving_yards_per_game` | character |  |
| `points_allowed` | character | Points for the opponent. |
| `points_allowed_rank` | character |  |
| `total_yards_allowed_per_game` | character |  |
| `rushing_attempts_allowed` | character | Opponent rushing attempts. |
| `games_kicking` | character |  |
| `completion_percentage_allowed` | character |  |
| `rushing_yards_allowed_per_game_rank` | character |  |
| `receiving_touchdowns` | character |  |
| `games_punting` | character |  |
| `total_offensive_yards` | character |  |
| `points` | character | Total points accumulated by the school in the poll's weighted voting. |
| `rushing_yards_per_game_rank` | character |  |
| `passing_yards_per_attempt` | character |  |
| `receptions_per_game` | character |  |
| `rushing_first_downs` | character |  |
| `receiving_first_downs` | character |  |
| `passing_first_downs` | character |  |
| `points_rank` | character |  |
| `passing_yards_allowed_per_game` | character |  |
| `fourth_down_conversions` | character |  |
| `receptions_allowed` | character |  |
| `passing_attempts_per_game` | character |  |
| `passing_yards` | character |  |
| `rushing_touchdowns_allowed` | character |  |
| `receiving_touchdowns_allowed` | character |  |
| `receptions_allowed_per_game` | character |  |
| `rushing_yards_allowed` | character | Opponent rushing yards. |
| `games_receiving` | character |  |
| `sacks_rank` | character |  |
| `points_allowed_per_game` | character |  |
| `rushing_yards` | character | Team rushing yards. |
| `games_rushing` | character |  |
| `longest_rush` | character |  |
| `games_offense` | character |  |
| `fourth_down_conversion_percentage` | character |  |
| `receiving_touchdowns_allowed_per_game` | character |  |
| `passing_attempts_allowed_per_game` | character |  |
| `passing_first_downs_allowed` | character |  |
| `passing_completions` | character | Pass completions (split from CFBD's `C/ATT` field). |
| `total_offensive_yards_rank` | character |  |
| `time_of_possession_per_game_rank` | character |  |
| `offensive_penalty_yards_lost` | character |  |
| `team_penalty_yards_lost` | character |  |
| `passing_yards_per_game_rank` | character |  |
| `interception_return_touchdowns` | character |  |
| `passing_yards_allowed_per_game_rank` | character |  |
| `sacks_yards_lost` | character |  |
| `games_defense` | character |  |
| `passing_yards_allowed` | character |  |
| `passing_touchdowns_allowed` | character |  |
| `games_passing` | character |  |
| `passing_completions_allowed_per_game` | character |  |
| `passing_touchdowns_allowed_per_game` | character |  |
| `rushing_yards_per_game` | character |  |
| `fourth_down_attempts` | character |  |
| `passing_yards_rank` | character |  |
| `rushing_attempts_per_game` | character |  |
| `third_down_conversion_percentage` | character |  |
| `passing_attempts` | character | Pass attempts (split from CFBD's `C/ATT` field). |
| `passing_interceptions` | character |  |
| `first_downs` | character | First downs earned by the team. |
| `first_downs_per_game` | character |  |
| `rushing_attempts_allowed_per_game` | character |  |
| `third_down_conversions` | character |  |
| `time_of_possession_per_game` | character |  |
| `offensive_penalties` | character |  |
| `season` | integer | Season (4-digit year). |

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
| `team` | character | Team name. |
| `team_abbreviation` | character | Team abbreviation. |
| `games_rushing` | character |  |
| `rushing_attempts` | character | Team rushing attempts. |
| `rushing_yards` | character | Team rushing yards. |
| `rushing_yards_per_attempt` | character |  |
| `rushing_attempts_per_game` | character |  |
| `rushing_yards_per_game` | character |  |
| `rushing_touchdowns` | character |  |
| `rushing_first_downs` | character |  |
| `longest_rush` | character |  |
| `rushing_fumbles` | character |  |
| `rushing_fumbles_lost` | character |  |
| `season` | integer | Season (4-digit year). |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |

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

Yahoo has no standalone teams resource (the documented
`sports.league.teams` resource 404s without auth). Instead the editorial
`scoreboard` payload is "fat": one call embeds the full ~186-team
directory under `service.scoreboard.teams` keyed by the dotted
`ncaaf.t.<id>` team id. This wrapper pulls that map for the requested
`(season, week)` and projects it to the directory columns -- it is the
Yahoo side of `sportsdataverse.cfb.cfb_teams_crosswalk`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season year (required; the scoreboard is fetched to obtain the embedded teams map). |
| `week` | `int` | `1` | Schedule week used to fetch the scoreboard. Defaults to `1`. The embedded directory is the full league list regardless of week. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the teams map to a DataFrame; if `False` return the raw scoreboard JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default) with one row per team -- columns `team_id`, `abbreviation`, `display_name`, `full_name`, `location`, `nickname`, `conference`, `conference_abbreviation`, `conference_id`, `division`, `division_id`, `seatgeek_id` -- a pandas DataFrame when `return_as_pandas=True`, or the raw scoreboard JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `team_id` | character | ESPN team id. |
| `abbreviation` | character | Metric abbreviation. |
| `display_name` | character | Human-readable metric name. |
| `full_name` | character | Venue full name (e.g. `Tenney Stadium`). |
| `location` | character | Team location / school name. |
| `nickname` | character | Team nickname / location label. |
| `conference` | character | Conference of the team. |
| `conference_abbreviation` | character |  |
| `conference_id` | character | Referencing conference id. |
| `division` | character | Division in the conference for the team. |
| `division_id` | character |  |
| `seatgeek_id` | character | SeatGeek performer or event identifier used to build the ticket-purchase link. |

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_teams
teams = yahoo_cfb_teams(season=2024)
abbr = dict(zip(teams["team_id"], teams["abbreviation"]))
```
