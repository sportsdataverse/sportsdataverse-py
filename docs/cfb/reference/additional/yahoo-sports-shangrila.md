# College football — additional Python functions — Yahoo Sports Shangrila

> College football — additional Python functions — Yahoo Sports Shangrila — function reference in sdv-py, the SportsDataverse Python package.

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
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |

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
| `gameid` | String | Date-encoded Yahoo composite game id for this row (e.g., "ncaaf.g.202509200023"). |
| `global_gameid` | String | Yahoo cross-provider game id, distinct from the date-encoded gameid (e.g., "ncaaf.g.13556882"). |
| `start_time` | String | Game start time. |
| `is_time_tba` | Boolean | Flag indicating that the scheduled start time has not yet been announced. |
| `season_phase_id` | String | Identifier of the season phase the game falls in (e.g., "season.phase.season"). |
| `game_type` | String |  |
| `winning_team_id` | String | Composite Yahoo team id of the side that won the game (e.g., "ncaaf.t.29"). |
| `is_rank_upset` | Boolean | Flag indicating that the lower-ranked side won, judged against the teams' poll rankings. |
| `is_spread_upset` | Boolean | Flag indicating that the winning side was the betting underdog against the closing spread. |
| `outcome_type` | String | Outcome classification for a completed game (e.g., "outcome.type.won", "outcome.type.tied"). |
| `home_team_id` | String | ESPN home team id (parsed from `home_team_ref`). |
| `away_team_id` | String | ESPN away team id (parsed from `away_team_ref`). |
| `week_number` | String |  |
| `navigation_links` | String |  |
| `sportacular_url` | String | Deep link into the Yahoo Sportacular mobile app for this game (a "ysportacular://" URL). |
| `status_display_name` | String | Short game or event status as shown on the scoreboard (e.g., "Final", "12:00 pm ET"). |
| `status_description` | String |  |
| `status_type` | String | Status type. |
| `total_away_points` | String | Points scored by the away team in the game. |
| `current_period_id` | String | Ordinal number of the period currently in progress, counting from 1. |
| `total_home_points` | String | Points scored by the home team in the game. |
| `total_away_shootout_points` | String | Shootout goals converted by the away team, populated only for sports that break ties by shootout. |
| `total_home_shootout_points` | String | Shootout goals converted by the home team, populated only for sports that break ties by shootout. |
| `home_team_stats` | String | JSON-encoded team-stat block for the home team, populated once the game is under way. |
| `away_team_stats` | String | JSON-encoded team-stat block for the away team, populated once the game is under way. |
| `game_period_balls` | String | Balls in the count for the at-bat in progress; baseball only. |
| `game_period_strikes` | String | Strikes in the count for the at-bat in progress; baseball only. |
| `game_period_outs` | String | Outs recorded so far in the current half-inning; baseball only. |
| `yards_to_endzone` | String | Distance from the current ball spot to the opponent's goal line, in yards. |
| `start_yardline` | String | Yard line at the drive start. |
| `distance` | String | Yards to gain for a first down (or to the goal line in goal-to-go situations). |
| `down` | String | Down of the play (1-4). |
| `team_in_possession` | String | Yahoo team id of the side currently in possession of the ball. |
| `power_play_strength_home` | String | Number of skaters the home team has on the ice during special-teams play; hockey only. |
| `power_play_strength_away` | String | Number of skaters the away team has on the ice during special-teams play; hockey only. |
| `game_time_elapsed` | String | Playing time elapsed in the game, in seconds. |
| `game_time_elapsed_display` | String | Playing time elapsed formatted for display (e.g., "67:12"), used by sports whose clock counts up. |
| `inning_status` | String | Half-inning indicator for a game in progress (e.g., "Top", "Bottom"); baseball only. |
| `away_timeouts` | String | Away-team timeouts remaining. |
| `home_timeouts` | String | Home-team timeouts remaining. |
| `is_halftime` | String | Flag indicating that the game is currently stopped at halftime. |
| `minimum_periods` | Int64 | Number of periods a game of this sport runs before overtime is required (4 for football, 9 for baseball). |
| `game_periods` | Int64 | JSON-encoded list of the game's period nodes, each carrying a period number and its display names. |
| `baserunners` | String | JSON-encoded baserunner occupancy for the game in progress; baseball only. |
| `season` | Int64 | Season (4-digit year). |
| `subleague` | String | Sub-league the game belongs to, for leagues split into constituent circuits. |
| `subleague_display_name` | String | Display name of the sub-league the game belongs to. |
| `agg_score` | String | Aggregate score across the legs of a two-leg tie, populated only for competitions decided on aggregate. |
| `leg_number` | String | Ordinal of this leg within a multi-leg tie, counting from 1. |
| `tv_coverage` | String | Network carrying the game, as a short broadcast abbreviation (e.g., "CBS", "ESPN"). |
| `provider_coverage` | String |  |
| `seatgeek_id` | String | SeatGeek performer or event identifier used to build the ticket-purchase link. |
| `last_updated` | String | Timestamp ESPN last refreshed the power index. |
| `teams` | String |  |
| `play_by_play` | String | JSON-encoded data-island pointer to the game's play-by-play collection in the same editorial payload. |
| `pitches` | String | JSON-encoded data-island pointer to the game's pitch-level feed; baseball only. |
| `at_bat` | String | JSON-encoded data-island pointer to the game's current at-bat feed; baseball only. |
| `penalty_summary` | String |  |
| `scoring_summary` | String |  |
| `stat_categories` | String | JSON-encoded pointer to the stat-category dictionary that groups this feed's statistics. |
| `stadium` | String |  |
| `stadium_id` | String |  |
| `stadium_image` | String | JSON-encoded data-island pointer to the venue photograph used on the game page. |
| `attendance` | String | Reported attendance at the game. |
| `lineups` | String | JSON-encoded data-island pointer to the game's lineup collection. |
| `top_performer` | String | JSON-encoded data-island pointer to the game's top-performing players. |
| `players` | String |  |
| `byline` | String |  |
| `highlight` | String | JSON-encoded data-island pointer to the game's highlight video. |
| `highlights` | String | Game highlight urls. |
| `live_video` | String | JSON-encoded data-island pointer to the live video stream for the game. |
| `odds` | String | JSON-encoded data-island pointer to the game's odds collection. |
| `current_players` | String | JSON-encoded data-island pointer to the players currently on the field, ice or court. |
| `last_play` | String | Free-text description of the most recent play. |
| `series_type` | String | JSON-encoded data-island pointer to the kind of series the game belongs to. |
| `series_status` | String | JSON-encoded data-island pointer to the current state of the series the game belongs to. |
| `games` | String | Number of games included in the ATS summary. |
| `series_games` | String | JSON-encoded data-island pointer to the games making up the series. |
| `game_details` | String | JSON-encoded data-island pointer to supplementary detail notes for the game. |
| `section_notes` | String | JSON-encoded data-island pointer to editorial section notes attached to the game page. |
| `articles` | String | JSON-encoded data-island pointer to the editorial articles attached to the game. |
| `tweets` | String | JSON-encoded data-island pointer to the social posts attached to the game page. |
| `playoff_round` | String |  |
| `media_stream` | String | JSON-encoded data-island pointer to the game's media-stream collection. |
| `playoff_series_status` | String | JSON-encoded data-island pointer to the current state of the playoff series the game belongs to. |
| `playoff_series_details` | String | JSON-encoded data-island pointer to detail about the playoff series the game belongs to. |
| `drives` | String | JSON-encoded data-island pointer to the game's drive collection; football only. |
| `user_teams_game` | String | JSON-encoded data-island pointer to the viewer's followed-team context for the game. |
| `page_metadata` | String | JSON-encoded data-island pointer to the SEO and page metadata for the entity. |
| `penalty_box` | String | JSON-encoded data-island pointer to the game's penalty-box feed; hockey only. |
| `starting_pitchers` | String | JSON-encoded data-island pointer to the game's announced starting pitchers; baseball only. |
| `unrestricted_streams` | String | JSON-encoded data-island pointer to the streams viewable without a subscription. |
| `tv_details` | String | JSON-encoded list of broadcast entries for the game, each carrying a network abbreviation and full channel name (e.g., [{"abbr": "NBC", "name": "NBC/Peacock"}]). |
| `away_seed` | String |  |
| `home_seed` | String |  |
| `week` | Int64 | Game week of the season. |

**Raises**

| Exception | Description |
|---|---|
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
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_teams
teams = yahoo_cfb_teams(season=2024)
abbr = dict(zip(teams["team_id"], teams["abbreviation"]))
```
