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

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_teams
teams = yahoo_cfb_teams(season=2024)
abbr = dict(zip(teams["team_id"], teams["abbreviation"]))
```
