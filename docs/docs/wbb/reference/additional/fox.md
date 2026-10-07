---
title: "WBB — additional Python functions — Fox"
sidebar_label: "Fox"
sidebar_position: 4
description: "WBB — additional Python functions — Fox — function reference in sdv-py, the SportsDataverse Python package."
---
# WBB — additional Python functions — Fox

### fox_wbb_boxscore {#fox_wbb_boxscore}

`fox_wbb_boxscore(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

WBB boxscore (long: one row per player-stat).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Fox Bifrost event id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the per-team stat tables to long form; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wbb import fox_wbb_boxscore
df = fox_wbb_boxscore("389046")
```

### fox_wbb_event_matchup {#fox_wbb_event_matchup}

`fox_wbb_event_matchup(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wcbk pregame team-stat comparison (one row per stat).

Wraps `wcbk/event/{game_id}/matchup`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wbb import fox_wbb_event_matchup
df = fox_wbb_event_matchup("...")
```

### fox_wbb_event_recap {#fox_wbb_event_recap}

`fox_wbb_event_recap(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wcbk postgame top performers (one row per player).

Wraps `wcbk/event/{game_id}/recap`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wbb import fox_wbb_event_recap
df = fox_wbb_event_recap("...")
```

### fox_wbb_event_standings {#fox_wbb_event_standings}

`fox_wbb_event_standings(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wcbk the two teams' standings context.

Wraps `wcbk/event/{game_id}/standings`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wbb import fox_wbb_event_standings
df = fox_wbb_event_standings("...")
```

### fox_wbb_league_conferences {#fox_wbb_league_conferences}

`fox_wbb_league_conferences(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wcbk conference / group directory.

Wraps `wcbk/league/conferences`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `group` | character | Group identifier (e.g. conference 'group_id'). |
| `fox_id` | character | Fox Sports conference id as a string, the trailing number of content_uri; ids are assigned per feed and usually differ between the men's and women's feeds (ACC 11 vs 12), though some coincide (America East is 9 in both), so never join the two feeds on it. |
| `abbreviation` | character | Short abbreviation. |
| `name` | character | Display name. |
| `content_uri` | character | Fox Bifrost content URI identifying the conference (e.g. 'basketball/wcbk/groups/11'); fox_id is its trailing number. |
| `content_type` | character | Fox entity type from the conference's entity link; always 'league' in sampled data, even though each row is a conference. |
| `web_url` | character | Site-relative foxsports.com path of the conference page (e.g. '/womens-college-basketball/acc'). |
| `color` | character | Primary color (hex without leading '#'). |
| `logo_url` | character | NBA CDN primary logo URL. |

**Example**

```python
from sportsdataverse.wbb import fox_wbb_league_conferences
df = fox_wbb_league_conferences()
```

### fox_wbb_league_header {#fox_wbb_league_header}

`fox_wbb_league_header(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wcbk league header (one row).

Wraps `wcbk/league/header`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `template` | character | Fox layout template name for the header block; always 'entity-header' in sampled data. |
| `title` | character | Title or label for the record. |
| `entity_id` | character | Fox id of the league entity as a string: the trailing number of the league's Fox contentUri. |
| `content_uri` | character | Fox Bifrost content URI of the league entity ('basketball/wcbk/league/1' in sampled data). |
| `content_type` | character | Fox entity type of the header's entity; 'league' in sampled data. |
| `color` | character | Primary color (hex without leading '#'). |
| `logo_url` | character | NBA CDN primary logo URL. |
| `image_alt_text` | character | Alt text Fox attaches to the header image, which reads as the league's display name ('Women's College Basketball' in sampled data). |
| `rank` | character | Rank. |
| `details` | character | Details. |

**Example**

```python
from sportsdataverse.wbb import fox_wbb_league_header
df = fox_wbb_league_header()
```

### fox_wbb_league_leaders {#fox_wbb_league_leaders}

`fox_wbb_league_leaders(category: 'str' = 'scoring', who: 'str' = 'player', page: 'int' = 0, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

WBB statistical leaders (`stats-con`); who=player|team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `category` | `str` | `'scoring'` | Stat category. Defaults to `"scoring"`. |
| `who` | `str` | `'player'` | `"player"` or `"team"`. Defaults to `"player"`. |
| `page` | `int` | `0` | 0-based result page. Defaults to `0`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the leader tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `players` | character | Nested list of per-player box scores. |
| `v1` | character | Leader's name as Fox abbreviates it, first initial plus surname (e.g. 'T. Sides'); the column is named v1 because its table header cell is blank. |
| `gp` | character | Games played. |
| `entity_id` | character | Fox id of the row's linked player or team as a string: the trailing number of the row's entityLink contentUri. |
| `gs` | character | Games started. |
| `mpg` | character | Minutes per game. |
| `ppg` | character | Points per game. |
| `pts` | character | Points scored. |

**Example**

```python
from sportsdataverse.wbb import fox_wbb_league_leaders
df = fox_wbb_league_leaders("scoring")
```

### fox_wbb_league_odds {#fox_wbb_league_odds}

`fox_wbb_league_odds(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wcbk league odds board (one row per team per game).

Wraps `wcbk/league/odds`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wbb import fox_wbb_league_odds
df = fox_wbb_league_odds()
```

### fox_wbb_league_player_news {#fox_wbb_league_player_news}

`fox_wbb_league_player_news(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wcbk league-wide player news feed.

Wraps `wcbk/league/playernews`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wbb import fox_wbb_league_player_news
df = fox_wbb_league_player_news()
```

### fox_wbb_league_polls {#fox_wbb_league_polls}

`fox_wbb_league_polls(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wcbk rankings / polls rendered as standings tables.

Wraps `wcbk/league/polls`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `section` | character | Ranking the row belongs to: 'ASSOCIATED PRESS', 'USA TODAY COACHES POLL' or 'RPI RANKINGS' (25 rows each in sampled data). |
| `ranking` | character | Team recruiting ranking. |
| `v1` | character | Unlabeled second table column whose meaning depends on section: in the two polls it is Fox's rank-change cell, an unsigned number string (e.g. '5') that is null when no change is shown; on 'RPI RANKINGS' rows it holds the team name. |
| `v2` | character | Team name on poll rows, with first-place votes in parentheses when the team received any (e.g. 'UCLA (31)'); null on 'RPI RANKINGS' rows, where the name is in v1. |
| `pts` | character | Points scored. |
| `entity_id` | character | Fox id of the row's linked team as a string: the trailing number of the row's entityLink contentUri. |
| `rpi` | character | RPI value as a decimal string with a leading dot (e.g. '.7007'); populated only on 'RPI RANKINGS' rows and null on both poll sections. |
| `sos` | character | Strength of schedule. |
| `home` | character | Home. |
| `away` | character | Away record. |
| `neutral` | character | Neutral. |

**Example**

```python
from sportsdataverse.wbb import fox_wbb_league_polls
df = fox_wbb_league_polls()
```

### fox_wbb_league_schedule {#fox_wbb_league_schedule}

`fox_wbb_league_schedule(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wcbk league schedule nav selections.

Wraps `wcbk/league/schedule`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | character | Which Fox navigation list the row came from: 'groupList' (group filters such as FEATURED, TOP 25 and each conference; uri is null) or 'dailyList' (one row per game date). |
| `id` | character | Unique play identification number |
| `title` | character | Title or label for the record. |
| `date` | character | Date in YYYY-MM-DD format. |
| `uri` | character | Absolute api.foxsports.com Bifrost URL of that date's segment payload (.../league/schedule-segment/<season start year>-YYYYMMDD?groupId=...); null on groupList rows. |
| `web_url` | character | Site-relative foxsports.com path of the page the selection opens (e.g. '/womens-college-basketball/schedule?groupId=top25'). |
| `selected` | logical | Fox's default-selection flag: True on the one group filter (a groupList row) Fox pre-selects, and null (never False) on every other row. |
| `group_id` | character | ESPN group id. |

**Example**

```python
from sportsdataverse.wbb import fox_wbb_league_schedule
df = fox_wbb_league_schedule()
```

### fox_wbb_league_scores {#fox_wbb_league_scores}

`fox_wbb_league_scores(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wcbk league scores nav selections.

Wraps `wcbk/league/scores`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | character | Which Fox navigation list the row came from: 'groupList' (group filters such as FEATURED, TOP 25 and each conference; uri is null) or 'dailyList' (one row per game date). |
| `id` | character | Unique play identification number |
| `title` | character | Title or label for the record. |
| `date` | character | Date in YYYY-MM-DD format. |
| `uri` | character | Absolute api.foxsports.com Bifrost URL of that date's segment payload (.../league/scores-segment/YYYYMMDD?groupId=...); null on groupList rows. |
| `web_url` | character | Site-relative foxsports.com path of the page the selection opens (e.g. '/womens-college-basketball/scores?groupId=top25'). |
| `selected` | logical | Fox's default-selection flag: True on the one group filter (a groupList row) Fox pre-selects, and null (never False) on every other row. |
| `group_id` | character | ESPN group id. |

**Example**

```python
from sportsdataverse.wbb import fox_wbb_league_scores
df = fox_wbb_league_scores()
```

### fox_wbb_league_standings {#fox_wbb_league_standings}

`fox_wbb_league_standings(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wcbk league-wide standings tables.

Wraps `wcbk/league/standings`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `section` | character | Title of the Fox standings section the row came from; always 'CONFERENCE' in sampled data. |
| `wcbk` | character | Team's position number from Fox's rank cell, as a string; it restarts down the single flattened table (1 for UConn on the first row, 41 on the last), so it is a position within a block of that table, not one national ordering. Named after the header cell text 'WCBK'. |
| `v1` | character | Team name as Fox displays it (e.g. 'UConn', 'South Carolina'); the column is named v1 because its table header cell is blank. |
| `conf` | character | character. |
| `w_l` | character | W l. |
| `top_25` | character | Record against Top 25 opponents as a 'W-L' string (e.g. '7-1'). |
| `home` | character | Home. |
| `away` | character | Away record. |
| `pf` | character | Personal fouls. |
| `pa` | character | Season total points allowed, as an integer string (e.g. '1966' for a 38-1 UConn). |
| `strk` | character | Current streak. |
| `entity_id` | character | Fox id of the row's linked team as a string: the trailing number of the row's entityLink contentUri. |

**Example**

```python
from sportsdataverse.wbb import fox_wbb_league_standings
df = fox_wbb_league_standings()
```

### fox_wbb_league_stat_leaders {#fox_wbb_league_stat_leaders}

`fox_wbb_league_stat_leaders(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wcbk league stats landing leaders.

Wraps `wcbk/league/stats`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `category` | character | Category label. |
| `stat` | character | Stat. |
| `stat_abbreviation` | character | Fox's short code for the leader stat, such as 'PPG', 'RPG', '3FGM/G', 'TS%' or 'HIGH'; the spelled-out name is in stat. |
| `player` | character | Player name. |
| `value` | character | Numeric or string value field. |

**Example**

```python
from sportsdataverse.wbb import fox_wbb_league_stat_leaders
df = fox_wbb_league_stat_leaders()
```

### fox_wbb_odds {#fox_wbb_odds}

`fox_wbb_odds(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

WBB game odds six-pack (spread / to-win / total per team).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Fox Bifrost event id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the six-pack market to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wbb import fox_wbb_odds
df = fox_wbb_odds("389046")
```

### fox_wbb_pbp {#fox_wbb_pbp}

`fox_wbb_pbp(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

WBB play-by-play (one row per play; period-based).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Fox Bifrost event id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the pbp layout to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wbb import fox_wbb_pbp
df = fox_wbb_pbp("389046")
```

### fox_wbb_scoreboard {#fox_wbb_scoreboard}

`fox_wbb_scoreboard(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wcbk scoreboard nav selections (weeks / dates / groups).

Wraps `wcbk/scoreboard/main`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | character | Which Fox navigation list the row came from: 'groupList' (group filters such as FEATURED, TOP 25 and each conference; uri is null) or 'dailyList' (one row per game date). |
| `id` | character | Unique play identification number |
| `title` | character | Title or label for the record. |
| `date` | character | Date in YYYY-MM-DD format. |
| `uri` | character | Absolute api.foxsports.com Bifrost URL of that date's segment payload (.../scoreboard/segment/YYYYMMDD?groupId=...); null on groupList rows. |
| `web_url` | character | Site-relative foxsports.com path of the page the selection opens (e.g. '/scores/womens-college-basketball?groupId=top25'). |
| `selected` | logical | Fox's default-selection flag: True on the one group filter (a groupList row) Fox pre-selects, and null (never False) on every other row. |
| `group_id` | character | ESPN group id. |

**Example**

```python
from sportsdataverse.wbb import fox_wbb_scoreboard
df = fox_wbb_scoreboard()
```

### fox_wbb_scorechip {#fox_wbb_scorechip}

`fox_wbb_scorechip(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wcbk compact live score chip (raw dict -- live-only, uncaptured shape).

Wraps `wcbk/scorechip/{chip_id}`.

**Returns**

The raw JSON `dict`.

**Example**

```python
from sportsdataverse.wbb import fox_wbb_scorechip
df = fox_wbb_scorechip("nfl12345")
```

### fox_wbb_scores_segment {#fox_wbb_scores_segment}

`fox_wbb_scores_segment(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wcbk one row per game in a scoreboard segment.

Wraps `wcbk/league/scores-segment/{segment_id}`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wbb import fox_wbb_scores_segment
df = fox_wbb_scores_segment("...")
```

### fox_wbb_standings {#fox_wbb_standings}

`fox_wbb_standings(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

WBB standings for a team's conference/division.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the standings tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wbb import fox_wbb_standings
df = fox_wbb_standings("11")
```

### fox_wbb_team_gamelog {#fox_wbb_team_gamelog}

`fox_wbb_team_gamelog(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

WBB team game log (long: one row per game-stat).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to long form; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wbb import fox_wbb_team_gamelog
df = fox_wbb_team_gamelog("11")
```

### fox_wbb_team_header {#fox_wbb_team_header}

`fox_wbb_team_header(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wcbk team header (one row).

Wraps `wcbk/team/{team_id}/header`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wbb import fox_wbb_team_header
df = fox_wbb_team_header("...")
```

### fox_wbb_team_roster {#fox_wbb_team_roster}

`fox_wbb_team_roster(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

WBB team roster (one row per player).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the position-group tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wbb import fox_wbb_team_roster
df = fox_wbb_team_roster("11")
```

### fox_wbb_team_stats {#fox_wbb_team_stats}

`fox_wbb_team_stats(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

WBB team stat leaders by category.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the leader sections to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wbb import fox_wbb_team_stats
df = fox_wbb_team_stats("11")
```

### fox_wbb_teamnav {#fox_wbb_teamnav}

`fox_wbb_teamnav(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wcbk team directory (one row per team).

Wraps `wcbk/league/teamnav`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `group` | character | Group identifier (e.g. conference 'group_id'). |
| `fox_id` | character | Fox Sports team id as a string, the trailing number of content_uri; ids are league-specific, so a school can have different ids in the men's and women's feeds. |
| `abbreviation` | character | Short abbreviation. |
| `name` | character | Display name. |
| `content_uri` | character | Fox Bifrost content URI identifying the team, shaped 'basketball/wcbk/teams/<fox_id>'. |
| `content_type` | character | Fox entity type from the team's entity link; always 'team' in sampled data. |
| `web_url` | character | Site-relative foxsports.com path of the team page (e.g. '/womens-college-basketball/michigan-wolverines-team'). |
| `color` | character | Primary color (hex without leading '#'). |
| `logo_url` | character | NBA CDN primary logo URL. |

**Example**

```python
from sportsdataverse.wbb import fox_wbb_teamnav
df = fox_wbb_teamnav()
```

### fox_wbb_teams {#fox_wbb_teams}

`fox_wbb_teams(team_id: 'Union[int, str]' = '11', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

WBB team directory (`fox_team_id` / `fox_team_name` / `fox_section`).

Derived from the seed team's standings endpoint, so a single call only
covers that team's conference — see `fox_wbb_teams_all` for the full
directory. This is the frame the wehoop WBB team crosswalk consumes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` | `'11'` | Seed Fox Bifrost team id whose conference standings are read. Defaults to `"11"` (UConn, Big East). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the standings to the team directory; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `fox_team_id` | character | Fox Bifrost team id (NA if unmatched). |
| `fox_team_name` | character | Fox team name (NA if unmatched). |
| `fox_section` | character | Fox conference/section label (NA if unmatched). |

**Example**

```python
from sportsdataverse.wbb import fox_wbb_teams
df = fox_wbb_teams("11")
```

### fox_wbb_teams_all {#fox_wbb_teams_all}

`fox_wbb_teams_all(max_id: 'int' = 500, max_calls: 'int' = 60, *, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Full WBB team directory by walking seed ids across conferences.

A single `fox_wbb_teams` call only returns the seed team's
conference, so this walks candidate team ids (skipping ids already seen in
an earlier conference) and unions the results, spending at most
`max_calls` standings fetches. Mirrors R `wehoop::fox_wbb_teams_all()`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `max_id` | `int` | `500` | Highest candidate team id to try. Defaults to `500`. |
| `max_calls` | `int` | `60` | Budget of standings fetches. Defaults to `60`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. |

**Returns**

A polars DataFrame (default) or pandas DataFrame, one row per team: `fox_team_id` / `fox_team_name` / `fox_section`.

| col_name | type | description |
|---|---|---|
| `fox_team_id` | character | Fox Bifrost team id (NA if unmatched). |
| `fox_team_name` | character | Fox team name (NA if unmatched). |
| `fox_section` | character | Fox conference/section label (NA if unmatched). |

**Example**

```python
from sportsdataverse.wbb import fox_wbb_teams_all
df = fox_wbb_teams_all()
```
