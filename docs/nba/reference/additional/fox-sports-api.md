# NBA — additional Python functions — Fox Sports API

> NBA — additional Python functions — Fox Sports API — function reference in sdv-py, the SportsDataverse Python package.

### fox_nba_boxscore {#fox_nba_boxscore}

`fox_nba_boxscore(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NBA boxscore (long: one row per player-stat).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Fox Bifrost event id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the per-team stat tables to long form; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `team` | character | Team-side label or team identifier. |
| `stat_group` | character |  |
| `player` | character | Player name. |
| `athlete_id` | character | Unique athlete identifier (ESPN). |
| `stat` | character | Stat. |
| `value` | character | Numeric or string value field. |

**Example**

```python
from sportsdataverse.nba import fox_nba_boxscore
df = fox_nba_boxscore("...")
```

### fox_nba_event_matchup {#fox_nba_event_matchup}

`fox_nba_event_matchup(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nba pregame team-stat comparison (one row per stat).

Wraps `nba/event/{game_id}/matchup`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nba import fox_nba_event_matchup
df = fox_nba_event_matchup("...")
```

### fox_nba_event_recap {#fox_nba_event_recap}

`fox_nba_event_recap(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nba postgame top performers (one row per player).

Wraps `nba/event/{game_id}/recap`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nba import fox_nba_event_recap
df = fox_nba_event_recap("...")
```

### fox_nba_event_standings {#fox_nba_event_standings}

`fox_nba_event_standings(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nba the two teams' standings context.

Wraps `nba/event/{game_id}/standings`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nba import fox_nba_event_standings
df = fox_nba_event_standings("...")
```

### fox_nba_league_conferences {#fox_nba_league_conferences}

`fox_nba_league_conferences(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nba conference / group directory.

Wraps `nba/league/conferences`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nba import fox_nba_league_conferences
df = fox_nba_league_conferences()
```

### fox_nba_league_header {#fox_nba_league_header}

`fox_nba_league_header(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nba league header (one row).

Wraps `nba/league/header`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `template` | character | Fox layout template name for the header block; always 'entity-header' in sampled data. |
| `title` | character | Title or label for the record. |
| `entity_id` | character | Fox id of the league entity as a string: the trailing number of the league's Fox contentUri. |
| `content_uri` | character | Fox Bifrost content URI of the league entity ('basketball/nba/league/1' in sampled data). |
| `content_type` | character | Fox entity type of the header's entity; always 'league' in sampled data. |
| `color` | character | Primary color (hex without leading '#'). |
| `logo_url` | character | NBA CDN primary logo URL. |
| `image_alt_text` | character | Alt text Fox attaches to the header image, which reads as the league's full name. |
| `rank` | character | Rank. |
| `details` | character | Details. |

**Example**

```python
from sportsdataverse.nba import fox_nba_league_header
df = fox_nba_league_header()
```

### fox_nba_league_leaders {#fox_nba_league_leaders}

`fox_nba_league_leaders(category: 'str' = 'scoring', who: 'str' = 'player', page: 'int' = 0, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NBA statistical leaders (`stats-con`); who=player|team.

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
| `v1` | character | Leader's name as Fox abbreviates it, first initial plus surname ('F. Lastname'); the column is named v1 because its table header cell is blank. |
| `gp` | character | Games played. |
| `entity_id` | character | Fox id of the row's linked player or team as a string: the trailing number of the row's entityLink contentUri. |
| `min` | character | Minutes played. |
| `mpg` | character | Minutes per game. |

**Example**

```python
from sportsdataverse.nba import fox_nba_league_leaders
df = fox_nba_league_leaders("scoring")
```

### fox_nba_league_odds {#fox_nba_league_odds}

`fox_nba_league_odds(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nba league odds board (one row per team per game).

Wraps `nba/league/odds`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nba import fox_nba_league_odds
df = fox_nba_league_odds()
```

### fox_nba_league_player_news {#fox_nba_league_player_news}

`fox_nba_league_player_news(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nba league-wide player news feed.

Wraps `nba/league/playernews`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nba import fox_nba_league_player_news
df = fox_nba_league_player_news()
```

### fox_nba_league_polls {#fox_nba_league_polls}

`fox_nba_league_polls(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nba rankings / polls rendered as standings tables.

Wraps `nba/league/polls`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nba import fox_nba_league_polls
df = fox_nba_league_polls()
```

### fox_nba_league_schedule {#fox_nba_league_schedule}

`fox_nba_league_schedule(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nba league schedule nav selections.

Wraps `nba/league/schedule`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | character | Which Fox navigation list the row came from ('groupList', 'dailyList' or 'selectionList'); always 'dailyList', one row per game date, in sampled data. |
| `id` | character | Id. |
| `title` | character | Title or label for the record. |
| `date` | character | Date in YYYY-MM-DD format. |
| `uri` | character | Absolute api.foxsports.com Bifrost URL of that date's segment payload (.../nba/league/schedule-segment/<year>-YYYYMMDD). |
| `web_url` | character | Site-relative foxsports.com path of the page for that date (e.g. '/nba/schedule?date=YYYY-MM-DD'). |
| `selected` | character | Fox's default-selection flag, which Fox sets only on group-filter (groupList) items; this league's navigation payload has no groupList, so the column is null on every row. The current date or week is marked by the payload-level currentSelectionId, which the parser does not return. |
| `group_id` | character | ESPN group id. |

**Example**

```python
from sportsdataverse.nba import fox_nba_league_schedule
df = fox_nba_league_schedule()
```

### fox_nba_league_scores {#fox_nba_league_scores}

`fox_nba_league_scores(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nba league scores nav selections.

Wraps `nba/league/scores`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | character | Which Fox navigation list the row came from ('groupList', 'dailyList' or 'selectionList'); always 'dailyList', one row per game date, in sampled data. |
| `id` | character | Id. |
| `title` | character | Title or label for the record. |
| `date` | character | Date in YYYY-MM-DD format. |
| `uri` | character | Absolute api.foxsports.com Bifrost URL of that date's segment payload (.../nba/league/scores-segment/YYYYMMDD). |
| `web_url` | character | Site-relative foxsports.com path of the page for that date (e.g. '/nba/scores?date=YYYY-MM-DD'). |
| `selected` | character | Fox's default-selection flag, which Fox sets only on group-filter (groupList) items; this league's navigation payload has no groupList, so the column is null on every row. The current date or week is marked by the payload-level currentSelectionId, which the parser does not return. |
| `group_id` | character | ESPN group id. |

**Example**

```python
from sportsdataverse.nba import fox_nba_league_scores
df = fox_nba_league_scores()
```

### fox_nba_league_standings {#fox_nba_league_standings}

`fox_nba_league_standings(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nba league-wide standings tables.

Wraps `nba/league/standings`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `section` | character | Title of the Fox standings section the row came from, such as 'CONFERENCE' or 'PRESEASON'; each team appears once per section. Values in sampled data: 'CONFERENCE', 'DIVISION', 'PRESEASON'. |
| `eastern_conference` | character | Team's position in the Eastern Conference table, from the first column whose header is the table name; null on rows from other tables, and '-' on every populated row of the sampled pre-season data. |
| `v1` | character | Team nickname as Fox displays it (e.g. 'Celtics', 'Knicks'); the column is named v1 because its table header cell is blank. |
| `w_l` | character | W l. |
| `pct` | character | Win percentage. |
| `gb` | character | Games behind the conference leader. |
| `pf` | character | Personal fouls. |
| `pa` | character | Points allowed per game as a one-decimal string; always '0.0' in the sampled pre-season data, where no games had been played. |
| `home` | character | Home. |
| `away` | character | Away record. |
| `conf` | character | character. |
| `div` | character | Record against division opponents as a 'W-L' string ('0-0' throughout the sampled pre-season data); null on rows from tables without a DIV column. |
| `l10` | character | Last-ten record. |
| `strk` | character | Current streak. |
| `entity_id` | character | Fox id of the row's linked team as a string: the trailing number of the row's entityLink contentUri. |
| `western_conference` | character | Team's position in the Western Conference table, from the first column whose header is the table name; null on rows from other tables, and '-' on every populated row of the sampled pre-season data. |
| `atlantic` | character | Team's position in the Atlantic division table, from the first column whose header is the table name; null on rows from other tables, and '-' on every populated row of the sampled pre-season data. |
| `central` | character | Team's position in the Central division table, from the first column whose header is the table name; null on rows from other tables, and '-' on every populated row of the sampled pre-season data. |
| `southeast` | character | Team's position in the Southeast division table, from the first column whose header is the table name; null on rows from other tables, and '-' on every populated row of the sampled pre-season data. |
| `northwest` | character | Team's position in the Northwest division table, from the first column whose header is the table name; null on rows from other tables, and '-' on every populated row of the sampled pre-season data. |
| `pacific` | character | Team's position in the Pacific division table, from the first column whose header is the table name; null on rows from other tables, and '-' on every populated row of the sampled pre-season data. |
| `southwest` | character | Team's position in the Southwest division table, from the first column whose header is the table name; null on rows from other tables, and '-' on every populated row of the sampled pre-season data. |

**Example**

```python
from sportsdataverse.nba import fox_nba_league_standings
df = fox_nba_league_standings()
```

### fox_nba_league_stat_leaders {#fox_nba_league_stat_leaders}

`fox_nba_league_stat_leaders(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nba league stats landing leaders.

Wraps `nba/league/stats`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `category` | character | Category label. |
| `stat` | character | Stat. |
| `stat_abbreviation` | character | Fox's short code for the leader stat, such as 'PPG', 'RPG', 'FG%', 'DBL DBL' or 'OFF RTG'; the spelled-out name is in stat. |
| `player` | character | Player name. |
| `value` | character | Numeric or string value field. |

**Example**

```python
from sportsdataverse.nba import fox_nba_league_stat_leaders
df = fox_nba_league_stat_leaders()
```

### fox_nba_odds {#fox_nba_odds}

`fox_nba_odds(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NBA game odds six-pack (spread / to-win / total per team).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Fox Bifrost event id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the six-pack market to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

No returns table is published for this function: no capture: for this sport Fox sends the odds under sectionList modules rather than the top-level sixPack key the shared parser reads (football and MLB still use sixPack), so it returns an empty frame for every game.

**Example**

```python
from sportsdataverse.nba import fox_nba_odds
df = fox_nba_odds("...")
```

### fox_nba_pbp {#fox_nba_pbp}

`fox_nba_pbp(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NBA play-by-play (one row per play; period-based).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Fox Bifrost event id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the pbp layout to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `period` | character | Period of the game (1-4 quarters; 5+ for OT). |
| `left_team` | character |  |
| `right_team` | character |  |
| `play_id` | character | Unique play identifier within a game. |
| `clock` | character | Game clock value. |
| `team` | character | Team-side label or team identifier. |
| `left_score_change` | logical |  |
| `right_score_change` | logical |  |
| `play_text` | character | Play description text. |

**Example**

```python
from sportsdataverse.nba import fox_nba_pbp
df = fox_nba_pbp("...")
```

### fox_nba_scoreboard {#fox_nba_scoreboard}

`fox_nba_scoreboard(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nba scoreboard nav selections (weeks / dates / groups).

Wraps `nba/scoreboard/main`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | character | Which Fox navigation list the row came from ('groupList', 'dailyList' or 'selectionList'); always 'dailyList', one row per game date, in sampled data. |
| `id` | character | Id. |
| `title` | character | Title or label for the record. |
| `date` | character | Date in YYYY-MM-DD format. |
| `uri` | character | Absolute api.foxsports.com Bifrost URL of that date's segment payload (.../nba/scoreboard/segment/YYYYMMDD). |
| `web_url` | character | Site-relative foxsports.com path of the page for that date (e.g. '/scores/nba?date=YYYY-MM-DD'). |
| `selected` | character | Fox's default-selection flag, which Fox sets only on group-filter (groupList) items; this league's navigation payload has no groupList, so the column is null on every row. The current date or week is marked by the payload-level currentSelectionId, which the parser does not return. |
| `group_id` | character | ESPN group id. |

**Example**

```python
from sportsdataverse.nba import fox_nba_scoreboard
df = fox_nba_scoreboard()
```

### fox_nba_scorechip {#fox_nba_scorechip}

`fox_nba_scorechip(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nba compact live score chip (raw dict -- live-only, uncaptured shape).

Wraps `nba/scorechip/{chip_id}`.

**Returns**

The raw JSON `dict`.

**Example**

```python
from sportsdataverse.nba import fox_nba_scorechip
df = fox_nba_scorechip("nfl12345")
```

### fox_nba_scores_segment {#fox_nba_scores_segment}

`fox_nba_scores_segment(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nba one row per game in a scoreboard segment.

Wraps `nba/league/scores-segment/{segment_id}`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nba import fox_nba_scores_segment
df = fox_nba_scores_segment("...")
```

### fox_nba_standings {#fox_nba_standings}

`fox_nba_standings(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NBA standings for a team's conference/division.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the standings tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `team_id` | character | Unique team identifier. |
| `section` | character |  |
| `eastern_conference` | character |  |
| `v1` | character |  |
| `w_l` | character | W l. |
| `pct` | character | Win percentage. |
| `pf` | character | Personal fouls. |
| `pa` | character |  |
| `strk` | character | Current streak. |
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `western_conference` | character |  |

**Example**

```python
from sportsdataverse.nba import fox_nba_standings
df = fox_nba_standings("...")
```

### fox_nba_team_gamelog {#fox_nba_team_gamelog}

`fox_nba_team_gamelog(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NBA team game log (long: one row per game-stat).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to long form; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `team_id` | character | Unique team identifier. |
| `season_type` | character | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `category` | character | Category label. |
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `opponent` | character | Opponent. |
| `stat` | character | Stat. |
| `value` | character | Numeric or string value field. |

**Example**

```python
from sportsdataverse.nba import fox_nba_team_gamelog
df = fox_nba_team_gamelog("...")
```

### fox_nba_team_header {#fox_nba_team_header}

`fox_nba_team_header(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nba team header (one row).

Wraps `nba/team/{team_id}/header`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nba import fox_nba_team_header
df = fox_nba_team_header("...")
```

### fox_nba_team_roster {#fox_nba_team_roster}

`fox_nba_team_roster(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NBA team roster (one row per player).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the roster tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `team_id` | character | Unique team identifier. |
| `position_group` | character |  |
| `player` | character | Player name. |
| `pos` | character | Position. |
| `age` | character | Player age (in years). |
| `ht` | character | Listed height. |
| `wt` | character | Listed weight (lbs). |
| `school` | character | Player school / pre-draft team. |
| `athlete_id` | character | Unique athlete identifier (ESPN). |

**Example**

```python
from sportsdataverse.nba import fox_nba_team_roster
df = fox_nba_team_roster("...")
```

### fox_nba_team_stats {#fox_nba_team_stats}

`fox_nba_team_stats(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NBA team stat leaders by category.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the leader sections to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `team_id` | character | Unique team identifier. |
| `category` | character | Category label. |
| `stat` | character | Stat. |
| `stat_abbreviation` | character |  |
| `player` | character | Player name. |
| `value` | character | Numeric or string value field. |

**Example**

```python
from sportsdataverse.nba import fox_nba_team_stats
df = fox_nba_team_stats("...")
```

### fox_nba_teamnav {#fox_nba_teamnav}

`fox_nba_teamnav(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nba team directory (one row per team).

Wraps `nba/league/teamnav`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `group` | character | Group identifier (e.g. conference 'group_id'). |
| `fox_id` | character | Fox Sports team id as a string, the trailing number of content_uri (e.g. '6'). |
| `abbreviation` | character | Short abbreviation. |
| `name` | character | Display name. |
| `content_uri` | character | Fox Bifrost content URI identifying the team, shaped 'basketball/nba/teams/<fox_id>'. |
| `content_type` | character | Fox entity type from the team's entity link; always 'team' in sampled data. |
| `web_url` | character | Site-relative foxsports.com path of the team page, shaped '/nba/<city-nickname>-team'. |
| `color` | character | Primary color (hex without leading '#'). |
| `logo_url` | character | NBA CDN primary logo URL. |

**Example**

```python
from sportsdataverse.nba import fox_nba_teamnav
df = fox_nba_teamnav()
```

### fox_nba_teams {#fox_nba_teams}

`fox_nba_teams(team_id: 'Union[int, str]' = '1', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NBA team directory (`fox_team_id` / `fox_team_name` / `fox_section`).

Derived from the standings endpoint (one league-wide payload of all 30
teams), this is the frame the hoopR NBA team crosswalk consumes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` | `'1'` | Seed Fox Bifrost team id whose standings page is read. Defaults to `"1"` -- any NBA team id returns the whole league. |
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
from sportsdataverse.nba import fox_nba_teams
df = fox_nba_teams()

# Pipeline next step (one line)

df.select("fox_team_id", "fox_team_name").head()
```
