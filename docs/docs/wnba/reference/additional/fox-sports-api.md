---
title: "WNBA — additional Python functions — Fox Sports API"
sidebar_label: "Fox Sports API"
sidebar_position: 4
description: "WNBA — additional Python functions — Fox Sports API — function reference in sdv-py, the SportsDataverse Python package."
---
# WNBA — additional Python functions — Fox Sports API

### fox_wnba_boxscore {#fox_wnba_boxscore}

`fox_wnba_boxscore(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

WNBA boxscore (long: one row per player-stat).

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
| `game_id` | String | Unique game identifier. |
| `team` | String | Team-side label or team identifier. |
| `stat_group` | String |  |
| `player` | String | Player name. |
| `athlete_id` | String | Unique athlete identifier (ESPN). |
| `stat` | String | Stat. |
| `value` | String | Numeric or string value field. |

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Fox returned 404 for the requested id. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |

**Example**

```python
from sportsdataverse.wnba import fox_wnba_boxscore
df = fox_wnba_boxscore("2278")
```

### fox_wnba_event_matchup {#fox_wnba_event_matchup}

`fox_wnba_event_matchup(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wnba pregame team-stat comparison (one row per stat).

Wraps `wnba/event/{game_id}/matchup`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wnba import fox_wnba_event_matchup
df = fox_wnba_event_matchup("...")
```

### fox_wnba_event_recap {#fox_wnba_event_recap}

`fox_wnba_event_recap(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wnba postgame top performers (one row per player).

Wraps `wnba/event/{game_id}/recap`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wnba import fox_wnba_event_recap
df = fox_wnba_event_recap("...")
```

### fox_wnba_event_standings {#fox_wnba_event_standings}

`fox_wnba_event_standings(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wnba the two teams' standings context.

Wraps `wnba/event/{game_id}/standings`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wnba import fox_wnba_event_standings
df = fox_wnba_event_standings("...")
```

### fox_wnba_league_conferences {#fox_wnba_league_conferences}

`fox_wnba_league_conferences(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wnba conference / group directory.

Wraps `wnba/league/conferences`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wnba import fox_wnba_league_conferences
df = fox_wnba_league_conferences()
```

### fox_wnba_league_header {#fox_wnba_league_header}

`fox_wnba_league_header(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wnba league header (one row).

Wraps `wnba/league/header`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `template` | String | Fox layout template name for the header block; always 'entity-header' in sampled data. |
| `title` | String | Title or label for the record. |
| `entity_id` | String | Fox id of the league entity as a string: the trailing number of the league's Fox contentUri. |
| `content_uri` | String | Fox Bifrost content URI of the league entity ('basketball/wnba/league/1' in sampled data). |
| `content_type` | String | Fox entity type of the header's entity; always 'league' in sampled data. |
| `color` | String | Primary color (hex without leading '#'). |
| `logo_url` | String | NBA CDN primary logo URL. |
| `image_alt_text` | String | Alt text Fox attaches to the header image, which reads as the league's full name. |
| `rank` | String | Rank. |
| `details` | String | Details. |

**Example**

```python
from sportsdataverse.wnba import fox_wnba_league_header
df = fox_wnba_league_header()
```

### fox_wnba_league_leaders {#fox_wnba_league_leaders}

`fox_wnba_league_leaders(category: 'str' = 'scoring', who: 'str' = 'player', page: 'int' = 0, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

WNBA statistical leaders (`stats-con`); who=player|team.

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
| `players` | String | Nested list of per-player box scores. |
| `v1` | String | Leader's name as Fox abbreviates it, first initial plus surname ('F. Lastname'); the column is named v1 because its table header cell is blank. |
| `gp` | String | Games played. |
| `entity_id` | String | Fox id of the row's linked player or team as a string: the trailing number of the row's entityLink contentUri. |
| `min` | String | Minutes played. |
| `mpg` | String | Minutes per game. |

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Fox returned 404 for the requested id. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |

**Example**

```python
from sportsdataverse.wnba import fox_wnba_league_leaders
df = fox_wnba_league_leaders("scoring")
```

### fox_wnba_league_odds {#fox_wnba_league_odds}

`fox_wnba_league_odds(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wnba league odds board (one row per team per game).

Wraps `wnba/league/odds`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `section` | String | Title of the Fox odds-board section the game is listed under; always 'GAMES' in sampled data. |
| `game_id` | String | Unique game identifier. |
| `event_time` | String | Scheduled start of the game as an ISO-8601 UTC timestamp string (e.g. '2026-09-17T23:30:00Z'). |
| `event_status` | Int64 | Fox numeric event-status code for the game (Int64); always 2 in the sampled board, where every game was still to be played at capture. |
| `team` | String | Team-side label or team identifier. |
| `spread` | String | Spread. |
| `to_win` | String | Moneyline for the team on this row as an American-odds string with explicit sign (e.g. '+884', '-126'). |
| `total` | String | Total. |

**Example**

```python
from sportsdataverse.wnba import fox_wnba_league_odds
df = fox_wnba_league_odds()
```

### fox_wnba_league_player_news {#fox_wnba_league_player_news}

`fox_wnba_league_player_news(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wnba league-wide player news feed.

Wraps `wnba/league/playernews`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `title` | String | Title or label for the record. |
| `subtitle` | String | Player tag line shaped '<TEAM> #<jersey> - <position>' (e.g. 'PHX #2 - G/F'). |
| `headline` | String | News headline. |
| `description` | String | Long-form description text. |
| `impact_title` | String | Heading label Fox shows above the impact note; always 'Impact' in sampled data. |
| `impact` | String | Fox's free-text impact note for the item, describing the player's availability or outlook (e.g. games missed with an injury and the next chance to play). |
| `date` | String | Date in YYYY-MM-DD format. |
| `source` | String |  |
| `athlete_id` | String | Unique athlete identifier (ESPN). |
| `content_uri` | String | Fox Bifrost content URI of the athlete the item is about, shaped 'basketball/wnba/athletes/<athlete_id>'. |
| `web_url` | String | Site-relative foxsports.com path of the player's page (e.g. '/wnba/kahleah-copper-player'). |

**Example**

```python
from sportsdataverse.wnba import fox_wnba_league_player_news
df = fox_wnba_league_player_news()
```

### fox_wnba_league_polls {#fox_wnba_league_polls}

`fox_wnba_league_polls(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wnba rankings / polls rendered as standings tables.

Wraps `wnba/league/polls`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wnba import fox_wnba_league_polls
df = fox_wnba_league_polls()
```

### fox_wnba_league_schedule {#fox_wnba_league_schedule}

`fox_wnba_league_schedule(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wnba league schedule nav selections.

Wraps `wnba/league/schedule`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | String | Which Fox navigation list the row came from ('groupList', 'dailyList' or 'selectionList'); always 'dailyList', one row per game date, in sampled data. |
| `id` | String | Unique play identification number |
| `title` | String | Title or label for the record. |
| `date` | String | Date in YYYY-MM-DD format. |
| `uri` | String | Absolute api.foxsports.com Bifrost URL of that date's segment payload (.../wnba/league/schedule-segment/<year>-YYYYMMDD). |
| `web_url` | String | Site-relative foxsports.com path of the page for that date (e.g. '/wnba/schedule?date=YYYY-MM-DD'). |
| `selected` | String | Fox's default-selection flag, which Fox sets only on group-filter (groupList) items; this league's navigation payload has no groupList, so the column is null on every row. The current date or week is marked by the payload-level currentSelectionId, which the parser does not return. |
| `group_id` | String | ESPN group id. |

**Example**

```python
from sportsdataverse.wnba import fox_wnba_league_schedule
df = fox_wnba_league_schedule()
```

### fox_wnba_league_scores {#fox_wnba_league_scores}

`fox_wnba_league_scores(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wnba league scores nav selections.

Wraps `wnba/league/scores`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | String | Which Fox navigation list the row came from ('groupList', 'dailyList' or 'selectionList'); always 'dailyList', one row per game date, in sampled data. |
| `id` | String | Unique play identification number |
| `title` | String | Title or label for the record. |
| `date` | String | Date in YYYY-MM-DD format. |
| `uri` | String | Absolute api.foxsports.com Bifrost URL of that date's segment payload (.../wnba/league/scores-segment/YYYYMMDD). |
| `web_url` | String | Site-relative foxsports.com path of the page for that date (e.g. '/wnba/scores?date=YYYY-MM-DD'). |
| `selected` | String | Fox's default-selection flag, which Fox sets only on group-filter (groupList) items; this league's navigation payload has no groupList, so the column is null on every row. The current date or week is marked by the payload-level currentSelectionId, which the parser does not return. |
| `group_id` | String | ESPN group id. |

**Example**

```python
from sportsdataverse.wnba import fox_wnba_league_scores
df = fox_wnba_league_scores()
```

### fox_wnba_league_standings {#fox_wnba_league_standings}

`fox_wnba_league_standings(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wnba league-wide standings tables.

Wraps `wnba/league/standings`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `section` | String | Title of the Fox standings section the row came from, such as 'CONFERENCE' or 'PRESEASON'; each team appears once per section. Values in sampled data: 'CONFERENCE', 'PRESEASON'. |
| `eastern` | String | Team's position in the Eastern conference table, from the first column whose header is the table name; null on rows from other tables, as a number string ('1' to '7' in sampled data). |
| `v1` | String | Team nickname as Fox displays it (e.g. 'Liberty', 'Fever'); the column is named v1 because its table header cell is blank. |
| `w_l` | String | W l. |
| `pct` | String | Win percentage. |
| `gb` | String | Games behind the conference leader. |
| `pf` | String | Personal fouls. |
| `pa` | String | Points allowed per game as a one-decimal string (e.g. '86.0'). |
| `home` | String | Home. |
| `away` | String | Away record. |
| `conf` | String | character. |
| `l10` | String | L10. |
| `strk` | String | Current streak. |
| `entity_id` | String | Fox id of the row's linked team as a string: the trailing number of the row's entityLink contentUri. |
| `western` | String | Team's position in the Western conference table, from the first column whose header is the table name; null on rows from other tables, as a number string ('1' to '8' in sampled data). |

**Example**

```python
from sportsdataverse.wnba import fox_wnba_league_standings
df = fox_wnba_league_standings()
```

### fox_wnba_league_stat_leaders {#fox_wnba_league_stat_leaders}

`fox_wnba_league_stat_leaders(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wnba league stats landing leaders.

Wraps `wnba/league/stats`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `category` | String | Category label. |
| `stat` | String | Stat. |
| `stat_abbreviation` | String | Fox's short code for the leader stat, such as 'PPG', 'RPG', 'FG%', 'DBL DBL' or 'OFF RTG'; the spelled-out name is in stat. |
| `player` | String | Player name. |
| `value` | String | Numeric or string value field. |

**Example**

```python
from sportsdataverse.wnba import fox_wnba_league_stat_leaders
df = fox_wnba_league_stat_leaders()
```

### fox_wnba_odds {#fox_wnba_odds}

`fox_wnba_odds(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

WNBA game odds six-pack (spread / to-win / total per team).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Fox Bifrost event id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the six-pack market to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

No returns table is published for this function: no capture: for this sport Fox sends the odds under sectionList modules rather than the top-level sixPack key the shared parser reads (football and MLB still use sixPack), so it returns an empty frame for every game.

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Fox returned 404 for the requested id. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |

**Example**

```python
from sportsdataverse.wnba import fox_wnba_odds
df = fox_wnba_odds("2278")
```

### fox_wnba_pbp {#fox_wnba_pbp}

`fox_wnba_pbp(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

WNBA play-by-play (one row per play; period-based).

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
| `game_id` | String | Unique game identifier. |
| `period` | String | Period of the game (1-4 quarters; 5+ for OT). |
| `left_team` | String |  |
| `right_team` | String |  |
| `play_id` | String | Unique play identifier within a game. |
| `clock` | String | Game clock value. |
| `team` | String | Team-side label or team identifier. |
| `left_score_change` | Boolean |  |
| `right_score_change` | Boolean |  |
| `play_text` | String | Play description text. |

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Fox returned 404 for the requested id. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |

**Example**

```python
from sportsdataverse.wnba import fox_wnba_pbp
df = fox_wnba_pbp("2278")
```

### fox_wnba_scoreboard {#fox_wnba_scoreboard}

`fox_wnba_scoreboard(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wnba scoreboard nav selections (weeks / dates / groups).

Wraps `wnba/scoreboard/main`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | String | Which Fox navigation list the row came from ('groupList', 'dailyList' or 'selectionList'); always 'dailyList', one row per game date, in sampled data. |
| `id` | String | Unique play identification number |
| `title` | String | Title or label for the record. |
| `date` | String | Date in YYYY-MM-DD format. |
| `uri` | String | Absolute api.foxsports.com Bifrost URL of that date's segment payload (.../wnba/scoreboard/segment/YYYYMMDD). |
| `web_url` | String | Site-relative foxsports.com path of the page for that date (e.g. '/scores/wnba?date=YYYY-MM-DD'). |
| `selected` | String | Fox's default-selection flag, which Fox sets only on group-filter (groupList) items; this league's navigation payload has no groupList, so the column is null on every row. The current date or week is marked by the payload-level currentSelectionId, which the parser does not return. |
| `group_id` | String | ESPN group id. |

**Example**

```python
from sportsdataverse.wnba import fox_wnba_scoreboard
df = fox_wnba_scoreboard()
```

### fox_wnba_scorechip {#fox_wnba_scorechip}

`fox_wnba_scorechip(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wnba compact live score chip (raw dict -- live-only, uncaptured shape).

Wraps `wnba/scorechip/{chip_id}`.

**Returns**

The raw JSON `dict`.

**Example**

```python
from sportsdataverse.wnba import fox_wnba_scorechip
df = fox_wnba_scorechip("nfl12345")
```

### fox_wnba_scores_segment {#fox_wnba_scores_segment}

`fox_wnba_scores_segment(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wnba one row per game in a scoreboard segment.

Wraps `wnba/league/scores-segment/{segment_id}`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wnba import fox_wnba_scores_segment
df = fox_wnba_scores_segment("...")
```

### fox_wnba_standings {#fox_wnba_standings}

`fox_wnba_standings(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

WNBA standings for a team's conference/division.

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
| `team_id` | String | Unique team identifier. |
| `section` | String |  |
| `eastern` | String |  |
| `v1` | String |  |
| `w_l` | String | W l. |
| `pct` | String | Win percentage. |
| `gb` | String | Games behind the conference leader. |
| `pf` | String | Personal fouls. |
| `pa` | String |  |
| `home` | String | Home. |
| `away` | String | Away record. |
| `conf` | String | character. |
| `l10` | String | L10. |
| `strk` | String | Current streak. |
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `western` | String |  |

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Fox returned 404 for the requested id. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |

**Example**

```python
from sportsdataverse.wnba import fox_wnba_standings
df = fox_wnba_standings("3")
```

### fox_wnba_team_gamelog {#fox_wnba_team_gamelog}

`fox_wnba_team_gamelog(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

WNBA team game log (long: one row per game-stat).

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
| `team_id` | String | Unique team identifier. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `category` | String | Category label. |
| `game_id` | String | Unique game identifier. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `opponent` | String | Opponent. |
| `stat` | String | Stat. |
| `value` | String | Numeric or string value field. |

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Fox returned 404 for the requested id. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |

**Example**

```python
from sportsdataverse.wnba import fox_wnba_team_gamelog
df = fox_wnba_team_gamelog("3")
```

### fox_wnba_team_header {#fox_wnba_team_header}

`fox_wnba_team_header(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wnba team header (one row).

Wraps `wnba/team/{team_id}/header`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.wnba import fox_wnba_team_header
df = fox_wnba_team_header("...")
```

### fox_wnba_team_roster {#fox_wnba_team_roster}

`fox_wnba_team_roster(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

WNBA team roster (one row per player).

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
| `team_id` | String | Unique team identifier. |
| `position_group` | String |  |
| `player` | String | Player name. |
| `pos` | String | Position. |
| `age` | String | Player age (in years). |
| `ht` | String | Listed height. |
| `wt` | String | Listed weight (lbs). |
| `college` | String | College or school attended. |
| `athlete_id` | String | Unique athlete identifier (ESPN). |

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Fox returned 404 for the requested id. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |

**Example**

```python
from sportsdataverse.wnba import fox_wnba_team_roster
df = fox_wnba_team_roster("3")
```

### fox_wnba_team_stats {#fox_wnba_team_stats}

`fox_wnba_team_stats(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

WNBA team stat leaders by category.

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
| `team_id` | String | Unique team identifier. |
| `category` | String | Category label. |
| `stat` | String | Stat. |
| `stat_abbreviation` | String |  |
| `player` | String | Player name. |
| `value` | String | Numeric or string value field. |

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Fox returned 404 for the requested id. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |

**Example**

```python
from sportsdataverse.wnba import fox_wnba_team_stats
df = fox_wnba_team_stats("3")
```

### fox_wnba_teamnav {#fox_wnba_teamnav}

`fox_wnba_teamnav(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports wnba team directory (one row per team).

Wraps `wnba/league/teamnav`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `group` | String | Group identifier (e.g. conference 'group_id'). |
| `fox_id` | String | Fox Sports team id as a string, the trailing number of content_uri (e.g. '6'). |
| `abbreviation` | String | Short abbreviation. |
| `name` | String | Display name. |
| `content_uri` | String | Fox Bifrost content URI identifying the team, shaped 'basketball/wnba/teams/<fox_id>'. |
| `content_type` | String | Fox entity type from the team's entity link; always 'team' in sampled data. |
| `web_url` | String | Site-relative foxsports.com path of the team page, shaped '/wnba/<city-nickname>-team'. |
| `color` | String | Primary color (hex without leading '#'). |
| `logo_url` | String | NBA CDN primary logo URL. |

**Example**

```python
from sportsdataverse.wnba import fox_wnba_teamnav
df = fox_wnba_teamnav()
```

### fox_wnba_teams {#fox_wnba_teams}

`fox_wnba_teams(team_id: 'Union[int, str]' = '3', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

WNBA team directory (`fox_team_id` / `fox_team_name` / `fox_section`).

Derived from the standings endpoint (one league-wide payload), this is the
frame the wehoop WNBA team crosswalk consumes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` | `'3'` | Seed Fox Bifrost team id whose standings page is read. Defaults to `"3"` — any WNBA team id returns the whole league. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the standings to the team directory; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `fox_team_id` | String | Fox Bifrost team id (NA if unmatched). |
| `fox_team_name` | String | Fox team name (NA if unmatched). |
| `fox_section` | String | Fox conference/section label (NA if unmatched). |

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Fox returned 404 for the requested id. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |

**Example**

```python
from sportsdataverse.wnba import fox_wnba_teams
df = fox_wnba_teams()
```
