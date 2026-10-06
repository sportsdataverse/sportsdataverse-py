---
title: "NHL — additional Python functions — Fox Sports API"
sidebar_label: "Fox Sports API"
sidebar_position: 2
description: "NHL — additional Python functions — Fox Sports API — function reference in sdv-py, the SportsDataverse Python package."
---
# NHL — additional Python functions — Fox Sports API

### fox_nhl_boxscore {#fox_nhl_boxscore}

`fox_nhl_boxscore(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NHL boxscore (long: one row per player-stat).

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
from sportsdataverse.nhl import fox_nhl_boxscore
df = fox_nhl_boxscore("...")
```

### fox_nhl_event_matchup {#fox_nhl_event_matchup}

`fox_nhl_event_matchup(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nhl pregame team-stat comparison (one row per stat).

Wraps `nhl/event/{game_id}/matchup`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nhl import fox_nhl_event_matchup
df = fox_nhl_event_matchup("...")
```

### fox_nhl_event_recap {#fox_nhl_event_recap}

`fox_nhl_event_recap(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nhl postgame top performers (one row per player).

Wraps `nhl/event/{game_id}/recap`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nhl import fox_nhl_event_recap
df = fox_nhl_event_recap("...")
```

### fox_nhl_event_standings {#fox_nhl_event_standings}

`fox_nhl_event_standings(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nhl the two teams' standings context.

Wraps `nhl/event/{game_id}/standings`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nhl import fox_nhl_event_standings
df = fox_nhl_event_standings("...")
```

### fox_nhl_league_conferences {#fox_nhl_league_conferences}

`fox_nhl_league_conferences(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nhl conference / group directory.

Wraps `nhl/league/conferences`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nhl import fox_nhl_league_conferences
df = fox_nhl_league_conferences()
```

### fox_nhl_league_header {#fox_nhl_league_header}

`fox_nhl_league_header(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nhl league header (one row).

Wraps `nhl/league/header`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `template` | character | Fox layout template name for the header payload; always 'entity-header' in sampled data. |
| `title` | character | Transaction title/headline. |
| `entity_id` | character | Fox id of the league entity as a string: the trailing number of the league's Fox contentUri. |
| `content_uri` | character | Fox content URI of the league entity (e.g., 'hockey/nhl/league/1'); entity_id is its trailing number. |
| `content_type` | character | Fox entity type of the header payload; always 'league' for the league header. |
| `color` | character | Primary color hex. |
| `logo_url` | character | NBA CDN primary logo URL. |
| `image_alt_text` | character | Image alt text Fox ships with the header, the full league name (e.g., 'National Hockey League'). |
| `rank` | character | Rank of the streak. |
| `details` | character | Odds detail string (e.g. "DET -185"). |

**Example**

```python
from sportsdataverse.nhl import fox_nhl_league_header
df = fox_nhl_league_header()
```

### fox_nhl_league_leaders {#fox_nhl_league_leaders}

`fox_nhl_league_leaders(category: 'str' = 'scoring', who: 'str' = 'player', page: 'int' = 0, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NHL statistical leaders (`stats-con`); who=player|team.

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
| `v1` | character | Abbreviated player name (e.g., 'A. Ovechkin'), from the leader table's unlabeled second column. |
| `gp` | character | Games played. |
| `entity_id` | character | Fox id of the row's linked player or team as a string: the trailing number of the row's entityLink contentUri. |
| `g` | character | Goals (skaters). |
| `a` | character | Assists (skaters). |
| `p` | character | P. |

**Example**

```python
from sportsdataverse.nhl import fox_nhl_league_leaders
df = fox_nhl_league_leaders("scoring")
```

### fox_nhl_league_odds {#fox_nhl_league_odds}

`fox_nhl_league_odds(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nhl league odds board (one row per team per game).

Wraps `nhl/league/odds`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nhl import fox_nhl_league_odds
df = fox_nhl_league_odds()
```

### fox_nhl_league_player_news {#fox_nhl_league_player_news}

`fox_nhl_league_player_news(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nhl league-wide player news feed.

Wraps `nhl/league/playernews`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `title` | character | Transaction title/headline. |
| `subtitle` | character | Player's team abbreviation, jersey number and position formatted 'TEAM #NN - POS' (e.g., 'WPG #37 - G'). |
| `headline` | character | Article headline. |
| `description` | character | Full text description of the event. |
| `impact_title` | character | Heading label for the impact paragraph; always 'Impact' in sampled data. |
| `impact` | character | Free-text analysis paragraph Fox shows under the item's impact_title heading. |
| `date` | character | Game date (ISO 8601 datetime string). |
| `source` | character | News source. |
| `athlete_id` | character | ESPN athlete identifier (echoed from arg). |
| `content_uri` | character | Fox content URI of the player the item is about (e.g., 'hockey/nhl/athletes/4442'); athlete_id is its trailing number. |
| `web_url` | character | Site-relative foxsports.com path of the player's page (e.g., '/nhl/connor-hellebuyck-player'). |

**Example**

```python
from sportsdataverse.nhl import fox_nhl_league_player_news
df = fox_nhl_league_player_news()
```

### fox_nhl_league_polls {#fox_nhl_league_polls}

`fox_nhl_league_polls(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nhl rankings / polls rendered as standings tables.

Wraps `nhl/league/polls`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nhl import fox_nhl_league_polls
df = fox_nhl_league_polls()
```

### fox_nhl_league_schedule {#fox_nhl_league_schedule}

`fox_nhl_league_schedule(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nhl league schedule nav selections.

Wraps `nhl/league/schedule`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | character | Which Fox navigation list the row came from ('groupList', 'dailyList' or 'selectionList'); always 'dailyList' (one row per date) in sampled data. |
| `id` | character | Unique player identifier. |
| `title` | character | Transaction title/headline. |
| `date` | character | Game date (ISO 8601 datetime string). |
| `uri` | character | Absolute Bifrost API URL of that date's schedule segment payload (e.g., 'https://api.foxsports.com/bifrost/v1/nhl/league/schedule-segment/2026-20260919'). |
| `web_url` | character | Site-relative foxsports.com schedule page path for that date (e.g., '/nhl/schedule?date=2026-09-19'). |
| `selected` | character | Fox's default-selection flag, which Fox sets only on group-filter (groupList) items; this league's navigation payload has no groupList, so the column is null on every row. The current date or week is marked by the payload-level currentSelectionId, which the parser does not return. |
| `group_id` | character | Group id (echoed from arg). |

**Example**

```python
from sportsdataverse.nhl import fox_nhl_league_schedule
df = fox_nhl_league_schedule()
```

### fox_nhl_league_scores {#fox_nhl_league_scores}

`fox_nhl_league_scores(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nhl league scores nav selections.

Wraps `nhl/league/scores`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | character | Which Fox navigation list the row came from ('groupList', 'dailyList' or 'selectionList'); always 'dailyList' (one row per date) in sampled data. |
| `id` | character | Unique player identifier. |
| `title` | character | Transaction title/headline. |
| `date` | character | Game date (ISO 8601 datetime string). |
| `uri` | character | Absolute Bifrost API URL of that date's scores segment payload (e.g., 'https://api.foxsports.com/bifrost/v1/nhl/league/scores-segment/20260919'). |
| `web_url` | character | Site-relative foxsports.com scores page path for that date (e.g., '/nhl/scores?date=2026-09-19'). |
| `selected` | character | Fox's default-selection flag, which Fox sets only on group-filter (groupList) items; this league's navigation payload has no groupList, so the column is null on every row. The current date or week is marked by the payload-level currentSelectionId, which the parser does not return. |
| `group_id` | character | Group id (echoed from arg). |

**Example**

```python
from sportsdataverse.nhl import fox_nhl_league_scores
df = fox_nhl_league_scores()
```

### fox_nhl_league_standings {#fox_nhl_league_standings}

`fox_nhl_league_standings(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nhl league-wide standings tables.

Wraps `nhl/league/standings`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `section` | character | Title of the standings section the row's table sits in ('CONFERENCE', 'DIVISION', 'WILD CARD' or 'PRESEASON'); each team appears once per section. |
| `eastern_conference` | character | First-column cell of Fox's EASTERN CONFERENCE tables (CONFERENCE and PRESEASON sections), whose header text names this column; all null in the sampled preseason data, where Fox left that cell blank. |
| `v1` | character | Team nickname (e.g., 'Bruins'), from the standings table's unlabeled second column. |
| `w_l_otl` | character | Record as a 'W-L-OTL' string (wins-losses-overtime losses); '0-0-0' for every team in the sampled preseason data. |
| `pts` | character | Points scored. |
| `gp` | character | Games played. |
| `row` | character | Row index within the game grouping (sequencing helper). |
| `sow` | character | Shootout wins (SOW column), as a string; '0' for every team in the sampled preseason data. |
| `sol` | character | Shootout losses (SOL column), as a string; '0' for every team in the sampled preseason data. |
| `gf` | character | Goals for (GF column), as a string; '0' for every team in the sampled preseason data. |
| `ga` | character | Goals against (goalies). |
| `gd` | character | Goal differential (GD column), as a string; '0' for every team in the sampled preseason data. |
| `home` | character | Whether the player's team was home. |
| `away` | character | Away team shots in the period. |
| `l10` | character | Last-ten record. |
| `strk` | character | Current streak. |
| `entity_id` | character | Fox id of the row's linked team as a string: the trailing number of the row's entityLink contentUri. |
| `western_conference` | character | First-column cell of Fox's WESTERN CONFERENCE tables (CONFERENCE and PRESEASON sections), whose header text names this column; all null in the sampled preseason data, where Fox left that cell blank. |
| `east_atlantic` | character | First-column cell of Fox's 'EAST, ATLANTIC' division tables (DIVISION and WILD CARD sections), whose header text names this column; all null in the sampled preseason data, where Fox left that cell blank. |
| `east_metropolitan` | character | First-column cell of Fox's 'EAST, METROPOLITAN' division tables (DIVISION and WILD CARD sections), whose header text names this column; all null in the sampled preseason data, where Fox left that cell blank. |
| `west_central` | character | First-column cell of Fox's 'WEST, CENTRAL' division tables (DIVISION and WILD CARD sections), whose header text names this column; all null in the sampled preseason data, where Fox left that cell blank. |
| `west_pacific` | character | First-column cell of Fox's 'WEST, PACIFIC' division tables (DIVISION and WILD CARD sections), whose header text names this column; all null in the sampled preseason data, where Fox left that cell blank. |
| `wild_card` | character | First-column cell of Fox's WILD CARD tables (one per conference in the WILD CARD section), whose header text names this column; all null in the sampled preseason data, where Fox left that cell blank. |

**Example**

```python
from sportsdataverse.nhl import fox_nhl_league_standings
df = fox_nhl_league_standings()
```

### fox_nhl_league_stat_leaders {#fox_nhl_league_stat_leaders}

`fox_nhl_league_stat_leaders(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nhl league stats landing leaders.

Wraps `nhl/league/stats`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `category` | character | Stat leader category. |
| `stat` | character | Stat. |
| `stat_abbreviation` | character | Fox's short label for the leader's stat (e.g., 'G', 'GAA', 'TOI/G'). |
| `player` | character | Penalized player name. |
| `value` | character | Leader stat numeric value. |

**Example**

```python
from sportsdataverse.nhl import fox_nhl_league_stat_leaders
df = fox_nhl_league_stat_leaders()
```

### fox_nhl_odds {#fox_nhl_odds}

`fox_nhl_odds(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NHL game odds six-pack (spread / to-win / total per team).

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
from sportsdataverse.nhl import fox_nhl_odds
df = fox_nhl_odds("...")
```

### fox_nhl_pbp {#fox_nhl_pbp}

`fox_nhl_pbp(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NHL play-by-play (one row per play; period-based).

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
from sportsdataverse.nhl import fox_nhl_pbp
df = fox_nhl_pbp("...")
```

### fox_nhl_scoreboard {#fox_nhl_scoreboard}

`fox_nhl_scoreboard(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nhl scoreboard nav selections (weeks / dates / groups).

Wraps `nhl/scoreboard/main`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | character | Which Fox navigation list the row came from ('groupList', 'dailyList' or 'selectionList'); always 'dailyList' (one row per date) in sampled data. |
| `id` | character | Unique player identifier. |
| `title` | character | Transaction title/headline. |
| `date` | character | Game date (ISO 8601 datetime string). |
| `uri` | character | Absolute Bifrost API URL of that date's scoreboard segment payload (e.g., 'https://api.foxsports.com/bifrost/v1/nhl/scoreboard/segment/20260919'). |
| `web_url` | character | Site-relative foxsports.com scoreboard page path for that date (e.g., '/scores/nhl?date=2026-09-19'). |
| `selected` | character | Fox's default-selection flag, which Fox sets only on group-filter (groupList) items; this league's navigation payload has no groupList, so the column is null on every row. The current date or week is marked by the payload-level currentSelectionId, which the parser does not return. |
| `group_id` | character | Group id (echoed from arg). |

**Example**

```python
from sportsdataverse.nhl import fox_nhl_scoreboard
df = fox_nhl_scoreboard()
```

### fox_nhl_scorechip {#fox_nhl_scorechip}

`fox_nhl_scorechip(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nhl compact live score chip (raw dict -- live-only, uncaptured shape).

Wraps `nhl/scorechip/{chip_id}`.

**Returns**

The raw JSON `dict`.

**Example**

```python
from sportsdataverse.nhl import fox_nhl_scorechip
df = fox_nhl_scorechip("nfl12345")
```

### fox_nhl_scores_segment {#fox_nhl_scores_segment}

`fox_nhl_scores_segment(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nhl one row per game in a scoreboard segment.

Wraps `nhl/league/scores-segment/{segment_id}`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nhl import fox_nhl_scores_segment
df = fox_nhl_scores_segment("...")
```

### fox_nhl_standings {#fox_nhl_standings}

`fox_nhl_standings(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NHL standings for a team's conference/division.

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
from sportsdataverse.nhl import fox_nhl_standings
df = fox_nhl_standings("...")
```

### fox_nhl_team_gamelog {#fox_nhl_team_gamelog}

`fox_nhl_team_gamelog(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NHL team game log (long: one row per game-stat).

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
from sportsdataverse.nhl import fox_nhl_team_gamelog
df = fox_nhl_team_gamelog("...")
```

### fox_nhl_team_header {#fox_nhl_team_header}

`fox_nhl_team_header(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nhl team header (one row).

Wraps `nhl/team/{team_id}/header`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nhl import fox_nhl_team_header
df = fox_nhl_team_header("...")
```

### fox_nhl_team_roster {#fox_nhl_team_roster}

`fox_nhl_team_roster(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NHL team roster (one row per player).

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
from sportsdataverse.nhl import fox_nhl_team_roster
df = fox_nhl_team_roster("...")
```

### fox_nhl_team_stats {#fox_nhl_team_stats}

`fox_nhl_team_stats(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NHL team stat leaders by category.

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
from sportsdataverse.nhl import fox_nhl_team_stats
df = fox_nhl_team_stats("...")
```

### fox_nhl_teamnav {#fox_nhl_teamnav}

`fox_nhl_teamnav(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nhl team directory (one row per team).

Wraps `nhl/league/teamnav`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `group` | character | Stat group (e.g. "hitting", "pitching", "fielding"). |
| `fox_id` | character | Fox Sports team id as a string (e.g., '14'), the trailing number of content_uri. |
| `abbreviation` | character | Team abbreviation. |
| `name` | character | Team mascot name. |
| `content_uri` | character | Fox content URI of the team (e.g., 'hockey/nhl/teams/14'); fox_id is its trailing number. |
| `content_type` | character | Fox entity type of the nav item; always 'team' in sampled data. |
| `web_url` | character | Site-relative foxsports.com path of the team page (e.g., '/nhl/detroit-red-wings-team'). |
| `color` | character | Primary color hex. |
| `logo_url` | character | NBA CDN primary logo URL. |

**Example**

```python
from sportsdataverse.nhl import fox_nhl_teamnav
df = fox_nhl_teamnav()
```
