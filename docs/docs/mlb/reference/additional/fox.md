---
title: "MLB — additional Python functions — Fox"
sidebar_label: "Fox"
sidebar_position: 1
description: "MLB — additional Python functions — Fox — function reference in sdv-py, the SportsDataverse Python package."
---
# MLB — additional Python functions — Fox

### fox_mlb_event_matchup {#fox_mlb_event_matchup}

`fox_mlb_event_matchup(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports mlb pregame team-stat comparison (one row per stat).

Wraps `mlb/event/{game_id}/matchup`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.mlb import fox_mlb_event_matchup
df = fox_mlb_event_matchup("...")
```

### fox_mlb_event_recap {#fox_mlb_event_recap}

`fox_mlb_event_recap(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports mlb postgame top performers (one row per player).

Wraps `mlb/event/{game_id}/recap`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.mlb import fox_mlb_event_recap
df = fox_mlb_event_recap("...")
```

### fox_mlb_event_standings {#fox_mlb_event_standings}

`fox_mlb_event_standings(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports mlb the two teams' standings context.

Wraps `mlb/event/{game_id}/standings`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.mlb import fox_mlb_event_standings
df = fox_mlb_event_standings("...")
```

### fox_mlb_league_conferences {#fox_mlb_league_conferences}

`fox_mlb_league_conferences(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports mlb conference / group directory.

Wraps `mlb/league/conferences`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.mlb import fox_mlb_league_conferences
df = fox_mlb_league_conferences()
```

### fox_mlb_league_header {#fox_mlb_league_header}

`fox_mlb_league_header(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports mlb league header (one row).

Wraps `mlb/league/header`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `template` | character | Fox layout template name for the header payload; always 'entity-header' in sampled data. |
| `title` | character | Specific role title for the assignment. |
| `entity_id` | character | Fox id of the league entity as a string: the trailing number of the league's Fox contentUri. |
| `content_uri` | character | Fox content URI of the league entity (e.g., 'baseball/mlb/league/1'); entity_id is its trailing number. |
| `content_type` | character | Fox entity type of the header payload; always 'league' for the league header. |
| `color` | character | Primary color (hex, no leading '#'). |
| `logo_url` | character |  |
| `image_alt_text` | character | Image alt text Fox ships with the header, the full league name (e.g., 'Major League Baseball'). |
| `rank` | character | Rank within the team leaderboard. |
| `details` | character | Details. |

**Example**

```python
from sportsdataverse.mlb import fox_mlb_league_header
df = fox_mlb_league_header()
```

### fox_mlb_league_leaders {#fox_mlb_league_leaders}

`fox_mlb_league_leaders(category: 'str' = 'batting', who: 'str' = 'player', page: 'int' = 0, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

MLB statistical leaders (`stats-con`); who=player|team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `category` | `str` | `'batting'` | Stat category. Defaults to `"batting"`. |
| `who` | `str` | `'player'` | `"player"` or `"team"`. Defaults to `"player"`. |
| `page` | `int` | `0` | 0-based result page. Defaults to `0`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the leader tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `players` | character |  |
| `v1` | character | Abbreviated player name (e.g., 'M. Olson'), from the leader table's unlabeled second column. |
| `g` | character |  |
| `entity_id` | character | Fox id of the row's linked player or team as a string: the trailing number of the row's entityLink contentUri. |
| `pa` | character | Plate appearances from the PA leader table, as a string (e.g., '687'); null on rows that come from another stat's leader table (G, AB, H). |
| `ab` | character | At-bats. |
| `h` | character | Hits. |

**Example**

```python
from sportsdataverse.mlb import fox_mlb_league_leaders
df = fox_mlb_league_leaders("batting")
```

### fox_mlb_league_odds {#fox_mlb_league_odds}

`fox_mlb_league_odds(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports mlb league odds board (one row per team per game).

Wraps `mlb/league/odds`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `section` | character | Title of the odds-board section the game sits in; always 'GAMES' in sampled data. |
| `game_id` | character | Unique ESPN game/event identifier. |
| `event_time` | character | Scheduled start time as an ISO 8601 UTC timestamp (e.g., '2026-09-17T16:35:00Z'). |
| `event_status` | integer | Fox numeric event status code; 2 for every sampled game, all scheduled to start after the sample was taken (Fox uses 3 for final). |
| `team` | character | Team. |
| `run_line` | character | Team's run-line spread as a signed string ('-1.5' or '+1.5'). |
| `to_win` | character | Team's moneyline in American odds, as a signed string (e.g., '-142', '+118'). |
| `total` | character | Total. |

**Example**

```python
from sportsdataverse.mlb import fox_mlb_league_odds
df = fox_mlb_league_odds()
```

### fox_mlb_league_player_news {#fox_mlb_league_player_news}

`fox_mlb_league_player_news(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports mlb league-wide player news feed.

Wraps `mlb/league/playernews`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `title` | character | Specific role title for the assignment. |
| `subtitle` | character | Player's team abbreviation, jersey number and position formatted 'TEAM #NN - POS' (e.g., 'AZ #14 - C'). |
| `headline` | character | News headline. |
| `description` | character | Long-form description text. |
| `impact_title` | character | Heading label for the impact paragraph; always 'Impact' in sampled data. |
| `impact` | character | Free-text analysis paragraph Fox shows under the item's impact_title heading. |
| `date` | character | Date in YYYY-MM-DD format. |
| `source` | character | Source. |
| `athlete_id` | character | Unique ESPN athlete identifier. |
| `content_uri` | character | Fox content URI of the player the item is about (e.g., 'baseball/mlb/athletes/11258'); athlete_id is its trailing number. |
| `web_url` | character | Site-relative foxsports.com path of the player's page (e.g., '/mlb/gabriel-moreno-player'). |

**Example**

```python
from sportsdataverse.mlb import fox_mlb_league_player_news
df = fox_mlb_league_player_news()
```

### fox_mlb_league_polls {#fox_mlb_league_polls}

`fox_mlb_league_polls(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports mlb rankings / polls rendered as standings tables.

Wraps `mlb/league/polls`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.mlb import fox_mlb_league_polls
df = fox_mlb_league_polls()
```

### fox_mlb_league_schedule {#fox_mlb_league_schedule}

`fox_mlb_league_schedule(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports mlb league schedule nav selections.

Wraps `mlb/league/schedule`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | character | Which Fox navigation list the row came from ('groupList', 'dailyList' or 'selectionList'); always 'dailyList' (one row per date) in sampled data. |
| `id` | character | Id. |
| `title` | character | Specific role title for the assignment. |
| `date` | character | Date in YYYY-MM-DD format. |
| `uri` | character | Absolute Bifrost API URL of that date's schedule segment payload (e.g., 'https://api.foxsports.com/bifrost/v1/mlb/league/schedule-segment/2026-20260220'). |
| `web_url` | character | Site-relative foxsports.com schedule page path for that date (e.g., '/mlb/schedule?date=2026-02-20'). |
| `selected` | character | Fox's default-selection flag, which Fox sets only on group-filter (groupList) items; this league's navigation payload has no groupList, so the column is null on every row. The current date or week is marked by the payload-level currentSelectionId, which the parser does not return. |
| `group_id` | character | ESPN group id. |

**Example**

```python
from sportsdataverse.mlb import fox_mlb_league_schedule
df = fox_mlb_league_schedule()
```

### fox_mlb_league_scores {#fox_mlb_league_scores}

`fox_mlb_league_scores(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports mlb league scores nav selections.

Wraps `mlb/league/scores`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | character | Which Fox navigation list the row came from ('groupList', 'dailyList' or 'selectionList'); always 'dailyList' (one row per date) in sampled data. |
| `id` | character | Id. |
| `title` | character | Specific role title for the assignment. |
| `date` | character | Date in YYYY-MM-DD format. |
| `uri` | character | Absolute Bifrost API URL of that date's scores segment payload (e.g., 'https://api.foxsports.com/bifrost/v1/mlb/league/scores-segment/20260220'). |
| `web_url` | character | Site-relative foxsports.com scores page path for that date (e.g., '/mlb/scores?date=2026-02-20'). |
| `selected` | character | Fox's default-selection flag, which Fox sets only on group-filter (groupList) items; this league's navigation payload has no groupList, so the column is null on every row. The current date or week is marked by the payload-level currentSelectionId, which the parser does not return. |
| `group_id` | character | ESPN group id. |

**Example**

```python
from sportsdataverse.mlb import fox_mlb_league_scores
df = fox_mlb_league_scores()
```

### fox_mlb_league_standings {#fox_mlb_league_standings}

`fox_mlb_league_standings(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports mlb league-wide standings tables.

Wraps `mlb/league/standings`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `section` | character | Title of the standings section the row's table sits in ('DIVISION', 'WILD CARD' or 'SPRING TRAINING'); each team appears once per section. |
| `al_east` | character | Position number ('1'-'5') from the first column of Fox's AL EAST division table, whose header text names this column; null on rows from every other table. |
| `v1` | character | Team nickname (e.g., 'Rays'), from the standings table's unlabeled second column. |
| `w_l` | character |  |
| `pct` | character |  |
| `gb` | character | Average exit velocity on ground balls (mph). |
| `home` | character | Home. |
| `away` | character |  |
| `rs` | character | Runs scored (RS column), as a string (e.g., '688'); null on SPRING TRAINING rows, whose tables carry no RS column. |
| `ra` | character | Runs allowed (RA column), as a string (e.g., '617'); null on SPRING TRAINING rows, whose tables carry no RA column. |
| `diff` | character |  |
| `l10` | character |  |
| `strk` | character |  |
| `entity_id` | character | Fox id of the row's linked team as a string: the trailing number of the row's entityLink contentUri. |
| `al_central` | character | Position number ('1'-'5') from the first column of Fox's AL CENTRAL division table, whose header text names this column; null on rows from every other table. |
| `al_west` | character | Position number ('1'-'5') from the first column of Fox's AL WEST division table, whose header text names this column; null on rows from every other table. |
| `nl_east` | character | Position number ('1'-'5') from the first column of Fox's NL EAST division table, whose header text names this column; null on rows from every other table. |
| `nl_central` | character | Position number ('1'-'5') from the first column of Fox's NL CENTRAL division table, whose header text names this column; null on rows from every other table. |
| `nl_west` | character | Position number ('1'-'5') from the first column of Fox's NL WEST division table, whose header text names this column; null on rows from every other table. |
| `division_leaders` | character | Number string from the first column of Fox's DIVISION LEADERS tables (one per league, WILD CARD section); Fox does not label it, sampled values '1'-'4' repeat and do not follow row order. Null on rows from every other table. |
| `wild_card` | character | Position number ('1'-'12') from the first column of Fox's WILD CARD tables (one per league, WILD CARD section), whose header text names this column; null on rows from every other table. |
| `cactus_league` | character | Position number ('1'-'15') from the first column of Fox's CACTUS LEAGUE spring-training table, not ordered by win percentage in sampled data; null on rows from every other table. |
| `grapefruit_league` | character | Position number ('1'-'15') from the first column of Fox's GRAPEFRUIT LEAGUE spring-training table, whose header text names this column; null on rows from every other table. |

**Example**

```python
from sportsdataverse.mlb import fox_mlb_league_standings
df = fox_mlb_league_standings()
```

### fox_mlb_league_stat_leaders {#fox_mlb_league_stat_leaders}

`fox_mlb_league_stat_leaders(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports mlb league stats landing leaders.

Wraps `mlb/league/stats`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `category` | character | Category label. |
| `stat` | character |  |
| `stat_abbreviation` | character | Fox's short label for the leader's stat (e.g., 'HR', 'ERA', 'ISO'). |
| `player` | character |  |
| `value` | character | Numeric value. |

**Example**

```python
from sportsdataverse.mlb import fox_mlb_league_stat_leaders
df = fox_mlb_league_stat_leaders()
```

### fox_mlb_odds {#fox_mlb_odds}

`fox_mlb_odds(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

MLB game odds six-pack (run line / to-win / total per team).

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
from sportsdataverse.mlb import fox_mlb_odds
df = fox_mlb_odds("...")
```

### fox_mlb_scoreboard {#fox_mlb_scoreboard}

`fox_mlb_scoreboard(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports mlb scoreboard nav selections (weeks / dates / groups).

Wraps `mlb/scoreboard/main`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | character | Which Fox navigation list the row came from ('groupList', 'dailyList' or 'selectionList'); always 'dailyList' (one row per date) in sampled data. |
| `id` | character | Id. |
| `title` | character | Specific role title for the assignment. |
| `date` | character | Date in YYYY-MM-DD format. |
| `uri` | character | Absolute Bifrost API URL of that date's scoreboard segment payload (e.g., 'https://api.foxsports.com/bifrost/v1/mlb/scoreboard/segment/20260220'). |
| `web_url` | character | Site-relative foxsports.com scoreboard page path for that date (e.g., '/scores/mlb?date=2026-02-20'). |
| `selected` | character | Fox's default-selection flag, which Fox sets only on group-filter (groupList) items; this league's navigation payload has no groupList, so the column is null on every row. The current date or week is marked by the payload-level currentSelectionId, which the parser does not return. |
| `group_id` | character | ESPN group id. |

**Example**

```python
from sportsdataverse.mlb import fox_mlb_scoreboard
df = fox_mlb_scoreboard()
```

### fox_mlb_scorechip {#fox_mlb_scorechip}

`fox_mlb_scorechip(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports mlb compact live score chip (raw dict -- live-only, uncaptured shape).

Wraps `mlb/scorechip/{chip_id}`.

**Returns**

The raw JSON `dict`.

**Example**

```python
from sportsdataverse.mlb import fox_mlb_scorechip
df = fox_mlb_scorechip("nfl12345")
```

### fox_mlb_scores_segment {#fox_mlb_scores_segment}

`fox_mlb_scores_segment(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports mlb one row per game in a scoreboard segment.

Wraps `mlb/league/scores-segment/{segment_id}`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.mlb import fox_mlb_scores_segment
df = fox_mlb_scores_segment("...")
```

### fox_mlb_standings {#fox_mlb_standings}

`fox_mlb_standings(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

MLB standings for a team's division/league.

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
from sportsdataverse.mlb import fox_mlb_standings
df = fox_mlb_standings("...")
```

### fox_mlb_team_gamelog {#fox_mlb_team_gamelog}

`fox_mlb_team_gamelog(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

MLB team game log (long: one row per game-stat).

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
from sportsdataverse.mlb import fox_mlb_team_gamelog
df = fox_mlb_team_gamelog("...")
```

### fox_mlb_team_header {#fox_mlb_team_header}

`fox_mlb_team_header(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports mlb team header (one row).

Wraps `mlb/team/{team_id}/header`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.mlb import fox_mlb_team_header
df = fox_mlb_team_header("...")
```

### fox_mlb_team_roster {#fox_mlb_team_roster}

`fox_mlb_team_roster(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

MLB team roster (one row per player).

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
from sportsdataverse.mlb import fox_mlb_team_roster
df = fox_mlb_team_roster("...")
```

### fox_mlb_team_stats {#fox_mlb_team_stats}

`fox_mlb_team_stats(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

MLB team stat leaders by category.

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
from sportsdataverse.mlb import fox_mlb_team_stats
df = fox_mlb_team_stats("...")
```

### fox_mlb_teamnav {#fox_mlb_teamnav}

`fox_mlb_teamnav(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports mlb team directory (one row per team).

Wraps `mlb/league/teamnav`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `group` | character | Stat group (e.g. "hitting", "pitching", "fielding"). |
| `fox_id` | character | Fox Sports team id as a string (e.g., '18'), the trailing number of content_uri. |
| `abbreviation` | character | Short abbreviation. |
| `name` | character | Display name. |
| `content_uri` | character | Fox content URI of the team (e.g., 'baseball/mlb/teams/18'); fox_id is its trailing number. |
| `content_type` | character | Fox entity type of the nav item; always 'team' in sampled data. |
| `web_url` | character | Site-relative foxsports.com path of the team page (e.g., '/mlb/philadelphia-phillies-team'). |
| `color` | character | Primary color (hex, no leading '#'). |
| `logo_url` | character |  |

**Example**

```python
from sportsdataverse.mlb import fox_mlb_teamnav
df = fox_mlb_teamnav()
```
