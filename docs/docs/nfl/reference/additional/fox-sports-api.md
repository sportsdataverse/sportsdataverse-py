---
title: "NFL — additional Python functions — Fox Sports API"
sidebar_label: "Fox Sports API"
sidebar_position: 9
description: "NFL — additional Python functions — Fox Sports API — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Fox Sports API

### fox_nfl_boxscore {#fox_nfl_boxscore}

`fox_nfl_boxscore(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NFL boxscore (long: one row per player-stat).

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
from sportsdataverse.nfl import fox_nfl_boxscore
df = fox_nfl_boxscore("...")
```

### fox_nfl_event_matchup {#fox_nfl_event_matchup}

`fox_nfl_event_matchup(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nfl pregame team-stat comparison (one row per stat).

Wraps `nfl/event/{game_id}/matchup`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nfl import fox_nfl_event_matchup
df = fox_nfl_event_matchup("...")
```

### fox_nfl_event_recap {#fox_nfl_event_recap}

`fox_nfl_event_recap(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nfl postgame top performers (one row per player).

Wraps `nfl/event/{game_id}/recap`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nfl import fox_nfl_event_recap
df = fox_nfl_event_recap("...")
```

### fox_nfl_event_standings {#fox_nfl_event_standings}

`fox_nfl_event_standings(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nfl the two teams' standings context.

Wraps `nfl/event/{game_id}/standings`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nfl import fox_nfl_event_standings
df = fox_nfl_event_standings("...")
```

### fox_nfl_league_conferences {#fox_nfl_league_conferences}

`fox_nfl_league_conferences(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nfl conference / group directory.

Wraps `nfl/league/conferences`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nfl import fox_nfl_league_conferences
df = fox_nfl_league_conferences()
```

### fox_nfl_league_header {#fox_nfl_league_header}

`fox_nfl_league_header(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nfl league header (one row).

Wraps `nfl/league/header`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `template` | character | Fox layout template name for the header payload; 'entity-header' in sampled data. |
| `title` | character |  |
| `entity_id` | character | Fox id of the league entity as a string: the trailing number of the league's Fox contentUri. |
| `content_uri` | character | Fox Bifrost content path of the league entity (e.g. 'football/nfl/league/1'); entity_id is its trailing number. |
| `content_type` | character | Fox entity type of the header's entity; 'league' for the league header. |
| `color` | character |  |
| `logo_url` | character |  |
| `image_alt_text` | character | Alt text Fox supplies for the entity's logo image (e.g. 'National Football League'). |
| `rank` | character | QBR Rank in specified timeframe |
| `details` | character |  |

**Example**

```python
from sportsdataverse.nfl import fox_nfl_league_header
df = fox_nfl_league_header()
```

### fox_nfl_league_leaders {#fox_nfl_league_leaders}

`fox_nfl_league_leaders(category: 'str' = 'scoring', who: 'str' = 'player', page: 'int' = 0, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NFL statistical leaders (`stats-con`); who=player|team.

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
| `players` | character |  |
| `v1` | character | Unlabeled second column of the Fox table (blank header, so named by position): the player's abbreviated name, e.g. 'J. Allen'. |
| `pts` | character |  |
| `gp` | character |  |
| `pts_g` | character | Points per game for the leader, as a one-decimal string (e.g. '24.0'); null on rows stacked in from a leader table that has no such column. |
| `entity_id` | character | Fox id of the row's linked player or team as a string: the trailing number of the row's entityLink contentUri. |
| `td` | character | Touchdowns credited to the leader, as an integer string (e.g. '4'); null on rows stacked in from a leader table that has no such column. |

**Example**

```python
from sportsdataverse.nfl import fox_nfl_league_leaders
df = fox_nfl_league_leaders("scoring")
```

### fox_nfl_league_odds {#fox_nfl_league_odds}

`fox_nfl_league_odds(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nfl league odds board (one row per team per game).

Wraps `nfl/league/odds`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `section` | character | Title of the Fox odds-board section the game is listed under; always 'GAMES' in sampled data. |
| `game_id` | character | Ten digit identifier for NFL game. |
| `event_time` | character | Scheduled start time of the game as an ISO-8601 UTC timestamp string (e.g. '2026-09-20T17:00:00Z'). |
| `event_status` | integer | Fox numeric event status code for the game; always 2 in sampled data, captured when every listed game was still to be played. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `spread` | character |  |
| `to_win` | character | Moneyline for the row's team to win the game, as an American-odds string (e.g. '+196', '-238'). |
| `total` | character | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |

**Example**

```python
from sportsdataverse.nfl import fox_nfl_league_odds
df = fox_nfl_league_odds()
```

### fox_nfl_league_player_news {#fox_nfl_league_player_news}

`fox_nfl_league_player_news(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nfl league-wide player news feed.

Wraps `nfl/league/playernews`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `title` | character |  |
| `subtitle` | character | Player context line in 'TEAM #jersey - POSITION' form (e.g. 'HOU #12 - WR'). |
| `headline` | character |  |
| `description` | character |  |
| `impact_title` | character | Heading Fox shows above the impact paragraph; always 'Impact' in sampled data. |
| `impact` | character | Free-text analysis paragraph (headed by impact_title) on what the news means for the player's availability or role. |
| `date` | character |  |
| `source` | character |  |
| `athlete_id` | character |  |
| `content_uri` | character | Fox Bifrost content path of the player the news item is about, in sport/league/entity-type/id form (e.g. 'football/nfl/athletes/22256'). |
| `web_url` | character | Site-relative foxsports.com path of the player's page (not an absolute URL), e.g. '/nfl/nico-collins-player'. |

**Example**

```python
from sportsdataverse.nfl import fox_nfl_league_player_news
df = fox_nfl_league_player_news()
```

### fox_nfl_league_polls {#fox_nfl_league_polls}

`fox_nfl_league_polls(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nfl rankings / polls rendered as standings tables.

Wraps `nfl/league/polls`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nfl import fox_nfl_league_polls
df = fox_nfl_league_polls()
```

### fox_nfl_league_schedule {#fox_nfl_league_schedule}

`fox_nfl_league_schedule(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nfl league schedule nav selections.

Wraps `nfl/league/schedule`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | character | Fox navigation list the row came from (groupList, dailyList or selectionList); always 'selectionList' in sampled data. |
| `id` | character | ID of the player in the 'name' column. |
| `title` | character |  |
| `date` | character |  |
| `uri` | character | Absolute Fox Bifrost API URL (https://api.foxsports.com/bifrost/v1/nfl/...) of the segment feed this selection loads, ending in a segment id such as 2026-1-1. |
| `web_url` | character | Site-relative foxsports.com path of the page for this selection (not an absolute URL), carrying seasonType and week query parameters such as seasonType=reg&week=1. |
| `selected` | character | Fox's default-selection flag, which Fox sets only on group-filter (groupList) items; this league's navigation payload has no groupList, so the column is null on every row. The current date or week is marked by the payload-level currentSelectionId, which the parser does not return. |
| `group_id` | character |  |

**Example**

```python
from sportsdataverse.nfl import fox_nfl_league_schedule
df = fox_nfl_league_schedule()
```

### fox_nfl_league_scores {#fox_nfl_league_scores}

`fox_nfl_league_scores(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nfl league scores nav selections.

Wraps `nfl/league/scores`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | character | Fox navigation list the row came from (groupList, dailyList or selectionList); always 'selectionList' in sampled data. |
| `id` | character | ID of the player in the 'name' column. |
| `title` | character |  |
| `date` | character |  |
| `uri` | character | Absolute Fox Bifrost API URL (https://api.foxsports.com/bifrost/v1/nfl/...) of the segment feed this selection loads, ending in a segment id such as 2026-1-1. |
| `web_url` | character | Site-relative foxsports.com path of the page for this selection (not an absolute URL), carrying seasonType and week query parameters such as seasonType=reg&week=1. |
| `selected` | character | Fox's default-selection flag, which Fox sets only on group-filter (groupList) items; this league's navigation payload has no groupList, so the column is null on every row. The current date or week is marked by the payload-level currentSelectionId, which the parser does not return. |
| `group_id` | character |  |

**Example**

```python
from sportsdataverse.nfl import fox_nfl_league_scores
df = fox_nfl_league_scores()
```

### fox_nfl_league_standings {#fox_nfl_league_standings}

`fox_nfl_league_standings(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nfl league-wide standings tables.

Wraps `nfl/league/standings`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `section` | character | Title of the Fox standings section the row's table belongs to: DIVISION, CONFERENCE or PRESEASON in sampled data. |
| `afc_east` | character | Team's position number in the AFC East standings table, stored as a string (e.g. '1', '2'); the column is named from that table's header cell, so it is null on rows from every other table. |
| `v1` | character | Unlabeled second column of the Fox table (blank header, so named by position): the team nickname, e.g. 'Bills'. |
| `w_l_t` | character | Team's record as a win-loss-tie string with the tie count shown only when nonzero (e.g. '1-0', '1-1-1'). |
| `pct` | character |  |
| `pf` | character |  |
| `pa` | character | Points allowed by the team, as an integer string (e.g. '31'). |
| `home` | character |  |
| `away` | character |  |
| `conf` | character |  |
| `div` | character | Team's division record as a 'W-L' string (e.g. '1-0', '0-1'); null on rows from standings tables that have no DIV column. |
| `strk` | character |  |
| `entity_id` | character | Fox id of the row's linked team as a string: the trailing number of the row's entityLink contentUri. |
| `afc_north` | character | Team's position number in the AFC North standings table, stored as a string (e.g. '1', '2'); the column is named from that table's header cell, so it is null on rows from every other table. |
| `afc_south` | character | Team's position number in the AFC South standings table, stored as a string (e.g. '1', '2'); the column is named from that table's header cell, so it is null on rows from every other table. |
| `afc_west` | character | Team's position number in the AFC West standings table, stored as a string (e.g. '1', '2'); the column is named from that table's header cell, so it is null on rows from every other table. |
| `nfc_east` | character | Team's position number in the NFC East standings table, stored as a string (e.g. '1', '2'); the column is named from that table's header cell, so it is null on rows from every other table. |
| `nfc_north` | character | Team's position number in the NFC North standings table, stored as a string (e.g. '1', '2'); the column is named from that table's header cell, so it is null on rows from every other table. |
| `nfc_south` | character | Team's position number in the NFC South standings table, stored as a string (e.g. '1', '2'); the column is named from that table's header cell, so it is null on rows from every other table. |
| `nfc_west` | character | Team's position number in the NFC West standings table, stored as a string (e.g. '1', '2'); the column is named from that table's header cell, so it is null on rows from every other table. |
| `american_football_conference` | character | Team's position number in the American Football Conference standings table, stored as a string (e.g. '1', '2'); the column is named from that table's header cell, so it is null on rows from every other table. |
| `national_football_conference` | character | Team's position number in the National Football Conference standings table, stored as a string (e.g. '1', '2'); the column is named from that table's header cell, so it is null on rows from every other table. |

**Example**

```python
from sportsdataverse.nfl import fox_nfl_league_standings
df = fox_nfl_league_standings()
```

### fox_nfl_league_stat_leaders {#fox_nfl_league_stat_leaders}

`fox_nfl_league_stat_leaders(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nfl league stats landing leaders.

Wraps `nfl/league/stats`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `category` | character | Broader category of player positions |
| `stat` | character |  |
| `stat_abbreviation` | character | Fox's short code for the leader stat (e.g. 'PYDS', 'RTD', 'K-RET YDS'). |
| `player` | character | Player name |
| `value` | character | Total contract value |

**Example**

```python
from sportsdataverse.nfl import fox_nfl_league_stat_leaders
df = fox_nfl_league_stat_leaders()
```

### fox_nfl_odds {#fox_nfl_odds}

`fox_nfl_odds(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NFL game odds six-pack (spread / to-win / total per team).

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
from sportsdataverse.nfl import fox_nfl_odds
df = fox_nfl_odds("...")
```

### fox_nfl_pbp {#fox_nfl_pbp}

`fox_nfl_pbp(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NFL play-by-play (one row per play; drive-based).

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
from sportsdataverse.nfl import fox_nfl_pbp
df = fox_nfl_pbp("...")
```

### fox_nfl_scoreboard {#fox_nfl_scoreboard}

`fox_nfl_scoreboard(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nfl scoreboard nav selections (weeks / dates / groups).

Wraps `nfl/scoreboard/main`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | character | Fox navigation list the row came from (groupList, dailyList or selectionList); always 'selectionList' in sampled data. |
| `id` | character | ID of the player in the 'name' column. |
| `title` | character |  |
| `date` | character |  |
| `uri` | character | Absolute Fox Bifrost API URL (https://api.foxsports.com/bifrost/v1/nfl/...) of the segment feed this selection loads, ending in a segment id such as 2026-1-1. |
| `web_url` | character | Site-relative foxsports.com path of the page for this selection (not an absolute URL), carrying seasonType and week query parameters such as seasonType=reg&week=1. |
| `selected` | character | Fox's default-selection flag, which Fox sets only on group-filter (groupList) items; this league's navigation payload has no groupList, so the column is null on every row. The current date or week is marked by the payload-level currentSelectionId, which the parser does not return. |
| `group_id` | character |  |

**Example**

```python
from sportsdataverse.nfl import fox_nfl_scoreboard
df = fox_nfl_scoreboard()
```

### fox_nfl_scorechip {#fox_nfl_scorechip}

`fox_nfl_scorechip(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nfl compact live score chip (raw dict -- live-only, uncaptured shape).

Wraps `nfl/scorechip/{chip_id}`.

**Returns**

The raw JSON `dict`.

**Example**

```python
from sportsdataverse.nfl import fox_nfl_scorechip
df = fox_nfl_scorechip("nfl12345")
```

### fox_nfl_scores_segment {#fox_nfl_scores_segment}

`fox_nfl_scores_segment(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nfl one row per game in a scoreboard segment.

Wraps `nfl/league/scores-segment/{segment_id}`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nfl import fox_nfl_scores_segment
df = fox_nfl_scores_segment("...")
```

### fox_nfl_standings {#fox_nfl_standings}

`fox_nfl_standings(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NFL standings for a team's conference/division.

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
from sportsdataverse.nfl import fox_nfl_standings
df = fox_nfl_standings("...")
```

### fox_nfl_team_gamelog {#fox_nfl_team_gamelog}

`fox_nfl_team_gamelog(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NFL team game log (long: one row per game-stat).

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
from sportsdataverse.nfl import fox_nfl_team_gamelog
df = fox_nfl_team_gamelog("...")
```

### fox_nfl_team_header {#fox_nfl_team_header}

`fox_nfl_team_header(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nfl team header (one row).

Wraps `nfl/team/{team_id}/header`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.nfl import fox_nfl_team_header
df = fox_nfl_team_header("...")
```

### fox_nfl_team_roster {#fox_nfl_team_roster}

`fox_nfl_team_roster(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NFL team roster (one row per player).

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
from sportsdataverse.nfl import fox_nfl_team_roster
df = fox_nfl_team_roster("...")
```

### fox_nfl_team_stats {#fox_nfl_team_stats}

`fox_nfl_team_stats(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NFL team stat leaders by category.

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
from sportsdataverse.nfl import fox_nfl_team_stats
df = fox_nfl_team_stats("...")
```

### fox_nfl_teamnav {#fox_nfl_teamnav}

`fox_nfl_teamnav(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports nfl team directory (one row per team).

Wraps `nfl/league/teamnav`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `group` | character |  |
| `fox_id` | character | Fox Sports team id as a string, taken from the trailing number of content_uri (e.g. '17'). |
| `abbreviation` | character |  |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `content_uri` | character | Fox Bifrost content path of the team in sport/league/entity-type/id form (e.g. 'football/nfl/teams/17'). |
| `content_type` | character | Fox entity type of the navigation item's entity; 'team' for team rows. |
| `web_url` | character | Site-relative foxsports.com path of the team's page (not an absolute URL), e.g. '/nfl/detroit-lions-team'. |
| `color` | character |  |
| `logo_url` | character |  |

**Example**

```python
from sportsdataverse.nfl import fox_nfl_teamnav
df = fox_nfl_teamnav()
```
