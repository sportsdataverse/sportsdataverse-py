---
title: "NFL — additional Python functions — Fox Sports API"
sidebar_label: "Fox Sports API"
sidebar_position: 10
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

| col_name | type | description |
|---|---|---|
| `game_id` | String | Ten digit identifier for NFL game. |
| `team` | String | NFL team. Uses official abbreviations as per NFL.com |
| `stat_group` | String |  |
| `player` | String | Player name |
| `athlete_id` | String |  |
| `stat` | String |  |
| `value` | String | Total contract value |

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
| `template` | String | Fox layout template name for the header payload; 'entity-header' in sampled data. |
| `title` | String |  |
| `entity_id` | String | Fox id of the league entity as a string: the trailing number of the league's Fox contentUri. |
| `content_uri` | String | Fox Bifrost content path of the league entity (e.g. 'football/nfl/league/1'); entity_id is its trailing number. |
| `content_type` | String | Fox entity type of the header's entity; 'league' for the league header. |
| `color` | String |  |
| `logo_url` | String |  |
| `image_alt_text` | String | Alt text Fox supplies for the entity's logo image (e.g. 'National Football League'). |
| `rank` | String | QBR Rank in specified timeframe |
| `details` | String |  |

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
| `players` | String |  |
| `v1` | String | Unlabeled second column of the Fox table (blank header, so named by position): the player's abbreviated name, e.g. 'J. Allen'. |
| `pts` | String |  |
| `gp` | String |  |
| `pts_g` | String | Points per game for the leader, as a one-decimal string (e.g. '24.0'); null on rows stacked in from a leader table that has no such column. |
| `entity_id` | String | Fox id of the row's linked player or team as a string: the trailing number of the row's entityLink contentUri. |
| `td` | String | Touchdowns credited to the leader, as an integer string (e.g. '4'); null on rows stacked in from a leader table that has no such column. |

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
| `section` | String | Title of the Fox odds-board section the game is listed under; always 'GAMES' in sampled data. |
| `game_id` | String | Ten digit identifier for NFL game. |
| `event_time` | String | Scheduled start time of the game as an ISO-8601 UTC timestamp string (e.g. '2026-09-20T17:00:00Z'). |
| `event_status` | Int64 | Fox numeric event status code for the game; always 2 in sampled data, captured when every listed game was still to be played. |
| `team` | String | NFL team. Uses official abbreviations as per NFL.com |
| `spread` | String |  |
| `to_win` | String | Moneyline for the row's team to win the game, as an American-odds string (e.g. '+196', '-238'). |
| `total` | String | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |

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
| `title` | String |  |
| `subtitle` | String | Player context line in 'TEAM #jersey - POSITION' form (e.g. 'HOU #12 - WR'). |
| `headline` | String |  |
| `description` | String |  |
| `impact_title` | String | Heading Fox shows above the impact paragraph; always 'Impact' in sampled data. |
| `impact` | String | Free-text analysis paragraph (headed by impact_title) on what the news means for the player's availability or role. |
| `date` | String |  |
| `source` | String |  |
| `athlete_id` | String |  |
| `content_uri` | String | Fox Bifrost content path of the player the news item is about, in sport/league/entity-type/id form (e.g. 'football/nfl/athletes/22256'). |
| `web_url` | String | Site-relative foxsports.com path of the player's page (not an absolute URL), e.g. '/nfl/nico-collins-player'. |

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
| `selection_list` | String | Fox navigation list the row came from (groupList, dailyList or selectionList); always 'selectionList' in sampled data. |
| `id` | String | ID of the player in the 'name' column. |
| `title` | String |  |
| `date` | String |  |
| `uri` | String | Absolute Fox Bifrost API URL (https://api.foxsports.com/bifrost/v1/nfl/...) of the segment feed this selection loads, ending in a segment id such as 2026-1-1. |
| `web_url` | String | Site-relative foxsports.com path of the page for this selection (not an absolute URL), carrying seasonType and week query parameters such as seasonType=reg&week=1. |
| `selected` | String | Fox's default-selection flag, which Fox sets only on group-filter (groupList) items; this league's navigation payload has no groupList, so the column is null on every row. The current date or week is marked by the payload-level currentSelectionId, which the parser does not return. |
| `group_id` | String |  |

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
| `selection_list` | String | Fox navigation list the row came from (groupList, dailyList or selectionList); always 'selectionList' in sampled data. |
| `id` | String | ID of the player in the 'name' column. |
| `title` | String |  |
| `date` | String |  |
| `uri` | String | Absolute Fox Bifrost API URL (https://api.foxsports.com/bifrost/v1/nfl/...) of the segment feed this selection loads, ending in a segment id such as 2026-1-1. |
| `web_url` | String | Site-relative foxsports.com path of the page for this selection (not an absolute URL), carrying seasonType and week query parameters such as seasonType=reg&week=1. |
| `selected` | String | Fox's default-selection flag, which Fox sets only on group-filter (groupList) items; this league's navigation payload has no groupList, so the column is null on every row. The current date or week is marked by the payload-level currentSelectionId, which the parser does not return. |
| `group_id` | String |  |

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
| `section` | String | Title of the Fox standings section the row's table belongs to: DIVISION, CONFERENCE or PRESEASON in sampled data. |
| `afc_east` | String | Team's position number in the AFC East standings table, stored as a string (e.g. '1', '2'); the column is named from that table's header cell, so it is null on rows from every other table. |
| `v1` | String | Unlabeled second column of the Fox table (blank header, so named by position): the team nickname, e.g. 'Bills'. |
| `w_l_t` | String | Team's record as a win-loss-tie string with the tie count shown only when nonzero (e.g. '1-0', '1-1-1'). |
| `pct` | String |  |
| `pf` | String |  |
| `pa` | String | Points allowed by the team, as an integer string (e.g. '31'). |
| `home` | String |  |
| `away` | String |  |
| `conf` | String |  |
| `div` | String | Team's division record as a 'W-L' string (e.g. '1-0', '0-1'); null on rows from standings tables that have no DIV column. |
| `strk` | String |  |
| `entity_id` | String | Fox id of the row's linked team as a string: the trailing number of the row's entityLink contentUri. |
| `afc_north` | String | Team's position number in the AFC North standings table, stored as a string (e.g. '1', '2'); the column is named from that table's header cell, so it is null on rows from every other table. |
| `afc_south` | String | Team's position number in the AFC South standings table, stored as a string (e.g. '1', '2'); the column is named from that table's header cell, so it is null on rows from every other table. |
| `afc_west` | String | Team's position number in the AFC West standings table, stored as a string (e.g. '1', '2'); the column is named from that table's header cell, so it is null on rows from every other table. |
| `nfc_east` | String | Team's position number in the NFC East standings table, stored as a string (e.g. '1', '2'); the column is named from that table's header cell, so it is null on rows from every other table. |
| `nfc_north` | String | Team's position number in the NFC North standings table, stored as a string (e.g. '1', '2'); the column is named from that table's header cell, so it is null on rows from every other table. |
| `nfc_south` | String | Team's position number in the NFC South standings table, stored as a string (e.g. '1', '2'); the column is named from that table's header cell, so it is null on rows from every other table. |
| `nfc_west` | String | Team's position number in the NFC West standings table, stored as a string (e.g. '1', '2'); the column is named from that table's header cell, so it is null on rows from every other table. |
| `american_football_conference` | String | Team's position number in the American Football Conference standings table, stored as a string (e.g. '1', '2'); the column is named from that table's header cell, so it is null on rows from every other table. |
| `national_football_conference` | String | Team's position number in the National Football Conference standings table, stored as a string (e.g. '1', '2'); the column is named from that table's header cell, so it is null on rows from every other table. |

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
| `category` | String | Broader category of player positions |
| `stat` | String |  |
| `stat_abbreviation` | String | Fox's short code for the leader stat (e.g. 'PYDS', 'RTD', 'K-RET YDS'). |
| `player` | String | Player name |
| `value` | String | Total contract value |

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

| col_name | type | description |
|---|---|---|
| `game_id` | String | Ten digit identifier for NFL game. |
| `team` | String | NFL team. Uses official abbreviations as per NFL.com |
| `spread` | String |  |
| `to_win` | String |  |
| `total` | String | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |

**Example**

```python
from sportsdataverse.nfl import fox_nfl_odds
df = fox_nfl_odds("...")
```

### fox_nfl_pbp {#fox_nfl_pbp}

`fox_nfl_pbp(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

NFL play-by-play (one row per play, oldest first, live or final; drive-based).

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
| `game_id` | String | Ten digit identifier for NFL game. |
| `quarter` | String |  |
| `drive_id` | String |  |
| `drive_result` | String |  |
| `drive_summary` | String |  |
| `drive_team` | String |  |
| `play_id` | String | Numeric play id that when used with game_id and drive provides the unique identifier for a single play. |
| `period` | String |  |
| `clock` | String |  |
| `field_position` | String | Ball spot expressed on Yahoo's 0-100 field scale, measured toward the offense's target goal line. |
| `play_text` | String |  |
| `play_team` | String | Club code from the game header (e.g. 'NYG') of the drive group Fox files the play under, i.e. the possession team; on a kickoff it is the receiving team, since Fox opens the receiver's drive with the kick. |
| `play_player` | String | Name of the athlete Fox credits the play to (its play-level entity link), upper-cased as Fox serves it (e.g. 'DEVIN SINGLETARY'); null on admin rows such as timeouts and period ends. |

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
| `selection_list` | String | Fox navigation list the row came from (groupList, dailyList or selectionList); always 'selectionList' in sampled data. |
| `id` | String | ID of the player in the 'name' column. |
| `title` | String |  |
| `date` | String |  |
| `uri` | String | Absolute Fox Bifrost API URL (https://api.foxsports.com/bifrost/v1/nfl/...) of the segment feed this selection loads, ending in a segment id such as 2026-1-1. |
| `web_url` | String | Site-relative foxsports.com path of the page for this selection (not an absolute URL), carrying seasonType and week query parameters such as seasonType=reg&week=1. |
| `selected` | String | Fox's default-selection flag, which Fox sets only on group-filter (groupList) items; this league's navigation payload has no groupList, so the column is null on every row. The current date or week is marked by the payload-level currentSelectionId, which the parser does not return. |
| `group_id` | String |  |

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

| col_name | type | description |
|---|---|---|
| `team_id` | String |  |
| `section` | String |  |
| `afc_west` | String |  |
| `v1` | String |  |
| `w_l_t` | String |  |
| `pct` | String |  |
| `pf` | String |  |
| `pa` | String |  |
| `home` | String |  |
| `away` | String |  |
| `conf` | String |  |
| `div` | String |  |
| `strk` | String |  |
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `afc_east` | String |  |
| `afc_north` | String |  |
| `afc_south` | String |  |
| `nfc_east` | String |  |
| `nfc_north` | String |  |
| `nfc_south` | String |  |
| `nfc_west` | String |  |
| `american_football_conference` | String |  |
| `national_football_conference` | String |  |

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

| col_name | type | description |
|---|---|---|
| `team_id` | String |  |
| `season_type` | String | REG or POST indicating if the timeframe belongs to regular or post season. |
| `category` | String | Broader category of player positions |
| `game_id` | String | Ten digit identifier for NFL game. |
| `game_date` | String | Date of the game. |
| `opponent` | String | Opposing team of player |
| `stat` | String |  |
| `value` | String | Total contract value |

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

| col_name | type | description |
|---|---|---|
| `team_id` | String |  |
| `position_group` | String | Postion group of player as listed by NFL |
| `player` | String | Player name |
| `pos` | String | Position as tracked by FP |
| `age` | String | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `ht` | String | Height of player (feet and inches) |
| `wt` | String | Weight of player (lbs) |
| `college` | String | Official college (usually the last one attended) |
| `athlete_id` | String |  |

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

| col_name | type | description |
|---|---|---|
| `team_id` | String |  |
| `category` | String | Broader category of player positions |
| `stat` | String |  |
| `stat_abbreviation` | String |  |
| `player` | String | Player name |
| `value` | String | Total contract value |

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
| `group` | String |  |
| `fox_id` | String | Fox Sports team id as a string, taken from the trailing number of content_uri (e.g. '17'). |
| `abbreviation` | String |  |
| `name` | String | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `content_uri` | String | Fox Bifrost content path of the team in sport/league/entity-type/id form (e.g. 'football/nfl/teams/17'). |
| `content_type` | String | Fox entity type of the navigation item's entity; 'team' for team rows. |
| `web_url` | String | Site-relative foxsports.com path of the team's page (not an absolute URL), e.g. '/nfl/detroit-lions-team'. |
| `color` | String |  |
| `logo_url` | String |  |

**Example**

```python
from sportsdataverse.nfl import fox_nfl_teamnav
df = fox_nfl_teamnav()
```
