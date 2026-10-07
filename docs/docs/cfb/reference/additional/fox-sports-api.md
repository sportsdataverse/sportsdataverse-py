---
title: "CFB — additional Python functions — Fox Sports API"
sidebar_label: "Fox Sports API"
sidebar_position: 6
description: "CFB — additional Python functions — Fox Sports API — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — Fox Sports API

### fox_cfb_boxscore {#fox_cfb_boxscore}

`fox_cfb_boxscore(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

Fox Sports CFB boxscore (long: one row per player-stat).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/event/{game_id}/data`
(the `boxscore` block).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Fox Bifrost event id (e.g. `"41616"`). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the per-team stat tables to long form; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_boxscore
df = fox_cfb_boxscore("41616")
```

### fox_cfb_event_matchup {#fox_cfb_event_matchup}

`fox_cfb_event_matchup(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports cfb pregame team-stat comparison (one row per stat).

Wraps `cfb/event/{game_id}/matchup`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_event_matchup
df = fox_cfb_event_matchup("...")
```

### fox_cfb_event_recap {#fox_cfb_event_recap}

`fox_cfb_event_recap(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports cfb postgame top performers (one row per player).

Wraps `cfb/event/{game_id}/recap`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_event_recap
df = fox_cfb_event_recap("...")
```

### fox_cfb_event_standings {#fox_cfb_event_standings}

`fox_cfb_event_standings(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports cfb the two teams' standings context.

Wraps `cfb/event/{game_id}/standings`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_event_standings
df = fox_cfb_event_standings("...")
```

### fox_cfb_league_conferences {#fox_cfb_league_conferences}

`fox_cfb_league_conferences(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports cfb conference / group directory.

Wraps `cfb/league/conferences`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `group` | character | Stat group (e.g. "hitting", "pitching", "fielding"). |
| `fox_id` | character | Fox group id of the conference as a string, the trailing number of content_uri (e.g. '9' for the ACC); the same ids are the groupId filters in the Fox scoreboard navigation. |
| `abbreviation` | character | Metric abbreviation. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `content_uri` | character | Fox Bifrost content path of the conference (e.g. 'football/cfb/groups/9'). |
| `content_type` | character | Fox entity type of the linked item; 'league' on every sampled conference row. |
| `web_url` | character | Site-relative foxsports.com path of the conference page (e.g. '/college-football/acc'). |
| `color` | character | Primary team color (hex, no `#`). |
| `logo_url` | character | NBA CDN primary logo URL. |

**Example**

```python
from sportsdataverse.cfb import fox_cfb_league_conferences
df = fox_cfb_league_conferences()
```

### fox_cfb_league_header {#fox_cfb_league_header}

`fox_cfb_league_header(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports cfb league header (one row).

Wraps `cfb/league/header`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `template` | character | Name of the Fox layout template for the header payload, 'entity-header' in the sample. |
| `title` | character | Specific role title for the assignment. |
| `entity_id` | character | Fox id of the league entity as a string: the trailing number of the league's Fox contentUri. |
| `content_uri` | character | Fox Bifrost content path of the league entity (e.g. 'football/cfb/league/1'); entity_id is its trailing number. |
| `content_type` | character | Fox entity type of the header; 'league' for this league-level header. |
| `color` | character | Primary team color (hex, no `#`). |
| `logo_url` | character | NBA CDN primary logo URL. |
| `image_alt_text` | character | Alt text Fox attaches to the league logo image, 'College Football' in the sample. |
| `rank` | character | Position of the school within the poll for the given week (1 = top-ranked). |
| `details` | character | ESPN's headline line string (e.g. `UGA -54.5`). |

**Example**

```python
from sportsdataverse.cfb import fox_cfb_league_header
df = fox_cfb_league_header()
```

### fox_cfb_league_leaders {#fox_cfb_league_leaders}

`fox_cfb_league_leaders(category: 'str' = 'passing', who: 'str' = 'player', page: 'int' = 0, group_id: 'Union[int, str]' = '2', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

Fox Sports CFB statistical leaders (one row per player/team).

Endpoint: `GET .../bifrost/v1/cfb/league/stats-con/{who}/{category}/{page}`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `category` | `str` | `'passing'` | Stat category -- passing, rushing, receiving, defense, kicking, returning, scoring, yardage (team adds downs, turnovers). Defaults to `"passing"`. |
| `who` | `str` | `'player'` | `"player"` or `"team"`. Defaults to `"player"`. |
| `page` | `int` | `0` | 0-based result page. Defaults to `0`. |
| `group_id` | `Union[int, str]` | `'2'` | Conference/group filter. Defaults to `"2"`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the leader tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `players` | character | Nested list of per-player box scores. |
| `v1` | character | Abbreviated player name, first initial plus surname (e.g. 'M. Alejado'), from Fox's untitled second column; the players column beside it holds the rank. |
| `comp` | character | Pass completions as a string (e.g. '81'). Filled only on the 25 rows of Fox's COMP table; null on the rows of the other two top-25 tables stacked into the default passing frame. |
| `gp` | character | Games played. |
| `entity_id` | character | Fox id of the row's linked player or team as a string: the trailing number of the row's entityLink contentUri. |
| `patt` | character | Pass attempts as a string (e.g. '137'). Filled only on the 25 rows of Fox's PATT table; null on the rows of the other two top-25 tables stacked into the default passing frame. |
| `att_g` | character | Pass attempts per game with one decimal as a string (e.g. '45.7'). Filled only on the 25 rows of Fox's ATT/G table; null on the rows of the other two top-25 tables stacked into the default passing frame. |

**Example**

```python
from sportsdataverse.cfb import fox_cfb_league_leaders
df = fox_cfb_league_leaders("passing")
```

### fox_cfb_league_odds {#fox_cfb_league_odds}

`fox_cfb_league_odds(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports cfb league odds board (one row per team per game).

Wraps `cfb/league/odds`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `section` | character | Title of the board section holding the game module; always 'GAMES' in sampled data. |
| `game_id` | character | ESPN game identifier. |
| `event_time` | character | Scheduled start of the game, an ISO-8601 UTC string with a trailing Z (e.g. '2026-09-19T16:00:00Z'). |
| `event_status` | integer | Fox's numeric event-status code for the game; always 2 in the sampled board, where no listed game had started. |
| `team` | character | Team name. |
| `spread` | character | Pre-game point spread from the selected provider. |
| `to_win` | character | The team's moneyline in American odds, kept as a signed string (e.g. '-1818', '+923'); a bare '-' when no price is posted. |
| `total` | character | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |

**Example**

```python
from sportsdataverse.cfb import fox_cfb_league_odds
df = fox_cfb_league_odds()
```

### fox_cfb_league_player_news {#fox_cfb_league_player_news}

`fox_cfb_league_player_news(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports cfb league-wide player news feed.

Wraps `cfb/league/playernews`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `title` | character | Specific role title for the assignment. |
| `subtitle` | character | Team abbreviation, jersey number and position of the player, formatted like 'LSU #10 - QB'. |
| `headline` | character | Headline ESPN attaches to the poll release. |
| `description` | character | ESPN's description of the stat. |
| `impact_title` | character | Heading Fox shows above the impact paragraph; always 'Impact' in sampled data. |
| `impact` | character | Fox's analysis paragraph under the 'Impact' heading, explaining what the news means for the player (e.g. 'Leavitt was listed as doubtful in LSU's initial injury report...'). |
| `date` | character | Date of the poll release. |
| `source` | character | News source. |
| `athlete_id` | character | ESPN athlete id. |
| `content_uri` | character | Fox Bifrost content path of the athlete the item is about (e.g. 'football/cfb/athletes/212300'); athlete_id is its trailing number. |
| `web_url` | character | Site-relative foxsports.com path of the player's page (e.g. '/college-football/sam-leavitt-player'). |

**Example**

```python
from sportsdataverse.cfb import fox_cfb_league_player_news
df = fox_cfb_league_player_news()
```

### fox_cfb_league_polls {#fox_cfb_league_polls}

`fox_cfb_league_polls(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports cfb rankings / polls rendered as standings tables.

Wraps `cfb/league/polls`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `section` | character | Poll the row belongs to: 'ASSOCIATED PRESS' or 'USA TODAY COACHES POLL'. |
| `ranking` | character | National rank of the team's overall SP+ rating (1 = best). |
| `v1` | character | Places the team moved since the previous poll, as an unsigned string (e.g. '3'); null when Fox shows no movement. The parser drops Fox's up/down flag, so a rise and a fall look the same. |
| `v2` | character | Team short name as Fox prints it in the poll, with first-place votes in parentheses when it got any (e.g. 'Texas (56)', 'Ohio State', 'Miami (FL) (4)'). |
| `pts` | character | Points scored. |
| `entity_id` | character | Fox id of the row's linked team as a string: the trailing number of the row's entityLink contentUri. |

**Example**

```python
from sportsdataverse.cfb import fox_cfb_league_polls
df = fox_cfb_league_polls()
```

### fox_cfb_league_schedule {#fox_cfb_league_schedule}

`fox_cfb_league_schedule(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports cfb league schedule nav selections.

Wraps `cfb/league/schedule`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | character | Which Fox navigation list the row came from: 'groupList' (group filters such as FEATURED, TOP 25, ACC) or 'selectionList' (the season's week segments); the parser also emits 'dailyList', which CFB samples never show. |
| `id` | character | 247Sports referencing id for the recruit. |
| `title` | character | Specific role title for the assignment. |
| `date` | character | Date of the poll release. |
| `uri` | character | Absolute Bifrost API URL of the week segment (e.g. 'https://api.foxsports.com/bifrost/v1/cfb/league/schedule-segment/2026-1-1?groupId=-4'); null on every groupList row. |
| `web_url` | character | Site-relative foxsports.com path for the selection, e.g. '/college-football/schedule?groupId=9' on a group row. |
| `selected` | logical | Fox's default-selection flag: True on the one group filter (a groupList row) Fox pre-selects, and null (never False) on every other row. |
| `group_id` | character | ESPN group (conference) id for the season. |

**Example**

```python
from sportsdataverse.cfb import fox_cfb_league_schedule
df = fox_cfb_league_schedule()
```

### fox_cfb_league_scores {#fox_cfb_league_scores}

`fox_cfb_league_scores(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports cfb league scores nav selections.

Wraps `cfb/league/scores`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | character | Which Fox navigation list the row came from: 'groupList' (group filters such as FEATURED, TOP 25, ACC) or 'selectionList' (the season's week segments); the parser also emits 'dailyList', which CFB samples never show. |
| `id` | character | 247Sports referencing id for the recruit. |
| `title` | character | Specific role title for the assignment. |
| `date` | character | Date of the poll release. |
| `uri` | character | Absolute Bifrost API URL of the week segment (e.g. 'https://api.foxsports.com/bifrost/v1/cfb/league/scores-segment/2026-1-1?groupId=-4'); null on every groupList row. |
| `web_url` | character | Site-relative foxsports.com path for the selection, e.g. '/college-football/scores?groupId=9' on a group row. |
| `selected` | logical | Fox's default-selection flag: True on the one group filter (a groupList row) Fox pre-selects, and null (never False) on every other row. |
| `group_id` | character | ESPN group (conference) id for the season. |

**Example**

```python
from sportsdataverse.cfb import fox_cfb_league_scores
df = fox_cfb_league_scores()
```

### fox_cfb_league_standings {#fox_cfb_league_standings}

`fox_cfb_league_standings(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports cfb league-wide standings tables.

Wraps `cfb/league/standings`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_league_standings
df = fox_cfb_league_standings()
```

### fox_cfb_league_stat_leaders {#fox_cfb_league_stat_leaders}

`fox_cfb_league_stat_leaders(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports cfb league stats landing leaders.

Wraps `cfb/league/stats`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `stat` | character | Stat. |
| `stat_abbreviation` | character | Fox's short code for the leader's stat (e.g. 'PYDS', 'PTD', 'RECYDS'); some codes contain a space, such as 'KR YDS'. |
| `player` | character | Player name. |
| `value` | character | Metric value. |

**Example**

```python
from sportsdataverse.cfb import fox_cfb_league_stat_leaders
df = fox_cfb_league_stat_leaders()
```

### fox_cfb_odds {#fox_cfb_odds}

`fox_cfb_odds(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

Fox Sports CFB game odds six-pack (spread / to win / total per team).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/event/{game_id}/odds`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Fox Bifrost event id (e.g. `"41616"`). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the six-pack market to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default; empty when no market is posted), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_odds
df = fox_cfb_odds("41616")
```

### fox_cfb_pbp {#fox_cfb_pbp}

`fox_cfb_pbp(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

Fox Sports CFB play-by-play (one row per play).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/event/{game_id}/data`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Fox Bifrost event id (e.g. `"41616"`) -- not the ESPN id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the pbp layout to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_pbp
df = fox_cfb_pbp("41616")
```

### fox_cfb_play_process {#fox_cfb_play_process}

`fox_cfb_play_process(event_id, odds_override: 'Optional[Dict[str, Any]]' = None, process: 'bool' = True, raw: 'bool' = False, **kwargs) -> 'Dict[str, Any]'`

Build a *processed* CFB play-by-play game from FoxSports as a backup to ESPN.

Where `~sportsdataverse.cfb.cfb_fox_ext.fox_cfb_pbp` returns the raw Fox
play-by-play rows, this runs Fox data through the full ESPN play processor:
it fetches FoxSports Bifrost `cfb/event/{event_id}/data`, adapts it into the
ESPN-`summary` shape via `fox_to_espn_summary`, and runs the same
`~sportsdataverse.cfb.cfb_pbp.CFBPlayProcess` pipeline ESPN games use
-- producing EPA / WPA / advanced box score. The result carries
`source="fox"` so downstream consumers know the provenance (and that
text-derived columns are lower fidelity than the ESPN path).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event_id` |  |  | FoxSports CFB event id (e.g. `41616`). |
| `odds_override` | `Optional[Dict[str, Any]]` | `None` | Optional `{gameSpread, overUnder, homeFavorite, gameSpreadAvailable}` dict. Fox does not expose a clean pre-game spread, so when omitted a neutral pick'em line is used (EPA is unaffected; only the WP model's spread term is neutralized). |
| `process` | `bool` | `True` | If `True` (default) run the full `~sportsdataverse.cfb.cfb_pbp.CFBPlayProcess.run_processing_pipeline` (EPA/WPA/box). If `False` run the lighter `~sportsdataverse.cfb.cfb_pbp.CFBPlayProcess.run_cleaning_pipeline`. |
| `raw` | `bool` | `False` | If `True` skip the processor entirely and return the adapted ESPN-summary dict (the input the processor would consume). |

**Returns**

The processed game payload (same keys as `CFBPlayProcess.run_processing_pipeline`) with an added `source="fox"` key. When `raw=True`, the adapted summary dict.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_play_process
game = fox_cfb_play_process(41616)
print(len(game["plays"]), game["source"])
```

### fox_cfb_schedule {#fox_cfb_schedule}

`fox_cfb_schedule(season: 'Optional[int]' = None, *, segment_id: 'Optional[str]' = None, group_id: 'Union[int, str]' = '2', return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

Fox Sports CFB full-season schedule (one row per game).

Fox lists games behind a two-step *selector -> segment* flow: `scoreboard/main`
enumerates the season's segments (its `selectionGroupList`), and
`league/scores-segment/{segmentId}` returns the games for one segment.
Pass a `season` to scrape the **whole season** -- every regular week plus
conference championships, bowls, and every College Football Playoff round --
enumerated from the live selector and unioned, deduplicated by `game_id`.

Segment ids encode the phase, not an ESPN-style integer week:
`"{season}-{week}-1"` for a regular-season week, `"{season}-bowls-2"` for
the bowls, `"{season}-cfp-2"` for the CFP (conference championships fall in
the final regular-season week). Pass `segment_id` to fetch just one of them.

The numeric `game_id` is the Fox Bifrost event id that `fox_cfb_pbp` /
`fox_cfb_odds` accept; `week_label` is the section title.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year -> scrape the full season. Ignored when `segment_id` is given; if both are `None` the current segment is returned. |
| `segment_id` | `Optional[str]` | `None` | Explicit Fox segment id (e.g. `"2025-5-1"`, `"2025-cfp-2"`) -> fetch just that segment. |
| `group_id` | `Union[int, str]` | `'2'` | Conference/division group filter. Defaults to `"2"` (FBS). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to a DataFrame; if `False` return the raw JSON (a single segment's `dict`, or a `{segment_id: dict}` map in full-season mode). |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default) with columns `game_id`, `date`, `status`, `week_label`, `home_team`, `home_team_id`, `away_team`, `away_team_id`, `segment_id`; a pandas DataFrame when `return_as_pandas=True`; or raw JSON when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `game_id` | character | ESPN game identifier. |
| `date` | character | Date of the poll release. |
| `status` | character | Game status (e.g. "scheduled", "in_progress", "completed"). |
| `week_label` | character | Title of the Fox segment section that listed the game (e.g. 'WEEK 3'); often null because Fox leaves most section titles blank. |
| `home_team` | character | Home team name. |
| `home_team_id` | character | ESPN home team id (parsed from `home_team_ref`). |
| `away_team` | character | Away team name. |
| `away_team_id` | character | ESPN away team id (parsed from `away_team_ref`). |
| `segment_id` | character | Fox scoreboard segment the game was fetched from: '{season}-{week}-1' for a regular-season week (sampled '2026-3-1'), '{season}-bowls-2' or '{season}-cfp-2' for the postseason. |

**Example**

```python
from sportsdataverse.cfb import fox_cfb_schedule
season = fox_cfb_schedule(2025)

# Fetch just one segment (a week, or the playoff)

wk5 = fox_cfb_schedule(segment_id="2025-5-1")
cfp = fox_cfb_schedule(segment_id="2025-cfp-2")
```

### fox_cfb_scoreboard {#fox_cfb_scoreboard}

`fox_cfb_scoreboard(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports cfb scoreboard nav selections (weeks / dates / groups).

Wraps `cfb/scoreboard/main`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `selection_list` | character | Which Fox navigation list the row came from: 'groupList' (group filters such as FEATURED, TOP 25, ACC) or 'selectionList' (the season's week segments); the parser also emits 'dailyList', which CFB samples never show. |
| `id` | character | 247Sports referencing id for the recruit. |
| `title` | character | Specific role title for the assignment. |
| `date` | character | Date of the poll release. |
| `uri` | character | Absolute Bifrost API URL of the week segment (e.g. 'https://api.foxsports.com/bifrost/v1/cfb/scoreboard/segment/2026-1-1?groupId=-4'); null on every groupList row. |
| `web_url` | character | Site-relative foxsports.com path for the selection, e.g. '/scores/college-football?groupId=9' on a group row; week rows add seasonType and week parameters ('...?groupId=-4&seasonType=reg&week=1'). |
| `selected` | logical | Fox's default-selection flag: True on the one group filter (a groupList row) Fox pre-selects, and null (never False) on every other row. |
| `group_id` | character | ESPN group (conference) id for the season. |

**Example**

```python
from sportsdataverse.cfb import fox_cfb_scoreboard
df = fox_cfb_scoreboard()
```

### fox_cfb_scorechip {#fox_cfb_scorechip}

`fox_cfb_scorechip(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports cfb compact live score chip (raw dict -- live-only, uncaptured shape).

Wraps `cfb/scorechip/{chip_id}`.

**Returns**

The raw JSON `dict`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_scorechip
df = fox_cfb_scorechip("nfl12345")
```

### fox_cfb_scores_segment {#fox_cfb_scores_segment}

`fox_cfb_scores_segment(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports cfb one row per game in a scoreboard segment.

Wraps `cfb/league/scores-segment/{segment_id}`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_scores_segment
df = fox_cfb_scores_segment("...")
```

### fox_cfb_standings {#fox_cfb_standings}

`fox_cfb_standings(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

Fox Sports CFB conference standings for a team's conference.

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/team/{team_id}/standings`
(the league-wide `league/standings` endpoint returns header-only tables, so
standings are keyed by team).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id (e.g. `"11"` = Miami (FL)). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the standings tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_standings
df = fox_cfb_standings("11")
```

### fox_cfb_team_gamelog {#fox_cfb_team_gamelog}

`fox_cfb_team_gamelog(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

Fox Sports CFB team game log -- tidy long: one row per (game, stat).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/team/{team_id}/gamelog`
The endpoint groups team per-game stats by category (passing, rushing,
defense, ...) and season-type split; this flattens to columns
`team_id, season_type, category, game_id, game_date, opponent, stat, value`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id (e.g. `"11"` = Miami (FL)). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to long form; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_team_gamelog
df = fox_cfb_team_gamelog("11")
```

### fox_cfb_team_header {#fox_cfb_team_header}

`fox_cfb_team_header(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports cfb team header (one row).

Wraps `cfb/team/{team_id}/header`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_team_header
df = fox_cfb_team_header("...")
```

### fox_cfb_team_roster {#fox_cfb_team_roster}

`fox_cfb_team_roster(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

Fox Sports CFB team roster (one row per player).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/team/{team_id}/roster`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id (e.g. `"11"` = Miami (FL)); discover via the league team directory (`cfb/league/teamnav`). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the position-group tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_team_roster
df = fox_cfb_team_roster("11")
```

### fox_cfb_team_stats {#fox_cfb_team_stats}

`fox_cfb_team_stats(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

Fox Sports CFB team stat leaders (one row per category leader).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/team/{team_id}/stats`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id (e.g. `"11"` = Miami (FL)). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the leader sections to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_team_stats
df = fox_cfb_team_stats("11")
```

### fox_cfb_teamnav {#fox_cfb_teamnav}

`fox_cfb_teamnav(*args: 'Any', **kwargs: 'Any') -> 'Any'`

Fox Sports cfb team directory (one row per team).

Wraps `cfb/league/teamnav`.

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `group` | character | Stat group (e.g. "hitting", "pitching", "fielding"). |
| `fox_id` | character | Fox Bifrost team id as a string, the trailing number of content_uri; the same id fox_cfb_teams returns as fox_team_id from this endpoint. |
| `abbreviation` | character | Metric abbreviation. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `content_uri` | character | Fox Bifrost content path of the team (e.g. 'football/cfb/teams/25'). |
| `content_type` | character | Fox entity type of the linked item; 'team' on every sampled row. |
| `web_url` | character | Site-relative foxsports.com team page path (e.g. '/college-football/ohio-state-buckeyes-team'); null for some teams. |
| `color` | character | Primary team color (hex, no `#`). |
| `logo_url` | character | NBA CDN primary logo URL. |

**Example**

```python
from sportsdataverse.cfb import fox_cfb_teamnav
df = fox_cfb_teamnav()
```

### fox_cfb_teams {#fox_cfb_teams}

`fox_cfb_teams(*, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"`

Fox Sports CFB team directory (one row per team).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/league/teamnav`

The team-nav payload is the canonical Fox directory: it maps every team's
Bifrost id to its abbreviation, full name, and web slug. This is the lookup
you need to translate a human team name into the numeric `team_id` the
other `fox_cfb_*` wrappers expect, and it is the Fox side of
`sportsdataverse.cfb.cfb_teams_crosswalk`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the nav items to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default) with columns `fox_team_id`, `abbreviation`, `name`, `slug`, `color`, `logo_url`; a pandas DataFrame when `return_as_pandas=True`; or the raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `fox_team_id` | character | Fox Bifrost team id (NA if unmatched). |
| `abbreviation` | character | Metric abbreviation. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `slug` | character | URL slug for the team. |
| `color` | character | Primary team color (hex, no `#`). |
| `logo_url` | character | NBA CDN primary logo URL. |

**Example**

```python
from sportsdataverse.cfb import fox_cfb_teams
teams = fox_cfb_teams()
fox_id = dict(zip(teams["abbreviation"], teams["fox_team_id"]))
```

### fox_to_espn_summary {#fox_to_espn_summary}

`fox_to_espn_summary(fox_data: 'Dict[str, Any]') -> 'Dict[str, Any]'`

Adapt a Fox `cfb/event/{id}/data` payload into the ESPN-summary shape.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fox_data` | `Dict[str, Any]` |  | Parsed JSON from `api.foxsports.com/bifrost/v1/cfb/event/{id}/data`. |

**Returns**

A dict shaped like ESPN's `college-football/summary` response (`header` + `drives` + stub `pickcenter`/`boxscore`/...), ready to assign onto `CFBPlayProcess(...).json`.
