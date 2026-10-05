---
title: "MBB — additional Python functions — Get"
sidebar_label: "Get"
sidebar_position: 5
description: "MBB — additional Python functions — Get — function reference in sdv-py, the SportsDataverse Python package."
---
# MBB — additional Python functions — Get

### get_ascending_time {#get_ascending_time}

`get_ascending_time(event: 'ShotEvent', period: 'int', is_women_game: 'bool') -> 'float'`

Converts the descending in-period clock time to an ascending

game-elapsed time (`ShotEventParser.get_ascending_time`, `:531-537`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` | `ShotEvent` |  | The shot event (only `~sportsdataverse.mbb .mbb_ncaa_models.ShotEvent.min`, the raw descending clock minute, is read). |
| `period` | `int` |  | The 1-indexed period the shot was taken in. |
| `is_women_game` | `bool` |  | Whether to use women's-quarters (10min) or men's- halves (20min, then 5min OTs) period lengths. |

**Returns**

The ascending game-elapsed time, in minutes.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shot_parser import get_ascending_time
get_ascending_time(shot_with_min_4, period=1, is_women_game=False)  # 16.0
```

### get_box_lineup {#get_box_lineup}

`get_box_lineup(filename: 'str', in_html: 'str', team_id: 'TeamId', format_version: 'int', external_roster: 'tuple[list[str], list[RosterEntry]]' = ([], []), neutral_game_dates: 'AbstractSet[str]' = frozenset(), home_team: 'Optional[str]' = None, away_team: 'Optional[str]' = None) -> 'Union[LineupEvent, list[ParseError]]'`

Gets the boxscore lineup from the HTML page (``BoxscoreParser

.get_box_lineup`, `:122-222``).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name -- used both for error reporting and to extract the period via `parse_period_from_filename` (e.g. `"test_p2.html"`). |
| `in_html` | `str` |  | The raw box-score-page HTML. |
| `team_id` | `TeamId` |  | The team this box score is being parsed for. |
| `format_version` | `int` |  | `0` for the legacy layout, `1` for the 2018+ layout (see the module docstring's selector-translation notes). |
| `external_roster` | `tuple[list[str], list[RosterEntry]]` | `([], [])` | `(other_players, roster_players)` -- either just names, or a full roster, to validate/fuzzy-correct box names against (see `inject_validated_players`). Also seeds `~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent.players_out` on the interim lineup (`roster_players`, each's `code` replaced by its jersey `number`). |
| `neutral_game_dates` | `AbstractSet[str]` | `frozenset()` | Date strings (the first whitespace-separated token of the raw date-cell text) known to be neutral-site games -- overrides the default home/away inference. |
| `home_team` | `Optional[str]` | `None` | The game's home team when the caller already knows it, forwarded to team-name resolution so a box page that names only one side (a non-D-I opponent has no header) still resolves. |
| `away_team` | `Optional[str]` | `None` | The game's away team, same purpose. Both are required together or neither is used. |

**Returns**

A `~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent` whose `players` is the validated box-score lineup (natural HTML order -- see the module docstring's "not sorted" note), or a `list[ParseError]` if any parsing step failed.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
from sportsdataverse.mbb.mbb_ncaa_models import TeamId

with open("tests/fixtures/ncaa/test_lineup.html", encoding="utf-8") as f:
    html = f.read()
result = get_box_lineup("test_p1.html", html, TeamId("TeamA"), format_version=0)
```

### get_config {#get_config}

`get_config() -> 'NcaaFetchConfig'`

Return the live `NcaaFetchConfig` singleton.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_fetch import get_config
cfg = get_config()
print(cfg.cache_dir, cfg.timeout)
```

### get_constants {#get_constants}

`get_constants(league: 'str') -> 'LeagueConstants'`

Return the `LeagueConstants` for a league.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str` |  | Either `"mens"` or `"womens"`. |

**Returns**

The league's `LeagueConstants`.

**Example**

```python
from sportsdataverse.mbb.mbb_prediction_constants import get_constants
get_constants("mens").hfa
```

### get_game_weight {#get_game_weight}

`get_game_weight(opp: 'OpponentGame', field: 'str', side: 'str') -> 'float'`

Weight for one game/field/side (`getGameWeight`, `ts:119-140`).

The field-specific shot volume (FGA for `efg`, 3PA for `3p`,
`2pmid_attempts` / `2prim_attempts` for the mid/rim fields); when that
is `0` (no shots of that type), **falls back to** `off_poss` /
`def_poss` so the game still carries weight.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `opp` | `OpponentGame` |  | One opponent game dict. |
| `field` | `str` |  | A stat field. |
| `side` | `str` |  | `"off"` or `"def"`. |

**Returns**

The (non-negative) game weight.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import get_game_weight

game = {"off_3p_attempts": 0, "off_poss": 70}
print(get_game_weight(game, "3p", "off"))  # 70.0 (poss fallback)
```

### get_neutral_games {#get_neutral_games}

`get_neutral_games(filename: 'str', in_html: 'str', format_version: 'int') -> 'Union[tuple[TeamId, set[str]], list[ParseError]]'`

Extracts the set of neutral/away-marked game dates from a saved NCAA

team-schedule page (`TeamScheduleParser.get_neutral_games`,
`TeamScheduleParser.scala:63-94`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name, used only for error reporting. |
| `in_html` | `str` |  | The raw team-schedule-page HTML. |
| `format_version` | `int` |  | `0` for the legacy `fieldset`/`legend` layout, `1` for the 2018+ `div.card-header`/`div.card-body` layout. |

**Returns**

`(team, neutral_game_dates)` -- the team parsed from the page's image `alt` attribute, and every `"MM/DD/YYYY"` date string found on an `"@Opponent"`-marked row -- or a single-element `list[ParseError]` if the HTML fails to parse, or the team name can't be located.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_team_parsers import get_neutral_games

with open("tests/fixtures/ncaa/test_schedule.html", encoding="utf-8") as f:
    html = f.read()
result = get_neutral_games("test_schedule.html", html, format_version=0)
if isinstance(result, list):
    raise RuntimeError(result)  # list[ParseError]
team, neutral_dates = result
```

### get_per_game_raw {#get_per_game_raw}

`get_per_game_raw(opp: 'OpponentGame', field: 'str', side: 'str') -> 'Optional[float]'`

Per-game raw shooting rate from one opponent row (`getPerGameRaw`, `ts:82-116`).

`efg` is `(2pmid_made + 2prim_made + 1.5 * 3p_made) / (2pmid_att +
2prim_att + 3p_att)`; `3p` / `2pmid` / `2prim` are `made /
attempts`. Every counter read is nullish (missing -> 0); the sole guard
is on total attempts.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `opp` | `OpponentGame` |  | One opponent game dict. |
| `field` | `str` |  | A stat field; an unknown field returns `None`. |
| `side` | `str` |  | `"off"` or `"def"` (selects the `off_`/`def_` prefix). |

**Returns**

The rate as a float, or `None` when the relevant attempts total is `<= 0` (game skipped by the weighted means -- **not** a 0-rate).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import get_per_game_raw

game = {"off_3p_made": 4, "off_3p_attempts": 10}
print(get_per_game_raw(game, "3p", "off"))  # 0.4
```

### get_player_value_constants {#get_player_value_constants}

`get_player_value_constants(league: 'str') -> 'PlayerValueConstants'`

Return the `PlayerValueConstants` for a league.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str` |  | `"mens"` or `"womens"`. |

**Returns**

The league's `PlayerValueConstants`.

**Example**

```python
from sportsdataverse.mbb.mbb_player_value_constants import get_player_value_constants
get_player_value_constants("mens").bundle_prefix
```

### get_sorted_pbp_events {#get_sorted_pbp_events}

`get_sorted_pbp_events(filename: 'str', in_html: 'str', box_lineup: 'LineupEvent', format_version: 'int') -> 'Union[list[PlayByPlayEvent], list[ParseError]]'`

Handy util to return the play-by-play events in chronological order,

used in a few other places (`PlayByPlayParser.get_sorted_pbp_events`,
`:221-239`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name, used only for error reporting. |
| `in_html` | `str` |  | The raw play-by-play-page HTML. |
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup (supplies `team`/`year`). |
| `format_version` | `int` |  | `0` for the legacy layout, `1` for the 2018+ layout. |

**Returns**

The play-by-play events in chronological (earliest-to-latest) order, or a `list[ParseError]` on failure. `enrich=True` is used internally to get the correct ascending timestamps, and its reversal is undone here (`.reverse`) to restore chronological order.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
from sportsdataverse.mbb.mbb_ncaa_models import TeamId
from sportsdataverse.mbb.mbb_ncaa_pbp_parser import get_sorted_pbp_events

with open("tests/fixtures/ncaa/test_lineup.html", encoding="utf-8") as f:
    box_html = f.read()
box_lineup = get_box_lineup("test_p1.html", box_html, TeamId("TeamA"), format_version=0)

with open("tests/fixtures/ncaa/test_play_by_play.html", encoding="utf-8") as f:
    pbp_html = f.read()
events = get_sorted_pbp_events("test.html", pbp_html, box_lineup, format_version=0)
```

### get_stats_diff {#get_stats_diff}

`get_stats_diff(stat_set1: 'LineupStatSet', stat_set2: 'LineupStatSet', off_title: 'str', def_title: 'str | None' = None) -> 'LineupStatSet'`

Straight (unweighted) field-by-field diff of two team stat sets.

Faithful port of `LineupUtils.getStatsDiff` (`LineupUtils.ts:185`).
For every field on `stat_set1`, subtracts the matching field's
`value` (and, when both sides carry one, `old_value`) from
`stat_set2`. No possession weighting or regression -- this is a raw
subtraction, unlike `weighted_avg` / `complete_weighted_avg`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stat_set1` | `LineupStatSet` |  | The "from" team stat set (e.g. this team). |
| `stat_set2` | `LineupStatSet` |  | The "to subtract" team stat set (e.g. the opponent, or a prior period). |
| `off_title` | `str` |  | Written into the result's `off_title` field verbatim. |
| `def_title` | `str \| None` | `None` | Written into the result's `def_title` field verbatim (`None` when omitted, mirroring the upstream optional arg). |

**Returns**

A new `LineupStatSet`: one `{"value": ..., "old_value": ..., "override": ...}` dict per field present on `stat_set1`, plus `off_title` / `def_title`. A field becomes `None` (the JS `undefined` analog) instead of a diff dict when either side is missing a `value` -- e.g. because that field was never populated for one of the two stat sets.

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import get_stats_diff

diff = get_stats_diff(team_a, team_b, "Team A", "Team B")
print(diff["off_ppp"]["value"])  # team_a.off_ppp - team_b.off_ppp
```

### get_team_raw_from_per_game {#get_team_raw_from_per_game}

`get_team_raw_from_per_game(team: 'TeamDetail', field: 'str') -> 'SideValues'`

A team's field rate as the weighted mean of its per-game raws (`getTeamRawFromPerGame`, `ts:224-250`).

Same accumulation as `compute_league_averages_from_per_game` but
scoped to one team's games; empty -> `0`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team` | `TeamDetail` |  | A `team_details` team dict. |
| `field` | `str` |  | A stat field. |

**Returns**

`{"off": float, "def": float}`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import get_team_raw_from_per_game

team = {"opponents": [{"off_3p_made": 4, "off_3p_attempts": 10}]}
print(get_team_raw_from_per_game(team, "3p")["off"])  # 0.4
```

### get_team_triples {#get_team_triples}

`get_team_triples(filename: 'str', in_html: 'str', old_format: 'bool' = False) -> 'Union[list[tuple[TeamId, str, ConferenceId]], list[ParseError]]'`

Extracts `(team, NCAA id, conference)` triples from a saved NCAA

team-list/attendance page (`TeamIdParser.get_team_triples`,
`TeamIdParser.scala:69-91`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name, used only for error reporting. |
| `in_html` | `str` |  | The raw team-list-page HTML. |
| `old_format` | `bool` | `False` | `True` for pages where the team name and conference are in separate `<td>`s; `False` (default) for pages where the conference is embedded in the team-name cell as `"Team (Conf)"`. |

**Returns**

One `(TeamId, ncaa_id, ConferenceId)` triple per row that has both a resolvable id and name/conference (rows missing either are silently skipped, matching the Scala's `case _ => Nil`), or a single-element `list[ParseError]` if the HTML itself fails to parse.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_team_parsers import get_team_triples

with open("tests/fixtures/ncaa/test_attendance_list.html", encoding="utf-8") as f:
    html = f.read()
result = get_team_triples("test_attendance_list.html", html, old_format=True)
```

### get_unified_ncaa_id {#get_unified_ncaa_id}

`get_unified_ncaa_id(filename: 'str', in_html: 'str') -> 'Union[Optional[str], list[ParseError]]'`

Gets a player's lowest cross-season NCAA id from a saved player page

(`RosterParser.get_unified_ncaa_id`, `RosterParser.scala:136-152`).

Always uses the v1 selector table -- this bonus lookup only exists on
2018+-era pages.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name, used only for error reporting. |
| `in_html` | `str` |  | The raw player-page HTML. |

**Returns**

The numerically-lowest NCAA id found, `None` if the page has no `tr[id^=player_season_]` rows, or a single-element `list[ParseError]` if the HTML couldn't be parsed at all.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_roster_parser import get_unified_ncaa_id
get_unified_ncaa_id("player.html", player_page_html)
```
