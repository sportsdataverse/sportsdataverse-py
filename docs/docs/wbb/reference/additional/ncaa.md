---
title: "WBB — additional Python functions — NCAA (stats.ncaa.org)"
sidebar_label: "NCAA (stats.ncaa.org)"
sidebar_position: 6
description: "WBB — additional Python functions — NCAA (stats.ncaa.org) — function reference in sdv-py, the SportsDataverse Python package."
---
# WBB — additional Python functions — NCAA (stats.ncaa.org)

### ncaa_espn_team_crosswalk {#ncaa_espn_team_crosswalk}

`ncaa_espn_team_crosswalk(league: 'str' = 'mbb', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Season-keyed stats.ncaa.org -> ESPN team-id crosswalk.

One row per `(season, ncaa_team_id)`. Teams that could not be resolved to
an ESPN team are kept with a null `espn_team_id` and
`match_method="unmatched"` -- never dropped -- so the row count always
equals `ncaa_{league}_team_ids()`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str` | `'mbb'` | `"mbb"` (men's, 2009-10 onward) or `"wbb"` (women's). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

DataFrame with columns `season` (str, `"YYYY-YY"`), `ncaa_team_id` (Int64 -- the season-specific stats.ncaa.org id), `ncaa_team` / `ncaa_conference` (str), `conference_id` (str, nullable -- the SDV group id, e.g. `"mbb:big-ten"`), `espn_team_id` (str, nullable -- ESPN ids are strings throughout sdv-py), `espn_display_name` / `espn_location` / `espn_mascot` / `espn_abbreviation` / `espn_conference_name` / `espn_conference_id` (str, nullable), and `match_method` (str -- `"exact"`, `"dict"`, `"alias"` or `"unmatched"`). The three conference columns and `ncaa_conference` are per season: Maryland is ACC in 2013-14 and Big Ten in 2014-15.

| col_name | type | description |
|---|---|---|
| `season` | character | Season identifier (4-digit year or 'YYYY-YY' string). |
| `ncaa_team_id` | integer | stats.ncaa.org team id (Int64) for that season; stats.ncaa.org issues a new id every season, so the same school has a different id on each season row. |
| `ncaa_team` | character | School name as stats.ncaa.org writes it, in AP-style abbreviations (e.g. 'Alabama St.', 'A&M-Corpus Christi'). |
| `ncaa_conference` | character | stats.ncaa.org label of the team's conference that season (e.g. 'SEC', 'MWC'), taken from the groups tables' NCAA aliases for the season's conference_id. A conference with no NCAA alias gets its SDV abbreviation (men's Great West -> 'GWC'); a team the groups table has no row for keeps the bundled stats.ncaa.org team-list label. The label style can differ by league ('MWC' in men's, 'Mountain West' in women's through 2022-23). |
| `espn_team_id` | character | ESPN team id (canonical key). |
| `espn_display_name` | character | ESPN display name (school + mascot). |
| `espn_location` | character | ESPN school/location only. |
| `espn_mascot` | character | ESPN team mascot/nickname. |
| `espn_abbreviation` | character | ESPN abbreviation. |
| `espn_conference_name` | character | Conference name for that season as the {mbb,wbb}_group_seasons table records it (e.g. 'Colonial Athletic Association' through 2022-23, 'Coastal Athletic Association' after). Null on the same rows as conference_id. |
| `espn_conference_id` | character | ESPN conference (group) id for that season as a string (e.g. '23' for the Southeastern Conference). ESPN group ids are sport-scoped (Summit League is 49 in men's, 47 in women's) and can change (men's Summit League was 15 before 2008). Null on the same rows as conference_id. |
| `match_method` | character | Combination of matched sources, e.g. "fox+bart" / "fox_only" / "bart_only" / "espn_only". |
| `conference_id` | character | SportsDataverse conference (group) id for that season, prefixed by the league (e.g. 'mbb:big-ten'), from the {mbb,wbb}_team_group_seasons release table joined on espn_team_id and the season's ending year. Stable across renames and shared with the {mbb,wbb}_groups tables; null only when that table has no row for the team that season. |

**Example**

```python
from sportsdataverse.mbb import ncaa_espn_team_crosswalk
df = ncaa_espn_team_crosswalk()
print(df.shape)

# Women's crosswalk as pandas

wdf = ncaa_espn_team_crosswalk(league="wbb", return_as_pandas=True)

# Pipeline next step (one line)

df.filter(pl.col("season") == "2025-26").select("ncaa_team_id", "espn_team_id")
```

### ncaa_wbb_box_scores {#ncaa_wbb_box_scores}

`ncaa_wbb_box_scores(game_ids: 'Union[str, int, Iterable[Union[str, int]]]', *, multi_games: 'bool' = False, fetcher: 'Optional[Any]' = None, return_as_pandas: 'bool' = False) -> "Union['pl.DataFrame', 'pd.DataFrame']"`

Scrape WBB per-player box scores (wbigballR `get_box_scores`/`scrape_box`).

Pure delegation to
`sportsdataverse.mbb.mbb_ncaa_box_stats.ncaa_mbb_box_scores` — see
it for the column contract, the tolerant header renames, and the fixed
`multi_games` aggregation (R's groups by a `Pos` column the current
markup no longer ships).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_ids` | `Union[str, int, Iterable[Union[str, int]]]` |  | NCAA contest ids; `None`/NaN entries are dropped. |
| `multi_games` | `bool` | `False` | Aggregate per player across all games (fixed grouping on player/clean_name/team). |
| `fetcher` | `Optional[Any]` | `None` | Optional injected fetcher exposing `fetch_game_individual_stats` (tests/offline). Defaults to a fresh `NcaaFetcher.with_browser()` context per call. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Per-player box rows (or per-player aggregates with `multi_games`).

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_box_stats import ncaa_wbb_box_scores
box = ncaa_wbb_box_scores(["5722355"])
print(box.shape)
```

### ncaa_wbb_date_games {#ncaa_wbb_date_games}

`ncaa_wbb_date_games(date: 'Optional[str]' = None, *, conference: 'str' = 'All', conference_id: 'Optional[int]' = None, fetcher: "Optional['NcaaFetcher']" = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Discover every NCAA WBB game played on a date (wbigballR `get_date_games`).

Same engine as
`sportsdataverse.mbb.mbb_ncaa_scoreboard.ncaa_mbb_date_games` with
the WBB `season_divisions` table bound (see the module docstring for
the 2010-11..2025-26 coverage caveat).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `Optional[str]` | `None` | `"MM/DD/YYYY"`. Defaults to yesterday (R default). |
| `conference` | `str` | `'All'` | Conference name filter (e.g. `"SEC"`, `"Summit League"`); case/punctuation-insensitive. Default `"All"` (every conference). Unknown names raise. |
| `conference_id` | `Optional[int]` | `None` | Explicit stats.ncaa.org conference id; overrides *conference* when given. |
| `fetcher` | `Optional['NcaaFetcher']` | `None` | Injectable `~sportsdataverse.mbb.mbb_ncaa_fetch. NcaaFetcher` (tests pass an offline fake). `None` uses `NcaaFetcher.with_browser()`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per game — see the MBB sibling for the full `SCOREBOARD_SCHEMA` column contract.

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_scoreboard import ncaa_wbb_date_games
games = ncaa_wbb_date_games("12/05/2024")
print(games.shape)
```

### ncaa_wbb_game_pbp {#ncaa_wbb_game_pbp}

`ncaa_wbb_game_pbp(game_id: 'object', *, fetcher: 'Optional[_SupportsFetchGamePbp]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Scrape one WBB game's play-by-play (wbigballR `scrape_game`, quarters fixed).

Same engine as `sportsdataverse.mbb.mbb_ncaa_game_pbp.ncaa_mbb_game_pbp`
with `period_model=(4, 600, 300)` bound (see the module docstring for why
this deliberately diverges from wbigballR's halves math).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `object` |  | NCAA contest id (e.g. `"5722355"`). |
| `fetcher` | `Optional[_SupportsFetchGamePbp]` | `None` | Optional injected fetcher exposing `fetch_game_pbp` (for tests/offline use). Defaults to a fresh `NcaaFetcher.with_browser()` context per call. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The 35-column play-by-play frame (zero rows when the game is not found).

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_game_pbp import ncaa_wbb_game_pbp
df = ncaa_wbb_game_pbp("5722355")
print(df.shape)
```

### ncaa_wbb_join_pbp_shots {#ncaa_wbb_join_pbp_shots}

`ncaa_wbb_join_pbp_shots(pbp: 'pl.DataFrame', shots: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Attach WBB chart shots onto the pbp frame (pure delegation).

See `sportsdataverse.mbb.mbb_ncaa_shots.ncaa_mbb_join_pbp_shots`
for the matching rules (FG-only, within-second same-result sequence) and
the joined 40-column contract.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | 35-column snake_case pbp frame (`ncaa_wbb_play_by_play`). |
| `shots` | `DataFrame` |  | Shots frame from `ncaa_wbb_shot_locations`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The pbp frame with shot columns attached (unmatched rows NA-filled).

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_shots import ncaa_wbb_join_pbp_shots
joined = ncaa_wbb_join_pbp_shots(pbp, shots)
print(joined.shape)
```

### ncaa_wbb_on_off {#ncaa_wbb_on_off}

`ncaa_wbb_on_off(players: 'Union[str, Sequence[str]]', lineups: 'pl.DataFrame', *, included: 'Union[str, Sequence[str], None]' = None, excluded: 'Union[str, Sequence[str], None]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Team stats for every on/off combination of the given WBB players.

Pure delegation to
`sportsdataverse.mbb.mbb_ncaa_lineups.ncaa_mbb_on_off`
(wbigballR `on_off_generator`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players` | `Union[str, Sequence[str]]` |  | Player name(s) to split on (the `status` axis). |
| `lineups` | `DataFrame` |  | Lineups frame from `ncaa_wbb_lineups`. |
| `included` | `Union[str, Sequence[str], None]` | `None` | Optional membership filter forwarded to `ncaa_wbb_player_lineups`. |
| `excluded` | `Union[str, Sequence[str], None]` | `None` | Optional membership filter forwarded to `ncaa_wbb_player_lineups`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`2^k` rows — `status` + the stat columns.

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_lineups import ncaa_wbb_on_off
onoff = ncaa_wbb_on_off("TE-HINA.PAOPAO", lineups)
print(onoff.shape)
```

### ncaa_wbb_play_by_play {#ncaa_wbb_play_by_play}

`ncaa_wbb_play_by_play(game_ids: 'Sequence[object]', *, fetcher: 'Optional[_SupportsFetchGamePbp]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Scrape many WBB games' play-by-play (wbigballR `get_play_by_play`, quarters fixed).

Same driver as `sportsdataverse.mbb.mbb_ncaa_game_pbp.ncaa_mbb_play_by_play`
(drop missing ids, shared fetcher session, one retry per empty scrape) with
the WBB quarter model `(4, 600, 300)` bound.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_ids` | `Sequence[object]` |  | NCAA contest ids; `None`/NaN entries are dropped. |
| `fetcher` | `Optional[_SupportsFetchGamePbp]` | `None` | Optional injected fetcher exposing `fetch_game_pbp`. Defaults to one shared `NcaaFetcher.with_browser()` context. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Row-bound play-by-play for every game that scraped successfully (zero-row contract frame when none did).

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_game_pbp import ncaa_wbb_play_by_play
df = ncaa_wbb_play_by_play(["5722355", "5732292"])
print(df.shape)
```

### ncaa_wbb_player_combos {#ncaa_wbb_player_combos}

`ncaa_wbb_player_combos(lineups: 'pl.DataFrame', *, n: 'int' = 2, min_mins: 'float' = 0, included: 'Union[str, Sequence[str], None]' = None, excluded: 'Union[str, Sequence[str], None]' = None, include_transition: 'bool' = False, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Team stats for every n-player WBB combination on the court together.

Pure delegation to
`sportsdataverse.mbb.mbb_ncaa_lineups.ncaa_mbb_player_combos`
(wbigballR `get_player_combos`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineups` | `DataFrame` |  | Lineups frame from `ncaa_wbb_lineups`. |
| `n` | `int` | `2` | Combination size, 1-5. |
| `min_mins` | `float` | `0` | Keep combos with total on-court minutes strictly greater than this. |
| `included` | `Union[str, Sequence[str], None]` | `None` | Player name(s) that must be on the court in every lineup. |
| `excluded` | `Union[str, Sequence[str], None]` | `None` | Player name(s) that must be off the court in every lineup. |
| `include_transition` | `bool` | `False` | Re-derive the trans`/half` ratio surface. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per combo: `team, p1..pn` + the stat surface.

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_lineups import ncaa_wbb_player_combos
combos = ncaa_wbb_player_combos(lineups, n=2)
print(combos.shape)
```

### ncaa_wbb_player_lineups {#ncaa_wbb_player_lineups}

`ncaa_wbb_player_lineups(lineups: 'pl.DataFrame', *, included: 'Union[str, Sequence[str], None]' = None, excluded: 'Union[str, Sequence[str], None]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Filter a WBB lineups frame by on-court player membership.

Pure delegation to
`sportsdataverse.mbb.mbb_ncaa_lineups.ncaa_mbb_player_lineups`
(wbigballR `get_player_lineups`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineups` | `DataFrame` |  | Lineups frame from `ncaa_wbb_lineups`. |
| `included` | `Union[str, Sequence[str], None]` | `None` | Player name(s) that must ALL be on the court. |
| `excluded` | `Union[str, Sequence[str], None]` | `None` | Player name(s) that must NONE be on the court. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Row-subset of `lineups`; schema unchanged.

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_lineups import ncaa_wbb_player_lineups
on = ncaa_wbb_player_lineups(lineups, included="TE-HINA.PAOPAO")
print(on.shape)
```

### ncaa_wbb_player_stats {#ncaa_wbb_player_stats}

`ncaa_wbb_player_stats(pbp: 'pl.DataFrame', *, multi_games: 'bool' = False, simple: 'bool' = False, fix_tip_in: 'bool' = True, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Aggregate WBB play-by-play into per-player box stats (wbigballR `get_player_stats`).

Pure delegation to
`sportsdataverse.mbb.mbb_ncaa_stats_agg.ncaa_mbb_player_stats` —
see it for the algorithm and column contracts.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame in the sdv-py 35-column snake_case bigballR contract (`ncaa_wbb_game_pbp` output). |
| `multi_games` | `bool` | `False` | Aggregate across games per (player, team) — the season-stat surface. |
| `simple` | `bool` | `False` | Return the reduced surface without the transition / assisted / putback / block-location splits. |
| `fix_tip_in` | `bool` | `True` | Count the real `"Tip In"` vocabulary (default); False reproduces R's `"Tip-In"` bug for oracle parity. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player+team (+game when `multi_games=False`).

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_stats_agg import ncaa_wbb_player_stats
stats = ncaa_wbb_player_stats(pbp)
print(stats.shape)
```

### ncaa_wbb_shot_locations {#ncaa_wbb_shot_locations}

`ncaa_wbb_shot_locations(game_ids: 'Sequence[object]', *, fetcher: 'Optional[Any]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Scrape WBB shot locations for one or more games.

Same driver as
`sportsdataverse.mbb.mbb_ncaa_shots.ncaa_mbb_shot_locations` with
the quarters `period_model` bound — see the mbb sibling for the parse
algorithm and the `~sportsdataverse.mbb.mbb_ncaa_shots.SHOTS_SCHEMA`
contract.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_ids` | `Sequence[object]` |  | NCAA contest ids; `None`/NaN entries are dropped. |
| `fetcher` | `Optional[Any]` | `None` | Optional injected fetcher exposing `fetch_game_box` (tests/offline). Defaults to a fresh `NcaaFetcher.with_browser()` context per call. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

All games' shots row-bound (zero-row schema frame when none found).

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_shots import ncaa_wbb_shot_locations
shots = ncaa_wbb_shot_locations(["5722355"])
print(shots.shape)
```

### ncaa_wbb_team_roster {#ncaa_wbb_team_roster}

`ncaa_wbb_team_roster(team_id: 'Optional[int]' = None, *, team: 'Optional[str]' = None, season: 'Optional[str]' = None, fetcher: "Optional['NcaaFetcher']" = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Scrape a women's team roster from stats.ncaa.org.

Port of wbigballR `get_team_roster` with name resolution fixed to the
WBB crosswalk (see the module docstring). The roster parser itself is
league-agnostic; algorithm detail:
`sportsdataverse.mbb.mbb_ncaa_schedule.ncaa_mbb_team_roster`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Optional[int]` | `None` | stats.ncaa.org team id (changes every season). |
| `team` | `Optional[str]` | `None` | School name, e.g. `"South Carolina"`. |
| `season` | `Optional[str]` | `None` | Season string, e.g. `"2024-25"`; required with `team`. |
| `fetcher` | `Optional['NcaaFetcher']` | `None` | Injectable `~sportsdataverse.mbb.mbb_ncaa_fetch. NcaaFetcher`; defaults to a fresh browser-transport fetcher. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player — see `~sportsdataverse.mbb.mbb_ncaa_schedule.parse_ncaa_bb_team_roster` for the column contract.

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_schedule import ncaa_wbb_team_roster
df = ncaa_wbb_team_roster(team="South Carolina", season="2024-25")
print(df.select("jersey", "player", "ht_inches").head())
```

### ncaa_wbb_team_schedule {#ncaa_wbb_team_schedule}

`ncaa_wbb_team_schedule(team_id: 'Optional[int]' = None, *, team: 'Optional[str]' = None, season: 'Optional[str]' = None, fetcher: "Optional['NcaaFetcher']" = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Scrape a women's team's season schedule from stats.ncaa.org.

Port of wbigballR `get_team_schedule` with name resolution fixed to the
WBB crosswalk (see the module docstring). Algorithm detail:
`sportsdataverse.mbb.mbb_ncaa_schedule.ncaa_mbb_team_schedule`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Optional[int]` | `None` | stats.ncaa.org team id (changes every season). |
| `team` | `Optional[str]` | `None` | School name, e.g. `"South Carolina"`. |
| `season` | `Optional[str]` | `None` | Season string, e.g. `"2024-25"`; required with `team`. |
| `fetcher` | `Optional['NcaaFetcher']` | `None` | Injectable `~sportsdataverse.mbb.mbb_ncaa_fetch. NcaaFetcher`; defaults to a fresh browser-transport fetcher. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per scheduled game — see `~sportsdataverse.mbb.mbb_ncaa_schedule.parse_ncaa_bb_team_schedule` for the column contract.

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_schedule import ncaa_wbb_team_schedule
df = ncaa_wbb_team_schedule(team="South Carolina", season="2024-25")
print(df.shape)
```

### ncaa_wbb_team_stats {#ncaa_wbb_team_stats}

`ncaa_wbb_team_stats(pbp: 'pl.DataFrame', *, include_transition: 'bool' = False, fix_tip_in: 'bool' = True, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Aggregate WBB play-by-play into per-team game stats (wbigballR `get_team_stats`).

Pure delegation to
`sportsdataverse.mbb.mbb_ncaa_stats_agg.ncaa_mbb_team_stats` — see
it for the algorithm and column contract.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame in the sdv-py 35-column snake_case bigballR contract (`ncaa_wbb_game_pbp` output). |
| `include_transition` | `bool` | `False` | Append the trans`/half` split surface. |
| `fix_tip_in` | `bool` | `True` | Count the real `"Tip In"` vocabulary (default); False reproduces R's `"Tip-In"` bug for oracle parity. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team per game.

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_stats_agg import ncaa_wbb_team_stats
team = ncaa_wbb_team_stats(pbp)
print(team.shape)
```
