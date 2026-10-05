---
title: "MBB — additional Python functions — NCAA (stats.ncaa.org)"
sidebar_label: "NCAA (stats.ncaa.org)"
sidebar_position: 8
description: "MBB — additional Python functions — NCAA (stats.ncaa.org) — function reference in sdv-py, the SportsDataverse Python package."
---
# MBB — additional Python functions — NCAA (stats.ncaa.org)

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
| `season` | character | Season year. |
| `ncaa_team_id` | integer | stats.ncaa.org team id (Int64) for that season; stats.ncaa.org issues a new id every season, so the same school has a different id on each season row. |
| `ncaa_team` | character | School name as stats.ncaa.org writes it, in AP-style abbreviations (e.g. 'Alabama St.', 'A&M-Corpus Christi'). |
| `ncaa_conference` | character | stats.ncaa.org label of the team's conference that season (e.g. 'SEC', 'MWC'), taken from the groups tables' NCAA aliases for the season's conference_id. A conference with no NCAA alias gets its SDV abbreviation (men's Great West -> 'GWC'); a team the groups table has no row for keeps the bundled stats.ncaa.org team-list label. The label style can differ by league ('MWC' in men's, 'Mountain West' in women's through 2022-23). |
| `espn_team_id` | character | ESPN team id (canonical key). |
| `espn_display_name` | character | ESPN display name (school + mascot). |
| `espn_location` | character | ESPN school/location only. |
| `espn_mascot` | character | ESPN mascot/nickname. |
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

### ncaa_mbb_box_scores {#ncaa_mbb_box_scores}

`ncaa_mbb_box_scores(game_ids: 'Union[str, int, Iterable[Union[str, int]]]', *, multi_games: 'bool' = False, fetcher: 'Optional[_SupportsFetchIndividualStats]' = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Box scores for one or more NCAA games (bigballR `get_box_scores` port).

Multi-game driver over `parse_ncaa_bb_box`
(`bigballR/R/all_functions.R:3603-3678`): drops null ids, isolates
per-game errors (failed ids are reported and skipped), binds rows, and
optionally aggregates across games.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_ids` | `Union[str, int, Iterable[Union[str, int]]]` |  | One id or an iterable of NCAA contest ids. |
| `multi_games` | `bool` | `False` | When `True`, aggregate one row per `(player, clean_name, team)` -- counters summed, `g` = games played, rates recomputed from the sums (R's `multi.games`; grouping adapted per module docstring). |
| `fetcher` | `Optional[_SupportsFetchIndividualStats]` | `None` | Optional injected fetcher exposing `fetch_game_individual_stats` (for offline replay/tests). Defaults to a fresh `NcaaFetcher.with_browser()` context per call -- stats.ncaa.org sits behind an Akamai challenge that the plain transport cannot clear. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

polars.DataFrame (or pandas with `return_as_pandas=True`): per-game rows in the `parse_ncaa_bb_box` contract, or the aggregated `multi_games` contract.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_box_stats import ncaa_mbb_box_scores
df = ncaa_mbb_box_scores(["6470186", "6479639"])
print(df.shape)

# Season aggregate for a scraped id list

agg = ncaa_mbb_box_scores(ids, multi_games=True)

# Offline with an injected fetcher

df = ncaa_mbb_box_scores("6470186", fetcher=my_fetcher)
```

### ncaa_mbb_date_games {#ncaa_mbb_date_games}

`ncaa_mbb_date_games(date: 'Optional[str]' = None, *, conference: 'str' = 'All', conference_id: 'Optional[int]' = None, fetcher: "'Optional[NcaaFetcher]'" = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Discover every NCAA MBB game played on a date (bigballR `get_date_games`).

Fetches `stats.ncaa.org/season_divisions/{sid}/scoreboards` for the
date's season and returns one row per game with the `/contests/{id}`
game id needed by the play-by-play / box-score scrapers. Port of bigballR
`get_date_games` (all_functions.R:1119-1427).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `Optional[str]` | `None` | `"MM/DD/YYYY"`. Defaults to yesterday (R default). |
| `conference` | `str` | `'All'` | Conference name filter (e.g. `"ACC"`, `"Big Ten"`, `"Metro"` / `"MAAC"`); case/punctuation-insensitive, and both the current stats.ncaa.org label and bigballR's abbreviation work. Default `"All"` (every conference). Unknown names raise. |
| `conference_id` | `Optional[int]` | `None` | Explicit stats.ncaa.org conference id; overrides *conference* when given (R's `conference.ID`). |
| `fetcher` | `Optional[NcaaFetcher]` | `None` | Injectable `~sportsdataverse.mbb.mbb_ncaa_fetch .NcaaFetcher` (tests pass an offline fake). `None` uses `NcaaFetcher.with_browser()` — the page is JS-rendered behind Akamai bm-verify, so the browser transport is the live default. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per game with columns `date, start_time, home, away, box_id, game_id, home_score, away_score, attendance, neutral_site, home_wins, home_losses, away_wins, away_losses` (`SCOREBOARD_SCHEMA`). Scores stay Utf8 — they hold `"Canceled"` / `"Ppd"` for unplayed games; `game_id` is null for games without a box score.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_scoreboard import ncaa_mbb_date_games
games = ncaa_mbb_date_games("11/11/2025")
print(games.shape)

# Useful parameter combination

acc_pd = ncaa_mbb_date_games("02/01/2025", conference="ACC",
                             return_as_pandas=True)

# Pipeline next step (one line)

games.filter(pl.col("game_id").is_not_null())["game_id"].to_list()
```

### ncaa_mbb_game_pbp {#ncaa_mbb_game_pbp}

`ncaa_mbb_game_pbp(game_id: 'object', *, fetcher: 'Optional[_SupportsFetchGamePbp]' = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, Any]'"`

Scrape one MBB game's play-by-play (bigballR `scrape_game`).

Fetches `stats.ncaa.org/contests/{game_id}/play_by_play` and parses it
through `parse_ncaa_bb_game_pbp` with the MBB period model
`(2, 1200, 300)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `object` |  | NCAA contest id (e.g. `"6470186"`). |
| `fetcher` | `Optional[_SupportsFetchGamePbp]` | `None` | Optional injected fetcher exposing `fetch_game_pbp` (for tests/offline use). Defaults to a fresh `NcaaFetcher.with_browser()` context per call. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The 35-column play-by-play frame (zero rows when the game is not found).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_game_pbp import ncaa_mbb_game_pbp
df = ncaa_mbb_game_pbp("6470186")
print(df.shape)

# Offline with an injected fetcher

df = ncaa_mbb_game_pbp("6470186", fetcher=my_fetcher)

# Pipeline next step (one line)

df.filter(pl.col("event_type") == "Three Point Jumper").head()
```

### ncaa_mbb_join_pbp_shots {#ncaa_mbb_join_pbp_shots}

`ncaa_mbb_join_pbp_shots(pbp: 'pl.DataFrame', shots: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, Any]'"`

Attach shot-chart coordinates to play-by-play rows (bigballR

`join_pbp_shots`, `get_shot_locations.R:93-135`).

FG attempts (`shot_value` 2/3) are matched to chart shots on
`(game_id, game_seconds, event_result == shot_result, shot_no)` where
`shot_no` is the within-second same-result sequence number on BOTH
sides — free throws and non-shot rows are deliberately excluded from
matching (the chart plots FGs only) and pass through NA-filled. Row
count and per-game row order are preserved; the explicit `shot_dist`
carry-through is fork-skew fix #11 (wbigballR's unexported copy drops
it). Works identically for the WBB extension — feed it quarter-model
pbp + shots frames.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | The 35-column snake_case pbp contract frame (see `~sportsdataverse.mbb.mbb_ncaa_game_pbp.PBP_SCHEMA`). |
| `shots` | `DataFrame` |  | A `SHOTS_SCHEMA` frame (from `parse_ncaa_bb_shots` / `ncaa_mbb_shot_locations`) covering exactly the same game ids. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The input pbp frame + `team`, `player`, `x`, `y`, `shot_dist` (null on non-FG rows and unmatched FG rows), sorted by (game_id, original per-game row order) exactly as R's `arrange(row, .by_group = TRUE)`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_game_pbp import ncaa_mbb_play_by_play
from sportsdataverse.mbb.mbb_ncaa_shots import (
    ncaa_mbb_join_pbp_shots,
    ncaa_mbb_shot_locations,
)
pbp = ncaa_mbb_play_by_play(["6470186"])
shots = ncaa_mbb_shot_locations(["6470186"])
joined = ncaa_mbb_join_pbp_shots(pbp, shots)

# Pipeline next step (one line)

joined.filter(pl.col("x").is_not_null()).head()
```

### ncaa_mbb_on_off {#ncaa_mbb_on_off}

`ncaa_mbb_on_off(players: 'Union[str, Sequence[str]]', lineups: 'pl.DataFrame', *, included: 'Union[str, Sequence[str], None]' = None, excluded: 'Union[str, Sequence[str], None]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Team stats for every on/off combination of the given players.

Port of bigballR `on_off_generator` (`all_functions.R:2555-2749`,
`include_transition=F` path). For k players, all `2^k` on/off
assignments are enumerated in R's `expand.grid` order (first player
varies fastest; first row all-On, last all-Off); each combination sums
the lineups whose membership matches exactly, re-derives the full ratio
block (including `e_poss`), rounds to 3 decimals, and zeroes NA/Inf.
Combinations matching zero lineups produce an all-zero row.

Faithful R quirk (kept, flagged): when `included`/`excluded` is
passed, the base lineup set comes from the membership filter ONLY — the
inferred-team filter is skipped (`all_functions.R:2584-2589`), so an
included player on another team would leak that team's lineups in.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players` | `Union[str, Sequence[str]]` |  | Player name(s) to split on (the `Status` axis). |
| `lineups` | `DataFrame` |  | Lineups frame from `ncaa_mbb_lineups`. |
| `included` | `Union[str, Sequence[str], None]` | `None` | Optional membership filter forwarded to `ncaa_mbb_player_lineups` (replaces the team filter). |
| `excluded` | `Union[str, Sequence[str], None]` | `None` | Optional membership filter forwarded to `ncaa_mbb_player_lineups` (replaces the team filter). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`) with `2^k` rows — `status` (e.g. `"A.PLAYER On | B.PLAYER Off"`) + the 69 stat columns (`ON_OFF_COLUMNS`).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_on_off
split = ncaa_mbb_on_off("KEATON.WAGLER", lineups)
print(split.shape)

# Two-player interaction

duo = ncaa_mbb_on_off(["A.PLAYER", "B.PLAYER"], lineups)

# Pipeline next step (one line)

split.select("status", "netrtg")
```

### ncaa_mbb_player_combos {#ncaa_mbb_player_combos}

`ncaa_mbb_player_combos(lineups: 'pl.DataFrame', *, n: 'int' = 2, min_mins: 'float' = 0, included: 'Union[str, Sequence[str], None]' = None, excluded: 'Union[str, Sequence[str], None]' = None, include_transition: 'bool' = False, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Team stats for every n-player combination on the court together.

Port of bigballR `get_player_combos` + `team_comb`
(`get_player_combos.R:19-38` + `:42-191`). Combos are enumerated per
team over the byte-sorted unique player pool (lexicographic
`gtools::combinations` order), filtered to combos whose lineups total
strictly more than `min_mins` minutes, then each combo's lineup rows
are summed and the ratio block re-derived. **Rounding is 2 decimals**
here (R rounds 3 in `get_lineups` / `on_off_generator`) and `e_poss`
is the SUM of the per-lineup estimates, not a recompute — both faithful.

R's `include_transition` switch is dead code (an exact-match guard at
`get_player_combos.R:28-30` always forces it back to `FALSE`); the
port implements the suffix-match intent but keeps the `False` default,
which is the only R-reachable behavior.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineups` | `DataFrame` |  | Lineups frame from `ncaa_mbb_lineups`. |
| `n` | `int` | `2` | Combination size, 1-5. |
| `min_mins` | `float` | `0` | Keep combos with total on-court minutes strictly greater than this (summed over the rounded per-lineup `mins`). |
| `included` | `Union[str, Sequence[str], None]` | `None` | Player name(s) that must be on the court in every lineup considered. |
| `excluded` | `Union[str, Sequence[str], None]` | `None` | Player name(s) that must be off the court in every lineup considered. |
| `include_transition` | `bool` | `False` | Re-derive the trans`/half` ratio surface (requires a transition lineups frame; forced `False` otherwise). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`) with one row per combo: `team, p1..pn` + the stat surface of the input frame. Teams appear in byte-sorted order.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_player_combos
duos = ncaa_mbb_player_combos(lineups, n=2, min_mins=5)
print(duos.shape)

# Anchored on one player

trios = ncaa_mbb_player_combos(lineups, n=3, included="KEATON.WAGLER")

# Pipeline next step (one line)

duos.sort("netrtg", descending=True).head()
```

### ncaa_mbb_player_lineups {#ncaa_mbb_player_lineups}

`ncaa_mbb_player_lineups(lineups: 'pl.DataFrame', *, included: 'Union[str, Sequence[str], None]' = None, excluded: 'Union[str, Sequence[str], None]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Filter a lineups frame by on-court player membership.

Port of bigballR `get_player_lineups` (`all_functions.R:2761-2792`):
keep rows where every `included` player is on the court (in `p1..p5`)
and no `excluded` player is. With both filters `None` the input is
returned unchanged (R's `Included = NA, Excluded = NA` passthrough).
Membership is tested by name against `p1..p5` (R tests the positional
first five columns); row order is preserved.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineups` | `DataFrame` |  | Lineups frame from `ncaa_mbb_lineups` (any frame with `p1..p5` works). |
| `included` | `Union[str, Sequence[str], None]` | `None` | Player name(s) that must ALL be on the court. |
| `excluded` | `Union[str, Sequence[str], None]` | `None` | Player name(s) that must NONE be on the court. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Row-subset of `lineups`; schema unchanged.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_player_lineups
on = ncaa_mbb_player_lineups(lineups, included="KEATON.WAGLER")
print(on.shape)

# Included + excluded combination

df = ncaa_mbb_player_lineups(lineups, included=["A.PLAYER"], excluded=["B.PLAYER"])

# Pipeline next step (one line)

on.select(pl.col("mins").sum())
```

### ncaa_mbb_player_stats {#ncaa_mbb_player_stats}

`ncaa_mbb_player_stats(pbp: 'pl.DataFrame', *, multi_games: 'bool' = False, simple: 'bool' = False, fix_tip_in: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Aggregate bigballR-contract play-by-play into per-player box stats.

Port of bigballR `get_player_stats` (`all_functions.R:2810-3177`) +
its `get_mins` helper (`:3240-3263`). Counting stats are summarised
per (game, team, player), assists counted from `player_2`, minutes and
offensive possessions derived from the ten on-court columns, and rates
(FG%, TS%, eFG%, rim/mid splits, ...) computed from the counters and
rounded to 3 decimals with R's `round` semantics. With
`multi_games=True` the per-game rows are summed per (player, team),
every rate is recomputed from the summed counters (never averaged), and
`GP`/`GS` are appended.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame in the sdv-py 35-column snake_case bigballR contract. May span multiple games. |
| `multi_games` | `bool` | `False` | When True, aggregate across games per (player, team) — the season-stat surface. When False (default, R parity), treat each game separately and keep the game id columns. |
| `simple` | `bool` | `False` | When True, return the reduced 33-column (multi) / 35-column (per-game) surface without the transition / assisted / putback / block-location splits. |
| `fix_tip_in` | `bool` | `True` | When True (default), rim and putback stats count the scrape engine's real `"Tip In"` vocabulary. When False, reproduce R's literal `"Tip-In"` test (`all_functions.R:2827`) — tip-ins silently excluded — for oracle parity. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`): one row per player+team (+game when `multi_games=False`). Columns follow `PLAYER_STATS_COLUMNS` / `PLAYER_STATS_SIMPLE_COLUMNS` / `PLAYER_GAME_STATS_COLUMNS` / `PLAYER_GAME_STATS_SIMPLE_COLUMNS`. Rows sorted by the group keys (byte order, matching dplyr's C-locale group order). Empty input yields an empty frame with the documented schema.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stats_agg import ncaa_mbb_player_stats
season = ncaa_mbb_player_stats(pbp, multi_games=True)
print(season.shape)

# Reduced surface, pandas out

df_pd = ncaa_mbb_player_stats(pbp, multi_games=True, simple=True, return_as_pandas=True)

# Pipeline next step (one line)

season.filter(pl.col("mins") > 50).sort("pts", descending=True).head()
```

### ncaa_mbb_shot_locations {#ncaa_mbb_shot_locations}

`ncaa_mbb_shot_locations(game_ids: "'Sequence[object]'", *, fetcher: 'Optional[_SupportsFetchGameBox]' = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, Any]'"`

Scrape MBB shot locations for one or more games (bigballR

`get_shot_locations`, `get_shot_locations.R:3-89`).

Fetches each game's `stats.ncaa.org/contests/{id}/box_score` page and
parses the embedded shot-chart JS through `parse_ncaa_bb_shots`.
NA ids are dropped up front (R `:5`); per-game "shots found" messages
go to the module logger (R `message`, `:69-70`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_ids` | `Sequence[object]` |  | NCAA contest ids; `None`/NaN entries are dropped. |
| `fetcher` | `Optional[_SupportsFetchGameBox]` | `None` | Optional injected fetcher exposing `fetch_game_box` (for tests/offline use). Defaults to a fresh `NcaaFetcher.with_browser()` context per call. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

All games' shots row-bound (zero-row `SHOTS_SCHEMA` frame when no ids survive or no charts are found).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shots import ncaa_mbb_shot_locations
df = ncaa_mbb_shot_locations(["6470186", "6479639"])
print(df.shape)

# Offline with an injected fetcher

df = ncaa_mbb_shot_locations(["6470186"], fetcher=my_fetcher)

# Pipeline next step (one line)

df.group_by("team").agg(pl.col("shot_dist").mean()).head()
```

### ncaa_mbb_team_stats {#ncaa_mbb_team_stats}

`ncaa_mbb_team_stats(pbp: 'pl.DataFrame', *, include_transition: 'bool' = False, fix_tip_in: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Aggregate bigballR-contract play-by-play into per-team game stats.

Port of bigballR `get_team_stats` (`all_functions.R:2530-2538`): the
ten on-court columns are blanked so every row shares one "lineup", then
`get_lineups` (`ncaa_mbb_lineups`) runs per game and the lineup
key columns are dropped — yielding two rows (one per team) per game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame in the sdv-py 35-column snake_case bigballR contract. May span multiple games. |
| `include_transition` | `bool` | `False` | When True, append the trans`/half` split surface plus `o_trans_pct`/`d_trans_pct`. |
| `fix_tip_in` | `bool` | `True` | When True (default), rim stats count the scrape engine's real `"Tip In"` vocabulary; `False` reproduces R's literal `"Tip-In"` bug for oracle parity. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`) with one row per team per game — `TEAM_STATS_COLUMNS` (73) or `TEAM_STATS_TRANSITION_COLUMNS` with `include_transition=True`. Games ordered by the Utf8 `game_id` byte sort (R's do() sorts a numeric ID — identical for equal-width ids), teams within a game byte-sorted. Empty input yields an empty frame with the documented schema.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stats_agg import ncaa_mbb_team_stats
teams = ncaa_mbb_team_stats(pbp)
print(teams.shape)

# Transition splits, pandas out

df_pd = ncaa_mbb_team_stats(pbp, include_transition=True, return_as_pandas=True)

# Pipeline next step (one line)

teams.sort("netrtg", descending=True).head()
```
