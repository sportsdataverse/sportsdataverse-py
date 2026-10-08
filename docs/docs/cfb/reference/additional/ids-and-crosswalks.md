---
title: "CFB — additional Python functions — IDs and crosswalks"
sidebar_label: "IDs and crosswalks"
sidebar_position: 11
description: "CFB — additional Python functions — IDs and crosswalks — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — IDs and crosswalks

### cfb_odds_events_crosswalk {#cfb_odds_events_crosswalk}

`cfb_odds_events_crosswalk(season: 'Optional[int]' = None, week: 'Optional[int]' = None, *, sport: 'str' = 'americanfootball_ncaaf', api_key: 'Optional[str]' = None, season_type: 'int' = 2, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'DataFrameT'`

Match The Odds API CFB events to ESPN game ids.

Pulls the upcoming/live events for `sport` from The Odds API and the ESPN
scoreboard for `(season, week)`, then joins them on the order-independent
team matchup so each odds event id maps to its ESPN `event` id. Because
The Odds API only lists near-term events, this is most useful for the
current/upcoming week.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | ESPN season year for the schedule side. Defaults to the most recent CFB season. |
| `week` | `Optional[int]` | `None` | ESPN schedule week. When `None`, ESPN returns its default (current) slate. |
| `sport` | `str` | `'americanfootball_ncaaf'` | The Odds API sport key. Defaults to `"americanfootball_ncaaf"`. |
| `api_key` | `Optional[str]` | `None` | The Odds API key; falls back to the `ODDS_API_KEY` env var. |
| `season_type` | `int` | `2` | ESPN season type (`2` regular, `3` post-season). Defaults to `2`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. |

**Returns**

A polars DataFrame (pandas when `return_as_pandas=True`), one row per odds event, with columns `matchup_key`, `odds_event_id`, `espn_game_id`, `home_team`, `away_team`, `commence_time`, `espn_date`, `matched_sources`.

| col_name | type | description |
|---|---|---|
| `matchup_key` | character | Order-independent key for the game: the two normalized team names sorted alphabetically and joined with a pipe (e.g. 'akron zips\|minnesota golden gophers'). Built from the Odds API home_team and away_team names. |
| `odds_event_id` | character | The Odds API event id, a 32-character lowercase hex string (e.g. 'f06e90b4212fb514f3564ded9f190107'); the id column of toa_sports_events. |
| `espn_game_id` | integer |  |
| `home_team` | character | Home team name. |
| `away_team` | character | Away team name. |
| `commence_time` | character | Scheduled kickoff of the Odds API event, an ISO-8601 UTC string with a trailing Z (e.g. '2026-09-19T16:00:00Z'), kept as text. |
| `espn_date` | character | Kickoff date as YYYY-MM-DD: the first ten characters of the matched ESPN game's UTC start timestamp; null when no ESPN game matched. |
| `matched_sources` | character | 'odds+espn' when the Odds API event matched an ESPN game on matchup_key, 'odds' when it did not; all 88 sampled rows were 'odds+espn'. |

**Example**

```python
from sportsdataverse.cfb import cfb_odds_events_crosswalk
xwalk = cfb_odds_events_crosswalk(season=2024, week=5)
matched = xwalk.filter(pl.col("espn_game_id").is_not_null())
```

### cfb_rosters_crosswalk {#cfb_rosters_crosswalk}

`cfb_rosters_crosswalk(espn_team_id: 'Union[int, str]', fox_team_id: 'Union[int, str]', *, season: 'Optional[int]' = None, providers: 'Optional[Sequence[str]]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'DataFrameT'`

Build the ESPN x Fox x Yahoo player-id crosswalk for one team.

Fetches the selected providers' players for the team, matches them on
normalized name (with jersey as a confidence signal), and returns each
player's ESPN, Fox, and Yahoo athlete ids side by side. Use
`cfb_teams_crosswalk` first to translate an ESPN team id into the
matching Fox team id.

ESPN and Fox provide full rosters, so the default is `("espn", "fox")`.
**Yahoo is opt-in** (pass `providers=("espn", "fox", "yahoo")`) because it
has no roster endpoint — its only player feed is the season stat-leaderboard
(`sportsdataverse.cfb.yahoo_cfb_player_season_stats`), which is the
league's top ~200 players (roughly one per team) and frequently includes no
player for a given team at all. When selected, the team is resolved by
matching Yahoo's (abbreviated) team name against the ESPN team's name; if it
can't be resolved, the Yahoo columns are simply null.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `espn_team_id` | `Union[int, str]` |  | ESPN team id (e.g. `194` for Ohio State). |
| `fox_team_id` | `Union[int, str]` |  | Fox Bifrost team id (e.g. `25` for Ohio State). |
| `season` | `Optional[int]` | `None` | Season year for the Yahoo player-stats leg. Defaults to the most recent CFB season. Unused when Yahoo isn't selected. |
| `providers` | `Optional[Sequence[str]]` | `None` | Which sources to include — any of `"espn"`, `"fox"`, `"yahoo"`. `None` (default) uses `("espn", "fox")`; add `"yahoo"` explicitly for its (sparse) leg, or pass a single source. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. |

**Returns**

A polars DataFrame (pandas when `return_as_pandas=True`) with columns `person_key`, `espn_athlete_id`, `fox_athlete_id`, `yahoo_athlete_id`, `name`, `espn_jersey`, `fox_jersey`, `espn_position`, `fox_position`, `yahoo_position`, `match_method`, `matched_sources`. `match_method` reflects the ESPN/Fox jersey agreement: `name_jersey` (agree), `name` (name only), `name_jersey_conflict` (jerseys differ — review), or `unmatched`.

| col_name | type | description |
|---|---|---|
| `person_key` | character |  |
| `espn_athlete_id` | integer |  |
| `fox_athlete_id` | character |  |
| `yahoo_athlete_id` | character |  |
| `name` | character | Position name (e.g. `Quarterback`). |
| `espn_jersey` | character |  |
| `fox_jersey` | character |  |
| `espn_position` | character |  |
| `fox_position` | character |  |
| `yahoo_position` | character |  |
| `match_method` | character | Combination of matched sources, e.g. "fox+bart" / "fox_only" / "bart_only" / "espn_only". |
| `matched_sources` | character |  |

**Example**

```python
from sportsdataverse.cfb import cfb_rosters_crosswalk
xwalk = cfb_rosters_crosswalk(espn_team_id=194, fox_team_id=25, season=2024)
matched = xwalk.filter(pl.col("matched_sources") == "espn+fox")

# Just ESPN vs Fox (skip Yahoo's partial leg)

espn_fox = cfb_rosters_crosswalk(194, 25, providers=("espn", "fox"))
```

### cfb_schedule_crosswalk {#cfb_schedule_crosswalk}

`cfb_schedule_crosswalk(season: 'int', week: 'Optional[int]' = None, *, season_type: 'int' = 2, providers: 'Optional[Sequence[str]]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'DataFrameT'`

Build the ESPN x Fox x Yahoo CFB game-id crosswalk.

Each ESPN game is keyed by its order-independent team matchup, and the Fox
and Yahoo games are mapped onto it, so each row pairs the ESPN `event` id
with the Fox Bifrost event id and the Yahoo dotted game id. Where a provider
has no game, its columns are `None` and `matched_sources` records who
contributed — so regular season, conference championships, bowls, and the
CFP all flow through the same call, degrading gracefully when a source lacks
a game.

Two modes:

* **Full season** (`week` omitted): pulls every ESPN game (regular weeks +
  bowls + CFP), Fox's full season, and Yahoo's full season, and matches on
  team **+ date** (date disambiguates rematches — a regular-season game vs a
  conference-championship or CFP rematch of the same teams).
* **Single week** (`week` given): just that week's slate, matched on team.

Each provider leg is best-effort: a Fox outage, a Yahoo per-week parser
hiccup, or Fox's offseason-projected CFP matchups simply leave that
provider's columns null rather than failing the call.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season year (e.g. `2024`). |
| `week` | `Optional[int]` | `None` | Schedule week number for single-week mode; omit (`None`) for the whole season. |
| `season_type` | `int` | `2` | ESPN season type for single-week mode — `2` regular, `3` post-season (`week=1` bowls, `week=999` CFP). Ignored in full-season mode. Defaults to `2`. |
| `providers` | `Optional[Sequence[str]]` | `None` | Which sources to include — any of `"espn"`, `"fox"`, `"yahoo"`. `None` (default) uses all three; pass a subset for a pairwise crosswalk (e.g. `("espn", "fox")`) or a single source. Unselected providers are not fetched and surface as null columns. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. |

**Returns**

A polars DataFrame (pandas when `return_as_pandas=True`) with columns `matchup_key`, `espn_game_id`, `fox_game_id`, `yahoo_game_id`, `yahoo_global_game_id`, `home_team`, `away_team`, `espn_date`, `fox_date`, `yahoo_date`, `matched_sources`.

| col_name | type | description |
|---|---|---|
| `matchup_key` | character |  |
| `espn_game_id` | integer |  |
| `fox_game_id` | character |  |
| `yahoo_game_id` | character |  |
| `yahoo_global_game_id` | character |  |
| `home_team` | character | Home team name. |
| `away_team` | character | Away team name. |
| `espn_date` | character |  |
| `fox_date` | character |  |
| `yahoo_date` | character |  |
| `matched_sources` | character |  |

**Example**

```python
from sportsdataverse.cfb import cfb_schedule_crosswalk
full = cfb_schedule_crosswalk(2024)
all_three = full.filter(pl.col("matched_sources") == "espn+fox+yahoo")

# Or just one week

wk5 = cfb_schedule_crosswalk(2024, 5)
```

### cfb_teams_crosswalk {#cfb_teams_crosswalk}

`cfb_teams_crosswalk(*, season: 'Optional[int]' = None, week: 'int' = 1, providers: 'Optional[Sequence[str]]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'DataFrameT'`

Build the ESPN x Fox x Yahoo CFB team-id crosswalk.

Fetches the selected provider team directories, normalizes each team name to
a shared key, and full-outer-joins them so every row carries each provider's
id, name, and abbreviation (`None` where a provider has no match). The
`matched_sources` column records which providers contributed.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year used only to fetch Yahoo's embedded team directory (Yahoo has no standalone teams endpoint). Defaults to the most recent CFB season. |
| `week` | `int` | `1` | Schedule week used for the Yahoo scoreboard fetch. Defaults to `1`. The embedded directory is the full league list regardless. |
| `providers` | `Optional[Sequence[str]]` | `None` | Which sources to include — any of `"espn"`, `"fox"`, `"yahoo"`. `None` (default) uses all three; pass a subset for a pairwise crosswalk (e.g. `("espn", "fox")`) or a single source. Unselected providers are not fetched and surface as null columns. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. |

**Returns**

A polars DataFrame (pandas when `return_as_pandas=True`) with columns `norm_key`, `espn_team_id`, `espn_team`, `espn_abbreviation`, `fox_team_id`, `fox_team`, `fox_abbreviation`, `yahoo_team_id`, `yahoo_team`, `yahoo_abbreviation`, `matched_sources`.

| col_name | type | description |
|---|---|---|
| `norm_key` | character | Shared join key across providers: the team name lowercased, ASCII-folded, stripped of punctuation, whitespace-collapsed and alias-mapped (e.g. 'alabama a m bulldogs'). |
| `espn_team_id` | integer |  |
| `espn_team` | character | ESPN's full team display name, school plus mascot (e.g. 'Akron Zips'); null on rows that matched no ESPN team. |
| `espn_abbreviation` | character |  |
| `fox_team_id` | character |  |
| `fox_team` | character | Fox Sports' team name, which that feed ships in all capitals (e.g. 'AIR FORCE FALCONS'); null when no Fox team matched. |
| `fox_abbreviation` | character | Fox Sports' short team code from its teamnav directory (e.g. 'AC', 'AKRON'), which can differ from the Yahoo code for the same school ('AKRON' vs 'AKR'); null when no Fox team matched. |
| `yahoo_team_id` | character |  |
| `yahoo_team` | character | Yahoo Sports' team display name, school plus mascot (e.g. 'Akron Zips'); null when no Yahoo team matched. |
| `yahoo_abbreviation` | character | Yahoo Sports' short team code (e.g. 'ACU', 'AKR'); null when no Yahoo team matched. |
| `matched_sources` | character | Plus-joined provenance tag naming which of espn, fox and yahoo contributed a directory row for this team, e.g. 'espn+fox+yahoo', 'espn', 'fox+yahoo'. |

**Example**

```python
from sportsdataverse.cfb import cfb_teams_crosswalk
xwalk = cfb_teams_crosswalk(season=2024)
row = xwalk.filter(pl.col("espn_team_id") == 194)  # Ohio State

# Pairwise — just ESPN vs Fox

espn_fox = cfb_teams_crosswalk(providers=("espn", "fox"))
```
