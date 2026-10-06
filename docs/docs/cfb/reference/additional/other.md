---
title: "CFB — additional Python functions — Other"
sidebar_label: "Other"
sidebar_position: 8
description: "CFB — additional Python functions — Other — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — Other

### espn_cfb_game_rosters {#espn_cfb_game_rosters}

`espn_cfb_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_cfb_game_rosters() - Pull the game by id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from espn_cfb_schedule(). |
| `raw` |  | `False` |  |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of game roster data with columns: 'athlete_id', 'athlete_uid', 'athlete_guid', 'athlete_type', 'first_name', 'last_name', 'full_name', 'athlete_display_name', 'short_name', 'weight', 'display_weight', 'height', 'display_height', 'age', 'date_of_birth', 'slug', 'jersey', 'linked', 'active', 'alternate_ids_sdr', 'birth_place_city', 'birth_place_state', 'birth_place_country', 'headshot_href', 'headshot_alt', 'experience_years', 'experience_display_value', 'experience_abbreviation', 'status_id', 'status_name', 'status_type', 'status_abbreviation', 'hand_type', 'hand_abbreviation', 'hand_display_value', 'draft_display_text', 'draft_round', 'draft_year', 'draft_selection', 'player_id', 'starter', 'valid', 'did_not_play', 'display_name', 'ejected', 'athlete_href', 'position_href', 'statistics_href', 'team_id', 'team_guid', 'team_uid', 'team_slug', 'team_location', 'team_name', 'team_nickname', 'team_abbreviation', 'team_display_name', 'team_short_display_name', 'team_color', 'team_alternate_color', 'is_active', 'is_all_star', 'team_alternate_ids_sdr', 'logo_href', 'logo_dark_href', 'game_id'

**Example**

```python
from sportsdataverse.cfb import espn_cfb_game_rosters
rosters = espn_cfb_game_rosters(game_id=401628334)
print(rosters.shape)

# Pandas round-trip

rosters_pd = espn_cfb_game_rosters(game_id=401628334, return_as_pandas=True)
rosters_pd.head()

# Pipeline next step (filter to game starters)

import polars as pl
starters = espn_cfb_game_rosters(game_id=401628334).filter(
    pl.col("starter") == True
)
```

### espn_cfb_play_participants {#espn_cfb_play_participants}

`espn_cfb_play_participants(game_id: 'int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, resolve_missing: 'bool' = True, resolve_missing_max: 'int' = 50, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull ESPN per-play participants for a college-football game.

The college-football entry point of the shared
`sportsdataverse.football.play_participants.espn_play_participants`;
see it for the column contract (`{type}_player_name` / `{type}_player_id`
scalars plus the `{type}_player_names` / `{type}_player_ids` lists per
participant type ESPN ships).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN game / event identifier. |
| `raw` | `bool` | `False` | If True, returns the raw list of play-items dicts. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; otherwise polars. |
| `resolve_missing` | `bool` | `True` | Fetch athletes the sidecar omits from their `$ref`. |
| `resolve_missing_max` | `int` | `50` | Cap on those per-athlete requests (default 50). |

**Returns**

Polars (or pandas) DataFrame, one row per play; the raw play dicts when `raw=True`.

**Example**

```python
from sportsdataverse.cfb import espn_cfb_play_participants
participants = espn_cfb_play_participants(game_id=401628334)
print(participants.shape)
```

### espn_cfb_teams {#espn_cfb_teams}

`espn_cfb_teams(groups=None, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_cfb_teams - look up the college football teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `groups` | `int` | `None` | Used to define different divisions. 80 is FBS, 81 is FCS. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing schedule dates for the requested season. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.cfb.espn_cfb_teams.clear_cache().

**Example**

```python
from sportsdataverse.cfb import espn_cfb_teams
teams = espn_cfb_teams()
print(teams.shape)

# Pull FCS teams (group 81)

fcs = espn_cfb_teams(groups=81, return_as_pandas=True)
fcs.head()

# Pipeline next step (build an abbreviation lookup)

teams = espn_cfb_teams()
abbr_map = dict(zip(teams["team_id"], teams["team_abbreviation"]))
```

### scoreboard_event_parsing {#scoreboard_event_parsing}

`scoreboard_event_parsing(event)`

Internal helper that flattens an ESPN scoreboard event dict into a shape

suitable for `pd.json_normalize`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` | `dict` |  | A single scoreboard `events[*]` entry from the ESPN college-football scoreboard API. |

**Returns**

The same event dict, mutated in place with `home`/`away` copies of the competitors and trimmed of unused link/odds keys.

**Example**

```python
from sportsdataverse.cfb import espn_cfb_schedule
sched = espn_cfb_schedule(dates=2023, week=5)
```

### load_cfb_betting_lines {#load_cfb_betting_lines}

`load_cfb_betting_lines(return_as_pandas=False) -> 'pl.DataFrame'`

Load college football betting lines information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing betting lines available for the available seasons.

| col_name | type | description |
|---|---|---|
| `id` | double | 247Sports referencing id for the recruit. |
| `game_id` | integer | ESPN game identifier. |
| `season` | double | Season (4-digit year). |
| `game_desc` | character | Human-readable description of the game, typically including team names and context. |
| `date_time` | character | Date and time of the game to which the betting line applies, as a string. |
| `market_type` | character | Geographic market type (e.g. `National`). |
| `abbr` | character | Selection/side this odds row applies to — a team abbreviation for spread and moneyline markets, or 'over'/'under' for total markets (the data is long-format, one row per book per selection per market_type). |
| `lines` | double | Numeric line for this row's market — the per-side point spread for spread markets or the over/under total points for total markets; null for moneyline rows. |
| `odds` | integer | American-odds price for this selection — the juice/vig on spread and total rows, or the moneyline price itself on moneyline rows. |
| `opening_lines` | double | Opening numeric line for this row's market (per-side spread or over/under total points) before line movement; null for moneyline rows. |
| `opening_odds` | integer | Opening American-odds price for this selection before line movement (vig on spread/total rows, moneyline price on moneyline rows). |
| `book` | character | Name of the sportsbook or oddsmaker that provided the betting line. |
| `season_type` | character | ESPN season type (2 = regular, 3 = postseason). |
| `week` | integer | Game week of the season. |
| `home_team_id` | integer | ESPN home team id (parsed from `home_team_ref`). |
| `away_team_id` | integer | ESPN away team id (parsed from `away_team_ref`). |

**Example**

```python
from sportsdataverse.cfb import load_cfb_betting_lines
lines = load_cfb_betting_lines()
print(lines.shape)

# Pandas round-trip

lines_pd = load_cfb_betting_lines(return_as_pandas=True)
lines_pd.head()

# Pipeline next step (filter to one provider in 2023)

import polars as pl
consensus_2023 = load_cfb_betting_lines().filter(
    (pl.col("season") == 2023) & (pl.col("provider") == "consensus")
)
```

### load_cfb_rosters_crosswalk {#load_cfb_rosters_crosswalk}

`load_cfb_rosters_crosswalk(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load the current ESPN x Fox CFB rosters crosswalk (single snapshot).

Unlike the per-season `load_cfb_teams_crosswalk` / `load_cfb_schedule_crosswalk`
loaders, this one is **season-less**: ESPN's and Fox's team-roster endpoints
only expose the *current* roster, so the published artifact is a single
snapshot rather than a historical per-season series. It is built by
`cfbfastR-cfb-data`'s `scripts/build_cfb_crosswalk.py` (which fans the
per-team `sportsdataverse.cfb.cfb_rosters_crosswalk` builder out over
the current season's ESPN<->Fox team-id pairs) and refreshed on that repo's
cadence.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

one row per matched player, carrying `espn_team_id` / `fox_team_id` provenance plus each provider's athlete id, name, jersey, position, and the `match_method` / `matched_sources` flags.

| col_name | type | description |
|---|---|---|
| `espn_team_id` | integer | ESPN team id (canonical key). |
| `fox_team_id` | character | Fox Bifrost team id (NA if unmatched). |
| `person_key` | character | Normalized player-name join key: 'Last, First' flipped, lowercased, ASCII-folded, punctuation stripped and runs of initials merged, so 'C.J.' and 'CJ' both give 'cj' (e.g. 'josh brown'). |
| `espn_athlete_id` | integer | ESPN athlete id. |
| `fox_athlete_id` | character | Fox athlete id (NA if unmatched). |
| `yahoo_athlete_id` | character | Present but unpopulated in the published data (all null): the asset is built with providers=('espn', 'fox'), so no Yahoo ids are joined. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `espn_jersey` | character | ESPN jersey number. |
| `fox_jersey` | character | Fox jersey number (NA if unmatched). |
| `espn_position` | character | ESPN position abbreviation. |
| `fox_position` | character | Position abbreviation from the Fox Sports roster (e.g. 'QB', 'OL', 'DB'); null when the player has no Fox roster match or Fox lists no position. |
| `yahoo_position` | character | Present but unpopulated in the published data (all null): the asset is built with providers=('espn', 'fox'), so no Yahoo positions are joined. |
| `match_method` | character | Combination of matched sources, e.g. "fox+bart" / "fox_only" / "bart_only" / "espn_only". |
| `matched_sources` | character | Plus-joined provenance tag naming which rosters listed the player: 'espn+fox', 'espn' or 'fox'. The published asset is built without Yahoo, so 'yahoo' never appears. |

**Example**

```python
from sportsdataverse.cfb import load_cfb_rosters_crosswalk
xwalk = load_cfb_rosters_crosswalk()
print(xwalk.shape)

# Pandas round-trip

xwalk_pd = load_cfb_rosters_crosswalk(return_as_pandas=True)

# Pipeline next step (one team's ESPN<->Fox athlete map)

import polars as pl
osu = load_cfb_rosters_crosswalk().filter(pl.col("espn_team_id") == 194)
```

### on3_industry_player_rankings {#on3_industry_player_rankings}

`on3_industry_player_rankings(year: 'Union[int, str]', sport_slug: 'str' = 'football', page: 'Any' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

On3 Industry Comparison player rankings (**deprecated** next/data` scrape).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  | recruiting class year (e.g. `2026`). |
| `sport_slug` | `str` | `'football'` | On3 sport slug (default `"football"`). |
| `page` | `Any` | `None` | 1-based page number, or `None` for the first page. |
| `return_parsed` | `bool` | `True` | return a tidy frame (default); `False` returns the raw dict. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row per recruit (consensus On3/Rivals/247/ESPN). Zero-row frame on empty.

**Example**

```python
from sportsdataverse.cfb import on3_players_industry_comparision  # forward RDB native
df = on3_players_industry_comparision(sport_key=1, year=2026)
print(df.shape)
```

### on3_industry_team_rankings {#on3_industry_team_rankings}

`on3_industry_team_rankings(year: 'Union[int, str]', sport_slug: 'str' = 'football', page: 'Any' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

On3 Industry Comparison team rankings (**deprecated** next/data` scrape).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  | recruiting class year (e.g. `2026`). |
| `sport_slug` | `str` | `'football'` | On3 sport slug (default `"football"`). |
| `page` | `Any` | `None` | 1-based page number, or `None` for the first page. |
| `return_parsed` | `bool` | `True` | return a tidy frame (default); `False` returns the raw dict. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row per team class (consensus ratings). Zero-row frame on empty payload.

**Example**

```python
from sportsdataverse.cfb import on3_team_ranking_consensus_team_rankings  # forward RDB native
df = on3_team_ranking_consensus_team_rankings(sport_slug="football", year=2025)
print(df.shape)
```

### on3_player_rankings {#on3_player_rankings}

`on3_player_rankings(year: 'Union[int, str]', sport_slug: 'str' = 'football', page: 'Any' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

On3 player rankings for a class year (**deprecated** next/data` scrape).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  | recruiting class year (e.g. `2026`). |
| `sport_slug` | `str` | `'football'` | On3 sport slug (default `"football"`). |
| `page` | `Any` | `None` | 1-based page number, or `None` for the first page. |
| `return_parsed` | `bool` | `True` | return a tidy frame (default); `False` returns the raw dict. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row per ranked recruit (On3 ratings). Zero-row frame on empty payload.

**Example**

```python
from sportsdataverse.cfb import on3_person_sport_rankings  # forward RDB native
df = on3_person_sport_rankings(sport_key=1, year=2026)
print(df.shape)
```

### on3_team_rankings {#on3_team_rankings}

`on3_team_rankings(year: 'Union[int, str]', sport_slug: 'str' = 'football', page: 'Any' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

On3 team recruiting-class rankings (**deprecated** next/data` scrape).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  | recruiting class year (e.g. `2026`). |
| `sport_slug` | `str` | `'football'` | On3 sport slug (default `"football"`). |
| `page` | `Any` | `None` | 1-based page number, or `None` for the first page. |
| `return_parsed` | `bool` | `True` | return a tidy frame (default); `False` returns the raw dict. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row per team class (On3 ratings). Zero-row frame on empty payload.

**Example**

```python
from sportsdataverse.cfb import on3_team_ranking_team_rankings  # forward RDB native
df = on3_team_ranking_team_rankings(sport_slug="football", year=2025)
print(df.shape)
```

### check_box_invariants {#check_box_invariants}

`check_box_invariants(drive_summary: 'dict | None' = None, situational: 'dict | None' = None) -> 'list[str]'`

Every identity the two aggregates must satisfy; violations as strings.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `drive_summary` | `dict \| None` | `None` | the dict from `create_drive_summary`, or None. |
| `situational` | `dict \| None` | `None` | the dict from `create_situational_stats`, or None. |

**Returns**

one line per violation, `[]` when everything holds.

**Example**

```python
assert check_box_invariants(summary, stats) == []
```
