---
title: "CFB — additional Python functions — sportsdataverse-data releases"
sidebar_label: "sportsdataverse-data releases"
sidebar_position: 3
description: "CFB — additional Python functions — sportsdataverse-data releases — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — sportsdataverse-data releases

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
| `espn_team_id` | integer |  |
| `fox_team_id` | character |  |
| `person_key` | character | Normalized player-name join key: 'Last, First' flipped, lowercased, ASCII-folded, punctuation stripped and runs of initials merged, so 'C.J.' and 'CJ' both give 'cj' (e.g. 'josh brown'). |
| `espn_athlete_id` | integer |  |
| `fox_athlete_id` | character |  |
| `yahoo_athlete_id` | character | Present but unpopulated in the published data (all null): the asset is built with providers=('espn', 'fox'), so no Yahoo ids are joined. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `espn_jersey` | character |  |
| `fox_jersey` | character |  |
| `espn_position` | character |  |
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
