---
title: The parser layer
sidebar_label: Parsers
sidebar_position: 1
---

# The parser layer

Every league wrapper returns raw `Dict` by default. The parser layer in
[`sportsdataverse._common_espn_parsers`](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/sportsdataverse/_common_espn_parsers.py)
turns those payloads into tidy polars (or pandas) DataFrames. Parsers
are league-agnostic: the same parser handles MLB, NFL, NBA, WBB, etc.
because ESPN's payload shapes are identical across leagues.

## Two ways to invoke a parser

### 1. The `return_parsed=True` shim (recommended)

Wrappers whose short name is in `ENDPOINT_PARSERS` accept an optional
`return_parsed=True` kwarg:

```python
from sportsdataverse.nba import espn_nba_team_roster

df = espn_nba_team_roster(team_id=13, return_parsed=True)        # → polars
df = espn_nba_team_roster(team_id=13, return_parsed=True,
                          return_as_pandas=True)                 # → pandas
raw = espn_nba_team_roster(team_id=13)                            # → raw Dict (default)
```

This is the shortest path and the right default for most callers.

### 2. Direct parser call (works for any payload)

You can always parse a previously-fetched payload — useful when chaining
calls or operating on cached responses:

```python
from sportsdataverse._common_espn_parsers import parse_team_roster
from sportsdataverse.nba import espn_nba_team_roster

raw = espn_nba_team_roster(team_id=13)
df  = parse_team_roster(raw)
df  = parse_team_roster(raw, return_as_pandas=True)
```

## The 18 parsers

| Parser | Endpoint family | Output shape |
|---|---|---|
| `parse_scoreboard` | `*_scoreboard` | One row per event |
| `parse_teams` | `*_teams_site`, `*_teams_core` | One row per team |
| `parse_standings` | `*_standings`, `*_standings_core` | One row per team standing |
| `parse_groups` | `*_conferences` | One row per group, flattened depth |
| `parse_athlete_overview` | `*_athlete_overview` (Web v3) | Single-entity summary |
| `parse_athlete_stats` | `*_athlete_stats` | One row per stat category |
| `parse_athlete_gamelog` | `*_athlete_gamelog` | One row per game |
| `parse_athlete_splits` | `*_athlete_splits` | One row per split |
| `parse_leaders` | `*_leaders` | One row per (category × leader) |
| `parse_coaches` | `*_coaches`, `*_season_coaches` | One row per coach |
| `parse_draft` | `*_season_draft` | One row per pick |
| `parse_event_competitor_roster` | `*_event_competitor_roster` | One row per athlete |
| `parse_event_competitor_statistics` | `*_event_competitor_statistics` | One row per stat |
| `parse_event_competitor_linescores` | `*_event_competitor_linescores` | One row per period |
| `parse_event_plays` | `*_event_plays` | One row per play |
| `parse_team_schedule` | `*_team_schedule` | One row per event |
| `parse_team_roster` | `*_team_roster` | One row per athlete |
| `parse_news` | `*_news`, `*_team_news`, `*_athlete_news` | One row per article |
| `parse_injuries` | `*_injuries`, `*_team_injuries`, `*_athlete_injuries` | One row per team with injuries |
| `parse_items` | Generic — Core v2 paginated `{items: [...]}` + Core v2 `{entries: [...]}` (athlete_statisticslog, etc.) | One row per item |
| `parse_summary` | Site v2 `summary` (dispatcher) | Dict of 18 sub-frames |
| `parse_summary_boxscore_player` | summary section: per-athlete boxscore | One row per (team × athlete) |
| `parse_summary_boxscore_team` | summary section: per-team boxscore | One row per (team × stat) |
| `parse_summary_plays` | summary section: play-by-play | One row per play |
| `parse_summary_winprobability` | summary section: win-prob over time | One row per probability tick |
| `parse_summary_leaders` | summary section: per-game stat leaders | One row per (team × category × leader) |
| `parse_summary_game_info` | summary section: venue + attendance | Single row |
| `parse_summary_officials` | summary section: refs / umpires | One row per official |
| `parse_summary_header` | summary section: event header + competition shell | Single row |
| `parse_summary_season_series` | summary section: head-to-head series | One row per series entry |
| `parse_summary_against_the_spread` | summary section: per-team ATS records | One row per (team × record) |
| `parse_summary_standings` | summary section: league standings snapshot | One row per team standing |
| `parse_summary_broadcasts` | summary section: TV broadcasts | One row per broadcast (sparse on past games) |
| `parse_summary_format` | summary section: game format (regulation + OT) | Single row |
| `parse_summary_pickcenter` | summary section: pre-game odds / picks | One row per provider (sparse) |
| `parse_summary_odds` | summary section: odds providers / markets | One row per entry (sparse) |
| `parse_summary_article` | summary section: recap article metadata | Single row |
| `parse_summary_injuries` | summary section: per-team injury lists | One row per team |
| `parse_summary_news` | summary section: embedded news feed | One row per article |
| `parse_summary_drives` | summary section: NFL/CFB drives (`drives.previous[]`) | One row per drive |
| `parse_summary_drive_plays` | summary section: NFL/CFB plays nested under drives | One row per play across every drive, with `drive_id` + `drive_sequence` for joinability |
| `parse_summary_scoring_plays` | summary section: NFL/CFB scoring summary | One row per scoring play |

## Contract guarantees

Every parser obeys these rules:

1. **Returns polars by default**; `pandas.DataFrame` via
   `return_as_pandas=True`.
2. **Empty / malformed payloads return a zero-row frame** rather than
   raising — callers guard the `height == 0` case.
3. **Output columns are snake-cased** via
   [`sportsdataverse.dl_utils.underscore`](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/sportsdataverse/dl_utils.py)
   (e.g. `displayName` → `display_name`, `shotsOnGoal` → `shots_on_goal`).
4. **Uses `pandas.json_normalize` for flattening** then converts to
   polars at the end. Mixed-type object columns are stringified to
   keep polars ingestion clean.

## ENDPOINT_PARSERS registry

The registry maps the *short name* in `_common_espn`'s wrapper tables
to its parser. **All 121 wrapper short names are registered** — every
factory-bound wrapper across all 8 leagues plus the hand-bound NCAA
bracketology helpers gains the `return_parsed=True` shim automatically.

```python
>>> from sportsdataverse._common_espn_parsers import ENDPOINT_PARSERS, parser_for
>>> parser_for("scoreboard").__name__
'parse_scoreboard'
>>> parser_for("venue").__name__
'parse_single_entity'
>>> len(ENDPOINT_PARSERS)
121
```

The registry uses three generic parsers as fall-throughs for
endpoint families that share a shape:

- **`parse_single_entity`** — for any Core v2 single-resource payload
  (`team`, `venue`, `franchise`, `coach`, `award`, `position`,
  `season_info`, `athlete_core`, `event_competitor`, etc.). Returns a
  one-row frame flattened via `pandas.json_normalize`.
- **`parse_items`** — for any Core v2 paginated `{items: [...]}` or
  Core v2 `{entries: [...]}` payload (athlete_statisticslog, calendar
  variants, draft, event lists, season_powerindex, talentpicks, etc.).
- **`parse_summary`** — for the rich Site v2 summary dispatcher
  (returns a dict of 20 sub-frames).

## Adding a new parser

1. Write the parser in `sportsdataverse/_common_espn_parsers.py`
   following the existing pattern (empty-payload guard,
   `pd.json_normalize`, `_snake_columns`, `_to_output`).
2. Add an entry to `ENDPOINT_PARSERS` mapping the short name to the
   parser.
3. The `return_parsed=True` shim picks it up automatically on next
   import — no extension-module changes needed.
4. Drop a captured fixture in `tests/fixtures/espn/` and a test in
   `tests/test_espn_universal_parsers.py`.

## See also

- [NHL EDGE parsers](../nhl/edge-parsers) — defensive family parsers
  for the EDGE Statcast surface.
- [ESPN cross-league architecture](../architecture/espn-cross-league) —
  how `make_league_module()` registers each wrapper with its parser.
