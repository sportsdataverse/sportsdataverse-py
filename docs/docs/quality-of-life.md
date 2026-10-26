---
title: Quality-of-life helpers
sidebar_label: Quality-of-life
sidebar_position: 3
description: "Name-to-ID resolvers (find_team, find_athlete, find_event) and a searchable function index (list_functions, function_count) for sportsdataverse-py."
---

# Quality-of-life helpers

Three habits and helpers that make the package easier to use day to day.

## 1. DataFrames by default

The `sportsdataverse.{league}` modules return a parsed polars DataFrame for
parser-backed endpoints; pass `return_parsed=False` for the raw `Dict`. Import
from the league module:

```python
from sportsdataverse.nba import espn_nba_scoreboard
df  = espn_nba_scoreboard()                     # → polars DataFrame
raw = espn_nba_scoreboard(return_parsed=False)  # → Dict
pdf = espn_nba_scoreboard(return_as_pandas=True)  # → pandas DataFrame
```

:::caution[Deprecated]
The old `sportsdataverse.parsed.*` namespace is **deprecated**. It survives only
as a generated back-compat alias layer over the league modules.
:::

To migrate, replace `from sportsdataverse.parsed.nba import ...` with
`from sportsdataverse.nba import ...`; nothing else changes. The old
`parsed.nba`, `parsed.wnba`, `parsed.mbb`, `parsed.wbb`, `parsed.cfb`,
`parsed.nfl`, `parsed.mlb` and `parsed.nhl` modules still import, but each
emits a `DeprecationWarning` and will be removed in a future release.

## 2. `find_team` / `find_athlete` / `find_event` — name-to-ID resolvers

ESPN wrappers expect numeric IDs (`team_id`, `athlete_id`, `event_id`).
New users almost always start with a name and have to detour through
a teams-list / search call. The find helpers collapse that step:

```python
from sportsdataverse import find_team, find_athlete, find_event

# Team name → ID
find_team("lakers", league="nba")["id"]                 # '13'
find_team("LAL", league="nba")["abbreviation"]          # 'LAL'

# Athlete name → ID (use team filter for speed)
find_athlete("LeBron", league="nba", team="lakers")["id"]   # '1966'
find_athlete("Aaron Judge", league="mlb", team="Yankees")["id"]  # '33192'

# Game date + teams → event ID
find_event(date="2024-03-17", league="nba", home="Orlando")["id"]
# '401585607' — regular season, Toronto at Orlando

find_event(date="20250209", league="nfl", away="KC")["name"]
# 'Kansas City Chiefs at Philadelphia Eagles' — Super Bowl LIX
```

All three support `multi=True` to get every match instead of just the
first. Matching is case-insensitive substring against the relevant
fields (displayName, location, abbreviation, etc.).

**Caching**: the team list per league is cached in-process to avoid
repeating the teams-site call on every lookup. Call
`sportsdataverse.clear_team_cache(league)` to refresh after a
mid-season change (trade, expansion, etc.).

## 3. `list_functions` / `function_count` — searchable function index

The package exports thousands of functions across more than a hundred
leagues and providers. `dir()` produces a flat list that's hard to scan;
`list_functions` is the searchable index.

```python
from sportsdataverse import list_functions, function_count

# Per-league counts (a dict keyed by league prefix)
counts = function_count()
counts["nba"], counts["mlb"]

# Search across leagues — "what wrappers exist for play-by-play?"
list_functions(search="pbp")
# {'cfb': ['load_cfb_pbp'],
#  'mbb': ['espn_mbb_pbp', 'helper_mbb_pbp', ...],
#  'nhl': ['espn_nhl_pbp', 'nhl_web_pbp', 'parse_nhl_web_pbp', ...]}

# Filter to one league + one prefix
list_functions(league="nba", search="leader")
# ['espn_nba_leaders', 'espn_nba_leaders_core',
#  'espn_nba_season_type_leaders', ...]

# Just the parsers from a module
list_functions(league="mlb", parsers_only=True)
# ['parse_mlb_api_list', 'parse_mlb_api_person_stats',
#  'parse_mlb_api_schedule', 'parse_mlb_api_standings', ...]

# Just the wrappers (exclude parse_*)
list_functions(league="mlb", wrappers_only=True)
```

## Putting it all together

The three helpers combine cleanly. A two-line end-to-end query:

```python
from sportsdataverse import find_event
from sportsdataverse.nba import espn_nba_summary

event = find_event(date="2024-06-17", league="nba", home="Boston")
out = espn_nba_summary(event_id=event["id"])     # dict of 21 sub-frames
out["boxscore_player"].head()                    # tidy polars frame
```

Without the helpers the equivalent is four steps:

```python
# Find scoreboard for the date
scoreboard = espn_nba_scoreboard(dates=20240617, return_parsed=False)

# Manually filter events for the right home team
target = next(
    e for e in scoreboard["events"]
    if any(c["team"]["displayName"] == "Boston Celtics"
           and c["homeAway"] == "home"
           for c in e["competitions"][0]["competitors"])
)
event_id = target["id"]

# Pull summary as raw Dict
raw = espn_nba_summary(event_id=event_id, return_parsed=False)

# Manually parse the boxscore section
from sportsdataverse._common_espn_parsers import parse_summary_boxscore_player
df = parse_summary_boxscore_player(raw)
```

## See also

- [Architecture overview](./architecture/espn-cross-league)
- [Parser layer](./parsers/index.md)
- [Migrating to 0.2.0](./guides/migration-0-2-0.md)
