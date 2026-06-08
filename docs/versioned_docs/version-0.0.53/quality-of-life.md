---
title: Quality-of-life helpers
sidebar_label: Quality-of-life
sidebar_position: 3
---

# Quality-of-life helpers

Three top-level helpers added in 0.0.51 that make the package
significantly easier to use day-to-day.

## 1. `sportsdataverse.parsed.*` — DataFrame by default

The default `sportsdataverse.{league}` modules return raw `Dict` —
the parser layer is opt-in via `return_parsed=True` so existing
callers from 0.0.50 and earlier are unaffected. **The
`sportsdataverse.parsed.{league}` namespace flips that default**:

```python
# Raw-Dict default (existing API, unchanged):
from sportsdataverse.nba import espn_nba_scoreboard
raw = espn_nba_scoreboard()                # → Dict

# DataFrame default (new namespace):
from sportsdataverse.parsed.nba import espn_nba_scoreboard
df = espn_nba_scoreboard()                 # → polars DataFrame

# Override either direction:
df  = espn_nba_scoreboard(return_parsed=True)   # works from raw module
raw = espn_nba_scoreboard(return_parsed=False)  # works from parsed module
```

Every league has a parsed variant: `parsed.nba`, `parsed.wnba`,
`parsed.mbb`, `parsed.wbb`, `parsed.cfb`, `parsed.nfl`, `parsed.mlb`,
`parsed.nhl`. Wrappers that don't have a registered parser (helpers,
loaders, etc.) pass through unchanged.

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
find_event(date="2024-06-17", league="nba", home="Boston")["id"]
# '401585607' — 2024 NBA Finals G5

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

The package exposes ~1,340 callables across 8 leagues. `dir()`
produces a flat list that's hard to scan; `list_functions` is the
searchable index.

```python
from sportsdataverse import list_functions, function_count

# Per-league counts
function_count()
# {'cfb': 149, 'mbb': 146, 'mlb': 196, 'nba': 143, 'nfl': 208,
#  'nhl': 199, 'wbb': 151, 'wnba': 148}

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
from sportsdataverse.parsed.nba import espn_nba_summary

event = find_event(date="2024-06-17", league="nba", home="Boston")
out = espn_nba_summary(event_id=event["id"])     # dict of 21 sub-frames
out["boxscore_player"].head()                    # tidy polars frame
```

Before 0.0.51 the equivalent was four steps:

```python
# Find scoreboard for the date
scoreboard = espn_nba_scoreboard(dates=20240617)

# Manually filter events for the right home team
target = next(
    e for e in scoreboard["events"]
    if any(c["team"]["displayName"] == "Boston Celtics"
           and c["homeAway"] == "home"
           for c in e["competitions"][0]["competitors"])
)
event_id = target["id"]

# Pull summary as raw Dict
raw = espn_nba_summary(event_id=event_id)

# Manually parse the boxscore section
from sportsdataverse._common_espn_parsers import parse_summary_boxscore_player
df = parse_summary_boxscore_player(raw)
```

## See also

- [Architecture overview](./architecture/espn-cross-league)
- [Parser layer](./parsers/index.md)
- [Building blocks](./architecture/building-blocks)
