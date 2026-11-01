# Package — additional Python functions — IDs and crosswalks

> Package — additional Python functions — IDs and crosswalks — function reference in sdv-py, the SportsDataverse Python package.

### find_athlete {#find_athlete}

`find_athlete(name: 'str', league: 'str', *, team: 'Optional[str]' = None, multi: 'bool' = False) -> 'Union[Optional[Dict[str, Any]], List[Dict[str, Any]]]'`

Resolve an athlete name to ESPN athlete metadata.

Strategy: when a `team=` filter is given, hit that team's roster
endpoint and search there (single fast call); otherwise iterate
every team in the league and search their rosters (slower).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | Athlete identifier — full or partial name. |
| `league` | `str` |  | League slug. |
| `team` | `Optional[str]` | `None` | Optional team filter (passed through to `find_team`). Strongly recommended for performance. |
| `multi` | `bool` | `False` | If `True`, return all matches. |

**Returns**

Single athlete dict or list. Each entry contains the athlete's ESPN ID plus the team they were found on (so callers can immediately call `espn_{league}_athlete_overview(athlete_id=...)` or other athlete-scoped wrappers).

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `league` is not one of the supported slugs. |
| `NoDataError` | The league's teams endpoint answered 404 (it is read to resolve `team=` or, without it, to list the teams to search). |
| `AssetFetchError` | A teams or roster fetch failed (403, rate limit, 5xx, dead connection), so whether the athlete exists is unknown. A roster that answers 404 is skipped. |

**Example**

```python
from sportsdataverse.find import find_athlete
athlete = find_athlete("lebron", league="nba", team="lakers")
print(athlete["id"], athlete["team_id"])

# Every match across the league (one roster call per team)

find_athlete("smith", league="wnba", multi=True)
```

### find_event {#find_event}

`find_event(date: 'str', league: 'str', *, home: 'Optional[str]' = None, away: 'Optional[str]' = None, multi: 'bool' = False) -> 'Union[Optional[Dict[str, Any]], List[Dict[str, Any]]]'`

Resolve a game to its ESPN event ID via the scoreboard endpoint.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `str` |  | YYYYMMDD (e.g. `"20240617"`) or YYYY-MM-DD (e.g. `"2024-06-17"`) — the latter is auto-normalized. |
| `league` | `str` |  | League slug. |
| `home` | `Optional[str]` | `None` | Optional home-team filter (partial name OK). |
| `away` | `Optional[str]` | `None` | Optional away-team filter (partial name OK). |
| `multi` | `bool` | `False` | If `True`, return every event on that date matching the filters (useful for double-headers / Game 1 vs Game 2). |

**Returns**

Single event dict (with the `id` you'd pass to `espn_{league}_summary(event_id=...)` / `espn_{league}_pbp(game_id=...)`) or a list when `multi=True`.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `league` is not one of the supported slugs, or `date` is not `YYYYMMDD` / `YYYY-MM-DD`. |
| `NoDataError` | The scoreboard endpoint answered 404. |
| `AssetFetchError` | The scoreboard fetch failed (403, rate limit, 5xx, dead connection), so the answer is unknown. |

**Example**

```python
from sportsdataverse.find import find_event
event = find_event("2024-06-17", league="nba", home="Boston")
print(event["id"], event["name"])

# Every game on a date (double-headers)

find_event("20240601", league="mlb", multi=True)
```

### find_team {#find_team}

`find_team(name: 'str', league: 'str', *, multi: 'bool' = False) -> 'Union[Optional[Dict[str, Any]], List[Dict[str, Any]]]'`

Resolve a team name to ESPN team metadata.

Performs case-insensitive substring matching against the team's
`displayName`, `location`, `shortDisplayName`, `name`,
`abbreviation`, and `nickname` fields. Returns the first match
by default; pass `multi=True` to get every candidate.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | Team identifier — full or partial name, location, or abbreviation. Case-insensitive. |
| `league` | `str` |  | League slug (`"nba"`, `"wnba"`, `"mbb"`, `"wbb"`, `"cfb"`, `"nfl"`, `"mlb"`, `"nhl"`). |
| `multi` | `bool` | `False` | If `True`, return every match. Default is single-match (first hit; `None` if nothing matches). |

**Returns**

Single team dict (`{id, displayName, abbreviation, location, ...}`) or `None` when nothing matches; or a list of all matches when `multi=True`.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `league` is not one of the supported slugs. |
| `NoDataError` | The teams endpoint answered 404. |
| `AssetFetchError` | The teams fetch failed (403, rate limit, 5xx, dead connection), so the answer is unknown. |

**Example**

```python
from sportsdataverse.find import find_team
team = find_team("lakers", league="nba")
print(team["id"], team["displayName"])  # the raw ESPN team dict (camelCase keys)

# Every candidate

find_team("new", league="nba", multi=True)  # New Orleans Pelicans and New York Knicks
```

### function_count {#function_count}

`function_count(league: 'Optional[str]' = None) -> 'Union[Dict[str, int], int]'`

Count callable functions per league, or in one league.

Cheap way to verify "what's actually in the package" — useful as a
sanity check after pulling a new version.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `None` | A league prefix (`"mlb"`); `None` counts every league. |

**Returns**

dict[str, int] | int: `{league: count}` for every league, or the one league's count when `league` is given.

### list_functions {#list_functions}

`list_functions(league: 'Optional[str]' = None, *, search: 'Optional[str]' = None, parsers_only: 'bool' = False, wrappers_only: 'bool' = False) -> 'Union[Dict[str, List[str]], List[str]]'`

Return an index of callable functions exposed by the package.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `None` | League prefix, as the docs site names it (`"nba"`, `"ufl"`, `"epl"`, `"liiga"`, `"euroleague"`, ...). When given, returns a flat list of function names for that league. With no league, returns a dict keyed by league. |
| `search` | `Optional[str]` | `None` | Case-insensitive substring to filter function names. `search="roster"` matches every `*_roster` / `roster_*` / `parse_*_roster` function across leagues. |
| `parsers_only` | `bool` | `False` | If `True`, restrict the result to `parse_*` callables. |
| `wrappers_only` | `bool` | `False` | If `True`, exclude `parse_*` callables. |

**Returns**

Dict (no league) or list (one league) of function names. Names are sorted alphabetically within each league.
