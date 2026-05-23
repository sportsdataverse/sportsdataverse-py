---
title: ESPN cross-league architecture
sidebar_label: ESPN cross-league architecture
sidebar_position: 1
---

# ESPN cross-league architecture

`sportsdataverse-py` wraps **804 ESPN endpoints** across eight leagues
(NBA, MBB, WNBA, WBB, CFB, NFL, MLB, NHL — *NHL via its own modern
api-web.nhle.com path; see the [NHL section](../nhl/index)*) with a
single core of ~80 `(sport, league)`-parameterized functions. This page
explains why that's possible and how to use the resulting surface.

## The observation that powers everything

Every ESPN API path follows the same template across sports — only the
`{sport}` and `{league}` slugs change:

```
https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/scoreboard
https://sports.core.api.espn.com/v2/sports/{sport}/leagues/{league}/seasons/{year}
https://site.web.api.espn.com/apis/common/v3/sports/{sport}/{league}/athletes/{athleteId}/overview
```

| API surface | Base | Wrappers per league |
|---|---|---|
| Site v2 | `site.api.espn.com/apis/site/v2/...` | 29 |
| Site v2 alt | `site.api.espn.com/apis/v2/...` | 1 (standings) |
| Web v3 | `site.web.api.espn.com/apis/common/v3/...` | 5 (athlete deep dives + leaders) |
| Core v2 | `sports.core.api.espn.com/v2/...` | 50 |
| **Total universal** | | **84** wrappers per league |
| NCAA-only extras | (3 wrappers) | enabled for `mbb`, `wbb`, `cfb` |
| Football-only extras | (2 wrappers — QBR) | enabled for `nfl`, `cfb` |
| MLB-only extras | (1 wrapper — `athlete_hotzones`) | enabled for `mlb` |

## The implementation: one core, 8 thin extension modules

```python
# sportsdataverse/_common_espn.py  — ~80 cross-league core functions
def _site_v2_scoreboard(sport: str, league: str, **kwargs) -> Dict:
    return _get(f"{_SITE_V2}/{sport}/{league}/scoreboard", **kwargs)

def _core_v2_season_info(sport: str, league: str, season, **kwargs) -> Dict:
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}", **kwargs)
# ...80 more
```

Each per-league extension module is a 5-line file that calls
`make_league_module()` to mass-register the wrappers as IDE-discoverable
attributes:

```python
# sportsdataverse/nba/nba_espn_ext.py
from sportsdataverse._common_espn import make_league_module
__all__ = make_league_module("basketball", "nba", "nba", globals())

# sportsdataverse/mbb/mbb_espn_ext.py
__all__ = make_league_module(
    "basketball", "mens-college-basketball", "mbb", globals(),
    include_ncaa=True,
)

# sportsdataverse/mlb/mlb_espn_ext.py
__all__ = make_league_module(
    "baseball", "mlb", "mlb", globals(),
    include_mlb=True,  # registers athlete_hotzones
)
```

Result: `from sportsdataverse.nba import espn_nba_scoreboard` works,
IDE auto-complete on `sportsdataverse.nba.<TAB>` lists all 113 wrappers,
and `help(espn_nba_scoreboard)` shows the core docstring augmented with
the bound `(sport, league)` pair.

## The bind helper (under the hood)

`make_league_module()` walks the wrapper tables and calls a `_bind()`
helper that does two things:

1. **Returns a `functools.partial`** of the core function with
   `(sport, league)` baked in.
2. **Sets `__name__` / `__qualname__` / `__doc__`** explicitly so the
   partial behaves like a real function for `help()`, IDE
   introspection, and `inspect.signature()`.

Wrappers whose `short` name has a registered parser in
[`ENDPOINT_PARSERS`](../parsers/index) gain a third treatment: instead
of a plain partial, they're wrapped in a closure that adds two optional
kwargs.

## The `return_parsed=True` shim

Every wrapper with a registered parser accepts an optional
`return_parsed=True` kwarg that dispatches the raw response through the
parser, returning a polars DataFrame (or pandas via
`return_as_pandas=True`):

```python
from sportsdataverse.nba import espn_nba_teams_site, espn_nba_scoreboard

# Default: raw Dict (unchanged from pre-shim API)
raw  = espn_nba_teams_site()                # → Dict
print(raw["sports"][0]["leagues"][0]["teams"][0]["team"]["displayName"])

# Opt-in: polars DataFrame
df   = espn_nba_teams_site(return_parsed=True)
print(df.select(["team_id", "team_abbreviation", "team_display_name"]).head())

# Opt-in: pandas DataFrame
pdf  = espn_nba_teams_site(return_parsed=True, return_as_pandas=True)
```

The shim is **backwards-compatible by design** — every existing caller
continues to get raw `Dict`. The two parsing kwargs are additive.

### Wrappers WITHOUT a parser

If you call a wrapper whose short name isn't in `ENDPOINT_PARSERS`
(e.g. `espn_nba_league_notes`), there's no `return_parsed` kwarg — the
wrapper stays a plain partial that returns raw `Dict`. You can still
pass the result through any parser manually:

```python
from sportsdataverse._common_espn_parsers import parse_items
from sportsdataverse.nba import espn_nba_venues

raw = espn_nba_venues(limit=10)
df  = parse_items(raw)                       # works on any {items: [...]} payload
```

## Function-name discoverability

Each registered wrapper is a real attribute on its module, so IDE
auto-complete works the same as any other function:

```python
>>> from sportsdataverse.nba import espn_nba_athlete_overview
>>> espn_nba_athlete_overview.__name__
'espn_nba_athlete_overview'
>>> help(espn_nba_athlete_overview)
# Pulls the core docstring + the (sport, league) binding note + the
# parser hint (if applicable).
```

## Per-league function counts

| League | espn_*_* wrappers (factory) | Originals | Total |
|---|---:|---:|---:|
| NBA  | 113 |  5 | **118** |
| MBB  | 116 |  5 | **121** |
| WNBA | 113 | 11 | **124** |
| WBB  | 116 | 10 | **126** |
| CFB  | 118 |  5 | **123** |
| NFL  | 115 |  4 | **119** |
| MLB  | 113 |  5 | **118** |
| NHL  | (separate api-web.nhle.com surface — see [NHL section](../nhl/index)) |

The naming convention diverges slightly from the R packages
(hoopR/wehoop/cfbfastR): where R collapses multiple `/teams` paths into
one function with branching internals, sdv-py exposes them as distinct
functions (`_teams_site`, `_teams_core`, `_season_teams`,
`_season_team`) so the caller picks the surface they want.

## See also

- [The parser layer](../parsers/index) — how `ENDPOINT_PARSERS` is
  built, what each parser does, and how to extend the registry.
- [NHL section](../nhl/index) — NHL gets its own modern
  `api-web.nhle.com/v1/` surface plus EDGE Statcast, Stats REST, and
  Records modules.
- [MLB section](../mlb/index) — MLB pairs the ESPN cross-league
  wrappers with the official MLB Stats API and Baseball Savant
  (Statcast) wrappers.
