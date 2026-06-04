---
title: ESPN cross-league architecture
sidebar_label: ESPN cross-league architecture
sidebar_position: 1
---

# ESPN cross-league architecture

`sportsdataverse-py` wraps **800+ ESPN endpoints** across eight leagues
(NBA, MBB, WNBA, WBB, CFB, NFL, MLB, NHL — *NHL also has its own modern
api-web.nhle.com path; see the [NHL section](../nhl/index.md)*) from a
single set of endpoint specs parameterized by the `{sport}`/`{league}`
slugs. This page explains why that's possible and how the wrappers are
generated. For the bigger picture — naming conventions, the R sister
packages, and how this fits the wider ecosystem — see
[Ecosystem & philosophy](../ecosystem.md).

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

## The implementation: declarative codegen

Earlier versions registered the wrappers at import time with a runtime
factory (`make_league_module()` + `functools.partial`). That has been
**retired** in favor of declarative code generation: the wrappers are now
plain, concrete functions written to disk, so they are trivially
greppable, IDE-introspectable, and diff-reviewable.

The endpoint catalog lives as YAML under `tools/codegen/endpoints/`. One
spec per ESPN API surface describes each endpoint once — its path, params,
parser, and example — using the `{sport}`/`{league}` template:

```yaml
# tools/codegen/endpoints/espn_site_v2.yaml  (excerpt)
- short: scoreboard
  path: /{sport}/{league}/scoreboard
  parser: parse_scoreboard
  returns_schema: scoreboard
```

`python tools/codegen/generate.py` renders those specs into one concrete
module per league — `sportsdataverse/<league>/<league>_espn_ext.py` —
substituting the slugs and applying the naming conventions (below):

```python
# sportsdataverse/nba/nba_espn_ext.py   — GENERATED, do not edit
def espn_nba_scoreboard(dates=None, ..., *, return_parsed=False,
                        return_as_pandas=False, **kwargs) -> Dict:
    raw = _get("https://site.api.espn.com/.../basketball/nba/scoreboard",
               params={...}, **kwargs)
    if return_parsed:
        return parse_scoreboard(raw, return_as_pandas=return_as_pandas)
    return raw
```

Result: `from sportsdataverse.nba import espn_nba_scoreboard` works, IDE
auto-complete lists every wrapper, and `help()` / `inspect.signature()`
show real signatures. A `--check` drift gate (run in CI and as a
pre-commit hook) fails if the committed modules fall out of sync with the
YAML — and the same generator emits these very reference docs via
`generate.py --docs`. See the
[codegen toolchain notes in `CLAUDE.md`](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/CLAUDE.md)
for the full workflow.

Wrappers whose endpoint has a registered parser additionally take two
optional kwargs (`return_parsed` / `return_as_pandas`), described next.

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

Each wrapper is a concrete, generated function, so IDE auto-complete,
`help()`, and `inspect.signature()` behave like any hand-written function:

```python
>>> from sportsdataverse.nba import espn_nba_player_overview
>>> espn_nba_player_overview.__name__
'espn_nba_player_overview'
>>> help(espn_nba_player_overview)
# The generated docstring: endpoint URL, args, return type, example.
```

Note the name: ESPN's raw `athletes/{id}/overview` endpoint surfaces as
`espn_nba_player_overview`, not `..._athlete_overview` — see the naming
conventions below.

## Naming conventions

The generator aligns ESPN's raw taxonomy to the cfbfastR/hoopR/wehoop
vocabulary, applied to **every** league:

- **Token renames:** `athlete → player`, `event → game` (with plurals), so
  `athletes/{id}` → `espn_<league>_player_info`, `events` → `..._games`.
- **Combined renames:** an `event_competitor` is a game's team
  (`event_competitor* → game_team*`); `event_competition → game_competition`.
- **Collision resolution:** when a rename would clash, one endpoint keeps
  the bare name and the other is version-qualified — so every league has a
  bare `espn_<league>_player_stats()` (season stats) plus a comprehensive
  `espn_<league>_player_stats_v3()`.

## Per-league function counts

| League | Generated `espn_*` wrappers | Hand-written originals | Total |
|---|---:|---:|---:|
| NBA  | 113 |  5 | **118** |
| MBB  | 116 |  5 | **121** |
| WNBA | 113 | 11 | **124** |
| WBB  | 116 | 10 | **126** |
| CFB  | 118 |  5 | **123** |
| NFL  | 115 |  4 | **119** |
| MLB  | 113 |  5 | **118** |
| NHL  | (separate api-web.nhle.com surface — see [NHL section](../nhl/index.md)) |

(Exact per-API counts are in each league's **Reference** section, which is
generated from the same specs.)

Beyond the vocabulary alignment above, the surface diverges from the R
packages (hoopR/wehoop/cfbfastR) in one deliberate way: where R collapses
multiple `/teams` paths into a single function with branching internals,
sdv-py exposes them as distinct functions (`espn_<league>_teams_site`,
`..._season_teams`, `..._season_team`) so the caller picks the surface
they want. See [Ecosystem & philosophy](../ecosystem.md) for the full
Python ↔ R mapping.

## See also

- [Ecosystem & philosophy](../ecosystem.md) — the design philosophy, the
  full naming paradigm, and the R/Python/Node sister packages.
- [The parser layer](../parsers/index.md) — how `ENDPOINT_PARSERS` is
  built, what each parser does, and how to extend the registry.
- [NHL section](../nhl/index.md) — NHL gets its own modern
  `api-web.nhle.com/v1/` surface plus EDGE Statcast, Stats REST, and
  Records modules.
- [MLB section](../mlb/index.md) — MLB pairs the ESPN cross-league
  wrappers with the official MLB Stats API and Baseball Savant
  (Statcast) wrappers.
