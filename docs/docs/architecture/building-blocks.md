---
title: Building blocks
sidebar_label: Building blocks
sidebar_position: 2
---

# Building blocks

The same five low-level patterns power every parser module in the
package. This page documents them so contributors who want to add a
new endpoint or surface a new parser know which building block to
reach for.

## 1. The `_bind` shim factory

[`sportsdataverse._common_espn._bind`](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/sportsdataverse/_common_espn.py)
is the closure factory that powers the ESPN cross-league surface.
It takes a core `(sport, league, ...)` function and returns either:

- A `functools.partial` with `(sport, league)` baked in — for
  wrappers without a registered parser. Acts identically to a normal
  function for IDE introspection (custom `__name__`/`__qualname__`/
  `__doc__`).
- A **shim closure** — for wrappers with a registered parser. Adds
  optional `return_parsed=False` and `return_as_pandas=False` kwargs
  that route the raw response through the parser when set.

```python
def _bind(core_fn, sport, league, full_name, parser=None):
    bound = partial(core_fn, sport, league)
    if parser is None:
        bound.__name__ = full_name
        bound.__doc__  = base_doc + binding_note
        return bound

    def wrapper(*args, return_parsed=False,
                return_as_pandas=False, **kwargs):
        result = bound(*args, **kwargs)
        if return_parsed:
            return parser(result, return_as_pandas=return_as_pandas)
        return result
    wrapper.__name__ = full_name
    wrapper.__doc__  = base_doc + binding_note + parser_hint
    return wrapper
```

**When to use**: writing a new core function for `_common_espn.py`
that should be exposed across multiple leagues. Don't call `_bind`
directly — add to one of the `_*_WRAPPERS` tables and
`make_league_module` calls it for you.

## 2. `make_league_module` — factory pattern

```python
# sportsdataverse/{league}/{league}_espn_ext.py
from sportsdataverse._common_espn import make_league_module

__all__ = make_league_module(
    sport, league, prefix, globals(),
    include_ncaa=True,      # adds rankings / season_recruits / season_week_rankings
    include_football=True,  # adds season_qbr / season_qbr_week
    include_mlb=False,      # adds athlete_hotzones (MLB-only)
)
```

Iterates `_UNIVERSAL_WRAPPERS` (+ optional `_NCAA_WRAPPERS` /
`_FOOTBALL_WRAPPERS` / `_MLB_WRAPPERS`), calls `_bind()` per entry,
registers each result in the caller's namespace with the canonical
`espn_{prefix}_{short}` name. Returns the list of registered names
for the caller to assign to `__all__`.

**When to use**: adding a new league extension module. The whole
file is typically 4-5 lines.

## 3. `_row_per_item` + `_single_row` helpers

The two universal `pandas.json_normalize` wrappers that every parser
module re-implements identically (with slight name variations):

```python
def _row_per_item(items, return_as_pandas):
    """Flatten list-of-dicts → tidy frame.
    Zero-row frame on empty input. Stringifies list-valued cells
    so polars accepts the frame."""

def _single_row(payload_dict, return_as_pandas):
    """Flatten a dict → single-row frame.
    Zero-row frame on empty/non-dict input. Same list-stringification."""
```

Both run the input through `pd.json_normalize(..., sep="_")`,
stringify any list-valued cells (polars rejects mixed-type object
columns), snake-case the columns via
`sportsdataverse.dl_utils.underscore`, and convert to polars (or
pandas if `return_as_pandas=True`).

**When to use**: writing any new parser. Reach for the helper
first, then add endpoint-specific unrolling only if the payload
has nested structures the generic flatten won't handle.

## 4. The `ENDPOINT_PARSERS` registry pattern

Every parser module exposes a name → callable dict + a
`parser_for_*` lookup function:

```python
# In each parsers module
ENDPOINT_PARSERS = {                       # or NHL_API_WEB_ENDPOINT_PARSERS, etc.
    "scoreboard":     parse_scoreboard,
    "teams_site":     parse_teams,
    # ...
}

def parser_for(short_name):
    """Return the registered parser or fall-through default."""
    return ENDPOINT_PARSERS.get(short_name, parse_items)   # never None
```

**Two fall-through conventions** across the parser modules:

- **Never-`None`**: `parser_for_mlb_api`, `parser_for_nhl_stats_rest`,
  `parser_for_nhl_records`, the ESPN cross-league `parser_for`. All
  fall back to a sensible generic parser so callers don't need
  null-checks.
- **`None`-on-unknown**: `parser_for_nhl_api_web` and
  `parser_for_edge` — used when there's no useful generic
  fall-through (e.g. NHL api-web `playoff_series` is too
  idiosyncratic for a generic flattener). Callers null-check.

**When to use**: adding a new parser module. Mirror one of the
existing modules — `mlb_api_parsers.py` is the simplest reference.

## 5. The dispatcher pattern

Used when an endpoint ships multiple unrelated sub-frames in one
payload. Returns a dict by default, or a single frame when
`section="<name>"`:

```python
def parse_X(payload, section=None, return_as_pandas=False):
    sub_parsers = {
        "boxscore_player": parse_X_boxscore_player,
        "plays":           parse_X_plays,
        # ...
    }
    if section is not None:
        if section not in sub_parsers:
            raise ValueError(
                f"Unknown section {section!r}. "
                f"Choose one of {sorted(sub_parsers)} or pass section=None."
            )
        return sub_parsers[section](payload, return_as_pandas=return_as_pandas)
    return {name: fn(payload, return_as_pandas=return_as_pandas)
            for name, fn in sub_parsers.items()}
```

Current dispatchers in the package:

| Function | Sections | Endpoint |
|---|---:|---|
| [`parse_summary`](../parsers/index.md) | 21 | ESPN Site v2 `summary` |
| [`parse_nhl_web_right_rail`](../nhl/reference/nhl_api_web.md) | 6 | NHL `gamecenter/{id}/right-rail` |
| [`parse_nhl_web_club_stats`](../nhl/reference/nhl_api_web.md) | 2 | NHL `club-stats/{team}/{season}/{game_type}` |

**When to use**: a new endpoint whose payload would force the
caller to extract 3+ unrelated sub-frames manually. The dispatcher
pattern saves that boilerplate.

## Adding a new parser — checklist

When adding a parser to any of the 5 parser modules:

1. **Capture a fixture** — drop a representative JSON payload in
   the matching `tests/fixtures/{module}/` directory. Document
   provenance in the directory's `README.md`.
2. **Write the parser** — reuse `_row_per_item` / `_single_row` if
   possible; add endpoint-specific unrolling only for nested
   structures the helpers won't flatten.
3. **Register it** — add to the module's `ENDPOINT_PARSERS` dict
   (or equivalent). The shim wires up automatically on next
   import.
4. **Add a test** — at minimum: row-count assertion against the
   fixture + empty-payload contract + pandas opt-in. Match the
   parametrize style of the existing tests for cross-league
   consistency.
5. **Update the docs** — add a row to the parser table in
   `docs/docs/parsers/index.md` (or the module's dedicated doc
   page).
6. **CHANGELOG entry** — note the new parser + which short names
   it covers in the `0.0.51 (unreleased)` section.

## See also

- [ESPN cross-league architecture](./espn-cross-league) — the
  factory / shim pattern in full.
- [Parsers overview](../parsers/index.md) — the registry contract +
  the 3 generic fall-throughs.
- [Test fixtures index](../parsers/fixtures) — the 89-capture
  inventory.
