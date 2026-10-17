---
title: ATRIUM — additional Python functions
sidebar_label: Additional functions
description: "ATRIUM — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# ATRIUM — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.basketball.atrium`
not covered by the generated API-endpoint reference above.

## Sportradar Atrium embed API

### lmb_pbp {#lmb_pbp}

`lmb_pbp(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of a Liga Mayor de Baloncesto (El Salvador) game.

Liga Mayor de Baloncesto (El Salvador) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_pbp` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lmb_pbp
lmb_pbp(fixture_id='<fixture uuid from lmb_schedule>')
```

### lmb_player_box {#lmb_player_box}

`lmb_player_box(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of a Liga Mayor de Baloncesto (El Salvador) game.

Liga Mayor de Baloncesto (El Salvador) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_box_score` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lmb_player_box
lmb_player_box(fixture_id='<fixture uuid from lmb_schedule>')
```

### lmb_schedule {#lmb_schedule}

`lmb_schedule(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Fixtures (schedule and results) of a Liga Mayor de Baloncesto (El Salvador) season.

Liga Mayor de Baloncesto (El Salvador) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_fixtures` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season END year (see the season rule above). Omit season and season_id for the site's current season. |
| `season_id` | `Optional[str]` | `None` | Season UUID (season_id in the seasons listing); skips the END-year lookup. |
| `competition_id` | `Optional[str]` | `None` | Competition UUID (a site carries many: conferences, cups); needed when several share the season. |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lmb_schedule
lmb_schedule(season=2026)
```

### lmb_seasons {#lmb_seasons}

`lmb_seasons(locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every season and competition id of Liga Mayor de Baloncesto (El Salvador).

Liga Mayor de Baloncesto (El Salvador) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_seasons` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lmb_seasons
lmb_seasons()
```

### lmb_shots {#lmb_shots}

`lmb_shots(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of a Liga Mayor de Baloncesto (El Salvador) game.

Liga Mayor de Baloncesto (El Salvador) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_shots` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lmb_shots
lmb_shots(fixture_id='<fixture uuid from lmb_schedule>')
```

### lmb_standings {#lmb_standings}

`lmb_standings(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Standings of a Liga Mayor de Baloncesto (El Salvador) season.

Liga Mayor de Baloncesto (El Salvador) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_standings` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season END year (see the season rule above). Omit season and season_id for the site's current season. |
| `season_id` | `Optional[str]` | `None` | Season UUID (season_id in the seasons listing); skips the END-year lookup. |
| `competition_id` | `Optional[str]` | `None` | Competition UUID (a site carries many: conferences, cups); needed when several share the season. |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lmb_standings
lmb_standings(season=2026)
```

### lmb_team_box {#lmb_team_box}

`lmb_team_box(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of a Liga Mayor de Baloncesto (El Salvador) game.

Liga Mayor de Baloncesto (El Salvador) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_team_box_score` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lmb_team_box
lmb_team_box(fixture_id='<fixture uuid from lmb_schedule>')
```

### lmb_teams {#lmb_teams}

`lmb_teams(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Teams of a Liga Mayor de Baloncesto (El Salvador) season.

Liga Mayor de Baloncesto (El Salvador) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_teams` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season END year (see the season rule above). Omit season and season_id for the site's current season. |
| `season_id` | `Optional[str]` | `None` | Season UUID (season_id in the seasons listing); skips the END-year lookup. |
| `competition_id` | `Optional[str]` | `None` | Competition UUID (a site carries many: conferences, cups); needed when several share the season. |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lmb_teams
lmb_teams(season=2026)
```

### lnb_pbp {#lnb_pbp}

`lnb_pbp(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of a LNB (France) game.

LNB (France) (men's); season rule: END year (2026 = 2025-26; the site's own year is the START year).

Convenience wrapper: a thin binding of `atrium_pbp` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lnb_pbp
lnb_pbp(fixture_id='<fixture uuid from lnb_schedule>')
```

### lnb_player_box {#lnb_player_box}

`lnb_player_box(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of a LNB (France) game.

LNB (France) (men's); season rule: END year (2026 = 2025-26; the site's own year is the START year).

Convenience wrapper: a thin binding of `atrium_box_score` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lnb_player_box
lnb_player_box(fixture_id='<fixture uuid from lnb_schedule>')
```

### lnb_schedule {#lnb_schedule}

`lnb_schedule(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Fixtures (schedule and results) of a LNB (France) season.

LNB (France) (men's); season rule: END year (2026 = 2025-26; the site's own year is the START year).

Convenience wrapper: a thin binding of `atrium_fixtures` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season END year (see the season rule above). Omit season and season_id for the site's current season. |
| `season_id` | `Optional[str]` | `None` | Season UUID (season_id in the seasons listing); skips the END-year lookup. |
| `competition_id` | `Optional[str]` | `None` | Competition UUID (a site carries many: conferences, cups); needed when several share the season. |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lnb_schedule
lnb_schedule(season=2026)
```

### lnb_seasons {#lnb_seasons}

`lnb_seasons(locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every season and competition id of LNB (France).

LNB (France) (men's); season rule: END year (2026 = 2025-26; the site's own year is the START year).

Convenience wrapper: a thin binding of `atrium_seasons` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lnb_seasons
lnb_seasons()
```

### lnb_shots {#lnb_shots}

`lnb_shots(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of a LNB (France) game.

LNB (France) (men's); season rule: END year (2026 = 2025-26; the site's own year is the START year).

Convenience wrapper: a thin binding of `atrium_shots` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lnb_shots
lnb_shots(fixture_id='<fixture uuid from lnb_schedule>')
```

### lnb_standings {#lnb_standings}

`lnb_standings(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Standings of a LNB (France) season.

LNB (France) (men's); season rule: END year (2026 = 2025-26; the site's own year is the START year).

Convenience wrapper: a thin binding of `atrium_standings` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season END year (see the season rule above). Omit season and season_id for the site's current season. |
| `season_id` | `Optional[str]` | `None` | Season UUID (season_id in the seasons listing); skips the END-year lookup. |
| `competition_id` | `Optional[str]` | `None` | Competition UUID (a site carries many: conferences, cups); needed when several share the season. |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lnb_standings
lnb_standings(season=2026)
```

### lnb_team_box {#lnb_team_box}

`lnb_team_box(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of a LNB (France) game.

LNB (France) (men's); season rule: END year (2026 = 2025-26; the site's own year is the START year).

Convenience wrapper: a thin binding of `atrium_team_box_score` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lnb_team_box
lnb_team_box(fixture_id='<fixture uuid from lnb_schedule>')
```

### lnb_teams {#lnb_teams}

`lnb_teams(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Teams of a LNB (France) season.

LNB (France) (men's); season rule: END year (2026 = 2025-26; the site's own year is the START year).

Convenience wrapper: a thin binding of `atrium_teams` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season END year (see the season rule above). Omit season and season_id for the site's current season. |
| `season_id` | `Optional[str]` | `None` | Season UUID (season_id in the seasons listing); skips the END-year lookup. |
| `competition_id` | `Optional[str]` | `None` | Competition UUID (a site carries many: conferences, cups); needed when several share the season. |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lnb_teams
lnb_teams(season=2026)
```

### lnbp_pbp {#lnbp_pbp}

`lnbp_pbp(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) game.

LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_pbp` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lnbp_pbp
lnbp_pbp(fixture_id='<fixture uuid from lnbp_schedule>')
```

### lnbp_player_box {#lnbp_player_box}

`lnbp_player_box(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) game.

LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_box_score` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lnbp_player_box
lnbp_player_box(fixture_id='<fixture uuid from lnbp_schedule>')
```

### lnbp_schedule {#lnbp_schedule}

`lnbp_schedule(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Fixtures (schedule and results) of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) season.

LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_fixtures` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season END year (see the season rule above). Omit season and season_id for the site's current season. |
| `season_id` | `Optional[str]` | `None` | Season UUID (season_id in the seasons listing); skips the END-year lookup. |
| `competition_id` | `Optional[str]` | `None` | Competition UUID (a site carries many: conferences, cups); needed when several share the season. |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lnbp_schedule
lnbp_schedule(season=2026)
```

### lnbp_seasons {#lnbp_seasons}

`lnbp_seasons(locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every season and competition id of LNBP (Mexico, with COPA VALUE and LNBP FEMENIL).

LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_seasons` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lnbp_seasons
lnbp_seasons()
```

### lnbp_shots {#lnbp_shots}

`lnbp_shots(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) game.

LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_shots` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lnbp_shots
lnbp_shots(fixture_id='<fixture uuid from lnbp_schedule>')
```

### lnbp_standings {#lnbp_standings}

`lnbp_standings(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Standings of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) season.

LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_standings` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season END year (see the season rule above). Omit season and season_id for the site's current season. |
| `season_id` | `Optional[str]` | `None` | Season UUID (season_id in the seasons listing); skips the END-year lookup. |
| `competition_id` | `Optional[str]` | `None` | Competition UUID (a site carries many: conferences, cups); needed when several share the season. |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lnbp_standings
lnbp_standings(season=2026)
```

### lnbp_team_box {#lnbp_team_box}

`lnbp_team_box(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) game.

LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_team_box_score` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lnbp_team_box
lnbp_team_box(fixture_id='<fixture uuid from lnbp_schedule>')
```

### lnbp_teams {#lnbp_teams}

`lnbp_teams(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Teams of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) season.

LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_teams` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season END year (see the season rule above). Omit season and season_id for the site's current season. |
| `season_id` | `Optional[str]` | `None` | Season UUID (season_id in the seasons listing); skips the END-year lookup. |
| `competition_id` | `Optional[str]` | `None` | Competition UUID (a site carries many: conferences, cups); needed when several share the season. |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lnbp_teams
lnbp_teams(season=2026)
```

### lpb_pbp {#lpb_pbp}

`lpb_pbp(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of a LPB / LPBF Liga Profesional de Baloncesto (Panama) game.

LPB / LPBF Liga Profesional de Baloncesto (Panama) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_pbp` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lpb_pbp
lpb_pbp(fixture_id='<fixture uuid from lpb_schedule>')
```

### lpb_player_box {#lpb_player_box}

`lpb_player_box(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of a LPB / LPBF Liga Profesional de Baloncesto (Panama) game.

LPB / LPBF Liga Profesional de Baloncesto (Panama) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_box_score` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lpb_player_box
lpb_player_box(fixture_id='<fixture uuid from lpb_schedule>')
```

### lpb_schedule {#lpb_schedule}

`lpb_schedule(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Fixtures (schedule and results) of a LPB / LPBF Liga Profesional de Baloncesto (Panama) season.

LPB / LPBF Liga Profesional de Baloncesto (Panama) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_fixtures` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season END year (see the season rule above). Omit season and season_id for the site's current season. |
| `season_id` | `Optional[str]` | `None` | Season UUID (season_id in the seasons listing); skips the END-year lookup. |
| `competition_id` | `Optional[str]` | `None` | Competition UUID (a site carries many: conferences, cups); needed when several share the season. |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lpb_schedule
lpb_schedule(season=2026)
```

### lpb_seasons {#lpb_seasons}

`lpb_seasons(locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every season and competition id of LPB / LPBF Liga Profesional de Baloncesto (Panama).

LPB / LPBF Liga Profesional de Baloncesto (Panama) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_seasons` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lpb_seasons
lpb_seasons()
```

### lpb_shots {#lpb_shots}

`lpb_shots(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of a LPB / LPBF Liga Profesional de Baloncesto (Panama) game.

LPB / LPBF Liga Profesional de Baloncesto (Panama) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_shots` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lpb_shots
lpb_shots(fixture_id='<fixture uuid from lpb_schedule>')
```

### lpb_standings {#lpb_standings}

`lpb_standings(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Standings of a LPB / LPBF Liga Profesional de Baloncesto (Panama) season.

LPB / LPBF Liga Profesional de Baloncesto (Panama) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_standings` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season END year (see the season rule above). Omit season and season_id for the site's current season. |
| `season_id` | `Optional[str]` | `None` | Season UUID (season_id in the seasons listing); skips the END-year lookup. |
| `competition_id` | `Optional[str]` | `None` | Competition UUID (a site carries many: conferences, cups); needed when several share the season. |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lpb_standings
lpb_standings(season=2026)
```

### lpb_team_box {#lpb_team_box}

`lpb_team_box(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of a LPB / LPBF Liga Profesional de Baloncesto (Panama) game.

LPB / LPBF Liga Profesional de Baloncesto (Panama) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_team_box_score` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lpb_team_box
lpb_team_box(fixture_id='<fixture uuid from lpb_schedule>')
```

### lpb_teams {#lpb_teams}

`lpb_teams(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Teams of a LPB / LPBF Liga Profesional de Baloncesto (Panama) season.

LPB / LPBF Liga Profesional de Baloncesto (Panama) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_teams` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season END year (see the season rule above). Omit season and season_id for the site's current season. |
| `season_id` | `Optional[str]` | `None` | Season UUID (season_id in the seasons listing); skips the END-year lookup. |
| `competition_id` | `Optional[str]` | `None` | Competition UUID (a site carries many: conferences, cups); needed when several share the season. |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import lpb_teams
lpb_teams(season=2026)
```

### nbl1_pbp {#nbl1_pbp}

`nbl1_pbp(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of a NBL1 (Australia) game.

NBL1 (Australia) (men's and women's); season rule: calendar year, Mar-Aug (END year); conferences are competition_ids.

Convenience wrapper: a thin binding of `atrium_pbp` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import nbl1_pbp
nbl1_pbp(fixture_id='<fixture uuid from nbl1_schedule>')
```

### nbl1_player_box {#nbl1_player_box}

`nbl1_player_box(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of a NBL1 (Australia) game.

NBL1 (Australia) (men's and women's); season rule: calendar year, Mar-Aug (END year); conferences are competition_ids.

Convenience wrapper: a thin binding of `atrium_box_score` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import nbl1_player_box
nbl1_player_box(fixture_id='<fixture uuid from nbl1_schedule>')
```

### nbl1_schedule {#nbl1_schedule}

`nbl1_schedule(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Fixtures (schedule and results) of a NBL1 (Australia) season.

NBL1 (Australia) (men's and women's); season rule: calendar year, Mar-Aug (END year); conferences are competition_ids.

Convenience wrapper: a thin binding of `atrium_fixtures` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season END year (see the season rule above). Omit season and season_id for the site's current season. |
| `season_id` | `Optional[str]` | `None` | Season UUID (season_id in the seasons listing); skips the END-year lookup. |
| `competition_id` | `Optional[str]` | `None` | Competition UUID (a site carries many: conferences, cups); needed when several share the season. |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import nbl1_schedule
nbl1_schedule(season=2026)
```

### nbl1_seasons {#nbl1_seasons}

`nbl1_seasons(locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every season and competition id of NBL1 (Australia).

NBL1 (Australia) (men's and women's); season rule: calendar year, Mar-Aug (END year); conferences are competition_ids.

Convenience wrapper: a thin binding of `atrium_seasons` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import nbl1_seasons
nbl1_seasons()
```

### nbl1_shots {#nbl1_shots}

`nbl1_shots(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of a NBL1 (Australia) game.

NBL1 (Australia) (men's and women's); season rule: calendar year, Mar-Aug (END year); conferences are competition_ids.

Convenience wrapper: a thin binding of `atrium_shots` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import nbl1_shots
nbl1_shots(fixture_id='<fixture uuid from nbl1_schedule>')
```

### nbl1_standings {#nbl1_standings}

`nbl1_standings(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Standings of a NBL1 (Australia) season.

NBL1 (Australia) (men's and women's); season rule: calendar year, Mar-Aug (END year); conferences are competition_ids.

Convenience wrapper: a thin binding of `atrium_standings` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season END year (see the season rule above). Omit season and season_id for the site's current season. |
| `season_id` | `Optional[str]` | `None` | Season UUID (season_id in the seasons listing); skips the END-year lookup. |
| `competition_id` | `Optional[str]` | `None` | Competition UUID (a site carries many: conferences, cups); needed when several share the season. |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import nbl1_standings
nbl1_standings(season=2026)
```

### nbl1_team_box {#nbl1_team_box}

`nbl1_team_box(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of a NBL1 (Australia) game.

NBL1 (Australia) (men's and women's); season rule: calendar year, Mar-Aug (END year); conferences are competition_ids.

Convenience wrapper: a thin binding of `atrium_team_box_score` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import nbl1_team_box
nbl1_team_box(fixture_id='<fixture uuid from nbl1_schedule>')
```

### nbl1_teams {#nbl1_teams}

`nbl1_teams(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Teams of a NBL1 (Australia) season.

NBL1 (Australia) (men's and women's); season rule: calendar year, Mar-Aug (END year); conferences are competition_ids.

Convenience wrapper: a thin binding of `atrium_teams` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season END year (see the season rule above). Omit season and season_id for the site's current season. |
| `season_id` | `Optional[str]` | `None` | Season UUID (season_id in the seasons listing); skips the END-year lookup. |
| `competition_id` | `Optional[str]` | `None` | Competition UUID (a site carries many: conferences, cups); needed when several share the season. |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.atrium import nbl1_teams
nbl1_teams(season=2026)
```

## Dates and seasons

### atrium_season_id {#atrium_season_id}

`atrium_season_id(website_id: 'int', season: 'int', competition_id: 'Optional[str]' = None, **kwargs: 'Any') -> 'str'`

The `seasonId` of a site's season, from its END year and optionally its competition.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `website_id` | `int` |  | the site (see `SITES`: 12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 326 Montenegro). |
| `season` | `int` |  | the sdv season END year (2026 = 2025-26 on LNB; the calendar year on the other sites). |
| `competition_id` | `Optional[str]` | `None` | the competition when the site lists several for that year (see `atrium_seasons`). Omit it when the year is unique. |

**Returns**

The season's UUID, for the `season_id` argument of every season-scoped wrapper.

**Example**

```python
from sportsdataverse.basketball.atrium import atrium_season_id
atrium_season_id(14, 2026, competition_id="16be8396-24fb-11ef-9ab7-df6be196e88f")
```
