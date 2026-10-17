---
title: GENIUS — additional Python functions
sidebar_label: Additional functions
description: "GENIUS — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# GENIUS — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.basketball.genius`
not covered by the generated API-endpoint reference above.

## Genius Sports LiveStats

### bal_competitions {#bal_competitions}

`bal_competitions(*, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every competition of the BAL Genius org, with its END-year season.

Basketball Africa League (men's); season rule: calendar year (BAL 2025 = 2025); 2023 lists two competitions, so pass comp_id.

Convenience wrapper: a thin binding of `genius_competitions` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import bal_competitions
bal_competitions()
```

### bal_pbp {#bal_pbp}

`bal_pbp(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of a finished Basketball Africa League game.

Basketball Africa League (men's); season rule: calendar year (BAL 2025 = 2025); 2023 lists two competitions, so pass comp_id.

Convenience wrapper: a thin binding of `genius_pbp` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import bal_pbp
bal_pbp(match_id=2902690)
```

### bal_player_box {#bal_player_box}

`bal_player_box(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of a finished Basketball Africa League game.

Basketball Africa League (men's); season rule: calendar year (BAL 2025 = 2025); 2023 lists two competitions, so pass comp_id.

Convenience wrapper: a thin binding of `genius_player_box` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import bal_player_box
bal_player_box(match_id=2902690)
```

### bal_schedule {#bal_schedule}

`bal_schedule(season: 'Optional[int]' = None, comp_id: 'Optional[int]' = None, round_number: 'Optional[int]' = -1, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every match of one Basketball Africa League season (hosted WebStats): ids, status, venue, teams and scores.

Basketball Africa League (men's); season rule: calendar year (BAL 2025 = 2025); 2023 lists two competitions, so pass comp_id.

Convenience wrapper: a thin binding of `genius_schedule` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | END-year season (see the season rule above). Omitted = the newest season the org lists. |
| `comp_id` | `Optional[int]` | `None` | Genius competition id; skips the season lookup (needed when the label rule matches several competitions). |
| `round_number` | `Optional[int]` | `-1` | Round to list; -1 (default) lists every round. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import bal_schedule
bal_schedule(season=2026)
```

### bal_shots {#bal_shots}

`bal_shots(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of a finished Basketball Africa League game.

Basketball Africa League (men's); season rule: calendar year (BAL 2025 = 2025); 2023 lists two competitions, so pass comp_id.

Convenience wrapper: a thin binding of `genius_shots` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import bal_shots
bal_shots(match_id=2902690)
```

### bal_team_box {#bal_team_box}

`bal_team_box(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of a finished Basketball Africa League game.

Basketball Africa League (men's); season rule: calendar year (BAL 2025 = 2025); 2023 lists two competitions, so pass comp_id.

Convenience wrapper: a thin binding of `genius_team_box` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import bal_team_box
bal_team_box(match_id=2902690)
```

### esake_competitions {#esake_competitions}

`esake_competitions(*, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every competition of the ESAKE Genius org, with its END-year season.

Greek Basket League (ESAKE) (men's); season rule: END year; Genius year + 1 (2022-23 = 2023).

Convenience wrapper: a thin binding of `genius_competitions` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import esake_competitions
esake_competitions()
```

### esake_pbp {#esake_pbp}

`esake_pbp(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of a finished Greek Basket League (ESAKE) game.

Greek Basket League (ESAKE) (men's); season rule: END year; Genius year + 1 (2022-23 = 2023).

Convenience wrapper: a thin binding of `genius_pbp` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import esake_pbp
esake_pbp(match_id=2902690)
```

### esake_player_box {#esake_player_box}

`esake_player_box(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of a finished Greek Basket League (ESAKE) game.

Greek Basket League (ESAKE) (men's); season rule: END year; Genius year + 1 (2022-23 = 2023).

Convenience wrapper: a thin binding of `genius_player_box` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import esake_player_box
esake_player_box(match_id=2902690)
```

### esake_schedule {#esake_schedule}

`esake_schedule(season: 'Optional[int]' = None, comp_id: 'Optional[int]' = None, round_number: 'Optional[int]' = -1, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every match of one Greek Basket League (ESAKE) season (hosted WebStats): ids, status, venue, teams and scores.

Greek Basket League (ESAKE) (men's); season rule: END year; Genius year + 1 (2022-23 = 2023).

Convenience wrapper: a thin binding of `genius_schedule` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | END-year season (see the season rule above). Omitted = the newest season the org lists. |
| `comp_id` | `Optional[int]` | `None` | Genius competition id; skips the season lookup (needed when the label rule matches several competitions). |
| `round_number` | `Optional[int]` | `-1` | Round to list; -1 (default) lists every round. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import esake_schedule
esake_schedule(season=2026)
```

### esake_shots {#esake_shots}

`esake_shots(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of a finished Greek Basket League (ESAKE) game.

Greek Basket League (ESAKE) (men's); season rule: END year; Genius year + 1 (2022-23 = 2023).

Convenience wrapper: a thin binding of `genius_shots` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import esake_shots
esake_shots(match_id=2902690)
```

### esake_team_box {#esake_team_box}

`esake_team_box(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of a finished Greek Basket League (ESAKE) game.

Greek Basket League (ESAKE) (men's); season rule: END year; Genius year + 1 (2022-23 = 2023).

Convenience wrapper: a thin binding of `genius_team_box` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import esake_team_box
esake_team_box(match_id=2902690)
```

### lfb_competitions {#lfb_competitions}

`lfb_competitions(*, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every competition of the FFBB Genius org, with its END-year season.

Ligue Feminine de Basket (France, FFBB) (women's); season rule: END year; Genius year + 1 (2018-19 = 2019).

Convenience wrapper: a thin binding of `genius_competitions` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import lfb_competitions
lfb_competitions()
```

### lfb_pbp {#lfb_pbp}

`lfb_pbp(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of a finished Ligue Feminine de Basket (France, FFBB) game.

Ligue Feminine de Basket (France, FFBB) (women's); season rule: END year; Genius year + 1 (2018-19 = 2019).

Convenience wrapper: a thin binding of `genius_pbp` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import lfb_pbp
lfb_pbp(match_id=2902690)
```

### lfb_player_box {#lfb_player_box}

`lfb_player_box(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of a finished Ligue Feminine de Basket (France, FFBB) game.

Ligue Feminine de Basket (France, FFBB) (women's); season rule: END year; Genius year + 1 (2018-19 = 2019).

Convenience wrapper: a thin binding of `genius_player_box` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import lfb_player_box
lfb_player_box(match_id=2902690)
```

### lfb_schedule {#lfb_schedule}

`lfb_schedule(season: 'Optional[int]' = None, comp_id: 'Optional[int]' = None, round_number: 'Optional[int]' = -1, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every match of one Ligue Feminine de Basket (France, FFBB) season (hosted WebStats): ids, status, venue, teams and scores.

Ligue Feminine de Basket (France, FFBB) (women's); season rule: END year; Genius year + 1 (2018-19 = 2019).

Convenience wrapper: a thin binding of `genius_schedule` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | END-year season (see the season rule above). Omitted = the newest season the org lists. |
| `comp_id` | `Optional[int]` | `None` | Genius competition id; skips the season lookup (needed when the label rule matches several competitions). |
| `round_number` | `Optional[int]` | `-1` | Round to list; -1 (default) lists every round. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import lfb_schedule
lfb_schedule(season=2026)
```

### lfb_shots {#lfb_shots}

`lfb_shots(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of a finished Ligue Feminine de Basket (France, FFBB) game.

Ligue Feminine de Basket (France, FFBB) (women's); season rule: END year; Genius year + 1 (2018-19 = 2019).

Convenience wrapper: a thin binding of `genius_shots` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import lfb_shots
lfb_shots(match_id=2902690)
```

### lfb_team_box {#lfb_team_box}

`lfb_team_box(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of a finished Ligue Feminine de Basket (France, FFBB) game.

Ligue Feminine de Basket (France, FFBB) (women's); season rule: END year; Genius year + 1 (2018-19 = 2019).

Convenience wrapper: a thin binding of `genius_team_box` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import lfb_team_box
lfb_team_box(match_id=2902690)
```

### lnbm_competitions {#lnbm_competitions}

`lnbm_competitions(*, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every competition of the FRB Genius org, with its END-year season.

Liga Nationala de Baschet Masculin (Romania) (men's); season rule: END year; Genius year + 1 (2016-17 = 2017).

Convenience wrapper: a thin binding of `genius_competitions` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import lnbm_competitions
lnbm_competitions()
```

### lnbm_pbp {#lnbm_pbp}

`lnbm_pbp(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of a finished Liga Nationala de Baschet Masculin (Romania) game.

Liga Nationala de Baschet Masculin (Romania) (men's); season rule: END year; Genius year + 1 (2016-17 = 2017).

Convenience wrapper: a thin binding of `genius_pbp` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import lnbm_pbp
lnbm_pbp(match_id=2902690)
```

### lnbm_player_box {#lnbm_player_box}

`lnbm_player_box(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of a finished Liga Nationala de Baschet Masculin (Romania) game.

Liga Nationala de Baschet Masculin (Romania) (men's); season rule: END year; Genius year + 1 (2016-17 = 2017).

Convenience wrapper: a thin binding of `genius_player_box` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import lnbm_player_box
lnbm_player_box(match_id=2902690)
```

### lnbm_schedule {#lnbm_schedule}

`lnbm_schedule(season: 'Optional[int]' = None, comp_id: 'Optional[int]' = None, round_number: 'Optional[int]' = -1, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every match of one Liga Nationala de Baschet Masculin (Romania) season (hosted WebStats): ids, status, venue, teams and scores.

Liga Nationala de Baschet Masculin (Romania) (men's); season rule: END year; Genius year + 1 (2016-17 = 2017).

Convenience wrapper: a thin binding of `genius_schedule` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | END-year season (see the season rule above). Omitted = the newest season the org lists. |
| `comp_id` | `Optional[int]` | `None` | Genius competition id; skips the season lookup (needed when the label rule matches several competitions). |
| `round_number` | `Optional[int]` | `-1` | Round to list; -1 (default) lists every round. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import lnbm_schedule
lnbm_schedule(season=2026)
```

### lnbm_shots {#lnbm_shots}

`lnbm_shots(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of a finished Liga Nationala de Baschet Masculin (Romania) game.

Liga Nationala de Baschet Masculin (Romania) (men's); season rule: END year; Genius year + 1 (2016-17 = 2017).

Convenience wrapper: a thin binding of `genius_shots` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import lnbm_shots
lnbm_shots(match_id=2902690)
```

### lnbm_team_box {#lnbm_team_box}

`lnbm_team_box(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of a finished Liga Nationala de Baschet Masculin (Romania) game.

Liga Nationala de Baschet Masculin (Romania) (men's); season rule: END year; Genius year + 1 (2016-17 = 2017).

Convenience wrapper: a thin binding of `genius_team_box` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import lnbm_team_box
lnbm_team_box(match_id=2902690)
```

### lub_competitions {#lub_competitions}

`lub_competitions(*, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every competition of the FUBB Genius org, with its END-year season.

Liga Uruguaya de Basquetbol (men's); season rule: END year; Genius year + 1 (2025-26 = 2026).

Convenience wrapper: a thin binding of `genius_competitions` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import lub_competitions
lub_competitions()
```

### lub_pbp {#lub_pbp}

`lub_pbp(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of a finished Liga Uruguaya de Basquetbol game.

Liga Uruguaya de Basquetbol (men's); season rule: END year; Genius year + 1 (2025-26 = 2026).

Convenience wrapper: a thin binding of `genius_pbp` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import lub_pbp
lub_pbp(match_id=2902690)
```

### lub_player_box {#lub_player_box}

`lub_player_box(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of a finished Liga Uruguaya de Basquetbol game.

Liga Uruguaya de Basquetbol (men's); season rule: END year; Genius year + 1 (2025-26 = 2026).

Convenience wrapper: a thin binding of `genius_player_box` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import lub_player_box
lub_player_box(match_id=2902690)
```

### lub_schedule {#lub_schedule}

`lub_schedule(season: 'Optional[int]' = None, comp_id: 'Optional[int]' = None, round_number: 'Optional[int]' = -1, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every match of one Liga Uruguaya de Basquetbol season (hosted WebStats): ids, status, venue, teams and scores.

Liga Uruguaya de Basquetbol (men's); season rule: END year; Genius year + 1 (2025-26 = 2026).

Convenience wrapper: a thin binding of `genius_schedule` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | END-year season (see the season rule above). Omitted = the newest season the org lists. |
| `comp_id` | `Optional[int]` | `None` | Genius competition id; skips the season lookup (needed when the label rule matches several competitions). |
| `round_number` | `Optional[int]` | `-1` | Round to list; -1 (default) lists every round. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import lub_schedule
lub_schedule(season=2026)
```

### lub_shots {#lub_shots}

`lub_shots(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of a finished Liga Uruguaya de Basquetbol game.

Liga Uruguaya de Basquetbol (men's); season rule: END year; Genius year + 1 (2025-26 = 2026).

Convenience wrapper: a thin binding of `genius_shots` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import lub_shots
lub_shots(match_id=2902690)
```

### lub_team_box {#lub_team_box}

`lub_team_box(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of a finished Liga Uruguaya de Basquetbol game.

Liga Uruguaya de Basquetbol (men's); season rule: END year; Genius year + 1 (2025-26 = 2026).

Convenience wrapper: a thin binding of `genius_team_box` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import lub_team_box
lub_team_box(match_id=2902690)
```

### nznbl_competitions {#nznbl_competitions}

`nznbl_competitions(*, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every competition of the NZNBL Genius org, with its END-year season.

New Zealand National Basketball League (men's); season rule: calendar year (2025 = 2025).

Convenience wrapper: a thin binding of `genius_competitions` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import nznbl_competitions
nznbl_competitions()
```

### nznbl_pbp {#nznbl_pbp}

`nznbl_pbp(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of a finished New Zealand National Basketball League game.

New Zealand National Basketball League (men's); season rule: calendar year (2025 = 2025).

Convenience wrapper: a thin binding of `genius_pbp` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import nznbl_pbp
nznbl_pbp(match_id=2902690)
```

### nznbl_player_box {#nznbl_player_box}

`nznbl_player_box(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of a finished New Zealand National Basketball League game.

New Zealand National Basketball League (men's); season rule: calendar year (2025 = 2025).

Convenience wrapper: a thin binding of `genius_player_box` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import nznbl_player_box
nznbl_player_box(match_id=2902690)
```

### nznbl_schedule {#nznbl_schedule}

`nznbl_schedule(season: 'Optional[int]' = None, comp_id: 'Optional[int]' = None, round_number: 'Optional[int]' = -1, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every match of one New Zealand National Basketball League season (hosted WebStats): ids, status, venue, teams and scores.

New Zealand National Basketball League (men's); season rule: calendar year (2025 = 2025).

Convenience wrapper: a thin binding of `genius_schedule` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | END-year season (see the season rule above). Omitted = the newest season the org lists. |
| `comp_id` | `Optional[int]` | `None` | Genius competition id; skips the season lookup (needed when the label rule matches several competitions). |
| `round_number` | `Optional[int]` | `-1` | Round to list; -1 (default) lists every round. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import nznbl_schedule
nznbl_schedule(season=2026)
```

### nznbl_shots {#nznbl_shots}

`nznbl_shots(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of a finished New Zealand National Basketball League game.

New Zealand National Basketball League (men's); season rule: calendar year (2025 = 2025).

Convenience wrapper: a thin binding of `genius_shots` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import nznbl_shots
nznbl_shots(match_id=2902690)
```

### nznbl_team_box {#nznbl_team_box}

`nznbl_team_box(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of a finished New Zealand National Basketball League game.

New Zealand National Basketball League (men's); season rule: calendar year (2025 = 2025).

Convenience wrapper: a thin binding of `genius_team_box` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import nznbl_team_box
nznbl_team_box(match_id=2902690)
```

### slb_competitions {#slb_competitions}

`slb_competitions(*, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every competition of the SLB Genius org, with its END-year season.

Super League Basketball (UK) (men's); season rule: END year; the label span 2025-26 = 2026.

Convenience wrapper: a thin binding of `genius_competitions` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import slb_competitions
slb_competitions()
```

### slb_pbp {#slb_pbp}

`slb_pbp(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of a finished Super League Basketball (UK) game.

Super League Basketball (UK) (men's); season rule: END year; the label span 2025-26 = 2026.

Convenience wrapper: a thin binding of `genius_pbp` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import slb_pbp
slb_pbp(match_id=2902690)
```

### slb_player_box {#slb_player_box}

`slb_player_box(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of a finished Super League Basketball (UK) game.

Super League Basketball (UK) (men's); season rule: END year; the label span 2025-26 = 2026.

Convenience wrapper: a thin binding of `genius_player_box` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import slb_player_box
slb_player_box(match_id=2902690)
```

### slb_schedule {#slb_schedule}

`slb_schedule(season: 'Optional[int]' = None, comp_id: 'Optional[int]' = None, round_number: 'Optional[int]' = -1, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every match of one Super League Basketball (UK) season (hosted WebStats): ids, status, venue, teams and scores.

Super League Basketball (UK) (men's); season rule: END year; the label span 2025-26 = 2026.

Convenience wrapper: a thin binding of `genius_schedule` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | END-year season (see the season rule above). Omitted = the newest season the org lists. |
| `comp_id` | `Optional[int]` | `None` | Genius competition id; skips the season lookup (needed when the label rule matches several competitions). |
| `round_number` | `Optional[int]` | `-1` | Round to list; -1 (default) lists every round. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import slb_schedule
slb_schedule(season=2026)
```

### slb_shots {#slb_shots}

`slb_shots(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of a finished Super League Basketball (UK) game.

Super League Basketball (UK) (men's); season rule: END year; the label span 2025-26 = 2026.

Convenience wrapper: a thin binding of `genius_shots` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import slb_shots
slb_shots(match_id=2902690)
```

### slb_team_box {#slb_team_box}

`slb_team_box(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of a finished Super League Basketball (UK) game.

Super League Basketball (UK) (men's); season rule: END year; the label span 2025-26 = 2026.

Convenience wrapper: a thin binding of `genius_team_box` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import slb_team_box
slb_team_box(match_id=2902690)
```

### wbbl_competitions {#wbbl_competitions}

`wbbl_competitions(*, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every competition of the WBBL Genius org, with its END-year season.

Women's British Basketball League (women's); season rule: END year; the label span 2025-26 = 2026.

Convenience wrapper: a thin binding of `genius_competitions` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import wbbl_competitions
wbbl_competitions()
```

### wbbl_pbp {#wbbl_pbp}

`wbbl_pbp(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of a finished Women's British Basketball League game.

Women's British Basketball League (women's); season rule: END year; the label span 2025-26 = 2026.

Convenience wrapper: a thin binding of `genius_pbp` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import wbbl_pbp
wbbl_pbp(match_id=2902690)
```

### wbbl_player_box {#wbbl_player_box}

`wbbl_player_box(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of a finished Women's British Basketball League game.

Women's British Basketball League (women's); season rule: END year; the label span 2025-26 = 2026.

Convenience wrapper: a thin binding of `genius_player_box` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import wbbl_player_box
wbbl_player_box(match_id=2902690)
```

### wbbl_schedule {#wbbl_schedule}

`wbbl_schedule(season: 'Optional[int]' = None, comp_id: 'Optional[int]' = None, round_number: 'Optional[int]' = -1, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every match of one Women's British Basketball League season (hosted WebStats): ids, status, venue, teams and scores.

Women's British Basketball League (women's); season rule: END year; the label span 2025-26 = 2026.

Convenience wrapper: a thin binding of `genius_schedule` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | END-year season (see the season rule above). Omitted = the newest season the org lists. |
| `comp_id` | `Optional[int]` | `None` | Genius competition id; skips the season lookup (needed when the label rule matches several competitions). |
| `round_number` | `Optional[int]` | `-1` | Round to list; -1 (default) lists every round. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import wbbl_schedule
wbbl_schedule(season=2026)
```

### wbbl_shots {#wbbl_shots}

`wbbl_shots(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of a finished Women's British Basketball League game.

Women's British Basketball League (women's); season rule: END year; the label span 2025-26 = 2026.

Convenience wrapper: a thin binding of `genius_shots` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import wbbl_shots
wbbl_shots(match_id=2902690)
```

### wbbl_team_box {#wbbl_team_box}

`wbbl_team_box(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of a finished Women's British Basketball League game.

Women's British Basketball League (women's); season rule: END year; the label span 2025-26 = 2026.

Convenience wrapper: a thin binding of `genius_team_box` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule), e.g. 2902690. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.genius import wbbl_team_box
wbbl_team_box(match_id=2902690)
```
