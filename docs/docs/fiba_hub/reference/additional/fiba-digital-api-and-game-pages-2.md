---
title: "FIBA (digital API and game pages) — additional Python functions — FIBA digital API and game pages: weurobasket_teams–wwc_teams"
sidebar_label: "FIBA digital API and game pages: weurobasket_teams–wwc_teams"
sidebar_position: 2
description: "FIBA (digital API and game pages) — additional Python functions — FIBA digital API and game pages: weurobasket_teams–wwc_teams — function reference in sdv-py, the SportsDataverse Python package."
---
# FIBA (digital API and game pages) — additional Python functions — FIBA digital API and game pages: weurobasket_teams–wwc_teams

### fiba_weurobasket_teams {#fiba_weurobasket_teams}

`fiba_weurobasket_teams(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Teams of one FIBA Women's EuroBasket edition.

FIBA Women's EuroBasket (women's); season rule: event year (2025 = 2025).

Convenience wrapper: a thin binding of `fiba_gdap_teams` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Omitted = the newest edition that has started. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_weurobasket_teams
fiba_weurobasket_teams(season=2023)
```

### fiba_woly_editions {#fiba_woly_editions}

`fiba_woly_editions(season: 'Optional[int]' = None, to_season: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every edition of Women's Olympic Basketball Tournament.

Women's Olympic Basketball Tournament (women's); season rule: event year (Paris 2024 = 2024).

Convenience wrapper: a thin binding of `fiba_gdap_editions` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | First season (inclusive); omitted = all editions. |
| `to_season` | `Optional[int]` | `None` | Last season, inclusive. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_woly_editions
fiba_woly_editions(season=2023)
```

### fiba_woly_pbp {#fiba_woly_pbp}

`fiba_woly_pbp(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of one Women's Olympic Basketball Tournament game.

Women's Olympic Basketball Tournament (women's); season rule: event year (Paris 2024 = 2024).

Convenience wrapper: a thin binding of `fiba_gamepage_pbp` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | FIBA game id (game_id in the schedule), e.g. 100720. |
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Omitted = the newest edition that has started. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_woly_pbp
fiba_woly_pbp(game_id=100720, season=2023)
```

### fiba_woly_player_box {#fiba_woly_player_box}

`fiba_woly_player_box(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of one Women's Olympic Basketball Tournament game.

Women's Olympic Basketball Tournament (women's); season rule: event year (Paris 2024 = 2024).

Convenience wrapper: a thin binding of `fiba_gamepage_box_players` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | FIBA game id (game_id in the schedule), e.g. 100720. |
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Omitted = the newest edition that has started. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_woly_player_box
fiba_woly_player_box(game_id=100720, season=2023)
```

### fiba_woly_schedule {#fiba_woly_schedule}

`fiba_woly_schedule(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Schedule and results of one Women's Olympic Basketball Tournament edition, one row per game.

Women's Olympic Basketball Tournament (women's); season rule: event year (Paris 2024 = 2024).

Convenience wrapper: a thin binding of `fiba_gdap_schedule` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Omitted = the newest edition that has started. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_woly_schedule
fiba_woly_schedule(season=2023)
```

### fiba_woly_shots {#fiba_woly_shots}

`fiba_woly_shots(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of one Women's Olympic Basketball Tournament game.

Women's Olympic Basketball Tournament (women's); season rule: event year (Paris 2024 = 2024).

Convenience wrapper: a thin binding of `fiba_gamepage_shots` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | FIBA game id (game_id in the schedule), e.g. 100720. |
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Omitted = the newest edition that has started. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_woly_shots
fiba_woly_shots(game_id=100720, season=2023)
```

### fiba_woly_standings {#fiba_woly_standings}

`fiba_woly_standings(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Final standings of one Women's Olympic Basketball Tournament edition.

Women's Olympic Basketball Tournament (women's); season rule: event year (Paris 2024 = 2024).

Convenience wrapper: a thin binding of `fiba_gdap_final_standings` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Omitted = the newest edition that has started. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_woly_standings
fiba_woly_standings(season=2023)
```

### fiba_woly_team_box {#fiba_woly_team_box}

`fiba_woly_team_box(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of one Women's Olympic Basketball Tournament game.

Women's Olympic Basketball Tournament (women's); season rule: event year (Paris 2024 = 2024).

Convenience wrapper: a thin binding of `fiba_gamepage_box_teams` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | FIBA game id (game_id in the schedule), e.g. 100720. |
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Omitted = the newest edition that has started. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_woly_team_box
fiba_woly_team_box(game_id=100720, season=2023)
```

### fiba_woly_teams {#fiba_woly_teams}

`fiba_woly_teams(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Teams of one Women's Olympic Basketball Tournament edition.

Women's Olympic Basketball Tournament (women's); season rule: event year (Paris 2024 = 2024).

Convenience wrapper: a thin binding of `fiba_gdap_teams` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Omitted = the newest edition that has started. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_woly_teams
fiba_woly_teams(season=2023)
```

### fiba_wwc_editions {#fiba_wwc_editions}

`fiba_wwc_editions(season: 'Optional[int]' = None, to_season: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every edition of FIBA Women's Basketball World Cup.

FIBA Women's Basketball World Cup (women's); season rule: event year (2022 = 2022).

Convenience wrapper: a thin binding of `fiba_gdap_editions` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | First season (inclusive); omitted = all editions. |
| `to_season` | `Optional[int]` | `None` | Last season, inclusive. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_wwc_editions
fiba_wwc_editions(season=2023)
```

### fiba_wwc_pbp {#fiba_wwc_pbp}

`fiba_wwc_pbp(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of one FIBA Women's Basketball World Cup game.

FIBA Women's Basketball World Cup (women's); season rule: event year (2022 = 2022).

Convenience wrapper: a thin binding of `fiba_gamepage_pbp` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | FIBA game id (game_id in the schedule), e.g. 100720. |
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Omitted = the newest edition that has started. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_wwc_pbp
fiba_wwc_pbp(game_id=100720, season=2023)
```

### fiba_wwc_player_box {#fiba_wwc_player_box}

`fiba_wwc_player_box(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of one FIBA Women's Basketball World Cup game.

FIBA Women's Basketball World Cup (women's); season rule: event year (2022 = 2022).

Convenience wrapper: a thin binding of `fiba_gamepage_box_players` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | FIBA game id (game_id in the schedule), e.g. 100720. |
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Omitted = the newest edition that has started. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_wwc_player_box
fiba_wwc_player_box(game_id=100720, season=2023)
```

### fiba_wwc_schedule {#fiba_wwc_schedule}

`fiba_wwc_schedule(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Schedule and results of one FIBA Women's Basketball World Cup edition, one row per game.

FIBA Women's Basketball World Cup (women's); season rule: event year (2022 = 2022).

Convenience wrapper: a thin binding of `fiba_gdap_schedule` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Omitted = the newest edition that has started. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_wwc_schedule
fiba_wwc_schedule(season=2023)
```

### fiba_wwc_shots {#fiba_wwc_shots}

`fiba_wwc_shots(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of one FIBA Women's Basketball World Cup game.

FIBA Women's Basketball World Cup (women's); season rule: event year (2022 = 2022).

Convenience wrapper: a thin binding of `fiba_gamepage_shots` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | FIBA game id (game_id in the schedule), e.g. 100720. |
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Omitted = the newest edition that has started. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_wwc_shots
fiba_wwc_shots(game_id=100720, season=2023)
```

### fiba_wwc_standings {#fiba_wwc_standings}

`fiba_wwc_standings(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Final standings of one FIBA Women's Basketball World Cup edition.

FIBA Women's Basketball World Cup (women's); season rule: event year (2022 = 2022).

Convenience wrapper: a thin binding of `fiba_gdap_final_standings` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Omitted = the newest edition that has started. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_wwc_standings
fiba_wwc_standings(season=2023)
```

### fiba_wwc_team_box {#fiba_wwc_team_box}

`fiba_wwc_team_box(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of one FIBA Women's Basketball World Cup game.

FIBA Women's Basketball World Cup (women's); season rule: event year (2022 = 2022).

Convenience wrapper: a thin binding of `fiba_gamepage_box_teams` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | FIBA game id (game_id in the schedule), e.g. 100720. |
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Omitted = the newest edition that has started. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_wwc_team_box
fiba_wwc_team_box(game_id=100720, season=2023)
```

### fiba_wwc_teams {#fiba_wwc_teams}

`fiba_wwc_teams(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Teams of one FIBA Women's Basketball World Cup edition.

FIBA Women's Basketball World Cup (women's); season rule: event year (2022 = 2022).

Convenience wrapper: a thin binding of `fiba_gdap_teams` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Omitted = the newest edition that has started. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_wwc_teams
fiba_wwc_teams(season=2023)
```
