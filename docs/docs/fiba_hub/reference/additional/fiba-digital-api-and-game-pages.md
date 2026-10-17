---
title: "FIBA (digital API and game pages) — additional Python functions — FIBA digital API and game pages: afrobasket_editions–weurobasket_team"
sidebar_label: "FIBA digital API and game pages: afrobasket_editions–weurobasket_team"
sidebar_position: 1
description: "FIBA (digital API and game pages) — additional Python functions — FIBA digital API and game pages: afrobasket_editions–weurobasket_team — function reference in sdv-py, the SportsDataverse Python package."
---
# FIBA (digital API and game pages) — additional Python functions — FIBA digital API and game pages: afrobasket_editions–weurobasket_team

### fiba_afrobasket_editions {#fiba_afrobasket_editions}

`fiba_afrobasket_editions(season: 'Optional[int]' = None, to_season: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every edition of FIBA AfroBasket.

FIBA AfroBasket (men's); season rule: event year (2025 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_afrobasket_editions
fiba_afrobasket_editions(season=2023)
```

### fiba_afrobasket_pbp {#fiba_afrobasket_pbp}

`fiba_afrobasket_pbp(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of one FIBA AfroBasket game.

FIBA AfroBasket (men's); season rule: event year (2025 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_afrobasket_pbp
fiba_afrobasket_pbp(game_id=100720, season=2023)
```

### fiba_afrobasket_player_box {#fiba_afrobasket_player_box}

`fiba_afrobasket_player_box(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of one FIBA AfroBasket game.

FIBA AfroBasket (men's); season rule: event year (2025 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_afrobasket_player_box
fiba_afrobasket_player_box(game_id=100720, season=2023)
```

### fiba_afrobasket_schedule {#fiba_afrobasket_schedule}

`fiba_afrobasket_schedule(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Schedule and results of one FIBA AfroBasket edition, one row per game.

FIBA AfroBasket (men's); season rule: event year (2025 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_afrobasket_schedule
fiba_afrobasket_schedule(season=2023)
```

### fiba_afrobasket_shots {#fiba_afrobasket_shots}

`fiba_afrobasket_shots(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of one FIBA AfroBasket game.

FIBA AfroBasket (men's); season rule: event year (2025 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_afrobasket_shots
fiba_afrobasket_shots(game_id=100720, season=2023)
```

### fiba_afrobasket_standings {#fiba_afrobasket_standings}

`fiba_afrobasket_standings(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Final standings of one FIBA AfroBasket edition.

FIBA AfroBasket (men's); season rule: event year (2025 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_afrobasket_standings
fiba_afrobasket_standings(season=2023)
```

### fiba_afrobasket_team_box {#fiba_afrobasket_team_box}

`fiba_afrobasket_team_box(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of one FIBA AfroBasket game.

FIBA AfroBasket (men's); season rule: event year (2025 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_afrobasket_team_box
fiba_afrobasket_team_box(game_id=100720, season=2023)
```

### fiba_afrobasket_teams {#fiba_afrobasket_teams}

`fiba_afrobasket_teams(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Teams of one FIBA AfroBasket edition.

FIBA AfroBasket (men's); season rule: event year (2025 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_afrobasket_teams
fiba_afrobasket_teams(season=2023)
```

### fiba_bcl_editions {#fiba_bcl_editions}

`fiba_bcl_editions(season: 'Optional[int]' = None, to_season: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every edition of Basketball Champions League (FIBA Men's European Club Competitions Tier 1).

Basketball Champions League (FIBA Men's European Club Competitions Tier 1) (men's); season rule: END year (2024-25 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_bcl_editions
fiba_bcl_editions(season=2023)
```

### fiba_bcl_pbp {#fiba_bcl_pbp}

`fiba_bcl_pbp(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) game.

Basketball Champions League (FIBA Men's European Club Competitions Tier 1) (men's); season rule: END year (2024-25 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_bcl_pbp
fiba_bcl_pbp(game_id=100720, season=2023)
```

### fiba_bcl_player_box {#fiba_bcl_player_box}

`fiba_bcl_player_box(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) game.

Basketball Champions League (FIBA Men's European Club Competitions Tier 1) (men's); season rule: END year (2024-25 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_bcl_player_box
fiba_bcl_player_box(game_id=100720, season=2023)
```

### fiba_bcl_schedule {#fiba_bcl_schedule}

`fiba_bcl_schedule(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Schedule and results of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) edition, one row per game.

Basketball Champions League (FIBA Men's European Club Competitions Tier 1) (men's); season rule: END year (2024-25 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_bcl_schedule
fiba_bcl_schedule(season=2023)
```

### fiba_bcl_shots {#fiba_bcl_shots}

`fiba_bcl_shots(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) game.

Basketball Champions League (FIBA Men's European Club Competitions Tier 1) (men's); season rule: END year (2024-25 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_bcl_shots
fiba_bcl_shots(game_id=100720, season=2023)
```

### fiba_bcl_standings {#fiba_bcl_standings}

`fiba_bcl_standings(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Final standings of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) edition.

Basketball Champions League (FIBA Men's European Club Competitions Tier 1) (men's); season rule: END year (2024-25 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_bcl_standings
fiba_bcl_standings(season=2023)
```

### fiba_bcl_team_box {#fiba_bcl_team_box}

`fiba_bcl_team_box(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) game.

Basketball Champions League (FIBA Men's European Club Competitions Tier 1) (men's); season rule: END year (2024-25 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_bcl_team_box
fiba_bcl_team_box(game_id=100720, season=2023)
```

### fiba_bcl_teams {#fiba_bcl_teams}

`fiba_bcl_teams(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Teams of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) edition.

Basketball Champions League (FIBA Men's European Club Competitions Tier 1) (men's); season rule: END year (2024-25 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_bcl_teams
fiba_bcl_teams(season=2023)
```

### fiba_elw_editions {#fiba_elw_editions}

`fiba_elw_editions(season: 'Optional[int]' = None, to_season: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every edition of EuroLeague Women (FIBA Women's European Club Competitions Tier 1).

EuroLeague Women (FIBA Women's European Club Competitions Tier 1) (women's); season rule: END year (2024-25 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_elw_editions
fiba_elw_editions(season=2023)
```

### fiba_elw_pbp {#fiba_elw_pbp}

`fiba_elw_pbp(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) game.

EuroLeague Women (FIBA Women's European Club Competitions Tier 1) (women's); season rule: END year (2024-25 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_elw_pbp
fiba_elw_pbp(game_id=100720, season=2023)
```

### fiba_elw_player_box {#fiba_elw_player_box}

`fiba_elw_player_box(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) game.

EuroLeague Women (FIBA Women's European Club Competitions Tier 1) (women's); season rule: END year (2024-25 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_elw_player_box
fiba_elw_player_box(game_id=100720, season=2023)
```

### fiba_elw_schedule {#fiba_elw_schedule}

`fiba_elw_schedule(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Schedule and results of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) edition, one row per game.

EuroLeague Women (FIBA Women's European Club Competitions Tier 1) (women's); season rule: END year (2024-25 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_elw_schedule
fiba_elw_schedule(season=2023)
```

### fiba_elw_shots {#fiba_elw_shots}

`fiba_elw_shots(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) game.

EuroLeague Women (FIBA Women's European Club Competitions Tier 1) (women's); season rule: END year (2024-25 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_elw_shots
fiba_elw_shots(game_id=100720, season=2023)
```

### fiba_elw_standings {#fiba_elw_standings}

`fiba_elw_standings(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Final standings of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) edition.

EuroLeague Women (FIBA Women's European Club Competitions Tier 1) (women's); season rule: END year (2024-25 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_elw_standings
fiba_elw_standings(season=2023)
```

### fiba_elw_team_box {#fiba_elw_team_box}

`fiba_elw_team_box(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) game.

EuroLeague Women (FIBA Women's European Club Competitions Tier 1) (women's); season rule: END year (2024-25 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_elw_team_box
fiba_elw_team_box(game_id=100720, season=2023)
```

### fiba_elw_teams {#fiba_elw_teams}

`fiba_elw_teams(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Teams of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) edition.

EuroLeague Women (FIBA Women's European Club Competitions Tier 1) (women's); season rule: END year (2024-25 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_elw_teams
fiba_elw_teams(season=2023)
```

### fiba_oly_editions {#fiba_oly_editions}

`fiba_oly_editions(season: 'Optional[int]' = None, to_season: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every edition of Men's Olympic Basketball Tournament.

Men's Olympic Basketball Tournament (men's); season rule: event year (Paris 2024 = 2024).

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
from sportsdataverse.basketball.fiba_hub import fiba_oly_editions
fiba_oly_editions(season=2023)
```

### fiba_oly_pbp {#fiba_oly_pbp}

`fiba_oly_pbp(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of one Men's Olympic Basketball Tournament game.

Men's Olympic Basketball Tournament (men's); season rule: event year (Paris 2024 = 2024).

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
from sportsdataverse.basketball.fiba_hub import fiba_oly_pbp
fiba_oly_pbp(game_id=100720, season=2023)
```

### fiba_oly_player_box {#fiba_oly_player_box}

`fiba_oly_player_box(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of one Men's Olympic Basketball Tournament game.

Men's Olympic Basketball Tournament (men's); season rule: event year (Paris 2024 = 2024).

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
from sportsdataverse.basketball.fiba_hub import fiba_oly_player_box
fiba_oly_player_box(game_id=100720, season=2023)
```

### fiba_oly_schedule {#fiba_oly_schedule}

`fiba_oly_schedule(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Schedule and results of one Men's Olympic Basketball Tournament edition, one row per game.

Men's Olympic Basketball Tournament (men's); season rule: event year (Paris 2024 = 2024).

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
from sportsdataverse.basketball.fiba_hub import fiba_oly_schedule
fiba_oly_schedule(season=2023)
```

### fiba_oly_shots {#fiba_oly_shots}

`fiba_oly_shots(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of one Men's Olympic Basketball Tournament game.

Men's Olympic Basketball Tournament (men's); season rule: event year (Paris 2024 = 2024).

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
from sportsdataverse.basketball.fiba_hub import fiba_oly_shots
fiba_oly_shots(game_id=100720, season=2023)
```

### fiba_oly_standings {#fiba_oly_standings}

`fiba_oly_standings(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Final standings of one Men's Olympic Basketball Tournament edition.

Men's Olympic Basketball Tournament (men's); season rule: event year (Paris 2024 = 2024).

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
from sportsdataverse.basketball.fiba_hub import fiba_oly_standings
fiba_oly_standings(season=2023)
```

### fiba_oly_team_box {#fiba_oly_team_box}

`fiba_oly_team_box(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of one Men's Olympic Basketball Tournament game.

Men's Olympic Basketball Tournament (men's); season rule: event year (Paris 2024 = 2024).

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
from sportsdataverse.basketball.fiba_hub import fiba_oly_team_box
fiba_oly_team_box(game_id=100720, season=2023)
```

### fiba_oly_teams {#fiba_oly_teams}

`fiba_oly_teams(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Teams of one Men's Olympic Basketball Tournament edition.

Men's Olympic Basketball Tournament (men's); season rule: event year (Paris 2024 = 2024).

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
from sportsdataverse.basketball.fiba_hub import fiba_oly_teams
fiba_oly_teams(season=2023)
```

### fiba_wc_editions {#fiba_wc_editions}

`fiba_wc_editions(season: 'Optional[int]' = None, to_season: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every edition of FIBA Basketball World Cup.

FIBA Basketball World Cup (men's); season rule: event year (World Cup 2023 = 2023).

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
from sportsdataverse.basketball.fiba_hub import fiba_wc_editions
fiba_wc_editions(season=2023)
```

### fiba_wc_pbp {#fiba_wc_pbp}

`fiba_wc_pbp(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of one FIBA Basketball World Cup game.

FIBA Basketball World Cup (men's); season rule: event year (World Cup 2023 = 2023).

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
from sportsdataverse.basketball.fiba_hub import fiba_wc_pbp
fiba_wc_pbp(game_id=100720, season=2023)
```

### fiba_wc_player_box {#fiba_wc_player_box}

`fiba_wc_player_box(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of one FIBA Basketball World Cup game.

FIBA Basketball World Cup (men's); season rule: event year (World Cup 2023 = 2023).

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
from sportsdataverse.basketball.fiba_hub import fiba_wc_player_box
fiba_wc_player_box(game_id=100720, season=2023)
```

### fiba_wc_schedule {#fiba_wc_schedule}

`fiba_wc_schedule(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Schedule and results of one FIBA Basketball World Cup edition, one row per game.

FIBA Basketball World Cup (men's); season rule: event year (World Cup 2023 = 2023).

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
from sportsdataverse.basketball.fiba_hub import fiba_wc_schedule
fiba_wc_schedule(season=2023)
```

### fiba_wc_shots {#fiba_wc_shots}

`fiba_wc_shots(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of one FIBA Basketball World Cup game.

FIBA Basketball World Cup (men's); season rule: event year (World Cup 2023 = 2023).

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
from sportsdataverse.basketball.fiba_hub import fiba_wc_shots
fiba_wc_shots(game_id=100720, season=2023)
```

### fiba_wc_standings {#fiba_wc_standings}

`fiba_wc_standings(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Final standings of one FIBA Basketball World Cup edition.

FIBA Basketball World Cup (men's); season rule: event year (World Cup 2023 = 2023).

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
from sportsdataverse.basketball.fiba_hub import fiba_wc_standings
fiba_wc_standings(season=2023)
```

### fiba_wc_team_box {#fiba_wc_team_box}

`fiba_wc_team_box(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of one FIBA Basketball World Cup game.

FIBA Basketball World Cup (men's); season rule: event year (World Cup 2023 = 2023).

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
from sportsdataverse.basketball.fiba_hub import fiba_wc_team_box
fiba_wc_team_box(game_id=100720, season=2023)
```

### fiba_wc_teams {#fiba_wc_teams}

`fiba_wc_teams(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Teams of one FIBA Basketball World Cup edition.

FIBA Basketball World Cup (men's); season rule: event year (World Cup 2023 = 2023).

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
from sportsdataverse.basketball.fiba_hub import fiba_wc_teams
fiba_wc_teams(season=2023)
```

### fiba_weurobasket_editions {#fiba_weurobasket_editions}

`fiba_weurobasket_editions(season: 'Optional[int]' = None, to_season: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Every edition of FIBA Women's EuroBasket.

FIBA Women's EuroBasket (women's); season rule: event year (2025 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_weurobasket_editions
fiba_weurobasket_editions(season=2023)
```

### fiba_weurobasket_pbp {#fiba_weurobasket_pbp}

`fiba_weurobasket_pbp(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Play-by-play of one FIBA Women's EuroBasket game.

FIBA Women's EuroBasket (women's); season rule: event year (2025 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_weurobasket_pbp
fiba_weurobasket_pbp(game_id=100720, season=2023)
```

### fiba_weurobasket_player_box {#fiba_weurobasket_player_box}

`fiba_weurobasket_player_box(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Player box score of one FIBA Women's EuroBasket game.

FIBA Women's EuroBasket (women's); season rule: event year (2025 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_weurobasket_player_box
fiba_weurobasket_player_box(game_id=100720, season=2023)
```

### fiba_weurobasket_schedule {#fiba_weurobasket_schedule}

`fiba_weurobasket_schedule(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Schedule and results of one FIBA Women's EuroBasket edition, one row per game.

FIBA Women's EuroBasket (women's); season rule: event year (2025 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_weurobasket_schedule
fiba_weurobasket_schedule(season=2023)
```

### fiba_weurobasket_shots {#fiba_weurobasket_shots}

`fiba_weurobasket_shots(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Shot chart of one FIBA Women's EuroBasket game.

FIBA Women's EuroBasket (women's); season rule: event year (2025 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_weurobasket_shots
fiba_weurobasket_shots(game_id=100720, season=2023)
```

### fiba_weurobasket_standings {#fiba_weurobasket_standings}

`fiba_weurobasket_standings(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Final standings of one FIBA Women's EuroBasket edition.

FIBA Women's EuroBasket (women's); season rule: event year (2025 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_weurobasket_standings
fiba_weurobasket_standings(season=2023)
```

### fiba_weurobasket_team_box {#fiba_weurobasket_team_box}

`fiba_weurobasket_team_box(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Any'`

Team box score of one FIBA Women's EuroBasket game.

FIBA Women's EuroBasket (women's); season rule: event year (2025 = 2025).

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
from sportsdataverse.basketball.fiba_hub import fiba_weurobasket_team_box
fiba_weurobasket_team_box(game_id=100720, season=2023)
```
