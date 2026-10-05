---
title: "WBB — additional Python functions — Wbb"
sidebar_label: "Wbb"
sidebar_position: 8
description: "WBB — additional Python functions — Wbb — function reference in sdv-py, the SportsDataverse Python package."
---
# WBB — additional Python functions — Wbb

### wbb_bracket_sim {#wbb_bracket_sim}

`wbb_bracket_sim(seeded_field: 'pl.DataFrame', ratings: 'pl.DataFrame', *, n_sims: 'int' = 10000, seed: 'int' = 0, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Women's single-elimination bracket Monte Carlo.

Delegates to `sportsdataverse.mbb.mbb_season_sim.mbb_bracket_sim` with `league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seeded_field` | `DataFrame` |  | Bracket-ordered rows with `team_id` (adjacent rows meet in round 1). |
| `ratings` | `DataFrame` |  | One row per team (`team_id, adj_em`). |
| `n_sims` | `int` | `10000` | Number of simulated brackets. |
| `seed` | `int` | `0` | RNG seed (deterministic output). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per field team: `team_id, seed?, reach_r32 .. champion` probabilities -- see the mbb core for the full contract.

**Example**

```python
from sportsdataverse.wbb import wbb_bracket_sim
odds = wbb_bracket_sim(field_64, ratings, n_sims=20000, seed=42)
```

### wbb_bracketology {#wbb_bracketology}

`wbb_bracketology(season: 'int', *, as_of_date: 'Union[datetime.date, None]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Women's projected tournament field for a season.

Delegates to `sportsdataverse.mbb.mbb_bracketology.mbb_bracketology` with `league="womens"` (WBB loaders + women's constants).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season to project (e.g. `2024`). |
| `as_of_date` | `Union[date, None]` | `None` | Only use games strictly before this date; `None` uses every completed game. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team: `season, team_id, resume_score, projected_seed, at_large_prob, auto_bid, bid` -- see the mbb core for the full contract.

**Example**

```python
from sportsdataverse.wbb import wbb_bracketology
field = wbb_bracketology(2024)
```

### wbb_in_game_win_prob {#wbb_in_game_win_prob}

`wbb_in_game_win_prob(pbp: 'pl.DataFrame', pregame_home_prob: 'float', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Women's per-play in-game win probability (bundled `wbb_in_game_wp.ubj`).

Delegates to `sportsdataverse.mbb.mbb_game_predict.mbb_in_game_win_prob` with `league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | One game's plays in the `load_wbb_pbp` schema. |
| `pregame_home_prob` | `float` |  | Pregame home win probability. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per play: the five feature columns plus `home_win_prob` -- see the mbb core for the full contract.

**Example**

```python
from sportsdataverse.wbb import wbb_in_game_win_prob
wp = wbb_in_game_win_prob(pbp, 0.62)
```

### wbb_pbp_disk {#wbb_pbp_disk}

`wbb_pbp_disk(game_id, path_to_json)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  |  |
| `path_to_json` |  |  |  |

### wbb_player_crosswalk {#wbb_player_crosswalk}

`wbb_player_crosswalk(season: 'Optional[int]' = None, min_confidence: 'float' = 0.92, *, return_as_pandas: 'bool' = False, strict: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the WBB cross-source player crosswalk (ESPN / Fox).

One row per ESPN athlete per team. Fox is matched by normalized name
within each team block -- exact first, then Jaro-Winkler at or above
`min_confidence` with a jersey tiebreak. Torvik has no per-player table
for WBB, so it is not joined; Yahoo columns are null placeholders.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year (e.g. `2026`). Defaults to the most recent WBB season. |
| `min_confidence` | `float` | `0.92` | Jaro-Winkler floor for fuzzy matches (R default 0.92). |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |
| `strict` | `bool` | `False` | Raise on the first failed per-team ESPN or Fox roster fetch (a 404 is still skipped) instead of skipping isolated failures. Default `False` matches the R producers; a provider whose every item failed raises either way. An item the host *answered* -- including a 404 -- counts as answered. |

**Returns**

`pl.DataFrame` (or pandas), one row per ESPN athlete, 17 columns ending in `match_method` / `match_confidence` / `match_keys`.

**Example**

```python
from sportsdataverse.wbb import wbb_player_crosswalk
df = wbb_player_crosswalk(season=2026)
print(df["match_method"].value_counts())

# Tighten the fuzzy floor

strict = wbb_player_crosswalk(season=2026, min_confidence=0.97)

# Pipeline next step (one line)

df.filter(pl.col("match_method") == "fuzzy_jw").head()
```

### wbb_predict_games {#wbb_predict_games}

`wbb_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Women's vectorized pregame predictions over a schedule.

Delegates to `sportsdataverse.mbb.mbb_game_predict.mbb_predict_games` with `league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | One row per game (`game_id, home_team_id, away_team_id` and optionally `neutral_site`). |
| `ratings` | `DataFrame` |  | One row per team (the `wbb_team_ratings` output). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per input game: `game_id, home_team_id, away_team_id, exp_margin, home_win_prob, exp_total` -- see the mbb core for the full contract.

**Example**

```python
from sportsdataverse.wbb import wbb_predict_games, wbb_team_ratings
preds = wbb_predict_games(games, wbb_team_ratings(2024))
```

### wbb_schedule_crosswalk {#wbb_schedule_crosswalk}

`wbb_schedule_crosswalk(season: 'Optional[int]' = None, *, return_as_pandas: 'bool' = False, strict: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the WBB cross-source schedule crosswalk (ESPN / Torvik).

One row per game. Dates are reduced to the Eastern-Time game date before
joining and Torvik's unordered `team1`/`team2` join through a sorted
ESPN team-pair key, so home/away is taken from the ESPN side only. Torvik
games whose teams cannot be resolved to ESPN ids survive as `bart_only`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year (e.g. `2026`). Defaults to the most recent WBB season. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |
| `strict` | `bool` | `False` | Raise on the first failed per-date ESPN scoreboard fetch (a 404 is still skipped) instead of skipping isolated failures. Default `False` matches the R producers; a provider whose every item failed raises either way. An item the host *answered* -- including a 404 -- counts as answered. |

**Returns**

`pl.DataFrame` (or pandas) with `SCHEDULE_COLUMNS`.

**Example**

```python
from sportsdataverse.wbb import wbb_schedule_crosswalk
df = wbb_schedule_crosswalk(season=2026)
print(df["match_method"].value_counts())

# Pipeline next step (one line)

df.filter(pl.col("match_method") == "both").select("espn_game_id", "bart_muid").head()
```

### wbb_season_sim {#wbb_season_sim}

`wbb_season_sim(ratings: 'pl.DataFrame', remaining_schedule: 'pl.DataFrame', *, n_sims: 'int' = 10000, seed: 'int' = 0, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Women's remaining-schedule Monte Carlo.

Delegates to `sportsdataverse.mbb.mbb_season_sim.mbb_season_sim` with `league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ratings` | `DataFrame` |  | One row per team (`season, team_id, adj_em` + optional `conference` / `current_wins`). |
| `remaining_schedule` | `DataFrame` |  | Games to simulate. |
| `n_sims` | `int` | `10000` | Number of simulated seasons. |
| `seed` | `int` | `0` | RNG seed (deterministic output). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team: `season, team_id, exp_wins, playoff_prob, conf_title_prob` -- see the mbb core for the full contract.

**Example**

```python
from sportsdataverse.wbb import wbb_season_sim
odds = wbb_season_sim(ratings, remaining, n_sims=5000, seed=42)
```

### wbb_strength_of_schedule {#wbb_strength_of_schedule}

`wbb_strength_of_schedule(seasons: 'list[int]', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Women's season-level SoS / Quad / WAB résumé.

Delegates to `sportsdataverse.mbb.mbb_strength_of_schedule.mbb_strength_of_schedule` with `league="womens"` (WBB loaders + women's constants).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to compute (e.g. `[2024]`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (season, team_id): `season, team_id, sos, sos_rank, wab, quad1_w .. quad4_l, quality_wins` -- see the mbb core for the full contract.

**Example**

```python
from sportsdataverse.wbb import wbb_strength_of_schedule
wbb_strength_of_schedule([2024]).sort("wab", descending=True).head(20)
```

### wbb_team_crosswalk {#wbb_team_crosswalk}

`wbb_team_crosswalk(season: 'Optional[int]' = None, *, fox: 'Optional[pl.DataFrame]' = None, bart: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, strict: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the WBB cross-source team crosswalk (ESPN / Fox / Torvik).

One row per ESPN team, keyed on `espn_team_id`. Fox is joined on the
normalized full mascot name (with the curated `FOX_DISPLAY_ALIAS`
bridge); Torvik on the normalized school name after the
`BART_ALIAS` pass. Yahoo columns are null placeholders.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year (e.g. `2026`). Defaults to the most recent WBB season. |
| `fox` | `Optional[DataFrame]` | `None` | Pre-fetched frame with `fox_team_id` / `fox_team_name` / `fox_section`. `None` fetches *season*'s conference standings live (`~sportsdataverse._crosswalk_basketball_sources.fox_season_teams`); Fox has none before 2018-19, so earlier seasons get null `fox_*`. Pass an empty frame to skip Fox entirely. |
| `bart` | `Optional[DataFrame]` | `None` | Pre-fetched `bart_wbb_ratings()` frame. `None` fetches live; women's Torvik starts in 2021, so earlier seasons get null `bart_*`. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |
| `strict` | `bool` | `False` | Accepted for parity with the schedule and player crosswalks, which forward it; the team build has no per-item fetch loop to relax, so every source failure raises. |

**Returns**

`pl.DataFrame` (or pandas), one row per ESPN team, with `TEAM_COLUMNS`.

**Example**

```python
from sportsdataverse.wbb import wbb_team_crosswalk
df = wbb_team_crosswalk(season=2026)
print(df.shape)

# Skip Fox

import polars as pl
df = wbb_team_crosswalk(season=2026, fox=pl.DataFrame())

# Pipeline next step (one line)

df.filter(pl.col("match_method") == "fox+bart").head()
```

### wbb_team_ratings {#wbb_team_ratings}

`wbb_team_ratings(seasons: 'Union[int, list[int]]', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Women's opponent-adjusted team ratings (AdjO/AdjD/AdjEM/AdjTempo).

Delegates to `sportsdataverse.mbb.mbb_team_ratings.mbb_team_ratings`
with `league="womens"` (WBB loaders + women's fitted constants).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | A season (e.g. `2024`) or list of seasons. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per (season, team_id) -- see the mbb core for the schema.

**Example**

```python
from sportsdataverse.wbb import wbb_team_ratings
wbb_team_ratings(2024).sort("rank").head()
```
