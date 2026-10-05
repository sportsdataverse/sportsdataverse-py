---
title: "MBB — additional Python functions — Mbb"
sidebar_label: "Mbb"
sidebar_position: 7
description: "MBB — additional Python functions — Mbb — function reference in sdv-py, the SportsDataverse Python package."
---
# MBB — additional Python functions — Mbb

### mbb_archetypes {#mbb_archetypes}

`mbb_archetypes(seasons: "'Union[int, list[int]]'", *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-player-season role archetype from the bundled KMeans centers.

Aggregates the season's player boxscores, builds the per-100 feature
vector (+ roster position score), standardizes with the artifact's
fit-time mean/sd, and assigns each player-season to the nearest center.
`dist_to_center` is the euclidean distance in z-space -- small = a
prototypical example of the archetype, large = a hybrid.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | A season (e.g. `2025`) or list of seasons. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the bundled artifact). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (player_id, season, team_id): `player_id:Utf8, player, season, team_id:Utf8, min, archetype, cluster:Int64, dist_to_center:Float64`. Empty input returns the schema with zero rows.

**Example**

```python
from sportsdataverse.mbb import mbb_archetypes
roles = mbb_archetypes(2025)

# Pipeline next step (one line)

roles.filter(pl.col("archetype") == "rim protector").sort("dist_to_center").head(10)
```

### mbb_box_bpm {#mbb_box_bpm}

`mbb_box_bpm(seasons: "'Union[int, list[int]]'", *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-player-season box Plus/Minus (offense, defense, total).

Aggregates the season's player boxscores, scores the per-100 features
through the bundled team-constrained coefficients, and applies the BPM
team adjustment so each team's minutes-weighted player scores sum to its
adjusted efficiency margin (points per 100 possessions above league
average; positive = good on both ends).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | A season (e.g. `2025`) or list of seasons. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the bundled artifact). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (player_id, season, team_id): `player_id:Utf8, player, season, team_id:Utf8, min, box_obpm, box_dbpm, box_bpm`. Empty input returns the schema with zero rows.

**Example**

```python
from sportsdataverse.mbb import mbb_box_bpm
bpm = mbb_box_bpm(2025)

# Pipeline next step (one line)

bpm.filter(pl.col("min") >= 400).sort("box_bpm", descending=True).head(15)
```

### mbb_bracket_sim {#mbb_bracket_sim}

`mbb_bracket_sim(seeded_field: 'pl.DataFrame', ratings: 'pl.DataFrame', *, n_sims: 'int' = 10000, seed: 'int' = 0, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Single-elimination Monte Carlo over a bracket-ordered field.

Rows of `seeded_field` are bracket slots: adjacent rows meet in round 1
and winners of adjacent games meet next round (the standard fold). All
games are neutral-site. Round columns are named from the END of a 64-team
bracket (`champion` back to `reach_r32`); with a smaller field the
early columns are 1.0 for everyone (trivially reached).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seeded_field` | `DataFrame` |  | Bracket-ordered rows with `team_id` (and typically `seed` for reference). |
| `ratings` | `DataFrame` |  | One row per team: `team_id, adj_em`. |
| `n_sims` | `int` | `10000` | Number of simulated brackets. |
| `seed` | `int` | `0` | Seed for `numpy.random.default_rng`. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per field team: `team_id, seed?, reach_r32, reach_s16, reach_e8, reach_f4, reach_final, champion` (probabilities).

**Example**

```python
from sportsdataverse.mbb.mbb_season_sim import mbb_bracket_sim
odds = mbb_bracket_sim(field_64, ratings, n_sims=20000, seed=42)
```

### mbb_bracketology {#mbb_bracketology}

`mbb_bracketology(season: 'int', *, as_of_date: 'datetime.date | None' = None, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Projected tournament field for a season from the released ESPN data.

Builds ratings + résumé (optionally as of a date -- games on or after
`as_of_date` are excluded), resolves conference auto-bids from the
standings, and selects/seeds the 68-team field via
`project_bracket`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season to project (e.g. `2024`). |
| `as_of_date` | `date \| None` | `None` | Only use games strictly before this date (Selection-Sunday style snapshots); `None` uses every completed game. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team -- see `project_bracket`.

**Example**

```python
from sportsdataverse.mbb import mbb_bracketology
field = mbb_bracketology(2024)

# Pipeline next step (one line)

field.filter(pl.col("bid") == True).sort("projected_seed")
```

### mbb_draft_projection {#mbb_draft_projection}

`mbb_draft_projection(seasons: "'Union[int, list[int]]'", *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Draft probability, projected pick, and pro tier per player-season.

`draft_prob` is the probability of being selected in the draft
immediately following the college season; `projected_pick` is the
expected overall pick conditional on being drafted (lower = better);
`pro_tier` buckets the pick through the bundled tier edges.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | A season (e.g. `2025`, feeding the June 2025 draft) or list of seasons. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the bundled artifact; womens = WNBA draft). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per qualifying player-season: `player_id:Utf8, player, season, team_id:Utf8, draft_prob, projected_pick, pro_tier`. Empty input returns the schema with zero rows.

**Example**

```python
from sportsdataverse.mbb import mbb_draft_projection
board = mbb_draft_projection(2025)

# Pipeline next step (one line)

board.sort("draft_prob", descending=True).head(30)
```

### mbb_in_game_win_prob {#mbb_in_game_win_prob}

`mbb_in_game_win_prob(pbp: 'pl.DataFrame', pregame_home_prob: 'float', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Per-play home win probability from the bundled in-game logistic.

Scores `in_game_features` through the committed artifact
(`sportsdataverse/mbb/models`, trained on the season before the pregame
gate season so the calibration backtest stays out-of-sample).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play for ONE game in the `load_mbb_pbp` schema (`start_game_seconds_remaining`, `home_score`, `away_score`, `team_id`, `home_team_id`). |
| `pregame_home_prob` | `float` |  | Pregame home win probability (e.g. from `win_prob_from_margin`). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the bundled artifact). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per play: the five feature columns plus `home_win_prob`.

**Example**

```python
from sportsdataverse.mbb.mbb_game_predict import mbb_in_game_win_prob
from sportsdataverse.mbb.mbb_loaders import load_mbb_pbp
pbp = load_mbb_pbp([2024]).filter(pl.col("game_id") == 401638643)
wp = mbb_in_game_win_prob(pbp, 0.62)
```

### mbb_pbp_disk {#mbb_pbp_disk}

`mbb_pbp_disk(game_id, path_to_json)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  |  |
| `path_to_json` |  |  |  |

### mbb_player_crosswalk {#mbb_player_crosswalk}

`mbb_player_crosswalk(season: 'Optional[int]' = None, min_confidence: 'float' = 0.92, *, return_as_pandas: 'bool' = False, strict: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the MBB cross-source player crosswalk (ESPN / Fox).

One row per ESPN athlete per team. Fox is matched by normalized name
within each team block -- exact first (jersey-tiebroken, per hoopR), then
Jaro-Winkler at or above `min_confidence`. KenPom and Torvik publish no
per-player tables, so neither is joined.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year (e.g. `2026`). Defaults to the most recent MBB season. |
| `min_confidence` | `float` | `0.92` | Jaro-Winkler floor for fuzzy matches (R default 0.92). |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |
| `strict` | `bool` | `False` | Raise on the first failed per-team ESPN or Fox roster fetch (a 404 is still skipped) instead of skipping isolated failures. Default `False` matches the R producers; a provider whose every item failed raises either way. An item the host *answered* -- including a 404 -- counts as answered. |

**Returns**

`pl.DataFrame` (or pandas), one row per ESPN athlete, 17 columns.

**Example**

```python
from sportsdataverse.mbb import mbb_player_crosswalk
df = mbb_player_crosswalk(season=2026)
print(df["match_method"].value_counts())

# Tighten the fuzzy floor

strict = mbb_player_crosswalk(season=2026, min_confidence=0.97)

# Pipeline next step (one line)

df.filter(pl.col("match_method") == "fuzzy_jw").head()
```

### mbb_predict_games {#mbb_predict_games}

`mbb_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Vectorized pregame predictions for a schedule of games.

Joins the ratings frame twice (home / away) and applies the closed-form
`predict_margin` / `win_prob_from_margin` /
`predict_total` math column-wise.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | One row per game with `game_id`, `home_team_id`, `away_team_id` and optionally `neutral_site` (missing column means every game is a true home game). Team-id dtypes must match `ratings['team_id']` exactly. |
| `ratings` | `DataFrame` |  | One row per team with `team_id, adj_o, adj_d, adj_em, adj_tempo` (the `mbb_team_ratings` output for one season / as-of date). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per input game: `game_id, home_team_id, away_team_id, exp_margin, home_win_prob, exp_total`. Games whose teams are missing from `ratings` carry nulls.

**Example**

```python
from sportsdataverse.mbb.mbb_game_predict import mbb_predict_games
from sportsdataverse.mbb.mbb_team_ratings import mbb_team_ratings
preds = mbb_predict_games(games, mbb_team_ratings([2024]))
```

### mbb_recruiting_projection {#mbb_recruiting_projection}

`mbb_recruiting_projection(seasons: "'Union[int, list[int]]'", *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Expected freshman box-BPM per recruit + over/under-performance residual.

Scores each recruit of the season's incoming class through the bundled
recruiting ridge (composite grade + log national rank; missing values
imputed with the class median / the bubble rank). When the freshman
season is already observable, `resume_residual = realized box_bpm -
exp_box_bpm` (null otherwise, and `player_id` carries the matched
college athlete id).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | Freshman college season(s) (e.g. `2025` = the class arriving for 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the bundled artifact). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per recruit: `recruit_id:Utf8, player_id:Utf8 (nullable), player, season, team_id:Utf8, composite, rank_nat, exp_box_bpm, resume_residual`. Empty input returns the schema with zero rows.

**Example**

```python
from sportsdataverse.mbb import mbb_recruiting_projection
proj = mbb_recruiting_projection(2026)

# Pipeline next step (one line)

proj.sort("exp_box_bpm", descending=True).head(15)
```

### mbb_schedule_crosswalk {#mbb_schedule_crosswalk}

`mbb_schedule_crosswalk(season: 'Optional[int]' = None, *, return_as_pandas: 'bool' = False, strict: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the MBB cross-source schedule crosswalk (ESPN / Torvik).

One row per game. Dates reduce to the Eastern-Time game date before
joining and Torvik's unordered `team1`/`team2` join through a sorted
ESPN team-pair key. Torvik games whose teams cannot be resolved to ESPN
ids are dropped (the MBB variant differs from WBB here). `kp_game_id` is
a null placeholder -- the R builder's optional KenPom enrichment needs a
paid subscription and is not ported.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year (e.g. `2026`). Defaults to the most recent MBB season. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |
| `strict` | `bool` | `False` | Raise on the first failed per-date ESPN scoreboard fetch (a 404 is still skipped) instead of skipping isolated failures. Default `False` matches the R producers; a provider whose every item failed raises either way. An item the host *answered* -- including a 404 -- counts as answered. |

**Returns**

`pl.DataFrame` (or pandas) with `SCHEDULE_COLUMNS`.

**Example**

```python
from sportsdataverse.mbb import mbb_schedule_crosswalk
df = mbb_schedule_crosswalk(season=2026)
print(df["match_method"].value_counts())

# Pipeline next step (one line)

df.filter(pl.col("match_method") == "both").select("espn_game_id", "bart_muid").head()
```

### mbb_season_sim {#mbb_season_sim}

`mbb_season_sim(ratings: 'pl.DataFrame', remaining_schedule: 'pl.DataFrame', *, n_sims: 'int' = 10000, seed: 'int' = 0, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Monte Carlo the remaining schedule: expected wins + title odds.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ratings` | `DataFrame` |  | One row per team: `season, team_id, adj_em` and optionally `conference` (enables `conf_title_prob`) and `current_wins` (added to the simulated remaining wins). |
| `remaining_schedule` | `DataFrame` |  | Games to simulate: `home_team_id, away_team_id, neutral_site`. |
| `n_sims` | `int` | `10000` | Number of simulated seasons. |
| `seed` | `int` | `0` | Seed for `numpy.random.default_rng` (deterministic output). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team: `season, team_id, exp_wins` (mean simulated total wins), `playoff_prob` (share of sims finishing in the top 68 win totals -- a field-size proxy, ties broken by `adj_em`) and `conf_title_prob` (share of sims with the most wins among conference members; ties count for every tied team; null without a `conference` column).

**Example**

```python
from sportsdataverse.mbb.mbb_season_sim import mbb_season_sim
odds = mbb_season_sim(ratings, remaining, n_sims=5000, seed=42)
```

### mbb_shooter_talent {#mbb_shooter_talent}

`mbb_shooter_talent(scored: 'pl.DataFrame', *, league: 'str' = 'mens', k: "'float | None'" = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-shooter EB-regressed make% over expected + points over expected.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored` | `DataFrame` |  | `mbb_shot_quality` output (needs `shooter_id, made, point_value, xmake, xpoints`). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (default `k` source). |
| `k` | `float \| None` | `None` | Shrinkage pseudo-shots; `None` uses `get_constants(league).shrink_k_talent` (fitted split-half). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per shooter: `shooter_id:Utf8, n_shots, make_rate, xmake_mean, oe_pct, oe_pct_regressed, points_over_expected, poe_per_100`. Empty input returns the zero-row schema.

**Example**

```python
from sportsdataverse.mbb import mbb_shot_data, mbb_shot_quality, mbb_shooter_talent
talent = mbb_shooter_talent(mbb_shot_quality(mbb_shot_data(2025)))

# Pipeline next step (one line)

talent.filter(pl.col("n_shots") >= 200).sort("oe_pct_regressed", descending=True).head(15)
```

### mbb_shot_data {#mbb_shot_data}

`mbb_shot_data(seasons: "'int | list[int]'", *, source: 'str' = 'espn', league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

Season(s) of shots in the canonical frame (the spine's data entry point).

`source="espn"` loads the sportsdataverse-data shots release
(`load_mbb_shots` / `load_wbb_shots`) and canonicalizes it. The NCAA
HTML path is per-game, not per-season -- parse with
`create_shot_event_data` and flatten via `shot_events_to_frame`
instead (`source="ncaa"` raises with that pointer).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season (e.g. `2025`) or list of seasons. |
| `source` | `str` | `'espn'` | `"espn"` (the only batch source). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The canonical shot frame; seasons the release doesn't cover are skipped, and no coverage at all returns the zero-row schema.

**Example**

```python
from sportsdataverse.mbb import mbb_shot_data
shots = mbb_shot_data(2025)

# Pipeline next step (one line)

shots.group_by("shot_zone").agg(pl.col("made").mean()).sort("shot_zone")
```

### mbb_shot_quality {#mbb_shot_quality}

`mbb_shot_quality(shots: 'pl.DataFrame', *, model: "'pl.DataFrame | None'" = None, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Score each shot with `xmake` / `xpoints` from the cell table.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shots` | `DataFrame` |  | Canonical shot frame. |
| `model` | `DataFrame \| None` | `None` | A `mbb_shot_quality_model` table. When `None` it is built from `shots` itself -- convenient, but leakage-safe evaluation should pass a model fit on PRIOR data. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`shots`'s columns plus `xmake:Float64, xpoints:Float64` (null for cells absent from the model). Empty input returns the input schema plus the two columns, zero rows.

**Example**

```python
from sportsdataverse.mbb import mbb_shot_data, mbb_shot_quality
scored = mbb_shot_quality(mbb_shot_data(2025))

# Pipeline next step (one line)

scored.group_by("team_id").agg(pl.col("xpoints").sum()).sort("xpoints", descending=True)
```

### mbb_shot_quality_model {#mbb_shot_quality_model}

`mbb_shot_quality_model(shots: 'pl.DataFrame', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Empirical-Bayes `zone x type` make-rate / xPoints table.

Each cell's raw make rate is shrunk toward its PARENT-ZONE mean by
`n / (n + k)` with `k = get_constants(league).shrink_k_zone`
pseudo-attempts, so sparse cells (e.g. tip-ins in the mid zone) borrow
strength from their zone; `xpoints = make_rate_shrunk * point_value`
(the cell's modal point value).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shots` | `DataFrame` |  | Canonical shot frame (needs `shot_zone, shot_type, made, point_value`). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the shrinkage `k`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `(shot_zone, shot_type)`: `shot_zone, shot_type, n, make_rate_raw, make_rate_shrunk, point_value, xpoints`. Empty input returns the zero-row schema.

**Example**

```python
from sportsdataverse.mbb import mbb_shot_data, mbb_shot_quality_model
model = mbb_shot_quality_model(mbb_shot_data(2025))

# Pipeline next step (one line)

model.sort("xpoints", descending=True).head(5)
```

### mbb_shot_selection {#mbb_shot_selection}

`mbb_shot_selection(scored: 'pl.DataFrame', *, group: 'str' = 'shooter_id', league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per shooter/team expected points per attempt vs the league-average mix.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored` | `DataFrame` |  | `mbb_shot_quality` output (needs `xpoints, point_value, made` + the group column). |
| `group` | `str` | `'shooter_id'` | `"shooter_id"` or `"team_id"`. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (interface parity; the math is league-free). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per group: `{group}:Utf8, n_shots:Int64, xppp, actual_ppp, selection_value, selection_value_total` (all value columns Float64). The attempt-weighted `selection_value` sums to zero across the league. Empty input returns the zero-row schema.

**Example**

```python
from sportsdataverse.mbb import mbb_shot_data, mbb_shot_quality, mbb_shot_selection
sel = mbb_shot_selection(mbb_shot_quality(mbb_shot_data(2025)), group="team_id")

# Pipeline next step (one line)

sel.sort("selection_value", descending=True).head(10)
```

### mbb_strength_of_schedule {#mbb_strength_of_schedule}

`mbb_strength_of_schedule(seasons: 'list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Season-level SoS / Quad / WAB résumé from the released ESPN data.

Loads the schedule + team boxscores, builds the opponent-adjusted ratings,
and applies `strength_of_schedule` per season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to compute (e.g. `[2024]`). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (season, team_id) -- see `strength_of_schedule`.

**Example**

```python
from sportsdataverse.mbb import mbb_strength_of_schedule
resume = mbb_strength_of_schedule([2024])

# Pipeline next step (one line)

resume.sort("wab", descending=True).head(20)
```

### mbb_team_crosswalk {#mbb_team_crosswalk}

`mbb_team_crosswalk(season: 'Optional[int]' = None, *, fox: 'Optional[pl.DataFrame]' = None, bart: 'Optional[pl.DataFrame]' = None, kenpom: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, strict: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the MBB cross-source team crosswalk (ESPN / Fox / Torvik / KenPom).

One row per ESPN team, keyed on `espn_team_id`. Fox joins on the
normalized mascot name via `FOX_DISPLAY_ALIAS`; Torvik and KenPom
each join on the normalized school name after `BART_ALIAS` /
`KP_ALIAS`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year (e.g. `2026`). Defaults to the most recent MBB season. |
| `fox` | `Optional[DataFrame]` | `None` | Pre-fetched frame with `fox_team_id` / `fox_team_name` / `fox_section`. `None` fetches *season*'s conference standings live (`~sportsdataverse._crosswalk_basketball_sources.fox_season_teams`); Fox has none before 2017-18, so earlier seasons get null `fox_*`. Pass an empty frame to skip Fox. |
| `bart` | `Optional[DataFrame]` | `None` | Pre-fetched `torvik_ratings()` frame. `None` fetches live; Torvik starts in 2008, so earlier seasons get null `bart_*`. |
| `kenpom` | `Optional[DataFrame]` | `None` | KenPom teams frame with `Team` / `Conf`. `None` (the default) uses the KenPom team/conference directory bundled with sdv-py (hoopR's `teams_links`, seasons 2002-2026), filtered to *season*. A season the bundle does not carry gets null `kp_*` columns -- never another season's labels. Pass an empty frame to skip KenPom. No KenPom subscription or credential is involved: the bundled data is the public directory, not ratings. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |
| `strict` | `bool` | `False` | Accepted for parity with the schedule and player crosswalks, which forward it; the team build has no per-item fetch loop to relax, so every source failure raises. |

**Returns**

`pl.DataFrame` (or pandas), one row per ESPN team, with `TEAM_COLUMNS`.

**Example**

```python
from sportsdataverse.mbb import mbb_team_crosswalk
df = mbb_team_crosswalk(season=2026)
print(df.shape)

# Skip Fox

import polars as pl
df = mbb_team_crosswalk(season=2026, fox=pl.DataFrame())

# Pipeline next step (one line)

df.filter(pl.col("match_method") == "espn_only").select("espn_display_name").head()
```

### mbb_team_ratings {#mbb_team_ratings}

`mbb_team_ratings(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Opponent-adjusted team ratings (AdjO/AdjD/AdjEM/AdjTempo) per team-season.

Loads schedule + team boxscore for `seasons`, computes per-game efficiency,
runs the opponent-adjustment fixed points, and adds a per-season dense
`rank` (on `adj_em` descending) and `adj_em_z` (z-score of `adj_em`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season (e.g. `2024`) or list of seasons. |
| `league` | `str` | `'mens'` | `"mens"` / `"womens"` -- selects the constants. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per (season, team_id) with columns `season, team_id, adj_o, adj_d, adj_em, adj_tempo, raw_o, raw_d, games, rank, adj_em_z`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.mbb.mbb_team_ratings import mbb_team_ratings
ratings = mbb_team_ratings(2024)
ratings.sort("rank").head()
```

### mbb_transfer_projection {#mbb_transfer_projection}

`mbb_transfer_projection(seasons: "'Union[int, list[int]]'", *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Projected post-transfer box-BPM for each transfer arriving in `seasons`.

Detects the transfer cohort from BOXSCORE discontinuity -- a player who
logged qualifying minutes for different teams in consecutive seasons
(the roster release under-reports moves ~70x, so production is the
cohort source of record; bench-riders pre-move are excluded, which is
fine because they carry no pre production to project from). Joins each
player's pre-transfer (from-season) `box_bpm` and scores the bundled
ridge. `proj_delta = proj_box_bpm - pre_box_bpm` (the expected
move-related change; typically shrinks stars toward the mean).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | Destination season(s), e.g. `2026` = arrived for 2025-26. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the bundled artifact). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per transfer: `player_id:Utf8, player, from_team_id:Utf8, to_team_id:Utf8, to_season:Int64, pre_box_bpm, proj_box_bpm, proj_delta`. Transfers without a qualifying pre-season sample are dropped. Empty input returns the schema with zero rows.

**Example**

```python
from sportsdataverse.mbb import mbb_transfer_projection
proj = mbb_transfer_projection(2026)

# Pipeline next step (one line)

proj.sort("proj_box_bpm", descending=True).head(15)
```
