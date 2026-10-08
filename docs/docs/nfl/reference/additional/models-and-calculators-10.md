---
title: "NFL — additional Python functions — Models and calculators: nfl_simulations–team_pressure"
sidebar_label: "Models and calculators: nfl_simulations–team_pressure"
sidebar_position: 23
description: "NFL — additional Python functions — Models and calculators: nfl_simulations–team_pressure — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Models and calculators: nfl_simulations–team_pressure

### nfl_simulations {#nfl_simulations}

`nfl_simulations(games: 'pl.DataFrame', compute_results: 'Optional[ComputeResultsFn]' = None, *, simulations: 'int' = 10000, playoff_seeds: 'int' = 7, byes_per_conf: 'int' = 1, tiebreaker_depth: 'str' = 'SOS', sim_include: 'str' = 'DRAFT', seed: 'Optional[int]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Dict[str, Union[pl.DataFrame, 'pd.DataFrame']]"`

Simulate an NFL season from a schedule with (partially) missing results.

Faithful port of `nflseedR::nfl_simulations()` +
`simulate_chunk()` (simulations.R L140-409,
simulations_simulate_chunks.R L1-284). Missing regular season results
are filled week by week via `compute_results`; standings, division
ranks and playoff seeds are then computed with the full NFL tiebreakers,
the postseason is simulated round by round (with reseeding and
`byes_per_conf` byes), and the draft order is derived. nflseedR's
furrr chunking is replaced by one vectorized pass over all simulated
seasons, so there is no `chunks` argument; reproducibility comes from
`seed`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | Schedule frame for ONE season with columns `sim` or `season`, `game_type`, `week`, `away_team`, `home_team`, `away_rest`, `home_rest`, `location`, and `result` (home margin; missing = not yet played). |
| `compute_results` | `Optional[ComputeResultsFn]` | `None` | Function filling results for one week, called as `compute_results(teams, games, week_num, rng=rng, **kwargs)` and returning `{"teams": ..., "games": ...}`. Defaults to `nfl_compute_results` (dynamic ELO + Normal(estimate, 13) margins). Must only fill results where `week == week_num` and `result` is missing, and must not produce postseason ties. |
| `simulations` | `int` | `10000` | Number of seasons to simulate. |
| `playoff_seeds` | `int` | `7` | Number of playoff seeds per conference. |
| `byes_per_conf` | `int` | `1` | First-round byes per conference (drives the number of wildcard games). |
| `tiebreaker_depth` | `str` | `'SOS'` | `'SOS'` (default), `'PRE-SOV'`, or `'RANDOM'` (`'POINTS'` is unavailable because simulated games carry margins, not scores). |
| `sim_include` | `str` | `'DRAFT'` | `'REG'` (standings/seeds only), `'POST'` (+ postseason), or `'DRAFT'` (default; + draft order). |
| `seed` | `Optional[int]` | `None` | Seed for the numpy RNG driving results and coin tosses. |
| `return_as_pandas` | `bool` | `False` | If `True`, return pandas DataFrames. |

**Returns**

Dict of frames mirroring the nflseedR simulation list: `standings` (one row per sim x team), `games` (all simulated games), `overall` (per-team probabilities: wins, playoff, div1, seed1, won_conf, won_sb, draft1, draft5), `team_wins` (over/under probabilities vs. half-win lines), and `game_summary` (per-matchup home/away win rates).

| col_name | type | description |
|---|---|---|
| `standings.sim` | integer | Simulated season identifier (1 through `simulations`). |
| `standings.conf` | character | Conference of the team (AFC or NFC). |
| `standings.division` | character | Division of the team (e.g. "AFC East"). |
| `standings.team` | character | Team abbreviation. |
| `standings.games` | integer | Number of regular season games played in the simulated season. |
| `standings.wins` | double | Regular season wins in the simulated season with ties counted as half a win. |
| `standings.true_wins` | integer | Regular season wins in the simulated season excluding ties. |
| `standings.losses` | integer | Regular season losses in the simulated season. |
| `standings.ties` | integer | Regular season ties in the simulated season. |
| `standings.win_pct` | double | Regular season win percentage in the simulated season with ties counted as half a win. |
| `standings.div_pct` | double | Win percentage against division opponents in the simulated season (0 when no division games). |
| `standings.conf_pct` | double | Win percentage against conference opponents in the simulated season (0 when no conference games). |
| `standings.sov` | double | Strength of victory in the simulated season - combined win percentage of all defeated opponents. |
| `standings.sos` | double | Strength of schedule in the simulated season - combined win percentage of all opponents faced. |
| `standings.div_rank` | integer | Division rank (1-4) in the simulated season after the NFL division tiebreakers. |
| `standings.div_tie_broken_by` | character | Tiebreaker step that resolved the division rank in this simulated season; null when no tiebreaker was needed. |
| `standings.conf_rank` | integer | Conference rank (playoff seed) in the simulated season after the NFL conference tiebreakers; null beyond `playoff_seeds`. |
| `standings.conf_tie_broken_by` | character | Tiebreaker step that resolved the conference rank in this simulated season; null when no tiebreaker was needed. |
| `standings.exit` | character | Round of the team's final game in the simulated season - REG, WC, DIV, CON, SB, or SB_WIN for the Super Bowl winner. |
| `standings.draft_rank` | integer | Draft pick position (1 = first overall) in the simulated season (present when sim_include="DRAFT"). |
| `standings.draft_tie_broken_by` | character | Tiebreaker step that resolved the draft rank in this simulated season; null when no tiebreaker was needed. |
| `games.sim` | integer | Simulated season identifier the game row belongs to. |
| `games.game_type` | character | Game type - REG for regular season or the playoff round (WC, DIV, CON, SB). |
| `games.week` | integer | Week number of the game; simulated playoff rounds are numbered from the last regular season week (+1 for WC through +4 for SB). |
| `games.away_team` | character | Team abbreviation of the away team (simulated playoff matchups are filled by seed). |
| `games.home_team` | character | Team abbreviation of the home team (simulated playoff matchups are filled by seed). |
| `games.away_rest` | integer | Days of rest for the away team before the game. |
| `games.home_rest` | integer | Days of rest for the home team before the game (14 for the top seed's divisional round game). |
| `games.location` | character | Game site indicator - "Home" or "Neutral" (Super Bowl). |
| `games.result` | integer | Home margin (home score minus away score); real where the input schedule had one, simulated otherwise. |
| `overall.conf` | character | Conference of the team (AFC or NFC). |
| `overall.division` | character | Division of the team (e.g. "AFC East"). |
| `overall.team` | character | Team abbreviation. |
| `overall.wins` | double | Mean regular season wins across all simulated seasons (ties counted as half a win). |
| `overall.playoff` | double | Share of simulated seasons in which the team made the playoffs (conference rank within `playoff_seeds`). |
| `overall.div1` | double | Share of simulated seasons in which the team won its division. |
| `overall.seed1` | double | Share of simulated seasons in which the team earned the conference number one seed. |
| `overall.won_conf` | double | Share of simulated seasons in which the team won the conference championship; null when sim_include="REG". |
| `overall.won_sb` | double | Share of simulated seasons in which the team won the Super Bowl; null when sim_include="REG". |
| `overall.draft1` | double | Share of simulated seasons in which the team held the first overall draft pick; null unless sim_include="DRAFT". |
| `overall.draft5` | double | Share of simulated seasons in which the team held a top-five draft pick; null unless sim_include="DRAFT". |
| `team_wins.team` | character | Team abbreviation. |
| `team_wins.wins` | double | Half-win line the over/under probabilities are evaluated against (0, 0.5, ... up to the number of regular season games). |
| `team_wins.over_prob` | double | Probability across simulated seasons that the team's outright win total exceeds the line. |
| `team_wins.under_prob` | double | Probability across simulated seasons that the team's outright win total falls below the line (exact pushes are the remainder). |
| `game_summary.game_type` | character | Game type of the matchup - REG for regular season or the playoff round (WC, DIV, CON, SB). |
| `game_summary.week` | integer | Week number of the matchup. |
| `game_summary.away_team` | character | Team abbreviation of the away team in the matchup. |
| `game_summary.home_team` | character | Team abbreviation of the home team in the matchup. |
| `game_summary.away_wins` | integer | Number of simulated seasons in which the away team won the matchup. |
| `game_summary.home_wins` | integer | Number of simulated seasons in which the home team won the matchup. |
| `game_summary.ties` | integer | Number of simulated seasons in which the matchup ended in a tie. |
| `game_summary.result` | double | Mean home margin of the matchup across the simulated seasons in which it was played. |
| `game_summary.games_played` | integer | Number of simulated seasons in which this exact matchup occurred (playoff pairings only arise in the simulations that produce them). |
| `game_summary.away_percentage` | double | Share of played simulations won by the away team, with ties counted as half a win. |
| `game_summary.home_percentage` | double | Share of played simulations won by the home team, with ties counted as half a win. |

**Example**

```python
import sportsdataverse.nfl as nfl
games = nfl.load_schedules([2024])
sim = nfl.nfl_simulations(games, simulations=1000, seed=42)
print(sim["overall"].head())

# Custom initial ELO ratings

sim = nfl.nfl_simulations(games, simulations=500, seed=1,
                          elo={"KC": 1700, "BUF": 1650})

# Pipeline next step (one line)

sim["overall"].sort("won_sb", descending=True).head()
```

### nfl_usage_projection {#nfl_usage_projection}

`nfl_usage_projection(seasons: 'List[int]', target_season: 'int', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Project next-season target share, air-yards share, and WOPR.

Projects each player's shares via the shared Marcel blend
(`sportsdataverse.nfl.nfl_projection._marcel_blend` — the same
recency/shrinkage engine as the rate projection), assigns each player to
their most recent team, **renormalizes shares within each projected team to
sum to 1.0** (the share invariant), and converts shares to volumes with a
team-level carry-forward of pass attempts (team targets) and air yards.
As-of-date clean: only seasons strictly before `target_season` are used.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | History seasons to load. |
| `target_season` | `int` |  | The season being projected. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. |

**Returns**

`player_id:Utf8, target_season:Int64, position_group:Utf8, proj_team:Utf8, proj_target_share:Float64, proj_air_yards_share:Float64, proj_wopr:Float64, proj_targets:Float64, proj_air_yards:Float64`. Empty history returns a zero-row frame.

| col_name | type | description |
|---|---|---|
| `player_id` | character | nflverse gsis player id (character join key). |
| `target_season` | integer | The season being projected (features use strictly earlier seasons only). |
| `position_group` | character | nflverse offensive position group. |
| `proj_team` | character | Most recent team (max season, tiebreak most targets) - the renormalization group. |
| `proj_target_share` | double | Projected share of team targets - Marcel share blend renormalized to sum to 1.0 within proj_team. |
| `proj_air_yards_share` | double | Projected share of team air yards, renormalized within proj_team. |
| `proj_wopr` | double | Projected weighted opportunity rating - 1.5 x proj_target_share + 0.7 x proj_air_yards_share. |
| `proj_targets` | double | Projected targets - proj_target_share x team pass-target carry-forward. |
| `proj_air_yards` | double | Projected receiving air yards - proj_air_yards_share x team air-yards carry-forward. |

**Example**

```python
from sportsdataverse.nfl.nfl_usage_projection import nfl_usage_projection
usage = nfl_usage_projection([2021, 2022, 2023], 2024)
usage.sort("proj_wopr", descending=True).head()
```

### opponent_adjusted_ridge {#opponent_adjusted_ridge}

`opponent_adjusted_ridge(plays: 'pl.DataFrame', *, off_col: 'str', def_col: 'str', home_col: 'str', resp_col: 'str', lam: 'float', penalize_home: 'bool' = False, hfa_col: 'str | None' = None) -> 'tuple[pl.DataFrame, float, float]'`

Ridge-regress `resp_col` on offense + defense team indicators + HFA.

League-agnostic (column names are arguments): builds the full
offense/defense-indicator + intercept + home design and solves the
ridge normal equations `beta = (X'X + lam*R)^-1 X'y`. Only team
coefficients are penalised; the intercept (and, unless
`penalize_home`, the home term) is free. Moved (T7.2) from
`sportsdataverse.nfl.nfl_ratings`. Callers: NFL ratings, and CFB
adjusted EPA (`cfb_adjusted_epa._fit_team_strengths`, via `hfa_col`);
`cfb_ratings` uses the different `dropped_level_ridge`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | One row per play. Rows with a null `off_col` / `def_col` / `resp_col` must be filtered by the caller. |
| `off_col` | `str` |  | Column naming the offense (possession) team. |
| `def_col` | `str` |  | Column naming the defense team. |
| `home_col` | `str` |  | Column naming the home team (HFA indicator is `off_col == home_col`). |
| `resp_col` | `str` |  | Numeric response column (e.g. `epa`). |
| `lam` | `float` |  | Ridge penalty applied to the team coefficients. |
| `penalize_home` | `bool` | `False` | Also penalise the home-field coefficient (default False). |
| `hfa_col` | `str \| None` | `None` | Numeric column used as the home regressor as-is (e.g. CFB's `+1` home offense / `0` neutral site / `-1` away), in place of the `off_col == home_col` indicator, which then goes unused. Must not contain nulls (raises `ValueError`). |

**Returns**

A `(frame, intercept, home_coef)` tuple: `frame` has one row per team (`team_id` Utf8, `off_coef` / `def_coef` Float64); `intercept` is the league baseline; `home_coef` the fitted HFA in response units. Zero-row frame + `(0.0, 0.0)` on empty input.

**Example**

```python
from sportsdataverse.nfl.nfl_ratings import opponent_adjusted_ridge
frame, intercept, hfa = opponent_adjusted_ridge(
    plays, off_col="posteam", def_col="defteam",
    home_col="home_team", resp_col="epa", lam=200.0,
)
frame.sort("off_coef", descending=True).head()
```

### pressure_pairs {#pressure_pairs}

`pressure_pairs(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Per (season, off_team, def_team) dropbacks + pressures (matchup grid).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | nflverse play-by-play with `season`, `posteam`, `defteam` and the dropback / pressure flags. |

**Returns**

One row per (`season`, `off_team`, `def_team`) matchup, with `dropbacks` and `pressures` (Int64), sorted by season and teams. A zero-row frame with that schema when the input has no dropbacks.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the matchup aggregate. |
| `off_team` | character | Offense team abbreviation. |
| `def_team` | character | Defense team abbreviation. |
| `dropbacks` | integer | Offense dropbacks in the matchup. |
| `pressures` | integer | Sacks plus QB hits in the matchup. |

### special_teams_ratings {#special_teams_ratings}

`special_teams_ratings(plays: 'pl.DataFrame', *, config: 'RatingsConfig | None' = None) -> 'pl.DataFrame'`

One row per team: opponent-adjusted special-teams EPA per play.

Reuses `opponent_adjusted_ridge` (no forked solver) restricted to
`special == 1` plays with `resp_col="epa"`; `adj_st_epa` is the
`off_coef` (the special-teams unit acting as "offense" on the play).
Teams appearing anywhere in `plays` but on no special-teams play get
the documented neutral fill `adj_st_epa = 0.0`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | An `load_nfl_pbp`-schema frame carrying `posteam`, `defteam`, `home_team`, `epa`, `special`. Not pre-filtered -- this function selects the ST plays itself. |
| `config` | `RatingsConfig \| None` | `None` | Tuning knobs (only `ridge_lambda` is consulted); defaults to `RatingsConfig`. |

**Returns**

One row per `team_id` (Utf8) with `adj_st_epa` (Float64). Zero-row, correctly-typed when `plays` is empty.

| col_name | type | description |
|---|---|---|
| `team_id` | character |  |
| `adj_st_epa` | double |  |

**Example**

```python
from sportsdataverse.nfl.nfl_ratings import special_teams_ratings
st = special_teams_ratings(pbp)
st.sort("adj_st_epa", descending=True).head()
```

### team_pressure_rates {#team_pressure_rates}

`team_pressure_rates(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Per (season, team) raw pressure rates, both sides of the ball.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | nflverse-format pbp with `season` / `posteam` / `defteam` / `qb_dropback` / `sack` / `qb_hit`. |

**Returns**

Per `(season, team)`: `dropbacks_off`, `pressures_allowed`, `pressure_rate_allowed`, `dropbacks_def`, `pressures_generated`, `pressure_rate_generated`. Empty input yields a zero-row frame.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the aggregate. |
| `team` | character | Team abbreviation. |
| `dropbacks_off` | integer | Offensive dropbacks (qb_dropback plays). |
| `pressures_allowed` | integer | Sacks plus QB hits allowed on the team's own dropbacks. |
| `pressure_rate_allowed` | double | pressures_allowed / dropbacks_off. |
| `dropbacks_def` | integer | Opponent dropbacks faced on defense. |
| `pressures_generated` | integer | Sacks plus QB hits generated against opponent dropbacks. |
| `pressure_rate_generated` | double | pressures_generated / dropbacks_def. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_line_grades import team_pressure_rates
rates = team_pressure_rates(load_nfl_pbp([2023]))
print(rates.sort("pressure_rate_generated", descending=True).head())
```
