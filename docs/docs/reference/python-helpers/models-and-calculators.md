---
title: "Package — additional Python functions — Models and calculators"
sidebar_label: "Models and calculators"
sidebar_position: 9
description: "Package — additional Python functions — Models and calculators — function reference in sdv-py, the SportsDataverse Python package."
---
# Package — additional Python functions — Models and calculators

### cricket_expected_runs {#cricket_expected_runs}

`cricket_expected_runs(state_wp: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Expected remaining runs + run rate from a win-probability-scored state frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `state_wp` | `DataFrame` |  | Output of `~sportsdataverse.cricket.cricket_win_prob.cricket_win_probability` (must carry `proj_final`, `runs`, `overs_left`). |
| `return_as_pandas` | `bool` | `False` | When True, return a `pandas.DataFrame`. |

**Returns**

The input rows plus `exp_runs_remaining:Float64` (`proj_final - runs`, floored at 0) and `exp_run_rate:Float64` (per remaining over; null when no overs remain). A zero-row input returns the schema with both columns appended (all null).

**Example**

```python
import polars as pl
from sportsdataverse.cricket.cricket_win_prob import cricket_win_probability
from sportsdataverse.cricket.cricket_wpa import cricket_expected_runs
scored = cricket_win_probability(state)
er = cricket_expected_runs(scored)
er.select("exp_runs_remaining", "exp_run_rate").head()
```

### cricket_match_state {#cricket_match_state}

`cricket_match_state(summary: 'dict', *, fmt: 'str', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Extract over-level match state from an ESPN cricket summary/scoreboard payload.

One row per innings with a parseable competitor score string. The batting
side that carries a `target` in its score is the second innings (chasing);
the other is the first innings (setting).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `summary` | `dict` |  | Raw ESPN cricket `summary` or `scoreboard` payload (dict). |
| `fmt` | `str` |  | Format slug (`"t20"` / `"odi"`); validated via `~sportsdataverse.cricket.cricket_model_constants.get_format`. |
| `return_as_pandas` | `bool` | `False` | When True, return a `pandas.DataFrame`. |

**Returns**

A `polars.DataFrame` (or pandas) with the documented state schema; a zero-row frame when the payload is empty/malformed.

**Example**

```python
from sportsdataverse.cricket import espn_cricket_summary
from sportsdataverse.cricket.cricket_win_prob import cricket_match_state
state = cricket_match_state(espn_cricket_summary(event="1385691", return_parsed=False), fmt="t20")
print(state.shape)
```

### cricket_win_probability {#cricket_win_probability}

`cricket_win_probability(state: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

In-play win probability for the batting/chasing team from match state.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `state` | `DataFrame` |  | Over-level match state carrying the documented state schema (`event_id, innings_number, batting_team_id, runs, wickets, balls_bowled, balls_total, target, fmt`) — e.g. the output of `cricket_match_state`. |
| `return_as_pandas` | `bool` | `False` | When True, return a `pandas.DataFrame`. |

**Returns**

The input rows plus `overs_left:Int64`, `wickets_left:Int64`, `resources_left:Float64`, `proj_final:Float64`, `win_prob_raw:Float64` (parametric core) and `win_prob:Float64` (calibrated, the shipped estimate). A zero-row input returns the schema with these columns appended (all null).

**Example**

```python
import polars as pl
from sportsdataverse.cricket.cricket_win_prob import cricket_win_probability
st = pl.DataFrame([{ "event_id": "M1", "innings_number": 2,
    "batting_team_id": "A", "runs": 120, "wickets": 3,
    "balls_bowled": 90, "balls_total": 120, "target": 160, "fmt": "t20"}])
cricket_win_probability(st).select("win_prob").item()
```

### cricket_wpa {#cricket_wpa}

`cricket_wpa(state_wp: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Batting/bowling win-probability added per over/wicket transition.

`wpa_batting` is the change in the batting team's win probability since the
previous state within the same innings; `wpa_bowling` is its negation (the
bowling side gains exactly what the batting side loses). The lead is taken
`.over(["event_id", "innings_number"])` so no change leaks across matches or
innings, and the first state of each innings has `wpa_batting = 0`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `state_wp` | `DataFrame` |  | Output of `~sportsdataverse.cricket.cricket_win_prob.cricket_win_probability` (must carry `event_id`, `innings_number`, `balls_bowled`, `win_prob`). |
| `return_as_pandas` | `bool` | `False` | When True, return a `pandas.DataFrame`. |

**Returns**

The input rows (sorted by `event_id, innings_number, balls_bowled`) plus `win_prob_before:Float64`, `wpa_batting:Float64` and `wpa_bowling:Float64`. A zero-row input returns the schema with those columns appended (all null).

**Example**

```python
from sportsdataverse.cricket.cricket_win_prob import cricket_win_probability
from sportsdataverse.cricket.cricket_wpa import cricket_wpa
wpa = cricket_wpa(cricket_win_probability(state))
wpa.select("wpa_batting", "wpa_bowling").head()
```

### deserved_wins {#deserved_wins}

`deserved_wins(games: 'pl.DataFrame') -> 'pl.DataFrame'`

Season deserved wins and luck per team from `paper_index_games` rows.

With `p_i` the team's `paper_share` in game `i` of the season and `W` its
real wins: `deserved_wins = sum(p_i)`, `luck_wins = W - deserved_wins` and
`luck_z = luck_wins / sqrt(sum(p_i * (1 - p_i)))`. Under the model a season's
wins are a sum of independent Bernoulli(`p_i`) draws, so `luck_z` is luck in
standard deviations; it is null when that variance is 0 (every share exactly 0
or 1).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | `paper_index_games` output (needs `season`, `team_id`, `won`, `paper_share`), any number of seasons of ONE league: ESPN team ids overlap across leagues (2 is Auburn in college and Buffalo in the NFL). |

**Returns**

one row per `(season, team_id)`, sorted by them: `games`, `wins`, `deserved_wins`, `luck_wins`, `luck_z` (`DESERVED_WINS_SCHEMA`; `team_id` keeps its dtype). An empty frame with the same columns when `games` is empty. Read with care: * `games` / `wins` count only the games `paper_index_games` scored, so they can differ from the official record: ties and snap-floor games are out (NFL ties: 1 in 2021, 2 in 2022), postseason games are in. * Luck includes home field: the share has no intercept, and home teams beat their shares in every 2022-2025 season (+0.013 to +0.047 wins per home game; fit holdouts: college 1,204 home wins vs 1,171 deserved, NFL 628 vs 596). A team's `luck_wins` is biased by about that rate x (home - away games), and the nominal home side at a neutral site gets it too. * Small samples: college FCS opponents appear with 1-2 games; filter on a minimum `games` before ranking. Over 2022-2025 the spread of `luck_z` for teams with 8+ games was 1.03-1.11 (college) and 0.85-1.28 (NFL).

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the games rolled up, as the pbp keys it (CFB and NFL starting year). |
| `team_id` | integer | Team id as paper_index_games carries it (the released pbp's pos_team_id dtype, ESPN team id). |
| `games` | integer | Games the team played that season that paper_index_games scored (completed, not tied, both sides at least 20 scrimmage snaps; NFL Pro Bowl excluded). |
| `wins` | integer | Games the team won among those scored games. |
| `deserved_wins` | double | Sum of the team's paper_share over the season: the wins the Paper Index says its play earned. |
| `luck_wins` | double | wins minus deserved_wins; positive means the team won more games than its play deserved. |
| `luck_z` | double | luck_wins / sqrt(sum(p * (1 - p))) over the team's game shares p: luck in standard deviations of a sum of independent Bernoulli(p) wins; null when that variance is 0. |

**Example**

```python
from sportsdataverse.cfb import load_cfb_pbp
from sportsdataverse.paper_index import PBP_COLUMNS, deserved_wins, paper_index_games

luck = deserved_wins(paper_index_games(load_cfb_pbp(2024).select(PBP_COLUMNS), "cfb"))
luck.sort("luck_z", descending=True).head(10)  # the season's luckiest teams
```

### paper_index_game {#paper_index_game}

`paper_index_game(pbp: 'pl.DataFrame', home_id: 'Union[int, str]', away_id: 'Union[int, str]', league: 'str') -> 'Optional[dict[str, Any]]'`

Paper Index of one game: each side's deserved-win share and the eight margins.

The served path, as Game on Paper's `paper_index.compute`: no snap floor and
no tie rule, just the model applied to the plays given.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | one game's released `espn_{cfb,nfl}_pbp` plays: `game_play_number` and the `PBP_COLUMNS` from `pos_team_id` on (`fg_made` only for the NFL; `game_id` optional). |
| `home_id` | `Union[int, str]` |  | the home team's id as `pos_team_id` carries it (int or str). |
| `away_id` | `Union[int, str]` |  | the away team's id. |
| `league` | `str` |  | `"cfb"` or `"nfl"` -- picks the fitted weights, the field-position curve and the field-goal rule. |

**Returns**

dict | None: `{"home_share": float, "away_share": float, "margins": {name: float}}` with the eight `MARGINS` from the home side (home minus away; havoc and turnovers signed so positive favors home). `away_share` is `1 - home_share`. None when either side has no usable inputs (no scrimmage snap, no drive id, or no successful play).

**Example**

```python
import polars as pl
from sportsdataverse.cfb import load_cfb_pbp
from sportsdataverse.paper_index import paper_index_game

pbp = load_cfb_pbp(2024).filter(pl.col("game_id") == 401628337)
out = paper_index_game(pbp, 2, 25, "cfb")  # Auburn (home) vs California
out["home_share"], out["margins"]["success"]
```

### paper_index_games {#paper_index_games}

`paper_index_games(pbp: 'pl.DataFrame', league: 'str') -> 'pl.DataFrame'`

Paper Index of every completed game in a pbp frame, one row per team per game.

A game counts when it passes the trainer's filters (fit_paper_index.py
`season_game_rows`): it has a winner (ties dropped), both sides ran at least
`MIN_PLAYS_PER_TEAM` scrimmage snaps, and (NFL) both teams are franchises
(the Pro Bowl dropped). The port adds two checks of its own: the game's last
play is marked completed (`status_type_completed`), and both sides' eight
inputs are computable and finite (where the trainer required a finite mean EPA).
Final scores and the home/away ids are the last play's (by `game_play_number`).
On the NFL seasons the fit read, this selects exactly the fit's games.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | released `espn_{cfb,nfl}_pbp` plays, any number of games and seasons; project to `PBP_COLUMNS` first (`load_cfb_pbp(s).select(PBP_COLUMNS)`). |
| `league` | `str` |  | `"cfb"` or `"nfl"`. |

**Returns**

keyed by `(game_id, team_id)`, sorted by them; `game_id` and `team_id` keep the pbp's `game_id` / `pos_team_id` dtype (never through float), the rest is `GAMES_SCHEMA`: * `season`, `season_type` (ESPN `seasonType`: 2 regular, 3 post), `week`. * `won`: the team outscored its opponent. * `paper_share`: the team's deserved-win probability; `opp_share` the opponent's (they sum to 1). * eight `{margin}_margin` columns, team minus opponent, positive favors the team. An empty frame with the same columns when no game qualifies.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | ESPN game id, typed as the released pbp's game_id (Int64 in espn_cfb_pbp and espn_nfl_pbp). |
| `team_id` | integer | ESPN id of the team the row describes, typed as the released pbp's pos_team_id (Int64 in espn_cfb_pbp and espn_nfl_pbp). |
| `season` | integer | Season of the game, as the pbp keys it (CFB and NFL starting year). |
| `season_type` | integer | ESPN season type of the game (seasonType): 2 regular season, 3 postseason. |
| `week` | integer | ESPN week number of the game within its season type. |
| `won` | logical | Whether the team outscored its opponent (final score from the game's last play). |
| `paper_share` | double | The team's Paper Index share in [0, 1]: its deserved-win probability from the eight margins through the league's fitted intercept-free logistic. |
| `opp_share` | double | The opponent's Paper Index share; paper_share + opp_share = 1. |
| `success_margin` | double | Team EPA success rate minus the opponent's, over scrimmage snaps. |
| `explosive_margin` | double | Team explosive-play rate (EPA_explosive) minus the opponent's, over scrimmage snaps. |
| `explosive_epa_margin` | double | Team explosiveness (mean EPA per successful snap) minus the opponent's. |
| `opp_conversion_margin` | double | Team scoring-opportunity conversion (share of opportunity drives that scored; 0.5 with no opportunities) minus the opponent's. |
| `pts_per_opp_margin` | double | Team points per scoring opportunity minus the opponent's (NFL counts made field goals; no opportunities takes the league's train-season average). |
| `field_position_margin` | double | Expected points of the team's average drive start on the league's bundled EP-by-yardline curve minus the opponent's. |
| `havoc_margin` | double | Havoc rate the team's defense created (havoc allowed on opponent snaps) minus the havoc rate its offense allowed. |
| `turnovers_margin` | double | Turnovers the opponent's offense committed minus the team's. |

**Example**

```python
import polars as pl
from sportsdataverse.paper_index import PBP_COLUMNS, paper_index_games

url = (
    "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
    "espn_nfl_pbp/play_by_play_2024.parquet"
)
games = paper_index_games(pl.read_parquet(url, columns=list(PBP_COLUMNS)), "nfl")

# Pipeline next step (one line)

games.filter(~pl.col("won") & (pl.col("paper_share") > 0.8))  # deserved to win, lost
```

### wch_ratings {#wch_ratings}

`wch_ratings(dates: 'list[str]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WCH opponent-adjusted goal-margin ratings over a set of scoreboard dates.

See the module docstring's coverage caveat -- ESPN's WCH scoreboard
coverage observed during this port was tournament-only.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `list[str]` |  | `YYYYMMDD` date strings to fetch. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

One row per team: `team_id, adj_off, adj_def, adj_net, raw_off, raw_def, games`.

**Example**

```python
from sportsdataverse.hockey.wch import wch_ratings
ratings = wch_ratings(["20250315", "20250321", "20250322", "20250323"])
ratings.sort("adj_net", descending=True).head()
```
