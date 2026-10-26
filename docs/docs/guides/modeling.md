---
title: Models, validation and analytics
sidebar_label: Modeling guide
sidebar_position: 5
---

# Models, validation and analytics

Beyond fetching and tidying data, sdv-py ships the models that turn football
play-by-play into expected points and win probability, a validator that checks a
processed game against invariant rules, a win-expectancy benchmarking harness, and
a set of season analytics built on the released play-by-play. Every bundled model and
fitted constant names the script that produced it on the
[Model lineage](../architecture/model-lineage.md) page.

## NFL expected points and win probability

`sportsdataverse/nfl/ep_wp.py` is the one place NFL models are applied and EPA / WPA
are derived. Construction code (the ESPN processor, the Shield rebuild,
`load_nfl_pbp`) builds a frame; `ep_wp` scores it.

`enrich_nfl_pbp(df)` scores an nflverse-shape frame in nflfastR's order: EP, EPA,
WP, WPA, CP and CPOE, then xYAC. The default and only implemented
`method="lead_diff"` ports nflfastR's `helper_add_ep_wp.R`: it scores one
start-of-play `ep`, derives `epa` / `wpa` with lead differences grouped by
`game_id` (so concatenated games never leak into each other), applies nflfastR's
scoring, end-of-half and overtime overlays, and substitutes kickoff features (the
season's touchback yard line, first and ten) before scoring. Against nflverse's own
columns the correlations are about 0.996 for `ep`, 0.994 for `epa`, 0.997 for `wp`
and 0.998 for `vegas_wp`; `wpa` sits near 0.89, which is the WP model's per-play
noise amplified by differencing, not a derivation error.

```python
import polars as pl
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import enrich_nfl_pbp

pbp = load_nfl_pbp([2024])
scored = enrich_nfl_pbp(pbp)                     # about a minute for a season
print(scored.select("game_id", "play_id", "ep", "epa", "wp", "wpa", "cpoe").drop_nulls("epa").head(3))
```

The bundled NFL boosters (`sportsdataverse/nfl/models/*.ubj`) are the EP, naive and
spread WP, and CP models from the `nfl_model_artifacts` release, plus field-goal,
QBR, two-point, expected-pass and punt models; the fourth-down decision surface lives
in `sportsdataverse/nfl/nfl_fourth_down.py`. A missing model warns once and nulls
its columns; `enrich_nfl_pbp(df, strict=True)` or `SDV_PY_MODELS_STRICT=1` raises
instead, for producers that must not publish nulls.

The ESPN path (`NFLPlayProcess`, `build_nfl_season(game_ids=...)`) shares the same
EPA / WPA derivation (`calculate_epa` / `calculate_wpa`), so both paths produce the
same numbers from the same inputs.

## CFB bundled models

`sportsdataverse/cfb/models/` ships rule-era XGBoost models trained in
`cfbfastR-cfb-data`: expected points, naive and spread win probability, completion
probability (with and without air yards), field goal, fourth down, two-point,
expected pass, QBR and an overtime reach model. `CFBPlayProcess.run_processing_pipeline()`
applies them, along with the fourth-down, field-goal and two-point decision surfaces,
which are on by default. Each CFB booster ships a `.card.json` beside it with its
training frame and seasons.

```python
from sportsdataverse.cfb import CFBPlayProcess

game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
game.run_processing_pipeline()
print(game.plays_frame.select("game_play_number", "EPA", "wp_before", "home_wp_before").tail(3))
```

## Validate a processed game

`sportsdataverse.validation.validate_game` checks one processed NFL or CFB game
against the packaged invariant rules, grouped by what they check (down, yards to go,
possession, score, period, timeouts, play type, flags, win probability, the plays
themselves and the box score), each scoped to the rule eras it applies to.
It is pure and offline: nothing is fetched or written, and a rule whose columns the
frame lacks is skipped rather than failed. It returns a `GameReport`.

```python
from sportsdataverse.validation import validate_game

report = validate_game(game.plays_frame, "cfb")
print(report.ok, len(report.errors), len(report.warnings))
print(report.counts_by_rule)
```

`report.ok` is `True` when no rule fired at `error` severity. `errors` and `warnings`
list the findings; `counts_by_rule` counts violations per rule whatever their
severity; `not_applicable` names the rules the source cannot support. For a source
that names columns differently (`source=` other than `"espn"`), `SOURCE_COLUMNS`
supplies the ESPN-shaped aliases.

## Win expectancy: wexp

`sportsdataverse.wexp` is a harness for building and benchmarking pregame win
probability models against the betting market. It has:

- **Market oracles**: `nfl_market_oracle_from_schedule` and
  `build_nfl_market_oracle` / `build_cfb_market_oracle` turn closing lines into a
  per-game frame with the market's home win probability.
- **Market helpers**: `prob_from_american`, `prob_from_decimal`,
  `devig_multiplicative`, `devig_shin`, `spread_to_prob`, `logit_blend`.
- **Engines**: margin Elo (`elo_ratings`, `elo_predictor`), Glickman-Stern
  (`glickman_stern_predictor`), rating predictors and vintage-based feature stores.
- **A walk-forward backtest**, `run_backtest(oracle, predictor, model_id=...)`: each
  week the engine sees only games completed earlier in the walk, and the predictions
  are scored per season and pooled.
- **Scoring**: `brier_score`, `log_loss_score`, `winner_accuracy`, `ece`,
  `calibration_table`, `closing_line_value`, and `score_baselines` for the market and
  naive baselines.

```python
from sportsdataverse.nfl import load_nfl_schedule
from sportsdataverse.wexp import elo_predictor, nfl_market_oracle_from_schedule, run_backtest

oracle = nfl_market_oracle_from_schedule(load_nfl_schedule([2022, 2023]))
probs, rows = run_backtest(oracle, elo_predictor(), model_id="elo")
print(rows.filter(pl.col("season") == -1).select("metric", "value", "n"))   # pooled: brier, log_loss, ...
```

```python
from sportsdataverse.wexp import devig_multiplicative, prob_from_american, spread_to_prob

raw = [prob_from_american(-150), prob_from_american(130)]
print(devig_multiplicative(raw))          # the two sides' no-vig probabilities
print(spread_to_prob(7.0, sigma=13.45))   # a 7-point home favourite
```

## Season analytics on the released play-by-play

Four modules turn released play-by-play into season-level tables. Each declares the
columns it reads as a constant, so you can load only those.

| Module | Function | What it returns |
|---|---|---|
| `paper_index` | `paper_index_games`, `deserved_wins`, `paper_index_game` | Each team's "deserved win" share of every game from eight per-game margins, and season deserved wins and luck |
| `rolling_windows` | `football_events`, `shot_events`, `rolling_windows` | Rolling-window form per player or team: current window, previous window and season start |
| `metric_curves` | `football_attempts`, `nflfastr_attempts`, `shot_attempts`, `metric_curves` | Success-rate curves along a continuous axis (field goals by distance, completions by air yards, fourth downs by yards to go) for the league, teams and players |
| `defense_vs_position` | `defense_vs_position` | EPA per play, success and explosive rates each defense allowed to QBs, RBs, WRs and TEs |

The football functions take CFB or NFL; `shot_events` / `shot_attempts` take NBA or
WNBA stats shots.

```python
from sportsdataverse.cfb import load_cfb_pbp, load_cfb_rosters
from sportsdataverse.analytics.paper_index import PBP_COLUMNS, deserved_wins, paper_index_games

cfb_pbp = load_cfb_pbp(2024)
games = paper_index_games(cfb_pbp.select(PBP_COLUMNS), "cfb")
luck = deserved_wins(games).sort("luck_wins")     # deserved_wins, luck_wins, luck_z per team
print(luck.head(3))
```

```python
from sportsdataverse.analytics.metric_curves import FOOTBALL_ATTEMPT_COLUMNS, football_attempts, metric_curves

curves = metric_curves(football_attempts(cfb_pbp.select(FOOTBALL_ATTEMPT_COLUMNS)), "cfb")
print(
    curves.filter((pl.col("entity_type") == "league") & (pl.col("metric") == "fg_pct_by_distance")).select(
        "x_lo", "x_hi", "attempts", "rate"
    )
)
```

```python
from sportsdataverse.analytics.defense_vs_position import PBP_COLUMNS as DVP_COLUMNS
from sportsdataverse.analytics.defense_vs_position import defense_vs_position

dvp = defense_vs_position(cfb_pbp.select(DVP_COLUMNS["cfb"]), load_cfb_rosters(2024), "cfb")
print(
    dvp.filter((pl.col("position_group") == "TE") & (pl.col("qualified") == True))
    .sort("epa_per_play_allowed")
    .select("team_id", "plays", "epa_per_play_allowed")
    .head(3)
)
```

Import from the modules themselves (`from sportsdataverse.analytics.paper_index import ...`):
the top-level `sportsdataverse` namespace binds `rolling_windows`,
`metric_curves` and `defense_vs_position` to the functions of the same name, not the
modules.

Paper Index is the model behind Game on Paper's deserved-win numbers; its fitted
weights and fitting script are listed on the
[Model lineage](../architecture/model-lineage.md) page.
