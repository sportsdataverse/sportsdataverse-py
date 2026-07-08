<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [CFB Ratings & Prediction oracle fixtures (2023)](#cfb-ratings--prediction-oracle-fixtures-2023)
  - [Polarity notes (for the Task 1.5 gate)](#polarity-notes-for-the-task-15-gate)
  - [Sanity anchors (asserted in the oracle tests — these are real values)](#sanity-anchors-asserted-in-the-oracle-tests--these-are-real-values)
  - [Re-capture](#re-capture)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# CFB Ratings & Prediction oracle fixtures (2023)

Committed validation corpus for the T2.1 CFB Ratings & Prediction spine
(`sportsdataverse/cfb/cfb_ratings.py`, `cfb_game_predict.py`, `cfb_resume.py`,
`cfb_season_odds.py`). Season **2023**, captured **2026-07-07**. All id columns
are `Utf8` (ESPN `team_id` / `game_id`). Read offline in the `tests/cfb/`
oracle + backtest tests; no network needed once committed.

| File | Rows | Source | Key columns |
|---|---:|---|---|
| `results_2023.parquet` | ~3.7k | `load_cfb_schedule([2023])`, completed games (all divisions; filter to FBS via `team_id` membership) | `game_id, season, week, date, home_team_id, away_team_id, home_score, away_score, neutral_site` |
| `pbp_2023_sample.parquet` | ~150k | `load_cfb_pbp([2023])`, FBS-vs-FBS plays, down-selected to the model-① columns | `game_id, week, pos_team, pos_team_id, def_pos_team_id, home, EPA, pass, rush, wp_before, drive_id, drive_result, play_type, neutral_site` |
| `fpi_2023.parquet` | 133 | `espn_cfb_season_powerindex_leaders(season=2023)` (ESPN FPI) | `team_id, team, fpi, fpi_rank` |
| `sp_plus_2023.parquet` | 133 | **CollegeFootballData SP+ exporter** — `https://collegefootballdata.com/exporter/ratings/sp` (Bill Connelly SP+). Team names → `team_id` via `load_cfb_team_info([2023]).school`. | `team_id, team, sp_overall, sp_off, sp_def, sp_special, sp_rank` |
| `fei_2023.parquet` | 133 | **bcftoys.com** — `https://bcftoys.com/2023-fei` (Brian Fremeau FEI), 9 chunked HTML tables concatenated. Team names → `team_id` via `load_cfb_team_info`. | `team_id, team, fei, fei_off, fei_def` |
| `espn_predictor_sample.parquet` | ~50 | `espn_cfb_game_predictor` / `_game_probabilities` over a completed-game sample | `game_id, home_team_id, away_team_id, home_win_prob, predicted_margin` |
| `espn_odds_sample.parquet` | ~84 | `espn_cfb_game_odds` over the same sample | `game_id, close_spread_home, close_total` |

**ESPN predictor/odds sample provenance & gaps:** both are a down-selection of
*completed* 2023 FBS games (weeks 1–14) — the subset ESPN published a
predictor/closing-line for. The backtest gate joins them on `game_id` and, after
restricting to games whose *both* teams have as-of ratings (weeks ≥ 5, drops
FBS-vs-FCS), retains ~30 predictor games and ~30/28 odds games. Three
`close_total` values are `null` and are `drop_nulls`'d by the total-MAE gate.
`close_spread_home` is the sportsbook **home** spread (negative = home favored);
the market-implied home margin is `-close_spread_home` (corr with ESPN
`predicted_margin` = −0.95).

## Polarity notes (for the Task 1.5 gate)

- **SP+ `sp_def` and FEI `fei_def` are captured with the source's native
  polarity.** SP+ `Defense Rating` is *lower = better* (e.g. Michigan 7.2 is an
  elite defense) — the same direction as the engine's `adj_def_epa` (EPA
  allowed), so `spearman(adj_def_epa, sp_def)` is expected **positive**; do not
  sign-flip. `sp_off` / `fei_off` are *higher = better*, matching `adj_off_epa`.
- SP+/FEI values are the published end-of-season ratings, so the ratings-oracle
  test builds full-season (`as_of_date=None`) ratings to compare.

## Sanity anchors (asserted in the oracle tests — these are real values)

- SP+ (`sp_overall`): Michigan 31.3 (#1), Georgia 31.2 (#2), Oregon 26.2 (#3).
- FEI (`fei`): Oregon 1.69 (#1).
- FPI (`fpi`): Michigan ≈28.4, Georgia ≈26.7, Ohio State ≈26.4 (top 3).

## Re-capture

Scratch scripts under `dev/cfb_prediction/` (gitignored): `capture_oracle.py`
(results/pbp/FPI/ESPN/FEI), `recapture_pbp.py` (full FBS-vs-FBS pbp),
`sp_plus_2023_cfbd.csv` (the user-provided CFBD SP+ export). Requires
`SDV_PY_LIVE_TESTS=1`. SP+/FEI team-name → `team_id` matching uses a normalized
`load_cfb_team_info` join + a 3-entry alias table (App State→2026,
Massachusetts/UMass→113, Hawai'i→62); all 133 teams resolve in both.
