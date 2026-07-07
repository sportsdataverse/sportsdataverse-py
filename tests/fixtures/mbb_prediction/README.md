<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [MBB prediction-stack oracle corpus (season 2024)](#mbb-prediction-stack-oracle-corpus-season-2024)
  - [Oracle notes](#oracle-notes)
  - [Not yet captured (added when their phase needs them)](#not-yet-captured-added-when-their-phase-needs-them)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# MBB prediction-stack oracle corpus (season 2024)

Offline validation fixtures for the MBB/WBB prediction & tournament stack
(Phase 0, Task 0.1). Captured **2026-07-07** for the **2024** season
(2023-24 men's Division I). Regenerate with
`uv run python dev/mbb_prediction/capture_oracle.py` (script is gitignored
under `dev/`).

**ID convention:** every team / game id is `Utf8` (cast from the raw ESPN
integer via `pl.col(id).cast(pl.Int64).cast(pl.Utf8)`), so joins across
fixtures and against the ratings engine never hit a dtype mismatch.

| File | Rows | Source | Notes |
|---|---:|---|---|
| `results_2024.parquet` | 6243 | `load_mbb_schedule([2024])` (ESPN, via sportsdataverse-data) | Completed games only (`status_type_completed`), deduped on `game_id`. Cols: `game_id, season, date, home_team_id, away_team_id, home_score, away_score, neutral_site`. |
| `team_box_2024.parquet` | 12480 | `load_mbb_team_boxscore([2024])` (ESPN) | Per-team possession inputs for the ratings engine. Cols: `game_id, season, game_date, team_id, opp_team_id, team_home_away, team_score, opp_score, field_goals_attempted, offensive_rebounds, turnovers, free_throws_attempted`. |
| `torvik_2024.parquet` | 350 | barttorvik `https://barttorvik.com/2024_team_results.csv` | `adj_o`=adjoe, `adj_d`=adjde, `adj_em`=adjoe−adjde, `rank`. Keyed to ESPN `team_id` via a contracting normalizer + `St.`→`State`/`Saint` candidate keys + a small alias table (`dev/mbb_prediction/capture_oracle.py`). |

## Oracle notes

- **Torvik is the Phase-1 ratings gate** (Spearman(`adj_em`, Torvik `adj_em`)
  ≥ 0.95 over the matched set). barttorvik ratings are keyed on team **name**;
  **350 of 362** teams (96.7%) matched to an ESPN `team_id`. The 12 unmatched
  are one-off small-school naming irregularities (American, FIU, Penn, Queens,
  Seattle, LIU, St. Thomas, Saint Francis, Southeastern Louisiana, IU Indy,
  East Texas A&M, Texas A&M-Corpus Christi) — dropped from the inner-join
  comparison, which is statistically immaterial to a 350-team rank correlation.
- **`adj_tempo` is null.** barttorvik's `trank.php?csv=1` (which carries the
  adjusted-tempo column) is bot-blocked (returns HTML); the static
  `2024_team_results.csv` has no tempo. This is fine: no gate depends on the
  Torvik tempo column — adjusted tempo (Task 1.3) is validated against a
  synthetic construction, not this oracle.

## Not yet captured (added when their phase needs them)

The ESPN per-game oracle samples used by later phases are **not** in this
commit — they are a rate-limited per-game scrape (ESPN Core v2 403s under
load) and only Phase 2/4 gates consume them:

- `espn_predictor_sample.parquet` — `espn_mbb_game_predictor` (Phase 2 win-prob).
- `espn_odds_sample.parquet` — `espn_mbb_game_odds` (Phase 2 spread/total MAE).
- `espn_bpi_2024.parquet` — `espn_mbb_season_powerindex` (Phase 4 SoS gate).
