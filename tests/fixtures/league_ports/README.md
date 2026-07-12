<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [league_ports fixtures — provenance](#league_ports-fixtures--provenance)
  - [Schema / dtype notes](#schema--dtype-notes)
  - [Train / holdout split (leakage boundary)](#train--holdout-split-leakage-boundary)
  - [Regenerate-together invariant](#regenerate-together-invariant)
- [Spring football (UFL / XFL) + NFL-parity fixtures](#spring-football-ufl--xfl--nfl-parity-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# league_ports fixtures — provenance

Oracle/holdout corpora for the T7.3 cricket models (`cricket_win_prob`,
`cricket_wpa`). Captured **2026-07-11**.

> **Data: [Cricsheet.org](https://cricsheet.org).** Ball-by-ball match data is
> from Cricsheet and is used under its license — please credit **Cricsheet.org**
> in any downstream use. Source corpus: **male T20 Internationals + One-Day
> Internationals, seasons 2002–2026** (`t20s_json.zip` + `odis_json.zip`),
> ~2.06M legal-ball deliveries across **5,984 matches** (3,436 T20I + 2,548 ODI).
> The raw zips and the parsed per-ball corpus are NOT committed (gitignored under
> `dev/cricket/.cache/`); only the two small holdout fixtures here and the two
> fitted model artifacts (`sportsdataverse/cricket/models/*.parquet`) are.

Ingest: `dev/cricket/fetch_cricsheet.py` (pure-Python zip parser, no R dep).
Fit + fixture build: `dev/league_ports/fit_cricket_resource_surface.py`.

| file | rows | matches | source | notes |
|---|---|---|---|---|
| `cricket_holdout.parquet` | 50,958 states | 861 | Cricsheet held-out matches, per-over states (`balls_bowled % 6 == 0`) | Phase-2 win-prob calibration gate. Columns = the win-prob state schema + `chasing_won` (label). |
| `cricket_wpa_holdout.parquet` | 11,604 states | 200 | Cricsheet held-out matches, per-over PLUS terminal states (last legal ball per innings) | Phase-3 WPA reconciliation + E[runs] gate. Adds `innings_final_runs`; terminal states let each win-prob trajectory reach the pinned outcome. |

## Schema / dtype notes

- `event_id` is **Utf8** (the Cricsheet match-id filename stem); `batting_team_id`
  is **Utf8** (team display name). `runs`/`wickets`/`balls_bowled`/`balls_total`/
  `target`/`innings_number`/`innings_final_runs` are **Int64** (`target` is null
  in the first innings).
- **`chasing_won`** is the per-row label = "**did this row's batting team win the
  match**" (1/0). It is correct for both innings (first-innings rows carry the
  first-innings team's outcome, second-innings rows the chaser's); the name reads
  "chasing" only because the second innings is the common case. `cricket_win_probability`
  returns `P(this row's batting team wins)`, so `chasing_won` is its aligned target.

## Train / holdout split (leakage boundary)

Both fixtures are the **complement of the training matches**: a single
**15% match-level random holdout** (`HOLDOUT_SEED = 7`) — no match appears in
both train and holdout. A random match split (not a season holdout) is the
correct instrument for a *calibration* gate; a season holdout would conflate
calibration error with genuine temporal scoring drift (T20/ODI totals have risen
2002→2026). Observed out-of-sample: Brier ≈ 0.190 (no-skill 0.25), max
per-decile calibration ≈ 0.031.

## Regenerate-together invariant

These two fixtures **and** the two committed model artifacts
(`cricket_resource_surface.parquet`, `cricket_winprob_calibration.parquet`) are
all written by ONE run of `dev/league_ports/fit_cricket_resource_surface.py` from
the SAME split. **Never regenerate one without the others** — a stale mix would
let the holdout silently overlap the training matches (leakage). If the Cricsheet
corpus is refreshed, re-run the fit script once and commit all four artifacts
plus the refreshed `FORMAT_TABLE` constants together.

# Spring football (UFL / XFL) + NFL-parity fixtures

Oracle/contract fixtures for the T7.3 spring-football EP/WP port
(`sportsdataverse/football/spring_football_ep_wp.py`). Captured
**2026-07-12** via
`https://site.api.espn.com/apis/site/v2/sports/football/<league>/summary?event=<id>`
— full per-fixture provenance, live-verification dates, and the two
capture findings the tests pin (UFL has NO ESPN play-by-play; the
`espn_{ufl,xfl}_game_probabilities` oracle returns HTTP 400 for both
leagues) live in [`FEASIBILITY.md`](FEASIBILITY.md).

| file | league | event | date | plays |
|---|---|---|---|---|
| `xfl_summary.json` | xfl | 401517780 | 2023-04-01 | 159 |
| `xfl_summary_2.json` | xfl | 401517747 | 2023-03-12 | 140 |
| `xfl_summary_3.json` | xfl | 401517746 | 2023-03-12 | 152 |
| `ufl_summary.json` | ufl | 401638335 | 2024-06-01 | 0 (real no-pbp capture) |
| `nfl_parity_2023_game.parquet` | nfl | `2023_01_ARI_WAS` | 2023-09-10 | 168 (gate-(a) input frame) |

All five are regenerated together by ONE run of
`dev/league_ports/capture_spring_football_fixtures.py` (`SDV_PY_LIVE_TESTS=1`;
the NFL slice additionally needs the local nfl-data checkout at
`$SDV_VALIDATION_NFL_DATA_ROOT`). The gate floors in
`tests/football/test_spring_football_parity.py` (Brier ceiling 0.11, row
floors 442/445) were observed on THIS committed corpus — regenerating the
fixtures means re-observing and re-documenting those numbers, never
loosening them.
