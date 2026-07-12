<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [league_ports fixtures — provenance](#league_ports-fixtures--provenance)
  - [Schema / dtype notes](#schema--dtype-notes)
  - [Train / holdout split (leakage boundary)](#train--holdout-split-leakage-boundary)
  - [Regenerate-together invariant](#regenerate-together-invariant)
  - [College baseball / softball fixtures (T7.3, model 5)](#college-baseball--softball-fixtures-t73-model-5)
    - [Feasibility finding: full base-out reconstruction, not the reduced fallback](#feasibility-finding-full-base-out-reconstruction-not-the-reduced-fallback)
    - [Known limitation: single-game sample size](#known-limitation-single-game-sample-size)

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

## College baseball / softball fixtures (T7.3, model 5)

Real single-game ESPN Core v2 `.../plays` captures, one per league. Captured
**2026-07-12**. Feed
`sportsdataverse.baseball.college_run_expectancy.college_baseball_state` /
`college_baseball_re24` / `college_baseball_wpa` (by-reference shims in
`sportsdataverse/baseball/college_{baseball,softball}/college_*_re.py`).

| file | game_id | PAs | final score | notes |
|---|---|---|---|---|
| `college_baseball_game_plays.json` | 401874444 | 71 | away 3, home 4 | 9-inning game; used by the real-fixture correctness + oracle-gate tests. |
| `college_softball_game_plays.json` | 401873598 | 59 | away 4, home 5 | 7-inning game (regulation confirmed: max `period.number` observed == 7). |
| `college_baseball_summary.json` | 401874444 | — | — | Site v2 summary capture for the same game; not currently consumed by the RE24/WPA port (state reconstruction only needs `game_plays`). |
| `college_softball_summary.json` | 401873598 | — | — | Same, softball. |

### Feasibility finding: full base-out reconstruction, not the reduced fallback

Both `game_plays` payloads carry `items[].type.text == "Play Result"` rows
with `participants[].type` in `{pitcher, batter, onFirst, onSecond, onThird}`
(post-play base occupancy), cumulative `outs`, cumulative `awayScore`/
`homeScore`, and `atBatId`/`team.$ref` grouping keys — enough to reconstruct
the full 24-state base-out substrate per PA (see
`sportsdataverse/baseball/college_run_expectancy.py` module docstring for the
exact extraction contract, including the multi-row-per-`atBatId` gotcha).
This is a **better outcome than the Phase 0 plan's fallback contingency**
(an inning-score-only reduced state), so `college_baseball_state` needs no
`reduced=True` flag.

### Known limitation: single-game sample size

Each fixture is **one game** (71 / 59 PAs) — enough to prove the
reconstruction is correct end to end (schema, no nulls, monotone outs,
`runs_after` tops out at the real final score — see
`tests/baseball/test_college_run_expectancy.py`), but **not** enough to
support a statistically meaningful RE24 matrix or a directional
college-vs-MLB run-environment comparison (the bases-empty/0-out anchor cell
has only ~14-18 observations per game). The oracle gate
(`tests/baseball/test_college_run_expectancy_oracle.py`) documents this and
gates on what a 1-game sample can support — hard non-increasing monotonicity
with a tie-count ratchet, strict `RE(___,0) > RE(___,1) > RE(___,2)` on the
fully-observed bases-empty column, an anchor band derived from the fitted
matrix's own observed value (0.75x–1.3x), and the exact run_value round-trip
identity `sum(run_value) == total_runs − n_halves × anchor` — rather than a
point comparison. All floors and the observed values they derive from are
tabulated in that test module's docstring; never lower them. No published
college RE24 snapshot exists to cross-check against either — capturing a
multi-game college corpus is a documented follow-up, not done here.

Capture script: `dev/league_ports/capture_college_baseball_softball.py`
(force-added despite the gitignored `dev/` convention — it is the fixtures'
provenance).
