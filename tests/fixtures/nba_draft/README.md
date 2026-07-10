<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NBA draft/projection oracle corpus (T3.4)](#nba-draftprojection-oracle-corpus-t34)
  - [Files](#files)
  - [Fitted box-value formula (Task 0.3)](#fitted-box-value-formula-task-03)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NBA draft/projection oracle corpus (T3.4)

Captured 2026-07-08 from a residential IP via `SDV_PY_NBA_STATS_LIVE=1 uv run
python dev/nba_draft/capture_corpus.py`. All `player_id` columns are `Utf8`
(the raw integer id, stringified via `.cast(pl.Int64).cast(pl.Utf8)` -- never
from a float, per the id/join-key discipline). `draft_year`/`season` are
`Int64`.

**Corpus expansion (2026-07-08, same day):** the initial capture covered only
combine classes 2016-2019 (~250 prospects). The Task-1.4 draft-model backtest
on that corpus produced a holdout Spearman of essentially zero (and a Phase-2
aging curve too noisy to interpret) -- exhaustively debugged (see
`tests/nba/test_nba_draft_backtest.py`'s module docstring) and traced to
genuine small-sample size, not a bug. `dev/nba_draft/capture_corpus_expand.py`
(scratch, gitignored like all of `dev/`) pulled combine classes back to 2000
(20 classes total, 1328 prospects) and their full season-stats histories
(7378 player-season rows) -- realizing the design doc's original intent that
the all-era career-value label span decades, not just the 2016+ v3-pbp
overlap. `combine_2016_2019.parquet` / `draft_outcomes.parquet` /
`season_stats_raw.parquet` below reflect the expanded corpus (filename kept
for git-history continuity even though it now spans 2000-2019).
`nba_bpm_overlap.parquet` / `aging_published.parquet` were NOT re-captured
(BPM only exists 2016+ anyway; the published-curve fixture is a hand
citation, not a live capture).

## Files

- **`combine_2016_2019.parquet`** (1328 rows, 20 classes 2000-2019) -- combine
  anthro + drills + spot shooting + non-stationary shooting, joined on
  `player_id`. Source: `nba_stats_draftcombineplayeranthro` /
  `draftcombinedrillresults` / `draftcombinespotshooting` /
  `draftcombinenonstationaryshooting` (`season_year="2000".."2019"`).
  `draft_year` here is the **combine class year** (the year the player
  attended the combine), which for a handful of players differs from the
  year they were actually drafted (players who returned to school / withdrew
  and re-entered) -- see `draft_outcomes.parquet`'s `draft_year` for the
  authoritative drafted year.
  `nba_stats_draftcombinestats` was tried and returns 0 rows for every NBA
  season in the capture sweep (confirmed live 2026-07-08) -- not used for the
  NBA corpus. `nba_stats_drafthistory` / `draftboard` are absent from the
  generated NBA wrapper surface entirely (dropped in the codegen capture
  sweep), confirming design-doc decision #2: draft outcome comes from
  `commonplayerinfo`, not a drafthistory endpoint.

- **`draft_outcomes.parquet`** (899 rows) -- `nba_stats_commonplayerinfo`
  (`CommonPlayerInfo` result set) per combine player_id: `draft_year`,
  `draft_round`, `draft_number`, `drafted` (`draft_number` present and > 0).
  A player is absent here (not zero-filled) when `CommonPlayerInfo` returned
  no row (no `person_id` record -- e.g. never signed an NBA contract).

- **`season_stats_raw.parquet`** (5773 rows, deduplicated -- see below) --
  `nba_stats_playercareerstats` (`SeasonTotalsRegularSeason` result set) per
  combine player_id, one row per `(player_id, season_id)`. This is the raw
  box-total corpus consumed by `dev/nba_draft/fit_box_value.py` to derive
  per-100 rates and, after fitting
  `box_value_coef`, to materialize `career_values.parquet` +
  `rookie_values.parquet` below.

  **Deduplication (2026-07-08):** the raw capture had 772/7378 rows that were
  mid-season-trade duplicates -- `nba_stats_playercareerstats` emits one row
  per team a traded player suited up for in a season *plus* a `team_id=0`
  "TOT" (total) aggregate row for the same `(player_id, season_id)`. Every
  downstream fit (`career_value_from_seasons`, the aging curve's per-season
  deltas, availability's GP%) was silently double/triple-counting those
  players' box totals and minutes by summing across the duplicate rows.
  Fixed by keeping only the `team_id=0` TOT row when one exists for a
  `(player_id, season_id)` group (else the single row), bringing the corpus
  to 5773 rows. 13 further rows carried a malformed `player_id` ("199", an
  upstream id-join glitch) with season labels back to 1987-88 -- excluded via
  `dev/nba_draft/fit_availability.py`'s `MIN_SEASON=2000` filter, not by
  further deduplication.

- **`nba_bpm_overlap.parquet`** (418 rows) -- the shipped `nba_bpm` scored via
  `nba_box_logs` + `nba_player_positions` for the 2016-17..2019-20 seasons
  (full-league box logs, filtered post-hoc to the combine-player id set).
  Columns: `player_id, season:Int64 (season start year), bpm, minutes`. This
  is the box-value formula's scale anchor (never the primary label). Not
  re-captured for the expanded corpus -- BPM only exists 2016+ (v3 pbp).

- **`career_values.parquet`** (847 rows) -- `player_id, career_value:Float64,
  seasons_played:Int64, total_minutes:Float64`. Materialized by
  `dev/nba_draft/fit_box_value.py` (Task 0.3) from `season_stats_raw.parquet`
  using the fitted `box_value_coef`/`replacement` (all-era: every season a
  combine player played, not just the 2016+ overlap). Combine players with
  no career games (undrafted / never played) are absent, not zero-filled --
  callers `fill_null`/`coalesce` to 0 on join for those.

- **`rookie_values.parquet`** (1328 rows) -- `player_id, draft_year,
  rookie_value:Float64, soph_value:Float64, rookie_min:Float64`. First/second
  season-index rows (by `season`) per player from the same fitted-coefficient
  pipeline; `0.0` where a player never played a rookie/soph NBA season.

- **`aging_published.parquet`** (19 rows, ages 20-38) -- hand-transcribed,
  order-of-magnitude published NBA aging-curve shape: a rise from age 20
  to a peak at age 27 (`rel_value = 1.0`), then decline, consistent with the
  broad pattern documented across published aging-curve research (Nate
  Silver / Neil Paine's "delta method" articles at
  FiveThirtyEight/Basketball-Reference, and Kevin Pelton's WARP aging-curve
  studies at ESPN). This is a **shape citation**, not a scrape of any single
  dataset -- used only as the Phase-2 calibration target (peak-age range +
  unimodal shape + rank correlation), per design-doc §4's "oracle *or*
  calibration target" allowance.

## Fitted box-value formula (Task 0.3)

`box_value_coef` (intercept + `pts100, reb100, ast100, stl100, blk100,
tov100, ts_pct, usg`) and `replacement` in `nba_draft_constants.py`'s
`LEAGUE_CONSTANTS["nba"]` are the printed output of
`dev/nba_draft/fit_box_value.py` (ridge-fit vs `nba_bpm_overlap.parquet`,
lambda chosen by 5-fold CV, `MIN_MINUTES=300`). In-sample
Spearman(fit, nba_bpm) = 0.94 on the 256-row anchor (re-fit after the
trade-duplicate dedup above). WNBA/G-League constants
are seeded from the NBA fit pending the Phase 5 women's re-fit on
`wnba_stats` data (documented in `wnba_draft_constants.py`).

Per-100 rates use a **per-player estimated-possessions proxy**
(`fga + 0.44*fta + tov`), not team-pace-adjusted true usage% --
`playercareerstats` season totals carry no team-pace context. This is a
documented approximation; it is sufficient for the rank-based (Spearman)
oracle gates this label serves.
