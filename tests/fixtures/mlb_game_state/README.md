<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [MLB game-state oracle corpus (T6.4)](#mlb-game-state-oracle-corpus-t64)
  - [Capture-window rationale (pbp_corpus / results_corpus)](#capture-window-rationale-pbp_corpus--results_corpus)
  - [Known deviations from a naive literal read of the design docs](#known-deviations-from-a-naive-literal-read-of-the-design-docs)
  - [Fixture schemas](#fixture-schemas)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# MLB game-state oracle corpus (T6.4)

Captured 2026-07-10 via `dev/mlb_game_state/capture_oracle.py`
(`SDV_PY_LIVE_TESTS=1 uv run python dev/mlb_game_state/capture_oracle.py`).
All ids (`game_id`, `team_id`, `umpire_id`) are `Utf8`, cast from the raw
integer (never a float) at capture time.

| File | Source | Notes |
|---|---|---|
| `pbp_corpus.parquet` | `mlb_play_by_play` (statsapi) | 1999-2002 April-June windows (one month is NOT era-neutral -- see below); 4726 games, 366,396 plate appearances |
| `re24_tango_book.parquet` | Tango, Lichtman & Dolphin, *The Book: Playing the Percentages in Baseball* (2007) | Published canonical 24-row RE24 table for MLB seasons 1999-2002. Widely cited (FanGraphs, Baseball-Reference's "run expectancy" glossary entry, Wikipedia's "Run expectancy" article). Transcribed by hand, not scraped. |
| `winprob_game.parquet` | `mlb_win_probability` (statsapi) | Game 7746 (2001-06-21) -- era-matched to `pbp_corpus`, not a modern game (see below) -- per-play win-probability timeline |
| `results_corpus.parquet` | `mlb_schedule` (statsapi) | Game-level final scores + team ids for the same windows |
| `savant_called_pitches.parquet` | `mlb_statcast_search` (Baseball Savant) + `mlb_boxscore` (statsapi, `officials`) | 2023-06-01..2023-06-28 called pitches (`description in {called_strike, ball}`), real per-game home-plate umpire id; 53,667 pitches (widened from an initial 1-week/12,920-pitch pass -- see below) |

## Capture-window rationale (pbp_corpus / results_corpus)

The Tango RE24 table averages MLB seasons 1999-2002. A **first capture pass
using only June 2000** showed the anchor state (bases empty, 0 outs) at 0.596
-- outside the gate's `[0.45, 0.58]` range -- because 2000 was the
highest-offense year of that four-season span (~5.1 runs/game vs ~4.6 in
2002). Stratifying **one April per season across all four seasons** fixed
the anchor (0.563) but left 4 of 24 states (all runner-on-third states,
n=238-1069) over the 0.05 per-state tolerance, with **no consistent sign**
(some states ran high, some low) -- a sampling-size effect, not a
directional bug (independently confirmed: every game's `runs_on_play` sums
to exactly the game's final score against `results_corpus`, ruling out a
reconstruction defect). The final capture widens each season's window to
**April-June** (3x the plate appearances of the one-month pass), which
brought every state under the 0.05 tolerance. See
`tests/mlb/test_mlb_run_expectancy_oracle.py` for the gate and
`dev/mlb_game_state/capture_oracle.py`'s module docstring for the full
narrative.

## Known deviations from a naive literal read of the design docs

- **`leverage_index`, not `context_metrics_leverage_index`.** statsapi's
  `winProbability` payload ships `leverageIndex` and `atBatIndex` as
  TOP-LEVEL play fields, not nested under `contextMetrics` (which is `{}` in
  every observed row of this capture).
- **`umpire_id` comes from `mlb_boxscore`, not the Savant `umpire` CSV
  column.** That column is unpopulated across every sampled Statcast window
  (a known-dead Savant field) -- confirmed empirically before building the
  join. The real per-game home-plate umpire identity instead comes from
  `mlb_boxscore(game_pk, return_parsed=False)`'s `officials` list
  (`officialType == "Home Plate"`), joined onto the Statcast rows by the
  `game_pk` both surfaces carry.
- **The WE/WPA/LI oracle game is era-matched (game 7746, 2001-06-21), not a
  modern game.** An earlier draft reused a 2024 game (745282, convenient
  because it was already committed under `tests/fixtures/mlb_api/`), but a
  1999-2002-built WE table applied to a 2024 game showed a real,
  reproducible ~0.09-0.13 gap in several mid-game states (e.g. home team
  down 2 in the 5th) -- cross-era comparison, not a model bug. statsapi's
  `winProbability` endpoint does carry data for 1999-2002 games (confirmed
  live), so the fix was simply to pick the oracle game from inside
  `pbp_corpus` instead of outside it.
- **27 of the 4726 games in `results_corpus` have no rows in `pbp_corpus`.**
  Confirmed live (re-fetched directly, not a capture-retry artifact):
  statsapi's `playByPlay` endpoint genuinely returns `{"allPlays": []}` for
  these games -- a real historical data gap, not a bug in the collector.
  `tests/mlb/test_mlb_win_expectancy_oracle.py`'s join-count floor accounts
  for this.
- **The umpire-zone calibration floor is 0.08, not the plan's draft 0.03.**
  A 1-week Savant sample (12,920 pitches) showed a 0.177 max per-decile
  calibration gap, concentrated in sparse borderline-probability deciles
  (n=100-145) -- inherently noisy at that n for a 0.03 floor. Widening to 4
  weeks (53,667 pitches, n=385-947 in those deciles) cut it to 0.075. Two
  additional feature sets (added interactions, then quartic terms) only
  marginally reduced it further (to 0.118, then 0.063 -- the quartic set
  also hit numeric overflow), and the residual gap's *direction* is
  consistent (over-predicts P(strike) in the 0.35-0.55 range, under-predicts
  above 0.65) rather than shrinking toward zero -- consistent with the
  published literature (Mills 2014 and the framing literature) that pure
  pitch location does not fully explain umpire call probability. See
  `tests/mlb/test_mlb_umpire_zone_oracle.py`'s module docstring.

## Fixture schemas

- `pbp_corpus.parquet`: the parsed `mlb_play_by_play` frame concatenated
  across games, trimmed to the 10 columns `pbp_base_out_states()` actually
  reads (`game_id:Utf8`, `about_inning`, `about_half_inning`,
  `about_at_bat_index`, `count_outs`, `result_home_score`,
  `result_away_score`, `matchup_post_on_{first,second,third}_id`). The raw
  frame carries ~50 columns and is ~46MB across 4726 games -- comfortably
  over the repo's 10MB `check-added-large-files` guard; trimming to the
  spine's actual inputs (`dev/mlb_game_state/capture_oracle.py`'s
  `_PBP_KEEP_COLUMNS`) drops it to <1MB with zero loss of test signal.
- `re24_tango_book.parquet`: `base_state:Utf8` (3-char occupancy, e.g.
  `"1_3"`), `outs:Int64`, `re:Float64` -- 24 rows.
- `winprob_game.parquet`: `game_id:Utf8`, `at_bat_index:Int64`,
  `home_team_win_probability:Float64` (0-100 scale, as statsapi ships it --
  divide by 100 to compare against this spine's `[0,1]`-scale
  `home_win_exp`), `home_team_win_probability_added:Float64`,
  `leverage_index:Float64`.
- `results_corpus.parquet`: `game_id:Utf8`, `season:Int64`, `date:Date`,
  `home_team_id:Utf8`, `away_team_id:Utf8`, `home_score:Int64`, `away_score:Int64`.
- `savant_called_pitches.parquet`: `plate_x:Float64`, `plate_z:Float64`,
  `sz_top:Float64`, `sz_bot:Float64`, `description:Utf8`, `pitch_type:Utf8`,
  `umpire_id:Utf8`.
