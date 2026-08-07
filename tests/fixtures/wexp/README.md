<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [wexp fixtures — win-expectancy bake-off market oracle](#wexp-fixtures--win-expectancy-bake-off-market-oracle)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# wexp fixtures — win-expectancy bake-off market oracle

Real captured data only (never synthetic). Regenerate with
`uv run python dev/wexp/capture_market_fixtures.py` (gitignored script).
Captured 2026-08-06.

| file | rows | source | contents |
|---|---|---|---|
| `nfl_schedule_sample.parquet` | 821 | `load_nfl_schedule` (nflverse `schedules/games.parquet`) | Full seasons 2009 / 2020 / 2024, 16 cols: keys, teams, `result`, closing `spread_line` / `total_line` / moneylines (PFR closing lines; **no opens exist for NFL pre-2020 anywhere in our stack**), rest days, QB ids. Null closing ML: 14 games (all 2009). |
| `cfb_line_odds_sample.parquet` | 73,775 | `cfbfastR-data/betting/parquet/cfb_line_odds.parquet` (local archive, 2006–2025) | Per-side per-book line rows for a deterministic sample (first 400 sorted non-null `game_id`s per season) of seasons 2015 (68,785 rows — many-book archive era) and 2024 (4,990 rows — CFBD era). `season` cast to Int32 (source stores f64). |
| `cfb_schedule_sample.parquet` | 799 | `load_cfb_schedule` (`espn_cfb_schedules` release) | Outcome + slice columns (`home_points`/`away_points`, divisions, `neutral_site`) for the sampled games; 799/800 archive games matched. |

Notes:

- ID discipline: `game_id` is Utf8 in oracle outputs, cast from the raw
  integer; the CFB line archive's `game_id` is Int64 at the join.
- The 2015 sample is the *first* 400 game_ids (deterministic, not random) —
  it skews early-season/lower-id games; oracle calibration numbers on it
  are sanity anchors, not market-truth estimates.
- Observed data garbage handled by the builder: money_line rows with
  `|odds| < 100` (e.g. 0.0) are dropped before the consensus median.
- Game-level opening-spread coverage (any book): 2015 100%, 2024 68.9% —
  much higher than row-level coverage because the median ignores books
  without an open.
