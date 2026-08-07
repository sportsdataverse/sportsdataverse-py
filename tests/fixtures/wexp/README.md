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
| `cfb_schedule_sample.parquet` | 799 | `load_cfb_schedule` (`espn_cfb_schedules` release) | Outcome + slice columns (`home_points`/`away_points`, divisions, `neutral_site`) plus `home_id`/`away_id` (ESPN team ids, added at the 2026-08-06 re-capture for the oracle's `home_team_id`/`away_team_id` contract columns) for the sampled games; 799/800 archive games matched. |
| `cfb_talent_sample.parquet` | 601 | `load_cfb_team_talent` (cfbfastR-data release) | 247 talent composite for seasons 2015 + 2024 (293 + 308 teams, 0 null composites). Keys: `season`, `team_id` (ESPN, Int64), display `team`. D3 continuity-prior input; preseason knowledge for its season. |
| `cfb_returning_sample.parquet` | 443 | `load_cfb_returning_production` (cfbfastR-data release) | Returning production for seasons 2015 + 2024 (214 + 229 teams, 0 null `overall_returning`). Only `overall_returning` feeds D3 (`def_returning` unusable pre-2023). |
| `nfl_ratings_weekly_sample.parquet` | 1,952 | `load_nfl_ratings_weekly` (sportsdataverse-data release, captured 2026-08-07) | Per-week as-of ratings vintages for seasons 2009 / 2020 / 2024 (32 teams x weeks 2-21/22; STRICTLY EXCLUSIVE `as_of_week`). Feeds the true-EPA ridge arm via `net_vintages_view`. `season` is Int64 at source (view casts Int32). |

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
- CFB oracle inner-join drops (16 of 799; itemized so a reviewer can see
  they are one-off irregulars, not a systematic class): game_ids
  400603836, 400756901, 400756914, 400756925, 400756943, 400756969,
  400756993, 400756997, 400760500, 400763470, 400763497 (2015, 11 games)
  and 401628336, 401628624, 401628637, 401629045, 401632069 (2024,
  5 games). All 16 HAVE spread rows; the drop cause is the modal
  abbr→name inference failing on this small 400-game sample
  (single-game/ambiguous abbrs: `MSU`, `NC`, FCS one-offs like
  Lindenwood/Merrimack). On the full 8k-game corpus the modal inference
  is unambiguous (per the ported cfb-data docstring); the test floor
  (>=780) plus a per-season keep-rate check guard against a season-wide
  resolution failure.
