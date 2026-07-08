<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NBA shot-value oracle fixtures](#nba-shot-value-oracle-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NBA shot-value oracle fixtures

Committed captures backing the T3.1 shot-value gates in
`tests/nba/test_nba_shot_value_oracle.py`. Captured **2026-07-08** from
stats.nba.com (residential IP; `SDV_PY_NBA_STATS_LIVE=1`) by
`dev/nba_shot_value/capture_fixtures.py` — season **2022-23**,
`league_id="00"`. All frames snake-cased by `parse_nba_stats_result_sets`;
`game_id` is `Utf8` (zero-padded, never int-cast), `player_id`/`team_id` are
`Int64`.

| File | Rows | Source (result set) |
|---|---:|---|
| `shotchart_2023.parquet` | 41,587 | `nba_stats_shotchartdetail` per-player `Shot_Chart_Detail`, concatenated. **Capture gotcha:** the wrapper's default `game_id_nullable=""` zero-pads to `"0000000000"` (a nonexistent game filter → 0 shots); pass `game_id_nullable=None` for all season shots. |
| `league_averages_2023.parquet` | 20 | `LeagueAverages` (the free zone-FG% table the same call returns). |
| `playerdashptshots_sample.parquet` | 80 | `playerdashptshots` `ClosestDefenderShooting` + `ShotClockShooting`, stacked with a `result_set` tag + common `bucket`, for 8 players. |
| `playerdashptshotdefend_sample.parquet` | 48 | `playerdashptshotdefend` for the same 8 players. |

**Player set (40, zone-diverse but ELITE):** rim bigs 203507 (Giannis), 203999 (Jokic), 1629627 (Zion), 1627734 (Sabonis), 1628389 (Bam), 203497 (Gobert), 1629028 (Ayton), 1628386 (J. Allen); corner-3 wings 202691 (Klay), 1627741 (Hield), 1629130 (D. Robinson), 1628960 (G. Allen), 1627736 (Beasley), 203925 (Harris), 203484 (KCP); mid-range 201942 (DeRozan), 101108 (Paul), 201142 (Durant), 1626164 (Booker), 1628969 (M. Bridges); scorers 201939 (Curry), 1629029 (Doncic), 1628369 (Tatum), 203954 (Embiid), 1628983 (SGA), 1629027 (T. Young), 1629630 (Morant), 1628378 (Mitchell), 203081 (Lillard), 1627759 (J. Brown), 1630162 (A. Edwards), 2544 (LeBron), 202695 (Kawhi), 202331 (George), 202710 (Butler), 202681 (Irving), 1628973 (Brunson), 1628368 (Fox), 1630169 (Haliburton), 1630178 (Maxey). Tracking subset: 201939, 201942, 203507, 203999, 202691, 1628369, 203954, 1628983.

**Published NBA zone-FG% bands** (oracle tolerance, Basketball-Reference /
league splits): Restricted Area 0.58–0.68, Mid-Range 0.36–0.44, Left/Right
Corner 3 0.36–0.42, Above the Break 3 0.35–0.38.

**Selection-bias note (drives the gate design):** these are 40 above-average
shooters, so applying the LEAGUE baseline nets **+5.3%** (`Σactual/Σxpoints
≈ 1.053`) — selection, not miscalibration. The Phase-1 calibration gate
therefore tests the model's invariant via **self-calibration** (a baseline
built from these same shots nets to ~0: observed 0.00007), guarding the zone
join + 2/3 value assignment independent of who the shooters are, and
separately bounds the elite over-performance to [1.0, 1.10]. **Defender-
distance FG% is location-confounded** on this big-heavy set (tightest bucket
0.537 > wide-open 0.464 because tight coverage ⇒ rim attacks), so the Phase-2
gate asserts plausibility + joint consistency, not unconditional
monotonicity. Shrinkage `k` fitted split-half = 70.1 (reliability 0.699 raw →
0.707 shrunk).

Re-capture: `SDV_PY_NBA_STATS_LIVE=1 uv run python dev/nba_shot_value/capture_fixtures.py`.
