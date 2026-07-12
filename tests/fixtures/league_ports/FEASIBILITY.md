<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Spring-football (UFL/XFL) capture-verification findings](#spring-football-uflxfl-capture-verification-findings)
  - [Finding 1 — ESPN publishes NO play-by-play for UFL games](#finding-1--espn-publishes-no-play-by-play-for-ufl-games)
  - [Finding 2 — the ESPN win-probability oracle does not exist for UFL/XFL](#finding-2--the-espn-win-probability-oracle-does-not-exist-for-uflxfl)
  - [Finding 3 — XFL 2023 summaries DO carry full play-by-play](#finding-3--xfl-2023-summaries-do-carry-full-play-by-play)
  - [Fixture provenance](#fixture-provenance)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Spring-football (UFL/XFL) capture-verification findings

Live findings the spring-football port's tests, docstrings, and gates cite.
All claims below were verified against the live ESPN API on the dates given;
the fixtures in this directory are the captured evidence. Regenerate
everything with ONE run of
`dev/league_ports/capture_spring_football_fixtures.py` (see the
"Regenerate-together" note in `README.md`).

## Finding 1 — ESPN publishes NO play-by-play for UFL games

Probed every completed event on the UFL scoreboard windows
`20240330-20240616` (2024 season) and `20250321-20250608` (2025 season)
during capture (2026-07-12): **zero** events carry
`drives.previous[].plays[]`. The summary payload has no `drives` key at all —
observed top-level keys: `againstTheSpread`, `boxscore`, `broadcasts`,
`format`, `gameInfo`, `header`, `leaders`, `meta`, `news`, `odds`,
`pickcenter`, `standings`. Re-verified live 2026-07-12 on completed events
`401638335` + `401638336` (2024-06-01), `401743455` (2025-04-12), and
`401743508` (2025-06-14): `plays=0` for all four.

`ufl_summary.json` is therefore a REAL no-play-by-play capture, kept
deliberately — `build_spring_football_pbp(..., league="ufl")` returning a
zero-row contract frame is the honest output on today's real data, and
`test_ufl_calibration_gate_is_a_documented_park_not_a_skip` pins the finding
so it goes loud (not silently skipped) the day ESPN backfills UFL pbp.

## Finding 2 — the ESPN win-probability oracle does not exist for UFL/XFL

`espn_{ufl,xfl}_game_probabilities(event_id, return_parsed=False)` returns:

```json
{"error": {"message": "Probabilities are not supported for sport: football,
league: <ufl|xfl>, competition: <event_id>", "code": 400}}
```

Verified live 2026-07-12 on `401638335` (ufl) and `401517780` / `401517747`
(xfl). There is no oracle payload to capture, so gate (b) in
`tests/football/test_spring_football_parity.py` substitutes a
realized-game-outcome Brier vs the naive 0.5-constant baseline (floors from
observed values, documented in that file).

## Finding 3 — XFL 2023 summaries DO carry full play-by-play

The three `xfl_summary*.json` fixtures below average ~150 plays/game and
drive the contract, calibration, and shim tests.

## Fixture provenance

Summary capture URL shape:
`https://site.api.espn.com/apis/site/v2/sports/football/<league>/summary?event=<id>`
(the `espn_{ufl,xfl}_summary(event_id, return_parsed=False)` wrapper).
Captured **2026-07-12**.

| file | league | event | date | result | plays |
|---|---|---|---|---|---|
| `xfl_summary.json` | xfl | 401517780 | 2023-04-01 | Vegas Vipers 26, San Antonio Brahmas 12 | 159 |
| `xfl_summary_2.json` | xfl | 401517747 | 2023-03-12 | Orlando Guardians 16, Houston Roughnecks 44 | 140 |
| `xfl_summary_3.json` | xfl | 401517746 | 2023-03-12 | Seattle Sea Dragons 15, San Antonio Brahmas 6 | 152 |
| `ufl_summary.json` | ufl | 401638335 | 2024-06-01 | Birmingham Stallions 20, Michigan Panthers 19 | 0 (Finding 1) |

`nfl_parity_2023_game.parquet` — one real NFL game (`2023_01_ARI_WAS`,
168 plays x 111 cols) in nflverse shape for the gate-(a) byte-for-byte
parity test. Sliced 2026-07-12 from the local nfl-data producer output
(`$SDV_VALIDATION_NFL_DATA_ROOT/out/model_pbp_2023.parquet`, the sdv-py
nflverse-parity `nfl_model_pbp` dataset build). The `enrich_nfl_pbp` OUTPUT
columns (`ep`/`epa`/`wp`/`vegas_wp`/`wpa`/`cp`/`cpoe`/`xyac_*`) are stripped
so the fixture is a pure model-INPUT frame; the `roof` one-hots
(`retractable`/`dome`/`outdoors`) are filled with `ep_wp`'s own unknown-roof
default (`retractable=1`) because `roof` is all-null in the producer output.
