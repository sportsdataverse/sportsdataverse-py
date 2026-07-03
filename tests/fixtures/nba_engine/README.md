<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NBA Engine Fixture Provenance](#nba-engine-fixture-provenance)
  - [Game ids captured](#game-ids-captured)
  - [Files per game directory](#files-per-game-directory)
  - [`cdn_playbyplay.json` / `cdn_boxscore.json` — v3->v2 adapter structured-truth oracle](#cdn_playbyplayjson--cdn_boxscorejson--v3-v2-adapter-structured-truth-oracle)
    - [Capture metadata](#capture-metadata)
    - [Why these fixtures exist](#why-these-fixtures-exist)
    - [Measured counts (pinned as regression floors)](#measured-counts-pinned-as-regression-floors)
  - [Regeneration](#regeneration)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NBA Engine Fixture Provenance

This directory holds committed, offline oracle fixtures for the sdv-py NBA
v3-native possession/lineup engine (Phase 0 onward) and the hoopR v3->v2
play-by-play adapter (`sportsdataverse/nba/nba_v3_v2_adapter.py`). One
subdirectory per captured game, keyed on the `stats.nba.com` `GameID`.

## Game ids captured

| Game ID | Season | Matchup (away @ home) | Notes |
|---|---|---|---|
| `0022100001` | 2021-22 Regular Season | Brooklyn Nets (1610612751) @ Milwaukee Bucks (1610612749) | 2021-10-19, season opener |
| `0022200001` | 2022-23 Regular Season | Philadelphia 76ers (1610612755) @ Boston Celtics (1610612738) | 2022-10-18, season opener; see `0022200001/README.md` for the Phase 0 fixture methodology (`lineups_expected.parquet` / `enhanced_pbp_expected.parquet` derivation + pbpstats cross-check) |
| `0022300001` | 2023-24 Regular Season | Cleveland Cavaliers (1610612739) @ Indiana Pacers (1610612754) | 2023-11-03; the original `desc_extract2.py` scratchpad-proven fixture (assist 55/55, block 14/14, steal 17/17) |

## Files per game directory

| File | Source | Description |
|---|---|---|
| `playbyplayv3.json` | `stats.nba.com/stats/playbyplayv3` | Raw v3 play-by-play payload; `payload["game"]["actions"]` is the action list |
| `boxscoretraditionalv3.json` | `stats.nba.com/stats/boxscoretraditionalv3` | Raw v3 box score; home/away team + per-player roster under `payload["boxScoreTraditional"]` |
| `gamerotation.json` | `stats.nba.com/stats/gamerotation` | Raw rotation stints (Phase 0 on-court oracle input) |
| `lineups_expected.parquet` | pbpstats `LiveEnhancedPbp` (Phase 0 primary oracle) | On-court 10 per distinct v3 action_number |
| `enhanced_pbp_expected.parquet` | v3 payload (self-derived) | Enhanced play metadata + canonical `order_index` total order |
| `cdn_playbyplay.json` | `cdn.nba.com` live-data feed | Structured-truth oracle for the v3->v2 adapter's secondary-player extraction (see below) |
| `cdn_boxscore.json` | `cdn.nba.com` live-data feed | Companion box score for the same live-data feed (currently unused by the adapter tests, captured alongside `cdn_playbyplay.json` for completeness / future use) |

See `0022200001/README.md` for the detailed Phase 0 provenance of the first
five files (capture method, the `LiveEnhancedPbp` oracle, the `order_index`
tiebreak derivation, and the 4-part cross-check).

## `cdn_playbyplay.json` / `cdn_boxscore.json` — v3->v2 adapter structured-truth oracle

### Capture metadata

- **Capture date:** 2026-07-02
- **Source URLs:**
  - `https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{gameId}.json`
  - `https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{gameId}.json`
- **Required headers:** `Referer: https://www.nba.com` and
  `Origin: https://www.nba.com` — the `cdn.nba.com` host rejects requests
  missing these. No TLS impersonation or auth token is needed (unlike
  `stats.nba.com`'s JA3 fingerprint gate).

### Why these fixtures exist

`cdn_playbyplay.json` is the NBA's own richer "live" play-by-play feed. Unlike
`playbyplayv3.json` (the `stats.nba.com` v3 feed the adapter consumes as
input), every action here carries `assistPersonId` / `blockPersonId` /
`stealPersonId` directly — no description-regex parsing required. That makes
it an independent, structured ground truth for two things the v3 feed itself
does NOT expose:

1. **`tests/nba/test_nba_v3_v2_adapter.py`'s Task 2 tests** compare the
   v3-derived `player2_id`/`player3_id` (recovered via description-regex +
   structural block/steal association, see
   `_extract_secondary_players`) against this fixture's
   `assistPersonId`/`blockPersonId`/`stealPersonId` fields at the same
   `actionNumber` — a 1-to-1 agreement check against structured truth, not a
   self-referential check against the adapter's own output.
2. **Task 4's pbpstats round-trip test** feeds `cdn_playbyplay.json` +
   `cdn_boxscore.json` through a vendored pbpstats checkout's own `live` data
   provider and compares its possessions/period-starters against the same
   game's v3-derived-then-adapted-to-v2 frame fed through pbpstats'
   `stats_nba` provider — an independent-oracle cross-check, not a
   round-trip through the adapter's own logic.

`cdn_boxscore.json` is captured alongside `cdn_playbyplay.json` because
pbpstats' file-mode `live` data provider requires both files to construct a
`Game` object (`Boxscore` + `Possessions` sources); it is not otherwise read
directly by the adapter test suite today.

### Measured counts (pinned as regression floors)

| Game ID | actions | assist | block | steal |
|---|---:|---:|---:|---:|
| `0022100001` | 603 | 44 | 18 | 11 |
| `0022200001` | 526 | 40 | 6 | 16 |
| `0022300001` | 576 | 55 | 14 | 17 |

These per-field counts match `_EXPECTED_MATCH_RATES` in
`tests/nba/test_nba_v3_v2_adapter.py` exactly (100% agreement on all three
fields, all three fixtures — no roster name-collision misses observed).

## Regeneration

Re-capture with the same URLs (`Referer`/`Origin: https://www.nba.com`
headers) and overwrite the files (stem-matched). The parser/adapter tests are
payload-agnostic so a newer capture keeps working as long as the schema
doesn't drift; if the measured assist/block/steal counts change, update
`_EXPECTED_MATCH_RATES` (and the table above) to match.
