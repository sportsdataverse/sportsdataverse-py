<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [WNBA Engine Fixture Provenance](#wnba-engine-fixture-provenance)
  - [Capture metadata](#capture-metadata)
  - [Game ids captured](#game-ids-captured)
  - [Files per game directory](#files-per-game-directory)
  - [Shape parity with NBA v3](#shape-parity-with-nba-v3)
  - [Rate-limit notes](#rate-limit-notes)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# WNBA Engine Fixture Provenance

Raw `return_parsed=False` payloads captured from `stats.nba.com` (which serves
WNBA game data under LeagueID=10 alongside `stats.wnba.com`) for the WNBA
possession-engine offline test suite (Phase 5, Task 0).

## Capture metadata

- **Capture date:** 2026-06-29
- **League ID:** 10 (WNBA)
- **Season:** 2024 (Regular Season)
- **Source host:** `stats.nba.com` via `sportsdataverse.nba.nba_stats` curl_cffi runtime
  (WNBA game data is served by both `stats.wnba.com` and `stats.nba.com`; the
  latter was used after `stats.wnba.com` returned timeouts with 0 bytes received
  — a transient geo/TLS block, not a structural difference)
- **Discovery:** game ids discovered via `wnba_stats_leaguegamelog(season="2024",
  season_type_all_star="Regular Season", league_id="10")` on `stats.wnba.com`

## Game ids captured

- `1022400001`
- `1022400003`

## Files per game directory

| File | Source endpoint | Notes |
|---|---|---|
| `playbyplayv3.json` | `GET https://stats.nba.com/stats/playbyplayv3?GameID=<id>` | Raw v3 PBP; `payload["game"]["actions"]` is the action list (434 / 429 actions) |
| `boxscoretraditionalv3.json` | `GET https://stats.nba.com/stats/boxscoretraditionalv3?GameID=<id>` | Raw v3 box score; home/away team ids under `payload["boxScoreTraditional"]` |
| `gamerotation.json` | `GET https://stats.nba.com/stats/gamerotation?GameID=<id>&LeagueID=10` | Raw rotation stints; AwayTeam/HomeTeam resultSets with PERSON_ID + IN/OUT_TIME_REAL |

## Shape parity with NBA v3

WNBA v3 endpoints share the **identical payload shape** as their NBA counterparts.
The `sportsdataverse.nba` possession-engine cores accept these payloads directly
with `league_id="10"` passed to `enhanced_pbp_from_payload`. Verified for both
captured games:

| Check | game 1022400001 | game 1022400003 |
|---|---|---|
| `pbp["game"]["actions"]` non-empty | 434 actions | 429 actions |
| action keys: actionNumber, period, clock, personId, description | PASS | PASS |
| `boxScoreTraditional.homeTeamId` + `awayTeamId` present | 1611661322 / 1611661313 | 1611661319 / 1611661317 |
| player `statistics.minutes` + `position` present | PASS | PASS |
| rotation HomeTeam + AwayTeam resultSets with PERSON_ID / IN_TIME_REAL / OUT_TIME_REAL | PASS (38+27 stints) | PASS (22+28 stints) |
| `enhanced_pbp_from_payload(league_id="10")` rows > 0 | 434 | 429 |
| `boxscore_home_away` returns two distinct positive ints | PASS | PASS |
| `players_on_court_from_rotation` rows > 0 | 434 | 429 |
| 10-distinct-ids spot check (row 0) | PASS | PASS |

## Rate-limit notes

Calls were spaced >= 3 seconds apart with 3-retry exponential backoff
(5s -> 15s -> 45s). The `curl_cffi` browser-TLS impersonation runtime is
required for both `stats.nba.com` and `stats.wnba.com`.
