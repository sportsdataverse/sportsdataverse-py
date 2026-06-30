<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NBA G-League Engine Fixture Provenance](#nba-g-league-engine-fixture-provenance)
  - [Capture metadata](#capture-metadata)
  - [Confirmed game_id prefix / format](#confirmed-game_id-prefix--format)
  - [G-League rotation coverage](#g-league-rotation-coverage)
  - [Game ids captured](#game-ids-captured)
  - [Files per game directory](#files-per-game-directory)
  - [Shape parity with NBA v3](#shape-parity-with-nba-v3)
  - [Rate-limit notes](#rate-limit-notes)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NBA G-League Engine Fixture Provenance

Raw `return_parsed=False` payloads captured from `stats.nba.com` for the
NBA G-League possession-engine offline test suite (Phase 5.1, Task 0).

## Capture metadata

- **Capture date:** 2026-06-29
- **League ID:** 20 (NBA G-League)
- **Season:** 2024 (Regular Season)
- **Source:** `stats.nba.com` via `sportsdataverse.nba.nba_stats` curl_cffi runtime

## Confirmed game_id prefix / format

First game id from live leaguegamelog (season=2024): `2022400003` (format: 10-digit string, prefix=`20`). Sample pool: ['2022400003', '2022400009', '2022400002', '2022400004', '2022400010']

## G-League rotation coverage

2 candidates tried; 2 had populated rotation.

> **Important:** `gamerotation` coverage is not universal for G-League games.
> Fixtures were selected to include only games with non-empty HomeTeam
> AND AwayTeam stint rowSets (required for the keystone minutes-recon gate).

## Game ids captured

- `2022400003`
- `2022400009`

## Files per game directory

| File | Source endpoint | Notes |
|---|---|---|
| `playbyplayv3.json` | `GET https://stats.nba.com/stats/playbyplayv3?GameID=<id>` | Raw v3 PBP; `payload["game"]["actions"]` is the action list; no LeagueID param |
| `boxscoretraditionalv3.json` | `GET https://stats.nba.com/stats/boxscoretraditionalv3?GameID=<id>` | Raw v3 box score; home/away team ids under `payload["boxScoreTraditional"]`; no LeagueID param |
| `gamerotation.json` | `GET https://stats.nba.com/stats/gamerotation?GameID=<id>&LeagueID=20` | Raw rotation stints; HomeTeam/AwayTeam resultSets with PERSON_ID + IN/OUT_TIME_REAL; **LeagueID=20 required** |

## Shape parity with NBA v3

G-League v3 endpoints share the same payload shape as their NBA counterparts.
The `sportsdataverse.nba` possession-engine cores
(`enhanced_pbp_from_payload`, `boxscore_home_away`,
`players_on_court_from_rotation`) accept these payloads directly with
`league_id="20"` passed to `enhanced_pbp_from_payload`.

## Rate-limit notes

Calls were spaced ≥ 3 seconds apart with 3-retry exponential backoff
(5s → 15s → 45s). `stats.nba.com` enforces TLS-fingerprint gating;
the `curl_cffi` impersonation runtime is required.
