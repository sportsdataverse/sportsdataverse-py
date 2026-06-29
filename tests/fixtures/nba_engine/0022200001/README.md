<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Fixture provenance — game 0022200001](#fixture-provenance--game-0022200001)
  - [Files](#files)
  - [Capture method](#capture-method)
  - [Primary oracle — pbpstats LiveEnhancedPbp](#primary-oracle--pbpstats-liveenhancedpbp)
  - [Cross-check](#cross-check)
  - [lineups_expected schema](#lineups_expected-schema)
  - [enhanced_pbp_expected schema](#enhanced_pbp_expected-schema)
  - [Regeneration](#regeneration)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Fixture provenance — game 0022200001

| Field | Value |
|---|---|
| Game ID | 0022200001 |
| Season | 2022-23 |
| Matchup | Boston Celtics (home, 1610612738) @ Philadelphia 76ers (away, 1610612755) |
| Capture date | 2026-06-29 |

## Files

| File | Source | Description |
|---|---|---|
| `playbyplayv3.json` | `stats.nba.com/stats/playbyplayv3` | Raw v3 play-by-play payload (468 actions) |
| `boxscoretraditionalv3.json` | `stats.nba.com/stats/boxscoretraditionalv3` | Raw v3 box score (home/away team + player list with starters) |
| `lineups_expected.parquet` | pbpstats LiveEnhancedPbp (primary oracle) | Per-event on-court 10; 452 rows covering 526 pbpstats events |
| `enhanced_pbp_expected.parquet` | v3 payload (self-derived) | Enhanced play metadata; 468 rows, 44 substitutions |

## Capture method

`playbyplayv3` and `boxscoretraditionalv3` were fetched via the sdv-py
`nba_stats_*` wrappers which use `curl_cffi` with Chrome TLS impersonation
(`impersonate="chrome"`) to satisfy the `stats.nba.com` JA3 fingerprint
requirement.

## Primary oracle — pbpstats LiveEnhancedPbp

`lineups_expected.parquet` is derived from
`pbpstats.data_loader.live.enhanced_pbp.{web,loader}.LiveEnhancedPbpWebLoader /
LiveEnhancedPbpLoader`.

This loader fetches from:

```
https://nba-prod-us-east-1-mediaops-stats.s3.amazonaws.com/NBA/liveData/playbyplay/playbyplay_0022200001.json
```

AWS S3 — open, no JA3 or IP-reputation block. 526 events parsed; each
`ev.current_players` = `{team_id: [player_ids]}` representing on-court 10 at
that point in game time. `ev.event_num` aligns with v3 `actionNumber` at shared
events (verified: both sequences begin at 2 for the period-start marker).

**Key detail — pbpstats event ordering:** pbpstats orders events by *game-clock
time*, not by `event_num`. At simultaneous dead-ball windows a substitution
(e.g. `event_num=67`) may be applied *before* a foul that carries a lower
`event_num` (e.g. `event_num=64`) when both occur at the same clock tick. This
is correct behavior — pbpstats resolves the true game state at each clock time.
The `lineups_expected.parquet` `action_number` column stores the pbpstats
`event_num` value, NOT a v3-ordered rank. Tasks 2/3 must join on
`action_number` using the pbpstats ordering.

## Cross-check

The NBA officially retired the `playbyplayv2` endpoint in late 2023 (returns
empty JSON — nba_api issue #591). `nba_on_court` (v2-based) cannot be run as
a live cross-check.

Two independent checks were performed instead:

**Part A — Starter agreement:** The period-1 pbpstats starters exactly match
the `boxScoreTraditional` starters (non-empty `position` field):

- Home (Celtics): `[201143, 203935, 1627759, 1628369, 1628401]`
  (Horford, Smart, Brown, Tatum, White)
- Away (76ers): `[200782, 201935, 202699, 203954, 1630178]`
  (Tucker, Harden, Harris, Embiid, Maxey)

**Part B — Internal consistency:** All 452 rows in `lineups_expected.parquet`
have exactly 5 distinct home player IDs and 5 distinct away player IDs, and
every ID belongs to the `boxScoreTraditional` roster. Zero violations.

## lineups_expected schema

```
game_id:        Utf8   — always "0022200001"
action_number:  Int64  — pbpstats event_num (== v3 actionNumber at shared events)
period:         Int64  — game period (1–4; OT periods are 5+)
home_player_1:  Int64  — Celtics on-court player id (sorted ascending)
home_player_2:  Int64
home_player_3:  Int64
home_player_4:  Int64
home_player_5:  Int64
away_player_1:  Int64  — 76ers on-court player id (sorted ascending)
away_player_2:  Int64
away_player_3:  Int64
away_player_4:  Int64
away_player_5:  Int64
```

## enhanced_pbp_expected schema

```
action_number:  Int64    — v3 actionNumber
period:         Int64    — game period
clock:          Utf8     — ISO-8601 duration (e.g. "PT08M24.00S")
clock_seconds:  Float64  — seconds remaining in period
team_id:        Int64    — team involved (0 for period/neutral events)
person_id:      Int64    — player involved (0 for team events)
action_type:    Utf8     — v3 actionType string
sub_type:       Utf8     — v3 subType string (may be empty)
event_type:     Int64    — integer code (see ACTION_TYPE_MAP in generator)
is_substitution: Boolean — actionType == "Substitution"
order_index:    Int64    — 0-based stable rank of action_number within game
description:    Utf8     — v3 description string
score_home:     Utf8     — running home score at event (may be empty string)
score_away:     Utf8     — running away score at event (may be empty string)
```

## Regeneration

```sh
cd <worktree>
uv run --with pbpstats python tools/fixtures/gen_nba_engine_fixtures.py 0022200001
```

Requires a residential IP (or VPN to residential) for `stats.nba.com` curl_cffi
calls. The S3 fetch (pbpstats) has no IP restriction.
