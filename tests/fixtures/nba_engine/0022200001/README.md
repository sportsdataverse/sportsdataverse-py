<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Fixture provenance — game 0022200001](#fixture-provenance--game-0022200001)
  - [Files](#files)
  - [Capture method](#capture-method)
  - [Primary oracle — pbpstats LiveEnhancedPbp](#primary-oracle--pbpstats-liveenhancedpbp)
  - [order_index — canonical deterministic total order (Task 3 implements verbatim)](#order_index--canonical-deterministic-total-order-task-3-implements-verbatim)
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
| `lineups_expected.parquet` | pbpstats LiveEnhancedPbp (primary oracle), keyed on v3 action_number | On-court 10 per distinct v3 action; 446 rows |
| `enhanced_pbp_expected.parquet` | v3 payload (self-derived) | Enhanced play metadata; 468 rows, 44 substitutions; `order_index` = strict total order |

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

**`lineups_expected` is keyed on v3 `action_number`, not pbpstats `event_num`.**
pbpstats emits MORE events than v3 (526 vs 468 here): it splits a v3 shot's
*block*, a v3 turnover's *steal*, and a v3 `SUB: X FOR Y` into separate pbpstats
events that each carry their own `event_num` with **no** matching v3
`actionNumber`. To keep `lineups_expected` joinable onto the enhanced frame, the
on-court 10 is **forward-filled onto each distinct v3 `action_number`**: for a
given v3 action, the lineup = the pbpstats `current_players` from the latest
pbpstats event whose `event_num` <= that `action_number`. This preserves every
distinct on-court lineup (31/31 for this game) while making
`lineups_expected.action_number` a strict subset of the v3 action set. Tasks 2/3
join `lineups_expected.action_number` directly onto
`enhanced_pbp_expected.action_number`.

## order_index — canonical deterministic total order (Task 3 implements verbatim)

`enhanced_pbp_expected.order_index` is the strict total order Task 3 must
reproduce from the raw v3 payload alone. **Spec:**

```
order_index = dense 0-based rank over the sort key:
    1. period             ASCENDING    (1, 2, 3, 4, OT 5+)
    2. seconds_remaining  DESCENDING   (clock parsed to seconds; chronological)
    3. action_number      ASCENDING    (v3 logged sequence number)
    4. payload_position   ASCENDING    (0-based index of the action in the raw
                                        actions[] list — final uniqueness tiebreak)
```

Result: `order_index` is **unique, contiguous 0..467, no nulls** (verified by
in-generator assertions).

**Why `action_number` is the equal-clock tiebreak (NOT an event-type priority):**
an event-type priority was tested and **provably cannot** reproduce pbpstats'
ordering, because equal-clock type-pair orderings are *contradictory across
groups* in this game:

- `Free Throw` before `Substitution` in 11 equal-clock groups, but
  `Substitution` before `Free Throw` in 4 others.
- `Foul` before `Rebound` in 10 groups, but `Rebound` before `Foul` in 9.

No single global type priority satisfies both. Empirically, within an equal-clock
group pbpstats orders by the v3 logged `action_number` (0 same-type
disagreements). Adding any event-type priority tier *worsens* agreement with
pbpstats from 3 → 90 pairwise inversions. So `action_number` ascending is the
correct, v3-derivable equal-clock tiebreak.

The 4th tiebreak (`payload_position`) only ever breaks the rare case of two
actions sharing the SAME `action_number` (v3 does this, e.g. a Turnover and its
paired STEAL both logged under `action_number 75`); it never reorders distinct
action_numbers.

## Cross-check

The NBA officially retired the `playbyplayv2` endpoint in late 2023 (returns
empty JSON — nba_api issue #591). `nba_on_court` (v2-based) cannot be run as
a live cross-check.

Four independent checks were performed instead:

**Part A — Starter agreement:** The period-1 pbpstats starters exactly match
the `boxScoreTraditional` starters (non-empty `position` field):

- Home (Celtics): `[201143, 203935, 1627759, 1628369, 1628401]`
  (Horford, Smart, Brown, Tatum, White)
- Away (76ers): `[200782, 201935, 202699, 203954, 1630178]`
  (Tucker, Harden, Harris, Embiid, Maxey)

**Part B — Internal consistency:** All 446 rows in `lineups_expected.parquet`
have exactly 5 distinct home player IDs and 5 distinct away player IDs, and
every ID belongs to the `boxScoreTraditional` roster. Zero violations.

**Part C — order_index vs pbpstats:** For the 443 events present in both the v3
enhanced frame and the pbpstats oracle (matched `action_number == event_num`),
the canonical `order_index` induces the same relative ordering as pbpstats with
**3 pairwise inversions**, all confined to a single equal-clock cluster in
period 4 at 251.0s remaining (action numbers 601 Timeout, 604 Rebound, 605
technical Free Throw, 617 technical Foul). This cluster follows a **coach's
challenge replay overturn** (`an=602`, `Instant Replay / Coach Challenge Overturn
Ruling`); pbpstats re-sequences the affected events post-overturn
(Rebound → Foul → Timeout → FreeThrow) in a way that is **not a pure function of
the v3 payload** and that Task 3 (a v3-only consumer) also cannot and should not
reproduce. This is a documented, bounded divergence — not a fixture defect. The
canonical v3-derivable order is optimal here (any event-type priority tier raises
inversions from 3 to 90; see the order_index section above).

**Part D — Join key subset:** Every `lineups_expected.action_number` (446 rows)
appears in `enhanced_pbp_expected.action_number`, so Task 3 can join lineups onto
the enhanced frame. Zero violations.

## lineups_expected schema

```
game_id:        Utf8   — always "0022200001"
action_number:  Int64  — v3 action_number (subset of enhanced action_number); the
                         on-court 10 is the pbpstats lineup forward-filled to this action
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
order_index:    Int64    — strict total order: dense 0-based rank over
                           (period asc, seconds_remaining desc, action_number asc,
                           payload_position asc); unique + contiguous 0..467, no nulls
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
