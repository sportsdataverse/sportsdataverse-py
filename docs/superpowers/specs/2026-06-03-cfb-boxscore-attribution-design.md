<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [CFB Advanced Box Score — Attribution Refactor (Design)](#cfb-advanced-box-score--attribution-refactor-design)
  - [1. Problem](#1-problem)
    - [Confirmed root causes (line refs in `cfb_pbp.py`)](#confirmed-root-causes-line-refs-in-cfb_pbppy)
    - [Empirically verified during design (game 401628334)](#empirically-verified-during-design-game-401628334)
    - [Verification fixture — game 401754598 (NC State 152 vs FSU 52)](#verification-fixture--game-401754598-nc-state-152-vs-fsu-52)
  - [2. Decisions (locked)](#2-decisions-locked)
  - [3. Goals / Non-goals](#3-goals--non-goals)
  - [4. Architecture & data flow](#4-architecture--data-flow)
  - [5. The attribution layer (`__add_attribution_cols`)](#5-the-attribution-layer-__add_attribution_cols)
    - [5.1 Special-teams team resolution (verified flip)](#51-special-teams-team-resolution-verified-flip)
    - [5.2 Role → credited team](#52-role-%E2%86%92-credited-team)
    - [5.3 New per-play columns](#53-new-per-play-columns)
    - [5.4 Team-abbreviation resolution (attribution ground truth)](#54-team-abbreviation-resolution-attribution-ground-truth)
  - [6. Turnover detection (scrimmage gate removed, text-driven)](#6-turnover-detection-scrimmage-gate-removed-text-driven)
  - [7. Penalty attribution](#7-penalty-attribution)
  - [8. Identity via participants (auto + fallback)](#8-identity-via-participants-auto--fallback)
  - [9. Output schema additions (additive only)](#9-output-schema-additions-additive-only)
  - [10. Testing & reconciliation invariants](#10-testing--reconciliation-invariants)
  - [11. Risks / back-compat](#11-risks--back-compat)
  - [12. Open items for the implementation plan](#12-open-items-for-the-implementation-plan)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# CFB Advanced Box Score — Attribution Refactor (Design)

- **Date:** 2026-06-03
- **Component:** `sportsdataverse/cfb/cfb_pbp.py` (`CFBPlayProcess.create_box_score` + pipeline)
- **Status:** Approved design — ready for implementation plan
- **Author:** Saiem Gilani (with Claude)

## 1. Problem

`create_box_score` builds the per-team / per-player advanced box score by grouping
plays on `pos_team` or `def_pos_team`. Those two columns **change meaning between
play types**, so any stat whose true owning team does not match the play's fixed
`pos_team` role is attributed to the wrong team — or dropped entirely. The user
reports the box "isn't accounting for all turnovers / yardage / INTs / fumbles /
fumbles lost/recovered / sacks / penalties, or attributes them to the wrong team,"
and that **most misses occur on special teams**.

### Confirmed root causes (line refs in `cfb_pbp.py`)

| # | Severity | Finding | Location |
|---|---|---|---|
| 1 | 🔴 | Turnover/forced-fumble boxes filter `scrimmage_play == True`, dropping **all** special-teams turnovers (muffed punts, kickoff fumbles, blocked-kick recoveries). This is the primary "special teams miss." | `:4702`, `:4645` |
| 2 | 🔴 | Punt-return players + yards credited to the **punting** team. `punt_return_player_name` grouped by `pos_team`, but on a punt `pos_team`=punting team; returner is on `def_pos_team`. (Kickoffs are correct because `pos_team`=receiving team on kickoffs.) | `:4815` |
| 3 | 🔴 | Fumble recoveries always credited to `def_pos_team`, even **own-team** recoveries (which belong to `pos_team`). | `:4794` |
| 4 | 🔴 | Penalty yards charged to `pos_team` regardless of which team committed the foul; uses `statYardage` not `yds_penalty`; declined/offsetting not zeroed. | `:4216` |
| 5 | 🔴 | Turnover home/away decided by **group-by row order** (`turnover_box_json[0]`=away, `[1]`=home) — unordered `group_by`, so margins/luck can be sign-flipped & team-swapped. | `:4718-4756` |
| 6 | 🟠 | `team_base_box.total_yards` sums `statYardage` over **all** plays (kickoffs/punts/penalties/returns) by `pos_team` — not offensive yards. | `:4205` |
| 7 | 🟠 | `pass_yards + rush_yards` never reconcile to `off_yards`: splits use regex-parsed yards (null→0 on parse miss, sacks excluded) while team total uses `statYardage`. | `:4274`/`:4295` vs `:4242` |
| 8 | 🟠 | `team_sp_box` mixes punting-team punts with receiving-team kickoffs (the `pos_team` flip). | `:4250` |
| 9 | 🟠 | `fumble_lost`/`fumble_recovered` keyed on next-play `change_of_pos_team` mislabels 4th-down turnovers, period boundaries, multi-fumble plays. | `:2275-2280` |
| 10 | 🟡 | `turnover_box.pass_breakups` (by `pos_team`) vs `def_box.pass_breakups` (by `def_pos_team`) — same name, opposite team. | `:4705` vs `:4673` |
| 11 | 🟡 | `turnover_box.fumbles_recovered` means own recoveries, easily mistaken for takeaways. | `:4707` |
| 12 | 🔵 | Yardage regex coverage: unmatched phrasings → null → `fill_null(0)` dilutes YPA/YPC; no `statYardage` fallback. Also `yds_penalty` parse leaves garbage prefixes (`') 4'`, `'n 5'`, `'  5'`, `'1'`). | `:2286-2434`, `:1991`/`:2436` |
| 13 | 🔵 | Multi-event plays: only `sack_player1/2` and a single `fumble_recovered_player` captured; split sacks / laterals dropped. Punt-return player extraction misses returns ("T.Anderson return 1 yard" → null). | `:2636`, `:2877`, `:2696` |
| 14 | 🔴 | **Muffs not detected as fumbles.** `fumble_vec` matches the literal word "fumble", so `"muffed by …"` (muffed punt/kick) is never a fumble and never a turnover — the single clearest special-teams miss. | `:1068` |
| 15 | 🔴 | **Fumbling team ≠ `pos_team`.** On a punt/kick-return fumble, the fumbler is on the returning team but `pos_team` is the kicking/other team, so `total_fumbles`/`fumbles_recovered` are charged to the wrong team. | `:4706-4708` |
| 16 | 🔴 | **`change_of_pos_team` / `end.pos_team.id` are unreliable for turnover detection.** Verified on plays where possession columns disagree with the text recovery clause. Must not be the primary signal. | `:2229-2280` |
| 17 | 🔴 | **Overturned / reviewed plays are parsed naively.** Text like `"CALL OVERTURNED. (Original Play: … fumble by … recovered by FSU …)"` describes a *negated* play; the fumble/recovery clause inside the `(Original Play: …)` parenthetical must be stripped before any fumble/turnover parsing, or a reversed fumble is counted as a real turnover. | new |

### Empirically verified during design (game 401628334)

- **Kickoff:** `pos_team` = receiving team (`pos_unit="Kickoff Return"`).
- **Punt:** `pos_team` = punting team (`pos_unit="Punt Offense"`).
- A punt-return row showed returner *Zavion Thomas* on `pos_team=30, def_pos_team=99`,
  while his kickoff-return row placed him on `pos_team=99` — proving punt returns file
  under the punting team. (Bug #2.)
- **Environment hazard:** the installed `sportsdataverse` in site-packages is a *stale*
  build (767-line `create_box_score`, no `specialists`/`defensive_players`). The working
  tree (this repo) is the current code. Fixes take effect only after reinstalling the
  editable package; verification must run against the working tree (e.g. `PYTHONPATH` or
  `uv run`).

### Verification fixture — game 401754598 (NC State 152 vs FSU 52)

User-supplied fixture. Concretely reproduces the core bugs (all confirmed live):

- **Muffed punt invisible (#14) → real FSU turnover missing.** Full text:
  `"… punt 25 yards to the FSU35 muffed by #24 K.Kirkland at FSU35 recovered by NCSU #98
  C.Noonkester at NCSU40"`. FSU's returner muffed; **NC State recovered**. `fumble_vec=False`
  ("muffed") and `scrimmage_play=False` → entirely absent. Should be FSU fumble lost / NC
  State takeaway.
- **Punt-return fumble charged to wrong team (#15) → real FSU turnover missing.** Full text:
  `"… #4 S.White return 2 yards to the FSU14 fumbled by #4 S.White at FSU14 recovered by NCSU
  #4 T.Thomas at FSU16"`. FSU's S.White fumbled; **NC State recovered**. `pos_team=152`, so
  it is mis-charged to NC State's `total_fumbles`/`fumbles_recovered` and not counted as an
  FSU turnover.
- **Overturned strip-sack NOT a turnover (#16/#17) — current code accidentally right.** Full
  text: `"#11 C.Bailey sacked … CALL OVERTURNED. (Original Play: … fumble by #11 C.Bailey
  recovered by FSU #40 A.Williams …)"`. The fumble was **reversed on review** — no turnover.
  The current code reports no turnover here (for the wrong reason: `end.pos_team.id=152`). A
  naive text parser that reads the `(Original Play: … recovered by FSU …)` clause would
  *invent* an NC State turnover — hence the overturned-stripping requirement (#17).
- **Defensive PI charged to offense (#4):** `Pass Interference` rows on
  `"C.Bailey (NCSU) pass incomplete"` carry `pos_team=152` — DPI by FSU charged to NC State.
- **`yds_penalty` garbage (#12):** parsed values `') 4'`, `'n 5'`, `'  5'`, `'1'`.
- **Reliable attribution signal:** recovery/fumble clauses carry the **team abbreviation**
  (`"recovered by NCSU #4 T.Thomas"`); `homeTeamAbbrev`/`awayTeamAbbrev` columns map abbrev→id.
  This is ground truth — possession columns are not.

**Current (buggy) turnover box:** NC State 152 → `turnovers=0`; FSU 52 → `turnovers=1`
(INT only). **Expected after fix:** NC State `turnovers=0` (the only NC State fumble was
overturned — value unchanged but now for the right reason); **FSU `turnovers≈3`** = 1 INT +
2 ST fumbles lost (the K.Kirkland muff and the S.White punt-return fumble, both recovered by
NC State). The fix's headline effect here is **+2 FSU special-teams turnovers** that are
currently dropped.

## 2. Decisions (locked)

| Decision | Choice |
|---|---|
| Output schema | **Additive only** — keep all existing keys/fields & meanings; correct wrong *values* in place; add new fields/sections; never rename/remove. |
| Participants join | **Auto-fetch + graceful fallback** — fetch `espn_cfb_play_participants` once per game for identity + role types; fall back to existing regex when missing/offline. |
| Architecture | **Approach A** — a per-play attribution-column layer (`__add_attribution_cols`); `create_box_score` groups by resolved columns. |
| ST turnover rule | **Count ST fumbles/muffs lost into the main turnover totals** (feed margin/luck), charged to the **fumbling team** (the receiving team on a muffed punt/kick), **and** also expose `st_turnovers_*` fields separately. |
| Verification | **Fixture set (frozen offline):** `401754598` (punt muff + punt-return fumble lost + overturned strip-sack + DPI + INT); `401309854` ASU@BYU and `401112081` BAY@TCU (kickoff-return fumble lost, kicking team recovers); `401135269` BYU@Hawaii (kickoff-return **own** recovery → tests #3); `401032062` WMU@BYU (punt-return **own** recovery → tests the false-`fumble_lost` regression trap, §6). |

## 3. Goals / Non-goals

**Goals**

- Every turnover (scrimmage **and** special teams) counted and charged to the correct team.
- Punt/kick returns, sacks, INTs, forced fumbles, fumble recoveries, and penalties attributed to the correct team and (where applicable) player.
- Turnover margin/luck computed by team identity, not list index.
- Reconcilable team yardage (pass+rush+sack ≈ offense total).
- Player identity robust to split sacks / laterals / parse misses via participants.

**Non-goals**

- No change to EPA/WPA/QBR modeling.
- No restructuring of the play-by-play schema beyond added columns.
- No new public API surface beyond what auto-fetch requires internally.
- Not migrating to a full event-stream model (Approach C) in this pass.

## 4. Architecture & data flow

```
run_processing_pipeline:
  … __add_yardage_cols → __add_player_cols
    → __add_attribution_cols          # NEW — pure, resolves credited-team per play
    → (participants identity join)    # NEW — auto-fetch + regex fallback
    → __after_cols → … → create_box_score   # grouping rewired to resolved columns
```

- `__add_attribution_cols(play_df)` is **pure/deterministic** (no I/O). Inputs: `pos_team`,
  `def_pos_team`, play-type flags (`kickoff_play`, `punt`, `fg_attempt`, `sp`,
  `scrimmage_play`), `fumble_vec`, `int`, `penalty_detail`, `end.pos_team.id`,
  `yds_penalty`. Outputs: the resolved columns in §5.
- Participants identity join is independent and **never** affects team attribution.
- `create_box_score`: group by resolved columns; remove `scrimmage_play` gate from
  turnover & forced-fumble counting; recompute turnover section by identity.

## 5. The attribution layer (`__add_attribution_cols`)

### 5.1 Special-teams team resolution (verified flip)

| Play type | `kicking_team` | `return_team` |
|---|---|---|
| kickoff (`kickoff_play`) | `def_pos_team` | `pos_team` |
| punt (`punt`) | `pos_team` | `def_pos_team` |
| field goal (`fg_attempt`) | `pos_team` | `def_pos_team` |
| scrimmage | null | null |

### 5.2 Role → credited team

| Role / event | Credited team |
|---|---|
| passer, rusher, receiver, target, completion, pass_td, rush_td | `pos_team` |
| sack (defender), pass-breakup, interception (made), forced fumble | `def_pos_team` |
| interception **thrown** (giveaway) | `pos_team` |
| kicker (FG), punter | `kicking_team` |
| kick returner, punt returner | `return_team` |

### 5.3 New per-play columns

- `kicking_team`, `return_team`
- `fumbling_team` (team that had the ball when the fumble/muff occurred — parsed, not
  assumed from `pos_team`), `recovery_team` (team that recovered — parsed from the
  `recovered by {ABBR} #` clause)
- `turnover_team` (team that lost the ball), `is_turnover` (bool), `is_st_turnover` (bool)
- `penalized_team`, `penalty_yards_signed` (= cleaned `yds_penalty`, 0 for declined/offsetting)
- event-team columns consumed by the player boxes:
  `sack_team`(=def), `interception_team`(=def), `pass_breakup_team`(=def),
  `forced_fumble_team`(=def), `fumble_recovery_team` (= `recovery_team`),
  `punt_return_team`(=`return_team`), `kick_return_team`(=`return_team`),
  `fg_team`/`punt_team`(=`kicking_team`).

### 5.4 Team-abbreviation resolution (attribution ground truth)

The fumble/recovery clauses carry the team abbreviation (`"recovered by NCSU #4 T.Thomas"`,
`"FSU #40 A.Williams"`). `__add_attribution_cols` parses the leading `{ABBR}` token from the
recovered-by / fumbled-by clauses and maps it to a team id via the existing per-play
`homeTeamAbbrev` / `awayTeamAbbrev` columns (→ `homeTeamId` / `awayTeamId`). This
abbreviation match — corroborated by the participants `recoveredBy`/`fumbledBy` athlete when
present — is the **primary** attribution signal. `change_of_pos_team` / `end.pos_team.id`
are used only as a last-resort fallback when no abbreviation/participant is parseable (they
are demonstrably wrong on some plays — finding #16). For special-teams muffs/returns where
only the recoverer's abbreviation is present, the fumbling team is the *other* of
{`kicking_team`, `return_team`} (a punt/kick has exactly two teams).

## 6. Turnover detection (scrimmage gate removed, text-driven)

Detection is driven by **what the text says happened**, not by the possession columns.

**Step 0a — strip overturned/reviewed clauses (finding #17).** Before any fumble parsing,
remove the negated portion of reviewed plays: drop the `(Original Play: …)` parenthetical
and treat `CALL OVERTURNED` / `ruled … REVERSED` text so a reversed fumble/recovery is not
parsed. Operate on a cleaned copy of `text` used only for turnover parsing.

**Step 0b — widen fumble detection.** `fumble_vec` (or a new `fumble_or_muff` flag) must also
match `"muff"`/`"muffed"` (finding #14) so muffed punts/kicks are seen as ball-on-ground
events.

**Step 1 — resolve `fumbling_team` and `recovery_team`** from the clause abbreviations
(§5.4): the fumbling/muffing player's team and the recovering player's team. Participants
corroborate; possession diff is fallback only.

**Step 2 — classify each play:**

1. **Interception:** `int == True` → `turnover_team = pos_team` (thrower), gained by `def_pos_team`.
2. **Fumble/muff lost:** ball-on-ground event with `recovery_team != fumbling_team`
   → `turnover_team = fumbling_team`, gained by `recovery_team`. `is_st_turnover = sp`.
   This single rule covers scrimmage strip-sacks, punt/kick-return fumbles, and muffed
   punts/kicks uniformly, regardless of `pos_team`'s role on the play.

`fumble_lost`/`fumble_recovered` are redefined from `fumbling_team` vs `recovery_team`
(not the next-play `change_of_pos_team` diff). `total_fumbles` is charged to `fumbling_team`,
not `pos_team`. Multi-fumble plays use the participants `recoveredBy`/`fumbledBy` order; the
final recovery determines `recovery_team`.

**Counting:** all fumble/muff losses (scrimmage **and** ST) flow into the main `turnovers` /
`turnover_margin` / `turnover_luck`, charged to `turnover_team`, **and** into separate
`st_turnovers_lost` / `st_turnovers_gained` fields (where `is_st_turnover`).

> ⚠️ **Regression guard (verified on 401032062).** On a punt, `pos_team` = punting team and
> the *normal* punt possession change sets `change_of_pos_team=True`, so a punt-return fumble
> the **receiving team recovers itself** still carries a spurious `fumble_lost=True`. Removing
> the `scrimmage_play` gate (#1) **without** the text-driven `recovery_team != fumbling_team`
> check would surface this as a phantom turnover charged to the punting team. The §6 ordering
> (text recovery team is authoritative; possession flags are fallback only) is what prevents
> this — it is a hard requirement, not an optimization.

## 7. Penalty attribution

- `penalized_team`: parse `PENALTY on {TEAM}` from text; reconcile with the
  offensive/defensive sense already in `penalty_detail` (Defensive Holding/PI/Offside/
  Roughing/12-men → `def_pos_team`; offensive fouls → `pos_team`).
- Team penalty yards = Σ `penalty_yards_signed` charged to `penalized_team`;
  declined/offsetting = 0.
- Keep existing `total_pen_yards` field (document it); add correctly-attributed
  `penalty_yards` (additive).

## 8. Identity via participants (auto + fallback)

- `run_processing_pipeline` calls `espn_cfb_play_participants(game_id)` once, joins on
  `play_id`. Name/id columns prefer participants; fall back to regex columns when a name is
  missing or the fetch fails (offline/disk mode). Failure is logged, not raised.
- List columns (`sack_player_names`, …) let `defensive_players` credit **both** sackers on
  split sacks and capture lateral returners (closes #13).
- Participants carry **no team** per athlete; team always comes from §5 (role → team).

## 9. Output schema additions (additive only)

- **`turnover[]`**: keep all fields, correct values; **add** `team_id`; recompute
  `expected_turnover_margin` / `turnover_margin` / `turnover_luck` **by team identity**
  (never list index); **add** `st_turnovers_lost`, `st_turnovers_gained`.
- **`defensive_players[]` / `specialists[]`**: fix team column (punt returners → returning
  team; own-fumble recoveries → offense); keep field names; multi-player rows via list
  columns.
- **`team[]`**: **add** `penalty_yards` (correctly attributed) beside existing
  `total_pen_yards`; reconcile pass/rush/sack yardage to `off_yards` via `statYardage`
  fallback when parsed yardage is null.

## 10. Testing & reconciliation invariants

Primary offline fixture: **game 401754598** (must include ST fumble, muff, INT, penalty).
Asserted invariants:

1. team `turnovers` == Σ(player INTs thrown + fumbles/muffs lost), **including ST**.
2. `pass_yards + rush_yards + sack_yards ≈ off_yards` (± rounding).
3. punt-return yards land on the returning team.
4. penalty yards land on the penalized team.
5. `turnover_margin` is exactly antisymmetric between the two teams.
6. `__add_attribution_cols` unit tests on synthetic plays (each play type × each event).

**Fixture-specific golden assertions (401754598):**

- NC State (152): `turnovers == 0` — the only NC State fumble (C.Bailey strip-sack) was
  **overturned on review** and must not be counted (validates #17).
- FSU (52): `turnovers == 3` = 1 INT thrown + 2 ST fumbles lost — the K.Kirkland muff and the
  S.White punt-return fumble, **both recovered by NC State** (validates #14, #15, #1).
- `st_turnovers_lost[FSU] == 2`; `st_turnovers_gained[NCSU] == 2`.
- No fumble is charged to a team whose player did not fumble (the S.White punt-return fumble
  must not land on NC State's `total_fumbles`).
- The DPI on C.Bailey incompletions is charged to FSU, not NC State.

**Cross-fixture golden assertions:**

- `401309854` (ASU@BYU): ASU's kickoff-return fumble lost counts as an ASU turnover / BYU
  takeaway (currently dropped → ASU `turnovers` rises from 2 to 3).
- `401112081` (BAY@TCU): Baylor's kickoff-return fumble lost counts as a Baylor turnover.
- `401135269` (BYU@Hawaii): BYU's kickoff-return **own** recovery is **not** a turnover, and
  the recovery is credited to **BYU** (receiving = `pos_team`), not Hawaii (validates #3).
- `401032062` (WMU@BYU): BYU's punt-return **own** recovery is **not** a turnover and is
  **not** charged to WMich as a fumble lost (validates the §6 regression guard).

## 11. Risks / back-compat

- Output is additive; existing consumers keep working; wrong values become right.
- **Rollout note:** stale site-packages build must be reinstalled for fixes to take effect
  in the live app; verification runs against the working tree.
- Auto-fetch adds one ESPN call per game; mitigated by graceful fallback and the existing
  download cache. Disk/offline mode unaffected (falls back to regex).

## 12. Open items for the implementation plan

- **Team-abbreviation → team-id map** (§5.4): use the existing `homeTeamAbbrev` /
  `awayTeamAbbrev` per-play columns. Handle abbrev variants defensively.
- Exact `PENALTY on {TEAM}` parsing (team name vs abbrev vs player) and reconciliation with
  `penalty_detail` sides; fix the `yds_penalty` regex garbage (`') 4'` → `4`).
- Precise multi-fumble ordering using participants vs text; final recovery wins.
- Overturned-clause stripping (§6 Step 0a): finalize the regex set for `(Original Play: …)`,
  `CALL OVERTURNED`, `REVERSED`, and "ruled down by contact" so reversed events are dropped.
- Whether `total_yards` (#6) is gated to scrimmage or renamed-and-kept (decide in plan;
  default: keep value, add `off_total_yards` scrimmage-gated field — additive).
- Widen `fumble_vec` vs introduce a separate `fumble_or_muff` flag — decide whether muffs
  should also flow into existing `fumble_vec`-based aggregations (player `Fum` columns) or
  only into turnover detection. Default: separate flag for turnover detection to avoid
  inflating QB/RB fumble counts with muffs.

> Resolved during design (no longer open): the K.Kirkland muff in 401754598 was recovered by
> NC State (so it **is** an FSU turnover); abbrev→id source columns confirmed to exist.
