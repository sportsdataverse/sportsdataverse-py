# Football source switch: adapter contract, dispatch, id map, parity harness

**Status:** design + skeleton (Stage 2 item 1 of the football sources program). Code under
`sportsdataverse/football/sources/` (`contract.py`, `dispatch.py`, `idmap.py`, `parity.py`), tests under
`tests/football/sources/`. Everything is underscore-prefixed / experimental: no public surface, no codegen
run, until the first adapter ships. **Adapters are NOT part of this spec's deliverable.**

**Evidence:** `background-research/2026-09-16-cfb-alt-sources/{SCORECARD,A_cfbplayprocess_contract}.md`,
`2026-09-16-nfl-alt-sources/SCORECARD.md`, `2026-09-16-nfl-shield-live/{PLAN,D_gop_nfl_consumption}.md`,
`2026-09-17-football-sources-program/LEDGER.md` ("Stage 2"). All file:line cites below are to those reports'
targets as of 2026-09-17 unless marked *(this tree)*.

## 1. Goal

`CFBPlayProcess` / `NFLPlayProcess` run unchanged on ESPN or an alternate source (NFL Shield, CBS, Yahoo, Fox,
NCAA), switchable at a moment's notice, with Game on Paper (GOP) wiring reduced to one call and ~20 lines.
Today (scorecards): the switch mechanism does not exist in sdv-py or GOP; the one shipped adapter (Fox CFB)
is numerically wrong and leaks ESPN calls with Fox ids; no source has an id map that survives an ESPN outage.

Design rule: **the processors stay ESPN-shaped.** An adapter produces an ESPN-summary-shaped dict; the
processor never learns about sources. The only ESPN-specific thing about the processors is the input shape,
and that shape is now a written, validated contract.

## 2. Adapter contract

### 2.1 Signature

```python
Adapter = Callable[[league: str, espn_id: int, ctx: SourceContext], AdaptedGame]   # dispatch.py
```

* Input: the **ESPN event id** (the only game id GOP knows) and a `SourceContext`:
  `idmap_row` (the game's pre-kickoff id-map row, §4), `payload` (the source's native payload when injected
  for offline runs; `None` = fetch), `participants`, `odds_override`.
* Output: `AdaptedGame(summary, participants=None, odds_override=None, native_ids={}, notes=[])`.
  `summary` is consumed **only** through `espn_{nfl,cfb}_pbp(summary=)` — never `proc.json = ...`
  (`cfb_pbp_fox.py:471-476` does that and thereby leaks participants/roster/odds calls with the Fox id
  and crashes without `gameInfo`; A report §0.1-2). A supplied summary sets `_offline=True` and, unless
  `participants=` is passed, forces `join_participants=False` (`nfl_pbp.py:398-402`, `cfb_pbp.py:1142-1146`
  *(this tree)*), so no network call happens inside the pipeline.
* Registration: `@_register(league, source)`; the registry is `dispatch._ADAPTERS`. ESPN is registered for
  both leagues (`payload` passthrough, or a `raw=True` fetch = one HTTP call).

### 2.2 REQUIRED fields (`contract.py`, checked by `_validate_summary`)

Levels: **required** = processor raises / zero plays; **value** = runs but output is wrong; **gop** = GOP
dereferences it unguarded (404 or blank header, processor unaffected).

| Path | Level | Why |
|---|---|---|
| `header.season.year` | required | `season` column; every era gate (touchback 75/80, XP suffix, FD bins, pre-2014 WP) |
| `header.week` | required (key; value may be null) | bracket read into the result dict |
| `header.competitions[0].playByPlaySource` | required | `"none"` skips all processing; **carries provenance** (§2.3) |
| `header.competitions[0].boxscoreSource` | required | bracket read |
| `header.competitions[0].status.type.completed` | required | corrupt check, `status_type_completed`, final-play WP pin |
| `header.competitions[0].status.type.{name,state,detail,description}` | gop | `GamePage.astro:80-86`, `GameHeader.astro:26-29,56`, `WinProbabilityChart.svelte:166`, `app.py:600-603` |
| `header.id`, `competitions[0].{date,neutralSite}` | gop | cache policy (`routes/game.ts:66` needs `date`), header |
| `competitors[]` exactly 2, `homeAway` ∈ {home, away}, **home first** | required (order = warning) | processor splits on `homeAway`; GOP assumes `[0]`=home (`GamePage.astro:74-77`) |
| `competitors[].team.id` int-castable, ≥ 0, **ESPN team id** | required | negative = TBD → `NoDataError`; logos/links/sort/Data API keyed by ESPN id (A §4b) |
| `competitors[].team.{location,abbreviation}` | required | `homeTeamName`/`NameAlt`; timeout + penalty team matching |
| `competitors[].team.name` (mascot) **non-empty** | value | `""` matches every Timeout row → every timeout charged to both teams (wp_before moved on 167/185 rows) |
| `competitors[].team.{displayName,color}`, `.score`, `.linescores[]` | gop | header, Linescore, drive chart colours |
| `drives.previous[]` (+ optional `drives.current`) | required | every key under `drives` is normalised; `current` = the open live drive |
| `drives[].id` | required | reorder gate, drive metrics, GOP drives panel join |
| `drives[].start.yardLine` | value | box `drives` section |
| `drives[].{team.shortDisplayName,displayResult,result,description}` | gop (soft) | Drives panel / Latest strip |
| `plays[].id` int-castable, **sorts into game order** | required | first sort key; dedupe; timeouts-by-id; every join |
| `plays[].sequenceNumber` | value | dedupe subset; OT ordering; late-insert reorder (CFB ≥2014) |
| `plays[].period.number`, `clock.displayValue` "MM:SS" | value | time remaining (end time = lagged start) |
| `plays[].type.text` | value | **ESPN vocabulary**, exact-string `is_in` behind every flag (A §2c) |
| `plays[].type.{id,abbreviation}` | gop | `app.py:206,208` bracket reads (key must exist; ESPN itself omits `abbreviation` on Sack / Pass Incompletion) |
| `plays[].text` | value | ~560 (CFB) / 322 (NFL) regex reads; see §2.3 grammar |
| `plays[].start.team.id` (**kicking team on kickoffs**) | value | possession; flipped via `kickoff_vec` |
| `plays[].start.{down,distance,yardLine,yardsToEndzone}` | value | `yardLine` is a hidden gate: `yardsToEndzone` is dropped when it is null |
| `plays[].end.team.id` | value | filled from next play's start when null (both processors) |
| `plays[].end.{down,distance,yardsToEndzone}` | required | `ColumnNotFoundError` |
| `plays[].end.yardLine` | value | same hidden gate as start |
| `plays[].homeScore`, `awayScore` | required | score after the play |
| `plays[].scoringPlay` | value | dedupe lead, score repair, end-of-game WP |
| `plays[].statYardage` | required | `downs_turnover`, yardage fallbacks, drive/box yards |

Optional but valuable: `scoringType.displayName` (relabels FG/XP), `pointAfterAttempt.{abbreviation,value,text}`
(XP / 2-pt result), `start.downDistanceText` ("goal"), `boxscore.players/teams` (19 `*_player_id` columns and
the `espn_team`/`espn_players` sections — supply via `participants=` instead), `pickcenter` (use
`odds_override=`).

Value rules the validator enforces: int-castable ids, clock format, play team ids ⊆ header team ids,
non-empty mascot, exactly one home + one away. Feed-order id violations and duplicate ids are **warnings**
(ESPN's own CFB feeds have late inserts in 19 of 26 committed fixtures; the processor reorders them).
`completed && plays < 50` warns: `corrupt_pbp_check()` short-circuits (`nfl_pbp.py:582-589`,
`cfb_pbp.py:1323-1330` *(this tree)*).

### 2.3 Semantics every adapter must implement (not checkable by field presence)

1. **Ids.** `gameId` = ESPN event id. Play id = `espn_event_id ‖ native_play_id` where the native id is
   monotonic (Shield `playId`, CBS NFL play id = GSIS playId → exact ESPN id parity); otherwise a
   synthesised monotonic sequence. `sequenceNumber` chronological. Drive id = `espn_event_id ‖ drive_no`.
2. **ESPN team ids everywhere** (header, `start/end.team.id`), rewritten from the id map's team table.
3. **`type.text` in ESPN vocabulary + matching `type.id`/`type.abbreviation`** from the observed map
   (A §2c, e.g. Rush 5/RUSH, Pass Reception 24/REC, Pass Incompletion 3, Sack 7, Penalty 8/PEN, Kickoff 53/K,
   Punt 52/PUNT, Field Goal Good 59/FG, Field Goal Missed 60/FGM, Passing TD 67/TD, Rushing TD 68/TD,
   Timeout 21/TO, End Period 2/EP, End of Half 65/EH, End of Game 66/EG, Pass Interception Return 26/INTR,
   Fumble Recovery (Own) 9, Fumble Recovery (Opponent) 29). The map lives with the first adapter.
4. **PAT folded into the TD row** (text + `pointAfterAttempt` + score after the PAT), never a separate row.
5. **End state from the next play across drive boundaries** (Fox adapter did it within a drive only → EPA
   off by 3.5-4.5 on drive-ending plays). Last play's end state = final.
6. **Admin / timeout / period rows** emitted with a period (Fox admin rows have none → drives filed under the
   wrong quarter), ESPN abbreviations in the timeout text ("Timeout #1 by CLE"), unique text per row (the NFL
   dedupe is on `text, id, type.text, start.down, sequenceNumber`, `nfl_pbp.py:1251-1255` *(this tree)*).
7. **Renderable header** for live games: `status.type.{name,state,detail,completed}` from the source's phase
   (Shield `summary.phase`, CBS `game_status`), scores, linescores, colours (from the team map), `date`.
8. **`drives.current`** for the open drive on live games; `previous` never repeats it (GOP bug #4).
9. **Provenance:** `header.competitions[0].playByPlaySource = "<source>"` (any value ≠ `"none"` runs; the
   processor copies it into the result). The dispatcher stamps the full provenance dict (§3.3).
10. **Odds:** alternates carry no usable ESPN-style `pickcenter`; pass `odds_override` (validated in both
    ctors; sets `odds_source="injected"`). Without it the processor defaults to 2.5 / 55.5 and drops the
    14 odds-dependent columns.
11. **Text grammar:** EPA/WP barely depend on text (team EPA/play ±0.01 with all text replaced) but player
    box sections collapse. Adapters should re-skin structured sub-events into ESPN/GSIS grammar where cheap
    and pass names/ids via `participants=` (frame with `id` + `{type}_player_name/_id` in ESPN athlete-id
    space) rather than fork the regexes. Known losses per source: §7.

### 2.4 Validator

`contract._validate_summary(summary, league) -> ContractReport` with `missing` (required), `invalid` (value
rules), `gop_missing`, `warnings`, `null_rate`; `report.ok` / `report.gop_ok`; `report.summary()` for logs.
Tests: passes on the real NFL fixture and all 26 committed CFB fixtures; a gutted copy reports the exact
paths (`tests/football/sources/test_contract.py`).

## 3. Dispatch entry (`dispatch._process_game`)

### 3.1 Signature

```python
_process_game(league, espn_id, *, source="espn", fallthrough=True, payloads=None,
              participants=None, odds_override=None, idmap_row=None) -> ProcessedGame
# ProcessedGame: game (processor dict + game["source"]), processor, plays_frame, provenance, health
```

GOP calls this instead of `_PROCESSORS[league]` + `getattr(game, fetch_name)()` + `run_processing_pipeline()`
(`app.py:337-340, 357-379`). It still gets the processor instance (`plays_frame`, `create_box_score` for spans
and the paper index; D §2.4) and the same dict, plus `game["source"]`.

### 3.2 Fall-through order and failure semantics

`SOURCE_ORDER = {"nfl": (espn, shield, cbs, yahoo, fox), "cfb": (espn, cbs, yahoo, ncaa, fox)}`.
Order tried = requested source, then the remaining alternates in canonical order, then **ESPN as the terminal
fallback** when an alternate was requested (fails fast in an outage, free otherwise). `fallthrough=False`
tries one source. A source hands over to the next on: adapter not registered (`"not implemented"`), adapter
exception (fetch failed, id unmapped — `SourceUnavailable`), **contract failure** (`report.ok` False; the
gutted summary never reaches the processor), or a processor exception. When every source fails,
`AllSourcesFailed` carries the per-source `attempts`; GOP maps it to its 404 (source-aware text, §6).

### 3.3 Provenance (`game["source"]`)

`requested`, `source`, `fallback`, `attempts[{source, ok, error, seconds}]`, `playByPlaySource`,
`native_ids`, `contract` (report summary), `lossy_columns` (`KNOWN_LOSSY[(league, source)]`), `notes`.
`ProcessedGame.health` = per-source outcome of this call (feeds GOP dq telemetry).

## 4. Pre-kickoff id map (`idmap.py`)

Built **nightly from schedules**, stored as two parquets (`games.parquet`, `teams.parquet`), read by GOP in O(1)
at request time (`_lookup(games, espn_id, teams)` → row + `home_team`/`away_team`). **Never built on ESPN at
request time**: `cfb_schedule_crosswalk` is a live ESPN-anchored name-match (≥3 calls/week, fails in the
outage it exists for, no CBS/NCAA leg, no team ids) and stays a *builder input*, not a lookup.

| games column | source of truth |
|---|---|
| `espn_event_id`, `season`, `season_type`, `week`, `kickoff_utc`, `neutral_site`, `home/away_espn_team_id` | ESPN schedule (nfl-raw crosswalk today; `espn_cfb_schedule` for CFB) |
| `shield_game_id`, `nflverse_game_id` | nfl-raw `nfl/espn/crosswalk/games.json` (exists; date/time matched) |
| `yahoo_game_id` (NFL) | **computed**: `nfl.g.{YYYYMMDD US-Eastern}{ESPN home id:03d}`; team `nfl.t.{ESPN id}` (16/16 wk1 + SB LX verified) |
| `yahoo_game_id` (CFB), `fox_game_id` | `load_cfb_schedule_crosswalk(season)` release parquet (built off-request); Fox NFL by name-join to the crosswalk (16/16) |
| `cbs_game_id` | **scraped from the CBS week scoreboard page** (`sb_<season>_<type>_<week>.html`; JAC→JAX, WAS→WSH); no API |
| `ncaa_game_id` | ncaa.com GraphQL game ids (≠ stats.ncaa.org contest ids); pre-game tiers only (date + voted team map — the score-matched tiers of `ncaa_mfb_06_xwalk_build.py` are post-game) |

Teams: `espn_team_id`, `espn_abbr`, `shield_team_id`, `nflverse_abbr`, `cbs/yahoo/fox/ncaa_team_id`. All ids
Utf8 (labels, never arithmetic). `_build_nfl_idmap()` materialises the ESPN↔Shield↔nflverse(+Yahoo) leg from
nfl-raw today; CBS/Fox/NCAA columns exist and stay null until their builders land. Where it runs: the nfl-raw
daily driver (after the crosswalk rebuild) and the cfb-raw equivalent; GOP reads the parquet from KV or the
Data API — decision for Stage 4.

## 5. Parity harness (`parity.py`)

Same game through ESPN and the alternate; compare **processed** output:

* `GOP_HARD_COLUMNS` (60): per-column agreement over paired plays (both-null = equal, floats within `tol`).
* `EPA, EP_start, EP_end, wp_before, wp_after, wpa`: Pearson r + mean |diff|.
* `advBoxScore` sections `pass, rush, receiver, team, situational, drives, defensive, turnover`: row
  equality as multisets (floats rounded to 4 dp).
* Pairing on ESPN play `id` (Shield, CBS) or `STATE_KEY = (period, clock, down, distance, yardsToEndzone)`
  (Yahoo, Fox).

**Gates are recorded from observed values and never lowered**: `report.gates()` emits the floors to pin
per `(league, source)` (JSON beside the adapter's tests), `report.check(pinned)` fails on regression only.
Starting points from the scratch runs (CLE@JAX 2026 wk1): CBS NFL EPA r .9994 / WP 1.0, Yahoo NFL EPA
r .9995 / WP 1.0, Fox NFL EPA r .999 / WP 1.000; CFB Yahoo .982/.998, CBS .939 EPA / .999 EP, NCAA .96-.985.
Tests: the ESPN fixture against itself is perfect; a perturbed copy reports exactly the perturbations
(`tests/football/sources/test_parity.py`).

## 6. Required changes outside this package (specified, not made)

### 6.1 sdv-py processors (owned by the S1 slices / later adapter PRs)

| # | Where *(this tree)* | Change | Needed by |
|---|---|---|---|
| H1 | `nfl/nfl_pbp.py:582-589`, `cfb/cfb_pbp.py:1323-1330` (+ `corrupt_pbp_check` 7068 / 8421) | make the `< 50` / `> 500` completed-game thresholds ctor kwargs (`corrupt_min_plays=50`) | sparse backup feeds (Fox NFL team-level, NCAA D2/D3) |
| H2 | `nfl/nfl_pbp.py:6855-6877, 6930-6953` (+ cleaning-pipeline twins 6984, 7059) result dicts | add `"odds_source": self.odds_source` (CFB already writes it, `cfb_pbp.py:2383, 8101`) | provenance parity NFL↔CFB |
| H3 | `nfl/nfl_pbp.py:1251-1255` dedupe subset | none if adapters keep text unique per row (§2.3.6); otherwise scope the text match to `(period, clock)` — the Fox run lost the final kneel | Fox / Yahoo NFL |
| H4 | `cfb/cfb_pbp.py:873-959` `_reorder_late_inserts` | none; gated on season ≥ 2014 and only fires on feed-order violations | — |
| H5 | `cfb/cfb_pbp_fox.py:471-476` | replace `proc.json = summary` with `espn_cfb_pbp(summary=)` and re-target through `_process_game` (C2 in LEDGER; do not publish its output meanwhile) | Fox CFB rewrite |
| H6 | both `__init__` | no `source=` argument on the processors — provenance is the dispatcher's job | — |

### 6.2 Game on Paper (D report file:line; ≈20 lines in Python)

* `python/app.py:337-340` `_PROCESSORS` → replace with `from sportsdataverse.football.sources.dispatch import
  _process_game`; `app.py:354-379` `_process_game("nfl"|"cfb", id)` → call it with
  `source=os.environ.get(f"{LEAGUE}_PBP_SOURCE", "espn")` and `idmap_row=<KV/Data API lookup>`; keep
  `participants`/`odds` behaviour by passing them through when the source is ESPN.
* `app.py:365-376` telemetry target `espn_pbp` → `game["source"]["source"]`; `app.py:531-540` 404 text
  "ESPN payload is malformed" → source-aware (`AllSourcesFailed.attempts`).
* `app.py:206,208` `_reshape_records` `type.id` / `type.abbreviation` → `.get` (defence in depth).
* `astro/src/resources/python.ts:1257` processed-response CF cache key: add `source` (or cap TTL to the live
  value when `source != "espn"`) so a degraded **final** is not cached for a year (`utils/config.ts:35-40`).
* `astro/src/routes/game.ts:46-68`: when `retrieveGamePageGuarded` returns `page=null`, still call
  `retrieveProcessedGame` and synthesise `espnGame = { gamepackageJSON: { header: game.header } }` so
  cache config (`competitions[0].date` + `status`), invalid-status, pregame and `GameRoute.astro:21-29`
  keep working. Without this, failover covers "site.api down, cdn alive" only.
* Later (PLAN Phase 7): scoreboard/schedule off ESPN; not needed for a game-page failover.

## 7. Known per-source lossy columns (scorecards; `contract.KNOWN_LOSSY`)

| League / source | Lost or degraded vs ESPN | Coverage |
|---|---|---|
| NFL Shield | nothing by construction (ESPN NFL = GSIS re-skinned; play id = `event ‖ playId`, 139/158 exact, misses = renumbered timeouts) | 1999/2002+ |
| NFL CBS | air yards on completions, YAC, CP/CPOE, shotgun, ESPN player ids; 12/15 name columns kept; replay reversals keep overturned text (keep text after "REVERSED.") | 2019+ |
| NFL Yahoo | air yards, YAC, CP, formation, fair catch, PBUs, punt returners; box pass/rush/receiver/usage row-for-row otherwise | plays 2015+, drives 2020+ |
| NFL Fox | rusher/receiver/sacker names mostly null, air yards, CP, ESPN box; admin rows have no period; team-level fallback only | 2024+ |
| CFB CBS | `yds_receiving/rushed/kickoff/punted`, receiver/kickoff/PBU/fumble names, `firstD_by_penalty`; penalty names differ ("Defensive Offside" vs "Offside") | no FCS-vs-FCS |
| CFB Yahoo | yardage breakdowns, air yards/YAC, FG/kickoff/punt-return/INT/XP/fumble names, `penalty_no_play`/`declined`; full names not `F.Last` | 0 FCS |
| CFB NCAA (ncaa.com GraphQL) | sack / FG-kicker / kickoff-return names (`Last,First` form), `yds_receiving` on classic grammar; missing `type.text` labels (Blocked FG TD, Kickoff Return (Offense), Fumble Recovery (Own), End of Half) | best FCS option; D3 seen |
| CFB Fox | receiver (always "intended"), half of passer names, `yds_punted/kickoff`, kickoff/PBU/return names, every `*_player_id`, `scoringType.*`, `pointAfterAttempt.*` | no FCS-vs-FCS |

## 8. Delivered in this PR

* `contract.py` — field lists, `KNOWN_LOSSY`, `_validate_summary` → `ContractReport`.
* `dispatch.py` — `SOURCE_ORDER`, adapter registry (ESPN registered), `_fallthrough_order`, `_process_game`,
  `ProcessedGame` with provenance + health, `SourceUnavailable` / `AllSourcesFailed`.
* `idmap.py` — `GAME_SCHEMA` / `TEAM_SCHEMA`, `_build_nfl_idmap` (nfl-raw crosswalk → Shield/nflverse/Yahoo
  legs), `_write_idmap` / `_load_idmap` (schema-asserted), `_lookup`, `_yahoo_nfl_game_id`.
* `parity.py` — `GOP_HARD_COLUMNS`, `GOP_BOX_SECTIONS`, `_compare_plays`, `_compare_box`,
  `_compare_processed`, `ParityReport.gates()/check()`.
* 61 offline tests on real fixtures (`summary_401872922.json`, 26 CFB summaries, a 17-game slice of the
  nfl-raw crosswalk); no network; two real pipeline runs per session.

Not included, by design: any Shield / CBS / Yahoo / Fox / NCAA adapter, the nightly id-map job, GOP edits,
the `type.text → type.id` map (lands with the first adapter), public exports / codegen.

## 9. Open questions (user)

1. **ESPN as terminal fallback** when an alternate is explicitly requested — keep (fails fast in an outage) or
   honour the operator's exclusion? (`_fallthrough_order` is one line either way.)
2. **Id-map delivery to GOP:** KV blob per league (Worker-readable, header fallback possible) vs Data API route
   (Postgres, joins with percentiles). Affects the nightly job's target.
3. **Odds for alternates:** `odds_override` from the nflverse/CFBD closing line stored beside the id map, or
   accept the 2.5/55.5 default on a failover page?
4. **H1 thresholds:** kwarg (default unchanged) vs bypass when `playByPlaySource != "full"`.
5. **Promotion path:** when the first adapter ships, does `_process_game` become the public
   `process_game(league, espn_id, source=)` (codegen + docs run), or stay private until GOP is wired?
