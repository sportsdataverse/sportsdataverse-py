<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [CFB fixtures](#cfb-fixtures)
  - [Sources](#sources)
  - [Games the whole-game oracle leans on](#games-the-whole-game-oracle-leans-on)
  - [cfbfastR R kicker oracle (`cfbfastr_r_opening_kickoff.csv`)](#cfbfastr-r-kicker-oracle-cfbfastr_r_opening_kickoffcsv)
  - [Overtime rule sets (C-M6)](#overtime-rule-sets-c-m6)
  - [cfb4th oracle slice (`cfb4th_2023_slice.csv`, task 1.5)](#cfb4th-oracle-slice-cfb4th_2023_slicecsv-task-15)
  - [Other files here](#other-files-here)
  - [Captures](#captures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# CFB fixtures

Provenance for the ESPN college-football captures under this directory (quality program task 1.2,
finding C-L14). Every `summary_<game_id>*` file is a real ESPN summary payload for one game; nothing
here is synthetic and nothing is a stored copy of the processor's output.

## Sources

The source URL of every summary is
`https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event=<game_id>`.
Three forms are committed (the **form** column below):

- **trimmed** -- `summary_<id>_trimmed.json.gz`: copied from `cfbfastR-cfb-raw/cfb/json/raw/<id>.json`
  (the scraper's store of that same endpoint) and reduced to `boxscore`, `drives`, `gameInfo`,
  `header`, `pickcenter` and `scoringPlays`, the keys the processor reads (see `_trimmed` in
  `tests/cfb/test_cfb_pbp_offline.py`). The whole game is kept.
- **raw allowlist** -- `summary_<id>.json`: `CFBPlayProcess(gameId=<id>, raw=True).espn_cfb_pbp()`,
  the endpoint's payload filtered to the processor's key allowlist (`tools/capture_cfb_fixtures.py`).
- **full summary** -- `summary_<id>.json`: the endpoint's payload as returned (it also carries keys
  such as `meta`, `news` or `wallclockAvailable`).

The capture date of the older files was not recorded; the **first committed** column (commit, date,
PR) is the latest date the payload can be from. `summary_401112489_trimmed.json.gz` was captured for
task 1.2 on 2026-10-11 (10:05 UTC) with
`CFBPlayProcess(gameId=401112489, raw=True).espn_cfb_pbp()`, one request, and trimmed as above; its
play ids match `cfbfastR-cfb-raw/cfb/json/raw/401112489.json` exactly (236 plays).

## Games the whole-game oracle leans on

- **400547866** (Kansas @ West Virginia, 2014) -- an away-kicked game (the home team receives) for
  C-NF-2 / C-M13: before task 2.6 the processor's row-1 value named the receiving team, so the column
  was not even a game-wide constant, and its home-kicked partners 400547699 and 400547865 were wrong
  on every row.
  `tests/cfb/test_cfb_whole_game_oracle.py` reads the opening kicker from the feed on 85 captures, 43
  of them kicked off by the home team.
- **400559176** (West Alabama @ Jacksonville State, 2014) -- the feed contradicts itself: ESPN files
  the opening kickoff under the RECEIVING team (West Alabama, 2695: "N/A kickoff for 55 yds, Daryl
  Collins return for 23 yds to the WAlab 33"), who then snaps first. Jacksonville State (55, home)
  kicked; the oracle settles it from the snap order (`_opening_kicker`). Not in cfbfastR's R tree.
- **401301042** -- the `summary_pickcenter` odds round trip (C-M19).
- **401856682** -- the Q4 Fumble Recovery (Own) with ESPN's degenerate `end` block (supp-002).

## cfbfastR R kicker oracle (`cfbfastr_r_opening_kickoff.csv`)

A second, independent opening-kicker oracle: cfbfastR's own R output, not sdv-py's. For each of the 89
captures from 2014-2026 found in `cfbfastR-dev/cfbfastR-data/data/rds/pbp_players_pos_<season>.rds`
(written by `week.R` -> `cfbfastR::cfbd_pbp_data(year, week, epa_wpa = TRUE)` on CFBD data; that tree
at commit `afd26c5`, 2026-10-11), the row holds the value of `receives_2H_kickoff` on cfbfastR's game
play 1 (1 = the home team kicked off), the home and away schools, and play 1's `play_type` and
`def_pos_team`. Extracted on 2026-10-11 with R 4.6.1:

```r
d <- readRDS(sprintf("data/rds/pbp_players_pos_%d.rds", season))
x <- d[d$game_id == game_id, ]; x <- x[order(x$game_play_number), ]
c(x$receives_2H_kickoff[1], x$home[1], x$away[1], x$play_type[1], x$def_pos_team[1])
```

The test compares it with the feed oracle where cfbfastR's play 1 is the kickoff (its rule falls back
to the first scrimmage play's defense otherwise, which CFBD's play order makes unreliable) and the
feed names the kicker: 64 of 64 agree.

## Overtime rule sets (C-M6)

One capture inside each CFB overtime rule set (sdv-internal-refs `rules/cfb.yaml`), each reaching the
overtime its rule is about; `tools/check_ot_era_captures.py` checks them.

| rule set | rule id | capture | why |
|---|---|---|---|
| <2019 | `cfb-1996-overtime-introduced` | 243040130 | Michigan State @ Michigan, 2004: three overtimes, the third forcing two-point tries; no shootout |
| 2019-20 | `cfb-2019-overtime-shootout-from-fifth` | 401112489 | North Carolina @ Virginia Tech, 2019-10-19: six overtimes, the two-point shootout from the fifth. ESPN files every overtime-1-to-5 row as period 5, the two touchdowns as period 6 and the winning try as period 10 |
| 2021+ | `cfb-2021-overtime-shootout-from-third` | 401282146 | Alabama @ Auburn, 2021: four overtimes, the shootout from the third |

The check reads ESPN's period LABEL, not an overtime count: on 401112489 ESPN files overtimes 1-5 as
period 5, so the label alone cannot tell the third overtime from the fifth. The processor's gate
(`_ot_no_touchdown_before`, table `model_vars.OT_SHOOTOUT_FROM_PERIOD`) therefore takes a try as a
shootout attempt when the season has a shootout and the row is either labelled inside it or not
preceded by a touchdown; `tests/cfb/test_cfb_overtime_eras.py` holds one test per rule set (401628439,
whose attempts are labelled 5, 9 and 12, covers 2021+ alongside 401282146). Two captures pin the table's
cuts, both trimmed from `cfbfastR-cfb-raw/cfb/json/raw/<id>.json` (committed there 2026-07-29,
`5c7cc45df3`) for task 3.2:

- **272870068** (Nevada @ Boise State, 2007, four overtimes all filed as period 5): Nevada's forced
  third-overtime try "Two-point conversion attempt, PASS FAILED." is filed before its touchdown; no
  shootout before 2019, so it is the touchdown's try.
- **401524025** (Arizona @ USC, 2023, three overtimes): "Caleb Williams Run for Two-Point Conversion"
  (period 7) follows the second-overtime touchdown; from 2021 the third overtime is a shootout.

Task 3.4 (g) adds two, trimmed the same way from `cfbfastR-cfb-raw/cfb/json/raw/<id>.json`:

- **401866418** (Jacksonville State @ Ohio, 2026, four overtimes; `89812aa7b4`): ESPN's period-6
  "End of OT." marker carries 17-34 (the first one, period 5, carries 31-31), which reaches the
  shootout attempts as a 7-point margin (the linescore has 27-27); those attempts have a null win
  probability.
- **401677180** (Pittsburgh @ Toledo, 2024, six overtimes; `af9c0c24cb`): ESPN's scores run Toledo 2
  ahead from the third overtime (42-44 where the linescore has 42-42), so the fifth overtime's first
  attempt starts on -2; a shootout period that does not open tied has a null win probability.

Other overtime captures: 252600087, 262522509, 322802005, 332570254, 400547980, 400869264,
400869817, 401112081, 401301042, 401628439 (eight overtimes), 401752854, 401754543, 401754554,
401754585, 401761665, 401762461, 401762856, 401858224.

## cfb4th oracle slice (`cfb4th_2023_slice.csv`, task 1.5)

Read by `tests/cfb/test_cfb_fourth_down.py::test_cfb4th_oracle_2023` (gap-C-124). 302 rows x 24 cols.

- **Oracle (`cfb4th_*` columns).** cfb4th 0.1.2 `add_4th_probs()` under R 4.6.1, run 2026-10-11 00:47 EDT by
  the live-local check on `cfbfastR::load_cfb_pbp(2023)` (the R-produced `cfbfastR-data` release, 254,090
  rows): fourth downs with a yard line, `spread` and `over_under` in periods 1-4 (21,851 rows),
  `set.seed(7); f[sample(nrow(f), 500), ]`, 490 scored. Script (verbatim in the check's directory,
  `cfb4th_oracle.R`):

```r
d <- cfbfastR::load_cfb_pbp(2023)
f <- d %>% filter(down == 4, !is.na(yards_to_goal), !is.na(spread), !is.na(over_under), period <= 4)
set.seed(7); f <- f[sample(nrow(f), 500), ] %>% arrange(game_id, id_play)
p <- cfb4th::add_4th_probs(f)   # -> cfb4th_oracle_2023.csv (sha256 prefix 0c3f86e9ce1534d1)
```

- **Inputs (every other column).** The sdv-py release pbp the Python surface reads,
  `sportsdataverse.cfb.load_cfb_pbp([2023])` (sportsdataverse-data `cfb_pbp` 2023 asset, read 2026-10-11),
  restricted to `id` + the `_PBP_COLS` sources of `cfb/cfb_fourth_down.py` (the release has no
  `ot_second_possession`; every row is regulation). Joined to the oracle on the play-state key
  (`game_id, period, start.TimeSecsRem, start.down, start.distance, start.yardsToEndzone`; R ids are
  float-rounded, so `id_play` cannot be the key), deduplicated on that key: 302 rows (255 of the 412
  oracle games exist in the Python 2023 asset, W-H6), 293 with a finite oracle `go_wp`.
- The R `spread` is NOT an input here: its sign is opposite ESPN's `homeTeamSpread` (substituting it
  collapses `go_wp` corr to 0.39), so the Python inputs stay native.

## Other files here

`fox/`, `series/` and `wp_overtime/` carry their own READMEs. The rest are described in the
docstrings of the tests that read them: `cbs_cfb/` (`tests/cfb/cbs_pbp/test_to_espn_summary.py`),
`yahoo_cfb/` (`tests/cfb/yahoo_pbp/test_to_espn_summary.py`, `tests/cfb/test_cfb_yahoo_ext.py`),
`participants_401856682.json` (`tests/cfb/test_cfb_box_score_denominators.py`,
`tests/cfb/test_cfb_jersey_special_teams.py`) and `fox_cfb_event_41616_data.json`
(`tests/cfb/test_cfb_pbp_fox_offline.py`). `supp002/` holds 37 trimmed captures (the `trimmed` form above,
from `cfbfastR-cfb-raw` at `98deb3f091`) for the supp-002 candidate count
(`tests/cfb/test_cfb_supp002_degenerate_end.py`); they are not `summary_*` files, so the whole-game
oracle does not pick them up.
`drive_meta/` holds 2 trimmed captures (the `trimmed` form above, from `cfbfastR-cfb-raw/cfb/json/raw/<id>.json`
at `ee755dc65a`, trimmed 2026-10-22) whose drives never carry one key: 272560120 (2007, no `start.clock`) and
400547808 (2014, no `team`). They drive `test_drive_columns_no_drive_carries_keep_the_documented_type`
(task 5.1, C-L5 / R980-1); they are not `summary_*` files at the top level, so the whole-game oracle does not pick them up.
`rush_forms/282782655_trimmed.json.gz` (Tulane @ Army, 2008) is one more trimmed capture, from
`cfbfastR-cfb-raw/cfb/json/raw/282782655.json` at `9c61a436cc`, for the directional rush-loss reader
(`test_stated_rush_yardage_is_read`, EX-1.2-a, play 282782655087); it is not a `summary_*` file either.
`stranded_copies/` holds 8 trimmed captures (the `trimmed` form above, from `cfbfastR-cfb-raw/cfb/json/raw/<id>.json`
at `19865a79e3`, trimmed 2026-10-20): 252462426, 292900218, 400548291, 401282272, 401309834, 401327913, 401524051 and
401636866. Each holds one real row that a key of the stranded-copy rule in `_drop_espn_play_copies` keeps or drops
(`test_a_stranded_copy_needs_every_key_of_its_twin`, R312-1 / R312-2). They are not `summary_*` files either.
`snapshot_copies/` holds 5 trimmed captures (the `trimmed` form above, from `cfbfastR-cfb-raw/cfb/json/raw/<id>.json`
at `5d81242edc`, trimmed 2026-10-30) for the live-feed snapshot rule `_drop_feed_snapshot_copies`: 292620166 (UTEP @
New Mexico State, 2009; drives refiled whole, 1,417 rows), 312602572 (Southeastern Louisiana @ Southern Miss, 2011;
growing prefixes, 889 rows), 400756912 (Miami @ Florida Atlantic, 2015; one completion 74 times, 1,493 rows), 401013351
(Wisconsin @ Penn State, 2018; back-to-back timeouts at 0:03, both real) and 400547745 (Louisville @ Clemson, 2014; a
copy filed ahead of the penalty that set it up). They are not `summary_*` files either: the whole-game oracle would
read their snapshot noise as the game.
`ot_timeouts/` holds 2 trimmed captures (the `trimmed` form above, from `cfbfastR-cfb-raw/cfb/json/raw/<id>.json` at
`b598a0f3c0`, trimmed 2026-11-01) for the overtime timeout windows (`tests/cfb/test_cfb_ot_timeout_windows.py`):
401754372 (California @ Virginia Tech, 2025, two overtimes; its row order files Cal's OT1 touchdown between two
Virginia Tech OT1 rows) and 401760426 (San Diego State @ New Mexico, 2025, two overtimes; New Mexico's OT2 timeout is
filed under a second-quarter drive). They are not `summary_*` files at the top level, so the whole-game oracle does not
pick them up.

## Captures

| game id | season | game | last period | form | file | first committed |
|---|---|---|---|---|---|---|
| 242972641 | 2004 | Texas Longhorns @ Texas Tech Red Raiders | 4 | trimmed | `summary_242972641_trimmed.json.gz` | `12ddb2ad28` 2026-09-22 #567 |
| 243040130 | 2004 | Michigan State Spartans @ Michigan Wolverines | 7 | raw allowlist | `summary_243040130.json` | `c578431ac2` 2026-09-17 #517 |
| 243040265 | 2004 | USC Trojans @ Washington State Cougars | 4 | trimmed | `summary_243040265_trimmed.json.gz` | `c146e82803` 2026-09-26 #585 |
| 243042579 | 2004 | Tennessee Volunteers @ South Carolina Gamecocks | 4 | raw allowlist | `summary_243042579.json` | `7c30c24442` 2026-09-17 #506 |
| 243110264 | 2004 | Arizona Wildcats @ Washington Huskies | 4 | trimmed | `summary_243110264_trimmed.json.gz` | `50d24c9978` 2026-09-26 #588 |
| 243392572 | 2004 | California Golden Bears @ Southern Miss Golden Eagles | 4 | trimmed | `summary_243392572_trimmed.json.gz` | `ffa116cd09` 2026-09-26 #586 |
| 252460252 | 2005 | Boston College Eagles @ BYU Cougars | 4 | raw allowlist | `summary_252460252.json` | `7c30c24442` 2026-09-17 #506 |
| 252532751 | 2005 | Louisiana Monroe Warhawks @ Wyoming Cowboys | 4 | raw allowlist | `summary_252532751.json` | `7c30c24442` 2026-09-17 #506 |
| 252600087 | 2005 | Michigan State Spartans @ Notre Dame Fighting Irish | 5 | raw allowlist | `summary_252600087.json` | `e9c3ca7c48` 2026-09-18 #538 |
| 252740026 | 2005 | Washington Huskies @ UCLA Bruins | 4 | full summary | `summary_252740026.json` | `5fc20535a6` 2026-08-29 |
| 262522509 | 2006 | Miami (OH) RedHawks @ Purdue Boilermakers | 5 | raw allowlist | `summary_262522509.json` | `14e1c8c10f` 2026-09-17 #526 |
| 262660245 | 2006 | Louisiana Tech Bulldogs @ Texas A&M Aggies | 4 | raw allowlist | `summary_262660245.json` | `14e1c8c10f` 2026-09-17 #526 |
| 262940151 | 2006 | SMU Mustangs @ East Carolina Pirates | 4 | full summary | `summary_262940151.json` | `bc28eda428` 2026-08-29 |
| 272650152 | 2007 | Clemson Tigers @ NC State Wolfpack | 4 | trimmed | `summary_272650152_trimmed.json.gz` | `edd88af3b2` 2026-09-26 #580 |
| 272650194 | 2007 | Northwestern Wildcats @ Ohio State Buckeyes | 4 | trimmed | `summary_272650194_trimmed.json.gz` | `99484724be` 2026-10-01 #651 |
| 272870068 | 2007 | Nevada Wolf Pack @ Boise State Broncos | 5 | trimmed | `summary_272870068_trimmed.json.gz` | task 3.2 (#792), 2026-10-11, from `cfbfastR-cfb-raw` `5c7cc45df3` |
| 282410024 | 2008 | Oregon State Beavers @ Stanford Cardinal | 4 | trimmed | `summary_282410024_trimmed.json.gz` | `50d24c9978` 2026-09-26 #588 |
| 282430151 | 2008 | Virginia Tech Hokies @ East Carolina Pirates | 4 | trimmed | `summary_282430151_trimmed.json.gz` | `ffa116cd09` 2026-09-26 #586 |
| 282432641 | 2008 | Eastern Washington Eagles @ Texas Tech Red Raiders | 4 | trimmed | `summary_282432641_trimmed.json.gz` | `ffa116cd09` 2026-09-26 #586 |
| 282570328 | 2008 | Utah Utes @ Utah State Aggies | 4 | trimmed | `summary_282570328_trimmed.json.gz` | `50d24c9978` 2026-09-26 #588 |
| 282640084 | 2008 | Ball State Cardinals @ Indiana Hoosiers | 4 | trimmed | `summary_282640084_trimmed.json.gz` | `99484724be` 2026-10-01 #651 |
| 282710130 | 2008 | Wisconsin Badgers @ Michigan Wolverines | 4 | trimmed | `summary_282710130_trimmed.json.gz` | `ffa116cd09` 2026-09-26 #586 |
| 292542649 | 2009 | Colorado Buffaloes @ Toledo Rockets | 4 | trimmed | `summary_292542649_trimmed.json.gz` | `ffa116cd09` 2026-09-26 #586 |
| 292810142 | 2009 | Nebraska Cornhuskers @ Missouri Tigers | 4 | trimmed | `summary_292810142_trimmed.json.gz` | `ffa116cd09` 2026-09-26 #586 |
| 293182305 | 2009 | Nebraska Cornhuskers @ Kansas Jayhawks | 4 | trimmed | `summary_293182305_trimmed.json.gz` | `ffa116cd09` 2026-09-26 #586 |
| 293250145 | 2009 | LSU Tigers @ Ole Miss Rebels | 4 | trimmed | `summary_293250145_trimmed.json.gz` | `50d24c9978` 2026-09-26 #588 |
| 302450154 | 2010 | Presbyterian College Blue Hose @ Wake Forest Demon Deacons | 4 | trimmed | `summary_302450154_trimmed.json.gz` | `ffa116cd09` 2026-09-26 #586 |
| 302540025 | 2010 | Colorado Buffaloes @ California Golden Bears | 4 | raw allowlist | `summary_302540025.json` | `e9c3ca7c48` 2026-09-18 #538 |
| 303102638 | 2010 | SMU Mustangs @ UTEP Miners | 4 | trimmed | `summary_303102638_trimmed.json.gz` | `88acd48d21` 2026-09-30 #643 |
| 313090025 | 2011 | Washington State Cougars @ California Golden Bears | 4 | trimmed | `summary_313090025_trimmed.json.gz` | `ffa116cd09` 2026-09-26 #586 |
| 322430041 | 2012 | Massachusetts Minutemen @ Connecticut Huskies | 4 | trimmed | `summary_322430041_trimmed.json.gz` | `c146e82803` 2026-09-26 #585 |
| 322590228 | 2012 | Furman Paladins @ Clemson Tigers | 4 | trimmed | `summary_322590228_trimmed.json.gz` | `50d24c9978` 2026-09-26 #588 |
| 322802005 | 2012 | Navy Midshipmen @ Air Force Falcons | 5 | trimmed | `summary_322802005_trimmed.json.gz` | `ffa116cd09` 2026-09-26 #586 |
| 332570254 | 2013 | Oregon State Beavers @ Utah Utes | 5 | raw allowlist | `summary_332570254.json` | `7c30c24442` 2026-09-17 #506 |
| 332852449 | 2013 | Missouri State Bears @ North Dakota State Bison | 4 | trimmed | `summary_332852449_trimmed.json.gz` | `50d24c9978` 2026-09-26 #588 |
| 332990030 | 2013 | Utah Utes @ USC Trojans | 4 | raw allowlist | `summary_332990030.json` | `c578431ac2` 2026-09-17 #517 |
| 333130023 | 2013 | San Diego State Aztecs @ San Jose State Spartans | 4 | raw allowlist | `summary_333130023.json` | `c578431ac2` 2026-09-17 #517 |
| 400547673 | 2014 | Tulane Green Wave @ Duke Blue Devils | 4 | trimmed | `summary_400547673_trimmed.json.gz` | `01d3c1ad62` 2026-09-30 #636 |
| 400547699 | 2014 | Memphis Tigers @ SMU Mustangs | 4 | trimmed | `summary_400547699_trimmed.json.gz` | `c7a7803182` 2026-10-01 #649 |
| 400547865 | 2014 | Baylor Bears @ Texas Longhorns | 4 | trimmed | `summary_400547865_trimmed.json.gz` | `190438a6b2` 2026-09-30 #641 |
| 400547866 | 2014 | Kansas Jayhawks @ West Virginia Mountaineers | 4 | trimmed | `summary_400547866_trimmed.json.gz` | `190438a6b2` 2026-09-30 #641 |
| 400547980 | 2014 | Northwestern Wildcats @ Notre Dame Fighting Irish | 5 | trimmed | `summary_400547980_trimmed.json.gz` | `c146e82803` 2026-09-26 #585 |
| 400548023 | 2014 | Rice Owls @ Texas A&M Aggies | 4 | trimmed | `summary_400548023_trimmed.json.gz` | `190438a6b2` 2026-09-30 #641 |
| 400548110 | 2014 | Western Michigan Broncos @ Idaho Vandals | 4 | trimmed | `summary_400548110_trimmed.json.gz` | cfb 3.4 R837-1, 2026-10-19, from `cfbfastR-cfb-raw` `ee755dc65a` |
| 400548134 | 2014 | Army Black Knights @ Kent State Golden Flashes | 4 | trimmed | `summary_400548134_trimmed.json.gz` | `46b754c9fb` 2026-09-30 #637 |
| 400548250 | 2014 | Utah State Aggies @ Boise State Broncos | 4 | trimmed | `summary_400548250_trimmed.json.gz` | `ffa116cd09` 2026-09-26 #586 |
| 400548425 | 2014 | Idaho Vandals @ Georgia Southern Eagles | 4 | trimmed | `summary_400548425_trimmed.json.gz` | `ffa116cd09` 2026-09-26 #586 |
| 400559176 | 2014 | West Alabama Tigers @ Jacksonville State Gamecocks | 4 | trimmed | `summary_400559176_trimmed.json.gz` | `01d3c1ad62` 2026-09-30 #636 |
| 400756970 | 2015 | Miami Hurricanes @ Duke Blue Devils | 4 | trimmed | `summary_400756970_trimmed.json.gz` | `99484724be` 2026-10-01 #651 |
| 400763571 | 2015 | Indiana Hoosiers @ Maryland Terrapins | 4 | trimmed | `summary_400763571_trimmed.json.gz` | `46b754c9fb` 2026-09-30 #637 |
| 400787459 | 2015 | Texas State Bobcats @ Georgia Southern Eagles | 4 | trimmed | `summary_400787459_trimmed.json.gz` | `46b754c9fb` 2026-09-30 #637 |
| 400869264 | 2016 | Texas State Bobcats @ Ohio Bobcats | 7 | trimmed | `summary_400869264_trimmed.json.gz` | `71a8f0863f` 2026-09-30 #639 |
| 400869270 | 2016 | Central Michigan Chippewas @ Oklahoma State Cowboys | 4 | full summary | `summary_400869270.json` | `fd65da22d9` 2026-06-30 #148 |
| 400869817 | 2016 | Nicholls Colonels @ South Alabama Jaguars | 5 | raw allowlist | `summary_400869817.json` | `c578431ac2` 2026-09-17 #517 |
| 400876049 | 2016 | Middle Tennessee Blue Raiders @ Hawai'i Rainbow Warriors | 4 | trimmed | `summary_400876049_trimmed.json.gz` | `50d24c9978` 2026-09-26 #588 |
| 400953391 | 2017 | San Diego State Aztecs @ Army Black Knights | 4 | trimmed | `summary_400953391_trimmed.json.gz` | `c921519970` 2026-09-19 #554 |
| 401012682 | 2018 | Oregon State Beavers @ Ohio State Buckeyes | 4 | raw allowlist | `summary_401012682.json` | `c578431ac2` 2026-09-17 #517 |
| 401032062 | 2018 | Western Michigan Broncos @ BYU Cougars | 4 | raw allowlist | `summary_401032062.json` | `3173503f76` 2026-06-08 #92 |
| 401110775 | 2019 | UT Martin Skyhawks @ Florida Gators | 4 | raw allowlist | `summary_401110775.json` | `c578431ac2` 2026-09-17 #517 |
| 401112081 | 2019 | Baylor Bears @ TCU Horned Frogs | 5 | raw allowlist | `summary_401112081.json` | `3173503f76` 2026-06-08 #92 |
| 401112489 | 2019 | North Carolina Tar Heels @ Virginia Tech Hokies | 10 | trimmed | `summary_401112489_trimmed.json.gz` | this PR, 2026-10-11 |
| 401135269 | 2019 | BYU Cougars @ Hawai'i Rainbow Warriors | 4 | raw allowlist | `summary_401135269.json` | `3173503f76` 2026-06-08 #92 |
| 401234597 | 2020 | North Carolina Tar Heels @ Boston College Eagles | 4 | trimmed | `summary_401234597_trimmed.json.gz` | `a43428ecf9` 2026-09-25 #577 |
| 401234617 | 2020 | Georgia Tech Yellow Jackets @ Boston College Eagles | 4 | trimmed | `summary_401234617_trimmed.json.gz` | `99484724be` 2026-10-01 #651 |
| 401236002 | 2020 | TCU Horned Frogs @ Texas Longhorns | 4 | full summary | `summary_401236002.json` | `fd65da22d9` 2026-06-30 #148 |
| 401282146 | 2021 | Alabama Crimson Tide @ Auburn Tigers | 8 | trimmed | `summary_401282146_trimmed.json.gz` | `ffa116cd09` 2026-09-26 #586 |
| 401301042 | 2021 | East Carolina Pirates @ Memphis Tigers | 5 | raw allowlist | `summary_401301042.json` | `c578431ac2` 2026-09-17 #517 |
| 401309611 | 2021 | Troy Trojans @ UL Monroe Warhawks | 4 | raw allowlist | `summary_401309611.json` | `c578431ac2` 2026-09-17 #517 |
| 401309854 | 2021 | Arizona State Sun Devils @ BYU Cougars | 4 | raw allowlist | `summary_401309854.json` | `3173503f76` 2026-06-08 #92 |
| 401403886 | 2022 | Ole Miss Rebels @ Georgia Tech Yellow Jackets | 4 | trimmed | `summary_401403886_trimmed.json.gz` | `99484724be` 2026-10-01 #651 |
| 401411109 | 2022 | Florida State Seminoles @ Louisville Cardinals | 4 | trimmed | `summary_401411109_trimmed.json.gz` | `12ddb2ad28` 2026-09-22 #567 |
| 401420836 | 2022 | Drake Bulldogs @ North Dakota State Bison | 4 | trimmed | `summary_401420836_trimmed.json.gz` | cfb 3.4 R837-1, 2026-10-19, from `cfbfastR-cfb-raw` `882a510899` |
| 401424416 | 2022 | Albany Great Danes @ Baylor Bears | 4 | full summary | `summary_401424416.json` | `a0c17d9a5e` 2026-08-29 |
| 401425418 | 2022 | Grambling Tigers @ Arkansas State Red Wolves | 4 | full summary | `summary_401425418.json` | `4b2d0b4676` 2026-08-29 |
| 401520349 | 2023 | Massachusetts Minutemen @ Army Black Knights | 4 | trimmed | `summary_401520349_trimmed.json.gz` | `d7ba21f92e` 2026-09-27 #596 |
| 401524025 | 2023 | Arizona Wildcats @ USC Trojans | 7 | trimmed | `summary_401524025_trimmed.json.gz` | task 3.2 (#792), 2026-10-11, from `cfbfastR-cfb-raw` `5c7cc45df3` |
| 401525860 | 2023 | UCF Knights @ Kansas Jayhawks | 4 | trimmed | `summary_401525860_trimmed.json.gz` | `edd88af3b2` 2026-09-26 #580 |
| 401525903 | 2023 | Kansas Jayhawks @ Cincinnati Bearcats | 4 | trimmed | `summary_401525903_trimmed.json.gz` | `50d24c9978` 2026-09-26 #588 |
| 401628334 | 2024 | USC Trojans @ LSU Tigers | 4 | full summary | `summary_401628334.json` | `fd65da22d9` 2026-06-30 #148 |
| 401628344 | 2024 | Buffalo Bulls @ Missouri Tigers | 4 | full summary | `summary_401628344.json` | `4a5dc77674` 2026-08-29 |
| 401628374 | 2024 | Georgia Bulldogs @ Alabama Crimson Tide | 4 | full summary | `summary_401628374.json` | `dbcc7837f7` 2026-08-29 |
| 401628439 | 2024 | Georgia Tech Yellow Jackets @ Georgia Bulldogs | 12 | trimmed | `summary_401628439_trimmed.json.gz` | `ffa116cd09` 2026-09-26 #586 |
| 401628455 | 2024 | Akron Zips @ Ohio State Buckeyes | 4 | full summary | `summary_401628455.json` | `c5ae9ef0f2` 2026-06-03 #91 |
| 401628559 | 2024 | Penn State Nittany Lions @ Minnesota Golden Gophers | 4 | trimmed | `summary_401628559_trimmed.json.gz` | `edd88af3b2` 2026-09-26 #580 |
| 401636889 | 2024 | Baylor Bears @ Iowa State Cyclones | 4 | full summary | `summary_401636889.json` | `825258e81b` 2026-08-29 |
| 401636929 | 2024 | Baylor Bears @ West Virginia Mountaineers | 4 | raw allowlist | `summary_401636929.json` | `7c30c24442` 2026-09-17 #506 |
| 401643775 | 2024 | North Texas Mean Green @ South Alabama Jaguars | 4 | trimmed | `summary_401643775_trimmed.json.gz` | `a69a6d063e` 2026-09-30 #631 |
| 401644749 | 2024 | Western Michigan Broncos @ Buffalo Bulls | 4 | full summary | `summary_401644749.json` | `825258e81b` 2026-08-29 |
| 401645333 | 2024 | Charlotte 49ers @ Rice Owls | 4 | raw allowlist | `summary_401645333.json` | `14e1c8c10f` 2026-09-17 #526 |
| 401677179 | 2024 | Indiana Hoosiers @ Notre Dame Fighting Irish | 4 | full summary | `summary_401677179.json` | `fd65da22d9` 2026-06-30 #148 |
| 401677180 | 2024 | Pittsburgh Panthers @ Toledo Rockets | 10 | trimmed | `summary_401677180_trimmed.json.gz` | task 3.4 (g), 2026-10-18, from `cfbfastR-cfb-raw` `af9c0c24cb` |
| 401752671 | 2025 | LSU Tigers @ Clemson Tigers | 4 | trimmed | `summary_401752671_trimmed.json.gz` | `01d3c1ad62` 2026-09-30 #636 |
| 401752684 | 2025 | South Florida Bulls @ Florida Gators | 4 | trimmed | `summary_401752684_trimmed.json.gz` | `29c209708f` 2026-09-27 #595 |
| 401752744 | 2025 | Oklahoma Sooners @ South Carolina Gamecocks | 4 | trimmed | `summary_401752744_trimmed.json.gz` | `29c209708f` 2026-09-27 #595 |
| 401752746 | 2025 | Auburn Tigers @ Arkansas Razorbacks | 4 | full summary | `summary_401752746.json` | `fe8e1077f5` 2026-08-29 |
| 401752753 | 2025 | Mississippi State Bulldogs @ Arkansas Razorbacks | 4 | full summary | `summary_401752753.json` | `fe8e1077f5` 2026-08-29 |
| 401752817 | 2025 | Western Illinois Leathernecks @ Northwestern Wildcats | 4 | trimmed | `summary_401752817_trimmed.json.gz` | `a69a6d063e` 2026-09-30 #631 |
| 401752844 | 2025 | Iowa Hawkeyes @ Rutgers Scarlet Knights | 4 | raw allowlist | `summary_401752844.json` | `7c30c24442` 2026-09-17 #506 |
| 401752854 | 2025 | Oregon Ducks @ Penn State Nittany Lions | 6 | trimmed | `summary_401752854_trimmed.json.gz` | `0dbba992e0` 2026-09-27 #597 |
| 401752914 | 2025 | Michigan Wolverines @ Maryland Terrapins | 4 | trimmed | `summary_401752914_trimmed.json.gz` | `99484724be` 2026-10-01 #651 |
| 401752946 | 2025 | Oregon State Beavers @ Washington State Cougars | 4 | trimmed | `summary_401752946_trimmed.json.gz` | `9e20c292a6` 2026-09-27 #593 |
| 401754543 | 2025 | Florida State Seminoles @ Virginia Cavaliers | 6 | full summary | `summary_401754543.json` | `fd65da22d9` 2026-06-30 #148 |
| 401754554 | 2025 | Virginia Cavaliers @ Louisville Cardinals | 5 | trimmed | `summary_401754554_trimmed.json.gz` | `408a21e9cb` 2026-09-30 #626 |
| 401754571 | 2025 | Syracuse Orange @ Georgia Tech Yellow Jackets | 4 | full summary | `summary_401754571.json` | `fd65da22d9` 2026-06-30 #148 |
| 401754579 | 2025 | Georgia Tech Yellow Jackets @ NC State Wolfpack | 4 | full summary | `summary_401754579.json` | `fd65da22d9` 2026-06-30 #148 |
| 401754585 | 2025 | California Golden Bears @ Louisville Cardinals | 5 | trimmed | `summary_401754585_trimmed.json.gz` | `408a21e9cb` 2026-09-30 #626 |
| 401754598 | 2025 | Florida State Seminoles @ NC State Wolfpack | 4 | raw allowlist | `summary_401754598.json` | `3173503f76` 2026-06-08 #92 |
| 401754623 | 2025 | Clemson Tigers @ Georgia Tech Yellow Jackets | 4 | trimmed | `summary_401754623_trimmed.json.gz` | `408a21e9cb` 2026-09-30 #626 |
| 401756930 | 2025 | Baylor Bears @ Cincinnati Bearcats | 4 | trimmed | `summary_401756930_trimmed.json.gz` | `c146e82803` 2026-09-26 #585 |
| 401756960 | 2025 | Kansas State Wildcats @ Utah Utes | 4 | trimmed | `summary_401756960_trimmed.json.gz` | `edd88af3b2` 2026-09-26 #580 |
| 401760419 | 2025 | Nevada Wolf Pack @ Wyoming Cowboys | 4 | full summary | `summary_401760419.json` | `bc28eda428` 2026-08-29 |
| 401761622 | 2025 | Southern Miss Golden Eagles @ Georgia Southern Eagles | 4 | trimmed | `summary_401761622_trimmed.json.gz` | `0dbba992e0` 2026-09-27 #597 |
| 401761656 | 2025 | Marshall Thundering Herd @ App State Mountaineers | 4 | trimmed | `summary_401761656_trimmed.json.gz` | `408a21e9cb` 2026-09-30 #626 |
| 401761657 | 2025 | Old Dominion Monarchs @ Georgia Southern Eagles | 4 | trimmed | `summary_401761657_trimmed.json.gz` | `9e20c292a6` 2026-09-27 #593 |
| 401761665 | 2025 | UL Monroe Warhawks @ Louisiana Ragin' Cajuns | 5 | trimmed | `summary_401761665_trimmed.json.gz` | `408a21e9cb` 2026-09-30 #626 |
| 401762461 | 2025 | North Texas Mean Green @ Western Michigan Broncos | 5 | trimmed | `summary_401762461_trimmed.json.gz` | `408a21e9cb` 2026-09-30 #626 |
| 401762835 | 2025 | Toledo Rockets @ Bowling Green Falcons | 4 | trimmed | `summary_401762835_trimmed.json.gz` | `29c209708f` 2026-09-27 #595 |
| 401762856 | 2025 | Kent State Golden Flashes @ Akron Zips | 5 | trimmed | `summary_401762856_trimmed.json.gz` | `408a21e9cb` 2026-09-30 #626 |
| 401762858 | 2025 | Buffalo Bulls @ Central Michigan Chippewas | 4 | raw allowlist | `summary_401762858.json` | `7c30c24442` 2026-09-17 #506 |
| 401777353 | 2025 | Indiana Hoosiers @ Ohio State Buckeyes | 4 | trimmed | `summary_401777353_trimmed.json.gz` | `408a21e9cb` 2026-09-30 #626 |
| 401778334 | 2025 | Wake Forest Demon Deacons @ Mississippi State Bulldogs | 4 | trimmed | `summary_401778334_trimmed.json.gz` | `edd88af3b2` 2026-09-26 #580 |
| 401831583 | 2025 | Arizona Wildcats @ SMU Mustangs | 4 | trimmed | `summary_401831583_trimmed.json.gz` | `ffa116cd09` 2026-09-26 #586 |
| 401856682 | 2026 | Ohio State Buckeyes @ Texas Longhorns | 4 | full summary | `summary_401856682.json` | `f9e3485176` 2026-09-15 #499 |
| 401856766 | 2026 | North Carolina Tar Heels @ TCU Horned Frogs | 4 | full summary | `summary_401856766.json` | `bc9664a898` 2026-08-29 |
| 401858213 | 2026 | Florida A&M Rattlers @ Miami Hurricanes | 4 | raw allowlist | `summary_401858213.json` | `5538e69b03` 2026-09-17 #503 |
| 401858221 | 2026 | Old Dominion Monarchs @ Virginia Tech Hokies | 4 | raw allowlist | `summary_401858221.json` | `7c30c24442` 2026-09-17 #506 |
| 401858224 | 2026 | Wake Forest Demon Deacons @ Purdue Boilermakers | 6 | raw allowlist | `summary_401858224.json` | `c578431ac2` 2026-09-17 #517 |
| 401858228 | 2026 | Mercer Bears @ Georgia Tech Yellow Jackets | 4 | trimmed | `summary_401858228_trimmed.json.gz` | `03746fc162` 2026-09-25 #571 |
| 401858426 | 2026 | Northern Illinois Huskies @ Iowa Hawkeyes | 4 | raw allowlist | `summary_401858426.json` | `7c30c24442` 2026-09-17 #506 |
| 401858435 | 2026 | Indiana State Sycamores @ Purdue Boilermakers | 4 | raw allowlist | `summary_401858435.json` | `5538e69b03` 2026-09-17 #503 |
| 401858439 | 2026 | Howard Bison @ Indiana Hoosiers | 4 | raw allowlist | `summary_401858439.json` | `5538e69b03` 2026-09-17 #503 |
| 401866418 | 2026 | Jacksonville State Gamecocks @ Ohio Bobcats | 8 | trimmed | `summary_401866418_trimmed.json.gz` | task 3.4 (g), 2026-10-18, from `cfbfastR-cfb-raw` `89812aa7b4` |
