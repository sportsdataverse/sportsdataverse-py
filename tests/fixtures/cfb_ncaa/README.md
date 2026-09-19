<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [cfb_ncaa fixtures](#cfb_ncaa-fixtures)
  - [Box-score tabs (contest 5362283 — California @ Auburn, 2024-09-07)](#box-score-tabs-contest-5362283--california--auburn-2024-09-07)
  - [cfbfastR-mapper pbp fixtures (vendored from `ncaa-mfb-football-raw`)](#cfbfastr-mapper-pbp-fixtures-vendored-from-ncaa-mfb-football-raw)
  - [2025-season page variants (captured 2026-08-19)](#2025-season-page-variants-captured-2026-08-19)
  - [2019-season and 2025 text/side-code variants (bundles captured 2026-08-19 / 2026-08-21)](#2019-season-and-2025-textside-code-variants-bundles-captured-2026-08-19--2026-08-21)
  - [Parsed bundles (NC12-NC15, vendored 2026-09-19)](#parsed-bundles-nc12-nc15-vendored-2026-09-19)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# cfb_ncaa fixtures

Real `stats.ncaa.org` college-football (NCAA sport code `MFB`) play-by-play
game pages, captured via the browser transport in
`sportsdataverse/mbb/mbb_ncaa_fetch.py` (patchright + `--headless=new` + US
residential IP) from `https://stats.ncaa.org/contests/{id}/play_by_play`.

Consumed by `tests/cfb/test_cfb_ncaa_pbp.py` (offline parser tests).

| file | contest_id | source URL | captured |
|---|---|---|---|
| `cfb_ncaa_pbp_5362535.html` | 5362535 | <https://stats.ncaa.org/contests/5362535/play_by_play> | 2026-07-16 |
| `cfb_ncaa_pbp_5336803.html` | 5336803 | <https://stats.ncaa.org/contests/5336803/play_by_play> | 2026-07-16 |
| `cfb_ncaa_pbp_5361446.html` | 5361446 | <https://stats.ncaa.org/contests/5361446/play_by_play> | 2026-07-16 |

The full 6-game capture corpus lives in the `ncaa-mfb-football-raw` producer repo;
three games are vendored here to prove the parser generalizes across games
(0 unknown play types on every game).

## Box-score tabs (contest 5362283 — California @ Auburn, 2024-09-07)

Consumed by `tests/cfb/test_cfb_ncaa_box.py`. The other stats.ncaa.org tabs of one
game, for the box/drives/officials parsers in `cfb/cfb_ncaa_box.py`:

| file | tab | source URL |
|---|---|---|
| `mfb_drives_5362283.html` | Drives | <https://stats.ncaa.org/contests/5362283/drives> |
| `mfb_team_stats_5362283.html` | Team Stats (by period) | <https://stats.ncaa.org/contests/5362283/team_stats> |
| `mfb_individual_stats_5362283.html` | Individual Stats | <https://stats.ncaa.org/contests/5362283/individual_stats> |
| `mfb_officials_5362283.html` | Officials | <https://stats.ncaa.org/contests/5362283/officials> |
| `mfb_box_score_5362283.html` | Box Score (linescore + game info) | <https://stats.ncaa.org/contests/5362283/box_score> |

## cfbfastR-mapper pbp fixtures (vendored from `ncaa-mfb-football-raw`)

Consumed by `tests/cfb/test_cfb_ncaa_cfbfastr.py` (`to_cfbfastr` running-score
finals). Copied byte-for-byte from the producer repo's
`tests/fixtures/mfb_pbp_{id}.html` corpus (same browser transport); the tests
also reuse `cfb_ncaa_pbp_5361446.html` / `cfb_ncaa_pbp_5362535.html` above for
their pinned finals (Boise St. 56-45 Ga. Southern; Air Force 21-6 Merrimack).

| file | contest_id | game | why this game | source URL |
|---|---|---|---|---|
| `mfb_pbp_5336803.html` | 5336803 | Akron @ Ohio St., 2024-08-31 (52-6) | pick-six (Powers 29-yd INT return TD) + the XP after it -- pins defensive-TD scoring attribution and XP-credited-to-the-scoring-defense; lowercase `"kick attempt good"` XPs | <https://stats.ncaa.org/contests/5336803/play_by_play> |
| `mfb_pbp_5362431.html` | 5362431 | Towson @ Cincinnati, 2024-08-31 (20-38) | fumble-laden game (own-team recoveries that must NOT flip possession/score, plus forced-turnover fumbles); lowercase `"kick attempt good"` XPs | <https://stats.ncaa.org/contests/5362431/play_by_play> |

## 2025-season page variants (captured 2026-08-19)

Single tabs extracted from the `ncaa-mfb-football-raw` producer's per-game
bundles (`mfb/json/{id}.json.gz`), captured 2026-08-19 via the same browser
transport. They pin the page variants surfaced by the full 2025-season sweep
(1,685 games, 1,685/1,685 exact-final QA) that the original FBS fixtures never
exercised. Consumed by `tests/cfb/test_cfb_ncaa_pbp.py` + `test_cfb_ncaa_box.py`, and by
`test_cfb_ncaa_cfbfastr.py` (field-position invariants: rushing/receiving TD
`yards_to_goal == yards_gained`, touchback -> first snap at 75 to go).

| file | contest_id | game | variant pinned | source URL |
|---|---|---|---|---|
| `mfb_play_by_play_6386335.html` | 6386335 | Tulsa @ East Carolina, 2025-10-16 | multi-word team name + a drive title with no result token (`"East Carolina"` was truncated to `"East"`) | <https://stats.ncaa.org/contests/6386335/play_by_play> |
| `mfb_play_by_play_6386574.html` | 6386574 | Rice @ South Fla., 2025-11-29 | mixed-case yard-line side code (`Ric25`) | <https://stats.ncaa.org/contests/6386574/play_by_play> |
| `mfb_drives_6386512.html` | 6386512 | Houston @ Oregon St., 2025-09-26 (1OT) | drives tab with `1OT` quarter rows (`period` = 5) | <https://stats.ncaa.org/contests/6386512/drives> |
| `mfb_box_score_6386512.html` | 6386512 | Houston @ Oregon St., 2025-09-26 (1OT) | `scoring_summary_table` with an OT row (concatenated `tr`s, re-chunked by 9) | <https://stats.ncaa.org/contests/6386512/box_score> |
| `mfb_play_by_play_6386512.html` | 6386512 | Houston @ Oregon St., 2025-09-26 (1OT, 27-24) | completes the 1OT game's pbp + box + drives bundle, so `to_cfbfastr` runs exactly as the `-data` build calls it (drive titles, linescore, scoring summary, OT synthesis); consumed by `test_cfb_ncaa_cfbfastr.py` field-position tests | <https://stats.ncaa.org/contests/6386512/play_by_play> |

## 2019-season and 2025 text/side-code variants (bundles captured 2026-08-19 / 2026-08-21)

Single `play_by_play` tabs extracted byte-for-byte from the `ncaa-mfb-football-raw`
per-game bundles (`mfb/raw/{academic_year}/{id}.json.gz`, same browser transport).
Consumed by `tests/cfb/test_cfb_ncaa_pbp.py` and `test_cfb_ncaa_cfbfastr.py`.

| file | contest_id | game (page date) | variant pinned | source URL |
|---|---|---|---|---|
| `mfb_play_by_play_1735106.html` | 1735106 | Villanova @ Colgate (2019, 34-14) | 2019-era text: `"for loss of N yards"`, a fumble advance with a later `"for 1 yard"` clause, `"to the 50 yardline"` | <https://stats.ncaa.org/contests/1735106/play_by_play> |
| `mfb_play_by_play_6386303.html` | 6386303 | WestConn @ New Haven (2025-10-11, 0-69) | 6-letter side code (`WSTCNN25`); sacks and kneels read `"for loss of N yards"` | <https://stats.ncaa.org/contests/6386303/play_by_play> |
| `mfb_play_by_play_6396796.html` | 6396796 | Auburn @ Oklahoma (2025-09-20, 17-24) | play text writes side codes the drive headers never use (`"OU36"` for the headers' `OKL`) | <https://stats.ncaa.org/contests/6396796/play_by_play> |
| `mfb_play_by_play_6386333.html` | 6386333 | Tulane @ Tulsa (2025-09-27, 31-14) | the text's side code starts with the OTHER team's header code (`"TULANE30"` vs Tulsa `TUL`, Tulane `TLN`), so a prefix match is confidently wrong | <https://stats.ncaa.org/contests/6386333/play_by_play> |
| `mfb_play_by_play_6389205.html` | 6389205 | Minnesota @ Ohio St. (2025-10-04, 3-42) | replay reviews: `PLAY OVERTURNED. (Original Play: ... TOUCHDOWN ...)` reprints the overturned call after the ruling | <https://stats.ncaa.org/contests/6389205/play_by_play> |
| `mfb_play_by_play_1735890.html` | 1735890 | LSU @ Vanderbilt (2019-09-21, 66-38) | 2019 text: `SH,` formation prefix, `First Last` names throughout (`C. Ed.-Helaire`), tacklers glued with no separator (`Kristian FultonJaCoby Stevens`), a fumble recovered in the end zone by the defense | <https://stats.ncaa.org/contests/1735890/play_by_play> |
| `mfb_play_by_play_1735120.html` | 1735120 | Wagner @ UConn (2019-08-29, 21-24) | `LAST, First` with a space and bare initials (`McKENZIE, D`), tacklers joined by a mangled `:` (`MORGAN, D.J.3aPAUL, Keyshawn`), TD and PAT printed in one row | <https://stats.ncaa.org/contests/1735120/play_by_play> |
| `mfb_play_by_play_6386493.html` | 6386493 | Western Ky. @ LSU (2025-11-22, 10-13) | an own fumble recovery and a defensive fumble recovery for a touchdown printed with no return clause (`"... recovered by WKU Flowers,Dylan at WKU29 TOUCHDOWN"`) | <https://stats.ncaa.org/contests/6386493/play_by_play> |
| `mfb_play_by_play_6386449.html` | 6386449 | South Carolina St. @ South Carolina (2025-09-06, 10-38) | return touchdowns by the drive's defense: punt return TD, blocked punt return TD, rush fumble-return TD | <https://stats.ncaa.org/contests/6386449/play_by_play> |
| `mfb_play_by_play_6414322.html` | 6414322 | The Citadel @ Samford (2025-09-06, 40-13) | pass and rush fumble-return TDs; a punt return TD `"nullified by penalty"` | <https://stats.ncaa.org/contests/6414322/play_by_play> |
| `mfb_play_by_play_6386300.html` | 6386300 | New Haven @ Saginaw Valley (2025-11-22) | own-side vote determinism (NC9): the game whose `yards_to_goal` flipped with `PYTHONHASHSEED` between two identical season builds on main | <https://stats.ncaa.org/contests/6386300/play_by_play> |
| `mfb_play_by_play_1736435.html` | 1736435 | SFA @ Lamar University (2019-09-28, 24-17) | side code ending in a digit: `"SFA225"` is `SFA2` + 25 | <https://stats.ncaa.org/contests/1736435/play_by_play> |
| `mfb_play_by_play_1735539.html` | 1735539 | Shorter @ ETSU (2019-09-07, 10-48) | hyphenated side code `SU-ETSU` (drive titles + yard lines) | <https://stats.ncaa.org/contests/1735539/play_by_play> |

## Parsed bundles (NC12-NC15, vendored 2026-09-19)

`mfb_parsed_{contest_id}.json.gz` are the producer's own PARSED bundles, copied from
`ncaa-mfb-football-raw/mfb/json/{id}.json.gz` and trimmed to the five frames
`to_cfbfastr` reads (`pbp`, `drive_titles`, `drives`, `linescore`, `scoring_summary`;
`player_stats` / `team_stats` / `officials` dropped). That store — not a page — is what
`ncaa-mfb-football-data` compiles, its rows carry whichever `sportsdataverse` parsed them
on the sweep, and the mapper is the one stage every republish re-runs. The HTML fixtures
cannot reach these paths: today's parser already cuts the reprinted call, so only a bundle
parsed before it shows the mapper reading one.

| file | contest_id | game | defect pinned |
|---|---|---|---|
| `mfb_parsed_5361987.json.gz` | 5361987 | Boise St. @ Oregon (2024-09-07, 34-37) | NC13 — the title of the drive BEFORE a kickoff already counts the return touchdown scored on it, so the walk overshot to 40 and the next checkpoint pulled it back; both teams' tries printed in one block |
| `mfb_parsed_5366625.json.gz` | 5366625 | Princeton @ Brown (2024, 6-…) | NC13 — `"kick attempt good ... NO PLAY."` scored, then the re-kick that replaced it failed |
| `mfb_parsed_5367688.json.gz` | 5367688 | Robert Morris @ Central Conn. St. (2024, 2OT) | NC14 — a synthesized OT `Interception Return` row set `int` without `pass` |
| `mfb_parsed_5361980.json.gz` | 5361980 | Eastern Ill. @ Illinois (2024) | NC15 — `PLAY OVERTURNED. (Original Play: ... TOUCHDOWN ...)` kept the pre-review touchdown and its yardage |
| `mfb_parsed_6398950.json.gz` | 6398950 | NC State @ Wake Forest (2025, 34-24) | NC13 — a checkpoint AHEAD for one team and BEHIND for the other clamped the behind side, so a touchdown walked its scorer 6 -> 0 |
