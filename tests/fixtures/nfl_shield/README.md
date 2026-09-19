# nfl_shield fixtures

Real Shield (`api.nfl.com`) `experience/v2/gamedetails/{uuid}?includeDriveChart=true`
payload bodies, gzipped verbatim (no trimming).

| file | game | phase | provenance |
|---|---|---|---|
| `2024_01_BAL_KC.json.gz` | 2024 wk1 BAL @ KC (final, 27-20) | `FINAL` | `nfl-raw` committed library `nfl/raw/2024/2024_01_BAL_KC.json` |
| `2026_02_DET_BUF_ingame_q2.json.gz` | 2026 wk2 DET @ BUF (TNF) | `INGAME` Q2 `00:05`, offset 262, 103 plays, **open drive** (`endedPlaySequenceNumber: null`) | live capture `2026-09-18T01:49:10Z`, snapshot 110/223 of `background-research/2026-09-16-nfl-shield-live/capture/data/401872932_DET_BUF` |
| `2026_02_DET_BUF_ingame_q4.json.gz` | 2026 wk2 DET @ BUF (TNF) | `INGAME` Q4 `15:00`, 143 plays, 99 of the Q2 snapshot's 103 plays byte-identical | same capture, snapshot 152/223, `2026-09-18T02:37:00Z` |
| `2026_01_CLE_JAX.json.gz` | 2026 wk1 CLE @ JAX (final) | `FINAL` | `nfl-raw` committed library `nfl/raw/2026/2026_01_CLE_JAX.json`; pairs with the ESPN summary `tests/nfl/fixtures/summary_401872922.json` for the adapter's finals-parity test |
| `2005_01_CIN_CLE.json.gz` | 2005 wk1 CIN @ CLE (final) | `FINAL` | `nfl-raw` committed library `nfl/raw/2005/2005_01_CIN_CLE.json`; the oldest-era adapter smoke (gamebook `CLV` abbreviation, no `specialTeamsPlayType`) |
| `2026_01_TB_CIN.json.gz` | 2026 wk1 TB @ CIN (final) | `FINAL` | `nfl-raw` committed library `nfl/raw/2026/2026_01_TB_CIN.json`; carries the two shapes the finals fixture does not — a try whose preceding row is the nullified attempt's `PENALTY` (the PAT-anchoring regression) and a strip-sack touchdown, typed 80 / `SFOP` (the scoring-end-state regression) |
| `2026_02_DET_BUF_ingame_open.json.gz` | 2026 wk2 DET @ BUF (TNF) | `INGAME` Q1, offset 26, 1 drive (**still open**), 13 real plays | same capture, snapshot 13/223, `2026-09-18T00:26:50Z`; the opening-drive state — every play lives in `drives.current` and `drives.previous` is empty |
| `2026_02_DET_BUF_pregame.json.gz` | 2026 wk2 DET @ BUF (TNF) | `PREGAME`, offset 3, 1 play (`GAME_START`) | same capture, snapshot 0/223, `2026-09-18T00:00:01Z` |

The four `2026_02_DET_BUF` files are the live-path fixtures: the Q2 in-progress one exercises
open-drive possession, the held-back game-outcome columns, the current-situation row
and the provisional tail; the pregame one exercises the degenerate 1-row payload; the
Q4 one is the *later* snapshot of the same game, so the pair pins the prefix invariant
(the parser is a pure function of the payload prefix) inside CI — the 223-snapshot
sweep that measured it lives outside the repo and CI never runs it.

Shield revises a play's `playDescription` / `stats` / yardage after the snap, up to ~11
plays back, so the invariant can only be asserted for plays whose **raw payload object**
is unchanged between the two snapshots; `test_prefix_invariant_across_two_snapshots`
selects exactly those.

## Phase 5 — player/team box fixtures

| file | what | source |
|---|---|---|
| `2026_01_TB_CIN_playerstats.json.gz` | Shield `GET /football/v2/stats/live/player-statistics/a8fc106b-…` for the same 2026 wk1 TB @ CIN final | fetched with the anonymous device token; **trimmed** by dropping every stat key whose value is `0` for that player (`box._n` defaults a missing key to 0, so the trim is lossless) |
| `2026_01_TB_CIN_teamstats.json.gz` | Shield `GET /football/v2/stats/live/team-statistics/a8fc106b-…`, same game | same fetch, untrimmed (5 KB) |
| `players_crosswalk_slice.json.gz` | `gsis_id -> [espn_id, full_name]` for the 63 players in that game | the slice of `nfl_players_crosswalk()` (nflverse players master) this fixture needs, so `test_box.py` exercises the real id join with **no network read** and without the circularity of taking the ids from ESPN's own box |
| `../../nfl/fixtures/box_401872925_espn_trimmed.json.gz` | ESPN's own `boxscore` for the same game | `nfl-raw` `nfl/espn/raw/2026/401872925.json.gz`, trimmed to `{"boxscore": …}` |

`2026_01_CLE_JAX` is deliberately **not** the Phase 5 fixture: its Shield player-statistics
route 404s (the one game of 41 probed that does), which is why the box is fail-open per route.
