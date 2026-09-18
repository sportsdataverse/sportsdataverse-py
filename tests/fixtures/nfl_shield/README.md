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
| `2026_02_DET_BUF_pregame.json.gz` | 2026 wk2 DET @ BUF (TNF) | `PREGAME`, offset 3, 1 play (`GAME_START`) | same capture, snapshot 0/223, `2026-09-18T00:00:01Z` |

The three 2026 files are the live-path fixtures: the Q2 in-progress one exercises
open-drive possession, the held-back game-outcome columns, the current-situation row
and the provisional tail; the pregame one exercises the degenerate 1-row payload; the
Q4 one is the *later* snapshot of the same game, so the pair pins the prefix invariant
(the parser is a pure function of the payload prefix) inside CI — the 223-snapshot
sweep that measured it lives outside the repo and CI never runs it.

Shield revises a play's `playDescription` / `stats` / yardage after the snap, up to ~11
plays back, so the invariant can only be asserted for plays whose **raw payload object**
is unchanged between the two snapshots; `test_prefix_invariant_across_two_snapshots`
selects exactly those.
