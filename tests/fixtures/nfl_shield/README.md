# nfl_shield fixtures

Real Shield (`api.nfl.com`) `experience/v2/gamedetails/{uuid}?includeDriveChart=true`
payload bodies, gzipped verbatim (no trimming).

| file | game | phase | provenance |
|---|---|---|---|
| `2024_01_BAL_KC.json.gz` | 2024 wk1 BAL @ KC (final, 27-20) | `FINAL` | `nfl-raw` committed library `nfl/raw/2024/2024_01_BAL_KC.json` |
| `2026_02_DET_BUF_ingame_q2.json.gz` | 2026 wk2 DET @ BUF (TNF) | `INGAME` Q2 `00:05`, offset 262, 103 plays, **open drive** (`endedPlaySequenceNumber: null`) | live capture `2026-09-18T01:49:10Z`, snapshot 110/223 of `background-research/2026-09-16-nfl-shield-live/capture/data/401872932_DET_BUF` |
| `2026_02_DET_BUF_pregame.json.gz` | 2026 wk2 DET @ BUF (TNF) | `PREGAME`, offset 3, 1 play (`GAME_START`) | same capture, snapshot 0/223, `2026-09-18T00:00:01Z` |

The two 2026 files are the live-path fixtures: the in-progress one exercises open-drive
possession, the held-back game-outcome columns, the current-situation row and the
provisional tail; the pregame one exercises the degenerate 1-row payload.
