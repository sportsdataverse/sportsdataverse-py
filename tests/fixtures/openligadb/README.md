# OpenLigaDB fixtures

Copied verbatim from `sdv-internal-refs/openligadb/captures/` (refs main `22b148b`), server
`https://api.openligadb.de`, captured 2026-10-06. Arrays are truncated to 3 elements at every
depth by the reference repo's capture tool, so row counts here are sample sizes, not live ones.

Example ids: `league=bl1` (Bundesliga), `season=2025` (= 2025/26), `group=1` (matchday 1),
`match_id=77264` (Heidenheim–Wolfsburg), `league_id=4937` (bl1 2026/27, a season still in
progress), `team_id=40` (FC Bayern München).

| File | URL |
|---|---|
| `getavailableleagues.json` | `/getavailableleagues` |
| `getavailablegroups__league__season.json` | `/getavailablegroups/bl1/2025` |
| `getcurrentgroup__league.json` | `/getcurrentgroup/bl1` |
| `getavailableteams__league__season.json` | `/getavailableteams/bl1/2025` |
| `getmatchdata__league__season.json` | `/getmatchdata/bl1/2025` |
| `getmatchdata__league__season__group.json` | `/getmatchdata/bl1/2025/1` |
| `getmatchdata__match_id.json` | `/getmatchdata/77264` |
| `getbltable__league__season.json` | `/getbltable/bl1/2025` |
| `getgoalgetters__league__season.json` | `/getgoalgetters/bl1/2025` |
| `getnextmatchbyleagueteam__league_id__team_id.json` | `/getnextmatchbyleagueteam/4937/40` |
| `getlastchangedate__league__season__group.json` | `/getlastchangedate/bl1/2025/1` (a bare quoted ISO string, not an object) |
