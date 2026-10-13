# IBBA captures — provenance

Captured **2026-10-12**, live and keyless, by `ibba/tools/capture.py` (curl_cffi, requests >= 1 s apart). Arrays are
cut to 3 rows at every depth; contact fields and coaches' dates of birth are `REDACTED`.

**`asmx/`** — `https://basket.co.il/ws/ws.asmx/{Op}`, named `{Op}@{param values in WSDL order}.json`:

| Sample | Why |
|---|---|
| Boxscore / Games `@26392_2026` | 2025-26 Winner League round 1, Beer Sheva 92-104 Maccabi Tel Aviv = Segev 50 |
| Boxscore / Games `@26646_2027` | 2026-27 round 1, Maccabi Tel Aviv 120-93 Galil Elion = Segev 419 |
| Boxscore `@1944_2005`, Games `@5_0_0_0_0_2005` | 2004-05, the earliest box score verified (Ironi Ashkelon 91-83 Ironi Nahariya) |
| (box scores) | each banks the first player row and both TOTAL rows, so the sample still passes the game-final guard |
| Configuration, Boards, Teams for 2026 and 2005 | the season-key diff |
| TeamById, Players, Coaches, PlayerStatsById, TeamStatsById, LeaderByType, BoardsEx, GamesEx, GamesByTeam | Maccabi Tel Aviv (club 10, team-season 1109), 2025-26 |

**`segev/`** — `https://stats.segevstats.com/realtimestat_heb/api/?method=&game_id=`, named `{method}@{game_id}.json`:

| game_id | Competition | Game |
|---|---|---|
| 50 | Winner League 2025-26 | = IBBA 26392 |
| 419 | Winner League 2026-27 | = IBBA 26646 |
| 300 | Women (2026-02-05) | RAMAT GAN (W) 67-56 M.B ASHDOD |

`getActions` is banked once per action type, `{method}@{game_id}~{type}.json`: the same envelope with `actions`
filtered to that type and cut to 3 rows, preferring one row per subtype / filled-parameter set (shots: one dunk, one
lay-up, one jump shot). The live body is a single list of ~770 actions in game order. `getActions@{game_id}.types` is the completion
marker: the action types banked, written after every type file. A set without it, or with type files it does not list,
is re-captured; a re-capture removes type files the new body no longer has.
