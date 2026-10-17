# ibba fixtures

Real bodies from the Israeli Premier League sources, used by `tests/basketball/ibba/` (CI has no
sdv-internal-refs checkout, so nothing reads it).

- `full/` -- untruncated bodies fetched live on 2026-10-12 (keyless, requests >= 1 s apart); the schemas' dtypes
  come from these. ASMX (`https://basket.co.il/ws/ws.asmx/{Op}`): `Boxscore.json` (game 26392, 2025-26 round 1,
  Beer Sheva 92-104 Maccabi Tel Aviv), `Teams.json` (board 5, 2026), `Players.json` (team 1109 / club 10, 2026),
  `Games.json` (board 5, round 1, 2026); contact fields in `Teams.json` are `REDACTED`. Segev Stats
  (`https://stats.segevstats.com/realtimestat_heb/api/`): `getGameInfo.json`, `getBoxScore.json`,
  `getActions.json` (game 50 = IBBA 26392, 768 actions) and `getBoxScore_419.json` (the 2026-27 game-stat keys).
- `captures/asmx/`, `captures/segev/` -- the truncated samples of sdv-internal-refs `ibba/captures/` (refs `main`
  `f8064c2`, captured 2026-10-12): one file per ASMX op, and per Segev method and game (50, 300 women's, 419),
  `getActions` once per action type. `captures/REFS_README.md` is the refs provenance note.
- `spec_paths.json`, `spec_paths_segev.json` -- the specs' routes, written by `tools/codegen/gen_ibba.py` and
  `gen_ibba_segev.py` so the tests can assert the YAML covers every route.
