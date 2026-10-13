# KBL fixtures

Source: `sdv-internal-refs` commit `cb7a95a`, directory `kbl/` (README of 2026-10-12), host `https://api.kbl.or.kr`.

- `captures/`: the 45 truncated refs captures, copied unchanged (captured live 2026-10-12, arrays cut to 3 rows).
- `full/`: untruncated bodies fetched live on 2026-10-13 with the headers `Channel: WEB`, `TeamCode: XX`, 1.5 s apart:
  game `S47G01N49` (header, text-cast with `quarterList=Q1,Q2,Q3,Q4,EQ1..EQ4`, player-stat, team-record, match-chart),
  `/leagues/S45G01/stats/players`, `/league/rank/S45G01`, `/league/playoff/new/S45G01`,
  `/league/rank/S45G01/team-tracks`, and the first 600 rows of `/players` (1,495 rows live; cut for size).
  Numeric dtypes in the returns schemas come from these bodies.
- `spec_paths.json`, `season_crosswalk.json`: written by `tools/codegen/gen_kbl.py` (the spec's routes, the route behind
  each wrapper, the README seasonCode crosswalk).
