# SHL fixtures (`www.shl.se/api`)

Real bodies from the SHL site API (keyless GET JSON; Sportality platform, stats by StatNet).

## Truncated route samples (`*.json`)

Copied unchanged from `sdv-internal-refs/shl/captures/` at refs commit `9e967aa` (captured
2026-10-12 by `shl/tools/capture.py`). Every array is cut to 3 items and every id-keyed map
to 3 keys, except play-by-play, which keeps the first event of each `type`. The 34
`statistics-v2__stats-info__module__<module>.json` files were fetched with `count=1000`
for the 2025/26 regular season (ssgt `iuzqg7dqk9`). File names follow `capture.py`'s slug
rule: the path with `/` -> `__` and the braces dropped, plus `__<module>` for stats-info.

Examples in them: game `bdhvuc5tex` (Frölunda HC 2-1 Linköping HC, 2025-09-13, StatNet
22003), team `087a-087aTQv9u` (Frölunda HC), athlete `qQ9-349c7WDv6` (Lars Johansson,
isa 2427), current ssgt `qa98unlbd6` (2026/27 regular season) for the match-day routes.

## Full bodies (`full/`)

Untruncated, fetched live on 2026-10-12. The column dtypes of the returns schemas and the
dtype tests come from these, not from the 3-row samples:

| File | URL |
|---|---|
| `gameday__play-by-play__bdhvuc5tex.json` | `https://www.shl.se/api/gameday/play-by-play/bdhvuc5tex` (131 events) |
| `gameday__boxscore__bdhvuc5tex.json` | `https://www.shl.se/api/gameday/boxscore/bdhvuc5tex` |
| `statistics-v2__stats-info__teams_result__iuzqg7dqk9.json` | `https://www.shl.se/api/statistics-v2/stats-info/teams_result?ssgtUuid=iuzqg7dqk9&count=1000` (14 teams) |
| `statistics-v2__stats-info__goalkeepers_summary__iuzqg7dqk9.json` | `https://www.shl.se/api/statistics-v2/stats-info/goalkeepers_summary?ssgtUuid=iuzqg7dqk9&count=1000` (36 goalies) |
| `statistics-v2__layout-info__directory.json` | **Derived** from `https://www.shl.se/api/statistics-v2/layout-info` (1.5 MB): the first (statnet) item only, with every ssgt's `state`, `ssgtUuid`, `gameType`, `season` and `series` (uuid, code, name) kept and the team lists, filters and module lists dropped. All 110 SHL ssgts, 1975/76 to 2026/27. |
