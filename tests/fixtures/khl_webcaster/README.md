# KHL webcaster fixtures (`khl_webcaster`)

Real responses from the KHL TV app's backend,
`GET https://khl.api.webcaster.pro/api/khl_mobile/<route>.json?locale=en` (keyless).

- `*.json` (top level): copies of `sdv-internal-refs/khl/captures/webcaster/` at refs commit
  `1104660`, captured 2026-10-12 by `khl/tools/capture.py`. Stage `370` (= khl.ru tournament 1369),
  event `2786714` (= game 898236), team `18` (= club 7), players `33405` and `1144`, `events_v2`
  page 1. TRUNCATED: every array keeps its first 3 entries.
- `full/data.json`: ONE complete `data.json` body, fetched 2026-10-12 (`?locale=en`), untruncated,
  written compact. It holds the whole season table (`stages_v2`, 2008-09 to 2026-27) that maps a
  season END year to a stage; the `stages` returns schema and the season-mapping tests come from it.

Redactions: `remote_ip` (the caller's IP) and `bets` / `fonbet` are removed at any depth, as the
runtime does. Nothing else is edited. Owner flag (refs `khl/README.md`): fine as test fixtures; no
published data release without sanctions sign-off.
