# HockeyAllsvenskan fixtures (`hockeyallsvenskan.se`)

**Per-route captures** (`*.json` in this directory): copied verbatim from
`sdv-internal-refs/hockeyallsvenskan/captures/` (refs commit `64e4e9f`, captured 2026-10-12). They are
truncated (every array cut to 3 elements, except `sort_by_options`, `teams` and `standings`, kept
whole; play-by-play keeps the first event of each type per period). See that directory's README for
the request bodies: season `2025` on the wire = 2025/26 (sdv END year 2026), game 23401
(`20260918-aik-modo`), leaderboard metric `TP`, directory team `AIK`. `@2005` files are the 2005/06
season-scope proofs; `@2026-unplayed` is the 2026/27 games table newest first (unplayed rows).

**Full body** (`full/`): fetched untruncated on 2026-10-12, keyless. Dtypes and the `ha_games`
returns-schema come from it, not from the 3-row captures, because the site sends
`efficiency` as `"14.29"`, `0` or null in one column.

| File | Request | Notes |
|---|---|---|
| `full/all-time-games-stats__2026__played.json` | `POST https://hockeyallsvenskan.se/api/all-time-games-stats` with the default filters, `season: "2026"`, `game_status: "played"`, `categories: "all"`, `page: 1`, `page_size: 1000` | every played 2026/27 game on 2026-10-12 (68 games, 55 KB) |
