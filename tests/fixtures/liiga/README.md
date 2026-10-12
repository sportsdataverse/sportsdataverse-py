# Liiga fixtures (`liiga.fi/api/v2`)

**Per-route captures** (`*.json` in this directory): copied verbatim from
`sdv-internal-refs/liiga/captures/` (refs commit `ee1b52e`, captured 2026-10-12). They are truncated
(every array cut to 3 elements, id-keyed maps to 3 keys); see that directory's README for the example
ids (season 2026, game 1 HPK-TPS, team `55786244:hpk`, player `21870028`) and the variant naming.

**Full bodies** (`full/`): fetched untruncated on 2026-10-12, keyless, 1.2 s apart. Dtypes and the
returns-schemas of the box score and the games list are derived from these, not from the 3-row captures,
because Liiga sends a rate as an integer when it is whole (`expectedGoals: 3`, `distance: 0`,
`hardestShot: 25`) and the truncated captures can hold only the integer form.

| File | URL | Notes |
|---|---|---|
| `full/games__stats__2026__1.json` | `https://liiga.fi/api/v2/games/stats/2026/1` | complete box score of game 1 of 2025-26 (223 KB) |
| `full/games__playoffs__2026__3games.json` | `https://liiga.fi/api/v2/games?tournament=playoffs&season=2026` | trimmed to 3 complete games (55080, 55081, 55116) of the 60; 55116's away `expectedGoals` is the integer `3` |
