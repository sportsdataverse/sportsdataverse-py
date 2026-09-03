# NFL Pro fixtures

Trimmed from the verified captures in `sdv-internal-refs/nfl/nflpro/captures/secured/`
(season 2024, seasonType REG, captured 2026-09-03). Each source body was fetched to
completion (`rows == the envelope's own total`) before trimming, so these are slices of
a complete response, not a truncated page.

| File | Route | Collection key |
|---|---|---|
| `players_offense_passing_season.json` | `/api/secured/stats/players-offense/passing/season` | `passers` |
| `team_offense_overview_season.json` | `/api/secured/stats/team-offense/overview/season` | `offense` |
| `fantasy_game.json` | `/api/secured/stats/fantasy/game` | `players` |
