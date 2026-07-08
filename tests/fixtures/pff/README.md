<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [PFF Premium Stats fixtures](#pff-premium-stats-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# PFF Premium Stats fixtures

Real response captures from **`premium.pff.com/api/v1`** (PFF Premium Stats 2.0),
captured 2026-07-08 from a logged-in session and mirrored from
`sdv-internal-refs/pff/captures/samples/`.

These drive the offline parser + wrapper tests in `tests/nfl/test_pff_parsers.py`
(transport injected — no network, no credentials). They are payload-agnostic:
re-captured fixtures continue to work as long as the envelope shape doesn't drift.

| Fixture | Endpoint | Shape |
|---|---|---|
| `facet_passing_summary.json` | `/facet/passing/summary` (By Position) | `{passing_summary: [rows]}` |
| `facet_defense_summary.json` | `/facet/defense/summary` | `{defense_summary: [rows]}` |
| `facet_team_filter.json` | `/facet/passing/summary?franchiseId=` (By Team) | `{passing_summary: [rows]}` |
| `facet_game_filter.json` | `/facet/passing/summary?gameId=` (By Game) | `{passing_summary: [rows]}` |
| `facet_passing_detail_wide.json` | `/facet/passing/detail` | `{passing_detail_stats: [wide rows]}` |
| `facet_receiving_coverage_matrix.json` | `/facet/receiving/coverage` | `{receiving_coverage_stats: {defenders, receivers, versus}}` |
| `player_passing_summary.json` | `/player/passing/summary` | `{passing_summary: {subject, week_totals, weeks}}` |
| `player_position_pivot.json` | `/player/position/pivot` | `{snaps: [rows]}` |
| `player_seasons.json` | `/player/seasons` | `{seasons: [years]}` |
| `players_search.json` / `players_by_id.json` | `/players` | `{players: [rows]}` |
| `leagues.json` | `/leagues` | `{leagues: [rows]}` |
| `teams.json` | `/teams` | `{franchise_groups, games, teams}` (multi-key) |
| `teams_overview.json` | `/teams/overview` | `{team_overview: [rows]}` |
| `games.json` | `/games` | `{games: [rows]}` |
