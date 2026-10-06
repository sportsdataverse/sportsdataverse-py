<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Sleeper fixtures](#sleeper-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Sleeper fixtures

Real, trimmed responses from the Sleeper fantasy API v1 (`https://api.sleeper.app/v1`,
keyless), captured 2026-10-06 and copied byte-for-byte from
`sdv-internal-refs/sleeper/captures/` at commit `e330067`.

| File | Route |
|---|---|
| `user__username.json` | `/user/{username}` |
| `user__user_id__leagues__nfl__season.json` | `/user/{user_id}/leagues/nfl/{season}` |
| `league__league_id.json` | `/league/{league_id}` |
| `league__league_id__rosters.json` | `/league/{league_id}/rosters` |
| `league__league_id__users.json` | `/league/{league_id}/users` |
| `league__league_id__matchups__week.json` | `/league/{league_id}/matchups/{week}` |
| `league__league_id__winners_bracket.json` | `/league/{league_id}/winners_bracket` |
| `league__league_id__transactions__week.json` | `/league/{league_id}/transactions/{week}` |
| `league__league_id__traded_picks.json` | `/league/{league_id}/traded_picks` |
| `league__league_id__drafts.json` | `/league/{league_id}/drafts` |
| `draft__draft_id.json` | `/draft/{draft_id}` |
| `draft__draft_id__picks.json` | `/draft/{draft_id}/picks` |
| `state__nfl.json` | `/state/nfl` |
| `players__nfl.json` | `/players/nfl` (id-keyed map; first 3 keys kept) |
| `players__nfl__trending__add.json` | `/players/nfl/trending/add` |

Example league `289646328504385536` (2018), draft `257270643320426496`, week `1`, user
`457511950237696`. Arrays are cut to their first 3 elements at every depth; the players map
keeps its first 3 keys. Regenerate by re-copying from the reference repo; do not hand-edit.
