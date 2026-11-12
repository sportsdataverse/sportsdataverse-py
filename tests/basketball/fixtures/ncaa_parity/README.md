<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Basketball parity corpus: stats.ncaa.org (BB-D16)](#basketball-parity-corpus-statsncaaorg-bb-d16)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Basketball parity corpus: stats.ncaa.org (BB-D16)

Real captures backing `tests/basketball/test_sources_ncaa.py`, which runs ESPN and the stats.ncaa.org adapter
(`sportsdataverse/mbb/ncaa_pbp/to_espn_summary.py`, `source="ncaa"`) through the basketball dispatch leg
(`helper_<lg>_pbp` + `game_page_frames`) and holds every game to `NCAA_GATES` in
`sportsdataverse/basketball/sources/parity.py`.

Sixteen 2026-27 finals, 8 MBB and 8 WBB, both teams Division I. They were drawn with `random.seed(16)` and
`random.sample(..., 8)` from the sorted contest ids of `{lg}.ncaa_team_box` (season 2027) whose two rows both
carry an ESPN game id, both ESPN team ids and points. Captured 2026-11-11, every game at least a day after its
final.

| File | Source |
|---|---|
| `ncaa_<lg>_<contest id>.json.gz` | the contest's rows from the Data API: `GET /v1/<lg>/ncaa_pbp`, `ncaa_shots`, `ncaa_team_box`, `ncaa_player_box` (`season=2027&contest_id=<id>`), and `ncaa_player_crosswalk` (`season=2027&ncaa_team_id=<id>`, both teams), one request at a time, 2 s apart. Keys `pbp`, `shots`, `team_box`, `player_box`, `player_crosswalk`; rows in the order the API returned them (storage blocks, not game order) |
| `espn_<lg>_<espn id>.json.gz` | `espn_mbb_pbp(id, raw=True)` / `espn_wbb_pbp(id, raw=True)`, 1.5 s apart |
| `teams_2027.json.gz` | `{lg: {espn team id: row}}`: the 2026-27 rows of `ncaa_espn_team_crosswalk(lg)` for the 16 teams of each league's games |

Nothing was fetched from stats.ncaa.org: the NCAA rows are the daily pipeline's published parse. Every JSON is
gzipped compact (`separators=(",", ":")`) and otherwise verbatim.

| League | ESPN id | Contest id | Why it is in the gate comments |
|---|---|---|---|
| MBB | 401909502 | 6634067 | |
| MBB | 401909514 | 6643006 | lowest play-count ratio and `wp_corr` |
| MBB | 401916287 | 6638305 | |
| MBB | 401917515 | 6642010 | overtime; a player the season's roster lacks |
| MBB | 401918967 | 6631086 | |
| MBB | 401920344 | 6608199 | |
| MBB | 401920769 | 6640714 | a basket and a team turnover the NCAA plays do not match |
| MBB | 401920856 | 6639277 | |
| WBB | 401906979 | 6635935 | lowest minutes agreement |
| WBB | 401913054 | 6641682 | score columns away first |
| WBB | 401913756 | 6645353 | |
| WBB | 401917938 | 6646439 | |
| WBB | 401918712 | 6647577 | |
| WBB | 401919679 | 6607284 | |
| WBB | 401926100 | 6638034 | four free throws missing from the NCAA plays |
| WBB | 401927060 | 6643825 | an administrative technical charged to a team |
