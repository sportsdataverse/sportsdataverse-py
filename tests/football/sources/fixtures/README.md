# `tests/football/sources` fixtures

Byte-faithful slices of real producer output. Never reformatted, never hand-edited:
the id shapes and the `kickoff_utc` spelling are what `idmap._build_nfl_idmap` parses.

| file | source | captured | rows | id dtypes |
|---|---|---|---|---|
| `nfl_espn_crosswalk_games_2026_wk1.json` | `nfl-raw/nfl/espn/crosswalk/games.json` (built by the nfl-raw daily driver) | 2026-09-17 | 17 (16 × 2026 week 1 + Super Bowl LX) | `espn_event_id` int, `home/away_espn_team_id` str, `shield_game_id` / `game_id` str, `kickoff_utc` `"%Y-%m-%dT%H:%MZ"` |
| `nfl_espn_crosswalk_teams.json` | `nfl-raw/nfl/espn/crosswalk/teams.json` | 2026-09-17 | 37 (32 clubs + relocations) | `espn_team_id` str, `shield_team_id` str |

The ESPN summaries these tests run the processors on are not copied here — they are the
existing captures in `tests/nfl/fixtures/summary_401872922.json` and
`tests/cfb/fixtures/summary_*.json` (26 games), reused through `conftest.py`.
