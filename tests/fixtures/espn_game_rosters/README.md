<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ESPN Core v2 game-roster chain captures (task 2.10)](#espn-core-v2-game-roster-chain-captures-task-210)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# ESPN Core v2 game-roster chain captures (task 2.10)

Captured 2026-10-18 from a residential IP with `requests` (User-Agent `Mozilla/5.0`, no credentials),
every answer HTTP 200, content unchanged, re-serialised with `json.dump(indent=1)`. They drive
`tests/test_espn_game_rosters_family.py`; nba uses the older `tests/fixtures/espn/event_competitors_nba.json`,
`athlete_core_nba.json` and `team_core_nba.json`. Kept out of `tests/fixtures/espn/` so the ESPN parser
sweep (`tests/test_espn_universal_parsers.py`) does not pick them up; no parser reads them.

The game of each league is the first event of `tests/fixtures/espn/scoreboard_<lg>.json`. The athlete is
the first entry of the first competitor's `roster`; the team is that competitor's `team`.

| file | request |
|---|---|
| `event_competitors_mbb.json` | `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/events/401715624/competitions/401715624/competitors` |
| `athlete_core_mbb.json` | `GET http://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/seasons/2025/athletes/4432869?lang=en&region=us` |
| `team_core_mbb.json` | `GET http://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/seasons/2025/teams/2752?lang=en&region=us` (carries the college `awards` / `coaches` links of B-GB-32) |
| `event_competitors_wbb.json` | `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/events/401724075/competitions/401724075/competitors` |
| `athlete_core_wbb.json` | `GET http://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/seasons/2025/athletes/4899052?lang=en&region=us` |
| `event_competitors_wnba.json` | `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/wnba/events/401620308/competitions/401620308/competitors` |
| `athlete_core_wnba.json` | `GET http://sports.core.api.espn.com/v2/sports/basketball/leagues/wnba/seasons/2024/athletes/3142055?lang=en&region=us` |
| `event_competitors_nfl.json` | `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401772940/competitions/401772940/competitors` |
| `athlete_core_nfl.json` | `GET http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2025/athletes/15718?lang=en&region=us` |
| `event_competitors_nhl.json` | `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/events/401687987/competitions/401687987/competitors` |
| `athlete_core_nhl.json` | `GET http://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/2025/athletes/3735?lang=en&region=us` (leaf position with a `parent` link) |
| `event_competitors_cfb.json` | `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401636892/competitions/401636892/competitors` |
| `athlete_core_cfb.json` | `GET http://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/athletes/4426773?lang=en&region=us` (leaf position with a `parent` link) |
| `event_competitors_mlb.json` | `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401701038/competitions/401701038/competitors` |
| `event_competitor_roster_mlb_19.json` | `GET http://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401701038/competitions/401701038/competitors/19/roster?lang=en&region=us` (captured 2026-10-25; the roster `$ref` of `event_competitors_mlb.json`; entries carry `athlete` / `position` as a bare `$ref`) |
| `event_competitor_roster_mlb_10.json` | `GET http://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401701038/competitions/401701038/competitors/10/roster?lang=en&region=us` (captured 2026-10-25, same shape) |
| `mlb_athletes/athlete_<playerId>.json` (21 files) | `GET http://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/seasons/2024/athletes/<playerId>?lang=en&region=us`, the athlete `$ref` of each entry in the two roster captures above (captured 2026-10-25, one request per athlete, sequential, 1.5 s apart) |
