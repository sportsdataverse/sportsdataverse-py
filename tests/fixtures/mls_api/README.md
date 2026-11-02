<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [MLS web API fixtures](#mls-web-api-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# MLS web API fixtures

Real, trimmed responses from the three auth-free mlssoccer.com hosts, captured
2026-07-18 and copied byte-for-byte from the maintainers' captures.

| File | Host | Route |
|---|---|---|
| `statsapi_competitions.json` | stats-api | `/competitions` |
| `statsapi_competitions_seasons.json` | stats-api | `/competitions/{id}/seasons` |
| `statsapi_matches_by_season.json` | stats-api | `/matches/seasons/{seasonId}` |
| `statsapi_standings_conference.json` | stats-api | `/competitions/{id}/seasons/{seasonId}/standings?category=conference` |
| `statsapi_match_single.json` | stats-api | `/matches/{matchId}` |
| `statsapi_club_single.json` | stats-api | `/clubs/{clubId}` |
| `sportapi_match_single.json` | sportapi | `/api/matches/{matchId}` |
| `sportapi_players_byclub.json` | sportapi | `/api/players/byClub/{clubId}` |
| `dapi_seasons_query.json` | dapi | `/v2/content/en-us/seasons` |

Row arrays are trimmed to three entries. Note these captures are the ground
truth where the OpenAPI spec disagrees: `/competitions`, the seasons list, the
season match list and the standings route all answer with an **envelope**, not
the bare array the spec declares. Do not hand-edit.
