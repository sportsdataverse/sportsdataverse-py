<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Yahoo Sports fixtures](#yahoo-sports-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Yahoo Sports fixtures

| file | source | notes |
|---|---|---|
| `shangrila_team_season_stats_ncaaf_2024.json` | `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsByTeam?lang=en-US&region=US&tz=America%2FChicago&leagues=ncaaf&season=2024&count=200&leagueStructureId=ncaaf.struct.div.1` through `yahoo_cfb_team_season_stats(season=2024, return_parsed=False)`, captured 2026-10-11 | HTTP 200, 2.9 MB; trimmed to the first 3 of 134 teams in `data.leagues[0].footballStats` (Clemson, Duke, Florida St.), every other key kept. Used by `tests/cfb/test_cfb_yahoo_ext.py` (B-L50). |

The other files in this directory came with #452 (`407db46cd8`, 2026-09-03) and predate this README.
