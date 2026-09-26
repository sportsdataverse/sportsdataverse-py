<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [tests/fixtures/pff_api — provenance](#testsfixturespff_api--provenance)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# tests/fixtures/pff_api — provenance

Every file here is an example response PFF itself **publishes** in the public PFF Developer API
spec (`https://api.pff.com/openapi.json`, v2.1.0, fetched 2026-09-26): the `example` on each
operation's `200 application/json` response. PFF documents each one as "an actual body … cut to
its first few rows" from one coherent request (Cincinnati Bengals, franchise 7, 2022, week 1;
Joe Burrow, player 28022; game 23108). None of them come from our own captures.

| File | Operation |
|---|---|
| `facet_passing_summary.json` | `GET /v1/facet/passing/summary` |
| `team_summary.json` | `GET /v1/teams/summary` |
| `player_passing_concept.json` | `GET /v1/player/passing/concept` |
| `player_offense_pass_blocking.json` | `GET /v1/player/offense/pass_blocking` |
| `team_directory.json` | `GET /v2/{league}/teams` |
| `team_stats.json` | `GET /v2/{league}/teams/stats` |
| `team_roster.json` | `GET /v2/{league}/teams/{team}/roster` |
| `team_leaders.json` | `GET /v2/{league}/teams/{team}/leaders` |
| `team_rushing_direction.json` | `GET /v2/{league}/teams/{team}/reports/rushing-direction` |
| `position_report.json` | `GET /v2/{league}/positions/reports/{report}` |

Refresh: re-extract from the vendored spec in sdv-internal-refs
(`pff/developer/pff-developer.openapi.json`) when PFF bumps `info.version`.
