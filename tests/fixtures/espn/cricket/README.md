<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ESPN Cricket Fixtures](#espn-cricket-fixtures)
  - [Endpoints captured per league](#endpoints-captured-per-league)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# ESPN Cricket Fixtures

Captured for offline parser tests. Each league slug gets a subdirectory.

| Dir | League | Capture date |
|---|---|---|
| `8048/` | IPL (Indian Premier League) | 2026-06-13 |

## Endpoints captured per league

- `site-v2/scoreboard.json` — `GET /sports/cricket/{league}/scoreboard`
- `site-v2/standings.json` — `GET /sports/cricket/{league}/standings`
- `site-v2/summary.json` — `GET /sports/cricket/{league}/summary?event={id}`
