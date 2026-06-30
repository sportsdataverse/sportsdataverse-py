<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NBA Engine — Tracking Fixtures](#nba-engine--tracking-fixtures)
  - [Files](#files)
  - [Provenance](#provenance)
  - [Structure](#structure)
  - [Re-capture](#re-capture)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NBA Engine — Tracking Fixtures

Offline payloads for the `leaguedashptstats` (Player Tracking) engine tests.

## Files

| File | Season | Rows | Description |
|---|---|---|---|
| `leaguedashptstats_drives_player_2223.json` | 2022-23 | 539 | Drives / Player / Totals / Regular Season |
| `leaguedashptstats_drives_player_2324.json` | 2023-24 | 572 | Drives / Player / Totals / Regular Season |

## Provenance

- **Endpoint:** `stats.nba.com/stats/leaguedashptstats`
- **Parameters:**
  - `pt_measure_type=Drives`
  - `player_or_team=Player`
  - `per_mode_simple=Totals`
  - `season_type_all_star=Regular Season`
  - `league_id=00`
- **Capture date:** 2026-06-29
- **Generator:** `tools/fixtures/gen_tracking_fixtures.py`

## Structure

Each file is the raw JSON response from the stats.nba.com API:

```json
{
  "resource": "leaguedashptstats",
  "parameters": { ... },
  "resultSets": [
    {
      "name": "LeagueDashPtStats",
      "headers": ["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "TEAM_ABBREVIATION", "GP", ...],
      "rowSet": [ [...], ... ]
    }
  ]
}
```

## Re-capture

If the schema drifts (new/removed columns) or you need fresher data:

```sh
uv run python tools/fixtures/gen_tracking_fixtures.py
```

Then verify the row counts look reasonable (500+ players per season) and commit the updated fixtures.
