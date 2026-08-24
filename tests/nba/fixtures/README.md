<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [stats.nba.com / stats.wnba.com capture fixtures](#statsnbacom--statswnbacom-capture-fixtures)
  - [2026-08-23 stats-surface expansion captures](#2026-08-23-stats-surface-expansion-captures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# stats.nba.com / stats.wnba.com capture fixtures

The original pilot captures (cap_leaguedashplayerstats_*, cap_playercareerstats_nba, cap_scoreboardv3_nba, cap_shotlocations_nba) come from the 0.0.72 capture sweep.

## 2026-08-23 stats-surface expansion captures

Captured live from a residential IP with curl_cffi `impersonate="chrome"`:

| fixture | endpoint / host | request |
|---|---|---|
| cap_playbyplayv2_wnba.json | stats.wnba.com/stats/playbyplayv2 | GameID=1022400050 |
| cap_boxscoretraditionalv3_wnba.json | stats.wnba.com/stats/boxscoretraditionalv3 | GameID=1022400001 |
| cap_boxscoresummaryv3_wnba.json | stats.wnba.com/stats/boxscoresummaryv3 | GameID=1022400001 |
| cap_homepagev2_wnba.json | stats.wnba.com/stats/homepagev2 | Season=2024, Team, Traditional |
| cap_playercareerbycollegerollup_wnba.json | stats.wnba.com/stats/playercareerbycollegerollup | Season=2024, Totals |
| cap_scoreboardv2_nba.json | stats.nba.com/stats/scoreboardv2 | GameDate=2025-01-15 |
