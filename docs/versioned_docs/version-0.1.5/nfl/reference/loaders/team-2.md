---
title: "NFL dataset loaders — Team: group"
sidebar_label: "Team: group"
sidebar_position: 5
description: "NFL dataset loaders — Team: group — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL dataset loaders — Team: group

## load_nfl_team_group_seasons

Release: [nfl_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nfl_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nfl_groups/nfl_team_group_seasons_{season}.parquet`

:::caution[Coverage]
One row per team per season: the SDV subdivision, conference and division group ids the team belonged to that season (null where a level does not apply), the team name as of that season, where the membership came from, and whether a second source agreed (null when only one source covers the season). team_id is a string: the ESPN team id; team_id_source names the id space. season is the STARTING year (2025 = the 2025-26 season); seasons 1970-2026.
:::

### Returns {#load_nfl_team_group_seasons-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("nfl"); the prefix of every group_id in it. |
| `season` | Int32 | Season of the membership (STARTING year: 2025 = the 2025-26 season). |
| `team_id` | String | Team id as a string: the ESPN team id where ESPN covers the team, otherwise the league's own id; team_id_source says which. |
| `team_id_source` | String | Id space of team_id (in this table: espn). |
| `team_name` | String | Team name as of that season, not today's. |
| `subdivision_id` | String | SDV group_id of the team's subdivision that season (e.g. FBS / FCS, Division I); null where the league has no subdivision level. |
| `conference_id` | String | SDV group_id of the team's conference that season; null where the team had no conference (an independent, or a season played without conferences). |
| `division_id` | String | SDV group_id of the team's division that season; null where the level does not apply. |
| `source` | String | Source the membership was taken from -- the most reliable per-season source for that era. |
| `sources_agree` | Boolean | Whether a second source agreed on the membership; null when only one source covers the season. |
| `notes` | String | Builder notes on the team-season, such as a source disagreement or which of several listed memberships was kept. |

```python
load_nfl_team_group_seasons(seasons=2024)
```
