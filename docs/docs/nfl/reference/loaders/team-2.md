---
title: "NFL dataset loaders — Team: group–aliases"
sidebar_label: "Team: group–aliases"
sidebar_position: 5
description: "NFL dataset loaders — Team: group–aliases — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL dataset loaders — Team: group–aliases

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

## load_nfl_team_aliases

Release: [nfl_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nfl_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nfl_groups/nfl_team_aliases.parquet`

:::caution[Coverage]
One season-less file: every id and name another source (cbs, fox, yahoo) uses for a team, keyed on the ESPN team id (team_id, a string; team_id_source = espn), with the seasons the source shows it on that team (valid_from / valid_to, inclusive; both null for an undated curated map). id_kind says whether source_id is the source's id for this sport's team or one id for every sport of a school; a name-only row has a null source_id. A source id means something only within its source (Fox 25 is not ESPN 25), so filter on source before joining on source_id. Seasons are the STARTING year (2025 = the 2025-26 season).
:::

### Returns {#load_nfl_team_aliases-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("nfl"). |
| `team_id` | String | ESPN team id the alias points to, as a string (e.g. "150"); the same id as team_id in load_nfl_team_group_seasons. Join on it to reach ESPN team data. |
| `team_id_source` | String | Id space of team_id; always "espn" in this table. |
| `source` | String | Provider that uses this id or name (in this table: cbs, fox, yahoo). |
| `source_id` | String | The source's own id for the team (e.g. Fox 25, Yahoo nba.t.1); null for a name-only row. It means something only within its source (Fox 25 is Ohio State, ESPN 25 is California), so filter on source first. |
| `id_kind` | String | "team" when source_id is the source's id for this sport's team, or "school" when it is one id for every sport of a school (the stats.ncaa.org org id, 247Sports institutionKey, On3 orgKey). |
| `name_kind` | String | Kind of label in value: "name" or "abbreviation"; null when the row carries only an id. An id with both a name and an abbreviation is two rows. |
| `value` | String | The source's own spelling of the team name or abbreviation; null when the row carries only an id. |
| `valid_from` | Int32 | First season the source shows this id or name on the team, inclusive (STARTING year: 2025 = the 2025-26 season); null, with valid_to, for an undated curated map. |
| `valid_to` | Int32 | Last season the source shows this id or name on the team, inclusive (STARTING year: 2025 = the 2025-26 season); null, with valid_from, for an undated curated map. A gap in the source splits the window into two rows. |
| `match_method` | String | How the row was tied to the ESPN team: "deterministic" (a formula or shared key, e.g. Yahoo {league}.t.{espn id}; CFBD ids are ESPN ids), "crosswalk" (carried from a published crosswalk leg), "curated" (a cited map), or for a hand-captured team directory row "alias", "exact_name" (normalized name) or "dict" (curated name dictionary). |
| `evidence` | String | The release asset or sportsdataverse/sdv-reference-data file the row came from. |

```python
load_nfl_team_aliases()
```
