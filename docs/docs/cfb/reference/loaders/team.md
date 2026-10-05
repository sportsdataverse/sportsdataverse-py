---
title: "CFB dataset loaders — Team: info–box"
sidebar_label: "Team: info–box"
sidebar_position: 9
description: "CFB dataset loaders — Team: info–box — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB dataset loaders — Team: info–box

## load_cfb_team_info

Release: [cfb_team_info](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_team_info) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_team_info/cfb_team_info_{season}.parquet`
### Returns {#load_cfb_team_info-returns}

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | ESPN team id. |
| `school` | String | Team name. |
| `mascot` | String | Team mascot. |
| `abbreviation` | String | Metric abbreviation. |
| `alt_name1` | String | Team alternate name 1 (as it appears in `play_text`). |
| `alt_name2` | String | Team alternate name 2 (as it appears in `play_text`). |
| `alt_name3` | String | Team alternate name 3 (as it appears in `play_text`). |
| `conference` | String | Conference of the team. |
| `division` | String | Division in the conference for the team. |
| `classification` | String | Conference classification (fbs, fcs, ii, iii). |
| `color` | String | Primary team color (hex, no `#`). |
| `alt_color` | String | Team color (alternate). |
| `logo` | String | Team or league logo URL. |
| `logo_2` | String | URL of the team's alternate dark-background 500-pixel logo on ESPN's CDN, null for programs with no dark variant. |
| `logos_3` | String | URL of the team's logo variant in slot 3 of ESPN's team-info logo list; null when the team publishes fewer variants. |
| `logos_4` | String | URL of the team's logo variant in slot 4 of ESPN's team-info logo list; null when the team publishes fewer variants. |
| `logos_5` | String | URL of the team's logo variant in slot 5 of ESPN's team-info logo list; null when the team publishes fewer variants. |
| `logos_6` | String | URL of the team's logo variant in slot 6 of ESPN's team-info logo list; null when the team publishes fewer variants. |
| `logos_7` | String | URL of the team's logo variant in slot 7 of ESPN's team-info logo list; null when the team publishes fewer variants. |
| `logos_8` | String | URL of the team's logo variant in slot 8 of ESPN's team-info logo list; null when the team publishes fewer variants. |
| `logos_9` | String | URL of the team's logo variant in slot 9 of ESPN's team-info logo list; null when the team publishes fewer variants. |
| `logos_10` | String | URL of the team's logo variant in slot 10 of ESPN's team-info logo list; null when the team publishes fewer variants. |
| `logos_11` | String | URL of the team's logo variant in slot 11 of ESPN's team-info logo list; null when the team publishes fewer variants. |
| `logos_12` | String | URL of the team's logo variant in slot 12 of ESPN's team-info logo list; null when the team publishes fewer variants. |
| `logos_13` | String | URL of the team's logo variant in slot 13 of ESPN's team-info logo list; null when the team publishes fewer variants. |
| `logos_14` | String | URL of the team's logo variant in slot 14 of ESPN's team-info logo list; null when the team publishes fewer variants. |
| `logos_15` | String | URL of the team's logo variant in slot 15 of ESPN's team-info logo list; null when the team publishes fewer variants. |
| `logos_16` | String | URL of the team's logo variant in slot 16 of ESPN's team-info logo list; null when the team publishes fewer variants. |
| `twitter` | String | The football program's Twitter/X handle including the leading at sign, populated for only a minority of listed teams. |
| `venue_id` | Int32 | Referencing venue id. |
| `venue_name` | String | Full name of the franchise's venue. |
| `city` | String | Venue city. |
| `state` | String | U.S. state where the school is located. |
| `zip` | String | Team/venue zip code. |
| `country_code` | String | Team/venue country code. |
| `timezone` | String | Time zone in which the venue resides (i.e. Eastern Time -> "America/New_York"). |
| `latitude` | Float64 | Venue latitude in decimal degrees. |
| `longitude` | Float64 | Venue longitude in decimal degrees. |
| `elevation` | String | Venue elevation above sea level. |
| `capacity` | Int32 | Stadium capacity. |
| `year_constructed` | Int32 | Year in which the venue was constructed. |
| `grass` | Boolean | TRUE/FALSE response on whether the field is grass or not. |
| `dome` | Boolean | TRUE/FALSE response to whether the venue has a dome or not. |

```python
load_cfb_team_info(seasons=2024)
```

## load_cfb_teams

Release: [espn_cfb_teams](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_teams) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_teams/cfb_teams_{season}.parquet`
### Returns {#load_cfb_teams-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season the team row describes. The dataset is season-scoped, so a school's conference, division and branding are what ESPN published for that year rather than today's values. |
| `team_id` | Int64 | ESPN's team identifier, stable across seasons and the join key to every other ESPN-sourced CFB dataset (schedules, rosters, pbp). |
| `uid` | String | ESPN's fully-qualified resource key for the team, e.g. s:20~l:23~t:333. Rarely needed directly; team_id is the practical join key. |
| `guid` | String | Opaque ESPN global identifier for the team record. Present for traceability back to the source payload; not a join key you would normally use. |
| `slug` | String | URL fragment ESPN uses for the team on espn.com, e.g. alabama-crimson-tide. |
| `abbreviation` | String | Short team code ESPN displays on scoreboards, e.g. ALA. Not unique across all divisions, so never join on it. |
| `display_name` | String | Full team name as ESPN renders it, e.g. Alabama Crimson Tide -- location plus nickname. |
| `short_display_name` | String | Condensed team label ESPN uses where space is tight, usually the nickname alone. |
| `name` | String | Team nickname as ESPN stores it, e.g. Crimson Tide, without the school or location prefix. |
| `nickname` | String | ESPN's alternate short label for the team; often the school shorthand rather than the mascot, and it can duplicate name. |
| `location` | String | School or city ESPN attaches to the team, e.g. Alabama. Combined with name to build display_name. |
| `color` | String | Primary team colour ESPN publishes, as a hex string without the leading hash. Useful for plotting; null for teams ESPN has no branding for. |
| `alternate_color` | String | Secondary team colour as a hex string without the leading hash. Frequently null outside Division I. |
| `is_active` | Boolean | ESPN's flag for whether the program was active that season. Do not use it as a division filter -- it stays true for teams ESPN has stopped classifying. |
| `is_all_star` | Boolean | ESPN's own all-star marker. It is unreliable on its own: it catches only about a third of the exhibition squads ESPN files inside the FBS and FCS groups, which is why is_exhibition exists. |
| `is_exhibition` | Boolean | Derived marker for bowl all-star and exhibition squads that ESPN files alongside real programs. Combine as is_fbs and not is_exhibition to count actual FBS teams -- 133 rather than 145 in 2023. |
| `division` | String | Most specific NCAA grouping ESPN places the team in: fbs, fcs, d2, d3, d2_d3 for teams filed directly under the Division II/III node, naia, or all_star. Null when ESPN lists the team in no group at all. |
| `is_fbs` | Boolean | Whether the team is classified FBS, as a non-null boolean. Prefer this over comparing division, because a null division comparison yields null in polars and silently drops rows from a mask. |
| `team_group_id` | Int64 | Identifier of the immediate ESPN group holding the team. That group is a division within a conference where one exists, so an SEC team can point at SEC - West rather than at the SEC itself. |
| `team_group_name` | String | Label of that immediate group, e.g. SEC - West. Use the conference_ columns when you want the conference proper. |
| `conference_id` | Int64 | ESPN identifier of the conference, resolved by walking up from the immediate group to the ancestor whose parent is the FBS or FCS node. Null for teams with no conference, which is normal outside Division I. |
| `conference_name` | String | Full conference name for that season, e.g. Southeastern Conference. It follows realignment, so a school carries different values across seasons. |
| `conference_short_name` | String | Shortened conference label ESPN uses in compact contexts, e.g. SEC, preserving ESPN's casing. |
| `conference_abbreviation` | String | Conference abbreviation as ESPN publishes it. Note ESPN lower-cases these, so it reads sec rather than SEC. |
| `conference_midsize_name` | String | Medium-length conference label sitting between the short name and the full name in ESPN's own hierarchy. |
| `conference_slug` | String | URL fragment ESPN uses for the conference on espn.com. |
| `conference_is_conference` | Boolean | ESPN's flag marking the resolved group as a true conference rather than a structural division node. Useful for detecting when the walk-up landed somewhere unexpected. |
| `conference_parent_id` | Int64 | Identifier of the group above the conference, which is the FBS or FCS node for Division I teams. |
| `team_logo` | String | URL of the team's primary logo on ESPN's CDN, sized 500x500. Null for programs ESPN has no artwork for, which is most of the NAIA universe. |
| `team_logo_dark` | String | URL of the dark-background variant of the team logo, where ESPN publishes one. |
| `conference_logo` | String | URL of the conference's logo on ESPN's CDN. Always null outside Division I -- ESPN publishes conference artwork only for FBS and FCS. |
| `venue_id` | Int64 | ESPN's identifier for the team's home venue, joinable to ESPN venue payloads. |
| `venue_name` | String | Home venue name as ESPN records it. |
| `venue_city` | String | City of the home venue per ESPN. Compare with the CFBD-sourced city column, which describes the school rather than the stadium. |
| `venue_state` | String | State or province of the home venue per ESPN. |
| `venue_indoor` | Boolean | Whether ESPN marks the home venue as indoor. The CFBD-sourced dome column answers the same question from the other feed and the two can disagree. |
| `venue_grass` | Boolean | Whether ESPN marks the home venue's playing surface as grass. See the CFBD-sourced grass column for the second opinion. |
| `school` | String | School name as CollegeFootballData spells it. Present for cross-source reconciliation; prefer team_id for joins because spellings differ between feeds. |
| `mascot` | String | Team mascot per the CollegeFootballData feed, which often differs in wording from ESPN's name column. |
| `alt_name1` | String | First alternate school spelling CollegeFootballData carries, useful when matching against a source that names schools differently. |
| `alt_name2` | String | Second alternate school spelling from the same alias list. |
| `alt_name3` | String | Third alternate school spelling from the same alias list. |
| `cfbd_conference` | String | Conference as recorded on the CollegeFootballData feed. Kept separate from the conference_ family so the two opinions never silently overwrite one another. |
| `classification` | String | Division as recorded on the CollegeFootballData feed: fbs, fcs, ii, iii. An independent second opinion on division -- the two agree for the vast majority of teams and disagreements are worth inspecting rather than reconciling blindly. |
| `city` | String | City the school is located in, from the CollegeFootballData feed. This describes the institution, not the stadium -- see venue_city for that. |
| `state` | String | State the school is located in, from the CollegeFootballData feed. |
| `country_code` | String | ISO country code for the school's location, effectively always US in this dataset. |
| `timezone` | String | Olson timezone for the school's location, e.g. America/Chicago. Handy for converting kickoff timestamps to local time. |
| `latitude` | Float64 | Latitude of the school or its stadium in decimal degrees, for mapping and travel-distance work. |
| `longitude` | Float64 | Longitude of the school or its stadium in decimal degrees. |
| `elevation` | String | Stadium elevation in metres. Carried as text on the source feed, so cast it before doing arithmetic. |
| `capacity` | Int64 | Listed seating capacity of the home stadium. Null where the feed has no figure, which is common below Division I. |
| `dome` | Boolean | Whether the stadium is domed per the CollegeFootballData feed. Compare with venue_indoor, ESPN's answer to the same question. |
| `grass` | Boolean | Whether the playing surface is grass per the CollegeFootballData feed. Compare with venue_grass from ESPN. |

```python
load_cfb_teams(seasons=2024)
```

## load_cfb_team_portal

Release: [cfb_team_portal](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_team_portal) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_team_portal/cfb_team_portal_{season}.parquet`

:::caution[Coverage]
Portal counts are D-I to D-I moves visible in ESPN rosters (FBS and FCS mixed): an athlete id on a different team's roster the prior season. JUCO and non-D-I arrivals are not counted. Talent points name-join players to the cfb_recruits release, so an unmatched player carries the 0-star default.
:::

### Returns {#load_cfb_team_portal-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Destination season of the roster comparison: the prior season's rosters are diffed against this one's. |
| `team_id` | Int64 | ESPN team id of the team whose roster is counted (FBS and FCS mixed; filter on team_info classification). |
| `roster_n` | Int64 | Distinct ESPN athletes on the team's roster this season (the portal_share denominator). |
| `transfers_in_n` | Int64 | Players on this season's roster whose ESPN athlete id sat on a different D-I team's roster the prior season. |
| `transfers_out_n` | Int64 | Players from the team's prior-season roster whose ESPN athlete id sits on a different D-I team's roster this season. |
| `portal_share` | Float64 | Incoming D-I transfers as a share of the roster (transfers_in_n / roster_n); JUCO and non-D-I arrivals are not counted. |
| `transfer_talent_in` | Float64 | Recruit-star talent points summed over incoming transfers, name-joined to cfb_recruits (unmatched players count the 0-star default). |
| `transfer_talent_out` | Float64 | Recruit-star talent points summed over outgoing transfers, name-joined to cfb_recruits (unmatched players count the 0-star default). |
| `net_transfer_talent` | Float64 | Incoming minus outgoing transfer talent points (transfer_talent_in - transfer_talent_out); positive means the team gained talent. |

```python
load_cfb_team_portal(seasons=2024)
```

## load_cfb_team_talent

Release: [cfb_team_talent](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_team_talent) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_team_talent/cfb_team_talent_{season}.parquet`
### Returns {#load_cfb_team_talent-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (4-digit year). |
| `team_id` | Int64 | ESPN team id. |
| `team` | String | Team name. |
| `talent_composite` | Float64 | Sum of composite recruiting ratings across the counted signing classes (the Team Talent measure). |
| `talent_rank` | Int64 | Dense rank of talent_composite within the season (1 = most talented roster). |
| `blue_chip_ratio` | Float64 | Share of the team's counted signees rated four or five stars over the rolling class window. |
| `n_recruits` | Int64 | Number of signees counted in the blue-chip-ratio window. |

```python
load_cfb_team_talent(seasons=2024)
```

## load_cfb_teams_crosswalk

Release: [cfb_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_crosswalk) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_crosswalk/cfb_teams_crosswalk_{season}.parquet`
### Returns {#load_cfb_teams_crosswalk-returns}

| col_name | type | description |
|---|---|---|
| `norm_key` | String | Shared join key across providers: the team name lowercased, ASCII-folded, stripped of punctuation, whitespace-collapsed, and alias-mapped. |
| `espn_team_id` | Int64 | ESPN team id for the crosswalk row. |
| `espn_team` | String | ESPN's full team display name, school plus mascot, null when the row was anchored on a non-ESPN provider. |
| `espn_abbreviation` | String | ESPN abbreviation. |
| `fox_team_id` | String | Fox Sports team id for the same team. |
| `fox_team` | String | Fox Sports' team name, which that feed ships in all capitals. |
| `fox_abbreviation` | String | Fox Sports' short team code, which frequently differs from the ESPN abbreviation for the same school. |
| `yahoo_team_id` | String | Yahoo Sports team id for the same team. |
| `yahoo_team` | String | Yahoo Sports' team display name, school plus mascot. |
| `yahoo_abbreviation` | String | Yahoo Sports' short team code for the school. |
| `matched_sources` | String | Plus-joined provenance tag naming which of espn, fox, and yahoo contributed a directory row for this team. |

```python
load_cfb_teams_crosswalk(seasons=2024)
```

## load_cfb_team_box

Release: [espn_cfb_team_box](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_team_box) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_team_box/team_box_{season}.parquet`
### Returns {#load_cfb_team_box-returns}

| col_name | type | description |
|---|---|---|
| `firstDowns` | String | Total first downs ESPN credits the team, carried verbatim from the box score as a string. |
| `thirdDownEff` | String | Third-down efficiency as ESPN's conversions-attempts string, for example 5-15. |
| `fourthDownEff` | String | Fourth-down efficiency as a conversions-attempts string, for example 3-4. |
| `totalYards` | String | Total offensive yards for the team, matching rushingYards plus netPassingYards in about 99.8 percent of games. |
| `netPassingYards` | String | Passing yards after yardage lost to sacks is deducted, the numerator behind yardsPerPass. |
| `completionAttempts` | String | Completions and pass attempts as a slash-separated string, for example 23/41. |
| `yardsPerPass` | String | Net passing yards per pass attempt, netPassingYards divided by the attempt count in completionAttempts and rounded to one decimal. |
| `rushingYards` | String | Net rushing yards gained. |
| `rushingAttempts` | String | Rushing attempts. |
| `yardsPerRushAttempt` | String | Yards gained per rushing attempt. |
| `totalPenaltiesYards` | String | Penalties and penalty yards as a hyphen-separated string, for example 7-64. |
| `turnovers` | String | Turnovers total. |
| `fumblesLost` | String | Number of fumbles the team lost to the opponent, carried as a string. |
| `interceptions` | String | Passing interceptions. |
| `possessionTime` | String | Time of possession as mm:ss; the two teams' values add up to 60 minutes in a regulation game. |
| `team_id` | Int64 | ESPN team id. |
| `team_abbreviation` | String | Team abbreviation. |
| `team_name` | String | Team nickname. |
| `home_away` | String | `home` or `away`. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |

```python
load_cfb_team_box(seasons=2024)
```
