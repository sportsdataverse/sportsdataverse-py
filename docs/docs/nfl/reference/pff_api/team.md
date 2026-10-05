---
title: "NFL — PFF Developer API (api.pff.com, API key) — Team: list–schedule"
sidebar_label: "Team: list–schedule"
sidebar_position: 14
description: "NFL — PFF Developer API (api.pff.com, API key) — Team: list–schedule — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — PFF Developer API (api.pff.com, API key) — Team: list–schedule

## pff_api_team_list

List a season's teams, franchise groups and schedule

**Endpoint URL:** `GET https://api.pff.com/v1/teams`

**Valid URL:** [https://api.pff.com/v1/teams?league=nfl&season=2022](https://api.pff.com/v1/teams?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |

### Returns {#pff_api_team_list-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `heirarchy` | list | Nested hierarchy of franchise groupings as returned by the PFF API; the field name's spelling follows the source. |
| `id` | numeric | PFF id of the conference, division or tier group (e.g. 1 = AFC, 2 = AFC East). |
| `name` | character | Group name (e.g. "AFC", "AFC East"; NCAA groups such as "FBS" or "The American"). |
| `slug` | character | URL-style slug of the group (e.g. "afc-east"). |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_team_list-example}

```python
pff_api_team_list(league='nfl', season=2022)
```

_Last validated n/a._

## pff_api_team_overview

Season-to-date team report, one row per team

**Endpoint URL:** `GET https://api.pff.com/v1/teams/overview`

**Valid URL:** [https://api.pff.com/v1/teams/overview?league=nfl&season=2022](https://api.pff.com/v1/teams/overview?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, as a four-digit year, taken as a positional argument. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |

### Returns {#pff_api_team_overview-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `abbreviation` | character | Team abbreviation. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `grades_coverage_defense` | numeric | PFF team coverage grade (0-100). |
| `grades_defense` | numeric | PFF team defense grade (0-100). |
| `grades_misc_st` | numeric | Team-level PFF miscellaneous special-teams grade, 0-100. |
| `grades_offense` | numeric | PFF team offense grade (0-100). |
| `grades_overall` | numeric | Team-level PFF overall grade, 0-100. |
| `grades_pass` | numeric | Team-level PFF passing grade, 0-100. |
| `grades_pass_block` | numeric | Team-level PFF pass-blocking grade, 0-100. |
| `grades_pass_route` | numeric | Team-level PFF receiving (route-running) grade, 0-100. |
| `grades_pass_rush_defense` | numeric | Team-level PFF pass-rush grade, 0-100. |
| `grades_run` | numeric | Team-level PFF rushing grade, 0-100. |
| `grades_run_block` | numeric | Team-level PFF run-blocking grade, 0-100. |
| `grades_run_defense` | numeric | Team-level PFF run-defense grade, 0-100. |
| `grades_tackle` | numeric | Team-level PFF tackling grade, 0-100. |
| `losses` | numeric | Games the team lost over the covered span. |
| `name` | character | Full team name (e.g. "Arizona Cardinals"). |
| `points_allowed` | numeric | Total points allowed by the team over the covered span. |
| `points_scored` | numeric | Total points scored by the team over the covered span. |
| `ties` | numeric | Games the team tied over the covered span. |
| `wins` | numeric | Games the team won over the covered span. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_team_overview-example}

```python
pff_api_team_overview(league='nfl', season=2022)
```

_Last validated n/a._

## pff_api_team_summary

Per-game team report for one franchise, one row per game

**Endpoint URL:** `GET https://api.pff.com/v1/teams/summary`

**Valid URL:** [https://api.pff.com/v1/teams/summary?league=nfl&season=2022&franchise_id=7](https://api.pff.com/v1/teams/summary?league=nfl&season=2022&franchise_id=7)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, as a four-digit year, taken as a positional argument. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  | `Y` |  | Franchise (team) id, and the THIRD positional argument of team-summary — the report is franchise-scoped, so there is no all-teams form of it. |

### Returns {#pff_api_team_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `away` | logical | True when the team was the away side in the game. |
| `franchise_id` | integer | PFF franchise (team) id of the team the row belongs to (the franchise_id requested). |
| `game_id` | integer | PFF game id of the game (the value the game_id query parameter takes). |
| `grades_coverage_defense` | numeric | PFF team coverage grade for the game (0-100). |
| `grades_defense` | numeric | PFF team defense grade for the game (0-100). |
| `grades_misc_st` | numeric | PFF team miscellaneous special-teams grade for the game (0-100). |
| `grades_offense` | numeric | PFF team offense grade for the game (0-100). |
| `grades_overall` | numeric | PFF overall team grade for the game (0-100). |
| `grades_pass` | numeric | PFF team passing grade for the game (0-100). |
| `grades_pass_block` | numeric | PFF team pass-blocking grade for the game (0-100). |
| `grades_pass_route` | numeric | PFF team receiving (route-running) grade for the game (0-100). |
| `grades_pass_rush_defense` | numeric | PFF team pass-rush grade for the game (0-100). |
| `grades_run` | numeric | PFF team rushing grade for the game (0-100). |
| `grades_run_block` | numeric | PFF team run-blocking grade for the game (0-100). |
| `grades_run_defense` | numeric | PFF team run-defense grade for the game (0-100). |
| `grades_tackle` | numeric | PFF team tackling grade for the game (0-100). |
| `home` | logical | True when the team was the home side in the game. |
| `lock_status` | character | Charting state of the game: "processed" means PFF has fully charted it. |
| `opponent` | character | Opponent's team abbreviation (e.g. "PIT"). |
| `opponent_franchise_id` | integer | PFF franchise id of the opponent. |
| `points_allowed` | integer | Points the team allowed in the game. |
| `points_scored` | integer | Points the team scored in the game. |
| `start` | character | Kickoff timestamp of the game in UTC (e.g. "2022-09-11T17:00:00Z"). |
| `week` | integer | Week id of the game, as PFF numbers weeks. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_team_summary-example}

```python
pff_api_team_summary(franchise_id=7, league='nfl', season=2022)
```

_Last validated n/a._

## pff_api_team_directory

The league's teams for a season, with ids, slugs, colours and groups

**Endpoint URL:** `GET https://api.pff.com/v2/{league}/teams`

**Valid URL:** [https://api.pff.com/v2/nfl/teams?season=2022](https://api.pff.com/v2/nfl/teams?season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug: nfl or ncaa. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year: 2006 or later, and at most one year past the current season. |

### Returns {#pff_api_team_directory-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `franchise_id` | integer | PFF franchise (team) id, stable across seasons; the value franchise_id / teamId carry everywhere else. |
| `slug` | character | URL slug PFF uses for the team in /v2 team routes (e.g. "arizona-cardinals"); pass it as the team path parameter. |
| `name` | character | Full team name (e.g. "Arizona Cardinals"). |
| `city` | character | City or place name PFF lists for the team (e.g. "Arizona"); null when PFF has none. |
| `nickname` | character | Team nickname (e.g. "Cardinals"); null when PFF has none. |
| `abbreviation` | character | Short display code for the team (e.g. "ARZ"): the directory's short code when it has one, else PFF's abbreviation; null when neither exists. |
| `primary_color` | character | Team primary colour as a hex string (e.g. "#97233f"); null when PFF has no colours for the team. |
| `secondary_color` | character | Team secondary colour as a hex string (e.g. "#ffffff"); null when PFF has no colours for the team. |
| `group_ids` | character | Ids of the conference, division or tier groups the team belongs to, joined with ";" (e.g. "6;10"); they resolve against the groups list in the raw response. Empty string for none. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_team_directory-example}

```python
pff_api_team_directory(league='nfl', season=2022)
```

_Last validated n/a._

## pff_api_team_stats

Team stats table for one category, every value ranked against the scope

**Endpoint URL:** `GET https://api.pff.com/v2/{league}/teams/stats`

**Valid URL:** [https://api.pff.com/v2/nfl/teams/stats?season=2022](https://api.pff.com/v2/nfl/teams/stats?season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug: nfl or ncaa. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year: 2006 or later, and at most one year past the current season. |
| `weekGroup` | `week_group` |  |  | `Y` | Which part of the season to cover: REG (regular season), PO (playoffs) or REGPO (both, the default). |
| `weekIds` | `week_ids` |  |  | `Y` | Comma-separated week ids to cover instead of a whole weekGroup — 1,2,3 is the first three regular-season weeks. |
| `category` | `category` |  |  | `Y` | Which stat category the table covers. |
| `scope` | `scope` |  |  | `Y` | Which teams the ranks are computed against — and which rows come back: league (every team, the default), a conference (afc, nfc) or a division (afc-east … nfc-west). |

### Returns {#pff_api_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
**offense-overall-success**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | PFF franchise (team) id, stable across seasons; the franchise_id used across the PFF Developer API. |
| `abbreviation` | character | Short team code, as the scoreboard prints it (e.g. "ARI"). |
| `name` | character | Full team name (e.g. "Arizona Cardinals"). |
| `epa_per_play` | numeric | Expected points added per offensive play (EPA / play). |
| `epa_per_play_rank` | integer | Rank of the team's EPA per play among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `success_rate` | numeric | Share of the offense's plays that were successful, as a fraction (0-1). |
| `success_rate_rank` | integer | Rank of the team's success rate among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `explosive_play_rate` | numeric | Share of the offense's plays that were explosive plays, as a fraction (0-1). |
| `explosive_play_rate_rank` | integer | Rank of the team's explosive-play rate among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `points_per_drive` | numeric | Points scored per offensive drive. |
| `points_per_drive_rank` | integer | Rank of the team's points per drive among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `third_down_conversion_rate` | numeric | Share of third downs the offense converted, as a fraction (0-1). |
| `third_down_conversion_rate_rank` | integer | Rank of the team's third-down conversion rate among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `red_zone_conversion_rate` | numeric | Red-zone conversion rate of the offense, as a fraction (0-1). |
| `red_zone_conversion_rate_rank` | integer | Rank of the team's red-zone conversion rate among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `offensive_turnovers` | integer | Turnovers committed by the offense. |
| `offensive_turnovers_rank` | integer | Rank of the team's offensive turnovers among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `wepa` | numeric | Weighted EPA per offensive play, PFF's 'Weighted EPA / play'. |
| `wepa_rank` | integer | Rank of the team's weighted EPA per play among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `conversion_after4` | numeric | Series conversion rate: share of the offense's series that produced a new first down or a touchdown, as a fraction (0-1). |
| `conversion_after4_rank` | integer | Rank of the team's series conversion rate among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |

**offense-passing**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | PFF franchise (team) id, stable across seasons; the franchise_id used across the PFF Developer API. |
| `abbreviation` | character | Short team code, as the scoreboard prints it (e.g. "ARI"). |
| `name` | character | Full team name (e.g. "Arizona Cardinals"). |
| `epa_per_pass_play` | numeric | Expected points added per pass play. |
| `epa_per_pass_play_rank` | integer | Rank of the team's EPA per pass play among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `pass_success_rate` | numeric | Share of the offense's pass plays that were successful, as a fraction (0-1). |
| `pass_success_rate_rank` | integer | Rank of the team's passing success rate among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `yards_per_attempt` | numeric | Passing yards per pass attempt. |
| `yards_per_attempt_rank` | integer | Rank of the team's yards per pass attempt among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `completion_percentage` | numeric | Completion percentage of the offense's passes, as a fraction (0-1). |
| `completion_percentage_rank` | integer | Rank of the team's completion percentage among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `explosive_pass_rate` | numeric | Share of the offense's pass plays that were explosive, as a fraction (0-1). |
| `explosive_pass_rate_rank` | integer | Rank of the team's explosive-pass rate among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `passer_rating` | numeric | Passer rating on the offense's pass attempts (NFL passer-rating scale). |
| `passer_rating_rank` | integer | Rank of the team's passer rating among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `pressure_rate_against` | numeric | Share of the offense's dropbacks on which the passer was pressured, as a fraction (0-1). |
| `pressure_rate_against_rank` | integer | Rank of the team's pressure rate against among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `pressure_oe_allowed` | numeric | Pressure rate allowed over expectation: the offense's pressure rate against minus PFF's expected rate, as a fraction (negative = fewer pressures than expected). |
| `pressure_oe_allowed_rank` | integer | Rank of the team's pressure rate over expectation against among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `sack_rate_against` | numeric | Share of the offense's dropbacks that ended in a sack, as a fraction (0-1). |
| `sack_rate_against_rank` | integer | Rank of the team's sack rate against among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `man_coverage_pct_against` | numeric | Share of the offense's pass plays that faced man coverage, as a fraction (0-1). |
| `man_coverage_pct_against_rank` | integer | Rank of the team's share of pass plays facing man coverage among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `zone_coverage_pct_against` | numeric | Share of the offense's pass plays that faced zone coverage, as a fraction (0-1). |
| `zone_coverage_pct_against_rank` | integer | Rank of the team's share of pass plays facing zone coverage among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `scramble_rushing_yards` | integer | Rushing yards gained on quarterback scrambles. |
| `scramble_rushing_yards_rank` | integer | Rank of the team's scramble rushing yards among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `scramble_rushing_touchdowns` | integer | Rushing touchdowns scored on quarterback scrambles. |
| `scramble_rushing_touchdowns_rank` | integer | Rank of the team's scramble rushing touchdowns among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `scramble_yards_per_carry` | numeric | Rushing yards per quarterback scramble. |
| `scramble_yards_per_carry_rank` | integer | Rank of the team's yards per scramble among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `pass_play_pct` | numeric | Share of the offense's plays that were pass plays, as a fraction (0-1). |
| `pass_play_pct_rank` | integer | Rank of the team's pass-play share among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `is_pass_oe` | numeric | Pass rate over expectation: the offense's pass rate minus PFF's expected pass rate, as a fraction (positive = passed more than expected). |
| `is_pass_oe_rank` | integer | Rank of the team's pass rate over expectation among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `play_action_rate` | numeric | Share of the offense's pass plays that used play action, as a fraction (0-1). |
| `play_action_rate_rank` | integer | Rank of the team's play-action rate among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `adot` | numeric | Average depth of target, in air yards, of the offense's targeted passes. |
| `adot_rank` | integer | Rank of the team's average depth of target among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `screen_rate` | numeric | Share of the offense's pass plays that were screens, as a fraction (0-1). |
| `screen_rate_rank` | integer | Rank of the team's screen rate among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `time_to_throw` | numeric | Average time to throw, in seconds from snap to release, on the offense's passes. |
| `time_to_throw_rank` | integer | Rank of the team's average time to throw among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `passes_behind_los_pct` | numeric | Share of the offense's passes thrown behind the line of scrimmage, as a fraction (0-1). |
| `passes_behind_los_pct_rank` | integer | Rank of the team's share of passes thrown behind the line of scrimmage among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `passes_short_pct` | numeric | Share of the offense's passes thrown short (0-9 air yards), as a fraction (0-1). |
| `passes_short_pct_rank` | integer | Rank of the team's share of short passes among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `passes_intermediate_pct` | numeric | Share of the offense's passes thrown to intermediate depth (10-19 air yards), as a fraction (0-1). |
| `passes_intermediate_pct_rank` | integer | Rank of the team's share of intermediate passes among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `passes_deep_pct` | numeric | Share of the offense's passes thrown deep (20+ air yards), as a fraction (0-1). |
| `passes_deep_pct_rank` | integer | Rank of the team's share of deep passes among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `target_share_wr` | numeric | Share of the team's targets that went to wide receivers, as a fraction (0-1). |
| `target_share_wr_rank` | integer | Rank of the team's wide-receiver target share among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `target_share_te` | numeric | Share of the team's targets that went to tight ends, as a fraction (0-1). |
| `target_share_te_rank` | integer | Rank of the team's tight-end target share among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `target_share_rb` | numeric | Share of the team's targets that went to running backs, as a fraction (0-1). |
| `target_share_rb_rank` | integer | Rank of the team's running-back target share among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |

**offense-rushing**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | PFF franchise (team) id, stable across seasons; the franchise_id used across the PFF Developer API. |
| `abbreviation` | character | Short team code, as the scoreboard prints it (e.g. "ARI"). |
| `name` | character | Full team name (e.g. "Arizona Cardinals"). |
| `epa_per_run_play` | numeric | Expected points added per run play. |
| `epa_per_run_play_rank` | integer | Rank of the team's EPA per run play among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `rush_success_rate` | numeric | Share of the offense's run plays that were successful, as a fraction (0-1). |
| `rush_success_rate_rank` | integer | Rank of the team's rushing success rate among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `explosive_run_rate` | numeric | Share of the offense's run plays that were explosive, as a fraction (0-1). |
| `explosive_run_rate_rank` | integer | Rank of the team's explosive-run rate among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `designed_rushing_yards` | integer | Rushing yards on designed runs (quarterback scrambles excluded). |
| `designed_rushing_yards_rank` | integer | Rank of the team's designed rushing yards among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `designed_rushing_touchdowns` | integer | Rushing touchdowns on designed runs. |
| `designed_rushing_touchdowns_rank` | integer | Rank of the team's designed rushing touchdowns among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `designed_yards_per_carry` | numeric | Rushing yards per carry on designed runs. |
| `designed_yards_per_carry_rank` | integer | Rank of the team's designed-run yards per carry among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `yards_after_contact_per_carry` | numeric | Rushing yards gained after first contact, per carry. |
| `yards_after_contact_per_carry_rank` | integer | Rank of the team's yards after contact per carry among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `yards_before_contact_per_carry` | numeric | Rushing yards gained before first contact, per carry. |
| `yards_before_contact_per_carry_rank` | integer | Rank of the team's yards before contact per carry among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `run_stuff_rate` | numeric | Share of the offense's runs that were stuffed (tackled for a loss or no gain), as a fraction (0-1). |
| `run_stuff_rate_rank` | integer | Rank of the team's run stuff rate among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `run_play_pct` | numeric | Share of the offense's plays that were runs, as a fraction (0-1). |
| `run_play_pct_rank` | integer | Rank of the team's run-play share among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `run_rate_oe` | numeric | Run rate over expectation: the offense's run rate minus PFF's expected run rate, as a fraction (positive = ran more than expected). |
| `run_rate_oe_rank` | integer | Rank of the team's run rate over expectation among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `zone_run_pct` | numeric | Share of the offense's runs using a zone blocking scheme, as a fraction (0-1). |
| `zone_run_pct_rank` | integer | Rank of the team's zone-run share among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `gap_run_pct` | numeric | Share of the offense's runs using a gap blocking scheme, as a fraction (0-1). |
| `gap_run_pct_rank` | integer | Rank of the team's gap-run share among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `inside_zone_rate` | numeric | Share of the offense's runs using the inside-zone concept, as a fraction (0-1). |
| `inside_zone_rate_rank` | integer | Rank of the team's inside-zone rate among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `outside_zone_rate` | numeric | Share of the offense's runs using the outside-zone concept, as a fraction (0-1). |
| `outside_zone_rate_rank` | integer | Rank of the team's outside-zone rate among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `man_duo_rate` | numeric | Share of the offense's runs using the man (duo) concept, as a fraction (0-1). |
| `man_duo_rate_rank` | integer | Rank of the team's man (duo) rate among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `power_rate` | numeric | Share of the offense's runs using the power concept, as a fraction (0-1). |
| `power_rate_rank` | integer | Rank of the team's power rate among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `counter_rate` | numeric | Share of the offense's runs using the counter concept, as a fraction (0-1). |
| `counter_rate_rank` | integer | Rank of the team's counter rate among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `pull_lead_rate` | numeric | Share of the offense's runs using the pull-lead (pin-pull) concept, as a fraction (0-1). |
| `pull_lead_rate_rank` | integer | Rank of the team's pull-lead (pin-pull) rate among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |

**defense-overall-success**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | PFF franchise (team) id, stable across seasons; the franchise_id used across the PFF Developer API. |
| `abbreviation` | character | Short team code, as the scoreboard prints it (e.g. "ARI"). |
| `name` | character | Full team name (e.g. "Arizona Cardinals"). |
| `epa_per_play_allowed` | numeric | Expected points added per play allowed by the defense. |
| `epa_per_play_allowed_rank` | integer | Rank of the team's EPA per play allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `success_rate_allowed` | numeric | Share of opponent plays that were successful, as a fraction (0-1). |
| `success_rate_allowed_rank` | integer | Rank of the team's success rate allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `explosive_play_rate_allowed` | numeric | Share of opponent plays that were explosive plays, as a fraction (0-1). |
| `explosive_play_rate_allowed_rank` | integer | Rank of the team's explosive-play rate allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `red_zone_conversion_rate_allowed` | numeric | Red-zone conversion rate allowed to opponents, as a fraction (0-1). |
| `red_zone_conversion_rate_allowed_rank` | integer | Rank of the team's red-zone conversion rate allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `third_down_conversion_rate_allowed` | numeric | Share of opponent third downs converted, as a fraction (0-1). |
| `third_down_conversion_rate_allowed_rank` | integer | Rank of the team's third-down conversion rate allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `defensive_turnovers` | integer | Turnovers forced by the defense. |
| `defensive_turnovers_rank` | integer | Rank of the team's defensive turnovers among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `points_per_drive_allowed` | numeric | Points allowed per opponent drive. |
| `points_per_drive_allowed_rank` | integer | Rank of the team's points per drive allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `missed_tackle_rate` | numeric | Missed-tackle rate of the defense, as a fraction (0-1). |
| `missed_tackle_rate_rank` | integer | Rank of the team's missed-tackle rate among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `wepa_allowed` | numeric | Weighted EPA per play allowed, PFF's 'Weighted EPA / play allowed'. |
| `wepa_allowed_rank` | integer | Rank of the team's weighted EPA per play allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `conversion_after4_allowed` | numeric | Series conversion rate allowed: share of opponent series that produced a new first down or a touchdown, as a fraction (0-1). |
| `conversion_after4_allowed_rank` | integer | Rank of the team's series conversion rate allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |

**defense-passing**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | PFF franchise (team) id, stable across seasons; the franchise_id used across the PFF Developer API. |
| `abbreviation` | character | Short team code, as the scoreboard prints it (e.g. "ARI"). |
| `name` | character | Full team name (e.g. "Arizona Cardinals"). |
| `epa_per_pass_play_allowed` | numeric | Expected points added per opponent pass play. |
| `epa_per_pass_play_allowed_rank` | integer | Rank of the team's EPA per pass play allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `pass_success_rate_allowed` | numeric | Share of opponent pass plays that were successful, as a fraction (0-1). |
| `pass_success_rate_allowed_rank` | integer | Rank of the team's passing success rate allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `explosive_pass_rate_allowed` | numeric | Share of opponent pass plays that were explosive, as a fraction (0-1). |
| `explosive_pass_rate_allowed_rank` | integer | Rank of the team's explosive-pass rate allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `yards_per_attempt_allowed` | numeric | Passing yards allowed per opponent pass attempt. |
| `yards_per_attempt_allowed_rank` | integer | Rank of the team's yards per pass attempt allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `completion_percentage_allowed` | numeric | Completion percentage allowed to opposing passers, as a fraction (0-1). |
| `completion_percentage_allowed_rank` | integer | Rank of the team's completion percentage allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `passer_rating_allowed` | numeric | Passer rating allowed to opposing passers (NFL passer-rating scale). |
| `passer_rating_allowed_rank` | integer | Rank of the team's passer rating allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `passing_touchdowns_allowed` | integer | Passing touchdowns allowed. |
| `passing_touchdowns_allowed_rank` | integer | Rank of the team's passing touchdowns allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `interceptions` | integer | Interceptions made by the defense. |
| `interceptions_rank` | integer | Rank of the team's interceptions among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `air_yard_pct_allowed` | numeric | Share of passing yards allowed that came through the air rather than after the catch, as a fraction (0-1); complements yac_pct_allowed. |
| `air_yard_pct_allowed_rank` | integer | Rank of the team's air-yard share of passing yards allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `yac_pct_allowed` | numeric | Share of passing yards allowed that came after the catch, as a fraction (0-1); complements air_yard_pct_allowed. |
| `yac_pct_allowed_rank` | integer | Rank of the team's yards-after-catch share of passing yards allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `yac_per_reception_allowed` | numeric | Yards after the catch allowed per reception. |
| `yac_per_reception_allowed_rank` | integer | Rank of the team's yards after catch allowed per reception among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `pressure_rate` | numeric | Share of opponent dropbacks on which the defense generated pressure, as a fraction (0-1). |
| `pressure_rate_rank` | integer | Rank of the team's pressure rate among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `pressure_oe` | numeric | Pressure rate over expectation: the defense's pressure rate minus PFF's expected rate, as a fraction (positive = more pressure than expected). |
| `pressure_oe_rank` | integer | Rank of the team's pressure rate over expectation among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `total_pressures` | integer | Total quarterback pressures generated by the defense. |
| `total_pressures_rank` | integer | Rank of the team's total pressures among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `sack_rate` | numeric | Share of opponent dropbacks that ended in a sack, as a fraction (0-1). |
| `sack_rate_rank` | integer | Rank of the team's sack rate among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `sacks` | integer | Sacks recorded by the defense. |
| `sacks_rank` | integer | Rank of the team's sacks among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `scramble_rushing_yards_allowed` | integer | Rushing yards allowed on opponent quarterback scrambles. |
| `scramble_rushing_yards_allowed_rank` | integer | Rank of the team's scramble rushing yards allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `scramble_rushing_touchdowns_allowed` | integer | Rushing touchdowns allowed on opponent quarterback scrambles. |
| `scramble_rushing_touchdowns_allowed_rank` | integer | Rank of the team's scramble rushing touchdowns allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `scramble_yards_per_carry_allowed` | numeric | Rushing yards allowed per opponent quarterback scramble. |
| `scramble_yards_per_carry_allowed_rank` | integer | Rank of the team's yards per scramble allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `man_coverage_pct` | numeric | Share of opponent pass plays on which the defense played man coverage, as a fraction (0-1). |
| `man_coverage_pct_rank` | integer | Rank of the team's man-coverage share among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `zone_coverage_pct` | numeric | Share of opponent pass plays on which the defense played zone coverage, as a fraction (0-1). |
| `zone_coverage_pct_rank` | integer | Rank of the team's zone-coverage share among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |

**defense-rushing**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | PFF franchise (team) id, stable across seasons; the franchise_id used across the PFF Developer API. |
| `abbreviation` | character | Short team code, as the scoreboard prints it (e.g. "ARI"). |
| `name` | character | Full team name (e.g. "Arizona Cardinals"). |
| `epa_per_run_play_allowed` | numeric | Expected points added per opponent run play. |
| `epa_per_run_play_allowed_rank` | integer | Rank of the team's EPA per run play allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `rush_success_rate_allowed` | numeric | Share of opponent run plays that were successful, as a fraction (0-1). |
| `rush_success_rate_allowed_rank` | integer | Rank of the team's rushing success rate allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `explosive_run_rate_allowed` | numeric | Share of opponent run plays that were explosive, as a fraction (0-1). |
| `explosive_run_rate_allowed_rank` | integer | Rank of the team's explosive-run rate allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `designed_rushing_yards_allowed` | integer | Rushing yards allowed on opponent designed runs. |
| `designed_rushing_yards_allowed_rank` | integer | Rank of the team's designed rushing yards allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `designed_rushing_touchdowns_allowed` | integer | Rushing touchdowns allowed on opponent designed runs. |
| `designed_rushing_touchdowns_allowed_rank` | integer | Rank of the team's designed rushing touchdowns allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `designed_yards_per_carry_allowed` | numeric | Rushing yards allowed per carry on opponent designed runs. |
| `designed_yards_per_carry_allowed_rank` | integer | Rank of the team's designed-run yards per carry allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `yards_after_contact_per_carry_allowed` | numeric | Rushing yards allowed after first contact, per opponent carry. |
| `yards_after_contact_per_carry_allowed_rank` | integer | Rank of the team's yards after contact per carry allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `yards_before_contact_per_carry_allowed` | numeric | Rushing yards allowed before first contact, per opponent carry. |
| `yards_before_contact_per_carry_allowed_rank` | integer | Rank of the team's yards before contact per carry allowed among the teams in the requested scope (league, conference or division); 1 = lowest, tied teams share a rank. |
| `stuff_rate` | numeric | Share of opponent runs the defense stuffed (tackle for loss or no gain), as a fraction (0-1). |
| `stuff_rate_rank` | integer | Rank of the team's stuff rate among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |

**defense-opponent-tendencies**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | PFF franchise (team) id, stable across seasons; the franchise_id used across the PFF Developer API. |
| `abbreviation` | character | Short team code, as the scoreboard prints it (e.g. "ARI"). |
| `name` | character | Full team name (e.g. "Arizona Cardinals"). |
| `play_action_rate_against` | numeric | Share of opponent pass plays against this defense that used play action, as a fraction (0-1). |
| `play_action_rate_against_rank` | integer | Rank of the team's play-action rate against among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `screen_rate_against` | numeric | Share of opponent pass plays against this defense that were screens, as a fraction (0-1). |
| `screen_rate_against_rank` | integer | Rank of the team's screen rate against among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `adot_against` | numeric | Average depth of target, in air yards, of opponent passes against this defense. |
| `adot_against_rank` | integer | Rank of the team's average depth of target against among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `time_to_throw_against` | numeric | Average time to throw, in seconds, of opposing passers against this defense. |
| `time_to_throw_against_rank` | integer | Rank of the team's average time to throw against among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `zone_run_pct_against` | numeric | Share of opponent runs against this defense using a zone blocking scheme, as a fraction (0-1). |
| `zone_run_pct_against_rank` | integer | Rank of the team's zone-run share against among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `gap_run_pct_against` | numeric | Share of opponent runs against this defense using a gap blocking scheme, as a fraction (0-1). |
| `gap_run_pct_against_rank` | integer | Rank of the team's gap-run share against among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `pass_rate_against` | numeric | Share of opponent plays against this defense that were passes, as a fraction (0-1). |
| `pass_rate_against_rank` | integer | Rank of the team's pass rate against among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |
| `run_rate_against` | numeric | Share of opponent plays against this defense that were runs, as a fraction (0-1). |
| `run_rate_against_rank` | integer | Rank of the team's run rate against among the teams in the requested scope (league, conference or division); 1 = highest, tied teams share a rank. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_team_stats-example}

```python
pff_api_team_stats(league='nfl', season=2022)
```

_Last validated n/a._

## pff_api_team_roster

A team's depth-chart roster with grades, ranks and snap counts

**Endpoint URL:** `GET https://api.pff.com/v2/{league}/teams/{team}/roster`

**Valid URL:** [https://api.pff.com/v2/nfl/teams/cincinnati-bengals/roster?season=2022](https://api.pff.com/v2/nfl/teams/cincinnati-bengals/roster?season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug: nfl or ncaa. |
| `team` | `team` |  | `Y` |  | The team, as its slug (los-angeles-rams) or its numeric franchise id (26) — both resolve through the league's team directory for the season, and both produce the same answer. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year: 2006 or later, and at most one year past the current season. |

### Returns {#pff_api_team_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `name` | character | Player's display name as PFF lists it (e.g. "Joe Burrow"). |
| `jersey` | character | Jersey number as a string (e.g. "9"); null when PFF lists none. |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `alignment` | character | Depth-chart alignment PFF lists the player at (e.g. "QB", "HB"); rows come in depth-chart order. |
| `unit` | character | Depth-chart unit of the row: offense, defense or special-teams. |
| `depth_order` | integer | Player's order on the depth chart at his alignment (1 = first on the depth chart). |
| `grade` | numeric | Player's PFF season grade (0-100) for the requested season; null when he has no graded snaps. |
| `grade_rank` | integer | Rank of the player's PFF grade within the pool PFF ranks him in (1 = highest grade; pool size in grade_rank_of). |
| `grade_rank_of` | integer | Size of the ranked pool behind grade_rank (PFF's 'Grade rank of'); null when the player is unranked. |
| `height` | integer | Player height packed as feet x 100 + inches (e.g. 604 = 6 ft 4 in); null when PFF has none. |
| `weight` | integer | Player weight in pounds; null when PFF has none. |
| `birth_date` | character | Player's date of birth (YYYY-MM-DD). |
| `eligibility_year` | integer | Eligibility year PFF lists for the player; on the NFL rows captured it is the season the player became draft-eligible (e.g. 2020). |
| `status` | character | Player availability status: "active", "questionable" or "out". |
| `snap_counts` | integer | Snaps the player played for the team in the requested season; null when he played none. |
| `snap_pct` | numeric | Player's snap share in percent (e.g. 98.8), PFF's 'Snap %'; null when he played no snaps. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_team_roster-example}

```python
pff_api_team_roster(league='nfl', season=2022, team='cincinnati-bengals')
```

_Last validated n/a._

## pff_api_team_schedule

A team's season schedule, with results and strength of schedule

**Endpoint URL:** `GET https://api.pff.com/v2/{league}/teams/{team}/schedule`

**Valid URL:** [https://api.pff.com/v2/nfl/teams/cincinnati-bengals/schedule?season=2022](https://api.pff.com/v2/nfl/teams/cincinnati-bengals/schedule?season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug: nfl or ncaa. |
| `team` | `team` |  | `Y` |  | The team, as its slug (los-angeles-rams) or its numeric franchise id (26) — both resolve through the league's team directory for the season, and both produce the same answer. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year: 2006 or later, and at most one year past the current season. |

### Returns {#pff_api_team_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `week` | integer | Week number of the row within the season, as PFF numbers weeks (see week_label for the display label). |
| `week_label` | character | Display label PFF shows for the week (e.g. "1"). |
| `is_bye` | logical | True when the week is the team's bye; a bye row carries null in every game field. |
| `game_id` | integer | PFF game id (the value the game_id query parameter takes); null on a bye. |
| `kickoff` | character | Scheduled kickoff in UTC, ISO 8601 (e.g. "2022-09-11T17:00:00.000Z"); null on a bye. |
| `home_away` | character | Whether the team was home or away: "home" or "away"; null on a bye. |
| `opponent_franchise_id` | integer | PFF franchise id of the opponent; null on a bye. |
| `opponent_abbr` | character | Opponent's short team code (e.g. "PIT"). |
| `opponent_name` | character | Opponent's full team name (e.g. "Pittsburgh Steelers"). |
| `opponent_division` | character | Division or conference the opponent belongs to (e.g. "AFC North"). |
| `opponent_wins` | integer | Opponent's wins entering the game (its record before kickoff). |
| `opponent_losses` | integer | Opponent's losses entering the game (its record before kickoff). |
| `result` | character | Game result for the team: W, L or T; null for a bye or a game not yet played. |
| `team_score` | integer | Points the team scored in the game; null for a bye or a game not yet played. |
| `opponent_score` | integer | Points the opponent scored in the game; null for a bye or a game not yet played. |
| `sos_score` | numeric | PFF strength-of-schedule score for this game ('SOS score'); in the captured rows it rises with opponent_elo (higher = tougher game). The season summary sits in the response's sos block. |
| `opponent_elo` | integer | Opponent's PFF Elo rating (e.g. 1486). |
| `opponent_elo_rank` | integer | Opponent's rank by PFF Elo within the league (1 = highest Elo). |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_team_schedule-example}

```python
pff_api_team_schedule(league='nfl', season=2022, team='cincinnati-bengals')
```

_Last validated n/a._
