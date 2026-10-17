---
title: "VTB — VTB United League Infobasket (Russia) — Team"
sidebar_label: "Team"
sidebar_position: 2
description: "VTB — VTB United League Infobasket (Russia) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# VTB — VTB United League Infobasket (Russia) — Team

## vtb_infobasket_team_stats

Season totals of every team of a season (all stages).

**Endpoint URL:** `GET https://org.infobasket.su/Comp/GetTeamStats/{season}`

**Valid URL:** [https://org.infobasket.su/Comp/GetTeamStats/2026?page=0&tag=vtb](https://org.infobasket.su/Comp/GetTeamStats/2026?page=0&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `team_id` | integer | Infobasket team id (TeamID): one club's entry in a competition family (Lokomotiv Kuban = 2650). Joins to /Widget/Team*/{teamId} and equals the v2 API `teamId`. |
| `game_count` | integer | Games in the totals (the whole competition, all stages, for the season-level CompID). |
| `points` | integer | Points. The wire's null is 0 here (null counts are filled). |
| `shot1` | integer | Free throws attempted. The wire's null is 0 here (null counts are filled). |
| `goal1` | integer | Free throws made. The wire's null is 0 here (null counts are filled). |
| `shot2` | integer | 2-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal2` | integer | 2-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot3` | integer | 3-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal3` | integer | 3-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot23` | integer | Field goals (2 + 3 points) attempted. The wire's null is 0 here (null counts are filled). |
| `goal23` | integer | Field goals (2 + 3 points) made. The wire's null is 0 here (null counts are filled). |
| `shot1_percent` | numeric | Free-throw percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot2_percent` | numeric | 2-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot3_percent` | numeric | 3-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot23_percent` | numeric | Field-goal percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `assist` | integer | Assists. The wire's null is 0 here (null counts are filled). |
| `blocks` | integer | Blocked shots. The wire's null is 0 here (null counts are filled). |
| `def_rebound` | integer | Defensive rebounds. The wire's null is 0 here (null counts are filled). |
| `off_rebound` | integer | Offensive rebounds. The wire's null is 0 here (null counts are filled). |
| `rebound` | integer | Total rebounds. The wire's null is 0 here (null counts are filled). |
| `steal` | integer | Steals. The wire's null is 0 here (null counts are filled). |
| `turnover` | integer | Turnovers. The wire's null is 0 here (null counts are filled). |
| `team_def_rebound` | integer | Team defensive rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_off_rebound` | integer | Team offensive rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_rebound` | integer | Team rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_steal` | integer | Team steals (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_turnover` | integer | Team turnovers (not credited to a player, e.g. shot-clock violations). The wire's null is 0 here (null counts are filled). |
| `kpi` | integer | Efficiency index (KPI, the platform's 'КПИ'); formula unverified. |
| `avg_points` | numeric | Per-game average of: points; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_assist` | numeric | Per-game average of: assists; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_blocks` | numeric | Per-game average of: blocked shots; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_def_rebound` | numeric | Per-game average of: defensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_off_rebound` | numeric | Per-game average of: offensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_rebound` | numeric | Per-game average of: total rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_steal` | numeric | Per-game average of: steals; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_turnover` | numeric | Per-game average of: turnovers; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_foul` | numeric | Per-game average of: personal fouls committed; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_opponent_foul` | numeric | Per-game average of: fouls drawn; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_plus_minus` | numeric | Per-game average of: plus-minus: point differential while on the court; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_def_rebound` | numeric | Per-game average of: team defensive rebounds (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_off_rebound` | numeric | Per-game average of: team offensive rebounds (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_rebound` | numeric | Per-game average of: team rebounds (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_steal` | numeric | Per-game average of: team steals (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_turnover` | numeric | Per-game average of: team turnovers (not credited to a player, e.g. shot-clock violations); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `foul` | integer | Personal fouls committed. The wire's null is 0 here (null counts are filled). |
| `opponent_foul` | integer | Fouls drawn. The wire's null is 0 here (null counts are filled). |
| `seconds` | integer | Playing time in seconds. |
| `played_time` | character | Playing time as 'minutes:seconds' (team rows sum the five players: '200:00' = 40 minutes x 5). |
| `avg_played_time` | character | Average playing time per game as 'minutes:seconds'. |
| `plus_minus` | integer | Plus-minus: point differential while on the court. Null where the host sends none. |
| `team_short_name_ru` | character | The the team's short name in Russian (as the competition names it). |
| `team_short_name_en` | character | The the team's short name in English (as the competition names it). |
| `team_name_ru` | character | The the team's name in Russian (as the competition names it). |
| `team_name_en` | character | The the team's name in English (as the competition names it). |
| `team_region_name_ru` | character | The the team's region or city in Russian (as the competition names it). |
| `team_region_name_en` | character | The the team's region or city in English (as the competition names it). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#vtb_infobasket_team_stats-example}

```python
vtb_infobasket_team_stats(season=2026)
```

_Last validated n/a._

## vtb_infobasket_team_stats_players

Season totals of every player of every team of a season.

**Endpoint URL:** `GET https://org.infobasket.su/Comp/GetTeamStats/{season}`

**Valid URL:** [https://org.infobasket.su/Comp/GetTeamStats/2026?page=0&tag=vtb](https://org.infobasket.su/Comp/GetTeamStats/2026?page=0&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_team_stats_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `team_id` | integer | Infobasket TeamID (2650 = Lokomotiv Kuban); the v2 teamId. Not a club-record id and not a registration id. |
| `person_id` | integer | Infobasket person id (PersonID): one person (player, coach or official) across clubs and seasons. Joins to /Widget/PlayerPage/{personId} (players) and /Widget/CoachPage/{personId} (coaches). |
| `player_number` | integer | Shirt number as an integer (0 when the player has none or wears 00). |
| `display_number` | character | Shirt number as displayed ('0', '7'); a coach's is the coach order (1 = head coach). |
| `is_active` | logical | True while the person is on the current roster. |
| `active_status` | character | Roster status text ('Accepted'), empty otherwise. |
| `points` | integer | Points. The wire's null is 0 here (null counts are filled). |
| `shot1` | integer | Free throws attempted. The wire's null is 0 here (null counts are filled). |
| `goal1` | integer | Free throws made. The wire's null is 0 here (null counts are filled). |
| `shot2` | integer | 2-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal2` | integer | 2-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot3` | integer | 3-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal3` | integer | 3-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot23` | integer | Field goals (2 + 3 points) attempted. The wire's null is 0 here (null counts are filled). |
| `goal23` | integer | Field goals (2 + 3 points) made. The wire's null is 0 here (null counts are filled). |
| `shot1_percent` | numeric | Free-throw percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot2_percent` | numeric | 2-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot3_percent` | numeric | 3-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot23_percent` | numeric | Field-goal percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `assist` | integer | Assists. The wire's null is 0 here (null counts are filled). |
| `blocks` | integer | Blocked shots. The wire's null is 0 here (null counts are filled). |
| `def_rebound` | integer | Defensive rebounds. The wire's null is 0 here (null counts are filled). |
| `off_rebound` | integer | Offensive rebounds. The wire's null is 0 here (null counts are filled). |
| `rebound` | integer | Total rebounds. The wire's null is 0 here (null counts are filled). |
| `steal` | integer | Steals. The wire's null is 0 here (null counts are filled). |
| `turnover` | integer | Turnovers. The wire's null is 0 here (null counts are filled). |
| `foul` | integer | Personal fouls committed. The wire's null is 0 here (null counts are filled). |
| `opponent_foul` | integer | Fouls drawn. The wire's null is 0 here (null counts are filled). |
| `plus_minus` | integer | Plus-minus: point differential while on the court. Null where the host sends none. |
| `kpi` | integer | Efficiency index (KPI, the platform's 'КПИ'); formula unverified. |
| `avg_points` | numeric | Per-game average of: points; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_assist` | numeric | Per-game average of: assists; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_blocks` | numeric | Per-game average of: blocked shots; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_def_rebound` | numeric | Per-game average of: defensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_off_rebound` | numeric | Per-game average of: offensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_rebound` | numeric | Per-game average of: total rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_steal` | numeric | Per-game average of: steals; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_turnover` | numeric | Per-game average of: turnovers; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_foul` | numeric | Per-game average of: personal fouls committed; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_opponent_foul` | numeric | Per-game average of: fouls drawn; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_plus_minus` | numeric | Per-game average of: plus-minus: point differential while on the court; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_kpi` | numeric | Per-game average of: efficiency index (kpi, the platform's 'кпи'); formula unverified; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `seconds` | integer | Playing time in seconds. |
| `played_time` | character | Playing time as 'minutes:seconds' (team rows sum the five players: '200:00' = 40 minutes x 5). |
| `avg_played_time` | character | Average playing time per game as 'minutes:seconds'. |
| `in_game_count` | integer | Games actually played (on the court). |
| `game_count` | integer | Games in the totals (the whole competition, all stages, for the season-level CompID). |
| `start_count` | integer | Games the person started. |
| `person_last_name_ru` | character | Last name, Russian. |
| `person_last_name_en` | character | Last name, English. |
| `person_first_name_ru` | character | First name, Russian. |
| `person_first_name_en` | character | First name, English. |
| `person_full_name_ru` | character | Full name, 'Last First', Russian. |
| `person_full_name_en` | character | Full name, 'Last First', English. |
| `person_gender` | integer | Person gender: 1 on every sampled (male) person. |
| `birth_date` | character | Birth date (ISO 8601 date) of a player: public bio, kept; coaches' and officials' birth data is never output. |
| `height` | character | Height in centimetres. |
| `weight` | character | Weight in kilograms. |
| `player_position` | integer | Player position code from the host (1 point guard, 2 shooting guard, 3 small forward ... 5 center; see position_id for the named position). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#vtb_infobasket_team_stats_players-example}

```python
vtb_infobasket_team_stats_players(season=2026)
```

_Last validated n/a._

## vtb_infobasket_team_stats_coaches

Technical fouls and games of every coach of every team of a season.

**Endpoint URL:** `GET https://org.infobasket.su/Comp/GetTeamStats/{season}`

**Valid URL:** [https://org.infobasket.su/Comp/GetTeamStats/2026?page=0&tag=vtb](https://org.infobasket.su/Comp/GetTeamStats/2026?page=0&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_team_stats_coaches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `team_id` | integer | Infobasket TeamID (2650 = Lokomotiv Kuban); the v2 teamId. Not a club-record id and not a registration id. |
| `person_id` | integer | Infobasket person id (PersonID): one person (player, coach or official) across clubs and seasons. Joins to /Widget/PlayerPage/{personId} (players) and /Widget/CoachPage/{personId} (coaches). |
| `player_number` | integer | Coach order (1 head coach). |
| `display_number` | character | Coach order (1 head coach). |
| `is_active` | logical | True while the person is on the current roster. |
| `active_status` | character | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `foul_c` | integer | Technical fouls on the head coach for his own conduct (scoresheet 'C'). |
| `foul_b` | integer | Technical fouls on the head coach for bench conduct (scoresheet 'B'). |
| `foul_d` | integer | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `foul` | integer | Personal fouls committed. The wire's null is 0 here (null counts are filled). |
| `game_count` | integer | Games in the totals (the whole competition, all stages, for the season-level CompID). |
| `person_last_name_ru` | character | Last name, Russian. |
| `person_last_name_en` | character | Last name, English. |
| `person_first_name_ru` | character | First name, Russian. |
| `person_first_name_en` | character | First name, English. |
| `person_full_name_ru` | character | Full name, 'Last First', Russian. |
| `person_full_name_en` | character | Full name, 'Last First', English. |
| `person_gender` | integer | Person gender: 1 on every sampled (male) person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#vtb_infobasket_team_stats_coaches-example}

```python
vtb_infobasket_team_stats_coaches(season=2026)
```

_Last validated n/a._

## vtb_infobasket_team_preview

Season totals of a game's two teams (the pre-game preview).

**Endpoint URL:** `GET https://org.infobasket.su/Comp/GetTeamStatsForPreview/{game_id}`

**Valid URL:** [https://org.infobasket.su/Comp/GetTeamStatsForPreview/921391?season=2026&tag=vtb](https://org.infobasket.su/Comp/GetTeamStatsForPreview/921391?season=2026&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Infobasket GameID (game_id in vtb_infobasket_calendar; the v2 matches' match_id), e.g. 921391. Global across seasons. |
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). Optional (the host accepts the call without it). |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_team_preview-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | Infobasket GameID of the game (the v2 matches' match_id); global across seasons. |
| `team_id` | integer | Infobasket team id (TeamID): one club's entry in a competition family (Lokomotiv Kuban = 2650). Joins to /Widget/Team*/{teamId} and equals the v2 API `teamId`. |
| `game_count` | integer | Games in the totals (the whole competition, all stages, for the season-level CompID). |
| `points` | integer | Points. The wire's null is 0 here (null counts are filled). |
| `shot1` | integer | Free throws attempted. The wire's null is 0 here (null counts are filled). |
| `goal1` | integer | Free throws made. The wire's null is 0 here (null counts are filled). |
| `shot2` | integer | 2-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal2` | integer | 2-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot3` | integer | 3-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal3` | integer | 3-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot23` | integer | Field goals (2 + 3 points) attempted. The wire's null is 0 here (null counts are filled). |
| `goal23` | integer | Field goals (2 + 3 points) made. The wire's null is 0 here (null counts are filled). |
| `shot1_percent` | numeric | Free-throw percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot2_percent` | numeric | 2-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot3_percent` | numeric | 3-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot23_percent` | numeric | Field-goal percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `assist` | integer | Assists. The wire's null is 0 here (null counts are filled). |
| `blocks` | integer | Blocked shots. The wire's null is 0 here (null counts are filled). |
| `def_rebound` | integer | Defensive rebounds. The wire's null is 0 here (null counts are filled). |
| `off_rebound` | integer | Offensive rebounds. The wire's null is 0 here (null counts are filled). |
| `rebound` | integer | Total rebounds. The wire's null is 0 here (null counts are filled). |
| `steal` | integer | Steals. The wire's null is 0 here (null counts are filled). |
| `turnover` | integer | Turnovers. The wire's null is 0 here (null counts are filled). |
| `team_def_rebound` | integer | Team defensive rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_off_rebound` | integer | Team offensive rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_rebound` | integer | Team rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_steal` | integer | Team steals (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_turnover` | integer | Team turnovers (not credited to a player, e.g. shot-clock violations). The wire's null is 0 here (null counts are filled). |
| `kpi` | integer | Efficiency index (KPI, the platform's 'КПИ'); formula unverified. |
| `avg_points` | numeric | Per-game average of: points; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_assist` | numeric | Per-game average of: assists; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_blocks` | numeric | Per-game average of: blocked shots; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_def_rebound` | numeric | Per-game average of: defensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_off_rebound` | numeric | Per-game average of: offensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_rebound` | numeric | Per-game average of: total rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_steal` | numeric | Per-game average of: steals; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_turnover` | numeric | Per-game average of: turnovers; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_foul` | numeric | Per-game average of: personal fouls committed; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_opponent_foul` | numeric | Per-game average of: fouls drawn; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_plus_minus` | numeric | Per-game average of: plus-minus: point differential while on the court; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_def_rebound` | numeric | Per-game average of: team defensive rebounds (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_off_rebound` | numeric | Per-game average of: team offensive rebounds (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_rebound` | numeric | Per-game average of: team rebounds (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_steal` | numeric | Per-game average of: team steals (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_turnover` | numeric | Per-game average of: team turnovers (not credited to a player, e.g. shot-clock violations); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `foul` | integer | Personal fouls committed. The wire's null is 0 here (null counts are filled). |
| `opponent_foul` | integer | Fouls drawn. The wire's null is 0 here (null counts are filled). |
| `seconds` | integer | Playing time in seconds. |
| `played_time` | character | Playing time as 'minutes:seconds' (team rows sum the five players: '200:00' = 40 minutes x 5). |
| `avg_played_time` | character | Average playing time per game as 'minutes:seconds'. |
| `plus_minus` | integer | Plus-minus: point differential while on the court. Null where the host sends none. |
| `team_short_name_ru` | character | The the team's short name in Russian (as the competition names it). |
| `team_short_name_en` | character | The the team's short name in English (as the competition names it). |
| `team_name_ru` | character | The the team's name in Russian (as the competition names it). |
| `team_name_en` | character | The the team's name in English (as the competition names it). |
| `team_region_name_ru` | character | The the team's region or city in Russian (as the competition names it). |
| `team_region_name_en` | character | The the team's region or city in English (as the competition names it). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#vtb_infobasket_team_preview-example}

```python
vtb_infobasket_team_preview(game_id=921391, season=2026)
```

_Last validated n/a._

## vtb_infobasket_team_box_score

Team box score of a finished game, with the head coach's technical fouls.

**Endpoint URL:** `GET https://org.infobasket.su/Widget/GetOnline/{game_id}`

**Valid URL:** [https://org.infobasket.su/Widget/GetOnline/921391?format=json](https://org.infobasket.su/Widget/GetOnline/921391?format=json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Infobasket GameID (game_id in vtb_infobasket_calendar; the v2 matches' match_id), e.g. 921391. Global across seasons. |

### Returns {#vtb_infobasket_team_box_score-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | Infobasket GameID of the game (the v2 matches' match_id); global across seasons. |
| `is_home` | logical | True for team 1, the team listed first (team A, the home side in the calendar); the body has no explicit home flag (unverified). |
| `team_number` | integer | Side: 1 team A (home), 2 team B (away). |
| `team_id` | integer | Infobasket team id (TeamID): one club's entry in a competition family (Lokomotiv Kuban = 2650). Joins to /Widget/Team*/{teamId} and equals the v2 API `teamId`. The scope check compares both sides with the request. |
| `score` | integer | Final points of the team. |
| `points` | integer | Points. The wire's null is 0 here (null counts are filled). |
| `shot1` | integer | Free throws attempted. The wire's null is 0 here (null counts are filled). |
| `goal1` | integer | Free throws made. The wire's null is 0 here (null counts are filled). |
| `shot2` | integer | 2-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal2` | integer | 2-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot3` | integer | 3-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal3` | integer | 3-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot1_percent` | numeric | Free-throw percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot2_percent` | numeric | 2-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot3_percent` | numeric | 3-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `assist` | integer | Assists. The wire's null is 0 here (null counts are filled). |
| `blocks` | integer | Blocked shots. The wire's null is 0 here (null counts are filled). |
| `def_rebound` | integer | Defensive rebounds. The wire's null is 0 here (null counts are filled). |
| `off_rebound` | integer | Offensive rebounds. The wire's null is 0 here (null counts are filled). |
| `rebound` | integer | Total rebounds. The wire's null is 0 here (null counts are filled). |
| `steal` | integer | Steals. The wire's null is 0 here (null counts are filled). |
| `turnover` | integer | Turnovers. The wire's null is 0 here (null counts are filled). |
| `team_def_rebound` | integer | Team defensive rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_off_rebound` | integer | Team offensive rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_rebound` | integer | Team rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_steal` | integer | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `team_turnover` | integer | Team turnovers (not credited to a player, e.g. shot-clock violations). The wire's null is 0 here (null counts are filled). |
| `foul` | integer | Personal fouls committed by the team. |
| `opponent_foul` | integer | Fouls drawn. The wire's null is 0 here (null counts are filled). |
| `seconds` | integer | Playing time in seconds. |
| `played_time` | character | Playing time as 'minutes:seconds' (team rows sum the five players: '200:00' = 40 minutes x 5). |
| `plus_minus` | character | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `team_short_name_ru` | character | The the team's short name in Russian (as the competition names it). |
| `team_short_name_en` | character | The the team's short name in English (as the competition names it). |
| `team_name_ru` | character | The the team's name in Russian (as the competition names it). |
| `team_name_en` | character | The the team's name in English (as the competition names it). |
| `team_region_name_ru` | character | The the team's region or city in Russian (as the competition names it). |
| `team_region_name_en` | character | The the team's region or city in English (as the competition names it). |
| `coach_person_id` | integer | Infobasket person id (PersonID): one person (player, coach or official) across clubs and seasons. Joins to /Widget/PlayerPage/{personId} (players) and /Widget/CoachPage/{personId} (coaches). |
| `coach_name_en` | character | Head coach's name in English, as 'Last First'. |
| `coach_name_ru` | character | Head coach's name in Russian, as 'Last First'. |
| `coach_foul_c` | integer | Technical fouls on the head coach for his own conduct (scoresheet 'C'). |
| `coach_foul_b` | integer | Technical fouls on the head coach for bench conduct (scoresheet 'B'). |
| `coach_foul_d` | integer | Disqualifying fouls on the coach (scoresheet 'D'). |
| `coach_foul` | integer | Fouls charged to the coach (C + B + D). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#vtb_infobasket_team_box_score-example}

```python
vtb_infobasket_team_box_score(game_id=921391)
```

_Last validated n/a._

## vtb_infobasket_team

Club profile: names, region, country, status.

**Endpoint URL:** `GET https://org.infobasket.su/Widget/TeamPage/{team_id}`

**Valid URL:** [https://org.infobasket.su/Widget/TeamPage/2650?format=json&lang=en&tag=vtb](https://org.infobasket.su/Widget/TeamPage/2650?format=json&lang=en&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Infobasket TeamID (team_id in the calendar, a box score or the v2 matches), e.g. 2650 (Lokomotiv Kuban). |
| `lang` | `lang` |  |  | `Y` | Label language (en or ru). Most bodies carry both languages whatever is asked; the leader categories' labels follow it. |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Infobasket team id (TeamID): one club's entry in a competition family (Lokomotiv Kuban = 2650). Joins to /Widget/Team*/{teamId} and equals the v2 API `teamId`. The scope check compares it with the request. |
| `team_type` | integer | Team type code: 1 on the club record. |
| `team_name_ru` | character | Club name, Russian. |
| `team_name_en` | character | Club name, English. |
| `team_short_name_ru` | character | Club short name, Russian. |
| `team_short_name_en` | character | Club short name, English. |
| `team_abc_name_ru` | character | Club abbreviation, Russian (the full name in the sample). |
| `team_abc_name_en` | character | Club abbreviation, English. |
| `team_gender` | integer | Team gender code: 0 on every sample (men). |
| `team_region` | integer | Infobasket region id of the club's city. |
| `team_country` | integer | Country id of the club (1 = Russia, unverified). |
| `team_logo` | integer | Logo flag; 0 in the samples. |
| `team_uniform1` | integer | First uniform colour code. |
| `team_uniform2` | integer | Second uniform colour code. |
| `team_status` | integer | Club record status (1 active). |
| `team_record_id` | integer | Id of the club-record name (CurrentTeamName.TeamID, 2259 for team 2650): a different key space from team_id. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#vtb_infobasket_team-example}

```python
vtb_infobasket_team(team_id=2650)
```

_Last validated n/a._

## vtb_infobasket_team_seasons

The seasons a club has data for (END year and the club's competition id).

**Endpoint URL:** `GET https://org.infobasket.su/Widget/GetTeamSeasons/{team_id}`

**Valid URL:** [https://org.infobasket.su/Widget/GetTeamSeasons/2650?format=json&lang=en&tag=vtb](https://org.infobasket.su/Widget/GetTeamSeasons/2650?format=json&lang=en&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Infobasket TeamID (team_id in the calendar, a box score or the v2 matches), e.g. 2650 (Lokomotiv Kuban). |
| `lang` | `lang` |  |  | `Y` | Label language (en or ru). Most bodies carry both languages whatever is asked; the leader categories' labels follow it. |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_team_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Infobasket TeamID (2650 = Lokomotiv Kuban); the v2 teamId. Not a club-record id and not a registration id. |
| `season` | integer | Season END year of the club's season entry (2027 = 2026/2027). |
| `comp_id` | integer | Competition id of the season in the club's rfb tag (56293 for 2027), not the VTB tag's season CompID. |
| `season_name` | character | Season name in Russian ('Сезон 2026/2027' = 'Season 2026/2027'). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#vtb_infobasket_team_seasons-example}

```python
vtb_infobasket_team_seasons(team_id=2650)
```

_Last validated n/a._

## vtb_infobasket_team_comps

The competitions a club played in one season.

**Endpoint URL:** `GET https://org.infobasket.su/Widget/GetTeamComps/{team_id}`

**Valid URL:** [https://org.infobasket.su/Widget/GetTeamComps/2650?format=json&season=2026&lang=en&tag=vtb](https://org.infobasket.su/Widget/GetTeamComps/2650?format=json&season=2026&lang=en&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Infobasket TeamID (team_id in the calendar, a box score or the v2 matches), e.g. 2650 (Lokomotiv Kuban). |
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). Required. |
| `lang` | `lang` |  |  | `Y` | Label language (en or ru). Most bodies carry both languages whatever is asked; the leader categories' labels follow it. |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_team_comps-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Infobasket TeamID (2650 = Lokomotiv Kuban); the v2 teamId. Not a club-record id and not a registration id. |
| `season` | integer | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `comp_id` | integer | Competition id (the season or a stage of it). |
| `comp_type` | integer | Stage type code: 0 season, regular season or group; 10 playoff container; 5 and 7 playoff series; -5 seen in season lists. Meanings inferred from the stage names, unverified. |
| `comp_sort` | integer | Sort order among sibling stages. |
| `level` | integer | Depth of the competition in the tree (1 = season level). |
| `comp_name_ru` | character | Competition name, Russian ('Единая лига ВТБ' = 'VTB United League'). |
| `comp_name_en` | character | Stage name (e.g. 'Regular Season'), English. |
| `parent_comp_id` | integer | Unobserved: null in every sample, so its type and meaning are unconfirmed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#vtb_infobasket_team_comps-example}

```python
vtb_infobasket_team_comps(season=2026, team_id=2650)
```

_Last validated n/a._

## vtb_infobasket_team_games

A club's games in one season (the calendar's row shape).

**Endpoint URL:** `GET https://org.infobasket.su/Widget/TeamGames/{team_id}`

**Valid URL:** [https://org.infobasket.su/Widget/TeamGames/2650?format=json&season=2026&tag=vtb](https://org.infobasket.su/Widget/TeamGames/2650?format=json&season=2026&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Infobasket TeamID (team_id in the calendar, a box score or the v2 matches), e.g. 2650 (Lokomotiv Kuban). |
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). Required. |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_team_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Infobasket TeamID (2650 = Lokomotiv Kuban); the v2 teamId. Not a club-record id and not a registration id. |
| `season` | integer | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `comp_id` | integer | Season-level Infobasket CompID of the season (50714 = 2025-26): the id every season-keyed route takes. |
| `game_id` | integer | Infobasket game id (GameID); equals the v2 API `matchId`. Joins to /Widget/GetOnline/{gameId} and /Widget/GamePage/{gameId}. |
| `game_number` | character | Game number within the competition, e.g. 'RS-002' (RS = regular season); playoff games use the series code. |
| `game_date` | character | Game date as an ISO 8601 date (YYYY-MM-DD); the wire sends DD.MM.YYYY. |
| `has_time` | logical | True when a tip-off time is set. |
| `game_time` | character | Tip-off local time, 'HH:MM'. |
| `game_time_msk` | character | Tip-off time in Moscow time ('HH:MM' on calendar rows, 'HH.MM' in GetOnline). |
| `game_date_time` | character | Tip-off instant as an ISO 8601 string in Moscow time with its +03:00 offset (the wire sends a .NET /Date(ms)/ value). |
| `game_date_time_moscow` | character | Tip-off instant as an ISO 8601 string in Moscow time with its +03:00 offset (the wire's second .NET date; equal to game_date_time in every sample). |
| `game_status` | integer | Game status: 0 planned, 1 completed, 2 live (the site's widget code). Samples are completed games. |
| `team_a_id` | integer | Team A (home) TeamID. Infobasket team id (TeamID): one club's entry in a competition family (Lokomotiv Kuban = 2650). Joins to /Widget/Team*/{teamId} and equals the v2 API `teamId`. |
| `team_b_id` | integer | Team B (away) TeamID. Infobasket team id (TeamID): one club's entry in a competition family (Lokomotiv Kuban = 2650). Joins to /Widget/Team*/{teamId} and equals the v2 API `teamId`. |
| `short_team_name_aru` | character | Team short name of team A (home), Russian. |
| `short_team_name_bru` | character | Team short name of team B (away), Russian. |
| `team_name_aru` | character | Team name with city in brackets of team A (home), Russian. |
| `team_name_bru` | character | Team name with city in brackets of team B (away), Russian. |
| `comp_team_name_aru` | character | Team name as registered in the competition of team A (home), Russian. |
| `comp_team_name_bru` | character | Team name as registered in the competition of team B (away), Russian. |
| `region_team_name_aru` | character | Team's city or region of team A (home), Russian. |
| `region_team_name_bru` | character | Team's city or region of team B (away), Russian. |
| `short_team_name_aen` | character | Team short name of team A (home), English. |
| `short_team_name_ben` | character | Team short name of team B (away), English. |
| `team_name_aen` | character | Team name with city in brackets of team A (home), English. |
| `team_name_ben` | character | Team name with city in brackets of team B (away), English. |
| `comp_team_name_aen` | character | Team name as registered in the competition of team A (home), English. |
| `comp_team_name_ben` | character | Team name as registered in the competition of team B (away), English. |
| `region_team_name_aen` | character | Team's city or region of team A (home), English. |
| `region_team_name_ben` | character | Team's city or region of team B (away), English. |
| `score_a` | integer | Final score of team A (home). |
| `score_b` | integer | Final score of team B (away). |
| `tv_ru` | character | TV channels, in Russian. |
| `tv_en` | character | TV channels, in English. |
| `region_ru` | character | City or region of the game's venue in Russian. |
| `region_en` | character | City or region of the game's venue in English. |
| `arena_en` | character | Arena name, English. |
| `arena_ru` | character | Arena, Russian ('СК' = sports complex). |
| `arena_id` | integer | Infobasket arena id; equals the v2 API `venueId` (11687 = SK Basket-Hall, Krasnodar). |
| `game_attendance` | integer | Spectators at the game (null when not recorded). |
| `comp_name_ru` | character | Stage name, Russian ('Регулярный чемпионат' = 'Regular Season', 'Группа А' = 'Group A'). |
| `comp_name_en` | character | Stage name (e.g. 'Regular Season'), English. |
| `league_name_ru` | character | League name, Russian ('Единая лига ВТБ' / 'Чемпионат Единой лиги ВТБ' = 'VTB United League' / 'VTB United League Championship'). |
| `league_name_en` | character | League name, English. |
| `league_short_name_ru` | character | League short name, Russian ('ВТБ' = 'VTB'). |
| `league_short_name_en` | character | League short name, English. |
| `gender` | integer | Competition gender: 0 on every VTB sample (men). Other values unverified. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#vtb_infobasket_team_games-example}

```python
vtb_infobasket_team_games(season=2026, team_id=2650)
```

_Last validated n/a._

## vtb_infobasket_team_season_stats

A club's season totals in one season.

**Endpoint URL:** `GET https://org.infobasket.su/Widget/TeamStats/{team_id}`

**Valid URL:** [https://org.infobasket.su/Widget/TeamStats/2650?format=json&season=2026&lang=en&tag=vtb](https://org.infobasket.su/Widget/TeamStats/2650?format=json&season=2026&lang=en&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Infobasket TeamID (team_id in the calendar, a box score or the v2 matches), e.g. 2650 (Lokomotiv Kuban). |
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). Required. |
| `lang` | `lang` |  |  | `Y` | Label language (en or ru). Most bodies carry both languages whatever is asked; the leader categories' labels follow it. |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_team_season_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `team_id` | integer | Infobasket team id (TeamID): one club's entry in a competition family (Lokomotiv Kuban = 2650). Joins to /Widget/Team*/{teamId} and equals the v2 API `teamId`. The scope check compares it with the request. |
| `game_count` | integer | Games in the totals (the whole competition, all stages, for the season-level CompID). |
| `points` | integer | Points. The wire's null is 0 here (null counts are filled). |
| `shot1` | integer | Free throws attempted. The wire's null is 0 here (null counts are filled). |
| `goal1` | integer | Free throws made. The wire's null is 0 here (null counts are filled). |
| `shot2` | integer | 2-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal2` | integer | 2-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot3` | integer | 3-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal3` | integer | 3-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot23` | integer | Field goals (2 + 3 points) attempted. The wire's null is 0 here (null counts are filled). |
| `goal23` | integer | Field goals (2 + 3 points) made. The wire's null is 0 here (null counts are filled). |
| `shot1_percent` | numeric | Free-throw percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot2_percent` | numeric | 2-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot3_percent` | numeric | 3-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot23_percent` | numeric | Field-goal percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `assist` | integer | Assists. The wire's null is 0 here (null counts are filled). |
| `blocks` | integer | Blocked shots. The wire's null is 0 here (null counts are filled). |
| `def_rebound` | integer | Defensive rebounds. The wire's null is 0 here (null counts are filled). |
| `off_rebound` | integer | Offensive rebounds. The wire's null is 0 here (null counts are filled). |
| `rebound` | integer | Total rebounds. The wire's null is 0 here (null counts are filled). |
| `steal` | integer | Steals. The wire's null is 0 here (null counts are filled). |
| `turnover` | integer | Turnovers. The wire's null is 0 here (null counts are filled). |
| `team_def_rebound` | integer | Team defensive rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_off_rebound` | integer | Team offensive rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_rebound` | integer | Team rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_steal` | integer | Team steals (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_turnover` | integer | Team turnovers (not credited to a player, e.g. shot-clock violations). The wire's null is 0 here (null counts are filled). |
| `kpi` | integer | Efficiency index (KPI, the platform's 'КПИ'); formula unverified. |
| `avg_points` | numeric | Per-game average of: points; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_assist` | numeric | Per-game average of: assists; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_blocks` | numeric | Per-game average of: blocked shots; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_def_rebound` | numeric | Per-game average of: defensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_off_rebound` | numeric | Per-game average of: offensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_rebound` | numeric | Per-game average of: total rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_steal` | numeric | Per-game average of: steals; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_turnover` | numeric | Per-game average of: turnovers; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_foul` | numeric | Per-game average of: personal fouls committed; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_opponent_foul` | numeric | Per-game average of: fouls drawn; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_plus_minus` | numeric | Per-game average of: plus-minus: point differential while on the court; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_def_rebound` | numeric | Per-game average of: team defensive rebounds (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_off_rebound` | numeric | Per-game average of: team offensive rebounds (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_rebound` | numeric | Per-game average of: team rebounds (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_steal` | numeric | Per-game average of: team steals (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_turnover` | numeric | Per-game average of: team turnovers (not credited to a player, e.g. shot-clock violations); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `foul` | integer | Personal fouls committed. The wire's null is 0 here (null counts are filled). |
| `opponent_foul` | integer | Fouls drawn. The wire's null is 0 here (null counts are filled). |
| `seconds` | integer | Playing time in seconds. |
| `played_time` | character | Playing time as 'minutes:seconds' (team rows sum the five players: '200:00' = 40 minutes x 5). |
| `avg_played_time` | character | Average playing time per game as 'minutes:seconds'. |
| `plus_minus` | integer | Plus-minus: point differential while on the court. Null where the host sends none. |
| `team_short_name_ru` | character | The the team's short name in Russian (as the competition names it). |
| `team_short_name_en` | character | The the team's short name in English (as the competition names it). |
| `team_name_ru` | character | The the team's name in Russian (as the competition names it). |
| `team_name_en` | character | The the team's name in English (as the competition names it). |
| `team_region_name_ru` | character | The the team's region or city in Russian (as the competition names it). |
| `team_region_name_en` | character | The the team's region or city in English (as the competition names it). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#vtb_infobasket_team_season_stats-example}

```python
vtb_infobasket_team_season_stats(season=2026, team_id=2650)
```

_Last validated n/a._

## vtb_infobasket_team_player_stats

Season totals of every player of a club in one season.

**Endpoint URL:** `GET https://org.infobasket.su/Widget/TeamStats/{team_id}`

**Valid URL:** [https://org.infobasket.su/Widget/TeamStats/2650?format=json&season=2026&lang=en&tag=vtb](https://org.infobasket.su/Widget/TeamStats/2650?format=json&season=2026&lang=en&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Infobasket TeamID (team_id in the calendar, a box score or the v2 matches), e.g. 2650 (Lokomotiv Kuban). |
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). Required. |
| `lang` | `lang` |  |  | `Y` | Label language (en or ru). Most bodies carry both languages whatever is asked; the leader categories' labels follow it. |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_team_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `team_id` | integer | Infobasket TeamID (2650 = Lokomotiv Kuban); the v2 teamId. Not a club-record id and not a registration id. |
| `person_id` | integer | Infobasket person id (PersonID): one person (player, coach or official) across clubs and seasons. Joins to /Widget/PlayerPage/{personId} (players) and /Widget/CoachPage/{personId} (coaches). |
| `player_number` | integer | Shirt number as an integer (0 when the player has none or wears 00). |
| `display_number` | character | Shirt number as displayed ('0', '7'); a coach's is the coach order (1 = head coach). |
| `is_active` | logical | True while the person is on the current roster. |
| `active_status` | character | Roster status text ('Accepted'), empty otherwise. |
| `points` | integer | Points. The wire's null is 0 here (null counts are filled). |
| `shot1` | integer | Free throws attempted. The wire's null is 0 here (null counts are filled). |
| `goal1` | integer | Free throws made. The wire's null is 0 here (null counts are filled). |
| `shot2` | integer | 2-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal2` | integer | 2-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot3` | integer | 3-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal3` | integer | 3-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot23` | integer | Field goals (2 + 3 points) attempted. The wire's null is 0 here (null counts are filled). |
| `goal23` | integer | Field goals (2 + 3 points) made. The wire's null is 0 here (null counts are filled). |
| `shot1_percent` | numeric | Free-throw percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot2_percent` | numeric | 2-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot3_percent` | numeric | 3-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot23_percent` | numeric | Field-goal percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `assist` | integer | Assists. The wire's null is 0 here (null counts are filled). |
| `blocks` | integer | Blocked shots. The wire's null is 0 here (null counts are filled). |
| `def_rebound` | integer | Defensive rebounds. The wire's null is 0 here (null counts are filled). |
| `off_rebound` | integer | Offensive rebounds. The wire's null is 0 here (null counts are filled). |
| `rebound` | integer | Total rebounds. The wire's null is 0 here (null counts are filled). |
| `steal` | integer | Steals. The wire's null is 0 here (null counts are filled). |
| `turnover` | integer | Turnovers. The wire's null is 0 here (null counts are filled). |
| `foul` | integer | Personal fouls committed. The wire's null is 0 here (null counts are filled). |
| `opponent_foul` | integer | Fouls drawn. The wire's null is 0 here (null counts are filled). |
| `plus_minus` | integer | Plus-minus: point differential while on the court. Null where the host sends none. |
| `kpi` | integer | Efficiency index (KPI, the platform's 'КПИ'); formula unverified. |
| `avg_points` | numeric | Per-game average of: points; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_assist` | numeric | Per-game average of: assists; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_blocks` | numeric | Per-game average of: blocked shots; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_def_rebound` | numeric | Per-game average of: defensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_off_rebound` | numeric | Per-game average of: offensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_rebound` | numeric | Per-game average of: total rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_steal` | numeric | Per-game average of: steals; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_turnover` | numeric | Per-game average of: turnovers; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_foul` | numeric | Per-game average of: personal fouls committed; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_opponent_foul` | numeric | Per-game average of: fouls drawn; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_plus_minus` | numeric | Per-game average of: plus-minus: point differential while on the court; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_kpi` | numeric | Per-game average of: efficiency index (kpi, the platform's 'кпи'); formula unverified; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `seconds` | integer | Playing time in seconds. |
| `played_time` | character | Playing time as 'minutes:seconds' (team rows sum the five players: '200:00' = 40 minutes x 5). |
| `avg_played_time` | character | Average playing time per game as 'minutes:seconds'. |
| `in_game_count` | integer | Games actually played (on the court). |
| `game_count` | integer | Games in the totals (the whole competition, all stages, for the season-level CompID). |
| `start_count` | integer | Games the person started. |
| `person_last_name_ru` | character | Last name, Russian. |
| `person_last_name_en` | character | Last name, English. |
| `person_first_name_ru` | character | First name, Russian. |
| `person_first_name_en` | character | First name, English. |
| `person_full_name_ru` | character | Full name, 'Last First', Russian. |
| `person_full_name_en` | character | Full name, 'Last First', English. |
| `person_gender` | integer | Person gender: 1 on every sampled (male) person. |
| `birth_date` | character | Birth date (ISO 8601 date) of a player: public bio, kept; coaches' and officials' birth data is never output. |
| `height` | character | Height in centimetres. |
| `weight` | character | Weight in kilograms. |
| `player_position` | integer | Player position code from the host (1 point guard, 2 shooting guard, 3 small forward ... 5 center; see position_id for the named position). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#vtb_infobasket_team_player_stats-example}

```python
vtb_infobasket_team_player_stats(season=2026, team_id=2650)
```

_Last validated n/a._

## vtb_infobasket_team_coach_stats

Technical fouls and games of every coach of a club in one season.

**Endpoint URL:** `GET https://org.infobasket.su/Widget/TeamStats/{team_id}`

**Valid URL:** [https://org.infobasket.su/Widget/TeamStats/2650?format=json&season=2026&lang=en&tag=vtb](https://org.infobasket.su/Widget/TeamStats/2650?format=json&season=2026&lang=en&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Infobasket TeamID (team_id in the calendar, a box score or the v2 matches), e.g. 2650 (Lokomotiv Kuban). |
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). Required. |
| `lang` | `lang` |  |  | `Y` | Label language (en or ru). Most bodies carry both languages whatever is asked; the leader categories' labels follow it. |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_team_coach_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `team_id` | integer | Infobasket TeamID (2650 = Lokomotiv Kuban); the v2 teamId. Not a club-record id and not a registration id. |
| `person_id` | integer | Infobasket person id (PersonID): one person (player, coach or official) across clubs and seasons. Joins to /Widget/PlayerPage/{personId} (players) and /Widget/CoachPage/{personId} (coaches). |
| `player_number` | integer | Coach order (1 head coach). |
| `display_number` | character | Coach order (1 head coach). |
| `is_active` | logical | True while the person is on the current roster. |
| `active_status` | character | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `foul_c` | integer | Technical fouls on the head coach for his own conduct (scoresheet 'C'). |
| `foul_b` | integer | Technical fouls on the head coach for bench conduct (scoresheet 'B'). |
| `foul_d` | integer | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `foul` | integer | Personal fouls committed. The wire's null is 0 here (null counts are filled). |
| `game_count` | integer | Games in the totals (the whole competition, all stages, for the season-level CompID). |
| `person_last_name_ru` | character | Last name, Russian. |
| `person_last_name_en` | character | Last name, English. |
| `person_first_name_ru` | character | First name, Russian. |
| `person_first_name_en` | character | First name, English. |
| `person_full_name_ru` | character | Full name, 'Last First', Russian. |
| `person_full_name_en` | character | Full name, 'Last First', English. |
| `person_gender` | integer | Person gender: 1 on every sampled (male) person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#vtb_infobasket_team_coach_stats-example}

```python
vtb_infobasket_team_coach_stats(season=2026, team_id=2650)
```

_Last validated n/a._
