---
title: "VTB — VTB United League Infobasket (Russia) — Team: player–coach"
sidebar_label: "Team: player–coach"
sidebar_position: 4
description: "VTB — VTB United League Infobasket (Russia) — Team: player–coach — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# VTB — VTB United League Infobasket (Russia) — Team: player–coach

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
| `height` | integer | Height in centimetres. |
| `weight` | integer | Weight in kilograms. |
| `player_position` | integer | Player position code from the host (1 point guard, 2 shooting guard, 3 small forward ... 5 center; see position_id for the named position). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_team_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_team_player_stats-example}

```python
vtb_infobasket_team_player_stats(season=2026, team_id=2650)
```

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

### Raises {#vtb_infobasket_team_coach_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_team_coach_stats-example}

```python
vtb_infobasket_team_coach_stats(season=2026, team_id=2650)
```
