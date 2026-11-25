---
title: "Lacrosse — Premier Lacrosse League stats API (stats.premierlacrosseleague.com; runtime only, no key, the site's Referer) — Team"
sidebar_label: "Team"
sidebar_position: 2
description: "Lacrosse — Premier Lacrosse League stats API (stats.premierlacrosseleague.com; runtime only, no key, the site's Referer) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# Lacrosse — Premier Lacrosse League stats API (stats.premierlacrosseleague.com; runtime only, no key, the site's Referer) — Team

## pll_team_box

A game's team box score: one row per team (scoring, shooting, faceoffs, clears, saves, special teams, possessions and time of possession).

**Endpoint URL:** `GET https://stats.premierlacrosseleague.com/api/v4/events/{event_slug}/teams/stats`

**Valid URL:** [https://stats.premierlacrosseleague.com/api/v4/events/2026-ev-4/teams/stats](https://stats.premierlacrosseleague.com/api/v4/events/2026-ev-4/teams/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_slug` | `event_slug` |  | `Y` |  | Game slug (event_slug in pll_schedule), 2021 onward. |

### Returns {#pll_team_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `event_slug` | String | Game slug, the id every per-game wrapper takes (e.g. 2026-ev-4; the format changes by season). |
| `team_id` | String | Team code (e.g. WAT). |
| `scores` | Int64 | Points scored (a 2-point goal counts 2). |
| `scores_against` | Int64 | Points allowed (a 2-point goal counts 2). |
| `goals` | Int64 | Goals (1-point plus 2-point; goals = one_point_goals + two_point_goals). |
| `one_point_goals` | Int64 | 1-point goals (inside the arc). |
| `two_point_goals` | Int64 | 2-point goals (from beyond the arc). |
| `assists` | Int64 | Assists credited (passes leading directly to a goal). |
| `shots` | Int64 | Shots taken (on and off goal). |
| `shot_pct` | Float64 | Goals per shot (goals / shots). |
| `shots_on_goal` | Int64 | Shots that were on goal (saved or scored). |
| `shots_on_goal_pct` | Float64 | Shots on goal per shot. |
| `two_point_shots` | Int64 | Shots from beyond the arc. |
| `two_point_shot_pct` | Float64 | Goals per shot from beyond the arc. |
| `two_point_shots_on_goal` | Int64 | 2-point shots on goal. |
| `two_point_shots_on_goal_pct` | Float64 | 2-point shots on goal per 2-point shot. |
| `goals_against` | Int64 | Goals allowed (each goal once, whatever its point value). |
| `two_point_goals_against` | Int64 | 2-point goals allowed. |
| `saves` | Int64 | Saves made by the goalie (or team). |
| `save_pct` | Float64 | Saves per shot on goal faced (saves / (saves + goals_against); checked on the captures). |
| `clean_saves` | Int64 | Saves the goalie controlled cleanly (no rebound). |
| `messy_saves` | Int64 | Saves that left a rebound. |
| `clean_save_pct` | Float64 | Clean saves per save (clean_saves / saves). |
| `ground_balls` | Int64 | Ground balls picked up. |
| `turnovers` | Int64 | Turnovers committed. |
| `caused_turnovers` | Int64 | Caused turnovers (takeaways credited to a defender). |
| `faceoffs_won` | Int64 | Faceoffs won by the player or team. |
| `faceoffs_lost` | Int64 | Faceoffs lost by the player or team. |
| `faceoffs` | Int64 | Faceoffs taken (faceoff_pct = faceoffs_won / faceoffs; won + lost can fall short of it, as in the captures). |
| `faceoff_pct` | Float64 | Faceoffs won per faceoff taken (faceoffs_won / faceoffs). |
| `clears` | Int64 | Successful clears. |
| `clear_attempts` | Int64 | Clearing attempts (moving the ball from the defensive to the offensive half). |
| `clear_pct` | Float64 | Successful clears per attempt (clears / clear_attempts). |
| `ride_attempts` | Int64 | Riding attempts (opponent clears contested). |
| `num_penalties` | Int64 | Penalties committed. |
| `pim` | Float64 | Penalty minutes. |
| `shot_clock_expirations` | Int64 | Shot-clock violations. |
| `times_man_up` | Int64 | Man-up (power-play) chances. |
| `power_play_goals` | Int64 | Power-play goals. |
| `power_play_shots` | Int64 | Power-play shots. |
| `power_play_pct` | Float64 | Power-play goals per man-up chance. |
| `times_short_handed` | Int64 | Man-down (short-handed) spells. |
| `power_play_goals_against` | Int64 | Power-play goals allowed. |
| `power_play_goals_against_pct` | Float64 | Power-play goals allowed per opponent man-up chance. |
| `man_down_pct` | Float64 | Man-down (penalty kill) success rate. |
| `total_passes` | Int64 | Passes made, as the league's tracking counts them. |
| `touches` | Int64 | Touches (possessions of the ball by the player). |
| `time_in_possesion` | Int64 | Time in possession, seconds (the API's spelling). |
| `time_in_possesion_pct` | Float64 | Share of game time in possession (0-1). |
| `total_possessions` | Int64 | Team possessions in the game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pll_team_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An unknown game, team or player slug, or a 2019-2020 game slug on a per-game route (HTTP 404). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected a parameter (a missing year, a season segment or stat name outside its allowed list; HTTP 400). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 origin refusal, any other failed fetch, or a 200 that is not the `{data: ...}` envelope. |

### Example {#pll_team_box-example}

```python
pll_team_box(event_slug='2026-ev-4')
```

## pll_team_season_stats

Every team's season totals and rates: one row per team (offense, defense, faceoffs, clears and rides, power play and penalty kill).

**Endpoint URL:** `GET https://stats.premierlacrosseleague.com/api/v4/teams/season-stats`

**Valid URL:** [https://stats.premierlacrosseleague.com/api/v4/teams/season-stats?year=2026&seasonSegment=regular](https://stats.premierlacrosseleague.com/api/v4/teams/season-stats?year=2026&seasonSegment=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Season; omitted, the current one. |
| `seasonSegment` | `season_segment` |  |  | `Y` | Season segment. |

### Returns {#pll_team_season_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (from the body, or from the request when the body does not say). |
| `season_segment` | String | Season segment: regular, post (playoffs), champseries (Championship Series) or allstar. |
| `team_id` | String | Team code (e.g. WAT). |
| `team_slug` | String | Team slug, the id pll_team_stats and pll_team_roster take (e.g. waterdogs). |
| `full_name` | String | Team name (e.g. Waterdogs). |
| `location` | String | City of the game, or the team's city or region. |
| `location_code` | String | Three-letter location code (e.g. PHI). |
| `conference` | String | Conference (eastern or western; null before conferences, 2019-2020). |
| `games_played` | Int64 | Games played in the season segment (or career, on career leaderboards). |
| `scores` | Int64 | Points scored (a 2-point goal counts 2). |
| `scores_against` | Int64 | Points allowed (a 2-point goal counts 2). |
| `scores_pg` | Float64 | Points scored per game. |
| `goals` | Int64 | Goals (1-point plus 2-point; goals = one_point_goals + two_point_goals). |
| `one_point_goals` | Int64 | 1-point goals (inside the arc). |
| `two_point_goals` | Int64 | 2-point goals (from beyond the arc). |
| `assists` | Int64 | Assists credited (passes leading directly to a goal). |
| `shots` | Int64 | Shots taken (on and off goal). |
| `shots_pg` | Float64 | Shots per game. |
| `shot_pct` | Float64 | Goals per shot (goals / shots). |
| `shots_on_goal` | Int64 | Shots that were on goal (saved or scored). |
| `shots_on_goal_pct` | Float64 | Shots on goal per shot. |
| `two_point_shots` | Int64 | Shots from beyond the arc. |
| `two_point_shot_pct` | Float64 | Goals per shot from beyond the arc. |
| `two_point_shots_on_goal` | Int64 | 2-point shots on goal. |
| `two_point_shots_on_goal_pct` | Float64 | 2-point shots on goal per 2-point shot. |
| `goals_against` | Int64 | Goals allowed (each goal once, whatever its point value). |
| `two_point_goals_against` | Int64 | 2-point goals allowed. |
| `saa` | Float64 | Scores against per 48 minutes on the field (scores_against / (tof / 2880); checked on the captures). |
| `saves` | Int64 | Saves made by the goalie (or team). |
| `save_pct` | Float64 | Saves per shot on goal faced (saves / (saves + goals_against); checked on the captures). |
| `clean_saves` | Int64 | Saves the goalie controlled cleanly (no rebound). |
| `messy_saves` | Int64 | Saves that left a rebound. |
| `clean_save_pct` | Float64 | Clean saves per save (clean_saves / saves). |
| `ground_balls` | Int64 | Ground balls picked up. |
| `turnovers` | Int64 | Turnovers committed. |
| `caused_turnovers` | Int64 | Caused turnovers (takeaways credited to a defender). |
| `faceoffs_won` | Int64 | Faceoffs won by the player or team. |
| `faceoffs_lost` | Int64 | Faceoffs lost by the player or team. |
| `faceoffs` | Int64 | Faceoffs taken (faceoff_pct = faceoffs_won / faceoffs; won + lost can fall short of it, as in the captures). |
| `faceoff_pct` | Float64 | Faceoffs won per faceoff taken (faceoffs_won / faceoffs). |
| `clears` | Int64 | Successful clears. |
| `clear_attempts` | Int64 | Clearing attempts (moving the ball from the defensive to the offensive half). |
| `clear_pct` | Float64 | Successful clears per attempt (clears / clear_attempts). |
| `rides` | Int64 | Successful rides (opponent clears stopped). |
| `ride_attempts` | Int64 | Riding attempts (opponent clears contested). |
| `rides_pct` | Float64 | Successful rides per ride attempt. |
| `num_penalties` | Int64 | Penalties committed. |
| `pim` | Float64 | Penalty minutes. |
| `offsides` | Int64 | Offsides violations. |
| `shot_clock_expirations` | Int64 | Shot-clock violations. |
| `times_man_up` | Int64 | Man-up (power-play) chances. |
| `power_play_goals` | Int64 | Power-play goals. |
| `power_play_shots` | Int64 | Power-play shots. |
| `power_play_pct` | Float64 | Power-play goals per man-up chance. |
| `times_short_handed` | Int64 | Man-down (short-handed) spells. |
| `short_handed_goals` | Int64 | Short-handed goals. |
| `short_handed_shots` | Int64 | Short-handed shots. |
| `short_handed_pct` | Float64 | Short-handed goals per man-down chance. |
| `power_play_goals_against` | Int64 | Power-play goals allowed. |
| `power_play_shots_against` | Int64 | Power-play shots allowed. |
| `power_play_goals_against_pct` | Float64 | Power-play goals allowed per opponent man-up chance. |
| `short_handed_goals_against` | Int64 | Short-handed goals allowed. |
| `short_handed_shots_against` | Int64 | Short-handed shots allowed. |
| `short_handed_goals_against_pct` | Float64 | Short-handed goals allowed per man-up chance. |
| `man_down_pct` | Float64 | Man-down (penalty kill) success rate. |
| `total_passes` | Int64 | Passes made, as the league's tracking counts them. |
| `touches` | Int64 | Touches (possessions of the ball by the player). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pll_team_season_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An unknown game, team or player slug, or a 2019-2020 game slug on a per-game route (HTTP 404). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected a parameter (a missing year, a season segment or stat name outside its allowed list; HTTP 400). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 origin refusal, any other failed fetch, or a 200 that is not the `{data: ...}` envelope. |

### Example {#pll_team_season_stats-example}

```python
pll_team_season_stats(year=2026)
```

## pll_team_stats

One team's record and stat lines for a season: one row per segment it played (regular season, playoffs).

**Endpoint URL:** `GET https://stats.premierlacrosseleague.com/api/v4/teams/{team_slug}/stats`

**Valid URL:** [https://stats.premierlacrosseleague.com/api/v4/teams/waterdogs/stats?year=2026](https://stats.premierlacrosseleague.com/api/v4/teams/waterdogs/stats?year=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_slug` | `team_slug` |  | `Y` |  | Team slug (team_slug in pll_team_season_stats), e.g. waterdogs, boston-cannons. |
| `year` | `year` |  |  | `Y` | Season; omitted, the current one. |

### Returns {#pll_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | String | Team code (e.g. WAT). |
| `team_slug` | String | Team slug, the id pll_team_stats and pll_team_roster take (e.g. waterdogs). |
| `full_name` | String | Team name (e.g. Waterdogs). |
| `location` | String | City of the game, or the team's city or region. |
| `location_code` | String | Three-letter location code (e.g. PHI). |
| `conference` | String | Conference (eastern or western; null before conferences, 2019-2020). |
| `wins` | Int64 | Wins in the season or segment (the season record, repeated on each segment row). |
| `losses` | Int64 | Losses in the season or segment (the season record, repeated on each segment row). |
| `ties` | Int64 | Ties in the season or segment (the season record, repeated on each segment row). |
| `seed` | Int64 | Playoff seed (null until set). |
| `season` | Int64 | Season (from the body, or from the request when the body does not say). |
| `season_segment` | String | Season segment: regular, post (playoffs), champseries (Championship Series) or allstar. |
| `games_played` | Int64 | Games played in the season segment (or career, on career leaderboards). |
| `scores` | Int64 | Points scored (a 2-point goal counts 2). |
| `scores_against` | Int64 | Points allowed (a 2-point goal counts 2). |
| `scores_pg` | Float64 | Points scored per game. |
| `goals` | Int64 | Goals (1-point plus 2-point; goals = one_point_goals + two_point_goals). |
| `one_point_goals` | Int64 | 1-point goals (inside the arc). |
| `two_point_goals` | Int64 | 2-point goals (from beyond the arc). |
| `assists` | Int64 | Assists credited (passes leading directly to a goal). |
| `shots` | Int64 | Shots taken (on and off goal). |
| `shots_pg` | Float64 | Shots per game. |
| `shot_pct` | Float64 | Goals per shot (goals / shots). |
| `shots_on_goal` | Int64 | Shots that were on goal (saved or scored). |
| `shots_on_goal_pct` | Float64 | Shots on goal per shot. |
| `two_point_shots` | Int64 | Shots from beyond the arc. |
| `two_point_shot_pct` | Float64 | Goals per shot from beyond the arc. |
| `two_point_shots_on_goal` | Int64 | 2-point shots on goal. |
| `two_point_shots_on_goal_pct` | Float64 | 2-point shots on goal per 2-point shot. |
| `goals_against` | Int64 | Goals allowed (each goal once, whatever its point value). |
| `two_point_goals_against` | Int64 | 2-point goals allowed. |
| `saa` | Float64 | Scores against per 48 minutes on the field (scores_against / (tof / 2880); checked on the captures). |
| `saves` | Int64 | Saves made by the goalie (or team). |
| `save_pct` | Float64 | Saves per shot on goal faced (saves / (saves + goals_against); checked on the captures). |
| `clean_saves` | Int64 | Saves the goalie controlled cleanly (no rebound). |
| `messy_saves` | Int64 | Saves that left a rebound. |
| `clean_save_pct` | Float64 | Clean saves per save (clean_saves / saves). |
| `ground_balls` | Int64 | Ground balls picked up. |
| `turnovers` | Int64 | Turnovers committed. |
| `caused_turnovers` | Int64 | Caused turnovers (takeaways credited to a defender). |
| `faceoffs_won` | Int64 | Faceoffs won by the player or team. |
| `faceoffs_lost` | Int64 | Faceoffs lost by the player or team. |
| `faceoffs` | Int64 | Faceoffs taken (faceoff_pct = faceoffs_won / faceoffs; won + lost can fall short of it, as in the captures). |
| `faceoff_pct` | Float64 | Faceoffs won per faceoff taken (faceoffs_won / faceoffs). |
| `clears` | Int64 | Successful clears. |
| `clear_attempts` | Int64 | Clearing attempts (moving the ball from the defensive to the offensive half). |
| `clear_pct` | Float64 | Successful clears per attempt (clears / clear_attempts). |
| `rides` | Int64 | Successful rides (opponent clears stopped). |
| `ride_attempts` | Int64 | Riding attempts (opponent clears contested). |
| `rides_pct` | Float64 | Successful rides per ride attempt. |
| `num_penalties` | Int64 | Penalties committed. |
| `pim` | Float64 | Penalty minutes. |
| `offsides` | Int64 | Offsides violations. |
| `shot_clock_expirations` | Int64 | Shot-clock violations. |
| `times_man_up` | Int64 | Man-up (power-play) chances. |
| `power_play_goals` | Int64 | Power-play goals. |
| `power_play_shots` | Int64 | Power-play shots. |
| `power_play_pct` | Float64 | Power-play goals per man-up chance. |
| `times_short_handed` | Int64 | Man-down (short-handed) spells. |
| `short_handed_goals` | Int64 | Short-handed goals. |
| `short_handed_shots` | Int64 | Short-handed shots. |
| `short_handed_pct` | Float64 | Short-handed goals per man-down chance. |
| `power_play_goals_against` | Int64 | Power-play goals allowed. |
| `power_play_shots_against` | Int64 | Power-play shots allowed. |
| `power_play_goals_against_pct` | Float64 | Power-play goals allowed per opponent man-up chance. |
| `short_handed_goals_against` | Int64 | Short-handed goals allowed. |
| `short_handed_shots_against` | Int64 | Short-handed shots allowed. |
| `short_handed_goals_against_pct` | Float64 | Short-handed goals allowed per man-up chance. |
| `man_down_pct` | Float64 | Man-down (penalty kill) success rate. |
| `total_passes` | Int64 | Passes made, as the league's tracking counts them. |
| `touches` | Int64 | Touches (possessions of the ball by the player). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pll_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An unknown game, team or player slug, or a 2019-2020 game slug on a per-game route (HTTP 404). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected a parameter (a missing year, a season segment or stat name outside its allowed list; HTTP 400). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 origin refusal, any other failed fetch, or a 200 that is not the `{data: ...}` envelope. |

### Example {#pll_team_stats-example}

```python
pll_team_stats(team_slug='waterdogs', year=2026)
```

## pll_team_roster

One team's roster: one row per player (jersey, position, bio, injury and contract status).

**Endpoint URL:** `GET https://stats.premierlacrosseleague.com/api/v4/teams/{team_slug}/roster`

**Valid URL:** [https://stats.premierlacrosseleague.com/api/v4/teams/waterdogs/roster?year=2026&seasonSegment=regular](https://stats.premierlacrosseleague.com/api/v4/teams/waterdogs/roster?year=2026&seasonSegment=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_slug` | `team_slug` |  | `Y` |  | Team slug (team_slug in pll_team_season_stats), e.g. waterdogs. |
| `year` | `year` |  |  | `Y` | Season; omitted, the current one. |
| `seasonSegment` | `season_segment` |  |  | `Y` | Season segment. |

### Returns {#pll_team_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_slug` | String | Team slug, the id pll_team_stats and pll_team_roster take (e.g. waterdogs). |
| `season` | Int64 | Season (from the body, or from the request when the body does not say). |
| `season_segment` | String | Season segment: regular, post (playoffs), champseries (Championship Series) or allstar. |
| `player_id` | String | Six-digit league player id (a STRING, e.g. 003009); the column to join players on. |
| `player_slug` | String | Player slug, the id pll_player and pll_player_seasons take (e.g. michael-sowers). |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `last_name_suffix` | String | Name suffix (Jr., III; null or empty when none). |
| `nickname` | String | Nickname (null when none). |
| `jersey_number` | String | Jersey number (a STRING). |
| `position` | String | Position code: A, M, SSDM, LSM, D, FO, G. |
| `position_name` | String | Position name (Attack, Midfield, Defensive Midfield, Long Stick Midfield, Defense, Faceoff, Goalie). |
| `dob` | String | Date of birth (YYYY-MM-DD). |
| `age` | Int64 | Age in years on the day of the request. |
| `height` | String | Height (feet and inches, e.g. 6'1). |
| `weight` | Int64 | Weight in pounds. |
| `handedness` | String | Shooting hand (R, L, or R/L). |
| `country` | String | Country of birth. |
| `country_code` | String | Country code (ISO 3166-1 alpha-3). |
| `hometown` | String | Player's hometown (city, state). |
| `high_school` | String | High school the player attended. |
| `college` | String | College the player attended. |
| `college_year` | Int64 | Final college season (graduation year). |
| `club_team` | String | Club (amateur) team. |
| `experience` | Int64 | PLL seasons of experience. |
| `is_captain` | Boolean | true for a team captain. |
| `scratch` | Boolean | true when scratched from the lineup. |
| `injury_status` | String | Injury status code (H healthy, Q questionable, IR injured reserve, as observed). |
| `injury_description` | String | Injury description (null when healthy). |
| `status` | String | Player status (current on a roster; active or retired on career leaderboards). |
| `contract_status` | String | Final season under contract (e.g. 2028). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pll_team_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An unknown game, team or player slug, or a 2019-2020 game slug on a per-game route (HTTP 404). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected a parameter (a missing year, a season segment or stat name outside its allowed list; HTTP 400). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 origin refusal, any other failed fetch, or a 200 that is not the `{data: ...}` envelope. |

### Example {#pll_team_roster-example}

```python
pll_team_roster(team_slug='waterdogs', year=2026)
```
