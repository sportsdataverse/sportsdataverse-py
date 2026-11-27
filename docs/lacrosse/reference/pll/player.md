# Lacrosse — Premier Lacrosse League stats API (stats.premierlacrosseleague.com; runtime only, no key, the site's Referer) — Player

> Lacrosse — Premier Lacrosse League stats API (stats.premierlacrosseleague.com; runtime only, no key, the site's Referer) — Player — function reference in sdv-py, the SportsDataverse Python package.

## pll_player_box

A game's player box score: one row per player (points, 1- and 2-point goals, assists, shots, ground balls, turnovers, faceoffs, saves, penalties, touches, passes).

**Endpoint URL:** `GET https://stats.premierlacrosseleague.com/api/v4/events/{event_slug}/players/stats`

**Valid URL:** [https://stats.premierlacrosseleague.com/api/v4/events/2026-ev-4/players/stats](https://stats.premierlacrosseleague.com/api/v4/events/2026-ev-4/players/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_slug` | `event_slug` |  | `Y` |  | Game slug (event_slug in pll_schedule), 2021 onward. |

### Returns {#pll_player_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `event_slug` | String | Game slug, the id every per-game wrapper takes (e.g. 2026-ev-4; the format changes by season). |
| `player_id` | String | Six-digit league player id (a STRING, e.g. 003009); the column to join players on. |
| `player_slug` | String | Player slug, the id pll_player and pll_player_seasons take (e.g. michael-sowers). |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `team_id` | String | Team code (e.g. WAT). |
| `position` | String | Position code: A, M, SSDM, LSM, D, FO, G. |
| `position_name` | String | Position name (Attack, Midfield, Defensive Midfield, Long Stick Midfield, Defense, Faceoff, Goalie). |
| `jersey_number` | String | Jersey number (a STRING). |
| `points` | Int64 | Points (one_point_goals + 2 x two_point_goals + assists; checked on the captures). |
| `goals` | Int64 | Goals (1-point plus 2-point; goals = one_point_goals + two_point_goals). |
| `one_point_goals` | Int64 | 1-point goals (inside the arc). |
| `two_point_goals` | Int64 | 2-point goals (from beyond the arc). |
| `assists` | Int64 | Assists credited (passes leading directly to a goal). |
| `shots` | Int64 | Shots taken (on and off goal). |
| `shot_pct` | Float64 | Goals per shot (goals / shots). |
| `shots_on_goal` | Int64 | Shots that were on goal (saved or scored). |
| `two_point_shots` | Int64 | Shots from beyond the arc. |
| `ground_balls` | Int64 | Ground balls picked up. |
| `turnovers` | Int64 | Turnovers committed. |
| `caused_turnovers` | Int64 | Caused turnovers (takeaways credited to a defender). |
| `faceoffs_won` | Int64 | Faceoffs won by the player or team. |
| `faceoffs_lost` | Int64 | Faceoffs lost by the player or team. |
| `faceoffs` | Int64 | Faceoffs taken (faceoff_pct = faceoffs_won / faceoffs; won + lost can fall short of it, as in the captures). |
| `faceoff_pct` | Float64 | Faceoffs won per faceoff taken (faceoffs_won / faceoffs). |
| `saves` | Int64 | Saves made by the goalie (or team). |
| `save_pct` | Float64 | Saves per shot on goal faced (saves / (saves + goals_against); checked on the captures). |
| `clean_saves` | Int64 | Saves the goalie controlled cleanly (no rebound). |
| `messy_saves` | Int64 | Saves that left a rebound. |
| `clean_save_pct` | Float64 | Clean saves per save (clean_saves / saves). |
| `scores_against` | Int64 | Points allowed (a 2-point goal counts 2). |
| `goals_against` | Int64 | Goals allowed (each goal once, whatever its point value). |
| `two_point_goals_against` | Int64 | 2-point goals allowed. |
| `saa` | Float64 | Scores against per 48 minutes on the field (scores_against / (tof / 2880); checked on the captures). |
| `gaa` | Float64 | Scores against per game played (scores_against / games_played). |
| `two_pt_gaa` | Float64 | 2-point goals against per game played. |
| `num_penalties` | Int64 | Penalties committed. |
| `pim` | Float64 | Penalty minutes. |
| `assist_opportunities` | Int64 | Passes that led to a shot (a potential assist). |
| `total_passes` | Int64 | Passes made, as the league's tracking counts them. |
| `touches` | Int64 | Touches (possessions of the ball by the player). |
| `games_played` | Int64 | Games played in the season segment (or career, on career leaderboards). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pll_player_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An unknown game, team or player slug, or a 2019-2020 game slug on a per-game route (HTTP 404). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected a parameter (a missing year, a season segment or stat name outside its allowed list; HTTP 400). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 origin refusal, any other failed fetch, or a 200 that is not the `{data: ...}` envelope. |

### Example {#pll_player_box-example}

```python
pll_player_box(event_slug='2026-ev-4')
```

## pll_player_season_stats

Every player's stat line for a season segment: one row per player (scoring, shooting, faceoffs, goalie, passing and per-touch rates).

**Endpoint URL:** `GET https://stats.premierlacrosseleague.com/api/v4/players/season-stats`

**Valid URL:** [https://stats.premierlacrosseleague.com/api/v4/players/season-stats?year=2026&seasonSegment=regular](https://stats.premierlacrosseleague.com/api/v4/players/season-stats?year=2026&seasonSegment=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Season; omitted, the current one. |
| `seasonSegment` | `season_segment` |  |  | `Y` | Season segment (the API requires one). |

### Returns {#pll_player_season_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (from the body, or from the request when the body does not say); null on every row when neither does, as when the call leaves year out. |
| `season_segment` | String | Season segment: regular, post (playoffs), champseries (Championship Series) or allstar. |
| `player_id` | String | Six-digit league player id (a STRING, e.g. 003009); the column to join players on. |
| `player_slug` | String | Player slug, the id pll_player and pll_player_seasons take (e.g. michael-sowers). |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `jersey_number` | String | Jersey number (a STRING). |
| `position` | String | Position code: A, M, SSDM, LSM, D, FO, G. |
| `position_name` | String | Position name (Attack, Midfield, Defensive Midfield, Long Stick Midfield, Defense, Faceoff, Goalie). |
| `experience` | Int64 | PLL seasons of experience. |
| `team_id` | String | Team code (e.g. WAT). |
| `team_name` | String | Team name (e.g. Waterdogs). |
| `points` | Int64 | Points (one_point_goals + 2 x two_point_goals + assists; checked on the captures). |
| `scoring_points` | Int64 | Points from goals alone (one_point_goals + 2 x two_point_goals). |
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
| `ground_balls` | Int64 | Ground balls picked up. |
| `turnovers` | Int64 | Turnovers committed. |
| `caused_turnovers` | Int64 | Caused turnovers (takeaways credited to a defender). |
| `faceoffs_won` | Int64 | Faceoffs won by the player or team. |
| `faceoffs_lost` | Int64 | Faceoffs lost by the player or team. |
| `faceoffs` | Int64 | Faceoffs taken (faceoff_pct = faceoffs_won / faceoffs; won + lost can fall short of it, as in the captures). |
| `faceoff_pct` | Float64 | Faceoffs won per faceoff taken (faceoffs_won / faceoffs). |
| `saves` | Int64 | Saves made by the goalie (or team). |
| `save_pct` | Float64 | Saves per shot on goal faced (saves / (saves + goals_against); checked on the captures). |
| `clean_saves` | Int64 | Saves the goalie controlled cleanly (no rebound). |
| `messy_saves` | Int64 | Saves that left a rebound. |
| `clean_save_pct` | Float64 | Clean saves per save (clean_saves / saves). |
| `scores_against` | Int64 | Points allowed (a 2-point goal counts 2). |
| `goals_against` | Int64 | Goals allowed (each goal once, whatever its point value). |
| `two_point_goals_against` | Int64 | 2-point goals allowed. |
| `saa` | Float64 | Scores against per 48 minutes on the field (scores_against / (tof / 2880); checked on the captures). |
| `gaa` | Float64 | Scores against per game played (scores_against / games_played). |
| `two_pt_gaa` | Float64 | 2-point goals against per game played. |
| `goalie_wins` | Int64 | Wins as the goalie of record. |
| `goalie_losses` | Int64 | Losses as the goalie of record. |
| `goalie_ties` | Int64 | Ties as the goalie of record. |
| `num_penalties` | Int64 | Penalties committed. |
| `pim` | Float64 | Penalty minutes. |
| `pim_value` | Float64 | Penalty time as minutes (0.5 = a 30-second penalty). |
| `power_play_goals` | Int64 | Power-play goals. |
| `power_play_shots` | Int64 | Power-play shots. |
| `short_handed_goals` | Int64 | Short-handed goals. |
| `short_handed_shots` | Int64 | Short-handed shots. |
| `short_handed_goals_against` | Int64 | Short-handed goals allowed. |
| `power_play_goals_against` | Int64 | Power-play goals allowed. |
| `tof` | Int64 | Time on the field, seconds. |
| `plus_minus` | Int64 | Plus-minus as the league reports it. |
| `shot_turnovers` | Int64 | Shots that ended the possession (the league's tracking count). |
| `touches` | Int64 | Touches (possessions of the ball by the player). |
| `total_passes` | Int64 | Passes made, as the league's tracking counts them. |
| `unassisted_goals` | Int64 | Unassisted goals. |
| `assisted_goals` | Int64 | Goals scored off an assist. |
| `pass_rate` | Float64 | Passes per touch (total_passes / touches). |
| `shot_rate` | Float64 | Shots per touch (shots / touches). |
| `goal_rate` | Float64 | Goals per touch (goals / touches). |
| `assist_rate` | Float64 | Assists per touch (assists / touches). |
| `turnover_rate` | Float64 | Turnovers per touch (turnovers / touches). |
| `games_played` | Int64 | Games played in the season segment (or career, on career leaderboards). |
| `games_started` | Int64 | Games the player started. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pll_player_season_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An unknown game, team or player slug, or a 2019-2020 game slug on a per-game route (HTTP 404). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected a parameter (a missing year, a season segment or stat name outside its allowed list; HTTP 400). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 origin refusal, any other failed fetch, or a 200 that is not the `{data: ...}` envelope. |

### Example {#pll_player_season_stats-example}

```python
pll_player_season_stats(year=2026)
```

## pll_player_advanced_stats

Advanced player stats for a season: one row per player (settled, fast-break and assisted goals, assist types, left- and right-handed shooting).

**Endpoint URL:** `GET https://stats.premierlacrosseleague.com/api/v4/players/stats/advanced`

**Valid URL:** [https://stats.premierlacrosseleague.com/api/v4/players/stats/advanced?year=2026&seasonSegment=regular](https://stats.premierlacrosseleague.com/api/v4/players/stats/advanced?year=2026&seasonSegment=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Season; omitted, the current one. |
| `seasonSegment` | `season_segment` |  |  | `Y` | Season segment. |

### Returns {#pll_player_advanced_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (from the body, or from the request when the body does not say); null on every row when neither does, as when the call leaves year out. |
| `season_segment` | String | Season segment: regular, post (playoffs), champseries (Championship Series) or allstar. |
| `player_id` | String | Six-digit league player id (a STRING, e.g. 003009); the column to join players on. |
| `player_slug` | String | Player slug, the id pll_player and pll_player_seasons take (e.g. michael-sowers). |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `jersey_number` | String | Jersey number (a STRING). |
| `position` | String | Position code: A, M, SSDM, LSM, D, FO, G. |
| `position_name` | String | Position name (Attack, Midfield, Defensive Midfield, Long Stick Midfield, Defense, Faceoff, Goalie). |
| `experience` | Int64 | PLL seasons of experience. |
| `team_id` | String | Team code (e.g. WAT). |
| `team_name` | String | Team name (e.g. Waterdogs). |
| `goals` | Int64 | Goals (1-point plus 2-point; goals = one_point_goals + two_point_goals). |
| `unassisted_goals` | Int64 | Unassisted goals. |
| `assisted_goals` | Int64 | Goals scored off an assist. |
| `settled_goals` | Int64 | Goals in the settled offense. |
| `fastbreak_goals` | Int64 | Goals in transition (fast break). |
| `substitution_goals` | Int64 | Goals the league's tracking tags as substitution goals (its label; the API does not define it). |
| `doorstep_goals` | Int64 | Goals the league's tracking tags as doorstep goals (in tight to the crease; its label). |
| `power_play_goals` | Int64 | Power-play goals. |
| `assist_opportunities` | Int64 | Passes that led to a shot (a potential assist). |
| `settled_assists` | Int64 | Assists in the settled offense. |
| `power_play_assists` | Int64 | Assists on the power play. |
| `fastbreak_assists` | Int64 | Assists in transition (fast break). |
| `dodge_assists` | Int64 | Assists off a dodge. |
| `pnr_assists` | Int64 | Assists off a pick-and-roll. |
| `unassisted_shots` | Int64 | Unassisted shots. |
| `assisted_shots` | Int64 | Shots taken off a pass that would be an assist if scored. |
| `unassisted_shot_pct` | Float64 | Goals per shot on unassisted shots. |
| `assisted_shot_pct` | Float64 | Goals per shot on assisted shots. |
| `pipe_shots` | Int64 | Shots the league's tracking tags as pipe shots (its label; the API does not define it). |
| `lh_shots` | Int64 | Left-handed shots. |
| `lh_goals` | Int64 | Left-handed goals. |
| `lh_shot_pct` | Float64 | Goals per shot, left-handed. |
| `rh_shots` | Int64 | Right-handed shots. |
| `rh_goals` | Int64 | Right-handed goals. |
| `rh_shot_pct` | Float64 | Goals per shot, right-handed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pll_player_advanced_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An unknown game, team or player slug, or a 2019-2020 game slug on a per-game route (HTTP 404). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected a parameter (a missing year, a season segment or stat name outside its allowed list; HTTP 400). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 origin refusal, any other failed fetch, or a 200 that is not the `{data: ...}` envelope. |

### Example {#pll_player_advanced_stats-example}

```python
pll_player_advanced_stats(year=2026)
```

## pll_player

One player's profile: team, position, jersey, birth date, height, weight, hometown, school, injury and contract status, seasons played, biography.

**Endpoint URL:** `GET https://stats.premierlacrosseleague.com/api/v4/players/{player_slug}`

**Valid URL:** [https://stats.premierlacrosseleague.com/api/v4/players/michael-sowers](https://stats.premierlacrosseleague.com/api/v4/players/michael-sowers)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_slug` | `player_slug` |  | `Y` |  | Player slug (player_slug in pll_player_season_stats), e.g. michael-sowers. |
| `year` | `year` |  |  | `Y` | Season the profile is read for; omitted, the current one. |

### Returns {#pll_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | String | Six-digit league player id (a STRING, e.g. 003009); the column to join players on. |
| `player_slug` | String | Player slug, the id pll_player and pll_player_seasons take (e.g. michael-sowers). |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `last_name_suffix` | String | Name suffix (Jr., III; null or empty when none). |
| `nickname` | String | Nickname (null when none). |
| `team_id` | String | Team code (e.g. WAT). |
| `team_name` | String | Team name (e.g. Waterdogs). |
| `position` | String | Position code: A, M, SSDM, LSM, D, FO, G. |
| `position_name` | String | Position name (Attack, Midfield, Defensive Midfield, Long Stick Midfield, Defense, Faceoff, Goalie). |
| `jersey_number` | String | Jersey number (a STRING). |
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
| `exp_from_year` | Int64 | First professional season. |
| `is_captain` | Boolean | true for a team captain. |
| `scratch` | Boolean | true when scratched from the lineup. |
| `injury_status` | String | Injury status code (H healthy, Q questionable, IR injured reserve, as observed). |
| `injury_description` | String | Injury description (null when healthy). |
| `status` | String | Player status (current on a roster; active or retired on career leaderboards). |
| `contract_status` | String | Final season under contract (e.g. 2028). |
| `all_years` | Int64 | Seasons the player has a PLL record in: a list of integers (List(Int64)). |
| `bio` | String | Player biography text. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pll_player-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An unknown game, team or player slug, or a 2019-2020 game slug on a per-game route (HTTP 404). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected a parameter (a missing year, a season segment or stat name outside its allowed list; HTTP 400). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 origin refusal, any other failed fetch, or a 200 that is not the `{data: ...}` envelope. |

### Example {#pll_player-example}

```python
pll_player(player_slug='michael-sowers')
```

## pll_player_seasons

One player's stat line for every season and segment he played.

**Endpoint URL:** `GET https://stats.premierlacrosseleague.com/api/v4/players/{player_slug}/stats/seasons`

**Valid URL:** [https://stats.premierlacrosseleague.com/api/v4/players/michael-sowers/stats/seasons](https://stats.premierlacrosseleague.com/api/v4/players/michael-sowers/stats/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_slug` | `player_slug` |  | `Y` |  | Player slug (player_slug in pll_player_season_stats), e.g. michael-sowers. |

### Returns {#pll_player_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_slug` | String | Player slug, the id pll_player and pll_player_seasons take (e.g. michael-sowers). |
| `season` | Int64 | Season (from the body, or from the request when the body does not say); null on every row when neither does, as when the call leaves year out. |
| `season_segment` | String | Season segment: regular, post (playoffs), champseries (Championship Series) or allstar. |
| `team_id` | String | Team code (e.g. WAT). |
| `team_name` | String | Team name (e.g. Waterdogs). |
| `points` | Int64 | Points (one_point_goals + 2 x two_point_goals + assists; checked on the captures). |
| `scoring_points` | Int64 | Points from goals alone (one_point_goals + 2 x two_point_goals). |
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
| `ground_balls` | Int64 | Ground balls picked up. |
| `turnovers` | Int64 | Turnovers committed. |
| `caused_turnovers` | Int64 | Caused turnovers (takeaways credited to a defender). |
| `faceoffs_won` | Int64 | Faceoffs won by the player or team. |
| `faceoffs_lost` | Int64 | Faceoffs lost by the player or team. |
| `faceoffs` | Int64 | Faceoffs taken (faceoff_pct = faceoffs_won / faceoffs; won + lost can fall short of it, as in the captures). |
| `faceoff_pct` | Float64 | Faceoffs won per faceoff taken (faceoffs_won / faceoffs). |
| `saves` | Int64 | Saves made by the goalie (or team). |
| `save_pct` | Float64 | Saves per shot on goal faced (saves / (saves + goals_against); checked on the captures). |
| `clean_saves` | Int64 | Saves the goalie controlled cleanly (no rebound). |
| `messy_saves` | Int64 | Saves that left a rebound. |
| `clean_save_pct` | Float64 | Clean saves per save (clean_saves / saves). |
| `scores_against` | Int64 | Points allowed (a 2-point goal counts 2). |
| `goals_against` | Int64 | Goals allowed (each goal once, whatever its point value). |
| `two_point_goals_against` | Int64 | 2-point goals allowed. |
| `saa` | Float64 | Scores against per 48 minutes on the field (scores_against / (tof / 2880); checked on the captures). |
| `gaa` | Float64 | Scores against per game played (scores_against / games_played). |
| `two_pt_gaa` | Float64 | 2-point goals against per game played. |
| `goalie_wins` | Int64 | Wins as the goalie of record. |
| `goalie_losses` | Int64 | Losses as the goalie of record. |
| `goalie_ties` | Int64 | Ties as the goalie of record. |
| `num_penalties` | Int64 | Penalties committed. |
| `pim` | Float64 | Penalty minutes. |
| `pim_value` | Float64 | Penalty time as minutes (0.5 = a 30-second penalty). |
| `power_play_goals` | Int64 | Power-play goals. |
| `power_play_shots` | Int64 | Power-play shots. |
| `short_handed_goals` | Int64 | Short-handed goals. |
| `short_handed_shots` | Int64 | Short-handed shots. |
| `short_handed_goals_against` | Int64 | Short-handed goals allowed. |
| `power_play_goals_against` | Int64 | Power-play goals allowed. |
| `tof` | Int64 | Time on the field, seconds. |
| `plus_minus` | Int64 | Plus-minus as the league reports it. |
| `shot_turnovers` | Int64 | Shots that ended the possession (the league's tracking count). |
| `touches` | Int64 | Touches (possessions of the ball by the player). |
| `total_passes` | Int64 | Passes made, as the league's tracking counts them. |
| `unassisted_goals` | Int64 | Unassisted goals. |
| `assisted_goals` | Int64 | Goals scored off an assist. |
| `pass_rate` | Float64 | Passes per touch (total_passes / touches). |
| `shot_rate` | Float64 | Shots per touch (shots / touches). |
| `goal_rate` | Float64 | Goals per touch (goals / touches). |
| `assist_rate` | Float64 | Assists per touch (assists / touches). |
| `turnover_rate` | Float64 | Turnovers per touch (turnovers / touches). |
| `games_played` | Int64 | Games played in the season segment (or career, on career leaderboards). |
| `games_started` | Int64 | Games the player started. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pll_player_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An unknown game, team or player slug, or a 2019-2020 game slug on a per-game route (HTTP 404). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected a parameter (a missing year, a season segment or stat name outside its allowed list; HTTP 400). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 origin refusal, any other failed fetch, or a 200 that is not the `{data: ...}` envelope. |

### Example {#pll_player_seasons-example}

```python
pll_player_seasons(player_slug='michael-sowers')
```
