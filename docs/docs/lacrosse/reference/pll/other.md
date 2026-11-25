---
title: "Lacrosse — Premier Lacrosse League stats API (stats.premierlacrosseleague.com; runtime only, no key, the site's Referer) — Other"
sidebar_label: "Other"
sidebar_position: 3
description: "Lacrosse — Premier Lacrosse League stats API (stats.premierlacrosseleague.com; runtime only, no key, the site's Referer) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# Lacrosse — Premier Lacrosse League stats API (stats.premierlacrosseleague.com; runtime only, no key, the site's Referer) — Other

## pll_schedule

A season's schedule and results: one row per game (teams, score, UTC start time, segment, week, venue, broadcast and the event_slug every per-game wrapper takes).

**Endpoint URL:** `GET https://stats.premierlacrosseleague.com/api/v4/events`

**Valid URL:** [https://stats.premierlacrosseleague.com/api/v4/events?year=2026](https://stats.premierlacrosseleague.com/api/v4/events?year=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | Season, 2019 onward. |
| `includeCS` | `include_champ_series` |  |  | `Y` | True adds the Championship Series games (the pre-season sixes event); omitted, the regular season and playoffs only. |

### Returns {#pll_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Numeric game id (a STRING); the per-game routes take event_slug, not this. |
| `event_slug` | String | Game slug, the id every per-game wrapper takes (e.g. 2026-ev-4; the format changes by season). |
| `event_id` | String | League event id (e.g. 2026_game_4; LACROSSE_PLL_2019_... in 2019-2020). |
| `external_id` | String | External event id (not ESPN's event id; join ESPN on start_time and the team codes). |
| `league` | String | League code: PLL, WLL, INTLM (Global Lacrosse Games men) or INTLW (women). |
| `season` | Int64 | Season (from the body, or from the request when the body does not say). |
| `season_segment` | String | Season segment: regular, post (playoffs), champseries (Championship Series) or allstar. |
| `week` | Int64 | Week of the season (0 for the Championship Series). |
| `game_number` | Int64 | Game number within the season. |
| `start_time` | String | Start time, UTC. |
| `event_status` | Int64 | Game status code (3 on final 2021-2026 games; the 2019 games carry 2). |
| `period` | Int64 | Period (quarter; 5 and up is overtime). |
| `home_team_id` | String | Home team code (e.g. WAT; equals ESPN's abbreviation). |
| `home_team_name` | String | Home team name (e.g. Waterdogs). |
| `home_team_location` | String | Home team city or region. |
| `home_team_location_code` | String | Home team three-letter location code (e.g. PHI). |
| `home_team_conference` | String | Home team conference (eastern, western; null for GLG and WLL). |
| `away_team_id` | String | Away team code (e.g. WHP; equals ESPN's abbreviation). |
| `away_team_name` | String | Away team name (e.g. Whipsnakes). |
| `away_team_location` | String | Away team city or region. |
| `away_team_location_code` | String | Away team three-letter location code. |
| `away_team_conference` | String | Away team conference. |
| `home_score` | Int64 | Home team points (a 2-point goal counts 2). |
| `away_score` | Int64 | Away team points (the API's visitorScore). |
| `venue` | String | Venue (stadium) name. |
| `location` | String | City of the game, or the team's city or region. |
| `broadcaster` | String | Broadcast and streaming outlets, comma-separated. |
| `description` | String | Game description (round labels on Championship Series games; null on most), or the play text. |
| `url_streaming` | String | Streaming link. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pll_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An unknown game, team or player slug, or a 2019-2020 game slug on a per-game route (HTTP 404). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected a parameter (a missing year, a season segment or stat name outside its allowed list; HTTP 400). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 origin refusal, any other failed fetch, or a 200 that is not the `{data: ...}` envelope. |

### Example {#pll_schedule-example}

```python
pll_schedule(year=2026)
```

## pll_game

One game's header: teams and records, score by period, clock, venue, broadcast and the pregame win probability.

**Endpoint URL:** `GET https://stats.premierlacrosseleague.com/api/v4/events/{event_slug}`

**Valid URL:** [https://stats.premierlacrosseleague.com/api/v4/events/2026-ev-4](https://stats.premierlacrosseleague.com/api/v4/events/2026-ev-4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_slug` | `event_slug` |  | `Y` |  | Game slug (event_slug in pll_schedule), e.g. 2026-ev-4. |

### Returns {#pll_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Numeric game id (a STRING); the per-game routes take event_slug, not this. |
| `event_slug` | String | Game slug, the id every per-game wrapper takes (e.g. 2026-ev-4; the format changes by season). |
| `event_id` | String | League event id (e.g. 2026_game_4; LACROSSE_PLL_2019_... in 2019-2020). |
| `external_id` | String | External event id (not ESPN's event id; join ESPN on start_time and the team codes). |
| `league` | String | League code: PLL, WLL, INTLM (Global Lacrosse Games men) or INTLW (women). |
| `season` | Int64 | Season (from the body, or from the request when the body does not say). |
| `season_segment` | String | Season segment: regular, post (playoffs), champseries (Championship Series) or allstar. |
| `week` | Int64 | Week of the season (0 for the Championship Series). |
| `game_number` | Int64 | Game number within the season. |
| `start_time` | String | Start time, UTC. |
| `event_status` | Int64 | Game status code (3 on final 2021-2026 games; the 2019 games carry 2). |
| `period` | Int64 | Period (quarter; 5 and up is overtime). |
| `home_team_id` | String | Home team code (e.g. WAT; equals ESPN's abbreviation). |
| `home_team_name` | String | Home team name (e.g. Waterdogs). |
| `home_team_location` | String | Home team city or region. |
| `home_team_location_code` | String | Home team three-letter location code (e.g. PHI). |
| `home_team_conference` | String | Home team conference (eastern, western; null for GLG and WLL). |
| `away_team_id` | String | Away team code (e.g. WHP; equals ESPN's abbreviation). |
| `away_team_name` | String | Away team name (e.g. Whipsnakes). |
| `away_team_location` | String | Away team city or region. |
| `away_team_location_code` | String | Away team three-letter location code. |
| `away_team_conference` | String | Away team conference. |
| `home_score` | Int64 | Home team points (a 2-point goal counts 2). |
| `away_score` | Int64 | Away team points (the API's visitorScore). |
| `venue` | String | Venue (stadium) name. |
| `location` | String | City of the game, or the team's city or region. |
| `broadcaster` | String | Broadcast and streaming outlets, comma-separated. |
| `description` | String | Game description (round labels on Championship Series games; null on most), or the play text. |
| `url_streaming` | String | Streaming link. |
| `home_period_scores` | Int64 | Home team points per period. |
| `away_period_scores` | Int64 | Away team points per period. |
| `home_team_wins` | Int64 | Home team wins, as the API reports them on the game. |
| `home_team_losses` | Int64 | Home team losses, as the API reports them on the game. |
| `home_team_ties` | Int64 | Home team ties. |
| `away_team_wins` | Int64 | Away team wins, as the API reports them on the game. |
| `away_team_losses` | Int64 | Away team losses, as the API reports them on the game. |
| `away_team_ties` | Int64 | Away team ties. |
| `pregame_home_win_probability` | Float64 | Home team's pregame win probability (0-1). |
| `pregame_away_win_probability` | Float64 | Away team's pregame win probability (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pll_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An unknown game, team or player slug, or a 2019-2020 game slug on a per-game route (HTTP 404). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected a parameter (a missing year, a season segment or stat name outside its allowed list; HTTP 400). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 origin refusal, any other failed fetch, or a 200 that is not the `{data: ...}` envelope. |

### Example {#pll_game-example}

```python
pll_game(event_slug='2026-ev-4')
```

## pll_play_by_play

A game's play-by-play: one row per logged play (clock, score, play type, shot value, the players involved, the win probability after it).

**Endpoint URL:** `GET https://stats.premierlacrosseleague.com/api/v4/events/{event_slug}/playlogs`

**Valid URL:** [https://stats.premierlacrosseleague.com/api/v4/events/2026-ev-4/playlogs](https://stats.premierlacrosseleague.com/api/v4/events/2026-ev-4/playlogs)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_slug` | `event_slug` |  | `Y` |  | Game slug (event_slug in pll_schedule), 2021 onward. |

### Returns {#pll_play_by_play-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `event_slug` | String | Game slug, the id every per-game wrapper takes (e.g. 2026-ev-4; the format changes by season). |
| `play_index` | Int64 | 0-based position of the play in the log (the log is in game order). |
| `marker_id` | String | Play marker id (<type>-<sequence>). |
| `event_type` | String | Play type: faceoff, groundball, shot, goal, turnover, penalty, shotclockexpired, pregame, gameEnd. |
| `period` | Int64 | Period (quarter; 5 and up is overtime). |
| `minutes` | Int64 | Clock minutes remaining in the period. |
| `seconds` | Int64 | Clock seconds remaining in the period. |
| `seconds_passed` | Int64 | Game seconds elapsed at the play, cumulative across periods (720 per quarter). |
| `team_id` | String | Team code (e.g. WAT). |
| `description` | String | Game description (round labels on Championship Series games; null on most), or the play text. |
| `shot_type` | String | Shot value: 1_PT, 2_PT (MU on some man-up rows; null when not a shot). |
| `home_score` | Int64 | Home team points (a 2-point goal counts 2). The score after the play on shot and goal rows; other rows carry the final score in the 2026 logs (the running score in 2021), so forward-fill from shot rows. |
| `away_score` | Int64 | Away team points (the API's visitorScore). The score after the play on shot and goal rows; other rows carry the final score in the 2026 logs (the running score in 2021), so forward-fill from shot rows. |
| `home_win_probability` | Float64 | Home team's win probability after the play (0-1; null on some rows). |
| `away_win_probability` | Float64 | Away team's win probability after the play (0-1; null on some rows). |
| `penalty_length` | Int64 | Penalty length, seconds. |
| `penalty_description` | String | Penalty description. |
| `faceoff_winner_id` | String | Player id of the faceoff winner (a STRING). |
| `faceoff_loser_id` | String | Player id of the faceoff loser (a STRING). |
| `ground_ball_player_id` | String | Player id of the player who picked up the ground ball (a STRING). |
| `committed_turnover_id` | String | Player id of the player who committed the turnover (a STRING; null on a team turnover). |
| `caused_turnover_id` | String | Player id of the defender who caused the turnover (a STRING). |
| `shooter_id` | String | Player id of the shooter (a STRING). |
| `goalie_id` | String | Player id of the defending goalie (a STRING). |
| `offense_goalie_id` | String | Player id of the shooting team's goalie (a STRING). |
| `committed_penalty_id` | String | Player id of the penalised player (a STRING). |
| `shot_assist_id` | String | Player id of the assisting player on a goal (a STRING). |
| `assist_opportunity_player_id` | String | Player id of the passer on a shot that was a potential assist (a STRING). |
| `closest_defender_id` | String | Player id of the closest defender on the shot (a STRING). |
| `shot_on_goal` | Boolean | True when the shot was on goal (null where the log has no shot details, e.g. 2021). |
| `shot_saved` | Boolean | True when the shot was saved (null where the log has no shot details). |
| `save_type` | String | Save kind, clean or messy (null where the log has no shot details). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pll_play_by_play-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An unknown game, team or player slug, or a 2019-2020 game slug on a per-game route (HTTP 404). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected a parameter (a missing year, a season segment or stat name outside its allowed list; HTTP 400). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 origin refusal, any other failed fetch, or a 200 that is not the `{data: ...}` envelope. |

### Example {#pll_play_by_play-example}

```python
pll_play_by_play(event_slug='2026-ev-4')
```

## pll_game_insights

The league's written insights on a game: one row per streak, milestone or matchup note.

**Endpoint URL:** `GET https://stats.premierlacrosseleague.com/api/v4/events/{event_slug}/insights`

**Valid URL:** [https://stats.premierlacrosseleague.com/api/v4/events/2026-ev-4/insights](https://stats.premierlacrosseleague.com/api/v4/events/2026-ev-4/insights)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_slug` | `event_slug` |  | `Y` |  | Game slug (event_slug in pll_schedule), 2021 onward. |

### Returns {#pll_game_insights-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `event_slug` | String | Game slug, the id every per-game wrapper takes (e.g. 2026-ev-4; the format changes by season). |
| `insight_index` | Int64 | 0-based position of the insight in the league's list. |
| `title` | String | Headline of the insight (e.g. Save Streak). |
| `insight_type` | String | Insight kind (save-streak, faceoff-streak, ...). |
| `subject` | String | Player or team the insight is about. |
| `detail` | String | Insight text (e.g. 'has made 4 saves in a row'). |
| `team_id` | String | Team code (e.g. WAT). |
| `player_id` | String | Six-digit league player id (a STRING, e.g. 003009); the column to join players on. |
| `position` | String | Position code: A, M, SSDM, LSM, D, FO, G. |
| `jersey_number` | String | Jersey number (a STRING). |
| `period` | Int64 | Period (quarter; 5 and up is overtime). |
| `minutes` | Int64 | Clock minutes remaining in the period. |
| `seconds` | Int64 | Clock seconds remaining in the period. |
| `primary_image_url` | String | Main image of the insight. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pll_game_insights-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An unknown game, team or player slug, or a 2019-2020 game slug on a per-game route (HTTP 404). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected a parameter (a missing year, a season segment or stat name outside its allowed list; HTTP 400). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 origin refusal, any other failed fetch, or a 200 that is not the `{data: ...}` envelope. |

### Example {#pll_game_insights-example}

```python
pll_game_insights(event_slug='2026-ev-4')
```

## pll_standings

Standings: one row per team (record, scores for and against, conference record; seeds, null until the league sets them).

**Endpoint URL:** `GET https://stats.premierlacrosseleague.com/api/v4/standings`

**Valid URL:** [https://stats.premierlacrosseleague.com/api/v4/standings?year=2026](https://stats.premierlacrosseleague.com/api/v4/standings?year=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | Season, 2019 onward. |
| `champSeries` | `champ_series` |  |  | `Y` | True for the Championship Series table instead of the season's. |

### Returns {#pll_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | String | League code: PLL, WLL, INTLM (Global Lacrosse Games men) or INTLW (women). |
| `season` | Int64 | Season (from the body, or from the request when the body does not say). |
| `champ_series` | Boolean | True for the Championship Series table (the request's champ_series). |
| `team_id` | String | Team code (e.g. WAT). |
| `full_name` | String | Team name (e.g. Waterdogs). |
| `location` | String | City of the game, or the team's city or region. |
| `location_code` | String | Three-letter location code (e.g. PHI). |
| `conference` | String | Conference (eastern or western; null before conferences, 2019-2020). |
| `seed` | Int64 | Playoff seed (null until set). |
| `conference_seed` | Int64 | Seed within the conference (null until set). |
| `wins` | Int64 | Wins in the season or segment. |
| `losses` | Int64 | Losses in the season or segment. |
| `ties` | Int64 | Ties in the season or segment. |
| `scores` | Int64 | Points scored (a 2-point goal counts 2). |
| `scores_against` | Int64 | Points allowed (a 2-point goal counts 2). |
| `score_diff` | Int64 | Points scored minus points allowed. |
| `conference_wins` | Int64 | Wins against conference opponents. |
| `conference_losses` | Int64 | Losses against conference opponents. |
| `conference_ties` | Int64 | Ties against conference opponents. |
| `conference_scores` | Int64 | Points scored against conference opponents. |
| `conference_scores_against` | Int64 | Points allowed to conference opponents. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pll_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An unknown game, team or player slug, or a 2019-2020 game slug on a per-game route (HTTP 404). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected a parameter (a missing year, a season segment or stat name outside its allowed list; HTTP 400). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 origin refusal, any other failed fetch, or a 200 that is not the `{data: ...}` envelope. |

### Example {#pll_standings-example}

```python
pll_standings(year=2026)
```

## pll_season_leaders

A season's leaderboards in long format: one row per stat and ranked player.

**Endpoint URL:** `GET https://stats.premierlacrosseleague.com/api/v4/players/stats/season-leaders`

**Valid URL:** [https://stats.premierlacrosseleague.com/api/v4/players/stats/season-leaders?year=2026&seasonSegment=regular&limit=5](https://stats.premierlacrosseleague.com/api/v4/players/stats/season-leaders?year=2026&seasonSegment=regular&limit=5)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Season; omitted, the current one. |
| `seasonSegment` | `season_segment` |  |  | `Y` | Season segment. |
| `limit` | `limit` |  |  | `Y` | Players per stat; omitted, the API's default. |
| `stats` | `stats` |  |  | `Y` | Stat name or list of stat names in the API's spelling (points, assists, savePct, faceoffPct, ...); omitted, every stat. A name outside the API's list raises ValueError. |

### Returns {#pll_season_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (from the body, or from the request when the body does not say). |
| `season_segment` | String | Season segment: regular, post (playoffs), champseries (Championship Series) or allstar. |
| `stat` | String | Stat name, snake case (points, save_pct, faceoff_pct, ...). |
| `rank` | Int64 | 1-based position in the league's list for the stat (ties are not merged). |
| `player_id` | String | Six-digit league player id (a STRING, e.g. 003009); the column to join players on. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `jersey_number` | String | Jersey number (a STRING). |
| `position` | String | Position code: A, M, SSDM, LSM, D, FO, G. |
| `team_id` | String | Team code (e.g. WAT). |
| `team_name` | String | Team name (e.g. Waterdogs). |
| `stat_value` | Float64 | The stat's value. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pll_season_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An unknown game, team or player slug, or a 2019-2020 game slug on a per-game route (HTTP 404). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected a parameter (a missing year, a season segment or stat name outside its allowed list; HTTP 400). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 origin refusal, any other failed fetch, or a 200 that is not the `{data: ...}` envelope. |

### Example {#pll_season_leaders-example}

```python
pll_season_leaders(limit=5, year=2026)
```

## pll_career_leaders

All-time PLL leaderboards in long format: one row per stat and ranked player, with games and seasons played.

**Endpoint URL:** `GET https://stats.premierlacrosseleague.com/api/v4/players/stats/career-leaders`

**Valid URL:** [https://stats.premierlacrosseleague.com/api/v4/players/stats/career-leaders?stats=points&limit=5](https://stats.premierlacrosseleague.com/api/v4/players/stats/career-leaders?stats=points&limit=5)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `stats` | `stats` |  | `Y` |  | Stat name or list of stat names in the API's spelling (points, goals, assists, saves, groundBalls, faceoffsWon, ...); required. |
| `limit` | `limit` |  |  | `Y` | Players per stat; omitted, the API's default. |

### Returns {#pll_career_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `stat` | String | Stat name, snake case (points, save_pct, faceoff_pct, ...). |
| `rank` | Int64 | 1-based position in the league's list for the stat (ties are not merged). |
| `player_id` | String | Six-digit league player id (a STRING, e.g. 003009); the column to join players on. |
| `player_slug` | String | Player slug, the id pll_player and pll_player_seasons take (e.g. michael-sowers). |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `status` | String | Player status (current on a roster; active or retired on career leaderboards). |
| `team_id` | String | Team code (e.g. WAT). |
| `team_name` | String | Team name (e.g. Waterdogs). |
| `stat_value` | Float64 | The stat's value. |
| `games_played` | Int64 | Games played in the season segment (or career, on career leaderboards). |
| `seasons_played` | Int64 | PLL seasons played. |
| `avg_per_season` | Float64 | Career stat value per season played (stat_value / seasons_played). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pll_career_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An unknown game, team or player slug, or a 2019-2020 game slug on a per-game route (HTTP 404). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected a parameter (a missing year, a season segment or stat name outside its allowed list; HTTP 400). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 origin refusal, any other failed fetch, or a 200 that is not the `{data: ...}` envelope. |

### Example {#pll_career_leaders-example}

```python
pll_career_leaders(limit=5, stats='points')
```

## pll_glg_schedule

Global Lacrosse Games (national teams) schedule and results: one row per game, in the pll_schedule columns.

**Endpoint URL:** `GET https://stats.premierlacrosseleague.com/api/glg/{glg_league}/games`

**Valid URL:** [https://stats.premierlacrosseleague.com/api/glg/intlm/games?year=2026](https://stats.premierlacrosseleague.com/api/glg/intlm/games?year=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `glg_league` | `glg_league` |  |  | `Y` | intlm for the men's tournament, intlw for the women's. |
| `year` | `year` |  |  | `Y` | Edition year. |

### Returns {#pll_glg_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Numeric game id (a STRING); the per-game routes take event_slug, not this. |
| `event_slug` | String | Game slug, the id every per-game wrapper takes (e.g. 2026-ev-4; the format changes by season). |
| `event_id` | String | League event id (e.g. 2026_game_4; LACROSSE_PLL_2019_... in 2019-2020). |
| `external_id` | String | External event id (not ESPN's event id; join ESPN on start_time and the team codes). |
| `league` | String | League code: PLL, WLL, INTLM (Global Lacrosse Games men) or INTLW (women). |
| `season` | Int64 | Season (from the body, or from the request when the body does not say). |
| `season_segment` | String | Season segment: regular, post (playoffs), champseries (Championship Series) or allstar. |
| `week` | Int64 | Week of the season (0 for the Championship Series). |
| `game_number` | Int64 | Game number within the season. |
| `start_time` | String | Start time, UTC. |
| `event_status` | Int64 | Game status code (3 on final 2021-2026 games; the 2019 games carry 2). |
| `period` | Int64 | Period (quarter; 5 and up is overtime). |
| `home_team_id` | String | Home team code (e.g. WAT; equals ESPN's abbreviation). |
| `home_team_name` | String | Home team name (e.g. Waterdogs). |
| `home_team_location` | String | Home team city or region. |
| `home_team_location_code` | String | Home team three-letter location code (e.g. PHI). |
| `home_team_conference` | String | Home team conference (eastern, western; null for GLG and WLL). |
| `away_team_id` | String | Away team code (e.g. WHP; equals ESPN's abbreviation). |
| `away_team_name` | String | Away team name (e.g. Whipsnakes). |
| `away_team_location` | String | Away team city or region. |
| `away_team_location_code` | String | Away team three-letter location code. |
| `away_team_conference` | String | Away team conference. |
| `home_score` | Int64 | Home team points (a 2-point goal counts 2). |
| `away_score` | Int64 | Away team points (the API's visitorScore). |
| `venue` | String | Venue (stadium) name. |
| `location` | String | City of the game, or the team's city or region. |
| `broadcaster` | String | Broadcast and streaming outlets, comma-separated. |
| `description` | String | Game description (round labels on Championship Series games; null on most), or the play text. |
| `url_streaming` | String | Streaming link. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pll_glg_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An unknown game, team or player slug, or a 2019-2020 game slug on a per-game route (HTTP 404). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected a parameter (a missing year, a season segment or stat name outside its allowed list; HTTP 400). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 origin refusal, any other failed fetch, or a 200 that is not the `{data: ...}` envelope. |

### Example {#pll_glg_schedule-example}

```python
pll_glg_schedule(glg_league='intlm', year=2026)
```

## pll_glg_standings

Global Lacrosse Games standings: one row per national team, in the pll_standings columns.

**Endpoint URL:** `GET https://stats.premierlacrosseleague.com/api/glg/{glg_league}/standings`

**Valid URL:** [https://stats.premierlacrosseleague.com/api/glg/intlm/standings?year=2026](https://stats.premierlacrosseleague.com/api/glg/intlm/standings?year=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `glg_league` | `glg_league` |  |  | `Y` | intlm for the men's tournament, intlw for the women's. |
| `year` | `year` |  |  | `Y` | Edition year. |

### Returns {#pll_glg_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | String | League code: PLL, WLL, INTLM (Global Lacrosse Games men) or INTLW (women). |
| `season` | Int64 | Season (from the body, or from the request when the body does not say). |
| `champ_series` | Boolean | True for the Championship Series table (the request's champ_series). |
| `team_id` | String | Team code (e.g. WAT). |
| `full_name` | String | Team name (e.g. Waterdogs). |
| `location` | String | City of the game, or the team's city or region. |
| `location_code` | String | Three-letter location code (e.g. PHI). |
| `conference` | String | Conference (eastern or western; null before conferences, 2019-2020). |
| `seed` | Int64 | Playoff seed (null until set). |
| `conference_seed` | Int64 | Seed within the conference (null until set). |
| `wins` | Int64 | Wins in the season or segment. |
| `losses` | Int64 | Losses in the season or segment. |
| `ties` | Int64 | Ties in the season or segment. |
| `scores` | Int64 | Points scored (a 2-point goal counts 2). |
| `scores_against` | Int64 | Points allowed (a 2-point goal counts 2). |
| `score_diff` | Int64 | Points scored minus points allowed. |
| `conference_wins` | Int64 | Wins against conference opponents. |
| `conference_losses` | Int64 | Losses against conference opponents. |
| `conference_ties` | Int64 | Ties against conference opponents. |
| `conference_scores` | Int64 | Points scored against conference opponents. |
| `conference_scores_against` | Int64 | Points allowed to conference opponents. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pll_glg_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An unknown game, team or player slug, or a 2019-2020 game slug on a per-game route (HTTP 404). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected a parameter (a missing year, a season segment or stat name outside its allowed list; HTTP 400). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 origin refusal, any other failed fetch, or a 200 that is not the `{data: ...}` envelope. |

### Example {#pll_glg_standings-example}

```python
pll_glg_standings(glg_league='intlm', year=2026)
```

## pll_wll_schedule

Women's Lacrosse League schedule and results: one row per game, in the pll_schedule columns (the proxy serves no other WLL route).

**Endpoint URL:** `GET https://stats.premierlacrosseleague.com/api/wll/v1/events`

**Valid URL:** [https://stats.premierlacrosseleague.com/api/wll/v1/events?year=2026](https://stats.premierlacrosseleague.com/api/wll/v1/events?year=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | Season. |

### Returns {#pll_wll_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Numeric game id (a STRING); the per-game routes take event_slug, not this. |
| `event_slug` | String | Game slug, the id every per-game wrapper takes (e.g. 2026-ev-4; the format changes by season). |
| `event_id` | String | League event id (e.g. 2026_game_4; LACROSSE_PLL_2019_... in 2019-2020). |
| `external_id` | String | External event id (not ESPN's event id; join ESPN on start_time and the team codes). |
| `league` | String | League code: PLL, WLL, INTLM (Global Lacrosse Games men) or INTLW (women). |
| `season` | Int64 | Season (from the body, or from the request when the body does not say). |
| `season_segment` | String | Season segment: regular, post (playoffs), champseries (Championship Series) or allstar. |
| `week` | Int64 | Week of the season (0 for the Championship Series). |
| `game_number` | Int64 | Game number within the season. |
| `start_time` | String | Start time, UTC. |
| `event_status` | Int64 | Game status code (3 on final 2021-2026 games; the 2019 games carry 2). |
| `period` | Int64 | Period (quarter; 5 and up is overtime). |
| `home_team_id` | String | Home team code (e.g. WAT; equals ESPN's abbreviation). |
| `home_team_name` | String | Home team name (e.g. Waterdogs). |
| `home_team_location` | String | Home team city or region. |
| `home_team_location_code` | String | Home team three-letter location code (e.g. PHI). |
| `home_team_conference` | String | Home team conference (eastern, western; null for GLG and WLL). |
| `away_team_id` | String | Away team code (e.g. WHP; equals ESPN's abbreviation). |
| `away_team_name` | String | Away team name (e.g. Whipsnakes). |
| `away_team_location` | String | Away team city or region. |
| `away_team_location_code` | String | Away team three-letter location code. |
| `away_team_conference` | String | Away team conference. |
| `home_score` | Int64 | Home team points (a 2-point goal counts 2). |
| `away_score` | Int64 | Away team points (the API's visitorScore). |
| `venue` | String | Venue (stadium) name. |
| `location` | String | City of the game, or the team's city or region. |
| `broadcaster` | String | Broadcast and streaming outlets, comma-separated. |
| `description` | String | Game description (round labels on Championship Series games; null on most), or the play text. |
| `url_streaming` | String | Streaming link. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pll_wll_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. An unknown game, team or player slug, or a 2019-2020 game slug on a per-game route (HTTP 404). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected a parameter (a missing year, a season segment or stat name outside its allowed list; HTTP 400). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 origin refusal, any other failed fetch, or a 200 that is not the `{data: ...}` envelope. |

### Example {#pll_wll_schedule-example}

```python
pll_wll_schedule(year=2026)
```
