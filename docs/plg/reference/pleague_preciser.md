# P. LEAGUE+ — P. LEAGUE+ vendor API (api.preciser.io, Taiwan)

> P. LEAGUE+ — P. LEAGUE+ vendor API (api.preciser.io, Taiwan) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.basketball.plg` — 7 endpoints.

> **Note:** Unofficial use of a THIRD-PARTY host: api.preciser.io is the analytics vendor behind the P. LEAGUE+ game page, not the league's own API. It is keyless, CORS-open, has no robots.txt (404) and no terms were found; it serves the data the league's own game page displays. Release flag: the data release needs owner sign-off (and the league's /user-policy reserves its content). The runtime waits at least 1 s before every request, retries included.

> **Note:** Wrappers take the Preciser game uuid (the uuid column of pleague_game; empty before 2022-23, which have no vendor data). Pass the optional gid to verify the body against the site's game page and box: the shot list and the rotation echo nothing about the game, so only then is the made-shot count, the roster and the court time checked (rotation minutes within 30 s of the box). An unknown uuid answers 200 [] (NoDataError) or HTTP 500 (AssetFetchError).

> **Note:** Id key spaces: the vendor names teams by uuid (home_id / away_id of the shot list, home_team.id of the summary) and persons by personId (the same value as the box row's person); the site's team_id (1-8, stable across seasons and renames) joins to a uuid only through the standings logo file name (photonew.preciser.io/plg/teams/<uuid>.png).

> **Note:** Shot x / y are in the VENDOR frame (percent of the court, the home team always attacks x = 95) while the site's play-by-play x / y are in the physical court frame (teams swap ends at half-time): x_vendor = 100 - x_pbp and y_vendor = 100 - y_pbp in the halves where the home team attacks x < 50. The court is 28.75 x 15.25 m. Clip urls carry a vendor access key (?key=); the runtime scrubs it from every body and no frame keeps a url.

> **Note:** Clubs, site team_id -> Preciser team uuid (the file name of the club logo): 1 = e86394aa-3d88-11ed-8ba8-7f44ba46b0b1 (勇士, Taipei Fubon Braves); 2 = e865bb20-3d88-11ed-a956-7f44ba46b0b1 (領航猿, Taoyuan Pauian Pilots (Taoyuan Pilots from 2024-25)); 3 = e8674343-3d88-11ed-98b4-7f44ba46b0b1 (攻城獅, Hsinchu Jko Lioneers (Toplus Lioneers from 2023-24)); 4 = e869743f-3d88-11ed-925e-7f44ba46b0b1 (夢想家, Formosa Taishin Dreamers (Formosa Dreamers from 2022-23)); 5 = e86cac11-3d88-11ed-8637-7f44ba46b0b1 (鋼鐵人, Kaohsiung 17LIVE Steelers); 6 = e86b0ecc-3d88-11ed-bed5-7f44ba46b0b1 (國王, New Taipei Kings); 7 = 3c3a601a-358a-11ed-ab92-711c34ed1298 (獵鷹, TSG Ghosthawks); 8 = bf957f5a-9389-11f0-812c-a38482ec4d30 (洋基工程, Yankey Ark).

## pleague_preciser_shots

Every field-goal attempt of one finished game with court x / y (vendor frame), zone, style and result.

**Endpoint URL:** `GET https://api.preciser.io/v1/chart-data/id/{uuid}`

**Valid URL:** [https://api.preciser.io/v1/chart-data/id/e6f98675-3e5b-11ed-949e-5d17350689cf](https://api.preciser.io/v1/chart-data/id/e6f98675-3e5b-11ed-949e-5d17350689cf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `uuid` | `uuid` |  | `Y` |  | Required. The Preciser game uuid the site's game page embeds (``uuid`` of pleague_game; empty before 2022-23, which have no vendor data), e.g. 'e6f98675-3e5b-11ed-949e-5d17350689cf' (gid 197). |
| `gid` | `gid` |  |  | `Y` | Optional. The site's game id for this uuid. When given the runtime also fetches the site's game page and box (cached) and checks the vendor body against them (uuid, final score, made shots, roster and minutes); the uuid must be the game page's. Without it only the checks the body allows by itself run. |

### Returns {#pleague_preciser_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_uuid` | String | Preciser game uuid (key space: the vendor's games, a version-1 uuid), the uuid asked for; `uuid` of pleague_game. |
| `shot_no` | Int64 | 1-based position in the order the vendor sent the shots, which is NOT guaranteed game order: sort on period and seconds_left. |
| `period` | Int64 | Period number: 1-4, 5 and up for overtime (OT1 = 5). |
| `period_label` | String | Period label: Q1-Q4, then OT1, OT2. |
| `clock` | String | Game clock REMAINING in the period 'mm:ss' (counts down). |
| `seconds_left` | Int64 | The clock as whole seconds remaining in the period (08:34 = 514), derived by the parser. |
| `side` | String | Which team took the shot: home or away, from the shooter's team uuid against home_id / away_id. |
| `team_uuid` | String | Preciser team uuid (key space: the vendor's teams; the file name of the club logo). Joins the site team_id through pleague_standings.team_uuid: 1 = e86394aa-3d88-11ed-8ba8-7f44ba46b0b1 (勇士, Taipei Fubon Braves); 2 = e865bb20-3d88-11ed-a956-7f44ba46b0b1 (領航猿, Taoyuan Pauian Pilots (Taoyuan Pilots from 2024-25)); 3 = e8674343-3d88-11ed-98b4-7f44ba46b0b1 (攻城獅, Hsinchu Jko Lioneers (Toplus Lioneers from 2023-24)); 4 = e869743f-3d88-11ed-925e-7f44ba46b0b1 (夢想家, Formosa Taishin Dreamers (Formosa Dreamers from 2022-23)); 5 = e86cac11-3d88-11ed-8637-7f44ba46b0b1 (鋼鐵人, Kaohsiung 17LIVE Steelers); 6 = e86b0ecc-3d88-11ed-bed5-7f44ba46b0b1 (國王, New Taipei Kings); 7 = 3c3a601a-358a-11ed-ab92-711c34ed1298 (獵鷹, TSG Ghosthawks); 8 = bf957f5a-9389-11f0-812c-a38482ec4d30 (洋基工程, Yankey Ark). |
| `person_id` | String | Preciser person uuid (key space: the vendor's persons): equal to the site box person_id of the same player, the join between this row and pleague_box_players (2022-23 on). |
| `player_name` | String | Shooter's name as printed (Traditional Chinese or a transliteration); equals the box name_zh. |
| `type` | String | Shot value: 2pt or 3pt. |
| `subtype` | String | Shot style: jumpShot, layup, drivingLayup, floatingJumpShot (floater), tipIn (putback), pullUpJumpShot, fadeAway, dunk, turnAroundJumpShot, hookShot, stepBackJumpShot, alleyOop; null on a few shots (7 of 2,566). |
| `area` | String | Shot zone, English label '{side} {distance band}': 'Center 8ft' (restricted-area band, within 8 ft), 'Center 8-16ft', 'Left 8-16ft', 'Right 8-16ft', 'Left 16-23ft', 'Center 16-23ft', 'Right 16-23ft', 'Left Center 16-23ft', ... and the three-point zones; null on some shots. |
| `success` | Boolean | True when the shot was made, false when missed. |
| `x` | Float64 | Shot x: PERCENT (0-100) of the court LENGTH in the VENDOR frame, in which the HOME team always attacks the basket at x = 95 and the away team the basket at x = 5 (the frame does not flip at half-time). x_vendor = 100 - x_pbp in the halves where the home team attacks x < 50 (pleague_pbp.x is the physical frame). Court 28.75 m long: distance = hypot((x - 5) x 0.2875, (y - 50) x 0.1525) m (mean error 0.010 m on 1,893 shots). |
| `y` | Float64 | Shot y: PERCENT (0-100) of the court WIDTH in the vendor frame (baskets at y = 50); y_vendor = 100 - y_pbp in the halves where the home team attacks x < 50 (held for 166 of the 168 shots of gid 197 matched to the play-by-play; one shot differs in y only). Which sideline is y = 0 is unverified. Court 15.25 m wide. |
| `message_zh` | String | Play text in Traditional Chinese, '{player}(未)投(進){distance}公尺遠的{shot}' = '{player} made / missed a {distance} m {shot}'; the distance is in metres and matches x / y. |
| `has_video` | Boolean | True when the shot has a clip. The clip url is never kept: it carries a vendor access key. |
| `home_team_uuid` | String | Preciser team uuid (key space: the vendor's teams; the file name of the club logo). Joins the site team_id through pleague_standings.team_uuid: 1 = e86394aa-3d88-11ed-8ba8-7f44ba46b0b1 (勇士, Taipei Fubon Braves); 2 = e865bb20-3d88-11ed-a956-7f44ba46b0b1 (領航猿, Taoyuan Pauian Pilots (Taoyuan Pilots from 2024-25)); 3 = e8674343-3d88-11ed-98b4-7f44ba46b0b1 (攻城獅, Hsinchu Jko Lioneers (Toplus Lioneers from 2023-24)); 4 = e869743f-3d88-11ed-925e-7f44ba46b0b1 (夢想家, Formosa Taishin Dreamers (Formosa Dreamers from 2022-23)); 5 = e86cac11-3d88-11ed-8637-7f44ba46b0b1 (鋼鐵人, Kaohsiung 17LIVE Steelers); 6 = e86b0ecc-3d88-11ed-bed5-7f44ba46b0b1 (國王, New Taipei Kings); 7 = 3c3a601a-358a-11ed-ab92-711c34ed1298 (獵鷹, TSG Ghosthawks); 8 = bf957f5a-9389-11f0-812c-a38482ec4d30 (洋基工程, Yankey Ark). |
| `away_team_uuid` | String | Preciser team uuid (key space: the vendor's teams; the file name of the club logo). Joins the site team_id through pleague_standings.team_uuid: 1 = e86394aa-3d88-11ed-8ba8-7f44ba46b0b1 (勇士, Taipei Fubon Braves); 2 = e865bb20-3d88-11ed-a956-7f44ba46b0b1 (領航猿, Taoyuan Pauian Pilots (Taoyuan Pilots from 2024-25)); 3 = e8674343-3d88-11ed-98b4-7f44ba46b0b1 (攻城獅, Hsinchu Jko Lioneers (Toplus Lioneers from 2023-24)); 4 = e869743f-3d88-11ed-925e-7f44ba46b0b1 (夢想家, Formosa Taishin Dreamers (Formosa Dreamers from 2022-23)); 5 = e86cac11-3d88-11ed-8637-7f44ba46b0b1 (鋼鐵人, Kaohsiung 17LIVE Steelers); 6 = e86b0ecc-3d88-11ed-bed5-7f44ba46b0b1 (國王, New Taipei Kings); 7 = 3c3a601a-358a-11ed-ab92-711c34ed1298 (獵鷹, TSG Ghosthawks); 8 = bf957f5a-9389-11f0-812c-a38482ec4d30 (洋基工程, Yankey Ark). |

**`return_parsed=False`** — the raw record (`dict`): `{'data': <the vendor's game object>, '_request': {...}}` with the clip access key scrubbed.

### Raises {#pleague_preciser_shots-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The source has nothing for the request (an unknown game, a stage the season does not have, a game with no play-by-play or vendor data). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a page or body for another game, an unfinished game, a body that does not match its game page, layout drift, a play-by-play phrase outside the observed vocabulary). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#pleague_preciser_shots-example}

```python
pleague_preciser_shots(uuid='e6f98675-3e5b-11ed-949e-5d17350689cf')
```

## pleague_preciser_summary_teams

Post-game team comparison of one finished game: totals, situational points, lead tracker and top performer.

**Endpoint URL:** `GET https://api.preciser.io/post/game/summary/o/plg/gameId/{uuid}`

**Valid URL:** [https://api.preciser.io/post/game/summary/o/plg/gameId/e6f98675-3e5b-11ed-949e-5d17350689cf](https://api.preciser.io/post/game/summary/o/plg/gameId/e6f98675-3e5b-11ed-949e-5d17350689cf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `uuid` | `uuid` |  | `Y` |  | Required. The Preciser game uuid the site's game page embeds (``uuid`` of pleague_game; empty before 2022-23, which have no vendor data), e.g. 'e6f98675-3e5b-11ed-949e-5d17350689cf' (gid 197). |
| `gid` | `gid` |  |  | `Y` | Optional. The site's game id for this uuid. When given the runtime also fetches the site's game page and box (cached) and checks the vendor body against them (uuid, final score, made shots, roster and minutes); the uuid must be the game page's. Without it only the checks the body allows by itself run. |

### Returns {#pleague_preciser_summary_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_uuid` | String | Preciser game uuid (key space: the vendor's games, a version-1 uuid), the uuid asked for; `uuid` of pleague_game. |
| `side` | String | Which team the row belongs to: home or away. |
| `team_uuid` | String | Preciser team uuid (key space: the vendor's teams; the file name of the club logo). Joins the site team_id through pleague_standings.team_uuid: 1 = e86394aa-3d88-11ed-8ba8-7f44ba46b0b1 (勇士, Taipei Fubon Braves); 2 = e865bb20-3d88-11ed-a956-7f44ba46b0b1 (領航猿, Taoyuan Pauian Pilots (Taoyuan Pilots from 2024-25)); 3 = e8674343-3d88-11ed-98b4-7f44ba46b0b1 (攻城獅, Hsinchu Jko Lioneers (Toplus Lioneers from 2023-24)); 4 = e869743f-3d88-11ed-925e-7f44ba46b0b1 (夢想家, Formosa Taishin Dreamers (Formosa Dreamers from 2022-23)); 5 = e86cac11-3d88-11ed-8637-7f44ba46b0b1 (鋼鐵人, Kaohsiung 17LIVE Steelers); 6 = e86b0ecc-3d88-11ed-bed5-7f44ba46b0b1 (國王, New Taipei Kings); 7 = 3c3a601a-358a-11ed-ab92-711c34ed1298 (獵鷹, TSG Ghosthawks); 8 = bf957f5a-9389-11f0-812c-a38482ec4d30 (洋基工程, Yankey Ark). |
| `team_name_zh` | String | Team full name in Traditional Chinese (臺北富邦勇士 = Taipei Fubon Braves, 桃園璞園領航猿 = Taoyuan Pauian Pilots, 新竹御頂攻城獅 = Hsinchu Toplus Lioneers, 福爾摩沙夢想家 = Formosa Dreamers, 高雄鋼鐵人 = Kaohsiung 17LIVE Steelers, 新北國王 = New Taipei Kings, 臺南台鋼獵鷹 = TSG Ghosthawks). |
| `color` | String | Team colour as a CSS hex string. |
| `logo_url` | String | Team logo url (photonew.preciser.io/plg/teams/{team uuid}.png). |
| `points` | Int64 | Team points (the comparison block). |
| `rebounds` | Int64 | Team total rebounds (the comparison block). |
| `assists` | Int64 | Team assists (the comparison block). |
| `steals` | Int64 | Team steals (the comparison block). |
| `blocks` | Int64 | Team blocks made (the comparison block). |
| `turnovers` | Int64 | Team turnovers. |
| `fg_made` | Int64 | Team field goals MADE: the vendor's key is field_goal_percentage but the value is a count of makes. |
| `fg2_pct` | Float64 | Team 2-point percentage 0-100. |
| `fg3_pct` | Float64 | Team 3-point percentage 0-100. |
| `ft_pct` | Float64 | Team free-throw percentage 0-100. |
| `fast_break_points` | Float64 | Team fast-break points. |
| `paint_points` | Float64 | Team points in the paint. |
| `second_chance_points` | Float64 | Team second-chance points. |
| `biggest_lead` | Int64 | The team's largest lead in points. |
| `biggest_scoring_run` | Int64 | The team's longest unanswered scoring run, in points. |
| `time_leading_seconds` | Int64 | Seconds the team led. |
| `lead_changes` | Int64 | Number of lead changes in the game (the same for both rows). |
| `top_player_name` | String | Name of the team's top performer (Traditional Chinese or a transliteration). |
| `top_player_jersey` | String | Shirt number of the top performer as a string. |
| `top_player_points` | Int64 | Points of the top performer in the game. |
| `top_player_rebounds` | Int64 | Total rebounds of the top performer in the game. |
| `top_player_assists` | Int64 | Assists of the top performer in the game. |

**`return_parsed=False`** — the raw record (`dict`): `{'data': <the vendor's game object>, '_request': {...}}` with the clip access key scrubbed.

### Raises {#pleague_preciser_summary_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The source has nothing for the request (an unknown game, a stage the season does not have, a game with no play-by-play or vendor data). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a page or body for another game, an unfinished game, a body that does not match its game page, layout drift, a play-by-play phrase outside the observed vocabulary). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#pleague_preciser_summary_teams-example}

```python
pleague_preciser_summary_teams(uuid='e6f98675-3e5b-11ed-949e-5d17350689cf')
```

## pleague_preciser_score_log

Scoring log of one finished game (scoring events only, with the running score).

**Endpoint URL:** `GET https://api.preciser.io/post/game/summary/o/plg/gameId/{uuid}`

**Valid URL:** [https://api.preciser.io/post/game/summary/o/plg/gameId/e6f98675-3e5b-11ed-949e-5d17350689cf](https://api.preciser.io/post/game/summary/o/plg/gameId/e6f98675-3e5b-11ed-949e-5d17350689cf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `uuid` | `uuid` |  | `Y` |  | Required. The Preciser game uuid the site's game page embeds (``uuid`` of pleague_game; empty before 2022-23, which have no vendor data), e.g. 'e6f98675-3e5b-11ed-949e-5d17350689cf' (gid 197). |
| `gid` | `gid` |  |  | `Y` | Optional. The site's game id for this uuid. When given the runtime also fetches the site's game page and box (cached) and checks the vendor body against them (uuid, final score, made shots, roster and minutes); the uuid must be the game page's. Without it only the checks the body allows by itself run. |

### Returns {#pleague_preciser_score_log-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_uuid` | String | Preciser game uuid (key space: the vendor's games, a version-1 uuid), the uuid asked for; `uuid` of pleague_game. |
| `seq` | Int64 | 1-based position of the scoring event in game order. |
| `period` | String | Period label: Q1-Q4, then OT1, OT2 (the vendor sends it in lower case). |
| `period_no` | Int64 | 1-based position of the period: 1-4 for Q1-Q4, 5 for OT1, 6 for OT2. |
| `clock` | String | Game clock REMAINING in the period 'mm:ss' (counts down). |
| `seconds_left` | Int64 | The clock as whole seconds remaining in the period (08:34 = 514), derived by the parser. |
| `home_score` | Int64 | Home score after the event. |
| `away_score` | Int64 | Away score after the event. |

**`return_parsed=False`** — the raw record (`dict`): `{'data': <the vendor's game object>, '_request': {...}}` with the clip access key scrubbed.

### Raises {#pleague_preciser_score_log-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The source has nothing for the request (an unknown game, a stage the season does not have, a game with no play-by-play or vendor data). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a page or body for another game, an unfinished game, a body that does not match its game page, layout drift, a play-by-play phrase outside the observed vocabulary). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#pleague_preciser_score_log-example}

```python
pleague_preciser_score_log(uuid='e6f98675-3e5b-11ed-949e-5d17350689cf')
```

## pleague_preciser_lead_flow

Score at each minute mark of one finished game, with the score difference (the lead tracker).

**Endpoint URL:** `GET https://api.preciser.io/post/game/summary/o/plg/gameId/{uuid}`

**Valid URL:** [https://api.preciser.io/post/game/summary/o/plg/gameId/e6f98675-3e5b-11ed-949e-5d17350689cf](https://api.preciser.io/post/game/summary/o/plg/gameId/e6f98675-3e5b-11ed-949e-5d17350689cf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `uuid` | `uuid` |  | `Y` |  | Required. The Preciser game uuid the site's game page embeds (``uuid`` of pleague_game; empty before 2022-23, which have no vendor data), e.g. 'e6f98675-3e5b-11ed-949e-5d17350689cf' (gid 197). |
| `gid` | `gid` |  |  | `Y` | Optional. The site's game id for this uuid. When given the runtime also fetches the site's game page and box (cached) and checks the vendor body against them (uuid, final score, made shots, roster and minutes); the uuid must be the game page's. Without it only the checks the body allows by itself run. |

### Returns {#pleague_preciser_lead_flow-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_uuid` | String | Preciser game uuid (key space: the vendor's games, a version-1 uuid), the uuid asked for; `uuid` of pleague_game. |
| `seq` | Int64 | 1-based position of the minute mark in game order. |
| `period` | String | Period label: Q1-Q4, then OT1, OT2 (the vendor sends it in lower case). |
| `period_no` | Int64 | 1-based position of the period: 1-4 for Q1-Q4, 5 for OT1, 6 for OT2. |
| `clock` | String | Game clock REMAINING in the period 'mm:ss' (counts down). |
| `seconds_left` | Int64 | The clock as whole seconds remaining in the period (08:34 = 514), derived by the parser. |
| `home_score` | Int64 | Home score after the event. |
| `away_score` | Int64 | Away score after the event. |
| `score_difference` | Int64 | home_score minus away_score (positive = home leads). |

**`return_parsed=False`** — the raw record (`dict`): `{'data': <the vendor's game object>, '_request': {...}}` with the clip access key scrubbed.

### Raises {#pleague_preciser_lead_flow-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The source has nothing for the request (an unknown game, a stage the season does not have, a game with no play-by-play or vendor data). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a page or body for another game, an unfinished game, a body that does not match its game page, layout drift, a play-by-play phrase outside the observed vocabulary). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#pleague_preciser_lead_flow-example}

```python
pleague_preciser_lead_flow(uuid='e6f98675-3e5b-11ed-949e-5d17350689cf')
```

## pleague_preciser_win_probability

The vendor's home-team win probability by minute for one finished game.

**Endpoint URL:** `GET https://api.preciser.io/post/game/summary/o/plg/gameId/{uuid}`

**Valid URL:** [https://api.preciser.io/post/game/summary/o/plg/gameId/e6f98675-3e5b-11ed-949e-5d17350689cf](https://api.preciser.io/post/game/summary/o/plg/gameId/e6f98675-3e5b-11ed-949e-5d17350689cf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `uuid` | `uuid` |  | `Y` |  | Required. The Preciser game uuid the site's game page embeds (``uuid`` of pleague_game; empty before 2022-23, which have no vendor data), e.g. 'e6f98675-3e5b-11ed-949e-5d17350689cf' (gid 197). |
| `gid` | `gid` |  |  | `Y` | Optional. The site's game id for this uuid. When given the runtime also fetches the site's game page and box (cached) and checks the vendor body against them (uuid, final score, made shots, roster and minutes); the uuid must be the game page's. Without it only the checks the body allows by itself run. |

### Returns {#pleague_preciser_win_probability-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_uuid` | String | Preciser game uuid (key space: the vendor's games, a version-1 uuid), the uuid asked for; `uuid` of pleague_game. |
| `minute` | Int64 | Minute mark: 0 is the tip-off (50 percent); 49 entries for a 48-minute game. |
| `home_win_pct` | Float64 | The home team's win probability in percent (0-100) at that minute; the vendor documents no method. |

**`return_parsed=False`** — the raw record (`dict`): `{'data': <the vendor's game object>, '_request': {...}}` with the clip access key scrubbed.

### Raises {#pleague_preciser_win_probability-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The source has nothing for the request (an unknown game, a stage the season does not have, a game with no play-by-play or vendor data). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a page or body for another game, an unfinished game, a body that does not match its game page, layout drift, a play-by-play phrase outside the observed vocabulary). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#pleague_preciser_win_probability-example}

```python
pleague_preciser_win_probability(uuid='e6f98675-3e5b-11ed-949e-5d17350689cf')
```

## pleague_preciser_rotation

On-court stints of every player of one finished game (the rotation chart).

**Endpoint URL:** `GET https://api.preciser.io/game/rotation/chart/o/plg/gameId/{uuid}`

**Valid URL:** [https://api.preciser.io/game/rotation/chart/o/plg/gameId/e6f98675-3e5b-11ed-949e-5d17350689cf](https://api.preciser.io/game/rotation/chart/o/plg/gameId/e6f98675-3e5b-11ed-949e-5d17350689cf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `uuid` | `uuid` |  | `Y` |  | Required. The Preciser game uuid the site's game page embeds (``uuid`` of pleague_game; empty before 2022-23, which have no vendor data), e.g. 'e6f98675-3e5b-11ed-949e-5d17350689cf' (gid 197). |
| `gid` | `gid` |  |  | `Y` | Optional. The site's game id for this uuid. When given the runtime also fetches the site's game page and box (cached) and checks the vendor body against them (uuid, final score, made shots, roster and minutes); the uuid must be the game page's. Without it only the checks the body allows by itself run. |

### Returns {#pleague_preciser_rotation-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_uuid` | String | Preciser game uuid (key space: the vendor's games, a version-1 uuid), the uuid asked for; `uuid` of pleague_game. |
| `side` | String | Which team the row belongs to: home or away. |
| `name_zh` | String | Player name (Traditional Chinese); equals the site box name_zh, which is how the runtime checks the roster. |
| `jersey` | Int64 | Shirt number (an integer here; the box prints it as a string). |
| `started` | Boolean | True when the player started the game. |
| `stint_no` | Int64 | 1-based position of the stint among the player's stints, in game order. |
| `start_period` | String | Period in which the stint starts (Q1-Q4, OT1, ...). |
| `start_clock` | String | Clock REMAINING in start_period when the stint starts, 'm:ss'. |
| `start_seconds` | Int64 | Seconds elapsed on the game clock when the stint starts (cumulative across periods). |
| `end_period` | String | Period in which the stint ends. |
| `end_clock` | String | Clock REMAINING in end_period when the stint ends, 'm:ss'. |
| `end_seconds` | Int64 | Seconds elapsed on the game clock when the stint ends (cumulative across periods). |

**`return_parsed=False`** — the raw record (`dict`): `{'data': <the vendor's game object>, '_request': {...}}` with the clip access key scrubbed.

### Raises {#pleague_preciser_rotation-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The source has nothing for the request (an unknown game, a stage the season does not have, a game with no play-by-play or vendor data). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a page or body for another game, an unfinished game, a body that does not match its game page, layout drift, a play-by-play phrase outside the observed vocabulary). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#pleague_preciser_rotation-example}

```python
pleague_preciser_rotation(uuid='e6f98675-3e5b-11ed-949e-5d17350689cf')
```

## pleague_preciser_injury

Injury report of one finished game: one row per team.

**Endpoint URL:** `GET https://api.preciser.io/game/injury/list/o/plg/gameId/{uuid}`

**Valid URL:** [https://api.preciser.io/game/injury/list/o/plg/gameId/e6f98675-3e5b-11ed-949e-5d17350689cf](https://api.preciser.io/game/injury/list/o/plg/gameId/e6f98675-3e5b-11ed-949e-5d17350689cf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `uuid` | `uuid` |  | `Y` |  | Required. The Preciser game uuid the site's game page embeds (``uuid`` of pleague_game; empty before 2022-23, which have no vendor data), e.g. 'e6f98675-3e5b-11ed-949e-5d17350689cf' (gid 197). |
| `gid` | `gid` |  |  | `Y` | Optional. The site's game id for this uuid. When given the runtime also fetches the site's game page and box (cached) and checks the vendor body against them (uuid, final score, made shots, roster and minutes); the uuid must be the game page's. Without it only the checks the body allows by itself run. |

### Returns {#pleague_preciser_injury-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_uuid` | String | Preciser game uuid (key space: the vendor's games, a version-1 uuid), the uuid asked for; `uuid` of pleague_game. |
| `side` | String | Which team the row belongs to: home or away. |
| `team_uuid` | String | Preciser team uuid (key space: the vendor's teams; the file name of the club logo). Joins the site team_id through pleague_standings.team_uuid: 1 = e86394aa-3d88-11ed-8ba8-7f44ba46b0b1 (勇士, Taipei Fubon Braves); 2 = e865bb20-3d88-11ed-a956-7f44ba46b0b1 (領航猿, Taoyuan Pauian Pilots (Taoyuan Pilots from 2024-25)); 3 = e8674343-3d88-11ed-98b4-7f44ba46b0b1 (攻城獅, Hsinchu Jko Lioneers (Toplus Lioneers from 2023-24)); 4 = e869743f-3d88-11ed-925e-7f44ba46b0b1 (夢想家, Formosa Taishin Dreamers (Formosa Dreamers from 2022-23)); 5 = e86cac11-3d88-11ed-8637-7f44ba46b0b1 (鋼鐵人, Kaohsiung 17LIVE Steelers); 6 = e86b0ecc-3d88-11ed-bed5-7f44ba46b0b1 (國王, New Taipei Kings); 7 = 3c3a601a-358a-11ed-ab92-711c34ed1298 (獵鷹, TSG Ghosthawks); 8 = bf957f5a-9389-11f0-812c-a38482ec4d30 (洋基工程, Yankey Ark). |
| `team_name_zh` | String | Team short name in Traditional Chinese ('攻城獅' = Lioneers). |
| `team_name_en` | String | Team short English name ('Lioneers'). |
| `color` | String | Team colour as a CSS hex string. |
| `logo_url` | String | Team logo url (photonew.preciser.io/plg/teams/{team uuid}.png). |
| `injury_report` | String | The team's injury report as a JSON string; null when the vendor sent null. It was null on all 16 sampled games, so its shape is unverified. |

**`return_parsed=False`** — the raw record (`dict`): `{'data': <the vendor's game object>, '_request': {...}}` with the clip access key scrubbed.

### Raises {#pleague_preciser_injury-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The source has nothing for the request (an unknown game, a stage the season does not have, a game with no play-by-play or vendor data). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a page or body for another game, an unfinished game, a body that does not match its game page, layout drift, a play-by-play phrase outside the observed vocabulary). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#pleague_preciser_injury-example}

```python
pleague_preciser_injury(uuid='e6f98675-3e5b-11ed-949e-5d17350689cf')
```
