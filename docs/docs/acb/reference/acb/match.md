---
title: "ACB — ACB API (api2.acb.com, Spain) — Match"
sidebar_label: "Match"
sidebar_position: 2
description: "ACB — ACB API (api2.acb.com, Spain) — Match — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# ACB — ACB API (api2.acb.com, Spain) — Match

## acb_match_header

Match header: teams, status, score, bonus, current period and the content the site offers.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/MatchHeader/match-header`

**Valid URL:** [https://api2.acb.com/api/matchdata/MatchHeader/match-header?matchId=105401](https://api2.acb.com/api/matchdata/MatchHeader/match-header?matchId=105401)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `matchId` | `match_id` |  | `Y` |  | Required. ACB match id (integer), one global sequence for every competition: legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = the 2005-06 final); ids from 2020-21 start around 100000 (105401 = Barca 89-86 Valencia Basket, Liga 2026-27 round 3). |

### Returns {#acb_match_header-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | Match id. Legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = 2005-06 final, 17718 = 2016-17 final); from 2020-21 they start at about 100000 (102920 in 2021-22, 105401 in 2026-27). One key space across competitions: the matchId param of every matchdata route. |
| `competition_id` | integer | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (x-competitions) |
| `start` | character | Tip-off, ISO 8601 UTC |
| `status` | character | Game state: FINALIZED once over; the live and scheduled values are unverified. |
| `is_on_half_time` | logical | At half time (live) |
| `time_left` | character | Period clock remaining as 'mm:ss' ('00:00' once the game is over). |
| `current_quarter` | integer | Current or last quarter (5+ = overtime; 0 for 2005-06 matches) |
| `home_score` | integer | Home points: the final score once the game is over, else the live score. |
| `away_score` | integer | Away points: the final score once the game is over, else the live score. |
| `home_bonus` | integer | Home team-foul bonus counter of the current period (unverified meaning). |
| `away_bonus` | integer | Away team-foul bonus counter of the current period (unverified meaning). |
| `home_team_id` | integer | Home season-team entry id (new every season). |
| `home_club_id` | integer | Home stable club id (2 = Barca, 9 = Real Madrid). |
| `home_team` | character | Team name that season (sponsor names change) |
| `home_team_abbr` | character | Three-letter abbreviation (BAR, RMB) |
| `away_team_id` | integer | Away season-team entry id (new every season). |
| `away_club_id` | integer | Away stable club id. |
| `away_team` | character | Team name that season (sponsor names change) |
| `away_team_abbr` | character | Three-letter abbreviation (BAR, RMB) |
| `edition_id` | integer | Season id (START year - 1935) read from the home team entry. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `has_overview` | logical | True when the site offers the overview panels (lead tracker, lineups, leaders) for the game. |
| `has_play_by_play` | logical | True when the site offers play-by-play (2016-17 onward). |
| `has_boxscore` | logical | True when the site offers a box score for the game. |
| `has_advanced` | logical | True when the site offers advanced statistics for the game. |
| `highlights_url` | character | Highlights video URL; null when none is published. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_match_header-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_match_header-example}

```python
acb_match_header(match_id=105401)
```

## acb_match_leaders

Each team's leaders of a game by category.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/Overview/match-leaders`

**Valid URL:** [https://api2.acb.com/api/matchdata/Overview/match-leaders?matchId=105401](https://api2.acb.com/api/matchdata/Overview/match-leaders?matchId=105401)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `matchId` | `match_id` |  | `Y` |  | Required. ACB match id (integer), one global sequence for every competition: legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = the 2005-06 final); ids from 2020-21 start around 100000 (105401 = Barca 89-86 Valencia Basket, Liga 2026-27 round 3). |

### Returns {#acb_match_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `edition_id` | integer | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `team_side` | character | Side of the game the team played: home or away (the feed lists the home team first). |
| `team_id` | integer | Season-team entry id: one club in one competition edition, NEW every season (Barca = 4467 in Liga 2026-27). Not the club: use club_id to follow a club across seasons. |
| `club_id` | integer | Stable ACB club id (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket): the same across seasons and sponsor names, and the club_id request argument. |
| `team` | character | Team name as registered for that season (sponsor names change; the club id does not). |
| `team_abbr` | character | Three-letter team abbreviation (BAR, RMB, VBC). |
| `metric` | character | Leader category in snake_case (the API's key, e.g. rating, points, rebounds, assists). |
| `rank` | integer | Place in the category list, 1 = the leader, in the order the feed lists them. |
| `value` | numeric | The player's count in the category (points, rebounds, assists, rating). |
| `player_id` | integer | ACB licence number: the person id everywhere. A player's box-score `player.id`, his play-by-play and shot `playerLicenseId` and the `playerLicenceId` / `playerLicense` request params hold the same number (all 24 players of match 105401 match). 20209407 = Edy Tavares. Older licences start 2020..., recent ones 3000.... |
| `player_name` | character | Display (sporting) name ('Edy Tavares') |
| `first_name` | character | Legal first name(s) |
| `last_name` | character | Legal last name(s) |
| `shirt_number` | character | Shirt number, a string ('' or null when unknown) |
| `game_role` | character | Position or staff role, in Spanish: Base (point guard), Escolta (shooting guard), Alero (small forward), Ala-pivot (power forward), Pivot (centre); Entrenador (head coach), Segundo Entrenador / Entrenador Ayudante (assistant coach). Null or '' in many rows |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_match_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_match_leaders-example}

```python
acb_match_leaders(match_id=105401)
```

## acb_match_advanced_stats

Team advanced statistics of a game against the team's season and win/loss splits.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/AdvancedStats/match-advanced-stats`

**Valid URL:** [https://api2.acb.com/api/matchdata/AdvancedStats/match-advanced-stats?matchId=105401](https://api2.acb.com/api/matchdata/AdvancedStats/match-advanced-stats?matchId=105401)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `matchId` | `match_id` |  | `Y` |  | Required. ACB match id (integer), one global sequence for every competition: legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = the 2005-06 final); ids from 2020-21 start around 100000 (105401 = Barca 89-86 Valencia Basket, Liga 2026-27 round 3). |

### Returns {#acb_match_advanced_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `edition_id` | integer | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `team_side` | character | Side of the game the team played: home or away (the feed lists the home team first). |
| `team_id` | integer | Season-team entry id: one club in one competition edition, NEW every season (Barca = 4467 in Liga 2026-27). Not the club: use club_id to follow a club across seasons. |
| `club_id` | integer | Stable ACB club id (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket): the same across seasons and sponsor names, and the club_id request argument. |
| `team` | character | Team name as registered for that season (sponsor names change; the club id does not). |
| `team_abbr` | character | Three-letter team abbreviation (BAR, RMB, VBC). |
| `group` | character | Statistic group in snake_case (four_factors, game_rhythm, ball_handling, shooting, points, ratings, rebounds). |
| `stat` | character | Statistic in snake_case (efg_pct, orb_pct, tov_pct, f_tr = free-throw rate, possessions, pace, points_per_possession, ast_ratio, ..., net_rating, oer, der, trb_pct). |
| `match_value` | numeric | The statistic in this game (the API's partido). |
| `season_value` | numeric | The team's value over the season (the API's temporada). |
| `win_value` | numeric | The team's value in its wins (the API's win). |
| `loss_value` | numeric | The team's value in its losses (the API's loss). |
| `season_diff` | numeric | Game value minus season value (temporadaDiff). |
| `win_diff` | numeric | Game value minus the wins value (winDiff). |
| `loss_diff` | numeric | Game value minus the losses value (lossDiff). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_match_advanced_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_match_advanced_stats-example}

```python
acb_match_advanced_stats(match_id=105401)
```
