# TBF — TBF app API (miniappapi.tbf.org.tr, Turkey: BSL, TBL, KBSL) — Team

> TBF — TBF app API (miniappapi.tbf.org.tr, Turkey: BSL, TBL, KBSL) — Team — function reference in sdv-py, the SportsDataverse Python package.

## tbf_team_box

Team totals of a finished game with the team-credited rebounds, turnovers and fouls.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-istatistik`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-istatistik?match_id=346279](https://miniappapi.tbf.org.tr/webapi-service/api/Match/mac-istatistik?match_id=346279)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Required. TBF game id (integer), e.g. 346279; match_id in tbf_schedule. One global sequence across leagues and seasons. Only a finished game is served. |

### Returns {#tbf_team_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, 346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the runtime stamps the integer asked for. |
| `side` | character | Side of the game the row belongs to: home or away (from the route's isHome flag or home / away block). |
| `team_process_id` | integer | Team-season id of the side (from the game header; key space: TBF team registrations). |
| `seconds_played` | integer | Time on court in seconds, parsed from the 'mm:ss' or 'hh:mm:ss' text the API sends (team totals sum their players, so 200:00 = 12000). |
| `points` | integer | Points scored in the game (or season) the row covers. |
| `fg_made` | integer | Team field goals made, 2PT + 3PT. |
| `fg_attempted` | integer | Team field goals attempted, 2PT + 3PT. |
| `fg_pct` | integer | Team field-goal percentage 0-100, rounded to an integer by the API. |
| `fg2_made` | integer | Team two-point field goals MADE (the API calls it twoPointShots). |
| `fg2_attempted` | integer | Team two-point field goals attempted. |
| `fg2_pct` | integer | Team two-point percentage 0-100, an integer. |
| `fg3_made` | integer | Team three-point field goals MADE (the API calls it threePointShots). |
| `fg3_attempted` | integer | Team three-point field goals attempted. |
| `fg3_pct` | integer | Team three-point percentage 0-100, an integer. |
| `ft_made` | integer | Team free throws MADE. |
| `ft_attempted` | integer | Team free throws attempted. |
| `ft_pct` | integer | Team free-throw percentage 0-100, an integer. |
| `off_rebounds` | integer | Offensive rebounds (hücum ribaundu). |
| `def_rebounds` | integer | Defensive rebounds (savunma ribaundu). |
| `rebounds` | integer | Total rebounds (toplam ribaund). |
| `assists` | integer | Assists in this game (asist). |
| `steals` | integer | Steals in this game (top çalma). |
| `blocks` | integer | Shots this player blocked in this game (blok). |
| `turnovers` | integer | Turnovers committed in this game (top kaybı). |
| `fouls` | integer | Personal fouls committed (faul). |
| `fouls_drawn` | integer | Fouls drawn by the team (rivalFoul). |
| `blocks_received` | integer | Team shots blocked by the opponent. |
| `efficiency` | integer | Team efficiency rating (verimlilik puani): the players' ratings summed; a number on team rows. |
| `team_off_rebounds` | integer | Team offensive rebounds not credited to a player (homeTeamStats). |
| `team_def_rebounds` | integer | Team defensive rebounds not credited to a player. |
| `team_rebounds` | integer | Team rebounds not credited to a player (offensive + defensive). |
| `team_turnovers` | integer | Team turnovers not credited to a player. |
| `team_fouls` | integer | Team fouls not credited to a player (bench / coach technicals); 0 in the samples. |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_team_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_team_box-example}

```python
tbf_team_box(match_id=346279)
```

_Last validated n/a._

## tbf_teams

Every team registered in one league-season with its team-season id (team_process_id) and club id.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Team/get-teams-by-leauge`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Team/get-teams-by-leauge?season=2027&competition=bsl](https://miniappapi.tbf.org.tr/webapi-service/api/Team/get-teams-by-leauge?season=2027&competition=bsl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the list lacks raises NoDataError. |
| `competition` | `competition` |  |  | `Y` | TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was named TBL until 2007-08 and is listed under bsl. |

### Returns {#tbf_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `competition` | character | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `team_process_id` | integer | Team-season id (teamProcessId = takimIslemId, a team REGISTRATION; key space: TBF team registrations, a NEW id per club per league-season: Tofas = 265995 in BSL 2026-27). The team_process_id argument of the team wrappers; not the stable club_id. |
| `club_id` | integer | Stable club id (takimID / teamId of the team lists; key space: TBF clubs, the same across seasons and leagues: Tofas = 13). Not the team-season id (team_process_id). |
| `team_name` | character | Team name as registered for that season, uppercase with Turkish letters. |
| `city` | character | Home city (teamCityName), uppercase Turkish. |
| `code` | character | Team short code (teamCode). |
| `season_id` | integer | Internal TBF season id (key space: TBF seasons, global across leagues: 174 = 2026-2027, 172 = 2025-2026; the odd ids are international UA- seasons). Never an sdv key: use season (END year). |
| `season_label` | character | Season label 'YYYY-YYYY' of the league-season. |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_teams-example}

```python
tbf_teams(competition='bsl', season=2027)
```

_Last validated n/a._

## tbf_team_player_stats

Season player statistics of one team-season: totals, percentages and per-game averages.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Team/get-team-detail-statistic`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Team/get-team-detail-statistic?season=2027&competition=bsl&team_process_id=265995](https://miniappapi.tbf.org.tr/webapi-service/api/Team/get-team-detail-statistic?season=2027&competition=bsl&team_process_id=265995)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the list lacks raises NoDataError. |
| `competition` | `competition` |  |  | `Y` | TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was named TBL until 2007-08 and is listed under bsl. |
| `team_process_id` | `team_process_id` |  | `Y` |  | Required. Team-season id (team registration), e.g. 265995 = Tofas in BSL 2026-27; team_process_id in tbf_teams and tbf_schedule. New for every club in every league-season; not the stable club_id. |

### Returns {#tbf_team_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `competition` | character | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `team_process_id` | integer | Team-season id of the request (key space: TBF team registrations). |
| `person_id` | integer | TBF person id (code; key space: TBF persons, stable across seasons and clubs). |
| `jersey_number` | character | Shirt number (oyuncuNo) as a string. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `games` | integer | Games played (macSayisi). |
| `seconds_played` | integer | Season total time on court in seconds (sureToplam 'hh:mm:ss'). |
| `points` | integer | Season total points (sayiToplam; the API sends it as a string). |
| `fg_made` | integer | Season field goals made (agBasarili; sent as a string). |
| `fg_attempted` | integer | Season field goals attempted (agDeneme). |
| `fg2_made` | integer | Season two-point field goals made (puan20Toplam). |
| `fg2_attempted` | integer | Season two-point field goals attempted (puan21Toplam). |
| `fg3_made` | integer | Season three-point field goals made (puan30Toplam). |
| `fg3_attempted` | integer | Season three-point field goals attempted (puan31Toplam). |
| `ft_made` | integer | Season free throws made (sA0Toplam). |
| `ft_attempted` | integer | Season free throws attempted (sA1Toplam). |
| `def_rebounds` | integer | Season defensive rebounds (drToplam). |
| `off_rebounds` | integer | Season offensive rebounds (hrToplam). |
| `rebounds` | integer | Season total rebounds (trToplam). |
| `assists` | integer | Season assists (asistToplam). |
| `blocks` | integer | Season blocks (blokToplam). |
| `steals` | integer | Season steals (topCalmaToplam). |
| `turnovers` | integer | Season turnovers (topKaybiToplam). |
| `fouls` | integer | Season personal fouls (faulToplam). |
| `plus_minus` | numeric | Season plus/minus contribution (oyunaKatki = contribution to the game), a decimal string. |
| `efficiency` | integer | Season efficiency rating total (vpToplam, verimlilik puani). |
| `fg_pct` | numeric | Field-goal percentage 0-100 with two decimals (agYuzdesi). |
| `fg2_pct` | numeric | Two-point percentage 0-100 with two decimals (ikilikYuzdesi). |
| `fg3_pct` | numeric | Three-point percentage 0-100 with two decimals (uclukYuzdesi). |
| `ft_pct` | numeric | Free-throw percentage 0-100 with two decimals (serbestAtisYuzdesi). |
| `seconds_per_game` | integer | Average time on court per game in seconds (sureOrtalama 'hh:mm:ss'). |
| `points_per_game` | numeric | Points per game (sayiOrtalama). |
| `rebounds_per_game` | numeric | Rebounds per game (trOrtalama). |
| `assists_per_game` | numeric | Assists per game (asistOrtalama). |
| `steals_per_game` | numeric | Steals per game (topCalmaOrtalama). |
| `blocks_per_game` | numeric | Blocks per game (blokOrtalama). |
| `turnovers_per_game` | numeric | Turnovers per game (topKaybiOrtalama). |
| `efficiency_per_game` | numeric | Efficiency rating per game (vpOrtalama). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_team_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_team_player_stats-example}

```python
tbf_team_player_stats(season=2027, team_process_id=265995)
```

_Last validated n/a._

## tbf_team_games

Games and results of one team-season, scheduled games included.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Team/get-team-detail-matches-by-season-and-league`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Team/get-team-detail-matches-by-season-and-league?season=2027&competition=bsl&team_process_id=265995](https://miniappapi.tbf.org.tr/webapi-service/api/Team/get-team-detail-matches-by-season-and-league?season=2027&competition=bsl&team_process_id=265995)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the list lacks raises NoDataError. |
| `competition` | `competition` |  |  | `Y` | TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was named TBL until 2007-08 and is listed under bsl. |
| `team_process_id` | `team_process_id` |  | `Y` |  | Required. Team-season id (team registration), e.g. 265995 = Tofas in BSL 2026-27; team_process_id in tbf_teams and tbf_schedule. New for every club in every league-season; not the stable club_id. |

### Returns {#tbf_team_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `competition` | character | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `team_process_id` | integer | Team-season id of the request (key space: TBF team registrations). |
| `match_id` | integer | TBF game id (key space: TBF games, one global sequence). |
| `match_code` | character | TBF short game code. |
| `tipoff` | character | Tip-off as a naive timestamp in Turkey local time (tarih). |
| `week` | character | Week label (formattedWeek), e.g. '1. Hafta'. |
| `city` | character | City of the venue (sehir), uppercase Turkish. |
| `venue` | character | Venue name (salon). |
| `home_team_process_id` | integer | Home team-season id (takimAId; key space: TBF team registrations). |
| `home_team` | character | Home team name (takimA). |
| `home_score` | integer | Home points (skorA); null before the game. |
| `away_team_process_id` | integer | Away team-season id (takimBId). |
| `away_team` | character | Away team name (takimB). |
| `away_score` | integer | Away points (skorB); null before the game. |
| `team_is_home` | logical | True when the asked team was the home side (isHome). |
| `team_won` | logical | True when the asked team won (isWin); false for a loss or an unplayed game. |
| `is_played` | logical | True once the game has been played (isPlayed). |
| `status` | character | Game status text in Turkish: Oynandi = played, Oynanmadi = not played (durum). |
| `status_id` | integer | Game status code (durumId): 1 = played, 4 = not played. |
| `has_overtime` | logical | Overtime flag (uzatma = overtime). Reliability unverified: false on every one of 64 BSL 2025-26 games checked. |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_team_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_team_games-example}

```python
tbf_team_games(season=2027, team_process_id=265995)
```

_Last validated n/a._

## tbf_team_roster

Roster of one team-season with games, minutes and per-game points and efficiency (birth dates dropped).

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Team/get-team-active-player-list-by-season-and-league`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Team/get-team-active-player-list-by-season-and-league?season=2027&competition=bsl&team_process_id=265995](https://miniappapi.tbf.org.tr/webapi-service/api/Team/get-team-active-player-list-by-season-and-league?season=2027&competition=bsl&team_process_id=265995)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the list lacks raises NoDataError. |
| `competition` | `competition` |  |  | `Y` | TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was named TBL until 2007-08 and is listed under bsl. |
| `team_process_id` | `team_process_id` |  | `Y` |  | Required. Team-season id (team registration), e.g. 265995 = Tofas in BSL 2026-27; team_process_id in tbf_teams and tbf_schedule. New for every club in every league-season; not the stable club_id. |

### Returns {#tbf_team_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `competition` | character | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `team_process_id` | integer | Team-season id of the request (key space: TBF team registrations). |
| `person_id` | integer | TBF person id (personId; key space: TBF persons). |
| `first_name` | character | Given name as registered with TBF, mixed case ('Yiğitcan'). |
| `last_name` | character | Family name as registered with TBF, mixed case ('Saybir'). |
| `nationality` | character | Nationality as a TURKISH country name (country); there is no code on this route. |
| `height_cm` | integer | Height in centimetres; null when TBF has none. |
| `jersey_number` | character | Shirt number as a string, so '0' and '00' stay distinct. |
| `games` | integer | Games played (macSayisi = game count). |
| `seconds_played` | integer | Season time on court in seconds (sure 'hh:mm:ss'). |
| `points_per_game` | numeric | Points per game (sayi). |
| `efficiency_per_game` | numeric | Efficiency rating per game (verimlilikPuani). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_team_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_team_roster-example}

```python
tbf_team_roster(season=2027, team_process_id=265995)
```

_Last validated n/a._

## tbf_team_staff

Coaching and club staff of one team-season.

**Endpoint URL:** `GET https://miniappapi.tbf.org.tr/webapi-service/api/Team/get-team-detail-team-staff-by-season-and-league`

**Valid URL:** [https://miniappapi.tbf.org.tr/webapi-service/api/Team/get-team-detail-team-staff-by-season-and-league?season=2027&competition=bsl&team_process_id=265995](https://miniappapi.tbf.org.tr/webapi-service/api/Team/get-team-detail-team-staff-by-season-and-league?season=2027&competition=bsl&team_process_id=265995)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the list lacks raises NoDataError. |
| `competition` | `competition` |  |  | `Y` | TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was named TBL until 2007-08 and is listed under bsl. |
| `team_process_id` | `team_process_id` |  | `Y` |  | Required. Team-season id (team registration), e.g. 265995 = Tofas in BSL 2026-27; team_process_id in tbf_teams and tbf_schedule. New for every club in every league-season; not the stable club_id. |

### Returns {#tbf_team_staff-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv season key. Not the internal TBF season id (season_id). |
| `competition` | character | TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix the API reports). |
| `team_process_id` | integer | Team-season id of the request (key space: TBF team registrations). |
| `person_id` | integer | TBF person id of the staff member (code; staff are TBF persons too). |
| `first_name` | character | Given name(s) as registered with TBF, uppercase ('MEVLÜT TULGA'). |
| `last_name` | character | Family name as registered with TBF, uppercase ('ÖNGÖREN'). |
| `role` | character | Staff role in Turkish as sent (job), e.g. Antrenor = head coach. |
| `role_code` | integer | Staff sub-role code (subRolesCode). |

**`return_parsed=False`** — the raw body (`dict`): the TBF envelope `{data, isSuccess, message, problemDetails}` with personal fields redacted, plus `_request` (the season, competition, game and team ids the runtime resolved).

### Raises {#tbf_team_staff-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TBF has nothing for the request (an unknown game or player, a season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a partial page or a changed capped list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#tbf_team_staff-example}

```python
tbf_team_staff(season=2027, team_process_id=265995)
```

_Last validated n/a._
