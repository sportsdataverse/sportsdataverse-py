# Israeli Premier League — IBBA ASMX web service (basket.co.il, Israel)

> Israeli Premier League — IBBA ASMX web service (basket.co.il, Israel) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.basketball.ibba` — 16 endpoints.

> **Note:** Access: unofficial and reverse-engineered, keyless, for internal use. The wrappers read basket.co.il's ASP.NET web service (the ops its own site calls) and space requests at least one second apart; keep crawls slow and incremental. Payloads are not redistributed as release assets. Contact details (e-mail, phone, fax) and coaches' dates of birth are dropped by the parsers.

> **Note:** Season and ids. `season` is the END year and the host's own `cYear` (2026 = 2025-26): no mapping. `team_id` and `player_id` are per-SEASON registrations (Maccabi Tel Aviv = team 1109 in 2025-26, 2111 in 2026-27; the ASMX host has no cross-season person id: the stable key is the Segev player id in `ibba_segev_*`). `team_uid` is the stable club id (Maccabi = 10). `ibba_games.segev_game_id` joins `ibba_segev_*` `game_id` from 2025-26.

> **Note:** Coverage: league box scores from 2004-05 (`season=2005`); `external_id` / `segev_game_id` only from 2025-26; the women's league and Liga Leumit are NOT on this host (they are on Genius Sports org IBBA and in Segev). Every ASMX value is a string and Hebrew text is HTML-entity encoded; the parsers cast ids, counts and dates and unescape text. An unknown id answers an empty body (NoDataError); box scores of unfinished games are refused.

## ibba_configuration

Season settings: the season label, round counters and the standings cut line.

**Endpoint URL:** `GET https://basket.co.il/ws/ws.asmx/Configuration`

**Valid URL:** [https://basket.co.il/ws/ws.asmx/Configuration?cYear=2026](https://basket.co.il/ws/ws.asmx/Configuration?cYear=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `cYear` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26); sent to the host as ``cYear`` unchanged. |

### Returns {#ibba_configuration-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year as an integer (2026 = 2025-26): the host's own cYear, no mapping. |
| `season_label` | String | Season label as 'YYYY-YY' ('2025-26', '2004-05'). |
| `red_line` | Int64 | Standings position the site draws its cut line under (12 in 2025-26, 10 in 2004-05); meaning inferred. |
| `active_board_id` | Int64 | Board id the site treats as the active competition (17 = final series in 2025-26, 5 = league in 2004-05); meaning inferred. |
| `next_round` | Int64 | Next round (game number) to be played; meaning inferred. |
| `total_rounds` | Int64 | Rounds in the regular season (32 in 2025-26, 22 in 2004-05); meaning inferred. |

**`return_parsed=False`** — the raw body (`dict`): the ASMX body `{"<key>": [row, ...]}` (every value a string) plus `_request` (the op and the wire parameters sent).

### Raises {#ibba_configuration-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (HTTP 404, an empty body for an unknown id, an empty list). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a non-200, an error or changed envelope, a row for another season, team, player, board or game, a box score of an unfinished game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#ibba_configuration-example}

```python
ibba_configuration(season=2026)
```

## ibba_boards

Competitions (boards) of a season: league, cup, playoff rounds, play-in, Supercup.

**Endpoint URL:** `GET https://basket.co.il/ws/ws.asmx/Boards`

**Valid URL:** [https://basket.co.il/ws/ws.asmx/Boards?cYear=2026](https://basket.co.il/ws/ws.asmx/Boards?cYear=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `cYear` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26); sent to the host as ``cYear`` unchanged. |

### Returns {#ibba_boards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `board_id` | Int64 | Competition (board) id: 5 league, 10 cup, 16 / 26 / 17 playoff quarterfinals / semifinals / final, 33 play-in, 34 Supercup, 11 the 2004-05 semifinal group; 1001+ external competitions (1001 EuroLeague, 1003 Champions League, 1005 pre-season, 1008 EuroBasket, 1012 EuroCup, 1013 State Cup). |
| `season` | Int64 | Season END year as an integer (2026 = 2025-26): the host's own cYear, no mapping. |
| `board_name_he` | String | Competition name in Hebrew, HTML-unescaped. |
| `board_title_he` | String | Sponsor-branded competition name in Hebrew ('ליגת Winner סל' = Winner League). |
| `category_he` | String | Competition format in Hebrew: 'טבלה' = table, 'נוק אאוט' = knockout, 'סדרה' = series. |

**`return_parsed=False`** — the raw body (`dict`): the ASMX body `{"<key>": [row, ...]}` (every value a string) plus `_request` (the op and the wire parameters sent).

### Raises {#ibba_boards-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (HTTP 404, an empty body for an unknown id, an empty list). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a non-200, an error or changed envelope, a row for another season, team, player, board or game, a box score of an unfinished game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#ibba_boards-example}

```python
ibba_boards(season=2026)
```

## ibba_boards_ex

External competitions a club played in during a season (European cups, national team, pre-season).

**Endpoint URL:** `GET https://basket.co.il/ws/ws.asmx/BoardsEx`

**Valid URL:** [https://basket.co.il/ws/ws.asmx/BoardsEx?team_uid=10&cYear=2026](https://basket.co.il/ws/ws.asmx/BoardsEx?team_uid=10&cYear=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_uid` | `team_uid` |  | `Y` |  | Required. IBBA club id, stable across seasons (Maccabi Tel Aviv = 10; ``team_uid`` in ibba_teams). Use 0 for all clubs only where the route says so. |
| `cYear` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26); sent to the host as ``cYear`` unchanged. |

### Returns {#ibba_boards_ex-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `board_id` | Int64 | Competition (board) id: 5 league, 10 cup, 16 / 26 / 17 playoff quarterfinals / semifinals / final, 33 play-in, 34 Supercup, 11 the 2004-05 semifinal group; 1001+ external competitions (1001 EuroLeague, 1003 Champions League, 1005 pre-season, 1008 EuroBasket, 1012 EuroCup, 1013 State Cup). |
| `board_name_he` | String | Competition name in Hebrew, HTML-unescaped. |

**`return_parsed=False`** — the raw body (`dict`): the ASMX body `{"<key>": [row, ...]}` (every value a string) plus `_request` (the op and the wire parameters sent).

### Raises {#ibba_boards_ex-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (HTTP 404, an empty body for an unknown id, an empty list). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a non-200, an error or changed envelope, a row for another season, team, player, board or game, a box score of an unfinished game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#ibba_boards_ex-example}

```python
ibba_boards_ex(season=2026, team_uid=10)
```

## ibba_teams

Teams of a board and season with their standings line and club profile fields.

**Endpoint URL:** `GET https://basket.co.il/ws/ws.asmx/Teams`

**Valid URL:** [https://basket.co.il/ws/ws.asmx/Teams?cYear=2026&board_id=5](https://basket.co.il/ws/ws.asmx/Teams?cYear=2026&board_id=5)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `cYear` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26); sent to the host as ``cYear`` unchanged. |
| `board_id` | `board_id` |  |  | `Y` | Competition (board) id: 5 league, 10 cup, 16 / 26 / 17 playoff quarterfinals / semifinals / final, 33 play-in, 34 Supercup. Default 5. |

### Returns {#ibba_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_uid` | Int64 | IBBA CLUB id, stable across seasons (Maccabi Tel Aviv = 10): the team_uid parameter of the roster routes. Not the per-season team_id. |
| `team_id` | Int64 | Team-season id (key space: IBBA team registrations; a NEW id every season for the same club, e.g. Maccabi Tel Aviv = 1109 in 2025-26 and 2111 in 2026-27). Joins ibba_teams, ibba_players, ibba_coaches, ibba_box_score and the games' home / away ids. |
| `season` | Int64 | Season END year as an integer (2026 = 2025-26): the host's own cYear, no mapping. |
| `team_name_he` | String | Team name in Hebrew, HTML-unescaped. |
| `team_short_name_he` | String | Short team name in Hebrew, HTML-unescaped. |
| `team_name` | String | Team name in English as registered for the season. |
| `team_short_name` | String | Short team name in English. |
| `board_id` | Int64 | Competition (board) id: 5 league, 10 cup, 16 / 26 / 17 playoff quarterfinals / semifinals / final, 33 play-in, 34 Supercup, 11 the 2004-05 semifinal group; 1001+ external competitions (1001 EuroLeague, 1003 Champions League, 1005 pre-season, 1008 EuroBasket, 1012 EuroCup, 1013 State Cup). |
| `place` | Int64 | Rank in the standings of the board (1 = first). |
| `games` | Int64 | Games played (in the standings or in the season stats). |
| `wins` | Int64 | Games won in the season so far (standings line). |
| `losses` | Int64 | Games lost in the season so far (standings line). |
| `points_for` | Int64 | Points scored over the games played. |
| `points_against` | Int64 | Points conceded over the games played. |
| `table_points` | Int64 | Standings points (2 per win, 1 per loss in the league table). |
| `logo_url` | String | Absolute URL of the team logo. |
| `photo_path` | String | Site-relative path of the photo (team or player picture); prefix with https://www.basket.co.il. |
| `address_he` | String | Club address in Hebrew, HTML-unescaped. |
| `sponsors_he` | String | Club sponsors in Hebrew, comma-separated. |
| `facebook_url` | String | Club Facebook page URL; null when none. |
| `twitter_url` | String | Club Twitter / X profile URL; null when none. |
| `instagram_url` | String | Club Instagram profile URL; null when none. |
| `youtube_url` | String | Club YouTube channel URL; null when none. |
| `ticket_url` | String | Ticket-sales URL; null when none. |
| `subscribe_url` | String | Season-ticket subscription URL; null when none. |
| `coaches_he` | String | Coaching staff as one Hebrew text ('מאמן: ..., עוזר מאמן: ...'). |

**`return_parsed=False`** — the raw body (`dict`): the ASMX body `{"<key>": [row, ...]}` (every value a string) plus `_request` (the op and the wire parameters sent).

### Raises {#ibba_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (HTTP 404, an empty body for an unknown id, an empty list). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a non-200, an error or changed envelope, a row for another season, team, player, board or game, a box score of an unfinished game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#ibba_teams-example}

```python
ibba_teams(season=2026)
```

## ibba_team

One team-season: standings line and club profile (colours, sponsors, social links, arena id).

**Endpoint URL:** `GET https://basket.co.il/ws/ws.asmx/TeamById`

**Valid URL:** [https://basket.co.il/ws/ws.asmx/TeamById?team_id=1109&team_uid=10&cYear=2026](https://basket.co.il/ws/ws.asmx/TeamById?team_id=1109&team_uid=10&cYear=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Required. Team-season id (new every season; Maccabi = 1109 in 2025-26): ``team_id`` in ibba_teams, ibba_games ``home_team_id`` / ``away_team_id`` or ibba_box_score. |
| `team_uid` | `team_uid` |  | `Y` |  | Required. IBBA club id, stable across seasons (Maccabi Tel Aviv = 10; ``team_uid`` in ibba_teams). Use 0 for all clubs only where the route says so. |
| `cYear` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26); sent to the host as ``cYear`` unchanged. |

### Returns {#ibba_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_uid` | Int64 | IBBA CLUB id, stable across seasons (Maccabi Tel Aviv = 10): the team_uid parameter of the roster routes. Not the per-season team_id. |
| `team_id` | Int64 | Team-season id (key space: IBBA team registrations; a NEW id every season for the same club, e.g. Maccabi Tel Aviv = 1109 in 2025-26 and 2111 in 2026-27). Joins ibba_teams, ibba_players, ibba_coaches, ibba_box_score and the games' home / away ids. |
| `season` | Int64 | Season END year as an integer (2026 = 2025-26): the host's own cYear, no mapping. |
| `team_name_he` | String | Team name in Hebrew, HTML-unescaped. |
| `team_short_name_he` | String | Short team name in Hebrew, HTML-unescaped. |
| `team_name` | String | Team name in English as registered for the season. |
| `team_short_name` | String | Short team name in English. |
| `wins` | Int64 | Games won in the season so far (standings line). |
| `losses` | Int64 | Games lost in the season so far (standings line). |
| `points_for` | Int64 | Points scored over the games played. |
| `points_against` | Int64 | Points conceded over the games played. |
| `table_points` | Int64 | Standings points (2 per win, 1 per loss in the league table). |
| `place` | Int64 | Rank in the standings of the board (1 = first). |
| `website_url` | String | Club website URL. |
| `photo_path` | String | Site-relative path of the photo (team or player picture); prefix with https://www.basket.co.il. |
| `icon_path` | String | Site-relative path of the small team icon. |
| `logo_url` | String | Absolute URL of the team logo. |
| `icon3_path` | String | Site-relative path of the third team icon variant. |
| `arena_id` | Int64 | Home arena id in the IBBA site's own arena table (no arena route is modelled). |
| `address_he` | String | Club address in Hebrew, HTML-unescaped. |
| `address` | String | Club address in English, HTML-unescaped. |
| `colors_he` | String | Team colours in Hebrew. |
| `colors` | String | Team colours in English. |
| `sponsors_he` | String | Club sponsors in Hebrew, comma-separated. |
| `sponsors` | String | Club sponsors in English, comma-separated. |
| `about_he` | String | Club profile in Hebrew as HTML markup (honours, history); not stripped. |
| `about` | String | Club profile in English as HTML markup; null when the club has none. |
| `facebook_url` | String | Club Facebook page URL; null when none. |
| `twitter_url` | String | Club Twitter / X profile URL; null when none. |
| `instagram_url` | String | Club Instagram profile URL; null when none. |
| `youtube_url` | String | Club YouTube channel URL; null when none. |
| `subscribe_url` | String | Season-ticket subscription URL; null when none. |
| `diary_url` | String | Calendar-subscription (add-to-diary) URL of the club's fixtures. |

**`return_parsed=False`** — the raw body (`dict`): the ASMX body `{"<key>": [row, ...]}` (every value a string) plus `_request` (the op and the wire parameters sent).

### Raises {#ibba_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (HTTP 404, an empty body for an unknown id, an empty list). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a non-200, an error or changed envelope, a row for another season, team, player, board or game, a box score of an unfinished game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#ibba_team-example}

```python
ibba_team(season=2026, team_id=1109, team_uid=10)
```

## ibba_players

The roster of a team-season: jersey, names, position, height and birth date.

**Endpoint URL:** `GET https://basket.co.il/ws/ws.asmx/Players`

**Valid URL:** [https://basket.co.il/ws/ws.asmx/Players?team_id=1109&team_uid=10&cYear=2026](https://basket.co.il/ws/ws.asmx/Players?team_id=1109&team_uid=10&cYear=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Required. Team-season id (new every season; Maccabi = 1109 in 2025-26): ``team_id`` in ibba_teams, ibba_games ``home_team_id`` / ``away_team_id`` or ibba_box_score. |
| `team_uid` | `team_uid` |  | `Y` |  | Required. IBBA club id, stable across seasons (Maccabi Tel Aviv = 10; ``team_uid`` in ibba_teams). Use 0 for all clubs only where the route says so. |
| `cYear` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26); sent to the host as ``cYear`` unchanged. |

### Returns {#ibba_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Player-season id (key space: IBBA player registrations; a NEW id every season, e.g. Jaylen Hoard = 21912 in 2025-26 and 25997 in 2026-27; the ASMX host has no cross-season person id). Joins ibba_players, ibba_player, ibba_box_score and ibba_leaders; null on the team-credited and TOTAL box rows. |
| `jersey` | String | Jersey number as a zero-padded string ('02'); a box score also uses '_TEAM' and 'TOTAL' for its two non-player lines. |
| `season` | Int64 | Season END year as an integer (2026 = 2025-26): the host's own cYear, no mapping. |
| `team_id` | Int64 | Team-season id (key space: IBBA team registrations; a NEW id every season for the same club, e.g. Maccabi Tel Aviv = 1109 in 2025-26 and 2111 in 2026-27). Joins ibba_teams, ibba_players, ibba_coaches, ibba_box_score and the games' home / away ids. |
| `player_name_he` | String | Player name in Hebrew, HTML-unescaped. |
| `player_name` | String | Player name in English, HTML-unescaped. |
| `is_replaced` | Boolean | True when the person was replaced during the season (left the roster or staff). |
| `is_local` | Boolean | True for an Israeli (local) player; the isr_ leader categories count only these. |
| `photo_path` | String | Site-relative path of the photo (team or player picture); prefix with https://www.basket.co.il. |
| `height_m` | Float64 | Height in metres (1.93). |
| `position_he` | String | Position in Hebrew. |
| `position` | String | Position code: PG, G, G-F, F, F-C or C. |
| `birth_date` | String | Date of birth (public player bio); null when not published. |

**`return_parsed=False`** — the raw body (`dict`): the ASMX body `{"<key>": [row, ...]}` (every value a string) plus `_request` (the op and the wire parameters sent).

### Raises {#ibba_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (HTTP 404, an empty body for an unknown id, an empty list). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a non-200, an error or changed envelope, a row for another season, team, player, board or game, a box score of an unfinished game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#ibba_players-example}

```python
ibba_players(season=2026, team_id=1109, team_uid=10)
```

## ibba_player

One player-season row: jersey, team, names and photo.

**Endpoint URL:** `GET https://basket.co.il/ws/ws.asmx/PlayerById`

**Valid URL:** [https://basket.co.il/ws/ws.asmx/PlayerById?player_id=21912&cYear=2026](https://basket.co.il/ws/ws.asmx/PlayerById?player_id=21912&cYear=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Required. Player-season id (new every season; Jaylen Hoard = 21912 in 2025-26): ``player_id`` in ibba_players. |
| `cYear` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26); sent to the host as ``cYear`` unchanged. |

### Returns {#ibba_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Player-season id (key space: IBBA player registrations; a NEW id every season, e.g. Jaylen Hoard = 21912 in 2025-26 and 25997 in 2026-27; the ASMX host has no cross-season person id). Joins ibba_players, ibba_player, ibba_box_score and ibba_leaders; null on the team-credited and TOTAL box rows. |
| `jersey` | String | Jersey number as a zero-padded string ('02'); a box score also uses '_TEAM' and 'TOTAL' for its two non-player lines. |
| `season` | Int64 | Season END year as an integer (2026 = 2025-26): the host's own cYear, no mapping. |
| `team_id` | Int64 | Team-season id (key space: IBBA team registrations; a NEW id every season for the same club, e.g. Maccabi Tel Aviv = 1109 in 2025-26 and 2111 in 2026-27). Joins ibba_teams, ibba_players, ibba_coaches, ibba_box_score and the games' home / away ids. |
| `player_name_he` | String | Player name in Hebrew, HTML-unescaped. |
| `player_name` | String | Player name in English, HTML-unescaped. |
| `is_replaced` | Boolean | True when the person was replaced during the season (left the roster or staff). |
| `is_local` | Boolean | True for an Israeli (local) player; the isr_ leader categories count only these. |
| `photo_path` | String | Site-relative path of the photo (team or player picture); prefix with https://www.basket.co.il. |

**`return_parsed=False`** — the raw body (`dict`): the ASMX body `{"<key>": [row, ...]}` (every value a string) plus `_request` (the op and the wire parameters sent).

### Raises {#ibba_player-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (HTTP 404, an empty body for an unknown id, an empty list). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a non-200, an error or changed envelope, a row for another season, team, player, board or game, a box score of an unfinished game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#ibba_player-example}

```python
ibba_player(player_id=21912, season=2026)
```

## ibba_coaches

The coaching staff of a team-season (birth dates are dropped).

**Endpoint URL:** `GET https://basket.co.il/ws/ws.asmx/Coaches`

**Valid URL:** [https://basket.co.il/ws/ws.asmx/Coaches?team_id=1109&team_uid=10&cYear=2026](https://basket.co.il/ws/ws.asmx/Coaches?team_id=1109&team_uid=10&cYear=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Required. Team-season id (new every season; Maccabi = 1109 in 2025-26): ``team_id`` in ibba_teams, ibba_games ``home_team_id`` / ``away_team_id`` or ibba_box_score. |
| `team_uid` | `team_uid` |  | `Y` |  | Required. IBBA club id, stable across seasons (Maccabi Tel Aviv = 10; ``team_uid`` in ibba_teams). Use 0 for all clubs only where the route says so. |
| `cYear` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26); sent to the host as ``cYear`` unchanged. |

### Returns {#ibba_coaches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `coach_id` | Int64 | Coach id in the IBBA coach table (per team-season registration as far as the samples show; stability across seasons is unverified). |
| `coach_type` | Int64 | Staff role code: 1 = head coach, 2 = assistant coach (inferred from the Maccabi Tel Aviv sample). |
| `season` | Int64 | Season END year as an integer (2026 = 2025-26): the host's own cYear, no mapping. |
| `team_id` | Int64 | Team-season id (key space: IBBA team registrations; a NEW id every season for the same club, e.g. Maccabi Tel Aviv = 1109 in 2025-26 and 2111 in 2026-27). Joins ibba_teams, ibba_players, ibba_coaches, ibba_box_score and the games' home / away ids. |
| `coach_name_he` | String | Coach name in Hebrew, HTML-unescaped. |
| `coach_name` | String | Coach name in English. |
| `is_replaced` | Boolean | True when the person was replaced during the season (left the roster or staff). |
| `photo_path` | String | Site-relative path of the photo (team or player picture); prefix with https://www.basket.co.il. |
| `sort_order` | Int64 | Display order of the coach within the staff list (mOrder). |

**`return_parsed=False`** — the raw body (`dict`): the ASMX body `{"<key>": [row, ...]}` (every value a string) plus `_request` (the op and the wire parameters sent).

### Raises {#ibba_coaches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (HTTP 404, an empty body for an unknown id, an empty list). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a non-200, an error or changed envelope, a row for another season, team, player, board or game, a box score of an unfinished game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#ibba_coaches-example}

```python
ibba_coaches(season=2026, team_id=1109, team_uid=10)
```

## ibba_box_score

Box score of a finished game: player lines, a team-credited line per side and the TOTAL line per side.

**Endpoint URL:** `GET https://basket.co.il/ws/ws.asmx/Boxscore`

**Valid URL:** [https://basket.co.il/ws/ws.asmx/Boxscore?game_id=26392&cYear=2026](https://basket.co.il/ws/ws.asmx/Boxscore?game_id=26392&cYear=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. IBBA game id (e.g. 26392): ``game_id`` in ibba_games. |
| `cYear` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26); sent to the host as ``cYear`` unchanged. |

### Returns {#ibba_box_score-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Player-season id (key space: IBBA player registrations; a NEW id every season, e.g. Jaylen Hoard = 21912 in 2025-26 and 25997 in 2026-27; the ASMX host has no cross-season person id). Joins ibba_players, ibba_player, ibba_box_score and ibba_leaders; null on the team-credited and TOTAL box rows. |
| `jersey` | String | Jersey number as a zero-padded string ('02'); a box score also uses '_TEAM' and 'TOTAL' for its two non-player lines. |
| `team_id` | Int64 | Team-season id (key space: IBBA team registrations; a NEW id every season for the same club, e.g. Maccabi Tel Aviv = 1109 in 2025-26 and 2111 in 2026-27). Joins ibba_teams, ibba_players, ibba_coaches, ibba_box_score and the games' home / away ids. |
| `game_id` | Int64 | IBBA game id (key space: basket.co.il games). Joins ibba_games to ibba_box_score; in ibba_games_ex it is an EXTERNAL game id (the ig_ space, e.g. 8888 for a pre-season game). |
| `board_id` | Int64 | Board id of the game (5 = league). |
| `season` | Int64 | Season END year as an integer (2026 = 2025-26): the host's own cYear, no mapping. |
| `round` | Int64 | Round (game number) of the game within its board. |
| `starter` | Boolean | True when the player started the game. |
| `minutes` | Int64 | Minutes played, whole minutes; a TOTAL line holds the team's player-minutes (200 plus 25 per overtime). |
| `pts` | Int64 | Points scored by the player or team (free throws included). |
| `fg2m` | Int64 | Two-point field goals made. |
| `fg2a` | Int64 | Two-point field goals attempted (verified equal to Segev made + missed). |
| `fg2_pct` | Float64 | Two-point field-goal percentage, 0-100. |
| `fg3m` | Int64 | Three-point field goals made. |
| `fg3a` | Int64 | Three-point field goals attempted. |
| `fg3_pct` | Float64 | Three-point field-goal percentage, 0-100. |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throws attempted. |
| `ft_pct` | Float64 | Free-throw percentage, 0-100. |
| `dreb` | Int64 | Defensive rebounds. |
| `oreb` | Int64 | Offensive rebounds. |
| `reb` | Int64 | Total rebounds. |
| `fouls_drawn` | Int64 | Fouls DRAWN: fouls opponents committed against the player or team (the wire's fouls_against; verified equal to Segev f_drawn). |
| `fouls` | Int64 | Fouls COMMITTED by the player or team (the wire's fouls_for; verified equal to Segev f). |
| `steals` | Int64 | Steals made by the player or team. |
| `turnovers` | Int64 | Turnovers (the wire's misspelt tournovers). |
| `assists` | Int64 | Assists credited to the player or team. |
| `blocks` | Int64 | Shots the player or team blocked. |
| `blocks_against` | Int64 | Own shots that were blocked. |
| `val` | Int64 | Efficiency rating (valuation; verified equal to Segev rate); -1 on 2004-05 team-credited lines. |
| `plus_minus` | Int64 | Plus-minus; 0 on every line of the 2004-05 sample (not recorded then, inferred). |
| `dunks` | Int64 | Dunks made by the player or team. |
| `row_type` | String | Kind of box line: player, team (actions credited to the team: team rebounds and turnovers) or total (the team's TOTAL line, equal to players plus team lines). |

**`return_parsed=False`** — the raw body (`dict`): the ASMX body `{"<key>": [row, ...]}` (every value a string) plus `_request` (the op and the wire parameters sent).

### Raises {#ibba_box_score-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (HTTP 404, an empty body for an unknown id, an empty list). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a non-200, an error or changed envelope, a row for another season, team, player, board or game, a box score of an unfinished game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#ibba_box_score-example}

```python
ibba_box_score(game_id=26392, season=2026)
```

## ibba_player_stats

A player-season's totals, or per-game averages with is_averages=True.

**Endpoint URL:** `GET https://basket.co.il/ws/ws.asmx/PlayerStatsById`

**Valid URL:** [https://basket.co.il/ws/ws.asmx/PlayerStatsById?player_id=21912&cYear=2026&isAverages=False&isPlayOff=False](https://basket.co.il/ws/ws.asmx/PlayerStatsById?player_id=21912&cYear=2026&isAverages=False&isPlayOff=False)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Required. Player-season id (new every season; Jaylen Hoard = 21912 in 2025-26): ``player_id`` in ibba_players. |
| `cYear` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26); sent to the host as ``cYear`` unchanged. |
| `isAverages` | `is_averages` |  |  | `Y` | True returns per-game averages (decimals); False (default) the season totals. |
| `isPlayOff` | `is_playoff` |  |  | `Y` | True returns the playoff games only; False (default) the regular season. |

### Returns {#ibba_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Player-season id (key space: IBBA player registrations; a NEW id every season, e.g. Jaylen Hoard = 21912 in 2025-26 and 25997 in 2026-27; the ASMX host has no cross-season person id). Joins ibba_players, ibba_player, ibba_box_score and ibba_leaders; null on the team-credited and TOTAL box rows. |
| `jersey` | String | Jersey number as a zero-padded string ('02'); a box score also uses '_TEAM' and 'TOTAL' for its two non-player lines. |
| `season` | Int64 | Season END year as an integer (2026 = 2025-26): the host's own cYear, no mapping. |
| `team_id` | Int64 | Team-season id (key space: IBBA team registrations; a NEW id every season for the same club, e.g. Maccabi Tel Aviv = 1109 in 2025-26 and 2111 in 2026-27). Joins ibba_teams, ibba_players, ibba_coaches, ibba_box_score and the games' home / away ids. |
| `games` | Int64 | Games played (in the standings or in the season stats). |
| `minutes` | Float64 | Minutes played, whole minutes; a TOTAL line holds the team's player-minutes (200 plus 25 per overtime). Season total, or the per-game average when is_averages is true. |
| `pts` | Float64 | Points scored by the player or team (free throws included). Season total, or the per-game average when is_averages is true. |
| `fg2m` | Float64 | Two-point field goals made. Season total, or the per-game average when is_averages is true. |
| `fg2a` | Float64 | Two-point field goals attempted (verified equal to Segev made + missed). Season total, or the per-game average when is_averages is true. |
| `fg2_pct` | Float64 | Two-point field-goal percentage, 0-100. Season total, or the per-game average when is_averages is true. |
| `fg3m` | Float64 | Three-point field goals made. Season total, or the per-game average when is_averages is true. |
| `fg3a` | Float64 | Three-point field goals attempted. Season total, or the per-game average when is_averages is true. |
| `fg3_pct` | Float64 | Three-point field-goal percentage, 0-100. Season total, or the per-game average when is_averages is true. |
| `ftm` | Float64 | Free throws made. Season total, or the per-game average when is_averages is true. |
| `fta` | Float64 | Free throws attempted. Season total, or the per-game average when is_averages is true. |
| `ft_pct` | Float64 | Free-throw percentage, 0-100. Season total, or the per-game average when is_averages is true. |
| `dreb` | Float64 | Defensive rebounds. Season total, or the per-game average when is_averages is true. |
| `oreb` | Float64 | Offensive rebounds. Season total, or the per-game average when is_averages is true. |
| `reb` | Float64 | Total rebounds. Season total, or the per-game average when is_averages is true. |
| `fouls_drawn` | Float64 | Fouls DRAWN: fouls opponents committed against the player or team (the wire's fouls_against; verified equal to Segev f_drawn). Season total, or the per-game average when is_averages is true. |
| `fouls` | Float64 | Fouls COMMITTED by the player or team (the wire's fouls_for; verified equal to Segev f). Season total, or the per-game average when is_averages is true. |
| `steals` | Float64 | Steals made by the player or team. Season total, or the per-game average when is_averages is true. |
| `turnovers` | Float64 | Turnovers (the wire's misspelt tournovers). Season total, or the per-game average when is_averages is true. |
| `assists` | Float64 | Assists credited to the player or team. Season total, or the per-game average when is_averages is true. |
| `blocks` | Float64 | Shots the player or team blocked. Season total, or the per-game average when is_averages is true. |
| `blocks_against` | Float64 | Own shots that were blocked. Season total, or the per-game average when is_averages is true. |
| `val` | Float64 | Efficiency rating (valuation; verified equal to Segev rate); -1 on 2004-05 team-credited lines. Season total, or the per-game average when is_averages is true. |
| `plus_minus` | Float64 | Plus-minus; 0 on every line of the 2004-05 sample (not recorded then, inferred). Season total, or the per-game average when is_averages is true. |
| `dunks` | Float64 | Dunks made by the player or team. Season total, or the per-game average when is_averages is true. |

**`return_parsed=False`** — the raw body (`dict`): the ASMX body `{"<key>": [row, ...]}` (every value a string) plus `_request` (the op and the wire parameters sent).

### Raises {#ibba_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (HTTP 404, an empty body for an unknown id, an empty list). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a non-200, an error or changed envelope, a row for another season, team, player, board or game, a box score of an unfinished game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#ibba_player_stats-example}

```python
ibba_player_stats(player_id=21912, season=2026)
```

## ibba_stats_types

The stat categories ibba_leaders accepts (the stat_type codes) with their Hebrew names.

**Endpoint URL:** `GET https://basket.co.il/ws/ws.asmx/StatsTypes`

**Valid URL:** [https://basket.co.il/ws/ws.asmx/StatsTypes](https://basket.co.il/ws/ws.asmx/StatsTypes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#ibba_stats_types-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `stat_type` | String | Stat category code to pass to ibba_leaders as stat_type (bracketed codes such as '[OR]' are literal). |
| `stat_name_he` | String | Stat category name in Hebrew. |

**`return_parsed=False`** — the raw body (`dict`): the ASMX body `{"<key>": [row, ...]}` (every value a string) plus `_request` (the op and the wire parameters sent).

### Raises {#ibba_stats_types-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (HTTP 404, an empty body for an unknown id, an empty list). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a non-200, an error or changed envelope, a row for another season, team, player, board or game, a box score of an unfinished game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#ibba_stats_types-example}

```python
ibba_stats_types()
```

## ibba_leaders

The top five players of a season in one stat category, for all teams or one team.

**Endpoint URL:** `GET https://basket.co.il/ws/ws.asmx/LeaderByType`

**Valid URL:** [https://basket.co.il/ws/ws.asmx/LeaderByType?stat_type=pts&cYear=2026&team_id=0](https://basket.co.il/ws/ws.asmx/LeaderByType?stat_type=pts&cYear=2026&team_id=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `stat_type` | `stat_type` |  | `Y` |  | Required. Stat category code from ibba_stats_types: VAL, pts, isr_pts, plusminus, dunk, ASS, TR, [OR], DR, isr_TR, ST, [TO], ASSTO, BKF, FF, [min], p2, p3, p1, p32T, p22T, p12T, PTSMIN, TRMIN, ASSMIN. |
| `cYear` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26); sent to the host as ``cYear`` unchanged. |
| `team_id` | `team_id` |  |  | `Y` | Team-season id to restrict the list to one team; 0 (default) for all teams. |

### Returns {#ibba_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `value` | Float64 | The leader's value in the requested category (per-game average, percentage or count). |
| `games` | Int64 | Games the leader played. |
| `season` | Int64 | Season END year as an integer (2026 = 2025-26): the host's own cYear, no mapping. |
| `team_id` | Int64 | Team-season id of the leader's team. |
| `jersey` | String | The leader's jersey number as a zero-padded string. |
| `player_id` | Int64 | Player-season id (key space: IBBA player registrations; a NEW id every season, e.g. Jaylen Hoard = 21912 in 2025-26 and 25997 in 2026-27; the ASMX host has no cross-season person id). Joins ibba_players, ibba_player, ibba_box_score and ibba_leaders; null on the team-credited and TOTAL box rows. |
| `player_name_he` | String | Player name in Hebrew, HTML-unescaped. |
| `photo_path` | String | Site-relative path of the photo (team or player picture); prefix with https://www.basket.co.il. |

**`return_parsed=False`** — the raw body (`dict`): the ASMX body `{"<key>": [row, ...]}` (every value a string) plus `_request` (the op and the wire parameters sent).

### Raises {#ibba_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (HTTP 404, an empty body for an unknown id, an empty list). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a non-200, an error or changed envelope, a row for another season, team, player, board or game, a box score of an unfinished game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#ibba_leaders-example}

```python
ibba_leaders(season=2026, stat_type='pts')
```

## ibba_team_stats

A team-season's totals, or per-game averages with is_averages=True.

**Endpoint URL:** `GET https://basket.co.il/ws/ws.asmx/TeamStatsById`

**Valid URL:** [https://basket.co.il/ws/ws.asmx/TeamStatsById?team_id=1109&cYear=2026&isAverages=False&isPlayOff=False](https://basket.co.il/ws/ws.asmx/TeamStatsById?team_id=1109&cYear=2026&isAverages=False&isPlayOff=False)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Required. Team-season id (new every season; Maccabi = 1109 in 2025-26): ``team_id`` in ibba_teams, ibba_games ``home_team_id`` / ``away_team_id`` or ibba_box_score. |
| `cYear` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26); sent to the host as ``cYear`` unchanged. |
| `isAverages` | `is_averages` |  |  | `Y` | True returns per-game averages (decimals); False (default) the season totals. |
| `isPlayOff` | `is_playoff` |  |  | `Y` | True returns the playoff games only; False (default) the regular season. |

### Returns {#ibba_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year as an integer (2026 = 2025-26): the host's own cYear, no mapping. |
| `team_id` | Int64 | Team-season id (key space: IBBA team registrations; a NEW id every season for the same club, e.g. Maccabi Tel Aviv = 1109 in 2025-26 and 2111 in 2026-27). Joins ibba_teams, ibba_players, ibba_coaches, ibba_box_score and the games' home / away ids. |
| `games` | Int64 | Games played (in the standings or in the season stats). |
| `minutes` | Float64 | Minutes played, whole minutes; a TOTAL line holds the team's player-minutes (200 plus 25 per overtime). Season total, or the per-game average when is_averages is true. |
| `pts` | Float64 | Points scored by the player or team (free throws included). Season total, or the per-game average when is_averages is true. |
| `fg2m` | Float64 | Two-point field goals made. Season total, or the per-game average when is_averages is true. |
| `fg2a` | Float64 | Two-point field goals attempted (verified equal to Segev made + missed). Season total, or the per-game average when is_averages is true. |
| `fg2_pct` | Float64 | Two-point field-goal percentage, 0-100. Season total, or the per-game average when is_averages is true. |
| `fg3m` | Float64 | Three-point field goals made. Season total, or the per-game average when is_averages is true. |
| `fg3a` | Float64 | Three-point field goals attempted. Season total, or the per-game average when is_averages is true. |
| `fg3_pct` | Float64 | Three-point field-goal percentage, 0-100. Season total, or the per-game average when is_averages is true. |
| `ftm` | Float64 | Free throws made. Season total, or the per-game average when is_averages is true. |
| `fta` | Float64 | Free throws attempted. Season total, or the per-game average when is_averages is true. |
| `ft_pct` | Float64 | Free-throw percentage, 0-100. Season total, or the per-game average when is_averages is true. |
| `dreb` | Float64 | Defensive rebounds. Season total, or the per-game average when is_averages is true. |
| `oreb` | Float64 | Offensive rebounds. Season total, or the per-game average when is_averages is true. |
| `reb` | Float64 | Total rebounds. Season total, or the per-game average when is_averages is true. |
| `fouls_drawn` | Float64 | Fouls DRAWN: fouls opponents committed against the player or team (the wire's fouls_against; verified equal to Segev f_drawn). Season total, or the per-game average when is_averages is true. |
| `fouls` | Float64 | Fouls COMMITTED by the player or team (the wire's fouls_for; verified equal to Segev f). Season total, or the per-game average when is_averages is true. |
| `steals` | Float64 | Steals made by the player or team. Season total, or the per-game average when is_averages is true. |
| `turnovers` | Float64 | Turnovers (the wire's misspelt tournovers). Season total, or the per-game average when is_averages is true. |
| `assists` | Float64 | Assists credited to the player or team. Season total, or the per-game average when is_averages is true. |
| `blocks` | Float64 | Shots the player or team blocked. Season total, or the per-game average when is_averages is true. |
| `blocks_against` | Float64 | Own shots that were blocked. Season total, or the per-game average when is_averages is true. |
| `val` | Float64 | Efficiency rating (valuation; verified equal to Segev rate); -1 on 2004-05 team-credited lines. Season total, or the per-game average when is_averages is true. |
| `plus_minus` | Float64 | Plus-minus; 0 on every line of the 2004-05 sample (not recorded then, inferred). Season total, or the per-game average when is_averages is true. |
| `dunks` | Float64 | Dunks made by the player or team. Season total, or the per-game average when is_averages is true. |

**`return_parsed=False`** — the raw body (`dict`): the ASMX body `{"<key>": [row, ...]}` (every value a string) plus `_request` (the op and the wire parameters sent).

### Raises {#ibba_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (HTTP 404, an empty body for an unknown id, an empty list). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a non-200, an error or changed envelope, a row for another season, team, player, board or game, a box score of an unfinished game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#ibba_team_stats-example}

```python
ibba_team_stats(season=2026, team_id=1109)
```

## ibba_games

Games (schedule and results) of a season filtered by board, round, team and game; 0 means any.

**Endpoint URL:** `GET https://basket.co.il/ws/ws.asmx/Games`

**Valid URL:** [https://basket.co.il/ws/ws.asmx/Games?cYear=2026&board_id=5&board_round=1&team_id=0&team_uid=0&game_id=0](https://basket.co.il/ws/ws.asmx/Games?cYear=2026&board_id=5&board_round=1&team_id=0&team_uid=0&game_id=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `cYear` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26); sent to the host as ``cYear`` unchanged. |
| `board_id` | `board_id` |  |  | `Y` | Competition (board) id: 5 league, 10 cup, 16 / 26 / 17 playoff quarterfinals / semifinals / final, 33 play-in, 34 Supercup. 0 (default) for every board. |
| `board_round` | `board_round` |  |  | `Y` | Round (game number) within the board; 0 (default) for every round. |
| `team_id` | `team_id` |  |  | `Y` | Team-season id; 0 (default) for every team. |
| `team_uid` | `team_uid` |  |  | `Y` | IBBA club id; 0 (default) for every club. |
| `game_id` | `game_id` |  |  | `Y` | IBBA game id; 0 (default) for every game. |

### Returns {#ibba_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | IBBA game id (key space: basket.co.il games). Joins ibba_games to ibba_box_score; in ibba_games_ex it is an EXTERNAL game id (the ig_ space, e.g. 8888 for a pre-season game). |
| `game_date` | String | Local Israel date of the game. |
| `season` | Int64 | Season END year as an integer (2026 = 2025-26): the host's own cYear, no mapping. |
| `home_team_id` | Int64 | Home team-season id (team1 on the wire; verified home against Segev). On external boards (board_id >= 1001) an external-team id (the ig_team_ space), not a team-season. |
| `away_team_id` | Int64 | Away team-season id (team2 on the wire). On external boards an external-team id. |
| `home_team_he` | String | Home team name in Hebrew, HTML-unescaped. |
| `away_team_he` | String | Away team name in Hebrew, HTML-unescaped. |
| `home_team` | String | Home team name in English; null for external games, which carry Hebrew names only. |
| `away_team` | String | Away team name in English; null for external games. |
| `home_logo_url` | String | Absolute URL of the home team logo. |
| `away_logo_url` | String | Absolute URL of the away team logo. |
| `home_score` | Int64 | Home final score; 0 before the game is played. |
| `away_score` | Int64 | Away final score; 0 before the game is played. |
| `board_id` | Int64 | Competition (board) id: 5 league, 10 cup, 16 / 26 / 17 playoff quarterfinals / semifinals / final, 33 play-in, 34 Supercup, 11 the 2004-05 semifinal group; 1001+ external competitions (1001 EuroLeague, 1003 Champions League, 1005 pre-season, 1008 EuroBasket, 1012 EuroCup, 1013 State Cup). |
| `board_name_he` | String | Competition name in Hebrew, HTML-unescaped. |
| `board_name` | String | Competition name in English ('Winner League'); null for external boards. |
| `round` | Int64 | Round (game number) within the board; null for external games. |
| `tip_off` | String | Tip-off 'HH:MM', local Israel time, as text; null in 2004-05. It can disagree with Segev (game 26646: 20:15 here, 19:30 in Segev). |
| `venue_he` | String | Venue in Hebrew, HTML-unescaped; null in 2004-05. |
| `venue` | String | Venue in English, HTML-unescaped; null in 2004-05. |
| `referees_he` | String | Referees' names in Hebrew, comma-separated; ', , ' when unknown. |
| `attendance` | Int64 | Attendance; 0 when unknown. |
| `overtime` | Boolean | True when the game went to overtime. |
| `overtimes` | Int64 | Number of overtime periods played. |
| `home_score_by_period` | String | Home points per period, pipe-separated ('18\|23\|30\|21'); null for external games. |
| `away_score_by_period` | String | Away points per period, pipe-separated ('29\|26\|27\|22'); null for external games. |
| `tickets_url` | String | Ticketing URL; null when none. |
| `external_id` | String | The wire's ExternalID: the Segev game id (a string) for league games from 2025-26, a news URL for pre-season games, null before 2025-26. |
| `segev_game_id` | Int64 | external_id read as an integer Segev game id (digits only), else null; joins ibba_segev_* game_id. Derived by the parser. |
| `is_external` | Boolean | True when board_id >= 1001 (an external competition, whose team ids are not team-seasons). Derived by the parser. |

**`return_parsed=False`** — the raw body (`dict`): the ASMX body `{"<key>": [row, ...]}` (every value a string) plus `_request` (the op and the wire parameters sent).

### Raises {#ibba_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (HTTP 404, an empty body for an unknown id, an empty list). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a non-200, an error or changed envelope, a row for another season, team, player, board or game, a box score of an unfinished game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#ibba_games-example}

```python
ibba_games(board_id=5, board_round=1, season=2026)
```

## ibba_games_ex

A club's games of a season in external competitions (BoardsEx boards: European cups, pre-season).

**Endpoint URL:** `GET https://basket.co.il/ws/ws.asmx/GamesEx`

**Valid URL:** [https://basket.co.il/ws/ws.asmx/GamesEx?team_uid=10&cYear=2026&board_id=0](https://basket.co.il/ws/ws.asmx/GamesEx?team_uid=10&cYear=2026&board_id=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_uid` | `team_uid` |  | `Y` |  | Required. IBBA club id, stable across seasons (Maccabi Tel Aviv = 10; ``team_uid`` in ibba_teams). Use 0 for all clubs only where the route says so. |
| `cYear` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26); sent to the host as ``cYear`` unchanged. |
| `board_id` | `board_id` |  |  | `Y` | External board id from ibba_boards_ex (1001 EuroLeague, 1003 Champions League, ...); 0 (default) for all. |

### Returns {#ibba_games_ex-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | IBBA game id (key space: basket.co.il games). Joins ibba_games to ibba_box_score; in ibba_games_ex it is an EXTERNAL game id (the ig_ space, e.g. 8888 for a pre-season game). |
| `game_date` | String | Local Israel date of the game. |
| `season` | Int64 | Season END year as an integer (2026 = 2025-26): the host's own cYear, no mapping. |
| `home_team_id` | Int64 | Home team-season id (team1 on the wire; verified home against Segev). On external boards (board_id >= 1001) an external-team id (the ig_team_ space), not a team-season. |
| `away_team_id` | Int64 | Away team-season id (team2 on the wire). On external boards an external-team id. |
| `home_team_he` | String | Home team name in Hebrew, HTML-unescaped. |
| `away_team_he` | String | Away team name in Hebrew, HTML-unescaped. |
| `home_logo_url` | String | Absolute URL of the home team logo. |
| `away_logo_url` | String | Absolute URL of the away team logo. |
| `home_score` | Int64 | Home final score; 0 before the game is played. |
| `away_score` | Int64 | Away final score; 0 before the game is played. |
| `board_id` | Int64 | Competition (board) id: 5 league, 10 cup, 16 / 26 / 17 playoff quarterfinals / semifinals / final, 33 play-in, 34 Supercup, 11 the 2004-05 semifinal group; 1001+ external competitions (1001 EuroLeague, 1003 Champions League, 1005 pre-season, 1008 EuroBasket, 1012 EuroCup, 1013 State Cup). |
| `board_name_he` | String | Competition name in Hebrew, HTML-unescaped. |
| `round` | Int64 | Round (game number) within the board; null for external games. |
| `tip_off` | String | Tip-off 'HH:MM', local Israel time, as text; null in 2004-05. It can disagree with Segev (game 26646: 20:15 here, 19:30 in Segev). |
| `venue_he` | String | Venue in Hebrew, HTML-unescaped; null in 2004-05. |

**`return_parsed=False`** — the raw body (`dict`): the ASMX body `{"<key>": [row, ...]}` (every value a string) plus `_request` (the op and the wire parameters sent).

### Raises {#ibba_games_ex-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (HTTP 404, an empty body for an unknown id, an empty list). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a non-200, an error or changed envelope, a row for another season, team, player, board or game, a box score of an unfinished game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#ibba_games_ex-example}

```python
ibba_games_ex(season=2026, team_uid=10)
```

## ibba_games_by_team

Every game of a club in a season, domestic and external, in one frame.

**Endpoint URL:** `GET https://basket.co.il/ws/ws.asmx/GamesByTeam`

**Valid URL:** [https://basket.co.il/ws/ws.asmx/GamesByTeam?team_uid=10&cYear=2026](https://basket.co.il/ws/ws.asmx/GamesByTeam?team_uid=10&cYear=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_uid` | `team_uid` |  | `Y` |  | Required. IBBA club id, stable across seasons (Maccabi Tel Aviv = 10; ``team_uid`` in ibba_teams). Use 0 for all clubs only where the route says so. |
| `cYear` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26); sent to the host as ``cYear`` unchanged. |

### Returns {#ibba_games_by_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | IBBA game id (key space: basket.co.il games). Joins ibba_games to ibba_box_score; in ibba_games_ex it is an EXTERNAL game id (the ig_ space, e.g. 8888 for a pre-season game). |
| `game_date` | String | Local Israel date of the game. |
| `season` | Int64 | Season END year as an integer (2026 = 2025-26): the host's own cYear, no mapping. |
| `home_team_id` | Int64 | Home team-season id (team1 on the wire; verified home against Segev). On external boards (board_id >= 1001) an external-team id (the ig_team_ space), not a team-season. |
| `away_team_id` | Int64 | Away team-season id (team2 on the wire). On external boards an external-team id. |
| `home_team_he` | String | Home team name in Hebrew, HTML-unescaped. |
| `away_team_he` | String | Away team name in Hebrew, HTML-unescaped. |
| `home_team` | String | Home team name in English; null for external games, which carry Hebrew names only. |
| `away_team` | String | Away team name in English; null for external games. |
| `home_logo_url` | String | Absolute URL of the home team logo. |
| `away_logo_url` | String | Absolute URL of the away team logo. |
| `home_score` | Int64 | Home final score; 0 before the game is played. |
| `away_score` | Int64 | Away final score; 0 before the game is played. |
| `board_id` | Int64 | Competition (board) id: 5 league, 10 cup, 16 / 26 / 17 playoff quarterfinals / semifinals / final, 33 play-in, 34 Supercup, 11 the 2004-05 semifinal group; 1001+ external competitions (1001 EuroLeague, 1003 Champions League, 1005 pre-season, 1008 EuroBasket, 1012 EuroCup, 1013 State Cup). |
| `board_name_he` | String | Competition name in Hebrew, HTML-unescaped. |
| `board_name` | String | Competition name in English ('Winner League'); null for external boards. |
| `round` | Int64 | Round (game number) within the board; null for external games. |
| `tip_off` | String | Tip-off 'HH:MM', local Israel time, as text; null in 2004-05. It can disagree with Segev (game 26646: 20:15 here, 19:30 in Segev). |
| `venue_he` | String | Venue in Hebrew, HTML-unescaped; null in 2004-05. |
| `venue` | String | Venue in English, HTML-unescaped; null in 2004-05. |
| `referees_he` | String | Referees' names in Hebrew, comma-separated; ', , ' when unknown. |
| `attendance` | Int64 | Attendance; 0 when unknown. |
| `overtime` | Boolean | True when the game went to overtime. |
| `overtimes` | Int64 | Number of overtime periods played. |
| `home_score_by_period` | String | Home points per period, pipe-separated ('18\|23\|30\|21'); null for external games. |
| `away_score_by_period` | String | Away points per period, pipe-separated ('29\|26\|27\|22'); null for external games. |
| `tickets_url` | String | Ticketing URL; null when none. |
| `external_id` | String | The wire's ExternalID: the Segev game id (a string) for league games from 2025-26, a news URL for pre-season games, null before 2025-26. |
| `segev_game_id` | Int64 | external_id read as an integer Segev game id (digits only), else null; joins ibba_segev_* game_id. Derived by the parser. |
| `is_external` | Boolean | True when board_id >= 1001 (an external competition, whose team ids are not team-seasons). Derived by the parser. |

**`return_parsed=False`** — the raw body (`dict`): the ASMX body `{"<key>": [row, ...]}` (every value a string) plus `_request` (the op and the wire parameters sent).

### Raises {#ibba_games_by_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (HTTP 404, an empty body for an unknown id, an empty list). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a non-200, an error or changed envelope, a row for another season, team, player, board or game, a box score of an unfinished game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#ibba_games_by_team-example}

```python
ibba_games_by_team(season=2026, team_uid=10)
```
