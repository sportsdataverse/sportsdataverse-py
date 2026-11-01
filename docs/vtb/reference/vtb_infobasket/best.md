# VTB — VTB United League Infobasket (Russia) — Best

> VTB — VTB United League Infobasket (Russia) — Best — function reference in sdv-py, the SportsDataverse Python package.

## vtb_infobasket_best_players

Player leaders of a season for the first `count` leader categories.

**Endpoint URL:** `GET https://org.infobasket.su/Comp/GetBestPlayers/{season}`

**Valid URL:** [https://org.infobasket.su/Comp/GetBestPlayers/2026?count=5&lang=en&tag=vtb](https://org.infobasket.su/Comp/GetBestPlayers/2026?count=5&lang=en&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). |
| `count` | `count` |  |  | `Y` | Leader categories to return (the site's widget asks 5). |
| `lang` | `lang` |  |  | `Y` | Label language (en or ru). Most bodies carry both languages whatever is asked; the leader categories' labels follow it. |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_best_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `param_id` | Int64 | Leader-category id (the `param` of the parameter lists). |
| `param_name` | String | Leader category name (ParamName), e.g. 'Points'. |
| `param_short_name` | String | Leader category short label, e.g. 'Points in a game'. |
| `param_label` | String | Leader category label in the requested language (lang). |
| `param_group` | Int64 | Category group; 0 in the samples. |
| `rank` | Int64 | Rank in the leader list. |
| `team_id` | Int64 | Infobasket TeamID (2650 = Lokomotiv Kuban); the v2 teamId. Not a club-record id and not a registration id. |
| `team_short_name_ru` | String | The the team's short name in Russian (as the competition names it). |
| `team_short_name_en` | String | The the team's short name in English (as the competition names it). |
| `team_name_ru` | String | The the team's name in Russian (as the competition names it). |
| `team_name_en` | String | The the team's name in English (as the competition names it). |
| `team_region_name_ru` | String | The the team's region or city in Russian (as the competition names it). |
| `team_region_name_en` | String | The the team's region or city in English (as the competition names it). |
| `person_id` | Int64 | Infobasket person id (PersonID): one person (player, coach or official) across clubs and seasons. Joins to /Widget/PlayerPage/{personId} (players) and /Widget/CoachPage/{personId} (coaches). |
| `person_last_name_ru` | String | Last name, Russian. |
| `person_last_name_en` | String | Last name, English. |
| `person_first_name_ru` | String | First name, Russian. |
| `person_first_name_en` | String | First name, English. |
| `person_full_name_ru` | String | Full name, 'Last First', Russian. |
| `person_full_name_en` | String | Full name, 'Last First', English. |
| `person_gender` | Int64 | Person gender: 1 on every sampled (male) person. |
| `game_id` | Int64 | Infobasket GameID of the game (the v2 matches' match_id); global across seasons. |
| `game_count` | Int64 | Games the value is averaged over. |
| `res` | Float64 | The leader's value (a per-game average for 'in a game' categories). |
| `pre_res` | String | The value as displayed, a string (e.g. '1070' for 10.70 without separator; format unverified). |
| `is_same` | Boolean | True when the row ties the previous one. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_best_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_best_players-example}

```python
vtb_infobasket_best_players(season=2026)
```

## vtb_infobasket_best_teams

Team leaders of a season for the first `count` leader categories.

**Endpoint URL:** `GET https://org.infobasket.su/Comp/GetBestTeams/{season}`

**Valid URL:** [https://org.infobasket.su/Comp/GetBestTeams/2026?count=5&lang=en&tag=vtb](https://org.infobasket.su/Comp/GetBestTeams/2026?count=5&lang=en&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). |
| `count` | `count` |  |  | `Y` | Leader categories to return (the site's widget asks 5). |
| `lang` | `lang` |  |  | `Y` | Label language (en or ru). Most bodies carry both languages whatever is asked; the leader categories' labels follow it. |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_best_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `param_id` | Int64 | Leader-category id (the `param` of the parameter lists). |
| `param_name` | String | Leader category name (ParamName), e.g. 'Points'. |
| `param_short_name` | String | Leader category short label, e.g. 'Points in a game'. |
| `param_label` | String | Leader category label in the requested language (lang). |
| `param_group` | Int64 | Category group; 0 in the samples. |
| `rank` | Int64 | Rank in the leader list. |
| `team_id` | Int64 | Infobasket TeamID (2650 = Lokomotiv Kuban); the v2 teamId. Not a club-record id and not a registration id. |
| `team_short_name_ru` | String | The the team's short name in Russian (as the competition names it). |
| `team_short_name_en` | String | The the team's short name in English (as the competition names it). |
| `team_name_ru` | String | The the team's name in Russian (as the competition names it). |
| `team_name_en` | String | The the team's name in English (as the competition names it). |
| `team_region_name_ru` | String | The the team's region or city in Russian (as the competition names it). |
| `team_region_name_en` | String | The the team's region or city in English (as the competition names it). |
| `game_id` | Int64 | Infobasket GameID of the game (the v2 matches' match_id); global across seasons. |
| `game_count` | Int64 | Games the value is averaged over. |
| `res` | Float64 | The leader's value (a per-game average for 'in a game' categories). |
| `pre_res` | String | The value as displayed, a string (e.g. '1070' for 10.70 without separator; format unverified). |
| `is_same` | Boolean | True when the row ties the previous one. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_best_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_best_teams-example}

```python
vtb_infobasket_best_teams(season=2026)
```

## vtb_infobasket_best_player_parameters

Player leader categories (a code table).

**Endpoint URL:** `GET https://org.infobasket.su/Widget/GetBestPlayerParameters/`

**Valid URL:** [https://org.infobasket.su/Widget/GetBestPlayerParameters/?format=json&lang=en](https://org.infobasket.su/Widget/GetBestPlayerParameters/?format=json&lang=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | Label language (en or ru). Most bodies carry both languages whatever is asked; the leader categories' labels follow it. |

### Returns {#vtb_infobasket_best_player_parameters-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `param` | Int64 | Leader-category id; pass it as `param` to /Widget/BestPlayers or /Widget/BestTeams. |
| `name` | String | Leader-category name, English (double spaces as sent). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_best_player_parameters-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_best_player_parameters-example}

```python
vtb_infobasket_best_player_parameters()
```

## vtb_infobasket_best_team_parameters

Team leader categories (a code table).

**Endpoint URL:** `GET https://org.infobasket.su/Widget/GetBestTeamParameters/`

**Valid URL:** [https://org.infobasket.su/Widget/GetBestTeamParameters/?format=json&lang=en](https://org.infobasket.su/Widget/GetBestTeamParameters/?format=json&lang=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | Label language (en or ru). Most bodies carry both languages whatever is asked; the leader categories' labels follow it. |

### Returns {#vtb_infobasket_best_team_parameters-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `param` | Int64 | Leader-category id; pass it as `param` to /Widget/BestPlayers or /Widget/BestTeams. |
| `name` | String | Leader-category name, English (double spaces as sent). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_best_team_parameters-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_best_team_parameters-example}

```python
vtb_infobasket_best_team_parameters()
```
