# Ligue Magnus — Ligue Magnus (liguemagnus.com admin-ajax JSON + game sheets, France)

> Ligue Magnus — Ligue Magnus (liguemagnus.com admin-ajax JSON + game sheets, France) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.hockey.magnus` — 11 endpoints.

## magnus_competitions

Competitions (one per season) of a competition type.

**Endpoint URL:** `GET https://liguemagnus.com/wp-admin/admin-ajax.php`

**Valid URL:** [https://liguemagnus.com/wp-admin/admin-ajax.php?action=get_competition_by_type&id_type=1](https://liguemagnus.com/wp-admin/admin-ajax.php?action=get_competition_by_type&id_type=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id_type` | `competition_type` |  |  | `Y` | Competition type: 1 = Ligue Magnus, 2 = Division 1. Omitted: success=false 'Aucune donnée disponible' (probed 2026-10-12). |

### Returns {#magnus_competitions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | Ligue Magnus competition id (Int64); one per season, the key get_phases and get_rencontres take (1 = 2020-21 ... 240 = 2026-27). |
| `season` | Int64 | Season by its END year (2027 = 2026-27), the federation's saison field. |
| `competition` | String | Competition name as published (sponsor name included, e.g., "Synerglace Ligue Magnus"). |
| `competition_type_id` | Int64 | Competition type id: 1 = Ligue Magnus, 2 = Division 1. |
| `competition_type_code` | String | Competition type code (NA for the Ligue Magnus). |
| `competition_type` | String | Competition type label (e.g., "Ligue Magnus"). |
| `external_url` | String | External site linked from the competition (streaming partner). |
| `published` | Boolean | Whether the competition is published on the league site. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#magnus_competitions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liguemagnus.com has no data for the request (success=false 'Aucune donnée disponible', an empty data stub, zero rows, a season outside 2020-21 onwards, an unknown or unplayed game). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is success=false with any other message, outside the requested scope, a partial schedule page, or a game page without its embedded game sheet. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#magnus_competitions-example}

```python
magnus_competitions()
```

## magnus_game

Game header from the game sheet: teams, score, start time, status, attendance and rink (one row).

**Endpoint URL:** `GET https://liguemagnus.com/rencontre/{game_id}/`

**Valid URL:** [https://liguemagnus.com/rencontre/77779/](https://liguemagnus.com/rencontre/77779/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Game id (get_rencontres data.data[].id). Example 77779 (2026-27, shootout + MVPs); also sampled: 68838, 865. |

### Returns {#magnus_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id (Int64 join key). |
| `competition` | String | Competition name as published. |
| `phase` | String | Phase name in French. |
| `round` | Int64 | Round (journée) number. |
| `game_datetime` | Datetime | Start time, Europe/Paris. |
| `status` | String | Game state: final, in_progress or scheduled. |
| `home_team_id` | Int64 | Team id of the home team (Int64). |
| `home_team` | String | Display name of the home team. |
| `home_team_abbreviation` | String | Abbreviation of the home team. |
| `away_team_id` | Int64 | Team id of the away team (Int64). |
| `away_team` | String | Display name of the away team. |
| `away_team_abbreviation` | String | Abbreviation of the away team. |
| `home_score` | Int64 | Goals of the home team (the final score, shootout included). |
| `away_score` | Int64 | Goals of the away team (the final score, shootout included). |
| `home_team_participant_id` | Int64 | Team participant id of the home team in this phase (Int64; joins the standings). |
| `away_team_participant_id` | Int64 | Team participant id of the away team in this phase (Int64; joins the standings). |
| `has_shootout` | Boolean | Whether the game went to a shootout. |
| `n_periods` | Int64 | Number of regulation periods. |
| `period_minutes` | Int64 | Length of a regulation period in minutes. |
| `attendance` | Int64 | Number of spectators (attendance). |
| `venue` | String | Name of the rink (arena). |

**`return_parsed=False`** — the game sheet `Dict` extracted from the page's embedded JSON (staff birth dates removed).

### Raises {#magnus_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liguemagnus.com has no data for the request (success=false 'Aucune donnée disponible', an empty data stub, zero rows, a season outside 2020-21 onwards, an unknown or unplayed game). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is success=false with any other message, outside the requested scope, a partial schedule page, or a game page without its embedded game sheet. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#magnus_game-example}

```python
magnus_game(game_id=77779)
```

## magnus_lineups

Game roster from the game sheet: one row per dressed player, with the person id, the per-game participant id, jersey, licence number, position, captaincy and MVP flag.

**Endpoint URL:** `GET https://liguemagnus.com/rencontre/{game_id}/`

**Valid URL:** [https://liguemagnus.com/rencontre/77779/](https://liguemagnus.com/rencontre/77779/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Game id (get_rencontres data.data[].id). Example 77779 (2026-27, shootout + MVPs); also sampled: 68838, 865. |

### Returns {#magnus_lineups-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id (Int64 join key). |
| `team_id` | Int64 | Team id (Int64). |
| `home_away` | String | The team's side: home or away. |
| `player_id` | Int64 | Person id (Int64; joueurs[].id = personne.id), stable across games; the player key of every Magnus frame. |
| `player_participant_id` | Int64 | The player's roster entry in THIS game (Int64; changes every game), the key the sheet's on-ice lists and MVPs use; never join across games on it. |
| `player_name` | String | Player name (SURNAME First). |
| `jersey_number` | String | Jersey number worn in this game (string as published). |
| `registered_jersey_number` | String | Registered jersey number of the person; can differ from the one worn. |
| `licence_number` | String | FFHG licence number (string; keeps leading zeros). |
| `position_code` | String | Position code: A (attaquant), D (défenseur), GB (gardien de but). |
| `position` | String | Position in English: forward, defence or goalie. |
| `captaincy` | String | C (captain) or A (alternate captain); null otherwise. |
| `is_mvp` | Boolean | Whether the player was named his team's MVP of the game. |
| `photo_url` | String | Photo thumbnail URL of the player. |

**`return_parsed=False`** — the game sheet `Dict` extracted from the page's embedded JSON (staff birth dates removed).

### Raises {#magnus_lineups-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liguemagnus.com has no data for the request (success=false 'Aucune donnée disponible', an empty data stub, zero rows, a season outside 2020-21 onwards, an unknown or unplayed game). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is success=false with any other message, outside the requested scope, a partial schedule page, or a game page without its embedded game sheet. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#magnus_lineups-example}

```python
magnus_lineups(game_id=77779)
```

## magnus_pbp

Play-by-play from the game sheet: goals, penalties, shots, faceoffs, goalie changes and timeouts in game order, with English event types and derived periods. No shot coordinates.

**Endpoint URL:** `GET https://liguemagnus.com/rencontre/{game_id}/`

**Valid URL:** [https://liguemagnus.com/rencontre/77779/](https://liguemagnus.com/rencontre/77779/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Game id (get_rencontres data.data[].id). Example 77779 (2026-27, shootout + MVPs); also sampled: 68838, 865. |

### Returns {#magnus_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id (Int64 join key). |
| `event_id` | String | Event id as published, the raw type plus a number (e.g., "BUT99238"); unique within a game. |
| `event_type` | String | Event type in English: goal, penalty, shot, faceoff, goalie_change or timeout. |
| `event_type_raw` | String | Event type as published: BUT, SANCTION, TIR, ENGA, REMPLACANT or TEMPS_MORT. |
| `period` | Int64 | Period derived from the elapsed seconds (4 = overtime). A faceoff at 1200 / 2400 / 3600 opens the next period; any other event at a boundary belongs to the period ending there. |
| `period_seconds` | Int64 | Elapsed seconds within the period. |
| `game_seconds` | Int64 | Elapsed game seconds (the sheet's temps). |
| `team_id` | Int64 | Team id of the event's team (Int64; for a faceoff, the winning team). |
| `team_abbreviation` | String | Abbreviation of the event's team. |
| `player_id` | Int64 | Person id (Int64, stable across games) of the acting player; the scorer, the shooter, the penalised player or the faceoff winner. Null on goalie changes, timeouts and bench penalties. |
| `player_name` | String | Name of the acting player (SURNAME First). |
| `assist1_player_id` | Int64 | Person id of the first assist on a goal (Int64). |
| `assist1_player_name` | String | Name of the first assist on a goal. |
| `assist2_player_id` | Int64 | Person id of the second assist on a goal (Int64). |
| `assist2_player_name` | String | Name of the second assist on a goal. |
| `goalie_player_id` | Int64 | Person id of the goalie the goal or shot was against (Int64). |
| `goalie_player_name` | String | Name of the goalie the goal or shot was against. |
| `faceoff_home_player_id` | Int64 | Person id of the home faceoff taker (Int64). |
| `faceoff_away_player_id` | Int64 | Person id of the away faceoff taker (Int64). |
| `strength_code` | String | Strength when a goal was scored: EG (even strength), SUP1 (power play, one man up); other codes unobserved. |
| `penalty_code` | String | Infraction code (e.g., CIN = slashing). |
| `penalty` | String | Infraction label in French (e.g., "Cinglage (coup de crosse)"). |
| `penalty_minutes` | Int64 | Penalty length in minutes. |
| `penalty_start_seconds` | Int64 | Elapsed game seconds when the penalty starts. |
| `penalty_end_seconds` | Int64 | Elapsed game seconds when the penalty ends. |
| `goalie_in_player_id` | Int64 | Person id of the goalie entering on a goalie change (Int64; never observed populated). |
| `goalie_out_player_id` | Int64 | Person id of the goalie leaving on a goalie change (Int64). |
| `on_ice_plus_player_ids` | List | Person ids of the scoring team's players on ice at a goal, goalie included (list of Int64). |
| `on_ice_minus_player_ids` | List | Person ids of the conceding team's players on ice at a goal (list of Int64). |

**`return_parsed=False`** — the game sheet `Dict` extracted from the page's embedded JSON (staff birth dates removed).

### Raises {#magnus_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liguemagnus.com has no data for the request (success=false 'Aucune donnée disponible', an empty data stub, zero rows, a season outside 2020-21 onwards, an unknown or unplayed game). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is success=false with any other message, outside the requested scope, a partial schedule page, or a game page without its embedded game sheet. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#magnus_pbp-example}

```python
magnus_pbp(game_id=77779)
```

## magnus_phases

Phases of a competition (regular season, playoffs, relegation).

**Endpoint URL:** `GET https://liguemagnus.com/wp-admin/admin-ajax.php`

**Valid URL:** [https://liguemagnus.com/wp-admin/admin-ajax.php?action=get_phases&competition_id=240](https://liguemagnus.com/wp-admin/admin-ajax.php?action=get_phases&competition_id=240)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_id` | `season` |  | `Y` |  | Required. Season by its END year (2027 = 2026-27), sent as the season's competition_id. Coverage starts in 2020-21 (2021); an earlier season, or one the federation does not list, raises NoDataError. |

### Returns {#magnus_phases-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `phase_id` | Int64 | Phase id (Int64), the value the phase_id argument of magnus_teams, magnus_rounds, magnus_round_games, magnus_standings and magnus_schedule takes. Ids change every season. |
| `phase` | String | Phase name in French (e.g., "Saison régulière" = regular season). |
| `phase_type_code` | String | Phase type code: C (championnat, round robin) or T (tableau, playoff bracket). |
| `visible` | Boolean | Whether the phase is shown on the public site. |
| `competition` | String | Name of the competition (season) the phase belongs to. |
| `n_teams` | Int64 | Number of teams in the phase. |
| `overtime` | Boolean | Whether tied games go to overtime in the phase. |
| `games_per_matchup` | Int64 | Games each pair of teams plays in the phase. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#magnus_phases-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liguemagnus.com has no data for the request (success=false 'Aucune donnée disponible', an empty data stub, zero rows, a season outside 2020-21 onwards, an unknown or unplayed game). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is success=false with any other message, outside the requested scope, a partial schedule page, or a game page without its embedded game sheet. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#magnus_phases-example}

```python
magnus_phases(season=2027)
```

## magnus_round_games

One round of a phase with its games.

**Endpoint URL:** `GET https://liguemagnus.com/wp-admin/admin-ajax.php`

**Valid URL:** [https://liguemagnus.com/wp-admin/admin-ajax.php?action=get_rencontrestours&phase_id=714&journee=1](https://liguemagnus.com/wp-admin/admin-ajax.php?action=get_rencontrestours&phase_id=714&journee=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `phase_id` | `phase_id` |  | `Y` |  | Required. Phase id from get_phases. Omitted: data='' (probed 2026-10-12). |
| `journee` | `round` |  | `Y` |  | Required. Round number (tour), 1-based. Omitted: data='' (probed 2026-10-12). |

### Returns {#magnus_round_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id (Int64 join key), the game_id argument of magnus_game, magnus_pbp, magnus_lineups and magnus_shootout. |
| `season` | Int64 | Season by its END year (2027 = 2026-27). |
| `competition_id` | Int64 | Competition (season) id (Int64). |
| `phase_id` | Int64 | Phase id (Int64; regular season, playoffs, relegation). |
| `phase` | String | Phase name in French (e.g., "Saison régulière"). |
| `round` | Int64 | Round (journée) number within the phase. |
| `game_datetime` | Datetime | Scheduled start, Europe/Paris time (get_rencontres publishes local time, a round body UTC; both are converted). |
| `status` | String | Game state: final, in_progress or scheduled. |
| `decision` | String | How a final game was decided: regulation, overtime or shootout; null until the game is final (round bodies carry no decision, so null there too). |
| `home_team_id` | Int64 | Team id of the home team (Int64). |
| `home_team` | String | Display name of the home team. |
| `home_team_abbreviation` | String | Abbreviation of the home team. |
| `away_team_id` | Int64 | Team id of the away team (Int64). |
| `away_team` | String | Display name of the away team. |
| `away_team_abbreviation` | String | Abbreviation of the away team. |
| `home_score` | Int64 | Goals of the home team (shootout winner included in the final score); null until the game starts (unplayed games publish a 0-0 placeholder). |
| `away_score` | Int64 | Goals of the away team; null until the game starts. |
| `home_team_participant_id` | Int64 | Team participant id of the home team in this phase (Int64; its own key space, joins team_participant_id of the standings; not a team id). |
| `away_team_participant_id` | Int64 | Team participant id of the away team in this phase (Int64; joins team_participant_id of the standings). |
| `venue_id` | Int64 | Rink id (Int64). |
| `venue` | String | Name of the rink (arena). |
| `game_label` | String | Game label as published ("HOME / AWAY"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#magnus_round_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liguemagnus.com has no data for the request (success=false 'Aucune donnée disponible', an empty data stub, zero rows, a season outside 2020-21 onwards, an unknown or unplayed game). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is success=false with any other message, outside the requested scope, a partial schedule page, or a game page without its embedded game sheet. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#magnus_round_games-example}

```python
magnus_round_games(phase_id=714, round=1)
```

## magnus_rounds

Rounds (journees) of a phase with their dates (games via get_rencontrestours).

**Endpoint URL:** `GET https://liguemagnus.com/wp-admin/admin-ajax.php`

**Valid URL:** [https://liguemagnus.com/wp-admin/admin-ajax.php?action=get_liste_tours&phase_id=714](https://liguemagnus.com/wp-admin/admin-ajax.php?action=get_liste_tours&phase_id=714)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `phase_id` | `phase_id` |  | `Y` |  | Required. Phase id from get_phases. Omitted: data='' (probed 2026-10-12). |

### Returns {#magnus_rounds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `round_id` | Int64 | Round id (Int64). |
| `round` | Int64 | Round (journée) number within the phase, the round argument of magnus_round_games and magnus_schedule. |
| `round_label` | String | Round label; null in every observed body. |
| `phase_id` | Int64 | Phase id (Int64). |
| `phase` | String | Phase name in French. |
| `competition_id` | Int64 | Competition (season) id (Int64). |
| `season` | Int64 | Season by its END year (2027 = 2026-27). |
| `start_date` | Date | First date of the round. |
| `end_date` | Date | Last date of the round. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#magnus_rounds-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liguemagnus.com has no data for the request (success=false 'Aucune donnée disponible', an empty data stub, zero rows, a season outside 2020-21 onwards, an unknown or unplayed game). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is success=false with any other message, outside the requested scope, a partial schedule page, or a game page without its embedded game sheet. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#magnus_rounds-example}

```python
magnus_rounds(phase_id=714)
```

## magnus_schedule

Games (schedule and results); Laravel-paginated, par_page=700 returns a full season.

**Endpoint URL:** `GET https://liguemagnus.com/wp-admin/admin-ajax.php`

**Valid URL:** [https://liguemagnus.com/wp-admin/admin-ajax.php?action=get_rencontres&par_page=700&competition_id=240](https://liguemagnus.com/wp-admin/admin-ajax.php?action=get_rencontres&par_page=700&competition_id=240)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_id` | `season` |  | `Y` |  | Required. Season by its END year (2027 = 2026-27), sent as the season's competition_id. Coverage starts in 2020-21 (2021); an earlier season, or one the federation does not list, raises NoDataError. |
| `phase_id` | `phase_id` |  |  | `Y` | Phase id. Omitted: every phase of the competition (314 games for 2025-26 vs 264 for its regular season, probed 2026-10-12). |
| `equipe_id` | `team_id` |  |  | `Y` | Team id (get_equipes data[].id): that team's games only (44 for 2025-26, probed 2026-10-12). Non-numeric: a validation error. |
| `date_min` | `start_date` |  |  | `Y` | Earliest game date, YYYY-MM-DD (verified: 2025-10-01..2025-10-31 gave 44 games, probed 2026-10-12). |
| `date_max` | `end_date` |  |  | `Y` | Latest game date, YYYY-MM-DD. |
| `journee` | `round` |  |  | `Y` | Round number (tour): that round's games only (6 per round, probed 2026-10-12). |

### Returns {#magnus_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id (Int64 join key), the game_id argument of magnus_game, magnus_pbp, magnus_lineups and magnus_shootout. |
| `season` | Int64 | Season by its END year (2027 = 2026-27). |
| `competition_id` | Int64 | Competition (season) id (Int64). |
| `phase_id` | Int64 | Phase id (Int64; regular season, playoffs, relegation). |
| `phase` | String | Phase name in French (e.g., "Saison régulière"). |
| `round` | Int64 | Round (journée) number within the phase. |
| `game_datetime` | Datetime | Scheduled start, Europe/Paris time (get_rencontres publishes local time, a round body UTC; both are converted). |
| `status` | String | Game state: final, in_progress or scheduled. |
| `decision` | String | How a final game was decided: regulation, overtime or shootout; null until the game is final (round bodies carry no decision, so null there too). |
| `home_team_id` | Int64 | Team id of the home team (Int64). |
| `home_team` | String | Display name of the home team. |
| `home_team_abbreviation` | String | Abbreviation of the home team. |
| `away_team_id` | Int64 | Team id of the away team (Int64). |
| `away_team` | String | Display name of the away team. |
| `away_team_abbreviation` | String | Abbreviation of the away team. |
| `home_score` | Int64 | Goals of the home team (shootout winner included in the final score); null until the game starts (unplayed games publish a 0-0 placeholder). |
| `away_score` | Int64 | Goals of the away team; null until the game starts. |
| `home_team_participant_id` | Int64 | Team participant id of the home team in this phase (Int64; its own key space, joins team_participant_id of the standings; not a team id). |
| `away_team_participant_id` | Int64 | Team participant id of the away team in this phase (Int64; joins team_participant_id of the standings). |
| `venue_id` | Int64 | Rink id (Int64). |
| `venue` | String | Name of the rink (arena). |
| `game_label` | String | Game label as published ("HOME / AWAY"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#magnus_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liguemagnus.com has no data for the request (success=false 'Aucune donnée disponible', an empty data stub, zero rows, a season outside 2020-21 onwards, an unknown or unplayed game). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is success=false with any other message, outside the requested scope, a partial schedule page, or a game page without its embedded game sheet. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#magnus_schedule-example}

```python
magnus_schedule(season=2027)
```

## magnus_shootout

Shootout attempts from the game sheet: round, shooter, goalie and result (zero rows when the game had no shootout).

**Endpoint URL:** `GET https://liguemagnus.com/rencontre/{game_id}/`

**Valid URL:** [https://liguemagnus.com/rencontre/77779/](https://liguemagnus.com/rencontre/77779/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Game id (get_rencontres data.data[].id). Example 77779 (2026-27, shootout + MVPs); also sampled: 68838, 865. |

### Returns {#magnus_shootout-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id (Int64 join key). |
| `attempt_id` | Int64 | Shootout attempt id (Int64). |
| `shootout_round` | Int64 | Shootout round of the attempt (one attempt per team per round); not a jersey number. |
| `team_id` | Int64 | Team id of the shooting team (Int64). |
| `team_abbreviation` | String | Abbreviation of the shooting team. |
| `shooter_player_id` | Int64 | Person id of the shooter (Int64); null on rounds not needed. |
| `shooter_name` | String | Name of the shooter. |
| `goalie_player_id` | Int64 | Person id of the goalie facing the attempt (Int64). |
| `goalie_name` | String | Name of the goalie facing the attempt. |
| `scored` | Boolean | Whether the attempt scored; null on rounds not needed. |

**`return_parsed=False`** — the game sheet `Dict` extracted from the page's embedded JSON (staff birth dates removed).

### Raises {#magnus_shootout-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liguemagnus.com has no data for the request (success=false 'Aucune donnée disponible', an empty data stub, zero rows, a season outside 2020-21 onwards, an unknown or unplayed game). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is success=false with any other message, outside the requested scope, a partial schedule page, or a game page without its embedded game sheet. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#magnus_shootout-example}

```python
magnus_shootout(game_id=77779)
```

## magnus_standings

Standings of a phase.

**Endpoint URL:** `GET https://liguemagnus.com/wp-admin/admin-ajax.php`

**Valid URL:** [https://liguemagnus.com/wp-admin/admin-ajax.php?action=get_classementsphase&phase_id=714](https://liguemagnus.com/wp-admin/admin-ajax.php?action=get_classementsphase&phase_id=714)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `phase_id` | `phase_id` |  | `Y` |  | Required. Phase id from get_phases. Omitted: data='' (probed 2026-10-12). |

### Returns {#magnus_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank in the standings table. |
| `team_id` | Int64 | Team id (Int64 join key). |
| `team` | String | Short team name as the table publishes it (e.g., "MARSEILLE"). |
| `team_abbreviation` | String | Team abbreviation. |
| `team_participant_id` | Int64 | Team participant id in this phase (Int64; joins home_team_participant_id / away_team_participant_id of the schedule). |
| `games_played` | Int64 | Games played in the table's scope. |
| `wins` | Int64 | Wins in regulation. |
| `ot_wins` | Int64 | Wins in overtime or a shootout. |
| `ot_losses` | Int64 | Losses in overtime or a shootout. |
| `losses` | Int64 | Losses in regulation. |
| `ties` | Int64 | Number of tied games. |
| `points` | Int64 | Points in the standings table. |
| `goals_for` | Int64 | Goals scored (total). |
| `goals_against` | Int64 | Goals allowed (goals against). |
| `penalty_minutes` | Int64 | Penalty minutes. |
| `last_results` | String | Form guide: the last results as W (win), L (loss) or T (tie) letters, in the order the table lists them. |
| `standings_id` | Int64 | Standings table id (Int64). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#magnus_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liguemagnus.com has no data for the request (success=false 'Aucune donnée disponible', an empty data stub, zero rows, a season outside 2020-21 onwards, an unknown or unplayed game). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is success=false with any other message, outside the requested scope, a partial schedule page, or a game page without its embedded game sheet. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#magnus_standings-example}

```python
magnus_standings(phase_id=714)
```

## magnus_teams

Teams of a phase.

**Endpoint URL:** `GET https://liguemagnus.com/wp-admin/admin-ajax.php`

**Valid URL:** [https://liguemagnus.com/wp-admin/admin-ajax.php?action=get_equipes&phase_id=714](https://liguemagnus.com/wp-admin/admin-ajax.php?action=get_equipes&phase_id=714)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `phase_id` | `phase_id` |  | `Y` |  | Required. Phase id from get_phases. Omitted: every team of every competition in the federation database (~680 KB) (probed 2026-10-12). |

### Returns {#magnus_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Team id (Int64 join key), stable across seasons; joins home_team_id / away_team_id of the schedule and team_id of the standings. |
| `team` | String | Team display name (e.g., "Rapaces de Gap"). |
| `team_abbreviation` | String | Team abbreviation (e.g., "GAP"). |
| `team_short_name` | String | Short team label (e.g., "GAP", "ANGLET"). |
| `team_label` | String | Back-office label, "<club code> -  <level>"; use team for display. |
| `gender` | String | Gender of the team: M (men; the only value observed). |
| `logo_url` | String | URL of the team logo image. |
| `club_id` | Int64 | Id of the club (legal entity) behind the team (Int64). |
| `club` | String | Name of the club (legal entity). |
| `club_code` | String | Federation club code (e.g., "93006S"). |
| `competition_ids` | List | Competition ids the team has entered, all seasons and levels (list of Int64). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#magnus_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liguemagnus.com has no data for the request (success=false 'Aucune donnée disponible', an empty data stub, zero rows, a season outside 2020-21 onwards, an unknown or unplayed game). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is success=false with any other message, outside the requested scope, a partial schedule page, or a game page without its embedded game sheet. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#magnus_teams-example}

```python
magnus_teams(phase_id=714)
```
