# ESPN Fantasy — ESPN Fantasy API (lm-api-reads.fantasy.espn.com/apis/v3; public leagues, no cookies)

> ESPN Fantasy — ESPN Fantasy API (lm-api-reads.fantasy.espn.com/apis/v3; public leagues, no cookies) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.providers.espn_fantasy` — 16 endpoints.

> **Note:** Public data only. No cookie is ever sent, so these wrappers read every game's league-agnostic routes and the PUBLIC leagues ESPN lists in espn_fantasy_league_directory. A private league answers HTTP 401: reading one needs its owner's espn_s2 / SWID cookies, which this package does not take.

> **Note:** Season ids are ESPN's: the start year for football (2026 = fall 2026), the end year for basketball and hockey (2026-27 = 2027). Ids are Int64; team_id is unique only inside one league. No member or owner account id is returned.

## espn_fantasy_default_scoring

Scoring items of one of ESPN's default league-settings templates (points per stat).

**Endpoint URL:** `GET https://lm-api-reads.fantasy.espn.com/apis/v3/games/{game}/seasons/{season}/segments/0/leaguedefaults/{league_default_id}`

**Valid URL:** [https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/2026/segments/0/leaguedefaults/1?view=mSettings](https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/2026/segments/0/leaguedefaults/1?view=mSettings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game` | `game` |  | `Y` |  | Fantasy game: ffl (football), fba (men's basketball), flb (baseball), fhl (hockey), wfba (women's basketball). |
| `season` | `season` |  | `Y` |  | ESPN's season id: the START year for ffl (2026 = the fall 2026 season), the END year for fba / fhl (2026-27 = 2027); flb and wfba fit in one year. espn_fantasy_games gives each game's current one. |
| `league_default_id` | `league_default_id` |  | `Y` |  | Default-settings template id (league_default_id in espn_fantasy_league_directory). |

### Returns {#espn_fantasy_default_scoring-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | Int64 | Default-settings template id (league_default_id), since a template is not a league. |
| `season_id` | Int64 | Season the template applies to. ESPN's season id: the start year for ffl (2026 = fall 2026), the end year for fba / fhl (2026-27 = 2027). |
| `game_id` | Int64 | ESPN's number for the fantasy game (1 football, 3 men's basketball, ...). |
| `scoring_type` | String | Scoring format of the template (e.g. H2H_POINTS, H2H_CATEGORY, ROTO); points only matter in points formats. |
| `stat_id` | Int64 | Stat being scored (per-game enum, the keys of the stats maps). |
| `points` | Float64 | Fantasy points per unit of the stat (negative for a penalty) in points formats; 1.0 on every item in ROTO. |
| `is_reverse_item` | Boolean | True when a lower value is better (categories leagues, e.g. turnovers). |
| `points_overrides` | String | JSON map of lineup-slot code to the points that slot earns instead (empty = none). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_fantasy_default_scoring-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. ESPN answered 404 (unknown game, season or id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for a private league (HTTP 401). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#espn_fantasy_default_scoring-example}

```python
espn_fantasy_default_scoring(game='ffl', league_default_id=1, season=2026)
```

## espn_fantasy_games

Every ESPN fantasy game (ffl, fba, flb, fhl, wfba) with its current season and scoring period.

**Endpoint URL:** `GET https://lm-api-reads.fantasy.espn.com/apis/v3/games`

**Valid URL:** [https://lm-api-reads.fantasy.espn.com/apis/v3/games](https://lm-api-reads.fantasy.espn.com/apis/v3/games)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_fantasy_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN's number for the fantasy game (1 football, 2 baseball, 3 men's basketball, 4 hockey, 5 women's basketball). |
| `game_abbrev` | String | Game abbreviation as ESPN writes it (FFL, FLB, FBA, FHL, WFBA); the lower-case form is the game argument. |
| `game_name` | String | Display name of the fantasy game (e.g. "Fantasy Football"). |
| `pro_sport_abbrev` | String | Abbreviation of the professional league the game is played on (NFL, MLB, NBA, NHL, WNBA). |
| `pro_sport_name` | String | Name of the professional sport the game follows (e.g. "football"). |
| `active` | Boolean | True while ESPN runs the game. |
| `display` | Boolean | True when ESPN shows the game in its fantasy front end. |
| `current_season_id` | Int64 | The game's current season. ESPN's season id: the start year for ffl (2026 = fall 2026), the end year for fba / fhl (2026-27 = 2027). |
| `current_scoring_period_id` | Int64 | Current scoring period of that season (NFL week for football, season day for the others). |
| `season_start_date` | Datetime | Start of ESPN's rollover window for the current season (UTC); wider than the pro schedule. |
| `season_end_date` | Datetime | End of ESPN's rollover window for the current season (UTC); wider than the pro schedule. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_fantasy_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. ESPN answered 404 (unknown game, season or id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for a private league (HTTP 401). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#espn_fantasy_games-example}

```python
espn_fantasy_games()
```

## espn_fantasy_league_boxscores

Box scores of a public league for one scoring period: each player's fantasy points by matchup.

**Endpoint URL:** `GET https://lm-api-reads.fantasy.espn.com/apis/v3/games/{game}/seasons/{season}/segments/0/leagues/{league_id}`

**Valid URL:** [https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027/segments/0/leagues/1535462463?view=mBoxscore&scoringPeriodId=37](https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027/segments/0/leagues/1535462463?view=mBoxscore&scoringPeriodId=37)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game` | `game` |  | `Y` |  | Fantasy game: ffl (football), fba (men's basketball), flb (baseball), fhl (hockey), wfba (women's basketball). |
| `season` | `season` |  | `Y` |  | ESPN's season id: the START year for ffl (2026 = the fall 2026 season), the END year for fba / fhl (2026-27 = 2027); flb and wfba fit in one year. espn_fantasy_games gives each game's current one. |
| `league_id` | `league_id` |  | `Y` |  | Public league id (league_id in espn_fantasy_league_directory). A private league answers HTTP 401 without its owner's cookies and is out of scope. |
| `scoringPeriodId` | `scoring_period_id` |  |  | `Y` | Scoring period: the NFL week for ffl, one day of the season for the other games. Omitted = the league's current period. |

### Returns {#espn_fantasy_league_boxscores-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | Int64 | Public league id (league_id in espn_fantasy_league_directory). |
| `season_id` | Int64 | Season of the league. ESPN's season id: the start year for ffl (2026 = fall 2026), the end year for fba / fhl (2026-27 = 2027). |
| `scoring_period_id` | Int64 | Scoring period the box score is for (the scoring_period_id requested). |
| `matchup_id` | Int64 | Matchup number inside the league's season schedule. |
| `matchup_period_id` | Int64 | Head-to-head period the matchup belongs to. |
| `side` | String | Which side of the matchup the team is on (home or away). |
| `team_id` | Int64 | Team number inside the league. |
| `player_id` | Int64 | ESPN athlete id of the player in the lineup. |
| `full_name` | String | Player's full display name. |
| `pro_team_id` | Int64 | Pro-team number of the player's team; see espn_fantasy_pro_teams. |
| `default_position_id` | Int64 | ESPN's position code for the player in this game (per-game enum). |
| `lineup_slot_id` | Int64 | Lineup slot the player filled in that scoring period (per-game enum; bench included). |
| `applied_stat_total` | Float64 | Fantasy points the player scored in the scoring period under the league's scoring. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_fantasy_league_boxscores-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. ESPN answered 404 (unknown game, season or id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for a private league (HTTP 401). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#espn_fantasy_league_boxscores-example}

```python
espn_fantasy_league_boxscores(game='fba', league_id=1535462463, scoring_period_id=37, season=2027)
```

## espn_fantasy_league_directory

ESPN's public-league directory: the public leagues anyone can join, readable by league id.

**Endpoint URL:** `GET https://lm-api-reads.fantasy.espn.com/apis/v3/games/{game}/seasons/{season}/leaguedirectory/{league_sub_type}`

**Valid URL:** [https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027/leaguedirectory/DRAFT_LOBBY](https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027/leaguedirectory/DRAFT_LOBBY)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game` | `game` |  | `Y` |  | Fantasy game: ffl (football), fba (men's basketball), flb (baseball), fhl (hockey), wfba (women's basketball). |
| `season` | `season` |  | `Y` |  | ESPN's season id: the START year for ffl (2026 = the fall 2026 season), the END year for fba / fhl (2026-27 = 2027); flb and wfba fit in one year. espn_fantasy_games gives each game's current one. |
| `league_sub_type` | `league_sub_type` |  |  | `Y` | DRAFT_LOBBY: ESPN-run public leagues with an upcoming or running draft. MOCKDRAFT_LOBBY: mock-draft rooms. |

### Returns {#espn_fantasy_league_directory-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | Int64 | Public league id, readable by the espn_fantasy_league_* wrappers. |
| `league_name` | String | League name ESPN gave the public league (e.g. "Minnesota Pro H2H Points League"). |
| `league_sub_type` | String | Directory the league was listed in (DRAFT_LOBBY or MOCKDRAFT_LOBBY). |
| `league_default_id` | Int64 | Default-settings template the league uses; pass it to espn_fantasy_default_scoring. |
| `league_size` | Int64 | Number of teams the league holds. |
| `teams_joined` | Int64 | Number of teams already claimed. |
| `full` | Boolean | True when every team is claimed. |
| `public` | Boolean | True for a public league anyone can join. |
| `standard_league` | Boolean | True when the league runs ESPN's standard settings for its template. |
| `customizable` | Boolean | True when the league manager may change the settings. |
| `draft_type` | String | Draft format (SNAKE or AUCTION). |
| `draft_date` | Datetime | Scheduled draft start (UTC). A passed time does not mean the draft has finished. |
| `draft_available_date` | Datetime | When the draft room opens (UTC). |
| `draft_in_progress` | Boolean | Flag ESPN sets on the directory entry; it can stay True after the draft ends. |
| `scoring_type` | String | Scoring format (H2H_POINTS, H2H_CATEGORY, H2H_MOST_CATEGORIES, ROTO, TOTAL_SEASON_POINTS, WEEKLY_ALL_PLAY_POINTS). |
| `experience_type` | String | Experience level ESPN labels the league with (BEGINNER, PRO or ANY). |
| `rank_type` | String | Player-rank list the league's draft uses (e.g. STANDARD, PPR). |
| `restriction_type` | String | Joining restriction on the league (NONE for open public leagues). |
| `roster_locktime_type` | String | When lineups lock (FIRSTGAME_SCORINGPERIOD or INDIVIDUAL_GAME). |
| `scoring_item_stat_ids` | String | JSON list of the stat ids the league scores. |
| `universe_ids` | String | JSON list of ESPN's player-universe groupings the league uses; ESPN does not document the values. |
| `season_id` | Int64 | Season of the league. ESPN's season id: the start year for ffl (2026 = fall 2026), the end year for fba / fhl (2026-27 = 2027). |
| `game_id` | Int64 | ESPN's number for the fantasy game (1 football, 3 men's basketball, ...). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_fantasy_league_directory-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. ESPN answered 404 (unknown game, season or id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for a private league (HTTP 401). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#espn_fantasy_league_directory-example}

```python
espn_fantasy_league_directory(game='fba', season=2027)
```

## espn_fantasy_league_draft

The draft of a public league: one row per pick, in pick order.

**Endpoint URL:** `GET https://lm-api-reads.fantasy.espn.com/apis/v3/games/{game}/seasons/{season}/segments/0/leagues/{league_id}`

**Valid URL:** [https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027/segments/0/leagues/1535462463?view=mDraftDetail](https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027/segments/0/leagues/1535462463?view=mDraftDetail)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game` | `game` |  | `Y` |  | Fantasy game: ffl (football), fba (men's basketball), flb (baseball), fhl (hockey), wfba (women's basketball). |
| `season` | `season` |  | `Y` |  | ESPN's season id: the START year for ffl (2026 = the fall 2026 season), the END year for fba / fhl (2026-27 = 2027); flb and wfba fit in one year. espn_fantasy_games gives each game's current one. |
| `league_id` | `league_id` |  | `Y` |  | Public league id (league_id in espn_fantasy_league_directory). A private league answers HTTP 401 without its owner's cookies and is out of scope. |

### Returns {#espn_fantasy_league_draft-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | Int64 | Public league id (league_id in espn_fantasy_league_directory). |
| `season_id` | Int64 | Season of the league. ESPN's season id: the start year for ffl (2026 = fall 2026), the end year for fba / fhl (2026-27 = 2027). |
| `pick_id` | Int64 | Pick record id in ESPN's draft detail (equal to the overall pick number in the captured draft). |
| `overall_pick_number` | Int64 | Overall pick number across all rounds. |
| `round_id` | Int64 | Draft round of the pick (1 = first round). |
| `round_pick_number` | Int64 | Pick number within the round. |
| `team_id` | Int64 | Team number inside the league that made the pick. |
| `player_id` | Int64 | ESPN athlete id of the drafted player. |
| `bid_amount` | Int64 | Winning bid in an auction draft; 0 in a snake draft. |
| `keeper` | Boolean | True when the pick is a kept player. |
| `reserved_for_keeper` | Boolean | True when the pick slot was reserved for a keeper. |
| `auto_draft_type_id` | Int64 | ESPN's code for how the pick was made (by the team, or one of ESPN's auto-draft modes). |
| `lineup_slot_id` | Int64 | Lineup slot the drafted player was placed in (per-game enum). |
| `nominating_team_id` | Int64 | Team that nominated the player in an auction draft; 0 in a snake draft. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_fantasy_league_draft-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. ESPN answered 404 (unknown game, season or id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for a private league (HTTP 401). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#espn_fantasy_league_draft-example}

```python
espn_fantasy_league_draft(game='fba', league_id=1535462463, season=2027)
```

## espn_fantasy_league_matchups

Every head-to-head matchup of a public league's season, with points and winner.

**Endpoint URL:** `GET https://lm-api-reads.fantasy.espn.com/apis/v3/games/{game}/seasons/{season}/segments/0/leagues/{league_id}`

**Valid URL:** [https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027/segments/0/leagues/1535462463?view=mMatchupScore](https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027/segments/0/leagues/1535462463?view=mMatchupScore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game` | `game` |  | `Y` |  | Fantasy game: ffl (football), fba (men's basketball), flb (baseball), fhl (hockey), wfba (women's basketball). |
| `season` | `season` |  | `Y` |  | ESPN's season id: the START year for ffl (2026 = the fall 2026 season), the END year for fba / fhl (2026-27 = 2027); flb and wfba fit in one year. espn_fantasy_games gives each game's current one. |
| `league_id` | `league_id` |  | `Y` |  | Public league id (league_id in espn_fantasy_league_directory). A private league answers HTTP 401 without its owner's cookies and is out of scope. |

### Returns {#espn_fantasy_league_matchups-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | Int64 | Public league id (league_id in espn_fantasy_league_directory). |
| `season_id` | Int64 | Season of the league. ESPN's season id: the start year for ffl (2026 = fall 2026), the end year for fba / fhl (2026-27 = 2027). |
| `matchup_id` | Int64 | Matchup number inside the league's season schedule. |
| `matchup_period_id` | Int64 | Head-to-head period of the matchup (a week; it spans one or more scoring periods). |
| `playoff_tier_type` | String | Playoff bracket the matchup belongs to (NONE for the regular season). |
| `winner` | String | Result (HOME, AWAY, TIE, or UNDECIDED for an unplayed or running matchup). |
| `home_team_id` | Int64 | Team number of the home side; unique only within one league. |
| `away_team_id` | Int64 | Team number of the away side (unique only within one league); null for a bye. |
| `home_total_points` | Float64 | Fantasy points of the home side (0 before the matchup is played). |
| `away_total_points` | Float64 | Fantasy points of the away side (0 before the matchup is played). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_fantasy_league_matchups-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. ESPN answered 404 (unknown game, season or id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for a private league (HTTP 401). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#espn_fantasy_league_matchups-example}

```python
espn_fantasy_league_matchups(game='fba', league_id=1535462463, season=2027)
```

## espn_fantasy_league_rosters

The rosters of a public league: one row per rostered player, with lineup slot and acquisition.

**Endpoint URL:** `GET https://lm-api-reads.fantasy.espn.com/apis/v3/games/{game}/seasons/{season}/segments/0/leagues/{league_id}`

**Valid URL:** [https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027/segments/0/leagues/1535462463?view=mRoster](https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027/segments/0/leagues/1535462463?view=mRoster)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game` | `game` |  | `Y` |  | Fantasy game: ffl (football), fba (men's basketball), flb (baseball), fhl (hockey), wfba (women's basketball). |
| `season` | `season` |  | `Y` |  | ESPN's season id: the START year for ffl (2026 = the fall 2026 season), the END year for fba / fhl (2026-27 = 2027); flb and wfba fit in one year. espn_fantasy_games gives each game's current one. |
| `league_id` | `league_id` |  | `Y` |  | Public league id (league_id in espn_fantasy_league_directory). A private league answers HTTP 401 without its owner's cookies and is out of scope. |

### Returns {#espn_fantasy_league_rosters-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | Int64 | Public league id (league_id in espn_fantasy_league_directory). |
| `season_id` | Int64 | Season of the league. ESPN's season id: the start year for ffl (2026 = fall 2026), the end year for fba / fhl (2026-27 = 2027). |
| `team_id` | Int64 | Team number inside the league that rosters the player. |
| `player_id` | Int64 | ESPN athlete id of the rostered player. |
| `full_name` | String | Player's full display name. |
| `pro_team_id` | Int64 | Pro-team number of the player's team (0 = free agent); see espn_fantasy_pro_teams. |
| `default_position_id` | Int64 | ESPN's position code for the player in this game (per-game enum). |
| `lineup_slot_id` | Int64 | Lineup slot the player fills on the team now (per-game enum; bench and IR included). |
| `acquisition_type` | String | How the team got the player (e.g. DRAFT, ADD). |
| `acquisition_date` | Datetime | When the team got the player (UTC). |
| `injury_status` | String | Injury status on the roster entry (NORMAL when healthy); the pool table's injury_status uses other values. |
| `player_status` | String | Player's pool status in the league (ONTEAM for every rostered player). |
| `applied_stat_total` | Float64 | Fantasy points of the stats entry ESPN attaches to the roster entry: one recent scoring period, not the season. |
| `percent_owned` | Float64 | Share of ESPN leagues (percent, 0-100) in which the player is rostered. |
| `percent_started` | Float64 | Share of ESPN leagues (percent, 0-100) in which the player is in a starting slot. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_fantasy_league_rosters-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. ESPN answered 404 (unknown game, season or id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for a private league (HTTP 401). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#espn_fantasy_league_rosters-example}

```python
espn_fantasy_league_rosters(game='fba', league_id=1535462463, season=2027)
```

## espn_fantasy_league_scoring

Scoring items of a public league (points per stat).

**Endpoint URL:** `GET https://lm-api-reads.fantasy.espn.com/apis/v3/games/{game}/seasons/{season}/segments/0/leagues/{league_id}`

**Valid URL:** [https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027/segments/0/leagues/1535462463?view=mSettings](https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027/segments/0/leagues/1535462463?view=mSettings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game` | `game` |  | `Y` |  | Fantasy game: ffl (football), fba (men's basketball), flb (baseball), fhl (hockey), wfba (women's basketball). |
| `season` | `season` |  | `Y` |  | ESPN's season id: the START year for ffl (2026 = the fall 2026 season), the END year for fba / fhl (2026-27 = 2027); flb and wfba fit in one year. espn_fantasy_games gives each game's current one. |
| `league_id` | `league_id` |  | `Y` |  | Public league id (league_id in espn_fantasy_league_directory). A private league answers HTTP 401 without its owner's cookies and is out of scope. |

### Returns {#espn_fantasy_league_scoring-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | Int64 | Public league id the scoring settings belong to. |
| `season_id` | Int64 | Season of the league. ESPN's season id: the start year for ffl (2026 = fall 2026), the end year for fba / fhl (2026-27 = 2027). |
| `game_id` | Int64 | ESPN's number for the fantasy game (1 football, 3 men's basketball, ...). |
| `scoring_type` | String | Scoring format of the league (e.g. H2H_POINTS, H2H_CATEGORY, ROTO); points only matter in points formats. |
| `stat_id` | Int64 | Stat being scored (per-game enum, the keys of the stats maps). |
| `points` | Float64 | Fantasy points per unit of the stat (negative for a penalty) in points formats; 1.0 on every item in ROTO. |
| `is_reverse_item` | Boolean | True when a lower value is better (categories leagues, e.g. turnovers). |
| `points_overrides` | String | JSON map of lineup-slot code to the points that slot earns instead (empty = none). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_fantasy_league_scoring-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. ESPN answered 404 (unknown game, season or id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for a private league (HTTP 401). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#espn_fantasy_league_scoring-example}

```python
espn_fantasy_league_scoring(game='fba', league_id=1535462463, season=2027)
```

## espn_fantasy_league_teams

The teams of a public league: overall record, points, ranks and transaction counters.

**Endpoint URL:** `GET https://lm-api-reads.fantasy.espn.com/apis/v3/games/{game}/seasons/{season}/segments/0/leagues/{league_id}`

**Valid URL:** [https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027/segments/0/leagues/1535462463?view=mTeam](https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027/segments/0/leagues/1535462463?view=mTeam)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game` | `game` |  | `Y` |  | Fantasy game: ffl (football), fba (men's basketball), flb (baseball), fhl (hockey), wfba (women's basketball). |
| `season` | `season` |  | `Y` |  | ESPN's season id: the START year for ffl (2026 = the fall 2026 season), the END year for fba / fhl (2026-27 = 2027); flb and wfba fit in one year. espn_fantasy_games gives each game's current one. |
| `league_id` | `league_id` |  | `Y` |  | Public league id (league_id in espn_fantasy_league_directory). A private league answers HTTP 401 without its owner's cookies and is out of scope. |

### Returns {#espn_fantasy_league_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | Int64 | Public league id (league_id in espn_fantasy_league_directory). |
| `season_id` | Int64 | Season of the league. ESPN's season id: the start year for ffl (2026 = fall 2026), the end year for fba / fhl (2026-27 = 2027). |
| `team_id` | Int64 | Team number inside the league; unique only within one league, so join on (league_id, team_id). |
| `team_name` | String | Team name its owner chose. |
| `team_abbrev` | String | Team abbreviation its owner chose. |
| `division_id` | Int64 | Division the team plays in (0 when the league has one division). |
| `wins` | Int64 | Head-to-head wins this season. |
| `losses` | Int64 | Head-to-head losses this season. |
| `ties` | Int64 | Head-to-head ties this season. |
| `win_percentage` | Float64 | Share of matchups won (0-1). |
| `points_for` | Float64 | Fantasy points the team has scored this season, including the matchup in progress. |
| `points_against` | Float64 | Fantasy points scored against the team this season, including the matchup in progress. |
| `games_back` | Float64 | Games behind the division leader. |
| `streak_type` | String | Current streak kind (WIN, LOSS, TIE or NONE). |
| `streak_length` | Int64 | Length of the current streak, in matchups. |
| `playoff_seed` | Int64 | Current playoff seed (standings rank). |
| `points` | Float64 | Season fantasy-point total ESPN reports for the team (equal to points_for in head-to-head points leagues). |
| `current_projected_rank` | Int64 | ESPN's projected final rank for the team now. |
| `draft_day_projected_rank` | Int64 | ESPN's projected final rank on draft day. |
| `rank_calculated_final` | Int64 | Final rank once the season is over; 0 before then. |
| `waiver_rank` | Int64 | Team's position in the waiver order (1 claims first). |
| `acquisitions` | Int64 | Players the team has added this season. |
| `drops` | Int64 | Players the team has dropped this season. |
| `trades` | Int64 | Trades the team has completed this season. |
| `move_to_active` | Int64 | Moves from a reserve slot to the active lineup this season. |
| `move_to_ir` | Int64 | Moves to the injured-reserve slot this season. |
| `acquisition_budget_spent` | Int64 | Free-agent budget the team has spent (budget leagues; 0 otherwise). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_fantasy_league_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. ESPN answered 404 (unknown game, season or id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for a private league (HTTP 401). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#espn_fantasy_league_teams-example}

```python
espn_fantasy_league_teams(game='fba', league_id=1535462463, season=2027)
```

## espn_fantasy_league_transactions

Recent transactions of a public league (adds, drops, trades, lineup moves), one row per item.

**Endpoint URL:** `GET https://lm-api-reads.fantasy.espn.com/apis/v3/games/{game}/seasons/{season}/segments/0/leagues/{league_id}`

**Valid URL:** [https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027/segments/0/leagues/1535462463?view=mTransactions2](https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027/segments/0/leagues/1535462463?view=mTransactions2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game` | `game` |  | `Y` |  | Fantasy game: ffl (football), fba (men's basketball), flb (baseball), fhl (hockey), wfba (women's basketball). |
| `season` | `season` |  | `Y` |  | ESPN's season id: the START year for ffl (2026 = the fall 2026 season), the END year for fba / fhl (2026-27 = 2027); flb and wfba fit in one year. espn_fantasy_games gives each game's current one. |
| `league_id` | `league_id` |  | `Y` |  | Public league id (league_id in espn_fantasy_league_directory). A private league answers HTTP 401 without its owner's cookies and is out of scope. |
| `scoringPeriodId` | `scoring_period_id` |  |  | `Y` | Scoring period. Honoured for past periods; a recent or omitted period returns the league's current activity (pending and future moves). Not a complete ledger. |

### Returns {#espn_fantasy_league_transactions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | Int64 | Public league id (league_id in espn_fantasy_league_directory). |
| `season_id` | Int64 | Season of the league. ESPN's season id: the start year for ffl (2026 = fall 2026), the end year for fba / fhl (2026-27 = 2027). |
| `transaction_id` | String | UUID of the transaction; every item row of one transaction shares it. |
| `transaction_type` | String | Kind of transaction (e.g. FREEAGENT, ROSTER, FUTURE_ROSTER). |
| `status` | String | Processing state of the transaction (e.g. EXECUTED). |
| `execution_type` | String | How ESPN processes the transaction (EXECUTE on every one seen). |
| `scoring_period_id` | Int64 | Scoring period the transaction takes effect in. |
| `proposed_date` | Datetime | When the transaction was proposed (UTC). |
| `team_id` | Int64 | Team number inside the league that made the transaction. |
| `bid_amount` | Int64 | Free-agent budget bid (budget waivers); 0 otherwise. |
| `is_pending` | Boolean | True while the transaction waits to process. |
| `item_type` | String | What the item does (e.g. ADD, DROP, LINEUP); null for a transaction without items. |
| `player_id` | Int64 | ESPN athlete id the item moves. |
| `from_team_id` | Int64 | Team number the player leaves; 0 = none (the free-agent pool on an ADD, not applicable on LINEUP). |
| `to_team_id` | Int64 | Team number the player joins; 0 = none (the free-agent pool on a DROP, not applicable on LINEUP). |
| `from_lineup_slot_id` | Int64 | Lineup slot the player leaves (-1 = none: the player was not on the roster). |
| `to_lineup_slot_id` | Int64 | Lineup slot the player moves to (-1 = none: the player leaves the roster). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_fantasy_league_transactions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. ESPN answered 404 (unknown game, season or id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for a private league (HTTP 401). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#espn_fantasy_league_transactions-example}

```python
espn_fantasy_league_transactions(game='fba', league_id=1535462463, season=2027)
```

## espn_fantasy_player_news

Fantasy news items for one player (site.api.espn.com).

**Endpoint URL:** `GET https://site.api.espn.com/apis/fantasy/v3/games/{game}/news/players`

**Valid URL:** [https://site.api.espn.com/apis/fantasy/v3/games/fba/news/players?playerId=5104157](https://site.api.espn.com/apis/fantasy/v3/games/fba/news/players?playerId=5104157)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game` | `game` |  | `Y` |  | Fantasy game: ffl (football), fba (men's basketball), flb (baseball), fhl (hockey), wfba (women's basketball). |
| `playerId` | `player_id` |  | `Y` |  | ESPN athlete id (player_id in espn_fantasy_players). |

### Returns {#espn_fantasy_player_news-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `news_id` | Int64 | ESPN's id for the news item; the number after the dash in now_id. |
| `now_id` | String | ESPN content id of the item as text (source prefix, then the item id, e.g. "23-54107746"). |
| `player_id` | Int64 | ESPN athlete id the news item is about. |
| `type` | String | Source or kind of the item (e.g. "Rotowire" for a third-party blurb, "Story" for ESPN's own). |
| `headline` | String | Headline of the news item. |
| `description` | String | Short summary of the news item. |
| `story` | String | Body text of the news item; null for a Media (video) item. |
| `published` | Datetime | When the item was published (UTC). |
| `last_modified` | Datetime | When the item was last edited (UTC). |
| `premium` | Boolean | True for an ESPN+ item. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_fantasy_player_news-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. ESPN answered 404 (unknown game, season or id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for a private league (HTTP 401). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#espn_fantasy_player_news-example}

```python
espn_fantasy_player_news(game='fba', player_id=5104157)
```

## espn_fantasy_player_pool

A public league's player pool: free agents, waivers and rostered players with ownership.

**Endpoint URL:** `GET https://lm-api-reads.fantasy.espn.com/apis/v3/games/{game}/seasons/{season}/segments/0/leagues/{league_id}`

**Valid URL:** [https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027/segments/0/leagues/1535462463?view=kona_player_info](https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027/segments/0/leagues/1535462463?view=kona_player_info)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game` | `game` |  | `Y` |  | Fantasy game: ffl (football), fba (men's basketball), flb (baseball), fhl (hockey), wfba (women's basketball). |
| `season` | `season` |  | `Y` |  | ESPN's season id: the START year for ffl (2026 = the fall 2026 season), the END year for fba / fhl (2026-27 = 2027); flb and wfba fit in one year. espn_fantasy_games gives each game's current one. |
| `league_id` | `league_id` |  | `Y` |  | Public league id (league_id in espn_fantasy_league_directory). A private league answers HTTP 401 without its owner's cookies and is out of scope. |
| `X-Fantasy-Filter` | `fantasy_filter` |  |  | `Y` | JSON object sent as the X-Fantasy-Filter request header. The default returns the 50 most-owned players; add "filterStatus":{"value":["FREEAGENT"]} inside players for free agents only. A limit needs a sort, or the host answers 400. |

### Returns {#espn_fantasy_player_pool-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | ESPN athlete id, shared with ESPN's site and core APIs. |
| `full_name` | String | Player's full display name. |
| `pro_team_id` | Int64 | Pro-team number of the player's team (0 = free agent); see espn_fantasy_pro_teams. |
| `default_position_id` | Int64 | ESPN's position code for the player in this game (per-game enum). |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `eligible_slots` | String | JSON list of the lineup-slot codes the player may fill (per-game enum). |
| `jersey` | String | Player's jersey number as text. |
| `injured` | Boolean | True when the player carries an injury designation. |
| `injury_status` | String | Player's injury designation (e.g. ACTIVE, OUT); the roster table's injury_status uses other values. |
| `status` | String | Player's status in this league (FREEAGENT, WAIVERS or ONTEAM). |
| `on_team_id` | Int64 | Team number inside the league that rosters the player; 0 when unrostered. |
| `keeper_value` | Int64 | Keeper cost of the player in this league; 0 outside keeper leagues. |
| `draft_auction_value` | Int64 | Auction value ESPN assigns the player in this league; 0 outside auction leagues. |
| `percent_owned` | Float64 | Share of ESPN leagues (percent, 0-100) in which the player is rostered. |
| `percent_started` | Float64 | Share of ESPN leagues (percent, 0-100) in which the player is in a starting slot. |
| `percent_change` | Float64 | Recent change in percent_owned, in percentage points. |
| `average_draft_position` | Float64 | Average pick at which the player goes in ESPN drafts. |
| `auction_value_average` | Float64 | Average winning bid for the player in ESPN auction drafts. |
| `last_news_date` | Datetime | Time of the player's latest fantasy news item (UTC); see espn_fantasy_player_news. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_fantasy_player_pool-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. ESPN answered 404 (unknown game, season or id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for a private league (HTTP 401). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#espn_fantasy_player_pool-example}

```python
espn_fantasy_player_pool(game='fba', league_id=1535462463, season=2027)
```

## espn_fantasy_players

The player universe of a fantasy season: ids, names, pro team, position and ownership.

**Endpoint URL:** `GET https://lm-api-reads.fantasy.espn.com/apis/v3/games/{game}/seasons/{season}/players`

**Valid URL:** [https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027/players?view=players_wl](https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027/players?view=players_wl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game` | `game` |  | `Y` |  | Fantasy game: ffl (football), fba (men's basketball), flb (baseball), fhl (hockey), wfba (women's basketball). |
| `season` | `season` |  | `Y` |  | ESPN's season id: the START year for ffl (2026 = the fall 2026 season), the END year for fba / fhl (2026-27 = 2027); flb and wfba fit in one year. espn_fantasy_games gives each game's current one. |
| `X-Fantasy-Filter` | `fantasy_filter` |  |  | `Y` | JSON object sent as the X-Fantasy-Filter request header. The default keeps active players only; pass '{}' for every player. A limit needs a sort, or the host answers 400. |

### Returns {#espn_fantasy_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | ESPN athlete id, shared with ESPN's site and core APIs. |
| `full_name` | String | Player's full display name. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `pro_team_id` | Int64 | Pro-team number of the player's team (0 = free agent); see espn_fantasy_pro_teams. |
| `default_position_id` | Int64 | ESPN's position code for the player in this game (per-game enum). |
| `eligible_slots` | String | JSON list of the lineup-slot codes the player may fill (per-game enum). |
| `droppable` | Boolean | False when ESPN does not let teams drop the player. |
| `percent_owned` | Float64 | Share of ESPN leagues (percent, 0-100) in which the player is rostered. |
| `last_news_date` | Datetime | Time of the player's latest fantasy news item (UTC); see espn_fantasy_player_news. |
| `universe_id` | Int64 | ESPN's player-universe grouping for the player; ESPN does not document its values. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_fantasy_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. ESPN answered 404 (unknown game, season or id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for a private league (HTTP 401). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#espn_fantasy_players-example}

```python
espn_fantasy_players(game='fba', season=2027)
```

## espn_fantasy_pro_schedule

The pro-game schedule of a fantasy season: one row per pro game, keyed to its scoring period.

**Endpoint URL:** `GET https://lm-api-reads.fantasy.espn.com/apis/v3/games/{game}/seasons/{season}`

**Valid URL:** [https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027?view=proTeamSchedules_wl](https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2027?view=proTeamSchedules_wl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game` | `game` |  | `Y` |  | Fantasy game: ffl (football), fba (men's basketball), flb (baseball), fhl (hockey), wfba (women's basketball). |
| `season` | `season` |  | `Y` |  | ESPN's season id: the START year for ffl (2026 = the fall 2026 season), the END year for fba / fhl (2026-27 = 2027); flb and wfba fit in one year. espn_fantasy_games gives each game's current one. |

### Returns {#espn_fantasy_pro_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `pro_game_id` | Int64 | ESPN event id of the pro game (the same id ESPN's scoreboard and summary routes use). |
| `scoring_period_id` | Int64 | Fantasy scoring period the pro game counts toward (NFL week, or season day). |
| `game_date` | Datetime | Scheduled start of the pro game (UTC). |
| `home_pro_team_id` | Int64 | Pro-team number of the home team (pro_team_id in espn_fantasy_pro_teams). |
| `away_pro_team_id` | Int64 | Pro-team number of the away team (pro_team_id in espn_fantasy_pro_teams). |
| `start_time_tbd` | Boolean | True while the pro game's start time is not set. |
| `stats_official` | Boolean | True once ESPN marks the pro game's stats official. |
| `valid_for_locking` | Boolean | ESPN's flag for whether the game's start counts toward lineup locks. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_fantasy_pro_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. ESPN answered 404 (unknown game, season or id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for a private league (HTTP 401). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#espn_fantasy_pro_schedule-example}

```python
espn_fantasy_pro_schedule(game='fba', season=2027)
```

## espn_fantasy_pro_teams

The pro teams of a fantasy season, with bye weeks (football) and the free-agent pool (id 0).

**Endpoint URL:** `GET https://lm-api-reads.fantasy.espn.com/apis/v3/games/{game}/seasons/{season}`

**Valid URL:** [https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/2026?view=proTeamSchedules_wl](https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/2026?view=proTeamSchedules_wl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game` | `game` |  | `Y` |  | Fantasy game: ffl (football), fba (men's basketball), flb (baseball), fhl (hockey), wfba (women's basketball). |
| `season` | `season` |  | `Y` |  | ESPN's season id: the START year for ffl (2026 = the fall 2026 season), the END year for fba / fhl (2026-27 = 2027); flb and wfba fit in one year. espn_fantasy_games gives each game's current one. |

### Returns {#espn_fantasy_pro_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `pro_team_id` | Int64 | ESPN team id of the pro team (the id ESPN's site API uses for that pro league); 0 is the free-agent pool. |
| `pro_team_abbrev` | String | Pro team abbreviation (e.g. "IND"; "FA" for the free-agent pool). |
| `pro_team_location` | String | Pro team city or region (e.g. "Indianapolis"). |
| `pro_team_name` | String | Pro team nickname (e.g. "Colts"). |
| `bye_week` | Int64 | Football bye week (the NFL week the team does not play); 0 in the other games and for the pool. |
| `universe_id` | Int64 | ESPN's player-universe grouping for the team; ESPN does not document its values. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_fantasy_pro_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. ESPN answered 404 (unknown game, season or id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for a private league (HTTP 401). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#espn_fantasy_pro_teams-example}

```python
espn_fantasy_pro_teams(game='ffl', season=2026)
```

## espn_fantasy_seasons

Every season of one fantasy game, newest first.

**Endpoint URL:** `GET https://lm-api-reads.fantasy.espn.com/apis/v3/games/{game}/seasons`

**Valid URL:** [https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons](https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game` | `game` |  | `Y` |  | Fantasy game: ffl (football), fba (men's basketball), flb (baseball), fhl (hockey), wfba (women's basketball). |

### Returns {#espn_fantasy_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN's number for the fantasy game the season belongs to (1 football, 3 men's basketball, ...). |
| `season_id` | Int64 | ESPN's season id: the start year for ffl (2026 = fall 2026), the end year for fba / fhl (2026-27 = 2027). |
| `season_abbrev` | String | Short season label, game plus year (e.g. "FFL 2026"). |
| `season_name` | String | Display name of the season (e.g. "Fantasy Football 2026"). |
| `active` | Boolean | True for the season ESPN currently runs. |
| `display` | Boolean | True when ESPN lists the season in its front end. |
| `current_scoring_period_id` | Int64 | Current (or final) scoring period of the season. |
| `start_date` | Datetime | Start of ESPN's rollover window for the season (UTC); it can open months before the pro season. |
| `end_date` | Datetime | End of ESPN's rollover window for the season (UTC); it can run past the next season's start. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_fantasy_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. ESPN answered 404 (unknown game, season or id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for a private league (HTTP 401). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#espn_fantasy_seasons-example}

```python
espn_fantasy_seasons(game='ffl')
```
