# GENIUS — Genius Sports LiveStats (fibalivestats.dcd.shared.geniussports.com, 20+ competitions)

> GENIUS — Genius Sports LiveStats (fibalivestats.dcd.shared.geniussports.com, 20+ competitions) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.basketball.genius` — 11 endpoints.

> **Note:** Access (sdv-internal-refs/genius-livestats/README.md): the Genius hosts publish no robots.txt and no terms. CloudFront refuses datacenter IPs on the LiveStats and hosted hosts: from a server set the optional `GENIUS_PROXY` environment variable to an http(s) proxy URL (it is never logged or printed). The wrappers space requests at least one second apart and read only the Genius hosts; keep crawls slow and incremental. Some league sites (the NZ NBL) forbid AI crawlers in their own robots.txt, which does not cover these hosts.

> **Note:** Org and season. Every competition is an ORG code plus a competition id (`comp_id`): `genius_competitions` lists an org's competitions with the sdv END-year `season` (a START-year org lists 2025-26 as season 2026), `genius_match_competition` maps a game to its competition, and `data.json` itself names no date, competition or season. The hosted ORG can differ from the LiveStats ORG of `/u/` (NZNBL vs NZN). Match ids are one global sequence across every Genius customer.

> **Note:** `data.json` carries no global team or player id: `tno` (1 or 2) and `pno` are in-game numbers. Join players across games on name, team code and shirt number; global team ids come from the hosted schedule, the widget or `genius_competition_games`. Only finished games are read.

## genius_game

One row per finished game: period and clock state, both teams' names, codes and scores, officials.

**Endpoint URL:** `GET https://fibalivestats.dcd.shared.geniussports.com/data/{match_id}/data.json`

**Valid URL:** [https://fibalivestats.dcd.shared.geniussports.com/data/2902690/data.json](https://fibalivestats.dcd.shared.geniussports.com/data/2902690/data.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Genius match id (match_id in genius_schedule, genius_widget_games or genius_competition_games), e.g. 2902690. |

### Returns {#genius_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | Genius match id of the game. data.json does not carry it: it is the match_id asked for, stamped by the runtime (the global match sequence shared by every Genius customer). |
| `period` | integer | Current (or last) period number; restarts at 1 in overtime |
| `period_type` | character | Current period type: REGULAR or OVERTIME |
| `in_ot` | integer | 1 when the game is (or ended) in overtime, else 0 |
| `clock` | character | Clock of the current period at capture time, 'mm:ss' remaining ('00:00' once the game is over) |
| `periods_max` | integer | Number of regulation periods (4) |
| `period_length_regular` | integer | Length of a regulation period, minutes (10 under FIBA rules, 12 in some leagues) |
| `period_length_overtime` | integer | Length of an overtime period, minutes |
| `attendance` | integer | Attendance as entered by the org; 0 when not entered |
| `disable_match` | integer | 1 when the org disabled the public feed for this game, else 0 |
| `total_time_added` | integer | Clock time added by corrections, seconds (unverified; 0 in every sample) |
| `team1_name` | character | Full name of team 1, the team listed first (the home team on the hosted schedule and widget). |
| `team1_code` | character | Short code of team 1, in the competition's own script when the org enters one. |
| `team1_score` | integer | Final points of team 1 (cast to an integer; the 2014-15 feed sends a string). |
| `team2_name` | character | Full name of team 2, the team listed second. |
| `team2_code` | character | Short code of team 2, in the competition's own script when the org enters one. |
| `team2_score` | integer | Final points of team 2 (cast to an integer; the 2014-15 feed sends a string). |
| `referee1` | character | First referee's name, from officials.referee1 (international form) or the flat officials_referee1 text. |
| `referee2` | character | Second referee's name, from officials.referee2 (international form) or the flat officials_referee2 text. |
| `referee3` | character | Third referee's name when the feed lists one; null otherwise. |
| `commissioner` | character | Commissioner (game supervisor) name when the feed lists one; null otherwise. |

**`return_parsed=False`** — the body or page record (`dict` / `list`): `data.json` as served plus `_request` (the match id asked for), or the sdv-internal-refs capture shape of a page.

### Raises {#genius_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#genius_game-example}

```python
genius_game(match_id=2902690)
```

_Last validated n/a._

## genius_team_box

Team box score of a finished game: score by period, fouls, timeouts and the team totals.

**Endpoint URL:** `GET https://fibalivestats.dcd.shared.geniussports.com/data/{match_id}/data.json`

**Valid URL:** [https://fibalivestats.dcd.shared.geniussports.com/data/2902690/data.json](https://fibalivestats.dcd.shared.geniussports.com/data/2902690/data.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Genius match id (match_id in genius_schedule, genius_widget_games or genius_competition_games), e.g. 2902690. |

### Returns {#genius_team_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | Genius match id of the game. data.json does not carry it: it is the match_id asked for, stamped by the runtime (the global match sequence shared by every Genius customer). |
| `tno` | integer | Team number within this game: 1 = the team listed first (home on the hosted schedule and widget), 2 = the other, 0 = no team. In-game only: not a team id. |
| `name` | character | Team name as the org enters it |
| `name_international` | character | Team name in Latin script |
| `short_name` | character | Short team name (shortName in the feed), in the competition's own script when the org enters one. |
| `code` | character | Team code, three letters as entered (may be in the local script, e.g. Greek 'ΑΕΚ') |
| `code_international` | character | Team code in Latin script |
| `coach` | character | Head coach display name |
| `score` | integer | Team points (current or final); a numeric string in the 2014-15 era sample (WBBL 12248), an integer since |
| `full_score` | integer | Team points including overtime; equal to score in every sample (a numeric string in the 2014-15 era) |
| `p1_score` | integer | Points scored in period 1 (a numeric string in the 2014-15 era) |
| `p2_score` | integer | Points scored in period 2 (a numeric string in the 2014-15 era) |
| `p3_score` | integer | Points scored in period 3 (a numeric string in the 2014-15 era) |
| `p4_score` | integer | Points scored in period 4 (a numeric string in the 2014-15 era) |
| `ot_score` | integer | Points the team scored in overtime; null without overtime. Whether several overtimes are summed is unverified. |
| `fouls` | integer | Team fouls in the current (last) period |
| `timeouts` | integer | Timeouts counter of the current period (unverified: remaining vs taken); 0 in every sampled (finished) game |
| `minutes` | character | Minutes as the feed prints them, an 'mm:ss' text (can exceed 59 for a team; '199:60' happens); the text of an integer when the feed sends one. |
| `seconds_played` | integer | Seconds played parsed from the printed mm:ss text as minutes * 60 + seconds (so '224:60' gives 13500); null when the feed sent an integer, whose unit is unknown. |
| `field_goals_made` | integer | Team total: field goals made |
| `field_goals_attempted` | integer | Team total: field goals attempted |
| `field_goals_percentage` | numeric | Team total: field-goal percentage, 0-100, rounded to an integer by the feed |
| `three_pointers_made` | integer | Team total: three-pointers made |
| `three_pointers_attempted` | integer | Team total: three-pointers attempted |
| `three_pointers_percentage` | numeric | Team total: three-point percentage, 0-100, rounded to an integer |
| `two_pointers_made` | integer | Team total: two-pointers made |
| `two_pointers_attempted` | integer | Team total: two-pointers attempted |
| `two_pointers_percentage` | numeric | Team total: two-point percentage, 0-100, rounded to an integer |
| `free_throws_made` | integer | Team total: free throws made |
| `free_throws_attempted` | integer | Team total: free throws attempted |
| `free_throws_percentage` | numeric | Team total: free-throw percentage, 0-100, rounded to an integer |
| `rebounds_defensive` | integer | Team total: defensive rebounds |
| `rebounds_offensive` | integer | Team total: offensive rebounds |
| `rebounds_total` | integer | Team total: total rebounds |
| `assists` | integer | Team total: assists |
| `turnovers` | integer | Team total: turnovers |
| `steals` | integer | Team total: steals |
| `blocks` | integer | Team total: blocks made |
| `blocks_received` | integer | Team total: own shots blocked by an opponent |
| `fouls_personal` | integer | Team total: personal fouls committed |
| `fouls_on` | integer | Team total: fouls drawn |
| `points` | integer | Team total: points |
| `points_from_turnovers` | integer | Team total: points off turnovers |
| `points_second_chance` | integer | Team total: second-chance points |
| `points_fast_break` | integer | Team total: fast-break points |
| `points_in_the_paint` | integer | Team total: points in the paint |
| `eff_1` | integer | Genius efficiency formula 1. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site (team total). Absent in the 2014-15 era sample (WBBL 12248), which carries eff_1..eff_6 on players and no team tot_eff_* |
| `eff_2` | integer | Genius efficiency formula 2. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site (team total). Team tot_eff_2 is 2 in every 2016+ sample (a constant, not a stat). Absent in the 2014-15 era sample (WBBL 12248), which carries eff_1..eff_6 on players and no team tot_eff_* |
| `eff_3` | numeric | Genius efficiency formula 3. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site (team total). Absent in the 2014-15 era sample (WBBL 12248), which carries eff_1..eff_6 on players and no team tot_eff_* |
| `eff_4` | numeric | Genius efficiency formula 4. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site (team total). Absent in the 2014-15 era sample (WBBL 12248), which carries eff_1..eff_6 on players and no team tot_eff_* |
| `eff_5` | integer | Genius efficiency formula 5. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site (team total). Absent in the 2014-15 era sample (WBBL 12248), which carries eff_1..eff_6 on players and no team tot_eff_* |
| `eff_6` | integer | Genius efficiency formula 6. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site (team total). Absent in the 2014-15 era sample (WBBL 12248), which carries eff_1..eff_6 on players and no team tot_eff_* |
| `eff_7` | integer | Genius efficiency formula 7. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site (team total). Absent in the 2014-15 era sample (WBBL 12248), which carries eff_1..eff_6 on players and no team tot_eff_* |
| `bench_points` | integer | Team total: points scored by non-starters |
| `biggest_lead` | integer | Team total: biggest lead, points |
| `biggest_scoring_run` | integer | Team total: biggest unanswered scoring run, points |
| `lead_changes` | integer | Team total: lead changes in the game (same value on both teams) |
| `times_scores_level` | integer | Team total: times the score was tied (same value on both teams) |
| `time_leading` | numeric | Team total: minutes the team led, decimal minutes (33.58 = 33 min 35 s) |
| `fouls_team` | integer | Team total: team (bench) fouls |
| `fouls_total` | integer | Team total: all fouls committed (personal + technical + team bench) |
| `rebounds_team` | integer | Team total: team rebounds (credited to no player) |
| `rebounds_team_defensive` | integer | Team total: team defensive rebounds |
| `rebounds_team_offensive` | integer | Team total: team offensive rebounds |
| `turnovers_team` | integer | Team total: team turnovers (credited to no player, e.g. shot clock) |

**`return_parsed=False`** — the body or page record (`dict` / `list`): `data.json` as served plus `_request` (the match id asked for), or the sdv-internal-refs capture shape of a page.

### Raises {#genius_team_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#genius_team_box-example}

```python
genius_team_box(match_id=2902690)
```

_Last validated n/a._

## genius_player_box

Player box score of a finished game: minutes, shooting, rebounds, assists and the efficiency formulas.

**Endpoint URL:** `GET https://fibalivestats.dcd.shared.geniussports.com/data/{match_id}/data.json`

**Valid URL:** [https://fibalivestats.dcd.shared.geniussports.com/data/2902690/data.json](https://fibalivestats.dcd.shared.geniussports.com/data/2902690/data.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Genius match id (match_id in genius_schedule, genius_widget_games or genius_competition_games), e.g. 2902690. |

### Returns {#genius_player_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | Genius match id of the game. data.json does not carry it: it is the match_id asked for, stamped by the runtime (the global match sequence shared by every Genius customer). |
| `tno` | integer | Team number within this game: 1 = the team listed first (home on the hosted schedule and widget), 2 = the other, 0 = no team. In-game only: not a team id. |
| `pno` | integer | Player number within this game, the key of the team's player map; 0 = no player. In-game only: not a person id and not the shirt number. |
| `name` | character | Display name ('T. Nichols') |
| `first_name` | character | Given name in the competition's own script and spelling (Greek, Hebrew, Czech, ... as entered) |
| `family_name` | character | Family name in the competition's own script |
| `international_first_name` | character | Given name in Latin script |
| `international_family_name` | character | Family name in Latin script (often upper case) |
| `scoreboard_name` | character | Short display name ('T. Nichols'), in the local script when the org enters one |
| `shirt_number` | character | Shirt number as a string ('0' and '00' differ) |
| `playing_position` | character | Playing position code (PG, SG, SF, PF, C, G, F, ...); empty string when not entered |
| `starter` | integer | 1 when the player started the game, else 0 |
| `captain` | integer | 1 for the team captain, else 0; absent in some orgs |
| `active` | integer | 1 when the player is on court now (at game end: on court at the final whistle), else 0 |
| `minutes` | character | Minutes as the feed prints them, an 'mm:ss' text (can exceed 59 for a team; '199:60' happens); the text of an integer when the feed sends one. |
| `seconds_played` | integer | Seconds played parsed from the printed mm:ss text as minutes * 60 + seconds (so '224:60' gives 13500); null when the feed sent an integer, whose unit is unknown. |
| `field_goals_made` | integer | Field goals the player made in the game (two- and three-pointers together; a count). |
| `field_goals_attempted` | integer | Field goals the player attempted in the game (two- and three-pointers together; a count). |
| `field_goals_percentage` | numeric | Field-goal percentage, 0-100, rounded to an integer by the feed |
| `three_pointers_made` | integer | Three-pointers made |
| `three_pointers_attempted` | integer | Three-pointers attempted |
| `three_pointers_percentage` | numeric | Three-point percentage, 0-100, rounded to an integer |
| `two_pointers_made` | integer | Two-pointers made |
| `two_pointers_attempted` | integer | Two-pointers attempted |
| `two_pointers_percentage` | numeric | Two-point percentage, 0-100, rounded to an integer |
| `free_throws_made` | integer | Free throws the player made in the game (a count). |
| `free_throws_attempted` | integer | Free throws the player attempted in the game (a count). |
| `free_throws_percentage` | numeric | Free-throw percentage, 0-100, rounded to an integer |
| `rebounds_defensive` | integer | Defensive rebounds |
| `rebounds_offensive` | integer | Offensive rebounds |
| `rebounds_total` | integer | Total rebounds (offensive plus defensive) the player collected in the game. |
| `assists` | integer | Assists credited to the player in the game. |
| `turnovers` | integer | Turnovers committed by the player in the game. |
| `steals` | integer | Steals credited to the player in the game. |
| `blocks` | integer | Shots the player blocked in the game (blocks made, not received). |
| `blocks_received` | integer | Own shots blocked by an opponent |
| `fouls_personal` | integer | Personal fouls committed |
| `fouls_on` | integer | Fouls drawn: personal fouls committed against the player in the game. |
| `points` | integer | Points the player scored in the game, free throws included. |
| `points_from_turnovers` | integer | Points off turnovers |
| `points_second_chance` | integer | Second-chance points |
| `points_fast_break` | integer | Fast-break points |
| `points_in_the_paint` | integer | Points the player scored in the paint in the game (the feed's sPointsInThePaint). |
| `plus_minus_points` | integer | Plus/minus: team point margin while the player was on court |
| `eff_1` | integer | Genius efficiency formula 1. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site |
| `eff_2` | integer | Genius efficiency formula 2. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site |
| `eff_3` | numeric | Genius efficiency formula 3. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site |
| `eff_4` | numeric | Genius efficiency formula 4. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site |
| `eff_5` | integer | Genius efficiency formula 5. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site |
| `eff_6` | integer | Genius efficiency formula 6. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site |
| `eff_7` | integer | Genius efficiency formula 7. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site. Absent in the 2014-15 era sample (WBBL 12248), which carries eff_1..eff_6 on players and no team tot_eff_* |

**`return_parsed=False`** — the body or page record (`dict` / `list`): `data.json` as served plus `_request` (the match id asked for), or the sdv-internal-refs capture shape of a page.

### Raises {#genius_player_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#genius_player_box-example}

```python
genius_player_box(match_id=2902690)
```

_Last validated n/a._

## genius_pbp

Play-by-play of a finished game, newest action first, with the running score and lead.

**Endpoint URL:** `GET https://fibalivestats.dcd.shared.geniussports.com/data/{match_id}/data.json`

**Valid URL:** [https://fibalivestats.dcd.shared.geniussports.com/data/2902690/data.json](https://fibalivestats.dcd.shared.geniussports.com/data/2902690/data.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Genius match id (match_id in genius_schedule, genius_widget_games or genius_competition_games), e.g. 2902690. |

### Returns {#genius_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | Genius match id of the game. data.json does not carry it: it is the match_id asked for, stamped by the runtime (the global match sequence shared by every Genius customer). |
| `row_index` | integer | Position of the action in the feed order, 0 = the newest action by game time (normally the game end row). The feed is not strictly sorted by action_number, so sort on action_number for entry order. |
| `action_number` | integer | Action sequence number, unique within the game (gaps where actions were deleted); a shot joins its play-by-play row on it. Null in the 2014-15 feed. |
| `previous_action` | integer | action_number of the linked earlier action (the shot an assist or rebound belongs to); null when the feed sends an empty string. |
| `period` | integer | Period number; restarts at 1 for the first overtime (read it with period_type). |
| `period_type` | character | Period type: REGULAR or OVERTIME. |
| `gt` | character | Game clock, 'mm:ss' remaining in the period |
| `clock` | character | Game clock with hundredths, 'mm:ss:cc' remaining in the period; absent in older feeds (the 2014-15 WBBL sample has gt only) |
| `tno` | integer | Team number within this game: 1 = the team listed first (home on the hosted schedule and widget), 2 = the other, 0 = no team. In-game only: not a team id. |
| `pno` | integer | Player number within this game, the key of the team's player map; 0 = no player. In-game only: not a person id and not the shirt number. |
| `player` | character | Player display name; '' when no player |
| `shirt_number` | character | Player shirt number (string); '' when no player |
| `first_name` | character | Given name in the competition's own script and spelling (Greek, Hebrew, Czech, ... as entered) |
| `family_name` | character | Family name in the competition's own script |
| `international_first_name` | character | Given name in Latin script |
| `international_family_name` | character | Family name in Latin script (often upper case) |
| `scoreboard_name` | character | Short display name ('T. Nichols'), in the local script when the org enters one |
| `action_type` | character | Action type: 2pt, 3pt, freethrow, rebound, assist, turnover, steal, block, foul, foulon (foul drawn), substitution, timeout, jumpball, period, game, headcoachchallenge |
| `sub_type` | character | Action subtype, free-text code per actionType: shot type for 2pt/3pt (layup, drivinglayup, jumpshot, pullupjumpshot, turnaroundjumpshot, ...), 'in'/'out' for substitutions, 'start'/'end' for period and game, foul kind for fouls; empty string when none |
| `qualifier` | character | Qualifier codes of the action joined with commas (pointsinthepaint, fromturnover, 2ndchance, fastbreak, blocked, ...); null when none. |
| `success` | integer | 1 = made / successful, 0 = missed (shots, free throws); 1 for other actions |
| `scoring` | integer | 1 when the action is a scoring attempt (shot or free throw), else 0 |
| `score_1` | integer | Team 1 score after the action, always an integer (the feed sends a string in most games). |
| `score_2` | integer | Team 2 score after the action, always an integer (the feed sends a string in most games). |
| `lead` | integer | Score margin after the action, team 1 minus team 2 (negative when team 2 leads) |

**`return_parsed=False`** — the body or page record (`dict` / `list`): `data.json` as served plus `_request` (the match id asked for), or the sdv-internal-refs capture shape of a page.

### Raises {#genius_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#genius_pbp-example}

```python
genius_pbp(match_id=2902690)
```

_Last validated n/a._

## genius_shots

Shot chart of a finished game: court x/y (percent of the full court), result and shooter.

**Endpoint URL:** `GET https://fibalivestats.dcd.shared.geniussports.com/data/{match_id}/data.json`

**Valid URL:** [https://fibalivestats.dcd.shared.geniussports.com/data/2902690/data.json](https://fibalivestats.dcd.shared.geniussports.com/data/2902690/data.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Genius match id (match_id in genius_schedule, genius_widget_games or genius_competition_games), e.g. 2902690. |

### Returns {#genius_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | Genius match id of the game. data.json does not carry it: it is the match_id asked for, stamped by the runtime (the global match sequence shared by every Genius customer). |
| `tno` | integer | Team number within this game: 1 = the team listed first (home on the hosted schedule and widget), 2 = the other, 0 = no team. In-game only: not a team id. |
| `pno` | integer | Player number within this game, the key of the team's player map; 0 = no player. In-game only: not a person id and not the shirt number. |
| `action_number` | integer | Action sequence number, unique within the game (gaps where actions were deleted); a shot joins its play-by-play row on it. Null in the 2014-15 feed. |
| `period` | integer | Period number; restarts at 1 for the first overtime (read it with period_type). |
| `period_type` | character | Period type: REGULAR or OVERTIME. |
| `action_type` | character | Shot value: 2pt or 3pt |
| `sub_type` | character | Shot type (layup, drivinglayup, jumpshot, pullupjumpshot, dunk, ...) |
| `result` | integer | Shot result: 1 = made, 0 = missed. |
| `x` | numeric | Court x, PERCENT of the full court length (0-100, float), along the long axis. Not normalised to one basket: a team attacks one end in periods 1-2 and the other end in periods 3-4 (ESAKE 2902690: team 1 mean x 90 and 86 in P1-P2, 11 and 19 in P3-P4). Which end x = 0 is, and which end team 1 starts at, are unverified |
| `y` | numeric | Court y, PERCENT of the full court width (0-100, float), across the court (observed 1.7-97.2). Origin side unverified |
| `player` | character | Shooter's display name as the org enters it, in the local script when it uses one. |
| `shirt_number` | character | Shooter's shirt number (string) |

**`return_parsed=False`** — the body or page record (`dict` / `list`): `data.json` as served plus `_request` (the match id asked for), or the sdv-internal-refs capture shape of a page.

### Raises {#genius_shots-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#genius_shots-example}

```python
genius_shots(match_id=2902690)
```

_Last validated n/a._

## genius_competition_games

The recent-games window of a competition, with the global Genius team ids.

**Endpoint URL:** `GET https://fibalivestats.dcd.shared.geniussports.com/data/competition/{comp_id}.json`

**Valid URL:** [https://fibalivestats.dcd.shared.geniussports.com/data/competition/49915.json](https://fibalivestats.dcd.shared.geniussports.com/data/competition/49915.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `comp_id` | `comp_id` |  | `Y` |  | Genius competition id: one per org competition-season (comp_id in genius_competitions), e.g. 49915. |

### Returns {#genius_competition_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | integer | Genius league id of the org (ESAKE = 115); the leagueId column of the hosted chooser |
| `competition_id` | integer | Genius competition id (compId): one per org competition-season; the {compId} of hosted and data/competition paths |
| `match_id` | integer | Genius match id: the {matchId} of data.json and /u/; one global sequence across every Genius customer (~0.3 M in 2016, ~2.9 M in Oct 2026) |
| `match_status` | character | Match status: COMPLETE, SCHEDULED, IN_PROGRESS, ... (upper case) |
| `match_time` | character | Local tip-off time, 'YYYY-MM-DD HH:MM:SS' (venue time zone, not stated) |
| `match_time_utc` | character | Tip-off time in UTC, 'YYYY-MM-DD HH:MM:SS' |
| `live` | integer | 1 while the game is live, else 0 |
| `period` | integer | Current period of a live or finished game (present on some rows only) |
| `period_type` | character | Current period type: REGULAR or OVERTIME (present on some rows only) |
| `clock` | character | Period clock of a live or finished game, 'mm:ss' remaining (present on some rows only) |
| `home_team_id` | integer | Home team: Genius team id (warehouse key space, global across orgs and seasons; the /team/{id} of hosted pages, the tid of the widget, the teamId of data/competition). data.json carries no team id: join on name/code |
| `home_name` | character | Home team full name, in the competition's own script when the org enters one. |
| `home_name_international` | character | Home team name in Latin script |
| `home_code` | character | Home team short code, in the competition's own script when the org enters one. |
| `home_score` | integer | Home points: a string on most rows, an integer on some ('' before tip-off); cast it |
| `away_team_id` | integer | Away team: Genius team id (warehouse key space, global across orgs and seasons; the /team/{id} of hosted pages, the tid of the widget, the teamId of data/competition). data.json carries no team id: join on name/code |
| `away_name` | character | Away team full name, in the competition's own script when the org enters one. |
| `away_name_international` | character | Away team name in Latin script |
| `away_code` | character | Away team short code, in the competition's own script when the org enters one. |
| `away_score` | integer | Away points: a string on most rows, an integer on some ('' before tip-off); cast it |

**`return_parsed=False`** — the body or page record (`dict` / `list`): `data.json` as served plus `_request` (the match id asked for), or the sdv-internal-refs capture shape of a page.

### Raises {#genius_competition_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#genius_competition_games-example}

```python
genius_competition_games(comp_id=49915)
```

_Last validated n/a._

## genius_match_competition

The competition (comp_id and name) a game belongs to, from its LiveStats HTML shell.

**Endpoint URL:** `GET https://fibalivestats.dcd.shared.geniussports.com/u/{org}/{match_id}/`

**Valid URL:** [https://fibalivestats.dcd.shared.geniussports.com/u/ESAKE/2902690/](https://fibalivestats.dcd.shared.geniussports.com/u/ESAKE/2902690/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org` | `org` |  | `Y` |  | LiveStats ORG of the game's shell (can differ from the hosted ORG: NZN vs NZNBL). |
| `match_id` | `match_id` |  | `Y` |  | Genius match id (match_id in genius_schedule, genius_widget_games or genius_competition_games), e.g. 2902690. |

### Returns {#genius_match_competition-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | Match id from the page's hidden input #matchId (equals the {matchId} asked for). Genius match id: the {matchId} of data.json and /u/; one global sequence across every Genius customer (~0.3 M in 2016, ~2.9 M in Oct 2026) |
| `comp_id` | integer | Competition id from the hidden input #compId. Genius competition id (compId): one per org competition-season; the {compId} of hosted and data/competition paths. The way to map a data.json game to its competition and season |
| `competition_name` | character | Competition label from #competitionName, free text ('Stoiximan GBL 2026-2027'); carries the season only when the org writes it in |
| `org_label` | character | First item of og:description: the org's display code (ESAKE) |
| `og_description` | character | The page's og:description: ' , <competition>, Check out the full statistics here' |
| `org` | character | ORG code the request used (the hosted ORG, or the LiveStats ORG for the /u/ shell). |

**`return_parsed=False`** — the body or page record (`dict` / `list`): `data.json` as served plus `_request` (the match id asked for), or the sdv-internal-refs capture shape of a page.

### Raises {#genius_match_competition-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#genius_match_competition-example}

```python
genius_match_competition(match_id=2902690, org='ESAKE')
```

_Last validated n/a._

## genius_competitions

Every competition of an org from the hosted WebStats chooser, with its END-year season.

**Endpoint URL:** `GET https://hosted.dcd.shared.geniussports.com/{org}/en/schedule`

**Valid URL:** [https://hosted.dcd.shared.geniussports.com/ESAKE/en/schedule](https://hosted.dcd.shared.geniussports.com/ESAKE/en/schedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org` | `org` |  | `Y` |  | Genius ORG code of the league (ESAKE, CEBL, BNXT, WBBL, ...); see genius_competitions. |

### Returns {#genius_competitions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `org` | character | ORG code the request used (the hosted ORG, or the LiveStats ORG for the /u/ shell). |
| `comp_id` | integer | Genius competition id (compId): one per org competition-season; the {compId} of hosted and data/competition paths |
| `label` | character | Competition label as the org typed it, free text in any language: may or may not carry a season ('BASKET LEAGUE 2019 - 20', '1st division', 'Κύπελλο Ελλάδας' = Greek Cup, 'Μαυροσκούφεια' = Mavroskoufeia youth tournament) |
| `league_id` | integer | Genius league id of the org (ESAKE = 115; IBBA has two: 187 and 549) |
| `year` | integer | Genius season year of the competition: the START year of a two-year season (BNXT '2023-2024' = 2023), the year itself for a calendar season. null when the page gives none |
| `season` | integer | sdv END-year season of the competition (2026 = 2025-26) from the label span, else the Genius year; null when no rule applies. See season_rule. |
| `season_rule` | character | Which rule produced season: 'label span', 'genius year + 1 (split)', 'genius year (calendar)', 'label year (calendar)' or 'unknown'. |
| `selected` | logical | True for the competition the hosted chooser pre-selects (the org's default, not always the newest). |

**`return_parsed=False`** — the body or page record (`dict` / `list`): `data.json` as served plus `_request` (the match id asked for), or the sdv-internal-refs capture shape of a page.

### Raises {#genius_competitions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#genius_competitions-example}

```python
genius_competitions(org='ESAKE')
```

_Last validated n/a._

## genius_schedule

Every match of a competition (hosted WebStats): ids, status, venue, teams and scores.

**Endpoint URL:** `GET https://hosted.dcd.shared.geniussports.com/{org}/en/competition/{comp_id}/schedule`

**Valid URL:** [https://hosted.dcd.shared.geniussports.com/CEBL/en/competition/47852/schedule?roundNumber=-1](https://hosted.dcd.shared.geniussports.com/CEBL/en/competition/47852/schedule?roundNumber=-1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org` | `org` |  | `Y` |  | Genius ORG code of the league (ESAKE, CEBL, BNXT, WBBL, ...); see genius_competitions. |
| `comp_id` | `comp_id` |  | `Y` |  | Genius competition id: one per org competition-season (comp_id in genius_competitions), e.g. 49915. |
| `roundNumber` | `round_number` |  |  | `Y` | Round to list; -1 (default) lists every round of the competition. |

### Returns {#genius_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | From the block id extfix_<id>. Genius match id: the {matchId} of data.json and /u/; one global sequence across every Genius customer (~0.3 M in 2016, ~2.9 M in Oct 2026) |
| `link_match_id` | integer | Match id of the block's /match/<id>/summary link; equals match_id (the capture refuses a page where it does not); null when the org hides match pages (FFBB) |
| `comp_id` | integer | CompId of the match link, or of the team links when the org hides match pages (FFBB); equals the {compId} asked for. A 'To be determined' slot (no team or match link) takes the {compId} the page's other blocks prove |
| `org` | character | ORG code of the match link (or the team links, as comp_id) |
| `status` | character | Status from the block class STATUS_*: COMPLETE, SCHEDULED, POSTPONED, ... |
| `date_time` | character | Tip-off as displayed, 'Mon D, YYYY, h:mm AM' (local time, zone not stated); null when unscheduled |
| `venue_id` | integer | Genius venue id (the /venue/{id} of hosted pages); null when the venue is plain text (FFBB) |
| `venue_name` | character | Venue name as entered (local script) |
| `home_team_id` | integer | Home team: Genius team id (warehouse key space, global across orgs and seasons; the /team/{id} of hosted pages, the tid of the widget, the teamId of data/competition). data.json carries no team id: join on name/code |
| `home_team_name` | character | Home team full name |
| `home_team_code` | character | Home team code (local script possible) |
| `home_score` | integer | Home points; null before tip-off (typed integer only on a route whose 3 sampled rows were all played) |
| `away_team_id` | integer | Away team: Genius team id (warehouse key space, global across orgs and seasons; the /team/{id} of hosted pages, the tid of the widget, the teamId of data/competition). data.json carries no team id: join on name/code |
| `away_team_name` | character | Away team full name |
| `away_team_code` | character | Away team code (local script possible) |
| `away_score` | integer | Away points; null before tip-off (typed integer only on a route whose 3 sampled rows were all played) |

**`return_parsed=False`** — the body or page record (`dict` / `list`): `data.json` as served plus `_request` (the match id asked for), or the sdv-internal-refs capture shape of a page.

### Raises {#genius_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#genius_schedule-example}

```python
genius_schedule(comp_id=47852, org='CEBL')
```

_Last validated n/a._

## genius_schedule_embed

Every match of a competition from the JSON-wrapped embed page (orgs whose plain pages answer 500).

**Endpoint URL:** `GET https://hosted.dcd.shared.geniussports.com/embednf/{org}/en/competition/{comp_id}/schedule`

**Valid URL:** [https://hosted.dcd.shared.geniussports.com/embednf/HKS/en/competition/49946/schedule?roundNumber=-1](https://hosted.dcd.shared.geniussports.com/embednf/HKS/en/competition/49946/schedule?roundNumber=-1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org` | `org` |  | `Y` |  | Genius ORG code of the league (ESAKE, CEBL, BNXT, WBBL, ...); see genius_competitions. |
| `comp_id` | `comp_id` |  | `Y` |  | Genius competition id: one per org competition-season (comp_id in genius_competitions), e.g. 49915. |
| `roundNumber` | `round_number` |  |  | `Y` | Round to list; -1 (default) lists every round of the competition. |

### Returns {#genius_schedule_embed-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | From the block id extfix_<id>. Genius match id: the {matchId} of data.json and /u/; one global sequence across every Genius customer (~0.3 M in 2016, ~2.9 M in Oct 2026) |
| `link_match_id` | integer | Match id of the block's /match/<id>/summary link; equals match_id (the capture refuses a page where it does not); null when the org hides match pages (FFBB) |
| `comp_id` | integer | CompId of the match link, or of the team links when the org hides match pages (FFBB); equals the {compId} asked for. A 'To be determined' slot (no team or match link) takes the {compId} the page's other blocks prove |
| `org` | character | ORG code of the match link (or the team links, as comp_id) |
| `status` | character | Status from the block class STATUS_*: COMPLETE, SCHEDULED, POSTPONED, ... |
| `date_time` | character | Tip-off as displayed, 'Mon D, YYYY, h:mm AM' (local time, zone not stated); null when unscheduled |
| `venue_id` | integer | Genius venue id (the /venue/{id} of hosted pages); null when the venue is plain text (FFBB) |
| `venue_name` | character | Venue name as entered (local script) |
| `home_team_id` | integer | Home team: Genius team id (warehouse key space, global across orgs and seasons; the /team/{id} of hosted pages, the tid of the widget, the teamId of data/competition). data.json carries no team id: join on name/code |
| `home_team_name` | character | Home team full name |
| `home_team_code` | character | Home team code (local script possible) |
| `home_score` | integer | Home points; null before tip-off (typed integer only on a route whose 3 sampled rows were all played) |
| `away_team_id` | integer | Away team: Genius team id (warehouse key space, global across orgs and seasons; the /team/{id} of hosted pages, the tid of the widget, the teamId of data/competition). data.json carries no team id: join on name/code |
| `away_team_name` | character | Away team full name |
| `away_team_code` | character | Away team code (local script possible) |
| `away_score` | integer | Away points; null before tip-off (typed integer only on a route whose 3 sampled rows were all played) |

**`return_parsed=False`** — the body or page record (`dict` / `list`): `data.json` as served plus `_request` (the match id asked for), or the sdv-internal-refs capture shape of a page.

### Raises {#genius_schedule_embed-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#genius_schedule_embed-example}

```python
genius_schedule_embed(comp_id=49946, org='HKS')
```

_Last validated n/a._

## genius_widget_games

The latest games a league site's results widget shows (needs no proxy).

**Endpoint URL:** `GET https://widget.wh.geniussports.com/widget/?{widget_key}`

**Valid URL:** [https://widget.wh.geniussports.com/widget/?RFJIN037I07QV520B7WEXKV1GYNA31](https://widget.wh.geniussports.com/widget/?RFJIN037I07QV520B7WEXKV1GYNA31)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `widget_key` | `widget_key` |  | `Y` |  | The league site's public widget key (the bare query string of widget.wh.geniussports.com/widget/). |

### Returns {#genius_widget_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | From the webcast link. Genius match id: the {matchId} of data.json and /u/; one global sequence across every Genius customer (~0.3 M in 2016, ~2.9 M in Oct 2026) |
| `webcast_org` | character | ORG code of the webcast link; can differ from the hosted ORG (NZNBL games link /webcast/NZN/) |
| `competition_name` | character | Competition label, free text |
| `status` | character | Status text as shown ('Final', 'Live', a tip-off time) |
| `date` | character | Date as shown, 'DD/MM/YYYY' in samples (format follows the widget locale; unverified for others) |
| `home_team_id` | integer | Home team from the 'tid' class. Genius team id (warehouse key space, global across orgs and seasons; the /team/{id} of hosted pages, the tid of the widget, the teamId of data/competition). data.json carries no team id: join on name/code |
| `home_team_code` | character | Home team short code from the widget, in the local script when the org enters one. |
| `home_score` | integer | Home points; null before tip-off (typed integer only on a route whose 3 sampled rows were all played) |
| `away_team_id` | integer | Away team from the 'tid' class. Genius team id (warehouse key space, global across orgs and seasons; the /team/{id} of hosted pages, the tid of the widget, the teamId of data/competition). data.json carries no team id: join on name/code |
| `away_team_code` | character | Away team short code from the widget, in the local script when the org enters one. |
| `away_score` | integer | Away points; null before tip-off (typed integer only on a route whose 3 sampled rows were all played) |

**`return_parsed=False`** — the body or page record (`dict` / `list`): `data.json` as served plus `_request` (the match id asked for), or the sdv-internal-refs capture shape of a page.

### Raises {#genius_widget_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#genius_widget_games-example}

```python
genius_widget_games(widget_key='RFJIN037I07QV520B7WEXKV1GYNA31')
```

_Last validated n/a._
