---
title: "Genius Sports LiveStats — additional Python functions — Genius Sports LiveStats: bal_competitions–esake_schedule"
sidebar_label: "Genius Sports LiveStats: bal_competitions–esake_schedule"
sidebar_position: 1
description: "Genius Sports LiveStats — additional Python functions — Genius Sports LiveStats: bal_competitions–esake_schedule — function reference in sdv-py, the SportsDataverse Python package."
---
# Genius Sports LiveStats — additional Python functions — Genius Sports LiveStats: bal_competitions–esake_schedule

### bal_competitions {#bal_competitions}

`bal_competitions(*, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Every competition of the BAL Genius org, with its END-year season.

Basketball Africa League (men's); season rule: calendar year (BAL 2025 = 2025); 2023 lists two competitions, so pass comp_id.

Convenience wrapper: a thin binding of `genius_competitions` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: Access: the Genius hosts publish no robots.txt and no terms. CloudFront refuses datacenter IPs on the LiveStats and hosted hosts: from a server set the optional `GENIUS_PROXY` environment variable to an http(s) proxy URL (it is never logged or printed). The wrappers space requests at least one second apart and read only the Genius hosts; keep crawls slow and incremental. Some league sites (the NZ NBL) forbid AI crawlers in their own robots.txt, which does not cover these hosts.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the chooser page record (`dict`) plus `_request` (the org asked for) when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `org` | String | ORG code the request used (the hosted ORG, or the LiveStats ORG for the /u/ shell). |
| `comp_id` | Int64 | Genius competition id (compId): one per org competition-season; the {compId} of hosted and data/competition paths |
| `label` | String | Competition label as the org typed it, free text in any language: may or may not carry a season ('BASKET LEAGUE 2019 - 20', '1st division', 'Κύπελλο Ελλάδας' = Greek Cup, 'Μαυροσκούφεια' = Mavroskoufeia youth tournament) |
| `league_id` | Int64 | Genius league id of the org (ESAKE = 115; IBBA has two: 187 and 549) |
| `year` | Int64 | Genius season year of the competition: the START year of a two-year season (BNXT '2023-2024' = 2023), the year itself for a calendar season. null when the page gives none |
| `season` | Int64 | sdv END-year season of the competition (2026 = 2025-26) from the label span, else the Genius year; null when no rule applies. See season_rule. |
| `season_rule` | String | Which rule produced season: 'label span', 'genius year + 1 (split)', 'genius year (calendar)', 'label year (calendar)' or 'unknown'. |
| `selected` | Boolean | True for the competition the hosted chooser pre-selects (the org's default, not always the newest). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.genius import bal_competitions
bal_competitions()
```

### bal_pbp {#bal_pbp}

`bal_pbp(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Play-by-play of a finished Basketball Africa League game.

Basketball Africa League (men's); season rule: calendar year (BAL 2025 = 2025); 2023 lists two competitions, so pass comp_id.

Convenience wrapper: a thin binding of `genius_pbp` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: Access: the Genius hosts publish no robots.txt and no terms. CloudFront refuses datacenter IPs on the LiveStats and hosted hosts: from a server set the optional `GENIUS_PROXY` environment variable to an http(s) proxy URL (it is never logged or printed). The wrappers space requests at least one second apart and read only the Genius hosts; keep crawls slow and incremental. Some league sites (the NZ NBL) forbid AI crawlers in their own robots.txt, which does not cover these hosts. Genius match ids are one global sequence shared by every customer, and data.json names no competition, so the id is checked on the game's LiveStats shell (/u/BAL/<match_id>/: one extra request per game, cached per process): its competition name must pass the label rule. genius_pbp(match_id=...) reads any game unchecked.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule) of a game of this competition. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus `_request` (the match id asked for), or the parsed record of a page when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | Genius match id of the game. data.json does not carry it: it is the match_id asked for, stamped by the runtime (the global match sequence shared by every Genius customer). |
| `row_index` | Int64 | Position of the action in the feed order, 0 = the newest action by game time (normally the game end row). The feed is not strictly sorted by action_number, so sort on action_number for entry order. |
| `action_number` | Int64 | Action sequence number, unique within the game (gaps where actions were deleted); a shot joins its play-by-play row on it. Null in the 2014-15 feed. |
| `previous_action` | Int64 | action_number of the linked earlier action (the shot an assist or rebound belongs to); null when the feed sends an empty string. |
| `period` | Int64 | Period number; restarts at 1 for the first overtime (read it with period_type). |
| `period_type` | String | Period type: REGULAR or OVERTIME. |
| `gt` | String | Game clock, 'mm:ss' remaining in the period |
| `clock` | String | Game clock with hundredths, 'mm:ss:cc' remaining in the period; absent in older feeds (the 2014-15 WBBL sample has gt only) |
| `tno` | Int64 | Team number within this game: 1 = the team listed first (home on the hosted schedule and widget), 2 = the other, 0 = no team. In-game only: not a team id. |
| `pno` | Int64 | Player number within this game, the key of the team's player map; 0 = no player. In-game only: not a person id and not the shirt number. |
| `player` | String | Player display name; '' when no player |
| `shirt_number` | String | Player shirt number (string); '' when no player |
| `first_name` | String | Given name in the competition's own script and spelling (Greek, Hebrew, Czech, ... as entered) |
| `family_name` | String | Family name in the competition's own script |
| `international_first_name` | String | Given name in Latin script |
| `international_family_name` | String | Family name in Latin script (often upper case) |
| `scoreboard_name` | String | Short display name ('T. Nichols'), in the local script when the org enters one |
| `action_type` | String | Action type: 2pt, 3pt, freethrow, rebound, assist, turnover, steal, block, foul, foulon (foul drawn), substitution, timeout, jumpball, period, game, headcoachchallenge |
| `sub_type` | String | Action subtype, free-text code per actionType: shot type for 2pt/3pt (layup, drivinglayup, jumpshot, pullupjumpshot, turnaroundjumpshot, ...), 'in'/'out' for substitutions, 'start'/'end' for period and game, foul kind for fouls; empty string when none |
| `qualifier` | String | Qualifier codes of the action joined with commas (pointsinthepaint, fromturnover, 2ndchance, fastbreak, blocked, ...); null when none. |
| `success` | Int64 | 1 = made / successful, 0 = missed (shots, free throws); 1 for other actions |
| `scoring` | Int64 | 1 when the action is a scoring attempt (shot or free throw), else 0 |
| `score_1` | Int64 | Team 1 score after the action, always an integer (the feed sends a string in most games). |
| `score_2` | Int64 | Team 2 score after the action, always an integer (the feed sends a string in most games). |
| `lead` | Int64 | Score margin after the action, team 1 minus team 2 (negative when team 2 leads) |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. The match is not a game of this competition (another org's game, or a cup or play-off of BAL), or the scope check could not run (the /u/ shell request failed twice or names no competition). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.genius import bal_schedule, bal_pbp
games = bal_schedule()
bal_pbp(match_id=int(games['match_id'][0]))
```

### bal_player_box {#bal_player_box}

`bal_player_box(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Player box score of a finished Basketball Africa League game.

Basketball Africa League (men's); season rule: calendar year (BAL 2025 = 2025); 2023 lists two competitions, so pass comp_id.

Convenience wrapper: a thin binding of `genius_player_box` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: Access: the Genius hosts publish no robots.txt and no terms. CloudFront refuses datacenter IPs on the LiveStats and hosted hosts: from a server set the optional `GENIUS_PROXY` environment variable to an http(s) proxy URL (it is never logged or printed). The wrappers space requests at least one second apart and read only the Genius hosts; keep crawls slow and incremental. Some league sites (the NZ NBL) forbid AI crawlers in their own robots.txt, which does not cover these hosts. Genius match ids are one global sequence shared by every customer, and data.json names no competition, so the id is checked on the game's LiveStats shell (/u/BAL/<match_id>/: one extra request per game, cached per process): its competition name must pass the label rule. genius_player_box(match_id=...) reads any game unchecked.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule) of a game of this competition. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus `_request` (the match id asked for), or the parsed record of a page when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | Genius match id of the game. data.json does not carry it: it is the match_id asked for, stamped by the runtime (the global match sequence shared by every Genius customer). |
| `tno` | Int64 | Team number within this game: 1 = the team listed first (home on the hosted schedule and widget), 2 = the other, 0 = no team. In-game only: not a team id. |
| `pno` | Int64 | Player number within this game, the key of the team's player map; 0 = no player. In-game only: not a person id and not the shirt number. |
| `name` | String | Display name ('T. Nichols') |
| `first_name` | String | Given name in the competition's own script and spelling (Greek, Hebrew, Czech, ... as entered) |
| `family_name` | String | Family name in the competition's own script |
| `international_first_name` | String | Given name in Latin script |
| `international_family_name` | String | Family name in Latin script (often upper case) |
| `scoreboard_name` | String | Short display name ('T. Nichols'), in the local script when the org enters one |
| `shirt_number` | String | Shirt number as a string ('0' and '00' differ) |
| `playing_position` | String | Playing position code (PG, SG, SF, PF, C, G, F, ...); empty string when not entered |
| `starter` | Int64 | 1 when the player started the game, else 0 |
| `captain` | Int64 | 1 for the team captain, else 0; absent in some orgs |
| `active` | Int64 | 1 when the player is on court now (at game end: on court at the final whistle), else 0 |
| `minutes` | String | Minutes as the feed prints them, an 'mm:ss' text (can exceed 59 for a team; '199:60' happens); the text of an integer when the feed sends one. |
| `seconds_played` | Int64 | Seconds played parsed from the printed mm:ss text as minutes * 60 + seconds (so '224:60' gives 13500); null when the feed sent an integer, whose unit is unknown. |
| `field_goals_made` | Int64 | Field goals the player made in the game (two- and three-pointers together; a count). |
| `field_goals_attempted` | Int64 | Field goals the player attempted in the game (two- and three-pointers together; a count). |
| `field_goals_percentage` | Float64 | Field-goal percentage, 0-100, rounded to an integer by the feed |
| `three_pointers_made` | Int64 | Three-pointers made |
| `three_pointers_attempted` | Int64 | Three-pointers attempted |
| `three_pointers_percentage` | Float64 | Three-point percentage, 0-100, rounded to an integer |
| `two_pointers_made` | Int64 | Two-pointers made |
| `two_pointers_attempted` | Int64 | Two-pointers attempted |
| `two_pointers_percentage` | Float64 | Two-point percentage, 0-100, rounded to an integer |
| `free_throws_made` | Int64 | Free throws the player made in the game (a count). |
| `free_throws_attempted` | Int64 | Free throws the player attempted in the game (a count). |
| `free_throws_percentage` | Float64 | Free-throw percentage, 0-100, rounded to an integer |
| `rebounds_defensive` | Int64 | Defensive rebounds |
| `rebounds_offensive` | Int64 | Offensive rebounds |
| `rebounds_total` | Int64 | Total rebounds (offensive plus defensive) the player collected in the game. |
| `assists` | Int64 | Assists credited to the player in the game. |
| `turnovers` | Int64 | Turnovers committed by the player in the game. |
| `steals` | Int64 | Steals credited to the player in the game. |
| `blocks` | Int64 | Shots the player blocked in the game (blocks made, not received). |
| `blocks_received` | Int64 | Own shots blocked by an opponent |
| `fouls_personal` | Int64 | Personal fouls committed |
| `fouls_on` | Int64 | Fouls drawn: personal fouls committed against the player in the game. |
| `points` | Int64 | Points the player scored in the game, free throws included. |
| `points_from_turnovers` | Int64 | Points off turnovers |
| `points_second_chance` | Int64 | Second-chance points |
| `points_fast_break` | Int64 | Fast-break points |
| `points_in_the_paint` | Int64 | Points the player scored in the paint in the game (the feed's sPointsInThePaint). |
| `plus_minus_points` | Int64 | Plus/minus: team point margin while the player was on court |
| `eff_1` | Int64 | Genius efficiency formula 1. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site |
| `eff_2` | Int64 | Genius efficiency formula 2. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site |
| `eff_3` | Float64 | Genius efficiency formula 3. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site |
| `eff_4` | Float64 | Genius efficiency formula 4. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site |
| `eff_5` | Int64 | Genius efficiency formula 5. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site |
| `eff_6` | Int64 | Genius efficiency formula 6. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site |
| `eff_7` | Int64 | Genius efficiency formula 7. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site. Absent in the 2014-15 era sample (WBBL 12248), which carries eff_1..eff_6 on players and no team tot_eff_* |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. The match is not a game of this competition (another org's game, or a cup or play-off of BAL), or the scope check could not run (the /u/ shell request failed twice or names no competition). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.genius import bal_schedule, bal_player_box
games = bal_schedule()
bal_player_box(match_id=int(games['match_id'][0]))
```

### bal_schedule {#bal_schedule}

`bal_schedule(season: 'Optional[int]' = None, comp_id: 'Optional[int]' = None, round_number: 'Optional[int]' = -1, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Every match of one Basketball Africa League season (hosted WebStats): ids, status, venue, teams and scores.

Basketball Africa League (men's); season rule: calendar year (BAL 2025 = 2025); 2023 lists two competitions, so pass comp_id.

Convenience wrapper: a thin binding of `genius_schedule` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: Access: the Genius hosts publish no robots.txt and no terms. CloudFront refuses datacenter IPs on the LiveStats and hosted hosts: from a server set the optional `GENIUS_PROXY` environment variable to an http(s) proxy URL (it is never logged or printed). The wrappers space requests at least one second apart and read only the Genius hosts; keep crawls slow and incremental. Some league sites (the NZ NBL) forbid AI crawlers in their own robots.txt, which does not cover these hosts. The competition is picked from BAL's chooser by the label rule include='.' exclude='^$'; cups and play-offs have their own comp_id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | END-year season (see the season rule above). Omitted = the newest season the org lists. |
| `comp_id` | `Optional[int]` | `None` | Genius competition id; skips the season lookup (needed when the label rule matches several competitions). |
| `round_number` | `Optional[int]` | `-1` | Round to list; -1 (default) lists every round. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the schedule page record (`dict`) when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | From the block id extfix_<id>. Genius match id: the {matchId} of data.json and /u/; one global sequence across every Genius customer (~0.3 M in 2016, ~2.9 M in Oct 2026) |
| `link_match_id` | Int64 | Match id of the block's /match/<id>/summary link; equals match_id (the capture refuses a page where it does not); null when the org hides match pages (FFBB) |
| `comp_id` | Int64 | CompId of the match link, or of the team links when the org hides match pages (FFBB); equals the {compId} asked for. A 'To be determined' slot (no team or match link) takes the {compId} the page's other blocks prove |
| `org` | String | ORG code of the match link (or the team links, as comp_id) |
| `status` | String | Status from the block class STATUS_*: COMPLETE, SCHEDULED, POSTPONED, ... |
| `date_time` | String | Tip-off as displayed, 'Mon D, YYYY, h:mm AM' (local time, zone not stated); null when unscheduled |
| `venue_id` | Int64 | Genius venue id (the /venue/{id} of hosted pages); null when the venue is plain text (FFBB) |
| `venue_name` | String | Venue name as entered (local script) |
| `home_team_id` | Int64 | Home team: Genius team id (warehouse key space, global across orgs and seasons; the /team/{id} of hosted pages, the tid of the widget, the teamId of data/competition). data.json carries no team id: join on name/code |
| `home_team_name` | String | Home team full name |
| `home_team_code` | String | Home team code (local script possible) |
| `home_score` | Int64 | Home points; null before tip-off (typed integer only on a route whose 3 sampled rows were all played) |
| `away_team_id` | Int64 | Away team: Genius team id (warehouse key space, global across orgs and seasons; the /team/{id} of hosted pages, the tid of the widget, the teamId of data/competition). data.json carries no team id: join on name/code |
| `away_team_name` | String | Away team full name |
| `away_team_code` | String | Away team code (local script possible) |
| `away_score` | Int64 | Away points; null before tip-off (typed integer only on a route whose 3 sampled rows were all played) |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). The org lists no matching competition. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The season matches several competitions (pass `comp_id`). |

**Example**

```python
from sportsdataverse.basketball.genius import bal_schedule
bal_schedule(season=2026)
```

### bal_shots {#bal_shots}

`bal_shots(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Shot chart of a finished Basketball Africa League game.

Basketball Africa League (men's); season rule: calendar year (BAL 2025 = 2025); 2023 lists two competitions, so pass comp_id.

Convenience wrapper: a thin binding of `genius_shots` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: Access: the Genius hosts publish no robots.txt and no terms. CloudFront refuses datacenter IPs on the LiveStats and hosted hosts: from a server set the optional `GENIUS_PROXY` environment variable to an http(s) proxy URL (it is never logged or printed). The wrappers space requests at least one second apart and read only the Genius hosts; keep crawls slow and incremental. Some league sites (the NZ NBL) forbid AI crawlers in their own robots.txt, which does not cover these hosts. Genius match ids are one global sequence shared by every customer, and data.json names no competition, so the id is checked on the game's LiveStats shell (/u/BAL/<match_id>/: one extra request per game, cached per process): its competition name must pass the label rule. genius_shots(match_id=...) reads any game unchecked.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule) of a game of this competition. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus `_request` (the match id asked for), or the parsed record of a page when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | Genius match id of the game. data.json does not carry it: it is the match_id asked for, stamped by the runtime (the global match sequence shared by every Genius customer). |
| `tno` | Int64 | Team number within this game: 1 = the team listed first (home on the hosted schedule and widget), 2 = the other, 0 = no team. In-game only: not a team id. |
| `pno` | Int64 | Player number within this game, the key of the team's player map; 0 = no player. In-game only: not a person id and not the shirt number. |
| `action_number` | Int64 | Action sequence number, unique within the game (gaps where actions were deleted); a shot joins its play-by-play row on it. Null in the 2014-15 feed. |
| `period` | Int64 | Period number; restarts at 1 for the first overtime (read it with period_type). |
| `period_type` | String | Period type: REGULAR or OVERTIME. |
| `action_type` | String | Shot value: 2pt or 3pt |
| `sub_type` | String | Shot type (layup, drivinglayup, jumpshot, pullupjumpshot, dunk, ...) |
| `result` | Int64 | Shot result: 1 = made, 0 = missed. |
| `x` | Float64 | Court x, PERCENT of the full court length (0-100, float), along the long axis. Not normalised to one basket: a team attacks one end in periods 1-2 and the other end in periods 3-4 (ESAKE 2902690: team 1 mean x 90 and 86 in P1-P2, 11 and 19 in P3-P4). Which end x = 0 is, and which end team 1 starts at, are unverified |
| `y` | Float64 | Court y, PERCENT of the full court width (0-100, float), across the court (observed 1.7-97.2). Origin side unverified |
| `player` | String | Shooter's display name as the org enters it, in the local script when it uses one. |
| `shirt_number` | String | Shooter's shirt number (string) |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. The match is not a game of this competition (another org's game, or a cup or play-off of BAL), or the scope check could not run (the /u/ shell request failed twice or names no competition). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.genius import bal_schedule, bal_shots
games = bal_schedule()
bal_shots(match_id=int(games['match_id'][0]))
```

### bal_team_box {#bal_team_box}

`bal_team_box(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Team box score of a finished Basketball Africa League game.

Basketball Africa League (men's); season rule: calendar year (BAL 2025 = 2025); 2023 lists two competitions, so pass comp_id.

Convenience wrapper: a thin binding of `genius_team_box` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: Access: the Genius hosts publish no robots.txt and no terms. CloudFront refuses datacenter IPs on the LiveStats and hosted hosts: from a server set the optional `GENIUS_PROXY` environment variable to an http(s) proxy URL (it is never logged or printed). The wrappers space requests at least one second apart and read only the Genius hosts; keep crawls slow and incremental. Some league sites (the NZ NBL) forbid AI crawlers in their own robots.txt, which does not cover these hosts. Genius match ids are one global sequence shared by every customer, and data.json names no competition, so the id is checked on the game's LiveStats shell (/u/BAL/<match_id>/: one extra request per game, cached per process): its competition name must pass the label rule. genius_team_box(match_id=...) reads any game unchecked.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule) of a game of this competition. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus `_request` (the match id asked for), or the parsed record of a page when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | Genius match id of the game. data.json does not carry it: it is the match_id asked for, stamped by the runtime (the global match sequence shared by every Genius customer). |
| `tno` | Int64 | Team number within this game: 1 = the team listed first (home on the hosted schedule and widget), 2 = the other, 0 = no team. In-game only: not a team id. |
| `name` | String | Team name as the org enters it |
| `name_international` | String | Team name in Latin script |
| `short_name` | String | Short team name (shortName in the feed), in the competition's own script when the org enters one. |
| `code` | String | Team code, three letters as entered (may be in the local script, e.g. Greek 'ΑΕΚ') |
| `code_international` | String | Team code in Latin script |
| `coach` | String | Head coach display name |
| `score` | Int64 | Team points (current or final); a numeric string in the 2014-15 era sample (WBBL 12248), an integer since |
| `full_score` | Int64 | Team points including overtime; equal to score in every sample (a numeric string in the 2014-15 era) |
| `p1_score` | Int64 | Points scored in period 1 (a numeric string in the 2014-15 era) |
| `p2_score` | Int64 | Points scored in period 2 (a numeric string in the 2014-15 era) |
| `p3_score` | Int64 | Points scored in period 3 (a numeric string in the 2014-15 era) |
| `p4_score` | Int64 | Points scored in period 4 (a numeric string in the 2014-15 era) |
| `ot_score` | Int64 | Points the team scored in overtime; null without overtime. Whether several overtimes are summed is unverified. |
| `fouls` | Int64 | Team fouls in the current (last) period |
| `timeouts` | Int64 | Timeouts counter of the current period (unverified: remaining vs taken); 0 in every sampled (finished) game |
| `minutes` | String | Minutes as the feed prints them, an 'mm:ss' text (can exceed 59 for a team; '199:60' happens); the text of an integer when the feed sends one. |
| `seconds_played` | Int64 | Seconds played parsed from the printed mm:ss text as minutes * 60 + seconds (so '224:60' gives 13500); null when the feed sent an integer, whose unit is unknown. |
| `field_goals_made` | Int64 | Team total: field goals made |
| `field_goals_attempted` | Int64 | Team total: field goals attempted |
| `field_goals_percentage` | Float64 | Team total: field-goal percentage, 0-100, rounded to an integer by the feed |
| `three_pointers_made` | Int64 | Team total: three-pointers made |
| `three_pointers_attempted` | Int64 | Team total: three-pointers attempted |
| `three_pointers_percentage` | Float64 | Team total: three-point percentage, 0-100, rounded to an integer |
| `two_pointers_made` | Int64 | Team total: two-pointers made |
| `two_pointers_attempted` | Int64 | Team total: two-pointers attempted |
| `two_pointers_percentage` | Float64 | Team total: two-point percentage, 0-100, rounded to an integer |
| `free_throws_made` | Int64 | Team total: free throws made |
| `free_throws_attempted` | Int64 | Team total: free throws attempted |
| `free_throws_percentage` | Float64 | Team total: free-throw percentage, 0-100, rounded to an integer |
| `rebounds_defensive` | Int64 | Team total: defensive rebounds |
| `rebounds_offensive` | Int64 | Team total: offensive rebounds |
| `rebounds_total` | Int64 | Team total: total rebounds |
| `assists` | Int64 | Team total: assists |
| `turnovers` | Int64 | Team total: turnovers |
| `steals` | Int64 | Team total: steals |
| `blocks` | Int64 | Team total: blocks made |
| `blocks_received` | Int64 | Team total: own shots blocked by an opponent |
| `fouls_personal` | Int64 | Team total: personal fouls committed |
| `fouls_on` | Int64 | Team total: fouls drawn |
| `points` | Int64 | Team total: points |
| `points_from_turnovers` | Int64 | Team total: points off turnovers |
| `points_second_chance` | Int64 | Team total: second-chance points |
| `points_fast_break` | Int64 | Team total: fast-break points |
| `points_in_the_paint` | Int64 | Team total: points in the paint |
| `eff_1` | Int64 | Genius efficiency formula 1. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site (team total). Absent in the 2014-15 era sample (WBBL 12248), which carries eff_1..eff_6 on players and no team tot_eff_* |
| `eff_2` | Int64 | Genius efficiency formula 2. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site (team total). Team tot_eff_2 is 2 in every 2016+ sample (a constant, not a stat). Absent in the 2014-15 era sample (WBBL 12248), which carries eff_1..eff_6 on players and no team tot_eff_* |
| `eff_3` | Float64 | Genius efficiency formula 3. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site (team total). Absent in the 2014-15 era sample (WBBL 12248), which carries eff_1..eff_6 on players and no team tot_eff_* |
| `eff_4` | Float64 | Genius efficiency formula 4. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site (team total). Absent in the 2014-15 era sample (WBBL 12248), which carries eff_1..eff_6 on players and no team tot_eff_* |
| `eff_5` | Int64 | Genius efficiency formula 5. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site (team total). Absent in the 2014-15 era sample (WBBL 12248), which carries eff_1..eff_6 on players and no team tot_eff_* |
| `eff_6` | Int64 | Genius efficiency formula 6. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site (team total). Absent in the 2014-15 era sample (WBBL 12248), which carries eff_1..eff_6 on players and no team tot_eff_* |
| `eff_7` | Int64 | Genius efficiency formula 7. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site (team total). Absent in the 2014-15 era sample (WBBL 12248), which carries eff_1..eff_6 on players and no team tot_eff_* |
| `bench_points` | Int64 | Team total: points scored by non-starters |
| `biggest_lead` | Int64 | Team total: biggest lead, points |
| `biggest_scoring_run` | Int64 | Team total: biggest unanswered scoring run, points |
| `lead_changes` | Int64 | Team total: lead changes in the game (same value on both teams) |
| `times_scores_level` | Int64 | Team total: times the score was tied (same value on both teams) |
| `time_leading` | Float64 | Team total: minutes the team led, decimal minutes (33.58 = 33 min 35 s) |
| `fouls_team` | Int64 | Team total: team (bench) fouls |
| `fouls_total` | Int64 | Team total: all fouls committed (personal + technical + team bench) |
| `rebounds_team` | Int64 | Team total: team rebounds (credited to no player) |
| `rebounds_team_defensive` | Int64 | Team total: team defensive rebounds |
| `rebounds_team_offensive` | Int64 | Team total: team offensive rebounds |
| `turnovers_team` | Int64 | Team total: team turnovers (credited to no player, e.g. shot clock) |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. The match is not a game of this competition (another org's game, or a cup or play-off of BAL), or the scope check could not run (the /u/ shell request failed twice or names no competition). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.genius import bal_schedule, bal_team_box
games = bal_schedule()
bal_team_box(match_id=int(games['match_id'][0]))
```

### esake_competitions {#esake_competitions}

`esake_competitions(*, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Every competition of the ESAKE Genius org, with its END-year season.

Greek Basket League (ESAKE) (men's); season rule: END year; Genius year + 1 (2022-23 = 2023).

Convenience wrapper: a thin binding of `genius_competitions` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: Access: the Genius hosts publish no robots.txt and no terms. CloudFront refuses datacenter IPs on the LiveStats and hosted hosts: from a server set the optional `GENIUS_PROXY` environment variable to an http(s) proxy URL (it is never logged or printed). The wrappers space requests at least one second apart and read only the Genius hosts; keep crawls slow and incremental. Some league sites (the NZ NBL) forbid AI crawlers in their own robots.txt, which does not cover these hosts.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the chooser page record (`dict`) plus `_request` (the org asked for) when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `org` | String | ORG code the request used (the hosted ORG, or the LiveStats ORG for the /u/ shell). |
| `comp_id` | Int64 | Genius competition id (compId): one per org competition-season; the {compId} of hosted and data/competition paths |
| `label` | String | Competition label as the org typed it, free text in any language: may or may not carry a season ('BASKET LEAGUE 2019 - 20', '1st division', 'Κύπελλο Ελλάδας' = Greek Cup, 'Μαυροσκούφεια' = Mavroskoufeia youth tournament) |
| `league_id` | Int64 | Genius league id of the org (ESAKE = 115; IBBA has two: 187 and 549) |
| `year` | Int64 | Genius season year of the competition: the START year of a two-year season (BNXT '2023-2024' = 2023), the year itself for a calendar season. null when the page gives none |
| `season` | Int64 | sdv END-year season of the competition (2026 = 2025-26) from the label span, else the Genius year; null when no rule applies. See season_rule. |
| `season_rule` | String | Which rule produced season: 'label span', 'genius year + 1 (split)', 'genius year (calendar)', 'label year (calendar)' or 'unknown'. |
| `selected` | Boolean | True for the competition the hosted chooser pre-selects (the org's default, not always the newest). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.genius import esake_competitions
esake_competitions()
```

### esake_pbp {#esake_pbp}

`esake_pbp(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Play-by-play of a finished Greek Basket League (ESAKE) game.

Greek Basket League (ESAKE) (men's); season rule: END year; Genius year + 1 (2022-23 = 2023).

Convenience wrapper: a thin binding of `genius_pbp` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: Access: the Genius hosts publish no robots.txt and no terms. CloudFront refuses datacenter IPs on the LiveStats and hosted hosts: from a server set the optional `GENIUS_PROXY` environment variable to an http(s) proxy URL (it is never logged or printed). The wrappers space requests at least one second apart and read only the Genius hosts; keep crawls slow and incremental. Some league sites (the NZ NBL) forbid AI crawlers in their own robots.txt, which does not cover these hosts. Genius match ids are one global sequence shared by every customer, and data.json names no competition, so the id is checked on the game's LiveStats shell (/u/ESAKE/<match_id>/: one extra request per game, cached per process): its competition name must pass the label rule. genius_pbp(match_id=...) reads any game unchecked.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule) of a game of this competition. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus `_request` (the match id asked for), or the parsed record of a page when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | Genius match id of the game. data.json does not carry it: it is the match_id asked for, stamped by the runtime (the global match sequence shared by every Genius customer). |
| `row_index` | Int64 | Position of the action in the feed order, 0 = the newest action by game time (normally the game end row). The feed is not strictly sorted by action_number, so sort on action_number for entry order. |
| `action_number` | Int64 | Action sequence number, unique within the game (gaps where actions were deleted); a shot joins its play-by-play row on it. Null in the 2014-15 feed. |
| `previous_action` | Int64 | action_number of the linked earlier action (the shot an assist or rebound belongs to); null when the feed sends an empty string. |
| `period` | Int64 | Period number; restarts at 1 for the first overtime (read it with period_type). |
| `period_type` | String | Period type: REGULAR or OVERTIME. |
| `gt` | String | Game clock, 'mm:ss' remaining in the period |
| `clock` | String | Game clock with hundredths, 'mm:ss:cc' remaining in the period; absent in older feeds (the 2014-15 WBBL sample has gt only) |
| `tno` | Int64 | Team number within this game: 1 = the team listed first (home on the hosted schedule and widget), 2 = the other, 0 = no team. In-game only: not a team id. |
| `pno` | Int64 | Player number within this game, the key of the team's player map; 0 = no player. In-game only: not a person id and not the shirt number. |
| `player` | String | Player display name; '' when no player |
| `shirt_number` | String | Player shirt number (string); '' when no player |
| `first_name` | String | Given name in the competition's own script and spelling (Greek, Hebrew, Czech, ... as entered) |
| `family_name` | String | Family name in the competition's own script |
| `international_first_name` | String | Given name in Latin script |
| `international_family_name` | String | Family name in Latin script (often upper case) |
| `scoreboard_name` | String | Short display name ('T. Nichols'), in the local script when the org enters one |
| `action_type` | String | Action type: 2pt, 3pt, freethrow, rebound, assist, turnover, steal, block, foul, foulon (foul drawn), substitution, timeout, jumpball, period, game, headcoachchallenge |
| `sub_type` | String | Action subtype, free-text code per actionType: shot type for 2pt/3pt (layup, drivinglayup, jumpshot, pullupjumpshot, turnaroundjumpshot, ...), 'in'/'out' for substitutions, 'start'/'end' for period and game, foul kind for fouls; empty string when none |
| `qualifier` | String | Qualifier codes of the action joined with commas (pointsinthepaint, fromturnover, 2ndchance, fastbreak, blocked, ...); null when none. |
| `success` | Int64 | 1 = made / successful, 0 = missed (shots, free throws); 1 for other actions |
| `scoring` | Int64 | 1 when the action is a scoring attempt (shot or free throw), else 0 |
| `score_1` | Int64 | Team 1 score after the action, always an integer (the feed sends a string in most games). |
| `score_2` | Int64 | Team 2 score after the action, always an integer (the feed sends a string in most games). |
| `lead` | Int64 | Score margin after the action, team 1 minus team 2 (negative when team 2 leads) |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. The match is not a game of this competition (another org's game, or a cup or play-off of ESAKE), or the scope check could not run (the /u/ shell request failed twice or names no competition). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.genius import esake_schedule, esake_pbp
games = esake_schedule()
esake_pbp(match_id=int(games['match_id'][0]))
```

### esake_player_box {#esake_player_box}

`esake_player_box(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Player box score of a finished Greek Basket League (ESAKE) game.

Greek Basket League (ESAKE) (men's); season rule: END year; Genius year + 1 (2022-23 = 2023).

Convenience wrapper: a thin binding of `genius_player_box` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: Access: the Genius hosts publish no robots.txt and no terms. CloudFront refuses datacenter IPs on the LiveStats and hosted hosts: from a server set the optional `GENIUS_PROXY` environment variable to an http(s) proxy URL (it is never logged or printed). The wrappers space requests at least one second apart and read only the Genius hosts; keep crawls slow and incremental. Some league sites (the NZ NBL) forbid AI crawlers in their own robots.txt, which does not cover these hosts. Genius match ids are one global sequence shared by every customer, and data.json names no competition, so the id is checked on the game's LiveStats shell (/u/ESAKE/<match_id>/: one extra request per game, cached per process): its competition name must pass the label rule. genius_player_box(match_id=...) reads any game unchecked.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule) of a game of this competition. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus `_request` (the match id asked for), or the parsed record of a page when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | Genius match id of the game. data.json does not carry it: it is the match_id asked for, stamped by the runtime (the global match sequence shared by every Genius customer). |
| `tno` | Int64 | Team number within this game: 1 = the team listed first (home on the hosted schedule and widget), 2 = the other, 0 = no team. In-game only: not a team id. |
| `pno` | Int64 | Player number within this game, the key of the team's player map; 0 = no player. In-game only: not a person id and not the shirt number. |
| `name` | String | Display name ('T. Nichols') |
| `first_name` | String | Given name in the competition's own script and spelling (Greek, Hebrew, Czech, ... as entered) |
| `family_name` | String | Family name in the competition's own script |
| `international_first_name` | String | Given name in Latin script |
| `international_family_name` | String | Family name in Latin script (often upper case) |
| `scoreboard_name` | String | Short display name ('T. Nichols'), in the local script when the org enters one |
| `shirt_number` | String | Shirt number as a string ('0' and '00' differ) |
| `playing_position` | String | Playing position code (PG, SG, SF, PF, C, G, F, ...); empty string when not entered |
| `starter` | Int64 | 1 when the player started the game, else 0 |
| `captain` | Int64 | 1 for the team captain, else 0; absent in some orgs |
| `active` | Int64 | 1 when the player is on court now (at game end: on court at the final whistle), else 0 |
| `minutes` | String | Minutes as the feed prints them, an 'mm:ss' text (can exceed 59 for a team; '199:60' happens); the text of an integer when the feed sends one. |
| `seconds_played` | Int64 | Seconds played parsed from the printed mm:ss text as minutes * 60 + seconds (so '224:60' gives 13500); null when the feed sent an integer, whose unit is unknown. |
| `field_goals_made` | Int64 | Field goals the player made in the game (two- and three-pointers together; a count). |
| `field_goals_attempted` | Int64 | Field goals the player attempted in the game (two- and three-pointers together; a count). |
| `field_goals_percentage` | Float64 | Field-goal percentage, 0-100, rounded to an integer by the feed |
| `three_pointers_made` | Int64 | Three-pointers made |
| `three_pointers_attempted` | Int64 | Three-pointers attempted |
| `three_pointers_percentage` | Float64 | Three-point percentage, 0-100, rounded to an integer |
| `two_pointers_made` | Int64 | Two-pointers made |
| `two_pointers_attempted` | Int64 | Two-pointers attempted |
| `two_pointers_percentage` | Float64 | Two-point percentage, 0-100, rounded to an integer |
| `free_throws_made` | Int64 | Free throws the player made in the game (a count). |
| `free_throws_attempted` | Int64 | Free throws the player attempted in the game (a count). |
| `free_throws_percentage` | Float64 | Free-throw percentage, 0-100, rounded to an integer |
| `rebounds_defensive` | Int64 | Defensive rebounds |
| `rebounds_offensive` | Int64 | Offensive rebounds |
| `rebounds_total` | Int64 | Total rebounds (offensive plus defensive) the player collected in the game. |
| `assists` | Int64 | Assists credited to the player in the game. |
| `turnovers` | Int64 | Turnovers committed by the player in the game. |
| `steals` | Int64 | Steals credited to the player in the game. |
| `blocks` | Int64 | Shots the player blocked in the game (blocks made, not received). |
| `blocks_received` | Int64 | Own shots blocked by an opponent |
| `fouls_personal` | Int64 | Personal fouls committed |
| `fouls_on` | Int64 | Fouls drawn: personal fouls committed against the player in the game. |
| `points` | Int64 | Points the player scored in the game, free throws included. |
| `points_from_turnovers` | Int64 | Points off turnovers |
| `points_second_chance` | Int64 | Second-chance points |
| `points_fast_break` | Int64 | Fast-break points |
| `points_in_the_paint` | Int64 | Points the player scored in the paint in the game (the feed's sPointsInThePaint). |
| `plus_minus_points` | Int64 | Plus/minus: team point margin while the player was on court |
| `eff_1` | Int64 | Genius efficiency formula 1. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site |
| `eff_2` | Int64 | Genius efficiency formula 2. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site |
| `eff_3` | Float64 | Genius efficiency formula 3. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site |
| `eff_4` | Float64 | Genius efficiency formula 4. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site |
| `eff_5` | Int64 | Genius efficiency formula 5. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site |
| `eff_6` | Int64 | Genius efficiency formula 6. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site |
| `eff_7` | Int64 | Genius efficiency formula 7. The feed carries seven formulas (eff_1..eff_7) with no labels; the definitions are not published. In samples eff_1/eff_6/eff_7 often agree (an index-rating style total), eff_3/eff_4 are decimals. Unverified: do not treat any as PIR/EFF without checking against a league site. Absent in the 2014-15 era sample (WBBL 12248), which carries eff_1..eff_6 on players and no team tot_eff_* |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. The match is not a game of this competition (another org's game, or a cup or play-off of ESAKE), or the scope check could not run (the /u/ shell request failed twice or names no competition). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.genius import esake_schedule, esake_player_box
games = esake_schedule()
esake_player_box(match_id=int(games['match_id'][0]))
```

### esake_schedule {#esake_schedule}

`esake_schedule(season: 'Optional[int]' = None, comp_id: 'Optional[int]' = None, round_number: 'Optional[int]' = -1, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Every match of one Greek Basket League (ESAKE) season (hosted WebStats): ids, status, venue, teams and scores.

Greek Basket League (ESAKE) (men's); season rule: END year; Genius year + 1 (2022-23 = 2023).

Convenience wrapper: a thin binding of `genius_schedule` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: Access: the Genius hosts publish no robots.txt and no terms. CloudFront refuses datacenter IPs on the LiveStats and hosted hosts: from a server set the optional `GENIUS_PROXY` environment variable to an http(s) proxy URL (it is never logged or printed). The wrappers space requests at least one second apart and read only the Genius hosts; keep crawls slow and incremental. Some league sites (the NZ NBL) forbid AI crawlers in their own robots.txt, which does not cover these hosts. The competition is picked from ESAKE's chooser by the label rule include='basket league|gbl' exclude='super cup|woman'; cups and play-offs have their own comp_id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | END-year season (see the season rule above). Omitted = the newest season the org lists. |
| `comp_id` | `Optional[int]` | `None` | Genius competition id; skips the season lookup (needed when the label rule matches several competitions). |
| `round_number` | `Optional[int]` | `-1` | Round to list; -1 (default) lists every round. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the schedule page record (`dict`) when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | From the block id extfix_<id>. Genius match id: the {matchId} of data.json and /u/; one global sequence across every Genius customer (~0.3 M in 2016, ~2.9 M in Oct 2026) |
| `link_match_id` | Int64 | Match id of the block's /match/<id>/summary link; equals match_id (the capture refuses a page where it does not); null when the org hides match pages (FFBB) |
| `comp_id` | Int64 | CompId of the match link, or of the team links when the org hides match pages (FFBB); equals the {compId} asked for. A 'To be determined' slot (no team or match link) takes the {compId} the page's other blocks prove |
| `org` | String | ORG code of the match link (or the team links, as comp_id) |
| `status` | String | Status from the block class STATUS_*: COMPLETE, SCHEDULED, POSTPONED, ... |
| `date_time` | String | Tip-off as displayed, 'Mon D, YYYY, h:mm AM' (local time, zone not stated); null when unscheduled |
| `venue_id` | Int64 | Genius venue id (the /venue/{id} of hosted pages); null when the venue is plain text (FFBB) |
| `venue_name` | String | Venue name as entered (local script) |
| `home_team_id` | Int64 | Home team: Genius team id (warehouse key space, global across orgs and seasons; the /team/{id} of hosted pages, the tid of the widget, the teamId of data/competition). data.json carries no team id: join on name/code |
| `home_team_name` | String | Home team full name |
| `home_team_code` | String | Home team code (local script possible) |
| `home_score` | Int64 | Home points; null before tip-off (typed integer only on a route whose 3 sampled rows were all played) |
| `away_team_id` | Int64 | Away team: Genius team id (warehouse key space, global across orgs and seasons; the /team/{id} of hosted pages, the tid of the widget, the teamId of data/competition). data.json carries no team id: join on name/code |
| `away_team_name` | String | Away team full name |
| `away_team_code` | String | Away team code (local script possible) |
| `away_score` | Int64 | Away points; null before tip-off (typed integer only on a route whose 3 sampled rows were all played) |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). The org lists no matching competition. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The season matches several competitions (pass `comp_id`). |

**Example**

```python
from sportsdataverse.basketball.genius import esake_schedule
esake_schedule(season=2026)
```
