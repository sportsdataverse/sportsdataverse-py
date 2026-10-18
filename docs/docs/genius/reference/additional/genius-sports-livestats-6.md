---
title: "GENIUS — additional Python functions — Genius Sports LiveStats: wbbl_team"
sidebar_label: "Genius Sports LiveStats: wbbl_team"
sidebar_position: 6
description: "GENIUS — additional Python functions — Genius Sports LiveStats: wbbl_team — function reference in sdv-py, the SportsDataverse Python package."
---
# GENIUS — additional Python functions — Genius Sports LiveStats: wbbl_team

### wbbl_team_box {#wbbl_team_box}

`wbbl_team_box(match_id: 'int', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Team box score of a finished Women's British Basketball League game.

Women's British Basketball League (women's); season rule: END year; the label span 2025-26 = 2026.

Convenience wrapper: a thin binding of `genius_team_box` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: Access (sdv-internal-refs/genius-livestats/README.md): the Genius hosts publish no robots.txt and no terms. CloudFront refuses datacenter IPs on the LiveStats and hosted hosts: from a server set the optional `GENIUS_PROXY` environment variable to an http(s) proxy URL (it is never logged or printed). The wrappers space requests at least one second apart and read only the Genius hosts; keep crawls slow and incremental. Some league sites (the NZ NBL) forbid AI crawlers in their own robots.txt, which does not cover these hosts. Genius match ids are one global sequence shared by every customer, and data.json names no competition, so the id is checked on the game's LiveStats shell (/u/WBBL/<match_id>/: one extra request per game, cached per process): its competition name must pass the label rule. genius_team_box(match_id=...) reads any game unchecked. Women's competition data: flagged for the wehoop mirror (Phase C).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `match_id` | `int` |  | Genius match id (match_id in the schedule) of a game of this competition. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the body or page record (`dict` / `list`): `data.json` as served plus `_request` (the match id asked for), or the sdv-internal-refs capture shape of a page when `return_parsed=False`.

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

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Genius has nothing for the request (an unknown match id or ORG, an org without hosted pages, a competition with no games). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed (the CloudFront block, a rate limit, a 5xx), the game is not finished, or the body is for another match, competition or ORG. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The match is not a game of this competition (another org's game, or a cup or play-off of WBBL). |

**Example**

```python
from sportsdataverse.basketball.genius import wbbl_schedule, wbbl_team_box
games = wbbl_schedule()
wbbl_team_box(match_id=int(games['match_id'][0]))
```
