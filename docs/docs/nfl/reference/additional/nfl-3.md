---
title: "NFL — additional Python functions — Nfl (3)"
sidebar_label: "Nfl (3)"
sidebar_position: 10
description: "NFL — additional Python functions — Nfl (3) — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Nfl (3)

### nfl_simulations {#nfl_simulations}

`nfl_simulations(games: 'pl.DataFrame', compute_results: 'Optional[ComputeResultsFn]' = None, *, simulations: 'int' = 10000, playoff_seeds: 'int' = 7, byes_per_conf: 'int' = 1, tiebreaker_depth: 'str' = 'SOS', sim_include: 'str' = 'DRAFT', seed: 'Optional[int]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Dict[str, Union[pl.DataFrame, 'pd.DataFrame']]"`

Simulate an NFL season from a schedule with (partially) missing results.

Faithful port of `nflseedR::nfl_simulations()` +
`simulate_chunk()` (simulations.R L140-409,
simulations_simulate_chunks.R L1-284). Missing regular season results
are filled week by week via `compute_results`; standings, division
ranks and playoff seeds are then computed with the full NFL tiebreakers,
the postseason is simulated round by round (with reseeding and
`byes_per_conf` byes), and the draft order is derived. nflseedR's
furrr chunking is replaced by one vectorized pass over all simulated
seasons, so there is no `chunks` argument; reproducibility comes from
`seed`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | Schedule frame for ONE season with columns `sim` or `season`, `game_type`, `week`, `away_team`, `home_team`, `away_rest`, `home_rest`, `location`, and `result` (home margin; missing = not yet played). |
| `compute_results` | `Optional[ComputeResultsFn]` | `None` | Function filling results for one week, called as `compute_results(teams, games, week_num, rng=rng, **kwargs)` and returning `{"teams": ..., "games": ...}`. Defaults to `nfl_compute_results` (dynamic ELO + Normal(estimate, 13) margins). Must only fill results where `week == week_num` and `result` is missing, and must not produce postseason ties. |
| `simulations` | `int` | `10000` | Number of seasons to simulate. |
| `playoff_seeds` | `int` | `7` | Number of playoff seeds per conference. |
| `byes_per_conf` | `int` | `1` | First-round byes per conference (drives the number of wildcard games). |
| `tiebreaker_depth` | `str` | `'SOS'` | `'SOS'` (default), `'PRE-SOV'`, or `'RANDOM'` (`'POINTS'` is unavailable because simulated games carry margins, not scores). |
| `sim_include` | `str` | `'DRAFT'` | `'REG'` (standings/seeds only), `'POST'` (+ postseason), or `'DRAFT'` (default; + draft order). |
| `seed` | `Optional[int]` | `None` | Seed for the numpy RNG driving results and coin tosses. |
| `return_as_pandas` | `bool` | `False` | If `True`, return pandas DataFrames. |

**Returns**

Dict of frames mirroring the nflseedR simulation list: `standings` (one row per sim x team), `games` (all simulated games), `overall` (per-team probabilities: wins, playoff, div1, seed1, won_conf, won_sb, draft1, draft5), `team_wins` (over/under probabilities vs. half-win lines), and `game_summary` (per-matchup home/away win rates).

| col_name | type | description |
|---|---|---|
| `standings.sim` | integer | Simulated season identifier (1 through `simulations`). |
| `standings.conf` | character | Conference of the team (AFC or NFC). |
| `standings.division` | character | Division of the team (e.g. "AFC East"). |
| `standings.team` | character | Team abbreviation. |
| `standings.games` | integer | Number of regular season games played in the simulated season. |
| `standings.wins` | double | Regular season wins in the simulated season with ties counted as half a win. |
| `standings.true_wins` | integer | Regular season wins in the simulated season excluding ties. |
| `standings.losses` | integer | Regular season losses in the simulated season. |
| `standings.ties` | integer | Regular season ties in the simulated season. |
| `standings.win_pct` | double | Regular season win percentage in the simulated season with ties counted as half a win. |
| `standings.div_pct` | double | Win percentage against division opponents in the simulated season (0 when no division games). |
| `standings.conf_pct` | double | Win percentage against conference opponents in the simulated season (0 when no conference games). |
| `standings.sov` | double | Strength of victory in the simulated season - combined win percentage of all defeated opponents. |
| `standings.sos` | double | Strength of schedule in the simulated season - combined win percentage of all opponents faced. |
| `standings.div_rank` | integer | Division rank (1-4) in the simulated season after the NFL division tiebreakers. |
| `standings.div_tie_broken_by` | character | Tiebreaker step that resolved the division rank in this simulated season; null when no tiebreaker was needed. |
| `standings.conf_rank` | integer | Conference rank (playoff seed) in the simulated season after the NFL conference tiebreakers; null beyond `playoff_seeds`. |
| `standings.conf_tie_broken_by` | character | Tiebreaker step that resolved the conference rank in this simulated season; null when no tiebreaker was needed. |
| `standings.exit` | character | Round of the team's final game in the simulated season - REG, WC, DIV, CON, SB, or SB_WIN for the Super Bowl winner. |
| `standings.draft_rank` | integer | Draft pick position (1 = first overall) in the simulated season (present when sim_include="DRAFT"). |
| `standings.draft_tie_broken_by` | character | Tiebreaker step that resolved the draft rank in this simulated season; null when no tiebreaker was needed. |
| `games.sim` | integer | Simulated season identifier the game row belongs to. |
| `games.game_type` | character | Game type - REG for regular season or the playoff round (WC, DIV, CON, SB). |
| `games.week` | integer | Week number of the game; simulated playoff rounds are numbered from the last regular season week (+1 for WC through +4 for SB). |
| `games.away_team` | character | Team abbreviation of the away team (simulated playoff matchups are filled by seed). |
| `games.home_team` | character | Team abbreviation of the home team (simulated playoff matchups are filled by seed). |
| `games.away_rest` | integer | Days of rest for the away team before the game. |
| `games.home_rest` | integer | Days of rest for the home team before the game (14 for the top seed's divisional round game). |
| `games.location` | character | Game site indicator - "Home" or "Neutral" (Super Bowl). |
| `games.result` | integer | Home margin (home score minus away score); real where the input schedule had one, simulated otherwise. |
| `overall.conf` | character | Conference of the team (AFC or NFC). |
| `overall.division` | character | Division of the team (e.g. "AFC East"). |
| `overall.team` | character | Team abbreviation. |
| `overall.wins` | double | Mean regular season wins across all simulated seasons (ties counted as half a win). |
| `overall.playoff` | double | Share of simulated seasons in which the team made the playoffs (conference rank within `playoff_seeds`). |
| `overall.div1` | double | Share of simulated seasons in which the team won its division. |
| `overall.seed1` | double | Share of simulated seasons in which the team earned the conference number one seed. |
| `overall.won_conf` | double | Share of simulated seasons in which the team won the conference championship; null when sim_include="REG". |
| `overall.won_sb` | double | Share of simulated seasons in which the team won the Super Bowl; null when sim_include="REG". |
| `overall.draft1` | double | Share of simulated seasons in which the team held the first overall draft pick; null unless sim_include="DRAFT". |
| `overall.draft5` | double | Share of simulated seasons in which the team held a top-five draft pick; null unless sim_include="DRAFT". |
| `team_wins.team` | character | Team abbreviation. |
| `team_wins.wins` | double | Half-win line the over/under probabilities are evaluated against (0, 0.5, ... up to the number of regular season games). |
| `team_wins.over_prob` | double | Probability across simulated seasons that the team's outright win total exceeds the line. |
| `team_wins.under_prob` | double | Probability across simulated seasons that the team's outright win total falls below the line (exact pushes are the remainder). |
| `game_summary.game_type` | character | Game type of the matchup - REG for regular season or the playoff round (WC, DIV, CON, SB). |
| `game_summary.week` | integer | Week number of the matchup. |
| `game_summary.away_team` | character | Team abbreviation of the away team in the matchup. |
| `game_summary.home_team` | character | Team abbreviation of the home team in the matchup. |
| `game_summary.away_wins` | integer | Number of simulated seasons in which the away team won the matchup. |
| `game_summary.home_wins` | integer | Number of simulated seasons in which the home team won the matchup. |
| `game_summary.ties` | integer | Number of simulated seasons in which the matchup ended in a tie. |
| `game_summary.result` | double | Mean home margin of the matchup across the simulated seasons in which it was played. |
| `game_summary.games_played` | integer | Number of simulated seasons in which this exact matchup occurred (playoff pairings only arise in the simulations that produce them). |
| `game_summary.away_percentage` | double | Share of played simulations won by the away team, with ties counted as half a win. |
| `game_summary.home_percentage` | double | Share of played simulations won by the home team, with ties counted as half a win. |

**Example**

```python
import sportsdataverse.nfl as nfl
games = nfl.load_schedules([2024])
sim = nfl.nfl_simulations(games, simulations=1000, seed=42)
print(sim["overall"].head())

# Custom initial ELO ratings

sim = nfl.nfl_simulations(games, simulations=500, seed=1,
                          elo={"KC": 1700, "BUF": 1650})

# Pipeline next step (one line)

sim["overall"].sort("won_sb", descending=True).head()
```

### nfl_special_teams_epa {#nfl_special_teams_epa}

`nfl_special_teams_epa(seasons: 'Union[int, List[int]]', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Special-teams EPA by team-unit.

Units: `punt` / `punt_return` / `kickoff` / `kickoff_return` /
`field_goal` / `extra_point`.  On each punt/kickoff the kicking
team's unit carries the play EPA signed to the kicking team and the
return team's unit its negation, so a team's units sum to its total
ST-play EPA.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, List[int]]` |  | Season or list of seasons. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

Per `(season, team, unit)`: `plays`, `epa`, `epa_per_play`. Empty seasons yield a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the aggregate. |
| `team` | character | Team abbreviation. |
| `unit` | character | Special-teams unit (punt, punt_return, kickoff, kickoff_return, field_goal, extra_point). |
| `plays` | integer | Plays credited to the unit. |
| `epa` | double | Total EPA credited to the unit (kicking team carries the play EPA signed to it; the return team carries its negation). |
| `epa_per_play` | double | EPA per play for the unit. |

**Example**

```python
from sportsdataverse.nfl.nfl_special_teams import nfl_special_teams_epa
st = nfl_special_teams_epa([2023])
print(st.filter(pl.col("unit") == "punt").sort("epa", descending=True).head())
```

### nfl_token_gen {#nfl_token_gen}

`nfl_token_gen(client_key: 'Optional[str]' = None, client_secret: 'Optional[str]' = None, force_refresh: 'bool' = False) -> 'str'`

Return a valid `api.nfl.com` bearer token, minting + caching as needed.

The token is cached in-process and reused until ~2 min before its own JWT
`exp`, then transparently re-minted -- so callers never have to think about
expiry or refresh. The first call (or any call after expiry / `force_refresh`)
mints a fresh token via the anonymous device-token grant at `/identity/v3/token`.

Resolution order (all overrides optional):

1. `NFL_ACCESS_TOKEN` env var -- returned verbatim, skipping minting and
   caching (you supply + manage the token). Ignored if explicit credentials
   are passed.
2. Credentials: explicit `client_key`/`client_secret` args ->
   `NFL_CLIENT_KEY`/`NFL_CLIENT_SECRET` env vars -> the bundled public
   `WEB_DESKTOP` web-app pair.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `client_key` | `Optional[str]` | `None` | Override the client key (else env var, else the web default). |
| `client_secret` | `Optional[str]` | `None` | Override the client secret (else env var, else the default). |
| `force_refresh` | `bool` | `False` | Mint a new token even if a cached one is still valid. |

**Returns**

The bearer `accessToken` string.

**Example**

```python
from sportsdataverse.nfl.nfl_games import nfl_token_gen
token = nfl_token_gen()                # mints + caches
assert nfl_token_gen() == token        # served from cache
assert isinstance(token, str) and token.startswith("ey")
```

### nfl_usage_projection {#nfl_usage_projection}

`nfl_usage_projection(seasons: 'List[int]', target_season: 'int', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Project next-season target share, air-yards share, and WOPR.

Projects each player's shares via the shared Marcel blend
(`sportsdataverse.nfl.nfl_projection._marcel_blend` — the same
recency/shrinkage engine as the rate projection), assigns each player to
their most recent team, **renormalizes shares within each projected team to
sum to 1.0** (the share invariant), and converts shares to volumes with a
team-level carry-forward of pass attempts (team targets) and air yards.
As-of-date clean: only seasons strictly before `target_season` are used.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | History seasons to load. |
| `target_season` | `int` |  | The season being projected. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. |

**Returns**

`player_id:Utf8, target_season:Int64, position_group:Utf8, proj_team:Utf8, proj_target_share:Float64, proj_air_yards_share:Float64, proj_wopr:Float64, proj_targets:Float64, proj_air_yards:Float64`. Empty history returns a zero-row frame.

| col_name | type | description |
|---|---|---|
| `player_id` | character | nflverse gsis player id (character join key). |
| `target_season` | integer | The season being projected (features use strictly earlier seasons only). |
| `position_group` | character | nflverse offensive position group. |
| `proj_team` | character | Most recent team (max season, tiebreak most targets) - the renormalization group. |
| `proj_target_share` | double | Projected share of team targets - Marcel share blend renormalized to sum to 1.0 within proj_team. |
| `proj_air_yards_share` | double | Projected share of team air yards, renormalized within proj_team. |
| `proj_wopr` | double | Projected weighted opportunity rating - 1.5 x proj_target_share + 0.7 x proj_air_yards_share. |
| `proj_targets` | double | Projected targets - proj_target_share x team pass-target carry-forward. |
| `proj_air_yards` | double | Projected receiving air yards - proj_air_yards_share x team air-yards carry-forward. |

**Example**

```python
from sportsdataverse.nfl.nfl_usage_projection import nfl_usage_projection
usage = nfl_usage_projection([2021, 2022, 2023], 2024)
usage.sort("proj_wopr", descending=True).head()
```

### nfl_week_games {#nfl_week_games}

`nfl_week_games(season: 'int' = 2024, season_type: 'str' = 'REG', week: 'int' = 1, headers: 'Optional[Dict[str, str]]' = None, return_as_pandas: 'bool' = False)`

Parsed `api.nfl.com` week schedule -- one row per game (polars/pandas frame).

Tidy wrapper over `nfl_game_schedule`: flattens the `games` list into a
DataFrame with `id` (uuid game id), `season`/`seasonType`/`week`,
`date`, `status_*`, and `homeTeam_*` / `awayTeam_*` columns.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | season year. season_type (str): `"PRE"`/`"REG"`/`"POST"`. |
| `season_type` | `str` | `'REG'` |  |
| `week` | `int` | `1` | week number. headers: reuse a `nfl_headers_gen` dict. |
| `headers` | `Optional[Dict[str, str]]` | `None` |  |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per game.

| col_name | type | description |
|---|---|---|
| `id` | character | ID of the player in the 'name' column. |
| `category` | character | Broader category of player positions |
| `date` | character | Date of the poll release. |
| `time` | character | Time at start of play provided in string format as minutes:seconds remaining in the quarter. |
| `gameType` | character | Game type identifier (3 for playoffs). |
| `international` | logical | Boolean flag indicating whether this game is designated as an international (outside the United States) game. |
| `neutralSite` | logical | Whether the game is at a neutral site. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `seasonType` | character | Phase of the season in which this game takes place (e.g., 'REG', 'POST', 'PRE'). |
| `status` | character | Game status (e.g. "scheduled", "in_progress", "completed"). |
| `week` | integer | Season week. |
| `weekType` | character | Classification of the week type within the season (e.g., 'REG', 'WC', 'DIV', 'CONF', 'SB'). |
| `externalIds` | character | Serialized list of external system identifiers (e.g., partner IDs, league IDs) mapped to this game. |
| `ticketUrl` | character | URL to the official ticket purchasing page for this game. |
| `ticketVendors` | character | Serialized list of authorized ticket vendors or marketplaces for this game. |
| `extensions` | character | Serialized JSON or string of additional extension metadata attached to the game record by the NFL Shield API. |
| `version` | integer | API response version integer returned by the NFL Shield API for this game record. |
| `homeTeam_id` | character | NFL Shield team identifier for the home team. |
| `homeTeam_currentLogo` | character | URL for the home team's current primary logo image as served by the NFL Shield API. |
| `homeTeam_fullName` | character | Full name of the home team (e.g., 'Dallas Cowboys'). |
| `awayTeam_id` | character | NFL Shield team identifier for the away team. |
| `awayTeam_currentLogo` | character | URL for the away team's current primary logo image as served by the NFL Shield API. |
| `awayTeam_fullName` | character | Full name of the away team (e.g., 'Kansas City Chiefs'). |
| `broadcastInfo_homeNetworkChannels` | character | Serialized list of TV/streaming channels designated as the home team's local broadcast. |
| `broadcastInfo_awayNetworkChannels` | character | Serialized list of TV/streaming channels designated as the away team's local broadcast. |
| `broadcastInfo_internationalWatchOptions` | character | Serialized list of international streaming or broadcast options for viewers outside the United States. |
| `broadcastInfo_streamingNetworks` | character | Serialized list of streaming platforms (e.g., Peacock, Amazon Prime Video) carrying the game. |
| `broadcastInfo_territory` | character | Geographic territory or market designation for which this broadcast record applies. |
| `broadcastInfo_audioNetworks` | character | Serialized list of radio networks carrying the game's audio broadcast. |
| `venue_id` | character | Referencing venue id. |
| `venue_name` | character | Full name of the franchise's venue. |
| `venue_city` | character | City where the venue is located. |
| `venue_country` | character | Country name or ISO code indicating where the game's venue is located. |

**Example**

```python
from sportsdataverse.nfl import nfl_week_games
sched = nfl_week_games(season=2024, season_type="REG", week=1)
sched.select(["id", "homeTeam_fullName", "awayTeam_fullName"]).head()
```
