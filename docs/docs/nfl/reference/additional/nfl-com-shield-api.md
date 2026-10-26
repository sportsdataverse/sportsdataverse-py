---
title: "NFL — additional Python functions — NFL.com Shield API"
sidebar_label: "NFL.com Shield API"
sidebar_position: 2
description: "NFL — additional Python functions — NFL.com Shield API — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — NFL.com Shield API

### nfl_clear_token_cache {#nfl_clear_token_cache}

`nfl_clear_token_cache() -> 'None'`

Drop the cached `api.nfl.com` token (forces a fresh mint on the next call).

### nfl_game_details {#nfl_game_details}

`nfl_game_details(game_id: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, raw: 'bool' = False) -> 'Dict'`

Pull full `api.nfl.com` game details (drives + plays) by game id.

Hits `/experience/v1/gamedetails/{game_id}`; the payload is the shield
`data.viewer.gameDetail` object (plays, drives, scoring summaries, line
scores, possession, weather, attendance, ...).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` | `None` | the uuid game id from `nfl_game_schedule` (e.g. `'7d3e8f84-1312-11ef-afd1-646009f18b2e'`). |
| `headers` | `Dict[str, str] \| None` | `None` | Pre-built header dict. Defaults to a fresh `nfl_headers_gen` call. |
| `raw` | `bool` | `False` | If True, return the full envelope (`{"data": {...}}`) untouched. If False (default), unwrap to the `gameDetail` object. |

**Returns**

the `gameDetail` object (or the raw envelope when `raw=True`). Empty `dict` if `gameDetail` is `null` (the game has no detail payload).

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | Api.nfl.com answered 404. |
| `ValueError` | Api.nfl.com answered 400 / 422. |
| `AssetFetchError` | Any other non-2xx or a connection failure after retries (the token mint's included), a 2xx whose body is not a JSON object or array, or a body with no `data.viewer.gameDetail` or carrying any GraphQL `errors` list, even alongside a populated `gameDetail` (a partial answer is an unknown one). |

**Example**

```python
from sportsdataverse.nfl.nfl_games import nfl_game_details
detail = nfl_game_details(game_id="7d3e8f84-1312-11ef-afd1-646009f18b2e")
len(detail["plays"]), len(detail["drives"])

# Reuse headers across many calls (avoids re-minting tokens)

from sportsdataverse.nfl.nfl_games import nfl_game_details, nfl_headers_gen
hdrs = nfl_headers_gen()
detail = nfl_game_details(game_id="7d3e8f84-1312-11ef-afd1-646009f18b2e", headers=hdrs)
```

### nfl_game_pbp {#nfl_game_pbp}

`nfl_game_pbp(game_id: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_as_pandas: 'bool' = False)`

Parsed `api.nfl.com` play-by-play -- one row per play (polars/pandas frame).

Tidy wrapper over `nfl_game_details`: flattens `gameDetail.plays` into a
DataFrame (`playId`, `quarter`, `down`, `yardsToGo`, `yardLine`,
`playType`, `playDescription`, `possessionTeam_*`, ...) and prepends the
game context (`game_id`, `home_team`, `visitor_team`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` | `None` | uuid game id from `nfl_week_games` / `nfl_game_schedule`. |
| `headers` | `Optional[Dict[str, str]]` | `None` | reuse a `nfl_headers_gen` dict. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per play (empty frame if the game has no play-by-play yet).

| col_name | type | description |
|---|---|---|
| `game_id` | String | Ten digit identifier for NFL game. |
| `home_team` | String | The home team. Note that this contains the designated home team for games which no team is playing at home such as Super Bowls or NFL International games. |
| `visitor_team` | String | Abbreviation or name of the visiting (away) team in this game. |
| `clockTime` | String | Game clock time at the start of the play, formatted as MM:SS within the quarter. |
| `down` | Int64 | The down for the given play. |
| `driveNetYards` | Int64 | Net yards gained on the current drive up to and including this play. |
| `drivePlayCount` | Int64 | Number of offensive plays run on the current drive up to and including this play. |
| `driveSequenceNumber` | Int64 | Sequential identifier for the drive within the game, starting at 1. |
| `driveTimeOfPossession` | String | Elapsed time of possession for the current drive, formatted as MM:SS. |
| `endClockTime` | String | Game clock time at the end of the play, formatted as MM:SS within the quarter. |
| `endYardLine` | String | Yard line where the ball was placed at the conclusion of the play. |
| `firstDown` | Boolean | Indicates whether the play resulted in a first down. |
| `goalToGo` | Boolean | Indicates whether the offensive team is in a goal-to-go situation at the start of the play. |
| `isBigPlay` | String | Classification label indicating whether this play is designated as a big play by the NFL. |
| `latestPlay` | String | Indicates whether this play is the most recent play recorded in the live feed. |
| `nextPlayIsGoalToGo` | Boolean | Indicates whether the next play will begin in a goal-to-go situation. |
| `nextPlayType` | String | Play type category anticipated or assigned to the next play in sequence. |
| `orderSequence` | Int64 | Sequential ordering value used to sort plays within the game in chronological order. |
| `penaltyOnPlay` | Boolean | Indicates whether a penalty was called on this play. |
| `playClock` | Int64 | Play clock value in seconds at the snap of the ball. |
| `playReviewStatus` | String | Status of any official review of the play (e.g., confirmed, overturned, stands). |
| `playDeleted` | Boolean | Indicates whether this play record has been marked as deleted or voided. |
| `playDescription` | String | Official text description of the play as provided by the NFL. |
| `playDescriptionWithJerseyNumbers` | String | Play description text augmented with player jersey numbers for participant identification. |
| `playId` | Int64 |  |
| `playStats` | Int64 | Internal NFL stat identifier linking this play to associated statistical records. |
| `playType` | String | Categorical classification of the play type (e.g., PASS, RUSH, PUNT, KICKOFF). |
| `prePlayByPlay` | String | Narrative text describing the game situation or setup immediately before this play. |
| `quarter` | Int64 | Game quarter in which the play occurred (1–4 for regulation, 5 for overtime). |
| `scoringPlay` | Boolean | Indicates whether this play resulted in points being scored. |
| `scoringPlayType` | String | Type of scoring event on this play (e.g., TD, FG, SAFETY, PAT). |
| `scoringTeam` | String | Abbreviation or identifier of the team that scored on this play, if applicable. |
| `shortDescription` | String | Short narrative summary of the play outcome (e.g., 'PENALTY', 'TOUCHDOWN'). |
| `specialTeamsPlay` | Boolean | Indicates whether this play was a special teams play. |
| `stPlayType` | String | Special teams play sub-type classification (e.g., PUNT, KICKOFF, FG_ATTEMPT) when applicable. |
| `timeOfDay` | String | Wall-clock time of day when the play occurred, typically in local or Eastern time. |
| `yardLine` | String | Yard line on the field where the ball was placed at the start of the play. |
| `yards` | Int64 | The number of receiving yards |
| `yardsToGo` | Int64 | Number of yards needed to gain a first down at the start of the play. |
| `possessionTeam_id` | String | NFL.com Shield API identifier for the team with offensive possession on this play. |
| `possessionTeam_abbreviation` | String | Abbreviated team name for the team with offensive possession on this play. |
| `possessionTeam_nickName` | String | Nickname (mascot name) of the team with offensive possession on this play. |
| `possessionTeam_franchise_primaryColor` | String | Primary brand color for the possessing team's franchise, expressed as a hex color code. |
| `possessionTeam_franchise_secondaryColor` | String | Secondary brand color for the possessing team's franchise, expressed as a hex color code. |
| `possessionTeam_franchise_tertiaryColor` | String | Tertiary brand color for the possessing team's franchise, expressed as a hex color code. |
| `possessionTeam_franchise_currentLogo` | String | URL of the current primary logo for the possessing team's franchise. |
| `possessionTeam_franchise_largeTypeColor` | String | Large-type display color for the possessing team's franchise branding, as a hex code. |
| `possessionTeam_franchise_decorativeElementsColor` | String | Decorative elements color for the possessing team's franchise branding, as a hex code. |
| `scoringTeam_id` | String | NFL.com Shield API identifier for the team that scored on this play. |
| `scoringTeam_abbreviation` | String | Abbreviated team name for the team that scored on this play. |
| `scoringTeam_nickName` | String | Nickname (mascot name) of the team that scored on this play. |
| `scoringTeam_franchise_primaryColor` | String | Primary brand color for the scoring team's franchise, expressed as a hex color code. |
| `scoringTeam_franchise_secondaryColor` | String | Secondary brand color for the scoring team's franchise, expressed as a hex color code. |
| `scoringTeam_franchise_tertiaryColor` | String | Tertiary brand color for the scoring team's franchise, expressed as a hex color code. |
| `scoringTeam_franchise_currentLogo` | String | URL of the current primary logo for the scoring team's franchise. |
| `scoringTeam_franchise_largeTypeColor` | String | Large-type display color for the scoring team's franchise branding, as a hex code. |
| `scoringTeam_franchise_decorativeElementsColor` | String | Decorative elements color for the scoring team's franchise branding, as a hex code. |

**Example**

```python
from sportsdataverse.nfl import nfl_game_pbp
pbp = nfl_game_pbp(game_id="7d3e8f84-1312-11ef-afd1-646009f18b2e")
pbp.select(["quarter", "down", "yardsToGo", "playType", "playDescription"]).head()
```

### nfl_game_schedule {#nfl_game_schedule}

`nfl_game_schedule(season: 'int' = 2024, season_type: 'str' = 'REG', week: 'int' = 1, headers: 'Optional[Dict[str, str]]' = None, raw: 'bool' = False) -> 'Dict'`

List `api.nfl.com` games for a season/week slice (`/football/v2/games`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | season year (e.g. `2024`). |
| `season_type` | `str` | `'REG'` | season type. One of `"PRE"`, `"REG"`, `"POST"`. |
| `week` | `int` | `1` | week number (1-18 regular season, 1-4 post-season). |
| `headers` | `Dict[str, str] \| None` | `None` | Pre-built header dict (skip the auth roundtrip). Defaults to a fresh `nfl_headers_gen` call. |
| `raw` | `bool` | `False` | currently ignored; the function returns the parsed JSON payload. |

**Returns**

payload with the games list under `"games"` plus `"pagination"`. Each game carries `id` (the uuid game id used by `nfl_game_details`), `homeTeam`/`awayTeam`, `date`, `status`, `externalIds` (gsis etc.).

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | Api.nfl.com answered 404. |
| `ValueError` | Api.nfl.com answered 400 / 422. |
| `AssetFetchError` | Any other non-2xx or a connection failure after retries (the token mint's included), or a 2xx whose body is not a JSON object or array. |

**Example**

```python
from sportsdataverse.nfl.nfl_games import nfl_game_schedule
week_one = nfl_game_schedule(season=2024, season_type="REG", week=1)
first_id = week_one["games"][0]["id"]
```

### nfl_headers_gen {#nfl_headers_gen}

`nfl_headers_gen(token: 'Optional[str]' = None) -> 'Dict[str, str]'`

Build the request-header dict expected by `api.nfl.com`.

Obtains a bearer token via `nfl_token_gen` (which caches + auto-renews,
or honors `NFL_ACCESS_TOKEN`) unless `token` is supplied, and combines it
with the browser-style headers the NFL.com web app sends. Token caching already
avoids re-minting, so callers rarely need to thread `token`/`headers` by hand.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `token` | `Optional[str]` | `None` | An existing access token to reuse; uses the cached/minted one when `None`. |

**Returns**

Header dict ready to drop into `requests.get`.

**Example**

```python
from sportsdataverse.nfl.nfl_games import nfl_headers_gen, nfl_game_schedule
hdrs = nfl_headers_gen()
week_one = nfl_game_schedule(season=2024, season_type="REG", week=1, headers=hdrs)
week_two = nfl_game_schedule(season=2024, season_type="REG", week=2, headers=hdrs)
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

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `/identity/v3/token` answered 400 / 422 (the credentials are rejected). |
| `NoDataError` | `/identity/v3/token` answered 404. |
| `AssetFetchError` | Any other non-2xx or a connection failure while minting, or a 2xx body without a string `accessToken`. |

**Example**

```python
from sportsdataverse.nfl.nfl_games import nfl_token_gen
token = nfl_token_gen()                # mints + caches
assert nfl_token_gen() == token        # served from cache
assert isinstance(token, str) and token.startswith("ey")
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
| `id` | String | ID of the player in the 'name' column. |
| `category` | String | Broader category of player positions |
| `date` | String |  |
| `time` | String | Time at start of play provided in string format as minutes:seconds remaining in the quarter. |
| `gameType` | String |  |
| `international` | Boolean | Boolean flag indicating whether this game is designated as an international (outside the United States) game. |
| `neutralSite` | Boolean |  |
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `seasonType` | String | Phase of the season in which this game takes place (e.g., 'REG', 'POST', 'PRE'). |
| `status` | String |  |
| `week` | Int64 | Season week. |
| `weekType` | String | Classification of the week type within the season (e.g., 'REG', 'WC', 'DIV', 'CONF', 'SB'). |
| `externalIds` | String | Serialized list of external system identifiers (e.g., partner IDs, league IDs) mapped to this game. |
| `ticketUrl` | String | URL to the official ticket purchasing page for this game. |
| `ticketVendors` | String | Serialized list of authorized ticket vendors or marketplaces for this game. |
| `extensions` | String | Serialized JSON or string of additional extension metadata attached to the game record by the NFL Shield API. |
| `version` | Int64 | API response version integer returned by the NFL Shield API for this game record. |
| `homeTeam_id` | String | NFL Shield team identifier for the home team. |
| `homeTeam_currentLogo` | String | URL for the home team's current primary logo image as served by the NFL Shield API. |
| `homeTeam_fullName` | String | Full name of the home team (e.g., 'Dallas Cowboys'). |
| `awayTeam_id` | String | NFL Shield team identifier for the away team. |
| `awayTeam_currentLogo` | String | URL for the away team's current primary logo image as served by the NFL Shield API. |
| `awayTeam_fullName` | String | Full name of the away team (e.g., 'Kansas City Chiefs'). |
| `broadcastInfo_homeNetworkChannels` | String | Serialized list of TV/streaming channels designated as the home team's local broadcast. |
| `broadcastInfo_awayNetworkChannels` | String | Serialized list of TV/streaming channels designated as the away team's local broadcast. |
| `broadcastInfo_internationalWatchOptions` | String | Serialized list of international streaming or broadcast options for viewers outside the United States. |
| `broadcastInfo_streamingNetworks` | String | Serialized list of streaming platforms (e.g., Peacock, Amazon Prime Video) carrying the game. |
| `broadcastInfo_territory` | String | Geographic territory or market designation for which this broadcast record applies. |
| `broadcastInfo_audioNetworks` | String | Serialized list of radio networks carrying the game's audio broadcast. |
| `venue_id` | String |  |
| `venue_name` | String |  |
| `venue_city` | String |  |
| `venue_country` | String | Country name or ISO code indicating where the game's venue is located. |

**Example**

```python
from sportsdataverse.nfl import nfl_week_games
sched = nfl_week_games(season=2024, season_type="REG", week=1)
sched.select(["id", "homeTeam_fullName", "awayTeam_fullName"]).head()
```
