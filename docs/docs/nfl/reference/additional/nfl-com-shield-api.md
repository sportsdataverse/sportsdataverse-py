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

the `gameDetail` object (or the raw envelope when `raw=True`). Empty `dict` if the game has no detail payload.

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
| `game_id` | character | Ten digit identifier for NFL game. |
| `home_team` | character | The home team. Note that this contains the designated home team for games which no team is playing at home such as Super Bowls or NFL International games. |
| `visitor_team` | character | Abbreviation or name of the visiting (away) team in this game. |
| `clockTime` | character | Game clock time at the start of the play, formatted as MM:SS within the quarter. |
| `down` | integer | The down for the given play. |
| `driveNetYards` | integer | Net yards gained on the current drive up to and including this play. |
| `drivePlayCount` | integer | Number of offensive plays run on the current drive up to and including this play. |
| `driveSequenceNumber` | integer | Sequential identifier for the drive within the game, starting at 1. |
| `driveTimeOfPossession` | character | Elapsed time of possession for the current drive, formatted as MM:SS. |
| `endClockTime` | character | Game clock time at the end of the play, formatted as MM:SS within the quarter. |
| `endYardLine` | character | Yard line where the ball was placed at the conclusion of the play. |
| `firstDown` | logical | Indicates whether the play resulted in a first down. |
| `goalToGo` | logical | Indicates whether the offensive team is in a goal-to-go situation at the start of the play. |
| `isBigPlay` | character | Classification label indicating whether this play is designated as a big play by the NFL. |
| `latestPlay` | character | Indicates whether this play is the most recent play recorded in the live feed. |
| `nextPlayIsGoalToGo` | logical | Indicates whether the next play will begin in a goal-to-go situation. |
| `nextPlayType` | character | Play type category anticipated or assigned to the next play in sequence. |
| `orderSequence` | integer | Sequential ordering value used to sort plays within the game in chronological order. |
| `penaltyOnPlay` | logical | Indicates whether a penalty was called on this play. |
| `playClock` | integer | Play clock value in seconds at the snap of the ball. |
| `playReviewStatus` | character | Status of any official review of the play (e.g., confirmed, overturned, stands). |
| `playDeleted` | logical | Indicates whether this play record has been marked as deleted or voided. |
| `playDescription` | character | Official text description of the play as provided by the NFL. |
| `playDescriptionWithJerseyNumbers` | character | Play description text augmented with player jersey numbers for participant identification. |
| `playId` | integer | Unique play event identifier (UUID). |
| `playStats` | integer | Internal NFL stat identifier linking this play to associated statistical records. |
| `playType` | character | Categorical classification of the play type (e.g., PASS, RUSH, PUNT, KICKOFF). |
| `prePlayByPlay` | character | Narrative text describing the game situation or setup immediately before this play. |
| `quarter` | integer | Game quarter in which the play occurred (1–4 for regulation, 5 for overtime). |
| `scoringPlay` | logical | Indicates whether this play resulted in points being scored. |
| `scoringPlayType` | character | Type of scoring event on this play (e.g., TD, FG, SAFETY, PAT). |
| `scoringTeam` | character | Abbreviation or identifier of the team that scored on this play, if applicable. |
| `shortDescription` | character | Short narrative summary of the play outcome (e.g., 'PENALTY', 'TOUCHDOWN'). |
| `specialTeamsPlay` | logical | Indicates whether this play was a special teams play. |
| `stPlayType` | character | Special teams play sub-type classification (e.g., PUNT, KICKOFF, FG_ATTEMPT) when applicable. |
| `timeOfDay` | character | Wall-clock time of day when the play occurred, typically in local or Eastern time. |
| `yardLine` | character | Yard line on the field where the ball was placed at the start of the play. |
| `yards` | integer | The number of receiving yards |
| `yardsToGo` | integer | Number of yards needed to gain a first down at the start of the play. |
| `possessionTeam_id` | character | NFL.com Shield API identifier for the team with offensive possession on this play. |
| `possessionTeam_abbreviation` | character | Abbreviated team name for the team with offensive possession on this play. |
| `possessionTeam_nickName` | character | Nickname (mascot name) of the team with offensive possession on this play. |
| `possessionTeam_franchise_primaryColor` | character | Primary brand color for the possessing team's franchise, expressed as a hex color code. |
| `possessionTeam_franchise_secondaryColor` | character | Secondary brand color for the possessing team's franchise, expressed as a hex color code. |
| `possessionTeam_franchise_tertiaryColor` | character | Tertiary brand color for the possessing team's franchise, expressed as a hex color code. |
| `possessionTeam_franchise_currentLogo` | character | URL of the current primary logo for the possessing team's franchise. |
| `possessionTeam_franchise_largeTypeColor` | character | Large-type display color for the possessing team's franchise branding, as a hex code. |
| `possessionTeam_franchise_decorativeElementsColor` | character | Decorative elements color for the possessing team's franchise branding, as a hex code. |
| `scoringTeam_id` | character | NFL.com Shield API identifier for the team that scored on this play. |
| `scoringTeam_abbreviation` | character | Abbreviated team name for the team that scored on this play. |
| `scoringTeam_nickName` | character | Nickname (mascot name) of the team that scored on this play. |
| `scoringTeam_franchise_primaryColor` | character | Primary brand color for the scoring team's franchise, expressed as a hex color code. |
| `scoringTeam_franchise_secondaryColor` | character | Secondary brand color for the scoring team's franchise, expressed as a hex color code. |
| `scoringTeam_franchise_tertiaryColor` | character | Tertiary brand color for the scoring team's franchise, expressed as a hex color code. |
| `scoringTeam_franchise_currentLogo` | character | URL of the current primary logo for the scoring team's franchise. |
| `scoringTeam_franchise_largeTypeColor` | character | Large-type display color for the scoring team's franchise branding, as a hex code. |
| `scoringTeam_franchise_decorativeElementsColor` | character | Decorative elements color for the scoring team's franchise branding, as a hex code. |

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
