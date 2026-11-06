# NBA — NBA Stats API (stats.nba.com) — League: leaguestandings–leaguestandingsv3

> NBA — NBA Stats API (stats.nba.com) — League: leaguestandings–leaguestandingsv3 — function reference in sdv-py, the SportsDataverse Python package.

## nba_stats_leaguestandings

League standings.

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguestandings`

**Valid URL:** [https://stats.nba.com/stats/leaguestandings?LeagueID=00&Season=2024-25&SeasonType=Regular+Season&SeasonYear=](https://stats.nba.com/stats/leaguestandings?LeagueID=00&Season=2024-25&SeasonType=Regular+Season&SeasonYear=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. An NBA season tips off in late October and becomes the default in November (``2025-26`` through October 2026, ``2026-27`` from November 2026); a G League season (regular season from late December) in January; a Summer League (played in July, which stats.nba.com labels ``2026-27`` in 2026) in August; a draft combine (May) in June; a draft (``drafthistory``, a year; late June) in July. With season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) the NBA and the G League roll over in May, after their playoffs start; with ``All Star`` the NBA rolls over in March, after the February game (the G League has no All-Star rows and keeps its own rule). A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs``, ``PlayIn`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` / ``PlayIn`` roll over in May (NBA, G League), ``All Star`` in March (NBA). |
| `SeasonYear` | `season_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_leaguestandings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | String | League identifier used in compact NBA Stats schedule and scoreboard result sets. |
| `season_id` | String | Stats API identifier for seasonid associated with this NBA or WNBA Stats row. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `conference` | String | Conference name. |
| `conference_record` | String | NBA or WNBA Stats value for conferencerecord in the leaguestandings result set. |
| `playoff_rank` | Int64 | NBA or WNBA Stats value for playoffrank in the leaguestandings result set. |
| `clinch_indicator` | String | NBA or WNBA Stats value for clinchindicator in the leaguestandings result set. |
| `division` | String | Team division. |
| `division_record` | String | NBA or WNBA Stats value for divisionrecord in the leaguestandings result set. |
| `division_rank` | Int64 | NBA or WNBA Stats value for divisionrank in the leaguestandings result set. |
| `wins` | Int64 | Total wins. |
| `losses` | Int64 | Total losses. |
| `win_pct` | Float64 | Winning percentage for the team or split represented by this row. |
| `league_rank` | Int64 | NBA or WNBA Stats value for leaguerank in the leaguestandings result set. |
| `record` | String | Overall win-loss record. |
| `home` | String | Home. |
| `road` | String | Road. |
| `l10` | String | Last-ten record. |
| `last10_home` | String | NBA or WNBA Stats value for last10home in the leaguestandings result set. |
| `last10_road` | String | NBA or WNBA Stats value for last10road in the leaguestandings result set. |
| `ot` | String | Ot. |
| `three_pts_or_less` | String | Scoring or score-margin metric for threeptsorless in the requested NBA or WNBA Stats split. |
| `ten_pts_or_more` | String | Scoring or score-margin metric for tenptsormore in the requested NBA or WNBA Stats split. |
| `long_home_streak` | Int64 | NBA or WNBA Stats value for longhomestreak in the leaguestandings result set. |
| `str_long_home_streak` | String | NBA or WNBA Stats value for strlonghomestreak in the leaguestandings result set. |
| `long_road_streak` | Int64 | NBA or WNBA Stats value for longroadstreak in the leaguestandings result set. |
| `str_long_road_streak` | String | NBA or WNBA Stats value for strlongroadstreak in the leaguestandings result set. |
| `long_win_streak` | Int64 | NBA or WNBA Stats value for longwinstreak in the leaguestandings result set. |
| `long_loss_streak` | Int64 | NBA or WNBA Stats value for longlossstreak in the leaguestandings result set. |
| `current_home_streak` | Int64 | NBA or WNBA Stats value for currenthomestreak in the leaguestandings result set. |
| `str_current_home_streak` | String | NBA or WNBA Stats value for strcurrenthomestreak in the leaguestandings result set. |
| `current_road_streak` | Int64 | NBA or WNBA Stats value for currentroadstreak in the leaguestandings result set. |
| `str_current_road_streak` | String | NBA or WNBA Stats value for strcurrentroadstreak in the leaguestandings result set. |
| `current_streak` | Int64 | NBA or WNBA Stats value for currentstreak in the leaguestandings result set. |
| `str_current_streak` | String |  |
| `conference_games_back` | Float64 | NBA or WNBA Stats value for conferencegamesback in the leaguestandings result set. |
| `division_games_back` | Float64 | NBA or WNBA Stats value for divisiongamesback in the leaguestandings result set. |
| `clinched_conference_title` | Int64 | Flag indicating clinchedconferencetitle for the requested NBA or WNBA Stats context. |
| `clinched_division_title` | Int64 | Flag indicating clincheddivisiontitle for the requested NBA or WNBA Stats context. |
| `clinched_playoff_birth` | Int64 | Flag indicating clinchedplayoffbirth for the requested NBA or WNBA Stats context. |
| `eliminated_conference` | Int64 | Flag indicating eliminatedconference for the requested NBA or WNBA Stats context. |
| `eliminated_division` | Int64 | Flag indicating eliminateddivision for the requested NBA or WNBA Stats context. |
| `ahead_at_half` | String | NBA or WNBA Stats value for aheadathalf in the leaguestandings result set. |
| `behind_at_half` | String | NBA or WNBA Stats value for behindathalf in the leaguestandings result set. |
| `tied_at_half` | String | NBA or WNBA Stats value for tiedathalf in the leaguestandings result set. |
| `ahead_at_third` | String | NBA or WNBA Stats value for aheadatthird in the leaguestandings result set. |
| `behind_at_third` | String | NBA or WNBA Stats value for behindatthird in the leaguestandings result set. |
| `tied_at_third` | String | NBA or WNBA Stats value for tiedatthird in the leaguestandings result set. |
| `score100_pts` | String | Scoring or score-margin metric for score100pts in the requested NBA or WNBA Stats split. |
| `opp_score100_pts` | String | Scoring or score-margin metric for oppscore100pts in the requested NBA or WNBA Stats split. |
| `opp_over500` | String | NBA or WNBA Stats value for oppover500 in the leaguestandings result set. |
| `lead_in_fgpct` | String | Shooting metric for leadinfgpct in the requested NBA or WNBA Stats split. |
| `lead_in_reb` | String | Rebounding metric for leadinreb in the requested NBA or WNBA Stats split. |
| `fewer_turnovers` | String | Turnover or loose-ball metric for fewerturnovers in the requested NBA or WNBA Stats split. |
| `points_pg` | Float64 | Scoring or score-margin metric for pointspg in the requested NBA or WNBA Stats split. |
| `opp_points_pg` | Float64 | Scoring or score-margin metric for opppointspg in the requested NBA or WNBA Stats split. |
| `diff_points_pg` | Float64 | Scoring or score-margin metric for diffpointspg in the requested NBA or WNBA Stats split. |
| `vs_east` | String | NBA or WNBA Stats value for vseast in the leaguestandings result set. |
| `vs_atlantic` | String | NBA or WNBA Stats value for vsatlantic in the leaguestandings result set. |
| `vs_central` | String | NBA or WNBA Stats value for vscentral in the leaguestandings result set. |
| `vs_southeast` | String | NBA or WNBA Stats value for vssoutheast in the leaguestandings result set. |
| `vs_west` | String | NBA or WNBA Stats value for vswest in the leaguestandings result set. |
| `vs_northwest` | String | NBA or WNBA Stats value for vsnorthwest in the leaguestandings result set. |
| `vs_pacific` | String | NBA or WNBA Stats value for vspacific in the leaguestandings result set. |
| `vs_southwest` | String | NBA or WNBA Stats value for vssouthwest in the leaguestandings result set. |
| `jan` | String | Value for January in the endpoint's monthly NBA or WNBA Stats split. |
| `feb` | String | Value for February in the endpoint's monthly NBA or WNBA Stats split. |
| `mar` | String | Value for March in the endpoint's monthly NBA or WNBA Stats split. |
| `apr` | String | Value for April in the endpoint's monthly NBA or WNBA Stats split. |
| `may` | String | Value for May in the endpoint's monthly NBA or WNBA Stats split. |
| `jun` | String | Value for June in the endpoint's monthly NBA or WNBA Stats split. |
| `jul` | String | Value for July in the endpoint's monthly NBA or WNBA Stats split. |
| `aug` | String | Value for August in the endpoint's monthly NBA or WNBA Stats split. |
| `sep` | String | Value for September in the endpoint's monthly NBA or WNBA Stats split. |
| `oct` | String | Value for October in the endpoint's monthly NBA or WNBA Stats split. |
| `nov` | String | Value for November in the endpoint's monthly NBA or WNBA Stats split. |
| `dec` | String | Value for December in the endpoint's monthly NBA or WNBA Stats split. |
| `pre_as` | String | NBA or WNBA Stats value for preas in the leaguestandings result set. |
| `post_as` | String | NBA or WNBA Stats value for postas in the leaguestandings result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_leaguestandings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_leaguestandings-example}

```python
nba_stats_leaguestandings(league_id='00', season='2024-25')
```

## nba_stats_leaguestandingsv3

League standings v3.

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguestandingsv3`

**Valid URL:** [https://stats.nba.com/stats/leaguestandingsv3?LeagueID=00&Season=2024-25&SeasonType=Regular+Season&SeasonYear=](https://stats.nba.com/stats/leaguestandingsv3?LeagueID=00&Season=2024-25&SeasonType=Regular+Season&SeasonYear=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. An NBA season tips off in late October and becomes the default in November (``2025-26`` through October 2026, ``2026-27`` from November 2026); a G League season (regular season from late December) in January; a Summer League (played in July, which stats.nba.com labels ``2026-27`` in 2026) in August; a draft combine (May) in June; a draft (``drafthistory``, a year; late June) in July. With season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) the NBA and the G League roll over in May, after their playoffs start; with ``All Star`` the NBA rolls over in March, after the February game (the G League has no All-Star rows and keeps its own rule). A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs``, ``PlayIn`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` / ``PlayIn`` roll over in May (NBA, G League), ``All Star`` in March (NBA). |
| `SeasonYear` | `season_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_leaguestandingsv3-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | String | League identifier used in compact NBA Stats schedule and scoreboard result sets. |
| `season_id` | String | Stats API identifier for seasonid associated with this NBA or WNBA Stats row. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_slug` | String | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `conference` | String | Conference name. |
| `conference_record` | String | NBA or WNBA Stats value for conferencerecord in the leaguestandingsv3 result set. |
| `playoff_rank` | Int64 | NBA or WNBA Stats value for playoffrank in the leaguestandingsv3 result set. |
| `clinch_indicator` | String | NBA or WNBA Stats value for clinchindicator in the leaguestandingsv3 result set. |
| `division` | String | Team division. |
| `division_record` | String | NBA or WNBA Stats value for divisionrecord in the leaguestandingsv3 result set. |
| `division_rank` | Int64 | NBA or WNBA Stats value for divisionrank in the leaguestandingsv3 result set. |
| `wins` | Int64 | Total wins. |
| `losses` | Int64 | Total losses. |
| `win_pct` | Float64 | Winning percentage for the team or split represented by this row. |
| `league_rank` | Int64 | NBA or WNBA Stats value for leaguerank in the leaguestandingsv3 result set. |
| `record` | String | Overall win-loss record. |
| `home` | String | Home. |
| `road` | String | Road. |
| `l10` | String | Last-ten record. |
| `last10_home` | String | NBA or WNBA Stats value for last10home in the leaguestandingsv3 result set. |
| `last10_road` | String | NBA or WNBA Stats value for last10road in the leaguestandingsv3 result set. |
| `ot` | String | Ot. |
| `three_pts_or_less` | String | Scoring or score-margin metric for threeptsorless in the requested NBA or WNBA Stats split. |
| `ten_pts_or_more` | String | Scoring or score-margin metric for tenptsormore in the requested NBA or WNBA Stats split. |
| `long_home_streak` | Int64 | NBA or WNBA Stats value for longhomestreak in the leaguestandingsv3 result set. |
| `str_long_home_streak` | String | NBA or WNBA Stats value for strlonghomestreak in the leaguestandingsv3 result set. |
| `long_road_streak` | Int64 | NBA or WNBA Stats value for longroadstreak in the leaguestandingsv3 result set. |
| `str_long_road_streak` | String | NBA or WNBA Stats value for strlongroadstreak in the leaguestandingsv3 result set. |
| `long_win_streak` | Int64 | NBA or WNBA Stats value for longwinstreak in the leaguestandingsv3 result set. |
| `long_loss_streak` | Int64 | NBA or WNBA Stats value for longlossstreak in the leaguestandingsv3 result set. |
| `current_home_streak` | Int64 | NBA or WNBA Stats value for currenthomestreak in the leaguestandingsv3 result set. |
| `str_current_home_streak` | String | NBA or WNBA Stats value for strcurrenthomestreak in the leaguestandingsv3 result set. |
| `current_road_streak` | Int64 | NBA or WNBA Stats value for currentroadstreak in the leaguestandingsv3 result set. |
| `str_current_road_streak` | String | NBA or WNBA Stats value for strcurrentroadstreak in the leaguestandingsv3 result set. |
| `current_streak` | Int64 | NBA or WNBA Stats value for currentstreak in the leaguestandingsv3 result set. |
| `str_current_streak` | String |  |
| `conference_games_back` | Float64 | NBA or WNBA Stats value for conferencegamesback in the leaguestandingsv3 result set. |
| `division_games_back` | Float64 | NBA or WNBA Stats value for divisiongamesback in the leaguestandingsv3 result set. |
| `clinched_conference_title` | Int64 | Flag indicating clinchedconferencetitle for the requested NBA or WNBA Stats context. |
| `clinched_division_title` | Int64 | Flag indicating clincheddivisiontitle for the requested NBA or WNBA Stats context. |
| `clinched_playoff_birth` | Int64 | Flag indicating clinchedplayoffbirth for the requested NBA or WNBA Stats context. |
| `clinched_play_in` | Int64 | Flag indicating clinchedplayin for the requested NBA or WNBA Stats context. |
| `eliminated_conference` | Int64 | Flag indicating eliminatedconference for the requested NBA or WNBA Stats context. |
| `eliminated_division` | Int64 | Flag indicating eliminateddivision for the requested NBA or WNBA Stats context. |
| `ahead_at_half` | String | NBA or WNBA Stats value for aheadathalf in the leaguestandingsv3 result set. |
| `behind_at_half` | String | NBA or WNBA Stats value for behindathalf in the leaguestandingsv3 result set. |
| `tied_at_half` | String | NBA or WNBA Stats value for tiedathalf in the leaguestandingsv3 result set. |
| `ahead_at_third` | String | NBA or WNBA Stats value for aheadatthird in the leaguestandingsv3 result set. |
| `behind_at_third` | String | NBA or WNBA Stats value for behindatthird in the leaguestandingsv3 result set. |
| `tied_at_third` | String | NBA or WNBA Stats value for tiedatthird in the leaguestandingsv3 result set. |
| `score100_pts` | String | Scoring or score-margin metric for score100pts in the requested NBA or WNBA Stats split. |
| `opp_score100_pts` | String | Scoring or score-margin metric for oppscore100pts in the requested NBA or WNBA Stats split. |
| `opp_over500` | String | NBA or WNBA Stats value for oppover500 in the leaguestandingsv3 result set. |
| `lead_in_fgpct` | String | Shooting metric for leadinfgpct in the requested NBA or WNBA Stats split. |
| `lead_in_reb` | String | Rebounding metric for leadinreb in the requested NBA or WNBA Stats split. |
| `fewer_turnovers` | String | Turnover or loose-ball metric for fewerturnovers in the requested NBA or WNBA Stats split. |
| `points_pg` | Float64 | Scoring or score-margin metric for pointspg in the requested NBA or WNBA Stats split. |
| `opp_points_pg` | Float64 | Scoring or score-margin metric for opppointspg in the requested NBA or WNBA Stats split. |
| `diff_points_pg` | Float64 | Scoring or score-margin metric for diffpointspg in the requested NBA or WNBA Stats split. |
| `vs_east` | String | NBA or WNBA Stats value for vseast in the leaguestandingsv3 result set. |
| `vs_atlantic` | String | NBA or WNBA Stats value for vsatlantic in the leaguestandingsv3 result set. |
| `vs_central` | String | NBA or WNBA Stats value for vscentral in the leaguestandingsv3 result set. |
| `vs_southeast` | String | NBA or WNBA Stats value for vssoutheast in the leaguestandingsv3 result set. |
| `vs_west` | String | NBA or WNBA Stats value for vswest in the leaguestandingsv3 result set. |
| `vs_northwest` | String | NBA or WNBA Stats value for vsnorthwest in the leaguestandingsv3 result set. |
| `vs_pacific` | String | NBA or WNBA Stats value for vspacific in the leaguestandingsv3 result set. |
| `vs_southwest` | String | NBA or WNBA Stats value for vssouthwest in the leaguestandingsv3 result set. |
| `jan` | String | Value for January in the endpoint's monthly NBA or WNBA Stats split. |
| `feb` | String | Value for February in the endpoint's monthly NBA or WNBA Stats split. |
| `mar` | String | Value for March in the endpoint's monthly NBA or WNBA Stats split. |
| `apr` | String | Value for April in the endpoint's monthly NBA or WNBA Stats split. |
| `may` | String | Value for May in the endpoint's monthly NBA or WNBA Stats split. |
| `jun` | String | Value for June in the endpoint's monthly NBA or WNBA Stats split. |
| `jul` | String | Value for July in the endpoint's monthly NBA or WNBA Stats split. |
| `aug` | String | Value for August in the endpoint's monthly NBA or WNBA Stats split. |
| `sep` | String | Value for September in the endpoint's monthly NBA or WNBA Stats split. |
| `oct` | String | Value for October in the endpoint's monthly NBA or WNBA Stats split. |
| `nov` | String | Value for November in the endpoint's monthly NBA or WNBA Stats split. |
| `dec` | String | Value for December in the endpoint's monthly NBA or WNBA Stats split. |
| `score_80_plus` | String | Scoring or score-margin metric for score 80 plus in the requested NBA or WNBA Stats split. |
| `opp_score_80_plus` | String | Opponent score 80 plus for the requested NBA or WNBA team, player, lineup, or game split. |
| `score_below_80` | String | Scoring or score-margin metric for score below 80 in the requested NBA or WNBA Stats split. |
| `opp_score_below_80` | String | Opponent score below 80 for the requested NBA or WNBA team, player, lineup, or game split. |
| `total_points` | Int64 | Scoring or score-margin metric for totalpoints in the requested NBA or WNBA Stats split. |
| `opp_total_points` | Int64 | Scoring or score-margin metric for opptotalpoints in the requested NBA or WNBA Stats split. |
| `diff_total_points` | Int64 | Scoring or score-margin metric for difftotalpoints in the requested NBA or WNBA Stats split. |
| `league_games_back` | Float64 | NBA or WNBA Stats value for leaguegamesback in the leaguestandingsv3 result set. |
| `playoff_seeding` | Int64 | NBA or WNBA Stats value for playoffseeding in the leaguestandingsv3 result set. |
| `clinched_post_season` | Int64 | Flag indicating clinchedpostseason for the requested NBA or WNBA Stats context. |
| `neutral` | String | Neutral. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_leaguestandingsv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_leaguestandingsv3-example}

```python
nba_stats_leaguestandingsv3(league_id='00', season='2024-25')
```
