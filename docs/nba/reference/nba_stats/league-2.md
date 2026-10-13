# NBA — NBA Stats API (stats.nba.com) — League: leaguestandingsv3

> NBA — NBA Stats API (stats.nba.com) — League: leaguestandingsv3 — function reference in sdv-py, the SportsDataverse Python package.

## nba_stats_leaguestandingsv3

GET /stats/leaguestandingsv3

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
| `league_id` | character | League identifier used in compact NBA Stats schedule and scoreboard result sets. |
| `season_id` | character | Stats API identifier for seasonid associated with this NBA or WNBA Stats row. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_slug` | character | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `conference` | character | Conference name. |
| `conference_record` | character | NBA or WNBA Stats value for conferencerecord in the leaguestandingsv3 result set. |
| `playoff_rank` | integer | NBA or WNBA Stats value for playoffrank in the leaguestandingsv3 result set. |
| `clinch_indicator` | character | NBA or WNBA Stats value for clinchindicator in the leaguestandingsv3 result set. |
| `division` | character | Team division. |
| `division_record` | character | NBA or WNBA Stats value for divisionrecord in the leaguestandingsv3 result set. |
| `division_rank` | integer | NBA or WNBA Stats value for divisionrank in the leaguestandingsv3 result set. |
| `wins` | integer | Total wins. |
| `losses` | integer | Total losses. |
| `win_pct` | numeric | Winning percentage for the team or split represented by this row. |
| `league_rank` | integer | NBA or WNBA Stats value for leaguerank in the leaguestandingsv3 result set. |
| `record` | character | Overall win-loss record. |
| `home` | character | Home. |
| `road` | character | Road. |
| `l10` | character | Last-ten record. |
| `last10_home` | character | NBA or WNBA Stats value for last10home in the leaguestandingsv3 result set. |
| `last10_road` | character | NBA or WNBA Stats value for last10road in the leaguestandingsv3 result set. |
| `ot` | character | Ot. |
| `three_pts_or_less` | character | Scoring or score-margin metric for threeptsorless in the requested NBA or WNBA Stats split. |
| `ten_pts_or_more` | character | Scoring or score-margin metric for tenptsormore in the requested NBA or WNBA Stats split. |
| `long_home_streak` | integer | NBA or WNBA Stats value for longhomestreak in the leaguestandingsv3 result set. |
| `str_long_home_streak` | character | NBA or WNBA Stats value for strlonghomestreak in the leaguestandingsv3 result set. |
| `long_road_streak` | integer | NBA or WNBA Stats value for longroadstreak in the leaguestandingsv3 result set. |
| `str_long_road_streak` | character | NBA or WNBA Stats value for strlongroadstreak in the leaguestandingsv3 result set. |
| `long_win_streak` | integer | NBA or WNBA Stats value for longwinstreak in the leaguestandingsv3 result set. |
| `long_loss_streak` | integer | NBA or WNBA Stats value for longlossstreak in the leaguestandingsv3 result set. |
| `current_home_streak` | integer | NBA or WNBA Stats value for currenthomestreak in the leaguestandingsv3 result set. |
| `str_current_home_streak` | character | NBA or WNBA Stats value for strcurrenthomestreak in the leaguestandingsv3 result set. |
| `current_road_streak` | integer | NBA or WNBA Stats value for currentroadstreak in the leaguestandingsv3 result set. |
| `str_current_road_streak` | character | NBA or WNBA Stats value for strcurrentroadstreak in the leaguestandingsv3 result set. |
| `current_streak` | integer | NBA or WNBA Stats value for currentstreak in the leaguestandingsv3 result set. |
| `str_current_streak` | character |  |
| `conference_games_back` | numeric | NBA or WNBA Stats value for conferencegamesback in the leaguestandingsv3 result set. |
| `division_games_back` | numeric | NBA or WNBA Stats value for divisiongamesback in the leaguestandingsv3 result set. |
| `clinched_conference_title` | integer | Flag indicating clinchedconferencetitle for the requested NBA or WNBA Stats context. |
| `clinched_division_title` | integer | Flag indicating clincheddivisiontitle for the requested NBA or WNBA Stats context. |
| `clinched_playoff_birth` | integer | Flag indicating clinchedplayoffbirth for the requested NBA or WNBA Stats context. |
| `clinched_play_in` | integer | Flag indicating clinchedplayin for the requested NBA or WNBA Stats context. |
| `eliminated_conference` | integer | Flag indicating eliminatedconference for the requested NBA or WNBA Stats context. |
| `eliminated_division` | integer | Flag indicating eliminateddivision for the requested NBA or WNBA Stats context. |
| `ahead_at_half` | character | NBA or WNBA Stats value for aheadathalf in the leaguestandingsv3 result set. |
| `behind_at_half` | character | NBA or WNBA Stats value for behindathalf in the leaguestandingsv3 result set. |
| `tied_at_half` | character | NBA or WNBA Stats value for tiedathalf in the leaguestandingsv3 result set. |
| `ahead_at_third` | character | NBA or WNBA Stats value for aheadatthird in the leaguestandingsv3 result set. |
| `behind_at_third` | character | NBA or WNBA Stats value for behindatthird in the leaguestandingsv3 result set. |
| `tied_at_third` | character | NBA or WNBA Stats value for tiedatthird in the leaguestandingsv3 result set. |
| `score100_pts` | character | Scoring or score-margin metric for score100pts in the requested NBA or WNBA Stats split. |
| `opp_score100_pts` | character | Scoring or score-margin metric for oppscore100pts in the requested NBA or WNBA Stats split. |
| `opp_over500` | character | NBA or WNBA Stats value for oppover500 in the leaguestandingsv3 result set. |
| `lead_in_fgpct` | character | Shooting metric for leadinfgpct in the requested NBA or WNBA Stats split. |
| `lead_in_reb` | character | Rebounding metric for leadinreb in the requested NBA or WNBA Stats split. |
| `fewer_turnovers` | character | Turnover or loose-ball metric for fewerturnovers in the requested NBA or WNBA Stats split. |
| `points_pg` | numeric | Scoring or score-margin metric for pointspg in the requested NBA or WNBA Stats split. |
| `opp_points_pg` | numeric | Scoring or score-margin metric for opppointspg in the requested NBA or WNBA Stats split. |
| `diff_points_pg` | numeric | Scoring or score-margin metric for diffpointspg in the requested NBA or WNBA Stats split. |
| `vs_east` | character | NBA or WNBA Stats value for vseast in the leaguestandingsv3 result set. |
| `vs_atlantic` | character | NBA or WNBA Stats value for vsatlantic in the leaguestandingsv3 result set. |
| `vs_central` | character | NBA or WNBA Stats value for vscentral in the leaguestandingsv3 result set. |
| `vs_southeast` | character | NBA or WNBA Stats value for vssoutheast in the leaguestandingsv3 result set. |
| `vs_west` | character | NBA or WNBA Stats value for vswest in the leaguestandingsv3 result set. |
| `vs_northwest` | character | NBA or WNBA Stats value for vsnorthwest in the leaguestandingsv3 result set. |
| `vs_pacific` | character | NBA or WNBA Stats value for vspacific in the leaguestandingsv3 result set. |
| `vs_southwest` | character | NBA or WNBA Stats value for vssouthwest in the leaguestandingsv3 result set. |
| `jan` | character | Value for January in the endpoint's monthly NBA or WNBA Stats split. |
| `feb` | character | Value for February in the endpoint's monthly NBA or WNBA Stats split. |
| `mar` | character | Value for March in the endpoint's monthly NBA or WNBA Stats split. |
| `apr` | character | Value for April in the endpoint's monthly NBA or WNBA Stats split. |
| `may` | character | Value for May in the endpoint's monthly NBA or WNBA Stats split. |
| `jun` | character | Value for June in the endpoint's monthly NBA or WNBA Stats split. |
| `jul` | character | Value for July in the endpoint's monthly NBA or WNBA Stats split. |
| `aug` | character | Value for August in the endpoint's monthly NBA or WNBA Stats split. |
| `sep` | character | Value for September in the endpoint's monthly NBA or WNBA Stats split. |
| `oct` | character | Value for October in the endpoint's monthly NBA or WNBA Stats split. |
| `nov` | character | Value for November in the endpoint's monthly NBA or WNBA Stats split. |
| `dec` | character | Value for December in the endpoint's monthly NBA or WNBA Stats split. |
| `score_80_plus` | character | Scoring or score-margin metric for score 80 plus in the requested NBA or WNBA Stats split. |
| `opp_score_80_plus` | character | Opponent score 80 plus for the requested NBA or WNBA team, player, lineup, or game split. |
| `score_below_80` | character | Scoring or score-margin metric for score below 80 in the requested NBA or WNBA Stats split. |
| `opp_score_below_80` | character | Opponent score below 80 for the requested NBA or WNBA team, player, lineup, or game split. |
| `total_points` | integer | Scoring or score-margin metric for totalpoints in the requested NBA or WNBA Stats split. |
| `opp_total_points` | integer | Scoring or score-margin metric for opptotalpoints in the requested NBA or WNBA Stats split. |
| `diff_total_points` | integer | Scoring or score-margin metric for difftotalpoints in the requested NBA or WNBA Stats split. |
| `league_games_back` | numeric | NBA or WNBA Stats value for leaguegamesback in the leaguestandingsv3 result set. |
| `playoff_seeding` | integer | NBA or WNBA Stats value for playoffseeding in the leaguestandingsv3 result set. |
| `clinched_post_season` | integer | Flag indicating clinchedpostseason for the requested NBA or WNBA Stats context. |
| `neutral` | character | Neutral. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_leaguestandingsv3-example}

```python
nba_stats_leaguestandingsv3(league_id='00', season='2024-25')
```

_Last validated n/a._
