# NBA — NBA Stats API (stats.nba.com) — Other: playoffpicture–synergyplaytypes

> NBA — NBA Stats API (stats.nba.com) — Other: playoffpicture–synergyplaytypes — function reference in sdv-py, the SportsDataverse Python package.

## nba_stats_playoffpicture

GET /stats/playoffpicture

**Endpoint URL:** `GET https://stats.nba.com/stats/playoffpicture`

**Valid URL:** [https://stats.nba.com/stats/playoffpicture?LeagueID=00&SeasonID=22022](https://stats.nba.com/stats/playoffpicture?LeagueID=00&SeasonID=22022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonID` | `season_id` |  |  | `Y` |  |

### Returns {#nba_stats_playoffpicture-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`EastConfPlayoffPicture`, `WestConfPlayoffPicture`, `EastConfStandings`, `WestConfStandings`, `EastConfRemainingGames`, `WestConfRemainingGames`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**EastConfPlayoffPicture**

| col_name | type | description |
|---|---|---|
| `conference` | character | Conference name. |
| `high_seed_rank` | integer |  |
| `high_seed_team` | character |  |
| `high_seed_team_id` | integer |  |
| `low_seed_rank` | integer |  |
| `low_seed_team` | character |  |
| `low_seed_team_id` | integer |  |
| `high_seed_series_w` | integer |  |
| `high_seed_series_l` | integer |  |
| `high_seed_series_remaining_g` | integer |  |
| `high_seed_series_remaining_home_g` | integer |  |
| `high_seed_series_remaining_away_g` | integer |  |

**WestConfPlayoffPicture**

| col_name | type | description |
|---|---|---|
| `conference` | character | Conference name. |
| `high_seed_rank` | integer |  |
| `high_seed_team` | character |  |
| `high_seed_team_id` | integer |  |
| `low_seed_rank` | integer |  |
| `low_seed_team` | character |  |
| `low_seed_team_id` | integer |  |
| `high_seed_series_w` | integer |  |
| `high_seed_series_l` | integer |  |
| `high_seed_series_remaining_g` | integer |  |
| `high_seed_series_remaining_home_g` | integer |  |
| `high_seed_series_remaining_away_g` | integer |  |

**EastConfStandings**

| col_name | type | description |
|---|---|---|
| `conference` | character | Conference name. |
| `rank` | integer | Conference standing position of the team (1 = top seed) in the playoff picture. |
| `team` | character | Team-side label or team identifier. |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_id` | integer | Unique team identifier. |
| `wins` | integer | Total wins. |
| `losses` | integer | Total losses. |
| `pct` | numeric | Win percentage. |
| `div` | character | Abbreviation of the team's division. |
| `conf` | character | character. |
| `home` | character | Home. |
| `away` | character | Away record. |
| `gb` | numeric | Games behind the conference leader. |
| `gr_over_500` | integer | Remaining games against teams with winning (over .500) records. |
| `gr_over_500_home` | integer | Remaining home games against teams with winning records. |
| `gr_over_500_away` | integer | Remaining road games against teams with winning records. |
| `gr_under_500` | integer | Remaining games against teams with losing (under .500) records. |
| `gr_under_500_home` | integer | Remaining home games against teams with losing records. |
| `gr_under_500_away` | integer | Remaining road games against teams with losing records. |
| `ranking_criteria` | integer | Code for the ranking or tiebreak criteria applied to the team in the playoff picture. |
| `clinched_playoffs` | integer | Flag (1/0) for whether the team has clinched a playoff berth. |
| `clinched_conference` | integer | Flag (1/0) for whether the team has clinched the conference title. |
| `clinched_division` | integer | Flag (1/0) for whether the team has clinched its division. |
| `eliminated_playoffs` | integer | Flag (1/0) for whether the team has been eliminated from playoff contention. |
| `sosa_remaining` | character | Strength of schedule of the team's remaining opponents (combined opponent winning percentage). |

**WestConfStandings**

| col_name | type | description |
|---|---|---|
| `conference` | character | Conference name. |
| `rank` | integer | Conference standing position of the team (1 = top seed) in the playoff picture. |
| `team` | character | Team-side label or team identifier. |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_id` | integer | Unique team identifier. |
| `wins` | integer | Total wins. |
| `losses` | integer | Total losses. |
| `pct` | numeric | Win percentage. |
| `div` | character | Abbreviation of the team's division. |
| `conf` | character | character. |
| `home` | character | Home. |
| `away` | character | Away record. |
| `gb` | numeric | Games behind the conference leader. |
| `gr_over_500` | integer | Remaining games against teams with winning (over .500) records. |
| `gr_over_500_home` | integer | Remaining home games against teams with winning records. |
| `gr_over_500_away` | integer | Remaining road games against teams with winning records. |
| `gr_under_500` | integer | Remaining games against teams with losing (under .500) records. |
| `gr_under_500_home` | integer | Remaining home games against teams with losing records. |
| `gr_under_500_away` | integer | Remaining road games against teams with losing records. |
| `ranking_criteria` | integer | Code for the ranking or tiebreak criteria applied to the team in the playoff picture. |
| `clinched_playoffs` | integer | Flag (1/0) for whether the team has clinched a playoff berth. |
| `clinched_conference` | integer | Flag (1/0) for whether the team has clinched the conference title. |
| `clinched_division` | integer | Flag (1/0) for whether the team has clinched its division. |
| `eliminated_playoffs` | integer | Flag (1/0) for whether the team has been eliminated from playoff contention. |
| `sosa_remaining` | character | Strength of schedule of the team's remaining opponents (combined opponent winning percentage). |

**EastConfRemainingGames**

| col_name | type | description |
|---|---|---|
| `team` | character | Team-side label or team identifier. |
| `team_id` | integer | Unique team identifier. |
| `remaining_g` | integer |  |
| `remaining_home_g` | integer |  |
| `remaining_away_g` | integer |  |

**WestConfRemainingGames**

| col_name | type | description |
|---|---|---|
| `team` | character | Team-side label or team identifier. |
| `team_id` | integer | Unique team identifier. |
| `remaining_g` | integer |  |
| `remaining_home_g` | integer |  |
| `remaining_away_g` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_playoffpicture-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_playoffpicture-example}

```python
nba_stats_playoffpicture(league_id='00')
```

_Last validated n/a._

## nba_stats_synergyplaytypes

GET /stats/synergyplaytypes

**Endpoint URL:** `GET https://stats.nba.com/stats/synergyplaytypes`

**Valid URL:** [https://stats.nba.com/stats/synergyplaytypes?LeagueID=00&PerMode=PerGame&PlayType=Isolation&PlayerOrTeam=P&SeasonType=Regular+Season&SeasonYear=2024-25&TypeGrouping=Offensive](https://stats.nba.com/stats/synergyplaytypes?LeagueID=00&PerMode=PerGame&PlayType=Isolation&PlayerOrTeam=P&SeasonType=Regular+Season&SeasonYear=2024-25&TypeGrouping=Offensive)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `PlayType` | `play_type_nullable` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team_abbreviation` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs``, ``PlayIn`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` / ``PlayIn`` roll over in May (NBA, G League), ``All Star`` in March (NBA). |
| `SeasonYear` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. An NBA season tips off in late October and becomes the default in November (``2025-26`` through October 2026, ``2026-27`` from November 2026); a G League season (regular season from late December) in January; a Summer League (played in July, which stats.nba.com labels ``2026-27`` in 2026) in August; a draft combine (May) in June; a draft (``drafthistory``, a year; late June) in July. With season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) the NBA and the G League roll over in May, after their playoffs start; with ``All Star`` the NBA rolls over in March, after the February game (the G League has no All-Star rows and keeps its own rule). A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `TypeGrouping` | `type_grouping_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_synergyplaytypes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_id` | character | Unique season identifier. |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `play_type` | character | Play type description. |
| `type_grouping` | character | Whether the play-type row measures the offensive or defensive side. |
| `percentile` | numeric | League percentile of the row's points per possession for the play type, as a decimal. |
| `gp` | integer | Games played. |
| `poss_pct` | numeric | Poss percentage (0-1 decimal). |
| `ppp` | numeric | Points scored per possession on the play type (Synergy play-type tracking). |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `ft_poss_pct` | numeric | Share of play-type possessions ending in free throws, as a decimal. |
| `tov_poss_pct` | numeric | Share of play-type possessions ending in a turnover, as a decimal. |
| `sf_poss_pct` | numeric | Share of play-type possessions on which a shooting foul was drawn, as a decimal. |
| `plusone_poss_pct` | numeric | Share of play-type possessions producing an and-one, as a decimal. |
| `score_poss_pct` | numeric | Share of play-type possessions on which points were scored, as a decimal. |
| `efg_pct` | numeric | Effective field goal percentage on the play type, as a decimal. |
| `poss` | numeric | Poss. |
| `pts` | numeric | Points scored. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fgmx` | numeric | Field goals missed on the play type. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_synergyplaytypes-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_synergyplaytypes-example}

```python
nba_stats_synergyplaytypes(league_id='00', season='2024-25')
```

_Last validated n/a._
