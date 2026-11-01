# NBA — NBA Stats API (stats.nba.com) — Other: playoffpicture–synergyplaytypes

> NBA — NBA Stats API (stats.nba.com) — Other: playoffpicture–synergyplaytypes — function reference in sdv-py, the SportsDataverse Python package.

## nba_stats_playoffpicture

Playoff picture.

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
| `conference` | String | Conference name. |
| `high_seed_rank` | Int64 |  |
| `high_seed_team` | String |  |
| `high_seed_team_id` | Int64 |  |
| `low_seed_rank` | Int64 |  |
| `low_seed_team` | String |  |
| `low_seed_team_id` | Int64 |  |
| `high_seed_series_w` | Int64 |  |
| `high_seed_series_l` | Int64 |  |
| `high_seed_series_remaining_g` | Int64 |  |
| `high_seed_series_remaining_home_g` | Int64 |  |
| `high_seed_series_remaining_away_g` | Int64 |  |

**WestConfPlayoffPicture**

| col_name | type | description |
|---|---|---|
| `conference` | String | Conference name. |
| `high_seed_rank` | Int64 |  |
| `high_seed_team` | String |  |
| `high_seed_team_id` | Int64 |  |
| `low_seed_rank` | Int64 |  |
| `low_seed_team` | String |  |
| `low_seed_team_id` | Int64 |  |
| `high_seed_series_w` | Int64 |  |
| `high_seed_series_l` | Int64 |  |
| `high_seed_series_remaining_g` | Int64 |  |
| `high_seed_series_remaining_home_g` | Int64 |  |
| `high_seed_series_remaining_away_g` | Int64 |  |

**EastConfStandings**

| col_name | type | description |
|---|---|---|
| `conference` | String | Conference name. |
| `rank` | Int64 | Conference standing position of the team (1 = top seed) in the playoff picture. |
| `team` | String | Team-side label or team identifier. |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_id` | Int64 | Unique team identifier. |
| `wins` | Int64 | Total wins. |
| `losses` | Int64 | Total losses. |
| `pct` | Float64 | Win percentage. |
| `div` | String | Abbreviation of the team's division. |
| `conf` | String | character. |
| `home` | String | Home. |
| `away` | String | Away record. |
| `gb` | Float64 | Games behind the conference leader. |
| `gr_over_500` | Int64 | Remaining games against teams with winning (over .500) records. |
| `gr_over_500_home` | Int64 | Remaining home games against teams with winning records. |
| `gr_over_500_away` | Int64 | Remaining road games against teams with winning records. |
| `gr_under_500` | Int64 | Remaining games against teams with losing (under .500) records. |
| `gr_under_500_home` | Int64 | Remaining home games against teams with losing records. |
| `gr_under_500_away` | Int64 | Remaining road games against teams with losing records. |
| `ranking_criteria` | Int64 | Code for the ranking or tiebreak criteria applied to the team in the playoff picture. |
| `clinched_playoffs` | Int64 | Flag (1/0) for whether the team has clinched a playoff berth. |
| `clinched_conference` | Int64 | Flag (1/0) for whether the team has clinched the conference title. |
| `clinched_division` | Int64 | Flag (1/0) for whether the team has clinched its division. |
| `eliminated_playoffs` | Int64 | Flag (1/0) for whether the team has been eliminated from playoff contention. |
| `sosa_remaining` | String | Strength of schedule of the team's remaining opponents (combined opponent winning percentage). |

**WestConfStandings**

| col_name | type | description |
|---|---|---|
| `conference` | String | Conference name. |
| `rank` | Int64 | Conference standing position of the team (1 = top seed) in the playoff picture. |
| `team` | String | Team-side label or team identifier. |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_id` | Int64 | Unique team identifier. |
| `wins` | Int64 | Total wins. |
| `losses` | Int64 | Total losses. |
| `pct` | Float64 | Win percentage. |
| `div` | String | Abbreviation of the team's division. |
| `conf` | String | character. |
| `home` | String | Home. |
| `away` | String | Away record. |
| `gb` | Float64 | Games behind the conference leader. |
| `gr_over_500` | Int64 | Remaining games against teams with winning (over .500) records. |
| `gr_over_500_home` | Int64 | Remaining home games against teams with winning records. |
| `gr_over_500_away` | Int64 | Remaining road games against teams with winning records. |
| `gr_under_500` | Int64 | Remaining games against teams with losing (under .500) records. |
| `gr_under_500_home` | Int64 | Remaining home games against teams with losing records. |
| `gr_under_500_away` | Int64 | Remaining road games against teams with losing records. |
| `ranking_criteria` | Int64 | Code for the ranking or tiebreak criteria applied to the team in the playoff picture. |
| `clinched_playoffs` | Int64 | Flag (1/0) for whether the team has clinched a playoff berth. |
| `clinched_conference` | Int64 | Flag (1/0) for whether the team has clinched the conference title. |
| `clinched_division` | Int64 | Flag (1/0) for whether the team has clinched its division. |
| `eliminated_playoffs` | Int64 | Flag (1/0) for whether the team has been eliminated from playoff contention. |
| `sosa_remaining` | String | Strength of schedule of the team's remaining opponents (combined opponent winning percentage). |

**EastConfRemainingGames**

| col_name | type | description |
|---|---|---|
| `team` | String | Team-side label or team identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `remaining_g` | Int64 |  |
| `remaining_home_g` | Int64 |  |
| `remaining_away_g` | Int64 |  |

**WestConfRemainingGames**

| col_name | type | description |
|---|---|---|
| `team` | String | Team-side label or team identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `remaining_g` | Int64 |  |
| `remaining_home_g` | Int64 |  |
| `remaining_away_g` | Int64 |  |

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

## nba_stats_synergyplaytypes

Synergy play types.

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
| `season_id` | String | Unique season identifier. |
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `play_type` | String | Play type description. |
| `type_grouping` | String | Whether the play-type row measures the offensive or defensive side. |
| `percentile` | Float64 | League percentile of the row's points per possession for the play type, as a decimal. |
| `gp` | Int64 | Games played. |
| `poss_pct` | Float64 | Poss percentage (0-1 decimal). |
| `ppp` | Float64 | Points scored per possession on the play type (Synergy play-type tracking). |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `ft_poss_pct` | Float64 | Share of play-type possessions ending in free throws, as a decimal. |
| `tov_poss_pct` | Float64 | Share of play-type possessions ending in a turnover, as a decimal. |
| `sf_poss_pct` | Float64 | Share of play-type possessions on which a shooting foul was drawn, as a decimal. |
| `plusone_poss_pct` | Float64 | Share of play-type possessions producing an and-one, as a decimal. |
| `score_poss_pct` | Float64 | Share of play-type possessions on which points were scored, as a decimal. |
| `efg_pct` | Float64 | Effective field goal percentage on the play type, as a decimal. |
| `poss` | Float64 | Poss. |
| `pts` | Float64 | Points scored. |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fgmx` | Float64 | Field goals missed on the play type. |

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
