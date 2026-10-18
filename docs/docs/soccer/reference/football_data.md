---
title: SOCCER — Football-Data.co.uk CSV archive (football-data.co.uk)
sidebar_label: Football-Data.co.uk CSV archive (football-data.co.uk)
description: "SOCCER — Football-Data.co.uk CSV archive (football-data.co.uk) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 14
toc_max_heading_level: 2
---
# SOCCER — Football-Data.co.uk CSV archive (football-data.co.uk)

`sportsdataverse.soccer` — 3 endpoints.

## football_data_extra_league

All seasons of one 'extra' league (BRA, ARG, USA, ...).

**Endpoint URL:** `GET https://www.football-data.co.uk/new/{country}.csv`

**Valid URL:** [https://www.football-data.co.uk/new/BRA.csv](https://www.football-data.co.uk/new/BRA.csv)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `country` | `country` |  | `Y` |  | Three-letter country code of an 'extra' league, e.g. BRA (Brazil); ARG, AUT, CHN, DNK, FIN, IRL, JPN, MEX, NOR, POL, ROU, RUS, SWE, SWZ, USA per new_leagues.php. |

### Returns {#football_data_extra_league-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `country` | character |  |
| `league` | character |  |
| `season` | integer |  |
| `date` | character | Match Date (dd/mm/yy) |
| `time` | character | Time of match kick off (from 2019-20) |
| `home` | character |  |
| `away` | character |  |
| `hg` | integer | Full Time Home Team Goals |
| `ag` | integer | Full Time Away Team Goals |
| `res` | character | Full Time Result (H=Home Win, D=Draw, A=Away Win) |
| `psch` | numeric | Closing Pinnacle home win odds (closing odds, from 2012-13) |
| `pscd` | numeric | Closing Pinnacle draw odds (closing odds, from 2012-13) |
| `psca` | numeric | Closing Pinnacle away win odds (closing odds, from 2012-13) |
| `max_ch` | numeric | Closing Market maximum home win odds (closing odds, from 2019-20) |
| `max_cd` | numeric | Closing Market maximum draw win odds (closing odds, from 2019-20) |
| `max_ca` | numeric | Closing Market maximum away win odds (closing odds, from 2019-20) |
| `avg_ch` | numeric | Closing Market average home win odds (closing odds, from 2019-20) |
| `avg_cd` | numeric | Closing Market average draw win odds (closing odds, from 2019-20) |
| `avg_ca` | numeric | Closing Market average away win odds (closing odds, from 2019-20) |
| `bfech` | character | Closing Betfair Exchange home win odds (closing odds, from 2019-20) |
| `bfecd` | character | Closing Betfair Exchange draw odds (closing odds, from 2019-20) |
| `bfeca` | character | Closing Betfair Exchange away win odds (closing odds, from 2019-20) |
| `b365_ch` | character | Closing Bet365 home win odds (closing odds, from 2019-20) |
| `b365_cd` | character | Closing Bet365 draw odds (closing odds, from 2019-20) |
| `b365_ca` | character | Closing Bet365 away win odds (closing odds, from 2019-20) |

**`return_parsed=False`** — the raw CSV response body (`str`).

### Raises {#football_data_extra_league-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | football-data.co.uk answered 404 (no file for that season or division). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#football_data_extra_league-example}

```python
football_data_extra_league(country='BRA')
```

_Last validated n/a._

## football_data_fixtures

Upcoming fixtures with odds.

**Endpoint URL:** `GET https://www.football-data.co.uk/fixtures.csv`

**Valid URL:** [https://www.football-data.co.uk/fixtures.csv](https://www.football-data.co.uk/fixtures.csv)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#football_data_fixtures-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `div` | character | League Division |
| `date` | character | Match Date (dd/mm/yy) |
| `time` | character | Time of match kick off (from 2019-20) |
| `home_team` | character | Home Team |
| `away_team` | character | Away Team |
| `referee` | character | Match Referee (from 2000-01, results-only files before) |
| `b365_h` | numeric | Bet365 home win odds (from 2002-03) |
| `b365_d` | numeric | Bet365 draw odds (from 2002-03) |
| `b365_a` | numeric | Bet365 away win odds (from 2002-03) |
| `bfdh` | numeric | Betfred home win odds (from 2025-26) |
| `bfdd` | numeric | Betfred draw odds (from 2025-26) |
| `bfda` | numeric | Betfred away win odds (from 2025-26) |
| `bvh` | numeric | Betvictor home win odds |
| `bvd` | numeric | Betvictor draw odds |
| `bva` | numeric | Betvictor away win odds |
| `bwh` | numeric | Bet&Win home win odds |
| `bwd` | numeric | Bet&Win draw odds |
| `bwa` | numeric | Bet&Win away win odds |
| `pph` | numeric | Paddy Power home win odds |
| `ppd` | numeric | Paddy Power draw odds |
| `ppa` | numeric | Paddy Power away win odds |
| `skbh` | numeric |  |
| `skbd` | numeric |  |
| `skba` | numeric |  |
| `max_h` | numeric | Market maximum home win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `max_d` | numeric | Market maximum draw win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `max_a` | numeric | Market maximum away win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg_h` | numeric | Market average home win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg_d` | numeric | Market average draw win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg_a` | numeric | Market average away win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `bfeh` | numeric | Betfair Exchange home win odds (from 2024-25) |
| `bfed` | numeric | Betfair Exchange draw odds (from 2024-25) |
| `bfea` | numeric | Betfair Exchange away win odds (from 2024-25) |
| `b365>2.5` | numeric | Bet365 over 2.5 goals (from 2002-03) |
| `b365<2.5` | numeric | Bet365 under 2.5 goals (from 2002-03) |
| `max>2.5` | numeric | Market maximum over 2.5 goals (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `max<2.5` | numeric | Market maximum under 2.5 goals (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg>2.5` | numeric | Market average over 2.5 goals (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg<2.5` | numeric | Market average under 2.5 goals (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `bfe>2.5` | numeric |  |
| `bfe<2.5` | numeric |  |
| `a_hh` | numeric | Market size of handicap (home team) (since 2019/2020) |
| `b365_ahh` | numeric | Bet365 Asian handicap home team odds (from 2002-03) |
| `b365_aha` | numeric | Bet365 Asian handicap away team odds (from 2002-03) |
| `max_ahh` | numeric | Market maximum Asian handicap home team odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `max_aha` | numeric | Market maximum Asian handicap away team odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg_ahh` | numeric | Market average Asian handicap home team odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg_aha` | numeric | Market average Asian handicap away team odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `bfeahh` | numeric |  |
| `bfeaha` | numeric |  |
| `b365_ch` | character | Closing Bet365 home win odds (closing odds, from 2019-20) |
| `b365_cd` | character | Closing Bet365 draw odds (closing odds, from 2019-20) |
| `b365_ca` | character | Closing Bet365 away win odds (closing odds, from 2019-20) |
| `bfdch` | character | Closing Betfred home win odds (closing odds, from 2019-20) |
| `bfdcd` | character | Closing Betfred draw odds (closing odds, from 2019-20) |
| `bfdca` | character | Closing Betfred away win odds (closing odds, from 2019-20) |
| `bvch` | character | Closing Betvictor home win odds (closing odds, from 2019-20) |
| `bvcd` | character | Closing Betvictor draw odds (closing odds, from 2019-20) |
| `bvca` | character | Closing Betvictor away win odds (closing odds, from 2019-20) |
| `bwch` | character | Closing Bet&Win home win odds (closing odds, from 2019-20) |
| `bwcd` | character | Closing Bet&Win draw odds (closing odds, from 2019-20) |
| `bwca` | character | Closing Bet&Win away win odds (closing odds, from 2019-20) |
| `ppch` | character | Closing Paddy Power home win odds |
| `ppcd` | character | Closing Paddy Power draw odds |
| `ppca` | character | Closing Paddy Power away win odds |
| `skbch` | character |  |
| `skbcd` | character |  |
| `skbca` | character |  |
| `max_ch` | character | Closing Market maximum home win odds (closing odds, from 2019-20) |
| `max_cd` | character | Closing Market maximum draw win odds (closing odds, from 2019-20) |
| `max_ca` | character | Closing Market maximum away win odds (closing odds, from 2019-20) |
| `avg_ch` | character | Closing Market average home win odds (closing odds, from 2019-20) |
| `avg_cd` | character | Closing Market average draw win odds (closing odds, from 2019-20) |
| `avg_ca` | character | Closing Market average away win odds (closing odds, from 2019-20) |
| `bfech` | character | Closing Betfair Exchange home win odds (closing odds, from 2019-20) |
| `bfecd` | character | Closing Betfair Exchange draw odds (closing odds, from 2019-20) |
| `bfeca` | character | Closing Betfair Exchange away win odds (closing odds, from 2019-20) |
| `b365_c>2.5` | character | Closing Bet365 over 2.5 goals (closing odds, from 2019-20) |
| `b365_c<2.5` | character | Closing Bet365 under 2.5 goals (closing odds, from 2019-20) |
| `max_c>2.5` | character | Closing Market maximum over 2.5 goals (closing odds, from 2019-20) |
| `max_c<2.5` | character | Closing Market maximum under 2.5 goals (closing odds, from 2019-20) |
| `avg_c>2.5` | character | Closing Market average over 2.5 goals (closing odds, from 2019-20) |
| `avg_c<2.5` | character | Closing Market average under 2.5 goals (closing odds, from 2019-20) |
| `bfec>2.5` | character |  |
| `bfec<2.5` | character |  |
| `ah_ch` | character | Closing Market size of handicap (home team) (since 2019/2020) |
| `b365_cahh` | character | Closing Bet365 Asian handicap home team odds (closing odds, from 2019-20) |
| `b365_caha` | character | Closing Bet365 Asian handicap away team odds (closing odds, from 2019-20) |
| `max_cahh` | character | Closing Market maximum Asian handicap home team odds (closing odds, from 2019-20) |
| `max_caha` | character | Closing Market maximum Asian handicap away team odds (closing odds, from 2019-20) |
| `avg_cahh` | character | Closing Market average Asian handicap home team odds (closing odds, from 2019-20) |
| `avg_caha` | character | Closing Market average Asian handicap away team odds (closing odds, from 2019-20) |
| `bfecahh` | character |  |
| `bfecaha` | character |  |

**`return_parsed=False`** — the raw CSV response body (`str`).

### Raises {#football_data_fixtures-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | football-data.co.uk answered 404 (no file for that season or division). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#football_data_fixtures-example}

```python
football_data_fixtures()
```

_Last validated n/a._

## football_data_league_season

One league-season of results + closing odds (main leagues).

**Endpoint URL:** `GET https://www.football-data.co.uk/mmz4281/{season}/{div}.csv`

**Valid URL:** [https://www.football-data.co.uk/mmz4281/2526/E0.csv](https://www.football-data.co.uk/mmz4281/2526/E0.csv)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Two-digit start and end years of the season, e.g. 2526 for 2025/26 (files go back to 9394). |
| `div` | `div` |  | `Y` |  | Division code, e.g. E0 (Premier League); E1-E3, EC, SC0-SC3, D1, D2, I1, I2, SP1, SP2, F1, F2, N1, B1, P1, T1, G1 per matches.php. |

### Returns {#football_data_league_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `div` | character | League Division |
| `date` | character | Match Date (dd/mm/yy) |
| `time` | character | Time of match kick off (from 2019-20) |
| `home_team` | character | Home Team |
| `away_team` | character | Away Team |
| `fthg` | integer | Full Time Home Team Goals |
| `ftag` | integer | Full Time Away Team Goals |
| `ftr` | character | Full Time Result (H=Home Win, D=Draw, A=Away Win) |
| `hthg` | integer | Half Time Home Team Goals |
| `htag` | integer | Half Time Away Team Goals |
| `htr` | character | Half Time Result (H=Home Win, D=Draw, A=Away Win) |
| `referee` | character | Match Referee (from 2000-01, results-only files before) |
| `hs` | integer | Home Team Shots (from 2000-01, results-only files before) |
| `as` | integer | Away Team Shots (from 2000-01, results-only files before) |
| `hst` | integer | Home Team Shots on Target (from 2000-01, results-only files before) |
| `ast` | integer | Away Team Shots on Target (from 2000-01, results-only files before) |
| `hf` | integer | Home Team Fouls Committed (from 2000-01, results-only files before) |
| `af` | integer | Away Team Fouls Committed (from 2000-01, results-only files before) |
| `hc` | integer | Home Team Corners (from 2000-01, results-only files before) |
| `ac` | integer | Away Team Corners (from 2000-01, results-only files before) |
| `hy` | integer | Home Team Yellow Cards (from 2000-01, results-only files before) |
| `ay` | integer | Away Team Yellow Cards (from 2000-01, results-only files before) |
| `hr` | integer | Home Team Red Cards (from 2000-01, results-only files before) |
| `ar` | integer | Away Team Red Cards (from 2000-01, results-only files before) |
| `b365_h` | numeric | Bet365 home win odds (from 2002-03) |
| `b365_d` | numeric | Bet365 draw odds (from 2002-03) |
| `b365_a` | numeric | Bet365 away win odds (from 2002-03) |
| `bfdh` | numeric | Betfred home win odds (from 2025-26) |
| `bfdd` | numeric | Betfred draw odds (from 2025-26) |
| `bfda` | numeric | Betfred away win odds (from 2025-26) |
| `bmgmh` | numeric | BetMGM home win odds |
| `bmgmd` | numeric | BetMGM draw odds |
| `bmgma` | numeric | BetMGM away win odds |
| `bvh` | numeric | Betvictor home win odds |
| `bvd` | numeric | Betvictor draw odds |
| `bva` | numeric | Betvictor away win odds |
| `bwh` | numeric | Bet&Win home win odds |
| `bwd` | numeric | Bet&Win draw odds |
| `bwa` | numeric | Bet&Win away win odds |
| `clh` | numeric | Coral home win odds |
| `cld` | numeric | Coral draw odds |
| `cla` | numeric | Coral away win odds |
| `lbh` | numeric | Ladbrokes home win odds |
| `lbd` | numeric | Ladbrokes draw odds |
| `lba` | numeric | Ladbrokes away win odds |
| `psh` | numeric | Pinnacle home win odds (from 2012-13) |
| `psd` | numeric | Pinnacle draw odds (from 2012-13) |
| `psa` | numeric | Pinnacle away win odds (from 2012-13) |
| `max_h` | numeric | Market maximum home win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `max_d` | numeric | Market maximum draw win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `max_a` | numeric | Market maximum away win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg_h` | numeric | Market average home win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg_d` | numeric | Market average draw win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg_a` | numeric | Market average away win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `bfeh` | numeric | Betfair Exchange home win odds (from 2024-25) |
| `bfed` | numeric | Betfair Exchange draw odds (from 2024-25) |
| `bfea` | numeric | Betfair Exchange away win odds (from 2024-25) |
| `b365>2.5` | numeric | Bet365 over 2.5 goals (from 2002-03) |
| `b365<2.5` | numeric | Bet365 under 2.5 goals (from 2002-03) |
| `p>2.5` | numeric | Pinnacle over 2.5 goals (from 2012-13) |
| `p<2.5` | numeric | Pinnacle under 2.5 goals (from 2012-13) |
| `max>2.5` | numeric | Market maximum over 2.5 goals (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `max<2.5` | numeric | Market maximum under 2.5 goals (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg>2.5` | numeric | Market average over 2.5 goals (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg<2.5` | numeric | Market average under 2.5 goals (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `bfe>2.5` | numeric |  |
| `bfe<2.5` | numeric |  |
| `a_hh` | numeric | Market size of handicap (home team) (since 2019/2020) |
| `b365_ahh` | numeric | Bet365 Asian handicap home team odds (from 2002-03) |
| `b365_aha` | numeric | Bet365 Asian handicap away team odds (from 2002-03) |
| `pahh` | numeric | Pinnacle Asian handicap home team odds (from 2012-13) |
| `paha` | numeric | Pinnacle Asian handicap away team odds (from 2012-13) |
| `max_ahh` | numeric | Market maximum Asian handicap home team odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `max_aha` | numeric | Market maximum Asian handicap away team odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg_ahh` | numeric | Market average Asian handicap home team odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg_aha` | numeric | Market average Asian handicap away team odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `bfeahh` | numeric |  |
| `bfeaha` | numeric |  |
| `b365_ch` | numeric | Closing Bet365 home win odds (closing odds, from 2019-20) |
| `b365_cd` | numeric | Closing Bet365 draw odds (closing odds, from 2019-20) |
| `b365_ca` | numeric | Closing Bet365 away win odds (closing odds, from 2019-20) |
| `bfdch` | numeric | Closing Betfred home win odds (closing odds, from 2019-20) |
| `bfdcd` | numeric | Closing Betfred draw odds (closing odds, from 2019-20) |
| `bfdca` | numeric | Closing Betfred away win odds (closing odds, from 2019-20) |
| `bmgmch` | numeric | Closing BetMGM home win odds (closing odds, from 2019-20) |
| `bmgmcd` | numeric | Closing BetMGM draw odds (closing odds, from 2019-20) |
| `bmgmca` | numeric | Closing BetMGM away win odds (closing odds, from 2019-20) |
| `bvch` | numeric | Closing Betvictor home win odds (closing odds, from 2019-20) |
| `bvcd` | numeric | Closing Betvictor draw odds (closing odds, from 2019-20) |
| `bvca` | numeric | Closing Betvictor away win odds (closing odds, from 2019-20) |
| `bwch` | numeric | Closing Bet&Win home win odds (closing odds, from 2019-20) |
| `bwcd` | numeric | Closing Bet&Win draw odds (closing odds, from 2019-20) |
| `bwca` | numeric | Closing Bet&Win away win odds (closing odds, from 2019-20) |
| `clch` | numeric | Closing Coral home win odds (closing odds, from 2019-20) |
| `clcd` | numeric | Closing Coral draw odds (closing odds, from 2019-20) |
| `clca` | numeric | Closing Coral away win odds (closing odds, from 2019-20) |
| `lbch` | numeric | Closing Ladbrokes home win odds (closing odds, from 2019-20) |
| `lbcd` | numeric | Closing Ladbrokes draw odds (closing odds, from 2019-20) |
| `lbca` | numeric | Closing Ladbrokes away win odds (closing odds, from 2019-20) |
| `psch` | numeric | Closing Pinnacle home win odds (closing odds, from 2012-13) |
| `pscd` | numeric | Closing Pinnacle draw odds (closing odds, from 2012-13) |
| `psca` | numeric | Closing Pinnacle away win odds (closing odds, from 2012-13) |
| `max_ch` | numeric | Closing Market maximum home win odds (closing odds, from 2019-20) |
| `max_cd` | numeric | Closing Market maximum draw win odds (closing odds, from 2019-20) |
| `max_ca` | numeric | Closing Market maximum away win odds (closing odds, from 2019-20) |
| `avg_ch` | numeric | Closing Market average home win odds (closing odds, from 2019-20) |
| `avg_cd` | numeric | Closing Market average draw win odds (closing odds, from 2019-20) |
| `avg_ca` | numeric | Closing Market average away win odds (closing odds, from 2019-20) |
| `bfech` | numeric | Closing Betfair Exchange home win odds (closing odds, from 2019-20) |
| `bfecd` | numeric | Closing Betfair Exchange draw odds (closing odds, from 2019-20) |
| `bfeca` | numeric | Closing Betfair Exchange away win odds (closing odds, from 2019-20) |
| `b365_c>2.5` | numeric | Closing Bet365 over 2.5 goals (closing odds, from 2019-20) |
| `b365_c<2.5` | numeric | Closing Bet365 under 2.5 goals (closing odds, from 2019-20) |
| `pc>2.5` | numeric | Closing Pinnacle over 2.5 goals (closing odds, from 2019-20) |
| `pc<2.5` | numeric | Closing Pinnacle under 2.5 goals (closing odds, from 2019-20) |
| `max_c>2.5` | numeric | Closing Market maximum over 2.5 goals (closing odds, from 2019-20) |
| `max_c<2.5` | numeric | Closing Market maximum under 2.5 goals (closing odds, from 2019-20) |
| `avg_c>2.5` | numeric | Closing Market average over 2.5 goals (closing odds, from 2019-20) |
| `avg_c<2.5` | numeric | Closing Market average under 2.5 goals (closing odds, from 2019-20) |
| `bfec>2.5` | numeric |  |
| `bfec<2.5` | numeric |  |
| `ah_ch` | numeric | Closing Market size of handicap (home team) (since 2019/2020) |
| `b365_cahh` | numeric | Closing Bet365 Asian handicap home team odds (closing odds, from 2019-20) |
| `b365_caha` | numeric | Closing Bet365 Asian handicap away team odds (closing odds, from 2019-20) |
| `pcahh` | numeric | Closing Pinnacle Asian handicap home team odds (closing odds, from 2019-20) |
| `pcaha` | numeric | Closing Pinnacle Asian handicap away team odds (closing odds, from 2019-20) |
| `max_cahh` | numeric | Closing Market maximum Asian handicap home team odds (closing odds, from 2019-20) |
| `max_caha` | numeric | Closing Market maximum Asian handicap away team odds (closing odds, from 2019-20) |
| `avg_cahh` | numeric | Closing Market average Asian handicap home team odds (closing odds, from 2019-20) |
| `avg_caha` | numeric | Closing Market average Asian handicap away team odds (closing odds, from 2019-20) |
| `bfecahh` | numeric |  |
| `bfecaha` | numeric |  |

**`return_parsed=False`** — the raw CSV response body (`str`).

### Raises {#football_data_league_season-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | football-data.co.uk answered 404 (no file for that season or division). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#football_data_league_season-example}

```python
football_data_league_season(div='E0', season='2526')
```

_Last validated n/a._
