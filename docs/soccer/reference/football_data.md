# Soccer (all) — Football-Data.co.uk CSV archive (football-data.co.uk)

> Soccer (all) — Football-Data.co.uk CSV archive (football-data.co.uk) — endpoint reference in sdv-py, the SportsDataverse Python package.

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
| `country` | String |  |
| `league` | String |  |
| `season` | Int64 |  |
| `date` | String | Match Date (dd/mm/yy) |
| `time` | String | Time of match kick off (from 2019-20) |
| `home` | String |  |
| `away` | String |  |
| `hg` | Int64 | Full Time Home Team Goals |
| `ag` | Int64 | Full Time Away Team Goals |
| `res` | String | Full Time Result (H=Home Win, D=Draw, A=Away Win) |
| `psch` | Float64 | Closing Pinnacle home win odds (closing odds, from 2012-13) |
| `pscd` | Float64 | Closing Pinnacle draw odds (closing odds, from 2012-13) |
| `psca` | Float64 | Closing Pinnacle away win odds (closing odds, from 2012-13) |
| `max_ch` | Float64 | Closing Market maximum home win odds (closing odds, from 2019-20) |
| `max_cd` | Float64 | Closing Market maximum draw win odds (closing odds, from 2019-20) |
| `max_ca` | Float64 | Closing Market maximum away win odds (closing odds, from 2019-20) |
| `avg_ch` | Float64 | Closing Market average home win odds (closing odds, from 2019-20) |
| `avg_cd` | Float64 | Closing Market average draw win odds (closing odds, from 2019-20) |
| `avg_ca` | Float64 | Closing Market average away win odds (closing odds, from 2019-20) |
| `bfech` | String | Closing Betfair Exchange home win odds (closing odds, from 2019-20) |
| `bfecd` | String | Closing Betfair Exchange draw odds (closing odds, from 2019-20) |
| `bfeca` | String | Closing Betfair Exchange away win odds (closing odds, from 2019-20) |
| `b365_ch` | String | Closing Bet365 home win odds (closing odds, from 2019-20) |
| `b365_cd` | String | Closing Bet365 draw odds (closing odds, from 2019-20) |
| `b365_ca` | String | Closing Bet365 away win odds (closing odds, from 2019-20) |

**`return_parsed=False`** — the raw CSV response body (`str`).

### Raises {#football_data_extra_league-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Football-data.co.uk answered 404 (no file for that season or division). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#football_data_extra_league-example}

```python
football_data_extra_league(country='BRA')
```

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
| `div` | String | League Division |
| `date` | String | Match Date (dd/mm/yy) |
| `time` | String | Time of match kick off (from 2019-20) |
| `home_team` | String | Home Team |
| `away_team` | String | Away Team |
| `referee` | String | Match Referee (from 2000-01, results-only files before) |
| `b365_h` | Float64 | Bet365 home win odds (from 2002-03) |
| `b365_d` | Float64 | Bet365 draw odds (from 2002-03) |
| `b365_a` | Float64 | Bet365 away win odds (from 2002-03) |
| `bfdh` | Float64 | Betfred home win odds (from 2025-26) |
| `bfdd` | Float64 | Betfred draw odds (from 2025-26) |
| `bfda` | Float64 | Betfred away win odds (from 2025-26) |
| `bvh` | Float64 | Betvictor home win odds |
| `bvd` | Float64 | Betvictor draw odds |
| `bva` | Float64 | Betvictor away win odds |
| `bwh` | Float64 | Bet&Win home win odds |
| `bwd` | Float64 | Bet&Win draw odds |
| `bwa` | Float64 | Bet&Win away win odds |
| `pph` | Float64 | Paddy Power home win odds |
| `ppd` | Float64 | Paddy Power draw odds |
| `ppa` | Float64 | Paddy Power away win odds |
| `skbh` | Float64 |  |
| `skbd` | Float64 |  |
| `skba` | Float64 |  |
| `max_h` | Float64 | Market maximum home win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `max_d` | Float64 | Market maximum draw win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `max_a` | Float64 | Market maximum away win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg_h` | Float64 | Market average home win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg_d` | Float64 | Market average draw win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg_a` | Float64 | Market average away win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `bfeh` | Float64 | Betfair Exchange home win odds (from 2024-25) |
| `bfed` | Float64 | Betfair Exchange draw odds (from 2024-25) |
| `bfea` | Float64 | Betfair Exchange away win odds (from 2024-25) |
| `b365>2.5` | Float64 | Bet365 over 2.5 goals (from 2002-03) |
| `b365<2.5` | Float64 | Bet365 under 2.5 goals (from 2002-03) |
| `max>2.5` | Float64 | Market maximum over 2.5 goals (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `max<2.5` | Float64 | Market maximum under 2.5 goals (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg>2.5` | Float64 | Market average over 2.5 goals (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg<2.5` | Float64 | Market average under 2.5 goals (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `bfe>2.5` | Float64 |  |
| `bfe<2.5` | Float64 |  |
| `a_hh` | Float64 | Market size of handicap (home team) (since 2019/2020) |
| `b365_ahh` | Float64 | Bet365 Asian handicap home team odds (from 2002-03) |
| `b365_aha` | Float64 | Bet365 Asian handicap away team odds (from 2002-03) |
| `max_ahh` | Float64 | Market maximum Asian handicap home team odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `max_aha` | Float64 | Market maximum Asian handicap away team odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg_ahh` | Float64 | Market average Asian handicap home team odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg_aha` | Float64 | Market average Asian handicap away team odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `bfeahh` | Float64 |  |
| `bfeaha` | Float64 |  |
| `b365_ch` | String | Closing Bet365 home win odds (closing odds, from 2019-20) |
| `b365_cd` | String | Closing Bet365 draw odds (closing odds, from 2019-20) |
| `b365_ca` | String | Closing Bet365 away win odds (closing odds, from 2019-20) |
| `bfdch` | String | Closing Betfred home win odds (closing odds, from 2019-20) |
| `bfdcd` | String | Closing Betfred draw odds (closing odds, from 2019-20) |
| `bfdca` | String | Closing Betfred away win odds (closing odds, from 2019-20) |
| `bvch` | String | Closing Betvictor home win odds (closing odds, from 2019-20) |
| `bvcd` | String | Closing Betvictor draw odds (closing odds, from 2019-20) |
| `bvca` | String | Closing Betvictor away win odds (closing odds, from 2019-20) |
| `bwch` | String | Closing Bet&Win home win odds (closing odds, from 2019-20) |
| `bwcd` | String | Closing Bet&Win draw odds (closing odds, from 2019-20) |
| `bwca` | String | Closing Bet&Win away win odds (closing odds, from 2019-20) |
| `ppch` | String | Closing Paddy Power home win odds |
| `ppcd` | String | Closing Paddy Power draw odds |
| `ppca` | String | Closing Paddy Power away win odds |
| `skbch` | String |  |
| `skbcd` | String |  |
| `skbca` | String |  |
| `max_ch` | String | Closing Market maximum home win odds (closing odds, from 2019-20) |
| `max_cd` | String | Closing Market maximum draw win odds (closing odds, from 2019-20) |
| `max_ca` | String | Closing Market maximum away win odds (closing odds, from 2019-20) |
| `avg_ch` | String | Closing Market average home win odds (closing odds, from 2019-20) |
| `avg_cd` | String | Closing Market average draw win odds (closing odds, from 2019-20) |
| `avg_ca` | String | Closing Market average away win odds (closing odds, from 2019-20) |
| `bfech` | String | Closing Betfair Exchange home win odds (closing odds, from 2019-20) |
| `bfecd` | String | Closing Betfair Exchange draw odds (closing odds, from 2019-20) |
| `bfeca` | String | Closing Betfair Exchange away win odds (closing odds, from 2019-20) |
| `b365_c>2.5` | String | Closing Bet365 over 2.5 goals (closing odds, from 2019-20) |
| `b365_c<2.5` | String | Closing Bet365 under 2.5 goals (closing odds, from 2019-20) |
| `max_c>2.5` | String | Closing Market maximum over 2.5 goals (closing odds, from 2019-20) |
| `max_c<2.5` | String | Closing Market maximum under 2.5 goals (closing odds, from 2019-20) |
| `avg_c>2.5` | String | Closing Market average over 2.5 goals (closing odds, from 2019-20) |
| `avg_c<2.5` | String | Closing Market average under 2.5 goals (closing odds, from 2019-20) |
| `bfec>2.5` | String |  |
| `bfec<2.5` | String |  |
| `ah_ch` | String | Closing Market size of handicap (home team) (since 2019/2020) |
| `b365_cahh` | String | Closing Bet365 Asian handicap home team odds (closing odds, from 2019-20) |
| `b365_caha` | String | Closing Bet365 Asian handicap away team odds (closing odds, from 2019-20) |
| `max_cahh` | String | Closing Market maximum Asian handicap home team odds (closing odds, from 2019-20) |
| `max_caha` | String | Closing Market maximum Asian handicap away team odds (closing odds, from 2019-20) |
| `avg_cahh` | String | Closing Market average Asian handicap home team odds (closing odds, from 2019-20) |
| `avg_caha` | String | Closing Market average Asian handicap away team odds (closing odds, from 2019-20) |
| `bfecahh` | String |  |
| `bfecaha` | String |  |

**`return_parsed=False`** — the raw CSV response body (`str`).

### Raises {#football_data_fixtures-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Football-data.co.uk answered 404 (no file for that season or division). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#football_data_fixtures-example}

```python
football_data_fixtures()
```

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
| `div` | String | League Division |
| `date` | String | Match Date (dd/mm/yy) |
| `time` | String | Time of match kick off (from 2019-20) |
| `home_team` | String | Home Team |
| `away_team` | String | Away Team |
| `fthg` | Int64 | Full Time Home Team Goals |
| `ftag` | Int64 | Full Time Away Team Goals |
| `ftr` | String | Full Time Result (H=Home Win, D=Draw, A=Away Win) |
| `hthg` | Int64 | Half Time Home Team Goals |
| `htag` | Int64 | Half Time Away Team Goals |
| `htr` | String | Half Time Result (H=Home Win, D=Draw, A=Away Win) |
| `referee` | String | Match Referee (from 2000-01, results-only files before) |
| `hs` | Int64 | Home Team Shots (from 2000-01, results-only files before) |
| `as` | Int64 | Away Team Shots (from 2000-01, results-only files before) |
| `hst` | Int64 | Home Team Shots on Target (from 2000-01, results-only files before) |
| `ast` | Int64 | Away Team Shots on Target (from 2000-01, results-only files before) |
| `hf` | Int64 | Home Team Fouls Committed (from 2000-01, results-only files before) |
| `af` | Int64 | Away Team Fouls Committed (from 2000-01, results-only files before) |
| `hc` | Int64 | Home Team Corners (from 2000-01, results-only files before) |
| `ac` | Int64 | Away Team Corners (from 2000-01, results-only files before) |
| `hy` | Int64 | Home Team Yellow Cards (from 2000-01, results-only files before) |
| `ay` | Int64 | Away Team Yellow Cards (from 2000-01, results-only files before) |
| `hr` | Int64 | Home Team Red Cards (from 2000-01, results-only files before) |
| `ar` | Int64 | Away Team Red Cards (from 2000-01, results-only files before) |
| `b365_h` | Float64 | Bet365 home win odds (from 2002-03) |
| `b365_d` | Float64 | Bet365 draw odds (from 2002-03) |
| `b365_a` | Float64 | Bet365 away win odds (from 2002-03) |
| `bfdh` | Float64 | Betfred home win odds (from 2025-26) |
| `bfdd` | Float64 | Betfred draw odds (from 2025-26) |
| `bfda` | Float64 | Betfred away win odds (from 2025-26) |
| `bmgmh` | Float64 | BetMGM home win odds |
| `bmgmd` | Float64 | BetMGM draw odds |
| `bmgma` | Float64 | BetMGM away win odds |
| `bvh` | Float64 | Betvictor home win odds |
| `bvd` | Float64 | Betvictor draw odds |
| `bva` | Float64 | Betvictor away win odds |
| `bwh` | Float64 | Bet&Win home win odds |
| `bwd` | Float64 | Bet&Win draw odds |
| `bwa` | Float64 | Bet&Win away win odds |
| `clh` | Float64 | Coral home win odds |
| `cld` | Float64 | Coral draw odds |
| `cla` | Float64 | Coral away win odds |
| `lbh` | Float64 | Ladbrokes home win odds |
| `lbd` | Float64 | Ladbrokes draw odds |
| `lba` | Float64 | Ladbrokes away win odds |
| `psh` | Float64 | Pinnacle home win odds (from 2012-13) |
| `psd` | Float64 | Pinnacle draw odds (from 2012-13) |
| `psa` | Float64 | Pinnacle away win odds (from 2012-13) |
| `max_h` | Float64 | Market maximum home win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `max_d` | Float64 | Market maximum draw win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `max_a` | Float64 | Market maximum away win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg_h` | Float64 | Market average home win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg_d` | Float64 | Market average draw win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg_a` | Float64 | Market average away win odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `bfeh` | Float64 | Betfair Exchange home win odds (from 2024-25) |
| `bfed` | Float64 | Betfair Exchange draw odds (from 2024-25) |
| `bfea` | Float64 | Betfair Exchange away win odds (from 2024-25) |
| `b365>2.5` | Float64 | Bet365 over 2.5 goals (from 2002-03) |
| `b365<2.5` | Float64 | Bet365 under 2.5 goals (from 2002-03) |
| `p>2.5` | Float64 | Pinnacle over 2.5 goals (from 2012-13) |
| `p<2.5` | Float64 | Pinnacle under 2.5 goals (from 2012-13) |
| `max>2.5` | Float64 | Market maximum over 2.5 goals (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `max<2.5` | Float64 | Market maximum under 2.5 goals (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg>2.5` | Float64 | Market average over 2.5 goals (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg<2.5` | Float64 | Market average under 2.5 goals (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `bfe>2.5` | Float64 |  |
| `bfe<2.5` | Float64 |  |
| `a_hh` | Float64 | Market size of handicap (home team) (since 2019/2020) |
| `b365_ahh` | Float64 | Bet365 Asian handicap home team odds (from 2002-03) |
| `b365_aha` | Float64 | Bet365 Asian handicap away team odds (from 2002-03) |
| `pahh` | Float64 | Pinnacle Asian handicap home team odds (from 2012-13) |
| `paha` | Float64 | Pinnacle Asian handicap away team odds (from 2012-13) |
| `max_ahh` | Float64 | Market maximum Asian handicap home team odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `max_aha` | Float64 | Market maximum Asian handicap away team odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg_ahh` | Float64 | Market average Asian handicap home team odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `avg_aha` | Float64 | Market average Asian handicap away team odds (from 2005-06, as Betbrain Bb* averages until 2018-19 and under the Max/Avg names from 2019-20) |
| `bfeahh` | Float64 |  |
| `bfeaha` | Float64 |  |
| `b365_ch` | Float64 | Closing Bet365 home win odds (closing odds, from 2019-20) |
| `b365_cd` | Float64 | Closing Bet365 draw odds (closing odds, from 2019-20) |
| `b365_ca` | Float64 | Closing Bet365 away win odds (closing odds, from 2019-20) |
| `bfdch` | Float64 | Closing Betfred home win odds (closing odds, from 2019-20) |
| `bfdcd` | Float64 | Closing Betfred draw odds (closing odds, from 2019-20) |
| `bfdca` | Float64 | Closing Betfred away win odds (closing odds, from 2019-20) |
| `bmgmch` | Float64 | Closing BetMGM home win odds (closing odds, from 2019-20) |
| `bmgmcd` | Float64 | Closing BetMGM draw odds (closing odds, from 2019-20) |
| `bmgmca` | Float64 | Closing BetMGM away win odds (closing odds, from 2019-20) |
| `bvch` | Float64 | Closing Betvictor home win odds (closing odds, from 2019-20) |
| `bvcd` | Float64 | Closing Betvictor draw odds (closing odds, from 2019-20) |
| `bvca` | Float64 | Closing Betvictor away win odds (closing odds, from 2019-20) |
| `bwch` | Float64 | Closing Bet&Win home win odds (closing odds, from 2019-20) |
| `bwcd` | Float64 | Closing Bet&Win draw odds (closing odds, from 2019-20) |
| `bwca` | Float64 | Closing Bet&Win away win odds (closing odds, from 2019-20) |
| `clch` | Float64 | Closing Coral home win odds (closing odds, from 2019-20) |
| `clcd` | Float64 | Closing Coral draw odds (closing odds, from 2019-20) |
| `clca` | Float64 | Closing Coral away win odds (closing odds, from 2019-20) |
| `lbch` | Float64 | Closing Ladbrokes home win odds (closing odds, from 2019-20) |
| `lbcd` | Float64 | Closing Ladbrokes draw odds (closing odds, from 2019-20) |
| `lbca` | Float64 | Closing Ladbrokes away win odds (closing odds, from 2019-20) |
| `psch` | Float64 | Closing Pinnacle home win odds (closing odds, from 2012-13) |
| `pscd` | Float64 | Closing Pinnacle draw odds (closing odds, from 2012-13) |
| `psca` | Float64 | Closing Pinnacle away win odds (closing odds, from 2012-13) |
| `max_ch` | Float64 | Closing Market maximum home win odds (closing odds, from 2019-20) |
| `max_cd` | Float64 | Closing Market maximum draw win odds (closing odds, from 2019-20) |
| `max_ca` | Float64 | Closing Market maximum away win odds (closing odds, from 2019-20) |
| `avg_ch` | Float64 | Closing Market average home win odds (closing odds, from 2019-20) |
| `avg_cd` | Float64 | Closing Market average draw win odds (closing odds, from 2019-20) |
| `avg_ca` | Float64 | Closing Market average away win odds (closing odds, from 2019-20) |
| `bfech` | Float64 | Closing Betfair Exchange home win odds (closing odds, from 2019-20) |
| `bfecd` | Float64 | Closing Betfair Exchange draw odds (closing odds, from 2019-20) |
| `bfeca` | Float64 | Closing Betfair Exchange away win odds (closing odds, from 2019-20) |
| `b365_c>2.5` | Float64 | Closing Bet365 over 2.5 goals (closing odds, from 2019-20) |
| `b365_c<2.5` | Float64 | Closing Bet365 under 2.5 goals (closing odds, from 2019-20) |
| `pc>2.5` | Float64 | Closing Pinnacle over 2.5 goals (closing odds, from 2019-20) |
| `pc<2.5` | Float64 | Closing Pinnacle under 2.5 goals (closing odds, from 2019-20) |
| `max_c>2.5` | Float64 | Closing Market maximum over 2.5 goals (closing odds, from 2019-20) |
| `max_c<2.5` | Float64 | Closing Market maximum under 2.5 goals (closing odds, from 2019-20) |
| `avg_c>2.5` | Float64 | Closing Market average over 2.5 goals (closing odds, from 2019-20) |
| `avg_c<2.5` | Float64 | Closing Market average under 2.5 goals (closing odds, from 2019-20) |
| `bfec>2.5` | Float64 |  |
| `bfec<2.5` | Float64 |  |
| `ah_ch` | Float64 | Closing Market size of handicap (home team) (since 2019/2020) |
| `b365_cahh` | Float64 | Closing Bet365 Asian handicap home team odds (closing odds, from 2019-20) |
| `b365_caha` | Float64 | Closing Bet365 Asian handicap away team odds (closing odds, from 2019-20) |
| `pcahh` | Float64 | Closing Pinnacle Asian handicap home team odds (closing odds, from 2019-20) |
| `pcaha` | Float64 | Closing Pinnacle Asian handicap away team odds (closing odds, from 2019-20) |
| `max_cahh` | Float64 | Closing Market maximum Asian handicap home team odds (closing odds, from 2019-20) |
| `max_caha` | Float64 | Closing Market maximum Asian handicap away team odds (closing odds, from 2019-20) |
| `avg_cahh` | Float64 | Closing Market average Asian handicap home team odds (closing odds, from 2019-20) |
| `avg_caha` | Float64 | Closing Market average Asian handicap away team odds (closing odds, from 2019-20) |
| `bfecahh` | Float64 |  |
| `bfecaha` | Float64 |  |

**`return_parsed=False`** — the raw CSV response body (`str`).

### Raises {#football_data_league_season-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Football-data.co.uk answered 404 (no file for that season or division). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#football_data_league_season-example}

```python
football_data_league_season(div='E0', season='2526')
```
