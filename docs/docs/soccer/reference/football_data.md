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
| `country` | `country` |  | `Y` |  | country path parameter. |

### Returns {#football_data_extra_league-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `country` | character | Venue country. |
| `league` | character | League slug. |
| `season` | integer | Season year. |
| `date` | character | Match Date (dd/mm/yy) |
| `time` | character | Time of match kick off |
| `home` | character | Home. |
| `away` | character | Away team shots in the period. |
| `hg` | integer | Full Time Home Team Goals |
| `ag` | integer | Full Time Away Team Goals |
| `res` | character | Full Time Result (H=Home Win, D=Draw, A=Away Win) |
| `psch` | numeric | Closing Pinnacle home win odds |
| `pscd` | numeric | Closing Pinnacle draw odds |
| `psca` | numeric | Closing Pinnacle away win odds |
| `max_ch` | numeric | Closing Market maximum home win odds |
| `max_cd` | numeric | Closing Market maximum draw win odds |
| `max_ca` | numeric | Closing Market maximum away win odds |
| `avg_ch` | numeric | Closing Market average home win odds |
| `avg_cd` | numeric | Closing Market average draw win odds |
| `avg_ca` | numeric | Closing Market average away win odds |
| `bfech` | character | Closing Betfair Exchange home win odds |
| `bfecd` | character | Closing Betfair Exchange draw odds |
| `bfeca` | character | Closing Betfair Exchange away win odds |
| `b365_ch` | character | Closing Bet365 home win odds |
| `b365_cd` | character | Closing Bet365 draw odds |
| `b365_ca` | character | Closing Bet365 away win odds |

**`return_parsed=False`** — the raw CSV response body (`str`).

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
| `time` | character | Time of match kick off |
| `home_team` | character | Home Team |
| `away_team` | character | Away Team |
| `referee` | character | Match Referee |
| `b365_h` | numeric | Bet365 home win odds |
| `b365_d` | numeric | Bet365 draw odds |
| `b365_a` | numeric | Bet365 away win odds |
| `bfdh` | numeric | Betfred home win odds |
| `bfdd` | numeric | Betfred draw odds |
| `bfda` | numeric | Betfred away win odds |
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
| `max_h` | numeric | Market maximum home win odds |
| `max_d` | numeric | Market maximum draw win odds |
| `max_a` | numeric | Market maximum away win odds |
| `avg_h` | numeric | Market average home win odds |
| `avg_d` | numeric | Market average draw win odds |
| `avg_a` | numeric | Market average away win odds |
| `bfeh` | numeric | Betfair Exchange home win odds |
| `bfed` | numeric | Betfair Exchange draw odds |
| `bfea` | numeric | Betfair Exchange away win odds |
| `b365>2.5` | numeric | Bet365 over 2.5 goals |
| `b365<2.5` | numeric | Bet365 under 2.5 goals |
| `max>2.5` | numeric | Market maximum over 2.5 goals |
| `max<2.5` | numeric | Market maximum under 2.5 goals |
| `avg>2.5` | numeric | Market average over 2.5 goals |
| `avg<2.5` | numeric | Market average under 2.5 goals |
| `bfe>2.5` | numeric |  |
| `bfe<2.5` | numeric |  |
| `a_hh` | numeric | Market size of handicap (home team) (since 2019/2020) |
| `b365_ahh` | numeric | Bet365 Asian handicap home team odds |
| `b365_aha` | numeric | Bet365 Asian handicap away team odds |
| `max_ahh` | numeric | Market maximum Asian handicap home team odds |
| `max_aha` | numeric | Market maximum Asian handicap away team odds |
| `avg_ahh` | numeric | Market average Asian handicap home team odds |
| `avg_aha` | numeric | Market average Asian handicap away team odds |
| `bfeahh` | numeric |  |
| `bfeaha` | numeric |  |
| `b365_ch` | character | Closing Bet365 home win odds |
| `b365_cd` | character | Closing Bet365 draw odds |
| `b365_ca` | character | Closing Bet365 away win odds |
| `bfdch` | character | Closing Betfred home win odds |
| `bfdcd` | character | Closing Betfred draw odds |
| `bfdca` | character | Closing Betfred away win odds |
| `bvch` | character | Closing Betvictor home win odds |
| `bvcd` | character | Closing Betvictor draw odds |
| `bvca` | character | Closing Betvictor away win odds |
| `bwch` | character | Closing Bet&Win home win odds |
| `bwcd` | character | Closing Bet&Win draw odds |
| `bwca` | character | Closing Bet&Win away win odds |
| `ppch` | character | Closing Paddy Power home win odds |
| `ppcd` | character | Closing Paddy Power draw odds |
| `ppca` | character | Closing Paddy Power away win odds |
| `skbch` | character |  |
| `skbcd` | character |  |
| `skbca` | character |  |
| `max_ch` | character | Closing Market maximum home win odds |
| `max_cd` | character | Closing Market maximum draw win odds |
| `max_ca` | character | Closing Market maximum away win odds |
| `avg_ch` | character | Closing Market average home win odds |
| `avg_cd` | character | Closing Market average draw win odds |
| `avg_ca` | character | Closing Market average away win odds |
| `bfech` | character | Closing Betfair Exchange home win odds |
| `bfecd` | character | Closing Betfair Exchange draw odds |
| `bfeca` | character | Closing Betfair Exchange away win odds |
| `b365_c>2.5` | character | Closing Bet365 over 2.5 goals |
| `b365_c<2.5` | character | Closing Bet365 under 2.5 goals |
| `max_c>2.5` | character | Closing Market maximum over 2.5 goals |
| `max_c<2.5` | character | Closing Market maximum under 2.5 goals |
| `avg_c>2.5` | character | Closing Market average over 2.5 goals |
| `avg_c<2.5` | character | Closing Market average under 2.5 goals |
| `bfec>2.5` | character |  |
| `bfec<2.5` | character |  |
| `ah_ch` | character | Closing Market size of handicap (home team) (since 2019/2020) |
| `b365_cahh` | character | Closing Bet365 Asian handicap home team odds |
| `b365_caha` | character | Closing Bet365 Asian handicap away team odds |
| `max_cahh` | character | Closing Market maximum Asian handicap home team odds |
| `max_caha` | character | Closing Market maximum Asian handicap away team odds |
| `avg_cahh` | character | Closing Market average Asian handicap home team odds |
| `avg_caha` | character | Closing Market average Asian handicap away team odds |
| `bfecahh` | character |  |
| `bfecaha` | character |  |

**`return_parsed=False`** — the raw CSV response body (`str`).

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
| `season` | `season` |  | `Y` |  | season path parameter. |
| `div` | `div` |  | `Y` |  | div path parameter. |

### Returns {#football_data_league_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `div` | character | League Division |
| `date` | character | Match Date (dd/mm/yy) |
| `time` | character | Time of match kick off |
| `home_team` | character | Home Team |
| `away_team` | character | Away Team |
| `fthg` | integer | Full Time Home Team Goals |
| `ftag` | integer | Full Time Away Team Goals |
| `ftr` | character | Full Time Result (H=Home Win, D=Draw, A=Away Win) |
| `hthg` | integer | Half Time Home Team Goals |
| `htag` | integer | Half Time Away Team Goals |
| `htr` | character | Half Time Result (H=Home Win, D=Draw, A=Away Win) |
| `referee` | character | Match Referee |
| `hs` | integer | Home Team Shots |
| `as` | integer | Away Team Shots |
| `hst` | integer | Home Team Shots on Target |
| `ast` | integer | Away Team Shots on Target |
| `hf` | integer | Home Team Fouls Committed |
| `af` | integer | Away Team Fouls Committed |
| `hc` | integer | Home Team Corners |
| `ac` | integer | Away Team Corners |
| `hy` | integer | Home Team Yellow Cards |
| `ay` | integer | Away Team Yellow Cards |
| `hr` | integer | Home Team Red Cards |
| `ar` | integer | Away Team Red Cards |
| `b365_h` | numeric | Bet365 home win odds |
| `b365_d` | numeric | Bet365 draw odds |
| `b365_a` | numeric | Bet365 away win odds |
| `bfdh` | numeric | Betfred home win odds |
| `bfdd` | numeric | Betfred draw odds |
| `bfda` | numeric | Betfred away win odds |
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
| `psh` | numeric | Pinnacle home win odds |
| `psd` | numeric | Pinnacle draw odds |
| `psa` | numeric | Pinnacle away win odds |
| `max_h` | numeric | Market maximum home win odds |
| `max_d` | numeric | Market maximum draw win odds |
| `max_a` | numeric | Market maximum away win odds |
| `avg_h` | numeric | Market average home win odds |
| `avg_d` | numeric | Market average draw win odds |
| `avg_a` | numeric | Market average away win odds |
| `bfeh` | numeric | Betfair Exchange home win odds |
| `bfed` | numeric | Betfair Exchange draw odds |
| `bfea` | numeric | Betfair Exchange away win odds |
| `b365>2.5` | numeric | Bet365 over 2.5 goals |
| `b365<2.5` | numeric | Bet365 under 2.5 goals |
| `p>2.5` | numeric | Pinnacle over 2.5 goals |
| `p<2.5` | numeric | Pinnacle under 2.5 goals |
| `max>2.5` | numeric | Market maximum over 2.5 goals |
| `max<2.5` | numeric | Market maximum under 2.5 goals |
| `avg>2.5` | numeric | Market average over 2.5 goals |
| `avg<2.5` | numeric | Market average under 2.5 goals |
| `bfe>2.5` | numeric |  |
| `bfe<2.5` | numeric |  |
| `a_hh` | numeric | Market size of handicap (home team) (since 2019/2020) |
| `b365_ahh` | numeric | Bet365 Asian handicap home team odds |
| `b365_aha` | numeric | Bet365 Asian handicap away team odds |
| `pahh` | numeric | Pinnacle Asian handicap home team odds |
| `paha` | numeric | Pinnacle Asian handicap away team odds |
| `max_ahh` | numeric | Market maximum Asian handicap home team odds |
| `max_aha` | numeric | Market maximum Asian handicap away team odds |
| `avg_ahh` | numeric | Market average Asian handicap home team odds |
| `avg_aha` | numeric | Market average Asian handicap away team odds |
| `bfeahh` | numeric |  |
| `bfeaha` | numeric |  |
| `b365_ch` | numeric | Closing Bet365 home win odds |
| `b365_cd` | numeric | Closing Bet365 draw odds |
| `b365_ca` | numeric | Closing Bet365 away win odds |
| `bfdch` | numeric | Closing Betfred home win odds |
| `bfdcd` | numeric | Closing Betfred draw odds |
| `bfdca` | numeric | Closing Betfred away win odds |
| `bmgmch` | numeric | Closing BetMGM home win odds |
| `bmgmcd` | numeric | Closing BetMGM draw odds |
| `bmgmca` | numeric | Closing BetMGM away win odds |
| `bvch` | numeric | Closing Betvictor home win odds |
| `bvcd` | numeric | Closing Betvictor draw odds |
| `bvca` | numeric | Closing Betvictor away win odds |
| `bwch` | numeric | Closing Bet&Win home win odds |
| `bwcd` | numeric | Closing Bet&Win draw odds |
| `bwca` | numeric | Closing Bet&Win away win odds |
| `clch` | numeric | Closing Coral home win odds |
| `clcd` | numeric | Closing Coral draw odds |
| `clca` | numeric | Closing Coral away win odds |
| `lbch` | numeric | Closing Ladbrokes home win odds |
| `lbcd` | numeric | Closing Ladbrokes draw odds |
| `lbca` | numeric | Closing Ladbrokes away win odds |
| `psch` | numeric | Closing Pinnacle home win odds |
| `pscd` | numeric | Closing Pinnacle draw odds |
| `psca` | numeric | Closing Pinnacle away win odds |
| `max_ch` | numeric | Closing Market maximum home win odds |
| `max_cd` | numeric | Closing Market maximum draw win odds |
| `max_ca` | numeric | Closing Market maximum away win odds |
| `avg_ch` | numeric | Closing Market average home win odds |
| `avg_cd` | numeric | Closing Market average draw win odds |
| `avg_ca` | numeric | Closing Market average away win odds |
| `bfech` | numeric | Closing Betfair Exchange home win odds |
| `bfecd` | numeric | Closing Betfair Exchange draw odds |
| `bfeca` | numeric | Closing Betfair Exchange away win odds |
| `b365_c>2.5` | numeric | Closing Bet365 over 2.5 goals |
| `b365_c<2.5` | numeric | Closing Bet365 under 2.5 goals |
| `pc>2.5` | numeric | Closing Pinnacle over 2.5 goals |
| `pc<2.5` | numeric | Closing Pinnacle under 2.5 goals |
| `max_c>2.5` | numeric | Closing Market maximum over 2.5 goals |
| `max_c<2.5` | numeric | Closing Market maximum under 2.5 goals |
| `avg_c>2.5` | numeric | Closing Market average over 2.5 goals |
| `avg_c<2.5` | numeric | Closing Market average under 2.5 goals |
| `bfec>2.5` | numeric |  |
| `bfec<2.5` | numeric |  |
| `ah_ch` | numeric | Closing Market size of handicap (home team) (since 2019/2020) |
| `b365_cahh` | numeric | Closing Bet365 Asian handicap home team odds |
| `b365_caha` | numeric | Closing Bet365 Asian handicap away team odds |
| `pcahh` | numeric | Closing Pinnacle Asian handicap home team odds |
| `pcaha` | numeric | Closing Pinnacle Asian handicap away team odds |
| `max_cahh` | numeric | Closing Market maximum Asian handicap home team odds |
| `max_caha` | numeric | Closing Market maximum Asian handicap away team odds |
| `avg_cahh` | numeric | Closing Market average Asian handicap home team odds |
| `avg_caha` | numeric | Closing Market average Asian handicap away team odds |
| `bfecahh` | numeric |  |
| `bfecaha` | numeric |  |

**`return_parsed=False`** — the raw CSV response body (`str`).

### Example {#football_data_league_season-example}

```python
football_data_league_season(div='E0', season='2526')
```

_Last validated n/a._
