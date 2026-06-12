---
title: Betting odds tutorial
sidebar_label: Betting odds
sidebar_position: 12
---

# Betting odds - `sportsdataverse-py`

`sportsdataverse.odds` wraps **[The Odds API](https://the-odds-api.com) v4** -
live and historical betting odds, scores, events, markets and participants
across a wide range of bookmakers and sports. The `toa_*` functions mirror the
sister R package [oddsapiR](https://oddsapir.sportsdataverse.org).

**API key.** The Odds API needs a key (free tier at
<https://the-odds-api.com/#get-access>). Set it once as `ODDS_API_KEY` (the
same variable `oddsapiR` uses) or pass `api_key=` per call. The live cells
below run only when a key is present, so this page renders either way.

## Setup

```sh
pip install sportsdataverse
```


```python
import os
import sportsdataverse.odds as odds

HAS_KEY = bool(os.environ.get("ODDS_API_KEY"))
print("ODDS_API_KEY set:", HAS_KEY)
```

    ODDS_API_KEY set: True


## What sports are available?

`toa_sports()` lists every in-season sport/league key (the value you pass as
`sport=`). It is a **free** call - it does not spend quota.


```python
if HAS_KEY:
    sports = odds.toa_sports(all_sports=True)
    out = sports.select([c for c in ["key", "group", "title", "active"] if c in sports.columns]).head(10)
else:
    out = "set ODDS_API_KEY to run: odds.toa_sports(all_sports=True)"
out
```




    shape: (10, 4)
    ┌─────────────────────────────────┬───────────────────┬───────────────────────────┬────────┐
    │ key                             ┆ group             ┆ title                     ┆ active │
    │ ---                             ┆ ---               ┆ ---                       ┆ ---    │
    │ str                             ┆ str               ┆ str                       ┆ bool   │
    ╞═════════════════════════════════╪═══════════════════╪═══════════════════════════╪════════╡
    │ americanfootball_cfl            ┆ American Football ┆ CFL                       ┆ true   │
    │ americanfootball_ncaaf          ┆ American Football ┆ NCAAF                     ┆ true   │
    │ americanfootball_ncaaf_champio… ┆ American Football ┆ NCAAF Championship Winner ┆ true   │
    │ americanfootball_nfl            ┆ American Football ┆ NFL                       ┆ true   │
    │ americanfootball_nfl_preseason  ┆ American Football ┆ NFL Preseason             ┆ true   │
    │ americanfootball_nfl_super_bow… ┆ American Football ┆ NFL Super Bowl Winner     ┆ true   │
    │ americanfootball_ufl            ┆ American Football ┆ UFL                       ┆ true   │
    │ aussierules_afl                 ┆ Aussie Rules      ┆ AFL                       ┆ true   │
    │ baseball_kbo                    ┆ Baseball          ┆ KBO                       ┆ true   │
    │ baseball_milb                   ┆ Baseball          ┆ MiLB                      ┆ false  │
    └─────────────────────────────────┴───────────────────┴───────────────────────────┴────────┘



## Current odds - long format

`toa_sports_odds(sport=..., regions=..., markets=...)` returns **one row per
outcome** (event x bookmaker x market x outcome) - the tidy shape modelling
wants. `regions` picks bookmaker regions (`us`/`uk`/`eu`/`au`); `markets` is a
comma-separated list (`h2h`, `spreads`, `totals`, ...).


```python
if HAS_KEY:
    odds_df = odds.toa_sports_odds(sport="americanfootball_nfl", regions="us", markets="h2h,spreads")
    keep = ["home_team", "away_team", "bookmaker_key", "market_key", "outcome_name", "outcome_price"]
    out = odds_df.select([c for c in keep if c in odds_df.columns]).head()
else:
    out = "set ODDS_API_KEY to run: odds.toa_sports_odds(sport='americanfootball_nfl', regions='us')"
out
```




    shape: (5, 6)
    ┌───────────┬─────────────┬───────────────┬────────────┬──────────────────────┬───────────────┐
    │ home_team ┆ away_team   ┆ bookmaker_key ┆ market_key ┆ outcome_name         ┆ outcome_price │
    │ ---       ┆ ---         ┆ ---           ┆ ---        ┆ ---                  ┆ ---           │
    │ str       ┆ str         ┆ str           ┆ str        ┆ str                  ┆ i64           │
    ╞═══════════╪═════════════╪═══════════════╪════════════╪══════════════════════╪═══════════════╡
    │ Seattle   ┆ New England ┆ draftkings    ┆ h2h        ┆ New England Patriots ┆ 170           │
    │ Seahawks  ┆ Patriots    ┆               ┆            ┆                      ┆               │
    │ Seattle   ┆ New England ┆ draftkings    ┆ h2h        ┆ Seattle Seahawks     ┆ -205          │
    │ Seahawks  ┆ Patriots    ┆               ┆            ┆                      ┆               │
    │ Seattle   ┆ New England ┆ draftkings    ┆ spreads    ┆ New England Patriots ┆ -105          │
    │ Seahawks  ┆ Patriots    ┆               ┆            ┆                      ┆               │
    │ Seattle   ┆ New England ┆ draftkings    ┆ spreads    ┆ Seattle Seahawks     ┆ -115          │
    │ Seahawks  ┆ Patriots    ┆               ┆            ┆                      ┆               │
    │ Seattle   ┆ New England ┆ betus         ┆ h2h        ┆ New England Patriots ┆ 170           │
    │ Seahawks  ┆ Patriots    ┆               ┆            ┆                      ┆               │
    └───────────┴─────────────┴───────────────┴────────────┴──────────────────────┴───────────────┘



## Scores & events

`toa_sports_scores(...)` for live/recent scores, `toa_sports_events(...)` for
the upcoming event list (both free).


```python
if HAS_KEY:
    out = odds.toa_sports_scores(sport="americanfootball_nfl", days_from=3).head()
else:
    out = "set ODDS_API_KEY to run: odds.toa_sports_scores(sport='americanfootball_nfl', days_from=3)"
out
```




    shape: (5, 9)
    ┌────────────┬────────────┬───────────┬───────────┬───┬───────────┬───────────┬────────┬───────────┐
    │ id         ┆ sport_key  ┆ sport_tit ┆ commence_ ┆ … ┆ home_team ┆ away_team ┆ scores ┆ last_upda │
    │ ---        ┆ ---        ┆ le        ┆ time      ┆   ┆ ---       ┆ ---       ┆ ---    ┆ te        │
    │ str        ┆ str        ┆ ---       ┆ ---       ┆   ┆ str       ┆ str       ┆ str    ┆ ---       │
    │            ┆            ┆ str       ┆ str       ┆   ┆           ┆           ┆        ┆ str       │
    ╞════════════╪════════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪════════╪═══════════╡
    │ 8c94552d02 ┆ americanfo ┆ NFL       ┆ 2026-09-1 ┆ … ┆ Seattle   ┆ New       ┆ null   ┆ null      │
    │ 2acec4a045 ┆ otball_nfl ┆           ┆ 0T00:15:0 ┆   ┆ Seahawks  ┆ England   ┆        ┆           │
    │ 8d70c19d3d ┆            ┆           ┆ 0Z        ┆   ┆           ┆ Patriots  ┆        ┆           │
    │ …          ┆            ┆           ┆           ┆   ┆           ┆           ┆        ┆           │
    │ acc580d743 ┆ americanfo ┆ NFL       ┆ 2026-09-1 ┆ … ┆ Los       ┆ San       ┆ null   ┆ null      │
    │ 44ea3b31bb ┆ otball_nfl ┆           ┆ 1T00:35:0 ┆   ┆ Angeles   ┆ Francisco ┆        ┆           │
    │ cdd057fe6a ┆            ┆           ┆ 0Z        ┆   ┆ Rams      ┆ 49ers     ┆        ┆           │
    │ …          ┆            ┆           ┆           ┆   ┆           ┆           ┆        ┆           │
    │ 95c01d1bb7 ┆ americanfo ┆ NFL       ┆ 2026-09-1 ┆ … ┆ Pittsburg ┆ Atlanta   ┆ null   ┆ null      │
    │ 97d6df1482 ┆ otball_nfl ┆           ┆ 3T17:00:0 ┆   ┆ h         ┆ Falcons   ┆        ┆           │
    │ 4b106c5a91 ┆            ┆           ┆ 0Z        ┆   ┆ Steelers  ┆           ┆        ┆           │
    │ …          ┆            ┆           ┆           ┆   ┆           ┆           ┆        ┆           │
    │ b6cfdcbafa ┆ americanfo ┆ NFL       ┆ 2026-09-1 ┆ … ┆ Indianapo ┆ Baltimore ┆ null   ┆ null      │
    │ 61ce220ba8 ┆ otball_nfl ┆           ┆ 3T17:00:0 ┆   ┆ lis Colts ┆ Ravens    ┆        ┆           │
    │ 7dc2d9b80c ┆            ┆           ┆ 0Z        ┆   ┆           ┆           ┆        ┆           │
    │ …          ┆            ┆           ┆           ┆   ┆           ┆           ┆        ┆           │
    │ 7e09efed7e ┆ americanfo ┆ NFL       ┆ 2026-09-1 ┆ … ┆ Houston   ┆ Buffalo   ┆ null   ┆ null      │
    │ 12c659b827 ┆ otball_nfl ┆           ┆ 3T17:00:0 ┆   ┆ Texans    ┆ Bills     ┆        ┆           │
    │ 40b67ce2f9 ┆            ┆           ┆ 0Z        ┆   ┆           ┆           ┆        ┆           │
    │ …          ┆            ┆           ┆           ┆   ┆           ┆           ┆        ┆           │
    └────────────┴────────────┴───────────┴───────────┴───┴───────────┴───────────┴────────┴───────────┘



## Player props for one event

Event-level markets (player props) come from
`toa_event_odds(sport, event_id, markets=...)`. Grab an `event_id` from
`toa_sports_events`, then request a player-prop market.


```python
if HAS_KEY:
    events = odds.toa_sports_events(sport="americanfootball_nfl", return_parsed=False)
    if events:
        eid = events[0]["id"]
        props = odds.toa_event_odds(sport="americanfootball_nfl", event_id=eid, markets="player_pass_tds")
        out = props.head()
    else:
        out = "no upcoming NFL events right now"
else:
    out = "set ODDS_API_KEY to run: odds.toa_event_odds(sport=..., event_id=..., markets='player_pass_tds')"
out
```




    shape: (1, 6)
    ┌───────────────────┬───────────────────┬─────────────┬──────────────────┬───────────┬─────────────┐
    │ event_id          ┆ sport_key         ┆ sport_title ┆ commence_time    ┆ home_team ┆ away_team   │
    │ ---               ┆ ---               ┆ ---         ┆ ---              ┆ ---       ┆ ---         │
    │ str               ┆ str               ┆ str         ┆ str              ┆ str       ┆ str         │
    ╞═══════════════════╪═══════════════════╪═════════════╪══════════════════╪═══════════╪═════════════╡
    │ 8c94552d022acec4a ┆ americanfootball_ ┆ NFL         ┆ 2026-09-10T00:15 ┆ Seattle   ┆ New England │
    │ 0458d70c19d3d…    ┆ nfl               ┆             ┆ :00Z             ┆ Seahawks  ┆ Patriots    │
    └───────────────────┴───────────────────┴─────────────┴──────────────────┴───────────┴─────────────┘



## Quota usage

Every call returns `x-requests-remaining` / `x-requests-used` headers;
`toa_usage()` reads the most recent pair **without** spending a request.


```python
odds.toa_usage() if HAS_KEY else "set ODDS_API_KEY to track quota with odds.toa_usage()"
```




    shape: (1, 3)
    ┌────────────────────┬───────────────┬───────────┐
    │ requests_remaining ┆ requests_used ┆ last_cost │
    │ ---                ┆ ---           ┆ ---       │
    │ i64                ┆ i64           ┆ i64       │
    ╞════════════════════╪═══════════════╪═══════════╡
    │ 19812              ┆ 188           ┆ 0         │
    └────────────────────┴───────────────┴───────────┘



## Historical odds

Paid plans can pull point-in-time snapshots with `toa_sports_odds_history(...)`,
`toa_sports_events_history(...)` and `toa_event_odds_history(...)` - pass a
`date=` ISO8601 timestamp and the snapshot is unwrapped to the same long
format, with each row stamped with the snapshot time.

## Where to go next

- Pass `return_as_pandas=True` for a pandas frame, or `return_parsed=False` for
  the raw JSON.
- Full reference: the **Betting -> Odds** page in the sidebar.
- R users: the same surface lives in [oddsapiR](https://oddsapir.sportsdataverse.org).
