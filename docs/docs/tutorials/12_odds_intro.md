---
title: Betting odds tutorial
sidebar_label: Betting odds
sidebar_position: 12
---

# 🎲 Betting odds with `sportsdataverse-py`

Welcome! In a few lines of Python you're about to pull **live betting odds**
from a whole market of sportsbooks — moneylines, spreads, totals, player
props, scores, even point-in-time history. `sportsdataverse.odds` wraps
[The Odds API](https://the-odds-api.com) v4 and hands you back tidy **polars**
DataFrames that are ready to model. 🚀

If you've used the R package [oddsapiR](https://oddsapir.sportsdataverse.org),
the `toa_*` names will feel right at home. Let's dive in!

## 🧰 The toolbox

Every function returns a tidy **polars** `DataFrame` by default — pass
`return_as_pandas=True` for pandas, or `return_parsed=False` for the raw JSON.
Here's the whole kit (click any name for the full reference):

| Function | What it gives you | Quota |
|---|---|---|
| [`toa_sports`](../odds/reference/additional.md#toa_sports) | Every in-season sport/league key (the `sport=` value) | 🆓 free |
| [`toa_sports_odds`](../odds/reference/additional.md#toa_sports_odds) | **Current odds** for a sport — one row per outcome | 💳 paid |
| [`toa_event_odds`](../odds/reference/additional.md#toa_event_odds) | Odds for a **single game**, including player props | 💳 paid |
| [`toa_event_markets`](../odds/reference/additional.md#toa_event_markets) | Which markets a game has on offer | 🆓 free |
| [`toa_sports_scores`](../odds/reference/additional.md#toa_sports_scores) | Live + recently-completed **scores** | 🆓 free |
| [`toa_sports_events`](../odds/reference/additional.md#toa_sports_events) | Upcoming + live **event list** (grab `event_id`s here) | 🆓 free |
| [`toa_sports_participants`](../odds/reference/additional.md#toa_sports_participants) | Teams / participants for a sport | 🆓 free |
| [`toa_sports_odds_history`](../odds/reference/additional.md#toa_sports_odds_history) | **Historical** odds snapshot (paid plans) | 💳 paid |
| [`toa_sports_events_history`](../odds/reference/additional.md#toa_sports_events_history) | Historical event snapshot | 💳 paid |
| [`toa_event_odds_history`](../odds/reference/additional.md#toa_event_odds_history) | Historical single-game odds | 💳 paid |
| [`toa_usage`](../odds/reference/additional.md#toa_usage) | Your remaining quota (reads cached headers) | 🆓 free |


## 🔑 Setup

```sh
pip install sportsdataverse
```

The Odds API needs a key — grab a free one at
[the-odds-api.com](https://the-odds-api.com/#get-access). Set it once as the
`ODDS_API_KEY` environment variable (the same name `oddsapiR` uses) or pass
`api_key=` to any call. The live cells below run only when a key is present,
so this page is happy either way. 😊


```python
import os
import polars as pl
import sportsdataverse.odds as odds

HAS_KEY = bool(os.environ.get("ODDS_API_KEY"))
print("ODDS_API_KEY set:", HAS_KEY, "— live cells will" + ("" if HAS_KEY else " NOT") + " run")
```

    ODDS_API_KEY set: True — live cells will run


## 🗂️ What's on the board?

Start with [`toa_sports`](../odds/reference/additional.md#toa_sports) — it lists every sport/league key,
and it's **free** (doesn't touch your quota). The `key` column is what you
pass as `sport=` everywhere else.


```python
if HAS_KEY:
    sports = odds.toa_sports(all_sports=True)
    out = sports.select([c for c in ["key", "group", "title", "active"] if c in sports.columns]).head(12)
else:
    out = "set ODDS_API_KEY to run: odds.toa_sports(all_sports=True)"
out
```




    shape: (12, 4)
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
    │ …                               ┆ …                 ┆ …                         ┆ …      │
    │ aussierules_afl                 ┆ Aussie Rules      ┆ AFL                       ┆ true   │
    │ baseball_kbo                    ┆ Baseball          ┆ KBO                       ┆ true   │
    │ baseball_milb                   ┆ Baseball          ┆ MiLB                      ┆ false  │
    │ baseball_mlb                    ┆ Baseball          ┆ MLB                       ┆ true   │
    │ baseball_mlb_preseason          ┆ Baseball          ┆ MLB Preseason             ┆ false  │
    └─────────────────────────────────┴───────────────────┴───────────────────────────┴────────┘



## 💰 The main event: live odds

[`toa_sports_odds`](../odds/reference/additional.md#toa_sports_odds) is the workhorse. It returns **long
format** — one row per *event × bookmaker × market × outcome* — which is
exactly the shape you want for filtering and modelling. Knobs:

- `regions` — bookmaker regions: `us`, `us2`, `uk`, `eu`, `au` (comma-separate to mix).
- `markets` — `h2h` (moneyline), `spreads`, `totals`, `outrights`, … (comma-separated).
- `odds_format` — `american` or `decimal`.
- `bookmakers` — pin specific books (takes precedence over `regions`).


```python
if HAS_KEY:
    board = odds.toa_sports_odds(sport="americanfootball_nfl", regions="us", markets="h2h,spreads")
    keep = ["home_team", "away_team", "bookmaker_key", "market_key", "outcome_name", "outcome_point", "outcome_price"]
    out = board.select([c for c in keep if c in board.columns]).head(10)
else:
    board = None
    out = "set ODDS_API_KEY to run: odds.toa_sports_odds(sport='americanfootball_nfl', regions='us')"
out
```




    shape: (10, 7)
    ┌──────────────┬──────────────┬─────────────┬────────────┬─────────────┬─────────────┬─────────────┐
    │ home_team    ┆ away_team    ┆ bookmaker_k ┆ market_key ┆ outcome_nam ┆ outcome_poi ┆ outcome_pri │
    │ ---          ┆ ---          ┆ ey          ┆ ---        ┆ e           ┆ nt          ┆ ce          │
    │ str          ┆ str          ┆ ---         ┆ str        ┆ ---         ┆ ---         ┆ ---         │
    │              ┆              ┆ str         ┆            ┆ str         ┆ f64         ┆ i64         │
    ╞══════════════╪══════════════╪═════════════╪════════════╪═════════════╪═════════════╪═════════════╡
    │ Seattle      ┆ New England  ┆ draftkings  ┆ h2h        ┆ New England ┆ null        ┆ 170         │
    │ Seahawks     ┆ Patriots     ┆             ┆            ┆ Patriots    ┆             ┆             │
    │ Seattle      ┆ New England  ┆ draftkings  ┆ h2h        ┆ Seattle     ┆ null        ┆ -205        │
    │ Seahawks     ┆ Patriots     ┆             ┆            ┆ Seahawks    ┆             ┆             │
    │ Seattle      ┆ New England  ┆ draftkings  ┆ spreads    ┆ New England ┆ 3.5         ┆ -105        │
    │ Seahawks     ┆ Patriots     ┆             ┆            ┆ Patriots    ┆             ┆             │
    │ Seattle      ┆ New England  ┆ draftkings  ┆ spreads    ┆ Seattle     ┆ -3.5        ┆ -115        │
    │ Seahawks     ┆ Patriots     ┆             ┆            ┆ Seahawks    ┆             ┆             │
    │ Seattle      ┆ New England  ┆ betus       ┆ h2h        ┆ New England ┆ null        ┆ 170         │
    │ Seahawks     ┆ Patriots     ┆             ┆            ┆ Patriots    ┆             ┆             │
    │ Seattle      ┆ New England  ┆ betus       ┆ h2h        ┆ Seattle     ┆ null        ┆ -200        │
    │ Seahawks     ┆ Patriots     ┆             ┆            ┆ Seahawks    ┆             ┆             │
    │ Seattle      ┆ New England  ┆ betus       ┆ spreads    ┆ New England ┆ 4.0         ┆ -110        │
    │ Seahawks     ┆ Patriots     ┆             ┆            ┆ Patriots    ┆             ┆             │
    │ Seattle      ┆ New England  ┆ betus       ┆ spreads    ┆ Seattle     ┆ -4.0        ┆ -110        │
    │ Seahawks     ┆ Patriots     ┆             ┆            ┆ Seahawks    ┆             ┆             │
    │ Seattle      ┆ New England  ┆ fanduel     ┆ h2h        ┆ New England ┆ null        ┆ 172         │
    │ Seahawks     ┆ Patriots     ┆             ┆            ┆ Patriots    ┆             ┆             │
    │ Seattle      ┆ New England  ┆ fanduel     ┆ h2h        ┆ Seattle     ┆ null        ┆ -205        │
    │ Seahawks     ┆ Patriots     ┆             ┆            ┆ Seahawks    ┆             ┆             │
    └──────────────┴──────────────┴─────────────┴────────────┴─────────────┴─────────────┴─────────────┘



## 🍳 Cookbook: common odds tasks

Because everything is one tidy long frame, the fun stuff is just a few polars
expressions away. Here are three recipes you'll reach for constantly.

### Recipe 1 — Best available moneyline (line shopping 🛒)

For each team, find the **highest** moneyline price across every book — and
which book is offering it. Sort by price descending, group, take the top.


```python
if HAS_KEY and board is not None:
    h2h = board.filter(pl.col("market_key") == "h2h")
    best = (
        h2h.sort("outcome_price", descending=True)
        .group_by(["home_team", "away_team", "outcome_name"], maintain_order=True)
        .agg(pl.first("outcome_price").alias("best_price"), pl.first("bookmaker_key").alias("best_book"))
    )
    out = best.head(10)
else:
    out = "needs ODDS_API_KEY"
out
```




    shape: (10, 5)
    ┌──────────────────────┬───────────────────┬───────────────────┬────────────┬────────────────┐
    │ home_team            ┆ away_team         ┆ outcome_name      ┆ best_price ┆ best_book      │
    │ ---                  ┆ ---               ┆ ---               ┆ ---        ┆ ---            │
    │ str                  ┆ str               ┆ str               ┆ i64        ┆ str            │
    ╞══════════════════════╪═══════════════════╪═══════════════════╪════════════╪════════════════╡
    │ Los Angeles Chargers ┆ Arizona Cardinals ┆ Arizona Cardinals ┆ 475        ┆ betmgm         │
    │ San Francisco 49ers  ┆ Arizona Cardinals ┆ Arizona Cardinals ┆ 455        ┆ draftkings     │
    │ San Francisco 49ers  ┆ Miami Dolphins    ┆ Miami Dolphins    ┆ 425        ┆ draftkings     │
    │ Arizona Cardinals    ┆ Seattle Seahawks  ┆ Arizona Cardinals ┆ 390        ┆ draftkings     │
    │ Detroit Lions        ┆ New York Jets     ┆ New York Jets     ┆ 375        ┆ williamhill_us │
    │ Jacksonville Jaguars ┆ Cleveland Browns  ┆ Cleveland Browns  ┆ 340        ┆ fanduel        │
    │ Los Angeles Chargers ┆ Las Vegas Raiders ┆ Las Vegas Raiders ┆ 330        ┆ draftkings     │
    │ Los Angeles Rams     ┆ New York Giants   ┆ New York Giants   ┆ 330        ┆ williamhill_us │
    │ Baltimore Ravens     ┆ Tennessee Titans  ┆ Tennessee Titans  ┆ 330        ┆ williamhill_us │
    │ New England Patriots ┆ Las Vegas Raiders ┆ Las Vegas Raiders ┆ 330        ┆ draftkings     │
    └──────────────────────┴───────────────────┴───────────────────┴────────────┴────────────────┘



### Recipe 2 — Spreads & totals for a slate 📋

Ask for `markets="spreads,totals"` and the `outcome_point` column carries the
line (the spread number / the over-under total).


```python
if HAS_KEY:
    st = odds.toa_sports_odds(sport="americanfootball_nfl", regions="us", markets="spreads,totals")
    out = (
        st.filter(pl.col("bookmaker_key") == st["bookmaker_key"][0])
        .select(["home_team", "away_team", "market_key", "outcome_name", "outcome_point", "outcome_price"])
        .head(10)
        if st.height else "no spreads/totals on the board right now"
    )
else:
    out = "needs ODDS_API_KEY"
out
```




    shape: (10, 6)
    ┌─────────────┬───────────────────┬────────────┬───────────────────┬───────────────┬───────────────┐
    │ home_team   ┆ away_team         ┆ market_key ┆ outcome_name      ┆ outcome_point ┆ outcome_price │
    │ ---         ┆ ---               ┆ ---        ┆ ---               ┆ ---           ┆ ---           │
    │ str         ┆ str               ┆ str        ┆ str               ┆ f64           ┆ i64           │
    ╞═════════════╪═══════════════════╪════════════╪═══════════════════╪═══════════════╪═══════════════╡
    │ Seattle     ┆ New England       ┆ spreads    ┆ New England       ┆ 3.5           ┆ -105          │
    │ Seahawks    ┆ Patriots          ┆            ┆ Patriots          ┆               ┆               │
    │ Seattle     ┆ New England       ┆ spreads    ┆ Seattle Seahawks  ┆ -3.5          ┆ -115          │
    │ Seahawks    ┆ Patriots          ┆            ┆                   ┆               ┆               │
    │ Seattle     ┆ New England       ┆ totals     ┆ Over              ┆ 44.5          ┆ -110          │
    │ Seahawks    ┆ Patriots          ┆            ┆                   ┆               ┆               │
    │ Seattle     ┆ New England       ┆ totals     ┆ Under             ┆ 44.5          ┆ -110          │
    │ Seahawks    ┆ Patriots          ┆            ┆                   ┆               ┆               │
    │ Los Angeles ┆ San Francisco     ┆ spreads    ┆ Los Angeles Rams  ┆ -3.0          ┆ -120          │
    │ Rams        ┆ 49ers             ┆            ┆                   ┆               ┆               │
    │ Los Angeles ┆ San Francisco     ┆ spreads    ┆ San Francisco     ┆ 3.0           ┆ 100           │
    │ Rams        ┆ 49ers             ┆            ┆ 49ers             ┆               ┆               │
    │ Los Angeles ┆ San Francisco     ┆ totals     ┆ Over              ┆ 48.5          ┆ -110          │
    │ Rams        ┆ 49ers             ┆            ┆                   ┆               ┆               │
    │ Los Angeles ┆ San Francisco     ┆ totals     ┆ Under             ┆ 48.5          ┆ -110          │
    │ Rams        ┆ 49ers             ┆            ┆                   ┆               ┆               │
    │ Pittsburgh  ┆ Atlanta Falcons   ┆ spreads    ┆ Atlanta Falcons   ┆ 3.0           ┆ 100           │
    │ Steelers    ┆                   ┆            ┆                   ┆               ┆               │
    │ Pittsburgh  ┆ Atlanta Falcons   ┆ spreads    ┆ Pittsburgh        ┆ -3.0          ┆ -120          │
    │ Steelers    ┆                   ┆            ┆ Steelers          ┆               ┆               │
    └─────────────┴───────────────────┴────────────┴───────────────────┴───────────────┴───────────────┘



### Recipe 3 — Just one book 🎯

Pin a single sportsbook with `bookmakers=`. Great for tracking *your* book's
line without paying for a whole region.


```python
if HAS_KEY:
    dk = odds.toa_sports_odds(sport="americanfootball_nfl", bookmakers="draftkings", markets="h2h")
    out = dk.select(["home_team", "away_team", "outcome_name", "outcome_price"]).head() if dk.height else "no lines yet"
else:
    out = "needs ODDS_API_KEY"
out
```




    shape: (5, 4)
    ┌─────────────────────┬──────────────────────┬──────────────────────┬───────────────┐
    │ home_team           ┆ away_team            ┆ outcome_name         ┆ outcome_price │
    │ ---                 ┆ ---                  ┆ ---                  ┆ ---           │
    │ str                 ┆ str                  ┆ str                  ┆ i64           │
    ╞═════════════════════╪══════════════════════╪══════════════════════╪═══════════════╡
    │ Seattle Seahawks    ┆ New England Patriots ┆ New England Patriots ┆ 170           │
    │ Seattle Seahawks    ┆ New England Patriots ┆ Seattle Seahawks     ┆ -205          │
    │ Los Angeles Rams    ┆ San Francisco 49ers  ┆ Los Angeles Rams     ┆ -175          │
    │ Los Angeles Rams    ┆ San Francisco 49ers  ┆ San Francisco 49ers  ┆ 145           │
    │ Pittsburgh Steelers ┆ Atlanta Falcons      ┆ Atlanta Falcons      ┆ 145           │
    └─────────────────────┴──────────────────────┴──────────────────────┴───────────────┘



## 🎯 Player props for one game

Event-level markets (player props!) live on [`toa_event_odds`](../odds/reference/additional.md#toa_event_odds).
Grab an `event_id` from [`toa_sports_events`](../odds/reference/additional.md#toa_sports_events), then ask
for a prop market like `player_pass_tds` or `player_anytime_td`.

Not sure which markets a game has? [`toa_event_markets`](../odds/reference/additional.md#toa_event_markets)
lists them (and it's free).


```python
if HAS_KEY:
    events = odds.toa_sports_events(sport="americanfootball_nfl", return_parsed=False)
    if events:
        eid = events[0]["id"]
        props = odds.toa_event_odds(sport="americanfootball_nfl", event_id=eid, markets="player_pass_tds")
        out = props.select([c for c in ["outcome_name", "outcome_description", "outcome_point", "outcome_price"]
                             if c in props.columns]).head()
    else:
        out = "no upcoming NFL events right now"
else:
    out = "set ODDS_API_KEY to run the player-props recipe"
out
```




    shape: (0, 0)
    ┌┐
    ╞╡
    └┘



## 📊 Scores & events

| Function | Use it for |
|---|---|
| [`toa_sports_scores`](../odds/reference/additional.md#toa_sports_scores) | live + recent final scores (`days_from=1..3`) |
| [`toa_sports_events`](../odds/reference/additional.md#toa_sports_events) | the upcoming schedule + `event_id`s |
| [`toa_sports_participants`](../odds/reference/additional.md#toa_sports_participants) | the teams in a league |



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



## ⛽ Mind your quota

Paid calls cost credits (every 10 bookmakers × market ≈ 1 credit). After any
call, [`toa_usage`](../odds/reference/additional.md#toa_usage) reads the most recent
`x-requests-remaining` / `x-requests-used` headers **without spending a
request** — handy to drop at the end of a script.


```python
odds.toa_usage() if HAS_KEY else "set ODDS_API_KEY to track quota with odds.toa_usage()"
```




    shape: (1, 3)
    ┌────────────────────┬───────────────┬───────────┐
    │ requests_remaining ┆ requests_used ┆ last_cost │
    │ ---                ┆ ---           ┆ ---       │
    │ i64                ┆ i64           ┆ i64       │
    ╞════════════════════╪═══════════════╪═══════════╡
    │ 19805              ┆ 195           ┆ 2         │
    └────────────────────┴───────────────┴───────────┘



## ⏳ Time travel: historical odds

On a paid plan you can pull point-in-time snapshots — perfect for *closing
line value* studies. Pass a `date=` ISO-8601 timestamp; the snapshot is
unwrapped to the same long format and every row is stamped with the snapshot
time.

| Function | Snapshot of… |
|---|---|
| [`toa_sports_odds_history`](../odds/reference/additional.md#toa_sports_odds_history) | a whole sport's odds at `date` |
| [`toa_sports_events_history`](../odds/reference/additional.md#toa_sports_events_history) | the events at `date` |
| [`toa_event_odds_history`](../odds/reference/additional.md#toa_event_odds_history) | one game's odds at `date` |

```python
odds.toa_sports_odds_history(sport="americanfootball_nfl", date="2023-11-29T22:45:00Z")
```

## 🎉 Where to next

- Pass `return_as_pandas=True` for a pandas frame, or `return_parsed=False` for raw JSON.
- Full reference: the **Betting → Odds** section in the sidebar.
- R user? The same surface lives in [oddsapiR](https://oddsapir.sportsdataverse.org).

Happy modelling — may your closing line value be ever positive! 📈
