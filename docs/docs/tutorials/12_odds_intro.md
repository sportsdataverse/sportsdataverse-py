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

## 🍳 Cookbook: common odds tasks

Because everything is one tidy long frame, the fun stuff is just a few polars
expressions away. Twelve recipes you'll reach for constantly — every live
cell is key-guarded, so the page renders fine with or without a key.

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

### Recipe 4 — Implied probability & the hold 🧮

American moneyline prices convert to **implied win probability** with a tiny
formula. Add up both sides and the excess over 100% is the book's *hold* (the
vig). Pure polars math on the frame you already pulled — no extra API call.


```python
if HAS_KEY and board is not None:
    h2h = board.filter(pl.col("market_key") == "h2h")
    devig = (
        h2h.with_columns(
            pl.when(pl.col("outcome_price") < 0)
            .then(-pl.col("outcome_price") / (-pl.col("outcome_price") + 100))
            .otherwise(100 / (pl.col("outcome_price") + 100))
            .alias("implied_prob")
        )
        .group_by(["home_team", "away_team", "bookmaker_key"], maintain_order=True)
        .agg(pl.sum("implied_prob").alias("market_total"))
        .with_columns(((pl.col("market_total") - 1) * 100).round(2).alias("hold_pct"))
        .sort("hold_pct")
    )
    out = devig.head(10)
else:
    out = "needs ODDS_API_KEY"
out
```

### Recipe 5 — Find the biggest favorite on the board 🐻

Sort the moneyline outcomes by price ascending — the most negative number is
the heaviest chalk on the slate. A classic "find the X" one-liner.


```python
if HAS_KEY and board is not None:
    faves = (
        board.filter(pl.col("market_key") == "h2h")
        .sort("outcome_price")
        .select(["home_team", "away_team", "outcome_name", "outcome_price", "bookmaker_key"])
        .head(5)
    )
    out = faves
else:
    out = "needs ODDS_API_KEY"
out
```

### Recipe 6 — Consensus over/under per game 📊

Books disagree by a half-point here and there. Take the **median** total
across every book to get a stable market consensus for each matchup.


```python
if HAS_KEY:
    tot = odds.toa_sports_odds(sport="americanfootball_nfl", regions="us", markets="totals")
    if tot.height:
        consensus = (
            tot.filter(pl.col("outcome_name") == "Over")
            .group_by(["home_team", "away_team"], maintain_order=True)
            .agg(
                pl.median("outcome_point").alias("consensus_total"),
                pl.col("bookmaker_key").n_unique().alias("n_books"),
            )
            .sort("consensus_total", descending=True)
        )
        out = consensus.head(10)
    else:
        out = "no totals on the board right now"
else:
    out = "needs ODDS_API_KEY"
out
```

### Recipe 7 — Just today's slate ⏰

Narrow the pull to a time window with `commence_time_from` / `commence_time_to`
(ISO-8601, UTC). Here: only games kicking off in the next 24 hours.


```python
from datetime import datetime, timedelta, timezone

if HAS_KEY:
    now = datetime.now(timezone.utc)
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    today = odds.toa_sports_odds(
        sport="americanfootball_nfl",
        regions="us",
        markets="h2h",
        commence_time_from=now.strftime(fmt),
        commence_time_to=(now + timedelta(hours=24)).strftime(fmt),
    )
    out = (
        today.select(["commence_time", "home_team", "away_team"]).unique(maintain_order=True).head(10)
        if today.height else "nothing kicks off in the next 24h"
    )
else:
    out = "set ODDS_API_KEY to run the commence-time filter recipe"
out
```

### Recipe 8 — Player props for one game 🎯

Event-level markets (player props!) live on [`toa_event_odds`](../odds/reference/additional.md#toa_event_odds).
Grab an `event_id` from [`toa_sports_events`](../odds/reference/additional.md#toa_sports_events), then ask
for a prop market like `player_pass_tds` or `player_anytime_td`.


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

### Recipe 9 — Which markets does a game offer? 🗃️

Not sure which props are even available? [`toa_event_markets`](../odds/reference/additional.md#toa_event_markets)
lists every market on offer per book — and it's **free**. Count them up to
see which sportsbook posts the deepest menu.


```python
if HAS_KEY:
    events = odds.toa_sports_events(sport="americanfootball_nfl", return_parsed=False)
    if events:
        eid = events[0]["id"]
        mk = odds.toa_event_markets(sport="americanfootball_nfl", event_id=eid)
        out = (
            mk.group_by("bookmaker_key").agg(pl.col("market_key").n_unique().alias("n_markets"))
            .sort("n_markets", descending=True).head(10)
            if mk.height else "no markets posted for this event yet"
        )
    else:
        out = "no upcoming NFL events right now"
else:
    out = "set ODDS_API_KEY to run the event-markets recipe"
out
```

### Recipe 10 — Recent finals & margin of victory 🏁

[`toa_sports_scores`](../odds/reference/additional.md#toa_sports_scores) returns live + recently
completed games (`days_from=1..3`, **free**). Keep the completed ones and
show the final scoreline — handy for grading bets after the fact.


```python
if HAS_KEY:
    sc = odds.toa_sports_scores(sport="americanfootball_nfl", days_from=3)
    keep = [c for c in ["completed", "home_team", "away_team", "scores", "last_update"] if c in sc.columns]
    if sc.height and "completed" in sc.columns:
        out = sc.filter(pl.col("completed")).select(keep).head(10)
    else:
        out = sc.select(keep).head(10) if sc.height else "no recent scores right now"
else:
    out = "set ODDS_API_KEY to run: odds.toa_sports_scores(sport='americanfootball_nfl', days_from=3)"
out
```

### Recipe 11 — Tour several leagues at once 🔁

The `sport=` key is the only thing that changes between leagues, so one loop
counts the upcoming events across a handful of them. `toa_sports_events` is
**free**, so this sweep costs you nothing.


```python
if HAS_KEY:
    keys = ["americanfootball_nfl", "basketball_nba", "icehockey_nhl", "baseball_mlb"]
    rows = []
    for k in keys:
        evs = odds.toa_sports_events(sport=k, return_parsed=False)
        rows.append({"sport": k, "upcoming_events": len(evs) if isinstance(evs, list) else 0})
    out = pl.DataFrame(rows).sort("upcoming_events", descending=True)
else:
    out = "set ODDS_API_KEY to tour leagues with odds.toa_sports_events(sport=...)"
out
```

### Recipe 12 — Who's in the league? (participants 👥)

[`toa_sports_participants`](../odds/reference/additional.md#toa_sports_participants) lists every team /
participant for a sport — the lookup table you join odds against by name.
Also **free**.


```python
if HAS_KEY:
    parts = odds.toa_sports_participants(sport="americanfootball_nfl")
    keep = [c for c in ["full_name", "id", "abbreviation"] if c in parts.columns]
    out = parts.select(keep if keep else parts.columns).head(10) if parts.height else "no participants listed"
else:
    out = "set ODDS_API_KEY to run: odds.toa_sports_participants(sport='americanfootball_nfl')"
out
```

## ⛽ Mind your quota

Paid calls cost credits (every 10 bookmakers × market ≈ 1 credit). After any
call, [`toa_usage`](../odds/reference/additional.md#toa_usage) reads the most recent
`x-requests-remaining` / `x-requests-used` headers **without spending a
request** — handy to drop at the end of a script.


```python
odds.toa_usage() if HAS_KEY else "set ODDS_API_KEY to track quota with odds.toa_usage()"
```

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
