---
title: ESPN Fantasy tutorial
description: "ESPN runs five fantasy games on one platform: football (ffl), men's basketball (fba), baseball (flb), hockey (fhl) and women's basketball (wfba)."
sidebar_label: ESPN Fantasy
sidebar_position: 20
custom_edit_url: https://github.com/sportsdataverse/sportsdataverse-py/edit/main/examples/notebooks/20_espn_fantasy_intro.ipynb
---

# 🏆 ESPN Fantasy with `sportsdataverse-py`

> This page is the executed notebook [`20_espn_fantasy_intro.ipynb`](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/examples/notebooks/20_espn_fantasy_intro.ipynb): [download it](https://raw.githubusercontent.com/sportsdataverse/sportsdataverse-py/main/examples/notebooks/20_espn_fantasy_intro.ipynb) to run it yourself.

ESPN runs five fantasy games on one platform: football (`ffl`), men's basketball (`fba`), baseball (`flb`), hockey (`fhl`) and women's basketball (`wfba`). `sportsdataverse.providers.espn_fantasy` reads the parts of it that are **public**: the games and seasons, the pro-team schedules, the player universe, ESPN's default scoring, player news, and the public leagues ESPN lists in its draft lobby.

No account and no cookies. A private league answers HTTP 401 and is out of scope.

## 🧰 The toolbox

| Function | What it gives you |
|---|---|
| [`espn_fantasy_games`](../espn_fantasy/reference/espn_fantasy.md#espn_fantasy_games) | Every fantasy game and its current season |
| [`espn_fantasy_players`](../espn_fantasy/reference/espn_fantasy.md#espn_fantasy_players) | The player universe of a season, with ownership |
| [`espn_fantasy_pro_schedule`](../espn_fantasy/reference/espn_fantasy.md#espn_fantasy_pro_schedule) | Pro games by scoring period |
| [`espn_fantasy_default_scoring`](../espn_fantasy/reference/espn_fantasy.md#espn_fantasy_default_scoring) | ESPN's default points per stat |
| [`espn_fantasy_league_directory`](../espn_fantasy/reference/espn_fantasy.md#espn_fantasy_league_directory) | ESPN's public leagues |
| [`espn_fantasy_league_teams`](../espn_fantasy/reference/espn_fantasy.md#espn_fantasy_league_teams) | A public league's standings |
| [`espn_fantasy_league_matchups`](../espn_fantasy/reference/espn_fantasy.md#espn_fantasy_league_matchups) | Its head-to-head schedule |
| [`espn_fantasy_player_pool`](../espn_fantasy/reference/espn_fantasy.md#espn_fantasy_player_pool) | Its free agents and rostered players |
| [`espn_fantasy_player_news`](../espn_fantasy/reference/espn_fantasy.md#espn_fantasy_player_news) | News items for one player |

## 🔌 Setup

```sh
pip install --pre "sportsdataverse"
```

`--pre` is needed while 0.2.0 is in beta.


```python
import polars as pl

from sportsdataverse.providers import espn_fantasy as ef

pl.Config.set_tbl_rows(10)
pl.Config.set_fmt_str_lengths(50)
```




    polars.config.Config



Fantasy routes are seasonal: an off-season game lists no public leagues, and a league that is still drafting has empty rosters. A small `safe()` helper prints a one-line skip instead of stopping the notebook, naming whether the answer was *nothing there* (`NoDataError`) or *the fetch failed* (`AssetFetchError`). 🛟


```python
from sportsdataverse.errors import AssetFetchError, NoDataError

def safe(label, thunk):
    try:
        out = thunk()
        print(f"✅ {label}")
        return out
    except (NoDataError, AssetFetchError) as e:
        print(f"\u23ed\ufe0f  {label}: {type(e).__name__}: {e}")
        return None
```

## 🎮 Games and seasons

Season ids are ESPN's own: football is named by the year it **starts** (`2026` = fall 2026), basketball and hockey by the year they **end** (2026-27 = `2027`). `espn_fantasy_games` gives each game's current one.


```python
games = ef.espn_fantasy_games()
season = dict(games.select(pl.col("game_abbrev").str.to_lowercase(), "current_season_id").iter_rows())
games.select("game_abbrev", "game_name", "pro_sport_abbrev", "current_season_id", "current_scoring_period_id")
```




    shape: (5, 5)
    ┌─────────────┬──────────────────────┬──────────────────┬───────────────────┬──────────────────────┐
    │ game_abbrev ┆ game_name            ┆ pro_sport_abbrev ┆ current_season_id ┆ current_scoring_peri │
    │ ---         ┆ ---                  ┆ ---              ┆ ---               ┆ od_id                │
    │ str         ┆ str                  ┆ str              ┆ i64               ┆ ---                  │
    │             ┆                      ┆                  ┆                   ┆ i64                  │
    ╞═════════════╪══════════════════════╪══════════════════╪═══════════════════╪══════════════════════╡
    │ FFL         ┆ Fantasy Football     ┆ NFL              ┆ 2026              ┆ 12                   │
    │ FLB         ┆ Fantasy Baseball     ┆ MLB              ┆ 2026              ┆ 188                  │
    │ FBA         ┆ Fantasy Basketball   ┆ NBA              ┆ 2027              ┆ 38                   │
    │ FHL         ┆ Fantasy Hockey       ┆ NHL              ┆ 2027              ┆ 59                   │
    │ WFBA        ┆ Fantasy Womens       ┆ WNBA             ┆ 2026              ┆ 141                  │
    │             ┆ Basketball           ┆                  ┆                   ┆                      │
    └─────────────┴──────────────────────┴──────────────────┴───────────────────┴──────────────────────┘



## 🧑‍🤝‍🧑 The player universe

`espn_fantasy_players` returns every active player of a season with the share of ESPN leagues that roster them. `player_id` is ESPN's athlete id, the same one ESPN's other APIs use.


```python
players = ef.espn_fantasy_players("fba", season["fba"])
players.sort("percent_owned", descending=True).select("player_id", "full_name", "pro_team_id", "percent_owned").head(10)
```




    shape: (10, 4)
    ┌───────────┬─────────────────────────┬─────────────┬───────────────┐
    │ player_id ┆ full_name               ┆ pro_team_id ┆ percent_owned │
    │ ---       ┆ ---                     ┆ ---         ┆ ---           │
    │ i64       ┆ str                     ┆ i64         ┆ f64           │
    ╞═══════════╪═════════════════════════╪═════════════╪═══════════════╡
    │ 5104157   ┆ Victor Wembanyama       ┆ 24          ┆ 99.95         │
    │ 3112335   ┆ Nikola Jokic            ┆ 7           ┆ 99.94         │
    │ 3908809   ┆ Donovan Mitchell        ┆ 5           ┆ 99.94         │
    │ 4278073   ┆ Shai Gilgeous-Alexander ┆ 25          ┆ 99.94         │
    │ 3945274   ┆ Luka Doncic             ┆ 13          ┆ 99.94         │
    │ 4431678   ┆ Tyrese Maxey            ┆ 20          ┆ 99.92         │
    │ 3032977   ┆ Giannis Antetokounmpo   ┆ 14          ┆ 99.91         │
    │ 3202      ┆ Kevin Durant            ┆ 10          ┆ 99.91         │
    │ 5041939   ┆ Cooper Flagg            ┆ 6           ┆ 99.91         │
    │ 3136195   ┆ Karl-Anthony Towns      ┆ 18          ┆ 99.91         │
    └───────────┴─────────────────────────┴─────────────┴───────────────┘



## 📅 Pro schedule and scoring

Football's scoring period is the NFL week; for the other games it is a day. The pro schedule says which pro games count toward each period, and ESPN's default template says what each stat is worth.


```python
sched = ef.espn_fantasy_pro_schedule("ffl", season["ffl"])
sched.group_by("scoring_period_id").agg(pl.len().alias("pro_games")).sort("scoring_period_id").head(6)
```




    shape: (6, 2)
    ┌───────────────────┬───────────┐
    │ scoring_period_id ┆ pro_games │
    │ ---               ┆ ---       │
    │ i64               ┆ u32       │
    ╞═══════════════════╪═══════════╡
    │ 1                 ┆ 16        │
    │ 2                 ┆ 16        │
    │ 3                 ┆ 16        │
    │ 4                 ┆ 16        │
    │ 5                 ┆ 15        │
    │ 6                 ┆ 14        │
    └───────────────────┴───────────┘




```python
scoring = ef.espn_fantasy_default_scoring("ffl", season["ffl"], league_default_id=1)
scoring.select("scoring_type", "stat_id", "points").sort("points", descending=True).head(8)
```




    shape: (8, 3)
    ┌──────────────┬─────────┬────────┐
    │ scoring_type ┆ stat_id ┆ points │
    │ ---          ┆ ---     ┆ ---    │
    │ str          ┆ i64     ┆ f64    │
    ╞══════════════╪═════════╪════════╡
    │ H2H_POINTS   ┆ 25      ┆ 6.0    │
    │ H2H_POINTS   ┆ 43      ┆ 6.0    │
    │ H2H_POINTS   ┆ 63      ┆ 6.0    │
    │ H2H_POINTS   ┆ 93      ┆ 6.0    │
    │ H2H_POINTS   ┆ 101     ┆ 6.0    │
    │ H2H_POINTS   ┆ 102     ┆ 6.0    │
    │ H2H_POINTS   ┆ 103     ┆ 6.0    │
    │ H2H_POINTS   ┆ 104     ┆ 6.0    │
    └──────────────┴─────────┴────────┘



## 🏟️ A public league

ESPN's draft lobby lists public leagues anyone can join; every one of them is readable by id. Leagues whose draft is over have rosters and played matchups.


```python
lobby = ef.espn_fantasy_league_directory("fba", season["fba"])
done = lobby.filter(pl.col("full") == True).sort("draft_date")
done.select("league_id", "league_name", "scoring_type", "draft_type", "draft_date").head(5)
```




    shape: (5, 5)
    ┌────────────┬───────────────────────────────┬──────────────┬────────────┬─────────────────────────┐
    │ league_id  ┆ league_name                   ┆ scoring_type ┆ draft_type ┆ draft_date              │
    │ ---        ┆ ---                           ┆ ---          ┆ ---        ┆ ---                     │
    │ i64        ┆ str                           ┆ str          ┆ str        ┆ datetime[ms, UTC]       │
    ╞════════════╪═══════════════════════════════╪══════════════╪════════════╪═════════════════════════╡
    │ 1535462463 ┆ Minnesota Pro H2H Points      ┆ H2H_POINTS   ┆ SNAKE      ┆ 2026-11-11 00:18:00 UTC │
    │            ┆ League                        ┆              ┆            ┆                         │
    │ 1804910101 ┆ San Antonio Pro H2H Points    ┆ H2H_POINTS   ┆ SNAKE      ┆ 2026-11-11 00:23:00 UTC │
    │            ┆ League                        ┆              ┆            ┆                         │
    │ 1109550710 ┆ Toronto Pro H2H Points League ┆ H2H_POINTS   ┆ SNAKE      ┆ 2026-11-12 00:18:00 UTC │
    │ 535080381  ┆ Charlotte Pro H2H Points      ┆ H2H_POINTS   ┆ SNAKE      ┆ 2026-11-12 00:23:00 UTC │
    │            ┆ League                        ┆              ┆            ┆                         │
    │ 254471202  ┆ Toronto Beginner H2H Points   ┆ H2H_POINTS   ┆ SNAKE      ┆ 2026-11-12 00:38:00 UTC │
    │            ┆ League                        ┆              ┆            ┆                         │
    └────────────┴───────────────────────────────┴──────────────┴────────────┴─────────────────────────┘




```python
league_id = done["league_id"][0] if done.height else None
teams = safe("league teams", lambda: ef.espn_fantasy_league_teams("fba", season["fba"], league_id)) if league_id else None
teams.select("team_id", "wins", "losses", "points_for", "playoff_seed").sort("playoff_seed") if teams is not None else None
```

    ✅ league teams





    shape: (10, 5)
    ┌─────────┬──────┬────────┬────────────┬──────────────┐
    │ team_id ┆ wins ┆ losses ┆ points_for ┆ playoff_seed │
    │ ---     ┆ ---  ┆ ---    ┆ ---        ┆ ---          │
    │ i64     ┆ i64  ┆ i64    ┆ f64        ┆ i64          │
    ╞═════════╪══════╪════════╪════════════╪══════════════╡
    │ 5       ┆ 1    ┆ 0      ┆ 1841.0     ┆ 1            │
    │ 3       ┆ 1    ┆ 0      ┆ 1801.0     ┆ 2            │
    │ 6       ┆ 1    ┆ 0      ┆ 1669.0     ┆ 3            │
    │ 4       ┆ 1    ┆ 0      ┆ 1531.0     ┆ 4            │
    │ 2       ┆ 1    ┆ 0      ┆ 1400.0     ┆ 5            │
    │ 7       ┆ 0    ┆ 1      ┆ 1402.0     ┆ 6            │
    │ 1       ┆ 0    ┆ 1      ┆ 1401.0     ┆ 7            │
    │ 9       ┆ 0    ┆ 1      ┆ 1345.0     ┆ 8            │
    │ 8       ┆ 0    ┆ 1      ┆ 1234.0     ┆ 9            │
    │ 10      ┆ 0    ┆ 1      ┆ 1182.0     ┆ 10           │
    └─────────┴──────┴────────┴────────────┴──────────────┘




```python
matchups = safe("league matchups", lambda: ef.espn_fantasy_league_matchups("fba", season["fba"], league_id)) if league_id else None
matchups.filter(pl.col("winner") != "UNDECIDED").tail(5) if matchups is not None else None
```

    ✅ league matchups





    shape: (5, 10)
    ┌───────────┬───────────┬───────────┬───────────┬───┬───────────┬───────────┬───────────┬──────────┐
    │ league_id ┆ season_id ┆ matchup_i ┆ matchup_p ┆ … ┆ home_team ┆ away_team ┆ home_tota ┆ away_tot │
    │ ---       ┆ ---       ┆ d         ┆ eriod_id  ┆   ┆ _id       ┆ _id       ┆ l_points  ┆ al_point │
    │ i64       ┆ i64       ┆ ---       ┆ ---       ┆   ┆ ---       ┆ ---       ┆ ---       ┆ s        │
    │           ┆           ┆ i64       ┆ i64       ┆   ┆ i64       ┆ i64       ┆ f64       ┆ ---      │
    │           ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆ f64      │
    ╞═══════════╪═══════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪══════════╡
    │ 153546246 ┆ 2027      ┆ 21        ┆ 5         ┆ … ┆ 6         ┆ 9         ┆ 1257.0    ┆ 909.0    │
    │ 3         ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
    │ 153546246 ┆ 2027      ┆ 22        ┆ 5         ┆ … ┆ 1         ┆ 3         ┆ 1046.0    ┆ 1277.0   │
    │ 3         ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
    │ 153546246 ┆ 2027      ┆ 23        ┆ 5         ┆ … ┆ 2         ┆ 7         ┆ 991.0     ┆ 929.0    │
    │ 3         ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
    │ 153546246 ┆ 2027      ┆ 24        ┆ 5         ┆ … ┆ 5         ┆ 10        ┆ 1269.0    ┆ 852.0    │
    │ 3         ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
    │ 153546246 ┆ 2027      ┆ 25        ┆ 5         ┆ … ┆ 4         ┆ 8         ┆ 1101.0    ┆ 737.0    │
    │ 3         ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
    └───────────┴───────────┴───────────┴───────────┴───┴───────────┴───────────┴───────────┴──────────┘



The player pool answers *who is still available?* The `X-Fantasy-Filter` header narrows it: here, the ten most-owned free agents.


```python
fa_filter = '{"players":{"limit":10,"sortPercOwned":{"sortPriority":1,"sortAsc":false},'
fa_filter += '"filterStatus":{"value":["FREEAGENT","WAIVERS"]}}}'
pool = safe("free agents", lambda: ef.espn_fantasy_player_pool("fba", season["fba"], league_id, fantasy_filter=fa_filter)) if league_id else None
pool.select("player_id", "full_name", "status", "percent_owned") if pool is not None else None
```

    ✅ free agents





    shape: (10, 4)
    ┌───────────┬────────────────────┬───────────┬───────────────┐
    │ player_id ┆ full_name          ┆ status    ┆ percent_owned │
    │ ---       ┆ ---                ┆ ---       ┆ ---           │
    │ i64       ┆ str                ┆ str       ┆ f64           │
    ╞═══════════╪════════════════════╪═══════════╪═══════════════╡
    │ 4222252   ┆ Isaiah Hartenstein ┆ FREEAGENT ┆ 64.11         │
    │ 4431767   ┆ Christian Braun    ┆ FREEAGENT ┆ 60.43         │
    │ 4277847   ┆ Wendell Carter Jr. ┆ FREEAGENT ┆ 56.27         │
    │ 4277961   ┆ Jaren Jackson Jr.  ┆ FREEAGENT ┆ 55.2          │
    │ 4433249   ┆ Moussa Diabate     ┆ FREEAGENT ┆ 53.45         │
    │ 3133628   ┆ Myles Turner       ┆ FREEAGENT ┆ 48.94         │
    │ 4433083   ┆ Cam Spencer        ┆ FREEAGENT ┆ 45.54         │
    │ 4591725   ┆ Ryan Rollins       ┆ FREEAGENT ┆ 43.53         │
    │ 3059319   ┆ Andrew Wiggins     ┆ FREEAGENT ┆ 43.44         │
    │ 4395630   ┆ Devin Vassell      ┆ FREEAGENT ┆ 42.07         │
    └───────────┴────────────────────┴───────────┴───────────────┘



## 📰 Player news


```python
top = players.sort("percent_owned", descending=True)["player_id"][0]
news = safe("player news", lambda: ef.espn_fantasy_player_news("fba", top))
news.select("published", "type", "headline").head(5) if news is not None else None
```

    ✅ player news





    shape: (5, 3)
    ┌─────────────────────────┬──────────┬─────────────────────────────────────────────────────┐
    │ published               ┆ type     ┆ headline                                            │
    │ ---                     ┆ ---      ┆ ---                                                 │
    │ datetime[ms, UTC]       ┆ str      ┆ str                                                 │
    ╞═════════════════════════╪══════════╪═════════════════════════════════════════════════════╡
    │ 2026-11-26 17:27:18 UTC ┆ Rotowire ┆ Wembanyama had 44 points (17-30 FG, 3-9 3Pt, 7-10 … │
    │ 2026-11-26 06:04:01 UTC ┆ Media    ┆ Blazers snag home win over the Spurs                │
    │ 2026-11-26 05:43:21 UTC ┆ Media    ┆ Wemby spins free for the alley-oop                  │
    │ 2026-11-26 05:23:19 UTC ┆ Media    ┆ Wemby finds an angle and banks it off the glass pl… │
    │ 2026-11-26 05:13:54 UTC ┆ Media    ┆ Wemby posterized by Sidy Cissoko with massive two-… │
    └─────────────────────────┴──────────┴─────────────────────────────────────────────────────┘



## 🧭 Where next

- Every wrapper takes `return_parsed=False` for the raw body and `return_as_pandas=True` for pandas.
- `team_id` is unique only inside one league; join league tables on `(league_id, team_id)`.
- Private leagues need their owner's `espn_s2` / `SWID` cookies; this package does not take them.
- The full list is on the [reference page](../espn_fantasy/reference/espn_fantasy.md).
