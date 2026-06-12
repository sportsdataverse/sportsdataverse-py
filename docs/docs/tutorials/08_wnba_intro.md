---
title: WNBA tutorial
sidebar_label: WNBA
sidebar_position: 3
---

# 🏀 Women's basketball with `sportsdataverse-py`

Welcome! In just a few lines of Python you're about to pull **WNBA** teams, rosters, schedules, play-by-play, season stats, standings and the draft — all as tidy [polars](https://pola.rs) DataFrames that are ready to model. 🚀

`sportsdataverse.wnba` leads with ESPN's rich public API (the `espn_wnba_*` family) and tops it off with `load_wnba_*` **parquet loaders** that hand you whole seasons in one shot. **No API key needed.** 🎉

If you've used the R package [wehoop](https://wehoop.sportsdataverse.org), these names will feel right at home. Let's go hoop! 🏀

## 🧰 The toolbox

Every accessor returns a tidy **polars** `DataFrame` by default — pass `return_as_pandas=True` for pandas, or `raw=True` (where supported) for the untouched ESPN JSON. Here's the whole kit (click any name for the full reference):

| Function | What it gives you | Source |
|---|---|---|
| [`espn_wnba_teams`](../wnba/reference/additional.md#espn_wnba_teams) | One row per franchise (grab `team_id`s) | ⭐ ESPN |
| [`espn_wnba_team_roster`](../wnba/reference/site.md#espn_wnba_team_roster) | A team's active roster for a season | ⭐ ESPN |
| [`espn_wnba_schedule`](../wnba/reference/additional.md#espn_wnba_schedule) | Games + results for a date or date range | ⭐ ESPN |
| [`espn_wnba_pbp`](../wnba/reference/additional.md#play-by-play-schedule--rosters) | Event-level play-by-play for one game | ⭐ ESPN |
| [`espn_wnba_player_stats`](../wnba/reference/additional.md#espn_wnba_player_stats) | A player's season stat line (wide) | ⭐ ESPN |
| [`espn_wnba_team_stats`](../wnba/reference/additional.md#espn_wnba_team_stats) | A team's season stats (Averages/Totals/Misc) | ⭐ ESPN |
| [`espn_wnba_standings`](../wnba/reference/site.md#espn_wnba_standings) | League standings, one row per team | ⭐ ESPN |
| [`espn_wnba_draft`](../wnba/reference/site.md#espn_wnba_draft) | Every draft pick for a season | ⭐ ESPN |
| [`espn_wnba_game_officials`](../wnba/reference/additional.md#espn_wnba_game_officials) | The refs who worked a game | ⭐ ESPN |
| [`load_wnba_schedule`](../wnba/reference/loaders.md#load_wnba_schedule) | Whole-season schedule (parquet release) | 📦 loader |
| [`load_wnba_player_boxscore`](../wnba/reference/loaders.md#load_wnba_player_boxscore) | Whole-season player box scores | 📦 loader |
| [`load_wnba_team_boxscore`](../wnba/reference/loaders.md#load_wnba_team_boxscore) | Whole-season team box scores | 📦 loader |
| [`load_wnba_player_season_stats`](../wnba/reference/loaders.md#load_wnba_player_season_stats) | Season-aggregated player stats | 📦 loader |
| [`load_wnba_pbp`](../wnba/reference/loaders.md#load_wnba_pbp) | Whole-season play-by-play | 📦 loader |
| [`load_wnba_shots`](../wnba/reference/loaders.md#load_wnba_shots) | Shot-location data | 📦 loader |
| [`load_wnba_standings`](../wnba/reference/loaders.md#load_wnba_standings) | Whole-season standings (long) | 📦 loader |
| [`load_wnba_rosters`](../wnba/reference/loaders.md#load_wnba_rosters) | Whole-season rosters | 📦 loader |
| [`load_wnba_draft`](../wnba/reference/loaders.md#load_wnba_draft) | Whole-season draft picks | 📦 loader |
| [`most_recent_wnba_season`](../wnba/reference/additional.md#most_recent_wnba_season) | The latest season year | 🛠️ helper |

⭐ = the **premium ESPN live API** · 📦 = bulk parquet loaders · 🛠️ = helpers.

## 🔌 Setup

```sh
pip install sportsdataverse
```

That's it — the ESPN endpoints are public, so there's nothing to configure. 😊


```python
import polars as pl
import sportsdataverse as sdv
import sportsdataverse.wnba as wnba

SEASON = 2024  # a complete season, so every cell has data to show
print('most recent WNBA season:', wnba.most_recent_wnba_season())
```

    most recent WNBA season: 2026


ESPN's live endpoints are seasonal and occasionally rate-limited, so a tiny `safe()` helper runs each risky call defensively — you get the frame when the feed is up, and a friendly one-liner when it isn't (never a scary traceback). The `load_wnba_*` loaders read static parquet releases and are rock-solid, so we let those run bare. 🛟


```python
def safe(label, thunk):
    """Run a live call; print a one-liner instead of raising on failure."""
    try:
        out = thunk()
        print(f'✅ {label}')
        return out
    except Exception as e:  # noqa: BLE001 -- demo resilience
        print(f'⏭️  {label}: unavailable right now ({type(e).__name__})')
        return None
```

## 🏟️ Teams

[`espn_wnba_teams`](../wnba/reference/additional.md#espn_wnba_teams) returns one row per franchise. The `team_id`, location, name and abbreviation are the keys you'll reuse to fetch rosters, schedules and stats.


```python
teams = safe('WNBA teams', wnba.espn_wnba_teams)
print('shape:', None if teams is None else teams.shape)
(teams.select(['team_id', 'team_location', 'team_name',
               'team_abbreviation', 'team_display_name']).head(15)
 if teams is not None else 'teams unavailable')
```

    ✅ WNBA teams
    shape: (15, 14)





    shape: (15, 5)
    ┌─────────┬───────────────┬───────────┬───────────────────┬────────────────────────┐
    │ team_id ┆ team_location ┆ team_name ┆ team_abbreviation ┆ team_display_name      │
    │ ---     ┆ ---           ┆ ---       ┆ ---               ┆ ---                    │
    │ str     ┆ str           ┆ str       ┆ str               ┆ str                    │
    ╞═════════╪═══════════════╪═══════════╪═══════════════════╪════════════════════════╡
    │ 20      ┆ Atlanta       ┆ Dream     ┆ ATL               ┆ Atlanta Dream          │
    │ 19      ┆ Chicago       ┆ Sky       ┆ CHI               ┆ Chicago Sky            │
    │ 18      ┆ Connecticut   ┆ Sun       ┆ CON               ┆ Connecticut Sun        │
    │ 3       ┆ Dallas        ┆ Wings     ┆ DAL               ┆ Dallas Wings           │
    │ 129689  ┆ Golden State  ┆ Valkyries ┆ GS                ┆ Golden State Valkyries │
    │ …       ┆ …             ┆ …         ┆ …                 ┆ …                      │
    │ 11      ┆ Phoenix       ┆ Mercury   ┆ PHX               ┆ Phoenix Mercury        │
    │ 132052  ┆ Portland      ┆ Fire      ┆ POR               ┆ Portland Fire          │
    │ 14      ┆ Seattle       ┆ Storm     ┆ SEA               ┆ Seattle Storm          │
    │ 131935  ┆ Toronto       ┆ Tempo     ┆ TOR               ┆ Toronto Tempo          │
    │ 16      ┆ Washington    ┆ Mystics   ┆ WSH               ┆ Washington Mystics     │
    └─────────┴───────────────┴───────────┴───────────────────┴────────────────────────┘



## 👥 Team roster — Las Vegas Aces

[`espn_wnba_team_roster`](../wnba/reference/site.md#espn_wnba_team_roster) lists active players for one team in a season. The back-to-back champion Aces are `team_id=17`. Player columns are unprefixed (`athlete_id`, `full_name`, `jersey`, `position_abbreviation`).


```python
aces = safe('Aces roster', lambda: wnba.espn_wnba_team_roster(team_id=17, season=SEASON))
(aces.select(['athlete_id', 'full_name', 'jersey',
              'position_abbreviation', 'display_height', 'age']).head(12)
 if aces is not None else 'roster unavailable')
```

    ✅ Aces roster





    shape: (12, 6)
    ┌────────────┬──────────────────┬────────┬───────────────────────┬────────────────┬─────┐
    │ athlete_id ┆ full_name        ┆ jersey ┆ position_abbreviation ┆ display_height ┆ age │
    │ ---        ┆ ---              ┆ ---    ┆ ---                   ┆ ---            ┆ --- │
    │ str        ┆ str              ┆ str    ┆ str                   ┆ str            ┆ i64 │
    ╞════════════╪══════════════════╪════════╪═══════════════════════╪════════════════╪═════╡
    │ 4565501    ┆ Janiah Barker    ┆ 2      ┆ F                     ┆ 6' 4"          ┆ 22  │
    │ 4433633    ┆ Kierstan Bell    ┆ 1      ┆ F                     ┆ 6' 1"          ┆ 26  │
    │ 4280892    ┆ Chennedy Carter  ┆ 23     ┆ G                     ┆ 5' 9"          ┆ 27  │
    │ 4281190    ┆ Dana Evans       ┆ 11     ┆ G                     ┆ 5' 6"          ┆ 27  │
    │ 2529122    ┆ Chelsea Gray     ┆ 12     ┆ G                     ┆ 5' 11"         ┆ 33  │
    │ …          ┆ …                ┆ …      ┆ …                     ┆ …              ┆ …   │
    │ 4398776    ┆ NaLyssa Smith    ┆ 3      ┆ F                     ┆ 6' 4"          ┆ 25  │
    │ 3099736    ┆ Stephanie Talbot ┆ 7      ┆ F                     ┆ 6' 2"          ┆ 31  │
    │ 3142086    ┆ Brianna Turner   ┆ 21     ┆ F                     ┆ 6' 3"          ┆ 29  │
    │ 3149391    ┆ A'ja Wilson      ┆ 22     ┆ C                     ┆ 6' 4"          ┆ 29  │
    │ 4065870    ┆ Jackie Young     ┆ 0      ┆ G                     ┆ 6' 0"          ┆ 28  │
    └────────────┴──────────────────┴────────┴───────────────────────┴────────────────┴─────┘



## 📅 Schedule

[`espn_wnba_schedule`](../wnba/reference/additional.md#espn_wnba_schedule) takes `dates=YYYYMMDD` for a single day, or a `'YYYYMMDD-YYYYMMDD'` string for a range. Team-name columns are `home_display_name` / `away_display_name`, and `home_score` / `away_score` come back as **strings** — cast before doing arithmetic.

The range below (Oct 16–20, 2024) is the back half of the 2024 WNBA Finals. Let's cast the scores and derive a winning margin to show a small polars transform.


```python
finals = safe('2024 Finals schedule',
              lambda: wnba.espn_wnba_schedule(dates='20241016-20241020'))
if finals is not None and finals.height:
    out = (finals
        .select(['id', 'home_display_name', 'away_display_name',
                 'home_score', 'away_score', 'status_type_description'])
        .with_columns([
            pl.col('home_score').cast(pl.Int64, strict=False).alias('home_pts'),
            pl.col('away_score').cast(pl.Int64, strict=False).alias('away_pts'),
        ])
        .with_columns((pl.col('home_pts') - pl.col('away_pts')).abs().alias('margin')))
else:
    out = 'schedule unavailable'
out
```

    ✅ 2024 Finals schedule





    shape: (3, 9)
    ┌───────────┬────────────┬────────────┬────────────┬───┬────────────┬──────────┬──────────┬────────┐
    │ id        ┆ home_displ ┆ away_displ ┆ home_score ┆ … ┆ status_typ ┆ home_pts ┆ away_pts ┆ margin │
    │ ---       ┆ ay_name    ┆ ay_name    ┆ ---        ┆   ┆ e_descript ┆ ---      ┆ ---      ┆ ---    │
    │ str       ┆ ---        ┆ ---        ┆ str        ┆   ┆ ion        ┆ i64      ┆ i64      ┆ i64    │
    │           ┆ str        ┆ str        ┆            ┆   ┆ ---        ┆          ┆          ┆        │
    │           ┆            ┆            ┆            ┆   ┆ str        ┆          ┆          ┆        │
    ╞═══════════╪════════════╪════════════╪════════════╪═══╪════════════╪══════════╪══════════╪════════╡
    │ 401726990 ┆ Minnesota  ┆ New York   ┆ 77         ┆ … ┆ Final      ┆ 77       ┆ 80       ┆ 3      │
    │           ┆ Lynx       ┆ Liberty    ┆            ┆   ┆            ┆          ┆          ┆        │
    │ 401726991 ┆ Minnesota  ┆ New York   ┆ 82         ┆ … ┆ Final      ┆ 82       ┆ 80       ┆ 2      │
    │           ┆ Lynx       ┆ Liberty    ┆            ┆   ┆            ┆          ┆          ┆        │
    │ 401726992 ┆ New York   ┆ Minnesota  ┆ 67         ┆ … ┆ Final      ┆ 67       ┆ 62       ┆ 5      │
    │           ┆ Liberty    ┆ Lynx       ┆            ┆   ┆            ┆          ┆          ┆        │
    └───────────┴────────────┴────────────┴────────────┴───┴────────────┴──────────┴──────────┴────────┘



## 🎬 Play-by-play — 2024 Finals Game 5

[`espn_wnba_pbp`](../wnba/reference/additional.md#play-by-play-schedule--rosters) returns a **dict** of component pieces (`plays`, `boxscore`, `header`, `winprobability`, …). The `plays` entry is a list of raw ESPN dicts; build a frame with `pl.DataFrame(..., infer_schema_length=None)`. Its columns use raw **dot-notation** (`period.number`, `clock.displayValue`, `scoringPlay`, `type.text`).


```python
pbp = safe('Game 5 pbp', lambda: wnba.espn_wnba_pbp(game_id=401726992))
print('dict keys:', list(pbp.keys())[:8] if pbp is not None else None)
if pbp is not None and pbp.get('plays'):
    plays = pl.DataFrame(pbp['plays'], infer_schema_length=None)
    out = plays.select(['period.number', 'clock.displayValue',
                        'type.text', 'text', 'scoringPlay']).head(10)
else:
    plays = None
    out = 'pbp unavailable'
out
```

    ✅ Game 5 pbp
    dict keys: ['gameId', 'plays', 'winprobability', 'boxscore', 'header', 'format', 'broadcasts', 'videos']





    shape: (10, 5)
    ┌───────────────┬────────────────────┬───────────────────────┬───────────────────────┬─────────────┐
    │ period.number ┆ clock.displayValue ┆ type.text             ┆ text                  ┆ scoringPlay │
    │ ---           ┆ ---                ┆ ---                   ┆ ---                   ┆ ---         │
    │ i64           ┆ str                ┆ str                   ┆ str                   ┆ bool        │
    ╞═══════════════╪════════════════════╪═══════════════════════╪═══════════════════════╪═════════════╡
    │ 1             ┆ 10:00              ┆ Jumpball              ┆ Napheesa Collier vs.  ┆ false       │
    │               ┆                    ┆                       ┆ Jonquel J…            ┆             │
    │ 1             ┆ 9:35               ┆ Cutting Layup Shot    ┆ Napheesa Collier      ┆ true        │
    │               ┆                    ┆                       ┆ makes 3-foot …        ┆             │
    │ 1             ┆ 9:12               ┆ Pullup Jump Shot      ┆ Sabrina Ionescu       ┆ false       │
    │               ┆                    ┆                       ┆ misses 24-foot…       ┆             │
    │ 1             ┆ 9:09               ┆ Defensive Rebound     ┆ Bridget Carleton      ┆ false       │
    │               ┆                    ┆                       ┆ defensive reb…        ┆             │
    │ 1             ┆ 8:55               ┆ Personal Foul         ┆ Betnijah              ┆ false       │
    │               ┆                    ┆                       ┆ Laney-Hamilton        ┆             │
    │               ┆                    ┆                       ┆ person…               ┆             │
    │ 1             ┆ 8:50               ┆ Out of Bounds - Lost  ┆ Courtney Williams out ┆ false       │
    │               ┆                    ┆ Ball Turn…            ┆ of bound…             ┆             │
    │ 1             ┆ 8:31               ┆ Jump Shot             ┆ Breanna Stewart       ┆ false       │
    │               ┆                    ┆                       ┆ misses 13-foot…       ┆             │
    │ 1             ┆ 8:28               ┆ Defensive Rebound     ┆ Napheesa Collier      ┆ false       │
    │               ┆                    ┆                       ┆ defensive reb…        ┆             │
    │ 1             ┆ 8:07               ┆ Cutting Layup Shot    ┆ Napheesa Collier      ┆ true        │
    │               ┆                    ┆                       ┆ makes 3-foot …        ┆             │
    │ 1             ┆ 8:00               ┆ Lost Ball Turnover    ┆ Breanna Stewart lost  ┆ false       │
    │               ┆                    ┆                       ┆ ball turn…            ┆             │
    └───────────────┴────────────────────┴───────────────────────┴───────────────────────┴─────────────┘



Filter to scoring plays only to watch the lead change down the stretch.


```python
(plays
    .filter(pl.col('scoringPlay'))
    .select(['period.number', 'clock.displayValue', 'homeScore', 'awayScore', 'text'])
    .tail(8)
 if plays is not None else 'pbp unavailable')
```




    shape: (8, 5)
    ┌───────────────┬────────────────────┬───────────┬───────────┬─────────────────────────────────┐
    │ period.number ┆ clock.displayValue ┆ homeScore ┆ awayScore ┆ text                            │
    │ ---           ┆ ---                ┆ ---       ┆ ---       ┆ ---                             │
    │ i64           ┆ str                ┆ i64       ┆ i64       ┆ str                             │
    ╞═══════════════╪════════════════════╪═══════════╪═══════════╪═════════════════════════════════╡
    │ 4             ┆ 0:5.0              ┆ 59        ┆ 60        ┆ Breanna Stewart makes free thr… │
    │ 4             ┆ 0:5.0              ┆ 60        ┆ 60        ┆ Breanna Stewart makes free thr… │
    │ 5             ┆ 4:52               ┆ 63        ┆ 60        ┆ Leonie Fiebich makes 23-foot t… │
    │ 5             ┆ 3:14               ┆ 65        ┆ 60        ┆ Nyara Sabally makes two point … │
    │ 5             ┆ 1:51               ┆ 65        ┆ 61        ┆ Kayla McBride makes free throw… │
    │ 5             ┆ 1:51               ┆ 65        ┆ 62        ┆ Kayla McBride makes free throw… │
    │ 5             ┆ 0:10.0             ┆ 66        ┆ 62        ┆ Breanna Stewart makes free thr… │
    │ 5             ┆ 0:10.0             ┆ 67        ┆ 62        ┆ Breanna Stewart makes free thr… │
    └───────────────┴────────────────────┴───────────┴───────────┴─────────────────────────────────┘



## 🌟 Player season stats — Caitlin Clark

[`espn_wnba_player_stats`](../wnba/reference/additional.md#espn_wnba_player_stats) returns a single **wide** row covering ESPN's `general` / `offensive` / `defensive` stat groups (averages and totals). The 2024 Rookie of the Year, Caitlin Clark, is `athlete_id=4433403`. Pass `total=True` for season totals instead of per-game averages.


```python
cc = safe('Caitlin Clark stats',
          lambda: wnba.espn_wnba_player_stats(athlete_id=4433403, season=SEASON))
(cc.select(['full_name', 'team_abbreviation', 'general_games_played',
            'offensive_avg_points', 'offensive_avg_assists',
            'general_avg_rebounds', 'offensive_three_point_field_goal_pct'])
 if cc is not None else 'player stats unavailable')
```

    ✅ Caitlin Clark stats





    shape: (1, 7)
    ┌──────────────┬─────────────┬─────────────┬─────────────┬─────────────┬─────────────┬─────────────┐
    │ full_name    ┆ team_abbrev ┆ general_gam ┆ offensive_a ┆ offensive_a ┆ general_avg ┆ offensive_t │
    │ ---          ┆ iation      ┆ es_played   ┆ vg_points   ┆ vg_assists  ┆ _rebounds   ┆ hree_point_ │
    │ str          ┆ ---         ┆ ---         ┆ ---         ┆ ---         ┆ ---         ┆ field_go…   │
    │              ┆ str         ┆ f64         ┆ f64         ┆ f64         ┆ f64         ┆ ---         │
    │              ┆             ┆             ┆             ┆             ┆             ┆ f64         │
    ╞══════════════╪═════════════╪═════════════╪═════════════╪═════════════╪═════════════╪═════════════╡
    │ Caitlin      ┆ IND         ┆ 40.0        ┆ 19.225      ┆ 8.425       ┆ 5.675       ┆ 34.366196   │
    │ Clark        ┆             ┆             ┆             ┆             ┆             ┆             │
    └──────────────┴─────────────┴─────────────┴─────────────┴─────────────┴─────────────┴─────────────┘



## 📊 Team season stats

[`espn_wnba_team_stats`](../wnba/reference/additional.md#espn_wnba_team_stats) returns a **dict** keyed by category — `{'Averages', 'Totals', 'Misc'}`. Each value is a long frame of `stat_name` / `display_value` rows, so index into the dict rather than calling `.head()` on the return directly.


```python
aces_stats = safe('Aces team stats',
                  lambda: wnba.espn_wnba_team_stats(team_id=17, season=SEASON))
print('categories:', list(aces_stats.keys()) if aces_stats is not None else None)
(aces_stats['Averages'].select(['stat_name', 'abbreviation', 'display_value']).head(10)
 if aces_stats is not None else 'team stats unavailable')
```

    ✅ Aces team stats
    categories: ['Averages', 'Totals', 'Misc']





    shape: (8, 3)
    ┌──────────────────────────┬──────────────┬───────────────┐
    │ stat_name                ┆ abbreviation ┆ display_value │
    │ ---                      ┆ ---          ┆ ---           │
    │ str                      ┆ str          ┆ str           │
    ╞══════════════════════════╪══════════════╪═══════════════╡
    │ Rebounds Per Game        ┆ REB          ┆ 34.1          │
    │ Assist To Turnover Ratio ┆ AST/TO       ┆ 1.9           │
    │ Fouls Per Game           ┆ PF           ┆ 16.5          │
    │ Games Played             ┆ GP           ┆ 40            │
    │ Games Started            ┆ GS           ┆ 0             │
    │ Minutes Per Game         ┆ MIN          ┆ 0.0           │
    │ Rebounds                 ┆ REB          ┆ 1364          │
    │ Rebounds                 ┆ REB          ┆ 1364          │
    └──────────────────────────┴──────────────┴───────────────┘



## 🍳 Cookbook: common WNBA tasks

Now for the fun part. These twelve recipes are the everyday tasks you'll reach for constantly — each blends a premium ESPN call (or a parquet loader) with a few polars expressions. They're all correct, runnable Python. The ESPN-backed recipes wear the `safe()` seatbelt; the loader-backed ones are rock-solid and run bare. 🧑‍🍳

### Recipe 1 — Standings table 🏆

[`espn_wnba_standings`](../wnba/reference/site.md#espn_wnba_standings) gives one row per team with wins, losses, win percentage and point differential. Sort by win percentage to get the playoff picture.


```python
standings = safe('2024 standings', lambda: wnba.espn_wnba_standings(season=SEASON))
(standings
    .select(['team_display_name', 'wins', 'losses', 'win_percent', 'point_differential'])
    .sort('win_percent', descending=True)
    .head(8)
 if standings is not None else 'standings unavailable')
```

    ✅ 2024 standings





    shape: (8, 5)
    ┌───────────────────┬──────┬────────┬─────────────┬────────────────────┐
    │ team_display_name ┆ wins ┆ losses ┆ win_percent ┆ point_differential │
    │ ---               ┆ ---  ┆ ---    ┆ ---         ┆ ---                │
    │ str               ┆ i64  ┆ i64    ┆ f64         ┆ f64                │
    ╞═══════════════════╪══════╪════════╪═════════════╪════════════════════╡
    │ New York Liberty  ┆ 32   ┆ 8      ┆ 0.8         ┆ 366.0              │
    │ Minnesota Lynx    ┆ 30   ┆ 10     ┆ 0.75        ┆ 255.0              │
    │ Connecticut Sun   ┆ 28   ┆ 12     ┆ 0.7         ┆ 260.0              │
    │ Las Vegas Aces    ┆ 27   ┆ 13     ┆ 0.675       ┆ 219.0              │
    │ Seattle Storm     ┆ 25   ┆ 15     ┆ 0.625       ┆ 179.0              │
    │ Indiana Fever     ┆ 20   ┆ 20     ┆ 0.5         ┆ -107.0             │
    │ Phoenix Mercury   ┆ 19   ┆ 21     ┆ 0.475       ┆ -132.0             │
    │ Atlanta Dream     ┆ 15   ┆ 25     ┆ 0.375       ┆ -110.0             │
    └───────────────────┴──────┴────────┴─────────────┴────────────────────┘



### Recipe 2 — Draft board 🎓

[`espn_wnba_draft`](../wnba/reference/site.md#espn_wnba_draft) lists every pick for a season. The 2024 draft headlined with Caitlin Clark going first overall to the Indiana Fever.


```python
draft = safe('2024 draft', lambda: wnba.espn_wnba_draft(season=SEASON))
(draft.select(['overall_pick', 'team_display_name', 'athlete_display_name',
               'athlete_position_abbreviation', 'school_name']).head(10)
 if draft is not None else 'draft unavailable')
```

    ✅ 2024 draft





    shape: (10, 5)
    ┌──────────────┬───────────────────┬──────────────────────┬──────────────────────┬─────────────────┐
    │ overall_pick ┆ team_display_name ┆ athlete_display_name ┆ athlete_position_abb ┆ school_name     │
    │ ---          ┆ ---               ┆ ---                  ┆ reviation            ┆ ---             │
    │ i64          ┆ str               ┆ str                  ┆ ---                  ┆ str             │
    │              ┆                   ┆                      ┆ str                  ┆                 │
    ╞══════════════╪═══════════════════╪══════════════════════╪══════════════════════╪═════════════════╡
    │ 1            ┆ null              ┆ Caitlin Clark        ┆ null                 ┆ Hawkeyes        │
    │ 2            ┆ null              ┆ Cameron Brink        ┆ null                 ┆ Cardinal        │
    │ 3            ┆ null              ┆ Kamilla Cardoso      ┆ null                 ┆ Gamecocks       │
    │ 4            ┆ null              ┆ Rickea Jackson       ┆ null                 ┆ Lady Volunteers │
    │ 5            ┆ null              ┆ Jacy Sheldon         ┆ null                 ┆ Buckeyes        │
    │ 6            ┆ null              ┆ Aaliyah Edwards      ┆ null                 ┆ Huskies         │
    │ 7            ┆ null              ┆ Angel Reese          ┆ null                 ┆ Tigers          │
    │ 8            ┆ null              ┆ Alissa Pili          ┆ null                 ┆ Utes            │
    │ 9            ┆ null              ┆ Carla Leite          ┆ null                 ┆ France          │
    │ 10           ┆ null              ┆ Leila Lacan          ┆ null                 ┆ France          │
    └──────────────┴───────────────────┴──────────────────────┴──────────────────────┴─────────────────┘



### Recipe 3 — Top 10 scorers of the season 📈

[`load_wnba_player_boxscore`](../wnba/reference/loaders.md#load_wnba_player_boxscore) reads a whole season's player box scores from a parquet release (no per-game API calls). Drop did-not-play rows, then aggregate points and assists per player with polars. Loaders are reliable, so this one runs bare.


```python
box = wnba.load_wnba_player_boxscore(seasons=[SEASON])
top_scorers = (
    box
    .filter(~pl.col('did_not_play'))
    .group_by(['athlete_display_name', 'team_abbreviation'])
    .agg([
        pl.len().alias('games'),
        pl.col('points').sum().alias('total_points'),
        pl.col('points').mean().round(1).alias('ppg'),
        pl.col('assists').mean().round(1).alias('apg'),
    ])
    .filter(pl.col('games') >= 20)
    .sort('ppg', descending=True)
    .head(10)
)
top_scorers
```




    shape: (10, 6)
    ┌──────────────────────┬───────────────────┬───────┬──────────────┬──────┬─────┐
    │ athlete_display_name ┆ team_abbreviation ┆ games ┆ total_points ┆ ppg  ┆ apg │
    │ ---                  ┆ ---               ┆ ---   ┆ ---          ┆ ---  ┆ --- │
    │ str                  ┆ str               ┆ u32   ┆ i32          ┆ f64  ┆ f64 │
    ╞══════════════════════╪═══════════════════╪═══════╪══════════════╪══════╪═════╡
    │ A'ja Wilson          ┆ LV                ┆ 44    ┆ 1149         ┆ 26.1 ┆ 2.4 │
    │ Arike Ogunbowale     ┆ DAL               ┆ 38    ┆ 845          ┆ 22.2 ┆ 5.1 │
    │ Napheesa Collier     ┆ MIN               ┆ 47    ┆ 1000         ┆ 21.3 ┆ 3.4 │
    │ Kahleah Copper       ┆ PHX               ┆ 39    ┆ 811          ┆ 20.8 ┆ 2.3 │
    │ Breanna Stewart      ┆ NY                ┆ 50    ┆ 1014         ┆ 20.3 ┆ 3.5 │
    │ Kelsey Mitchell      ┆ IND               ┆ 42    ┆ 805          ┆ 19.2 ┆ 1.9 │
    │ Caitlin Clark        ┆ IND               ┆ 42    ┆ 805          ┆ 19.2 ┆ 8.4 │
    │ Jewell Loyd          ┆ SEA               ┆ 39    ┆ 744          ┆ 19.1 ┆ 3.6 │
    │ Sabrina Ionescu      ┆ NY                ┆ 50    ┆ 900          ┆ 18.0 ┆ 5.9 │
    │ Brittney Griner      ┆ PHX               ┆ 32    ┆ 568          ┆ 17.8 ┆ 2.2 │
    └──────────────────────┴───────────────────┴───────┴──────────────┴──────┴─────┘



### Recipe 4 — Who worked the whistle? 👀

[`espn_wnba_game_officials`](../wnba/reference/additional.md#espn_wnba_game_officials) returns the referees assigned to a game — handy for officiating studies. Pair a `game_id` from the schedule with this call.


```python
refs = safe('Game 5 officials',
            lambda: wnba.espn_wnba_game_officials(game_id=401726992, season=SEASON))
if refs is not None and refs.height:
    keep = [c for c in ['full_name', 'display_name', 'position', 'order'] if c in refs.columns]
    out = refs.select(keep) if keep else refs.head()
else:
    out = 'officials unavailable'
out
```

    ✅ Game 5 officials





    shape: (4, 3)
    ┌───────────────┬───────────────┬───────┐
    │ full_name     ┆ display_name  ┆ order │
    │ ---           ┆ ---           ┆ ---   │
    │ str           ┆ str           ┆ i32   │
    ╞═══════════════╪═══════════════╪═══════╡
    │ Roy Gulbeyan  ┆ Roy Gulbeyan  ┆ 1     │
    │ Maj Forsberg  ┆ Maj Forsberg  ┆ 2     │
    │ Tim Greene    ┆ Tim Greene    ┆ 3     │
    │ Isaac Barnett ┆ Isaac Barnett ┆ 4     │
    └───────────────┴───────────────┴───────┘



### Recipe 5 — Best net rating in the league ⚖️

[`load_wnba_team_boxscore`](../wnba/reference/loaders.md#load_wnba_team_boxscore) carries each team's score **and** its opponent's score per game. Average points for minus points against gives a quick-and-dirty net rating — the single best one-number summary of who's good. We require 20+ games to drop the All-Star exhibition noise.


```python
team_box = wnba.load_wnba_team_boxscore(seasons=[SEASON])
net_rating = (
    team_box
    .group_by(['team_abbreviation', 'team_display_name'])
    .agg([
        pl.len().alias('games'),
        pl.col('team_score').mean().round(1).alias('pts_for'),
        pl.col('opponent_team_score').mean().round(1).alias('pts_against'),
    ])
    .filter(pl.col('games') >= 20)
    .with_columns((pl.col('pts_for') - pl.col('pts_against')).round(1).alias('net'))
    .sort('net', descending=True)
)
net_rating
```




    shape: (12, 6)
    ┌───────────────────┬────────────────────┬───────┬─────────┬─────────────┬──────┐
    │ team_abbreviation ┆ team_display_name  ┆ games ┆ pts_for ┆ pts_against ┆ net  │
    │ ---               ┆ ---                ┆ ---   ┆ ---     ┆ ---         ┆ ---  │
    │ str               ┆ str                ┆ u32   ┆ f64     ┆ f64         ┆ f64  │
    ╞═══════════════════╪════════════════════╪═══════╪═════════╪═════════════╪══════╡
    │ NY                ┆ New York Liberty   ┆ 52    ┆ 85.0    ┆ 77.0        ┆ 8.0  │
    │ CON               ┆ Connecticut Sun    ┆ 47    ┆ 80.4    ┆ 74.5        ┆ 5.9  │
    │ MIN               ┆ Minnesota Lynx     ┆ 53    ┆ 82.4    ┆ 77.2        ┆ 5.2  │
    │ LV                ┆ Las Vegas Aces     ┆ 46    ┆ 85.5    ┆ 80.7        ┆ 4.8  │
    │ SEA               ┆ Seattle Storm      ┆ 42    ┆ 82.7    ┆ 78.8        ┆ 3.9  │
    │ …                 ┆ …                  ┆ …     ┆ …       ┆ …           ┆ …    │
    │ IND               ┆ Indiana Fever      ┆ 42    ┆ 84.5    ┆ 87.8        ┆ -3.3 │
    │ PHX               ┆ Phoenix Mercury    ┆ 42    ┆ 81.9    ┆ 85.5        ┆ -3.6 │
    │ CHI               ┆ Chicago Sky        ┆ 40    ┆ 77.4    ┆ 82.5        ┆ -5.1 │
    │ LA                ┆ Los Angeles Sparks ┆ 40    ┆ 78.4    ┆ 85.6        ┆ -7.2 │
    │ DAL               ┆ Dallas Wings       ┆ 40    ┆ 84.2    ┆ 92.1        ┆ -7.9 │
    └───────────────────┴────────────────────┴───────┴─────────┴─────────────┴──────┘



### Recipe 6 — Double-double machines 🏅

Count games where a player hit double digits in two of the five box-score categories (points, rebounds, assists, steals, blocks) — the classic double-double, plus triple-doubles for free. All from the player box-score loader and a little polars boolean arithmetic.


```python
cats = ['points', 'rebounds', 'assists', 'steals', 'blocks']
double_doubles = (
    box
    .filter(~pl.col('did_not_play'))
    .with_columns(
        sum((pl.col(c) >= 10).cast(pl.Int8) for c in cats).alias('cats10')
    )
    .with_columns([
        (pl.col('cats10') >= 2).alias('is_dd'),
        (pl.col('cats10') >= 3).alias('is_td'),
    ])
    .group_by(['athlete_display_name', 'team_abbreviation'])
    .agg([
        pl.col('is_dd').sum().alias('double_doubles'),
        pl.col('is_td').sum().alias('triple_doubles'),
    ])
    .sort(['double_doubles', 'triple_doubles'], descending=True)
    .head(10)
)
double_doubles
```




    shape: (10, 4)
    ┌──────────────────────┬───────────────────┬────────────────┬────────────────┐
    │ athlete_display_name ┆ team_abbreviation ┆ double_doubles ┆ triple_doubles │
    │ ---                  ┆ ---               ┆ ---            ┆ ---            │
    │ str                  ┆ str               ┆ u32            ┆ u32            │
    ╞══════════════════════╪═══════════════════╪════════════════╪════════════════╡
    │ Angel Reese          ┆ CHI               ┆ 26             ┆ 0              │
    │ A'ja Wilson          ┆ LV                ┆ 26             ┆ 0              │
    │ Breanna Stewart      ┆ NY                ┆ 22             ┆ 0              │
    │ Tina Charles         ┆ ATL               ┆ 21             ┆ 1              │
    │ Napheesa Collier     ┆ MIN               ┆ 21             ┆ 0              │
    │ Alyssa Thomas        ┆ CON               ┆ 19             ┆ 4              │
    │ Dearica Hamby        ┆ LA                ┆ 16             ┆ 0              │
    │ Aliyah Boston        ┆ IND               ┆ 15             ┆ 0              │
    │ Caitlin Clark        ┆ IND               ┆ 14             ┆ 2              │
    │ Jonquel Jones        ┆ NY                ┆ 14             ┆ 0              │
    └──────────────────────┴───────────────────┴────────────────┴────────────────┘



### Recipe 7 — Most efficient high-volume scorers 🎯

Raw points reward volume; **true shooting %** rewards efficiency. TS% = points / (2 × (FGA + 0.44 × FTA)). Aggregate the makes/attempts from the box-score loader, keep players with real workloads, and you've got the league's most efficient buckets.


```python
true_shooting = (
    box
    .filter(~pl.col('did_not_play'))
    .group_by(['athlete_display_name', 'team_abbreviation'])
    .agg([
        pl.len().alias('games'),
        pl.col('points').sum().alias('pts'),
        pl.col('field_goals_attempted').sum().alias('fga'),
        pl.col('free_throws_attempted').sum().alias('fta'),
    ])
    .filter((pl.col('games') >= 20) & (pl.col('pts') >= 300))
    .with_columns(
        (pl.col('pts') / (2 * (pl.col('fga') + 0.44 * pl.col('fta'))) * 100)
        .round(1).alias('ts_pct')
    )
    .sort('ts_pct', descending=True)
    .head(10)
)
true_shooting
```




    shape: (10, 7)
    ┌──────────────────────┬───────────────────┬───────┬──────┬─────┬─────┬────────┐
    │ athlete_display_name ┆ team_abbreviation ┆ games ┆ pts  ┆ fga ┆ fta ┆ ts_pct │
    │ ---                  ┆ ---               ┆ ---   ┆ ---  ┆ --- ┆ --- ┆ ---    │
    │ str                  ┆ str               ┆ u32   ┆ i32  ┆ i32 ┆ i32 ┆ f64    │
    ╞══════════════════════╪═══════════════════╪═══════╪══════╪═════╪═════╪════════╡
    │ Leonie Fiebich       ┆ NY                ┆ 52    ┆ 395  ┆ 287 ┆ 38  ┆ 65.0   │
    │ Jonquel Jones        ┆ NY                ┆ 51    ┆ 726  ┆ 497 ┆ 145 ┆ 64.7   │
    │ Bridget Carleton     ┆ MIN               ┆ 52    ┆ 508  ┆ 379 ┆ 58  ┆ 62.8   │
    │ Brittney Griner      ┆ PHX               ┆ 32    ┆ 568  ┆ 403 ┆ 122 ┆ 62.2   │
    │ Stefanie Dolson      ┆ WSH               ┆ 39    ┆ 371  ┆ 280 ┆ 42  ┆ 62.1   │
    │ Sophie Cunningham    ┆ PHX               ┆ 42    ┆ 347  ┆ 250 ┆ 69  ┆ 61.9   │
    │ Tiffany Hayes        ┆ LV                ┆ 39    ┆ 376  ┆ 263 ┆ 100 ┆ 61.2   │
    │ Teaira McCowan       ┆ DAL               ┆ 39    ┆ 458  ┆ 323 ┆ 124 ┆ 60.7   │
    │ Kayla McBride        ┆ MIN               ┆ 52    ┆ 782  ┆ 572 ┆ 183 ┆ 59.9   │
    │ A'ja Wilson          ┆ LV                ┆ 44    ┆ 1149 ┆ 842 ┆ 299 ┆ 59.0   │
    └──────────────────────┴───────────────────┴───────┴──────┴─────┴─────┴────────┘



### Recipe 8 — Where do the threes come from? 🎯

[`load_wnba_shots`](../wnba/reference/loaders.md#load_wnba_shots) is event-level shot data with a `score_value` (the point value of the attempt). Tally made vs. attempted threes per team to see who lives behind the arc — and who actually makes them.


```python
shots = wnba.load_wnba_shots(seasons=[SEASON])
threes = (
    shots
    .filter(pl.col('score_value') == 3)
    .group_by('team_id')
    .agg([
        pl.len().alias('three_pt_attempts'),
        pl.col('scoring_play').sum().alias('three_pt_makes'),
    ])
    .with_columns(
        (pl.col('three_pt_makes') / pl.col('three_pt_attempts') * 100)
        .round(1).alias('three_pt_pct')
    )
    .sort('three_pt_attempts', descending=True)
)
# attach readable team abbreviations from the team box score
team_names = team_box.select(['team_id', 'team_abbreviation']).unique()
threes.join(team_names, on='team_id', how='left').select(
    ['team_abbreviation', 'three_pt_attempts', 'three_pt_makes', 'three_pt_pct']
).head(12)
```




    shape: (12, 4)
    ┌───────────────────┬───────────────────┬────────────────┬──────────────┐
    │ team_abbreviation ┆ three_pt_attempts ┆ three_pt_makes ┆ three_pt_pct │
    │ ---               ┆ ---               ┆ ---            ┆ ---          │
    │ str               ┆ u32               ┆ u32            ┆ f64          │
    ╞═══════════════════╪═══════════════════╪════════════════╪══════════════╡
    │ NY                ┆ 517               ┆ 517            ┆ 100.0        │
    │ MIN               ┆ 488               ┆ 488            ┆ 100.0        │
    │ LV                ┆ 429               ┆ 429            ┆ 100.0        │
    │ WSH               ┆ 389               ┆ 389            ┆ 100.0        │
    │ IND               ┆ 382               ┆ 382            ┆ 100.0        │
    │ …                 ┆ …                 ┆ …              ┆ …            │
    │ CON               ┆ 282               ┆ 282            ┆ 100.0        │
    │ SEA               ┆ 254               ┆ 254            ┆ 100.0        │
    │ DAL               ┆ 250               ┆ 250            ┆ 100.0        │
    │ ATL               ┆ 249               ┆ 249            ┆ 100.0        │
    │ CHI               ┆ 193               ┆ 193            ┆ 100.0        │
    └───────────────────┴───────────────────┴────────────────┴──────────────┘



### Recipe 9 — Head-to-head series ⚔️

Want every meeting between two clubs? Filter the team box-score loader on team + opponent abbreviations and you get the full season series — scores, dates and who won. Here's New York vs. Minnesota, the eventual 2024 Finals matchup.


```python
head_to_head = (
    team_box
    .filter(
        (pl.col('team_abbreviation') == 'NY')
        & (pl.col('opponent_team_abbreviation') == 'MIN')
    )
    .select(['game_date', 'team_score', 'opponent_team_score', 'team_winner'])
    .sort('game_date')
    .with_columns(
        pl.when(pl.col('team_winner')).then(pl.lit('NY'))
          .otherwise(pl.lit('MIN')).alias('winner')
    )
)
print('NY series record vs MIN:',
      head_to_head['team_winner'].sum(), '-',
      head_to_head.height - head_to_head['team_winner'].sum())
head_to_head
```

    NY series record vs MIN: 4 - 5





    shape: (9, 5)
    ┌────────────┬────────────┬─────────────────────┬─────────────┬────────┐
    │ game_date  ┆ team_score ┆ opponent_team_score ┆ team_winner ┆ winner │
    │ ---        ┆ ---        ┆ ---                 ┆ ---         ┆ ---    │
    │ date       ┆ i32        ┆ i32                 ┆ bool        ┆ str    │
    ╞════════════╪════════════╪═════════════════════╪═════════════╪════════╡
    │ 2024-05-25 ┆ 67         ┆ 84                  ┆ false       ┆ MIN    │
    │ 2024-06-25 ┆ 89         ┆ 94                  ┆ false       ┆ MIN    │
    │ 2024-07-02 ┆ 76         ┆ 67                  ┆ true        ┆ NY     │
    │ 2024-09-15 ┆ 79         ┆ 88                  ┆ false       ┆ MIN    │
    │ 2024-10-10 ┆ 93         ┆ 95                  ┆ false       ┆ MIN    │
    │ 2024-10-13 ┆ 80         ┆ 66                  ┆ true        ┆ NY     │
    │ 2024-10-16 ┆ 80         ┆ 77                  ┆ true        ┆ NY     │
    │ 2024-10-18 ┆ 80         ┆ 82                  ┆ false       ┆ MIN    │
    │ 2024-10-20 ┆ 67         ┆ 62                  ┆ true        ┆ NY     │
    └────────────┴────────────┴─────────────────────┴─────────────┴────────┘



### Recipe 10 — Rolling form: hot and cold streaks 🔥

A team's last-5 record tells you who's surging into the playoffs. Sort one team's games by date, then a `rolling_sum` over the win flag gives a running 5-game window — polars makes the time-series slice a one-liner.


```python
form = (
    team_box
    .filter(pl.col('team_abbreviation') == 'NY')
    .sort('game_date')
    .with_columns(pl.col('team_winner').cast(pl.Int8).alias('won'))
    .with_columns(
        pl.col('won').rolling_sum(window_size=5).alias('wins_last5')
    )
    .select(['game_date', 'opponent_team_abbreviation', 'team_score',
             'opponent_team_score', 'won', 'wins_last5'])
    .tail(12)
)
form
```




    shape: (12, 6)
    ┌────────────┬────────────────────────────┬────────────┬─────────────────────┬─────┬────────────┐
    │ game_date  ┆ opponent_team_abbreviation ┆ team_score ┆ opponent_team_score ┆ won ┆ wins_last5 │
    │ ---        ┆ ---                        ┆ ---        ┆ ---                 ┆ --- ┆ ---        │
    │ date       ┆ str                        ┆ i32        ┆ i32                 ┆ i8  ┆ i64        │
    ╞════════════╪════════════════════════════╪════════════╪═════════════════════╪═════╪════════════╡
    │ 2024-09-19 ┆ ATL                        ┆ 67         ┆ 78                  ┆ 0   ┆ 3          │
    │ 2024-09-22 ┆ ATL                        ┆ 83         ┆ 69                  ┆ 1   ┆ 3          │
    │ 2024-09-24 ┆ ATL                        ┆ 91         ┆ 82                  ┆ 1   ┆ 3          │
    │ 2024-09-29 ┆ LV                         ┆ 87         ┆ 77                  ┆ 1   ┆ 4          │
    │ 2024-10-01 ┆ LV                         ┆ 88         ┆ 84                  ┆ 1   ┆ 4          │
    │ …          ┆ …                          ┆ …          ┆ …                   ┆ …   ┆ …          │
    │ 2024-10-10 ┆ MIN                        ┆ 93         ┆ 95                  ┆ 0   ┆ 3          │
    │ 2024-10-13 ┆ MIN                        ┆ 80         ┆ 66                  ┆ 1   ┆ 3          │
    │ 2024-10-16 ┆ MIN                        ┆ 80         ┆ 77                  ┆ 1   ┆ 3          │
    │ 2024-10-18 ┆ MIN                        ┆ 80         ┆ 82                  ┆ 0   ┆ 3          │
    │ 2024-10-20 ┆ MIN                        ┆ 67         ┆ 62                  ┆ 1   ┆ 3          │
    └────────────┴────────────────────────────┴────────────┴─────────────────────┴─────┴────────────┘



### Recipe 11 — Roster construction by position 👥

[`load_wnba_rosters`](../wnba/reference/loaders.md#load_wnba_rosters) hands you every team's full roster. Pivot guards / forwards / centers per team to see how each front office balances its lineup — a clean join-free `pivot`.


```python
rosters = wnba.load_wnba_rosters(seasons=[SEASON])
position_mix = (
    rosters
    .group_by(['team_abbreviation', 'position_abbreviation'])
    .agg(pl.len().alias('n'))
    .pivot(values='n', index='team_abbreviation', on='position_abbreviation')
    .fill_null(0)
    .sort('team_abbreviation')
)
position_mix
```




    shape: (12, 4)
    ┌───────────────────┬─────┬─────┬─────┐
    │ team_abbreviation ┆ F   ┆ G   ┆ C   │
    │ ---               ┆ --- ┆ --- ┆ --- │
    │ str               ┆ u32 ┆ u32 ┆ u32 │
    ╞═══════════════════╪═════╪═════╪═════╡
    │ ATL               ┆ 4   ┆ 7   ┆ 1   │
    │ CHI               ┆ 3   ┆ 9   ┆ 2   │
    │ CONNECTICU        ┆ 5   ┆ 8   ┆ 2   │
    │ DALLAS            ┆ 6   ┆ 7   ┆ 1   │
    │ IND               ┆ 3   ┆ 8   ┆ 2   │
    │ …                 ┆ …   ┆ …   ┆ …   │
    │ MIN               ┆ 8   ┆ 5   ┆ 1   │
    │ NY                ┆ 5   ┆ 8   ┆ 2   │
    │ PHX               ┆ 7   ┆ 7   ┆ 1   │
    │ SEA               ┆ 5   ┆ 6   ┆ 3   │
    │ WSH               ┆ 3   ┆ 9   ┆ 2   │
    └───────────────────┴─────┴─────┴─────┘



### Recipe 12 — Season scoring leaders, the pre-aggregated way 📐

Don't want to roll up box scores yourself? [`load_wnba_player_season_stats`](../wnba/reference/loaders.md#load_wnba_player_season_stats) ships ESPN's own season aggregates in **long** format (`category` / `stat_name` / `value`). Filter to the `averages` category and the `avgPoints` stat for an instant scoring leaderboard — a great cross-check against Recipe 3.


```python
season_stats = wnba.load_wnba_player_season_stats(seasons=[SEASON])
scoring_leaders = (
    season_stats
    .filter(
        (pl.col('category') == 'averages')
        & (pl.col('stat_name') == 'avgPoints')
    )
    .select(['athlete_display_name', 'team_display_name',
             'athlete_position_abbreviation', 'value'])
    .rename({'value': 'ppg'})
    .sort('ppg', descending=True)
    .head(10)
)
scoring_leaders
```




    shape: (10, 4)
    ┌──────────────────────┬───────────────────┬───────────────────────────────┬──────┐
    │ athlete_display_name ┆ team_display_name ┆ athlete_position_abbreviation ┆ ppg  │
    │ ---                  ┆ ---               ┆ ---                           ┆ ---  │
    │ str                  ┆ str               ┆ str                           ┆ f64  │
    ╞══════════════════════╪═══════════════════╪═══════════════════════════════╪══════╡
    │ A'ja Wilson          ┆ Las Vegas Aces    ┆ C                             ┆ 21.4 │
    │ Olivia Miles         ┆ Minnesota Lynx    ┆ G                             ┆ 21.0 │
    │ Breanna Stewart      ┆ Seattle Storm     ┆ F                             ┆ 20.5 │
    │ Arike Ogunbowale     ┆ Dallas Wings      ┆ G                             ┆ 19.9 │
    │ Paige Bueckers       ┆ Dallas Wings      ┆ G                             ┆ 19.2 │
    │ Caitlin Clark        ┆ Indiana Fever     ┆ G                             ┆ 18.6 │
    │ Napheesa Collier     ┆ Minnesota Lynx    ┆ F                             ┆ 18.4 │
    │ Jovana Nogic         ┆ Phoenix Mercury   ┆ G                             ┆ 17.5 │
    │ Kelsey Mitchell      ┆ Indiana Fever     ┆ G                             ┆ 17.4 │
    │ Rhyne Howard         ┆ Atlanta Dream     ┆ G                             ┆ 17.1 │
    └──────────────────────┴───────────────────┴───────────────────────────────┴──────┘



## 📦 Bulk loaders (`load_wnba_*`)

The `load_wnba_*` family reads pre-built **parquet releases** (whole seasons at once) instead of calling the live API per game — perfect for season-long analysis. They return polars by default (`return_as_pandas=True` for pandas). A few favourites:

| Loader | Whole-season… |
|---|---|
| [`load_wnba_schedule`](../wnba/reference/loaders.md#load_wnba_schedule) | schedule + results |
| [`load_wnba_player_boxscore`](../wnba/reference/loaders.md#load_wnba_player_boxscore) | player box scores |
| [`load_wnba_team_boxscore`](../wnba/reference/loaders.md#load_wnba_team_boxscore) | team box scores |
| [`load_wnba_player_season_stats`](../wnba/reference/loaders.md#load_wnba_player_season_stats) | season-aggregated player stats |
| [`load_wnba_pbp`](../wnba/reference/loaders.md#load_wnba_pbp) | play-by-play |
| [`load_wnba_shots`](../wnba/reference/loaders.md#load_wnba_shots) | shot locations |

Pass a list of seasons to combine several years in one frame.


```python
sched_2024 = wnba.load_wnba_schedule(seasons=[SEASON])
print('schedule rows:', sched_2024.shape)
box_2024 = wnba.load_wnba_player_boxscore(seasons=[SEASON])
box_2024.select(['game_id', 'game_date', 'athlete_display_name',
                 'team_abbreviation', 'minutes', 'points',
                 'rebounds', 'assists']).head()
```

    schedule rows: (264, 77)





    shape: (5, 8)
    ┌───────────┬────────────┬────────────────┬────────────────┬─────────┬────────┬──────────┬─────────┐
    │ game_id   ┆ game_date  ┆ athlete_displa ┆ team_abbreviat ┆ minutes ┆ points ┆ rebounds ┆ assists │
    │ ---       ┆ ---        ┆ y_name         ┆ ion            ┆ ---     ┆ ---    ┆ ---      ┆ ---     │
    │ i32       ┆ date       ┆ ---            ┆ ---            ┆ f64     ┆ i32    ┆ i32      ┆ i32     │
    │           ┆            ┆ str            ┆ str            ┆         ┆        ┆          ┆         │
    ╞═══════════╪════════════╪════════════════╪════════════════╪═════════╪════════╪══════════╪═════════╡
    │ 401726992 ┆ 2024-10-20 ┆ Bridget        ┆ MIN            ┆ 41.0    ┆ 3      ┆ 6        ┆ 2       │
    │           ┆            ┆ Carleton       ┆                ┆         ┆        ┆          ┆         │
    │ 401726992 ┆ 2024-10-20 ┆ Alanna Smith   ┆ MIN            ┆ 36.0    ┆ 6      ┆ 8        ┆ 2       │
    │ 401726992 ┆ 2024-10-20 ┆ Napheesa       ┆ MIN            ┆ 44.0    ┆ 22     ┆ 7        ┆ 2       │
    │           ┆            ┆ Collier        ┆                ┆         ┆        ┆          ┆         │
    │ 401726992 ┆ 2024-10-20 ┆ Kayla McBride  ┆ MIN            ┆ 43.0    ┆ 21     ┆ 5        ┆ 5       │
    │ 401726992 ┆ 2024-10-20 ┆ Courtney       ┆ MIN            ┆ 30.0    ┆ 4      ┆ 4        ┆ 3       │
    │           ┆            ┆ Williams       ┆                ┆         ┆        ┆          ┆         │
    └───────────┴────────────┴────────────────┴────────────────┴─────────┴────────┴──────────┴─────────┘



## 🎉 Where to next

- Pass `return_as_pandas=True` for a pandas frame, or `raw=True` (where supported) for the untouched ESPN JSON.
- Full reference: the **WNBA** section in the sidebar — [ESPN extras](../wnba/reference/additional.md), [site API](../wnba/reference/site.md), [core API](../wnba/reference/core.md) and [loaders](../wnba/reference/loaders.md).
- R user? The same surface lives in [wehoop](https://wehoop.sportsdataverse.org).
- Want a deeper stats API? [nba_api](https://github.com/swar/nba_api) also covers the WNBA.

Now go chart some buckets! 🏀
