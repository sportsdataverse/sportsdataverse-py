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

## 👥 Team roster — Las Vegas Aces

[`espn_wnba_team_roster`](../wnba/reference/site.md#espn_wnba_team_roster) lists active players for one team in a season. The back-to-back champion Aces are `team_id=17`. Player columns are unprefixed (`athlete_id`, `full_name`, `jersey`, `position_abbreviation`).


```python
aces = safe('Aces roster', lambda: wnba.espn_wnba_team_roster(team_id=17, season=SEASON))
(aces.select(['athlete_id', 'full_name', 'jersey',
              'position_abbreviation', 'display_height', 'age']).head(12)
 if aces is not None else 'roster unavailable')
```

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

Filter to scoring plays only to watch the lead change down the stretch.


```python
(plays
    .filter(pl.col('scoringPlay'))
    .select(['period.number', 'clock.displayValue', 'homeScore', 'awayScore', 'text'])
    .tail(8)
 if plays is not None else 'pbp unavailable')
```

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

## 📊 Team season stats

[`espn_wnba_team_stats`](../wnba/reference/additional.md#espn_wnba_team_stats) returns a **dict** keyed by category — `{'Averages', 'Totals', 'Misc'}`. Each value is a long frame of `stat_name` / `display_value` rows, so index into the dict rather than calling `.head()` on the return directly.


```python
aces_stats = safe('Aces team stats',
                  lambda: wnba.espn_wnba_team_stats(team_id=17, season=SEASON))
print('categories:', list(aces_stats.keys()) if aces_stats is not None else None)
(aces_stats['Averages'].select(['stat_name', 'abbreviation', 'display_value']).head(10)
 if aces_stats is not None else 'team stats unavailable')
```

## 🍳 Cookbook: common WNBA tasks

Now for the fun part. These four recipes are the everyday tasks you'll reach for constantly — each blends a premium ESPN call (or a parquet loader) with a few polars expressions. They're all correct, runnable Python. 🧑‍🍳

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

### Recipe 2 — Draft board 🎓

[`espn_wnba_draft`](../wnba/reference/site.md#espn_wnba_draft) lists every pick for a season. The 2024 draft headlined with Caitlin Clark going first overall to the Indiana Fever.


```python
draft = safe('2024 draft', lambda: wnba.espn_wnba_draft(season=SEASON))
(draft.select(['overall_pick', 'team_display_name', 'athlete_display_name',
               'athlete_position_abbreviation', 'school_name']).head(10)
 if draft is not None else 'draft unavailable')
```

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

## 🎉 Where to next

- Pass `return_as_pandas=True` for a pandas frame, or `raw=True` (where supported) for the untouched ESPN JSON.
- Full reference: the **WNBA** section in the sidebar — [ESPN extras](../wnba/reference/additional.md), [site API](../wnba/reference/site.md), [core API](../wnba/reference/core.md) and [loaders](../wnba/reference/loaders.md).
- R user? The same surface lives in [wehoop](https://wehoop.sportsdataverse.org).
- Want a deeper stats API? [nba_api](https://github.com/swar/nba_api) also covers the WNBA.

Now go chart some buckets! 🏀
