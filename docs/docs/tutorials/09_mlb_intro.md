---
title: MLB tutorial
sidebar_label: MLB
sidebar_position: 8
---

# ⚾ Baseball with `sportsdataverse-py`

Welcome to the ballpark! 🏟️ In just a few lines of Python you're about to
pull **official MLB data** — schedules, standings, rosters, box scores,
play-by-play — straight from the league's own **MLB Stats API**, plus
**pitch-level Statcast** tracking from [Baseball Savant](https://baseballsavant.mlb.com/).
Every premium call hands you back a tidy **polars** DataFrame (or raw JSON
when you want it), ready to model. 🚀

If you've used the R package [baseballr](https://billpetti.github.io/baseballr/),
or Python's [pybaseball](https://github.com/jldbc/pybaseball), the data shapes
will feel right at home. Let's play ball! ⚾

## 🧰 The toolbox

We **lead with the premium sources** — the **MLB Stats API** (`mlb_*`,
backed by `statsapi.mlb.com`) and the **comprehensive Statcast** surface
(`mlb_statcast_*`, from Baseball
Savant). ESPN (`espn_mlb_*`) is a handy secondary path. Click any name for the
full reference:

| Function | What it gives you | Source |
|---|---|---|
| [`mlb_schedule`](../mlb/reference/additional.md#mlb_schedule) · [`parse_mlb_api_schedule`](../mlb/reference/additional.md#mlb_schedule) | Games for a date / range — one row per game (with `game_pk`) | 🟢 **MLB Stats API** |
| [`mlb_teams`](../mlb/reference/additional.md#mlb_teams) · [`parse_mlb_api_teams`](../mlb/reference/additional.md#mlb_teams) | Every club — one row per team | 🟢 **MLB Stats API** |
| [`mlb_standings`](../mlb/reference/additional.md#mlb_standings) · [`parse_mlb_api_standings`](../mlb/reference/additional.md#mlb_standings) | Division standings — wins, losses, run diff | 🟢 **MLB Stats API** |
| [`mlb_team_roster`](../mlb/reference/mlb_api.md#mlb_team_roster) | A team's roster — one row per player | 🟢 **MLB Stats API** |
| [`mlb_person`](../mlb/reference/mlb_api.md#mlb_person) | A player's bio (one tidy row) | 🟢 **MLB Stats API** |
| [`mlb_person_stats`](../mlb/reference/additional.md#mlb_person_stats) · [`parse_mlb_api_person_stats`](../mlb/reference/additional.md#mlb_person_stats) | A player's season stat splits | 🟢 **MLB Stats API** |
| [`mlb_boxscore`](../mlb/reference/mlb_api.md#mlb_boxscore) | Full game box score | 🟢 **MLB Stats API** |
| [`mlb_play_by_play`](../mlb/reference/mlb_api.md#mlb_play_by_play) | Plate-appearance-level play-by-play | 🟢 **MLB Stats API** |
| [`mlb_stats_leaders`](../mlb/reference/additional.md#mlb_stats_leaders) | League leaders for any stat (HR, AVG, ERA, …) | 🟢 **MLB Stats API** |
| [`mlb_win_probability`](../mlb/reference/mlb_api.md#mlb_win_probability) | Per-play win probability + WPA for a game | 🟢 **MLB Stats API** |
| [`mlb_awards`](../mlb/reference/mlb_api.md#mlb_awards) · [`mlb_award_recipients`](../mlb/reference/mlb_api.md#mlb_award_recipients) | Award catalog + season winners (MVP, Cy Young, …) | 🟢 **MLB Stats API** |
| [`mlb_draft`](../mlb/reference/mlb_api.md#mlb_draft) | Amateur draft board — one row per pick | 🟢 **MLB Stats API** |
| [`mlb_statcast_search`](../mlb/reference/additional.md#mlb_statcast_search) | Every pitch matching a filter — ~110 cols/pitch; auto date-chunks past the 25k cap; friendly filters (`batters_lookup`, `pitch_type`, `at_bat_result`, …) | 🔵 **Statcast** |
| [`mlb_statcast_search_minors`](../mlb/reference/additional.md#mlb_statcast_search_minors) · [`mlb_statcast_search_wbc`](../mlb/reference/additional.md#mlb_statcast_search_wbc) | Same pitch search for MiLB and the World Baseball Classic | 🔵 **Statcast** |
| `mlb_statcast_leaderboard_*` (37 of them) — e.g. [`…_sprint_speed`](../mlb/reference/mlb_statcast.md#mlb_statcast_leaderboard_sprint_speed), [`…_expected_stats`](../mlb/reference/mlb_statcast.md#mlb_statcast_leaderboard_expected_stats), [`…_bat_tracking`](../mlb/reference/mlb_statcast.md#mlb_statcast_leaderboard_bat_tracking), [`…_outs_above_average`](../mlb/reference/mlb_statcast.md#mlb_statcast_leaderboard_outs_above_average) | Every Savant leaderboard: expected stats, sprint speed, bat tracking, pitch arsenals/movement/tempo, OAA, arm strength, catcher framing/blocking/throwing, baserunning, park factors, … | 🔵 **Statcast** |
| [`mlb_statcast_gamefeed`](../mlb/reference/mlb_statcast.md#mlb_statcast_gamefeed) | Savant single-game feed — one tidy row per pitch | 🔵 **Statcast** |
| [`mlb_statcast_player`](../mlb/reference/additional.md#mlb_statcast_player) | A player's Savant page metrics | 🔵 **Statcast** |
| [`espn_mlb_teams`](../mlb/reference/additional.md#espn_mlb_teams) · [`espn_mlb_schedule`](../mlb/reference/additional.md#espn_mlb_schedule) | ESPN teams / schedule (wide frames) | ⚪ ESPN |
| [`most_recent_mlb_season`](../mlb/reference/additional.md#most_recent_mlb_season) | Current season helper | ⚪ helper |

## 🔌 Setup

```sh
pip install sportsdataverse
```

**No API key needed** for any of the premium MLB endpoints — the MLB Stats API
and Baseball Savant are both public. 🎉


```python
import polars as pl
import sportsdataverse.mlb as mlb

pl.Config.set_tbl_rows(12)
print("most recent MLB season:", mlb.most_recent_mlb_season())
```

    most recent MLB season: 2026


The MLB Stats API and Savant are public and reliable, but they're still
**live network calls** — a date with no games, an offseason day, or a blip can
make a call come back empty. So we use a tiny `safe()` helper: you get the
frame when the feed is up, and a friendly one-liner when it isn't (never a
scary traceback). 🛟

We also pick a stable **completed-season** date for our examples so the page
renders the same in June as in October.


```python
def safe(label, thunk):
    """Run a live call defensively: return its result, or print a one-liner."""
    try:
        out = thunk()
        print(f"✅ {label}")
        return out
    except Exception as e:  # noqa: BLE001 -- demo resilience
        print(f"⏭️  {label}: unavailable right now ({type(e).__name__})")
        return None

# A known completed regular-season slate — stable for the docs build.
SAMPLE_SEASON = 2024
SAMPLE_DATE = "2024-07-01"  # YYYY-MM-DD for the Stats API
JUDGE_ID = 592450           # Aaron Judge, NYY — our running example player
YANKEES_ID = 147            # New York Yankees team_id
```

## 📅 The schedule (MLB Stats API)

[`mlb_schedule`](../mlb/reference/additional.md#mlb_schedule) returns the
raw JSON `dict`; its partner
[`parse_mlb_api_schedule`](../mlb/reference/additional.md#mlb_schedule)
flattens it to **one row per game**. The most important column is `game_pk` —
that's the id you feed to the box score and play-by-play endpoints. Pass a
single `date=`, or a `start_date`/`end_date` range, `team_id`, or `season`.


```python
schedule = safe(
    "schedule",
    lambda: mlb.parse_mlb_api_schedule(mlb.mlb_schedule(date=SAMPLE_DATE)),
)
cols = ["game_pk", "status_detailed_state",
        "teams_away_team_name", "teams_away_score",
        "teams_home_team_name", "teams_home_score"]
(schedule.select([c for c in cols if c in schedule.columns]).head()
 if schedule is not None else "schedule unavailable right now")
```

    ✅ schedule





    shape: (3, 6)
    ┌─────────┬─────────────────┬─────────────────┬─────────────────┬─────────────────┬────────────────┐
    │ game_pk ┆ status_detailed ┆ teams_away_team ┆ teams_away_scor ┆ teams_home_team ┆ teams_home_sco │
    │ ---     ┆ _state          ┆ _name           ┆ e               ┆ _name           ┆ re             │
    │ i64     ┆ ---             ┆ ---             ┆ ---             ┆ ---             ┆ ---            │
    │         ┆ str             ┆ str             ┆ i64             ┆ str             ┆ i64            │
    ╞═════════╪═════════════════╪═════════════════╪═════════════════╪═════════════════╪════════════════╡
    │ 744914  ┆ Final           ┆ Houston Astros  ┆ 3               ┆ Toronto Blue    ┆ 1              │
    │         ┆                 ┆                 ┆                 ┆ Jays            ┆                │
    │ 744840  ┆ Final           ┆ New York Mets   ┆ 9               ┆ Washington      ┆ 7              │
    │         ┆                 ┆                 ┆                 ┆ Nationals       ┆                │
    │ 746535  ┆ Final           ┆ Milwaukee       ┆ 7               ┆ Colorado        ┆ 8              │
    │         ┆                 ┆ Brewers         ┆                 ┆ Rockies         ┆                │
    └─────────┴─────────────────┴─────────────────┴─────────────────┴─────────────────┴────────────────┘



## 🏆 Standings (MLB Stats API)

[`mlb_standings`](../mlb/reference/additional.md#mlb_standings) covers
both leagues by default (`league_id="103,104"`).
[`parse_mlb_api_standings`](../mlb/reference/additional.md#mlb_standings)
returns one row per team with wins/losses, division rank, and winning
percentage.


```python
standings = safe(
    "standings",
    lambda: mlb.parse_mlb_api_standings(mlb.mlb_standings(season=SAMPLE_SEASON)),
)
keep = ["team_name", "standings_division_name", "wins", "losses",
        "winning_percentage", "division_rank"]
(standings.select([c for c in keep if c in standings.columns])
         .sort("wins", descending=True).head(10)
 if standings is not None else "standings unavailable right now")
```

    ✅ standings





    shape: (10, 6)
    ┌───────────┬─────────────────────────┬──────┬────────┬────────────────────┬───────────────┐
    │ team_name ┆ standings_division_name ┆ wins ┆ losses ┆ winning_percentage ┆ division_rank │
    │ ---       ┆ ---                     ┆ ---  ┆ ---    ┆ ---                ┆ ---           │
    │ str       ┆ str                     ┆ i64  ┆ i64    ┆ str                ┆ str           │
    ╞═══════════╪═════════════════════════╪══════╪════════╪════════════════════╪═══════════════╡
    │ Dodgers   ┆ null                    ┆ 98   ┆ 64     ┆ .605               ┆ 1             │
    │ Phillies  ┆ null                    ┆ 95   ┆ 67     ┆ .586               ┆ 1             │
    │ Yankees   ┆ null                    ┆ 94   ┆ 68     ┆ .580               ┆ 1             │
    │ Brewers   ┆ null                    ┆ 93   ┆ 69     ┆ .574               ┆ 1             │
    │ Padres    ┆ null                    ┆ 93   ┆ 69     ┆ .574               ┆ 2             │
    │ Guardians ┆ null                    ┆ 92   ┆ 69     ┆ .571               ┆ 1             │
    │ Orioles   ┆ null                    ┆ 91   ┆ 71     ┆ .562               ┆ 2             │
    │ Braves    ┆ null                    ┆ 89   ┆ 73     ┆ .549               ┆ 2             │
    │ Mets      ┆ null                    ┆ 89   ┆ 73     ┆ .549               ┆ 3             │
    │ D-backs   ┆ null                    ┆ 89   ┆ 73     ┆ .549               ┆ 3             │
    └───────────┴─────────────────────────┴──────┴────────┴────────────────────┴───────────────┘



## 🧢 Teams & rosters (MLB Stats API)

[`mlb_teams`](../mlb/reference/additional.md#mlb_teams) +
[`parse_mlb_api_teams`](../mlb/reference/additional.md#mlb_teams) lists every
club — grab a `team_id` here.
[`mlb_team_roster`](../mlb/reference/mlb_api.md#mlb_team_roster) then
returns a tidy frame directly (one row per player).


```python
teams = safe(
    "teams",
    lambda: mlb.parse_mlb_api_teams(mlb.mlb_teams(season=SAMPLE_SEASON)),
)
(teams.select(["id", "name", "abbreviation", "location_name", "team_name"]).head()
 if teams is not None else "teams unavailable right now")
```

    ✅ teams





    shape: (5, 5)
    ┌─────┬──────────────────────┬──────────────┬───────────────┬───────────┐
    │ id  ┆ name                 ┆ abbreviation ┆ location_name ┆ team_name │
    │ --- ┆ ---                  ┆ ---          ┆ ---           ┆ ---       │
    │ i64 ┆ str                  ┆ str          ┆ str           ┆ str       │
    ╞═════╪══════════════════════╪══════════════╪═══════════════╪═══════════╡
    │ 133 ┆ Oakland Athletics    ┆ OAK          ┆ Oakland       ┆ Athletics │
    │ 134 ┆ Pittsburgh Pirates   ┆ PIT          ┆ Pittsburgh    ┆ Pirates   │
    │ 135 ┆ San Diego Padres     ┆ SD           ┆ San Diego     ┆ Padres    │
    │ 136 ┆ Seattle Mariners     ┆ SEA          ┆ Seattle       ┆ Mariners  │
    │ 137 ┆ San Francisco Giants ┆ SF           ┆ San Francisco ┆ Giants    │
    └─────┴──────────────────────┴──────────────┴───────────────┴───────────┘




```python
roster = safe(
    "Yankees roster",
    lambda: mlb.mlb_team_roster(team_id=YANKEES_ID, season=SAMPLE_SEASON),
)
rcols = ["jersey_number", "person_id", "person_full_name",
         "position_abbreviation", "status_description"]
(roster.select([c for c in rcols if c in roster.columns]).head()
 if roster is not None else "roster unavailable right now")
```

    ✅ Yankees roster





    shape: (5, 5)
    ┌───────────────┬───────────┬──────────────────┬───────────────────────┬───────────────────────┐
    │ jersey_number ┆ person_id ┆ person_full_name ┆ position_abbreviation ┆ status_description    │
    │ ---           ┆ ---       ┆ ---              ┆ ---                   ┆ ---                   │
    │ str           ┆ i64       ┆ str              ┆ str                   ┆ str                   │
    ╞═══════════════╪═══════════╪══════════════════╪═══════════════════════╪═══════════════════════╡
    │ 74            ┆ 677076    ┆ Clayton Andrews  ┆ P                     ┆ Minor League Contract │
    │ 85            ┆ 690925    ┆ Clayton Beeter   ┆ P                     ┆ Forty Man             │
    │ 19            ┆ 542932    ┆ Jon Berti        ┆ 3B                    ┆ Forty Man             │
    │ 53            ┆ 641360    ┆ Phil Bickford    ┆ P                     ┆ Minor League Contract │
    │ 57            ┆ 595897    ┆ Nick Burdi       ┆ P                     ┆ Minor League Contract │
    └───────────────┴───────────┴──────────────────┴───────────────────────┴───────────────────────┘



## 🧍 Player bio & season stats (MLB Stats API)

[`mlb_person`](../mlb/reference/mlb_api.md#mlb_person) returns a one-row
bio frame. [`mlb_person_stats`](../mlb/reference/additional.md#mlb_person_stats)
returns the raw stat-split `dict`;
[`parse_mlb_api_person_stats`](../mlb/reference/additional.md#mlb_person_stats)
flattens it. Our running example is Aaron Judge (`person_id=592450`).


```python
bio = safe("Judge bio", lambda: mlb.mlb_person(person_id=JUDGE_ID))
bcols = ["id", "full_name", "primary_number", "birth_date",
         "height", "weight", "mlb_debut_date"]
(bio.select([c for c in bcols if c in bio.columns])
 if bio is not None else "bio unavailable right now")
```

    ✅ Judge bio





    shape: (1, 7)
    ┌────────┬─────────────┬────────────────┬────────────┬────────┬────────┬────────────────┐
    │ id     ┆ full_name   ┆ primary_number ┆ birth_date ┆ height ┆ weight ┆ mlb_debut_date │
    │ ---    ┆ ---         ┆ ---            ┆ ---        ┆ ---    ┆ ---    ┆ ---            │
    │ i64    ┆ str         ┆ str            ┆ str        ┆ str    ┆ i64    ┆ str            │
    ╞════════╪═════════════╪════════════════╪════════════╪════════╪════════╪════════════════╡
    │ 592450 ┆ Aaron Judge ┆ 99             ┆ 1992-04-26 ┆ 6' 7"  ┆ 282    ┆ 2016-08-13     │
    └────────┴─────────────┴────────────────┴────────────┴────────┴────────┴────────────────┘




```python
hitting = safe(
    "Judge 2024 hitting",
    lambda: mlb.parse_mlb_api_person_stats(
        mlb.mlb_person_stats(person_id=JUDGE_ID, stats="season",
                                 group="hitting", season=SAMPLE_SEASON)
    ),
)
scols = ["season", "stat_games_played", "stat_home_runs", "stat_rbi",
         "stat_avg", "stat_obp", "stat_slg", "stat_ops"]
(hitting.select([c for c in scols if c in hitting.columns])
 if hitting is not None else "stats unavailable right now")
```

    ✅ Judge 2024 hitting





    shape: (1, 8)
    ┌────────┬─────────────────┬────────────────┬──────────┬──────────┬──────────┬──────────┬──────────┐
    │ season ┆ stat_games_play ┆ stat_home_runs ┆ stat_rbi ┆ stat_avg ┆ stat_obp ┆ stat_slg ┆ stat_ops │
    │ ---    ┆ ed              ┆ ---            ┆ ---      ┆ ---      ┆ ---      ┆ ---      ┆ ---      │
    │ str    ┆ ---             ┆ i64            ┆ i64      ┆ str      ┆ str      ┆ str      ┆ str      │
    │        ┆ i64             ┆                ┆          ┆          ┆          ┆          ┆          │
    ╞════════╪═════════════════╪════════════════╪══════════╪══════════╪══════════╪══════════╪══════════╡
    │ 2024   ┆ 158             ┆ 58             ┆ 144      ┆ .322     ┆ .458     ┆ .701     ┆ 1.159    │
    └────────┴─────────────────┴────────────────┴──────────┴──────────┴──────────┴──────────┴──────────┘



## 🎯 Pitch-level Statcast (Baseball Savant)

Now the fun part — **every single pitch**.
[`mlb_statcast_search`](../mlb/reference/additional.md#mlb_statcast_search) pulls each
pitch matching your filter, with 100+ columns (velocity, spin, launch angle,
expected stats). Keep windows **small** (one player, one game, or a 1–2 day
slice) — a full season is millions of pitches. Here's every pitch Aaron Judge
saw over a two-day window.


```python
pitches = safe(
    "Judge pitches (2-day)",
    lambda: mlb.mlb_statcast_search(start_dt="2024-07-01", end_dt="2024-07-02",
                                batters_lookup=JUDGE_ID),
)
if pitches is not None and pitches.height:
    print("shape:", pitches.shape)
    out = pitches.select(["game_date", "player_name", "pitch_type", "release_speed",
                          "launch_speed", "launch_angle", "events", "description"]).head()
else:
    out = "no pitches in that window right now"
out
```

    ✅ Judge pitches (2-day)
    shape: (11, 119)





    shape: (5, 8)
    ┌────────────┬────────────┬────────────┬───────────┬───────────┬───────────┬───────────┬───────────┐
    │ game_date  ┆ player_nam ┆ pitch_type ┆ release_s ┆ launch_sp ┆ launch_an ┆ events    ┆ descripti │
    │ ---        ┆ e          ┆ ---        ┆ peed      ┆ eed       ┆ gle       ┆ ---       ┆ on        │
    │ str        ┆ ---        ┆ str        ┆ ---       ┆ ---       ┆ ---       ┆ str       ┆ ---       │
    │            ┆ str        ┆            ┆ f64       ┆ f64       ┆ f64       ┆           ┆ str       │
    ╞════════════╪════════════╪════════════╪═══════════╪═══════════╪═══════════╪═══════════╪═══════════╡
    │ 2024-07-02 ┆ Judge,     ┆ FC         ┆ 97.1      ┆ 91.0      ┆ 19.0      ┆ single    ┆ hit_into_ │
    │            ┆ Aaron      ┆            ┆           ┆           ┆           ┆           ┆ play      │
    │ 2024-07-02 ┆ Judge,     ┆ FC         ┆ 97.2      ┆ null      ┆ null      ┆ null      ┆ swinging_ │
    │            ┆ Aaron      ┆            ┆           ┆           ┆           ┆           ┆ strike    │
    │ 2024-07-02 ┆ Judge,     ┆ SL         ┆ 87.5      ┆ 94.3      ┆ 42.0      ┆ field_out ┆ hit_into_ │
    │            ┆ Aaron      ┆            ┆           ┆           ┆           ┆           ┆ play      │
    │ 2024-07-02 ┆ Judge,     ┆ FC         ┆ 96.3      ┆ null      ┆ null      ┆ null      ┆ ball      │
    │            ┆ Aaron      ┆            ┆           ┆           ┆           ┆           ┆           │
    │ 2024-07-02 ┆ Judge,     ┆ SL         ┆ 90.1      ┆ null      ┆ null      ┆ null      ┆ foul      │
    │            ┆ Aaron      ┆            ┆           ┆           ┆           ┆           ┆           │
    └────────────┴────────────┴────────────┴───────────┴───────────┴───────────┴───────────┴───────────┘



## 🍳 Cookbook: common baseball tasks

A handful of recipes you'll reach for constantly — every one leads with a
**premium** source.

### Recipe 1 — A team's schedule + where they sit in the standings 📋

Pull one club's slate with `mlb_schedule(team_id=...)`, then find their row
in the standings. Two premium calls, one tidy snapshot.


```python
yanks_sched = safe(
    "Yankees July schedule",
    lambda: mlb.parse_mlb_api_schedule(
        mlb.mlb_schedule(team_id=YANKEES_ID,
                             start_date="2024-07-01", end_date="2024-07-07")
    ),
)
sched_cols = ["game_pk", "official_date", "teams_away_team_name",
              "teams_home_team_name", "teams_away_score", "teams_home_score"]
if yanks_sched is not None and yanks_sched.height:
    games = yanks_sched.select([c for c in sched_cols if c in yanks_sched.columns])
else:
    games = "schedule unavailable right now"

if standings is not None and "team_name" in standings.columns:
    rank = (standings.filter(pl.col("team_name").str.contains("Yankees"))
                     .select([c for c in ["team_name", "wins", "losses", "division_rank"]
                              if c in standings.columns]))
else:
    rank = "standings unavailable"
print(rank)
games
```

    ✅ Yankees July schedule
    shape: (1, 4)
    ┌───────────┬──────┬────────┬───────────────┐
    │ team_name ┆ wins ┆ losses ┆ division_rank │
    │ ---       ┆ ---  ┆ ---    ┆ ---           │
    │ str       ┆ i64  ┆ i64    ┆ str           │
    ╞═══════════╪══════╪════════╪═══════════════╡
    │ Yankees   ┆ 94   ┆ 68     ┆ 1             │
    └───────────┴──────┴────────┴───────────────┘





    shape: (6, 6)
    ┌─────────┬───────────────┬──────────────────┬─────────────────┬─────────────────┬─────────────────┐
    │ game_pk ┆ official_date ┆ teams_away_team_ ┆ teams_home_team ┆ teams_away_scor ┆ teams_home_scor │
    │ ---     ┆ ---           ┆ name             ┆ _name           ┆ e               ┆ e               │
    │ i64     ┆ str           ┆ ---              ┆ ---             ┆ ---             ┆ ---             │
    │         ┆               ┆ str              ┆ str             ┆ i64             ┆ i64             │
    ╞═════════╪═══════════════╪══════════════════╪═════════════════╪═════════════════╪═════════════════╡
    │ 745730  ┆ 2024-07-02    ┆ Cincinnati Reds  ┆ New York        ┆ 5               ┆ 4               │
    │         ┆               ┆                  ┆ Yankees         ┆                 ┆                 │
    │ 745728  ┆ 2024-07-03    ┆ Cincinnati Reds  ┆ New York        ┆ 3               ┆ 2               │
    │         ┆               ┆                  ┆ Yankees         ┆                 ┆                 │
    │ 745726  ┆ 2024-07-04    ┆ Cincinnati Reds  ┆ New York        ┆ 8               ┆ 4               │
    │         ┆               ┆                  ┆ Yankees         ┆                 ┆                 │
    │ 745725  ┆ 2024-07-05    ┆ Boston Red Sox   ┆ New York        ┆ 5               ┆ 3               │
    │         ┆               ┆                  ┆ Yankees         ┆                 ┆                 │
    │ 745724  ┆ 2024-07-06    ┆ Boston Red Sox   ┆ New York        ┆ 4               ┆ 14              │
    │         ┆               ┆                  ┆ Yankees         ┆                 ┆                 │
    │ 745723  ┆ 2024-07-07    ┆ Boston Red Sox   ┆ New York        ┆ 3               ┆ 0               │
    │         ┆               ┆                  ┆ Yankees         ┆                 ┆                 │
    └─────────┴───────────────┴──────────────────┴─────────────────┴─────────────────┴─────────────────┘



### Recipe 2 — A Statcast leaderboard 🏃

The `mlb_statcast_leaderboard_*` family wraps Savant's *pre-aggregated* season
leaderboards — fast, because the heavy lifting happens server-side. Here's the
2024 **sprint speed** leaderboard, fastest first.


```python
sprint = safe(
    "sprint speed leaderboard",
    lambda: mlb.mlb_statcast_leaderboard_sprint_speed(year=SAMPLE_SEASON, min_opp=10),
)
spcols = ["last_name, first_name", "team", "position", "competitive_runs", "sprint_speed"]
(sprint.select([c for c in spcols if c in sprint.columns])
       .sort("sprint_speed", descending=True).head(10)
 if sprint is not None and sprint.height else "leaderboard unavailable right now")
```

    ✅ sprint speed leaderboard





    shape: (10, 5)
    ┌───────────────────────┬──────┬──────────┬──────────────────┬──────────────┐
    │ last_name, first_name ┆ team ┆ position ┆ competitive_runs ┆ sprint_speed │
    │ ---                   ┆ ---  ┆ ---      ┆ ---              ┆ ---          │
    │ str                   ┆ str  ┆ str      ┆ i64              ┆ f64          │
    ╞═══════════════════════╪══════╪══════════╪══════════════════╪══════════════╡
    │ Witt Jr., Bobby       ┆ KC   ┆ SS       ┆ 298              ┆ 30.5         │
    │ Rojas, Johan          ┆ PHI  ┆ CF       ┆ 176              ┆ 30.1         │
    │ De La Cruz, Elly      ┆ CIN  ┆ SS       ┆ 249              ┆ 30.0         │
    │ Fitzgerald, Tyler     ┆ SF   ┆ SS       ┆ 99               ┆ 30.0         │
    │ Clase, Jonatan        ┆ TOR  ┆ LF       ┆ 20               ┆ 30.0         │
    │ Crow-Armstrong, Pete  ┆ CHC  ┆ CF       ┆ 149              ┆ 30.0         │
    │ Scott II, Victor      ┆ STL  ┆ CF       ┆ 62               ┆ 30.0         │
    │ Mateo, Jorge          ┆ BAL  ┆ 2B       ┆ 77               ┆ 29.9         │
    │ Siri, Jose            ┆ TB   ┆ CF       ┆ 116              ┆ 29.9         │
    │ Hampson, Garrett      ┆ KC   ┆ CF       ┆ 89               ┆ 29.8         │
    └───────────────────────┴──────┴──────────┴──────────────────┴──────────────┘



### Recipe 3 — Box score for one game 📊

Take a `game_pk` from any schedule and pull the full box score with
[`mlb_boxscore`](../mlb/reference/mlb_api.md#mlb_boxscore). Asking for
`return_parsed=False` gives the raw `dict`, which carries per-team batting and
pitching lines under `teams.home` / `teams.away`.


```python
def team_line(game_pk):
    box = mlb.mlb_boxscore(game_pk=game_pk, return_parsed=False)
    rows = []
    for side in ("away", "home"):
        t = box["teams"][side]
        bat = t["teamStats"]["batting"]
        rows.append({"side": side, "team": t["team"]["name"],
                     "runs": bat["runs"], "hits": bat["hits"],
                     "home_runs": bat["homeRuns"], "rbi": bat["rbi"], "avg": bat["avg"]})
    return pl.DataFrame(rows)

# Use a game_pk from the schedule we pulled, or fall back to a known game.
gid = int(schedule["game_pk"][0]) if (schedule is not None and schedule.height) else 744914
box_df = safe(f"boxscore {gid}", lambda: team_line(gid))
out = box_df if box_df is not None else "boxscore unavailable right now"
out
```

    ✅ boxscore 744914





    shape: (2, 7)
    ┌──────┬───────────────────┬──────┬──────┬───────────┬─────┬──────┐
    │ side ┆ team              ┆ runs ┆ hits ┆ home_runs ┆ rbi ┆ avg  │
    │ ---  ┆ ---               ┆ ---  ┆ ---  ┆ ---       ┆ --- ┆ ---  │
    │ str  ┆ str               ┆ i64  ┆ i64  ┆ i64       ┆ i64 ┆ str  │
    ╞══════╪═══════════════════╪══════╪══════╪═══════════╪═════╪══════╡
    │ away ┆ Houston Astros    ┆ 3    ┆ 4    ┆ 2         ┆ 3   ┆ .264 │
    │ home ┆ Toronto Blue Jays ┆ 1    ┆ 4    ┆ 1         ┆ 1   ┆ .234 │
    └──────┴───────────────────┴──────┴──────┴───────────┴─────┴──────┘



### Recipe 4 — Plate-appearance play-by-play + outcome mix ⚾

[`mlb_play_by_play`](../mlb/reference/mlb_api.md#mlb_play_by_play)
returns a `dict` with an `allPlays` list — one entry per plate appearance.
Flatten it with `pl.json_normalize` (dot-notation columns), then tally the
plate-appearance outcomes.


```python
def pbp_frame(game_pk):
    raw = mlb.mlb_play_by_play(game_pk=game_pk, return_parsed=False)
    return pl.json_normalize(raw["allPlays"], separator=".", max_level=2)

plays = safe(f"play-by-play {gid}", lambda: pbp_frame(gid))
if plays is not None and plays.height:
    pcols = ["about.inning", "about.halfInning", "matchup.batter.fullName",
             "matchup.pitcher.fullName", "result.event"]
    out = plays.select([c for c in pcols if c in plays.columns]).head()
else:
    out = "play-by-play unavailable right now"
out
```

    ✅ play-by-play 744914





    shape: (5, 5)
    ┌──────────────┬──────────────────┬────────────────────────┬────────────────────────┬──────────────┐
    │ about.inning ┆ about.halfInning ┆ matchup.batter.fullNam ┆ matchup.pitcher.fullNa ┆ result.event │
    │ ---          ┆ ---              ┆ e                      ┆ me                     ┆ ---          │
    │ i64          ┆ str              ┆ ---                    ┆ ---                    ┆ str          │
    │              ┆                  ┆ str                    ┆ str                    ┆              │
    ╞══════════════╪══════════════════╪════════════════════════╪════════════════════════╪══════════════╡
    │ 1            ┆ top              ┆ Alex Bregman           ┆ Yariel Rodríguez       ┆ Flyout       │
    │ 1            ┆ top              ┆ Jake Meyers            ┆ Yariel Rodríguez       ┆ Strikeout    │
    │ 1            ┆ top              ┆ Yordan Alvarez         ┆ Yariel Rodríguez       ┆ Groundout    │
    │ 1            ┆ bottom           ┆ Bo Bichette            ┆ Hunter Brown           ┆ Groundout    │
    │ 1            ┆ bottom           ┆ Spencer Horwitz        ┆ Hunter Brown           ┆ Lineout      │
    └──────────────┴──────────────────┴────────────────────────┴────────────────────────┴──────────────┘




```python
# Outcome mix for the game — the shape of every plate appearance.
if plays is not None and plays.height and "result.event" in plays.columns:
    out = (plays.group_by("result.event")
                .agg(pl.len().alias("count"))
                .sort("count", descending=True).head(10))
else:
    out = "no play-by-play to summarize right now"
out
```




    shape: (10, 2)
    ┌──────────────┬───────┐
    │ result.event ┆ count │
    │ ---          ┆ ---   │
    │ str          ┆ u32   │
    ╞══════════════╪═══════╡
    │ Strikeout    ┆ 16    │
    │ Groundout    ┆ 14    │
    │ Pop Out      ┆ 10    │
    │ Flyout       ┆ 7     │
    │ Walk         ┆ 6     │
    │ Lineout      ┆ 4     │
    │ Single       ┆ 3     │
    │ Home Run     ┆ 3     │
    │ Double       ┆ 2     │
    │ Forceout     ┆ 1     │
    └──────────────┴───────┘



### Recipe 5 — League leaders for any stat 🥇

[`mlb_stats_leaders`](../mlb/reference/additional.md#mlb_stats_leaders)
gives you the league leaderboard for **any** category — `homeRuns`, `avg`,
`era`, `strikeouts`, you name it. The leaders come back nested under each
category, so we flatten the top-N into a tidy frame. Here's the 2024 home-run
race.


```python
def hr_leaders(season, category="homeRuns", group="hitting", n=10):
    raw = mlb.mlb_stats_leaders(leader_categories=category, season=season,
                                    stat_group=group, limit=n)
    leaders = raw["leagueLeaders"][0]["leaders"]
    rows = [{"rank": l["rank"], "player": l["person"]["fullName"],
             "team": l.get("team", {}).get("name"), "value": l["value"]}
            for l in leaders]
    return pl.DataFrame(rows)

leaders = safe("2024 HR leaders",
               lambda: hr_leaders(SAMPLE_SEASON, "homeRuns", "hitting", 10))
leaders if leaders is not None else "leaders unavailable right now"
```

    ✅ 2024 HR leaders





    shape: (10, 4)
    ┌──────┬───────────────────┬───────────────────────┬───────┐
    │ rank ┆ player            ┆ team                  ┆ value │
    │ ---  ┆ ---               ┆ ---                   ┆ ---   │
    │ i64  ┆ str               ┆ str                   ┆ str   │
    ╞══════╪═══════════════════╪═══════════════════════╪═══════╡
    │ 1    ┆ Aaron Judge       ┆ New York Yankees      ┆ 58    │
    │ 2    ┆ Shohei Ohtani     ┆ Los Angeles Dodgers   ┆ 54    │
    │ 3    ┆ Anthony Santander ┆ Baltimore Orioles     ┆ 44    │
    │ 4    ┆ Juan Soto         ┆ New York Yankees      ┆ 41    │
    │ 5    ┆ Marcell Ozuna     ┆ Atlanta Braves        ┆ 39    │
    │ 5    ┆ José Ramírez      ┆ Cleveland Guardians   ┆ 39    │
    │ 5    ┆ Brent Rooker      ┆ Oakland Athletics     ┆ 39    │
    │ 8    ┆ Kyle Schwarber    ┆ Philadelphia Phillies ┆ 38    │
    │ 9    ┆ Gunnar Henderson  ┆ Baltimore Orioles     ┆ 37    │
    │ 10   ┆ Ketel Marte       ┆ Arizona Diamondbacks  ┆ 36    │
    └──────┴───────────────────┴───────────────────────┴───────┘



### Recipe 6 — Who's beating their expected stats? 🎲

Statcast's expected stats ask *what should have happened* given each ball's
exit velocity and launch angle.
[`mlb_statcast_leaderboard_expected_stats`](../mlb/reference/mlb_statcast.md#mlb_statcast_leaderboard_expected_stats)
hands you `ba`/`est_ba`, `slg`/`est_slg`, `woba`/`est_woba` side by side —
sort by the diff to find the luckiest (and unluckiest) hitters.


```python
xstats = safe(
    "expected stats",
    lambda: mlb.mlb_statcast_leaderboard_expected_stats(
        year=SAMPLE_SEASON, type="batter", min="q"),
)
if xstats is not None and xstats.height and "est_woba_minus_woba_diff" in xstats.columns:
    cols = ["last_name, first_name", "pa", "woba", "est_woba",
            "est_woba_minus_woba_diff"]
    # Most negative diff = outperforming their expected wOBA the most.
    out = (xstats.select([c for c in cols if c in xstats.columns])
                 .sort("est_woba_minus_woba_diff").head(10))
else:
    out = "expected-stats leaderboard unavailable right now"
out
```

    ✅ expected stats





    shape: (10, 5)
    ┌───────────────────────┬─────┬───────┬──────────┬──────────────────────────┐
    │ last_name, first_name ┆ pa  ┆ woba  ┆ est_woba ┆ est_woba_minus_woba_diff │
    │ ---                   ┆ --- ┆ ---   ┆ ---      ┆ ---                      │
    │ str                   ┆ i64 ┆ f64   ┆ f64      ┆ f64                      │
    ╞═══════════════════════╪═════╪═══════╪══════════╪══════════════════════════╡
    │ Drury, Brandon        ┆ 360 ┆ 0.217 ┆ 0.264    ┆ -0.047                   │
    │ Soto, Juan            ┆ 713 ┆ 0.421 ┆ 0.463    ┆ -0.042                   │
    │ Bailey, Patrick       ┆ 448 ┆ 0.281 ┆ 0.322    ┆ -0.041                   │
    │ Sosa, Lenyn           ┆ 369 ┆ 0.28  ┆ 0.321    ┆ -0.041                   │
    │ Morel, Christopher    ┆ 611 ┆ 0.28  ┆ 0.316    ┆ -0.036                   │
    │ Garcia, Maikel        ┆ 626 ┆ 0.27  ┆ 0.305    ┆ -0.035                   │
    │ Martinez, J.D.        ┆ 495 ┆ 0.318 ┆ 0.353    ┆ -0.035                   │
    │ Harris II, Michael    ┆ 470 ┆ 0.312 ┆ 0.346    ┆ -0.034                   │
    │ Margot, Manuel        ┆ 343 ┆ 0.276 ┆ 0.31     ┆ -0.034                   │
    │ Kirk, Alejandro       ┆ 386 ┆ 0.297 ┆ 0.329    ┆ -0.032                   │
    └───────────────────────┴─────┴───────┴──────────┴──────────────────────────┘



### Recipe 7 — The fastest bats in baseball 💨

Bat tracking is one of Statcast's newest toys.
[`mlb_statcast_leaderboard_bat_tracking`](../mlb/reference/mlb_statcast.md#mlb_statcast_leaderboard_bat_tracking)
returns average bat speed, swing length, and "hard-swing rate" per hitter —
sort by `avg_bat_speed` to see who's swinging the hardest.


```python
bats = safe(
    "bat tracking",
    lambda: mlb.mlb_statcast_leaderboard_bat_tracking(year=SAMPLE_SEASON, type="batter"),
)
if bats is not None and bats.height and "avg_bat_speed" in bats.columns:
    cols = ["name", "swings_competitive", "avg_bat_speed",
            "hard_swing_rate", "swing_length"]
    out = (bats.select([c for c in cols if c in bats.columns])
               .sort("avg_bat_speed", descending=True).head(10))
else:
    out = "bat-tracking leaderboard unavailable right now"
out
```

    ✅ bat tracking





    shape: (10, 5)
    ┌────────────────────┬────────────────────┬───────────────┬─────────────────┬──────────────┐
    │ name               ┆ swings_competitive ┆ avg_bat_speed ┆ hard_swing_rate ┆ swing_length │
    │ ---                ┆ ---                ┆ ---           ┆ ---             ┆ ---          │
    │ str                ┆ i64                ┆ f64           ┆ f64             ┆ f64          │
    ╞════════════════════╪════════════════════╪═══════════════╪═════════════════╪══════════════╡
    │ Caminero, Junior   ┆ 666                ┆ 79.869797     ┆ 0.884384        ┆ 8.669529     │
    │ Walker, Jordan     ┆ 729                ┆ 79.156099     ┆ 0.855967        ┆ 8.316276     │
    │ Cruz, Oneil        ┆ 447                ┆ 78.461902     ┆ 0.780761        ┆ 7.622225     │
    │ Kurtz, Nick        ┆ 641                ┆ 78.253254     ┆ 0.808112        ┆ 7.748839     │
    │ Smith, Cam         ┆ 666                ┆ 77.496754     ┆ 0.762763        ┆ 7.703753     │
    │ Mitchell, Garrett  ┆ 533                ┆ 77.385725     ┆ 0.78424         ┆ 7.229286     │
    │ Caglianone, Jac    ┆ 656                ┆ 77.365695     ┆ 0.762195        ┆ 7.90592      │
    │ Adell, Jo          ┆ 808                ┆ 77.332608     ┆ 0.711634        ┆ 7.722094     │
    │ Contreras, Willson ┆ 670                ┆ 77.093347     ┆ 0.719403        ┆ 7.92869      │
    │ Schwarber, Kyle    ┆ 761                ┆ 77.064997     ┆ 0.746386        ┆ 7.482423     │
    └────────────────────┴────────────────────┴───────────────┴─────────────────┴──────────────┘



### Recipe 8 — The best gloves: Outs Above Average 🧤

Offense is easy to measure; defense is hard. Statcast's
[`mlb_statcast_leaderboard_outs_above_average`](../mlb/reference/mlb_statcast.md#mlb_statcast_leaderboard_outs_above_average)
credits fielders for the plays they make *relative to expectation*. Sort by
`outs_above_average` to find the season's best defenders.


```python
oaa = safe(
    "outs above average",
    lambda: mlb.mlb_statcast_leaderboard_outs_above_average(year=SAMPLE_SEASON),
)
if oaa is not None and oaa.height and "outs_above_average" in oaa.columns:
    cols = ["last_name, first_name", "display_team_name",
            "primary_pos_formatted", "outs_above_average",
            "fielding_runs_prevented"]
    out = (oaa.select([c for c in cols if c in oaa.columns])
              .sort("outs_above_average", descending=True).head(10))
else:
    out = "OAA leaderboard unavailable right now"
out
```

    ✅ outs above average





    shape: (10, 5)
    ┌───────────────────┬───────────────────┬───────────────────┬───────────────────┬──────────────────┐
    │ last_name,        ┆ display_team_name ┆ primary_pos_forma ┆ outs_above_averag ┆ fielding_runs_pr │
    │ first_name        ┆ ---               ┆ tted              ┆ e                 ┆ evented          │
    │ ---               ┆ str               ┆ ---               ┆ ---               ┆ ---              │
    │ str               ┆                   ┆ str               ┆ i64               ┆ i64              │
    ╞═══════════════════╪═══════════════════╪═══════════════════╪═══════════════════╪══════════════════╡
    │ Giménez, Andrés   ┆ Guardians         ┆ 2B                ┆ 21                ┆ 16               │
    │ Young, Jacob      ┆ Nationals         ┆ CF                ┆ 20                ┆ 18               │
    │ Semien, Marcus    ┆ Rangers           ┆ 2B                ┆ 19                ┆ 14               │
    │ Swanson, Dansby   ┆ Cubs              ┆ SS                ┆ 17                ┆ 13               │
    │ Siani, Michael    ┆ Cardinals         ┆ CF                ┆ 16                ┆ 14               │
    │ Siri, Jose        ┆ Rays              ┆ CF                ┆ 16                ┆ 14               │
    │ Witt Jr., Bobby   ┆ Royals            ┆ SS                ┆ 16                ┆ 12               │
    │ Lindor, Francisco ┆ Mets              ┆ SS                ┆ 15                ┆ 11               │
    │ Santana, Carlos   ┆ Twins             ┆ 1B                ┆ 15                ┆ 11               │
    │ Tovar, Ezequiel   ┆ Rockies           ┆ SS                ┆ 15                ┆ 11               │
    └───────────────────┴───────────────────┴───────────────────┴───────────────────┴──────────────────┘



### Recipe 9 — Find the X: the hardest-hit homers 🚀

`mlb_statcast_search` isn't just for one player — point its filters at an outcome.
Pass `at_bat_result="home_run"` over a short window to pull **every homer**,
then sort by `launch_speed` to find the ones that were absolutely crushed.
(Keep the window small — a couple of days at a time.)


```python
homers = safe(
    "home runs (2-day)",
    lambda: mlb.mlb_statcast_search(start_dt="2024-07-01", end_dt="2024-07-02",
                                at_bat_result="home_run"),
)
if homers is not None and homers.height and "launch_speed" in homers.columns:
    print("homers in window:", homers.height)
    cols = ["game_date", "player_name", "launch_speed",
            "launch_angle", "hit_distance_sc"]
    out = (homers.select([c for c in cols if c in homers.columns])
                 .sort("launch_speed", descending=True).head(10))
else:
    out = "no homers in that window right now"
out
```

    ✅ home runs (2-day)
    homers in window: 51





    shape: (10, 5)
    ┌────────────┬────────────────────┬──────────────┬──────────────┬─────────────────┐
    │ game_date  ┆ player_name        ┆ launch_speed ┆ launch_angle ┆ hit_distance_sc │
    │ ---        ┆ ---                ┆ ---          ┆ ---          ┆ ---             │
    │ str        ┆ str                ┆ f64          ┆ i64          ┆ i64             │
    ╞════════════╪════════════════════╪══════════════╪══════════════╪═════════════════╡
    │ 2024-07-02 ┆ De La Cruz, Elly   ┆ 114.1        ┆ 21           ┆ 425             │
    │ 2024-07-02 ┆ Judge, Aaron       ┆ 112.5        ┆ 25           ┆ 381             │
    │ 2024-07-02 ┆ Sánchez, Jesús     ┆ 112.5        ┆ 24           ┆ 448             │
    │ 2024-07-02 ┆ Ohtani, Shohei     ┆ 112.0        ┆ 37           ┆ 433             │
    │ 2024-07-02 ┆ Soler, Jorge       ┆ 109.0        ┆ 21           ┆ 394             │
    │ 2024-07-02 ┆ Rooker, Brent      ┆ 108.7        ┆ 34           ┆ 405             │
    │ 2024-07-02 ┆ Turner, Trea       ┆ 108.5        ┆ 20           ┆ 422             │
    │ 2024-07-02 ┆ Schneemann, Daniel ┆ 108.3        ┆ 28           ┆ 408             │
    │ 2024-07-02 ┆ Riley, Austin      ┆ 108.3        ┆ 36           ┆ 407             │
    │ 2024-07-02 ┆ Witt Jr., Bobby    ┆ 108.0        ┆ 24           ┆ 399             │
    └────────────┴────────────────────┴──────────────┴──────────────┴─────────────────┘



### Recipe 13 — Every pitch of a single game (Savant gamefeed) 🎮

[`mlb_statcast_gamefeed`](../mlb/reference/mlb_statcast.md#mlb_statcast_gamefeed)
pulls Baseball Savant's rich single-game feed and tidies it to **one row per
pitch** — pitch type, velocity, plate location, and the batted-ball result —
across both teams. Feed it any `game_pk` from a schedule.


```python
gf = safe(
    f"gamefeed {gid}",
    lambda: mlb.mlb_statcast_gamefeed(game_pk=gid),
)
if gf is not None and gf.height:
    print("pitches tracked:", gf.height)
    gcols = ["inning", "half_inning", "batter_name", "pitcher_name",
             "pitch_type", "start_speed", "launch_speed", "events"]
    out = gf.select([c for c in gcols if c in gf.columns]).head()
else:
    out = "gamefeed unavailable right now"
out
```

    ✅ gamefeed 744914
    pitches tracked: 244





    shape: (5, 8)
    ┌────────┬────────────┬────────────┬────────────┬────────────┬────────────┬────────────┬───────────┐
    │ inning ┆ half_innin ┆ batter_nam ┆ pitcher_na ┆ pitch_type ┆ start_spee ┆ launch_spe ┆ events    │
    │ ---    ┆ g          ┆ e          ┆ me         ┆ ---        ┆ d          ┆ ed         ┆ ---       │
    │ i64    ┆ ---        ┆ ---        ┆ ---        ┆ str        ┆ ---        ┆ ---        ┆ str       │
    │        ┆ str        ┆ str        ┆ str        ┆            ┆ f64        ┆ str        ┆           │
    ╞════════╪════════════╪════════════╪════════════╪════════════╪════════════╪════════════╪═══════════╡
    │ 1      ┆ top        ┆ Alex       ┆ Yariel     ┆ FF         ┆ 95.9       ┆ null       ┆ Flyout    │
    │        ┆            ┆ Bregman    ┆ Rodríguez  ┆            ┆            ┆            ┆           │
    │ 1      ┆ top        ┆ Alex       ┆ Yariel     ┆ FF         ┆ 94.1       ┆ null       ┆ Flyout    │
    │        ┆            ┆ Bregman    ┆ Rodríguez  ┆            ┆            ┆            ┆           │
    │ 1      ┆ top        ┆ Alex       ┆ Yariel     ┆ SL         ┆ 85.8       ┆ 92.9       ┆ Flyout    │
    │        ┆            ┆ Bregman    ┆ Rodríguez  ┆            ┆            ┆            ┆           │
    │ 1      ┆ top        ┆ Jake       ┆ Yariel     ┆ FF         ┆ 94.1       ┆ null       ┆ Strikeout │
    │        ┆            ┆ Meyers     ┆ Rodríguez  ┆            ┆            ┆            ┆           │
    │ 1      ┆ top        ┆ Jake       ┆ Yariel     ┆ FF         ┆ 95.4       ┆ null       ┆ Strikeout │
    │        ┆            ┆ Meyers     ┆ Rodríguez  ┆            ┆            ┆            ┆           │
    └────────┴────────────┴────────────┴────────────┴────────────┴────────────┴────────────┴───────────┘



### Recipe 10 — The biggest swings of a game (WPA) 📈

[`mlb_win_probability`](../mlb/reference/mlb_api.md#mlb_win_probability)
returns every play with the live win-probability before and after, plus
**Win Probability Added** (`homeTeamWinProbabilityAdded`). Sort by its absolute
value to surface the most pivotal moments of the game.


```python
def wpa_swings(game_pk, n=8):
    plays = mlb.mlb_win_probability(game_pk=game_pk, return_parsed=False)
    df = pl.json_normalize(plays, separator=".", max_level=2)
    keep = ["about.inning", "about.halfInning", "result.event",
            "result.description", "homeTeamWinProbabilityAdded"]
    df = df.select([c for c in keep if c in df.columns])
    if "homeTeamWinProbabilityAdded" in df.columns:
        df = (df.with_columns(
                  pl.col("homeTeamWinProbabilityAdded").abs().alias("wpa_abs"))
                .sort("wpa_abs", descending=True).drop("wpa_abs").head(n))
    return df

# Reuse the game_pk we pulled earlier (falls back to a known game).
wpa = safe(f"WPA swings {gid}", lambda: wpa_swings(gid))
wpa if wpa is not None else "win-probability unavailable right now"
```

    ✅ WPA swings 744914





    shape: (8, 5)
    ┌──────────────┬──────────────────┬──────────────────┬──────────────────────┬──────────────────────┐
    │ about.inning ┆ about.halfInning ┆ result.event     ┆ result.description   ┆ homeTeamWinProbabili │
    │ ---          ┆ ---              ┆ ---              ┆ ---                  ┆ tyAdded              │
    │ i64          ┆ str              ┆ str              ┆ str                  ┆ ---                  │
    │              ┆                  ┆                  ┆                      ┆ f64                  │
    ╞══════════════╪══════════════════╪══════════════════╪══════════════════════╪══════════════════════╡
    │ 8            ┆ bottom           ┆ Double           ┆ Spencer Horwitz      ┆ 23.7                 │
    │              ┆                  ┆                  ┆ doubles (3) on…      ┆                      │
    │ 8            ┆ bottom           ┆ Groundout        ┆ Daulton Varsho       ┆ -20.4                │
    │              ┆                  ┆                  ┆ grounds out sha…     ┆                      │
    │ 8            ┆ bottom           ┆ Lineout          ┆ George Springer      ┆ -19.3                │
    │              ┆                  ┆                  ┆ lines out to t…      ┆                      │
    │ 5            ┆ top              ┆ Home Run         ┆ Jeremy Peña homers   ┆ -14.8                │
    │              ┆                  ┆                  ┆ (6) on a fl…         ┆                      │
    │ 9            ┆ top              ┆ Home Run         ┆ Yordan Alvarez       ┆ -12.6                │
    │              ┆                  ┆                  ┆ homers (17) on …     ┆                      │
    │ 8            ┆ bottom           ┆ Walk             ┆ Addison Barger       ┆ 9.8                  │
    │              ┆                  ┆                  ┆ walks.               ┆                      │
    │ 8            ┆ bottom           ┆ Strikeout        ┆ Bo Bichette strikes  ┆ -9.0                 │
    │              ┆                  ┆                  ┆ out swingi…          ┆                      │
    │ 7            ┆ top              ┆ Grounded Into DP ┆ Yainer Diaz grounds  ┆ 8.1                  │
    │              ┆                  ┆                  ┆ into a dou…          ┆                      │
    └──────────────┴──────────────────┴──────────────────┴──────────────────────┴──────────────────────┘



### Recipe 11 — Season award winners (MVP, Cy Young) 🏅

[`mlb_awards`](../mlb/reference/mlb_api.md#mlb_awards) is the catalog of
every award id; [`mlb_award_recipients`](../mlb/reference/mlb_api.md#mlb_award_recipients)
names the season's winner for one id. We grab the four marquee awards — AL/NL
MVP and AL/NL Cy Young — and stack them into one tidy board.


```python
def award_board(season, award_ids):
    frames = []
    for label, aid in award_ids.items():
        df = mlb.mlb_award_recipients(award_id=aid, season=season)
        if df is not None and df.height:
            name_col = ("player_name_first_last" if "player_name_first_last"
                        in df.columns else "name")
            frames.append(df.select([
                pl.lit(label).alias("award"),
                pl.col("season"),
                pl.col(name_col).alias("winner"),
            ]))
    return pl.concat(frames, how="vertical") if frames else pl.DataFrame()

AWARDS = {"AL MVP": "ALMVP", "NL MVP": "NLMVP",
          "AL Cy Young": "ALCY", "NL Cy Young": "NLCY"}
board = safe("2024 award winners", lambda: award_board(SAMPLE_SEASON, AWARDS))
board if (board is not None and board.height) else "awards unavailable right now"
```

    ✅ 2024 award winners





    shape: (4, 3)
    ┌─────────────┬────────┬───────────────┐
    │ award       ┆ season ┆ winner        │
    │ ---         ┆ ---    ┆ ---           │
    │ str         ┆ str    ┆ str           │
    ╞═════════════╪════════╪═══════════════╡
    │ AL MVP      ┆ 2024   ┆ Aaron Judge   │
    │ NL MVP      ┆ 2024   ┆ Shohei Ohtani │
    │ AL Cy Young ┆ 2024   ┆ Tarik Skubal  │
    │ NL Cy Young ┆ 2024   ┆ Chris Sale    │
    └─────────────┴────────┴───────────────┘



### Recipe 12 — The first-round draft board 🎓

[`mlb_draft`](../mlb/reference/mlb_api.md#mlb_draft) returns the amateur
draft, organized into rounds of picks. Pass `round_=1` and flatten the picks
into one row per selection — who went where, and from which school.


```python
def draft_board(year, round_=1):
    raw = mlb.mlb_draft(year=year, round_=round_, return_parsed=False)
    picks = raw["drafts"]["rounds"][0]["picks"]
    rows = [{
        "pick": p.get("pickNumber"),
        "player": p.get("person", {}).get("fullName"),
        "team": p.get("team", {}).get("name"),
        "school": p.get("school", {}).get("name"),
    } for p in picks]
    return pl.DataFrame(rows)

draft = safe("2024 first round", lambda: draft_board(2024, round_=1))
draft.head(12) if (draft is not None and draft.height) else "draft unavailable right now"
```

    ✅ 2024 first round





    shape: (12, 4)
    ┌──────┬───────────────────┬──────────────────────┬─────────────────────┐
    │ pick ┆ player            ┆ team                 ┆ school              │
    │ ---  ┆ ---               ┆ ---                  ┆ ---                 │
    │ i64  ┆ str               ┆ str                  ┆ str                 │
    ╞══════╪═══════════════════╪══════════════════════╪═════════════════════╡
    │ 1    ┆ Travis Bazzana    ┆ Cleveland Guardians  ┆ Oregon State        │
    │ 2    ┆ Chase Burns       ┆ Cincinnati Reds      ┆ Wake Forest         │
    │ 3    ┆ Charlie Condon    ┆ Colorado Rockies     ┆ Georgia             │
    │ 4    ┆ Nick Kurtz        ┆ Athletics            ┆ Wake Forest         │
    │ 5    ┆ Hagen Smith       ┆ Chicago White Sox    ┆ Arkansas            │
    │ 6    ┆ Jac Caglianone    ┆ Kansas City Royals   ┆ Florida             │
    │ 7    ┆ JJ Wetherholt     ┆ St. Louis Cardinals  ┆ West Virginia       │
    │ 8    ┆ Christian Moore   ┆ Los Angeles Angels   ┆ Tennessee           │
    │ 9    ┆ Konnor Griffin    ┆ Pittsburgh Pirates   ┆ Jackson Prep School │
    │ 10   ┆ Seaver King       ┆ Washington Nationals ┆ Wake Forest         │
    │ 11   ┆ Bryce Rainer      ┆ Detroit Tigers       ┆ Harvard-Westlake HS │
    │ 12   ┆ Braden Montgomery ┆ Boston Red Sox       ┆ Texas A&M           │
    └──────┴───────────────────┴──────────────────────┴─────────────────────┘



## 📅 A whole season's schedule via ESPN

Want *every* game in a season without looping over dates? The bulk
`load_mlb_*` release-parquet loaders are still being wired up (they raise a
friendly `NotImplementedError` for now), and they point you to the working
path: [`espn_mlb_schedule`](../mlb/reference/additional.md#espn_mlb_schedule)
with `dates=<season year>` pulls the full slate as one wide frame. Scores come
back as **strings** — cast before doing arithmetic.


```python
season_sched = safe(
    "ESPN 2024 season schedule",
    lambda: mlb.espn_mlb_schedule(dates=2024),
)
if season_sched is not None and season_sched.height:
    print("games:", season_sched.height)
    scols = ["game_id", "away_display_name", "away_score",
             "home_display_name", "home_score", "status_type_completed"]
    out = season_sched.select([c for c in scols if c in season_sched.columns]).head()
else:
    out = "ESPN schedule unavailable right now"
out
```

    ✅ ESPN 2024 season schedule
    games: 500





    shape: (5, 6)
    ┌───────────┬────────────────────┬────────────┬───────────────────┬────────────┬───────────────────┐
    │ game_id   ┆ away_display_name  ┆ away_score ┆ home_display_name ┆ home_score ┆ status_type_compl │
    │ ---       ┆ ---                ┆ ---        ┆ ---               ┆ ---        ┆ eted              │
    │ str       ┆ str                ┆ str        ┆ str               ┆ str        ┆ ---               │
    │           ┆                    ┆            ┆                   ┆            ┆ bool              │
    ╞═══════════╪════════════════════╪════════════╪═══════════════════╪════════════╪═══════════════════╡
    │ 401576167 ┆ Los Angeles        ┆ 14         ┆ San Diego Padres  ┆ 1          ┆ true              │
    │           ┆ Dodgers            ┆            ┆                   ┆            ┆                   │
    │ 401576169 ┆ Kansas City Royals ┆ 4          ┆ Texas Rangers     ┆ 5          ┆ true              │
    │ 401576643 ┆ Chicago White Sox  ┆ 1          ┆ Chicago Cubs      ┆ 8          ┆ true              │
    │ 401576170 ┆ San Diego Padres   ┆ 1          ┆ Los Angeles       ┆ 4          ┆ true              │
    │           ┆                    ┆            ┆ Dodgers           ┆            ┆                   │
    │ 401576168 ┆ Arizona            ┆ 0          ┆ Colorado Rockies  ┆ 3          ┆ true              │
    │           ┆ Diamondbacks       ┆            ┆                   ┆            ┆                   │
    └───────────┴────────────────────┴────────────┴───────────────────┴────────────┴───────────────────┘



## ⚪ Secondary path: ESPN teams (`espn_mlb_*`)

[`espn_mlb_teams`](../mlb/reference/additional.md#espn_mlb_teams) returns one
wide polars frame — handy as a cross-check, or when you want ESPN's display
names and ids alongside the MLB Stats API ones.


```python
espn_teams = safe("ESPN teams", lambda: mlb.espn_mlb_teams())
ecols = ["team_id", "team_location", "team_name", "team_abbreviation", "team_display_name"]
(espn_teams.select([c for c in ecols if c in espn_teams.columns]).head()
 if espn_teams is not None else "ESPN teams unavailable right now")
```

    ✅ ESPN teams





    shape: (5, 5)
    ┌─────────┬───────────────┬──────────────┬───────────────────┬──────────────────────┐
    │ team_id ┆ team_location ┆ team_name    ┆ team_abbreviation ┆ team_display_name    │
    │ ---     ┆ ---           ┆ ---          ┆ ---               ┆ ---                  │
    │ str     ┆ str           ┆ str          ┆ str               ┆ str                  │
    ╞═════════╪═══════════════╪══════════════╪═══════════════════╪══════════════════════╡
    │ 29      ┆ Arizona       ┆ Diamondbacks ┆ ARI               ┆ Arizona Diamondbacks │
    │ 11      ┆ Athletics     ┆ Athletics    ┆ ATH               ┆ Athletics            │
    │ 15      ┆ Atlanta       ┆ Braves       ┆ ATL               ┆ Atlanta Braves       │
    │ 1       ┆ Baltimore     ┆ Orioles      ┆ BAL               ┆ Baltimore Orioles    │
    │ 2       ┆ Boston        ┆ Red Sox      ┆ BOS               ┆ Boston Red Sox       │
    └─────────┴───────────────┴──────────────┴───────────────────┴──────────────────────┘



## 🎉 Where to next

- Everything returns **polars** by default — pass `return_as_pandas=True` for a
  pandas frame, or `return_parsed=False` on the `mlb_*` wrappers for raw JSON.
- Full reference: the **MLB** pages in the sidebar —
  [MLB Stats API + Statcast helpers](../mlb/reference/additional.md),
  [the full MLB Stats API surface](../mlb/reference/mlb_api.md),
  and the ESPN [core](../mlb/reference/core.md) / [site](../mlb/reference/site.md) / [web](../mlb/reference/web.md) endpoints.
- R user? The same data lives in [baseballr](https://billpetti.github.io/baseballr/).
- Compare conventions with the other league intros (`04_nba_intro.ipynb`,
  `07_nhl_intro.ipynb`) or the cross-sport `01_quickstart.ipynb`.

Now go find the next 60-homer season. ⚾🔥
