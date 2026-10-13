# MLB tutorial

> > This page is the executed notebook [`09_mlb_intro.ipynb`](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/examples/notebooks/09_mlb_intro.ipynb): [download it](https://raw.githubusercontent.com/sportsdataverse/sportsdataverse-py/main/examples/notebooks/09_mlb_intro.ipynb) to run it yourself.

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
| [`mlb_schedule`](../mlb/reference/additional/mlb-stats-api.md#mlb_schedule) · [`parse_mlb_api_schedule`](../mlb/reference/additional/mlb-stats-api.md#mlb_schedule) | Games for a date / range — one row per game (with `game_pk`) | 🟢 **MLB Stats API** |
| [`mlb_teams`](../mlb/reference/additional/mlb-stats-api.md#mlb_teams) · [`parse_mlb_api_teams`](../mlb/reference/additional/mlb-stats-api.md#mlb_teams) | Every club — one row per team | 🟢 **MLB Stats API** |
| [`mlb_standings`](../mlb/reference/additional/mlb-stats-api.md#mlb_standings) · [`parse_mlb_api_standings`](../mlb/reference/additional/mlb-stats-api.md#mlb_standings) | Division standings — wins, losses, run diff | 🟢 **MLB Stats API** |
| [`mlb_team_roster`](../mlb/reference/mlb_api/team.md#mlb_team_roster) | A team's roster — one row per player | 🟢 **MLB Stats API** |
| [`mlb_person`](../mlb/reference/mlb_api/other.md#mlb_person) | A player's bio (one tidy row) | 🟢 **MLB Stats API** |
| [`mlb_person_stats`](../mlb/reference/additional/mlb-stats-api.md#mlb_person_stats) · [`parse_mlb_api_person_stats`](../mlb/reference/additional/mlb-stats-api.md#mlb_person_stats) | A player's season stat splits | 🟢 **MLB Stats API** |
| [`mlb_boxscore`](../mlb/reference/mlb_api/other.md#mlb_boxscore) | Full game box score | 🟢 **MLB Stats API** |
| [`mlb_play_by_play`](../mlb/reference/mlb_api/play.md#mlb_play_by_play) | Plate-appearance-level play-by-play | 🟢 **MLB Stats API** |
| [`mlb_stats_leaders`](../mlb/reference/additional/mlb-stats-api.md#mlb_stats_leaders) | League leaders for any stat (HR, AVG, ERA, …) | 🟢 **MLB Stats API** |
| [`mlb_win_probability`](../mlb/reference/mlb_api/other.md#mlb_win_probability) | Per-play win probability + WPA for a game | 🟢 **MLB Stats API** |
| [`mlb_awards`](../mlb/reference/mlb_api/other.md#mlb_awards) · [`mlb_award_recipients`](../mlb/reference/mlb_api/other.md#mlb_award_recipients) | Award catalog + season winners (MVP, Cy Young, …) | 🟢 **MLB Stats API** |
| [`mlb_draft`](../mlb/reference/mlb_api/draft.md#mlb_draft) | Amateur draft board — one row per pick | 🟢 **MLB Stats API** |
| [`mlb_statcast_search`](../mlb/reference/additional/baseball-savant-statcast.md#mlb_statcast_search) | Every pitch matching a filter — ~110 cols/pitch; auto date-chunks past the 25k cap; friendly filters (`batters_lookup`, `pitch_type`, `at_bat_result`, …) | 🔵 **Statcast** |
| [`mlb_statcast_search_minors`](../mlb/reference/additional/baseball-savant-statcast.md#mlb_statcast_search_minors) · [`mlb_statcast_search_wbc`](../mlb/reference/additional/baseball-savant-statcast.md#mlb_statcast_search_wbc) | Same pitch search for MiLB and the World Baseball Classic | 🔵 **Statcast** |
| `mlb_statcast_leaderboard_*` (37 of them) — e.g. [`…_sprint_speed`](../mlb/reference/mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_sprint_speed), [`…_expected_stats`](../mlb/reference/mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_expected_stats), [`…_bat_tracking`](../mlb/reference/mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_bat_tracking), [`…_outs_above_average`](../mlb/reference/mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_outs_above_average) | Every Savant leaderboard: expected stats, sprint speed, bat tracking, pitch arsenals/movement/tempo, OAA, arm strength, catcher framing/blocking/throwing, baserunning, park factors, … | 🔵 **Statcast** |
| [`mlb_statcast_gamefeed`](../mlb/reference/mlb_statcast/other.md#mlb_statcast_gamefeed) | Savant single-game feed — one tidy row per pitch | 🔵 **Statcast** |
| [`mlb_statcast_player`](../mlb/reference/additional/baseball-savant-statcast.md#mlb_statcast_player) | A player's Savant page metrics | 🔵 **Statcast** |
| [`espn_mlb_teams`](../mlb/reference/additional/espn.md#espn_mlb_teams) · [`espn_mlb_schedule`](../mlb/reference/additional/espn.md#espn_mlb_schedule) | ESPN teams / schedule (wide frames) | ⚪ ESPN |
| [`most_recent_mlb_season`](../mlb/reference/additional/dates-and-seasons.md#most_recent_mlb_season) | Current season helper | ⚪ helper |

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

The MLB Stats API and Savant are public and reliable, but they're still
**live network calls** — a date with no games, an offseason day, or a blip can
make a call come back empty. So we use a tiny `safe()` helper: you get the
frame when the feed is up, and a friendly one-liner when it isn't (never a
scary traceback). 🛟

We also pick a stable **completed-season** date for our examples so the page
renders the same in June as in October.

```python
from sportsdataverse.errors import AssetFetchError, NoDataError

def safe(label, thunk):
    """Run a live call defensively: return its result, or print a one-liner."""
    try:
        out = thunk()
        print(f"✅ {label}")
        return out
    except (NoDataError, AssetFetchError) as e:
        print(f"\u23ed\ufe0f  {label}: {type(e).__name__}: {e}")
        return None

# A known completed regular-season slate — stable for the docs build.
SAMPLE_SEASON = 2024
SAMPLE_DATE = "2024-07-01"  # YYYY-MM-DD for the Stats API
JUDGE_ID = 592450           # Aaron Judge, NYY — our running example player
YANKEES_ID = 147            # New York Yankees team_id
```

## 📅 The schedule (MLB Stats API)

[`mlb_schedule`](../mlb/reference/additional/mlb-stats-api.md#mlb_schedule) returns the
raw JSON `dict`; its partner
[`parse_mlb_api_schedule`](../mlb/reference/additional/mlb-stats-api.md#mlb_schedule)
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

## 🏆 Standings (MLB Stats API)

[`mlb_standings`](../mlb/reference/additional/mlb-stats-api.md#mlb_standings) covers
both leagues by default (`league_id="103,104"`).
[`parse_mlb_api_standings`](../mlb/reference/additional/mlb-stats-api.md#mlb_standings)
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

## 🧢 Teams & rosters (MLB Stats API)

[`mlb_teams`](../mlb/reference/additional/mlb-stats-api.md#mlb_teams) +
[`parse_mlb_api_teams`](../mlb/reference/additional/mlb-stats-api.md#mlb_teams) lists every
club — grab a `team_id` here.
[`mlb_team_roster`](../mlb/reference/mlb_api/team.md#mlb_team_roster) then
returns a tidy frame directly (one row per player).

```python
teams = safe(
    "teams",
    lambda: mlb.parse_mlb_api_teams(mlb.mlb_teams(season=SAMPLE_SEASON)),
)
(teams.select(["id", "name", "abbreviation", "location_name", "team_name"]).head()
 if teams is not None else "teams unavailable right now")
```

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

## 🧍 Player bio & season stats (MLB Stats API)

[`mlb_person`](../mlb/reference/mlb_api/other.md#mlb_person) returns a one-row
bio frame. [`mlb_person_stats`](../mlb/reference/additional/mlb-stats-api.md#mlb_person_stats)
returns the raw stat-split `dict`;
[`parse_mlb_api_person_stats`](../mlb/reference/additional/mlb-stats-api.md#mlb_person_stats)
flattens it. Our running example is Aaron Judge (`person_id=592450`).

```python
bio = safe("Judge bio", lambda: mlb.mlb_person(person_id=JUDGE_ID))
bcols = ["id", "full_name", "primary_number", "birth_date",
         "height", "weight", "mlb_debut_date"]
(bio.select([c for c in bcols if c in bio.columns])
 if bio is not None else "bio unavailable right now")
```

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

## 🎯 Pitch-level Statcast (Baseball Savant)

Now the fun part — **every single pitch**.
[`mlb_statcast_search`](../mlb/reference/additional/baseball-savant-statcast.md#mlb_statcast_search) pulls each
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

### Recipe 3 — Box score for one game 📊

Take a `game_pk` from any schedule and pull the full box score with
[`mlb_boxscore`](../mlb/reference/mlb_api/other.md#mlb_boxscore). Asking for
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

### Recipe 4 — Plate-appearance play-by-play + outcome mix ⚾

[`mlb_play_by_play`](../mlb/reference/mlb_api/play.md#mlb_play_by_play)
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

### Recipe 5 — League leaders for any stat 🥇

[`mlb_stats_leaders`](../mlb/reference/additional/mlb-stats-api.md#mlb_stats_leaders)
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

### Recipe 6 — Who's beating their expected stats? 🎲

Statcast's expected stats ask *what should have happened* given each ball's
exit velocity and launch angle.
[`mlb_statcast_leaderboard_expected_stats`](../mlb/reference/mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_expected_stats)
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

### Recipe 7 — The fastest bats in baseball 💨

Bat tracking is one of Statcast's newest toys.
[`mlb_statcast_leaderboard_bat_tracking`](../mlb/reference/mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_bat_tracking)
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

### Recipe 8 — The best gloves: Outs Above Average 🧤

Offense is easy to measure; defense is hard. Statcast's
[`mlb_statcast_leaderboard_outs_above_average`](../mlb/reference/mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_outs_above_average)
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

### Recipe 13 — Every pitch of a single game (Savant gamefeed) 🎮

[`mlb_statcast_gamefeed`](../mlb/reference/mlb_statcast/other.md#mlb_statcast_gamefeed)
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

### Recipe 10 — The biggest swings of a game (WPA) 📈

[`mlb_win_probability`](../mlb/reference/mlb_api/other.md#mlb_win_probability)
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

### Recipe 11 — Season award winners (MVP, Cy Young) 🏅

[`mlb_awards`](../mlb/reference/mlb_api/other.md#mlb_awards) is the catalog of
every award id; [`mlb_award_recipients`](../mlb/reference/mlb_api/other.md#mlb_award_recipients)
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

### Recipe 12 — The first-round draft board 🎓

[`mlb_draft`](../mlb/reference/mlb_api/draft.md#mlb_draft) returns the amateur
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

## 📅 A whole season's schedule via ESPN

Want *every* game in a season without looping over dates? The bulk
`load_mlb_*` release-parquet loaders are still being wired up (they raise a
friendly `NotImplementedError` for now), and they point you to the working
path: [`espn_mlb_schedule`](../mlb/reference/additional/espn.md#espn_mlb_schedule)
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

## ⚪ Secondary path: ESPN teams (`espn_mlb_*`)

[`espn_mlb_teams`](../mlb/reference/additional/espn.md#espn_mlb_teams) returns one
wide polars frame — handy as a cross-check, or when you want ESPN's display
names and ids alongside the MLB Stats API ones.

```python
espn_teams = safe("ESPN teams", lambda: mlb.espn_mlb_teams())
ecols = ["team_id", "team_location", "team_name", "team_abbreviation", "team_display_name"]
(espn_teams.select([c for c in ecols if c in espn_teams.columns]).head()
 if espn_teams is not None else "ESPN teams unavailable right now")
```

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
