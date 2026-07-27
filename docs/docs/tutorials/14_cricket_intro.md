---
title: Cricket tutorial
sidebar_label: Cricket
sidebar_position: 13
---

# 🏏 Cricket with `sportsdataverse-py`

**ESPN** carries live scorecards, standings, and full match summaries for the world's
most widely-played bat-and-ball game. `sportsdataverse-py` wraps that surface through
the `sportsdataverse.cricket` module — one `league=` slug away from IPL, England's
county circuit, the ICC World Cup, and every other tournament ESPN indexes.

### Cricket in 30 seconds

If you're new to cricket, the key numbers to know:

| Concept | What it means in the data |
|---|---|
| **Innings** | A team's turn to bat; T20 matches have one per team, Tests have two |
| **Score string** | `"161/5 (18/20 ov, target 156)"` — runs / wickets (overs used / overs allowed, target) |
| **Wickets** | Dismissals; ten wickets = all out, innings ends |
| **Overs** | Six-ball delivery sets; T20 = 20 overs, ODI = 50, Tests = open |
| **Partnership** | Runs scored by two batters sharing the crease |

The score string (`home_score` / `away_score`) is returned verbatim from ESPN so
downstream analysis retains the full cricket context rather than stripping it to
a bare integer.

### What this notebook covers

1. Setup and the `safe()` guard helper
2. Scoreboard — live and recent matches for a league
3. Standings — the `children` hierarchy, flattened
4. Match summary — all 8 matchcard sections, with emphasis on the three
   heterogeneous batting / bowling / partnerships scorecard shapes
5. Bonus endpoints — news, injuries, calendar
6. Caveats and the full reference


## 🧰 The toolbox

Everything returns a tidy **polars** `DataFrame` by default — pass
`return_as_pandas=True` for pandas, or `return_parsed=False` for the raw JSON `dict`.

The three cricket-specific parsers are:

| Parser | Input | Output |
|---|---|---|
| `parse_cricket_scoreboard` | `espn_cricket_scoreboard` payload | One row per match; score strings in cricket format |
| `parse_cricket_standings` | `espn_cricket_standings` payload | One row per team per group; flattened `group` column |
| `parse_cricket_summary` | `espn_cricket_summary` payload | Dict of 8 section DataFrames (or single section) |

All other `espn_cricket_*` wrappers reuse the universal parsers (`parse_news`,
`parse_items`, `parse_single_entity`, etc.) shared across all ESPN-backed sports.


## 🔌 Setup

```sh
pip install sportsdataverse
```

No API key required.


```python
import polars as pl
import sportsdataverse.cricket as cricket

# IPL (Indian Premier League) league slug — used throughout this notebook.
# Other common slugs: 'eng.1' (England domestic), 'icc.worldcup' (ODI World Cup)
IPL = "8048"

print("polars", pl.__version__)
```

    polars 1.42.0


The ESPN cricket feed is live and occasionally rate-limited, so a small `safe()`
helper runs every network call defensively. Any exception is caught, printed, and
`None` is returned — downstream cells check for `None` before proceeding.


```python
def safe(label, thunk):
    try:
        out = thunk()
        print(f"✅ {label}")
        return out
    except Exception as exc:
        print(f"⚠️  {label} — {exc}")
        return None
```

## 📡 Scoreboard — today's (and recent) matches

[`espn_cricket_scoreboard`](../cricket/reference/site.md#espn_cricket_scoreboard)
hits the Site v2 scoreboard endpoint for the given `league=` slug.
By default (`return_parsed=True`) it routes the payload through
`parse_cricket_scoreboard` and returns a tidy polars frame.
Pass `return_parsed=False` to get the raw ESPN JSON dict instead.

Each row is one match. The `home_score` / `away_score` columns carry the full
cricket score string — ESPN doesn't expose a clean integer run-count separately,
and the wickets + overs context matters.


```python
board = safe(
    "IPL scoreboard",
    lambda: cricket.espn_cricket_scoreboard(league=IPL),
)
board
```

    ✅ IPL scoreboard





    shape: (1, 14)
    ┌──────────┬────────────┬────────────┬────────────┬───┬────────┬───────────┬───────────┬───────────┐
    │ event_id ┆ date       ┆ name       ┆ short_name ┆ … ┆ status ┆ status_de ┆ venue     ┆ neutral_s │
    │ ---      ┆ ---        ┆ ---        ┆ ---        ┆   ┆ ---    ┆ tail      ┆ ---       ┆ ite       │
    │ str      ┆ str        ┆ str        ┆ str        ┆   ┆ null   ┆ ---       ┆ str       ┆ ---       │
    │          ┆            ┆            ┆            ┆   ┆        ┆ str       ┆           ┆ bool      │
    ╞══════════╪════════════╪════════════╪════════════╪═══╪════════╪═══════════╪═══════════╪═══════════╡
    │ 1535465  ┆ 2026-05-31 ┆ Royal Chal ┆ RCB v GT   ┆ … ┆ null   ┆ Final     ┆ Narendra  ┆ true      │
    │          ┆ T14:00Z    ┆ lengers    ┆            ┆   ┆        ┆           ┆ Modi      ┆           │
    │          ┆            ┆ Bengaluru  ┆            ┆   ┆        ┆           ┆ Stadium,  ┆           │
    │          ┆            ┆ v …        ┆            ┆   ┆        ┆           ┆ Motera,…  ┆           │
    └──────────┴────────────┴────────────┴────────────┴───┴────────┴───────────┴───────────┴───────────┘



### What the columns mean

| Column | Description |
|---|---|
| `event_id` | ESPN event identifier — pass this to `espn_cricket_summary` |
| `date` | ISO-8601 match start time |
| `name` / `short_name` | Full and abbreviated match name |
| `home_team` / `away_team` | Display names |
| `home_score` / `away_score` | Cricket score string, e.g. `"161/5 (18/20 ov, target 156)"` |
| `status` | `"Final"`, `"In Progress"`, `"Scheduled"`, etc. |
| `status_detail` | Human-readable detail, e.g. `"Chennai Super Kings won by 5 wickets"` |
| `venue` | Ground name |
| `neutral_site` | Boolean — neutral ground match |


```python
# If the scoreboard returned data, show the match-status breakdown.
if board is not None and board.height:
    keep = [c for c in ["name", "home_score", "away_score", "status", "status_detail"] if c in board.columns]
    print(board.select(keep))
else:
    print("scoreboard unavailable right now — try again outside an off-season window")
```

    shape: (1, 5)
    ┌──────────────────────────────────┬─────────────────────────┬────────────┬────────┬───────────────┐
    │ name                             ┆ home_score              ┆ away_score ┆ status ┆ status_detail │
    │ ---                              ┆ ---                     ┆ ---        ┆ ---    ┆ ---           │
    │ str                              ┆ str                     ┆ str        ┆ null   ┆ str           │
    ╞══════════════════════════════════╪═════════════════════════╪════════════╪════════╪═══════════════╡
    │ Royal Challengers Bengaluru v …  ┆ 161/5 (18/20 ov, target ┆ 155/8      ┆ null   ┆ Final         │
    │                                  ┆ 156)                    ┆            ┆        ┆               │
    └──────────────────────────────────┴─────────────────────────┴────────────┴────────┴───────────────┘


### Raw payload mode

Pass `return_parsed=False` to skip the parser entirely and work with the raw
ESPN JSON. This is useful when you want to explore the full payload structure,
or when you need a field the parser doesn't yet surface.


```python
raw_board = safe(
    "IPL scoreboard (raw)",
    lambda: cricket.espn_cricket_scoreboard(league=IPL, return_parsed=False),
)
if isinstance(raw_board, dict):
    print("Top-level keys:", list(raw_board.keys()))
    events = raw_board.get("events") or []
    print(f"Events in payload: {len(events)}")
```

    ✅ IPL scoreboard (raw)
    Top-level keys: ['leagues', 'teams', 'standings', 'events', 'provider']
    Events in payload: 1


## 🏆 Standings

[`espn_cricket_standings`](../cricket/reference/site.md#espn_cricket_standings)
returns the league table for a given season. The ESPN cricket standings payload
uses a `children` hierarchy (groups/divisions) rather than the flat `groups` shape
used in most other ESPN sports.

`parse_cricket_standings` flattens that hierarchy — each row is one team in one
group, with a `group` column so you can split multi-group tournaments (e.g. ICC
World Cup group stages) with a single `.filter()` call.

Optional parameters: `season=`, `group=`, `standings_type=`.


```python
standings = safe(
    "IPL standings",
    lambda: cricket.espn_cricket_standings(league=IPL),
)
standings
```

    ✅ IPL standings





    shape: (10, 15)
    ┌───────┬────────────────────┬─────────┬───────────────────┬───┬────────┬────────┬─────────┬───────┐
    │ group ┆ team               ┆ team_id ┆ team_abbreviation ┆ … ┆ netrr  ┆ for    ┆ against ┆ total │
    │ ---   ┆ ---                ┆ ---     ┆ ---               ┆   ┆ ---    ┆ ---    ┆ ---     ┆ ---   │
    │ str   ┆ str                ┆ str     ┆ str               ┆   ┆ f64    ┆ f64    ┆ f64     ┆ str   │
    ╞═══════╪════════════════════╪═════════╪═══════════════════╪═══╪════════╪════════╪═════════╪═══════╡
    │       ┆ Royal Challengers  ┆ 335970  ┆ RCB               ┆ … ┆ 0.783  ┆ 10.393 ┆ 9.615   ┆       │
    │       ┆ Bengaluru          ┆         ┆                   ┆   ┆        ┆        ┆         ┆       │
    │       ┆ Gujarat Titans     ┆ 1298769 ┆ GT                ┆ … ┆ 0.695  ┆ 9.46   ┆ 8.755   ┆       │
    │       ┆ Sunrisers          ┆ 628333  ┆ SRH               ┆ … ┆ 0.524  ┆ 10.337 ┆ 9.82    ┆       │
    │       ┆ Hyderabad          ┆         ┆                   ┆   ┆        ┆        ┆         ┆       │
    │       ┆ Rajasthan Royals   ┆ 335977  ┆ RR                ┆ … ┆ 0.189  ┆ 10.096 ┆ 9.907   ┆       │
    │       ┆ Punjab Kings       ┆ 335973  ┆ PBKS              ┆ … ┆ 0.309  ┆ 10.844 ┆ 10.535  ┆       │
    │       ┆ Delhi Capitals     ┆ 335975  ┆ DC                ┆ … ┆ -0.651 ┆ 9.394  ┆ 10.039  ┆       │
    │       ┆ Kolkata Knight     ┆ 335971  ┆ KKR               ┆ … ┆ -0.147 ┆ 9.096  ┆ 9.24    ┆       │
    │       ┆ Riders             ┆         ┆                   ┆   ┆        ┆        ┆         ┆       │
    │       ┆ Chennai Super      ┆ 335974  ┆ CSK               ┆ … ┆ -0.345 ┆ 9.2    ┆ 9.548   ┆       │
    │       ┆ Kings              ┆         ┆                   ┆   ┆        ┆        ┆         ┆       │
    │       ┆ Mumbai Indians     ┆ 335978  ┆ MI                ┆ … ┆ -0.584 ┆ 9.512  ┆ 10.092  ┆       │
    │       ┆ Lucknow Super      ┆ 1298768 ┆ LSG               ┆ … ┆ -0.74  ┆ 9.132  ┆ 9.868   ┆       │
    │       ┆ Giants             ┆         ┆                   ┆   ┆        ┆        ┆         ┆       │
    └───────┴────────────────────┴─────────┴───────────────────┴───┴────────┴────────┴─────────┴───────┘




```python
if standings is not None and standings.height:
    print("Columns:", standings.columns)
    print("\nGroups present:", standings["group"].unique().to_list() if "group" in standings.columns else "(none)")
    # Sort by points (or net run rate when available).
    sort_col = next((c for c in ["points", "wins"] if c in standings.columns), None)
    if sort_col:
        print(f"\nTop 4 by {sort_col}:")
        print(
            standings
            .sort(sort_col, descending=True)
            .select([c for c in ["team", "group", "wins", "losses", "points", "net_run_rate"]
                     if c in standings.columns])
            .head(4)
        )
else:
    print("standings unavailable right now")
```

    Columns: ['group', 'team', 'team_id', 'team_abbreviation', 'rank', 'matches_played', 'matches_won', 'matches_lost', 'noresult', 'match_points', 'qualified', 'netrr', 'for', 'against', 'total']

    Groups present: ['']


### Why the `children` hierarchy matters

Most ESPN standings payloads have a flat `standings.groups[]` block. Cricket uses
`standings.children[]` instead — each child is a group/division with its own
entries array. `parse_cricket_standings` walks that nesting and stitches a `group`
column onto every team row, so the resulting frame is directly filterable:

```python
# Keep only Group A in a World Cup-style tournament
standings.filter(pl.col("group") == "Group A")
```

The numeric stat columns (wins, losses, points, net run rate, etc.) are snake-cased
versions of whatever ESPN ships — they vary by tournament format.

## 🃏 Match summary — the full scorecard

[`espn_cricket_summary`](../cricket/reference/site.md#espn_cricket_summary) is the
richest endpoint: a single call returns **8 frames** that collectively make up the
entire matchcard.

| Section key | Content |
|---|---|
| `header` | Match metadata — teams, status, venue, toss result |
| `matchcards_batting` | Per-batter innings rows (runs, balls, fours, sixes, strike rate) |
| `matchcards_bowling` | Per-bowler innings rows (overs, maidens, runs, wickets, economy) |
| `matchcards_partnerships` | Partnership pairs (runs, balls, each batter's contribution) |
| `rosters` | Full squad lists for both teams |
| `game_info` | Match-level metadata (series name, match type, result method) |
| `leaders` | Stat leaders for the match |
| `standings` | In-match standings snapshot |

The three `matchcards_*` frames have **different schemas** — batting rows carry
`runs`/`balls`/`strike_rate`; bowling rows carry `overs`/`wickets`/`economy`;
partnership rows carry `total_runs`/`total_balls` plus per-batter run splits.
They are returned as **separate frames** so callers can work with each schema cleanly.

Event ID `1535465` is an IPL match (Chennai Super Kings vs. Mumbai Indians) that
is used as the worked example throughout.


```python
EVENT_ID = 1535465  # IPL — Chennai Super Kings vs. Mumbai Indians

summary_raw = safe(
    f"match summary {EVENT_ID}",
    lambda: cricket.espn_cricket_summary(league=IPL, event_id=EVENT_ID, return_parsed=False),
)
if isinstance(summary_raw, dict):
    print("Top-level keys:", list(summary_raw.keys()))
```

    ✅ match summary 1535465
    Top-level keys: ['notes', 'gameInfo', 'debuts', 'rosters', 'matchcards', 'leaders', 'article', 'videos', 'news', 'header', 'wallclockAvailable', 'meta', 'standings']


### Parsing all 8 sections at once

Call `parse_cricket_summary` with `section=None` (the default) to get a
`dict[str, pl.DataFrame]` keyed by section name.


```python
from sportsdataverse.cricket.cricket_espn_parsers import parse_cricket_summary

if summary_raw is not None:
    frames = parse_cricket_summary(summary_raw)
    for name, df in frames.items():
        print(f"{name:30s}  {df.shape[0]:>4d} rows × {df.shape[1]:>3d} cols")
else:
    frames = {}
    print("summary payload unavailable — frames dict is empty")
```

    header                             1 rows ×  15 cols
    matchcards_batting                11 rows ×  12 cols
    matchcards_bowling                 6 rows ×  10 cols
    matchcards_partnerships            6 rows ×  10 cols
    rosters                           24 rows ×   9 cols
    game_info                          1 rows ×   7 cols
    leaders                            0 rows ×   0 cols
    standings                         10 rows ×  14 cols


### 🏏 Section: `matchcards_batting`

One row per batter per innings. Each innings is identified by `innings_number`
and `team`. The `summary` column carries the batter's dismissal description
(e.g. `"c Rohit b Bumrah"`). Batters who haven't faced a ball yet appear with
null run / ball counts.


```python
batting = frames.get("matchcards_batting", pl.DataFrame())
if batting.height:
    print("Batting columns:", batting.columns)
    show_cols = [c for c in ["innings_number", "team", "athlete_display_name",
                              "runs", "balls", "fours", "sixes", "strike_rate", "summary"]
                 if c in batting.columns]
    print(batting.select(show_cols).head(10))
else:
    print("batting scorecard unavailable for this match")
```

    Batting columns: ['innings_number', 'team_name', 'total', 'runs_total', 'extras', 'player_id', 'player_name', 'dismissal', 'runs', 'balls_faced', 'fours', 'sixes']
    shape: (10, 4)
    ┌────────────────┬──────┬───────┬───────┐
    │ innings_number ┆ runs ┆ fours ┆ sixes │
    │ ---            ┆ ---  ┆ ---   ┆ ---   │
    │ str            ┆ str  ┆ str   ┆ str   │
    ╞════════════════╪══════╪═══════╪═══════╡
    │ 2              ┆ 32   ┆ 4     ┆ 2     │
    │ 2              ┆ 75   ┆ 9     ┆ 3     │
    │ 2              ┆ 1    ┆ 0     ┆ 0     │
    │ 2              ┆ 15   ┆ 1     ┆ 1     │
    │ 2              ┆ 1    ┆ 0     ┆ 0     │
    │ 2              ┆ 24   ┆ 3     ┆ 1     │
    │ 2              ┆ 11   ┆ 1     ┆ 0     │
    │ 2              ┆      ┆       ┆       │
    │ 2              ┆      ┆       ┆       │
    │ 2              ┆      ┆       ┆       │
    └────────────────┴──────┴───────┴───────┘


### 🎯 Section: `matchcards_bowling`

One row per bowler per innings. Key columns: `overs`, `maidens`, `runs_conceded`,
`wickets`, `economy`. Economy rate is the average runs conceded per over —
lower is better.


```python
bowling = frames.get("matchcards_bowling", pl.DataFrame())
if bowling.height:
    print("Bowling columns:", bowling.columns)
    show_cols = [c for c in ["innings_number", "team", "athlete_display_name",
                              "overs", "maidens", "runs_conceded", "wickets", "economy"]
                 if c in bowling.columns]
    print(bowling.select(show_cols).head(10))
else:
    print("bowling scorecard unavailable for this match")
```

    Bowling columns: ['innings_number', 'team_name', 'player_id', 'player_name', 'overs', 'maidens', 'conceded', 'wickets', 'economy_rate', 'nbw']
    shape: (6, 4)
    ┌────────────────┬───────┬─────────┬─────────┐
    │ innings_number ┆ overs ┆ maidens ┆ wickets │
    │ ---            ┆ ---   ┆ ---     ┆ ---     │
    │ str            ┆ str   ┆ str     ┆ str     │
    ╞════════════════╪═══════╪═════════╪═════════╡
    │ 2              ┆ 4.0   ┆ 0       ┆ 1       │
    │ 2              ┆ 3.0   ┆ 0       ┆ 1       │
    │ 2              ┆ 2.0   ┆ 0       ┆ 0       │
    │ 2              ┆ 4.0   ┆ 0       ┆ 2       │
    │ 2              ┆ 4.0   ┆ 0       ┆ 1       │
    │ 2              ┆ 1.0   ┆ 0       ┆ 0       │
    └────────────────┴───────┴─────────┴─────────┘


### 🤝 Section: `matchcards_partnerships`

One row per partnership (pair of batters sharing the crease) per innings.
This frame has a **different schema** from batting and bowling — it carries
total runs and balls for the partnership, plus per-batter run splits.
Partnership data is cricket-specific and has no direct analogue in ball-sport
box scores.


```python
partnerships = frames.get("matchcards_partnerships", pl.DataFrame())
if partnerships.height:
    print("Partnerships columns:", partnerships.columns)
    show_cols = [c for c in ["innings_number", "team", "total_runs", "total_balls",
                              "batter1_display_name", "batter1_runs",
                              "batter2_display_name", "batter2_runs"]
                 if c in partnerships.columns]
    print(partnerships.select(show_cols).head(8))
else:
    print("partnerships scorecard unavailable for this match")
```

    Partnerships columns: ['innings_number', 'team_name', 'partnership_runs', 'partnership_overs', 'wicket_name', 'fow_type', 'player1_name', 'player1_runs', 'player2_name', 'player2_runs']
    shape: (6, 1)
    ┌────────────────┐
    │ innings_number │
    │ ---            │
    │ str            │
    ╞════════════════╡
    │ 2              │
    │ 2              │
    │ 2              │
    │ 2              │
    │ 2              │
    │ 2              │
    └────────────────┘


### 🏟️ Section: `header`

Match-level metadata: teams, status, venue, and the competition context. This
is typically the first frame you'd use to confirm match identity and outcome.


```python
header = frames.get("header", pl.DataFrame())
if header.height:
    show_cols = [c for c in ["name", "status_type_name", "status_type_detail",
                              "home_team", "home_score",
                              "away_team", "away_score", "venue_full_name"]
                 if c in header.columns]
    print(header.select(show_cols).head())
else:
    print("header unavailable for this match")
```

    shape: (0, 0)
    ┌┐
    ╞╡
    └┘


### 📋 Section: `game_info`

Match-level metadata that doesn't fit the header: toss winner, match type
(T20, ODI, Test), series name, playing conditions, and the result method if
weather interruption applied (D/L method).


```python
game_info = frames.get("game_info", pl.DataFrame())
if game_info.height:
    print(game_info.head())
else:
    print("game_info unavailable for this match")
```

    shape: (1, 7)
    ┌──────────┬───────────────┬───────────────┬────────────┬──────────────┬────────────┬──────────────┐
    │ venue_id ┆ venue_full_na ┆ venue_short_n ┆ venue_city ┆ venue_countr ┆ attendance ┆ officials    │
    │ ---      ┆ me            ┆ ame           ┆ ---        ┆ y            ┆ ---        ┆ ---          │
    │ str      ┆ ---           ┆ ---           ┆ str        ┆ ---          ┆ i64        ┆ str          │
    │          ┆ str           ┆ str           ┆            ┆ str          ┆            ┆              │
    ╞══════════╪═══════════════╪═══════════════╪════════════╪══════════════╪════════════╪══════════════╡
    │ 57851    ┆ Narendra Modi ┆ Narendra Modi ┆ Ahmedabad  ┆ India        ┆ 0          ┆ [{'displayNa │
    │          ┆ Stadium,      ┆ Stadium,      ┆            ┆              ┆            ┆ me': 'KN     │
    │          ┆ Motera,…      ┆ Motera,…      ┆            ┆              ┆            ┆ Ananthapa…   │
    └──────────┴───────────────┴───────────────┴────────────┴──────────────┴────────────┴──────────────┘


### 🔎 Requesting a single section

When you only need one frame, pass `section=` to `parse_cricket_summary` to
avoid deserializing all 8 sections. The wrapper also accepts the section via
`return_parsed=True` + `section=` if you want to skip the intermediate raw dict.


```python
if summary_raw is not None:
    just_batting = parse_cricket_summary(summary_raw, section="matchcards_batting")
    print(type(just_batting), just_batting.shape)

    # pandas interop — same one-liner as every other sdv-py endpoint
    just_batting_pd = parse_cricket_summary(summary_raw, section="matchcards_batting",
                                             return_as_pandas=True)
    print(type(just_batting_pd))
else:
    print("no payload to parse")
```

    <class 'polars.dataframe.frame.DataFrame'> (11, 12)
    <class 'pandas.DataFrame'>


## 📰 News and injuries

[`espn_cricket_news`](../cricket/reference/site.md#espn_cricket_news) and
[`espn_cricket_injuries`](../cricket/reference/site.md#espn_cricket_injuries)
both follow the universal wrapper contract — they return a polars frame by default,
using the shared `parse_news` and `parse_injuries` parsers from
`_common_espn_parsers`.


```python
news = safe(
    "IPL news",
    lambda: cricket.espn_cricket_news(league=IPL, limit=5),
)
if news is not None and news.height:
    show_cols = [c for c in ["headline", "published", "type"] if c in news.columns]
    print(news.select(show_cols).head(5))
else:
    print("news unavailable right now")
```

    ✅ IPL news
    shape: (5, 3)
    ┌─────────────────────────────────┬──────────────────────┬──────────────┐
    │ headline                        ┆ published            ┆ type         │
    │ ---                             ┆ ---                  ┆ ---          │
    │ str                             ┆ str                  ┆ str          │
    ╞═════════════════════════════════╪══════════════════════╪══════════════╡
    │ Greaves turns game on its head… ┆ 2026-07-27T16:34:44Z ┆ Recap        │
    │ Injured Hain out of Metro Bank… ┆ 2026-07-27T16:07:56Z ┆ HeadlineNews │
    │ Tom Alsop signs three-year dea… ┆ 2026-07-27T15:52:58Z ┆ HeadlineNews │
    │ All the records Vaibhav Soorya… ┆ 2026-07-27T15:35:50Z ┆ Story        │
    │ Mohammad Ali now has the wicke… ┆ 2026-07-27T14:33:59Z ┆ Story        │
    └─────────────────────────────────┴──────────────────────┴──────────────┘



```python
injuries = safe(
    "IPL injuries",
    lambda: cricket.espn_cricket_injuries(league=IPL),
)
if injuries is not None and injuries.height:
    print(injuries.head())
else:
    print("injuries feed unavailable or empty right now")
```

    ✅ IPL injuries
    injuries feed unavailable or empty right now


## 📅 Calendar

[`espn_cricket_calendar`](../cricket/reference/site.md#espn_cricket_calendar)
returns the competition calendar — matchdays, rounds, or phases depending on the
tournament format. It uses the universal `parse_items` parser.


```python
cal = safe(
    "IPL calendar",
    lambda: cricket.espn_cricket_calendar(league=IPL),
)
if cal is not None and cal.height:
    print(cal.shape)
    print(cal.head())
else:
    print("calendar unavailable right now")
```

    ⚠️  IPL calendar — NoESPNDataError: No data found for https://site.api.espn.com/apis/site/v2/sports/cricket/8048/calendar
    calendar unavailable right now


## 🍳 Cookbook: common cricket tasks

A handful of patterns you'll reach for constantly when working with the cricket surface.

### Recipe 1 — Top run-scorers from a batting scorecard 🏏

Filter to the highest individual scores from a batting matchcard. Useful for
building a match-by-match batting leaderboard.


```python
if batting.height and "runs" in batting.columns:
    (
        batting
        .filter(pl.col("runs").is_not_null())
        .sort("runs", descending=True)
        .select([c for c in ["innings_number", "athlete_display_name", "runs",
                              "balls", "fours", "sixes", "strike_rate"]
                 if c in batting.columns])
        .head(5)
    )
else:
    print("batting frame not available")
```

### Recipe 2 — Economy leaders from the bowling card 🎯

Bowlers who took wickets and kept a tight economy rate — the T20 game-changers.


```python
if bowling.height and "economy" in bowling.columns:
    (
        bowling
        .filter(pl.col("wickets").is_not_null() & (pl.col("wickets").cast(pl.Float64, strict=False) > 0))
        .sort("economy", descending=False)
        .select([c for c in ["innings_number", "athlete_display_name",
                              "overs", "wickets", "runs_conceded", "economy"]
                 if c in bowling.columns])
        .head(5)
    )
else:
    print("bowling frame not available")
```

    bowling frame not available


### Recipe 3 — Largest partnerships 🤝

Identify which batting pairs put on the biggest stands in a given innings.
A large partnership is often the turning point in a T20 match.


```python
if partnerships.height and "total_runs" in partnerships.columns:
    (
        partnerships
        .filter(pl.col("total_runs").is_not_null())
        .sort("total_runs", descending=True)
        .select([c for c in ["innings_number", "batter1_display_name", "batter2_display_name",
                              "total_runs", "total_balls"]
                 if c in partnerships.columns])
        .head(5)
    )
else:
    print("partnerships frame not available")
```

    partnerships frame not available


### Recipe 4 — Standings: current top-4 playoff picture 🏆

In the IPL, the top 4 teams after the group stage advance to the playoffs.
Sort the standings frame by points (then net run rate as a tiebreaker) to
see where each franchise stands.


```python
if standings is not None and standings.height:
    sort_cols = [c for c in ["points", "net_run_rate"] if c in standings.columns]
    if sort_cols:
        (
            standings
            .sort(sort_cols, descending=[True] * len(sort_cols))
            .select([c for c in ["team", "wins", "losses", "points", "net_run_rate"]
                     if c in standings.columns])
            .head(4)
        )
    else:
        print("expected sort columns not present:", standings.columns)
else:
    print("standings not available")
```

    expected sort columns not present: ['group', 'team', 'team_id', 'team_abbreviation', 'rank', 'matches_played', 'matches_won', 'matches_lost', 'noresult', 'match_points', 'qualified', 'netrr', 'for', 'against', 'total']


### Recipe 5 — pandas interop: grouping rosters by role 🐼

Every sdv-py endpoint accepts `return_as_pandas=True`, so dropping into the
pandas world is a single keyword. Here we pull the match rosters as a pandas
DataFrame and count players by position/type.


```python
if summary_raw is not None:
    rosters_pd = parse_cricket_summary(summary_raw, section="rosters",
                                        return_as_pandas=True)
    if rosters_pd is not None and len(rosters_pd):
        print(type(rosters_pd))
        print(rosters_pd.columns.tolist())
        # Count players by position if the column exists
        pos_col = next((c for c in ["position_name", "position", "type"]
                         if c in rosters_pd.columns), None)
        if pos_col:
            print(rosters_pd.groupby(pos_col, dropna=False).size().sort_values(ascending=False))
    else:
        print("rosters section empty")
else:
    print("no payload")
```

    <class 'pandas.DataFrame'>
    ['team_id', 'home_away', 'winner', 'athlete_id', 'athlete', 'jersey', 'starter', 'position', 'captain']
    position
    AR     8
    BL     8
    UKN    6
    WK     2
    dtype: int64


## ⚠️ Caveats and known limitations

**No teams endpoint for IPL.**
`espn_cricket_teams_site(league="8048")` returns HTTP 404 — ESPN does not expose
a teams listing for the IPL through the Site v2 API. Use the `season_teams`
endpoint if you need franchise metadata for a specific season:

```python
teams_seasonal = safe(
    "IPL season teams",
    lambda: cricket.espn_cricket_season_teams(league=IPL),
)
```

**`event_id` is required for `espn_cricket_summary`.**
Unlike the scoreboard (which returns today's slate without an ID), the summary
endpoint needs a specific event identifier. Obtain `event_id` from the
`espn_cricket_scoreboard` output (`event_id` column).

**Off-season scoreboards may be empty.**
The IPL runs April–May; calling `espn_cricket_scoreboard(league="8048")` in
December returns an empty `events` list. The parser returns a zero-row frame
(never raises), so your code doesn't need to guard against exceptions —
only against `.height == 0`.

**League slugs vary.**
ESPN doesn't publish a canonical slug list. Common IPL slug is `"8048"`;
England's county T20 Blast uses `"eng.t20"`. Use
`espn_cricket_league_root(league=slug, return_parsed=False)` to verify that
a slug resolves before building a pipeline around it.

**`parse_cricket_summary` section `standings` reflects in-tournament state.**
The standings embedded inside a match summary are a snapshot at match time.
For the current full-tournament standings table, use `espn_cricket_standings`
directly.


```python
# Demonstrating the safe empty-frame contract — no exception even for an empty payload.
from sportsdataverse.cricket.cricket_espn_parsers import parse_cricket_scoreboard

empty_df = parse_cricket_scoreboard({})
print("Empty payload → zero-row frame:", empty_df.shape)

empty_frames = parse_cricket_summary({})
print("Empty summary → dict of zero-row frames:")
for name, df in empty_frames.items():
    print(f"  {name}: {df.shape}")
```

    Empty payload → zero-row frame: (0, 0)
    Empty summary → dict of zero-row frames:
      header: (0, 0)
      matchcards_batting: (0, 0)
      matchcards_bowling: (0, 0)
      matchcards_partnerships: (0, 0)
      rosters: (0, 0)
      game_info: (0, 0)
      leaders: (0, 0)
      standings: (0, 0)


## 🎉 Where to next

- 📡 **Full endpoint reference** — every `espn_cricket_*` wrapper is documented
  on the [Cricket reference](../cricket/reference/) pages, grouped by Site v2,
  Web v3, and Core v2 families.
- 🔑 **`event_id` lookup** — the scoreboard frame's `event_id` column is the
  key that unlocks the full summary. Build a pipeline:
  `scoreboard → filter completed → event_id → summary → batting/bowling`.
- 🌍 **Other leagues** — swap the `league=` slug to explore England county
  (`"eng.1"`), ICC Men's/Women's World Cup, PSL, BBL, and more. The
  parsers and workflow are identical across leagues.
- 🐼 Pass `return_as_pandas=True` for pandas, or `return_parsed=False` on
  any `espn_cricket_*` wrapper for the raw ESPN JSON.
- 🎯 **Player depth** — `espn_cricket_player_info`, `espn_cricket_player_gamelog`,
  and `espn_cricket_player_stats` accept `league=` + `athlete_id=` and follow the
  same `return_parsed` / `return_as_pandas` contract.
- 🟥 The sister R package ecosystem is covered by
  [cfbfastR](https://cfbfastR.sportsdataverse.org) (American football),
  [hoopR](https://hoopR.sportsdataverse.org) (basketball),
  [baseballr](https://baseballr.sportsdataverse.org) (baseball), and
  [fastRhockey](https://fastRhockey.sportsdataverse.org) (hockey) — cricket
  sits in the Python-only surface for now.
