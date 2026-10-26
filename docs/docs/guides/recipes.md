---
title: How-to recipes
sidebar_label: How-to recipes
sidebar_position: 4
description: "Short, complete sportsdataverse-py recipes for common tasks: finding ids, joining sources, loading seasons and more."
---

# How-to recipes

Short, complete recipes for the tasks people ask about most. Each block runs as
written against the live sources; later blocks in a section reuse names from the
earlier ones.

## Join play-by-play to rosters by id

Join on ids, never on names, and check the dtypes on both sides first. The
[IDs and joins](ids-and-joins.md) page explains which dtype each source uses.

**College football.** The released CFB play-by-play and rosters both carry ESPN
athlete ids as `Int64`. A roster has one row per team and athlete per season, so
keep one row per `(season, athlete_id)` before joining.

```python
import polars as pl
from sportsdataverse.cfb import load_cfb_pbp, load_cfb_rosters

pbp = load_cfb_pbp(2024)
rosters = load_cfb_rosters(2024).unique(["season", "athlete_id"], keep="first")

passers = rosters.select(
    "season",
    pl.col("athlete_id").alias("passer_player_id"),
    pl.col("athlete_display_name").alias("passer"),
    pl.col("position_abbreviation").alias("passer_position"),
)
assert pbp.schema["passer_player_id"] == passers.schema["passer_player_id"]

dropbacks = pbp.filter(pl.col("pass") == True).join(
    passers, on=["season", "passer_player_id"], how="left", validate="m:1"
)
print(
    dropbacks.group_by("passer", "pos_team")
    .agg(pl.len().alias("dropbacks"), pl.col("EPA").mean().alias("epa_per_dropback"))
    .filter(pl.col("dropbacks") >= 300)
    .sort("epa_per_dropback", descending=True)
    .head(5)
)
```

**NFL.** nflverse play-by-play and rosters key players on the gsis id, which is
text by design (`"00-0034796"`), so both sides are String and join as they are.

```python
from sportsdataverse.nfl import load_nfl_pbp, load_nfl_rosters

nfl_pbp = load_nfl_pbp([2024])
nfl_rosters = load_nfl_rosters([2024]).unique(["season", "gsis_id"], keep="first")

receivers = nfl_rosters.select(
    "season",
    pl.col("gsis_id").alias("receiver_player_id"),
    pl.col("position").alias("receiver_position"),
)
assert nfl_pbp.schema["receiver_player_id"] == receivers.schema["receiver_player_id"]

targets = nfl_pbp.filter(pl.col("receiver_player_id").is_not_null()).join(
    receivers, on=["season", "receiver_player_id"], how="left", validate="m:1"
)
print(targets.group_by("receiver_position").agg(pl.col("epa").mean(), pl.len()).sort("len", descending=True))
```

nflverse rosters also carry `espn_id`, as String. To join them to ESPN-sourced frames,
whose ids are `Int64`, cast it first: `pl.col("espn_id").cast(pl.Int64, strict=False)`.

## Build a season of EPA

### NFL from the nflverse release

The quickest season of EPA is the nflverse play-by-play release, which already
carries nflfastR's `epa`, `wpa` and the rest.

```python
team_epa = (
    nfl_pbp.filter(pl.col("pass") == 1)
    .group_by("posteam")
    .agg(pl.col("epa").mean().alias("epa_per_dropback"), pl.len().alias("plays"))
    .sort("epa_per_dropback", descending=True)
)
print(team_epa.head(5))
```

### NFL scored by sdv-py's own models

`enrich_nfl_pbp` re-scores an nflverse-shape frame with the models bundled in the
package, in nflfastR's order: EP, EPA, WP, WPA, CP, CPOE and xYAC. The default
`method="lead_diff"` is a port of nflfastR's `helper_add_ep_wp.R`. It is the way to
score frames the release does not cover, such as a season rebuilt from NFL.com Shield
JSON with `build_nfl_season(seasons=[...], source="shield", raw_dir=...)`. A full
season takes about a minute.

```python
from sportsdataverse.nfl.ep_wp import enrich_nfl_pbp

scored = enrich_nfl_pbp(nfl_pbp)
both = pl.DataFrame({"nflverse": nfl_pbp["epa"], "sdv": scored["epa"]}).drop_nulls()
print(both.select(pl.corr("nflverse", "sdv")))   # about 0.99
```

Pass `strict=True` (or set `SDV_PY_MODELS_STRICT=1`) to raise when a model file is
missing instead of nulling its columns.

### NFL from ESPN games

`build_nfl_season` with `source="espn"` (the default) processes ESPN games one by
one through `NFLPlayProcess`, caches each processed game, and concatenates them. A
game that fails is logged and skipped. The ESPN frame uses ESPN-style names: `EPA`,
`wpa`, `home_wp_before`, and `pos_team` as an `Int64` team id.

```python
from sportsdataverse import find_event
from sportsdataverse.nfl import build_nfl_season

super_bowl = int(find_event(date="20250209", league="nfl")["id"])
espn_plays = build_nfl_season(game_ids=[super_bowl])
print(espn_plays.group_by("pos_team").agg(pl.col("EPA").mean()))
```

### College football

The released CFB play-by-play carries EPA and WP for every game of a season. For a
single game, or one the release does not hold yet, process it with `CFBPlayProcess`.

```python
cfb_team_epa = (
    pbp.filter(pl.col("scrimmage_play") == True)
    .group_by("pos_team")
    .agg(pl.col("EPA").mean().alias("epa_per_play"), pl.len().alias("plays"))
    .filter(pl.col("plays") >= 500)
    .sort("epa_per_play", descending=True)
)
print(cfb_team_epa.head(5))
```

```python
from sportsdataverse.cfb import CFBPlayProcess

game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
game.run_processing_pipeline()
plays = game.plays_frame                     # one row per play, with EPA, wp_before, ...
print(plays.select("game_play_number", "pos_team", "EPA", "home_wp_before").tail(3))
```

## Draw a win-probability chart

ESPN-processed games carry `home_wp_before`, the home side's win probability before
each play. You need matplotlib (`pip install matplotlib`; the `sportypy` extra and
`sdvplot[mpl]` also pull it).

```python
import matplotlib.pyplot as plt

wp = espn_plays.select("game_play_number", "home_wp_before", "homeTeamAbbrev", "awayTeamAbbrev").drop_nulls(
    "home_wp_before"
)
home, away = wp["homeTeamAbbrev"][0], wp["awayTeamAbbrev"][0]

fig, ax = plt.subplots(figsize=(9, 4))
ax.plot(wp["game_play_number"], wp["home_wp_before"], color="black")
ax.axhline(0.5, color="grey", linewidth=0.8, linestyle="--")
ax.set_ylim(0, 1)
ax.set_xlabel("Play number")
ax.set_ylabel(f"{home} win probability")
ax.set_title(f"{away} at {home}")
fig.savefig("wp.png", dpi=120, bbox_inches="tight")
```

With [sdvplot](https://github.com/sportsdataverse/sdvplot)
(`pip install "sportsdataverse[sdvplot]"`), the same chart takes each side's team
color. `sdvplot.team_colors` resolves abbreviations, ESPN ids or names to hex colors.

```python
import sdvplot

home_color, away_color = sdvplot.team_colors("nfl", [home, away])
x, y = wp["game_play_number"].to_numpy(), wp["home_wp_before"].to_numpy()

fig, ax = plt.subplots(figsize=(9, 4))
ax.fill_between(x, 0.5, y, where=y >= 0.5, interpolate=True, color=home_color, label=home)
ax.fill_between(x, 0.5, y, where=y < 0.5, interpolate=True, color=away_color, label=away)
ax.set_ylim(0, 1)
ax.set_xlabel("Play number")
ax.set_ylabel(f"{home} win probability")
ax.legend(loc="upper left")
fig.savefig("wp_team_colors.png", dpi=120, bbox_inches="tight")
```

`sdvplot.matplotlib` also has `add_logos` and `title_image` to put team logos on the
axes or in the title.

## NFL cache and configuration

The `load_nfl_*` loaders share one cache and one configuration (an `NflConfig`).
There are three cache modes:

| Mode | Storage | Lifetime |
|---|---|---|
| `memory` (default) | a per-process dict | `cache_duration` seconds |
| `filesystem` | parquet files under `cache_dir` | `cache_duration` seconds, across sessions |
| `off` | none | every call fetches |

```python
from sportsdataverse.nfl import clear_cache, get_config, reset_config, update_config

update_config(cache_mode="filesystem", cache_duration=3600)   # keep frames on disk for an hour
print(get_config())
clear_cache()                                                 # wipes the memory and filesystem entries
update_config(cache_mode="off")                               # every call fetches
reset_config()                                                # back to the environment / defaults
```

An unknown mode raises `ValueError`. The cache key is the loader's name and
arguments, excluding `return_as_pandas` (one stored polars frame serves both), and
it does not include the URL: after a release file is republished, call
`clear_cache()`.

The starting values come from environment variables, which an explicit
`update_config()` overrides:

| Variable | Sets |
|---|---|
| `SDV_PY_NFL_CACHE` | the mode: `memory`, `filesystem` or `off` |
| `SDV_PY_NFL_CACHE_DIR` | the filesystem cache directory |
| `SDV_PY_NFL_CACHE_DURATION` | the lifetime in seconds |
| `SDV_PY_NFL_VERBOSE` | progress output on or off |
| `SDV_PY_NFL_TIMEOUT` | the HTTP timeout in seconds |

The package-wide HTTP response cache is separate. It caches raw responses from the
live wrappers by URL and parameters, with a lifetime chosen per endpoint (30 days
for a finished game's play-by-play, 5 minutes for news and injuries, never for a live
scoreboard or an in-progress game), and it is off by default. Control it from Python or with `sdv cache` on the
[command line](install-and-cli.md#the-sdv-command-line):

```python
import sportsdataverse as sdv

sdv.set_cache_mode("memory")     # "off" (default), "memory" or "filesystem"
print(sdv.cache_stats())
sdv.set_cache_mode("off")
```

## Rebuild a CFB game offline

`CFBPlayProcess` can rebuild a game's processed plays from a saved ESPN summary
without touching the network. That is how a raw library is reprocessed after a
parser fix. Two things would otherwise make requests: joining the per-play
participants, and looking up the betting line when the summary's `pickcenter` has
fewer than two providers (it is empty for 2024 and later games). The spread and total
are EPA and WP inputs, so persist them with the raw JSON and pass them back as
`odds_override`, a dict with `gameSpread`, `overUnder`, `homeFavorite`,
`gameSpreadAvailable` and, optionally, `overUnderAvailable`.

First capture the summary once:

```python
import json
import tempfile
from pathlib import Path

raw_dir = Path(tempfile.mkdtemp())
summary = CFBPlayProcess(gameId=401628334, raw=True).espn_cfb_pbp()
(raw_dir / "401628334.json").write_text(json.dumps(summary), encoding="utf-8")

odds = {"gameSpread": -4.0, "overUnder": 58.5, "homeFavorite": True, "gameSpreadAvailable": True, "overUnderAvailable": True}
```

Then rebuild from the dict with `espn_cfb_pbp(summary=...)`, which skips the
participant requests and reads the odds from the summary or the override:

```python
offline = CFBPlayProcess(gameId=401628334, odds_override=odds)
offline.espn_cfb_pbp(summary=summary)
offline.run_processing_pipeline()
print(offline.odds_source, offline.plays_frame.height)   # injected 169
```

Or from the file on disk with `cfb_pbp_disk()`. Here `join_participants=False` is
required, because reading from disk alone does not switch off the participant
requests:

```python
from_disk = CFBPlayProcess(gameId=401628334, path_to_json=str(raw_dir), join_participants=False, odds_override=odds)
from_disk.cfb_pbp_disk()
from_disk.run_processing_pipeline()
```

`odds_source` records where the line came from: `"summary_pickcenter"`,
`"core_odds_api"`, `"default"` or `"injected"`. A malformed `odds_override` raises
`ValueError` in the constructor. Leave `overUnderAvailable` out only when you do not
know whether the total is a market one; it is then null, never guessed.

## Release loaders or live wrappers?

The package reaches data two ways.

- **Release loaders** (`load_<lg>_*`) read parquet files that SportsDataverse
  producers publish to GitHub releases. A whole season comes back in one fast
  request, already processed (EPA, WP, cleaned names), and the same call returns the
  same frame tomorrow. They lag the live game by the producer's schedule, usually a
  day, and some keep the dtypes their files were published with.
- **Live wrappers** (`espn_<lg>_*`, `nhl_web_*`, `pwhl_*`, the provider families) call
  the source's API when you call them. They are current to the minute and reach
  endpoints no release covers, but you make one request per game or page, and the
  answer can change.

| You want | Use |
|---|---|
| A full season or several, for analysis or modeling | A release loader: `load_cfb_pbp(2024)`, `load_nfl_pbp([2024])` |
| Tonight's games, live scores, today's injuries | A live wrapper: `espn_nba_scoreboard()`, `espn_nfl_injuries()` |
| A game the release does not hold yet | A live wrapper, or the processor (`CFBPlayProcess`, `build_nfl_season`) |
| A reproducible pipeline | Release loaders, or live data captured once and rebuilt offline |
| An endpoint with no release (standings, odds, team pages) | A live wrapper |

```python
from sportsdataverse.cfb import espn_cfb_schedule, load_cfb_schedule

season = load_cfb_schedule([2024])                  # every 2024 game, one request
week_one = espn_cfb_schedule(dates=2024, week=1)    # ESPN's live schedule for one week
print(season.height, week_one.height)
```

A season below a loader's floor raises `SeasonNotFoundError` before any request. A
season the release has no file for is skipped by most loaders and raises `NoDataError`
in a few (the NFL loaders among them); a download that failed always raises
`AssetFetchError`, never an empty frame. See [Troubleshooting](troubleshooting.md).
