"""Capture the NBA prediction-stack oracle/backtest fixture corpus (Phase 0, Task 0.1).

Gitignored working script (``dev/`` is not tracked; see repo CLAUDE.md) --
regenerate the committed fixtures under ``tests/fixtures/nba_prediction/``
with:

    SDV_PY_NBA_STATS_LIVE=1 SDV_PY_LIVE_TESTS=1 uv run python dev/nba_prediction/capture_oracle.py

Captures, for validation season **2023-24** (int ``2024``; stats-API season
string ``"2023-24"``) plus season **2022-23** (int ``2023``; stats-API
``"2022-23"``) for the Phase-4 out-of-sample clutch gate:

* ``results_2024.parquet``            -- ``load_nba_schedule([2024])`` (ESPN release download, any IP)
* ``team_box_2024.parquet``           -- ``load_nba_team_boxscore([2024])`` (ESPN release download)
* ``player_box_logs_2024.parquet``    -- ``load_nba_player_boxscore([2024])`` (ESPN release download)
* ``team_ratings_oracle_2024.parquet``-- ``nba_stats_leaguedashteamstats`` (stats.nba.com, Advanced) + ``espn_nba_season_powerindex`` (BPI, per-team)
* ``espn_predictor_sample_2024.parquet`` -- ``espn_nba_game_predictor`` sampled games (ESPN Core v2)
* ``espn_odds_sample_2024.parquet``   -- ``espn_nba_game_odds`` same sampled games
* ``winprob_sample_2024.parquet``     -- ``winprobabilitypbp`` (stats.nba.com; oracle-only helper, NOT a generated wrapper) for a handful of games
* ``clutch_team_2024.parquet`` / ``clutch_team_2023.parquet`` -- ``nba_stats_leaguedashteamclutch`` (stats.nba.com, Advanced)

stats.nba.com calls MUST go through ``nba_stats_runtime._get`` /
the ``nba_stats_*`` wrappers (curl_cffi impersonate=chrome) -- never
``dl_utils.download()`` (silent timeout). ESPN Core v2 calls are unaffected
by the IP block and use the ordinary wrappers.
"""

from __future__ import annotations

import datetime as dt
import sys
import time
from pathlib import Path

import polars as pl

from sportsdataverse.nba import nba_stats_runtime
from sportsdataverse.nba.nba_espn_ext import (
    espn_nba_game_odds,
    espn_nba_game_predictor,
    espn_nba_season_powerindex,
)
from sportsdataverse.nba.nba_loaders import (
    load_nba_player_boxscore,
    load_nba_schedule,
    load_nba_team_boxscore,
)
from sportsdataverse.nba.nba_stats import (
    nba_stats_leaguedashteamclutch,
    nba_stats_leaguedashteamstats,
)
from sportsdataverse.nba.nba_teams import espn_nba_teams

FIXTURE_DIR = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "nba_prediction"
FIXTURE_DIR.mkdir(parents=True, exist_ok=True)

# WNBA season string == the int year (stats.wnba.com uses "2024", not "2023-24").
WNBA_SEASON_INT = 2024
WNBA_SEASON_STR = "2024"

# int season <-> stats.nba.com season-string crosswalk (pinned at this capture boundary).
SEASON_INT = 2024
SEASON_STR = "2023-24"
PRIOR_SEASON_INT = 2023
PRIOR_SEASON_STR = "2022-23"


def winprobabilitypbp(game_id: str, *, run_type: str = "each second") -> dict:
    """stats.nba.com native win-probability feed (oracle only, scoped -- Decision 6).

    Not a generated ``nba_stats`` wrapper (absent from the capture-confirmed
    112-endpoint surface); this in-game model never *depends* on it, it is a
    concurrent validation oracle only for the Phase-3 gate.
    """
    return nba_stats_runtime._get(
        "winprobabilitypbp",
        params={"GameID": game_id, "RunType": run_type},
    )


def _cast_ids(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    exprs = []
    for c in cols:
        if c in df.columns:
            exprs.append(pl.col(c).cast(pl.Int64, strict=False).cast(pl.Utf8).alias(c))
    return df.with_columns(exprs) if exprs else df


def capture_results_and_box() -> tuple[pl.DataFrame, pl.DataFrame]:
    print("capturing results_2024 + team_box_2024 (ESPN release download)...")
    sched = load_nba_schedule([SEASON_INT])
    sched = sched.rename({"home_id": "home_team_id", "away_id": "away_team_id"})
    if "date" in sched.columns:
        sched = sched.drop("date")
    sched = sched.rename({"game_date": "date"})
    results = (
        sched.filter(pl.col("status_type_completed") == True)  # noqa: E712
        .select(
            pl.col("id").alias("game_id"),
            pl.lit(SEASON_INT, dtype=pl.Int64).alias("season"),
            pl.col("date").cast(pl.Date),
            "home_team_id",
            "away_team_id",
            pl.col("home_score").cast(pl.Int64),
            pl.col("away_score").cast(pl.Int64),
            pl.col("neutral_site").cast(pl.Boolean),
        )
        .unique(subset=["game_id"])
    )
    results = _cast_ids(results, ["game_id", "home_team_id", "away_team_id"])
    results.write_parquet(FIXTURE_DIR / "results_2024.parquet")
    print(f"  results_2024: {results.height} rows")

    box = load_nba_team_boxscore([SEASON_INT])
    team_box = box.select(
        pl.col("game_id"),
        pl.col("team_id"),
        pl.col("field_goals_attempted").cast(pl.Float64),
        pl.col("offensive_rebounds").cast(pl.Float64),
        pl.col("turnovers").cast(pl.Float64),
        pl.col("free_throws_attempted").cast(pl.Float64),
        pl.col("team_score").cast(pl.Float64),
    )
    team_box = _cast_ids(team_box, ["game_id", "team_id"])
    team_box.write_parquet(FIXTURE_DIR / "team_box_2024.parquet")
    print(f"  team_box_2024: {team_box.height} rows")
    return results, team_box


def capture_player_box_logs() -> None:
    print("capturing player_box_logs_2024 (ESPN release download)...")
    pbox = load_nba_player_boxscore([SEASON_INT])
    box = load_nba_team_boxscore([SEASON_INT]).select("game_id", "team_id", pl.col("team_home_away"))
    home_team = box.filter(pl.col("team_home_away") == "home").select(
        "game_id", pl.col("team_id").alias("home_team_id")
    )
    away_team = box.filter(pl.col("team_home_away") == "away").select(
        "game_id", pl.col("team_id").alias("away_team_id")
    )
    logs = (
        pbox.select(
            pl.col("game_id"),
            pl.col("athlete_id").alias("player_id"),
            pl.col("team_id"),
            pl.col("minutes").cast(pl.Float64),
            pl.col("points").cast(pl.Float64).alias("pts") if "points" in pbox.columns else pl.lit(None).alias("pts"),
            pl.col("rebounds").cast(pl.Float64).alias("reb"),
            pl.col("assists").cast(pl.Float64).alias("ast"),
            pl.col("three_point_field_goals_made").cast(pl.Float64).alias("fg3m"),
        )
        .join(home_team, on="game_id", how="left")
        .join(away_team, on="game_id", how="left")
        .with_columns(
            (pl.col("team_id") == pl.col("home_team_id")).alias("is_home"),
            pl.when(pl.col("team_id") == pl.col("home_team_id"))
            .then(pl.col("away_team_id"))
            .otherwise(pl.col("home_team_id"))
            .alias("opp_team_id"),
        )
        .select("game_id", "player_id", "team_id", "opp_team_id", "is_home", "minutes", "pts", "reb", "ast", "fg3m")
    )
    logs = _cast_ids(logs, ["game_id", "player_id", "team_id", "opp_team_id"])
    logs.write_parquet(FIXTURE_DIR / "player_box_logs_2024.parquet")
    print(f"  player_box_logs_2024: {logs.height} rows")


def capture_team_ratings_oracle() -> None:
    """Capture NET_RATING/PACE (stats.nba.com) + BPI (ESPN), keyed to the ESPN team_id.

    stats.nba.com and ESPN use two different team-id systems (stats.nba.com's
    10-digit franchise id, e.g. 1610612737, vs ESPN's small integer, e.g. 1 for
    ATL) -- there is no shared numeric id. The engine's ratings frame is keyed
    on the ESPN team_id (it reads ``load_nba_schedule``/``load_nba_team_boxscore``),
    so this crosswalks stats.nba.com rows to ESPN team_id via the full team
    display name (``"Atlanta Hawks"`` on both sides -- unambiguous for 30 teams,
    unlike the college-basketball name-matching problem).
    """
    print("capturing team_ratings_oracle_2024 (stats.nba.com Advanced + ESPN BPI)...")
    stats = nba_stats_leaguedashteamstats(
        season=SEASON_STR, measure_type_detailed_defense="Advanced", season_type_all_star="Regular Season"
    )
    if isinstance(stats, dict):
        stats = list(stats.values())[0]

    teams = espn_nba_teams()
    name_xwalk = teams.select(pl.col("team_id").alias("espn_team_id"), pl.col("team_display_name").alias("team_name"))

    ora = (
        stats.select(
            pl.col("team_name"),
            pl.col("off_rating").cast(pl.Float64),
            pl.col("def_rating").cast(pl.Float64),
            pl.col("net_rating").cast(pl.Float64),
            pl.col("pace").cast(pl.Float64),
        )
        .join(name_xwalk, on="team_name", how="inner")
        .rename({"espn_team_id": "team_id", "team_name": "team"})
    )
    unmatched = stats.height - ora.height
    if unmatched:
        print(f"  WARNING: {unmatched} stats.nba.com teams failed to crosswalk to an ESPN team_id")

    team_ids = teams["team_id"].to_list()
    bpi_rows = []
    for tid in team_ids:
        try:
            raw = espn_nba_season_powerindex(SEASON_INT, team_id=tid, return_parsed=False)
        except Exception as exc:  # pragma: no cover - live network
            print(f"  BPI fetch failed for team_id={tid}: {exc}")
            continue
        stat_list = raw.get("stats") if isinstance(raw, dict) else None
        bpi_val = None
        if stat_list:
            bpi_val = next((s.get("value") for s in stat_list if s.get("name") == "bpi"), None)
        bpi_rows.append({"team_id": str(int(tid)), "bpi": float(bpi_val) if bpi_val is not None else None})
        time.sleep(0.15)
    bpi = pl.DataFrame(bpi_rows) if bpi_rows else pl.DataFrame(schema={"team_id": pl.Utf8, "bpi": pl.Float64})

    out = (
        ora.join(bpi, on="team_id", how="left")
        .with_columns(pl.col("net_rating").rank(method="dense", descending=True).cast(pl.Int64).alias("rank"))
        .select("team_id", "team", "off_rating", "def_rating", "net_rating", "pace", "bpi", "rank")
    )
    out.write_parquet(FIXTURE_DIR / "team_ratings_oracle_2024.parquet")
    print(f"  team_ratings_oracle_2024: {out.height} rows ({out['bpi'].null_count()} missing BPI)")


def capture_espn_samples(results: pl.DataFrame, *, every: int = 25) -> None:
    """Sample games' pregame predictor win-prob + closing odds (raw-payload parsed).

    ESPN's ``predictor`` payload only ever populates ``awayTeam.statistics``
    (``homeTeam`` carries just the team ``$ref``, confirmed across multiple
    real games) -- ``home_win_prob = 1 - away gameProjection/100``. The
    flattened/parsed frame stringifies that nested stat list, so this reads
    the raw JSON directly instead. ``odds`` ships one row per sportsbook
    provider; the first (highest-priority) row's home-anchored
    ``homeTeamOdds.close.pointSpread`` / top-level ``total.close`` are used.
    """
    print("capturing espn_predictor_sample_2024 + espn_odds_sample_2024...")
    sample = results.sort("date").with_row_index("i").filter(pl.col("i") % every == 0)
    pred_rows, odds_rows = [], []
    for row in sample.iter_rows(named=True):
        gid = row["game_id"]
        try:
            raw = espn_nba_game_predictor(gid, return_parsed=False)
            away_stats = (raw.get("awayTeam") or {}).get("statistics") or []
            away_proj = next((s.get("value") for s in away_stats if s.get("name") == "gameProjection"), None)
            if away_proj is not None:
                pred_rows.append(
                    {
                        "game_id": gid,
                        "home_team_id": row["home_team_id"],
                        "away_team_id": row["away_team_id"],
                        "home_win_prob": 1.0 - float(away_proj) / 100.0,
                    }
                )
        except Exception as exc:  # pragma: no cover - live network
            print(f"  predictor fetch failed for game_id={gid}: {exc}")
        try:
            raw_odds = espn_nba_game_odds(gid, return_parsed=False)
            items = raw_odds.get("items") if isinstance(raw_odds, dict) else None
            book = (
                items[0] if items else (raw_odds if isinstance(raw_odds, dict) and "homeTeamOdds" in raw_odds else None)
            )
            if book is not None:
                pt_spread = ((book.get("homeTeamOdds") or {}).get("close") or {}).get("pointSpread") or {}
                home_spread = pt_spread.get("value")
                close_total = book.get("overUnder")
                odds_rows.append({"game_id": gid, "close_spread_home": home_spread, "close_total": close_total})
        except Exception as exc:  # pragma: no cover - live network
            print(f"  odds fetch failed for game_id={gid}: {exc}")
        time.sleep(0.1)

    pred_df = (
        pl.DataFrame(pred_rows)
        if pred_rows
        else pl.DataFrame(
            schema={"game_id": pl.Utf8, "home_team_id": pl.Utf8, "away_team_id": pl.Utf8, "home_win_prob": pl.Float64}
        )
    )
    pred_df.write_parquet(FIXTURE_DIR / "espn_predictor_sample_2024.parquet")
    print(f"  espn_predictor_sample_2024: {pred_df.height} rows")

    odds_df = (
        pl.DataFrame(odds_rows)
        if odds_rows
        else pl.DataFrame(schema={"game_id": pl.Utf8, "close_spread_home": pl.Float64, "close_total": pl.Float64})
    )
    odds_df.write_parquet(FIXTURE_DIR / "espn_odds_sample_2024.parquet")
    print(f"  espn_odds_sample_2024: {odds_df.height} rows")


def _espn_to_stats_game_id_crosswalk(results: pl.DataFrame, n_games: int) -> pl.DataFrame:
    """Map a tail sample of ESPN games to their stats.nba.com GAME_ID.

    stats.nba.com and ESPN use unrelated game-id systems (e.g. stats.nba.com
    ``"0022300XXX"`` vs ESPN ``401584690``) with no shared numeric key, so
    this joins on ``(game_date, home team abbreviation)`` -- both sourced
    from ``nba_stats_leaguegamefinder`` (home rows have a ``"vs."`` matchup)
    and ``espn_nba_teams`` (abbreviation for the ESPN ``home_team_id``).
    """
    from sportsdataverse.nba.nba_stats import nba_stats_leaguegamefinder  # noqa: PLC0415

    # nba_stats_leaguegamefinder defaults to Regular Season only; restrict the sample to
    # regular-season dates so the (date, home_abbrev) crosswalk actually has a match.
    reg_season_cutoff = dt.date(2024, 4, 15)
    tail = results.filter(pl.col("date") < reg_season_cutoff).sort("date").tail(n_games)
    teams = espn_nba_teams().select(
        pl.col("team_id").alias("home_team_id"), pl.col("team_abbreviation").alias("home_abbrev")
    )
    espn_side = tail.join(teams, on="home_team_id", how="left")

    gf = nba_stats_leaguegamefinder(season_nullable=SEASON_STR, league_id="00", player_or_team_abbreviation="T")
    home_rows = gf.filter(pl.col("matchup").str.contains("vs.")).select(
        pl.col("game_id").alias("stats_game_id"),
        pl.col("game_date").cast(pl.Date).alias("date"),
        pl.col("team_abbreviation").alias("home_abbrev"),
    )
    return espn_side.join(home_rows, on=["date", "home_abbrev"], how="inner")


def capture_winprob_sample(results: pl.DataFrame, *, n_games: int = 8) -> None:
    """Capture the ``winprobabilitypbp`` concurrent-oracle sample.

    FINDING (2026-07-08): ``winprobabilitypbp`` is a DEAD stats.nba.com
    endpoint, not merely uncaptured. Confirmed two ways: (1) the correctly
    zero-padded ``GameID`` + valid ``RunType`` params (matching hoopR's own
    request shape) return HTTP 500 with an empty body for every game tried
    (playoff and regular-season alike); (2) hoopR's own
    ``nba_winprobabilitypbp()`` (``R/nba_stats_scoreboard.R``) is itself
    ``lifecycle::deprecate_stop()``-ed as of hoopR 3.0.0, replaced by
    ``nba_playbyplayv3()`` -- i.e. the upstream sibling package no longer
    calls this endpoint either. This fixture is therefore written with the
    documented zero-row schema; the Phase-3 in-game-WP gate's *(b)* "MAE vs
    native winprobabilitypbp" concurrent check is not obtainable from this
    endpoint and must rely on gate *(a)* (per-bucket realized-outcome
    calibration) alone, or a substitute oracle if one is found later.
    """
    print("capturing winprob_sample_2024 (stats.nba.com winprobabilitypbp)...")
    xwalk = _espn_to_stats_game_id_crosswalk(results, n_games)
    rows = []
    for row in xwalk.iter_rows(named=True):
        gid, stats_gid = row["game_id"], row["stats_game_id"]
        try:
            resp = winprobabilitypbp(stats_gid)
        except Exception as exc:  # pragma: no cover - live network
            print(f"  winprobabilitypbp failed for game_id={gid} (stats={stats_gid}): {exc}")
            continue
        result_sets = resp.get("resultSets") or resp.get("resultSet") or []
        if isinstance(result_sets, dict):
            result_sets = [result_sets]
        target = next((r for r in result_sets if "winprob" in r.get("name", "").lower()), None)
        if target is None or not target.get("rowSet"):
            print(f"  no WinProbPBP rows for game_id={gid} (stats={stats_gid})")
            continue
        headers = [h.upper() for h in target["headers"]]
        for i, r in enumerate(target["rowSet"]):
            d = dict(zip(headers, r))
            home_pt = d.get("HOME_PCT")
            if home_pt is None:
                continue
            rows.append(
                {
                    "game_id": gid,
                    "event_num": int(d.get("EVENT_NUM", i)),
                    "sec_left": float(d.get("SECONDS_REMAINING", 0.0) or 0.0),
                    "score_diff": int(d.get("HOME_MARGIN", d.get("HOME_PT", 0)) or 0),
                    "home_pct": float(home_pt),
                }
            )
        time.sleep(0.2)
    wp = (
        pl.DataFrame(rows)
        if rows
        else pl.DataFrame(
            schema={
                "game_id": pl.Utf8,
                "event_num": pl.Int64,
                "sec_left": pl.Float64,
                "score_diff": pl.Int64,
                "home_pct": pl.Float64,
            }
        )
    )
    wp.write_parquet(FIXTURE_DIR / "winprob_sample_2024.parquet")
    print(f"  winprob_sample_2024: {wp.height} rows")


def capture_clutch(season_int: int, season_str: str, out_name: str) -> None:
    print(f"capturing {out_name} (stats.nba.com leaguedashteamclutch Advanced)...")
    clutch = nba_stats_leaguedashteamclutch(season=season_str, measure_type_detailed_defense="Advanced")
    if isinstance(clutch, dict):
        clutch = list(clutch.values())[0]
    out = clutch.select(
        pl.lit(season_int, dtype=pl.Int64).alias("season"),
        pl.col("team_id"),
        pl.col("off_rating").cast(pl.Float64).alias("clutch_off_rating"),
        pl.col("def_rating").cast(pl.Float64).alias("clutch_def_rating"),
        pl.col("net_rating").cast(pl.Float64).alias("clutch_net_rating"),
        pl.col("poss").cast(pl.Float64).alias("clutch_poss")
        if "poss" in clutch.columns
        else pl.lit(None).alias("clutch_poss"),
    )
    out = _cast_ids(out, ["team_id"])
    out.write_parquet(FIXTURE_DIR / out_name)
    print(f"  {out_name}: {out.height} rows")


def capture_team_net(season_int: int, season_str: str, out_name: str) -> None:
    """Full-game team net_rating keyed on stats.nba.com team_id (the clutch baseline).

    Same id system as the clutch fixtures (10-digit franchise id), so the
    clutch-delta join needs no stats<->ESPN crosswalk. Both this net_rating and
    the clutch net_rating are un-opponent-adjusted, so the delta is like-with-like.
    """
    print(f"capturing {out_name} (stats.nba.com leaguedashteamstats Advanced full-game net)...")
    stats = nba_stats_leaguedashteamstats(season=season_str, measure_type_detailed_defense="Advanced")
    if isinstance(stats, dict):
        stats = list(stats.values())[0]
    out = stats.select(
        pl.lit(season_int, dtype=pl.Int64).alias("season"),
        pl.col("team_id"),
        pl.col("net_rating").cast(pl.Float64).alias("adj_net_rtg"),
    )
    out = _cast_ids(out, ["team_id"])
    out.write_parquet(FIXTURE_DIR / out_name)
    print(f"  {out_name}: {out.height} rows")


def capture_pbp_sample(*, n_games: int = 400, play_stride: int = 30) -> None:
    """Capture the Phase-3 in-game-WP calibration sample: sampled 2024 plays + as-of
    pregame home prob + realized home_win label.

    Each sampled game's ``pregame_home_prob`` is the leakage-safe as-of-date pregame
    prediction (``nba_team_ratings(..., as_of_date=game.date)`` then
    ``nba_predict_games``), so the calibration gate is honest. Every ``play_stride``-th
    play of each game is kept (a few plays from many games beats every play from few
    games -- plays within a game are correlated). Columns match ``load_nba_pbp`` so
    ``in_game_features`` runs against it offline: ``game_id, start_game_seconds_remaining,
    home_score, away_score, team_id, home_team_id`` + ``pregame_home_prob, home_win``.
    """
    from sportsdataverse.nba.nba_game_predict import nba_predict_games
    from sportsdataverse.nba.nba_loaders import load_nba_pbp
    from sportsdataverse.nba.nba_team_ratings import nba_team_ratings

    print("capturing pbp_sample_2024 (in-game-WP calibration; ESPN release download)...")
    results = _load_results()
    # sample regular-season games with a decent as-of warmup (skip the first ~200 games)
    warmup_cut = results.sort("date")["date"][200]
    pool = results.filter((pl.col("date") >= warmup_cut) & (pl.col("date") < dt.date(2024, 4, 15))).sort("date")
    sample = pool.gather_every(max(1, pool.height // n_games)).head(n_games)

    pbp_all = load_nba_pbp([SEASON_INT]).with_columns(pl.col("game_id").cast(pl.Int64, strict=False).cast(pl.Utf8))

    frames = []
    for g in sample.iter_rows(named=True):
        ratings = nba_team_ratings(SEASON_INT, league_id="00", as_of_date=g["date"])
        one = pl.DataFrame(
            {
                "game_id": [g["game_id"]],
                "home_team_id": [g["home_team_id"]],
                "away_team_id": [g["away_team_id"]],
                "neutral_site": [g["neutral_site"]],
            }
        )
        pr = nba_predict_games(one, ratings, league_id="00")
        if pr.height == 0 or pr["home_win_prob"][0] is None:
            continue
        pregame = float(pr["home_win_prob"][0])
        home_win = int(g["home_score"] > g["away_score"])
        gp = (
            pbp_all.filter(pl.col("game_id") == g["game_id"])
            .select(
                "game_id",
                pl.col("start_game_seconds_remaining").cast(pl.Float64),
                pl.col("home_score").cast(pl.Int64),
                pl.col("away_score").cast(pl.Int64),
                pl.col("team_id").cast(pl.Int64, strict=False).cast(pl.Utf8),
                pl.col("home_team_id").cast(pl.Int64, strict=False).cast(pl.Utf8),
            )
            .gather_every(play_stride)
            .with_columns(
                pl.lit(pregame, dtype=pl.Float64).alias("pregame_home_prob"),
                pl.lit(home_win, dtype=pl.Int64).alias("home_win"),
            )
        )
        frames.append(gp)

    out = (
        pl.concat(frames)
        if frames
        else pl.DataFrame(
            schema={
                "game_id": pl.Utf8,
                "start_game_seconds_remaining": pl.Float64,
                "home_score": pl.Int64,
                "away_score": pl.Int64,
                "team_id": pl.Utf8,
                "home_team_id": pl.Utf8,
                "pregame_home_prob": pl.Float64,
                "home_win": pl.Int64,
            }
        )
    )
    out.write_parquet(FIXTURE_DIR / "pbp_sample_2024.parquet")
    print(f"  pbp_sample_2024: {out.height} rows from {out['game_id'].n_unique()} games")


def capture_wnba() -> None:
    """WNBA (league_id=10) oracle corpus for the Phase-6 gates (season 2024).

    Mirrors the NBA capture: results + team_box (ESPN release, ESPN wnba team_id)
    and a team-ratings oracle (stats.wnba.com net_rating/pace crosswalked to the
    ESPN wnba team_id by full team display name -- same two-id-system problem as
    the NBA capture). WNBA plays a 40-minute game; stats.wnba.com season string
    is the plain year "2024".
    """
    from sportsdataverse.wnba.wnba_loaders import load_wnba_schedule, load_wnba_team_boxscore
    from sportsdataverse.wnba.wnba_stats import wnba_stats_leaguedashteamstats
    from sportsdataverse.wnba.wnba_teams import espn_wnba_teams

    print("capturing wnba_results_2024 + wnba_team_box_2024 (ESPN release)...")
    sched = load_wnba_schedule([WNBA_SEASON_INT]).rename({"home_id": "home_team_id", "away_id": "away_team_id"})
    if "date" in sched.columns:
        sched = sched.drop("date")
    sched = sched.rename({"game_date": "date"})
    results = (
        sched.filter(pl.col("status_type_completed") == True)  # noqa: E712
        .select(
            pl.col("id").alias("game_id"),
            pl.lit(WNBA_SEASON_INT, dtype=pl.Int64).alias("season"),
            pl.col("date").cast(pl.Date),
            "home_team_id",
            "away_team_id",
            pl.col("home_score").cast(pl.Int64),
            pl.col("away_score").cast(pl.Int64),
            pl.col("neutral_site").cast(pl.Boolean),
        )
        .unique(subset=["game_id"])
    )
    results = _cast_ids(results, ["game_id", "home_team_id", "away_team_id"])
    results.write_parquet(FIXTURE_DIR / "wnba_results_2024.parquet")
    print(f"  wnba_results_2024: {results.height} rows")

    box = load_wnba_team_boxscore([WNBA_SEASON_INT])
    team_box = _cast_ids(
        box.select(
            "game_id",
            "team_id",
            pl.col("field_goals_attempted").cast(pl.Float64),
            pl.col("offensive_rebounds").cast(pl.Float64),
            pl.col("turnovers").cast(pl.Float64),
            pl.col("free_throws_attempted").cast(pl.Float64),
            pl.col("team_score").cast(pl.Float64),
        ),
        ["game_id", "team_id"],
    )
    team_box.write_parquet(FIXTURE_DIR / "wnba_team_box_2024.parquet")
    print(f"  wnba_team_box_2024: {team_box.height} rows")

    print("capturing wnba_team_ratings_oracle_2024 (stats.wnba.com Advanced)...")
    stats = wnba_stats_leaguedashteamstats(season=WNBA_SEASON_STR, measure_type_detailed_defense="Advanced")
    if isinstance(stats, dict):
        stats = list(stats.values())[0]
    teams = espn_wnba_teams()
    name_xwalk = teams.select(pl.col("team_id").alias("espn_team_id"), pl.col("team_display_name").alias("team_name"))
    ora = (
        stats.select(
            pl.col("team_name"),
            pl.col("off_rating").cast(pl.Float64),
            pl.col("def_rating").cast(pl.Float64),
            pl.col("net_rating").cast(pl.Float64),
            pl.col("pace").cast(pl.Float64),
        )
        .join(name_xwalk, on="team_name", how="inner")
        .rename({"espn_team_id": "team_id", "team_name": "team"})
        .with_columns(pl.col("net_rating").rank(method="dense", descending=True).cast(pl.Int64).alias("rank"))
        .select("team_id", "team", "off_rating", "def_rating", "net_rating", "pace", "rank")
    )
    ora = _cast_ids(ora, ["team_id"])
    unmatched = stats.height - ora.height
    if unmatched:
        print(f"  WARNING: {unmatched} stats.wnba.com teams failed to crosswalk to an ESPN wnba team_id")
    ora.write_parquet(FIXTURE_DIR / "wnba_team_ratings_oracle_2024.parquet")
    print(f"  wnba_team_ratings_oracle_2024: {ora.height} rows")


def _load_results() -> pl.DataFrame:
    """Load the already-captured ``results_2024`` fixture (for a solo step re-run)."""
    return pl.read_parquet(FIXTURE_DIR / "results_2024.parquet")


def main() -> None:
    steps = sys.argv[1:] or ["all"]
    results = None
    if "all" in steps or "base" in steps:
        results, _team_box = capture_results_and_box()
        capture_player_box_logs()
    if "all" in steps or "ratings" in steps:
        capture_team_ratings_oracle()
    if "all" in steps or "espn" in steps:
        capture_espn_samples(results if results is not None else _load_results())
    if "all" in steps or "wp" in steps:
        capture_winprob_sample(results if results is not None else _load_results())
    if "all" in steps or "clutch" in steps:
        capture_clutch(PRIOR_SEASON_INT, PRIOR_SEASON_STR, "clutch_team_2023.parquet")
        capture_clutch(SEASON_INT, SEASON_STR, "clutch_team_2024.parquet")
        capture_team_net(PRIOR_SEASON_INT, PRIOR_SEASON_STR, "team_net_2023.parquet")
        capture_team_net(SEASON_INT, SEASON_STR, "team_net_2024.parquet")
    if "all" in steps or "pbp" in steps:
        capture_pbp_sample()
    if "all" in steps or "wnba" in steps:
        capture_wnba()
    print("done:", dt.datetime.now().isoformat())


if __name__ == "__main__":
    main()
