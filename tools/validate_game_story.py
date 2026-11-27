"""Score the game-story validation gates on one league-season of real games.

Gates (pre-registered in the game-story design spec):

* G1 edge direction -- on decided games, the rank-1 edge has ``points_worth > 0``. Prints the share, its
  95% Wilson lower bound and the X that bound sets (rounded down to a whole percent).
* G2 moment -- on games with any ``wpa``, the moment is the max-``|wpa|`` play recomputed here from the
  input (earliest on ties). Must be 100%.
* G3 periods -- on games whose play-by-play reaches the official final, the period sums equal it. Must be
  100%; games whose source ends on another score are counted and listed.

It also prints how far each game's accounting edges are from the margin (exact in basketball).

Usage::

    uv run python tools/validate_game_story.py --league nba --season 2025 [--data-dir DIR] [--out results.json]

``--data-dir`` reads ``<lg>_{pbp,team_boxscore,player_boxscore}_<season>.parquet`` (basketball) or
``espn_<lg>_pbp_<season>.parquet`` (football) from a local directory instead of the release loaders.
The NFL's official final is the nflverse schedule's score (joined on ``nflverse_game_id``).
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path

import polars as pl

_BASKETBALL = ("nba", "wnba", "mbb", "wbb")
_FOOTBALL = ("cfb", "nfl")
_RELEASE = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"


def wilson_lower(passed: int, n: int, z: float = 1.959963984540054) -> float:
    """The 95% Wilson score lower bound of ``passed / n``."""
    if n == 0:
        return float("nan")
    p = passed / n
    den = 1 + z * z / n
    centre = p + z * z / (2 * n)
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (centre - half) / den


def _basketball(lg: str, season: int, data_dir: Path | None) -> tuple[dict, pl.DataFrame, pl.DataFrame]:
    import importlib

    from sportsdataverse.nba.nba_game_page import game_regulation_periods, score_win_prob
    from sportsdataverse.nba.nba_game_story import nba_game_story

    if data_dir:
        pbp, tb, pb = (
            pl.read_parquet(data_dir / f"{lg}_{k}_{season}.parquet")
            for k in ("pbp", "team_boxscore", "player_boxscore")
        )
    else:
        m = importlib.import_module(f"sportsdataverse.{lg}.{lg}_loaders")
        pbp, tb, pb = (getattr(m, f"load_{lg}_{k}")([season]) for k in ("pbp", "team_boxscore", "player_boxscore"))
    pbp = pbp.with_columns(pl.col("game_id").cast(pl.Int64)).sort("game_id", "game_play_number", maintain_order=True)
    # score once here, so G2 compares the story's moment with an argmax taken outside it
    scored = []
    for _, one in pbp.group_by("game_id", maintain_order=True):
        s = one["season"].drop_nulls()
        reg = game_regulation_periods(lg, season=int(s[0]) if s.len() else None)
        scored.append(score_win_prob(one, league=lg, regulation_periods=reg))
    pbp = pl.concat(scored, how="diagonal_relaxed")
    story = nba_game_story(pbp, tb, pb, league=lg)
    wp = pbp.select(
        "game_id",
        pl.col("id").cast(pl.Int64).alias("play_id"),
        pl.col("game_play_number").cast(pl.Int64).alias("order"),
        pl.col("wp_delta").alias("wpa"),
    )
    last = (
        pbp.sort("game_id", "game_play_number")
        .with_columns(pl.col("home_score", "away_score").forward_fill().over("game_id"))
        .group_by("game_id")
        .agg(pl.col("home_score").last().alias("pbp_home"), pl.col("away_score").last().alias("pbp_away"))
    )
    return story, wp, last


def _football(
    lg: str, season: int, data_dir: Path | None
) -> tuple[dict, pl.DataFrame, pl.DataFrame, pl.DataFrame | None]:
    from sportsdataverse.cfb.cfb_game_story import _football_game_story

    tag = f"espn_{lg}_pbp"
    path = data_dir / f"{tag}_{season}.parquet" if data_dir else f"{_RELEASE}{tag}/play_by_play_{season}.parquet"
    pbp = pl.read_parquet(path, use_pyarrow=True)
    story = _football_game_story(pbp)
    home = pl.col("pos_team_id") == pl.col("homeTeamId")
    wp = pbp.select(
        pl.col("game_id").cast(pl.Int64),
        pl.col("id").cast(pl.Int64).alias("play_id"),
        pl.col("game_play_number").cast(pl.Int64).alias("order"),
        pl.when(pl.col("pos_team_id").is_null())
        .then(None)
        .when(home)
        .then(pl.col("wpa"))
        .otherwise(-pl.col("wpa"))
        .alias("wpa"),
    )
    last = (
        pbp.sort("game_id", "game_play_number")
        .group_by("game_id")
        .agg(
            pl.col("homeScore").drop_nulls().last().alias("pbp_home"),
            pl.col("awayScore").drop_nulls().last().alias("pbp_away"),
        )
        .with_columns(pl.col("game_id").cast(pl.Int64))
    )
    official = None
    if lg == "nfl":
        from sportsdataverse.nfl import load_nfl_schedule

        sched = load_nfl_schedule([season]).select(
            pl.col("game_id").alias("nflverse_game_id"),
            pl.col("home_score").cast(pl.Int64).alias("off_home"),
            pl.col("away_score").cast(pl.Int64).alias("off_away"),
        )
        official = (
            pbp.select(pl.col("game_id").cast(pl.Int64), "nflverse_game_id")
            .unique("game_id")
            .join(sched, on="nflverse_game_id", how="left")
            .select("game_id", "off_home", "off_away")
        )
    return story, wp, last, official


def run(lg: str, season: int, data_dir: Path | None) -> dict:
    """Score G1-G3 on one league-season.

    Args:
        lg: ``nba``, ``wnba``, ``mbb``, ``wbb``, ``cfb`` or ``nfl``.
        season: The season (the release's end-year convention).
        data_dir: A directory of cached release parquets, or ``None`` to read the releases.

    Returns:
        ``{"league", "season", "games", "seconds", "G1", "G2", "G3", "accounting_gap", "rank1_edges"}``: each
        gate's ``n``, ``pass`` and ``share`` (G1 adds the Wilson lower bound, the X it sets and the failing
        games; G2 the play-by-play games with no story; G3 the games whose source ends off the final), the
        accounting edges' distance from the margin, and how often each edge ranks first.
    """
    t0 = time.time()
    official = None
    if lg in _BASKETBALL:
        story, wp, last = _basketball(lg, season, data_dir)
    else:
        story, wp, last, official = _football(lg, season, data_dir)
    summ = story["summary"]
    if official is not None:  # the NFL's final is the schedule's, not the play-by-play's
        summ = summ.join(official, on="game_id", how="left").with_columns(
            pl.coalesce("off_home", "home_score").alias("home_score"),
            pl.coalesce("off_away", "away_score").alias("away_score"),
        )
    decided = summ.filter(pl.col("home_score") != pl.col("away_score"))
    top = story["edges"].filter(pl.col("rank") == 1).join(decided.select("game_id"), on="game_id", how="semi")
    g1_n, g1_pass = top.height, int((top["points_worth"] > 0).sum())
    lower = wilson_lower(g1_pass, g1_n)
    # G2: argmax |wpa| on the input, earliest on ties
    truth = (
        wp.filter(pl.col("wpa").is_not_null() & pl.col("wpa").is_not_nan())
        .sort("game_id", pl.col("wpa").abs(), "order", descending=[False, True, False])
        .group_by("game_id", maintain_order=True)
        .first()
        .select("game_id", pl.col("play_id").alias("true_play"))
    )
    mom = story["moment"].filter(pl.col("rule") == "max_abs_wpa").select("game_id", "play_id")
    no_story = truth.join(summ.select("game_id"), on="game_id", how="anti").height  # no box: no final, no story
    g2 = truth.join(summ.select("game_id"), on="game_id", how="semi").join(mom, on="game_id", how="left")
    g2_pass = int((g2["true_play"] == g2["play_id"]).fill_null(False).sum())
    # G3: on games whose pbp reaches the official final
    per = (
        story["periods"]
        .group_by("game_id")
        .agg(pl.col("home_pts").sum().alias("ph"), pl.col("away_pts").sum().alias("pa"))
    )
    g3 = summ.join(last, on="game_id", how="left").join(per, on="game_id", how="left")
    reaches = (g3["pbp_home"] == g3["home_score"]) & (g3["pbp_away"] == g3["away_score"])
    g3r = g3.filter(reaches.fill_null(False))
    g3_pass = int(((g3r["ph"] == g3r["home_score"]) & (g3r["pa"] == g3r["away_score"])).sum())
    off_final = g3.filter(~reaches.fill_null(False))
    acct = (
        story["edges"]
        .filter(pl.col("kind") == "accounting")
        .group_by("game_id")
        .agg(pl.col("points_worth").sum().alias("acct"))
        .join(summ.select("game_id", "margin"), on="game_id")
        .with_columns((pl.col("acct") - pl.col("margin")).abs().alias("gap"))
    )
    top_edges = top.group_by("edge").len().sort("len", descending=True)
    return {
        "league": lg,
        "season": season,
        "games": summ.height,
        "seconds": round(time.time() - t0, 1),
        "G1": {
            "n": g1_n,
            "pass": g1_pass,
            "share": g1_pass / g1_n if g1_n else None,
            "wilson_lower": lower,
            "X": math.floor(100 * lower) if g1_n else None,
            "fails": top.filter(pl.col("points_worth") <= 0)["game_id"].to_list(),
        },
        "G2": {
            "n": g2.height,
            "pass": g2_pass,
            "share": g2_pass / g2.height if g2.height else None,
            "pbp_games_without_story": no_story,
        },
        "G3": {
            "n": g3r.height,
            "pass": g3_pass,
            "share": g3_pass / g3r.height if g3r.height else None,
            "source_off_final": off_final.height,
            "source_off_final_games": off_final["game_id"].to_list()[:50],
        },
        "accounting_gap": {"max": acct["gap"].max(), "mean": acct["gap"].mean(), "median": acct["gap"].median()},
        "rank1_edges": dict(zip(top_edges["edge"].to_list(), top_edges["len"].to_list())),
    }


def main() -> None:
    """Parse the arguments, score the gates and print (and optionally write) the JSON."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--league", required=True, choices=[*_BASKETBALL, *_FOOTBALL])
    ap.add_argument("--season", required=True, type=int)
    ap.add_argument("--data-dir", type=Path)
    ap.add_argument("--out", type=Path)
    a = ap.parse_args()
    res = run(a.league, a.season, a.data_dir)
    text = json.dumps(res, indent=2, default=str)
    print(text, flush=True)
    if a.out:
        a.out.write_text(text + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
