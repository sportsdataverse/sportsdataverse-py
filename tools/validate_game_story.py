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
_MORE = ("nhl", "pwhl", "mlb", "epl")


def _rd(data_dir: Path, name: str) -> pl.DataFrame:
    return pl.read_parquet(data_dir / f"{name}.parquet")


def _more(lg: str, season: int, data_dir: Path | None) -> tuple[dict, pl.DataFrame | None, pl.DataFrame]:
    """Hockey, baseball and soccer: the story, the G2 reference plays (None where G2 does not apply) and each
    game's last running score, from the cached release parquets in ``data_dir`` (``epl`` reads the raw ESPN
    summaries in ``data_dir/soccer/eng.1_<season start year>``)."""
    if data_dir is None:
        raise SystemExit(f"--data-dir is required for {lg}")
    if lg == "nhl":
        return _nhl(season, data_dir)
    if lg == "pwhl":
        return _pwhl(season, data_dir)
    if lg == "mlb":
        return _mlb(season, data_dir)
    return _epl(season, data_dir)


def _nhl(season: int, data_dir: Path) -> tuple[dict, pl.DataFrame, pl.DataFrame]:
    from sportsdataverse.nhl.nhl_game_story import nhl_game_story
    from sportsdataverse.nhl.nhl_market import nhl_in_game_win_prob, win_prob_from_margin
    from sportsdataverse.nhl.nhl_prediction_constants import get_constants

    pbp = _rd(data_dir, f"nhl_pbp_full_{season}").with_columns(pl.col("game_id").cast(pl.Int64))
    sched = _rd(data_dir, f"nhl_schedules_{season}").filter(pl.col("game_state").is_in(["OFF", "FINAL"]))
    story = nhl_game_story(pbp, sched, _rd(data_dir, f"nhl_skater_boxscores_{season}"))
    # G2 reference: the model run here, shootout attempts unscored, the shootout's swing on one closing row
    prior = win_prob_from_margin(get_constants("nhl").hfa)
    so = (pl.col("period_type") == "SHOOTOUT").fill_null(False)
    p = pbp.filter(pl.col("game_id").is_not_null()).sort("game_id", "event_idx")
    wp = nhl_in_game_win_prob(p, prior)["home_win_prob"]
    p = p.with_columns(pl.when(so).then(None).otherwise(wp).alias("_wp")).with_columns(
        (pl.col("_wp") - pl.col("_wp").shift(1).over("game_id").fill_null(prior)).alias("wpa")
    )
    ref = p.select("game_id", pl.col("event_idx").cast(pl.Int64).alias("order"), "wpa")
    fin = sched.select(
        pl.col("game_id").cast(pl.Int64),
        pl.col("home_score").cast(pl.Int64).alias("_h"),
        pl.col("away_score").cast(pl.Int64).alias("_a"),
    )
    g = (
        p.group_by("game_id")
        .agg(
            pl.col("_wp").drop_nulls().last().fill_null(prior).alias("_lw"),
            pl.col("event_idx").max().alias("_im"),
            pl.col("home_score").drop_nulls().last().alias("_ph"),
            pl.col("away_score").drop_nulls().last().alias("_pa"),
            so.any().alias("_so"),
        )
        .join(fin, on="game_id")
        .with_columns(
            (pl.col("_so") & ((pl.col("_h") + pl.col("_a")) == (pl.col("_ph") + pl.col("_pa") + 1))).alias("_sg")
        )
    )
    so_rows = g.filter(pl.col("_sg")).select(
        "game_id",
        (pl.col("_im") + 1).cast(pl.Int64).alias("order"),
        ((pl.col("_h") > pl.col("_a")).cast(pl.Float64) - pl.col("_lw")).alias("wpa"),
    )
    # the plays reach the final: the last running score, plus the shootout's goal where there is one
    last = g.select(
        "game_id",
        (pl.col("_ph") + (pl.col("_sg") & (pl.col("_h") > pl.col("_a"))).cast(pl.Int64)).alias("pbp_home"),
        (pl.col("_pa") + (pl.col("_sg") & (pl.col("_a") > pl.col("_h"))).cast(pl.Int64)).alias("pbp_away"),
    )
    return story, pl.concat([ref, so_rows]), last


def _pwhl(season: int, data_dir: Path) -> tuple[dict, None, pl.DataFrame]:
    from sportsdataverse.pwhl.pwhl_game_story import pwhl_game_story

    sched = _rd(data_dir, f"pwhl_schedules_{season}")
    pbp = _rd(data_dir, f"pwhl_pbp_{season}")
    story = pwhl_game_story(
        pbp, sched, _rd(data_dir, f"pwhl_skater_boxscores_{season}"), _rd(data_dir, f"pwhl_xg_pbp_{season}")
    )
    # reach: the goal events (plus the shootout's one goal) against the official final
    s = sched.select(
        pl.col("game_id").cast(pl.Int64),
        pl.col("home_team_id").cast(pl.Int64),
        pl.col("away_team_id").cast(pl.Int64),
        (pl.col("game_status") == "Final SO").alias("_so"),
        pl.col("home_score").cast(pl.Int64).alias("_h"),
        pl.col("away_score").cast(pl.Int64).alias("_a"),
    )
    goals = pbp.filter(pl.col("event") == "goal").select(
        pl.col("game_id").cast(pl.Int64), pl.col("team_id").cast(pl.Int64, strict=False).alias("_t")
    )
    last = (
        s.join(goals, on="game_id", how="left")
        .group_by("game_id", "_so", "_h", "_a")
        .agg(
            (pl.col("_t") == pl.col("home_team_id")).sum().cast(pl.Int64).alias("_gh"),
            (pl.col("_t") == pl.col("away_team_id")).sum().cast(pl.Int64).alias("_ga"),
        )
        .select(
            "game_id",
            (pl.col("_gh") + (pl.col("_so") & (pl.col("_h") > pl.col("_a"))).cast(pl.Int64)).alias("pbp_home"),
            (pl.col("_ga") + (pl.col("_so") & (pl.col("_a") > pl.col("_h"))).cast(pl.Int64)).alias("pbp_away"),
        )
    )
    return story, None, last


def _mlb(season: int, data_dir: Path) -> tuple[dict, pl.DataFrame, pl.DataFrame]:
    from sportsdataverse.mlb.mlb_game_story import mlb_game_story

    sched = _rd(data_dir, f"mlb_schedule_{season}").filter(
        (pl.col("detailed_state") == "Final") & pl.col("game_type").is_in(["R", "F", "D", "L", "W"])
    )
    pbp, wpa = _rd(data_dir, f"mlb_pbp_{season}"), _rd(data_dir, f"mlb_wpa_{season}")
    story = mlb_game_story(pbp, sched, wpa, _rd(data_dir, f"mlb_re24_{season}"))
    # G2 reference: the release's WPA moved back to the PA that made it (index 0 is the pregame step)
    ref = wpa.filter(pl.col("at_bat_index") > 0).select(
        pl.col("game_id").cast(pl.Int64), (pl.col("at_bat_index") - 1).cast(pl.Int64).alias("order"), "wpa"
    )
    last = (
        pbp.sort("game_pk", "at_bat_index")
        .group_by("game_pk")
        .agg(pl.col("home_score").last().alias("pbp_home"), pl.col("away_score").last().alias("pbp_away"))
        .select(pl.col("game_pk").cast(pl.Int64).alias("game_id"), "pbp_home", "pbp_away")
    )
    return story, ref, last


def _epl(season: int, data_dir: Path) -> tuple[dict, None, pl.DataFrame]:
    import glob
    import gzip
    import json

    from sportsdataverse.soccer.soccer_game_story import soccer_game_story

    files = sorted(glob.glob(str(data_dir / "soccer" / f"eng.1_{season}" / "*.json.gz")))
    payloads = []
    for f in files:
        with gzip.open(f, "rt", encoding="utf-8") as fh:
            payloads.append(json.load(fh))
    story = soccer_game_story(payloads)
    rows = []
    for p in payloads:  # reach: the scoring key events (the shootout left out) against the competitors' scores
        side = {c["team"]["id"]: c["homeAway"] for c in p["header"]["competitions"][0]["competitors"]}
        n = {"home": 0, "away": 0}
        for e in p.get("keyEvents") or []:
            who = side.get((e.get("team") or {}).get("id"))
            if e.get("scoringPlay") and (e.get("period") or {}).get("number", 99) < 5 and who in n:
                n[who] += 1
        rows.append({"game_id": int(p["header"]["id"]), "pbp_home": n["home"], "pbp_away": n["away"]})
    return story, None, pl.DataFrame(rows, schema={"game_id": pl.Int64, "pbp_home": pl.Int64, "pbp_away": pl.Int64})


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
    official = None
    if lg == "nfl":  # the NFL frame has no finals: give it the schedule's before the story picks a winner
        from sportsdataverse.nfl import load_nfl_schedule

        sched = load_nfl_schedule([season]).select(
            pl.col("game_id").alias("nflverse_game_id"),
            pl.col("home_score").cast(pl.Int64).alias("homeFinalScore"),
            pl.col("away_score").cast(pl.Int64).alias("awayFinalScore"),
        )
        pbp = pbp.drop("homeFinalScore", "awayFinalScore", strict=False).join(sched, on="nflverse_game_id", how="left")
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
        lg: ``nba``, ``wnba``, ``mbb``, ``wbb``, ``cfb``, ``nfl``, ``nhl``, ``pwhl``, ``mlb`` or ``epl``.
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
    elif lg in _FOOTBALL:
        story, wp, last, official = _football(lg, season, data_dir)
    else:
        story, wp, last = _more(lg, season, data_dir)
    summ = story["summary"]
    if official is not None:  # the NFL's final is the schedule's, not the play-by-play's
        summ = summ.join(official, on="game_id", how="left").with_columns(
            pl.coalesce("off_home", "home_score").alias("home_score"),
            pl.coalesce("off_away", "away_score").alias("away_score"),
        )
    decided = summ.filter(pl.col("home_score") != pl.col("away_score"))
    # every decided game counts: one whose edges are all unpriced has no rank-1 row and fails
    top = decided.select("game_id").join(story["edges"].filter(pl.col("rank") == 1), on="game_id", how="left")
    g1_n, g1_pass = top.height, int((top["points_worth"] > 0).sum())
    lower = wilson_lower(g1_pass, g1_n)
    # G2: argmax |wpa| on the input, earliest on ties
    if wp is None:  # no win probability in this league: G2 does not apply
        wp = pl.DataFrame(schema={"game_id": pl.Int64, "order": pl.Int64, "wpa": pl.Float64})
    truth = (
        wp.filter(pl.col("wpa").is_not_null() & pl.col("wpa").is_not_nan())
        .sort("game_id", pl.col("wpa").abs(), "order", descending=[False, True, False])
        .group_by("game_id", maintain_order=True)
        .first()
        .select("game_id", pl.col("order").alias("true_order"))
    )
    # by play order: a hockey shootout's closing row has no play id
    mom = story["moment"].filter(pl.col("rule") == "max_abs_wpa").select("game_id", "order")
    no_story = truth.join(summ.select("game_id"), on="game_id", how="anti").height  # no box: no final, no story
    g2 = truth.join(summ.select("game_id"), on="game_id", how="semi").join(mom, on="game_id", how="left")
    g2_pass = int((g2["true_order"] == g2["order"]).fill_null(False).sum())
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
        "rank1_edges": dict(zip(top_edges["edge"].to_list(), top_edges["len"].to_list(), strict=True)),
    }


def main() -> None:
    """Parse the arguments, score the gates and print (and optionally write) the JSON."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--league", required=True, choices=[*_BASKETBALL, *_FOOTBALL, *_MORE])
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
