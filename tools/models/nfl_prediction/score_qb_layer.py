"""Score the NFL starting-QB layer once on its pre-registered out-of-sample window (ACC-2026-0016).

The rule (program spec, section 6), on the same games and paired by game, SEs clustered by week (CR1):

1. MAE vs the closing spread improves: ``dbar + 2 SE < 0``.
2. MAE vs the realized margin is non-inferior: ``dbar + t(0.975, G-1) SE <= 0.10``.
3. 80% coverage: (a) games with a Questionable/Doubtful expected starter -- Q within 0.05 of 0.80 and closer
   than base (not evaluable under 20 games); (b) games where both starters are certain -- within 0.02 of base.
4. Brier vs results is non-inferior: ``dbar + t(0.975, G-1) SE <= 0.003``.

``spread_line`` is read here for scoring only; no arm ever reads it.

Usage (after the last week-18 game)::

    uv run python tools/models/nfl_prediction/score_qb_layer.py --season 2026 --weeks 5-18 \\
      --constants tools/models/nfl_prediction/qb_layer_constants.json --expect-sha256 <sha> --out <dir>

``--dev-season`` runs the pipeline on a development season (a smoke test, never evidence). On the
pre-registered season the script refuses other weeks, a constants file with another sha256, and a scored game
with no result.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import io
import json
import sys
import urllib.request
from pathlib import Path

import numpy as np
import polars as pl
from scipy.stats import t as student_t

from sportsdataverse.config import NFL_BASE_URL, NFL_DEPTH_CHARTS_URL, NFL_INJURIES_URL, NFL_TEAM_SCHEDULE_URL
from sportsdataverse.nfl import load_nfl_depth_charts, load_nfl_injuries, load_nfl_pbp, load_nfl_schedule
from sportsdataverse.nfl.nfl_prediction_constants import QBLayerConfig
from sportsdataverse.nfl.nfl_qb_layer import _team_games, nfl_expected_starters, nfl_qb_predict_games

PREREGISTERED_SEASON = 2026
PREREGISTERED_WEEKS = (5, 18)
#: coverage is k / n, so a boundary value (0.75, 0.85) must not fail on float rounding
EPS = 1e-9
PBP_COLS = [
    "game_id",
    "season",
    "game_date",
    "posteam",
    "defteam",
    "home_team",
    "play_type",
    "qb_dropback",
    "qb_epa",
    "epa",
    "wp",
    "special",
    "qb_kneel",
    "qb_spike",
    "passer_player_id",
    "rusher_player_id",
]
INPUT_URLS = {
    "schedule": NFL_TEAM_SCHEDULE_URL,
    "pbp": NFL_BASE_URL,
    "depth_charts": NFL_DEPTH_CHARTS_URL,
    "injuries": NFL_INJURIES_URL,
}


def cluster_se(d: np.ndarray, clusters: np.ndarray) -> float:
    """CR1 standard error of the mean of ``d`` with one cluster per distinct value of ``clusters`` (nan under 2)."""
    n = d.size
    groups = np.unique(clusters)
    g = groups.size
    if g < 2:
        return float("nan")
    dbar = d.mean()
    sums = np.array([(d[clusters == c] - dbar).sum() for c in groups])
    return float(np.sqrt(g / (g - 1) * (sums**2).sum()) / n)


def paired(d: np.ndarray, weeks: np.ndarray) -> dict[str, float]:
    g = np.unique(weeks).size
    return {
        "dbar": float(d.mean()),
        "se": cluster_se(d, weeks),
        "n": int(d.size),
        "clusters": g,
        "t975": float(student_t.ppf(0.975, g - 1)) if g > 1 else float("nan"),
    }


def coverage(df: pl.DataFrame, lo: str, hi: str) -> float | None:
    if df.height == 0:
        return None
    return float(df.select(((pl.col("result") >= pl.col(lo)) & (pl.col("result") <= pl.col(hi))).mean()).item())


def score(games: pl.DataFrame) -> dict:
    """Apply the four criteria to a joined frame (one row per game, base_* and qb_* columns, result, spread_line)."""
    w = games["week"].to_numpy()
    out: dict = {}
    d1 = (games["qb_margin"] - games["spread_line"]).abs() - (games["base_margin"] - games["spread_line"]).abs()
    c1 = paired(d1.to_numpy(), w)
    c1["pass"] = c1["dbar"] + 2 * c1["se"] < 0
    out["1_mae_vs_close"] = c1
    d2 = (games["qb_margin"] - games["result"]).abs() - (games["base_margin"] - games["result"]).abs()
    c2 = paired(d2.to_numpy(), w)
    c2["upper"] = c2["dbar"] + c2["t975"] * c2["se"]
    c2["pass"] = c2["upper"] <= 0.10
    out["2_mae_vs_result"] = c2
    uncertain = games.filter(
        pl.col("home_status").is_in(["Questionable", "Doubtful"])
        | pl.col("away_status").is_in(["Questionable", "Doubtful"])
    )
    certain = games.filter((pl.col("home_status") == "none") & (pl.col("away_status") == "none"))
    cq, cb = coverage(uncertain, "qb_lo80", "qb_hi80"), coverage(uncertain, "base_lo80", "base_hi80")
    c3a: dict = {"n": uncertain.height, "cov_q": cq, "cov_base": cb}
    if uncertain.height < 20 or cq is None or cb is None:
        c3a["pass"], c3a["evaluable"] = True, False
    else:
        c3a["evaluable"] = True
        c3a["pass"] = abs(cq - 0.80) <= 0.05 + EPS and abs(cq - 0.80) < abs(cb - 0.80) - EPS
    out["3a_coverage_uncertain"] = c3a
    kq, kb = coverage(certain, "qb_lo80", "qb_hi80"), coverage(certain, "base_lo80", "base_hi80")
    out["3b_coverage_certain"] = {
        "n": certain.height,
        "cov_q": kq,
        "cov_base": kb,
        "pass": kq is not None and kb is not None and abs(kq - kb) <= 0.02 + EPS,
    }
    y = (games["result"] > 0).cast(pl.Float64) + 0.5 * (games["result"] == 0).cast(pl.Float64)
    d4 = (games["qb_win_prob"] - y) ** 2 - (games["base_win_prob"] - y) ** 2
    c4 = paired(d4.to_numpy(), w)
    c4["upper"] = c4["dbar"] + c4["t975"] * c4["se"]
    c4["pass"] = c4["upper"] <= 0.003
    out["4_brier"] = c4
    out["keep"] = all(
        out[k]["pass"]
        for k in ("1_mae_vs_close", "2_mae_vs_result", "3a_coverage_uncertain", "3b_coverage_certain", "4_brier")
    )
    return out


def load_config(path: Path, expect_sha: str | None) -> tuple[QBLayerConfig, dict, str]:
    raw = path.read_bytes()
    sha = hashlib.sha256(raw).hexdigest()
    if expect_sha is not None and sha != expect_sha:
        raise SystemExit(f"constants sha256 {sha} != expected {expect_sha}: refusing to score")
    payload = json.loads(raw)
    return QBLayerConfig(**payload["constants"]), payload.get("fit", {}), sha


def _frame_sha(df: pl.DataFrame) -> str:
    buf = io.BytesIO()
    df.write_ipc(buf)
    return hashlib.sha256(buf.getvalue()).hexdigest()


def _asset_updated_at(url: str) -> str:
    """The GitHub release asset's ``updated_at`` for a release-download URL (``unknown: <why>`` otherwise)."""
    try:
        owner, repo, _, _, tag, name = url.split("github.com/")[1].split("/")[:6]
        api = f"https://api.github.com/repos/{owner}/{repo}/releases/tags/{tag}"
        req = urllib.request.Request(api, headers={"User-Agent": "sdv-py"})
        with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 -- fixed https host
            assets = json.load(resp)["assets"]
        return str(next(a["updated_at"] for a in assets if a["name"] == name))
    except Exception as exc:  # noqa: BLE001 -- provenance is recorded, never a reason to stop
        return f"unknown: {type(exc).__name__}"


def _provenance(season: int, frames: dict[str, pl.DataFrame]) -> dict[str, dict]:
    out = {}
    for name, df in frames.items():
        url = INPUT_URLS[name].format(season=season)
        out[name] = {"url": url, "updated_at": _asset_updated_at(url), "rows": df.height, "ipc_sha256": _frame_sha(df)}
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--weeks", default="5-18")
    ap.add_argument("--constants", type=Path, default=Path(__file__).with_name("qb_layer_constants.json"))
    ap.add_argument("--expect-sha256", default=None)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--dev-season", action="store_true", help="smoke-run on a development season (never evidence)")
    args = ap.parse_args(argv)
    lo, hi = (int(x) for x in args.weeks.split("-"))
    if args.season == PREREGISTERED_SEASON:
        if args.dev_season:
            raise SystemExit("--dev-season cannot be used on the pre-registered season")
        if args.expect_sha256 is None:
            raise SystemExit("--expect-sha256 is required for the pre-registered season")
        if (lo, hi) != PREREGISTERED_WEEKS:
            raise SystemExit(f"the pre-registered weeks are {PREREGISTERED_WEEKS}, not {(lo, hi)}")
    elif not args.dev_season:
        raise SystemExit(f"only season {PREREGISTERED_SEASON} is pre-registered; pass --dev-season for a smoke run")
    cfg, fit, sha = load_config(args.constants, args.expect_sha256)

    schedule = load_nfl_schedule([args.season]).filter(pl.col("game_type") == "REG")
    window = schedule.filter(pl.col("week").is_between(lo, hi))
    if window["result"].null_count():
        raise SystemExit(f"{window['result'].null_count()} week {lo}-{hi} games have no result yet: refusing to score")
    no_line = window.filter(pl.col("spread_line").is_null())["game_id"].to_list()
    scored = window.filter(pl.col("spread_line").is_not_null())
    if scored["week"].n_unique() != hi - lo + 1:
        raise SystemExit(f"expected {hi - lo + 1} scored weeks, found {scored['week'].n_unique()}")

    seasons = list(range(cfg.history_start_season, args.season + 1))
    pbp = pl.concat([load_nfl_pbp([s]).select(PBP_COLS) for s in seasons], how="vertical_relaxed")
    depth = load_nfl_depth_charts([args.season])
    injuries = load_nfl_injuries([args.season])
    starters = nfl_expected_starters(schedule, depth_charts=depth, injuries=injuries, config=cfg)
    in_window = starters.filter(pl.col("game_id").is_in(scored["game_id"].to_list()))
    sources = dict(in_window["starter_source"].fill_null("none").value_counts().iter_rows())
    if sources.get("depth_chart", 0) == 0:
        raise SystemExit("no scored team-game has a depth-chart starter: the pregame source is missing")

    q = nfl_qb_predict_games(scored, pbp, starters=starters, config=cfg)
    r = nfl_qb_predict_games(scored, pbp, starter="realized", config=cfg)
    truth = scored.select(
        pl.col("game_id").cast(pl.Utf8), pl.col("result").cast(pl.Float64), pl.col("spread_line").cast(pl.Float64)
    )
    assert q.schema["game_id"] == truth.schema["game_id"] == r.schema["game_id"]
    games = q.join(truth, on="game_id", how="inner")
    if games.height != scored.height:
        raise SystemExit(f"priced {games.height} of {scored.height} games: a team had no rated play")
    realized = r.join(truth, on="game_id", how="inner")

    # reported only: games where a team's expected starter is not its previous-game starter
    prev = _team_games(schedule).select("game_id", "team", "prev_qb_id")
    assert prev.schema["game_id"] == in_window.schema["game_id"]
    switched = in_window.join(prev, on=["game_id", "team"], how="left").filter(
        pl.col("expected_qb_id").is_not_null() & (pl.col("expected_qb_id") != pl.col("prev_qb_id"))
    )
    changed = games.filter(pl.col("game_id").is_in(switched["game_id"].unique().to_list()))
    out_games = games.filter((pl.col("home_status") == "Out") | (pl.col("away_status") == "Out"))
    age_h = in_window.filter(pl.col("chart_dt").is_not_null()).select(
        (pl.col("kickoff") - pl.col("chart_dt")).dt.total_minutes() / 60
    )
    age = age_h.to_series()

    result = {
        "season": args.season,
        "weeks": [lo, hi],
        "dev_season_smoke_run": bool(args.dev_season),
        "scored_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "constants_sha256": sha,
        "inputs": _provenance(
            args.season,
            {
                "schedule": schedule,
                "pbp": pbp.filter(pl.col("season") == args.season),
                "depth_charts": depth,
                "injuries": injuries,
            },
        ),
        "games_without_a_line_dropped": no_line,
        "starter_sources_scored_window": sources,
        "layer_applied_sides": int(games["home_layer_applied"].sum() + games["away_layer_applied"].sum()),
        "chart_age_hours": {"min": age.min(), "median": age.median(), "max": age.max()} if age.len() else None,
        "Q": score(games),
        "reported_only": {
            "R_realized_starter": score(realized),
            "expected_differs_from_previous_starter": score(changed) if changed.height else {"n": 0},
            "out_status_coverage": {
                "n": out_games.height,
                "cov_q": coverage(out_games, "qb_lo80", "qb_hi80"),
                "cov_base": coverage(out_games, "base_lo80", "base_hi80"),
            },
            "fit": {k: fit.get(k) for k in ("k_grid_loss", "gamma_sse_profile", "gamma_se_season_week_clustered")},
        },
    }
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "score.json").write_text(json.dumps(result, indent=2, default=str) + "\n", encoding="utf-8")
    flag = pl.col("game_id").is_in(changed["game_id"].to_list()).alias("expected_differs_from_previous")
    pl.concat([games.with_columns(arm=pl.lit("Q")), realized.with_columns(arm=pl.lit("R"))]).with_columns(
        flag
    ).write_parquet(args.out / "games.parquet")
    verdict = "KEEP" if result["Q"]["keep"] else "DO NOT KEEP"
    smoke = " (development-season smoke run, not evidence)" if args.dev_season else ""
    (args.out / "README.md").write_text(
        f"# NFL QB layer score, {args.season} weeks {lo}-{hi}\n\nVerdict: **{verdict}**{smoke}.\n\n"
        f"Constants sha256 `{sha}`. Criteria and inputs are in `score.json`; one row per game and arm in"
        " `games.parquet`.\n",
        encoding="utf-8",
    )
    print(json.dumps(result["Q"], indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
