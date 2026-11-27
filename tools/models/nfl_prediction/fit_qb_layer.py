"""Fit the NFL starting-QB layer constants on regular seasons 2015-2025 (careers from 2006).

Pre-registered in the program's spec for ACC-2026-0016 (section 4). Reads no 2026 row of any input: the
out-of-sample window is 2026 weeks 5-18, scored once by ``score_qb_layer.py``.

Writes ``qb_layer_constants.json`` next to this file (constants, counts and profiles) and prints its
sha256; the constants are then copied into ``QB_LAYER_CONSTANTS`` in
``sportsdataverse/nfl/nfl_prediction_constants.py`` (``tests/nfl/test_nfl_qb_layer.py`` asserts they
match).

Usage::

    uv run python tools/models/nfl_prediction/fit_qb_layer.py
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import sys
from pathlib import Path

import polars as pl

from sportsdataverse.nfl import load_nfl_injuries, load_nfl_pbp, load_nfl_schedule
from sportsdataverse.nfl.nfl_prediction_constants import QBLayerConfig, get_constants
from sportsdataverse.nfl.nfl_qb_layer import _team_games, nfl_expected_starters, nfl_qb_predict_games, nfl_qb_dropbacks

FIT_SEASONS = list(range(2015, 2026))
HISTORY_START = 2006
K_GRID = [25, 50, 100, 150, 200, 300, 400, 600, 800, 1200, 1600]
NEW_QB = 100
OUT = Path(__file__).with_name("qb_layer_constants.json")
PBP_COLS = [
    "game_id",
    "season",
    "game_date",
    "season_type",
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


def _log(msg: str) -> None:
    print(msg, flush=True)


def load_inputs() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    assert max(FIT_SEASONS) < 2026
    frames = []
    for season in range(HISTORY_START, max(FIT_SEASONS) + 1):
        frames.append(load_nfl_pbp([season]).select(PBP_COLS))
        _log(f"pbp {season}: {frames[-1].height} rows")
    pbp = pl.concat(frames, how="vertical_relaxed")
    schedule = load_nfl_schedule(FIT_SEASONS).filter(pl.col("game_type") == "REG")
    injuries = load_nfl_injuries(FIT_SEASONS)
    assert pbp["season"].max() < 2026 and schedule["season"].max() < 2026
    return pbp, schedule, injuries


def fit_priors(db: pl.DataFrame) -> tuple[float, float, int, int]:
    drop = db.filter(pl.col("is_dropback") == True).with_row_index("_i")  # noqa: E712
    drop = drop.sort("qb_id", "game_date", "game_id", "_i").with_columns(before=pl.int_range(pl.len()).over("qb_id"))
    fit = drop.filter(pl.col("season").is_in(FIT_SEASONS))
    league = float(fit["qb_epa"].mean())  # type: ignore[arg-type]
    new = fit.filter(pl.col("before") < NEW_QB)
    replacement = float(new["qb_epa"].mean())  # type: ignore[arg-type]
    return league, replacement, fit.height, new.height


def qb_games(db: pl.DataFrame) -> pl.DataFrame:
    games = (
        db.filter(pl.col("is_dropback") == True)  # noqa: E712
        .group_by("qb_id", "game_id", "game_date", "season")
        .agg(n=pl.len(), total=pl.col("qb_epa").sum())
        .sort("qb_id", "game_date", "game_id")
    )
    return games.with_columns(
        cum_n=pl.col("n").cum_sum().shift(1, fill_value=0).over("qb_id"),
        cum_total=pl.col("total").cum_sum().shift(1, fill_value=0.0).over("qb_id"),
    )


def fit_k(
    games: pl.DataFrame, league: float, replacement: float, reg_games: list[str]
) -> tuple[float, dict[str, float], int]:
    # careers count every game; the scored QB-games are regular season only (spec section 4)
    scored = games.filter(pl.col("season").is_in(FIT_SEASONS) & (pl.col("n") >= 5) & pl.col("game_id").is_in(reg_games))
    prior = pl.when(pl.col("cum_n") < NEW_QB).then(pl.lit(replacement)).otherwise(pl.lit(league))
    profile: dict[str, float] = {}
    for k in K_GRID:
        v = (pl.col("cum_total") + k * prior) / (pl.col("cum_n") + k)
        loss = scored.select((pl.col("n") * (pl.col("total") / pl.col("n") - v) ** 2).sum() / pl.col("n").sum()).item()
        profile[str(k)] = float(loss)
    best = min(K_GRID, key=lambda k: profile[str(k)])
    return float(best), profile, scored.height


def fit_p(schedule: pl.DataFrame, injuries: pl.DataFrame, cfg: QBLayerConfig) -> tuple[dict[str, float], dict]:
    starters = nfl_expected_starters(schedule, injuries=injuries, config=cfg)
    realized = _team_games(schedule).select("game_id", "team", "season", "week", "realized_qb_id")
    for key in ("game_id", "team"):
        assert starters.schema[key] == realized.schema[key]
    df = starters.join(realized, on=["game_id", "team"], how="inner").filter(
        pl.col("expected_qb_id").is_not_null() & pl.col("week").is_between(2, 18)
    )
    tab = (
        df.group_by("status")
        .agg(n=pl.len(), started=(pl.col("expected_qb_id") == pl.col("realized_qb_id")).sum())
        .sort("status")
    )
    p = {r["status"]: r["started"] / r["n"] for r in tab.iter_rows(named=True)}
    counts = {r["status"]: {"n": r["n"], "started": r["started"]} for r in tab.iter_rows(named=True)}
    # nfl_expected_starters raises on an injury team code the schedule lacks, so no report row is dropped
    assert set(p) == {"none", "Questionable", "Doubtful", "Out"}, p
    return p, counts


def fit_gamma(
    schedule: pl.DataFrame, pbp: pl.DataFrame, cfg: QBLayerConfig
) -> tuple[float, float, dict[str, float], int]:
    sched = schedule.filter(pl.col("week").is_between(5, 18) & pl.col("result").is_not_null())
    unit = dataclasses.replace(cfg, gamma=1.0)
    preds = nfl_qb_predict_games(sched, pbp, starter="realized", config=unit)
    assert preds.height == sched.height, f"priced {preds.height} of {sched.height} games: a team code is unmatched"
    ppn = get_constants().points_per_net
    res = sched.select(
        pl.col("game_id").cast(pl.Utf8), pl.col("result").cast(pl.Float64), pl.col("season"), pl.col("week")
    )
    assert preds.schema["game_id"] == res.schema["game_id"]
    df = preds.join(res, on="game_id").select(
        x=ppn * (pl.col("home_shift") - pl.col("away_shift")),
        y=pl.col("result") - pl.col("base_margin"),
        week=pl.col("season") * 100 + pl.col("week"),
    )
    nonzero = float((df["x"] != 0).mean())  # type: ignore[arg-type]
    if nonzero < 0.5:
        raise RuntimeError(f"only {nonzero:.0%} of games carry a QB shift: the QB ids or the exposure are broken")
    sxx = float((df["x"] ** 2).sum())
    gamma = float((df["x"] * df["y"]).sum()) / sxx
    # season-week clustered SE of the no-intercept slope
    u = df.with_columns(u=pl.col("x") * (pl.col("y") - gamma * pl.col("x"))).group_by("week").agg(pl.col("u").sum())
    g = u.height
    se = float((g / (g - 1) * (u["u"] ** 2).sum()) ** 0.5 / sxx)
    grid = [round(0.1 * i, 1) for i in range(41)]
    profile = {str(v): float(((df["y"] - v * df["x"]) ** 2).sum()) for v in grid}
    return gamma, se, profile, df.height


def main() -> int:
    pbp, schedule, injuries = load_inputs()
    db = nfl_qb_dropbacks(pbp)
    league, replacement, n_db, n_new = fit_priors(db)
    _log(f"league {league:.5f} ({n_db} dropbacks)  replacement {replacement:.5f} ({n_new})")
    reg_games = pbp.filter(pl.col("season_type") == "REG")["game_id"].cast(pl.Utf8).unique().to_list()
    k, k_profile, n_k = fit_k(qb_games(db), league, replacement, reg_games)
    _log(f"k {k}  profile {k_profile}")
    unit_p = {s: 1.0 for s in ("none", "Questionable", "Doubtful", "Out")}  # p is not read by the p fit itself
    cfg = QBLayerConfig(k=k, gamma=1.0, league=league, replacement=replacement, p_start=unit_p)
    p, p_counts = fit_p(schedule, injuries, cfg)
    _log(f"p_start {p}  counts {p_counts}")
    cfg = dataclasses.replace(cfg, p_start=p)
    gamma, gamma_se, g_profile, n_games = fit_gamma(schedule, pbp, cfg)
    _log(f"gamma {gamma:.4f} (SE {gamma_se:.4f}) over {n_games} games")
    payload = {
        "constants": {
            "k": k,
            "gamma": round(gamma, 4),
            "league": round(league, 5),
            "replacement": round(replacement, 5),
            "p_start": {s: round(v, 4) for s, v in sorted(p.items())},
            "new_qb_dropbacks": NEW_QB,
            "history_start_season": HISTORY_START,
        },
        "fit": {
            "seasons": FIT_SEASONS,
            "dropbacks": n_db,
            "new_qb_dropbacks_rows": n_new,
            "k_grid_loss": k_profile,
            "k_scored_qb_games": n_k,
            "p_start_counts": p_counts,
            "gamma_games": n_games,
            "gamma_sse_profile": g_profile,
            "gamma_raw": gamma,
            "gamma_se_season_week_clustered": gamma_se,
        },
    }
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    OUT.write_text(text, encoding="utf-8", newline="\n")
    sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    OUT.with_name(OUT.name + ".sha256").write_text(f"{sha}  {OUT.name}\n", encoding="utf-8", newline="\n")
    _log(f"wrote {OUT}  sha256 {sha}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
