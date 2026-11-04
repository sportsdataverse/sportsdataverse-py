# tools/validate_pro_season_sim.py
"""Validate the NBA / WNBA season sim on past seasons: mid-season sims vs the final standings.

For each season the sim runs as of the first date on which half of the regular-season games are
done (:func:`sportsdataverse.nba.nba_season_sim.simulate_pro_season`, the shipped
``SIM_CONSTANTS``, ``n_sims`` draws, a fixed seed), with and without the per-season rating
shift. It is scored against what happened:

* the final standings, from :func:`sportsdataverse.nba.nba_season_rules.nba_season_standings`
  on every regular-season game (seed, division winner, wins);
* who made the playoffs and the play-in, from the schedule's ``playoffs`` / ``play-in`` games.
  The standings are first checked against them: the NBA play-in teams must be seeds 7-10, the
  rest of the playoff field seeds 1-6, and the first-round pairs of seeds 3-6 must sum to 9; the
  WNBA field must be seeds 1-8, paired 1-8, 2-7, 3-6, 4-5, the better seed at home in game 1.

Metrics: Brier of ``p_playoffs``, ``p_top6`` / ``p_play_in`` / ``p_division`` (NBA) and
``p_top4``; MAE of ``proj_wins`` against final wins, next to the pace baseline (wins so far
plus the games left at the current winning percentage); and how often the final wins fall in
the sim's 10-90% band (0.80 if calibrated).

With ``--closes DIR`` (WNBA only; private ClaudeCowork data, not in this repo) it also scores the
sim's game model one day ahead (ratings refit before each date, the first 20 games of the
season skipped, as the 2026-11-02 sim-vs-market bench did) against the closing market and the
engine bench on the same games: Brier with paired differences, SE clustered by date.

Usage::

    uv run python tools/validate_pro_season_sim.py --league nba --seasons 2024 2025 2026
    uv run python tools/validate_pro_season_sim.py --league wnba --seasons 2024 2025 \
        --closes ../ClaudeCowork/data/2026-11-02-wnba-sim-vs-market \
        --bench ../sdv-engine/dev/wnba_market
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

import numpy as np
import polars as pl
from scipy.stats import norm

from sportsdataverse._common.metrics import brier_score
from sportsdataverse.nba.nba_season_rules import nba_season_standings
from sportsdataverse.nba.nba_season_sim import (
    PreseasonPrior,
    _records,
    _regular_games,
    as_of_game_model,
    preseason_prior,
    simulate_pro_season,
)
from sportsdataverse.nba.nba_team_season_rates import _league_loaders, nba_possession_rate_rows

_LEAGUE_ID = {"nba": "00", "wnba": "10"}


def midseason(games: pl.DataFrame) -> dt.date:
    """The date of the season's median game; the sim treats the games before it as played.

    Games on that date are simulated (as of a date means before it), so slightly fewer than half
    of the games are in the fit. The pinned gates are computed at this date.
    """
    dates = games.get_column("game_date").sort()
    return dates[dates.len() // 2]


def postseason_teams(schedule: pl.DataFrame, season_type: str) -> set[int]:
    """Every team in a game of ``season_type``."""
    sub = schedule.filter(pl.col("season_type") == season_type)
    return set(sub["home_team_id"].to_list()) | set(sub["away_team_id"].to_list())


def check_standings(final: pl.DataFrame, schedule: pl.DataFrame, league_id: str) -> dict[str, object]:
    """The computed seeds against the real postseason field and first-round pairs."""
    seed = dict(zip(final["team_id"].to_list(), final["seed"].to_list(), strict=True))
    playoffs = postseason_teams(schedule, "playoffs")
    po = schedule.filter(pl.col("season_type") == "playoffs").with_columns(
        pl.col("game_date").cast(pl.Utf8).str.slice(0, 10).str.to_date()
    )
    first: dict[frozenset[int], tuple[int, int]] = {}
    for h, a in po.sort("game_date").select("home_team_id", "away_team_id").iter_rows():
        key = frozenset((h, a))
        if not any(h in k or a in k for k in first) or key in first:
            first.setdefault(key, (h, a))
    bad: list[str] = []
    if league_id == "00":
        play_in = postseason_teams(schedule, "play-in")
        if {t for t in play_in} != {t for t, s in seed.items() if 7 <= s <= 10}:
            bad.append("play-in field != seeds 7-10")
        if playoffs - play_in != {t for t, s in seed.items() if s <= 6}:
            bad.append("direct playoff field != seeds 1-6")
        conf = dict(zip(final["team_id"].to_list(), final["conference"].to_list(), strict=True))
        for h, a in first.values():
            sh, sa = seed[h], seed[a]
            if conf[h] == conf[a] and max(sh, sa) <= 6 and sh + sa != 9:
                bad.append(f"first round {sh} v {sa}")
            if sh > sa and sa <= 6 and sh <= 6:
                bad.append(f"worse seed {sh} hosted {sa}")
    else:
        if playoffs != {t for t, s in seed.items() if s <= 8}:
            bad.append("playoff field != seeds 1-8")
        for h, a in first.values():
            if seed[h] + seed[a] != 9 or seed[h] > seed[a]:
                bad.append(f"first round {seed[h]} v {seed[a]} (home {seed[h]})")
    return {"first_round_series": len(first), "mismatches": bad}


def outcomes(sim: pl.DataFrame, final: pl.DataFrame, schedule: pl.DataFrame) -> pl.DataFrame:
    """The sim joined to what happened: final wins, seed, division winner, playoff / play-in."""
    assert sim.schema["team_id"] == final.schema["team_id"], "team_id dtype differs"
    playoffs, play_in = postseason_teams(schedule, "playoffs"), postseason_teams(schedule, "play-in")
    missing = set(sim["team_id"].to_list()) - set(final["team_id"].to_list())
    if missing:
        raise ValueError(f"simulated teams with no final standings row: {sorted(missing)}")
    return sim.join(
        final.select("team_id", pl.col("wins").alias("final_wins"), "seed", "division_leader"),
        on="team_id",
        how="inner",
        validate="1:1",
    ).with_columns(
        pl.col("team_id").is_in(list(playoffs)).cast(pl.Float64).alias("y_playoffs"),
        pl.col("team_id").is_in(list(play_in)).cast(pl.Float64).alias("y_play_in"),
        ((pl.col("seed") <= 6) & ~pl.col("team_id").is_in(list(play_in))).cast(pl.Float64).alias("y_top6"),
        (pl.col("seed") <= 4).cast(pl.Float64).alias("y_top4"),
        pl.col("division_leader").cast(pl.Float64).alias("y_division"),
    )


def metrics(j: pl.DataFrame, league_id: str) -> dict[str, object]:
    """Brier / MAE / band coverage over team rows of :func:`outcomes`."""
    final_w = j["final_wins"].to_numpy().astype(float)
    played = (j["wins"] + j["losses"]).to_numpy().astype(float)
    pace = j["wins"].to_numpy() + j["games_remaining"].to_numpy() * j["wins"].to_numpy() / np.maximum(played, 1)
    p10, p90 = j["proj_wins_p10"].to_numpy(), j["proj_wins_p90"].to_numpy()
    out: dict[str, object] = {
        "teams": j.height,
        "brier_playoffs": brier_score(j["y_playoffs"].to_numpy(), j["p_playoffs"].to_numpy()),
        "brier_top4": brier_score(j["y_top4"].to_numpy(), j["p_top4"].to_numpy()),
        "mae_wins": float(np.abs(j["proj_wins"].to_numpy() - final_w).mean()),
        "mae_wins_pace": float(np.abs(pace - final_w).mean()),
        "band80_coverage": float(((final_w >= p10) & (final_w <= p90)).mean()),
        "max_p_playoffs_se": float(j["p_playoffs_se"].max()),
    }
    if league_id == "00":
        for k in ("top6", "play_in", "division"):
            out[f"brier_{k}"] = brier_score(j[f"y_{k}"].to_numpy(), j[f"p_{k}"].to_numpy())
    return {k: round(v, 4) if isinstance(v, float) else v for k, v in out.items()}


def one_day_ahead(
    rows: pl.DataFrame, games: pl.DataFrame, league_id: str, warmup: int = 20, prior: PreseasonPrior | None = None
) -> pl.DataFrame:
    """The sim's game-model home win probability for each game, ratings through the day before."""
    reg = games.filter(pl.col("home_pts").is_not_null()).sort("game_date", "game_id")
    rows = rows.join(reg.select("game_id", "game_date"), on="game_id", how="inner", validate="m:1")
    out = []
    for d in reg["game_date"].unique().sort().to_list():
        today = reg.filter(pl.col("game_date") == d)
        if reg.filter(pl.col("game_date") < d).height < warmup:
            continue
        model = as_of_game_model(
            rows.filter(pl.col("game_date") < d).drop("game_date"), league_id=league_id, prior=prior
        )
        net = dict(zip(model.ratings["team_id"].to_list(), model.ratings["adj_net_rtg"].to_list(), strict=True))
        m = model.margin(
            np.array([net.get(t, 0.0) for t in today["home_team_id"].to_list()]),
            np.array([net.get(t, 0.0) for t in today["away_team_id"].to_list()]),
        )
        out.append(
            today.select(
                "game_id",
                "game_date",
                (pl.col("home_pts") > pl.col("away_pts")).cast(pl.Float64).alias("y"),
                (pl.col("home_pts") - pl.col("away_pts")).cast(pl.Float64).alias("margin"),
            ).with_columns(pl.Series("sim_margin", m), pl.Series("p_sim", norm.cdf(m / model.margin_sd)))
        )
    return pl.concat(out)


def paired_brier(frame: pl.DataFrame, a: str, b: str) -> dict[str, float]:
    """Mean Brier difference ``a - b`` with its SE clustered by date."""
    d = (frame[a] - frame["y"]) ** 2 - (frame[b] - frame["y"]) ** 2
    f = pl.DataFrame({"g": frame["game_date"], "d": d})
    mean = float(d.mean())
    sums = f.group_by("g").agg(pl.col("d").sum().alias("s"), pl.len().alias("n"))
    se = float(np.sqrt(((sums["s"].to_numpy() - sums["n"].to_numpy() * mean) ** 2).sum()) / d.len())
    return {"diff": round(mean, 4), "se": round(se, 4)}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--league", choices=sorted(_LEAGUE_ID), required=True)
    ap.add_argument("--seasons", type=int, nargs="+", required=True, help="seasons by END year")
    ap.add_argument("--n-sims", type=int, default=10_000)
    ap.add_argument("--seed", type=int, default=20261103)
    ap.add_argument("--closes", type=Path, default=None, help="dir with closes_wnba_{S}.parquet (private)")
    ap.add_argument("--bench", type=Path, default=None, help="dir with the engine's bench_{S}.parquet")
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--no-prior", action="store_true", help="rate without the preseason prior (the BB-D18 sim)")
    args = ap.parse_args(argv)
    if len(set(args.seasons)) != len(args.seasons):
        ap.error("--seasons has a duplicate")
    if args.closes is not None and args.league != "wnba":
        ap.error("--closes is WNBA only (the 2026-11-02 sim-vs-market spec)")
    if args.bench is not None and args.closes is None:
        ap.error("--bench needs --closes")

    league_id = _LEAGUE_ID[args.league]
    load_possessions, load_schedule, offset = _league_loaders(league_id)
    result: dict[str, object] = {
        "league": args.league,
        "n_sims": args.n_sims,
        "seed": args.seed,
        "preseason_prior": not args.no_prior,
        "seasons": {},
    }
    pooled: dict[str, list[pl.DataFrame]] = {"shift": [], "fixed": []}
    for season in args.seasons:
        schedule = load_schedule([season - offset])
        full = _regular_games(schedule, None)
        rows = nba_possession_rate_rows(load_possessions([season - offset]), schedule, league_id=league_id)
        final = nba_season_standings(full, league_id=league_id)
        cut = midseason(full)
        games = _regular_games(schedule, cut)
        done = games.filter(pl.col("home_pts").is_not_null())
        last_schedule = load_schedule([season - 1 - offset])
        pre = None
        if not args.no_prior:
            last_rows = nba_possession_rate_rows(
                load_possessions([season - 1 - offset]), last_schedule, league_id=league_id
            )
            pre = preseason_prior(last_rows, sorted(set(full["home_team_id"].to_list())), league_id=league_id)
        model = as_of_game_model(
            rows.join(done.select("game_id"), on="game_id", how="semi"), league_id=league_id, prior=pre
        )
        prior = _records(_regular_games(last_schedule, None)) if league_id == "10" else None
        entry: dict[str, object] = {"as_of": str(cut), "standings_check": check_standings(final, schedule, league_id)}
        for name, unc in (("shift", True), ("fixed", False)):
            sim = simulate_pro_season(
                games.select("season", "game_id", "home_team_id", "away_team_id", "home_pts", "away_pts"),
                model,
                league_id=league_id,
                n_sims=args.n_sims,
                seed=args.seed,
                prior_records=prior,
                rating_uncertainty=unc,
                as_of_date=cut,
            )
            joined = outcomes(sim, final, schedule)
            entry[name] = metrics(joined, league_id)
            pooled[name].append(joined)
        if args.closes is not None and league_id == "10":
            oda = one_day_ahead(rows, full, league_id, prior=pre)
            closes = pl.read_parquet(args.closes / f"closes_wnba_{season}.parquet").select(
                "game_id", "mkt_p_home", "mkt_margin"
            )
            j = oda.join(closes, on="game_id", how="inner", validate="1:1").drop_nulls(["mkt_p_home", "mkt_margin"])
            mkt = {
                "games": j.height,
                "brier_sim": round(brier_score(j["y"].to_numpy(), j["p_sim"].to_numpy()), 4),
                "brier_close": round(brier_score(j["y"].to_numpy(), j["mkt_p_home"].to_numpy()), 4),
                "mae_sim": round(float((j["sim_margin"] - j["margin"]).abs().mean()), 3),
                "mae_close": round(float((j["mkt_margin"] - j["margin"]).abs().mean()), 3),
                "sim_minus_close": paired_brier(j, "p_sim", "mkt_p_home"),
            }
            if args.bench is not None:
                b = pl.read_parquet(args.bench / f"bench_{season}.parquet").select(
                    "game_id", pl.col("p_home").alias("p_engine")
                )
                jb = j.join(b, on="game_id", how="inner", validate="1:1")
                mkt |= {
                    "games_with_engine": jb.height,
                    "brier_engine": round(brier_score(jb["y"].to_numpy(), jb["p_engine"].to_numpy()), 4),
                    "brier_sim_on_engine_games": round(brier_score(jb["y"].to_numpy(), jb["p_sim"].to_numpy()), 4),
                    "sim_minus_engine": paired_brier(jb, "p_sim", "p_engine"),
                }
            entry["vs_market"] = mkt
        result["seasons"][str(season)] = entry  # type: ignore[index]
        print(season, json.dumps(entry), flush=True)
    result["pooled"] = {name: metrics(pl.concat(frames), league_id) for name, frames in pooled.items()}
    text = json.dumps(result, indent=2)
    print(text)
    if args.out:
        args.out.write_text(text + "\n", encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
