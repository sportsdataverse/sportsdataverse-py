# tools/models/lifecycle/coverage.py
"""Lifecycle check (a): realized win-total interval coverage of the NBA / WNBA season sim.

The sim (:mod:`sportsdataverse.nba.nba_season_sim`) projects each team's season wins from a fixed
as-of state. This scores those projections against the wins the team really finished with, on
seasons outside the sim's fit, at two cutoffs per season:

* **preseason** -- the season's first game date (nothing played; the ratings are the preseason prior);
* **in-season** -- the first game day on or after Dec 1 (NBA) or June 15 (WNBA).

At each cutoff the sim runs three times: ``rating_uncertainty=True`` (``shift``, shipped),
``False`` (``fixed``), and ``True`` with the rating sd at the ridge posterior
(``shift_penalty_sd``: ``prior_poss`` at the SIM_CONSTANTS penalty, the form shipped before
:data:`~sportsdataverse.nba.nba_season_sim.PRIOR_MISS_POSS`; reported, never read). Inputs come
from the shipped path (``_load_inputs``: the as-of ridge, the preseason prior, rest and travel),
with the shipped constants and no refit. The Cup is left out (``cup="never"``): a finished season's
release lists every game, so each team already has its full schedule. From 2024-25 that includes the
Cup knockout pairings, known in hindsight (which teams meet, never a result); the shipped sim draws
them. Seasons with no ``game_sites.csv`` home rows run without rest and travel, as the shipped sim
does (it warns).

**Coverage.** For each team the sim's draws give a discrete win distribution. At each level (50, 80,
95%) the central interval is the inclusive ``[lo, hi]`` whose tails each hold at most
``(1 - level) / 2``; it holds at least ``level``, and its own mass is reported next to the hit rate.
The 80% interval is the integer form of the shipped ``proj_wins_p10`` / ``proj_wins_p90``.

**Clusters.** The unit is the team-season and the cluster is the season (one season's league-wide
shocks move every team's error together). Per-season coverage, the pooled coverage and the pooled
``coverage - mass``, with a CR1 SE clustered by season, quoted only with at least
:data:`MIN_CLUSTERS` seasons. No row-level SE is computed.

**Readings** (80% level, both cutoffs): ``OK`` when ``|coverage - mass| <= max(0.05, 2 SE)``,
``WATCH`` otherwise, ``TOO_FEW_SEASONS`` below :data:`MIN_CLUSTERS`. The silent no-op check: at the
preseason cutoff the shipped arm must cover at least as well as the fixed arm AND its 80% intervals
must be wider on average; if not, the rating draw is not doing what the module doc says, which is a
bug report (``BUG``), not a WATCH.

Usage::

    uv run python tools/models/lifecycle/coverage.py --league nba --out lifecycle_nba_season_sim.json
    uv run python tools/models/lifecycle/coverage.py --league wnba --n-sims 5000

Design: ClaudeCowork ``specs/2026-11-25-model-lifecycle-checks-design.md`` (a).
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

# the package re-exports the function under the module's name, so import the module itself
sim = importlib.import_module("sportsdataverse.nba.nba_season_sim")
# the CR1 SE lives in the sibling retire.py (one implementation for both checks)
_spec = importlib.util.spec_from_file_location("lifecycle_retire", Path(__file__).with_name("retire.py"))
assert _spec is not None and _spec.loader is not None
retire = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(retire)

# The reading's constants are the spec's, set before any run and never loosened to pass a model:
# the 80% level, the 0.05 / 2 SE tolerance, and a CR1 SE only at 8 or more seasons
# (ClaudeCowork specs/2026-11-25-model-lifecycle-checks-design.md, section (a)).
LEVELS = (0.5, 0.8, 0.95)
GATE_LEVEL = 0.8
MIN_CLUSTERS = 8
TOL_FLOOR = 0.05
TOL_SE = 2.0
#: The no-op check's floor on the preseason 80% width ratio, shipped arm over fixed. Observed
#: 3.2 (NBA) and 3.4 (WNBA) pooled at 10,000 sims, 3.49 on the 2025-26 fixture at 400; a draw
#: shrunk to a tenth of its sd gives 1.09 there. Never lowered to pass.
WIDTH_RATIO_FLOOR = 1.5
_LEAGUE_ID = {"nba": "00", "wnba": "10"}
#: The in-season cutoff: the first game day on or after this (month, day).
_IN_SEASON = {"nba": (12, 1), "wnba": (6, 15)}

#: Every fit on the shipped path and the seasons it read (END years), from the constants' comments.
#: A scored season must be outside all of them (``tests/tools/test_model_lifecycle.py``).
PATH_FITS: dict[str, dict[str, list[int]]] = {
    "nba": {
        "SIM_CONSTANTS / PRIOR_CONSTANTS / HFA_PSEUDO_POSS / PRIOR_MISS_POSS train": [2018, 2019, 2022, 2023],
        "REST_TRAVEL_CONSTANTS fit": [2018, 2019, 2022, 2023, 2024, 2025, 2026],
        "team_strength.RIDGE_LAMBDA CV": [2022, 2023, 2024, 2025, 2026],
    },
    "wnba": {
        "SIM_CONSTANTS / PRIOR_CONSTANTS / HFA_PSEUDO_POSS / PRIOR_MISS_POSS train": [2018, 2019, 2021],
        "team_strength.RIDGE_LAMBDA CV": [2021, 2022, 2023, 2024, 2025],
    },
}

#: Seasons scored, by END year, and the ones left out with the reason. Only seasons outside every
#: fit the shipped path uses count: the SIM_CONSTANTS / PRIOR_CONSTANTS / HFA_PSEUDO_POSS comments
#: in nba_season_sim.py name the train seasons (NBA 2018 2019 2022 2023, WNBA 2018 2019 2021), the
#: WNBA seasons the 2026-11-02 sim-vs-market spec reserves (2022 2023 2026), and the NBA
#: REST_TRAVEL_CONSTANTS (nba_rest_travel.py) are a fit on 2018 2019 2022-2026, and the preseason
#: prior's final-rating penalty (``team_strength.RIDGE_LAMBDA``) was chosen by CV on NBA 2022-2026 and
#: WNBA 2021-2025, so NBA 2024-2026 and WNBA 2024-2025 are near-holdout (:data:`NEAR_HOLDOUT`).
#: Every season here is in the ``{nba,wnba}_stats_possessions`` and ``_schedules`` releases
#: (checked 2026-11-25), and so is the season before it, which the preseason prior reads.
SEASONS: dict[str, list[int]] = {
    "nba": [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017],
    "wnba": [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017],
}
#: Seasons outside the sim's own fit but inside another fit on its path, scored only with
#: ``--near-holdout`` and then labelled so in the report.
NEAR_HOLDOUT: dict[str, dict[int, str]] = {
    "nba": {s: "in the REST_TRAVEL_CONSTANTS fit and the RIDGE_LAMBDA CV (2022-2026)" for s in (2024, 2025, 2026)},
    "wnba": {s: "in the RIDGE_LAMBDA CV (2021-2025)" for s in (2024, 2025)},
}
EXCLUDED: dict[str, dict[int, str]] = {
    "nba": {
        2018: "train",
        2019: "train",
        2020: "bubble, 72 games",
        2021: "72 games",
        2022: "train",
        2023: "train",
        2027: "in progress",
    },
    "wnba": {
        2018: "train",
        2019: "train",
        2020: "bubble, 22 games",
        2021: "train",
        2022: "reserved (sim-vs-market spec)",
        2023: "reserved (sim-vs-market spec)",
        2026: "reserved (sim-vs-market spec)",
    },
}


def central_interval(pmf: np.ndarray, level: float) -> tuple[int, int, float]:
    """Inclusive central ``[lo, hi]`` of a discrete PMF over 0..n at ``level``, and its own mass."""
    cdf = np.cumsum(pmf)
    lo = int(np.searchsorted(cdf, (1.0 - level) / 2.0 - 1e-12))
    hi = int(np.searchsorted(cdf, (1.0 + level) / 2.0 - 1e-12))
    return lo, hi, float(cdf[hi] - (cdf[lo - 1] if lo else 0.0))


def team_rows(team_ids: list[int], total_wins: np.ndarray, final_wins: dict[int, int]) -> pl.DataFrame:
    """One row per team: final wins and, per level, the interval, its mass, its width and the hit."""
    missing = sorted(set(team_ids) - set(final_wins))
    if missing:
        raise ValueError(f"simulated teams with no final record: {missing}")
    out: dict[str, list[Any]] = {"team_id": list(team_ids), "final_wins": [final_wins[t] for t in team_ids]}
    for lv in LEVELS:
        key = f"{round(lv * 100)}"
        cols: dict[str, list[Any]] = {f"lo_{key}": [], f"hi_{key}": [], f"mass_{key}": [], f"hit_{key}": []}
        for j, t in enumerate(team_ids):
            draws = total_wins[:, j].astype(np.int64)
            pmf = np.bincount(draws) / draws.size
            lo, hi, mass = central_interval(pmf, lv)
            cols[f"lo_{key}"].append(lo)
            cols[f"hi_{key}"].append(hi)
            cols[f"mass_{key}"].append(mass)
            cols[f"hit_{key}"].append(lo <= final_wins[t] <= hi)
        out |= cols
    return pl.DataFrame(out).with_columns(
        (pl.col(f"hi_{round(lv * 100)}") - pl.col(f"lo_{round(lv * 100)}")).alias(f"width_{round(lv * 100)}")
        for lv in LEVELS
    )


def summarize(rows: pl.DataFrame, level: float) -> dict[str, Any]:
    """Per-season and pooled coverage at ``level`` over team-season rows with a ``season`` column."""
    key = f"{round(level * 100)}"
    hit = rows[f"hit_{key}"].cast(pl.Float64).to_numpy()
    mass = rows[f"mass_{key}"].to_numpy()
    seasons = rows["season"].to_numpy()
    by_season = (
        rows.group_by("season")
        .agg(
            pl.len().alias("teams"),
            pl.col(f"hit_{key}").cast(pl.Float64).mean().alias("coverage"),
            pl.col(f"mass_{key}").mean().alias("mass"),
            pl.col(f"width_{key}").cast(pl.Float64).mean().alias("width"),
        )
        .sort("season")
    )
    n_clusters = int(np.unique(seasons).size)
    diff = hit - mass
    se = retire.cr1_se(diff, seasons) if n_clusters >= MIN_CLUSTERS else None
    return {
        "level": level,
        "by_season": by_season.to_dicts(),
        "seasons": n_clusters,
        "team_seasons": rows.height,
        "coverage": float(hit.mean()),
        "mass": float(mass.mean()),
        "width": float(rows[f"width_{key}"].cast(pl.Float64).mean()),
        "diff": float(diff.mean()),
        "diff_se_cr1_season": se,
    }


def tolerance(s: dict[str, Any]) -> float | None:
    """``max(TOL_FLOOR, TOL_SE * SE)`` for one :func:`summarize` result; None without a quoted SE."""
    se = s["diff_se_cr1_season"]
    return None if se is None else max(TOL_FLOOR, TOL_SE * se)


def reading(s: dict[str, Any]) -> str:
    """``OK`` / ``WATCH`` / ``TOO_FEW_SEASONS`` for one :func:`summarize` result."""
    tol = tolerance(s)
    if tol is None:
        return "TOO_FEW_SEASONS"
    return "OK" if abs(s["diff"]) <= tol else "WATCH"


def noop_check(shift: dict[str, Any], fixed: dict[str, Any]) -> dict[str, Any]:
    """The rating draw must widen the 80% intervals by :data:`WIDTH_RATIO_FLOOR` and cover at least as well.

    Run on the preseason summaries, where the draw is the whole rating uncertainty; ``report`` also
    lists the in-season widths, reported only.
    """
    ratio = shift["width"] / fixed["width"] if fixed["width"] > 0 else float("inf")
    covers = shift["coverage"] >= fixed["coverage"]
    return {
        "width_shift": shift["width"],
        "width_fixed": fixed["width"],
        "width_ratio": ratio,
        "width_ratio_floor": WIDTH_RATIO_FLOOR,
        "coverage_shift": shift["coverage"],
        "coverage_fixed": fixed["coverage"],
        "status": "PASS" if ratio >= WIDTH_RATIO_FLOOR and covers else "BUG",
    }


def final_wins(schedule: pl.DataFrame, league_id: str) -> dict[int, int]:
    """Each team's regular-season wins from every scored regular-season game."""
    g = sim._regular_games(schedule, None, league_id).filter(
        pl.col("home_pts").is_not_null() & pl.col("away_pts").is_not_null()
    )
    winner = pl.when(pl.col("home_pts") > pl.col("away_pts")).then("home_team_id").otherwise("away_team_id")
    w = g.select(winner.alias("team_id")).group_by("team_id").len()
    teams = set(g["home_team_id"].to_list()) | set(g["away_team_id"].to_list())
    wins = dict(zip(w["team_id"].to_list(), w["len"].to_list(), strict=True))
    return {int(t): int(wins.get(t, 0)) for t in teams}


def cutoffs(schedule: pl.DataFrame, league: str, season: int) -> dict[str, dt.date]:
    """The preseason (first game date) and in-season cutoffs of a season's regular-season schedule.

    The in-season cutoff is the first game day on or after Dec 1 of the season's first year (NBA)
    or June 15 (WNBA). A season that opens after that date (the NBA's 2011-12, from Dec 25) has no
    in-season cutoff distinct from the preseason one and is scored at the preseason only.
    """
    dates = sim._regular_games(schedule, None, _LEAGUE_ID[league])["game_date"].unique().sort()
    month, day = _IN_SEASON[league]
    year = season - 1 if league == "nba" else season
    start = dates[0]
    out = {"preseason": start}
    later = dates.filter(dates >= dt.date(year, month, day))
    if later.len() and later[0] > start:
        out["in_season"] = later[0]
    return out


def run_season(league: str, season: int, *, n_sims: int, seed: int) -> pl.DataFrame:
    """Team rows (:func:`team_rows`) for one season, both cutoffs and both arms."""
    league_id = _LEAGUE_ID[league]
    _, load_schedule, offset = sim._league_loaders(league_id)
    schedule = load_schedule([season - offset])
    final = final_wins(schedule, league_id)
    lam = sim.SIM_CONSTANTS[league_id][0]
    frames = []
    for name, cut in cutoffs(schedule, league, season).items():
        games, model, records, season_games, _ = sim._load_inputs(season, cut, league_id, cup="never")
        penalty_sd = sim._load_inputs(season, cut, league_id, cup="never", prior_poss=lam)[1]
        cols = games.select(*sim._SIM_COLS).with_columns(pl.lit(season, dtype=pl.Int64).alias("season"))
        for arm, unc, m in (("shift", True, model), ("fixed", False, model), ("shift_penalty_sd", True, penalty_sd)):
            _, _, (teams, total_wins) = sim._simulate_season_draws(
                cols,
                m,
                league_id=league_id,
                n_sims=n_sims,
                seed=seed,
                prior_records=records,
                rating_uncertainty=unc,
                as_of_date=cut,
                season_games=season_games,
            )
            frames.append(
                team_rows(teams, total_wins, final).with_columns(
                    pl.lit(season, dtype=pl.Int64).alias("season"),
                    pl.lit(name).alias("cutoff"),
                    pl.lit(str(cut)).alias("as_of"),
                    pl.lit(arm).alias("arm"),
                )
            )
    return pl.concat(frames)


def report(rows: pl.DataFrame, league: str) -> dict[str, Any]:
    """The (a) report: per cutoff and arm, every level's summary; the readings; the no-op check."""
    out: dict[str, Any] = {"league": league, "cutoffs": {}}
    for cut in ("preseason", "in_season"):
        entry: dict[str, Any] = {}
        for arm in ("shift", "fixed", "shift_penalty_sd"):
            sub = rows.filter((pl.col("cutoff") == cut) & (pl.col("arm") == arm))
            entry[arm] = {f"{round(lv * 100)}": summarize(sub, lv) for lv in LEVELS}
        entry["reading"] = reading(entry["shift"][f"{round(GATE_LEVEL * 100)}"])
        entry["tolerance"] = tolerance(entry["shift"][f"{round(GATE_LEVEL * 100)}"])
        out["cutoffs"][cut] = entry
    gate = f"{round(GATE_LEVEL * 100)}"
    out["noop_check"] = noop_check(
        out["cutoffs"]["preseason"]["shift"][gate], out["cutoffs"]["preseason"]["fixed"][gate]
    )
    ins = out["cutoffs"]["in_season"]
    out["noop_check"]["in_season_reported_only"] = {
        "width_shift": ins["shift"][gate]["width"],
        "width_fixed": ins["fixed"][gate]["width"],
        "coverage_shift": ins["shift"][gate]["coverage"],
        "coverage_fixed": ins["fixed"][gate]["coverage"],
    }
    readings = [out["cutoffs"][c]["reading"] for c in ("preseason", "in_season")]
    out["reading"] = (
        "BUG"
        if out["noop_check"]["status"] == "BUG"
        else "WATCH"
        if "WATCH" in readings
        else "TOO_FEW_SEASONS"
        if "TOO_FEW_SEASONS" in readings
        else "OK"
    )
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--league", choices=sorted(_LEAGUE_ID), required=True)
    ap.add_argument("--seasons", type=int, nargs="+", default=None, help="END years; default SEASONS[league]")
    ap.add_argument("--n-sims", type=int, default=10_000)
    ap.add_argument("--seed", type=int, default=20261103)
    ap.add_argument("--near-holdout", action="store_true", help="also score NEAR_HOLDOUT[league], labelled")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args(argv)
    near = NEAR_HOLDOUT[args.league]
    seasons = args.seasons or [*SEASONS[args.league], *(sorted(near) if args.near_holdout else [])]
    bad = sorted(set(seasons) & set(EXCLUDED[args.league]))
    if bad:
        ap.error(f"excluded seasons {bad}: {[EXCLUDED[args.league][s] for s in bad]}")
    if not args.near_holdout and set(seasons) & set(near):
        ap.error(f"seasons {sorted(set(seasons) & set(near))} are near-holdout ({near}); pass --near-holdout")
    print(f"{args.league}: seasons {seasons}; excluded {EXCLUDED[args.league]}", flush=True)
    frames = []
    for season in seasons:
        frames.append(run_season(args.league, season, n_sims=args.n_sims, seed=args.seed))
        print(season, "done", flush=True)
    rows = pl.concat(frames)
    result = {
        "check": "lifecycle (a) win-total interval coverage",
        "model": f"{args.league}/pro_season_sim",
        "n_sims": args.n_sims,
        "seed": args.seed,
        "seasons": seasons,
        "excluded": {str(k): v for k, v in EXCLUDED[args.league].items()},
        "near_holdout_seasons": {str(k): v for k, v in near.items() if k in seasons},
        **report(rows, args.league),
    }
    text = json.dumps(result, indent=2, default=str)
    print(text)
    if args.out:
        args.out.write_text(text + "\n", encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
