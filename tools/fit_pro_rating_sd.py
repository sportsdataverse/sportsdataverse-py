# tools/fit_pro_rating_sd.py
"""Candidate refit of the NBA / WNBA season sim's rating-uncertainty pseudo-count. FAILED its gate; not shipped.

The sim draws one rating shift per team per simulated season,
``N(0, s2 / (n_off + k) + s2 / (n_def + k))`` (``s2`` the variance of points per 100 per
possession, ``n`` the team's possessions so far), with ``k`` the ridge penalty, so
the preseason sd is ``sqrt(2 s2 / penalty)``, a number the penalty's log-loss choice sets, not a
measure of how far the prior misses. On seasons outside the fit the preseason 80% win bands covered
93% (NBA) and 99% (WNBA) against a mass of about 82% (``tools/models/lifecycle/coverage.py``).

**The fit.** For each TRAIN season, the preseason prior (:func:`nba_season_sim.preseason_prior`:
last season's final ratings, regressed) is compared with the season's own final ratings, the
possession ridge on every regular-season game at a near-zero penalty (:data:`TRUTH_LAMBDA`), so the
truth is not shrunk toward average. Per returning team ``miss = final net - prior net`` (both
centred on the league). The final rating's own sampling variance ``s2 / (n_off + lam) +
s2 / (n_def + lam)`` is subtracted: ``V = T / (T - 1) * mean(miss^2) - mean(noise)`` (``T`` teams;
the factor undoes the within-season centring), the variance of the prior's
real miss. Matching the draw's preseason variance ``2 s2_prior / k`` to it gives
``k = 2 * mean(s2_prior) / mean(V)`` over the train seasons (``s2_prior`` last season's, the one the
sim uses). Expansion teams are left out (their prior is a quantile default, not a regressed rating).

**The gate** (stated before the run): ``coverage.py`` with the fitted ``k`` in place of the penalty
on its clean seasons (NBA and WNBA 2010-2017), the 80% band within ``max(0.05, 2 SE)`` of its own
mass at both cutoffs (0.05 where too few seasons for an SE) and the no-op check passing.

**Result (2026-11-25): not shipped; the sim keeps the penalty.** The gate run used the first
fit (NBA k 3,090, preseason sd 3.0 per 100 against 5.8 shipped; WNBA k 1,200, 4.6 against 11.4).
With the centring correction above the fit is NBA k 2,950 (sd 3.06), WNBA k 1,060 (sd 4.95); not
re-gated, since a third look at 2010-2017 can only be a comparison.

* NBA preseason: a real but narrow FAIL, 80% coverage 0.746 against a mass of 0.822 (tolerance
  0.070, so 0.077 off).
* WNBA preseason: passed, 0.792 against 0.834.
* In-season: not tested consistently. The gate put ``k`` into the in-season sd
  ``s2 / (n + k)`` while the point estimate still shrinks at the penalty ``lam``; the error
  variance of that estimate is ``(lam / (n + lam))^2 * V + n * s2 / (n + lam)^2`` per side, about
  1.4x (NBA, Dec 1) and 1.75x (WNBA, June 15) the variance the gate used. The in-season failures (NBA
  0.748 against 0.831, WNBA 0.729 against 0.853) are largely that.

The gate seasons had been seen before (the first coverage run found the wide preseason bands on
them). Reports: ClaudeCowork ``data/2026-11-25-model-lifecycle/refit_candidate/``.

The train seasons are the sim's own (``SIM_CONSTANTS``). Inputs are the
``{nba,wnba}_stats_possessions`` / ``_schedules`` releases (network), cached by
``fit_pro_season_ratings.load_season`` under ``--cache``. ``--out`` writes the report.

Usage::

    uv run python tools/fit_pro_rating_sd.py --league nba --train 2018 2019 2022 2023
    uv run python tools/fit_pro_rating_sd.py --league wnba --train 2018 2019 2021
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fit_pro_season_ratings import load_season  # noqa: E402

from sportsdataverse._common.team_strength import possession_ridge_ratings  # noqa: E402
from sportsdataverse.nba.nba_season_sim import SIM_CONSTANTS, _ridge_s2, preseason_prior  # noqa: E402

#: The final ratings' penalty: near zero, so the truth proxy is not shrunk (its noise is subtracted).
TRUTH_LAMBDA = 1.0
_PRO = {"nba": "00", "wnba": "10"}


def _teams(games: pl.DataFrame) -> list[int]:
    return sorted(set(games["home_team_id"].to_list()) | set(games["away_team_id"].to_list()))


def season_miss(lid: str, season: int, cache: Path) -> dict[str, float]:
    """One train season: the prior's raw and noise-corrected squared miss, and last season's ``s2``."""
    rows, games = load_season(lid, season, cache)
    prev_rows, prev_games = load_season(lid, season - 1, cache)
    prior = preseason_prior(
        prev_rows.rename({"date": "game_date"}), _teams(games), league_id=lid, prev_teams=_teams(prev_games)
    )
    assert prior is not None, f"no prior for {season}"
    final = possession_ridge_ratings(rows, lam=TRUTH_LAMBDA)
    n_off = rows.group_by(pl.col("offense_team_id").alias("team_id")).len("n_off")
    n_def = rows.group_by(pl.col("defense_team_id").alias("team_id")).len("n_def")
    pri = prior.ratings.filter(~pl.col("new_team")).select(
        "team_id", (pl.col("off_dev") - pl.col("def_dev")).alias("prior_net")
    )
    for frame in (n_off, n_def, pri):
        assert frame.schema["team_id"] == final.schema["team_id"], "team_id dtype differs across the joins"
    s2 = _ridge_s2(rows)
    j = (
        final.select("team_id", "adj_net_rtg")
        .join(n_off, on="team_id", how="inner", validate="1:1")
        .join(n_def, on="team_id", how="inner", validate="1:1")
        .join(pri, on="team_id", how="inner", validate="1:1")
        .with_columns(
            (pl.col("adj_net_rtg") - pl.col("adj_net_rtg").mean()).alias("final_net"),
            (pl.col("prior_net") - pl.col("prior_net").mean()).alias("prior_net"),
            (s2 / (pl.col("n_off") + TRUTH_LAMBDA) + s2 / (pl.col("n_def") + TRUTH_LAMBDA)).alias("noise"),
        )
    )
    miss2 = float(((j["final_net"] - j["prior_net"]) ** 2).mean())
    noise = float(j["noise"].mean())
    return {
        "teams": j.height,
        "mean_miss2": round(miss2, 4),
        "mean_noise": round(noise, 4),
        "v": round(j.height / (j.height - 1) * miss2 - noise, 4),
        "s2_prior": round(prior.s2, 2),
        "shipped_preseason_sd": round(float(np.sqrt(2.0 * prior.s2 / SIM_CONSTANTS[lid][0])), 4),
    }


def fit(lg: str, train: list[int], cache: Path) -> dict[str, object]:
    lid = _PRO[lg]
    by = {str(s): season_miss(lid, s, cache) for s in train}
    v = float(np.mean([b["v"] for b in by.values()]))
    s2 = float(np.mean([b["s2_prior"] for b in by.values()]))
    if v <= 0:
        raise ValueError(f"the prior's miss is no larger than the final ratings' noise ({v})")
    k = 2.0 * s2 / v
    return {
        "league": lg,
        "train": train,
        "truth_lambda": TRUTH_LAMBDA,
        "by_season": by,
        "mean_v": round(v, 4),
        "mean_s2_prior": round(s2, 2),
        "preseason_sd_fit": round(float(np.sqrt(v)), 4),
        "pick": round(k, -1),  # possessions, to 10
        "ridge_penalty": SIM_CONSTANTS[lid][0],  # what the sim uses: the pick failed its gate
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--league", choices=sorted(_PRO), required=True)
    ap.add_argument("--train", type=int, nargs="+", required=True)
    ap.add_argument("--cache", type=Path, default=Path(".cache/pro_season"))
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args(argv)
    report = fit(args.league, args.train, args.cache)
    if args.out:
        args.out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
