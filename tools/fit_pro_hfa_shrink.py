# tools/fit_pro_hfa_shrink.py
"""Fit the NBA / WNBA as-of home term's pseudo-count (``nba_season_sim.HFA_PSEUDO_POSS``).

:func:`sportsdataverse.nba.nba_season_sim.as_of_game_model` shrinks the possession ridge's home
term toward last season's (the preseason prior's ``hfa``) with ``k`` non-neutral possessions of
prior weight. With no prior, or after a season with no home games (the 2020 bubbles), it stays free:
there is no as-of target. Before, the term was always free: early in a season it swung by several
points a day.

**The fit.** Every regular-season game of the TRAIN seasons is rated as of its date, exactly as
BB-D25 rates it (:func:`sportsdataverse.nba.nba_rated_prior.rated_home_probs`: the possessions of
games dated strictly before it, last season's prior), once per ``k`` in :data:`GRID`. ``k``
minimizes pooled log loss. The holdout seasons are not read here: the BB-D25 gate
(``tools/models/nba_prediction/wp_model_cards.py``) scores them once with the frozen ``k``.

A season whose prior has no identified home term (WNBA 2021, after the 2020 bubble) is scored at
every ``k`` but is unshrunk at all of them, so it does not move the pick; the report records each
season's prior home term and whether it was identified.

Inputs are the ``{nba,wnba}_stats_possessions`` / ``_schedules`` releases (network), cached by
``fit_pro_season_ratings.load_season`` under ``--cache``. The report is committed at
``tests/fixtures/nba_prediction/hfa_shrink_<lg>.json``; ``tests/nba/test_nba_season_sim.py``
checks the constants against it.

Usage::

    uv run python tools/fit_pro_hfa_shrink.py --league nba --train 2018 2019 2022 2023
    uv run python tools/fit_pro_hfa_shrink.py --league wnba --train 2018 2019 2021
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fit_preseason_prior import paired  # noqa: E402
from fit_pro_season_ratings import load_season  # noqa: E402

from sportsdataverse._common.metrics import brier_score, log_loss_score  # noqa: E402
from sportsdataverse._common.team_strength import RIDGE_LAMBDA, possession_ridge_ratings  # noqa: E402
from sportsdataverse.nba.nba_rated_prior import rated_home_probs  # noqa: E402
from sportsdataverse.nba.nba_season_sim import preseason_prior  # noqa: E402

GRID = [0.0, 2500.0, 5000.0, 10000.0, 20000.0, 40000.0, 80000.0, 160000.0, 320000.0, 640000.0]
EARLY_DAYS = 28
_PRO = {"nba": "00", "wnba": "10"}
OUT = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "nba_prediction"


def _teams(games: pl.DataFrame) -> list[int]:
    return sorted(set(games["home_team_id"].to_list()) | set(games["away_team_id"].to_list()))


def season_frame(lid: str, season: int, cache: Path) -> tuple[pl.DataFrame, dict[str, object]]:
    """Every regular-season game of ``season`` rated at each ``k`` (``p_<k>``) with its outcome, and the season's home terms."""
    rows, games = load_season(lid, season, cache)
    prev_rows, prev_games = load_season(lid, season - 1, cache)
    rows = rows.rename({"date": "game_date"})
    prior = preseason_prior(
        prev_rows.rename({"date": "game_date"}), _teams(games), league_id=lid, prev_teams=_teams(prev_games)
    )
    assert prior is not None, f"no prior for {season}"
    neutral = rows.group_by("game_id").agg(pl.col("neutral_site").fill_null(False).any())
    assert games.schema["game_id"] == neutral.schema["game_id"], "game_id dtype differs"
    games = (
        games.rename({"date": "game_date"})
        .join(neutral, on="game_id", how="left", validate="1:1")
        .filter(pl.col("margin") != 0)
        .with_columns(pl.col("neutral_site").fill_null(False), (pl.col("margin") > 0).cast(pl.Float64).alias("y"))
        .sort("game_date", "game_id")
    )
    out = games.select("season", "game_id", "game_date", "neutral_site", "y")
    start = games["game_date"].min()
    for k in GRID:
        r = rated_home_probs(rows, games, league_id=lid, prior=prior, hfa_poss=k)
        assert r["game_id"].to_list() == out["game_id"].to_list()
        out = out.with_columns(r["rated_home_prob"].alias(f"p_{k:g}"), r["hfa"].alias(f"hfa_{k:g}"))
    terms = {
        "prior_hfa": round(prior.hfa, 4),
        "prior_hfa_identified": prior.hfa_identified,
        "final_hfa": round(float(possession_ridge_ratings(rows, lam=RIDGE_LAMBDA[lid])["hfa"][0]), 4),
    }
    return out.with_columns(((pl.col("game_date") - start).dt.total_days() < EARLY_DAYS).alias("early")), terms


def _scores(frame: pl.DataFrame) -> dict[str, dict[str, float]]:
    y = frame["y"].to_numpy()
    return {
        f"{k:g}": {
            "log_loss": round(log_loss_score(y, frame[f"p_{k:g}"].to_numpy()), 5),
            "brier": round(brier_score(y, frame[f"p_{k:g}"].to_numpy()), 5),
        }
        for k in GRID
    }


def fit(lg: str, train: list[int], cache: Path) -> dict[str, object]:
    lid = _PRO[lg]
    parts = [season_frame(lid, s, cache) for s in train]
    tr = pl.concat([f for f, _ in parts])
    pooled = _scores(tr)
    best = min(GRID, key=lambda k: pooled[f"{k:g}"]["log_loss"])
    return {
        "league": lg,
        "train": train,
        "games": tr.height,
        "early_days": EARLY_DAYS,
        "grid": GRID,
        "train_scores": pooled,
        "train_scores_early": _scores(tr.filter(pl.col("early"))),
        "train_scores_by_season": {str(s): _scores(tr.filter(pl.col("season") == s)) for s in train},
        "paired_best_minus_unshrunk": paired(tr, f"p_{best:g}", "p_0"),
        "pick": best,
        "home_terms_by_season": {str(s): t for s, (_, t) in zip(train, parts, strict=True)},
        # the as-of home term (per 100) the games were rated with, unshrunk and at the pick
        "home_term_range": {
            f"{k:g}": [round(float(tr[f"hfa_{k:g}"].min()), 3), round(float(tr[f"hfa_{k:g}"].max()), 3)]
            for k in (0.0, best)
        },
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--league", choices=sorted(_PRO), required=True)
    ap.add_argument("--train", type=int, nargs="+", required=True)
    ap.add_argument("--cache", type=Path, default=Path(".cache/pro_season"))
    args = ap.parse_args(argv)
    report = fit(args.league, args.train, args.cache)
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / f"hfa_shrink_{args.league}.json"
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(json.dumps({k: report[k] for k in ("pick", "paired_best_minus_unshrunk", "train_scores")}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
