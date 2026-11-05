"""Fit the BB-D20 play-style archetype artifacts (NBA offense and defense players, NBA defense teams, MBB/WBB players).

For each model: read the committed feature corpus, choose ``k`` (and, for the NBA team models,
the feature set: :data:`~sportsdataverse.nba.nba_play_style.NBA_TEAM_FEATURE_CANDIDATES`) on the
training seasons only, fit the frozen centers on the training seasons
(:func:`sportsdataverse._common.play_style.fit_play_style`), then measure the stability on the
holdout seasons once (:func:`stability_report`) and write the artifact with its sweep and report.

Selection never reads a holdout season. Each candidate is scored by
:func:`~sportsdataverse._common.play_style.selection_score`: the mean of the inner-split ARI (fit
on all but the last two training seasons, scored on those two) and the bootstrap ARI over players
or teams on the training seasons; the highest score wins, ties to the smaller ``k`` and the
shorter feature set. College has one training season, so the bootstrap alone scores it. The ``k``
range starts at 5 archetypes for players and 4 for teams so the chips carry information.

The training corpus is committed: each model's features for every season live in
``tests/fixtures/bb_play_style/<artifact name>.parquet`` (floats rounded to 6 decimals; its
sha256 is recorded in the artifact), so the exact training and holdout rows of a bundled artifact
are reproducible and the tests refit from them. ``--rebuild`` re-reads the releases and
overwrites the corpus first.

Cadence: the artifacts are frozen. Refit (with ``--rebuild``) once a season is complete, to move
the holdout window forward, or when a feature or a release changes; then re-pin the test gates
from the new ``stability`` blocks and update the ``model_ledger.json`` rows.

Usage (from the repo root)::

    uv run python tools/models/play_style/fit_play_style.py [--rebuild] [--only nba_offense_player]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import polars as pl

from sportsdataverse._common.play_style import fit_play_style, selection_score, stability_report
from sportsdataverse.nba.nba_play_style import NBA_PLAY_STYLE_FIRST_SEASON

ROOT = Path(__file__).resolve().parents[3]
CORPUS = ROOT / "tests" / "fixtures" / "bb_play_style"
SEED = 20261105


def _nba(side: str, entity: str) -> dict:
    from sportsdataverse.nba.nba_play_style import (
        NBA_PLAY_STYLE_FEATURES,
        NBA_PLAY_STYLE_FIRST_SEASON,
        NBA_PLAY_STYLE_PHRASES,
        NBA_TEAM_FEATURE_CANDIDATES,
        nba_play_style_features,
    )

    first = NBA_PLAY_STYLE_FIRST_SEASON[(side, entity)]
    sets = NBA_TEAM_FEATURE_CANDIDATES.get((side, entity), {"all": NBA_PLAY_STYLE_FEATURES[(side, entity)]})
    return {
        "name": f"nba_play_style_{side}_{entity}",
        "path": ROOT / "sportsdataverse" / "nba" / "models" / f"nba_play_style_{side}_{entity}.json",
        "build": lambda: nba_play_style_features(list(range(first, 2027)), side=side, entity=entity),
        "feature_sets": sets,
        "phrases": NBA_PLAY_STYLE_PHRASES[(side, entity)],
        "entity": f"{entity}_id",
        "train": list(range(first, 2023)),
        "holdout": [2023, 2024, 2025, 2026],
        "k_range": range(5, 10) if entity == "player" else range(4, 8),
    }


def _college(league: str) -> dict:
    from sportsdataverse.mbb.mbb_play_style import (
        COLLEGE_PLAY_STYLE_FEATURES,
        COLLEGE_PLAY_STYLE_PHRASES,
        college_play_style_features,
    )

    prefix = "mbb" if league == "mens" else "wbb"
    return {
        "name": f"{prefix}_play_style_player",
        "path": ROOT / "sportsdataverse" / "mbb" / "models" / f"{prefix}_play_style_player.json",
        "build": lambda: college_play_style_features([2025, 2026], league=league),
        "feature_sets": {"all": COLLEGE_PLAY_STYLE_FEATURES},
        "phrases": COLLEGE_PLAY_STYLE_PHRASES,
        "entity": "player_id",
        "train": [2025],
        "holdout": [2026],
        "k_range": range(5, 10),
    }


MODELS = {
    **{f"nba_{s}_{e}": (lambda s=s, e=e: _nba(s, e)) for s, e in NBA_PLAY_STYLE_FIRST_SEASON},
    "mbb_player": lambda: _college("mens"),
    "wbb_player": lambda: _college("womens"),
}


def _features(spec: dict, rebuild: bool) -> pl.DataFrame:
    """The committed corpus, or (``rebuild``) the releases' features rounded and written over it."""
    path = CORPUS / f"{spec['name']}.parquet"
    if path.exists() and not rebuild:
        return pl.read_parquet(path)
    feats = spec["build"]().with_columns(pl.col(pl.Float64).round(6)).sort("season", spec["entity"])
    path.parent.mkdir(parents=True, exist_ok=True)
    feats.write_parquet(path, compression="zstd", compression_level=19)
    return pl.read_parquet(path)


def _sweep(key: str, spec: dict, feats: pl.DataFrame) -> list[dict]:
    """Training-only scores of every (feature set, k) candidate, shorter sets first."""
    sweep = []
    for name, cols in sorted(spec["feature_sets"].items(), key=lambda kv: len(kv[1])):
        for k in spec["k_range"]:
            sc = selection_score(feats, cols, k=k, train_seasons=spec["train"], entity=spec["entity"], seed=SEED)
            sweep.append({"feature_set": name, "k": k, **sc})
            print(key, sweep[-1], flush=True)
    return sweep


def fit_one(key: str, rebuild: bool = False) -> dict:
    """Choose the candidate on training seasons, fit it, report the holdout stability, write the artifact."""
    spec = MODELS[key]()
    feats = _features(spec, rebuild)
    sweep = _sweep(key, spec, feats)
    best = max(enumerate(sweep), key=lambda iv: (iv[1]["score"], -iv[0]))[1]
    cols = list(spec["feature_sets"][best["feature_set"]])
    art = fit_play_style(feats, cols, k=best["k"], train_seasons=spec["train"], phrases=spec["phrases"], seed=SEED)
    art["holdout_seasons"] = spec["holdout"]
    art["selection"] = "max training-only score: mean of inner-split ARI and entity-bootstrap ARI (holdout unread)"
    art["feature_set"] = best["feature_set"]
    art["k_sweep"] = sweep
    art["stability"] = stability_report(feats, art, entity=spec["entity"], holdout_seasons=spec["holdout"])
    art["script"] = "tools/models/play_style/fit_play_style.py"
    corpus = CORPUS / f"{spec['name']}.parquet"
    art["corpus"] = corpus.relative_to(ROOT).as_posix()
    art["corpus_sha256"] = hashlib.sha256(corpus.read_bytes()).hexdigest()
    art["n_by_season"] = {str(s): n for s, n in feats.group_by("season").len().sort("season").iter_rows()}
    spec["path"].parent.mkdir(parents=True, exist_ok=True)
    spec["path"].write_text(json.dumps(art, indent=1) + "\n", encoding="utf-8")
    print(key, best, "labels =", art["labels"], json.dumps(art["stability"], indent=1), flush=True)
    return art


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--rebuild", action="store_true", help="re-read the releases into the corpus first")
    ap.add_argument("--only", choices=sorted(MODELS), action="append")
    args = ap.parse_args(argv)
    for key in args.only or sorted(MODELS):
        fit_one(key, args.rebuild)
    return 0


if __name__ == "__main__":
    sys.exit(main())
