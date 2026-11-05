"""Fit the BB-D20 play-style archetype artifacts (NBA off/def x player/team, MBB/WBB player).

For each model: build the features for every season, sweep ``k``, fit the frozen centers on the
training seasons only (:func:`sportsdataverse._common.play_style.fit_play_style`), measure the
stability on the holdout seasons (:func:`stability_report`) and write the artifact with its sweep
and report. ``k`` is the most stable value in the sweep range: the highest mean of the holdout ARI
and the training bootstrap ARI (ties to the smaller ``k``). The range starts at 5 archetypes for
players and 4 for teams so the chips carry information.

The training corpus is committed: each model's features for every season live in
``tests/fixtures/bb_play_style/<artifact name>.parquet`` (floats rounded to 6 decimals), so the
exact training and holdout rows of a bundled artifact are reproducible and the tests refit from
them. ``--rebuild`` re-reads the releases and overwrites the corpus first.

Usage (from the repo root)::

    uv run python tools/models/play_style/fit_play_style.py [--rebuild] [--only nba_offense_player]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import polars as pl

from sportsdataverse._common.play_style import fit_play_style, stability_report

ROOT = Path(__file__).resolve().parents[3]
CORPUS = ROOT / "tests" / "fixtures" / "bb_play_style"
SEED = 20261105


def _nba(side: str, entity: str) -> dict:
    from sportsdataverse.nba.nba_play_style import (
        NBA_PLAY_STYLE_FEATURES,
        NBA_PLAY_STYLE_FIRST_SEASON,
        NBA_PLAY_STYLE_PHRASES,
        nba_play_style_features,
    )

    first = NBA_PLAY_STYLE_FIRST_SEASON[(side, entity)]
    return {
        "name": f"nba_play_style_{side}_{entity}",
        "path": ROOT / "sportsdataverse" / "nba" / "models" / f"nba_play_style_{side}_{entity}.json",
        "build": lambda: nba_play_style_features(list(range(first, 2027)), side=side, entity=entity),
        "features": NBA_PLAY_STYLE_FEATURES[(side, entity)],
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
        "features": COLLEGE_PLAY_STYLE_FEATURES,
        "phrases": COLLEGE_PLAY_STYLE_PHRASES,
        "entity": "player_id",
        "train": [2025],
        "holdout": [2026],
        "k_range": range(5, 10),
    }


MODELS = {
    **{f"nba_{s}_{e}": (lambda s=s, e=e: _nba(s, e)) for s in ("offense", "defense") for e in ("player", "team")},
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


def fit_one(key: str, rebuild: bool = False) -> dict:
    """Sweep k, fit the chosen k on the training seasons and write the artifact."""
    spec = MODELS[key]()
    feats = _features(spec, rebuild)
    cols, entity = list(spec["features"]), spec["entity"]
    sweep = []
    for k in spec["k_range"]:
        art = fit_play_style(feats, cols, k=k, train_seasons=spec["train"], phrases=spec["phrases"], seed=SEED)
        rep = stability_report(feats, art, entity=entity, holdout_seasons=spec["holdout"])
        score = (rep["holdout_ari"] + rep["train_bootstrap_ari"]) / 2
        sweep.append(
            {
                "k": k,
                "score": round(score, 4),
                **{
                    m: rep[m]
                    for m in ("holdout_ari", "train_bootstrap_ari", "holdout_persistence", "holdout_chance_persistence")
                },
            }
        )
        print(key, sweep[-1], flush=True)
    best = max(sweep, key=lambda r: (r["score"], -r["k"]))["k"]
    art = fit_play_style(feats, cols, k=best, train_seasons=spec["train"], phrases=spec["phrases"], seed=SEED)
    art["holdout_seasons"] = spec["holdout"]
    art["k_sweep"] = sweep
    art["stability"] = stability_report(feats, art, entity=entity, holdout_seasons=spec["holdout"])
    art["script"] = "tools/models/play_style/fit_play_style.py"
    art["corpus"] = f"tests/fixtures/bb_play_style/{spec['name']}.parquet"
    art["n_by_season"] = {str(s): n for s, n in feats.group_by("season").len().sort("season").iter_rows()}
    spec["path"].parent.mkdir(parents=True, exist_ok=True)
    spec["path"].write_text(json.dumps(art, indent=1) + "\n", encoding="utf-8")
    print(key, "k =", best, "labels =", art["labels"], json.dumps(art["stability"], indent=1), flush=True)
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
