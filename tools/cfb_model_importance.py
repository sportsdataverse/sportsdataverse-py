"""Write ``<model>.importance.json`` beside every bundled CFB booster (task 3.8, C-L28).

The importance is read off the booster itself (``Booster.get_score``), so it needs no training
data and reproduces from the repo alone: ``gain`` and ``cover`` (means per split), ``total_gain``
and ``weight`` (split count) per feature, in the booster's (= the card's) feature order. A feature
the booster never splits on gets zeros rather than vanishing, so "the model ignores X" is visible.
The multiclass EP booster's scores pool all seven class trees.

``sha256`` pins the booster bytes the numbers came from; ``tests/cfb/test_model_cards.py``
recomputes every file through :func:`importance` and fails on any drift, so a swapped booster
without a regenerated artifact (or a hand-edited number) goes red.

Usage::

    uv run --frozen python tools/cfb_model_importance.py          # rewrite every artifact
    uv run --frozen python tools/cfb_model_importance.py --check  # exit 1 if any is stale
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import xgboost as xgb

MODEL_DIR = Path(__file__).resolve().parents[1] / "sportsdataverse" / "cfb" / "models"
_TYPES = ("gain", "total_gain", "weight", "cover")


def _round(x: float) -> float:
    # 10 significant digits: stable across platforms for one xgboost version, and the test
    # compares with a relative tolerance so a last-bit change between versions is not drift.
    return float(f"{x:.10g}")


def importance(ubj: Path) -> dict[str, Any]:
    """Importance record for one booster file, in its feature order."""
    booster = xgb.Booster()
    booster.load_model(str(ubj))
    names = list(booster.feature_names or [])
    if not names:
        raise ValueError(f"{ubj.name} carries no feature names")
    scores = {t: booster.get_score(importance_type=t) for t in _TYPES}
    total = sum(scores["total_gain"].values()) or 1.0
    return {
        "model": ubj.stem,
        "sha256": hashlib.sha256(ubj.read_bytes()).hexdigest(),
        "method": (
            "xgboost Booster.get_score: gain and cover are means per split, total_gain sums them, "
            "weight counts splits; a multiclass booster pools all its class trees"
        ),
        "features": [
            {
                "feature": f,
                **{t: _round(scores[t].get(f, 0.0)) for t in _TYPES},
                "share_total_gain": _round(scores["total_gain"].get(f, 0.0) / total),
            }
            for f in names
        ],
    }


def _path(ubj: Path) -> Path:
    return ubj.with_name(f"{ubj.stem}.importance.json")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--check", action="store_true", help="fail if any artifact is missing or stale")
    args = ap.parse_args(argv)
    stale = []
    for ubj in sorted(MODEL_DIR.glob("*.ubj")):
        text = json.dumps(importance(ubj), indent=2) + "\n"
        out = _path(ubj)
        if args.check:
            if not out.exists() or out.read_text(encoding="utf-8") != text:
                stale.append(out.name)
        else:
            out.write_text(text, encoding="utf-8", newline="\n")
            print(f"wrote {out.name}")
    if stale:
        print("stale importance artifacts: " + ", ".join(stale), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
