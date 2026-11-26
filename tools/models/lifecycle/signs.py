# tools/models/lifecycle/signs.py
"""Lifecycle check (c): per-season sign stability of a model's fitted coefficients.

A model refit per held-out season (an expanding-window fold per season) gives one estimate of each
coefficient per fold. For each coefficient, with its SE in that fold (clustered the way the model's
held-out evidence is, e.g. by date):

* **real flip** -- the sign differs between two folds and both estimates are more than 2 SE from
  zero. This is a finding: the feature means different things in different seasons. The folds are
  named.
* **noise flip** -- the sign differs, but every opposite-signed pair has a side within 2 SE of zero.
  Reported, and expected for near-zero terms.
* **stable** -- one sign in every fold.
* **unclassified** -- the sign differs but a fold has no usable SE (missing, not finite, or not
  positive), so real and noise cannot be told apart.

A boosted model passes its per-season mean signed TreeSHAP contribution of the top features as the
estimate (and that mean's SE), from the committed importance artifact. This check is never a gate on
its own: the owner reads a real flip on a shipped model as a WATCH at its next retire-rule season
(``retire.py`` takes no flip input; the flip is quoted beside that season's record).

Input: one JSON per fold, ``{"fold_season": int, "coef": {name: estimate}, "coef_se": {name: se}}``
(``intercept`` is skipped). Usage::

    uv run python tools/models/lifecycle/signs.py fold_2022.json fold_2023.json ... --out signs.json

Design: ClaudeCowork ``specs/2026-11-25-model-lifecycle-checks-design.md`` (c).
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Mapping, Sequence
from itertools import combinations
from pathlib import Path
from typing import Any

Z = 2.0


def classify(by_fold: Mapping[int, tuple[float, float | None]]) -> dict[str, Any]:
    """One coefficient's ``{fold: (estimate, se)}`` -> its class and the folds of any real flip."""
    folds = sorted(by_fold)
    signs = {f: (by_fold[f][0] > 0) - (by_fold[f][0] < 0) for f in folds}
    if len({s for s in signs.values() if s}) <= 1:
        return {"class": "stable", "real_flip_folds": []}
    real: list[list[int]] = []
    unknown = False
    for a, b in combinations(folds, 2):
        if signs[a] * signs[b] >= 0:
            continue
        (ea, sa), (eb, sb) = by_fold[a], by_fold[b]
        if sa is None or sb is None:
            unknown = True
        elif abs(ea) > Z * sa and abs(eb) > Z * sb:
            real.append([a, b])
    if real:
        return {"class": "real_flip", "real_flip_folds": real}
    return {"class": "unclassified" if unknown else "noise_flip", "real_flip_folds": []}


def sign_table(folds: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """The (c) table over fold dicts: per coefficient, each fold's estimate and SE and the class."""
    seasons = [int(f["fold_season"]) for f in folds]
    if len(seasons) < 2:
        raise ValueError(f"sign stability needs at least two folds, got {seasons}")
    if len(set(seasons)) != len(seasons):
        raise ValueError(f"duplicate fold seasons {seasons}")
    names = sorted({k for f in folds for k in f["coef"]} - {"intercept"})
    table: dict[str, Any] = {}
    for name in names:
        missing = [int(f["fold_season"]) for f in folds if name not in f["coef"]]
        if missing:
            raise ValueError(f"{name} is not in folds {missing}: a fixed design has every coefficient in every fold")
        by_fold = {int(f["fold_season"]): (float(f["coef"][name]), _se(f, name)) for f in folds}
        table[name] = {"by_fold": {str(k): {"est": e, "se": s} for k, (e, s) in by_fold.items()}, **classify(by_fold)}
    counts: dict[str, int] = {}
    for entry in table.values():
        counts[entry["class"]] = counts.get(entry["class"], 0) + 1
    return {"folds": sorted(seasons), "counts": counts, "coefficients": table}


def _se(fold: Mapping[str, Any], name: str) -> float | None:
    se = (fold.get("coef_se") or {}).get(name)
    if se is None:
        return None
    se = float(se)
    return se if math.isfinite(se) and se > 0 else None  # an unusable SE is no SE


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("folds", type=Path, nargs="+", help="one fold JSON per held-out season")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args(argv)
    result = sign_table([json.loads(p.read_text(encoding="utf-8")) for p in args.folds])
    text = json.dumps(result, indent=2)
    print(text)
    if args.out:
        args.out.write_text(text + "\n", encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
