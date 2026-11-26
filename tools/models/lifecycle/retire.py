# tools/models/lifecycle/retire.py
"""Lifecycle check (b): the retire rule for shipped models, and the ledger's lifecycle fields.

**The ledger** (``tools/models/model_ledger.json``, schema 1.1). Every row carries ``baseline``,
``primary_metric``, ``adoption_margin``, ``status`` and ``lifecycle_note``
(:func:`check_ledger`). ``baseline`` names the model the shipped one was shown to beat or tie on
held-out data and ``primary_metric`` that comparison's metric and SE clustering; both are null when
no committed evidence holds one, and ``lifecycle_note`` then says why. ``adoption_margin`` is the
comparison's paired ``model - baseline`` difference in loss orientation (negative = model better),
null unless the evidence states it. ``status`` is ``active`` or ``retired``; only the owner sets
``retired``, with a ``retired_reason``.

**The rule.** After each completed season past a model's holdout, the shipped model is scored once
against its named baseline on that season: the provenance's metric, paired, with the provenance's
clustering, no refit. One :func:`season_record` per season, in loss orientation (for a
higher-is-better metric pass ``baseline - model``). Then :func:`retire_status`:

* **RETIRE-candidate** if two consecutive scored seasons fail to beat the baseline (the upper 97.5%
  bound of ``model - baseline`` is >= 0), or one season is worse than the baseline by more than
  ``|adoption_margin|`` at 1.96 SE (its lower 97.5% bound ``diff - Z * se`` above the margin; the
  owner's 2026-11-25 change to the spec's point-estimate trigger, which a model adopted on a tie
  would trip on noise alone in about one season in four);
* **WATCH** if the latest season fails and neither trigger holds;
* **KEEP** otherwise; **UNSCORED** before any season past the holdout is scored.

The rule runs only on a row with both a ``baseline`` and ``holdout_seasons`` (a LOSO-only or
in-sample comparison names no season past the fit).

The action is the owner's: ``status: retired`` with the reason, a one-release warning through
``sportsdataverse/_deprecation.py``, then the public function returns the baseline. Reinstating
needs a new fit with its own holdout; a model is never re-adopted on seasons it already failed.

Usage::

    uv run python tools/models/lifecycle/retire.py --check-ledger
    uv run python tools/models/lifecycle/retire.py --league nba --model pro_season_sim --records seasons.json

``seasons.json`` is a list of :func:`season_record` dicts. Design: ClaudeCowork
``specs/2026-11-25-model-lifecycle-checks-design.md`` (b).
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np

LEDGER = Path(__file__).resolve().parents[1] / "model_ledger.json"
LIFECYCLE_FIELDS = ("baseline", "primary_metric", "adoption_margin", "status", "lifecycle_note")
STATUSES = ("active", "retired")
#: one-sided 97.5% normal quantile
Z = 1.959963984540054


def check_ledger(ledger: Mapping[str, Any]) -> list[str]:
    """Every problem with the ledger's lifecycle fields; an empty list means it is consistent."""
    problems: list[str] = []
    seen: set[tuple[str, str]] = set()
    for row in ledger["models"]:
        key = (row.get("league"), row.get("model"))
        name = f"{key[0]}/{key[1]}"
        if key in seen:
            problems.append(f"{name}: duplicate row")
        seen.add(key)  # type: ignore[arg-type]
        missing = [f for f in LIFECYCLE_FIELDS if f not in row]
        if missing:
            problems.append(f"{name}: missing {missing}")
            continue
        if row["status"] not in STATUSES:
            problems.append(f"{name}: status {row['status']!r} not in {STATUSES}")
        if row["status"] == "retired" and not row.get("retired_reason"):
            problems.append(f"{name}: retired without retired_reason")
        if not isinstance(row["lifecycle_note"], str) or not row["lifecycle_note"].strip():
            problems.append(f"{name}: empty lifecycle_note (cite the evidence or say why a field is null)")
        if (row["baseline"] is None) != (row["primary_metric"] is None):
            problems.append(f"{name}: baseline and primary_metric must be both set or both null")
        margin = row["adoption_margin"]
        if margin is not None:
            if row["baseline"] is None:
                problems.append(f"{name}: adoption_margin without a baseline")
            if not isinstance(margin, (int, float)) or not math.isfinite(margin):
                problems.append(f"{name}: adoption_margin {margin!r} is not a finite number")
            elif margin > 0 and "tie" not in row["lifecycle_note"]:
                problems.append(f"{name}: adoption_margin > 0 (model worse) without the note calling it a tie")
    return problems


def cr1_se(d: np.ndarray, cluster: np.ndarray) -> float:
    """CR1 SE of ``mean(d)``, ``cluster`` the independent units (``G / (G - 1)`` correction).

    Raises:
        ValueError: On mismatched lengths, no rows, or fewer than two clusters.
    """
    d = np.asarray(d, dtype=np.float64)
    cluster = np.asarray(cluster)
    if d.shape != cluster.shape or d.size == 0:
        raise ValueError("d and cluster must be non-empty and the same length")
    groups, inv = np.unique(cluster, return_inverse=True)
    g = groups.size
    if g < 2:
        raise ValueError("a clustered SE needs at least two clusters")
    sums = np.bincount(inv, weights=d - d.mean())
    return float(math.sqrt(g / (g - 1) * float((sums**2).sum())) / d.size)


def season_record(season: int, diff: np.ndarray, cluster: np.ndarray) -> dict[str, Any]:
    """One scored season: mean ``model - baseline`` loss, its CR1 SE by ``cluster``, the upper 97.5% bound.

    A scorer that reports only a paired ``(diff, se)`` (the provenance scripts do) gives the same
    record as ``{"season", "diff", "se", "upper": diff + Z * se}``.

    Args:
        season: The season scored.
        diff: Per-unit ``model loss - baseline loss`` (e.g. per game-cutoff log loss).
        cluster: The provenance's cluster per unit (e.g. the game id).

    Returns:
        ``{"season", "n", "clusters", "diff", "se", "upper"}``.

    Raises:
        ValueError: On mismatched lengths, an empty season, or fewer than two clusters.
    """
    diff = np.asarray(diff, dtype=np.float64)
    se = cr1_se(diff, cluster)
    mean = float(diff.mean())
    return {
        "season": int(season),
        "n": int(diff.size),
        "clusters": int(np.unique(np.asarray(cluster)).size),
        "diff": mean,
        "se": se,
        "upper": mean + Z * se,
    }


def _check_record(r: Mapping[str, Any]) -> None:
    """A season record is finite, has a positive SE and an upper bound that is ``diff + Z * se``."""
    diff, se, upper = (float(r[k]) for k in ("diff", "se", "upper"))
    if not all(math.isfinite(v) for v in (diff, se, upper)) or se <= 0:
        raise ValueError(
            f"season {r['season']}: diff / se / upper must be finite with se > 0, got {diff}, {se}, {upper}"
        )
    if abs(upper - (diff + Z * se)) > 1e-9:
        raise ValueError(f"season {r['season']}: upper {upper} is not diff + {Z:.2f} * se = {diff + Z * se}")
    if "clusters" in r and int(r["clusters"]) < 2:
        raise ValueError(f"season {r['season']}: fewer than two clusters")


def retire_status(
    records: Sequence[Mapping[str, Any]], adoption_margin: float | None, *, holdout_end: int
) -> dict[str, Any]:
    """UNSCORED / KEEP / WATCH / RETIRE-candidate over the post-holdout :func:`season_record` list (any order).

    ``holdout_end`` is the last holdout season: a record on or before it raises, since the rule
    scores only seasons the fit and its holdout never saw. ``adoption_margin`` None (not stated in
    the evidence) turns the margin trigger off; the result says so in ``margin_trigger``. The
    rule's constants (97.5% bounds, two seasons, the adoption margin) are the spec's, with the
    margin trigger moved to 1.96 SE by the owner on 2026-11-25, and are never loosened to keep a
    model (ClaudeCowork specs/2026-11-25-model-lifecycle-checks-design.md,
    section (b)).

    Raises:
        ValueError: On a duplicate season, a season inside the fit's window, or a malformed record.
    """
    recs = sorted(records, key=lambda r: int(r["season"]))
    seasons = [int(r["season"]) for r in recs]
    if len(set(seasons)) != len(seasons):
        raise ValueError(f"duplicate seasons in {seasons}")
    if any(s <= holdout_end for s in seasons):
        raise ValueError(f"seasons {seasons} include the fit's window (holdout ends {holdout_end})")
    for r in recs:
        _check_record(r)
    fails = [r["upper"] >= 0.0 for r in recs]
    reasons = [
        f"{a['season']} and {b['season']} both fail to beat the baseline (upper bounds {a['upper']:.4g}, {b['upper']:.4g})"
        for a, b, fa, fb in zip(recs, recs[1:], fails, fails[1:], strict=False)
        if fa and fb
    ]
    if adoption_margin is not None:
        bar = abs(float(adoption_margin))
        reasons += [
            f"{r['season']}: worse than the baseline by {r['diff']:.4g}, beyond the adoption margin {bar:.4g} "
            f"at 1.96 SE (lower bound {r['diff'] - Z * r['se']:.4g})"
            for r in recs
            if r["diff"] - Z * float(r["se"]) > bar
        ]
    if not recs:
        status = "UNSCORED"
    elif reasons:
        status = "RETIRE-candidate"
    elif fails[-1]:
        status = "WATCH"
    else:
        status = "KEEP"
    return {
        "status": status,
        "reasons": reasons,
        "scored_seasons": seasons,
        "margin_trigger": "off (no adoption_margin in the ledger)" if adoption_margin is None else "on",
    }


def ledger_row(ledger: Mapping[str, Any], league: str, model: str) -> Mapping[str, Any]:
    """The one ledger row for ``league`` / ``model``."""
    rows = [r for r in ledger["models"] if r["league"] == league and r["model"] == model]
    if len(rows) != 1:
        raise KeyError(f"{len(rows)} ledger rows for {league}/{model}")
    return rows[0]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--ledger", type=Path, default=LEDGER)
    ap.add_argument("--check-ledger", action="store_true")
    ap.add_argument("--league")
    ap.add_argument("--model")
    ap.add_argument("--records", type=Path, help="JSON list of season records")
    args = ap.parse_args(argv)
    ledger = json.loads(args.ledger.read_text(encoding="utf-8"))
    if args.check_ledger:
        problems = check_ledger(ledger)
        print("\n".join(problems) if problems else f"ok: {len(ledger['models'])} rows")
        return 1 if problems else 0
    if not (args.league and args.model):
        ap.error("--league and --model (or --check-ledger)")
    row = ledger_row(ledger, args.league, args.model)
    if row["baseline"] is None:
        print(
            f"{args.league}/{args.model}: no baseline in the ledger, so the rule cannot run ({row['lifecycle_note']})"
        )
        return 1
    if not row.get("holdout_seasons"):
        print(f"{args.league}/{args.model}: no holdout_seasons in the ledger, so no season is known to be past the fit")
        return 1
    records = json.loads(args.records.read_text(encoding="utf-8")) if args.records else []
    out = {
        "model": f"{args.league}/{args.model}",
        "baseline": row["baseline"],
        "primary_metric": row["primary_metric"],
        "holdout_seasons": row.get("holdout_seasons"),
        "adoption_margin": row["adoption_margin"],
        "ledger_status": row["status"],
        **retire_status(records, row["adoption_margin"], holdout_end=int(row["holdout_seasons"][1])),
    }
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
