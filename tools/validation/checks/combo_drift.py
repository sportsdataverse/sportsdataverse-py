"""Play-type x flag combination drift.

The definitional rules assert relationships we already know. This check asks
the complementary question: **has the data produced a play shape we have never
seen before?** A genuinely new (play type x mechanics flags) combination is
either a real new football situation, an upstream vocabulary change, or a
parser regression -- all three are worth a human look, and none of them are
catchable by a rule written in advance.

The snapshot (``combos/<dataset>.json``) enumerates every combination observed
across the full published history, with its occurrence count and season span.
A frame producing a combination outside that set yields one WARN finding
carrying up to ``_SAMPLE_N`` example combinations.

Refresh the snapshot with ``python -m tools.validation.checks.combo_drift
--refresh <dataset>`` after confirming that new combinations are legitimate --
never to silence a finding you have not explained.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from tools.validation.findings import CheckContext, Finding, Severity

_SNAPSHOT_DIR = Path(__file__).parent / "combos"
_SAMPLE_N = 5
_SEP = "|"

#: dataset -> (type column, flag columns forming the signature)
SIGNATURES: dict[str, tuple[str, tuple[str, ...]]] = {
    "cfb_pbp": (
        "type.text",
        (
            "rush",
            "pass",
            "completion",
            "pass_attempt",
            "target",
            "sack",
            "int",
            "kickoff_play",
            "punt_play",
            "fg_attempt",
            "fg_made",
            "penalty_flag",
            "fumble_vec",
            "fumble_lost",
            "safety",
            "td_play",
            "scoring_play",
            "kneel_down",
            "scrimmage_play",
            "punt_blocked",
            "kickoff_onside",
            "kickoff_tb",
            "punt_tb",
            "turnover_vec",
            "change_of_pos_team",
        ),
    ),
}


def _signature_expr(type_col: str, flags: tuple[str, ...]) -> pl.Expr:
    """Build the combination key: play type + a 0/1 digit per flag.

    Nulls fold to 0 so an era that predates a flag does not mint a separate
    combination from the same real play shape.
    """
    parts: list[pl.Expr] = [pl.col(type_col).fill_null("<null>")]
    for flag in flags:
        parts.append(
            pl.when(pl.col(flag) == True).then(pl.lit("1")).otherwise(pl.lit("0"))  # noqa: E712
        )
    return pl.concat_str(parts, separator=_SEP).alias("_combo")


def snapshot_path(dataset: str) -> Path:
    return _SNAPSHOT_DIR / f"{dataset}.json"


def load_snapshot(dataset: str) -> dict | None:
    path = snapshot_path(dataset)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def build_snapshot(dataset: str, frame: pl.DataFrame) -> dict:
    """Compute the full combination inventory for a frame."""
    type_col, flags = SIGNATURES[dataset]
    usable = [f for f in flags if f in frame.columns]
    agg = [pl.len().alias("n")]
    if "season" in frame.columns:
        agg += [pl.col("season").min().alias("first"), pl.col("season").max().alias("last")]
    counts = (
        frame.with_columns(_signature_expr(type_col, tuple(usable)))
        .group_by("_combo")
        .agg(agg)
        .sort("n", descending=True)
    )
    return {
        "dataset": dataset,
        "type_column": type_col,
        "flags": list(usable),
        "n_rows": frame.height,
        "combos": {r.pop("_combo"): r for r in counts.to_dicts()},
    }


def run(dataset: str, frame: pl.DataFrame, ctx: CheckContext) -> list[Finding]:
    """Flag play-type/flag combinations absent from the committed snapshot.

    Skips silently when the dataset has no signature registered, no snapshot
    committed, or the frame lacks the type column. A snapshot whose flag list
    does not match the frame's available flags is itself reported (WARN) rather
    than silently compared against a different alphabet -- comparing two
    different signatures would produce meaningless "new" combinations.

    Args:
        dataset: Registered dataset name.
        frame: The data frame under validation.
        ctx: Check context supplying the domain.

    Returns:
        A list of Finding records; empty when nothing new appears.
    """
    if dataset not in SIGNATURES:
        return []
    snap = load_snapshot(dataset)
    if snap is None:
        return []
    type_col, _ = SIGNATURES[dataset]
    if type_col not in frame.columns:
        return []

    snap_flags = tuple(snap.get("flags", ()))
    missing = [f for f in snap_flags if f not in frame.columns]
    if missing:
        return [
            Finding(
                "combo_drift",
                Severity.WARN,
                ctx.domain,
                dataset,
                f"snapshot signature needs {len(missing)} column(s) absent from the frame "
                f"({', '.join(missing[:5])}) -- combination comparison skipped",
                locator={"missing_columns": missing},
                needs_judgment=True,
            )
        ]

    observed = frame.with_columns(_signature_expr(type_col, snap_flags)).group_by("_combo").agg(pl.len().alias("n"))
    known = set(snap.get("combos", {}))
    new = observed.filter(~pl.col("_combo").is_in(list(known))).sort("n", descending=True)
    if new.height == 0:
        return []

    sample = [
        {"combo": r["_combo"], "n": r["n"], "decoded": _decode(r["_combo"], snap_flags)}
        for r in new.head(_SAMPLE_N).to_dicts()
    ]
    return [
        Finding(
            "combo_drift",
            Severity.WARN,
            ctx.domain,
            dataset,
            f"{new.height} play-type/flag combination(s) not in the committed snapshot "
            f"({int(new.get_column('n').sum())} row(s)) -- new football situation, upstream "
            "vocabulary change, or parser regression",
            locator={"snapshot": str(snapshot_path(dataset)), "known_combos": len(known)},
            metric=float(new.height),
            needs_judgment=True,
            sample=sample,
        )
    ]


def _decode(combo: str, flags: tuple[str, ...]) -> str:
    """Render a signature as 'Play Type: flag_a, flag_b' for human triage."""
    parts = combo.split(_SEP)
    active = [f for f, bit in zip(flags, parts[1:]) if bit == "1"]
    return f"{parts[0]}: " + (", ".join(active) if active else "<no flags>")


def _refresh(dataset: str) -> int:
    from tools.validation.registry import resolve

    frame, _ = resolve(dataset)
    snap = build_snapshot(dataset, frame)
    _SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    path = snapshot_path(dataset)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        json.dump(snap, fh, indent=1, sort_keys=True)
        fh.write("\n")
    print(f"wrote {path}: {len(snap['combos'])} combos from {snap['n_rows']:,} rows")
    return 0


if __name__ == "__main__":
    import sys

    args = sys.argv[1:]
    if len(args) == 2 and args[0] == "--refresh":
        sys.exit(_refresh(args[1]))
    print(__doc__)
    sys.exit(2)
