"""Per-season event-rate collapse.

Row-level rules check that a play is *internally* consistent. They cannot see
that an entire event type went **missing for a season** -- every surviving row
stays perfectly self-consistent, so nothing fires.

That is exactly how the 2013 sack outage hid: the published 2013 season carries
32 ``type.text == "Sack"`` plays against 3,047 in 2012 and 3,213 in 2014, and
``yds_sacked`` is 100% null. The parser is not at fault -- the 2013 feed simply
does not contain sacks, and they were not folded into rushes either
(rush-for-loss holds at 2.84/game in 2013 vs 2.86 in 2012 and 3.06 in 2014).
No row-level rule can catch an absence.

This check compares each season's per-game event rate against the **median
season** for that flag and reports the collapses. Measured over 2004-2025
(22 seasons x 12 flags = 264 season-flag cells): 2013/sack is the sole cell
below 0.5x median, at **0.015x**; the next-worst cell anywhere is 0.560x. The
floor is therefore well separated from normal era variation rather than tuned.

Findings are WARN/``needs_judgment``: a rate collapse is sometimes a genuine
rule change (kickoff rates move when touchback rules change), so it is a triage
signal, not an automatic defect.
"""

from __future__ import annotations

import polars as pl

from tools.validation.findings import CheckContext, Finding, Severity

#: dataset -> (season column, per-season denominator key, flag columns)
RATE_SPECS: dict[str, tuple[str, str, tuple[str, ...]]] = {
    "cfb_pbp": (
        "season",
        "game_id",
        (
            "rush",
            "pass",
            "completion",
            "sack",
            "int",
            "fumble_vec",
            "penalty_flag",
            "td_play",
            "kickoff_play",
            "punt_play",
            "fg_attempt",
            "safety",
        ),
    ),
}

#: Ratio-to-median below which a season's rate counts as a collapse. Measured:
#: 2013/sack = 0.015, next-worst cell = 0.560, so 0.5 separates the real outage
#: from every normal era swing without landing between two observed values.
DEFAULT_FLOOR = 0.5
_MIN_SEASONS = 3


def run(dataset: str, frame: pl.DataFrame, ctx: CheckContext) -> list[Finding]:
    """Report seasons whose per-game event rate collapsed versus the median season.

    Skips when the dataset has no spec, the season/denominator columns are
    absent, or fewer than ``_MIN_SEASONS`` seasons are present (a median over
    one or two seasons is not a baseline). Flags whose median rate is zero are
    skipped -- there is no meaningful ratio to take.

    Args:
        dataset: Registered dataset name.
        frame: The data frame under validation.
        ctx: Check context supplying the domain and thresholds.

    Returns:
        One Finding per (flag, season) collapse; empty when every season holds.
        Never raises: an unregistered dataset, missing columns, too few seasons,
        or a zero-median flag all short-circuit to an empty list, because a
        check that aborts a run is worse than one that reports nothing.

    Example:
        Run the check over a registered dataset::

            from tools.validation.checks import rate_anomaly
            from tools.validation.registry import resolve

            frame, ctx = resolve("cfb_pbp")
            for finding in rate_anomaly.run("cfb_pbp", frame, ctx):
                print(finding.severity.value, finding.message)

        Tighten or loosen the collapse floor per run::

            ctx = dataclasses.replace(ctx, thresholds={"season_rate_floor": 0.25})
    """
    spec = RATE_SPECS.get(dataset)
    if spec is None:
        return []
    season_col, denom_col, flags = spec
    if season_col not in frame.columns or denom_col not in frame.columns:
        return []
    usable = [f for f in flags if f in frame.columns]
    if not usable:
        return []

    per_season = (
        frame.group_by(season_col)
        .agg(
            _denom=pl.col(denom_col).n_unique(),
            **{f: (pl.col(f) == True).fill_null(False).sum() for f in usable},  # noqa: E712
        )
        .sort(season_col)
    )
    if per_season.height < _MIN_SEASONS:
        return []

    rates = per_season.select(
        season_col,
        pl.col("_denom"),
        *[(pl.col(f) / pl.col("_denom")).alias(f) for f in usable],
    )
    floor = ctx.thresholds.get("season_rate_floor", DEFAULT_FLOOR)

    findings: list[Finding] = []
    for flag in usable:
        median = rates.get_column(flag).median()
        if not median:  # zero or null median -> no meaningful ratio
            continue
        collapsed = (
            rates.select(season_col, "_denom", flag, _ratio=pl.col(flag) / median)
            .filter(pl.col("_ratio") < floor)
            .sort("_ratio")
        )
        for row in collapsed.to_dicts():
            findings.append(
                Finding(
                    "rate_anomaly",
                    Severity.WARN,
                    ctx.domain,
                    dataset,
                    f"{flag!r} rate collapsed in {row[season_col]}: "
                    f"{row[flag]:.3f}/game vs median {median:.3f}/game "
                    f"({row['_ratio']:.3f}x, floor {floor})",
                    locator={"column": flag, "season": row[season_col]},
                    expected=round(median, 4),
                    actual=round(row[flag], 4),
                    metric=round(row["_ratio"], 4),
                    needs_judgment=True,
                    sample=[{"season": row[season_col], "games": row["_denom"], "rate": round(row[flag], 4)}],
                )
            )
    return findings
