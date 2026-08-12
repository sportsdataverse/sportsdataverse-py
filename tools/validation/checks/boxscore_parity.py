"""Per-season pbp-to-boxscore parity regression.

The definitional rules check a play against itself; ``rate_anomaly`` checks a
season's event rate against other seasons. Neither can tell you whether the
flags still **add up to the official box score** -- which is the cheapest
end-to-end judge of whether parsing puts the right events on the right team.

This check aggregates the pbp by ``(game_id, pos_team_id)``, joins ESPN's own
team box (committed oracle, extracted from the cfb-raw ``final.json`` store),
and compares each season's exact-match rate against a **committed floor
measured from the current published data**. A drop below the floor means a
producer or parser change moved parity backwards.

Floors are measured, never guessed, and are stored per (stat, season) because
parity is strongly era-dependent -- a single pooled floor would be far too
loose for 2014-2020 (90%+) and impossible for 2004 (0.3% on yardage). See
``dev/boxscore_parity/FINDINGS.md`` for the era analysis.

Aggregation definitions encode conventions PROVEN against the box score:
  * NCAA charges sacks to RUSHING -- attempts and yardage (unlike the NFL)
  * pass attempts EXCLUDE sacks
  * penalties belong to the team that COMMITTED them, not the team with the ball

Refresh the floors with::

    python -m tools.validation.checks.boxscore_parity --refresh cfb_pbp

only after confirming a parity CHANGE is an improvement -- never to silence a
regression you have not explained.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from tools.validation.findings import CheckContext, Finding, Severity

_HERE = Path(__file__).parent
_ORACLE_DIR = _HERE / "oracles"
_FLOOR_DIR = _HERE / "parity_floors"

#: Absolute percentage points a season may fall below its recorded rate before
#: the check fires. Parity is recomputed from the same published data, so the
#: expected drift is 0; 2.0 absorbs a genuinely marginal rounding/mix change
#: without hiding a real regression.
DEFAULT_TOLERANCE_PP = 2.0

c = pl.col


def _b(col: str) -> pl.Expr:
    return (c(col) == True).fill_null(False).cast(pl.Int64)  # noqa: E712


def _y(col: str, when: pl.Expr | None = None) -> pl.Expr:
    expr = c(col).fill_null(0)
    return pl.when(when).then(expr).otherwise(0) if when is not None else expr


#: canonical stat -> (computed column, ESPN column)
STATS: dict[str, tuple[str, str]] = {
    "completions": ("completions", "espn_completions"),
    "interceptions": ("interceptions", "espn_interceptions"),
    "fumbles_lost": ("fumbles_lost", "espn_fumbles_lost"),
    "turnovers": ("turnovers", "espn_turnovers"),
    "rushing_attempts": ("rush_att", "espn_rushing_attempts"),
    "pass_attempts": ("pass_att", "espn_pass_attempts"),
    "rushing_yards": ("rush_yds", "espn_rushing_yards"),
    "net_passing_yards": ("pass_yds", "espn_net_passing_yards"),
    "penalties": ("penalties", "espn_penalties"),
    "penalty_yards": ("penalty_yds", "espn_penalty_yards"),
}

_REQUIRED = [
    "game_id",
    "season",
    "pos_team_id",
    "def_pos_team_id",
    "rush",
    "pass",
    "completion",
    "sack",
    "int",
    "fumble_lost",
    "penalty_flag",
    "penalty_yards_signed",
    "yds_rushed",
    "yds_receiving",
    "yds_sacked",
]


def oracle_path(dataset: str) -> Path:
    return _ORACLE_DIR / f"{dataset}_espn_team_box.parquet"


def floor_path(dataset: str) -> Path:
    return _FLOOR_DIR / f"{dataset}.json"


def aggregate(frame: pl.DataFrame) -> pl.DataFrame:
    """Aggregate pbp to one row per (game_id, team) using the proven definitions."""
    is_rush, is_sack, is_pass = c("rush") == True, c("sack") == True, c("pass") == True  # noqa: E712
    off = (
        frame.filter(c("pos_team_id").is_not_null())
        .group_by("game_id", "pos_team_id")
        .agg(
            season=c("season").first(),
            completions=_b("completion").sum(),
            interceptions=_b("int").sum(),
            fumbles_lost=_b("fumble_lost").sum(),
            turnovers=(_b("int") + _b("fumble_lost")).sum(),
            rush_att=(_b("rush") + _b("sack")).sum(),
            pass_att=(_b("pass") - _b("sack")).sum(),
            rush_yds=(_y("yds_rushed", is_rush) + _y("yds_sacked", is_sack)).sum(),
            pass_yds=_y("yds_receiving", is_pass).sum(),
        )
        .rename({"pos_team_id": "team_key"})
    )
    # Penalties are charged to the COMMITTING team: a positive signed yardage
    # means the offense gained, i.e. the defense was flagged.
    pen = (
        frame.filter((c("penalty_flag") == True).fill_null(False))  # noqa: E712
        .with_columns(
            charged=pl.when(c("penalty_yards_signed") > 0).then(c("def_pos_team_id")).otherwise(c("pos_team_id"))
        )
        .filter(c("charged").is_not_null())
        .group_by("game_id", "charged")
        .agg(penalties=pl.len(), penalty_yds=c("penalty_yards_signed").fill_null(0).abs().sum())
        .rename({"charged": "team_key"})
    )
    return off.join(pen, on=["game_id", "team_key"], how="left")


def measure(frame: pl.DataFrame, oracle: pl.DataFrame) -> pl.DataFrame:
    """Return per (stat, season) exact-match percentage."""
    agg = aggregate(frame).with_columns(team_id=c("team_key").cast(pl.Int64, strict=False)).drop("team_key")
    joined = agg.join(oracle.drop("season"), on=["game_id", "team_id"], how="inner")
    rows = []
    for stat, (mine, theirs) in STATS.items():
        if mine not in joined.columns or theirs not in joined.columns:
            continue
        scoped = joined.filter(c(theirs).is_not_null())
        if scoped.is_empty():
            continue
        delta = c(mine).fill_null(0) - c(theirs)
        rows.append(
            scoped.group_by("season")
            .agg(n=pl.len(), exact_pct=(100.0 * (delta == 0).sum() / pl.len()))
            .with_columns(stat=pl.lit(stat))
        )
    return pl.concat(rows) if rows else pl.DataFrame()


def run(dataset: str, frame: pl.DataFrame, ctx: CheckContext) -> list[Finding]:
    """Report seasons whose box-score parity regressed below its measured floor.

    Skips (returning an empty list) when the committed oracle or floor snapshot
    is absent, when required columns are missing, or when nothing joins -- a
    check that aborts the run is worse than one that reports nothing.

    Args:
        dataset: Registered dataset name.
        frame: The data frame under validation.
        ctx: Check context supplying domain and thresholds
            (``boxscore_parity_tolerance_pp`` overrides the default).

    Returns:
        One WARN Finding per (stat, season) that fell below floor minus
        tolerance; empty when every season holds.

    Example:
        Run over the registered dataset::

            from tools.validation.checks import boxscore_parity
            from tools.validation.registry import resolve

            frame, ctx = resolve("cfb_pbp")
            for f in boxscore_parity.run("cfb_pbp", frame, ctx):
                print(f.message)
    """
    op, fp = oracle_path(dataset), floor_path(dataset)
    if not op.exists() or not fp.exists():
        return []
    if any(col not in frame.columns for col in _REQUIRED):
        return []

    floors = json.loads(fp.read_text(encoding="utf-8")).get("floors", {})
    if not floors:
        return []
    measured = measure(frame, pl.read_parquet(op))
    if measured.is_empty():
        return []

    tol = ctx.thresholds.get("boxscore_parity_tolerance_pp", DEFAULT_TOLERANCE_PP)
    findings: list[Finding] = []
    for row in measured.sort(["stat", "season"]).to_dicts():
        floor = floors.get(row["stat"], {}).get(str(row["season"]))
        if floor is None:
            continue
        if row["exact_pct"] < floor - tol:
            findings.append(
                Finding(
                    "boxscore_parity",
                    Severity.WARN,
                    ctx.domain,
                    dataset,
                    f"{row['stat']!r} box-score parity regressed in {row['season']}: "
                    f"{row['exact_pct']:.1f}% vs floor {floor:.1f}% "
                    f"(tolerance {tol}pp, n={row['n']:,})",
                    locator={"stat": row["stat"], "season": row["season"]},
                    expected=round(floor, 2),
                    actual=round(row["exact_pct"], 2),
                    metric=round(row["exact_pct"] - floor, 2),
                    needs_judgment=True,
                )
            )
    return findings


def _refresh(dataset: str) -> int:
    from tools.validation.registry import resolve

    op = oracle_path(dataset)
    if not op.exists():
        print(f"missing oracle {op} -- generate it with dev/boxscore_parity/extract_espn_box.py")
        return 2
    frame, _ = resolve(dataset)
    measured = measure(frame, pl.read_parquet(op))
    floors: dict[str, dict[str, float]] = {}
    for row in measured.to_dicts():
        floors.setdefault(row["stat"], {})[str(row["season"])] = round(row["exact_pct"], 2)
    _FLOOR_DIR.mkdir(parents=True, exist_ok=True)
    with floor_path(dataset).open("w", encoding="utf-8", newline="\n") as fh:
        json.dump(
            {"dataset": dataset, "tolerance_pp": DEFAULT_TOLERANCE_PP, "floors": floors}, fh, indent=1, sort_keys=True
        )
        fh.write("\n")
    print(f"wrote {floor_path(dataset)}: {sum(len(v) for v in floors.values())} (stat, season) floors")
    return 0


if __name__ == "__main__":
    import sys

    args = sys.argv[1:]
    if len(args) == 2 and args[0] == "--refresh":
        sys.exit(_refresh(args[1]))
    print(__doc__)
    sys.exit(2)
