"""Player-level pbp-to-boxscore parity regression.

``boxscore_parity`` proves the team totals add up; it cannot see a carry
credited to the wrong runner or a returner whose id never resolved. This check
sums the pbp per ``(game_id, athlete_id, category)`` and joins ESPN's own
player box (the ``espn_cfb_player_box`` release, read from
``SDV_VALIDATION_DATA_ROOT``) on the athlete id.

Two rates per (category, stat, era), each against a committed floor:
  * ``exact`` -- of the lines found on both sides, the share that agree
  * ``coverage`` -- of ESPN's lines, the share the pbp produced at all. Ids
    that vanish (a garbled regex name, a lost roster join) drop this one.

Conventions match the team check: a sack is a carry for the sacked passer and
its yards are rushing yards; pass attempts exclude sacks. ESPN rows that are
not lines of the stat are dropped (tacklers with no sack, kickers with only
extra points). Before 2012 ESPN's player box lists about half the players
(O2), so coverage there is low by construction; the floors record it.

Refresh the floors with::

    python -m tools.validation.checks.boxscore_parity_player --refresh cfb_pbp

only after confirming a parity CHANGE is an improvement.
"""

from __future__ import annotations

import glob
import json
import os
from pathlib import Path

import polars as pl

from sportsdataverse.validation.findings import CheckContext, Finding, Severity

_FLOOR_DIR = Path(__file__).parent / "parity_floors"

#: Absolute percentage points an era may fall below its floor before firing.
DEFAULT_TOLERANCE_PP = 2.0

#: Era buckets: pre-2007 text, the 2007-13 "Last, First" feed, the 2014-19
#: vendor feed, and 2020 on (truncated / skipped-down feeds).
ERAS: tuple[tuple[str, int, int], ...] = (
    ("2004-06", 2004, 2006),
    ("2007-13", 2007, 2013),
    ("2014-19", 2014, 2019),
    ("2020+", 2020, 9999),  # open-ended: a new season must not fall out of the check
)

c = pl.col

_ID_COLUMNS = (
    "passer_player_id",
    "rusher_player_id",
    "receiver_player_id",
    "fg_kicker_player_id",
    "punter_player_id",
    "kickoff_return_player_id",
    "punt_return_player_id",
    "interception_player_id",
    "sack_player_id",
)
_FLAGS = (
    "pass_attempt",
    "completion",
    "sack",
    "rush",
    "int",
    "pass_td",
    "rush_td",
    "fg_attempt",
    "fg_made",
    "punt",
    "kickoff_tb",
    "kickoff_fair_catch",
    "punt_tb",
    "punt_fair_catch",
)
_REQUIRED = (
    "game_id",
    "season",
    *_ID_COLUMNS,
    *_FLAGS,
    "yds_receiving",
    "yds_rushed",
    "yds_sacked",
    "yds_punted",
    "yds_kickoff_return",
    "yds_punt_return",
    "yds_int_return",
)


class JoinKeyError(RuntimeError):
    """``game_id`` / ``athlete_id`` differ in dtype between pbp and ESPN, so the join would match nothing."""


def _b(col: str) -> pl.Expr:
    return (c(col) == True).fill_null(False)  # noqa: E712


def _n(col: str) -> pl.Expr:
    return c(col).fill_null(0)


def _int(col: str) -> pl.Expr:
    return c(col).str.replace_all(",", "").cast(pl.Float64, strict=False)


def _split(col: str, i: int, sep: str) -> pl.Expr:
    return c(col).str.split(sep).list.get(i, null_on_oob=True).cast(pl.Float64, strict=False)


def espn_box_glob() -> str:
    """Glob of ESPN's player-box parquet under ``SDV_VALIDATION_DATA_ROOT``.

    Returns:
        The expanded glob; it may match nothing.

    Example:
        Count the seasons on disk::

            import glob
            from tools.validation.checks import boxscore_parity_player

            len(glob.glob(boxscore_parity_player.espn_box_glob()))
    """
    return os.path.expandvars("${SDV_VALIDATION_DATA_ROOT}/cfb/player_box/parquet/player_box_*.parquet")


def floor_path(dataset: str) -> Path:
    """Path to the committed per-(category, stat, era) player floors for ``dataset``.

    Args:
        dataset: Registered dataset name.

    Returns:
        The floors JSON path; may not exist.

    Example:
        Inspect the shipped floors::

            import json
            from tools.validation.checks import boxscore_parity_player

            json.loads(boxscore_parity_player.floor_path("cfb_pbp").read_text())["floors"].keys()
    """
    return _FLOOR_DIR / f"{dataset}_player.json"


def pbp_player_lines(frame: pl.DataFrame) -> pl.DataFrame:
    """Sum the pbp to one row per ``(game_id, athlete_id, category)``, stats named like ESPN's keys.

    Args:
        frame: Play-by-play frame carrying the columns in ``_REQUIRED``.

    Returns:
        Long frame of player lines; stat columns are null where a category
        does not carry them.

    Example:
        Lines for one season::

            from tools.validation.checks import boxscore_parity_player
            from tools.validation.registry import resolve

            frame, _ = resolve("cfb_pbp")
            boxscore_parity_player.pbp_player_lines(frame).head()
    """

    def by(id_col: str, cat: str, mask: pl.Expr, **aggs: pl.Expr) -> pl.DataFrame:
        return (
            frame.filter(mask & c(id_col).is_not_null())
            .group_by("game_id", id_col)
            .agg(**aggs)
            .rename({id_col: "athlete_id"})
            .with_columns(category=pl.lit(cat))
        )

    comp, sack = _b("completion"), _b("sack")
    passing = by(
        "passer_player_id",
        "passing",
        _b("pass_attempt") | sack,
        completions=comp.sum(),
        passingAttempts=(_b("pass_attempt") & ~sack).sum(),
        passingYards=pl.when(comp).then(_n("yds_receiving")).otherwise(0).sum(),
        passingTouchdowns=_b("pass_td").sum(),
        interceptions=_b("int").sum(),
    )
    rushing = (
        pl.concat(
            [
                by(
                    "rusher_player_id",
                    "rushing",
                    _b("rush"),
                    rushingAttempts=pl.len(),
                    rushingYards=_n("yds_rushed").sum(),
                    rushingTouchdowns=_b("rush_td").sum(),
                ),
                by(
                    "passer_player_id",
                    "rushing",
                    sack,
                    rushingAttempts=pl.len(),
                    rushingYards=_n("yds_sacked").sum(),
                    rushingTouchdowns=pl.lit(0, pl.UInt32),
                ),
            ],
            how="diagonal_relaxed",
        )
        .group_by("game_id", "athlete_id", "category")
        .agg(c("rushingAttempts").sum(), c("rushingYards").sum(), c("rushingTouchdowns").sum())
    )
    receiving = by(
        "receiver_player_id",
        "receiving",
        comp,
        receptions=pl.len(),
        receivingYards=_n("yds_receiving").sum(),
        receivingTouchdowns=_b("pass_td").sum(),
    )
    kicking = by(
        "fg_kicker_player_id",
        "kicking",
        _b("fg_attempt"),
        fieldGoalsMade=_b("fg_made").sum(),
        fieldGoalAttempts=pl.len(),
    )
    punting = by("punter_player_id", "punting", _b("punt"), punts=pl.len(), puntYards=_n("yds_punted").sum())
    kick_ret = by(
        "kickoff_return_player_id",
        "kickReturns",
        c("yds_kickoff_return").is_not_null() & ~_b("kickoff_tb") & ~_b("kickoff_fair_catch"),
        kickReturns=pl.len(),
        kickReturnYards=_n("yds_kickoff_return").sum(),
    )
    punt_ret = by(
        "punt_return_player_id",
        "puntReturns",
        c("yds_punt_return").is_not_null() & ~_b("punt_tb") & ~_b("punt_fair_catch"),
        puntReturns=pl.len(),
        puntReturnYards=_n("yds_punt_return").sum(),
    )
    ints = by(
        "interception_player_id",
        "interceptions",
        _b("int"),
        interceptions=pl.len(),
        interceptionYards=_n("yds_int_return").sum(),
    )
    sacks = by("sack_player_id", "defensive", sack, sacks=pl.len().cast(pl.Float64))
    return pl.concat(
        [passing, rushing, receiving, kicking, punting, kick_ret, punt_ret, ints, sacks], how="diagonal_relaxed"
    )


#: category -> (stat, expression over an ESPN player-box row). 2004-05 files
#: carry passing as stat_1 ("c/att") and stat_2 (yards).
ESPN_STATS: dict[str, list[tuple[str, pl.Expr]]] = {
    "passing": [
        ("completions", pl.coalesce(_split("completions/passingAttempts", 0, "/"), _split("stat_1", 0, "/"))),
        ("passingAttempts", pl.coalesce(_split("completions/passingAttempts", 1, "/"), _split("stat_1", 1, "/"))),
        ("passingYards", pl.coalesce(_int("passingYards"), _int("stat_2"))),
        ("passingTouchdowns", _int("passingTouchdowns")),
        ("interceptions", _int("interceptions")),
    ],
    "rushing": [
        ("rushingAttempts", _int("rushingAttempts")),
        ("rushingYards", _int("rushingYards")),
        ("rushingTouchdowns", _int("rushingTouchdowns")),
    ],
    "receiving": [
        ("receptions", _int("receptions")),
        ("receivingYards", _int("receivingYards")),
        ("receivingTouchdowns", _int("receivingTouchdowns")),
    ],
    "kicking": [
        ("fieldGoalsMade", _split("fieldGoalsMade/fieldGoalAttempts", 0, "/")),
        ("fieldGoalAttempts", _split("fieldGoalsMade/fieldGoalAttempts", 1, "/")),
    ],
    "punting": [("punts", _int("punts")), ("puntYards", _int("puntYards"))],
    "kickReturns": [("kickReturns", _int("kickReturns")), ("kickReturnYards", _int("kickReturnYards"))],
    "puntReturns": [("puntReturns", _int("puntReturns")), ("puntReturnYards", _int("puntReturnYards"))],
    "interceptions": [("interceptions", _int("interceptions")), ("interceptionYards", _int("interceptionYards"))],
    "defensive": [("sacks", _int("sacks"))],
}

_ESPN_SOURCE_COLS = sorted(
    {
        "completions/passingAttempts",
        "passingYards",
        "passingTouchdowns",
        "interceptions",
        "rushingAttempts",
        "rushingYards",
        "rushingTouchdowns",
        "receptions",
        "receivingYards",
        "receivingTouchdowns",
        "fieldGoalsMade/fieldGoalAttempts",
        "punts",
        "puntYards",
        "kickReturns",
        "kickReturnYards",
        "puntReturns",
        "puntReturnYards",
        "interceptionYards",
        "sacks",
        "stat_1",
        "stat_2",
    }
)

#: ESPN rows that are not lines of the stat: tacklers with no sack, kickers with only extra points.
_ESPN_ROW_FILTER: dict[str, pl.Expr] = {
    "defensive": c("espn_sacks").fill_null(0) > 0,
    "kicking": c("espn_fieldGoalAttempts").fill_null(0) > 0,
}


def espn_player_lines(source_glob: str) -> pl.DataFrame:
    """Read ESPN's player box into one row per ``(game_id, athlete_id, category)``.

    Args:
        source_glob: Glob of ``player_box_*.parquet`` files.

    Returns:
        Long frame with ``espn_<stat>`` Float64 columns; empty when the glob
        matches nothing.

    Example:
        Read the box under the data root::

            from tools.validation.checks import boxscore_parity_player as bpp

            bpp.espn_player_lines(bpp.espn_box_glob()).head()
    """
    frames = []
    for path in sorted(glob.glob(source_glob)):
        pb = pl.read_parquet(path)
        missing = [col for col in _ESPN_SOURCE_COLS if col not in pb.columns]
        pb = pb.with_columns(*[pl.lit(None, dtype=pl.String).alias(col) for col in missing])
        for cat, stats in ESPN_STATS.items():
            part = pb.filter((c("category") == cat) & c("athlete_id").is_not_null())
            if part.height:
                frames.append(
                    part.select(
                        c("game_id").cast(pl.Int64),
                        c("athlete_id").cast(pl.Int64),
                        "athlete_name",
                        category=pl.lit(cat),
                        **{f"espn_{s}": e for s, e in stats},
                    )
                )
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


#: Schema of :func:`measure`'s result, so an empty measurement still has its columns.
_SUMMARY_SCHEMA = {
    "category": pl.String,
    "stat": pl.String,
    "era": pl.String,
    "exact": pl.UInt32,
    "differs": pl.UInt32,
    "espn_only": pl.UInt32,
    "pbp_only": pl.UInt32,
    "exact_pct": pl.Float64,
    "coverage_pct": pl.Float64,
}


def _era() -> pl.Expr:
    expr = pl.lit(None, pl.String)
    for name, lo, hi in reversed(ERAS):
        expr = pl.when(c("season").is_between(lo, hi)).then(pl.lit(name)).otherwise(expr)
    return expr


def measure(frame: pl.DataFrame, espn: pl.DataFrame) -> pl.DataFrame:
    """Compare pbp player lines with ESPN's, per (category, stat, era).

    Args:
        frame: Play-by-play frame carrying the columns in ``_REQUIRED``.
        espn: ESPN player lines from :func:`espn_player_lines`.

    Returns:
        One row per (category, stat, era) with line counts (``exact``,
        ``differs``, ``espn_only``, ``pbp_only``), ``exact_pct`` (of lines on
        both sides) and ``coverage_pct`` (of ESPN's lines). Empty (with those
        columns) when ESPN's side is empty.

    Raises:
        JoinKeyError: If ``game_id`` or ``athlete_id`` differ in dtype.

    Example:
        Measure the registered dataset::

            from tools.validation.checks import boxscore_parity_player as bpp
            from tools.validation.registry import resolve

            frame, _ = resolve("cfb_pbp")
            bpp.measure(frame, bpp.espn_player_lines(bpp.espn_box_glob()))
    """
    pbp = pbp_player_lines(frame)
    if espn.is_empty():
        return pl.DataFrame(schema=_SUMMARY_SCHEMA)
    for key in ("game_id", "athlete_id"):
        if pbp.schema[key] != espn.schema[key]:
            raise JoinKeyError(f"{key}: pbp {pbp.schema[key]} vs ESPN {espn.schema[key]}")
    games = frame.select("game_id", "season").unique(subset="game_id")
    espn = espn.join(games, on="game_id", how="semi")  # judge only games the pbp carries
    rows = []
    for cat, stats in ESPN_STATS.items():
        if any(f"espn_{s}" not in espn.columns for s, _ in stats):
            continue  # no ESPN line of this category in the box
        e = espn.filter((c("category") == cat) & _ESPN_ROW_FILTER.get(cat, pl.lit(True)))
        p = pbp.filter(c("category") == cat)
        j = e.join(p, on=["game_id", "athlete_id", "category"], how="full", coalesce=True)
        for s, _ in stats:
            if s not in j.columns:
                continue
            rows.append(
                j.select(
                    "game_id",
                    "category",
                    stat=pl.lit(s),
                    espn=c(f"espn_{s}").cast(pl.Float64),
                    pbp=c(s).cast(pl.Float64),
                )
            )
    if not rows:
        return pl.DataFrame(schema=_SUMMARY_SCHEMA)
    long = pl.concat(rows).join(games, on="game_id", how="inner").with_columns(era=_era())
    return (
        long.filter(c("era").is_not_null())
        .group_by("category", "stat", "era")
        .agg(
            exact=(c("espn").is_not_null() & c("pbp").is_not_null() & ((c("espn") - c("pbp")).abs() < 0.01)).sum(),
            differs=(c("espn").is_not_null() & c("pbp").is_not_null() & ((c("espn") - c("pbp")).abs() >= 0.01)).sum(),
            espn_only=(c("espn").is_not_null() & c("pbp").is_null()).sum(),
            pbp_only=(c("espn").is_null() & c("pbp").is_not_null()).sum(),
        )
        .with_columns(
            exact_pct=pl.when(c("exact") + c("differs") > 0).then(100.0 * c("exact") / (c("exact") + c("differs"))),
            coverage_pct=pl.when(c("exact") + c("differs") + c("espn_only") > 0).then(
                100.0 * (c("exact") + c("differs")) / (c("exact") + c("differs") + c("espn_only"))
            ),
        )
        .sort("category", "stat", "era")
        .cast(_SUMMARY_SCHEMA)
    )


#: metric -> (floors key in the snapshot, measured column)
METRICS: dict[str, tuple[str, str]] = {
    "exact": ("floors", "exact_pct"),
    "coverage": ("floors_coverage", "coverage_pct"),
}


def run(dataset: str, frame: pl.DataFrame, ctx: CheckContext) -> list[Finding]:
    """Report (category, stat, era) player parity that fell below its floor.

    Skips (returning an empty list) when the floors or ESPN's player box are
    absent, or required columns are missing.

    Args:
        dataset: Registered dataset name.
        frame: The data frame under validation.
        ctx: Check context; ``boxscore_parity_tolerance_pp`` overrides the tolerance.

    Returns:
        One WARN Finding per (category, stat, era, metric) below floor minus tolerance.

    Raises:
        JoinKeyError: Propagated from :func:`measure`.

    Example:
        Run over the registered dataset::

            from tools.validation.checks import boxscore_parity_player
            from tools.validation.registry import resolve

            frame, ctx = resolve("cfb_pbp")
            boxscore_parity_player.run("cfb_pbp", frame, ctx)
    """
    fp = floor_path(dataset)
    if not fp.exists() or any(col not in frame.columns for col in _REQUIRED):
        return []
    snapshot = json.loads(fp.read_text(encoding="utf-8"))
    espn = espn_player_lines(espn_box_glob())
    if espn.is_empty():
        return []
    measured = measure(frame, espn)
    if measured.is_empty():
        return []
    tol = ctx.thresholds.get("boxscore_parity_tolerance_pp", DEFAULT_TOLERANCE_PP)
    findings: list[Finding] = []
    for row in measured.to_dicts():
        key = f"{row['category']}.{row['stat']}"
        for metric, (floors_key, col) in METRICS.items():
            floor = snapshot.get(floors_key, {}).get(key, {}).get(row["era"])
            if floor is None or row[col] is None or row[col] >= floor - tol:
                continue
            findings.append(
                Finding(
                    "boxscore_parity_player",
                    Severity.WARN,
                    ctx.domain,
                    dataset,
                    f"{key} player parity ({metric}) regressed in {row['era']}: "
                    f"{row[col]:.1f}% vs floor {floor:.1f}% (tolerance {tol}pp)",
                    locator={"category": row["category"], "stat": row["stat"], "era": row["era"], "metric": metric},
                    expected=round(floor, 2),
                    actual=round(row[col], 2),
                    metric=round(row[col] - floor, 2),
                    needs_judgment=True,
                )
            )
    return findings


def _refresh(dataset: str) -> int:
    from tools.validation.registry import resolve

    frame, _ = resolve(dataset)
    measured = measure(frame, espn_player_lines(espn_box_glob()))
    if measured.is_empty():
        print(f"nothing measured -- is ESPN's player box under {espn_box_glob()}?")
        return 2
    payload: dict[str, object] = {"dataset": dataset, "tolerance_pp": DEFAULT_TOLERANCE_PP}
    for floors_key, col in METRICS.values():
        floors: dict[str, dict[str, float]] = {}
        for row in measured.to_dicts():
            if row[col] is not None:
                floors.setdefault(f"{row['category']}.{row['stat']}", {})[row["era"]] = round(row[col], 2)
        payload[floors_key] = floors
    _FLOOR_DIR.mkdir(parents=True, exist_ok=True)
    with floor_path(dataset).open("w", encoding="utf-8", newline="\n") as fh:
        json.dump(payload, fh, indent=1, sort_keys=True)
        fh.write("\n")
    print(f"wrote {floor_path(dataset)}: {measured.height} (category, stat, era) rows")
    return 0


if __name__ == "__main__":
    import sys

    args = sys.argv[1:]
    if len(args) == 2 and args[0] == "--refresh":
        sys.exit(_refresh(args[1]))
    print(__doc__)
    sys.exit(2)
