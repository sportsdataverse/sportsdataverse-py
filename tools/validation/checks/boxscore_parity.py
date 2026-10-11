"""Per-season pbp-to-boxscore parity regression.

The definitional rules check a play against itself; ``rate_anomaly`` checks a
season's event rate against other seasons. Neither can tell you whether the
flags still **add up to the official box score** -- which is the cheapest
end-to-end judge of whether parsing puts the right events on the right team.

This check aggregates the pbp by ``(game_id, team)``, joins ESPN's own team box
(committed oracle, extracted from the ``espn_cfb_team_box`` release with
``--extract-oracle``), and compares each season against a **committed floor
measured from the current published data**. A drop below the floor means a
producer or parser change moved parity backwards.

Three rates are graded per (stat, season), each against its own floor:
  * ``exact`` -- share of team-games where pbp equals the box
  * ``complete_feed`` -- the same, restricted to team-games whose feed is
    complete (pbp rushing and passing attempts both equal the box). ESPN's
    2021+ feed truncates games and skips downs; this subset is where a parser
    regression shows through the source noise.
  * ``within_5`` -- yardage stats only: share within 5 yards. Box and text
    disagree by 1-2 yards on sacks often enough that exact team yardage
    plateaus near 65% even in the best era.

Floors are measured, never guessed, and are stored per (stat, season) because
parity is strongly era-dependent -- a single pooled floor would be far too
loose for 2014-2020 (90%+) and impossible for 2004 (0.3% on yardage). See
``dev/boxscore_parity/FINDINGS.md`` for the era analysis.

Aggregation definitions encode conventions PROVEN against the box score:
  * NCAA charges sacks to RUSHING -- attempts and yardage (unlike the NFL)
  * pass attempts EXCLUDE sacks
  * penalties count ACCEPTED fouls only (not declined or offsetting), belong to
    the team that COMMITTED them (the parser's ``penalty_team_id``, else the
    yardage sign), and intentional grounding counts 0 yards
  * fumbles lost: ``pos_fumble_lost`` to the offense, ``def_fumble_lost`` to the
    defense (return fumbles), plays wiped out by a penalty dropped
  * net passing is judged against ``totalYards - rushingYards``: ESPN's
    pre-2012 ``netPassingYards`` adds sack yards back in, disagreeing with its
    own passer lines

Source gaps are exempt (``EXEMPT``): 2004-05 box penalties are "0-0", and
ESPN's 2013 play feed has almost no sacks, so 2013 rushing cannot match.

Regenerate the oracle, then refresh the floors::

    python -m tools.validation.checks.boxscore_parity --extract-oracle cfb_pbp
    python -m tools.validation.checks.boxscore_parity --refresh cfb_pbp

only after confirming a parity CHANGE is an improvement -- never to silence a
regression you have not explained.
"""

from __future__ import annotations

import glob
import json
import os
from pathlib import Path

import polars as pl

from sportsdataverse.validation.findings import CheckContext, Finding, Severity

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
    "net_passing_yards": ("pass_yds", "espn_passing_yards_derived"),
    "total_yards": ("total_yds", "espn_total_yards"),
    "first_downs": ("first_downs", "espn_first_downs"),
    "penalties": ("penalties", "espn_penalties"),
    "penalty_yards": ("penalty_yds", "espn_penalty_yards"),
}

#: Yardage stats additionally graded within this many yards.
YARD_TOLERANCE = 5
_YARDAGE_STATS = frozenset({"rushing_yards", "net_passing_yards", "total_yards", "penalty_yards"})

#: (stat -> seasons) the box score cannot judge: ESPN's 2004-05 box penalties
#: are "0-0" in 97% of team-games though the feed carries the flags, and ESPN's
#: 2013 play feed has 0.03 sacks per game (3.8 in 2012), so rushing (which owns
#: sacks) and total yards cannot match. Neither is fixable in the parser.
EXEMPT: dict[str, frozenset[int]] = {
    "penalties": frozenset({2004, 2005}),
    "penalty_yards": frozenset({2004, 2005}),
    "rushing_attempts": frozenset({2013}),
    "rushing_yards": frozenset({2013}),
    "total_yards": frozenset({2013}),
}

#: metric name -> (floors key in the snapshot, measured column)
METRICS: dict[str, tuple[str, str]] = {
    "exact": ("floors", "exact_pct"),
    "complete_feed": ("floors_complete", "complete_pct"),
    "within_5": ("floors_within_5", "within_5_pct"),
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
    "pos_fumble_lost",
    "def_fumble_lost",
    "penalty_flag",
    "penalty_declined",
    "penalty_offset",
    "penalty_no_play",
    "penalty_team_id",
    "penalty_yards_signed",
    "first_down_yards",
    "first_down_penalty",
    "text",
    "yds_rushed",
    "yds_receiving",
    "yds_sacked",
]

#: ESPN team-box string column -> oracle column(s). A pair splits "made-att" /
#: "comp/att" strings; a single name is a plain (comma-grouped) integer.
_TEAM_BOX_FIELDS: dict[str, tuple[str, ...]] = {
    "firstDowns": ("espn_first_downs",),
    "totalYards": ("espn_total_yards",),
    "netPassingYards": ("espn_net_passing_yards",),
    "completionAttempts": ("espn_completions", "espn_pass_attempts"),
    "rushingYards": ("espn_rushing_yards",),
    "rushingAttempts": ("espn_rushing_attempts",),
    "totalPenaltiesYards": ("espn_penalties", "espn_penalty_yards"),
    "turnovers": ("espn_turnovers",),
    "fumblesLost": ("espn_fumbles_lost",),
    "interceptions": ("espn_interceptions",),
    "thirdDownEff": ("espn_third_down_conv", "espn_third_down_att"),
    "fourthDownEff": ("espn_fourth_down_conv", "espn_fourth_down_att"),
}


def oracle_path(dataset: str) -> Path:
    """Path to the committed ESPN team-box oracle for ``dataset``.

    Args:
        dataset: Registered dataset name.

    Returns:
        The oracle parquet path; may not exist.

    Example:
        Read the shipped oracle::

            import polars as pl
            from tools.validation.checks import boxscore_parity

            pl.read_parquet(boxscore_parity.oracle_path("cfb_pbp"))
    """
    return _ORACLE_DIR / f"{dataset}_espn_team_box.parquet"


def floor_path(dataset: str) -> Path:
    """Path to the committed per-(stat, season) parity floors for ``dataset``.

    Args:
        dataset: Registered dataset name.

    Returns:
        The floors JSON path; may not exist.

    Example:
        Inspect the shipped floors::

            import json
            from tools.validation.checks import boxscore_parity

            json.loads(boxscore_parity.floor_path("cfb_pbp").read_text())["floors"].keys()
    """
    return _FLOOR_DIR / f"{dataset}.json"


def team_box_glob() -> str:
    """Glob of ESPN's team-box parquet under ``SDV_VALIDATION_DATA_ROOT``.

    The files are the ``espn_cfb_team_box`` release (``cfbfastR-cfb-data``
    commits the same frames), a reshape of ESPN's summary JSON that never
    touches the pbp -- which is what makes it an independent judge.

    Returns:
        The expanded glob; it may match nothing.

    Example:
        Count the seasons on disk::

            import glob
            from tools.validation.checks import boxscore_parity

            len(glob.glob(boxscore_parity.team_box_glob()))
    """
    return os.path.expandvars("${SDV_VALIDATION_DATA_ROOT}/cfb/team_box/parquet/team_box_*.parquet")


def extract_oracle(source_glob: str) -> pl.DataFrame:
    """Build the team-box oracle from ESPN team-box parquet files.

    ESPN ships every stat as a string ("1,074", "13/17", "5-55"); this parses
    them to Int64, one row per ``(game_id, team_id)``. Rows with no stat at all
    (games ESPN never boxed) are dropped -- they carry nothing to judge.

    Args:
        source_glob: Glob of ``team_box_*.parquet`` files.

    Returns:
        The oracle frame: ``game_id``, ``team_id`` (Int64 keys), ``home_away``,
        ``season`` and one ``espn_*`` Int64 column per parsed stat.

    Raises:
        FileNotFoundError: If ``source_glob`` matches no files.

    Example:
        Regenerate the committed oracle::

            from tools.validation.checks import boxscore_parity

            oracle = boxscore_parity.extract_oracle(boxscore_parity.team_box_glob())
            oracle.write_parquet(boxscore_parity.oracle_path("cfb_pbp"))
    """
    paths = sorted(glob.glob(source_glob))
    if not paths:
        raise FileNotFoundError(f"no team-box parquet matches {source_glob!r}")
    frames = []
    for path in paths:
        tb = pl.read_parquet(path)
        exprs = []
        for src, outs in _TEAM_BOX_FIELDS.items():
            col = c(src).cast(pl.String) if src in tb.columns else pl.lit(None, pl.String)
            if len(outs) == 1:
                exprs.append(col.str.replace_all(",", "").cast(pl.Int64, strict=False).alias(outs[0]))
            else:
                # "13/17", "5-55", "14--3" (negative penalty yards)
                parts = col.str.extract_groups(r"^\s*(\d+)\s*[/-]\s*(-?\d+)\s*$")
                exprs += [parts.struct.field(str(i + 1)).cast(pl.Int64).alias(o) for i, o in enumerate(outs)]
        frames.append(
            tb.select(
                c("game_id").cast(pl.Int64),
                c("team_id").cast(pl.Int64),
                c("home_away").cast(pl.String),
                c("season").cast(pl.Int64),
                *exprs,
            )
        )
    stat_cols = [o for outs in _TEAM_BOX_FIELDS.values() for o in outs]
    return (
        pl.concat(frames, how="vertical")
        .filter(pl.any_horizontal(c(stat_cols).is_not_null()))
        .unique(subset=["game_id", "team_id"], keep="first", maintain_order=True)
        .sort("season", "game_id", "team_id")
    )


def aggregate(frame: pl.DataFrame) -> pl.DataFrame:
    """Aggregate pbp to one row per ``(game_id, team_key)`` using proven definitions.

    Encodes the conventions measured against the box score: NCAA charges sacks
    to rushing, pass attempts exclude sacks, penalties count accepted fouls
    charged to the committing team (the parser's ``penalty_team_id``, else the
    yardage sign: positive means the offense gained, so the defense was
    flagged), and fumbles lost are split between offense and defense with
    nullified plays dropped.

    Args:
        frame: Play-by-play frame carrying the columns in ``_REQUIRED``.

    Returns:
        One row per ``(game_id, team_key)`` with every tracked stat.

    Example:
        Aggregate a season::

            from tools.validation.checks import boxscore_parity
            from tools.validation.registry import resolve

            frame, _ = resolve("cfb_pbp")
            boxscore_parity.aggregate(frame).head()
    """
    is_rush, is_sack, is_pass = c("rush") == True, c("sack") == True, c("pass") == True  # noqa: E712
    off = (
        frame.filter(c("pos_team_id").is_not_null())
        .group_by("game_id", "pos_team_id")
        .agg(
            season=c("season").first(),
            completions=_b("completion").sum(),
            interceptions=_b("int").sum(),
            rush_att=(_b("rush") + _b("sack")).sum(),
            pass_att=(_b("pass") - _b("sack")).sum(),
            rush_yds=(_y("yds_rushed", is_rush) + _y("yds_sacked", is_sack)).sum(),
            pass_yds=_y("yds_receiving", is_pass).sum(),
            first_downs=(_b("first_down_yards") | _b("first_down_penalty")).sum(),
        )
        .with_columns(total_yds=c("rush_yds") + c("pass_yds"))
        .rename({"pos_team_id": "team_key"})
    )
    # Accepted fouls only. Text yardage before 2014 is always positive, so the
    # sign rule alone would put every foul on the defense: prefer the parser's
    # resolved team. Intentional grounding is a 0-yard penalty in ESPN's box
    # (its loss is the play result).
    accepted = (_b("penalty_flag") == 1) & (_b("penalty_declined") == 0) & (_b("penalty_offset") == 0)
    sign_team = pl.when(c("penalty_yards_signed") > 0).then(c("def_pos_team_id")).otherwise(c("pos_team_id"))
    grounding = c("text").str.contains("(?i)intentional grounding").fill_null(False)
    pen = (
        frame.filter(accepted)
        .with_columns(charged=pl.coalesce(c("penalty_team_id").cast(pl.Int64), sign_team.cast(pl.Int64)))
        .filter(c("charged").is_not_null())
        .group_by("game_id", "charged")
        .agg(
            penalties=pl.len(),
            penalty_yds=pl.when(grounding).then(0).otherwise(c("penalty_yards_signed").fill_null(0).abs()).sum(),
        )
        .rename({"charged": "team_key"})
    )
    # A play wiped out by an accepted foul keeps no fumble.
    nullified = ((_b("penalty_no_play") == 1) & (_b("penalty_declined") == 0) & (_b("penalty_offset") == 0)) | c(
        "text"
    ).str.contains("(?i)nullified|NO PLAY").fill_null(False)
    kept = frame.filter(~nullified)
    fum = (
        pl.concat(
            [
                kept.filter(_b("pos_fumble_lost") == 1).select("game_id", team_key=c("pos_team_id").cast(pl.Int64)),
                kept.filter(_b("def_fumble_lost") == 1).select("game_id", team_key=c("def_pos_team_id").cast(pl.Int64)),
            ]
        )
        .filter(c("team_key").is_not_null())
        .group_by("game_id", "team_key")
        .agg(fumbles_lost=pl.len())
    )
    off = off.with_columns(c("team_key").cast(pl.Int64, strict=False).alias("__k"))
    out = (
        off.join(pen.rename({"team_key": "__k"}), on=["game_id", "__k"], how="left")
        .join(fum.rename({"team_key": "__k"}), on=["game_id", "__k"], how="left")
        .drop("__k")
        .with_columns(c("penalties", "penalty_yds", "fumbles_lost").fill_null(0))
    )
    return out.with_columns(turnovers=c("interceptions") + c("fumbles_lost"))


class JoinKeyError(RuntimeError):
    """A team join key could not be represented as canonical ``Int64``.

    Raised rather than silently dropping the affected rows: a null join key is
    removed by the inner join, which SHIFTS the measured parity rate without any
    finding being emitted -- the check would quietly grade itself on a subset.
    """


def measure(frame: pl.DataFrame, oracle: pl.DataFrame) -> pl.DataFrame:
    """Compute per (stat, season) parity rates against the ESPN box.

    Args:
        frame: Play-by-play frame carrying the columns in ``_REQUIRED``.
        oracle: ESPN team box, one row per ``(game_id, team_id)``.

    Returns:
        One row per (stat, season) with ``n`` and ``exact_pct``; ``n_complete``
        and ``complete_pct`` over team-games whose pbp rushing and passing
        attempts both equal the box; and ``within_5_pct`` for yardage stats
        (null for counts). Exempt (stat, season) pairs are omitted. An empty
        frame when no tracked stat is present on both sides.

    Raises:
        JoinKeyError: If a non-null team key fails to cast to ``Int64``, or the
            oracle's ``team_id`` is not ``Int64``. Both would silently shrink
            the joined population and bias the measured rate.

    Example:
        Measure against the committed oracle::

            import polars as pl
            from tools.validation.checks import boxscore_parity
            from tools.validation.registry import resolve

            frame, _ = resolve("cfb_pbp")
            oracle = pl.read_parquet(boxscore_parity.oracle_path("cfb_pbp"))
            print(boxscore_parity.measure(frame, oracle).head())
    """
    agg = aggregate(frame)
    # ID-dtype discipline: assert the cast is lossless instead of letting
    # `strict=False` null malformed ids that the inner join then drops.
    before = int(agg.get_column("team_key").is_not_null().sum())
    agg = agg.with_columns(team_id=c("team_key").cast(pl.Int64, strict=False)).drop("team_key")
    after = int(agg.get_column("team_id").is_not_null().sum())
    if after != before:
        raise JoinKeyError(f"{before - after} team key(s) did not survive the Int64 cast; refusing to measure a subset")
    if oracle.schema["team_id"] != pl.Int64:
        raise JoinKeyError(f"oracle team_id must be Int64, got {oracle.schema['team_id']}")
    if {"espn_total_yards", "espn_rushing_yards"} <= set(oracle.columns):
        oracle = oracle.with_columns(espn_passing_yards_derived=c("espn_total_yards") - c("espn_rushing_yards"))
    joined = agg.join(oracle.drop("season", strict=False), on=["game_id", "team_id"], how="inner")
    if {"espn_rushing_attempts", "espn_pass_attempts"} <= set(joined.columns):
        complete = (c("rush_att") == c("espn_rushing_attempts")) & (c("pass_att") == c("espn_pass_attempts"))
    else:
        complete = pl.lit(None, pl.Boolean)
    joined = joined.with_columns(__complete=complete.fill_null(False))
    rows = []
    for stat, (mine, theirs) in STATS.items():
        if mine not in joined.columns or theirs not in joined.columns:
            continue
        scoped = joined.filter(c(theirs).is_not_null() & ~c("season").is_in(sorted(EXEMPT.get(stat, ()))))
        if scoped.is_empty():
            continue
        delta = c(mine).fill_null(0) - c(theirs)
        within = (100.0 * (delta.abs() <= YARD_TOLERANCE).sum() / pl.len()) if stat in _YARDAGE_STATS else None
        rows.append(
            scoped.group_by("season")
            .agg(
                n=pl.len(),
                exact_pct=(100.0 * (delta == 0).sum() / pl.len()),
                n_complete=c("__complete").sum(),
                complete_pct=pl.when(c("__complete").sum() > 0).then(
                    100.0 * ((delta == 0) & c("__complete")).sum() / c("__complete").sum()
                ),
                within_5_pct=within if within is not None else pl.lit(None, pl.Float64),
            )
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
        One WARN Finding per (stat, season, metric) that fell below floor minus
        tolerance; empty when every season holds.

    Raises:
        JoinKeyError: Propagated from :func:`measure` when a team join key
            cannot be represented as canonical ``Int64`` -- a silent subset is
            worse than a loud failure, because it biases the measured rate.

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

    snapshot = json.loads(fp.read_text(encoding="utf-8"))
    if not any(snapshot.get(key) for key, _ in METRICS.values()):
        return []
    measured = measure(frame, pl.read_parquet(op))
    if measured.is_empty():
        return []

    tol = ctx.thresholds.get("boxscore_parity_tolerance_pp", DEFAULT_TOLERANCE_PP)
    findings: list[Finding] = []
    for row in measured.sort(["stat", "season"]).to_dicts():
        for metric, (key, col) in METRICS.items():
            floor = snapshot.get(key, {}).get(row["stat"], {}).get(str(row["season"]))
            if floor is None or row[col] is None or row[col] >= floor - tol:
                continue
            n = row["n_complete"] if metric == "complete_feed" else row["n"]
            findings.append(
                Finding(
                    "boxscore_parity",
                    Severity.WARN,
                    ctx.domain,
                    dataset,
                    f"{row['stat']!r} box-score parity ({metric}) regressed in {row['season']}: "
                    f"{row[col]:.1f}% vs floor {floor:.1f}% "
                    f"(tolerance {tol}pp, n={n:,})",
                    locator={"stat": row["stat"], "season": row["season"], "metric": metric},
                    expected=round(floor, 2),
                    actual=round(row[col], 2),
                    metric=round(row[col] - floor, 2),
                    needs_judgment=True,
                )
            )
    return findings


def _extract(dataset: str) -> int:
    oracle = extract_oracle(team_box_glob())
    _ORACLE_DIR.mkdir(parents=True, exist_ok=True)
    oracle.write_parquet(oracle_path(dataset))
    print(
        f"wrote {oracle_path(dataset)}: {oracle.height:,} team-games, seasons {oracle['season'].min()}-{oracle['season'].max()}"
    )
    return 0


def _refresh(dataset: str) -> int:
    from tools.validation.registry import resolve

    op = oracle_path(dataset)
    if not op.exists():
        print(f"missing oracle {op} -- generate it with --extract-oracle {dataset}")
        return 2
    frame, _ = resolve(dataset)
    measured = measure(frame, pl.read_parquet(op))
    payload: dict[str, object] = {"dataset": dataset, "tolerance_pp": DEFAULT_TOLERANCE_PP}
    for key, col in METRICS.values():
        floors: dict[str, dict[str, float]] = {}
        for row in measured.to_dicts():
            if row[col] is not None:
                floors.setdefault(row["stat"], {})[str(row["season"])] = round(row[col], 2)
        payload[key] = floors
    _FLOOR_DIR.mkdir(parents=True, exist_ok=True)
    with floor_path(dataset).open("w", encoding="utf-8", newline="\n") as fh:
        json.dump(payload, fh, indent=1, sort_keys=True)
        fh.write("\n")
    n = sum(len(v) for key, _ in METRICS.values() for v in payload[key].values())  # type: ignore[union-attr]
    print(f"wrote {floor_path(dataset)}: {n} (stat, season, metric) floors")
    return 0


if __name__ == "__main__":
    import sys

    args = sys.argv[1:]
    if len(args) == 2 and args[0] == "--refresh":
        sys.exit(_refresh(args[1]))
    if len(args) == 2 and args[0] == "--extract-oracle":
        sys.exit(_extract(args[1]))
    print(__doc__)
    sys.exit(2)
