"""BB-D44's ``rules="mekonen"`` against ACC-2026-0075's own 2-for-1 table, 2025-26.

Run (a hoopR-nba-stats-data checkout and a clone of NathanielMekonen/NBA-2for1-Analysis @ f15d5dc)::

    uv run python tools/models/nba_two_for_one_parity.py <hoopR-nba-stats-data>/nba_stats <clone>/data/final_2for1_stats.csv

The study builds its own possessions from ``PlayByPlayV3`` and keeps 778 2025-26 rows (582 attempts,
196 non-attempts; 724 regular season, 54 playoffs). This flags our ``nba_stats_possessions`` 2026 release
(END year: 2025-26, so game ``0022500001`` must be in it) with
:func:`~sportsdataverse.nba.nba_possession_flags.add_two_for_one_flags` ``rules="mekonen"`` and joins each
of their rows to ours on ``game_id`` (Utf8 both sides), ``period`` and the offense (``TEAM_ID`` against
``offense_team_id``, Int64), taking our nearest possession within 2 s. Their start clock is their possession's
first event: the shot itself after a rebound their builder dropped (our ``start_seconds_remaining``), or the
rebound or steal when it kept one (our ``change_clock``); a join on our start clock alone matches 325 of 778.
The nearer of the two is taken (``anchor`` says which). The CSV is read with every column
as Utf8 and cast, so a numeric-looking ``game_id`` never loses its leading zeros.

Each row is tagged: ``agree``, or the first cause that explains a disagreement:

* ``unmatched_team``: no possession of ours by that offense within 2 s of their start clock, but one of the
  other team's is (their builder put the opponent's rebound or shot in this team's possession);
* ``unmatched_clock``: no possession of ours by either team starts within 2 s (their builder split or merged
  possessions where ours changes hands elsewhere, e.g. at a team rebound);
* ``start_window``: our change clock (rounded half-to-even, as theirs) is outside 34-40;
* ``late_margin``: period 4 and our ``|margin_at_change|`` is over 10 at the change of possession;
* ``no_fga``: our possession has no field-goal attempt (their possession split ours, or a block or team
  rebound moved the shot);
* ``shot_time``: the two sides put the first shot on different sides of 7 s. We time it from the change of
  possession; they use their possession's length or its time to first shot by event count
  (``their_rule``: ``one_event`` and ``three_event`` are their special cases);
* ``end_clock``: both say the first shot came after 7 s, and the end clocks fall either side of 24.

Their builder is heuristic, so a disagreement is not a defect in ours by default. The per-row result is
written beside this file as ``nba_two_for_one_parity.csv``; the summary prints.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

from sportsdataverse.nba.nba_possession_flags import TWO_FOR_ONE_SCHEMA, add_two_for_one_flags

SEASON = 2026
TOLERANCE = 2.0


def theirs(csv: Path) -> pl.DataFrame:
    m = pl.read_csv(csv, infer_schema=False)
    clock = pl.col("START_CLOCK").str.extract_groups(r"PT(\d+)M([\d.]+)S")
    end = pl.col("END_CLOCK").str.extract_groups(r"PT(\d+)M([\d.]+)S")
    first = pl.col("ACTION_TYPES").str.split(",").list.first().str.strip_chars()
    last = pl.col("ACTION_TYPES").str.split(",").list.last().str.strip_chars()
    events = pl.col("EVENTS").cast(pl.Int64)
    return m.with_row_index("their_row").select(
        "their_row",
        pl.col("GAME_ID").alias("game_id"),
        pl.col("PERIOD").cast(pl.Int64).alias("period"),
        pl.col("TEAM_ID").cast(pl.Int64).alias("offense_team_id"),
        (clock.struct[0].cast(pl.Float64) * 60 + clock.struct[1].cast(pl.Float64)).alias("their_start"),
        (end.struct[0].cast(pl.Float64) * 60 + end.struct[1].cast(pl.Float64)).alias("their_end"),
        pl.col("SHOT_TIME_FOR_ANALYSIS").cast(pl.Float64).alias("their_shot_time"),
        pl.when(pl.col("TWO_FOR_ONE_ATTEMPT") == "1").then(pl.lit("went")).otherwise(pl.lit("held")).alias("theirs"),
        pl.when((events == 1) & last.is_in(["Made Shot", "Missed Shot"]))
        .then(pl.lit("one_event"))
        .when((events == 3) & (first == "Missed Shot") & last.is_in(["Made Shot", "Missed Shot"]))
        .then(pl.lit("three_event"))
        .when((events <= 2) | (first == "Missed Shot"))
        .then(pl.lit("poss_length"))
        .otherwise(pl.lit("first_shot"))
        .alias("their_rule"),
    )


def ours(data: Path) -> pl.DataFrame:
    poss = pl.read_parquet(data / "possessions" / "parquet" / f"nba_possessions_{SEASON}.parquet")
    pbp = pl.read_parquet(data / "pbp" / "parquet" / f"nba_play_by_play_{SEASON}.parquet")
    assert (poss["game_id"] == "0022500001").any(), "the 2026 release is not 2025-26"
    f = add_two_for_one_flags(poss.drop(*TWO_FOR_ONE_SCHEMA, strict=False), pbp, rules="mekonen")
    return f.select(
        "game_id",
        "period",
        "offense_team_id",
        "possession_number",
        pl.col("start_seconds_remaining").alias("our_start"),
        pl.col("end_seconds_remaining").alias("our_end"),
        "change_clock",
        "seconds_to_first_shot",
        "margin_at_change",
        pl.col("two_for_one").alias("ours"),
    )


def compare(t: pl.DataFrame, o: pl.DataFrame) -> pl.DataFrame:
    keys = ["game_id", "period", "offense_team_id"]
    for k in keys:
        assert t.schema[k] == o.schema[k], f"{k}: theirs {t.schema[k]} vs ours {o.schema[k]}"

    def nearest(anchor: str, by: list[str] = keys) -> pl.DataFrame:
        return (
            t.sort("their_start")
            .join_asof(
                o.sort(anchor).with_columns(pl.col(anchor).alias("_k")),
                left_on="their_start",
                right_on="_k",
                by=by,
                strategy="nearest",
                tolerance=TOLERANCE,
                check_sortedness=False,
            )
            .with_columns((pl.col("_k") - pl.col("their_start")).abs().alias("_d"), pl.lit(anchor).alias("anchor"))
            .drop("_k")
        )

    # Their start is their possession's first event: the shot itself (our start clock) when their builder
    # dropped the rebound or steal, else that event (our change clock). Take whichever of ours is nearer.
    both = pl.concat([nearest("our_start"), nearest("change_clock")])
    j = (
        both.sort("their_row", pl.col("_d").fill_null(float("inf")), "anchor")
        .unique("their_row", keep="first", maintain_order=True)
        .with_columns(pl.when(pl.col("_d").is_null()).then(None).otherwise(pl.col("anchor")).alias("anchor"))
        .drop("_d")
    )
    # an unmatched row whose clock does match a possession of the other team: they credited the wrong side
    side = ["game_id", "period"]
    other = (
        pl.concat(
            nearest(a, side).select("their_row", pl.col("possession_number").is_not_null().alias("_any"))
            for a in ("our_start", "change_clock")
        )
        .group_by("their_row")
        .agg(pl.col("_any").any())
    )
    j = j.join(other, on="their_row", how="left")
    start_ok = pl.col("change_clock").round(0, mode="half_to_even").is_between(34, 40)
    late = (pl.col("period") == 4) & (pl.col("margin_at_change").abs() > 10)
    return j.with_columns(
        pl.when(pl.col("possession_number").is_null() & pl.col("_any"))
        .then(pl.lit("unmatched_team"))
        .when(pl.col("possession_number").is_null())
        .then(pl.lit("unmatched_clock"))
        .when(pl.col("ours") == pl.col("theirs"))
        .then(pl.lit("agree"))
        .when(~start_ok)
        .then(pl.lit("start_window"))
        .when(late)
        .then(pl.lit("late_margin"))
        .when(pl.col("seconds_to_first_shot").is_null())
        .then(pl.lit("no_fga"))
        .when((pl.col("seconds_to_first_shot") <= 7) != (pl.col("their_shot_time") <= 7))
        .then(pl.lit("shot_time"))
        .otherwise(pl.lit("end_clock"))
        .alias("cause")
    ).drop("_any")


if __name__ == "__main__":
    data, csv = Path(sys.argv[1]), Path(sys.argv[2])
    res = compare(theirs(csv), ours(data))
    res.write_csv(Path(__file__).with_suffix(".csv"), float_precision=3)
    matched = res.filter(~pl.col("cause").str.starts_with("unmatched"))
    for a in ("our_start", "change_clock"):
        print(f"matched nearest on {a}: {res.filter(pl.col('anchor') == a).height}")
    print(
        f"their rows {res.height}; matched within {TOLERANCE} s: {matched.height} ({matched.height / res.height:.3f})"
    )
    agree = matched.filter(pl.col("cause") == "agree").height
    print(f"class agreement on matched rows: {agree} ({agree / matched.height:.3f})")
    with pl.Config(tbl_rows=40):
        print(res.group_by("cause").len().sort("len", descending=True))
        print(res.group_by("theirs", pl.col("ours").fill_null("none")).len().sort("theirs", "ours"))
        print(res.filter(pl.col("cause") == "shot_time").group_by("their_rule").len().sort("len", descending=True))
        print(res.group_by(pl.col("game_id").str.slice(2, 1).alias("type"), "cause").len().sort("type", "cause"))
