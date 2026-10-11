"""Stage 3 of the ESPN play-text audit: diff what each play's text says against what sdv-py parsed.

Expectations come from the clause tags (``clauses.py``), never from the parser:
a play whose text has a ``rush`` clause is expected to carry ``rush``; a ``pass``
inside a penalty clause ("pass interference") is not a pass attempt, because a
clause's kind is its main action and penalty outranks everything. Yardage is read
independently from the raw text with one anchored pattern per action and compared
with the parser's yardage columns.

For every (field, form, era) the diff counts plays where the text expects the flag
but the parser has it false (``miss``) and plays where the parser sets it with no
supporting text (``extra``). A disagreement is a lead, not a verdict: some fields
mean something narrower than the text (a nullified play, a synthesized row), which
the examples make visible. Output: ``diff_flags.parquet``, ``diff_yards.parquet``
and ``diff_summary.parquet``.

    python tools/play_text_audit/diff.py --data <data dir>
"""

from __future__ import annotations

import argparse
import os

import polars as pl

ERAS = ["2004-06", "2007-13", "2014-19", "2020-26"]
ACTION_PASS = {"pass_complete", "pass_incomplete", "pass", "interception"}
# The parser does not count a play the officials wiped out ("..., NO PLAY") as a rush, pass, sack or
# turnover. Those plays are counted as `nullified`, never as misses.
NOT_ON_NULLIFIED = {"rush", "pass_attempt", "completion", "sack", "interception", "fumble", "touchdown", "kneel"}


def _kinds(*k: str) -> pl.Expr:
    return pl.any_horizontal([pl.col("kinds").list.contains(x) for x in k])


def _tag(tag: str) -> pl.Expr:
    """Tag carried by any non-penalty clause (a penalty clause's words describe the foul, not the play)."""
    return pl.col("np_tags").list.contains(tag)


def _any_tag(tag: str) -> pl.Expr:
    return pl.col("all_tags").list.contains(tag)


# field -> (expectation expression over the form's clauses, parser column)
FLAGS: dict[str, tuple[pl.Expr, str]] = {
    "rush": (_kinds("rush", "kneel"), "rush"),  # sdv-py counts a kneel as a rush, as cfbfastR does
    "pass_attempt": (_kinds(*ACTION_PASS), "pass_attempt"),
    "completion": (_kinds("pass_complete"), "completion"),
    "sack": (_kinds("sack"), "sack"),
    "interception": (_tag("interception"), "int"),
    "fumble": (_tag("fumble"), "fumble_vec"),
    "penalty": (_kinds("penalty"), "penalty_flag"),
    "penalty_no_play": (_any_tag("no_play"), "penalty_no_play"),
    "penalty_declined": (_any_tag("declined"), "penalty_declined"),
    "touchdown": (_tag("touchdown"), "touchdown"),  # td_play is a narrower text-match flag
    "safety": (_tag("safety"), "safety"),
    "kickoff": (_kinds("kickoff"), "kickoff_play"),
    "kickoff_onside": (_tag("onside"), "kickoff_onside"),
    "kickoff_touchback": (_kinds("kickoff") & _tag("touchback"), "kickoff_tb"),
    "kickoff_fair_catch": (_kinds("kickoff") & _tag("fair_catch"), "kickoff_fair_catch"),
    "punt": (_kinds("punt"), "punt"),
    "punt_touchback": (_kinds("punt") & _tag("touchback"), "punt_tb"),
    "punt_fair_catch": (_kinds("punt") & _tag("fair_catch"), "punt_fair_catch"),
    "punt_blocked": (_kinds("punt") & _tag("blocked"), "punt_blocked"),
    "field_goal": (_kinds("field_goal"), "fg_attempt"),
    "field_goal_made": (_kinds("field_goal") & _tag("good") & ~_tag("missed") & ~_tag("blocked"), "fg_made"),
    "extra_point": (_tag("extra_point"), "xp_attempt"),
    "extra_point_made": (_tag("extra_point") & _tag("good") & ~_tag("missed") & ~_tag("blocked"), "xp_made"),
    "kneel": (_kinds("kneel"), "kneel_down"),
    "pass_breakup": (_tag("breakup"), "pass_breakup"),
}
PARSER_FLAGS = sorted({c for _, c in FLAGS.values()})

# yardage read straight from the raw text. Groups: 1 "no gain" (0), 2 "loss of N" / 3 "N yds loss" (2025+
# vendor) -> -N, 4 signed yards. Polars runs these, so Rust regex: no lookaround.
YD = r"(?:yards?|yds?|yd)\b"
GAIN = r"\bfor (?:(no gain)|(?:a )?loss of (\d+)|(\d+) " + YD + r" loss|(-?\d+) " + YD + ")"
# Each read applies only to plays whose clauses are that action ("Xavier Rush for 10 yds" is a catch).
YARDS = {
    "yds_rushed": (r"(?i)\b(?:run|rush|rushed|scrambles?|scrambled|keeper)\b[^,;]*?" + GAIN, "yds_rushed", "exp_rush"),
    "yds_receiving": (r"(?i)\bcomplete\b[^;]*?" + GAIN, "yds_receiving", "exp_completion"),
    "yds_punted": (r"(?i)\bpunt(?:s|ed)?\b(?: for)? ()()()(\d+) " + YD, "yds_punted", "exp_punt"),
    "yds_kickoff": (r"(?i)\bkick-?off\b(?: for)? ()()()(\d+) " + YD, "yds_kickoff", "exp_kickoff"),
}


def text_yards(rx: str) -> pl.Expr:
    nogain = pl.col("text").str.extract(rx, 1)
    loss = pl.coalesce(pl.col("text").str.extract(rx, 2), pl.col("text").str.extract(rx, 3)).cast(
        pl.Int64, strict=False
    )
    signed = pl.col("text").str.extract(rx, 4).cast(pl.Int64, strict=False)
    return pl.when(nogain.is_not_null() & (nogain != "")).then(0).when(loss.is_not_null()).then(-loss).otherwise(signed)


def form_expectations(clauses: pl.DataFrame) -> pl.DataFrame:
    per_form = clauses.group_by("form").agg(
        pl.col("kind").alias("kinds"),
        pl.col("tags").explode(empty_as_null=False).drop_nulls().unique().alias("all_tags"),
        pl.col("tags")
        .filter(pl.col("kind") != "penalty")
        .explode(empty_as_null=False)
        .drop_nulls()
        .unique()
        .alias("np_tags"),
    )
    return per_form.select("form", *[expr.alias(f"exp_{f}") for f, (expr, _) in FLAGS.items()])


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--data", required=True)
    a = ap.parse_args(argv)

    plays = pl.read_parquet(
        os.path.join(a.data, "plays.parquet"),
        columns=["row_id", "game_id", "season", "era", "play_type", "orig_play_type", "type_id", "text", "form"],
    )
    parser = pl.read_parquet(os.path.join(a.data, "parser.parquet"))
    assert plays.schema["row_id"] == parser.schema["row_id"], (plays.schema["row_id"], parser.schema["row_id"])
    exp = form_expectations(pl.read_parquet(os.path.join(a.data, "clauses.parquet")))
    d = plays.join(exp, on="form", how="left").join(parser, on="row_id", how="left")
    assert d.height == plays.height

    # Games the parser never processed (most plays carry no flag at all) are one finding, not
    # thousands of per-form misses: list them and leave them out of the cells.
    any_flag = pl.any_horizontal(
        [
            pl.col(c).fill_null(False)
            for c in ("rush", "pass", "sack", "punt", "kickoff_play", "fg_attempt", "penalty_flag", "xp_attempt")
        ]
    )
    games = d.group_by("game_id", "season").agg(pl.len().alias("plays"), (~any_flag).mean().alias("no_flag_share"))
    unparsed = games.filter(pl.col("no_flag_share") > 0.5).sort("season", "game_id")
    unparsed.write_parquet(os.path.join(a.data, "diff_unparsed_games.parquet"))
    d = d.join(unparsed.select("game_id"), on="game_id", how="anti")

    # ---- flags
    flag_rows = []
    for f, (_, col) in FLAGS.items():
        got = pl.col(col).fill_null(False)
        nullified = pl.col("exp_penalty_no_play").fill_null(False) if f in NOT_ON_NULLIFIED else pl.lit(False)
        e = pl.col(f"exp_{f}").fill_null(False) & ~nullified
        flag_rows.append(
            d.group_by("era", "form")
            .agg(
                pl.lit(f).alias("field"),
                pl.lit(col).alias("parser_col"),
                pl.len().alias("plays"),
                e.sum().alias("expected"),
                (e & got).sum().alias("agree"),
                (e & ~got).sum().alias("miss"),
                (~e & got).sum().alias("extra"),
                (pl.col(f"exp_{f}").fill_null(False) & nullified & ~got).sum().alias("nullified"),
                pl.col("text").filter(e & ~got).head(3).alias("miss_examples"),
                pl.col("text").filter(~e & got).head(3).alias("extra_examples"),
                pl.col("play_type").filter(e & ~got).mode().first().alias("miss_play_type"),
            )
            .filter((pl.col("miss") > 0) | (pl.col("extra") > 0))
        )
    flags = pl.concat(flag_rows).sort("miss", descending=True)
    flags.write_parquet(os.path.join(a.data, "diff_flags.parquet"))

    # ---- yards
    yard_rows = []
    for f, (rx, col, gate) in YARDS.items():
        t = d.select(
            "era",
            "form",
            "text",
            "play_type",
            text_yards(rx).alias("text_yds"),
            pl.col(col).alias("parser_yds"),
            pl.col("exp_penalty_no_play").fill_null(False).alias("nullified"),
            pl.col(gate).fill_null(False).alias("gate"),
        )
        t = t.filter(
            pl.col("text_yds").is_not_null() & ~pl.col("nullified") & pl.col("gate")
        )  # wiped-out play: no yardage
        yard_rows.append(
            t.group_by("era", "form")
            .agg(
                pl.lit(f).alias("field"),
                pl.len().alias("plays"),
                (pl.col("parser_yds") == pl.col("text_yds")).sum().alias("agree"),
                pl.col("parser_yds").is_null().sum().alias("parser_null"),
                ((pl.col("parser_yds") != pl.col("text_yds")) & pl.col("parser_yds").is_not_null())
                .sum()
                .alias("mismatch"),
                pl.struct("text", "text_yds", "parser_yds")
                .filter(pl.col("parser_yds").is_null() | (pl.col("parser_yds") != pl.col("text_yds")))
                .head(3)
                .alias("examples"),
            )
            .filter((pl.col("parser_null") + pl.col("mismatch")) > 0)
        )
    yards = (
        pl.concat(yard_rows)
        .with_columns((pl.col("parser_null") + pl.col("mismatch")).alias("bad"))
        .sort("bad", descending=True)
    )
    yards.write_parquet(os.path.join(a.data, "diff_yards.parquet"))

    # ---- per field x era summary
    summ_flags = flags.group_by("field", "parser_col", "era").agg(
        pl.col("expected").sum(), pl.col("miss").sum(), pl.col("extra").sum()
    )
    summ_yards = yards.group_by("field", "era").agg(
        pl.col("plays").sum(), pl.col("parser_null").sum(), pl.col("mismatch").sum()
    )
    summ_flags.write_parquet(os.path.join(a.data, "diff_summary_flags.parquet"))
    summ_yards.write_parquet(os.path.join(a.data, "diff_summary_yards.parquet"))
    print(
        f"flags: {flags['miss'].sum():,} misses / {flags['extra'].sum():,} extras over {flags.height:,} (field, form, era) cells"
    )
    print(
        f"yards: {yards['parser_null'].sum():,} parser-null / {yards['mismatch'].sum():,} mismatches over {yards.height:,} cells"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
