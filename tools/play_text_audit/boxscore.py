"""Stage 5 of the ESPN play-text audit: player- and team-level parity of pbp aggregates vs ESPN's box score.

ESPN's own box (cfbfastR-cfb-data ``player_box`` / ``team_box``, a reshape of the
summary JSON -- independent of pbp) is the judge. pbp is summed with ESPN's
college conventions, which the team check (``tools/validation/checks/boxscore_parity``)
already proved: sacks are RUSHING (the sacked passer's carries and yards), pass
attempts exclude sacks.

Player grain: (game_id, athlete_id, category) for passing, rushing, receiving,
kicking (FG), punting, kick/punt returns, interceptions, and defensive sacks.
Team grain: first downs and total yards from ``team_box`` (the existing oracle's
two unchecked columns). Writes ``box_player.parquet`` (every compared player
line), ``box_player_summary.parquet`` (exact-match % per stat x season, with
coverage), ``box_team_extra.parquet``.

    python tools/play_text_audit/boxscore.py --data <data dir> [--box <cfb-data cfb dir>]
"""

from __future__ import annotations

import argparse
import glob
import os
import sys

import polars as pl

T = pl.col


# The player-line definitions are the validation gate's; the audit adds the
# long per-line output and the team extras on top.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from tools.validation.checks.boxscore_parity_player import (  # noqa: E402
    _ESPN_ROW_FILTER,
    ESPN_STATS,
    pbp_player_lines,
)
from tools.validation.checks.boxscore_parity_player import espn_player_lines as _espn_lines  # noqa: E402


def _b(col: str) -> pl.Expr:
    return (T(col) == True).fill_null(False)


def _n(col: str) -> pl.Expr:
    return T(col).fill_null(0)


def _int(col: str) -> pl.Expr:
    return T(col).str.replace_all(",", "").cast(pl.Float64, strict=False)


def espn_player_lines(box_dir: str) -> pl.DataFrame:
    return _espn_lines(os.path.join(box_dir, "player_box", "parquet", "player_box_*.parquet"))


def compare_players(pbp: pl.DataFrame, espn: pl.DataFrame, games: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Long (game, athlete, category, stat) comparison + exact-match summary per stat x season."""
    for k in ("game_id", "athlete_id"):
        assert pbp.schema[k] == espn.schema[k], (k, pbp.schema[k], espn.schema[k])
    espn = espn.drop("season", strict=False).join(
        games.select("game_id"), on="game_id", how="semi"
    )  # only games present in pbp
    pbp = pbp.join(games.select("game_id"), on="game_id", how="semi")
    rows = []
    for cat, stats in ESPN_STATS.items():
        e = espn.filter((T("category") == cat) & _ESPN_ROW_FILTER.get(cat, pl.lit(True)))
        p = pbp.filter(T("category") == cat)
        j = e.join(p, on=["game_id", "athlete_id", "category"], how="full", coalesce=True)
        for s, _ in stats:
            if s not in j.columns:
                continue
            rows.append(
                j.select(
                    "game_id",
                    "athlete_id",
                    "category",
                    pl.lit(s).alias("stat"),
                    T("athlete_name"),
                    T(f"espn_{s}").cast(pl.Float64).alias("espn"),
                    T(s).cast(pl.Float64).alias("pbp"),
                )
            )
    long = pl.concat(rows, how="diagonal_relaxed").join(games, on="game_id", how="left")  # season per game
    long = long.with_columns(
        pl.when(T("espn").is_null())
        .then(pl.lit("pbp_only"))
        .when(T("pbp").is_null())
        .then(pl.lit("espn_only"))
        .when((T("espn") - T("pbp")).abs() < 0.01)
        .then(pl.lit("exact"))
        .otherwise(pl.lit("differs"))
        .alias("status")
    )
    summary = (
        long.filter(T("season").is_not_null())
        .group_by("category", "stat", "season")
        .agg(
            pl.len().alias("lines"),
            (T("status") == "exact").sum().alias("exact"),
            (T("status") == "differs").sum().alias("differs"),
            (T("status") == "espn_only").sum().alias("espn_only"),
            (T("status") == "pbp_only").sum().alias("pbp_only"),
        )
        .with_columns(
            (T("exact") / (T("exact") + T("differs")) * 100).round(1).alias("exact_pct_matched"),
            (T("exact") / (T("lines") - T("pbp_only")) * 100).round(1).alias("exact_pct_of_espn"),
        )
        .sort("category", "stat", "season")
    )
    return long, summary


def team_extra(d: pl.DataFrame, box_dir: str) -> pl.DataFrame:
    """First downs and total yards per (game, team) vs ESPN team_box."""
    tb = pl.concat(
        [
            pl.read_parquet(f)
            for f in sorted(glob.glob(os.path.join(box_dir, "team_box", "parquet", "team_box_*.parquet")))
        ],
        how="diagonal_relaxed",
    )
    tb = tb.select(
        "game_id",
        T("team_id").alias("pos_team_id"),
        "season",
        _int("firstDowns").alias("espn_first_downs"),
        _int("totalYards").alias("espn_total_yards"),
    )
    first = _b("firstD_by_poss").cast(pl.Int64)  # one per new series earned on the drive's plays
    agg = (
        d.filter(T("pos_team_id").is_not_null())
        .group_by("game_id", "pos_team_id")
        .agg(
            first_downs=(_b("first_down_yards") | _b("first_down_penalty")).sum(),
            first_downs_poss=first.sum(),
            total_yards=(
                pl.when(_b("rush")).then(_n("yds_rushed")).otherwise(0)
                + pl.when(_b("sack")).then(_n("yds_sacked")).otherwise(0)
                + pl.when(_b("completion")).then(_n("yds_receiving")).otherwise(0)
            ).sum(),
        )
    )
    return agg.join(tb, on=["game_id", "pos_team_id"], how="inner")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--data", required=True)
    ap.add_argument("--box", default="/mnt/sdv_repos/cfbfastR-cfb-data/cfb")
    a = ap.parse_args(argv)
    plays = pl.read_parquet(os.path.join(a.data, "plays.parquet"), columns=["row_id", "game_id", "season"])
    d = plays.join(pl.read_parquet(os.path.join(a.data, "parser.parquet")), on="row_id")
    unparsed = pl.read_parquet(os.path.join(a.data, "diff_unparsed_games.parquet")).select("game_id")
    d = d.join(unparsed, on="game_id", how="anti")
    games = d.select("game_id", "season").unique()

    long, summary = compare_players(pbp_player_lines(d), espn_player_lines(a.box), games)
    long.write_parquet(os.path.join(a.data, "box_player.parquet"))
    summary.write_parquet(os.path.join(a.data, "box_player_summary.parquet"))
    te = team_extra(d, a.box)
    te.write_parquet(os.path.join(a.data, "box_team_extra.parquet"))
    pooled = (
        summary.group_by("category", "stat")
        .agg(T("exact", "differs", "espn_only", "pbp_only").sum())
        .with_columns((T("exact") / (T("exact") + T("differs")) * 100).round(1).alias("exact_pct_matched"))
        .sort("category", "stat")
    )
    print(pooled)
    print(f"{long.height:,} player-stat comparisons; team extra {te.height:,} team-games")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
