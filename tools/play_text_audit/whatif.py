"""Stage 6 of the ESPN play-text audit: project how much box-score parity each candidate fix would recover.

Each "what-if" rewrites the parsed columns the way the proposed sdv-py fix would, then
re-measures player-level parity (``boxscore.compare_players``) against ESPN's player box.
The rewrite is the stand-in for the fixed parser, so the gain is an estimate of the fix's
reach, not proof the fix is right -- the fix still needs its fixture test.

What-ifs:
* ``F1_rush_yards``  -- null ``yds_rushed`` on rush plays filled from the text (finding R2).
* ``F3_roster_ids``  -- ``*_player_id`` filled by matching the name against the game's roster,
  unique matches only (finding R5: offline ids come from the box score, which lists ~27 of
  ~60 players before 2014).
* ``ALL``            -- every what-if applied together.

    python tools/play_text_audit/whatif.py --data <data dir>
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

import polars as pl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import boxscore as BX
import diff as DF

T = pl.col
ROSTERS = "/mnt/sdv_repos/cfbfastR-cfb-raw/cfb/game_rosters/json"
ROLE_COLS = [
    (f"{r}_player_name", f"{r}_player_id")
    for r in (
        "passer",
        "rusher",
        "receiver",
        "sack",
        "interception",
        "punter",
        "punt_return",
        "kickoff",
        "kickoff_return",
        "fg_kicker",
    )
]


def name_key(s: str | None) -> str | None:
    """Order-free name key: lowercase letters only, suffixes dropped, tokens sorted ("Smith Jr., John" == "John Smith")."""
    if not s:
        return None
    toks = [t for t in re.sub(r"[^a-z\s]", " ", s.lower()).split() if t not in {"jr", "sr", "ii", "iii", "iv"}]
    return " ".join(sorted(toks)) or None


def load_rosters(game_ids: set[int]) -> pl.DataFrame:
    rows = []
    for f in os.listdir(ROSTERS):
        if not f.endswith(".json"):
            continue
        gid = int(f.split(".")[0])
        if gid not in game_ids:
            continue
        with open(os.path.join(ROSTERS, f)) as fh:
            j = json.load(fh)
        for a in j.get("data") or []:
            for nm in {a.get("athlete_display_name"), a.get("full_name")}:
                k = name_key(nm)
                if k and a.get("athlete_id") is not None:
                    rows.append((gid, k, int(a["athlete_id"])))
    r = pl.DataFrame(
        rows, schema={"game_id": pl.Int64, "key": pl.String, "athlete_id": pl.Int64}, orient="row"
    ).unique()
    # a key that maps to two athletes in one game is ambiguous: drop it rather than guess
    return r.group_by("game_id", "key").agg(T("athlete_id").first(), pl.len().alias("n")).filter(T("n") == 1).drop("n")


def f1_rush_yards(d: pl.DataFrame) -> pl.DataFrame:
    txt = DF.text_yards(DF.YARDS["yds_rushed"][0])
    return d.with_columns(pl.coalesce("yds_rushed", pl.when(T("rush") == True).then(txt)).alias("yds_rushed"))


def f3_roster_ids(d: pl.DataFrame, rosters: pl.DataFrame) -> pl.DataFrame:
    for name_col, id_col in ROLE_COLS:
        k = f"__k_{id_col}"
        names = d.select(T(name_col).alias("__n")).unique().drop_nulls()  # key each distinct name once
        names = names.with_columns(T("__n").map_elements(name_key, return_dtype=pl.String).alias(k))
        d = d.join(names, left_on=name_col, right_on="__n", how="left")
        d = d.join(rosters.rename({"key": k, "athlete_id": f"__r_{id_col}"}), on=["game_id", k], how="left")
        d = d.with_columns(pl.coalesce(id_col, f"__r_{id_col}").alias(id_col)).drop(k, f"__r_{id_col}")
    return d


def r10_kneel_yards(d: pl.DataFrame) -> pl.DataFrame:
    """Kneel-downs with null yards: "(team loss of 2)" / "for loss of 6 yards" -> -N, else statYardage (R10)."""
    loss = T("text").str.extract(r"(?i)loss of (\d+)", 1).cast(pl.Int64, strict=False)
    fill = pl.when(_kneel()).then(pl.coalesce(-loss, T("statYardage")))
    return d.with_columns(pl.coalesce("yds_rushed", fill).alias("yds_rushed"))


def _kneel() -> pl.Expr:
    return (T("kneel_down") == True).fill_null(False) | T("text").str.contains(r"(?i)\bkneel|takes a knee")


def team_parity(d: pl.DataFrame, oracle: pl.DataFrame, pass_oracle: str) -> pl.DataFrame:
    """Team rush/pass parity per season with the boxscore_parity conventions (sacks are rushing)."""

    def b(col: str) -> pl.Expr:
        return (T(col) == True).fill_null(False)

    agg = (
        d.filter(T("pos_team_id").is_not_null())
        .group_by("game_id", "pos_team_id")
        .agg(
            T("season").first(),
            (b("rush").cast(pl.Int64) + b("sack").cast(pl.Int64)).sum().alias("rush_att"),
            (
                pl.when(b("rush")).then(T("yds_rushed").fill_null(0)).otherwise(0)
                + pl.when(b("sack")).then(T("yds_sacked").fill_null(0)).otherwise(0)
            )
            .sum()
            .alias("rush_yds"),
            pl.when(b("completion")).then(T("yds_receiving").fill_null(0)).otherwise(0).sum().alias("pass_yds"),
        )
    )
    o = oracle.with_columns((T("espn_total_yards") - T("espn_rushing_yards")).alias("espn_pass_corrected")).select(
        T("game_id"),
        T("team_id").alias("pos_team_id"),
        "espn_rushing_attempts",
        "espn_rushing_yards",
        T(pass_oracle).alias("espn_pass"),
    )
    j = agg.join(o, on=["game_id", "pos_team_id"])

    def ex(a: str, e: str) -> pl.Expr:
        return ((T(a) - T(e)).abs() < 0.5).mean().mul(100).round(1)

    return (
        j.group_by("season")
        .agg(
            ex("rush_att", "espn_rushing_attempts").alias("rush_att"),
            ex("rush_yds", "espn_rushing_yards").alias("rush_yds"),
            ex("pass_yds", "espn_pass").alias("pass_yds"),
        )
        .sort("season")
    )


def measure(d: pl.DataFrame, espn: pl.DataFrame, games: pl.DataFrame, label: str) -> pl.DataFrame:
    _, summary = BX.compare_players(BX.pbp_player_lines(d), espn, games)
    era = (
        pl.when(T("season") <= 2006)
        .then(pl.lit("2004-06"))
        .when(T("season") <= 2013)
        .then(pl.lit("2007-13"))
        .when(T("season") <= 2019)
        .then(pl.lit("2014-19"))
        .otherwise(pl.lit("2020-26"))
    )
    return (
        summary.with_columns(era.alias("era"))
        .group_by("category", "stat", "era")
        .agg(T("exact", "differs", "espn_only").sum())
        .with_columns(
            (T("exact") / (T("exact") + T("differs") + T("espn_only")) * 100).round(1).alias(label),
        )
        .select("category", "stat", "era", label)
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--data", required=True)
    ap.add_argument("--box", default="/mnt/sdv_repos/cfbfastR-cfb-data/cfb")
    ap.add_argument("--team", action="store_true", help="team-level projection only")
    a = ap.parse_args(argv)
    plays = pl.read_parquet(os.path.join(a.data, "plays.parquet"), columns=["row_id", "game_id", "season", "text"])
    d = plays.join(pl.read_parquet(os.path.join(a.data, "parser.parquet")), on="row_id")
    d = d.join(
        pl.read_parquet(os.path.join(a.data, "diff_unparsed_games.parquet")).select("game_id"), on="game_id", how="anti"
    )
    games = d.select("game_id", "season").unique()
    if a.team:
        return team_main(d, a.data)
    espn = BX.espn_player_lines(a.box)
    rosters = load_rosters(set(games["game_id"].to_list()))
    print(f"roster keys: {rosters.height:,} across {rosters['game_id'].n_unique():,} games", file=sys.stderr)

    variants = {
        "now": d,
        "F1_rush_yards": f1_rush_yards(d),
        "F3_roster_ids": f3_roster_ids(d, rosters),
    }
    variants["ALL"] = f3_roster_ids(f1_rush_yards(d), rosters)
    out = None
    for label, frame in variants.items():
        m = measure(frame, espn, games, label)
        out = m if out is None else out.join(m, on=["category", "stat", "era"], how="full", coalesce=True)
    out = out.sort("category", "stat", "era")
    out.write_parquet(os.path.join(a.data, "whatif_player.parquet"))
    pl.Config.set_tbl_rows(120)
    pl.Config.set_tbl_width_chars(200)
    print(out.with_columns((T("ALL") - T("now")).round(1).alias("gain")).filter(T("gain").abs() >= 0.5))
    return 0


def team_main(d: pl.DataFrame, data: str) -> int:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
    from tools.validation.checks import boxscore_parity as bp

    oracle = pl.read_parquet(bp.oracle_path("cfb_pbp")).with_columns(T("game_id", "team_id").cast(pl.Int64))
    steps = [
        ("now", d, "espn_net_passing_yards"),
        ("+O1 pass oracle", d, "espn_pass_corrected"),
        ("+R2 rush yds", f1_rush_yards(d), "espn_pass_corrected"),
        ("+R10 kneel yds", r10_kneel_yards(f1_rush_yards(d)), "espn_pass_corrected"),
    ]
    out = None
    for label, frame, po in steps:
        t = team_parity(frame, oracle, po).rename({c: f"{c} | {label}" for c in ("rush_att", "rush_yds", "pass_yds")})
        out = t if out is None else out.join(t, on="season")
    out.write_parquet(os.path.join(data, "whatif_team.parquet"))
    keep = [
        "season",
        "rush_yds | now",
        "rush_yds | +R2 rush yds",
        "rush_yds | +R10 kneel yds",
        "pass_yds | now",
        "pass_yds | +O1 pass oracle",
        "rush_att | now",
    ]
    pl.Config.set_tbl_rows(30)
    pl.Config.set_tbl_width_chars(220)
    print(out.select(keep))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
