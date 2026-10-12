"""Stage 4 of the ESPN play-text audit: build the JSON the browsable workspace page reads.

Four layers, all at a grain the page can regroup in the browser:

* ``rows``    (sdv-py play type, ESPN type, ESPN id, form) with per-era counts and examples;
* ``clauses`` (play type, ESPN type, id, clause kind, clause form) with tags, era counts, an example;
* ``shapes``  (play type, ESPN type, id, sequence of clause kinds) -- "rush > tackle > first_down";
* ``diff``    the parser diff: a full-corpus summary per field, the ranked (field, form, era)
  disagreement cells with example plays, the yardage cells, and the games the parser never processed.

Rows seen fewer than MIN_ROWS times are summarized per type instead of listed. Every sort
has a full tie-break, so the JSON is stable across runs.

    python tools/play_text_audit/workspace.py --data <data dir> --out <workspace.json>
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import polars as pl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import diff as DF
import templates

ERAS = [e[2] for e in templates.ERAS]
KEYS = ["play_type", "orig_play_type", "type_id"]
MIN_ROWS = 3
TAIL_SAMPLES = 8
DIFF_CELLS = 4000


def _eras(col: str = "era") -> list[pl.Expr]:
    return [(pl.col(col) == e).sum().alias(e) for e in ERAS]


def _desc(df: pl.DataFrame, first: str, rest: list[str]) -> pl.DataFrame:
    return df.sort([first, *rest], descending=[True] + [False] * len(rest))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--data", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)

    def path(name: str) -> str:
        return os.path.join(a.data, name)

    d = pl.read_parquet(path("plays.parquet")).with_columns(pl.col(*KEYS).fill_null("(none)"))
    cl = pl.read_parquet(path("clauses.parquet"))

    # ---- forms
    rows = (
        d.group_by([*KEYS, "form"])
        .agg(
            pl.len().alias("n"),
            *_eras(),
            pl.col("template").n_unique().alias("nt"),
            pl.col("text").sort_by("season", "text").first().alias("x0"),
            pl.col("text").sort_by("season", "text").get(pl.len() // 2).alias("x1"),
            pl.col("text").sort_by("season", "text").last().alias("x2"),
            pl.col("season").min().alias("s0"),
            pl.col("season").max().alias("s1"),
        )
        .pipe(_desc, "n", [*KEYS, "form"])
    )
    big, small = rows.filter(pl.col("n") >= MIN_ROWS), rows.filter(pl.col("n") < MIN_ROWS)
    tail = (
        small.sort([*KEYS, "form"])
        .group_by(KEYS, maintain_order=True)
        .agg(
            pl.len().alias("forms"),
            pl.col("n").sum().alias("plays"),
            pl.struct("form", "x0").head(TAIL_SAMPLES).alias("samples"),
        )
        .sort(KEYS)
    )

    # ---- clauses and shapes (weighted by plays through the form)
    per_form = d.group_by([*KEYS, "form", "era"]).agg(pl.len().alias("n"), pl.col("text").min().alias("x"))
    pc = per_form.join(cl.select("form", "idx", "clause", "kind", "tags"), on="form")
    clauses = (
        pc.group_by([*KEYS, "kind", "clause"])
        .agg(
            pl.col("n").sum().alias("n"),
            *[pl.col("n").filter(pl.col("era") == e).sum().alias(e) for e in ERAS],
            pl.col("tags").first().alias("tags"),
            pl.col("x").min().alias("x"),
        )
        .filter(pl.col("n") >= MIN_ROWS)
        .pipe(_desc, "n", [*KEYS, "kind", "clause"])
    )
    shape_of = (
        cl.sort("form", "idx").group_by("form", maintain_order=True).agg(pl.col("kind").str.join(" > ").alias("shape"))
    )
    shapes = (
        per_form.join(shape_of, on="form")
        .group_by([*KEYS, "shape"])
        .agg(pl.col("n").sum().alias("n"), *[pl.col("n").filter(pl.col("era") == e).sum().alias(e) for e in ERAS])
        .filter(pl.col("n") >= MIN_ROWS)
        .pipe(_desc, "n", [*KEYS, "shape"])
    )

    # ---- parser diff
    exp = DF.form_expectations(cl)
    unparsed = pl.read_parquet(path("diff_unparsed_games.parquet"))
    dd = (
        d.select("row_id", "game_id", "form")
        .join(exp, on="form", how="left")
        .join(pl.read_parquet(path("parser.parquet")), on="row_id")
        .join(unparsed.select("game_id"), on="game_id", how="anti")
    )
    summary = []
    for f, (_, col) in DF.FLAGS.items():
        nul = pl.col("exp_penalty_no_play").fill_null(False) if f in DF.NOT_ON_NULLIFIED else pl.lit(False)
        e, got = pl.col(f"exp_{f}").fill_null(False), pl.col(col).fill_null(False)
        r = dd.select(
            (e & ~nul).sum().alias("a"),
            (e & ~nul & ~got).sum().alias("b"),
            (~e & got).sum().alias("c"),
            (e & nul & ~got).sum().alias("d"),
        ).row(0)
        summary.append({"field": f, "col": col, "expected": r[0], "miss": r[1], "extra": r[2], "nullified": r[3]})
    flags = pl.read_parquet(path("diff_flags.parquet"))
    era_miss = flags.group_by("field").agg(*[pl.col("miss").filter(pl.col("era") == e).sum().alias(e) for e in ERAS])
    em = {r["field"]: [r[e] for e in ERAS] for r in era_miss.iter_rows(named=True)}
    for s in summary:
        s["eraMiss"] = em.get(s["field"], [0, 0, 0, 0])
    cells = (
        flags.with_columns((pl.col("miss") + pl.col("extra")).alias("w"))
        .pipe(_desc, "w", ["field", "era", "form"])
        .head(DIFF_CELLS)
    )
    yards = pl.read_parquet(path("diff_yards.parquet")).pipe(_desc, "bad", ["field", "era", "form"])

    def ex(r: dict) -> list[str]:
        return list(dict.fromkeys(t[:240] for t in (r["x0"], r["x1"], r["x2"]) if t))

    payload = {
        "meta": {
            "plays": d.height,
            "games": d["game_id"].n_unique(),
            "seasons": [int(d["season"].min()), int(d["season"].max())],
            "forms": d["form"].n_unique(),
            "templates": d["template"].n_unique(),
            "clauseForms": cl["clause"].n_unique(),
            "eras": ERAS,
            "eraPlays": [int(d.filter(pl.col("era") == e).height) for e in ERAS],
            "minRows": MIN_ROWS,
            "source": "cfbfastR-cfb-data cfb/pbp/parquet, built 2026-10-09 with sdv-py main abfd4928",
        },
        "rows": [
            [r["play_type"], r["orig_play_type"], r["type_id"], r["form"], r["n"], [r[e] for e in ERAS],
             r["nt"], [r["s0"], r["s1"]], ex(r)]
            for r in big.iter_rows(named=True)
        ],
        "tail": [
            [r["play_type"], r["orig_play_type"], r["type_id"], r["forms"], r["plays"],
             [[s["form"], (s["x0"] or "")[:240]] for s in r["samples"]]]
            for r in tail.iter_rows(named=True)
        ],
        "clauses": [
            [r["play_type"], r["orig_play_type"], r["type_id"], r["kind"], r["clause"], r["n"], [r[e] for e in ERAS],
             r["tags"], (r["x"] or "")[:240]]
            for r in clauses.iter_rows(named=True)
        ],
        "shapes": [
            [r["play_type"], r["orig_play_type"], r["type_id"], r["shape"], r["n"], [r[e] for e in ERAS]]
            for r in shapes.iter_rows(named=True)
        ],
        "diff": {
            "summary": summary,
            "cells": [
                [r["field"], r["parser_col"], r["era"], r["form"], r["plays"], r["expected"], r["miss"], r["extra"],
                 r["nullified"], r["miss_play_type"], [x[:240] for x in r["miss_examples"]], [x[:240] for x in r["extra_examples"]]]
                for r in cells.iter_rows(named=True)
            ],
            "yards": [
                [r["field"], r["era"], r["form"], r["plays"], r["parser_null"], r["mismatch"],
                 [[x["text"][:240], x["text_yds"], x["parser_yds"]] for x in r["examples"]]]
                for r in yards.head(DIFF_CELLS).iter_rows(named=True)
            ],
            "unparsed": [[r["game_id"], r["season"], r["plays"]] for r in unparsed.iter_rows(named=True)],
        },
    }  # fmt: skip
    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, separators=(",", ":"))
    print(
        f"{len(payload['rows']):,} form rows, {len(payload['clauses']):,} clause rows, {len(payload['shapes']):,} shapes, "
        f"{len(payload['diff']['cells']):,} diff cells -> {os.path.getsize(a.out) / 1e6:.1f} MB"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
