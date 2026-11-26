"""Walk-forward evaluation behind the 2026-11-25 garbage-time rule in ``cfb_adjusted_epa``.

Ratings through week w-1 predict week w margins (``scale * rating_diff + hfa * home``); the two
coefficients for a test season are fit on the other seasons (leave-one-season-out). Scored: week
>= 4 including the postseason (ordered after the regular season), both teams with >= 2 prior
games, games every variant rates. CI: paired MAE delta vs ``shipped``, bootstrap over
(season, week) clusters. Also the Spearman of the week-4 as-of rating against the rest-of-season
mean point margin.

Variants (opponent-strength fit plays, own-EPA plays):
  shipped      naive WP 5-95, every play   (the fit before 2026-11-25)
  wp_both      naive WP 5-95, same plays
  margin_fit   margin rule,   every play
  margin_both  margin rule,   same plays    (the fit since 2026-11-25)

2014-2023 (5,829 games): margin_both -0.194 [-0.293, -0.101], margin_fit -0.032 [-0.090, +0.022],
wp_both -0.064 [-0.204, +0.072]; week-4 Spearman 0.430 / 0.412 / 0.430 vs shipped 0.384.

Usage::

    PYTHONPATH=<sdv-py checkout> python -m tools.validation.cfb_garbage_rule_walkforward 2014 ... 2023
"""

from __future__ import annotations

import argparse

import numpy as np
import polars as pl

from sportsdataverse.cfb import load_cfb_pbp, load_cfb_schedule
from sportsdataverse.cfb.cfb_adjusted_epa import _ADJ_EPA_LAMBDA, _adjust_games, _fit_team_strengths
from sportsdataverse.cfb.cfb_opponent_adjust import flag_garbage_time
from sportsdataverse.cfb.cfb_ratings import _alias_released_pbp, _drop_kneel_downs, _home_into_pos_team_namespace

_FITS = {"wp": pl.col("wp_before_naive").is_between(0.05, 0.95), "margin": pl.col("garbage_time") == False}  # noqa: E712
VARIANTS = {
    "shipped": ("wp", False),
    "wp_both": ("wp", True),
    "margin_fit": ("margin", False),
    "margin_both": ("margin", True),
}


def season_plays(season: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """FBS-vs-FBS pass/rush plays with EPA (kneels dropped, garbage flagged) and one row per game."""
    pbp = _home_into_pos_team_namespace(
        _alias_released_pbp(load_cfb_pbp([season]).with_columns(pl.col("game_id").cast(pl.Utf8)))
    )
    sched = load_cfb_schedule([season]).select(
        pl.col("game_id").cast(pl.Utf8),
        "neutral_site",
        "home_division",
        "away_division",
        "home_points",
        "away_points",
        pl.col("home_id").cast(pl.Utf8),
        pl.col("away_id").cast(pl.Utf8),
    )
    assert pbp.schema["game_id"] == sched.schema["game_id"]
    p = _drop_kneel_downs(
        pbp.join(sched, on="game_id").filter((pl.col("home_division") == "fbs") & (pl.col("away_division") == "fbs"))
    )
    p = p.filter(pl.col("EPA").is_not_null() & ((pl.col("pass") == 1) | (pl.col("rush") == 1))).with_columns(
        pl.col("pos_team_id").cast(pl.Utf8),
        pl.col("def_pos_team_id").cast(pl.Utf8),
        abs_score_diff=pl.col("pos_score_diff_start").abs(),
        _order=pl.col("week") + pl.when(pl.col("seasonType") == 3).then(100).otherwise(0),
        hfa=pl.when(pl.col("neutral_site") == True)
        .then(0)
        .when(pl.col("pos_team") == pl.col("home"))
        .then(1)
        .otherwise(-1),  # noqa: E712
    )
    p = flag_garbage_time(p, margin_col="abs_score_diff")
    games = p.group_by("game_id").agg(
        pl.col("_order", "home_id", "away_id").first(),
        margin=pl.col("home_points").first() - pl.col("away_points").first(),
        home=pl.when(pl.col("neutral_site").first() == True).then(0).otherwise(1),  # noqa: E712
    )
    return p, games


def _ratings(base: pl.DataFrame, clean: pl.DataFrame, own: bool) -> pl.DataFrame:
    off, dfn, icpt = _fit_team_strengths(clean, _ADJ_EPA_LAMBDA)
    g = _adjust_games(clean if own else base, off, dfn, fill_strength=icpt)
    net = pl.col("adj_off_epa").mean() - pl.col("adj_def_epa").mean()
    return (
        g.group_by("pos_team_id")
        .agg(games=pl.len(), net=net)
        .filter(pl.col("games") >= 2)
        .rename({"pos_team_id": "team_id"})
    )


def season_run(season: int) -> tuple[pl.DataFrame, dict[str, float]]:
    """Every variant's week-w predictions (w >= 3rd week) and week-4 Spearman for one season."""
    p, games = season_plays(season)
    rest = games.filter(pl.col("_order") >= 4)
    rest = pl.concat([rest.select(team_id="home_id", m="margin"), rest.select(team_id="away_id", m=-pl.col("margin"))])
    rest = rest.group_by("team_id").agg(rest=pl.col("m").mean())
    preds, spearman = [], {}
    for w in sorted(games["_order"].unique().to_list())[2:]:
        prior = p.filter(pl.col("_order") < w)
        for name, (fit, own) in VARIANTS.items():
            r = _ratings(prior, prior.filter(_FITS[fit]), own).select("team_id", "net")
            preds.append(
                games.filter(pl.col("_order") == w)
                .join(r.rename({"team_id": "home_id", "net": "h"}), on="home_id")
                .join(r.rename({"team_id": "away_id", "net": "a"}), on="away_id")
                .with_columns(diff=pl.col("h") - pl.col("a"), variant=pl.lit(name), season=pl.lit(season))
            )
            if w == 4:
                spearman[name] = r.join(rest, on="team_id").select(pl.corr("net", "rest", method="spearman")).item()
    return pl.concat(preds).filter(pl.col("_order") >= 4), spearman


def score(preds: pl.DataFrame, seasons: list[int], n_boot: int = 2000) -> pl.DataFrame:
    """Leave-one-season-out MAE and (season, week)-cluster CIs of each variant's delta vs shipped."""
    errs = []
    for s in seasons:
        for v in VARIANTS:
            tr = preds.filter((pl.col("season") != s) & (pl.col("variant") == v))
            te = preds.filter((pl.col("season") == s) & (pl.col("variant") == v))
            c = np.linalg.lstsq(np.column_stack([tr["diff"], tr["home"]]), tr["margin"].to_numpy(), rcond=None)[0]
            e = (te["margin"] - (c[0] * te["diff"] + c[1] * te["home"])).abs()
            errs.append(te.select("season", "_order", "game_id", "variant").with_columns(err=e))
    wide = pl.concat(errs).pivot(on="variant", index=["season", "_order", "game_id"], values="err").drop_nulls()
    rng, rows = np.random.default_rng(7), []
    for scope, df in [*((str(s), wide.filter(pl.col("season") == s)) for s in seasons), ("pooled", wide)]:
        row: dict[str, object] = {"season": scope, "n": df.height, "MAE_shipped": df["shipped"].mean()}
        for v in [v for v in VARIANTS if v != "shipped"]:
            d = (
                df.with_columns(d=pl.col(v) - pl.col("shipped"))
                .group_by("season", "_order")
                .agg(s=pl.col("d").sum(), n=pl.len())
            )
            s_, n_ = d["s"].to_numpy(), d["n"].to_numpy()
            idx = rng.integers(0, len(s_), size=(n_boot, len(s_)))
            lo, hi = np.percentile(s_[idx].sum(1) / n_[idx].sum(1), [2.5, 97.5])
            row[v] = f"{s_.sum() / n_.sum():+.3f} [{lo:+.3f}, {hi:+.3f}]"
        rows.append(row)
    return pl.DataFrame(rows)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="cfb_garbage_rule_walkforward")
    ap.add_argument("seasons", nargs="+", type=int)
    seasons = ap.parse_args(argv).seasons
    runs = {s: season_run(s) for s in seasons}
    with pl.Config(tbl_cols=10, tbl_width_chars=200, tbl_rows=40):
        print(score(pl.concat([r[0] for r in runs.values()]), seasons))
        print(pl.DataFrame([{"season": s, **runs[s][1]} for s in seasons]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
